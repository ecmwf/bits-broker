use std::borrow::Cow;
use std::sync::Arc;

use async_trait::async_trait;
use futures::future::BoxFuture;

use crate::{
    actions::{Action, ActionError, CheckResult, TargetAction, TargetResult, TransformResult},
    dispatcher::DispatchGuard,
    job::Job,
    routing::Route,
};

/// Tries named routes in sequence, returning the result of the first that does not reject.
#[derive(Debug)]
pub struct Switch {
    routes: Vec<Route>,
}

impl Switch {
    pub fn new(routes: Vec<Route>) -> Self {
        Self { routes }
    }

    pub fn route_names(&self) -> Vec<&str> {
        self.routes.iter().map(|r| r.name.as_str()).collect()
    }

    pub fn validate(&self) -> Result<(), ActionError> {
        for route in &self.routes {
            validate_route(route)?;
        }
        Ok(())
    }
}

fn validate_route(route: &Route) -> Result<(), ActionError> {
    if route.actions.is_empty() {
        return Err(ActionError::ConfigError(format!(
            "route '{}' must not be empty",
            route.name
        )));
    }

    for (index, action) in route.actions.iter().enumerate() {
        if let Action::Switch(switch) = action {
            switch.validate()?;
        }

        if action.is_terminal() {
            if index + 1 != route.actions.len() {
                return Err(ActionError::ConfigError(format!(
                    "route '{}' has unreachable action(s) after terminal step at index {}",
                    route.name, index
                )));
            }
            return Ok(());
        }
    }

    Err(ActionError::ConfigError(format!(
        "route '{}' must end with a target or switch",
        route.name
    )))
}

// Switch implements TargetAction because its external contract is identical to a target's:
// it either completes the job (first matching route succeeds) or rejects it (no route matched).
// This also allows switches to be nested inside other pipelines as an Action::Switch.
#[async_trait]
impl TargetAction for Switch {
    async fn dispatch(&self, job: &Job) -> Result<TargetResult, ActionError> {
        'route: for pipeline in &self.routes {
            // Defer cloning the job until a Transform action actually needs to mutate it.
            let mut current_job: Cow<Job> = Cow::Borrowed(job);

            for action in &pipeline.actions {
                if current_job.is_cancelled() {
                    return Err(ActionError::Cancelled);
                }
                match action {
                    Action::Check(check, dispatcher) => {
                        let result = match dispatcher {
                            Some(d) => {
                                let c = Arc::clone(check);
                                let j = (*current_job).clone();
                                let work: BoxFuture<'static, Result<CheckResult, ActionError>> =
                                    Box::pin(async move { c.evaluate(&j).await });
                                d.dispatch(&current_job, DispatchGuard::Cancelled, work)
                                    .await?
                            }
                            None => check.evaluate(&current_job).await?,
                        };
                        match result {
                            CheckResult::Pass => {}
                            CheckResult::Reject { .. } => continue 'route,
                        }
                    }
                    Action::Transform(transform, dispatcher) => {
                        let result = match dispatcher {
                            Some(d) => {
                                let t = Arc::clone(transform);
                                let job_mux =
                                    Arc::new(tokio::sync::Mutex::new((*current_job).clone()));
                                let job_mux2 = Arc::clone(&job_mux);
                                let work: BoxFuture<'static, Result<TransformResult, ActionError>> =
                                    Box::pin(async move {
                                        let mut guard = job_mux2.lock().await;
                                        t.execute(&mut guard).await
                                    });
                                let result = d
                                    .dispatch(&current_job, DispatchGuard::Cancelled, work)
                                    .await?;
                                if matches!(result, TransformResult::Continue) {
                                    let modified = Arc::try_unwrap(job_mux)
                                        .expect("work future completed; Arc should be unique")
                                        .into_inner();
                                    *current_job.to_mut() = modified;
                                }
                                result
                            }
                            None => transform.execute(current_job.to_mut()).await?,
                        };
                        match result {
                            TransformResult::Continue => {}
                            TransformResult::Reject { .. } => continue 'route,
                        }
                    }
                    Action::Target(target, dispatcher) => {
                        let result = match dispatcher {
                            Some(d) => {
                                let t = Arc::clone(target);
                                let j = (*current_job).clone();
                                let work: BoxFuture<'static, Result<TargetResult, ActionError>> =
                                    Box::pin(async move { t.dispatch(&j).await });
                                d.dispatch(&current_job, DispatchGuard::CancelledOrClientGone, work)
                                    .await?
                            }
                            None => {
                                if current_job.is_cancelled() {
                                    return Err(ActionError::Cancelled);
                                }
                                if !current_job.client_present() {
                                    return Err(ActionError::ClientGone);
                                }
                                target.dispatch(&current_job).await?
                            }
                        };
                        match result {
                            TargetResult::Complete(result) => {
                                return Ok(TargetResult::Complete(result));
                            }
                            TargetResult::Reject { .. } => continue 'route,
                        }
                    }
                    Action::Switch(switch) => match switch.dispatch(&current_job).await? {
                        TargetResult::Complete(result) => {
                            return Ok(TargetResult::Complete(result));
                        }
                        TargetResult::Reject { .. } => continue 'route,
                    },
                }
            }
        }

        Ok(TargetResult::Reject {
            reason: "No route matched the job".to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicUsize, Ordering};

    use super::*;
    use crate::dispatcher::{Dispatcher, ExecutorKind, QueueKind};
    use crate::result::JobResult;

    struct AlwaysSucceed;

    #[async_trait]
    impl TargetAction for AlwaysSucceed {
        async fn dispatch(&self, _job: &Job) -> Result<TargetResult, ActionError> {
            Ok(TargetResult::Complete(JobResult::Error {
                message: "dummy".into(),
            }))
        }
    }

    #[tokio::test]
    async fn client_gone_before_target() {
        let switch = Switch::new(vec![Route::new(
            "default".to_string(),
            vec![Action::Target(Arc::new(AlwaysSucceed), None)],
        )]);

        // Job::new() has reconnect_deadline = Instant::now() (immediately expired)
        // and client_connected = false, so client_present() returns false.
        let job = Job::new(serde_json::json!({}));
        let result = switch.dispatch(&job).await;
        assert!(matches!(result, Err(ActionError::ClientGone)));
    }

    #[tokio::test]
    async fn queued_target_rechecks_client_presence_before_execution() {
        let dispatcher = Dispatcher::<TargetResult>::from_config(Some(&QueueKind::Fifo), Some(&ExecutorKind::AsyncPool {
            concurrency: Some(1),
        }), None, None)
        .unwrap();
        let ran = Arc::new(AtomicUsize::new(0));

        struct CountingTarget {
            ran: Arc<AtomicUsize>,
        }

        #[async_trait]
        impl TargetAction for CountingTarget {
            async fn dispatch(&self, _job: &Job) -> Result<TargetResult, ActionError> {
                self.ran.fetch_add(1, Ordering::SeqCst);
                Ok(TargetResult::Complete(JobResult::Error {
                    message: "dummy".into(),
                }))
            }
        }

        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        struct BlockingTarget {
            release_rx: std::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
        }

        #[async_trait]
        impl TargetAction for BlockingTarget {
            async fn dispatch(&self, _job: &Job) -> Result<TargetResult, ActionError> {
                let rx = self.release_rx.lock().unwrap().take().unwrap();
                let _ = rx.await;
                Ok(TargetResult::Complete(JobResult::Error {
                    message: "blocker".into(),
                }))
            }
        }

        let switch = Switch::new(vec![Route::new(
            "blocker".to_string(),
            vec![Action::Target(
                Arc::new(BlockingTarget {
                    release_rx: std::sync::Mutex::new(Some(release_rx)),
                }),
                Some(dispatcher.clone()),
            )],
        )]);

        let blocker_job = Job::new(serde_json::json!({}));
        blocker_job.set_reconnect_deadline_for_test(
            std::time::Instant::now() + std::time::Duration::from_secs(5),
        );
        let blocker_handle = tokio::spawn(async move { switch.dispatch(&blocker_job).await });

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let switch = Switch::new(vec![Route::new(
            "default".to_string(),
            vec![Action::Target(
                Arc::new(CountingTarget {
                    ran: Arc::clone(&ran),
                }),
                Some(dispatcher),
            )],
        )]);

        let job = Job::new(serde_json::json!({}));
        job.set_reconnect_deadline_for_test(
            std::time::Instant::now() + std::time::Duration::from_secs(5),
        );
        let dispatch_handle = tokio::spawn({
            let switch = switch;
            let job = job.clone();
            async move { switch.dispatch(&job).await }
        });

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        job.set_reconnect_deadline_for_test(std::time::Instant::now());

        let _ = release_tx.send(());
        let _ = blocker_handle.await;
        let result = dispatch_handle.await.unwrap();

        assert!(matches!(result, Err(ActionError::ClientGone)));
        assert_eq!(ran.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn queued_check_rechecks_cancellation_before_execution() {
        let dispatcher = Dispatcher::<CheckResult>::from_config(Some(&QueueKind::Fifo), Some(&ExecutorKind::AsyncPool {
            concurrency: Some(1),
        }), None, None)
        .unwrap();

        struct BlockingCheck {
            release_rx: std::sync::Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
        }

        #[async_trait]
        impl crate::actions::CheckAction for BlockingCheck {
            async fn evaluate(&self, _job: &Job) -> Result<CheckResult, ActionError> {
                let rx = self.release_rx.lock().unwrap().take().unwrap();
                let _ = rx.await;
                Ok(CheckResult::Pass)
            }
        }

        struct CountingCheck {
            ran: Arc<AtomicUsize>,
        }

        #[async_trait]
        impl crate::actions::CheckAction for CountingCheck {
            async fn evaluate(&self, _job: &Job) -> Result<CheckResult, ActionError> {
                self.ran.fetch_add(1, Ordering::SeqCst);
                Ok(CheckResult::Pass)
            }
        }

        let ran = Arc::new(AtomicUsize::new(0));
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();

        let switch = Switch::new(vec![Route::new(
            "default".to_string(),
            vec![
                Action::Check(
                    Arc::new(BlockingCheck {
                        release_rx: std::sync::Mutex::new(Some(release_rx)),
                    }),
                    Some(dispatcher.clone()),
                ),
                Action::Target(Arc::new(AlwaysSucceed), None),
            ],
        )]);

        let blocker_job = Job::new(serde_json::json!({}));
        blocker_job.set_reconnect_deadline_for_test(
            std::time::Instant::now() + std::time::Duration::from_secs(5),
        );
        let blocker_handle = tokio::spawn(async move { switch.dispatch(&blocker_job).await });

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        let switch = Switch::new(vec![Route::new(
            "default".to_string(),
            vec![
                Action::Check(
                    Arc::new(CountingCheck {
                        ran: Arc::clone(&ran),
                    }),
                    Some(dispatcher),
                ),
                Action::Target(Arc::new(AlwaysSucceed), None),
            ],
        )]);

        let job = Job::new(serde_json::json!({}));
        job.set_reconnect_deadline_for_test(
            std::time::Instant::now() + std::time::Duration::from_secs(5),
        );
        let dispatch_handle = tokio::spawn({
            let switch = switch;
            let job = job.clone();
            async move { switch.dispatch(&job).await }
        });

        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        job.cancelled
            .store(true, std::sync::atomic::Ordering::Relaxed);

        let _ = release_tx.send(());
        let _ = blocker_handle.await;
        let result = dispatch_handle.await.unwrap();

        assert!(matches!(result, Err(ActionError::Cancelled)));
        assert_eq!(ran.load(Ordering::SeqCst), 0);
    }
}
