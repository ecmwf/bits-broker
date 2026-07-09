// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

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

    /// Recursively collects [`describe`](crate::actions::Action::describe) output
    /// from every action in the routing tree, including nested switches.
    ///
    /// Non-empty descriptors are returned as a flat list.
    pub fn describe_actions(&self) -> Vec<serde_json::Value> {
        let mut out = Vec::new();
        for route in &self.routes {
            for action in &route.actions {
                let desc = action.describe();
                if let serde_json::Value::Object(ref map) = desc
                    && !map.is_empty()
                {
                    out.push(desc);
                }
                if let Action::Switch(switch) = action {
                    out.extend(switch.describe_actions());
                }
            }
        }
        out
    }

    pub(crate) fn close_all(&self) {
        for route in &self.routes {
            for action in &route.actions {
                action.close();
            }
        }
    }

    pub fn validate(&self) -> Result<(), crate::error::RoutingError> {
        for route in &self.routes {
            validate_route(route)?;
        }
        Ok(())
    }
}

fn validate_route(route: &Route) -> Result<(), crate::error::RoutingError> {
    if route.actions.is_empty() {
        return Err(crate::error::RoutingError::InvalidRoute {
            route: route.name.clone(),
            reason: "must not be empty".to_string(),
        });
    }

    for (index, action) in route.actions.iter().enumerate() {
        if let Action::Switch(switch) = action {
            switch.validate()?;
        }

        if action.is_terminal() && index + 1 != route.actions.len() {
            return Err(crate::error::RoutingError::InvalidRoute {
                route: route.name.clone(),
                reason: format!("unreachable action(s) after terminal step at index {index}"),
            });
        }

        if action.is_terminal() {
            return Ok(());
        }
    }

    Err(crate::error::RoutingError::MissingTarget {
        route: route.name.clone(),
    })
}

// Switch implements TargetAction because its external contract is identical to a target's:
// it either completes the job (first matching route succeeds) or rejects it (no route matched).
// This also allows switches to be nested inside other pipelines as an Action::Switch.
#[async_trait]
impl TargetAction for Switch {
    async fn dispatch(&self, job: &Job) -> Result<TargetResult, ActionError> {
        let mut rejections: Vec<String> = Vec::new();

        'route: for pipeline in &self.routes {
            // Defer cloning the job until a Transform action actually needs to mutate it.
            let mut current_job: Cow<Job> = Cow::Borrowed(job);

            for action in &pipeline.actions {
                if current_job.is_cancelled() {
                    return Err(ActionError::Cancelled);
                }
                match action {
                    Action::Check(check, dispatcher, silent_override) => {
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
                            CheckResult::Reject { reason, silent } => {
                                if !silent_override.unwrap_or(silent) {
                                    rejections.push(reason);
                                }
                                continue 'route;
                            }
                        }
                    }
                    Action::Transform(transform, dispatcher, silent_override) => {
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
                                        .map_err(|_| {
                                            ActionError::ResourceError(
                                                "transform Arc still shared after dispatch".into(),
                                            )
                                        })?
                                        .into_inner();
                                    *current_job.to_mut() = modified;
                                }
                                result
                            }
                            None => transform.execute(current_job.to_mut()).await?,
                        };
                        match result {
                            TransformResult::Continue => {}
                            TransformResult::Reject { reason, silent } => {
                                if !silent_override.unwrap_or(silent) {
                                    rejections.push(reason);
                                }
                                continue 'route;
                            }
                        }
                    }
                    Action::Target(target, dispatcher, silent_override) => {
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
                            TargetResult::Reject { reason, silent } => {
                                if !silent_override.unwrap_or(silent) {
                                    rejections.push(reason);
                                }
                                continue 'route;
                            }
                        }
                    }
                    Action::Switch(switch) => match switch.dispatch(&current_job).await? {
                        TargetResult::Complete(result) => {
                            return Ok(TargetResult::Complete(result));
                        }
                        TargetResult::Reject { reason, silent } => {
                            if !silent {
                                rejections.push(reason);
                            }
                            continue 'route;
                        }
                    },
                }
            }
        }

        if rejections.is_empty() {
            Ok(TargetResult::Reject {
                reason: "no route matched the request".to_string(),
                silent: true,
            })
        } else {
            Ok(TargetResult::Reject {
                reason: rejections.join("; "),
                silent: false,
            })
        }
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
            vec![Action::Target(Arc::new(AlwaysSucceed), None, None)],
        )]);

        // Job::new() has reconnect_deadline = Instant::now() (immediately expired)
        // and active_pollers = 0, so client_present() returns false.
        let job = Job::new(serde_json::json!({}));
        let result = switch.dispatch(&job).await;
        assert!(matches!(result, Err(ActionError::ClientGone)));
    }

    #[tokio::test]
    async fn queued_target_rechecks_client_presence_before_execution() {
        let dispatcher = Dispatcher::<TargetResult>::from_config(
            Some(&QueueKind::Fifo),
            Some(&ExecutorKind::AsyncPool {
                concurrency: Some(1),
            }),
            None,
            None,
            crate::dispatcher::DEFAULT_QUEUE_CAPACITY,
        )
        .expect("dispatcher config should not error")
        .expect("dispatcher should be created");
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
                None,
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
                None,
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
        let dispatcher = Dispatcher::<CheckResult>::from_config(
            Some(&QueueKind::Fifo),
            Some(&ExecutorKind::AsyncPool {
                concurrency: Some(1),
            }),
            None,
            None,
            crate::dispatcher::DEFAULT_QUEUE_CAPACITY,
        )
        .expect("dispatcher config should not error")
        .expect("dispatcher should be created");

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
                    None,
                ),
                Action::Target(Arc::new(AlwaysSucceed), None, None),
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
                    None,
                ),
                Action::Target(Arc::new(AlwaysSucceed), None, None),
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
