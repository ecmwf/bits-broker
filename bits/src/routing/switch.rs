use std::borrow::Cow;
use std::sync::Arc;

use async_trait::async_trait;
use futures::future::BoxFuture;

use crate::{
    job::Job,
    actions::{Action, ActionError, CheckResult, TargetAction, TargetResult, TransformResult},
    routing::Route,
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::result::JobResult;

    struct AlwaysSucceed;

    #[async_trait]
    impl TargetAction for AlwaysSucceed {
        async fn dispatch(&self, _job: &Job) -> Result<TargetResult, ActionError> {
            Ok(TargetResult::Complete(JobResult::Error { message: "dummy".into() }))
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
}

/// Tries named routes in sequence, returning the result of the first that does not reject.
#[derive(Debug)]
pub struct Switch {
    routes: Vec<Route>,
}

impl Switch {
    pub fn new(routes: Vec<Route>) -> Self {
        Self { routes }
    }
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
                                d.dispatch(&current_job, work).await?
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
                                let job_mux = Arc::new(tokio::sync::Mutex::new((*current_job).clone()));
                                let job_mux2 = Arc::clone(&job_mux);
                                let work: BoxFuture<'static, Result<TransformResult, ActionError>> =
                                    Box::pin(async move {
                                        let mut guard = job_mux2.lock().await;
                                        t.execute(&mut *guard).await
                                    });
                                let result = d.dispatch(&current_job, work).await?;
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
                        if !current_job.client_present() {
                            return Err(ActionError::ClientGone);
                        }
                        let result = match dispatcher {
                            Some(d) => {
                                let t = Arc::clone(target);
                                let j = (*current_job).clone();
                                let work: BoxFuture<'static, Result<TargetResult, ActionError>> =
                                    Box::pin(async move { t.dispatch(&j).await });
                                d.dispatch(&current_job, work).await?
                            }
                            None => target.dispatch(&current_job).await?,
                        };
                        match result {
                            TargetResult::Complete(result) => return Ok(TargetResult::Complete(result)),
                            TargetResult::Reject { .. } => continue 'route,
                        }
                    },
                    Action::Switch(switch) => {
                        match switch.dispatch(&current_job).await? {
                            TargetResult::Complete(result) => return Ok(TargetResult::Complete(result)),
                            TargetResult::Reject { .. } => continue 'route,
                        }
                    },
                }
            }
        }

        Ok(TargetResult::Reject {
            reason: "No route matched the job".to_string(),
        })
    }
}
