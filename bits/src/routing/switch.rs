use crate::{
    job::Job,
    actions::{Action, ActionError, CheckResult, TargetAction, TargetResult, TransformResult},
    routing::Route,
};
use async_trait::async_trait;
use std::borrow::Cow;

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
            vec![Action::Target(Box::new(AlwaysSucceed))],
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
                    Action::Check(check) => match check.evaluate(&current_job).await? {
                        CheckResult::Pass => {}
                        CheckResult::Reject { .. } => continue 'route,
                    },
                    Action::Transform(transform) => match transform.execute(current_job.to_mut()).await? {
                        TransformResult::Continue => {}
                        TransformResult::Reject { .. } => continue 'route,
                    },
                    Action::Target(target) => {
                        if !current_job.client_present() {
                            return Err(ActionError::ClientGone);
                        }
                        match target.dispatch(&current_job).await? {
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
                    Action::Persist => {
                        current_job.to_mut().persistent = true;
                    }
                }
            }
        }

        Ok(TargetResult::Reject {
            reason: "No route matched the job".to_string(),
        })
    }
}
