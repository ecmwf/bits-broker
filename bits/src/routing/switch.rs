use crate::{
    job::Job,
    actions::{Action, ActionError, CheckResult, TargetAction, TargetResult, TransformResult},
    routing::Route,
};
use async_trait::async_trait;
use std::borrow::Cow;
use std::collections::HashMap;

/// Tries named routes in sequence, returning the result of the first that does not reject.
#[derive(Debug)]
pub struct Switch {
    routes: HashMap<String, Route>,
}

impl Switch {
    pub fn new(routes: HashMap<String, Route>) -> Self {
        Self { routes }
    }
}

// Switch implements TargetAction because its external contract is identical to a target's:
// it either completes the job (first matching route succeeds) or rejects it (no route matched).
// This also allows switches to be nested inside other pipelines as an Action::Switch.
#[async_trait]
impl TargetAction for Switch {
    async fn dispatch(&self, job: &Job) -> Result<TargetResult, ActionError> {
        'route: for (_name, pipeline) in &self.routes {
            // Defer cloning the job until a Transform action actually needs to mutate it.
            let mut current_job: Cow<Job> = Cow::Borrowed(job);

            for action in &pipeline.actions {
                match action {
                    Action::Check(check) => match check.evaluate(&current_job).await? {
                        CheckResult::Pass => {}
                        CheckResult::Reject { .. } => continue 'route,
                    },
                    Action::Transform(transform) => match transform.execute(current_job.to_mut()).await? {
                        TransformResult::Continue => {}
                        TransformResult::Reject { .. } => continue 'route,
                    },
                    Action::Target(target) => match target.dispatch(&current_job).await? {
                        TargetResult::Complete(result) => return Ok(TargetResult::Complete(result)),
                        TargetResult::Reject { .. } => continue 'route,
                    },
                    Action::Switch(switch) => match switch.dispatch(&current_job).await? {
                        TargetResult::Complete(result) => return Ok(TargetResult::Complete(result)),
                        TargetResult::Reject { .. } => continue 'route,
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
