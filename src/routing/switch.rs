use crate::{
    job::Job,
    actions::{Action, ActionError, CheckResult, RouteAction, RouteResult, ViaResult},
    routing::Route,
};
use async_trait::async_trait;
use std::borrow::Cow;
use std::collections::HashMap;

/// Tries named route branches in sequence, returning the result of the first that does not reject.
#[derive(Debug)]
pub struct Switch {
    routes: HashMap<String, Route>,
}

impl Switch {
    pub fn new(routes: HashMap<String, Route>) -> Self {
        Self { routes }
    }
}

#[async_trait]
impl RouteAction for Switch {
    async fn route(&self, job: &Job) -> Result<RouteResult, ActionError> {
        'route: for (_name, route) in &self.routes {
            // Defer cloning the job until a Via action actually needs to mutate it.
            let mut current_job: Cow<Job> = Cow::Borrowed(job);

            for action in &route.actions {
                match action {
                    Action::Check(check) => match check.evaluate(&current_job).await? {
                        CheckResult::Pass => {}
                        CheckResult::Reject { .. } => continue 'route,
                    },
                    Action::Via(via) => match via.execute(current_job.to_mut()).await? {
                        ViaResult::Continue => {}
                        ViaResult::Reject { .. } => continue 'route,
                    },
                    Action::Router(router) => match router.route(&current_job).await? {
                        RouteResult::Complete(result) => return Ok(RouteResult::Complete(result)),
                        RouteResult::Reject { .. } => continue 'route,
                    },
                    Action::Switch(switch) => match switch.route(&current_job).await? {
                        RouteResult::Complete(result) => return Ok(RouteResult::Complete(result)),
                        RouteResult::Reject { .. } => continue 'route,
                    },
                    Action::Persist => {
                        current_job.to_mut().persistent = true;
                    }
                    Action::Queue { .. } => {
                        todo!("queue execution not yet implemented")
                    }
                }
            }
        }

        Ok(RouteResult::Reject {
            reason: "No route matched the job".to_string(),
        })
    }
}
