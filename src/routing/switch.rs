use crate::{
    job::Job,
    routing::{
        actions::{Action, ActionError, CheckResult, RouteAction, RouteResult, ViaResult},
        Route,
    },
};
use async_trait::async_trait;
use std::borrow::Cow;

/// A switch that tries multiple routes in sequence, returning the first route that is not rejected
pub struct Switch {
    routes: Vec<Route>,
}

impl Switch {
    pub fn new(routes: Vec<Route>) -> Self {
        Self { routes }
    }
}

#[async_trait]
impl RouteAction for Switch {
    async fn route(&self, job: &mut Job) -> Result<RouteResult, ActionError> {
        for route in &self.routes {
            // we only need to clone the job if we need to send it to a Via or Router action
            // this stops us needing to clone the job for every branch of the switch
            let mut current_job: Cow<Job> = Cow::Borrowed(job);

            for action in &route.actions {
                match action {
                    Action::Check(check) => match check.evaluate(&current_job).await? {
                        CheckResult::Pass => continue,
                        CheckResult::Reject { reason: _ } => {
                            break;
                        }
                    },
                    Action::Via(via) => {
                        match via.execute(current_job.to_mut()).await? {
                            ViaResult::Continue => continue,
                            ViaResult::Reject { reason: _ } => {
                                break;
                            }
                        }
                    }
                    Action::Router(router) => match router.route(current_job.to_mut()).await? {
                        RouteResult::Complete(result) => {
                            return Ok(RouteResult::Complete(result));
                        }
                        RouteResult::Reject { reason: _ } => {
                            break;
                        }
                    },
                }
            }
        }

        Ok(RouteResult::Reject {
            reason: "No route in switch matched the job".to_string(),
        })
    }
}
