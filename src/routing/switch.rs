use crate::{
    job::Job,
    routing::{
        actions::{Action, ActionError, CheckResult, HopAction, HopResult},
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
impl HopAction for Switch {
    async fn hop(&self, job: &mut Job) -> Result<HopResult, ActionError> {
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
                        via.execute(current_job.to_mut()).await?;
                    }
                    Action::Router(router) => match router.hop(current_job.to_mut()).await? {
                        HopResult::Complete(result) => {
                            return Ok(HopResult::Complete(result));
                        }
                        HopResult::Reject { reason: _ } => {
                            break;
                        }
                    },
                }
            }
        }

        Ok(HopResult::Reject {
            reason: "No route in switch matched the job".to_string(),
        })
    }
}
