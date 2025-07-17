use crate::{
    job::Job,
    actions::{Action, ActionError, CheckResult, RouteAction, RouteResult, ViaResult},
    routing::Route,
};
use async_trait::async_trait;
use serde::{Deserialize, Deserializer};
use serde_json::Value;
use std::borrow::Cow;
use std::collections::HashMap;

/// A switch that tries multiple routes in sequence, returning the first route that is not rejected
#[derive(Debug)]
pub struct Switch {
    routes: HashMap<String, Route>,
}

impl<'de> Deserialize<'de> for Switch {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let routes_map: HashMap<String, Value> = HashMap::deserialize(deserializer)?;
        let mut routes = HashMap::new();
        
        for (name, config) in routes_map {
            let mut route: Route = Route::deserialize(config)
                .map_err(|e| serde::de::Error::custom(format!("Failed to parse route '{}': {}", name, e)))?;
            route.set_name(name.clone());
            routes.insert(name, route);
        }
        
        Ok(Switch { routes })
    }
}

impl Switch {
    pub fn new(routes: HashMap<String, Route>) -> Self {
        Self { routes }
    }


}

#[async_trait]
impl RouteAction for Switch {
    async fn route(&self, job: &Job) -> Result<RouteResult, ActionError> {
        for (_route_name, route) in &self.routes {
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
                    Action::Router(router) => match router.route(&current_job).await? {
                        RouteResult::Complete(result) => {
                            return Ok(RouteResult::Complete(result));
                        }
                        RouteResult::Reject { reason: _ } => {
                            break;
                        }
                    },
                    Action::Switch(switch) => match switch.route(&current_job).await? {
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
