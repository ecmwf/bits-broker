pub mod actions;

use crate::config::{Action, DestinationTarget};
use crate::job::{Job, JobResult};
use crate::queue::QueueManager;
use std::collections::HashMap;
use std::sync::Arc;
use std::pin::Pin;
use std::future::Future;

/// Router executes the routing logic for jobs.
pub struct Router {
    routes: HashMap<String, Vec<Action>>,
    queue_manager: Arc<QueueManager>,
}

impl Router {
    /// Create a new router with the given routes and queue manager.
    pub fn new(routes: &HashMap<String, Vec<Action>>, queue_manager: &QueueManager) -> Self {
        Self {
            routes: routes.clone(),
            queue_manager: Arc::new(queue_manager.clone()),
        }
    }

    /// Route a job through the configured routes.
    pub async fn route_job(&self, job: Job) -> JobResult {
        // Try each route in order until one matches
        for (route_name, actions) in &self.routes {
            if let Some(result) = self.try_route(&job, route_name, actions).await {
                return result;
            }
        }

        // No route matched
        JobResult::error("No matching route found for job")
    }

    /// Try to execute a specific route.
    fn try_route<'a>(&'a self, job: &'a Job, route_name: &'a str, actions: &'a [Action]) -> Pin<Box<dyn Future<Output = Option<JobResult>> + 'a>> {
        Box::pin(async move {
            let mut current_job = job.clone();

            for action in actions {
                match self.execute_action(&mut current_job, action).await {
                    ActionResult::Continue => continue,
                    ActionResult::Complete(result) => return Some(result),
                    ActionResult::Skip => return None, // This route doesn't match
                }
            }

            // If we get here, all actions passed but no destination was reached
            Some(JobResult::error(format!("Route '{}' completed without destination", route_name)))
        })
    }

    /// Execute a single action.
    async fn execute_action(&self, job: &mut Job, action: &Action) -> ActionResult {
        match action {
            Action::Filter { condition } => {
                if job.matches_condition(condition) {
                    ActionResult::Continue
                } else {
                    ActionResult::Skip
                }
            }
            
            Action::Check { rule: _rule } => {
                // TODO: Implement rule checking system
                // For now, just pass through
                ActionResult::Continue
            }
            
            Action::Via { queue } => {
                match self.queue_manager.process_via_queue(job.clone(), queue).await {
                    Ok(processed_job) => {
                        *job = processed_job;
                        ActionResult::Continue
                    }
                    Err(e) => ActionResult::Complete(JobResult::error(format!("Via queue error: {}", e))),
                }
            }
            
            Action::Switch { routes } => {
                // Try each sub-route
                for (sub_route_name, sub_actions) in routes {
                    if let Some(result) = self.try_route(job, sub_route_name, sub_actions).await {
                        return ActionResult::Complete(result);
                    }
                }
                ActionResult::Skip
            }
            
            Action::Destination { target } => {
                match target {
                    DestinationTarget::Queue { queue } => {
                        match self.queue_manager.send_to_destination_queue(job.clone(), queue).await {
                            Ok(result) => ActionResult::Complete(JobResult::Completed(result)),
                            Err(e) => ActionResult::Complete(JobResult::error(format!("Destination queue error: {}", e))),
                        }
                    }
                    DestinationTarget::Http { url: _url, method: _method, headers: _headers, timeout: _timeout } => {
                        // TODO: Implement HTTP client
                        ActionResult::Complete(JobResult::Forwarded)
                    }
                }
            }
        }
    }
}

/// Result of executing an action.
enum ActionResult {
    /// Continue to the next action.
    Continue,
    /// Complete the job with this result.
    Complete(JobResult),
    /// Skip this route (filter didn't match).
    Skip,
} 