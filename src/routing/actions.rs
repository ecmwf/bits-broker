use crate::job::{Job, JobResult};
use async_trait::async_trait;
use serde_json::Value;

/// Trait for all actions that can be executed on jobs.
#[async_trait]
pub trait Action: Send + Sync {
    /// Execute this action on a job.
    async fn execute(&self, job: Job) -> ActionResult;
}

/// Result of executing an action.
#[derive(Debug)]
pub enum ActionResult {
    /// Continue processing with the (possibly modified) job.
    Continue(Job),
    /// Complete processing with this result.
    Complete(JobResult),
    /// Skip this action/route (condition not met).
    Skip,
}

/// Filter action - tests conditions on job data.
#[derive(Debug, Clone)]
pub struct FilterAction {
    pub condition: Value,
}

impl FilterAction {
    pub fn new(condition: Value) -> Self {
        Self { condition }
    }
}

#[async_trait]
impl Action for FilterAction {
    async fn execute(&self, job: Job) -> ActionResult {
        if job.matches_condition(&self.condition) {
            ActionResult::Continue(job)
        } else {
            ActionResult::Skip
        }
    }
}

/// Check action - runs a named rule/guard.
#[derive(Debug, Clone)]
pub struct CheckAction {
    pub rule_name: String,
}

impl CheckAction {
    pub fn new(rule_name: String) -> Self {
        Self { rule_name }
    }
}

#[async_trait]
impl Action for CheckAction {
    async fn execute(&self, job: Job) -> ActionResult {
        // TODO: Implement rule checking system
        // For now, just pass through
        ActionResult::Continue(job)
    }
}

/// Via action - processes job through a queue.
#[derive(Debug, Clone)]
pub struct ViaAction {
    pub queue_name: String,
}

impl ViaAction {
    pub fn new(queue_name: String) -> Self {
        Self { queue_name }
    }
}

#[async_trait]
impl Action for ViaAction {
    async fn execute(&self, job: Job) -> ActionResult {
        // TODO: Process through queue and return modified job
        // For now, just pass through
        ActionResult::Continue(job)
    }
}

/// Switch action - contains nested routes.
pub struct SwitchAction {
    pub routes: Vec<(String, Vec<Box<dyn Action>>)>,
}

impl SwitchAction {
    pub fn new() -> Self {
        Self {
            routes: Vec::new(),
        }
    }

    pub fn add_route(mut self, name: String, actions: Vec<Box<dyn Action>>) -> Self {
        self.routes.push((name, actions));
        self
    }
}

#[async_trait]
impl Action for SwitchAction {
    async fn execute(&self, job: Job) -> ActionResult {
        // Try each route until one succeeds
        for (_route_name, actions) in &self.routes {
            let mut current_job = job.clone();
            let mut route_matches = true;

            for action in actions {
                match action.execute(current_job.clone()).await {
                    ActionResult::Continue(modified_job) => {
                        current_job = modified_job;
                    }
                    ActionResult::Complete(result) => {
                        return ActionResult::Complete(result);
                    }
                    ActionResult::Skip => {
                        route_matches = false;
                        break;
                    }
                }
            }

            if route_matches {
                return ActionResult::Continue(current_job);
            }
        }

        ActionResult::Skip
    }
}

/// Destination action - final routing destination.
#[derive(Debug, Clone)]
pub enum DestinationAction {
    Queue { queue: String },
    Http {
        url: String,
        method: Option<String>,
        headers: Option<std::collections::HashMap<String, String>>,
    },
}

impl DestinationAction {
    pub fn queue(name: String) -> Self {
        Self::Queue { queue: name }
    }

    pub fn http(url: String) -> Self {
        Self::Http {
            url,
            method: None,
            headers: None,
        }
    }

    pub fn http_with_method(url: String, method: String) -> Self {
        Self::Http {
            url,
            method: Some(method),
            headers: None,
        }
    }
}

#[async_trait]
impl Action for DestinationAction {
    async fn execute(&self, job: Job) -> ActionResult {
        match self {
            DestinationAction::Queue { queue: _queue } => {
                // TODO: Send to destination queue
                ActionResult::Complete(JobResult::completed(job).unwrap_or_else(|e| {
                    JobResult::error(format!("Failed to serialize job: {}", e))
                }))
            }
            DestinationAction::Http { url: _url, method: _method, headers: _headers } => {
                // TODO: Send HTTP request
                ActionResult::Complete(JobResult::Forwarded)
            }
        }
    }
} 