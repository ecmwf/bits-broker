use std::sync::Arc;

use crate::job::Job;
use crate::result::JobResult;
use async_trait::async_trait;

pub mod registry;
pub mod target_http;
pub mod target_remote;

pub use registry::{create_action, list_actions};
pub use target_http::*;
pub use target_remote::*;

// Re-export the macro at crate root for convenience
pub use crate::register_action;

// ================================
//   Action Errors
// ================================

#[derive(Debug)]
/// Error returned by an action during routing or dispatch.
pub enum ActionError {
    NetworkError(String),
    QueueFull(String),
    Timeout(String),
    ConfigError(String),
    AuthError(String),
    ResourceError(String),
    Cancelled,
    ClientGone,
}

impl std::fmt::Display for ActionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ActionError::NetworkError(msg) => write!(f, "Network error: {}", msg),
            ActionError::QueueFull(msg) => write!(f, "Queue full: {}", msg),
            ActionError::Timeout(msg) => write!(f, "Timeout: {}", msg),
            ActionError::ConfigError(msg) => write!(f, "Config error: {}", msg),
            ActionError::AuthError(msg) => write!(f, "Auth error: {}", msg),
            ActionError::ResourceError(msg) => write!(f, "Resource error: {}", msg),
            ActionError::Cancelled => write!(f, "Cancelled"),
            ActionError::ClientGone => {
                write!(f, "Client disconnected before data could be delivered")
            }
        }
    }
}

impl std::error::Error for ActionError {}

// ================================
//   Actions
// ================================

/// A configured pipeline step.
pub enum Action {
    Check(
        Arc<dyn CheckAction>,
        Option<crate::dispatcher::Dispatcher<CheckResult>>,
    ),
    Transform(
        Arc<dyn TransformAction>,
        Option<crate::dispatcher::Dispatcher<TransformResult>>,
    ),
    Target(
        Arc<dyn TargetAction>,
        Option<crate::dispatcher::Dispatcher<TargetResult>>,
    ),
    Switch(crate::routing::switch::Switch),
}

impl Action {
    /// Returns true when this action ends a route.
    pub fn is_terminal(&self) -> bool {
        matches!(self, Action::Target(..) | Action::Switch(..))
    }
}

impl std::fmt::Debug for Action {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Action::Check(..) => write!(f, "Action::Check(..)"),
            Action::Transform(..) => write!(f, "Action::Transform(..)"),
            Action::Target(..) => write!(f, "Action::Target(..)"),
            Action::Switch(_) => write!(f, "Action::Switch(..)"),
        }
    }
}

// ================================
//   Check Actions
// ================================

/// Guard conditions on a job. A rejection stops the current pipeline branch.
#[async_trait]
pub trait CheckAction: Send + Sync {
    async fn evaluate(&self, job: &Job) -> Result<CheckResult, ActionError>;
}

#[derive(Debug)]
/// Result of evaluating a check action.
pub enum CheckResult {
    /// The job may continue through the route.
    Pass,
    /// The route is rejected with a human-readable reason.
    Reject { reason: String },
}

// ================================
//   Transform Actions
// ================================

/// Transformations that mutate the job and continue the pipeline.
#[async_trait]
pub trait TransformAction: Send + Sync {
    async fn execute(&self, job: &mut Job) -> Result<TransformResult, ActionError>;
}

#[derive(Debug)]
/// Result of running a transform action.
pub enum TransformResult {
    /// The job was updated and may continue through the route.
    Continue,
    /// The route is rejected with a human-readable reason.
    Reject { reason: String },
}

// ================================
//   Target Actions
// ================================

/// Terminal dispatch — sends the job to a destination and returns a result.
#[async_trait]
pub trait TargetAction: Send + Sync {
    async fn dispatch(&self, job: &Job) -> Result<TargetResult, ActionError>;
}

#[derive(Debug)]
/// Result of dispatching a job to a terminal target.
pub enum TargetResult {
    /// The target produced a final job result.
    Complete(JobResult),
    /// The target rejected the job without a system failure.
    Reject { reason: String },
}
