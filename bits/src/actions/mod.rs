use std::sync::Arc;

use crate::job::Job;
use crate::result::JobResult;
use async_trait::async_trait;

pub mod check_hasrole;
pub mod target_http;
pub mod target_remote;
pub mod registry;

pub use check_hasrole::*;
pub use target_http::*;
pub use target_remote::*;
pub use registry::{create_action, list_actions};

// Re-export the macro at crate root for convenience
pub use crate::register_action;

// ================================
//   Action Errors
// ================================

#[derive(Debug)]
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
            ActionError::ClientGone => write!(f, "Client disconnected before data could be delivered"),
        }
    }
}

impl std::error::Error for ActionError {}

// ================================
//   Actions
// ================================

pub enum Action {
    Check(Arc<dyn CheckAction>, Option<crate::dispatcher::Dispatcher<CheckResult>>),
    Transform(Arc<dyn TransformAction>, Option<crate::dispatcher::Dispatcher<TransformResult>>),
    Target(Arc<dyn TargetAction>, Option<crate::dispatcher::Dispatcher<TargetResult>>),
    Switch(crate::routing::switch::Switch),
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
pub enum CheckResult {
    Pass,
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
pub enum TransformResult {
    Continue,
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
pub enum TargetResult {
    Complete(JobResult),
    Reject { reason: String },
}
