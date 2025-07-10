use crate::job::Job;
use crate::result::JobResult;
use async_trait::async_trait;
use axum::routing::Route;

// ================================
//        Action Errors
// ================================

/// System-level errors that can occur during action execution
#[derive(Debug)]
pub enum ActionError {
    NetworkError(String),
    QueueFull(String),
    Timeout(String),
    ConfigError(String),
    AuthError(String),
    ResourceError(String),
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
        }
    }
}

impl std::error::Error for ActionError {}

// ================================
//   Actions
// ================================

pub enum Action {
    Check(Box<dyn CheckAction>),
    Via(Box<dyn ViaAction>),
    Router(Box<dyn HopAction>),
}

// ================================
//   Check Actions
// ================================

/// Actions which check conditions on a job, typically checking the job data itself or the job's metadata
#[async_trait]
pub trait CheckAction: Send + Sync {
    async fn evaluate(&self, job: &Job) -> Result<CheckResult, ActionError>;
}

/// Result of a check action
#[derive(Debug)]
pub enum CheckResult {
    Pass,
    Reject { reason: String },
}

// ================================
//   Via Actions
// ================================

/// Actions that transform jobs, typically by adding or changing metadata
#[async_trait]
pub trait ViaAction: Send + Sync {
    async fn execute(&self, job: &mut Job) -> Result<(), ActionError>;
}

// ================================
//    Hop Actions
// ================================

/// Actions that direct jobs to another route segment or destination
#[async_trait]
pub trait HopAction: Send + Sync {
    async fn hop(&self, job: &mut Job) -> Result<HopResult, ActionError>;
}

pub enum HopResult {
    Complete(JobResult),
    Reject { reason: String },
}
