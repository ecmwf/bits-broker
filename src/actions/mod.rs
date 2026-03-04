use crate::job::Job;
use crate::result::JobResult;
use async_trait::async_trait;

pub mod check_hasrole;
pub mod target_http;

pub use check_hasrole::*;
pub use target_http::*;

pub use crate::routing::registry::{create_action, list_actions};

// ================================
//   Registration Macro
// ================================

/// Register an action with the global registry.
/// Usage:
///   register_action!(check,     "match",             Match);
///   register_action!(transform, "metkit_expansion",  MetkitExpansion);
///   register_action!(target,    "mars_destination",  MarsDestination);
#[macro_export]
macro_rules! register_action {
    (check, $name:expr, $action_type:ty) => {
        inventory::submit! {
            $crate::routing::registry::ActionRegistration {
                name: $name,
                factory: |config| {
                    let action: $action_type = serde_json::from_value(config)
                        .map_err(|e| $crate::actions::ActionError::ConfigError(e.to_string()))?;
                    Ok($crate::actions::Action::Check(Box::new(action)))
                }
            }
        }
    };
    (transform, $name:expr, $action_type:ty) => {
        inventory::submit! {
            $crate::routing::registry::ActionRegistration {
                name: $name,
                factory: |config| {
                    let action: $action_type = serde_json::from_value(config)
                        .map_err(|e| $crate::actions::ActionError::ConfigError(e.to_string()))?;
                    Ok($crate::actions::Action::Transform(Box::new(action)))
                }
            }
        }
    };
    (target, $name:expr, $action_type:ty) => {
        inventory::submit! {
            $crate::routing::registry::ActionRegistration {
                name: $name,
                factory: |config| {
                    let action: $action_type = serde_json::from_value(config)
                        .map_err(|e| $crate::actions::ActionError::ConfigError(e.to_string()))?;
                    Ok($crate::actions::Action::Target(Box::new(action)))
                }
            }
        }
    };
}

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
    Transform(Box<dyn TransformAction>),
    Target(Box<dyn TargetAction>),
    Switch(crate::routing::switch::Switch),
    /// Mark the job as persistent from this point forward.
    Persist,
    /// Bound concurrency and add backpressure for the wrapped action.
    Queue(crate::queue::Queue),
}

impl std::fmt::Debug for Action {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Action::Check(_) => write!(f, "Action::Check(..)"),
            Action::Transform(_) => write!(f, "Action::Transform(..)"),
            Action::Target(_) => write!(f, "Action::Target(..)"),
            Action::Switch(_) => write!(f, "Action::Switch(..)"),
            Action::Persist => write!(f, "Action::Persist"),
            Action::Queue(q) => {
                write!(f, "Action::Queue(capacity={}, workers={:?})", q.capacity, q.workers)
            }
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
