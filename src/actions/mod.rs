use crate::job::Job;
use crate::result::JobResult;
use async_trait::async_trait;

pub mod check;
pub mod via;
pub mod route;

pub use check::*;
pub use via::*;
pub use route::*;

pub use crate::routing::registry::{create_action, list_actions};

// ================================
//   Registration Macro
// ================================

/// Register an action with the global registry.
/// Usage:
///   register_action!(check, "match", Match);
///   register_action!(via,   "metkit_expansion", MetkitExpansion);
///   register_action!(route, "mars_destination", MarsDestination);
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
    (via, $name:expr, $action_type:ty) => {
        inventory::submit! {
            $crate::routing::registry::ActionRegistration {
                name: $name,
                factory: |config| {
                    let action: $action_type = serde_json::from_value(config)
                        .map_err(|e| $crate::actions::ActionError::ConfigError(e.to_string()))?;
                    Ok($crate::actions::Action::Via(Box::new(action)))
                }
            }
        }
    };
    (route, $name:expr, $action_type:ty) => {
        inventory::submit! {
            $crate::routing::registry::ActionRegistration {
                name: $name,
                factory: |config| {
                    let action: $action_type = serde_json::from_value(config)
                        .map_err(|e| $crate::actions::ActionError::ConfigError(e.to_string()))?;
                    Ok($crate::actions::Action::Router(Box::new(action)))
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
    Via(Box<dyn ViaAction>),
    Router(Box<dyn RouteAction>),
    Switch(crate::routing::switch::Switch),
    /// Mark the job as persistent from this point forward.
    Persist,
    /// Bound concurrency and add backpressure for the wrapped action.
    Queue {
        capacity: usize,
        workers: Option<usize>,
        action: Box<Action>,
    },
}

impl std::fmt::Debug for Action {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Action::Check(_) => write!(f, "Action::Check(..)"),
            Action::Via(_) => write!(f, "Action::Via(..)"),
            Action::Router(_) => write!(f, "Action::Router(..)"),
            Action::Switch(_) => write!(f, "Action::Switch(..)"),
            Action::Persist => write!(f, "Action::Persist"),
            Action::Queue { capacity, workers, .. } => {
                write!(f, "Action::Queue(capacity={}, workers={:?})", capacity, workers)
            }
        }
    }
}

// ================================
//   Check Actions
// ================================

/// Guard conditions on a job. A rejection stops the current route branch.
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
//   Via Actions
// ================================

/// Transformations that mutate the job and continue the pipeline.
#[async_trait]
pub trait ViaAction: Send + Sync {
    async fn execute(&self, job: &mut Job) -> Result<ViaResult, ActionError>;
}

#[derive(Debug)]
pub enum ViaResult {
    Continue,
    Reject { reason: String },
}

// ================================
//   Route Actions
// ================================

/// Terminal dispatch — sends the job somewhere and returns a result.
#[async_trait]
pub trait RouteAction: Send + Sync {
    async fn route(&self, job: &Job) -> Result<RouteResult, ActionError>;
}

#[derive(Debug)]
pub enum RouteResult {
    Complete(JobResult),
    Reject { reason: String },
}
