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
    CircuitOpen(String),
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
            ActionError::CircuitOpen(msg) => write!(f, "Circuit open: {}", msg),
            ActionError::Cancelled => write!(f, "Cancelled"),
            ActionError::ClientGone => {
                write!(f, "Client disconnected before data could be delivered")
            }
        }
    }
}

impl std::error::Error for ActionError {}

impl ActionError {
    pub fn code(&self) -> &'static str {
        match self {
            ActionError::NetworkError(_) => "ACTION_NETWORK",
            ActionError::QueueFull(_) => "ACTION_QUEUE_FULL",
            ActionError::Timeout(_) => "ACTION_TIMEOUT",
            ActionError::ConfigError(_) => "ACTION_CONFIG",
            ActionError::AuthError(_) => "ACTION_AUTH",
            ActionError::ResourceError(_) => "ACTION_RESOURCE",
            ActionError::CircuitOpen(_) => "ACTION_CIRCUIT_OPEN",
            ActionError::Cancelled => "ACTION_CANCELLED",
            ActionError::ClientGone => "ACTION_CLIENT_GONE",
        }
    }

    pub fn is_retryable(&self) -> bool {
        matches!(
            self,
            ActionError::NetworkError(_)
                | ActionError::Timeout(_)
                | ActionError::QueueFull(_)
                | ActionError::CircuitOpen(_)
        )
    }
}

// ================================
//   Actions
// ================================

/// A configured pipeline step.
pub enum Action {
    Check(
        Arc<dyn CheckAction>,
        Option<crate::dispatcher::Dispatcher<CheckResult>>,
        Option<bool>,
    ),
    Transform(
        Arc<dyn TransformAction>,
        Option<crate::dispatcher::Dispatcher<TransformResult>>,
        Option<bool>,
    ),
    Target(
        Arc<dyn TargetAction>,
        Option<crate::dispatcher::Dispatcher<TargetResult>>,
        Option<bool>,
        Option<std::sync::Arc<crate::circuit_breaker::CircuitBreaker>>,
    ),
    Switch(crate::routing::switch::Switch),
}

impl Action {
    pub(crate) fn close(&self) {
        match self {
            Action::Check(_, Some(d), _) => d.close(),
            Action::Transform(_, Some(d), _) => d.close(),
            Action::Target(_, Some(d), _, _) => d.close(),
            Action::Switch(switch) => switch.close_all(),
            _ => {}
        }
    }

    /// Returns true when this action ends a route.
    pub fn is_terminal(&self) -> bool {
        matches!(self, Action::Target(..) | Action::Switch(..))
    }

    /// Config-level override for whether this action's rejections are silent.
    pub fn silent_override(&self) -> Option<bool> {
        match self {
            Action::Check(_, _, o) | Action::Transform(_, _, o) | Action::Target(_, _, o, _) => *o,
            Action::Switch(_) => None,
        }
    }

    /// Returns the [`describe`](CheckAction::describe) output for the inner action.
    ///
    /// For `Switch` variants this returns the default empty object; use
    /// [`Switch::describe_actions`](crate::routing::switch::Switch::describe_actions)
    /// to recurse into nested switches.
    pub fn describe(&self) -> serde_json::Value {
        match self {
            Action::Check(a, _, _) => a.describe(),
            Action::Transform(a, _, _) => a.describe(),
            Action::Target(a, _, _, _) => a.describe(),
            Action::Switch(_) => serde_json::json!({}),
        }
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

    /// Machine-readable descriptor for introspection.
    ///
    /// Override this to expose action-specific metadata (e.g. collection names,
    /// configuration values) that callers can query at runtime.  The default
    /// implementation returns an empty JSON object.
    fn describe(&self) -> serde_json::Value {
        serde_json::json!({})
    }
}

#[derive(Debug)]
/// Result of evaluating a check action.
pub enum CheckResult {
    /// The job may continue through the route.
    Pass,
    /// The route is rejected with a human-readable reason.
    ///
    /// When `silent` is false and the entire switch fails, this rejection
    /// reason is included in the error returned to the user.  Route-selection
    /// checks (e.g. `Match`) normally set this to `true`; validation checks
    /// (e.g. `ScheduleReleased`) set it to `false`.
    Reject { reason: String, silent: bool },
}

// ================================
//   Transform Actions
// ================================

/// Transformations that mutate the job and continue the pipeline.
#[async_trait]
pub trait TransformAction: Send + Sync {
    async fn execute(&self, job: &mut Job) -> Result<TransformResult, ActionError>;

    /// See [`CheckAction::describe`].
    fn describe(&self) -> serde_json::Value {
        serde_json::json!({})
    }
}

#[derive(Debug)]
/// Result of running a transform action.
pub enum TransformResult {
    /// The job was updated and may continue through the route.
    Continue,
    /// The route is rejected with a human-readable reason.
    ///
    /// See [`CheckResult::Reject::silent`] for semantics.
    Reject { reason: String, silent: bool },
}

// ================================
//   Target Actions
// ================================

/// Terminal dispatch — sends the job to a destination and returns a result.
#[async_trait]
pub trait TargetAction: Send + Sync {
    async fn dispatch(&self, job: &Job) -> Result<TargetResult, ActionError>;

    /// See [`CheckAction::describe`].
    fn describe(&self) -> serde_json::Value {
        serde_json::json!({})
    }
}

#[derive(Debug)]
/// Result of dispatching a job to a terminal target.
pub enum TargetResult {
    /// The target produced a final job result.
    Complete(JobResult),
    /// The target rejected the job without a system failure.
    ///
    /// See [`CheckResult::Reject::silent`] for semantics.
    Reject { reason: String, silent: bool },
}
