use crate::job::Job;
use crate::result::JobResult;
use crate::queue::queued_action::{FIFOQueue, QueueConfig};
use async_trait::async_trait;
use serde::{Deserialize, Deserializer};
use serde_json::Value;

// Re-export concrete action implementations
pub mod check;
pub mod via;
pub mod route;

// Re-export all the action types for convenience
pub use check::*;
pub use via::*;
pub use route::*;

// Re-export registry functions for convenience
pub use crate::routing::registry::{create_action, list_actions};

// ================================
//   Registration Macro
// ================================

/// Unified macro for registering actions 
/// Usage: register_action!(check, "match", Match);
///        register_action!(via, "metkit_expansion", MetkitExpansion);
///        register_action!(route, "mars_destination", MarsDestination);
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
    Router(Box<dyn RouteAction>),
    Switch(crate::routing::switch::Switch),
}

impl std::fmt::Debug for Action {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Action::Check(_) => write!(f, "Action::Check(..)"),
            Action::Via(_) => write!(f, "Action::Via(..)"),
            Action::Router(_) => write!(f, "Action::Router(..)"),
            Action::Switch(_) => write!(f, "Action::Switch(..)"),
        }
    }
}

impl<'de> Deserialize<'de> for Action {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Value::deserialize(deserializer)?;
        
        if let Some(obj) = value.as_object() {
            // Handle switch case
            if let Some(switch_config) = obj.get("switch") {
                let switch = crate::routing::switch::Switch::deserialize(switch_config.clone())
                    .map_err(serde::de::Error::custom)?;
                return Ok(Action::Switch(switch));
            }
            
            // Handle action_type::action_name format
            for (key, config) in obj {
                if key.contains("::") {
                    let parts: Vec<&str> = key.split("::").collect();
                    if parts.len() == 2 {
                        let action_name = parts[1];
                        
                        // Check if this action has a queue configuration
                        if let Some(config_obj) = config.as_object() {
                            if let Some(queue_config) = config_obj.get("queue") {
                                // Parse the queue configuration
                                let queue_config: QueueConfig = serde_json::from_value(queue_config.clone())
                                    .map_err(serde::de::Error::custom)?;
                                
                                // Create a new config without the queue field for the underlying action
                                let mut action_config = config_obj.clone();
                                action_config.remove("queue");
                                let action_config = Value::Object(action_config);
                                
                                // Create a QueuedAction with the name and config
                                let queued_action = FIFOQueue::new(action_name.to_string(), action_config, queue_config);
                                
                                // Return the appropriate action type based on the original action
                                match parts[0] {
                                    "check" => return Ok(Action::Check(Box::new(queued_action))),
                                    "via" => return Ok(Action::Via(Box::new(queued_action))),
                                    "route" => return Ok(Action::Router(Box::new(queued_action))),
                                    _ => return Err(serde::de::Error::custom("Unknown action type")),
                                }
                            }
                        }
                        
                        // No queue configuration, create action normally
                        let action = crate::routing::registry::create_action(action_name, config.clone())
                            .map_err(serde::de::Error::custom)?;
                        return Ok(action);
                    }
                }
            }
        }
        
        Err(serde::de::Error::custom("Invalid action format"))
    }
}


// ================================
//   Check Actions
// ================================

/// Actions which check conditions on a job, typically checking the job data itself or the job's metadata
#[async_trait]
pub trait CheckAction: Send + Sync {
    async fn evaluate(&mut self, job: &Job) -> Result<CheckResult, ActionError>;
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
    async fn execute(&mut self, job: &mut Job) -> Result<ViaResult, ActionError>;
}

#[derive(Debug)]
pub enum ViaResult {
    Continue,
    Reject { reason: String },
}

// ================================
//    Route Actions
// ================================

/// Actions that direct jobs to another route segment or destination
#[async_trait]
pub trait RouteAction: Send + Sync {
    async fn route(&mut self, job: &Job) -> Result<RouteResult, ActionError>;
}

#[derive(Debug)]
pub enum RouteResult {
    Complete(JobResult),
    Reject { reason: String },
} 