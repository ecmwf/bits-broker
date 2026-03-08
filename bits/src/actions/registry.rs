// Re-export from routing for convenience
pub use crate::routing::registry::{create_action, list_actions, ActionRegistration, ActionFactory};

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
                    Ok($crate::actions::Action::Check(std::sync::Arc::new(action), None))
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
                    Ok($crate::actions::Action::Transform(std::sync::Arc::new(action), None))
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
                    Ok($crate::actions::Action::Target(std::sync::Arc::new(action), None))
                }
            }
        }
    };
}

