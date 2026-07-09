// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::{Arc, OnceLock};

use crate::actions::{Action, ActionError};
use dashmap::DashMap;
use serde_json::Value;

/// Type alias for compile-time action factory functions (used by the `inventory` mechanism).
pub type ActionFactory = fn(Value) -> Result<Action, ActionError>;

/// Type alias for runtime action factory functions (used by `register_runtime_action`).
/// Boxed + Send + Sync so they can be stored in a global map and called from any thread.
pub type RuntimeActionFactory = Arc<dyn Fn(Value) -> Result<Action, ActionError> + Send + Sync>;

/// Action registration struct for inventory (compile-time).
pub struct ActionRegistration {
    pub name: &'static str,
    pub factory: ActionFactory,
}

inventory::collect!(ActionRegistration);

/// Global runtime registry — populated at startup before `Bits::from_config` is called.
fn runtime_registry() -> &'static DashMap<String, RuntimeActionFactory> {
    static REGISTRY: OnceLock<DashMap<String, RuntimeActionFactory>> = OnceLock::new();
    REGISTRY.get_or_init(DashMap::new)
}

/// Register an action factory at runtime, under the given name.
///
/// Returns an error if the name conflicts with an existing builtin or runtime action.
///
/// Must be called before `Bits::from_config`.
pub fn register_runtime_action(
    name: &str,
    factory: RuntimeActionFactory,
) -> Result<(), ActionError> {
    for reg in inventory::iter::<ActionRegistration> {
        if reg.name == name {
            return Err(ActionError::ConfigError(format!(
                "Cannot register '{}': conflicts with a built-in action",
                name
            )));
        }
    }

    let registry = runtime_registry();
    if registry.contains_key(name) {
        return Err(ActionError::ConfigError(format!(
            "Cannot register '{}': already registered as a runtime action",
            name
        )));
    }

    registry.insert(name.to_string(), factory);
    Ok(())
}

/// Create an action by name. Runtime registrations are checked first, then compile-time inventory.
pub fn create_action(name: &str, config: Value) -> Result<Action, ActionError> {
    if let Some(factory) = runtime_registry().get(name) {
        return factory(config);
    }

    for reg in inventory::iter::<ActionRegistration> {
        if reg.name == name {
            return (reg.factory)(config);
        }
    }

    let available = list_actions();
    Err(ActionError::ConfigError(format!(
        "Unknown action: '{}'. Available actions: [{}]",
        name,
        available.join(", "),
    )))
}

/// List all registered action names (both compile-time and runtime).
pub fn list_actions() -> Vec<String> {
    let mut names: Vec<String> = inventory::iter::<ActionRegistration>()
        .map(|reg| reg.name.to_string())
        .collect();
    for entry in runtime_registry().iter() {
        names.push(entry.key().clone());
    }
    names
}

/// Register an action with the global registry.
/// Usage:
///   register_action!(check,     "match",             Match);
///   register_action!(transform, "metkit_expansion",  MetkitExpansion);
///   register_action!(target,    "mars_destination",  MarsDestination);
#[macro_export]
macro_rules! register_action {
    (check, $name:expr_2021, $action_type:ty) => {
        inventory::submit! {
            $crate::actions::registry::ActionRegistration {
                name: $name,
                factory: |config| {
                    let action: $action_type = serde_json::from_value(config)
                        .map_err(|e| $crate::actions::ActionError::ConfigError(e.to_string()))?;
                    Ok($crate::actions::Action::Check(std::sync::Arc::new(action), None, None))
                }
            }
        }
    };
    (transform, $name:expr_2021, $action_type:ty) => {
        inventory::submit! {
            $crate::actions::registry::ActionRegistration {
                name: $name,
                factory: |config| {
                    let action: $action_type = serde_json::from_value(config)
                        .map_err(|e| $crate::actions::ActionError::ConfigError(e.to_string()))?;
                    Ok($crate::actions::Action::Transform(std::sync::Arc::new(action), None, None))
                }
            }
        }
    };
    (target, $name:expr_2021, $action_type:ty) => {
        inventory::submit! {
            $crate::actions::registry::ActionRegistration {
                name: $name,
                factory: |config| {
                    let action: $action_type = serde_json::from_value(config)
                        .map_err(|e| $crate::actions::ActionError::ConfigError(e.to_string()))?;
                    Ok($crate::actions::Action::Target(std::sync::Arc::new(action), None, None))
                }
            }
        }
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_builtin_actions_registered() {
        let actions = list_actions();
        assert!(actions.contains(&"http".to_string()));
        assert!(actions.contains(&"remote".to_string()));
    }

    #[test]
    fn test_create_http_action() {
        let config = json!({"url": "http://localhost:8080"});
        let action = create_action("http", config).unwrap();
        assert!(matches!(action, Action::Target(..)));
    }

    #[test]
    fn test_unknown_action_errors_with_available_list() {
        let result = create_action("nonexistent", json!({}));
        match &result {
            Err(ActionError::ConfigError(msg)) => {
                assert!(msg.contains("Unknown action: 'nonexistent'"), "{msg}");
                assert!(msg.contains("Available actions:"), "{msg}");
                assert!(msg.contains("http"), "should list built-in actions: {msg}");
            }
            other => panic!("expected ConfigError, got {other:?}"),
        }
    }

    #[test]
    fn test_runtime_registration_and_lookup() {
        use crate::actions::{ActionError as AE, CheckAction, CheckResult};
        use async_trait::async_trait;

        #[derive(Debug)]
        struct AlwaysPass;
        #[async_trait]
        impl CheckAction for AlwaysPass {
            async fn evaluate(&self, _job: &crate::job::Job) -> Result<CheckResult, AE> {
                Ok(CheckResult::Pass)
            }
        }

        let factory: RuntimeActionFactory =
            Arc::new(|_config| Ok(Action::Check(Arc::new(AlwaysPass), None, None)));

        let name = "test_runtime_always_pass_9f3a";
        register_runtime_action(name, factory).expect("registration should succeed");

        let action = create_action(name, json!({})).expect("should find runtime action");
        assert!(matches!(action, Action::Check(..)));

        assert!(list_actions().contains(&name.to_string()));
    }

    #[test]
    fn test_runtime_duplicate_registration_fails() {
        let factory: RuntimeActionFactory =
            Arc::new(|_| Err(ActionError::ConfigError("unused".into())));

        let name = "test_runtime_dup_8b2c";
        register_runtime_action(name, Arc::clone(&factory))
            .expect("first registration should succeed");
        let err =
            register_runtime_action(name, factory).expect_err("second registration should fail");
        assert!(matches!(err, ActionError::ConfigError(_)));
    }

    #[test]
    fn test_runtime_builtin_conflict_fails() {
        let factory: RuntimeActionFactory =
            Arc::new(|_| Err(ActionError::ConfigError("unused".into())));

        let err = register_runtime_action("http", factory)
            .expect_err("should fail: conflicts with builtin");
        assert!(matches!(err, ActionError::ConfigError(_)));
    }
}
