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
    // Check against compile-time builtins first.
    for reg in inventory::iter::<ActionRegistration> {
        if reg.name == name {
            return Err(ActionError::ConfigError(format!(
                "Cannot register '{}': conflicts with a built-in action",
                name
            )));
        }
    }
    // Check for duplicate runtime registrations.
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

/// Create an action by name.  Runtime registrations are checked first (fast path),
/// then compile-time inventory.  Name clashes with builtins are prevented at
/// registration time, so there is no ambiguity.
pub fn create_action(name: &str, config: Value) -> Result<Action, ActionError> {
    // Check runtime registry first.
    if let Some(factory) = runtime_registry().get(name) {
        return factory(config);
    }
    // Fall back to compile-time inventory.
    for reg in inventory::iter::<ActionRegistration> {
        if reg.name == name {
            return (reg.factory)(config);
        }
    }
    Err(ActionError::ConfigError(format!("Unknown action: '{}'", name)))
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

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_builtin_actions_registered() {
        let actions = list_actions();
        assert!(actions.contains(&"has_role".to_string()));
        assert!(actions.contains(&"http".to_string()));
    }

    #[test]
    fn test_create_has_role_action() {
        let config = json!({"role": "admin"});
        let action = create_action("has_role", config).unwrap();
        assert!(matches!(action, Action::Check(..)));
    }

    #[test]
    fn test_unknown_action_errors() {
        let result = create_action("nonexistent", json!({}));
        assert!(matches!(result, Err(ActionError::ConfigError(_))));
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

        let factory: RuntimeActionFactory = Arc::new(|_config| {
            Ok(Action::Check(Arc::new(AlwaysPass), None))
        });

        // Use a unique name so parallel tests don't clash on the global registry.
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
        let err = register_runtime_action(name, factory)
            .expect_err("second registration should fail");
        assert!(matches!(err, ActionError::ConfigError(_)));
    }

    #[test]
    fn test_runtime_builtin_conflict_fails() {
        let factory: RuntimeActionFactory =
            Arc::new(|_| Err(ActionError::ConfigError("unused".into())));

        let err = register_runtime_action("has_role", factory)
            .expect_err("should fail: conflicts with builtin");
        assert!(matches!(err, ActionError::ConfigError(_)));
    }
}
