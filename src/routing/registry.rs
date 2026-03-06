use crate::actions::{Action, ActionError};
use serde_json::Value;

/// Type alias for action factory functions.
pub type ActionFactory = fn(Value) -> Result<Action, ActionError>;

/// Action registration struct for inventory.
pub struct ActionRegistration {
    pub name: &'static str,
    pub factory: ActionFactory,
}

inventory::collect!(ActionRegistration);

/// Create an action by name.
pub fn create_action(name: &str, config: Value) -> Result<Action, ActionError> {
    for reg in inventory::iter::<ActionRegistration> {
        if reg.name == name {
            return (reg.factory)(config);
        }
    }
    Err(ActionError::ConfigError(format!("Unknown action: {}", name)))
}

/// List all registered action names.
pub fn list_actions() -> Vec<String> {
    inventory::iter::<ActionRegistration>()
        .map(|reg| reg.name.to_string())
        .collect()
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
        assert!(matches!(action, Action::Check(_)));
    }

    #[test]
    fn test_unknown_action_errors() {
        let result = create_action("nonexistent", json!({}));
        assert!(matches!(result, Err(ActionError::ConfigError(_))));
    }
} 