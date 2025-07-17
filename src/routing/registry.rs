use crate::actions::{Action, ActionError};
use serde_json::Value;

/// Type alias for action factory functions
pub type ActionFactory = fn(Value) -> Result<Action, ActionError>;

/// Action registration struct for inventory
pub struct ActionRegistration {
    pub name: &'static str,
    pub factory: ActionFactory,
}

/// Collect all registered actions using inventory
inventory::collect!(ActionRegistration);

/// Create an action from a name and config
pub fn create_action(name: &str, config: Value) -> Result<Action, ActionError> {
    for registration in inventory::iter::<ActionRegistration> {
        if registration.name == name {
            return (registration.factory)(config);
        }
    }
    Err(ActionError::ConfigError(format!("Unknown action: {}", name)))
}

/// Get all registered action names
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
    fn test_action_registry() {
        let actions = list_actions();
        assert!(actions.contains(&"match".to_string()));
        assert!(actions.contains(&"mars_destination".to_string()));
        
        // Test creating an action
        let config = json!({"class": "od"});
        let action = create_action("match", config).unwrap();
        
        match action {
            Action::Check(_) => {}, // Expected
            _ => panic!("Expected Check action"),
        }
    }
} 