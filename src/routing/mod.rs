pub mod registry;
pub mod switch;

use crate::actions::Action;
use serde::{Deserialize, Deserializer};
use serde_json::Value;

#[derive(Debug)]
pub struct Route {
    pub name: String,
    pub actions: Vec<Action>,
    pub config: Value,
}

impl<'de> Deserialize<'de> for Route {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let config = Value::deserialize(deserializer)?;
        let actions = if let Some(action_list) = config.as_array() {
            action_list.iter()
                .map(|action_config| Action::deserialize(action_config.clone()))
                .collect::<Result<Vec<_>, _>>()
                .map_err(serde::de::Error::custom)?
        } else {
            return Err(serde::de::Error::custom("Route config must be an array of actions"));
        };
        
        Ok(Route {
            name: String::new(),
            actions,
            config,
        })
    }
}

impl Route {
    pub fn new(name: String, actions: Vec<Action>) -> Self {
        Self { 
            name, 
            actions,
            config: Value::Null,
        }
    }
    
    pub fn set_name(&mut self, name: String) {
        self.name = name;
    }
}
