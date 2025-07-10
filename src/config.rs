// use serde::{Deserialize, Serialize};
// use serde_json::Value;
// use std::collections::HashMap;

// /// Main BITS configuration structure.
// #[derive(Debug, Deserialize, Serialize, Clone)]
// pub struct BitsConfig {
//     pub queues: HashMap<String, QueueConfig>,
//     pub routes: HashMap<String, Vec<Action>>,
// }

// impl Default for BitsConfig {
//     fn default() -> Self {
//         Self {
//             queues: HashMap::new(),
//             routes: HashMap::new(),
//         }
//     }
// }

// /// Configuration for a queue.
// #[derive(Debug, Deserialize, Serialize, Clone)]
// pub struct QueueConfig {
//     #[serde(rename = "type")]
//     pub queue_type: String, // "fifo", "priority", "fair"
//     pub capacity: usize,
//     pub workers: Option<usize>, // None = external workers
// }

// /// Individual action in a route.
// #[derive(Debug, Deserialize, Serialize, Clone)]
// #[serde(tag = "type", rename_all = "lowercase")]
// pub enum Action {
//     Filter { 
//         #[serde(flatten)]
//         condition: Value 
//     },
//     Check { 
//         rule: String 
//     },
//     Via { 
//         queue: String 
//     },
//     Switch {
//         routes: HashMap<String, Vec<Action>>
//     },
//     Destination {
//         #[serde(flatten)]
//         target: DestinationTarget
//     },
// }

// /// Destination can be a queue name or HTTP endpoint.
// #[derive(Debug, Deserialize, Serialize, Clone)]
// #[serde(untagged)]
// pub enum DestinationTarget {
//     Queue { queue: String },
//     Http {
//         url: String,
//         method: Option<String>,
//         headers: Option<HashMap<String, String>>,
//         timeout: Option<String>,
//     },
// }

// impl BitsConfig {
//     /// Load configuration from YAML string.
//     pub fn from_yaml(yaml: &str) -> Result<Self, serde_yaml::Error> {
//         serde_yaml::from_str(yaml)
//     }

//     /// Load configuration from YAML file.
//     pub fn from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
//         let content = std::fs::read_to_string(path)?;
//         Ok(Self::from_yaml(&content)?)
//     }

//     /// Convert configuration to YAML string.
//     pub fn to_yaml(&self) -> Result<String, serde_yaml::Error> {
//         serde_yaml::to_string(self)
//     }
// } 