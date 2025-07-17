use crate::actions::{ActionError, ViaAction, ViaResult};
use crate::job::Job;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

// ================================
//   MetkitExpansion Action
// ================================

/// Expand metkit request parameters
#[derive(Debug, Serialize, Deserialize)]
pub struct MetkitExpansion {
    pub expand_parameters: bool,
}

#[async_trait]
impl ViaAction for MetkitExpansion {
    async fn execute(&self, job: &mut Job) -> Result<ViaResult, ActionError> {
        if self.expand_parameters {
            // Add metkit expansion metadata
            let mut metadata = job.metadata.as_object().unwrap_or(&serde_json::Map::new()).clone();
            metadata.insert("metkit_expanded".to_string(), serde_json::json!(true));
            job.metadata = serde_json::Value::Object(metadata);
        }
        Ok(ViaResult::Continue)
    }
}

// Register the MetkitExpansion action
crate::register_action!(via, "metkit_expansion", MetkitExpansion); 