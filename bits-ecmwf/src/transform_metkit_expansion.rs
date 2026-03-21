use async_trait::async_trait;
use bits::Job;
use bits::actions::{ActionError, TransformAction, TransformResult};
use serde::{Deserialize, Serialize};

/// Expand MARS request parameters using Metkit conventions.
#[derive(Debug, Serialize, Deserialize)]
pub struct MetkitExpansion {
    pub expand_parameters: bool,
}

#[async_trait]
impl TransformAction for MetkitExpansion {
    async fn execute(&self, job: &mut Job) -> Result<TransformResult, ActionError> {
        if self.expand_parameters {
            let mut map = job
                .metadata
                .as_object()
                .unwrap_or(&serde_json::Map::new())
                .clone();
            map.insert("metkit_expanded".to_string(), serde_json::json!(true));
            job.metadata = std::sync::Arc::new(serde_json::Value::Object(map));
        }
        Ok(TransformResult::Continue)
    }
}

bits::register_action!(transform, "metkit_expansion", MetkitExpansion);
