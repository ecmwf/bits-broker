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
            let meta = job.metadata_mut();
            if !meta.is_object() {
                *meta = serde_json::json!({});
            }
            meta["metkit_expanded"] = serde_json::json!(true);
        }
        Ok(TransformResult::Continue)
    }
}

bits::register_action!(transform, "metkit_expansion", MetkitExpansion);
