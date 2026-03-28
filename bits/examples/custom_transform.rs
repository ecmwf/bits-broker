//! Demonstrates creating and registering a custom transform action.
//!
//!     cargo run --example custom_transform

use async_trait::async_trait;
use bits::{
    ActionError, Job, TransformAction, TransformResult, create_action, list_actions,
    register_action,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Serialize, Deserialize)]
pub struct AddField {
    pub key: String,
    pub value: String,
}

#[async_trait]
impl TransformAction for AddField {
    async fn execute(&self, job: &mut Job) -> Result<TransformResult, ActionError> {
        let Some(map) = job.request.as_object_mut() else {
            return Ok(TransformResult::Reject {
                reason: "request must be a JSON object".to_string(),
                silent: true,
            });
        };

        map.insert(
            self.key.clone(),
            serde_json::Value::String(self.value.clone()),
        );
        Ok(TransformResult::Continue)
    }
}

register_action!(transform, "custom_add_field", AddField);

fn main() -> Result<(), bits::ActionError> {
    let action = create_action(
        "custom_add_field",
        json!({ "key": "source", "value": "custom_transform" }),
    )?;
    println!("created: {:?}", action);

    let names = list_actions();
    println!(
        "registered: {}",
        names.iter().any(|n| n == "custom_add_field")
    );

    Ok(())
}
