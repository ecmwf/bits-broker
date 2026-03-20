//! Demonstrates creating and registering a custom check action.
//!
//!     cargo run --example custom_check

use async_trait::async_trait;
use bits::{
    ActionError, CheckAction, CheckResult, Job, create_action, list_actions, register_action,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Serialize, Deserialize)]
pub struct MatchField {
    pub field: String,
    pub value: String,
}

#[async_trait]
impl CheckAction for MatchField {
    async fn evaluate(&self, job: &Job) -> Result<CheckResult, ActionError> {
        match job.request.get(&self.field).and_then(|v| v.as_str()) {
            Some(v) if v == self.value => Ok(CheckResult::Pass),
            _ => Ok(CheckResult::Reject {
                reason: format!("{}={} not matched", self.field, self.value),
                silent: true,
            }),
        }
    }
}

register_action!(check, "custom_match_field", MatchField);

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let action = create_action(
        "custom_match_field",
        json!({ "field": "type", "value": "fc" }),
    )?;
    println!("created: {:?}", action);

    let names = list_actions();
    println!(
        "registered: {}",
        names.iter().any(|n| n == "custom_match_field")
    );

    Ok(())
}
