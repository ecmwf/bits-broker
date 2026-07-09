// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

//! Demonstrates creating and registering a custom target action.
//!
//!     cargo run --example custom_target

use async_trait::async_trait;
use bits::{
    ActionError, Job, TargetAction, TargetResult, create_action, list_actions, register_action,
};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Serialize, Deserialize)]
pub struct Echo {
    pub label: String,
}

#[async_trait]
impl TargetAction for Echo {
    async fn dispatch(&self, job: &Job) -> Result<TargetResult, ActionError> {
        let body = json!({ "route": self.label, "request": job.request });
        let bytes = bytes::Bytes::from(body.to_string().into_bytes());
        let size = bytes.len() as i64;
        let stream = Box::new(futures::stream::iter(vec![Ok(bytes)]));
        Ok(TargetResult::Complete(bits::JobResult::Success {
            content_type: "application/json".to_string(),
            size,
            stream,
        }))
    }
}

register_action!(target, "custom_echo_target", Echo);

fn main() -> Result<(), bits::ActionError> {
    let action = create_action("custom_echo_target", json!({ "label": "main" }))?;
    println!("created: {:?}", action);

    let names = list_actions();
    println!(
        "registered: {}",
        names.iter().any(|n| n == "custom_echo_target")
    );

    Ok(())
}
