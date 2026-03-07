//! Demonstrates custom actions and job routing.
//!
//!     cargo run --example routing

use async_trait::async_trait;
use bits::*;
use serde::{Deserialize, Serialize};
use serde_json::json;

// ── Custom check: requires a specific field value ─────────────────────────────

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
            }),
        }
    }
}

register_action!(check, "match_field", MatchField);

// ── Custom target: echoes the request back as JSON ────────────────────────────

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
        Ok(TargetResult::Complete(JobResult::Success {
            content_type: "application/json".to_string(),
            size,
            stream,
        }))
    }
}

register_action!(target, "echo", Echo);

// ── Config ────────────────────────────────────────────────────────────────────

const CONFIG: &str = r#"
routes:
  forecast:
    - check::match_field:
        field: type
        value: fc
    - target::echo:
        label: forecast
  analysis:
    - check::match_field:
        field: type
        value: an
    - target::echo:
        label: analysis
"#;

// ── Main ──────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let bits = Bits::from_config(CONFIG)?;

    let jobs = vec![
        json!({ "type": "fc", "date": "2024-01-15" }),
        json!({ "type": "an", "date": "2024-01-15" }),
        json!({ "type": "unknown" }),
    ];

    for request in jobs {
        print!("{} -> ", request);
        match bits.process(Job::new(request)).await {
            JobResult::Success { content_type, size, .. } => {
                println!("{} ({} bytes)", content_type, size);
            }
            JobResult::Redirect { location, .. } => {
                println!("redirect: {}", location);
            }
            JobResult::Error { message } => {
                println!("rejected: {}", message);
            }
            JobResult::Failed { reason } => {
                println!("failed: {}", reason);
            }
        }
    }

    Ok(())
}