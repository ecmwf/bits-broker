use crate::actions::{TargetAction, TargetResult};
use crate::config::parse_config;
use crate::job::Job;
use crate::result::JobResult;
use crate::routing::switch::Switch;

pub struct Bits {
    router: Switch,
}

impl Bits {
    pub fn from_config(config: &str) -> Result<Self, Box<dyn std::error::Error>> {
        Ok(Bits { router: parse_config(config)? })
    }

    pub async fn process(&self, job: Job) -> JobResult {
        match self.router.dispatch(&job).await {
            Ok(TargetResult::Complete(result)) => result,
            Ok(TargetResult::Reject { reason }) => JobResult::Error {
                message: format!("All pipelines rejected: {}", reason),
            },
            Err(err) => JobResult::Failed {
                reason: format!("Dispatch failed: {}", err),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn test_empty_pipeline() {
        let config = r#"
routes:
  test_pipeline: []
"#;
        let bits = Bits::from_config(config).expect("Failed to parse config");
        let job = Job::new(json!({"class": "od"}));
        match bits.process(job).await {
            JobResult::Error { .. } => {}
            r => panic!("Expected error for empty pipeline, got: {:?}", r),
        }
    }
}
