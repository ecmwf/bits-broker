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

    #[tokio::test]
    async fn test_inline_actions() {
        let config = r#"
routes:
  test_pipeline:
    - check::match:
        class: "od"
    - transform::metkit_expansion:
        expand_parameters: true
    - target::mars_destination:
        endpoint: "mars.example.com:8080"
"#;
        let bits = Bits::from_config(config).expect("Failed to parse config");
        let job = Job::new(json!({"class": "od"}));
        match bits.process(job).await {
            JobResult::Success { content_type, size, .. } => {
                assert_eq!(content_type, "application/json");
                assert_eq!(size, 55);
            }
            r => panic!("Expected success, got: {:?}", r),
        }
    }

    #[tokio::test]
    async fn test_named_registries() {
        let config = r#"
checks:
  is_od:
    type: match
    class: "od"

targets:
  mars:
    type: mars_destination
    endpoint: "mars.example.com:8080"

routes:
  test_pipeline:
    - check::is_od
    - target::mars
"#;
        let bits = Bits::from_config(config).expect("Failed to parse config");
        let job = Job::new(json!({"class": "od"}));
        match bits.process(job).await {
            JobResult::Success { .. } => {}
            r => panic!("Expected success, got: {:?}", r),
        }
    }

    #[tokio::test]
    async fn test_persist_sets_flag() {
        let config = r#"
routes:
  test_pipeline:
    - check::match:
        class: "od"
    - persist
    - target::mars_destination:
        endpoint: "mars.example.com:8080"
"#;
        let bits = Bits::from_config(config).expect("Failed to parse config");
        let job = Job::new(json!({"class": "od"}));
        match bits.process(job).await {
            JobResult::Success { .. } => {}
            r => panic!("Expected success, got: {:?}", r),
        }
    }

    #[tokio::test]
    async fn test_nested_switch() {
        let config = r#"
routes:
  complex_pipeline:
    - check::match:
        class: "ea"
    - switch:
        privileged:
          - check::has_license:
              license: "era5"
          - target::mars_destination:
              endpoint: "mars.example.com:8080"
        public:
          - target::dss_destination:
              endpoint: "dss.example.com:9090"
"#;
        let bits = Bits::from_config(config).expect("Failed to parse config");
        let job = Job::new(json!({"class": "ea"}));
        match bits.process(job).await {
            JobResult::Success { content_type, .. } => {
                assert_eq!(content_type, "application/json");
            }
            r => panic!("Expected success, got: {:?}", r),
        }
    }
}
