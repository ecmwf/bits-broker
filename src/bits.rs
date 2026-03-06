use std::sync::Arc;

use crate::actions::{TargetAction, TargetResult};
use crate::config::{parse_config, ServerConfig};
use crate::job::Job;
use crate::result::JobResult;
use crate::routing::switch::Switch;
use crate::service::{HttpService, Service};

pub struct Bits {
    router: Switch,
    server: Option<ServerConfig>,
}

impl Bits {
    pub fn from_config(config: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let parsed = parse_config(config)?;
        Ok(Bits { router: parsed.router, server: parsed.server })
    }

    /// Start the configured server. Consumes `self` — call this as the main entry point
    /// when running BITS as a service rather than using it as a library.
    pub async fn serve(self) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let Bits { router, server } = self;
        let server_config = server.ok_or("no server block in config")?;
        let bits = Arc::new(Bits { router, server: None });
        match server_config {
            ServerConfig::Http { bind } => HttpService::new(bind, bits).run().await,
        }
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
