use crate::job::Job;
use crate::result::JobResult;
use crate::actions::RouteAction;
use crate::routing::switch::Switch;
use crate::shared::ResourceRegistry;
use serde::Deserialize;

/// The main BITS system that orchestrates job processing.
#[derive(Debug)]
pub struct Bits {
    router: Switch,
    #[allow(dead_code)]
    resource_registry: ResourceRegistry,
}

impl Bits {
    /// Create a new BITS system from configuration.
    pub fn from_config(config: &str) -> Result<Self, Box<dyn std::error::Error>> {
        // Create resource registry
        let resource_registry = ResourceRegistry::new();
        
        // For now, parse without shared resources - we'll need to implement 
        // a custom approach for this later
        #[derive(Deserialize)]
        struct BitsConfig {
            #[serde(rename = "routes")]
            router: Switch,
        }
        
        let config: BitsConfig = serde_yaml::from_str(config)?;
        
        Ok(Bits {
            router: config.router,
            resource_registry,
        })
    }

    /// Get a reference to the resource registry
    pub fn resource_registry(&self) -> &ResourceRegistry {
        &self.resource_registry
    }

    /// Accept and process a job through the routing system.
    pub async fn process(&self, job: Job) -> JobResult {
        match self.router.route(&job).await {
            Ok(crate::actions::RouteResult::Complete(result)) => result,
            Ok(crate::actions::RouteResult::Reject { reason }) => JobResult::Error { 
                message: format!("All routes rejected: {}", reason) 
            },
            Err(err) => JobResult::Failed { 
                reason: format!("Routing failed: {}", err) 
            },
        }
    }
}





#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn test_basic_config_parsing() {
        let config = r#"
routes:
  test_route: []
"#;

        let bits = Bits::from_config(config).expect("Failed to parse config");
        
        let job_data = json!({
            "class": "od",
            "stream": "oper",
            "type": "fc"
        });

        let job = Job::new(job_data);
        let result = bits.process(job).await;

        // Should get an error since no actions are configured
        match result {
            JobResult::Error { .. } => {
                // Expected - no actions configured
            }
            _ => panic!("Expected error for empty route"),
        }
    }

    #[tokio::test]
    async fn test_action_parsing() {
        let config = r#"
routes:
  test_route:
    - check::match:
        class: "od"
    - via::metkit_expansion:
        expand_parameters: true
    - route::mars_destination:
        endpoint: "mars.example.com:8080"
"#;

        let bits = Bits::from_config(config).expect("Failed to parse config");
        
        let job_data = json!({
            "class": "od",
            "stream": "oper",
            "type": "fc"
        });

        let job = Job::new(job_data);
        let result = bits.process(job).await;

        // Should process successfully and return data from mars_destination
        match result {
            JobResult::Success { content_type, size, .. } => {
                assert_eq!(content_type, "application/json");
                assert_eq!(size, 51);
            }
            _ => panic!("Expected successful processing, got: {:?}", result),
        }
    }

    #[tokio::test]
    async fn test_nested_switch() {
        let config = r#"
routes:
  complex_route:
    - check::match:
        class: "ea"
    - switch:
        privileged:
          - check::has_license:
              license: "era5"
          - route::mars_destination:
              endpoint: "mars.example.com:8080"
        public:
          - route::dss_destination:
              endpoint: "dss.example.com:9090"
"#;

        let bits = Bits::from_config(config).expect("Failed to parse config");
        
        let job_data = json!({
            "class": "ea",
            "stream": "oper",
            "type": "an"
        });

        let job = Job::new(job_data);
        let result = bits.process(job).await;

        // Should process through the nested switch
        match result {
            JobResult::Success { content_type, .. } => {
                // Could be either Mars (json) or DSS (json) depending on license check
                assert_eq!(content_type, "application/json");
            }
            _ => panic!("Expected successful processing through nested switch, got: {:?}", result),
        }
    }
}  