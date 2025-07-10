pub mod config;
pub mod engine;
pub mod job;
pub mod routing;
pub mod queue;
pub mod http;

// Re-export main types for convenience
pub use config::{BitsConfig, QueueConfig, Action, DestinationTarget};
pub use engine::BitsEngine;
pub use job::{Job, JobResult};
pub use routing::Router;
pub use queue::QueueManager;

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn test_basic_job_processing() {
        let config = BitsConfig::default();
        let engine = BitsEngine::new(config);
        
        let job_data = json!({
            "class": "od",
            "stream": "oper", 
            "type": "fc",
            "param": "2t"
        });
        
        let job = Job::new(job_data);
        let result = engine.accept_job(job).await;
        
        // Should get an error since no routes are configured
        match result {
            JobResult::Error(_) => {
                // Expected - no routes configured
            }
            _ => panic!("Expected error for unconfigured engine"),
        }
    }

    #[test]
    fn test_config_parsing() {
        let yaml = r#"
queues:
  test_queue:
    type: "fifo"
    capacity: 100
    workers: 4

routes:
  test_route:
    - type: "filter"
      class: "od"
    - type: "destination"
      queue: "test_queue"
"#;
        
        let config = BitsConfig::from_yaml(yaml).expect("Failed to parse config");
        assert_eq!(config.queues.len(), 1);
        assert_eq!(config.routes.len(), 1);
        
        let queue_config = config.queues.get("test_queue").unwrap();
        assert_eq!(queue_config.queue_type, "fifo");
        assert_eq!(queue_config.capacity, 100);
        assert_eq!(queue_config.workers, Some(4));
    }

    #[test]
    fn test_job_matching() {
        let job_data = json!({
            "class": "od",
            "stream": "oper",
            "type": "fc"
        });
        
        let job = Job::new(job_data);
        
        // Test exact match
        let condition1 = json!({"class": "od"});
        assert!(job.matches_condition(&condition1));
        
        // Test array match
        let condition2 = json!({"type": ["fc", "an"]});
        assert!(job.matches_condition(&condition2));
        
        // Test no match
        let condition3 = json!({"class": "ea"});
        assert!(!job.matches_condition(&condition3));
        
        // Test multiple conditions
        let condition4 = json!({"class": "od", "stream": "oper"});
        assert!(job.matches_condition(&condition4));
    }
}