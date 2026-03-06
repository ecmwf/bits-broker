use bits::*;
use serde::{Deserialize, Serialize};
use async_trait::async_trait;

// ================================
//   Custom Actions with In-Situ Registration
// ================================

// Example custom check action - registered right after definition
#[derive(Debug, Serialize, Deserialize)]
pub struct CustomFilter {
    pub required_field: String,
}

#[async_trait]
impl CheckAction for CustomFilter {
    async fn evaluate(&self, job: &Job) -> Result<CheckResult, ActionError> {
        if let Some(value) = job.request.get("custom_field") {
            if value.as_str() == Some(&self.required_field) {
                Ok(CheckResult::Pass)
            } else {
                Ok(CheckResult::Reject {
                    reason: format!("Custom field '{}' does not match required '{}'",
                                   value, self.required_field)
                })
            }
        } else {
            Ok(CheckResult::Reject {
                reason: "Missing custom_field".to_string()
            })
        }
    }
}

// Register the custom filter action in-situ
register_action!(check, "custom_filter", CustomFilter);

// ================================
//   Another Custom Action Module
// ================================

// This could be in a completely different module or crate
mod my_custom_actions {
    use super::*;

    // Example custom transform action - registered right after definition
    #[derive(Debug, Serialize, Deserialize)]
    pub struct CustomTransform {
        pub add_field: String,
    }

    #[async_trait]
    impl TransformAction for CustomTransform {
        async fn execute(&self, job: &mut Job) -> Result<TransformResult, ActionError> {
            let mut metadata = job.metadata.as_object().unwrap_or(&serde_json::Map::new()).clone();
            metadata.insert("custom_transform".to_string(), serde_json::json!(self.add_field));
            job.metadata = serde_json::Value::Object(metadata);
            Ok(TransformResult::Continue)
        }
    }

    // Register the custom transform action in-situ
    register_action!(transform, "custom_transform", CustomTransform);
}

// ================================
//   Third Party Action Example
// ================================

// This could be in a third-party crate or plugin
#[derive(Debug, Serialize, Deserialize)]
pub struct CustomDestination {
    pub endpoint: String,
}

#[async_trait]
impl TargetAction for CustomDestination {
    async fn dispatch(&self, _job: &Job) -> Result<TargetResult, ActionError> {
        let data = format!("Data sent to {}", self.endpoint);
        let data_bytes = bytes::Bytes::from(data.into_bytes());
        let size = data_bytes.len() as i64;
        let stream: Box<dyn futures::Stream<Item = Result<bytes::Bytes, std::io::Error>> + Send + Unpin> =
            Box::new(futures::stream::iter(vec![Ok(data_bytes)]));

        Ok(TargetResult::Complete(JobResult::Success {
            content_type: "text/plain".to_string(),
            size,
            stream,
        }))
    }
}

// Register the custom destination action in-situ
register_action!(target, "custom_destination", CustomDestination);

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Custom Action Registration Example");
    println!("==================================");
    println!();

    // Show all available actions (including our custom ones)
    println!("Available actions: {:?}", list_actions());
    println!();

    // Test the custom filter action
    println!("Testing custom_filter action:");
    let config = serde_json::json!({
        "required_field": "test_value"
    });

    let action = create_action("custom_filter", config)?;
    println!("✓ Created custom action: {:?}", action);

    // Create a test job
    let job_data = serde_json::json!({
        "custom_field": "test_value",
        "other_field": "some_data"
    });

    let job = Job::new(job_data);

    // Test the action
    match action {
        Action::Check(check_action) => {
            let result = check_action.evaluate(&job).await?;
            match result {
                CheckResult::Pass => println!("✓ Custom filter passed!"),
                CheckResult::Reject { reason } => println!("✗ Custom filter rejected: {}", reason),
            }
        }
        _ => println!("Unexpected action type"),
    }

    println!();

    // Test the custom transform action
    println!("Testing custom_transform action:");
    let config = serde_json::json!({
        "add_field": "transformed_data"
    });

    let action = create_action("custom_transform", config)?;
    println!("✓ Created custom transform action: {:?}", action);

    // Test the transform action
    let mut job = Job::new(serde_json::json!({"test": "data"}));
    match action {
        Action::Transform(transform_action) => {
            let result = transform_action.execute(&mut job).await?;
            match result {
                TransformResult::Continue => {
                    println!("✓ Custom transform executed successfully!");
                    println!("  Job metadata: {}", job.metadata);
                }
                TransformResult::Reject { reason } => println!("✗ Custom transform rejected: {}", reason),
            }
        }
        _ => println!("Unexpected action type"),
    }

    println!();

    // Test the custom destination action
    println!("Testing custom_destination action:");
    let config = serde_json::json!({
        "endpoint": "https://my-custom-service.com/api"
    });

    let action = create_action("custom_destination", config)?;
    println!("✓ Created custom destination action: {:?}", action);

    // Test the target action
    let job = Job::new(serde_json::json!({"data": "to_route"}));
    match action {
        Action::Target(target_action) => {
            let result = target_action.dispatch(&job).await?;
            match result {
                TargetResult::Complete(job_result) => {
                    println!("✓ Custom destination dispatched successfully!");
                    println!("  Result: {:?}", job_result);
                }
                TargetResult::Reject { reason } => println!("✗ Custom destination rejected: {}", reason),
            }
        }
        _ => println!("Unexpected action type"),
    }

    println!();
    println!("All custom actions registered and tested successfully!");
    println!("Actions can be defined anywhere in the codebase and will be automatically discovered.");

    Ok(())
}
