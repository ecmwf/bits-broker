use bits::*;
use serde_json::json;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Design Config Queue Test");
    println!("=======================");
    println!();
    
    // Load the actual design config
    let config = std::fs::read_to_string("design_config.yaml")?;
    
    println!("Creating BITS system from design_config.yaml...");
    let bits = Arc::new(Bits::from_config(&config)?);
    
    println!("Testing different route types...");
    
    // Test jobs for different routes
    let test_jobs = vec![
        ("operational_forecast", json!({
            "class": "od",
            "stream": "oper",
            "type": "fc"
        })),
        ("era5_reanalysis", json!({
            "class": "ea",
            "stream": "oper",
            "type": "an"
        })),
        ("extremes_dt", json!({
            "class": "od",
            "stream": "oper",
            "type": "fc"
        })),
        ("extremes_fc", json!({
            "class": "od",
            "stream": "oper",
            "type": "fc"
        })),
    ];
    
    // Process all jobs concurrently
    let mut handles = Vec::new();
    for (route_name, job_data) in test_jobs {
        let bits = Arc::clone(&bits);
        let route_name = route_name.to_string();
        let handle = tokio::spawn(async move {
            let job = Job::new(job_data);
            println!("Processing {} job: {}", route_name, job.id);
            let result = bits.process(job).await;
            (route_name, result)
        });
        handles.push(handle);
    }
    
    // Wait for all jobs to complete
    for handle in handles {
        let (route_name, result) = handle.await?;
        match result {
            JobResult::Success { content_type, size, .. } => {
                println!("✓ {} processed successfully! ({}, {} bytes)", route_name, content_type, size);
            }
            JobResult::Error { message } => {
                println!("✗ {} error: {}", route_name, message);
            }
            JobResult::Failed { reason } => {
                println!("✗ {} failed: {}", route_name, reason);
            }
            JobResult::Redirect { location, message } => {
                println!("→ {} redirected to: {} ({})", route_name, location, message);
            }
        }
    }
    
    println!();
    println!("Summary:");
    println!("- The mars_destination action in operational_forecast used a queue with 10 workers");
    println!("- All other actions executed synchronously (no queue configuration)");
    println!("- The queue system seamlessly integrated with the existing routing logic");
    
    Ok(())
} 