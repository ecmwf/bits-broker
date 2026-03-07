use bits::{Bits, Job, JobResult};
use bits_ecmwf as _;
use serde_json::json;
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("Design Config Queue Test");
    println!("=======================");
    println!();

    let config = std::fs::read_to_string("design_config.yaml")?;

    println!("Creating BITS system from design_config.yaml...");
    let bits = Arc::new(Bits::from_config(&config)?);

    println!("Testing different route types...");

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

    let mut handles = Vec::new();
    for (route_name, job_data) in test_jobs {
        let bits = Arc::clone(&bits);
        let route_name = route_name.to_string();
        let handle = tokio::spawn(async move {
            let job = Job::new(job_data);
            println!("Processing {} job: {}", route_name, job.id);
            let handle = bits.submit(job);
            let result = match bits.poll(&handle.id, None).await {
                bits::PollOutcome::Ready(r) => r,
                _ => unreachable!(),
            };
            (route_name, result)
        });
        handles.push(handle);
    }

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
            JobResult::Cancelled | JobResult::ClientGone => {
                println!("✗ {} cancelled", route_name);
            }
        }
    }

    Ok(())
}
