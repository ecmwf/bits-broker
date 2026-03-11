use bits::{Bits, Job, JobResult};
use bits_ecmwf as _;
use serde_json::json;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let config = std::fs::read_to_string("bits-ecmwf/examples/basic_usage.yaml")?;
    let bits = Bits::from_config(&config)?;

    let jobs = vec![
        Job::new(json!({
            "class": "od",
            "stream": "oper",
            "type": "fc",
            "param": "2t",
            "levtype": "sfc",
            "date": "2024-01-15",
            "time": "1200",
            "step": "24"
        })),
        Job::new(json!({
            "class": "ea",
            "stream": "oper",
            "type": "an",
            "param": "msl",
            "levtype": "sfc",
            "date": "2024-01-15",
            "time": "0000"
        })),
    ];

    for (i, job) in jobs.into_iter().enumerate() {
        println!("Processing job {}: {}", i + 1, job.request);

        let handle = bits.submit(job);
        let result = match bits.poll(&handle.id, None).await {
            bits::PollOutcome::Ready(r) => r,
            _ => unreachable!(),
        };
        match result {
            JobResult::Success {
                content_type, size, ..
            } => {
                println!("Job {} completed successfully", i + 1);
                println!("Content-Type: {}, Size: {} bytes", content_type, size);
            }
            JobResult::Redirect { location, message } => {
                println!("Job {} redirected to: {} ({})", i + 1, location, message);
            }
            JobResult::Error { message } => {
                println!("Job {} failed: {}", i + 1, message);
            }
            JobResult::Failed { reason } => {
                println!("Job {} system failure: {}", i + 1, reason);
            }
            JobResult::Cancelled | JobResult::ClientGone => {
                println!("Job {} cancelled", i + 1);
            }
        }
        println!();
    }

    Ok(())
}
