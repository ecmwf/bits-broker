// use bits::{BitsConfig, BitsEngine, Job};
// use serde_json::json;

// #[tokio::main]
// async fn main() -> Result<(), Box<dyn std::error::Error>> {
//     // Load configuration from YAML file
//     let config = BitsConfig::from_file("demo_config.yaml")?;

//     // Create and start the BITS engine
//     let engine = BitsEngine::new(config);
//     engine.start().await?;

//     // Create some example meteorological data jobs
//     let jobs = vec![
//         Job::new(json!({
//             "class": "od",
//             "stream": "oper",
//             "type": "fc",
//             "param": "2t",
//             "levtype": "sfc",
//             "date": "2024-01-15",
//             "time": "1200",
//             "step": "24"
//         })),
//         Job::new(json!({
//             "class": "ea",
//             "stream": "oper",
//             "type": "an",
//             "param": "msl",
//             "levtype": "sfc",
//             "date": "2024-01-15",
//             "time": "0000"
//         })),
//         Job::new(json!({
//             "class": "c3",
//             "stream": "moda",
//             "type": "fc",
//             "param": ["2t", "tp"],
//             "levtype": "sfc",
//             "date": "2024-01-15"
//         })),
//     ];

//     // Process each job through the BITS system
//     for (i, job) in jobs.into_iter().enumerate() {
//         println!("Processing job {}: {}", i + 1, job.data);

//         let result = engine.accept_job(job).await;
//         match result {
//             bits::JobResult::Completed(data) => {
//                 println!("Job {} completed successfully", i + 1);
//                 println!("Result: {}", String::from_utf8_lossy(&data));
//             }
//             bits::JobResult::Forwarded => {
//                 println!("Job {} forwarded to external system", i + 1);
//             }
//             bits::JobResult::Redirect(location) => {
//                 println!("Job {} redirected to: {}", i + 1, location);
//             }
//             bits::JobResult::Error(error) => {
//                 println!("Job {} failed: {}", i + 1, error);
//             }
//         }
//         println!();
//     }

//     // Shutdown the engine
//     engine.shutdown().await?;

//     Ok(())
// }
