// use bits::*;
// use serde_json::json;
// use std::sync::Arc;

// #[tokio::main]
// async fn main() -> Result<(), Box<dyn std::error::Error>> {
//     println!("Queue System Example");
//     println!("===================");
//     println!();
    
//     // Test configuration with multiple queued actions
//     let config = r#"
// routes:
//   queued_route:
//     - check::match:
//         class: "od"
//         queue:
//           type: "basic"
//           pool_size: 1
//     - via::metkit_expansion:
//         expand_parameters: true
//         queue:
//           type: "priority"
//           pool_size: 2
//     - route::mars_destination:
//         endpoint: "mars.ecmwf.int:8080"
//         queue:
//           type: "fair"
//           pool_size: 3
// "#;

//     println!("Creating BITS system with multiple queued actions...");
//     let bits = Arc::new(Bits::from_config(config)?);
    
//     println!("Processing multiple jobs concurrently...");
    
//     // Create multiple test jobs
//     let jobs = vec![
//         Job::new(json!({
//             "class": "od",
//             "stream": "oper",
//             "type": "fc"
//         })),
//         Job::new(json!({
//             "class": "od",
//             "stream": "oper",
//             "type": "an"
//         })),
//         Job::new(json!({
//             "class": "od",
//             "stream": "wave",
//             "type": "fc"
//         })),
//     ];
    
//     // Process all jobs concurrently
//     let mut handles = Vec::new();
//     for (i, job) in jobs.into_iter().enumerate() {
//         let bits = Arc::clone(&bits);
//         let handle = tokio::spawn(async move {
//             println!("Processing job {}: {}", i + 1, job.id);
//             let result = bits.process(job).await;
//             (i + 1, result)
//         });
//         handles.push(handle);
//     }
    
//     // Wait for all jobs to complete
//     for handle in handles {
//         let (job_num, result) = handle.await?;
//         match result {
//             JobResult::Success { content_type, size, .. } => {
//                 println!("✓ Job {} processed successfully! ({}, {} bytes)", job_num, content_type, size);
//             }
//             JobResult::Error { message } => {
//                 println!("✗ Job {} error: {}", job_num, message);
//             }
//             JobResult::Failed { reason } => {
//                 println!("✗ Job {} failed: {}", job_num, reason);
//             }
//             JobResult::Redirect { location, message } => {
//                 println!("→ Job {} redirected to: {} ({})", job_num, location, message);
//             }
//         }
//     }
    
//     println!();
//     println!("Summary:");
//     println!("- check::match used a 'basic' queue with 1 worker");
//     println!("- via::metkit_expansion used a 'priority' queue with 2 workers");
//     println!("- route::mars_destination used a 'fair' queue with 3 workers");
//     println!("- All actions were executed asynchronously through their respective queues");
//     println!("- Worker pools were automatically started when actions were created");
    
//     Ok(())
// } 