// use crate::config::QueueConfig;
// use crate::job::Job;
// use crate::queue::{Queue, QueueType, WorkerPool};
// use bytes::Bytes;
// use std::collections::HashMap;
// use std::sync::Arc;
// use tokio::sync::RwLock;

// /// Manages all queues and their workers.
// #[derive(Clone)]
// pub struct QueueManager {
//     queues: Arc<RwLock<HashMap<String, Arc<dyn Queue>>>>,
//     worker_pools: Arc<RwLock<HashMap<String, WorkerPool>>>,
// }

// impl QueueManager {
//     /// Create a new queue manager from queue configurations.
//     pub fn new(queue_configs: &HashMap<String, QueueConfig>) -> Self {
//         let mut queues = HashMap::new();
//         let mut worker_pools = HashMap::new();

//         for (name, config) in queue_configs {
//             // Create the appropriate queue type
//             let queue: Arc<dyn Queue> = match config.queue_type.as_str() {
//                 "fifo" => Arc::new(QueueType::fifo(config.capacity)),
//                 "priority" => Arc::new(QueueType::priority(config.capacity)),
//                 "fair" => Arc::new(QueueType::fair(config.capacity)),
//                 _ => Arc::new(QueueType::fifo(config.capacity)), // Default to FIFO
//             };

//             queues.insert(name.clone(), queue.clone());

//             // Create worker pool if this queue has internal workers
//             if let Some(worker_count) = config.workers {
//                 let worker_pool = WorkerPool::new(worker_count, queue.clone());
//                 worker_pools.insert(name.clone(), worker_pool);
//             }
//         }

//         Self {
//             queues: Arc::new(RwLock::new(queues)),
//             worker_pools: Arc::new(RwLock::new(worker_pools)),
//         }
//     }

//     /// Start all internal worker pools.
//     pub async fn start_workers(&self) -> Result<(), Box<dyn std::error::Error>> {
//         let mut worker_pools = self.worker_pools.write().await;
//         for (name, pool) in worker_pools.iter_mut() {
//             println!("Starting worker pool for queue: {}", name);
//             pool.start().await?;
//         }
//         Ok(())
//     }

//     /// Shutdown all worker pools.
//     pub async fn shutdown(&self) -> Result<(), Box<dyn std::error::Error>> {
//         let mut worker_pools = self.worker_pools.write().await;
//         for (name, pool) in worker_pools.iter_mut() {
//             println!("Shutting down worker pool for queue: {}", name);
//             pool.shutdown().await?;
//         }
//         Ok(())
//     }

//     /// Process a job through a via queue (returns modified job).
//     pub async fn process_via_queue(&self, job: Job, queue_name: &str) -> Result<Job, String> {
//         let queues = self.queues.read().await;
//         let queue = queues.get(queue_name)
//             .ok_or_else(|| format!("Queue '{}' not found", queue_name))?;

//         // For via queues, we enqueue the job and wait for it to be processed
//         queue.enqueue(job.clone()).await
//             .map_err(|e| format!("Failed to enqueue job: {}", e))?;

//         // TODO: Wait for job to be processed and return the result
//         // For now, just return the original job
//         Ok(job)
//     }

//     /// Send a job to a destination queue (returns final result).
//     pub async fn send_to_destination_queue(&self, job: Job, queue_name: &str) -> Result<Bytes, String> {
//         let queues = self.queues.read().await;
//         let queue = queues.get(queue_name)
//             .ok_or_else(|| format!("Queue '{}' not found", queue_name))?;

//         // For destination queues, we enqueue and return immediately
//         queue.enqueue(job.clone()).await
//             .map_err(|e| format!("Failed to enqueue job: {}", e))?;

//         // TODO: Return appropriate result based on queue type
//         Ok(Bytes::from(format!("Job sent to queue: {}", queue_name)))
//     }

//     /// Get a queue by name (for external worker polling).
//     pub async fn get_queue(&self, name: &str) -> Option<Arc<dyn Queue>> {
//         let queues = self.queues.read().await;
//         queues.get(name).cloned()
//     }

//     /// List all queue names.
//     pub async fn list_queues(&self) -> Vec<String> {
//         let queues = self.queues.read().await;
//         queues.keys().cloned().collect()
//     }
// } 