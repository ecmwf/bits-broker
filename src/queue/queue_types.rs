// use crate::job::Job;
// use async_trait::async_trait;
// use std::collections::VecDeque;
// use tokio::sync::{Mutex, Notify};
// use std::sync::Arc;

// /// Trait for all queue types.
// #[async_trait]
// pub trait Queue: Send + Sync {
//     /// Add a job to the queue.
//     async fn enqueue(&self, job: Job) -> Result<(), String>;
    
//     /// Remove a job from the queue.
//     async fn dequeue(&self) -> Option<Job>;
    
//     /// Get the current queue size.
//     async fn size(&self) -> usize;
    
//     /// Check if the queue is empty.
//     async fn is_empty(&self) -> bool;
    
//     /// Get the queue capacity.
//     fn capacity(&self) -> usize;
// }

// /// Factory for creating different queue types.
// pub struct QueueType;

// impl QueueType {
//     /// Create a FIFO queue.
//     pub fn fifo(capacity: usize) -> FifoQueue {
//         FifoQueue::new(capacity)
//     }
    
//     /// Create a priority queue.
//     pub fn priority(capacity: usize) -> PriorityQueue {
//         PriorityQueue::new(capacity)
//     }
    
//     /// Create a fair queue.
//     pub fn fair(capacity: usize) -> FairQueue {
//         FairQueue::new(capacity)
//     }
// }

// /// FIFO (First In, First Out) queue implementation.
// pub struct FifoQueue {
//     queue: Arc<Mutex<VecDeque<Job>>>,
//     capacity: usize,
//     notify: Arc<Notify>,
// }

// impl FifoQueue {
//     pub fn new(capacity: usize) -> Self {
//         Self {
//             queue: Arc::new(Mutex::new(VecDeque::new())),
//             capacity,
//             notify: Arc::new(Notify::new()),
//         }
//     }
// }

// #[async_trait]
// impl Queue for FifoQueue {
//     async fn enqueue(&self, job: Job) -> Result<(), String> {
//         let mut queue = self.queue.lock().await;
//         if queue.len() >= self.capacity {
//             return Err("Queue is full".to_string());
//         }
//         queue.push_back(job);
//         self.notify.notify_one();
//         Ok(())
//     }
    
//     async fn dequeue(&self) -> Option<Job> {
//         let mut queue = self.queue.lock().await;
//         queue.pop_front()
//     }
    
//     async fn size(&self) -> usize {
//         let queue = self.queue.lock().await;
//         queue.len()
//     }
    
//     async fn is_empty(&self) -> bool {
//         let queue = self.queue.lock().await;
//         queue.is_empty()
//     }
    
//     fn capacity(&self) -> usize {
//         self.capacity
//     }
// }

// /// Priority queue implementation (higher priority jobs first).
// pub struct PriorityQueue {
//     queue: Arc<Mutex<Vec<(Job, u32)>>>, // (job, priority)
//     capacity: usize,
//     notify: Arc<Notify>,
// }

// impl PriorityQueue {
//     pub fn new(capacity: usize) -> Self {
//         Self {
//             queue: Arc::new(Mutex::new(Vec::new())),
//             capacity,
//             notify: Arc::new(Notify::new()),
//         }
//     }
    
//     fn get_job_priority(job: &Job) -> u32 {
//         // Extract priority from job metadata or data
//         // Default to 0 if no priority specified
//         job.metadata.get("priority")
//             .and_then(|p| p.parse().ok())
//             .unwrap_or(0)
//     }
// }

// #[async_trait]
// impl Queue for PriorityQueue {
//     async fn enqueue(&self, job: Job) -> Result<(), String> {
//         let mut queue = self.queue.lock().await;
//         if queue.len() >= self.capacity {
//             return Err("Queue is full".to_string());
//         }
        
//         let priority = Self::get_job_priority(&job);
//         queue.push((job, priority));
        
//         // Sort by priority (highest first)
//         queue.sort_by(|a, b| b.1.cmp(&a.1));
        
//         self.notify.notify_one();
//         Ok(())
//     }
    
//     async fn dequeue(&self) -> Option<Job> {
//         let mut queue = self.queue.lock().await;
//         queue.pop().map(|(job, _)| job)
//     }
    
//     async fn size(&self) -> usize {
//         let queue = self.queue.lock().await;
//         queue.len()
//     }
    
//     async fn is_empty(&self) -> bool {
//         let queue = self.queue.lock().await;
//         queue.is_empty()
//     }
    
//     fn capacity(&self) -> usize {
//         self.capacity
//     }
// }

// /// Fair queue implementation (round-robin between different job types).
// pub struct FairQueue {
//     queues: Arc<Mutex<std::collections::HashMap<String, VecDeque<Job>>>>,
//     round_robin_state: Arc<Mutex<usize>>,
//     capacity: usize,
//     notify: Arc<Notify>,
// }

// impl FairQueue {
//     pub fn new(capacity: usize) -> Self {
//         Self {
//             queues: Arc::new(Mutex::new(std::collections::HashMap::new())),
//             round_robin_state: Arc::new(Mutex::new(0)),
//             capacity,
//             notify: Arc::new(Notify::new()),
//         }
//     }
    
//     fn get_job_type(job: &Job) -> String {
//         // Extract job type for fair scheduling
//         job.data.get("type")
//             .and_then(|v| v.as_str())
//             .unwrap_or("default")
//             .to_string()
//     }
    
//     async fn total_size(&self) -> usize {
//         let queues = self.queues.lock().await;
//         queues.values().map(|q| q.len()).sum()
//     }
// }

// #[async_trait]
// impl Queue for FairQueue {
//     async fn enqueue(&self, job: Job) -> Result<(), String> {
//         if self.total_size().await >= self.capacity {
//             return Err("Queue is full".to_string());
//         }
        
//         let job_type = Self::get_job_type(&job);
//         let mut queues = self.queues.lock().await;
        
//         let type_queue = queues.entry(job_type).or_insert_with(VecDeque::new);
//         type_queue.push_back(job);
        
//         self.notify.notify_one();
//         Ok(())
//     }
    
//     async fn dequeue(&self) -> Option<Job> {
//         let mut queues = self.queues.lock().await;
//         let mut round_robin_state = self.round_robin_state.lock().await;
        
//         if queues.is_empty() {
//             return None;
//         }
        
//         let queue_keys: Vec<String> = queues.keys().cloned().collect();
//         if queue_keys.is_empty() {
//             return None;
//         }
        
//         // Round-robin through job types
//         let start_index = *round_robin_state % queue_keys.len();
        
//         for i in 0..queue_keys.len() {
//             let index = (start_index + i) % queue_keys.len();
//             let key = &queue_keys[index];
            
//             if let Some(type_queue) = queues.get_mut(key) {
//                 if let Some(job) = type_queue.pop_front() {
//                     *round_robin_state = index + 1;
                    
//                     // Clean up empty queues
//                     if type_queue.is_empty() {
//                         queues.remove(key);
//                     }
                    
//                     return Some(job);
//                 }
//             }
//         }
        
//         None
//     }
    
//     async fn size(&self) -> usize {
//         self.total_size().await
//     }
    
//     async fn is_empty(&self) -> bool {
//         let queues = self.queues.lock().await;
//         queues.values().all(|q| q.is_empty())
//     }
    
//     fn capacity(&self) -> usize {
//         self.capacity
//     }
// } 