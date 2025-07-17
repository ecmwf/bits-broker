// use crate::queue::{Queue, queued_action::{QueuedTask, QueuedTaskSender}};
// use crate::actions::Action;
// use std::sync::Arc;
// use tokio::task::JoinHandle;

// /// Manages a pool of workers for processing queued tasks.
// pub struct WorkerPool {
//     worker_count: usize,
//     queue: Arc<dyn Queue<QueuedTask>>,
//     handles: Vec<JoinHandle<()>>,
// }

// impl WorkerPool {
//     /// Create a new worker pool.
//     pub fn new(worker_count: usize, queue: Arc<dyn Queue<QueuedTask>>) -> Self {
//         Self {
//             worker_count,
//             queue,
//             handles: Vec::new(),
//         }
//     }

//     /// Start all workers in the pool.
//     pub async fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
//         for i in 0..self.worker_count {
//             let queue = self.queue.clone();
//             let worker_id = i;
            
//             let handle = tokio::spawn(async move {
//                 Self::worker_loop(worker_id, queue).await;
//             });
            
//             self.handles.push(handle);
//         }
        
//         println!("Started {} workers", self.worker_count);
//         Ok(())
//     }

//     /// Shutdown all workers.
//     pub async fn shutdown(&mut self) -> Result<(), Box<dyn std::error::Error>> {
//         // TODO: Implement graceful shutdown
//         // For now, just abort all workers
//         for handle in &self.handles {
//             handle.abort();
//         }
        
//         self.handles.clear();
//         println!("Shutdown {} workers", self.worker_count);
//         Ok(())
//     }

//     /// Main worker loop.
//     async fn worker_loop(worker_id: usize, queue: Arc<dyn Queue<QueuedTask>>) {
//         println!("Worker {} started", worker_id);
        
//         loop {
//             // Poll for tasks
//             if let Some(task) = queue.dequeue().await {
//                 println!("Worker {} processing task: {}", worker_id, task.id);
                
//                 // Process the task
//                 Self::process_task(task).await;
//             } else {
//                 // No tasks available, sleep briefly
//                 tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
//             }
//         }
//     }

//     /// Process a single queued task.
//     async fn process_task(task: QueuedTask) {
//         let QueuedTask { id, job, action_name, action_config, response_sender } = task;
        
//         // Recreate the action from the stored name and config
//         let action = match crate::routing::registry::create_action(&action_name, action_config) {
//             Ok(action) => action,
//             Err(e) => {
//                 eprintln!("Failed to recreate action {} for task {}: {}", action_name, id, e);
//                 return;
//             }
//         };
        
//         match (action, response_sender) {
//             (Action::Check(check_action), QueuedTaskSender::Check(sender)) => {
//                 let result = check_action.evaluate(&job).await;
//                 if let Err(_) = sender.send(result) {
//                     eprintln!("Failed to send check result for task {}", id);
//                 }
//             }
//             (Action::Via(via_action), QueuedTaskSender::Via(sender)) => {
//                 let mut job_copy = job.clone();
//                 let result = via_action.execute(&mut job_copy).await;
//                 if let Err(_) = sender.send(result) {
//                     eprintln!("Failed to send via result for task {}", id);
//                 }
//                 // TODO: Handle job mutations - this is a limitation we need to address
//             }
//             (Action::Router(route_action), QueuedTaskSender::Route(sender)) => {
//                 let result = route_action.route(&job).await;
//                 if let Err(_) = sender.send(result) {
//                     eprintln!("Failed to send route result for task {}", id);
//                 }
//             }
//             _ => {
//                 eprintln!("Mismatched action and response sender types for task {}", id);
//             }
//         }
//     }
// } 