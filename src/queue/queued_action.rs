use crate::{job::Job, Action, ActionError, ActionResult, CheckAction, CheckResult, RouteAction, RouteResult, ViaAction, ViaResult};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use tokio::sync::{mpsc, oneshot::{self, Receiver, Sender}};
use tokio::task::JoinHandle;


struct Queue {
    action: Action,
    buffer: VecDeque<Job>,
}

impl Queue {
    pub fn new(action: Action) -> Self {
        Self { action, buffer: VecDeque::new() }
    }
    
}

impl ViaAction for Queue {
    async fn execute(&mut self, job: &mut Job) -> Result<ViaResult, ActionError> {
        self.buffer.push_back(job.clone());
    }
}

// /// Configuration for queuing any action
// #[derive(Debug, Clone, Serialize, Deserialize)]
// pub struct QueueConfig {
//     #[serde(rename = "type")]
//     pub queue_type: String,
//     pub pool_size: usize,
//     #[serde(default = "default_capacity")]
//     pub capacity: usize,
// }

// fn default_capacity() -> usize {
//     1000
// }


// pub struct QueuedJob<T> {
//     job: Job,
//     response: Sender<T>,
// }

// /// Trait for processing jobs of different types
// #[async_trait]
// pub trait JobProcessor<T>: Send + Sync {
//     async fn process(&mut self, job: &Job) -> T;
// }

// /// Trait for different queue implementations
// pub trait Queue<T> {
//     fn enqueue(&mut self, job: Job) -> impl Future<Output = Result<T, ActionError>> + Send;
// }

// /// FIFO queue implementation with built-in worker processing
// pub struct FifoQueue<T> {
//     sender: mpsc::UnboundedSender<QueuedJob<T>>,
//     _worker_handle: JoinHandle<()>,
// }

// impl<T: Send + 'static> FifoQueue<T> {
//     pub fn new<P: JobProcessor<T> + 'static>(mut processor: P) -> Self {
//         let (tx, mut rx) = mpsc::unbounded_channel::<QueuedJob<T>>();
        
//         let worker_handle = tokio::spawn(async move {
//             while let Some(queued_job) = rx.recv().await {
//                 let result = processor.process(&queued_job.job).await;
//                 let _ = queued_job.response.send(result);
//             }
//         });

//         Self {
//             sender: tx,
//             _worker_handle: worker_handle,
//         }
//     }
// }

// impl<T: Send + 'static> Queue<T> for FifoQueue<T> {
//     fn enqueue(&mut self, job: Job) -> impl Future<Output = Result<T, ActionError>> + Send {
//         let (tx, rx) = oneshot::channel();
//         let queued_job = QueuedJob {
//             job,
//             response: tx,
//         };

//         // either the job workers themselves can process the action, or the calling worker can process it when
//         // the job is scheduled.
        
//         self.sender.send(queued_job).expect("Worker should be running");
        
//         // Return the future directly
//         async move {
//             rx.await.map_err(|_| ActionError::Timeout("Queue response timeout".to_string()))
//         }
//     }
// }

// // ================================
// // JobProcessor implementations for different action types
// // ================================

// /// Processor for CheckActions
// pub struct CheckProcessor {
//     action: Box<dyn CheckAction + Send>,
// }

// impl CheckProcessor {
//     pub fn new(action: Box<dyn CheckAction + Send>) -> Self {
//         Self { action }
//     }
// }

// #[async_trait]
// impl JobProcessor<CheckResult> for CheckProcessor {
//     async fn process(&mut self, job: &Job) -> CheckResult {
//         self.action.evaluate(job).await.unwrap_or(CheckResult::Reject { 
//             reason: "Processing failed".to_string() 
//         })
//     }
// }

// /// Processor for ViaActions
// pub struct ViaProcessor {
//     action: Box<dyn ViaAction + Send>,
// }

// impl ViaProcessor {
//     pub fn new(action: Box<dyn ViaAction + Send>) -> Self {
//         Self { action }
//     }
// }

// #[async_trait]
// impl JobProcessor<(Job, ViaResult)> for ViaProcessor {
//     async fn process(&mut self, job: &Job) -> (Job, ViaResult) {
//         let mut job_copy = job.clone();
//         let result = self.action.execute(&mut job_copy).await.unwrap_or(ViaResult::Reject { 
//             reason: "Processing failed".to_string() 
//         });
//         (job_copy, result)
//     }
// }

// /// Processor for RouteActions
// pub struct RouteProcessor {
//     action: Box<dyn RouteAction + Send>,
// }

// impl RouteProcessor {
//     pub fn new(action: Box<dyn RouteAction + Send>) -> Self {
//         Self { action }
//     }
// }

// #[async_trait]
// impl JobProcessor<RouteResult> for RouteProcessor {
//     async fn process(&mut self, job: &Job) -> RouteResult {
//         self.action.route(job).await.unwrap_or(RouteResult::Reject { 
//             reason: "Processing failed".to_string() 
//         })
//     }
// }



