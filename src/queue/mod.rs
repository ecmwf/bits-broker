pub mod semaphore_queue;
pub mod worker_queue;
#[cfg(test)]
pub mod test_helpers;

pub use semaphore_queue::SemaphoreQueue;
pub use worker_queue::WorkerQueue;

use crate::actions::{CheckAction, TargetAction, TransformAction};

/// All queue types must implement all three action traits so they are
/// transparent to the pipeline — the switch never needs to know a queue
/// is involved.
pub trait Queue: CheckAction + TransformAction + TargetAction {}
