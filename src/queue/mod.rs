use crate::actions::Action;

/// Wraps any action with bounded concurrency and an optional worker pool.
///
/// A Queue is a decorator — it can wrap a Check, Transform, or Target action.
/// The execution path in switch.rs must branch on the inner action type, running
/// it with a semaphore (or channel) acquired first and released after.
///
/// - `capacity`: maximum number of jobs queued or in-flight at once.
/// - `workers`: if `Some(n)`, a fixed internal worker pool drains the queue
///   (push model). If `None`, callers pull directly with backpressure from
///   the semaphore (pull model).
#[derive(Debug)]
pub struct Queue {
    pub capacity: usize,
    pub workers: Option<usize>,
    pub action: Box<Action>,
}

impl Queue {
    pub fn new(capacity: usize, workers: Option<usize>, action: Action) -> Self {
        Self { capacity, workers, action: Box::new(action) }
    }
}
