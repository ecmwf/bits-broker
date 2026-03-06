pub mod cost_weighted;
pub mod fifo;

pub use cost_weighted::CostWeightedQueue;
pub use fifo::FifoQueue;

use async_trait::async_trait;

use crate::actions::ActionError;
use crate::job::Job;

/// A queue controls when a task is allowed to proceed.
///
/// `acquire` suspends the caller until the queue grants admission.
/// The returned permit holds the slot open; dropping it releases it.
#[async_trait]
pub trait Queue: Send + Sync {
    type Permit: Send;
    async fn acquire(&self, job: &Job) -> Result<Self::Permit, ActionError>;
}
