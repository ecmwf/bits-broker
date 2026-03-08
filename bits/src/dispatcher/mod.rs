pub mod external_pool;
pub mod scheduled;
pub mod semaphore;
pub mod thread_pool;

pub use external_pool::ExternalPoolDispatcher;
pub use scheduled::ScheduledDispatcher;
pub use semaphore::SemaphoreDispatcher;
pub use thread_pool::ThreadPoolDispatcher;

use futures::future::BoxFuture;

use crate::actions::{ActionError, TargetResult};
use crate::job::Job;

/// Selects the dispatcher implementation to construct from config.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DispatcherKind {
    Semaphore,
    ThreadPool,
}

/// A dispatcher controls how a unit of work is executed.
///
/// Actions call `dispatch` with the job and an async `work` future that
/// captures everything the action needs. The dispatcher applies its policy
/// (rate-limiting, thread-pool offload, remote hand-off, …) and returns the
/// result.
///
/// `work` is `'static` so implementations that require it — such as the
/// thread-pool dispatcher — can hand the future off to another task without
/// lifetime constraints on the caller.
pub trait Dispatcher: Send + Sync {
    fn dispatch(
        &self,
        job: &Job,
        work: BoxFuture<'static, Result<TargetResult, ActionError>>,
    ) -> BoxFuture<'_, Result<TargetResult, ActionError>>;
}
