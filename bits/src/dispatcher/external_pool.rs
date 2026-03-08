use futures::future::BoxFuture;

use crate::actions::{ActionError, TargetResult};
use crate::dispatcher::Executor;
use crate::job::Job;

/// Dispatches jobs to external workers via HTTP long-poll.
///
/// Not yet implemented. When complete, this executor will hand the job off
/// to a remote worker process, hold the caller suspended, and deliver the
/// result when the worker posts it back.
pub struct ExternalPoolExecutor;

impl Executor for ExternalPoolExecutor {
    fn execute(
        &self,
        _job: &Job,
        _work: BoxFuture<'static, Result<TargetResult, ActionError>>,
    ) -> BoxFuture<'_, Result<TargetResult, ActionError>> {
        Box::pin(async { Err(ActionError::ResourceError("external pool not yet implemented".into())) })
    }
}
