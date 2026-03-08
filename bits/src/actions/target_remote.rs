use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::actions::{ActionError, TargetAction, TargetResult};
use crate::job::Job;

/// A noop target that defers all work to an external worker pool.
///
/// This action does nothing locally — it must always be paired with the
/// `remote_pool` executor in config, which hands the job to a remote worker.
/// If this action is ever dispatched directly (i.e., without a `remote_pool`
/// dispatcher), it returns a `ConfigError` immediately.
#[derive(Debug, Serialize, Deserialize)]
pub struct RemoteTarget;

#[async_trait]
impl TargetAction for RemoteTarget {
    async fn dispatch(&self, _job: &Job) -> Result<TargetResult, ActionError> {
        Err(ActionError::ConfigError(
            "'remote' target must be paired with 'executor: remote_pool'".into(),
        ))
    }
}

crate::register_action!(target, "remote", RemoteTarget);
