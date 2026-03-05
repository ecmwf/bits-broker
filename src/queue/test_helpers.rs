use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use async_trait::async_trait;
use tokio::time::Duration;
use serde_json::json;

use crate::actions::{ActionError, CheckAction, CheckResult, TargetAction, TargetResult, TransformAction, TransformResult};
use crate::job::Job;

pub fn test_job() -> Job {
    Job::new(json!({"test": true}))
}

pub struct PassCheck;
#[async_trait]
impl CheckAction for PassCheck {
    async fn evaluate(&self, _job: &Job) -> Result<CheckResult, ActionError> {
        Ok(CheckResult::Pass)
    }
}

/// Tracks concurrent executions and sleeps for `delay` to hold the slot.
pub struct SlowCheck {
    pub concurrent: Arc<AtomicUsize>,
    pub max_concurrent: Arc<AtomicUsize>,
    pub delay: Duration,
}

impl SlowCheck {
    pub fn new(delay: Duration) -> (Self, Arc<AtomicUsize>) {
        let concurrent = Arc::new(AtomicUsize::new(0));
        let max_concurrent = Arc::new(AtomicUsize::new(0));
        let check = Self {
            concurrent: Arc::clone(&concurrent),
            max_concurrent: Arc::clone(&max_concurrent),
            delay,
        };
        (check, max_concurrent)
    }
}

#[async_trait]
impl CheckAction for SlowCheck {
    async fn evaluate(&self, _job: &Job) -> Result<CheckResult, ActionError> {
        let current = self.concurrent.fetch_add(1, Ordering::SeqCst) + 1;
        self.max_concurrent.fetch_max(current, Ordering::SeqCst);
        tokio::time::sleep(self.delay).await;
        self.concurrent.fetch_sub(1, Ordering::SeqCst);
        Ok(CheckResult::Pass)
    }
}

pub struct SetMetadata(pub serde_json::Value);
#[async_trait]
impl TransformAction for SetMetadata {
    async fn execute(&self, job: &mut Job) -> Result<TransformResult, ActionError> {
        job.metadata = self.0.clone();
        Ok(TransformResult::Continue)
    }
}

pub struct RejectTarget;
#[async_trait]
impl TargetAction for RejectTarget {
    async fn dispatch(&self, _job: &Job) -> Result<TargetResult, ActionError> {
        Ok(TargetResult::Reject { reason: "test reject".into() })
    }
}
