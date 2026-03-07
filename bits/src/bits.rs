use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tokio::sync::Notify;
use tracing::Instrument;

use crate::actions::{TargetAction, TargetResult};
use crate::config::parse_config;
use crate::job::Job;
use crate::result::JobResult;
use crate::routing::switch::Switch;

// ================================
//   InFlightJob
// ================================

struct InFlightJob {
    result: Mutex<Option<JobResult>>,
    notify: Notify,
    started: Instant,
}

// ================================
//   PollOutcome
// ================================

pub enum PollOutcome {
    /// Job finished — contains the result.
    Ready(JobResult),
    /// Job still running — caller should retry with the given ID.
    Pending { id: String },
    /// No job found with this ID (expired or never existed).
    NotFound,
}

// ================================
//   JobHandle
// ================================

pub struct JobHandle {
    pub id: String,
}

// ================================
//   Bits
// ================================

pub struct Bits {
    router: Arc<Switch>,
    jobs: Arc<DashMap<String, Arc<InFlightJob>>>,
}

impl Bits {
    pub fn from_config(config: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let parsed = parse_config(config)?;
        Ok(Bits {
            router: Arc::new(parsed.router),
            jobs: Arc::new(DashMap::new()),
        })
    }

    /// Submit a job for async processing. Returns immediately with a handle; the job runs in the background.
    /// Use [`Bits::poll`] to retrieve the result.
    pub fn submit(&self, job: Job) -> JobHandle {
        let job_id = job.id.clone();
        let in_flight = Arc::new(InFlightJob {
            result: Mutex::new(None),
            notify: Notify::new(),
            started: Instant::now(),
        });

        self.jobs.insert(job_id.clone(), in_flight.clone());

        let router = self.router.clone();
        let jobs = self.jobs.clone();
        let span = tracing::info_span!("job", job.id = %job_id);
        tracing::info!(parent: &span, "job received");

        let job_id_spawn = job_id.clone();
        tokio::spawn(
            async move {
                let result = dispatch(&router, job).await;
                let ms = in_flight.started.elapsed().as_millis();
                match &result {
                    JobResult::Success { .. } => tracing::info!(duration_ms = ms, "job completed"),
                    JobResult::Redirect { .. } => tracing::info!(duration_ms = ms, "job redirected"),
                    JobResult::Error { message } => tracing::warn!(duration_ms = ms, error = %message, "job error"),
                    JobResult::Failed { reason } => tracing::error!(duration_ms = ms, reason = %reason, "job failed"),
                }
                *in_flight.result.lock().unwrap() = Some(result);
                in_flight.notify.notify_waiters();
                jobs.remove(&job_id_spawn);
            }
            .instrument(span),
        );

        JobHandle { id: job_id }
    }

    /// Poll for the result of a submitted job, waiting up to `timeout`.
    pub async fn poll(&self, id: &str, timeout: Duration) -> PollOutcome {
        let Some(in_flight) = self.jobs.get(id).map(|r| r.clone()) else {
            return PollOutcome::NotFound;
        };

        // Register interest BEFORE checking result to close the race window.
        let notified = in_flight.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        // Fast path: result already available.
        if let Some(result) = in_flight.result.lock().unwrap().take() {
            return PollOutcome::Ready(result);
        }

        match tokio::time::timeout(timeout, notified).await {
            Ok(()) => match in_flight.result.lock().unwrap().take() {
                Some(result) => PollOutcome::Ready(result),
                None => PollOutcome::Pending { id: id.to_string() }, // spurious wakeup
            },
            Err(_) => PollOutcome::Pending { id: id.to_string() },
        }
    }

    /// Process a job directly, bypassing the submit/poll lifecycle. Useful in library contexts.
    pub async fn process(&self, job: Job) -> JobResult {
        dispatch(&self.router, job).await
    }
}

async fn dispatch(router: &Switch, job: Job) -> JobResult {
    match router.dispatch(&job).await {
        Ok(TargetResult::Complete(result)) => result,
        Ok(TargetResult::Reject { reason }) => JobResult::Error {
            message: format!("All pipelines rejected: {}", reason),
        },
        Err(err) => JobResult::Failed {
            reason: format!("Dispatch failed: {}", err),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[tokio::test]
    async fn test_empty_pipeline() {
        let config = r#"
routes:
  test_pipeline: []
"#;
        let bits = Bits::from_config(config).expect("Failed to parse config");
        let job = Job::new(json!({"class": "od"}));
        match bits.process(job).await {
            JobResult::Error { .. } => {}
            r => panic!("Expected error for empty pipeline, got: {:?}", r),
        }
    }
}
