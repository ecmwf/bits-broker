use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use tracing::Instrument;

use crate::actions::{TargetAction, TargetResult};
use crate::config::parse_config;
use crate::job::Job;
use crate::result::JobResult;
use crate::routing::switch::Switch;

/// Buffer added on top of the poll timeout to allow for the reconnect round-trip.
const RECONNECT_BUFFER: Duration = Duration::from_secs(5);
/// Default sweep interval for removing expired completed jobs.
const DEFAULT_SWEEP_INTERVAL: Duration = Duration::from_secs(5);

// ================================
//   ConnectedGuard
// ================================

/// Sets `client_connected` to true on creation and back to false on drop.
/// Guarantees the flag is cleared even if the poll future is cancelled mid-await.
struct ConnectedGuard(Arc<AtomicBool>);

impl ConnectedGuard {
    fn new(flag: Arc<AtomicBool>) -> Self {
        flag.store(true, Ordering::Relaxed);
        Self(flag)
    }
}

impl Drop for ConnectedGuard {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Relaxed);
    }
}

// ================================
//   PollOutcome
// ================================

#[derive(Debug)]
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
    jobs: Arc<DashMap<String, Arc<Job>>>,
}

impl Bits {
    pub fn from_config(config: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let parsed = parse_config(config)?;
        let sweep_interval = parsed.sweep_interval.unwrap_or(DEFAULT_SWEEP_INTERVAL);
        let bits = Bits {
            router: Arc::new(parsed.router),
            jobs: Arc::new(DashMap::new()),
        };

        start_sweeper(bits.jobs.clone(), sweep_interval);

        Ok(bits)
    }

    /// Submit a job for async processing. Returns immediately with a handle; the job runs in the background.
    /// Use [`Bits::poll`] to retrieve the result.
    pub fn submit(&self, job: Job) -> JobHandle {
        let job_id = job.id.clone();

        // Give the client a short window to make their first poll() call.
        *job.reconnect_deadline.lock().unwrap() = Instant::now() + RECONNECT_BUFFER;

        // Seal the job into an Arc. The dispatch task gets a clone of the job
        // (sharing lifecycle arcs) while the Arc stays in the map for poll/cancel.
        let job = Arc::new(job);
        self.jobs.insert(job_id.clone(), job.clone());

        let router = self.router.clone();
        let span = tracing::info_span!("job", job.id = %job_id);
        tracing::info!(parent: &span, "job received");

        tokio::spawn(
            async move {
                let started = Instant::now();
                let result = dispatch(&router, (*job).clone()).await;
                let ms = started.elapsed().as_millis();
                match &result {
                    JobResult::Success { .. } => tracing::info!(duration_ms = ms, "job completed"),
                    JobResult::Redirect { .. } => tracing::info!(duration_ms = ms, "job redirected"),
                    JobResult::Error { message } => tracing::warn!(duration_ms = ms, error = %message, "job error"),
                    JobResult::Failed { reason } => tracing::error!(duration_ms = ms, reason = %reason, "job failed"),
                    JobResult::Cancelled => tracing::info!(duration_ms = ms, "job cancelled"),
                    JobResult::ClientGone => tracing::info!(duration_ms = ms, "job abandoned: client gone"),
                }
                *job.result.lock().unwrap() = Some(result);
                job.notify.notify_waiters();
                // Do NOT remove from jobs here — poll() removes the entry when it
                // consumes the result. Removing here would cause NotFound if the
                // client polls after a fast job completes.
            }
            .instrument(span),
        );

        JobHandle { id: job_id }
    }

    /// Cancel a submitted job. The job continues processing until the next action boundary,
    /// at which point the pipeline will stop and return `JobResult::Cancelled`.
    pub fn cancel(&self, id: &str) {
        if let Some(job) = self.jobs.get(id) {
            job.cancelled.store(true, Ordering::Relaxed);
        }
    }

    /// Poll for the result of a submitted job.
    ///
    /// Waits up to `timeout` for the result, or indefinitely if `None`.
    /// Returns `Pending` on timeout (caller should reconnect) or `NotFound` if the job has gone.
    pub async fn poll(&self, id: &str, timeout: Option<Duration>) -> PollOutcome {
        let Some(job) = self.jobs.get(id).map(|r| r.clone()) else {
            return PollOutcome::NotFound;
        };

        // Mark client as connected for the duration of this call.
        // ConnectedGuard clears the flag on drop, even if this future is cancelled.
        let _guard = ConnectedGuard::new(job.client_connected.clone());

        // Register interest BEFORE checking result to close the race window.
        let notified = job.notify.notified();
        tokio::pin!(notified);
        notified.as_mut().enable();

        // Fast path: result already available.
        if let Some(result) = job.result.lock().unwrap().take() {
            self.jobs.remove(id);
            return PollOutcome::Ready(result);
        }

        let outcome = match timeout {
            Some(t) => match tokio::time::timeout(t, notified).await {
                Ok(()) => match job.result.lock().unwrap().take() {
                    Some(result) => {
                        self.jobs.remove(id);
                        PollOutcome::Ready(result)
                    }
                    None => PollOutcome::Pending { id: id.to_string() }, // spurious wakeup
                },
                Err(_) => PollOutcome::Pending { id: id.to_string() },
            },
            None => {
                notified.await;
                match job.result.lock().unwrap().take() {
                    Some(result) => {
                        self.jobs.remove(id);
                        PollOutcome::Ready(result)
                    }
                    None => PollOutcome::Pending { id: id.to_string() }, // spurious wakeup
                }
            }
        };

        // Client made it to the end — give them the reconnect window.
        // Not reached if this future is dropped (client disconnected mid-poll).
        *job.reconnect_deadline.lock().unwrap() = Instant::now() + RECONNECT_BUFFER;

        outcome
    }
}

fn start_sweeper(jobs: Arc<DashMap<String, Arc<Job>>>, sweep_interval: Duration) {
    std::thread::spawn(move || loop {
        std::thread::sleep(sweep_interval);
        let mut expired = Vec::new();

        for entry in jobs.iter() {
            let job = entry.value();
            let has_result = job.result.lock().unwrap().is_some();

            if has_result && !job.client_present() {
                expired.push(entry.key().clone());
            }
        }

        for id in expired {
            jobs.remove(&id);
        }
    });
}

async fn dispatch(router: &Switch, job: Job) -> JobResult {
    match router.dispatch(&job).await {
        Ok(TargetResult::Complete(result)) => result,
        Ok(TargetResult::Reject { reason }) => JobResult::Error {
            message: format!("All pipelines rejected: {}", reason),
        },
        Err(crate::actions::ActionError::Cancelled) => JobResult::Cancelled,
        Err(crate::actions::ActionError::ClientGone) => JobResult::ClientGone,
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
        let handle = bits.submit(Job::new(json!({"class": "od"})));
        match bits.poll(&handle.id, None).await {
            PollOutcome::Ready(JobResult::Error { .. }) => {}
            r => panic!("Expected error for empty pipeline, got: {:?}", r),
        }
    }
}
