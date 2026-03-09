use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use dashmap::DashMap;
use futures::TryStreamExt;
use tracing::Instrument;

use crate::actions::{TargetAction, TargetResult};
use crate::config::parse_config;
use crate::db::{ClaimResult, PersistenceStore};
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
    /// Job owner was unavailable and no durable record exists.
    JobLost,
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
    broker_id: String,
    internal_poll_base_url: String,
    internal_poll_timeout: Duration,
    job_store: Option<Arc<dyn PersistenceStore>>,
    lock_ttl: Duration,
    internal_client: reqwest::Client,
}

impl Bits {
    pub fn from_config(config: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let parsed = parse_config(config)?;
        let sweep_interval = parsed.sweep_interval.unwrap_or(DEFAULT_SWEEP_INTERVAL);
        let bits = Bits {
            router: Arc::new(parsed.router),
            jobs: Arc::new(DashMap::new()),
            broker_id: parsed.broker_id,
            internal_poll_base_url: parsed.internal_poll_base_url,
            internal_poll_timeout: parsed.internal_poll_timeout,
            job_store: parsed.job_store,
            lock_ttl: parsed.lock_ttl,
            internal_client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()?,
        };

        start_sweeper(bits.jobs.clone(), sweep_interval);
        bits.start_broker_lease_heartbeat(parsed.broker_lease_ttl);

        Ok(bits)
    }

    /// Submit a job for async processing. Returns immediately with a handle; the job runs in the background.
    /// Use [`Bits::poll`] to retrieve the result.
    pub fn submit(&self, mut job: Job) -> JobHandle {
        if owner_from_job_id(&job.id).is_none() {
            job.id = self.new_job_id();
        }
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
        if let Some(outcome) = self.poll_local(id, timeout).await {
            return outcome;
        }

        let Some(owner) = owner_from_job_id(id) else {
            return PollOutcome::NotFound;
        };

        if owner != self.broker_id {
            if let Some(outcome) = self.try_proxy(owner, id, timeout).await {
                return outcome;
            }
        }

        let Some(store) = &self.job_store else {
            return PollOutcome::NotFound;
        };

        let claim_result = if owner == self.broker_id {
            store.try_claim_expired(id, &self.broker_id, self.lock_ttl).await
        } else {
            store.force_claim(id, &self.broker_id, self.lock_ttl).await
        };
        match claim_result {
            Ok(ClaimResult::Claimed(record)) => {
                self.submit(Job::restore(record));
                PollOutcome::Pending { id: id.to_string() }
            }
            Ok(ClaimResult::Active { owner_broker_id }) => {
                if owner_broker_id == self.broker_id {
                    self.poll_local(id, timeout)
                        .await
                        .unwrap_or(PollOutcome::Pending { id: id.to_string() })
                } else {
                    self.try_proxy(&owner_broker_id, id, timeout)
                        .await
                        .unwrap_or(PollOutcome::Pending { id: id.to_string() })
                }
            }
            Ok(ClaimResult::NotFound) => {
                if owner == self.broker_id {
                    PollOutcome::NotFound
                } else {
                    PollOutcome::JobLost
                }
            }
            Err(err) => {
                tracing::warn!(job.id = %id, error = %err, "claim failed");
                PollOutcome::Pending { id: id.to_string() }
            }
        }
    }

    async fn poll_local(&self, id: &str, timeout: Option<Duration>) -> Option<PollOutcome> {
        let Some(job) = self.jobs.get(id).map(|r| r.clone()) else {
            return None;
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
            return Some(PollOutcome::Ready(result));
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

        Some(outcome)
    }

    fn new_job_id(&self) -> String {
        format!("{}~{}", self.broker_id, uuid::Uuid::new_v4())
    }

    async fn try_proxy(&self, owner_broker_id: &str, id: &str, timeout: Option<Duration>) -> Option<PollOutcome> {
        let store = self.job_store.as_ref()?;
        let lease = match store.get_broker_lease(owner_broker_id).await {
            Ok(Some(lease)) => lease,
            Ok(None) => return None,
            Err(err) => {
                tracing::warn!(owner = %owner_broker_id, error = %err, "broker lease lookup failed");
                return None;
            }
        };
        if lease.lease_until <= chrono::Utc::now() {
            return None;
        }

        let timeout = timeout.unwrap_or(self.internal_poll_timeout);
        let base = lease.internal_poll_base_url.trim_end_matches('/');
        let url = format!("{base}/{id}");
        let response = self
            .internal_client
            .get(url)
            .timeout(timeout)
            .send()
            .await
            .ok()?;

        let status = response.status();
        if status == reqwest::StatusCode::OK {
            let content_type = response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("application/octet-stream")
                .to_string();
            let size = response.content_length().map(|n| n as i64).unwrap_or(-1);
            let stream = Box::new(
                response
                    .bytes_stream()
                    .map_err(|e| std::io::Error::other(e.to_string())),
            );
            return Some(PollOutcome::Ready(JobResult::Success {
                content_type,
                size,
                stream,
            }));
        }

        if status == reqwest::StatusCode::SEE_OTHER || status == reqwest::StatusCode::TEMPORARY_REDIRECT {
            let location = response
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .to_string();
            if location.contains(id) {
                return Some(PollOutcome::Pending { id: id.to_string() });
            }
            return Some(PollOutcome::Ready(JobResult::Redirect {
                location,
                message: "proxied redirect".into(),
            }));
        }

        if status == reqwest::StatusCode::NOT_FOUND {
            return Some(PollOutcome::NotFound);
        }
        if status == reqwest::StatusCode::BAD_REQUEST {
            return Some(PollOutcome::Ready(JobResult::Error {
                message: response.text().await.unwrap_or_default(),
            }));
        }
        if status == reqwest::StatusCode::GONE {
            return Some(PollOutcome::Ready(JobResult::Cancelled));
        }
        if status.is_server_error() {
            return Some(PollOutcome::Ready(JobResult::Failed {
                reason: response.text().await.unwrap_or_else(|_| format!("upstream status {status}")),
            }));
        }

        None
    }

    fn start_broker_lease_heartbeat(&self, broker_lease_ttl: Duration) {
        let Some(store) = &self.job_store else {
            return;
        };
        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            tracing::warn!(
                broker_id = %self.broker_id,
                "no runtime available; skipping broker lease heartbeat"
            );
            return;
        };
        let store = Arc::clone(store);
        let broker_id = self.broker_id.clone();
        let base_url = self.internal_poll_base_url.clone();
        handle.spawn(async move {
            let tick = broker_lease_ttl.div_f64(2.0).max(Duration::from_millis(100));
            loop {
                if let Err(err) = store
                    .upsert_broker_lease(&broker_id, &base_url, broker_lease_ttl)
                    .await
                {
                    tracing::warn!(broker_id = %broker_id, error = %err, "broker lease upsert failed");
                }
                tokio::time::sleep(tick).await;
            }
        });
    }
}

fn owner_from_job_id(job_id: &str) -> Option<&str> {
    let (owner, _suffix) = job_id.split_once('~')?;
    if owner.is_empty() {
        None
    } else {
        Some(owner)
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
