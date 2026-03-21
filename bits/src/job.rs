use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, LazyLock, Mutex};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Notify;

use crate::db::PersistentJobRecord;
use crate::result::JobResult;

static EPOCH: LazyLock<Instant> = LazyLock::new(Instant::now);

pub(crate) fn instant_to_nanos(instant: Instant) -> u64 {
    instant.saturating_duration_since(*EPOCH).as_nanos() as u64
}

fn nanos_to_instant(nanos: u64) -> Instant {
    *EPOCH + Duration::from_nanos(nanos)
}

fn default_cancelled() -> Arc<AtomicBool> {
    Arc::new(AtomicBool::new(false))
}

fn default_active_pollers() -> Arc<AtomicUsize> {
    Arc::new(AtomicUsize::new(0))
}

fn default_reconnect_deadline_nanos() -> Arc<AtomicU64> {
    Arc::new(AtomicU64::new(instant_to_nanos(Instant::now())))
}

fn default_persisted() -> AtomicBool {
    AtomicBool::new(false)
}

#[derive(Debug, Serialize, Deserialize)]
/// A unit of work flowing through checks, transforms, and a terminal target.
pub struct Job {
    /// Unique identifier for the job.
    pub id: String,
    /// The original request as submitted by the client. Never modified after creation.
    /// Used as the restart point on broker recovery.
    pub original_request: Value,
    /// The working request, mutated by transform actions as the job flows through the pipeline.
    pub request: Value,
    /// Arbitrary user-scoped context carried alongside the request.
    pub user: Value,
    /// Creation timestamp used for routing and persistence metadata.
    pub created_at: DateTime<Utc>,
    /// Free-form metadata available to actions and persistence backends.
    pub metadata: Value,
    /// Set by `Bits::cancel()`. Checked in the pipeline before each action.
    #[serde(skip, default = "default_cancelled")]
    pub(crate) cancelled: Arc<AtomicBool>,
    /// Number of active `Bits::poll()` calls for this job (refcount, not boolean).
    #[serde(skip, default = "default_active_pollers")]
    pub(crate) active_pollers: Arc<AtomicUsize>,
    /// Deadline by which the client must reconnect after a poll completes.
    /// Stored as nanoseconds since process-epoch for lock-free access.
    #[serde(skip, default = "default_reconnect_deadline_nanos")]
    pub(crate) reconnect_deadline_nanos: Arc<AtomicU64>,
    /// True once a durable record has been successfully written for this job.
    /// Used by poll and sweeper to know whether durable cleanup is needed.
    #[serde(skip, default = "default_persisted")]
    pub(crate) persisted: AtomicBool,
    /// Result slot written by the dispatch task and consumed by `Bits::poll()`.
    #[serde(skip)]
    pub(crate) result: Mutex<Option<JobResult>>,
    /// Notifies waiting pollers when the result is ready.
    #[serde(skip)]
    pub(crate) notify: Notify,
}

impl Job {
    /// Creates a new job with a generated id.
    pub fn new(request: Value) -> Self {
        Self::new_with_id(uuid::Uuid::new_v4().to_string(), request)
    }

    /// Creates a new job with an explicit id.
    pub fn new_with_id(id: String, request: Value) -> Self {
        Self {
            id,
            original_request: request.clone(),
            request,
            user: serde_json::json!({}),
            created_at: Utc::now(),
            metadata: serde_json::json!({}),
            cancelled: default_cancelled(),
            active_pollers: default_active_pollers(),
            reconnect_deadline_nanos: default_reconnect_deadline_nanos(),
            persisted: AtomicBool::new(false),
            result: Mutex::new(None),
            notify: Notify::new(),
        }
    }

    /// Reconstructs a job from a durable persistence record.
    pub fn restore(record: PersistentJobRecord) -> Self {
        Self {
            id: record.job_id,
            original_request: record.original_request.clone(),
            request: record.original_request,
            user: record.user,
            created_at: record.created_at,
            metadata: record.metadata,
            cancelled: default_cancelled(),
            active_pollers: default_active_pollers(),
            reconnect_deadline_nanos: default_reconnect_deadline_nanos(),
            persisted: AtomicBool::new(false),
            result: Mutex::new(None),
            notify: Notify::new(),
        }
    }

    /// Returns whether cancellation has been requested for this job.
    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Acquire)
    }

    /// Returns true if the client is currently polling or is within the reconnect window.
    pub fn client_present(&self) -> bool {
        self.active_pollers.load(Ordering::Acquire) > 0
            || Instant::now()
                < nanos_to_instant(self.reconnect_deadline_nanos.load(Ordering::Acquire))
    }

    pub(crate) fn set_reconnect_deadline(&self, deadline: Instant) {
        self.reconnect_deadline_nanos
            .store(instant_to_nanos(deadline), Ordering::Release);
    }
}

impl Clone for Job {
    fn clone(&self) -> Self {
        Self {
            // Domain fields — deep copy.
            id: self.id.clone(),
            original_request: self.original_request.clone(),
            request: self.request.clone(),
            user: self.user.clone(),
            created_at: self.created_at,
            metadata: self.metadata.clone(),
            // Lifecycle arcs — share the same underlying state so the pipeline
            // clone can still read cancellation / client-presence correctly.
            cancelled: self.cancelled.clone(),
            active_pollers: self.active_pollers.clone(),
            reconnect_deadline_nanos: self.reconnect_deadline_nanos.clone(),
            // Result slot, notifier, and persisted flag are not shared — the
            // pipeline clone never writes results or persistence state.
            persisted: AtomicBool::new(false),
            result: Mutex::new(None),
            notify: Notify::new(),
        }
    }
}

#[cfg(test)]
impl Job {
    pub(crate) fn set_reconnect_deadline_for_test(&self, deadline: Instant) {
        self.set_reconnect_deadline(deadline);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_job_creation() {
        let request = json!({"class": "od", "stream": "oper"});
        let job = Job::new(request.clone());
        assert_eq!(job.original_request, request);
        assert_eq!(job.request, request);
    }
}
