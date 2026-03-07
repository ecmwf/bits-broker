use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Instant;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::Notify;

use crate::result::JobResult;

fn default_cancelled() -> Arc<AtomicBool> {
    Arc::new(AtomicBool::new(false))
}

fn default_client_connected() -> Arc<AtomicBool> {
    Arc::new(AtomicBool::new(false))
}

fn default_reconnect_deadline() -> Arc<Mutex<Instant>> {
    // Initialise to now (already expired) — no client assumed on deserialisation.
    Arc::new(Mutex::new(Instant::now()))
}


#[derive(Debug, Serialize, Deserialize)]
pub struct Job {
    pub id: String,
    /// The original request as submitted by the client. Never modified after creation.
    /// Used as the restart point on broker recovery.
    pub original_request: Value,
    /// The working request, mutated by transform actions as the job flows through the pipeline.
    pub request: Value,
    pub user: Value,
    pub created_at: DateTime<Utc>,
    pub metadata: Value,
    pub persistent: bool,
    /// Set by `Bits::cancel()`. Checked in the pipeline before each action.
    #[serde(skip, default = "default_cancelled")]
    pub(crate) cancelled: Arc<AtomicBool>,
    /// True while a `Bits::poll()` call is in flight for this job.
    #[serde(skip, default = "default_client_connected")]
    pub(crate) client_connected: Arc<AtomicBool>,
    /// Deadline by which the client must reconnect after a poll completes.
    #[serde(skip, default = "default_reconnect_deadline")]
    pub(crate) reconnect_deadline: Arc<Mutex<Instant>>,
    /// Result slot written by the dispatch task and consumed by `Bits::poll()`.
    #[serde(skip)]
    pub(crate) result: Mutex<Option<JobResult>>,
    /// Notifies waiting pollers when the result is ready.
    #[serde(skip)]
    pub(crate) notify: Notify,
}

impl Job {
    pub fn new(request: Value) -> Self {
        Self {
            id: uuid::Uuid::new_v4().to_string(),
            original_request: request.clone(),
            request,
            user: serde_json::json!({}),
            created_at: Utc::now(),
            metadata: serde_json::json!({}),
            persistent: false,
            cancelled: default_cancelled(),
            client_connected: default_client_connected(),
            reconnect_deadline: default_reconnect_deadline(),
            result: Mutex::new(None),
            notify: Notify::new(),
        }
    }

    pub fn is_cancelled(&self) -> bool {
        self.cancelled.load(Ordering::Relaxed)
    }

    /// Returns true if the client is currently polling or is within the reconnect window.
    pub fn client_present(&self) -> bool {
        self.client_connected.load(Ordering::Relaxed)
            || Instant::now() < *self.reconnect_deadline.lock().unwrap()
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
            persistent: self.persistent,
            // Lifecycle arcs — share the same underlying state so the pipeline
            // clone can still read cancellation / client-presence correctly.
            cancelled: self.cancelled.clone(),
            client_connected: self.client_connected.clone(),
            reconnect_deadline: self.reconnect_deadline.clone(),
            // Result slot and notifier are not shared — the pipeline clone
            // never writes results, so it gets fresh empty values.
            result: Mutex::new(None),
            notify: Notify::new(),
        }
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
        assert!(!job.persistent);
    }
}
