use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::TryStreamExt;

use crate::db::{BrokerLeaseRecord, ClaimResult, DbError, PersistenceStore};
use crate::result::JobResult;
use crate::{Bits, PollOutcome};

pub(crate) enum LeaseLookup {
    Active(BrokerLeaseRecord),
    MissingOrExpired,
    Unknown,
}

impl Bits {
    pub(crate) async fn claim_with_backoff(
        &self,
        store: &Arc<dyn PersistenceStore>,
        id: &str,
        expected_owner: &str,
        timeout: Option<Duration>,
    ) -> Result<ClaimResult, DbError> {
        let budget = timeout
            .unwrap_or(Duration::from_secs(2))
            .min(Duration::from_secs(2));
        let deadline = Instant::now() + budget;
        let mut delay = Duration::from_millis(100);

        loop {
            match store
                .claim_if_owner(id, expected_owner, &self.broker_id)
                .await
            {
                Ok(result) => return Ok(result),
                Err(DbError::Conflict(message)) => return Err(DbError::Conflict(message)),
                Err(DbError::Backend(message)) => {
                    let now = Instant::now();
                    if now >= deadline {
                        return Err(DbError::Backend(message));
                    }
                    let remaining = deadline.saturating_duration_since(now);
                    let sleep_for = delay.min(remaining);
                    if sleep_for.is_zero() {
                        return Err(DbError::Backend(message));
                    }
                    tracing::warn!(
                        job.id = %id,
                        backoff_ms = sleep_for.as_millis(),
                        "claim backend error; retrying"
                    );
                    tokio::time::sleep(sleep_for).await;
                    delay = delay.saturating_mul(2).min(Duration::from_secs(1));
                }
            }
        }
    }

    pub(crate) async fn lookup_owner_lease(&self, owner_broker_id: &str) -> LeaseLookup {
        let Some(store) = &self.job_store else {
            return LeaseLookup::Unknown;
        };
        match store.get_broker_lease(owner_broker_id).await {
            Ok(Some(lease)) if lease.lease_until > chrono::Utc::now() => LeaseLookup::Active(lease),
            Ok(_) => LeaseLookup::MissingOrExpired,
            Err(err) => {
                tracing::warn!(owner = %owner_broker_id, error = %err, "broker lease lookup failed");
                LeaseLookup::Unknown
            }
        }
    }

    pub(crate) async fn try_proxy_with_lease(
        &self,
        lease: &BrokerLeaseRecord,
        id: &str,
        timeout: Option<Duration>,
    ) -> Option<PollOutcome> {
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

        if status == reqwest::StatusCode::SEE_OTHER
            || status == reqwest::StatusCode::TEMPORARY_REDIRECT
        {
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
            return Some(PollOutcome::Pending { id: id.to_string() });
        }

        None
    }

    pub(crate) fn start_broker_lease_heartbeat(&self, broker_lease_ttl: Duration) {
        let Some(store) = &self.job_store else {
            return;
        };
        let store = Arc::clone(store);
        let broker_id = self.broker_id.clone();
        let base_url = self.internal_poll_base_url.clone();
        std::thread::spawn(move || {
            let tick = broker_lease_ttl
                .div_f64(2.0)
                .max(Duration::from_millis(100));
            let runtime = match tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
            {
                Ok(runtime) => runtime,
                Err(err) => {
                    tracing::warn!(broker_id = %broker_id, error = %err, "failed to start broker lease heartbeat runtime");
                    return;
                }
            };

            loop {
                if let Err(err) = runtime.block_on(store.upsert_broker_lease(
                    &broker_id,
                    &base_url,
                    broker_lease_ttl,
                )) {
                    tracing::warn!(broker_id = %broker_id, error = %err, "broker lease upsert failed");
                }
                std::thread::sleep(tick);
            }
        });
    }
}

pub(crate) fn owner_from_job_id(job_id: &str) -> Option<&str> {
    let (owner, _suffix) = job_id.split_once('~')?;
    if owner.is_empty() { None } else { Some(owner) }
}
