// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use futures::TryStreamExt;

use crate::bits::PENDING_STATUS_HEADER;
use crate::db::{BrokerLeaseRecord, ClaimResult, DbError, PersistenceStore, durable_job_present};
use crate::result::JobResult;
use crate::{Bits, PendingStatus, PollOutcome};

pub(crate) enum LeaseLookup {
    Active(BrokerLeaseRecord),
    MissingOrExpired,
    Unknown,
}

impl Bits {
    fn lease_grace_duration(lease: &BrokerLeaseRecord) -> chrono::Duration {
        let ttl = (lease.lease_until - lease.updated_at)
            .to_std()
            .unwrap_or_default();
        let grace = ttl.div_f64(10.0).min(Duration::from_secs(1));
        chrono::Duration::from_std(grace).unwrap_or_else(|_| chrono::Duration::zero())
    }

    fn lease_is_active_with_grace(lease: &BrokerLeaseRecord) -> bool {
        lease.lease_until + Self::lease_grace_duration(lease) > chrono::Utc::now()
    }

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
                .claim_if_owner(id, expected_owner, &self.submit_context.broker_id)
                .await
            {
                Ok(result) => return Ok(result),
                Err(DbError::Conflict(message)) => return Err(DbError::Conflict(message)),
                Err(err @ DbError::SlotExhausted { .. }) => return Err(err),
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
                        request.id = %id,
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
        let Some(store) = &self.submit_context.job_store else {
            return LeaseLookup::Unknown;
        };
        match store.get_broker_lease(owner_broker_id).await {
            Ok(Some(lease)) if Self::lease_is_active_with_grace(&lease) => {
                LeaseLookup::Active(lease)
            }
            Ok(_) => LeaseLookup::MissingOrExpired,
            Err(err) => {
                tracing::warn!(owner = %owner_broker_id, error = %err, "broker lease lookup failed");
                LeaseLookup::Unknown
            }
        }
    }

    async fn owner_not_found_outcome(&self, id: &str) -> PollOutcome {
        let Some(store) = &self.submit_context.job_store else {
            return PollOutcome::NotFound;
        };

        match durable_job_present(store.as_ref(), id).await {
            Ok(true) => {
                tracing::warn!(request.id = %id, "proxy owner returned 404 while durable record still exists");
                PollOutcome::queued(id)
            }
            Ok(false) => PollOutcome::NotFound,
            Err(err) => {
                tracing::warn!(request.id = %id, error = %err, "failed to verify durable record after owner 404");
                PollOutcome::queued(id)
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
        let response = match self.internal_client.get(&url).timeout(timeout).send().await {
            Ok(resp) => resp,
            Err(err) if err.is_timeout() => {
                // Timeouts are expected when the owner broker is still
                // long-polling and the caller's poll budget expires first.
                tracing::debug!(
                    request.id = %id,
                    owner_broker = %lease.broker_id,
                    error = %err,
                    "proxy request to owner broker timed out"
                );
                return None;
            }
            Err(err) => {
                tracing::warn!(
                    request.id = %id,
                    owner_broker = %lease.broker_id,
                    url = %url,
                    error = %err,
                    "proxy request to owner broker failed"
                );
                return None;
            }
        };

        let status = response.status();
        let pending_status = response
            .headers()
            .get(PENDING_STATUS_HEADER)
            .and_then(|value| value.to_str().ok())
            .and_then(PendingStatus::from_header)
            .unwrap_or(PendingStatus::Queued);
        if status == reqwest::StatusCode::OK {
            let content_type = match response.headers().get(reqwest::header::CONTENT_TYPE) {
                Some(value) => match value.to_str() {
                    Ok(s) => s.to_string(),
                    Err(err) => {
                        tracing::warn!(
                            request.id = %id,
                            error = %err,
                            "content-type header has invalid value; using default"
                        );
                        "application/octet-stream".to_string()
                    }
                },
                None => "application/octet-stream".to_string(),
            };
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
            || status == reqwest::StatusCode::FOUND
            || status == reqwest::StatusCode::PERMANENT_REDIRECT
        {
            let location = response
                .headers()
                .get(reqwest::header::LOCATION)
                .and_then(|v| v.to_str().ok())
                .unwrap_or_default()
                .to_string();
            if location.is_empty() {
                tracing::warn!(request.id = %id, status = %status, "proxy redirect with empty Location");
                return Some(PollOutcome::pending(id, pending_status));
            }
            let last_segment = location
                .split('?')
                .next()
                .unwrap_or(&location)
                .split('/')
                .next_back()
                .unwrap_or_default();
            if last_segment == id {
                return Some(PollOutcome::pending(id, pending_status));
            }
            // Recover the object's content metadata from the owner broker's
            // response headers so the v1 redirect body keeps parity across a
            // cross-broker proxy hop.
            let content_type = response
                .headers()
                .get("x-polytope-content-type")
                .and_then(|v| v.to_str().ok())
                .map(str::to_string);
            let content_length = response
                .headers()
                .get("x-polytope-content-length")
                .and_then(|v| v.to_str().ok())
                .and_then(|s| s.parse::<u64>().ok());
            return Some(PollOutcome::Ready(JobResult::Redirect {
                location,
                message: "proxied redirect".into(),
                content_type,
                content_length,
            }));
        }

        if status == reqwest::StatusCode::NOT_FOUND {
            return Some(self.owner_not_found_outcome(id).await);
        }
        if status == reqwest::StatusCode::BAD_REQUEST {
            return Some(PollOutcome::Ready(JobResult::Error {
                message: response.text().await.unwrap_or_default(),
            }));
        }
        if status == reqwest::StatusCode::GONE {
            return Some(PollOutcome::Ready(JobResult::Cancelled));
        }
        if status == reqwest::StatusCode::UNAUTHORIZED || status == reqwest::StatusCode::FORBIDDEN {
            tracing::warn!(request.id = %id, status = %status, "proxy auth error from owner");
            return Some(PollOutcome::queued(id));
        }
        if status == reqwest::StatusCode::TOO_MANY_REQUESTS {
            tracing::warn!(request.id = %id, "proxy throttled by owner");
            return Some(PollOutcome::queued(id));
        }
        if status.is_server_error() {
            return Some(PollOutcome::queued(id));
        }

        tracing::warn!(request.id = %id, status = %status, "proxy received unexpected status");
        Some(PollOutcome::queued(id))
    }

    pub(crate) fn start_broker_lease_heartbeat(
        &self,
        broker_lease_ttl: Duration,
    ) -> Option<std::thread::JoinHandle<()>> {
        let store = self.submit_context.job_store.as_ref()?;
        let store = Arc::clone(store);
        let broker_id = self.submit_context.broker_id.clone();
        let shutdown = self.shutdown.clone();
        let in_flight = self.submit_context.in_flight.clone();
        let base_url = self.internal_poll_base_url.clone();
        Some(std::thread::spawn(move || {
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

            let drain_and_delete = || {
                let deadline = Instant::now() + broker_lease_ttl;
                while in_flight.load(Ordering::Acquire) > 0 && Instant::now() < deadline {
                    std::thread::sleep(Duration::from_millis(50));
                }
                if let Err(err) = runtime.block_on(store.delete_broker_lease(&broker_id)) {
                    tracing::debug!(broker_id = %broker_id, error = %err, "lease cleanup on shutdown failed");
                }
            };

            loop {
                if shutdown.is_stopped() {
                    drain_and_delete();
                    return;
                }

                match runtime.block_on(store.upsert_broker_lease(
                    &broker_id,
                    &base_url,
                    broker_lease_ttl,
                )) {
                    Ok(()) => {}
                    Err(err) => {
                        tracing::warn!(broker_id = %broker_id, error = %err, "broker lease upsert failed; retrying");
                        shutdown.wait_timeout(Duration::from_millis(500).min(tick));
                        if shutdown.is_stopped() {
                            drain_and_delete();
                            return;
                        }
                        if let Err(retry_err) = runtime.block_on(store.upsert_broker_lease(
                            &broker_id,
                            &base_url,
                            broker_lease_ttl,
                        )) {
                            tracing::warn!(broker_id = %broker_id, error = %retry_err, "broker lease retry also failed");
                        }
                    }
                }

                shutdown.wait_timeout(tick);
            }
        }))
    }
}

pub(crate) fn decode_job_id(
    job_id: &str,
) -> Result<crate::request_id::DecodedId, crate::request_id::DecodeError> {
    crate::request_id::decode(job_id)
}

pub(crate) fn slot_from_job_id(job_id: &str) -> Result<String, crate::request_id::DecodeError> {
    let decoded = decode_job_id(job_id)?;
    Ok(format!(
        "{}-{}-{}",
        decoded.site, decoded.env, decoded.broker_slot
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::request_id::{DecodeError, encode_with_fixed_random};
    use chrono::{DateTime, TimeZone, Utc};

    fn epoch() -> DateTime<Utc> {
        DateTime::parse_from_rfc3339(crate::request_id::CUSTOM_EPOCH)
            .unwrap()
            .with_timezone(&Utc)
    }

    #[test]
    fn request_id_decode_returns_decoded_fields() {
        let id = encode_with_fixed_random(
            "bol",
            "dev",
            42,
            Utc.with_ymd_and_hms(2025, 1, 1, 0, 1, 2).unwrap(),
            [0x01, 0x23, 0x45, 0x67, 0x89],
        )
        .unwrap();

        let decoded = decode_job_id(&id).unwrap();

        assert_eq!(decoded.version, 1);
        assert_eq!(decoded.site, "bol");
        assert_eq!(decoded.env, "dev");
        assert_eq!(
            decoded.timestamp,
            Utc.with_ymd_and_hms(2025, 1, 1, 0, 1, 2).unwrap()
        );
        assert_eq!(decoded.broker_slot, 42);
        assert_eq!(decoded.random, [0x01, 0x23, 0x45, 0x67, 0x89]);
    }

    #[test]
    fn request_id_decode_slot_returns_stable_broker_identity() {
        let id = encode_with_fixed_random("bol", "dev", 42, epoch(), [0, 1, 2, 3, 4]).unwrap();

        assert_eq!(slot_from_job_id(&id).unwrap(), "bol-dev-42");
    }

    #[test]
    fn request_id_decode_rejects_legacy_owner_tilde_uuid() {
        assert!(decode_job_id("bol-dev-42~550e8400-e29b-41d4-a716-446655440000").is_err());
    }

    #[test]
    fn request_id_decode_rejects_malformed_crockford() {
        let mut id = encode_with_fixed_random("bol", "dev", 42, epoch(), [0, 1, 2, 3, 4]).unwrap();
        id.replace_range(0..1, "i");

        assert!(decode_job_id(&id).is_err());
    }

    #[test]
    fn request_id_decode_preserves_unknown_version_error() {
        let mut id = encode_with_fixed_random("bol", "dev", 42, epoch(), [0, 1, 2, 3, 4]).unwrap();
        id.replace_range(0..2, "02");

        assert_eq!(
            decode_job_id(&id),
            Err(DecodeError::UnknownVersion { version: 2 })
        );
    }

    #[test]
    fn lease_stays_active_briefly_past_expiry() {
        let updated_at = chrono::Utc::now();
        let lease = BrokerLeaseRecord {
            broker_id: "broker-a".into(),
            internal_poll_base_url: "http://127.0.0.1:8080/job".into(),
            updated_at,
            lease_until: updated_at + chrono::Duration::milliseconds(500),
        };

        assert!(Bits::lease_is_active_with_grace(&lease));
        let grace = Bits::lease_grace_duration(&lease);
        assert_eq!(grace, chrono::Duration::milliseconds(50));
    }

    #[test]
    fn lease_grace_is_capped() {
        let updated_at = chrono::Utc::now();
        let lease = BrokerLeaseRecord {
            broker_id: "broker-a".into(),
            internal_poll_base_url: "http://127.0.0.1:8080/job".into(),
            updated_at,
            lease_until: updated_at + chrono::Duration::seconds(30),
        };

        assert_eq!(
            Bits::lease_grace_duration(&lease),
            chrono::Duration::seconds(1)
        );
    }
}
