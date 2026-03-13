pub mod memory;
#[cfg(feature = "tikv")]
pub mod tikv;

use std::time::Duration;

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistentJobRecord {
    pub job_id: String,
    pub broker_id: String,
    pub original_request: Value,
    pub user: Value,
    pub metadata: Value,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerLeaseRecord {
    pub broker_id: String,
    pub internal_poll_base_url: String,
    pub lease_until: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub enum ClaimResult {
    NotFound,
    Active { owner_broker_id: String },
    Claimed(PersistentJobRecord),
}

#[derive(Debug, Clone)]
pub enum DbError {
    Conflict(String),
    Backend(String),
}

impl std::fmt::Display for DbError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Conflict(message) => write!(f, "conflict: {message}"),
            Self::Backend(message) => write!(f, "backend error: {message}"),
        }
    }
}

impl std::error::Error for DbError {}

#[async_trait]
/// Durable storage contract for job records.
///
/// `JobStore` is the persistence boundary used by the broker runtime when
/// threshold persistence is enabled. Implementations are expected to provide
/// atomic ownership transitions so that only one broker can reclaim a job when
/// the previous owner is considered dead.
///
/// Behavioral expectations:
/// - `upsert_job` writes the latest durable representation of a job.
/// - `delete_job` removes the durable record after terminal completion.
/// - `claim_if_owner` performs an ownership-aware compare-and-set operation.
///
/// `claim_if_owner` must be safe under concurrent callers:
/// - return `ClaimResult::Claimed` when the job currently belongs to
///   `expected_owner_broker_id` and ownership is switched to
///   `claimant_broker_id` atomically,
/// - return `ClaimResult::Active` with the current owner when ownership no
///   longer matches `expected_owner_broker_id`,
/// - return `ClaimResult::NotFound` when no durable record exists.
///
/// Implementations should surface backend transport/storage failures as
/// `DbError::Backend` and optimistic write races as `DbError::Conflict`.
pub trait JobStore: Send + Sync {
    /// Insert or replace a durable job record.
    ///
    /// This is used by delayed persistence once a job has been in-flight longer
    /// than the configured threshold. Implementations may overwrite existing
    /// records for the same `job_id`.
    async fn upsert_job(&self, record: PersistentJobRecord) -> Result<(), DbError>;

    /// Delete a durable job record by id.
    ///
    /// Called after a job reaches terminal state and the broker no longer needs
    /// recovery metadata for this `job_id`.
    async fn delete_job(&self, job_id: &str) -> Result<(), DbError>;

    /// Attempt to claim a durable job only if the expected owner still matches.
    ///
    /// This operation is the core reclaim primitive used after owner-lease
    /// expiry. The check and ownership update should be atomic.
    ///
    /// Parameters:
    /// - `job_id`: durable job identifier to reclaim.
    /// - `expected_owner_broker_id`: owner observed by the caller before claim.
    /// - `claimant_broker_id`: broker attempting to become new owner.
    ///
    /// Returns:
    /// - `ClaimResult::Claimed` when claim succeeds (or claimant already owns).
    /// - `ClaimResult::Active` when another owner is currently authoritative.
    /// - `ClaimResult::NotFound` when no durable record exists.
    async fn claim_if_owner(
        &self,
        job_id: &str,
        expected_owner_broker_id: &str,
        claimant_broker_id: &str,
    ) -> Result<ClaimResult, DbError>;
}

#[async_trait]
/// Lease registry contract for broker liveness and endpoint discovery.
///
/// `BrokerLeaseStore` is used to map `broker_id -> internal poll endpoint` and
/// to decide whether reclaim is allowed. Reclaim logic treats missing/expired
/// leases as owner unavailable.
///
/// Implementations should store lease expiry based on `ttl` at write time and
/// return the most recent known lease record on read.
pub trait BrokerLeaseStore: Send + Sync {
    /// Insert or renew a broker lease.
    ///
    /// `ttl` defines how long the lease stays valid from the write timestamp.
    /// Brokers usually renew at `ttl / 2` intervals.
    async fn upsert_broker_lease(
        &self,
        broker_id: &str,
        internal_poll_base_url: &str,
        ttl: Duration,
    ) -> Result<(), DbError>;

    /// Fetch the broker lease if present.
    ///
    /// Returns `Ok(None)` when the broker has no recorded lease. Callers are
    /// responsible for evaluating `lease_until` against current time.
    async fn get_broker_lease(&self, broker_id: &str)
    -> Result<Option<BrokerLeaseRecord>, DbError>;

    /// Remove a broker lease record.
    ///
    /// Used on graceful shutdown (best-effort) and by tests.
    async fn delete_broker_lease(&self, broker_id: &str) -> Result<(), DbError>;
}

/// Composite persistence capability used by the broker runtime.
///
/// Any backend that implements both `JobStore` and `BrokerLeaseStore`
/// automatically implements `PersistenceStore` via the blanket impl below.
pub trait PersistenceStore: JobStore + BrokerLeaseStore {}

impl<T: JobStore + BrokerLeaseStore> PersistenceStore for T {}

pub async fn durable_job_present(
    store: &dyn PersistenceStore,
    job_id: &str,
) -> Result<bool, DbError> {
    match store
        .claim_if_owner(job_id, "__never_expected__", "__presence_probe__")
        .await?
    {
        ClaimResult::NotFound => Ok(false),
        ClaimResult::Active { .. } | ClaimResult::Claimed(_) => Ok(true),
    }
}
