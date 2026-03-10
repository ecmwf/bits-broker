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
pub trait JobStore: Send + Sync {
    async fn upsert_job(&self, record: PersistentJobRecord) -> Result<(), DbError>;
    async fn delete_job(&self, job_id: &str) -> Result<(), DbError>;
    async fn claim_if_owner(
        &self,
        job_id: &str,
        expected_owner_broker_id: &str,
        claimant_broker_id: &str,
    ) -> Result<ClaimResult, DbError>;
}

#[async_trait]
pub trait BrokerLeaseStore: Send + Sync {
    async fn upsert_broker_lease(
        &self,
        broker_id: &str,
        internal_poll_base_url: &str,
        ttl: Duration,
    ) -> Result<(), DbError>;
    async fn get_broker_lease(&self, broker_id: &str) -> Result<Option<BrokerLeaseRecord>, DbError>;
    async fn delete_broker_lease(&self, broker_id: &str) -> Result<(), DbError>;
}

pub trait PersistenceStore: JobStore + BrokerLeaseStore {}

impl<T: JobStore + BrokerLeaseStore> PersistenceStore for T {}
