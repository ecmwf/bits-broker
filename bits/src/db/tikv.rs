use async_trait::async_trait;
use chrono::Utc;
use std::time::Duration;
use tokio::sync::OnceCell;

use crate::db::{
    BrokerLeaseRecord, BrokerLeaseStore, ClaimResult, DbError, JobStore, PersistentJobRecord,
};

fn is_write_conflict(err: &tikv_client::Error) -> bool {
    match err {
        tikv_client::Error::KeyError(key_err) => key_err.conflict.is_some(),
        tikv_client::Error::MultipleKeyErrors(errs) => errs.iter().any(is_write_conflict),
        tikv_client::Error::ExtractedErrors(errs) => errs.iter().any(is_write_conflict),
        _ => false,
    }
}

const JOB_PREFIX: &str = "jobs/";
const BROKER_PREFIX: &str = "brokers/";

pub struct TiKvStore {
    endpoints: Vec<String>,
    connect_timeout: Duration,
    client: OnceCell<tikv_client::TransactionClient>,
}

impl TiKvStore {
    pub fn new(endpoints: Vec<String>, connect_timeout: Duration) -> Self {
        Self {
            endpoints,
            connect_timeout,
            client: OnceCell::new(),
        }
    }

    async fn client(&self) -> Result<&tikv_client::TransactionClient, DbError> {
        let timeout = self.connect_timeout;
        self.client
            .get_or_try_init(|| async {
                tokio::time::timeout(
                    timeout,
                    tikv_client::TransactionClient::new(self.endpoints.clone()),
                )
                .await
                .map_err(|_| {
                    DbError::Backend(format!(
                        "TiKV connection timed out after {:.1}s",
                        timeout.as_secs_f64()
                    ))
                })?
                .map_err(|err| DbError::Backend(format!("failed to connect to TiKV: {err}")))
            })
            .await
    }

    fn job_key(job_id: &str) -> String {
        format!("{JOB_PREFIX}{job_id}")
    }

    fn broker_key(broker_id: &str) -> String {
        format!("{BROKER_PREFIX}{broker_id}")
    }

    fn serialize<T: serde::Serialize>(value: &T) -> Result<Vec<u8>, DbError> {
        serde_json::to_vec(value)
            .map_err(|err| DbError::Backend(format!("serialize failed: {err}")))
    }

    fn deserialize<T: serde::de::DeserializeOwned>(value: Vec<u8>) -> Result<T, DbError> {
        serde_json::from_slice(&value)
            .map_err(|err| DbError::Backend(format!("deserialize failed: {err}")))
    }

    async fn claim_with_owner(
        &self,
        job_id: &str,
        expected_owner_broker_id: &str,
        claimant_broker_id: &str,
    ) -> Result<ClaimResult, DbError> {
        let key = Self::job_key(job_id);
        let client = self.client().await?;

        for _ in 0..2 {
            let mut txn = client
                .begin_optimistic()
                .await
                .map_err(|err| DbError::Backend(format!("begin txn failed: {err}")))?;
            let current = txn
                .get(key.clone())
                .await
                .map_err(|err| DbError::Backend(format!("get failed: {err}")))?;
            let Some(current) = current else {
                if let Err(err) = txn.rollback().await {
                    tracing::warn!(job.id = %job_id, error = %err, "transaction rollback failed");
                }
                return Ok(ClaimResult::NotFound);
            };

            let mut record: PersistentJobRecord = Self::deserialize(current)?;
            if record.broker_id == claimant_broker_id {
                if let Err(err) = txn.rollback().await {
                    tracing::warn!(job.id = %job_id, error = %err, "transaction rollback failed");
                }
                return Ok(ClaimResult::Claimed(record));
            }

            if record.broker_id != expected_owner_broker_id {
                if let Err(err) = txn.rollback().await {
                    tracing::warn!(job.id = %job_id, error = %err, "transaction rollback failed");
                }
                return Ok(ClaimResult::Active {
                    owner_broker_id: record.broker_id.clone(),
                });
            }

            record.broker_id = claimant_broker_id.to_string();
            txn.put(key.clone(), Self::serialize(&record)?)
                .await
                .map_err(|err| DbError::Backend(format!("put failed: {err}")))?;
            match txn.commit().await {
                Ok(_) => return Ok(ClaimResult::Claimed(record)),
                Err(err) => {
                    if is_write_conflict(&err) {
                        continue;
                    }
                    return Err(DbError::Backend(format!("commit failed: {err}")));
                }
            }
        }

        Err(DbError::Conflict(format!(
            "claim conflict for job '{job_id}'"
        )))
    }
}

#[async_trait]
impl JobStore for TiKvStore {
    async fn upsert_job(&self, record: PersistentJobRecord) -> Result<(), DbError> {
        let key = Self::job_key(&record.job_id);
        let client = self.client().await?;
        let mut txn = client
            .begin_optimistic()
            .await
            .map_err(|err| DbError::Backend(format!("begin txn failed: {err}")))?;
        txn.put(key, Self::serialize(&record)?)
            .await
            .map_err(|err| DbError::Backend(format!("put failed: {err}")))?;
        txn.commit()
            .await
            .map_err(|err| DbError::Backend(format!("commit failed: {err}")))?;
        Ok(())
    }

    async fn delete_job(&self, job_id: &str) -> Result<(), DbError> {
        let key = Self::job_key(job_id);
        let client = self.client().await?;
        let mut txn = client
            .begin_optimistic()
            .await
            .map_err(|err| DbError::Backend(format!("begin txn failed: {err}")))?;
        txn.delete(key)
            .await
            .map_err(|err| DbError::Backend(format!("delete failed: {err}")))?;
        txn.commit()
            .await
            .map_err(|err| DbError::Backend(format!("commit failed: {err}")))?;
        Ok(())
    }

    async fn claim_if_owner(
        &self,
        job_id: &str,
        expected_owner_broker_id: &str,
        claimant_broker_id: &str,
    ) -> Result<ClaimResult, DbError> {
        self.claim_with_owner(job_id, expected_owner_broker_id, claimant_broker_id)
            .await
    }
}

#[async_trait]
impl BrokerLeaseStore for TiKvStore {
    async fn upsert_broker_lease(
        &self,
        broker_id: &str,
        internal_poll_base_url: &str,
        ttl: Duration,
    ) -> Result<(), DbError> {
        let key = Self::broker_key(broker_id);
        let client = self.client().await?;
        let mut txn = client
            .begin_optimistic()
            .await
            .map_err(|err| DbError::Backend(format!("begin txn failed: {err}")))?;
        let now = Utc::now();
        let record = BrokerLeaseRecord {
            broker_id: broker_id.to_string(),
            internal_poll_base_url: internal_poll_base_url.to_string(),
            lease_until: now
                + chrono::Duration::from_std(ttl).unwrap_or_else(|_| chrono::Duration::seconds(60)),
            updated_at: now,
        };
        txn.put(key, Self::serialize(&record)?)
            .await
            .map_err(|err| DbError::Backend(format!("put failed: {err}")))?;
        txn.commit()
            .await
            .map_err(|err| DbError::Backend(format!("commit failed: {err}")))?;
        Ok(())
    }

    async fn get_broker_lease(
        &self,
        broker_id: &str,
    ) -> Result<Option<BrokerLeaseRecord>, DbError> {
        let key = Self::broker_key(broker_id);
        let client = self.client().await?;
        let mut txn = client
            .begin_optimistic()
            .await
            .map_err(|err| DbError::Backend(format!("begin txn failed: {err}")))?;
        let value = txn
            .get(key)
            .await
            .map_err(|err| DbError::Backend(format!("get failed: {err}")))?;
        if let Err(err) = txn.rollback().await {
            tracing::warn!(broker_id = %broker_id, error = %err, "transaction rollback failed");
        }
        value.map(Self::deserialize).transpose()
    }

    async fn delete_broker_lease(&self, broker_id: &str) -> Result<(), DbError> {
        let key = Self::broker_key(broker_id);
        let client = self.client().await?;
        let mut txn = client
            .begin_optimistic()
            .await
            .map_err(|err| DbError::Backend(format!("begin txn failed: {err}")))?;
        txn.delete(key)
            .await
            .map_err(|err| DbError::Backend(format!("delete failed: {err}")))?;
        txn.commit()
            .await
            .map_err(|err| DbError::Backend(format!("commit failed: {err}")))?;
        Ok(())
    }
}
