use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;

use crate::db::{
    BrokerLeaseRecord, BrokerLeaseStore, ClaimResult, DbError, JobStore, PersistentJobRecord,
};

pub struct MemoryStore {
    jobs: Mutex<HashMap<String, PersistentJobRecord>>,
    brokers: Mutex<HashMap<String, BrokerLeaseRecord>>,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self {
            jobs: Mutex::new(HashMap::new()),
            brokers: Mutex::new(HashMap::new()),
        }
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl JobStore for MemoryStore {
    async fn upsert_job(&self, record: PersistentJobRecord) -> Result<(), DbError> {
        self.jobs
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(record.job_id.clone(), record);
        Ok(())
    }

    async fn delete_job(&self, job_id: &str) -> Result<(), DbError> {
        self.jobs
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(job_id);
        Ok(())
    }

    async fn claim_if_owner(
        &self,
        job_id: &str,
        expected_owner_broker_id: &str,
        claimant_broker_id: &str,
    ) -> Result<ClaimResult, DbError> {
        let mut jobs = self.jobs.lock().unwrap_or_else(|p| p.into_inner());
        let Some(record) = jobs.get_mut(job_id) else {
            return Ok(ClaimResult::NotFound);
        };

        if record.broker_id == claimant_broker_id {
            return Ok(ClaimResult::Claimed(record.clone()));
        }

        if record.broker_id != expected_owner_broker_id {
            return Ok(ClaimResult::Active {
                owner_broker_id: record.broker_id.clone(),
            });
        }

        record.broker_id = claimant_broker_id.to_string();
        Ok(ClaimResult::Claimed(record.clone()))
    }
}

#[async_trait]
impl BrokerLeaseStore for MemoryStore {
    async fn upsert_broker_lease(
        &self,
        broker_id: &str,
        internal_poll_base_url: &str,
        ttl: Duration,
    ) -> Result<(), DbError> {
        let now = Utc::now();
        self.brokers
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(
                broker_id.to_string(),
                BrokerLeaseRecord {
                    broker_id: broker_id.to_string(),
                    internal_poll_base_url: internal_poll_base_url.to_string(),
                    lease_until: now
                        + chrono::Duration::from_std(ttl)
                            .unwrap_or_else(|_| chrono::Duration::seconds(60)),
                    updated_at: now,
                },
            );
        Ok(())
    }

    async fn get_broker_lease(
        &self,
        broker_id: &str,
    ) -> Result<Option<BrokerLeaseRecord>, DbError> {
        let lease = self
            .brokers
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .get(broker_id)
            .cloned();
        Ok(lease)
    }

    async fn delete_broker_lease(&self, broker_id: &str) -> Result<(), DbError> {
        self.brokers
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(broker_id);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use chrono::Utc;
    use serde_json::json;

    use super::*;

    fn sample_job(job_id: &str, broker_id: &str) -> PersistentJobRecord {
        PersistentJobRecord {
            job_id: job_id.to_string(),
            broker_id: broker_id.to_string(),
            original_request: json!({"foo": "bar"}),
            user: json!({"name": "alice"}),
            metadata: json!({"cost": 1}),
            created_at: Utc::now(),
        }
    }

    #[tokio::test]
    async fn upsert_claim_delete_job() {
        let store = MemoryStore::new();
        let job = sample_job("broker-1~1", "broker-1");
        store.upsert_job(job.clone()).await.unwrap();

        match store
            .claim_if_owner("broker-1~1", "broker-1", "broker-2")
            .await
            .unwrap()
        {
            ClaimResult::Claimed(record) => assert_eq!(record.broker_id, "broker-2"),
            other => panic!("expected claimed, got {other:?}"),
        }

        store.delete_job("broker-1~1").await.unwrap();
        assert!(matches!(
            store
                .claim_if_owner("broker-1~1", "broker-2", "broker-3")
                .await
                .unwrap(),
            ClaimResult::NotFound
        ));
    }

    #[tokio::test]
    async fn claim_requires_expected_owner() {
        let store = MemoryStore::new();
        store
            .upsert_job(sample_job("broker-1~2", "broker-1"))
            .await
            .unwrap();

        assert!(matches!(
            store
                .claim_if_owner("broker-1~2", "broker-9", "broker-2")
                .await
                .unwrap(),
            ClaimResult::Active { .. }
        ));

        assert!(matches!(
            store
                .claim_if_owner("broker-1~2", "broker-1", "broker-2")
                .await
                .unwrap(),
            ClaimResult::Claimed(_)
        ));
    }

    #[tokio::test]
    async fn claim_idempotent_for_same_claimant() {
        let store = MemoryStore::new();
        store
            .upsert_job(sample_job("broker-1~4", "broker-1"))
            .await
            .unwrap();

        store
            .claim_if_owner("broker-1~4", "broker-1", "broker-9")
            .await
            .unwrap();

        let result = store
            .claim_if_owner("broker-1~4", "broker-1", "broker-9")
            .await
            .unwrap();

        match result {
            ClaimResult::Claimed(record) => assert_eq!(record.broker_id, "broker-9"),
            other => panic!("expected claimed, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn broker_lease_lifecycle() {
        let store = MemoryStore::new();
        store
            .upsert_broker_lease(
                "broker-2",
                "http://broker-2.bits-headless.default.svc.cluster.local:3000",
                Duration::from_secs(300),
            )
            .await
            .unwrap();

        let lease = store.get_broker_lease("broker-2").await.unwrap().unwrap();
        assert_eq!(lease.broker_id, "broker-2");

        store.delete_broker_lease("broker-2").await.unwrap();
        assert!(store.get_broker_lease("broker-2").await.unwrap().is_none());
    }
}
