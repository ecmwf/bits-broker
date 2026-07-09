// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use std::collections::HashMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use chrono::Utc;

use crate::db::{
    BrokerLeaseRecord, BrokerLeaseStore, ClaimResult, DbError, JobStore, PersistentJobRecord,
    UserLimitEntry,
};
use crate::request_id;

pub struct MemoryStore {
    jobs: Mutex<HashMap<String, PersistentJobRecord>>,
    brokers: Mutex<HashMap<String, BrokerLeaseRecord>>,
    broker_slots: Mutex<HashMap<(String, String), u32>>,
    /// (scope, user) -> job_id -> entry. Per-user in-flight admission records.
    user_slots: Mutex<HashMap<(String, String), HashMap<String, UserLimitEntry>>>,
    /// Monotonic source for `UserLimitEntry::seq` (strict FIFO ranking).
    user_seq: AtomicU64,
}

impl MemoryStore {
    pub fn new() -> Self {
        Self {
            jobs: Mutex::new(HashMap::new()),
            brokers: Mutex::new(HashMap::new()),
            broker_slots: Mutex::new(HashMap::new()),
            user_slots: Mutex::new(HashMap::new()),
            user_seq: AtomicU64::new(0),
        }
    }
}

impl Default for MemoryStore {
    fn default() -> Self {
        Self::new()
    }
}

fn memory_job_key(job_id: &str) -> Result<String, DbError> {
    let decoded = request_id::decode(job_id)
        .map_err(|err| DbError::Backend(format!("invalid public job ID {job_id:?}: {err}")))?;
    Ok(format!(
        "{}/{}/{}/{}",
        decoded.site, decoded.env, decoded.broker_slot, job_id
    ))
}

#[async_trait]
impl JobStore for MemoryStore {
    async fn upsert_job(&self, record: PersistentJobRecord) -> Result<(), DbError> {
        let key = memory_job_key(&record.job_id)?;
        self.jobs
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(key, record);
        Ok(())
    }

    async fn delete_job(&self, job_id: &str) -> Result<(), DbError> {
        let key = memory_job_key(job_id)?;
        self.jobs
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .remove(&key);
        Ok(())
    }

    async fn claim_if_owner(
        &self,
        job_id: &str,
        expected_owner_broker_id: &str,
        claimant_broker_id: &str,
    ) -> Result<ClaimResult, DbError> {
        let key = memory_job_key(job_id)?;
        let mut jobs = self.jobs.lock().unwrap_or_else(|p| p.into_inner());
        let Some(record) = jobs.get_mut(&key) else {
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

impl MemoryStore {
    pub(crate) fn allocate_memory_broker_slot(
        &self,
        site_tag: &str,
        env_tag: &str,
    ) -> Result<u16, DbError> {
        let mut slots = self.broker_slots.lock().unwrap_or_else(|p| p.into_inner());
        let next_slot = slots
            .entry((site_tag.to_string(), env_tag.to_string()))
            .or_insert(0);

        if *next_slot > u32::from(u16::MAX) {
            return Err(DbError::SlotExhausted {
                site: site_tag.to_string(),
                env: env_tag.to_string(),
                ceiling: u16::MAX,
            });
        }

        let allocated_slot = *next_slot as u16;
        *next_slot += 1;
        Ok(allocated_slot)
    }

    pub(crate) fn reserve_user_slot_memory(
        &self,
        scope: &str,
        user: &str,
        job_id: &str,
        owner_broker_id: &str,
    ) -> Result<u64, DbError> {
        let mut map = self.user_slots.lock().unwrap_or_else(|p| p.into_inner());
        let entries = map
            .entry((scope.to_string(), user.to_string()))
            .or_default();
        if let Some(existing) = entries.get(job_id) {
            return Ok(existing.seq);
        }
        let seq = self.user_seq.fetch_add(1, Ordering::Relaxed) + 1;
        entries.insert(
            job_id.to_string(),
            UserLimitEntry {
                scope: scope.to_string(),
                user: user.to_string(),
                job_id: job_id.to_string(),
                owner_broker_id: owner_broker_id.to_string(),
                seq,
                created_at: Utc::now(),
            },
        );
        Ok(seq)
    }

    pub(crate) fn release_user_slot_memory(
        &self,
        scope: &str,
        user: &str,
        job_id: &str,
    ) -> Result<(), DbError> {
        let mut map = self.user_slots.lock().unwrap_or_else(|p| p.into_inner());
        let key = (scope.to_string(), user.to_string());
        if let Some(entries) = map.get_mut(&key) {
            entries.remove(job_id);
            if entries.is_empty() {
                map.remove(&key);
            }
        }
        Ok(())
    }

    pub(crate) fn list_user_slots_memory(
        &self,
        scope: &str,
        user: &str,
    ) -> Result<Vec<UserLimitEntry>, DbError> {
        let map = self.user_slots.lock().unwrap_or_else(|p| p.into_inner());
        Ok(map
            .get(&(scope.to_string(), user.to_string()))
            .map(|entries| entries.values().cloned().collect())
            .unwrap_or_default())
    }

    pub(crate) fn reclaim_user_slots_memory(&self) -> Result<u64, DbError> {
        let now = Utc::now();
        let live: std::collections::HashSet<String> = self
            .brokers
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .values()
            .filter(|b| b.lease_until > now)
            .map(|b| b.broker_id.clone())
            .collect();
        let mut map = self.user_slots.lock().unwrap_or_else(|p| p.into_inner());
        let mut removed = 0u64;
        map.retain(|_, entries| {
            entries.retain(|_, e| {
                let keep = live.contains(&e.owner_broker_id);
                if !keep {
                    removed += 1;
                }
                keep
            });
            !entries.is_empty()
        });
        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;
    use std::sync::Arc;
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

    fn deterministic_job_id(site: &str, env: &str, broker_slot: u16) -> String {
        let timestamp = chrono::DateTime::parse_from_rfc3339(crate::request_id::CUSTOM_EPOCH)
            .unwrap()
            .with_timezone(&Utc);
        crate::request_id::encode_with_fixed_random(
            site,
            env,
            broker_slot,
            timestamp,
            [0x01, 0x23, 0x45, 0x67, 0x89],
        )
        .unwrap()
    }

    fn decoded_memory_job_key(job_id: &str) -> String {
        let decoded = crate::request_id::decode(job_id).unwrap();
        format!(
            "{}/{}/{}/{}",
            decoded.site, decoded.env, decoded.broker_slot, job_id
        )
    }

    #[tokio::test]
    async fn job_key_uses_decoded_site_env_slot() {
        let store = MemoryStore::new();
        let job_id = deterministic_job_id("bol", "dev", 42);
        let expected_key = decoded_memory_job_key(&job_id);

        let job_store: &dyn JobStore = &store;
        job_store
            .upsert_job(sample_job(&job_id, "broker-1"))
            .await
            .unwrap();

        let jobs = store.jobs.lock().unwrap_or_else(|p| p.into_inner());
        assert!(
            jobs.contains_key(&expected_key),
            "memory jobs should be keyed by decoded (site, env, slot, public_id) {expected_key:?}, got {:?}",
            jobs.keys().collect::<Vec<_>>()
        );
        assert!(
            !jobs.contains_key(&job_id),
            "memory jobs must not be keyed by the whole public ID"
        );
    }

    #[tokio::test]
    async fn job_key_rejects_invalid_public_id() {
        let store = MemoryStore::new();
        let job_store: &dyn JobStore = &store;

        let result = job_store
            .upsert_job(sample_job("not-a-new-format-id", "broker-1"))
            .await;

        assert!(
            result.is_err(),
            "malformed public job IDs should be rejected at the persistence boundary"
        );
        assert!(
            store
                .jobs
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .is_empty(),
            "invalid job IDs must not be silently stored"
        );
    }

    #[tokio::test]
    async fn job_key_delete_uses_decoded_site_env_slot() {
        let store = MemoryStore::new();
        let job_id = deterministic_job_id("bol", "dev", 42);
        let expected_key = decoded_memory_job_key(&job_id);

        store
            .jobs
            .lock()
            .unwrap_or_else(|p| p.into_inner())
            .insert(expected_key, sample_job(&job_id, "broker-1"));

        let job_store: &dyn JobStore = &store;
        job_store.delete_job(&job_id).await.unwrap();

        assert!(
            store
                .jobs
                .lock()
                .unwrap_or_else(|p| p.into_inner())
                .is_empty(),
            "delete_job should remove records using the decoded (site, env, slot, public_id) key derived from the public job_id"
        );
    }

    #[tokio::test]
    async fn upsert_claim_delete_job() {
        let store = MemoryStore::new();
        let job_id = deterministic_job_id("bol", "dev", 1);
        let job = sample_job(&job_id, "broker-1");
        store.upsert_job(job.clone()).await.unwrap();

        match store
            .claim_if_owner(&job_id, "broker-1", "broker-2")
            .await
            .unwrap()
        {
            ClaimResult::Claimed(record) => assert_eq!(record.broker_id, "broker-2"),
            other => panic!("expected claimed, got {other:?}"),
        }

        store.delete_job(&job_id).await.unwrap();
        assert!(matches!(
            store
                .claim_if_owner(&job_id, "broker-2", "broker-3")
                .await
                .unwrap(),
            ClaimResult::NotFound
        ));
    }

    #[tokio::test]
    async fn claim_requires_expected_owner() {
        let store = MemoryStore::new();
        let job_id = deterministic_job_id("bol", "dev", 2);
        store
            .upsert_job(sample_job(&job_id, "broker-1"))
            .await
            .unwrap();

        assert!(matches!(
            store
                .claim_if_owner(&job_id, "broker-9", "broker-2")
                .await
                .unwrap(),
            ClaimResult::Active { .. }
        ));

        assert!(matches!(
            store
                .claim_if_owner(&job_id, "broker-1", "broker-2")
                .await
                .unwrap(),
            ClaimResult::Claimed(_)
        ));
    }

    #[tokio::test]
    async fn claim_idempotent_for_same_claimant() {
        let store = MemoryStore::new();
        let job_id = deterministic_job_id("bol", "dev", 4);
        store
            .upsert_job(sample_job(&job_id, "broker-1"))
            .await
            .unwrap();

        store
            .claim_if_owner(&job_id, "broker-1", "broker-9")
            .await
            .unwrap();

        let result = store
            .claim_if_owner(&job_id, "broker-1", "broker-9")
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

    #[tokio::test]
    async fn slot_first_allocation_is_zero() {
        let store = MemoryStore::new();

        let slot = crate::db::BrokerSlotStore::allocate_broker_slot(&store, "ams", "prd")
            .await
            .unwrap();

        assert_eq!(slot, 0);
    }

    #[tokio::test]
    async fn slot_counter_is_independent_per_site_env() {
        let store = MemoryStore::new();

        assert_eq!(
            crate::db::BrokerSlotStore::allocate_broker_slot(&store, "ams", "prd")
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            crate::db::BrokerSlotStore::allocate_broker_slot(&store, "ams", "dev")
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            crate::db::BrokerSlotStore::allocate_broker_slot(&store, "lon", "prd")
                .await
                .unwrap(),
            0
        );
        assert_eq!(
            crate::db::BrokerSlotStore::allocate_broker_slot(&store, "ams", "prd")
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            crate::db::BrokerSlotStore::allocate_broker_slot(&store, "ams", "dev")
                .await
                .unwrap(),
            1
        );
    }

    #[tokio::test]
    async fn slot_allocation_is_safe_under_concurrent_callers() {
        let store = Arc::new(MemoryStore::new());
        let caller_count = 128_u16;
        let mut handles = Vec::new();

        for _ in 0..caller_count {
            let store = Arc::clone(&store);
            handles.push(tokio::spawn(async move {
                crate::db::BrokerSlotStore::allocate_broker_slot(&*store, "ams", "prd").await
            }));
        }

        let mut slots = Vec::new();
        for handle in handles {
            slots.push(handle.await.unwrap().unwrap());
        }
        slots.sort_unstable();

        let unique_slots: HashSet<u16> = slots.iter().copied().collect();
        assert_eq!(unique_slots.len(), usize::from(caller_count));
        assert_eq!(slots, (0..caller_count).collect::<Vec<_>>());
    }

    #[tokio::test]
    async fn slot_counter_persists_within_store_instance() {
        let store = Arc::new(MemoryStore::new());

        assert_eq!(
            crate::db::BrokerSlotStore::allocate_broker_slot(&*store, "ams", "prd")
                .await
                .unwrap(),
            0
        );

        let shared_store = Arc::clone(&store);
        assert_eq!(
            crate::db::BrokerSlotStore::allocate_broker_slot(&*shared_store, "ams", "prd")
                .await
                .unwrap(),
            1
        );
        assert_eq!(
            crate::db::BrokerSlotStore::allocate_broker_slot(&*store, "ams", "prd")
                .await
                .unwrap(),
            2
        );
    }

    #[tokio::test]
    async fn slot_exhaustion_returns_actionable_error_at_u16_max() {
        let store = MemoryStore::new();

        for expected_slot in 0..=u16::MAX {
            let slot = crate::db::BrokerSlotStore::allocate_broker_slot(&store, "ams", "prd")
                .await
                .unwrap();
            assert_eq!(slot, expected_slot);
        }

        let error = crate::db::BrokerSlotStore::allocate_broker_slot(&store, "ams", "prd")
            .await
            .unwrap_err();

        match &error {
            DbError::SlotExhausted { site, env, ceiling } => {
                assert_eq!(site, "ams");
                assert_eq!(env, "prd");
                assert_eq!(*ceiling, u16::MAX);
            }
            other => panic!("expected SlotExhausted, got {other:?}"),
        }

        let message = error.to_string();
        assert!(message.contains(&u16::MAX.to_string()));
        assert!(message.contains("ceiling"));
        assert!(message.contains("version byte"));
        assert!(message.contains("bump"));
    }
}
