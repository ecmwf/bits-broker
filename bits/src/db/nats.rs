// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

use std::time::Duration;

use async_nats::jetstream;
use async_nats::jetstream::kv;
use async_trait::async_trait;
use chrono::Utc;
use tokio::sync::OnceCell;

use crate::db::{
    BrokerLeaseRecord, BrokerLeaseStore, ClaimResult, DbError, JobStore, PersistentJobRecord,
    UserLimitEntry,
};

pub struct NatsStore {
    url: String,
    jobs_bucket: String,
    leases_bucket: String,
    user_limits_bucket: String,
    lease_ttl: Duration,
    num_replicas: usize,
    connect_timeout: Duration,
    /// Override for the client's per-subscription mpsc buffer (`async-nats`
    /// default 65,536 messages). `None` keeps the crate default. Test-only
    /// knob for reproducing slow-consumer behaviour at small scale; the
    /// production scan path (see `list_user_slots_nats`) no longer depends on
    /// it for correctness.
    subscription_capacity: Option<usize>,
    stores: OnceCell<(kv::Store, kv::Store, kv::Store)>,
}

impl NatsStore {
    pub fn new(
        url: String,
        jobs_bucket: String,
        leases_bucket: String,
        lease_ttl: Duration,
        num_replicas: usize,
        connect_timeout: Duration,
    ) -> Self {
        let user_limits_bucket = format!("{leases_bucket}-userlimits");
        Self {
            url,
            jobs_bucket,
            leases_bucket,
            user_limits_bucket,
            lease_ttl,
            num_replicas,
            connect_timeout,
            subscription_capacity: None,
            stores: OnceCell::new(),
        }
    }

    /// Override the client's per-subscription buffer size. Test-only;
    /// production should leave this at the crate default (or raise it, never
    /// lower it).
    pub fn with_subscription_capacity(mut self, capacity: usize) -> Self {
        self.subscription_capacity = Some(capacity);
        self
    }

    async fn stores(&self) -> Result<&(kv::Store, kv::Store, kv::Store), DbError> {
        let timeout = self.connect_timeout;
        self.stores
            .get_or_try_init(|| async {
                tokio::time::timeout(timeout, async {
                    let client = match self.subscription_capacity {
                        Some(capacity) => {
                            async_nats::connect_with_options(
                                &self.url,
                                async_nats::ConnectOptions::new().subscription_capacity(capacity),
                            )
                            .await
                        }
                        None => async_nats::connect(&self.url).await,
                    }
                    .map_err(|e| DbError::Backend(format!("NATS connect failed: {e}")))?;
                    let js = jetstream::new(client);

                    let jobs = Self::get_or_create_bucket(
                        &js,
                        kv::Config {
                            bucket: self.jobs_bucket.clone(),
                            history: 1,
                            num_replicas: self.num_replicas,
                            ..Default::default()
                        },
                    )
                    .await
                    .map_err(|e| DbError::Backend(format!("jobs bucket: {e}")))?;

                    let leases = Self::get_or_create_bucket(
                        &js,
                        kv::Config {
                            bucket: self.leases_bucket.clone(),
                            history: 1,
                            num_replicas: self.num_replicas,
                            storage: async_nats::jetstream::stream::StorageType::Memory,
                            max_age: self.lease_ttl.saturating_mul(2),
                            ..Default::default()
                        },
                    )
                    .await
                    .map_err(|e| DbError::Backend(format!("leases bucket: {e}")))?;

                    // Per-user admission entries. Memory storage (server-side,
                    // survives broker restarts); reclaimed via broker leases.
                    let user_limits = Self::get_or_create_bucket(
                        &js,
                        kv::Config {
                            bucket: self.user_limits_bucket.clone(),
                            history: 1,
                            num_replicas: self.num_replicas,
                            storage: async_nats::jetstream::stream::StorageType::Memory,
                            ..Default::default()
                        },
                    )
                    .await
                    .map_err(|e| DbError::Backend(format!("user-limits bucket: {e}")))?;

                    Ok((jobs, leases, user_limits))
                })
                .await
                .map_err(|_| {
                    DbError::Backend(format!(
                        "NATS init timed out after {:.1}s",
                        timeout.as_secs_f64()
                    ))
                })?
            })
            .await
    }

    pub async fn init(&self) -> Result<(), DbError> {
        self.stores().await?;
        Ok(())
    }

    async fn get_or_create_bucket(
        js: &jetstream::Context,
        config: kv::Config,
    ) -> Result<kv::Store, String> {
        let bucket_name = config.bucket.clone();
        match js.create_key_value(config).await {
            Ok(store) => Ok(store),
            Err(create_err) => {
                tracing::debug!(
                    bucket = %bucket_name,
                    error = %create_err,
                    "bucket create failed, attempting get"
                );
                js.get_key_value(&bucket_name).await.map_err(|e| {
                    format!(
                        "bucket '{bucket_name}': create failed ({create_err}), get also failed: {e}"
                    )
                })
            }
        }
    }

    async fn jobs(&self) -> Result<&kv::Store, DbError> {
        Ok(&self.stores().await?.0)
    }

    async fn leases(&self) -> Result<&kv::Store, DbError> {
        Ok(&self.stores().await?.1)
    }

    async fn user_limits(&self) -> Result<&kv::Store, DbError> {
        Ok(&self.stores().await?.2)
    }

    /// Hex-encode a key component so arbitrary scope/user/job_id strings (which
    /// may contain characters invalid in NATS KV keys, e.g. the \u{1f} user-key
    /// separator) produce a safe, dotless token.
    fn hex_token(s: &str) -> String {
        use std::fmt::Write;
        let mut out = String::with_capacity(s.len() * 2);
        for b in s.bytes() {
            let _ = write!(out, "{b:02x}");
        }
        out
    }

    fn user_slot_key(scope: &str, user: &str, job_id: &str) -> String {
        format!(
            "ul.{}.{}.{}",
            Self::hex_token(scope),
            Self::hex_token(user),
            Self::hex_token(job_id)
        )
    }

    fn user_slot_prefix(scope: &str, user: &str) -> String {
        format!("ul.{}.{}.", Self::hex_token(scope), Self::hex_token(user))
    }

    pub(crate) async fn reserve_user_slot_nats(
        &self,
        scope: &str,
        user: &str,
        job_id: &str,
        owner_broker_id: &str,
    ) -> Result<u64, DbError> {
        let store = self.user_limits().await?;
        let key = Self::user_slot_key(scope, user, job_id);
        // Idempotent: an existing entry keeps its original creation revision (seq).
        if let Some(e) = store
            .entry(&key)
            .await
            .map_err(|e| DbError::Backend(format!("get user slot: {e}")))?
            && e.operation == kv::Operation::Put
        {
            return Ok(e.revision);
        }
        let entry = UserLimitEntry {
            scope: scope.to_string(),
            user: user.to_string(),
            job_id: job_id.to_string(),
            owner_broker_id: owner_broker_id.to_string(),
            seq: 0, // authoritative seq is the KV revision, read back on list
            created_at: Utc::now(),
        };
        match store.create(&key, Self::serialize(&entry)?).await {
            Ok(revision) => Ok(revision),
            Err(e) if matches!(e.kind(), kv::CreateErrorKind::AlreadyExists) => {
                // Concurrent create won; return the winner's revision.
                let existing = store
                    .entry(&key)
                    .await
                    .map_err(|e| DbError::Backend(format!("get user slot after race: {e}")))?;
                Ok(existing.map(|en| en.revision).unwrap_or(0))
            }
            Err(e) => Err(DbError::Backend(format!("create user slot: {e}"))),
        }
    }

    pub(crate) async fn release_user_slot_nats(
        &self,
        scope: &str,
        user: &str,
        job_id: &str,
    ) -> Result<(), DbError> {
        let store = self.user_limits().await?;
        store
            .purge(&Self::user_slot_key(scope, user, job_id))
            .await
            .map_err(|e| DbError::Backend(format!("purge user slot: {e}")))?;
        Ok(())
    }

    pub(crate) async fn list_user_slots_nats(
        &self,
        scope: &str,
        user: &str,
    ) -> Result<Vec<UserLimitEntry>, DbError> {
        use futures::StreamExt;
        let store = self.user_limits().await?;
        let prefix = Self::user_slot_prefix(scope, user);
        let mut keys = store
            .keys()
            .await
            .map_err(|e| DbError::Backend(format!("list user slots: {e}")))?;
        let mut out = Vec::new();
        while let Some(k) = keys.next().await {
            let k = k.map_err(|e| DbError::Backend(format!("user slot key: {e}")))?;
            if !k.starts_with(&prefix) {
                continue;
            }
            if let Some(e) = store
                .entry(&k)
                .await
                .map_err(|e| DbError::Backend(format!("get user slot entry: {e}")))?
                && e.operation == kv::Operation::Put
            {
                let mut ent: UserLimitEntry = Self::deserialize(&e.value)?;
                ent.seq = e.revision; // store-monotonic creation order
                out.push(ent);
            }
        }
        Ok(out)
    }

    pub(crate) async fn reclaim_user_slots_nats(&self) -> Result<u64, DbError> {
        use futures::StreamExt;
        let now = Utc::now();
        // Live owners = brokers with an unexpired lease.
        let leases = self.leases().await?;
        let mut live: std::collections::HashSet<String> = std::collections::HashSet::new();
        let mut lkeys = leases
            .keys()
            .await
            .map_err(|e| DbError::Backend(format!("list leases: {e}")))?;
        while let Some(k) = lkeys.next().await {
            let k = k.map_err(|e| DbError::Backend(format!("lease key: {e}")))?;
            if let Some(e) = leases
                .entry(&k)
                .await
                .map_err(|e| DbError::Backend(format!("get lease entry: {e}")))?
                && e.operation == kv::Operation::Put
            {
                let rec: BrokerLeaseRecord = Self::deserialize(&e.value)?;
                if rec.lease_until > now {
                    live.insert(rec.broker_id);
                }
            }
        }

        let ul = self.user_limits().await?;
        let mut removed = 0u64;
        let mut ukeys = ul
            .keys()
            .await
            .map_err(|e| DbError::Backend(format!("list user slots for reclaim: {e}")))?;
        while let Some(k) = ukeys.next().await {
            let k = k.map_err(|e| DbError::Backend(format!("user slot key: {e}")))?;
            if !k.starts_with("ul.") {
                continue;
            }
            if let Some(e) = ul
                .entry(&k)
                .await
                .map_err(|e| DbError::Backend(format!("get user slot entry: {e}")))?
                && e.operation == kv::Operation::Put
            {
                let ent: UserLimitEntry = Self::deserialize(&e.value)?;
                if !live.contains(&ent.owner_broker_id) {
                    ul.purge(&k)
                        .await
                        .map_err(|e| DbError::Backend(format!("purge reclaimed slot: {e}")))?;
                    removed += 1;
                }
            }
        }
        Ok(removed)
    }

    fn serialize<T: serde::Serialize>(value: &T) -> Result<bytes::Bytes, DbError> {
        serde_json::to_vec(value)
            .map(bytes::Bytes::from)
            .map_err(|e| DbError::Backend(format!("serialize failed: {e}")))
    }

    fn deserialize<T: serde::de::DeserializeOwned>(value: &[u8]) -> Result<T, DbError> {
        serde_json::from_slice(value)
            .map_err(|e| DbError::Backend(format!("deserialize failed: {e}")))
    }

    fn job_key(public_id: &str) -> Result<String, DbError> {
        let decoded = crate::request_id::decode(public_id)
            .map_err(|e| DbError::Backend(format!("invalid public job ID {public_id:?}: {e}")))?;
        Ok(format!(
            "jobs.{}.{}.{}.{}",
            decoded.site, decoded.env, decoded.broker_slot, public_id
        ))
    }

    fn broker_lease_key(broker_id: &str) -> String {
        format!("brokers.{broker_id}")
    }

    #[cfg(test)]
    fn encode_key(public_id: &str) -> String {
        Self::job_key(public_id).unwrap_or_else(|e| format!("invalid_public_job_id.{e}"))
    }

    fn broker_slot_counter_key(site_tag: &str, env_tag: &str) -> String {
        format!("counters.broker_slot.{site_tag}.{env_tag}")
    }

    pub(crate) async fn allocate_nats_broker_slot(
        &self,
        site_tag: &str,
        env_tag: &str,
    ) -> Result<u16, DbError> {
        let key = Self::broker_slot_counter_key(site_tag, env_tag);
        let jobs = self.jobs().await?;

        loop {
            let entry = jobs
                .entry(&key)
                .await
                .map_err(|e| DbError::Backend(format!("get broker slot counter: {e}")))?;

            let Some(entry) = entry else {
                match jobs.create(&key, Self::serialize(&1_u32)?).await {
                    Ok(_) => return Ok(0),
                    Err(e) if matches!(e.kind(), kv::CreateErrorKind::AlreadyExists) => {
                        tokio::task::yield_now().await;
                        continue;
                    }
                    Err(e) => {
                        return Err(DbError::Backend(format!("create broker slot counter: {e}")));
                    }
                }
            };

            if entry.operation != kv::Operation::Put {
                match jobs
                    .update(&key, Self::serialize(&1_u32)?, entry.revision)
                    .await
                {
                    Ok(_) => return Ok(0),
                    Err(e) if matches!(e.kind(), kv::UpdateErrorKind::WrongLastRevision) => {
                        tokio::task::yield_now().await;
                        continue;
                    }
                    Err(e) => {
                        return Err(DbError::Backend(format!(
                            "recreate broker slot counter: {e}"
                        )));
                    }
                }
            }

            let next_slot: u32 = Self::deserialize(&entry.value)?;
            if next_slot > u32::from(u16::MAX) {
                return Err(DbError::SlotExhausted {
                    site: site_tag.to_string(),
                    env: env_tag.to_string(),
                    ceiling: u16::MAX,
                });
            }

            let allocated_slot = u16::try_from(next_slot).map_err(|e| {
                DbError::Backend(format!("broker slot conversion failed unexpectedly: {e}"))
            })?;
            let next_value = next_slot + 1;
            match jobs
                .update(&key, Self::serialize(&next_value)?, entry.revision)
                .await
            {
                Ok(_) => return Ok(allocated_slot),
                Err(e) if matches!(e.kind(), kv::UpdateErrorKind::WrongLastRevision) => {
                    tokio::task::yield_now().await;
                }
                Err(e) => return Err(DbError::Backend(format!("update broker slot counter: {e}"))),
            }
        }
    }
}

#[async_trait]
impl JobStore for NatsStore {
    async fn upsert_job(&self, record: PersistentJobRecord) -> Result<(), DbError> {
        let key = Self::job_key(&record.job_id)?;
        let value = Self::serialize(&record)?;
        self.jobs()
            .await?
            .put(&key, value)
            .await
            .map_err(|e| DbError::Backend(format!("put job: {e}")))?;
        Ok(())
    }

    async fn delete_job(&self, job_id: &str) -> Result<(), DbError> {
        let key = Self::job_key(job_id)?;
        self.jobs()
            .await?
            .purge(&key)
            .await
            .map_err(|e| DbError::Backend(format!("purge job: {e}")))?;
        Ok(())
    }

    async fn claim_if_owner(
        &self,
        job_id: &str,
        expected_owner_broker_id: &str,
        claimant_broker_id: &str,
    ) -> Result<ClaimResult, DbError> {
        const MAX_CAS_ATTEMPTS: u8 = 3;
        let key = Self::job_key(job_id)?;
        let jobs = self.jobs().await?;

        for _ in 0..MAX_CAS_ATTEMPTS {
            let entry = jobs
                .entry(&key)
                .await
                .map_err(|e| DbError::Backend(format!("get job entry: {e}")))?;

            let Some(entry) = entry else {
                return Ok(ClaimResult::NotFound);
            };

            if entry.operation != kv::Operation::Put {
                return Ok(ClaimResult::NotFound);
            }

            let record: PersistentJobRecord = Self::deserialize(&entry.value)?;

            if record.broker_id == claimant_broker_id {
                return Ok(ClaimResult::Claimed(record));
            }

            if record.broker_id != expected_owner_broker_id {
                return Ok(ClaimResult::Active {
                    owner_broker_id: record.broker_id,
                });
            }

            let mut claimed = record;
            claimed.broker_id = claimant_broker_id.to_string();
            let value = Self::serialize(&claimed)?;

            match jobs.update(&key, value, entry.revision).await {
                Ok(_) => return Ok(ClaimResult::Claimed(claimed)),
                Err(e) => {
                    if !matches!(e.kind(), kv::UpdateErrorKind::WrongLastRevision) {
                        return Err(DbError::Backend(format!("update job: {e}")));
                    }
                }
            }
        }

        let refreshed = jobs
            .entry(&key)
            .await
            .map_err(|e| DbError::Backend(format!("re-read after CAS retries: {e}")))?;
        match refreshed {
            Some(e) if e.operation == kv::Operation::Put => {
                let current: PersistentJobRecord = Self::deserialize(&e.value)?;
                if current.broker_id == claimant_broker_id {
                    Ok(ClaimResult::Claimed(current))
                } else {
                    Ok(ClaimResult::Active {
                        owner_broker_id: current.broker_id,
                    })
                }
            }
            _ => Ok(ClaimResult::NotFound),
        }
    }
}

#[async_trait]
impl BrokerLeaseStore for NatsStore {
    async fn upsert_broker_lease(
        &self,
        broker_id: &str,
        internal_poll_base_url: &str,
        ttl: Duration,
    ) -> Result<(), DbError> {
        let now = Utc::now();
        let lease_duration = chrono::Duration::from_std(ttl)
            .map_err(|e| DbError::Backend(format!("invalid broker lease ttl: {e}")))?;
        let record = BrokerLeaseRecord {
            broker_id: broker_id.to_string(),
            internal_poll_base_url: internal_poll_base_url.to_string(),
            lease_until: now + lease_duration,
            updated_at: now,
        };
        let key = Self::broker_lease_key(broker_id);
        let value = Self::serialize(&record)?;
        self.leases()
            .await?
            .put(&key, value)
            .await
            .map_err(|e| DbError::Backend(format!("put lease: {e}")))?;
        Ok(())
    }

    async fn get_broker_lease(
        &self,
        broker_id: &str,
    ) -> Result<Option<BrokerLeaseRecord>, DbError> {
        let key = Self::broker_lease_key(broker_id);
        let entry = self
            .leases()
            .await?
            .entry(&key)
            .await
            .map_err(|e| DbError::Backend(format!("get lease entry: {e}")))?;

        match entry {
            Some(e) if e.operation == kv::Operation::Put => {
                let record: BrokerLeaseRecord = Self::deserialize(&e.value)?;
                Ok(Some(record))
            }
            _ => Ok(None),
        }
    }

    async fn delete_broker_lease(&self, broker_id: &str) -> Result<(), DbError> {
        let key = Self::broker_lease_key(broker_id);
        self.leases()
            .await?
            .delete(&key)
            .await
            .map_err(|e| DbError::Backend(format!("delete lease: {e}")))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use base64::Engine;
    use chrono::Utc;

    use super::*;

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

    fn structured_job_subject(job_id: &str) -> String {
        let decoded = crate::request_id::decode(job_id).unwrap();
        format!(
            "jobs.{}.{}.{}.{}",
            decoded.site, decoded.env, decoded.broker_slot, job_id
        )
    }

    #[test]
    fn nats_job_key_uses_structured_subject() {
        let job_id = deterministic_job_id("bol", "dev", 42);
        let expected_subject = structured_job_subject(&job_id);
        let current_subject = NatsStore::encode_key(&job_id);
        let whole_id_base64url =
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(job_id.as_bytes());

        assert_eq!(
            current_subject, expected_subject,
            "NATS job records should be stored under structured subject {expected_subject:?} with the public ID used directly as a safe subject token"
        );
        assert_ne!(
            current_subject, whole_id_base64url,
            "NATS job records must not key by whole-ID base64url encoding"
        );
    }

    #[test]
    fn nats_job_key_rejects_invalid_public_id() {
        let malformed_id = "not-a-new-format-id";
        assert!(
            crate::request_id::decode(malformed_id).is_err(),
            "test fixture must be malformed"
        );

        let current_subject = NatsStore::encode_key(malformed_id);
        let whole_id_base64url =
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(malformed_id.as_bytes());

        assert_ne!(
            current_subject, whole_id_base64url,
            "malformed public job IDs should be rejected at the NATS persistence boundary, not converted to a whole-ID base64url key"
        );
    }

    #[test]
    fn nats_job_key_delete_uses_structured_subject() {
        let job_id = deterministic_job_id("bol", "dev", 42);
        let expected_subject = structured_job_subject(&job_id);
        let delete_subject = NatsStore::encode_key(&job_id);
        let whole_id_base64url =
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(job_id.as_bytes());

        assert_eq!(
            delete_subject, expected_subject,
            "delete_job should purge the same structured subject derived from decoded (site, env, slot, public_id)"
        );
        assert_ne!(
            delete_subject, whole_id_base64url,
            "delete_job must not purge the whole-ID base64url key"
        );
    }
}
