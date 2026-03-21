use std::time::Duration;

use async_nats::jetstream;
use async_nats::jetstream::kv;
use async_trait::async_trait;
use base64::Engine;
use chrono::Utc;
use tokio::sync::OnceCell;

use crate::db::{
    BrokerLeaseRecord, BrokerLeaseStore, ClaimResult, DbError, JobStore, PersistentJobRecord,
};

pub struct NatsStore {
    url: String,
    jobs_bucket: String,
    leases_bucket: String,
    lease_ttl: Duration,
    num_replicas: usize,
    stores: OnceCell<(kv::Store, kv::Store)>,
}

impl NatsStore {
    pub fn new(
        url: String,
        jobs_bucket: String,
        leases_bucket: String,
        lease_ttl: Duration,
        num_replicas: usize,
    ) -> Self {
        Self {
            url,
            jobs_bucket,
            leases_bucket,
            lease_ttl,
            num_replicas,
            stores: OnceCell::new(),
        }
    }

    async fn stores(&self) -> Result<&(kv::Store, kv::Store), DbError> {
        self.stores
            .get_or_try_init(|| async {
                let client = async_nats::connect(&self.url)
                    .await
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
                        max_age: self.lease_ttl * 2,
                        ..Default::default()
                    },
                )
                .await
                .map_err(|e| DbError::Backend(format!("leases bucket: {e}")))?;

                Ok((jobs, leases))
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

    fn serialize<T: serde::Serialize>(value: &T) -> Result<bytes::Bytes, DbError> {
        serde_json::to_vec(value)
            .map(bytes::Bytes::from)
            .map_err(|e| DbError::Backend(format!("serialize failed: {e}")))
    }

    fn deserialize<T: serde::de::DeserializeOwned>(value: &[u8]) -> Result<T, DbError> {
        serde_json::from_slice(value)
            .map_err(|e| DbError::Backend(format!("deserialize failed: {e}")))
    }

    fn encode_key(key: &str) -> String {
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(key.as_bytes())
    }
}

#[async_trait]
impl JobStore for NatsStore {
    async fn upsert_job(&self, record: PersistentJobRecord) -> Result<(), DbError> {
        let key = Self::encode_key(&record.job_id);
        let value = Self::serialize(&record)?;
        self.jobs()
            .await?
            .put(&key, value)
            .await
            .map_err(|e| DbError::Backend(format!("put job: {e}")))?;
        Ok(())
    }

    async fn delete_job(&self, job_id: &str) -> Result<(), DbError> {
        let key = Self::encode_key(job_id);
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
        let key = Self::encode_key(job_id);
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
        let record = BrokerLeaseRecord {
            broker_id: broker_id.to_string(),
            internal_poll_base_url: internal_poll_base_url.to_string(),
            lease_until: now
                + chrono::Duration::from_std(ttl).unwrap_or_else(|_| chrono::Duration::seconds(60)),
            updated_at: now,
        };
        let key = Self::encode_key(broker_id);
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
        let key = Self::encode_key(broker_id);
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
        let key = Self::encode_key(broker_id);
        self.leases()
            .await?
            .delete(&key)
            .await
            .map_err(|e| DbError::Backend(format!("delete lease: {e}")))?;
        Ok(())
    }
}
