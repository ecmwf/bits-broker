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

/// How long a `bits-leases-userlimits` purge tombstone is kept before NATS
/// physically expires it (via per-message TTL, `purge_with_ttl`).
///
/// Only ever applies to a slot that has *already been released* --
/// `reserve_user_slot_nats`'s `create()` call for the live entry carries no
/// TTL, so an in-flight job's admission slot is never at risk of expiring
/// early regardless of job duration. Without this, every completed job
/// leaves a permanent message in the stream (JetStream's KV `purge` leaves
/// a tombstone behind, and the stream's `max_age` is unbounded), so growth
/// is proportional to total historical throughput rather than any leak or
/// error rate. Chosen short relative to any realistic sweep interval, long
/// enough to still be visible for a while afterwards.
const USER_LIMIT_TOMBSTONE_TTL: Duration = Duration::from_secs(600);

/// How long a *delete marker* is kept once a per-message-TTL'd entry (i.e. a
/// [`USER_LIMIT_TOMBSTONE_TTL`]-bounded purge tombstone) actually expires.
///
/// A distinct, secondary knob from `USER_LIMIT_TOMBSTONE_TTL`: it does
/// nothing on its own (a plain `purge()` with only this enabled never
/// expires) and only governs what NATS leaves behind *after* a TTL'd
/// message expires, so a watcher/mirror can tell "this key expired" apart
/// from "this key never existed". Nothing in `bits` relies on that
/// distinction, so this only affects how much longer a trace of a released
/// slot lingers after the tombstone itself is gone -- defaulted to the same
/// duration as the tombstone TTL, independently overridable.
const USER_LIMIT_DELETE_MARKER_TTL: Duration = Duration::from_secs(600);

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
    /// Override for [`USER_LIMIT_TOMBSTONE_TTL`]. `None` keeps the default.
    /// Test-only knob so tests can observe real tombstone expiry without
    /// waiting the production duration.
    user_limit_tombstone_ttl: Option<Duration>,
    /// Override for [`USER_LIMIT_DELETE_MARKER_TTL`]. `None` keeps the default.
    user_limit_delete_marker_ttl: Option<Duration>,
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
            user_limit_tombstone_ttl: None,
            user_limit_delete_marker_ttl: None,
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

    /// Override how long a released user-limit slot's purge tombstone is
    /// kept before it self-expires (see [`USER_LIMIT_TOMBSTONE_TTL`]).
    /// Test-only; production should leave this at the default.
    pub fn with_user_limit_tombstone_ttl(mut self, ttl: Duration) -> Self {
        self.user_limit_tombstone_ttl = Some(ttl);
        self
    }

    fn user_limit_tombstone_ttl(&self) -> Duration {
        self.user_limit_tombstone_ttl
            .unwrap_or(USER_LIMIT_TOMBSTONE_TTL)
    }

    /// Override how long a delete marker lingers after a TTL'd entry expires
    /// (see [`USER_LIMIT_DELETE_MARKER_TTL`]). Test-only; production should
    /// leave this at the default.
    pub fn with_user_limit_delete_marker_ttl(mut self, ttl: Duration) -> Self {
        self.user_limit_delete_marker_ttl = Some(ttl);
        self
    }

    fn user_limit_delete_marker_ttl(&self) -> Duration {
        self.user_limit_delete_marker_ttl
            .unwrap_or(USER_LIMIT_DELETE_MARKER_TTL)
    }

    /// Test-only: create the user-limits bucket using the pre-TTL-fix shape
    /// (no `limit_markers`, i.e. `allow_msg_ttl` left off), to simulate a
    /// bucket that already existed before this fix shipped. Must be called
    /// before any other method that touches `self`, so this store's own
    /// (fixed) `stores()` call hits the create-already-exists branch and
    /// exercises `get_or_create_bucket`'s update-based reconciliation.
    pub async fn debug_precreate_legacy_user_limits_bucket(&self) -> Result<(), DbError> {
        self.debug_precreate_user_limits_bucket_with_storage(
            async_nats::jetstream::stream::StorageType::Memory,
        )
        .await
    }

    /// Like [`Self::debug_precreate_legacy_user_limits_bucket`], but using
    /// **File** storage -- a config `get_or_create_bucket` cannot reconcile
    /// via `update_key_value` (NATS rejects storage-type changes on an
    /// existing stream). Simulates reconciliation itself failing, exercising
    /// `purge_user_slot`'s fallback path instead of the update path.
    pub async fn debug_precreate_incompatible_user_limits_bucket(&self) -> Result<(), DbError> {
        self.debug_precreate_user_limits_bucket_with_storage(
            async_nats::jetstream::stream::StorageType::File,
        )
        .await
    }

    async fn debug_precreate_user_limits_bucket_with_storage(
        &self,
        storage: async_nats::jetstream::stream::StorageType,
    ) -> Result<(), DbError> {
        let client = async_nats::connect(&self.url)
            .await
            .map_err(|e| DbError::Backend(format!("NATS connect failed: {e}")))?;
        let js = jetstream::new(client);
        js.create_key_value(kv::Config {
            bucket: self.user_limits_bucket.clone(),
            history: 1,
            num_replicas: self.num_replicas,
            storage,
            ..Default::default()
        })
        .await
        .map_err(|e| DbError::Backend(format!("legacy user-limits bucket: {e}")))?;
        Ok(())
    }

    /// Test-only: the *raw* JetStream message count backing the user-limits
    /// bucket's stream, including not-yet-expired purge tombstones -- unlike
    /// the KV API (`list_user_slots`), which only reports live vs. deleted,
    /// not whether a deleted key's tombstone still physically occupies
    /// space in the stream.
    pub async fn debug_user_limits_stream_message_count(&self) -> Result<u64, DbError> {
        let store = self.user_limits().await?;
        let info = store
            .stream
            .info_with_subjects(format!("{}>", store.prefix))
            .await
            .map_err(|e| DbError::Backend(format!("stream info: {e}")))?;
        Ok(info.info.state.messages)
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
                    //
                    // `limit_markers` enables per-message TTL on this bucket's
                    // underlying stream (`allow_msg_ttl`), required for
                    // `purge_with_ttl` in
                    // `release_user_slot_nats`/`reclaim_user_slots_nats` to
                    // have any effect. Its value here (USER_LIMIT_DELETE_MARKER_TTL)
                    // is a distinct, secondary knob from the tombstone TTL
                    // itself -- see its doc comment.
                    let user_limits = Self::get_or_create_bucket(
                        &js,
                        kv::Config {
                            bucket: self.user_limits_bucket.clone(),
                            history: 1,
                            num_replicas: self.num_replicas,
                            storage: async_nats::jetstream::stream::StorageType::Memory,
                            limit_markers: Some(self.user_limit_delete_marker_ttl()),
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
        match js.create_key_value(config.clone()).await {
            Ok(store) => Ok(store),
            Err(create_err) => {
                // The bucket already exists (the common case after the first
                // deploy). Reconcile its underlying stream config to match
                // `config` via `update_key_value` -- required, not just
                // best-effort: `create_key_value` only ever applies a config
                // on first creation, so a bucket created before a config
                // change (e.g. enabling `limit_markers`/`allow_msg_ttl`)
                // would otherwise keep running with its stale config
                // forever, causing `purge_with_ttl` to fail outright with
                // "per-message TTL is disabled". `update_stream` reconciles
                // in place, no data loss.
                tracing::debug!(
                    bucket = %bucket_name,
                    error = %create_err,
                    "bucket create failed, attempting update"
                );
                match js.update_key_value(config).await {
                    Ok(store) => Ok(store),
                    Err(update_err) => {
                        tracing::warn!(
                            bucket = %bucket_name,
                            error = %update_err,
                            "bucket update also failed, falling back to get (config may be stale)"
                        );
                        js.get_key_value(&bucket_name).await.map_err(|e| {
                            format!(
                                "bucket '{bucket_name}': create failed ({create_err}), update failed ({update_err}), get also failed: {e}"
                            )
                        })
                    }
                }
            }
        }
    }

    /// Purge a user-limit slot's key, preferring a self-expiring tombstone
    /// (`purge_with_ttl`) but falling back to a plain, permanent `purge` if
    /// that fails for *any* reason.
    ///
    /// The fallback matters for safe rollout: on a bucket that predates this
    /// fix, `purge_with_ttl` fails outright ("per-message TTL is disabled")
    /// until the NATS server process restarts -- reconciling the stream's
    /// config via `update_key_value` takes effect in the stream's stored
    /// metadata immediately, but does not retroactively arm that server
    /// process's own TTL-expiry tracking. Without this fallback, a slot that
    /// fails to release this way would stay live indefinitely (reclaim's
    /// dead-broker cleanup hits the same error), which is worse for
    /// availability than the bloat this fix addresses. With it, a deploy
    /// onto a pre-existing bucket is safe immediately: releases keep working
    /// as before (permanent tombstone) until NATS restarts, at which point
    /// TTL self-expiry starts working automatically.
    async fn purge_user_slot(store: &kv::Store, key: &str, ttl: Duration) -> Result<(), DbError> {
        if let Err(ttl_err) = store.purge_with_ttl(key, ttl).await {
            tracing::warn!(
                key = %key,
                error = %ttl_err,
                "purge_with_ttl failed, falling back to a plain purge (tombstone will not \
                 self-expire until the NATS server is restarted)"
            );
            store
                .purge(key)
                .await
                .map_err(|e| DbError::Backend(format!("purge user slot (fallback): {e}")))?;
        }
        Ok(())
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
        Self::purge_user_slot(
            store,
            &Self::user_slot_key(scope, user, job_id),
            self.user_limit_tombstone_ttl(),
        )
        .await
    }

    /// Fetch the *current* set of entries matching `key_pattern` (a KV key
    /// pattern, e.g. `ul.<scope>.<user>.>` or `>` for the whole bucket) in a
    /// single server-side-filtered streaming call.
    ///
    /// Replaces the previous `store.keys()` + one `store.entry()` round trip
    /// per key used by both `list_user_slots_nats` and
    /// `reclaim_user_slots_nats`, which had two compounding problems:
    ///
    /// 1. `store.keys()` has no server-side prefix filter — it subscribes to
    ///    the *entire* bucket and relies on client-side filtering, so a scan
    ///    for one user's ~1-3 slots was O(total bucket size).
    /// 2. Draining that stream one key at a time with a synchronous `entry()`
    ///    RPC between items is slow enough relative to how fast JetStream
    ///    pushes a `LastPerSubject` replay that the client's `Ordered`
    ///    consumer repeatedly detects a sequence gap (buffer overflow) and
    ///    resubscribes — a self-healing but pathologically slow retry storm
    ///    that flooded logs with `Event::SlowConsumer`.
    ///
    /// A single `watch_with_history` call filtered server-side to
    /// `key_pattern`, carrying full entry values, fixes both: a per-user
    /// pattern only ever returns that user's own entries regardless of bucket
    /// size, and even the whole-bucket pattern is one streaming RPC instead
    /// of N+1 synchronous ones.
    ///
    /// Before doing that, a cheap subject-filtered `STREAM.INFO` probe checks
    /// whether `key_pattern` currently matches *anything at all*. This is
    /// deliberately not left to `watch_with_history` itself: that call only
    /// reliably signals "caught up" (`seen_current`) once at least one
    /// message is actually delivered, and relies on the client's `Watch`
    /// stream short-circuiting immediately when the underlying consumer's
    /// `num_pending` is already zero at creation time. That short-circuit is
    /// an internal implementation detail, not a documented contract — and it
    /// is not stable across client versions: confirmed empirically that a
    /// newer `async-nats` release (0.50.0, vs. the pinned 0.38.0) dropped it,
    /// so `watch_with_history` on a pattern matching zero current keys hangs
    /// forever (idle heartbeats are exchanged internally but nothing is ever
    /// surfaced to the stream). A zero-match pattern is not a rare case here:
    /// the whole-bucket reclaim sweep (`ul.>`) hits it any time the bucket is
    /// genuinely empty, e.g. right after a manual purge. The probe makes
    /// correctness independent of that internal client behaviour rather than
    /// relying on it, at the cost of one extra cheap RPC when there is at
    /// least one match (the common case).
    async fn scan_current_entries(
        store: &kv::Store,
        key_pattern: &str,
    ) -> Result<Vec<kv::Entry>, DbError> {
        use futures::StreamExt;

        let full_subject = format!("{}{key_pattern}", store.prefix);
        let mut probe = store
            .stream
            .info_with_subjects(&full_subject)
            .await
            .map_err(|e| DbError::Backend(format!("info {key_pattern}: {e}")))?;
        if probe.next().await.is_none() {
            return Ok(Vec::new());
        }

        let mut watch = store
            .watch_with_history(key_pattern)
            .await
            .map_err(|e| DbError::Backend(format!("watch {key_pattern}: {e}")))?;
        let mut out = Vec::new();
        // `watch_with_history` is a live tail; stop once the initial
        // historical replay catches up to "now". The crate sets
        // `seen_current` at that point (and keeps it set), so the first entry
        // with `seen_current == true` is the end of the snapshot. The probe
        // above already guarantees at least one entry exists, so this loop is
        // no longer relied upon to terminate on an empty match.
        while let Some(entry) = watch.next().await {
            let entry = entry.map_err(|e| DbError::Backend(format!("watch entry: {e}")))?;
            let done = entry.seen_current;
            out.push(entry);
            if done {
                break;
            }
        }
        Ok(out)
    }

    pub(crate) async fn list_user_slots_nats(
        &self,
        scope: &str,
        user: &str,
    ) -> Result<Vec<UserLimitEntry>, DbError> {
        let store = self.user_limits().await?;
        let prefix = Self::user_slot_prefix(scope, user);
        let entries = Self::scan_current_entries(store, &format!("{prefix}>")).await?;
        let mut out = Vec::new();
        for e in entries {
            if e.operation != kv::Operation::Put {
                continue;
            }
            let mut ent: UserLimitEntry = Self::deserialize(&e.value)?;
            ent.seq = e.revision; // store-monotonic creation order
            out.push(ent);
        }
        Ok(out)
    }

    pub(crate) async fn reclaim_user_slots_nats(&self) -> Result<u64, DbError> {
        const ALL_KEYS: &str = ">";
        let now = Utc::now();
        // Live owners = brokers with an unexpired lease.
        let leases = self.leases().await?;
        let mut live: std::collections::HashSet<String> = std::collections::HashSet::new();
        for e in Self::scan_current_entries(leases, ALL_KEYS).await? {
            if e.operation == kv::Operation::Put {
                let rec: BrokerLeaseRecord = Self::deserialize(&e.value)?;
                if rec.lease_until > now {
                    live.insert(rec.broker_id);
                }
            }
        }

        let ul = self.user_limits().await?;
        let mut removed = 0u64;
        for e in Self::scan_current_entries(ul, ALL_KEYS).await? {
            let k = &e.key;
            if !k.starts_with("ul.") {
                continue;
            }
            if e.operation == kv::Operation::Put {
                let ent: UserLimitEntry = Self::deserialize(&e.value)?;
                if !live.contains(&ent.owner_broker_id) {
                    Self::purge_user_slot(ul, &k, self.user_limit_tombstone_ttl()).await?;
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
