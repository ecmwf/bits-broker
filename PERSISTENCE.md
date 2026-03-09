# Plan: Sticky-First Routing + Internal Owner Proxy + DB Leases

## Context

BITS is a library, not a service binary, so correctness cannot depend on a specific ingress layout. Ingress stickiness (hash on `Authorization`) is treated as a performance optimization only. The library itself must still work when a poll lands on a non-owner broker due to scaling, rollouts, or hash-ring changes.

Goal:
- long-running jobs are recoverable after broker failure,
- wrong-broker polls are resolved internally,
- large payloads are usually still direct owner->client because sticky routing is expected most of the time.

## Decisions

- Job IDs are owner-aware: `{broker_id}~{uuid}`.
- `persist` pipeline action is removed; persistence is dispatcher-level (`dispatcher.persistent: true`).
- Wrong-owner poll handling is internal proxying in BITS (no client redirect required for this hop).
- Broker endpoint resolution is DB-backed (broker lease records), not hard-coded DNS assumptions.
- TiKV backend is behind traits so storage can be swapped later.

---

## High-level Flow

### Submit

1. Broker assigns owner-aware job id if caller did not already set one.
2. Job runs as before in-memory.
3. If action dispatcher has `persistent: true`, dispatcher persists job record before queueing and maintains lock heartbeat while running.

### Poll

1. If job is in local map: serve local `Ready`/`Pending` as before.
2. On local miss:
   - Parse owner from `job_id`.
   - If owner != self: resolve owner endpoint from broker lease table and proxy poll internally.
   - If proxy fails or owner lease is expired/missing: attempt DB claim/recovery path.
3. Recovery path:
   - Persistent record exists and claim succeeds: restore from `original_request`, preserve `created_at`, resubmit, return `Pending`.
   - No durable record: return `JobLost`.

### Broker lease lifecycle

1. On startup/runtime, broker periodically upserts lease (`broker_id -> internal_poll_base_url`, `lease_until`).
2. Renewal interval is `broker_lease_ttl / 2`.
3. Lease expiry is treated as owner unavailable.

---

## DB abstraction (`bits/src/db/`)

### Traits

```rust
#[async_trait]
pub trait JobStore: Send + Sync {
    async fn upsert_job(&self, record: PersistentJobRecord) -> Result<(), DbError>;
    async fn renew_job_lock(&self, job_id: &str, broker_id: &str, ttl: Duration) -> Result<(), DbError>;
    async fn delete_job(&self, job_id: &str) -> Result<(), DbError>;
    async fn try_claim_expired(&self, job_id: &str, broker_id: &str, ttl: Duration) -> Result<ClaimResult, DbError>;
    async fn force_claim(&self, job_id: &str, broker_id: &str, ttl: Duration) -> Result<ClaimResult, DbError>;
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
```

### Logical separation in TiKV

- Jobs namespace: `jobs/{job_id}`
- Broker leases namespace: `brokers/{broker_id}`

This is table-like separation while staying in a single TiKV cluster.

### Backends

- `memory.rs`: in-memory implementation for tests.
- `tikv.rs`: TiKV transactional implementation (compiled behind `bits` feature `tikv`).

---

## Config model

### Bits-level (`bits:`)

- `broker_id` (optional; defaults to generated local ID)
- `internal_poll_base_url` (optional; default `http://127.0.0.1:8080/job`)
- `internal_poll_timeout_ms` (optional)
- `job_cleanup_interval_ms` (optional)
- `tikv` (optional):
  - `endpoints: [..]`
  - `lock_ttl_secs`
  - `broker_lease_ttl_secs`

### Dispatcher-level (`dispatcher:`)

- Existing queue/executor/concurrency fields continue to work.
- New persistence fields:
  - `persistent: bool`
  - `lock_ttl_secs: float` (optional override)

---

## File-level implementation summary

- `bits/src/db/mod.rs`
  - Add persistence traits/types/errors.
- `bits/src/db/memory.rs`
  - Add in-memory implementation + unit tests.
- `bits/src/db/tikv.rs`
  - Add TiKV implementation with key prefix separation.
- `bits/src/config.rs`
  - Parse new `bits` and dispatcher persistence fields.
  - Enforce `persistent: true` requires configured store.
  - Reject legacy `persist` pipeline step.
- `bits/src/dispatcher/mod.rs`
  - Inject store, `broker_id`, lock TTL, and persistent flag.
  - Upsert before enqueue for persistent actions.
  - Heartbeat renew while awaiting completion.
  - Delete persisted record on completion.
- `bits/src/job.rs`
  - Add `Job::new_with_id` and `Job::restore`.
- `bits/src/bits.rs`
  - Add broker identity + store/client fields.
  - Generate owner-aware IDs.
  - Add wrong-owner proxy and claim/recover on miss.
  - Add broker lease heartbeat.
  - Add `PollOutcome::JobLost`.
- `bits/src/actions/mod.rs`, `bits/src/routing/switch.rs`
  - Remove `Action::Persist` variant and execution path.
- `bits/Cargo.toml`
  - Add optional `tikv-client` dependency behind feature `tikv`.

---

## Verification

1. Unit tests for memory store:
   - upsert/renew/delete,
   - active vs expired claim,
   - force-claim,
   - broker lease lifecycle.
2. Existing workspace tests continue to pass (`cargo test`).
3. Integration follow-up (to add):
   - two brokers, poll on wrong broker proxies to owner,
   - owner lease missing/expired triggers recovery for persistent jobs,
   - non-persistent owner miss returns `JobLost`.

---

## Operational notes

- Ingress sticky hash on auth header remains recommended for performance.
- Correctness does not depend on sticky routing because BITS handles wrong-owner polls.
- If `bits.tikv` is configured but the crate is built without feature `tikv`, startup fails fast with config error.
