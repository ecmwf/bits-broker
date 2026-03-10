# Plan: Threshold Persistence + Strict Lease Reclaim

## Context

BITS should not depend on ingress routing for correctness. Sticky ingress hashing remains a performance optimization. The library must still function when polls land on non-owner brokers.

This plan simplifies persistence overhead:

- persist once when a job crosses a time threshold,
- no per-job lock heartbeat writes,
- reclaim only when owner broker lease is missing/expired.

## Decisions

- Job IDs are owner-aware: `{broker_instance_id}~{uuid}`.
- Broker instance ID is unique per process start (restart creates a new ID).
- `persist` pipeline action is removed.
- Persistence is configured globally with a threshold timer (`bits.persist_after_ms`).
- Reclaim is strict: proxy failure alone does not reclaim if owner lease is still valid.

---

## High-level flow

### Submit

1. Assign owner-aware job id if needed.
2. Start processing immediately in memory.
3. If `persist_after_ms` is configured and job is still running at that time, write one persistent job record.
4. On completion, delete persistent job record if it was written.

### Poll

1. Try local map first.
2. On miss, parse owner from job id.
3. If owner lease is active, proxy poll to owner.
4. If owner lease missing/expired, attempt claim+replay from DB.
5. If no record exists, return `JobLost`.

### Broker lease lifecycle

1. Each broker periodically upserts lease (`broker_id -> internal_poll_base_url`, `lease_until`).
2. Renewal interval is `broker_lease_ttl / 2`.
3. Lease expiry is the only reclaim trigger.

---

## DB abstraction (`bits/src/db/`)

### Traits

```rust
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
```

### Data separation in TiKV

- Jobs namespace: `jobs/{job_id}`
- Broker leases namespace: `brokers/{broker_id}`

### Backends

- `memory.rs`: in-memory trait implementation + tests.
- `tikv.rs`: TiKV transactional implementation (feature-gated behind `tikv`).

---

## Config model

### Bits-level (`bits:`)

- `broker_id` (optional base name)
- `internal_poll_base_url` (optional)
- `internal_poll_timeout_ms` (optional)
- `job_cleanup_interval_ms` (optional)
- `persist_after_ms` (optional; enables threshold persistence)
- `poll_timeout_ms` (default `30000`)
- `persist_guard_ms` (default `1000`)
- `tikv` (optional):
  - `endpoints: [..]`
  - `broker_lease_ttl_secs`

Validation:

- If `persist_after_ms` is set, TiKV config must be present.
- Must satisfy: `persist_after_ms + persist_guard_ms < poll_timeout_ms`.

### Dispatcher-level (`dispatcher:`)

- Supports queue/executor/concurrency only.
- Persistence flags under dispatcher are removed.

---

## File-level implementation summary

- `bits/src/db/mod.rs`
  - JobStore/BrokerLeaseStore traits updated for strict lease reclaim model.
- `bits/src/db/memory.rs`
  - Removed lock-renew behavior; added owner-checked claim behavior.
- `bits/src/db/tikv.rs`
  - Removed lock-renew behavior; owner-checked claim with CAS.
- `bits/src/config.rs`
  - Added threshold persistence settings and validation.
  - Removed dispatcher-level persistence parsing.
- `bits/src/dispatcher/mod.rs`
  - Dispatcher no longer performs persistence writes/heartbeats.
- `bits/src/bits.rs`
  - Submit path uses delayed persist timer.
  - Poll path enforces strict lease-gated reclaim.
  - Broker ID made unique per process start.
- `bits/src/job.rs`
  - Removed `persistent` field from runtime Job model.

---

## Verification

1. Unit tests in memory store cover:
   - upsert/claim/delete,
   - claim owner mismatch handling,
   - idempotent claim by same claimant,
   - broker lease lifecycle.
2. Existing workspace tests pass (`cargo test`).
3. Follow-up integration tests (recommended):
   - wrong-broker proxy while lease active,
   - reclaim only after lease expiry,
   - no reclaim on transient owner proxy failure with active lease.
