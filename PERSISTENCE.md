# Plan: TiKV-backed Job Persistence

## Context

Long-running dispatched jobs are ephemeral today — if a broker crashes, in-flight jobs are lost and clients get no result. We need persistence so that after a broker failure (or TTL expiry), any surviving broker can reclaim the job from the database and re-run it, preserving the original submission time so age-priority queues (CostWeighted) still promote recovered jobs correctly.

Decisions:
- **Persist at dispatcher level** (not pipeline step): `persistent: true` on a dispatcher config. Removes the built-in `Persist` action entirely.
- **Result delivery is in-memory only**: no result stored in DB; client must poll while broker is live.
- **Shared LB topology**: any broker can answer a poll; poll-miss triggers DB lookup + optional reclaim.
- **Recovery = full re-run** from `original_request`, preserving `created_at`.

---

## Deployment Topology

**StatefulSet** (not Deployment):
- Pods have stable internal DNS: `broker-0.bits.svc.cluster.local`, etc.
- `broker_id` = `POD_NAME` env var (k8s injects automatically for StatefulSets)
- LB is **pure round-robin** nginx — no special hashing needed

**Job ID format: `{broker_id}/{uuid}`**
- Example: `broker-2/550e8400-e29b-41d4-a716-446655440000`
- The owning broker is self-describing in every job ID
- URL-safe; `GET /poll/broker-2/550e8400-...` is a natural URL structure
- Clients don't need sticky sessions or routing hints — they just pass the opaque job ID

**Poll routing (any broker can receive any poll)**:
1. Parse `broker_id` prefix from job_id
2. If prefix == self → serve from local job_map
3. If prefix != self → **proxy internally** to `{prefix}.{svc_domain}/poll/{job_id}`
4. If proxy fails (pod dead):
   - Persistent job → DB claim (force) + re-run → Pending
   - Non-persistent job → `JobLost` error (job was on dead broker, no DB record)

## Architecture Overview

```
Dispatcher::dispatch() [persistent=true]
  ├─ tokio::spawn: store.upsert(record)       // Write lock to DB before enqueue
  ├─ queue.enqueue(job)
  └─ return future that:
       ├─ spawns heartbeat task (renew lock every ttl/2)
       ├─ awaits reply_rx
       ├─ aborts heartbeat
       └─ tokio::spawn: store.delete(job_id)  // Clean up on completion

Bits::poll(job_id) [job not in local job_map]
  ├─ Parse broker prefix from job_id
  ├─ If prefix == self.broker_id:
  │    check DB → Claimed → restore_and_submit → Pending
  │              → NotFound → NotFound
  └─ If prefix != self.broker_id:
       proxy to {prefix}.{svc_domain}/poll/{job_id}
         ├─ Success  → return proxied result
         └─ Failure (pod dead):
              check DB → Claimed → restore_and_submit → Pending
                       → Active  → proxy to DB owner → return result
                       → NotFound:
                           persistent  → force_claim + restore → Pending
                           ephemeral   → JobLost
```

---

## DB Module (`bits/src/db/`)

### `mod.rs` — trait + types

```rust
pub struct PersistentJobRecord {
    pub job_id: String,
    pub broker_id: String,
    pub locked_until: DateTime<Utc>,
    pub original_request: Value,
    pub user: Value,
    pub metadata: Value,
    pub created_at: DateTime<Utc>,
}

pub enum ClaimResult {
    NotFound,
    Active,                          // Valid lock held by another broker
    Claimed(PersistentJobRecord),    // Was expired; now owned by caller
}

#[async_trait]
pub trait JobStore: Send + Sync {
    async fn upsert(&self, record: PersistentJobRecord) -> Result<(), DbError>;
    async fn renew_lock(&self, job_id: &str, broker_id: &str, ttl: Duration) -> Result<(), DbError>;
    async fn delete(&self, job_id: &str) -> Result<(), DbError>;
    async fn try_claim_expired(&self, job_id: &str, broker_id: &str, ttl: Duration) -> Result<ClaimResult, DbError>;
}
```

### `tikv.rs` — TiKV transactional client

- Use `tikv-client` crate with the **transactional API** for CAS semantics on lock claims.
- Key: `job:{job_id}` (bytes)
- Value: JSON-encoded `PersistentJobRecord`
- `try_claim_expired`: begin txn → read → if missing=NotFound, if `locked_until > now`=Active, else update broker_id+locked_until → commit. Retry once on conflict.

### `memory.rs` — in-memory store for tests

- `DashMap<String, PersistentJobRecord>` with Mutex for CAS simulation.

---

## Changes by File

### `bits/src/bits.rs`
- Add `broker_id: String`, `job_store: Option<Arc<dyn JobStore>>`, `internal_client: reqwest::Client`, `svc_domain: String` to `Bits` struct.
- Job IDs generated as `{broker_id}/{uuid}` (new `fn new_job_id(&self) -> String`).
- `poll()` on map miss: parse broker prefix from job_id → if self, check DB directly; if other, proxy first then fall back to DB (see Architecture flow above).
- New helper `fn restore_and_submit(&self, record: PersistentJobRecord)`: rebuilds `Job::restore(record)` → `self.submit(job)`.
- `fn proxy_poll(&self, broker_id: &str, job_id: &str, timeout: Duration) -> PollOutcome`: internal HTTP call to `http://{broker_id}.{svc_domain}/poll/{job_id}?timeout_ms=N`.

### `bits/src/config.rs`
- Add `BrokerConfig` fields: `broker_id: Option<String>`, `tikv: Option<TiKVConfig>`.
- `TiKVConfig { endpoints: Vec<String>, lock_ttl_secs: f64 }`.
- `parse_dispatcher_fields()`: extract `persistent: bool` and optional `lock_ttl_secs`.
- `Dispatcher::from_config()` signature gains `job_store: Option<Arc<dyn JobStore>>`, `broker_id: String`, `lock_ttl: Duration` parameters.
- Remove `Action::Persist` variant (and `persist` pipeline step parsing).

### `bits/src/dispatcher/mod.rs`
- Add fields to `Dispatcher<T>`: `job_store: Option<Arc<dyn JobStore>>`, `broker_id: String`, `lock_ttl: Duration`.
- In `dispatch()`:
  - If `job_store` is Some: spawn DB upsert, then inside returned future spawn heartbeat + delete on completion (see architecture above).

### `bits/src/job.rs`
- Add `Job::restore(record: PersistentJobRecord) -> Job`:
  - Sets `id = record.job_id`, `original_request = record.original_request.clone()`, `request = record.original_request`, `user`, `metadata`, `created_at = record.created_at` (preserved!), `persistent = true`.
  - Fresh `Arc<AtomicBool>`, `Arc<Mutex<Instant>>`, etc.

### `bits/src/actions/mod.rs`
- Remove `Action::Persist` variant.
- Remove corresponding pipeline execution arm.

### `Cargo.toml` (`bits/Cargo.toml`)
- Add `tikv-client = "0.3"` (check latest version).

---

## Config Examples

### Abstract (`design_config.yaml`)

```yaml
bits:
  broker_id: "${POD_NAME}"                           # injected by k8s StatefulSet
  svc_domain: bits.default.svc.cluster.local         # internal proxy DNS suffix
  tikv:
    endpoints: ["tikv-pd.default.svc.cluster.local:2379"]
    lock_ttl_secs: 300

targets:
  mars_od:
    type: http
    url: "http://mars.ecmwf.int:8080/retrieve"
    queue: cost_weighted
    concurrency: 10
    persistent: true                                 # replaces `persist` pipeline step

  dss_od:
    type: http
    url: "http://dss.ecmwf.int:9090/retrieve"
    queue: fifo
    concurrency: 8
    persistent: true

  fdb_worker:
    type: remote
    executor: remote_pool                            # explicit (was implicit)
    queue: cost_weighted
    concurrency: 50
    persistent: true

routes:
  operational_forecast:
    - transform::expand
    - check::valid_data
    - target::mars_od                                # `persist` step removed

  era5_reanalysis:
    - transform::expand
    - switch:
        era5_privileged:
          - check::era5_data
          - check::era5_license
          - target::dss_od                          # `persist` step removed
        era5_public:
          - check::era5_data
          - target::dss_od
```

### ECMWF (`bits-ecmwf/examples/basic_usage.yaml`)

Same pattern: add `bits:` section, add `persistent: true` to `mars_od` and `dss_od`, remove `persist` pipeline steps.

---

## Critical Files

| File | Change |
|------|--------|
| `bits/src/db/mod.rs` | **NEW** — trait + types |
| `bits/src/db/tikv.rs` | **NEW** — TiKV impl |
| `bits/src/db/memory.rs` | **NEW** — test impl |
| `bits/src/bits.rs` | Add broker_id, job_store; poll() DB fallback |
| `bits/src/config.rs` | Parse tikv/broker/persistent fields |
| `bits/src/dispatcher/mod.rs` | Inject job_store, heartbeat, cleanup |
| `bits/src/job.rs` | Add `Job::restore()` |
| `bits/src/actions/mod.rs` | Remove `Action::Persist` |
| `bits/Cargo.toml` | Add tikv-client |

---

## Internal Proxy Detail

For any poll request where job_id's broker prefix != self:
1. Make `GET http://{prefix}.{svc_domain}/poll/{job_id}?timeout_ms=N` — same timeout as original request minus overhead
2. On success: stream result back to original client
3. On failure (pod dead, connection refused):
   - If `job_store` is Some: `try_claim_expired` (force=true if needed) → restore + re-run
   - If no job_store: return `PollOutcome::JobLost`
4. `svc_domain` configurable in `BitsConfig` (default: `bits.default.svc.cluster.local`)

## Open Questions / Noted Risks

1. **`Action::Persist` removal**: Any existing config files or tests using `persist:` pipeline step will break. Audit before removing.
2. **Broker-id source**: For StatefulSet, use `POD_NAME` env var; for local dev/test, auto-generate UUID at startup and log it.
3. **TiKV not configured**: All `persistent: true` dispatchers must fail at startup (not silently) if no TiKV config is provided.
4. **Heartbeat abort on early cancel**: If the job is cancelled before the reply arrives, the returned future may be dropped — ensure heartbeat handle is aborted via a `Drop` guard or `tokio::select!`.
5. **Force-claim on proxy failure**: When the internal proxy fails (upstream dead), we need to claim even if `locked_until` hasn't expired yet. The `try_claim_expired` API needs a `force: bool` or a separate `force_claim()` method.
6. **Non-persistent job on dead broker**: returns `JobLost` (new `PollOutcome` variant). Client must resubmit. This is acceptable — the broker that owned the job is gone.

---

## Verification

1. **Unit tests** (`db/memory.rs`): upsert → get, upsert → renew, upsert → delete, try_claim while valid (→ Active), try_claim after expiry (→ Claimed), concurrent claim race (only one winner).
2. **Integration test**: submit job to persistent dispatcher → kill broker process → start new broker → poll → confirm job is reclaimed and re-run → result delivered.
3. **Age preservation test**: submit two jobs; let first expire and be reclaimed; confirm reclaimed job has older `created_at` and is dequeued first by CostWeightedQueue.
4. Run existing test suite: `cargo test` — all tests should still pass (memory store injected in tests).
