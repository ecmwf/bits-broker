# Operational Notes

## Configuration reference

All options live under the `bits:` key in your YAML config file.

### Top-level options

| Key | Type | Default | Description |
|---|---|---|---|
| `broker_id_prefix` | string | `"broker-{uuid}"` | Stable prefix embedded in every job ID. The full per-process identity is `{broker_id_prefix}-{uuid}`. |
| `internal_poll_endpoint` | string | auto-derived from `server.host`/`port` | URL at which **peer brokers** can reach this instance's poll endpoint. When not set, BITS derives this from `server.host` and `server.port`. If `server.host` is a wildcard bind (`0.0.0.0` or `::`), it falls back to a loopback address and emits a warning; set this explicitly for multi-broker deployments. |
| `internal_poll_timeout_secs` | float (secs) | `2.5` | Timeout for outbound proxy poll requests to peer brokers. |
| `sweep_interval_secs` | float (secs) | `180.0` | How often the sweeper thread evicts completed, unpolled jobs from memory. |
| `persist_after_secs` | float (secs) | *(none)* | Threshold before a job is written to durable storage. Omitting this key disables persistence. |

### Persistence options (`bits.persistence`)

| Key | Type | Default | Description |
|---|---|---|---|
| `type` | string | *(required)* | Backend type: `nats` or `tikv`. |
| `broker_lease_ttl_secs` | float | `30.0` | Lease TTL in seconds. Heartbeat renews at TTL ÷ 2. |
| `url` | string | *(nats only, required)* | NATS server URL, e.g. `"nats://localhost:4222"`. |
| `jobs_bucket` | string | `"bits-jobs"` | *(nats only)* KV bucket name for job records. |
| `leases_bucket` | string | `"bits-leases"` | *(nats only)* KV bucket name for broker leases. |
| `num_replicas` | integer | `1` | *(nats only)* JetStream replication factor. Use 3 for production. |
| `endpoints` | list of strings | *(tikv only, required)* | TiKV PD endpoints, e.g. `["pd:2379"]`. |

## Config validation

When persistence is enabled, BITS validates at startup that
`persist_after_secs + 1s < server.poll_timeout_secs`. The 1-second guard
accounts for I/O jitter on the persistence write. This check ensures a job
has time to be persisted before any client's poll window could expire.
Adjust `persist_after_secs` downward (write sooner) or
`server.poll_timeout_secs` upward (wider window) if you hit this error.

## Removed configuration options

BITS actively rejects several legacy keys to prevent silent misconfiguration:

| Old key | Replacement |
|---|---|
| `dispatcher.persistent` | Use `bits.persist_after_secs` |
| `dispatcher.lock_ttl_secs` | Use `bits.persistence.broker_lease_ttl_secs` |
| `bits.tikv` | Use `bits.persistence` with `type: tikv` |
| `bits.nats` | Use `bits.persistence` with `type: nats` |
| `persist` as a route step name | Persistence is now threshold-based; remove the step |

## Tuning guidance

### `persist_after_secs`

Set this to a value comfortably shorter than your clients' expected poll timeout window. For
example, if clients poll with a 30-second timeout, `persist_after_secs: 28.0` is a reasonable
starting point.

Jobs that complete before this threshold are never written to storage, so short requests pay no
I/O cost for persistence.

### `broker_lease_ttl_secs`

This controls the window during which jobs are unrecoverable after a broker crashes. A lower
TTL (e.g., 10 s) means faster recovery but more frequent persistence writes. A higher TTL
reduces write pressure at the cost of longer client wait times after a crash.

### Sticky ingress

Configure your load balancer to hash incoming requests on a stable client property (for example
`Authorization`). This maximises the fraction of polls that hit the owning broker and return
from local in-memory state, avoiding proxy or recovery paths entirely.

## Failure mode reference

| Scenario | Behaviour |
|---|---|
| Store unreachable at lease lookup | `Pending` returned to client. No claim attempted. |
| Store unreachable at claim (repeated) | Exponential backoff (100 ms → 1 s), budget `min(timeout, 2 s)`. Returns `Pending` on exhaustion. |
| Two brokers claim the same job simultaneously | Backend-specific CAS ensures only one wins (TiKV: optimistic transaction, NATS: revision-based update). Loser returns `Pending`; next poll finds the new owner. |
| `upsert_job` fails at persist threshold | Logged as a warning; job continues in memory. If the broker subsequently crashes, the job is unrecoverable. |
| `delete_job` fails at job completion | Logged as a warning (`durable record cleanup failed`). The stale record remains in the store. While the owning broker's lease is active, peers still see that lease. If the owner later loses its lease, the stale record may be treated as recoverable. |
| Broker crashes without lease cleanup | Lease expires after `broker_lease_ttl_secs` (default 30 s). Jobs become recoverable after that window. NATS leases auto-expire via bucket `max_age`; TiKV lease records require application-level expiry checks. |
| `persistence.type` set, feature not compiled | `Bits::from_config` returns an error immediately at startup. |
| Missing required backend fields | `Bits::from_config` returns an error immediately at startup (e.g., empty `endpoints` for TiKV, empty `url` for NATS). |
