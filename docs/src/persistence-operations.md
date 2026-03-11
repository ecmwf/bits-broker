# Operational Notes

## Configuration reference

All options live under the `bits:` key in your YAML config file.

### Top-level options

| Key | Type | Default | Description |
|---|---|---|---|
| `broker_id` | string | `"broker-{uuid}"` | Stable prefix embedded in every job ID. The full per-process identity is `{broker_id}-{uuid}`. |
| `internal_poll_base_url` | string | `"http://127.0.0.1:8080/job"` | URL at which **peer brokers** can reach this instance's poll endpoint. |
| `internal_poll_timeout_ms` | integer (ms) | `2500` | Timeout for outbound proxy poll requests to peer brokers. |
| `job_cleanup_interval_ms` | integer (ms) | `5000` | How often the sweeper thread evicts completed, unpolled jobs from memory. |
| `persist_after_ms` | integer (ms) | *(none)* | Threshold before a job is written to durable storage. Omitting this key disables persistence. |
| `poll_timeout_ms` | integer (ms) | `30000` | Used only for config validation (see below). Not enforced at runtime. |
| `persist_guard_ms` | integer (ms) | `1000` | Used only for config validation (see below). Not enforced at runtime. |

### TiKV options (`bits.tikv`)

| Key | Type | Default | Description |
|---|---|---|---|
| `endpoints` | list of strings | *(required)* | TiKV PD endpoints, e.g. `["pd:2379"]`. Must be non-empty. |
| `broker_lease_ttl_secs` | float | `30.0` | Lease TTL in seconds. Heartbeat renews at TTL ÷ 2. |

## Config validation

At startup, BITS rejects configurations where:

```
persist_after_ms + persist_guard_ms >= poll_timeout_ms
```

This check ensures a job has time to be persisted before any client's poll window could expire.
Adjust `persist_after_ms` downward (write sooner) or `poll_timeout_ms` upward (wider window) if
you hit this error.

## Removed configuration options

BITS actively rejects several legacy keys to prevent silent misconfiguration:

| Old key | Replacement |
|---|---|
| `dispatcher.persistent` | Use `bits.persist_after_ms` |
| `dispatcher.lock_ttl_secs` | Use `bits.tikv.broker_lease_ttl_secs` |
| `persist` as a route step name | Persistence is now threshold-based; remove the step |

## Tuning guidance

### `persist_after_ms`

Set this to a value comfortably shorter than your clients' expected poll timeout window. For
example, if clients poll with a 30-second timeout and you require a 1-second guard buffer,
`persist_after_ms: 28000` is a reasonable starting point.

Jobs that complete before this threshold are never written to storage, so short requests pay no
I/O cost for persistence.

### `broker_lease_ttl_secs`

This controls the window during which jobs are unrecoverable after a broker crashes. A lower
TTL (e.g., 10 s) means faster recovery but more frequent TiKV writes. A higher TTL reduces
write pressure at the cost of longer client wait times after a crash.

### Sticky ingress

Configure your load balancer to hash incoming requests on a stable client property (for example
`Authorization`). This maximises the fraction of polls that hit the owning broker and return
from local in-memory state, avoiding proxy or recovery paths entirely.

## Failure mode reference

| Scenario | Behaviour |
|---|---|
| Store unreachable at lease lookup | `Pending` returned to client. No claim attempted. |
| Store unreachable at claim (repeated) | Exponential backoff (100 ms → 1 s), budget `min(timeout, 2 s)`. Returns `Pending` on exhaustion. |
| Two brokers claim the same job simultaneously | TiKV optimistic transaction ensures only one wins. Loser returns `Pending`; next poll finds the new owner. |
| `upsert_job` fails at persist threshold | Logged as a warning; job continues in memory. If the broker subsequently crashes, the job is unrecoverable. |
| `delete_job` fails at job completion | Error is silently ignored. The stale record remains in TiKV but is harmless — the reading broker will see an active lease for the owning broker. |
| Broker crashes without lease cleanup | Lease expires after `broker_lease_ttl_secs` (default 30 s). Jobs become recoverable after that window. Old lease records are never purged from TiKV automatically. |
| `tikv` config present, feature not compiled | `Bits::from_config` returns an error immediately at startup. |
| Empty `tikv.endpoints` | `Bits::from_config` returns an error immediately at startup. |
