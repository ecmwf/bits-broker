# Operational Notes

## Configuration reference

All options live under the `bits:` key in your YAML config file.

### Top-level options

| Key | Type | Default | Description |
|---|---|---|---|
| `site` | string/integer tag | *(required)* | 1-3 lowercase letters or digits identifying the deployment site. Encoded into request IDs. |
| `env` | string/integer tag | *(required)* | 1-3 lowercase letters or digits identifying the environment. Encoded into request IDs. |
| `internal_poll_endpoint` | string | auto-derived from `server.host`/`port` | URL at which peer brokers can reach this instance's poll endpoint. Set explicitly for multi-broker deployments. |
| `internal_poll_timeout_secs` | float | `2.5` | Timeout for outbound proxy poll requests to peer brokers. |
| `sweep_interval_secs` | float | `180.0` | How often the sweeper thread evicts completed, unpolled jobs from memory. |
| `reconnect_buffer_secs` | float | `5.0` | Grace period after a client disconnect before the job is eligible for sweep. |
| `persist_after_secs` | float | *(none)* | Threshold before a job is written to durable storage. Has no effect without `bits.persistence`. |

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

## Request IDs and ownership

Public request IDs are opaque. BITS decodes them internally to derive `(site, env, slot)` as an owner hint, then forms the internal broker ID `{site}-{env}-{slot}`. Durable job records carry the authoritative owner in `broker_id`, and that value may change after recovery.

Slot allocation is monotonic per `(site, env)` and uses backend counters. If the `u16` slot range is exhausted, startup fails; recovery requires a new request-ID version before more slots can be allocated for that site/env pair.

## Config validation

When persistence is enabled, BITS validates that `persist_after_secs + 1s < server.poll_timeout_secs`. The guard gives the persistence write time to complete before a client's poll window could expire.

## Tuning guidance

### `persist_after_secs`

Set this comfortably shorter than the expected client poll timeout. Jobs that complete before this threshold never touch storage.

### `broker_lease_ttl_secs`

This controls the recovery delay after a broker disappears. Lower TTLs recover faster and write heartbeats more often; higher TTLs reduce write pressure and increase client wait time after a crash.

### Sticky ingress

Use load-balancer affinity on a stable client property such as `Authorization`. This maximises local in-memory poll hits. Do not route clients by parsing request IDs.

## Failure mode reference

| Scenario | Behaviour |
|---|---|
| Store unreachable at lease lookup | `Pending` returned to client. No claim attempted. |
| Store unreachable at claim | Exponential backoff with a `min(timeout, 2 s)` budget. Returns `Pending` on exhaustion. |
| Two brokers claim the same job | Backend compare-and-swap ensures only one wins. Loser returns `Pending`; next poll finds the new owner. |
| `upsert_job` fails at persist threshold | Logged as a warning; job continues in memory. If the broker then crashes, the job is unrecoverable. |
| `delete_job` fails at completion | Logged as a warning; the stale durable record remains and may need cleanup. |
| Broker crashes | Jobs become recoverable after the owner's lease expires. |
| `persistence.type` set, feature not compiled | Startup fails. |
| Missing required backend fields | Startup fails. |

## Hard cutover and orphan cleanup

The current request-ID format and key layout are a hard cutover boundary. After deploying it, persisted records and leases written by an incompatible format are not discoverable by normal poll recovery.

Operationally, drain or stop old brokers before deploying the new format. After all old leases have expired and no old-format work is expected to complete, remove orphaned job records, broker lease records, and slot counters from the old key spaces using the backend's administrative tools. Keep the cleanup scoped to BITS buckets/prefixes and take a backup first.
