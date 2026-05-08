# Broker Leases

Each persistent BITS instance registers itself in the shared store so peer brokers can locate it for [internal poll proxying](persistence-poll-recovery.md). This registration is a broker lease.

## Lease record

A lease record contains:

| Field | Description |
|---|---|
| `broker_id` | Stable internal broker identity for this process: `{site}-{env}-{slot}`. |
| `internal_poll_base_url` | URL at which this broker's poll endpoint is reachable from peers, set with `bits.internal_poll_endpoint`. |
| `lease_until` | Wall-clock timestamp after which this lease is considered expired. |
| `updated_at` | Timestamp of the last upsert. |

The `broker_id` is internal. Public request IDs remain opaque to clients.

## Heartbeat

At startup, a persistent broker allocates a durable slot for its `(site, env)` pair, forms its internal broker ID, and starts a heartbeat task. The task upserts the lease record every TTL ÷ 2, floored at 100 ms. For the default TTL of 30 seconds, renewal happens about every 15 seconds.

When a broker exits, its lease record may remain until `lease_until` passes. Until then, peers treat that owner as live and proxy polls to it. After expiry, peers may claim persisted jobs owned by that broker.

## Expiry evaluation

The reading broker compares `lease.lease_until > current_wall_time` to decide if the lease is active. Backend cleanup is secondary:

- **NATS**: The leases bucket uses `max_age` at 2× TTL; JetStream removes old entries.
- **TiKV**: Expired records remain until overwritten or manually cleaned; timestamp comparison is authoritative.

## Storage layout

Broker leases and job records are JSON values in separate key spaces:

- NATS broker leases: `brokers.{broker_id}`
- TiKV broker leases: `brokers/{broker_id}`
- NATS job records: `jobs.{site}.{env}.{slot}.{job_id}`
- TiKV job records: `jobs/{site}/{env}/{slot}/{job_id}`

Broker slot counters are also scoped by `(site, env)`:

- NATS: `counters.broker_slot.{site}.{env}`
- TiKV: `counters/broker_slot/{site}/{env}`

## Configuration

The persistence backend is selected via `bits.persistence.type`:

### NATS JetStream KV

```yaml
bits:
  site: bol
  env: dev
  internal_poll_endpoint: "http://bits-0.bits-headless:8080/job"
  persistence:
    type: nats
    url: "nats://localhost:4222"
    jobs_bucket: "bits-jobs"
    leases_bucket: "bits-leases"
    broker_lease_ttl_secs: 30
    num_replicas: 1
```

Build with `--features nats`. Requires NATS with JetStream enabled.

### TiKV

```yaml
bits:
  site: bol
  env: prd
  internal_poll_endpoint: "http://bits-0.bits-headless:8080/job"
  persistence:
    type: tikv
    endpoints: ["pd:2379"]
    broker_lease_ttl_secs: 30
```

Build with `--features tikv`. Requires a TiKV cluster with PD.

### No persistence

Omit `bits.persistence`. Jobs are in memory only; no lease records are written and the broker slot is `0`.

`broker_lease_ttl_secs` controls the lease lifetime. Lower values reduce recovery delay after broker loss and increase heartbeat write frequency.
