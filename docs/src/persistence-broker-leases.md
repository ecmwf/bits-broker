# Broker Leases

Each BITS instance registers itself in the shared persistence store so that peer brokers can
locate it for [internal poll proxying](persistence-poll-recovery.md). This registration is
called a *broker lease*.

## Lease record

A lease record contains:

| Field | Description |
|---|---|
| `broker_id` | The unique per-process broker identity (`{configured_broker_id}-{uuid}`). |
| `internal_poll_base_url` | The URL at which this broker's poll endpoint is reachable from peers. |
| `lease_until` | Wall-clock timestamp after which this lease is considered expired. |
| `updated_at` | Timestamp of the last upsert. |

## Heartbeat

At startup, BITS spawns a background task that upserts the lease record on a repeating
interval. The renewal period is **TTL ÷ 2**, floored at 100 ms. For the default TTL of 30
seconds, the heartbeat fires approximately every 15 seconds.

There is no graceful shutdown signal. When a broker process exits, its lease record remains in
the store until the `lease_until` timestamp passes. Until then, peer brokers treat that owner
as live and will proxy polls to it (which will fail with a network error, returning `Pending`
to clients). Once the lease expires, peers will attempt to [claim and recover](persistence-poll-recovery.md)
any outstanding jobs.

## Expiry evaluation

BITS does not rely on any server-side TTL mechanism. The reading broker compares
`lease.lease_until > current_wall_time` to decide if the lease is active. Expired records are
never deleted from TiKV automatically — they remain until the broker comes back online and
overwrites them. This is safe because the timestamp comparison is the only check that matters.

## TiKV storage

Broker leases are stored at the key `brokers/{broker_id}` as a JSON-encoded record. Job records
are stored at `jobs/{job_id}`. These key spaces are disjoint.

## Configuration

The persistence backend is selected via `bits.persistence.type`:

### NATS JetStream KV

```yaml
bits:
  persistence:
    type: nats
    url: "nats://localhost:4222"
    jobs_bucket: "bits-jobs"         # default
    leases_bucket: "bits-leases"     # default
    broker_lease_ttl_secs: 30        # default: 30 seconds
    num_replicas: 1                  # use 3 for production
```

Build with `--features nats`. Requires `nats-server` with JetStream enabled.

### TiKV

```yaml
bits:
  persistence:
    type: tikv
    endpoints: ["pd:2379"]
    broker_lease_ttl_secs: 30        # default: 30 seconds
```

Build with `--features tikv`. Requires a TiKV cluster with PD.

### No persistence (in-memory only)

Omit the `persistence` section entirely. Jobs are lost on broker restart.

`broker_lease_ttl_secs` is the only tuning knob for lease lifetime. Setting it lower reduces
the window during which a crashed broker's jobs are unrecoverable, at the cost of more frequent
heartbeat writes.

> **Note:** Broker leases are only active when a persistence store is configured. Without
> `bits.persistence`, the heartbeat task does not start and no lease records are written.
