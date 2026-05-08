# Concepts

## Public request IDs and broker identity

Every submitted job receives a public request ID. It is an opaque 26-character string; clients must store and replay it unchanged, but should not parse it. See [Request IDs](request-ids.md) for the byte-level format.

BITS decodes the request ID internally to derive an owner hint from `(site, env, broker_slot)`. The corresponding stable internal broker ID is:

```text
{site}-{env}-{slot}
```

For example, site `bol`, env `dev`, slot `42` maps to broker ID `bol-dev-42`.

The owner hint is a fast path for locating the broker that originally accepted the job. It is not authoritative after recovery. The durable job record's `broker_id` field is the authoritative owner and is updated when another broker claims the job.

## Broker slots

When persistence is configured, each broker allocates a durable `u16` slot at startup from a counter scoped by `(site, env)`. Slot counters are stored in the persistence backend:

- NATS: `counters.broker_slot.{site}.{env}`
- TiKV: `counters/broker_slot/{site}/{env}`

Without persistence, BITS uses slot `0` for the in-memory-only broker.

## Threshold persistence

When `bits.persist_after_secs` is configured with a persistence backend, BITS applies a threshold before writing to the persistence store:

1. The job starts in memory immediately when submitted.
2. A timer is set for `persist_after_secs`.
3. If the job is still running when the timer fires, BITS writes one durable record containing `job_id`, authoritative owner `broker_id`, `original_request`, `user`, `metadata`, and `created_at`.
4. When the job reaches a terminal state, the durable record is deleted.

Jobs that complete before the threshold are never written to the store. Jobs that cross the threshold are written once. There are no per-job heartbeat writes.

## Liveness without per-job heartbeats

BITS determines owner liveness through broker leases. Each broker periodically renews a lease record in durable storage. If a broker's lease expires, that broker is considered unavailable and its persisted jobs are eligible for claim by live brokers.

The persistence store sees one write per long-running job, one delete on completion, and periodic lease renewals per broker.

## Store abstraction and key layout

The persistence layer is implemented behind `PersistenceStore`. Implementations include:

- `MemoryStore` for tests and in-memory operation
- `NatsStore` using NATS JetStream KV (build with `--features nats`)
- `TiKvStore` using TiKV (build with `--features tikv`)

Job keys are derived by decoding the public request ID and grouping records by site, environment, and slot:

- Memory: `{site}/{env}/{slot}/{job_id}`
- NATS: `jobs.{site}.{env}.{slot}.{job_id}`
- TiKV: `jobs/{site}/{env}/{slot}/{job_id}`

Broker lease keys are separate:

- NATS: `brokers.{broker_id}`
- TiKV: `brokers/{broker_id}`

This separation keeps public IDs opaque while giving the store an efficient, stable layout.
