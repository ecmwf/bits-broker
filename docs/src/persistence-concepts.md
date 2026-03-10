# Concepts

## Owner-aware job IDs

Every job ID has the form `{broker_id}~{uuid}`.

The `broker_id` prefix encodes which broker originally accepted the job. This means any broker
that receives a poll for that job can determine the likely owner from the ID alone — without
scanning all broker instances or querying a central coordinator.

## Threshold persistence

When `bits.persist_after_ms` is configured, BITS applies a threshold before writing to the
persistence store:

1. The job starts in-memory immediately when submitted.
2. A timer is set for `persist_after_ms`.
3. If the job is still running when the timer fires, BITS writes one durable record containing:
   `job_id`, `broker_id` (owner), `original_request`, `user`, `metadata`, and `created_at`.
4. When the job reaches a terminal state (success, failure, or cancellation), the durable record
   is deleted.

Jobs that complete before the threshold are never written to the store. Jobs that cross the
threshold are written exactly once. There are no heartbeat writes for individual jobs.

## Liveness without per-job heartbeats

BITS determines owner liveness through **broker leases**, not per-job heartbeats. Each broker
periodically renews a lease record in durable storage. If a broker's lease expires, that broker
is considered unavailable and its persisted jobs are eligible for reclaim.

This means the persistence store sees one write per long-running job (on persist) and one delete
(on completion), plus periodic lease renewals per broker — not one heartbeat per in-flight job.

## Store abstraction

The persistence layer is implemented behind a trait interface, so the same behavior works with
different backends. An in-memory store (for testing) and a TiKV store (for production) both
implement this interface.

Two logical namespaces keep records separate:

- **Job records** — one record per persisted job.
- **Broker lease records** — one record per live broker.
