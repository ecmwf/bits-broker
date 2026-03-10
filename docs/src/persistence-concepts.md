# Concepts

## Owner-aware job IDs

Jobs use owner-aware identifiers in the form `{broker_id}~{uuid}`.

This allows any broker receiving a poll request to determine likely ownership without scanning
all instances.

## Threshold persistence behavior

With `bits.persist_after_ms` configured:

1. Job starts immediately in-memory.
2. If still running at the threshold, the broker writes a durable job record once.
3. On completion, the durable record is removed.

No per-job lock heartbeat is required; owner liveness comes from broker lease TTL.

## Store abstraction

The persistence layer is abstracted behind traits so in-memory and TiKV backends can implement
the same behavior.

Logical namespaces separate job records and broker lease records.
