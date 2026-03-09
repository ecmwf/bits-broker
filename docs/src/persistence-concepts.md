# Concepts

## Owner-aware job IDs

Jobs use owner-aware identifiers in the form `{broker_id}~{uuid}`.

This allows any broker receiving a poll request to determine likely ownership without scanning
all instances.

## Persistent dispatcher behavior

With `dispatcher.persistent: true`:

1. The broker writes or updates the persistent job record before enqueueing.
2. While executing, lock heartbeats are renewed.
3. On completion, the durable record is removed.

Persistence is therefore tied to the execution boundary where queueing and coordination happen.

## Store abstraction

The persistence layer is abstracted behind traits so in-memory and TiKV backends can implement
the same behavior.

Logical namespaces separate job records and broker lease records.
