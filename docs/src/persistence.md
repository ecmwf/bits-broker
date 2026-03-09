# Persistence

BITS persistence is dispatcher-level and owner-aware.

When a dispatcher is marked persistent, the broker stores durable job state before queueing and
maintains lock heartbeats while work is active. This allows recovery after broker failure and
supports multi-broker ownership handoff.

This section covers:

- Persistence concepts and data model
- Sticky routing versus correctness guarantees
- Internal poll proxying and recovery behavior
- Broker lease lifecycle
- Operational guidance
