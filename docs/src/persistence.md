# Persistence

BITS persistence is owner-aware and time-threshold based.

When `bits.persist_after_ms` is configured, a job is persisted once after it has been in-flight for
that duration. Short jobs avoid database writes, while longer jobs become recoverable.

Reclaim is strict: a non-owner broker only attempts claim/replay when the owner broker lease is
missing or expired. Transient proxy failures do not trigger reclaim while the owner lease is valid.

This section covers:

- Persistence concepts and data model
- Sticky routing versus correctness guarantees
- Internal poll proxying and recovery behavior
- Broker lease lifecycle
- Operational guidance
