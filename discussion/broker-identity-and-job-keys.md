# Broker identity and job-key notes

## Context

This note captures decisions and open questions from the broker identity discussion so they can be revisited during future persistence and routing changes.

## Current behavior (as implemented)

- `job_id` currently includes an owner hint (`{owner}~{job_uuid}`).
- Poll path parses owner from `job_id`.
- Poll currently has a local short-circuit: if owner in `job_id` equals current broker id and local job map misses, it returns `NotFound`.
- Reclaim updates durable `record.broker_id` in DB, but the original `job_id` string remains unchanged.

## Why broker-id reuse is risky in current shape

- If a restarted broker reuses the same broker id, old `job_id` owner hints can match the new process id.
- With the local short-circuit, this can produce incorrect `NotFound` before reclaim path runs.
- Therefore, current model expects runtime broker identity to be unique per process lifetime.

## Important distinction: owner hint vs authoritative owner

- Owner in `job_id` is a routing hint.
- Owner in durable DB record (`record.broker_id`) is authoritative after reclaim.
- After reclaim, hint may be stale while DB owner is current.

## Lookup-path discussion

- Main poll path already avoids a job-record lookup in healthy owner cases:
  1. Parse owner from URL/job id.
  2. Check owner broker lease.
  3. Proxy if lease is active.
- Job-record claim lookup happens only when owner lease is missing/expired.
- Storing "fully qualified owner+job" keys in DB helps organization, but does not necessarily reduce common-case poll lookup count.

## Option discussed: reusable short broker ids

Short ids (for example numeric slots) are possible, but require careful handling.

- Safe path with reusable ids:
  - Remove/relax the local short-circuit on owner==self local miss.
  - On local miss, continue through DB-backed lease/claim path.
  - Keep DB owner as source of truth for reclaim/proxy decisions.
- Additional guard still needed:
  - Prevent two live brokers from holding same active id (lease registration must enforce uniqueness).

## Potential future shapes

1. Keep owner in public handle
- Pros: direct owner lookup from URL/job id.
- Cons: internal detail leaked into public id format.

2. Path-based owner + opaque job id
- Example: `/job/{owner}/{job_id}`.
- Pros: clearer external semantics (`job_id` stays pure).
- Cons: still requires owner lifecycle rules and stale-hint handling.

3. Opaque public id, owner fully DB-resolved
- Pros: no internal routing detail in public id.
- Cons: more DB dependency on poll misses.

## Open questions to revisit

- Should local owner short-circuit be removed to permit safe broker-id reuse?
- Do we want stable short broker slots, or per-start unique runtime ids only?
- Should owner hint remain part of public API, or move to path/internal tokening?
- If using short reusable ids, do we need slot+incarnation identity internally?
