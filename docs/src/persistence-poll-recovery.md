# Poll Proxying and Recovery

On poll, a broker follows this sequence:

1. Check local in-memory state.
2. If missing, parse owner from `job_id`.
3. If owner is another broker, resolve owner endpoint from lease records and proxy internally.
4. If owner lease is missing/expired, attempt claim-and-recover from durable storage.

Important: proxy failure alone does not trigger reclaim while owner lease is still active.

Recovery behavior:

- If a persistent record exists and claim succeeds, the broker restores from
  `original_request`, preserves `created_at`, resubmits, and returns `Pending`.
- If no durable record exists, the result is `JobLost`.

This keeps the client contract stable while allowing broker ownership changes.
