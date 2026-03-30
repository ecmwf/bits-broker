# Poll Proxying and Recovery

When a poll request arrives for a job that is not in local memory, the broker follows a
structured decision sequence to locate the result or recover the job.

## Decision sequence

```
poll(id)
  │
  ├─ 1. Check local in-memory state
  │       ↓ found → long-poll, return result
  │       ↓ not found
  │
  ├─ 2. Parse owner from job_id (split on first '~')
  │       ↓ unparseable → NotFound
  │       ↓ owner == self → NotFound  (we are authoritative; job never existed or was evicted)
  │       ↓ owner == another broker
  │
  ├─ 3. Look up owner's broker lease
  │       ↓ Active lease     → Step 4a (proxy)
  │       ↓ Missing/expired  → Step 4b (claim-and-recover)
  │       ↓ Store unreachable → Pending (no claim attempted)
  │
  ├─ 4a. Proxy to owner's internal_poll_base_url/{id}
  │       ↓ success → return translated response (see table below)
  │       ↓ network/timeout failure → Pending (no claim while lease is active)
  │
  └─ 4b. Claim-and-recover from durable storage
          ↓ Claimed     → restore job, long-poll, return Pending
          ↓ Active(new) → look up new owner's lease; proxy if active, else Pending
          ↓ NotFound    → JobLost
          ↓ Error       → Pending (with exponential backoff retries)
```

## Proxy response translation

When proxying to the owner broker, BITS translates the HTTP response back to a `PollOutcome`:

| Owner response | Caller receives |
|---|---|
| `200 OK` | `Ready(Success)` - body streamed through |
| `303`/`307` with `Location` containing the job ID | `Pending` - job is still in progress |
| `303`/`307` with `Location` not containing the job ID | `Ready(Redirect)` - result is a redirect |
| `404 Not Found` | `NotFound` |
| `400 Bad Request` | `Ready(Error)` |
| `410 Gone` | `Ready(Cancelled)` |
| `5xx` | `Pending` - owner is alive but errored; client should retry |
| Network/timeout failure | `Pending` - no ownership transfer while lease is active |

## Claim and recovery

Recovery is only attempted when the owner's broker lease is missing or expired. The broker
runs `claim_with_backoff`:

- Budget: `min(requested_timeout, 2 s)`
- Backoff: starts at 100 ms, doubles each retry, capped at 1 s
- Uses backend-specific CAS: optimistic transaction in TiKV, revision-based update with retry in NATS

On a successful claim, `Job::restore` reconstructs the job from the durable record:

- `request` is reset to `original_request`. The pipeline re-runs from the beginning
- `created_at` is preserved from the original submission timestamp
- All runtime state (cancelled flag, client-connected flag, etc.) starts fresh

The job is submitted immediately and the polling request is attached to it, so the client
receives `Pending` in response to the recovery poll without an extra round-trip.

## Claim outcomes

| `ClaimResult` | Action |
|---|---|
| `Claimed(record)` | Job restored and submitted; poll attaches and waits. Returns `Pending`. |
| `Active { owner_broker_id }` | Another broker won the race. Look up its lease; proxy if active, else `Pending`. |
| `NotFound` | No durable record exists. Returns `JobLost`. |
| `Conflict` (commit race) | Returns `Pending`; next poll will find the winner. |
| Backend error (after retries) | Returns `Pending`. |

## Client reconnect window

After each `poll_local` call returns, the broker extends a 5-second reconnect deadline. A job
considers the client "present" while `client_connected` is set **or** while the reconnect
deadline has not yet elapsed. This gives the client 5 seconds to reconnect between polls before
the dispatch pipeline treats the connection as lost and stops work.
