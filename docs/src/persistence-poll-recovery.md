<!--
SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)

SPDX-License-Identifier: Apache-2.0
-->

# Poll Proxying and Recovery

When a poll request arrives for a job that is not in local memory, the broker follows a lease-gated sequence to locate the result or recover the job.

## Decision sequence

```text
poll(id)
  │
  ├─ 1. Check local in-memory state
  │       ↓ found → long-poll, return result
  │       ↓ not found
  │
  ├─ 2. Decode request ID
  │       ↓ invalid → NotFound
  │       ↓ valid → derive hinted broker ID: {site}-{env}-{slot}
  │
  ├─ 3. Look up hinted broker lease
  │       ↓ Active lease     → Step 4a (proxy)
  │       ↓ Missing/expired  → Step 4b (load durable record)
  │       ↓ Store unreachable → Pending
  │
  ├─ 4a. Proxy to lease.internal_poll_base_url/{id}
  │       ↓ success → return translated response
  │       ↓ network/timeout failure → Pending
  │
  └─ 4b. Claim-and-recover from durable storage
          ↓ Record owner differs from hint and owner lease active → proxy to authoritative owner
          ↓ Claimed     → restore job, long-poll, return Pending
          ↓ Active(new) → look up new owner's lease; proxy if active, else Pending
          ↓ NotFound    → JobLost
          ↓ Error       → Pending
```

The owner decoded from the request ID is only a hint. The durable job record's `broker_id` is authoritative once a job has been persisted and may change after recovery.

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

Recovery is only attempted when the relevant broker lease is missing or expired. The broker runs `claim_with_backoff`:

- Budget: `min(requested_timeout, 2 s)`
- Backoff: starts at 100 ms, doubles each retry, capped at 1 s
- Uses backend-specific compare-and-swap semantics: optimistic transaction in TiKV, revision-based update in NATS

On a successful claim, `Job::restore` reconstructs the job from the durable record:

- `request` is reset to `original_request`; the pipeline runs again from the beginning.
- `created_at` is preserved from the original submission.
- Runtime state starts fresh.

The polling request attaches to the restored job and receives `Pending` unless the job finishes within the poll timeout.

## Claim outcomes

| `ClaimResult` | Action |
|---|---|
| `Claimed(record)` | Job restored and submitted; poll attaches and waits. Returns `Pending` unless completed during the wait. |
| `Active { owner_broker_id }` | Another broker owns the record. Look up its lease; proxy if active, else `Pending`. |
| `NotFound` | No durable record exists. Returns `JobLost`. |
| `Conflict` | Returns `Pending`; next poll will find the winner. |
| Backend error | Returns `Pending`. |

## Client reconnect window

After each local poll returns, the broker extends the reconnect deadline by `bits.reconnect_buffer_secs` (default 5 seconds). This gives the client time to reconnect between polls before the dispatch pipeline treats the connection as lost.
