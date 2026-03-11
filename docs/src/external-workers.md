# External Workers

BITS can hand jobs to custom remote workers with `target::remote` and
`executor: remote_pool`.

This mode is useful when:

- worker code lives outside the broker process,
- workers need their own runtime, dependencies, or hardware,
- you want pull-based execution (workers fetch work when ready).

## 1) Configure a remote target

You can define a named target:

```yaml
targets:
  worker_pool:
    type: remote
    dispatcher:
      queue: cost_weighted
      executor:
        remote_pool:
          bind: "0.0.0.0:9001"
          heartbeat_timeout_secs: 60

routes:
  default:
    - target::worker_pool
```

Or inline:

```yaml
routes:
  default:
    - target::remote: ~
      dispatcher:
        executor:
          remote_pool:
            bind: "0.0.0.0:9001"
            heartbeat_timeout_secs: 60
```

Important rules:

- `target::remote` must use `executor: remote_pool`.
- `executor: remote_pool` is only valid with `target::remote`.
- If you use `target::remote` without an explicit executor, BITS injects a default
  remote pool config (`bind: 0.0.0.0:9001`, `heartbeat_timeout_secs: 60`).

## 2) Worker API contract

When `remote_pool` is configured, BITS starts an HTTP server with dedicated work, heartbeat,
and terminal outcome endpoints.

| Endpoint | Method | Purpose | Success codes |
|---|---|---|---|
| `/work?timeout_ms=N` | `GET` | Long-poll for one available job | `200`, `204` |
| `/heartbeat/{job_id}` | `POST` | Keep an in-progress job alive | `200`, `404` |
| `/complete/data/{job_id}` | `POST` | Stream successful response body | `200`, `404` |
| `/complete/reject/{job_id}` | `POST` | Submit business rejection JSON | `200`, `404` |
| `/complete/error/{job_id}` | `POST` | Submit worker failure JSON | `200`, `404` |
| `/complete/redirect/{job_id}` | `POST` | Submit redirect JSON | `200`, `404` |

### `GET /work?timeout_ms=N`

- Blocks until a job is available or timeout expires.
- Returns `200 OK` with JSON when a job is assigned.
- Returns `204 No Content` on timeout (no work available).

Response body:

```json
{
  "job_id": "broker-a123~9d2f...",
  "request": {"dataset": "era5"},
  "user": {"id": "alice"},
  "metadata": {"cost": 2.0}
}
```

Notes:

- `request` is the transformed request as it reached the remote target step.
- `metadata` includes values produced by earlier transforms.
- One poll returns at most one job.

### `POST /heartbeat/{job_id}`

Send heartbeats while processing a job.

- `200 OK`: heartbeat accepted.
- `404 Not Found`: job is no longer in progress (already completed, evicted, or unknown).

If no heartbeat arrives within `heartbeat_timeout_secs`, BITS evicts the in-progress job and
the broker side returns a failed result.

### `POST /complete/data/{job_id}`

Submit exactly one successful terminal outcome for a claimed job.

- The HTTP request body is the result stream.
- `Content-Type` is forwarded to the final client response.
- `Content-Length` is forwarded when provided, otherwise the client receives a chunked stream.

Example:

```http
POST /complete/data/broker-a123~9d2f... HTTP/1.1
Content-Type: application/x-grib

...streamed bytes...
```

### `POST /complete/reject/{job_id}`

```json
{
  "reason": "unsupported request"
}
```

### `POST /complete/redirect/{job_id}`

```json
{
  "location": "https://my-bucket.s3.amazonaws.com/path/object.grib?X-Amz-Signature=...",
  "message": "Download is ready"
}
```

`message` is optional for redirect.

### `POST /complete/error/{job_id}`

```json
{
  "message": "worker internal failure"
}
```

Response codes:

- `200 OK`: completion accepted.
- `404 Not Found`: job is not in progress (unknown, already completed, or heartbeat timed out).

## 3) End-to-end flow

1. Client submits job to BITS.
2. Route reaches `target::remote`.
3. BITS queues the job in the remote pool.
4. A worker long-polls `/work` and receives job payload.
5. Worker processes job and sends periodic `/heartbeat/{job_id}`.
6. Worker posts final outcome to one of the `/complete/.../{job_id}` endpoints.
7. BITS unblocks the waiting route and returns result to the client.

## 4) Outcome mapping inside BITS

| Worker outcome | Internal action result | Client-visible effect |
|---|---|---|
| `complete/data` | `TargetResult::Complete(JobResult::Success)` | Success response streamed end-to-end with worker `Content-Type` |
| `redirect` | `TargetResult::Complete(JobResult::Redirect)` | Redirect response (for HTTP frontends typically `303 See Other` with `Location`) |
| `reject` | `TargetResult::Reject` | Current route is rejected; switch tries next route. If none match, client gets an error |
| `error` | `ActionError::ResourceError` | Job becomes `Failed` |
| Heartbeat timeout / worker disconnect | `ActionError::ResourceError` | Job becomes `Failed` |

## 5) Worker implementation checklist

- Long-poll `/work` continuously (with jittered retry on transport errors).
- Start heartbeating immediately after receiving a job.
- Stop heartbeating once a `/complete/.../{job_id}` endpoint returns `200` or `404`.
- Treat `404` from heartbeat or complete as terminal for that job.
- Distinguish business rejection (`status: reject`) from execution failure (`status: error`).
- Use `status: redirect` when data should be fetched from object storage rather than streamed
  through BITS.
- Make processing idempotent where possible (network retries and worker restarts happen).

## 6) Minimal worker loop (Python)

```python
import asyncio
import aiohttp

BASE = "http://bits-broker:9001"
WORK_TIMEOUT_MS = 30000
HEARTBEAT_PERIOD_SECS = 10

async def heartbeat_loop(session: aiohttp.ClientSession, job_id: str, stop: asyncio.Event):
    while not stop.is_set():
        await asyncio.sleep(HEARTBEAT_PERIOD_SECS)
        async with session.post(f"{BASE}/heartbeat/{job_id}") as resp:
            if resp.status == 404:
                stop.set()
                return

async def process_job(work: dict):
    req = work["request"]
    if "dataset" not in req:
        return "reject", {"reason": "dataset is required"}
    result = __import__("json").dumps({"ok": True, "dataset": req["dataset"]}).encode()
    return "data", {
        "content_type": "application/json",
        "body": result,
    }

async def worker():
    async with aiohttp.ClientSession() as session:
        while True:
            try:
                async with session.get(
                    f"{BASE}/work", params={"timeout_ms": WORK_TIMEOUT_MS}
                ) as resp:
                    if resp.status == 204:
                        continue
                    resp.raise_for_status()
                    work = await resp.json()
            except Exception:
                await asyncio.sleep(1.0)
                continue

            job_id = work["job_id"]
            stop = asyncio.Event()
            hb_task = asyncio.create_task(heartbeat_loop(session, job_id, stop))
            try:
                status, payload = await process_job(work)
                if status == "data":
                    async with session.post(
                        f"{BASE}/complete/data/{job_id}",
                        data=payload["body"],
                        headers={"Content-Type": payload["content_type"]},
                    ) as done:
                        if done.status not in (200, 404):
                            done.raise_for_status()
                elif status == "reject":
                    async with session.post(
                        f"{BASE}/complete/reject/{job_id}", json=payload
                    ) as done:
                        if done.status not in (200, 404):
                            done.raise_for_status()
            except Exception as exc:
                async with session.post(
                    f"{BASE}/complete/error/{job_id}",
                    json={"message": str(exc)},
                ):
                    pass
            finally:
                stop.set()
                await hb_task

if __name__ == "__main__":
    asyncio.run(worker())
```

## 7) Example: redirect to S3 download

If your worker uploads output to S3 (or another object store), return `redirect` instead of
streaming bytes through BITS:

```python
import aioboto3

async def process_job(work: dict) -> tuple[str, dict]:
    req = work["request"]
    key = f"results/{work['job_id']}.json"

    async with aioboto3.Session().client("s3") as s3:
        await s3.put_object(
            Bucket="my-results-bucket",
            Key=key,
            Body=b'{"status":"ready"}',
            ContentType="application/json",
        )
        url = await s3.generate_presigned_url(
            "get_object",
            Params={"Bucket": "my-results-bucket", "Key": key},
            ExpiresIn=3600,
        )

    return "redirect", {
        "location": url,
        "message": "Result stored in S3",
    }
```

The worker then posts this payload to `/complete/redirect/{job_id}`:

```json
{
  "location": "https://my-results-bucket.s3.amazonaws.com/results/abc.json?...",
  "message": "Result stored in S3"
}
```

## 8) Operational guidance

- Run the worker API on a trusted internal network. The remote pool endpoints do not implement
  authentication themselves.
- Scale throughput by running more workers polling `/work`.
- Use route dispatchers (for example `queue: cost_weighted`) to control which jobs workers
  receive first.
- Choose `heartbeat_timeout_secs` to match expected execution time and heartbeat cadence.
