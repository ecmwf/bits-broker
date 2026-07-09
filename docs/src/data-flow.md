<!--
SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)

SPDX-License-Identifier: Apache-2.0
-->

# Data Flow

This page explains how data moves through BITS, from client submission to
backend dispatch and back. Understanding the data flow helps you debug routing
issues, write custom actions, and reason about what each component sees.

## The life of a job

When a client submits a request, BITS creates a Job and routes it through a
pipeline of checks, transforms, and a terminal target:

```mermaid
sequenceDiagram
    participant C as Client
    participant B as BITS Broker
    participant T as Backend

    C->>B: POST /job {"class":"od","date":"2024-01-15"}
    Note over B: Create Job<br/>id = opaque request ID<br/>original_request = frozen copy<br/>request = working copy
    Note over B: Spawn pipeline task
    Note over B: Pipeline:<br/>check → pass<br/>transform → mutate request<br/>target → dispatch
    B->>T: POST http://backend/api<br/>Body: job.request (after transforms)
    T-->>B: 200 OK + result bytes
    Note over B: Write result to Job<br/>Notify waiting pollers
    B-->>C: 200 OK (stream body)
```

Key points:

- `submit()` is non-blocking. It creates the Job and spawns a background
  task, returning immediately with a `JobHandle`.
- `poll()` is a long-poll. It waits for the result or times out.
- The pipeline runs in a separate async task from the poll.
- Results are delivered via a shared `Notify`. The poll task sleeps until
  the pipeline writes the result and wakes it.

## What the backend receives

When `target::http` dispatches a job, it takes the job's `request` field and
POSTs it as the JSON body:

```http
POST /api HTTP/1.1
Host: my-backend
Content-Type: application/json

{"class": "od", "date": "2024-01-15"}
```

The request body is exactly `job.request`, the working copy after any
transforms. Nothing is added or wrapped. HTTP headers like `Content-Type`
are set automatically by the HTTP client.

## What transforms do to the data

Transforms can mutate `request`, `metadata`, or both. Each transform in the
pipeline sees the changes from the previous one:

```mermaid
flowchart TD
    A["Client submits<br/>{class: od, param: 2t/msl}"]
    A --> B["Transform: coercion"]
    B --> C["request becomes<br/>{class: od, param: [2t, msl]}"]
    C --> D["Transform: metkit_expansion"]
    D --> E["metadata becomes<br/>{metkit_expanded: true}"]
    E --> F["Target dispatches request to backend"]
```

The `original_request` is never touched. It stays as the client submitted
it. This frozen copy is used to restart the job from scratch if the broker
crashes and another broker recovers it.

## Two HTTP servers, two data paths

BITS runs two independent HTTP servers for different audiences:

```mermaid
graph LR
    subgraph "BITS Broker"
        subgraph "Client Server :8080"
            A1["POST /job"]
            A2["GET /job/{id}"]
        end
        subgraph "Worker Server :9001"
            B1["GET /mars/work"]
            B2["POST /mars/complete/..."]
        end
    end
    Users["API Clients"] --> A1
    Users --> A2
    Workers["External Workers"] --> B1
    Workers --> B2
```

**Client server** (port 8080): Receives job submissions from end users and
serves poll results. Configured under `server:` in the YAML.

**Worker server** (port 9001): Serves work to external pull-based workers.
Configured under `bits.worker_server:` in the YAML. Only started when
`target::remote` is used.

## Push model: target::http

BITS calls the backend:

```mermaid
sequenceDiagram
    participant B as BITS Broker
    participant S as Backend Service

    B->>S: POST http://backend/api<br/>Body: job.request (JSON)
    alt 2xx
        S-->>B: Body: result data
        Note over B: JobResult::Success { stream }
    else 4xx (route rejection)
        S-->>B: Body: reason
        Note over B: TargetResult::Reject<br/>Switch tries next route
    else 5xx / transport error
        S-->>B: failure
        Note over B: JobResult::Failed { reason }
    end
```

A `4xx` from the backend does not immediately fail the job. It becomes a
route rejection. If there are more routes in the switch, the next route is
tried. Only if all routes reject does the client see the rejection.

## Pull model: target::remote

Workers call BITS:

```mermaid
sequenceDiagram
    participant W as Worker
    participant B as BITS Broker :9001

    W->>B: GET /mars/work?timeout_ms=30000
    Note over B: Dequeue job from queue
    B-->>W: 200 {job_id, request, user, metadata}

    loop Every N seconds
        W->>B: POST /mars/heartbeat/{job_id}
        B-->>W: 200 OK
    end

    W->>B: POST /mars/complete/data/{job_id}<br/>Body: result bytes
    B-->>W: 200 OK
    Note over B: Result delivered to waiting poller
```

When a worker receives a job, the response body contains:

```json
{
  "job_id": "0217scypcc00000000000000",
  "request": {"class": "od", "param": ["2t", "msl"]},
  "user": {"name": "alice", "group": "research"},
  "metadata": {"cost": 42, "metkit_expanded": true}
}
```

- `request` is the working copy after transforms.
- `user` and `metadata` are passed through as-is.
- `job_id` is used for heartbeats and completion calls.

If the worker stops sending heartbeats, the broker times out the entry and
the job fails. It is not automatically re-queued.

## Multi-broker data flow

When multiple brokers run behind a load balancer, a poll might land on a
different broker than the one processing the job:

```mermaid
sequenceDiagram
    participant C as Client
    participant LB as Load Balancer
    participant A as Broker A
    participant B as Broker B
    participant S as NATS/TiKV

    C->>LB: POST /job
    LB->>A: forward
    A->>A: Submit + dispatch job
    A->>S: Persist (after threshold)

    Note over A: Broker A crashes

    C->>LB: GET /job/{id}
    LB->>B: forward
    B->>S: Decode owner hint & check lease → expired
    B->>S: Claim authoritative job record (atomic CAS)
    B->>B: Restore from original_request
    B->>B: Re-dispatch through pipeline
    B-->>C: Result
```

If Broker A is still alive, Broker B proxies the poll instead of claiming:

```mermaid
sequenceDiagram
    participant C as Client
    participant B as Broker B
    participant A as Broker A

    C->>B: GET /job/{id}
    B->>B: Owner hint names A & lease active
    B->>A: Proxy poll to A's internal URL
    A-->>B: Result
    B-->>C: Result
```

If the proxy fails (A is unreachable but lease hasn't expired), B returns
`Pending` to the client. It does not trigger a reclaim while the lease is
active.

## The persistence threshold

Not all jobs are persisted. Fast jobs stay entirely in memory:

```mermaid
flowchart TD
    A[Job submitted] --> B{Timer: persist_after_secs}
    B -->|Job finishes first| C[No persistence - fast path]
    B -->|Timer fires first| D[Write record to store]
    D --> E[Job finishes]
    E --> F[Delete record from store]
```

Only `original_request`, `user`, `metadata`, and `created_at` are persisted.
The working `request` (after transforms) is not stored. On recovery, the
job is rebuilt from `original_request` and transforms run again.

## The reconnect window

Between polls, the reconnect window (`bits.reconnect_buffer_secs`, default 5 s) keeps the job alive:

```mermaid
sequenceDiagram
    participant C as Client
    participant B as BITS Broker

    C->>B: POST /job
    Note over B: Job running...
    B-->>C: 303 /job/{id}, Retry-After: 0

    Note over C: Reconnect window: 5s

    C->>B: GET /job/{id}
    Note over B: Still running...
    B-->>C: 303 /job/{id}

    C->>B: GET /job/{id}
    Note over B: Job finished!
    B-->>C: 200 OK (result body)
```

If the client disconnects mid-poll, the `ConnectedGuard` extends the
reconnect deadline automatically. If the client never comes back, the
sweeper eventually removes the completed job from memory.
