_This document is a design specification primarily for AI consumption_

# BITS Design

**Broker for Intelligent Task Scheduling** — a policy-aware job broker that routes requests
across distributed infrastructure based on job attributes, user quotas, and resource availability.

---

## Features

- **Fast and slow requests, one broker** — ephemeral jobs flow through the pipeline in-memory with no overhead; long-lived jobs opt into persistence with a single `persist` step.

- **Horizontal scalability with consistency** — multiple broker instances share quota of shared resources safely and efficiently.

- **Weighted fair scheduling** — requests are costed before dispatch; the scheduler prioritises cheap requests and enforces per-user fairness so heavy users don't starve others.

- **External worker pools** — targets can dispatch to external workers via HTTP long-poll. Workers pull jobs from the broker and post results back. The submitting connection is held open and receives the result when the worker completes.

- **Persistence and recovery** — persistent jobs survive broker termination. Jobs are rebalanced to live instances.

- **Pluggable actions** — additional actions can be created in Rust or Python.

---

## Core Concepts

### Job

A `Job` is the unit of work. It carries:

- `original_request` — the payload as submitted by the client. Never modified. Used as the
  restart point on broker recovery.
- `request` — the working copy of the request, mutated by `transform` actions as the job flows
  through the pipeline.
- `metadata` — mutable annotations added by `transform` actions during routing
- `user` — identity of the submitting user
- `persistent` — flag set by the `persist` action; when true, the job is synced to the DB

### Pipeline

A job flows through a **pipeline** — an ordered list of actions. Three action types:

- **Check** — guard condition. Evaluates the job and either passes or rejects. A rejection stops
  the current route and tries the next route in the switch. Examples: `check::match`,
  `check::has_role`, `check::has_license`.

- **Transform** — mutation. Mutates the job, perhaps changing the request or adding metadata, and
  continues. Examples: `transform::cost`, `transform::metkit_expansion`.

- **Target** — terminal dispatch. Sends the job to a destination and returns a result.
  Examples: `target::mars_retrieval`, `target::external_pool`.

### Switch

A `Switch` contains named routes and tries them in sequence, returning the result of
the first route that does not reject. This is the branching primitive — use it to express
conditional routing (e.g. privileged vs public access paths).

Switches can be nested inside routes.

---

## Configuration

Config is YAML with four top-level sections — three typed registries and a pipelines section:

```yaml
checks:
  is_privileged:
    type: has_role
    role: privileged

transforms:
  cost:
    type: metkit_cost
    endpoint: "cost-service.ecmwf.int:9000"
    max_concurrent: 16      # concurrency limit for calls to the cost service

targets:
  mars_retrieval:
    type: mars_destination
    endpoint: "mars.ecmwf.int:8080"
    capacity: 200           # max jobs waiting in the scheduler

  dss_workers:
    type: external_pool
    capacity: 50            # max jobs waiting for a worker to claim them

pipelines:
  ecmwf_data:
    - persist                   # built-in: mark job as persistent and write to DB
    - transform::cost           # evaluate the cost of the request
    - switch:                   # try privileged path first, fall back to public
        privileged:
          - check::is_privileged
          - target::mars_retrieval
        public:
          - check::match:       # inline — no name needed
              class: od
          - target::dss_workers
```

**Key decisions:**

- Named entries in `checks:`, `transforms:`, and `targets:` are resolved at parse time. The
  `check::`, `transform::`, and `target::` prefixes in pipelines are meaningful — they identify
  which registry to look up. This makes the type of each pipeline step visible at a glance.
- `type:` is the reserved key within each entry. All other keys at the same level are config
  for that action, keeping definitions flat.
- Scheduling and concurrency config (e.g. `capacity`, `max_concurrent`) are properties of the
  action that needs them, not a generic `queue:` wrapper. Each action manages its own internal
  scheduling primitives.
- The reserved string `"persist"` is a built-in pipeline step, not a user-defined entry.
- YAML anchors are deliberately not used — the named registries are an explicit feature of the
  schema, not a YAML trick. Named entries also ensure shared resources are the same instance in memory.
- Inline actions (e.g. `check::match:` directly in a pipeline) bypass the registry entirely and
  are not reusable.

---

## Action Registry

Actions are registered at compile time using the `inventory` crate. This allows external crates
to register their own actions without the core crate knowing about them:

```rust
register_action!(check,     "match",            Match);
register_action!(transform, "metkit_cost",      MetkitCost);
register_action!(target,    "mars_destination", MarsDestination);
```

At runtime, `create_action(name, config)` looks up the registered factory and constructs the
action from its JSON config.

---

## Scheduling Primitives

Scheduling is not a pipeline concept — it is infrastructure that action implementations use
internally. The pipeline only sees `CheckAction`, `TransformAction`, `TargetAction`. How an
action manages concurrency or ordering is entirely its own concern.

Two primitives cover all cases:

### Semaphore

A standard concurrency limit. Any action that calls an external service with finite capacity
holds a `Semaphore` and acquires a permit before each call. The permit is dropped when the call
returns, freeing the slot for the next waiter. Waiters queue in FIFO order internally.

Used by: costing transforms, per-user throttle checks, any action with a `max_concurrent` config.

### Scheduler

A weighted fair queue. Accepts jobs with a cost, prioritises cheap jobs over expensive ones,
and enforces fairness across users. Used when the ordering of execution matters — specifically
for dispatch to backends with finite capacity where large requests should not starve small ones.

`acquire(job, cost)` suspends the calling task until the scheduler assigns execution. It returns
a handle the caller uses to complete the work and receive the result.

Used by: target actions dispatching to finite-capacity backends.

### External Worker Pool

A target action that dispatches to external workers via HTTP long-poll. The job thread calls
`scheduler.acquire(job, cost)`, which suspends it. An external worker connects and calls a
long-poll endpoint; the scheduler assigns the next job to that worker and wakes the job thread
with a handle to the worker connection. The job thread holds the original client connection open
and streams the result back when the worker completes.

The scheduler inside an external pool target is the same `Scheduler` primitive — the difference
is only that workers are remote processes rather than internal async tasks.

---

## Persistence

Jobs are either **ephemeral** (in-memory only, lost on broker crash) or **persistent** (synced
to a database). The distinction is made in the pipeline, not at submission time.

### The `persist` action

`persist` is a built-in pipeline step that sets `job.persistent = true` and writes the job to
the DB. It can appear at any point in the pipeline, making persistence conditional on what came
before it. A job that is rejected before reaching `persist` is never written to the DB.

The DB record stores:

```
job_id
original_request    — written once at persist; mirrors job.original_request, never updated
checkpoint_state    — current job state (request + metadata), updated at each scheduler entry
checkpoint_name     — name of the last scheduler entered; null means start from original_request
status              — registered | queued | in_flight | done | failed
```

### `Job::sync()`

`sync()` is a no-op for ephemeral jobs and a DB upsert for persistent ones. It is called
automatically by the pipeline executor at structural checkpoints — not by action authors.

### Recovery

When a broker restarts, it loads persistent jobs from the DB and recovers based on status:

| checkpoint_name | action |
|---|---|
| null | Re-run from `original_request` (persist action and all transforms will re-execute) |
| matches a scheduler in the current pipeline | Re-insert into that scheduler with `checkpoint_state` |
| does not match any scheduler | Re-run from `original_request` |

If a pipeline is reconfigured and a checkpoint name no longer exists, the job restarts from the
beginning. This is acceptable for redeployment scenarios — all pre-scheduler actions are pure and
safe to replay.

**Scheduler actions must be idempotent** — if a worker dies mid-execution, the job times back to
`queued` state and will be retried.

---

## HTTP Transport

Clients submit jobs and poll for results over HTTP. Because jobs may wait in schedulers for
extended periods, the connection model matters:

- **Long polling** — the client holds the HTTP connection open while the job is processing.
  When the job is nearly ready, BITS holds the connection and streams the result when available.
- **Retry-After** — for jobs deep in a scheduler (long estimated wait), BITS returns a `Retry-After`
  header and the client switches to periodic polling. This frees the connection slot on the F5
  load balancer.
- **Reconnect** — clients reconnect with their `job_id` to reattach to a waiting job. The
  async task for the job remains alive across reconnects; only the HTTP connection is reestablished.
- **Jitter** — reconnect timers should include random jitter to avoid thundering herd on restart.

For large data responses, the preferred model is a **signed redirect** — BITS handles auth/routing
and returns a URL the client fetches directly, keeping BITS out of the data path.

---

## Distributed Broker

Multiple broker instances each hold a shard of the scheduler:

- Brokers **lease resource quotas** from a central Postgres DB atomically. Local quota
  reservations avoid per-job DB queries. On broker crash, unreleased reservations expire by TTL.
- **Persistent job ownership** uses a `claimed_by` + heartbeat column. Orphaned jobs (missed
  heartbeat) are reclaimed by other brokers via a simple `UPDATE ... WHERE claimed_by = $dead`.
- **Ephemeral jobs** are lost on broker crash — this is acceptable by design.
- **Database loss** causes brokers to stop accepting new jobs. In-flight ephemeral jobs may
  continue; persistent jobs cannot be safely committed.
- **Network partition** — any broker unable to reach the DB is treated as if it has crashed.

---

## What is not yet implemented

- Scheduler runtime (weighted fair queue, semaphore primitives in `src/scheduler/`)
- External worker pool target (HTTP long-poll worker API, job handoff, result callback)
- HTTP server (`src/api/` is a stub)
- Persistence / DB integration
- Resource quota tracking and broker negotiation
- User statistics
