_This document is a design specification primarily for AI consumption_

# BITS Design

**Broker for Intelligent Task Scheduling** — a policy-aware job broker that routes requests
across distributed infrastructure based on job attributes, user quotas, and resource availability.

---

## Features

- **Fast and slow requests, one broker** — ephemeral jobs flow through the pipeline in-memory with no overhead; long-lived jobs opt into persistence with a single `persist` step.

- **Horizontal scalability with consistency** — multiple broker instances share quota of shared resources safely and efficiently.

- **Queuing and backpressure** — any pipeline action can have bounded queues with configurable capacity and worker count.

- **Forward targets and pull targets** — push jobs to a target directly from the pipeline, or publish to a topic for external workers to pull.

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

- **Transform** — mutation. Mutates the job, perhaps changing the request or adding metadata, and continues.
  Examples: `transform::cost`.

- **Target** — terminal dispatch. Sends the job to a destination and returns a result.
  Examples: `target::mars_destination`, `target::dss_destination`, `target::pull`.

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
    type: size_estimation_cost
    scaling_factor: 0.1

targets:
  mars_retrieval:
    type: mars_destination
    endpoint: "mars.ecmwf.int:8080"
    queue:
      type: fifo
      capacity: 200
      workers: 8

  dss_pull:
    type: pull
    topic: dss-jobs
    queue:
      type: fifo
      capacity: 50

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
          - target::dss_pull
```

**Key decisions:**

- Named entries in `checks:`, `transforms:`, and `targets:` are resolved at parse time. The
  `check::`, `transform::`, and `target::` prefixes in pipelines are meaningful — they identify
  which registry to look up. This makes the type of each pipeline step visible at a glance.
- `type:` is the reserved key within each entry. All other keys at the same level are config
  for that action, keeping definitions flat.
- `queue:` is a sibling property on target entries, not a wrapper. `type:` on a queue selects the
  scheduling discipline (`fifo`, `priority`).
- The reserved string `"persist"` is a built-in pipeline step, not a user-defined entry.
- YAML anchors are deliberately not used — the named registries are an explicit feature of the
  schema, not a YAML trick. Named entries also ensure shared resources (e.g. a queue shared
  across multiple pipelines) are the same instance in memory.
- Inline actions (e.g. `check::match:` directly in a pipeline) bypass the registry entirely and
  are not reusable.

---

## Action Registry

Actions are registered at compile time using the `inventory` crate. This allows external crates
to register their own actions without the core crate knowing about them:

```rust
register_action!(check,     "match",            Match);
register_action!(transform, "metkit_expansion", MetkitExpansion);
register_action!(target,    "mars_destination", MarsDestination);
```

At runtime, `create_action(name, config)` looks up the registered factory and constructs the
action from its JSON config.

---

## Queue

A `Queue` can feature in any action and adds a queue to that particular action, providing:

- **Bounded concurrency** — at most `capacity` jobs waiting, `workers` executing concurrently
- **Backpressure** — jobs are rejected or block when the queue is full
- **Persistence checkpoint** — when a persistent job enters a queue, its state is written to the
  DB (see Persistence)

`Queue` can be implemented with dedicated threads, external worker processes, or just a wait on the async task.

Some actions (e.g. `target::pull`) must have a queue to function, because a shared state is needed to connect the job to a worker that will execute it. For other actions, the queue is optional — if omitted, the action executes immediately in the pipeline.

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
checkpoint_state    — current job state (request + metadata), updated at each queue entry
checkpoint_name     — name of the last queue entered; null means start from original_request
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
| matches a queue in the current pipeline | Re-insert into that queue with `checkpoint_state` |
| does not match any queue | Re-run from `original_request` |

If a pipeline is reconfigured and a checkpoint name no longer exists, the job restarts from the
beginning. This is acceptable for redeployment scenarios — all pre-queue actions are pure and
safe to replay.

**Queue actions must be idempotent** — if a worker dies mid-execution, the job times back to
`queued` state and will be retried.

---

## HTTP Transport

Clients submit jobs and poll for results over HTTP. Because jobs may wait in queues for extended
periods, the connection model matters:

- **Long polling** — the client holds the HTTP connection open while the job is processing.
  When the job is nearly ready, BITS holds the connection and streams the result when available.
- **Retry-After** — for jobs deep in a queue (long estimated wait), BITS returns a `Retry-After`
  header and the client switches to periodic polling. This frees the connection slot on the F5
  load balancer.
- **Reconnect** — clients reconnect with their `job_id` to reattach to a waiting job. The
  async task for the job remains alive across reconnects; only the HTTP connection is reestablished.
- **Jitter** — reconnect timers should include random jitter to avoid thundering herd on restart.

For large data responses, the preferred model is a **signed redirect** — BITS handles auth/routing
and returns a URL the client fetches directly, keeping BITS out of the data path.

---

## Distributed Broker

Multiple broker instances each hold a shard of the queue:

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

- Queue runtime (the `Action::Queue` variant is parsed and stored; execution is `todo!()`)
- Pull target worker API (registration, heartbeat, job handoff endpoints)
- HTTP server (`src/api/` is a stub)
- Persistence / DB integration
- Resource quota tracking and broker negotiation
- User statistics
