_This document is a design specification primarily for AI consumption_

# BITS Design

**Broker for Intelligent Task Scheduling** — a policy-aware job broker that routes requests
across distributed infrastructure based on job attributes, user quotas, and resource availability.

---

## Features

- **Fast and slow requests, one broker** — ephemeral jobs flow through the pipeline in-memory with no overhead; long-lived jobs opt into persistence with a single `persist` step.

- **Horizontal scalability with consistency** — multiple broker instances share quota of shared resources safely and efficiently.

- **Weighted fair scheduling** — requests are costed before dispatch; the scheduler prioritises cheap requests and enforces per-user fairness so heavy users don't starve others.

- **External worker pools** — the `remote` target action hands jobs to external workers via HTTP long-poll. Workers pull jobs from the broker and post results back.

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
  the current route and tries the next route in the switch. Examples: `check::has_role`,
  `check::has_license`.

- **Transform** — mutation. Mutates the job, perhaps changing the request or adding metadata, and
  continues. Examples: `transform::metkit_expansion`, `transform::cost`.

- **Target** — terminal dispatch. Sends the job to a destination and returns a result.
  Examples: `target::http`, `target::remote`.

### Switch

A `Switch` contains named routes and tries them in sequence, returning the result of
the first route that does not reject. This is the branching primitive — use it to express
conditional routing (e.g. privileged vs public access paths).

Switches can be nested inside routes.

---

## Configuration

Config is YAML. The top-level sections are three typed registries (`checks`, `transforms`,
`targets`) for reusable named entries, and a `routes` section defining the pipelines:

```yaml
checks:
  is_privileged:
    type: has_role
    role: privileged

transforms:
  expand:
    type: metkit_expansion
    expand_parameters: true

targets:
  mars_retrieval:
    type: http
    url: "http://mars.ecmwf.int:8080"
    dispatcher:
      queue: cost_weighted   # dispatcher config — see Dispatcher section
      concurrency: 8

  fdb_workers:
    type: remote           # noop; work is done by external workers
    dispatcher:
      queue: cost_weighted
      concurrency: 50

routes:
  ecmwf_data:
    - persist                   # built-in: mark job as persistent and write to DB
    - transform::expand         # evaluate the cost of the request
    - switch:                   # try privileged path first, fall back to public
        privileged:
          - check::is_privileged
          - target::mars_retrieval
        public:
          - check::has_role:    # inline — no registry entry needed
              role: registered
          - target::fdb_workers
```

**Key decisions:**

- Named entries in `checks:`, `transforms:`, and `targets:` are resolved at parse time. The
  `check::`, `transform::`, and `target::` prefixes in routes are meaningful — they identify
  which registry to look up. This makes the type of each route step visible at a glance.
- `type:` is the reserved key within each registry entry. The optional `dispatcher:` key holds
  dispatcher config (`queue`, `executor`, `concurrency`); all other keys are config for the action.
- Dispatcher config (`queue`, `executor`, `concurrency`) is a **route-step concern** — it sits
  under a `dispatcher:` key in a registry entry, or as a sibling `dispatcher:` key for inline
  step definitions. See the Dispatcher section below.
- The reserved string `"persist"` is a built-in pipeline step, not a user-defined entry.
- YAML anchors are deliberately not used — the named registries are an explicit feature of the
  schema, not a YAML trick. Named entries also ensure shared resources are the same instance in memory.
- Inline actions (e.g. `check::has_role:` directly in a pipeline) bypass the registry entirely and
  are not reusable.

---

## Action Registry

Actions are registered at compile time using the `inventory` crate. This allows external crates
to register their own actions without the core crate knowing about them:

```rust
register_action!(check,     "has_role",          HasRole);
register_action!(transform, "metkit_expansion",  MetkitExpansion);
register_action!(target,    "http",              HttpTarget);
register_action!(target,    "remote",            RemoteTarget);
```

At runtime, `create_action(name, config)` looks up the registered factory and constructs the
action from its JSON config.

---

## Dispatcher

Any pipeline step — Check, Transform, or Target — can have an optional **dispatcher** that
controls ordering and concurrency before the action runs. A dispatcher composes two orthogonal
concerns: a **queue** that controls *which* job runs next, and an **executor** that controls
*how* the work runs.

### Queue — ordering

| Kind | Behaviour |
|------|-----------|
| `fifo` | First-in, first-out. Default when `queue` is set without an explicit kind. |
| `cost_weighted` | Cheaper jobs (lower `metadata["cost"]`) run before expensive ones. |

### Executor — execution

| Kind | Behaviour |
|------|-----------|
| `semaphore` | Runs work inline on the async scheduler, bounded by `concurrency`. Default. |
| `thread_pool` | Offloads work to a pool of `concurrency` dedicated OS threads. Use for CPU-bound or blocking work that would otherwise starve the async runtime. |
| `remote_pool` | Hands the job to an external worker via HTTP long-poll. Used exclusively with `target::remote`. |

### Attaching a dispatcher to a step

For **named registry entries**, dispatcher fields are nested under a `dispatcher:` key:

```yaml
targets:
  mars_retrieval:
    type: http
    url: "http://mars.ecmwf.int:8080"
    dispatcher:
      queue: cost_weighted
      concurrency: 8
```

For **inline steps**, `dispatcher:` is a sibling key in the action mapping:

```yaml
routes:
  default:
    - target::http:
        url: "http://mars.ecmwf.int:8080"
      dispatcher:
        queue: cost_weighted
        concurrency: 8
```

Dispatcher config is valid on any step type — Check, Transform, or Target:

```yaml
transforms:
  expand:
    type: metkit_expansion
    expand_parameters: true
    dispatcher:
      concurrency: 4            # limit concurrent expansion calls
```

`queue` accepts `fifo` or `cost_weighted`. `executor` accepts `semaphore`, `thread_pool`, or
`remote_pool`. `concurrency` is a positive integer; omitting it with a queue defaults to unlimited.

### Remote pool

`target::remote` is a no-op action paired exclusively with `executor: remote_pool`. When a job
reaches this step, the dispatcher holds the caller suspended and hands the job to an external
worker via HTTP long-poll. The worker posts the result back; the caller is woken and the result
is returned to the client.

```yaml
targets:
  fdb_workers:
    type: remote
    dispatcher:
      queue: cost_weighted
      concurrency: 50
```

`remote` always implies `executor: remote_pool` — it is auto-inserted if not specified. Any
other executor paired with `remote`, or `remote_pool` paired with a non-`remote` action, is
rejected at config parse time.

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
checkpoint_state    — current job state (request + metadata), updated at each dispatcher entry
checkpoint_name     — name of the last dispatcher entered; null means start from original_request
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
| matches a dispatcher in the current pipeline | Re-insert into that dispatcher with `checkpoint_state` |
| does not match any dispatcher | Re-run from `original_request` |

If a pipeline is reconfigured and a checkpoint name no longer exists, the job restarts from the
beginning. This is acceptable for redeployment scenarios — all pre-dispatcher actions are pure and
safe to replay.

**Dispatcher actions must be idempotent** — if a worker dies mid-execution, the job times back to
`queued` state and will be retried.

---

## HTTP Transport

Clients submit jobs and poll for results over HTTP. Because jobs may wait in dispatchers for
extended periods, the connection model matters:

- **Long polling** — the client holds the HTTP connection open while the job is processing.
  When the job is nearly ready, BITS holds the connection and streams the result when available.
- **Retry-After** — for jobs deep in a dispatcher (long estimated wait), BITS returns a `Retry-After`
  header and the client switches to periodic polling. This frees the connection slot on the load
  balancer.
- **Reconnect** — clients reconnect with their `job_id` to reattach to a waiting job. The
  async task for the job remains alive across reconnects; only the HTTP connection is reestablished.
- **Jitter** — reconnect timers should include random jitter to avoid thundering herd on restart.

For large data responses, the preferred model is a **signed redirect** — BITS handles auth/routing
and returns a URL the client fetches directly, keeping BITS out of the data path.

---

## Distributed Broker

Multiple broker instances each hold a shard of the dispatcher:

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

- Remote pool runtime — full HTTP long-poll worker API, job handoff, and result callback
  (`executor: remote_pool` is stubbed and returns an error)
- HTTP server (`src/api/` is a stub)
- Persistence / DB integration
- Resource quota tracking and broker negotiation
- User statistics
