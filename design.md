# BITS Design

**Broker for Intelligent Task Scheduling** — a policy-aware job broker that routes requests
across distributed infrastructure based on job attributes, user quotas, and resource availability.

The primary use case is routing meteorological data requests (ECMWF) to appropriate HPC backends
(MARS, DSS, EuroHPC clusters), but the design is general.

---

## Core Concepts

### Job

A `Job` is the unit of work. It carries:

- `request` — the original payload from the client (immutable reference point)
- `metadata` — mutable annotations added by `via` actions during routing
- `user` — identity of the submitting user
- `persistent` — flag set by the `persist` action; when true, the job is synced to the DB

### Pipeline

A job flows through a **pipeline** — an ordered list of actions. Three action types:

- **Check** — guard condition. Evaluates the job and either passes or rejects. A rejection stops
  the current branch and tries the next route in the switch. Examples: `check::match`,
  `check::has_role`, `check::has_license`.

- **Via** — transformation. Mutates the job (typically adding metadata) and continues.
  Examples: `via::metkit_expansion`.

- **Route** — terminal dispatch. Sends the job to a destination and returns a result.
  Examples: `route::mars_destination`, `route::dss_destination`, `route::pull`.

### Switch

A `Switch` contains named route branches and tries them in sequence, returning the result of
the first branch that does not reject. This is the branching primitive — use it to express
conditional routing (e.g. privileged vs public access paths).

Switches can be nested inside route branches.

---

## Configuration

Config is YAML with four top-level sections — three typed registries and a pipelines section:

```yaml
checks:
  is_privileged:
    type: has_role
    role: privileged

vias:
  expand:
    type: metkit_expansion
    expand_parameters: true

routes:
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
      capacity: 50            # omit workers: for pull routes (drained by external workers)

pipelines:
  ecmwf_data:
    - persist                 # built-in: mark job as persistent and write to DB
    - via::expand             # resolve metkit request parameters
    - switch:                 # try privileged path first, fall back to public
        privileged:
          - check::is_privileged
          - route::mars_retrieval
        public:
          - check::match:     # inline — no name needed
              class: od
          - route::dss_pull
```

**Key decisions:**

- Named entries in `checks:`, `vias:`, and `routes:` are resolved at parse time. The `check::`,
  `via::`, and `route::` prefixes in pipelines are meaningful — they identify which registry to
  look up. This makes the type of each pipeline step visible at a glance.
- `type:` is the reserved key within each entry. All other keys at the same level are config
  for that action, keeping definitions flat.
- `queue:` is a sibling property on route entries, not a wrapper. `type:` on a queue selects the
  scheduling discipline (`fifo`, `priority`). Omitting `workers:` signals a pull route.
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
register_action!(check, "match", Match);
register_action!(via,   "metkit_expansion", MetkitExpansion);
register_action!(route, "mars_destination", MarsDestination);
```

At runtime, `create_action(name, config)` looks up the registered factory and constructs the
action from its JSON config.

---

## Queue

A `Queue` wraps any action and provides:

- **Bounded concurrency** — at most `capacity` jobs waiting, `workers` executing concurrently
- **Backpressure** — jobs are rejected or block when the queue is full
- **Persistence checkpoint** — when a persistent job enters a queue, its state is written to the
  DB (see Persistence)

`Queue` is not an async mechanism — jobs are already async tasks. It is purely a **concurrency
bound**. Without a queue, an action executes inline in the job's async task with no limit on
concurrency.

**Pull routes** (`route::pull`) always have an implicit buffer — the shared channel that job
tasks and worker connections rendezvous on. Wrapping a pull route in a `queue:` adds a capacity
bound to that buffer. Omitting `workers:` signals that the queue is drained by external workers,
not internal ones.

---

## Persistence

Jobs are either **ephemeral** (in-memory only, lost on broker crash) or **persistent** (synced
to PostgreSQL). The distinction is made in the pipeline, not at submission time.

### The `persist` action

`persist` is a built-in pipeline step that sets `job.persistent = true` and writes the job to
the DB. It can appear at any point in the pipeline, making persistence conditional on what came
before it. A job that is rejected before reaching `persist` is never written to the DB.

The DB record stores:

```
job_id
original_request    — written once at persist, never updated
checkpoint_state    — current job state (including via mutations), updated at each queue entry
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
| null | Re-run from `original_request` (persist action and all via transforms will re-execute) |
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

For large data responses (GRIB fields can be gigabytes), the preferred model is a **signed
redirect** — BITS handles auth/routing and returns a URL the client fetches directly, keeping
BITS out of the data path.

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
- Pull route worker API (registration, heartbeat, job handoff endpoints)
- HTTP server (`src/api/` is a stub)
- Persistence / DB integration
- Resource quota tracking and broker negotiation
- User statistics
