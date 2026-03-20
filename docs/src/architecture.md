# Architecture

![BITS pipeline routing architecture](images/bits_pipeline_tree.svg)

## Jobs

A **job** is the unit of work in BITS. When a client submits a request, BITS creates a job
carrying:

- `original_request` — the payload exactly as submitted. Never mutated. Used as the restart
  point if the job needs to be recovered after a broker failure.
- `request` — a working copy of the payload, mutated by transform actions as the job flows
  through the pipeline.
- `metadata` — mutable annotations added by transforms during routing (for example, a computed
  cost value).
- `user` — identity of the submitting user.

## Pipelines

Every job flows through a **pipeline**: an ordered list of actions. BITS evaluates each action
in sequence. There are three action types:

### Check

A guard condition. It evaluates the job and either **passes** or **rejects**. A rejection stops
the current route and causes the enclosing `switch` to try the next named route. Checks do not
mutate the job.

Examples: `check::match`, `check::has_license`

### Transform

A mutation step. It modifies the job (for example, expanding request parameters or computing a
cost), then continues through the pipeline.

Examples: `transform::metkit_expansion`, `transform::cost`

### Target

The terminal dispatch step. It sends the job to a backend and returns a result. Every route must
end with a target.

Examples: `target::http`, `target::remote`

## Switch

A `switch` contains named routes and tries them **in order**, returning the result of the first
route whose checks all pass. This is the branching primitive — use it to express conditional
routing such as privileged versus public access paths. Switches can be nested inside routes.

When all routes reject, the switch collects rejection reasons from non-silent actions and returns
them to the user. Silent rejections (route-selection checks like `match`) are omitted; non-silent
rejections (validation checks like `schedule_released`) are included so the user understands why
a matched route still failed.

## Dispatcher

Any action step — check, transform, or target — can have an optional **dispatcher** that controls
ordering and concurrency before the action runs.

A dispatcher has two concerns:

- **Queue** — controls which job runs next (`fifo`, `cost_weighted`, or `age_priority`). The queue
  only stores `Job` metadata; it knows nothing about work futures or result types.
- **Executor** — owns the scheduling loop that pulls jobs from the queue and runs the associated
  work (`async_pool`, `thread_pool`, or `remote_pool`).

Each executor implements its own scheduling model:

- **Async pool** — N Tokio tasks (one per concurrency slot) each loop on `queue.dequeue()` and run
  work inline.
- **Thread pool** — dedicated OS threads each call `queue.dequeue()` directly, run work via
  `block_on`, and send results back.
- **Remote pool** — the `/work` HTTP handler calls `queue.dequeue()` directly when a remote worker
  polls, so external workers pull work at their own pace with no intermediate feeder task.

This makes scheduling behavior explicit at the step level rather than a global setting. See
[Configuration](configuration.md) for dispatcher syntax and options.

## Background threads and tasks

BITS starts background threads and async tasks at startup and per-job. Operators should be
aware of these when reasoning about resource usage, process behaviour on shutdown, and
interaction with the host's thread model.

### Tokio async runtime

The `bits-ecmwf` binary uses `#[tokio::main]` with the default multi-thread scheduler. Tokio
spawns one worker thread per logical CPU. These are OS threads managed entirely by Tokio and
are not counted in the items below.

### Started at `Bits::from_config()` — always

**Completed-job sweeper (1 OS thread)**
A single `std::thread::spawn` thread that wakes every 5 seconds (configurable via
`bits.job_cleanup_interval_ms`) and removes finished jobs from the in-memory map once no client
is polling them. A real blocking OS thread is used deliberately so it cannot interfere with the
Tokio scheduler even if it is saturated.

### Started at `Bits::from_config()` — only when persistence is configured

**Broker lease heartbeat (1 Tokio task)**
A long-running async task that periodically upserts this broker's lease record in the persistence
store. It renews at half the configured lease TTL (minimum 100 ms between renewals). This task
is the mechanism by which other brokers determine that this instance is alive. See
[Broker Leases](persistence-broker-leases.md).

### Started per dispatcher — at config parse time

Each action step with a `dispatcher:` block starts background workers when the configuration is
loaded. The executor owns the scheduling loop — the number and kind of workers depend on the
executor type configured.

**Async pool scheduler (N Tokio tasks, if `executor: async_pool`)**
One Tokio task per `concurrency` slot. Each task loops: dequeue a job from the queue, run
the work future inline, send the result back, then dequeue the next. Concurrency is controlled
by the number of tasks.

**Cost-weighted queue worker (1 Tokio task, if `queue: cost_weighted`)**
Maintains the priority heap and services dequeue requests in cost order. Not started when
`queue: fifo` is used — FIFO uses a plain channel with no background task.

**Thread pool workers (N OS threads, if `executor: thread_pool`)**
One OS thread per `concurrency` unit. Each thread calls `queue.dequeue()` directly (via
`block_on`), resolves the pending work, and runs it on that OS thread. Used to isolate
CPU-bound or blocking work from the async scheduler. Not started unless `executor: thread_pool`
is explicitly configured.

**Remote pool HTTP server + heartbeat reaper (2 Tokio tasks, if `executor: remote_pool`)**
One task runs an axum HTTP server on the configured bind address (default `0.0.0.0:9001`).
The `/work` endpoint calls `queue.dequeue()` directly when a remote worker long-polls — there
is no intermediate feeder task. A second task scans in-progress jobs every
`heartbeat_timeout / 2` seconds and evicts any worker that has stopped sending heartbeats.
Only started when `executor: remote_pool` is configured (or when `target::remote` is used, which
implies it).

For the wire protocol and worker implementation contract, see
[External Workers](external-workers.md).

### Started per submitted job

**Job dispatch task (1 Tokio task per job)**
Runs the full pipeline for one job. If `bits.persist_after_ms` is configured, this task also
handles the persistence threshold: it races the pipeline against a timer, and if the timer fires
first it writes the durable job record before continuing to wait for the pipeline to complete.
There is no separate timer task — the threshold logic lives inside this task via `tokio::select!`.

### Summary

| Background worker | Kind | Count | Started when |
|---|---|---|---|
| Completed-job sweeper | OS thread | 1 per `Bits` instance | Always, at startup |
| Broker lease heartbeat | Tokio task | 1 per `Bits` instance | At startup, if persistence configured |
| Async pool scheduler | Tokio tasks | N per `async_pool` executor | At config parse, per dispatcher block |
| Cost-weighted queue worker | Tokio task | 1 per `cost_weighted` queue | At config parse, if `queue: cost_weighted` |
| Thread pool OS workers | OS threads | N per `thread_pool` executor | At config parse, if `executor: thread_pool` |
| Remote pool HTTP server | Tokio task | 1 per `remote_pool` executor | At config parse, if `executor: remote_pool` |
| Remote pool heartbeat reaper | Tokio task | 1 per `remote_pool` executor | At config parse, if `executor: remote_pool` |
| Job dispatch + persist timer | Tokio task | 1 per submitted job | Per `Bits::submit()` |

## Multi-broker cooperation

In production, multiple broker instances run behind a load balancer. Each broker holds an
in-memory shard of the dispatcher state and a broker lease record in durable storage.

When a poll request arrives at a broker that does not own the job:

1. The broker parses the owner from the job ID.
2. It resolves the owner's endpoint from broker lease records.
3. It proxies the poll internally to the owner.
4. If the owner lease is missing or expired, it may claim and recover the job from durable
   storage.

For full details, see the [Persistence](persistence.md) section.

For implementation-level detail and design rationale, see `design.md` in the repository root.
