_This document is a design specification primarily for AI consumption_

# BITS Design

**Broker for Intelligent Task Scheduling** — a policy-aware job broker that routes requests
across distributed infrastructure based on job attributes, user quotas, and resource availability.

---

## Features

- **Fast and slow requests, one broker** — ephemeral jobs flow through the pipeline in-memory with no overhead; long-lived jobs persist after a configurable in-flight threshold.

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
- runtime lifecycle state for cancellation, polling, and result delivery

### Pipeline

A job flows through a **pipeline** — an ordered list of actions. Three action types:

- **Check** — guard condition. Evaluates the job and either passes or rejects. A rejection stops
  the current route and tries the next route in the switch. Examples: `check::match`,
  `check::has_license`.

- **Transform** — mutation. Mutates the job, perhaps changing the request or adding metadata, and
  continues. Examples: `transform::metkit_expansion`, `transform::cost`.

- **Target** — terminal dispatch. Sends the job to a destination and returns a result.
  Examples: `target::http`, `target::remote`.

### Switch

A `Switch` contains named routes and tries them in sequence, returning the result of
the first route that does not reject. This is the branching primitive — use it to express
conditional routing (e.g. privileged vs public access paths).

When all routes reject, the switch returns non-silent rejection reasons to the user. Each action
controls whether its rejections are silent (route-selection, not shown) or non-silent (validation
failures the user should see). The `silent` config key on any step overrides the action's default.

Switches can be nested inside routes.

---

## Configuration

Config is YAML. The top-level sections are three typed registries (`checks`, `transforms`,
`targets`) for reusable named entries, and a `routes` section defining the pipelines:

```yaml
bits:
  site: bol
  env: dev

checks:
  is_privileged:
    type: match
    class: od

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
      executor:
        type: async_pool
        concurrency: 8

  fdb_workers:
    type: remote           # noop; work is done by external workers
    dispatcher:
      queue: cost_weighted
      executor:
        type: remote_pool

routes:
  - ecmwf_data:
      - transform::expand         # evaluate the cost of the request
      - switch:                   # try privileged path first, fall back to public
          privileged:
            - check::is_privileged
            - target::mars_retrieval
          public:
            - check::match:       # inline — no registry entry needed
                class: ea
            - target::fdb_workers
```

**Key decisions:**

- Named entries in `checks:`, `transforms:`, and `targets:` are resolved at parse time. The
  `check::`, `transform::`, and `target::` prefixes in routes are meaningful — they identify
  which registry to look up. This makes the type of each route step visible at a glance.
- `type:` is the reserved key within each registry entry. The optional `dispatcher:` key holds
  dispatcher config (`queue`, `executor`); the optional `silent:` key overrides
  rejection visibility; all other keys are config for the action.
- Dispatcher config (`queue`, `executor`) is a **route-step concern** — it sits
  under a `dispatcher:` key in a registry entry, or as a sibling `dispatcher:` key for inline
  step definitions. See the Dispatcher section below.
- Broker identity is configured with compact `bits.site` and `bits.env` tags. Persistence is configured at `bits` top level (`persist_after_secs`) and validated against `server.poll_timeout_secs`.
- YAML anchors are deliberately not used — the named registries are an explicit feature of the
  schema, not a YAML trick. Named entries also ensure shared resources are the same instance in memory.
- Inline actions (e.g. `check::match:` directly in a pipeline) bypass the registry entirely and
  are not reusable.

---

## Action Registry

Actions are registered at compile time using the `inventory` crate. This allows external crates
to register their own actions without the core crate knowing about them:

```rust
register_action!(check,     "match",             Match);
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
| `async_pool` | Runs work on the async scheduler, bounded by `concurrency`. Default. |
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
      executor:
        type: async_pool
        concurrency: 8
```

For **inline steps**, `dispatcher:` is a sibling key in the action mapping:

```yaml
routes:
  - default:
      - target::http:
            url: "http://mars.ecmwf.int:8080"
        dispatcher:
          queue: cost_weighted
          executor:
            type: async_pool
            concurrency: 8
```

Dispatcher config is valid on any step type:

```yaml
transforms:
  expand:
    type: metkit_expansion
    expand_parameters: true
    dispatcher:
      executor:
        type: async_pool
        concurrency: 4
```

`queue` accepts `fifo`, `cost_weighted`, or `age_priority`. `executor` accepts `async_pool`,
`thread_pool`, or `remote_pool`. Concurrency is set inside the executor block; default is 256.

### Remote pool

`target::remote` is a no-op action paired exclusively with `executor.type: remote_pool`. When a job
reaches this step, the dispatcher holds the caller suspended and hands the job to an external
worker via HTTP long-poll. The worker posts the result back; the caller is woken and the result
is returned to the client.

**Worker server**: All remote pools share a single HTTP server configured at
`bits.worker_server.host` / `bits.worker_server.port`. Each pool's endpoints are namespaced under `/{pool_name}/`:

- `GET  /{pool_name}/work?timeout_ms=N`           — long-poll; returns job JSON or 204
- `POST /{pool_name}/heartbeat/{job_id}`           — worker keepalive
- `POST /{pool_name}/complete/data/{job_id}`       — stream result body
- `POST /{pool_name}/complete/redirect/{job_id}`   — redirect outcome
- `POST /{pool_name}/complete/reject/{job_id}`     — rejection outcome
- `POST /{pool_name}/complete/error/{job_id}`      — error outcome

The pool name is derived from the target's registry entry name. `target::remote` must be
defined as a **named registry entry** (not inline) so the pool name can be derived.

```yaml
bits:
  site: bol
  env: dev
  worker_server:
    bind: "0.0.0.0:9001"   # single shared server for all remote pools

targets:
  mars:
    type: remote            # pool name = "mars" → endpoints at /mars/work etc.
    dispatcher:
      queue: cost_weighted
      executor:
        type: remote_pool
        heartbeat_timeout_secs: 60

  fdb_workers:
    type: remote            # pool name = "fdb_workers" → /fdb_workers/work etc.
    dispatcher:
      queue: cost_weighted
      executor:
        type: remote_pool
        heartbeat_timeout_secs: 60
```

`remote` always implies `executor: remote_pool` — it is auto-inserted if not specified. Any
other executor paired with `remote`, or `remote_pool` paired with a non-`remote` action, is
rejected at config parse time. `bits.worker_server` must be configured whenever any `remote`
target is present.

---

## Persistence

BITS supports threshold persistence for long-running work.

- Jobs start in memory immediately.
- Every job receives an opaque 26-character public request ID. Internally, the ID encodes version, site tag, environment tag, seconds since `2025-01-01T00:00:00Z`, broker slot, and 5 bytes from `OsRng`.
- With persistence configured, each broker allocates a durable `u16` slot for its `(site, env)` pair and forms an internal broker ID `{site}-{env}-{slot}`.
- If `bits.persist_after_secs` is configured and a job remains in-flight past that threshold, BITS writes a durable record (`job_id`, authoritative owner `broker_id`, `original_request`, `user`, `metadata`, `created_at`).
- Job records are keyed by decoded `(site, env, slot, job_id)`; broker leases are keyed separately by internal broker ID.
- On terminal completion, the durable record is deleted.

Recovery flow:

1. Poll lands on any broker.
2. Broker checks local state first.
3. On local miss, broker decodes the request ID to derive an owner hint and resolves that broker's lease.
4. If the hinted owner's lease is active, broker proxies poll to that owner.
5. If the lease is missing/expired, broker reads and claims the durable record using its authoritative owner field; on success it restores from `original_request` and resubmits.

Reclaim is strictly lease-gated: proxy failure with an active owner lease does not trigger claim.

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

## Observability

BITS records job-lifecycle and dispatcher metrics through the OpenTelemetry **global** meter.
The instrumentation lives in the core library and is always compiled; with no meter provider
installed the instruments are no-ops (tests, or embedding hosts that bring their own provider).

- **Instruments** — job counters (accepted / finished by `outcome`), job-duration histograms,
  and dispatcher queue depth / wait time, each also emitted per route handle.
- **Opt-in Prometheus exporter** — the `metrics-prometheus` cargo feature installs an
  `SdkMeterProvider` backed by a Prometheus reader and exposes a scrapeable `GET /metrics` on
  the same HTTP server as the job API. The feature is **additive**: the server picks up the
  handle installed by `metrics::init_prometheus` from a process-global `OnceLock`, so enabling
  the feature never changes the public `serve` signatures.
- **Config-driven histogram buckets** — a top-level `metrics:` section sets `duration_buckets`
  and `queue_wait_buckets` (seconds), with sensible defaults. Boundaries are applied at meter
  provider construction via SDK **Views** keyed by instrument name, so the core instrument
  definitions stay free of exporter concerns. Bucket lists are validated at parse time
  (non-empty, finite, non-negative, strictly increasing).
- **Naming** — counter instruments are named without a `.total` suffix; the Prometheus exporter
  appends `_total`. Every series also carries an `otel_scope_name="bits"` label.

Metrics are process-global by design, consistent with the OpenTelemetry global meter provider.
An embedding host may install its own provider instead of the built-in exporter.

---

## Distributed Broker

Multiple broker instances each hold a shard of the dispatcher:

- Brokers **lease resource quotas** from a central Postgres DB atomically. Local quota
  reservations avoid per-job DB queries. On broker crash, unreleased reservations expire by TTL.
- **Persistent job ownership** uses owner-aware durable records plus broker lease TTL. Reclaim is
  allowed only after owner lease expiry/missing and uses ownership-aware claim semantics.
- **Ephemeral jobs** are lost on broker crash — this is acceptable by design.
- **Database loss** causes brokers to stop accepting new jobs. In-flight ephemeral jobs may
  continue; persistent jobs cannot be safely committed.
- **Network partition** — any broker unable to reach the DB is treated as if it has crashed.

---

## What is not yet implemented

- HTTP server (`src/api/` is a stub)
- Resource quota tracking and broker negotiation
- User statistics
