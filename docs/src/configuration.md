# Configuration

BITS configuration is a single YAML file with typed registries
(`checks`, `transforms`, `targets`) and an optional `routes` section.

Routes can be defined statically in the YAML or added programmatically at runtime via
`Bits::add_route()`. This is useful when the host application manages its own collection
or tenant model and maps each to a separate BITS route. See
[Programmatic Routes](#programmatic-routes) below.

## Registries

Registries hold **named, reusable action definitions**. Each entry has a `type:` key that
identifies the action, plus any action-specific fields.

```yaml
checks:
  is_privileged:
    type: match
    class: od

transforms:
  expand:
    type: metkit_expansion
    expand_parameters: true

targets:
  backend:
    type: http
    url: "http://my-service/api"
```

Named registry entries are referenced in routes using the `check::`, `transform::`, and
`target::` prefixes. This makes the type of each step visible in the route and ensures shared
resources (such as a target with a dispatcher) are the same instance in memory.

> The examples in this page use actions from the `bits-ecmwf` application crate
> (`match`, `metkit_expansion`, `has_role`, `schedule_released`, etc.). The core
> `bits` library ships only `target::http` and `target::remote`. Your application
> registers its own actions. See [Custom Actions](custom-actions.md).

## Routes

The `routes` section is **optional**. When present, it defines ordered pipelines of action steps
that are loaded at config parse time. Steps reference named registry entries or define actions
inline. When omitted, routes must be added programmatically via `add_route()`. See
[Programmatic Routes](#programmatic-routes).

```yaml
routes:
  - default:
      - transform::expand          # reference a named registry entry
      - switch:
          - privileged:
              - check::is_privileged
              - target::backend
          - public:
              - check::match:      # inline action - no registry entry needed
                  class: ea
              - target::backend
```

A `switch` tries its named routes in order and returns the result of the first route whose checks
all pass. Routes may be nested.

### Inline actions

Actions can be defined directly in a route without a registry entry:

```yaml
- check::match:
    class: od
```

Inline actions are not reusable. Each occurrence is an independent instance. Use the registry
when you want to share an action (and its dispatcher) across multiple routes.

### Role-based access control

`has_role` checks that the authenticated user belongs to a listed realm and holds at least one of
the allowed roles for that realm. The `roles` field is a map of realm name to allowed role lists:

```yaml
- check::has_role:
    roles:
      ecmwf:
        - admin
        - data_access
      cds:
        - viewer
```

A user passes if their realm appears in the map **and** they hold any of the roles listed under
that realm. Users whose realm is not listed, or who lack a matching role, are rejected.

Rejections use a generic `"insufficient permissions"` message to avoid leaking realm/role details.
Two distinct `WARN`-level log messages help operators diagnose failures:

- `"user realm not listed in allowed realms"`: the user's realm doesn't appear in the `roles` map.
- `"realm matched but user lacks a required role"`: the realm matched but the user holds none of the
  allowed roles.

## Dispatcher

Any action step (check, transform, or target) can include a `dispatcher:` section to control
queuing and concurrency.

For **named registry entries**, `dispatcher:` is nested inside the entry alongside `type:`:

```yaml
targets:
  backend:
    type: http
    url: "http://my-service/api"
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
            url: "http://my-service/api"
        dispatcher:
          queue: cost_weighted
          executor:
            type: async_pool
            concurrency: 8
```

### Dispatcher fields

| Field | Values | Default |
|-------|--------|---------|
| `queue` | `fifo`, `cost_weighted`, `age_priority` | `fifo` |
| `executor.type` | `async_pool`, `thread_pool`, `remote_pool` | `async_pool` |
| `executor.concurrency` | positive integer | 256 |

- `cost_weighted` ordering requires a `metadata["cost"]` value set by a prior transform.
- `age_priority` ages waiting jobs into service while still making larger-cost jobs wait longer to gain queue priority.
- `thread_pool` offloads work to dedicated OS threads. Use this for CPU-bound or blocking work.
- `remote_pool` is only valid with `target::remote` and is auto-inserted when using that target
  type. Any other combination is rejected at config parse time.
- For the remote worker HTTP API and worker lifecycle, see
  [External Workers](external-workers.md).

## Silent rejections

When all routes in a switch reject a job, the error returned to the user includes rejection
reasons from actions that are **not silent**. Route-selection checks (like `match`) are silent by
default, so their rejections are internal routing decisions. Validation checks (like
`schedule_released`) are not silent, so their rejections should reach the user.

Each action sets its own default. You can override per-step with the `silent` key:

For **named registry entries**:

```yaml
checks:
  schedule:
    type: schedule_released
    path: /etc/schedule.xml
    silent: true               # override: suppress this check's rejections
```

For **inline steps**:

```yaml
routes:
  - default:
      - check::match:
            class: od
        silent: false          # override: surface this match check's rejections
      - target::backend
```

Built-in defaults (from `bits-ecmwf`):

| Action | `silent` |
|--------|----------|
| `match` | `true` |
| `has_role` | `false` |
| `has_license` | `true` |
| `has_key` | `true` |
| `date_checker` | `false` |
| `schedule_released` | `false` |

## Top-level bits settings

The optional `bits:` section configures broker identity and persistence policy:

```yaml
bits:
  persist_after_ms: 10000    # persist jobs still in-flight after this many milliseconds
  poll_timeout_ms: 30000     # how long a poll can wait before returning Pending
  persist_guard_ms: 1000     # extra margin before a persisted job is eligible for reclaim
```

| Field | Purpose |
|-------|---------|
| `persist_after_ms` | Threshold before a job is written to durable storage. Jobs completing before this threshold are never persisted. |
| `poll_timeout_ms` | Maximum time a poll request waits before returning a `Pending` response to the client. |
| `persist_guard_ms` | Grace period added to the lease TTL before another broker may reclaim a persisted job. |

Persistence requires a configured storage backend. TiKV is supported when the `tikv` Cargo
feature is enabled. If TiKV configuration is present but the crate is built without the `tikv`
feature, startup fails immediately with a configuration error.

## Programmatic Routes

When the host application manages its own routing model (e.g. per-collection or per-tenant
routing), routes can be added after config load via `Bits::add_route()`.

The YAML file still defines registries (`checks`, `targets`, `transforms`), `bits:` settings, and
optionally a `worker_server:` block, but the `routes` section can be omitted entirely.

### add_route

`Bits::add_route()` parses a route YAML fragment against the already-loaded registries and
returns a `RouteHandle`:

```rust
let bits = Bits::from_config(config)?;

// Parse a route from a YAML value. Actions that reference named
// registry entries (e.g. target::backend) share the same Arc instances.
let handle: RouteHandle = bits.add_route("my_collection", &route_yaml_value)?;

// Submit a job via the named route handle
let job_handle = handle.submit(Job::new(json!({"class": "od"})));
```

### RouteHandle

`add_route()` returns a `RouteHandle`, an opaque handle the host application uses to submit
jobs to a specific route. The host keeps the mapping from its own collection/tenant names to
route handles.

### Shared resources

Routes added via `add_route()` share the same action registries and target `Arc`s as routes
defined in the YAML. This means a named target like `target::backend` with a dispatcher is the
same instance across all routes, so jobs from any route contend on the same concurrency pool and
queue.

### Worker server

When using `target::remote` with programmatic routes, remote pool targets may be registered
during `add_route()`, after the initial config parse. The worker server is started
automatically inside `add_route()` whenever remote pools have been registered. The call is
idempotent. Once the listener is bound, subsequent `add_route()` calls skip re-binding.

No explicit worker server management is needed from the host application.
