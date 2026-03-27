# Configuration

BITS configuration is a single YAML file with four top-level sections: three typed registries
(`checks`, `transforms`, `targets`) and a `routes` section.

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

## Routes

Routes define ordered pipelines of action steps. Steps reference named registry entries or define
actions inline.

```yaml
routes:
  default:
    - transform::expand          # reference a named registry entry
    - switch:
        privileged:
          - check::is_privileged
          - target::backend
        public:
          - check::match:        # inline action — no registry entry needed
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

Inline actions are not reusable — each occurrence is an independent instance. Use the registry
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
Detailed rejection reasons are logged at `WARN` level for operator visibility.

## Dispatcher

Any action step — check, transform, or target — can include a `dispatcher:` section to control
queuing and concurrency.

For **named registry entries**, `dispatcher:` is nested inside the entry alongside `type:`:

```yaml
targets:
  backend:
    type: http
    url: "http://my-service/api"
    dispatcher:
      queue: cost_weighted
      concurrency: 8
```

For **inline steps**, `dispatcher:` is a sibling key in the action mapping:

```yaml
routes:
  default:
    - target::http:
        url: "http://my-service/api"
      dispatcher:
        queue: cost_weighted
        concurrency: 8
```

### Dispatcher fields

| Field | Values | Default |
|-------|--------|---------|
| `queue` | `fifo`, `cost_weighted`, `age_priority` | none (FIFO when `concurrency` is set) |
| `executor` | `async_pool`, `thread_pool`, `remote_pool` | `async_pool` |
| `concurrency` | positive integer | unlimited |

- `cost_weighted` ordering requires a `metadata["cost"]` value set by a prior transform.
- `age_priority` ages waiting jobs into service while still making larger-cost jobs wait longer to gain queue priority.
- `thread_pool` offloads work to dedicated OS threads — use this for CPU-bound or blocking work.
- `remote_pool` is only valid with `target::remote` and is auto-inserted when using that target
  type. Any other combination is rejected at config parse time.
- For the remote worker HTTP API and worker lifecycle, see
  [External Workers](external-workers.md).

## Silent rejections

When all routes in a switch reject a job, the error returned to the user includes rejection
reasons from actions that are **not silent**. Route-selection checks (like `match`) are silent by
default — their rejections are internal routing decisions. Validation checks (like
`schedule_released`) are not silent — their rejections should reach the user.

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

Built-in defaults:

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
