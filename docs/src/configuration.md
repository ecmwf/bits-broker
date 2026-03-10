# Configuration

BITS configuration is a single YAML file with four top-level sections: three typed registries
(`checks`, `transforms`, `targets`) and a `routes` section.

## Registries

Registries hold **named, reusable action definitions**. Each entry has a `type:` key that
identifies the action, plus any action-specific fields.

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
          - check::has_role:     # inline action — no registry entry needed
              role: registered
            - target::backend
```

A `switch` tries its named routes in order and returns the result of the first route whose checks
all pass. Routes may be nested.

### Inline actions

Actions can be defined directly in a route without a registry entry:

```yaml
- check::has_role:
    role: registered
```

Inline actions are not reusable — each occurrence is an independent instance. Use the registry
when you want to share an action (and its dispatcher) across multiple routes.

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
| `queue` | `fifo`, `cost_weighted` | none (FIFO when `concurrency` is set) |
| `executor` | `semaphore`, `thread_pool`, `remote_pool` | `semaphore` |
| `concurrency` | positive integer | unlimited |

- `cost_weighted` ordering requires a `metadata["cost"]` value set by a prior transform.
- `thread_pool` offloads work to dedicated OS threads — use this for CPU-bound or blocking work.
- `remote_pool` is only valid with `target::remote` and is auto-inserted when using that target
  type. Any other combination is rejected at config parse time.

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
