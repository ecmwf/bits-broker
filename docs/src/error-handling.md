# Error Handling

Every error from the BITS library is typed and matchable. Callers can
distinguish YAML syntax errors from missing config fields from persistence
failures, programmatically and without string parsing.

## Error hierarchy

```
BitsError
├── Config(ConfigError)
│   ├── Yaml(serde_yaml::Error)
│   ├── Validation { path, reason }
│   ├── MissingField { path }
│   ├── FeatureDisabled { path, feature }
│   ├── Decode { path, target, source }
│   └── PersistenceInit { path, backend, reason }
├── Routing(RoutingError)
│   ├── InvalidRoute { route, reason }
│   ├── InvalidAction { route, action, reason }
│   └── MissingTarget { route }
├── Persistence(DbError)
│   ├── Conflict(String)
│   └── Backend(String)
├── Action(ActionError)
│   ├── NetworkError(String)
│   ├── QueueFull(String)
│   ├── Timeout(String)
│   ├── ConfigError(String)
│   ├── AuthError(String)
│   ├── ResourceError(String)
│   ├── Cancelled
│   └── ClientGone
└── WorkerServer(WorkerServerError)
    ├── Bind { address, reason }
    └── PoolRegistration { name, reason }
```

`BitsError`, `ConfigError`, `RoutingError`, and `WorkerServerError` are
`#[non_exhaustive]`, so new variants can be added without breaking your match
arms (as long as you have a wildcard). `ActionError` and `DbError` are not
yet `#[non_exhaustive]` but may become so in a future release.

## Error codes

Every error has a stable machine-readable `code()` string. These codes are
part of the public API contract and won't change between releases:

| Code | Variant | Description |
|------|---------|-------------|
| `CONFIG_YAML_SYNTAX` | `ConfigError::Yaml` | Invalid YAML syntax. |
| `CONFIG_VALIDATION` | `ConfigError::Validation` | Field value is invalid. |
| `CONFIG_MISSING_FIELD` | `ConfigError::MissingField` | Required field is absent. |
| `CONFIG_FEATURE_DISABLED` | `ConfigError::FeatureDisabled` | Persistence needs a feature flag. |
| `CONFIG_DECODE` | `ConfigError::Decode` | Action config deserialization failed. |
| `CONFIG_PERSISTENCE_INIT` | `ConfigError::PersistenceInit` | Can't connect to NATS/TiKV. |
| `ROUTING_INVALID_ROUTE` | `RoutingError::InvalidRoute` | Route definition is invalid. |
| `ROUTING_INVALID_ACTION` | `RoutingError::InvalidAction` | Unknown action type or namespace. |
| `ROUTING_MISSING_TARGET` | `RoutingError::MissingTarget` | Route doesn't end with a target. |
| `PERSISTENCE_CONFLICT` | `DbError::Conflict` | CAS contention during claim. |
| `PERSISTENCE_BACKEND` | `DbError::Backend` | Persistence store error. |
| `ACTION_NETWORK` | `ActionError::NetworkError` | Upstream call failed. |
| `ACTION_QUEUE_FULL` | `ActionError::QueueFull` | Dispatcher queue at capacity. |
| `ACTION_TIMEOUT` | `ActionError::Timeout` | Operation timed out. |
| `ACTION_CONFIG` | `ActionError::ConfigError` | Action misconfigured. |
| `ACTION_AUTH` | `ActionError::AuthError` | Missing credentials. |
| `ACTION_RESOURCE` | `ActionError::ResourceError` | Resource unavailable. |
| `ACTION_CANCELLED` | `ActionError::Cancelled` | Job was cancelled. |
| `ACTION_CLIENT_GONE` | `ActionError::ClientGone` | Client disconnected. |
| `WORKER_BIND` | `WorkerServerError::Bind` | Worker server port in use. |
| `WORKER_POOL` | `WorkerServerError::PoolRegistration` | Pool registration failed. |

## Retryability

Some errors are caused by temporary conditions, such as a network blip, a busy queue,
two brokers racing to claim the same job. If you retry the same operation a
moment later, it will likely succeed. These are **retryable**.

Other errors are caused by something fundamentally wrong: invalid config, a
misspelled action name, a cancelled job. No amount of retrying will fix them.
The operator or developer needs to change something first. These are
**not retryable**.

`is_retryable()` tells you which category an error falls into, so your code
can decide automatically:

```rust
match bits.poll(&job_id, Some(Duration::from_secs(30))).await {
    PollOutcome::Ready(result) => handle(result),
    PollOutcome::Pending { .. } => { /* poll again */ }
    PollOutcome::NotFound => { /* job gone, don't retry */ }
}

// For startup errors:
match Bits::from_config(config) {
    Ok(bits) => run(bits),
    Err(e) if e.is_retryable() => {
        // Transient - back off and retry.
        // Examples: NATS connection timeout, CAS contention,
        // upstream network error, queue at capacity.
        eprintln!("Transient error [{}]: {e}, retrying...", e.code());
        tokio::time::sleep(Duration::from_secs(1)).await;
        // retry...
    }
    Err(e) => {
        // Permanent - retrying won't help, fix the cause.
        // Examples: invalid YAML, missing config field,
        // feature not compiled, bad credentials.
        eprintln!("Fatal error [{}]: {e}", e.code());
        std::process::exit(1);
    }
}
```

### Which errors are retryable?

| Error | Retryable | Why |
|-------|-----------|-----|
| `NetworkError` | Yes | Upstream call failed - might succeed on retry. |
| `Timeout` | Yes | Operation timed out - might complete with longer timeout. |
| `QueueFull` | Yes | Dispatcher queue at capacity - space may free up. |
| `DbError::Conflict` | Yes | CAS contention during claim - another broker won the race, try again. |
| `DbError::Backend` | Yes | Persistence store error - typically connection issues. |
| `ConfigError::*` | No | Config is broken - fix the YAML and restart. |
| `RoutingError::*` | No | Route definition is wrong - fix the config. |
| `AuthError` | No | Missing credentials - fix the user context. |
| `Cancelled` | No | Job was explicitly cancelled - don't retry cancelled work. |
| `ClientGone` | No | Client disconnected - nobody to deliver the result to. |
| `WorkerServerError::*` | No | Port binding failed - fix the config or free the port. |

### Retryability in HTTP responses

When building an HTTP API on top of BITS, use `is_retryable()` to set the
appropriate status code and headers:

```rust
if err.is_retryable() {
    // 503 Service Unavailable + Retry-After
    (StatusCode::SERVICE_UNAVAILABLE, [("Retry-After", "1")])
} else {
    // 500, no retry
    (StatusCode::INTERNAL_SERVER_ERROR, [])
}
```

## Matching on errors

```rust
use bits::{Bits, BitsError, ConfigError, RoutingError};

match Bits::from_config(config) {
    Ok(bits) => { /* ready */ }

    // YAML is broken
    Err(BitsError::Config(ConfigError::Yaml(e))) => {
        eprintln!("Fix your YAML syntax: {e}");
    }

    // Feature not compiled
    Err(BitsError::Config(ConfigError::FeatureDisabled { feature, .. })) => {
        eprintln!("Build with: cargo build --features {feature}");
    }

    // Can't connect to persistence store
    Err(BitsError::Config(ConfigError::PersistenceInit { backend, reason, .. })) => {
        eprintln!("{backend} is down: {reason}");
    }

    // Route validation failed
    Err(BitsError::Routing(RoutingError::MissingTarget { route })) => {
        eprintln!("Route '{route}' needs a target at the end");
    }

    // Anything else
    Err(e) => {
        eprintln!("Error [{}]: {e}", e.code());
        eprintln!("Retryable: {}", e.is_retryable());
    }
}
```

## Config error paths

Config errors include the YAML field path that caused the failure:

```
config: bits.persistence.broker_lease_ttl_secs: must be a finite number >= 1.0
config: bits.persistence.url: must not be empty
config: bits.persistence.type: feature 'nats' not enabled
config: bits.persistence: nats initialization failed: connection refused
```

This makes it easy to pinpoint the exact field without guessing.

## Error propagation for library users

`BitsError` implements `std::error::Error`, so it works with `?` in any
function that returns `Box<dyn Error>` or `anyhow::Result`:

```rust
// Works in anyhow context
fn setup() -> anyhow::Result<Bits> {
    Ok(Bits::from_config(config)?)
}

// Works in Box<dyn Error> context
fn setup() -> Result<Bits, Box<dyn std::error::Error>> {
    Ok(Bits::from_config(config)?)
}

// Best: use the typed error directly
fn setup() -> Result<Bits, BitsError> {
    Bits::from_config(config)
}
```

## Forward compatibility

All error enums use `#[non_exhaustive]`. To handle this safely:

```rust
match err {
    BitsError::Config(_) => { /* handle config errors */ }
    BitsError::Routing(_) => { /* handle routing errors */ }
    _ => { /* catch-all for future variants */ }
}
```

The `code()` strings are stable across releases. You can use them in
monitoring dashboards, alerting rules, and log aggregation queries:

```
bits_errors_total{code="CONFIG_YAML_SYNTAX"}
bits_errors_total{code="PERSISTENCE_BACKEND"}
```
