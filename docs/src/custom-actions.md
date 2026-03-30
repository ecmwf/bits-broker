# Writing Custom Actions

Actions are the extension point for adding new behaviour to BITS pipelines. You can add custom
checks, transforms, and targets in **Rust** or **Python**. Both share the same three action
types, the same YAML registration model, and the same job data model.

---

## The three action types

| Type | Method | Returns | Purpose |
|------|--------|---------|---------|
| `CheckAction` | `evaluate(job)` | `Pass` or `Reject` | Guard: pass or reject the current route. Does not mutate the job. |
| `TransformAction` | `execute(job)` | `Continue` or `Reject` | Mutate `job.request` / `job.metadata`, then continue. |
| `TargetAction` | `dispatch(job)` | `Success`, `Redirect`, `Error`, or `Reject` | Terminal dispatch: send the job and return a result. |

A `Reject` from any action type stops the current route and causes the enclosing `switch` to
try the next named branch. Each `Reject` carries a `silent` flag: when `silent` is `true` (the
default for route-selection checks), the rejection reason is not shown to the user. When `silent`
is `false` (the default for validation checks like `schedule_released`), the reason is included in
the error if all routes fail. The `silent` config key on any action step can override this default.

---

## The Job object

All actions receive a job. The fields relevant to action authors:

| Field | Writable | Description |
|-------|----------|-------------|
| `id` | No | Unique job identifier (`{broker_id}~{uuid}`). |
| `request` | Transform only | Working request payload. Mutated by transforms; read by checks and targets. |
| `original_request` | No | Snapshot of the request as submitted. Used as restart point on recovery. Never modify this. |
| `metadata` | Transform only | Pipeline-internal annotations (cost, roles, license, etc.). Written by transforms; read by checks, targets, and dispatchers. |
| `user` | No | Identity of the submitting user. |

---

## Check action

A check reads the job and returns `Pass` or `Reject`.

{{#tabs global="lang" }}
{{#tab name="Rust" }}

```rust
use bits::actions::{ActionError, CheckAction, CheckResult};
use bits::Job;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct HasLicense {
    pub license: String,
}

#[async_trait]
impl CheckAction for HasLicense {
    async fn evaluate(&self, job: &Job) -> Result<CheckResult, ActionError> {
        match job.metadata.get("license").and_then(|v| v.as_str()) {
            Some(l) if l == self.license => Ok(CheckResult::Pass),
            Some(l) => Ok(CheckResult::Reject {
                reason: format!("license '{}' does not match required '{}'", l, self.license),
                silent: true,
            }),
            None => Ok(CheckResult::Reject {
                reason: "no license field found".to_string(),
                silent: true,
            }),
        }
    }
}

bits::register_action!(check, "has_license", HasLicense);
```

{{#endtab }}
{{#tab name="Python" }}

```python
from bits_py import CheckAction, Pass, Reject, register_action

class HasLicense(CheckAction):
    def __init__(self, license: str):
        self.license = license

    async def evaluate(self, job) -> Pass | Reject:
        actual = (job.metadata or {}).get("license")
        if actual == self.license:
            return Pass()
        reason = ("no license field found" if actual is None
                  else f"license '{actual}' does not match required '{self.license}'")
        return Reject(reason)

register_action("has_license", HasLicense)
```

{{#endtab }}
{{#endtabs }}

**YAML**

```yaml
checks:
  needs_open_license:
    type: has_license
    license: open-data
```

---

## Transform action

A transform mutates `job.request` and/or `job.metadata`, then returns `Continue`.

{{#tabs global="lang" }}
{{#tab name="Rust" }}

```rust
use bits::actions::{ActionError, TransformAction, TransformResult};
use bits::Job;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct ComputeCost {
    pub cost_field: String,
}

#[async_trait]
impl TransformAction for ComputeCost {
    async fn execute(&self, job: &mut Job) -> Result<TransformResult, ActionError> {
        let cost = job.request
            .get(&self.cost_field)
            .and_then(|v| v.as_f64())
            .unwrap_or(1.0);

        let metadata = job.metadata.as_object_mut()
            .ok_or_else(|| ActionError::ConfigError("metadata is not an object".into()))?;
        metadata.insert("cost".to_string(), serde_json::json!(cost));

        Ok(TransformResult::Continue)
    }
}

bits::register_action!(transform, "compute_cost", ComputeCost);
```

{{#endtab }}
{{#tab name="Python" }}

```python
from bits_py import TransformAction, Continue, register_action

class ComputeCost(TransformAction):
    def __init__(self, cost_field: str):
        self.cost_field = cost_field

    async def execute(self, job) -> Continue:
        cost = float((job.request or {}).get(self.cost_field, 1.0))
        meta = dict(job.metadata) if job.metadata else {}
        meta["cost"] = cost
        job.metadata = meta  # must assign back - in-place mutation is not detected
        return Continue()

register_action("compute_cost", ComputeCost)
```

> **Note:** Mutations to `job.request` and `job.metadata` are propagated back into the
> Rust pipeline only when you **assign** to the attribute (e.g. `job.metadata = new_dict`).
> Mutating the returned dict in-place without reassigning has no effect.

{{#endtab }}
{{#endtabs }}

**YAML**

```yaml
transforms:
  cost:
    type: compute_cost
    cost_field: volume_mb
```

---

## Target action

A target dispatches the job and returns one of these outcome types:

**In Rust:**

| Return | Meaning |
|--------|---------|
| `TargetResult::Complete(JobResult::Success { stream, content_type, size })` | Job completed with a streaming body. |
| `TargetResult::Complete(JobResult::Redirect { location, message })` | Client should follow a redirect URL. |
| `TargetResult::Complete(JobResult::Error { message })` | Job-level error (invalid request, validation failure). Terminal, not retried. |
| `TargetResult::Reject { reason, silent }` | This route cannot handle the job. The switch tries the next branch. |
| `Err(ActionError::NetworkError(...))` | Transient upstream failure. |
| `Err(ActionError::ResourceError(...))` | Non-transient resource issue. |

**In Python:**

| Return | Meaning |
|--------|---------|
| `Success(body, content_type="...")` | Job completed. `body` is `bytes` or `str` (auto-encoded UTF-8). |
| `Success.json(value)` | Convenience: serializes a Python dict/list as `application/json`. Equivalent to `Success(json.dumps(value).encode(), content_type="application/json")`. |
| `Redirect(location, message="")` | Client should follow a redirect URL. |
| `Error(message)` | Job-level error (invalid request, auth failure, etc.). |
| `Reject(reason)` | This route cannot handle the job. The switch tries the next branch. |

When using `target::remote`, an external worker returns outcomes by posting
to the `/{pool}/complete/...` endpoints. See [External Workers](external-workers.md).

{{#tabs global="lang" }}
{{#tab name="Rust" }}

```rust
use bits::actions::{ActionError, TargetAction, TargetResult};
use bits::result::JobResult;
use bits::Job;
use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
pub struct MyHttpTarget {
    pub url: String,
}

#[async_trait]
impl TargetAction for MyHttpTarget {
    async fn dispatch(&self, job: &Job) -> Result<TargetResult, ActionError> {
        let client = reqwest::Client::new();
        let response = client
            .post(&self.url)
            .json(&job.request)
            .send()
            .await
            .map_err(|e| ActionError::NetworkError(e.to_string()))?;

        if response.status().is_client_error() {
            return Ok(TargetResult::Reject {
                reason: format!("upstream rejected: {}", response.status()),
                silent: true,
            });
        }
        if !response.status().is_success() {
            return Err(ActionError::NetworkError(format!(
                "upstream error: {}", response.status()
            )));
        }

        let body = Bytes::from(
            response.bytes().await.map_err(|e| ActionError::NetworkError(e.to_string()))?
        );
        let size = body.len() as i64;
        let stream = Box::new(futures::stream::iter(vec![Ok::<_, std::io::Error>(body)]));
        Ok(TargetResult::Complete(JobResult::Success {
            content_type: "application/octet-stream".to_string(),
            size,
            stream,
        }))
    }
}

bits::register_action!(target, "my_http", MyHttpTarget);
```

{{#endtab }}
{{#tab name="Python" }}

```python
import aiohttp
from bits_py import TargetAction, Success, Redirect, Error, Reject, register_action

class MyHttpTarget(TargetAction):
    def __init__(self, url: str):
        self.url = url

    async def dispatch(self, job) -> Success | Redirect | Error | Reject:
        async with aiohttp.ClientSession() as session:
            async with session.post(self.url, json=job.request) as resp:
                if resp.status == 400:
                    return Error(f"bad request: {await resp.text()}")
                if resp.status == 403:
                    return Reject("upstream rejected request")
                if not resp.ok:
                    raise RuntimeError(f"upstream error: {resp.status}")
                body = await resp.read()
                return Success(body, content_type=resp.content_type)

register_action("my_http", MyHttpTarget)
```

{{#endtab }}
{{#endtabs }}

**YAML**

```yaml
targets:
  my_backend:
    type: my_http
    url: "http://my-service/api"
```

---

## Registration

{{#tabs global="lang" }}
{{#tab name="Rust" }}

### `register_action!` macro

Call the macro at the bottom of the same file as the implementation. It uses the
[`inventory`](https://docs.rs/inventory) crate for compile-time distributed registration. No
central file needs to be edited.

```rust
bits::register_action!(check,     "my_check",     MyCheck);
bits::register_action!(transform, "my_transform", MyTransform);
bits::register_action!(target,    "my_target",    MyTarget);
```

The macro deserialises the YAML config block into your struct via `serde_json`. Your struct must
derive `Deserialize`. All YAML keys except `type`, `dispatcher`, and `silent` are passed as
config fields.

### Crate setup

Actions can live in any crate that depends on `bits`. A minimal `Cargo.toml`:

```toml
[dependencies]
bits        = { path = "../bits" }
async-trait = "0.1"
serde       = { version = "1", features = ["derive"] }
serde_json  = "1"
inventory   = "0.3"
```

Make sure the crate is actually linked into your binary. `inventory` relies on
static initialisation, which only fires if the crate is linked. If the action
crate is not a direct dependency of the binary, the linker may drop it
entirely and your actions won't be registered.

To force linkage, add an explicit `use` in your binary's `main.rs`:

```rust
// Force the linker to include the my_actions crate so that
// its register_action! calls fire during static initialisation.
use my_actions as _;
```

This works in standard Cargo debug and release builds. If you encounter a
platform where aggressive dead-code elimination still drops the registrations
(actions missing at runtime), expose a dummy init function from your crate
and call it from main:

```rust
// In my_actions/src/lib.rs:
pub fn init() {} // no-op, just ensures linkage

// In your binary's main.rs:
my_actions::init();
```

Without forced linkage, `Bits::from_config` won't find your actions and will
return `ROUTING_INVALID_ACTION` errors.

### Error handling

Prefer returning `Reject { reason, silent }` for expected policy rejections. Set `silent: true`
for route-selection logic (wrong class, missing key) and `silent: false` for validation failures
the user should see (data not released, date out of range). Reserve `Err(ActionError)` for
unexpected system failures.

| Variant | When to use |
|---------|-------------|
| `NetworkError(msg)` | A transient upstream call failed. |
| `ConfigError(msg)` | The action was misconfigured. Typically raised during `serde` deserialisation. |
| `AuthError(msg)` | The job lacks required credentials. |
| `ResourceError(msg)` | A required resource (quota, license, etc.) is unavailable. |
| `Timeout(msg)` | An operation timed out. |
| `Cancelled` | The job was cancelled. Check `job.is_cancelled()` in long-running actions. |
| `ClientGone` | The client disconnected before the result could be delivered. |

{{#endtab }}
{{#tab name="Python" }}

### `register_action(name, class)`

Call `register_action` **before** `Bits.from_config`. Registration is validated immediately:

- The class must subclass exactly one of `CheckAction`, `TransformAction`, or `TargetAction`.
- The required method (`evaluate`, `execute`, or `dispatch`) must exist and be `async`.
- The name must not already be registered (including built-in action names).

```python
from bits_py import Bits, TargetAction, Success, register_action

class MyTarget(TargetAction):
    async def dispatch(self, job):
        return Success(b"hello")

register_action("my_target", MyTarget)          # register first
bits = await Bits.from_config(config_yaml)      # then load config
```

YAML config keys (excluding `type`, `dispatcher`, and `silent`) are forwarded to `__init__` as
keyword arguments when the action is instantiated at config-load time:

```yaml
checks:
  gate:
    type: my_check
    role: admin        # → MyCheck(role="admin")
```

### Error handling

Prefer returning `Reject(reason)` for expected policy rejections. Pass `silent=False` when the
rejection reason should reach the user (e.g. data not yet released); the default is `silent=True`
(route-selection). For unexpected failures, raise an exception. It surfaces as a `Failed` result
to the client.

{{#endtab }}
{{#endtabs }}
