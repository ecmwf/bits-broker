<!--
SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)

SPDX-License-Identifier: Apache-2.0
-->

<div align="center">

# BITS

**Broker for Intelligent Task Scheduling**

[![Static Badge](https://github.com/ecmwf/codex/raw/refs/heads/main/Project%20Maturity/incubating_badge.svg)](https://github.com/ecmwf/codex/raw/refs/heads/main/Project%20Maturity#incubating)
[![Rust](https://img.shields.io/badge/rust-stable-blue)]()
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)](LICENSE)

</div>

> \[!IMPORTANT\]
> This software is **Incubating** and subject to ECMWF's guidelines on [Software Maturity](https://github.com/ecmwf/codex/raw/refs/heads/main/Project%20Maturity).



**A policy-aware job broker that classifies, transforms, and dispatches requests across distributed infrastructure — with queuing, persistence, and fault recovery built in.**

## Features

- **Fast and slow requests, one broker** — ephemeral jobs flow through the pipeline in-memory with no overhead; long-lived jobs persist after a configurable in-flight threshold.

- **Horizontal scalability with consistency** — multiple broker instances share quota of shared resources safely and efficiently.

- **Queuing and backpressure** — any pipeline step (check, transform, or target) can have a dispatcher with configurable ordering and concurrency.

- **Persistence and recovery** — long-running jobs can be persisted after a configurable in-flight threshold, then reclaimed by live brokers after owner lease expiry.

- **Pluggable actions** — additional actions can be created in Rust or Python.

---

## Quick Start

Define your routing policy in YAML:

```yaml
bits:
  site: dev
  env: loc

checks:
  is_operational:
    type: match
    class: od

targets:
  backend:
    type: http
    url: "http://my-service/api"
    dispatcher:
      queue: cost_weighted
      executor:
        type: async_pool
        concurrency: 8

routes:
  - default:
      - check::is_operational
      - target::backend
```

Load it and process jobs:

```rust
let bits = Bits::from_config(config)?;

// Submit a job — returns a handle immediately
let handle = bits.submit(Job::new(json!({"class": "od"})))
    .expect_accepted("broker at capacity");

// Poll for the result (blocks until ready or timeout)
let outcome = bits.poll(&handle.id, Some(Duration::from_secs(30))).await;
```

### Async Python interface

An asyncio-native Python extension is available in the `bits-py` crate.

Build and install it into your current Python environment:

```bash
pip install maturin
maturin develop --manifest-path bits-py/Cargo.toml
```

**Submitting jobs from Python:**

```python
import asyncio
from bits_py import Bits

async def main():
    bits = await Bits.from_config(open("config.yaml").read())
    job_id = await bits.submit({"dataset": "era5", "year": 2020})
    outcome = await bits.poll(job_id, timeout_secs=30.0)
    print(outcome)

asyncio.run(main())
```

**Writing custom actions in Python:**

Register Python action classes before loading the config. Subclass the
appropriate ABC and implement the async method:

```python
from bits_py import (
    Bits, CheckAction, TargetAction,
    Pass, Reject, Success,
    register_action,
)

class HasRole(CheckAction):
    def __init__(self, role: str):
        self.role = role

    async def evaluate(self, job) -> Pass | Reject:
        roles = (job.user or {}).get("roles", [])
        return Pass() if self.role in roles else Reject(f"missing role: {self.role}")

class EchoTarget(TargetAction):
    async def dispatch(self, job) -> Success:
        return Success.json({"echo": job.request})

# Register before from_config
register_action("has_role_py", HasRole)
register_action("echo", EchoTarget)

config = """
bits:
  site: dev
  env: loc

checks:
  gate:
    type: has_role_py
    role: admin
targets:
  echo:
    type: echo
routes:
  - default:
      - check::gate
      - target::echo
"""

async def main():
    bits = await Bits.from_config(config)
    job_id = await bits.submit({})
    print(await bits.poll(job_id, timeout_secs=5.0))
```

See [Writing Custom Actions](docs/src/custom-actions.md) for the full Python API reference.

---

## Programmatic Routes

The `routes` section in YAML is optional. When your application manages its own collection or
tenant model, you can add routes at runtime via `Bits::add_route()`:

```rust
let bits = Bits::from_config(config)?;

// Add a route from a YAML fragment — shared targets use the same Arc
let handle = bits.add_route("my_collection", &route_yaml)?;

// Submit via the named route
let job_handle = handle.submit(Job::new(json!({"class": "od"})))
    .expect_accepted("broker at capacity");
```

If the route uses `target::remote`, the worker server is started automatically inside
`add_route()` — no extra call is needed.

Routes added this way share registries and target instances with YAML-defined routes. See
[Programmatic Routes](docs/src/configuration.md#programmatic-routes) for details.

---

## Dispatcher Config

Any step in a pipeline can be given a dispatcher via a `dispatcher:` key. For named registry entries it sits alongside `type:`; for inline steps it is a sibling key of the action mapping:

```yaml
# Named registry entry
targets:
  mars:
    type: http
    url: "http://mars/api"
    dispatcher:
      queue: cost_weighted
      executor:
        type: async_pool
        concurrency: 10

# Inline step
routes:
  - default:
      - transform::metkit_expansion:
            expand_parameters: true
        dispatcher:
          executor:
            type: async_pool
            concurrency: 4
      - target::http:
            url: "http://mars/api"
        dispatcher:
          queue: cost_weighted
          executor:
            type: async_pool
            concurrency: 10
```

| Field | Values | Default |
| ------- | -------- | --------- |
| `queue` | `fifo`, `cost_weighted`, `age_priority` | `fifo` |
| `executor.type` | `async_pool`, `thread_pool`, `remote_pool`* | `async_pool` |
| `executor.concurrency` | positive integer | 256 |

\* `remote_pool` is only valid with `target::remote`.

---

## Metrics

BITS is instrumented with [OpenTelemetry](https://opentelemetry.io/). Build with the
`metrics-prometheus` feature (enabled by default in the `bits-server` binary) to install a
Prometheus exporter and expose a scrapeable endpoint on the same port as the job API:

```
GET /metrics
```

Exposed series include job counters (`bits_jobs_accepted_total`, `bits_jobs_finished_total`
by `outcome`), duration histograms (`bits_job_duration_seconds`), and dispatcher
instruments (`bits_dispatcher_queue_depth`, `bits_dispatcher_queue_wait_seconds`), plus
per-route variants labelled by `route_handle`.

Histogram bucket boundaries (seconds) are configurable via an optional top-level `metrics:`
section, with sensible defaults:

```yaml
metrics:
  duration_buckets:   [0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 25, 60, 120]
  queue_wait_buckets: [0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1, 2.5, 5, 10, 30]
```

When BITS is embedded as a library, the instrumentation is a no-op unless a meter provider
is installed — either call `bits::metrics::init_prometheus(..)`, or install your own
OpenTelemetry provider. See [Deployment → Metrics](docs/src/deployment.md) for the full
metric list and a Prometheus scrape config.

---

## Architecture

<p align="center">
  <img src="docs/src/images/bits_pipeline_tree.svg" alt="BITS pipeline routing architecture" width="100%">
</p>

See [design.md](design.md) for the full design specification.

---

## Developer hooks

This repo includes a pre-commit configuration that runs rustfmt automatically:

```bash
pip install pre-commit
pre-commit install
```

The hook runs `cargo fmt --all` on each commit.
You can also run it manually:

```bash
pre-commit run --all-files
```

## License

[Apache License 2.0](LICENSE) In applying this licence, ECMWF does not waive the privileges and immunities granted to it by virtue of its status as an intergovernmental organisation nor does it submit to any jurisdiction.
