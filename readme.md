<div align="center">

# BITS

**Broker for Intelligent Task Scheduling**

[![Static Badge](https://github.com/ecmwf/codex/raw/refs/heads/main/Project%20Maturity/sandbox_badge.svg)](https://github.com/ecmwf/codex/raw/refs/heads/main/Project%20Maturity#sandbox)
[![Rust](https://img.shields.io/badge/rust-stable-blue)]()
[![License](https://img.shields.io/badge/license-Apache%202.0-blue)]()

</div>

> \[!IMPORTANT\]
> This software is **Sandbox** and subject to ECMWF's guidelines on [Software Maturity](https://github.com/ecmwf/codex/raw/refs/heads/main/Project%20Maturity).



> A policy-aware job broker that classifies, transforms, and dispatches requests across distributed infrastructure — with queuing, persistence, and fault recovery built in.

---

## ✨ Features

- ⚡ **Fast and slow requests, one broker** — ephemeral jobs flow through the pipeline in-memory with no overhead; long-lived jobs persist after a configurable in-flight threshold.

- 📡 **Horizontal scalability with consistency** — multiple broker instances share quota of shared resources safely and efficiently.

- 🚦 **Queuing and backpressure** — any pipeline step (check, transform, or target) can have a dispatcher with configurable ordering and concurrency.

- 🔀 **Push and pull targets** — push jobs directly to a target via HTTP, or use `target::remote` to hand off to an external worker pool.

- 💾 **Persistence and recovery** — persistent jobs survive broker termination. Jobs are rebalanced to live instances.

- 🔌 **Pluggable actions** — additional actions can be created in Rust or Python.


---

## 🚀 Quick Start

Define your routing policy in YAML:

```yaml
checks:
  is_privileged:
    type: has_role
    role: privileged

targets:
  backend:
    type: http
    url: "http://my-service/api"
    dispatcher:
      queue: cost_weighted   # order cheap jobs first
      concurrency: 8         # max simultaneous requests

routes:
  default:
    - switch:
        privileged:
          - check::is_privileged
          - target::backend
        public:
          - check::has_role:
              role: registered
          - target::backend
```

Load it and process jobs:

```rust
let bits = Bits::from_config(config)?;

// Submit a job — returns a handle immediately
let handle = bits.submit(Job::new(json!({"class": "od"})));

// Poll for the result (blocks until ready or timeout)
let outcome = bits.poll(&handle.id, Some(Duration::from_secs(30))).await;
```

### Async Python interface

An asyncio-native Python extension is available in the `bits-py` crate.

Build and install it into your current Python environment:

```bash
pip install maturin aiohttp
maturin develop --manifest-path bits-py/Cargo.toml
```

Run the Python HTTP server example:

```bash
python bits/examples/python_http_server.py
```

---

## 📐 Dispatcher Config

Any step in a pipeline can be given a dispatcher via a `dispatcher:` key. For named registry entries it sits alongside `type:`; for inline steps it is a sibling key of the action mapping:

```yaml
# Named registry entry
targets:
  mars:
    type: http
    url: "http://mars/api"
    dispatcher:
      queue: cost_weighted
      concurrency: 10

# Inline step
routes:
  default:
    - transform::metkit_expansion:
        expand_parameters: true
      dispatcher:
        concurrency: 4
    - target::http:
        url: "http://mars/api"
      dispatcher:
        queue: cost_weighted
        concurrency: 10
```

| Field | Values | Default |
|-------|--------|---------|
| `queue` | `fifo`, `cost_weighted` | none (FIFO when `concurrency` is set) |
| `executor` | `semaphore`, `thread_pool`, `remote_pool`* | `semaphore` |
| `concurrency` | positive integer | unlimited |

\* `remote_pool` is only valid with `target::remote`.

---

## 🏗 Architecture

See [design.md](design.md) for the full design specification.
