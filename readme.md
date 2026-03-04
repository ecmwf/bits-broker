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

- ⚡ **Fast and slow requests, one broker** — ephemeral jobs flow through the pipeline in-memory with no overhead; long-lived jobs opt into persistence with a single `persist` step.

- 📡 **Horizontal scalability with consistency** — multiple broker instances share quota of shared resources safely and efficiently.

- 🚦 **Queuing and backpressure** — any pipeline action can have bounded queues with configurable capacity and worker count.

- 🔀 **Push and pull targets** — push jobs to a target consumer directly from the pipeline, or publish to a topic for external workers to pull.

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

transforms:
  expand:
    type: metkit_expansion
    expand_parameters: true

targets:
  mars:
    type: mars_destination
    endpoint: "mars.ecmwf.int:8080"
    queue:
      type: fifo
      capacity: 200
      workers: 8

pipelines:
  ecmwf_data:
    - persist
    - transform::expand
    - switch:
        privileged:
          - check::is_privileged
          - target::mars
        public:
          - check::match:
              class: od
          - target::mars
```

Load it and process jobs:

```rust
let bits = Bits::from_config(config)?;
let result = bits.process(job).await;
```
