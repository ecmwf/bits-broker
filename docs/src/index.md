# BITS Documentation

BITS (Broker for Intelligent Task Scheduling) is a policy-aware job broker that classifies,
transforms, and dispatches requests across distributed infrastructure.

It is designed for environments where jobs vary widely in cost and duration — from millisecond
checks to long-running compute tasks — and where multiple broker instances must coordinate
without per-job coordination overhead.

## What BITS does

A client submits a job. BITS routes it through a configurable pipeline of **checks**,
**transforms**, and a **target**. The pipeline enforces access policy, mutates the request as
needed, and dispatches to the right backend. Short jobs are handled entirely in-memory; long
jobs are persisted so they survive broker restarts and can be recovered by other brokers.

## How to use these docs

- **[Getting Started](getting-started.md)** — build, test, and run BITS locally.
- **[Architecture](architecture.md)** — understand the core concepts: jobs, pipelines, actions,
  and how multiple brokers cooperate.
- **[Configuration](configuration.md)** — reference for YAML configuration: registries, routes,
  dispatchers, and top-level settings.
- **[Persistence](persistence.md)** — how jobs are persisted, recovered, and routed across
  broker instances.

For a concise overview and quick-start example, see the repository `README`.
For implementation-level design rationale, see `design.md` in the repository root.
