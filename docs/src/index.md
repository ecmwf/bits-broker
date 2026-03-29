# BITS Documentation

BITS (Broker for Intelligent Task Scheduling) is a policy-aware job broker that classifies,
transforms, and dispatches requests across distributed infrastructure.

It is designed for environments where jobs vary widely in cost and duration, from millisecond
checks to long-running compute tasks, and where multiple broker instances must coordinate
without per-job coordination overhead.

## What BITS does

A client submits a job. BITS routes it through a configurable pipeline of **checks**,
**transforms**, and a **target**. The pipeline enforces access policy, mutates the request as
needed, and dispatches to the right backend. Short jobs are handled entirely in-memory; long
jobs are persisted so they survive broker restarts and can be recovered by other brokers.

## How to use these docs

**Getting started:**
- **[Quick Start](getting-started.md)** - build, test, and run BITS locally.

**Understand the system:**
- **[Architecture](architecture.md)** - core concepts: jobs, pipelines, actions,
  switches, dispatchers.
- **[Data Flow](data-flow.md)** - how data moves through the system, with
  diagrams showing what each component sends and receives.
- **[Error Handling](error-handling.md)** - typed error hierarchy, machine-readable
  codes, retryability hints.

**Configure and extend:**
- **[Configuration](configuration.md)** - YAML reference: registries, routes,
  dispatchers, top-level settings.
- **[Custom Actions](custom-actions.md)** - write checks, transforms, and targets
  in Rust or Python.

**Integrate:**
- **[HTTP Server API](http-api.md)** - submit jobs, poll for results, status
  codes, connection model.
- **[External Workers](external-workers.md)** - pull-based remote workers with
  HTTP long-poll.

**Operate:**
- **[Persistence](persistence.md)** - durable storage, recovery, multi-broker
  coordination.
- **[Deployment](deployment.md)** - production setup, persistence backends, load
  balancing, Kubernetes.
- **[Troubleshooting](troubleshooting.md)** - common issues and how to fix them.

For implementation-level design rationale, see `design.md` in the repository
root.
