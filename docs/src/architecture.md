# Architecture

BITS routes each job through a pipeline of actions:

- **Check**: validate constraints and route eligibility
- **Transform**: mutate request payload and metadata
- **Target**: dispatch to an execution destination

Each action step can optionally attach a dispatcher that controls queue ordering and
concurrency. This makes scheduling behavior explicit at the route-step level.

At runtime, multiple brokers can cooperate. Correctness is not tied to ingress stickiness;
sticky routing is a performance optimization only.

For implementation-level detail and design rationale, see `design.md` and
`PERSISTENCE.md` in the repository root.
