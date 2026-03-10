# Architecture

## Jobs

A **job** is the unit of work in BITS. When a client submits a request, BITS creates a job
carrying:

- `original_request` — the payload exactly as submitted. Never mutated. Used as the restart
  point if the job needs to be recovered after a broker failure.
- `request` — a working copy of the payload, mutated by transform actions as the job flows
  through the pipeline.
- `metadata` — mutable annotations added by transforms during routing (for example, a computed
  cost value).
- `user` — identity of the submitting user.

## Pipelines

Every job flows through a **pipeline**: an ordered list of actions. BITS evaluates each action
in sequence. There are three action types:

### Check

A guard condition. It evaluates the job and either **passes** or **rejects**. A rejection stops
the current route and causes the enclosing `switch` to try the next named route. Checks do not
mutate the job.

Examples: `check::has_role`, `check::has_license`

### Transform

A mutation step. It modifies the job (for example, expanding request parameters or computing a
cost), then continues through the pipeline.

Examples: `transform::metkit_expansion`, `transform::cost`

### Target

The terminal dispatch step. It sends the job to a backend and returns a result. Every route must
end with a target.

Examples: `target::http`, `target::remote`

## Switch

A `switch` contains named routes and tries them **in order**, returning the result of the first
route whose checks all pass. This is the branching primitive — use it to express conditional
routing such as privileged versus public access paths. Switches can be nested inside routes.

## Dispatcher

Any action step — check, transform, or target — can have an optional **dispatcher** that controls
ordering and concurrency before the action runs.

A dispatcher has two concerns:

- **Queue** — controls which job runs next (`fifo` or `cost_weighted`).
- **Executor** — controls how the work runs (`semaphore`, `thread_pool`, or `remote_pool`).

This makes scheduling behavior explicit at the step level rather than a global setting. See
[Configuration](configuration.md) for dispatcher syntax and options.

## Multi-broker cooperation

In production, multiple broker instances run behind a load balancer. Each broker holds an
in-memory shard of the dispatcher state and a broker lease record in durable storage.

When a poll request arrives at a broker that does not own the job:

1. The broker parses the owner from the job ID.
2. It resolves the owner's endpoint from broker lease records.
3. It proxies the poll internally to the owner.
4. If the owner lease is missing or expired, it may claim and recover the job from durable
   storage.

For full details, see the [Persistence](persistence.md) section.

For implementation-level detail and design rationale, see `design.md` in the repository root.
