# Configuration

BITS configuration is defined in YAML with typed registries and routes.

## Registries

- `checks`
- `transforms`
- `targets`

Named entries are reused by route steps such as `check::name`, `transform::name`, and
`target::name`.

## Routes

Routes define ordered pipelines of steps and may contain nested `switch` blocks for branching.

## Dispatcher

Any action step can include a `dispatcher:` section for queueing and execution controls.

Typical fields:

- `queue`: `fifo` or `cost_weighted`
- `executor`: `semaphore`, `thread_pool`, or `remote_pool`
- `concurrency`: positive integer limit
- `persistent`: opt in to durable job tracking at the dispatcher level

Persistence is not a standalone route step. It is configured on dispatcher-capable actions.

## Bits-level settings

Top-level `bits` settings may include broker identity and persistence backend controls,
including TiKV settings when the `tikv` feature is enabled.
