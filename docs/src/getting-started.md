# Getting Started

## Prerequisites

- Rust toolchain matching `rust-toolchain.toml`
- Cargo workspace checked out locally
- A YAML config defining checks, transforms, targets, and routes

## Build and test

From the workspace root:

```bash
cargo test
```

## Run locally

The workspace contains:

- `bits` - core routing and dispatching library
- `bits-ecmwf` - application crate using the core library

Start by adapting `design_config.yaml` or example configs in `bits-ecmwf/examples/`.

## Preview docs

Once mdBook is installed:

```bash
mdbook serve docs
```

This serves the documentation locally with live reload.
