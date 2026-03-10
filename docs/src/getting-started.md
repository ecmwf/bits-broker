# Getting Started

## Prerequisites

- Rust toolchain matching the version in `rust-toolchain.toml`
- Cargo workspace checked out locally
- A YAML configuration file defining your checks, transforms, targets, and routes

## Workspace layout

The repository is a Cargo workspace with these crates:

| Crate | Purpose |
|-------|---------|
| `bits` | Core library: routing engine, pipeline execution, dispatcher, persistence |
| `bits-ecmwf` | Application crate built on the core library with ECMWF-specific actions |
| `bits-py` | Asyncio-native Python extension (built with `maturin`) |

## Build and test

From the workspace root:

```bash
cargo build
cargo test
```

## Run locally

The `bits-ecmwf` crate contains runnable examples. Start by copying and adapting one of the
example configs from `bits-ecmwf/examples/`, or adapt `design_config.yaml` in the repository root.

To run an example:

```bash
cargo run --bin <example-name>
```

## Python extension

An asyncio-native Python interface is available via the `bits-py` crate. To build and install it
into your current Python environment:

```bash
pip install maturin aiohttp
maturin develop --manifest-path bits-py/Cargo.toml
```

Then run the HTTP server example:

```bash
python bits/examples/python_http_server.py
```

## Preview these docs

Once `mdbook` is installed:

```bash
mdbook serve docs
```

This serves the documentation locally with live reload at `http://localhost:3000`.
