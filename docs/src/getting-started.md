# Getting Started

## Prerequisites

- Rust toolchain matching the version in `rust-toolchain.toml`
- Cargo workspace checked out locally

## Workspace layout

The repository is a Cargo workspace with these crates:

| Crate | Purpose |
|-------|---------|
| `bits` | Core library: routing engine, pipeline execution, dispatcher, persistence |
| `bits-server` | Standalone HTTP server binary wrapping the core library |
| `bits-py` | Asyncio-native Python extension (built with `maturin`) |

## Build and test

From the workspace root:

```bash
cargo build
cargo test
```

## Run the hello_bits example

The fastest way to see BITS in action:

```bash
cargo run -p bits --example hello_bits
```

This demonstrates custom actions and conditional routing. The config is embedded
in the example source. See `bits/examples/hello_bits.rs`.

## Write your own config

Create a file called `my-config.yaml`:

```yaml
routes:
  - default:
      - target::http:
          url: "http://httpbin.org/post"
```

This routes every job to `httpbin.org` which echoes back whatever you send.

Start the server:

```bash
cargo run -p bits-server -- my-config.yaml
```

Then submit a job from another terminal:

```bash
curl -X POST http://localhost:8080/job \
  -H "Content-Type: application/json" \
  -d '{"dataset": "era5", "date": "2024-01-15"}'
```

You'll get back either the result directly (if the backend responds within the
poll timeout) or a `303` redirect to `/job/{id}` for reconnection.

## Python extension

An asyncio-native Python interface is available via the `bits-py` crate. To build and install it
into your current Python environment:

```bash
pip install maturin aiohttp
maturin develop --manifest-path bits-py/Cargo.toml
```

Then run the HTTP server example:

```bash
python bits-py/examples/python_http_server.py
```

Or run the smaller Python examples under:

```bash
python bits-py/examples/hello_bits.py
python bits-py/examples/custom_check.py
python bits-py/examples/custom_target.py
python bits-py/examples/custom_transform.py
```

## Preview these docs

Once `mdbook` is installed:

```bash
mdbook serve docs
```

This serves the documentation locally with live reload at `http://localhost:3000`.
