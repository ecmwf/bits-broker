# HTTP Server API

BITS is a library first. The core API (`Bits::submit()`, `Bits::poll()`,
`Bits::cancel()`) is designed for you to embed in your application and build
whatever interface fits your needs.

The built-in HTTP server described here is a thin wrapper around that API. It
provides a working submit/poll interface and serves as a reference
implementation. The source is in `bits/src/server.rs`.

## Endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/job` | Submit a job (JSON body) and long-poll for the result. |
| `GET` | `/job/{id}` | Reconnect to an existing job and long-poll. |

## Submitting a job

Send the request payload as a JSON body:

```http
POST /job HTTP/1.1
Host: bits-broker:8080
Content-Type: application/json

{"class": "od", "param": "2t/msl", "date": "2024-01-15"}
```

The JSON body becomes the job's `request` field. Nothing is added or wrapped.

## Response status codes

| Status | Meaning |
|--------|---------|
| `200 OK` | Success with streaming data body. |
| `303 See Other` | Redirect to data URL, or pending (poll again). |
| `400 Bad Request` | Job-level validation error from the backend. |
| `404 Not Found` | Unknown job ID or already consumed. |
| `410 Gone` | Job cancelled, client disconnected, or owner broker lost. |
| `500 Internal Server Error` | Action panicked or system failure. |

## Structured error responses

Application-level 4xx/5xx responses from the direct owner broker return a JSON
body with three fields:

```json
{
  "code": "JOB_NOT_FOUND",
  "message": "job does not exist or was already consumed",
  "retryable": false
}
```

| Field | Type | Description |
|-------|------|-------------|
| `code` | string | Stable machine-readable error code. Safe to use in monitoring, alerting, and client-side branching. |
| `message` | string | Human-readable description. May change between releases. |
| `retryable` | boolean | Whether the client should retry the same HTTP request (GET poll or POST submit). Currently `false` for all outcomes. |

### Error codes by status

| Status | Code | When |
|--------|------|------|
| `404` | `JOB_NOT_FOUND` | Job ID doesn't exist or result was already consumed. |
| `410` | `JOB_LOST` | Job existed but owner broker disappeared without durable state. |
| `410` | `ACTION_CANCELLED` | Job was explicitly cancelled. |
| `410` | `ACTION_CLIENT_GONE` | Client disconnected and reconnect window expired. |
| `400` | `JOB_ERROR` | Backend returned a job-level validation error. |
| `500` | `JOB_FAILED` | Action panicked or returned a system-level failure. |

### Two code namespaces

The `code` field uses two naming conventions:

- **`JOB_*` codes** are HTTP-layer codes for poll and result outcomes. They
  exist only in HTTP responses and are defined in `bits::server`.
- **`ACTION_*` codes** bridge the library error system (`ActionError::code()`)
  and the HTTP layer. `ACTION_CANCELLED` and `ACTION_CLIENT_GONE` are the
  same codes used by `BitsError::code()`.

Library-level codes like `CONFIG_YAML_SYNTAX` or `PERSISTENCE_BACKEND` don't
appear in HTTP responses because config and persistence errors happen at
startup, not during job processing.

### What is NOT structured

Success responses (`200 OK`) return the raw streaming body from the backend,
not JSON. Redirect responses (`303 See Other`) return headers only.

Framework-level rejections (malformed JSON body, wrong Content-Type) are
handled by Axum before the handler runs and currently return plain text
errors, not structured JSON.

Proxied responses from a non-owner broker always use the structured JSON
error envelope, but the `code` and `message` fields may differ from the
owner's original response. The proxy maps HTTP status to `PollOutcome` and
back, which is lossy: for example, all `410` responses become
`ACTION_CANCELLED` regardless of the original code, and `500` from the owner
is treated as transient (returned as a pending redirect, not surfaced as
`JOB_FAILED`).

## Graceful shutdown

The server supports graceful shutdown via `serve_with_shutdown()`:

```rust
bits::server::serve_with_shutdown(
    bits,
    server_config,
    bits::server::shutdown_signal(),
).await?;
```

On SIGTERM/SIGINT, the server stops accepting new connections and drains
in-flight requests before returning.
