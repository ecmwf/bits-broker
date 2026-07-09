// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

//! Core broker runtime, routing primitives, and built-in HTTP server for BITS.

/// Action traits, result enums, and built-in action implementations.
pub mod actions;
mod bits;
mod config;
/// Durable storage traits and in-memory / optional backend implementations.
pub mod db;
/// Queue and executor abstractions for scheduled action execution.
pub mod dispatcher;
/// Typed error hierarchy for the bits library.
pub mod error;
/// Job model and lifecycle state.
pub mod job;
/// Job lifecycle metrics (OpenTelemetry).
pub mod metrics;
/// Public request ID encoding and decoding helpers.
pub mod request_id;
/// Terminal job results returned to clients.
pub mod result;
mod route_handle;
/// Routing types used to build pipelines and branching behavior.
pub mod routing;
mod runtime;
/// Built-in Axum HTTP server for submitting and polling jobs.
pub mod server;
/// Telemetry helpers and configuration.
pub mod telemetry;
/// Shared HTTP server for all remote worker pools.
pub mod worker_server;

pub use actions::registry::{
    RuntimeActionFactory, create_action, list_actions, register_runtime_action,
};
pub use actions::*;
pub use bits::{ActiveJobSnapshot, Bits, DEFAULT_MAX_JOBS, JobHandle, PollOutcome, SubmitOutcome};
pub use config::{Bootstrap, RouteFactory, parse_bootstrap};
pub use db::*;
pub use error::{BitsError, ConfigError, RoutingError, WorkerServerError};
pub use job::Job;
pub use result::JobResult;
pub use route_handle::RouteHandle;
pub use server::ServerConfig;
