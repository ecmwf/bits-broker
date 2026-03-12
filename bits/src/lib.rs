//! Core broker runtime, routing primitives, and built-in HTTP server for BITS.

/// Action traits, result enums, and built-in action implementations.
pub mod actions;
mod bits;
mod config;
/// Durable storage traits and in-memory / optional backend implementations.
pub mod db;
/// Queue and executor abstractions for scheduled action execution.
pub mod dispatcher;
/// Job model and lifecycle state.
pub mod job;
/// Terminal job results returned to clients.
pub mod result;
/// Routing types used to build pipelines and branching behavior.
pub mod routing;
mod runtime;
/// Built-in Axum HTTP server for submitting and polling jobs.
pub mod server;
/// Telemetry helpers and configuration.
pub mod telemetry;

pub use actions::registry::{
    RuntimeActionFactory, create_action, list_actions, register_runtime_action,
};
pub use actions::*;
pub use bits::{Bits, JobHandle, PollOutcome};
pub use config::{Bootstrap, parse_bootstrap};
pub use db::*;
pub use job::Job;
pub use result::JobResult;
pub use server::ServerConfig;
