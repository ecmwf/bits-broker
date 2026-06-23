//! Job lifecycle metrics for the BITS broker.
//!
//! Records measurements using the OpenTelemetry global meter provider.
//! All instruments are no-ops when no provider is installed (tests, local dev).

use std::sync::OnceLock;
use std::time::Instant;

use opentelemetry::metrics::{Counter, Histogram, Meter, UpDownCounter};
use opentelemetry::{KeyValue, global};

use crate::result::JobResult;

const METER_NAME: &str = "bits";

const OUTCOME_KEY: &str = "outcome";
const ROUTE_HANDLE_KEY: &str = "route_handle";

struct GlobalInstruments {
    jobs_accepted: Counter<u64>,
    jobs_finished: Counter<u64>,
    job_duration: Histogram<f64>,
}

struct RouteHandleInstruments {
    jobs_accepted: Counter<u64>,
    jobs_finished: Counter<u64>,
    job_duration: Histogram<f64>,
}

fn meter() -> Meter {
    global::meter(METER_NAME)
}

fn global_instruments() -> &'static GlobalInstruments {
    static INSTANCE: OnceLock<GlobalInstruments> = OnceLock::new();
    INSTANCE.get_or_init(|| {
        let m = meter();
        GlobalInstruments {
            jobs_accepted: m.u64_counter("bits.jobs.accepted.total").build(),
            jobs_finished: m.u64_counter("bits.jobs.finished.total").build(),
            job_duration: m.f64_histogram("bits.job.duration.seconds").build(),
        }
    })
}

fn route_handle_instruments() -> &'static RouteHandleInstruments {
    static INSTANCE: OnceLock<RouteHandleInstruments> = OnceLock::new();
    INSTANCE.get_or_init(|| {
        let m = meter();
        RouteHandleInstruments {
            jobs_accepted: m
                .u64_counter("bits.route_handle.jobs.accepted.total")
                .build(),
            jobs_finished: m
                .u64_counter("bits.route_handle.jobs.finished.total")
                .build(),
            job_duration: m
                .f64_histogram("bits.route_handle.job.duration.seconds")
                .build(),
        }
    })
}

/// Maps a terminal `JobResult` to its stable outcome label value.
pub fn job_result_outcome(result: &JobResult) -> &'static str {
    match result {
        JobResult::Success { .. } => "success",
        JobResult::Redirect { .. } => "redirect",
        JobResult::Error { .. } => "error",
        JobResult::Failed { .. } => "failed",
        JobResult::Overloaded { .. } => "overloaded",
        JobResult::Cancelled => "cancelled",
        JobResult::ClientGone => "client_gone",
    }
}

// --- Dispatcher / queue metrics ---

struct DispatcherInstruments {
    queue_depth: UpDownCounter<i64>,
    queue_wait_seconds: Histogram<f64>,
}

fn dispatcher_instruments() -> &'static DispatcherInstruments {
    static INSTANCE: OnceLock<DispatcherInstruments> = OnceLock::new();
    INSTANCE.get_or_init(|| {
        let m = meter();
        DispatcherInstruments {
            queue_depth: m
                .i64_up_down_counter("bits.dispatcher.queue_depth")
                .with_description("Current number of jobs waiting in the dispatch queue")
                .build(),
            queue_wait_seconds: m
                .f64_histogram("bits.dispatcher.queue_wait.seconds")
                .with_description("Time a job spent waiting in the dispatch queue")
                .build(),
        }
    })
}

pub fn record_queue_enqueued() {
    dispatcher_instruments().queue_depth.add(1, &[]);
}

pub fn record_queue_dequeued(enqueued_at: Instant) {
    let instruments = dispatcher_instruments();
    instruments.queue_depth.add(-1, &[]);
    instruments
        .queue_wait_seconds
        .record(enqueued_at.elapsed().as_secs_f64(), &[]);
}

/// Call for each stranded item when the dispatcher closes.
pub fn record_queue_drained(count: usize) {
    if count > 0 {
        dispatcher_instruments().queue_depth.add(-(count as i64), &[]);
    }
}

// --- Whole-BITS metrics ---

pub fn record_job_accepted() {
    global_instruments().jobs_accepted.add(1, &[]);
}

pub fn record_job_finished(outcome: &str) {
    global_instruments()
        .jobs_finished
        .add(1, &[KeyValue::new(OUTCOME_KEY, outcome.to_owned())]);
}

pub fn record_job_duration(outcome: &str, seconds: f64) {
    global_instruments()
        .job_duration
        .record(seconds, &[KeyValue::new(OUTCOME_KEY, outcome.to_owned())]);
}

// --- RouteHandle-scoped metrics ---

pub fn record_route_handle_job_accepted(route_handle: &str) {
    route_handle_instruments().jobs_accepted.add(
        1,
        &[KeyValue::new(ROUTE_HANDLE_KEY, route_handle.to_owned())],
    );
}

pub fn record_route_handle_job_finished(route_handle: &str, outcome: &str) {
    route_handle_instruments().jobs_finished.add(
        1,
        &[
            KeyValue::new(ROUTE_HANDLE_KEY, route_handle.to_owned()),
            KeyValue::new(OUTCOME_KEY, outcome.to_owned()),
        ],
    );
}

pub fn record_route_handle_job_duration(route_handle: &str, outcome: &str, seconds: f64) {
    route_handle_instruments().job_duration.record(
        seconds,
        &[
            KeyValue::new(ROUTE_HANDLE_KEY, route_handle.to_owned()),
            KeyValue::new(OUTCOME_KEY, outcome.to_owned()),
        ],
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn outcome_maps_all_variants() {
        let cases: Vec<(JobResult, &str)> = vec![
            (
                JobResult::Success {
                    content_type: String::new(),
                    size: 0,
                    stream: Box::new(futures::stream::empty()),
                },
                "success",
            ),
            (
                JobResult::Redirect {
                    location: String::new(),
                    message: String::new(),
                },
                "redirect",
            ),
            (
                JobResult::Error {
                    message: String::new(),
                },
                "error",
            ),
            (
                JobResult::Failed {
                    reason: String::new(),
                },
                "failed",
            ),
            (
                JobResult::Overloaded {
                    reason: String::new(),
                },
                "overloaded",
            ),
            (JobResult::Cancelled, "cancelled"),
            (JobResult::ClientGone, "client_gone"),
        ];

        for (result, expected) in &cases {
            assert_eq!(job_result_outcome(result), *expected);
        }
    }
}
