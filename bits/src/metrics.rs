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

/// Histogram bucket boundaries (in seconds) for the broker's duration and
/// queue-wait histograms. Resolved from config; defaults live here next to the
/// instruments they describe.
#[derive(Debug, Clone)]
pub struct HistogramBuckets {
    /// Boundaries for the job-duration histograms
    /// (`bits.job.duration.seconds` and the route_handle variant).
    pub duration: Vec<f64>,
    /// Boundaries for the dispatcher queue-wait histogram
    /// (`bits.dispatcher.queue_wait.seconds`).
    pub queue_wait: Vec<f64>,
}

impl Default for HistogramBuckets {
    fn default() -> Self {
        Self {
            duration: vec![
                0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 25.0, 60.0, 120.0,
            ],
            queue_wait: vec![
                0.001, 0.005, 0.01, 0.05, 0.1, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0,
            ],
        }
    }
}

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
            jobs_accepted: m.u64_counter("bits.jobs.accepted").build(),
            jobs_finished: m.u64_counter("bits.jobs.finished").build(),
            job_duration: m.f64_histogram("bits.job.duration.seconds").build(),
        }
    })
}

fn route_handle_instruments() -> &'static RouteHandleInstruments {
    static INSTANCE: OnceLock<RouteHandleInstruments> = OnceLock::new();
    INSTANCE.get_or_init(|| {
        let m = meter();
        RouteHandleInstruments {
            jobs_accepted: m.u64_counter("bits.route_handle.jobs.accepted").build(),
            jobs_finished: m.u64_counter("bits.route_handle.jobs.finished").build(),
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
        dispatcher_instruments()
            .queue_depth
            .add(-(count as i64), &[]);
    }
}

// --- Whole-BITS metrics ---

pub fn record_job_accepted() {
    global_instruments().jobs_accepted.add(1, &[]);
}

pub fn record_job_finished(outcome: &'static str) {
    global_instruments()
        .jobs_finished
        .add(1, &[KeyValue::new(OUTCOME_KEY, outcome)]);
}

pub fn record_job_duration(outcome: &'static str, seconds: f64) {
    global_instruments()
        .job_duration
        .record(seconds, &[KeyValue::new(OUTCOME_KEY, outcome)]);
}

// --- RouteHandle-scoped metrics ---

pub fn record_route_handle_job_accepted(route_handle: &str) {
    route_handle_instruments().jobs_accepted.add(
        1,
        &[KeyValue::new(ROUTE_HANDLE_KEY, route_handle.to_owned())],
    );
}

pub fn record_route_handle_job_finished(route_handle: &str, outcome: &'static str) {
    route_handle_instruments().jobs_finished.add(
        1,
        &[
            KeyValue::new(ROUTE_HANDLE_KEY, route_handle.to_owned()),
            KeyValue::new(OUTCOME_KEY, outcome),
        ],
    );
}

pub fn record_route_handle_job_duration(route_handle: &str, outcome: &'static str, seconds: f64) {
    route_handle_instruments().job_duration.record(
        seconds,
        &[
            KeyValue::new(ROUTE_HANDLE_KEY, route_handle.to_owned()),
            KeyValue::new(OUTCOME_KEY, outcome),
        ],
    );
}

#[cfg(feature = "metrics-prometheus")]
mod prometheus_export {
    use std::sync::{Arc, OnceLock};

    use opentelemetry_sdk::metrics::{Aggregation, Instrument, SdkMeterProvider, Stream};
    use prometheus::{Encoder, TextEncoder};

    use super::HistogramBuckets;

    /// Cloneable handle over the installed Prometheus registry. Rendering it
    /// yields the text exposition format for a `/metrics` endpoint.
    #[derive(Clone)]
    pub struct PrometheusHandle {
        registry: prometheus::Registry,
        // Keeps the meter provider alive; dropping it stops collection.
        _provider: Arc<SdkMeterProvider>,
    }

    impl PrometheusHandle {
        /// Renders the Prometheus text exposition format for the current metrics.
        pub fn render(&self) -> String {
            let mut buf = Vec::new();
            let encoder = TextEncoder::new();
            let families = self.registry.gather();
            let _ = encoder.encode(&families, &mut buf);
            String::from_utf8(buf).unwrap_or_default()
        }

        /// Content-type for the exposition format, derived from the encoder.
        pub fn content_type(&self) -> String {
            TextEncoder::new().format_type().to_string()
        }
    }

    static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();

    /// Installs the global Prometheus meter provider exactly once and returns a
    /// cloneable handle. `buckets` carries the histogram boundaries resolved
    /// from config. Subsequent calls ignore `buckets` and clone the existing
    /// handle. Must run before the first metric is recorded.
    pub fn init_prometheus(buckets: HistogramBuckets) -> PrometheusHandle {
        HANDLE.get_or_init(|| build_handle(buckets)).clone()
    }

    /// Returns the handle installed by [`init_prometheus`], if any. Lets the
    /// HTTP server wire up `/metrics` without threading the handle through its
    /// public signature (metrics are process-global by design).
    pub fn installed_handle() -> Option<PrometheusHandle> {
        HANDLE.get().cloned()
    }

    fn build_handle(buckets: HistogramBuckets) -> PrometheusHandle {
        let registry = prometheus::Registry::new();
        let reader = opentelemetry_prometheus::exporter()
            .with_registry(registry.clone())
            .build()
            .expect("prometheus exporter should build");

        let provider = SdkMeterProvider::builder()
            .with_reader(reader)
            .with_view(move |inst: &Instrument| {
                let boundaries = match inst.name() {
                    "bits.job.duration.seconds" | "bits.route_handle.job.duration.seconds" => {
                        buckets.duration.clone()
                    }
                    "bits.dispatcher.queue_wait.seconds" => buckets.queue_wait.clone(),
                    _ => return None,
                };
                Stream::builder()
                    .with_aggregation(Aggregation::ExplicitBucketHistogram {
                        boundaries,
                        record_min_max: false,
                    })
                    .build()
                    .ok()
            })
            .build();

        opentelemetry::global::set_meter_provider(provider.clone());

        PrometheusHandle {
            registry,
            _provider: Arc::new(provider),
        }
    }
}

#[cfg(feature = "metrics-prometheus")]
pub use prometheus_export::{PrometheusHandle, init_prometheus, installed_handle};

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
                    content_type: None,
                    content_length: None,
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

    #[test]
    fn default_buckets_are_strictly_increasing() {
        let b = HistogramBuckets::default();
        assert!(b.duration.windows(2).all(|w| w[0] < w[1]));
        assert!(b.queue_wait.windows(2).all(|w| w[0] < w[1]));
    }
}

#[cfg(all(test, feature = "metrics-prometheus"))]
mod prometheus_tests {
    use opentelemetry::metrics::MeterProvider;
    use opentelemetry_sdk::metrics::SdkMeterProvider;
    use prometheus::{Encoder, TextEncoder};

    // Builds a LOCAL provider + registry (never the global path) so rendered
    // metric names can be asserted deterministically without process-global
    // OnceLock/global-provider interference.
    #[test]
    fn rendered_counter_names_have_single_total_suffix() {
        let registry = prometheus::Registry::new();
        let reader = opentelemetry_prometheus::exporter()
            .with_registry(registry.clone())
            .build()
            .expect("exporter builds");
        let provider = SdkMeterProvider::builder().with_reader(reader).build();
        let meter = provider.meter("bits");

        meter.u64_counter("bits.jobs.accepted").build().add(1, &[]);
        meter
            .f64_histogram("bits.job.duration.seconds")
            .build()
            .record(0.5, &[]);

        let mut buf = Vec::new();
        TextEncoder::new()
            .encode(&registry.gather(), &mut buf)
            .expect("encode");
        let rendered = String::from_utf8(buf).expect("utf8");

        assert!(
            rendered.contains("bits_jobs_accepted_total"),
            "expected single-total counter name, got:\n{rendered}"
        );
        assert!(
            !rendered.contains("_total_total"),
            "counter must not be double-suffixed, got:\n{rendered}"
        );
        assert!(
            rendered.contains("bits_job_duration_seconds_bucket"),
            "expected histogram bucket series, got:\n{rendered}"
        );
    }
}
