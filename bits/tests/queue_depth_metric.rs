// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

//! Regression test: `bits.dispatcher.queue_depth` must return to zero when a
//! job is cancelled while still queued.
//!
//! The gauge is a process-global OTel instrument, so this lives in its own
//! integration-test binary (own process) with a single test, and installs the
//! meter provider before any metric is recorded.

mod common;

use std::sync::Arc;
use std::time::Duration;

use bits::{Bits, Job, JobResult, PollOutcome};
use opentelemetry::global;
use opentelemetry_sdk::metrics::data::{AggregatedMetrics, MetricData};
use opentelemetry_sdk::metrics::{InMemoryMetricExporter, PeriodicReader, SdkMeterProvider};

const QUEUE_DEPTH: &str = "bits.dispatcher.queue_depth";

struct Probe {
    provider: SdkMeterProvider,
    exporter: InMemoryMetricExporter,
}

impl Probe {
    fn install() -> Self {
        let exporter = InMemoryMetricExporter::default();
        let reader = PeriodicReader::builder(exporter.clone()).build();
        let provider = SdkMeterProvider::builder().with_reader(reader).build();
        global::set_meter_provider(provider.clone());
        Self { provider, exporter }
    }

    /// Current cumulative value of the queue-depth gauge (0 if never recorded).
    fn queue_depth(&self) -> i64 {
        self.exporter.reset();
        self.provider.force_flush().expect("flush should succeed");
        let finished = self.exporter.get_finished_metrics().unwrap();
        let mut depth = 0;
        for resource_metrics in &finished {
            for scope in resource_metrics.scope_metrics() {
                for metric in scope.metrics() {
                    if metric.name() != QUEUE_DEPTH {
                        continue;
                    }
                    let AggregatedMetrics::I64(MetricData::Sum(sum)) = metric.data() else {
                        panic!("{QUEUE_DEPTH} should be an i64 sum");
                    };
                    depth += sum.data_points().map(|dp| dp.value()).sum::<i64>();
                }
            }
        }
        depth
    }

    async fn wait_for_depth(&self, expected: i64) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while self.queue_depth() != expected {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap_or_else(|_| {
            panic!(
                "{QUEUE_DEPTH} never reached {expected} (last seen {})",
                self.queue_depth()
            )
        });
    }
}

#[tokio::test]
async fn queue_depth_returns_to_zero_after_queued_job_is_cancelled() {
    let probe = Probe::install();
    let _ = common::TargetDummyDelay::new(0);

    // async_pool concurrency 1 — the first job occupies the only worker, so the second
    // stays queued until it is cancelled.
    let config = r#"
bits:
  site: tst
  env: dev
routes:
  - default:
      - target::dummy_dispatch:
          duration_ms: 500
        dispatcher:
          queue: fifo
          executor:
            type: async_pool
            concurrency: 1
"#;
    let bits = Arc::new(Bits::from_config(config).unwrap());

    let running = bits
        .submit(Job::new(serde_json::json!({})))
        .expect_accepted("first submit should be accepted");
    // Running job has been dequeued by the executor.
    probe.wait_for_depth(0).await;
    tokio::time::sleep(Duration::from_millis(50)).await;

    let queued = bits
        .submit(Job::new(serde_json::json!({})))
        .expect_accepted("second submit should be accepted");
    probe.wait_for_depth(1).await;

    assert!(bits.cancel(&queued.id));
    let outcome = bits.poll(&queued.id, Some(Duration::from_secs(2))).await;
    assert!(matches!(outcome, PollOutcome::Ready(JobResult::Cancelled)));

    let outcome = bits.poll(&running.id, Some(Duration::from_secs(5))).await;
    assert!(matches!(outcome, PollOutcome::Ready(_)));

    // Let the executor drain the tombstoned queue entry too, then check the
    // gauge reflects an empty queue.
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        probe.queue_depth(),
        0,
        "{QUEUE_DEPTH} leaked after a queued job was cancelled"
    );
}
