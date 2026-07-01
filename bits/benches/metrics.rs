//! Benchmarks for bits with the Prometheus metrics exporter active.
//!
//! Run with:
//!   cargo bench --bench metrics --features metrics-prometheus
//!
//! Compare `latency` and `throughput` numbers against `cargo bench --bench pipelines`
//! (identical pipeline shapes, no provider installed) to measure the per-job OTel
//! recording overhead. The `render` group measures the cost of generating the text
//! exposition for a `/metrics` scrape.

use std::sync::{Arc, OnceLock};

use async_trait::async_trait;
use bits::actions::{
    ActionError, CheckAction, CheckResult, TargetAction, TargetResult, TransformAction,
    TransformResult,
};
use bits::job::Job;
use bits::metrics::{HistogramBuckets, PrometheusHandle, init_prometheus};
use bits::result::JobResult;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// No-op actions — re-registered here since this is a separate bench binary.
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
struct NoopCheck;

#[async_trait]
impl CheckAction for NoopCheck {
    async fn evaluate(&self, _job: &Job) -> Result<CheckResult, ActionError> {
        Ok(CheckResult::Pass)
    }
}

bits::register_action!(check, "noop_check", NoopCheck);

#[derive(Debug, Serialize, Deserialize)]
struct NoopTransform;

#[async_trait]
impl TransformAction for NoopTransform {
    async fn execute(&self, _job: &mut Job) -> Result<TransformResult, ActionError> {
        Ok(TransformResult::Continue)
    }
}

bits::register_action!(transform, "noop_transform", NoopTransform);

#[derive(Debug, Serialize, Deserialize)]
struct NoopTarget;

#[async_trait]
impl TargetAction for NoopTarget {
    async fn dispatch(&self, _job: &Job) -> Result<TargetResult, ActionError> {
        Ok(TargetResult::Complete(JobResult::Redirect {
            location: String::new(),
            message: String::new(),
        }))
    }
}

bits::register_action!(target, "noop", NoopTarget);

// ---------------------------------------------------------------------------
// Provider setup — installed exactly once for this bench binary.
// ---------------------------------------------------------------------------

fn installed_handle() -> &'static PrometheusHandle {
    static HANDLE: OnceLock<PrometheusHandle> = OnceLock::new();
    HANDLE.get_or_init(|| init_prometheus(HistogramBuckets::default()))
}

// ---------------------------------------------------------------------------
// Pipeline configurations — identical to pipelines.rs for direct comparison.
// ---------------------------------------------------------------------------

fn config_target_only() -> &'static str {
    r#"
bits:
  site: tst
  env: dev
routes:
  - default:
      - target::noop: ~
"#
}

fn config_check_target() -> &'static str {
    r#"
bits:
  site: tst
  env: dev
routes:
  - default:
      - check::noop_check: ~
      - target::noop: ~
"#
}

fn config_transform_target() -> &'static str {
    r#"
bits:
  site: tst
  env: dev
routes:
  - default:
      - transform::noop_transform: ~
      - target::noop: ~
"#
}

fn config_full_pipeline() -> &'static str {
    r#"
bits:
  site: tst
  env: dev
routes:
  - default:
      - check::noop_check: ~
      - transform::noop_transform: ~
      - target::noop: ~
"#
}

fn config_deep_pipeline() -> &'static str {
    r#"
bits:
  site: tst
  env: dev
routes:
  - default:
      - check::noop_check: ~
      - check::noop_check: ~
      - check::noop_check: ~
      - transform::noop_transform: ~
      - transform::noop_transform: ~
      - transform::noop_transform: ~
      - target::noop: ~
"#
}

type BenchConfig = (&'static str, fn() -> &'static str);

const CONFIGS: &[BenchConfig] = &[
    ("target_only", config_target_only as fn() -> &'static str),
    ("check_target", config_check_target),
    ("transform_target", config_transform_target),
    ("full_pipeline", config_full_pipeline),
    ("deep_pipeline", config_deep_pipeline),
];

const CONCURRENCY_LEVELS: &[u32] = &[4, 16, 32, 64, 128];

// ---------------------------------------------------------------------------
// Latency — single submit+poll round trip.
// Compare to `pipelines/latency/submit_poll/*` to isolate recording overhead.
// ---------------------------------------------------------------------------

fn bench_latency(c: &mut Criterion) {
    let _ = installed_handle();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("latency");
    group.throughput(Throughput::Elements(1));

    for &(name, config_fn) in CONFIGS {
        let bits = bits::Bits::from_config(config_fn()).unwrap();
        group.bench_function(BenchmarkId::new("submit_poll", name), |b| {
            b.to_async(&rt).iter(|| async {
                let handle = bits
                    .submit(Job::new(serde_json::json!({})))
                    .expect_accepted("submit should not be rejected");
                bits.poll(&handle.id, None).await
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Throughput — N concurrent submit+poll pairs.
// Compare to `pipelines/throughput/*` to isolate recording overhead.
// ---------------------------------------------------------------------------

fn bench_throughput(c: &mut Criterion) {
    let _ = installed_handle();
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("throughput");

    for &(name, config_fn) in CONFIGS {
        let bits = Arc::new(bits::Bits::from_config(config_fn()).unwrap());
        for &n in CONCURRENCY_LEVELS {
            group.throughput(Throughput::Elements(n as u64));
            group.bench_with_input(BenchmarkId::new(name, n), &n, |b, &n| {
                let bits = bits.clone();
                b.to_async(&rt).iter(|| async {
                    let futs: Vec<_> = (0..n)
                        .map(|_| {
                            let bits = bits.clone();
                            async move {
                                let handle = bits
                                    .submit(Job::new(serde_json::json!({})))
                                    .expect_accepted("submit should not be rejected");
                                bits.poll(&handle.id, None).await
                            }
                        })
                        .collect();
                    futures::future::join_all(futs).await
                });
            });
        }
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Render — cost of generating the Prometheus text exposition.
//
// Warmup pass ensures all OTel series are lazily created before measurement.
// Render cost depends on series count (fixed by label cardinality), not on
// the number of samples recorded.
// ---------------------------------------------------------------------------

fn bench_render(c: &mut Criterion) {
    let handle = installed_handle();
    let rt = tokio::runtime::Runtime::new().unwrap();

    {
        let bits = Arc::new(bits::Bits::from_config(config_full_pipeline()).unwrap());
        rt.block_on(async {
            let futs: Vec<_> = (0..1_000)
                .map(|_| {
                    let bits = bits.clone();
                    async move {
                        let h = bits
                            .submit(Job::new(serde_json::json!({})))
                            .expect_accepted("warmup submit");
                        bits.poll(&h.id, None).await;
                    }
                })
                .collect();
            futures::future::join_all(futs).await;
        });
    }

    let mut group = c.benchmark_group("render");
    group.bench_function("text_exposition", |b| b.iter(|| handle.render()));
    group.finish();
}

criterion_group!(benches, bench_latency, bench_throughput, bench_render);
criterion_main!(benches);
