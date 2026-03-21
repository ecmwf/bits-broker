use async_trait::async_trait;
use bits::Bits;
use bits::actions::{
    ActionError, CheckAction, CheckResult, TargetAction, TargetResult, TransformAction,
    TransformResult,
};
use bits::job::Job;
use bits::result::JobResult;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// No-op actions — zero-cost implementations for pipeline overhead measurement
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
// Pipeline configurations
// ---------------------------------------------------------------------------

fn make_bits(config: &str) -> Bits {
    Bits::from_config(config).unwrap()
}

/// Baseline: just a target, nothing else.
fn config_target_only() -> &'static str {
    r#"
routes:
  - default:
      - target::noop: ~
"#
}

fn config_check_target() -> &'static str {
    r#"
routes:
  - default:
      - check::noop_check: ~
      - target::noop: ~
"#
}

fn config_transform_target() -> &'static str {
    r#"
routes:
  - default:
      - transform::noop_transform: ~
      - target::noop: ~
"#
}

fn config_full_pipeline() -> &'static str {
    r#"
routes:
  - default:
      - check::noop_check: ~
      - transform::noop_transform: ~
      - target::noop: ~
"#
}

fn config_deep_pipeline() -> &'static str {
    r#"
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

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

type BenchConfig = (&'static str, fn() -> &'static str);

const CONFIGS: &[BenchConfig] = &[
    ("target_only", config_target_only as fn() -> &'static str),
    ("check_target", config_check_target),
    ("transform_target", config_transform_target),
    ("full_pipeline", config_full_pipeline),
    ("deep_pipeline", config_deep_pipeline),
];

const CONCURRENCY_LEVELS: &[u32] = &[4, 16, 32, 64, 128];

/// Latency: single submit+poll round-trip for each pipeline configuration.
fn bench_latency(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("latency");
    group.throughput(Throughput::Elements(1));

    for &(name, config_fn) in CONFIGS {
        let bits = make_bits(config_fn());
        group.bench_function(BenchmarkId::new("submit_poll", name), |b| {
            b.to_async(&rt).iter(|| async {
                let handle = bits.submit(Job::new(serde_json::json!({})));
                bits.poll(&handle.id, None).await
            });
        });
    }

    group.finish();
}

fn large_request() -> serde_json::Value {
    serde_json::json!({
        "class": "od", "type": "fc", "stream": "oper", "expver": "0001",
        "date": "2024-01-15/to/2024-01-20", "time": "00:00:00/06:00:00/12:00:00/18:00:00",
        "step": "0/6/12/18/24/30/36/42/48", "levtype": "pl",
        "levelist": "1/2/3/5/7/10/20/30/50/70/100/150/200/250/300/400/500/600/700/800/850/900/925/950/1000",
        "param": "129/130/131/132/133/135/138/155/157/203/246/247/248",
        "area": "90/-180/-90/180", "grid": "0.25/0.25",
        "user": {"name": "alice", "group": "research", "project": "climate-2024"},
        "metadata": {"priority": 5, "cost_estimate": 1200, "source": "web-api"}
    })
}

fn bench_latency_large(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("latency_large");
    group.throughput(Throughput::Elements(1));

    for &(name, config_fn) in CONFIGS {
        let bits = make_bits(config_fn());
        group.bench_function(BenchmarkId::new("submit_poll", name), |b| {
            b.to_async(&rt).iter(|| async {
                let handle = bits.submit(Job::new(large_request()));
                bits.poll(&handle.id, None).await
            });
        });
    }

    group.finish();
}

fn bench_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("throughput");

    for &(name, config_fn) in CONFIGS {
        let bits = std::sync::Arc::new(make_bits(config_fn()));

        for &n in CONCURRENCY_LEVELS {
            group.throughput(Throughput::Elements(n as u64));
            group.bench_with_input(BenchmarkId::new(name, n), &n, |b, &n| {
                let bits = bits.clone();
                b.to_async(&rt).iter(|| async {
                    let futs: Vec<_> = (0..n)
                        .map(|_| {
                            let bits = bits.clone();
                            async move {
                                let handle = bits.submit(Job::new(serde_json::json!({})));
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

fn bench_throughput_large(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("throughput_large");

    let bits = std::sync::Arc::new(make_bits(config_deep_pipeline()));

    for &n in &[4u32, 32, 128] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::new("deep_pipeline", n), &n, |b, &n| {
            let bits = bits.clone();
            b.to_async(&rt).iter(|| async {
                let futs: Vec<_> = (0..n)
                    .map(|_| {
                        let bits = bits.clone();
                        async move {
                            let handle = bits.submit(Job::new(large_request()));
                            bits.poll(&handle.id, None).await
                        }
                    })
                    .collect();
                futures::future::join_all(futs).await
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_latency,
    bench_latency_large,
    bench_throughput,
    bench_throughput_large
);
criterion_main!(benches);
