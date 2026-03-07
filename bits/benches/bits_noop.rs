use async_trait::async_trait;
use bits::actions::{ActionError, TargetAction, TargetResult};
use bits::job::Job;
use bits::result::JobResult;
use bits::Bits;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use serde::{Deserialize, Serialize};

// ---------------------------------------------------------------------------
// No-op target — identical to the one in benches/http.rs, registered under
// the same name so both benches can share the same config strings.
// ---------------------------------------------------------------------------

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
// Helpers
// ---------------------------------------------------------------------------

fn make_bits() -> Bits {
    Bits::from_config(
        r#"
routes:
  default:
    - target::noop: ~
"#,
    )
    .unwrap()
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

/// Cost of a single submit+poll round-trip through the routing + action pipeline,
/// with no HTTP, no network, and no queue — pure dispatch overhead.
fn bench_sequential(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let bits = make_bits();

    let mut group = c.benchmark_group("sequential");
    group.throughput(Throughput::Elements(1));

    group.bench_function("submit_poll", |b| {
        b.to_async(&rt).iter(|| async {
            let handle = bits.submit(Job::new(serde_json::json!({})));
            bits.poll(&handle.id, None).await
        });
    });

    group.finish();
}

/// Same but with N jobs dispatched concurrently in the same tokio tick,
/// showing how the routing layer scales under parallel load.
fn bench_concurrent(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let bits = std::sync::Arc::new(make_bits());

    let mut group = c.benchmark_group("concurrent");

    for &n in &[4u32, 16, 32, 64, 128] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
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

    group.finish();
}

criterion_group!(benches, bench_sequential, bench_concurrent);
criterion_main!(benches);
