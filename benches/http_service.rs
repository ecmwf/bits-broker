use std::sync::{Arc, OnceLock};
use std::time::Duration;

use async_trait::async_trait;
use bits::actions::{ActionError, TargetAction, TargetResult};
use bits::job::Job;
use bits::queue::{CostWeightedQueue, Queue};
use bits::result::JobResult;
use bits::Bits;
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;

// ---------------------------------------------------------------------------
// Inline dummy target (mirrors tests/common/target_dummy_delay.rs)
// benches/ can't use tests/common/, so we re-register it here.
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize, Deserialize)]
struct BenchTarget {
    duration_ms: u64,
    concurrency: usize,
    #[serde(skip)]
    queue: OnceLock<Arc<CostWeightedQueue>>,
}

impl BenchTarget {
    fn new(duration_ms: u64, concurrency: usize) -> Self {
        Self { duration_ms, concurrency, queue: OnceLock::new() }
    }

    fn queue(&self) -> &Arc<CostWeightedQueue> {
        self.queue.get_or_init(|| Arc::new(CostWeightedQueue::new(self.concurrency)))
    }
}

#[async_trait]
impl TargetAction for BenchTarget {
    async fn dispatch(&self, job: &Job) -> Result<TargetResult, ActionError> {
        let _permit = self.queue().acquire(job).await?;
        tokio::time::sleep(Duration::from_millis(self.duration_ms)).await;
        Ok(TargetResult::Complete(JobResult::Redirect {
            location: String::new(),
            message: format!("bench dispatch complete for job {}", job.id),
        }))
    }
}

bits::register_action!(target, "dummy_dispatch", BenchTarget);

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

async fn free_port() -> u16 {
    TcpListener::bind("127.0.0.1:0")
        .await
        .unwrap()
        .local_addr()
        .unwrap()
        .port()
}

async fn start_server(duration_ms: u64, concurrency: usize) -> (u16, reqwest::Client) {
    let _ = BenchTarget::new(0, 1); // ensure inventory entry is linked

    let port = free_port().await;
    let config = format!(
        r#"
server:
  type: http
  bind: "127.0.0.1:{port}"
routes:
  default:
    - target::dummy_dispatch:
        duration_ms: {duration_ms}
        concurrency: {concurrency}
"#
    );

    tokio::spawn(async move {
        Bits::from_config(&config).unwrap().serve().await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(50)).await;

    let client = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .unwrap();

    (port, client)
}

// ---------------------------------------------------------------------------
// Benchmarks
// ---------------------------------------------------------------------------

/// Throughput of sequential job submissions where the job completes within
/// a single poll window (fast path — no redirect/re-poll needed).
fn bench_sequential(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (port, client) = rt.block_on(start_server(0, 16));

    let mut group = c.benchmark_group("sequential");
    group.throughput(Throughput::Elements(1));

    group.bench_function("post_job", |b| {
        b.to_async(&rt).iter(|| async {
            client
                .post(format!("http://127.0.0.1:{port}/job"))
                .json(&serde_json::json!({}))
                .send()
                .await
                .unwrap()
        });
    });

    group.finish();
}

/// Throughput of N concurrent job submissions, varying the fan-out width.
/// All jobs run instantly (duration_ms=0) with enough concurrency, so the
/// bottleneck is the HTTP + routing stack, not the target itself.
fn bench_concurrent(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let (port, client) = rt.block_on(start_server(0, 64));
    let client = Arc::new(client);

    let mut group = c.benchmark_group("concurrent");

    for &n in &[4u32, 16, 32] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::from_parameter(n), &n, |b, &n| {
            let client = client.clone();
            b.to_async(&rt).iter(|| async {
                let futs: Vec<_> = (0..n)
                    .map(|_| {
                        client
                            .post(format!("http://127.0.0.1:{port}/job"))
                            .json(&serde_json::json!({}))
                            .send()
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
