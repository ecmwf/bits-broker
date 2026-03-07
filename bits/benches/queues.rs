use std::sync::Arc;

use bits::job::Job;
use bits::queue::{CostWeightedQueue, FifoQueue, Queue};
use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};

fn job() -> Job {
    Job::new(serde_json::json!({}))
}

fn job_with_cost(cost: u64) -> Job {
    let mut j = Job::new(serde_json::json!({}));
    j.metadata["cost"] = serde_json::json!(cost);
    j
}

// ---------------------------------------------------------------------------
// Single uncontended acquire
//
// Both queues have ample capacity so every acquire is immediately admitted.
// This isolates the raw overhead of the acquire path itself.
// ---------------------------------------------------------------------------

fn bench_single_uncontended(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let fifo = FifoQueue::new(1024);
    let cost = rt.block_on(async { CostWeightedQueue::new(1024) });

    let mut group = c.benchmark_group("single_uncontended");
    group.throughput(Throughput::Elements(1));

    group.bench_function("fifo", |b| {
        b.to_async(&rt).iter(|| async {
            let j = job();
            let permit = fifo.acquire(&j).await.unwrap();
            drop(permit);
        });
    });

    group.bench_function("cost_weighted", |b| {
        b.to_async(&rt).iter(|| async {
            let j = job();
            let permit = cost.acquire(&j).await.unwrap();
            drop(permit);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Sequential drain through concurrency=1
//
// One slot, jobs acquired and released one at a time. Measures the minimum
// cycle time of each queue — how fast can it turn over a single permit.
// ---------------------------------------------------------------------------

fn bench_sequential_drain(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let fifo = FifoQueue::new(1);
    let cost = rt.block_on(async { CostWeightedQueue::new(1) });

    let mut group = c.benchmark_group("sequential_drain");
    group.throughput(Throughput::Elements(1));

    group.bench_function("fifo", |b| {
        b.to_async(&rt).iter(|| async {
            let j = job();
            let permit = fifo.acquire(&j).await.unwrap();
            drop(permit);
        });
    });

    group.bench_function("cost_weighted", |b| {
        b.to_async(&rt).iter(|| async {
            let j = job();
            let permit = cost.acquire(&j).await.unwrap();
            drop(permit);
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Concurrent fan-out (N tasks, N slots)
//
// N tasks all acquire simultaneously into a queue sized exactly N, so none
// block. Measures how each queue handles parallel acquire pressure.
// ---------------------------------------------------------------------------

fn bench_concurrent_fan_out(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("concurrent_fan_out");

    for &n in &[4u32, 16, 32, 64, 128] {
        let fifo = Arc::new(FifoQueue::new(n as usize));
        let cost = Arc::new(rt.block_on(async { CostWeightedQueue::new(n as usize) }));

        group.throughput(Throughput::Elements(n as u64));

        group.bench_with_input(BenchmarkId::new("fifo", n), &n, |b, &n| {
            let fifo = fifo.clone();
            b.to_async(&rt).iter(|| async {
                let futs: Vec<_> = (0..n)
                    .map(|_| {
                        let fifo = fifo.clone();
                        async move {
                            let j = job();
                            let permit = fifo.acquire(&j).await.unwrap();
                            drop(permit);
                        }
                    })
                    .collect();
                futures::future::join_all(futs).await
            });
        });

        group.bench_with_input(BenchmarkId::new("cost_weighted", n), &n, |b, &n| {
            let cost = cost.clone();
            b.to_async(&rt).iter(|| async {
                let futs: Vec<_> = (0..n)
                    .map(|i| {
                        let cost = cost.clone();
                        async move {
                            let j = job_with_cost(i as u64);
                            let permit = cost.acquire(&j).await.unwrap();
                            drop(permit);
                        }
                    })
                    .collect();
                futures::future::join_all(futs).await
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Contended fan-in (N tasks, 1 slot)
//
// N tasks compete for a single slot. Each must wait for the previous to
// finish. Measures scheduling overhead and worker throughput under
// head-of-line conditions.
// ---------------------------------------------------------------------------

fn bench_contended_fan_in(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("contended_fan_in");

    for &n in &[4u32, 16, 32] {
        group.throughput(Throughput::Elements(n as u64));

        group.bench_with_input(BenchmarkId::new("fifo", n), &n, |b, &n| {
            b.to_async(&rt).iter(|| async move {
                let fifo = Arc::new(FifoQueue::new(1));
                let futs: Vec<_> = (0..n)
                    .map(|_| {
                        let fifo = fifo.clone();
                        tokio::spawn(async move {
                            let j = job();
                            let permit = fifo.acquire(&j).await.unwrap();
                            drop(permit);
                        })
                    })
                    .collect();
                futures::future::join_all(futs).await
            });
        });

        group.bench_with_input(BenchmarkId::new("cost_weighted", n), &n, |b, &n| {
            b.to_async(&rt).iter(|| async move {
                let cost = Arc::new(CostWeightedQueue::new(1));
                let futs: Vec<_> = (0..n)
                    .map(|i| {
                        let cost = cost.clone();
                        tokio::spawn(async move {
                            let j = job_with_cost(i as u64);
                            let permit = cost.acquire(&j).await.unwrap();
                            drop(permit);
                        })
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
    bench_single_uncontended,
    bench_sequential_drain,
    bench_concurrent_fan_out,
    bench_contended_fan_in
);
criterion_main!(benches);
