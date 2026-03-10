use std::sync::Arc;

use bits::job::Job;
use bits::dispatcher::{CostWeightedQueue, FifoQueue, Queue};
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
// Single uncontended enqueue + dequeue
//
// Both queues are empty before each iteration; every enqueue is immediately
// followed by a dequeue. Measures the raw round-trip overhead.
// ---------------------------------------------------------------------------

fn bench_single_roundtrip(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let fifo = FifoQueue::new();
    let cost = CostWeightedQueue::new();

    let mut group = c.benchmark_group("single_roundtrip");
    group.throughput(Throughput::Elements(1));

    group.bench_function("fifo", |b| {
        b.to_async(&rt).iter(|| async {
            fifo.enqueue(job());
            let _ = fifo.dequeue().await;
        });
    });

    group.bench_function("cost_weighted", |b| {
        b.to_async(&rt).iter(|| async {
            cost.enqueue(job());
            let _ = cost.dequeue().await;
        });
    });

    group.finish();
}

// ---------------------------------------------------------------------------
// Sequential drain (N pre-loaded items)
//
// Enqueue N items, then dequeue all N sequentially. Measures dequeue
// throughput once the queue is already populated.
// ---------------------------------------------------------------------------

fn bench_sequential_drain(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("sequential_drain");

    for &n in &[1u32, 16, 64] {
        group.throughput(Throughput::Elements(n as u64));

        group.bench_with_input(BenchmarkId::new("fifo", n), &n, |b, &n| {
            b.to_async(&rt).iter(|| async move {
                let q = FifoQueue::new();
                for _ in 0..n { q.enqueue(job()); }
                for _ in 0..n { let _ = q.dequeue().await; }
            });
        });

        group.bench_with_input(BenchmarkId::new("cost_weighted", n), &n, |b, &n| {
            b.to_async(&rt).iter(|| async move {
                let q = CostWeightedQueue::new();
                for i in 0..n { q.enqueue(job_with_cost(i as u64)); }
                // Give the worker a moment to ingest all items before draining.
                tokio::time::sleep(std::time::Duration::from_micros(100)).await;
                for _ in 0..n { let _ = q.dequeue().await; }
            });
        });
    }

    group.finish();
}

// ---------------------------------------------------------------------------
// Concurrent fan-out (N producers, N consumers)
//
// N tasks each enqueue one item; N tasks each dequeue one item in parallel.
// Measures how each queue handles parallel pressure.
// ---------------------------------------------------------------------------

fn bench_concurrent_fan_out(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("concurrent_fan_out");

    for &n in &[4u32, 16, 32, 64] {
        let fifo = Arc::new(FifoQueue::new());
        let cost = Arc::new(CostWeightedQueue::new());

        group.throughput(Throughput::Elements(n as u64));

        group.bench_with_input(BenchmarkId::new("fifo", n), &n, |b, &n| {
            let fifo = fifo.clone();
            b.to_async(&rt).iter(|| {
                let fifo = fifo.clone();
                async move {
                    let producers: Vec<_> = (0..n).map(|_| {
                        let q = fifo.clone();
                        async move { q.enqueue(job()); }
                    }).collect();
                    let consumers: Vec<_> = (0..n).map(|_| {
                        let q = fifo.clone();
                        async move { let _ = q.dequeue().await; }
                    }).collect();
                    futures::future::join_all(producers).await;
                    futures::future::join_all(consumers).await;
                }
            });
        });

        group.bench_with_input(BenchmarkId::new("cost_weighted", n), &n, |b, &n| {
            let cost = cost.clone();
            b.to_async(&rt).iter(|| {
                let cost = cost.clone();
                async move {
                    let producers: Vec<_> = (0..n).map(|i| {
                        let q = cost.clone();
                        async move { q.enqueue(job_with_cost(i as u64)); }
                    }).collect();
                    let consumers: Vec<_> = (0..n).map(|_| {
                        let q = cost.clone();
                        async move { let _ = q.dequeue().await; }
                    }).collect();
                    futures::future::join_all(producers).await;
                    futures::future::join_all(consumers).await;
                }
            });
        });
    }

    group.finish();
}

criterion_group!(benches, bench_single_roundtrip, bench_sequential_drain, bench_concurrent_fan_out);
criterion_main!(benches);
