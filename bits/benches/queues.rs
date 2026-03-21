use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;
use std::time::Duration;

use bits::dispatcher::{AgePriorityQueue, CostWeightedQueue, FifoQueue, Queue};
use bits::job::Job;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

fn job() -> Job {
    Job::new(serde_json::json!({}))
}

fn job_with_cost(cost: u64) -> Job {
    let mut j = Job::new(serde_json::json!({}));
    j.metadata_mut()["cost"] = serde_json::json!(cost);
    j
}

#[derive(Clone)]
struct ScheduledJob {
    id: String,
    seq: u64,
    cost: u64,
    enqueue_at: Duration,
}

#[derive(Clone)]
struct AccuracyScenario {
    name: &'static str,
    jobs: Vec<ScheduledJob>,
}

struct PopEvent {
    id: String,
    popped_at: Duration,
}

#[derive(Default, Clone, Copy)]
struct AccuracyReport {
    top1_hits: usize,
    total_pops: usize,
    rank_sum: usize,
}

impl AccuracyReport {
    fn top1_percent(self) -> f64 {
        100.0 * self.top1_hits as f64 / self.total_pops as f64
    }

    fn mean_rank(self) -> f64 {
        self.rank_sum as f64 / self.total_pops as f64
    }
}

fn scheduled_job(seq: u64, cost: u64, enqueue_at: Duration) -> ScheduledJob {
    ScheduledJob {
        id: format!("job-{seq}"),
        seq,
        cost,
        enqueue_at,
    }
}

fn accuracy_scenarios() -> Vec<AccuracyScenario> {
    let uniform_batch = AccuracyScenario {
        name: "uniform_batch_256",
        jobs: (0..256u64)
            .map(|seq| scheduled_job(seq, (seq * 37) % 64 + 1, Duration::ZERO))
            .collect(),
    };

    let bursty_staggered = AccuracyScenario {
        name: "bursty_staggered_256",
        jobs: (0..256u64)
            .map(|seq| {
                let burst = seq / 32;
                let intra = seq % 32;
                let cost = ((seq * 29 + intra * 7) % 96) + 1;
                scheduled_job(seq, cost, Duration::from_millis(burst * 2))
            })
            .collect(),
    };

    let mut adversarial_jobs = Vec::new();
    let mut seq = 0u64;
    for _ in 0..16 {
        adversarial_jobs.push(scheduled_job(seq, 256, Duration::ZERO));
        seq += 1;
    }
    for wave in 0..6u64 {
        for i in 0..32u64 {
            let cost = (i % 4) + 1;
            adversarial_jobs.push(scheduled_job(
                seq,
                cost,
                Duration::from_millis(4 + wave * 3),
            ));
            seq += 1;
        }
    }
    let adversarial_aging = AccuracyScenario {
        name: "adversarial_aging_208",
        jobs: adversarial_jobs,
    };

    vec![uniform_batch, bursty_staggered, adversarial_aging]
}

fn compare_scheduled_jobs(
    left: &ScheduledJob,
    right: &ScheduledJob,
    now: Duration,
) -> std::cmp::Ordering {
    let left_cost = integer_sqrt(left.cost.max(1) as u128);
    let right_cost = integer_sqrt(right.cost.max(1) as u128);
    let left_wait = now.saturating_sub(left.enqueue_at).as_nanos();
    let right_wait = now.saturating_sub(right.enqueue_at).as_nanos();

    let left_score = left_wait.saturating_mul(right_cost);
    let right_score = right_wait.saturating_mul(left_cost);

    match left_score.cmp(&right_score) {
        std::cmp::Ordering::Equal => right.seq.cmp(&left.seq),
        other => other,
    }
}

fn integer_sqrt(value: u128) -> u128 {
    let mut x = value;
    let mut y = x.div_ceil(2);

    while y < x {
        x = y;
        y = (x + value / x) / 2;
    }

    x.max(1)
}

fn compute_accuracy(jobs: &[ScheduledJob], pops: &[PopEvent]) -> AccuracyReport {
    let index_by_id: HashMap<&str, usize> = jobs
        .iter()
        .enumerate()
        .map(|(idx, job)| (job.id.as_str(), idx))
        .collect();
    let mut remaining = vec![true; jobs.len()];
    let mut report = AccuracyReport::default();

    for pop in pops {
        let chosen = *index_by_id.get(pop.id.as_str()).unwrap();
        let mut eligible: Vec<usize> = jobs
            .iter()
            .enumerate()
            .filter(|(idx, job)| remaining[*idx] && job.enqueue_at <= pop.popped_at)
            .map(|(idx, _)| idx)
            .collect();

        eligible.sort_by(|left, right| {
            compare_scheduled_jobs(&jobs[*left], &jobs[*right], pop.popped_at)
        });

        let chosen_pos = eligible.iter().position(|idx| *idx == chosen).unwrap();
        let rank = eligible.len() - chosen_pos;

        report.total_pops += 1;
        report.rank_sum += rank;
        report.top1_hits += usize::from(rank == 1);
        remaining[chosen] = false;
    }

    report
}

async fn run_accuracy_trial(scenario: &AccuracyScenario) -> (Duration, AccuracyReport) {
    let q = Arc::new(AgePriorityQueue::new());
    run_accuracy_trial_with_queue(q, scenario).await
}

async fn run_accuracy_trial_with_queue<Q: Queue + 'static>(
    q: Arc<Q>,
    scenario: &AccuracyScenario,
) -> (Duration, AccuracyReport) {
    let jobs = scenario.jobs.clone();
    let producer_q = Arc::clone(&q);
    let started_at = std::time::Instant::now();

    let producer = tokio::spawn(async move {
        for scheduled in &jobs {
            let elapsed = started_at.elapsed();
            if scheduled.enqueue_at > elapsed {
                tokio::time::sleep(scheduled.enqueue_at - elapsed).await;
            }

            let mut job = Job::new_with_id(scheduled.id.clone(), serde_json::json!({}));
            job.metadata_mut()["cost"] = serde_json::json!(scheduled.cost);
            producer_q.enqueue(job);
        }
    });

    let timed_at = std::time::Instant::now();
    let mut pops = Vec::with_capacity(scenario.jobs.len());
    for _ in 0..scenario.jobs.len() {
        let job = q.dequeue().await.unwrap();
        pops.push(PopEvent {
            id: job.id,
            popped_at: started_at.elapsed(),
        });
    }
    let elapsed = timed_at.elapsed();

    producer.await.unwrap();
    let report = compute_accuracy(&scenario.jobs, &pops);
    (elapsed, report)
}

// ---------------------------------------------------------------------------
// Single uncontended enqueue + dequeue
//
// Both queues are empty before each iteration; every enqueue is immediately
// followed by a dequeue. Measures the raw round-trip overhead.
// ---------------------------------------------------------------------------

fn bench_single_roundtrip(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();

    let mut group = c.benchmark_group("single_roundtrip");
    group.throughput(Throughput::Elements(1));

    group.bench_function("fifo", |b| {
        b.to_async(&rt).iter(|| async {
            let fifo = FifoQueue::new();
            fifo.enqueue(job());
            let _ = fifo.dequeue().await;
        });
    });

    group.bench_function("cost_weighted", |b| {
        b.to_async(&rt).iter(|| async {
            let cost = CostWeightedQueue::new();
            cost.enqueue(job());
            let _ = cost.dequeue().await;
        });
    });

    group.bench_function("age_priority", |b| {
        b.to_async(&rt).iter(|| async {
            let age = AgePriorityQueue::new();
            age.enqueue(job());
            let _ = age.dequeue().await;
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
                for _ in 0..n {
                    q.enqueue(job());
                }
                for _ in 0..n {
                    let _ = q.dequeue().await;
                }
            });
        });

        group.bench_with_input(BenchmarkId::new("cost_weighted", n), &n, |b, &n| {
            b.to_async(&rt).iter(|| async move {
                let q = CostWeightedQueue::new();
                for i in 0..n {
                    q.enqueue(job_with_cost(i as u64));
                }
                // Give the worker a moment to ingest all items before draining.
                tokio::time::sleep(std::time::Duration::from_micros(100)).await;
                for _ in 0..n {
                    let _ = q.dequeue().await;
                }
            });
        });

        group.bench_with_input(BenchmarkId::new("age_priority", n), &n, |b, &n| {
            b.to_async(&rt).iter(|| async move {
                let q = AgePriorityQueue::new();
                for i in 0..n {
                    q.enqueue(job_with_cost(i as u64));
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
                for _ in 0..n {
                    let _ = q.dequeue().await;
                }
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
        group.throughput(Throughput::Elements(n as u64));

        group.bench_with_input(BenchmarkId::new("fifo", n), &n, |b, &n| {
            b.to_async(&rt).iter(|| async move {
                let fifo = Arc::new(FifoQueue::new());
                let producers: Vec<_> = (0..n)
                    .map(|_| {
                        let q = fifo.clone();
                        async move {
                            q.enqueue(job());
                        }
                    })
                    .collect();
                let consumers: Vec<_> = (0..n)
                    .map(|_| {
                        let q = fifo.clone();
                        async move {
                            let _ = q.dequeue().await;
                        }
                    })
                    .collect();
                futures::future::join_all(producers).await;
                futures::future::join_all(consumers).await;
            });
        });

        group.bench_with_input(BenchmarkId::new("cost_weighted", n), &n, |b, &n| {
            b.to_async(&rt).iter(|| async move {
                let cost = Arc::new(CostWeightedQueue::new());
                let producers: Vec<_> = (0..n)
                    .map(|i| {
                        let q = cost.clone();
                        async move {
                            q.enqueue(job_with_cost(i as u64));
                        }
                    })
                    .collect();
                let consumers: Vec<_> = (0..n)
                    .map(|_| {
                        let q = cost.clone();
                        async move {
                            let _ = q.dequeue().await;
                        }
                    })
                    .collect();
                futures::future::join_all(producers).await;
                futures::future::join_all(consumers).await;
            });
        });

        group.bench_with_input(BenchmarkId::new("age_priority", n), &n, |b, &n| {
            b.to_async(&rt).iter(|| async move {
                let age = Arc::new(AgePriorityQueue::new());
                let producers: Vec<_> = (0..n)
                    .map(|i| {
                        let q = age.clone();
                        async move {
                            q.enqueue(job_with_cost(i as u64));
                        }
                    })
                    .collect();
                let consumers: Vec<_> = (0..n)
                    .map(|_| {
                        let q = age.clone();
                        async move {
                            let _ = q.dequeue().await;
                        }
                    })
                    .collect();
                futures::future::join_all(producers).await;
                futures::future::join_all(consumers).await;
            });
        });
    }

    group.finish();
}

fn bench_age_priority_max_throughput(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("age_priority_max_throughput");

    for &n in &[256u32, 1024, 4096, 16384] {
        group.throughput(Throughput::Elements(n as u64));
        group.bench_with_input(BenchmarkId::new("batch_drain", n), &n, |b, &n| {
            b.to_async(&rt).iter(|| async move {
                let q = AgePriorityQueue::new();
                for i in 0..n {
                    q.enqueue(job_with_cost((i % 64) as u64 + 1));
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
                for _ in 0..n {
                    let _ = q.dequeue().await;
                }
            });
        });
    }

    group.finish();
}

fn bench_age_priority_accuracy(c: &mut Criterion) {
    let rt = tokio::runtime::Runtime::new().unwrap();
    let mut group = c.benchmark_group("age_priority_accuracy");
    group.sample_size(10);

    for scenario in accuracy_scenarios() {
        group.throughput(Throughput::Elements(scenario.jobs.len() as u64));
        group.bench_function(scenario.name, |b| {
            let scenario = scenario.clone();
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                let mut aggregate = AccuracyReport::default();

                for _ in 0..iters {
                    let (elapsed, report) = rt.block_on(run_accuracy_trial(&scenario));
                    total += elapsed;
                    aggregate.top1_hits += report.top1_hits;
                    aggregate.total_pops += report.total_pops;
                    aggregate.rank_sum += report.rank_sum;
                }

                black_box((aggregate.top1_percent(), aggregate.mean_rank()));
                println!(
                    "accuracy {}: top1={:.2}% mean_rank={:.3} over {} trials",
                    scenario.name,
                    aggregate.top1_percent(),
                    aggregate.mean_rank(),
                    iters,
                );

                total
            });
        });
    }

    group.finish();
}

criterion_group!(
    benches,
    bench_single_roundtrip,
    bench_sequential_drain,
    bench_concurrent_fan_out,
    bench_age_priority_max_throughput,
    bench_age_priority_accuracy
);
criterion_main!(benches);
