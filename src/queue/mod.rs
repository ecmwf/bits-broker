// Queue implementation — pending
//
// A Queue wraps any action and provides:
//   - Bounded concurrency (capacity)
//   - Optional internal worker pool (workers: Some(n))
//   - Pull routes: capacity without workers (workers: None)
//
// Queue is represented as Action::Queue { capacity, workers, action } in the
// action pipeline and handled by the routing switch. Runtime implementation
// (the shared channel, semaphore, and worker tasks) is pending.
