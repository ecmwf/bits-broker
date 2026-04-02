use std::time::{Duration, Instant};

use serde::Deserialize;

use crate::actions::{ActionError, TargetResult};

const DEFAULT_FAILURE_THRESHOLD: u32 = 5;
const DEFAULT_OPEN_TIMEOUT_SECS: f64 = 30.0;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Status {
    Closed,
    Open,
    HalfOpen,
}

struct State {
    status: Status,
    consecutive_failures: u32,
    opened_at: Instant,
    generation: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CircuitBreakerConfig {
    #[serde(default = "default_failure_threshold")]
    pub failure_threshold: u32,
    #[serde(default = "default_open_timeout_secs")]
    pub open_timeout_secs: f64,
}

fn default_failure_threshold() -> u32 {
    DEFAULT_FAILURE_THRESHOLD
}

fn default_open_timeout_secs() -> f64 {
    DEFAULT_OPEN_TIMEOUT_SECS
}

impl CircuitBreakerConfig {
    pub fn validate(&self) -> Result<(), String> {
        if self.failure_threshold == 0 {
            return Err("circuit_breaker.failure_threshold must be > 0".into());
        }
        if !self.open_timeout_secs.is_finite() || self.open_timeout_secs <= 0.0 {
            return Err(
                "circuit_breaker.open_timeout_secs must be a positive finite number".into(),
            );
        }
        Duration::try_from_secs_f64(self.open_timeout_secs)
            .map_err(|_| "circuit_breaker.open_timeout_secs overflows Duration".to_string())?;
        Ok(())
    }
}

pub struct CircuitBreaker {
    state: std::sync::Mutex<State>,
    failure_threshold: u32,
    open_timeout: Duration,
    target_name: String,
}

impl CircuitBreaker {
    pub fn new(config: &CircuitBreakerConfig, target_name: String) -> Self {
        Self {
            state: std::sync::Mutex::new(State {
                status: Status::Closed,
                consecutive_failures: 0,
                opened_at: Instant::now(),
                generation: 0,
            }),
            failure_threshold: config.failure_threshold,
            open_timeout: Duration::from_secs_f64(config.open_timeout_secs),
            target_name,
        }
    }

    pub fn allow_request(&self) -> Result<u64, ActionError> {
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        match state.status {
            Status::Closed => Ok(state.generation),
            Status::Open => {
                if state.opened_at.elapsed() >= self.open_timeout {
                    state.generation += 1;
                    state.status = Status::HalfOpen;
                    tracing::warn!(
                        target_name = %self.target_name,
                        "circuit breaker half-open; allowing probe request"
                    );
                    Ok(state.generation)
                } else {
                    Err(ActionError::CircuitOpen(format!(
                        "target {} circuit open",
                        self.target_name
                    )))
                }
            }
            Status::HalfOpen => Err(ActionError::CircuitOpen(format!(
                "target {} circuit half-open; probe in flight",
                self.target_name
            ))),
        }
    }

    pub fn record_outcome(&self, generation: u64, result: &Result<TargetResult, ActionError>) {
        let signal = classify(result);
        let mut state = self.state.lock().unwrap_or_else(|p| p.into_inner());
        if generation != state.generation {
            return;
        }
        match signal {
            Signal::Success => self.do_record_success(&mut state),
            Signal::Failure => self.do_record_failure(&mut state),
            Signal::Neutral if state.status == Status::HalfOpen => {
                self.do_record_failure(&mut state);
            }
            Signal::Neutral => {}
        }
    }

    fn do_record_success(&self, state: &mut State) {
        state.consecutive_failures = 0;
        if state.status == Status::HalfOpen {
            state.generation += 1;
            state.status = Status::Closed;
            tracing::warn!(
                target_name = %self.target_name,
                "circuit breaker closed; probe succeeded"
            );
        }
    }

    fn do_record_failure(&self, state: &mut State) {
        state.consecutive_failures += 1;
        match state.status {
            Status::Closed => {
                if state.consecutive_failures >= self.failure_threshold {
                    state.generation += 1;
                    state.status = Status::Open;
                    state.opened_at = Instant::now();
                    tracing::warn!(
                        target_name = %self.target_name,
                        failures = state.consecutive_failures,
                        open_timeout_secs = self.open_timeout.as_secs_f64(),
                        "circuit breaker opened"
                    );
                }
            }
            Status::HalfOpen => {
                state.generation += 1;
                state.status = Status::Open;
                state.opened_at = Instant::now();
                tracing::warn!(
                    target_name = %self.target_name,
                    "circuit breaker re-opened; probe failed"
                );
            }
            Status::Open => {}
        }
    }
}

enum Signal {
    Success,
    Failure,
    Neutral,
}

fn classify(result: &Result<TargetResult, ActionError>) -> Signal {
    match result {
        Ok(TargetResult::Complete(_) | TargetResult::Reject { .. }) => Signal::Success,
        Err(ActionError::NetworkError(_) | ActionError::Timeout(_)) => Signal::Failure,
        _ => Signal::Neutral,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::result::JobResult;

    fn test_config(threshold: u32, timeout_secs: f64) -> CircuitBreakerConfig {
        CircuitBreakerConfig {
            failure_threshold: threshold,
            open_timeout_secs: timeout_secs,
        }
    }

    fn breaker(threshold: u32, timeout_secs: f64) -> CircuitBreaker {
        CircuitBreaker::new(&test_config(threshold, timeout_secs), "test-target".into())
    }

    fn network_error() -> Result<TargetResult, ActionError> {
        Err(ActionError::NetworkError("connection refused".into()))
    }

    fn success() -> Result<TargetResult, ActionError> {
        Ok(TargetResult::Complete(JobResult::Cancelled))
    }

    fn reject() -> Result<TargetResult, ActionError> {
        Ok(TargetResult::Reject {
            reason: "bad request".into(),
            silent: true,
        })
    }

    fn record(cb: &CircuitBreaker, result: &Result<TargetResult, ActionError>) {
        let g = cb.state.lock().unwrap().generation;
        cb.record_outcome(g, result);
    }

    #[test]
    fn closed_allows_requests() {
        let cb = breaker(3, 30.0);
        assert!(cb.allow_request().is_ok());
    }

    #[test]
    fn opens_after_threshold_failures() {
        let cb = breaker(3, 30.0);
        for _ in 0..3 {
            record(&cb, &network_error());
        }
        assert!(cb.allow_request().is_err());
    }

    #[test]
    fn success_resets_failure_count() {
        let cb = breaker(3, 30.0);
        record(&cb, &network_error());
        record(&cb, &network_error());
        record(&cb, &success());
        record(&cb, &network_error());
        record(&cb, &network_error());
        assert!(cb.allow_request().is_ok());
    }

    #[test]
    fn reject_counts_as_success() {
        let cb = breaker(3, 30.0);
        record(&cb, &network_error());
        record(&cb, &network_error());
        record(&cb, &reject());
        record(&cb, &network_error());
        record(&cb, &network_error());
        assert!(cb.allow_request().is_ok());
    }

    #[test]
    fn open_rejects_before_timeout() {
        let cb = breaker(1, 999.0);
        record(&cb, &network_error());
        assert!(cb.allow_request().is_err());
    }

    #[test]
    fn open_transitions_to_half_open_after_timeout() {
        let cb = breaker(1, 0.0);
        record(&cb, &network_error());
        std::thread::sleep(Duration::from_millis(1));
        assert!(cb.allow_request().is_ok());
    }

    #[test]
    fn half_open_rejects_concurrent_requests() {
        let cb = breaker(1, 0.0);
        record(&cb, &network_error());
        std::thread::sleep(Duration::from_millis(1));
        assert!(cb.allow_request().is_ok());
        assert!(cb.allow_request().is_err());
    }

    #[test]
    fn half_open_probe_success_closes() {
        let cb = breaker(1, 0.0);
        record(&cb, &network_error());
        std::thread::sleep(Duration::from_millis(1));
        let g = cb.allow_request().unwrap();
        cb.record_outcome(g, &success());
        assert!(cb.allow_request().is_ok());
    }

    #[test]
    fn half_open_probe_failure_reopens() {
        let cb = breaker(1, 999.0);
        let g;
        {
            let mut state = cb.state.lock().unwrap();
            state.status = Status::HalfOpen;
            g = state.generation;
        }
        cb.record_outcome(g, &network_error());
        assert!(cb.allow_request().is_err());
    }

    #[test]
    fn neutral_errors_do_not_affect_closed_state() {
        let cb = breaker(2, 30.0);
        record(&cb, &network_error());
        record(&cb, &Err(ActionError::Cancelled));
        record(&cb, &Err(ActionError::ResourceError("x".into())));
        assert!(cb.allow_request().is_ok());
    }

    #[test]
    fn stale_success_during_half_open_is_ignored() {
        let cb = breaker(1, 0.0);
        let closed_g = cb.allow_request().unwrap();
        record(&cb, &network_error());
        std::thread::sleep(Duration::from_millis(1));
        let probe_g = cb.allow_request().unwrap();
        assert_ne!(closed_g, probe_g);
        cb.record_outcome(closed_g, &success());
        assert!(
            cb.allow_request().is_err(),
            "stale success should not close the circuit"
        );
        cb.record_outcome(probe_g, &success());
        assert!(cb.allow_request().is_ok(), "probe success should close it");
    }

    #[test]
    fn stale_failure_during_half_open_is_ignored() {
        let cb = breaker(1, 0.0);
        let closed_g = cb.allow_request().unwrap();
        record(&cb, &network_error());
        std::thread::sleep(Duration::from_millis(1));
        let probe_g = cb.allow_request().unwrap();
        cb.record_outcome(closed_g, &network_error());
        cb.record_outcome(probe_g, &success());
        assert!(
            cb.allow_request().is_ok(),
            "stale failure should not reopen the circuit"
        );
    }

    #[test]
    fn neutral_during_half_open_reopens() {
        let cb = breaker(1, 999.0);
        {
            let mut state = cb.state.lock().unwrap();
            state.status = Status::HalfOpen;
        }
        let g = cb.state.lock().unwrap().generation;
        cb.record_outcome(g, &Err(ActionError::Cancelled));
        assert!(
            cb.allow_request().is_err(),
            "neutral result during half-open should reopen"
        );
    }

    #[test]
    fn config_validation() {
        assert!(test_config(0, 30.0).validate().is_err());
        assert!(test_config(5, 0.0).validate().is_err());
        assert!(test_config(5, -1.0).validate().is_err());
        assert!(test_config(5, f64::INFINITY).validate().is_err());
        assert!(test_config(5, 30.0).validate().is_ok());
    }
}
