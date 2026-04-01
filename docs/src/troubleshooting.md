# Troubleshooting

This guide covers common issues you may encounter when operating BITS,
along with diagnostic steps and solutions.

---

## Job stuck in Pending

A job that remains in `Pending` state longer than expected indicates the
request is queued but not yet processed.

### Possible causes

**Target is slow or unresponsive**

The target action is taking longer than the client poll timeout. BITS returns
`Pending` to the client, but work continues in the background.

- Check target health and response times
- Increase `server.poll_timeout_secs` if the target is legitimately slow
- Add metrics/logging at the target to confirm requests arrive

**Dispatcher queue is full**

When a dispatcher's queue reaches capacity, new jobs wait until slots open.

- Increase queue depth or add more target capacity
- Check for backed-up jobs with the same target across multiple routes

**Concurrency limit reached**

The dispatcher's `concurrency` setting caps simultaneous executions.

```yaml
targets:
  backend:
    type: http
    url: "http://my-service/api"
    dispatcher:
      executor:
        type: async_pool
        concurrency: 8   # increase if target can handle more
```

**All routes rejected the job**

When every route in a switch rejects a job (for example, via a check failure),
the job returns a rejection error to the client (not a stalled Pending).

- Review logs for rejection reasons from non-silent checks
- Verify that at least one route in each switch can accept the job

---

## Job returns NotFound

A `NotFound` response means the broker cannot locate the job for the given ID.

`NotFound` primarily indicates a malformed ID or a job that has already been
consumed.

### Possible causes

**Job ID is malformed**

If the job ID cannot be parsed (truncated, from a different cluster, or never
issued by this deployment), the broker returns `NotFound`.

- Verify the job ID comes from a successful submit response
- Ensure you are querying the correct environment

**Job ID is from a different broker**

Job IDs encode the owning broker. When you poll the wrong broker:

- If the owner is alive but proxying fails, the poll returns `Pending`.
- If the owner is gone and there is no durable record, the poll returns
  `410 Gone` (`JobLost`).

`NotFound` is not returned for these wrong-broker cases.

**Job was already consumed by another poll**

Jobs are single-consumer. Once a poll receives the result, subsequent polls
return `NotFound`.

- Check client retry logic to avoid duplicate polls after success

**Sweeper cleaned up the job**

Jobs have a limited reconnect window (`bits.reconnect_buffer_secs`, default 5 seconds). If the client
disconnects for longer than the deadline, the sweeper marks the job for
cleanup.

- Implement reliable polling with shorter intervals
- Handle `NotFound` as a terminal state and resubmit if needed

---

## Job returns Gone (410)

A `410 Gone` response indicates the job existed but is no longer available
in a recoverable state.

### Possible causes

**Job was explicitly cancelled**

The client or an admin operation cancelled the job before it completed.

- Check application logs for cancel operations
- Review any automated cancellation policies

**Client disconnected and reconnect window expired**

After the reconnect deadline passes, the job is considered abandoned. Further
polls return `Gone`.

- Ensure clients poll reliably within the deadline window
- Consider increasing the deadline only if the architecture requires it

**Owner broker disappeared without durable state**

The broker that owned the job crashed before persisting it (the job completed
before `persist_after_secs`). Another broker detects the expired lease but
finds no durable record to recover from. The job is permanently lost.

- This only affects jobs that complete faster than `persist_after_secs`
- Lower `persist_after_secs` to reduce the window of vulnerability
- See [Persistence](persistence.md) for how the threshold works

---

## Config errors at startup

BITS uses a typed error system with stable codes. Startup failures include
the error code and a descriptive message.

### `CONFIG_YAML_SYNTAX` - Invalid YAML

The configuration file has a parse error.

```
Error: config: YAML syntax error: missing colon at line 12, column 5
Code: CONFIG_YAML_SYNTAX
```

**Solution**: Validate YAML syntax with `yamllint` or an online parser.
Common issues include:
- Missing colons after keys
- Incorrect indentation
- Unclosed quotes

### `CONFIG_VALIDATION` - Field value invalid

A field value fails validation constraints.

```
Error: config: bits.persistence.broker_lease_ttl_secs: must be > 0
Code: CONFIG_VALIDATION
```

**Solution**: Check the field constraints. Common validations:
- TTL values must be positive
- Port numbers must be in range 1-65535
- URLs must be valid

### `CONFIG_MISSING_FIELD` - Required field absent

A required configuration field is not present.

```
Error: config: routes: missing required field
Code: CONFIG_MISSING_FIELD
```

**Solution**: Add the missing field. Note that `routes` can be omitted when
using programmatic route registration via `Bits::add_route()`.

### `CONFIG_FEATURE_DISABLED` - Feature flag not enabled

A persistence backend requires a Cargo feature that was not compiled in.

```
Error: config: bits.persistence: feature 'tikv' not enabled
Code: CONFIG_FEATURE_DISABLED
```

**Solution**: Rebuild with the required feature:

```bash
cargo build --features tikv
cargo build --features nats
```

### `CONFIG_DECODE` - Action config deserialization failed

An action's configuration could not be parsed into its expected structure.

```
Error: config: targets.backend: failed to decode http target: missing field `url`
Code: CONFIG_DECODE
```

**Solution**: Check the action's required fields. Each action type has
specific configuration requirements documented in [Custom Actions](custom-actions.md).

### `CONFIG_PERSISTENCE_INIT` - Cannot connect to storage backend

The persistence backend (NATS or TiKV) connection failed during startup.

```
Error: config: bits.persistence: nats initialization failed: connection refused
Code: CONFIG_PERSISTENCE_INIT
```

**Solution**:
- Verify the backend is running and accessible
- Check network connectivity and firewall rules
- Confirm the URL/endpoint configuration is correct

### `ROUTING_INVALID_ACTION` - Unknown action type

A route references an action type that does not exist.

```
Error: routing: route 'default' action 'transform::unknown': unknown action type 'metkit_unknown'
Code: ROUTING_INVALID_ACTION
```

**Solution**: Check for typos in the `type:` field. Available action types
are listed in [Configuration](configuration.md).

### `ROUTING_MISSING_TARGET` - Route does not end with a target

Every route must terminate with a target action or a switch. A route ending
with only checks or transforms is invalid.

```
Error: routing: route 'incomplete': must end with a target or switch
Code: ROUTING_MISSING_TARGET
```

**Solution**: Add a target step at the end of the route:

```yaml
routes:
  default:
    - check::is_valid
    - target::backend   # route must end with a target or switch
```

---

## Persistence issues

### NATS connection refused

The broker cannot connect to the NATS server.

**Diagnostic**: Check the error message for the connection URL.

**Solutions**:
- Verify NATS server is running: `nats-server -js`
- Confirm JetStream is enabled (`-js` flag)
- Check network connectivity between broker and NATS
- Review NATS authentication credentials if configured

### TiKV timeout

Connection to the TiKV cluster times out.

**Diagnostic**: Look for `PERSISTENCE_BACKEND` errors with timeout messages.

**Solutions**:
- Verify PD endpoint is correct and reachable
- Check TiKV cluster health with `pd-ctl`
- Ensure sufficient network bandwidth between broker and TiKV nodes
- Review TiKV logs for storage or replication issues

### Broker lease expired while jobs running

A broker crashed or lost network connectivity. Its lease expired, and peers
attempted recovery.

**Behavior**:
- Jobs are reclaimed by other brokers and re-executed from the start
- In-flight results from the dead broker are lost
- Clients polling the old broker may see `Pending` until recovery completes

**Solutions**:
- Monitor broker health and restart crashed instances
- Reduce `broker_lease_ttl_secs` for faster recovery (increases heartbeat writes)
- Ensure persistent jobs are idempotent where possible

---

## Worker issues (remote pool)

### Heartbeat timeout

A remote worker claimed a job but stopped sending heartbeats.

**Behavior**: BITS evicts the job and returns a failure to the client.

**Solutions**:
- Increase `heartbeat_timeout_secs` if workers have long processing times
- Ensure workers send heartbeats at regular intervals (recommended: half the
timeout period)
- Check worker logs for crashes or blocking operations

### Worker disconnect

The worker closed its connection or crashed mid-processing.

**Behavior**: Job fails when the heartbeat timeout expires.

**Solutions**:
- Implement reliable worker supervision (systemd, Kubernetes, etc.)
- Design jobs to be safely retryable
- Use persistence so jobs can be reclaimed by other workers

### Malformed completion

A worker submitted an invalid completion payload.

**Behavior**: BITS rejects the completion with `400 Bad Request`.

**Solutions**:
- Validate completion JSON before sending
- Check that the `job_id` in the completion URL matches the claimed job

---

## Performance issues

### High latency

Jobs take longer than expected to complete.

**Diagnostic steps**:
1. Check dispatcher queue depth per target
2. Review target response times independently
3. Verify no resource contention (CPU, memory, network)

**Solutions**:
- Increase `concurrency` for the bottleneck target
- Add more target instances behind a load balancer
- Use `cost_weighted` queueing to prioritize smaller jobs

### Queue depth growing

The dispatcher queue accumulates jobs faster than they complete.

**Solutions**:
- Scale out target capacity horizontally
- Add circuit breakers to shed load during overload
- Review for slow or stuck jobs blocking the queue

### Too many concurrent jobs

Resource exhaustion from unbounded concurrency.

**Solutions**:
- Set explicit `concurrency` limits on all dispatchers
- Use separate dispatchers for different SLA classes
- Monitor memory usage per in-flight job

---

## Multi-broker issues

### Proxy failures

A broker cannot proxy a poll to the job's owner broker.

**Behavior**: The polling broker returns `Pending` rather than claiming the
job, because the owner's lease is still valid.

**Causes**:
- Owner broker crashed but lease has not expired yet
- Network partition between brokers
- Owner broker's `internal_poll_endpoint` is misconfigured

**Solutions**:
- Verify `internal_poll_endpoint` is reachable from peer brokers
- Reduce `broker_lease_ttl_secs` for faster failover
- Monitor inter-broker network health

### Split brain

Two brokers both believe they own the same job.

**Behavior**: Claim conflicts occur (reported as `PERSISTENCE_CONFLICT`).
BITS resolves this automatically, but clients may see transient errors.

**Solutions**:
- Ensure broker clocks are synchronized (NTP)
- Review lease TTL settings for your use case
- Monitor claim conflict rates as a health indicator

### Stale leases

A dead broker's lease remains visible until it expires.

**Behavior**: Peers waste time proxying to an unreachable broker.

**Solutions**:
- Tune `broker_lease_ttl_secs` based on your crash-recovery requirements
- Consider faster detection with lower TTL (trade-off: more heartbeat writes)
- Monitor proxy failure rates to detect stale lease issues

---

## Getting help

If issues persist after following this guide:

1. Enable debug logging (`RUST_LOG=bits=debug`)
2. Collect error codes and messages from logs
3. Capture the configuration file (redact sensitive values)
4. Document the expected vs actual behavior

Report issues with the error code, broker version, and reproduction steps.
