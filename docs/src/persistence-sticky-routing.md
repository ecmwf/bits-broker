# Sticky Routing

BITS assigns every job an ID that encodes the owning broker:

```
{broker_id}-{instance_uuid}~{job_uuid}
```

For example: `api-broker-a1b2c3d4~f7e6d5c4-b3a2-...`

The broker prefix is everything before the first `~`. Any broker that receives a poll request
can determine the intended owner by splitting the job ID on `~`, without consulting any routing
table or database.

## Why sticky ingress matters

On poll, a broker first checks its **local in-memory state**. If the job is found there, the
result is returned immediately — no database access, no network call. If the job is not found
locally but the parsed owner prefix names a different broker, the request must be proxied or
the job must be recovered.

Configuring your load balancer to apply session affinity (for example, consistent hashing on
the `Authorization` header) ensures that most polls land on the owning broker and take the fast
path. Without affinity, every poll may incur an internal proxy call.

## Internal proxy

When a poll misses locally and the job ID names another broker, BITS looks up that broker's
registered endpoint from the [broker lease table](persistence-broker-leases.md) and proxies the
poll directly — without involving the client. The client receives the same response it would
have received had it contacted the owner directly.

Proxy failure (network error, timeout) while the owner's lease is still active returns
`Pending` to the client. BITS does **not** attempt to claim the job while a valid lease exists.

See [Poll Proxying and Recovery](persistence-poll-recovery.md) for the full decision sequence.

## Configuration

Set `broker_id` in your config to a stable, human-readable string. Each process instance
appends its own UUID at startup, so replicas of the same service share a common `broker_id`
prefix but have distinct per-process identities:

```yaml
bits:
  broker_id: api-broker             # stable prefix; instance ID becomes api-broker-{uuid}
  internal_poll_base_url: "http://bits-0.bits-headless.default.svc.cluster.local:8080/job"
```

`internal_poll_base_url` is the URL at which **other brokers** can reach this instance's poll
endpoint. It must be reachable from all peer brokers. Each job ID appended to this base URL
forms the proxy target: `{internal_poll_base_url}/{job_id}`.
