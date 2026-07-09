<!--
SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)

SPDX-License-Identifier: Apache-2.0
-->

# Sticky Routing

BITS request IDs are opaque public strings. Internally, BITS can decode a request ID to obtain a site, environment, and broker slot. Those fields form an owner hint, not a user-facing routing contract.

## Why sticky ingress matters

On poll, a broker first checks its local in-memory state. If the job is found, the result is returned immediately with no persistence lookup and no peer call.

Configure your load balancer to apply session affinity using a stable client property such as the `Authorization` header, client IP, or tenant header. This makes most poll requests return through the local fast path.

Without affinity, a poll may land on a non-owner broker. That broker decodes the request ID for an owner hint, checks broker leases, and either proxies the poll or starts recovery.

## Internal proxy

When a poll misses locally, BITS derives the hinted broker ID as `{site}-{env}-{slot}` and looks up that broker's lease. If the lease is active, BITS proxies the poll to the lease's `internal_poll_base_url` and returns the translated result to the client.

Proxy failure while the hinted owner's lease is still active returns `Pending`. BITS does not claim the job while a valid lease exists.

If the hinted owner's lease is missing or expired, BITS reads the durable job record. The record's `broker_id` is authoritative: after a previous recovery it may name a different owner than the request ID hint. See [Poll Proxying and Recovery](persistence-poll-recovery.md).

## Configuration

Set compact deployment tags and a broker-to-broker poll endpoint:

```yaml
bits:
  site: bol
  env: dev
  internal_poll_endpoint: "http://bits-0.bits-headless.default.svc.cluster.local:8080/job"
```

`site` and `env` must be 1-3 lowercase letters or digits. `internal_poll_endpoint` is the base URL that peer brokers use for proxied polls; appending `/{job_id}` forms the target URL.
