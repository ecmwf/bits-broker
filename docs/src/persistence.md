<!--
SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)

SPDX-License-Identifier: Apache-2.0
-->

# Persistence

BITS persistence is designed around two goals:

1. **Avoid overhead for short jobs.** Most jobs complete quickly. Writing every job to a database
   would add latency and storage cost for no benefit.
2. **Make long jobs recoverable.** Jobs that are still in-flight when a broker restarts or fails
   should be recoverable by another broker, without losing the client's view of the job.

## How it works

When `bits.persist_after_secs` is set, BITS uses a threshold approach:

- A job starts entirely in-memory, immediately.
- If the job is still in-flight when the threshold elapses, BITS writes a single durable record
  to the persistence store.
- When the job completes (success or failure), the durable record is deleted.

Jobs that finish before the threshold never touch the database. Jobs that cross the threshold are
written exactly once.

## Reclaim is strictly lease-gated

BITS does not use per-job heartbeats. Instead, each broker maintains a **broker lease**, a
record in durable storage that proves the broker is alive and advertises its internal endpoint.

A non-owner broker may only claim and recover a persistent job if the owner's broker lease is
**missing or expired**. A proxy failure while the owner lease is still valid does not trigger
reclaim. This prevents split-brain recovery and avoids double-execution.

## What is covered in this section

- [Concepts](persistence-concepts.md) - data model, owner-aware job IDs, and the store abstraction.
- [Sticky Routing](persistence-sticky-routing.md) - why sticky ingress matters and how BITS handles
  off-owner polls.
- [Poll Proxying and Recovery](persistence-poll-recovery.md) - the full decision sequence a broker
  follows when a poll arrives.
- [Broker Leases](persistence-broker-leases.md) - how brokers register themselves and how lease
  expiry enables recovery.
- [Operational Notes](persistence-operations.md) - tuning guidance for production deployments.
