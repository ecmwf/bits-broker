#![cfg(feature = "nats")]
// SPDX-FileCopyrightText: 2026 European Centre for Medium-Range Weather Forecasts (ECMWF)
//
// SPDX-License-Identifier: Apache-2.0

//! Regression test for a `bits-leases-userlimits` KV bucket-bloat incident
//! (2026-08-21).
//!
//! Root cause: `list_user_slots_nats` and `reclaim_user_slots_nats`
//! enumerated the **entire** bucket via `store.keys()` plus one extra
//! `store.entry()` round trip per key, filtering client-side for the target
//! user's prefix, instead of a single server-side-filtered query.
//!
//! At scale (hundreds of thousands of unrelated entries) every strict-mode
//! admission check — which calls `list_user_slots` on the hot request path —
//! became pathologically expensive: draining `keys()` one key at a time with
//! a synchronous RPC in between is slow enough relative to how fast
//! JetStream pushes a `LastPerSubject` snapshot that the client's `Ordered`
//! consumer wrapper kept detecting a sequence gap (local buffer overflow)
//! and resubscribing — a self-healing but extremely slow retry storm that
//! flooded logs with `slow consumers` and stalled the dispatcher queue.
//!
//! These tests reproduce the mechanism deterministically at small scale by
//! shrinking the client's per-subscription buffer
//! (`NatsStore::with_subscription_capacity`) instead of needing hundreds of
//! thousands of real entries. The (entries, capacity) pair below was picked
//! empirically for a wide margin: the unfixed scan reliably took ~20-25s,
//! the fixed subject-filtered version ~2-3ms.

mod common;

use std::sync::Arc;
use std::time::Duration;

use bits::db::PersistenceStore;
use bits::db::UserLimitStore;
use bits::db::nats::NatsStore;
use common::recovery::ensure_nats_server;

const SCOPE: &str = "leak-repro";

/// Deliberately small so a few hundred/thousand unrelated bucket entries
/// reproduce the same client-side "Ordered consumer gap detected,
/// resubscribing" retry storm that production hit with hundreds of
/// thousands of entries and the crate's real default (65,536).
const SMALL_SUBSCRIPTION_CAPACITY: usize = 64;

/// Comfortably larger than [`SMALL_SUBSCRIPTION_CAPACITY`] — enough unrelated
/// noise to reliably overflow the buffer and trigger the retry storm on the
/// unfixed scan pattern, with a wide margin (empirically: the unfixed
/// `list_user_slots_nats` took ~20-25s here across repeated runs; the fixed
/// version took ~2-3ms).
const NOISE_ENTRIES: usize = 1000;

fn small_capacity_store() -> Arc<dyn PersistenceStore> {
    let url = ensure_nats_server();
    let unique = uuid::Uuid::new_v4();
    Arc::new(
        NatsStore::new(
            url,
            format!("leak-jobs-{unique}"),
            format!("leak-leases-{unique}"),
            Duration::from_secs(30),
            1,
            Duration::from_secs(10),
        )
        .with_subscription_capacity(SMALL_SUBSCRIPTION_CAPACITY),
    )
}

/// Populate the bucket with `n` entries unrelated to the user/broker under
/// test — standing in for the leaked, never-released slots. Reserved
/// concurrently so seeding stays fast regardless of `n`.
async fn seed_unrelated_noise(store: &Arc<dyn PersistenceStore>, n: usize, owner: &str) {
    const BATCH: usize = 50;
    for chunk_start in (0..n).step_by(BATCH) {
        let chunk_end = (chunk_start + BATCH).min(n);
        let mut handles = Vec::new();
        for i in chunk_start..chunk_end {
            let store = Arc::clone(store);
            let owner = owner.to_string();
            handles.push(tokio::spawn(async move {
                store
                    .reserve_user_slot(
                        SCOPE,
                        &format!("noise-user-{i}"),
                        &format!("noise-job-{i}"),
                        &owner,
                    )
                    .await
                    .expect("reserve noise slot");
            }));
        }
        for h in handles {
            h.await.expect("seed task panicked");
        }
    }
}

/// A full-bucket scan (`list_user_slots`) must find the target user's own
/// entry quickly, regardless of how much unrelated bucket noise exists.
///
/// Before the fix (client-side `keys()` + per-key `entry()`) the scan is
/// O(bucket size) with N+1 sequential RPCs sharing one fixed buffer with the
/// noise, reliably tripping the retry storm (~20-25s at [`NOISE_ENTRIES`]).
/// After the fix (single server-side subject-filtered stream) the server
/// only sends this user's own key prefix, so bucket size is irrelevant and
/// the call takes low milliseconds.
#[tokio::test]
async fn list_user_slots_is_fast_despite_bucket_noise() {
    let store = small_capacity_store();

    seed_unrelated_noise(&store, NOISE_ENTRIES, "noise-broker").await;

    store
        .reserve_user_slot(SCOPE, "alice", "job-alice-1", "broker-a")
        .await
        .expect("reserve alice's own slot");

    // Comfortably above the fixed implementation's measured ~2-3ms and
    // comfortably below the unfixed implementation's measured ~20-25s at
    // this (entries, capacity) configuration — see the module doc comment.
    let bound = Duration::from_secs(10);
    let result = tokio::time::timeout(bound, store.list_user_slots(SCOPE, "alice")).await;
    let entries = match result {
        Ok(res) => res.expect("list_user_slots must not error"),
        Err(_) => panic!(
            "list_user_slots(alice) did not complete within {bound:?} despite alice having \
             only one entry — {NOISE_ENTRIES} unrelated bucket entries should not make a \
             single user's scan this slow (O(bucket size) scan pattern regression)"
        ),
    };

    assert_eq!(
        entries.len(),
        1,
        "alice must have exactly her own entry, found {entries:?}"
    );
    assert_eq!(entries[0].job_id, "job-alice-1");
}

/// Companion test for the periodic reclaim sweep. Unlike `list_user_slots`,
/// reclaim needs to see the *whole* bucket (any entry owned by any dead
/// broker), so it cannot be subject-filtered and remains O(bucket size) by
/// design. The fix here is only doing it as a single streaming RPC
/// (`watch_with_history`) instead of `keys()` + one `entry()` per key —
/// fewer round trips, but not reliably fast on a large bucket. That residual
/// risk is why `bits::runtime::maintenance` bounds the reclaim call with a
/// timeout (`RECLAIM_TIMEOUT`).
///
/// So this test only asserts *correctness* under bucket noise (the one
/// reclaimable entry is still found and removed), within a generous
/// safety-net bound, not a performance bound.
#[tokio::test]
async fn reclaim_is_correct_despite_bucket_noise() {
    let store = small_capacity_store();

    // A smaller noise count than the `list_user_slots` test above: this test
    // only needs *some* noise to prove correctness isn't lost to the retry
    // storm, not the wide fast-vs-slow margin.
    const RECLAIM_NOISE_ENTRIES: usize = 200;
    seed_unrelated_noise(&store, RECLAIM_NOISE_ENTRIES, "noise-broker").await;
    store
        .upsert_broker_lease("noise-broker", "http://noise:8080", Duration::from_secs(60))
        .await
        .expect("lease for the noise broker (keeps noise entries live)");

    store
        .reserve_user_slot(SCOPE, "carol", "job-dead", "dead-broker")
        .await
        .expect("reserve carol's dead-broker slot");
    // No lease for "dead-broker": reclaim should remove this one entry.

    // Safety net against a genuine hang, not a speed claim. Normal completion
    // at this noise level is well under a second.
    let bound = Duration::from_secs(30);
    let result = tokio::time::timeout(bound, store.reclaim_user_slots()).await;
    let removed = match result {
        Ok(res) => res.expect("reclaim_user_slots must not error"),
        Err(_) => panic!("reclaim_user_slots did not complete within {bound:?} (hung)"),
    };

    assert_eq!(
        removed, 1,
        "reclaim must find and remove exactly the one dead-broker entry \
         despite {RECLAIM_NOISE_ENTRIES} unrelated live-broker entries in the bucket"
    );

    let remaining = store
        .list_user_slots(SCOPE, "carol")
        .await
        .expect("list carol's slots after reclaim");
    assert!(
        remaining.is_empty(),
        "carol's dead-broker entry should have been reclaimed, found {remaining:?}"
    );
}

/// Regression test for `nats-io/nats.rs#1376` ("KV::watch_with_history stop
/// if store is empty"): upstream deliberately changed `watch_with_history`
/// so it no longer terminates on its own when a pattern matches zero current
/// keys — it just keeps waiting, forever, for a future write that may never
/// come. That's the documented, correct behaviour for an open-ended watch,
/// but `scan_current_entries` (backing both `list_user_slots_nats` and
/// `reclaim_user_slots_nats`) used to depend on the *old*, since-reverted
/// behaviour to know when a scan was done. Confirmed directly against a real
/// `async-nats 0.50.0` client (vs. the pinned 0.38.0): with the old
/// implementation this hangs indefinitely instead of returning empty.
///
/// Note these two tests do *not* fail against the pinned 0.38.0 even with
/// the fix reverted — 0.38.0 still has the old (pre-#1376-fix) short-circuit
/// baked in, so it happens to return fast today regardless. They exist to
/// pin the desired behaviour explicitly and catch a *future* dependency bump
/// silently reintroducing the hang, rather than as a red-then-green
/// regression test against the currently pinned version.
///
/// `list_user_slots` for a user with zero entries must return fast and
/// empty regardless of how much unrelated noise exists elsewhere in the
/// bucket (mirroring the strict-mode admission path, which can legitimately
/// query a user who has never reserved a slot).
#[tokio::test]
async fn list_user_slots_for_a_user_with_no_entries_returns_fast_and_empty() {
    let store = small_capacity_store();

    seed_unrelated_noise(&store, NOISE_ENTRIES, "noise-broker").await;

    // "dave" never reserved anything: this pattern matches zero current keys.
    let bound = Duration::from_secs(10);
    let result = tokio::time::timeout(bound, store.list_user_slots(SCOPE, "dave")).await;
    let entries = match result {
        Ok(res) => res.expect("list_user_slots must not error"),
        Err(_) => panic!(
            "list_user_slots(dave) did not complete within {bound:?} — a pattern \
             matching zero current keys must not hang (nats-io/nats.rs#1376)"
        ),
    };

    assert!(
        entries.is_empty(),
        "dave has no reservations, expected no entries, found {entries:?}"
    );
}

/// Companion test for the whole-bucket reclaim sweep against a genuinely
/// empty bucket — exactly the state `bits-leases-userlimits` is in right
/// after a manual purge in production. The reclaim pattern (`ul.>`) matches
/// zero current keys here, the same shape of query that hung indefinitely
/// against a client without the zero-match probe fix.
#[tokio::test]
async fn reclaim_on_a_genuinely_empty_bucket_returns_fast_and_zero() {
    let store = small_capacity_store();

    let bound = Duration::from_secs(10);
    let result = tokio::time::timeout(bound, store.reclaim_user_slots()).await;
    let removed = match result {
        Ok(res) => res.expect("reclaim_user_slots must not error"),
        Err(_) => panic!(
            "reclaim_user_slots did not complete within {bound:?} on an empty bucket — \
             a pattern matching zero current keys must not hang (nats-io/nats.rs#1376)"
        ),
    };

    assert_eq!(
        removed, 0,
        "nothing to reclaim in a freshly created, empty bucket"
    );
}

/// Regression test for the `bits-leases-userlimits` bucket-bloat root cause
/// itself (not the scan-storm symptom above): `release_user_slot_nats`'s
/// `purge()` call left a permanent tombstone message in the underlying
/// JetStream stream, because the bucket had no `max_age`/TTL — unlike the
/// sibling `bits-leases` bucket, which does. Every correctly-completed job
/// left one message behind forever; growth was proportional to total
/// historical throughput, not to any leak or error rate. Confirmed directly
/// against lumi-prod's real bucket: 278 of 292 current stream messages were
/// already-deleted `KV-Operation: PURGE` tombstones from successful
/// releases, not stuck live entries.
///
/// Fixed via per-message TTL (`purge_with_ttl`, `kv::Config.limit_markers`)
/// so a tombstone self-expires instead of living forever — verified here
/// against the *raw* stream message count (`debug_user_limits_stream_message_count`),
/// which is the only way to observe a tombstone's continued physical
/// presence; the KV-level API (`list_user_slots`) correctly reports a
/// released slot as gone either way and can't tell the two cases apart.
#[tokio::test]
async fn released_slot_tombstone_self_expires_instead_of_accumulating_forever() {
    let url = ensure_nats_server();
    let unique = uuid::Uuid::new_v4();
    let store = NatsStore::new(
        url,
        format!("leak-jobs-{unique}"),
        format!("leak-leases-{unique}"),
        Duration::from_secs(30),
        1,
        Duration::from_secs(10),
    )
    .with_user_limit_tombstone_ttl(Duration::from_secs(1));

    UserLimitStore::reserve_user_slot(&store, SCOPE, "erin", "job-erin-1", "broker-a")
        .await
        .expect("reserve erin's slot");
    assert_eq!(
        store
            .debug_user_limits_stream_message_count()
            .await
            .unwrap(),
        1,
        "one live reservation message expected before release"
    );

    UserLimitStore::release_user_slot(&store, SCOPE, "erin", "job-erin-1")
        .await
        .expect("release erin's slot");

    // Immediately after release: the KV layer correctly reports it gone...
    assert!(
        store
            .list_user_slots(SCOPE, "erin")
            .await
            .unwrap()
            .is_empty()
    );
    // ...but the purge tombstone itself is still a real, physical message in
    // the stream at this point (this is the crux of the bug: without a TTL,
    // it would stay this way forever).
    assert_eq!(
        store
            .debug_user_limits_stream_message_count()
            .await
            .unwrap(),
        1,
        "the purge tombstone must still physically exist immediately after release"
    );

    // Wait past the (test-shortened) tombstone TTL and confirm NATS actually
    // reclaims the message physically, not just logically.
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    loop {
        let count = store
            .debug_user_limits_stream_message_count()
            .await
            .unwrap();
        if count == 0 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "tombstone was not physically reclaimed within {:?} of its 1s TTL expiring \
             (still {count} raw stream message(s)) — this is exactly the \
             bits-leases-userlimits bucket-bloat root cause",
            deadline.elapsed()
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
