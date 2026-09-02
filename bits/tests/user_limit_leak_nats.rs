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
