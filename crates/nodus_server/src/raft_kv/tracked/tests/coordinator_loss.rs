use super::*;
use nodus_raftstore::txn_resolution as journal;
use replay::defer_resolutions;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A decided-but-incomplete cross-shard commit whose coordinator was the meta
/// leader is recovered automatically once a NEW meta leader takes over: the
/// survivor's leadership-gain trigger — not a manual `recover_pending_txns`
/// call, and not a loop on a node that already led meta — re-drives the
/// decision. Until recovery actually runs, the decided rows stay invisible.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn new_meta_leader_recovers_decided_commit_after_coordinator_loss() {
    let dir = tempfile::tempdir().unwrap();
    let mut c = Cluster::open_coordinator_loss(dir.path()).await;
    let txn = TxnId::new();
    defer_resolutions(&c.engines[0].router, Some(txn.0));
    c.write_pair(txn).await;
    let e = c.engines[0].clone();
    let commit = tokio::task::spawn_blocking(move || e.commit(txn, 1_000_000))
        .await
        .unwrap();
    assert!(commit.is_err(), "deferred resolutions fail the commit");
    assert_eq!(
        c.engines[0].authority_read_for_test(txn).await.phase,
        authority::PhaseV1::Committed {
            commit_ts: 1_000_000
        }
    );
    // The decision and its resolution journal replicated to both survivors.
    for engine in &c.engines[1..] {
        let local = engine.local.clone();
        tokio::time::timeout(Duration::from_secs(10), async move {
            while journal::progress(local.as_ref(), txn.0).unwrap().is_none() {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
    }
    // Gate recovery on both survivors and run the production recovery loop on
    // each, wired exactly as the server wires it; only the node that wins the
    // meta election after the coordinator dies will fire it.
    let attempts = Arc::new(AtomicUsize::new(0));
    let mut loops = Vec::new();
    for engine in &c.engines[1..] {
        let gated = attempts.clone();
        engine.set_recovery_gate_for_test(Some(Arc::new(move || {
            gated.fetch_add(1, Ordering::Relaxed);
            Err(anyhow::anyhow!("recovery paused for test"))
        })));
        let meta = engine.manager.get(META_SHARD).await.unwrap();
        let (stop, stopped) = tokio::sync::watch::channel(());
        let handle = tokio::spawn(crate::raft_kv::pending_txn_recovery_loop(
            meta,
            engine.clone(),
            engine.manager.node_id(),
            Duration::from_millis(50),
            stopped,
        ));
        loops.push((stop, handle));
    }
    // The coordinator and meta leader dies; both data groups keep quorum.
    c.stop_node(0).await;
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            for engine in &c.engines[1..] {
                let meta = engine.manager.get(META_SHARD).await.unwrap();
                if meta.metrics().borrow().current_leader == Some(engine.manager.node_id()) {
                    return;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("a survivor must win the meta election");
    // The new leader's loop fired and was held by the gate...
    tokio::time::timeout(Duration::from_secs(10), async {
        while attempts.load(Ordering::Relaxed) == 0 {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("the new meta leader must attempt recovery");
    // ...so the decided rows remain invisible until recovery actually runs.
    let keys = c.keys();
    assert_eq!(
        c.engines[1]
            .engine_for(&c.groups[0])
            .get(&keys[0], u64::MAX)
            .unwrap(),
        None
    );
    assert_eq!(
        c.engines[2]
            .engine_for(&c.groups[1])
            .get(&keys[1], u64::MAX)
            .unwrap(),
        None
    );
    // Release the gates: the running loop's next pass completes the commit.
    for engine in &c.engines[1..] {
        engine.set_recovery_gate_for_test(None);
    }
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            let first = c.engines[1]
                .engine_for(&c.groups[0])
                .get(&keys[0], u64::MAX)
                .unwrap();
            let second = c.engines[2]
                .engine_for(&c.groups[1])
                .get(&keys[1], u64::MAX)
                .unwrap();
            if first == Some(Bytes::from_static(b"atomic"))
                && second == Some(Bytes::from_static(b"atomic"))
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .expect("recovery must make the decided rows visible");
    tokio::time::timeout(Duration::from_secs(10), async {
        while !journal::progress(c.engines[1].local.as_ref(), txn.0)
            .unwrap()
            .unwrap()
            .complete
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("a survivor must journal the completed resolution");
    for (engine, group) in [(&c.engines[1], &c.groups[0]), (&c.engines[2], &c.groups[1])] {
        assert_eq!(
            participant::read(engine.engine_for(group).as_ref(), txn.0)
                .unwrap()
                .unwrap()
                .phase,
            participant::PhaseV1::Committed {
                commit_ts: 1_000_000
            }
        );
    }
    for (stop, handle) in loops {
        stop.send(()).unwrap();
        handle.await.unwrap();
    }
    c.shutdown().await;
}
