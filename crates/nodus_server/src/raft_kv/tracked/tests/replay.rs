use super::*;
use nodus_raftstore::txn_resolution as journal;

pub(super) fn defer_resolutions(router: &RaftRouter, selected: Option<Uuid>) {
    router.set_authority_hook_for_test(Some(Arc::new(move |cmd| {
        if let ShardCommand::TxnParticipantV1(cmd) = cmd {
            if matches!(cmd.operation, participant::OperationV1::Resolve { .. })
                && selected.is_none_or(|id| id == cmd.txn_id)
            {
                anyhow::bail!("participant temporarily unavailable");
            }
        }
        Ok(())
    })));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn automatic_terminal_replay_continues_past_failures_and_protects_live_owners() {
    let dir = tempfile::tempdir().unwrap();
    let c = Cluster::open(dir.path()).await;
    let engine = c.engines[0].clone();
    let failed = TxnId::new();
    let healthy = TxnId::new();
    let live = TxnId::new();
    defer_resolutions(&engine.router, None);
    let e = engine.clone();
    let tables = c.tables;
    tokio::task::spawn_blocking(move || {
        for txn in [failed, healthy, live] {
            for table in tables {
                e.write_intent(
                    txn,
                    Bytes::from(format!("{table}:{txn:?}")),
                    Bytes::from_static(b"row"),
                )
                .unwrap();
            }
            if txn != live {
                assert!(e.commit(txn, 1_000_000).is_err());
                assert!(matches!(
                    e.authority_read(txn).unwrap().unwrap().phase,
                    authority::PhaseV1::Committed { .. }
                ));
            }
        }
    })
    .await
    .unwrap();
    defer_resolutions(&engine.router, Some(failed.0));
    // A broken legacy record still reports an error, but cannot prevent a
    // tracked terminal decision from making independently verifiable progress.
    let corrupt = TxnId::new();
    engine
        .local
        .write_intent(
            corrupt,
            Bytes::from_static(b"\0txn2pc\0broken"),
            Bytes::from_static(b"invalid"),
        )
        .unwrap();
    engine.local.commit(corrupt, 2_000_000).unwrap();
    let raft = engine.manager.get(META_SHARD).await.unwrap();
    let (stop, stopped) = tokio::sync::watch::channel(());
    let recovery = tokio::spawn(crate::raft_kv::pending_txn_recovery_loop(
        raft,
        engine.clone(),
        1,
        Duration::from_millis(50),
        stopped,
    ));
    tokio::time::timeout(Duration::from_secs(10), async {
        while !journal::progress(engine.local.as_ref(), healthy.0)
            .unwrap()
            .unwrap()
            .complete
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        !journal::progress(engine.local.as_ref(), failed.0)
            .unwrap()
            .unwrap()
            .complete
    );
    let active = engine.authority_read_for_test(live).await;
    assert_eq!(active.phase, authority::PhaseV1::Active);
    assert_eq!(active.token.generation, 1);
    assert!(
        journal::progress(engine.local.as_ref(), live.0)
            .unwrap()
            .is_none()
    );
    for (index, table) in c.tables.iter().enumerate() {
        let store = engine.engine_for(&c.groups[index]);
        for (txn, visible) in [(failed, false), (healthy, true), (live, false)] {
            assert_eq!(
                store
                    .get(format!("{table}:{txn:?}").as_bytes(), u64::MAX)
                    .unwrap(),
                visible.then(|| Bytes::from_static(b"row"))
            );
        }
    }
    engine.router.set_authority_hook_for_test(None);
    tokio::time::timeout(Duration::from_secs(10), async {
        while !journal::progress(engine.local.as_ref(), failed.0)
            .unwrap()
            .unwrap()
            .complete
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    stop.send(()).unwrap();
    recovery.await.unwrap();
    assert_eq!(engine.authority_read_for_test(live).await, active);
    drop(engine);
    c.shutdown().await;
}
