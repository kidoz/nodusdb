use super::*;
use std::time::Duration;

mod coordinator_loss;
mod crash;
mod fixture;
mod ingress;
mod marker_gap;
mod replay;
mod sql;
mod unavailable;
use fixture::Cluster;

fn pause_before(
    router: &RaftRouter,
    commit: bool,
) -> (
    tokio::sync::oneshot::Receiver<()>,
    std::sync::mpsc::Sender<()>,
) {
    let (entered, reached) = tokio::sync::oneshot::channel();
    let (release, wait) = std::sync::mpsc::channel();
    let gate = Mutex::new(Some((entered, wait)));
    router.set_authority_hook_for_test(Some(Arc::new(move |cmd| {
        let selected = if commit {
            matches!(
                cmd,
                ShardCommand::TxnAuthorityV1(authority::CommandV1 {
                    operation: authority::OperationV1::Commit { .. },
                    ..
                })
            )
        } else {
            matches!(
                cmd,
                ShardCommand::TxnParticipantV1(participant::CommandV1 {
                    operation: participant::OperationV1::Resolve { .. },
                    ..
                })
            )
        };
        if selected {
            let pause = gate.lock().unwrap().take();
            if let Some((entered, wait)) = pause {
                let _ = entered.send(());
                wait.recv_timeout(Duration::from_secs(20))
                    .map_err(|e| anyhow::anyhow!("coordinator pause timed out: {e}"))?;
            }
        }
        Ok(())
    })));
    (reached, release)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn recovery_fences_paused_coordinator_and_all_delayed_mutations() {
    let dir = tempfile::tempdir().unwrap();
    let c = Cluster::open(dir.path()).await;
    let txn = TxnId::new();
    c.write_pair(txn).await;
    let original = c.engines[0].authority_read_for_test(txn).await;
    assert_eq!(original.participants.len(), 2);
    let (reached, release) = pause_before(&c.engines[0].router, true);
    let e = c.engines[0].clone();
    let commit = tokio::task::spawn_blocking(move || e.commit(txn, 1_000_000));
    tokio::time::timeout(Duration::from_secs(10), reached)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        authority::read(c.engines[0].local.as_ref(), txn.0)
            .unwrap()
            .unwrap()
            .phase,
        authority::PhaseV1::Preparing
    );
    for index in 0..2 {
        assert!(matches!(
            participant::read(
                c.engines[index].engine_for(&c.groups[index]).as_ref(),
                txn.0
            )
            .unwrap()
            .unwrap()
            .phase,
            participant::PhaseV1::Prepared { .. }
        ));
    }
    let e = c.engines[1].clone();
    let recovered = tokio::task::spawn_blocking(move || e.recover_tracked_transaction(txn))
        .await
        .unwrap();
    release.send(()).unwrap();
    recovered.unwrap();
    assert!(commit.await.unwrap().is_err());
    let terminal = c.engines[0].authority_read_for_test(txn).await;
    assert_eq!(terminal.phase, authority::PhaseV1::Aborted);
    assert_eq!(terminal.token.generation, 2);
    c.assert_rows(false);
    let keys = c.keys();
    let e = c.engines[0].clone();
    let group = c.groups[1].clone();
    tokio::task::spawn_blocking(move || {
        e.router.set_authority_hook_for_test(None);
        assert!(
            e.write_intent(txn, keys[1].clone(), Bytes::from_static(b"late"))
                .is_err()
        );
        assert!(e.delete_intent(txn, keys[1].clone()).is_err());
        assert!(
            e.replace_intent(txn, keys[1].clone(), IntentReplacement::Clear)
                .is_err()
        );
        assert!(
            e.write_intent_batch(
                txn,
                vec![nodus_storage_api::IntentOperation::Put {
                    key: keys[1].clone(),
                    value: Bytes::from_static(b"late")
                }]
            )
            .is_err()
        );
        assert!(
            e.router
                .submit(
                    &group,
                    ShardCommand::CommitTxn {
                        txn_id: txn.0.to_string(),
                        commit_ts: 2_000_000,
                        shard_id: Some(group.clone())
                    }
                )
                .is_err()
        );
    })
    .await
    .unwrap();
    c.assert_rows(false);
    c.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn decided_commit_wins_recovery_and_both_resolvers_converge() {
    let dir = tempfile::tempdir().unwrap();
    let c = Cluster::open(dir.path()).await;
    let txn = TxnId::new();
    c.write_pair(txn).await;
    let (reached, release) = pause_before(&c.engines[0].router, false);
    let e = c.engines[0].clone();
    let commit = tokio::task::spawn_blocking(move || e.commit(txn, 1_000_000));
    tokio::time::timeout(Duration::from_secs(10), reached)
        .await
        .unwrap()
        .unwrap();
    c.assert_rows(false);
    let e = c.engines[1].clone();
    let recovery = tokio::task::spawn_blocking(move || e.recover_tracked_transaction(txn))
        .await
        .unwrap();
    release.send(()).unwrap();
    recovery.unwrap();
    commit.await.unwrap().unwrap();
    c.assert_rows(true);
    assert_eq!(
        c.engines[0].authority_read_for_test(txn).await.phase,
        authority::PhaseV1::Committed {
            commit_ts: 1_000_000
        }
    );
    c.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_participant_batches_and_savepoint_clear_use_tracked_resolution() {
    let dir = tempfile::tempdir().unwrap();
    let c = Cluster::open(dir.path()).await;
    let e = c.engines[0].clone();
    let key = c.keys()[1].clone();
    let other = Bytes::from(format!("{}:other", c.tables[1]));
    tokio::task::spawn_blocking(move || {
        let txn = TxnId::new();
        e.write_intent_batch(
            txn,
            vec![
                nodus_storage_api::IntentOperation::Put {
                    key: key.clone(),
                    value: Bytes::from_static(b"first"),
                },
                nodus_storage_api::IntentOperation::Put {
                    key: other.clone(),
                    value: Bytes::from_static(b"cleared"),
                },
            ],
        )
        .unwrap();
        e.replace_intent(txn, other.clone(), IntentReplacement::Clear)
            .unwrap();
        e.replace_intent(
            txn,
            key.clone(),
            IntentReplacement::Put(Bytes::from_static(b"restored")),
        )
        .unwrap();
        e.commit(txn, 1_000_000).unwrap();
        let txn = TxnId::new();
        e.delete_intent(txn, key.clone()).unwrap();
        e.replace_intent(txn, key.clone(), IntentReplacement::Clear)
            .unwrap();
        e.commit(txn, 2_000_000).unwrap(); // Empty prepared set still commits its fence.
    })
    .await
    .unwrap();
    let store = c.engines[1].engine_for(&c.groups[1]);
    assert_eq!(
        store.get(&c.keys()[1], u64::MAX).unwrap(),
        Some(Bytes::from_static(b"restored"))
    );
    assert!(
        store
            .get(format!("{}:other", c.tables[1]).as_bytes(), u64::MAX)
            .unwrap()
            .is_none()
    );
    drop(store);
    c.shutdown().await;
}

impl RaftKvEngine {
    async fn authority_read_for_test(self: &Arc<Self>, txn: TxnId) -> authority::RecordV1 {
        let e = self.clone();
        tokio::task::spawn_blocking(move || e.authority_read(txn).unwrap().unwrap())
            .await
            .unwrap()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn activation_keeps_existing_legacy_transaction_on_its_original_protocol() {
    let dir = tempfile::tempdir().unwrap();
    let c = Cluster::open(dir.path()).await;
    let original = &c.engines[0];
    let e = Arc::new(RaftKvEngine {
        local: original.local.clone(),
        router: RaftRouter::spawn(original.manager.clone()),
        shard_router: original.shard_router.clone(),
        manager: original.manager.clone(),
        txn_groups: Mutex::new(HashMap::new()),
        metrics: nodus_monitoring::Metrics::default(),
        recovery_gate: Default::default(),
    });
    let keys = c.keys();
    let clone = e.clone();
    let groups = c.groups.clone();
    tokio::task::spawn_blocking(move || {
        let txn = TxnId::new();
        clone
            .write_intent(txn, keys[0].clone(), Bytes::from_static(b"atomic"))
            .unwrap();
        clone.router.enable_txn_authority_for_test();
        clone
            .write_intent(txn, keys[1].clone(), Bytes::from_static(b"atomic"))
            .unwrap();
        clone.commit(txn, 1_000_000).unwrap();
        assert!(clone.authority_read(txn).unwrap().is_none());
        assert_eq!(
            nodus_raftstore::txn_resolution::high_water(clone.local.as_ref()).unwrap(),
            0
        );
        for group in groups {
            assert!(
                participant::read(clone.engine_for(&group).as_ref(), txn.0)
                    .unwrap()
                    .is_none()
            );
        }
    })
    .await
    .unwrap();
    c.assert_rows(true);
    drop(e);
    c.shutdown().await;
}
