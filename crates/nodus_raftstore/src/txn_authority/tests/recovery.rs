use super::*;
use crate::{network::NodusNetworkFactory, server::NodusRaft};
use nodus_storage_lsm::LsmKvEngine;
use openraft::storage::Adaptor;
use std::{path::Path, time::Duration};

async fn raft(kv: Arc<dyn KvEngine>, dir: &Path) -> (NodusRaft, NodusRaftStore) {
    let store = NodusRaftStore::with_kv_at(kv, dir.join("snapshots"))
        .unwrap()
        .with_snapshot_group("shard-meta");
    let (log, sm) = Adaptor::new(store.clone());
    let config = openraft::Config {
        heartbeat_interval: 50,
        election_timeout_min: 150,
        election_timeout_max: 300,
        snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(1_000_000),
        ..Default::default()
    }
    .validate()
    .unwrap();
    let raft = NodusRaft::new(
        1,
        Arc::new(config),
        NodusNetworkFactory::new("shard-meta".into(), Default::default()),
        log,
        sm,
    )
    .await
    .unwrap();
    if !raft.is_initialized().await.unwrap() {
        raft.initialize(BTreeMap::from([(
            1,
            openraft::BasicNode::new("127.0.0.1:1"),
        )]))
        .await
        .unwrap();
    }
    tokio::time::timeout(Duration::from_secs(10), async {
        while raft.metrics().borrow().current_leader != Some(1) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    (raft, store)
}

async fn submit(raft: &NodusRaft, cmd: CommandV1) -> bool {
    tokio::time::timeout(
        Duration::from_secs(10),
        raft.client_write(ShardCommand::TxnAuthorityV1(cmd)),
    )
    .await
    .unwrap()
    .unwrap()
    .data
    .success
}

async fn prepare_raft(raft: &NodusRaft, kv: &dyn KvEngine, txn: Uuid) -> RecordV1 {
    assert!(submit(raft, begin(txn)).await);
    for operation in [
        OperationV1::Enroll {
            group: "shard-a".into(),
            epoch: 7,
        },
        OperationV1::Seal,
    ] {
        let record = read(kv, txn).unwrap().unwrap();
        assert!(submit(raft, command(&record, operation)).await);
    }
    read(kv, txn).unwrap().unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_raft_commit_and_recovery_have_exactly_one_winner() {
    let dir = tempfile::tempdir().unwrap();
    let kv = Arc::new(MemKvEngine::new());
    let (raft, store) = raft(kv.clone(), dir.path()).await;
    for _ in 0..16 {
        let txn = Uuid::new_v4();
        let pending = prepare_raft(&raft, kv.as_ref(), txn).await;
        let commit = command(&pending, OperationV1::Commit { commit_ts: 100 });
        let abort = command(
            &pending,
            OperationV1::FenceAbort {
                recovery: owner(20),
            },
        );
        let (committed, aborted) =
            tokio::join!(submit(&raft, commit.clone()), submit(&raft, abort));
        assert_ne!(committed, aborted);
        let terminal = read(kv.as_ref(), txn).unwrap().unwrap();
        assert_eq!(
            terminal.phase,
            if committed {
                PhaseV1::Committed { commit_ts: 100 }
            } else {
                PhaseV1::Aborted
            }
        );
        assert_eq!(submit(&raft, commit).await, committed);
    }
    raft.shutdown().await.unwrap();
    drop((raft, store));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn authority_crash_child() {
    let Some(path) = std::env::var_os("NODUS_TEST_TXN_AUTHORITY_CRASH_DIR") else {
        return;
    };
    let dir = Path::new(&path);
    let kv = Arc::new(LsmKvEngine::with_wal(dir.join("kv"), None).unwrap());
    let (raft, _store) = raft(kv.clone(), dir).await;
    let txn = Uuid::from_u128(123);
    let pending = prepare_raft(&raft, kv.as_ref(), txn).await;
    match std::env::var("NODUS_TEST_TXN_AUTHORITY_CRASH_PHASE")
        .unwrap()
        .as_str()
    {
        "commit" => assert!(
            submit(
                &raft,
                command(&pending, OperationV1::Commit { commit_ts: 100 })
            )
            .await
        ),
        "abort" => assert!(
            submit(
                &raft,
                command(
                    &pending,
                    OperationV1::FenceAbort {
                        recovery: owner(20)
                    }
                )
            )
            .await
        ),
        "pending" => {}
        _ => panic!("unknown crash phase"),
    }
    // The Raft acknowledgement has arrived. Skip all engine/Raft destructors.
    std::process::exit(86);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acknowledged_decisions_and_pending_owners_survive_process_exit() {
    for phase in ["pending", "commit", "abort"] {
        let dir = tempfile::tempdir().unwrap();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "txn_authority::tests::recovery::authority_crash_child",
                "--nocapture",
            ])
            .env("NODUS_TEST_TXN_AUTHORITY_CRASH_DIR", dir.path())
            .env("NODUS_TEST_TXN_AUTHORITY_CRASH_PHASE", phase)
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(30);
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if std::time::Instant::now() >= deadline {
                child.kill().unwrap();
                let _ = child.wait();
                panic!("authority crash child timed out");
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        };
        assert_eq!(status.code(), Some(86));
        let kv = Arc::new(LsmKvEngine::with_wal(dir.path().join("kv"), None).unwrap());
        let txn = Uuid::from_u128(123);
        let record = read(kv.as_ref(), txn).unwrap().unwrap();
        assert_eq!(record.participants, BTreeMap::from([("shard-a".into(), 7)]));
        assert_eq!(
            record.phase,
            match phase {
                "commit" => PhaseV1::Committed { commit_ts: 100 },
                "abort" => PhaseV1::Aborted,
                _ => PhaseV1::Preparing,
            }
        );
        let (raft, store) = raft(kv.clone(), dir.path()).await;
        assert!(store.state_machine.read().await.last_applied_log.is_some());
        if phase == "pending" {
            assert!(
                submit(
                    &raft,
                    command(
                        &record,
                        OperationV1::FenceAbort {
                            recovery: owner(20)
                        }
                    )
                )
                .await
            );
        }
        let delayed = CommandV1 {
            txn_id: txn,
            expected: Some(TokenV1 {
                owner: owner(10),
                generation: 1,
                revision: 3,
            }),
            operation: OperationV1::Commit { commit_ts: 100 },
        };
        assert_eq!(submit(&raft, delayed).await, phase == "commit");
        let storage = Arc::downgrade(&kv);
        raft.shutdown().await.unwrap();
        drop((raft, store, kv));
        tokio::time::timeout(Duration::from_secs(10), async {
            while storage.strong_count() != 0 {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .unwrap();
    }
}
