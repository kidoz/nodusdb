mod cluster;
mod faults;
use bytes::Bytes;
use nodus_catalog::TableId;
use nodus_raftstore::migration::{
    EpochMutationV1 as Mutation, MigrationCommandV1 as Control, MigrationPhase, MigrationPlan,
    read_fence, read_record,
};
use nodus_raftstore::{NodusRaftStore, ShardCommand, ShardResponse};
use nodus_storage_api::{KvEngine, NamespacedKvEngine, TxnId};
use openraft::storage::{RaftSnapshotBuilder, RaftStorage};
use std::sync::Arc;
use uuid::Uuid;

struct Harness {
    kv: Arc<dyn KvEngine>,
    store: NodusRaftStore,
    index: u64,
}

impl Harness {
    async fn new(kv: Arc<dyn KvEngine>, snapshots: std::path::PathBuf) -> Self {
        let store = NodusRaftStore::with_kv_at(kv.clone(), snapshots);
        store.state_machine.write().await.meta_store =
            Some(Arc::new(nodus_meta::MemMetaStore::new()));
        Self {
            kv,
            store,
            index: 0,
        }
    }

    async fn apply(&mut self, cmd: ShardCommand) -> ShardResponse {
        self.index += 1;
        let entry = openraft::Entry {
            log_id: openraft::LogId::new(openraft::CommittedLeaderId::new(1, 1), self.index),
            payload: openraft::EntryPayload::Normal(cmd),
        };
        self.store.append_to_log([entry.clone()]).await.unwrap();
        self.store
            .apply_to_state_machine(&[entry])
            .await
            .unwrap()
            .remove(0)
    }

    async fn control(&mut self, cmd: Control) -> ShardResponse {
        self.apply(ShardCommand::MigrationV1(cmd)).await
    }

    async fn mutation(&mut self, epoch: u64, mutation: Mutation) -> ShardResponse {
        self.apply(ShardCommand::EpochWriteV1 { epoch, mutation })
            .await
    }
}

fn plan() -> MigrationPlan {
    MigrationPlan {
        operation_id: Uuid::new_v4(),
        table_id: TableId::new(),
        source_epochs: std::collections::BTreeMap::from([("source".into(), 0)]),
        sources: vec!["source".into()],
        destinations: vec!["left".into(), "right".into()],
    }
}

#[tokio::test]
async fn migration_journal_is_conditional_and_snapshot_preserves_fence() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = Harness::new(
        Arc::new(nodus_storage_mem::MemKvEngine::new()),
        dir.path().join("source"),
    )
    .await;
    let plan = plan();
    assert!(h.control(Control::Plan(plan.clone())).await.success);
    assert!(h.control(Control::Plan(plan.clone())).await.success);
    let mut conflict = plan.clone();
    conflict.operation_id = Uuid::new_v4();
    assert!(!h.control(Control::Plan(conflict)).await.success);
    assert!(
        h.control(Control::StartFencing {
            table_id: plan.table_id,
            operation_id: plan.operation_id
        })
        .await
        .success
    );
    assert!(
        h.control(Control::Acquire {
            operation_id: plan.operation_id,
            expected_epoch: 0
        })
        .await
        .success
    );
    assert!(
        h.control(Control::Acquire {
            operation_id: plan.operation_id,
            expected_epoch: 0
        })
        .await
        .success
    );
    assert_eq!(
        read_record(h.kv.as_ref(), plan.table_id)
            .unwrap()
            .unwrap()
            .phase,
        MigrationPhase::Fencing
    );
    let snap = h.store.build_snapshot().await.unwrap();
    let mut receiver = Harness::new(
        Arc::new(nodus_storage_mem::MemKvEngine::new()),
        dir.path().join("receiver"),
    )
    .await;
    receiver
        .store
        .install_snapshot(&snap.meta, snap.snapshot)
        .await
        .unwrap();
    receiver.index = h.index;
    assert_eq!(
        read_fence(receiver.kv.as_ref()).unwrap(),
        read_fence(h.kv.as_ref()).unwrap()
    );
    assert_eq!(
        read_record(receiver.kv.as_ref(), plan.table_id).unwrap(),
        read_record(h.kv.as_ref(), plan.table_id).unwrap()
    );
    assert!(
        !receiver
            .mutation(
                0,
                Mutation::Put {
                    txn_id: Uuid::new_v4(),
                    key: b"late".to_vec(),
                    value: b"bad".to_vec()
                }
            )
            .await
            .success
    );
    assert!(
        receiver
            .control(Control::RequestCancel {
                table_id: plan.table_id,
                operation_id: plan.operation_id
            })
            .await
            .success
    );
    assert!(
        !receiver
            .control(Control::StartFencing {
                table_id: plan.table_id,
                operation_id: plan.operation_id
            })
            .await
            .success
    );
}

#[tokio::test]
async fn fence_waits_for_intents_and_rejects_stale_writes_prepare_and_commit() {
    let dir = tempfile::tempdir().unwrap();
    let mut h = Harness::new(
        Arc::new(nodus_storage_mem::MemKvEngine::new()),
        dir.path().into(),
    )
    .await;
    let txn = Uuid::new_v4();
    let operation_id = Uuid::new_v4();
    assert!(
        h.apply(ShardCommand::PutIntent {
            txn_id: txn.to_string(),
            key: b"row".to_vec(),
            value: b"accepted".to_vec(),
            shard_id: None
        })
        .await
        .success
    );
    assert!(
        h.apply(ShardCommand::PrepareTxn {
            txn_id: txn.to_string(),
            shard_id: None
        })
        .await
        .success
    );
    assert!(
        !h.control(Control::Acquire {
            operation_id,
            expected_epoch: 0
        })
        .await
        .success
    );
    assert!(read_fence(h.kv.as_ref()).unwrap().is_none());
    assert!(
        h.apply(ShardCommand::CommitTxn {
            txn_id: txn.to_string(),
            commit_ts: 100,
            shard_id: None
        })
        .await
        .success
    );
    assert!(
        h.control(Control::Acquire {
            operation_id,
            expected_epoch: 0
        })
        .await
        .success
    );
    for mutation in [
        Mutation::Put {
            txn_id: txn,
            key: b"late".to_vec(),
            value: b"bad".to_vec(),
        },
        Mutation::Delete {
            txn_id: txn,
            key: b"row".to_vec(),
        },
        Mutation::Prepare { txn_id: txn },
        Mutation::Commit {
            txn_id: txn,
            commit_ts: 200,
        },
    ] {
        assert!(!h.mutation(0, mutation).await.success);
    }
    assert!(
        !h.apply(ShardCommand::DeleteIntent {
            txn_id: txn.to_string(),
            key: b"row".to_vec(),
            shard_id: None
        })
        .await
        .success
    );
    assert!(
        h.control(Control::Release {
            operation_id,
            epoch: 1
        })
        .await
        .success
    );
    assert!(
        h.control(Control::Release {
            operation_id,
            epoch: 1
        })
        .await
        .success
    );
    assert!(
        !h.control(Control::Acquire {
            operation_id,
            expected_epoch: 0
        })
        .await
        .success
    );
    assert!(
        !h.mutation(
            0,
            Mutation::Commit {
                txn_id: txn,
                commit_ts: 200
            }
        )
        .await
        .success
    );
    let current = Uuid::new_v4();
    assert!(
        h.mutation(
            1,
            Mutation::Put {
                txn_id: current,
                key: b"new".to_vec(),
                value: b"current".to_vec()
            }
        )
        .await
        .success
    );
    assert!(
        !h.control(Control::Acquire {
            operation_id: Uuid::new_v4(),
            expected_epoch: 1
        })
        .await
        .success
    );
    assert!(
        h.mutation(
            1,
            Mutation::Commit {
                txn_id: current,
                commit_ts: 201
            }
        )
        .await
        .success
    );
    assert!(
        h.control(Control::Acquire {
            operation_id: Uuid::new_v4(),
            expected_epoch: 1
        })
        .await
        .success
    );
    assert!(
        !h.control(Control::Release {
            operation_id,
            epoch: 1
        })
        .await
        .success
    );
    assert_eq!(read_fence(h.kv.as_ref()).unwrap().unwrap().epoch, 2);
    assert_eq!(
        h.kv.get(b"row", 300).unwrap().unwrap(),
        Bytes::from_static(b"accepted")
    );
    assert!(h.kv.get(b"late", 300).unwrap().is_none());
    assert_eq!(
        h.kv.get(b"new", 300).unwrap().unwrap(),
        Bytes::from_static(b"current")
    );
}

#[tokio::test]
async fn intent_inspection_is_namespace_scoped_and_replay_cleans_only_control_intent() {
    let dir = tempfile::tempdir().unwrap();
    let base: Arc<dyn KvEngine> =
        Arc::new(nodus_storage_lsm::LsmKvEngine::with_wal(dir.path(), None).unwrap());
    let a: Arc<dyn KvEngine> = Arc::new(NamespacedKvEngine::new(base.clone(), "a"));
    let b = NamespacedKvEngine::new(base, "b");
    b.write_intent(TxnId::new(), Bytes::from("other"), Bytes::from("pending"))
        .unwrap();
    assert!(!a.has_pending_intents(b"").unwrap());
    assert!(b.has_pending_intents(b"").unwrap());
    let mut h = Harness::new(a, dir.path().join("snapshots")).await;
    // Reproduce a control write that reached the WAL before a failed commit.
    let key = b"\x01migration/v1/fence";
    let mut identity = key.to_vec();
    identity.extend_from_slice(&1u64.to_be_bytes());
    let txn = TxnId(Uuid::new_v5(&Uuid::NAMESPACE_OID, &identity));
    h.kv.write_intent(txn, Bytes::copy_from_slice(key), Bytes::from("partial"))
        .unwrap();
    assert!(
        h.control(Control::Acquire {
            operation_id: Uuid::new_v4(),
            expected_epoch: 0
        })
        .await
        .success
    );
    assert!(
        b.has_pending_intents(b"").unwrap(),
        "unrelated intents survive control replay"
    );
}

#[test]
fn fence_survives_abrupt_exit_after_raft_acknowledgement() {
    const CHILD_DIR: &str = "NODUS_R3_CRASH_TEST_DIR";
    let table_id = TableId(Uuid::from_u128(17));
    if let Some(dir) = std::env::var_os(CHILD_DIR) {
        let runtime = tokio::runtime::Runtime::new().unwrap();
        runtime.block_on(async {
            let dir = std::path::PathBuf::from(dir);
            let kv: Arc<dyn KvEngine> =
                Arc::new(nodus_storage_lsm::LsmKvEngine::with_wal(&dir, None).unwrap());
            let store = NodusRaftStore::with_kv_at(kv, dir.join("snapshots"));
            store.state_machine.write().await.meta_store =
                Some(Arc::new(nodus_meta::MemMetaStore::new()));
            let (log, sm) = openraft::storage::Adaptor::new(store);
            let raft = nodus_raftstore::server::NodusRaft::new(
                1,
                Arc::new(openraft::Config::default().validate().unwrap()),
                nodus_raftstore::network::NodusNetworkFactory::new(
                    "fixture".into(),
                    Default::default(),
                ),
                log,
                sm,
            )
            .await
            .unwrap();
            raft.initialize(std::collections::BTreeMap::from([(
                1,
                openraft::BasicNode::new("127.0.0.1:0"),
            )]))
            .await
            .unwrap();
            tokio::time::timeout(std::time::Duration::from_secs(5), async {
                while raft.metrics().borrow().current_leader != Some(1) {
                    tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            let mut plan = plan();
            plan.table_id = table_id;
            let txn = Uuid::new_v4();
            for command in [
                ShardCommand::PutIntent {
                    txn_id: txn.to_string(),
                    key: b"committed".to_vec(),
                    value: b"survives".to_vec(),
                    shard_id: None,
                },
                ShardCommand::CommitTxn {
                    txn_id: txn.to_string(),
                    commit_ts: 1000,
                    shard_id: None,
                },
                ShardCommand::MigrationV1(Control::Plan(plan.clone())),
                ShardCommand::MigrationV1(Control::StartFencing {
                    table_id,
                    operation_id: plan.operation_id,
                }),
            ] {
                assert!(raft.client_write(command).await.unwrap().data.success);
            }
            let result = raft
                .client_write(ShardCommand::MigrationV1(Control::Acquire {
                    operation_id: plan.operation_id,
                    expected_epoch: 0,
                }))
                .await
                .unwrap();
            assert!(result.data.success);
            // Exit before Raft shutdown, runtime shutdown, or engine Drop.
            std::process::exit(0);
        });
        unreachable!();
    }
    let dir = tempfile::tempdir().unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "migration_tests::fence_survives_abrupt_exit_after_raft_acknowledgement",
            "--nocapture",
        ])
        .env(CHILD_DIR, dir.path())
        .status()
        .unwrap();
    assert!(status.success());
    let runtime = tokio::runtime::Runtime::new().unwrap();
    runtime.block_on(async {
        let kv: Arc<dyn KvEngine> =
            Arc::new(nodus_storage_lsm::LsmKvEngine::with_wal(dir.path(), None).unwrap());
        assert!(read_fence(kv.as_ref()).unwrap().unwrap().closed);
        let record = read_record(kv.as_ref(), table_id).unwrap().unwrap();
        assert_eq!(record.phase, MigrationPhase::Fencing);
        assert_eq!(
            read_fence(kv.as_ref()).unwrap().unwrap().operation_id,
            record.plan.operation_id
        );
        assert_eq!(
            kv.get(b"committed", u64::MAX).unwrap().unwrap(),
            Bytes::from_static(b"survives")
        );
        let mut h = Harness::new(kv, dir.path().join("snapshots")).await;
        h.index = h
            .store
            .state_machine
            .read()
            .await
            .last_applied_log
            .unwrap()
            .index;
        let result = h
            .apply(ShardCommand::CommitTxn {
                txn_id: Uuid::new_v4().to_string(),
                commit_ts: 100,
                shard_id: None,
            })
            .await;
        assert!(!result.success);
        assert!(result.error.unwrap().contains("epoch is required"));
    });
}
