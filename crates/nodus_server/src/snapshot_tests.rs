//! Persistent integration and abrupt-process evidence for snapshot publication.
use anyhow::Result;
use bytes::Bytes;
use nodus_catalog::MemoryCatalog;
use nodus_raftstore::NodusRaftStore;
use nodus_storage_api::{
    KeyRange, KvEngine, KvPair, KvResult, NamespacedKvEngine, SnapshotRow, SnapshotScope, TxnId,
};
use nodus_storage_lsm::LsmKvEngine;
use openraft::storage::{RaftSnapshotBuilder, RaftStorage};
use std::{path::Path, sync::Arc};

struct FinalizedSnapshotFixture;
impl nodus_raftstore::SnapshotCompatibility for FinalizedSnapshotFixture {
    fn finalized_cluster_version(&self) -> Result<u64> {
        Ok(2)
    }
    fn member_snapshot_versions(&self) -> Result<std::collections::BTreeMap<u64, u16>> {
        Ok(std::collections::BTreeMap::from([(1, 2), (2, 2)]))
    }
}

fn put(kv: &dyn KvEngine, key: &'static [u8], value: &'static [u8], ts: u64) {
    let txn = TxnId::new();
    kv.write_intent(txn, Bytes::from_static(key), Bytes::from_static(value))
        .unwrap();
    kv.commit(txn, ts).unwrap();
}

async fn meta_store(kv: Arc<dyn KvEngine>, dir: &Path) -> (NodusRaftStore, Arc<MemoryCatalog>) {
    let cat = Arc::new(
        MemoryCatalog::with_store(Arc::new(nodus_executor::KvCatalogStore::new(kv.clone())))
            .unwrap(),
    );
    let store = NodusRaftStore::with_kv_at(kv.clone(), dir.into());
    {
        let mut sm = store.state_machine.write().await;
        sm.catalog_reader = Some(cat.clone());
        sm.catalog_writer = Some(cat.clone());
        sm.meta_store = Some(Arc::new(nodus_meta::PersistentMetaStore::new(kv)));
    }
    (store, cat)
}

#[tokio::test]
async fn meta_snapshot_preserves_local_shards_and_recovers_catalog_authorization() {
    use nodus_catalog::*;
    let source = tempfile::tempdir().unwrap();
    let src = Arc::new(LsmKvEngine::with_wal(source.path().join("kv"), None).unwrap());
    let (mut sender, cat) = meta_store(src.clone(), &source.path().join("snap")).await;
    cat.create_database(CreateDatabaseRequest {
        id: DatabaseId::new(),
        name: "incoming".into(),
        owner_role_id: None,
    })
    .unwrap();
    let user = PrincipalId::new();
    let role = PrincipalId::new();
    for (id, name, principal_type) in [
        (user, "alice", PrincipalType::User),
        (role, "reader", PrincipalType::Role),
    ] {
        cat.create_role(CreateRoleRequest {
            id,
            name: name.into(),
            principal_type,
            database_id: None,
        })
        .unwrap();
    }
    cat.add_role_member(AddRoleMemberRequest {
        role_principal_id: role,
        member_id: user,
    })
    .unwrap();
    put(src.as_ref(), b"meta:shard_placements", b"routing", 10);
    put(
        src.as_ref(),
        b"\0txn2pc\0decision",
        b"committed-decision",
        10,
    );
    let foreign = NamespacedKvEngine::new(src.clone(), "shard-a");
    put(&foreign, b"row", b"must-not-export", 10);
    let foreign_txn = TxnId::new();
    foreign
        .write_intent(
            foreign_txn,
            Bytes::from_static(b"pending"),
            Bytes::from_static(b"must-not-export"),
        )
        .unwrap();
    let snapshot = sender.build_snapshot().await.unwrap();
    let mut bytes = Vec::new();
    use tokio::io::{AsyncReadExt, AsyncSeekExt};
    let mut wire = snapshot.snapshot;
    wire.read_to_end(&mut bytes).await.unwrap();
    assert_eq!(
        &bytes[..7],
        b"NSNP\0\x01\x01",
        "legacy wire version remains unchanged"
    );
    assert!(
        !bytes
            .windows(b"must-not-export".len())
            .any(|w| w == b"must-not-export")
    );
    wire.rewind().await.unwrap();

    let target = tempfile::tempdir().unwrap();
    let dst = Arc::new(LsmKvEngine::with_wal(target.path().join("kv"), None).unwrap());
    let a = NamespacedKvEngine::new(dst.clone(), "shard-a");
    put(&a, b"row", b"local-shard", 10);
    let pending = TxnId::new();
    a.write_intent(
        pending,
        Bytes::from_static(b"index"),
        Bytes::from_static(b"pending-index"),
    )
    .unwrap();
    put(dst.as_ref(), b"\0hlc\0clock", b"local-clock", 10);
    let (mut receiver, old_cat) = meta_store(dst.clone(), &target.path().join("snap")).await;
    old_cat
        .create_database(CreateDatabaseRequest {
            id: DatabaseId::new(),
            name: "orphan".into(),
            owner_role_id: None,
        })
        .unwrap();
    receiver
        .install_snapshot(&snapshot.meta, wire)
        .await
        .unwrap();
    assert!(old_cat.get_database("incoming").is_ok());
    assert!(old_cat.get_database("orphan").is_err());
    assert!(
        old_cat
            .get_effective_principals(user)
            .unwrap()
            .contains(&role)
    );
    assert_eq!(a.get(b"row", 10).unwrap().unwrap().as_ref(), b"local-shard");
    a.commit(pending, 30).unwrap();
    drop((receiver, old_cat, a, dst));
    let dst = Arc::new(LsmKvEngine::with_wal(target.path().join("kv"), None).unwrap());
    let (mut receiver, cat) = meta_store(dst.clone(), &target.path().join("snap")).await;
    assert!(cat.get_database("incoming").is_ok());
    assert!(cat.get_effective_principals(user).unwrap().contains(&role));
    for (key, expected) in [
        (b"meta:shard_placements".as_slice(), b"routing".as_slice()),
        (b"\0txn2pc\0decision", b"committed-decision"),
        (b"\0hlc\0clock", b"local-clock"),
    ] {
        assert_eq!(dst.get(key, u64::MAX).unwrap().unwrap().as_ref(), expected);
    }
    let a = NamespacedKvEngine::new(dst, "shard-a");
    assert_eq!(
        a.get(b"index", 30).unwrap().unwrap().as_ref(),
        b"pending-index"
    );
    assert!(receiver.get_current_snapshot().await.unwrap().is_some());
}

// The wrapper terminates only after the real LSM checkpoint returns. No Drop,
// graceful shutdown, or mocked persistence occurs at this boundary.
struct CheckpointExit {
    inner: Arc<dyn KvEngine>,
    boundary: String,
}
impl KvEngine for CheckpointExit {
    fn replace_intent(
        &self,
        txn: TxnId,
        key: Bytes,
        replacement: nodus_storage_api::IntentReplacement,
    ) -> KvResult<()> {
        self.inner.replace_intent(txn, key, replacement)
    }

    fn snapshot_rows(&self, scope: &SnapshotScope) -> Result<Vec<SnapshotRow>> {
        self.inner.snapshot_rows(scope)
    }
    fn replace_snapshot(
        &self,
        scope: &SnapshotScope,
        rows: Vec<SnapshotRow>,
        pointers: Vec<SnapshotRow>,
    ) -> Result<()> {
        if self.boundary == "before-checkpoint" {
            std::process::exit(87);
        }
        self.inner.replace_snapshot(scope, rows, pointers)?;
        std::process::exit(87);
    }
    fn get(&self, key: &[u8], ts: u64) -> Result<Option<Bytes>> {
        self.inner.get(key, ts)
    }
    fn scan(
        &self,
        range: KeyRange,
        ts: u64,
    ) -> Result<Box<dyn Iterator<Item = Result<KvPair>> + Send>> {
        self.inner.scan(range, ts)
    }
    fn write_intent(&self, txn: TxnId, key: Bytes, value: Bytes) -> KvResult<()> {
        self.inner.write_intent(txn, key, value)
    }
    fn delete_intent(&self, txn: TxnId, key: Bytes) -> KvResult<()> {
        self.inner.delete_intent(txn, key)
    }
    fn commit(&self, txn: TxnId, ts: u64) -> KvResult<()> {
        self.inner.commit(txn, ts)
    }
    fn abort(&self, txn: TxnId) -> KvResult<()> {
        self.inner.abort(txn)
    }
}

#[tokio::test]
async fn snapshot_install_crash_child() {
    let Ok(dir) = std::env::var("NODUS_TEST_RAFT_SNAPSHOT_DIR") else {
        return;
    };
    let dir = Path::new(&dir);
    let mvcc = std::env::var("NODUS_TEST_RAFT_SNAPSHOT_MVCC").as_deref() == Ok("true");
    let source = Arc::new(nodus_storage_mem::MemKvEngine::new());
    if mvcc {
        put(source.as_ref(), b"row", b"history", 5);
        source
            .write_intent(
                TxnId(uuid::Uuid::from_u128(42)),
                Bytes::from_static(b"pending"),
                Bytes::from_static(b"uncommitted"),
            )
            .unwrap();
    }
    put(source.as_ref(), b"row", b"new", 20);
    put(source.as_ref(), b"index", b"new", 20);
    let mut sender = NodusRaftStore::with_kv_at(source, dir.join("source-snap"));
    sender.state_machine.write().await.last_applied_log = Some(openraft::LogId::new(
        openraft::CommittedLeaderId::new(1, 1),
        20,
    ));
    if mvcc {
        sender = sender.with_snapshot_compatibility(Arc::new(FinalizedSnapshotFixture));
        sender.state_machine.write().await.last_membership = openraft::StoredMembership::new(
            None,
            openraft::Membership::new(
                vec![std::collections::BTreeSet::from([1])],
                std::collections::BTreeMap::from([(1, openraft::BasicNode::new("fixture"))]),
            ),
        );
    }
    let snapshot = sender.build_snapshot().await.unwrap();
    let inner = Arc::new(LsmKvEngine::with_wal(dir.join("kv"), None).unwrap());
    let kv = Arc::new(CheckpointExit {
        inner,
        boundary: std::env::var("NODUS_TEST_RAFT_SNAPSHOT_BOUNDARY").unwrap(),
    });
    let mut receiver = NodusRaftStore::with_kv_at(kv, dir.join("snap"));
    receiver
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .unwrap();
    panic!("snapshot fault not reached");
}

#[tokio::test]
async fn abrupt_install_restart_never_serves_mixed_snapshot_and_applied_state() {
    exercise_install_crash(false).await;
}

#[tokio::test]
async fn mvcc_install_crash_recovers_history_intents_and_backup_generation() {
    exercise_install_crash(true).await;
}

async fn exercise_install_crash(mvcc: bool) {
    for boundary in ["before-checkpoint", "after-checkpoint"] {
        let dir = tempfile::tempdir().unwrap();
        {
            let kv = Arc::new(LsmKvEngine::with_wal(dir.path().join("kv"), None).unwrap());
            put(kv.as_ref(), b"row", b"old", 10);
            put(kv.as_ref(), b"orphan", b"old", 10);
            let mut receiver = NodusRaftStore::with_kv_at(kv, dir.path().join("snap"));
            receiver.build_snapshot().await.unwrap();
        }
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "snapshot_tests::snapshot_install_crash_child",
                "--nocapture",
            ])
            .env("NODUS_TEST_RAFT_SNAPSHOT_DIR", dir.path())
            .env("NODUS_TEST_RAFT_SNAPSHOT_BOUNDARY", boundary)
            .env("NODUS_TEST_RAFT_SNAPSHOT_MVCC", mvcc.to_string())
            .status()
            .unwrap();
        assert_eq!(
            status.code(),
            Some(87),
            "real checkpoint boundary must be reached"
        );
        let kv = Arc::new(LsmKvEngine::with_wal(dir.path().join("kv"), None).unwrap());
        let mut receiver = NodusRaftStore::with_kv_at(kv.clone(), dir.path().join("snap"));
        let installed = boundary == "after-checkpoint";
        assert_eq!(
            kv.get(b"row", u64::MAX).unwrap().unwrap().as_ref(),
            if installed { b"new" } else { b"old" }
        );
        assert_eq!(kv.get(b"orphan", u64::MAX).unwrap().is_none(), installed);
        assert_eq!(kv.get(b"index", u64::MAX).unwrap().is_some(), installed);
        assert_eq!(
            receiver
                .state_machine
                .read()
                .await
                .last_applied_log
                .map(|l| l.index),
            if installed { Some(20) } else { None }
        );
        assert!(
            receiver.get_current_snapshot().await.unwrap().is_none(),
            "publication gap must not serve the previous file with new metadata"
        );
        assert_eq!(kv.recovery_generation().unwrap().is_some(), installed);
        if mvcc && installed {
            assert_eq!(kv.get(b"row", 5).unwrap().unwrap().as_ref(), b"history");
            let pending = TxnId(uuid::Uuid::from_u128(42));
            assert_eq!(
                kv.pending_intent_keys(pending),
                vec![Bytes::from_static(b"pending")]
            );
            assert!(kv.get(b"pending", u64::MAX).unwrap().is_none());
            kv.abort(pending).unwrap();
            assert!(kv.get(b"pending", u64::MAX).unwrap().is_none());
            receiver = receiver.with_snapshot_compatibility(Arc::new(FinalizedSnapshotFixture));
        }
        let rebuilt = receiver.build_snapshot().await.unwrap();
        assert_eq!(
            rebuilt.meta.last_log_id.map(|l| l.index),
            if installed { Some(20) } else { None }
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lagging_learner_receives_snapshot_over_tcp_and_reopens_on_lsm() {
    exercise_learner_snapshot(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mvcc_learner_preserves_history_and_resolves_snapshotted_intent_over_tcp() {
    exercise_learner_snapshot(true).await;
}

async fn exercise_learner_snapshot(mvcc: bool) {
    use nodus_raftstore::{
        ShardCommand,
        network::NodusNetworkFactory,
        server::{NodusRaft, RaftState, raft_routes},
    };
    use std::{collections::BTreeMap, time::Duration};
    let dir = tempfile::tempdir().unwrap();
    let config = Arc::new(
        openraft::Config {
            max_in_snapshot_log_to_keep: 0,
            purge_batch_size: 1,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );
    let mut rafts = Vec::new();
    let mut stores = Vec::new();
    let mut engines = Vec::new();
    let mut servers = Vec::new();
    let mut members = BTreeMap::new();
    for id in 1..=2 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        members.insert(
            id,
            openraft::BasicNode::new(listener.local_addr().unwrap().to_string()),
        );
        let kv =
            Arc::new(LsmKvEngine::with_wal(dir.path().join(format!("kv-{id}")), None).unwrap());
        let other = NamespacedKvEngine::new(kv.clone(), "shard-other");
        put(&other, b"row", b"local-other", 10);
        let group = Arc::new(NamespacedKvEngine::new(kv.clone(), "shard-a"));
        let store = NodusRaftStore::with_kv_at(group, dir.path().join(format!("snap-{id}")));
        let store = if mvcc {
            store
                .with_snapshot_group("shard-a")
                .with_snapshot_compatibility(Arc::new(FinalizedSnapshotFixture))
        } else {
            store
        };
        let (log, sm) = openraft::storage::Adaptor::new(store.clone());
        let raft = NodusRaft::new(
            id,
            config.clone(),
            NodusNetworkFactory::new("shard-a".into(), Default::default()),
            log,
            sm,
        )
        .await
        .unwrap();
        let state = RaftState::new();
        state
            .rafts
            .write()
            .await
            .insert("shard-a".into(), raft.clone());
        servers.push(tokio::spawn(async move {
            axum::serve(listener, raft_routes().with_state(state))
                .await
                .unwrap();
        }));
        stores.push(store);
        rafts.push(raft);
        engines.push(kv);
    }
    rafts[0]
        .initialize(BTreeMap::from([(1, members[&1].clone())]))
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while rafts[0].metrics().borrow().current_leader != Some(1) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let txn_id = uuid::Uuid::new_v4().to_string();
    assert!(
        rafts[0]
            .client_write(ShardCommand::PutIntent {
                txn_id: txn_id.clone(),
                key: b"row".to_vec(),
                value: b"replicated".to_vec(),
                shard_id: Some("shard-a".into())
            })
            .await
            .unwrap()
            .data
            .success
    );
    let mut committed = rafts[0]
        .client_write(ShardCommand::CommitTxn {
            txn_id,
            commit_ts: 20,
            shard_id: Some("shard-a".into()),
        })
        .await
        .unwrap()
        .log_id;
    let pending = uuid::Uuid::new_v4().to_string();
    if mvcc {
        let changed = uuid::Uuid::new_v4().to_string();
        rafts[0]
            .client_write(ShardCommand::PutIntent {
                txn_id: changed.clone(),
                key: b"row".to_vec(),
                value: b"updated".to_vec(),
                shard_id: Some("shard-a".into()),
            })
            .await
            .unwrap();
        rafts[0]
            .client_write(ShardCommand::CommitTxn {
                txn_id: changed,
                commit_ts: 30,
                shard_id: Some("shard-a".into()),
            })
            .await
            .unwrap();
        committed = rafts[0]
            .client_write(ShardCommand::PutIntent {
                txn_id: pending.clone(),
                key: b"row".to_vec(),
                value: b"resolved".to_vec(),
                shard_id: Some("shard-a".into()),
            })
            .await
            .unwrap()
            .log_id;
    }
    rafts[0].trigger().snapshot().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while rafts[0]
            .metrics()
            .borrow()
            .purged
            .is_none_or(|l| l.index < committed.index)
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    // Its required log has been purged: adding this empty learner necessarily
    // goes through the real HTTP snapshot transfer, not append-only catch-up.
    tokio::time::timeout(
        Duration::from_secs(10),
        rafts[0].add_learner(2, members[&2].clone(), true),
    )
    .await
    .unwrap()
    .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if stores[1].get_current_snapshot().await.unwrap().is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let received = stores[1].get_current_snapshot().await.unwrap().unwrap();
    assert!(received.meta.last_log_id.unwrap().index >= committed.index);
    if mvcc {
        let group = NamespacedKvEngine::new(engines[1].clone(), "shard-a");
        assert_eq!(
            group.get(b"row", 20).unwrap().unwrap().as_ref(),
            b"replicated"
        );
        assert_eq!(group.get(b"row", 30).unwrap().unwrap().as_ref(), b"updated");
        assert_eq!(
            group
                .pending_intent_keys(TxnId(uuid::Uuid::parse_str(&pending).unwrap()))
                .len(),
            1
        );
        rafts[0]
            .client_write(ShardCommand::CommitTxn {
                txn_id: pending,
                commit_ts: 40,
                shard_id: Some("shard-a".into()),
            })
            .await
            .unwrap();
        tokio::time::timeout(Duration::from_secs(10), async {
            while group.get(b"row", 40).unwrap().as_deref() != Some(b"resolved") {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .unwrap();
    }
    for raft in &rafts {
        raft.shutdown().await.unwrap();
    }
    for server in servers {
        server.abort();
        let _ = server.await;
    }
    drop((rafts, stores, engines));
    let kv = Arc::new(LsmKvEngine::with_wal(dir.path().join("kv-2"), None).unwrap());
    let group = Arc::new(NamespacedKvEngine::new(kv.clone(), "shard-a"));
    assert_eq!(
        group.get(b"row", 20).unwrap().unwrap().as_ref(),
        b"replicated"
    );
    if mvcc {
        assert_eq!(group.get(b"row", 30).unwrap().unwrap().as_ref(), b"updated");
        assert_eq!(
            group.get(b"row", 40).unwrap().unwrap().as_ref(),
            b"resolved"
        );
    }
    let other = NamespacedKvEngine::new(kv, "shard-other");
    assert_eq!(
        other.get(b"row", 10).unwrap().unwrap().as_ref(),
        b"local-other"
    );
    let mut reopened = NodusRaftStore::with_kv_at(group, dir.path().join("snap-2"));
    assert!(reopened.get_current_snapshot().await.unwrap().is_some());
}

#[tokio::test]
async fn durable_checkpoint_backup_boundary_survives_restart_and_new_full_restores_rows() {
    use nodus_backup::{BackupObject, BackupOrchestrator, FsBackupRepository};
    let dir = tempfile::tempdir().unwrap();
    let repo = Arc::new(FsBackupRepository::new(dir.path().join("backup")));
    let objects = |value: &str, ts: u64| {
        vec![BackupObject {
            name: "kv_data.json".into(),
            bytes: Bytes::from(
                serde_json::to_vec(&serde_json::json!([
                    {"key": b"row".to_vec(), "value": value.as_bytes(), "version": ts},
                    {"key": b"index".to_vec(), "value": value.as_bytes(), "version": ts}
                ]))
                .unwrap(),
            ),
        }]
    };
    let base;
    let generation;
    {
        let kv = Arc::new(LsmKvEngine::with_wal(dir.path().join("kv"), None).unwrap());
        put(kv.as_ref(), b"row", b"old", 10);
        let backup = BackupOrchestrator::new(repo.clone()).with_recovery_source(kv.clone());
        base = backup
            .create_full_backup("local", 10, 1, 1, objects("old", 10))
            .await
            .unwrap();
        kv.replace_snapshot(
            &SnapshotScope {
                exclude_raft: true,
                ..Default::default()
            },
            vec![
                SnapshotRow::committed(Bytes::from_static(b"row"), Bytes::from_static(b"new"), 20),
                SnapshotRow::committed(
                    Bytes::from_static(b"index"),
                    Bytes::from_static(b"new"),
                    20,
                ),
            ],
            vec![],
        )
        .unwrap();
        generation = kv.recovery_generation().unwrap().unwrap();
        assert!(generation.wal_floor > 0);
    }
    let kv = Arc::new(LsmKvEngine::with_wal(dir.path().join("kv"), None).unwrap());
    assert_eq!(kv.recovery_generation().unwrap(), Some(generation));
    let backup = BackupOrchestrator::new(repo.clone()).with_recovery_source(kv.clone());
    let mut data = objects("new", 20);
    backup
        .seal_export(backup.capture_generation().unwrap(), &mut data)
        .unwrap();
    assert!(
        backup
            .create_incremental_backup("local", &base.backup_id, 20, 1, 1, data.clone())
            .await
            .is_err()
    );
    assert!(backup.plan_pitr_restore(20).await.is_err());
    let fresh = backup
        .create_full_backup("local", 20, 1, 1, data)
        .await
        .unwrap();
    // An older WAL may contain future/skewed timestamps. It must not enter
    // replay just because a newly installed snapshot has lower timestamps.
    backup
        .archive_wal_indexed(
            "0.log",
            Bytes::from_static(b"old-wal-not-decodable"),
            vec!["old-txn".into()],
            vec![nodus_backup::WalCommittedTxn {
                txn_id: "old-txn".into(),
                commit_ts: 30,
            }],
            None,
        )
        .await
        .unwrap();
    assert!(
        backup
            .plan_pitr_restore(30)
            .await
            .unwrap()
            .wal_segments
            .is_empty()
    );
    let offline = BackupOrchestrator::new(repo);
    let objects = offline.restore(&fresh.backup_id).await.unwrap();
    {
        let restored = LsmKvEngine::with_wal(dir.path().join("restored"), None).unwrap();
        BackupOrchestrator::restore_backup_objects_to_kv(&objects, &restored).unwrap();
    }
    let restored = LsmKvEngine::with_wal(dir.path().join("restored"), None).unwrap();
    assert_eq!(restored.get(b"row", 20).unwrap().unwrap().as_ref(), b"new");
    assert_eq!(
        restored.get(b"index", 20).unwrap().unwrap().as_ref(),
        b"new"
    );
    assert!(
        restored.recovery_generation().unwrap().is_none(),
        "a backup's source identity is not node-local restore state"
    );
}
