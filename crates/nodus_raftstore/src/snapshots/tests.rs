use super::*;
use nodus_storage_mem::MemKvEngine;

fn put(kv: &dyn KvEngine, key: &'static [u8], value: &'static [u8], ts: u64) {
    let txn = TxnId::new();
    kv.write_intent(txn, Bytes::from_static(key), Bytes::from_static(value))
        .unwrap();
    kv.commit(txn, ts).unwrap();
}

async fn wire(records: &[(&[u8], &[u8])]) -> Vec<u8> {
    let mut bytes = Vec::new();
    write_snapshot_header(&mut bytes, None).await.unwrap();
    for (key, value) in records {
        write_kv_record(&mut bytes, key, value, 20).await.unwrap();
    }
    bytes
}

#[tokio::test]
async fn malformed_payloads_preserve_rows_applied_and_current_snapshot() {
    let mut partial = wire(&[(b"incoming", b"new")]).await;
    partial.push(0); // A partial next key length used to be accepted as EOF.
    let mut excessive_length = wire(&[]).await;
    excessive_length.extend_from_slice(&u64::MAX.to_be_bytes());
    let mut invalid_flag = wire(&[]).await;
    invalid_flag[6] = 2;
    let mut future_version = wire(&[]).await;
    future_version[5] = 2;
    let mut truncated_value = wire(&[(b"incoming", b"new")]).await;
    truncated_value.truncate(truncated_value.len() - 9);
    for bytes in [
        partial,
        excessive_length,
        invalid_flag,
        future_version,
        truncated_value,
        wire(&[(b"same", b"one"), (b"same", b"two")]).await,
        wire(&[(b"z", b"one"), (b"a", b"two")]).await,
        wire(&[(b"\0raft\0vote", b"foreign-vote")]).await,
        wire(&[(b"\0hlc\0clock", b"foreign-clock")]).await,
        wire(&[(b"shard-b\0row", b"foreign-group")]).await,
    ] {
        let kv = Arc::new(MemKvEngine::new());
        put(kv.as_ref(), b"old", b"keep", 10);
        let mut store = NodusRaftStore::with_kv(kv.clone());
        store.state_machine.write().await.meta_store =
            Some(Arc::new(nodus_meta::MemMetaStore::new()));
        let original = store.build_snapshot().await.unwrap();
        let old_applied = store.state_machine.read().await.last_applied_log;
        let meta = SnapshotMeta {
            last_log_id: Some(LogId::new(openraft::CommittedLeaderId::new(1, 1), 20)),
            snapshot_id: "invalid".into(),
            ..original.meta.clone()
        };
        let mut file = store.begin_receiving_snapshot().await.unwrap();
        file.write_all(&bytes).await.unwrap();
        file.flush().await.unwrap();
        assert!(store.install_snapshot(&meta, file).await.is_err());
        assert_eq!(kv.get(b"old", u64::MAX).unwrap().unwrap().as_ref(), b"keep");
        assert!(kv.get(b"incoming", u64::MAX).unwrap().is_none());
        assert_eq!(
            store.state_machine.read().await.last_applied_log,
            old_applied
        );
        assert_eq!(
            store
                .get_current_snapshot()
                .await
                .unwrap()
                .unwrap()
                .meta
                .snapshot_id,
            original.meta.snapshot_id
        );
        assert!(
            !std::fs::read_dir(&store.snapshot_dir)
                .unwrap()
                .any(|entry| entry
                    .unwrap()
                    .file_name()
                    .to_string_lossy()
                    .starts_with("install-")),
            "rejected files are cleaned up"
        );
    }
}

#[tokio::test]
async fn legacy_build_refuses_intents_history_and_tombstones_without_replacing_snapshot() {
    for state in ["intent", "history", "tombstone"] {
        let kv = Arc::new(MemKvEngine::new());
        put(kv.as_ref(), b"row", b"first", 10);
        let mut store = NodusRaftStore::with_kv(kv.clone());
        let original = store.build_snapshot().await.unwrap();
        let txn = TxnId::new();
        if state == "tombstone" {
            kv.delete_intent(txn, Bytes::from_static(b"row")).unwrap();
        } else {
            kv.write_intent(
                txn,
                Bytes::from_static(b"row"),
                Bytes::from_static(b"second"),
            )
            .unwrap();
        }
        if state != "intent" {
            kv.commit(txn, 20).unwrap();
        }
        assert!(
            store.build_snapshot().await.is_err(),
            "legacy wire cannot represent {state}"
        );
        assert_eq!(
            store
                .get_current_snapshot()
                .await
                .unwrap()
                .unwrap()
                .meta
                .snapshot_id,
            original.meta.snapshot_id
        );
        assert_eq!(kv.get(b"row", 10).unwrap().unwrap().as_ref(), b"first");
        if state == "intent" {
            kv.commit(txn, 20).unwrap();
        }
    }
}

#[tokio::test]
async fn catalog_kind_mismatch_is_rejected_before_replacing_data() {
    let kv = Arc::new(MemKvEngine::new());
    put(kv.as_ref(), b"old", b"keep", 10);
    let mut store = NodusRaftStore::with_kv(kv.clone());
    let mut bytes = Vec::new();
    write_snapshot_header(
        &mut bytes,
        Some(&serde_json::to_vec(&nodus_catalog::CatalogSnapshot::default()).unwrap()),
    )
    .await
    .unwrap();
    let mut file = store.begin_receiving_snapshot().await.unwrap();
    file.write_all(&bytes).await.unwrap();
    file.flush().await.unwrap();
    let meta = SnapshotMeta {
        last_log_id: None,
        last_membership: Default::default(),
        snapshot_id: "meta-in-data-group".into(),
    };
    assert!(store.install_snapshot(&meta, file).await.is_err());
    assert_eq!(kv.get(b"old", u64::MAX).unwrap().unwrap().as_ref(), b"keep");
}

#[tokio::test]
async fn higher_term_snapshot_cannot_move_applied_index_backwards() {
    let kv = Arc::new(MemKvEngine::new());
    put(kv.as_ref(), b"old", b"keep", 10);
    let mut store = NodusRaftStore::with_kv(kv.clone());
    let applied = LogId::new(openraft::CommittedLeaderId::new(1, 1), 10);
    store.state_machine.write().await.last_applied_log = Some(applied);
    let meta = SnapshotMeta {
        last_log_id: Some(LogId::new(openraft::CommittedLeaderId::new(2, 1), 5)),
        last_membership: Default::default(),
        snapshot_id: "regression".into(),
    };
    let mut file = store.begin_receiving_snapshot().await.unwrap();
    file.write_all(&wire(&[]).await).await.unwrap();
    file.flush().await.unwrap();
    assert!(store.install_snapshot(&meta, file).await.is_err());
    assert_eq!(
        store.state_machine.read().await.last_applied_log,
        Some(applied)
    );
    assert_eq!(kv.get(b"old", 10).unwrap().unwrap().as_ref(), b"keep");
}

#[test]
fn recovery_purges_conflicting_log_term_covered_by_checkpoint() {
    let old = LogId::new(openraft::CommittedLeaderId::new(1, 1), 1);
    let new = LogId::new(openraft::CommittedLeaderId::new(2, 1), 1);
    let mut log = BTreeMap::from([(
        1,
        Entry {
            log_id: old,
            payload: openraft::EntryPayload::Blank,
        },
    )]);
    let mut applied = AppliedState {
        last_applied: Some(new),
        ..Default::default()
    };
    assert!(reconcile_torn_recovery(&mut log, &mut applied));
    assert_eq!(applied.last_applied, Some(new));
    assert_eq!(applied.last_purged, Some(new));
    assert!(log.is_empty());
}

struct Compatibility {
    finalized: std::sync::atomic::AtomicU64,
    formats: std::sync::Mutex<BTreeMap<u64, u16>>,
}
impl SnapshotCompatibility for Compatibility {
    fn finalized_cluster_version(&self) -> anyhow::Result<u64> {
        Ok(self.finalized.load(Ordering::SeqCst))
    }
    fn member_snapshot_versions(&self) -> anyhow::Result<BTreeMap<u64, u16>> {
        Ok(self.formats.lock().unwrap().clone())
    }
}

async fn mvcc_source() -> (NodusRaftStore, Arc<MemKvEngine>, Arc<Compatibility>, TxnId) {
    let kv = Arc::new(MemKvEngine::new());
    put(kv.as_ref(), b"row", b"old", 10);
    put(kv.as_ref(), b"row", b"new", 20);
    let pending = TxnId::new();
    kv.delete_intent(pending, Bytes::from_static(b"row"))
        .unwrap();
    put(kv.as_ref(), b"deleted", b"old", 10);
    let deleted = TxnId::new();
    kv.delete_intent(deleted, Bytes::from_static(b"deleted"))
        .unwrap();
    kv.commit(deleted, 20).unwrap();
    let compatibility = Arc::new(Compatibility {
        finalized: std::sync::atomic::AtomicU64::new(1),
        formats: std::sync::Mutex::new(BTreeMap::from([(1, 2), (2, 1)])),
    });
    let store = NodusRaftStore::with_kv(kv.clone())
        .with_snapshot_group("shard-a")
        .with_snapshot_compatibility(compatibility.clone());
    store.state_machine.write().await.last_membership = StoredMembership::new(
        None,
        openraft::Membership::new(
            vec![std::collections::BTreeSet::from([1])],
            BTreeMap::from([
                (1, openraft::BasicNode::new("one")),
                (2, openraft::BasicNode::new("learner")),
            ]),
        ),
    );
    (store, kv, compatibility, pending)
}

#[tokio::test]
async fn mvcc_snapshots_require_finalization_and_every_voter_and_learner() {
    let (mut source, _, compatibility, pending) = mvcc_source().await;
    assert!(source.build_snapshot().await.is_err());
    compatibility.finalized.store(2, Ordering::SeqCst);
    assert!(
        source.build_snapshot().await.is_err(),
        "old learner blocks v2"
    );
    compatibility.formats.lock().unwrap().insert(2, 2);
    let snapshot = source.build_snapshot().await.unwrap();
    let target = Arc::new(MemKvEngine::new());
    let mut receiver = NodusRaftStore::with_kv(target.clone()).with_snapshot_group("shard-a");
    receiver
        .install_snapshot(&snapshot.meta, snapshot.snapshot)
        .await
        .unwrap();
    assert_eq!(target.get(b"row", 10).unwrap().unwrap().as_ref(), b"old");
    assert_eq!(target.get(b"row", 20).unwrap().unwrap().as_ref(), b"new");
    assert!(target.get(b"deleted", 20).unwrap().is_none());
    assert_eq!(
        target.get(b"deleted", 10).unwrap().unwrap().as_ref(),
        b"old"
    );
    assert_eq!(
        target.pending_intent_keys(pending),
        vec![Bytes::from_static(b"row")]
    );
    target.commit(pending, 30).unwrap();
    assert!(target.get(b"row", 30).unwrap().is_none());
    compatibility.formats.lock().unwrap().remove(&2);
    assert!(
        source.build_snapshot().await.is_err(),
        "missing report must not count as support"
    );
}

#[tokio::test]
async fn mvcc_payload_checksum_group_and_metadata_mismatch_leave_old_state() {
    let (mut source, _, compatibility, _) = mvcc_source().await;
    compatibility.finalized.store(2, Ordering::SeqCst);
    compatibility.formats.lock().unwrap().insert(2, 2);
    let snapshot = source.build_snapshot().await.unwrap();
    let mut bytes = Vec::new();
    snapshot
        .snapshot
        .take(SNAPSHOT_MEMORY_LIMIT as u64)
        .read_to_end(&mut bytes)
        .await
        .unwrap();
    assert_eq!(&bytes[..6], b"NSNP\0\x02");
    for case in ["checksum", "metadata", "group", "truncated"] {
        let kv = Arc::new(MemKvEngine::new());
        put(kv.as_ref(), b"old", b"keep", 10);
        let mut store =
            NodusRaftStore::with_kv(kv.clone()).with_snapshot_group(if case == "group" {
                "shard-b"
            } else {
                "shard-a"
            });
        let mut meta = snapshot.meta.clone();
        let mut payload = bytes.clone();
        if case == "checksum" {
            payload[20] ^= 1;
        }
        if case == "metadata" {
            meta.snapshot_id = "tampered".into();
        }
        if case == "truncated" {
            payload.pop();
        }
        let mut file = store.begin_receiving_snapshot().await.unwrap();
        file.write_all(&payload).await.unwrap();
        file.flush().await.unwrap();
        assert!(store.install_snapshot(&meta, file).await.is_err());
        assert_eq!(kv.get(b"old", 10).unwrap().unwrap().as_ref(), b"keep");
        assert!(kv.get(b"row", 20).unwrap().is_none());
        assert!(kv.recovery_generation().unwrap().is_none());
    }
}

#[test]
fn mvcc_codec_rejects_conflicting_intents_and_duplicate_timestamps() {
    use nodus_storage_api::SnapshotValue;
    let mut row =
        SnapshotRow::committed(Bytes::from_static(b"row"), Bytes::from_static(b"one"), 10);
    row.versions.push(row.versions[0].clone());
    row.versions[1].value = Some(b"conflicting".to_vec());
    assert!(v2::canonicalize(std::slice::from_mut(&mut row)).is_err());
    row.versions = vec![SnapshotValue {
        value: None,
        version: 12,
        txn_id: Some(TxnId::new()),
        is_intent: true,
    }];
    assert!(v2::canonicalize(std::slice::from_mut(&mut row)).is_err());
}
