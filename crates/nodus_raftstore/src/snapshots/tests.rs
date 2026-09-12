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
