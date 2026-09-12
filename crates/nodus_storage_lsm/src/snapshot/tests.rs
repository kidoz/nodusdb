use super::*;
use nodus_storage_api::NamespacedKvEngine;

fn put(kv: &dyn KvEngine, key: &'static [u8], value: &'static [u8], ts: u64) {
    let txn = TxnId::new();
    kv.write_intent(txn, Bytes::from_static(key), Bytes::from_static(value))
        .unwrap();
    kv.commit(txn, ts).unwrap();
}

fn seed(dir: &Path, encryption: Option<[u8; 32]>) -> TxnId {
    let kv = Arc::new(LsmKvEngine::with_wal(dir, encryption).unwrap());
    let a = NamespacedKvEngine::new(kv.clone(), "shard-a");
    let b = NamespacedKvEngine::new(kv.clone(), "shard-b");
    put(&a, b"row", b"old", 10);
    put(&a, b"orphan-index", b"old", 10);
    put(&a, b"\0raft\0vote", b"local-vote", 10);
    put(&b, b"row", b"first", 5);
    kv.flush().unwrap();
    put(&b, b"row", b"second", 10);
    put(kv.as_ref(), b"\0hlc\0clock", b"local-clock", 10);
    let txn = TxnId::new();
    b.write_intent(
        txn,
        Bytes::from_static(b"row"),
        Bytes::from_static(b"pending"),
    )
    .unwrap();
    b.delete_intent(txn, Bytes::from_static(b"deleted"))
        .unwrap();
    kv.flush().unwrap();
    txn
}

fn replace(kv: Arc<LsmKvEngine>) {
    let a = NamespacedKvEngine::new(kv, "shard-a");
    a.replace_snapshot(
        &SnapshotScope {
            exclude_raft: true,
            ..Default::default()
        },
        vec![
            SnapshotRow::committed(Bytes::from_static(b"row"), Bytes::from_static(b"new"), 20),
            SnapshotRow::committed(
                Bytes::from_static(b"new-index"),
                Bytes::from_static(b"new"),
                20,
            ),
        ],
        vec![SnapshotRow::committed(
            Bytes::from_static(b"\0raft\0applied"),
            Bytes::from_static(b"new-pointer"),
            20,
        )],
    )
    .unwrap();
}

fn verify(dir: &Path, encryption: Option<[u8; 32]>, installed: bool, txn: TxnId) {
    let kv = Arc::new(LsmKvEngine::with_wal(dir, encryption).unwrap());
    let a = NamespacedKvEngine::new(kv.clone(), "shard-a");
    let b = NamespacedKvEngine::new(kv.clone(), "shard-b");
    assert_eq!(
        a.get(b"row", u64::MAX).unwrap().unwrap().as_ref(),
        if installed { b"new" } else { b"old" }
    );
    assert_eq!(
        a.get(b"orphan-index", u64::MAX).unwrap().is_none(),
        installed
    );
    assert_eq!(a.get(b"new-index", u64::MAX).unwrap().is_some(), installed);
    assert_eq!(
        a.get(b"\0raft\0applied", u64::MAX).unwrap().is_some(),
        installed
    );
    assert_eq!(
        a.get(b"\0raft\0vote", u64::MAX).unwrap().unwrap().as_ref(),
        b"local-vote"
    );
    assert_eq!(
        kv.get(b"\0hlc\0clock", u64::MAX).unwrap().unwrap().as_ref(),
        b"local-clock"
    );
    assert_eq!(b.get(b"row", 5).unwrap().unwrap().as_ref(), b"first");
    assert_eq!(b.get(b"row", 10).unwrap().unwrap().as_ref(), b"second");
    assert_eq!(b.pending_intent_keys(txn).len(), 2);
    b.commit(txn, 30).unwrap();
    assert_eq!(b.get(b"row", 30).unwrap().unwrap().as_ref(), b"pending");
    assert!(b.get(b"deleted", 30).unwrap().is_none());
    drop((a, b, kv));
    let kv = Arc::new(LsmKvEngine::with_wal(dir, encryption).unwrap());
    let b = NamespacedKvEngine::new(kv, "shard-b");
    assert_eq!(b.get(b"row", 30).unwrap().unwrap().as_ref(), b"pending");
}

#[test]
fn checkpoint_preserves_other_group_history_intents_and_local_records() {
    for encryption in [None, Some([42; 32])] {
        let dir = tempfile::tempdir().unwrap();
        let txn = seed(dir.path(), encryption);
        replace(Arc::new(
            LsmKvEngine::with_wal(dir.path(), encryption).unwrap(),
        ));
        verify(dir.path(), encryption, true, txn);
    }
}

#[test]
fn checkpoint_crash_child() {
    let Ok(dir) = std::env::var("NODUS_TEST_CHECKPOINT_DIR") else {
        return;
    };
    replace(Arc::new(LsmKvEngine::with_wal(dir, None).unwrap()));
    panic!("checkpoint crash hook was not reached");
}

#[test]
fn abrupt_exit_at_manifest_boundary_recovers_old_or_complete_checkpoint() {
    for boundary in ["before-manifest", "after-manifest"] {
        let dir = tempfile::tempdir().unwrap();
        let txn = seed(dir.path(), None);
        let status = std::process::Command::new(std::env::current_exe().unwrap())
            .args([
                "--exact",
                "snapshot::tests::checkpoint_crash_child",
                "--nocapture",
            ])
            .env("NODUS_TEST_CHECKPOINT_DIR", dir.path())
            .env("NODUS_TEST_CHECKPOINT_BOUNDARY", boundary)
            .status()
            .unwrap();
        assert_eq!(status.code(), Some(86), "fault must be reached");
        verify(dir.path(), None, boundary == "after-manifest", txn);
    }
}

#[test]
fn out_of_scope_replacement_is_rejected_without_mutation() {
    let kv = LsmKvEngine::new();
    put(&kv, b"keep", b"old", 1);
    assert!(
        kv.replace_snapshot(
            &SnapshotScope {
                prefix: b"owned".to_vec(),
                ..Default::default()
            },
            vec![SnapshotRow::committed(
                Bytes::from_static(b"keep"),
                Bytes::from_static(b"bad"),
                2
            )],
            vec![]
        )
        .is_err()
    );
    assert_eq!(kv.get(b"keep", 2).unwrap().unwrap().as_ref(), b"old");
}
