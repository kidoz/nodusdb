//! Errors before a flush's manifest publication must preserve acknowledged rows.

use super::*;
use tempfile::TempDir;

fn put(engine: &LsmKvEngine, key: &'static [u8], value: &'static [u8], ts: Timestamp) {
    let txn = TxnId::new();
    engine
        .write_intent(txn, Bytes::from_static(key), Bytes::from_static(value))
        .unwrap();
    engine.commit(txn, ts).unwrap();
}

fn assert_rows(engine: &LsmKvEngine, retried: bool) {
    assert_eq!(engine.get(b"kept", 10).unwrap(), Some(Bytes::from("v1")));
    assert_eq!(engine.get(b"kept", 20).unwrap(), Some(Bytes::from("v2")));
    assert_eq!(
        engine.get(b"deleted", 10).unwrap(),
        Some(Bytes::from("old"))
    );
    assert_eq!(engine.get(b"deleted", 100).unwrap(), None);
    assert_eq!(engine.get(b"aborted", 100).unwrap(), None);

    let mut expected = vec![(Bytes::from("kept"), Bytes::from("v2"))];
    if retried {
        expected.push((Bytes::from("later"), Bytes::from("new")));
        expected.push((Bytes::from("pending"), Bytes::from("p")));
    }
    let range = || KeyRange {
        start: Bytes::new(),
        end: Bytes::from_static(b"\xff"),
    };
    let rows = engine
        .scan(range(), 100)
        .unwrap()
        .map(|row| row.map(|row| (row.key, row.value)))
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(rows, expected);
    let versions = engine
        .scan_versions(range(), 0, 100)
        .unwrap()
        .collect::<Result<Vec<_>>>()
        .unwrap();
    assert_eq!(versions.len(), if retried { 6 } else { 4 });
}

fn failed_flush_preserves_rows(blocker_name: &str) {
    for restart_before_retry in [false, true] {
        let dir = TempDir::new().unwrap();
        let mut engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();
        put(&engine, b"kept", b"v1", 10);
        put(&engine, b"kept", b"v2", 20);
        put(&engine, b"deleted", b"old", 10);
        let delete = TxnId::new();
        engine
            .delete_intent(delete, Bytes::from("deleted"))
            .unwrap();
        engine.commit(delete, 20).unwrap();
        let aborted = TxnId::new();
        engine
            .write_intent(aborted, Bytes::from("aborted"), Bytes::from("hidden"))
            .unwrap();
        engine.abort(aborted).unwrap();
        let pending = TxnId::new();
        engine
            .write_intent(pending, Bytes::from("pending"), Bytes::from("p"))
            .unwrap();
        assert_rows(&engine, false);

        let manifest_path = dir.path().join(MANIFEST_FILE);
        let before = std::fs::read(&manifest_path).unwrap();
        // A fresh engine uses WAL 1, then SSTable 2 and WAL 3 on its first
        // flush. A directory at the selected file path forces a real I/O error
        // without a test-only production fault hook or permission assumptions.
        let blocker = dir.path().join(blocker_name);
        std::fs::create_dir(&blocker).unwrap();
        let error = engine.flush().expect_err("blocked publication must fail");
        assert!(
            error.chain().any(|cause| cause.is::<std::io::Error>()),
            "expected an I/O failure, got {error:#}"
        );
        assert_eq!(std::fs::read(&manifest_path).unwrap(), before);
        assert_rows(&engine, false);
        std::fs::remove_dir(&blocker).unwrap();

        // No SSTable was named by the original manifest. Any completed output
        // from this failed flush is an orphan, not a required live read source.
        for entry in std::fs::read_dir(dir.path()).unwrap() {
            let path = entry.unwrap().path();
            if path.extension().is_some_and(|extension| extension == "sst") {
                std::fs::remove_file(path).unwrap();
            }
        }
        assert_rows(&engine, false);

        if restart_before_retry {
            drop(engine);
            engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();
            assert_rows(&engine, false);
        }
        // Resolve the retained intent before retrying so a successful flush can
        // advance the WAL replay floor. Replaying the old WAL must not be what
        // saves rows discarded by the failed attempt.
        engine.commit(pending, 30).unwrap();
        put(&engine, b"later", b"new", 40);
        engine.flush().unwrap();
        assert_rows(&engine, true);
        assert!(engine.reclaim_archived_wal_segment(1).unwrap());
        drop(engine);

        let recovered = LsmKvEngine::with_wal(dir.path(), None).unwrap();
        assert_rows(&recovered, true);
    }
}

#[test]
fn failed_sstable_flush_preserves_rows_through_retry_and_restart() {
    failed_flush_preserves_rows("2.sst.tmp");
}

#[test]
fn failed_wal_rotation_preserves_rows_through_retry_and_restart() {
    failed_flush_preserves_rows("3.log");
}

#[test]
fn failed_manifest_flush_preserves_rows_through_retry_and_restart() {
    failed_flush_preserves_rows("MANIFEST.tmp");
}
