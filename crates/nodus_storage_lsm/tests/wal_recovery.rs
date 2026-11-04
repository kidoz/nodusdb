use bytes::Bytes;
use nodus_storage_api::{KvEngine, TxnId};
use nodus_storage_lsm::LsmKvEngine;
use nodus_storage_wal::{FileWalEngine, WalEngine, WalRecord, WalRecordV1};
use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};

fn directory_image(dir: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| {
            let path = entry.unwrap().path();
            let bytes = std::fs::read(&path).unwrap();
            (path, bytes)
        })
        .collect()
}

#[test]
fn failed_encrypted_startup_preserves_files_and_correct_key_recovers_commits() {
    let dir = tempfile::tempdir().unwrap();
    let key = Some([42; 32]);
    {
        let engine = LsmKvEngine::with_wal(dir.path(), key).unwrap();
        let first = TxnId::new();
        engine
            .write_intent(first, Bytes::from("flushed"), Bytes::from("first"))
            .unwrap();
        engine.commit(first, 10).unwrap();
        let spanning = TxnId::new();
        engine
            .write_intent(spanning, Bytes::from("spanning"), Bytes::from("second"))
            .unwrap();
        // Retain the older segment: this intent commits only after rotation.
        engine.flush().unwrap();
        engine.commit(spanning, 20).unwrap();
        let last = TxnId::new();
        engine
            .write_intent(last, Bytes::from("active"), Bytes::from("third"))
            .unwrap();
        engine.commit(last, 30).unwrap();
        let aborted = TxnId::new();
        engine
            .write_intent(aborted, Bytes::from("aborted"), Bytes::from("hidden"))
            .unwrap();
        engine.abort(aborted).unwrap();
    }
    let original = directory_image(dir.path());
    for bad_key in [Some([7; 32]), None] {
        let result = LsmKvEngine::with_wal(dir.path(), bad_key);
        assert_eq!(directory_image(dir.path()), original);
        let error = result.err().expect("incorrect key must reject LSM startup");
        assert!(error.to_string().contains("WAL"), "{error:#}");
    }

    let recovered = LsmKvEngine::with_wal(dir.path(), key).unwrap();
    for (name, value) in [
        ("flushed", "first"),
        ("spanning", "second"),
        ("active", "third"),
    ] {
        assert_eq!(
            recovered.get(name.as_bytes(), 40).unwrap(),
            Some(Bytes::from(value))
        );
    }
    assert_eq!(recovered.get(b"aborted", 40).unwrap(), None);
    let later = TxnId::new();
    recovered
        .write_intent(later, Bytes::from("later"), Bytes::from("fourth"))
        .unwrap();
    recovered.commit(later, 50).unwrap();
    drop(recovered);
    let reopened = LsmKvEngine::with_wal(dir.path(), key).unwrap();
    assert_eq!(
        reopened.get(b"later", 60).unwrap(),
        Some(Bytes::from("fourth"))
    );
    assert_eq!(reopened.get(b"aborted", 60).unwrap(), None);
}

#[test]
fn authentication_failure_in_predecessor_segment_rejects_startup_without_truncation() {
    let dir = tempfile::tempdir().unwrap();
    let key = Some([42; 32]);
    {
        let engine = LsmKvEngine::with_wal(dir.path(), key).unwrap();
        let txn = TxnId::new();
        engine
            .write_intent(txn, Bytes::from("spanning"), Bytes::from("durable"))
            .unwrap();
        engine.flush().unwrap();
        engine.commit(txn, 10).unwrap();
    }
    let predecessor = dir.path().join("1.log");
    let original_segment = std::fs::read(&predecessor).unwrap();
    {
        // Inject a complete CRC-valid frame that fails authentication only
        // after the active segment and the predecessor's valid prefix replay.
        let other = FileWalEngine::with_encryption(&predecessor, Some([7; 32])).unwrap();
        other
            .append(WalRecord::V1(WalRecordV1::Checkpoint { ts: 20 }))
            .unwrap();
        other.sync().unwrap();
    }
    // A repairable active tail must stay intact when an older required
    // segment fails authentication later in the preflight walk.
    let meta: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dir.path().join("MANIFEST")).unwrap()).unwrap();
    let active = meta["active_wal"].as_u64().unwrap();
    let mut tail = std::fs::OpenOptions::new()
        .append(true)
        .open(dir.path().join(format!("{active}.log")))
        .unwrap();
    tail.write_all(&[1, 2, 3]).unwrap();
    tail.sync_all().unwrap();
    drop(tail);
    let image = directory_image(dir.path());
    let result = LsmKvEngine::with_wal(dir.path(), key);
    assert_eq!(directory_image(dir.path()), image);
    let error = result
        .err()
        .expect("unreadable predecessor must reject startup");
    let message = error.to_string();
    assert!(message.contains("authentication failed"), "{message}");
    assert!(
        message.contains(&predecessor.display().to_string()),
        "{message}"
    );

    // Undo only the injected frame; the acknowledged cross-segment commit
    // must still be recoverable from the preserved files.
    std::fs::write(&predecessor, original_segment).unwrap();
    let recovered = LsmKvEngine::with_wal(dir.path(), key).unwrap();
    assert_eq!(
        recovered.get(b"spanning", 15).unwrap(),
        Some(Bytes::from("durable"))
    );
}
