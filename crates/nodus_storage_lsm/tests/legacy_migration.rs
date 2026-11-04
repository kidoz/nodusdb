use bytes::Bytes;
use nodus_mvcc::VersionChain;
use nodus_storage_api::{KvEngine, TxnId};
use nodus_storage_lsm::{LsmKvEngine, migrate_legacy_manifest, sstable::Sstable};
use nodus_storage_wal::{FileWalEngine, WalEngine, WalRecord, WalRecordV1};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

fn image(dir: &Path) -> BTreeMap<PathBuf, Vec<u8>> {
    std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| {
            let path = entry.unwrap().path();
            let bytes = std::fs::read(&path).unwrap();
            (path, bytes)
        })
        .collect()
}

fn seed(dir: &Path, key: Option<[u8; 32]>) {
    let mut chain = VersionChain::new();
    let txn = TxnId::new();
    chain.write_intent(txn, b"flushed".to_vec()).unwrap();
    chain.commit(txn, 10).unwrap();
    Sstable::build(
        dir.join("1.sst"),
        &BTreeMap::from([(Bytes::from("sst"), chain)]),
    )
    .unwrap();
    let wal = FileWalEngine::with_encryption(dir.join("2.log"), key).unwrap();
    let txn = TxnId::new();
    wal.append(WalRecord::V1(WalRecordV1::WriteIntent {
        txn_id: txn,
        key: b"wal".to_vec(),
        value: b"replayed".to_vec(),
        started_at: None,
    }))
    .unwrap();
    wal.append(WalRecord::V1(WalRecordV1::CommitTxn {
        txn_id: txn,
        commit_ts: 20,
    }))
    .unwrap();
    wal.sync().unwrap();
}

#[test]
fn explicit_migration_validates_plaintext_and_encrypted_legacy_stores() {
    for key in [None, Some([42; 32])] {
        let dir = tempfile::tempdir().unwrap();
        seed(dir.path(), key);
        let before = image(dir.path());
        assert!(LsmKvEngine::with_wal(dir.path(), key).is_err());
        assert!(image(dir.path()) == before);
        migrate_legacy_manifest(dir.path(), &[1], 2, key).unwrap();
        for (path, bytes) in &before {
            assert_eq!(std::fs::read(path).unwrap(), *bytes);
        }
        let engine = LsmKvEngine::with_wal(dir.path(), key).unwrap();
        assert_eq!(
            engine.get(b"sst", 30).unwrap(),
            Some(Bytes::from("flushed"))
        );
        assert_eq!(
            engine.get(b"wal", 30).unwrap(),
            Some(Bytes::from("replayed"))
        );
        drop(engine);
        let migrated = image(dir.path());
        assert!(migrate_legacy_manifest(dir.path(), &[1], 2, key).is_err());
        assert!(
            image(dir.path()) == migrated,
            "existing manifest was replaced"
        );
    }
}

#[test]
fn migration_validation_failures_preserve_every_file() {
    for case in [
        "missing_sst",
        "duplicate_ids",
        "wrong_active",
        "orphan_sst",
        "missing_wal",
        "truncated_sst",
        "torn_wal",
        "bad_crc",
        "modern_header",
        "modern_batch",
        "manifest_tmp",
        "snapshot_tmp",
        "staging_tmp",
    ] {
        let dir = tempfile::tempdir().unwrap();
        seed(dir.path(), None);
        let mut ids = vec![1];
        let mut active = 2;
        match case {
            "missing_sst" => {
                std::fs::remove_file(dir.path().join("1.sst")).unwrap();
            }
            "duplicate_ids" => ids.push(1),
            "wrong_active" => active = 3,
            "orphan_sst" => {
                std::fs::copy(dir.path().join("1.sst"), dir.path().join("999.sst")).unwrap();
            }
            "missing_wal" => {
                std::fs::remove_file(dir.path().join("2.log")).unwrap();
            }
            "truncated_sst" => {
                std::fs::write(dir.path().join("1.sst"), b"broken").unwrap();
            }
            "torn_wal" | "bad_crc" => {
                let path = dir.path().join("2.log");
                let mut bytes = std::fs::read(&path).unwrap();
                if case == "torn_wal" {
                    bytes.extend_from_slice(&[1, 2]);
                } else {
                    bytes[9] ^= 0x80;
                }
                std::fs::write(path, bytes).unwrap();
            }
            "modern_header" | "modern_batch" => {
                let wal = FileWalEngine::new(dir.path().join("2.log")).unwrap();
                let record = if case == "modern_header" {
                    WalRecordV1::SegmentHeader {
                        predecessor: Some(1),
                    }
                } else {
                    WalRecordV1::CommitTxnBatchV1 {
                        commits: vec![(TxnId::new(), 50)],
                    }
                };
                wal.append(WalRecord::V1(record)).unwrap();
                wal.sync().unwrap();
            }
            "manifest_tmp" | "snapshot_tmp" | "staging_tmp" => {
                let name = match case {
                    "manifest_tmp" => "MANIFEST.tmp",
                    "snapshot_tmp" => "SNAPSHOT_MANIFEST.tmp",
                    _ => "LEGACY_MANIFEST.tmp",
                };
                std::fs::write(dir.path().join(name), b"publication residue").unwrap();
            }
            _ => unreachable!(),
        }
        let before = image(dir.path());
        assert!(
            migrate_legacy_manifest(dir.path(), &ids, active, None).is_err(),
            "case {case}"
        );
        assert!(
            image(dir.path()) == before,
            "case {case} changed data files"
        );
    }
}

#[test]
fn wrong_or_missing_migration_key_preserves_encrypted_wal() {
    let dir = tempfile::tempdir().unwrap();
    seed(dir.path(), Some([42; 32]));
    let before = image(dir.path());
    for key in [None, Some([7; 32])] {
        assert!(migrate_legacy_manifest(dir.path(), &[1], 2, key).is_err());
        assert!(image(dir.path()) == before);
    }
    migrate_legacy_manifest(dir.path(), &[1], 2, Some([42; 32])).unwrap();
}

#[cfg(unix)]
#[test]
fn migration_refuses_symlinked_data_files() {
    let dir = tempfile::tempdir().unwrap();
    seed(dir.path(), None);
    let outside = tempfile::NamedTempFile::new().unwrap();
    std::fs::copy(dir.path().join("2.log"), outside.path()).unwrap();
    std::fs::remove_file(dir.path().join("2.log")).unwrap();
    std::os::unix::fs::symlink(outside.path(), dir.path().join("2.log")).unwrap();
    let before = std::fs::read(outside.path()).unwrap();
    assert!(migrate_legacy_manifest(dir.path(), &[1], 2, None).is_err());
    assert_eq!(std::fs::read(outside.path()).unwrap(), before);
    assert!(!dir.path().join("MANIFEST").exists());
}
