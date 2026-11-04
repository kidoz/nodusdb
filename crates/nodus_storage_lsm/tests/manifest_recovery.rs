use bytes::Bytes;
use nodus_mvcc::VersionChain;
use nodus_storage_api::{KeyRange, KvEngine, TxnId};
use nodus_storage_lsm::{LsmKvEngine, sstable::Sstable};
use nodus_storage_wal::{FileWalEngine, WalEngine, WalRecord, WalRecordV1};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

#[derive(Debug, PartialEq, Eq)]
enum EntryImage {
    File(Vec<u8>),
    Directory,
    Symlink(PathBuf),
}

fn directory_image(dir: &Path) -> BTreeMap<PathBuf, EntryImage> {
    fn visit(dir: &Path, image: &mut BTreeMap<PathBuf, EntryImage>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let entry = entry.unwrap();
            let path = entry.path();
            let kind = entry.file_type().unwrap();
            let contents = if kind.is_symlink() {
                EntryImage::Symlink(std::fs::read_link(&path).unwrap())
            } else if kind.is_dir() {
                visit(&path, image);
                EntryImage::Directory
            } else {
                EntryImage::File(std::fs::read(&path).unwrap())
            };
            image.insert(path, contents);
        }
    }
    let mut image = BTreeMap::new();
    visit(dir, &mut image);
    image
}

fn put(engine: &LsmKvEngine, key: &'static str, value: &'static str, ts: u64) {
    let txn = TxnId::new();
    engine
        .write_intent(txn, Bytes::from(key), Bytes::from(value))
        .unwrap();
    engine.commit(txn, ts).unwrap();
}

fn populated_directory(dir: &Path) -> Vec<u8> {
    {
        let engine = LsmKvEngine::with_wal(dir, None).unwrap();
        put(&engine, "flushed", "first", 10);
        put(&engine, "deleted", "old", 10);
        let spanning = TxnId::new();
        engine
            .write_intent(spanning, Bytes::from("spanning"), Bytes::from("second"))
            .unwrap();
        engine.flush().unwrap();
        engine.commit(spanning, 20).unwrap();
        put(&engine, "wal-only", "third", 30);
        let delete = TxnId::new();
        engine
            .delete_intent(delete, Bytes::from("deleted"))
            .unwrap();
        engine.commit(delete, 40).unwrap();
        let aborted = TxnId::new();
        engine
            .write_intent(aborted, Bytes::from("aborted"), Bytes::from("hidden"))
            .unwrap();
        engine.abort(aborted).unwrap();
    }
    // A complete but unpublished output must never become authoritative just
    // because the real manifest cannot be read. Its high id also makes the
    // directory-scan fallback guess the wrong active WAL.
    let mut ghost = VersionChain::new();
    let txn = TxnId::new();
    ghost.write_intent(txn, b"unpublished".to_vec()).unwrap();
    ghost.commit(txn, 50).unwrap();
    Sstable::build(
        dir.join("999.sst"),
        &BTreeMap::from([(Bytes::from("ghost"), ghost)]),
    )
    .unwrap();
    std::fs::read(dir.join("MANIFEST")).unwrap()
}

fn assert_rows(engine: &LsmKvEngine) {
    let rows = engine
        .scan(
            KeyRange {
                start: Bytes::new(),
                end: Bytes::from_static(b"\xff"),
            },
            100,
        )
        .unwrap()
        .map(|row| {
            let row = row.unwrap();
            (row.key, row.value)
        })
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        vec![
            (Bytes::from("flushed"), Bytes::from("first")),
            (Bytes::from("spanning"), Bytes::from("second")),
            (Bytes::from("wal-only"), Bytes::from("third")),
        ]
    );
    assert_eq!(
        engine.get(b"deleted", 10).unwrap(),
        Some(Bytes::from("old"))
    );
    assert_eq!(engine.get(b"deleted", 100).unwrap(), None);
    assert_eq!(engine.get(b"aborted", 100).unwrap(), None);
    assert_eq!(engine.get(b"ghost", 100).unwrap(), None);
}

fn rejected_without_changes(dir: &Path, expected: &str) -> anyhow::Error {
    let before = directory_image(dir);
    let result = LsmKvEngine::with_wal(dir, None);
    assert!(
        directory_image(dir) == before,
        "failed startup changed data files"
    );
    let error = result.err().expect("unusable manifest must reject startup");
    let message = error.to_string();
    assert!(message.contains(expected), "{error:#}");
    assert!(
        message.contains(&dir.join("MANIFEST").display().to_string()),
        "{error:#}"
    );
    error
}

#[test]
fn corrupt_manifest_rejects_orphans_and_preserves_wal_only_commits() {
    for invalid in [
        b"".as_slice(),
        b"{ this is not valid json",
        br#"{"active_wal":3,"sstables":[2]"#,
        br#"{"sstables":[]}"#,
        br#"{"active_wal":"wrong type","sstables":[]}"#,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let manifest = populated_directory(dir.path());
        std::fs::write(dir.path().join("MANIFEST"), invalid).unwrap();
        rejected_without_changes(dir.path(), "decode");

        // Restoring the authoritative manifest must recover exactly the
        // committed rows, without adopting the orphan or resurrecting deletes.
        std::fs::write(dir.path().join("MANIFEST"), manifest).unwrap();
        let recovered = LsmKvEngine::with_wal(dir.path(), None).unwrap();
        assert_rows(&recovered);
        put(&recovered, "later", "fourth", 110);
        drop(recovered);
        let reopened = LsmKvEngine::with_wal(dir.path(), None).unwrap();
        assert_rows(&reopened);
        assert_eq!(
            reopened.get(b"later", 120).unwrap(),
            Some(Bytes::from("fourth"))
        );
    }
}

#[test]
fn unreadable_manifest_rejects_startup_before_creating_files() {
    let dir = tempfile::tempdir().unwrap();
    let manifest = populated_directory(dir.path());
    let path = dir.path().join("MANIFEST");
    std::fs::remove_file(&path).unwrap();
    // A directory forces a real read error regardless of UID/permission rules.
    std::fs::create_dir(&path).unwrap();
    std::fs::write(path.join("sentinel"), b"preserve me").unwrap();
    let error = rejected_without_changes(dir.path(), "read");
    assert!(error.chain().any(|cause| cause.is::<std::io::Error>()));
    std::fs::remove_dir_all(&path).unwrap();
    std::fs::write(path, manifest).unwrap();
    assert_rows(&LsmKvEngine::with_wal(dir.path(), None).unwrap());
}

#[cfg(unix)]
#[test]
fn dangling_manifest_symlink_is_not_a_missing_legacy_manifest() {
    let dir = tempfile::tempdir().unwrap();
    let manifest = populated_directory(dir.path());
    let path = dir.path().join("MANIFEST");
    std::fs::remove_file(&path).unwrap();
    std::os::unix::fs::symlink("missing-manifest-target", &path).unwrap();
    rejected_without_changes(dir.path(), "read");
    std::fs::remove_file(&path).unwrap();
    std::fs::write(path, manifest).unwrap();
    assert_rows(&LsmKvEngine::with_wal(dir.path(), None).unwrap());
}

#[test]
fn absent_manifest_initializes_empty_and_new_directories() {
    let root = tempfile::tempdir().unwrap();
    for dir in [root.path().to_path_buf(), root.path().join("new-store")] {
        let engine = LsmKvEngine::with_wal(&dir, None).unwrap();
        put(&engine, "fresh", "durable", 10);
        drop(engine);
        assert!(dir.join("MANIFEST").is_file());
        let reopened = LsmKvEngine::with_wal(&dir, None).unwrap();
        assert_eq!(
            reopened.get(b"fresh", 20).unwrap(),
            Some(Bytes::from("durable"))
        );
    }
}

#[test]
fn pre_manifest_layout_recovers_and_publishes_authoritative_manifest() {
    let dir = tempfile::tempdir().unwrap();
    // Historical layout: SSTable N and active WAL N+1, without a manifest or
    // SegmentHeader records. Build the fixture independently of with_wal.
    let mut chain = VersionChain::new();
    let txn = TxnId::new();
    chain.write_intent(txn, b"flushed".to_vec()).unwrap();
    chain.commit(txn, 10).unwrap();
    Sstable::build(
        dir.path().join("1.sst"),
        &BTreeMap::from([(Bytes::from("sst"), chain)]),
    )
    .unwrap();
    {
        let wal = FileWalEngine::new(dir.path().join("2.log")).unwrap();
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
    std::fs::write(dir.path().join("99.sst.tmp"), b"unpublished partial file").unwrap();
    let original = directory_image(dir.path());
    let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();
    assert_eq!(
        engine.get(b"sst", 30).unwrap(),
        Some(Bytes::from("flushed"))
    );
    assert_eq!(
        engine.get(b"wal", 30).unwrap(),
        Some(Bytes::from("replayed"))
    );
    let manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dir.path().join("MANIFEST")).unwrap()).unwrap();
    assert_eq!(manifest["sstables"], serde_json::json!([1]));
    assert_eq!(manifest["active_wal"], 2);
    assert_eq!(manifest["min_replay_wal"], 0);
    for (path, bytes) in original {
        assert_eq!(directory_image(dir.path()).get(&path), Some(&bytes));
    }
    drop(engine);
    let reopened = LsmKvEngine::with_wal(dir.path(), None).unwrap();
    assert_eq!(
        reopened.get(b"wal", 30).unwrap(),
        Some(Bytes::from("replayed"))
    );
}

#[test]
fn legacy_manifest_without_replay_floor_recovers_cross_segment_commits() {
    let dir = tempfile::tempdir().unwrap();
    let manifest = populated_directory(dir.path());
    let mut legacy: serde_json::Value = serde_json::from_slice(&manifest).unwrap();
    legacy.as_object_mut().unwrap().remove("min_replay_wal");
    std::fs::write(
        dir.path().join("MANIFEST"),
        serde_json::to_vec(&legacy).unwrap(),
    )
    .unwrap();
    assert_rows(&LsmKvEngine::with_wal(dir.path(), None).unwrap());
}
