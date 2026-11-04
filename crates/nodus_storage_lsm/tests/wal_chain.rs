use bytes::Bytes;
use nodus_storage_api::{KvEngine, TxnId};
use nodus_storage_lsm::LsmKvEngine;
use nodus_storage_wal::{FileWalEngine, WalEngine, WalRecord, WalRecordV1};
use std::collections::BTreeMap;
use std::io::Write;
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

fn manifest(dir: &Path) -> serde_json::Value {
    serde_json::from_slice(&std::fs::read(dir.join("MANIFEST")).unwrap()).unwrap()
}

fn save_manifest(dir: &Path, value: &serde_json::Value) {
    std::fs::write(dir.join("MANIFEST"), serde_json::to_vec(value).unwrap()).unwrap();
}

fn put(engine: &LsmKvEngine, key: &'static str, value: &'static str, ts: u64) {
    let txn = TxnId::new();
    engine
        .write_intent(txn, Bytes::from(key), Bytes::from(value))
        .unwrap();
    engine.commit(txn, ts).unwrap();
}

// The acknowledged spanning transaction needs the oldest segment's intent and
// the newest segment's commit, with a middle segment joining their lineage.
fn seed(dir: &Path) -> (u64, u64) {
    let engine = LsmKvEngine::with_wal(dir, None).unwrap();
    put(&engine, "flushed", "first", 10);
    let spanning = TxnId::new();
    engine
        .write_intent(spanning, Bytes::from("spanning"), Bytes::from("durable"))
        .unwrap();
    engine.flush().unwrap();
    let middle = manifest(dir)["active_wal"].as_u64().unwrap();
    put(&engine, "middle", "second", 20);
    engine.flush().unwrap();
    engine.commit(spanning, 30).unwrap();
    put(&engine, "tail", "third", 40);
    let aborted = TxnId::new();
    engine
        .write_intent(aborted, Bytes::from("aborted"), Bytes::from("hidden"))
        .unwrap();
    engine.abort(aborted).unwrap();
    let active = manifest(dir)["active_wal"].as_u64().unwrap();
    assert_eq!(manifest(dir)["min_replay_wal"], 0);
    (active, middle)
}

fn assert_rows(engine: &LsmKvEngine) {
    for (key, value) in [
        ("flushed", "first"),
        ("middle", "second"),
        ("spanning", "durable"),
        ("tail", "third"),
    ] {
        assert_eq!(
            engine.get(key.as_bytes(), 100).unwrap(),
            Some(Bytes::from(value)),
            "key {key}"
        );
    }
    assert_eq!(engine.get(b"aborted", 100).unwrap(), None);
}

fn rejected_without_changes(dir: &Path) -> String {
    let before = image(dir);
    let result = LsmKvEngine::with_wal(dir, None);
    assert!(
        image(dir) == before,
        "failed WAL-chain validation changed files"
    );
    let error = result
        .err()
        .expect("incomplete/invalid WAL chain must reject startup");
    format!("{error:#}")
}

fn rewrite(dir: &Path, id: u64, records: Vec<WalRecord>) {
    let path = dir.join(format!("{id}.log"));
    std::fs::write(&path, []).unwrap();
    let wal = FileWalEngine::new(path).unwrap();
    for record in records {
        wal.append(record).unwrap();
    }
    wal.sync().unwrap();
}

fn header(predecessor: Option<u64>) -> WalRecord {
    WalRecord::V1(WalRecordV1::SegmentHeader { predecessor })
}

#[test]
fn missing_active_middle_or_root_wal_rejects_startup_and_restores_commits() {
    for missing in ["active", "middle", "root"] {
        let dir = tempfile::tempdir().unwrap();
        let (active, middle) = seed(dir.path());
        let id = match missing {
            "active" => active,
            "middle" => middle,
            _ => 1,
        };
        let path = dir.path().join(format!("{id}.log"));
        let original = std::fs::read(&path).unwrap();
        std::fs::remove_file(&path).unwrap();
        let message = rejected_without_changes(dir.path());
        assert!(message.contains(&path.display().to_string()), "{message}");
        assert!(!path.exists(), "recovery created a missing required WAL");
        std::fs::write(&path, original).unwrap();
        assert_rows(&LsmKvEngine::with_wal(dir.path(), None).unwrap());
    }
}

#[test]
fn missing_predecessor_does_not_repair_active_tail_until_chain_is_valid() {
    let dir = tempfile::tempdir().unwrap();
    let (active, middle) = seed(dir.path());
    let missing = dir.path().join(format!("{middle}.log"));
    let original = std::fs::read(&missing).unwrap();
    std::fs::remove_file(&missing).unwrap();
    let active_path = dir.path().join(format!("{active}.log"));
    let valid_len = std::fs::metadata(&active_path).unwrap().len();
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&active_path)
        .unwrap();
    file.write_all(&[1, 2, 3]).unwrap();
    file.sync_all().unwrap();
    drop(file);
    rejected_without_changes(dir.path());
    std::fs::write(missing, original).unwrap();
    let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();
    assert_eq!(std::fs::metadata(&active_path).unwrap().len(), valid_len);
    assert_rows(&engine);
    put(&engine, "later", "fourth", 50);
    drop(engine);
    let reopened = LsmKvEngine::with_wal(dir.path(), None).unwrap();
    assert_rows(&reopened);
    assert_eq!(
        reopened.get(b"later", 100).unwrap(),
        Some(Bytes::from("fourth"))
    );
}

#[test]
fn malformed_segment_headers_and_links_reject_without_modifying_files() {
    for case in [
        "self",
        "forward",
        "cycle",
        "zero",
        "duplicate",
        "misplaced",
        "premature_root",
        "skipped_floor",
        "missing_header",
        "missing_floor_header",
    ] {
        let dir = tempfile::tempdir().unwrap();
        let (active, middle) = seed(dir.path());
        let original = image(dir.path());
        let mut records =
            FileWalEngine::inspect(dir.path().join(format!("{active}.log")), None).unwrap();
        let mut meta = manifest(dir.path());
        match case {
            "self" => records[0] = header(Some(active)),
            "forward" => records[0] = header(Some(active + 1)),
            "cycle" => {
                let root = FileWalEngine::inspect(dir.path().join("1.log"), None).unwrap();
                rewrite(
                    dir.path(),
                    1,
                    std::iter::once(header(Some(active))).chain(root).collect(),
                );
            }
            "zero" => records[0] = header(Some(0)),
            "duplicate" => records.push(header(Some(middle))),
            "misplaced" => records.insert(0, WalRecord::V1(WalRecordV1::Checkpoint { ts: 0 })),
            "premature_root" => {
                records[0] = header(None);
                meta["min_replay_wal"] = 1.into();
            }
            "skipped_floor" => {
                records[0] = header(Some(1));
                meta["min_replay_wal"] = middle.into();
            }
            "missing_header" => {
                records.remove(0);
                meta["min_replay_wal"] = 1.into();
            }
            "missing_floor_header" => {
                let mut floor_records =
                    FileWalEngine::inspect(dir.path().join(format!("{middle}.log")), None).unwrap();
                floor_records.remove(0);
                rewrite(dir.path(), middle, floor_records);
                meta["min_replay_wal"] = middle.into();
            }
            _ => unreachable!(),
        }
        rewrite(dir.path(), active, records);
        save_manifest(dir.path(), &meta);
        let message = rejected_without_changes(dir.path());
        assert!(message.contains("WAL"), "case {case}: {message}");
        for (path, bytes) in original {
            std::fs::write(path, bytes).unwrap();
        }
        assert_rows(&LsmKvEngine::with_wal(dir.path(), None).unwrap());
    }
}

#[test]
fn replay_floor_allows_reclaimed_predecessors_and_ignores_orphan_wals() {
    for reclaim in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();
        put(&engine, "flushed", "durable", 10);
        engine.flush().unwrap();
        put(&engine, "tail", "active", 20);
        let active = manifest(dir.path())["active_wal"].as_u64().unwrap();
        assert_eq!(manifest(dir.path())["min_replay_wal"], active);
        if reclaim {
            assert!(engine.reclaim_archived_wal_segment(1).unwrap());
        }
        drop(engine);
        if !reclaim {
            std::fs::write(dir.path().join("1.log"), b"unneeded corrupt old WAL").unwrap();
        }
        std::fs::write(dir.path().join("999.log"), b"unpublished WAL").unwrap();
        let recovered = LsmKvEngine::with_wal(dir.path(), None).unwrap();
        assert_eq!(
            recovered.get(b"flushed", 30).unwrap(),
            Some(Bytes::from("durable"))
        );
        assert_eq!(
            recovered.get(b"tail", 30).unwrap(),
            Some(Bytes::from("active"))
        );
    }
}

#[test]
fn invalid_manifest_wal_ids_and_floor_fail_without_creating_files() {
    for (active, floor) in [(0, 0), (u64::MAX, 0), (1, 2)] {
        let dir = tempfile::tempdir().unwrap();
        seed(dir.path());
        let mut meta = manifest(dir.path());
        meta["active_wal"] = active.into();
        meta["min_replay_wal"] = floor.into();
        save_manifest(dir.path(), &meta);
        rejected_without_changes(dir.path());
    }
}

#[cfg(unix)]
#[test]
fn dangling_active_wal_symlink_cannot_create_its_target() {
    let dir = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let (active, _) = seed(dir.path());
    let path = dir.path().join(format!("{active}.log"));
    std::fs::remove_file(&path).unwrap();
    let target = outside.path().join("missing.log");
    std::os::unix::fs::symlink(&target, &path).unwrap();
    let manifest_bytes = std::fs::read(dir.path().join("MANIFEST")).unwrap();
    assert!(LsmKvEngine::with_wal(dir.path(), None).is_err());
    assert!(!target.exists());
    assert_eq!(
        std::fs::read(dir.path().join("MANIFEST")).unwrap(),
        manifest_bytes
    );
    assert_eq!(std::fs::read_link(path).unwrap(), target);
}

#[test]
fn restart_reserves_orphan_ids_without_reading_or_reusing_their_files() {
    let dir = tempfile::tempdir().unwrap();
    let (active, _) = seed(dir.path());
    let partial = dir.path().join(format!("{}.sst.tmp", active + 1));
    let orphan = dir.path().join(format!("{}.log", active + 2));
    std::fs::write(&partial, b"unpublished SSTable output").unwrap();
    // Invalid bytes prove the orphan is not read as part of the chain.
    std::fs::write(&orphan, b"unpublished WAL output").unwrap();
    let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();
    assert_rows(&engine);
    put(&engine, "later", "fourth", 50);
    engine.flush().unwrap();
    let next_active = manifest(dir.path())["active_wal"].as_u64().unwrap();
    assert!(next_active > active + 2);
    assert_eq!(
        std::fs::read(&partial).unwrap(),
        b"unpublished SSTable output"
    );
    assert_eq!(std::fs::read(&orphan).unwrap(), b"unpublished WAL output");
    let records =
        FileWalEngine::inspect(dir.path().join(format!("{next_active}.log")), None).unwrap();
    assert_eq!(
        records
            .iter()
            .filter(|record| matches!(record, WalRecord::V1(WalRecordV1::SegmentHeader { .. })))
            .count(),
        1
    );
    drop(engine);
    let reopened = LsmKvEngine::with_wal(dir.path(), None).unwrap();
    assert_rows(&reopened);
    assert_eq!(
        reopened.get(b"later", 100).unwrap(),
        Some(Bytes::from("fourth"))
    );
}
