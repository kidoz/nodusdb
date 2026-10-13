//! Simulated power-loss tests for the persistent LSM engine.
//!
//! SIGKILL-style crash tests cannot prove the fsync discipline: the surviving
//! OS flushes the dirty pages of a dead process, so every byte ever written is
//! visible after the kill. A real power cut is different — only what an
//! `fdatasync` returned over is durable, the unsynced tail may survive as any
//! prefix (or not at all), and the final write may be torn mid-frame.
//!
//! The harness below virtualizes exactly that boundary. A [`SyncTrackingWal`]
//! wraps the engine's real [`FileWalEngine`] (append/sync/recover all delegate,
//! so every test exercises the production code paths) and records two frontier
//! offsets for the active segment: `written_len` (bytes handed to the OS) and
//! `synced_len` (bytes an `fdatasync` returned over). A simulated power cut
//! then rewrites the segment file to a state a crash could legitimately leave:
//! the synced prefix intact, the unsynced region kept up to an arbitrary byte
//! boundary, optionally followed by garbage modelling a torn final write.
//! Recovery is always the real [`LsmKvEngine::with_wal`] reopen.
//!
//! What this proves: given a storage device that honors fsync, acknowledged
//! commits survive and unacknowledged state is never partially applied. What
//! it does NOT prove: real device behavior (volatile write caches, filesystems
//! that lie about fsync), which no in-process test can establish.

use super::*;
use nodus_storage_api::{SnapshotRow, SnapshotScope};
use tempfile::TempDir;

/// The durable frontier of one WAL segment: `synced_len` bytes survive a power
/// cut; the region `[synced_len, written_len)` may survive as any prefix.
#[derive(Default)]
struct WalSyncState {
    synced_len: u64,
    written_len: u64,
}

/// A `WalEngine` wrapper that records the durable frontier on every `sync`.
/// Behavior is unchanged: everything delegates to the real engine.
struct SyncTrackingWal {
    inner: Arc<dyn WalEngine>,
    path: PathBuf,
    state: Arc<Mutex<WalSyncState>>,
}

impl WalEngine for SyncTrackingWal {
    fn append(&self, record: WalRecord) -> anyhow::Result<u64> {
        let lsn = self.inner.append(record)?;
        self.state.lock().unwrap().written_len = std::fs::metadata(&self.path)?.len();
        Ok(lsn)
    }

    fn sync(&self) -> anyhow::Result<()> {
        self.inner.sync()?;
        let len = std::fs::metadata(&self.path)?.len();
        let mut state = self.state.lock().unwrap();
        state.synced_len = len;
        state.written_len = len;
        Ok(())
    }

    fn recover(&self) -> anyhow::Result<Vec<WalRecord>> {
        self.inner.recover()
    }
}

type SyncTracker = Arc<Mutex<WalSyncState>>;

/// Replaces the engine's active WAL with a sync-tracking wrapper and returns
/// the shared tracker. The current file content is already durable (every
/// commit so far synced), so both frontiers start at the file length.
fn track_active_wal(engine: &LsmKvEngine) -> SyncTracker {
    let wal_id = engine.active_wal_id.load(Ordering::Relaxed);
    let path = engine
        .data_dir
        .as_ref()
        .expect("tracked engine must be persistent")
        .join(format!("{wal_id}.log"));
    let mut guard = engine.wal.write().unwrap();
    let inner = guard.take().expect("persistent engine has a WAL");
    let len = std::fs::metadata(&path).unwrap().len();
    let tracker = Arc::new(Mutex::new(WalSyncState {
        synced_len: len,
        written_len: len,
    }));
    *guard = Some(Arc::new(SyncTrackingWal {
        inner,
        path,
        state: tracker.clone(),
    }));
    tracker
}

fn frontier(tracker: &SyncTracker) -> (usize, usize) {
    let state = tracker.lock().unwrap();
    (state.synced_len as usize, state.written_len as usize)
}

/// Rewrites the WAL segment to a legitimate post-power-cut state: the first
/// `keep` bytes of `image` survived, followed by `garbage` bytes of torn-write
/// residue (a partial sector that was never a valid frame).
fn apply_power_cut(dir: &Path, wal_id: u64, image: &[u8], keep: usize, garbage: usize) {
    let path = dir.join(format!("{wal_id}.log"));
    let mut crashed = image[..keep].to_vec();
    crashed.extend(std::iter::repeat_n(0xA5u8, garbage));
    std::fs::write(&path, &crashed).unwrap();
    std::fs::File::open(&path).unwrap().sync_all().unwrap();
}

fn wal_image(dir: &Path, wal_id: u64) -> Vec<u8> {
    std::fs::read(dir.join(format!("{wal_id}.log"))).unwrap()
}

/// Commits `key=value` at `ts` in one acknowledged transaction.
fn put(engine: &LsmKvEngine, key: &str, value: &str, ts: Timestamp) {
    let txn = TxnId::new();
    engine
        .write_intent(
            txn,
            Bytes::from(key.to_owned()),
            Bytes::from(value.to_owned()),
        )
        .unwrap();
    engine.commit(txn, ts).unwrap();
}

/// Copies every regular file of `src` into `dst`: a crash image captures each
/// file exactly as the crash left it.
fn copy_dir(src: &Path, dst: &Path) {
    std::fs::create_dir_all(dst).unwrap();
    for entry in std::fs::read_dir(src).unwrap() {
        let entry = entry.unwrap();
        if entry.file_type().unwrap().is_file() {
            std::fs::copy(entry.path(), dst.join(entry.file_name())).unwrap();
        }
    }
}

/// Acknowledged commits must survive a power cut no matter where in the
/// unsynced tail the cut lands. This is the test that pins the fsync
/// discipline: the tracker's `synced_len` only covers the commit records if
/// `commit()` really synced before acknowledging — drop that `wal.sync()` and
/// the durable frontier stops short of the acknowledged commits, and the cuts
/// below lose them.
#[test]
fn acknowledged_commits_survive_power_cut_at_every_byte_of_the_unsynced_tail() {
    let dir = TempDir::new().unwrap();
    let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();
    let tracker = track_active_wal(&engine);

    // Three acknowledged commits; each fsyncs the WAL before returning.
    put(&engine, "k1", "a", 10);
    put(&engine, "k2", "b", 20);
    put(&engine, "k3", "c", 30);

    // An uncommitted intent extends the unsynced tail; it was never promised.
    let pending = TxnId::new();
    engine
        .write_intent(pending, Bytes::from("pending"), Bytes::from("p"))
        .unwrap();

    let wal_id = engine.active_wal_id.load(Ordering::Relaxed);
    let image = wal_image(dir.path(), wal_id);
    let (synced_len, written_len) = frontier(&tracker);
    assert!(
        synced_len < written_len,
        "the test needs a non-empty unsynced tail"
    );
    drop(engine);

    // Cut at every byte boundary of the unsynced tail.
    for keep in synced_len..=written_len {
        apply_power_cut(dir.path(), wal_id, &image, keep, 0);
        let recovered = LsmKvEngine::with_wal(dir.path(), None).unwrap();
        for (key, value) in [("k1", "a"), ("k2", "b"), ("k3", "c")] {
            assert_eq!(
                recovered.get(key.as_bytes(), 100).unwrap(),
                Some(Bytes::from(value)),
                "acknowledged commit lost (cut at byte {keep} of {written_len})"
            );
        }
        assert_eq!(
            recovered.get(b"pending", 100).unwrap(),
            None,
            "uncommitted intent became visible (cut at byte {keep})"
        );
        drop(recovered);
    }

    // Torn-write variant: garbage residue past the cut point (a stride of
    // boundaries; every boundary is covered by the clean cuts above).
    for keep in (synced_len..written_len).step_by(9) {
        apply_power_cut(dir.path(), wal_id, &image, keep, 13);
        let recovered = LsmKvEngine::with_wal(dir.path(), None).unwrap();
        assert_eq!(
            recovered.get(b"k3", 100).unwrap(),
            Some(Bytes::from("c")),
            "acknowledged commit lost (torn cut at byte {keep})"
        );
        assert_eq!(recovered.get(b"pending", 100).unwrap(), None);
        drop(recovered);
    }
}

/// A commit whose fsync never completed (the machine died mid-`commit()`) must
/// be all-or-nothing after recovery: replay sees a frame prefix, so the
/// `CommitTxn` is present only if both of its intents are — the transaction is
/// fully applied or fully absent, never torn to a subset of its keys.
#[test]
fn unacknowledged_commit_is_never_partially_applied_after_power_cut() {
    let dir = TempDir::new().unwrap();
    let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();
    let tracker = track_active_wal(&engine);
    put(&engine, "anchor", "s", 5);

    // A two-key transaction whose commit record reached the OS but whose fsync
    // never completed when power failed — modelled by appending the `CommitTxn`
    // record without a sync, exactly where `commit()` would have written it.
    let victim = TxnId::new();
    engine
        .write_intent(victim, Bytes::from("ta"), Bytes::from("x"))
        .unwrap();
    engine
        .write_intent(victim, Bytes::from("tb"), Bytes::from("y"))
        .unwrap();
    {
        let wal = engine.wal.read().unwrap();
        wal.as_ref()
            .unwrap()
            .append(WalRecord::V1(WalRecordV1::CommitTxn {
                txn_id: victim,
                commit_ts: 20,
            }))
            .unwrap();
    }

    let wal_id = engine.active_wal_id.load(Ordering::Relaxed);
    let image = wal_image(dir.path(), wal_id);
    let (synced_len, written_len) = frontier(&tracker);
    assert!(synced_len < written_len);
    drop(engine);

    let check = |dir: &Path, keep: usize, garbage: usize| {
        apply_power_cut(dir, wal_id, &image, keep, garbage);
        let recovered = LsmKvEngine::with_wal(dir, None).unwrap();
        let a = recovered.get(b"ta", 100).unwrap().is_some();
        let b = recovered.get(b"tb", 100).unwrap().is_some();
        assert_eq!(
            a, b,
            "transaction partially applied (cut at byte {keep}, garbage {garbage})"
        );
        // The acknowledged setup write is always intact.
        assert_eq!(
            recovered.get(b"anchor", 100).unwrap(),
            Some(Bytes::from("s"))
        );
    };
    for keep in synced_len..=written_len {
        check(dir.path(), keep, 0);
    }
    for keep in (synced_len..written_len).step_by(9) {
        check(dir.path(), keep, 7);
    }
}

/// A torn final frame — a partial write inside the region an fsync covered —
/// must truncate cleanly at recovery, and commits acknowledged after that
/// recovery must themselves survive the next power cut. Without tail
/// truncation, records appended after the tear would sit behind undecodable
/// bytes and every later replay would stop at the same tear.
#[test]
fn torn_wal_tail_recovers_and_later_commits_survive_the_next_power_cut() {
    let dir = TempDir::new().unwrap();
    let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();
    let tracker = track_active_wal(&engine);
    put(&engine, "k1", "v1", 10);

    // An unsynced intent, partially flushed when power failed: the segment
    // ends a few bytes into the frame — a torn write past the synced prefix.
    let dropped = TxnId::new();
    engine
        .write_intent(dropped, Bytes::from("torn"), Bytes::from("x"))
        .unwrap();
    let wal_id = engine.active_wal_id.load(Ordering::Relaxed);
    let image = wal_image(dir.path(), wal_id);
    let (synced_len, _) = frontier(&tracker);
    drop(engine);
    apply_power_cut(dir.path(), wal_id, &image, synced_len + 11, 0);

    // Reopen: the tear truncates, the acknowledged commit is intact, and the
    // segment is the active log again.
    let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();
    assert_eq!(engine.get(b"k1", 100).unwrap(), Some(Bytes::from("v1")));
    assert_eq!(engine.get(b"torn", 100).unwrap(), None);
    assert_eq!(
        wal_image(dir.path(), wal_id).len(),
        synced_len,
        "recovery must truncate the torn tail"
    );

    // A commit acknowledged AFTER the torn-tail recovery, then a second power
    // cut with its own unsynced tail.
    let tracker = track_active_wal(&engine);
    put(&engine, "k2", "v2", 20);
    let dropped = TxnId::new();
    engine
        .write_intent(dropped, Bytes::from("torn2"), Bytes::from("y"))
        .unwrap();
    let image = wal_image(dir.path(), wal_id);
    let (synced_len, _) = frontier(&tracker);
    drop(engine);
    apply_power_cut(dir.path(), wal_id, &image, synced_len + 5, 0);

    let recovered = LsmKvEngine::with_wal(dir.path(), None).unwrap();
    assert_eq!(recovered.get(b"k1", 100).unwrap(), Some(Bytes::from("v1")));
    assert_eq!(
        recovered.get(b"k2", 100).unwrap(),
        Some(Bytes::from("v2")),
        "commit after a torn-tail recovery was lost at the next crash"
    );
    assert_eq!(recovered.get(b"torn2", 100).unwrap(), None);
}

/// A power cut around a flush lands on either side of the manifest rename —
/// the commit point — and may additionally lose directory entries that were
/// never fsynced. Whatever the combination, recovery must come up consistent:
/// the store matches the OLD manifest (replaying the WAL lineage) or the NEW
/// one (reading the published SSTable), never a hybrid.
#[test]
fn power_cut_around_flush_publishes_old_or_new_manifest() {
    let dir = TempDir::new().unwrap();
    let engine = Arc::new(LsmKvEngine::with_wal(dir.path(), None).unwrap());
    put(&engine, "stable", "s", 10);

    // Park the flush after the WAL swap and SSTable publication but before the
    // manifest save: the on-disk state is exactly a pre-rename crash image —
    // OLD manifest, with the new SSTable and WAL segment present as
    // complete-but-unpublished orphans.
    engine.arm_flush_pause();
    let flusher = {
        let engine = engine.clone();
        std::thread::spawn(move || engine.flush())
    };
    engine.wait_for_parked_flush(1);

    let pre = TempDir::new().unwrap();
    copy_dir(dir.path(), pre.path());
    // Same crash, but the new segment's directory entry is also gone (the
    // flush had not dir-fsynced it yet when power failed). The old manifest
    // does not name it, so this must be harmless.
    let pre_no_wal = TempDir::new().unwrap();
    copy_dir(dir.path(), pre_no_wal.path());
    let unpublished = engine.active_wal_id.load(Ordering::Relaxed);
    std::fs::remove_file(pre_no_wal.path().join(format!("{unpublished}.log"))).unwrap();
    // And the same crash losing the unpublished SSTable as well.
    let pre_bare = TempDir::new().unwrap();
    copy_dir(dir.path(), pre_bare.path());
    for entry in std::fs::read_dir(pre_bare.path()).unwrap() {
        let path = entry.unwrap().path();
        if path.extension().and_then(|s| s.to_str()) == Some("sst")
            || path.file_name().and_then(|s| s.to_str())
                == Some(format!("{unpublished}.log").as_str())
        {
            std::fs::remove_file(&path).unwrap();
        }
    }

    engine.release_parked_flush();
    flusher.join().unwrap().unwrap();

    // A power cut just after the rename instead: the NEW manifest is durable
    // and every file it names is complete (SSTable build and the manifest
    // publication each dir-fsynced). Old files are still on disk — cleanup
    // lags the commit point.
    let post = TempDir::new().unwrap();
    copy_dir(dir.path(), post.path());
    // Robustness beyond the legitimate states: even if the new segment file
    // itself were lost, the published SSTable still carries the data.
    let post_no_wal = TempDir::new().unwrap();
    copy_dir(dir.path(), post_no_wal.path());
    let active = engine.active_wal_id.load(Ordering::Relaxed);
    std::fs::remove_file(post_no_wal.path().join(format!("{active}.log"))).unwrap();
    drop(engine);

    // Every image is a consistent store holding the acknowledged write, and
    // stays writable after recovery.
    for image in [
        pre.path(),
        pre_no_wal.path(),
        pre_bare.path(),
        post.path(),
        post_no_wal.path(),
    ] {
        let recovered = LsmKvEngine::with_wal(image, None).unwrap();
        assert_eq!(
            recovered.get(b"stable", 100).unwrap(),
            Some(Bytes::from("s")),
            "acknowledged write lost for crash image {image:?}"
        );
        put(&recovered, "after", "x", 20);
        drop(recovered);
        let recovered = LsmKvEngine::with_wal(image, None).unwrap();
        assert_eq!(
            recovered.get(b"stable", 100).unwrap(),
            Some(Bytes::from("s"))
        );
        assert_eq!(
            recovered.get(b"after", 100).unwrap(),
            Some(Bytes::from("x"))
        );
    }
}

/// The checkpoint (`replace_snapshot`) publication has the same commit-point
/// structure as the flush: a complete new SSTable and WAL segment, then the
/// manifest rename. A power cut leaves the OLD or the NEW checkpoint in full —
/// never a hybrid of new data files under the old manifest or vice versa.
#[test]
fn power_cut_around_checkpoint_publishes_old_or_new_snapshot() {
    let dir = TempDir::new().unwrap();
    let engine = LsmKvEngine::with_wal(dir.path(), None).unwrap();
    put(&engine, "row", "old", 10);
    put(&engine, "local", "l", 10);

    // Crash image before publication.
    let pre = TempDir::new().unwrap();
    copy_dir(dir.path(), pre.path());

    engine
        .replace_snapshot(
            &SnapshotScope {
                prefix: b"row".to_vec(),
                ..Default::default()
            },
            vec![SnapshotRow::committed(
                Bytes::from("row"),
                Bytes::from("new"),
                20,
            )],
            vec![],
        )
        .unwrap();

    // Crash image after publication (the old files deliberately remain).
    let post = TempDir::new().unwrap();
    copy_dir(dir.path(), post.path());
    drop(engine);

    // A crash between the tmp-manifest fsync and the rename: the old MANIFEST
    // is still in place, the new manifest sits orphaned as
    // `SNAPSHOT_MANIFEST.tmp`, and the new data files are present but unnamed.
    let mid = TempDir::new().unwrap();
    copy_dir(pre.path(), mid.path());
    for entry in std::fs::read_dir(post.path()).unwrap() {
        let entry = entry.unwrap();
        let name = entry.file_name();
        if !pre.path().join(&name).exists() {
            std::fs::copy(entry.path(), mid.path().join(&name)).unwrap();
        }
    }
    std::fs::copy(
        post.path().join(MANIFEST_FILE),
        mid.path().join("SNAPSHOT_MANIFEST.tmp"),
    )
    .unwrap();

    // A crash where the rename itself was lost (no dir fsync reached the
    // manifest entry): new files on disk, old MANIFEST content.
    let rename_lost = TempDir::new().unwrap();
    copy_dir(post.path(), rename_lost.path());
    std::fs::copy(
        pre.path().join(MANIFEST_FILE),
        rename_lost.path().join(MANIFEST_FILE),
    )
    .unwrap();

    // Old side: the checkpoint never happened; out-of-scope data is intact.
    for image in [pre.path(), mid.path(), rename_lost.path()] {
        let recovered = LsmKvEngine::with_wal(image, None).unwrap();
        assert_eq!(
            recovered.get(b"row", 100).unwrap(),
            Some(Bytes::from("old")),
            "old-manifest crash image must show the pre-checkpoint state ({image:?})"
        );
        assert_eq!(
            recovered.get(b"local", 100).unwrap(),
            Some(Bytes::from("l"))
        );
    }
    // New side: the checkpoint is installed in full.
    let recovered = LsmKvEngine::with_wal(post.path(), None).unwrap();
    assert_eq!(
        recovered.get(b"row", 100).unwrap(),
        Some(Bytes::from("new"))
    );
    assert_eq!(
        recovered.get(b"local", 100).unwrap(),
        Some(Bytes::from("l"))
    );
}
