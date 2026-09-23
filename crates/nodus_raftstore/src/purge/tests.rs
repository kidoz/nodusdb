use super::*;
use nodus_storage_api::{IntentReplacement, KvPair};
use nodus_storage_lsm::LsmKvEngine;
use std::sync::Mutex;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

#[derive(Clone, Copy, Default)]
enum Fault {
    #[default]
    None,
    Delete,
    Watermark,
    BeforeCommit,
    AfterCommit,
    ExitBeforeCommit,
    ExitAfterCommit,
}

struct ProbeKv {
    inner: Arc<dyn KvEngine>,
    commits: AtomicUsize,
    fault: Mutex<(usize, Fault)>,
    entered: tokio::sync::Notify,
    gate: Mutex<Option<std::sync::mpsc::Receiver<()>>>,
}

impl ProbeKv {
    fn new(inner: Arc<dyn KvEngine>) -> Self {
        Self {
            inner,
            commits: AtomicUsize::new(0),
            fault: Mutex::new((0, Fault::None)),
            entered: tokio::sync::Notify::new(),
            gate: Mutex::new(None),
        }
    }

    fn arm(&self, batch: usize, fault: Fault) {
        self.commits.store(0, Ordering::SeqCst);
        *self.fault.lock().unwrap() = (batch, fault);
    }

    fn fault(&self) -> Fault {
        let (batch, fault) = *self.fault.lock().unwrap();
        if self.commits.load(Ordering::SeqCst) + 1 == batch {
            fault
        } else {
            Fault::None
        }
    }

    fn fail() -> KvResult<()> {
        Err(KvError::Other(anyhow::anyhow!("injected purge failure")))
    }
}

impl KvEngine for ProbeKv {
    fn get(&self, key: &[u8], ts: u64) -> anyhow::Result<Option<Bytes>> {
        self.inner.get(key, ts)
    }
    fn scan(
        &self,
        range: KeyRange,
        ts: u64,
    ) -> anyhow::Result<Box<dyn Iterator<Item = anyhow::Result<KvPair>> + Send>> {
        self.inner.scan(range, ts)
    }
    fn write_intent(&self, txn: TxnId, key: Bytes, value: Bytes) -> KvResult<()> {
        if matches!(self.fault(), Fault::Watermark) && key.as_ref() == RAFT_APPLIED_KEY {
            return Self::fail();
        }
        self.inner.write_intent(txn, key, value)
    }
    fn delete_intent(&self, txn: TxnId, key: Bytes) -> KvResult<()> {
        self.inner.delete_intent(txn, key)?;
        if matches!(self.fault(), Fault::Delete) {
            return Self::fail();
        }
        Ok(())
    }
    fn replace_intent(
        &self,
        txn: TxnId,
        key: Bytes,
        replacement: IntentReplacement,
    ) -> KvResult<()> {
        self.inner.replace_intent(txn, key, replacement)
    }
    fn commit(&self, txn: TxnId, ts: u64) -> KvResult<()> {
        if let Some(gate) = self.gate.lock().unwrap().take() {
            self.entered.notify_one();
            gate.recv_timeout(Duration::from_secs(5)).unwrap();
        }
        let fault = self.fault();
        self.commits.fetch_add(1, Ordering::SeqCst);
        if matches!(fault, Fault::BeforeCommit) {
            return Self::fail();
        }
        if matches!(fault, Fault::ExitBeforeCommit) {
            std::process::exit(73);
        }
        self.inner.commit(txn, ts)?;
        if matches!(fault, Fault::AfterCommit) {
            return Self::fail();
        }
        if matches!(fault, Fault::ExitAfterCommit) {
            std::process::exit(73);
        }
        Ok(())
    }
    fn abort(&self, txn: TxnId) -> KvResult<()> {
        self.inner.abort(txn)
    }
}

fn entry(index: u64) -> Entry<NodusTypeConfig> {
    Entry {
        log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), index),
        payload: EntryPayload::Blank,
    }
}

async fn seed(kv: Arc<dyn KvEngine>, dir: PathBuf, count: u64) -> NodusRaftStore {
    let mut store = NodusRaftStore::with_kv_at(kv, dir);
    let mut entries: Vec<_> = (1..=count).map(entry).collect();
    entries[0].payload = EntryPayload::Membership(openraft::Membership::new(
        vec![std::collections::BTreeSet::from([1, 2, 3])],
        None,
    ));
    store.append_to_log(entries.clone()).await.unwrap();
    store
        .apply_to_state_machine(&entries[..entries.len() - 1])
        .await
        .unwrap();
    store.save_vote(&Vote::new(1, 1)).await.unwrap();
    store
}

// Read persisted records directly first: constructor recovery reconciliation
// must not mask a mismatch between the tombstones and the purge watermark.
async fn assert_prefix(kv: Arc<dyn KvEngine>, dir: PathBuf, count: u64, purged: u64) {
    let meta = RaftMetaStore::new(kv.clone());
    assert_eq!(
        meta.load_applied().last_membership.log_id(),
        &Some(entry(1).log_id)
    );
    assert_eq!(
        meta.load_applied()
            .last_membership
            .membership()
            .voter_ids()
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert_eq!(
        meta.load_applied().last_purged.map(|id| id.index),
        (purged > 0).then_some(purged)
    );
    assert_eq!(
        meta.load_log().keys().copied().collect::<Vec<_>>(),
        ((purged + 1)..=count).collect::<Vec<_>>()
    );
    let mut reopened = NodusRaftStore::with_kv_at(kv, dir);
    assert_eq!(
        reopened.get_log_state().await.unwrap().last_log_id,
        Some(entry(count).log_id)
    );
    assert_eq!(
        reopened.last_applied_state().await.unwrap().0,
        Some(entry(count - 1).log_id)
    );
    assert_eq!(reopened.read_vote().await.unwrap(), Some(Vote::new(1, 1)));
}

#[tokio::test]
async fn purge_batches_preserve_tail_and_recover_watermark() {
    let dir = tempfile::tempdir().unwrap();
    let inner = Arc::new(LsmKvEngine::with_wal(dir.path().join("data"), None).unwrap());
    let kv = Arc::new(ProbeKv::new(inner));
    let mut store = seed(kv.clone(), dir.path().join("snap"), 701).await;
    kv.arm(0, Fault::None);
    store.purge_logs_upto(entry(600).log_id).await.unwrap();
    assert_eq!(
        kv.commits.load(Ordering::SeqCst),
        3,
        "600 tombstones need only three durable commits"
    );
    store.purge_logs_upto(entry(500).log_id).await.unwrap();
    assert_eq!(
        kv.commits.load(Ordering::SeqCst),
        3,
        "stale request cannot regress watermark"
    );
    drop(store);
    drop(kv);
    let reopened = Arc::new(LsmKvEngine::with_wal(dir.path().join("data"), None).unwrap());
    assert_prefix(reopened, dir.path().join("snap"), 701, 600).await;
}

#[tokio::test]
async fn purge_exact_batch_and_empty_log_keep_the_durable_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let kv = Arc::new(LsmKvEngine::with_wal(dir.path().join("data"), None).unwrap());
    let mut store = seed(kv.clone(), dir.path().join("snap"), 257).await;
    store.purge_logs_upto(entry(256).log_id).await.unwrap();
    assert_prefix(kv.clone(), dir.path().join("snap"), 257, 256).await;
    store.apply_to_state_machine(&[entry(257)]).await.unwrap();
    store.purge_logs_upto(entry(257).log_id).await.unwrap();
    // Snapshot-installed state may have no retained entries at the target.
    // Reset only the purge pointer to exercise this case without fake logs.
    store.state_machine.write().await.last_purged = None;
    store.purge_logs_upto(entry(257).log_id).await.unwrap();
    drop(store);
    drop(kv);
    let kv = Arc::new(LsmKvEngine::with_wal(dir.path().join("data"), None).unwrap());
    let mut store = NodusRaftStore::with_kv_at(kv, dir.path().join("snap"));
    let state = store.get_log_state().await.unwrap();
    assert_eq!(state.last_purged_log_id, Some(entry(257).log_id));
    assert_eq!(state.last_log_id, Some(entry(257).log_id));
    assert!(store.try_get_log_entries(..).await.unwrap().is_empty());
}

#[tokio::test]
async fn purge_failures_keep_a_recoverable_prefix_and_report_error() {
    for fault in [
        Fault::Delete,
        Fault::Watermark,
        Fault::BeforeCommit,
        Fault::AfterCommit,
    ] {
        let dir = tempfile::tempdir().unwrap();
        let inner = Arc::new(LsmKvEngine::with_wal(dir.path().join("data"), None).unwrap());
        let kv = Arc::new(ProbeKv::new(inner));
        let mut store = seed(kv.clone(), dir.path().join("snap"), 601).await;
        kv.arm(2, fault);
        let error = store.purge_logs_upto(entry(600).log_id).await.unwrap_err();
        assert!(error.to_string().contains("injected purge failure"));
        // Only acknowledged batches reach the cache, including ambiguous commit errors.
        assert_eq!(
            store.get_log_state().await.unwrap().last_purged_log_id,
            Some(entry(256).log_id)
        );
        assert_eq!(
            store.try_get_log_entries(257..=601).await.unwrap().len(),
            345
        );
        drop(store);
        drop(kv);
        let reopened = Arc::new(LsmKvEngine::with_wal(dir.path().join("data"), None).unwrap());
        let purged = if matches!(fault, Fault::AfterCommit) {
            512
        } else {
            256
        };
        assert_prefix(reopened, dir.path().join("snap"), 601, purged).await;
    }
}

#[tokio::test(flavor = "current_thread")]
async fn cancelling_purge_keeps_publication_and_cache_update_together() {
    let dir = tempfile::tempdir().unwrap();
    let kv = Arc::new(ProbeKv::new(
        Arc::new(nodus_storage_mem::MemKvEngine::new()),
    ));
    let store = seed(kv.clone(), dir.path().join("snap"), 301).await;
    let mut worker = store.clone();
    let (release, gate) = std::sync::mpsc::channel();
    *kv.gate.lock().unwrap() = Some(gate);
    let purge = tokio::spawn(async move { worker.purge_logs_upto(entry(300).log_id).await });
    // On a single-worker runtime this only progresses if disk I/O is offloaded.
    tokio::time::timeout(Duration::from_secs(2), kv.entered.notified())
        .await
        .unwrap();
    purge.abort();
    assert!(purge.await.unwrap_err().is_cancelled());
    assert!(
        store.log.try_read().is_err(),
        "blocking task retains the cache lock"
    );
    release.send(()).unwrap();
    let mut observer = store.clone();
    let state = tokio::time::timeout(Duration::from_secs(2), observer.get_log_state())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(state.last_purged_log_id, Some(entry(300).log_id));
    assert_prefix(kv, dir.path().join("snap"), 301, 300).await;
}

#[tokio::test]
async fn purge_crash_child() {
    let Ok(path) = std::env::var("NODUS_PURGE_CRASH_DIR") else {
        return;
    };
    let dir = PathBuf::from(path);
    let kv = Arc::new(ProbeKv::new(Arc::new(
        LsmKvEngine::with_wal(dir.join("data"), None).unwrap(),
    )));
    let mut store = seed(kv.clone(), dir.join("snap"), 601).await;
    let after = std::env::var("NODUS_PURGE_CRASH_AFTER").unwrap() == "true";
    kv.arm(
        2,
        if after {
            Fault::ExitAfterCommit
        } else {
            Fault::ExitBeforeCommit
        },
    );
    store.purge_logs_upto(entry(600).log_id).await.unwrap();
    panic!("crash hook was not reached");
}

#[tokio::test]
async fn abrupt_exit_during_purge_recovers_atomic_batches() {
    for after in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let mut child = std::process::Command::new(std::env::current_exe().unwrap())
            .args(["--exact", "purge::tests::purge_crash_child", "--nocapture"])
            .env("NODUS_PURGE_CRASH_DIR", dir.path())
            .env("NODUS_PURGE_CRASH_AFTER", after.to_string())
            .spawn()
            .unwrap();
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        let status = loop {
            if let Some(status) = child.try_wait().unwrap() {
                break status;
            }
            if std::time::Instant::now() >= deadline {
                child.kill().unwrap();
                child.wait().unwrap();
                panic!("purge crash child timed out");
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        assert_eq!(
            status.code(),
            Some(73),
            "child exited at the armed commit boundary without destructors"
        );
        let kv = Arc::new(LsmKvEngine::with_wal(dir.path().join("data"), None).unwrap());
        assert_prefix(
            kv,
            dir.path().join("snap"),
            601,
            if after { 512 } else { 256 },
        )
        .await;
    }
}

/// Repeatable local fsync comparison, deliberately excluded from normal tests.
#[tokio::test]
#[ignore = "local persistent purge benchmark"]
async fn benchmark_purge_commits() {
    for batched in [false, true] {
        let dir = tempfile::tempdir().unwrap();
        let kv = Arc::new(LsmKvEngine::with_wal(dir.path().join("data"), None).unwrap());
        let mut store = seed(kv.clone(), dir.path().join("snap"), 4001).await;
        let start = std::time::Instant::now();
        if batched {
            store.purge_logs_upto(entry(4000).log_id).await.unwrap();
        } else {
            let meta = RaftMetaStore::new(kv.clone());
            for index in 1..=4000 {
                meta.delete_checked(&log_key(index)).unwrap();
            }
            let mut applied = meta.load_applied();
            applied.last_purged = Some(entry(4000).log_id);
            meta.put_checked(
                RAFT_APPLIED_KEY,
                encode_raft(serde_json::to_vec(&applied).unwrap()),
            )
            .unwrap();
        }
        eprintln!(
            "purge 4000 entries: batched={batched}, elapsed={:?}",
            start.elapsed()
        );
        assert_prefix(kv, dir.path().join("snap"), 4001, 4000).await;
    }
}
