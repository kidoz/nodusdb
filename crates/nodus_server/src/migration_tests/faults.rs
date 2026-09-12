use super::*;
use anyhow::Result;
use nodus_storage_api::{KeyRange, KvPair, KvResult};
use std::sync::atomic::{AtomicBool, Ordering};

struct FaultEngine {
    inner: nodus_storage_mem::MemKvEngine,
    fail_commit: AtomicBool,
    fail_scan: AtomicBool,
}

impl FaultEngine {
    fn new() -> Self {
        Self {
            inner: nodus_storage_mem::MemKvEngine::new(),
            fail_commit: AtomicBool::new(false),
            fail_scan: AtomicBool::new(false),
        }
    }
}

impl KvEngine for FaultEngine {
    fn snapshot_rows(
        &self,
        scope: &nodus_storage_api::SnapshotScope,
    ) -> Result<Vec<nodus_storage_api::SnapshotRow>> {
        anyhow::ensure!(
            !self.fail_scan.load(Ordering::SeqCst),
            "injected snapshot read failure"
        );
        self.inner.snapshot_rows(scope)
    }
    fn replace_snapshot(
        &self,
        scope: &nodus_storage_api::SnapshotScope,
        rows: Vec<nodus_storage_api::SnapshotRow>,
        pointers: Vec<nodus_storage_api::SnapshotRow>,
    ) -> Result<()> {
        anyhow::ensure!(
            !self.fail_commit.load(Ordering::SeqCst),
            "injected checkpoint failure"
        );
        self.inner.replace_snapshot(scope, rows, pointers)
    }

    fn replace_intent(
        &self,
        txn: TxnId,
        key: Bytes,
        replacement: nodus_storage_api::IntentReplacement,
    ) -> KvResult<()> {
        self.inner.replace_intent(txn, key, replacement)
    }

    fn get(&self, key: &[u8], ts: u64) -> Result<Option<Bytes>> {
        self.inner.get(key, ts)
    }
    fn scan(
        &self,
        range: KeyRange,
        ts: u64,
    ) -> Result<Box<dyn Iterator<Item = Result<KvPair>> + Send>> {
        anyhow::ensure!(
            !self.fail_scan.load(Ordering::SeqCst),
            "injected scan failure"
        );
        self.inner.scan(range, ts)
    }
    fn write_intent(&self, txn: TxnId, key: Bytes, value: Bytes) -> KvResult<()> {
        self.inner.write_intent(txn, key, value)
    }
    fn delete_intent(&self, txn: TxnId, key: Bytes) -> KvResult<()> {
        self.inner.delete_intent(txn, key)
    }
    fn commit(&self, txn: TxnId, ts: u64) -> KvResult<()> {
        if self.fail_commit.load(Ordering::SeqCst) {
            return Err(anyhow::anyhow!("injected commit failure").into());
        }
        self.inner.commit(txn, ts)
    }
    fn abort(&self, txn: TxnId) -> KvResult<()> {
        self.inner.abort(txn)
    }
    fn pending_intent_keys(&self, txn: TxnId) -> Vec<Bytes> {
        self.inner.pending_intent_keys(txn)
    }
    fn has_pending_intents(&self, prefix: &[u8]) -> Result<bool> {
        self.inner.has_pending_intents(prefix)
    }
}

#[tokio::test]
async fn failed_fence_commit_never_advances_applied_and_replay_finishes() {
    let dir = tempfile::tempdir().unwrap();
    let kv = Arc::new(FaultEngine::new());
    let mut h = Harness::new(kv.clone(), dir.path().into()).await;
    let entry = openraft::Entry {
        log_id: openraft::LogId::new(openraft::CommittedLeaderId::new(1, 1), 1),
        payload: openraft::EntryPayload::Normal(ShardCommand::MigrationV1(Control::Acquire {
            operation_id: Uuid::new_v4(),
            expected_epoch: 0,
        })),
    };
    kv.fail_commit.store(true, Ordering::SeqCst);
    assert!(
        h.store
            .apply_to_state_machine(std::slice::from_ref(&entry))
            .await
            .is_err()
    );
    assert!(
        h.store
            .state_machine
            .read()
            .await
            .last_applied_log
            .is_none()
    );
    assert!(read_fence(kv.as_ref()).unwrap().is_none());
    kv.fail_commit.store(false, Ordering::SeqCst);
    assert!(
        h.store
            .apply_to_state_machine(std::slice::from_ref(&entry))
            .await
            .unwrap()[0]
            .success
    );
    assert!(read_fence(kv.as_ref()).unwrap().unwrap().closed);
    assert_eq!(
        h.store.state_machine.read().await.last_applied_log,
        Some(entry.log_id)
    );
}

#[tokio::test]
async fn snapshots_fail_instead_of_omitting_or_losing_a_fence() {
    let dir = tempfile::tempdir().unwrap();
    let kv = Arc::new(FaultEngine::new());
    let mut h = Harness::new(kv.clone(), dir.path().join("source")).await;
    assert!(
        h.control(Control::Acquire {
            operation_id: Uuid::new_v4(),
            expected_epoch: 0
        })
        .await
        .success
    );
    kv.fail_scan.store(true, Ordering::SeqCst);
    assert!(h.store.build_snapshot().await.is_err());
    assert!(h.store.get_current_snapshot().await.unwrap().is_none());
    kv.fail_scan.store(false, Ordering::SeqCst);
    let snapshot = h.store.build_snapshot().await.unwrap();
    let receiver_kv = Arc::new(FaultEngine::new());
    let mut receiver = Harness::new(receiver_kv.clone(), dir.path().join("receiver")).await;
    receiver_kv.fail_commit.store(true, Ordering::SeqCst);
    assert!(
        receiver
            .store
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .is_err()
    );
    assert!(
        receiver
            .store
            .state_machine
            .read()
            .await
            .last_applied_log
            .is_none()
    );
    assert!(
        receiver
            .store
            .get_current_snapshot()
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        !receiver_kv.has_pending_intents(b"\0raft\0").unwrap(),
        "failed descriptor commit must release its intent for retry"
    );
    receiver_kv.fail_commit.store(false, Ordering::SeqCst);
    let retry = h.store.build_snapshot().await.unwrap();
    receiver
        .store
        .install_snapshot(&retry.meta, retry.snapshot)
        .await
        .unwrap();
    assert!(read_fence(receiver_kv.as_ref()).unwrap().unwrap().closed);
}

#[tokio::test]
async fn snapshot_cannot_erase_or_roll_back_an_existing_fence() {
    let dir = tempfile::tempdir().unwrap();
    let mut source = Harness::new(
        Arc::new(nodus_storage_mem::MemKvEngine::new()),
        dir.path().join("source"),
    )
    .await;
    let mut receiver = Harness::new(
        Arc::new(nodus_storage_mem::MemKvEngine::new()),
        dir.path().join("receiver"),
    )
    .await;
    let first = Uuid::new_v4();
    assert!(
        receiver
            .control(Control::Acquire {
                operation_id: first,
                expected_epoch: 0
            })
            .await
            .success
    );
    let snapshot = source.store.build_snapshot().await.unwrap();
    assert!(
        receiver
            .store
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .is_err()
    );
    assert!(read_fence(receiver.kv.as_ref()).unwrap().unwrap().closed);
    assert!(
        source
            .control(Control::Acquire {
                operation_id: first,
                expected_epoch: 0
            })
            .await
            .success
    );
    let snapshot = source.store.build_snapshot().await.unwrap();
    assert!(
        receiver
            .control(Control::Release {
                operation_id: first,
                epoch: 1
            })
            .await
            .success
    );
    assert!(
        receiver
            .control(Control::Acquire {
                operation_id: Uuid::new_v4(),
                expected_epoch: 1
            })
            .await
            .success
    );
    assert!(
        receiver
            .store
            .install_snapshot(&snapshot.meta, snapshot.snapshot)
            .await
            .is_err()
    );
    assert_eq!(read_fence(receiver.kv.as_ref()).unwrap().unwrap().epoch, 2);
}

#[tokio::test]
async fn unknown_fence_versions_fail_closed_and_legacy_commands_keep_their_shape() {
    let legacy = r#"{"CommitTxn":{"txn_id":"00000000-0000-0000-0000-000000000001","commit_ts":7}}"#;
    let command: ShardCommand = serde_json::from_str(legacy).unwrap();
    assert!(matches!(
        command,
        ShardCommand::CommitTxn { shard_id: None, .. }
    ));
    let response: ShardResponse = serde_json::from_str(r#"{"success":true}"#).unwrap();
    assert!(response.error.is_none());
    assert_eq!(
        serde_json::to_string(&response).unwrap(),
        r#"{"success":true}"#
    );
    let dir = tempfile::tempdir().unwrap();
    let kv: Arc<dyn KvEngine> = Arc::new(nodus_storage_mem::MemKvEngine::new());
    let txn = TxnId::new();
    kv.write_intent(
        txn,
        Bytes::from_static(b"\x01migration/v1/fence"),
        Bytes::from(nodus_common::versioned::encode(99, b"{}")),
    )
    .unwrap();
    kv.commit(txn, 1).unwrap();
    let mut h = Harness::new(kv, dir.path().into()).await;
    let entry = openraft::Entry {
        log_id: openraft::LogId::new(openraft::CommittedLeaderId::new(1, 1), 1),
        payload: openraft::EntryPayload::Normal(command),
    };
    assert!(h.store.apply_to_state_machine(&[entry]).await.is_err());
    assert!(
        h.store
            .state_machine
            .read()
            .await
            .last_applied_log
            .is_none()
    );
}
