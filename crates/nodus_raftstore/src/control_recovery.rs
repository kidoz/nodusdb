//! Startup-only recovery of unpublished local consensus metadata writes.
//! The caller exclusively owns this group's storage: no old Raft task may
//! still be writing it. User transactions and replicated authority records
//! are outside this reserved namespace and must never be discarded here.
use super::*;

const PREFIX: &[u8] = b"\0raft\0";

pub(super) fn discard_unpublished(kv: &dyn KvEngine) -> anyhow::Result<()> {
    if !kv.has_pending_intents(PREFIX)? {
        return Ok(());
    }
    let pending = kv.pending_intents(b"")?;
    let mut internal = Vec::new();
    // Validate every candidate before changing anything. A transaction that
    // mixes user and consensus keys is corruption, not permission to abort it.
    for txn in pending {
        if txn.keys.iter().any(|key| key.starts_with(PREFIX)) {
            anyhow::ensure!(
                txn.keys.iter().all(|key| key.starts_with(PREFIX)),
                "unfinished Raft metadata transaction also contains user keys"
            );
            internal.push(txn.txn_id);
        }
    }
    anyhow::ensure!(
        !internal.is_empty(),
        "pending Raft metadata inspection is inconsistent"
    );
    for txn in &internal {
        kv.abort_physical(*txn)?;
    }
    tracing::info!(
        transactions = internal.len(),
        "discarded unpublished local Raft metadata writes during exclusive recovery"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodus_storage_lsm::LsmKvEngine;
    use nodus_storage_mem::MemKvEngine;

    #[tokio::test]
    async fn recovered_metadata_intents_do_not_lock_applied_state_or_delete_user_intents() {
        for namespaced in [false, true] {
            let dir = tempfile::tempdir().unwrap();
            let user = TxnId::new();
            {
                let raw = Arc::new(LsmKvEngine::with_wal(dir.path().join("kv"), None).unwrap());
                let kv: Arc<dyn KvEngine> = if namespaced {
                    Arc::new(nodus_storage_api::NamespacedKvEngine::new(raw, "shard-a"))
                } else {
                    raw
                };
                let mut store = NodusRaftStore::with_kv(kv.clone()).unwrap();
                let entry = Entry {
                    log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), 1),
                    payload: EntryPayload::Blank,
                };
                store.append_to_log(vec![entry.clone()]).await.unwrap();
                store.apply_to_state_machine(&[entry]).await.unwrap();
                kv.write_intent(
                    user,
                    Bytes::from_static(b"user"),
                    Bytes::from_static(b"keep"),
                )
                .unwrap();
                kv.write_intent(
                    TxnId::new(),
                    Bytes::from_static(RAFT_APPLIED_KEY),
                    Bytes::from_static(b"unpublished"),
                )
                .unwrap();
            }
            let raw = Arc::new(LsmKvEngine::with_wal(dir.path().join("kv"), None).unwrap());
            let kv: Arc<dyn KvEngine> = if namespaced {
                Arc::new(nodus_storage_api::NamespacedKvEngine::new(raw, "shard-a"))
            } else {
                raw
            };
            let mut store = NodusRaftStore::with_kv(kv.clone()).unwrap();
            assert!(!kv.has_pending_intents(PREFIX).unwrap());
            assert_eq!(
                kv.pending_intent_keys(user),
                vec![Bytes::from_static(b"user")]
            );
            assert_eq!(
                store
                    .state_machine
                    .read()
                    .await
                    .last_applied_log
                    .unwrap()
                    .index,
                1
            );
            let entry = Entry {
                log_id: LogId::new(openraft::CommittedLeaderId::new(1, 1), 2),
                payload: EntryPayload::Blank,
            };
            store.append_to_log(vec![entry.clone()]).await.unwrap();
            store.apply_to_state_machine(&[entry]).await.unwrap();
            assert_eq!(
                kv.pending_intent_keys(user),
                vec![Bytes::from_static(b"user")]
            );
        }
    }

    #[test]
    fn mixed_metadata_and_user_intents_fail_without_discarding_either() {
        let kv = Arc::new(MemKvEngine::new());
        let txn = TxnId::new();
        for key in [RAFT_APPLIED_KEY, b"user".as_slice()] {
            kv.write_intent(
                txn,
                Bytes::copy_from_slice(key),
                Bytes::from_static(b"unpublished"),
            )
            .unwrap();
        }
        let result = NodusRaftStore::with_kv(kv.clone());
        assert!(
            result
                .err()
                .unwrap()
                .to_string()
                .contains("also contains user keys")
        );
        assert_eq!(kv.pending_intent_keys(txn).len(), 2);
    }
}
