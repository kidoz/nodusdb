use anyhow::Result;
use bytes::Bytes;
use nodus_catalog::TableId;
use nodus_raftstore::ShardCommand;
use nodus_sharding::ShardRouter;
use nodus_storage_api::{IntentReplacement, KeyRange, KvEngine, KvPair, NamespacedKvEngine, Timestamp, TxnId};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use uuid::Uuid;

use crate::multi_raft::{META_SHARD, MultiRaftManager};
use crate::raft_router::RaftRouter;

/// Reserved key prefix under which the meta group stores 2PC coordinator records.
/// The leading NUL keeps these out of any `{table_id}:{pk}` row key space.
const TXN2PC_PREFIX: &[u8] = b"\x00txn2pc\x00";

/// A durable coordinator record for an in-flight cross-shard commit. Its mere
/// existence means the transaction was *decided to commit*; recovery re-drives
/// the commit to every participant and then clears the record.
#[derive(Serialize, Deserialize)]
struct PendingTxn {
    participants: Vec<String>,
    commit_ts: Timestamp,
}

fn record_key(txn_id: &str) -> Vec<u8> {
    let mut k = TXN2PC_PREFIX.to_vec();
    k.extend_from_slice(txn_id.as_bytes());
    k
}

/// `KvEngine` that replicates mutations through Raft and routes each key to the
/// group that owns its shard. A key `"{table_id}:{pk}"` is mapped via the
/// [`ShardRouter`] to a `ShardId`; if this node hosts that data group, the
/// write/read is directed there (over a namespaced view of the local store).
/// Otherwise — unsharded tables, non-row keys (e.g. `i:` index keys), or a shard
/// not hosted here — it falls back to the meta group and the raw store, which is
/// the pre-routing behaviour.
///
/// `commit`/`abort` carry only a `txn_id`, so the engine remembers which groups
/// each transaction wrote to and finalizes on exactly those groups.
///
/// Write methods route through the async [`RaftRouter`] (`blocking_recv`), so
/// they MUST be invoked from a blocking context, never a reactor worker thread.
pub struct RaftKvEngine {
    pub local: Arc<dyn KvEngine>,
    pub router: RaftRouter,
    pub shard_router: Arc<dyn ShardRouter>,
    pub manager: Arc<MultiRaftManager>,
    /// Groups each in-flight transaction has written to, so `commit`/`abort`
    /// target exactly those groups. Cross-shard commit is non-atomic until 2PC
    /// (Phase 4).
    pub txn_groups: Mutex<HashMap<TxnId, HashSet<String>>>,
}

/// Parses the leading `{table_id}` of a row key. Returns `None` for non-row keys
/// (index keys, scalars) or malformed input — those stay on the meta group.
fn parse_table_id(key: &[u8]) -> Option<TableId> {
    let text = std::str::from_utf8(key).ok()?;
    let prefix = text.split(':').next()?;
    Uuid::parse_str(prefix).ok().map(TableId)
}

impl RaftKvEngine {
    /// Resolves the Raft group that owns `key`. Falls back to the meta group
    /// unless the key belongs to a sharded table whose data group is hosted here.
    fn route(&self, key: &[u8]) -> String {
        if let Some(table_id) = parse_table_id(key)
            && let Ok(shard_id) = self.shard_router.locate_key(table_id, key)
        {
            let group_id = MultiRaftManager::data_group_id(shard_id);
            if self.manager.hosts(&group_id) {
                return group_id;
            }
        }
        META_SHARD.to_string()
    }

    /// The local engine view for a group: the raw store for the meta group, a
    /// namespaced view for a data group (matching that group's own engine).
    fn engine_for(&self, group_id: &str) -> Arc<dyn KvEngine> {
        if group_id == META_SHARD {
            self.local.clone()
        } else {
            Arc::new(NamespacedKvEngine::new(self.local.clone(), group_id))
        }
    }

    /// The `shard_id` recorded on a replicated command (`None` for the meta group).
    fn shard_field(group_id: &str) -> Option<String> {
        (group_id != META_SHARD).then(|| group_id.to_string())
    }

    fn record_txn_group(&self, txn_id: TxnId, group_id: &str) {
        self.txn_groups
            .lock()
            .unwrap()
            .entry(txn_id)
            .or_default()
            .insert(group_id.to_string());
    }

    /// Groups to finalize for `txn_id`: those it wrote to, or the meta group if
    /// it wrote nothing through this engine (e.g. read-only or overlay-only).
    fn finalize_targets(&self, txn_id: TxnId) -> Vec<String> {
        let groups = self
            .txn_groups
            .lock()
            .unwrap()
            .remove(&txn_id)
            .unwrap_or_default();
        if groups.is_empty() {
            vec![META_SHARD.to_string()]
        } else {
            groups.into_iter().collect()
        }
    }

    /// Two-phase commit across `participants`. Atomicity rests on a single
    /// durable decision record in the meta group: the transaction's intents are
    /// already replicated, so the commit is "all-or-nothing" around that write —
    /// a crash before it leaves every intent uncommitted (invisible, i.e.
    /// effectively aborted); a crash after it is repaired by [`Self::recover_pending_txns`],
    /// which re-drives the commit to every participant.
    fn commit_cross_shard(
        &self,
        txn_id: TxnId,
        participants: &[String],
        commit_ts: Timestamp,
    ) -> Result<()> {
        let txn = txn_id.0.to_string();

        // Phase 1 — prepare. Confirms each participant's leader is current and
        // ready; a failure here aborts before any decision is recorded.
        for group_id in participants {
            self.router.submit(
                group_id,
                ShardCommand::PrepareTxn {
                    txn_id: txn.clone(),
                    shard_id: Self::shard_field(group_id),
                },
            )?;
        }

        // Decision point — durably record COMMIT before any participant commits.
        let record = serde_json::to_vec(&PendingTxn {
            participants: participants.to_vec(),
            commit_ts,
        })
        .map_err(|e| anyhow::anyhow!("encode 2pc record: {e}"))?;
        self.meta_put_committed(&record_key(&txn), &record, commit_ts)?;

        // Phase 2 — commit every participant, then clear the record. If the
        // clear is lost to a crash, recovery re-commits idempotently and clears.
        self.drive_commit(&txn, participants, commit_ts)?;
        self.meta_delete_committed(&record_key(&txn), commit_ts + 1)?;
        Ok(())
    }

    fn drive_commit(
        &self,
        txn: &str,
        participants: &[String],
        commit_ts: Timestamp,
    ) -> Result<()> {
        for group_id in participants {
            self.router.submit(
                group_id,
                ShardCommand::CommitTxn {
                    txn_id: txn.to_string(),
                    commit_ts,
                    shard_id: Self::shard_field(group_id),
                },
            )?;
        }
        Ok(())
    }

    /// Writes a committed key/value into the meta group via a synthetic
    /// transaction (used for coordinator records).
    fn meta_put_committed(&self, key: &[u8], value: &[u8], version: Timestamp) -> Result<()> {
        let coord = TxnId::new().0.to_string();
        self.router.submit(
            META_SHARD,
            ShardCommand::PutIntent {
                txn_id: coord.clone(),
                key: key.to_vec(),
                value: value.to_vec(),
                shard_id: None,
            },
        )?;
        self.router.submit(
            META_SHARD,
            ShardCommand::CommitTxn {
                txn_id: coord,
                commit_ts: version,
                shard_id: None,
            },
        )
    }

    /// Tombstones a committed key in the meta group (clears a coordinator record).
    fn meta_delete_committed(&self, key: &[u8], version: Timestamp) -> Result<()> {
        let coord = TxnId::new().0.to_string();
        self.router.submit(
            META_SHARD,
            ShardCommand::DeleteIntent {
                txn_id: coord.clone(),
                key: key.to_vec(),
                shard_id: None,
            },
        )?;
        self.router.submit(
            META_SHARD,
            ShardCommand::CommitTxn {
                txn_id: coord,
                commit_ts: version,
                shard_id: None,
            },
        )
    }

    /// Re-drives any cross-shard commit that was decided but not fully applied
    /// before a crash/restart, then clears its record. Idempotent: committing an
    /// already-committed transaction is a no-op. MUST run on a blocking thread
    /// (it submits through the Raft router). Returns the number repaired.
    pub fn recover_pending_txns(&self) -> Result<usize> {
        let mut end = TXN2PC_PREFIX.to_vec();
        *end.last_mut().unwrap() += 1; // prefix successor bounds the scan
        let range = KeyRange {
            start: Bytes::from(TXN2PC_PREFIX.to_vec()),
            end: Bytes::from(end),
        };

        let pending: Vec<(Vec<u8>, PendingTxn)> = self
            .local
            .scan(range, u64::MAX)?
            .filter_map(|r| r.ok())
            .filter_map(|pair| {
                serde_json::from_slice::<PendingTxn>(&pair.value)
                    .ok()
                    .map(|rec| (pair.key.to_vec(), rec))
            })
            .collect();

        let mut repaired = 0;
        for (key, rec) in pending {
            let Some(txn) = key.strip_prefix(TXN2PC_PREFIX) else {
                continue;
            };
            let txn = String::from_utf8_lossy(txn).into_owned();
            self.drive_commit(&txn, &rec.participants, rec.commit_ts)?;
            self.meta_delete_committed(&key, rec.commit_ts + 1)?;
            repaired += 1;
        }
        Ok(repaired)
    }
}

impl KvEngine for RaftKvEngine {
    fn get(&self, key: &[u8], read_ts: Timestamp) -> Result<Option<Bytes>> {
        let group_id = self.route(key);
        self.engine_for(&group_id).get(key, read_ts)
    }

    fn scan(
        &self,
        range: KeyRange,
        read_ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = Result<KvPair>> + Send>> {
        // Routed by the range start; a single-shard table resolves to one group.
        // Multi-shard scatter/gather is deferred to a later phase.
        let group_id = self.route(range.start.as_ref());
        self.engine_for(&group_id).scan(range, read_ts)
    }

    fn write_intent(&self, txn_id: TxnId, key: Bytes, value: Bytes) -> Result<()> {
        let group_id = self.route(&key);
        self.record_txn_group(txn_id, &group_id);
        let cmd = ShardCommand::PutIntent {
            txn_id: txn_id.0.to_string(),
            key: key.to_vec(),
            value: value.to_vec(),
            shard_id: Self::shard_field(&group_id),
        };
        self.router.submit(&group_id, cmd)
    }

    fn delete_intent(&self, txn_id: TxnId, key: Bytes) -> Result<()> {
        let group_id = self.route(&key);
        self.record_txn_group(txn_id, &group_id);
        let cmd = ShardCommand::DeleteIntent {
            txn_id: txn_id.0.to_string(),
            key: key.to_vec(),
            shard_id: Self::shard_field(&group_id),
        };
        self.router.submit(&group_id, cmd)
    }

    fn replace_intent(
        &self,
        txn_id: TxnId,
        key: Bytes,
        replacement: IntentReplacement,
    ) -> Result<()> {
        // Savepoint overlay fix-up is applied locally (not replicated), against
        // the same engine view the key's writes target.
        let group_id = self.route(&key);
        self.engine_for(&group_id)
            .replace_intent(txn_id, key, replacement)
    }

    fn commit(&self, txn_id: TxnId, commit_ts: Timestamp) -> Result<()> {
        let targets = self.finalize_targets(txn_id);
        if targets.len() <= 1 {
            // Single-shard (incl. meta fallback): one atomic group commit.
            let group_id = targets
                .into_iter()
                .next()
                .unwrap_or_else(|| META_SHARD.to_string());
            self.router.submit(
                &group_id,
                ShardCommand::CommitTxn {
                    txn_id: txn_id.0.to_string(),
                    commit_ts,
                    shard_id: Self::shard_field(&group_id),
                },
            )
        } else {
            self.commit_cross_shard(txn_id, &targets, commit_ts)
        }
    }

    fn abort(&self, txn_id: TxnId) -> Result<()> {
        for group_id in self.finalize_targets(txn_id) {
            let cmd = ShardCommand::AbortTxn {
                txn_id: txn_id.0.to_string(),
                shard_id: Self::shard_field(&group_id),
            };
            self.router.submit(&group_id, cmd)?;
        }
        Ok(())
    }

    fn garbage_collect(&self, watermark: Timestamp) -> Result<usize> {
        self.local.garbage_collect(watermark)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodus_raftstore::server::{NodusRaft, RaftState};
    use std::collections::BTreeMap;

    async fn elect(raft: &NodusRaft) {
        let mut members = BTreeMap::new();
        members.insert(1u64, openraft::BasicNode::new("127.0.0.1:0"));
        let _ = raft.initialize(members).await;
        for _ in 0..30 {
            if raft.metrics().borrow().current_leader == Some(1) {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        panic!("group did not elect a leader");
    }

    fn namespaced_key(group_id: &str, logical: &str) -> Vec<u8> {
        let mut k = group_id.as_bytes().to_vec();
        k.push(0u8);
        k.extend_from_slice(logical.as_bytes());
        k
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn writes_route_to_the_owning_shard_namespace_with_meta_fallback() {
        // A single-shard table T, plus an unsharded table U.
        let meta = Arc::new(nodus_meta::MemMetaStore::new());
        let orchestrator = nodus_sharding::ShardOrchestrator::new(meta.clone());
        let table_t = TableId(Uuid::new_v4());
        let shard = orchestrator.init_single_shard(table_t).unwrap();
        let group_id = MultiRaftManager::data_group_id(shard);

        // Manager over a shared base store; meta group + the data group, both led.
        let base: Arc<dyn KvEngine> = Arc::new(nodus_storage_mem::MemKvEngine::new());
        let config = Arc::new(openraft::Config::default().validate().unwrap());
        let manager = Arc::new(MultiRaftManager::new(1, "127.0.0.1:0".into(), config, RaftState::new(), base.clone()));

        let catalog = Arc::new(nodus_catalog::MemoryCatalog::new());
        let upgrade = Arc::new(nodus_upgrade::DefaultUpgradeCoordinator::new(
            1,
            vec!["new_storage_format".into()],
            1,
        ));
        // The meta group shares the base store, exactly as `run_server` wires it
        // (the meta group's engine == `RaftKvEngine.local`).
        let meta_raft = manager
            .create_meta(base.clone(), catalog.clone(), catalog.clone(), upgrade)
            .await
            .unwrap();
        elect(&meta_raft).await;
        let data_raft = manager.get_or_create_data(&group_id).await.unwrap();
        elect(&data_raft).await;

        let shard_router: Arc<dyn ShardRouter> =
            Arc::new(nodus_sharding::CatalogShardRouter::new(meta.clone()));
        let engine = Arc::new(RaftKvEngine {
            local: base.clone(),
            router: RaftRouter::spawn(manager.clone()),
            shard_router,
            manager: manager.clone(),
            txn_groups: Mutex::new(HashMap::new()),
        });

        let row_t = format!("{table_t}:pk1");
        let row_u = format!("{}:pk1", Uuid::new_v4()); // unsharded table

        // Writes go through the (blocking) Raft submit path -> spawn_blocking.
        let e = engine.clone();
        let (rt, ru) = (row_t.clone(), row_u.clone());
        tokio::task::spawn_blocking(move || {
            let t1 = TxnId::new();
            e.write_intent(t1, Bytes::from(rt.into_bytes()), Bytes::from_static(b"vt"))
                .unwrap();
            e.commit(t1, 10).unwrap();
            let t2 = TxnId::new();
            e.write_intent(t2, Bytes::from(ru.into_bytes()), Bytes::from_static(b"vu"))
                .unwrap();
            e.commit(t2, 10).unwrap();
        })
        .await
        .unwrap();

        // Reads route to the same place and observe the values.
        assert_eq!(
            engine.get(row_t.as_bytes(), 100).unwrap(),
            Some(Bytes::from_static(b"vt"))
        );
        assert_eq!(
            engine.get(row_u.as_bytes(), 100).unwrap(),
            Some(Bytes::from_static(b"vu"))
        );

        // The sharded row physically lives in the data namespace, never as a raw
        // key in the base store; the unsharded row lives raw (meta fallback).
        assert_eq!(base.get(row_t.as_bytes(), 100).unwrap(), None);
        assert_eq!(
            base.get(&namespaced_key(&group_id, &row_t), 100).unwrap(),
            Some(Bytes::from_static(b"vt"))
        );
        assert_eq!(
            base.get(row_u.as_bytes(), 100).unwrap(),
            Some(Bytes::from_static(b"vu"))
        );
    }

    /// A two-data-shard cluster (tables T and U on distinct, hosted, led groups),
    /// returning the engine plus each table id and its group id.
    struct TwoShard {
        engine: Arc<RaftKvEngine>,
        table_t: TableId,
        group_t: String,
        table_u: TableId,
        group_u: String,
    }

    async fn setup_two_shards() -> TwoShard {
        let meta = Arc::new(nodus_meta::MemMetaStore::new());
        let orchestrator = nodus_sharding::ShardOrchestrator::new(meta.clone());
        let table_t = TableId(Uuid::new_v4());
        let table_u = TableId(Uuid::new_v4());
        let group_t = MultiRaftManager::data_group_id(orchestrator.init_single_shard(table_t).unwrap());
        let group_u = MultiRaftManager::data_group_id(orchestrator.init_single_shard(table_u).unwrap());

        let base: Arc<dyn KvEngine> = Arc::new(nodus_storage_mem::MemKvEngine::new());
        let config = Arc::new(openraft::Config::default().validate().unwrap());
        let manager = Arc::new(MultiRaftManager::new(1, "127.0.0.1:0".into(), config, RaftState::new(), base.clone()));

        let catalog = Arc::new(nodus_catalog::MemoryCatalog::new());
        let upgrade = Arc::new(nodus_upgrade::DefaultUpgradeCoordinator::new(
            1,
            vec!["new_storage_format".into()],
            1,
        ));
        elect(&manager.create_meta(base.clone(), catalog.clone(), catalog, upgrade).await.unwrap()).await;
        elect(&manager.get_or_create_data(&group_t).await.unwrap()).await;
        elect(&manager.get_or_create_data(&group_u).await.unwrap()).await;

        let engine = Arc::new(RaftKvEngine {
            local: base.clone(),
            router: RaftRouter::spawn(manager.clone()),
            shard_router: Arc::new(nodus_sharding::CatalogShardRouter::new(meta)),
            manager,
            txn_groups: Mutex::new(HashMap::new()),
        });
        TwoShard {
            engine,
            table_t,
            group_t,
            table_u,
            group_u,
        }
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cross_shard_commit_is_atomic_and_leaves_no_record() {
        let fx = setup_two_shards().await;
        let engine = fx.engine.clone();
        let row_t = format!("{}:pk", fx.table_t);
        let row_u = format!("{}:pk", fx.table_u);

        // One transaction writes to two distinct shard groups, then commits.
        let (e, rt, ru) = (engine.clone(), row_t.clone(), row_u.clone());
        let recovered = tokio::task::spawn_blocking(move || {
            let txn = TxnId::new();
            e.write_intent(txn, Bytes::from(rt.into_bytes()), Bytes::from_static(b"vt"))
                .unwrap();
            e.write_intent(txn, Bytes::from(ru.into_bytes()), Bytes::from_static(b"vu"))
                .unwrap();
            e.commit(txn, 10).unwrap();
            // A clean 2PC commit clears its coordinator record.
            e.recover_pending_txns().unwrap()
        })
        .await
        .unwrap();

        assert_eq!(recovered, 0, "a completed commit must leave no pending record");
        assert_eq!(
            engine.get(row_t.as_bytes(), 100).unwrap(),
            Some(Bytes::from_static(b"vt"))
        );
        assert_eq!(
            engine.get(row_u.as_bytes(), 100).unwrap(),
            Some(Bytes::from_static(b"vu"))
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn recovery_redrives_a_decided_but_incomplete_cross_shard_commit() {
        let fx = setup_two_shards().await;
        let engine = fx.engine.clone();
        let row_t = format!("{}:pk", fx.table_t);
        let row_u = format!("{}:pk", fx.table_u);
        let (group_t, group_u) = (fx.group_t.clone(), fx.group_u.clone());

        // Simulate a crash after the COMMIT decision was recorded but before the
        // second participant (group U) committed: intents on both, decision
        // recorded, only group T committed.
        let (e, rt, ru) = (engine.clone(), row_t.clone(), row_u.clone());
        let txn_str = tokio::task::spawn_blocking(move || {
            let txn = TxnId::new();
            e.write_intent(txn, Bytes::from(rt.into_bytes()), Bytes::from_static(b"vt"))
                .unwrap();
            e.write_intent(txn, Bytes::from(ru.into_bytes()), Bytes::from_static(b"vu"))
                .unwrap();
            let txn_str = txn.0.to_string();
            let record = serde_json::to_vec(&PendingTxn {
                participants: vec![group_t.clone(), group_u.clone()],
                commit_ts: 10,
            })
            .unwrap();
            e.meta_put_committed(&record_key(&txn_str), &record, 10).unwrap();
            e.drive_commit(&txn_str, std::slice::from_ref(&group_t), 10).unwrap(); // only T commits
            txn_str
        })
        .await
        .unwrap();

        // Group T is visible; group U is still an uncommitted (invisible) intent.
        assert_eq!(
            engine.get(row_t.as_bytes(), 100).unwrap(),
            Some(Bytes::from_static(b"vt"))
        );
        assert_eq!(engine.get(row_u.as_bytes(), 100).unwrap(), None);

        // Recovery re-drives the decided commit to every participant.
        let e = engine.clone();
        let repaired = tokio::task::spawn_blocking(move || e.recover_pending_txns().unwrap())
            .await
            .unwrap();
        assert_eq!(repaired, 1);
        assert_eq!(
            engine.get(row_u.as_bytes(), 100).unwrap(),
            Some(Bytes::from_static(b"vu"))
        );

        // Idempotent: once cleared, a second recovery finds nothing.
        let e = engine.clone();
        let again = tokio::task::spawn_blocking(move || e.recover_pending_txns().unwrap())
            .await
            .unwrap();
        assert_eq!(again, 0, "txn {txn_str} record should be cleared");
    }
}
