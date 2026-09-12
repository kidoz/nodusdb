mod epochs;
mod routing;
#[cfg(test)]
mod routing_tests;

use anyhow::Result;
use bytes::Bytes;
use nodus_catalog::TableId;
use nodus_raftstore::ShardCommand;
use nodus_sharding::ShardRouter;
use nodus_storage_api::{
    IntentReplacement, KeyRange, KvEngine, KvPair, KvResult, NamespacedKvEngine, Timestamp, TxnId,
};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap, HashSet};
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
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    epochs: BTreeMap<String, u64>,
}

fn record_key(txn_id: &str) -> Vec<u8> {
    let mut k = TXN2PC_PREFIX.to_vec();
    k.extend_from_slice(txn_id.as_bytes());
    k
}

/// `KvEngine` that replicates mutations through Raft and routes each key to the
/// group that owns its shard. A key `"{table_id}:{pk}"` is mapped via the
/// [`ShardRouter`] to a `ShardId`; reads and writes use a namespaced view of the
/// local store for that group.
/// Missing assigned replicas and metadata errors fail explicitly. Only tables
/// with no shard map and non-row keys (e.g. `i:` index keys) use the meta group.
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
    /// target exactly those groups.
    pub txn_groups: Mutex<HashMap<TxnId, BTreeMap<String, TxnParticipant>>>,
    /// Observability for the cross-shard commit/recovery paths.
    pub metrics: nodus_monitoring::Metrics,
}

/// In-memory write set: only acknowledged mutations change `intents`. A
/// successfully replicated Clear can make a participant empty without losing
/// the epoch that the still-live transaction must present at prepare/commit.
#[derive(Clone, Default)]
pub struct TxnParticipant {
    epoch: u64,
    intents: HashSet<Vec<u8>>,
}

/// Parses the leading `{table_id}` of a row key. Returns `None` for non-row keys
/// (index keys, scalars) or malformed input — those stay on the meta group.
fn parse_table_id(key: &[u8]) -> Option<TableId> {
    if key.get(36) != Some(&b':') {
        return None;
    }
    let prefix = std::str::from_utf8(key.get(..36)?).ok()?;
    Uuid::parse_str(prefix).ok().map(TableId)
}

impl RaftKvEngine {
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

    /// Capture once, even if a savepoint later clears every intent. A stale
    /// transaction must fail instead of adopting a newly opened epoch.
    fn record_txn_group(&self, txn_id: TxnId, group_id: &str) -> Result<u64> {
        let mut txns = self.txn_groups.lock().unwrap();
        let groups = txns.entry(txn_id).or_default();
        if let Some(participant) = groups.get(group_id) {
            return Ok(participant.epoch);
        }
        let epoch = self.current_epoch(group_id)?;
        groups.insert(
            group_id.to_string(),
            TxnParticipant {
                epoch,
                intents: HashSet::new(),
            },
        );
        Ok(epoch)
    }

    fn record_intent(&self, txn: TxnId, group: &str, key: &[u8], live: bool) {
        if let Some(participant) = self
            .txn_groups
            .lock()
            .unwrap()
            .get_mut(&txn)
            .and_then(|groups| groups.get_mut(group))
        {
            if live {
                participant.intents.insert(key.to_vec());
            } else {
                participant.intents.remove(key);
            }
        }
    }

    fn finalize_targets(&self, txn_id: TxnId) -> BTreeMap<String, TxnParticipant> {
        self.txn_groups
            .lock()
            .unwrap()
            .remove(&txn_id)
            .unwrap_or_default()
    }

    /// Prepare participants, persist the meta decision, then drive commits.
    /// Recovery retains the decision's recorded epochs. Uncertain decisions and
    /// orphan intents still need further recovery work; migration quiescence
    /// remains blocked while such intents or decisions exist.
    fn commit_cross_shard(
        &self,
        txn_id: TxnId,
        targets: &BTreeMap<String, TxnParticipant>,
        commit_ts: Timestamp,
    ) -> Result<()> {
        let epochs: BTreeMap<String, u64> = targets
            .iter()
            .map(|(group, target)| (group.clone(), target.epoch))
            .collect();
        let participants: Vec<String> = epochs.keys().cloned().collect();
        let txn = txn_id.0.to_string();
        let _span = tracing::info_span!("txn.cross_shard_commit", txn = %txn, participants = participants.len()).entered();

        // Phase 1 — prepare. Participants verify expected live intents or an
        // acknowledged savepoint clear. Any NO vote — or a participant we
        // can't reach to ask — aborts the whole transaction before any commit
        // decision is recorded, so a lost intent can never produce a torn commit.
        let mut prepared = true;
        for group_id in &participants {
            let target = &targets[group_id];
            let cmd = if self.router.migration_enabled() || target.epoch != 0 {
                ShardCommand::EpochPrepareV2 {
                    epoch: target.epoch,
                    txn_id: txn_id.0,
                    expect_intents: !target.intents.is_empty(),
                }
            } else {
                // Legacy Clear is local; its completed write set is known here.
                if target.intents.is_empty() {
                    continue;
                }
                self.epoch_command(
                    group_id,
                    target.epoch,
                    nodus_raftstore::migration::EpochMutationV1::Prepare { txn_id: txn_id.0 },
                )
            };
            match self.router.submit_voting(group_id, cmd) {
                Ok(resp) if resp.success => {}
                Ok(_) => {
                    tracing::warn!("cross-shard prepare: {group_id} voted NO for txn {txn}");
                    prepared = false;
                    break;
                }
                Err(e) => {
                    tracing::warn!(
                        "cross-shard prepare: {group_id} unreachable for txn {txn}: {e}"
                    );
                    prepared = false;
                    break;
                }
            }
        }
        if !prepared {
            // No decision was recorded, so the intents are simply discarded: the
            // transaction commits nowhere. Abort every participant to release them.
            self.abort_participants(&txn, &participants);
            self.metrics.cross_shard_aborts_total.inc();
            anyhow::bail!("cross-shard transaction aborted: a participant could not prepare");
        }

        // Decision point — durably record COMMIT before any participant commits.
        let record = PendingTxn {
            participants: participants.to_vec(),
            commit_ts,
            epochs: if self.router.migration_enabled() || epochs.values().any(|e| *e != 0) {
                epochs.clone()
            } else {
                BTreeMap::new()
            },
        }
        .encode()?;
        self.meta_put_committed(&record_key(&txn), &record, commit_ts)?;

        // Phase 2 — commit every participant, then clear the record. If the
        // clear is lost to a crash, recovery re-commits idempotently and clears.
        self.drive_commit(&txn, &participants, commit_ts, &epochs)?;
        self.meta_delete_committed(&record_key(&txn), commit_ts + 1)?;
        self.metrics.cross_shard_commits_total.inc();
        Ok(())
    }

    /// Aborts a transaction on every participant (best-effort): releases their
    /// intents when a prepare vote fails. Errors are ignored — an unreachable
    /// participant's intents are uncommitted, hence invisible, so the outcome is
    /// still "committed nowhere".
    fn abort_participants(&self, txn: &str, participants: &[String]) {
        for group_id in participants {
            let _ = self.router.submit(
                group_id,
                ShardCommand::AbortTxn {
                    txn_id: txn.to_string(),
                    shard_id: Self::shard_field(group_id),
                },
            );
        }
    }

    fn drive_commit(
        &self,
        txn: &str,
        participants: &[String],
        commit_ts: Timestamp,
        epochs: &BTreeMap<String, u64>,
    ) -> Result<()> {
        let txn_id = Uuid::parse_str(txn)?;
        for group_id in participants {
            self.router.submit(
                group_id,
                self.epoch_command(
                    group_id,
                    epochs.get(group_id).copied().unwrap_or(0),
                    nodus_raftstore::migration::EpochMutationV1::Commit { txn_id, commit_ts },
                ),
            )?;
        }
        Ok(())
    }

    /// Writes a committed key/value into the meta group via a synthetic
    /// transaction (used for coordinator records).
    fn meta_put_committed(&self, key: &[u8], value: &[u8], version: Timestamp) -> Result<()> {
        let coord = TxnId::new().0;
        let epoch = self.current_epoch(META_SHARD)?;
        self.router.submit(
            META_SHARD,
            self.epoch_command(
                META_SHARD,
                epoch,
                nodus_raftstore::migration::EpochMutationV1::Put {
                    txn_id: coord,
                    key: key.to_vec(),
                    value: value.to_vec(),
                },
            ),
        )?;
        self.router.submit(
            META_SHARD,
            self.epoch_command(
                META_SHARD,
                epoch,
                nodus_raftstore::migration::EpochMutationV1::Commit {
                    txn_id: coord,
                    commit_ts: version,
                },
            ),
        )
    }

    fn meta_delete_committed(&self, key: &[u8], version: Timestamp) -> Result<()> {
        let coord = TxnId::new().0;
        let epoch = self.current_epoch(META_SHARD)?;
        self.router.submit(
            META_SHARD,
            self.epoch_command(
                META_SHARD,
                epoch,
                nodus_raftstore::migration::EpochMutationV1::Delete {
                    txn_id: coord,
                    key: key.to_vec(),
                },
            ),
        )?;
        self.router.submit(
            META_SHARD,
            self.epoch_command(
                META_SHARD,
                epoch,
                nodus_raftstore::migration::EpochMutationV1::Commit {
                    txn_id: coord,
                    commit_ts: version,
                },
            ),
        )
    }

    /// Re-drives any cross-shard commit that was decided but not fully applied
    /// before a crash/restart, then clears its record. Idempotent: committing an
    /// already-committed transaction is a no-op. MUST run on a blocking thread
    /// (it submits through the Raft router). Returns the number repaired.
    pub fn recover_pending_txns(&self) -> Result<usize> {
        let _span = tracing::info_span!("txn.recover_pending").entered();
        let mut end = TXN2PC_PREFIX.to_vec();
        *end.last_mut().unwrap() += 1; // prefix successor bounds the scan
        let range = KeyRange {
            start: Bytes::from(TXN2PC_PREFIX.to_vec()),
            end: Bytes::from(end),
        };

        let mut repaired = 0;
        for pair in self.local.scan(range, u64::MAX)? {
            let pair = pair?;
            let rec = PendingTxn::decode(&pair.value)?;
            let txn = std::str::from_utf8(&pair.key[TXN2PC_PREFIX.len()..])?;
            self.drive_commit(txn, &rec.participants, rec.commit_ts, &rec.epochs)?;
            self.meta_delete_committed(
                &pair.key,
                rec.commit_ts
                    .checked_add(1)
                    .ok_or_else(|| anyhow::anyhow!("2PC timestamp exhausted"))?,
            )?;
            repaired += 1;
        }
        self.metrics.txn_recoveries_total.inc_by(repaired as u64);
        Ok(repaired)
    }
}

impl KvEngine for RaftKvEngine {
    fn get(&self, key: &[u8], read_ts: Timestamp) -> Result<Option<Bytes>> {
        let group_id = self.route(key)?;
        self.engine_for(&group_id).get(key, read_ts)
    }

    fn scan(
        &self,
        range: KeyRange,
        read_ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = Result<KvPair>> + Send>> {
        self.routed_scan(range, move |engine, range| engine.scan(range, read_ts))
    }

    fn scan_versions(
        &self,
        range: KeyRange,
        since_ts: Timestamp,
        read_ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = Result<nodus_storage_api::KvVersion>> + Send>> {
        self.routed_scan(range, move |engine, range| {
            engine.scan_versions(range, since_ts, read_ts)
        })
    }

    fn write_intent(&self, txn_id: TxnId, key: Bytes, value: Bytes) -> KvResult<()> {
        let group_id = self.route(&key)?;
        let epoch = self.record_txn_group(txn_id, &group_id)?;
        let cmd = self.epoch_command(
            &group_id,
            epoch,
            nodus_raftstore::migration::EpochMutationV1::Put {
                txn_id: txn_id.0,
                key: key.to_vec(),
                value: value.to_vec(),
            },
        );
        self.router.submit(&group_id, cmd)?;
        self.record_intent(txn_id, &group_id, &key, true);
        Ok(())
    }

    fn delete_intent(&self, txn_id: TxnId, key: Bytes) -> KvResult<()> {
        let group_id = self.route(&key)?;
        let epoch = self.record_txn_group(txn_id, &group_id)?;
        let cmd = self.epoch_command(
            &group_id,
            epoch,
            nodus_raftstore::migration::EpochMutationV1::Delete {
                txn_id: txn_id.0,
                key: key.to_vec(),
            },
        );
        self.router.submit(&group_id, cmd)?;
        self.record_intent(txn_id, &group_id, &key, true);
        Ok(())
    }

    fn replace_intent(
        &self,
        txn_id: TxnId,
        key: Bytes,
        replacement: IntentReplacement,
    ) -> KvResult<()> {
        let group_id = self.route(&key)?;
        let epoch = self.record_txn_group(txn_id, &group_id)?;
        let live = !matches!(replacement, IntentReplacement::Clear);
        if self.router.migration_enabled() || epoch != 0 {
            self.router.submit(
                &group_id,
                ShardCommand::EpochRepairV2 {
                    epoch,
                    txn_id: txn_id.0,
                    key: key.to_vec(),
                    replacement: replacement.into(),
                },
            )?;
        } else {
            self.router
                .repair_legacy(&group_id, txn_id, key.clone(), replacement)?;
        }
        self.record_intent(txn_id, &group_id, &key, live);
        Ok(())
    }

    fn commit(&self, txn_id: TxnId, commit_ts: Timestamp) -> KvResult<()> {
        let targets = self.finalize_targets(txn_id);
        if targets.len() <= 1 {
            for (group, target) in &targets {
                self.router.submit(
                    group,
                    self.epoch_command(
                        group,
                        target.epoch,
                        nodus_raftstore::migration::EpochMutationV1::Commit {
                            txn_id: txn_id.0,
                            commit_ts,
                        },
                    ),
                )?;
            }
        } else {
            self.commit_cross_shard(txn_id, &targets, commit_ts)?;
        }
        Ok(())
    }

    fn abort(&self, txn_id: TxnId) -> KvResult<()> {
        for group_id in self.finalize_targets(txn_id).keys() {
            self.router.submit(
                group_id,
                ShardCommand::AbortTxn {
                    txn_id: txn_id.0.to_string(),
                    shard_id: Self::shard_field(group_id),
                },
            )?;
        }
        Ok(())
    }

    fn read_barrier(&self, key: &[u8]) -> KvResult<()> {
        // Barrier the group that owns the key, exactly where its reads route.
        let group_id = self.route(key)?;
        self.router.read_barrier(&group_id)?;
        self.metrics.linearizable_reads_total.inc();
        Ok(())
    }

    fn read_range_barrier(&self, range: KeyRange) -> KvResult<()> {
        self.range_barrier(range)?;
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
        let manager = Arc::new(MultiRaftManager::new(
            1,
            "127.0.0.1:0".into(),
            config,
            RaftState::new(),
            base.clone(),
            None,
            Arc::new(nodus_txn::MemTxnManager::new()),
            None,
            nodus_raftstore::network::RaftTransport::default(),
        ));

        let catalog = Arc::new(nodus_catalog::MemoryCatalog::new());
        let upgrade = Arc::new(nodus_upgrade::DefaultUpgradeCoordinator::new(
            1,
            vec!["new_storage_format".into()],
            1,
        ));
        // The meta group shares the base store, exactly as `run_server` wires it
        // (the meta group's engine == `RaftKvEngine.local`).
        let meta_raft = manager
            .create_meta(
                base.clone(),
                catalog.clone(),
                catalog.clone(),
                upgrade,
                Arc::new(nodus_meta::MemMetaStore::new()),
            )
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
            metrics: nodus_monitoring::Metrics::default(),
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
        let group_t =
            MultiRaftManager::data_group_id(orchestrator.init_single_shard(table_t).unwrap());
        let group_u =
            MultiRaftManager::data_group_id(orchestrator.init_single_shard(table_u).unwrap());

        let base: Arc<dyn KvEngine> = Arc::new(nodus_storage_mem::MemKvEngine::new());
        let config = Arc::new(openraft::Config::default().validate().unwrap());
        let manager = Arc::new(MultiRaftManager::new(
            1,
            "127.0.0.1:0".into(),
            config,
            RaftState::new(),
            base.clone(),
            None,
            Arc::new(nodus_txn::MemTxnManager::new()),
            None,
            nodus_raftstore::network::RaftTransport::default(),
        ));

        let catalog = Arc::new(nodus_catalog::MemoryCatalog::new());
        let upgrade = Arc::new(nodus_upgrade::DefaultUpgradeCoordinator::new(
            1,
            vec!["new_storage_format".into()],
            1,
        ));
        elect(
            &manager
                .create_meta(
                    base.clone(),
                    catalog.clone(),
                    catalog,
                    upgrade,
                    Arc::new(nodus_meta::MemMetaStore::new()),
                )
                .await
                .unwrap(),
        )
        .await;
        elect(&manager.get_or_create_data(&group_t).await.unwrap()).await;
        elect(&manager.get_or_create_data(&group_u).await.unwrap()).await;

        let engine = Arc::new(RaftKvEngine {
            local: base.clone(),
            router: RaftRouter::spawn(manager.clone()),
            shard_router: Arc::new(nodus_sharding::CatalogShardRouter::new(meta)),
            manager,
            txn_groups: Mutex::new(HashMap::new()),
            metrics: nodus_monitoring::Metrics::default(),
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

        assert_eq!(
            recovered, 0,
            "a completed commit must leave no pending record"
        );
        assert_eq!(
            engine.metrics.cross_shard_commits_total.get(),
            1,
            "the cross-shard commit should be counted"
        );
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
    async fn cross_shard_commit_aborts_when_a_participant_cannot_prepare() {
        let fx = setup_two_shards().await;
        let engine = fx.engine.clone();
        let row_t = format!("{}:pk", fx.table_t);
        let row_u = format!("{}:pk", fx.table_u);
        let group_u = fx.group_u.clone();

        let (e, rt, ru, gu) = (
            engine.clone(),
            row_t.clone(),
            row_u.clone(),
            group_u.clone(),
        );
        let result = tokio::task::spawn_blocking(move || {
            let txn = TxnId::new();
            e.write_intent(txn, Bytes::from(rt.into_bytes()), Bytes::from_static(b"vt"))
                .unwrap();
            e.write_intent(txn, Bytes::from(ru.into_bytes()), Bytes::from_static(b"vu"))
                .unwrap();
            // Simulate group U losing this transaction's intent (GC'd, aborted, or
            // never replicated there) by aborting it on U alone, before commit.
            e.router
                .submit(
                    &gu,
                    ShardCommand::AbortTxn {
                        txn_id: txn.0.to_string(),
                        shard_id: Some(gu.clone()),
                    },
                )
                .unwrap();
            // The cross-shard commit must now FAIL (U can't prepare) rather than
            // committing T and silently dropping U — a torn write.
            e.commit(txn, 10)
        })
        .await
        .unwrap();

        assert!(
            result.is_err(),
            "commit must fail when a participant cannot prepare"
        );
        assert_eq!(engine.metrics.cross_shard_aborts_total.get(), 1);
        assert_eq!(
            engine.metrics.cross_shard_commits_total.get(),
            0,
            "an aborted transaction must not be counted as committed"
        );
        // All-or-nothing: neither participant committed.
        assert_eq!(
            engine.get(row_t.as_bytes(), 100).unwrap(),
            None,
            "T must not commit when U aborts"
        );
        assert_eq!(engine.get(row_u.as_bytes(), 100).unwrap(), None);
        // No commit decision was recorded, so recovery has nothing to re-drive.
        let e = engine.clone();
        let pending = tokio::task::spawn_blocking(move || e.recover_pending_txns().unwrap())
            .await
            .unwrap();
        assert_eq!(pending, 0, "no commit decision should have been recorded");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn read_barrier_succeeds_on_the_leader() {
        // On a single-node group this node is the leader, so the linearizable
        // barrier resolves via `ensure_linearizable` (a self-quorum heartbeat)
        // without needing a follower read-index round-trip.
        let fx = setup_two_shards().await;
        let engine = fx.engine.clone();
        let row_t = format!("{}:pk", fx.table_t);

        let e = engine.clone();
        let key = row_t.clone();
        tokio::task::spawn_blocking(move || e.read_barrier(key.as_bytes()).unwrap())
            .await
            .unwrap();

        assert_eq!(
            engine.metrics.linearizable_reads_total.get(),
            1,
            "a completed barrier should be counted"
        );
        // The barrier is a pure precondition: a read after it still works.
        assert_eq!(engine.get(row_t.as_bytes(), 100).unwrap(), None);
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
                epochs: BTreeMap::new(),
            })
            .unwrap();
            e.meta_put_committed(&record_key(&txn_str), &record, 10)
                .unwrap();
            e.drive_commit(
                &txn_str,
                std::slice::from_ref(&group_t),
                10,
                &BTreeMap::new(),
            )
            .unwrap(); // only T commits
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
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn epoch_decisions_recover_without_restamping_participants() {
        let mut fx = setup_two_shards().await;
        Arc::get_mut(&mut fx.engine)
            .unwrap()
            .router
            .enable_migration_for_test();
        for group in [&fx.group_t, &fx.group_u] {
            let raft = fx.engine.manager.get(group).await.unwrap();
            let operation_id = Uuid::new_v4();
            for command in [
                nodus_raftstore::migration::MigrationCommandV1::Acquire {
                    operation_id,
                    expected_epoch: 0,
                },
                nodus_raftstore::migration::MigrationCommandV1::Release {
                    operation_id,
                    epoch: 1,
                },
            ] {
                assert!(
                    raft.client_write(ShardCommand::MigrationV1(command))
                        .await
                        .unwrap()
                        .data
                        .success
                );
            }
        }
        let row_t = Bytes::from(format!("{}:pk", fx.table_t));
        let row_u = Bytes::from(format!("{}:pk", fx.table_u));
        let e = fx.engine.clone();
        let groups = vec![fx.group_t, fx.group_u];
        tokio::task::spawn_blocking(move || {
            let txn = TxnId::new();
            e.write_intent(txn, row_t.clone(), Bytes::from_static(b"t"))
                .unwrap();
            e.write_intent(txn, row_u.clone(), Bytes::from_static(b"u"))
                .unwrap();
            let epochs = groups.iter().map(|group| (group.clone(), 1)).collect();
            let record = PendingTxn {
                participants: groups.clone(),
                commit_ts: 100,
                epochs,
            };
            let bytes = record.encode().unwrap();
            assert!(matches!(
                nodus_common::versioned::decode(&bytes),
                nodus_common::versioned::Envelope::Versioned { version: 2, .. }
            ));
            e.meta_put_committed(&record_key(&txn.0.to_string()), &bytes, 100)
                .unwrap();
            e.drive_commit(&txn.0.to_string(), &groups[..1], 100, &record.epochs)
                .unwrap();
            let recovered = RaftKvEngine {
                local: e.local.clone(),
                router: e.router.clone(),
                shard_router: e.shard_router.clone(),
                manager: e.manager.clone(),
                txn_groups: Mutex::new(HashMap::new()),
                metrics: Default::default(),
            };
            assert_eq!(recovered.recover_pending_txns().unwrap(), 1);
            assert_eq!(
                recovered.get(&row_t, 100).unwrap(),
                Some(Bytes::from_static(b"t"))
            );
            assert_eq!(
                recovered.get(&row_u, 100).unwrap(),
                Some(Bytes::from_static(b"u"))
            );
            assert_eq!(recovered.recover_pending_txns().unwrap(), 0);
            // An old decision must fail, not be silently restamped to epoch 1.
            let stale = TxnId::new();
            e.write_intent(stale, row_t.clone(), Bytes::from_static(b"bad"))
                .unwrap();
            let record = PendingTxn {
                participants: groups[..1].to_vec(),
                commit_ts: 200,
                epochs: BTreeMap::from([(groups[0].clone(), 0)]),
            };
            e.meta_put_committed(
                &record_key(&stale.0.to_string()),
                &record.encode().unwrap(),
                200,
            )
            .unwrap();
            assert!(recovered.recover_pending_txns().is_err());
            assert_eq!(
                recovered.get(&row_t, 200).unwrap(),
                Some(Bytes::from_static(b"t"))
            );
            e.abort(stale).unwrap();
        })
        .await
        .unwrap();
        fx.engine.manager.shutdown_all().await;
    }

    #[test]
    fn decision_formats_preserve_legacy_bytes_and_reject_unknown_epochs() {
        let legacy = br#"{"participants":["a"],"commit_ts":7}"#;
        let record = PendingTxn::decode(legacy).unwrap();
        assert_eq!(record.encode().unwrap(), legacy);
        assert!(PendingTxn::decode(&nodus_common::versioned::encode(99, legacy)).is_err());
        assert!(PendingTxn::decode(&nodus_common::versioned::encode(2, legacy)).is_err());
        assert!(
            PendingTxn::decode(br#"{"participants":["a"],"commit_ts":7,"epochs":{"a":1}}"#)
                .is_err()
        );
    }
}
