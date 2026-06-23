//! Manages this node's Raft groups: one `shard-meta` group for catalog/RBAC and
//! cluster metadata, plus zero or more data-shard groups that each replicate a
//! disjoint slice of the key space.
//!
//! Phase 1 establishes the *capability* — a node can host the meta group plus
//! independent per-shard groups, each with an isolated KV namespace (so a
//! group's snapshot only contains its own keys). Routing user writes to data
//! groups (deciding which group owns a key) is Phase 2; placement-driven group
//! lifecycle ([`MultiRaftManager::reconcile`]) is Phase 5.

use anyhow::Result;
use bytes::Bytes;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, RwLock};

use nodus_catalog::{ShardId, TableId};
use nodus_raftstore::NodusRaftStore;
use nodus_raftstore::network::NodusNetworkFactory;
use nodus_raftstore::server::{NodusRaft, RaftState};
use nodus_sharding::ShardOrchestrator;
use nodus_storage_api::{KeyRange, KvEngine, NamespacedKvEngine, TxnId};

/// Identifier of the metadata group that owns catalog, RBAC, and cluster state.
pub const META_SHARD: &str = "shard-meta";

/// Owns the shared `RaftState` group map and the machinery to spin up new Raft
/// groups on demand. Cheap to clone is not required; it is held behind an `Arc`.
pub struct MultiRaftManager {
    node_id: u64,
    /// This node's Raft advertise address, used when single-node-initializing a
    /// freshly created data group.
    advertise_addr: String,
    config: Arc<openraft::Config>,
    state: RaftState,
    /// The node's local KV store; data groups receive namespaced views of it.
    base_kv: Arc<dyn KvEngine>,
    /// Synchronous mirror of the group ids in `state`, so the (synchronous)
    /// KV write/read path can check group membership without awaiting the
    /// async `RaftState` lock. Always updated alongside `state`.
    hosted: Arc<RwLock<HashSet<String>>>,
}

impl MultiRaftManager {
    pub fn new(
        node_id: u64,
        advertise_addr: String,
        config: Arc<openraft::Config>,
        state: RaftState,
        base_kv: Arc<dyn KvEngine>,
    ) -> Self {
        Self {
            node_id,
            advertise_addr,
            config,
            state,
            base_kv,
            hosted: Arc::new(RwLock::new(HashSet::new())),
        }
    }

    /// Returns an existing group's handle, if this node hosts it.
    pub async fn get(&self, shard_id: &str) -> Option<NodusRaft> {
        self.state.rafts.read().await.get(shard_id).cloned()
    }

    /// Synchronously reports whether this node hosts a group for `shard_id`.
    /// Used by the KV write/read path to decide routing without awaiting.
    pub fn hosts(&self, shard_id: &str) -> bool {
        self.hosted.read().unwrap().contains(shard_id)
    }

    /// Health of the data-shard groups hosted here: `(total, without_leader)`.
    /// A group with no current leader is unavailable for reads/writes. The meta
    /// group is excluded. Feeds the cluster-overview shard health.
    pub async fn shard_health(&self) -> (u32, u32) {
        let rafts = self.state.rafts.read().await;
        let mut total = 0;
        let mut without_leader = 0;
        for (id, raft) in rafts.iter() {
            if id == META_SHARD {
                continue;
            }
            total += 1;
            if raft.metrics().borrow().current_leader.is_none() {
                without_leader += 1;
            }
        }
        (total, without_leader)
    }

    /// Creates the metadata group with full catalog/RBAC/upgrade components.
    /// Membership initialization (single-node bootstrap or join) is driven by
    /// the caller, as today.
    pub async fn create_meta(
        &self,
        kv: Arc<dyn KvEngine>,
        catalog_writer: Arc<dyn nodus_catalog::CatalogWriter>,
        catalog_reader: Arc<dyn nodus_catalog::CatalogReader>,
        upgrade: Arc<dyn nodus_upgrade::UpgradeCoordinator>,
    ) -> Result<NodusRaft> {
        let store = NodusRaftStore::with_components(kv, catalog_writer, catalog_reader, upgrade);
        self.spawn_group(META_SHARD, store).await
    }

    /// Returns the data group for `shard_id`, creating it (backed by a
    /// namespaced view of the node's store) if absent. The new group is inert
    /// until its membership is initialized by the caller.
    pub async fn get_or_create_data(&self, shard_id: &str) -> Result<NodusRaft> {
        if let Some(existing) = self.get(shard_id).await {
            return Ok(existing);
        }
        let kv: Arc<dyn KvEngine> =
            Arc::new(NamespacedKvEngine::new(self.base_kv.clone(), shard_id));
        let store = NodusRaftStore::with_kv(kv);
        self.spawn_group(shard_id, store).await
    }

    /// Builds a Raft instance for `store`, registers it under `shard_id`, and
    /// returns the handle. The network factory carries `shard_id` so RPCs are
    /// routed to `/raft/{shard_id}/...` on peers.
    async fn spawn_group(&self, shard_id: &str, store: NodusRaftStore) -> Result<NodusRaft> {
        let (log_store, state_machine) = openraft::storage::Adaptor::new(store);
        let network = NodusNetworkFactory::new(shard_id.to_string());
        let raft = NodusRaft::new(
            self.node_id,
            self.config.clone(),
            network,
            log_store,
            state_machine,
        )
        .await
        .map_err(|e| anyhow::anyhow!("raft init for shard '{shard_id}': {e}"))?;
        self.state
            .rafts
            .write()
            .await
            .insert(shard_id.to_string(), raft.clone());
        self.hosted.write().unwrap().insert(shard_id.to_string());
        Ok(raft)
    }

    /// Group id convention for the data shard identified by `shard_id`. Also the
    /// KV namespace prefix, so the routing read/write path and the group's own
    /// state-machine engine agree on where a key lives.
    pub fn data_group_id(shard_id: ShardId) -> String {
        format!("shard-{shard_id}")
    }

    /// Brings this node's hosted groups into line with `placements`
    /// (`ShardId -> node id`): every shard placed on this node gets a data group,
    /// created and single-node-initialized if missing. Idempotent — already-hosted
    /// shards are skipped. Returns the number newly created.
    ///
    /// Called at startup (to re-host assigned shards after a restart) and after
    /// placement-changing admin operations. Tear-down of groups no longer placed
    /// here is deferred to data movement (Phase 6); a placement string matches a
    /// node by its `cluster.node_id` rendered as text.
    pub async fn reconcile(&self, placements: &HashMap<ShardId, String>) -> Result<usize> {
        let mine = self.node_id.to_string();
        let mut created = 0;
        for (shard_id, node) in placements {
            if *node != mine {
                continue;
            }
            let group_id = Self::data_group_id(*shard_id);
            if self.hosts(&group_id) {
                continue;
            }
            let raft = self.get_or_create_data(&group_id).await?;
            self.init_single(&raft).await;
            created += 1;
        }
        Ok(created)
    }

    /// Single-node bootstrap so a freshly created group can immediately accept
    /// writes, mirroring how the meta group is initialized. Multi-replica shard
    /// membership is future work.
    async fn init_single(&self, raft: &NodusRaft) {
        let mut members = BTreeMap::new();
        members.insert(self.node_id, openraft::BasicNode::new(&self.advertise_addr));
        let _ = raft.initialize(members).await;
    }

    /// Decommissions the group for `group_id`: shuts down its Raft instance and
    /// drops it from the group map / hosted set, so routing no longer reaches it.
    /// The namespace's bytes become unreachable; physical reclamation is left to
    /// GC/compaction.
    pub async fn remove_group(&self, group_id: &str) -> Result<()> {
        let raft = self.state.rafts.write().await.remove(group_id);
        self.hosted.write().unwrap().remove(group_id);
        if let Some(raft) = raft {
            let _ = raft.shutdown().await;
        }
        Ok(())
    }

    /// Physically splits a shard. Plans the split, creates and initializes the
    /// child groups, **relocates the source's data into them before flipping
    /// routing** (so no read lands on an empty child and no committed key is lost
    /// or duplicated), commits the flip, then decommissions the source group.
    /// Single-node today; cross-node transfer (learner + snapshot ship) is future.
    pub async fn split_shard(
        &self,
        orchestrator: &ShardOrchestrator,
        table_id: TableId,
        shard_id: ShardId,
        split_key: Vec<u8>,
    ) -> Result<(ShardId, ShardId)> {
        let plan = orchestrator.plan_split(table_id, shard_id, split_key)?;
        let (left_id, right_id) = (plan.left.id, plan.right.id);
        let local = plan.source_node.as_deref() == Some(self.node_id.to_string().as_str());

        if local {
            let left_group = Self::data_group_id(left_id);
            let right_group = Self::data_group_id(right_id);
            let lr = self.get_or_create_data(&left_group).await?;
            self.init_single(&lr).await;
            let rr = self.get_or_create_data(&right_group).await?;
            self.init_single(&rr).await;
            self.relocate_partition(
                &Self::data_group_id(shard_id),
                &left_group,
                &right_group,
                &plan.split_key,
            )?;
        }

        orchestrator.commit_split(&plan)?; // atomic routing flip

        if local {
            self.remove_group(&Self::data_group_id(shard_id)).await?;
        }
        Ok((left_id, right_id))
    }

    /// Copies every committed entry from the source namespace into the left or
    /// right child namespace depending on whether its (namespace-stripped) key
    /// sorts before `split_key`, preserving each entry's commit version. Writes
    /// go straight to the shared base store under the child namespaces, which on
    /// a single node is equivalent to the child group applying them.
    fn relocate_partition(
        &self,
        source_group: &str,
        left_group: &str,
        right_group: &str,
        split_key: &[u8],
    ) -> Result<()> {
        let source = NamespacedKvEngine::new(self.base_kv.clone(), source_group);
        let left = NamespacedKvEngine::new(self.base_kv.clone(), left_group);
        let right = NamespacedKvEngine::new(self.base_kv.clone(), right_group);

        let full = KeyRange {
            start: Bytes::new(),
            end: Bytes::from(vec![255u8; 1024]),
        };
        let entries: Vec<(Bytes, Bytes, u64)> = source
            .scan(full, u64::MAX)?
            .filter_map(|r| r.ok())
            .map(|p| (p.key, p.value, p.version))
            .collect();

        for (key, value, version) in entries {
            let dest = if key.as_ref() < split_key {
                &left
            } else {
                &right
            };
            let txn = TxnId::new();
            dest.write_intent(txn, key, value)?;
            dest.commit(txn, version)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use bytes::Bytes;
    use nodus_storage_api::{KeyRange, TxnId};
    use std::collections::BTreeMap;

    fn manager() -> MultiRaftManager {
        let config = Arc::new(openraft::Config::default().validate().unwrap());
        let base_kv: Arc<dyn KvEngine> = Arc::new(nodus_storage_mem::MemKvEngine::new());
        MultiRaftManager::new(1, "127.0.0.1:0".into(), config, RaftState::new(), base_kv)
    }

    async fn elect_single_node(raft: &NodusRaft) -> bool {
        let mut members = BTreeMap::new();
        members.insert(1u64, openraft::BasicNode::new("127.0.0.1:0"));
        let _ = raft.initialize(members).await;
        for _ in 0..30 {
            if raft.metrics().borrow().current_leader == Some(1) {
                return true;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        false
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn meta_and_data_groups_are_independent_and_idempotent() {
        let mgr = manager();
        let catalog = Arc::new(nodus_catalog::MemoryCatalog::new());
        let upgrade = Arc::new(nodus_upgrade::DefaultUpgradeCoordinator::new(
            1,
            vec!["new_storage_format".into()],
            1,
        ));

        let meta = mgr
            .create_meta(
                Arc::new(nodus_storage_mem::MemKvEngine::new()),
                catalog.clone(),
                catalog.clone(),
                upgrade,
            )
            .await
            .unwrap();
        let data = mgr.get_or_create_data("shard-1").await.unwrap();

        // Both groups are registered and reachable.
        assert!(mgr.get(META_SHARD).await.is_some());
        assert!(mgr.get("shard-1").await.is_some());

        // get_or_create_data is idempotent — the same handle, no duplicate group.
        let data_again = mgr.get_or_create_data("shard-1").await.unwrap();
        assert_eq!(data.metrics().borrow().id, data_again.metrics().borrow().id);

        // Each group elects a leader independently of the other.
        assert!(elect_single_node(&meta).await, "meta group should elect");
        assert!(elect_single_node(&data).await, "data group should elect");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn reconcile_hosts_shards_placed_on_this_node() {
        let mgr = manager(); // node_id = 1
        let mine = ShardId::new();
        let other = ShardId::new();
        let mut placements = HashMap::new();
        placements.insert(mine, "1".to_string()); // placed on this node
        placements.insert(other, "2".to_string()); // placed elsewhere

        let created = mgr.reconcile(&placements).await.unwrap();
        assert_eq!(created, 1, "only the shard placed here is created");
        assert!(mgr.hosts(&MultiRaftManager::data_group_id(mine)));
        assert!(!mgr.hosts(&MultiRaftManager::data_group_id(other)));

        // Idempotent: a second reconcile creates nothing new.
        assert_eq!(mgr.reconcile(&placements).await.unwrap(), 0);

        // The hosted group is led and ready to accept writes.
        let raft = mgr
            .get(&MultiRaftManager::data_group_id(mine))
            .await
            .unwrap();
        let mut led = false;
        for _ in 0..30 {
            if raft.metrics().borrow().current_leader == Some(1) {
                led = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert!(led, "reconciled shard group should elect a leader");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn shard_health_reports_groups_without_a_leader() {
        let mgr = manager(); // node_id = 1
        // A led group and an uninitialized (leaderless) group.
        let led = mgr.get_or_create_data("shard-led").await.unwrap();
        mgr.init_single(&led).await;
        for _ in 0..30 {
            if led.metrics().borrow().current_leader == Some(1) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        let _idle = mgr.get_or_create_data("shard-idle").await.unwrap();

        let (total, without_leader) = mgr.shard_health().await;
        assert_eq!(total, 2, "both data groups counted");
        assert_eq!(
            without_leader, 1,
            "only the uninitialized group lacks a leader"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn split_relocates_data_into_children_and_decommissions_the_source() {
        let base: Arc<dyn KvEngine> = Arc::new(nodus_storage_mem::MemKvEngine::new());
        let config = Arc::new(openraft::Config::default().validate().unwrap());
        let mgr = MultiRaftManager::new(
            1,
            "127.0.0.1:0".into(),
            config,
            RaftState::new(),
            base.clone(),
        );

        let meta = Arc::new(nodus_meta::PersistentMetaStore::new(base.clone()));
        let orch = nodus_sharding::ShardOrchestrator::new(meta);
        let table = TableId(uuid::Uuid::new_v4());
        let source = orch.init_single_shard(table).unwrap();
        orch.move_shard(source, "1").unwrap();

        // Host the source group and seed it with one key on each side of 'm'.
        mgr.reconcile(&orch.placements()).await.unwrap();
        let src_ns =
            NamespacedKvEngine::new(base.clone(), &MultiRaftManager::data_group_id(source));
        for (k, v) in [(b"a".as_slice(), b"va".as_slice()), (b"z", b"vz")] {
            let t = TxnId::new();
            src_ns
                .write_intent(t, Bytes::copy_from_slice(k), Bytes::copy_from_slice(v))
                .unwrap();
            src_ns.commit(t, 5).unwrap();
        }

        // Split at 'm' (0x6d): "a" → left, "z" → right.
        let (left, right) = mgr
            .split_shard(&orch, table, source, vec![0x6d])
            .await
            .unwrap();

        // Routing flipped: children hosted, source decommissioned.
        assert!(mgr.hosts(&MultiRaftManager::data_group_id(left)));
        assert!(mgr.hosts(&MultiRaftManager::data_group_id(right)));
        assert!(!mgr.hosts(&MultiRaftManager::data_group_id(source)));
        assert_eq!(orch.shard_map(table).unwrap().shards.len(), 2);

        // Each committed key lands in exactly one child, version preserved.
        let lhs = NamespacedKvEngine::new(base.clone(), &MultiRaftManager::data_group_id(left));
        let rhs = NamespacedKvEngine::new(base.clone(), &MultiRaftManager::data_group_id(right));
        assert_eq!(lhs.get(b"a", 100).unwrap(), Some(Bytes::from_static(b"va")));
        assert_eq!(lhs.get(b"z", 100).unwrap(), None);
        assert_eq!(rhs.get(b"z", 100).unwrap(), Some(Bytes::from_static(b"vz")));
        assert_eq!(rhs.get(b"a", 100).unwrap(), None);
    }

    #[test]
    fn placements_and_shard_maps_survive_reopening_a_persistent_store() {
        let kv: Arc<dyn KvEngine> = Arc::new(nodus_storage_mem::MemKvEngine::new());
        let table = nodus_catalog::TableId(uuid::Uuid::new_v4());

        // Write shard map + placement through one store/orchestrator.
        let shard_id = {
            let meta = Arc::new(nodus_meta::PersistentMetaStore::new(kv.clone()));
            let orch = nodus_sharding::ShardOrchestrator::new(meta);
            let sid = orch.init_single_shard(table).unwrap();
            orch.move_shard(sid, "1").unwrap();
            sid
        };

        // Reopen against the same backing KV (a restart on a persistent store):
        // the orchestrator reloads placements, and the shard map is still there.
        let meta = Arc::new(nodus_meta::PersistentMetaStore::new(kv));
        let orch = nodus_sharding::ShardOrchestrator::new(meta);
        assert_eq!(orch.placement(shard_id), Some("1".to_string()));
        assert!(orch.shard_map(table).is_ok());
    }

    #[test]
    fn namespaced_engine_isolates_keys_per_shard() {
        let inner: Arc<dyn KvEngine> = Arc::new(nodus_storage_mem::MemKvEngine::new());
        let a = NamespacedKvEngine::new(inner.clone(), "shard-1");
        let b = NamespacedKvEngine::new(inner.clone(), "shard-10");

        // Same logical key in two namespaces holds independent values.
        let t1 = TxnId::new();
        a.write_intent(t1, Bytes::from_static(b"k"), Bytes::from_static(b"va"))
            .unwrap();
        a.commit(t1, 10).unwrap();
        let t2 = TxnId::new();
        b.write_intent(t2, Bytes::from_static(b"k"), Bytes::from_static(b"vb"))
            .unwrap();
        b.commit(t2, 10).unwrap();

        assert_eq!(a.get(b"k", 100).unwrap(), Some(Bytes::from_static(b"va")));
        assert_eq!(b.get(b"k", 100).unwrap(), Some(Bytes::from_static(b"vb")));

        // A full scan of one namespace returns only its own keys, prefix stripped.
        let full = KeyRange {
            start: Bytes::new(),
            end: Bytes::from(vec![255u8; 16]),
        };
        let a_pairs: Vec<(Bytes, Bytes)> = a
            .scan(full, 100)
            .unwrap()
            .filter_map(|r| r.ok())
            .map(|p| (p.key, p.value))
            .collect();
        assert_eq!(
            a_pairs,
            vec![(Bytes::from_static(b"k"), Bytes::from_static(b"va"))]
        );
    }
}
