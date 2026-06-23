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
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, RwLock};

use nodus_catalog::ShardId;
use nodus_raftstore::NodusRaftStore;
use nodus_raftstore::network::NodusNetworkFactory;
use nodus_raftstore::server::{NodusRaft, RaftState};
use nodus_storage_api::{KvEngine, NamespacedKvEngine};

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
            // Single-node bootstrap so the group can immediately accept writes,
            // mirroring how the meta group is initialized. Multi-replica shard
            // membership lands with data movement (Phase 6).
            let mut members = BTreeMap::new();
            members.insert(self.node_id, openraft::BasicNode::new(&self.advertise_addr));
            let _ = raft.initialize(members).await;
            created += 1;
        }
        Ok(created)
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
