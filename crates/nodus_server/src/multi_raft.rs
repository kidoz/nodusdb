//! Manages this node's Raft groups: one `shard-meta` group for catalog/RBAC and
//! cluster metadata, plus zero or more data-shard groups that each replicate a
//! disjoint slice of the key space.
//!
//! Phase 1 establishes the *capability* — a node can host the meta group plus
//! independent per-shard groups, each with an isolated KV namespace (so a
//! group's snapshot only contains its own keys). Routing user writes to data
//! groups (deciding which group owns a key) is Phase 2; placement-driven group
//! lifecycle is Phase 5.

use anyhow::Result;
use std::sync::Arc;

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
    config: Arc<openraft::Config>,
    state: RaftState,
    /// The node's local KV store; data groups receive namespaced views of it.
    /// Consumed by `get_or_create_data` once routing lands in Phase 2.
    #[allow(dead_code)]
    base_kv: Arc<dyn KvEngine>,
}

impl MultiRaftManager {
    pub fn new(
        node_id: u64,
        config: Arc<openraft::Config>,
        state: RaftState,
        base_kv: Arc<dyn KvEngine>,
    ) -> Self {
        Self {
            node_id,
            config,
            state,
            base_kv,
        }
    }

    /// Returns an existing group's handle, if this node hosts it.
    pub async fn get(&self, shard_id: &str) -> Option<NodusRaft> {
        self.state.rafts.read().await.get(shard_id).cloned()
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
    /// until its membership is initialized by the caller. Wired into the write
    /// path by Phase 2 (routing); covered by tests until then.
    #[allow(dead_code)]
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
        Ok(raft)
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
        MultiRaftManager::new(1, config, RaftState::new(), base_kv)
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
