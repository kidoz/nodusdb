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
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::sync::{Arc, RwLock};

use nodus_catalog::{ShardId, TableId};
use nodus_raftstore::NodusRaftStore;
use nodus_raftstore::ShardCommand;
use nodus_raftstore::network::NodusNetworkFactory;
use nodus_raftstore::server::{NodusRaft, RaftState};
use nodus_sharding::ShardOrchestrator;
use nodus_storage_api::{
    IntentReplacement, KeyRange, KvEngine, KvPair, KvResult, KvVersion, NamespacedKvEngine,
    Timestamp, TxnId,
};

/// Wraps a group's KV engine so that applying a committed transaction also
/// advances (and durably reserves) this node's transaction clock past the
/// commit timestamp. This keeps commit timestamps monotonic across a leadership
/// change: a follower that applied `commit_ts` will never, once promoted, issue
/// a commit at or below it — even after a restart. All other operations
/// delegate unchanged.
struct ClockAdvancingKvEngine {
    inner: Arc<dyn KvEngine>,
    clock: Arc<dyn nodus_txn::TxnManager>,
}

impl ClockAdvancingKvEngine {
    fn wrap(inner: Arc<dyn KvEngine>, clock: Arc<dyn nodus_txn::TxnManager>) -> Arc<dyn KvEngine> {
        Arc::new(Self { inner, clock })
    }
}

impl KvEngine for ClockAdvancingKvEngine {
    fn get(&self, key: &[u8], read_ts: Timestamp) -> Result<Option<Bytes>> {
        self.inner.get(key, read_ts)
    }
    fn scan(
        &self,
        range: KeyRange,
        read_ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = Result<KvPair>> + Send>> {
        self.inner.scan(range, read_ts)
    }
    fn scan_versions(
        &self,
        range: KeyRange,
        since_ts: Timestamp,
        read_ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = Result<KvVersion>> + Send>> {
        self.inner.scan_versions(range, since_ts, read_ts)
    }
    fn write_intent(&self, txn_id: TxnId, key: Bytes, value: Bytes) -> KvResult<()> {
        self.inner.write_intent(txn_id, key, value)
    }
    fn delete_intent(&self, txn_id: TxnId, key: Bytes) -> KvResult<()> {
        self.inner.delete_intent(txn_id, key)
    }
    fn replace_intent(
        &self,
        txn_id: TxnId,
        key: Bytes,
        replacement: IntentReplacement,
    ) -> KvResult<()> {
        self.inner.replace_intent(txn_id, key, replacement)
    }
    fn commit(&self, txn_id: TxnId, commit_ts: Timestamp) -> KvResult<()> {
        self.inner.commit(txn_id, commit_ts)?;
        // The data commit is already durable, so a watermark-persist failure must
        // not fail it — but it is a real durability hazard (a restart/leadership
        // change could then issue a commit_ts at or below this applied version),
        // so surface it at ERROR rather than swallowing it. The next issued
        // timestamp re-reserves on this node, recovering the watermark.
        if let Err(e) = self.clock.observe_durable(commit_ts) {
            tracing::error!(
                "clock watermark persist failed for applied commit {commit_ts}: {e}; \
                 commit_ts could regress after a restart until the next reservation"
            );
        }
        Ok(())
    }
    fn abort(&self, txn_id: TxnId) -> KvResult<()> {
        self.inner.abort(txn_id)
    }
    fn garbage_collect(&self, watermark: Timestamp) -> Result<usize> {
        self.inner.garbage_collect(watermark)
    }
    fn flush(&self) -> Result<()> {
        self.inner.flush()
    }
}

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
    /// Bearer token presented when asking a peer to instantiate a replica of a
    /// data group (the `/api/v1/shards/{group}/replica` admin endpoint).
    admin_token: Option<String>,
    /// Reused HTTP client for peer replica-instantiation requests.
    http: reqwest::Client,
    /// This node's transaction clock; advanced as groups apply committed writes
    /// so timestamps stay monotonic across leadership changes.
    clock: Arc<dyn nodus_txn::TxnManager>,
    /// Data directory; each group's snapshots stream to/from durable files under
    /// `{data_dir}/snapshots/{shard_id}`. `None` (no data dir) falls back to a
    /// temp directory, matching the ephemeral in-memory store.
    data_dir: Option<std::path::PathBuf>,
}

impl MultiRaftManager {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: u64,
        advertise_addr: String,
        config: Arc<openraft::Config>,
        state: RaftState,
        base_kv: Arc<dyn KvEngine>,
        admin_token: Option<String>,
        clock: Arc<dyn nodus_txn::TxnManager>,
        data_dir: Option<std::path::PathBuf>,
    ) -> Self {
        Self {
            node_id,
            advertise_addr,
            config,
            state,
            base_kv,
            hosted: Arc::new(RwLock::new(HashSet::new())),
            admin_token,
            http: reqwest::Client::new(),
            clock,
            data_dir,
        }
    }

    /// Durable directory for `shard_id`'s snapshots, or a unique temp dir when no
    /// data dir is configured (ephemeral / in-memory mode).
    fn group_snapshot_dir(&self, shard_id: &str) -> std::path::PathBuf {
        match &self.data_dir {
            Some(dir) => dir.join("snapshots").join(shard_id),
            None => {
                std::env::temp_dir().join(format!("nodus-snap-{shard_id}-{}", uuid::Uuid::new_v4()))
            }
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
        meta_store: Arc<dyn nodus_meta::MetaStore>,
    ) -> Result<NodusRaft> {
        let kv = ClockAdvancingKvEngine::wrap(kv, self.clock.clone());
        let store = NodusRaftStore::with_components(
            kv,
            catalog_writer,
            catalog_reader,
            upgrade,
            meta_store,
            self.group_snapshot_dir(META_SHARD),
        );
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
        let kv = ClockAdvancingKvEngine::wrap(kv, self.clock.clone());
        let store = NodusRaftStore::with_kv_at(kv, self.group_snapshot_dir(shard_id));
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
    /// (`ShardId -> node id`): every shard placed on this node is formed as a
    /// data group replicated across all current cluster members, with this node
    /// as the primary (see [`Self::form_replicated`]). Idempotent and
    /// convergent — re-running folds in nodes that joined after a group formed
    /// and re-instantiates replicas on restarted peers. Returns the number of
    /// groups newly created on this node.
    ///
    /// Called at startup (to re-host assigned shards after a restart), after
    /// placement-changing admin operations, and periodically by the reconcile
    /// loop. Tear-down of groups no longer placed here is deferred to data
    /// movement (Phase 6); a placement string matches a node by its
    /// `cluster.node_id` rendered as text.
    pub async fn reconcile(&self, placements: &HashMap<ShardId, String>) -> Result<usize> {
        let mine = self.node_id.to_string();
        let mut created = 0;
        let mut owned: HashSet<String> = HashSet::new();
        for (shard_id, node) in placements {
            if *node != mine {
                continue;
            }
            let group_id = Self::data_group_id(*shard_id);
            owned.insert(group_id.clone());
            let newly = !self.hosts(&group_id);
            if let Err(e) = self.form_replicated(&group_id).await {
                tracing::warn!("forming data group {group_id} failed: {e}");
                continue;
            }
            if newly {
                created += 1;
            }
        }

        // Leadership-aware pass: for any data group this node currently *leads*
        // but does not own (it failed over here from its original primary), keep
        // its replica set in sync with the cluster. Without this, a group whose
        // owner is gone would never fold in a node that joined or restarted
        // afterwards. A no-op in steady state, where a node only leads groups it
        // owns.
        let led: Vec<(String, NodusRaft)> = {
            let rafts = self.state.rafts.read().await;
            rafts
                .iter()
                .filter(|(id, _)| id.as_str() != META_SHARD && !owned.contains(id.as_str()))
                .filter(|(_, raft)| raft.metrics().borrow().current_leader == Some(self.node_id))
                .map(|(id, raft)| (id.clone(), raft.clone()))
                .collect()
        };
        for (group_id, raft) in led {
            if let Err(e) = self.ensure_group_membership(&group_id, &raft).await {
                tracing::warn!("ensuring membership for led group {group_id} failed: {e}");
            }
        }
        Ok(created)
    }

    /// Single-node bootstrap so a freshly created group can immediately accept
    /// writes and lead, mirroring how the meta group is initialized. Cross-node
    /// voters are then folded in by [`Self::form_replicated`].
    async fn init_single(&self, raft: &NodusRaft) {
        let mut members = BTreeMap::new();
        members.insert(self.node_id, openraft::BasicNode::new(&self.advertise_addr));
        let _ = raft.initialize(members).await;
    }

    /// The current cluster membership (`node id -> raft advertise/HTTP address`),
    /// read from the meta group's Raft membership — the authoritative roster of
    /// nodes and where to reach them. Empty if the meta group isn't hosted here.
    pub async fn cluster_members(&self) -> BTreeMap<u64, String> {
        let Some(meta) = self.get(META_SHARD).await else {
            return BTreeMap::new();
        };
        let metrics = meta.metrics().borrow().clone();
        metrics
            .membership_config
            .membership()
            .nodes()
            .map(|(id, node)| (*id, node.addr.clone()))
            .collect()
    }

    /// Polls until this node is the leader of `raft` (so membership changes are
    /// accepted) or a short deadline passes. Returns whether leadership settled.
    async fn await_leadership(&self, raft: &NodusRaft) -> bool {
        for _ in 0..30 {
            if raft.metrics().borrow().current_leader == Some(self.node_id) {
                return true;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        false
    }

    /// Asks the peer at `addr` to instantiate a local replica of `group_id` so it
    /// can receive the group's log. Idempotent on the peer side (re-instantiating
    /// reloads durable Raft state). The peer's HTTP address doubles as its Raft
    /// advertise address.
    async fn instantiate_remote(&self, addr: &str, group_id: &str) -> Result<()> {
        let url = format!("http://{addr}/api/v1/shards/{group_id}/replica");
        let mut req = self.http.post(&url);
        if let Some(token) = &self.admin_token {
            req = req.bearer_auth(token);
        }
        let resp = req
            .send()
            .await
            .map_err(|e| anyhow::anyhow!("replica request to {addr}: {e}"))?;
        if resp.status().is_success() {
            Ok(())
        } else {
            Err(anyhow::anyhow!(
                "replica instantiate on {addr} returned {}",
                resp.status()
            ))
        }
    }

    /// Forms `group_id` as a Raft group replicated across all current cluster
    /// members, with this node as the primary. Bootstraps the group single-node
    /// (so this node leads), then — for every other member — instantiates a
    /// replica there and folds it in as a voter. Idempotent and convergent:
    /// re-running re-instantiates restarted replicas and adds members that
    /// joined the cluster after the group first formed.
    ///
    /// Membership changes require leadership, so if this node isn't (yet) the
    /// group's leader the call returns without changing membership and a later
    /// reconcile pass retries.
    pub async fn form_replicated(&self, group_id: &str) -> Result<()> {
        let raft = self.get_or_create_data(group_id).await?;

        // Bootstrap as a single-node leader the first time we host the group; a
        // group that already carries voters (this node restarted, or a prior
        // pass formed it) is left as-is.
        let initialized = raft
            .metrics()
            .borrow()
            .membership_config
            .membership()
            .voter_ids()
            .next()
            .is_some();
        if !initialized {
            self.init_single(&raft).await;
        }

        // Only the leader may drive membership; back off if leadership hasn't
        // settled and let a later pass converge.
        if !self.await_leadership(&raft).await {
            return Ok(());
        }
        self.ensure_group_membership(group_id, &raft).await
    }

    /// Leader-only: brings `group_id`'s voter set up to the full cluster
    /// membership — instantiating a replica on every other member and folding it
    /// in. A no-op unless this node currently leads the group, so it is safe to
    /// call on any hosted group. Idempotent and convergent: re-instantiates
    /// restarted replicas and adds members that joined after the group formed.
    /// This is what makes membership self-healing regardless of *which* node
    /// leads the group (see the leadership-aware pass in [`Self::reconcile`]).
    async fn ensure_group_membership(&self, group_id: &str, raft: &NodusRaft) -> Result<()> {
        let metrics = raft.metrics();
        let (is_leader, current): (bool, BTreeSet<u64>) = {
            let m = metrics.borrow();
            (
                m.current_leader == Some(self.node_id),
                m.membership_config.membership().voter_ids().collect(),
            )
        };
        if !is_leader {
            return Ok(());
        }

        let members = self.cluster_members().await;
        let mut target = current.clone();
        for (id, addr) in &members {
            if *id == self.node_id {
                continue;
            }
            // Ensure the peer hosts a (possibly freshly recreated) replica so it
            // can receive the log; idempotent on the peer side.
            if let Err(e) = self.instantiate_remote(addr, group_id).await {
                tracing::warn!("replica of {group_id} on node {id} unavailable: {e}");
                continue;
            }
            if !current.contains(id) {
                let node = openraft::BasicNode::new(addr);
                if let Err(e) = raft.add_learner(*id, node, true).await {
                    tracing::warn!("add_learner {id} for {group_id} failed: {e}");
                    continue;
                }
                target.insert(*id);
            }
        }
        if target != current {
            raft.change_membership(target, true)
                .await
                .map_err(|e| anyhow::anyhow!("change_membership for {group_id}: {e}"))?;
        }
        Ok(())
    }

    /// Shuts down every Raft group on this node and clears the group map. Called
    /// on server shutdown so a stopped node stops participating in consensus
    /// (otherwise it keeps heartbeating to peers and they never re-elect).
    pub async fn shutdown_all(&self) {
        let groups: Vec<NodusRaft> = self
            .state
            .rafts
            .write()
            .await
            .drain()
            .map(|(_, r)| r)
            .collect();
        self.hosted.write().unwrap().clear();
        for raft in groups {
            let _ = raft.shutdown().await;
        }
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

    /// Physically splits a shard. Plans the split, forms the child groups
    /// **replicated across the cluster** (this node as primary), **relocates the
    /// source's data into them through Raft before flipping routing** (so the
    /// data lands on every child replica — not just locally — and no read hits an
    /// empty child or loses a committed key), commits the flip, then
    /// decommissions the source group on this node.
    pub async fn split_shard(
        &self,
        orchestrator: &Arc<ShardOrchestrator>,
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
            // Read the source's committed data before forming the children, then
            // form each child as a replicated group led here.
            let entries = self.scan_user_data(&Self::data_group_id(shard_id))?;
            self.form_replicated(&left_group).await?;
            self.form_replicated(&right_group).await?;
            let left = self
                .get(&left_group)
                .await
                .ok_or_else(|| anyhow::anyhow!("left child group {left_group} missing"))?;
            let right = self
                .get(&right_group)
                .await
                .ok_or_else(|| anyhow::anyhow!("right child group {right_group} missing"))?;

            // Relocate each key into the child whose range it falls in, through
            // that child's Raft so every replica applies it. Commit version is
            // preserved.
            for (key, value, version) in entries {
                let (dest, dest_group) = if key.as_ref() < plan.split_key.as_slice() {
                    (&left, &left_group)
                } else {
                    (&right, &right_group)
                };
                self.raft_relocate(dest, dest_group, key, value, version)
                    .await?;
            }
        }

        // The routing flip replicates through the meta Raft group (blocking
        // submit), so commit off the reactor.
        let orch = orchestrator.clone();
        tokio::task::spawn_blocking(move || orch.commit_split(&plan))
            .await
            .map_err(|e| anyhow::anyhow!("commit_split task panicked: {e}"))??;

        if local {
            self.remove_group(&Self::data_group_id(shard_id)).await?;
        }
        Ok((left_id, right_id))
    }

    /// Physically merges two adjacent shards. Plans the merge, forms the merged
    /// group **replicated across the cluster** (this node as primary),
    /// **relocates both sources' data into it through Raft before flipping
    /// routing** (so the union lands on every merged replica with no key lost),
    /// commits the flip, then decommissions both source groups on this node.
    pub async fn merge_shards(
        &self,
        orchestrator: &Arc<ShardOrchestrator>,
        table_id: TableId,
        left_id: ShardId,
        right_id: ShardId,
    ) -> Result<ShardId> {
        let plan = orchestrator.plan_merge(table_id, left_id, right_id)?;
        let merged_id = plan.merged.id;
        let local = plan.source_node.as_deref() == Some(self.node_id.to_string().as_str());

        if local {
            let merged_group = Self::data_group_id(merged_id);
            // Read both sources' committed data before forming the merged group.
            let mut entries = self.scan_user_data(&Self::data_group_id(left_id))?;
            entries.extend(self.scan_user_data(&Self::data_group_id(right_id))?);
            self.form_replicated(&merged_group).await?;
            let merged = self
                .get(&merged_group)
                .await
                .ok_or_else(|| anyhow::anyhow!("merged group {merged_group} missing"))?;

            // The two sources cover disjoint ranges, so their keys never collide;
            // relocate the union into the merged group through Raft.
            for (key, value, version) in entries {
                self.raft_relocate(&merged, &merged_group, key, value, version)
                    .await?;
            }
        }

        // The routing flip replicates through the meta Raft group (blocking
        // submit), so commit off the reactor.
        let orch = orchestrator.clone();
        tokio::task::spawn_blocking(move || orch.commit_merge(&plan))
            .await
            .map_err(|e| anyhow::anyhow!("commit_merge task panicked: {e}"))??;

        if local {
            self.remove_group(&Self::data_group_id(left_id)).await?;
            self.remove_group(&Self::data_group_id(right_id)).await?;
        }
        Ok(merged_id)
    }

    /// Snapshots a group's committed user data (`key, value, commit version`).
    /// Starts at `0x01` so the group's own `\0`-prefixed Raft state (log/vote/
    /// applied) is never read — that is private to the group and would collide
    /// with a destination group's Raft keys.
    fn scan_user_data(&self, group_id: &str) -> Result<Vec<(Bytes, Bytes, u64)>> {
        let source = NamespacedKvEngine::new(self.base_kv.clone(), group_id);
        let full = KeyRange {
            start: Bytes::from_static(&[1]),
            end: Bytes::from(vec![255u8; 1024]),
        };
        Ok(source
            .scan(full, u64::MAX)?
            .filter_map(|r| r.ok())
            .map(|p| (p.key, p.value, p.version))
            .collect())
    }

    /// Relocates one committed entry into `dest_group` through its Raft, so every
    /// replica of the destination applies it. The commit version is preserved by
    /// committing at the source entry's version. `dest` must be led by this node.
    async fn raft_relocate(
        &self,
        dest: &NodusRaft,
        dest_group: &str,
        key: Bytes,
        value: Bytes,
        version: u64,
    ) -> Result<()> {
        let txn_id = uuid::Uuid::new_v4().to_string();
        let shard_id = Some(dest_group.to_string());
        dest.client_write(ShardCommand::PutIntent {
            txn_id: txn_id.clone(),
            key: key.to_vec(),
            value: value.to_vec(),
            shard_id: shard_id.clone(),
        })
        .await
        .map_err(|e| anyhow::anyhow!("relocate put into {dest_group}: {e}"))?;
        dest.client_write(ShardCommand::CommitTxn {
            txn_id,
            commit_ts: version,
            shard_id,
        })
        .await
        .map_err(|e| anyhow::anyhow!("relocate commit into {dest_group}: {e}"))?;
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
        MultiRaftManager::new(
            1,
            "127.0.0.1:0".into(),
            config,
            RaftState::new(),
            base_kv,
            None,
            Arc::new(nodus_txn::MemTxnManager::new()),
            None,
        )
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
                Arc::new(nodus_meta::MemMetaStore::new()),
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
    async fn reconcile_maintains_membership_of_groups_it_leads_but_does_not_own() {
        let mgr = manager(); // node_id = 1
        // Host and lead a data group with NO placement for it — i.e. one this
        // node leads but does not own (as if it failed over here).
        let group = mgr.get_or_create_data("shard-led-unowned").await.unwrap();
        mgr.init_single(&group).await;
        for _ in 0..30 {
            if group.metrics().borrow().current_leader == Some(1) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        // Reconcile owns nothing, but the leadership-aware pass must still visit
        // the led group (without error) and leave it hosted. With no meta group
        // there are no cluster members to fold in, so membership is unchanged.
        let created = mgr.reconcile(&HashMap::new()).await.unwrap();
        assert_eq!(created, 0, "nothing owned is newly created");
        assert!(
            mgr.hosts("shard-led-unowned"),
            "the led-but-unowned group stays hosted across reconcile"
        );
        let voters = group
            .metrics()
            .borrow()
            .membership_config
            .membership()
            .voter_ids()
            .count();
        assert_eq!(
            voters, 1,
            "no cluster members to add, so still single-voter"
        );
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
            None,
            Arc::new(nodus_txn::MemTxnManager::new()),
            None,
        );

        let meta = Arc::new(nodus_meta::PersistentMetaStore::new(base.clone()));
        let orch = Arc::new(nodus_sharding::ShardOrchestrator::new(meta));
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

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn merge_relocates_both_sources_into_the_merged_group_and_decommissions_them() {
        let base: Arc<dyn KvEngine> = Arc::new(nodus_storage_mem::MemKvEngine::new());
        let config = Arc::new(openraft::Config::default().validate().unwrap());
        let mgr = MultiRaftManager::new(
            1,
            "127.0.0.1:0".into(),
            config,
            RaftState::new(),
            base.clone(),
            None,
            Arc::new(nodus_txn::MemTxnManager::new()),
            None,
        );

        let meta = Arc::new(nodus_meta::PersistentMetaStore::new(base.clone()));
        let orch = Arc::new(nodus_sharding::ShardOrchestrator::new(meta));
        let table = TableId(uuid::Uuid::new_v4());
        let source = orch.init_single_shard(table).unwrap();
        orch.move_shard(source, "1").unwrap();
        mgr.reconcile(&orch.placements()).await.unwrap();

        // Seed one key on each side of 'm', then split so we have two adjacent,
        // populated, hosted children to merge back.
        let src_ns =
            NamespacedKvEngine::new(base.clone(), &MultiRaftManager::data_group_id(source));
        for (k, v) in [(b"a".as_slice(), b"va".as_slice()), (b"z", b"vz")] {
            let t = TxnId::new();
            src_ns
                .write_intent(t, Bytes::copy_from_slice(k), Bytes::copy_from_slice(v))
                .unwrap();
            src_ns.commit(t, 5).unwrap();
        }
        let (left, right) = mgr
            .split_shard(&orch, table, source, vec![0x6d])
            .await
            .unwrap();

        // Merge the two children back into one shard.
        let merged = mgr.merge_shards(&orch, table, left, right).await.unwrap();

        // Routing flipped: merged hosted, both sources decommissioned, one shard.
        assert!(mgr.hosts(&MultiRaftManager::data_group_id(merged)));
        assert!(!mgr.hosts(&MultiRaftManager::data_group_id(left)));
        assert!(!mgr.hosts(&MultiRaftManager::data_group_id(right)));
        let map = orch.shard_map(table).unwrap();
        assert_eq!(map.shards.len(), 1);
        assert_eq!(map.shards[0].id, merged);

        // Both committed keys now live in the merged namespace, versions intact.
        let merged_ns =
            NamespacedKvEngine::new(base.clone(), &MultiRaftManager::data_group_id(merged));
        assert_eq!(
            merged_ns.get(b"a", 100).unwrap(),
            Some(Bytes::from_static(b"va"))
        );
        assert_eq!(
            merged_ns.get(b"z", 100).unwrap(),
            Some(Bytes::from_static(b"vz"))
        );
    }

    #[test]
    fn placements_and_shard_maps_survive_reopening_a_persistent_store() {
        let kv: Arc<dyn KvEngine> = Arc::new(nodus_storage_mem::MemKvEngine::new());
        let table = nodus_catalog::TableId(uuid::Uuid::new_v4());

        // Write shard map + placement through one store/orchestrator.
        let shard_id = {
            let meta = Arc::new(nodus_meta::PersistentMetaStore::new(kv.clone()));
            let orch = Arc::new(nodus_sharding::ShardOrchestrator::new(meta));
            let sid = orch.init_single_shard(table).unwrap();
            orch.move_shard(sid, "1").unwrap();
            sid
        };

        // Reopen against the same backing KV (a restart on a persistent store):
        // the orchestrator reloads placements, and the shard map is still there.
        let meta = Arc::new(nodus_meta::PersistentMetaStore::new(kv));
        let orch = Arc::new(nodus_sharding::ShardOrchestrator::new(meta));
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
