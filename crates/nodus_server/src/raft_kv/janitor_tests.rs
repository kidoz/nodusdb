//! Two persistent nodes over TCP: coordinator liveness is not participant-local.
use super::*;
use nodus_raftstore::server::{RaftRouteAuth, RaftState, raft_routes};
use nodus_storage_api::{PendingIntents, SnapshotRow, SnapshotScope};
use std::time::Duration;

type DecisionWait = (
    tokio::sync::oneshot::Sender<()>,
    std::sync::mpsc::Receiver<()>,
);

#[derive(Default)]
struct DecisionGate(Mutex<Option<DecisionWait>>);

impl DecisionGate {
    fn arm(
        &self,
    ) -> (
        tokio::sync::oneshot::Receiver<()>,
        std::sync::mpsc::Sender<()>,
    ) {
        let (entered_tx, entered_rx) = tokio::sync::oneshot::channel();
        let (release_tx, release_rx) = std::sync::mpsc::channel();
        *self.0.lock().unwrap() = Some((entered_tx, release_rx));
        (entered_rx, release_tx)
    }

    fn wait_if_armed(&self, key: &[u8]) -> Result<()> {
        if key.starts_with(TXN2PC_PREFIX) {
            let pause = self.0.lock().unwrap().take();
            if let Some((entered, release)) = pause {
                let _ = entered.send(());
                release
                    .recv_timeout(Duration::from_secs(15))
                    .map_err(|e| anyhow::anyhow!("decision pause was not released: {e}"))?;
            }
        }
        Ok(())
    }
}

struct PausedDecisionKv {
    inner: Arc<dyn KvEngine>,
    gate: Arc<DecisionGate>,
}

impl KvEngine for PausedDecisionKv {
    fn get(&self, key: &[u8], ts: Timestamp) -> Result<Option<Bytes>> {
        self.inner.get(key, ts)
    }
    fn scan(
        &self,
        range: KeyRange,
        ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = Result<KvPair>> + Send>> {
        self.inner.scan(range, ts)
    }
    fn write_intent(&self, txn: TxnId, key: Bytes, value: Bytes) -> KvResult<()> {
        // The production commit path reaches this meta decision write only
        // after every participant has voted YES. No storage lock is held here.
        self.gate.wait_if_armed(&key)?;
        self.inner.write_intent(txn, key, value)
    }
    fn delete_intent(&self, txn: TxnId, key: Bytes) -> KvResult<()> {
        self.inner.delete_intent(txn, key)
    }
    fn replace_intent(&self, txn: TxnId, key: Bytes, value: IntentReplacement) -> KvResult<()> {
        self.inner.replace_intent(txn, key, value)
    }
    fn commit(&self, txn: TxnId, ts: Timestamp) -> KvResult<()> {
        self.inner.commit(txn, ts)
    }
    fn abort(&self, txn: TxnId) -> KvResult<()> {
        self.inner.abort(txn)
    }
    fn pending_intent_keys(&self, txn: TxnId) -> Vec<Bytes> {
        self.inner.pending_intent_keys(txn)
    }
    fn pending_intents(&self, prefix: &[u8]) -> Result<Vec<PendingIntents>> {
        self.inner.pending_intents(prefix)
    }
    fn has_pending_intents(&self, prefix: &[u8]) -> Result<bool> {
        self.inner.has_pending_intents(prefix)
    }
    fn snapshot_rows(&self, scope: &SnapshotScope) -> Result<Vec<SnapshotRow>> {
        self.inner.snapshot_rows(scope)
    }
    fn replace_snapshot(
        &self,
        scope: &SnapshotScope,
        rows: Vec<SnapshotRow>,
        pointers: Vec<SnapshotRow>,
    ) -> Result<()> {
        self.inner.replace_snapshot(scope, rows, pointers)
    }
}

struct Cluster {
    engines: Vec<Arc<RaftKvEngine>>,
    groups: [String; 2],
    tables: [TableId; 2],
    gate: Arc<DecisionGate>,
    servers: Vec<tokio::task::JoinHandle<()>>,
    _dir: tempfile::TempDir,
}

impl Cluster {
    async fn new() -> Self {
        let dir = tempfile::tempdir().unwrap();
        let mapping = Arc::new(nodus_meta::MemMetaStore::new());
        let orchestrator = nodus_sharding::ShardOrchestrator::new(mapping.clone());
        let tables = [TableId::new(), TableId::new()];
        let groups = tables.map(|table| {
            MultiRaftManager::data_group_id(orchestrator.init_single_shard(table).unwrap())
        });
        let gate = Arc::new(DecisionGate::default());
        let mut engines = Vec::new();
        let mut servers = Vec::new();
        let mut addresses = Vec::new();
        let config = Arc::new(
            openraft::Config {
                heartbeat_interval: 50,
                election_timeout_min: 150,
                election_timeout_max: 300,
                snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(1_000_000),
                ..Default::default()
            }
            .validate()
            .unwrap(),
        );
        for id in 1..=2 {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let address = listener.local_addr().unwrap().to_string();
            addresses.push(address.clone());
            let node_dir = dir.path().join(format!("node-{id}"));
            let raw: Arc<dyn KvEngine> = Arc::new(
                nodus_storage_lsm::LsmKvEngine::with_wal(node_dir.join("kv"), None).unwrap(),
            );
            let local: Arc<dyn KvEngine> = if id == 1 {
                Arc::new(PausedDecisionKv {
                    inner: raw,
                    gate: gate.clone(),
                })
            } else {
                raw
            };
            let state = RaftState::new();
            let manager = Arc::new(MultiRaftManager::new(
                id,
                address,
                config.clone(),
                state.clone(),
                local.clone(),
                Some(node_dir.to_string_lossy().into_owned()),
                Arc::new(nodus_txn::MemTxnManager::new()),
                None,
                Default::default(),
            ));
            let catalog = Arc::new(nodus_catalog::MemoryCatalog::new());
            manager
                .create_meta(
                    local.clone(),
                    catalog.clone(),
                    catalog,
                    Arc::new(nodus_meta::MemMetaStore::new()),
                )
                .await
                .unwrap();
            for group in &groups {
                manager.get_or_create_data(group).await.unwrap();
            }
            let engine = Arc::new(RaftKvEngine {
                local,
                router: RaftRouter::spawn(manager.clone()),
                shard_router: Arc::new(nodus_sharding::CatalogShardRouter::new(mapping.clone())),
                manager,
                txn_groups: Mutex::new(HashMap::new()),
                metrics: nodus_monitoring::Metrics::default(),
                recovery_gate: Default::default(),
            });
            engines.push(engine);
            servers.push(tokio::spawn(async move {
                axum::serve(
                    listener,
                    raft_routes(RaftRouteAuth::AdminToken {
                        token: None,
                        allow_insecure: true,
                    })
                    .with_state(state),
                )
                .await
                .unwrap();
            }));
        }
        // One voter and one learner in each real TCP group. Meta and T lead on
        // the coordinator; U leads remotely, where txn_groups knows no owner.
        for (group, leader_id) in [
            (META_SHARD, 1),
            (groups[0].as_str(), 1),
            (groups[1].as_str(), 2),
        ] {
            let leader = engines[leader_id as usize - 1]
                .manager
                .get(group)
                .await
                .unwrap();
            leader
                .initialize(BTreeMap::from([(
                    leader_id,
                    openraft::BasicNode::new(addresses[leader_id as usize - 1].clone()),
                )]))
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(10), async {
                while leader.metrics().borrow().current_leader != Some(leader_id) {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            let follower_id = 3 - leader_id;
            leader
                .add_learner(
                    follower_id,
                    openraft::BasicNode::new(addresses[follower_id as usize - 1].clone()),
                    true,
                )
                .await
                .unwrap();
            let follower = engines[follower_id as usize - 1]
                .manager
                .get(group)
                .await
                .unwrap();
            tokio::time::timeout(Duration::from_secs(10), async {
                while follower.metrics().borrow().current_leader != Some(leader_id) {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
        }
        Self {
            engines,
            groups,
            tables,
            gate,
            servers,
            _dir: dir,
        }
    }

    fn key(&self, table: usize, suffix: &str) -> Bytes {
        Bytes::from(format!("{}:{suffix}", self.tables[table]))
    }

    async fn shutdown(self) {
        for engine in &self.engines {
            engine.manager.shutdown_all().await;
        }
        for server in self.servers {
            server.abort();
            let _ = server.await;
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn old_remote_transaction_survives_sweep_and_commits_every_write() {
    let c = Cluster::new().await;
    let coordinator = c.engines[0].clone();
    let remote = c.engines[1].clone();
    let txn = TxnId::new();
    let keys = [c.key(1, "a"), c.key(1, "b")];
    let (writer, writes) = (coordinator.clone(), keys.clone());
    tokio::task::spawn_blocking(move || {
        for key in writes {
            writer
                .write_intent(txn, key, Bytes::from_static(b"durable"))
                .unwrap();
        }
    })
    .await
    .unwrap();
    assert!(coordinator.txn_groups.lock().unwrap().contains_key(&txn));
    assert!(remote.txn_groups.lock().unwrap().is_empty());
    let report = remote
        .sweep_abandoned_intents(
            Duration::from_secs(300),
            nodus_storage_api::wallclock_micros() + 600_000_000,
        )
        .await
        .unwrap();
    tokio::task::spawn_blocking(move || coordinator.commit(txn, 100))
        .await
        .unwrap()
        .expect("the remote transaction acknowledged commit");
    let store = remote.engine_for(&c.groups[1]);
    for key in keys {
        assert_eq!(
            store.get(&key, u64::MAX).unwrap(),
            Some(Bytes::from_static(b"durable")),
            "acknowledged remote write was lost"
        );
    }
    assert_eq!(report.aborted, 0);
    assert_eq!(report.uncertain, 1);
    let resolved = remote
        .sweep_abandoned_intents(
            Duration::from_secs(300),
            nodus_storage_api::wallclock_micros() + 600_000_000,
        )
        .await
        .unwrap();
    assert_eq!(resolved.uncertain, 0);
    c.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn remote_sweep_between_prepare_and_decision_cannot_tear_a_commit() {
    let c = Cluster::new().await;
    let coordinator = c.engines[0].clone();
    let remote = c.engines[1].clone();
    let txn = TxnId::new();
    let keys = [c.key(0, "local"), c.key(1, "remote")];
    let (writer, writes) = (coordinator.clone(), keys.clone());
    tokio::task::spawn_blocking(move || {
        for key in writes {
            writer
                .write_intent(txn, key, Bytes::from_static(b"atomic"))
                .unwrap();
        }
    })
    .await
    .unwrap();
    let (entered, release) = c.gate.arm();
    let committing = tokio::task::spawn_blocking(move || coordinator.commit(txn, 100));
    // This is reached by the production 2PC path after both prepare votes and
    // before its meta decision write. The pause has a bounded release timeout.
    tokio::time::timeout(Duration::from_secs(10), entered)
        .await
        .unwrap()
        .unwrap();
    let decision = record_key(&txn.0.to_string());
    let no_decision = c
        .engines
        .iter()
        .all(|e| e.local.get(&decision, u64::MAX).unwrap().is_none());
    let unknown_remotely = remote.txn_groups.lock().unwrap().is_empty();
    let report = remote
        .sweep_abandoned_intents(
            Duration::from_secs(300),
            nodus_storage_api::wallclock_micros() + 600_000_000,
        )
        .await;
    release.send(()).unwrap();
    tokio::time::timeout(Duration::from_secs(15), committing)
        .await
        .unwrap()
        .unwrap()
        .expect("2PC acknowledged commit");
    assert!(
        no_decision && unknown_remotely,
        "the intended uncertain window was not reached"
    );
    for (index, key) in keys.iter().enumerate() {
        let store = c.engines[index].engine_for(&c.groups[index]);
        assert_eq!(
            store.get(key, u64::MAX).unwrap(),
            Some(Bytes::from_static(b"atomic")),
            "2PC acknowledged a partial commit at participant {index}"
        );
    }
    let report = report.unwrap();
    assert_eq!(report.aborted, 0);
    assert_eq!(report.uncertain, 1);
    c.shutdown().await;
}
