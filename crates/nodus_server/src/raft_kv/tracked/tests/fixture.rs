use super::*;
use nodus_raftstore::server::{RaftRouteAuth, RaftState, raft_routes};
use std::path::{Path, PathBuf};

pub(super) struct Cluster {
    pub engines: Vec<Arc<RaftKvEngine>>,
    pub groups: [String; 2],
    pub tables: [TableId; 2],
    pub mapping: Arc<nodus_meta::MemMetaStore>,
    pub catalog: Arc<nodus_catalog::MemoryCatalog>,
    pub clock: Arc<nodus_txn::MemTxnManager>,
    servers: Vec<(
        tokio::sync::oneshot::Sender<()>,
        tokio::task::JoinHandle<()>,
    )>,
}

// Loopback-only fixture forwarding. Production /write rejects both dormant
// command families at every existing level. Consensus, snapshots and ReadIndex
// still use the actual TCP handlers; this bypass exists only in this test file.
async fn fixture_write(
    axum::extract::Path(group): axum::extract::Path<String>,
    axum::extract::State(state): axum::extract::State<RaftState>,
    axum::Json(cmd): axum::Json<ShardCommand>,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    let raft = state.rafts.read().await.get(&group).cloned().unwrap();
    match raft.client_write(cmd).await {
        Ok(response) => axum::Json(response.data).into_response(),
        Err(error) => (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            error.to_string(),
        )
            .into_response(),
    }
}

impl Cluster {
    pub async fn open(root: &Path) -> Self {
        std::fs::create_dir_all(root).unwrap();
        let layout = root.join("addresses.json");
        let previous: Option<Vec<String>> = std::fs::read(&layout)
            .ok()
            .map(|b| serde_json::from_slice(&b).unwrap());
        let mut listeners = Vec::new();
        let mut addresses = Vec::new();
        for index in 0..2 {
            let address = previous
                .as_ref()
                .map_or("127.0.0.1:0", |a| a[index].as_str());
            let listener = tokio::net::TcpListener::bind(address).await.unwrap();
            addresses.push(listener.local_addr().unwrap().to_string());
            listeners.push(listener);
        }
        std::fs::write(layout, serde_json::to_vec(&addresses).unwrap()).unwrap();
        let mapping = Arc::new(nodus_meta::MemMetaStore::new());
        let tables = [TableId(Uuid::from_u128(100)), TableId(Uuid::from_u128(200))];
        let shard_ids = [
            nodus_catalog::ShardId(Uuid::from_u128(101)),
            nodus_catalog::ShardId(Uuid::from_u128(201)),
        ];
        // Persisted group identities must stay stable across process exit.
        use nodus_meta::MetaStore;
        for (table, shard) in tables.iter().zip(shard_ids.iter()) {
            mapping
                .update_shard_map(nodus_meta::ShardMap {
                    table_id: *table,
                    shards: vec![nodus_catalog::ShardDescriptor {
                        id: *shard,
                        table_id: *table,
                        name: "test-shard".into(),
                        version: 1,
                        created_at: chrono::Utc::now(),
                        updated_at: chrono::Utc::now(),
                        state: nodus_catalog::DescriptorState::Public,
                        start_key: vec![],
                        end_key: vec![],
                    }],
                })
                .unwrap();
        }
        let groups = shard_ids.map(MultiRaftManager::data_group_id);
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
        let mut engines = Vec::new();
        let mut servers = Vec::new();
        let mut first_catalog = None;
        let mut first_clock = None;
        for (index, listener) in listeners.into_iter().enumerate() {
            let node_dir: PathBuf = root.join(format!("node-{}", index + 1));
            let raw: Arc<dyn KvEngine> = Arc::new(
                nodus_storage_lsm::LsmKvEngine::with_wal(node_dir.join("kv"), None).unwrap(),
            );
            let local = if std::env::var("NODUS_TRACKED_CRASH_PHASE").as_deref() == Ok("marker") {
                Arc::new(super::marker_gap::ExitBeforeMarkerCommit(raw)) as Arc<dyn KvEngine>
            } else {
                raw
            };
            let state = RaftState::new();
            let clock = Arc::new(nodus_txn::MemTxnManager::new());
            if index == 0 {
                first_clock = Some(clock.clone());
            }
            let manager = Arc::new(MultiRaftManager::new(
                index as u64 + 1,
                addresses[index].clone(),
                config.clone(),
                state.clone(),
                local.clone(),
                Some(node_dir.to_string_lossy().into_owned()),
                clock,
                None,
                Default::default(),
            ));
            let catalog = Arc::new(nodus_catalog::MemoryCatalog::new());
            if index == 0 {
                first_catalog = Some(catalog.clone());
            }
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
            let router = RaftRouter::spawn(manager.clone());
            router.enable_txn_authority_for_test();
            engines.push(Arc::new(RaftKvEngine {
                local,
                router,
                shard_router: Arc::new(nodus_sharding::CatalogShardRouter::new(mapping.clone())),
                manager,
                txn_groups: Mutex::new(HashMap::new()),
                metrics: nodus_monitoring::Metrics::default(),
            }));
            let fallback = raft_routes(RaftRouteAuth::AdminToken {
                token: None,
                allow_insecure: true,
            })
            .with_state(state.clone());
            let app = axum::Router::new()
                .route("/raft/{group}/write", axum::routing::post(fixture_write))
                .with_state(state)
                .fallback_service(fallback);
            let (stop, stopped) = tokio::sync::oneshot::channel();
            let task = tokio::spawn(async move {
                axum::serve(listener, app)
                    .with_graceful_shutdown(async {
                        let _ = stopped.await;
                    })
                    .await
                    .unwrap();
            });
            servers.push((stop, task));
        }
        for (group, leader_id) in [(META_SHARD, 1), (&groups[0], 1), (&groups[1], 2)] {
            let leader = engines[leader_id as usize - 1]
                .manager
                .get(group)
                .await
                .unwrap();
            if !leader.is_initialized().await.unwrap() {
                leader
                    .initialize(BTreeMap::from([(
                        leader_id,
                        openraft::BasicNode::new(addresses[leader_id as usize - 1].clone()),
                    )]))
                    .await
                    .unwrap();
            }
            tokio::time::timeout(Duration::from_secs(10), async {
                while leader.metrics().borrow().current_leader != Some(leader_id) {
                    tokio::time::sleep(Duration::from_millis(10)).await;
                }
            })
            .await
            .unwrap();
            let other = 3 - leader_id;
            let has_learner = leader
                .metrics()
                .borrow()
                .membership_config
                .membership()
                .nodes()
                .any(|(id, _)| *id == other);
            if !has_learner {
                leader
                    .add_learner(
                        other,
                        openraft::BasicNode::new(addresses[other as usize - 1].clone()),
                        true,
                    )
                    .await
                    .unwrap();
            }
            let follower = engines[other as usize - 1]
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
            mapping,
            catalog: first_catalog.unwrap(),
            clock: first_clock.unwrap(),
            servers,
        }
    }

    pub fn keys(&self) -> [Bytes; 2] {
        self.tables.map(|t| Bytes::from(format!("{t}:row")))
    }

    pub async fn write_pair(&self, txn: TxnId) {
        let engine = self.engines[0].clone();
        let keys = self.keys();
        tokio::task::spawn_blocking(move || {
            engine.write_intent_batch(
                txn,
                keys.into_iter()
                    .map(|key| nodus_storage_api::IntentOperation::Put {
                        key,
                        value: Bytes::from_static(b"atomic"),
                    })
                    .collect(),
            )
        })
        .await
        .unwrap()
        .unwrap();
    }

    pub fn assert_rows(&self, visible: bool) {
        for (index, key) in self.keys().iter().enumerate() {
            assert_eq!(
                self.engines[index]
                    .engine_for(&self.groups[index])
                    .get(key, u64::MAX)
                    .unwrap(),
                visible.then(|| Bytes::from_static(b"atomic"))
            );
        }
    }

    pub async fn shutdown(self) {
        let storage: Vec<_> = self
            .engines
            .iter()
            .map(|e| Arc::downgrade(&e.local))
            .collect();
        for engine in &self.engines {
            engine.manager.shutdown_all().await;
        }
        for (stop, mut task) in self.servers {
            let _ = stop.send(());
            if tokio::time::timeout(Duration::from_secs(5), &mut task)
                .await
                .is_err()
            {
                task.abort();
                let _ = task.await;
            }
        }
        drop(self.engines);
        tokio::time::timeout(Duration::from_secs(10), async {
            while storage.iter().any(|s| s.strong_count() > 0) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("old Raft storage must quiesce before reopen");
    }
}
