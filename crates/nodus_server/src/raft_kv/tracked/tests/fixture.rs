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
        axum_server::Handle<std::net::SocketAddr>,
        tokio::task::JoinHandle<()>,
    )>,
}

impl Cluster {
    pub async fn open(root: &Path) -> Self {
        std::fs::create_dir_all(root).unwrap();
        let (cert, key, ca) = crate::tests::write_test_certs(root);
        let cluster = nodus_config::ClusterConfig {
            tls: nodus_config::ClusterTlsConfig {
                enabled: true,
                cert_path: Some(cert),
                key_path: Some(key),
                ca_path: Some(ca),
            },
            ..Default::default()
        };
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
                heartbeat_interval: 200,
                election_timeout_min: 2000,
                election_timeout_max: 4000,
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
            let transport = crate::build_raft_transport(&cluster)
                .unwrap()
                .with_snapshot_checks()
                .with_admission_source(local.clone());
            let manager = Arc::new(MultiRaftManager::new(
                index as u64 + 1,
                addresses[index].clone(),
                config.clone(),
                state.clone(),
                local.clone(),
                Some(node_dir.to_string_lossy().into_owned()),
                clock,
                None,
                transport,
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
            let gate_kv = local.clone();
            let router = RaftRouter::spawn(manager.clone()).with_txn_authority_gate(move || {
                nodus_raftstore::upgrade::read(gate_kv.as_ref()).is_ok_and(|r| {
                    r.cluster_version >= nodus_raftstore::upgrade::TXN_AUTHORITY_LEVEL
                })
            });
            engines.push(Arc::new(RaftKvEngine {
                local,
                router,
                shard_router: Arc::new(nodus_sharding::CatalogShardRouter::new(mapping.clone())),
                manager,
                txn_groups: Mutex::new(HashMap::new()),
                metrics: nodus_monitoring::Metrics::default(),
            }));
            let app = raft_routes(RaftRouteAuth::TransportAuthenticated).with_state(state);
            let (tls, _) = crate::load_raft_tls_config(&cluster.tls).unwrap().unwrap();
            let stop = axum_server::Handle::new();
            let shutdown = stop.clone();
            let task = tokio::spawn(async move {
                axum_server::from_tcp_rustls(
                    listener.into_std().unwrap(),
                    axum_server::tls_rustls::RustlsConfig::from_config(tls),
                )
                .unwrap()
                .handle(shutdown)
                .serve(app.into_make_service())
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
        let service = crate::raft_upgrade::RaftUpgradeCoordinator {
            kv: engines[0].local.clone(),
            manager: engines[0].manager.clone(),
            transport: engines[0].manager.transport(),
            membership_lock: Arc::new(tokio::sync::Mutex::new(())),
        };
        for target in [
            nodus_raftstore::upgrade::TARGET,
            nodus_raftstore::upgrade::MIGRATION_TARGET,
            nodus_raftstore::upgrade::WRITE_BATCH_TARGET,
            nodus_raftstore::upgrade::TXN_AUTHORITY_TARGET,
        ] {
            let level = nodus_raftstore::upgrade::read(engines[0].local.as_ref())
                .unwrap()
                .cluster_version;
            let target_level = match target {
                nodus_raftstore::upgrade::TARGET => 2,
                nodus_raftstore::upgrade::MIGRATION_TARGET => 3,
                nodus_raftstore::upgrade::WRITE_BATCH_TARGET => 4,
                _ => 5,
            };
            if level < target_level {
                settle_meta(&engines).await;
                service.start_upgrade(target.into()).await.unwrap();
                settle_meta(&engines).await;
                service.report_node_upgraded("1").await.unwrap();
                settle_meta(&engines).await;
                service.finalize_upgrade().await.unwrap();
            }
        }
        for engine in &engines {
            let e = engine.clone();
            tokio::task::spawn_blocking(move || e.router.read_barrier(META_SHARD))
                .await
                .unwrap()
                .unwrap();
            assert!(engine.router.txn_authority_enabled());
        }
        drop(service);
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
            stop.shutdown();
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

async fn settle_meta(engines: &[Arc<RaftKvEngine>]) {
    for engine in engines {
        let e = engine.clone();
        tokio::task::spawn_blocking(move || e.router.read_barrier(META_SHARD))
            .await
            .unwrap()
            .unwrap();
    }
}
