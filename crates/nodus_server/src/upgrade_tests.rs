//! Real LSM, mTLS/Raft failover and abrupt restart evidence for upgrade authority.
use crate::{
    multi_raft::{META_SHARD, MultiRaftManager},
    raft_upgrade::RaftUpgradeCoordinator,
};
use bytes::Bytes;
use nodus_raftstore::{
    NodusRaftStore,
    server::{NodusRaft, RaftState, raft_routes},
    upgrade::{self, Phase},
};
use nodus_storage_api::{KvEngine, TxnId};
use nodus_storage_lsm::LsmKvEngine;
use openraft::storage::{RaftSnapshotBuilder, RaftStorage};
use std::{collections::BTreeMap, path::Path, sync::Arc, time::Duration};
use tokio::net::TcpListener;

struct Node {
    kv: Arc<LsmKvEngine>,
    raft: NodusRaft,
    service: RaftUpgradeCoordinator,
    server: tokio::task::JoinHandle<()>,
}
impl Node {
    async fn stop(self) {
        self.service.manager.shutdown_all().await;
        self.server.abort();
        let _ = self.server.await;
    }
}
async fn node(
    id: u64,
    dir: &Path,
    listener: TcpListener,
    cluster: &nodus_config::ClusterConfig,
) -> Node {
    let kv = Arc::new(LsmKvEngine::with_wal(dir.join("kv"), None).unwrap());
    let state = RaftState::new();
    let transport = crate::build_raft_transport(cluster)
        .unwrap()
        .with_snapshot_checks();
    let config = Arc::new(
        openraft::Config {
            heartbeat_interval: 200,
            election_timeout_min: 700,
            election_timeout_max: 1100,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );
    let manager = Arc::new(MultiRaftManager::new(
        id,
        listener.local_addr().unwrap().to_string(),
        config,
        state.clone(),
        kv.clone(),
        None,
        Arc::new(nodus_txn::MemTxnManager::new()),
        Some(dir.into()),
        transport.clone(),
    ));
    let catalog = Arc::new(
        nodus_catalog::MemoryCatalog::with_store(Arc::new(nodus_executor::KvCatalogStore::new(
            kv.clone(),
        )))
        .unwrap(),
    );
    let raft = manager
        .create_meta(
            kv.clone(),
            catalog.clone(),
            catalog,
            Arc::new(nodus_meta::PersistentMetaStore::new(kv.clone())),
        )
        .await
        .unwrap();
    let tls = crate::load_raft_tls_config(&cluster.tls).unwrap();
    let server = tokio::spawn(async move {
        let app = raft_routes().with_state(state);
        if let Some((cfg, _)) = tls {
            axum_server::from_tcp_rustls(
                listener.into_std().unwrap(),
                axum_server::tls_rustls::RustlsConfig::from_config(cfg),
            )
            .unwrap()
            .serve(app.into_make_service())
            .await
            .unwrap();
        } else {
            axum::serve(listener, app).await.unwrap();
        }
    });
    let service = RaftUpgradeCoordinator {
        kv: kv.clone(),
        manager,
        transport,
        membership_lock: Arc::new(tokio::sync::Mutex::new(())),
    };
    Node {
        kv,
        raft,
        service,
        server,
    }
}
async fn wait_leader(nodes: &[Node]) -> usize {
    tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            for (i, node) in nodes.iter().enumerate() {
                if node.raft.metrics().borrow().current_leader
                    == Some(node.service.manager.node_id())
                    && node.raft.ensure_linearizable().await.is_ok()
                {
                    return i;
                }
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .unwrap()
}
async fn converge(nodes: &[Node], phase: Phase) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !nodes
            .iter()
            .all(|n| upgrade::read(n.kv.as_ref()).unwrap().phase == phase)
        {
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn authenticated_upgrade_survives_leader_loss_and_requires_every_member() {
    let dir = tempfile::tempdir().unwrap();
    let (cert, key, ca) = crate::tests::write_test_certs(dir.path());
    let config = nodus_config::ClusterConfig {
        tls: nodus_config::ClusterTlsConfig {
            enabled: true,
            cert_path: Some(cert),
            key_path: Some(key),
            ca_path: Some(ca),
        },
        ..Default::default()
    };
    let mut nodes = Vec::new();
    let mut members = BTreeMap::new();
    for id in 1..=3 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        members.insert(
            id,
            openraft::BasicNode::new(listener.local_addr().unwrap().to_string()),
        );
        nodes.push(node(id, &dir.path().join(id.to_string()), listener, &config).await);
    }
    nodes[0].raft.initialize(members.clone()).await.unwrap();
    let leader = wait_leader(&nodes).await;
    let service = &nodes[leader].service;
    assert!(service.start_upgrade("0.2.0".into()).await.is_err());
    service.start_upgrade(upgrade::TARGET.into()).await.unwrap();
    assert!(service.report_node_upgraded("99").await.is_err());
    assert!(service.finalize_upgrade().await.is_err());
    service.report_node_upgraded("1").await.unwrap();
    converge(&nodes, Phase::ReadyToFinalize).await;
    let old = nodes.remove(leader);
    let old_id = old.service.manager.node_id();
    old.stop().await;
    let new_leader = wait_leader(&nodes).await;
    assert_eq!(
        nodes[new_leader].service.get_state().await.unwrap()["phase"],
        "ReadyToFinalize"
    );
    assert!(
        nodes[new_leader].service.finalize_upgrade().await.is_err(),
        "unreachable member must not be omitted"
    );
    let listener = TcpListener::bind(&members[&old_id].addr).await.unwrap();
    nodes.push(
        node(
            old_id,
            &dir.path().join(old_id.to_string()),
            listener,
            &config,
        )
        .await,
    );
    let leader = wait_leader(&nodes).await;
    nodes[leader].service.finalize_upgrade().await.unwrap();
    converge(&nodes, Phase::Finalized).await;
    for node in &nodes {
        assert_eq!(upgrade::read(node.kv.as_ref()).unwrap().cluster_version, 2);
        assert!(node.service.check_membership_change().is_err());
    }
    assert!(nodes[leader].service.rollback().await.is_err());
    for node in nodes {
        node.stop().await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn upgrade_crash_child() {
    let Some(dir) = std::env::var_os("NODUS_TEST_UPGRADE_CRASH_DIR") else {
        return;
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let node = node(1, Path::new(&dir), listener, &Default::default()).await;
    node.raft
        .initialize(BTreeMap::from([(1, openraft::BasicNode::new(addr))]))
        .await
        .unwrap();
    let nodes = [node];
    wait_leader(&nodes).await;
    nodes[0]
        .service
        .start_upgrade(upgrade::TARGET.into())
        .await
        .unwrap();
    nodes[0].service.report_node_upgraded("1").await.unwrap();
    nodes[0].service.finalize_upgrade().await.unwrap();
    std::process::exit(88);
}

#[tokio::test]
async fn finalized_authority_survives_abrupt_exit_and_snapshot_transfer() {
    let dir = tempfile::tempdir().unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "upgrade_tests::upgrade_crash_child",
            "--nocapture",
        ])
        .env("NODUS_TEST_UPGRADE_CRASH_DIR", dir.path())
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(88));
    let kv = Arc::new(LsmKvEngine::with_wal(dir.path().join("kv"), None).unwrap());
    assert_eq!(upgrade::read(kv.as_ref()).unwrap().phase, Phase::Finalized);
    let mut store = NodusRaftStore::with_kv_at(kv.clone(), dir.path().join("snapshots/shard-meta"))
        .with_snapshot_group(META_SHARD)
        .with_snapshot_compatibility(Arc::new(upgrade::DurableSnapshotCompatibility(kv.clone())));
    store.state_machine.write().await.meta_store =
        Some(Arc::new(nodus_meta::PersistentMetaStore::new(kv.clone())));
    for (ts, value) in [(100, b"old"), (200, b"new")] {
        let txn = TxnId::new();
        kv.write_intent(
            txn,
            Bytes::from_static(b"row"),
            Bytes::copy_from_slice(value),
        )
        .unwrap();
        kv.commit(txn, ts).unwrap();
    }
    let snapshot = store.build_snapshot().await.unwrap();
    let mut file = snapshot.snapshot;
    use tokio::io::{AsyncReadExt, AsyncSeekExt};
    let mut prefix = [0; 6];
    file.read_exact(&mut prefix).await.unwrap();
    assert_eq!(&prefix, b"NSNP\0\x02");
    file.rewind().await.unwrap();
    let target = tempfile::tempdir().unwrap();
    let dst = Arc::new(LsmKvEngine::with_wal(target.path().join("kv"), None).unwrap());
    let mut receiver = NodusRaftStore::with_kv_at(dst.clone(), target.path().join("snap"))
        .with_snapshot_group(META_SHARD);
    receiver.state_machine.write().await.meta_store =
        Some(Arc::new(nodus_meta::PersistentMetaStore::new(dst.clone())));
    receiver
        .install_snapshot(&snapshot.meta, file)
        .await
        .unwrap();
    assert_eq!(upgrade::read(dst.as_ref()).unwrap().cluster_version, 2);
    assert_eq!(dst.get(b"row", 100).unwrap().unwrap().as_ref(), b"old");
    let missing = target.path().join("missing-authority.snap");
    tokio::fs::write(&missing, b"NSNP\0\x01\0").await.unwrap();
    let file = tokio::fs::File::open(missing).await.unwrap();
    assert!(
        receiver
            .install_snapshot(&snapshot.meta, Box::new(file))
            .await
            .is_err(),
        "a legacy snapshot cannot erase finalized authority"
    );
    assert_eq!(upgrade::read(dst.as_ref()).unwrap().cluster_version, 2);
    drop(receiver);
    drop(dst);
    let dst = LsmKvEngine::with_wal(target.path().join("kv"), None).unwrap();
    assert_eq!(upgrade::read(&dst).unwrap().phase, Phase::Finalized);
}

#[tokio::test]
async fn snapshot_transport_rechecks_old_recipients_and_resumed_chunks() {
    use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
    let dir = tempfile::tempdir().unwrap();
    let (cert, key, ca) = crate::tests::write_test_certs(dir.path());
    let config = nodus_config::ClusterConfig {
        tls: nodus_config::ClusterTlsConfig {
            enabled: true,
            cert_path: Some(cert),
            key_path: Some(key),
            ca_path: Some(ca),
        },
        ..Default::default()
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen = calls.clone();
    let app = axum::Router::new()
        .route(
            "/raft/capabilities/v1",
            axum::routing::post(
                |axum::Json(request): axum::Json<upgrade::ProbeV1>| async move {
                    let mut report = upgrade::CapabilityV1::local(2, request.challenge);
                    report.snapshot_version = 1;
                    axum::Json(report)
                },
            ),
        )
        .route(
            "/raft/shard-meta/snapshot",
            axum::routing::post(move || {
                let seen = seen.clone();
                async move {
                    seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    axum::http::StatusCode::OK
                }
            }),
        );
    let (tls, _) = crate::load_raft_tls_config(&config.tls).unwrap().unwrap();
    let server = tokio::spawn(async move {
        axum_server::from_tcp_rustls(
            listener.into_std().unwrap(),
            axum_server::tls_rustls::RustlsConfig::from_config(tls),
        )
        .unwrap()
        .serve(app.into_make_service())
        .await
        .unwrap();
    });
    let transport = crate::build_raft_transport(&config)
        .unwrap()
        .with_snapshot_checks();
    assert!(
        transport
            .probe(99, &addr, uuid::Uuid::new_v4(), false)
            .await
            .is_err()
    );
    assert!(
        nodus_raftstore::network::RaftTransport::plain()
            .probe(2, &addr, uuid::Uuid::new_v4(), false)
            .await
            .is_err()
    );
    let mut factory =
        nodus_raftstore::network::NodusNetworkFactory::new(META_SHARD.into(), transport);
    let mut client = factory.new_client(2, &openraft::BasicNode::new(addr)).await;
    for (offset, data) in [
        (0, b"NSNP\0\x02".to_vec()),
        (6, b"tail".to_vec()),
        (0, b"N".to_vec()),
    ] {
        let rpc = openraft::raft::InstallSnapshotRequest {
            vote: openraft::Vote::new(1, 1),
            meta: Default::default(),
            offset,
            data,
            done: false,
        };
        assert!(
            client
                .install_snapshot(rpc, RPCOption::new(Duration::from_secs(3)))
                .await
                .is_err()
        );
    }
    assert_eq!(
        calls.load(std::sync::atomic::Ordering::SeqCst),
        0,
        "old peer must receive no v2 chunks"
    );
    server.abort();
    let _ = server.await;
}
