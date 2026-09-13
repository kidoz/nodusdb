use super::*;
use nodus_raftstore::{ShardCommand, SnapshotCompatibility, upgrade::admission};

fn tls_config(dir: &Path) -> nodus_config::ClusterConfig {
    let (cert, key, ca) = crate::tests::write_test_certs(dir);
    nodus_config::ClusterConfig {
        tls: nodus_config::ClusterTlsConfig {
            enabled: true,
            cert_path: Some(cert),
            key_path: Some(key),
            ca_path: Some(ca),
        },
        ..Default::default()
    }
}
async fn finalize(nodes: &[Node]) -> usize {
    let leader = wait_leader(nodes).await;
    nodes[leader]
        .service
        .start_upgrade(upgrade::TARGET.into())
        .await
        .unwrap();
    nodes[leader]
        .service
        .report_node_upgraded("1")
        .await
        .unwrap();
    nodes[leader].service.finalize_upgrade().await.unwrap();
    converge(nodes, Phase::Finalized).await;
    leader
}
async fn replace_leader(
    nodes: &mut Vec<Node>,
    leader: usize,
    dir: &Path,
    config: &nodus_config::ClusterConfig,
    members: &BTreeMap<u64, openraft::BasicNode>,
) -> usize {
    let old = nodes.remove(leader);
    let id = old.service.manager.node_id();
    old.stop().await;
    let next = wait_leader(nodes).await;
    let next_id = nodes[next].service.manager.node_id();
    let listener = TcpListener::bind(&members[&id].addr).await.unwrap();
    nodes.push(node(id, &dir.join(id.to_string()), listener, config).await);
    let next = nodes
        .iter()
        .position(|n| n.service.manager.node_id() == next_id)
        .unwrap();
    nodes[next].service.get_state().await.unwrap();
    next
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admission_resumes_across_two_leaders_and_purged_snapshot_catchup() {
    let dir = tempfile::tempdir().unwrap();
    let config = tls_config(dir.path());
    let mut nodes = Vec::new();
    let mut members = BTreeMap::new();
    for id in 1..=4 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        members.insert(
            id,
            openraft::BasicNode::new(listener.local_addr().unwrap().to_string()),
        );
        nodes.push(node(id, &dir.path().join(id.to_string()), listener, &config).await);
    }
    nodes[0]
        .raft
        .initialize(
            members
                .iter()
                .filter(|(id, _)| **id != 4)
                .map(|(id, n)| (*id, n.clone()))
                .collect::<BTreeMap<_, _>>(),
        )
        .await
        .unwrap();
    let leader = finalize(&nodes[..3]).await;
    let candidate = &members[&4].addr;
    let immutable = upgrade::read(nodes[leader].kv.as_ref()).unwrap();
    assert!(immutable.reports.values().all(|r| r.admission_version == 0));
    assert!(
        nodes[leader]
            .service
            .admission_step(99, candidate)
            .await
            .is_err(),
        "endpoint identity mismatch"
    );
    assert!(
        admission::read(nodes[leader].kv.as_ref())
            .unwrap()
            .is_none()
    );
    assert!(
        !nodes[leader]
            .service
            .admission_step(4, candidate)
            .await
            .unwrap()
    );
    let approved = admission::read(nodes[leader].kv.as_ref()).unwrap().unwrap();
    assert_eq!(
        approved.pending.as_ref().unwrap().stage,
        admission::StageV1::Approved
    );
    assert!(
        !nodes[leader]
            .service
            .manager
            .cluster_members()
            .await
            .contains_key(&4)
    );
    assert!(
        nodes[leader]
            .service
            .admission_step(5, "127.0.0.1:5555")
            .await
            .is_err()
    );
    assert!(
        nodes[leader]
            .service
            .admission_step(4, "127.0.0.1:5555")
            .await
            .is_err()
    );
    let leader = replace_leader(&mut nodes, leader, dir.path(), &config, &members).await;
    let mut committed = 0;
    for (ts, value) in [(100, "old"), (200, "new")] {
        let txn = uuid::Uuid::new_v4().to_string();
        nodes[leader]
            .raft
            .client_write(ShardCommand::PutIntent {
                txn_id: txn.clone(),
                key: b"admission-row".to_vec(),
                value: value.as_bytes().to_vec(),
                shard_id: None,
            })
            .await
            .unwrap();
        committed = nodes[leader]
            .raft
            .client_write(ShardCommand::CommitTxn {
                txn_id: txn,
                commit_ts: ts,
                shard_id: None,
            })
            .await
            .unwrap()
            .log_id
            .index;
    }
    let pending = uuid::Uuid::new_v4().to_string();
    nodes[leader]
        .raft
        .client_write(ShardCommand::PutIntent {
            txn_id: pending.clone(),
            key: b"admission-pending".to_vec(),
            value: b"resolved".to_vec(),
            shard_id: None,
        })
        .await
        .unwrap();
    nodes[leader].raft.trigger().snapshot().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while nodes[leader]
            .raft
            .metrics()
            .borrow()
            .purged
            .is_none_or(|l| l.index < committed)
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert!(
        !nodes[leader]
            .service
            .admission_step(4, candidate)
            .await
            .unwrap()
    );
    assert_eq!(
        admission::read(nodes[leader].kv.as_ref())
            .unwrap()
            .unwrap()
            .pending
            .unwrap()
            .stage,
        admission::StageV1::Promoting
    );
    let learner = nodes
        .iter()
        .find(|n| n.service.manager.node_id() == 4)
        .unwrap();
    assert_eq!(
        learner
            .kv
            .get(b"admission-row", 100)
            .unwrap()
            .unwrap()
            .as_ref(),
        b"old"
    );
    assert_eq!(
        learner
            .kv
            .get(b"admission-row", 200)
            .unwrap()
            .unwrap()
            .as_ref(),
        b"new"
    );
    let snapshot_path = dir.path().join("4/snapshots/shard-meta/current.snap");
    tokio::time::timeout(Duration::from_secs(5), async {
        while !snapshot_path.exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(&std::fs::read(&snapshot_path).unwrap()[..6], b"NSNP\0\x02");
    assert!(
        learner
            .kv
            .get(b"admission-pending", u64::MAX)
            .unwrap()
            .is_none()
    );
    let leader = replace_leader(&mut nodes, leader, dir.path(), &config, &members).await;
    nodes[leader]
        .service
        .admit_member_locked(4, candidate)
        .await
        .unwrap();
    nodes[leader]
        .service
        .admit_member_locked(4, candidate)
        .await
        .unwrap();
    assert!(
        nodes[leader]
            .service
            .admit_member_locked(4, "127.0.0.1:9999")
            .await
            .is_err()
    );
    nodes[leader]
        .raft
        .client_write(ShardCommand::CommitTxn {
            txn_id: pending,
            commit_ts: 300,
            shard_id: None,
        })
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            if nodes.iter().all(|n| {
                admission::read(n.kv.as_ref())
                    .unwrap()
                    .is_some_and(|r| r.pending.is_none())
                    && n.kv.get(b"admission-pending", 300).unwrap().is_some()
            }) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    for n in &nodes {
        assert_eq!(upgrade::read(n.kv.as_ref()).unwrap(), immutable);
        assert_eq!(
            upgrade::DurableSnapshotCompatibility(n.kv.clone())
                .member_snapshot_versions()
                .unwrap()
                .get(&4),
            Some(&2)
        );
    }
    for n in nodes {
        n.stop().await;
    }
    let kv = LsmKvEngine::with_wal(dir.path().join("4/kv"), None).unwrap();
    assert!(admission::read(&kv).unwrap().unwrap().pending.is_none());
    assert_eq!(
        kv.get(b"admission-row", 100).unwrap().unwrap().as_ref(),
        b"old"
    );
    assert_eq!(
        kv.get(b"admission-pending", 300).unwrap().unwrap().as_ref(),
        b"resolved"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn admission_crash_child() {
    let Some(dir) = std::env::var_os("NODUS_TEST_ADMISSION_CRASH_DIR") else {
        return;
    };
    let dir = Path::new(&dir);
    let config = tls_config(dir);
    let mut nodes = Vec::new();
    let mut members = BTreeMap::new();
    for id in 1..=2 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        members.insert(
            id,
            openraft::BasicNode::new(listener.local_addr().unwrap().to_string()),
        );
        nodes.push(node(id, &dir.join(id.to_string()), listener, &config).await);
    }
    nodes[0]
        .raft
        .initialize(BTreeMap::from([(1, members[&1].clone())]))
        .await
        .unwrap();
    finalize(&nodes[..1]).await;
    nodes[0]
        .service
        .admission_step(2, &members[&2].addr)
        .await
        .unwrap();
    std::fs::write(
        dir.join("members.json"),
        serde_json::to_vec(&members).unwrap(),
    )
    .unwrap();
    std::process::exit(89);
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn acknowledged_admission_survives_abrupt_exit_and_resumes() {
    let dir = tempfile::tempdir().unwrap();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "upgrade_tests::admission::admission_crash_child",
            "--nocapture",
        ])
        .env("NODUS_TEST_ADMISSION_CRASH_DIR", dir.path())
        .status()
        .unwrap();
    assert_eq!(status.code(), Some(89));
    let members: BTreeMap<u64, openraft::BasicNode> =
        serde_json::from_slice(&std::fs::read(dir.path().join("members.json")).unwrap()).unwrap();
    let config = tls_config(dir.path());
    let mut nodes = Vec::new();
    for id in 1..=2 {
        let listener = TcpListener::bind(&members[&id].addr).await.unwrap();
        nodes.push(node(id, &dir.path().join(id.to_string()), listener, &config).await);
    }
    let recovered = admission::read(nodes[0].kv.as_ref()).unwrap().unwrap();
    assert_eq!(
        recovered.pending.as_ref().unwrap().stage,
        admission::StageV1::Approved
    );
    wait_leader(&nodes).await;
    nodes[0]
        .service
        .admit_member_locked(2, &members[&2].addr)
        .await
        .unwrap();
    assert!(
        admission::read(nodes[0].kv.as_ref())
            .unwrap()
            .unwrap()
            .pending
            .is_none()
    );
    for n in nodes {
        n.stop().await;
    }
}

#[tokio::test]
async fn admitted_cluster_blocks_old_reader_before_append_and_snapshot_bytes() {
    use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
    let dir = tempfile::tempdir().unwrap();
    let config = tls_config(dir.path());
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let seen = calls.clone();
    let app = axum::Router::new()
        .route(
            "/raft/capabilities/v1",
            axum::routing::post(
                |axum::Json(request): axum::Json<upgrade::ProbeV1>| async move {
                    let mut report = upgrade::CapabilityV1::local(2, request.challenge);
                    report.admission_version = 0;
                    axum::Json(report)
                },
            ),
        )
        .fallback(move || {
            let seen = seen.clone();
            async move {
                seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                axum::http::StatusCode::OK
            }
        });
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
    let kv = Arc::new(nodus_storage_mem::MemKvEngine::new());
    let ledger = admission::RecordV1 {
        version: 1,
        revision: 2,
        authority_revision: 1,
        pending: None,
        admitted: BTreeMap::from([(
            2,
            admission::MemberV1 {
                address: address.clone(),
                report: upgrade::CapabilityV1::local(2, uuid::Uuid::new_v4()),
            },
        )]),
    };
    let txn = TxnId::new();
    kv.write_intent(
        txn,
        Bytes::from_static(admission::KEY),
        Bytes::from(serde_json::to_vec(&ledger).unwrap()),
    )
    .unwrap();
    kv.commit(txn, 2).unwrap();
    let transport = crate::build_raft_transport(&config)
        .unwrap()
        .with_snapshot_checks()
        .with_admission_source(kv);
    let mut factory =
        nodus_raftstore::network::NodusNetworkFactory::new(META_SHARD.into(), transport);
    let mut client = factory
        .new_client(2, &openraft::BasicNode::new(address))
        .await;
    for (offset, data) in [(0, b"NSNP\0\x02".to_vec()), (6, b"resume".to_vec())] {
        assert!(
            client
                .install_snapshot(
                    openraft::raft::InstallSnapshotRequest {
                        vote: openraft::Vote::new(1, 1),
                        meta: Default::default(),
                        offset,
                        data,
                        done: false
                    },
                    RPCOption::new(Duration::from_secs(3))
                )
                .await
                .is_err()
        );
    }
    assert!(
        client
            .append_entries(
                openraft::raft::AppendEntriesRequest {
                    vote: openraft::Vote::new(1, 1),
                    prev_log_id: None,
                    entries: vec![],
                    leader_commit: None
                },
                RPCOption::new(Duration::from_secs(3))
            )
            .await
            .is_err()
    );
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    server.abort();
    let _ = server.await;
}
