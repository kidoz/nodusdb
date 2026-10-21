//! Real three-node mTLS cluster evidence for the V3 spine: a split driven
//! through the production backend survives meta-leader loss mid-copy; the
//! surviving meta leader's discovery pass resumes it, and the exact row
//! multiset lands at the destinations.
use super::driver::{is_table_key, user_range};
use super::*;
use crate::migration_coordinator::{drive_open_journals_once, v3_backend::ProductionBackendV3};
use crate::multi_raft::{META_SHARD, MultiRaftManager};
use crate::raft_router::RaftRouter;
use nodus_meta::MetaStore;
use nodus_raftstore::migration::coordinator::v3::{self, CommandV3 as C3, PhaseV3 as P3, PlanV3};
use nodus_raftstore::server::{RaftRouteAuth, RaftState, raft_routes};
use nodus_raftstore::upgrade::{self, CapabilityV1, Phase, RecordV1};
use nodus_sharding::ShardOrchestrator;
use nodus_storage_api::NamespacedKvEngine;
use openraft::{Membership, StoredMembership};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::Duration;

/// More than the production copy batch, so a mid-copy checkpoint exists.
const ROWS: usize = 300;

struct V3Node {
    id: u64,
    kv: Arc<dyn KvEngine>,
    manager: Arc<MultiRaftManager>,
    router: RaftRouter,
    server: Option<tokio::task::JoinHandle<()>>,
}

/// A finalized level-3 authority record naming the real fixture roster.
fn finalized_level3(membership: &StoredMembership<u64, openraft::BasicNode>) -> Vec<u8> {
    let session = Uuid::new_v4();
    let reports = membership
        .membership()
        .nodes()
        .map(|(id, _)| {
            let mut report = CapabilityV1::local(*id, session);
            report.ready_for_finalize = true;
            (*id, report)
        })
        .collect();
    let record = RecordV1 {
        version: 1,
        revision: 3,
        phase: Phase::Finalized,
        cluster_version: upgrade::MIGRATION_LEVEL,
        session,
        membership: membership.clone(),
        reports,
    };
    serde_json::to_vec(&record).unwrap()
}

fn write_authority(kv: &dyn KvEngine, bytes: &[u8]) {
    let txn = TxnId::new();
    kv.write_intent(
        txn,
        Bytes::from_static(upgrade::KEY),
        Bytes::from(bytes.to_vec()),
    )
    .unwrap();
    kv.commit(txn, 1_000).unwrap();
}

async fn v3_node(
    id: u64,
    listener: tokio::net::TcpListener,
    dir: &std::path::Path,
    tls: &nodus_config::ClusterTlsConfig,
    authority: &[u8],
) -> V3Node {
    let addr = listener.local_addr().unwrap().to_string();
    let kv: Arc<dyn KvEngine> = Arc::new(nodus_storage_mem::MemKvEngine::new());
    write_authority(kv.as_ref(), authority);
    let state = RaftState::new();
    let cluster = nodus_config::ClusterConfig {
        node_id: id,
        raft_advertise_addr: addr.clone(),
        tls: tls.clone(),
        ..Default::default()
    };
    let transport = crate::build_raft_transport(&cluster)
        .unwrap()
        .with_snapshot_checks();
    let manager = Arc::new(MultiRaftManager::new(
        id,
        addr,
        Arc::new(openraft::Config::default().validate().unwrap()),
        state.clone(),
        kv.clone(),
        None,
        Arc::new(nodus_txn::MemTxnManager::new()),
        Some(dir.to_path_buf()),
        transport,
    ));
    let catalog = Arc::new(nodus_catalog::MemoryCatalog::new());
    let meta: Arc<dyn nodus_meta::MetaStore> =
        Arc::new(nodus_meta::PersistentMetaStore::new(kv.clone()));
    manager
        .create_meta(kv.clone(), catalog.clone(), catalog, meta)
        .await
        .unwrap();
    // The production gate: durable authority on this node, fail closed.
    let gate_kv = kv.clone();
    let router = RaftRouter::spawn(manager.clone()).with_migration_gate(move || {
        upgrade::read(gate_kv.as_ref()).is_ok_and(|a| a.cluster_version >= upgrade::MIGRATION_LEVEL)
    });
    let (server_cfg, _resolver) = crate::load_raft_tls_config(&cluster.tls).unwrap().unwrap();
    let app = raft_routes(RaftRouteAuth::TransportAuthenticated).with_state(state);
    let server = tokio::spawn(async move {
        axum_server::from_tcp_rustls(
            listener.into_std().unwrap(),
            axum_server::tls_rustls::RustlsConfig::from_config(server_cfg),
        )
        .unwrap()
        .serve(app.into_make_service())
        .await
        .unwrap();
    });
    V3Node {
        id,
        kv,
        manager,
        router,
        server: Some(server),
    }
}

impl V3Node {
    async fn meta_leader(&self) -> bool {
        match self.manager.get(META_SHARD).await {
            Some(raft) => raft.metrics().borrow().current_leader == Some(self.id),
            None => false,
        }
    }
}

/// Waits (bounded) for `group` to elect a leader among `nodes`, returning its index.
async fn await_leader(nodes: &[V3Node], group: &str) -> usize {
    tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            for (i, node) in nodes.iter().enumerate() {
                if let Some(raft) = node.manager.get(group).await
                    && raft.metrics().borrow().current_leader == Some(node.id)
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

#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn split_survives_meta_leader_loss_mid_copy() {
    let dir = tempfile::tempdir().unwrap();
    let (cert_path, key_path, ca_path) = crate::tests::write_test_certs(dir.path());
    let tls = nodus_config::ClusterTlsConfig {
        enabled: true,
        cert_path: Some(cert_path),
        key_path: Some(key_path),
        ca_path: Some(ca_path),
    };
    let mut listeners = Vec::new();
    let mut members = BTreeMap::new();
    for id in 1..=3u64 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        members.insert(
            id,
            openraft::BasicNode::new(listener.local_addr().unwrap().to_string()),
        );
        listeners.push((id, listener));
    }
    let membership = StoredMembership::new(
        None,
        Membership::new(vec![BTreeSet::from([1, 2, 3])], members),
    );
    let authority = finalized_level3(&membership);
    let mut nodes = Vec::new();
    for (id, listener) in listeners {
        nodes.push(
            v3_node(
                id,
                listener,
                &dir.path().join(id.to_string()),
                &tls,
                &authority,
            )
            .await,
        );
    }

    // Meta group across all three; node 1 leads.
    nodes[0]
        .manager
        .get(META_SHARD)
        .await
        .unwrap()
        .initialize(membership_members(&membership))
        .await
        .unwrap();
    assert_eq!(await_leader(&nodes, META_SHARD).await, 0);

    // A table with one full-range shard placed on node 1.
    let table = TableId::new();
    let source = nodus_catalog::ShardId::new();
    let now = chrono::Utc::now();
    let source_descriptor = nodus_catalog::ShardDescriptor {
        id: source,
        name: "shard-0".into(),
        version: 1,
        created_at: now,
        updated_at: now,
        state: nodus_catalog::DescriptorState::Public,
        table_id: table,
        start_key: vec![],
        end_key: vec![],
    };
    let source_group = MultiRaftManager::data_group_id(source);
    let meta_raft = nodes[0].manager.get(META_SHARD).await.unwrap();
    for cmd in [
        ShardCommand::UpdateShardMap(nodus_meta::ShardMap {
            table_id: table,
            shards: vec![source_descriptor.clone()],
        }),
        ShardCommand::UpdateShardPlacements(HashMap::from([(source, "1".to_string())])),
    ] {
        assert!(meta_raft.client_write(cmd).await.unwrap().data.success);
    }

    // The source group, replicated to all three, holding ROWS committed rows.
    for node in &nodes {
        node.manager
            .get_or_create_data(&source_group)
            .await
            .unwrap();
    }
    nodes[0]
        .manager
        .get(&source_group)
        .await
        .unwrap()
        .initialize(membership_members(&membership))
        .await
        .unwrap();
    await_leader(&nodes, &source_group).await;
    let source_raft = nodes[0].manager.get(&source_group).await.unwrap();
    let mut expected: Vec<(Bytes, Bytes, u64)> = Vec::new();
    for i in 0..ROWS {
        let txn = Uuid::new_v4().to_string();
        let key = format!("{table}:{i:03}").into_bytes();
        let value = format!("v{i}").into_bytes();
        for cmd in [
            ShardCommand::PutIntent {
                txn_id: txn.clone(),
                key: key.clone(),
                value: value.clone(),
                shard_id: Some(source_group.clone()),
            },
            ShardCommand::CommitTxn {
                txn_id: txn,
                commit_ts: 100 + i as u64,
                shard_id: Some(source_group.clone()),
            },
        ] {
            assert!(source_raft.client_write(cmd).await.unwrap().data.success);
        }
        expected.push((Bytes::from(key), Bytes::from(value), 100 + i as u64));
    }
    expected.sort();

    // The split plan: children at "{table}:1", pre-formed on all three nodes
    // (the fixture serves no admin replica-instantiation endpoint; production
    // forms them via `form_replicated`).
    let orchestrator = ShardOrchestrator::new(Arc::new(nodus_meta::PersistentMetaStore::new(
        nodes[0].kv.clone(),
    )));
    let split = orchestrator
        .plan_split(table, source, format!("{table}:1").into_bytes())
        .unwrap();
    let destinations = vec![
        MultiRaftManager::data_group_id(split.left.id),
        MultiRaftManager::data_group_id(split.right.id),
    ];
    for group in &destinations {
        for node in &nodes {
            node.manager.get_or_create_data(group).await.unwrap();
        }
        nodes[0]
            .manager
            .get(group)
            .await
            .unwrap()
            .initialize(membership_members(&membership))
            .await
            .unwrap();
    }
    let plan = PlanV3 {
        migration: MigrationPlan {
            operation_id: Uuid::new_v4(),
            table_id: table,
            source_epochs: BTreeMap::from([(META_SHARD.to_string(), 0), (source_group.clone(), 0)]),
            sources: vec![META_SHARD.to_string(), source_group.clone()],
            destinations: destinations.clone(),
        },
        expected_map: Some(nodus_meta::ShardMap {
            table_id: table,
            shards: vec![source_descriptor],
        }),
        expected_placements: HashMap::from([(source, "1".to_string())]),
        destination_map: Some(split.new_map.clone()),
        destination_placements: HashMap::from([
            (split.left.id, "1".to_string()),
            (split.right.id, "1".to_string()),
        ]),
    };

    // Journal the plan through the router (gate: durable level 3).
    let router = nodes[0].router.clone();
    let plan_cmd = ShardCommand::MigrationV3(C3::Plan(plan.clone()));
    tokio::task::spawn_blocking(move || router.submit(META_SHARD, plan_cmd))
        .await
        .unwrap()
        .unwrap();

    // Drive on node 1 until the copy is mid-flight (a journaled partial
    // checkpoint beyond one full batch).
    let backend = ProductionBackendV3::new(nodes[0].manager.clone(), nodes[0].router.clone());
    tokio::time::timeout(Duration::from_secs(60), async {
        loop {
            let phase = v3::driver::resume(&backend, table, Duration::from_secs(5))
                .await
                .unwrap();
            let journal = v3::read_record_v3(nodes[0].kv.as_ref(), table)
                .unwrap()
                .unwrap();
            let mid_copy = journal
                .checkpoints
                .get(&source_group)
                .is_some_and(|c| !c.complete && !c.cursor.is_empty());
            if phase == P3::Copying && mid_copy {
                break;
            }
            assert!(
                !matches!(phase, P3::Done | P3::Cancelled),
                "operation finished before the crash point"
            );
        }
    })
    .await
    .unwrap();

    // Kill node 1: meta leader, source leader, and destination leader all move.
    nodes[0].manager.shutdown_all().await;
    nodes[0].server.take().unwrap().abort();

    // A survivor wins the meta election and its discovery pass resumes the
    // interrupted operation — fencing was journaled, checkpoints resume the
    // copy, validation and publication complete there.
    let mut survivors: Vec<&V3Node> = nodes.iter().skip(1).collect();
    let leader = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            for (i, node) in survivors.iter().enumerate() {
                if node.meta_leader().await {
                    return i;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    let driver_node = survivors.remove(leader);
    tokio::time::timeout(Duration::from_secs(120), async {
        loop {
            if std::env::var("V3_DEBUG").is_ok() {
                let backend = ProductionBackendV3::new(
                    driver_node.manager.clone(),
                    driver_node.router.clone(),
                );
                let outcome =
                    v3::driver::drive_some(&backend, table, Duration::from_secs(5), 64).await;
                eprintln!("drive outcome: {outcome:?}");
            } else {
                drive_open_journals_once(&driver_node.manager, &driver_node.router)
                    .await
                    .unwrap();
            }
            let journal = v3::read_record_v3(driver_node.kv.as_ref(), table)
                .unwrap()
                .unwrap();
            if std::env::var("V3_DEBUG").is_ok() {
                eprintln!(
                    "phase={:?} prepared={:?} checkpoints={:?} validation={:?} completed={:?}",
                    journal.phase,
                    journal.prepared,
                    journal.checkpoints,
                    journal.validation,
                    journal.completed
                );
            }
            if journal.phase == P3::Done && !driver_node.manager.hosts(&source_group) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();

    // The exact key/version multiset landed at the destinations.
    let mut copied = Vec::new();
    for group in &destinations {
        let ns = NamespacedKvEngine::new(driver_node.kv.clone(), group);
        copied.extend(
            ns.scan(user_range(), u64::MAX)
                .unwrap()
                .map(|r| r.unwrap())
                .filter(|p| is_table_key(table, &p.key))
                .map(|p| (p.key, p.value, p.version)),
        );
    }
    copied.sort();
    assert_eq!(copied, expected, "exact multiset moved across the failover");

    // Routing flipped on both survivors; the source fence reopened at epoch 1.
    for node in &survivors {
        let map = nodus_meta::PersistentMetaStore::new(node.kv.clone())
            .get_shard_map(table)
            .unwrap();
        assert_eq!(map.shards.len(), 2, "destination map replicated");
    }
    let source_ns: Arc<dyn KvEngine> = Arc::new(NamespacedKvEngine::new(
        driver_node.kv.clone(),
        &source_group,
    ));
    let fence = read_fence(source_ns.as_ref()).unwrap().unwrap();
    assert!(!fence.closed && fence.epoch == 1);

    for node in &mut nodes[1..] {
        node.manager.shutdown_all().await;
        if let Some(server) = node.server.take() {
            server.abort();
        }
    }
}

fn membership_members(
    membership: &StoredMembership<u64, openraft::BasicNode>,
) -> BTreeMap<u64, openraft::BasicNode> {
    membership
        .membership()
        .nodes()
        .map(|(id, node)| (*id, node.clone()))
        .collect()
}

/// Meta-leader loss with an exporting backup job: the surviving leader's sweep
/// marks the abandoned job `Failed`, so a crashed exporter never blocks future
/// backups. The journal replicates; the sweep runs on the new leader.
#[tokio::test(flavor = "multi_thread", worker_threads = 6)]
async fn backup_job_abandoned_by_a_dead_exporter_is_swept_after_failover() {
    use nodus_raftstore::backup::{self, JobV1, KindV1, PhaseV1 as BP};

    let dir = tempfile::tempdir().unwrap();
    let (cert_path, key_path, ca_path) = crate::tests::write_test_certs(dir.path());
    let tls = nodus_config::ClusterTlsConfig {
        enabled: true,
        cert_path: Some(cert_path),
        key_path: Some(key_path),
        ca_path: Some(ca_path),
    };
    let mut listeners = Vec::new();
    let mut members = BTreeMap::new();
    for id in 1..=3u64 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        members.insert(
            id,
            openraft::BasicNode::new(listener.local_addr().unwrap().to_string()),
        );
        listeners.push((id, listener));
    }
    let membership = StoredMembership::new(
        None,
        Membership::new(vec![BTreeSet::from([1, 2, 3])], members),
    );
    let authority = finalized_level3(&membership);
    let mut nodes = Vec::new();
    for (id, listener) in listeners {
        nodes.push(
            v3_node(
                id,
                listener,
                &dir.path().join(id.to_string()),
                &tls,
                &authority,
            )
            .await,
        );
    }
    nodes[0]
        .manager
        .get(META_SHARD)
        .await
        .unwrap()
        .initialize(membership_members(&membership))
        .await
        .unwrap();
    assert_eq!(await_leader(&nodes, META_SHARD).await, 0);

    // Node 1 (meta leader) opens an export and dies before completing it.
    let job = JobV1 {
        job_id: Uuid::from_u128(77),
        backup_id: "backup-77".into(),
        creator: 1,
        cluster_id: "test".into(),
        kind: KindV1::Full,
        backup_ts: 0,
        protected_ts: None,
        updated_at_ms: 0,
        phase: BP::Exporting,
        error: None,
    };
    crate::backup_coordinator::plan_job(&nodes[0].router, job)
        .await
        .unwrap();
    nodes[0].manager.shutdown_all().await;
    nodes[0].server.take().unwrap().abort();

    // The survivor leader sweeps the abandoned job.
    let survivors: Vec<&V3Node> = nodes.iter().skip(1).collect();
    let leader = tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            for (i, node) in survivors.iter().enumerate() {
                if node.meta_leader().await {
                    return i;
                }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .unwrap();
    let leader = survivors[leader];
    let now_ms = 10_000_000u64;
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            let failed = crate::backup_coordinator::sweep_stale_jobs_once(
                &leader.manager,
                &leader.router,
                now_ms,
                crate::backup_coordinator::JOB_STALE_AFTER,
            )
            .await
            .unwrap();
            if failed == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .unwrap();
    let record = backup::read_job(leader.kv.as_ref(), Uuid::from_u128(77))
        .unwrap()
        .unwrap();
    assert_eq!(record.phase, BP::Failed, "journal replicated and swept");

    // The failed job frees the slot: the next backup plans on the survivor.
    crate::backup_coordinator::plan_job(
        &leader.router,
        JobV1 {
            job_id: Uuid::from_u128(78),
            backup_id: "backup-78".into(),
            creator: leader.id,
            cluster_id: "test".into(),
            kind: KindV1::Full,
            backup_ts: 0,
            protected_ts: None,
            updated_at_ms: now_ms,
            phase: BP::Exporting,
            error: None,
        },
    )
    .await
    .unwrap();

    for node in &mut nodes[1..] {
        node.manager.shutdown_all().await;
        if let Some(server) = node.server.take() {
            server.abort();
        }
    }
}
