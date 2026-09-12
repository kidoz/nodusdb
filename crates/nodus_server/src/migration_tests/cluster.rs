use super::*;
use nodus_raftstore::server::{NodusRaft, RaftState, raft_routes};
use std::collections::BTreeMap;
use std::time::Duration;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fence_replicates_over_tcp_and_survives_leader_failure() {
    let dir = tempfile::tempdir().unwrap();
    let mut listeners = Vec::new();
    let mut members = BTreeMap::new();
    for id in 1..=3 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        members.insert(
            id,
            openraft::BasicNode::new(listener.local_addr().unwrap().to_string()),
        );
        listeners.push(listener);
    }
    let config = Arc::new(openraft::Config::default().validate().unwrap());
    let mut rafts: Vec<NodusRaft> = Vec::new();
    let mut engines = Vec::new();
    let mut servers = Vec::new();
    for (i, listener) in listeners.into_iter().enumerate() {
        let kv: Arc<dyn KvEngine> = Arc::new(nodus_storage_mem::MemKvEngine::new());
        let store = NodusRaftStore::with_kv_at(kv.clone(), dir.path().join(i.to_string()));
        let (log, sm) = openraft::storage::Adaptor::new(store);
        let raft = NodusRaft::new(
            i as u64 + 1,
            config.clone(),
            nodus_raftstore::network::NodusNetworkFactory::new(
                "fixture".into(),
                Default::default(),
            ),
            log,
            sm,
        )
        .await
        .unwrap();
        let state = RaftState::new();
        state
            .rafts
            .write()
            .await
            .insert("fixture".into(), raft.clone());
        servers.push(tokio::spawn(async move {
            axum::serve(listener, raft_routes().with_state(state))
                .await
                .unwrap();
        }));
        rafts.push(raft);
        engines.push(kv);
    }
    rafts[0].initialize(members.clone()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while rafts[0].metrics().borrow().current_leader != Some(1) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    // Two contenders enter the actual leader concurrently; only one can own
    // the same expected epoch. Entries replicate through real append RPCs.
    let request = || {
        ShardCommand::MigrationV1(Control::Acquire {
            operation_id: Uuid::new_v4(),
            expected_epoch: 0,
        })
    };
    let (a, b) = tokio::join!(
        rafts[0].client_write(request()),
        rafts[0].client_write(request())
    );
    assert_ne!(a.unwrap().data.success, b.unwrap().data.success);
    tokio::time::timeout(Duration::from_secs(10), async {
        while !engines.iter().all(|e| {
            read_fence(e.as_ref())
                .unwrap()
                .is_some_and(|f| f.epoch == 1 && f.closed)
        }) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let fence = read_fence(engines[0].as_ref()).unwrap().unwrap();
    // Production client ingress stays disabled even on a compatible test
    // cluster; direct client_write above is a protocol fixture, not activation.
    let response = reqwest::Client::new()
        .post(format!("http://{}/raft/fixture/write", members[&1].addr))
        .json(&request())
        .send()
        .await
        .unwrap();
    assert_eq!(response.status(), reqwest::StatusCode::NOT_IMPLEMENTED);
    rafts[0].shutdown().await.unwrap();
    servers[0].abort();
    let leader = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            for (i, raft) in rafts.iter().enumerate().skip(1) {
                if raft.metrics().borrow().current_leader == Some(i as u64 + 1) {
                    return i;
                }
            }
            tokio::time::sleep(Duration::from_millis(30)).await;
        }
    })
    .await
    .unwrap();
    let txn_id = Uuid::new_v4();
    // Rejected legacy forwards must not enter the success-only dedup cache.
    let http = reqwest::Client::new();
    for _ in 0..2 {
        let response: ShardResponse = http
            .post(format!(
                "http://{}/raft/fixture/write",
                members[&(leader as u64 + 1)].addr
            ))
            .header(
                nodus_raftstore::server::REQUEST_ID_HEADER,
                "rejected-stale-request",
            )
            .json(&ShardCommand::DeleteIntent {
                txn_id: txn_id.to_string(),
                key: b"stale".to_vec(),
                shard_id: Some("fixture".into()),
            })
            .send()
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert!(!response.success);
        assert!(response.error.unwrap().contains("epoch is required"));
    }
    let stale = rafts[leader]
        .client_write(ShardCommand::EpochWriteV1 {
            epoch: 0,
            mutation: Mutation::Put {
                txn_id,
                key: b"stale".to_vec(),
                value: b"bad".to_vec(),
            },
        })
        .await
        .unwrap();
    assert!(!stale.data.success);
    assert!(
        rafts[leader]
            .client_write(ShardCommand::MigrationV1(Control::Release {
                operation_id: fence.operation_id,
                epoch: 1
            }))
            .await
            .unwrap()
            .data
            .success
    );
    assert!(
        rafts[leader]
            .client_write(ShardCommand::EpochWriteV1 {
                epoch: 1,
                mutation: Mutation::Put {
                    txn_id,
                    key: b"current".to_vec(),
                    value: b"ok".to_vec()
                }
            })
            .await
            .unwrap()
            .data
            .success
    );
    assert!(
        rafts[leader]
            .client_write(ShardCommand::EpochWriteV1 {
                epoch: 1,
                mutation: Mutation::Commit {
                    txn_id,
                    commit_ts: 1000
                }
            })
            .await
            .unwrap()
            .data
            .success
    );
    assert!(engines[leader].get(b"stale", 1000).unwrap().is_none());
    assert_eq!(
        engines[leader].get(b"current", 1000).unwrap().unwrap(),
        Bytes::from_static(b"ok")
    );
    for raft in rafts.iter().skip(1) {
        raft.shutdown().await.unwrap();
    }
    for server in servers {
        server.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn coordinator_journal_resumes_on_new_tcp_leader_without_acquire_ack() {
    use nodus_raftstore::migration::coordinator::{
        self, CommandV2 as C, PhaseV2 as P, PlanV2, RecordV2, driver::Backend,
    };
    struct Node {
        raft: NodusRaft,
        kv: Arc<dyn KvEngine>,
    }
    impl Backend for Node {
        async fn load(&self, table: TableId) -> anyhow::Result<RecordV2> {
            self.raft.ensure_linearizable().await?;
            Ok(coordinator::read_record_v2(self.kv.as_ref(), table)?.unwrap())
        }
        async fn submit(&self, group: &str, command: ShardCommand) -> anyhow::Result<()> {
            assert_eq!(group, "shard-meta");
            let response = self.raft.client_write(command).await?.data;
            anyhow::ensure!(response.success, "{:?}", response.error);
            Ok(())
        }
        async fn routing_matches(&self, _plan: &PlanV2) -> anyhow::Result<bool> {
            Ok(true)
        }
    }
    let dir = tempfile::tempdir().unwrap();
    let mut listeners = Vec::new();
    let mut members = BTreeMap::new();
    for id in 1..=3 {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        members.insert(
            id,
            openraft::BasicNode::new(listener.local_addr().unwrap().to_string()),
        );
        listeners.push(listener);
    }
    let config = Arc::new(openraft::Config::default().validate().unwrap());
    let mut rafts: Vec<NodusRaft> = Vec::new();
    let mut engines = Vec::new();
    let mut servers = Vec::new();
    for (i, listener) in listeners.into_iter().enumerate() {
        let kv: Arc<dyn KvEngine> = Arc::new(nodus_storage_mem::MemKvEngine::new());
        let store = NodusRaftStore::with_kv_at(kv.clone(), dir.path().join(i.to_string()));
        store.state_machine.write().await.meta_store =
            Some(Arc::new(nodus_meta::MemMetaStore::new()));
        let (log, sm) = openraft::storage::Adaptor::new(store);
        let raft = NodusRaft::new(
            i as u64 + 1,
            config.clone(),
            nodus_raftstore::network::NodusNetworkFactory::new(
                "shard-meta".into(),
                Default::default(),
            ),
            log,
            sm,
        )
        .await
        .unwrap();
        let state = RaftState::new();
        state
            .rafts
            .write()
            .await
            .insert("shard-meta".into(), raft.clone());
        servers.push(tokio::spawn(async move {
            axum::serve(listener, raft_routes().with_state(state))
                .await
                .unwrap();
        }));
        rafts.push(raft);
        engines.push(kv);
    }
    rafts[0].initialize(members.clone()).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        while rafts[0].metrics().borrow().current_leader != Some(1) {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();

    let table = TableId::new();
    let operation = Uuid::new_v4();
    let node = Node {
        raft: rafts[0].clone(),
        kv: engines[0].clone(),
    };
    node.submit(
        "shard-meta",
        ShardCommand::MigrationV2(C::Plan(PlanV2 {
            migration: MigrationPlan {
                table_id: table,
                operation_id: operation,
                sources: vec!["shard-meta".into()],
                destinations: vec!["destination".into()],
                source_epochs: BTreeMap::from([("shard-meta".into(), 0)]),
            },
            expected_map: None,
        })),
    )
    .await
    .unwrap();
    node.submit(
        "shard-meta",
        ShardCommand::MigrationV2(C::Begin { table, operation }),
    )
    .await
    .unwrap();
    node.submit(
        "shard-meta",
        ShardCommand::MigrationV1(Control::Acquire {
            operation_id: operation,
            expected_epoch: 0,
        }),
    )
    .await
    .unwrap();
    assert!(node.load(table).await.unwrap().acquired.is_empty());
    // Ensure both followers applied the acquired fence, then lose the leader
    // before its coordinator durably acknowledges that participant.
    tokio::time::timeout(Duration::from_secs(10), async {
        while !engines
            .iter()
            .all(|e| read_fence(e.as_ref()).unwrap().is_some_and(|f| f.closed))
        {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    rafts[0].shutdown().await.unwrap();
    servers[0].abort();
    let leader = tokio::time::timeout(Duration::from_secs(15), async {
        loop {
            for (i, raft) in rafts.iter().enumerate().skip(1) {
                if raft.metrics().borrow().current_leader == Some(i as u64 + 1) {
                    return i;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    let node = Node {
        raft: rafts[leader].clone(),
        kv: engines[leader].clone(),
    };
    assert_eq!(
        coordinator::driver::resume(&node, table, Duration::from_secs(5))
            .await
            .unwrap(),
        P::Fencing
    );
    assert_eq!(
        coordinator::driver::resume(&node, table, Duration::from_secs(5))
            .await
            .unwrap(),
        P::Fenced
    );
    node.submit(
        "shard-meta",
        ShardCommand::MigrationV2(C::RequestCancel { table, operation }),
    )
    .await
    .unwrap();
    coordinator::driver::resume(&node, table, Duration::from_secs(5))
        .await
        .unwrap();
    assert_eq!(
        coordinator::driver::resume(&node, table, Duration::from_secs(5))
            .await
            .unwrap(),
        P::Cancelled
    );
    assert!(!read_fence(node.kv.as_ref()).unwrap().unwrap().closed);
    assert!(
        !node
            .raft
            .client_write(ShardCommand::MigrationV1(Control::Acquire {
                operation_id: operation,
                expected_epoch: 0
            }))
            .await
            .unwrap()
            .data
            .success
    );
    // New network writers stay off, including coordinator and repair variants.
    for command in [
        ShardCommand::MigrationV2(C::Begin { table, operation }),
        ShardCommand::EpochRepairV2 {
            epoch: 1,
            txn_id: Uuid::new_v4(),
            key: b"row".to_vec(),
            replacement: nodus_raftstore::migration::writes::ReplacementV2::Clear,
        },
    ] {
        let response = reqwest::Client::new()
            .post(format!(
                "http://{}/raft/shard-meta/write",
                members[&(leader as u64 + 1)].addr
            ))
            .json(&command)
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::NOT_IMPLEMENTED);
    }
    for raft in rafts.iter().skip(1) {
        raft.shutdown().await.unwrap();
    }
    for server in servers {
        server.abort();
    }
}
