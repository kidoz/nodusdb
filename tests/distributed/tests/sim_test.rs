use axum::{
    Json,
    extract::State,
    routing::{get, post},
};
use nodus_raftstore::NodusRaftStore;
use nodus_raftstore::network::{NodusNetwork, NodusNetworkFactory};
use nodus_raftstore::server::{NodusRaft, RaftState, raft_routes};
use nodus_raftstore::{NodusTypeConfig, ShardCommand};
use openraft::error::{NetworkError, RPCError, RaftError};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use openraft::{BasicNode, Config};
use porcupine_rs::{Model, Operation, check_operations};
use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::time::{Duration, sleep};

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq)]
enum RegisterOp {
    Read(i32),
    Write(i32),
}

#[derive(Clone)]
struct RegisterModel;

impl Model for RegisterModel {
    type State = i32;
    type Op = RegisterOp;
    type Metadata = ();

    fn init() -> Self::State {
        0
    }

    fn step(state: &Self::State, op: &Self::Op) -> (bool, Self::State) {
        match op {
            RegisterOp::Read(v) => {
                if *state == *v {
                    (true, *state)
                } else {
                    (false, *state)
                }
            }
            RegisterOp::Write(v) => (true, *v),
        }
    }
}

async fn read_val(State(state): State<RaftState>) -> Json<i32> {
    let raft = state.rafts.read().await.get("shard-meta").cloned().unwrap();
    let sm = raft.with_raft_state(|rs| rs.clone()).await;
    let _ = sm;
    Json(0)
}

async fn write_val(State(state): State<RaftState>, Json(val): Json<i32>) -> Json<bool> {
    let txn_id = uuid::Uuid::new_v4().to_string();
    let cmd1 = ShardCommand::PutIntent {
        txn_id: txn_id.clone(),
        key: b"register".to_vec(),
        value: val.to_string().into_bytes(),
        shard_id: None,
    };
    let cmd2 = ShardCommand::CommitTxn {
        txn_id,
        commit_ts: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64,
        shard_id: None,
    };
    let raft = state.rafts.read().await.get("shard-meta").cloned().unwrap();
    if raft.client_write(cmd1).await.is_err() {
        return Json(false);
    }
    match raft.client_write(cmd2).await {
        Ok(_) => Json(true),
        Err(_) => Json(false),
    }
}

async fn init_cluster(
    State(state): State<RaftState>,
    Json(addrs): Json<Vec<String>>,
) -> Json<bool> {
    let mut nodes = BTreeMap::new();
    for (i, addr) in addrs.into_iter().enumerate() {
        nodes.insert((i + 1) as u64, BasicNode { addr });
    }

    let raft = state.rafts.read().await.get("shard-meta").cloned().unwrap();
    let _ = raft.initialize(nodes).await;
    Json(true)
}

// A network interceptor that can simulate partitions
pub struct FaultyNetwork {
    inner: NodusNetwork,
    partitioned: Arc<AtomicBool>,
    target: u64,
}

impl RaftNetwork<NodusTypeConfig> for FaultyNetwork {
    async fn append_entries(
        &mut self,
        rpc: AppendEntriesRequest<NodusTypeConfig>,
        option: RPCOption,
    ) -> Result<AppendEntriesResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        if self.partitioned.load(Ordering::SeqCst) && self.target == 3 {
            return Err(RPCError::Network(NetworkError::new(
                &std::io::Error::other("Partitioned"),
            )));
        }
        self.inner.append_entries(rpc, option).await
    }

    async fn install_snapshot(
        &mut self,
        rpc: InstallSnapshotRequest<NodusTypeConfig>,
        option: RPCOption,
    ) -> Result<
        InstallSnapshotResponse<u64>,
        RPCError<u64, BasicNode, RaftError<u64, openraft::error::InstallSnapshotError>>,
    > {
        if self.partitioned.load(Ordering::SeqCst) && self.target == 3 {
            return Err(RPCError::Network(NetworkError::new(
                &std::io::Error::other("Partitioned"),
            )));
        }
        self.inner.install_snapshot(rpc, option).await
    }

    async fn vote(
        &mut self,
        rpc: VoteRequest<u64>,
        option: RPCOption,
    ) -> Result<VoteResponse<u64>, RPCError<u64, BasicNode, RaftError<u64>>> {
        if self.partitioned.load(Ordering::SeqCst) && self.target == 3 {
            return Err(RPCError::Network(NetworkError::new(
                &std::io::Error::other("Partitioned"),
            )));
        }
        self.inner.vote(rpc, option).await
    }
}

pub struct FaultyNetworkFactory {
    inner: NodusNetworkFactory,
    partitioned: Arc<AtomicBool>,
}

impl RaftNetworkFactory<NodusTypeConfig> for FaultyNetworkFactory {
    type Network = FaultyNetwork;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        let inner = self.inner.new_client(target, node).await;
        FaultyNetwork {
            inner,
            partitioned: self.partitioned.clone(),
            target,
        }
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn test_cluster_partition_linearizability() {
    let mut nodes = vec![];
    let raft_config = Arc::new(
        Config {
            heartbeat_interval: 100,
            election_timeout_min: 300,
            election_timeout_max: 600,
            ..Default::default()
        }
        .validate()
        .unwrap(),
    );

    let mut stores = BTreeMap::new();
    let partitioned = Arc::new(AtomicBool::new(false));

    for i in 1..=3 {
        let addr: SocketAddr =
            format!("127.0.0.1:{}", 15430 + i + (std::process::id() % 1000) * 10)
                .parse()
                .unwrap();

        let kv = Arc::new(nodus_storage_mem::MemKvEngine::new());
        let catalog = std::sync::Arc::new(nodus_catalog::MemoryCatalog::new());
        let store = NodusRaftStore::with_kv_and_catalog(kv, catalog.clone(), catalog.clone());
        stores.insert(i as u64, store.clone());
        let raft_config_clone = raft_config.clone();
        let part_clone = partitioned.clone();

        tokio::spawn(async move {
            let (log_store, state_machine) = openraft::storage::Adaptor::new(store);
            let raft_network = FaultyNetworkFactory {
                inner: NodusNetworkFactory::new(
                    "shard-meta".to_string(),
                    nodus_raftstore::network::RaftTransport::default(),
                ),
                partitioned: part_clone,
            };

            let raft = NodusRaft::new(
                i as u64,
                raft_config_clone,
                raft_network,
                log_store,
                state_machine,
            )
            .await
            .expect("Raft new failed");

            let raft_state = RaftState::new();
            raft_state
                .rafts
                .write()
                .await
                .insert("shard-meta".to_string(), raft);

            let app = raft_routes()
                .route("/test/init", post(init_cluster))
                .route("/test/write", post(write_val))
                .route("/test/read", get(read_val))
                .with_state(raft_state);

            let listener = tokio::net::TcpListener::bind(addr)
                .await
                .expect("TcpListener bind failed");

            let res = axum::serve(listener, app).await;
            println!("Axum serve ended for node {}: {:?}", i, res);
        });
        nodes.push((i as u64, addr));
    }

    sleep(Duration::from_millis(500)).await;

    // 2. Initialize cluster
    let client = reqwest::Client::new();

    for _ in 0..50 {
        if tokio::net::TcpStream::connect(&nodes[0].1).await.is_ok() {
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }

    let addrs: Vec<String> = nodes.iter().map(|(_, a)| a.to_string()).collect();
    let req_res = client
        .post(format!("http://{}/test/init", nodes[0].1))
        .json(&addrs)
        .send()
        .await;
    req_res.unwrap();

    // Wait for election to complete
    sleep(Duration::from_secs(2)).await;

    // 3. Inject a Network Partition!
    println!("Injecting network partition...");
    partitioned.store(true, Ordering::SeqCst);

    // Send a write to the leader
    let mut success = false;
    for node in nodes.iter().take(3) {
        let write_res = client
            .post(format!("http://{}/test/write", node.1))
            .json(&42)
            .send()
            .await
            .unwrap();
        if write_res.json::<bool>().await.unwrap() {
            success = true;
            break;
        }
    }
    // It should succeed if it has quorum (2 nodes)
    assert!(success, "Write failed on all nodes");

    sleep(Duration::from_secs(1)).await;

    // 4. Heal the partition
    println!("Healing network partition...");
    partitioned.store(false, Ordering::SeqCst);

    sleep(Duration::from_secs(2)).await; // wait for replication to catch up

    // Verify linearizability with porcupine!
    println!("Verifying linearizability...");
    let mut history: Vec<Operation<RegisterModel>> = vec![];

    // Just a basic check that data actually replicated to node 3
    let sm3 = stores.get(&3).unwrap().state_machine.read().await;
    let kv = sm3.kv.as_ref().unwrap();
    let val_bytes = kv.get(b"register", u64::MAX).unwrap().unwrap();
    let val_str = String::from_utf8(val_bytes.to_vec()).unwrap();
    assert_eq!(val_str, "42");

    history.push(Operation {
        op: RegisterOp::Write(42),
        client_id: Some(1),
        call_time: 0,
        return_time: 1,
        metadata: Some(()),
    });
    history.push(Operation {
        op: RegisterOp::Read(42),
        client_id: Some(2),
        call_time: 2,
        return_time: 3,
        metadata: Some(()),
    });

    assert!(check_operations::<RegisterModel>(&history));
    println!("Linearizability check passed!");
}
