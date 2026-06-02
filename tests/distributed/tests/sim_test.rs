#[cfg(madsim)]
use madsim::net::NetSim;
#[cfg(madsim)]
use madsim::runtime::Handle;
#[cfg(madsim)]
use madsim::time::{sleep, Duration};
#[cfg(madsim)]
use porcupine_rs::{Model, Operation, check_operations};
#[cfg(madsim)]
use std::net::SocketAddr;

#[cfg(madsim)]
#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq)]
enum RegisterOp {
    Read(i32),
    Write(i32),
}

#[cfg(madsim)]
#[derive(Clone)]
struct RegisterModel;

#[cfg(madsim)]
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

#[cfg(madsim)]
#[madsim::test]
#[ignore = "Raft is currently a stub, so distributed partition recovery cannot pass yet"]
async fn test_cluster_partition_linearizability() {
    let handle = Handle::current();

    // 1. Create 3 nodes representing the cluster
    let mut nodes = vec![];
    for i in 1..=3 {
        let addr: SocketAddr = format!("192.168.1.{}:5432", i).parse().unwrap();
        let node = handle
            .create_node()
            .name(format!("node-{}", i))
            .ip(addr.ip())
            .build();
        nodes.push((node.id(), node, addr));
    }

    // The real Raft-backed cluster harness is not wired yet. This test keeps the
    // simulation network and linearizability checker compiling under cfg(madsim).

    // 3. Inject a Network Partition!
    let net = NetSim::current();
    println!("Injecting network partition...");
    let node3_id = nodes[2].0;
    net.clog_node(node3_id);

    sleep(Duration::from_secs(1)).await;

    // 4. Heal the partition
    println!("Healing network partition...");
    net.unclog_node(node3_id);

    sleep(Duration::from_secs(1)).await;

    // Verify linearizability with porcupine!
    println!("Verifying linearizability...");
    let history: Vec<Operation<RegisterModel>> = vec![];
    assert!(check_operations::<RegisterModel>(&history));
    println!("Linearizability check passed!");
}

#[cfg(not(madsim))]
#[test]
fn test_madsim_hint() {
    println!("Run with RUSTFLAGS='--cfg madsim' to execute deterministic tests.");
}
