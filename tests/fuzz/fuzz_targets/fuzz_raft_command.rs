use afl::fuzz;

fn main() {
    fuzz!(|data: &[u8]| {
        // Fuzz the Raft command deserialization to ensure malformed network
        // requests cannot panic the node's state machine layer.
        let _ = serde_json::from_slice::<nodus_raftstore::ShardCommand>(data);
    });
}