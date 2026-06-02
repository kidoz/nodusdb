use afl::fuzz;

fn main() {
    fuzz!(|data: &[u8]| {
        // Fuzz the WAL record deserialization to ensure corrupted disk records
        // do not cause the recovery process to panic or crash.
        let _ = serde_json::from_slice::<nodus_storage_wal::WalRecord>(data);
    });
}