//! Node-local recovery generation, replaced atomically with a storage checkpoint.
use super::*;

pub const RECOVERY_GENERATION_KEY: &[u8] = b"\0recovery\0generation";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct RecoveryGeneration {
    pub id: Uuid,
    /// First WAL segment of this checkpoint. Older segments cannot be replayed
    /// even when their timestamps overlap a newer full backup.
    pub wal_floor: u64,
}

impl RecoveryGeneration {
    pub fn decode(bytes: &[u8]) -> Result<Self> {
        anyhow::ensure!(
            bytes.len() == 30 && &bytes[..6] == b"NRBG\0\x01",
            "unsupported or corrupt recovery generation record"
        );
        Ok(Self {
            id: Uuid::from_slice(&bytes[6..22])?,
            wal_floor: u64::from_be_bytes(bytes[22..30].try_into()?),
        })
    }

    /// Existing KV/SST formats carry this versioned local metadata record.
    pub fn checkpoint_row(wal_floor: u64) -> SnapshotRow {
        let mut bytes = b"NRBG\0\x01".to_vec();
        bytes.extend_from_slice(Uuid::new_v4().as_bytes());
        bytes.extend_from_slice(&wal_floor.to_be_bytes());
        SnapshotRow::committed(
            Bytes::from_static(RECOVERY_GENERATION_KEY),
            Bytes::from(bytes),
            0,
        )
    }
}
