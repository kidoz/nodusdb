use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use nodus_storage_api::{Timestamp, TxnId};
use serde::{Deserialize, Serialize};
use std::fs::{File, OpenOptions};
use std::io::{BufReader, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalRecordV1 {
    BeginTxn {
        txn_id: TxnId,
    },
    WriteIntent {
        txn_id: TxnId,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    CommitTxn {
        txn_id: TxnId,
        commit_ts: Timestamp,
    },
    AbortTxn {
        txn_id: TxnId,
    },
    DeleteIntent {
        txn_id: TxnId,
        key: Vec<u8>,
    },
    Checkpoint {
        ts: Timestamp,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum WalRecord {
    V1(WalRecordV1),
}

pub trait WalEngine: Send + Sync {
    fn append(&self, record: WalRecord) -> anyhow::Result<u64>; // returns LSN
    fn sync(&self) -> anyhow::Result<()>;
    fn recover(&self) -> anyhow::Result<Vec<WalRecord>>;
}

pub struct FileWalEngine {
    file: Arc<Mutex<File>>,
    path: PathBuf,
}

impl FileWalEngine {
    pub fn new<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;

        Ok(Self {
            file: Arc::new(Mutex::new(file)),
            path: path.as_ref().to_path_buf(),
        })
    }
}

impl WalEngine for FileWalEngine {
    fn append(&self, record: WalRecord) -> anyhow::Result<u64> {
        let mut file = self.file.lock().unwrap();
        let data = serde_json::to_vec(&record)?;
        let len = data.len() as u32;

        // Write length prefix then data
        file.write_u32::<LittleEndian>(len)?;
        file.write_all(&data)?;

        // Return a dummy LSN for MVP
        Ok(0)
    }

    fn sync(&self) -> anyhow::Result<()> {
        let file = self.file.lock().unwrap();
        file.sync_data()?;
        Ok(())
    }

    fn recover(&self) -> anyhow::Result<Vec<WalRecord>> {
        let file = File::open(&self.path)?;
        let mut reader = BufReader::new(file);
        let mut records = Vec::new();

        loop {
            match reader.read_u32::<LittleEndian>() {
                Ok(len) => {
                    let mut buf = vec![0u8; len as usize];
                    reader.read_exact(&mut buf)?;
                    if let Ok(record) = serde_json::from_slice(&buf) {
                        records.push(record);
                    }
                }
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                    break;
                }
                Err(e) => return Err(e.into()),
            }
        }

        Ok(records)
    }
}
