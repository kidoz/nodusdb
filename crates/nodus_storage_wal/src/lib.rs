use aes_gcm::aead::{Aead, KeyInit};
use aes_gcm::{Aes256Gcm, Key, Nonce};
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use nodus_storage_api::{Timestamp, TxnId};
use rand::Rng;
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
    cipher: Option<Aes256Gcm>,
}

impl FileWalEngine {
    pub fn new<P: AsRef<Path>>(path: P) -> anyhow::Result<Self> {
        Self::with_encryption(path, None)
    }

    pub fn with_encryption<P: AsRef<Path>>(path: P, key: Option<[u8; 32]>) -> anyhow::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;

        let cipher = key.map(|k| Aes256Gcm::new(Key::<Aes256Gcm>::from_slice(&k)));

        Ok(Self {
            file: Arc::new(Mutex::new(file)),
            path: path.as_ref().to_path_buf(),
            cipher,
        })
    }
}

impl WalEngine for FileWalEngine {
    fn append(&self, record: WalRecord) -> anyhow::Result<u64> {
        let mut file = self.file.lock().unwrap();
        let mut data = serde_json::to_vec(&record)?;

        if let Some(cipher) = &self.cipher {
            let mut nonce_bytes = [0u8; 12];
            rand::rng().fill_bytes(&mut nonce_bytes);
            let nonce = Nonce::from_slice(&nonce_bytes);
            data = cipher.encrypt(nonce, data.as_ref()).map_err(|e| anyhow::anyhow!("encryption failed: {}", e))?;
            // Prepend nonce to data
            let mut final_data = nonce_bytes.to_vec();
            final_data.extend_from_slice(&data);
            data = final_data;
        }

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
                    
                    let data = if let Some(cipher) = &self.cipher {
                        if buf.len() < 12 {
                            return Err(anyhow::anyhow!("corrupt WAL record: too short for nonce"));
                        }
                        let nonce = Nonce::from_slice(&buf[..12]);
                        cipher.decrypt(nonce, &buf[12..]).map_err(|e| anyhow::anyhow!("decryption failed: {}", e))?
                    } else {
                        buf
                    };

                    if let Ok(record) = serde_json::from_slice(&data) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_wal_encryption_roundtrip() {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_path_buf();
        
        let key = [42u8; 32];
        let engine = FileWalEngine::with_encryption(&path, Some(key)).unwrap();

        let record = WalRecord::V1(WalRecordV1::BeginTxn { txn_id: TxnId::new() });
        engine.append(record.clone()).unwrap();
        engine.sync().unwrap();

        let recovered = engine.recover().unwrap();
        assert_eq!(recovered.len(), 1);
        if let WalRecord::V1(WalRecordV1::BeginTxn { txn_id }) = &recovered[0] {
            if let WalRecord::V1(WalRecordV1::BeginTxn { txn_id: orig_id }) = record {
                assert_eq!(*txn_id, orig_id);
            }
        } else {
            panic!("Wrong record type recovered");
        }
    }
}
