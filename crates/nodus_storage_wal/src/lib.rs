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

/// On-disk format version of a WAL record payload. Carried in a self-describing
/// envelope so the record encoding can evolve (and a future serializer change)
/// without the serde enum tag being the only version signal — and so an older
/// binary refuses to misreplay a record from a newer format.
const WAL_RECORD_VERSION: u16 = 1;

/// CRC32 (IEEE/reflected) of `data`, used to detect torn or corrupt WAL records
/// on recovery. Bit-by-bit (no table) — WAL records are small, so the cost is
/// negligible and it keeps the crate dependency-free.
fn crc32(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
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
        // Wrap the serialized record in a versioned envelope before framing, so a
        // reader can dispatch on the record format version.
        let mut data =
            nodus_common::versioned::encode(WAL_RECORD_VERSION, &serde_json::to_vec(&record)?);

        if let Some(cipher) = &self.cipher {
            let mut nonce_bytes = [0u8; 12];
            rand::rng().fill_bytes(&mut nonce_bytes);
            let nonce = Nonce::from_slice(&nonce_bytes);
            data = cipher
                .encrypt(nonce, data.as_ref())
                .map_err(|e| anyhow::anyhow!("encryption failed: {}", e))?;
            // Prepend nonce to data
            let mut final_data = nonce_bytes.to_vec();
            final_data.extend_from_slice(&data);
            data = final_data;
        }

        let len = data.len() as u32;
        let crc = crc32(&data);

        // Frame: [len: u32][crc32: u32][payload]. The CRC lets recovery detect a
        // torn or corrupt trailing record and stop cleanly rather than replaying
        // garbage.
        file.write_u32::<LittleEndian>(len)?;
        file.write_u32::<LittleEndian>(crc)?;
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
            // A clean end of log: nothing more to read.
            let len = match reader.read_u32::<LittleEndian>() {
                Ok(len) => len,
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            };

            // From here on, any short read or checksum mismatch means the tail
            // record was torn/corrupted by a crash mid-append. Stop replaying at
            // that point instead of erroring out — every record before it is
            // valid and durable, so the engine still recovers and starts.
            let crc = match reader.read_u32::<LittleEndian>() {
                Ok(crc) => crc,
                Err(_) => break,
            };
            let mut buf = vec![0u8; len as usize];
            if reader.read_exact(&mut buf).is_err() {
                break;
            }
            if crc32(&buf) != crc {
                break;
            }

            let data = if let Some(cipher) = &self.cipher {
                if buf.len() < 12 {
                    break;
                }
                let nonce = Nonce::from_slice(&buf[..12]);
                match cipher.decrypt(nonce, &buf[12..]) {
                    Ok(data) => data,
                    Err(_) => break,
                }
            } else {
                buf
            };

            // Dispatch on the record envelope: parse a supported version or
            // legacy (pre-envelope) bytes; refuse a record from a newer WAL
            // format rather than silently misreplaying it.
            use nodus_common::versioned::{Envelope, decode};
            match decode(&data) {
                Envelope::Versioned { version, payload } if version == WAL_RECORD_VERSION => {
                    if let Ok(record) = serde_json::from_slice(payload) {
                        records.push(record);
                    }
                }
                Envelope::Versioned { version, .. } => {
                    anyhow::bail!(
                        "unsupported WAL record version {version}; this binary supports {WAL_RECORD_VERSION}"
                    );
                }
                Envelope::Legacy(legacy) => {
                    if let Ok(record) = serde_json::from_slice(legacy) {
                        records.push(record);
                    }
                }
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

        let record = WalRecord::V1(WalRecordV1::BeginTxn {
            txn_id: TxnId::new(),
        });
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

    /// Writes one frame (`[len][crc][payload]`) of raw bytes, as a pre-envelope
    /// WAL writer would have.
    fn write_legacy_frame(path: &Path, payload: &[u8]) {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .unwrap();
        file.write_u32::<LittleEndian>(payload.len() as u32)
            .unwrap();
        file.write_u32::<LittleEndian>(crc32(payload)).unwrap();
        file.write_all(payload).unwrap();
        file.sync_data().unwrap();
    }

    #[test]
    fn recovers_legacy_unversioned_records() {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_path_buf();

        // A pre-envelope record: the raw JSON of the record, framed directly.
        let record = WalRecord::V1(WalRecordV1::CommitTxn {
            txn_id: TxnId::new(),
            commit_ts: 42,
        });
        write_legacy_frame(&path, &serde_json::to_vec(&record).unwrap());

        let recovered = FileWalEngine::new(&path).unwrap().recover().unwrap();
        assert_eq!(recovered.len(), 1);
        assert!(matches!(
            recovered[0],
            WalRecord::V1(WalRecordV1::CommitTxn { commit_ts: 42, .. })
        ));
    }

    #[test]
    fn versioned_records_round_trip_unencrypted() {
        let file = NamedTempFile::new().unwrap();
        let engine = FileWalEngine::new(file.path()).unwrap();
        engine
            .append(WalRecord::V1(WalRecordV1::Checkpoint { ts: 99 }))
            .unwrap();
        engine.sync().unwrap();
        let recovered = engine.recover().unwrap();
        assert_eq!(recovered.len(), 1);
        assert!(matches!(
            recovered[0],
            WalRecord::V1(WalRecordV1::Checkpoint { ts: 99 })
        ));
    }

    #[test]
    fn recover_rejects_a_newer_record_version() {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_path_buf();

        // A record from a newer WAL format: a valid frame whose payload is a
        // versioned envelope this binary does not support.
        let future = nodus_common::versioned::encode(WAL_RECORD_VERSION + 1, b"{}");
        write_legacy_frame(&path, &future);

        assert!(FileWalEngine::new(&path).unwrap().recover().is_err());
    }
}
