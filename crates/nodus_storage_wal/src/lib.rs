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
    /// First record of a rotated WAL segment, naming the segment it follows.
    /// This makes the WAL-segment lineage explicit so PITR can verify an
    /// unbroken chain of archived segments even though segment ids are sparse
    /// (the file-id counter is shared with SSTables/compaction). `predecessor`
    /// is `None` for the very first segment.
    SegmentHeader {
        predecessor: Option<u64>,
    },
    BeginTxn {
        txn_id: TxnId,
    },
    WriteIntent {
        txn_id: TxnId,
        key: Vec<u8>,
        value: Vec<u8>,
        /// Write time (HLC-scale wall-clock µs), so an intent's age survives
        /// crash replay for the abandoned-intent janitor. Absent in records
        /// written before this field existed; replay stamps those at replay
        /// time (they read as young — the safe direction).
        #[serde(default)]
        started_at: Option<Timestamp>,
    },
    CommitTxn {
        txn_id: TxnId,
        commit_ts: Timestamp,
    },
    CommitTxnBatchV1 {
        commits: Vec<(TxnId, Timestamp)>,
    },
    AbortTxn {
        txn_id: TxnId,
    },
    DeleteIntent {
        txn_id: TxnId,
        key: Vec<u8>,
        /// See `WriteIntent::started_at`.
        #[serde(default)]
        started_at: Option<Timestamp>,
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
    /// Replays supported records. File-backed recovery may repair a torn tail,
    /// but authentication and record-decoding errors must preserve the file.
    /// Run recovery before admitting concurrent appenders.
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

        let cipher = key.map(|k| Aes256Gcm::new(&Key::<Aes256Gcm>::from(k)));

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
            let nonce = Nonce::from(nonce_bytes);
            data = cipher
                .encrypt(&nonce, data.as_ref())
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
        let file_len = file.metadata()?.len();
        let mut reader = BufReader::new(file);
        let mut records = Vec::new();
        // Offset just past the last fully-valid record. Only a torn frame may
        // reach tail repair; complete-frame authentication/decode errors return
        // without altering the file (a wrong key is not crash residue).
        let mut valid_end: u64 = 0;

        while valid_end < file_len {
            let frame_error = |message: String| {
                anyhow::anyhow!(
                    "WAL {} at byte offset {valid_end}: {message}",
                    self.path.display()
                )
            };
            // A short header/payload is an incomplete trailing append. Check
            // available bytes before allocating from an untrusted length.
            if file_len - valid_end < 8 {
                break;
            }
            let len = reader
                .read_u32::<LittleEndian>()
                .map_err(|e| frame_error(format!("failed to read frame length: {e}")))?;
            let crc = reader
                .read_u32::<LittleEndian>()
                .map_err(|e| frame_error(format!("failed to read frame checksum: {e}")))?;
            if u64::from(len) > file_len - valid_end - 8 {
                break;
            }
            let mut buf = vec![0u8; len as usize];
            reader
                .read_exact(&mut buf)
                .map_err(|e| frame_error(format!("failed to read frame payload: {e}")))?;
            // Retain the existing checksum-based torn-write repair policy.
            if crc32(&buf) != crc {
                break;
            }

            let data = if let Some(cipher) = &self.cipher {
                // AES-GCM requires a 12-byte nonce and a 16-byte tag even for
                // empty plaintext. A complete CRC-valid frame shorter than that
                // is invalid, not evidence of an incomplete append.
                if buf.len() < 28 {
                    return Err(frame_error(
                        "invalid encrypted WAL payload: expected a nonce and authentication tag; check WAL encryption configuration".into(),
                    ));
                }
                let nonce = <&Nonce<_>>::try_from(&buf[..12])
                    .map_err(|e| frame_error(format!("invalid WAL nonce: {e}")))?;
                cipher.decrypt(nonce, &buf[12..]).map_err(|_| {
                    frame_error(
                        "WAL authentication failed: check the encryption key/configuration and file integrity".into(),
                    )
                })?
            } else {
                buf
            };

            // Dispatch on the record envelope: parse a supported version or
            // legacy (pre-envelope) bytes; refuse a record from a newer WAL
            // format rather than silently misreplaying it.
            use nodus_common::versioned::{Envelope, decode};
            let record = match decode(&data) {
                Envelope::Versioned { version, payload } if version == WAL_RECORD_VERSION => {
                    serde_json::from_slice(payload).map_err(|error| {
                        frame_error(format!("failed to decode WAL record version {version}: {error}"))
                    })?
                }
                Envelope::Versioned { version, .. } => {
                    return Err(frame_error(format!(
                        "unsupported WAL record version {version}; this binary supports {WAL_RECORD_VERSION}"
                    )));
                }
                Envelope::Legacy(legacy) => serde_json::from_slice(legacy).map_err(|error| {
                    frame_error(format!(
                        "failed to decode legacy WAL record: {error}; check WAL encryption configuration and file integrity"
                    ))
                })?,
            };
            records.push(record);
            valid_end += 8 + u64::from(len);
        }

        // Only incomplete or checksum-failing frames reach this repair path.
        // Truncate the torn tail so later appends remain replayable. Errors in
        // complete CRC-valid frames above leave every byte intact.
        if valid_end < file_len {
            let file = OpenOptions::new().write(true).open(&self.path)?;
            file.set_len(valid_end)?;
            file.sync_data()?;
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

    #[test]
    fn recovers_encrypted_record_from_aes_gcm_0_10() {
        // Generated with aes-gcm 0.10.3: key [42; 32], nonce [7; 12],
        // plaintext NDBv + version 1 (u16 LE) + {"V1":{"Checkpoint":{"ts":99}}}.
        // Fixed old-codec bytes catch compatibility breaks that a round trip
        // using only the new codec would miss. Payload is nonce + ciphertext + tag.
        let payload = [
            7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 7, 113, 187, 189, 211, 84, 148, 94, 19, 175, 123, 82,
            154, 241, 222, 244, 83, 95, 229, 6, 3, 241, 84, 14, 65, 136, 0, 249, 181, 40, 79, 32,
            40, 199, 149, 137, 102, 3, 178, 179, 112, 225, 52, 220, 106, 18, 36, 106, 44, 192, 233,
            88, 47, 152,
        ];
        let file = NamedTempFile::new().unwrap();
        write_legacy_frame(file.path(), &payload);

        let engine = FileWalEngine::with_encryption(file.path(), Some([42; 32])).unwrap();
        let recovered = engine.recover().unwrap();
        assert_eq!(recovered.len(), 1);
        assert!(matches!(
            recovered[0],
            WalRecord::V1(WalRecordV1::Checkpoint { ts: 99 })
        ));
    }

    #[test]
    fn incorrect_encryption_configuration_preserves_wal_bytes() {
        for (written_key, recovery_key) in [
            (Some([42; 32]), Some([7; 32])),
            (Some([42; 32]), None),
            (None, Some([42; 32])),
        ] {
            let file = NamedTempFile::new().unwrap();
            {
                let writer = FileWalEngine::with_encryption(file.path(), written_key).unwrap();
                writer.append(checkpoint(1)).unwrap();
                writer.append(checkpoint(2)).unwrap();
                writer.sync().unwrap();
            }
            let original = std::fs::read(file.path()).unwrap();
            let wrong = FileWalEngine::with_encryption(file.path(), recovery_key).unwrap();
            let result = wrong.recover();
            assert_eq!(std::fs::read(file.path()).unwrap(), original);
            let error = result.expect_err("incorrect encryption configuration must fail");
            assert!(
                error
                    .to_string()
                    .contains(&file.path().display().to_string())
            );
            assert!(error.to_string().contains("byte offset 0"));
            drop(wrong);

            let correct = FileWalEngine::with_encryption(file.path(), written_key).unwrap();
            assert_eq!(checkpoint_ts(&correct.recover().unwrap()), vec![1, 2]);
            correct.append(checkpoint(3)).unwrap();
            correct.sync().unwrap();
            assert_eq!(checkpoint_ts(&correct.recover().unwrap()), vec![1, 2, 3]);
        }
    }

    #[test]
    fn authentication_failure_after_valid_prefix_preserves_entire_wal() {
        let file = NamedTempFile::new().unwrap();
        let engine = FileWalEngine::with_encryption(file.path(), Some([42; 32])).unwrap();
        engine.append(checkpoint(1)).unwrap();
        engine.sync().unwrap();
        let offset = std::fs::metadata(file.path()).unwrap().len();
        let other = FileWalEngine::with_encryption(file.path(), Some([7; 32])).unwrap();
        other.append(checkpoint(2)).unwrap();
        other.sync().unwrap();
        engine.append(checkpoint(3)).unwrap();
        engine.sync().unwrap();
        let original = std::fs::read(file.path()).unwrap();

        let result = engine.recover();
        assert_eq!(std::fs::read(file.path()).unwrap(), original);
        let message = result.unwrap_err().to_string();
        assert!(message.contains("authentication failed"), "{message}");
        assert!(
            message.contains(&format!("byte offset {offset}")),
            "{message}"
        );
    }

    #[test]
    fn complete_frames_with_short_encrypted_payloads_are_not_truncated() {
        for len in [0, 11, 12, 27] {
            let file = NamedTempFile::new().unwrap();
            write_legacy_frame(file.path(), &vec![0; len]);
            let original = std::fs::read(file.path()).unwrap();
            let engine = FileWalEngine::with_encryption(file.path(), Some([42; 32])).unwrap();
            let result = engine.recover();
            assert_eq!(std::fs::read(file.path()).unwrap(), original);
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("encrypted WAL payload")
            );
        }
    }

    #[test]
    fn undecodable_legacy_frames_are_not_skipped_or_truncated() {
        for payload in [
            b"{}".as_slice(),
            b"not JSON",
            br#"{"V1":{"UnknownRecord":{}}}"#,
            b"NDBv\x01",
        ] {
            let file = NamedTempFile::new().unwrap();
            let engine = FileWalEngine::new(file.path()).unwrap();
            engine.append(checkpoint(1)).unwrap();
            engine.sync().unwrap();
            write_legacy_frame(file.path(), payload);
            engine.append(checkpoint(2)).unwrap();
            engine.sync().unwrap();
            let original = std::fs::read(file.path()).unwrap();

            let result = engine.recover();
            assert_eq!(std::fs::read(file.path()).unwrap(), original);
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .contains("failed to decode legacy WAL record")
            );
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

    #[test]
    fn recover_rejects_undecodable_supported_version_payloads() {
        for payload in [b"{}".as_slice(), br#"{"V1":{"UnknownRecord":{}}}"#] {
            let file = NamedTempFile::new().unwrap();
            let path = file.path().to_path_buf();
            let engine = FileWalEngine::new(&path).unwrap();
            engine.append(checkpoint(1)).unwrap();
            engine.sync().unwrap();
            let data = nodus_common::versioned::encode(WAL_RECORD_VERSION, payload);
            write_legacy_frame(&path, &data);
            engine.append(checkpoint(2)).unwrap();
            engine.sync().unwrap();

            let original = std::fs::read(&path).unwrap();
            let error = engine.recover().unwrap_err();
            assert_eq!(std::fs::read(&path).unwrap(), original);
            assert!(
                error
                    .to_string()
                    .contains("failed to decode WAL record version 1")
            );
        }
    }

    // -----------------------------------------------------------------------
    // Simulated power loss. A real power cut preserves exactly what fsync
    // returned over, may keep any prefix of the unsynced tail, and may leave
    // the final write torn. These tests reproduce those on-disk states by
    // truncating the segment file at every byte boundary of the unsynced
    // region, then running the real `recover` over the result.
    // -----------------------------------------------------------------------

    fn checkpoint(ts: Timestamp) -> WalRecord {
        WalRecord::V1(WalRecordV1::Checkpoint { ts })
    }

    /// The `ts` of every recovered `Checkpoint` record, for equality assertions
    /// (the record enums do not derive `PartialEq`).
    fn checkpoint_ts(records: &[WalRecord]) -> Vec<Timestamp> {
        records
            .iter()
            .map(|r| match r {
                WalRecord::V1(WalRecordV1::Checkpoint { ts }) => *ts,
                other => panic!("unexpected record: {other:?}"),
            })
            .collect()
    }

    /// Rewrites `path` to the crash image: the first `keep` bytes of `image`,
    /// plus `garbage` bytes of torn-write residue.
    fn apply_power_cut(path: &Path, image: &[u8], keep: usize, garbage: usize) {
        let mut crashed = image[..keep].to_vec();
        crashed.extend(std::iter::repeat_n(0xA5u8, garbage));
        std::fs::write(path, &crashed).unwrap();
        File::open(path).unwrap().sync_all().unwrap();
    }

    #[test]
    fn power_cut_preserves_exactly_the_synced_prefix() {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_path_buf();
        let synced_len;
        let image;
        {
            let engine = FileWalEngine::new(&path).unwrap();
            engine.append(checkpoint(1)).unwrap();
            engine.append(checkpoint(2)).unwrap();
            engine.sync().unwrap();
            // Everything the fsync returned over is durable; anything appended
            // afterwards is the unsynced tail a power cut may drop or tear.
            synced_len = std::fs::metadata(&path).unwrap().len() as usize;
            engine.append(checkpoint(3)).unwrap();
            image = std::fs::read(&path).unwrap();
            assert!(synced_len < image.len());
        }

        // A power cut may preserve any prefix of the unsynced tail: cut at
        // every byte boundary of that region.
        for keep in synced_len..=image.len() {
            apply_power_cut(&path, &image, keep, 0);
            let recovered = FileWalEngine::new(&path).unwrap().recover().unwrap();
            let expected = if keep == image.len() {
                vec![1, 2, 3]
            } else {
                vec![1, 2]
            };
            assert_eq!(
                checkpoint_ts(&recovered),
                expected,
                "cut at byte {keep} of {}",
                image.len()
            );
        }
    }

    #[test]
    fn power_cut_with_garbled_tail_recovers_the_synced_prefix() {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_path_buf();
        let synced_len;
        let image;
        {
            let engine = FileWalEngine::new(&path).unwrap();
            engine.append(checkpoint(1)).unwrap();
            engine.sync().unwrap();
            synced_len = std::fs::metadata(&path).unwrap().len() as usize;
            engine.append(checkpoint(2)).unwrap();
            image = std::fs::read(&path).unwrap();
        }

        // A torn write can also leave garbage behind (a partial sector), not
        // just a short file: a few residue bytes past any cut point must be
        // rejected by the frame CRC, never replayed.
        for keep in (synced_len..image.len()).step_by(7) {
            apply_power_cut(&path, &image, keep, 11);
            let recovered = FileWalEngine::new(&path).unwrap().recover().unwrap();
            assert_eq!(checkpoint_ts(&recovered), vec![1], "cut at byte {keep}");
        }
        // A bit flip inside a fully-present but never-synced frame: the record
        // is corrupt, so it is dropped with the rest of the tail.
        let mut flipped = image.clone();
        flipped[synced_len + 9] ^= 0xFF;
        std::fs::write(&path, &flipped).unwrap();
        let recovered = FileWalEngine::new(&path).unwrap().recover().unwrap();
        assert_eq!(checkpoint_ts(&recovered), vec![1]);
    }

    #[test]
    fn recover_truncates_a_torn_tail_so_later_appends_stay_replayable() {
        let file = NamedTempFile::new().unwrap();
        let path = file.path().to_path_buf();
        let engine = FileWalEngine::new(&path).unwrap();
        engine.append(checkpoint(1)).unwrap();
        engine.sync().unwrap();
        let valid_len = std::fs::metadata(&path).unwrap().len();

        // Crash mid-append: a torn frame claims 1000 payload bytes and stops.
        {
            let mut f = OpenOptions::new().append(true).open(&path).unwrap();
            f.write_u32::<LittleEndian>(1000).unwrap();
            f.write_all(&[1, 2, 3, 4]).unwrap();
            f.sync_all().unwrap();
        }

        // Recovery replays the valid prefix and truncates the tear, so the
        // segment can keep serving as the active log.
        assert_eq!(checkpoint_ts(&engine.recover().unwrap()), vec![1]);
        assert_eq!(
            std::fs::metadata(&path).unwrap().len(),
            valid_len,
            "torn tail must be truncated"
        );

        // A record appended after the tear must survive the next recovery —
        // without truncation it would sit behind undecodable bytes and every
        // later replay would stop at the same tear.
        engine.append(checkpoint(2)).unwrap();
        engine.sync().unwrap();
        drop(engine);
        let recovered = FileWalEngine::new(&path).unwrap().recover().unwrap();
        assert_eq!(checkpoint_ts(&recovered), vec![1, 2]);
    }

    #[test]
    fn encrypted_incomplete_tail_is_repaired_before_later_appends() {
        let file = NamedTempFile::new().unwrap();
        let key = Some([42; 32]);
        let synced_len;
        let image;
        {
            let engine = FileWalEngine::with_encryption(file.path(), key).unwrap();
            engine.append(checkpoint(1)).unwrap();
            engine.sync().unwrap();
            synced_len = std::fs::metadata(file.path()).unwrap().len() as usize;
            engine.append(checkpoint(2)).unwrap();
            image = std::fs::read(file.path()).unwrap();
        }
        for keep in synced_len..image.len() {
            apply_power_cut(file.path(), &image, keep, 0);
            let engine = FileWalEngine::with_encryption(file.path(), key).unwrap();
            assert_eq!(checkpoint_ts(&engine.recover().unwrap()), vec![1]);
            assert_eq!(std::fs::read(file.path()).unwrap(), image[..synced_len]);
            engine.append(checkpoint(3)).unwrap();
            engine.sync().unwrap();
            drop(engine);
            let reopened = FileWalEngine::with_encryption(file.path(), key).unwrap();
            assert_eq!(checkpoint_ts(&reopened.recover().unwrap()), vec![1, 3]);
        }
    }
}
