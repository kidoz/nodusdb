use anyhow::Result;
use bytes::Bytes;
use nodus_mvcc::VersionChain;
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

/// A naive SSTable containing serialized VersionChains sorted by key.
#[derive(Debug)]
pub struct Sstable {
    pub path: PathBuf,
}

impl Sstable {
    pub fn build<P: AsRef<Path>>(
        path: P,
        data: &std::collections::BTreeMap<Bytes, VersionChain>,
    ) -> Result<Self> {
        let path = path.as_ref();
        // Write to a temp sibling, fsync it, then atomically rename into place and
        // fsync the directory. A crash mid-build leaves only the `.tmp` (ignored
        // on recovery) — readers never observe a partial `.sst`.
        let tmp_path = path.with_extension("sst.tmp");
        {
            let mut file = File::create(&tmp_path)?;
            // Simplistic format:
            // [num_entries: u32]
            // repeat: [key_len: u32][key_bytes][value_len: u32][value_bytes (json versionchain)]
            let num_entries = data.len() as u32;
            file.write_all(&num_entries.to_le_bytes())?;

            for (k, v) in data {
                let key_bytes = k.as_ref();
                let key_len = key_bytes.len() as u32;
                file.write_all(&key_len.to_le_bytes())?;
                file.write_all(key_bytes)?;

                let val_bytes = serde_json::to_vec(v)?;
                let val_len = val_bytes.len() as u32;
                file.write_all(&val_len.to_le_bytes())?;
                file.write_all(&val_bytes)?;
            }
            file.sync_all()?;
        }

        std::fs::rename(&tmp_path, path)?;
        if let Some(dir) = path.parent()
            && let Ok(dir_file) = File::open(dir)
        {
            // Make the rename durable: a renamed-but-undirsynced file can vanish
            // on a crash even though its contents were fsynced.
            let _ = dir_file.sync_all();
        }

        Ok(Self {
            path: path.to_path_buf(),
        })
    }

    pub fn open<P: AsRef<Path>>(path: P) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
        }
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<VersionChain>> {
        let mut file = match File::open(&self.path) {
            Ok(f) => f,
            Err(_) => return Ok(None), // Or handle explicitly
        };

        let mut num_entries_bytes = [0u8; 4];
        if file.read_exact(&mut num_entries_bytes).is_err() {
            return Ok(None);
        }
        let num_entries = u32::from_le_bytes(num_entries_bytes);

        for _ in 0..num_entries {
            let mut len_bytes = [0u8; 4];
            file.read_exact(&mut len_bytes)?;
            let key_len = u32::from_le_bytes(len_bytes);

            let mut current_key = vec![0u8; key_len as usize];
            file.read_exact(&mut current_key)?;

            file.read_exact(&mut len_bytes)?;
            let val_len = u32::from_le_bytes(len_bytes);

            if current_key == key {
                let mut val_bytes = vec![0u8; val_len as usize];
                file.read_exact(&mut val_bytes)?;
                let chain: VersionChain = serde_json::from_slice(&val_bytes)?;
                return Ok(Some(chain));
            } else {
                // skip value
                file.seek(SeekFrom::Current(val_len as i64))?;
            }
        }

        Ok(None)
    }

    pub fn iter(&self) -> Result<SstableIterator> {
        let file = File::open(&self.path)?;
        Ok(SstableIterator::new(file))
    }
}

pub struct SstableIterator {
    file: File,
    entries_remaining: u32,
}

impl SstableIterator {
    pub fn new(mut file: File) -> Self {
        let mut num_entries_bytes = [0u8; 4];
        let entries_remaining = if file.read_exact(&mut num_entries_bytes).is_ok() {
            u32::from_le_bytes(num_entries_bytes)
        } else {
            0
        };

        Self {
            file,
            entries_remaining,
        }
    }
}

impl Iterator for SstableIterator {
    type Item = Result<(Bytes, VersionChain)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.entries_remaining == 0 {
            return None;
        }

        let mut len_bytes = [0u8; 4];
        if let Err(e) = self.file.read_exact(&mut len_bytes) {
            return Some(Err(e.into()));
        }
        let key_len = u32::from_le_bytes(len_bytes);

        let mut key = vec![0u8; key_len as usize];
        if let Err(e) = self.file.read_exact(&mut key) {
            return Some(Err(e.into()));
        }

        if let Err(e) = self.file.read_exact(&mut len_bytes) {
            return Some(Err(e.into()));
        }
        let val_len = u32::from_le_bytes(len_bytes);

        let mut val_bytes = vec![0u8; val_len as usize];
        if let Err(e) = self.file.read_exact(&mut val_bytes) {
            return Some(Err(e.into()));
        }

        let chain: VersionChain = match serde_json::from_slice(&val_bytes) {
            Ok(c) => c,
            Err(e) => return Some(Err(e.into())),
        };

        self.entries_remaining -= 1;
        Some(Ok((Bytes::from(key), chain)))
    }
}
