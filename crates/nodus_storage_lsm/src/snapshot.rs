#[cfg(test)]
mod tests;

#[cfg(test)]
fn crash_checkpoint(dir: &std::path::Path, boundary: &str) {
    if std::env::var_os("NODUS_TEST_CHECKPOINT_DIR").as_deref() == Some(dir.as_os_str())
        && std::env::var("NODUS_TEST_CHECKPOINT_BOUNDARY").as_deref() == Ok(boundary)
    {
        std::process::exit(86);
    }
}

use super::*;
use nodus_storage_api::{SnapshotRow, SnapshotScope, snapshot::SNAPSHOT_MEMORY_LIMIT};

impl LsmKvEngine {
    pub(super) fn snapshot_available(&self) -> Result<()> {
        anyhow::ensure!(
            !self.snapshot_failed.load(Ordering::Acquire),
            "snapshot publication outcome uncertain; reopen storage before serving reads or writes"
        );
        Ok(())
    }

    fn collect_snapshot(&self, scope: &SnapshotScope) -> Result<BTreeMap<Bytes, VersionChain>> {
        let mut merged: BTreeMap<Bytes, VersionChain> = BTreeMap::new();
        let mut size = 0usize;
        let mut add = |key: Bytes, chain: VersionChain| -> Result<()> {
            if scope.owns(&key) {
                size = size
                    .checked_add(chain_bytes(&key, &chain) + 64 * chain.versions.len())
                    .ok_or_else(|| anyhow::anyhow!("snapshot size overflow"))?;
                anyhow::ensure!(
                    size <= SNAPSHOT_MEMORY_LIMIT,
                    "snapshot exceeds 128 MiB checkpoint memory limit"
                );
                merged
                    .entry(key)
                    .or_default()
                    .versions
                    .extend(chain.versions);
            }
            Ok(())
        };
        for sst in self.sstables.read().unwrap().iter() {
            for pair in sst.iter()? {
                let (key, chain) = pair?;
                add(key, chain)?;
            }
        }
        for (key, chain) in self.memtable.read().unwrap().iter() {
            add(key.clone(), chain.clone())?;
        }
        for chain in merged.values_mut() {
            chain.versions.sort_by_key(|v| (v.version, v.is_intent));
            chain.versions.dedup();
        }
        Ok(merged)
    }

    pub(super) fn export_snapshot_rows(&self, scope: &SnapshotScope) -> Result<Vec<SnapshotRow>> {
        let _gate = self.snapshot_gate.write().unwrap();
        self.snapshot_available()?;
        Ok(self
            .collect_snapshot(scope)?
            .into_iter()
            .map(|(key, chain)| SnapshotRow {
                key,
                versions: chain.versions.into_iter().map(Into::into).collect(),
            })
            .collect())
    }

    pub(super) fn replace_snapshot_rows(
        &self,
        scope: &SnapshotScope,
        rows: Vec<SnapshotRow>,
        pointers: Vec<SnapshotRow>,
    ) -> Result<()> {
        nodus_storage_api::snapshot::check_snapshot_size(&rows)?;
        nodus_storage_api::snapshot::check_snapshot_size(&pointers)?;
        anyhow::ensure!(
            rows.iter().all(|r| scope.owns(&r.key)),
            "snapshot escaped scope"
        );
        let _gate = self.snapshot_gate.write().unwrap();
        self.snapshot_available()?;
        let _maintenance = self.flush_compact_lock.lock().unwrap();
        let mut merged = self.collect_snapshot(&SnapshotScope::default())?;
        merged.retain(|key, _| !scope.owns(key));
        for row in rows.into_iter().chain(pointers) {
            merged.insert(
                row.key,
                VersionChain {
                    versions: row.versions.into_iter().map(Into::into).collect(),
                },
            );
        }
        let size: usize = merged
            .iter()
            .map(|(key, chain)| chain_bytes(key, chain) + 64 * chain.versions.len())
            .sum();
        anyhow::ensure!(
            size <= SNAPSHOT_MEMORY_LIMIT,
            "replacement exceeds 128 MiB checkpoint memory limit"
        );
        let mut live = HashMap::<TxnId, Vec<Bytes>>::new();
        let mut pending = BTreeMap::new();
        for (key, chain) in &merged {
            if chain.versions.iter().any(|v| v.is_intent) {
                pending.insert(key.clone(), chain.clone());
                for v in chain.versions.iter().filter(|v| v.is_intent) {
                    let txn = v
                        .txn_id
                        .ok_or_else(|| anyhow::anyhow!("snapshot intent has no transaction"))?;
                    live.entry(txn).or_default().push(key.clone());
                }
            }
        }
        let Some(dir) = &self.data_dir else {
            *self.memtable.write().unwrap() = merged;
            *self.intents.write().unwrap() = live;
            self.memtable_bytes.store(size, Ordering::Relaxed);
            return Ok(());
        };
        let allocate = || loop {
            let id = self.next_file_id.fetch_add(1, Ordering::SeqCst);
            if !dir.join(format!("{id}.sst")).exists() && !dir.join(format!("{id}.log")).exists() {
                break id;
            }
        };
        let sst_id = allocate();
        let wal_id = allocate();
        // Existing SSTable and WAL formats: old readers can reopen either side
        // of publication. Intents are checkpointed into a fresh WAL, never
        // stranded in immutable SSTables.
        let mut committed = merged;
        committed.retain(|_, chain| {
            chain.versions.retain(|v| !v.is_intent);
            !chain.versions.is_empty()
        });
        let sst = Sstable::build(dir.join(format!("{sst_id}.sst")), &committed)?;
        let wal = Arc::new(FileWalEngine::with_encryption(
            dir.join(format!("{wal_id}.log")),
            self._wal_key,
        )?);
        // A replacement starts a new lineage: old WAL must not resurrect
        // replaced keys. PITR cannot silently bridge this checkpoint boundary.
        wal.append(WalRecord::V1(WalRecordV1::SegmentHeader {
            predecessor: None,
        }))?;
        for (key, chain) in &pending {
            for v in chain.versions.iter().filter(|v| v.is_intent) {
                let txn_id = v
                    .txn_id
                    .ok_or_else(|| anyhow::anyhow!("intent without transaction"))?;
                let record = match &v.value {
                    Some(value) => WalRecordV1::WriteIntent {
                        txn_id,
                        key: key.to_vec(),
                        value: value.clone(),
                    },
                    None => WalRecordV1::DeleteIntent {
                        txn_id,
                        key: key.to_vec(),
                    },
                };
                wal.append(WalRecord::V1(record))?;
            }
        }
        wal.sync()?;
        std::fs::File::open(dir)?.sync_all()?;
        let manifest = Manifest {
            active_wal: wal_id,
            sstables: vec![sst_id],
            min_replay_wal: wal_id,
        };
        let tmp = dir.join("SNAPSHOT_MANIFEST.tmp");
        std::fs::write(&tmp, serde_json::to_vec(&manifest)?)?;
        std::fs::File::open(&tmp)?.sync_all()?;
        // If rename/fsync has an uncertain result, no old in-memory view may be
        // served. Reopening consults the authoritative manifest.
        #[cfg(test)]
        crash_checkpoint(dir, "before-manifest");
        self.snapshot_failed.store(true, Ordering::Release);
        std::fs::rename(tmp, dir.join(MANIFEST_FILE))?;
        std::fs::File::open(dir)?.sync_all()?;
        #[cfg(test)]
        crash_checkpoint(dir, "after-manifest");
        *self.sstables.write().unwrap() = vec![sst];
        *self.wal.write().unwrap() = Some(wal);
        *self.memtable.write().unwrap() = pending;
        *self.intents.write().unwrap() = live;
        self.active_wal_id.store(wal_id, Ordering::Relaxed);
        self.min_replay_wal.store(wal_id, Ordering::Relaxed);
        self.memtable_bytes.store(size, Ordering::Relaxed);
        self.snapshot_failed.store(false, Ordering::Release);
        // Old files deliberately remain available to existing scan iterators
        // and crash recovery; reclaiming them needs a separate retention policy.
        Ok(())
    }
}
