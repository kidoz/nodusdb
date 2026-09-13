//! Legacy and gated MVCC snapshots with strict validation and atomic install.
#[cfg(test)]
mod tests;
pub(crate) mod v2;

use super::*;
use nodus_storage_api::{SnapshotRow, SnapshotScope, snapshot::SNAPSHOT_MEMORY_LIMIT};

const CATALOG_KEY: &[u8] = b"\0catalog\0state";

// Remove abandoned private files on every return path. Durable publication
// renames the file, so cleanup after success is a harmless missing-file removal.
struct StagedSnapshot(PathBuf);
impl Drop for StagedSnapshot {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn scope(sm: &StateMachine) -> SnapshotScope {
    SnapshotScope {
        exclude_raft: true,
        exclude_data_groups: sm.meta_store.is_some() || sm.catalog_reader.is_some(),
        ..Default::default()
    }
}

fn internal(key: &[u8]) -> bool {
    key.starts_with(b"\0")
        || key.starts_with(b"meta:")
        || migration::is_control_key(key)
        || key.starts_with(b"\x01upgrade/")
}

fn sync_dir(path: &std::path::Path) -> anyhow::Result<()> {
    std::fs::File::open(path)?.sync_all()?;
    Ok(())
}

fn validate_control(kv: &dyn KvEngine, row: &SnapshotRow) -> anyhow::Result<()> {
    if migration::is_control_key(&row.key)
        || row.key.as_ref() == CATALOG_KEY
        || row.key.starts_with(b"\x01upgrade/")
    {
        anyhow::ensure!(
            !row.versions.iter().any(|v| v.is_intent),
            "snapshot contains unfinished control/catalog state"
        );
        let latest = row
            .versions
            .iter()
            .max_by_key(|v| v.version)
            .ok_or_else(|| anyhow::anyhow!("empty control snapshot row"))?;
        let bytes = latest
            .value
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("deleted control/catalog snapshot state"))?;
        if row.key.as_ref() == upgrade::KEY {
            upgrade::validate_snapshot(kv, bytes, None)?;
        } else if row.key.as_ref() == upgrade::admission::KEY {
            upgrade::admission::validate_snapshot(kv, bytes)?;
        } else {
            anyhow::ensure!(
                !row.key.starts_with(b"\x01upgrade/"),
                "unknown upgrade snapshot record"
            );
            migration::validate_snapshot_record(kv, &row.key, bytes, latest.version)?;
        }
    }
    Ok(())
}

impl NodusRaftStore {
    /// Invalidate the descriptor before touching `current.snap`. A crash in the
    /// publication gap leaves no servable snapshot, never a mismatched file and
    /// descriptor. The complete KV state can rebuild it, including old binaries.
    fn invalidate_snapshot_file(&self) -> anyhow::Result<()> {
        if let Some(meta) = &self.meta {
            meta.delete_checked(RAFT_SNAPSHOT_META_KEY)?;
        }
        match std::fs::remove_file(self.snapshot_dir.join(CURRENT_SNAPSHOT_FILE)) {
            Ok(()) => sync_dir(&self.snapshot_dir)?,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
            Err(e) => return Err(e.into()),
        }
        Ok(())
    }

    pub(super) async fn build_atomic_snapshot(&self) -> anyhow::Result<Snapshot<NodusTypeConfig>> {
        let sm = self.state_machine.read().await;
        let scope = scope(&sm);
        let mut rows = if let Some(kv) = &sm.kv {
            kv.snapshot_rows(&scope)?
        } else {
            Vec::new()
        };
        // An aborted intent can leave an empty in-memory chain. It carries no
        // state and is omitted, while wire readers still reject empty chains.
        rows.retain(|row| !row.versions.is_empty());
        let use_v2 = v2::permitted(self, &sm)?;
        if !use_v2 {
            anyhow::ensure!(
                !rows.iter().flat_map(|r| &r.versions).any(|v| v.is_intent),
                "legacy snapshot cannot represent pending intents; finalized MVCC snapshot support is required"
            );
        }
        anyhow::ensure!(
            !rows
                .iter()
                .filter(|r| r.key.as_ref() == CATALOG_KEY
                    || migration::is_control_key(&r.key)
                    || r.key.starts_with(b"\x01upgrade/"))
                .flat_map(|r| &r.versions)
                .any(|v| v.is_intent),
            "snapshot contains an unfinished catalog/control mutation"
        );
        let catalog = sm.catalog_reader.as_ref().map(|c| c.export_snapshot());
        if let Some(catalog) = &sm.catalog_reader {
            let durable = catalog.export_raft_catalog()?;
            let version = rows
                .iter()
                .find(|r| r.key.as_ref() == CATALOG_KEY)
                .and_then(|r| r.versions.iter().map(|v| v.version).max())
                .unwrap_or(sm.last_applied_log.map_or(0, |l| l.index));
            rows.retain(|r| r.key.as_ref() != CATALOG_KEY);
            rows.push(SnapshotRow::committed(
                Bytes::from_static(CATALOG_KEY),
                Bytes::from(durable),
                version,
            ));
        }
        rows.sort_by(|a, b| a.key.cmp(&b.key));
        nodus_storage_api::snapshot::check_snapshot_size(&rows)?;
        // V1 cannot encode intents, tombstones, or retained user history. Refuse
        // before publishing/purging a log whose effects it cannot represent.
        for row in &rows {
            if use_v2 {
                continue;
            }
            anyhow::ensure!(
                !row.versions.iter().any(|v| v.is_intent),
                "legacy snapshot cannot represent pending intents; drain transactions first"
            );
            if !internal(&row.key) {
                anyhow::ensure!(
                    row.versions.len() <= 1 && row.versions.iter().all(|v| v.value.is_some()),
                    "legacy snapshot cannot represent retained MVCC history; a negotiated snapshot format is required"
                );
            }
        }
        let meta = SnapshotMeta {
            last_log_id: sm.last_applied_log,
            last_membership: sm.last_membership.clone(),
            snapshot_id: format!("snapshot-{}", uuid::Uuid::new_v4()),
        };
        let tmp = self
            .snapshot_dir
            .join(format!("build-{}.tmp", uuid::Uuid::new_v4()));
        let _cleanup = StagedSnapshot(tmp.clone());
        let file = tokio::fs::File::create(&tmp).await?;
        let mut writer = BufWriter::new(file);
        for row in &rows {
            if let Some(kv) = &sm.kv {
                validate_control(kv.as_ref(), row)?;
            }
        }
        if use_v2 {
            v2::canonicalize(&mut rows)?;
            writer
                .write_all(&v2::encode(
                    &self.snapshot_group,
                    &meta,
                    catalog.is_some(),
                    &rows,
                )?)
                .await?;
        } else {
            let catalog_bytes = catalog.map(|c| serde_json::to_vec(&c)).transpose()?;
            write_snapshot_header(&mut writer, catalog_bytes.as_deref()).await?;
            for row in rows {
                if let Some(v) = row.versions.iter().max_by_key(|v| v.version)
                    && let Some(value) = &v.value
                {
                    write_kv_record(&mut writer, &row.key, value, v.version).await?;
                }
            }
        }
        writer.flush().await?;
        writer.into_inner().sync_all().await?;
        let mut current = self.current_snapshot_meta.write().await;
        *current = None;
        self.invalidate_snapshot_file()?;
        std::fs::rename(&tmp, self.snapshot_dir.join(CURRENT_SNAPSHOT_FILE))?;
        sync_dir(&self.snapshot_dir)?;
        if let Some(store) = &self.meta {
            store.put_checked(
                RAFT_SNAPSHOT_META_KEY,
                encode_raft(serde_json::to_vec(&meta)?),
            )?;
        }
        *current = Some(meta.clone());
        tracing::info!(snapshot = %meta.snapshot_id, format_version = if use_v2 { 2 } else { 1 }, "validated snapshot published");
        Ok(Snapshot {
            meta,
            snapshot: Box::new(
                self.open_current_snapshot()
                    .await?
                    .ok_or_else(|| anyhow::anyhow!("published snapshot disappeared"))?,
            ),
        })
    }

    pub(super) async fn install_atomic_snapshot(
        &self,
        meta: &NodusSnapshotMeta,
        mut file: tokio::fs::File,
    ) -> anyhow::Result<()> {
        // Copy to a private file first; the received stream may be reused by a
        // later transfer. No row/catalog mutation occurs during parsing.
        file.seek(SeekFrom::Start(0)).await?;
        let tmp = self
            .snapshot_dir
            .join(format!("install-{}.tmp", uuid::Uuid::new_v4()));
        let _cleanup = StagedSnapshot(tmp.clone());
        let mut staged = tokio::fs::File::create(&tmp).await?;
        anyhow::ensure!(
            tokio::io::copy(
                &mut file.take(SNAPSHOT_MEMORY_LIMIT as u64 + 1),
                &mut staged
            )
            .await?
                <= SNAPSHOT_MEMORY_LIMIT as u64,
            "snapshot exceeds 128 MiB input limit"
        );
        staged.sync_all().await?;
        let mut reader = BufReader::new(tokio::fs::File::open(&tmp).await?);
        let mut prefix = [0; 6];
        reader.read_exact(&mut prefix).await?;
        reader.seek(SeekFrom::Start(0)).await?;
        let use_v2 = &prefix == b"NSNP\0\x02";
        let (catalog_header, incoming) = if use_v2 {
            let mut bytes = Vec::new();
            reader.read_to_end(&mut bytes).await?;
            let (catalog, rows) = v2::decode(&bytes, &self.snapshot_group, meta)?;
            (catalog.then(nodus_catalog::CatalogSnapshot::default), rows)
        } else {
            let catalog_bytes = read_snapshot_catalog(&mut reader).await?;
            let catalog = catalog_bytes
                .as_deref()
                .map(serde_json::from_slice)
                .transpose()?;
            let mut rows = Vec::new();
            let mut size = 0usize;
            while let Some((key, value, version)) = read_kv_record(&mut reader).await? {
                let row = SnapshotRow::committed(Bytes::from(key), Bytes::from(value), version);
                size = size.saturating_add(row.memory_size());
                anyhow::ensure!(
                    size <= SNAPSHOT_MEMORY_LIMIT,
                    "snapshot exceeds checkpoint memory limit"
                );
                rows.push(row);
            }
            (catalog, rows)
        };
        let mut sm = self.state_machine.write().await;
        anyhow::ensure!(
            meta.last_log_id >= sm.last_applied_log
                && meta.last_log_id.map(|l| l.index) >= sm.last_applied_log.map(|l| l.index),
            "snapshot is older than applied state"
        );
        anyhow::ensure!(
            catalog_header.is_some() == sm.catalog_writer.is_some(),
            "snapshot catalog/group kind mismatch"
        );
        let scope = scope(&sm);
        let mut rows = BTreeMap::<Bytes, SnapshotRow>::new();
        let mut last: Option<Bytes> = None;
        for row in incoming {
            anyhow::ensure!(
                scope.owns(&row.key),
                "snapshot contains another group's or node-local state"
            );
            anyhow::ensure!(
                last.as_ref().is_none_or(|last| last < &row.key),
                "snapshot keys are duplicated or out of order"
            );
            last = Some(row.key.clone());
            if let Some(kv) = &sm.kv {
                validate_control(kv.as_ref(), &row)?;
            }
            rows.insert(row.key.clone(), row);
        }
        if use_v2 && catalog_header.is_some() {
            anyhow::ensure!(
                rows.contains_key(CATALOG_KEY),
                "MVCC snapshot is missing durable catalog"
            );
        }
        let kv = sm
            .kv
            .clone()
            .ok_or_else(|| anyhow::anyhow!("snapshot requires storage"))?;
        for row in kv.snapshot_rows(&scope)? {
            anyhow::ensure!(
                !(migration::is_control_key(&row.key) || row.key.starts_with(b"\x01upgrade/"))
                    || rows.contains_key(&row.key),
                "snapshot missing migration or upgrade state"
            );
        }
        if let Some(row) = rows.get(upgrade::KEY)
            && let Some(bytes) = row
                .versions
                .iter()
                .max_by_key(|v| v.version)
                .and_then(|v| v.value.as_ref())
        {
            upgrade::validate_snapshot(kv.as_ref(), bytes, Some(meta))?;
        }
        if let Some(row) = rows.get(upgrade::admission::KEY) {
            let latest = row
                .versions
                .iter()
                .max_by_key(|v| v.version)
                .and_then(|v| v.value.as_ref())
                .ok_or_else(|| anyhow::anyhow!("missing admission state"))?;
            let ledger = upgrade::admission::decode(latest)?;
            let base = rows
                .get(upgrade::KEY)
                .and_then(|r| r.versions.iter().max_by_key(|v| v.version))
                .and_then(|v| v.value.as_ref())
                .ok_or_else(|| anyhow::anyhow!("admission snapshot lacks upgrade authority"))?;
            let authority = upgrade::decode(base)?;
            upgrade::admission::approved(&authority, Some(&ledger))?;
            anyhow::ensure!(
                meta.last_log_id.is_some_and(|l| l.index >= ledger.revision),
                "admission revision exceeds snapshot applied pointer"
            );
            if ledger.pending.is_some() {
                upgrade::admission::validate_progress(&ledger, &meta.last_membership)?;
            } else {
                upgrade::admission::validate_stable(
                    &authority,
                    Some(&ledger),
                    &meta.last_membership,
                )?;
            }
        }
        let durable_catalog =
            if let (Some(cat), Some(header)) = (&sm.catalog_writer, catalog_header) {
                match rows
                    .get(CATALOG_KEY)
                    .and_then(|r| {
                        r.versions
                            .iter()
                            .filter(|v| !v.is_intent)
                            .max_by_key(|v| v.version)
                    })
                    .and_then(|v| v.value.clone())
                {
                    Some(bytes) => Some(bytes),
                    None => {
                        let bytes = cat.prepare_legacy_raft_catalog(header)?;
                        rows.insert(
                            Bytes::from_static(CATALOG_KEY),
                            SnapshotRow::committed(
                                Bytes::from_static(CATALOG_KEY),
                                Bytes::from(bytes.clone()),
                                meta.last_log_id.map_or(0, |l| l.index),
                            ),
                        );
                        Some(bytes)
                    }
                }
            } else {
                None
            };
        let applied = AppliedState {
            last_applied: meta.last_log_id,
            last_membership: meta.last_membership.clone(),
            last_purged: sm.last_purged,
        };
        let ts = self.meta.as_ref().map_or(0, |store| store.next_ts());
        let pointers = vec![
            SnapshotRow::committed(
                Bytes::from_static(RAFT_APPLIED_KEY),
                Bytes::from(encode_raft(serde_json::to_vec(&applied)?)),
                ts,
            ),
            SnapshotRow::committed(
                Bytes::from_static(RAFT_SNAPSHOT_META_KEY),
                Bytes::from(encode_raft(serde_json::to_vec(meta)?)),
                ts,
            ),
        ];
        let mut batch = Some((rows.into_values().collect(), pointers));
        let mut current = self.current_snapshot_meta.write().await;
        let mut publish = || -> anyhow::Result<()> {
            *current = None;
            self.invalidate_snapshot_file()?;
            let (rows, pointers) = batch
                .take()
                .ok_or_else(|| anyhow::anyhow!("snapshot callback invoked twice"))?;
            kv.replace_snapshot(&scope, rows, pointers)
        };
        if let (Some(cat), Some(bytes)) = (&sm.catalog_writer, durable_catalog) {
            cat.install_raft_catalog(&bytes, &mut publish)?;
        } else {
            publish()?;
        }
        sm.last_applied_log = meta.last_log_id;
        sm.last_membership = meta.last_membership.clone();
        std::fs::rename(&tmp, self.snapshot_dir.join(CURRENT_SNAPSHOT_FILE))?;
        sync_dir(&self.snapshot_dir)?;
        *current = Some(meta.clone());
        tracing::info!(snapshot = %meta.snapshot_id, format_version = if use_v2 { 2 } else { 1 }, "atomic snapshot installed");
        Ok(())
    }
}
