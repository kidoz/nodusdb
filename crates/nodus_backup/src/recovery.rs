//! Recovery-generation checks without changing existing backup manifest formats.
use super::*;
use nodus_storage_api::recovery::RecoveryGeneration;

pub const GENERATION_OBJECT: &str = "recovery-generation.v1.json";
const REPOSITORY_GENERATION: &str = "recovery/current.v1.json";

#[derive(Serialize, Deserialize)]
struct Record {
    version: u16,
    generation: RecoveryGeneration,
}
fn encode(generation: RecoveryGeneration) -> Result<Bytes> {
    Ok(Bytes::from(serde_json::to_vec(&Record {
        version: 1,
        generation,
    })?))
}
fn decode(bytes: &[u8]) -> Result<RecoveryGeneration> {
    let record: Record = serde_json::from_slice(bytes)?;
    anyhow::ensure!(
        record.version == 1,
        "unsupported backup recovery-generation version"
    );
    Ok(record.generation)
}
fn object_generation(objects: &[BackupObject]) -> Result<Option<RecoveryGeneration>> {
    let mut values = objects.iter().filter(|o| o.name == GENERATION_OBJECT);
    let value = values.next().map(|o| decode(&o.bytes)).transpose()?;
    anyhow::ensure!(
        values.next().is_none(),
        "duplicate backup recovery-generation object"
    );
    Ok(value)
}

impl BackupOrchestrator {
    /// Bind to the physical source engine. Production backup, archive and PITR
    /// operations compare the durable checkpoint identity with their base.
    pub fn with_recovery_source(mut self, source: Arc<dyn KvEngine>) -> Self {
        self.recovery_source = Some(source);
        self
    }

    /// Capture before exporting catalog/data; pass the result to `seal_export`.
    pub fn capture_generation(&self) -> Result<Option<RecoveryGeneration>> {
        self.recovery_source
            .as_ref()
            .map(|s| s.recovery_generation())
            .transpose()
            .map(Option::flatten)
    }

    /// Reject export crossing a checkpoint and attach a checksummed manifest
    /// object. Legacy backups retain their existing bytes before any checkpoint.
    pub fn seal_export(
        &self,
        generation: Option<RecoveryGeneration>,
        objects: &mut Vec<BackupObject>,
    ) -> Result<()> {
        anyhow::ensure!(
            self.capture_generation()? == generation,
            "storage checkpoint changed during backup export; retry a full backup"
        );
        anyhow::ensure!(
            !objects.iter().any(|o| o.name == GENERATION_OBJECT),
            "recovery-generation object is reserved"
        );
        if let Some(generation) = generation {
            objects.push(BackupObject {
                name: GENERATION_OBJECT.into(),
                bytes: encode(generation)?,
            });
        }
        Ok(())
    }

    async fn repository_generation(&self) -> Result<Option<RecoveryGeneration>> {
        if !self.repo.object_exists(REPOSITORY_GENERATION).await? {
            return Ok(None);
        }
        decode(&self.repo.get_object(REPOSITORY_GENERATION, None).await?).map(Some)
    }

    /// Publish the observed checkpoint before uploading any subsequent archive
    /// segment or backup. Offline planners then also reject obsolete bases.
    pub async fn sync_recovery_generation(&self) -> Result<Option<RecoveryGeneration>> {
        let _guard = self.recovery_publication.lock().await;
        let known = self.repository_generation().await?;
        let Some(source) = &self.recovery_source else {
            return Ok(known);
        };
        let current = source.recovery_generation()?;
        anyhow::ensure!(
            current.is_some() || known.is_none(),
            "repository has checkpoint history missing from this source; use a separate repository"
        );
        if current != known
            && let Some(generation) = current
        {
            self.repo
                .put_object(
                    REPOSITORY_GENERATION,
                    encode(generation)?,
                    PutOptions::default(),
                )
                .await?;
            tracing::info!(generation = %generation.id, wal_floor = generation.wal_floor, "backup recovery boundary published; subsequent recovery requires a new full base");
        }
        Ok(current)
    }

    pub(super) async fn validate_export_generation(&self, objects: &[BackupObject]) -> Result<()> {
        let current = self.sync_recovery_generation().await?;
        anyhow::ensure!(
            object_generation(objects)? == current,
            "backup export belongs to another recovery generation; retry a full backup"
        );
        Ok(())
    }

    pub(super) async fn manifest_generation(
        &self,
        manifest: &BackupManifest,
    ) -> Result<Option<RecoveryGeneration>> {
        let suffix = format!("/{GENERATION_OBJECT}");
        let mut matches = manifest.files.iter().filter(|k| k.ends_with(&suffix));
        let Some(key) = matches.next() else {
            return Ok(None);
        };
        anyhow::ensure!(
            matches.next().is_none(),
            "duplicate recovery-generation object in manifest"
        );
        let bytes = self.repo.get_object(key, None).await?;
        anyhow::ensure!(
            manifest.checksums.get(key) == Some(&checksum(&bytes)),
            "backup recovery-generation checksum mismatch"
        );
        decode(&bytes).map(Some)
    }

    pub(super) async fn validate_incremental_generation(
        &self,
        parent: &BackupManifest,
        objects: &[BackupObject],
    ) -> Result<()> {
        self.validate_export_generation(objects).await?;
        anyhow::ensure!(
            self.manifest_generation(parent).await? == object_generation(objects)?,
            "incremental backup crosses a storage checkpoint; create a new full backup"
        );
        Ok(())
    }

    pub(super) async fn validate_chain_generation(
        &self,
        parent: &BackupManifest,
        child: &BackupManifest,
    ) -> Result<()> {
        anyhow::ensure!(
            self.manifest_generation(parent).await? == self.manifest_generation(child).await?,
            "backup chain crosses a storage checkpoint"
        );
        Ok(())
    }

    pub(super) async fn validate_pitr_generation(
        &self,
        base: &BackupManifest,
        target: u64,
    ) -> Result<()> {
        if target == base.snapshot_ts {
            return Ok(());
        }
        let current = self.sync_recovery_generation().await?;
        anyhow::ensure!(
            self.manifest_generation(base).await? == current,
            "PITR base precedes a storage checkpoint; select a full backup from the current recovery generation"
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nodus_storage_api::{SnapshotRow, SnapshotScope};
    fn objects() -> Vec<BackupObject> {
        vec![BackupObject {
            name: "kv_data.json".into(),
            bytes: Bytes::from_static(b"[]"),
        }]
    }
    fn checkpoint(kv: &dyn KvEngine) {
        kv.replace_snapshot(
            &SnapshotScope {
                exclude_raft: true,
                ..Default::default()
            },
            vec![SnapshotRow::committed(
                Bytes::from_static(b"row"),
                Bytes::from_static(b"new"),
                20,
            )],
            vec![],
        )
        .unwrap();
    }

    #[tokio::test]
    async fn checkpoint_rejects_incremental_and_pitr_until_new_full_backup() {
        let kv = Arc::new(nodus_storage_mem::MemKvEngine::new());
        let repo = Arc::new(MemBackupRepository::new());
        let backup = BackupOrchestrator::new(repo.clone()).with_recovery_source(kv.clone());
        let base = backup
            .create_full_backup("local", 10, 1, 1, objects())
            .await
            .unwrap();
        checkpoint(kv.as_ref());
        let generation = backup.capture_generation().unwrap();
        let mut data = objects();
        backup.seal_export(generation, &mut data).unwrap();
        assert!(
            backup
                .create_incremental_backup("local", &base.backup_id, 20, 1, 1, data.clone())
                .await
                .unwrap_err()
                .to_string()
                .contains("checkpoint")
        );
        assert!(
            backup
                .plan_pitr_restore(20)
                .await
                .unwrap_err()
                .to_string()
                .contains("checkpoint")
        );
        let offline = BackupOrchestrator::new(repo);
        assert!(
            offline.plan_pitr_restore(20).await.is_err(),
            "repository boundary protects offline planner after publication"
        );
        assert!(
            offline.restore(&base.backup_id).await.is_ok(),
            "historical full restore remains available"
        );
        let fresh = backup
            .create_full_backup("local", 20, 1, 1, data.clone())
            .await
            .unwrap();
        backup
            .create_incremental_backup("local", &fresh.backup_id, 21, 1, 1, data)
            .await
            .unwrap();
        assert!(backup.plan_pitr_restore(22).await.is_ok());
        assert!(
            backup.plan_pitr_restore(10).await.is_ok(),
            "exact full backup target needs no WAL"
        );
    }

    #[tokio::test]
    async fn export_checkpoint_race_and_corrupt_boundary_never_publish_complete_backup() {
        let kv = Arc::new(nodus_storage_mem::MemKvEngine::new());
        let repo = Arc::new(MemBackupRepository::new());
        let backup = BackupOrchestrator::new(repo.clone()).with_recovery_source(kv.clone());
        let before = backup.capture_generation().unwrap();
        checkpoint(kv.as_ref());
        assert!(backup.seal_export(before, &mut objects()).is_err());
        assert!(
            backup
                .create_full_backup("local", 20, 1, 1, objects())
                .await
                .is_err()
        );
        assert!(backup.list_backups().await.unwrap().is_empty());
        repo.put_object(
            REPOSITORY_GENERATION,
            Bytes::from_static(b"{\"version\":99}"),
            PutOptions::default(),
        )
        .await
        .unwrap();
        assert!(backup.sync_recovery_generation().await.is_err());
    }

    #[tokio::test]
    async fn restore_rejects_cross_generation_ancestry_even_with_valid_objects() {
        let kv = Arc::new(nodus_storage_mem::MemKvEngine::new());
        let backup = BackupOrchestrator::new(Arc::new(MemBackupRepository::new()))
            .with_recovery_source(kv.clone());
        let old = backup
            .create_full_backup("local", 10, 1, 1, objects())
            .await
            .unwrap();
        checkpoint(kv.as_ref());
        let mut data = objects();
        backup
            .seal_export(backup.capture_generation().unwrap(), &mut data)
            .unwrap();
        let mut fresh = backup
            .create_full_backup("local", 20, 1, 1, data)
            .await
            .unwrap();
        // Simulate a repository produced by a writer without ancestry guards.
        fresh.backup_type = BackupType::Incremental;
        fresh.parent_backup_id = Some(old.backup_id);
        backup
            .put_manifest(&manifest_key(&fresh.backup_id), &fresh)
            .await
            .unwrap();
        assert!(backup.verify(&fresh.backup_id).await.is_ok());
        assert!(
            backup
                .restore(&fresh.backup_id)
                .await
                .err()
                .unwrap()
                .to_string()
                .contains("checkpoint")
        );
        assert!(backup.plan_pitr_restore(20).await.is_err());
    }

    #[tokio::test]
    async fn pitr_plan_is_rechecked_if_checkpoint_happens_after_planning() {
        let kv = Arc::new(nodus_storage_mem::MemKvEngine::new());
        let backup = BackupOrchestrator::new(Arc::new(MemBackupRepository::new()))
            .with_recovery_source(kv.clone());
        backup
            .create_full_backup("local", 10, 1, 1, objects())
            .await
            .unwrap();
        let plan = backup.plan_pitr_restore(11).await.unwrap();
        checkpoint(kv.as_ref());
        assert!(backup.load_pitr_wal_segments(&plan).await.is_err());
    }
}
