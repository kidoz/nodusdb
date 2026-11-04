//! Explicit offline migration of the historical pre-manifest file layout.

use super::{FileWalEngine, MANIFEST_FILE, Manifest, Sstable, WalRecord, WalRecordV1};
use anyhow::{Context, Result, ensure};
use std::collections::BTreeSet;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;

const STAGING_MANIFEST: &str = "LEGACY_MANIFEST.tmp";

pub(super) fn ensure_fresh_directory(dir: &Path) -> Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let path = entry?.path();
        let name = path.file_name().unwrap_or_default().to_string_lossy();
        let numeric_wal = name
            .strip_suffix(".log")
            .is_some_and(|id| id.parse::<u64>().is_ok());
        ensure!(
            !(numeric_wal
                || name.ends_with(".sst")
                || name.ends_with(".sst.tmp")
                || matches!(
                    name.as_ref(),
                    "MANIFEST.tmp" | "SNAPSHOT_MANIFEST.tmp" | STAGING_MANIFEST
                )),
            "missing LSM manifest {} with existing storage artifact {}; restore the correct manifest/backup, or explicitly migrate a verified historical layout with nodusctl storage migrate-legacy",
            dir.join(MANIFEST_FILE).display(),
            path.display()
        );
    }
    Ok(())
}

/// Publishes a manifest for an explicitly identified historical offline store.
///
/// The node must be stopped and the directory backed up; this function does
/// not lock out other processes. All final SSTables must be listed, and the
/// active WAL must be `max_sstable_id + 1` (or 1 without SSTables). Every WAL
/// must be intact and lack modern segment headers/batched commits. Files are
/// validated read-only before publication; no existing manifest is replaced.
/// This cannot reconstruct the authoritative file set of a lost modern store.
pub fn migrate_legacy_manifest(
    dir: impl AsRef<Path>,
    sstables: &[u64],
    active_wal: u64,
    key: Option<[u8; 32]>,
) -> Result<()> {
    let dir = dir.as_ref();
    let ids: BTreeSet<_> = sstables.iter().copied().collect();
    ensure!(
        ids.len() == sstables.len() && !ids.contains(&0),
        "SSTable ids must be unique and positive"
    );
    let expected_wal = ids
        .last()
        .copied()
        .unwrap_or(0)
        .checked_add(1)
        .context("legacy SSTable id overflow")?;
    ensure!(
        active_wal == expected_wal && active_wal < u64::MAX,
        "legacy active WAL must be max SSTable id + 1 (expected {expected_wal})"
    );
    let mut found_sst = BTreeSet::new();
    let mut found_wal = BTreeSet::new();
    for entry in std::fs::read_dir(dir)
        .with_context(|| format!("reading legacy directory {}", dir.display()))?
    {
        let entry = entry?;
        let path = entry.path();
        let name = entry.file_name();
        let name = name.to_string_lossy();
        ensure!(
            !matches!(
                name.as_ref(),
                MANIFEST_FILE | "MANIFEST.tmp" | "SNAPSHOT_MANIFEST.tmp" | STAGING_MANIFEST
            ),
            "refusing legacy migration with manifest/publication artifact {}",
            path.display()
        );
        let (id, extension) = if let Some(id) = name.strip_suffix(".sst") {
            (id, "sst")
        } else if let Some(id) = name.strip_suffix(".log") {
            (id, "log")
        } else {
            continue;
        };
        let id: u64 = id
            .parse()
            .with_context(|| format!("unrecognized legacy file {}", path.display()))?;
        ensure!(
            id > 0 && name == format!("{id}.{extension}"),
            "noncanonical legacy file {}",
            path.display()
        );
        ensure!(
            entry.file_type()?.is_file(),
            "legacy data must be regular files: {}",
            path.display()
        );
        if extension == "sst" {
            found_sst.insert(id);
        } else {
            found_wal.insert(id);
        }
    }
    ensure!(
        found_sst == ids,
        "explicit SSTable ids must match every final SSTable in the legacy directory"
    );
    ensure!(
        found_wal.contains(&active_wal) && found_wal.iter().all(|id| *id <= active_wal),
        "legacy directory must contain active WAL {active_wal} and no later WALs"
    );
    for id in &ids {
        let path = dir.join(format!("{id}.sst"));
        let table = Sstable::open(&path);
        let mut previous = None;
        for row in table
            .iter()
            .with_context(|| format!("validating {}", path.display()))?
        {
            let (key, _) = row.with_context(|| format!("validating {}", path.display()))?;
            ensure!(
                previous.as_ref().is_none_or(|prior| prior < &key),
                "unsorted/duplicate keys in {}",
                path.display()
            );
            ensure!(
                table.get(&key)?.is_some(),
                "SSTable index cannot find a stored key in {}",
                path.display()
            );
            previous = Some(key);
        }
    }
    for id in found_wal {
        let path = dir.join(format!("{id}.log"));
        let records = FileWalEngine::inspect(&path, key)
            .with_context(|| format!("validating legacy WAL {}", path.display()))?;
        ensure!(
            !records.iter().any(|record| matches!(
                record,
                WalRecord::V1(
                    WalRecordV1::SegmentHeader { .. } | WalRecordV1::CommitTxnBatchV1 { .. }
                )
            )),
            "modern WAL records in {}; restore its original manifest instead of legacy migration",
            path.display()
        );
    }
    let manifest = Manifest {
        sstables: ids.into_iter().collect(),
        active_wal,
        min_replay_wal: 0,
    };
    publish_manifest(dir, &manifest)
}

fn publish_manifest(dir: &Path, manifest: &Manifest) -> Result<()> {
    let staging = dir.join(STAGING_MANIFEST);
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&staging)
        .context("creating legacy manifest staging file")?;
    file.write_all(&serde_json::to_vec(manifest)?)?;
    file.sync_all()?;
    drop(file);
    #[cfg(test)]
    exit_at_publication(dir, "before-link");
    // A hard link publishes the fully synced file atomically and cannot
    // overwrite a manifest created by another process during validation.
    std::fs::hard_link(&staging, dir.join(MANIFEST_FILE))
        .context("publishing legacy manifest without replacing an existing manifest")?;
    #[cfg(test)]
    exit_at_publication(dir, "after-link");
    File::open(dir)?.sync_all()?;
    std::fs::remove_file(staging)?;
    File::open(dir)?.sync_all()?;
    Ok(())
}

#[cfg(test)]
fn exit_at_publication(dir: &Path, phase: &str) {
    if std::env::var_os("NODUS_LEGACY_PUBLICATION_DIR").as_deref() == Some(dir.as_os_str())
        && std::env::var("NODUS_LEGACY_PUBLICATION_PHASE").as_deref() == Ok(phase)
    {
        std::process::exit(75);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn publication_does_not_overwrite_a_manifest_created_after_validation() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(MANIFEST_FILE);
        let original = b"another writer's manifest";
        std::fs::write(&path, original).unwrap();
        assert!(
            publish_manifest(
                dir.path(),
                &Manifest {
                    active_wal: 1,
                    ..Default::default()
                }
            )
            .is_err()
        );
        assert_eq!(std::fs::read(path).unwrap(), original);
    }

    #[test]
    fn publication_child() {
        let Some(dir) = std::env::var_os("NODUS_LEGACY_PUBLICATION_DIR") else {
            return;
        };
        migrate_legacy_manifest(Path::new(&dir), &[], 1, None).unwrap();
        panic!("publication exit boundary was not reached");
    }

    #[test]
    fn abrupt_exit_around_publication_leaves_missing_or_complete_manifest() {
        for phase in ["before-link", "after-link"] {
            let dir = tempfile::tempdir().unwrap();
            std::fs::write(dir.path().join("1.log"), []).unwrap();
            File::open(dir.path().join("1.log"))
                .unwrap()
                .sync_all()
                .unwrap();
            let status = std::process::Command::new(std::env::current_exe().unwrap())
                .args(["--exact", "legacy::tests::publication_child", "--nocapture"])
                .env("NODUS_LEGACY_PUBLICATION_DIR", dir.path())
                .env("NODUS_LEGACY_PUBLICATION_PHASE", phase)
                .status()
                .unwrap();
            assert_eq!(
                status.code(),
                Some(75),
                "exit boundary {phase} was not reached"
            );
            assert!(std::fs::read(dir.path().join("1.log")).unwrap().is_empty());
            if phase == "before-link" {
                assert!(!dir.path().join(MANIFEST_FILE).exists());
                assert!(crate::LsmKvEngine::with_wal(dir.path(), None).is_err());
                // The fully synced staging file was never authoritative.
                std::fs::remove_file(dir.path().join(STAGING_MANIFEST)).unwrap();
                migrate_legacy_manifest(dir.path(), &[], 1, None).unwrap();
            } else {
                let bytes = std::fs::read(dir.path().join(MANIFEST_FILE)).unwrap();
                let manifest: Manifest = serde_json::from_slice(&bytes).unwrap();
                assert_eq!(manifest.active_wal, 1);
                assert!(manifest.sstables.is_empty());
            }
            crate::LsmKvEngine::with_wal(dir.path(), None).unwrap();
        }
    }
}
