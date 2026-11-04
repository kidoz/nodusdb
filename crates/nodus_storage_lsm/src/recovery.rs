//! Read-only validation of the manifest's required WAL lineage.

use anyhow::{Result, bail, ensure};
use nodus_storage_wal::{FileWalEngine, WalRecord, WalRecordV1, WalRecoveryScan};
use std::collections::HashSet;
use std::path::Path;

// Filenames reserve ids but confer no authority to read/replay their contents.
// Otherwise a restart can reuse an unpublished WAL and append a second header.
pub(super) fn next_file_id(dir: &Path, published_max: u64) -> Result<u64> {
    let mut high_water = published_max;
    for entry in std::fs::read_dir(dir)? {
        let name = entry?.file_name();
        let name = name.to_string_lossy();
        if let Some(id) = name
            .strip_suffix(".sst.tmp")
            .or_else(|| name.strip_suffix(".sst"))
            .or_else(|| name.strip_suffix(".log"))
            .and_then(|id| id.parse::<u64>().ok())
        {
            high_water = high_water.max(id);
        }
    }
    high_water.checked_add(1).ok_or_else(|| {
        anyhow::anyhow!(
            "LSM file id space exhausted in {}; refusing counter wraparound",
            dir.display()
        )
    })
}

pub(super) fn scan_chain(
    dir: &Path,
    active: u64,
    floor: u64,
    key: Option<[u8; 32]>,
) -> Result<Vec<WalRecoveryScan>> {
    ensure!(
        active > 0 && floor <= active,
        "invalid WAL recovery bounds in {}: active {active}, replay floor {floor}",
        dir.join("MANIFEST").display()
    );
    let mut scans = Vec::new();
    let mut visited = HashSet::new();
    let mut id = active;
    loop {
        ensure!(
            visited.insert(id),
            "cycle in required WAL chain at {}",
            dir.join(format!("{id}.log")).display()
        );
        let path = dir.join(format!("{id}.log"));
        let scan = FileWalEngine::scan(&path, key)?;
        for (position, record) in scan.records().iter().enumerate() {
            if matches!(record, WalRecord::V1(WalRecordV1::SegmentHeader { .. })) {
                ensure!(
                    position == 0,
                    "WAL {} has a duplicate or misplaced segment header at record {position}",
                    path.display()
                );
            }
        }
        ensure!(
            floor == 0
                || matches!(
                    scan.records().first(),
                    Some(WalRecord::V1(WalRecordV1::SegmentHeader { .. }))
                ),
            "WAL {} is missing the segment header required by positive replay floor {floor}",
            path.display()
        );
        let predecessor = match scan.records().first() {
            Some(WalRecord::V1(WalRecordV1::SegmentHeader { predecessor })) => *predecessor,
            _ => None,
        };
        if let Some(previous) = predecessor {
            ensure!(
                !visited.contains(&previous),
                "cycle in WAL {}: predecessor {previous} already visited",
                path.display()
            );
            ensure!(
                previous > 0 && previous < id,
                "invalid predecessor {previous} in WAL {}: expected a positive id below {id}",
                path.display()
            );
        }
        scans.push(scan);
        if floor > 0 && id == floor {
            break;
        }
        match predecessor {
            Some(previous) => {
                ensure!(
                    previous >= floor,
                    "WAL {} skips required replay floor {floor}: predecessor {previous}",
                    path.display()
                );
                id = previous;
            }
            None if floor > 0 => bail!(
                "WAL {} terminates before required replay floor {floor}; missing segment header or predecessor",
                path.display()
            ),
            // Historical/headerless roots and explicit root headers are valid
            // only when no positive required replay floor remains to reach.
            None => break,
        }
    }
    Ok(scans)
}
