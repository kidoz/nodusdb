use anyhow::Result;
use bytes::Bytes;
use nodus_mvcc::VersionChain;
use nodus_storage_api::{
    IntentReplacement, KeyRange, KvEngine, KvPair, KvResult, KvVersion, Timestamp, TxnId,
};
use std::collections::{BTreeMap, HashMap};
use std::sync::RwLock;

pub struct MemKvEngine {
    snapshot_gate: RwLock<()>,
    // simplified: key -> version chain
    store: RwLock<BTreeMap<Bytes, VersionChain>>,
    intents: RwLock<HashMap<TxnId, Vec<Bytes>>>,
}

impl MemKvEngine {
    pub fn new() -> Self {
        Self {
            snapshot_gate: RwLock::new(()),
            store: RwLock::new(BTreeMap::new()),
            intents: RwLock::new(HashMap::new()),
        }
    }
}

impl Default for MemKvEngine {
    fn default() -> Self {
        Self::new()
    }
}

impl KvEngine for MemKvEngine {
    fn snapshot_rows(
        &self,
        scope: &nodus_storage_api::SnapshotScope,
    ) -> Result<Vec<nodus_storage_api::SnapshotRow>> {
        let _snapshot = self.snapshot_gate.read().unwrap();
        let store = self.store.read().unwrap();
        let mut rows = Vec::new();
        let mut size = 0usize;
        for (key, chain) in store.iter().filter(|(k, _)| scope.owns(k)) {
            size += key.len()
                + chain
                    .versions
                    .iter()
                    .map(|v| v.value.as_ref().map_or(0, Vec::len) + 64)
                    .sum::<usize>();
            anyhow::ensure!(
                size <= nodus_storage_api::snapshot::SNAPSHOT_MEMORY_LIMIT,
                "snapshot exceeds checkpoint memory limit"
            );
            rows.push(nodus_storage_api::SnapshotRow {
                key: key.clone(),
                versions: chain.versions.clone().into_iter().map(Into::into).collect(),
            });
        }
        Ok(rows)
    }

    fn replace_snapshot(
        &self,
        scope: &nodus_storage_api::SnapshotScope,
        rows: Vec<nodus_storage_api::SnapshotRow>,
        pointers: Vec<nodus_storage_api::SnapshotRow>,
    ) -> Result<()> {
        nodus_storage_api::snapshot::check_snapshot_size(&rows)?;
        nodus_storage_api::snapshot::check_snapshot_size(&pointers)?;
        anyhow::ensure!(
            rows.iter()
                .chain(&pointers)
                .flat_map(|r| &r.versions)
                .all(|v| !v.is_intent || v.txn_id.is_some()),
            "snapshot intent has no transaction"
        );
        anyhow::ensure!(
            rows.iter().all(|r| scope.owns(&r.key)),
            "snapshot escaped scope"
        );
        let _snapshot = self.snapshot_gate.write().unwrap();
        let mut store = self.store.write().unwrap();
        let mut intents = self.intents.write().unwrap();
        store.retain(|key, _| !scope.owns(key));
        for row in rows.into_iter().chain(pointers) {
            store.insert(
                row.key,
                VersionChain {
                    versions: row.versions.into_iter().map(Into::into).collect(),
                },
            );
        }
        let generation = nodus_storage_api::recovery::RecoveryGeneration::checkpoint_row(0);
        store.insert(
            generation.key,
            VersionChain {
                versions: generation.versions.into_iter().map(Into::into).collect(),
            },
        );
        intents.clear();
        for (key, chain) in store.iter() {
            for v in &chain.versions {
                if v.is_intent
                    && let Some(txn) = v.txn_id
                {
                    intents.entry(txn).or_default().push(key.clone());
                }
            }
        }
        Ok(())
    }

    fn has_pending_intents(&self, prefix: &[u8]) -> Result<bool> {
        let _snapshot = self.snapshot_gate.read().unwrap();
        Ok(self
            .intents
            .read()
            .unwrap()
            .values()
            .flatten()
            .any(|key| key.starts_with(prefix)))
    }

    fn get(&self, key: &[u8], read_ts: Timestamp) -> Result<Option<Bytes>> {
        let _snapshot = self.snapshot_gate.read().unwrap();
        let guard = self.store.read().unwrap();
        if let Some(chain) = guard.get(key)
            && let Some(val) = chain.read(read_ts)
        {
            return Ok(Some(Bytes::from(val.to_vec())));
        }
        Ok(None)
    }

    fn scan(
        &self,
        range: KeyRange,
        read_ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = Result<KvPair>> + Send>> {
        let _snapshot = self.snapshot_gate.read().unwrap();
        let guard = self.store.read().unwrap();
        let mut results = Vec::new();

        for (k, chain) in guard.range(range.start..range.end) {
            if let Some(val) = chain.read(read_ts) {
                // Find the version of this read - for the scanner we need the actual version.
                // We'll peek into the versions since `read` just gives us the value.
                let version = chain
                    .versions
                    .iter()
                    .filter(|v| v.is_visible(read_ts))
                    .map(|v| v.version)
                    .max()
                    .unwrap_or(0);

                results.push(Ok(KvPair {
                    key: k.clone(),
                    value: Bytes::from(val.to_vec()),
                    version,
                }));
            }
        }

        Ok(Box::new(results.into_iter()))
    }

    fn scan_versions(
        &self,
        range: KeyRange,
        since_ts: Timestamp,
        read_ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = Result<KvVersion>> + Send>> {
        let _snapshot = self.snapshot_gate.read().unwrap();
        let guard = self.store.read().unwrap();
        let mut results = Vec::new();

        for (key, chain) in guard.range(range.start..range.end) {
            for version in chain
                .versions
                .iter()
                .filter(|v| !v.is_intent && v.version > since_ts && v.version <= read_ts)
            {
                results.push(Ok(KvVersion {
                    key: key.clone(),
                    value: version
                        .value
                        .as_ref()
                        .map(|value| Bytes::from(value.clone())),
                    version: version.version,
                }));
            }
        }
        results.sort_by(|a, b| match (a, b) {
            (Ok(left), Ok(right)) => left
                .key
                .cmp(&right.key)
                .then_with(|| left.version.cmp(&right.version)),
            _ => std::cmp::Ordering::Equal,
        });

        Ok(Box::new(results.into_iter()))
    }

    fn write_intent(&self, txn_id: TxnId, key: Bytes, value: Bytes) -> KvResult<()> {
        let _snapshot = self.snapshot_gate.read().unwrap();
        let mut store_guard = self.store.write().unwrap();
        let mut intents_guard = self.intents.write().unwrap();

        let chain = store_guard.entry(key.clone()).or_default();
        chain.write_intent(txn_id, value.to_vec())?;

        intents_guard.entry(txn_id).or_default().push(key);
        Ok(())
    }

    fn delete_intent(&self, txn_id: TxnId, key: Bytes) -> KvResult<()> {
        let _snapshot = self.snapshot_gate.read().unwrap();
        let mut store_guard = self.store.write().unwrap();
        let mut intents_guard = self.intents.write().unwrap();

        let chain = store_guard.entry(key.clone()).or_default();
        chain.delete_intent(txn_id)?;

        intents_guard.entry(txn_id).or_default().push(key);
        Ok(())
    }

    fn replace_intent(
        &self,
        txn_id: TxnId,
        key: Bytes,
        replacement: IntentReplacement,
    ) -> KvResult<()> {
        let _snapshot = self.snapshot_gate.read().unwrap();
        let mut store_guard = self.store.write().unwrap();
        let mut intents_guard = self.intents.write().unwrap();
        let chain = store_guard.entry(key.clone()).or_default();
        chain
            .versions
            .retain(|v| !(v.is_intent && v.txn_id == Some(txn_id)));
        match replacement {
            IntentReplacement::Put(value) => {
                chain.write_intent(txn_id, value.to_vec())?;
                intents_guard.entry(txn_id).or_default().push(key);
            }
            IntentReplacement::Delete => {
                chain.delete_intent(txn_id)?;
                intents_guard.entry(txn_id).or_default().push(key);
            }
            IntentReplacement::Clear => {
                if let Some(keys) = intents_guard.get_mut(&txn_id) {
                    keys.retain(|k| k != &key);
                    if keys.is_empty() {
                        intents_guard.remove(&txn_id);
                    }
                }
            }
        }
        Ok(())
    }

    fn pending_intent_keys(&self, txn_id: TxnId) -> Vec<Bytes> {
        let _snapshot = self.snapshot_gate.read().unwrap();
        self.intents
            .read()
            .unwrap()
            .get(&txn_id)
            .cloned()
            .unwrap_or_default()
    }

    fn commit(&self, txn_id: TxnId, commit_ts: Timestamp) -> KvResult<()> {
        let _snapshot = self.snapshot_gate.read().unwrap();
        let mut store_guard = self.store.write().unwrap();
        let mut intents_guard = self.intents.write().unwrap();

        if let Some(keys) = intents_guard.remove(&txn_id) {
            for key in keys {
                if let Some(chain) = store_guard.get_mut(&key) {
                    let _ = chain.commit(txn_id, commit_ts);
                }
            }
        }
        Ok(())
    }

    fn abort(&self, txn_id: TxnId) -> KvResult<()> {
        let _snapshot = self.snapshot_gate.read().unwrap();
        let mut store_guard = self.store.write().unwrap();
        let mut intents_guard = self.intents.write().unwrap();

        if let Some(keys) = intents_guard.remove(&txn_id) {
            for key in keys {
                if let Some(chain) = store_guard.get_mut(&key) {
                    let _ = chain.abort(txn_id);
                }
            }
        }
        Ok(())
    }

    fn garbage_collect(&self, watermark: Timestamp) -> Result<usize> {
        let _snapshot = self.snapshot_gate.read().unwrap();
        let mut store = self.store.write().unwrap();
        let mut removed = 0usize;
        let mut dead_keys = Vec::new();

        for (key, chain) in store.iter_mut() {
            removed += chain.garbage_collect(watermark);
            if chain.versions.is_empty() {
                dead_keys.push(key.clone());
            }
        }

        for k in dead_keys {
            store.remove(&k);
        }
        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_mvcc_visibility() {
        let engine = MemKvEngine::new();
        let k1 = Bytes::from("k1");
        let v1 = Bytes::from("v1");

        let txn = TxnId::new();
        engine.write_intent(txn, k1.clone(), v1.clone()).unwrap();

        // Cannot read intent before commit
        let res = engine.get(k1.as_ref(), 10).unwrap();
        assert!(res.is_none());

        engine.commit(txn, 10).unwrap();

        // Visible after commit at correct timestamp
        let res = engine.get(k1.as_ref(), 10).unwrap();
        assert_eq!(res.unwrap(), v1);

        // Not visible at older timestamp
        let res = engine.get(k1.as_ref(), 9).unwrap();
        assert!(res.is_none());
    }

    #[test]
    fn test_scan_versions_includes_tombstones() {
        let engine = MemKvEngine::new();
        let key = Bytes::from("k1");

        let put = TxnId::new();
        engine
            .write_intent(put, key.clone(), Bytes::from("v1"))
            .unwrap();
        engine.commit(put, 10).unwrap();

        let delete = TxnId::new();
        engine.delete_intent(delete, key.clone()).unwrap();
        engine.commit(delete, 20).unwrap();

        let versions: Vec<_> = engine
            .scan_versions(
                KeyRange {
                    start: Bytes::from("k"),
                    end: Bytes::from("l"),
                },
                10,
                30,
            )
            .unwrap()
            .map(|item| item.unwrap())
            .collect();

        assert_eq!(versions.len(), 1);
        assert_eq!(versions[0].key, key);
        assert_eq!(versions[0].version, 20);
        assert!(versions[0].value.is_none());
    }

    #[test]
    fn test_garbage_collect_prunes_old_versions() {
        let engine = MemKvEngine::new();
        let k = Bytes::from("k");

        let t1 = TxnId::new();
        engine
            .write_intent(t1, k.clone(), Bytes::from("v1"))
            .unwrap();
        engine.commit(t1, 5).unwrap();

        let t2 = TxnId::new();
        engine
            .write_intent(t2, k.clone(), Bytes::from("v2"))
            .unwrap();
        engine.commit(t2, 10).unwrap();

        // With no reader below 10, the version at ts=5 is reclaimable.
        let removed = engine.garbage_collect(10).unwrap();
        assert_eq!(removed, 1);
        // Latest value still readable.
        assert_eq!(
            engine.get(k.as_ref(), 10).unwrap().unwrap(),
            Bytes::from("v2")
        );

        // Idempotent: a second pass reclaims nothing.
        assert_eq!(engine.garbage_collect(10).unwrap(), 0);
    }
}
