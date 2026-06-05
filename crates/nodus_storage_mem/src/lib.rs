use anyhow::Result;
use bytes::Bytes;
use nodus_mvcc::VersionChain;
use nodus_storage_api::{KeyRange, KvEngine, KvPair, Timestamp, TxnId};
use std::collections::{BTreeMap, HashMap};
use std::sync::RwLock;

pub struct MemKvEngine {
    // simplified: key -> version chain
    store: RwLock<BTreeMap<Bytes, VersionChain>>,
    intents: RwLock<HashMap<TxnId, Vec<Bytes>>>,
}

impl MemKvEngine {
    pub fn new() -> Self {
        Self {
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
    fn get(&self, key: &[u8], read_ts: Timestamp) -> Result<Option<Bytes>> {
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

    fn write_intent(&self, txn_id: TxnId, key: Bytes, value: Bytes) -> Result<()> {
        let mut store_guard = self.store.write().unwrap();
        let mut intents_guard = self.intents.write().unwrap();

        let chain = store_guard.entry(key.clone()).or_default();
        if let Err(e) = chain.write_intent(txn_id, value.to_vec()) {
            anyhow::bail!("Write intent failed: {}", e);
        }

        intents_guard.entry(txn_id).or_default().push(key);
        Ok(())
    }

    fn delete_intent(&self, txn_id: TxnId, key: Bytes) -> Result<()> {
        let mut store_guard = self.store.write().unwrap();
        let mut intents_guard = self.intents.write().unwrap();

        let chain = store_guard.entry(key.clone()).or_default();
        if let Err(e) = chain.delete_intent(txn_id) {
            anyhow::bail!("Delete intent failed: {}", e);
        }

        intents_guard.entry(txn_id).or_default().push(key);
        Ok(())
    }

    fn commit(&self, txn_id: TxnId, commit_ts: Timestamp) -> Result<()> {
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

    fn abort(&self, txn_id: TxnId) -> Result<()> {
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
