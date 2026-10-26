pub mod recovery;
pub mod snapshot;
use anyhow::Result;
use bytes::Bytes;
use nodus_catalog::{IndexId, TableId};
use serde::{Deserialize, Serialize};
pub use snapshot::{SnapshotRow, SnapshotScope, SnapshotValue};
use std::sync::Arc;
use uuid::Uuid;

pub type Timestamp = u64;

/// Wall-clock microseconds since the Unix epoch — the physical component of
/// the transaction HLC's timestamps, so values from this clock compare
/// directly against `read_ts`/`commit_ts`/intent ages. Returns 0 if the
/// system clock is before the epoch (never panics).
pub fn wallclock_micros() -> Timestamp {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

/// A typed failure from a `KvEngine` mutation. The distinguished variants let a
/// caller — notably Raft state-machine apply — tell a benign, idempotent outcome
/// (e.g. committing a transaction whose intent was already consumed, which
/// happens on legitimate post-crash log replay) apart from a real storage/I/O
/// failure that must not be silently dropped.
#[derive(Debug, thiserror::Error)]
pub enum KvError {
    /// No pending intent exists for the transaction — e.g. it was already
    /// committed/aborted. On apply this is the benign replay-of-an-applied-entry
    /// case.
    #[error("no pending intent for transaction {0:?}")]
    IntentNotFound(TxnId),
    /// A concurrent committed write conflicts with this intent.
    #[error("write-write conflict for transaction {0:?}")]
    WriteConflict(TxnId),
    /// Any other (storage/I/O/serialization) failure. Treated as fatal by
    /// callers that distinguish benign outcomes.
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type KvResult<T> = std::result::Result<T, KvError>;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TxnId(pub Uuid);

impl TxnId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

impl Default for TxnId {
    fn default() -> Self {
        Self::new()
    }
}

pub struct KeyRange {
    pub start: Bytes,
    pub end: Bytes,
}

pub struct KvPair {
    pub key: Bytes,
    pub value: Bytes,
    pub version: Timestamp,
}

pub struct KvVersion {
    pub key: Bytes,
    pub value: Option<Bytes>,
    pub version: Timestamp,
}

pub enum IntentReplacement {
    Put(Bytes),
    Delete,
    Clear,
}

#[derive(Debug, Clone)]
pub enum IntentOperation {
    Put { key: Bytes, value: Bytes },
    Delete { key: Bytes },
}

impl IntentOperation {
    pub fn key(&self) -> &[u8] {
        match self {
            Self::Put { key, .. } | Self::Delete { key } => key,
        }
    }
}

/// One transaction's outstanding intents in an engine, as listed by
/// [`KvEngine::pending_intents`] for the intent janitor. `txn_id` is the id as
/// recorded in *this* engine: a namespaced group view reports the stored
/// (already namespace-mapped) id, which is what an abort of it must carry.
#[derive(Debug, Clone)]
pub struct PendingIntents {
    pub txn_id: TxnId,
    /// The transaction's intent keys that matched the requested prefix.
    pub keys: Vec<Bytes>,
    /// Write time (HLC-scale wall-clock microseconds) of the oldest matched
    /// intent. Sourced from the durable intent record, so the age survives a
    /// server restart rather than restarting with the process.
    pub oldest_ts: Timestamp,
}

pub trait KvEngine: Send + Sync {
    /// Removes an already-archived local WAL segment only when the durable
    /// checkpoint no longer needs it for crash recovery. The caller must first
    /// satisfy archive/backup retention policy. Unsupported engines retain it.
    /// Implementations serialize the check and removal with checkpoint publication.
    fn reclaim_archived_wal_segment(&self, _segment_id: u64) -> Result<bool> {
        Ok(false)
    }

    /// Node-local checkpoint identity. Namespace adapters must delegate to the
    /// physical engine; it is never transferred in a Raft snapshot.
    fn recovery_generation(&self) -> Result<Option<recovery::RecoveryGeneration>> {
        self.get(recovery::RECOVERY_GENERATION_KEY, u64::MAX)?
            .map(|b| recovery::RecoveryGeneration::decode(&b))
            .transpose()
    }

    /// Full version chains, including intents, bounded by SNAPSHOT_MEMORY_LIMIT.
    fn snapshot_rows(&self, _scope: &SnapshotScope) -> Result<Vec<SnapshotRow>> {
        anyhow::bail!("snapshot export is unsupported by this engine")
    }

    /// Atomically replaces the scope, plus explicit consensus pointer records.
    /// Failure before publication leaves old state; a publication I/O ambiguity
    /// must fail closed until reopen. Other groups and their intents survive.
    fn replace_snapshot(
        &self,
        _scope: &SnapshotScope,
        _rows: Vec<SnapshotRow>,
        _pointers: Vec<SnapshotRow>,
    ) -> Result<()> {
        anyhow::bail!("atomic snapshot replacement is unsupported by this engine")
    }

    /// The newest value visible at `read_ts`, or `None` when no version is
    /// visible OR the newest visible version is a committed tombstone: a
    /// delete must hide values held by older sources (older SSTables), never
    /// fall through to them.
    fn get(&self, key: &[u8], read_ts: Timestamp) -> Result<Option<Bytes>>;
    fn scan(
        &self,
        range: KeyRange,
        read_ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = Result<KvPair>> + Send>>;

    /// Scans committed MVCC versions in `range` with `since_ts < version <= read_ts`.
    /// Unlike `scan`, this includes tombstones (`value = None`) so incremental
    /// backup can preserve deletes.
    fn scan_versions(
        &self,
        range: KeyRange,
        since_ts: Timestamp,
        read_ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = Result<KvVersion>> + Send>> {
        let iter = self.scan(range, read_ts)?;
        Ok(Box::new(iter.filter_map(move |item| match item {
            Ok(pair) if pair.version > since_ts => Some(Ok(KvVersion {
                key: pair.key,
                value: Some(pair.value),
                version: pair.version,
            })),
            Ok(_) => None,
            Err(e) => Some(Err(e)),
        })))
    }

    fn write_intent(&self, txn_id: TxnId, key: Bytes, value: Bytes) -> KvResult<()>;
    /// Writes a deletion (tombstone) intent for `key`. After commit the key
    /// reads as absent at timestamps at or after the commit.
    fn delete_intent(&self, txn_id: TxnId, key: Bytes) -> KvResult<()>;
    /// Writes one transaction's intents as a batch. Engines may override the
    /// sequential default to make conflict validation and publication atomic.
    fn write_intent_batch(&self, txn_id: TxnId, ops: Vec<IntentOperation>) -> KvResult<()> {
        for op in ops {
            match op {
                IntentOperation::Put { key, value } => self.write_intent(txn_id, key, value)?,
                IntentOperation::Delete { key } => self.delete_intent(txn_id, key)?,
            }
        }
        Ok(())
    }
    /// Replaces or clears this transaction's current intent for one key.
    /// Used by SQL savepoints to restore the uncommitted state that existed
    /// when the savepoint was created.
    fn replace_intent(
        &self,
        txn_id: TxnId,
        key: Bytes,
        replacement: IntentReplacement,
    ) -> KvResult<()>;
    fn commit(&self, txn_id: TxnId, commit_ts: Timestamp) -> KvResult<()>;
    /// Commits transactions in sequence unless the engine provides an atomic
    /// batch implementation.
    fn commit_batch(&self, commits: Vec<(TxnId, Timestamp)>) -> KvResult<()> {
        for (txn_id, commit_ts) in commits {
            self.commit(txn_id, commit_ts)?;
        }
        Ok(())
    }
    fn abort(&self, txn_id: TxnId) -> KvResult<()>;

    /// Keys on which `txn_id` currently holds uncommitted intents in this engine
    /// (empty once it commits, aborts, or if it never wrote here). Cross-shard
    /// 2PC prepare uses this to vote: a participant asked to prepare a
    /// transaction whose intents have vanished cannot commit it, so it must vote
    /// no rather than later swallow a missing-intent commit as a benign replay.
    /// Default: empty — an engine that doesn't track intents reports none.
    fn pending_intent_keys(&self, _txn_id: TxnId) -> Vec<Bytes> {
        Vec::new()
    }

    /// Whether any transaction has intents under `prefix`. Fence acquisition
    /// calls this under the owning Raft group's apply lock. Unsupported engines
    /// must fail closed; an empty answer would allow fencing accepted writes.
    fn has_pending_intents(&self, _prefix: &[u8]) -> Result<bool> {
        anyhow::bail!("pending-intent inspection is unsupported by this engine")
    }

    /// Lists every transaction with outstanding intents under `prefix`, with
    /// the matched keys and the oldest matched intent's write timestamp.
    /// Used by the intent janitor to find intents abandoned by dead sessions.
    /// Unsupported engines must fail closed (like [`Self::has_pending_intents`]):
    /// an empty answer would let the janitor conclude there is nothing to do.
    fn pending_intents(&self, _prefix: &[u8]) -> Result<Vec<PendingIntents>> {
        anyhow::bail!("pending-intent listing is unsupported by this engine")
    }

    /// Aborts the intents recorded under exactly `txn_id` in this engine,
    /// bypassing any namespace id remapping that [`Self::abort`] applies. The
    /// intent janitor aborts orphans by their *recorded* id (the only one
    /// recoverable from the store after a restart), so Raft apply falls back
    /// to this when the mapped lookup finds nothing. Default: same as `abort`
    /// (correct for engines without id remapping).
    fn abort_physical(&self, txn_id: TxnId) -> KvResult<()> {
        self.abort(txn_id)
    }

    /// Ensures this engine reflects every write committed before the call for the
    /// group that owns `key` — a linearizable-read barrier. A reader that runs
    /// this before scanning observes all earlier committed writes, closing the
    /// stale-read window a replica's replication lag would otherwise open.
    /// Default: no-op — a single-node engine has no lag to wait out; only the
    /// Raft routing engine performs a real cross-node (ReadIndex) barrier.
    fn read_barrier(&self, _key: &[u8]) -> KvResult<()> {
        Ok(())
    }

    /// Establishes the read-barrier contract for a whole half-open range. Routed
    /// engines must cover all owners or reject unsupported consistency modes;
    /// checking only the start key is insufficient for a multi-shard scan.
    fn read_range_barrier(&self, range: KeyRange) -> KvResult<()> {
        self.read_barrier(&range.start)
    }

    /// Reclaims MVCC versions that no active reader can observe: for each key,
    /// committed versions strictly older than the newest version at or below
    /// `watermark` are removed. `watermark` must be ≤ the oldest active read
    /// timestamp. Returns the number of versions reclaimed. Default: no-op.
    ///
    /// An engine whose reads fall through to older sources (a memtable above
    /// SSTables) must RETAIN the newest-visible tombstone of each key: erasing
    /// it would empty the chain and resurrect the deleted value from the older
    /// source. Only a pass that merges every source (compaction) or a
    /// single-source engine may reclaim such a tombstone outright.
    fn garbage_collect(&self, _watermark: Timestamp) -> Result<usize> {
        Ok(0)
    }

    /// Flushes any in-memory data to persistent storage and rotates the write-ahead log.
    fn flush(&self) -> Result<()> {
        Ok(())
    }
}

/// A `KvEngine` view that confines all keys to a per-shard namespace by
/// prefixing them, so multiple Raft groups can share one underlying store
/// without their key spaces overlapping. Keys are transparently prefixed on
/// write and stripped on read; scans are translated into the namespace's
/// physical key range. As a result, a group's snapshot (a full scan of its
/// engine) only ever observes its own namespace's keys.
///
/// Transaction-scoped operations use namespace-mapped ids so one group's
/// commit or abort cannot finalize another group's intents. Garbage collection
/// delegates by watermark, and a given transaction writes within one namespace.
pub struct NamespacedKvEngine {
    inner: Arc<dyn KvEngine>,
    prefix: Bytes,
}

impl NamespacedKvEngine {
    /// Builds a namespace from `namespace` plus a `0x00` separator. The
    /// separator guarantees one namespace's prefix is never a prefix of
    /// another's (e.g. `shard-1` vs `shard-10`).
    pub fn new(inner: Arc<dyn KvEngine>, namespace: &str) -> Self {
        let mut prefix = Vec::with_capacity(namespace.len() + 1);
        prefix.extend_from_slice(namespace.as_bytes());
        prefix.push(0u8);
        Self {
            inner,
            prefix: Bytes::from(prefix),
        }
    }

    fn physical_key(&self, key: &[u8]) -> Bytes {
        let mut out = Vec::with_capacity(self.prefix.len() + key.len());
        out.extend_from_slice(&self.prefix);
        out.extend_from_slice(key);
        Bytes::from(out)
    }

    /// Derives a per-namespace transaction id. `commit`/`abort` on the shared
    /// inner engine are keyed by transaction id and would otherwise finalize a
    /// transaction's intents across *every* namespace at once; mapping the id
    /// deterministically per namespace keeps each shard's commit independent
    /// (essential for cross-shard 2PC, where one shard may commit before
    /// another). The mapping is deterministic so writes and their later
    /// commit/abort agree.
    fn namespaced_txn(&self, txn_id: TxnId) -> TxnId {
        let ns = Uuid::new_v5(&Uuid::NAMESPACE_OID, &self.prefix);
        TxnId(Uuid::new_v5(&ns, txn_id.0.as_bytes()))
    }

    /// The id under which `txn_id`'s intents are recorded in the shared inner
    /// engine. Callers scanning the raw store (the intent janitor) use this to
    /// recognize a live session's intents among the recorded ids.
    pub fn map_txn_id(&self, txn_id: TxnId) -> TxnId {
        self.namespaced_txn(txn_id)
    }
}

impl KvEngine for NamespacedKvEngine {
    fn recovery_generation(&self) -> Result<Option<recovery::RecoveryGeneration>> {
        self.inner.recovery_generation()
    }

    fn snapshot_rows(&self, scope: &SnapshotScope) -> Result<Vec<SnapshotRow>> {
        let mut rows = self.inner.snapshot_rows(&scope.namespaced(&self.prefix))?;
        for row in &mut rows {
            let key = row
                .key
                .strip_prefix(self.prefix.as_ref())
                .ok_or_else(|| anyhow::anyhow!("snapshot escaped namespace"))?;
            row.key = Bytes::copy_from_slice(key);
        }
        Ok(rows)
    }

    fn replace_snapshot(
        &self,
        scope: &SnapshotScope,
        mut rows: Vec<SnapshotRow>,
        mut pointers: Vec<SnapshotRow>,
    ) -> Result<()> {
        for row in rows.iter_mut().chain(pointers.iter_mut()) {
            row.key = self.physical_key(&row.key);
        }
        self.inner
            .replace_snapshot(&scope.namespaced(&self.prefix), rows, pointers)
    }

    fn has_pending_intents(&self, prefix: &[u8]) -> Result<bool> {
        self.inner.has_pending_intents(&self.physical_key(prefix))
    }

    /// Lists the namespace's pending intents. Keys are stripped of the
    /// namespace prefix; the transaction ids stay as *recorded* in the shared
    /// engine (i.e. namespace-mapped) — they are the ids [`Self::abort_physical`]
    /// expects, and the only ids recoverable after a restart.
    fn pending_intents(&self, prefix: &[u8]) -> Result<Vec<PendingIntents>> {
        let physical = self.physical_key(prefix);
        let mut out = self.inner.pending_intents(&physical)?;
        for txn in &mut out {
            txn.keys.retain(|k| k.starts_with(self.prefix.as_ref()));
            for key in &mut txn.keys {
                *key = Bytes::copy_from_slice(&key[self.prefix.len()..]);
            }
        }
        out.retain(|txn| !txn.keys.is_empty());
        Ok(out)
    }

    fn abort_physical(&self, txn_id: TxnId) -> KvResult<()> {
        // Bypass `namespaced_txn`: `txn_id` is already the id as recorded.
        self.inner.abort(txn_id)
    }

    fn get(&self, key: &[u8], read_ts: Timestamp) -> Result<Option<Bytes>> {
        self.inner.get(&self.physical_key(key), read_ts)
    }

    fn scan(
        &self,
        range: KeyRange,
        read_ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = Result<KvPair>> + Send>> {
        let physical = KeyRange {
            start: self.physical_key(range.start.as_ref()),
            end: self.physical_key(range.end.as_ref()),
        };
        let prefix = self.prefix.clone();
        let iter = self.inner.scan(physical, read_ts)?;
        Ok(Box::new(iter.map(move |item| {
            item.map(|pair| {
                let key = match pair.key.strip_prefix(prefix.as_ref()) {
                    Some(suffix) => Bytes::copy_from_slice(suffix),
                    None => pair.key,
                };
                KvPair {
                    key,
                    value: pair.value,
                    version: pair.version,
                }
            })
        })))
    }

    fn scan_versions(
        &self,
        range: KeyRange,
        since_ts: Timestamp,
        read_ts: Timestamp,
    ) -> Result<Box<dyn Iterator<Item = Result<KvVersion>> + Send>> {
        let physical = KeyRange {
            start: self.physical_key(range.start.as_ref()),
            end: self.physical_key(range.end.as_ref()),
        };
        let prefix = self.prefix.clone();
        let iter = self.inner.scan_versions(physical, since_ts, read_ts)?;
        Ok(Box::new(iter.map(move |item| {
            item.map(|version| {
                let key = match version.key.strip_prefix(prefix.as_ref()) {
                    Some(suffix) => Bytes::copy_from_slice(suffix),
                    None => version.key,
                };
                KvVersion {
                    key,
                    value: version.value,
                    version: version.version,
                }
            })
        })))
    }

    fn write_intent(&self, txn_id: TxnId, key: Bytes, value: Bytes) -> KvResult<()> {
        self.inner
            .write_intent(self.namespaced_txn(txn_id), self.physical_key(&key), value)
    }

    fn delete_intent(&self, txn_id: TxnId, key: Bytes) -> KvResult<()> {
        self.inner
            .delete_intent(self.namespaced_txn(txn_id), self.physical_key(&key))
    }

    fn write_intent_batch(&self, txn_id: TxnId, ops: Vec<IntentOperation>) -> KvResult<()> {
        let txn_id = self.namespaced_txn(txn_id);
        let ops = ops
            .into_iter()
            .map(|op| match op {
                IntentOperation::Put { key, value } => IntentOperation::Put {
                    key: self.physical_key(&key),
                    value,
                },
                IntentOperation::Delete { key } => IntentOperation::Delete {
                    key: self.physical_key(&key),
                },
            })
            .collect();
        self.inner.write_intent_batch(txn_id, ops)
    }

    fn replace_intent(
        &self,
        txn_id: TxnId,
        key: Bytes,
        replacement: IntentReplacement,
    ) -> KvResult<()> {
        self.inner.replace_intent(
            self.namespaced_txn(txn_id),
            self.physical_key(&key),
            replacement,
        )
    }

    fn commit(&self, txn_id: TxnId, commit_ts: Timestamp) -> KvResult<()> {
        self.inner.commit(self.namespaced_txn(txn_id), commit_ts)
    }

    fn commit_batch(&self, commits: Vec<(TxnId, Timestamp)>) -> KvResult<()> {
        let commits = commits
            .into_iter()
            .map(|(txn_id, commit_ts)| (self.namespaced_txn(txn_id), commit_ts))
            .collect();
        self.inner.commit_batch(commits)
    }

    fn abort(&self, txn_id: TxnId) -> KvResult<()> {
        self.inner.abort(self.namespaced_txn(txn_id))
    }

    fn pending_intent_keys(&self, txn_id: TxnId) -> Vec<Bytes> {
        // The namespaced txn id isolates this group's intents in the shared
        // inner store, so this is precise per group with no key filtering.
        self.inner.pending_intent_keys(self.namespaced_txn(txn_id))
    }

    fn garbage_collect(&self, watermark: Timestamp) -> Result<usize> {
        self.inner.garbage_collect(watermark)
    }

    fn flush(&self) -> Result<()> {
        self.inner.flush()
    }
}

pub type RowKey = Bytes;
pub type Datum = Bytes; // simplified

pub trait IndexKvCodec {
    fn encode_primary_key(
        &self,
        table_id: TableId,
        primary_key: &RowKey,
        version_ts: Timestamp,
    ) -> Result<Bytes>;
    fn encode_secondary_key(
        &self,
        index_id: IndexId,
        values: &[Datum],
        primary_key: &RowKey,
        version_ts: Timestamp,
    ) -> Result<Bytes>;
}
