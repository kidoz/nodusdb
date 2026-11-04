//! Conservative intent inspection. A local liveness map and a TTL cannot prove
//! that a remote coordinator is dead or that it can no longer commit.
//! Old intents with no visible owner/decision are retained as uncertain.
//! Automatic orphan abortion requires a durable, fenced ownership/decision
//! authority that does not exist yet; this sweep submits no abort commands.
//!
//! Safety rules, in check order (when in doubt, skip):
//!
//! 1. UPGRADE GATE: retain the existing rolling-upgrade pause and fail closed
//!    when its authority cannot be read.
//! 2. LIVE: an intent whose transaction is in `txn_groups` — begun here and
//!    not yet finalized, including one mid-`commit`/`mid-abort` (those keep
//!    their entry until finalize completes) — is never aborted.
//! 3. 2PC: an intent whose transaction has a `\x00txn2pc\x00` decision record
//!    belongs to the cross-shard recovery machinery, which re-drives the
//!    decided commit; the janitor never touches it. An undecodable record
//!    fails the whole sweep closed.
//! 4. YOUNG: younger intents remain in the young classification. Durable write
//!    stamps keep age meaningful after restart; age is observability, not proof.
//! 5. UNCERTAIN: expired, locally unknown intents remain protected. A remote
//!    transaction may still be live, preparing, or publishing its decision.
//!
//! Counts cover only led groups. Explicit transaction rollback and durable
//! decided-commit recovery retain their normal replicated resolution paths.

use anyhow::Result;
use nodus_raftstore::upgrade::{self, Phase};
use nodus_storage_api::{NamespacedKvEngine, Timestamp, TxnId};
use std::collections::{BTreeMap, HashSet};
use std::time::Duration;

use crate::multi_raft::META_SHARD;

use super::{PendingTxn, RaftKvEngine, TXN2PC_PREFIX};

/// What the janitor concluded about one transaction's pending intents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum IntentVerdict {
    /// Owned by a transaction begun on this node that has not finalized.
    LiveSession,
    /// A decided cross-shard commit; owned by the 2PC recovery machinery.
    TwoPcCoordinated,
    /// Younger than the TTL; may still be finalized by its owner.
    Young,
    /// Old and locally unknown; no authoritative proof permits automatic abort.
    UncertainOwner,
}

/// Classifies one pending intent set. `now`/`oldest_ts` are HLC-scale
/// wall-clock microseconds (the scale intents are stamped with).
pub(crate) fn classify_intent(
    now: Timestamp,
    oldest_ts: Timestamp,
    ttl: Duration,
    live: bool,
    coordinated: bool,
) -> IntentVerdict {
    // Liveness and coordination outrank age: a stale-looking intent whose
    // owner is alive or whose commit was decided must never be aborted.
    if live {
        return IntentVerdict::LiveSession;
    }
    if coordinated {
        return IntentVerdict::TwoPcCoordinated;
    }
    let age = Duration::from_micros(now.saturating_sub(oldest_ts));
    if age < ttl {
        IntentVerdict::Young
    } else {
        IntentVerdict::UncertainOwner
    }
}

/// Outcome of one janitor sweep, for metrics and tests.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct SweepReport {
    /// Transactions with pending intents observed across led groups.
    pub pending: usize,
    pub live: usize,
    pub coordinated: usize,
    pub young: usize,
    /// Old transaction/group sets retained because ownership is uncertain.
    pub uncertain: usize,
    /// Kept for report compatibility; inspection-only sweeps submit no aborts.
    pub aborted: usize,
    /// Kept for report compatibility; inspection-only sweeps attempt no aborts.
    pub failed: usize,
}

impl RaftKvEngine {
    /// The id under which `txn`'s intents are recorded in `group`'s slice of
    /// the shared store: the session id on the meta group, the namespaced id
    /// on a data group.
    fn recorded_txn_id(&self, group: &str, txn: TxnId) -> TxnId {
        if group == META_SHARD {
            txn
        } else {
            NamespacedKvEngine::new(self.local.clone(), group).map_txn_id(txn)
        }
    }

    /// Transactions with a durable 2PC commit decision, as (group, recorded
    /// id) pairs the janitor must leave to recovery. Fails closed: an
    /// undecodable record aborts the sweep rather than risking a skip.
    fn coordinated_txns(&self) -> Result<HashSet<(String, TxnId)>> {
        let mut end = TXN2PC_PREFIX.to_vec();
        *end.last_mut().unwrap() += 1; // prefix successor bounds the scan
        let range = nodus_storage_api::KeyRange {
            start: bytes::Bytes::from(TXN2PC_PREFIX.to_vec()),
            end: bytes::Bytes::from(end),
        };
        let mut protected = HashSet::new();
        for pair in self.local.scan(range, u64::MAX)? {
            let pair = pair?;
            let record = PendingTxn::decode(&pair.value).map_err(|e| {
                anyhow::anyhow!("undecodable 2PC record; failing sweep closed: {e}")
            })?;
            let txn = std::str::from_utf8(&pair.key[TXN2PC_PREFIX.len()..])?;
            let txn_id = TxnId(uuid::Uuid::parse_str(txn)?);
            for group in &record.participants {
                protected.insert((group.clone(), self.recorded_txn_id(group, txn_id)));
            }
        }
        Ok(protected)
    }

    /// Transactions begun on this node and not yet finalized (including
    /// mid-commit/mid-abort, which keep their entry until finalize completes),
    /// as (group, recorded id) pairs.
    fn live_txns(&self) -> HashSet<(String, TxnId)> {
        let txns = self.txn_groups.lock().unwrap();
        let mut live = HashSet::new();
        for (txn, groups) in txns.iter() {
            for group in groups.keys() {
                live.insert((group.clone(), self.recorded_txn_id(group, *txn)));
            }
        }
        live
    }

    /// Classifies pending transaction/group sets led here, retaining uncertain
    /// owners. This inspection-only pass does not mutate intents. `ttl` is an
    /// observation threshold, not abort authority; `now` is an HLC-scale stamp.
    pub async fn sweep_abandoned_intents(
        &self,
        ttl: Duration,
        now: Timestamp,
    ) -> Result<SweepReport> {
        let mut report = SweepReport::default();

        // Retain the upgrade pause; fail closed on an unreadable authority.
        let phase = upgrade::read(self.local.as_ref())?.phase;
        if matches!(phase, Phase::RollingNodes | Phase::ReadyToFinalize) {
            tracing::debug!(?phase, "intent janitor paused during rolling upgrade");
            return Ok(report);
        }

        let pending = self.local.pending_intents(b"")?;
        let protected = self.coordinated_txns()?;
        let live = self.live_txns();

        // Attribute each recorded intent to its owning group. Data-group keys
        // carry the `{group}\0` namespace prefix; anything else is the meta
        // group — except a `shard-*` prefix no hosted group claims, which is
        // skipped: another node's unhosted group is outside this sweep's scope.
        let groups: Vec<(String, Vec<u8>)> = self
            .manager
            .hosted_group_ids()
            .into_iter()
            .filter(|g| g != META_SHARD)
            .map(|g| {
                let mut prefix = g.clone().into_bytes();
                prefix.push(0u8);
                (g, prefix)
            })
            .collect();
        let mut by_txn: BTreeMap<(String, TxnId), Timestamp> = BTreeMap::new();
        for intents in pending {
            for key in &intents.keys {
                let group = match groups
                    .iter()
                    .find(|(_, prefix)| key.starts_with(prefix))
                    .map(|(group, _)| group.clone())
                {
                    Some(group) => group,
                    None if key.starts_with(b"shard-") => continue, // unhosted namespace
                    None => META_SHARD.to_string(),
                };
                let entry = by_txn
                    .entry((group, intents.txn_id))
                    .or_insert(intents.oldest_ts);
                *entry = (*entry).min(intents.oldest_ts);
            }
        }

        // Count only groups led here. Leadership makes this a useful applied
        // view, but does not establish the death of a transaction's coordinator.
        for ((group, txn_id), oldest_ts) in by_txn {
            let led = match self.manager.get(&group).await {
                Some(raft) => {
                    raft.metrics().borrow().current_leader == Some(self.manager.node_id())
                }
                None => false,
            };
            if !led {
                continue;
            }
            report.pending += 1;
            match classify_intent(
                now,
                oldest_ts,
                ttl,
                live.contains(&(group.clone(), txn_id)),
                protected.contains(&(group.clone(), txn_id)),
            ) {
                IntentVerdict::LiveSession => report.live += 1,
                IntentVerdict::TwoPcCoordinated => report.coordinated += 1,
                IntentVerdict::Young => report.young += 1,
                IntentVerdict::UncertainOwner => report.uncertain += 1,
            }
        }

        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const NOW: Timestamp = 1_000_000_000_000; // arbitrary HLC-scale instant
    const TTL: Duration = Duration::from_secs(300);

    #[test]
    fn classification_prioritizes_safety_over_age() {
        let old = NOW - 600_000_000; // 600s ago, past the 300s TTL
        // A live session's intent is never aborted, however old.
        assert_eq!(
            classify_intent(NOW, old, TTL, true, false),
            IntentVerdict::LiveSession
        );
        // A decided 2PC transaction is never aborted, however old.
        assert_eq!(
            classify_intent(NOW, old, TTL, false, true),
            IntentVerdict::TwoPcCoordinated
        );
        // Liveness wins over the coordination flag too (defensive ordering).
        assert_eq!(
            classify_intent(NOW, old, TTL, true, true),
            IntentVerdict::LiveSession
        );
        // A young orphan is left to its owner.
        let young = NOW - 10_000_000; // 10s ago
        assert_eq!(
            classify_intent(NOW, young, TTL, false, false),
            IntentVerdict::Young
        );
        // Age and local absence do not establish a dead owner.
        assert_eq!(
            classify_intent(NOW, old, TTL, false, false),
            IntentVerdict::UncertainOwner
        );
        // At the threshold it is uncertain, never eligible for expiry-only abort.
        assert_eq!(
            classify_intent(NOW, NOW - 300_000_000, TTL, false, false),
            IntentVerdict::UncertainOwner
        );
    }

    #[test]
    fn classification_tolerates_clock_regression() {
        // A stamp ahead of `now` (clock skew across nodes) reads as age zero —
        // young — never as a huge bogus age.
        let future = NOW + 10_000_000;
        assert_eq!(
            classify_intent(NOW, future, TTL, false, false),
            IntentVerdict::Young
        );
    }
}
