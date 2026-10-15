//! The abandoned-intent janitor: bounds the lifetime of write intents whose
//! owning transaction can never finalize — its session died without cleanup,
//! or the coordinating node crashed — so they cannot block writers with
//! `WriteConflict`, pin MVCC GC, or stall (legacy-format) snapshot builds
//! forever, which is what grew the Raft log without bound.
//!
//! Safety rules, in check order (when in doubt, skip):
//!
//! 1. UPGRADE GATE: no aborts while a rolling upgrade is in flight. The abort
//!    of an orphan names the id as *recorded* in the group's store, applied
//!    via the `abort_physical` fallback a pre-upgrade binary lacks; applying
//!    it there would be a benign no-op and diverge the replica.
//! 2. LIVE: an intent whose transaction is in `txn_groups` — begun here and
//!    not yet finalized, including one mid-`commit`/`mid-abort` (those keep
//!    their entry until finalize completes) — is never aborted.
//! 3. 2PC: an intent whose transaction has a `\x00txn2pc\x00` decision record
//!    belongs to the cross-shard recovery machinery, which re-drives the
//!    decided commit; the janitor never touches it. An undecodable record
//!    fails the whole sweep closed.
//! 4. YOUNG: only intents older than `txn.abandoned_intent_ttl_secs` are
//!    eligible. The age comes from the intent's durable HLC-scale write stamp,
//!    so it survives a restart rather than resetting with process uptime. The
//!    TTL (default 300s) must dwarf any in-flight commit window (ms), which is
//!    what makes the prepare-to-decision race in (3) unreachable in practice;
//!    a transaction whose *coordinator lives on another node* is invisible to
//!    (2), so the TTL must also exceed the longest legitimate transaction.
//!
//! Aborts are replicated (`ShardCommand::AbortTxn` through the group's Raft),
//! never local-only, so every replica releases the intent identically; and
//! they run only for groups this node currently leads, so replicas don't
//! duplicate the sweep.

use anyhow::Result;
use nodus_raftstore::ShardCommand;
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
    /// None of the above: the owner is definitively dead; abort.
    Abandoned,
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
        IntentVerdict::Abandoned
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
    /// Aborts submitted and applied.
    pub aborted: usize,
    /// Aborts that failed to replicate (retried next sweep).
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

    /// One janitor pass over the groups this node leads. Aborts every pending
    /// intent older than `ttl` whose transaction is definitively dead (see the
    /// module docs for the safety rules). `now` is an HLC-scale timestamp; in
    /// production `wallclock_micros()`, parameterized for tests.
    pub async fn sweep_abandoned_intents(
        &self,
        ttl: Duration,
        now: Timestamp,
    ) -> Result<SweepReport> {
        let mut report = SweepReport::default();

        // Upgrade gate: a mixed-version cluster must not replicate janitor
        // aborts (see module docs, rule 1). Fail closed on an unreadable
        // authority.
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
        // skipped: aborting another node's group state locally would diverge
        // this node from that group's real replicas.
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

        // Only sweep groups this node leads: a single sweeper per group, and
        // the leader's applied view is authoritative.
        let mut candidates = Vec::new();
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
                IntentVerdict::Abandoned => candidates.push((group, txn_id)),
            }
        }

        if candidates.is_empty() {
            return Ok(report);
        }
        // The router's submit blocks on a oneshot and must run off the reactor.
        let router = self.router.clone();
        let (aborted, failed) = tokio::task::spawn_blocking(move || {
            let mut aborted = 0;
            let mut failed = 0;
            for (group, txn_id) in candidates {
                let result = router.submit(
                    &group,
                    ShardCommand::AbortTxn {
                        txn_id: txn_id.0.to_string(),
                        shard_id: RaftKvEngine::shard_field(&group),
                        recorded: true,
                    },
                );
                match result {
                    Ok(()) => {
                        aborted += 1;
                        tracing::info!(
                            txn = %txn_id.0,
                            group,
                            "janitor aborted abandoned transaction's intents"
                        );
                    }
                    Err(e) => {
                        failed += 1;
                        tracing::warn!(
                            txn = %txn_id.0,
                            group,
                            "janitor abort failed (retried next sweep): {e}"
                        );
                    }
                }
            }
            (aborted, failed)
        })
        .await
        .map_err(|e| anyhow::anyhow!("janitor abort task panicked: {e}"))?;
        report.aborted = aborted;
        report.failed = failed;
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
        // An old orphan is abandoned and aborted.
        assert_eq!(
            classify_intent(NOW, old, TTL, false, false),
            IntentVerdict::Abandoned
        );
        // Exactly at the TTL boundary the intent is old enough to collect.
        assert_eq!(
            classify_intent(NOW, NOW - 300_000_000, TTL, false, false),
            IntentVerdict::Abandoned
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
