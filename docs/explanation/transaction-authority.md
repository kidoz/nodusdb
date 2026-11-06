# Transaction ownership and decisions

Tracked transactions activate only after **`transaction-authority-v5`** is
finalized. Levels 1–4 retain the existing commit-only 2PC protocol. The meta
leader automatically replays terminal tracked decisions with durable progress;
undecided and untracked transactions remain protected from automatic abort.

**Status:** production activation and bounded terminal recovery implemented,
2026-11-05. Performance and safe decision reclamation remain unqualified.

## Why a separate authority exists

An old intent can belong to a live coordinator on another node. Neither its age
nor absence from a local session map permits deleting it. The existing 2PC
journal records a commit decision only after prepare; it cannot fence an older
coordinator or prove an abort. See the [durability contract](../reference/durability-contract.md).

The authority records a transaction ID, coordinator node and process incarnation,
generation, revision, participant group epochs, and one phase. Records use a
versioned envelope under `\x01txn-authority/v1/`. They survive with the meta
group's storage, snapshots and replicated log. Terminal records are retained:
absence must never mean abort, and deleting a tombstone would allow ID reuse.

## State machine

| Command | Preconditions | Result |
| --- | --- | --- |
| Begin | Previously unused ID and valid owner | Active, generation 1, revision 1 |
| Enroll | Exact owner, generation and revision; Active | Add a group and its epoch |
| Seal | Exact token; Active with participants | Preparing; participant set frozen |
| Commit | Exact token; Preparing; positive commit timestamp | Immutable Committed |
| Abort | Exact token; Active or Preparing | Immutable Aborted |
| FenceAbort | Exact token; different recovery owner | Increment generation, change owner, immutable Aborted |

All transitions execute under the same Raft state-machine apply lock. A stale
revision loses the comparison and makes no write. Repeating the same owner's
commit or abort is successful; an opposite decision or different commit
timestamp is rejected. A lost-response retry of another command, including
fence-abort, requires a linearizable read before deciding whether to retry with
a fresh token. Storage and decode errors stop apply rather
than masquerading as missing state. No transition uses a wall clock or lease.

Recovery need not prove death to *choose* abort for a fully tracked pending
transaction: an authoritative abort may cause a live transaction to fail, but
cannot coexist with an authoritative commit. This argument holds only after
every participant enforces the protocol. It does not authorize cleanup of
legacy transactions.

## Acknowledgement and participant boundary

Authority, participant and resolution commands require finalized level 5 and
peer mTLS at forwarded ingress. The production router reads the durable gate.
Authority success means its conditional transition was committed and applied;
user commit success additionally requires resolution on every enrolled
participant. Existing legacy transactions stay on their original protocol.

The implemented coordinator enrolls each participant through meta Raft **before
sending its first intent**, freezes the complete set, prepares every participant,
durably decides commit, and resolves from a fresh linearizable authority read.
Single-shard transactions, batches, deletes and savepoint repairs use the same
protocol. The protocol is pinned at a transaction's first write, so
activation cannot convert a live legacy transaction halfway through its work.

Participant records bind the owner, generation and shard epoch. Prepare compares
a canonical hash of the exact acknowledged intent-key set, then freezes all
writes and repairs, including changes to an existing key. Values are protected
by their original durable Raft acknowledgements. Batches retain the 256-operation
and 1 MiB bounds; oversized tracked single writes are rejected too. Namespace
adapters return logical intent keys, matching the coordinator's key set.

For commit, the terminal participant marker is an intent in the **same storage
transaction as the user writes**. One KV commit publishes them together. A crash
after the marker intent but before commit leaves a prepared record and invisible
intents; replay excludes the private marker from the key hash and retries the
atomic commit. A durable committed marker proves that the user commit completed,
so its retry needs no missing-intent assumption. Commit HLC timestamps must exceed
the applying participant's Raft index, keeping the terminal marker newer than
its earlier control versions. Abort persists a terminal fence before discarding
intents and repeats the discard after a crash. Abort also creates a fence on an
enrolled participant whose first write is delayed, preventing resurrection.

Legacy writes, prepares, commits, aborts, batched commits and local savepoint
repairs cannot mutate a tracked logical transaction ID. Historical recorded-ID
cleanup aborts are rejected globally, including replay: physical IDs cannot
prove ownership and a raw-meta abort could otherwise reach another group's
intents. Explicit legacy owner rollback remains available. Existing untracked
intents cannot be adopted or discarded by the new protocol. Unresolved tracked
records block migration fencing even when a savepoint cleared every user intent.
The apply path caches whether a group has participant records, avoiding extra
storage lookups on legacy writes in untouched groups. That guard is recovered at
startup and set before participant apply or snapshot installation; it never
clears while a store is live.

A local replica read or a caller-supplied record is not a decision certificate.
The resolver reads through meta ReadIndex, and forwarded participant writes,
prepares and resolutions independently pass another meta ReadIndex before the
receiver compares owner, generation, epoch, phase and the exact terminal record
to canonical authority. A forged or stale decision is rejected even from a
CA-authenticated peer. Without meta quorum, resolution is deferred.

## Bounded terminal recovery

The terminal decision atomically publishes a pending queue entry, progress record
and high-water index in the same KV transaction as the authority. The queue is
ordered by the meta Raft index. After a participant acknowledges resolution,
the resolver replicates that group's acknowledgement through meta Raft. A lost
response safely repeats participant resolution. Once all groups acknowledge,
the queue entry is deleted and compact completion proof is retained.

On meta leadership gain and periodic ticks, the existing recovery loop scans at
most 32 pending transactions and attempts at most 16 participant steps per pass.
It checks a two-second budget between RPCs; a single RPC can exceed that budget
within its own transport/retry timeout. A replicated conditional cursor persists
scan progress. Each cycle fixes its end index, so continuous incoming work cannot
prevent wrapping. Failed participants stay queued and later transactions get
another turn. Tracked replay runs before the legacy journal, so a failed legacy
record cannot starve this queue. Storage/decode failures stop the pass rather than hide corruption.
The existing recovery metric counts completed transactions; pass summaries and
participant failures are logged.

The worker never chooses abort. Active and Preparing records have no terminal
queue entry. Explicit internal recovery can conditionally fence-abort a tracked
pending record; commit and fence-abort serialize in meta Raft. This is not an
operator cleanup API and is not invoked by the automatic worker or intent janitor.
Unknown legacy intents remain protected.

Snapshots preserve pending work, acknowledgements, high water and cursor.
Installation rejects an omitted pending entry unless the incoming snapshot also
contains its durable completion proof, and rejects acknowledgement or decision
rollback. Logical backups and PITR omit these source-cluster control records:
they restore committed user versions on a new lineage, without resuming source
coordinators. A restore destination containing transaction records counts as
used, so their export exclusion cannot authorize an overlay restore.

## Compatibility and rollback

| Binary and cluster | Writers | Readers |
| --- | --- | --- |
| Previous binary, levels 1–4 | Existing formats | Existing formats |
| New binary, levels 1–4 | Existing formats; no new transaction records | Existing and tracked formats |
| New binary, finalized level 5 | Tracked writes for newly selected transactions; legacy transactions can finish | Authority, participant and resolution enforcement |

The target is sequential after `write-batching-v4`. Start, refresh and finalize
require fresh challenge-bound `txn_authority_version >= 1` reports from **every
voter and learner**, as well as earlier level prerequisites. Production nodes
omit this capability unless peer mTLS is configured. Before finalization,
`transaction_authority` stays false and no tracked records are emitted by SQL.
There is no configuration switch that bypasses finalization.

The level-5 target is explicit in its version-1 upgrade command. Levels 2–4 keep
the historical command representation; optional capabilities are omitted from
persisted base reports. Admission records also omit the new capability below
level 5, matching readers that discard the optional field. The sender re-probes a recipient before transmitting the
first level-5 control command or tracked command. During the level-5 upgrade and
after finalization, consensus senders and receivers require fresh reader evidence
for append, vote and every snapshot chunk, including cached/resumed streams.
Admission approval and retries enforce the same requirement before catch-up or
promotion. Restart without configured mTLS refuses this reader boundary.

Finalization is irreversible. Rollback to level 1 is allowed only before the
first finalized level and before any admission ledger exists. The current admin service refuses rollback across prior finalized levels.
Version-1 apply retains the old level-2/3 rollback result for deterministic mixed
readers; do not issue those later rollbacks through an older coordinator. The
level-5 all-reader fence permits the stronger replicated refusal at level 4. Replacing a
level-5 member with a readerless executable is unsupported and fenced by current
peers; the actual-binary gate also tests the pinned level-4 reader's startup
refusal against level-5 data.

## Verification and remaining work

Tests cover conditional decision orderings, stale tokens, namespace protection,
exact prepare sets, legacy bypass rejection, and snapshots preserving pending
work or proof of completion. The coordinator fixture uses two LSM-backed nodes,
one voter and one learner per group, real mTLS, production handlers, sequential
level-5 finalization and different data-group leaders. Coordinator pauses test
both commit/fence-abort orderings. SQL tests verify rows, indexes and savepoints.
An unavailable meta voter protects intents and blocks supplied decisions.

Process-exit tests stop after the durable decision, after the participant marker
intent before atomic user commit, and after acknowledgement. Reopen and automatic
terminal replay recover committed values. The timer-driven recovery test retains
one failed transaction while completing a later transaction and preserving a live
owner. Exclusive startup recovers unpublished Raft metadata as described in the
[durability contract](../reference/durability-contract.md#raft-log-purge).

The candidate [mixed-binary gate](../how-to/test-mixed-binary-upgrades.md) adds
level-4 reader refusal, level-5 finalization, production forwarded SQL, forged
decision rejection, SIGKILL/failover/restart, snapshot catch-up, and incompatible
admission/downgrade checks. Its result file records completion and blockers for
the exact candidate. This is not a power-loss, Byzantine, sustained-load or
concurrent-history linearizability proof.

Additional meta barriers and reader re-probes add latency; throughput has not
been qualified. Terminal authority, participant fences and completion proofs
remain retained. Their accumulated size is subject to the existing 128 MiB
snapshot limit, so this gate is not sustained-load qualification. Safe
reclamation, streaming snapshots and automatic abort of orphaned tracked owners
are separate work; legacy orphan cleanup remains disabled.
