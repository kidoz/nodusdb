# Transaction ownership and decisions

This is a staged design for safe transaction recovery. Authority, participant
fencing and coordinator integration are implemented, with activation available
only in isolated tests. Normal SQL transactions still use the existing
commit-only 2PC journal. Automatic intent cleanup remains disabled.

**Status:** participant and coordinator integration implemented; production
activation unavailable, 2026-11-05.

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

Both dormant command families can be applied through Raft for tests; every
public forwarded write rejects them, including authenticated peer traffic.
The production router has no activation path. Authority success means its
conditional transition was committed and applied; user commit success additionally
requires resolution on every enrolled participant.

The implemented coordinator enrolls each participant through meta Raft **before
sending its first intent**, freezes the complete set, prepares every participant,
durably decides commit, and resolves from a fresh linearizable authority read.
Single-shard transactions, batches, deletes and savepoint repairs use the same
protocol. The protocol is pinned at a transaction's first write, so future
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

A local replica read or a caller-supplied record is not a decision certificate:
the trusted resolver establishes the authoritative meta decision through
ReadIndex before constructing a resolution. Production activation must restrict
these messages to authenticated peer transport and compatible readers; accepting
an arbitrary JSON record is insufficient.

On coordinator failure, a meta leader can read and conditionally fence-abort a
pending record; a concurrent commit and fence-abort serialize in meta Raft.
If commit wins, recovery finishes commit. If abort wins, the delayed coordinator
cannot obtain a commit decision. Without a meta quorum, no new decision can be
made; availability yields to safety. Partial participant resolution must resume
from the retained decision. Existing untracked intents remain protected.

## Compatibility and rollback

| Binary and cluster | Writers | Readers |
| --- | --- | --- |
| Existing binary, levels 1–4 | Existing transaction formats | Existing formats; unknown authority command fails closed |
| New binary, levels 1–4 | Same existing transaction formats | Existing formats plus dormant authority records |
| Future activation | Requires a new finalized level and capability evidence from every voter and learner | Requires authority and participant enforcement on every member |

This stage adds no upgrade target, capability claim, configuration switch or
janitor writer. Level 4 is insufficient to enable it. Commands are appended to
the enum, preserving old variant ordinals, and authority and participant keys
are reserved against ordinary and epoch-aware mutations. Normal traffic produces
no new protocol records, so rollback has no new format boundary in this stage.
Test-created protocol data is not a supported old-binary input. Rejection of
historical recorded-ID cleanup can retain additional unknown intents; it never
converts them into a commit or an authoritative abort.
Activation will close rollback at its finalized format boundary and must update
node admission, snapshot capability checks and the mixed-binary matrix together.

## Verification and remaining work

Authority tests cover conditional decision orderings, concurrent single-voter Raft requests,
stale generations and revisions, invalid or future records, namespace protection,
ingress rejection, and process-exit recovery after acknowledged Raft decisions.
Participant and coordinator tests use two independent LSM-backed nodes over real
TCP, with one voter and one learner per group and different data-group leaders.
A test-only loopback write handler allows the dormant messages; production
ingress rejection is tested separately at every existing cluster level.

The tests pause the actual coordinator after prepare, let recovery fence-abort
through meta Raft, resolve all participants, resume the coordinator, and reject
its delayed mutations. The opposite ordering lets commit win and both resolvers
converge. SQL tests verify rows, unique indexes and rollback to savepoint across
data and meta participants. Stopping the sole meta voter leaves data intents
protected until the authority restarts. Process-exit tests terminate the whole
fixture after the durable decision, after the marker's WAL intent but before
commit, and after acknowledged user commit; reopening both stores recovers every
committed value. Snapshot tests retain participant fences and reject omissions.
These crash tests also exposed unfinished local applied-state writes blocking
restart. Exclusive group startup now recovers unpublished Raft metadata writes
as described in the [durability contract](../reference/durability-contract.md#raft-log-purge),
while preserving all uncertain user intents.
This is not a deterministic simulation, a multi-voter partition test, a real
production-binary activation test, or power-loss evidence.

The next slice is a new finalized upgrade level, reader capability checks on
every voter and learner, authenticated forwarding, admission and snapshot
compatibility, and an expanded mixed-binary matrix. Performance of the additional
meta barriers and tracked-group legacy lookups is not qualified. Only after that
gate may automatic recovery consider tracked pending transactions; legacy intents
remain protected. Safe reclamation of retained decisions is separate future work.
