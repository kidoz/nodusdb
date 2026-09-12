# Shard migration protocol

**Status: dormant.** A durable pre-copy coordinator and epoch-aware SQL write
paths are implemented and tested in isolated fixtures. Public initialization,
split, merge, and rebalance remain disabled. Production SQL submission and
internal HTTP client-write ingress reject the new command variants; there is no
runtime switch to bypass verified cluster compatibility.

## Coordinator journal

The meta group stores a version 2 journal per table. Its immutable plan contains
an operation UUID, the expected complete shard map (or typed absence), every row
source group plus `shard-meta` for secondary indexes, source epochs, and
destination group IDs. The meta group must be the first source. Plans have
bounded size and participant counts. Plan acceptance, beginning acquisition,
and declaring fencing complete compare routing at Raft apply. Descriptor order
alone is not a routing change; malformed coverage and metadata errors fail.

The phases are `Planned`, `Fencing`, `Fenced`, `Cancelling`, and `Cancelled`.
Acquired and cancelled participant acknowledgements are persisted separately.
`Fencing` records intent; only acknowledgements from every planned source allow
`Fenced`. Cancellation addresses **every** source, including one whose acquire
committed but whose response or acknowledgement was lost. `Cancelled` requires
all cancellation acknowledgements. Only a cancelled journal can be replaced by a
new operation. No phase copies data, publishes routing, or removes a source.

`MigrationCoordinator::resume(table)` reloads the journal after a meta-leader
ReadIndex barrier. Each call advances at most one participant and has a
five-second deadline covering reads, submission, and acknowledgement. A timeout
has an uncertain outcome: a later call retries from persisted state. Operation,
phase, source, and epoch checks at apply make overlapping or stale coordinators
safe. Changed routing requests cancellation. The service is dormant, operates
on a known table, and has no production journal-discovery worker or start API.
Its production transport also cannot forward new commands to remote leaders
while ingress is disabled.

## Participant fencing and cancellation

Each participant retains a version 1 fence record: operation UUID, group-local
epoch, and whether writes are closed. Epochs never decrease.

- `Acquire(expected_epoch)` executes under the group's Raft apply lock. It
  rejects competing owners, stale epochs, overflow, and live intents, including
  prepared transactions. The meta group also refuses while durable 2PC decision
  records await recovery. A successful acquire advances the epoch and closes
  writes. A retry of the same closed operation does not advance again.
- Cancellation reopens an acquired source at its advanced epoch. For a source
  never acquired, `CancelParticipant` waits for quiescence, then advances and
  opens the epoch. This seals delayed acquire RPCs permanently. If a newer
  operation already advanced beyond the old expected epoch, cancellation leaves
  that newer owner untouched.
- Cancellation can remain pending while transactions or decisions need recovery;
  it does not discard accepted writes or infer success from a missing response.
- The raw meta engine conservatively sees pending intents in other physical
  namespaces sharing its backing store. This can delay fencing. Unsupported
  intent-inspection implementations and storage errors fail closed.

The meta group is fenced first, preventing new index writes and 2PC decisions
before other source epochs advance. This is a conservative pause of the whole
meta write domain, not table-scoped online availability. Orphan transaction
resolution and uncertain 2PC outcomes still require further work.

## SQL writes and savepoints

`RaftKvEngine` captures a participant epoch on its transaction's first write to
that group. Successful savepoint clears update the acknowledged write set but
retain that epoch until finalization. Puts, deletes, prepare, commit, and
replicated savepoint repair present the captured epoch. A transaction that
clears every intent cannot adopt a new epoch after fencing or cancellation.
Row and secondary-index mutations use the same storage path.

Version 2 prepare distinguishes a participant intentionally emptied by completed
savepoint repairs from one unexpectedly missing live intents. It still checks
the original epoch. Schema and shard-map mutations are rejected while the meta
participant is closed. Abort remains available to drain old transactions.
Reserved migration keys cannot be changed by ordinary writes or repairs.

Before activation, ordinary writes retain existing wire formats. Legacy
savepoint repair remains local, but now takes the **same state-machine apply
lock** as fence acquisition and refuses any existing fence epoch. In protocol
fixtures, savepoint Put/Delete/Clear repairs are replicated with `EpochRepairV2`.
Production replicated savepoint rollout remains part of compatibility activation.

Epoch-aware 2PC decision records use envelope version 2 and persist the exact
participant epochs. Recovery uses those epochs without restamping. Existing
legacy JSON records remain readable and normal workloads preserve their bytes.
Unknown versions, invalid epoch sets, decode errors, and scan errors stop
recovery rather than silently skipping decisions. These changes do not establish
complete cross-shard atomicity during uncertain decision/commit failures.

## Persistence and snapshots

Version 1 journals and fences remain readable. Version 2 journals use the
reserved `\x01migration/v2/table/` prefix and their own envelope. A table with a
version 1 journal requires explicit reconciliation before version 2 planning.
New command variants are appended; existing command and successful-response
shapes remain compatible.

Control updates commit through the owning participant's KV/WAL path before Raft
returns success, using the applying log index as their timestamp. Deterministic
control transaction IDs let replay remove only that entry's interrupted control
intent. Storage failure does not advance the applied watermark.

Snapshot build holds the owning group's apply lock. Build/install errors
propagate, and successful installation alone advances its watermark. Both
journal formats are included and validated; unknown versions, removal of local
control state, or an epoch rollback are rejected. Atomic installation across
process crashes and namespace isolation between groups sharing one backing
engine remain activation prerequisites.

## Compatibility

| Binary / cluster state | Existing formats | New migration formats |
| --- | --- | --- |
| Earlier binary | Existing formats only | Unsupported |
| New binary, current cluster | Reads and writes existing formats | SQL and HTTP ingress reject submission |
| Isolated protocol fixture | Reads legacy and V1 records | V2 journal/decisions and epoch-aware SQL paths exercised |
| Future verified activation | Requires compatibility proof | Member verification, authenticated forwarding, discovery, and rollout still required |

The test-only writer constructor is compiled only into unit-test builds; it is
not a Cargo feature or server configuration option. No mixed-binary upgrade is
claimed. Rollback to a binary unable to enforce these formats after deliberate
protocol writes is unsupported.

## Evidence

The server suite exercises:

- Restart from a real LSM directory after a subprocess exits immediately after
  quorum acknowledgement of acquisition, before the coordinator persists its
  participant acknowledgement; recovery finishes fencing from that journal.
- Three-node real TCP replication and leader loss at the same boundary; the new
  leader resumes fencing and cancellation. Production HTTP V2 ingress remains
  rejected in that cluster.
- Lost acknowledgement timeouts, changed routing, invalid source acknowledgements,
  pending transactions/decisions, and delayed acquire after cancellation.
- Durable SQL row and unique-index rollback across distinct Raft groups, including
  UPDATE, DELETE, INSERT, savepoints, empty-participant prepare, fenced DDL, and
  stale-epoch rejection after all intents were cleared.
- Concurrent legacy savepoint repair and acquire; they cannot both succeed.
- Epoch-preserving 2PC recovery, old-format bytes, V2 snapshot transfer and unknown
  versions, plus the earlier fence/control I/O and snapshot failure tests.

These are real Raft/TCP, subprocess, and injected-boundary tests, not a
linearizability proof or deterministic simulation. See
[run the test suites](../how-to/run-the-test-suites.md).

## Remaining work

Verified member capability negotiation and writer/forwarding activation,
automatic journal discovery, orphan/uncertain 2PC recovery, reader and backup
retention, and snapshot namespace/crash safety remain prerequisites. Destination
readiness, durable copy checkpoints, data validation, conditional map/placement
publication, and source cleanup are not implemented by this pre-copy slice.
Public shard mutations stay disabled until those conditions are satisfied.
