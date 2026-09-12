# Shard migration protocol

**Status: dormant.** The durable pre-copy journal and participant write fencing
described here exist and are tested. They do not enable initialization, split,
merge, or rebalance. The SQL writer and the internal HTTP client-write endpoint
reject the new protocol until cluster-wide compatibility checks and a recovery
coordinator exist, and there is no configuration switch to bypass that. Protocol
tests submit commands directly to Raft.

This page explains what the foundation guarantees, and — more importantly —
what it deliberately does not.

## State and invariants

The metadata group stores one active migration record per table. Its immutable
plan identifies an operation UUID, source and destination groups, and expected
source epochs. A retry must match the entire original plan; a conflicting
operation is rejected outright rather than merged.

The available phases are `Planned`, `Fencing`, and `CancelRequested`. A
cancellation request cannot transition back to fencing. `Fencing` describes
coordinator *intent*, not proof that every participant is fenced — a distinction
that matters, because treating intent as proof is how migrations lose data. This
slice has no copy, publication, cleanup, or terminal transition at all.

Each participant stores its own fence record: the operation UUID, a
monotonically increasing epoch, and whether writes are closed. These are group
epochs, not a new table-wide catalog generation.

- `Acquire(expected_epoch)` runs under that group's Raft apply lock. It rejects
  competing owners, stale epochs, overflow, and any remaining intents. Prepared
  transactions retain intents and therefore also prevent acquisition. The
  command makes a bounded attempt: it does not wait, and it never discards
  writes it has already accepted.
- A successful acquisition durably advances the epoch and closes writes. A retry
  while still closed succeeds without advancing again, so a retried command is
  not a second epoch bump.
- `Release(operation_id, epoch)` reopens the same source at the advanced epoch
  for cancellation. It does not roll the epoch back. A delayed acquisition after
  release, or a release from an earlier operation, is rejected.
- Epoch-tagged writes check the epoch and the open state before put, delete,
  prepare, or commit. Legacy untagged mutations are rejected once a fence record
  exists. Aborts remain available. Reserved control keys cannot be mutated by
  ordinary put or delete commands. A write conflict returns a rejection rather
  than halting Raft — a stalled consensus group is a worse outcome than a
  rejected write.

Fence acquisition requires an engine that can inspect pending intents; the trait
default fails closed. The memory and LSM engines inspect existing intent keys
without collecting them, and namespace wrappers qualify the prefix. The raw
metadata engine conservatively sees intents in other namespaces that share its
backing store, which can delay acquisition — a false positive that costs time,
never correctness.

## Persistence, replay, and snapshots

Records use envelope version 1 under a reserved key prefix and are included in
the existing snapshot data range. Unknown envelopes and decoding errors fail
closed. Migration and epoch-write commands are explicit new variants appended to
the command enum; existing command shapes remain readable.

A record update commits through the participant's existing key-value and WAL
path before Raft returns success, and its timestamp is the applying log index. A
deterministic control transaction id lets replay remove only that entry's
unfinished control intent before retrying — user intents are never touched by
this cleanup. Storage failures stop apply without advancing the applied
watermark, so a failed write cannot be forgotten.

Snapshot construction holds the group's apply lock for the duration of the
build, so the snapshot header and the fence state agree. The cost is that writes
pause while a snapshot is built. Scan, write, and orphan-cleanup errors
propagate; installation advances the applied watermark only after the snapshot
is successfully written and published. Incoming control records are validated,
and a snapshot that would remove local migration state or roll a participant
epoch backwards is rejected.

This does not establish atomic snapshot installation across process crashes, nor
an atomic snapshot across several groups sharing one backing engine. Both remain
prerequisites for activation.

## Compatibility

| Binary / cluster state | Existing commands and records | New migration writes |
| --- | --- | --- |
| Earlier binary | Existing formats only | Unsupported |
| New binary, current cluster | Reads and writes existing formats | Rejected at SQL submission and HTTP client-write ingress |
| Explicit protocol test fixture | Reads version 1 migration records | Direct Raft submissions exercise the dormant protocol |
| Future verified activation | Not implemented in this slice | Requires authoritative member compatibility and writer rollout |

Normal workloads emit none of the new records. Response errors are optional and
omitted on success, preserving the legacy response shape, and rejected forwarded
requests are not cached as successful retries.

Rollback after writing migration records is unsupported: older binaries cannot
enforce these fences. No mixed-binary upgrade or activation is claimed by the
tests.

## Evidence

Tests cover conflicting plans, pending and prepared transactions, stale
mutations, monotonic epochs after cancellation, control-intent replay, snapshot
transfer, unknown versions, and failed control commits and snapshot operations.
A subprocess exits without shutdown immediately after a single-node Raft
acknowledgement; reopening its LSM directory recovers the journal, the closed
fence, and the committed row. A three-node test uses real TCP replication,
concurrent contenders, leader loss, stale-request rejection, and a current-epoch
write after cancellation.

See [run the test suites](../how-to/run-the-test-suites.md) for how to run them.

## What has to happen before activation

A coordinator must persist participant acknowledgements, resume fencing and
cancellation after a crash, validate the plan against authoritative routing, and
prove every source is fenced before any copying starts. It must bind destination
readiness, retained MVCC history, two-phase-commit resolution, and map and
placement publication into one conditional protocol.

Snapshot namespace isolation and crash-safe installation remain prerequisites,
as do epoch-aware SQL writers and verified cluster feature gates before network
submission can be enabled. The savepoint intent-repair path still writes locally
outside Raft apply, and must join the serialized write protocol before a
coordinator can rely on the quiescence check.

Public shard mutations stay disabled throughout. What exists today establishes
the participant command contract — not a complete fence around every SQL
execution path.
