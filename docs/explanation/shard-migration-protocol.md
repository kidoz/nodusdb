# Shard migration protocol

**Status: partially activated.** The durable pre-copy coordinator, V3
copy/cutover journal and driver, epoch-aware SQL write paths, authenticated
mTLS forwarding, and meta-leader journal-discovery worker are implemented and
tested. Shard **init, split, merge, and rebalance are enabled** once the durable
upgrade authority finalizes migration level 3. Pre-finalization, SQL submission
and HTTP ingress reject migration commands and public operations return 501;
there is no runtime switch to bypass cluster compatibility. Data groups
currently replicate to every cluster member. Source-group decommission at
`Done` is driver-local: replicas on other members remain inert until
roster-wide teardown is reconciled. Sections marked as design remain the agreed
target.

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
safe. Changed routing requests cancellation. There is no start API beyond the
(fenced) admin endpoints; a bounded journal-discovery worker on the meta leader
(`migration_coordinator::journal_discovery_loop`, wired beside the 2PC
recovery loop) finds and resumes interrupted V2 and V3 journals after restart
or election, driving each by a bounded number of steps per tick. The worker is
inert until the durable migration cluster level is finalized, and forwarding of
migration-protocol commands to remote leaders requires the mTLS peer transport
(see the activation design below).

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
meta write domain, not table-scoped online availability. Orphan and uncertain
2PC outcomes are resolved, never inferred, before an affected participant's
epoch advances: fencing refuses while any pending intent or undecided decision
exists, a decided commit is re-driven by meta-leader recovery, and an intent
whose transaction has no decision record past the abandoned-transaction TTL is
resolved by the janitor's replicated abort (the composition is proven by
`migration_tests::coordinator::orphan_intent_blocks_fencing_until_janitor_resolution`).

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

Snapshot build holds the owning group's apply lock. The meta snapshot excludes
physical `shard-*\0` data namespaces and node-local Raft/clock records. It includes
routing metadata, 2PC decisions, migration journals, and the full existing durable
catalog representation (including authorization and role memberships). Data snapshots
operate through the owning namespace. This relies on the server's `shard-*` naming
convention; arbitrary custom namespace names are not a supported meta scope.

Installation validates the entire bounded payload before replacing any rows. Unknown
versions, malformed/truncated records, duplicate or unordered keys, foreign namespaces,
missing migration state, epoch rollback, and applied-index regression are rejected.
A legacy catalog header is converted using the previous import semantics if no durable
catalog row is present: local authorization is retained. When present, the durable
catalog row is authoritative; the header remains for wire compatibility.

The storage engine publishes the replacement, catalog bytes, and applied/snapshot
pointers together using existing SSTable, WAL, and manifest formats. Unrelated groups'
versions, pending intents, votes/logs, and the node clock are retained. Catalog validation
precedes publication; readers of its in-memory projection are serialized while the
checkpoint and projection change. The server reserves its clock above incoming
committed versions before installing them.

The previous snapshot descriptor is durably invalidated before replacing `current.snap`.
If the process exits after the KV checkpoint but before publishing the file, restart
finds complete installed state and its applied pointer, with no servable snapshot. It
can rebuild the file from that state. File and directory sync errors propagate; an
uncertain storage-manifest publication disables engine reads/writes until reopen.

### Snapshot limits

- The NSNP version 1 wire format is unchanged. It cannot encode pending intents,
  user tombstones, or retained user MVCC history. Builds **refuse those states** before
  replacing a snapshot or authorizing log purging. Latest internal metadata values
  remain representable. The new NSNP v2 codec handles these states. Its production writer requires
  explicit verified finalization through the [upgrade authority](upgrade-authority.md). OpenRaft treats a snapshot-build error as fatal to the group, so
  encountering one of these states during an automatic build can stop that group.
  General production snapshot operation therefore requires that finalization.
- Wire input is limited to 128 MiB. Checkpoint construction materializes bounded
  state, including unrelated groups in a shared LSM, and rejects a checkpoint above
  the 128 MiB accounting limit. Temporary copies and allocation overhead make actual
  peak memory higher. The shared engine pauses operations during replacement.
- Superseded storage files are retained; cleanup needs a retention policy. Process-crash
  tests assume an intact authoritative manifest. Recovery by scanning files after
  losing/corrupting that manifest is not safe with superseded checkpoint files.
- Installation starts a new WAL lineage and atomically records a local recovery
  generation and WAL floor. Source-bound backup operations reject incremental
  ancestry and PITR extension across generations, requiring a new full backup.
  Offline planners enforce the boundary once it is published to the repository;
  publication is not atomic with the storage checkpoint. See the
  [recovery contract](mvcc-snapshots.md#backup-recovery-boundary) for this limit.
- Version 1 carries no data-group identity. Correct group routing still depends on the
  existing Raft transport boundary. This change does not certify a mixed-binary
  transfer, live-reader continuity through installation, or power-loss behavior.

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


Snapshot compatibility is separate from migration activation:

| Reader | Snapshot/checkpoint behavior |
| --- | --- |
| New binary, old NSNP v1 file | Reads representable payloads; rejects old meta files containing data namespaces; retains legacy catalog-import semantics when the durable catalog row is absent. |
| New binary, production writer | Same NSNP v1 framing; strict validation and atomic installation as described above. |
| New binary with trusted compatibility provider | NSNP v2 only after finalization at version 2 or later and v2 support reported for every voter and learner. Production supplies the durable meta authority; membership is frozen during and after activation. |
| Previous binary, new checkpoint on disk | No new SST/WAL/manifest/catalog envelope is emitted; existing readers understand the formats. |
| Previous binary receiving a new snapshot | Wire framing is readable, but its old destructive installer is not made safe by this change. Mixed-version installation is not certified. |

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
- Abrupt subprocess exits before/after the LSM manifest swap and before/after the
  Raft storage checkpoint; reopen verifies old or complete rows/indexes and pointers.
- Meta snapshot transfer/reopen with catalog role membership, routing/decision bytes,
  another shard's pending index write, and local clock preservation.
- Real-TCP lagging learners forced through v1 and v2 snapshot transfer after log purge,
  followed by LSM/Raft reopen; v2 retains historical reads and an intent subsequently
  committed through Raft. Checksum, group and metadata mismatches fail before install.
- V2 installation interrupted before/after checkpoint, retaining history, intent identity
  and the recovery generation on reopen; cross-generation backup rejection and a new
  full backup restored into a fresh persistent store.

These are real Raft/TCP, subprocess, and injected-boundary tests, not a
linearizability proof or deterministic simulation. See
[run the test suites](../how-to/run-the-test-suites.md).

## Remaining work

Verified member capability negotiation and writer/forwarding activation,
automatic journal discovery, orphan/uncertain 2PC recovery, reader and backup
retention, post-finalization membership admission, and repository boundary publication
coordination remain prerequisites. The [v2 codec and recovery boundary](mvcc-snapshots.md)
are implemented with the documented limits. Destination
readiness, durable copy checkpoints, data validation, conditional map/placement
publication, and source cleanup are not implemented by this pre-copy slice.
Public shard mutations stay disabled until those conditions are satisfied.

## Activation design (target, not implemented)

This section defines how the dormant protocol becomes a complete, safe shard
migration feature. It is the agreed design; the implementation lands behind the
existing fences and each operation is enabled only after its fault matrix
passes. The design uses the durable [upgrade authority](upgrade-authority.md)
for activation; the legacy in-memory `nodus_upgrade` model never authorizes
persisted or network format changes.

### Version and command matrix

Activation introduces a third journal envelope and a third cluster-compatibility
level. Old binaries fail closed on both.

| Record / surface | Version | Old-reader behavior |
| --- | --- | --- |
| V1 journal `\x01migration/v1/table/{id}`, participant fence `\x01migration/v1/fence` | envelope 1 | readable by all current binaries |
| V2 journal `\x01migration/v2/table/{id}` (pre-copy fencing) | envelope 2 | decode bails on non-2; V1 journal blocks V2 planning and vice versa |
| V3 journal `\x01migration/v3/table/{id}` (full copy/cutover phases) | envelope 3 | decode bails on non-3; a V1/V2 journal must be explicitly reconciled to completion or cancellation before V3 planning |
| 2PC decision `\0txn2pc\0{txn}` | legacy JSON or envelope 2 | legacy bytes still decode; unknown versions stop recovery |
| Routing `meta:shard_map:*`, `meta:shard_placements` | envelope 1 | unknown future versions are a hard error; conditional publication compares the expected envelope-1 content |
| Upgrade authority `\x01upgrade/v1/state` | envelope 1, `cluster_version` 3 domain | `decode` hard-errors on unknown version or cluster level; additive capability fields use serde defaults so old readers persist identical records |

Cluster-compatibility levels and what they unlock:

| `cluster_version` | Target | Unlocks |
| --- | --- | --- |
| 1 | (initial) | existing formats only; all migration ingress rejected |
| 2 | `snapshot-v2` | NSNP v2 snapshot writer (existing) |
| 3 | `migration-v3` | V3 journal writes, epoch-aware SQL commands, authenticated V2 leader forwarding, and the per-operation enablement ladder below |

`CapabilityV1` gains an optional `migration_version` report field (serde
default, like `admission_version`). Finalization to level 3 requires a fresh
report from every voter and learner with `migration_version >= 4`, the
membership freeze, and the existing preflight: drained intents, no undecided
2PC records, and no in-flight migration journals. Post-finalization membership
changes continue through the admission ledger; an admitted member must report
`migration_version >= 4` before promotion.

### Per-operation enablement ladder

The blanket 501 on `POST /api/v1/shards/{table}/{init|split|merge|rebalance}`
is retired one operation at a time, in this order, each only after its fault
matrix (below) passes on a finalized level-3 fixture:

1. **init** — first-shard initialization (**enabled**). A table without a shard
   map keeps its rows on the meta group, so init is not necessarily empty: its
   copy phase moves the table's row keys off meta (secondary-index entries stay
   on meta permanently; routing never moves them). Even an empty table races
   concurrent INSERTs, so init plans through the V3 journal (meta fence first)
   and publishes the map conditionally. `POST /api/v1/shards/{table}/init`
   answers `200` when the synchronous drive completes, `202` when the operation
   is journaled and left for the meta leader's discovery worker, `409` when a
   map or an active migration already exists, and `501` before level 3
   finalizes.
2. **split** — one source range into two children (**enabled**).
   `POST /api/v1/shards/{table}/split?shard={id}&key={bytes}` takes the raw
   row-key split point (`{table}:{pk}` space), plans through the V3 journal
   (meta fence, source fence, journaled copy checkpoints, exact-digest
   validation, conditional publication), and answers 200/202/409/501 exactly
   like init. The children inherit the source's placement.
3. **merge** — two adjacent sources into one child (**enabled**).
   `POST /api/v1/shards/{table}/merge?left={id}&right={id}` plans through the
   V3 journal like split, with both sources' multisets copied and validated;
   the merged shard inherits the sources' placement.
4. **rebalance / move** — placement reassignment (**enabled**). Data groups
   replicate to every cluster member, so a rebalance moves primacy, not bytes:
   `POST /api/v1/shards/{table}/rebalance?nodes=1,2` is a guarded, atomic
   placements write through the meta group (round-robin by start key over the
   given live roster members), refused while any migration journal could still
   drive routing changes. If placements later stop implying universal
   replication, this rung must be revisited as a real data-movement operation
   through the V3 journal.

Each operation remains authenticated, authorized (`ManageShards`), and audited
exactly as the fenced endpoints are today, including the audit of denied
attempts before activation. Until level 3 finalizes, every behavior stays as
documented in the dormant sections above: admin 501, router submission
rejection, and HTTP ingress 501.

### V3 journal phases and acknowledgement boundaries

The V3 journal extends the V2 fencing state machine. Every transition is a Raft
apply on the meta group with operation, phase, routing, and epoch revalidation;
every participant interaction persists its acknowledgement separately before
the phase can advance. Routing drift at any check requests cancellation rather
than completing.

`Planned → Fencing → Fenced` — as implemented for V2 (meta first, complete
source coverage, quiescence including prepared transactions and durable 2PC
decisions).

`Fenced → Preparing → Copying → Validating → Publishing → Completing → Done` —
the new copy/cutover spine:

- **Preparing**: destination groups are formed replicated (placement durable in
  the meta group first), and the destination clock is reserved above every
  source's visible committed timestamp. The acknowledgement boundary is the
  destination group reporting a durable, serving replica set.
- **Copying**: a range-scoped snapshot plus tail writes move the exact key
  multiset at a protected read timestamp; durable copy checkpoints in the
  journal bound resume so a coordinator restart continues from the last
  checkpoint instead of restarting the copy. GC is blocked below the protected
  timestamp for the duration (reader/backup retention coordination).
- **Validating**: destination and source key/version multisets are compared
  exactly (per-range counts and versioned digests); a mismatch cancels, never
  publishes.
- **Publishing**: the shard map and placements flip by conditional apply —
  the apply compares the expected current map content and journal phase and
  fails closed on any drift. Before the flip the driver idempotently
  re-prepares every destination group, so a driver restarted after
  `Preparing` (which may physically lack a destination that received no
  copied rows) never publishes routing to an unserved group. Once the
  publication commits, cancellation is no longer possible; the operation
  can only complete.
- **Completing**: residual writers observe the new map (fences open on the new
  epoch), and every participant acknowledges the cutover.
- **Done**: the source groups are decommissioned and their ranges purged —
  only after the destination data and the replicated routing decision are both
  durable. Source removal is journaled like every other step, one still-hosted
  source group per driver step.

`Cancelling → Cancelled` — reachable from every phase before `Publishing`
commits, addressing every source including ones whose acquire/ack was lost;
sealing semantics are the V2 ones (a cancelled source's delayed acquires are
permanently stale; a newer operation at a higher epoch is untouched). After all
sources are sealed, cancellation conditionally restores the pre-operation
placements — the preparation step adds destination placements before group
formation, so cleanup must undo that flip — then removes the prepared
destination groups, each step journaled. `Cancelled` requires all source seals,
the restored placements, and every prepared destination's cleanup
acknowledgement.

Crash and recovery rules follow the existing coordinator invariants: the
journal and acknowledgements are durable before any external effect is trusted;
`resume` after a meta-leader ReadIndex barrier advances at most one bounded
step per call; a timeout is an uncertain outcome retried from persisted state;
orphan and uncertain 2PC outcomes are resolved (never inferred) before the
affected participant's epoch can advance. A bounded journal-discovery worker on
the meta leader finds and resumes interrupted operations after restart or
election — there is no start API beyond the admin endpoints.

### Forwarding and membership/backup interactions

- **Authenticated V2 forwarding**: leader forwarding of migration-protocol
  commands moves onto the peer mTLS transport (the upgrade-probe path is the
  precedent), requires the finalized level-3 record, and re-checks the
  recipient's reported `migration_version` before sending. The shared-listener
  bearer-token path never carries migration commands. Rejected forwards never
  enter the dedup cache (existing behavior). Consequently a multi-node cluster
  must configure `cluster.tls` before finalizing level 3: without the mTLS
  peer listener a migration-protocol command whose target group's leader is
  remote cannot be forwarded and fails closed (single-node clusters never
  forward and are unaffected).
- **Membership**: the upgrade authority freezes meta membership during an
  upgrade; symmetrically, migration planning is refused while an upgrade or
  admission ledger operation is in flight, and upgrade preflight (which already
  scans `\x01migration/`) refuses to start while any migration journal is
  active.
- **Backup**: migration state remains excluded from backup export and replay.
  Backup and restore operations serialize against migration through the meta
  group: a backup publication waits for (or cancels) in-flight migrations, and
  migration planning is refused while a backup holds a protected timestamp over
  the affected ranges, so neither invalidates the other's reads.
- **Rollback**: rollback to a binary that cannot enforce these formats after
  deliberate V3 protocol writes is unsupported, matching the existing rule for
  V2; the level-3 finalization record makes that boundary durable and visible
  to old binaries (they fail closed on the unknown cluster version).

### Fault matrix per operation

Before an operation's 501 is retired, it must pass, with no lost, duplicated,
or prematurely visible keys: process kill before and after every durable
boundary above; concurrent reads and writes (including savepoints and prepared
transactions) through copy and cutover; leader loss of the meta group and of
any participant mid-phase; network partition and heal between coordinator and
participants; request timeout and idempotent retry at every boundary;
destination-node restart and snapshot catch-up during copy; cancellation at
every cancellable phase; and rollback attempts after publication (must fail
closed). Suites: migration unit/fault fixtures, integration, real-process
three-node cluster, partition, loom, and the candidate mixed-binary matrix.

**rebalance coverage (retired 2026-10-21):** the migration-in-flight refusal
and terminality tracking by
`migration_tests::discovery::active_migration_tracks_journal_terminality`;
containment, roster validation, assignment response, racing-rebalance
convergence, and data untouchedness by
`tests/integration/tests/shard_rebalance.rs`. No crash matrix applies: the
operation is one atomic Raft write with no durable intermediate state.

**merge coverage (retired 2026-10-21):** the two-source exact-multiset driver
test (`v3_driver_merge_moves_the_exact_multiset`), journal-level cancellation
from every phase, and the SQL-visible end-to-end proof
(`tests/integration/tests/shard_merge.rs`). Multi-node leader-loss mechanics
are shared with split verbatim (same driver and discovery path); merge's
distinct risk — two sources — is covered at the driver and journal levels.

**split coverage (retired 2026-10-21):** everything init covers, plus the
SQL-visible exact-multiset proof across cutover
(`tests/integration/tests/shard_split.rs`), a stale-plan fail-fast at apply
(`v3_plan_with_stale_meta_epoch_is_rejected_at_planning`), and meta-leader loss
mid-copy on a real three-node mTLS cluster with the surviving leader's
discovery pass completing the operation
(`migration_tests::cluster_v3::split_survives_meta_leader_loss_mid_copy`).

**init coverage (retired 2026-10-20):** journal-level crash/resume and
cancellation matrices are covered by the V3 journal and driver suites
(`migration_tests::v3`, `migration_tests::driver`, including a subprocess
crash between copy checkpoints); discovery-worker resume of an interrupted
operation by `migration_tests::discovery`; the enablement path, the
insert/init race, repeat-init conflict, and movement of rows written before
the map existed (with secondary-index continuity) by
`tests/integration/tests/shard_init.rs`; and the pre-finalization 501,
routing/data preservation, and audit of denied attempts by
`tests/integration/tests/shard_guard.rs`. Init places the first shard on the
requesting node, so the multi-node leader-loss and partition rungs of the
matrix are exercised with split, which has real source data movement.
