# Snapshot upgrade authority

The production server can enable NSNP v2 after explicit, verified finalization.
It starts at compatibility level 1 and writes v1 snapshots until then. This
first authority protocol supports the target **`snapshot-v2`** and freezes meta
membership during the upgrade and after finalization. It does not enable shard
migration, a new SST/WAL format, or general rolling binary upgrades.

## Authority and evidence

The meta group replicates `UpgradeControlV1` operations with an expected state
revision, exact applied membership (including its log ID), a fresh UUID challenge,
and capability reports. Start, refresh, finalize and rollback commit a version 1
JSON record at `\x01upgrade/v1/state`. Its revision is the applying log index.
KV/WAL commit precedes acknowledgement; replay after an interrupted applied-pointer
write recognizes the already committed revision. Storage errors stop apply;
validation errors return a failed command result. Startup and snapshots reject
corrupt/unknown authority records, and snapshot installation cannot lower the
revision or finalized level or omit an existing authority record.

Status uses a leader ReadIndex barrier and reads durable state directly. Followers
return an actionable error; send upgrade operations to the meta leader. The old
in-memory coordinator is no longer part of server or state-machine wiring.
Legacy `UpgradeStart`/node-name/finalize commands cannot activate features, and
all upgrade commands are rejected by generic `/raft/{group}/write` ingress.
The authorized admin service submits directly to its verified local leader.

The leader probes every voter and learner at its applied membership address.
Remote probes require the configured peer mTLS transport: only the cluster CA
is trusted, server hostname verification remains enabled, redirects are disabled,
and HTTP downgrade is prohibited. Reports must echo the fresh challenge and
match the requested numeric node ID, supported authority version and snapshot
version. Missing, duplicate, unknown, stale or incompatible reports cannot enable
v2. Start also probes every member, because every recipient must already read the
new authority command before it enters the Raft log.

This assumes trusted, non-Byzantine cluster peers and correct CA/certificate
issuance. The protocol binds a certificate-validated endpoint to its reported ID;
it does not add a node-ID certificate extension or remote binary attestation.
Tests use a shared test certificate across loopback nodes. Actual deployments
must protect peer identities and their issuance process.

Preflight reports observe pending user intents, unresolved meta 2PC decisions and
migration records on each node. Start/finalize require all reports ready. Physical
node-local Raft/clock records, including those inside shard namespaces, are excluded
from the intent check. These observations are replicated in the command: apply
never makes a consensus decision from each replica's unrelated local shard state.
Preflight is a readiness observation, not a distributed transaction fence. Snapshot
v2 itself can retain intents accepted after the observation. Migration activation
continues to require its separate recovery and retention prerequisites.

## Operator flow

Use the existing authenticated `/api/v1/upgrade` API on the meta leader:

1. `GET /api/v1/upgrade` to inspect phase, revision, cluster level, membership,
   reports and the `mvcc_snapshots` feature gate.
2. `POST /api/v1/upgrade/start?target=snapshot-v2` to verify readers and persist
   `RollingNodes`. Other target strings are rejected.
3. `POST /api/v1/upgrade/node-upgraded?node=<numeric-member-id>` to refresh reports
   from **all** members and persist `ReadyToFinalize`. The name supplied by the
   caller is not evidence that a binary was upgraded.
4. `POST /api/v1/upgrade/finalize` to run fresh verification and commit level 2.
   Existing authorized admin middleware continues to enforce permission and audit.

Rollback before finalization clears the session/reports, keeps a newer revision
and returns to level 1. Finalization closes rollback. Repeated/stale commands
cannot reset the phase or reuse an old challenge. A timed-out operation has an
uncertain outcome; inspect status before retrying. Membership changes during
probing invalidate the operation at apply. Joint membership cannot start/finalize.

Meta join admission shares the leader's upgrade lock and rejects new learners or
voters while the authority is active or finalized. Existing voters may retry
join without changing membership. Data-group reconciliation can converge only
to the frozen verified meta roster. Adding or removing meta members after
finalization is deliberately unsupported in this slice; do not edit authority
records to bypass it. A future replicated admission protocol must preserve the
minimum reader requirement across new members, restarts and address changes.

## Snapshot serving and recovery

Every production group reads the durable meta authority for writer eligibility.
The existing per-group membership gate must also pass. The sender re-probes the
recipient before **each v2 chunk**, including a cached snapshot and resumed stream;
an unknown resumed stream is treated conservatively as v2. An old/missing endpoint,
identity mismatch, insecure transport or incompatible response prevents that chunk
from being sent. Readers retain v1 support. Existing v1 files remain readable and
servable; a new build after finalization emits v2.

The authority is included in meta Raft snapshots and survives persistent reopen.
It is excluded from logical backup data and skipped during logical/PITR replay:
a restore must use the destination cluster's membership and independently verified
upgrade policy. Backup manifests report the durable compatibility level. The old
catalog `get_cluster_version` descriptor remains a legacy placeholder and is not
used as writer or backup authority.

## Limits and evidence

- At most 256 members; addresses at most 512 bytes, binary strings at most 128,
  capability responses at most 8 KiB and authority records at most 256 KiB.
  Probe requests have a three-second timeout; admin mutations have a fifteen-second
  overall timeout including lock/probe/consensus waits. Status has a five-second
  ReadIndex timeout. Sequential probes may require retries in a slow large cluster.
- Preflight uses the bounded storage snapshot export (128 MiB accounting) on the
  blocking pool. This may refuse a larger physical store and allocate temporary
  copies. Snapshot chunk probes only check capabilities, without exporting storage.
  Streaming preflight/installation and retained-file cleanup remain future work.
- Before finalization, v1 still refuses user history/tombstones/intents; OpenRaft
  treats a build error as fatal. This availability limit ends for eligible v2 groups
  after finalization, subject to the existing snapshot size and installation limits.
- Binary `c138079` and earlier do not read `UpgradeControlV1`. Deploy this authority
  reader to every member before starting the protocol. No actual historical/new
  binary pair has been certified. Rolling back the feature before finalization
  does **not** prove rollback to an older executable once authority records/logs
  exist. A supported two-binary baseline is still required for the R6 rolling-upgrade
  acceptance gate.
- Tests cover full voter/learner reports, duplicate/unknown/stale reports, premature
  finalization, rollback, malformed records, actual mTLS/Raft leader loss and reopen,
  an unavailable member blocking finalization, abrupt subprocess exit after an
  acknowledged finalization, subsequent v2 snapshot transfer, incompatible recipient
  rejection before first/resumed chunks, admin admission/ingress, and destination
  policy preservation during backup/PITR replay. They do not establish Byzantine
  safety, power-loss behavior, S3 recovery or a mixed-binary rolling upgrade.

See [MVCC snapshots](mvcc-snapshots.md) and the
[durability contract](../reference/durability-contract.md).
