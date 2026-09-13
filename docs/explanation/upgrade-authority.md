# Snapshot upgrade authority

The production server can enable NSNP v2 after explicit, verified finalization.
It starts at compatibility level 1 and writes v1 snapshots until then. This
authority protocol supports the target **`snapshot-v2`** and freezes meta
membership during the upgrade. After finalization, verified additive admission
can introduce new voters. It does not enable shard
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

## Adding members after finalization

Deploy the admission-v1 reader to **every existing voter and learner** before
joining a new node. Send the existing authenticated `POST /api/v1/cluster/join`
request to the meta leader, with `node_id` and `raft_advertise_addr` (`host:port`).
The candidate must already serve its configured ID over peer mTLS. The endpoint
returns success only after stable voter membership and durable completion.
`GET /api/v1/upgrade` includes `member_admission`, which is null before the first
admission and otherwise contains the revision, admitted members and pending plan.

The finalized base authority stays immutable. A separate version 1 ledger at
`\x01upgrade/admission/v1/state`, anchored to its revision, records this sequence:

1. **Approved:** probe every current voter, learner and candidate with a fresh
   challenge; require snapshot v2 and admission v1 support; replicate the reserved
   ID/address, candidate report, operation UUID and original membership.
2. Add the candidate as a learner and wait for catch-up. Re-probe all members with
   another challenge and replicate **Promoting** before changing voter membership.
3. Complete joint consensus, verify the resulting stable voter membership, and
   replicate completion. The admitted identity remains in the ledger permanently.

Each command compares the exact applied membership and expected ledger revision.
One pending operation serializes admissions across retries and leader changes.
A timeout has an uncertain outcome: inspect `member_admission` and retry the same
ID/address on the current leader. Retries resume approval, learner catch-up,
promotion or completion; they do not allocate another identity. An already joined
voter succeeds only if its approved address matches and fresh capability checks
pass. Conflicting IDs/addresses, concurrent different candidates, missing readers,
and unexpected membership changes are rejected before continuing.

The shared leader lock covers join/upgrade operations; consensus checks guard
against leadership changes beyond that process-local lock. Admission has a
30-second execution bound after acquiring the admin membership lock, in addition
to the per-probe and ReadIndex bounds. Large or slow clusters may need retries.
There is no automatic coordinator recovery worker; the joining node's existing
join loop or an operator retry drives the recorded operation forward.

Removal, address replacement, promotion of a pre-existing base learner, and
cancellation of an approved candidate remain unsupported. Cancellation needs a
fence against a late learner RPC; editing the ledger is unsafe. Restore the
candidate at the reserved address to resume. Data groups reconcile against the
base roster plus approved additions, including the pending candidate once it
appears in applied meta membership. Reconciliation reads membership and authority
under the same apply lock and validates voter sets as well as addresses. The writer
gate includes its verified reader before
learner creation, so catch-up can use v2 snapshots with history and intents.

## Reader compatibility

| Binary capability | Existing authority / NSNP v1 and v2 | Admission commands and ledger | Can participate in admission |
| --- | --- | --- | --- |
| Authority reader without `admission_version` (R6a) | Reads | Unsupported | No |
| Admission-v1 reader before first admission | Reads; existing base stays unchanged | Reads | Yes, after all-member verification |
| Admission-v1 reader after admission | Reads | Reads and writes anchored ledger | Yes |

A missing capability field decodes as admission version 0; it does not authorize
admission. Base-authority commands and transitions clear that optional field,
so old and new authority readers persist identical base records; the immutable
base does not acquire admission-only evidence. Before the first new admission
command, the service probes all current members.
The sender also probes before transmitting an admission command. Once a ledger
exists, it requires admission-v1 support before every append RPC and every v2
snapshot chunk, including cached/resumed transfers. This adds one capability RPC
to append traffic after admission; failure blocks replication to that peer.
This is a fail-closed reader guard, not a supported executable downgrade procedure.

## Snapshot serving and recovery

Every production group reads the durable meta authority for writer eligibility.
The existing per-group membership gate must also pass. The sender re-probes the
recipient before **each v2 chunk**, including a cached snapshot and resumed stream;
an unknown resumed stream is treated conservatively as v2. An old/missing endpoint,
identity mismatch, insecure transport or incompatible response prevents that chunk
from being sent. Readers retain v1 support. Existing v1 files remain readable and
servable; a new build after finalization emits v2.

The authority and admission ledger are included in meta Raft snapshots and survive
persistent reopen. Installation rejects omitted, regressed, unanchored or replaced
admission identities and checks the snapshot membership and applied pointer.
Both records are excluded from logical backup data and skipped during logical/PITR replay:
a restore must use the destination cluster's membership and independently verified
upgrade policy. Backup manifests report the durable compatibility level. The old
catalog `get_cluster_version` descriptor remains a legacy placeholder and is not
used as writer or backup authority.

## Limits and evidence

- At most 256 members; addresses at most 512 bytes, binary strings at most 128,
  capability responses at most 8 KiB, authority records at most 256 KiB and
  admission ledgers at most 512 KiB.
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
  reader to every member before starting the protocol. The pinned R6a/R6b
  [two-binary gate](../how-to/test-mixed-binary-upgrades.md) exercises same-directory
  executable rollback before admission, but found snapshot login failures and
  long log-purge pauses. This pair is not certified for uninterrupted upgrades.
  Feature rollback does **not** establish arbitrary executable compatibility.
- Tests cover full voter/learner reports, duplicate/unknown/stale reports, premature
  finalization, rollback, malformed records, actual mTLS/Raft leader loss and reopen,
  an unavailable member blocking finalization, abrupt subprocess exit after an
  acknowledged finalization, subsequent v2 snapshot transfer, incompatible recipient
  rejection before first/resumed chunks, admin admission/ingress, and destination
  policy preservation during backup/PITR replay. They do not establish Byzantine
  safety, power-loss behavior or S3 recovery. Mixed-binary observations and their
  blockers are recorded separately by the process gate.
- Admission tests use real LSM and mTLS Raft nodes: leader changes after approval
  and after learner/promotion intent, purged-log v2 catch-up, historical reads,
  transferred intent resolution, persistent reopen, and abrupt subprocess exit
  after acknowledged approval followed by retry. Rejection tests cover older
  readers before append/snapshot bytes, stale reports, competing IDs/addresses,
  premature voter completion, missing/misanchored snapshot metadata, and source
  admission records in logical backup/PITR replay. Joint-state validation is tested
  directly; a forced process failure between the two joint-consensus commits
  remains follow-up evidence. Completing diagnostic two-binary checkpoints with
  recipient restarts does not close the snapshot authorization defect.

See [MVCC snapshots](mvcc-snapshots.md) and the
[durability contract](../reference/durability-contract.md).
