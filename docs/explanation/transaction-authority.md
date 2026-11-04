# Transaction ownership and decisions

This is a staged design for safe transaction recovery. The authority state
machine is implemented as dormant Raft scaffolding; SQL transactions still use
the existing commit-only 2PC journal. Automatic intent cleanup remains disabled.

**Status:** foundation implemented, activation unavailable, 2026-11-04.

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

The dormant command can be applied through Raft for tests; every public forwarded
write rejects it, including authenticated peer traffic. No production caller
emits it. A Raft success means the conditional authority transition was committed
and applied; it does not acknowledge user data or participant resolution.

Before activation, coordinator integration must enroll each participant through
meta Raft **before acknowledging any intent there**, freeze the complete set,
prepare all participants, durably decide commit, and resolve from that decision.
No single-shard or batched fast path may bypass the authority. Prepare votes and
resolution must bind the transaction generation, participant epoch and complete
write set. Participant apply must persist a terminal fence and reject legacy or
stale finalization for tracked IDs, including writes delayed until after abort.
A local replica read or a caller-supplied record is not a decision certificate:
the trusted resolver must establish the authoritative meta decision and submit
the corresponding generation through authenticated peer transport.

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

This foundation adds no upgrade target, capability claim, configuration switch,
SQL behavior or janitor writer. Level 4 is insufficient to enable it. The new
command is appended to the enum, preserving old variant ordinals, and authority
keys are reserved against ordinary and epoch-aware intent mutations. No new
authority bytes are produced by normal traffic, so rollback has no new boundary
in this stage. Test-created authority data is not a supported old-binary input.
Activation will close rollback at its finalized format boundary and must update
node admission, snapshot capability checks and the mixed-binary matrix together.

## Verification and remaining work

Foundation tests cover conditional decision orderings, concurrent single-voter Raft requests,
stale generations and revisions, invalid or future records, namespace protection,
ingress rejection, and process-exit recovery after acknowledged Raft decisions.
They establish the authority's durable decision boundary, not end-to-end orphan
resolution, a network partition, deterministic simulation, or power-loss safety.

The next slice is participant enforcement and coordinator integration, followed
by the new upgrade level. Its acceptance test must pause a real coordinator,
fence-abort through meta Raft, resolve every participant, resume the coordinator,
and prove that no delayed write or commit becomes visible. A second schedule
must let commit win and recover every acknowledged value after a crash. Only
then can the janitor use the authority; legacy records remain non-destructive.
