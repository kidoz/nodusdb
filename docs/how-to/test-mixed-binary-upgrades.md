# Test a rolling upgrade with two binaries

The gate compares an authority-only maintenance reader with an admission-capable
newer reader. Both contain the bootstrap-authentication and local WAL-retention
fixes. The older reader still lacks admission commands; patching these defects
must not erase the protocol boundary being tested.

The September 14, 2026 run **passed all thirteen checkpoints in 360.45 seconds**
with `passed: true`, `matrix_completed: true` and an empty blocker list. It used
the pinned pair below, with neither `--candidate` nor `--diagnose`. Both snapshot
directions, executable rollback before admission, admission refusals, four-node
admission and crash/restart recovery passed. All five committed rows survived;
the aborted row remained absent. No snapshot-login restart was needed.

Run the compatibility gate with:

```bash
just test-mixed-binary
```

It first runs the source-pin tests, then builds and runs both servers. Cargo/Rust,
Python 3, Git, tar and OpenSSL are required on a Unix host. The default run uses
neither a working-tree candidate nor diagnostic restart workarounds. Acceptance
requires `matrix_completed: true`, `passed: true`, thirteen checks and no blockers.

## Pinned sources

| Reader | Revision | Admission reader |
| --- | --- | --- |
| R6a maintenance | `65313a236ce54adb510dde1835e4a652ece3a0e8` | Absent; unknown commands are rejected |
| Newer reader | `d1989d3c0d21d22f60507b681d1eb288a8f42dce` | Admission v1 |

R6a maintenance backports only commits `225a5dc` (bootstrap authentication) and
`f82094c` (local WAL retention) onto
`31897cd7d7bbbf7b51cc3d522a2a77f4e7ec908a`. The reviewed
[backport patch](../../tools/testing/fixtures/r6a-maintenance.patch) reconstructs
Git tree `9dd2beafcee4ae17bde6be0acc74d70d249d4d6d`. The runner verifies both the
patch SHA-256 and this tree before building. It uses a temporary Git index, so
staged changes and working files stay intact. This works in a fresh main-branch
checkout without the local maintenance branch or its commit object. See
[source provenance](../../tools/testing/fixtures/README.md) for regeneration.
The newer reader is archived directly from its reachable Git revision.

These are production server sources with explicit maintenance fixes, not a claim
that the original historical binaries contain those fixes or that a maintenance
release has been published. No command decoder, capability response, feature gate
or production configuration default is rewritten by the runner.

Both report package version `0.1.0`; that string alone cannot identify a reader.
`builds.json` schema 2 records the revisions, old base/tree/patch identity, lockfile
SHA-256, toolchain and binary hashes. Runtime capability responses and the
admission-command rejection test establish the actual reader boundary. Identical
binary hashes are rejected. Each revision has a separate Cargo target directory
to prevent accidental reuse between archived source roots.

The builds use Cargo's dev profile with debug information disabled. Their timings
are not release-build performance measurements. The output directory contains
binaries, source archives, build/test logs, per-process logs and `results.json`.
Keep that directory with the result when reviewing an upgrade. Its certificates
and databases are disposable test fixtures. The wrapper owns the process group and kills it on
failure, timeout or interruption; the Rust fixture kills/reaps child servers on
return or panic. A hard kill of the wrapper itself cannot execute cleanup.
The ordinary workspace test suite deliberately ignores this slow process test.

## Reuse and candidate evaluation

```bash
python3 -B tools/testing/mixed_binary.py --output /tmp/nodus-mixed-maintenance
python3 -B tools/testing/mixed_binary.py --output /tmp/nodus-mixed-maintenance --reuse-builds
```

A fresh build requires a new output directory. Directories from the original
historical pins cannot be reused for this pair: their identities differ.
`--build-only` prepares binaries without running the matrix. Target artifacts
live under `target/mixed-binary-build`. Reuse verifies the recorded identities.

To evaluate future changes, add `--candidate` to build the current working tree
as the newer reader, while retaining the pinned R6a maintenance reader:

```bash
python3 -B tools/testing/mixed_binary.py --output /tmp/nodus-mixed-maintenance --reuse-builds --candidate
```

Candidate HEAD, source patch, untracked crate sources, lockfile/toolchain and
binary hashes are recorded separately in `candidate.json`, `candidate.patch` and
`candidate-untracked/`. The pinned binaries and `builds.json` remain intact.
`--diagnose` is an investigation-only option: it records snapshot-login failures
and restarts recipients to continue. Any recorded blocker still fails the gate.
It is not part of the acceptance command above.

## Historical baseline

The original pair (`31897cd` / `3b4cca4`) exposed stale bootstrap identity after
snapshot installation and premature deletion of WAL still needed for recovery.
The September 13 candidate run completed thirteen checkpoints in 339.09 seconds,
but required one old-recipient login restart and therefore failed acceptance.
Those original executables remain unfixed. The maintenance backport addresses
both defects without changing durable formats or admission capabilities. It does
not automatically repair already-missing local WAL. See
[authentication rules](../reference/admin-api-authorization.md#authentication-schemes)
and [local WAL cleanup](../reference/durability-contract.md#local-wal-archive-cleanup).

The fixture also corrected two independent issues: one-second forced elections
preempted an up-to-date peer while a stale candidate could not win, and the seed
list omitted the newly admitted node when it became leader. Election retries now
allow ten seconds to settle, and every known admin seed is configured.

## Workload and failure boundaries

The fixture runs three, then four, independent server processes over real TCP and
peer mTLS. Every process has a persistent LSM directory and fixed loopback ports.
SQL uses `tokio-postgres`; upgrade/join operations use authenticated admin APIs.
Certificates are issued by a disposable test CA, with one shared leaf for this
loopback test. This does not exercise production PKI issuance or node attestation.
Every process is configured with all other known admin seed addresses, including
the fourth member. Startup join currently needs to reach the leader directly;
follower seed responses do not discover or forward to a newly elected leader.
A seed list containing only the original members can therefore leave a restarted
node not-ready after the newly admitted member becomes leader.

The snapshot checks advance the real meta log beyond the production 5,000-entry
threshold using bounded concurrent abort commands. Synchronous deletion of thousands
of retained Raft entries can pause these binaries for tens of seconds. The gate
allows uncertain responses only for filler aborts after the expected snapshot file
is published, then waits up to four minutes for Raft RPCs to respond. SQL commit
acknowledgements and row assertions are never relaxed. This pause is a known
availability limitation of the pinned pair, not a latency guarantee. Filler aborts
avoid pending user intents during v1 snapshot creation. The fixture stops one
replica before compaction and restarts it against the same persistent directory,
forcing snapshot catch-up from beyond its retained log position. It checks the
installed `NSNP` wire header and reads the catalog and rows through SQL. Every
executable replacement reuses the existing directory to exercise WAL, snapshot
and authority reopen. Erasing an existing voter's history is deliberately excluded:
OpenRaft forbids follower log reversion. Only a newly admitted identity starts empty.

The workload covers acknowledged inserts and updates, an explicitly aborted row,
reader changes, feature rollback, old/new leader elections, v1 transfer from old
to new, v2 transfer from new to old, candidate refusal with two and then one old
voter, refusal of an old candidate, and successful admission once every reader is
new. It also sends a valid admission-command envelope to both readers: R6a
rejects its unknown variant (HTTP 422), while the newer reader recognizes it and rejects generic
ingress (HTTP 403). Finally, it kills the current leader after an acknowledged write, writes on
the elected replacement, restarts the killed process and checks all rows on all
four members. It is an ordered regression scenario, not a concurrent-workload
linearizability proof or a performance benchmark.

## Rollback boundaries for this pair

| Cluster state | Feature rollback to level 1 | New executable replaced by R6a maintenance |
| --- | --- | --- |
| Before snapshot-v2 finalization | Permitted by the upgrade API | Exercised against the same persistent directory after feature rollback |
| Finalized v2, before any admission | Rejected; finalization is irreversible | Exercised against the same persistent directory |
| An admission approval has entered the log | Rejected | Unsupported; R6a cannot interpret admission commands or enforce the ledger |

Before adding a member, replace every existing voter and learner with an
admission-capable binary. The join coordinator rejects admission while any probed
member or candidate lacks the reader. Once approval is recorded, do not roll a
member back to R6a, including while the admission is still pending. A restarted
historical executable has no new-ledger startup fence. Outbound reader probes in
R6b do not turn an arbitrary old executable downgrade into a safe operation.

The tests do not certify older revisions, member removal/address replacement,
original learner promotion, cancellation, power loss, live-reader continuity,
S3 recovery, or a crash between the two joint-consensus commits. Migration remains
disabled. See [upgrade authority](../explanation/upgrade-authority.md) for the
protocol and [durability contract](../reference/durability-contract.md) for the
storage guarantees.
