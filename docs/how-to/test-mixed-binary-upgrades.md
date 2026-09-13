# Test a rolling upgrade with two binaries

The pinned historical pair has not passed uninterrupted rolling-upgrade acceptance:
snapshot installation replaces the bootstrap principal ID, leaving the historical
password authenticator bound to the old ID. The working-tree candidate fixes this
for upgraded readers. Diagnostic runs retain failures on historical recipients
and use explicit restarts to continue the compatibility checks.

The final September 13, 2026 candidate run completed **all thirteen checkpoints in
339.09 seconds**, including both snapshot directions, both executable rollback
boundaries before admission, admission refusals, four-node admission and crash
recovery with all five committed rows present and the aborted row absent. Neither
new-reader snapshot recipient needed a login restart. The sole recorded blocker
was the unchanged R6a recipient's snapshot-login failure, handled by one explicit
diagnostic restart. Thus `matrix_completed` is true but `passed` remains false;
this is diagnostic compatibility evidence, not a clean acceptance result.

Run the slow compatibility gate with:

```bash
just test-mixed-binary
```

It requires Cargo/Rust, Python 3, Git, tar and OpenSSL on a Unix host. It builds
unmodified production `nodus_server` executables from these full Git revisions:

- Authority-only reader: `31897cd7d7bbbf7b51cc3d522a2a77f4e7ec908a` (R6a).
- Admission-v1 reader: `3b4cca438485c7a1b0f1a6c43c4cc2cfba51ea1c` (R6b).

Both report package version `0.1.0`; that string alone cannot identify the reader.
The gate records revision, lockfile SHA-256, toolchain and binary SHA-256 in
`builds.json`, then checks actual capability responses. It rejects identical
binary hashes. Each revision has a separate Cargo target directory: a shared
target can incorrectly reuse path-package artifacts from relocated Git archives
whose timestamps were preserved. No production source or configuration default
is patched to make the historical binaries pass.
The builds use Cargo's dev profile with debug information disabled. They execute
production code paths; their timings are not release-build performance measurements.

The command prints an evidence directory containing binaries, source archives,
build/test logs, per-process logs and `results.json`. Keep it with the result when
reviewing an upgrade. These directories contain test-only certificates and data.
The runner creates a new process group and kills it on timeout or interruption;
the Rust fixture also kills/reaps its child servers on return or panic. A hard
kill of the runner itself cannot execute cleanup. The ordinary workspace test
suite deliberately ignores this test; the wrapper explicitly selects it and
requires a fresh successful result with all thirteen checkpoints.

The pinned pair currently fails SQL authorization after snapshot installation:
the catalog's bootstrap principal changes, while the password authenticator keeps
the recipient's original principal ID. `readyz` can already be successful at this
point. Restarting the recipient reloads its credential against the installed
catalog. The default gate stops at this regression. To gather the remaining
compatibility evidence, use explicit diagnostic mode:

```bash
python3 tools/testing/mixed_binary.py --output /tmp/nodus-mixed-evidence --reuse-builds --diagnose
```

Diagnostic mode records the failure, restarts that recipient and continues. It
still exits unsuccessfully: `matrix_completed: true` means all checkpoints ran,
whereas `passed: false` and `blockers` prevent treating the workaround as a clean
upgrade. The current fix treats the configured bootstrap password as an explicit
operator credential for an existing, privileged global administrator. Ordinary
user passwords remain bound to immutable IDs. See
[authentication rules](../reference/admin-api-authorization.md#authentication-schemes).

To test the current working tree as the new reader against the unchanged R6a
binary, use:

```bash
python3 tools/testing/mixed_binary.py --output /tmp/nodus-mixed-evidence --reuse-builds --candidate --diagnose
```

`--candidate` builds the working tree in an isolated Cargo target and records its
HEAD, tracked source patch, untracked crate sources, lockfile/toolchain and binary
hash in `candidate.json`, `candidate.patch` and `candidate-untracked/`. It leaves
the historical binaries and `builds.json` intact. This is a candidate evaluation,
not a claim that the pinned R6b executable contains the fix. The historical R6a
recipient still needs the diagnostic login restart after receiving v2.

To choose a retained output directory, or rerun the same verified binaries:

```bash
python3 tools/testing/mixed_binary.py --output /tmp/nodus-mixed-evidence
python3 tools/testing/mixed_binary.py --output /tmp/nodus-mixed-evidence --reuse-builds
```

A fresh build requires a new output directory. `--build-only` prepares binaries
without starting the matrix. Target artifacts live under `target/mixed-binary-build`.
Reuse verifies the recorded hashes, revisions, lockfile hashes and toolchain.

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

The earlier 534.67-second diagnostic run reached only four checkpoints because
the fixture forced elections every second on a node with a stale log. The other
live voter correctly rejected those votes. Copies of the same saved stores
recovered a leader through normal elections in about four seconds. Election
retries now allow ten seconds for an up-to-date node to win and replicate first.
This fixture correction is separate from the confirmed synchronous purge pause.

A subsequent candidate run reached five checkpoints in 326.54 seconds, then
found a separate recovery defect during executable rollback. The local manifest
still required WAL segment 370, but the archiver had removed it; its archived
copy contained the committed term-7 vote. The remaining store reopened with a
term-6 vote and term-7 log entries, which OpenRaft rejected. The candidate now
delegates local WAL deletion to the storage engine, which checks the published
replay floor under the checkpoint lock. Backup retention approval alone cannot
authorize deletion. This repair changes no durable format and does not repair
already-missing local WAL automatically.

The workload covers acknowledged inserts and updates, an explicitly aborted row,
reader changes, feature rollback, old/new leader elections, v1 transfer from old
to new, v2 transfer from new to old, candidate refusal with two and then one old
voter, refusal of an old candidate, and successful admission once every reader is
new. It also sends a valid admission-command envelope to both readers: R6a
rejects its unknown variant (HTTP 422), while R6b recognizes it and rejects generic
ingress (HTTP 403). Finally, it kills the current leader after an acknowledged write, writes on
the elected replacement, restarts the killed process and checks all rows on all
four members. It is an ordered regression scenario, not a concurrent-workload
linearizability proof or a performance benchmark.

## Rollback boundaries for this pair

| Cluster state | Feature rollback to level 1 | New executable replaced by R6a |
| --- | --- | --- |
| Before snapshot-v2 finalization | Permitted by the upgrade API | Exercised against the same persistent directory after feature rollback |
| Finalized v2, before any admission | Rejected; finalization is irreversible | Exercised with the candidate against the same persistent directory |
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
