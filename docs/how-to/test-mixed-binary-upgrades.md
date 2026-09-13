# Test a rolling upgrade with two binaries

The September 13, 2026 diagnostic run **failed** after 534.67 seconds. It reached
four of thirteen checkpoints: initial old-reader SQL/abort behavior, v1 transfer
to the new reader (requiring a recipient restart), feature and executable rollback
before finalization, and mixed-reader v2 finalization with feature rollback refused.
After publishing the v2 snapshot, it could not recover the requested leader within
the 240-second election window. Both live voters reported no leader; the cause of
that sustained failure is still under investigation. A separate process sample
confirmed per-entry disk synchronization during purge, but does not establish the
cause of the election failure. The remaining nine checkpoints are **unverified**,
including v2 transfer to the old reader, admission and post-admission crash recovery.
This pair has not passed rolling-upgrade acceptance.

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
upgrade. The defect needs an identity-safe bootstrap/catalog lifecycle fix;
rebinding ordinary user passwords to reusable names is not a valid repair.

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

The planned workload covers acknowledged inserts and updates, an explicitly aborted row,
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

| Cluster state | Feature rollback to level 1 | R6b executable replaced by R6a |
| --- | --- | --- |
| Before snapshot-v2 finalization | Permitted by the upgrade API | Exercised against the same persistent directory after feature rollback |
| Finalized v2, before any admission | Rejected; finalization is irreversible | Planned; not reached in the recorded run |
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
