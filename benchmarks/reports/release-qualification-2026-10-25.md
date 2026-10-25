# Release qualification manifest — 2026-10-25

## Verdict

**PASS for clean-commit release qualification** on commit
`c3f31f942b32afe22143d52e3e796728ba738df3`.

This is not a production-readiness approval. NodusDB remains **NO-GO for general
production use**. Write-path scalability, sustained budgets and soak testing,
and the remaining operational requirements are still open. The experimental
warning stays in force.

## Source and configuration identity

- Commit: `c3f31f942b32afe22143d52e3e796728ba738df3`
- Git tree: `1713ab43f261bce21d4feb7e94bf76f1e75546e8`
- Candidate source patch: empty; SHA-256
  `e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855`
- Candidate untracked sources: none
- Mixed-binary candidate SHA-256:
  `80fcdd79a46fe1889a36452a824ef37d5da706bbb26254ed8cdf3c9410b6fbf9`
- Main `Cargo.lock` SHA-256:
  `ac0d781e70793d8eaeda8e4320daffd6e9d05a080ae9c2b9afcf0efff74feab2`
- Fuzz `Cargo.lock` SHA-256:
  `78eb689757299d00275327fcc2c316107ac723a1d6219ab935b55f300af4584b`
- `frontend/package-lock.json` SHA-256:
  `d2b94aaee72dc8393f327d19963ac9494c643133c50ff28044695ba9eb124523`

Local gates ran in a detached clean worktree with no tracked or untracked source
changes. The host was an Apple M4 Max MacBook Pro with 48 GiB RAM, macOS 27.0.0
arm64, and rustc/cargo 1.99.0. Hosted gates used the workflow-managed Ubuntu,
stable Rust, Node 24, Docker, MinIO, and OpenTelemetry Collector configuration.

## Hosted evidence

All three runs executed against the full candidate commit above.

| Workflow | Run | Result | Evidence |
| --- | --- | --- | --- |
| CI | [40179422458](https://github.com/kidoz/nodusdb/actions/runs/40179422458) | PASS | Both cargo-deny workspaces; formatting; Clippy; Iceberg feature checks; workspace tests; partition; Loom; benchmark compile and smoke; frontend format/test/build/lint/Storybook and both npm audits; production container; live MinIO/OTLP and S3 restore drill |
| Fuzz | [40180614471](https://github.com/kidoz/nodusdb/actions/runs/40180614471) | PASS | AFL runtime and targets built; all three 10-second smoke runs completed without crash artifacts |
| Release, non-publishing | [40188472698](https://github.com/kidoz/nodusdb/actions/runs/40188472698) | PASS | Format, Clippy, workspace tests, release binaries, checksums, binary/image SBOMs, production image and container smoke |

The release run was `workflow_dispatch`, not a tag push. Cosign installation,
checksum/image signing, GHCR login/publication, and GitHub release publication
were skipped by the workflow's tag-only conditions. No tag or release was
created.

The hosted S3 recovery artifact is
`live-infrastructure-recovery-40179422458-1` (artifact ID `12594266458`). Its
full, incremental, and PITR scenarios all passed with restart, index, and
permission verification and zero lost or unexpected rows.

## Local gate evidence

The local gate sequence started at `2026-10-25T11:20:46Z` and completed at
`2026-10-25T11:53:57Z`.

| Gate | Command | Result |
| --- | --- | --- |
| Formatting | `cargo fmt --all -- --check` | PASS |
| Workspace lint | `cargo clippy --workspace --all-targets --locked -- -D warnings` | PASS |
| Iceberg lint | `cargo clippy -p nodus_iceberg -p nodus_cli --all-targets --features nodus_cli/iceberg --locked -- -D warnings` | PASS |
| Iceberg tests | `cargo test -p nodus_iceberg -p nodus_cli --features nodus_cli/iceberg --locked` | PASS |
| Workspace tests | `cargo test --workspace --locked` | PASS: 876 passed, 0 failed, 5 ignored |
| SQL golden | `just test-sql` | PASS |
| Migration | `cargo test -p nodus_server migration_tests --locked` | PASS: 44 passed |
| Fault | `just test-fault` | PASS |
| Filesystem restore | `cargo test -p nodus_integration_tests --test restore_drill persistent_full_incremental_pitr_restore_drill --locked -- --nocapture` | PASS |
| Partition | `just test-partition` | PASS: 1/1 |
| Loom | `just test-loom` | PASS: 3/3 |
| TLS | `cargo test -p nodus_compatibility_tests --test tls_handshake --locked` | PASS: 4/4 |
| Fuzz compile | `just fuzz-check` | PASS |
| Server benchmark smoke | `NODUS_BENCH_SCALE=smoke cargo bench --locked -p nodus_integration_tests --bench server_bench` | PASS |
| Mixed binary | `python3 -B tools/testing/mixed_binary.py --output ../mixed-binary-c3f31f9 --candidate` | PASS: 13/13, `matrix_completed: true`, no blockers |

The five ordinary workspace ignores were the opt-in mixed-binary test, a local
purge benchmark, an English-stem dump utility, and two SQL calibration/diagnostic
tools. The required mixed-binary test was executed separately and passed. No
required check was ignored, and no diagnostic mode or restart workaround was
used.

The filesystem full, incremental, and PITR drill passed with restart, index, and
permission verification and zero lost or unexpected rows. The mixed-binary run
used the empty source patch and binary identities listed above; all five
committed rows survived and the aborted row remained absent.

## Retained artifacts

Local evidence is retained under `target/release-qualification/`:

- `logs-c3f31f9/`: one log per local gate.
- `mixed-binary-c3f31f9/`: `candidate.json`, empty `candidate.patch`,
  `results.json`, binaries, and process logs.
- `source/target/restore-drill/filesystem.json` and `.prom`: local recovery
  evidence and metrics.
- `hosted-recovery/`: downloaded hosted S3 recovery evidence and MinIO log.
- `source/target/criterion/`: local server benchmark smoke output.

The full six-workload performance baseline remains the separate
[2026-10-24 server baseline](server-baseline-2026-10-24.md). It predates this
clean commit and is supporting performance evidence, not a release-qualification
result or a production capacity guarantee.

## Remaining limitations

This qualification establishes a reproducible correctness and
release-engineering baseline. It
does not establish write scalability, approved latency/throughput SLOs, memory or
Raft-log bounds, repeated full-load results, a 24-hour soak, distributed backup
completeness, or the remaining migration/security/operational contracts. Those
items block any general production claim.
