# Filesystem restore recovery measurements — 2026-11-07

## Scope and result

A release-mode filesystem restore drill passed for full, incremental, and
point-in-time recovery. Each scenario restored into a separate fresh persistent
destination, matched the expected SQL rows exactly with no unexpected rows,
retained the secondary index and transitive role permission, allowed the restored
role to query the expected rows, and passed the same checks after restart.

No expected row was missing at any selected recovery point. The schema-2 report
therefore records zero observed acknowledged-write loss for this exact fixture
and its clean-shutdown, fully archived failure model. This is not a crash,
power-loss, distributed-consistency, or arbitrary-workload recovery claim.

| Scenario | Restore API | Restore to ready | Restart-inclusive recovery | Missing / unexpected rows |
| --- | ---: | ---: | ---: | ---: |
| Full | 296 ms | 324 ms | 543 ms | 0 / 0 |
| Incremental | 612 ms | 644 ms | 820 ms | 0 / 0 |
| PITR | 1,426 ms | 1,456 ms | 1,668 ms | 0 / 0 |

`restore_to_ready_ms` starts immediately before the restore request and ends
after SQL rows, index metadata, and permissions pass validation. Destination
startup and pre-restore object verification are excluded.
`restart_inclusive_recovery_ms` additionally covers destination shutdown,
restart, and repeated validation. The `budget_ms` and `within_budget` fields are
null: these measurements are observations, not approved recovery-time budgets.
General production remains **NO-GO**.

## Environment and method

- Machine: MacBook Pro (Mac16,5), Apple M4 Max, 16 cores, 48 GiB RAM.
- OS: macOS 27.2 (26B123), arm64; local journaled APFS.
- Toolchain: rustc 1.99.1 (58cf8f9c3 2026-10-13); cargo 1.99.1
  (5f94df478 2026-08-27).
- Build: Cargo `release` profile, optimized and locked.
- Source: `b1d39576e6efd1f6c86e8c8c722183e61a76dd17`, tree
  `77e322f5019ecb0379e10b0a0b32096500d3dd9f`.
- Source tree state: the pre-existing maintainer `.gitignore` modification was
  not a build input. An untracked workflow and two unrelated untracked Markdown
  files were also left untouched. The report consequently records `dirty: true`.
- Deployment: one node, persistent LSM, finalized `snapshot-v2`, and a fresh
  empty destination directory for each scenario.
- Failure model: clean source shutdown, WAL archive caught up through the final
  acknowledged write, and no injected failure.
- Available filesystem space after the drill: 19 GiB. This local, near-full
  filesystem is part of the measured environment, not a supported-deployment
  recommendation.

The drill used the documented test with a fresh artifact path:

```bash
NODUS_RESTORE_DRILL_REPORT=target/recovery-rpo-rto-b1d3957-20261107/filesystem.json \
cargo test --release --locked -p nodus_integration_tests --test restore_drill \
  persistent_full_incremental_pitr_restore_drill -- --nocapture
```

The test passed in 5.04 seconds. Compilation and test execution completed
successfully; no other benchmark or build from this task ran concurrently.

## Recovered states

The fixture created a primary-key table, secondary index, transitive role grant,
and inserts, updates, and deletes. Recovery used one full backup followed by one
incremental backup; PITR used that chain and replayed two archived WAL segments
to a timestamp between later writes.

- Full expected and actual rows: `1:one`, `3:three`.
- Incremental expected and actual rows: `1:one-inc`, `2:two-inc`, `4:four-inc`.
- PITR expected and actual rows: `1:one-inc`, `2:two-inc`, `4:four-pitr`,
  `5:five-pitr`.
- The post-target `6:after-target` insert and post-target deletion of row 1 were
  not present in the PITR result.

Independent validation checked schema version 2, source and deployment metadata,
exact row equality, indexes, permissions, restart persistence, zero missing and
unexpected rows, finite and coherent timings, null unapproved budgets, PITR
segment counts, and exact agreement between JSON timings and Prometheus gauges.

## Artifacts

Generated evidence is under `target/recovery-rpo-rto-b1d3957-20261107/`:

- `filesystem.json`: schema-2 source metadata, expected and actual rows,
  permission/index/restart results, replay details, and RPO/RTO measurements;
- `filesystem.prom`: matching Prometheus textfile gauges, including
  `nodus_restore_drill_restore_to_ready_seconds` for all three scenarios.

SHA-256 values:

- JSON: `b221161a367949066d663f238a2fcf31131d6573748e2d5cabb45d39d9c9d1bc`
- Prometheus: `7b0271bd8c6c586925c55e70777f2ed40516b3b29f6afbf2c45d186ac837d9f3`

## Limits and follow-up

- This was one small-fixture run on one macOS host. It establishes no variance,
  capacity, large-backup restore, or production recovery-time objective.
- No live S3-compatible repository was exercised in this run. Filesystem results
  do not establish object-store behavior.
- No crash, power loss, repository outage, corrupt object, partial backup,
  concurrent workload, or multi-node restore was injected.
- Maintainer-approved recovery budgets and repeated measurements on the
  supported deployment profile remain required. Sustained-load and final
  release-candidate recovery evidence also remain open.
- This focused drill does not qualify the source commit through the complete
  correctness, compatibility, security, hosted-CI, or release-pipeline gates.
