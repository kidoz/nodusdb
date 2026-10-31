# Run the test suites

NodusDB separates tests by scope and runtime cost. This guide covers which
command to run when. For what each suite actually contains, see
[test suites](../reference/test-suites.md).

The project uses [`just`](https://github.com/casey/just) as its task runner.
Most recipes wrap `cargo`; infrastructure recipes also drive Python and Docker.

## While you are editing code

Run the unit tests and the ordinary workspace tests:

```bash
just test
```

For a single crate, skip the workspace and go narrow — this is the fastest loop:

```bash
cargo test -p nodus_executor
```

## Before you push

Format, lint, and test in one step:

```bash
just check
```

This runs `cargo fmt --all`, `cargo clippy --workspace --all-targets -- -D
warnings`, and `cargo test --workspace`. To check formatting without rewriting
files, use `cargo fmt --all --check`.

## Cross-crate product behaviour

```bash
just test-cross
```

That runs the integration, PostgreSQL compatibility, and SQL golden suites
together. Run them individually when you are iterating on one:

```bash
just test-integration   # server and admin behaviour
just test-compat        # PostgreSQL wire and client behaviour
just test-sql           # sqllogictest golden cases
```

## Crash, partition, and concurrency tests

These are slower and target specific failure classes:

```bash
just test-fault        # crash and fault-injection tests
just test-partition    # Raft partition regression over real TCP
just test-loom         # loom model-checked concurrency for the transaction manager
just test-mixed-binary # pinned historical/current-reader processes (slow, opt-in)
```

`just test-sim` is an alias for `just test-partition`. Despite the historical
`sim_test.rs` filename, that test uses ordinary Tokio and real TCP; it is not a
deterministic simulation, and its hand-written operation history does not prove
linearizability under concurrent traffic. The recipe first asserts the expected
test exists, then runs it, so a silently renamed or skipped test fails the job
rather than passing vacuously.

The [mixed-binary gate](test-mixed-binary-upgrades.md) builds two pinned production
revisions and checks upgrade, snapshot, admission and rollback behavior. It is
ignored by ordinary workspace tests. Its diagnostic mode records known blockers
and continues with explicit workarounds; blockers still make the gate fail.

## Frontend and production container

Install the locked frontend dependencies, then run the component tests and
static checks:

```bash
cd frontend
npm ci
npm run format:check
npm test
npm run lint
npm run build
npm run build-storybook
npm audit --audit-level=high
npm audit --omit=dev --audit-level=high
```

From the repository root, build and smoke-test the production image, including
SQL and the packaged console assets:

```bash
just docker-smoke
```

## Fuzz targets

The fuzz targets live in their own workspace, so they are not built by
`cargo test --workspace`. Check that they still compile:

```bash
just fuzz-check
```

The scheduled AFL workflow runs each target for 10 seconds as a startup and
crash smoke test. Only timeout status 124 is an expected end; every other
nonzero status and any crash artifact fails the job. These short runs do not
establish long-running fuzzing assurance.

## Live infrastructure and restore drills

Run the independent-writer Iceberg gate against a disposable Lakekeeper and
MinIO stack with:

```sh
just test-iceberg-live
```

It requires Docker Compose and `uv`, runs without external credentials, and
writes `target/iceberg-live/report.json`. See
[test Lakekeeper interoperability](test-lakekeeper-interoperability.md) for
the pinned versions, checks, and cleanup behavior.

The persistent filesystem restore drill needs no external service:

```bash
cargo test -p nodus_integration_tests --test restore_drill \
  persistent_full_incremental_pitr_restore_drill --locked -- --nocapture
```

The `Live MinIO and OTLP recovery gates` CI job builds pinned official MinIO
source releases, creates an isolated bucket, starts a pinned OpenTelemetry
Collector, and runs these focused tests:

```bash
cargo test -p nodus_backup --locked \
  s3_live_backup_verify_restore_delete_and_failures -- --nocapture
cargo test -p nodus_telemetry --locked otlp_export_smoke -- --nocapture
cargo test -p nodus_integration_tests --test restore_drill --locked \
  s3_full_incremental_pitr_restore_drill -- --nocapture
```

Set the S3 and OTLP variables described in
[back up and restore](back-up-and-restore.md#run-restore-drills) before running
these locally. The tests print an explicit skip when their endpoint variable is
absent; CI always supplies it. The Compose MinIO image can be built locally with
`docker build -f deploy/docker/minio.Dockerfile -t nodus-minio:dev .` if you do
not use the CI job's Go-based service setup.

Restore drills write JSON recovery evidence and Prometheus textfile metrics under
`target/restore-drill/`. CI uploads the S3 report even when the job fails.

## Mixed-activity smoke drill

Run the opt-in PR2 workload with backup, automatic compaction, snapshot-required
follower catch-up, restart, split, and rebalance activity:

```bash
NODUS_SOAK_ARTIFACT_DIR=target/pr2-mixed-activity-smoke \
cargo test --release --locked -p nodus_integration_tests \
  --test mixed_activity_soak -- --ignored --nocapture
```

The [workload definition](../../benchmarks/workloads/mixed_activity_soak.md)
describes the fixed schedule and versioned artifacts. The drill checks exact
acknowledged rows on every node and records resource observations; its short
observation window does not prove bounded resources, production budgets, or the
final 24-hour soak gate.

## Running one test by name

Any suite can be filtered. For example, the shard migration protocol tests:

```bash
cargo test -p nodus_server migration_tests --locked
```

## Choosing what to run

Match the suite to the risk of the change:

- Touching storage, WAL, SSTables, the catalog, or backup metadata → add
  `just test-fault`.
- Touching MVCC, transactions, or locking → add `just test-loom`.
- Touching Raft, routing, or replication → add `just test-partition`.
- Touching SQL semantics → prefer a golden case in the sqllogictest suite over
  Rust test scaffolding.
- Touching the wire protocol or a client-visible type → add `just test-compat`.
