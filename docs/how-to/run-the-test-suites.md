# Run the test suites

NodusDB separates tests by scope and runtime cost. This guide covers which
command to run when. For what each suite actually contains, see
[test suites](../reference/test-suites.md).

The project uses [`just`](https://github.com/casey/just) as its task runner;
each recipe is a thin wrapper around `cargo`.

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

## Fuzz targets

The fuzz targets live in their own workspace, so they are not built by
`cargo test --workspace`. Check that they still compile:

```bash
just fuzz-check
```

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
