# Run the benchmarks

NodusDB measures the storage engine with [Criterion](https://bheisler.github.io/criterion.rs/book/).
Benchmarks live beside the crate they measure:

```text
crates/nodus_storage_lsm/benches/engine_bench.rs
```

## Run them

```bash
just bench
```

That is `cargo bench -p nodus_storage_lsm`. Criterion writes its HTML reports
and raw samples under `target/criterion`, which is build output — never commit
it.

## Run the server SQL benchmarks

The server harness exercises real pgwire connections, persistent storage, and
an optional three-node mTLS cluster:

```bash
cargo bench --locked -p nodus_integration_tests --bench server_bench
NODUS_BENCH_SCALE=smoke cargo bench --locked -p nodus_integration_tests --bench server_bench
```

The full run uses the fixed datasets in
[Server-level SQL workloads](../../benchmarks/workloads/server_sql.md) and can
take a long time on fsync-bound storage. Smoke mode uses 1/100 of the dataset
and skips the cluster variant. Run one workload by appending a filter, for
example `-- point_read`. Do not run another build or test suite during a
baseline capture.

In addition to Criterion output, each run writes per-request latency, RSS, and
Prometheus evidence to `target/server-bench/<run-id>/measurements.jsonl`. The
JSONL schema and measurement semantics are documented with the workloads. To
place the artifact in a specific directory, set it explicitly:

```bash
NODUS_BENCH_SCALE=smoke \
NODUS_BENCH_ARTIFACT_DIR=target/server-bench-smoke \
cargo bench --locked -p nodus_integration_tests --bench server_bench -- --noplot
```

The output directory must not already contain `measurements.jsonl`; use a fresh
directory for each run. Generated artifacts stay under `target/` and are not
committed.

## Check that they still compile

Compiling the benchmarks is much faster than running them, and it is what CI
does on every push:

```bash
cargo bench --no-run -p nodus_storage_lsm
```

Run this after any change to the storage engine's public surface; a benchmark
that no longer compiles is a broken build that ordinary `cargo test` will not
catch.

## Compare two revisions

Criterion compares each run against the previous one stored in
`target/criterion`, so the order matters:

1. Check out the baseline revision and run `just bench`.
2. Return to your change and run `just bench` again.
3. Read the `change` column in the output, or open
   `target/criterion/report/index.html`.

Do not compare numbers across machines, and do not trust a single run on a
laptop that is thermally throttling or running a build in another terminal.

## Record what you measured

`benchmarks/` holds the durable part of benchmarking:

- `benchmarks/workloads/` describes the workloads being measured, so a result
  can be reproduced later.
- `benchmarks/reports/` holds result notes worth keeping.

A number without its workload description and hardware context is not a result.
