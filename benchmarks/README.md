# Benchmarks

Benchmark source code belongs beside the crate it measures, under `crates/<crate>/benches`.

Use this directory for benchmark plans, workload descriptions, captured reports, and comparison notes. Do not store generated Criterion output here; it belongs under `target/criterion`.

Current benchmark entrypoints:

```bash
just bench
cargo bench --locked -p nodus_integration_tests --bench server_bench
```

The second command runs the [server SQL workloads](workloads/server_sql.md).
See [Run the benchmarks](../docs/how-to/run-benchmarks.md) for smoke mode and
result-recording rules. The [2026-10-27 write-batching comparison](reports/server-write-batching-2026-10-27.md)
contains the current repeated measurements and evidence limits; the
[2026-10-24 server baseline](reports/server-baseline-2026-10-24.md) retains the
earlier single-run baseline. The
[release qualification manifest](reports/release-qualification-2026-10-25.md) records
the clean-commit local, hosted-CI, recovery, and release-gate evidence.
