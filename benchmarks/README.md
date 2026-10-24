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
result-recording rules, and the [2026-10-24 server baseline](reports/server-baseline-2026-10-24.md)
for the current measurements and evidence limits.
