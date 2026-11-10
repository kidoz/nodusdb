# Benchmarks

Criterion benchmark source belongs beside the crate it measures, under
`crates/<crate>/benches`. The external Iceberg resource gate lives alongside its
independent-writer harness under `tools/testing/iceberg`.

Use this directory for benchmark plans, workload descriptions, captured reports, and comparison notes. Do not store generated Criterion output here; it belongs under `target/criterion`.

Current benchmark entrypoints:

```bash
just bench
cargo bench --locked -p nodus_integration_tests --bench server_bench
just bench-soak
python3 -B tools/testing/bench_floor.py target/server-bench/<run-id>
just test-iceberg-resource-live
```

The second command runs the [server SQL workloads](workloads/server_sql.md).
See [Run the benchmarks](../docs/how-to/run-benchmarks.md) for smoke mode and
result-recording rules. The [unapproved resource-budget proposal](reports/resource-budget-proposal-2026-11-08.md)
records the current sustained-load observations and the decisions that remain
with maintainers. The [2026-11-10 sustained-load drill](reports/mixed-activity-sustained-2026-11-10.md)
records the first one-hour sustained run of the mixed-activity drill with
independent validation, its resource trends, and the meta-group quorum
instability finding. The [2026-11-06 write-batching resource comparison](reports/server-write-batching-resources-2026-11-06.md)
contains the current repeated latency, throughput, memory, queue, and Raft-log
observations and their evidence limits. The
[2026-10-27 write-batching comparison](reports/server-write-batching-2026-10-27.md)
and [2026-10-24 server baseline](reports/server-baseline-2026-10-24.md) retain the
earlier measurements. The
[release qualification manifest](reports/release-qualification-2026-10-25.md) records
the clean-commit local, hosted-CI, recovery, and release-gate evidence.

The Iceberg command runs the [external-reader resource workloads](workloads/iceberg_resources.md)
with exact-result verification and fixed regression budgets. Its
[initial baseline](reports/iceberg-resources-2026-11-03.md) records local observations
and the limits of that evidence.
