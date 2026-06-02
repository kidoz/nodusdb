# Benchmarks

Benchmark source code belongs beside the crate it measures, under `crates/<crate>/benches`.

Use this directory for benchmark plans, workload descriptions, captured reports, and comparison notes. Do not store generated Criterion output here; it belongs under `target/criterion`.

Current benchmark entrypoint:

```bash
just bench
```
