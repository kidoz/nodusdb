# Test suites

Where tests live and what each place is for. For the commands, see
[run the test suites](../how-to/run-the-test-suites.md).

## Layout

| Location | Contains | Runner |
| --- | --- | --- |
| `crates/<crate>/src` | Unit tests next to the code they verify. | `just test` |
| `tests/integration` | Starts NodusDB and checks server and admin behaviour end to end. | `just test-integration` |
| `tests/compatibility` | PostgreSQL wire and client behaviour, including the JDBC and Npgsql driver suites. | `just test-compat` |
| `tests/sqllogictest` | SQL golden cases and the runner that executes them. | `just test-sql` |
| `tests/fault` | Crash, recovery, and fault-injection tests. | `just test-fault` |
| `tests/distributed` | Real-TCP partition and distributed behaviour tests. | `just test-partition` |
| `tests/fuzz` | Fuzz targets. A separate workspace, deliberately outside the main test flow. | `just fuzz-check` |
| `crates/nodus_txn` (loom cfg) | Model-checked concurrency proofs for the transaction manager. | `just test-loom` |
| `crates/nodus_storage_lsm/benches` | Criterion benchmarks for the storage engine. | `just bench` |

## Notes on individual suites

**`tests/distributed`.** The partition regression lives in `sim_test.rs`, but
the name is historical: it runs on an ordinary Tokio runtime over real TCP, not
under a deterministic simulator. Its operation history is hand-written and does
not constitute a linearizability proof under concurrent traffic.

`mixed_binary.rs` is a separate opt-in process test driven by
`just test-mixed-binary`. It builds two full Git revisions and retains binary hashes,
process logs and machine-readable results. See the
[mixed-binary guide](../how-to/test-mixed-binary-upgrades.md). A diagnostic run can
complete its checkpoints with explicit restarts while still failing the gate for
snapshot authorization blockers; this is not an uninterrupted-upgrade certificate.

**`tests/fuzz`.** A separate Cargo workspace with its own lockfile, so
`cargo test --workspace` does not build it. Corpora and crash artefacts are not
checked in.

**Loom tests.** Only run under `RUSTFLAGS="--cfg loom"`; they are compiled out of
ordinary builds.

**Fault injection.** `nodus_testkit::FaultInjector` covers component-level fault
injection. The storage engines do not consume it — the low-level crates cannot
depend on the testkit — so storage crash residue is reproduced directly on disk
instead. See the [durability contract](durability-contract.md) for the resulting
matrix.
