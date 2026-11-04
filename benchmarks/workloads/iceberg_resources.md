# Iceberg reader resource workloads

This gate measures the optional CLI's reads from real Lakekeeper/MinIO services.
It runs on Linux and macOS through `just test-iceberg-resource-live`, using the
development Cargo profile and the locked Python environment. It does not run a
NodusDB SQL server. The authoritative workload is `WORKLOAD` in
`tools/testing/iceberg/resources.py`; its hash must match the committed budgets.

| Scenario | Data files | Rows per file | Total rows | Binary width | Nominal payload |
| --- | ---: | ---: | ---: | ---: | ---: |
| small | 32 | 4,096 | 131,072 | 512 B | 64 MiB |
| large | 32 | 32,768 | 1,048,576 | 512 B | 512 MiB |
| many_files | 256 | 1,024 | 262,144 | 512 B | 128 MiB |
| wide | 32 | 1,024 | 32,768 | 8 KiB | 256 MiB |

Payloads are deterministic SHAKE-256 bytes derived from the row ID, with null
payloads every 23 rows. Labels include nulls and empty strings. Payloads resist
compression. Nominal volumes assume every payload is present; actual sizes
reflect nulls, labels, IDs and format overhead.
PyIceberg writes one file per append, with a 4,096-row Parquet row-group limit,
32 MiB target file size and Zstandard compression. Actual file counts, compressed
sizes, manifest counts and pinned snapshot IDs are recorded. Layout drift fails.
Objects and snapshots remain immutable throughout measurement.

The large/small comparison increases row count eightfold at a fixed file count
and width. The many-file and wide-row cases independently exercise metadata and
variable-width data costs. These shapes fit the reader's existing manifest and
64 MiB single-range limits; they do not establish support for arbitrary large
column chunks or metadata.

## Correctness and measurement

PyIceberg independently streams and verifies each source before the CLI samples.
Every CLI result is also streamed and checked for exact types, column order,
values, nulls and ID coverage. A one-byte-per-ID bitmap detects every missing or
duplicate row regardless of output order. The checker does not materialize the
entire result or substitute counts or probabilistic aggregate hashes.

Each workload runs in three fresh CLI processes. Measurements exclude writer,
reference-reader and output-verification time and memory. `posix_spawn` avoids
forking the large Python/Arrow address space; `wait4` collects the exact CLI
child's native RSS high-water mark and CPU times. macOS RSS bytes and Linux RSS
KiB are normalized to bytes. Native canary tests verify units, independent child
measurements and timeout reaping.

Time to first batch is the elapsed time until a polling observer decodes the
first complete IPC record batch from the CLI's private temporary file or final
output. A schema header or truncated message cannot qualify. This is an observed
upper bound with 20 ms polling and decode overhead, rather than an internal
library timestamp. Total time runs from process spawn through observed exit,
including output synchronization and publication. No OS cache flush is performed;
an independent source read precedes the samples.

The report records every sample, min/median/max, sample standard deviation and a
Student-t 95% mean interval for three samples. Such small-sample intervals are
descriptive and assume independent repeated observations; cache and host effects
limit interpretation. Hardware, RAM, Docker CPU/RAM, scratch disk capacity, local
compiler, binary/profile, configuration and workload fingerprints accompany them.

## Interruption checks

A loopback HTTP fixture streams requests to the real MinIO service without
changing Host, authorization, range headers or bodies. It adds 100 ms before each
Parquet GET, so termination tests reliably start consuming data before finishing.
Data remains independently written and MinIO still verifies the signatures. The
fixture streams response bodies in 64 KiB chunks and logs no request material.

A full paced small-table scan is checked first. Then the many-file scan receives
SIGINT immediately after a complete IPC batch is observed, and another scan uses
the CLI's two-second read deadline. Both must emit the expected error, fail with
nonzero status, reach real Parquet reads, and leave neither a published output
nor a private temporary file. Exit latency is measured. A separate wall guard
kills and reaps a hung child and fails the gate; it never counts as successful
native cancellation. Proxy threads, Compose services and datasets are cleaned up.

## Regression budgets

`tools/testing/iceberg/resource-budgets.json` contains explicit, workload-hashed
ceilings established after the [initial baseline](../reports/iceberg-resources-2026-11-03.md).
Each unpaced scan sample must pass its limits; scaling uses the median RSS
values. Paced control and interruption samples are recorded separately and use
the interruption checks below.

| Scenario | Peak RSS | First complete batch | Total CLI time |
| --- | ---: | ---: | ---: |
| small | 128 MiB | 3 s | 15 s |
| large | 128 MiB | 3 s | 45 s |
| many_files | 96 MiB | 3 s | 30 s |
| wide | 384 MiB | 3 s | 20 s |

Large/small median RSS must stay within both 1.75× and a 48 MiB increase.
SIGINT must exit within two seconds of signaling. The deadline scan may take at
most two seconds beyond its two-second read deadline to exit. These are broad
CI regression limits for these development-profile fixtures. They are checked
after execution and are not runtime memory quotas, production SLOs, or proof of
bounded memory for arbitrary schemas and Parquet layouts. The wide-row case's
substantially higher RSS remains visible in its separate ceiling.

To capture a diagnostic baseline without applying budgets, pass
`--record-baseline` to `resources.py` and choose a separate `--report` path. Such
a run reports `baseline_recorded` and `budgets_enforced: false`, never `passed`.
Correctness and interruption checks remain mandatory. CI never uses that flag.
