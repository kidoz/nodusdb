# Iceberg reader resource baseline — 2026-11-03

The initial and final recipe captures of the [fixed workloads](../workloads/iceberg_resources.md)
passed exact source/output verification, SIGINT, deadlines and cleanup. Each
capture used three fresh CLI processes per workload. The second applied the
new regression budgets. These are development-profile observations on one
machine, with no production throughput or universal memory-limit claim.

## Environment and provenance

- Apple M4 Max, arm64, 16 logical CPUs, 48 GiB RAM; Darwin 27.2.0.
- Docker: 16 CPUs, about 7.75 GiB RAM, overlayfs storage driver.
- Rust 1.99.0 (`b940084d7`), Cargo development profile; Python 3.12.16,
  PyIceberg 0.12.0, PyArrow 26.0.0.
- Digest-pinned Lakekeeper 0.14.2/PostgreSQL and the existing source-pinned
  MinIO image, all in disposable loopback-only Compose projects.
- Both captures used CLI SHA-256
  `e038eb88033a2edd9cbf761618b944d4e981aab6deeda231b037041f540013af`.
- Workload SHA-256:
  `cb516dfb09052fd9f1f4c59d444a3b07e6c193a8c2626505b4b4f4039d6cf9ef`.
- Initial report SHA-256:
  `a10a3bd7e15e8313f8d20a4ac22ae9ffcc79b389a133cc84f656b27602ab2f93`.

Generated evidence is under `target/iceberg-live/resource-baseline.json` and
`target/iceberg-live/resource-report.json`. It includes individual samples,
snapshot/layout checks, metadata fingerprints, hardware and disk context,
descriptive variability and cleanup results. Generated files are not committed.
No cache flush or machine isolation was imposed. An independent PyIceberg scan
preceded each workload's samples. Source preparation and result verification
are excluded from native CLI timings and RSS.

## Observations

Initial-capture ranges, with three samples per row:

| Workload | Peak CLI RSS | Observed first batch | Total CLI time |
| --- | ---: | ---: | ---: |
| small | 51.6–57.4 MiB | 0.076–0.189 s | 0.862–1.241 s |
| large | 52.5–62.3 MiB | 0.062–0.067 s | 3.107–5.944 s |
| many_files | 44.3–44.9 MiB | 0.063–0.064 s | 1.951–1.964 s |
| wide | 293.5–293.8 MiB | 0.085–0.087 s | 1.064–1.165 s |

Budget-enforced repeat, min / median / max:

| Workload | Peak CLI RSS (MiB) | First batch (s) | Total CLI time (s) |
| --- | --- | --- | --- |
| small | 48.0 / 49.9 / 52.6 | 0.065 / 0.065 / 1.064 | 0.477 / 0.488 / 1.501 |
| large | 50.5 / 50.9 / 58.6 | 0.064 / 0.065 / 0.066 | 2.749 / 2.761 / 2.856 |
| many_files | 44.1 / 44.5 / 44.8 | 0.043 / 0.066 / 0.069 | 1.731 / 1.747 / 1.762 |
| wide | 293.5 / 293.6 / 293.6 | 0.063 / 0.066 / 0.067 | 0.997 / 1.048 / 1.081 |

The eightfold row-count increase at a fixed 32 files and 512-byte width changed
median RSS by about 0.974× in the first capture and 1.019× (+1.0 MiB) in the
final recipe capture. This supports a low memory-scaling observation for this particular
layout. It does not establish a bound for arbitrary data and metadata. The final
small-table first-batch observations ranged from 0.065 to 1.064 seconds; the
report retains that variation and no timing cause was profiled.

The 8 KiB-wide case consumed about 294 MiB even though its row count was lower.
That cost is recorded with a separate 384 MiB regression ceiling. The gate did
not profile its allocation cause. A row-count batch limit must not be treated as
a byte-memory quota when planning SQL integration or low-memory deployments.

## Interruption evidence

The full paced control returned exact rows through signed MinIO range reads.
SIGINT was sent only after a complete private IPC batch was decoded. The CLI
returned its expected cancellation error, exited in about 25 ms after signaling,
and left neither destination nor temporary file. The two-second native deadline
similarly ran after data consumption started and exited at 2.034 s in the first
capture and 2.024 s in the final recipe, with the expected deadline error and no files.
The pacing fixture delays only Parquet GETs; these times do not represent normal
scan throughput. Both runs removed their Compose services and datasets.

## Budgets and practical limits

The committed ceilings retain headroom for CI: 128 MiB for small/large, 96 MiB for
many_files and 384 MiB for wide; a large/small RSS ratio of at most 1.75× and
increase of at most 48 MiB. First-batch ceilings are 3 s; total ceilings range
from 15 to 45 s. SIGINT exit and deadline grace each have a 2 s ceiling. The
exact values and workload hash live in `tools/testing/iceberg/resource-budgets.json`.

These limits target gross regressions and stuck work. They were selected from
the initial measured ranges and then passed by the independent second capture;
they are not fine-grained performance targets or production SLOs. Per-sample
peaks and three-sample variance remain in the JSON, including descriptive
Student-t mean intervals whose small sample size and cache assumptions limit
inference. The CI step is configured; hosted Linux execution of the new gate was
not observed during this task. TLS, real AWS, arbitrary Parquet layouts, sustained
loads and byte-aware runtime quotas remain separate evidence requirements.
