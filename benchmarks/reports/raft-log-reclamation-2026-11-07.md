# Raft-log reclamation and physical-storage measurements — 2026-11-07

## Scope and result

The [mixed-activity smoke drill](../workloads/mixed_activity_soak.md) passed in
2,080.648 seconds (34 minutes 40.648 seconds) on three persistent in-process
nodes with real pgwire and Raft over mTLS. It completed 6,000 acknowledged
inserts, backup verification, automatic compaction, two real snapshot/purge
cycles, snapshot-required follower catch-up, split/rebalance, and exact restart
validation.

Both snapshot cycles reduced retained entries and serialized Raft payload
bytes. The run also captured periodic logical file lengths, allocated blocks,
and volume free space. Every node returned exactly 6,000 correct base rows and
one expected secondary-index row after all data groups and the meta group
converged. Ten quiet-window samples had zero router and commit queue depth on
every node.

This establishes the measured reclamation mechanism and short-run physical
high-water observations. It does not establish bounded storage, bounded memory,
approved budgets, or general production readiness. General production remains
**NO-GO**.

## Environment and method

- Machine: MacBook Pro (Mac16,5), Apple M4 Max, 16 cores, 48 GiB RAM.
- OS: macOS 27.2, arm64; local journaled APFS on `/dev/disk3s1`.
- Toolchain: rustc 1.99.1 (58cf8f9c3 2026-10-13); cargo 1.99.1
  (5f94df478 2026-08-27).
- Build: Cargo `release` profile, optimized and locked.
- Configuration: legacy writes; 200 ms Raft heartbeat; 600–1200 ms election
  timeouts; 5,000-log snapshot policy; 1,000 entries retained at purge;
  256 KiB snapshot chunks; 30-second snapshot RPC timeout.
- Workload: four writers and one reader targeting the seed; 6,000 unique
  inserts with an ID prefix plus 256 value bytes. Split/rebalance used a
  coordinated write pause with continued reads.
- Base source: `bddbee75faaa866d65f58d2a7d1430eaaff7c1a3`, plus the preserved
  implementation and focused-regression patches listed below. The tested
  working tree was dirty; this was not a clean-commit capture.
- No other build, test, or benchmark command ran concurrently with the drill.

Command:

```bash
NODUS_SOAK_ARTIFACT_DIR=target/raft-reclamation-2026-11-07/run-4 \
cargo test --release --locked -p nodus_integration_tests \
  --test mixed_activity_soak -- --ignored --nocapture \
  > target/raft-reclamation-2026-11-07/run-4-console.log 2>&1
```

## Snapshot and log reclamation

The growth columns are observations before each snapshot. Peak and retained
columns are the values that demonstrate reclamation; continuing writes explain
why retained entries slightly exceed the 1,000-entry purge setting.

| Observation | Entries grown | Serialized bytes grown | Peak entries | Retained entries | Entries reclaimed | Peak payload | Retained payload | Payload reclaimed |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| First snapshot | 324 → 1,507 | 192.34 → 897.52 KiB | 4,995 | 1,012 | 3,983 | 2.93 MiB | 0.59 MiB | 2.33 MiB |
| Second snapshot, follower down | 1,168 → 1,514 | 703.74 → 912.55 KiB | 6,000 | 1,027 | 4,973 | 3.54 MiB | 0.61 MiB | 2.93 MiB |

Before the second restart, the follower had missed 4,997 log entries while only
1,027 remained at the leader observation, so append-only catch-up could not
supply the complete missed prefix. After restart, the original data group
reached applied index 10,064 on every node.

## Physical storage

Samples were collected throughout the run, including workload, maintenance,
restart, and quiet windows. `logical_bytes` sums regular-file lengths in the
whole data directory. `allocated_bytes` sums filesystem-reported 512-byte
allocated blocks on this Unix host. Volume available bytes use `f_bavail`
multiplied by `f_frsize`; all three directories shared one filesystem.

| Node | Successful scans | Scan errors | Logical start | Logical peak | Logical end | Allocated start | Allocated peak | Allocated end |
| --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 1,034 | 1 | 0.30 MiB | 399.71 MiB | 399.71 MiB | 0.31 MiB | 466.90 MiB | 416.08 MiB |
| 2 | 1,034 | 1 | 0.30 MiB | 396.21 MiB | 396.21 MiB | 0.32 MiB | 410.10 MiB | 410.10 MiB |
| 3 | 1,032 | 3 | 0.29 MiB | 419.79 MiB | 361.66 MiB | 0.31 MiB | 431.15 MiB | 373.55 MiB |

The five failed scans returned `No such file or directory` while maintenance
changed directory contents. They remain explicit in the artifacts and are not
counted as zero-sized samples. The validator required at least two complete
samples per node; all samples on this host had allocated-block and free-space
measurements when their directory scan succeeded.

Minimum observed available filesystem space was 37.73 GiB. The largest observed
allocated directory was 466.90 MiB. Logical and allocated peaks need not occur
in the same sample, and physical values must not be inferred from the serialized
Raft payload gauge. One-second scans can miss shorter-lived peaks. Net physical
growth and a lower final value on node 3 do not prove a long-term storage bound.

## Workload and restart validation

| Measurement | Result |
| --- | ---: |
| Acknowledged inserts | 6,000 |
| Write attempts / retries | 6,010 / 20 |
| Uncertain writes recovered by exact-value confirmation | 10 |
| Successful reads / read retries | 227,803 / 112 |
| Write mean / p50 / p95 / p99 / max | 1.087 / 1.081 / 1.489 / 2.480 / 3.936 s |
| Read mean / p50 / p95 / p99 / max | 1.493 / 0.854 / 2.291 / 18.989 / 1,845.066 ms |
| Process RSS start / end / high water | 8.88 / 427.61 / 1,031.70 MiB |
| Allocator start / end / high water | 0.25 / 9.28 / 688.29 MiB |

The two post-split data groups converged at applied indexes 8,393 and 12,511.
The meta group then converged at 48,664 after index creation. Every node had
zero missing, unexpected, duplicate, or value-mismatched base rows, and every
node's secondary-index lookup returned exactly one expected row. Validation
used quiescent MVCC reads, not a shared cross-shard linearizable snapshot.

The memory values describe the whole test process, including three servers and
clients. Their high-water and final observations do not demonstrate long-term
stabilization.

## Focused index-replication checks

A separate optimized regression forced snapshot purge and follower catch-up,
then split and rebalanced 6,000 rows before creating the secondary index. It
passed exact entry and query checks on every node in 556.77 seconds. An earlier
run without split/rebalance also passed in 197.37 seconds. A fast meta-applied
regression passed in 4.71 seconds.

The full drill initially compared follower index entries before meta-group
convergence and exposed a harness timing gap. No production index-code defect
was reproduced by the focused checks. The completed drill now requires
meta-group applied-index convergence before its exact-entry assertions.

## Operational finding and resolution

The first successful console log contained 81,951 OpenRaft append-response
decode errors. Of these, 16,232 referred to `shard-meta` during the
follower-stop/restart window. The remaining 65,719 referred to the retired
pre-split data group and continued through the end of the run. Migration
removed the source only on the driver, leaving other replicas active; a removed
peer's plain-text 404 was also parsed as JSON.

The follow-up implementation removes sources with `Done` migration journals on
every member through the existing reconcile loop, and checks HTTP status before
decoding append, vote, and snapshot responses. Active-migration sources remain
hosted. A focused snapshot/split/rebalance/index regression passed in 589.99
seconds and required source removal on every node.

The full follow-up drill passed in 2,123.209 seconds with
`source_decommissioned_on_every_node=true`, exact rows and indexes, both
reclamation cycles, and zero quiet-window queues. Its log has **zero JSON decode
errors**. Retired-group append traffic stopped 1,005.443 seconds before the run
finished, rather than continuing through completion. During the removal window,
65,203 connection-refused errors and 6,995 explicit HTTP 404 errors were still
recorded, so transient transport log volume remains an operational concern.
The first capture attempt was also terminated by the tool's console-output
limit; later runs redirected output to files.

Follow-up evidence is under `target/raft-teardown-2026-11-08/`, with
independent validation by `summarize.py`. SHA-256 values:

- Events: `839ec6e8bafc7b3b03c34fba63a2e7b1715bd285e1ae60758de3d27f43abde43`
- Drill summary: `fd5806476e5579c60309e0899c148333004e6fe1c32f388573a9e7fe6f95f07e`
- Independent summary: `8aa95af9f8bc9e57a30ca91f87c3859aa2881595b2ae20b93f51ff810817e8a7`
- Implementation patch: `4860c9941a50f8154e0903379c010cdcd9726ffd513106b9d08aa466b1d5dce8`
- Validator: `4b687f48be3b5d1f785c8781c9589a3809a89db450c89447b9a8cf9f185ce2ed`

Revalidate with `python3 -B target/raft-teardown-2026-11-08/summarize.py`.
Expected output begins with `validated 1021 events, 1012 storage sample sets,
two snapshot/purge cycles, restart catch-up, zero quiet queues, and exact rows
on three nodes`.

## Artifacts and independent validation

Generated evidence is under `target/raft-reclamation-2026-11-07/`:

- `run-4/summary.json`: passed drill summary;
- `run-4/events.jsonl`: 1,044 complete events, including 1,035 sample sets;
- `run-4-console.log`: complete console output;
- `implementation.patch` and `index-replication.patch`: tested source changes;
- `summary.json`: independently validated measurements and hashes;
- `summarize.py`: repeatable independent validator.

Focused logs are under `target/index-replication-2026-11-07/`.

SHA-256 values:

- Events: `3e91b5d2a3c8a5376bc4154e9bf4428cf1a9c4e4918237e62233d4be4032a4d1`
- Drill summary: `ad0ad580b5ddda23ccf8d6d35c6adc7ab93e9005323620d2903549e1d163aefa`
- Independent summary: `948aea288938915b6d2d5eb03c7bc18b0a6e9d28ef5e33a5c3bccc18db4402d6`
- Implementation patch: `d48cf3ca28a0bb9c80fc5e083bd14fee1bd4599a2368d1ffa4bc6d6d5d7bebc5`
- Focused-regression patch: `1b0dbaf696d762fdfeef3828f43758b33e695493b15fcee144486b673cc1d703`
- Validator: `19c5a65d16016c65e6fdb9ad5f9aae68a36271a65654eea0a8fb8bce2f86d766`

Revalidate with:

```bash
python3 -B target/raft-reclamation-2026-11-07/summarize.py
```

Expected output begins with `validated 1044 events, 1035 storage sample sets,
two snapshot/purge cycles, restart catch-up, zero quiet queues, and exact rows
on three nodes`.

Earlier incomplete or failed development attempts are preserved under the same
artifact root but are not counted as passing evidence.

## Quality gates

The completed source passed Clippy, formatting, diff checks, and the full
workspace suite: **1,064 tests passed, 0 failed, and 7 intentional ignores
across 111 suites**. The focused roster regression, full follow-up drill, and
both independent artifact validators also passed. These are local working-tree
results, not clean-checkout, hosted-CI, or release-candidate qualification.

## Limits

- One short macOS run and focused checks are not a 24-hour soak, repeated
  supported-profile qualification, or a production resource budget.
- Restart was graceful and in-process. This does not prove SIGKILL, machine
  crash, power-loss, or lying-device recovery.
- Filesystem backup creation and verification were exercised, not restoration
  or S3 behavior.
- Migration paused writes while reads continued. Uninterrupted online writes
  and migration progress under continuous write pressure were not proven.
- Exact validation used per-group convergence and quiescent reads, not a shared
  cross-shard linearizable snapshot or a cross-shard isolation proof.
- Physical peaks can be missed between scans. Long-term storage, memory, and
  queue bounds remain unassessed. Retired-group traffic stopped in the
  follow-up run, but transient connection and HTTP-error log volume remains an
  operational concern.
