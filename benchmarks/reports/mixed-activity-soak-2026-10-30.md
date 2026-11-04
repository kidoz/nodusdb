# Mixed-activity smoke drill — measured 2026-11-01

## Scope and verdict

Attempt 36 of the [mixed-activity smoke drill](../workloads/mixed_activity_soak.md)
passed all nine activities in 2,192.84 seconds (36 minutes 32.84 seconds).
Attempts 46–48 later passed three consecutive repeatability runs with independent
artifact validation. Every run exercised three persistent in-process nodes with
real pgwire and Raft over mTLS, concurrent SQL load, backup verification,
automatic compaction, two snapshot/log-reclamation cycles, graceful follower
restart with snapshot-required catch-up, split/rebalance, and exact post-recovery
validation. Three consecutive passes are an engineering repeatability check, not
PR2 acceptance or proof that every intermittent failure seen earlier is fixed.

This is a bounded PR2 correctness and observability result, not a 24-hour soak,
production SLO approval, capacity guarantee, or general production-readiness
verdict. General production remains **NO-GO**.

## Environment and method

- Machine: MacBook Pro (Mac16,5), Apple M4 Max, 16 cores, 48 GiB RAM.
- OS: macOS 27.2 (26B123), arm64; local journaled APFS on `/dev/disk3s1`.
- Toolchain: rustc 1.99.0 (b940084d7 2026-09-28); cargo 1.99.0
  (5f94df478 2026-08-27).
- Build: Cargo `release` profile, locked dependencies.
- Server: three in-process nodes with persistent temporary LSM directories,
  dedicated mTLS Raft listeners, filesystem backup repository, legacy writes,
  200 ms Raft heartbeat, 600–1200 ms election timeouts, 256 KiB snapshot chunks,
  and a 30-second snapshot RPC timeout.
- Workload: four writers and one reader targeting the seed; 6,000 unique inserts
  with an ID prefix plus 256 value bytes. Migration used a coordinated write
  pause with continued reads, 256-row copy checkpoints, concurrency 16, and a
  120-second step deadline.
- Source recorded by the artifact: `214fdadf5e21b2bfc2145aac42e1da7d2418d0d1`,
  with the worktree marked dirty. The tested working-tree changes were committed
  during the run. A later `if let` cleanup removed a clippy warning without
  changing drill behavior. Pre-existing maintainer files were left untouched.
- Repeatability attempts 46–48 recorded source
  `1f983de9e08425cac1f88a71a2cbf2ff093de5f5`, also with the worktree marked
  dirty. Their final harness used bounded 300-second readiness/rejoin waits, a
  360-second pending-intent drain covering the 300-second abandoned-intent TTL,
  and index creation only after post-split data convergence.
- No other build, test, or benchmark command ran concurrently with the drill.

Command:

```bash
NODUS_SOAK_ARTIFACT_DIR=target/pr2-mixed-activity-smoke-529a186-attempt36 \
cargo test --release --locked -p nodus_integration_tests \
  --test mixed_activity_soak -- --ignored --nocapture
```

## Activity results

Elapsed times below are the recorded activity durations, not a full breakdown of
run time. Exact validation is recorded after its checks complete, so its activity
record is not a validation-latency measurement.

| Activity | Elapsed | Measured result |
| --- | ---: | --- |
| Full backup verified | 0.712 s | Backup `6397de15-e9a6-4510-bfee-51a0af76bfc4` completed and verified |
| Automatic LSM compaction | 0.003 s | 11 successful compactions already observed |
| First snapshot/log reclamation | 363.907 s | 3,984 entries reclaimed; 1,017 retained at observation |
| Graceful follower stop | 5.582 s | Node 3 stopped at follower applied index 5,029 |
| Second snapshot/log reclamation while follower was down | 323.858 s | 4,968 entries reclaimed; 1,032 retained at observation |
| Snapshot-required follower restart | 0.948 s | Node 3 restarted after missing 5,002 log entries, exceeding the 1,032 retained entries |
| Snapshot catch-up verified | 0.001 s | Applied index 10,055 on every node at the check |
| Split and rebalance with read load | 218.135 s | Two shards, phase `done`, rebalanced across three nodes |
| Exact recovery validation | recorded after checks | Passed on every node |

## Workload and validation

| Measurement | Result |
| --- | ---: |
| Acknowledged inserts | 6,000 |
| Write attempts / retries | 6,015 / 30 |
| Uncertain writes recovered by exact-value confirmation | 15 |
| Successful reads / read retries | 238,814 / 86 |
| Write attempts measured for latency | 6,015 |
| Write mean / p50 / p95 / p99 / max | 1.144 / 1.132 / 1.489 / 3.234 / 5.138 s |
| Read attempts measured for latency | 238,900 |
| Read mean / p50 / p95 / p99 / max | 1.561 / 0.903 / 1.967 / 20.365 / 1,770.124 ms |

After the writers and readers stopped and queues drained, every post-split data
group reached the same applied index on all three nodes:

- `shard-46648f10-0f32-4893-9755-39ca99e8843f`: 8,395.
- `shard-a9f3c633-0e5e-4e68-88bb-ce1994111502`: 12,530.

Each node returned exactly 6,000 rows with zero missing, unexpected, duplicate,
or value-mismatched rows. Each node's secondary-index lookup returned exactly one
expected row. Validation used quiescent MVCC reads after per-group applied-index
convergence; it did not use or claim a shared cross-shard linearizable snapshot.

Ten `post_activity_observation` samples covered 9.163 seconds within the
10-second quiet window. Router and commit queue depths were zero on every node in
all those samples. Node 1's router queue high-water was 17 and its commit queue
high-water was 0; these are observations, not queue bounds.

## Resource observations

| Measurement | Start | End | High water |
| --- | ---: | ---: | ---: |
| Process RSS | 8.94 MiB | 901.02 MiB | 1,048.09 MiB |
| Allocator bytes | 0.24 MiB | 10.41 MiB | 857.32 MiB |

The summary deliberately leaves `bounded_memory`, `bounded_queues`, and
`production_budgets_met` null. This short run and its high-water samples do not
establish long-term stabilization or approved resource budgets.

## Independent checks and quality gates

An independent JSON/JSONL assertion pass verified schema version 1, `passed`
status, all nine activities in order, exact-row/index results on all nodes, both
post-split convergence records against the split response, snapshot-required
catch-up arithmetic, source/build metadata, null resource verdicts, zero quiet-
window queue depths, and the observation samples. The event stream contained
1,066 complete records and no partial JSONL record.

Final gates on the tested source plus the nonfunctional clippy cleanup passed:

- `cargo fmt --all -- --check`
- `just clippy`
- `cargo test --workspace --locked`: 103 suites, 924 passed, 0 failed, 6 ignored
- `git diff --check`

The focused mTLS restart regression also passed after being strengthened to
require a post-restart write to reach the rejoined follower.

## Repeatability: attempts 46–48

Three consecutive runs with the final harness passed every activity and the same
independent assertions used for attempt 36. Every node returned exactly 6,000
correct base rows and one expected secondary-index row per run. Both post-split
groups converged on every node, and all quiet-window queue depths were zero.

| Attempt | Duration | Write attempts / retries | Successful reads / retries | Write p95 / p99 | Quiet samples |
| --- | ---: | ---: | ---: | ---: | ---: |
| 46 | 2,103.08 s | 6,015 / 30 | 232,004 / 63 | 1.437 / 2.757 s | 10 samples over 9.182 s |
| 47 | 2,114.74 s | 6,016 / 32 | 238,979 / 56 | 1.488 / 2.980 s | 9 samples over 8.174 s |
| 48 | 2,101.26 s | 6,008 / 16 | 231,249 / 77 | 1.527 / 2.649 s | 10 samples over 9.192 s |

Attempt 47's first and last samples leave less than two seconds of the
10-second window uncovered at either boundary. Attempt 48's additional 4.829
seconds after the last sample are sampler scrape/join time, not a shortened
observation window. All three runs preserve the null bounded-resource verdicts.

The final harness also passed `cargo fmt --all -- --check`, `just clippy`,
`git diff --check`, and `cargo test --workspace --locked` with 103 suites, 924
passed, 0 failed, and 6 ignored.

## Failed development attempts

Attempts 28–45 are preserved under `target/` and are not counted as passing
soaks. They exposed unsupported cross-shard linearizable validation, interrupted
runs, premature catch-up/index checks, post-split write-liveness failure,
index-lookup failures, and a pending-intent drain that was shorter than the
engine's abandoned-intent TTL. Attempt 34 stalled at 3,731 acknowledged writes;
attempts 41, 43, and 44 returned zero rows from node 3's secondary-index lookup.
Not every intermittent failure was root-caused. Three subsequent consecutive
passes improve the evidence but do not prove those intermittent issues are
resolved; scheduled repeated runs remain necessary.

## Artifacts

- `target/pr2-mixed-activity-smoke-529a186-attempt36/summary.json`
- `target/pr2-mixed-activity-smoke-529a186-attempt36/events.jsonl`
- `target/pr2-mixed-activity-smoke-529a186-attempt36-console.log`
- `target/pr2-mixed-activity-smoke-529a186-attempt46/` (`summary.json`,
  `events.jsonl`)
- `target/pr2-mixed-activity-smoke-529a186-attempt47/` (`summary.json`,
  `events.jsonl`)
- `target/pr2-mixed-activity-smoke-529a186-attempt48/` (`summary.json`,
  `events.jsonl`)
- Matching attempt 46–48 console logs remain beside those directories with
  `-console.log` suffixes.

## Limits and remaining gates

- No 24-hour sustained soak or production performance/SLO approval.
- No SIGKILL, machine-crash, power-loss, or lying-device proof in this drill;
  restart was graceful and in-process.
- Filesystem backup only; no S3 evidence and no backup restore in this drill.
- Migration paused writes while reads continued; uninterrupted online writes and
  progress under continuous write pressure were not proven.
- No shared cross-shard linearizable snapshot or cross-shard isolation proof.
- No bounded-memory, bounded-queue, or production-budget claim from short-run
  high-water observations.
- Scheduled repeatability and sustained full-load automation, repeated liveness
  evidence, resource stabilization, approved budgets, and the final 24-hour
  release-candidate soak remain open.
