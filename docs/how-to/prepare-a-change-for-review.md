# Prepare a change for review

Work through this before handing a change to someone else. It is ordered so the
cheap checks fail first.

## 1. Run the gates

```bash
just check
```

Formatting, `clippy` with warnings denied, and the workspace tests. If you skip
any of it, say which command you skipped and why when you hand the change over.

Add the suite that matches the risk of your change — see
[run the test suites](run-the-test-suites.md) for the mapping.

## 2. Check the change against the invariants

Read [correctness invariants](../reference/correctness-invariants.md) and
confirm your change preserves every one it touches. If a change makes one of
them harder to guarantee, that belongs in the handover text, not in a comment.

## 3. Check the boundaries

- Does the change sit in the crate that owns the concern? See the
  [crate map](../reference/crate-map.md).
- Are new public APIs intentional, and documented where the intent is not
  obvious from the signature?
- Does a new file belong in an existing crate as a private module instead of a
  new crate? Extract a crate only for a real architectural boundary, a separate
  dependency set, or an API several crates need.

## 4. Check compatibility surfaces

If the change touches storage, the WAL, SSTables, catalog records, Raft
messages, backup metadata, or network protocol records, treat it as a
compatibility surface:

- Version fields are present and bumped where the encoding changed.
- Older records still decode, or the migration path is described.
- Mixed-version behaviour does not enable an irreversible format before cluster
  finalization.

## 5. Check the failure paths

- Errors name the operation that failed and carry stable identifiers — table,
  shard, transaction, backup, path.
- Source errors are preserved with `#[source]` or `?`.
- User-facing SQL and wire errors map to stable SQLSTATE values.
- Distinct correctness failures are not collapsed into one generic internal
  error.
- Production paths introduce no `unwrap()` or `expect()` whose invariant is not
  local, obvious, and documented.
- Async code does not block the runtime and does not hold a lock across
  `.await`.
- Security-sensitive actions still pass through authorization and emit audit
  events.
- New long-running work has `tracing` spans and a useful failure signal.

## 6. Check the tests

- At least one test covers the most important correctness path.
- At least one test covers a failure path.
- Crash-recovery, WAL, SSTable, backup, and catalog durability changes have a
  fault or recovery test.
- MVCC, transaction, and locking changes considered loom or simulation tests.
- Parser, codec, storage, and protocol edge cases considered property tests or
  fuzz targets.

## 7. Update the documentation

Update the docs when the change affects public commands or configuration,
persistent data compatibility, backup or recovery behaviour, SQL and
PostgreSQL-wire compatibility, security and audit behaviour, or
operator-visible metrics, traces, and logs.

Put it in the right place: a command belongs in a how-to guide, a contract or
limit in reference, a rationale in explanation. See the
[documentation index](../README.md) for the split.
