# Engineering approach

Why NodusDB's Rust code is written the way it is. This page argues for the
defaults; the checkable form lives in
[prepare a change for review](../how-to/prepare-a-change-for-review.md), the
crate boundaries in the [crate map](../reference/crate-map.md), and the
properties themselves in
[correctness invariants](../reference/correctness-invariants.md).

The project's bias is stated plainly: in a database, correctness, durable format
compatibility, security, and reviewability matter more than clever
abstractions. Nearly every rule below follows from that ordering.

## Baseline

Stable Rust, Edition 2024, with workspace dependency versions pinned in the root
`Cargo.toml` so every crate agrees on what it is building against.

Cargo features stay *additive*. A feature may enable extra behaviour; it must
never remove an API or quietly change durable or network format semantics. A
feature flag that changes what bytes land on disk turns a single binary into two
incompatible products that are hard to tell apart after the fact.

`unsafe` is avoided. Where it is genuinely necessary, it is isolated to the
smallest possible block and documented with the invariant that makes it sound —
the documentation is the point, since the compiler has stopped helping.

Errors are modelled with `thiserror` in libraries and flattened with `anyhow`
only at binary and CLI boundaries, where the error is being reported rather than
matched on. `unwrap()` and `expect()` do not belong in production paths unless
the invariant is local, obvious, and written down; tests are free to use them,
because a failed assumption there should abort the test.

## Architecture

The component map exists so that new code has an obvious home. Check it before
introducing a crate or a dependency — the common case is that the concern
already belongs to someone.

When a file grows unwieldy, the first move is to split it into private modules
inside the same crate. Extracting a crate is a heavier step, justified by a real
architectural boundary, a distinct dependency set, or an API several crates
genuinely need. Crates are not a filing system; each one is a compilation unit,
a dependency edge, and a versioning surface.

Crate roots stay small — private modules and a deliberate `pub use` surface.
`pub mod` as a shortcut leaks internal structure into the public API, where it
becomes something callers depend on by accident.

`nodus_server` and `nodus_cli` orchestrate. They are the two places where it is
easiest for core database logic to accumulate, and the hardest places to test it
once it has.

## Compatibility surfaces

Some code is ordinary, and some code writes bytes that a future binary will have
to read. Storage layouts, WAL records, SSTables, catalog records, Raft messages,
backup metadata, and network protocol records are all in the second category.

Treat a change to any of them as a compatibility surface: add or update version
fields, keep old records decodable, and describe the migration. The asymmetry is
worth internalising — a bug in ordinary code is fixed by deploying a fix, while
a format mistake is fixed by migrating everyone's data.

## Async and concurrency

Tokio runs the async code, under a few rules that exist because violating them
produces failures that are invisible in tests and obvious in production:

- Never block on disk or long CPU work inside an async task; move it to an
  appropriate blocking boundary. One blocked worker silently degrades every
  other task sharing it.
- Do not hold a mutex guard across `.await` unless the lock type and lock
  ordering were designed for it.
- Give long-running background work an explicit shutdown path that observes
  cancellation, and make it emit tracing.
- Wrap request handling, recovery, compaction, backup, and shard movement in
  `tracing` spans. These are precisely the operations you will need to explain
  after the fact.

## Errors

An error message is an operational interface. Make it actionable: name the
operation that failed, carry stable identifiers (table, shard, transaction,
backup, path), and preserve the source with `#[source]` or `?`.

User-facing SQL and wire errors map to stable SQLSTATE values, because clients
branch on them. And distinct correctness failures must not be collapsed into one
generic internal error before tests or callers have had a chance to distinguish
them — a generic error is a debugging session you have decided to have later.

## Testing

Tests are matched to the risk of the change rather than applied uniformly.

Unit tests live beside the implementation; cross-crate behaviour lives under
`tests/`. SQL semantics are usually clearer as a golden case in the sqllogictest
suite than as Rust scaffolding. Crash recovery, WAL, SSTable, backup, and
catalog durability changes need a fault or recovery test — the failure modes
only appear when something is interrupted. MVCC, transaction, and locking
changes are candidates for loom, which explores interleavings a test run will
not. Parser, codec, storage, and protocol edge cases are candidates for property
tests or fuzz targets, where the interesting inputs are the ones nobody thought
to write down.

For a small local change, run the narrow crate test first and widen once the
touched crate is shared or the behaviour is user-visible.

## Dependencies

Before adding a crate: check whether the workspace already has something
suitable, verify the crate is maintained and compatible with the current
toolchain, and prefer small focused libraries over broad frameworks for database
internals. Keep dependency upgrades separate from behaviour changes where
practical, so a bisect can tell them apart.

Low-level crates are held to a stricter rule: a dependency that would leak into
storage, format, or protocol APIs is a dependency that becomes part of the
on-disk or on-wire contract.

The balance is straightforward. Do not add a dependency to avoid a small amount
of straightforward code. Do add one when it reduces correctness risk in a hard
domain — SQL parsing, protocol handling, cryptography, compression, consensus.

## Documentation

Documentation is updated when a change affects public commands or
configuration, persistent data compatibility, backup and recovery behaviour, SQL
or wire compatibility, security and audit behaviour, or operator-visible
metrics, traces, and logs.

Prefer short pages with commands that can actually be run, and mark illustrative
examples clearly when they cannot.
