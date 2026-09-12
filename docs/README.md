# NodusDB documentation

NodusDB is an experimental PostgreSQL-wire-compatible distributed SQL database
written in Rust. These pages describe how the system actually behaves, and say
plainly where a design is still a proposal rather than shipped behaviour.

The documentation is organised with [Diátaxis](https://diataxis.fr/), which
separates four different needs. Every page belongs to exactly one of them.

| Section | Orientation | Read it when |
| --- | --- | --- |
| **[Tutorials](tutorials/)** | Learning | You are new here and want a guided first success. |
| **[How-to guides](how-to/)** | Task | You have a goal and need the steps that reach it. |
| **[Reference](reference/)** | Information | You need to look up a contract, a limit, or a surface. |
| **[Explanation](explanation/)** | Understanding | You want the design, the research behind it, and its trade-offs. |

## Tutorials

Lessons for newcomers. Follow them start to finish; they are meant to be typed
out, and every step is one you can complete.

- [Getting started](tutorials/getting-started.md) — run a server, connect with
  `psql`, create a table, and watch the data survive a restart.

## How-to guides

Recipes for people who already know what they want.

- [Import a PostgreSQL dump](how-to/import-a-postgresql-dump.md)
- [Back up and restore](how-to/back-up-and-restore.md)
- [Run the test suites](how-to/run-the-test-suites.md)
- [Run the benchmarks](how-to/run-benchmarks.md)
- [Prepare a change for review](how-to/prepare-a-change-for-review.md)

## Reference

Factual descriptions of the machinery. Look things up here; do not expect to be
taught.

- [Durability contract](reference/durability-contract.md) — what survives a
  crash, and which test proves each case.
- [Correctness invariants](reference/correctness-invariants.md) — properties
  every change must preserve.
- [Crate map](reference/crate-map.md) — which crate owns what.
- [Admin API authorization](reference/admin-api-authorization.md) — auth
  schemes and the route-to-privilege table.
- [PostgreSQL dump compatibility](reference/postgres-dump-compatibility.md) —
  supported dump profile, translation rules, report fields.
- [Backup contracts](reference/backup-contracts.md) — repository layout and the
  rules a backup implementation may not break.
- [Test suites](reference/test-suites.md) — what lives in each test directory.

## Explanation

Design discussion and research. These pages argue for a shape and weigh
alternatives; several describe work that is planned rather than done, and say so
in their opening lines.

- [How durability works](explanation/durability.md)
- [Backup architecture](explanation/backup-architecture.md)
- [PostgreSQL dump import](explanation/postgres-dump-import.md)
- [Shard migration protocol](explanation/shard-migration-protocol.md)
- [Engineering approach](explanation/engineering-approach.md)

## Conventions used here

- **One page, one mode.** A tutorial does not explain internals; an explanation
  page does not hand you commands to paste.
- **Unshipped work is labelled.** A design page that describes a target
  architecture says so in its first paragraph and carries a `Status` line with
  the date its claims were last checked against the code.
- **Examples are real.** Commands and outputs are taken from runs against the
  server, not invented. Illustrative fragments are marked as such.
