# PostgreSQL dump import

Why importing a `pg_dump` script into NodusDB is built the way it is: a
sink-agnostic translation library rather than a hopeful replay of arbitrary SQL.

**Status.** The library, the CLI command, the admin endpoint, and server-side
`COPY FROM STDIN` all exist. Sequences, identity columns, and deferred
constraints do not. Current-state claims here were checked against the code on
2026-09-12; the original architecture review is dated 2026-06-25.

The user-facing rules are in
[PostgreSQL dump compatibility](../reference/postgres-dump-compatibility.md),
and the procedure is in
[import a PostgreSQL dump](../how-to/import-a-postgresql-dump.md).

## The problem

A stock `pg_dump` script is not a neutral SQL file. It is a program written
against PostgreSQL's exact feature set, and it assumes things NodusDB does not
provide: sequences behind `SERIAL` columns, constraints added after the data
loads, a rich type system, session-setup commands, and ownership and privilege
statements.

There are two honest responses. One is to declare a supported dump profile and
translate everything inside it, reporting anything outside. The other is to
accept arbitrary dumps and quietly drop what does not fit. The second option
produces a database that looks like it imported successfully and is missing rows
— the worst possible failure for a system whose entire value proposition is not
losing data.

NodusDB takes the first. Every construct is translated, skipped with a recorded
reason, or reported as an error. Nothing is silently lost.

## Anatomy of a plain dump

A plain-format dump (`pg_dump -Fp`) is an ordered script with three logical
sections:

- **pre-data** — `CREATE SCHEMA`, `CREATE TABLE`, `CREATE TYPE`, `CREATE
  SEQUENCE`, `CREATE FUNCTION`, and column defaults. Structure.
- **data** — table contents, by default as `COPY <table> (...) FROM stdin;`
  followed by tab-separated rows terminated by a lone `\.`, plus sequence values
  via `setval`.
- **post-data** — `CREATE INDEX`, `ALTER TABLE ... ADD CONSTRAINT` for primary
  keys, foreign keys, unique and check constraints, triggers, and `COMMENT ON`.

It is also full of session setup an importer must tolerate: `SET
statement_timeout`, `SET search_path`, `set_config`, `\connect`, and ownership
and privilege noise.

The consequence that shapes the whole design is in post-data. Stock dumps add
constraints *after* loading the rows, relying on late constraint creation.
NodusDB enforces constraints immediately and has no `ALTER TABLE ... ADD
CONSTRAINT`. Replaying a dump statement by statement therefore fails at exactly
the point where the schema is supposed to become correct.

## Two layers, one set of rules

The design splits into a library and its sinks:

```text
                    ┌─────────────────────────────────────────────┐
   dump.sql ─────►  │                nodus_import                 │
 (plain pg_dump)    │                                             │
                    │  ┌───────────┐  ┌───────────┐  ┌─────────┐  │
                    │  │  Splitter │─►│ Classifier│─►│ Rewriter│  │
                    │  │ (stmts +  │  │ (section, │  │ (trans- │  │
                    │  │  COPY     │  │  kind)    │  │  late / │  │
                    │  │  blocks)  │  └───────────┘  │  skip)  │  │
                    │  └───────────┘                 └────┬────┘  │
                    │  ┌─────────────────┐                │       │
                    │  │ COPY text/CSV   │────────────────┤       │
                    │  │ decoder → rows  │                │       │
                    │  └─────────────────┘                ▼       │
                    │                            ┌─────────────┐  │
                    │                            │ Import Sink │  │
                    │                            └──────┬──────┘  │
                    └───────────────────────────────────┼─────────┘
                                                        │
                  ┌─────────────────────────────────────┴────────────────┐
                  ▼                                                      ▼
         Admin import endpoint                            COPY FROM STDIN handler
   POST /api/v1/import → plan → execute            decode CopyData → exec_insert
```

`nodus_import` does no I/O of its own beyond reading the dump stream. It emits a
typed event stream and a final `ImportReport`, and both consumers — the admin
import endpoint and the wire protocol's `COPY` handler — share it. Writing the
translation rules once is the entire point: a rule that exists in two places
will eventually disagree with itself.

**The splitter** is a streaming, line-aware tokenizer rather than a SQL parser.
It accumulates statements until a `;` at statement depth, outside string
literals, dollar-quoted bodies, and comments; switches to raw line mode inside a
`COPY` block until a lone `\.`, mirroring `psql`; and surfaces meta lines for
the classifier to drop. Streaming is not an optimisation — dumps are routinely
multi-gigabyte, and slurping one into memory would violate the project's bound
on unbounded memory growth.

**The classifier** tags each statement with a section and kind, using
`sqlparser` ASTs where a clean parse is available and a cheap keyword probe
where it is not. `CREATE SEQUENCE` is the motivating example: `sqlparser`
rejects it, and "unparseable" is not the same as "unknown".

**The rewriter** applies deterministic, table-driven rules, each producing
exactly one of translate, skip with a warning, or hard error. Every decision is
recorded, which is what makes the final report a complete account of the import
rather than a summary of the parts that worked.

## Constraint folding

Because constraints cannot be added later, the rewriter buffers pre-data
`CREATE TABLE` statements until it has seen that table's post-data constraints,
then re-emits each table with its primary key, unique, check, and foreign key
constraints inline. Emission order becomes schema, tables, data, indexes.

Foreign key targets must therefore exist before dependent rows load. `pg_dump`
already emits its data section in topological order, so preserving that order is
enough for the ordinary case. Circular foreign keys cannot be satisfied by any
ordering; the tool reports the cycle rather than pretending. A future
`--defer-fk` mode can load all data and validate at the end once deferred
constraints are modelled.

## Why the report is versioned

The `ImportReport` is a versioned JSON artifact, not log output. It records
counts, per-construct skip reasons, lossy coercions, dropped defaults, captured
`setval` calls, and per-table row counts.

It exists for the same reason backup verification exists: an operation that
claims success needs evidence a human or a script can check. "Did this import
completely?" becomes a question with a machine-readable answer, and "clean
import" gets a precise definition — an empty `lossy` array and an empty
`failures` array.

## Delivery, and what is left

1. **Phase 1 — the library and a CLI command.** Splitter, classifier, rewriter,
   constraint folding, batched replay, and the report. *Done.*
2. **Phase 2 — inline `COPY` decoding.** The text and CSV decoder, so stock
   plain dumps import without `--inserts`. *Done.*
3. **Phase 3 — server-side `COPY FROM STDIN`.** The wire handler decodes frames
   and routes rows to the executor under one transaction per `COPY`, and the
   admin endpoint accepts a streamed dump body. *Done.*
4. **Phase 4 — fidelity.** Model sequences and identity, deferred foreign keys,
   and first-class `DATE`, `TIMESTAMP`, `NUMERIC`, and `UUID` types. *Not
   started.* Each item independently removes an entry from the lossy list.

Two limitations are worth naming because they surprise people. Column defaults
are now modelled in the catalog, but the importer still strips them, because a
default of `nextval('seq')` is meaningless without sequences — the dump's
explicit data values are what actually preserve the rows. And an import is not
idempotent: re-running the same dump reports conflicts rather than reconciling
them, which is the conservative behaviour but not the convenient one.

## Invariants this design protects

- **Silent data loss is unacceptable.** Every statement lands in exactly one of
  executed, skipped, or failed.
- **Lossy coercions are visible.** Type downgrades and dropped defaults are
  recorded per column.
- **Ordering is tested.** Constraint folding and parent-before-child loading are
  covered by tests with foreign key chains, including a circular case.
- **Versioned, audited, bounded.** The report format is versioned, the
  server-side import path is authorized and audited, and every path streams with
  bounded batches.
