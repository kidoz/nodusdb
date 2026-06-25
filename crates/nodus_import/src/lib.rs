//! `nodus_import` — a sink-agnostic engine for importing a plain-format
//! PostgreSQL `pg_dump` script into NodusDB.
//!
//! The pipeline is **Splitter → Classifier → Rewriter → COPY decoder → typed
//! event stream**. The engine reads a dump, folds post-data `ADD CONSTRAINT`
//! back into the originating `CREATE TABLE` (NodusDB enforces constraints
//! immediately and has no `ALTER TABLE ... ADD CONSTRAINT`), strips unsupported
//! constructs with a recorded reason, decodes `COPY` data into batched
//! `INSERT`s, and drives an [`ImportSink`]. Nothing is silently dropped: every
//! skip, lossy coercion, and failure lands in the versioned [`ImportReport`].
//!
//! The engine itself performs no I/O and is not coupled to the executor — the
//! caller supplies an [`ImportSink`] (the server provides one backed by the
//! in-process executor).

mod copy_decoder;
mod splitter;

pub use copy_decoder::{Cell, CopyFormat, CopySpec, decode_rows, parse_copy_header};
pub use splitter::{RawUnit, is_copy_from_stdin, split};

use serde::{Deserialize, Serialize};
use sqlparser::ast::{AlterTableOperation, ColumnOption, ObjectType, Statement, TableConstraint};
use sqlparser::dialect::PostgreSqlDialect;
use sqlparser::parser::Parser;

/// Current on-disk/wire version of [`ImportReport`].
pub const IMPORT_REPORT_VERSION: u32 = 1;

/// How the engine reacts when a sink rejects a statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OnError {
    /// Stop the import at the first failing statement.
    Stop,
    /// Record the failure and continue with the next statement.
    Continue,
}

/// Tunables for an import run.
#[derive(Debug, Clone)]
pub struct ImportOptions {
    /// Maximum rows folded into a single synthesized `INSERT`.
    pub batch_rows: usize,
    /// Failure policy.
    pub on_error: OnError,
    /// Strip column `DEFAULT`/identity clauses (NodusDB has no sequences).
    pub strip_defaults: bool,
    /// Emit `CREATE INDEX` statements (after data) rather than skipping them.
    pub create_indexes: bool,
}

impl Default for ImportOptions {
    fn default() -> Self {
        Self {
            batch_rows: 500,
            on_error: OnError::Continue,
            strip_defaults: true,
            create_indexes: true,
        }
    }
}

/// The role a statement plays once classified.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum StmtKind {
    CreateSchema,
    CreateTable,
    CreateIndex,
    Insert,
    Update,
    Delete,
    Other,
}

/// A single statement the engine wants the sink to execute.
#[derive(Debug, Clone)]
pub struct ImportStatement {
    pub sql: String,
    pub kind: StmtKind,
    pub table: Option<String>,
    /// Number of data rows carried by this statement (for `INSERT` batches).
    pub rows: u64,
}

/// A sink that executes import statements. The server backs this with the
/// in-process executor; tests use a collecting sink.
pub trait ImportSink {
    /// Executes one statement, returning the number of rows it affected.
    fn execute(&mut self, stmt: &ImportStatement) -> anyhow::Result<u64>;
}

/// Why a statement was not executed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SkippedItem {
    pub reason: String,
    pub statement: String,
}

/// A non-fatal fidelity loss (dropped default, downgraded type).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LossyNote {
    pub table: String,
    pub column: String,
    pub detail: String,
}

/// A captured `setval(...)` call — recorded but not replayed (no sequences yet).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CapturedSequence {
    pub call: String,
}

/// A statement the sink rejected.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailureItem {
    pub kind: StmtKind,
    pub table: Option<String>,
    pub error: String,
    pub statement: String,
}

/// Versioned, auditable account of an import run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportReport {
    pub import_report_version: u32,
    pub schemas_created: usize,
    pub tables_created: usize,
    pub indexes_created: usize,
    pub statements_executed: usize,
    pub statements_failed: usize,
    pub rows_inserted: u64,
    pub constraints_folded: usize,
    pub stopped_early: bool,
    pub skipped: Vec<SkippedItem>,
    pub lossy: Vec<LossyNote>,
    pub captured_sequences: Vec<CapturedSequence>,
    pub failures: Vec<FailureItem>,
}

impl Default for ImportReport {
    fn default() -> Self {
        Self {
            import_report_version: IMPORT_REPORT_VERSION,
            schemas_created: 0,
            tables_created: 0,
            indexes_created: 0,
            statements_executed: 0,
            statements_failed: 0,
            rows_inserted: 0,
            constraints_folded: 0,
            stopped_early: false,
            skipped: Vec::new(),
            lossy: Vec::new(),
            captured_sequences: Vec::new(),
            failures: Vec::new(),
        }
    }
}

impl ImportReport {
    /// `true` if no statement failed and the run was not aborted.
    pub fn is_clean(&self) -> bool {
        self.statements_failed == 0 && !self.stopped_early
    }
}

/// A table buffered in pass 1 so post-data constraints can be folded into it.
struct PendingTable {
    name: String,
    stmt: Statement,
}

const DIALECT: PostgreSqlDialect = PostgreSqlDialect {};

/// Imports a complete plain-format dump, driving `sink` and returning the
/// report. The dump is scanned twice (schema, then data) over the borrowed
/// string, so only the small set of `CREATE TABLE` ASTs is buffered — table
/// data is streamed in batches.
pub fn import_str(dump: &str, opts: &ImportOptions, sink: &mut dyn ImportSink) -> ImportReport {
    let mut report = ImportReport::default();
    let units = split(dump);

    // ---- Pass 1: collect schema, fold constraints, defer indexes. ----
    let mut tables: Vec<PendingTable> = Vec::new();
    let mut schemas: Vec<String> = Vec::new();
    let mut indexes: Vec<String> = Vec::new();

    for unit in &units {
        let RawUnit::Statement(text) = unit else {
            continue;
        };
        // Unparsable statements are handled (skipped) in pass 2 to avoid
        // double counting.
        if let Some(stmt) = parse_one(text) {
            collect_schema(
                stmt,
                text,
                &mut tables,
                &mut schemas,
                &mut indexes,
                &mut report,
            );
        }
    }

    // Emit schemas, then folded tables.
    for sql in schemas {
        let stmt = ImportStatement {
            sql,
            kind: StmtKind::CreateSchema,
            table: None,
            rows: 0,
        };
        if run(sink, &stmt, &mut report) {
            report.schemas_created += 1;
        } else if opts.on_error == OnError::Stop {
            report.stopped_early = true;
            return report;
        }
    }
    for mut pending in tables {
        if opts.strip_defaults {
            strip_table_defaults(&mut pending.stmt, &pending.name, &mut report);
        }
        record_lossy_types(&pending.stmt, &pending.name, &mut report);
        let stmt = ImportStatement {
            sql: pending.stmt.to_string(),
            kind: StmtKind::CreateTable,
            table: Some(pending.name.clone()),
            rows: 0,
        };
        if run(sink, &stmt, &mut report) {
            report.tables_created += 1;
        } else if opts.on_error == OnError::Stop {
            report.stopped_early = true;
            return report;
        }
    }

    // ---- Pass 2: stream data in source order. ----
    for unit in &units {
        let stop = match unit {
            RawUnit::Statement(text) => emit_data_statement(text, sink, opts, &mut report),
            RawUnit::Copy { header, body } => {
                emit_copy_block(header, body, sink, opts, &mut report)
            }
            RawUnit::Meta(_) => false,
        };
        if stop {
            report.stopped_early = true;
            return report;
        }
    }

    // ---- Indexes last, after data is loaded. ----
    if opts.create_indexes {
        for sql in indexes {
            let stmt = ImportStatement {
                sql,
                kind: StmtKind::CreateIndex,
                table: None,
                rows: 0,
            };
            if run(sink, &stmt, &mut report) {
                report.indexes_created += 1;
            } else if opts.on_error == OnError::Stop {
                report.stopped_early = true;
                return report;
            }
        }
    }

    report
}

fn parse_one(text: &str) -> Option<Statement> {
    let mut stmts = Parser::parse_sql(&DIALECT, text).ok()?;
    if stmts.len() == 1 {
        Some(stmts.remove(0))
    } else {
        None
    }
}

/// Pass-1 routing: buffer tables, fold constraints, collect schemas/indexes.
fn collect_schema(
    stmt: Statement,
    raw: &str,
    tables: &mut Vec<PendingTable>,
    schemas: &mut Vec<String>,
    indexes: &mut Vec<String>,
    report: &mut ImportReport,
) {
    match stmt {
        Statement::CreateSchema { schema_name, .. } => {
            let name = schema_name.to_string();
            schemas.push(format!("CREATE SCHEMA IF NOT EXISTS {name}"));
        }
        Statement::CreateTable { ref name, .. } => {
            let name = name.to_string();
            tables.push(PendingTable {
                name,
                stmt: stmt.clone(),
            });
        }
        Statement::AlterTable {
            name, operations, ..
        } => {
            let target = name.to_string();
            for op in operations {
                if let AlterTableOperation::AddConstraint(constraint) = op {
                    fold_constraint(tables, &target, constraint, report);
                }
                // Non-constraint ALTERs are reported as skipped in pass 2.
            }
        }
        Statement::CreateIndex { .. } => indexes.push(raw.to_string()),
        _ => {}
    }
}

/// Pushes a constraint into the matching pending `CREATE TABLE` AST.
fn fold_constraint(
    tables: &mut [PendingTable],
    target: &str,
    constraint: TableConstraint,
    report: &mut ImportReport,
) {
    if let Some(pending) = tables.iter_mut().find(|t| t.name == target)
        && let Statement::CreateTable { constraints, .. } = &mut pending.stmt
    {
        constraints.push(constraint);
        report.constraints_folded += 1;
    }
}

/// Pass-2 routing for SQL statements: pass through DML, capture `setval`, skip
/// the rest (schema was already emitted in pass 1). Returns `true` to stop.
fn emit_data_statement(
    text: &str,
    sink: &mut dyn ImportSink,
    opts: &ImportOptions,
    report: &mut ImportReport,
) -> bool {
    let parsed = parse_one(text);
    let (kind, table) = match &parsed {
        Some(Statement::Insert { table_name, .. }) => {
            (StmtKind::Insert, Some(table_name.to_string()))
        }
        Some(Statement::Update { .. }) => (StmtKind::Update, None),
        Some(Statement::Delete { .. }) => (StmtKind::Delete, None),
        Some(Statement::Query(_)) => {
            if text.to_ascii_lowercase().contains("setval") {
                report.captured_sequences.push(CapturedSequence {
                    call: text.to_string(),
                });
            } else {
                skip(report, "non-data query", text);
            }
            return false;
        }
        // Schema statements were handled in pass 1.
        Some(Statement::CreateTable { .. })
        | Some(Statement::CreateSchema { .. })
        | Some(Statement::CreateIndex { .. }) => return false,
        Some(Statement::AlterTable { operations, .. }) => {
            if operations
                .iter()
                .all(|op| matches!(op, AlterTableOperation::AddConstraint(_)))
            {
                return false; // already folded
            }
            skip(report, "unsupported ALTER TABLE", text);
            return false;
        }
        Some(Statement::Drop { object_type, .. }) if *object_type == ObjectType::Table => {
            (StmtKind::Other, None)
        }
        Some(_) => {
            skip(report, "unsupported statement", text);
            return false;
        }
        None => {
            skip(report, classify_unparseable(text), text);
            return false;
        }
    };

    let stmt = ImportStatement {
        sql: text.to_string(),
        kind,
        table,
        rows: matches!(kind, StmtKind::Insert) as u64,
    };
    let ok = run(sink, &stmt, report);
    if matches!(kind, StmtKind::Insert) && ok {
        report.rows_inserted += 1;
    }
    !ok && opts.on_error == OnError::Stop
}

/// Decodes a `COPY` block and emits it as batched `INSERT`s. Returns `true` to
/// stop the import.
fn emit_copy_block(
    header: &str,
    body: &str,
    sink: &mut dyn ImportSink,
    opts: &ImportOptions,
    report: &mut ImportReport,
) -> bool {
    let spec = match copy_decoder::parse_copy_header(header) {
        Ok(spec) => spec,
        Err(e) => {
            skip(report, format!("unparsable COPY header: {e}"), header);
            return false;
        }
    };
    let rows = match copy_decoder::decode_rows(body, spec.format) {
        Ok(rows) => rows,
        Err(e) => {
            skip(report, format!("undecodable COPY body: {e}"), header);
            return false;
        }
    };

    let batch_rows = opts.batch_rows.max(1);
    for chunk in rows.chunks(batch_rows) {
        let sql = synthesize_insert(&spec, chunk);
        let stmt = ImportStatement {
            sql,
            kind: StmtKind::Insert,
            table: Some(spec.table.clone()),
            rows: chunk.len() as u64,
        };
        if run(sink, &stmt, report) {
            report.rows_inserted += chunk.len() as u64;
        } else if opts.on_error == OnError::Stop {
            return true;
        }
    }
    false
}

/// Builds an `INSERT INTO <table> [(cols)] VALUES (...), ...;` from decoded
/// rows. Every cell is emitted as a text literal (or `NULL`); the executor
/// coerces to the column's type, matching how it already handles `INSERT`.
/// Shared with the wire-protocol `COPY FROM STDIN` path.
pub fn synthesize_insert(spec: &CopySpec, rows: &[Vec<Cell>]) -> String {
    let mut sql = String::from("INSERT INTO ");
    sql.push_str(&spec.table);
    if !spec.columns.is_empty() {
        sql.push_str(" (");
        sql.push_str(&spec.columns.join(", "));
        sql.push(')');
    }
    sql.push_str(" VALUES ");
    for (r, row) in rows.iter().enumerate() {
        if r > 0 {
            sql.push_str(", ");
        }
        sql.push('(');
        for (c, cell) in row.iter().enumerate() {
            if c > 0 {
                sql.push_str(", ");
            }
            match cell {
                Cell::Null => sql.push_str("NULL"),
                Cell::Text(value) => {
                    sql.push('\'');
                    sql.push_str(&value.replace('\'', "''"));
                    sql.push('\'');
                }
            }
        }
        sql.push(')');
    }
    sql
}

/// Removes `DEFAULT`/identity clauses from a `CREATE TABLE`, noting each drop.
fn strip_table_defaults(stmt: &mut Statement, table: &str, report: &mut ImportReport) {
    let Statement::CreateTable { columns, .. } = stmt else {
        return;
    };
    for col in columns {
        let had = col.options.len();
        let col_name = col.name.to_string();
        col.options.retain(|opt| {
            !matches!(
                opt.option,
                ColumnOption::Default(_) | ColumnOption::Generated { .. }
            )
        });
        if col.options.len() != had {
            report.lossy.push(LossyNote {
                table: table.to_string(),
                column: col_name,
                detail: "dropped DEFAULT/identity (no sequences); explicit data values preserved"
                    .to_string(),
            });
        }
    }
}

/// Records columns whose declared type NodusDB stores losslessly only as text.
fn record_lossy_types(stmt: &Statement, table: &str, report: &mut ImportReport) {
    let Statement::CreateTable { columns, .. } = stmt else {
        return;
    };
    for col in columns {
        let ty = col.data_type.to_string().to_ascii_uppercase();
        let detail = if ty.contains("TIMESTAMP") || ty.contains("DATE") || ty.contains("TIME") {
            Some("temporal type stored as text")
        } else if ty.contains("NUMERIC") || ty.contains("DECIMAL") {
            Some("exact numeric coerced to float")
        } else if ty.contains("UUID") || ty.contains("BYTEA") || ty.contains("INET") {
            Some("type stored as text")
        } else {
            None
        };
        if let Some(detail) = detail {
            report.lossy.push(LossyNote {
                table: table.to_string(),
                column: col.name.to_string(),
                detail: detail.to_string(),
            });
        }
    }
}

/// Best-effort reason for a statement `sqlparser` could not parse.
fn classify_unparseable(text: &str) -> String {
    let upper = text.trim_start().to_ascii_uppercase();
    let reason = if upper.starts_with("CREATE SEQUENCE") || upper.starts_with("ALTER SEQUENCE") {
        "sequences unsupported"
    } else if upper.starts_with("CREATE EXTENSION") {
        "extensions unsupported"
    } else if upper.starts_with("CREATE FUNCTION")
        || upper.starts_with("CREATE OR REPLACE FUNCTION")
    {
        "functions unsupported"
    } else if upper.starts_with("COMMENT ") {
        "comments unsupported"
    } else if upper.starts_with("SET ") || upper.contains("SET_CONFIG") {
        "session setup ignored"
    } else {
        "unsupported statement"
    };
    reason.to_string()
}

fn skip(report: &mut ImportReport, reason: impl Into<String>, statement: &str) {
    report.skipped.push(SkippedItem {
        reason: reason.into(),
        statement: preview(statement),
    });
}

/// Executes a statement against the sink, updating counters. Returns `true` on
/// success.
fn run(sink: &mut dyn ImportSink, stmt: &ImportStatement, report: &mut ImportReport) -> bool {
    match sink.execute(stmt) {
        Ok(_) => {
            report.statements_executed += 1;
            true
        }
        Err(e) => {
            report.statements_failed += 1;
            report.failures.push(FailureItem {
                kind: stmt.kind,
                table: stmt.table.clone(),
                error: e.to_string(),
                statement: preview(&stmt.sql),
            });
            false
        }
    }
}

/// Truncates long statements for the report so it stays bounded.
fn preview(sql: &str) -> String {
    const MAX: usize = 200;
    let trimmed = sql.trim();
    if trimmed.len() <= MAX {
        trimmed.to_string()
    } else {
        let mut end = MAX;
        while !trimmed.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}…", &trimmed[..end])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A sink that records executed SQL and can be told to fail on a substring.
    #[derive(Default)]
    struct CollectingSink {
        executed: Vec<String>,
        fail_on: Option<String>,
    }

    impl ImportSink for CollectingSink {
        fn execute(&mut self, stmt: &ImportStatement) -> anyhow::Result<u64> {
            if let Some(needle) = &self.fail_on {
                if stmt.sql.contains(needle.as_str()) {
                    anyhow::bail!("forced failure");
                }
            }
            self.executed.push(stmt.sql.clone());
            Ok(stmt.rows)
        }
    }

    #[test]
    fn folds_post_data_constraints_into_create_table() {
        let dump = "\
CREATE TABLE public.parent (id integer NOT NULL);
CREATE TABLE public.child (id integer NOT NULL, parent_id integer);
ALTER TABLE ONLY public.parent ADD CONSTRAINT parent_pkey PRIMARY KEY (id);
ALTER TABLE ONLY public.child ADD CONSTRAINT child_parent_fk FOREIGN KEY (parent_id) REFERENCES public.parent(id);
";
        let mut sink = CollectingSink::default();
        let report = import_str(dump, &ImportOptions::default(), &mut sink);

        assert_eq!(report.tables_created, 2);
        assert_eq!(report.constraints_folded, 2);
        let parent = sink
            .executed
            .iter()
            .find(|s| s.contains("parent") && s.starts_with("CREATE TABLE"))
            .unwrap();
        assert!(parent.to_uppercase().contains("PRIMARY KEY"));
        let child = sink
            .executed
            .iter()
            .find(|s| s.contains("child") && s.starts_with("CREATE TABLE"))
            .unwrap();
        assert!(child.to_uppercase().contains("FOREIGN KEY"));
    }

    #[test]
    fn emits_tables_before_data_and_indexes_after() {
        let dump = "\
CREATE TABLE t (id integer, name text);
COPY t (id, name) FROM stdin;
1\talpha
2\tbeta
\\.
CREATE INDEX t_name_idx ON t (name);
";
        let mut sink = CollectingSink::default();
        let report = import_str(dump, &ImportOptions::default(), &mut sink);

        assert_eq!(report.tables_created, 1);
        assert_eq!(report.rows_inserted, 2);
        assert_eq!(report.indexes_created, 1);
        assert!(sink.executed[0].starts_with("CREATE TABLE"));
        assert!(sink.executed[1].starts_with("INSERT INTO t"));
        assert!(sink.executed.last().unwrap().starts_with("CREATE INDEX"));
        assert!(report.is_clean());
    }

    #[test]
    fn batches_copy_rows() {
        let dump = "\
CREATE TABLE t (id integer);
COPY t (id) FROM stdin;
1
2
3
\\.
";
        let mut sink = CollectingSink::default();
        let opts = ImportOptions {
            batch_rows: 2,
            ..ImportOptions::default()
        };
        let report = import_str(dump, &opts, &mut sink);
        assert_eq!(report.rows_inserted, 3);
        // One CREATE TABLE + two INSERT batches (2 rows, then 1 row).
        let inserts: Vec<_> = sink
            .executed
            .iter()
            .filter(|s| s.starts_with("INSERT"))
            .collect();
        assert_eq!(inserts.len(), 2);
        assert!(inserts[0].contains("(1), (2)") || inserts[0].contains("('1'), ('2')"));
    }

    #[test]
    fn strips_defaults_and_records_lossy_notes() {
        let dump = "CREATE TABLE t (id integer DEFAULT nextval('s'), created timestamp, amount numeric(10,2));";
        let mut sink = CollectingSink::default();
        let report = import_str(dump, &ImportOptions::default(), &mut sink);
        assert_eq!(report.tables_created, 1);
        assert!(!sink.executed[0].to_uppercase().contains("DEFAULT"));
        // One dropped default + temporal note + numeric note.
        assert!(report.lossy.iter().any(|l| l.detail.contains("DEFAULT")));
        assert!(report.lossy.iter().any(|l| l.detail.contains("text")));
        assert!(report.lossy.iter().any(|l| l.detail.contains("float")));
    }

    #[test]
    fn skips_unsupported_constructs_with_reasons() {
        let dump = "\
CREATE EXTENSION IF NOT EXISTS pgcrypto;
COMMENT ON TABLE t IS 'hi';
SET statement_timeout = 0;
SELECT pg_catalog.setval('s', 42, true);
\\connect mydb
";
        let mut sink = CollectingSink::default();
        let report = import_str(dump, &ImportOptions::default(), &mut sink);
        assert_eq!(report.captured_sequences.len(), 1);
        assert!(
            report
                .skipped
                .iter()
                .any(|s| s.statement.contains("EXTENSION"))
        );
        assert!(
            report
                .skipped
                .iter()
                .any(|s| s.statement.contains("COMMENT"))
        );
        assert_eq!(report.statements_executed, 0);
    }

    #[test]
    fn on_error_stop_aborts_run() {
        let dump = "\
CREATE TABLE a (id integer);
CREATE TABLE b (id integer);
";
        let mut sink = CollectingSink {
            fail_on: Some("TABLE b".to_string()),
            ..CollectingSink::default()
        };
        let opts = ImportOptions {
            on_error: OnError::Stop,
            ..ImportOptions::default()
        };
        let report = import_str(dump, &opts, &mut sink);
        assert!(report.stopped_early);
        assert_eq!(report.statements_failed, 1);
        assert_eq!(report.tables_created, 1);
    }
}
