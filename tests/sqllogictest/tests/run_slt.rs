//! Executes every `cases/*.slt` file against a fresh in-memory NodusDB
//! [`TestServer`] over the PostgreSQL wire protocol.
//!
//! Each case runs against its own server (config default = ephemeral in-memory
//! storage), giving per-file isolation without a shared data dir. Query results
//! are read via the simple query protocol, so every value is rendered as text by
//! NodusDB's own pgwire encoder — exactly the surface `pg_regress`/`psql`
//! compare, which is what a compatibility suite wants to pin.

use nodus_sqllogictest::{Expect, Record, case_paths, compare, parse};
use nodus_testkit::TestServer;
use std::net::SocketAddr;
use std::path::Path;
use std::time::Duration;
use tokio_postgres::{Client, NoTls, SimpleQueryMessage};

async fn connect(addr: &SocketAddr) -> Client {
    let conn_str = format!(
        "host={} port={} user=nodus password=nodus dbname=default",
        addr.ip(),
        addr.port()
    );
    for _ in 0..30 {
        if let Ok((client, connection)) = tokio_postgres::connect(&conn_str, NoTls).await {
            tokio::spawn(async move {
                let _ = connection.await;
            });
            return client;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("failed to connect to pgwire at {addr}");
}

/// Renders a simple-query result set as one TAB-joined line per row, with SQL
/// NULL shown as `NULL`.
fn render_rows(messages: &[SimpleQueryMessage]) -> Vec<String> {
    let mut rows = Vec::new();
    for message in messages {
        if let SimpleQueryMessage::Row(row) = message {
            let mut cols = Vec::with_capacity(row.len());
            for i in 0..row.len() {
                cols.push(row.get(i).unwrap_or("NULL").to_string());
            }
            rows.push(cols.join("\t"));
        }
    }
    rows
}

async fn run_record(client: &Client, record: &Record) -> Result<(), String> {
    match &record.expect {
        Expect::StatementOk => client
            .simple_query(&record.sql)
            .await
            .map(|_| ())
            .map_err(|e| format!("statement failed: {e}\n  SQL: {}", record.sql)),
        Expect::StatementError { contains } => match client.simple_query(&record.sql).await {
            Ok(_) => Err(format!(
                "expected an error but statement succeeded\n  SQL: {}",
                record.sql
            )),
            Err(e) => match contains {
                Some(sub) if !e.to_string().contains(sub) => Err(format!(
                    "error message mismatch: got '{e}', expected substring '{sub}'\n  SQL: {}",
                    record.sql
                )),
                _ => Ok(()),
            },
        },
        Expect::Query { sort, rows } => {
            let messages = client
                .simple_query(&record.sql)
                .await
                .map_err(|e| format!("query failed: {e}\n  SQL: {}", record.sql))?;
            let actual = render_rows(&messages);
            compare(*sort, rows, &actual).map_err(|diff| format!("{diff}\n  SQL: {}", record.sql))
        }
    }
}

async fn run_file(path: &Path) -> Result<(), String> {
    let body = std::fs::read_to_string(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    let records = parse(&body).map_err(|e| format!("{}: parse error: {e}", path.display()))?;

    let server = TestServer::start()
        .await
        .map_err(|e| format!("{}: server start failed: {e}", path.display()))?;
    let client = connect(&server.pgwire_addr).await;

    // Collect every failing record (rather than stopping at the first) so a
    // single run reports the full picture; later records still execute, which is
    // what we want while calibrating and growing the suite.
    let file = path.file_name().unwrap_or_default().to_string_lossy();
    let mut errors = Vec::new();
    for record in &records {
        if let Err(e) = run_record(&client, record).await {
            errors.push(format!("{file}:{}: {e}", record.line));
        }
    }

    drop(client);
    server.shutdown().await;
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("\n\n"))
    }
}

/// Investigative probe (not a regression gate): prints how a handful of values
/// come back through the extended (native typed) vs simple (text) protocols, to
/// attribute the calibration mismatches. Run with:
///   cargo test -p nodus_sqllogictest --test run_slt probe_protocols -- --nocapture --include-ignored
#[ignore = "diagnostic probe, run explicitly"]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn probe_protocols() {
    let server = TestServer::start().await.unwrap();
    let client = connect(&server.pgwire_addr).await;

    // For each SQL returning one column: report the extended-protocol column
    // TYPE and the simple-protocol text VALUE(S), to characterize the evaluator.
    async fn describe(client: &Client, sql: &str) {
        let ty = match client.query_opt(sql, &[]).await {
            Ok(Some(r)) => r.columns()[0].type_().to_string(),
            Ok(None) => "<no row>".to_string(),
            Err(e) => format!("ERR({e})"),
        };
        let val = match client.simple_query(sql).await {
            Ok(m) => {
                let rows = render_rows(&m);
                if rows.is_empty() { "<no rows>".to_string() } else { rows.join(" ; ") }
            }
            Err(e) => format!("ERR({e})"),
        };
        println!("PROBE | type={ty:>10} | val={val:<24} | {sql}");
    }

    // FROM-less scalar expressions.
    for sql in [
        "SELECT 1",
        "SELECT 1.5",
        "SELECT 1.5::float8",
        "SELECT 3.14::numeric",
        "SELECT 1 = 1",
        "SELECT NULL::int",
        "SELECT 2 + 3",
        "SELECT 10 / 3",
        "SELECT 'a' || 'b'",
        "SELECT upper('x')",
        "SELECT length('abc')",
        "SELECT abs(-4)",
    ] {
        describe(&client, sql).await;
    }

    // Expressions over a table (projection) and in WHERE.
    let _ = client.simple_query("CREATE TABLE p (a INT, b INT)").await;
    let _ = client
        .simple_query("INSERT INTO p VALUES (1, 2), (3, 3)")
        .await;
    for sql in [
        "SELECT a + b FROM p ORDER BY a",
        "SELECT a = b FROM p ORDER BY a",
        "SELECT a::float8 FROM p ORDER BY a",
        "SELECT a FROM p WHERE a = 3",
        "SELECT a FROM p WHERE a > 1 ORDER BY a",
        "SELECT sum(a), avg(a), min(a), max(a) FROM p",
    ] {
        describe(&client, sql).await;
    }

    server.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn run_all_cases() {
    let dir = format!("{}/cases", env!("CARGO_MANIFEST_DIR"));
    let paths = case_paths(&dir).expect("read cases directory");
    assert!(!paths.is_empty(), "no .slt cases found in {dir}");

    let mut failures = Vec::new();
    for path in &paths {
        if let Err(e) = run_file(path).await {
            failures.push(e);
        }
    }

    if !failures.is_empty() {
        panic!(
            "\n{} sqllogictest case file(s) failed:\n\n{}\n",
            failures.len(),
            failures.join("\n\n")
        );
    }
}
