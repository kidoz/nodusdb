//! EXPLAIN, TRUNCATE, object-identifier casts, `jsonb` output, and the
//! errors for unsupported statements, driven through SQL.

use crate::dml_join_tests::{rows, session};

#[test]
fn explain_describes_scans_in_text_and_json() {
    let sql = session();
    sql("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").unwrap();
    let out = sql("EXPLAIN (COSTS OFF) SELECT * FROM t WHERE id = 1").unwrap();
    assert_eq!(out.columns, ["QUERY PLAN"]);
    let lines: Vec<String> = out
        .rows
        .iter()
        .map(|r| crate::render(&r.values[0]))
        .collect();
    assert_eq!(
        lines,
        ["Index Scan using t_pkey on t", "  Index Cond: (id = 1)"]
    );

    let out = sql("EXPLAIN (FORMAT JSON) SELECT v FROM t WHERE v = 'x' ORDER BY v").unwrap();
    let plan: serde_json::Value =
        serde_json::from_str(&crate::render(&out.rows[0].values[0])).unwrap();
    let sort = &plan[0]["Plan"];
    assert_eq!(sort["Node Type"], "Sort");
    assert_eq!(sort["Sort Key"], serde_json::json!(["v"]));
    let scan = &sort["Plans"][0];
    assert_eq!(scan["Node Type"], "Seq Scan");
    assert_eq!(scan["Relation Name"], "t");
    assert_eq!(scan["Filter"], "(v = 'x'::text)");
    assert!(scan["Total Cost"].is_number());
}

#[test]
fn explain_analyze_runs_the_statement() {
    let sql = session();
    sql("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    let out =
        sql("EXPLAIN (ANALYZE, COSTS OFF, TIMING OFF) INSERT INTO t VALUES (1), (2)").unwrap();
    let first = crate::render(&out.rows[0].values[0]);
    assert_eq!(first, "Insert on t (actual rows=0.00 loops=1)");
    assert!(crate::render(&out.rows.last().unwrap().values[0]).starts_with("Execution Time:"));
    assert_eq!(rows(&sql("SELECT count(*) FROM t").unwrap()), ["2"]);
}

#[test]
fn truncate_empties_tables_and_restarts_identity() {
    let sql = session();
    sql("CREATE TABLE t (id SERIAL PRIMARY KEY, v TEXT)").unwrap();
    sql("INSERT INTO t (v) VALUES ('a'), ('b')").unwrap();
    assert_eq!(sql("TRUNCATE t").unwrap().tag, "TRUNCATE TABLE");
    sql("INSERT INTO t (v) VALUES ('c')").unwrap();
    assert_eq!(rows(&sql("SELECT id FROM t").unwrap()), ["3"]);
    sql("TRUNCATE TABLE t RESTART IDENTITY").unwrap();
    sql("INSERT INTO t (v) VALUES ('d')").unwrap();
    assert_eq!(rows(&sql("SELECT id, v FROM t").unwrap()), ["1|d"]);
}

#[test]
fn jsonb_is_stored_and_printed_normalized() {
    let sql = session();
    sql("CREATE TABLE t (id INT PRIMARY KEY, d JSONB, j JSON)").unwrap();
    sql(r#"INSERT INTO t VALUES (1, '{"b": 1, "a": {"y": 2, "b": 3}, "b": 4}', '{"b": 1,  "a": 2}')"#)
        .unwrap();
    let out = sql("SELECT d, j FROM t").unwrap();
    assert_eq!(
        rows(&out),
        [r#"{"a": {"b": 3, "y": 2}, "b": 4}|{"b": 1,  "a": 2}"#]
    );
    let out = sql(r#"UPDATE t SET d = (d || '{"c": true}') - 'b' RETURNING d"#).unwrap();
    assert_eq!(rows(&out), [r#"{"a": {"b": 3, "y": 2}, "c": true}"#]);
    let err = sql("INSERT INTO t VALUES (2, 'not json', '{}')").unwrap_err();
    assert_eq!(err.to_string(), "invalid input syntax for type json");
}

#[test]
fn unsupported_statements_are_named_briefly() {
    let sql = session();
    let err = sql("CREATE FUNCTION f() RETURNS int AS 'select 1' LANGUAGE sql").unwrap_err();
    assert_eq!(err.to_string(), "CREATE FUNCTION is not supported");
    let err = sql("EXPLAIN CREATE TABLE x (a int)").unwrap_err();
    assert_eq!(err.to_string(), "EXPLAIN of CREATE TABLE is not supported");
}
