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
fn object_identifiers_read_and_print_as_names() {
    let sql = session();
    sql("CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    let out = sql("SELECT 't'::regclass, 't_pkey'::regclass::text, 'int4'::regtype").unwrap();
    assert_eq!(rows(&out), ["t|t_pkey|integer"]);
    let out = sql("SELECT relname FROM pg_catalog.pg_class WHERE oid = 't'::regclass").unwrap();
    assert_eq!(rows(&out), ["t"]);
    let err = sql("SELECT 'nosuch'::regclass").unwrap_err();
    assert_eq!(err.to_string(), "relation \"nosuch\" does not exist");
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

#[test]
fn subscripts_and_select_list_set_functions() {
    let sql = session();
    let out = sql("SELECT (ARRAY[1,2,3])[2], (ARRAY[1,2,3])[2:3], (ARRAY[1,2])[9]").unwrap();
    assert_eq!(rows(&out), ["2|{2,3}|"]);
    let out = sql("SELECT generate_series(1, 3) AS n").unwrap();
    assert_eq!(out.columns, ["n"]);
    assert_eq!(rows(&out), ["1", "2", "3"]);
}

#[test]
fn integer_arithmetic_overflows_in_its_type() {
    let sql = session();
    sql("CREATE TABLE t (id INT PRIMARY KEY, i INT, s SMALLINT)").unwrap();
    sql("INSERT INTO t VALUES (1, 2147483647, 32767)").unwrap();
    let err = sql("SELECT i + 1 FROM t").unwrap_err();
    assert_eq!(err.to_string(), "integer out of range");
    let err = sql("SELECT s + s FROM t").unwrap_err();
    assert_eq!(err.to_string(), "smallint out of range");
    let err = sql("SELECT 2147483647 + 1").unwrap_err();
    assert_eq!(err.to_string(), "integer out of range");
    // A bigint operand widens the arithmetic.
    assert_eq!(
        rows(&sql("SELECT i + 1::bigint, s + 1 FROM t").unwrap()),
        ["2147483648|32768"]
    );
    assert_eq!(
        rows(&sql("SELECT pg_typeof(s + s) FROM t").unwrap()),
        ["smallint"]
    );
}

#[test]
fn using_joins_merge_their_columns() {
    let sql = session();
    sql("CREATE TABLE a (id INT, x TEXT)").unwrap();
    sql("CREATE TABLE b (id INT, y TEXT)").unwrap();
    sql("INSERT INTO a VALUES (1, 'p'), (2, 'q')").unwrap();
    sql("INSERT INTO b VALUES (1, 'r'), (3, 's')").unwrap();
    let out = sql("SELECT * FROM a FULL JOIN b USING (id)").unwrap();
    assert_eq!(out.columns, ["id", "x", "y"]);
    assert_eq!(rows(&out), ["1|p|r", "2|q|", "3||s"]);
    let out = sql("SELECT b.*, 0 AS z FROM a JOIN b ON a.id = b.id").unwrap();
    assert_eq!(out.columns, ["id", "y", "z"]);
    let out = sql("SELECT a.x, b.y FROM a, b WHERE a.id = b.id").unwrap();
    assert_eq!(rows(&out), ["p|r"]);
    let err = sql("SELECT id FROM a JOIN b ON a.id = b.id").unwrap_err();
    assert_eq!(err.to_string(), "column reference \"id\" is ambiguous");
}

#[test]
fn materialized_views_keep_rows_until_refreshed() {
    let sql = session();
    sql("CREATE TABLE src (a INT)").unwrap();
    sql("INSERT INTO src VALUES (1), (2)").unwrap();
    assert_eq!(
        sql("CREATE MATERIALIZED VIEW mv AS SELECT a FROM src")
            .unwrap()
            .tag,
        "SELECT 2"
    );
    sql("INSERT INTO src VALUES (3)").unwrap();
    assert_eq!(rows(&sql("SELECT count(*) FROM mv").unwrap()), ["2"]);
    assert_eq!(
        sql("REFRESH MATERIALIZED VIEW mv").unwrap().tag,
        "REFRESH MATERIALIZED VIEW"
    );
    assert_eq!(rows(&sql("SELECT count(*) FROM mv").unwrap()), ["3"]);
    let err = sql("DELETE FROM mv").unwrap_err();
    assert_eq!(err.to_string(), "cannot change materialized view \"mv\"");
    sql("CREATE TABLE empty AS SELECT a FROM src WITH NO DATA").unwrap();
    assert_eq!(rows(&sql("SELECT count(*) FROM empty").unwrap()), ["0"]);
}

#[test]
fn jsonb_numbers_keep_their_scale() {
    let sql = session();
    let out = sql(r#"SELECT '[1.50, 1e3, -0.0]'::jsonb, to_jsonb(2.50), '{"a": 1.0}'::jsonb = '{"a": 1}'::jsonb"#)
        .unwrap();
    assert_eq!(rows(&out), ["[1.50, 1000, 0.0]|2.50|t"]);
}
