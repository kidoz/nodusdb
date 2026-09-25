//! EXPLAIN, TRUNCATE, object-identifier casts, `jsonb` output, and the
//! errors for unsupported statements, driven through SQL.

use crate::dml_join_tests::{rows, session};

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
}
