//! Set-returning functions in select lists, RETURNING expressions, JSON layouts
//! and whole-row references, and column renames, driven through SQL.

use super::*;
use crate::dml_join_tests::{rows, session};

/// The first value of the first row of `out`, rendered.
fn value(out: &QueryOutput) -> String {
    crate::render(&out.rows[0].values[0])
}

#[test]
fn select_list_set_returning_functions_repeat_their_rows() {
    let sql = session();
    sql("CREATE TABLE t (id INT PRIMARY KEY, tags TEXT[])").unwrap();
    sql("INSERT INTO t VALUES (1, '{a,b}'), (2, '{}'), (3, NULL)").unwrap();
    let out = sql("SELECT id, unnest(tags) AS tag FROM t").unwrap();
    assert_eq!(out.columns, ["id", "tag"]);
    assert_eq!(rows(&out), ["1|a", "1|b"]);
    // Several run in lockstep; the shorter pads with NULL.
    let out = sql("SELECT id, unnest(tags), generate_series(1, 3) FROM t WHERE id = 1").unwrap();
    assert_eq!(rows(&out), ["1|a|1", "1|b|2", "1||3"]);
    let err = sql("SELECT count(*), unnest(tags) FROM t").unwrap_err();
    assert!(err.to_string().contains("set-returning functions"), "{err}");
}

#[test]
fn returning_computes_over_old_and_new_rows() {
    let sql = session();
    sql("CREATE TABLE t (id INT PRIMARY KEY, n INT)").unwrap();
    let out = sql("INSERT INTO t VALUES (1, 10) RETURNING id * 2 AS twice, old.n, new.n").unwrap();
    assert_eq!(out.columns, ["twice", "n", "n"]);
    assert_eq!(rows(&out), ["2||10"]);
    let out = sql("UPDATE t SET n = n + 5 RETURNING n - old.n AS delta").unwrap();
    assert_eq!(rows(&out), ["5"]);
    let out = sql("DELETE FROM t RETURNING n, new.n IS NULL AS gone").unwrap();
    assert_eq!(rows(&out), ["15|t"]);
    let out = sql("MERGE INTO t USING (SELECT 7 AS id) s ON t.id = s.id \
         WHEN NOT MATCHED THEN INSERT VALUES (s.id, 1) RETURNING merge_action(), t.id")
    .unwrap();
    assert_eq!(rows(&out), ["INSERT|7"]);
    let err = sql("INSERT INTO t VALUES (2, 1) RETURNING count(*)").unwrap_err();
    assert_eq!(
        err.to_string(),
        "aggregate functions are not allowed in RETURNING"
    );
    let err = sql("SELECT merge_action()").unwrap_err();
    assert_eq!(
        err.to_string(),
        "MERGE_ACTION() can only be used in the RETURNING list of a MERGE command"
    );
}

#[test]
fn json_functions_write_postgresql_layouts() {
    let sql = session();
    let out = sql(
        "SELECT json_build_object('a', 1, 'b', ARRAY[1,2]), json_build_array(1, 'x'), \
         to_json(ARRAY['a','b']), row_to_json(row(1, 'x'))",
    )
    .unwrap();
    assert_eq!(
        rows(&out),
        [r#"{"a" : 1, "b" : [1,2]}|[1, "x"]|["a","b"]|{"f1":1,"f2":"x"}"#]
    );
    assert_eq!(out.types[0], "JSON");
    let out = sql("SELECT json_object_agg(k, v) FROM (VALUES ('a', 1), ('b', 2)) t(k, v)").unwrap();
    assert_eq!(value(&out), r#"{ "a" : 1, "b" : 2 }"#);
    // Rows and arrays each start a line after the first.
    let out = sql("SELECT json_agg(t) FROM (VALUES (1, 'a'), (2, 'b')) t(x, y)").unwrap();
    assert_eq!(
        value(&out),
        "[{\"x\":1,\"y\":\"a\"}, \n {\"x\":2,\"y\":\"b\"}]"
    );
    let out = sql("SELECT json_agg(x) FROM (VALUES (ARRAY[1]), (NULL), (ARRAY[2])) t(x)").unwrap();
    assert_eq!(value(&out), "[[1], null, \n [2]]");
}

#[test]
fn json_values_keep_their_text() {
    let sql = session();
    sql("CREATE TABLE t (id INT PRIMARY KEY, j JSON)").unwrap();
    sql(r#"INSERT INTO t VALUES (1, '{"b": 1,  "a": [1, 2]}')"#).unwrap();
    let out = sql("SELECT j, j -> 'a', j ->> 'b', json_build_object('j', j), to_jsonb(j) FROM t")
        .unwrap();
    assert_eq!(
        rows(&out),
        [r#"{"b": 1,  "a": [1, 2]}|[1, 2]|1|{"j" : {"b": 1,  "a": [1, 2]}}|{"a": [1, 2], "b": 1}"#]
    );
    let out = sql("SELECT row_to_json(t), pg_typeof(j) FROM t").unwrap();
    assert_eq!(rows(&out), [r#"{"id":1,"j":{"b": 1,  "a": [1, 2]}}|json"#]);
    // Text columns take a json value as its text.
    sql("CREATE TABLE s (v TEXT)").unwrap();
    sql("INSERT INTO s SELECT j FROM t").unwrap();
    assert_eq!(
        rows(&sql("SELECT v, pg_typeof(v) FROM s").unwrap()),
        [r#"{"b": 1,  "a": [1, 2]}|text"#]
    );
}

#[test]
fn a_relation_name_stands_for_its_row() {
    let sql = session();
    let out = sql("SELECT t, to_json(t) FROM (SELECT 1 AS a, 'x y' AS b, NULL AS c) t").unwrap();
    assert_eq!(rows(&out), [r#"(1,"x y",)|{"a":1,"b":"x y","c":null}"#]);
    // A function's row is its value.
    let out = sql("SELECT x FROM jsonb_array_elements_text('[\"x\", 1]') x").unwrap();
    assert_eq!(rows(&out), ["1", "x"]);
    let out = sql("SELECT * FROM json_array_elements('[1, {\"a\" :  2}]')").unwrap();
    assert_eq!(out.columns, ["value"]);
    assert_eq!(rows(&out), ["1", "{\"a\" :  2}"]);
}

#[test]
fn renaming_a_missing_column_keeps_the_table() {
    let sql = session();
    sql("CREATE TABLE t (id INT PRIMARY KEY, n INT)").unwrap();
    sql("INSERT INTO t VALUES (1, 2)").unwrap();
    let err = sql("ALTER TABLE t RENAME COLUMN nope TO m").unwrap_err();
    assert_eq!(err.to_string(), "column \"nope\" does not exist");
    assert_eq!(rows(&sql("SELECT n FROM t").unwrap()), ["2"]);
}
