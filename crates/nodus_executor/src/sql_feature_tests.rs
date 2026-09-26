//! Set-returning functions in select lists and RETURNING expressions, driven
//! through SQL.

use crate::dml_join_tests::{rows, session};

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
