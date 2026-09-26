//! Set-returning functions in select lists, driven through SQL.

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
