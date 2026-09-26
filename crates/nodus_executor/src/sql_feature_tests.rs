//! Set-returning functions in select lists, RETURNING expressions, JSON layouts
//! and whole-row references, `COMMENT ON`, column renames, grants to `PUBLIC`,
//! and relation sizes, driven through SQL.

use super::*;
use crate::dml_join_tests::{rows, session};
use nodus_audit::MemoryAuditSink;

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
fn comments_describe_relations_and_columns() {
    let sql = session();
    sql("CREATE TABLE t (id INT PRIMARY KEY, n INT)").unwrap();
    sql("CREATE VIEW v AS SELECT 1 AS x").unwrap();
    assert_eq!(sql("COMMENT ON TABLE t IS 'rows'").unwrap().tag, "COMMENT");
    sql("COMMENT ON COLUMN t.n IS 'a number'").unwrap();
    sql("COMMENT ON VIEW v IS 'a view'").unwrap();
    let out = sql("SELECT obj_description('t'::regclass, 'pg_class'), \
         col_description('t'::regclass, 2), obj_description('v'::regclass, 'pg_class')")
    .unwrap();
    assert_eq!(rows(&out), ["rows|a number|a view"]);
    let out = sql("SELECT objsubid, description FROM pg_description").unwrap();
    assert_eq!(rows(&out), ["0|a view", "0|rows", "2|a number"]);
    sql("COMMENT ON TABLE t IS NULL").unwrap();
    sql("COMMENT ON COLUMN t.n IS ''").unwrap();
    let out = sql("SELECT obj_description('t'::regclass, 'pg_class'), \
         col_description('t'::regclass, 2)")
    .unwrap();
    assert_eq!(rows(&out), ["|"]);
    for (statement, error) in [
        ("COMMENT ON TABLE v IS 'x'", "\"v\" is not a table"),
        ("COMMENT ON VIEW t IS 'x'", "\"t\" is not a view"),
        (
            "COMMENT ON COLUMN t.nope IS 'x'",
            "column \"nope\" of relation \"t\" does not exist",
        ),
        (
            "COMMENT ON TABLE nope IS 'x'",
            "relation \"nope\" does not exist",
        ),
        (
            "COMMENT ON COLUMN n IS 'x'",
            "column name must be qualified",
        ),
    ] {
        assert_eq!(
            sql(statement).unwrap_err().to_string(),
            error,
            "{statement}"
        );
    }
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

#[test]
fn grants_to_public_reach_every_principal() {
    let (exec, cat) = MemExecutor::shared(Arc::new(MemoryAuditSink::new()));
    let principal = |name: &str| {
        cat.create_role(nodus_catalog::CreateRoleRequest {
            id: nodus_catalog::PrincipalId::new(),
            name: name.into(),
            principal_type: nodus_catalog::PrincipalType::User,
            database_id: None,
        })
        .unwrap()
    };
    let admin = principal("admin");
    cat.grant_privilege(nodus_catalog::GrantPrivilegeRequest {
        id: nodus_catalog::GrantId::new(),
        principal_id: admin.id,
        resource: nodus_catalog::ResourceRef::System,
        privilege: "ALL".into(),
    })
    .unwrap();
    let user = principal("alice");
    let run = |principal_id, statement: &str| {
        let ctx = ExecutionContext {
            session_id: format!("{principal_id:?}"),
            principal_id,
            active_roles: vec![],
            authz_catalog_version: 1,
        };
        let mut statements = nodus_sql::parse_sql(statement)?;
        exec.execute_logical(&ctx, plan_statement(&statements.remove(0), &[])?)
    };
    run(admin.id, "CREATE TABLE t (id INT PRIMARY KEY)").unwrap();
    assert!(run(user.id, "SELECT * FROM t").is_err());
    run(admin.id, "GRANT SELECT ON t TO PUBLIC").unwrap();
    assert_eq!(
        run(user.id, "SELECT count(*) FROM t").unwrap().rows.len(),
        1
    );
    assert!(run(user.id, "INSERT INTO t VALUES (1)").is_err());
    run(admin.id, "REVOKE SELECT ON t FROM PUBLIC").unwrap();
    assert!(run(user.id, "SELECT * FROM t").is_err());
    // Revoking what PUBLIC never had is no error.
    run(admin.id, "REVOKE UPDATE ON t FROM PUBLIC").unwrap();
    let err = run(admin.id, "CREATE ROLE public").unwrap_err();
    assert_eq!(err.to_string(), "role name \"public\" is reserved");
}

#[test]
fn relation_sizes_count_stored_pages() {
    let sql = session();
    sql("CREATE TABLE t (id INT PRIMARY KEY, v TEXT)").unwrap();
    sql("CREATE INDEX t_v ON t (v)").unwrap();
    let sizes = "SELECT pg_relation_size('t'), pg_indexes_size('t'), \
                 pg_total_relation_size('t'), pg_relation_size('t_v')";
    assert_eq!(rows(&sql(sizes).unwrap()), ["0|0|0|0"]);
    sql("INSERT INTO t SELECT g, repeat('x', 100) FROM generate_series(1, 200) g").unwrap();
    let out = sql(sizes).unwrap();
    let sizes: Vec<i64> = out.rows[0]
        .values
        .iter()
        .map(|v| crate::render(v).parse().unwrap())
        .collect();
    assert!(sizes[0] > 0 && sizes[0] % 8192 == 0, "{sizes:?}");
    // The indexes are the primary key's and `t_v`.
    assert!(sizes[3] > 0 && sizes[1] > sizes[3], "{sizes:?}");
    assert_eq!(sizes[2], sizes[0] + sizes[1]);
    assert_eq!(value(&sql("SELECT pg_relation_size(0)").unwrap()), "");
    let err = sql("SELECT pg_table_size('nope')").unwrap_err();
    assert_eq!(err.to_string(), "relation \"nope\" does not exist");
}
