//! Enums, domains, indexes on expressions, updatable views, and cursors,
//! driven through SQL.

use crate::constraint_tests::{field, fields, session};
use crate::dml_join_tests::rows;

/// A result's rows in their order, each as its values joined by `|`.
fn ordered(out: &crate::QueryOutput) -> Vec<String> {
    out.rows
        .iter()
        .map(|r| {
            r.values
                .iter()
                .map(crate::render)
                .collect::<Vec<_>>()
                .join("|")
        })
        .collect()
}

#[test]
fn enums_order_by_their_labels_and_refuse_others() {
    let (sql, notices) = session();
    sql("CREATE TYPE mood AS ENUM ('sad', 'ok', 'happy')").unwrap();
    sql("CREATE TABLE p (name text, m mood)").unwrap();
    sql("INSERT INTO p VALUES ('a', 'happy'), ('b', 'sad'), ('c', 'ok')").unwrap();
    let (message, f) = fields(sql("INSERT INTO p VALUES ('d', 'angry')").unwrap_err());
    assert_eq!(message, "invalid input value for enum mood: \"angry\"");
    assert_eq!(field(&f, "code").as_deref(), Some("22P02"));
    assert_eq!(
        ordered(&sql("SELECT name FROM p ORDER BY m").unwrap()),
        ["b", "c", "a"]
    );
    assert_eq!(
        rows(&sql("SELECT name FROM p WHERE m > 'ok'").unwrap()),
        ["a"]
    );
    let out = sql("SELECT min(m), max(m), pg_typeof(min(m)) FROM p").unwrap();
    assert_eq!(out.rows[0].values[0], crate::Value::Text("sad".into()));
    assert_eq!(out.rows[0].values[1], crate::Value::Text("happy".into()));

    sql("ALTER TYPE mood ADD VALUE 'meh' BEFORE 'ok'").unwrap();
    sql("ALTER TYPE mood ADD VALUE IF NOT EXISTS 'meh'").unwrap();
    assert_eq!(notices(), ["enum label \"meh\" already exists, skipping"]);
    let out = sql("SELECT enum_range(NULL::mood)::text").unwrap();
    assert_eq!(rows(&out), ["{sad,meh,ok,happy}"]);
    sql("ALTER TYPE mood RENAME VALUE 'ok' TO 'fine'").unwrap();
    assert_eq!(
        rows(&sql("SELECT m FROM p WHERE name = 'c'").unwrap()),
        ["fine"]
    );
    // A type is no relation.
    assert!(sql("SELECT * FROM mood").is_err());
    let (message, f) = fields(sql("DROP TYPE mood").unwrap_err());
    assert_eq!(
        message,
        "cannot drop type mood because other objects depend on it"
    );
    assert_eq!(
        field(&f, "detail").as_deref(),
        Some("column m of table p depends on type mood")
    );
    sql("DROP TYPE mood CASCADE").unwrap();
    let out = sql("SELECT * FROM p ORDER BY name").unwrap();
    assert_eq!(out.columns, ["name"]);
}

#[test]
fn domains_check_stored_and_cast_values() {
    let (sql, _) = session();
    sql("CREATE DOMAIN pos AS int CHECK (VALUE > 0)").unwrap();
    sql("CREATE DOMAIN email AS text NOT NULL CHECK (VALUE LIKE '%@%')").unwrap();
    sql("CREATE DOMAIN code AS varchar(3) DEFAULT 'abc'").unwrap();
    sql("CREATE TABLE acct (id pos PRIMARY KEY, mail email, c code)").unwrap();
    let out = sql("INSERT INTO acct (id, mail) VALUES (1, 'a@b') RETURNING c").unwrap();
    assert_eq!(rows(&out), ["abc"]);
    let (message, f) = fields(sql("INSERT INTO acct VALUES (0, 'a@b')").unwrap_err());
    assert_eq!(
        message,
        "value for domain pos violates check constraint \"pos_check\""
    );
    assert_eq!(field(&f, "code").as_deref(), Some("23514"));
    assert_eq!(field(&f, "datatype").as_deref(), Some("pos"));
    let (message, _) = fields(sql("INSERT INTO acct VALUES (2, NULL)").unwrap_err());
    assert_eq!(message, "domain email does not allow null values");
    assert!(sql("SELECT (-1)::pos").is_err());
    // Operators take the base type's values.
    let out = sql("SELECT id + 1, pg_typeof(id), pg_typeof(id + 1) FROM acct").unwrap();
    assert_eq!(out.rows[0].values[0], crate::Value::Int(2));
    assert_eq!(out.rows[0].values[1], crate::Value::Text("pos".into()));
    assert_eq!(out.rows[0].values[2], crate::Value::Text("integer".into()));

    sql("ALTER DOMAIN pos ADD CONSTRAINT pos_small CHECK (VALUE < 100)").unwrap();
    assert!(sql("INSERT INTO acct VALUES (100, 'x@y')").is_err());
    sql("ALTER DOMAIN pos DROP CONSTRAINT pos_small").unwrap();
    sql("INSERT INTO acct VALUES (100, 'x@y')").unwrap();
    let out =
        sql("SELECT pg_get_constraintdef(oid) FROM pg_constraint WHERE contypid = 'pos'::regtype")
            .unwrap();
    assert_eq!(rows(&out), ["CHECK ((VALUE > 0))"]);
    assert!(sql("DROP DOMAIN pos").is_err());
}

#[test]
fn indexes_on_expressions_keep_their_keys_unique() {
    let (sql, _) = session();
    sql("CREATE TABLE u (id int PRIMARY KEY, email text)").unwrap();
    sql("CREATE UNIQUE INDEX u_email ON u (lower(email))").unwrap();
    sql("INSERT INTO u VALUES (1, 'A@x.com'), (2, NULL), (3, NULL)").unwrap();
    let (message, f) = fields(sql("INSERT INTO u VALUES (4, 'a@X.COM')").unwrap_err());
    assert_eq!(
        message,
        "duplicate key value violates unique constraint \"u_email\""
    );
    assert_eq!(
        field(&f, "detail").as_deref(),
        Some("Key (lower(email))=(a@x.com) already exists.")
    );
    // The key follows an update, and frees the old one.
    sql("UPDATE u SET email = 'b@x.com' WHERE id = 1").unwrap();
    sql("INSERT INTO u VALUES (4, 'A@x.com')").unwrap();
    let out = sql(
        "INSERT INTO u VALUES (5, 'B@X.com') ON CONFLICT (lower(email)) DO UPDATE SET email = excluded.email RETURNING id, email",
    )
    .unwrap();
    assert_eq!(out.rows[0].values[0], crate::Value::Int(1));
    let out = sql("SELECT pg_get_indexdef('u_email'::regclass)").unwrap();
    assert_eq!(
        rows(&out),
        ["CREATE UNIQUE INDEX u_email ON public.u USING btree (lower(email))"]
    );
}

#[test]
fn simple_views_write_their_table_within_their_condition() {
    let (sql, _) = session();
    sql("CREATE TABLE b (id int PRIMARY KEY, name text, score int DEFAULT 0)").unwrap();
    sql("CREATE VIEW v AS SELECT id, name AS label FROM b WHERE score >= 0 WITH CHECK OPTION")
        .unwrap();
    let out = sql("INSERT INTO v VALUES (1, 'a') RETURNING *").unwrap();
    assert_eq!(out.columns, ["id", "label"]);
    sql("UPDATE v SET label = 'z' WHERE id = 1").unwrap();
    assert_eq!(rows(&sql("SELECT name FROM b").unwrap()), ["z"]);
    sql("UPDATE b SET score = -1").unwrap();
    // Rows the view does not show are not its to change.
    assert_eq!(sql("DELETE FROM v").unwrap().tag, "DELETE 0");
    sql("CREATE VIEW w AS SELECT id, score FROM b WHERE score < 10 WITH CHECK OPTION").unwrap();
    let (message, f) = fields(sql("INSERT INTO w VALUES (2, 50)").unwrap_err());
    assert_eq!(message, "new row violates check option for view \"w\"");
    assert_eq!(
        field(&f, "detail").as_deref(),
        Some("Failing row contains (2, null, 50).")
    );
    sql("CREATE VIEW n AS SELECT count(*) AS n FROM b").unwrap();
    let (message, f) = fields(sql("INSERT INTO n VALUES (1)").unwrap_err());
    assert_eq!(message, "cannot insert into view \"n\"");
    assert_eq!(
        field(&f, "detail").as_deref(),
        Some("Views that return aggregate functions are not automatically updatable.")
    );
}

#[test]
fn cursors_fetch_from_their_position() {
    let (sql, _) = session();
    sql("CREATE TABLE c (id int PRIMARY KEY)").unwrap();
    sql("INSERT INTO c SELECT g FROM generate_series(1, 5) g").unwrap();
    assert!(sql("DECLARE k CURSOR FOR SELECT id FROM c").is_err());
    sql("BEGIN").unwrap();
    sql("DECLARE k SCROLL CURSOR FOR SELECT id FROM c ORDER BY id").unwrap();
    let out = sql("FETCH 2 FROM k").unwrap();
    assert_eq!(
        (rows(&out), out.tag.as_str()),
        (vec!["1".into(), "2".into()], "FETCH 2")
    );
    assert_eq!(sql("MOVE LAST IN k").unwrap().tag, "MOVE 1");
    assert_eq!(rows(&sql("FETCH PRIOR FROM k").unwrap()), ["4"]);
    assert_eq!(rows(&sql("FETCH RELATIVE -2 FROM k").unwrap()), ["2"]);
    sql("DECLARE h CURSOR WITH HOLD FOR SELECT id FROM c WHERE id > 3 ORDER BY id").unwrap();
    sql("COMMIT").unwrap();
    // Only the held cursor outlives the transaction.
    assert!(sql("FETCH k").is_err());
    assert_eq!(rows(&sql("FETCH ALL FROM h").unwrap()), ["4", "5"]);
    assert_eq!(sql("CLOSE h").unwrap().tag, "CLOSE CURSOR");
    assert!(sql("CLOSE h").is_err());
}
