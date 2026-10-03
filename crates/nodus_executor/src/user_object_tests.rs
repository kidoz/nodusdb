//! Enums, domains, indexes on expressions, updatable views, and cursors,
//! driven through SQL.

use crate::Executor;
use crate::constraint_tests::{field, fields, session};
use crate::dml_join_tests::rows;
use nodus_catalog::CatalogWriter;

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

#[test]
fn composite_types_read_write_and_change_their_fields() {
    let (sql, _) = session();
    sql("CREATE TYPE pair AS (a int, b text)").unwrap();
    sql("CREATE TABLE ct (id int PRIMARY KEY, p pair)").unwrap();
    sql("INSERT INTO ct VALUES (1, ROW(1, 'x')), (2, '(2,y)'), (3, NULL)").unwrap();
    assert_eq!(
        ordered(&sql("SELECT id, (p).a, (p).b FROM ct ORDER BY id").unwrap()),
        ["1|1|x", "2|2|y", "3||"]
    );
    assert_eq!(
        rows(&sql("SELECT id FROM ct WHERE (p).a = 2").unwrap()),
        ["2"]
    );
    // A field write changes only that field.
    sql("UPDATE ct SET p.a = 10 WHERE id = 1").unwrap();
    assert_eq!(
        rows(&sql("SELECT p FROM ct WHERE id = 1").unwrap()),
        ["(10,x)"]
    );
    // `(p).*` spreads the fields into columns.
    let out = sql("SELECT (p).* FROM ct WHERE id = 2").unwrap();
    assert_eq!(
        (out.columns.clone(), ordered(&out)),
        (
            vec!["a".to_string(), "b".to_string()],
            vec!["2|y".to_string()]
        )
    );
    // A record of the wrong width is malformed.
    let (message, f) = fields(sql("INSERT INTO ct VALUES (4, '(1)')").unwrap_err());
    assert_eq!(message, "malformed record literal: \"(1)\"");
    assert_eq!(field(&f, "code").as_deref(), Some("22P02"));
    // The type's relation lists its attributes.
    let out = sql("SELECT attname FROM pg_attribute WHERE attrelid = \
         (SELECT typrelid FROM pg_type WHERE typname = 'pair') ORDER BY attnum")
    .unwrap();
    assert_eq!(rows(&out), ["a", "b"]);
    // An attribute added later reads as NULL in rows written before it.
    sql("ALTER TYPE pair ADD ATTRIBUTE c int").unwrap();
    assert_eq!(
        rows(&sql("SELECT p FROM ct WHERE id = 2").unwrap()),
        ["(2,y,)"]
    );
    sql("UPDATE ct SET p.c = 9 WHERE id = 2").unwrap();
    assert_eq!(
        rows(&sql("SELECT p FROM ct WHERE id = 2").unwrap()),
        ["(2,y,9)"]
    );
    sql("ALTER TYPE pair RENAME ATTRIBUTE c TO cc").unwrap();
    assert_eq!(
        rows(&sql("SELECT (p).cc FROM ct WHERE id = 2").unwrap()),
        ["9"]
    );
    sql("ALTER TYPE pair DROP ATTRIBUTE cc").unwrap();
    assert_eq!(
        rows(&sql("SELECT p FROM ct WHERE id = 2").unwrap()),
        ["(2,y)"]
    );
}

#[test]
fn a_tables_row_type_names_its_row() {
    let (sql, _) = session();
    sql("CREATE TABLE rt (a int, b text)").unwrap();
    sql("INSERT INTO rt VALUES (1, 'one')").unwrap();
    assert_eq!(rows(&sql("SELECT (rt).b FROM rt").unwrap()), ["one"]);
    assert_eq!(rows(&sql("SELECT pg_typeof(rt) FROM rt").unwrap()), ["rt"]);
    assert_eq!(rows(&sql("SELECT ROW(2, 'two')::rt").unwrap()), ["(2,two)"]);
    assert_eq!(rows(&sql("SELECT (rt).* FROM rt").unwrap()), ["1|one"]);
}

#[test]
fn type_ddl_is_authorized() {
    use nodus_audit::MemoryAuditSink;
    let (exec, cat) = crate::MemExecutor::shared(std::sync::Arc::new(MemoryAuditSink::new()));
    let role = |name: &str| {
        cat.create_role(nodus_catalog::CreateRoleRequest {
            id: nodus_catalog::PrincipalId::new(),
            name: name.into(),
            principal_type: nodus_catalog::PrincipalType::User,
            database_id: None,
        })
        .unwrap()
    };
    let admin = role("admin");
    cat.grant_privilege(nodus_catalog::GrantPrivilegeRequest {
        id: nodus_catalog::GrantId::new(),
        principal_id: admin.id,
        resource: nodus_catalog::ResourceRef::System,
        privilege: "ALL".into(),
    })
    .unwrap();
    let guest = role("guest");
    let run = |principal: nodus_catalog::PrincipalId, sql: &str| {
        let ctx = crate::ExecutionContext {
            session_id: "test".into(),
            principal_id: principal,
            active_roles: Vec::new(),
            authz_catalog_version: 1,
        };
        let mut statements = nodus_sql::parse_sql(sql)?;
        let plan = crate::plan_statement(&statements.remove(0), &[])?;
        exec.execute_logical(&ctx, plan)
    };
    run(admin.id, "CREATE TYPE mood AS ENUM ('a')").unwrap();
    run(admin.id, "CREATE DOMAIN pos AS int").unwrap();
    for sql in [
        "ALTER TYPE mood ADD VALUE 'b'",
        "ALTER TYPE mood RENAME TO humour",
        "DROP TYPE mood",
        "COMMENT ON TYPE mood IS 'x'",
        "ALTER DOMAIN pos SET NOT NULL",
        "DROP DOMAIN pos",
        "ALTER TYPE mood SET SCHEMA pg_catalog",
    ] {
        let denied = run(guest.id, sql).unwrap_err();
        assert!(
            denied.to_string().contains("permission denied"),
            "{sql}: {denied}"
        );
    }
    run(admin.id, "ALTER TYPE mood ADD VALUE 'b'").unwrap();
    run(admin.id, "COMMENT ON TYPE mood IS 'x'").unwrap();
    run(admin.id, "DROP TYPE mood").unwrap();
}

#[test]
fn range_types_read_write_and_operate() {
    let (sql, _) = session();
    sql("CREATE TABLE r (id int PRIMARY KEY, span int4range)").unwrap();
    sql("INSERT INTO r VALUES (1, '[1,5)'), (2, 'empty'), (3, '(5,9]')").unwrap();
    // Canonicalized on the way in, `empty` first in order.
    assert_eq!(
        ordered(&sql("SELECT span FROM r ORDER BY span").unwrap()),
        ["empty", "[1,5)", "[6,10)"]
    );
    assert_eq!(
        rows(&sql("SELECT id FROM r WHERE span @> 7").unwrap()),
        ["3"]
    );
    assert_eq!(
        rows(&sql("SELECT id FROM r WHERE span && '[4,6]'::int4range ORDER BY id").unwrap()),
        ["1", "3"]
    );
    assert_eq!(
        rows(&sql("SELECT '[1,5)'::int4range + '[5,9)'::int4range").unwrap()),
        ["[1,9)"]
    );
    assert_eq!(
        rows(&sql("SELECT '[1,5)'::int4range - '[4,5)'::int4range").unwrap()),
        ["[1,4)"]
    );
    assert_eq!(
        rows(&sql("SELECT lower('[1,5)'::int4range), isempty('empty'::int4range)").unwrap()),
        ["1|t"]
    );
    assert_eq!(
        rows(&sql("SELECT '[1,5)'::int4range::text").unwrap()),
        ["[1,5)"]
    );
    assert_eq!(rows(&sql("SELECT count(*) FROM pg_range").unwrap()), ["6"]);
    let (message, f) = fields(sql("SELECT '[1,5)'::int4range - '[2,3)'::int4range").unwrap_err());
    assert!(message.contains("result of range difference would not be contiguous"));
    assert_eq!(field(&f, "code").as_deref(), Some("22000"));
    let (message, f) = fields(sql("SELECT 'x'::int4range").unwrap_err());
    assert!(message.contains("malformed range literal"));
    assert_eq!(field(&f, "code").as_deref(), Some("22P02"));
    let (message, _) = fields(sql("SELECT '[1,5)'::int4range - 3").unwrap_err());
    assert_eq!(message, "operator does not exist: int4range - integer");
}

#[test]
fn a_cast_to_an_unknown_type_is_refused() {
    let (sql, _) = session();
    // As a cast, a column, an attribute, or a domain's base.
    for sql_text in [
        "SELECT 'x'::nosuch",
        "CREATE TABLE bt (x nosuch)",
        "CREATE TABLE bt (x nosuch[])",
        "CREATE TYPE bc AS (x nosuch)",
        "CREATE DOMAIN bd AS nosuch",
    ] {
        let (message, f) = fields(sql(sql_text).unwrap_err());
        assert!(
            message.starts_with("type \"nosuch"),
            "{sql_text}: {message}"
        );
        assert_eq!(field(&f, "code").as_deref(), Some("42704"), "{sql_text}");
    }
    sql("CREATE TABLE bt (x nosuch)").unwrap_err();
    // A cast to a user type's value keeps the type (`mood[]` too), even
    // though the cast is folded while the statement is planned.
    sql("CREATE TYPE mood AS ENUM ('a', 'b')").unwrap();
    sql("CREATE TABLE tm (m mood, ms mood[])").unwrap();
    sql("INSERT INTO tm VALUES ('a'::mood, ARRAY['b']::mood[])").unwrap();
    assert_eq!(rows(&sql("SELECT m, ms[1] FROM tm").unwrap()), ["a|b"]);
    let (message, _) = fields(sql("INSERT INTO tm VALUES ('c'::mood, NULL)").unwrap_err());
    assert_eq!(message, "invalid input value for enum mood: \"c\"");
    // The interval's word forms name the type too.
    sql("CREATE TABLE ti (i interval day to second)").unwrap();
}

#[test]
fn dropping_a_type_cascades_to_composite_attributes() {
    let (sql, _) = session();
    sql("CREATE TYPE mood AS ENUM ('a')").unwrap();
    sql("CREATE TYPE both AS (n int, m mood)").unwrap();
    let (message, f) = fields(sql("DROP TYPE mood").unwrap_err());
    assert_eq!(
        message,
        "cannot drop type mood because other objects depend on it"
    );
    assert_eq!(
        field(&f, "detail").as_deref(),
        Some("column m of composite type both depends on type mood")
    );
    sql("DROP TYPE mood CASCADE").unwrap();
    let out = sql("SELECT attname FROM pg_attribute WHERE attrelid = \
         (SELECT typrelid FROM pg_type WHERE typname = 'both') ORDER BY attnum")
    .unwrap();
    assert_eq!(rows(&out), ["n"]);
}

#[test]
fn a_view_writes_through_on_conflict() {
    let (sql, _) = session();
    sql("CREATE TABLE vb (id int PRIMARY KEY, name text, hits int DEFAULT 0)").unwrap();
    sql("CREATE VIEW vv AS SELECT id, name AS label, hits FROM vb WHERE hits >= 0").unwrap();
    let out = sql("INSERT INTO vv (id, label) VALUES (1, 'a') \
         ON CONFLICT (id) DO UPDATE SET hits = vv.hits + 1 RETURNING *")
    .unwrap();
    assert_eq!(ordered(&out), ["1|a|0"]);
    let out = sql("INSERT INTO vv (id, label) VALUES (1, 'b') \
         ON CONFLICT (id) DO UPDATE SET hits = vv.hits + 1, label = excluded.label RETURNING *")
    .unwrap();
    assert_eq!(ordered(&out), ["1|b|1"]);
    let out = sql("INSERT INTO vv (id, label) VALUES (1, 'c') \
         ON CONFLICT (id) DO UPDATE SET hits = 100 WHERE vv.hits > 5 RETURNING *")
    .unwrap();
    assert!(out.rows.is_empty());
    assert_eq!(ordered(&sql("SELECT * FROM vb").unwrap()), ["1|b|1"]);
}

#[test]
fn network_and_money_types_read_write_and_operate() {
    let (sql, _) = session();
    sql("CREATE TABLE n (id int PRIMARY KEY, ip inet, m macaddr, v money)").unwrap();
    sql("INSERT INTO n VALUES (1, '192.168.1.5/24', '08:00:2b:01:02:03', '1234.56')").unwrap();
    sql("INSERT INTO n VALUES (2, '10.0.0.1', '09:00:2b:01:02:03', '-5.00')").unwrap();
    sql("INSERT INTO n VALUES (3, NULL, NULL, NULL)").unwrap();
    // The stored form is canonical; the shown one is PostgreSQL's.
    assert_eq!(
        ordered(&sql("SELECT ip, m, v FROM n ORDER BY id").unwrap()),
        [
            "192.168.1.5/24|08:00:2b:01:02:03|$1,234.56",
            "10.0.0.1|09:00:2b:01:02:03|-$5.00",
            "||",
        ]
    );
    // Containment reads the address prefixes, comparisons the values.
    assert_eq!(
        ordered(&sql("SELECT ip FROM n WHERE ip <<= '192.168.0.0/16'::inet ORDER BY ip").unwrap()),
        ["192.168.1.5/24"]
    );
    assert_eq!(
        ordered(&sql("SELECT v FROM n WHERE v > '0.00'::money ORDER BY v").unwrap()),
        ["$1,234.56"]
    );
    assert_eq!(
        ordered(&sql("SELECT ip FROM n ORDER BY ip").unwrap()),
        ["10.0.0.1", "192.168.1.5/24", ""]
    );
    assert_eq!(
        rows(&sql("SELECT min(ip), max(ip), sum(v), pg_typeof(sum(v)) FROM n").unwrap()),
        ["10.0.0.1|192.168.1.5/24|$1,229.56|money"]
    );
    assert_eq!(
        ordered(&sql("SELECT cash_words('1234567.89'::money)").unwrap()),
        [
            "One million two hundred thirty four thousand five hundred sixty seven dollars and eighty nine cents"
        ]
    );
    // Money is not a number, and only its own operations take it.
    let (message, _) = fields(sql("SELECT '1234.56'::money * '2.00'::money").unwrap_err());
    assert_eq!(message, "operator does not exist: money * money");
    let (message, _) = fields(sql("SELECT '1234.56'::money + 1").unwrap_err());
    assert_eq!(message, "operator does not exist: money + integer");
    let (message, f) = fields(sql("SELECT avg(v) FROM n").unwrap_err());
    assert_eq!(message, "function avg(money) does not exist");
    assert_eq!(field(&f, "code").as_deref(), Some("42883"));
    let (message, _) = fields(sql("SELECT abs('5.00'::money)").unwrap_err());
    assert_eq!(message, "function abs(money) does not exist");
    // A check constraint over an address rejects a row, as PostgreSQL does.
    sql("CREATE TABLE ck (ip inet CHECK (ip <<= '10.0.0.0/8'::inet))").unwrap();
    sql("INSERT INTO ck VALUES ('10.1.2.3')").unwrap();
    let (message, f) = fields(sql("INSERT INTO ck VALUES ('192.168.1.1')").unwrap_err());
    assert_eq!(
        message,
        "new row for relation \"ck\" violates check constraint \"ck_ip_check\""
    );
    assert_eq!(
        field(&f, "detail").as_deref(),
        Some("Failing row contains (192.168.1.1).")
    );
    // The catalog carries the five types, their arrays, and their OIDs.
    assert_eq!(
        rows(&sql("SELECT typname FROM pg_type WHERE typname IN ('inet', 'cidr', 'macaddr', 'macaddr8', 'money') ORDER BY typname").unwrap()),
        ["cidr", "inet", "macaddr", "macaddr8", "money"]
    );
    assert_eq!(
        rows(&sql("SELECT format_type(869, NULL), format_type(650, NULL), format_type(829, NULL), format_type(774, NULL), format_type(790, NULL)").unwrap()),
        ["inet|cidr|macaddr|macaddr8|money"]
    );
}
