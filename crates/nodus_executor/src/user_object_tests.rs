//! Indexes on expressions, driven through SQL.

use crate::constraint_tests::{field, fields, session};
use crate::dml_join_tests::rows;

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
