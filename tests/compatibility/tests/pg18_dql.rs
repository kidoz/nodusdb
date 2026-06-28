use nodus_testkit::TestServer;
use std::time::Duration;
use tokio_postgres::{NoTls, SimpleQueryMessage};

async fn connect(server: &TestServer) -> tokio_postgres::Client {
    let conn_str = format!(
        "host={} port={} user=nodus password=nodus dbname=default",
        server.pgwire_addr.ip(),
        server.pgwire_addr.port()
    );
    for _ in 0..30 {
        if let Ok((client, connection)) = tokio_postgres::connect(&conn_str, NoTls).await {
            tokio::spawn(async move {
                let _ = connection.await;
            });
            return client;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("PGWire server did not start in time");
}

fn rows_of(msgs: &[SimpleQueryMessage]) -> Vec<&tokio_postgres::SimpleQueryRow> {
    msgs.iter()
        .filter_map(|m| match m {
            SimpleQueryMessage::Row(r) => Some(r),
            _ => None,
        })
        .collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pg18_joins() {
    let server = TestServer::start().await.expect("server starts");
    let client = connect(&server).await;

    client
        .simple_query("CREATE SCHEMA pg18_dql;")
        .await
        .unwrap();

    client
        .simple_query(
            "CREATE TABLE pg18_dql.departments (dept_id INT PRIMARY KEY, dept_name TEXT);",
        )
        .await
        .unwrap();
    client.simple_query("CREATE TABLE pg18_dql.employees (emp_id INT PRIMARY KEY, name TEXT, dept_id INT, salary NUMERIC);").await.unwrap();

    client.simple_query("INSERT INTO pg18_dql.departments (dept_id, dept_name) VALUES (1, 'Engineering'), (2, 'Sales'), (3, 'HR');").await.unwrap();
    client.simple_query("INSERT INTO pg18_dql.employees (emp_id, name, dept_id, salary) VALUES (101, 'Alice', 1, 90000), (102, 'Bob', 1, 85000), (103, 'Charlie', 2, 75000), (104, 'David', NULL, 60000);").await.unwrap();

    // INNER JOIN
    let msgs = client.simple_query("SELECT e.emp_id, e.name, d.dept_name FROM pg18_dql.employees e JOIN pg18_dql.departments d ON e.dept_id = d.dept_id ORDER BY e.emp_id;").await.unwrap();
    let rows = rows_of(&msgs);
    if let Some(r) = rows.first() {
        println!(
            "COLUMNS: {:?}",
            r.columns().iter().map(|c| c.name()).collect::<Vec<_>>()
        );
    }
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get("emp_id"), Some("101"));
    assert_eq!(rows[1].get("emp_id"), Some("102"));
    assert_eq!(rows[2].get("emp_id"), Some("103"));

    // LEFT JOIN
    let msgs = client.simple_query("SELECT e.emp_id, e.name, d.dept_name FROM pg18_dql.employees e LEFT JOIN pg18_dql.departments d ON e.dept_id = d.dept_id ORDER BY e.emp_id;").await.unwrap();
    let rows = rows_of(&msgs);
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[3].get("emp_id"), Some("104"));
    assert_eq!(rows[3].get("dept_name"), None); // NULL in the text format

    // RIGHT JOIN
    let msgs = client.simple_query("SELECT e.emp_id, e.name, d.dept_name FROM pg18_dql.employees e RIGHT JOIN pg18_dql.departments d ON e.dept_id = d.dept_id ORDER BY d.dept_id, e.emp_id;").await.unwrap();
    let rows = rows_of(&msgs);
    assert_eq!(rows.len(), 4);
    assert_eq!(rows[3].get("dept_name"), Some("HR"));
    assert_eq!(rows[3].get("emp_id"), None);

    // FULL OUTER JOIN
    let msgs = client.simple_query("SELECT e.emp_id, e.name, d.dept_name FROM pg18_dql.employees e FULL OUTER JOIN pg18_dql.departments d ON e.dept_id = d.dept_id;").await.unwrap();
    let rows = rows_of(&msgs);
    assert_eq!(rows.len(), 5);
}

/// JOIN ... USING (col) and NATURAL JOIN — equi-joins over named common columns.
/// Both are used by IDE introspection (and ordinary SQL); they must equi-join on
/// the shared column, not error or cross-join.
#[tokio::test(flavor = "multi_thread")]
async fn test_pg18_join_using_and_natural() {
    let server = TestServer::start().await.expect("server starts");
    let client = connect(&server).await;
    client.simple_query("CREATE SCHEMA j;").await.unwrap();
    client
        .simple_query("CREATE TABLE j.departments (dept_id INT PRIMARY KEY, dept_name TEXT);")
        .await
        .unwrap();
    client
        .simple_query("CREATE TABLE j.employees (emp_id INT PRIMARY KEY, name TEXT, dept_id INT);")
        .await
        .unwrap();
    client
        .simple_query(
            "INSERT INTO j.departments (dept_id, dept_name) VALUES (1, 'Eng'), (2, 'Sales');",
        )
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO j.employees (emp_id, name, dept_id) VALUES (101, 'Alice', 1), (102, 'Bob', 2), (103, 'Cara', 1);")
        .await
        .unwrap();

    // USING (dept_id): one row per employee, matched to its department.
    let msgs = client
        .simple_query(
            "SELECT e.name, d.dept_name FROM j.employees e \
             JOIN j.departments d USING (dept_id) ORDER BY e.emp_id;",
        )
        .await
        .expect("JOIN USING resolves");
    let rows = rows_of(&msgs);
    assert_eq!(
        rows.len(),
        3,
        "USING must equi-join on dept_id, not cross-join"
    );
    assert_eq!(rows[0].get("dept_name"), Some("Eng"));
    assert_eq!(rows[1].get("dept_name"), Some("Sales"));

    // NATURAL JOIN: dept_id is the only common column, so same pairing.
    let msgs = client
        .simple_query(
            "SELECT name, dept_name FROM j.employees NATURAL JOIN j.departments ORDER BY emp_id;",
        )
        .await
        .expect("NATURAL JOIN resolves");
    assert_eq!(rows_of(&msgs).len(), 3, "NATURAL must equi-join on dept_id");

    // An ARRAY[...] constructor over a column ref in the projection must not fail
    // the statement (introspection builds these, e.g. ARRAY[d.objsubid]).
    let msgs = client
        .simple_query("SELECT ARRAY[emp_id] FROM j.employees;")
        .await
        .expect("ARRAY[col] projection does not error");
    assert_eq!(rows_of(&msgs).len(), 3);

    // A qualified wildcard `t.*` projects the table's columns rather than erroring.
    let msgs = client
        .simple_query("SELECT e.* FROM j.employees e ORDER BY e.emp_id;")
        .await
        .expect("qualified wildcard resolves");
    let rows = rows_of(&msgs);
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get("name"), Some("Alice"));
    assert_eq!(rows[0].get("dept_id"), Some("1"));
}

/// ALTER TABLE ... ALTER COLUMN ... SET DATA TYPE retypes a column (catalog
/// metadata) without erroring, and the table keeps serving its rows.
#[tokio::test(flavor = "multi_thread")]
async fn test_pg18_alter_column_set_data_type() {
    let server = TestServer::start().await.expect("server starts");
    let client = connect(&server).await;
    client.simple_query("CREATE SCHEMA a;").await.unwrap();
    client
        .simple_query("CREATE TABLE a.t (id INT PRIMARY KEY, v TEXT);")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO a.t (id, v) VALUES (1, 'x'), (2, 'y');")
        .await
        .unwrap();

    client
        .simple_query("ALTER TABLE a.t ALTER COLUMN id SET DATA TYPE NUMERIC USING id::NUMERIC;")
        .await
        .expect("ALTER COLUMN SET DATA TYPE succeeds");

    // The new declared type is reflected in the catalog.
    let msgs = client
        .simple_query(
            "SELECT data_type FROM information_schema.columns \
             WHERE table_schema = 'a' AND table_name = 't' AND column_name = 'id';",
        )
        .await
        .unwrap();
    let rows = rows_of(&msgs);
    assert_eq!(rows.len(), 1);
    assert!(
        rows[0]
            .get("data_type")
            .unwrap_or_default()
            .to_lowercase()
            .contains("numeric"),
        "id should be retyped to numeric, got {:?}",
        rows[0].get("data_type")
    );

    // Existing rows still read back.
    let msgs = client
        .simple_query("SELECT id, v FROM a.t ORDER BY id;")
        .await
        .unwrap();
    assert_eq!(rows_of(&msgs).len(), 2);
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pg18_ctes() {
    let server = TestServer::start().await.expect("server starts");
    let client = connect(&server).await;

    client
        .simple_query("CREATE SCHEMA pg18_dql;")
        .await
        .unwrap();
    client
        .simple_query(
            "CREATE TABLE pg18_dql.employees (emp_id INT PRIMARY KEY, name TEXT, dept_id INT);",
        )
        .await
        .unwrap();
    client.simple_query("INSERT INTO pg18_dql.employees (emp_id, name, dept_id) VALUES (101, 'Alice', 1), (102, 'Bob', 1);").await.unwrap();

    // Basic CTE
    let msgs = client.simple_query("WITH eng_emps AS (SELECT emp_id, name FROM pg18_dql.employees WHERE dept_id = 1) SELECT emp_id, name FROM eng_emps ORDER BY emp_id;").await.unwrap();
    let rows = rows_of(&msgs);
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get("name"), Some("Alice"));
    assert_eq!(rows[1].get("name"), Some("Bob"));
}

#[tokio::test(flavor = "multi_thread")]
async fn test_pg18_window_functions() {
    let server = TestServer::start().await.expect("server starts");
    let client = connect(&server).await;

    client
        .simple_query("CREATE SCHEMA pg18_dql;")
        .await
        .unwrap();
    client.simple_query("CREATE TABLE pg18_dql.employees (emp_id INT PRIMARY KEY, name TEXT, dept_id INT, salary NUMERIC);").await.unwrap();
    client.simple_query("INSERT INTO pg18_dql.employees (emp_id, name, dept_id, salary) VALUES (101, 'Alice', 1, 90000), (102, 'Bob', 1, 85000), (103, 'Charlie', 2, 75000), (104, 'David', 1, 60000);").await.unwrap();

    let msgs = client.simple_query("SELECT emp_id, name, ROW_NUMBER() OVER (PARTITION BY dept_id ORDER BY salary DESC) as rnk FROM pg18_dql.employees ORDER BY emp_id;").await.unwrap();
    let rows = rows_of(&msgs);
    assert_eq!(rows.len(), 4);
    // 101 -> rnk 1, 102 -> rnk 2, 103 -> rnk 1, 104 -> rnk 3
    assert_eq!(rows[0].get("rnk"), Some("1"));
    assert_eq!(rows[1].get("rnk"), Some("2"));
    assert_eq!(rows[2].get("rnk"), Some("1"));
    assert_eq!(rows[3].get("rnk"), Some("3"));
}

/// Set-returning functions in FROM: `generate_series`, `unnest` (+ WITH
/// ORDINALITY and column aliases), and the comma/lateral form used by
/// introspection. These previously failed to parse/plan.
#[tokio::test(flavor = "multi_thread")]
async fn test_pg18_table_functions() {
    let server = TestServer::start().await.expect("server starts");
    let client = connect(&server).await;

    // Standalone generate_series.
    let msgs = client
        .simple_query("SELECT * FROM generate_series(1, 4);")
        .await
        .expect("generate_series resolves");
    assert_eq!(rows_of(&msgs).len(), 4);

    // unnest of a literal array WITH ORDINALITY, with AS u(value, n) column names.
    let msgs = client
        .simple_query(
            "SELECT v, n FROM unnest(ARRAY['a','b','c']) WITH ORDINALITY AS u(v, n) ORDER BY n;",
        )
        .await
        .expect("unnest WITH ORDINALITY resolves");
    let rows = rows_of(&msgs);
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get("v"), Some("a"));
    assert_eq!(rows[0].get("n"), Some("1"));
    assert_eq!(rows[2].get("n"), Some("3"));

    // Comma/lateral form: a table cross-joined with a set-returning function —
    // the shape introspection uses (`FROM t, unnest(...) WITH ORDINALITY`).
    client
        .simple_query("CREATE TABLE tf (id INT PRIMARY KEY);")
        .await
        .unwrap();
    client
        .simple_query("INSERT INTO tf (id) VALUES (10);")
        .await
        .unwrap();
    let msgs = client
        .simple_query("SELECT t.id FROM tf t, generate_series(1, 3) AS g;")
        .await
        .expect("comma-joined table function resolves");
    assert_eq!(rows_of(&msgs).len(), 3, "1 driving row x 3 series rows");
}
