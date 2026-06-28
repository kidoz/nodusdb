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
