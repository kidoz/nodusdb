use nodus_catalog::{
    CatalogWriter, CreateDatabaseRequest, CreateRoleRequest, CreateSchemaRequest, PrincipalId,
    PrincipalType, RoleAttributes, SchemaId,
};
use nodus_security::ScramKeys;
use tokio::net::TcpListener;
use tokio_postgres::{Client, NoTls};

fn conn_str(port: u16, database: &str, user: &str, password: &str) -> String {
    format!("host=127.0.0.1 port={port} user={user} password={password} dbname={database}")
}

async fn connect(
    port: u16,
    database: &str,
    user: &str,
    password: &str,
) -> Result<Client, tokio_postgres::Error> {
    let (client, connection) =
        tokio_postgres::connect(&conn_str(port, database, user, password), NoTls).await?;
    tokio::spawn(connection);
    Ok(client)
}

#[tokio::test(flavor = "multi_thread")]
async fn database_scoped_credentials_and_grants_are_enforced() {
    let mut config = nodus_config::NodusConfig::default();
    config.admin.password = Some("nodus".into());
    let (stop, shutdown) = tokio::sync::watch::channel(());
    let server = nodus_server::run_server_with_config(
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
        TcpListener::bind("127.0.0.1:0").await.unwrap(),
        config,
        shutdown,
    )
    .await
    .unwrap();
    let catalog = server.catalog();
    let db1 = catalog
        .create_database(CreateDatabaseRequest {
            id: nodus_catalog::DatabaseId::new(),
            name: "db1".into(),
            owner_role_id: None,
        })
        .unwrap()
        .id;
    catalog
        .create_database(CreateDatabaseRequest {
            id: nodus_catalog::DatabaseId::new(),
            name: "db2".into(),
            owner_role_id: None,
        })
        .unwrap();
    catalog
        .create_schema(CreateSchemaRequest {
            id: SchemaId::new(),
            database_id: db1,
            name: "public".into(),
            owner_role_id: None,
            managed_access: false,
        })
        .unwrap();
    let password = ScramKeys::derive("scoped-secret", vec![7], nodus_security::PBKDF2_ITERATIONS)
        .to_verifier_string();
    let scoped = catalog
        .create_role(CreateRoleRequest {
            id: PrincipalId::new(),
            name: "scoped".into(),
            principal_type: PrincipalType::User,
            database_id: Some(db1),
            attributes: RoleAttributes {
                can_login: true,
                password: Some(password),
                ..Default::default()
            },
        })
        .unwrap();

    let admin = connect(server.pgwire_addr.port(), "db1", "nodus", "nodus")
        .await
        .unwrap();
    admin
        .simple_query("CREATE TABLE items (id INT PRIMARY KEY)")
        .await
        .unwrap();
    admin
        .simple_query("INSERT INTO items VALUES (1)")
        .await
        .unwrap();
    admin
        .simple_query("GRANT SELECT ON items TO scoped")
        .await
        .unwrap();

    let scoped_client = connect(server.pgwire_addr.port(), "db1", "scoped", "scoped-secret")
        .await
        .unwrap();
    let rows = scoped_client
        .query("SELECT id FROM items", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    let has_select = scoped_client
        .query_one("SELECT has_table_privilege('items', 'SELECT')", &[])
        .await
        .unwrap();
    assert!(has_select.get::<_, bool>(0));

    // Prepared execution re-plans and re-authorizes, so revocation takes effect
    // even when the statement was prepared before REVOKE.
    let statement = scoped_client.prepare("SELECT id FROM items").await.unwrap();
    admin
        .simple_query("REVOKE SELECT ON items FROM scoped")
        .await
        .unwrap();
    assert!(scoped_client.execute(&statement, &[]).await.is_err());
    let has_select = scoped_client
        .query_one("SELECT has_table_privilege('items', 'SELECT')", &[])
        .await
        .unwrap();
    assert!(!has_select.get::<_, bool>(0));

    let wrong_database = connect(server.pgwire_addr.port(), "db2", "scoped", "scoped-secret").await;
    assert!(wrong_database.is_err());
    let unknown_database = connect(server.pgwire_addr.port(), "missing", "nodus", "nodus").await;
    assert!(unknown_database.is_err());

    drop((admin, scoped_client, statement, scoped));
    let _ = stop.send(());
    let _ = server.pgwire_task.await;
    let _ = server.http_task.await;
    for task in server.background_tasks {
        let _ = task.await;
    }
}
