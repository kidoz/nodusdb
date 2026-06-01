use reqwest::Client;
use std::process::Command;
use std::time::Duration;

#[tokio::test]
async fn test_server_health_endpoints() {
    // Execute the binary directly to avoid cargo build locks
    let mut server = Command::new("../../target/debug/nodus_server")
        .spawn()
        .expect("Failed to start server. Make sure it is compiled first.");

    let client = Client::new();

    // Wait for the server to start with retries
    let mut is_up = false;
    for _ in 0..30 {
        if let Ok(res) = client.get("http://127.0.0.1:8088/healthz").send().await
            && res.status().is_success()
        {
            is_up = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    assert!(is_up, "Server did not start in time");

    let res = client
        .get("http://127.0.0.1:8088/healthz")
        .send()
        .await
        .expect("Failed to fetch healthz");
    assert!(res.status().is_success());
    assert_eq!(res.text().await.unwrap(), "OK");

    let res = client
        .get("http://127.0.0.1:8088/readyz")
        .send()
        .await
        .expect("Failed to fetch readyz");
    assert!(res.status().is_success());
    assert_eq!(res.text().await.unwrap(), "OK");

    let res = client
        .get("http://127.0.0.1:8088/metrics")
        .send()
        .await
        .expect("Failed to fetch metrics");
    assert!(res.status().is_success());

    server.kill().expect("Failed to kill server");
    server.wait().expect("Failed to wait on server");
}
