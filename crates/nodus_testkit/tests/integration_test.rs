use nodus_testkit::TestServer;
use reqwest::Client;
use std::time::Duration;

#[tokio::test]
async fn test_server_health_endpoints() {
    let server = TestServer::start().await.expect("Failed to start server");

    let client = Client::new();

    // Wait for the server to start with retries
    let mut is_up = false;
    for _ in 0..30 {
        if let Ok(res) = client
            .get(format!("http://{}/healthz", server.http_addr))
            .send()
            .await
            && res.status().is_success()
        {
            is_up = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(is_up, "Server did not start in time");

    let res = client
        .get(format!("http://{}/healthz", server.http_addr))
        .send()
        .await
        .expect("Failed to fetch healthz");
    assert!(res.status().is_success());
    assert_eq!(res.text().await.unwrap(), "OK");

    let res = client
        .get(format!("http://{}/readyz", server.http_addr))
        .send()
        .await
        .expect("Failed to fetch readyz");
    assert!(res.status().is_success());
    assert_eq!(res.text().await.unwrap(), "OK");

    let res = client
        .get(format!("http://{}/metrics", server.http_addr))
        .send()
        .await
        .expect("Failed to fetch metrics");
    assert!(res.status().is_success());
}
