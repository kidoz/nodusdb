pub mod fault;

use std::net::SocketAddr;
use tokio::net::TcpListener;
use tokio::task::JoinHandle;

pub struct TestServer {
    pub pgwire_addr: SocketAddr,
    pub http_addr: SocketAddr,
    pgwire_task: JoinHandle<anyhow::Result<()>>,
    http_task: JoinHandle<std::io::Result<()>>,
}

impl TestServer {
    pub async fn start() -> anyhow::Result<Self> {
        let pgwire_listener = TcpListener::bind("127.0.0.1:0").await?;
        let http_listener = TcpListener::bind("127.0.0.1:0").await?;

        let handle = nodus_server::run_server(pgwire_listener, http_listener).await?;

        Ok(Self {
            pgwire_addr: handle.pgwire_addr,
            http_addr: handle.http_addr,
            pgwire_task: handle.pgwire_task,
            http_task: handle.http_task,
        })
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.pgwire_task.abort();
        self.http_task.abort();
    }
}
