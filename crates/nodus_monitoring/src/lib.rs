use axum::{Router, http::StatusCode, routing::get};
use prometheus_client::registry::Registry;
use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

pub struct AppState {
    pub is_ready: AtomicBool,
    pub registry: std::sync::Mutex<Registry>,
}

impl Default for AppState {
    fn default() -> Self {
        Self {
            is_ready: AtomicBool::new(false),
            registry: std::sync::Mutex::new(Registry::default()),
        }
    }
}

pub fn monitoring_routes(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics))
        .with_state(state)
}

async fn healthz() -> &'static str {
    "OK"
}

async fn readyz(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
) -> Result<&'static str, StatusCode> {
    if state.is_ready.load(Ordering::Acquire) {
        Ok("OK")
    } else {
        Err(StatusCode::SERVICE_UNAVAILABLE)
    }
}

async fn metrics(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
) -> Result<String, StatusCode> {
    let mut buffer = String::new();
    let registry = state
        .registry
        .lock()
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    prometheus_client::encoding::text::encode(&mut buffer, &registry)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(buffer)
}
