use axum::{Router, routing::get};

pub fn web_console_routes() -> Router {
    // The cluster overview API lives in `nodus_monitoring`, which owns the live
    // `ClusterState`. The console serves only its own assets/health here.
    Router::new().route("/console/health", get(|| async { "Web Console OK" }))
}
