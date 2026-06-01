use axum::{Router, routing::get};

pub fn web_console_routes() -> Router {
    Router::new().route("/console/health", get(|| async { "Web Console OK" }))
}
