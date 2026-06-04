use axum::{Router, routing::get};
use tower_http::services::{ServeDir, ServeFile};

pub fn web_console_routes() -> Router {
    // The cluster overview API lives in `nodus_monitoring`, which owns the live
    // `ClusterState`. The console serves its own assets here.
    
    // Serve the frontend static files. Fall back to index.html for SPA routing.
    let serve_dir = ServeDir::new("frontend/dist")
        .not_found_service(ServeFile::new("frontend/dist/index.html"));

    Router::new()
        .route("/console/health", get(|| async { "Web Console OK" }))
        .fallback_service(serve_dir)
}
