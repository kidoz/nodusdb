use axum::{Json, Router, routing::get};
use nodus_monitoring::ClusterOverview;

pub fn web_console_routes() -> Router {
    Router::new()
        .route("/console/health", get(|| async { "Web Console OK" }))
        .route("/api/v1/cluster/overview", get(get_cluster_overview))
}

async fn get_cluster_overview() -> Json<ClusterOverview> {
    let overview = ClusterOverview {
        cluster_status: "Healthy".into(),
        nodes_live: 3,
        nodes_total: 3,
        shards_total: 12,
        shards_unavailable: 0,
        qps: 1542.5,
        active_alerts: 0,
    };
    Json(overview)
}
