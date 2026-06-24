//! OpenTelemetry tracing setup for NodusDB.
//!
//! `init_otlp` installs a global tracer provider that exports spans to an OTLP
//! HTTP endpoint (e.g. an OpenTelemetry Collector). `start_span` emits a span
//! using the global provider; it is a cheap no-op when no provider is installed.

use anyhow::Result;
use opentelemetry::global;
use opentelemetry::global::BoxedSpan;
use opentelemetry::trace::Tracer;
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::trace::TracerProvider;

const SERVICE: &str = "nodusd";

/// Builds an OTLP (HTTP/protobuf) batch exporter to `endpoint` and installs it
/// as the global tracer provider. Keep the returned provider alive for the
/// process lifetime; spans are flushed on a background batch processor.
pub fn init_otlp(endpoint: &str) -> Result<TracerProvider> {
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_endpoint(endpoint)
        .build()?;
    let provider = TracerProvider::builder()
        .with_batch_exporter(exporter, opentelemetry_sdk::runtime::Tokio)
        .build();
    global::set_tracer_provider(provider.clone());
    Ok(provider)
}

/// Starts a span on the global tracer. The returned guard ends the span when
/// dropped, so binding it to a local times the enclosing scope.
pub fn start_span(name: &'static str) -> BoxedSpan {
    global::tracer(SERVICE).start(name)
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry::trace::TracerProvider as _;
    use opentelemetry_sdk::testing::trace::InMemorySpanExporter;

    #[test]
    fn span_is_exported_to_provider() {
        let exporter = InMemorySpanExporter::default();
        let provider = TracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let tracer = provider.tracer("test");
        {
            let _span = tracer.start("unit-span");
        }
        let spans = exporter.get_finished_spans().unwrap();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].name.as_ref(), "unit-span");
    }

    /// Verifies the OTLP/HTTP export network path end-to-end in CI by standing up
    /// a tiny in-process collector and asserting it actually receives the
    /// exported span batch — no external collector required.
    ///
    /// Multi-threaded on purpose: `force_flush` blocks the calling thread until
    /// the batch processor drains, which would deadlock a single-threaded
    /// runtime that also has to run that processor.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn otlp_export_reaches_in_process_collector() {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};

        // A catch-all handler counts every POST the exporter makes (the path is
        // `/v1/traces`, but matching anything keeps us robust to endpoint
        // normalization).
        let hits = Arc::new(AtomicUsize::new(0));
        let handler_hits = hits.clone();
        let app = axum::Router::new().fallback(move || {
            let hits = handler_hits.clone();
            async move {
                hits.fetch_add(1, Ordering::SeqCst);
                axum::http::StatusCode::OK
            }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, app).await;
        });

        let provider = init_otlp(&format!("http://{addr}")).unwrap();
        {
            let _span = start_span("otlp-span");
        }
        let _ = provider.force_flush();

        // The export batch is delivered on a background task; give it a moment.
        let mut received = false;
        for _ in 0..50 {
            if hits.load(Ordering::SeqCst) > 0 {
                received = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }
        assert!(received, "OTLP collector never received an export");
    }

    /// Exercises real OTLP export against an external collector; ignored unless
    /// one is reachable:
    /// `NODUS_OTLP_ENDPOINT=http://127.0.0.1:4318 cargo test -p nodus_telemetry -- --ignored`
    #[tokio::test]
    #[ignore = "requires a running OTLP collector"]
    async fn otlp_export_smoke() {
        let endpoint = std::env::var("NODUS_OTLP_ENDPOINT").unwrap();
        let provider = init_otlp(&endpoint).unwrap();
        {
            let _span = start_span("otlp-span");
        }
        let _ = provider.force_flush();
    }
}
