//! HTTP server for Prometheus metrics endpoint.

use std::convert::Infallible;
use std::net::SocketAddr;

use bytes::Bytes;
use http_body_util::Full;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tracing::{debug, error, info};

use crate::metrics::METRICS;

/// Start the metrics HTTP server on the given address.
///
/// The server exposes:
/// - GET /metrics - Prometheus format metrics
/// - GET /health - Health check endpoint
pub async fn run_metrics_server(addr: SocketAddr) -> std::io::Result<()> {
    let listener = TcpListener::bind(addr).await?;
    info!("Metrics server listening on http://{}/metrics", addr);

    loop {
        let (stream, remote_addr) = match listener.accept().await {
            Ok(conn) => conn,
            Err(e) => {
                error!("Failed to accept metrics connection: {}", e);
                continue;
            }
        };

        tokio::spawn(async move {
            let io = TokioIo::new(stream);

            let service = service_fn(handle_request);

            if let Err(e) = http1::Builder::new()
                .serve_connection(io, service)
                .await
            {
                debug!("Metrics connection error from {}: {}", remote_addr, e);
            }
        });
    }
}

/// Handle an HTTP request.
async fn handle_request(req: Request<hyper::body::Incoming>) -> Result<Response<Full<Bytes>>, Infallible> {
    let response = match (req.method(), req.uri().path()) {
        (&hyper::Method::GET, "/metrics") => {
            let snapshot = METRICS.snapshot();
            let body = snapshot.to_prometheus_string();

            Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
                .body(Full::new(Bytes::from(body)))
                .unwrap()
        }
        (&hyper::Method::GET, "/health") => {
            Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "application/json")
                .body(Full::new(Bytes::from("{\"status\":\"ok\"}")))
                .unwrap()
        }
        (&hyper::Method::GET, "/") => {
            let body = r#"<!DOCTYPE html>
<html>
<head><title>QuotaDB Metrics</title></head>
<body>
<h1>QuotaDB Metrics</h1>
<p><a href="/metrics">Metrics (Prometheus format)</a></p>
<p><a href="/health">Health Check</a></p>
</body>
</html>"#;
            Response::builder()
                .status(StatusCode::OK)
                .header("Content-Type", "text/html")
                .body(Full::new(Bytes::from(body)))
                .unwrap()
        }
        _ => {
            Response::builder()
                .status(StatusCode::NOT_FOUND)
                .body(Full::new(Bytes::from("Not Found")))
                .unwrap()
        }
    };

    Ok(response)
}

// Integration tests are done via `make smoke-test` and curl
