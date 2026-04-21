use std::net::SocketAddr;

use axum::{
    Router,
    routing::{get, post},
};
use tokio::{signal, sync::broadcast};
use tracing_subscriber;

mod api;
mod broker;
mod metrics;
mod models;
mod ws;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let addr: SocketAddr = "0.0.0.0:8080".parse()?;

    let (shutdown_tx, _) = broadcast::channel::<()>(1);

    let shutdown_tx_clone = shutdown_tx.clone();
    tokio::spawn(async move {
        signal::ctrl_c().await.unwrap();
        tracing::info!("Shutdown signal received");
        let _ = shutdown_tx_clone.send(());
    });

    let metrics = metrics::Metrics::default();

    let hub = ws::wshub::WsHub::new(metrics.clone());
    let (broker, worker) = broker::Broker::new(1024, hub.clone(), shutdown_tx.subscribe());
    tokio::spawn(async move {
        worker.run().await;
    });

    let app = Router::new()
        .route("/health", get(api::health))
        .route(
            "/events",
            post({
                let broker = broker.clone();
                let metrics = metrics.clone();
                move |payload| api::events::ingest_event(payload, broker, metrics)
            }),
        )
        .route(
            "/ws",
            get({
                let hub = hub.clone();
                let metrics = metrics.clone();
                let shutdown_tx = shutdown_tx.clone();
                move |ws, query| ws::handler::ws_handler(ws, query, hub, shutdown_tx, metrics)
            }),
        )
        .route(
            "/metrics",
            get(move || api::metrics::metrics_handler(metrics.clone())),
        );

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();

    tracing::info!("Server listening on port {}", addr);

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(shutdown_tx.clone()))
        .await
        .unwrap();

    Ok(())
}

async fn shutdown_signal(shutdown_tx: broadcast::Sender<()>) {
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install Ctrl+C handler");

    tracing::info!("Shutdown signal received");

    // Notify all workers
    let _ = shutdown_tx.send(());
}
