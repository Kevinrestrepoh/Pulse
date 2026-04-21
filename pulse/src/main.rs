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

#[derive(Clone)]
struct AppState {
    broker: broker::Broker,
    hub: ws::wshub::WsHub,
    metrics: metrics::Metrics,
    shutdown_tx: broadcast::Sender<()>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();
    let addr: SocketAddr = "0.0.0.0:8080".parse()?;

    let (shutdown_tx, _) = broadcast::channel::<()>(1);

    let shutdown_tx_graceful = shutdown_tx.clone();
    let shutdown_tx_ctrlc = shutdown_tx.clone();
    tokio::spawn(async move {
        signal::ctrl_c().await.unwrap();
        tracing::info!("Shutdown signal received");
        let _ = shutdown_tx_ctrlc.send(());
    });

    let metrics = metrics::Metrics::default();

    let hub = ws::wshub::WsHub::new(metrics.clone());
    let (broker, worker) = broker::Broker::new(1024, hub.clone(), shutdown_tx.subscribe());
    tokio::spawn(async move {
        worker.run().await;
    });

    let state = AppState {
        broker,
        hub,
        metrics,
        shutdown_tx,
    };

    let app = Router::new()
        .route("/health", get(api::health))
        .route("/events", post(api::events::ingest_event))
        .route("/ws", get(ws::handler::ws_handler))
        .route("/metrics", get(api::metrics::metrics_handler))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();

    tracing::info!("Server listening on port {}", addr);

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(shutdown_tx_graceful))
        .await
        .unwrap();

    Ok(())
}

async fn shutdown_signal(shutdown_tx: broadcast::Sender<()>) {
    tracing::info!("Shutdown signal received");

    // Notify all workers
    let _ = shutdown_tx.send(());
}
