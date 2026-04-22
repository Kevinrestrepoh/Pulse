use crate::{AppState, metrics::Metrics, models::event::Event, ws::wshub::WsHub};
use axum::{
    extract::{
        Query, State,
        ws::{Message, WebSocket, WebSocketUpgrade},
    },
    response::IntoResponse,
};
use futures::SinkExt;
use std::{collections::HashMap, time::Duration};
use tokio::{
    sync::{broadcast, mpsc},
    time::interval,
};

pub async fn ws_handler(
    State(state): State<AppState>,
    ws: WebSocketUpgrade,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let topic = params
        .get("topic")
        .cloned()
        .unwrap_or_else(|| "default".to_string());

    let hub = state.hub.clone();
    let metrics = state.metrics.clone();
    let shutdown = state.shutdown_tx.clone();

    ws.on_upgrade(move |socket| handle_socket(socket, hub, topic, shutdown.subscribe(), metrics))
}

async fn handle_socket(
    mut socket: WebSocket,
    hub: WsHub,
    topic: String,
    mut shutdown: broadcast::Receiver<()>,
    metrics: Metrics,
) {
    let (tx, mut rx) = mpsc::channel::<Event>(32);

    if let Err(e) = hub.subscribe(topic.clone(), tx).await {
        tracing::error!("Failed to subscribe to topic '{}': {}", topic, e);
        let _ = socket.close().await;
        return;
    }

    let mut ticker = interval(Duration::from_millis(50));
    let mut buffer: Vec<Event> = Vec::with_capacity(32);

    metrics.inc_ws();

    loop {
        tokio::select! {
            maybe_event = rx.recv() => {
                match maybe_event {
                    Some(event) => buffer.push(event),
                    None => break,
                }
            }
            _ = ticker.tick() => {
                if !buffer.is_empty() {
                    match serde_json::to_string(&buffer) {
                        Ok(json) => {
                            if let Err(e) = socket.send(Message::Text(Into::into(json))).await {
                                tracing::error!("WebSocket connection error: {}", e);
                                break;
                            }
                        }
                        Err(e) => {
                            tracing::error!("Failed to serialize events: {}", e);
                            break;
                        }
                    }
                    buffer.clear();
                }
            }
            _ = shutdown.recv() => {
                tracing::info!("WebSocket shutting down gracefully");
                let _ = socket.close().await;
                break;
            }
        }
    }

    metrics.dec_ws();
    tracing::info!("WebSocket client disconnected from {}", topic);
}
