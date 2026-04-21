use crate::{AppState, models::event::Event};
use axum::{Json, extract::State};
use serde_json::json;

pub async fn ingest_event(
    State(state): State<AppState>,
    Json(event): Json<Event>,
) -> Json<serde_json::Value> {
    state.metrics.inc_received();

    tracing::info!(
        event_id = %event.event_id,
        payload = ?event.payload,
        "Event ingested"
    );

    let topic = event.payload.route_topic();
    state.broker.publish(topic, event).await;
    Json(json!({ "message": "event created" }))
}
