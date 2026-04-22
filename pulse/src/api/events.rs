use crate::{AppState, models::event::Event};
use axum::{Json, extract::State, response::IntoResponse};
use serde_json::json;

pub async fn ingest_event(
    State(state): State<AppState>,
    Json(event): Json<Event>,
) -> impl IntoResponse {
    state.metrics.inc_received();

    tracing::info!(
        event_id = %event.event_id,
        payload = ?event.payload,
        "Event ingested"
    );

    let topic = event.payload.route_topic();
    match state.broker.publish(topic, event).await {
        Ok(_) => (
            axum::http::StatusCode::ACCEPTED,
            Json(json!({ "message": "event created" })),
        ),
        Err(e) => {
            tracing::error!("Failed to publish event: {}", e);
            (
                axum::http::StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "error": "Failed to process event" })),
            )
        }
    }
}
