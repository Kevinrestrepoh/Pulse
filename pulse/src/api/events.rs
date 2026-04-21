use crate::{broker::Broker, metrics::Metrics, models::event::Event};
use axum::Json;
use serde_json::json;

pub async fn ingest_event(
    Json(event): Json<Event>,
    broker: Broker,
    metrics: Metrics,
) -> Json<serde_json::Value> {
    metrics.inc_received();

    tracing::info!(
        event_id = %event.event_id,
        payload = ?event.payload,
        "Event ingested"
    );

    let topic = event.payload.route_topic();
    broker.publish(topic, event).await;
    Json(json!({ "message": "event created" }))
}
