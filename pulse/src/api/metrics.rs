use crate::metrics::Metrics;
use axum::response::IntoResponse;
use std::sync::atomic::Ordering;

pub async fn metrics_handler(metrics: Metrics) -> impl IntoResponse {
    format!(
        "events_received {}\nevents_delivered {}\ndropped_events {}\nactive_ws {}\n",
        metrics.events_received.load(Ordering::Relaxed),
        metrics.events_delivered.load(Ordering::Relaxed),
        metrics.dropped_events.load(Ordering::Relaxed),
        metrics.active_ws.load(Ordering::Relaxed),
    )
}
