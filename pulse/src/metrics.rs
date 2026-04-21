use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

#[derive(Default, Clone)]
pub struct Metrics {
    pub events_received: Arc<AtomicU64>,
    pub events_delivered: Arc<AtomicU64>,
    pub dropped_events: Arc<AtomicU64>,
    pub active_ws: Arc<AtomicU64>,
}

impl Metrics {
    pub fn inc_received(&self) {
        self.events_received.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_delivered(&self, n: u64) {
        self.events_delivered.fetch_add(n, Ordering::Relaxed);
    }

    pub fn inc_dropped(&self) {
        self.dropped_events.fetch_add(1, Ordering::Relaxed);
    }

    pub fn inc_ws(&self) {
        self.active_ws.fetch_add(1, Ordering::Relaxed);
    }

    pub fn dec_ws(&self) {
        self.active_ws.fetch_sub(1, Ordering::Relaxed);
    }
}
