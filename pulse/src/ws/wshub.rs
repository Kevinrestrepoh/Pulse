use crate::{metrics::Metrics, models::event::Event};
use std::{collections::HashMap, sync::Arc};
use tokio::sync::{RwLock, mpsc};

pub type SubscriberTx = mpsc::Sender<Event>;

#[derive(Clone, Default)]
pub struct WsHub {
    topics: Arc<RwLock<HashMap<String, Vec<SubscriberTx>>>>,
    metrics: Metrics,
}

impl WsHub {
    pub fn new(metrics: Metrics) -> Self {
        WsHub {
            topics: Arc::new(RwLock::new(HashMap::new())),
            metrics,
        }
    }

    pub async fn subscribe(&self, topic: String, tx: SubscriberTx) {
        let mut topics = self.topics.write().await;
        topics.entry(topic).or_default().push(tx);
    }

    pub async fn publish(&self, topic: String, event: Event) {
        let topics = self.topics.read().await;

        if let Some(subscribers) = topics.get(&topic) {
            let priority = event.payload.priority();

            for sub in subscribers {
                match priority {
                    crate::models::event::Priority::Critical => {
                        if sub.send(event.clone()).await.is_ok() {
                            self.metrics.inc_delivered(1);
                        }
                    }
                    crate::models::event::Priority::Normal => match sub.try_send(event.clone()) {
                        Ok(_) => {
                            self.metrics.inc_delivered(1);
                        }
                        Err(err) => {
                            self.metrics.inc_dropped();
                            tracing::warn!("Dropping event for slow subscriber: {}", err);
                        }
                    },
                }
            }
        }
    }
}
