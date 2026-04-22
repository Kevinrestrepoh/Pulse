use crate::{metrics::Metrics, models::event::Event, error::{PulseError, Result}};
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

    pub async fn subscribe(&self, topic: String, tx: SubscriberTx) -> Result<()> {
        if topic.is_empty() {
            return Err(PulseError::InvalidTopic("Topic cannot be empty".to_string()));
        }
        
        if topic.len() > 255 {
            return Err(PulseError::InvalidTopic("Topic too long (max 255 chars)".to_string()));
        }
        
        let mut topics = self.topics.write().await;
        topics.entry(topic).or_default().push(tx);
        Ok(())
    }

    pub async fn publish(&self, topic: String, event: Event) -> Result<()> {
        let topics = self.topics.read().await;

        if let Some(subscribers) = topics.get(&topic) {
            let priority = event.payload.priority();

            for sub in subscribers {
                match priority {
                    crate::models::event::Priority::Critical => {
                        sub.send(event.clone()).await
                            .map_err(|e| PulseError::Channel(format!("Critical event send failed: {}", e)))?;
                        self.metrics.inc_delivered(1);
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
        Ok(())
    }
}
