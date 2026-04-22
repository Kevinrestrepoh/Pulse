use std::sync::Arc;

use crate::{
    error::{PulseError, Result},
    models::event::Event,
    ws::wshub::WsHub,
};
use tokio::sync::{broadcast, mpsc};

#[derive(Clone)]
pub struct Broker {
    sender: mpsc::Sender<BrokerMessage>,
}

#[derive(Clone)]
pub struct BrokerMessage {
    pub topic: String,
    pub event: Event,
}

impl Broker {
    pub fn new(
        buffer: usize,
        hub: WsHub,
        shutdown: broadcast::Receiver<()>,
    ) -> (Self, BrokerWorker) {
        let (tx, rx) = mpsc::channel(buffer);
        let broker = Broker { sender: tx };
        let worker = BrokerWorker {
            receiver: rx,
            hub,
            shutdown,
        };
        (broker, worker)
    }

    pub async fn publish(&self, topic: String, event: Event) -> Result<()> {
        let msg = BrokerMessage { topic, event };
        self.sender
            .send(msg)
            .await
            .map_err(|e| PulseError::Broker(format!("Failed to publish event to broker: {}", e)))?;
        Ok(())
    }
}

pub struct BrokerWorker {
    receiver: mpsc::Receiver<BrokerMessage>,
    hub: WsHub,
    shutdown: broadcast::Receiver<()>,
}

impl BrokerWorker {
    pub async fn run(mut self) {
        loop {
            tokio::select! {
                maybe_msg = self.receiver.recv() => {
                    if let Some(msg) = maybe_msg {
                        if let Err(e) = self.hub.publish(msg.topic, Arc::new(msg.event)).await {
                            tracing::error!("Failed to publish event to hub: {}", e);
                        }
                    } else {
                        tracing::info!("Broker channel closed, shutting down");
                        break;
                    }
                }
                _ = self.shutdown.recv() => {
                    tracing::info!("Broker worker shutting down gracefully");
                    break;
                }
            }
        }

        tracing::info!("Broker worker shut down");
    }
}
