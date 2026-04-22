use thiserror::Error;

#[derive(Error, Debug)]
pub enum PulseError {
    #[error("Broker error: {0}")]
    Broker(String),

    #[error("WebSocket error: {0}")]
    WebSocket(#[from] axum::Error),

    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Channel error: {0}")]
    Channel(String),

    #[error("Invalid topic format: {0}")]
    InvalidTopic(String),

    #[error("error: {0}")]
    ParseString(String),
}

pub type Result<T> = std::result::Result<T, PulseError>;

impl From<std::net::AddrParseError> for PulseError {
    fn from(err: std::net::AddrParseError) -> Self {
        PulseError::ParseString(format!("{}", err))
    }
}

impl From<tokio::sync::broadcast::error::RecvError> for PulseError {
    fn from(err: tokio::sync::broadcast::error::RecvError) -> Self {
        PulseError::Channel(format!("Broadcast receive error: {}", err))
    }
}

impl From<tokio::sync::broadcast::error::SendError<()>> for PulseError {
    fn from(err: tokio::sync::broadcast::error::SendError<()>) -> Self {
        PulseError::Channel(format!("Broadcast send error: {}", err))
    }
}

impl<T> From<tokio::sync::mpsc::error::SendError<T>> for PulseError {
    fn from(err: tokio::sync::mpsc::error::SendError<T>) -> Self {
        PulseError::Channel(format!("MPSC send error: {}", err))
    }
}

impl<T> From<tokio::sync::mpsc::error::TrySendError<T>> for PulseError {
    fn from(err: tokio::sync::mpsc::error::TrySendError<T>) -> Self {
        PulseError::Channel(format!("MPSC try send error: {}", err))
    }
}
