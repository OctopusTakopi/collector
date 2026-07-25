use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConnectorError {
    #[error("SerdeError: {0}")]
    SerdeError(#[from] serde_json::Error),
    #[error("format error")]
    FormatError,
    #[error("writer channel is closed")]
    WriterClosed,
    /// The connection task that owns the socket is gone, so control messages
    /// (subscription retries) can no longer be delivered.
    #[error("websocket connection task is gone")]
    ConnectionGone,
}
