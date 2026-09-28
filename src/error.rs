use thiserror::Error;

/// Why the consumer could not record a frame.
#[derive(Error, Debug)]
pub enum ConnectorError {
    #[error("invalid JSON: {0}")]
    Json(#[from] serde_json::Error),
    #[error("frame has no `{0}` field")]
    MissingField(&'static str),
    #[error("writer channel is closed")]
    WriterClosed,
    /// The connection task that owns the socket is gone, so control messages
    /// (subscription retries) can no longer be delivered.
    #[error("websocket connection task is gone")]
    ConnectionGone,
}
