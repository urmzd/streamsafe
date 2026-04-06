#[derive(Debug, thiserror::Error)]
pub enum StreamSafeError {
    #[error("pipeline cancelled")]
    Cancelled,

    #[error("channel closed")]
    ChannelClosed,

    #[error("{0}")]
    Other(#[from] Box<dyn std::error::Error + Send + Sync>),
}

pub type Result<T> = std::result::Result<T, StreamSafeError>;

impl StreamSafeError {
    pub fn other(e: impl std::error::Error + Send + Sync + 'static) -> Self {
        Self::Other(Box::new(e))
    }
}
