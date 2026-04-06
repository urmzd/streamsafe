use crate::error::Result;

/// Produces items. Returns `Ok(None)` when the stream is exhausted.
pub trait Source: Send + 'static {
    type Output: Send + 'static;

    fn produce(&mut self)
        -> impl std::future::Future<Output = Result<Option<Self::Output>>> + Send;
}
