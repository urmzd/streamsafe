use crate::error::Result;

/// Consumes items (terminal stage of a pipeline).
pub trait Sink: Send + 'static {
    type Input: Send + 'static;

    fn consume(
        &mut self,
        input: Self::Input,
    ) -> impl std::future::Future<Output = Result<()>> + Send;
}
