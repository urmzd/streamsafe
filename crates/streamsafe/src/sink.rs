use crate::error::Result;

/// Consumes items (terminal stage of a pipeline).
///
/// The pipeline runtime calls [`consume`](Self::consume) once per upstream item,
/// then [`finish`](Self::finish) once after the upstream channel closes (natural
/// end-of-stream, not cancellation). Use `finish` to flush buffers, await inner
/// tasks, or finalize output.
pub trait Sink: Send + 'static {
    type Input: Send + 'static;

    fn consume(
        &mut self,
        input: Self::Input,
    ) -> impl std::future::Future<Output = Result<()>> + Send;

    /// Called once after the upstream stream closes naturally. Default: no-op.
    /// Not called on external cancellation.
    fn finish(&mut self) -> impl std::future::Future<Output = Result<()>> + Send {
        async { Ok(()) }
    }
}
