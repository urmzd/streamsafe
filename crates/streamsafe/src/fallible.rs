use crate::error::{Result, StreamSafeError};

/// Transform that may emit a recoverable, per-item error without aborting the pipeline.
///
/// Returns `Ok(Ok(output))` on success, `Ok(Err(err))` to route a single item to the
/// error rail while the main stream continues, or `Err(err)` to abort the stage.
/// Use with [`.try_pipe()`](crate::PipelineBuilder::try_pipe).
pub trait FallibleTransform: Send + 'static {
    type Input: Send + 'static;
    type Output: Send + 'static;

    fn apply(
        &mut self,
        input: Self::Input,
    ) -> impl std::future::Future<Output = Result<std::result::Result<Self::Output, StreamSafeError>>>
           + Send;
}
