use crate::error::{Result, StreamSafeError};
use crate::sink::Sink;

/// Default error-rail sink. Drops recoverable errors after a warn-level log.
///
/// Used when the pipeline is built without an explicit [`.on_errors()`](crate::PipelineBuilder::on_errors)
/// call. Fatal errors (those returned by the outer `Err` of `produce`/`apply`/`consume`)
/// are unaffected — they still surface as the pipeline's return value.
#[derive(Default, Clone, Copy, Debug)]
pub struct DiscardErrors;

impl Sink for DiscardErrors {
    type Input = StreamSafeError;

    async fn consume(&mut self, err: StreamSafeError) -> Result<()> {
        tracing::warn!(error = %err, "pipeline error discarded (no .on_errors sink configured)");
        Ok(())
    }
}
