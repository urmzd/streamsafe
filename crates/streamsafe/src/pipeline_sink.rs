//! [`PipelineSink`]: adapt a sub-pipeline so it can be used as a [`Sink`].
//!
//! Lets a branch of a `.broadcast(...)` (or any other sink slot) own its own
//! transform chain and terminal sink, while still participating in the outer
//! pipeline's lifecycle:
//!
//! ```ignore
//! outer
//!     .broadcast(
//!         PipelineSink::new(|b| b.pipe(GifEncoder).into(GifFile::new(...))),
//!         PipelineSink::new(|b| b.pipe(PngEncoder).into(PngFile::new(...))),
//!     )
//!     .run()
//!     .await?;
//! ```
//!
//! Under the hood: [`PipelineSink::new`] eagerly spawns the sub-pipeline via
//! `tokio::spawn` (so it must be called inside a tokio runtime). Items arrive
//! over an internal bounded channel. On natural end-of-stream the outer runtime
//! calls [`Sink::finish`], which drops the sender and awaits the sub-pipeline's
//! handle, surfacing its result to the outer pipeline.

use crate::channel_source::ChannelSource;
use crate::error::{Result, StreamSafeError};
use crate::pipeline::{PipelineBuilder, SourceStage};
use crate::sink::Sink;
use std::future::Future;
use std::pin::Pin;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;

/// Trait bound for anything that can be run to completion as a sub-pipeline.
/// Implemented for [`RunnablePipeline`](crate::RunnablePipeline) and
/// [`BroadcastPipeline`](crate::BroadcastPipeline).
pub trait SubPipeline: Send + 'static {
    fn into_future(self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>>;
}

/// Adapts a sub-pipeline as a [`Sink`]. See module docs.
pub struct PipelineSink<T: Send + 'static> {
    tx: Option<mpsc::Sender<T>>,
    handle: Option<JoinHandle<Result<()>>>,
}

impl<T: Send + 'static> PipelineSink<T> {
    /// Build a sub-pipeline and adapt it as a `Sink<Input = T>`.
    ///
    /// The closure receives a [`PipelineBuilder`] whose source is an internal
    /// channel and must return a runnable pipeline.
    ///
    /// Must be called inside a tokio runtime (uses `tokio::spawn`).
    pub fn new<F, R>(build: F) -> Self
    where
        F: FnOnce(PipelineBuilder<SourceStage<ChannelSource<T>>>) -> R,
        R: SubPipeline,
    {
        Self::with_buffer(build, 64)
    }

    /// Like [`new`](Self::new), with an explicit channel buffer capacity.
    pub fn with_buffer<F, R>(build: F, buffer: usize) -> Self
    where
        F: FnOnce(PipelineBuilder<SourceStage<ChannelSource<T>>>) -> R,
        R: SubPipeline,
    {
        let (tx, rx) = mpsc::channel(buffer);
        let builder = PipelineBuilder::from(ChannelSource::new(rx));
        let runnable = build(builder);
        let handle = tokio::spawn(runnable.into_future());
        Self {
            tx: Some(tx),
            handle: Some(handle),
        }
    }
}

impl<T: Send + 'static> Sink for PipelineSink<T> {
    type Input = T;

    async fn consume(&mut self, input: T) -> Result<()> {
        let tx = self
            .tx
            .as_ref()
            .ok_or(StreamSafeError::ChannelClosed)?;
        tx.send(input)
            .await
            .map_err(|_| StreamSafeError::ChannelClosed)
    }

    async fn finish(&mut self) -> Result<()> {
        // Drop the sender so the sub-pipeline's ChannelSource sees end-of-stream.
        drop(self.tx.take());

        if let Some(handle) = self.handle.take() {
            match handle.await {
                Ok(result) => result,
                Err(join_err) => Err(StreamSafeError::other(join_err)),
            }
        } else {
            Ok(())
        }
    }
}

impl<T: Send + 'static> Drop for PipelineSink<T> {
    fn drop(&mut self) {
        // If finish wasn't called (cancellation, panic), abort the sub-pipeline
        // rather than leaving it detached.
        if let Some(handle) = self.handle.take() {
            handle.abort();
        }
    }
}
