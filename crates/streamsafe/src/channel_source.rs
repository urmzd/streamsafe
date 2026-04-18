//! `Source` implementation that reads from an external `mpsc::Receiver`.
//!
//! Used internally by [`PipelineSink`](crate::PipelineSink) to seed a
//! sub-pipeline from items delivered via the `Sink::consume` boundary.

use crate::error::Result;
use crate::source::Source;
use tokio::sync::mpsc;

/// A [`Source`] whose items arrive over an `mpsc` channel.
pub struct ChannelSource<T: Send + 'static> {
    rx: mpsc::Receiver<T>,
}

impl<T: Send + 'static> ChannelSource<T> {
    pub(crate) fn new(rx: mpsc::Receiver<T>) -> Self {
        Self { rx }
    }
}

impl<T: Send + 'static> Source for ChannelSource<T> {
    type Output = T;

    async fn produce(&mut self) -> Result<Option<T>> {
        Ok(self.rx.recv().await)
    }
}
