//! Fan-in: merge multiple sources of the same output type into one stream.
//!
//! Each source runs on its own task and writes into a shared channel. Sources
//! that exhaust simply stop sending; the merged stream ends when all sources
//! are exhausted.

use crate::error::{Result, StreamSafeError};
use crate::pipeline::Stage;
use crate::source::Source;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Stage that merges two sources of matching output type.
///
/// Construct via [`PipelineBuilder::merge`](crate::PipelineBuilder::merge).
pub struct MergeStage<S1, S2>
where
    S1: Source,
    S2: Source<Output = S1::Output>,
{
    pub(crate) s1: S1,
    pub(crate) s2: S2,
}

impl<S1, S2> Stage for MergeStage<S1, S2>
where
    S1: Source,
    S2: Source<Output = S1::Output>,
{
    type Output = S1::Output;

    fn spawn(
        self,
        buffer: usize,
        token: CancellationToken,
        _error_tx: mpsc::Sender<StreamSafeError>,
        handles: &mut Vec<JoinHandle<Result<()>>>,
    ) -> mpsc::Receiver<Self::Output> {
        let (tx, rx) = mpsc::channel(buffer);
        spawn_source_task(self.s1, tx.clone(), token.clone(), handles);
        spawn_source_task(self.s2, tx, token, handles);
        rx
    }
}

/// Three-way variant of [`MergeStage`].
///
/// Construct via [`PipelineBuilder::merge3`](crate::PipelineBuilder::merge3).
pub struct Merge3Stage<S1, S2, S3>
where
    S1: Source,
    S2: Source<Output = S1::Output>,
    S3: Source<Output = S1::Output>,
{
    pub(crate) s1: S1,
    pub(crate) s2: S2,
    pub(crate) s3: S3,
}

impl<S1, S2, S3> Stage for Merge3Stage<S1, S2, S3>
where
    S1: Source,
    S2: Source<Output = S1::Output>,
    S3: Source<Output = S1::Output>,
{
    type Output = S1::Output;

    fn spawn(
        self,
        buffer: usize,
        token: CancellationToken,
        _error_tx: mpsc::Sender<StreamSafeError>,
        handles: &mut Vec<JoinHandle<Result<()>>>,
    ) -> mpsc::Receiver<Self::Output> {
        let (tx, rx) = mpsc::channel(buffer);
        spawn_source_task(self.s1, tx.clone(), token.clone(), handles);
        spawn_source_task(self.s2, tx.clone(), token.clone(), handles);
        spawn_source_task(self.s3, tx, token, handles);
        rx
    }
}

fn spawn_source_task<S: Source>(
    mut source: S,
    tx: mpsc::Sender<S::Output>,
    token: CancellationToken,
    handles: &mut Vec<JoinHandle<Result<()>>>,
) {
    handles.push(tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                _ = token.cancelled() => return Ok(()),
                result = source.produce() => {
                    match result? {
                        Some(item) => {
                            if tx.send(item).await.is_err() {
                                return Ok(());
                            }
                        }
                        None => return Ok(()),
                    }
                }
            }
        }
    }));
}
