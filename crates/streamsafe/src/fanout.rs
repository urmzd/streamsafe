//! Concurrent fan-out to multiple sinks.
//!
//! [`BroadcastPipeline`] spawns each terminal sink on its own task with its own
//! bounded channel. A slow sink only backs up its own channel; sibling sinks
//! continue at their own pace up to the configured buffer depth.
//!
//! A sub-sink returning an error ends only that branch's task. Its error
//! surfaces in the final aggregated result; siblings are not cancelled.

use crate::error::{Result, StreamSafeError};
use crate::error_sink::DiscardErrors;
use crate::pipeline::Stage;
use crate::pipeline_sink::SubPipeline;
use crate::runtime::{ctrlc_token, join_handles, spawn_error_sink};
use crate::sink::Sink;
use std::future::Future;
use std::pin::Pin;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Terminal pipeline that fans each item out to two sinks concurrently.
///
/// Constructed via [`PipelineBuilder::broadcast`](crate::PipelineBuilder::broadcast).
pub struct BroadcastPipeline<
    Stg: Stage,
    A: Sink<Input = Stg::Output>,
    B: Sink<Input = Stg::Output>,
    E: Sink<Input = StreamSafeError> = DiscardErrors,
> where
    Stg::Output: Clone,
{
    pub(crate) stage: Stg,
    pub(crate) sink_a: A,
    pub(crate) sink_b: B,
    pub(crate) error_sink: E,
    pub(crate) buffer: usize,
}

impl<Stg, A, B, E> SubPipeline for BroadcastPipeline<Stg, A, B, E>
where
    Stg: Stage,
    A: Sink<Input = Stg::Output>,
    B: Sink<Input = Stg::Output>,
    Stg::Output: Clone,
    E: Sink<Input = StreamSafeError>,
{
    fn into_future(self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>> {
        Box::pin(self.run())
    }
}

impl<Stg, A, B, E> BroadcastPipeline<Stg, A, B, E>
where
    Stg: Stage,
    A: Sink<Input = Stg::Output>,
    B: Sink<Input = Stg::Output>,
    Stg::Output: Clone,
    E: Sink<Input = StreamSafeError>,
{
    /// Run the pipeline with an internal Ctrl-C shutdown handler.
    pub async fn run(self) -> Result<()> {
        self.run_with_token(ctrlc_token()).await
    }

    /// Run the pipeline with an externally-provided cancellation token.
    pub async fn run_with_token(self, token: CancellationToken) -> Result<()> {
        let mut handles: Vec<JoinHandle<Result<()>>> = Vec::new();

        let (error_tx, error_rx) = mpsc::channel::<StreamSafeError>(self.buffer);

        let mut rx = self
            .stage
            .spawn(self.buffer, token.clone(), error_tx.clone(), &mut handles);

        let (tx_a, rx_a) = mpsc::channel::<Stg::Output>(self.buffer);
        let (tx_b, rx_b) = mpsc::channel::<Stg::Output>(self.buffer);

        let fanout_token = token.clone();
        handles.push(tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = fanout_token.cancelled() => return Ok(()),
                    item = rx.recv() => {
                        match item {
                            Some(input) => {
                                let cloned = input.clone();
                                // Concurrent sends: a slow/full channel doesn't
                                // block the sibling. A closed channel means that
                                // branch's sink task has exited — its error will
                                // surface via its own handle.
                                let _ = tokio::join!(tx_a.send(input), tx_b.send(cloned));
                            }
                            None => return Ok(()),
                        }
                    }
                }
            }
        }));

        spawn_sink_task(self.sink_a, rx_a, token.clone(), &mut handles);
        spawn_sink_task(self.sink_b, rx_b, token.clone(), &mut handles);
        spawn_error_sink(self.error_sink, error_rx, token.clone(), &mut handles);

        drop(error_tx);

        join_handles(handles).await
    }
}

/// Terminal pipeline that fans each item out to three sinks concurrently.
///
/// Constructed via [`PipelineBuilder::broadcast3`](crate::PipelineBuilder::broadcast3).
pub struct Broadcast3Pipeline<
    Stg: Stage,
    A: Sink<Input = Stg::Output>,
    B: Sink<Input = Stg::Output>,
    C: Sink<Input = Stg::Output>,
    E: Sink<Input = StreamSafeError> = DiscardErrors,
> where
    Stg::Output: Clone,
{
    pub(crate) stage: Stg,
    pub(crate) sink_a: A,
    pub(crate) sink_b: B,
    pub(crate) sink_c: C,
    pub(crate) error_sink: E,
    pub(crate) buffer: usize,
}

impl<Stg, A, B, C, E> SubPipeline for Broadcast3Pipeline<Stg, A, B, C, E>
where
    Stg: Stage,
    A: Sink<Input = Stg::Output>,
    B: Sink<Input = Stg::Output>,
    C: Sink<Input = Stg::Output>,
    Stg::Output: Clone,
    E: Sink<Input = StreamSafeError>,
{
    fn into_future(self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>> {
        Box::pin(self.run())
    }
}

impl<Stg, A, B, C, E> Broadcast3Pipeline<Stg, A, B, C, E>
where
    Stg: Stage,
    A: Sink<Input = Stg::Output>,
    B: Sink<Input = Stg::Output>,
    C: Sink<Input = Stg::Output>,
    Stg::Output: Clone,
    E: Sink<Input = StreamSafeError>,
{
    pub async fn run(self) -> Result<()> {
        self.run_with_token(ctrlc_token()).await
    }

    pub async fn run_with_token(self, token: CancellationToken) -> Result<()> {
        let mut handles: Vec<JoinHandle<Result<()>>> = Vec::new();
        let (error_tx, error_rx) = mpsc::channel::<StreamSafeError>(self.buffer);

        let mut rx = self
            .stage
            .spawn(self.buffer, token.clone(), error_tx.clone(), &mut handles);

        let (tx_a, rx_a) = mpsc::channel::<Stg::Output>(self.buffer);
        let (tx_b, rx_b) = mpsc::channel::<Stg::Output>(self.buffer);
        let (tx_c, rx_c) = mpsc::channel::<Stg::Output>(self.buffer);

        let fanout_token = token.clone();
        handles.push(tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = fanout_token.cancelled() => return Ok(()),
                    item = rx.recv() => {
                        match item {
                            Some(input) => {
                                let c1 = input.clone();
                                let c2 = input.clone();
                                let _ = tokio::join!(
                                    tx_a.send(input),
                                    tx_b.send(c1),
                                    tx_c.send(c2),
                                );
                            }
                            None => return Ok(()),
                        }
                    }
                }
            }
        }));

        spawn_sink_task(self.sink_a, rx_a, token.clone(), &mut handles);
        spawn_sink_task(self.sink_b, rx_b, token.clone(), &mut handles);
        spawn_sink_task(self.sink_c, rx_c, token.clone(), &mut handles);
        spawn_error_sink(self.error_sink, error_rx, token.clone(), &mut handles);

        drop(error_tx);

        join_handles(handles).await
    }
}

/// Spawn a task that drains `rx` into `sink`, then calls `sink.finish()` on
/// natural end-of-stream. `finish` is skipped on external cancellation.
pub(crate) fn spawn_sink_task<S>(
    mut sink: S,
    mut rx: mpsc::Receiver<S::Input>,
    token: CancellationToken,
    handles: &mut Vec<JoinHandle<Result<()>>>,
) where
    S: Sink,
{
    handles.push(tokio::spawn(async move {
        loop {
            tokio::select! {
                biased;
                _ = token.cancelled() => return Ok(()),
                item = rx.recv() => {
                    match item {
                        Some(input) => sink.consume(input).await?,
                        None => return sink.finish().await,
                    }
                }
            }
        }
    }));
}
