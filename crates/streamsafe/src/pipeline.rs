use crate::error::{Result, StreamSafeError};
use crate::sink::Sink;
use crate::source::Source;
use crate::transform::Transform;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio_util::sync::CancellationToken;

/// Internal trait: a stage that can spawn itself as a tokio task
/// and return a Receiver for the next downstream stage.
// Sealed: users can't implement Stage, but it must be pub for generic bounds.
pub trait Stage: Send + 'static {
    type Output: Send + 'static;

    fn spawn(
        self,
        buffer: usize,
        token: CancellationToken,
        handles: &mut Vec<JoinHandle<Result<()>>>,
    ) -> mpsc::Receiver<Self::Output>;
}

/// Wraps a Source as a Stage.
pub struct SourceStage<S: Source> {
    source: S,
}

impl<S: Source> Stage for SourceStage<S> {
    type Output = S::Output;

    fn spawn(
        self,
        buffer: usize,
        token: CancellationToken,
        handles: &mut Vec<JoinHandle<Result<()>>>,
    ) -> mpsc::Receiver<Self::Output> {
        let (tx, rx) = mpsc::channel(buffer);
        let mut source = self.source;

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

        rx
    }
}

/// Wraps a Transform + its predecessor Stage.
pub struct TransformStage<Prev: Stage, T: Transform<Input = Prev::Output>> {
    prev: Prev,
    transform: T,
}

impl<Prev, T> Stage for TransformStage<Prev, T>
where
    Prev: Stage,
    T: Transform<Input = Prev::Output>,
{
    type Output = T::Output;

    fn spawn(
        self,
        buffer: usize,
        token: CancellationToken,
        handles: &mut Vec<JoinHandle<Result<()>>>,
    ) -> mpsc::Receiver<Self::Output> {
        let mut rx = self.prev.spawn(buffer, token.clone(), handles);
        let (tx, out_rx) = mpsc::channel(buffer);
        let mut transform = self.transform;

        handles.push(tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = token.cancelled() => return Ok(()),
                    item = rx.recv() => {
                        match item {
                            Some(input) => {
                                let output = transform.apply(input).await?;
                                if tx.send(output).await.is_err() {
                                    return Ok(());
                                }
                            }
                            None => return Ok(()),
                        }
                    }
                }
            }
        }));

        out_rx
    }
}

/// Builder: accumulates stages with type-safe chaining.
///
/// Each `.pipe()` call wraps the previous stage in a new generic type,
/// encoding the full type chain at compile time. Mismatched types between
/// stages produce compile errors.
pub struct PipelineBuilder<S: Stage> {
    stage: S,
    buffer: usize,
}

impl<S: Source> PipelineBuilder<SourceStage<S>> {
    /// Start a pipeline from a source.
    pub fn from(source: S) -> Self {
        PipelineBuilder {
            stage: SourceStage { source },
            buffer: 64,
        }
    }
}

impl<Stg: Stage> PipelineBuilder<Stg> {
    /// Chain a transform. Compile-time error if types don't match.
    pub fn pipe<T>(self, transform: T) -> PipelineBuilder<TransformStage<Stg, T>>
    where
        T: Transform<Input = Stg::Output>,
    {
        PipelineBuilder {
            stage: TransformStage {
                prev: self.stage,
                transform,
            },
            buffer: self.buffer,
        }
    }

    /// Set the channel buffer capacity for all inter-stage channels.
    pub fn buffer(mut self, size: usize) -> Self {
        self.buffer = size;
        self
    }

    /// Terminate the pipeline with a sink. Returns a runnable pipeline.
    pub fn into<K>(self, sink: K) -> RunnablePipeline<Stg, K>
    where
        K: Sink<Input = Stg::Output>,
    {
        RunnablePipeline {
            stage: self.stage,
            sink,
            buffer: self.buffer,
        }
    }
}

/// A fully-wired pipeline ready to execute.
pub struct RunnablePipeline<Stg: Stage, K: Sink<Input = Stg::Output>> {
    stage: Stg,
    sink: K,
    buffer: usize,
}

impl<Stg: Stage, K: Sink<Input = Stg::Output>> RunnablePipeline<Stg, K> {
    /// Run the pipeline with an internal ctrl-c shutdown handler.
    pub async fn run(self) -> Result<()> {
        let token = CancellationToken::new();
        let shutdown = token.clone();
        tokio::spawn(async move {
            let _ = tokio::signal::ctrl_c().await;
            shutdown.cancel();
        });
        self.run_with_token(token).await
    }

    /// Run the pipeline with an externally-provided cancellation token.
    pub async fn run_with_token(self, token: CancellationToken) -> Result<()> {
        let mut handles: Vec<JoinHandle<Result<()>>> = Vec::new();

        let mut rx = self.stage.spawn(self.buffer, token.clone(), &mut handles);

        let mut sink = self.sink;
        let sink_token = token.clone();
        handles.push(tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = sink_token.cancelled() => return Ok(()),
                    item = rx.recv() => {
                        match item {
                            Some(input) => sink.consume(input).await?,
                            None => return Ok(()),
                        }
                    }
                }
            }
        }));

        let mut first_error: Option<StreamSafeError> = None;
        for handle in handles {
            match handle.await {
                Ok(Ok(())) => {}
                Ok(Err(e)) => {
                    if first_error.is_none() {
                        first_error = Some(e);
                        token.cancel();
                    }
                }
                Err(join_err) => {
                    if first_error.is_none() {
                        first_error = Some(StreamSafeError::other(join_err));
                        token.cancel();
                    }
                }
            }
        }

        first_error.map_or(Ok(()), Err)
    }
}
