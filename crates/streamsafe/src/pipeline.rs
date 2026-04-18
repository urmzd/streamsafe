use crate::error::{Result, StreamSafeError};
use crate::error_sink::DiscardErrors;
use crate::fallible::FallibleTransform;
use crate::fanout::{spawn_sink_task, Broadcast3Pipeline, BroadcastPipeline};
use crate::filter_transform::FilterTransform;
use crate::merge::{Merge3Stage, MergeStage};
use crate::pipeline_sink::SubPipeline;
use crate::runtime::{ctrlc_token, join_handles, spawn_error_sink};
use crate::sink::Sink;
use crate::source::Source;
use crate::transform::Transform;
use std::future::Future;
use std::pin::Pin;
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
        error_tx: mpsc::Sender<StreamSafeError>,
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
        _error_tx: mpsc::Sender<StreamSafeError>,
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
        error_tx: mpsc::Sender<StreamSafeError>,
        handles: &mut Vec<JoinHandle<Result<()>>>,
    ) -> mpsc::Receiver<Self::Output> {
        let mut rx = self.prev.spawn(buffer, token.clone(), error_tx, handles);
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

/// Wraps a FilterTransform + its predecessor Stage. Items returning None are skipped.
pub struct FilterMapStage<Prev: Stage, T: FilterTransform<Input = Prev::Output>> {
    prev: Prev,
    transform: T,
}

impl<Prev, T> Stage for FilterMapStage<Prev, T>
where
    Prev: Stage,
    T: FilterTransform<Input = Prev::Output>,
{
    type Output = T::Output;

    fn spawn(
        self,
        buffer: usize,
        token: CancellationToken,
        error_tx: mpsc::Sender<StreamSafeError>,
        handles: &mut Vec<JoinHandle<Result<()>>>,
    ) -> mpsc::Receiver<Self::Output> {
        let mut rx = self.prev.spawn(buffer, token.clone(), error_tx, handles);
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
                                if let Some(output) = transform.apply(input).await? {
                                    if tx.send(output).await.is_err() {
                                        return Ok(());
                                    }
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

/// Wraps a FallibleTransform + its predecessor Stage. Per-item recoverable errors
/// are forwarded to the pipeline's error rail; the main stream continues.
pub struct FallibleTransformStage<Prev: Stage, T: FallibleTransform<Input = Prev::Output>> {
    prev: Prev,
    transform: T,
}

impl<Prev, T> Stage for FallibleTransformStage<Prev, T>
where
    Prev: Stage,
    T: FallibleTransform<Input = Prev::Output>,
{
    type Output = T::Output;

    fn spawn(
        self,
        buffer: usize,
        token: CancellationToken,
        error_tx: mpsc::Sender<StreamSafeError>,
        handles: &mut Vec<JoinHandle<Result<()>>>,
    ) -> mpsc::Receiver<Self::Output> {
        let mut rx = self
            .prev
            .spawn(buffer, token.clone(), error_tx.clone(), handles);
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
                                match transform.apply(input).await? {
                                    Ok(output) => {
                                        if tx.send(output).await.is_err() {
                                            return Ok(());
                                        }
                                    }
                                    Err(recoverable) => {
                                        // Error rail is best-effort: if the rail
                                        // sink has exited, drop silently.
                                        let _ = error_tx.send(recoverable).await;
                                    }
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

/// Accumulates items into batches of a fixed size, emitting `Vec<T>`.
/// The final batch may be smaller (flushed on stream end).
pub struct BatchStage<Prev: Stage> {
    prev: Prev,
    size: usize,
}

impl<Prev: Stage> Stage for BatchStage<Prev> {
    type Output = Vec<Prev::Output>;

    fn spawn(
        self,
        buffer: usize,
        token: CancellationToken,
        error_tx: mpsc::Sender<StreamSafeError>,
        handles: &mut Vec<JoinHandle<Result<()>>>,
    ) -> mpsc::Receiver<Self::Output> {
        let mut rx = self.prev.spawn(buffer, token.clone(), error_tx, handles);
        let (tx, out_rx) = mpsc::channel(buffer);
        let batch_size = self.size;

        handles.push(tokio::spawn(async move {
            let mut batch = Vec::with_capacity(batch_size);
            loop {
                tokio::select! {
                    biased;
                    _ = token.cancelled() => return Ok(()),
                    item = rx.recv() => {
                        match item {
                            Some(input) => {
                                batch.push(input);
                                if batch.len() >= batch_size {
                                    let full_batch = std::mem::replace(
                                        &mut batch,
                                        Vec::with_capacity(batch_size),
                                    );
                                    if tx.send(full_batch).await.is_err() {
                                        return Ok(());
                                    }
                                }
                            }
                            None => {
                                if !batch.is_empty() {
                                    let _ = tx.send(batch).await;
                                }
                                return Ok(());
                            }
                        }
                    }
                }
            }
        }));

        out_rx
    }
}

/// Mid-pipeline fan-out with rejoin. Items from `prev` are cloned into two
/// sub-chains built at spawn time by the `left`/`right` closures. Both
/// sub-chains must produce the same output type; their outputs are merged
/// into a single stream (merge order non-deterministic).
pub struct SplitStage<Prev, LF, RF, L, R>
where
    Prev: Stage,
    Prev::Output: Clone,
    LF: FnOnce(
            PipelineBuilder<SourceStage<crate::channel_source::ChannelSource<Prev::Output>>>,
        ) -> PipelineBuilder<L>
        + Send
        + 'static,
    RF: FnOnce(
            PipelineBuilder<SourceStage<crate::channel_source::ChannelSource<Prev::Output>>>,
        ) -> PipelineBuilder<R>
        + Send
        + 'static,
    L: Stage,
    R: Stage<Output = L::Output>,
{
    prev: Prev,
    left: LF,
    right: RF,
    _phantom: std::marker::PhantomData<fn() -> (L, R)>,
}

impl<Prev, LF, RF, L, R> Stage for SplitStage<Prev, LF, RF, L, R>
where
    Prev: Stage,
    Prev::Output: Clone,
    LF: FnOnce(
            PipelineBuilder<SourceStage<crate::channel_source::ChannelSource<Prev::Output>>>,
        ) -> PipelineBuilder<L>
        + Send
        + 'static,
    RF: FnOnce(
            PipelineBuilder<SourceStage<crate::channel_source::ChannelSource<Prev::Output>>>,
        ) -> PipelineBuilder<R>
        + Send
        + 'static,
    L: Stage,
    R: Stage<Output = L::Output>,
{
    type Output = L::Output;

    fn spawn(
        self,
        buffer: usize,
        token: CancellationToken,
        error_tx: mpsc::Sender<StreamSafeError>,
        handles: &mut Vec<JoinHandle<Result<()>>>,
    ) -> mpsc::Receiver<Self::Output> {
        let mut upstream_rx = self
            .prev
            .spawn(buffer, token.clone(), error_tx.clone(), handles);

        let (tx_l, rx_l) = mpsc::channel::<Prev::Output>(buffer);
        let (tx_r, rx_r) = mpsc::channel::<Prev::Output>(buffer);

        let fanout_token = token.clone();
        handles.push(tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = fanout_token.cancelled() => return Ok(()),
                    item = upstream_rx.recv() => {
                        match item {
                            Some(input) => {
                                let cloned = input.clone();
                                let _ = tokio::join!(tx_l.send(input), tx_r.send(cloned));
                            }
                            None => return Ok(()),
                        }
                    }
                }
            }
        }));

        // Build each branch at spawn-time from a ChannelSource seeded with its
        // fan-out receiver, then spawn the resulting Stage on the outer runtime.
        let left_builder = PipelineBuilder::from(crate::channel_source::ChannelSource::new(rx_l));
        let left_stage = (self.left)(left_builder).into_stage();
        let mut left_out = left_stage.spawn(buffer, token.clone(), error_tx.clone(), handles);

        let right_builder = PipelineBuilder::from(crate::channel_source::ChannelSource::new(rx_r));
        let right_stage = (self.right)(right_builder).into_stage();
        let mut right_out = right_stage.spawn(buffer, token.clone(), error_tx, handles);

        // Rejoin: merge both branch outputs into one channel.
        let (tx_out, rx_out) = mpsc::channel::<Self::Output>(buffer);
        let rejoin_token = token.clone();
        handles.push(tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = rejoin_token.cancelled() => return Ok(()),
                    item = left_out.recv() => {
                        match item {
                            Some(v) => {
                                if tx_out.send(v).await.is_err() {
                                    return Ok(());
                                }
                            }
                            None => break,
                        }
                    }
                    item = right_out.recv() => {
                        match item {
                            Some(v) => {
                                if tx_out.send(v).await.is_err() {
                                    return Ok(());
                                }
                            }
                            None => break,
                        }
                    }
                }
            }
            // One side exhausted; drain the other.
            loop {
                tokio::select! {
                    biased;
                    _ = rejoin_token.cancelled() => return Ok(()),
                    item = left_out.recv() => match item {
                        Some(v) => {
                            if tx_out.send(v).await.is_err() {
                                return Ok(());
                            }
                        }
                        None => break,
                    }
                }
            }
            loop {
                tokio::select! {
                    biased;
                    _ = rejoin_token.cancelled() => return Ok(()),
                    item = right_out.recv() => match item {
                        Some(v) => {
                            if tx_out.send(v).await.is_err() {
                                return Ok(());
                            }
                        }
                        None => break,
                    }
                }
            }
            Ok(())
        }));

        rx_out
    }
}

/// Three-way variant of [`SplitStage`]. See [`PipelineBuilder::split3`].
pub struct Split3Stage<Prev, LF, MF, RF, L, M, R>
where
    Prev: Stage,
    Prev::Output: Clone,
    LF: FnOnce(
            PipelineBuilder<SourceStage<crate::channel_source::ChannelSource<Prev::Output>>>,
        ) -> PipelineBuilder<L>
        + Send
        + 'static,
    MF: FnOnce(
            PipelineBuilder<SourceStage<crate::channel_source::ChannelSource<Prev::Output>>>,
        ) -> PipelineBuilder<M>
        + Send
        + 'static,
    RF: FnOnce(
            PipelineBuilder<SourceStage<crate::channel_source::ChannelSource<Prev::Output>>>,
        ) -> PipelineBuilder<R>
        + Send
        + 'static,
    L: Stage,
    M: Stage<Output = L::Output>,
    R: Stage<Output = L::Output>,
{
    prev: Prev,
    left: LF,
    middle: MF,
    right: RF,
    #[allow(clippy::type_complexity)]
    _phantom: std::marker::PhantomData<fn() -> (L, M, R)>,
}

impl<Prev, LF, MF, RF, L, M, R> Stage for Split3Stage<Prev, LF, MF, RF, L, M, R>
where
    Prev: Stage,
    Prev::Output: Clone,
    LF: FnOnce(
            PipelineBuilder<SourceStage<crate::channel_source::ChannelSource<Prev::Output>>>,
        ) -> PipelineBuilder<L>
        + Send
        + 'static,
    MF: FnOnce(
            PipelineBuilder<SourceStage<crate::channel_source::ChannelSource<Prev::Output>>>,
        ) -> PipelineBuilder<M>
        + Send
        + 'static,
    RF: FnOnce(
            PipelineBuilder<SourceStage<crate::channel_source::ChannelSource<Prev::Output>>>,
        ) -> PipelineBuilder<R>
        + Send
        + 'static,
    L: Stage,
    M: Stage<Output = L::Output>,
    R: Stage<Output = L::Output>,
{
    type Output = L::Output;

    fn spawn(
        self,
        buffer: usize,
        token: CancellationToken,
        error_tx: mpsc::Sender<StreamSafeError>,
        handles: &mut Vec<JoinHandle<Result<()>>>,
    ) -> mpsc::Receiver<Self::Output> {
        let mut upstream_rx = self
            .prev
            .spawn(buffer, token.clone(), error_tx.clone(), handles);

        let (tx_l, rx_l) = mpsc::channel::<Prev::Output>(buffer);
        let (tx_m, rx_m) = mpsc::channel::<Prev::Output>(buffer);
        let (tx_r, rx_r) = mpsc::channel::<Prev::Output>(buffer);

        let fanout_token = token.clone();
        handles.push(tokio::spawn(async move {
            loop {
                tokio::select! {
                    biased;
                    _ = fanout_token.cancelled() => return Ok(()),
                    item = upstream_rx.recv() => {
                        match item {
                            Some(input) => {
                                let c1 = input.clone();
                                let c2 = input.clone();
                                let _ = tokio::join!(
                                    tx_l.send(input),
                                    tx_m.send(c1),
                                    tx_r.send(c2),
                                );
                            }
                            None => return Ok(()),
                        }
                    }
                }
            }
        }));

        let l_builder = PipelineBuilder::from(crate::channel_source::ChannelSource::new(rx_l));
        let l_stage = (self.left)(l_builder).into_stage();
        let mut l_out = l_stage.spawn(buffer, token.clone(), error_tx.clone(), handles);

        let m_builder = PipelineBuilder::from(crate::channel_source::ChannelSource::new(rx_m));
        let m_stage = (self.middle)(m_builder).into_stage();
        let mut m_out = m_stage.spawn(buffer, token.clone(), error_tx.clone(), handles);

        let r_builder = PipelineBuilder::from(crate::channel_source::ChannelSource::new(rx_r));
        let r_stage = (self.right)(r_builder).into_stage();
        let mut r_out = r_stage.spawn(buffer, token.clone(), error_tx, handles);

        let (tx_out, rx_out) = mpsc::channel::<Self::Output>(buffer);
        let rejoin_token = token.clone();
        handles.push(tokio::spawn(async move {
            let mut done_l = false;
            let mut done_m = false;
            let mut done_r = false;

            while !(done_l && done_m && done_r) {
                tokio::select! {
                    biased;
                    _ = rejoin_token.cancelled() => return Ok(()),
                    item = l_out.recv(), if !done_l => {
                        match item {
                            Some(v) => {
                                if tx_out.send(v).await.is_err() {
                                    return Ok(());
                                }
                            }
                            None => done_l = true,
                        }
                    }
                    item = m_out.recv(), if !done_m => {
                        match item {
                            Some(v) => {
                                if tx_out.send(v).await.is_err() {
                                    return Ok(());
                                }
                            }
                            None => done_m = true,
                        }
                    }
                    item = r_out.recv(), if !done_r => {
                        match item {
                            Some(v) => {
                                if tx_out.send(v).await.is_err() {
                                    return Ok(());
                                }
                            }
                            None => done_r = true,
                        }
                    }
                }
            }
            Ok(())
        }));

        rx_out
    }
}

/// Builder: accumulates stages with type-safe chaining.
///
/// Each `.pipe()` call wraps the previous stage in a new generic type,
/// encoding the full type chain at compile time. Mismatched types between
/// stages produce compile errors.
///
/// The second generic parameter is the error-rail sink. Defaults to
/// [`DiscardErrors`]; override with [`.on_errors()`](Self::on_errors) to
/// route recoverable per-item errors from [`FallibleTransform`] stages.
pub struct PipelineBuilder<S: Stage, E: Sink<Input = StreamSafeError> = DiscardErrors> {
    stage: S,
    error_sink: E,
    buffer: usize,
}

impl<S: Source> PipelineBuilder<SourceStage<S>, DiscardErrors> {
    /// Start a pipeline from a source. The error rail defaults to [`DiscardErrors`].
    pub fn from(source: S) -> Self {
        PipelineBuilder {
            stage: SourceStage { source },
            error_sink: DiscardErrors,
            buffer: 64,
        }
    }
}

impl<S1, S2> PipelineBuilder<MergeStage<S1, S2>, DiscardErrors>
where
    S1: Source,
    S2: Source<Output = S1::Output>,
{
    /// Start a pipeline from two merged sources. Items from each source are
    /// interleaved into a single stream; merge order is non-deterministic.
    pub fn merge(a: S1, b: S2) -> Self {
        PipelineBuilder {
            stage: MergeStage { s1: a, s2: b },
            error_sink: DiscardErrors,
            buffer: 64,
        }
    }
}

impl<S1, S2, S3> PipelineBuilder<Merge3Stage<S1, S2, S3>, DiscardErrors>
where
    S1: Source,
    S2: Source<Output = S1::Output>,
    S3: Source<Output = S1::Output>,
{
    /// Three-way variant of [`merge`](Self::merge).
    pub fn merge3(a: S1, b: S2, c: S3) -> Self {
        PipelineBuilder {
            stage: Merge3Stage {
                s1: a,
                s2: b,
                s3: c,
            },
            error_sink: DiscardErrors,
            buffer: 64,
        }
    }
}

impl<Stg: Stage, E: Sink<Input = StreamSafeError>> PipelineBuilder<Stg, E> {
    /// Chain a transform. Compile-time error if types don't match.
    pub fn pipe<T>(self, transform: T) -> PipelineBuilder<TransformStage<Stg, T>, E>
    where
        T: Transform<Input = Stg::Output>,
    {
        PipelineBuilder {
            stage: TransformStage {
                prev: self.stage,
                transform,
            },
            error_sink: self.error_sink,
            buffer: self.buffer,
        }
    }

    /// Chain a filter-transform. Items returning `None` are silently dropped.
    /// Compile-time error if types don't match.
    pub fn filter_pipe<T>(self, transform: T) -> PipelineBuilder<FilterMapStage<Stg, T>, E>
    where
        T: FilterTransform<Input = Stg::Output>,
    {
        PipelineBuilder {
            stage: FilterMapStage {
                prev: self.stage,
                transform,
            },
            error_sink: self.error_sink,
            buffer: self.buffer,
        }
    }

    /// Chain a fallible transform. Recoverable errors (inner `Err`) are routed to the
    /// error rail; the main stream continues. Fatal errors (outer `Err`) abort the stage.
    pub fn try_pipe<T>(self, transform: T) -> PipelineBuilder<FallibleTransformStage<Stg, T>, E>
    where
        T: FallibleTransform<Input = Stg::Output>,
    {
        PipelineBuilder {
            stage: FallibleTransformStage {
                prev: self.stage,
                transform,
            },
            error_sink: self.error_sink,
            buffer: self.buffer,
        }
    }

    /// Accumulate items into fixed-size batches. The output type becomes `Vec<T>`.
    /// The final batch may be smaller than `size` (flushed on stream end).
    pub fn batch(self, size: usize) -> PipelineBuilder<BatchStage<Stg>, E> {
        PipelineBuilder {
            stage: BatchStage {
                prev: self.stage,
                size,
            },
            error_sink: self.error_sink,
            buffer: self.buffer,
        }
    }

    /// Set the channel buffer capacity for all inter-stage channels.
    pub fn buffer(mut self, size: usize) -> Self {
        self.buffer = size;
        self
    }

    /// Extract the inner stage. Used by split/merge adapters to compose
    /// sub-builders into larger stages.
    pub fn into_stage(self) -> Stg {
        self.stage
    }

    /// Mid-pipeline fan-out: items flow into two sub-chains (cloned), both must
    /// produce the same output type, and their outputs merge back into a single
    /// stream for the continuation.
    ///
    /// Merge order is non-deterministic (whichever branch yields first). Each
    /// branch runs on its own task; a slow branch only backpressures itself up
    /// to the configured buffer depth. Fallible stages inside the branches
    /// emit to the outer pipeline's error rail. The error sink configured
    /// *inside* a sub-builder is ignored — configure it on the outer builder.
    pub fn split<LF, RF, L, R>(
        self,
        left: LF,
        right: RF,
    ) -> PipelineBuilder<SplitStage<Stg, LF, RF, L, R>, E>
    where
        Stg::Output: Clone,
        LF: FnOnce(
                PipelineBuilder<SourceStage<crate::channel_source::ChannelSource<Stg::Output>>>,
            ) -> PipelineBuilder<L>
            + Send
            + 'static,
        RF: FnOnce(
                PipelineBuilder<SourceStage<crate::channel_source::ChannelSource<Stg::Output>>>,
            ) -> PipelineBuilder<R>
            + Send
            + 'static,
        L: Stage,
        R: Stage<Output = L::Output>,
    {
        PipelineBuilder {
            stage: SplitStage {
                prev: self.stage,
                left,
                right,
                _phantom: std::marker::PhantomData,
            },
            error_sink: self.error_sink,
            buffer: self.buffer,
        }
    }

    /// Install an error-rail sink. Recoverable errors emitted by any
    /// [`FallibleTransform`] in this pipeline are routed to `sink`.
    pub fn on_errors<E2>(self, sink: E2) -> PipelineBuilder<Stg, E2>
    where
        E2: Sink<Input = StreamSafeError>,
    {
        PipelineBuilder {
            stage: self.stage,
            error_sink: sink,
            buffer: self.buffer,
        }
    }

    /// Terminate the pipeline by broadcasting each item to two sinks. Each sink
    /// runs on its own task with its own bounded channel — a slow sink only
    /// backpressures itself, not its sibling.
    ///
    /// A sub-sink returning an error ends only that branch; the other continues.
    /// Errors are aggregated and the first surfaces as the pipeline's return value.
    ///
    /// Input must implement `Clone`.
    pub fn broadcast<A, B>(self, a: A, b: B) -> BroadcastPipeline<Stg, A, B, E>
    where
        A: Sink<Input = Stg::Output>,
        B: Sink<Input = Stg::Output>,
        Stg::Output: Clone,
    {
        BroadcastPipeline {
            stage: self.stage,
            sink_a: a,
            sink_b: b,
            error_sink: self.error_sink,
            buffer: self.buffer,
        }
    }

    /// Three-way variant of [`broadcast`](Self::broadcast).
    pub fn broadcast3<A, B, C>(self, a: A, b: B, c: C) -> Broadcast3Pipeline<Stg, A, B, C, E>
    where
        A: Sink<Input = Stg::Output>,
        B: Sink<Input = Stg::Output>,
        C: Sink<Input = Stg::Output>,
        Stg::Output: Clone,
    {
        Broadcast3Pipeline {
            stage: self.stage,
            sink_a: a,
            sink_b: b,
            sink_c: c,
            error_sink: self.error_sink,
            buffer: self.buffer,
        }
    }

    /// Three-way variant of [`split`](Self::split). All three sub-chains must
    /// produce the same output type; outputs merge into a single stream.
    #[allow(clippy::type_complexity)]
    pub fn split3<LF, MF, RF, L, M, R>(
        self,
        left: LF,
        middle: MF,
        right: RF,
    ) -> PipelineBuilder<Split3Stage<Stg, LF, MF, RF, L, M, R>, E>
    where
        Stg::Output: Clone,
        LF: FnOnce(
                PipelineBuilder<SourceStage<crate::channel_source::ChannelSource<Stg::Output>>>,
            ) -> PipelineBuilder<L>
            + Send
            + 'static,
        MF: FnOnce(
                PipelineBuilder<SourceStage<crate::channel_source::ChannelSource<Stg::Output>>>,
            ) -> PipelineBuilder<M>
            + Send
            + 'static,
        RF: FnOnce(
                PipelineBuilder<SourceStage<crate::channel_source::ChannelSource<Stg::Output>>>,
            ) -> PipelineBuilder<R>
            + Send
            + 'static,
        L: Stage,
        M: Stage<Output = L::Output>,
        R: Stage<Output = L::Output>,
    {
        PipelineBuilder {
            stage: Split3Stage {
                prev: self.stage,
                left,
                middle,
                right,
                _phantom: std::marker::PhantomData,
            },
            error_sink: self.error_sink,
            buffer: self.buffer,
        }
    }

    /// Terminate the pipeline with a sink. Returns a runnable pipeline.
    pub fn into<K>(self, sink: K) -> RunnablePipeline<Stg, K, E>
    where
        K: Sink<Input = Stg::Output>,
    {
        RunnablePipeline {
            stage: self.stage,
            sink,
            error_sink: self.error_sink,
            buffer: self.buffer,
        }
    }
}

/// A fully-wired pipeline ready to execute.
pub struct RunnablePipeline<
    Stg: Stage,
    K: Sink<Input = Stg::Output>,
    E: Sink<Input = StreamSafeError> = DiscardErrors,
> {
    stage: Stg,
    sink: K,
    error_sink: E,
    buffer: usize,
}

impl<Stg, K, E> SubPipeline for RunnablePipeline<Stg, K, E>
where
    Stg: Stage,
    K: Sink<Input = Stg::Output>,
    E: Sink<Input = StreamSafeError>,
{
    fn into_future(self) -> Pin<Box<dyn Future<Output = Result<()>> + Send + 'static>> {
        Box::pin(self.run())
    }
}

impl<Stg, K, E> RunnablePipeline<Stg, K, E>
where
    Stg: Stage,
    K: Sink<Input = Stg::Output>,
    E: Sink<Input = StreamSafeError>,
{
    /// Run the pipeline with an internal ctrl-c shutdown handler.
    pub async fn run(self) -> Result<()> {
        self.run_with_token(ctrlc_token()).await
    }

    /// Run the pipeline with an externally-provided cancellation token.
    ///
    /// A failure in any task does not cancel sibling tasks — they run to their
    /// natural end, and the first fatal error surfaces as the return value.
    /// Only external cancellation (via `token`) aborts the pipeline early.
    pub async fn run_with_token(self, token: CancellationToken) -> Result<()> {
        let mut handles: Vec<JoinHandle<Result<()>>> = Vec::new();

        let (error_tx, error_rx) = mpsc::channel::<StreamSafeError>(self.buffer);

        let rx = self
            .stage
            .spawn(self.buffer, token.clone(), error_tx.clone(), &mut handles);

        spawn_sink_task(self.sink, rx, token.clone(), &mut handles);
        spawn_error_sink(self.error_sink, error_rx, token.clone(), &mut handles);

        // Drop our copy so the rail closes once all stages release their senders.
        drop(error_tx);

        join_handles(handles).await
    }
}
