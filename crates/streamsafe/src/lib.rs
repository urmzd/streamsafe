//! # StreamSafe
//!
//! Type-safe async pipeline framework for data processing.
//!
//! Build pipelines with compile-time type checking:
//!
//! ```ignore
//! PipelineBuilder::from(source)
//!     .pipe(transform)
//!     .into(sink)
//!     .run()
//!     .await?;
//! ```

mod broadcast;
mod channel_source;
mod error;
mod error_sink;
mod fallible;
mod fanout;
mod filter_transform;
mod merge;
mod pipeline;
mod pipeline_sink;
mod runtime;
mod sink;
mod source;
mod transform;

#[cfg(feature = "media")]
pub mod media;

pub use broadcast::BroadcastSink;
pub use channel_source::ChannelSource;
pub use error::{Result, StreamSafeError};
pub use error_sink::DiscardErrors;
pub use fallible::FallibleTransform;
pub use fanout::{Broadcast3Pipeline, BroadcastPipeline};
pub use filter_transform::{filter_map_fn, FilterMapFn, FilterTransform};
pub use merge::{Merge3Stage, MergeStage};
pub use pipeline::{PipelineBuilder, RunnablePipeline};
pub use pipeline_sink::{PipelineSink, SubPipeline};
pub use sink::Sink;
pub use source::Source;
pub use transform::Transform;

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Arc, Mutex};
    use tokio_util::sync::CancellationToken;

    // -- Test helpers --

    struct Counter {
        current: u32,
        max: u32,
    }

    impl Counter {
        fn new(max: u32) -> Self {
            Self { current: 0, max }
        }
    }

    impl Source for Counter {
        type Output = u32;

        async fn produce(&mut self) -> Result<Option<u32>> {
            if self.current >= self.max {
                return Ok(None);
            }
            self.current += 1;
            Ok(Some(self.current))
        }
    }

    struct Double;

    impl Transform for Double {
        type Input = u32;
        type Output = u32;

        async fn apply(&mut self, n: u32) -> Result<u32> {
            Ok(n * 2)
        }
    }

    #[derive(Clone)]
    struct CollectSink<T: Send + 'static> {
        items: Arc<Mutex<Vec<T>>>,
    }

    impl<T: Send + 'static> CollectSink<T> {
        fn new() -> Self {
            Self {
                items: Arc::new(Mutex::new(Vec::new())),
            }
        }

        fn items(&self) -> Vec<T>
        where
            T: Clone,
        {
            self.items.lock().unwrap().clone()
        }
    }

    impl<T: Send + 'static> Sink for CollectSink<T> {
        type Input = T;

        async fn consume(&mut self, input: T) -> Result<()> {
            self.items.lock().unwrap().push(input);
            Ok(())
        }
    }

    // -- Tests --

    #[tokio::test]
    async fn test_basic_pipeline() {
        let sink = CollectSink::new();
        let sink_ref = sink.clone();

        PipelineBuilder::from(Counter::new(5))
            .pipe(Double)
            .into(sink)
            .run_with_token(CancellationToken::new())
            .await
            .unwrap();

        assert_eq!(sink_ref.items(), vec![2, 4, 6, 8, 10]);
    }

    #[tokio::test]
    async fn test_filter_transform() {
        struct EvenFilter;

        impl FilterTransform for EvenFilter {
            type Input = u32;
            type Output = u32;

            async fn apply(&mut self, n: u32) -> Result<Option<u32>> {
                Ok(if n % 2 == 0 { Some(n) } else { None })
            }
        }

        let sink = CollectSink::new();
        let sink_ref = sink.clone();

        PipelineBuilder::from(Counter::new(10))
            .filter_pipe(EvenFilter)
            .into(sink)
            .run_with_token(CancellationToken::new())
            .await
            .unwrap();

        assert_eq!(sink_ref.items(), vec![2, 4, 6, 8, 10]);
    }

    #[tokio::test]
    async fn test_filter_map_fn() {
        let sink = CollectSink::new();
        let sink_ref = sink.clone();

        PipelineBuilder::from(Counter::new(6))
            .filter_pipe(filter_map_fn(|n: u32| if n > 3 { Some(n) } else { None }))
            .into(sink)
            .run_with_token(CancellationToken::new())
            .await
            .unwrap();

        assert_eq!(sink_ref.items(), vec![4, 5, 6]);
    }

    #[tokio::test]
    async fn test_batch() {
        let sink = CollectSink::<Vec<u32>>::new();
        let sink_ref = sink.clone();

        PipelineBuilder::from(Counter::new(10))
            .batch(3)
            .into(sink)
            .run_with_token(CancellationToken::new())
            .await
            .unwrap();

        assert_eq!(
            sink_ref.items(),
            vec![vec![1, 2, 3], vec![4, 5, 6], vec![7, 8, 9], vec![10]]
        );
    }

    #[tokio::test]
    async fn test_batch_exact() {
        let sink = CollectSink::<Vec<u32>>::new();
        let sink_ref = sink.clone();

        PipelineBuilder::from(Counter::new(6))
            .batch(3)
            .into(sink)
            .run_with_token(CancellationToken::new())
            .await
            .unwrap();

        assert_eq!(sink_ref.items(), vec![vec![1, 2, 3], vec![4, 5, 6]]);
    }

    #[tokio::test]
    async fn test_broadcast() {
        let sink_a = CollectSink::new();
        let sink_b = CollectSink::new();
        let ref_a = sink_a.clone();
        let ref_b = sink_b.clone();

        PipelineBuilder::from(Counter::new(5))
            .broadcast(sink_a, sink_b)
            .run_with_token(CancellationToken::new())
            .await
            .unwrap();

        assert_eq!(ref_a.items(), vec![1, 2, 3, 4, 5]);
        assert_eq!(ref_b.items(), vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn test_broadcast_sinks_run_concurrently() {
        use std::time::{Duration, Instant};

        // Each sink sleeps per item. If they ran serially the total time
        // would be ~2x; running concurrently keeps it close to the slowest.
        struct SlowSink {
            delay: Duration,
            items: Arc<Mutex<Vec<u32>>>,
        }

        impl Sink for SlowSink {
            type Input = u32;

            async fn consume(&mut self, n: u32) -> Result<()> {
                tokio::time::sleep(self.delay).await;
                self.items.lock().unwrap().push(n);
                Ok(())
            }
        }

        let items_a = Arc::new(Mutex::new(Vec::new()));
        let items_b = Arc::new(Mutex::new(Vec::new()));
        let sink_a = SlowSink { delay: Duration::from_millis(20), items: items_a.clone() };
        let sink_b = SlowSink { delay: Duration::from_millis(20), items: items_b.clone() };

        let start = Instant::now();
        PipelineBuilder::from(Counter::new(5))
            .broadcast(sink_a, sink_b)
            .run_with_token(CancellationToken::new())
            .await
            .unwrap();
        let elapsed = start.elapsed();

        assert_eq!(items_a.lock().unwrap().len(), 5);
        assert_eq!(items_b.lock().unwrap().len(), 5);

        // Serial would be ~200ms (5*20 + 5*20). Concurrent is ~100ms (5*20).
        // Allow slack for scheduling; assert it's much closer to concurrent.
        assert!(
            elapsed < Duration::from_millis(180),
            "broadcast sinks appear serial: took {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn test_split_rejoin() {
        // Split: each side transforms independently; outputs merge back.
        // Left: n * 10. Right: n * 100. Merge order non-deterministic, so sort.
        struct TimesTen;
        impl Transform for TimesTen {
            type Input = u32;
            type Output = u32;
            async fn apply(&mut self, n: u32) -> Result<u32> { Ok(n * 10) }
        }

        struct TimesHundred;
        impl Transform for TimesHundred {
            type Input = u32;
            type Output = u32;
            async fn apply(&mut self, n: u32) -> Result<u32> { Ok(n * 100) }
        }

        let sink = CollectSink::<u32>::new();
        let sink_ref = sink.clone();

        PipelineBuilder::from(Counter::new(3))
            .split(
                |b| b.pipe(TimesTen),
                |b| b.pipe(TimesHundred),
            )
            .into(sink)
            .run_with_token(CancellationToken::new())
            .await
            .unwrap();

        let mut items = sink_ref.items();
        items.sort();
        assert_eq!(items, vec![10, 20, 30, 100, 200, 300]);
    }

    #[tokio::test]
    async fn test_split_continues_pipeline_after_rejoin() {
        struct TimesTen;
        impl Transform for TimesTen {
            type Input = u32;
            type Output = u32;
            async fn apply(&mut self, n: u32) -> Result<u32> { Ok(n * 10) }
        }

        struct PlusOne;
        impl Transform for PlusOne {
            type Input = u32;
            type Output = u32;
            async fn apply(&mut self, n: u32) -> Result<u32> { Ok(n + 1) }
        }

        let sink = CollectSink::<u32>::new();
        let sink_ref = sink.clone();

        // Both branches output n*10, merged, then every item gets +1.
        PipelineBuilder::from(Counter::new(2))
            .split(|b| b.pipe(TimesTen), |b| b.pipe(TimesTen))
            .pipe(PlusOne)
            .into(sink)
            .run_with_token(CancellationToken::new())
            .await
            .unwrap();

        let mut items = sink_ref.items();
        items.sort();
        // Each item appears twice (once per branch), each gets +1: 1→11×2, 2→21×2.
        assert_eq!(items, vec![11, 11, 21, 21]);
    }

    #[tokio::test]
    async fn test_merge_two_sources() {
        let sink = CollectSink::<u32>::new();
        let sink_ref = sink.clone();

        PipelineBuilder::merge(Counter::new(3), Counter::new(3))
            .into(sink)
            .run_with_token(CancellationToken::new())
            .await
            .unwrap();

        let mut items = sink_ref.items();
        items.sort();
        assert_eq!(items, vec![1, 1, 2, 2, 3, 3]);
    }

    #[tokio::test]
    async fn test_merge3_sources() {
        let sink = CollectSink::<u32>::new();
        let sink_ref = sink.clone();

        PipelineBuilder::merge3(Counter::new(2), Counter::new(2), Counter::new(2))
            .into(sink)
            .run_with_token(CancellationToken::new())
            .await
            .unwrap();

        let mut items = sink_ref.items();
        items.sort();
        assert_eq!(items, vec![1, 1, 1, 2, 2, 2]);
    }

    #[tokio::test]
    async fn test_broadcast3() {
        let a = CollectSink::new();
        let b = CollectSink::new();
        let c = CollectSink::new();
        let ra = a.clone();
        let rb = b.clone();
        let rc = c.clone();

        PipelineBuilder::from(Counter::new(3))
            .broadcast3(a, b, c)
            .run_with_token(CancellationToken::new())
            .await
            .unwrap();

        assert_eq!(ra.items(), vec![1, 2, 3]);
        assert_eq!(rb.items(), vec![1, 2, 3]);
        assert_eq!(rc.items(), vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn test_split3_rejoin() {
        struct Add(u32);
        impl Transform for Add {
            type Input = u32;
            type Output = u32;
            async fn apply(&mut self, n: u32) -> Result<u32> { Ok(n + self.0) }
        }

        let sink = CollectSink::<u32>::new();
        let sink_ref = sink.clone();

        PipelineBuilder::from(Counter::new(2))
            .split3(
                |b| b.pipe(Add(10)),
                |b| b.pipe(Add(20)),
                |b| b.pipe(Add(30)),
            )
            .into(sink)
            .run_with_token(CancellationToken::new())
            .await
            .unwrap();

        let mut items = sink_ref.items();
        items.sort();
        // For each n ∈ {1, 2}: n+10, n+20, n+30.
        assert_eq!(items, vec![11, 12, 21, 22, 31, 32]);
    }

    #[tokio::test]
    async fn test_pipeline_sink_as_broadcast_branch() {
        // Each branch has its own transform chain before the terminal sink —
        // the multi-format teasr use case.
        let gif_like = CollectSink::<u32>::new();
        let gif_ref = gif_like.clone();
        let png_like = CollectSink::<u32>::new();
        let png_ref = png_like.clone();

        PipelineBuilder::from(Counter::new(4))
            .broadcast(
                PipelineSink::new(move |b| b.pipe(Double).into(gif_like)),
                PipelineSink::new(move |b| {
                    b.filter_pipe(filter_map_fn(|n: u32| {
                        if n >= 2 { Some(n) } else { None }
                    }))
                    .into(png_like)
                }),
            )
            .run_with_token(CancellationToken::new())
            .await
            .unwrap();

        assert_eq!(gif_ref.items(), vec![2, 4, 6, 8]);
        assert_eq!(png_ref.items(), vec![2, 3, 4]);
    }

    #[tokio::test]
    async fn test_pipeline_sink_propagates_fatal_error() {
        struct BoomTransform;
        impl Transform for BoomTransform {
            type Input = u32;
            type Output = u32;
            async fn apply(&mut self, n: u32) -> Result<u32> {
                if n == 3 {
                    Err(StreamSafeError::Other("sub-boom".into()))
                } else {
                    Ok(n)
                }
            }
        }

        let sink = CollectSink::<u32>::new();

        let result = PipelineBuilder::from(Counter::new(5))
            .into(PipelineSink::new(|b| b.pipe(BoomTransform).into(sink)))
            .run_with_token(CancellationToken::new())
            .await;

        assert!(result.is_err(), "sub-pipeline error must surface via finish()");
    }

    #[tokio::test]
    async fn test_broadcast_one_sink_error_doesnt_kill_sibling() {
        struct FailsAtThree {
            items: Arc<Mutex<Vec<u32>>>,
        }

        impl Sink for FailsAtThree {
            type Input = u32;

            async fn consume(&mut self, n: u32) -> Result<()> {
                if n == 3 {
                    return Err(StreamSafeError::Other("boom".into()));
                }
                self.items.lock().unwrap().push(n);
                Ok(())
            }
        }

        let failing_items = Arc::new(Mutex::new(Vec::new()));
        let healthy = CollectSink::new();
        let healthy_ref = healthy.clone();

        let result = PipelineBuilder::from(Counter::new(5))
            .broadcast(
                FailsAtThree { items: failing_items.clone() },
                healthy,
            )
            .run_with_token(CancellationToken::new())
            .await;

        // Failing sink's error surfaces at the pipeline level...
        assert!(result.is_err(), "expected sub-sink error to surface");
        // ...but the healthy sink still received every item.
        assert_eq!(healthy_ref.items(), vec![1, 2, 3, 4, 5]);
    }

    #[tokio::test]
    async fn test_fallible_transform_routes_recoverable_errors() {
        struct OddFails;

        impl FallibleTransform for OddFails {
            type Input = u32;
            type Output = u32;

            async fn apply(
                &mut self,
                n: u32,
            ) -> Result<std::result::Result<u32, StreamSafeError>> {
                if n % 2 == 0 {
                    Ok(Ok(n))
                } else {
                    Ok(Err(StreamSafeError::Other(
                        format!("odd: {n}").into(),
                    )))
                }
            }
        }

        let main_sink = CollectSink::<u32>::new();
        let main_ref = main_sink.clone();
        let error_sink = CollectSink::<StreamSafeError>::new();
        let error_ref = error_sink.items.clone();

        PipelineBuilder::from(Counter::new(6))
            .on_errors(error_sink)
            .try_pipe(OddFails)
            .into(main_sink)
            .run_with_token(CancellationToken::new())
            .await
            .unwrap();

        assert_eq!(main_ref.items(), vec![2, 4, 6]);
        let errors = error_ref.lock().unwrap();
        assert_eq!(errors.len(), 3);
        assert!(errors.iter().all(|e| matches!(e, StreamSafeError::Other(_))));
    }

    #[tokio::test]
    async fn test_fallible_fatal_error_surfaces_without_cancelling_siblings() {
        // A fallible stage's outer Err is fatal to that stage, but other tasks
        // still run to their natural end (no cross-task cancellation).
        struct FatalAtThree;

        impl FallibleTransform for FatalAtThree {
            type Input = u32;
            type Output = u32;

            async fn apply(
                &mut self,
                n: u32,
            ) -> Result<std::result::Result<u32, StreamSafeError>> {
                if n == 3 {
                    Err(StreamSafeError::Other("fatal at 3".into()))
                } else {
                    Ok(Ok(n))
                }
            }
        }

        let sink = CollectSink::<u32>::new();

        let err = PipelineBuilder::from(Counter::new(10))
            .try_pipe(FatalAtThree)
            .into(sink)
            .run_with_token(CancellationToken::new())
            .await;

        assert!(err.is_err(), "expected fatal error to surface");
    }

    #[tokio::test]
    async fn test_default_error_sink_discards() {
        // No .on_errors() — errors should be silently discarded, main stream continues.
        struct EvensOnly;

        impl FallibleTransform for EvensOnly {
            type Input = u32;
            type Output = u32;

            async fn apply(
                &mut self,
                n: u32,
            ) -> Result<std::result::Result<u32, StreamSafeError>> {
                if n % 2 == 0 {
                    Ok(Ok(n))
                } else {
                    Ok(Err(StreamSafeError::Other("odd".into())))
                }
            }
        }

        let sink = CollectSink::new();
        let sink_ref = sink.clone();

        PipelineBuilder::from(Counter::new(5))
            .try_pipe(EvensOnly)
            .into(sink)
            .run_with_token(CancellationToken::new())
            .await
            .unwrap();

        assert_eq!(sink_ref.items(), vec![2, 4]);
    }

    #[tokio::test]
    async fn test_filter_then_batch() {
        let sink = CollectSink::<Vec<u32>>::new();
        let sink_ref = sink.clone();

        PipelineBuilder::from(Counter::new(10))
            .filter_pipe(filter_map_fn(
                |n: u32| {
                    if n % 2 == 0 {
                        Some(n)
                    } else {
                        None
                    }
                },
            ))
            .batch(2)
            .into(sink)
            .run_with_token(CancellationToken::new())
            .await
            .unwrap();

        assert_eq!(sink_ref.items(), vec![vec![2, 4], vec![6, 8], vec![10]]);
    }
}
