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
mod error;
mod filter_transform;
mod pipeline;
mod sink;
mod source;
mod transform;

#[cfg(feature = "media")]
pub mod media;

pub use broadcast::BroadcastSink;
pub use error::{Result, StreamSafeError};
pub use filter_transform::{filter_map_fn, FilterMapFn, FilterTransform};
pub use pipeline::{PipelineBuilder, RunnablePipeline};
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
            .filter_pipe(filter_map_fn(|n: u32| {
                if n > 3 {
                    Some(n)
                } else {
                    None
                }
            }))
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
    async fn test_filter_then_batch() {
        let sink = CollectSink::<Vec<u32>>::new();
        let sink_ref = sink.clone();

        PipelineBuilder::from(Counter::new(10))
            .filter_pipe(filter_map_fn(|n: u32| {
                if n % 2 == 0 { Some(n) } else { None }
            }))
            .batch(2)
            .into(sink)
            .run_with_token(CancellationToken::new())
            .await
            .unwrap();

        assert_eq!(
            sink_ref.items(),
            vec![vec![2, 4], vec![6, 8], vec![10]]
        );
    }
}
