use crate::error::Result;
use std::marker::PhantomData;

/// Like [`Transform`](crate::Transform), but may skip items by returning `Ok(None)`.
/// Use with [`.filter_pipe()`](crate::PipelineBuilder::filter_pipe) on `PipelineBuilder`.
pub trait FilterTransform: Send + 'static {
    type Input: Send + 'static;
    type Output: Send + 'static;

    fn apply(
        &mut self,
        input: Self::Input,
    ) -> impl std::future::Future<Output = Result<Option<Self::Output>>> + Send;
}

/// Wraps a `FnMut(I) -> Option<O>` as a sync [`FilterTransform`].
/// Construct with [`filter_map_fn`].
pub struct FilterMapFn<I, O, F> {
    f: F,
    _marker: PhantomData<fn(I) -> O>,
}

/// Create a [`FilterMapFn`] from a closure. Types are inferred from the pipeline.
///
/// ```ignore
/// builder.filter_pipe(filter_map_fn(|n: u32| if n % 2 == 0 { Some(n) } else { None }))
/// ```
pub fn filter_map_fn<I, O, F>(f: F) -> FilterMapFn<I, O, F>
where
    F: FnMut(I) -> Option<O> + Send + 'static,
    I: Send + 'static,
    O: Send + 'static,
{
    FilterMapFn {
        f,
        _marker: PhantomData,
    }
}

impl<I, O, F> FilterTransform for FilterMapFn<I, O, F>
where
    F: FnMut(I) -> Option<O> + Send + 'static,
    I: Send + 'static,
    O: Send + 'static,
{
    type Input = I;
    type Output = O;

    async fn apply(&mut self, input: I) -> Result<Option<O>> {
        Ok((self.f)(input))
    }
}
