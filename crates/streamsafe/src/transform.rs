use crate::error::Result;

/// Maps one item to another. Compile-time type checking ensures that
/// adjacent stages in a pipeline have matching Input/Output types.
pub trait Transform: Send + 'static {
    type Input: Send + 'static;
    type Output: Send + 'static;

    fn apply(
        &mut self,
        input: Self::Input,
    ) -> impl std::future::Future<Output = Result<Self::Output>> + Send;
}
