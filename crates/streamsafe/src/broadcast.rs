use crate::error::Result;
use crate::sink::Sink;

/// Sends each item to two sinks sequentially. Input must be `Clone`.
///
/// For 3+ sinks, nest: `BroadcastSink::new(a, BroadcastSink::new(b, c))`.
pub struct BroadcastSink<A, B> {
    a: A,
    b: B,
}

impl<A, B> BroadcastSink<A, B> {
    pub fn new(a: A, b: B) -> Self {
        Self { a, b }
    }
}

impl<A, B> Sink for BroadcastSink<A, B>
where
    A: Sink,
    B: Sink<Input = A::Input>,
    A::Input: Clone,
{
    type Input = A::Input;

    async fn consume(&mut self, input: Self::Input) -> Result<()> {
        let cloned = input.clone();
        self.a.consume(input).await?;
        self.b.consume(cloned).await?;
        Ok(())
    }
}
