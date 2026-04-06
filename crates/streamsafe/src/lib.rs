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

mod error;
mod pipeline;
mod sink;
mod source;
mod transform;

#[cfg(feature = "media")]
pub mod media;

pub use error::{Result, StreamSafeError};
pub use pipeline::{PipelineBuilder, RunnablePipeline};
pub use sink::Sink;
pub use source::Source;
pub use transform::Transform;
