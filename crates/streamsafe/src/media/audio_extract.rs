use super::{AudioFrame, Frame};
use crate::error::{Result, StreamSafeError};
use crate::transform::Transform;

/// Filters a `Frame` stream to only `AudioFrame` variants.
/// Errors on video frames — use downstream of sources that yield audio,
/// or add a filter transform upstream.
pub struct AudioExtractor;

impl Transform for AudioExtractor {
    type Input = Frame;
    type Output = AudioFrame;

    async fn apply(&mut self, frame: Frame) -> Result<AudioFrame> {
        match frame {
            Frame::Audio(af) => Ok(af),
            Frame::Video(_) => Err(StreamSafeError::other(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "expected audio frame, got video",
            ))),
        }
    }
}
