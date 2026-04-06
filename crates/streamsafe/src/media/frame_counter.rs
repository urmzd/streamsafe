use super::Frame;
use crate::error::Result;
use crate::transform::Transform;

/// Counts video frames and marks every Nth with `end_of_segment = true`.
/// Audio frames pass through unchanged.
pub struct FrameCountSplitter {
    threshold: u32,
    counter: u32,
}

impl FrameCountSplitter {
    pub fn new(threshold: u32) -> Self {
        Self {
            threshold,
            counter: 0,
        }
    }
}

impl Transform for FrameCountSplitter {
    type Input = Frame;
    type Output = Frame;

    async fn apply(&mut self, mut frame: Frame) -> Result<Frame> {
        if matches!(frame, Frame::Video(_)) {
            self.counter += 1;
            if self.counter >= self.threshold {
                frame.meta_mut().end_of_segment = true;
                self.counter = 0;
            }
        }
        Ok(frame)
    }
}
