use super::AudioFrame;
use crate::error::{Result, StreamSafeError};
use crate::sink::Sink;
use std::path::PathBuf;

/// Writes `AudioFrame` data to a WAV file.
pub struct WavSink {
    writer: Option<hound::WavWriter<std::io::BufWriter<std::fs::File>>>,
    path: PathBuf,
}

impl WavSink {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            writer: None,
            path: path.into(),
        }
    }
}

impl Sink for WavSink {
    type Input = AudioFrame;

    async fn consume(&mut self, chunk: AudioFrame) -> Result<()> {
        if self.writer.is_none() {
            let spec = hound::WavSpec {
                channels: chunk.channels,
                sample_rate: chunk.sample_rate,
                bits_per_sample: 16,
                sample_format: hound::SampleFormat::Int,
            };
            self.writer = Some(
                hound::WavWriter::create(&self.path, spec).map_err(StreamSafeError::other)?,
            );
        }

        let writer = self.writer.as_mut().unwrap();
        for sample in chunk.data.chunks_exact(2) {
            let s = i16::from_le_bytes([sample[0], sample[1]]);
            writer.write_sample(s).map_err(StreamSafeError::other)?;
        }

        Ok(())
    }
}
