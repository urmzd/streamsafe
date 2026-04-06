use super::Frame;
use crate::error::{Result, StreamSafeError};
use crate::sink::Sink;
use std::path::PathBuf;

/// Writes video frames to segmented MP4 files using `muxide`.
/// Audio frames are silently skipped.
/// Splits on keyframe boundaries when `end_of_segment` is set.
pub struct Mp4SegmentSink {
    output_dir: PathBuf,
    filename_pattern: String,
    segment_index: u32,
    pending_split: bool,
    muxer: Option<muxide::api::Muxer<std::io::BufWriter<std::fs::File>>>,
    sps: Option<Vec<u8>>,
    pps: Option<Vec<u8>>,
    width: u32,
    height: u32,
    fps: f64,
}

impl Mp4SegmentSink {
    pub fn new(output_dir: impl Into<PathBuf>, pattern: &str) -> Self {
        Self {
            output_dir: output_dir.into(),
            filename_pattern: pattern.to_string(),
            segment_index: 0,
            pending_split: false,
            muxer: None,
            sps: None,
            pps: None,
            width: 1920,
            height: 1080,
            fps: 30.0,
        }
    }

    fn open_segment(&mut self) -> Result<()> {
        let filename = self
            .filename_pattern
            .replace("{}", &self.segment_index.to_string());
        let path = self.output_dir.join(filename);
        std::fs::create_dir_all(&self.output_dir).map_err(StreamSafeError::other)?;

        let file =
            std::io::BufWriter::new(std::fs::File::create(path).map_err(StreamSafeError::other)?);

        let muxer = muxide::api::MuxerBuilder::new(file)
            .video(
                muxide::api::VideoCodec::H264,
                self.width,
                self.height,
                self.fps,
            )
            .build()
            .map_err(|e| StreamSafeError::other(std::io::Error::other(format!("{e:?}"))))?;

        self.muxer = Some(muxer);
        tracing::info!(segment = self.segment_index, "opened new segment");
        Ok(())
    }

    fn close_segment(&mut self) -> Result<()> {
        if let Some(muxer) = self.muxer.take() {
            muxer
                .finish()
                .map_err(|e| StreamSafeError::other(std::io::Error::other(format!("{e:?}"))))?;
            tracing::info!(segment = self.segment_index, "closed segment");
            self.segment_index += 1;
        }
        Ok(())
    }
}

impl Sink for Mp4SegmentSink {
    type Input = Frame;

    async fn consume(&mut self, frame: Frame) -> Result<()> {
        let vf = match frame {
            Frame::Video(vf) => vf,
            Frame::Audio(_) => return Ok(()),
        };

        if let Some(params) = &vf.codec_params {
            self.sps = Some(params.sps.to_vec());
            self.pps = Some(params.pps.to_vec());
            self.width = params.width;
            self.height = params.height;
            self.fps = params.frame_rate;
        }

        if self.pending_split && vf.keyframe {
            self.close_segment()?;
            self.pending_split = false;
        }

        if self.muxer.is_none() {
            self.open_segment()?;
        }

        self.muxer
            .as_mut()
            .unwrap()
            .write_video(vf.meta.pts.as_secs_f64(), &vf.data, vf.keyframe)
            .map_err(|e| StreamSafeError::other(std::io::Error::other(format!("{e:?}"))))?;

        if vf.meta.end_of_segment {
            self.pending_split = true;
        }

        Ok(())
    }
}
