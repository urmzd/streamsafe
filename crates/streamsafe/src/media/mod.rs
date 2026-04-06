use bytes::Bytes;
use std::time::Duration;

pub mod audio_extract;
pub mod file_source;
pub mod frame_counter;
pub mod rtsp_source;
pub mod segment_sink;
pub mod wav_sink;

pub use audio_extract::AudioExtractor;
pub use file_source::FileSource;
pub use frame_counter::FrameCountSplitter;
pub use rtsp_source::RtspSource;
pub use segment_sink::Mp4SegmentSink;
pub use wav_sink::WavSink;

/// Metadata common to all frame variants.
#[derive(Debug, Clone)]
pub struct FrameMeta {
    /// Presentation timestamp relative to stream start.
    pub pts: Duration,
    /// Source URI: `"file:///path/to/video.mp4"` or `"rtsp://host:554/stream"`.
    pub source_uri: String,
    /// Absolute frame/chunk index in the source stream.
    pub index: u64,
    /// True when from a live source (RTSP).
    pub is_live: bool,
    /// Signals downstream that a segment boundary should occur after this frame.
    pub end_of_segment: bool,
}

/// A discriminated union of media data flowing through the pipeline.
#[derive(Debug, Clone)]
pub enum Frame {
    Video(VideoFrame),
    Audio(AudioFrame),
}

impl Frame {
    pub fn meta(&self) -> &FrameMeta {
        match self {
            Frame::Video(v) => &v.meta,
            Frame::Audio(a) => &a.meta,
        }
    }

    pub fn meta_mut(&mut self) -> &mut FrameMeta {
        match self {
            Frame::Video(v) => &mut v.meta,
            Frame::Audio(a) => &mut a.meta,
        }
    }
}

/// A single encoded video frame.
#[derive(Debug, Clone)]
pub struct VideoFrame {
    pub meta: FrameMeta,
    pub data: Bytes,
    pub codec: VideoCodec,
    pub keyframe: bool,
    pub source_fps: f64,
    pub codec_params: Option<VideoCodecParams>,
}

/// A chunk of audio data covering a time window.
#[derive(Debug, Clone)]
pub struct AudioFrame {
    pub meta: FrameMeta,
    pub data: Bytes,
    pub codec: AudioCodec,
    pub sample_rate: u32,
    pub channels: u16,
    pub duration: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VideoCodec {
    H264,
    H265,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AudioCodec {
    Aac,
    Opus,
    Pcm,
}

/// Video codec initialization parameters (e.g. H.264 SPS/PPS).
#[derive(Debug, Clone)]
pub struct VideoCodecParams {
    pub sps: Bytes,
    pub pps: Bytes,
    pub width: u32,
    pub height: u32,
    pub frame_rate: f64,
}
