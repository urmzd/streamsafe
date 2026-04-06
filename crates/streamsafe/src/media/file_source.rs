use super::Frame;
use crate::error::{Result, StreamSafeError};
use crate::source::Source;
use std::path::PathBuf;

/// Reads local video/audio files and yields `Frame` variants.
///
/// Requires the `ffmpeg` feature and FFmpeg system libraries.
/// Without the feature, calling `produce()` returns an error.
pub struct FileSource {
    path: PathBuf,
    rx: Option<tokio::sync::mpsc::Receiver<Result<Frame>>>,
}

impl FileSource {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self {
            path: path.into(),
            rx: None,
        }
    }

    fn start_decoder(&mut self) -> Result<()> {
        let (tx, rx) = tokio::sync::mpsc::channel(64);
        let path = self.path.clone();

        std::thread::spawn(move || {
            if let Err(e) = decode_file(&path, &tx) {
                let _ = tx.blocking_send(Err(e));
            }
        });

        self.rx = Some(rx);
        Ok(())
    }
}

impl Source for FileSource {
    type Output = Frame;

    async fn produce(&mut self) -> Result<Option<Frame>> {
        if self.rx.is_none() {
            self.start_decoder()?;
        }

        match self.rx.as_mut().unwrap().recv().await {
            Some(Ok(frame)) => Ok(Some(frame)),
            Some(Err(e)) => Err(e),
            None => Ok(None),
        }
    }
}

#[cfg(feature = "ffmpeg")]
fn decode_file(
    path: &std::path::Path,
    tx: &tokio::sync::mpsc::Sender<Result<Frame>>,
) -> Result<()> {
    use super::{AudioCodec, AudioFrame, FrameMeta, VideoCodec, VideoFrame};
    use bytes::Bytes;
    use std::time::Duration;

    ffmpeg_next::init().map_err(StreamSafeError::other)?;

    let ictx = ffmpeg_next::format::input(path).map_err(StreamSafeError::other)?;

    let source_uri = format!("file://{}", path.display());
    let mut frame_index: u64 = 0;

    let video_stream_idx = ictx
        .streams()
        .best(ffmpeg_next::media::Type::Video)
        .map(|s| s.index());
    let audio_stream_idx = ictx
        .streams()
        .best(ffmpeg_next::media::Type::Audio)
        .map(|s| s.index());

    // Extract stream metadata before iterating packets
    let video_fps = video_stream_idx
        .and_then(|idx| ictx.stream(idx))
        .map(|s| {
            let rate = s.avg_frame_rate();
            if rate.denominator() > 0 {
                rate.numerator() as f64 / rate.denominator() as f64
            } else {
                0.0
            }
        })
        .unwrap_or(0.0);

    let video_codec = video_stream_idx
        .and_then(|idx| ictx.stream(idx))
        .map(|s| {
            let params = s.parameters();
            match params.id() {
                ffmpeg_next::codec::Id::H265 | ffmpeg_next::codec::Id::HEVC => VideoCodec::H265,
                _ => VideoCodec::H264,
            }
        })
        .unwrap_or(VideoCodec::H264);

    let (audio_sample_rate, audio_channels, audio_codec) = audio_stream_idx
        .and_then(|idx| ictx.stream(idx))
        .map(|s| {
            let params = s.parameters();
            let codec = match params.id() {
                ffmpeg_next::codec::Id::OPUS => AudioCodec::Opus,
                ffmpeg_next::codec::Id::PCM_S16LE
                | ffmpeg_next::codec::Id::PCM_S16BE
                | ffmpeg_next::codec::Id::PCM_F32LE => AudioCodec::Pcm,
                _ => AudioCodec::Aac,
            };
            let decoder = ffmpeg_next::codec::context::Context::from_parameters(params)
                .ok()
                .and_then(|ctx| ctx.decoder().audio().ok());
            let rate = decoder.as_ref().map(|d| d.rate()).unwrap_or(0);
            let ch = decoder.as_ref().map(|d| d.channels()).unwrap_or(0);
            (rate, ch, codec)
        })
        .unwrap_or((0, 0, AudioCodec::Aac));

    for (stream, packet) in ictx.packets() {
        let stream_idx = stream.index();
        let time_base = stream.time_base();

        let pts_secs = packet
            .pts()
            .map(|pts| pts as f64 * time_base.numerator() as f64 / time_base.denominator() as f64);
        let pts_duration = Duration::from_secs_f64(pts_secs.unwrap_or(0.0));

        if Some(stream_idx) == video_stream_idx {
            let data = packet.data().unwrap_or(&[]);
            let frame = Frame::Video(VideoFrame {
                meta: FrameMeta {
                    pts: pts_duration,
                    source_uri: source_uri.clone(),
                    index: frame_index,
                    is_live: false,
                    end_of_segment: false,
                },
                data: Bytes::copy_from_slice(data),
                codec: video_codec,
                keyframe: packet.is_key(),
                source_fps: video_fps,
                codec_params: None,
            });

            frame_index += 1;
            if tx.blocking_send(Ok(frame)).is_err() {
                return Ok(());
            }
        } else if Some(stream_idx) == audio_stream_idx {
            let data = packet.data().unwrap_or(&[]);
            let duration_secs = packet.duration() as f64 * time_base.numerator() as f64
                / time_base.denominator() as f64;

            let frame = Frame::Audio(AudioFrame {
                meta: FrameMeta {
                    pts: pts_duration,
                    source_uri: source_uri.clone(),
                    index: frame_index,
                    is_live: false,
                    end_of_segment: false,
                },
                data: Bytes::copy_from_slice(data),
                codec: audio_codec,
                sample_rate: audio_sample_rate,
                channels: audio_channels as u16,
                duration: Duration::from_secs_f64(duration_secs),
            });

            frame_index += 1;
            if tx.blocking_send(Ok(frame)).is_err() {
                return Ok(());
            }
        }
    }

    Ok(())
}

#[cfg(not(feature = "ffmpeg"))]
fn decode_file(
    _path: &std::path::Path,
    _tx: &tokio::sync::mpsc::Sender<Result<Frame>>,
) -> Result<()> {
    Err(StreamSafeError::other(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "FileSource requires the 'ffmpeg' feature: streamsafe = { features = [\"ffmpeg\"] }",
    )))
}
