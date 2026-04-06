use super::{AudioCodec, AudioFrame, Frame, FrameMeta, VideoCodec, VideoFrame};
use crate::error::{Result, StreamSafeError};
use crate::source::Source;
use bytes::Bytes;
use futures::StreamExt;
use retina::codec::CodecItem;
use std::pin::Pin;
use std::time::Duration;
use url::Url;

/// RTSP source that yields `Frame::Video` and `Frame::Audio` variants
/// using the `retina` crate for RTSP/RTP demuxing.
pub struct RtspSource {
    url: Url,
    session: Option<Pin<Box<retina::client::Demuxed>>>,
    frame_index: u64,
    sent_params: bool,
}

impl RtspSource {
    pub fn new(uri: &str) -> std::result::Result<Self, url::ParseError> {
        Ok(Self {
            url: Url::parse(uri)?,
            session: None,
            frame_index: 0,
            sent_params: false,
        })
    }

    async fn connect(&mut self) -> Result<()> {
        let mut session = retina::client::Session::describe(
            self.url.clone(),
            retina::client::SessionOptions::default(),
        )
        .await
        .map_err(StreamSafeError::other)?;

        // Setup all streams (video + audio if available)
        for i in 0..session.streams().len() {
            session
                .setup(i, retina::client::SetupOptions::default())
                .await
                .map_err(StreamSafeError::other)?;
        }

        let demuxed = session
            .play(retina::client::PlayOptions::default())
            .await
            .map_err(StreamSafeError::other)?
            .demuxed()
            .map_err(StreamSafeError::other)?;

        self.session = Some(Box::pin(demuxed));
        Ok(())
    }
}

impl Source for RtspSource {
    type Output = Frame;

    async fn produce(&mut self) -> Result<Option<Frame>> {
        if self.session.is_none() {
            self.connect().await?;
        }

        let session = self.session.as_mut().unwrap();
        let uri = self.url.to_string();

        loop {
            match session.next().await {
                Some(Ok(CodecItem::VideoFrame(vf))) => {
                    let index = self.frame_index;
                    self.frame_index += 1;

                    let keyframe = vf.is_random_access_point();
                    let has_new = vf.has_new_parameters();
                    let pts = Duration::from_secs_f64(vf.timestamp().elapsed_secs());

                    let codec_params = if !self.sent_params || has_new {
                        self.sent_params = true;
                        // TODO: extract SPS/PPS from retina VideoParameters
                        None
                    } else {
                        None
                    };

                    return Ok(Some(Frame::Video(VideoFrame {
                        meta: FrameMeta {
                            pts,
                            source_uri: uri,
                            index,
                            is_live: true,
                            end_of_segment: false,
                        },
                        data: Bytes::from(vf.into_data()),
                        codec: VideoCodec::H264,
                        keyframe,
                        source_fps: 0.0, // TODO: extract from stream params
                        codec_params,
                    })));
                }
                Some(Ok(CodecItem::AudioFrame(af))) => {
                    let index = self.frame_index;
                    self.frame_index += 1;

                    let pts = Duration::from_secs_f64(af.timestamp().elapsed_secs());

                    return Ok(Some(Frame::Audio(AudioFrame {
                        meta: FrameMeta {
                            pts,
                            source_uri: uri,
                            index,
                            is_live: true,
                            end_of_segment: false,
                        },
                        data: Bytes::copy_from_slice(af.data()),
                        codec: AudioCodec::Aac, // TODO: detect from stream
                        sample_rate: 0,         // TODO: extract from params
                        channels: 0,
                        duration: Duration::ZERO,
                    })));
                }
                Some(Ok(_)) => continue,
                Some(Err(e)) => return Err(StreamSafeError::other(e)),
                None => return Ok(None),
            }
        }
    }
}
