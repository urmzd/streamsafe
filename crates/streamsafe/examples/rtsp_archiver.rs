use clap::Parser;
use streamsafe::media::{FrameCountSplitter, Mp4SegmentSink, RtspSource};
use streamsafe::PipelineBuilder;

#[derive(Parser)]
#[command(name = "rtsp-archiver")]
struct Cli {
    #[arg(long, env = "RTSP_URI", default_value = "rtsp://localhost:8554/vsa")]
    uri: String,
    #[arg(long, default_value = "300")]
    frames: u32,
    #[arg(long, default_value = "segments")]
    output: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("streamsafe=info")
        .init();
    let cli = Cli::parse();

    PipelineBuilder::from(RtspSource::new(&cli.uri)?)
        .pipe(FrameCountSplitter::new(cli.frames))
        .buffer(128)
        .into(Mp4SegmentSink::new(&cli.output, "segment-{}.mp4"))
        .run()
        .await?;

    Ok(())
}
