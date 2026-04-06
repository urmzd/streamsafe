use clap::Parser;
use streamsafe::media::{AudioExtractor, RtspSource, WavSink};
use streamsafe::PipelineBuilder;

#[derive(Parser)]
#[command(name = "audio-extract")]
struct Cli {
    #[arg(long, env = "RTSP_URI")]
    uri: String,
    #[arg(long, default_value = "output.wav")]
    output: String,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("streamsafe=info")
        .init();
    let cli = Cli::parse();

    // RtspSource yields Frame (Video|Audio)
    // AudioExtractor narrows to AudioFrame only
    // WavSink consumes AudioFrame
    PipelineBuilder::from(RtspSource::new(&cli.uri)?)
        .pipe(AudioExtractor)
        .into(WavSink::new(&cli.output))
        .run()
        .await?;

    Ok(())
}
