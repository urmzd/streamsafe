<p align="center">
  <h1 align="center">StreamSafe</h1>
  <p align="center">
    Type-safe async pipeline framework for data processing in Rust. Mismatched types between stages are compile errors, not runtime crashes.
    <br /><br />
    <a href="#installation">Install</a>
    &middot;
    <a href="https://github.com/urmzd/streamsafe/issues">Report Bug</a>
    &middot;
    <a href="https://crates.io/crates/streamsafe">Crates.io</a>
  </p>
</p>

<p align="center">
  <a href="https://github.com/urmzd/streamsafe/actions/workflows/ci.yml"><img src="https://github.com/urmzd/streamsafe/actions/workflows/ci.yml/badge.svg" alt="CI"></a>
  &nbsp;
  <a href="https://crates.io/crates/streamsafe"><img src="https://img.shields.io/crates/v/streamsafe" alt="crates.io"></a>
  &nbsp;
  <a href="LICENSE"><img src="https://img.shields.io/github/license/urmzd/streamsafe" alt="License"></a>
</p>

Build pipelines with compile-time type checking: the full chain is known at compile time, so mismatched stages never reach runtime.

## Quick Start

```rust
use streamsafe::{PipelineBuilder, Source, Transform, Sink};

PipelineBuilder::from(my_source)
    .pipe(my_transform)
    .into(my_sink)
    .run()
    .await?;
```

## Installation

```sh
cargo add streamsafe
```

Or add to `Cargo.toml`:

```toml
[dependencies]
streamsafe = "0.1"
```

The `media` feature (enabled by default) includes RTSP ingestion, MP4 segment writing, WAV audio output, and a rich media frame model. Disable it for the core framework only:

```toml
streamsafe = { version = "0.1", default-features = false }
```

## Core Concepts

Three traits form the pipeline:

- **`Source<Output>`** — produces items. Returns `Ok(None)` when exhausted.
- **`Transform<Input, Output>`** — maps one item to another.
- **`Sink<Input>`** — consumes items (terminal stage).

`PipelineBuilder` chains them with recursive generics (like Iterator adapters). Each `.pipe()` call wraps the previous stage in a new type — the full chain is known at compile time.

```rust
// This compiles — types align:
PipelineBuilder::from(rtsp_source)       // Output = Frame
    .pipe(FrameCountSplitter::new(300))  // Input = Frame, Output = Frame
    .into(Mp4SegmentSink::new(...))      // Input = Frame ✓

// This is a compile error — WavSink expects AudioFrame, not Frame:
PipelineBuilder::from(rtsp_source)       // Output = Frame
    .into(WavSink::new("out.wav"))       // Input = AudioFrame ✗
```

### Backpressure & Shutdown

Each stage runs as a separate tokio task connected by bounded `mpsc` channels. Slow consumers apply backpressure to upstream stages automatically. Graceful shutdown via `CancellationToken` or Ctrl-C.

## Media Feature

The `media` feature provides a discriminated union data model and ready-made pipeline nodes for audio/video processing.

### Data Model

```rust
enum Frame {
    Video(VideoFrame),  // H.264/H.265 encoded frames
    Audio(AudioFrame),  // PCM/AAC/Opus audio chunks
}
```

Each variant is self-contained — no `Option` fields to check at runtime. Pattern matching enforces handling.

### Nodes

| Node | Type | Description |
|------|------|-------------|
| `RtspSource` | Source\<Frame\> | RTSP/RTP demuxing via `retina` |
| `FileSource` | Source\<Frame\> | Local file reading (stub) |
| `FrameCountSplitter` | Transform\<Frame, Frame\> | Marks segment boundaries every N video frames |
| `AudioExtractor` | Transform\<Frame, AudioFrame\> | Narrows Frame stream to audio only |
| `Mp4SegmentSink` | Sink\<Frame\> | Writes segmented MP4 files via `muxide` |
| `WavSink` | Sink\<AudioFrame\> | Writes WAV audio via `hound` |

### Examples

**RTSP to segmented MP4:**

```sh
cargo run --example rtsp_archiver -- --uri rtsp://localhost:8554/stream --frames 300
```

```rust
PipelineBuilder::from(RtspSource::new("rtsp://localhost:8554/stream")?)
    .pipe(FrameCountSplitter::new(300))
    .into(Mp4SegmentSink::new("segments/", "segment-{}.mp4"))
    .run()
    .await?;
```

**RTSP audio extraction to WAV:**

```sh
cargo run --example audio_extract -- --uri rtsp://localhost:8554/stream
```

```rust
PipelineBuilder::from(RtspSource::new("rtsp://localhost:8554/stream")?)
    .pipe(AudioExtractor)
    .into(WavSink::new("output.wav"))
    .run()
    .await?;
```

## Implementing Custom Nodes

```rust
use streamsafe::{Source, Transform, Sink, Result};

// Source that counts to 10
struct Counter(u32);

impl Source for Counter {
    type Output = u32;
    async fn produce(&mut self) -> Result<Option<u32>> {
        if self.0 >= 10 { return Ok(None); }
        self.0 += 1;
        Ok(Some(self.0))
    }
}

// Transform that doubles
struct Double;

impl Transform for Double {
    type Input = u32;
    type Output = u32;
    async fn apply(&mut self, n: u32) -> Result<u32> {
        Ok(n * 2)
    }
}

// Sink that prints
struct Printer;

impl Sink for Printer {
    type Input = u32;
    async fn consume(&mut self, n: u32) -> Result<()> {
        println!("{n}");
        Ok(())
    }
}

// Wire them together
PipelineBuilder::from(Counter(0))
    .pipe(Double)
    .into(Printer)
    .run()
    .await?;
```

## Development

A `docker-compose.yml` is included for local testing with a mock RTSP server:

```sh
docker-compose up -d video_server video_stream
cargo run --example rtsp_archiver
```

## Agent Skill

This repo's conventions are available as portable agent skills in [`skills/`](skills/).

## License

[Apache-2.0](LICENSE)
