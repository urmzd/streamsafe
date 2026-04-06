# fight-flow

AI-powered combat sports video analysis pipeline built on StreamSafe.

Models the [fighter-iq](https://github.com/urmzd/fighter-iq) architecture as a typed DAG: video frames flow through detection, analysis, and narrative stitching stages with compile-time type safety.

## Pipeline

```
VideoSource                  → produces VideoFrame (decoded pixels + timestamp)
  │
  ├─ DetectionTransform      → VideoFrame → FrameAnalysis (YOLO pose + VLM description + spatial)
  │
  ├─ filter_pipe             → drop frames with no fighters detected
  │
  ├─ batch(segment_size)     → collect N frames into Vec<FrameAnalysis>
  │
  ├─ StitchTransform         → Vec<FrameAnalysis> → Segment (narrative + control + impacts)
  │
  └─ JsonSink                → write each segment as JSONL to output file
```

```rust
PipelineBuilder::from(VideoSource::new(&path, interval))
    .pipe(DetectionTransform::new())
    .filter_pipe(filter_map_fn(|a: FrameAnalysis| {
        if a.fighters.is_empty() { None } else { Some(a) }
    }))
    .batch(segment_size)
    .pipe(StitchTransform)
    .into(JsonSink::new(&output))
    .run()
    .await?;
```

## Data Flow

| Stage | Input | Output | Real Implementation |
|-------|-------|--------|---------------------|
| `VideoSource` | file path + fps | `VideoFrame` | OpenCV / ffmpeg frame extraction |
| `DetectionTransform` | `VideoFrame` | `FrameAnalysis` | YOLOv8-pose detection, VLM (Qwen2.5-VL / Gemma-4) description, spatial metrics |
| `filter_pipe` | `FrameAnalysis` | `FrameAnalysis` | Drop frames with no detected fighters |
| `batch(N)` | `FrameAnalysis` | `Vec<FrameAnalysis>` | StreamSafe built-in batching |
| `StitchTransform` | `Vec<FrameAnalysis>` | `Segment` | LLM-based narrative stitching (Qwen2.5-1.5B / Gemma-4) |
| `JsonSink` | `Segment` | JSONL file | One JSON object per line |

## Extending

To build a real implementation, replace the stub logic in each node:

- **VideoSource**: Use `ffmpeg-next` or the streamsafe `media` feature's `FileSource` to decode video frames.
- **DetectionTransform**: Call YOLO via ONNX runtime or a Python sidecar for pose estimation. Add VLM inference for per-frame descriptions and spatial analysis for control scoring.
- **StitchTransform**: Call an LLM to summarize batched frame descriptions into coherent narratives.
- **Post-pipeline**: Run tactic identification and strategy classification on the collected segments (keyword + embedding boundary detection, sliding-window classification).

## Running

```sh
cargo run -p fight-flow -- --input video.mp4 --segment-size 5 --output analysis.jsonl
```

All ML inference is stubbed with synthetic data. This example demonstrates the pipeline architecture, not the model implementations.
