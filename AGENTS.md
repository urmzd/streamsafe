# streamsafe

A type-safe async pipeline framework for data processing in Rust. Mismatched types between stages are compile errors, not runtime crashes.

## Architecture

A pipeline is a chain of stages, each running as its own tokio task, connected by bounded `mpsc` channels. The full chain is encoded in the type system: `PipelineBuilder` wraps the previous stage in a new generic type on every combinator, so adjacent stages must agree on their item type or the program does not compile.

| Module | Role |
|--------|------|
| `source.rs` | `Source` trait: produces items; `Ok(None)` signals exhaustion |
| `transform.rs` | `Transform` trait: 1-to-1 mapping; output type must match the next stage's input |
| `sink.rs` | `Sink` trait: terminal stage; `consume` per item, `finish` once after close |
| `filter_transform.rs` | `FilterTransform` + `filter_map_fn`: may drop items by returning `Ok(None)` |
| `fallible.rs` | `FallibleTransform`: per-item recoverable errors route to the error rail |
| `pipeline.rs` | `PipelineBuilder`, `RunnablePipeline`, the sealed `Stage` trait, batching, split/rejoin |
| `fanout.rs` | `broadcast`/`broadcast3`: fan-out to N sinks, each on its own channel |
| `merge.rs` | `merge`/`merge3`: fan-in from N sources into one stream |
| `pipeline_sink.rs` | `PipelineSink`, `SubPipeline`: nest a sub-pipeline as a sink (per-branch chains) |
| `broadcast.rs` | `BroadcastSink`: sink-side fan-out helper |
| `channel_source.rs` | `ChannelSource`: adapt an external `mpsc` receiver into a `Source` |
| `error.rs` / `error_sink.rs` | `StreamSafeError`, `Result`, and `DiscardErrors` (the default error sink) |
| `runtime.rs` | Task spawning, `CancellationToken` / Ctrl-C handling, join-and-surface-first-error |
| `media/` | Optional `media` feature: `Frame` model + RTSP/MP4/WAV nodes |

## Usage

streamsafe is a library, not a CLI. Build a pipeline with the three core traits and run it:

```rust
use streamsafe::{PipelineBuilder, Source, Transform, Sink};

PipelineBuilder::from(my_source)   // Output = T
    .pipe(my_transform)            // Input = T, Output = U
    .into(my_sink)                 // Input = U  (mismatch => compile error)
    .run()
    .await?;
```

Combinators on `PipelineBuilder`: `pipe`, `filter_pipe`, `try_pipe`, `batch`, `split`/`split3`, `broadcast`/`broadcast3`, `merge`/`merge3`, `on_errors`, then `into` + `run` (or `run_with_token` for external cancellation).

Runnable examples live in `crates/streamsafe/examples/` (`rtsp_archiver`, `audio_extract`) and `examples/` (`fight-flow`, `finance-flow`):

```bash
docker compose up -d video_server video_stream   # mock RTSP for the media examples
cargo run --example rtsp_archiver -- --uri rtsp://localhost:8554/stream --frames 300
```

The `media` feature is on by default; disable it (`default-features = false`) for the core framework only.

## Commands

```bash
just            # default: fmt + lint + test
just build      # cargo build --release
just test       # cargo test --workspace
just lint       # cargo clippy --workspace -- -D warnings
just fmt        # cargo fmt --all
just init       # set git hooks path, fetch deps
```

## Commit Convention

Angular conventional commits, enforced by the git hook:

- `feat:` new feature
- `fix:` bug fix
- `docs:` documentation
- `refactor:` code restructuring
- `test:` test changes
- `chore:` maintenance
- `ci:` CI/CD changes
- `perf:` performance

## Code Style

- Three public traits (`Source`, `Transform`, `Sink`) plus their filter/fallible variants; everything else composes from them.
- The `Stage` trait is sealed (`pub(crate)`) — the type-state builder is the only way to assemble a pipeline.
- One tokio task per stage, joined by bounded `mpsc` channels; backpressure is the channel filling up.
- `biased` `tokio::select!` so cancellation always wins over producing the next item.
- Errors are a single `thiserror` enum (`StreamSafeError`); recoverable per-item failures go through `FallibleTransform` to an error sink, fatal errors surface from `run` without cancelling sibling tasks.
- No cross-task cancellation except via `CancellationToken` (external or Ctrl-C).
- Durability is the caller's concern: an SDK does not own a write-ahead log. A stage that persists before forwarding (`Sink` writes, then a downstream `Source` resumes from that store) is the user's WAL — emit the unit, make it durable, then transmit.
- Async tests with `#[tokio::test]`, asserting against a `CollectSink` helper; the `media` module sits behind its feature flag.
