# finance-flow

AI-powered financial document ingestion pipeline built on StreamSafe.

Models the [finfree](https://github.com/urmzd/finfree) architecture as a typed DAG: bank statements flow through text extraction, LLM-based structured extraction, normalization, and deduplicating storage with compile-time type safety.

## Pipeline

```
DocumentSource               → produces Document (file path + raw bytes)
  │
  ├─ TextExtractTransform    → Document → ExtractedText (plain text from PDF/CSV/OFX)
  │
  ├─ LlmExtractTransform    → ExtractedText → ExtractionResult (structured records via LLM)
  │
  ├─ NormalizeTransform      → ExtractionResult → IngestResult (typed transactions + account summary)
  │
  └─ StorageSink             → persist to disk with SHA-256 dedup
```

```rust
PipelineBuilder::from(DocumentSource::new(&paths))
    .pipe(TextExtractTransform)
    .pipe(LlmExtractTransform::new(provider, model))
    .pipe(NormalizeTransform::new(currency))
    .into(StorageSink::new(&data_dir)?)
    .run()
    .await?;
```

## Data Flow

| Stage | Input | Output | Real Implementation |
|-------|-------|--------|---------------------|
| `DocumentSource` | file paths | `Document` | Read file bytes, detect format (PDF/CSV/OFX/TXT) |
| `TextExtractTransform` | `Document` | `ExtractedText` | pdfminer.six for PDF, CSV row formatting, raw read for OFX/TXT |
| `LlmExtractTransform` | `ExtractedText` | `ExtractionResult` | langextract with Ollama/Gemini/OpenAI + few-shot examples |
| `NormalizeTransform` | `ExtractionResult` | `IngestResult` | Date parsing, amount cleaning ($, commas, parens), type validation |
| `StorageSink` | `IngestResult` | JSON on disk | SHA-256 content hash dedup, index file, `~/.finfree/data/` |

## Extending

To build a real implementation, replace the stub logic in each node:

- **DocumentSource**: Enumerate files from CLI args or a watched directory. For continuous ingestion, watch a directory with `notify` and yield new files as they appear.
- **TextExtractTransform**: Use `pdf-extract` or call `pdfminer.six` via a Python sidecar for PDF text extraction. Format CSV rows as labeled key-value pairs for LLM consumption. Pass OFX/QFX through as raw text.
- **LlmExtractTransform**: Call an LLM (Ollama local, Gemini, or OpenAI) with few-shot examples of transaction extraction. Parse the structured output into `RawTransaction` records.
- **NormalizeTransform**: Parse dates with `chrono`, clean monetary amounts, validate debit/credit types, sort by date.
- **StorageSink**: Compute SHA-256 of source file for dedup. Write `IngestResult` as JSON. Maintain an index file for querying.

## Running

```sh
cargo run -p finance-flow -- --input statements/ --output data/ --currency CAD
```

All LLM extraction is stubbed with synthetic data. This example demonstrates the pipeline architecture, not the model implementations.
