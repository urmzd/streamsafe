#![allow(dead_code)] // Domain model fields are intentionally complete for documentation.

//! # finance-flow
//!
//! Financial document ingestion pipeline built on StreamSafe.
//!
//! Pipeline: DocumentSource → TextExtract → LlmExtract → Normalize → StorageSink
//!
//! All LLM extraction is stubbed. See README.md for real implementation guidance.

use clap::Parser;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::fs::{self, File};
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use streamsafe::{PipelineBuilder, Result, Sink, Source, StreamSafeError, Transform};

// ── CLI ─────────────────────────────────────────────────────────────────────

#[derive(Parser)]
#[command(name = "finance-flow")]
struct Cli {
    /// Path to a file or directory of bank statements.
    #[arg(long)]
    input: String,

    /// Output directory for ingestion results.
    #[arg(long, default_value = "data")]
    output: String,

    /// Default currency for transactions.
    #[arg(long, default_value = "CAD")]
    currency: String,
}

// ── Data Model ──────────────────────────────────────────────────────────────

/// Supported document formats.
#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "lowercase")]
enum DocumentFormat {
    Pdf,
    Csv,
    Ofx,
    Txt,
}

/// A source document to ingest.
#[derive(Clone, Debug)]
struct Document {
    path: PathBuf,
    format: DocumentFormat,
    /// Raw file bytes.
    data: Vec<u8>,
    /// SHA-256 hash of the file contents (for dedup).
    content_hash: String,
}

/// Plain text extracted from a document.
#[derive(Clone, Debug)]
struct ExtractedText {
    source_path: PathBuf,
    content_hash: String,
    format: DocumentFormat,
    /// The extracted text content.
    text: String,
}

/// Raw record extracted by LLM — not yet normalized.
#[derive(Clone, Debug, Serialize)]
struct RawTransaction {
    date: String,
    description: String,
    amount: String,
    #[serde(rename = "type")]
    txn_type: String,
    balance: Option<String>,
}

/// Raw account info extracted by LLM.
#[derive(Clone, Debug, Serialize)]
struct RawAccountInfo {
    institution: Option<String>,
    account_type: Option<String>,
    currency: Option<String>,
}

/// LLM extraction result for a document.
#[derive(Clone, Debug)]
struct ExtractionResult {
    source_path: PathBuf,
    content_hash: String,
    transactions: Vec<RawTransaction>,
    account_info: Option<RawAccountInfo>,
}

/// Normalized transaction.
#[derive(Clone, Debug, Serialize)]
struct Transaction {
    date: String,
    description: String,
    amount: f64,
    #[serde(rename = "type")]
    txn_type: TransactionType,
    balance_after: Option<f64>,
}

#[derive(Clone, Debug, Serialize)]
#[serde(rename_all = "lowercase")]
enum TransactionType {
    Debit,
    Credit,
}

/// Account summary.
#[derive(Clone, Debug, Serialize)]
struct AccountSummary {
    institution: Option<String>,
    account_type: Option<String>,
    currency: String,
}

/// Final ingestion result for a single document.
#[derive(Clone, Debug, Serialize)]
struct IngestResult {
    source_file: String,
    content_hash: String,
    transactions: Vec<Transaction>,
    account_summary: Option<AccountSummary>,
}

// ── Source: Document Reader ─────────────────────────────────────────────────

/// Yields documents from a file path or directory.
///
/// Real implementation: walk directory, filter by supported extensions,
/// read file bytes, detect format from extension.
struct DocumentSource {
    paths: Vec<PathBuf>,
    index: usize,
}

impl DocumentSource {
    fn new(input: &str) -> std::result::Result<Self, StreamSafeError> {
        let input_path = Path::new(input);
        let paths = if input_path.is_dir() {
            fs::read_dir(input_path)
                .map_err(StreamSafeError::other)?
                .filter_map(|e| e.ok())
                .map(|e| e.path())
                .filter(|p| {
                    p.extension()
                        .and_then(|e| e.to_str())
                        .map(|e| matches!(e, "pdf" | "csv" | "ofx" | "qfx" | "txt"))
                        .unwrap_or(false)
                })
                .collect()
        } else {
            vec![input_path.to_path_buf()]
        };

        Ok(Self { paths, index: 0 })
    }
}

impl Source for DocumentSource {
    type Output = Document;

    async fn produce(&mut self) -> Result<Option<Document>> {
        if self.index >= self.paths.len() {
            return Ok(None);
        }

        let path = &self.paths[self.index];
        self.index += 1;

        let data = fs::read(path).map_err(StreamSafeError::other)?;

        let mut hasher = Sha256::new();
        hasher.update(&data);
        let content_hash = format!("{:x}", hasher.finalize());

        let format = match path.extension().and_then(|e| e.to_str()) {
            Some("pdf") => DocumentFormat::Pdf,
            Some("csv") => DocumentFormat::Csv,
            Some("ofx" | "qfx") => DocumentFormat::Ofx,
            _ => DocumentFormat::Txt,
        };

        tracing::info!(file = %path.display(), format = ?format, "read document");

        Ok(Some(Document {
            path: path.clone(),
            format,
            data,
            content_hash,
        }))
    }
}

// ── Transform: Text Extraction ──────────────────────────────────────────────

/// Extracts plain text from a document based on its format.
///
/// Real implementation:
///   - PDF: pdfminer.six or pdf-extract crate
///   - CSV: format rows as labeled key-value pairs for LLM consumption
///   - OFX/QFX: pass raw SGML text through (LLM parses the tags)
///   - TXT: direct passthrough
struct TextExtractTransform;

impl Transform for TextExtractTransform {
    type Input = Document;
    type Output = ExtractedText;

    async fn apply(&mut self, doc: Document) -> Result<ExtractedText> {
        // Stub: treat all formats as UTF-8 text.
        // Real implementation dispatches by format.
        let text = String::from_utf8_lossy(&doc.data).into_owned();

        tracing::info!(
            file = %doc.path.display(),
            chars = text.len(),
            "extracted text"
        );

        Ok(ExtractedText {
            source_path: doc.path,
            content_hash: doc.content_hash,
            format: doc.format,
            text,
        })
    }
}

// ── Transform: LLM-Based Structured Extraction ─────────────────────────────

/// Sends extracted text to an LLM with few-shot examples to extract
/// structured transaction records and account info.
///
/// Real implementation:
///   - Build prompt with few-shot examples (Canadian/US bank statement formats)
///   - Call Ollama (local gemma3), Gemini, or OpenAI
///   - Parse structured output into RawTransaction records
///   - Extract account info (institution, type, balances, period, currency)
struct LlmExtractTransform {
    provider: String,
    model: String,
}

impl LlmExtractTransform {
    fn new(provider: &str, model: &str) -> Self {
        Self {
            provider: provider.to_string(),
            model: model.to_string(),
        }
    }
}

impl Transform for LlmExtractTransform {
    type Input = ExtractedText;
    type Output = ExtractionResult;

    async fn apply(&mut self, text: ExtractedText) -> Result<ExtractionResult> {
        // Stub: generate synthetic transactions from the text.
        // Real implementation: LLM call with few-shot prompting.
        tracing::info!(
            provider = %self.provider,
            model = %self.model,
            file = %text.source_path.display(),
            "extracting transactions via LLM"
        );

        let transactions = vec![
            RawTransaction {
                date: "2024-01-15".to_string(),
                description: "GROCERY STORE #1234".to_string(),
                amount: "-45.67".to_string(),
                txn_type: "debit".to_string(),
                balance: Some("1,234.56".to_string()),
            },
            RawTransaction {
                date: "2024-01-16".to_string(),
                description: "PAYROLL DEPOSIT".to_string(),
                amount: "2,500.00".to_string(),
                txn_type: "credit".to_string(),
                balance: Some("3,734.56".to_string()),
            },
        ];

        let account_info = Some(RawAccountInfo {
            institution: Some("Example Bank".to_string()),
            account_type: Some("chequing".to_string()),
            currency: Some("CAD".to_string()),
        });

        Ok(ExtractionResult {
            source_path: text.source_path,
            content_hash: text.content_hash,
            transactions,
            account_info,
        })
    }
}

// ── Transform: Normalization ────────────────────────────────────────────────

/// Normalizes raw LLM-extracted records into typed transactions.
///
/// Real implementation:
///   - Parse dates with chrono (flexible format detection)
///   - Clean amounts: remove $, commas, handle parentheses notation for negatives
///   - Validate transaction types (debit/credit)
///   - Sort transactions by date
struct NormalizeTransform {
    default_currency: String,
}

impl NormalizeTransform {
    fn new(currency: &str) -> Self {
        Self {
            default_currency: currency.to_string(),
        }
    }
}

impl Transform for NormalizeTransform {
    type Input = ExtractionResult;
    type Output = IngestResult;

    async fn apply(&mut self, extraction: ExtractionResult) -> Result<IngestResult> {
        let mut transactions = Vec::with_capacity(extraction.transactions.len());

        for raw in &extraction.transactions {
            // Stub normalization: strip $ and commas, parse as f64.
            let amount_str = raw.amount.replace(['$', ','], "");
            let amount: f64 = amount_str.parse().unwrap_or(0.0);

            let balance_after = raw.balance.as_ref().and_then(|b| {
                let cleaned = b.replace(['$', ','], "");
                cleaned.parse::<f64>().ok()
            });

            let txn_type = if raw.txn_type.eq_ignore_ascii_case("credit") {
                TransactionType::Credit
            } else {
                TransactionType::Debit
            };

            transactions.push(Transaction {
                date: raw.date.clone(),
                description: raw.description.clone(),
                amount,
                txn_type,
                balance_after,
            });
        }

        // Sort by date.
        transactions.sort_by(|a, b| a.date.cmp(&b.date));

        let account_summary = extraction.account_info.map(|info| AccountSummary {
            institution: info.institution,
            account_type: info.account_type,
            currency: info
                .currency
                .unwrap_or_else(|| self.default_currency.clone()),
        });

        tracing::info!(
            file = %extraction.source_path.display(),
            transactions = transactions.len(),
            "normalized"
        );

        Ok(IngestResult {
            source_file: extraction.source_path.display().to_string(),
            content_hash: extraction.content_hash,
            transactions,
            account_summary,
        })
    }
}

// ── Sink: Deduplicating Storage ─────────────────────────────────────────────

/// Persists ingestion results to disk with SHA-256 content-hash deduplication.
///
/// Real implementation:
///   - Check content hash against index before writing
///   - Write IngestResult as JSON to `{data_dir}/ingestions/{uuid}.json`
///   - Update index file at `{data_dir}/index.json`
///   - Set file permissions to 0o600
struct StorageSink {
    data_dir: PathBuf,
    seen_hashes: HashSet<String>,
}

impl StorageSink {
    fn new(data_dir: &str) -> std::result::Result<Self, StreamSafeError> {
        let dir = PathBuf::from(data_dir);
        fs::create_dir_all(dir.join("ingestions")).map_err(StreamSafeError::other)?;
        Ok(Self {
            data_dir: dir,
            seen_hashes: HashSet::new(),
        })
    }
}

impl Sink for StorageSink {
    type Input = IngestResult;

    async fn consume(&mut self, result: IngestResult) -> Result<()> {
        // Dedup: skip if we've already ingested this file.
        if !self.seen_hashes.insert(result.content_hash.clone()) {
            tracing::warn!(
                file = %result.source_file,
                hash = %result.content_hash,
                "duplicate — skipped"
            );
            return Ok(());
        }

        // Write result as JSON.
        let filename = format!("{}.json", &result.content_hash[..16]);
        let path = self.data_dir.join("ingestions").join(&filename);
        let file = File::create(&path).map_err(StreamSafeError::other)?;
        let writer = BufWriter::new(file);
        serde_json::to_writer_pretty(writer, &result).map_err(StreamSafeError::other)?;

        tracing::info!(
            file = %result.source_file,
            transactions = result.transactions.len(),
            output = %path.display(),
            "stored"
        );

        Ok(())
    }
}

// ── Main ────────────────────────────────────────────────────────────────────

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter("finance_flow=info")
        .init();
    let cli = Cli::parse();

    tracing::info!(input = %cli.input, currency = %cli.currency, "starting ingestion");

    PipelineBuilder::from(DocumentSource::new(&cli.input)?)
        .pipe(TextExtractTransform)
        .pipe(LlmExtractTransform::new("ollama", "gemma3"))
        .pipe(NormalizeTransform::new(&cli.currency))
        .into(StorageSink::new(&cli.output)?)
        .run()
        .await?;

    tracing::info!(output = %cli.output, "ingestion complete");
    Ok(())
}
