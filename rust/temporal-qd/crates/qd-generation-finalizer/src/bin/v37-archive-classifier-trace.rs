//! Research-only classifier trace for a sealed V37 fast-ephemeral source.
//!
//! This binary opens no market, writes no historical run artifact, and cannot
//! replace or alter the Variant-0 reducer outputs.  It writes one canonical
//! trace file into a caller-provided fresh path.

use std::env;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::PathBuf;

use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use temporal_qd_contract::canonical_json_line;
use temporal_qd_generation_finalizer::trace_fast_ephemeral_source;

fn main() -> Result<()> {
    let mut args = env::args_os();
    let _binary = args.next();
    let source_path = PathBuf::from(
        args.next()
            .context("usage: v37-archive-classifier-trace <frozen-source.json> <new-trace.json>")?,
    );
    let trace_path = PathBuf::from(
        args.next()
            .context("usage: v37-archive-classifier-trace <frozen-source.json> <new-trace.json>")?,
    );
    ensure!(
        args.next().is_none(),
        "expected exactly two positional arguments"
    );
    ensure!(
        source_path.is_file(),
        "frozen source must be an existing file"
    );
    ensure!(!trace_path.exists(), "trace output must not already exist");

    let source_raw = fs::read(&source_path).context("read frozen source")?;
    let source: Value = serde_json::from_slice(&source_raw).context("parse frozen source")?;
    ensure!(
        canonical_json_line(&source)? == source_raw,
        "frozen source must be canonical JSON plus LF"
    );
    let trace = trace_fast_ephemeral_source(source)?;
    if let Some(parent) = trace_path.parent() {
        fs::create_dir_all(parent).context("create trace output directory")?;
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(&trace_path)
        .with_context(|| format!("create trace output: {}", trace_path.display()))?;
    file.write_all(&canonical_json_line(&trace)?)
        .with_context(|| format!("write trace output: {}", trace_path.display()))?;
    file.flush().context("flush trace output")?;
    print!(
        "{}",
        String::from_utf8(canonical_json_line(&json!({
            "schemaVersion":"temporal_qd_v37_native_classifier_trace_runner_v1",
            "generationIndex":trace["generationIndex"],
            "sourceSha256":trace["sourceSha256"],
            "traceSha256":trace["traceSha256"],
            "candidateCount":trace["candidates"].as_array().map_or(0, Vec::len),
        }))?)?
    );
    Ok(())
}
