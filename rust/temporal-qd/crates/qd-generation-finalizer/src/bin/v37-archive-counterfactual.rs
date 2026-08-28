//! Narrow V37 research runner for the historical fast-ephemeral reducer.
//!
//! It accepts a frozen `source.json`, never opens a market or proposal path,
//! and writes the same two native reducer outputs into a fresh research
//! directory.  It is intentionally only a Variant-0 parity runner; any
//! counterfactual delta must bind this parity result first.

use std::env;
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use temporal_qd_contract::canonical_json_line;
use temporal_qd_generation_finalizer::reduce_fast_ephemeral_source;

const CUMULATIVE_PATH: &str = "evidence/cumulative-archive.json";
const ARCHIVE_PATH: &str = "archive.json";

fn write_new(path: &Path, value: &Value) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent).context("create research output directory")?;
    }
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .with_context(|| format!("create research output: {}", path.display()))?;
    file.write_all(&canonical_json_line(value)?)
        .with_context(|| format!("write research output: {}", path.display()))?;
    file.flush().context("flush research output")?;
    Ok(())
}

fn main() -> Result<()> {
    let mut args = env::args_os();
    let _binary = args.next();
    let source_path = PathBuf::from(args.next().context(
        "usage: v37-archive-counterfactual <frozen-source.json> <empty-output-directory>",
    )?);
    let output_dir = PathBuf::from(args.next().context(
        "usage: v37-archive-counterfactual <frozen-source.json> <empty-output-directory>",
    )?);
    ensure!(
        args.next().is_none(),
        "expected exactly two positional arguments"
    );
    ensure!(
        source_path.is_file(),
        "frozen source must be an existing file"
    );
    ensure!(
        !output_dir.exists(),
        "research output directory must not already exist"
    );

    let source_raw = fs::read(&source_path).context("read frozen source")?;
    let source: Value = serde_json::from_slice(&source_raw).context("parse frozen source")?;
    ensure!(
        canonical_json_line(&source)? == source_raw,
        "frozen source must be canonical JSON plus LF"
    );
    let source_sha = source
        .get("sourceSha256")
        .and_then(Value::as_str)
        .context("frozen source lacks sourceSha256")?
        .to_owned();
    let (cumulative, archive) = reduce_fast_ephemeral_source(source)?;
    write_new(&output_dir.join(CUMULATIVE_PATH), &cumulative)?;
    write_new(&output_dir.join(ARCHIVE_PATH), &archive)?;
    print!(
        "{}",
        String::from_utf8(canonical_json_line(&json!({
            "schemaVersion":"temporal_qd_v37_archive_counterfactual_v0_runner_v1",
            "sourceSha256":source_sha,
            "cumulativeArchiveSha256":cumulative["archiveSha256"],
            "parentArchiveSha256":archive["archiveSha256"],
        }))?)?
    );
    Ok(())
}
