//! Prepare a sealed V37 source for a fixed-stream candidate-admission study.
//!
//! The command changes only the native v5 newcomer limit.  It neither runs
//! evaluations nor invents prior-panel evidence; the historical finalizer
//! remains responsible for all archive decisions.

use std::env;
use std::fs::{self, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result, anyhow, ensure};
use serde_json::Value;
use temporal_qd_contract::canonical_json_line;
use temporal_qd_rotating_prefinalizer::v5::rebuild_fast_ephemeral_source_with_newcomer_limit;

fn read_canonical_json(path: &Path, name: &str) -> Result<Value> {
    let raw = fs::read(path).with_context(|| format!("read {name}"))?;
    let value = serde_json::from_slice(&raw).with_context(|| format!("parse {name}"))?;
    ensure!(
        canonical_json_line(&value)? == raw,
        "{name} must be canonical JSON plus LF"
    );
    Ok(value)
}

fn read_canonical_jsonl(path: &Path, name: &str) -> Result<Vec<Value>> {
    let file = fs::File::open(path).with_context(|| format!("open {name}"))?;
    let mut rows = Vec::new();
    for (index, line) in BufReader::new(file).split(b'\n').enumerate() {
        let line = line.with_context(|| format!("read {name} line {}", index + 1))?;
        if line.is_empty() {
            continue;
        }
        let value: Value = serde_json::from_slice(&line)
            .with_context(|| format!("parse {name} line {}", index + 1))?;
        ensure!(
            canonical_json_line(&value)? == [line, vec![b'\n']].concat(),
            "{name} line {} must be canonical JSON plus LF",
            index + 1
        );
        rows.push(value);
    }
    ensure!(!rows.is_empty(), "{name} must not be empty");
    Ok(rows)
}

fn write_new(path: &Path, value: &Value) -> Result<()> {
    let mut file = OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
        .with_context(|| format!("create output source: {}", path.display()))?;
    file.write_all(&canonical_json_line(value)?)
        .context("write output source")?;
    file.flush().context("flush output source")?;
    Ok(())
}

fn main() -> Result<()> {
    let mut args = env::args_os();
    let _binary = args.next();
    let source_path = PathBuf::from(args.next().context(
        "usage: v37-full-pool-source <source.json> <evaluated-members.jsonl> <candidate-panel-bundles.jsonl> <newcomer-limit> <new-source.json>",
    )?);
    let members_path = PathBuf::from(args.next().context(
        "usage: v37-full-pool-source <source.json> <evaluated-members.jsonl> <candidate-panel-bundles.jsonl> <newcomer-limit> <new-source.json>",
    )?);
    let bundles_path = PathBuf::from(args.next().context(
        "usage: v37-full-pool-source <source.json> <evaluated-members.jsonl> <candidate-panel-bundles.jsonl> <newcomer-limit> <new-source.json>",
    )?);
    let limit = args
        .next()
        .context("v37-full-pool-source requires newcomer-limit")?
        .to_string_lossy()
        .parse::<usize>()
        .map_err(|_| anyhow!("newcomer-limit must be an unsigned integer"))?;
    let output_path = PathBuf::from(args.next().context(
        "usage: v37-full-pool-source <source.json> <evaluated-members.jsonl> <candidate-panel-bundles.jsonl> <newcomer-limit> <new-source.json>",
    )?);
    ensure!(
        args.next().is_none(),
        "expected exactly five positional arguments"
    );
    ensure!(source_path.is_file() && members_path.is_file() && bundles_path.is_file());
    ensure!(
        !output_path.exists(),
        "output source must not already exist"
    );

    let source = read_canonical_json(&source_path, "source")?;
    let members = read_canonical_jsonl(&members_path, "evaluated members")?;
    let bundles = read_canonical_jsonl(&bundles_path, "candidate panel bundles")?;
    let output =
        rebuild_fast_ephemeral_source_with_newcomer_limit(source, &members, &bundles, limit)?;
    write_new(&output_path, &output)?;
    print!(
        "{}",
        String::from_utf8(canonical_json_line(&serde_json::json!({
            "schemaVersion":"temporal_qd_v37_full_pool_source_runner_v1",
            "newcomerLimit":limit,
            "sourceSha256":output["sourceSha256"],
            "candidateCount":output["provisional"]["candidateCount"],
        }))?)?
    );
    Ok(())
}
