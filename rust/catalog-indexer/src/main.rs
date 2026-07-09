use rayon::prelude::*;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::UNIX_EPOCH;

#[derive(Debug, Deserialize)]
struct RunInput {
    run_id: String,
    path: String,
}

#[derive(Debug, Deserialize)]
struct ScanInput {
    db_path: String,
    schema_version: i64,
    extraction_version: String,
    run_dirs: Vec<RunInput>,
}

#[derive(Debug, Serialize)]
struct ReusableRun {
    run_id: String,
    row_count: i64,
}

#[derive(Debug, Serialize)]
struct MigrationRun {
    run_id: String,
    row_count: i64,
    from_extraction_version: String,
}

#[derive(Debug, Serialize)]
struct ScanOutput {
    ok: bool,
    backend: &'static str,
    scanned_run_count: usize,
    existing_signature_count: usize,
    reusable_runs: Vec<ReusableRun>,
    migration_runs: Vec<MigrationRun>,
    invalid_run_count: usize,
    missing_signature_count: usize,
    stale_signature_count: usize,
}

#[derive(Debug)]
struct ExistingSignature {
    signature_json: String,
    row_count: i64,
}

#[derive(Debug)]
struct RunScanResult {
    reusable: Option<ReusableRun>,
    migration: Option<MigrationRun>,
    invalid: bool,
    missing: bool,
}

fn file_signature(path: &Path, display_path: String) -> Value {
    match fs::metadata(path) {
        Ok(metadata) => {
            let mtime_ns = metadata
                .modified()
                .ok()
                .and_then(|mtime| mtime.duration_since(UNIX_EPOCH).ok())
                .and_then(|duration| i64::try_from(duration.as_nanos()).ok());
            json!({
                "path": display_path,
                "exists": true,
                "size": i64::try_from(metadata.len()).unwrap_or(i64::MAX),
                "mtime_ns": mtime_ns,
            })
        }
        Err(_) => {
            json!({
                "path": display_path,
                "exists": false,
                "size": 0,
                "mtime_ns": null,
            })
        }
    }
}

fn source_signature(run_dir: &Path, run_dir_display: &str) -> Value {
    let attempts_display = format!(
        "{}{}attempts.jsonl",
        run_dir_display,
        std::path::MAIN_SEPARATOR
    );
    let metadata_display = format!(
        "{}{}run-metadata.json",
        run_dir_display,
        std::path::MAIN_SEPARATOR
    );
    json!({
        "attempts": file_signature(&run_dir.join("attempts.jsonl"), attempts_display),
        "run_metadata": file_signature(&run_dir.join("run-metadata.json"), metadata_display),
    })
}

fn artifact_signatures(existing_signature: &Value) -> Option<Vec<Value>> {
    let artifacts = existing_signature.get("artifacts")?.as_array()?;
    let mut signatures = Vec::with_capacity(artifacts.len());
    for artifact in artifacts {
        let path = artifact.get("path")?.as_str()?.trim();
        if path.is_empty() {
            return None;
        }
        signatures.push(file_signature(Path::new(path), path.to_string()));
    }
    Some(signatures)
}

fn rebuilt_signature(
    input: &ScanInput,
    run: &RunInput,
    existing_signature: &Value,
) -> Option<Value> {
    let artifacts = artifact_signatures(existing_signature)?;
    Some(json!({
        "schema_version": input.schema_version,
        "extraction_version": input.extraction_version,
        "run_id": run.run_id,
        "sources": source_signature(Path::new(&run.path), &run.path),
        "artifacts": artifacts,
    }))
}

fn signature_is_reusable(input: &ScanInput, run: &RunInput, existing_json: &str) -> bool {
    let Ok(existing_signature) = serde_json::from_str::<Value>(existing_json) else {
        return false;
    };
    if existing_signature
        .get("schema_version")
        .and_then(Value::as_i64)
        != Some(input.schema_version)
    {
        return false;
    }
    if existing_signature
        .get("extraction_version")
        .and_then(Value::as_str)
        != Some(input.extraction_version.as_str())
    {
        return false;
    }
    if existing_signature.get("run_id").and_then(Value::as_str) != Some(run.run_id.as_str()) {
        return false;
    }
    let Some(current_signature) = rebuilt_signature(input, run, &existing_signature) else {
        return false;
    };
    current_signature == existing_signature
}

fn signature_is_migration_candidate(
    input: &ScanInput,
    run: &RunInput,
    existing_json: &str,
) -> Option<String> {
    let Ok(existing_signature) = serde_json::from_str::<Value>(existing_json) else {
        return None;
    };
    if existing_signature
        .get("schema_version")
        .and_then(Value::as_i64)
        != Some(input.schema_version)
    {
        return None;
    }
    let from_extraction_version = existing_signature
        .get("extraction_version")
        .and_then(Value::as_str)?;
    if from_extraction_version == input.extraction_version {
        return None;
    }
    if existing_signature.get("run_id").and_then(Value::as_str) != Some(run.run_id.as_str()) {
        return None;
    }
    let mut candidate = existing_signature.clone();
    if let Value::Object(ref mut object) = candidate {
        object.insert(
            "extraction_version".to_string(),
            Value::String(input.extraction_version.clone()),
        );
    } else {
        return None;
    }
    let current_signature = rebuilt_signature(input, run, &candidate)?;
    if current_signature == candidate {
        Some(from_extraction_version.to_string())
    } else {
        None
    }
}

fn existing_signature_count(db_path: &Path) -> Result<usize, String> {
    let conn = Connection::open(db_path).map_err(|err| format!("open sqlite: {err}"))?;
    let count = conn
        .query_row("SELECT COUNT(*) FROM run_signatures", params![], |row| {
            row.get::<_, i64>(0)
        })
        .map_err(|err| format!("count signatures: {err}"))?;
    Ok(usize::try_from(count).unwrap_or(usize::MAX))
}

fn load_existing_signature(
    conn: &Connection,
    run_id: &str,
) -> Result<Option<ExistingSignature>, String> {
    conn.query_row(
        "SELECT signature_json, row_count FROM run_signatures WHERE run_id = ?1",
        params![run_id],
        |row| {
            Ok(ExistingSignature {
                signature_json: row.get::<_, String>(0)?,
                row_count: row.get::<_, i64>(1)?,
            })
        },
    )
    .optional()
    .map_err(|err| format!("load signature for {run_id}: {err}"))
}

fn scan(input: ScanInput) -> Result<ScanOutput, String> {
    let existing_signature_count = existing_signature_count(Path::new(&input.db_path))?;
    let db_path = input.db_path.clone();
    let results: Vec<RunScanResult> = input
        .run_dirs
        .par_iter()
        .map_init(
            || Connection::open(&db_path),
            |connection, run| -> Result<RunScanResult, String> {
                let connection = connection
                    .as_ref()
                    .map_err(|err| format!("open sqlite worker connection: {err}"))?;
                let existing = load_existing_signature(connection, &run.run_id)?;
                Ok(match existing.as_ref() {
                    Some(signature)
                        if signature_is_reusable(&input, run, &signature.signature_json) =>
                    {
                        RunScanResult {
                            reusable: Some(ReusableRun {
                                run_id: run.run_id.clone(),
                                row_count: signature.row_count,
                            }),
                            migration: None,
                            invalid: false,
                            missing: false,
                        }
                    }
                    Some(signature) => {
                        if let Some(from_extraction_version) =
                            signature_is_migration_candidate(&input, run, &signature.signature_json)
                        {
                            RunScanResult {
                                reusable: None,
                                migration: Some(MigrationRun {
                                    run_id: run.run_id.clone(),
                                    row_count: signature.row_count,
                                    from_extraction_version,
                                }),
                                invalid: false,
                                missing: false,
                            }
                        } else {
                            RunScanResult {
                                reusable: None,
                                migration: None,
                                invalid: true,
                                missing: false,
                            }
                        }
                    }
                    None => RunScanResult {
                        reusable: None,
                        migration: None,
                        invalid: false,
                        missing: true,
                    },
                })
            },
        )
        .collect::<Result<Vec<_>, _>>()?;
    let invalid_run_count = results.iter().filter(|result| result.invalid).count();
    let missing_signature_count = results.iter().filter(|result| result.missing).count();
    let matched_signature_count = results.iter().filter(|result| !result.missing).count();
    let migration_runs: Vec<MigrationRun> = results
        .iter()
        .filter_map(|result| {
            result.migration.as_ref().map(|migration| MigrationRun {
                run_id: migration.run_id.clone(),
                row_count: migration.row_count,
                from_extraction_version: migration.from_extraction_version.clone(),
            })
        })
        .collect();
    let reusable_runs: Vec<ReusableRun> = results
        .into_iter()
        .filter_map(|result| result.reusable)
        .collect();

    let stale_signature_count = existing_signature_count.saturating_sub(matched_signature_count);

    Ok(ScanOutput {
        ok: true,
        backend: "rust",
        scanned_run_count: input.run_dirs.len(),
        existing_signature_count,
        reusable_runs,
        migration_runs,
        invalid_run_count,
        missing_signature_count,
        stale_signature_count,
    })
}

fn main() {
    let mut args = env::args().skip(1);
    let Some(input_path) = args.next() else {
        eprintln!("usage: catalog-indexer-rs <scan-input.json>");
        std::process::exit(2);
    };
    let input_text = match fs::read_to_string(PathBuf::from(input_path)) {
        Ok(text) => text,
        Err(err) => {
            eprintln!("read input: {err}");
            std::process::exit(2);
        }
    };
    let input = match serde_json::from_str::<ScanInput>(&input_text) {
        Ok(input) => input,
        Err(err) => {
            eprintln!("parse input: {err}");
            std::process::exit(2);
        }
    };
    match scan(input) {
        Ok(output) => {
            println!(
                "{}",
                serde_json::to_string(&output).expect("serialize output")
            );
        }
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(1);
        }
    }
}
