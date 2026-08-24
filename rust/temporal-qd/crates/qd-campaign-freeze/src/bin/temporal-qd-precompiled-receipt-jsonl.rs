use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::io::{self, BufRead, Write};
use temporal_qd_campaign_freeze::validate_precompiled_execution_receipt;

fn main() -> Result<()> {
    let stdin = io::stdin();
    let mut stdout = io::BufWriter::new(io::stdout().lock());
    for (index, line) in stdin.lock().lines().enumerate() {
        let line = line.with_context(|| format!("read request line {}", index + 1))?;
        if line.trim().is_empty() {
            continue;
        }
        let request: Value = serde_json::from_str(&line)
            .with_context(|| format!("parse request line {}", index + 1))?;
        let map = request
            .as_object()
            .context("precompiled receipt request must be an object")?;
        ensure!(
            map.len() == 3
                && map.get("schemaVersion").and_then(Value::as_str)
                    == Some("temporal_qd_precompiled_receipt_admission_request_v1"),
            "precompiled receipt request schema/fields are incompatible"
        );
        let task = map
            .get("task")
            .context("precompiled receipt task is missing")?;
        let result = map
            .get("result")
            .context("precompiled receipt result is missing")?;
        validate_precompiled_execution_receipt(task, result)?;
        serde_json::to_writer(
            &mut stdout,
            &json!({
                "schemaVersion": "temporal_qd_precompiled_receipt_admission_response_v1",
                "status": "accepted"
            }),
        )?;
        stdout.write_all(b"\n")?;
        stdout.flush()?;
    }
    Ok(())
}
