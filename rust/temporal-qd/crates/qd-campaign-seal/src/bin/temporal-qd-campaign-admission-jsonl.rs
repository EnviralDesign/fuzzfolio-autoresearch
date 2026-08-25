use anyhow::{Context, Result, ensure};
use serde_json::{Value, json};
use std::io::{self, BufRead, Write};
use temporal_qd_campaign_seal::{
    CandidateWindowResultAdmission, admit_candidate_window_task_result,
};

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
            .context("campaign admission request must be an object")?;
        ensure!(
            map.len() == 3
                && map.get("schemaVersion").and_then(Value::as_str)
                    == Some("temporal_qd_campaign_admission_request_v1"),
            "campaign admission request schema/fields are incompatible"
        );
        let task = map
            .get("task")
            .context("campaign admission task is missing")?;
        let result = map
            .get("result")
            .context("campaign admission result is missing")?;
        let disposition = match admit_candidate_window_task_result(task, result)? {
            CandidateWindowResultAdmission::Admitted => "admitted",
            CandidateWindowResultAdmission::Rejected => "rejected",
        };
        serde_json::to_writer(
            &mut stdout,
            &json!({
                "schemaVersion": "temporal_qd_campaign_admission_response_v1",
                "disposition": disposition
            }),
        )?;
        stdout.write_all(b"\n")?;
        stdout.flush()?;
    }
    Ok(())
}
