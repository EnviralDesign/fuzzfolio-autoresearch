use serde_json::Value;
use std::io::{self, BufRead, Write};
use temporal_qd_contract::canonical_json_line;
use temporal_qd_kernel::topology_panel_usefulness_v2::arm_eligibility_projection;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let stdin = io::stdin();
    let mut stdout = io::stdout().lock();
    for line in stdin.lock().lines() {
        let line = line?;
        if line.is_empty() {
            return Err("blank JSONL request".into());
        }
        let request: Value = serde_json::from_str(&line)?;
        let fields = request.as_object().ok_or("request must be an object")?;
        if fields.len() != 3
            || fields.get("schemaVersion").and_then(Value::as_str)
                != Some("temporal_qd_topology_arm_eligibility_request_v2")
            || !fields.contains_key("archivePolicyAuthority")
            || !fields.contains_key("member")
        {
            return Err("arm eligibility request fields are incompatible".into());
        }
        let result =
            arm_eligibility_projection(&fields["member"], &fields["archivePolicyAuthority"])
                .map_err(|error| format!("arm eligibility failed: {error}"))?;
        stdout.write_all(&canonical_json_line(&result)?)?;
    }
    Ok(())
}
