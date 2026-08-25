use serde_json::Value;
use std::io::{self, BufRead, Write};
use temporal_qd_contract::canonical_json_line;
use temporal_qd_kernel::topology_panel_usefulness_v2::{
    arm_eligibility_projection, evaluate_panel_usefulness_v2, evaluate_replication_survival_v3,
};

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
        let result = match fields.get("schemaVersion").and_then(Value::as_str) {
            Some("temporal_qd_topology_arm_eligibility_request_v2")
                if fields.len() == 3
                    && fields.contains_key("archivePolicyAuthority")
                    && fields.contains_key("member") =>
            {
                arm_eligibility_projection(&fields["member"], &fields["archivePolicyAuthority"])
                    .map_err(|error| format!("arm eligibility failed: {error}"))?
            }
            Some("temporal_qd_topology_panel_usefulness_request_v2")
                if fields.len() == 2 && fields.contains_key("arms") =>
            {
                evaluate_panel_usefulness_v2(&fields["arms"])
                    .map_err(|error| format!("panel usefulness failed: {error}"))?
            }
            Some("temporal_qd_topology_replication_survival_request_v3")
                if fields.len() == 3
                    && fields.contains_key("panelUsefulProgressiveInnovationV2")
                    && fields.contains_key("identitiesValid") =>
            {
                let panels = fields["panelUsefulProgressiveInnovationV2"]
                    .as_object()
                    .ok_or("panel usefulness values must be an object")?;
                let read = |panel: &str| panels.get(panel).and_then(Value::as_bool);
                evaluate_replication_survival_v3(
                    read("panel-3"),
                    read("panel-1"),
                    read("panel-2"),
                    fields["identitiesValid"]
                        .as_bool()
                        .ok_or("identitiesValid must be Boolean")?,
                )
            }
            _ => return Err("topology policy request fields are incompatible".into()),
        };
        stdout.write_all(&canonical_json_line(&result)?)?;
    }
    Ok(())
}
