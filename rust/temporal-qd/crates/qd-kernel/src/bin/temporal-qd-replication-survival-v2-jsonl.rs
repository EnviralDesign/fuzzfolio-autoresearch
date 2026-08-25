use serde_json::Value;
use std::io::{self, BufRead, Write};
use temporal_qd_contract::canonical_json_line;
use temporal_qd_kernel::topology_replication_survival_v2::evaluate;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut output = io::BufWriter::new(io::stdout().lock());
    for line in io::stdin().lock().lines() {
        let request: Value = serde_json::from_str(&line?)?;
        let map = request
            .as_object()
            .ok_or("replication request must be an object")?;
        if map.len() != 3
            || map.get("schemaVersion").and_then(Value::as_str)
                != Some("temporal_qd_topology_replication_request_v2")
        {
            return Err("replication request schema/fields are incompatible".into());
        }
        let inputs = map
            .get("inputs")
            .and_then(Value::as_object)
            .ok_or("replication inputs must be an object")?;
        let read = |panel: &str| inputs.get(panel).and_then(Value::as_bool);
        let identities_valid = map
            .get("identitiesValid")
            .and_then(Value::as_bool)
            .ok_or("identitiesValid must be Boolean")?;
        let result = evaluate(
            read("panel-3"),
            read("panel-1"),
            read("panel-2"),
            identities_valid,
        );
        output.write_all(&canonical_json_line(&result)?)?;
    }
    Ok(())
}
