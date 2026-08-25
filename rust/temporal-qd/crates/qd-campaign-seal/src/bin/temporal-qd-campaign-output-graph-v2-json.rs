use anyhow::{Context, Result, ensure};
use std::env;
use std::io::{self, Write};
use std::path::Path;
use temporal_qd_campaign_seal::authenticate_campaign_output_graph_v2;
use temporal_qd_contract::canonical_json_line;

fn main() -> Result<()> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    ensure!(
        args.len() == 2 && args[0] == "--campaign-output-checkpoint",
        "usage: temporal-qd-campaign-output-graph-v2-json --campaign-output-checkpoint PATH"
    );
    let graph = authenticate_campaign_output_graph_v2(Path::new(&args[1]))
        .with_context(|| format!("authenticate V2 campaign-output graph: {}", args[1]))?;
    io::stdout()
        .lock()
        .write_all(&canonical_json_line(&graph)?)?;
    Ok(())
}
