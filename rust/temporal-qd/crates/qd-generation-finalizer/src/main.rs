use std::env;
use std::path::PathBuf;

use anyhow::{Context, Result, ensure};
use temporal_qd_contract::canonical_json_line;
use temporal_qd_generation_finalizer::execute_manifest;

fn main() -> Result<()> {
    let mut args = env::args_os();
    let _binary = args.next();
    let manifest = args.next().context(
        "usage: temporal-qd-generation-finalizer <generation-finalization-manifest.json>",
    )?;
    ensure!(args.next().is_none(), "expected exactly one manifest path");
    let result = execute_manifest(&PathBuf::from(manifest))?;
    print!("{}", String::from_utf8(canonical_json_line(&result)?)?);
    Ok(())
}
