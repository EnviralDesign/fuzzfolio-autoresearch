use std::env;
use std::path::PathBuf;
use std::time::Instant;

use anyhow::{Context, Result, ensure};
use temporal_qd_contract::{
    NativeProgress, NativeProgressSection, NativeProgressSpec, canonical_json_line,
};
use temporal_qd_generation_finalizer::execute_manifest;

fn main() -> Result<()> {
    let mut args = env::args_os();
    let _binary = args.next();
    let manifest = args.next().context(
        "usage: temporal-qd-generation-finalizer <generation-finalization-manifest.json>",
    )?;
    ensure!(args.next().is_none(), "expected exactly one manifest path");
    let mut spec = NativeProgressSpec::new("generation_finalizer", "generation_finalization");
    spec.subphase = "apply_selection_and_commit_generation".to_owned();
    let progress = NativeProgress::from_environment(spec);
    let handle = progress.handle();
    let started = Instant::now();
    let result = execute_manifest(&PathBuf::from(manifest))?;
    handle.record_section(NativeProgressSection::wall(
        "generation_finalization",
        started.elapsed(),
    ));
    progress.finish(None);
    print!("{}", String::from_utf8(canonical_json_line(&result)?)?);
    Ok(())
}
