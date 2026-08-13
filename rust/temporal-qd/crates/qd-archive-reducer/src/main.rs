use std::env;
use std::path::Path;
use std::time::Instant;

use anyhow::{Result, bail};
use temporal_qd_contract::{NativeProgress, NativeProgressSection, NativeProgressSpec};

fn main() {
    if let Err(error) = run() {
        eprintln!("ERROR: {error:#}");
        std::process::exit(2);
    }
}

fn run() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        bail!("usage: temporal-qd-archive-reducer (--manifest PATH | --certify-archive PATH)");
    }
    let phase = if args[1] == "--certify-archive" {
        "archive_certification"
    } else {
        "archive_reduction"
    };
    let mut spec = NativeProgressSpec::new("archive_reducer", phase);
    spec.subphase = "execute_current_v5_archive_path".to_owned();
    let progress = NativeProgress::from_environment(spec);
    let handle = progress.handle();
    let started = Instant::now();
    let result = match args[1].as_str() {
        "--manifest" => temporal_qd_archive_reducer::execute_manifest(Path::new(&args[2]))?,
        "--certify-archive" => {
            temporal_qd_archive_reducer::certify_archive_transport(Path::new(&args[2]))?
        }
        _ => bail!("usage: temporal-qd-archive-reducer (--manifest PATH | --certify-archive PATH)"),
    };
    handle.record_section(NativeProgressSection::wall(phase, started.elapsed()));
    progress.finish(None);
    println!("{}", temporal_qd_contract::canonical_json(&result)?);
    Ok(())
}
