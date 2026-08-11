use std::env;
use std::path::Path;

use anyhow::{Result, bail};

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
    let result = match args[1].as_str() {
        "--manifest" => temporal_qd_archive_reducer::execute_manifest(Path::new(&args[2]))?,
        "--certify-archive" => {
            temporal_qd_archive_reducer::certify_archive_transport(Path::new(&args[2]))?
        }
        _ => bail!("usage: temporal-qd-archive-reducer (--manifest PATH | --certify-archive PATH)"),
    };
    println!("{}", temporal_qd_contract::canonical_json(&result)?);
    Ok(())
}
