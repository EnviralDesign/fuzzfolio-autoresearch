use std::{env, path::Path};

use anyhow::{Result, bail};

fn main() {
    let outcome = std::thread::Builder::new()
        .name("temporal-qd-campaign-seal".to_owned())
        .stack_size(32 * 1024 * 1024)
        .spawn(run)
        .and_then(|worker| {
            worker
                .join()
                .map_err(|_| std::io::Error::other("campaign seal worker panicked"))
        });
    if let Err(error) = outcome.unwrap_or_else(|error| Err(error.into())) {
        eprintln!("ERROR: {error:#}");
        std::process::exit(2);
    }
    report_peak_working_set();
}

fn run() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 {
        bail!("usage: temporal-qd-campaign-seal (--manifest|--build-source-manifest) PATH");
    }
    let result = match args[1].as_str() {
        "--manifest" => temporal_qd_campaign_seal::execute_manifest(Path::new(&args[2]))?,
        "--build-source-manifest" => {
            temporal_qd_campaign_seal::build_source_manifest(Path::new(&args[2]))?
        }
        _ => bail!("usage: temporal-qd-campaign-seal (--manifest|--build-source-manifest) PATH"),
    };
    println!("{}", temporal_qd_contract::canonical_json(&result)?);
    Ok(())
}

#[cfg(windows)]
fn report_peak_working_set() {
    if env::var_os("TEMPORAL_QD_SEAL_REPORT_PEAK").is_none() {
        return;
    }
    use windows_sys::Win32::System::ProcessStatus::{
        GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS,
    };
    use windows_sys::Win32::System::Threading::GetCurrentProcess;
    let mut counters = PROCESS_MEMORY_COUNTERS {
        cb: std::mem::size_of::<PROCESS_MEMORY_COUNTERS>() as u32,
        ..Default::default()
    };
    let ok = unsafe {
        GetProcessMemoryInfo(
            GetCurrentProcess(),
            &mut counters,
            std::mem::size_of::<PROCESS_MEMORY_COUNTERS>() as u32,
        )
    };
    if ok != 0 {
        eprintln!(
            "{{\"peakWorkingSetBytes\":{}}}",
            counters.PeakWorkingSetSize
        );
    }
}

#[cfg(not(windows))]
fn report_peak_working_set() {}
