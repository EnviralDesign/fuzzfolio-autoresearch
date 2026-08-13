use std::{env, path::Path, time::Instant};

use anyhow::{Result, bail};
use temporal_qd_contract::{NativeProgress, NativeProgressSection, NativeProgressSpec};

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
    if args.len() != 3 || args[1] != "--campaign-output-manifest" {
        bail!("usage: temporal-qd-campaign-seal --campaign-output-manifest PATH");
    }
    let mut spec = NativeProgressSpec::new("campaign_seal", "campaign_output_commit");
    spec.subphase = "authenticate_results_reduce_tail_and_commit".to_owned();
    let progress = NativeProgress::from_environment(spec);
    let handle = progress.handle();
    let started = Instant::now();
    let result = temporal_qd_campaign_seal::execute_campaign_output_manifest(Path::new(&args[2]))?;
    handle.record_section(NativeProgressSection::wall(
        "campaign_output_commit",
        started.elapsed(),
    ));
    progress.finish(None);
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
