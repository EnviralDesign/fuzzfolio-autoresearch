use anyhow::{Result, bail};
use std::{env, path::Path};

fn main() {
    let outcome = std::thread::Builder::new()
        .name("temporal-qd-campaign-freeze".to_owned())
        .stack_size(16 * 1024 * 1024)
        .spawn(run)
        .and_then(|worker| {
            worker
                .join()
                .map_err(|_| std::io::Error::other("campaign-freeze worker panicked"))
        });
    if let Err(error) = outcome.unwrap_or_else(|error| Err(error.into())) {
        eprintln!("ERROR: {error:#}");
        std::process::exit(2);
    }
    report_peak_working_set();
}
fn run() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 || args[1] != "--manifest" {
        bail!("usage: temporal-qd-campaign-freeze --manifest PATH");
    }
    println!(
        "{}",
        temporal_qd_contract::canonical_json(&temporal_qd_campaign_freeze::execute_manifest(
            Path::new(&args[2])
        )?)?
    );
    Ok(())
}
#[cfg(windows)]
fn report_peak_working_set() {
    if env::var_os("TEMPORAL_QD_CAMPAIGN_FREEZE_REPORT_PEAK").is_none() {
        return;
    }
    use windows_sys::Win32::System::ProcessStatus::{
        GetProcessMemoryInfo, PROCESS_MEMORY_COUNTERS,
    };
    use windows_sys::Win32::System::Threading::GetCurrentProcess;
    let mut c = PROCESS_MEMORY_COUNTERS {
        cb: std::mem::size_of::<PROCESS_MEMORY_COUNTERS>() as u32,
        ..Default::default()
    };
    if unsafe {
        GetProcessMemoryInfo(
            GetCurrentProcess(),
            &mut c,
            std::mem::size_of::<PROCESS_MEMORY_COUNTERS>() as u32,
        )
    } != 0
    {
        eprintln!("{{\"peakWorkingSetBytes\":{}}}", c.PeakWorkingSetSize);
    }
}
#[cfg(not(windows))]
fn report_peak_working_set() {}
