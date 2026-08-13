use std::env;
use std::path::Path;
use std::time::Instant;

use anyhow::{Result, bail};
use temporal_qd_contract::{NativeProgress, NativeProgressSection, NativeProgressSpec};

fn main() {
    let outcome = std::thread::Builder::new()
        .name("temporal-qd-tail-reducer".to_owned())
        .stack_size(32 * 1024 * 1024)
        .spawn(run)
        .and_then(|worker| {
            worker
                .join()
                .map_err(|_| std::io::Error::other("tail reducer worker panicked"))
        });
    let result = match outcome {
        Ok(result) => result,
        Err(error) => Err(error.into()),
    };
    if let Err(error) = result {
        eprintln!("ERROR: {error:#}");
        std::process::exit(2);
    }
    report_peak_working_set();
}

#[cfg(windows)]
fn report_peak_working_set() {
    if env::var_os("TEMPORAL_QD_TAIL_REPORT_PEAK").is_none() {
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
    // SAFETY: both handles and the initialized writable structure satisfy the
    // Win32 API contract for querying the current process.
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

fn run() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 3 || args[1] != "--manifest" {
        bail!("usage: temporal-qd-tail-reducer --manifest PATH");
    }
    let mut spec = NativeProgressSpec::new("tail_reducer", "tail_reduction");
    spec.subphase = "stream_results_and_reduce_survivors".to_owned();
    let progress = NativeProgress::from_environment(spec);
    let handle = progress.handle();
    let started = Instant::now();
    let result = temporal_qd_tail_reducer::execute_manifest(Path::new(&args[2]))?;
    handle.record_section(NativeProgressSection::wall(
        "tail_reduction",
        started.elapsed(),
    ));
    progress.finish(None);
    print!("{}", temporal_qd_contract::canonical_json(&result)?);
    println!();
    Ok(())
}
