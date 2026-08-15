use std::{env, fs, path::PathBuf, time::Duration};

use anyhow::{Result, bail, ensure};
use temporal_qd_contract::{NativeProgress, NativeProgressSpec};
use temporal_qd_gateway_dispatch::{
    DEFAULT_MAX_HTTP_RESPONSE_BYTES, DispatchMode, GatewayDispatchRequest, GatewayRuntimeOptions,
    execute_gateway_dispatch_with_progress,
};

fn main() {
    let outcome = std::thread::Builder::new()
        .name("temporal-qd-gateway-dispatch".to_owned())
        .stack_size(16 * 1024 * 1024)
        .spawn(run)
        .and_then(|worker| {
            worker
                .join()
                .map_err(|_| std::io::Error::other("gateway-dispatch worker panicked"))
        });
    if let Err(error) = outcome.unwrap_or_else(|error| Err(error.into())) {
        eprintln!("ERROR: {error:#}");
        std::process::exit(2);
    }
    report_peak_working_set();
}

fn run() -> Result<()> {
    let mut campaign_input_checkpoint = None::<PathBuf>;
    let mut output_root = None::<PathBuf>;
    let mut gateway_url = None::<String>;
    let mut token = None::<String>;
    let mut token_file = None::<PathBuf>;
    let mut mode = None::<DispatchMode>;
    let mut timeout_seconds = 900_u64;
    let mut request_timeout_seconds = 30_u64;
    let mut poll_interval_millis = 250_u64;
    let mut enqueue_batch_size = 128_usize;
    let mut result_batch_size = 128_usize;
    let mut max_request_bytes = 64 * 1024 * 1024_usize;
    let mut max_response_bytes = DEFAULT_MAX_HTTP_RESPONSE_BYTES;
    let mut maintenance_probe_interval_millis = 30_000_u64;
    let mut maintenance_timeout_seconds = 12 * 60 * 60_u64;

    let mut args = env::args().skip(1);
    while let Some(argument) = args.next() {
        let value = |name: &str, args: &mut std::iter::Skip<std::env::Args>| -> Result<String> {
            args.next()
                .ok_or_else(|| anyhow::anyhow!("{name} requires a value"))
        };
        match argument.as_str() {
            "--campaign-input-checkpoint" => {
                campaign_input_checkpoint = Some(PathBuf::from(value(
                    "--campaign-input-checkpoint",
                    &mut args,
                )?))
            }
            "--output-root" => {
                output_root = Some(PathBuf::from(value("--output-root", &mut args)?))
            }
            "--gateway-url" => gateway_url = Some(value("--gateway-url", &mut args)?),
            "--gateway-token" => token = Some(value("--gateway-token", &mut args)?),
            "--gateway-token-file" => {
                token_file = Some(PathBuf::from(value("--gateway-token-file", &mut args)?))
            }
            "--fresh" => set_mode(&mut mode, DispatchMode::Fresh)?,
            "--resume" => set_mode(&mut mode, DispatchMode::Resume)?,
            "--timeout-seconds" => {
                timeout_seconds = parse(&value("--timeout-seconds", &mut args)?, "timeout seconds")?
            }
            "--request-timeout-seconds" => {
                request_timeout_seconds = parse(
                    &value("--request-timeout-seconds", &mut args)?,
                    "request timeout seconds",
                )?
            }
            "--poll-interval-millis" => {
                poll_interval_millis = parse(
                    &value("--poll-interval-millis", &mut args)?,
                    "poll interval milliseconds",
                )?
            }
            "--enqueue-batch-size" => {
                enqueue_batch_size = parse(
                    &value("--enqueue-batch-size", &mut args)?,
                    "enqueue batch size",
                )?
            }
            "--result-batch-size" => {
                result_batch_size = parse(
                    &value("--result-batch-size", &mut args)?,
                    "result batch size",
                )?
            }
            "--max-request-bytes" => {
                max_request_bytes = parse(
                    &value("--max-request-bytes", &mut args)?,
                    "maximum request bytes",
                )?
            }
            "--max-response-bytes" => {
                max_response_bytes = parse(
                    &value("--max-response-bytes", &mut args)?,
                    "maximum response bytes",
                )?
            }
            "--maintenance-probe-interval-millis" => {
                maintenance_probe_interval_millis = parse(
                    &value("--maintenance-probe-interval-millis", &mut args)?,
                    "maintenance probe interval milliseconds",
                )?
            }
            "--maintenance-timeout-seconds" => {
                maintenance_timeout_seconds = parse(
                    &value("--maintenance-timeout-seconds", &mut args)?,
                    "maintenance timeout seconds",
                )?
            }
            "--help" | "-h" => {
                print_usage();
                return Ok(());
            }
            _ => bail!("unknown argument: {argument}"),
        }
    }
    ensure!(
        token.is_none() || token_file.is_none(),
        "provide at most one gateway token source"
    );
    let token = match (token, token_file) {
        (Some(token), None) => Some(token),
        (None, Some(path)) => Some(
            fs::read_to_string(&path)
                .map_err(|error| {
                    anyhow::anyhow!("read gateway token file {}: {error}", path.display())
                })?
                .trim()
                .to_owned(),
        ),
        (None, None) => None,
        (Some(_), Some(_)) => unreachable!(),
    };
    let mut request = GatewayDispatchRequest::bounded(
        campaign_input_checkpoint
            .ok_or_else(|| anyhow::anyhow!("--campaign-input-checkpoint is required"))?,
        output_root.ok_or_else(|| anyhow::anyhow!("--output-root is required"))?,
        mode.ok_or_else(|| anyhow::anyhow!("exactly one of --fresh or --resume is required"))?,
    );
    request.timeout = Duration::from_secs(timeout_seconds);
    request.poll_interval = Duration::from_millis(poll_interval_millis);
    request.enqueue_batch_size = enqueue_batch_size;
    request.result_batch_size = result_batch_size;
    request.max_request_bytes = max_request_bytes;
    request.max_response_bytes = max_response_bytes;
    request.maintenance_probe_interval = Duration::from_millis(maintenance_probe_interval_millis);
    request.maintenance_timeout = Duration::from_secs(maintenance_timeout_seconds);
    let mut runtime = GatewayRuntimeOptions::new(
        gateway_url.ok_or_else(|| anyhow::anyhow!("--gateway-url is required"))?,
        token,
    );
    runtime.request_timeout = Duration::from_secs(request_timeout_seconds);
    let mut spec = NativeProgressSpec::new("gateway_dispatch", "startup");
    spec.subphase = "parse_runtime_and_open_checkpoint".to_owned();
    let progress = NativeProgress::from_environment(spec);
    let handle = progress.handle();
    let result = execute_gateway_dispatch_with_progress(&request, &runtime, Some(&handle))?;
    progress.finish(None);
    println!("{}", temporal_qd_contract::canonical_json(&result)?);
    Ok(())
}

fn set_mode(slot: &mut Option<DispatchMode>, mode: DispatchMode) -> Result<()> {
    ensure!(
        slot.is_none(),
        "exactly one of --fresh or --resume is required"
    );
    *slot = Some(mode);
    Ok(())
}

fn parse<T: std::str::FromStr>(value: &str, label: &str) -> Result<T> {
    value
        .parse()
        .map_err(|_| anyhow::anyhow!("{label} is invalid"))
}

fn print_usage() {
    println!(
        "usage: temporal-qd-gateway-dispatch --campaign-input-checkpoint PATH --output-root PATH --gateway-url URL (--fresh|--resume) [--gateway-token TOKEN | --gateway-token-file PATH] [--timeout-seconds N] [--request-timeout-seconds N] [--poll-interval-millis N] [--enqueue-batch-size N] [--result-batch-size N] [--max-request-bytes N] [--max-response-bytes N] [--maintenance-probe-interval-millis N] [--maintenance-timeout-seconds N]"
    );
}

#[cfg(windows)]
fn report_peak_working_set() {
    if env::var_os("TEMPORAL_QD_GATEWAY_DISPATCH_REPORT_PEAK").is_none() {
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
    if unsafe {
        GetProcessMemoryInfo(
            GetCurrentProcess(),
            &mut counters,
            std::mem::size_of::<PROCESS_MEMORY_COUNTERS>() as u32,
        )
    } != 0
    {
        eprintln!(
            "{{\"peakWorkingSetBytes\":{}}}",
            counters.PeakWorkingSetSize
        );
    }
}

#[cfg(not(windows))]
fn report_peak_working_set() {}
