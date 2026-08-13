use std::{path::PathBuf, time::Instant};

use temporal_qd_contract::{NativeProgress, NativeProgressSection, NativeProgressSpec};

fn main() {
    let mut args = std::env::args_os().skip(1);
    let Some(first) = args.next() else {
        eprintln!(
            "usage: temporal-qd-rotating-prefinalizer <manifest.json> | extract-core-funnel <input.json> <attempts.jsonl> | extract-attempt-receipt <input.json> <attempts.jsonl> <receipt.json> | extract-g0-selected-attempts <chain-input.json> <attempts.jsonl> <receipt.json> | assemble-funnel <input.json> <source.json> | build-panel-receipt <input.json> <receipt.json> | build-panel-bundle-sidecar <input.json> <bundles.jsonl> <receipt.json>"
        );
        std::process::exit(2);
    };
    let command = first.to_string_lossy().into_owned();
    let mut spec = NativeProgressSpec::new("rotating_prefinalizer", "prefinalizer_and_panel_path");
    spec.subphase = command.clone();
    let progress = NativeProgress::from_environment(spec);
    let progress_handle = progress.handle();
    let progress_started = Instant::now();
    let result = if first == "extract-core-funnel" {
        let (Some(input), Some(output)) = (args.next(), args.next()) else {
            eprintln!(
                "usage: temporal-qd-rotating-prefinalizer extract-core-funnel <input.json> <attempts.jsonl>"
            );
            std::process::exit(2);
        };
        if args.next().is_some() {
            eprintln!(
                "usage: temporal-qd-rotating-prefinalizer extract-core-funnel <input.json> <attempts.jsonl>"
            );
            std::process::exit(2);
        }
        temporal_qd_rotating_prefinalizer::core_receipt::extract_to_path(
            &PathBuf::from(input),
            &PathBuf::from(output),
        )
    } else if first == "extract-evolved-attempts" {
        let (Some(adapter), Some(output)) = (args.next(), args.next()) else {
            eprintln!(
                "usage: temporal-qd-rotating-prefinalizer extract-evolved-attempts <adapter.json> <attempts.jsonl>"
            );
            std::process::exit(2);
        };
        if args.next().is_some() {
            eprintln!(
                "usage: temporal-qd-rotating-prefinalizer extract-evolved-attempts <adapter.json> <attempts.jsonl>"
            );
            std::process::exit(2);
        }
        temporal_qd_rotating_prefinalizer::core_receipt::extract_evolved_adapter_to_path(
            &PathBuf::from(adapter),
            &PathBuf::from(output),
        )
    } else if first == "extract-evolved-chain-attempts" {
        let (Some(input), Some(output)) = (args.next(), args.next()) else {
            eprintln!(
                "usage: temporal-qd-rotating-prefinalizer extract-evolved-chain-attempts <chain-input.json> <attempts.jsonl>"
            );
            std::process::exit(2);
        };
        if args.next().is_some() {
            eprintln!(
                "usage: temporal-qd-rotating-prefinalizer extract-evolved-chain-attempts <chain-input.json> <attempts.jsonl>"
            );
            std::process::exit(2);
        }
        temporal_qd_rotating_prefinalizer::core_receipt::extract_evolved_chain_to_path(
            &PathBuf::from(input),
            &PathBuf::from(output),
        )
    } else if first == "extract-evolved-chain" {
        let (Some(input), Some(attempts), Some(receipt)) = (args.next(), args.next(), args.next())
        else {
            eprintln!(
                "usage: temporal-qd-rotating-prefinalizer extract-evolved-chain <chain-input.json> <attempts.jsonl> <receipt.json>"
            );
            std::process::exit(2);
        };
        if args.next().is_some() {
            eprintln!(
                "usage: temporal-qd-rotating-prefinalizer extract-evolved-chain <chain-input.json> <attempts.jsonl> <receipt.json>"
            );
            std::process::exit(2);
        }
        temporal_qd_rotating_prefinalizer::core_receipt::extract_evolved_chain_to_receipt_path(
            &PathBuf::from(input),
            &PathBuf::from(attempts),
            &PathBuf::from(receipt),
        )
    } else if first == "extract-g0-funnel-source" {
        eprintln!(
            "extract-g0-funnel-source is deprecated; use extract-g0-selected-attempts and assemble-funnel with temporal_qd_v5_native_funnel_reduction_input_v2"
        );
        std::process::exit(2);
    } else if first == "extract-g0-selected-attempts" {
        let (Some(input), Some(attempts), Some(receipt)) = (args.next(), args.next(), args.next())
        else {
            eprintln!(
                "usage: temporal-qd-rotating-prefinalizer extract-g0-selected-attempts <chain-input.json> <attempts.jsonl> <receipt.json>"
            );
            std::process::exit(2);
        };
        if args.next().is_some() {
            eprintln!(
                "usage: temporal-qd-rotating-prefinalizer extract-g0-selected-attempts <chain-input.json> <attempts.jsonl> <receipt.json>"
            );
            std::process::exit(2);
        }
        temporal_qd_rotating_prefinalizer::core_receipt::extract_g0_selected_attempts_to_path(
            &PathBuf::from(input),
            &PathBuf::from(attempts),
            &PathBuf::from(receipt),
        )
    } else if first == "extract-attempt-receipt" {
        let (Some(input), Some(attempts), Some(receipt)) = (args.next(), args.next(), args.next())
        else {
            eprintln!(
                "usage: temporal-qd-rotating-prefinalizer extract-attempt-receipt <input.json> <attempts.jsonl> <receipt.json>"
            );
            std::process::exit(2);
        };
        if args.next().is_some() {
            eprintln!(
                "usage: temporal-qd-rotating-prefinalizer extract-attempt-receipt <input.json> <attempts.jsonl> <receipt.json>"
            );
            std::process::exit(2);
        }
        temporal_qd_rotating_prefinalizer::core_receipt::extract_receipt_to_path(
            &PathBuf::from(input),
            &PathBuf::from(attempts),
            &PathBuf::from(receipt),
        )
    } else if first == "build-campaign-receipt" {
        let (Some(input), Some(output)) = (args.next(), args.next()) else {
            eprintln!(
                "usage: temporal-qd-rotating-prefinalizer build-campaign-receipt <input.json> <receipt.json>"
            );
            std::process::exit(2);
        };
        if args.next().is_some() {
            eprintln!(
                "usage: temporal-qd-rotating-prefinalizer build-campaign-receipt <input.json> <receipt.json>"
            );
            std::process::exit(2);
        }
        temporal_qd_rotating_prefinalizer::campaign_receipt::build_to_path(
            &PathBuf::from(input),
            &PathBuf::from(output),
        )
    } else if first == "assemble-funnel" {
        let (Some(input), Some(output)) = (args.next(), args.next()) else {
            eprintln!(
                "usage: temporal-qd-rotating-prefinalizer assemble-funnel <input.json> <source.json>"
            );
            std::process::exit(2);
        };
        if args.next().is_some() {
            eprintln!(
                "usage: temporal-qd-rotating-prefinalizer assemble-funnel <input.json> <source.json>"
            );
            std::process::exit(2);
        }
        temporal_qd_rotating_prefinalizer::funnel_source::assemble_to_path_compact(
            &PathBuf::from(input),
            &PathBuf::from(output),
        )
    } else if first == "build-panel-receipt" {
        let (Some(input), Some(output)) = (args.next(), args.next()) else {
            eprintln!(
                "usage: temporal-qd-rotating-prefinalizer build-panel-receipt <input.json> <receipt.json>"
            );
            std::process::exit(2);
        };
        if args.next().is_some() {
            eprintln!(
                "usage: temporal-qd-rotating-prefinalizer build-panel-receipt <input.json> <receipt.json>"
            );
            std::process::exit(2);
        }
        temporal_qd_rotating_prefinalizer::panel_receipt::build_to_path(
            &PathBuf::from(input),
            &PathBuf::from(output),
        )
    } else if first == "build-panel-bundle-sidecar" {
        let (Some(input), Some(sidecar), Some(receipt)) = (args.next(), args.next(), args.next())
        else {
            eprintln!(
                "usage: temporal-qd-rotating-prefinalizer build-panel-bundle-sidecar <input.json> <bundles.jsonl> <receipt.json>"
            );
            std::process::exit(2);
        };
        if args.next().is_some() {
            eprintln!(
                "usage: temporal-qd-rotating-prefinalizer build-panel-bundle-sidecar <input.json> <bundles.jsonl> <receipt.json>"
            );
            std::process::exit(2);
        }
        temporal_qd_rotating_prefinalizer::panel_receipt::build_sidecar_to_path(
            &PathBuf::from(input),
            &PathBuf::from(sidecar),
            &PathBuf::from(receipt),
        )
    } else if args.next().is_none() {
        temporal_qd_rotating_prefinalizer::execute_manifest_compact(&PathBuf::from(first))
    } else {
        eprintln!(
            "usage: temporal-qd-rotating-prefinalizer <manifest.json> | extract-core-funnel <input.json> <attempts.jsonl> | extract-attempt-receipt <input.json> <attempts.jsonl> <receipt.json> | extract-g0-selected-attempts <chain-input.json> <attempts.jsonl> <receipt.json> | assemble-funnel <input.json> <source.json> | build-panel-receipt <input.json> <receipt.json> | build-panel-bundle-sidecar <input.json> <bundles.jsonl> <receipt.json>"
        );
        std::process::exit(2);
    };
    match result {
        Ok(value) => {
            progress_handle.record_section(NativeProgressSection::wall(
                command,
                progress_started.elapsed(),
            ));
            progress.finish(None);
            match temporal_qd_contract::canonical_json_line(&value) {
                Ok(bytes) => print!("{}", String::from_utf8_lossy(&bytes)),
                Err(error) => {
                    eprintln!("serialize execution: {error:#}");
                    std::process::exit(1);
                }
            }
        }
        Err(error) => {
            eprintln!("rotating prefinalizer: {error:#}");
            std::process::exit(1);
        }
    }
}
