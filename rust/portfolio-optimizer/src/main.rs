use std::env;
use std::fs;
use std::process;

use portfolio_optimizer_rs::{OptimizerInput, optimize_input};

fn main() {
    let mut input_path: Option<String> = None;
    let mut pretty = false;
    let mut args = env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--input" => input_path = args.next(),
            "--pretty" => pretty = true,
            "--help" | "-h" => {
                eprintln!("Usage: portfolio-optimizer-rs --input <fixture.json> [--pretty]");
                return;
            }
            other => {
                eprintln!("unknown argument: {other}");
                process::exit(2);
            }
        }
    }
    let Some(path) = input_path else {
        eprintln!("missing --input <fixture.json>");
        process::exit(2);
    };
    let text = match fs::read_to_string(&path) {
        Ok(value) => value,
        Err(error) => {
            eprintln!("failed to read {path}: {error}");
            process::exit(1);
        }
    };
    let input: OptimizerInput = match serde_json::from_str(&text) {
        Ok(value) => value,
        Err(error) => {
            eprintln!("failed to parse {path}: {error}");
            process::exit(1);
        }
    };
    let output = optimize_input(input);
    let rendered = if pretty {
        serde_json::to_string_pretty(&output)
    } else {
        serde_json::to_string(&output)
    };
    match rendered {
        Ok(value) => println!("{value}"),
        Err(error) => {
            eprintln!("failed to serialize optimizer output: {error}");
            process::exit(1);
        }
    }
}
