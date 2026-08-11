use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use tempfile::tempdir;
use temporal_qd_contract::{CONTRACT_VERSION, canonical_json_line, canonical_sha256};
use temporal_qd_generation_finalizer::{
    ARCHIVE_PATH, CHECKPOINT_PATH, COMMIT_PATH, COMMIT_SCHEMA, CUMULATIVE_PATH, FUNNEL_PATH,
    FUNNEL_SNAPSHOT_PATH, LEDGER_PATH, MANIFEST_SCHEMA, OPERATION, RECORD_PATH, STATE_PATCH_PATH,
};

const HASH_A: &str = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
const HASH_B: &str = "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";

fn add_self_hash(value: &mut Value, field: &str) {
    let digest = canonical_sha256(value).unwrap();
    value[field] = json!(digest);
}

fn file_sha256(path: &Path) -> String {
    let mut file = File::open(path).unwrap();
    let mut digest = Sha256::new();
    let mut buffer = vec![0_u8; 64 * 1024];
    loop {
        let read = file.read(&mut buffer).unwrap();
        if read == 0 {
            break;
        }
        digest.update(&buffer[..read]);
    }
    format!("sha256:{:x}", digest.finalize())
}

fn compact_fixture(root: &Path, sizes: [u64; 8]) -> PathBuf {
    let mut manifest = json!({
        "schemaVersion": MANIFEST_SCHEMA,
        "contractVersion": CONTRACT_VERSION,
        "operation": OPERATION,
        "runtimeAuthoritySha256": HASH_B,
        "semanticAuthoritySha256": HASH_A,
        "sourcePath": root.join("absent-source.json").to_string_lossy(),
        "sourceSha256": HASH_A,
        "resultPath": COMMIT_PATH,
    });
    add_self_hash(&mut manifest, "manifestSha256");
    let manifest_path = root.join("manifest.json");
    fs::write(&manifest_path, canonical_json_line(&manifest).unwrap()).unwrap();

    let mut commit = json!({
        "schemaVersion": COMMIT_SCHEMA,
        "contractVersion": CONTRACT_VERSION,
        "sourceSha256": HASH_A,
        "manifestSha256": manifest["manifestSha256"],
        "runtimeAuthoritySha256": HASH_B,
        "semanticAuthoritySha256": HASH_A,
        "generationIndex": 1,
        "auxiliaryPlanSha256": HASH_B,
    });
    for ((descriptor_name, output_path), size) in [
        ("cumulativeArchive", CUMULATIVE_PATH),
        ("parentArchive", ARCHIVE_PATH),
        ("generationFunnel", FUNNEL_PATH),
        ("generationFunnelSnapshot", FUNNEL_SNAPSHOT_PATH),
        ("checkpoint", CHECKPOINT_PATH),
        ("ledger", LEDGER_PATH),
        ("generationRecord", RECORD_PATH),
        ("statePatch", STATE_PATCH_PATH),
    ]
    .into_iter()
    .zip(sizes)
    {
        let output = root.join(output_path);
        fs::create_dir_all(output.parent().unwrap()).unwrap();
        if size == 3 {
            fs::write(&output, b"{}\n").unwrap();
        } else {
            File::create(&output).unwrap().set_len(size).unwrap();
        }
        commit[descriptor_name] = json!({
            "path": output_path,
            "bytes": size,
            "fileSha256": file_sha256(&output),
        });
    }
    add_self_hash(&mut commit, "commitSha256");
    fs::write(
        root.join(COMMIT_PATH),
        canonical_json_line(&commit).unwrap(),
    )
    .unwrap();
    manifest_path
}

#[test]
fn release_binary_reopens_compact_commit_and_hashes_bound_outputs() {
    // This is intentionally a process-level test. On Windows, the release
    // binary's main-thread stack is smaller than Rust test-worker stacks, so
    // calling the library in-process would not catch a large local hash buffer.
    let root = tempdir().unwrap();
    let manifest_path = compact_fixture(root.path(), [3; 8]);

    let output = Command::new(env!("CARGO_BIN_EXE_temporal-qd-generation-finalizer"))
        .arg(&manifest_path)
        .output()
        .unwrap();
    assert!(
        output.status.success(),
        "release binary failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let execution: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(execution["restart"], true);
    assert_eq!(
        execution["restartValidation"],
        "compact_commit_and_output_hashes"
    );
    assert_eq!(execution["commit"]["runtimeAuthoritySha256"], HASH_B);
}

#[test]
#[ignore = "manual representative-size compact restart benchmark"]
fn benchmark_release_binary_hashes_representative_generation_outputs() {
    let root = tempdir().unwrap();
    // G1's authoritative archive/cumulative/funnel sizes, with conservative
    // allowances for the four smaller finalizer-only boundary files.
    let manifest_path = compact_fixture(
        root.path(),
        [
            1_912_690, 96_705_013, 4_149_700, 1_000_000, 5_843, 41_205, 1_000_000, 1_000_000,
        ],
    );
    let started = Instant::now();
    let output = Command::new(env!("CARGO_BIN_EXE_temporal-qd-generation-finalizer"))
        .arg(&manifest_path)
        .output()
        .unwrap();
    let elapsed = started.elapsed();
    assert!(
        output.status.success(),
        "release binary failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let execution: Value = serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(
        execution["restartValidation"],
        "compact_commit_and_output_hashes"
    );
    eprintln!("representative compact restart wall time: {elapsed:?}");
}
