//! Standalone, one-shot Temporal QD native batch executable.
//!
//! The foundation probe validates a closed manifest and publishes one tiny
//! immutable result. Coarse pair generation composes the admitted kernel and
//! runtime crates behind a second exact, self-hashed manifest.

mod generation_contract;

use std::env;
use std::fs;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use temporal_qd_contract::{
    CONTRACT_VERSION, FoundationResult, JsonNewline, NativeVersion, Value, canonical_json_line,
    canonical_sha256, parse_foundation_manifest, python_pretty_json_line,
};
use temporal_qd_kernel::{
    generation::{GenerateGenerationRequest, generate_generation},
    journal::FinalNewline,
    publication::PublicationPolicy,
    schedule::RotatingParentSchedule,
};
use temporal_qd_runtime::{
    DashboardPairAuthority, RuntimeManifest, RuntimeParentSelector,
    archive_bootstrap_inputs_from_manifest, bootstrap_global_identity_ledger_inputs,
    global_identity_ledger_from_public,
};

use crate::generation_contract::{
    FRONT_GENERATION_PROGRESS_SCHEMA, GENERATION_MANIFEST_SCHEMA, assemble_runtime_manifest_owned,
    build_generation_result, parse_generation_manifest, validate_generation_result,
};

fn main() {
    match run() {
        Ok(()) => {}
        Err(error) => {
            eprintln!("ERROR: {error:#}");
            std::process::exit(2);
        }
    }
}

fn run() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() == 2 && args[1] == "--version-json" {
        return write_stdout_json(&serde_json::to_value(NativeVersion::current())?);
    }
    if args.len() == 3 && args[1] == "--manifest" {
        return execute_manifest(Path::new(&args[2]));
    }
    bail!("usage: temporal-qd-batch --version-json | --manifest PATH")
}

fn execute_manifest(manifest_path: &Path) -> Result<()> {
    let manifest_path = safe_existing_file(manifest_path, "manifest")?;
    let raw = fs::read(&manifest_path)
        .with_context(|| format!("read manifest: {}", manifest_path.display()))?;
    let value: Value = serde_json::from_slice(&raw).context("parse manifest dispatch envelope")?;
    match value.get("schemaVersion").and_then(Value::as_str) {
        Some(GENERATION_MANIFEST_SCHEMA) => execute_generation(&manifest_path, &raw),
        _ => execute_foundation_bytes(&manifest_path, &raw),
    }
}

fn execute_foundation_bytes(manifest_path: &Path, manifest_bytes: &[u8]) -> Result<()> {
    let manifest = parse_foundation_manifest(manifest_bytes)
        .map_err(|error| anyhow!("validate manifest: {error}"))?;
    let parent = manifest_path
        .parent()
        .ok_or_else(|| anyhow!("manifest has no parent directory"))?;
    ensure_safe_existing_directory(parent, "manifest parent")?;
    let result_path = parent.join(&manifest.result_path);
    let result = FoundationResult::from_manifest(&manifest);
    result
        .validate()
        .map_err(|error| anyhow!("validate foundation result: {error}"))?;
    let result_value = serde_json::to_value(&result)?;
    let result_bytes = canonical_json_line(&result_value)
        .map_err(|error| anyhow!("encode canonical foundation result: {error}"))?;
    publish_once(&result_path, &result_bytes)?;
    write_stdout_bytes(&result_bytes)
}

fn execute_generation(manifest_path: &Path, manifest_bytes: &[u8]) -> Result<()> {
    let manifest = parse_generation_manifest(manifest_bytes, CONTRACT_VERSION)
        .context("validate native generation manifest")?;
    let parent = manifest_path
        .parent()
        .ok_or_else(|| anyhow!("generation manifest has no parent directory"))?;
    ensure_safe_existing_directory(parent, "generation manifest parent")?;

    let archive_path = safe_existing_file(
        Path::new(&manifest.parent_archive_path),
        "generation parent archive",
    )?;
    let archive_bytes = read_stable_existing_file(&archive_path, "generation parent archive")?;
    let archive: Value =
        serde_json::from_slice(&archive_bytes).context("parse generation parent archive")?;
    // `RuntimeManifest` retains its own strict archive projection.  The raw
    // file bytes are staging-only and can be substantial for real generations.
    drop(archive_bytes);
    let ledger_path = safe_existing_file(
        Path::new(&manifest.identity_ledger_path),
        "generation identity ledger",
    )?;
    let ledger_bytes = read_stable_existing_file(&ledger_path, "generation identity ledger")?;
    let final_newline = generation_final_newline(&manifest.final_newline);
    let identity_ledger = parse_python_pretty_json_document(
        &ledger_bytes,
        JsonNewline::Lf,
        "generation identity ledger",
    )?;
    let runtime_value = assemble_runtime_manifest_owned(&manifest, archive, identity_ledger)
        .context("assemble runtime manifest from verified path inputs")?;

    let output_root =
        ensure_safe_directory_tree(Path::new(&manifest.output_root), "generation output root")?;
    let runtime = RuntimeManifest::from_owned_value(runtime_value)
        .context("validate concrete runtime manifest")?;
    let mut parents =
        RuntimeParentSelector::from_manifest(&runtime, manifest.allow_empty_quality_bootstrap)
            .context("initialize verified parent selector")?;
    let mut authority = DashboardPairAuthority::from_manifest(&runtime)
        .context("initialize frozen Dashboard pair authority")?;
    let evidence_identity_context = runtime.evidence_identity_context.clone();
    let archive_bootstrap_inputs = archive_bootstrap_inputs_from_manifest(&runtime)
        .context("reduce verified parent archive to identity-ledger inputs")?;
    // Selector, authority, and bootstrap inputs are self-contained.  Consume
    // the validated public ledger and release the full runtime envelope before
    // proposal generation, avoiding both a duplicate ledger and a retained
    // parent archive.
    let mut ledger = global_identity_ledger_from_public(runtime.into_identity_ledger())
        .context("open global identity ledger")?;
    bootstrap_global_identity_ledger_inputs(&mut ledger, archive_bootstrap_inputs)
        .context("bootstrap global identity ledger from verified parent archive")?;
    let request = GenerateGenerationRequest {
        output_root: output_root.clone(),
        final_newline,
        pair_config: manifest.generation_config.clone(),
        config_sha256: manifest.generation_config_sha256.clone(),
        generation_index: manifest
            .generation_config
            .get("generationIndex")
            .and_then(Value::as_u64)
            .expect("generation contract checked generationIndex"),
        target_unique_candidates: manifest.target_unique_candidates,
        max_proposal_attempts: manifest.max_proposal_attempts,
        max_new_proposals: manifest.max_new_proposals,
        parent_schedule: parse_parent_schedule(manifest.parent_schedule.as_ref())?,
        expected_native_authority_sha256: manifest.native_proposal_authority_sha256.clone(),
        publication_policy: parse_publication_policy(&manifest.publication_policy)?,
        g0_evaluation_width: manifest.g0_evaluation_width,
        evidence_identity_context,
        frozen_construction_catalog: manifest.frozen_construction_catalog.clone(),
        factory_construction_policy: manifest
            .generation_config
            .get("immigrantConstructionPolicy")
            .cloned(),
    };
    let kernel_result = generate_generation(&request, &mut authority, &mut parents, &mut ledger)
        .context("execute native generation")?;
    let inner = if kernel_result.get("completed").and_then(Value::as_bool) == Some(true) {
        if kernel_result.get("schemaVersion").and_then(Value::as_str)
            != Some("temporal_qd_front_generation_result_v1")
        {
            bail!("completed kernel generation wrapper has an incompatible schema")
        }
        kernel_result
            .get("pairGenerationResult")
            .cloned()
            .ok_or_else(|| anyhow!("completed kernel generation lacks pairGenerationResult"))?
    } else if kernel_result.get("schemaVersion").and_then(Value::as_str)
        == Some(FRONT_GENERATION_PROGRESS_SCHEMA)
        && kernel_result.get("completed").and_then(Value::as_bool) == Some(false)
    {
        kernel_result
    } else {
        bail!("kernel generation returned an incompatible result")
    };
    let output_ledger_path = output_root.join("identity-ledger.json");
    let output_ledger_bytes = fs::read(&output_ledger_path).with_context(|| {
        format!(
            "read native output identity ledger: {}",
            output_ledger_path.display()
        )
    })?;
    let output_ledger = parse_python_pretty_json_document(
        &output_ledger_bytes,
        JsonNewline::Lf,
        "native output identity ledger",
    )?;
    let output_ledger_sha256 = output_ledger
        .get("ledgerSha256")
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("native output identity ledger lacks ledgerSha256"))?
        .to_owned();
    let mut output_ledger_material = output_ledger.clone();
    output_ledger_material
        .as_object_mut()
        .ok_or_else(|| anyhow!("native output identity ledger must be an object"))?
        .remove("ledgerSha256");
    if canonical_sha256(&output_ledger_material)? != output_ledger_sha256 {
        bail!("native output identity ledger self-hash mismatch")
    }
    replace_mutable_ledger(
        &ledger_path,
        &ledger_bytes,
        &output_ledger_bytes,
        &manifest.identity_ledger_sha256,
    )?;
    let result = build_generation_result(&manifest, inner, output_ledger_sha256)?;
    validate_generation_result(&result.value, &manifest)?;
    let result_bytes = canonical_json_line(&result.value)
        .map_err(|error| anyhow!("encode canonical generation result: {error}"))?;
    publish_once(&parent.join(&manifest.result_path), &result_bytes)?;
    write_stdout_bytes(&result_bytes)
}

fn generation_final_newline(value: &str) -> FinalNewline {
    match value {
        "lf" => FinalNewline::Lf,
        "crlf" => FinalNewline::Crlf,
        _ => unreachable!("generation contract validated finalNewline"),
    }
}

fn parse_parent_schedule(value: Option<&Value>) -> Result<Option<RotatingParentSchedule>> {
    let Some(value) = value else {
        return Ok(None);
    };
    let fields = value
        .as_object()
        .ok_or_else(|| anyhow!("parent schedule must be an object"))?;
    let schedule = RotatingParentSchedule::validated(
        fields
            .get("breederWidth")
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow!("parent schedule breederWidth is invalid"))?,
        fields
            .get("breederParentCount")
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow!("parent schedule breederParentCount is invalid"))?,
        fields
            .get("offspringNumerator")
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow!("parent schedule offspringNumerator is invalid"))?,
        fields
            .get("offspringDenominator")
            .and_then(Value::as_u64)
            .ok_or_else(|| anyhow!("parent schedule offspringDenominator is invalid"))?,
    )
    .context("validate rotating parent schedule")?;
    if fields.get("scheduleSha256").and_then(Value::as_str)
        != Some(schedule.schedule_sha256().as_str())
    {
        bail!("parent schedule identity mismatch")
    }
    Ok(Some(schedule))
}

fn parse_python_pretty_json_document(
    raw: &[u8],
    newline: JsonNewline,
    label: &str,
) -> Result<Value> {
    let value: Value = serde_json::from_slice(raw).with_context(|| format!("parse {label}"))?;
    let expected = python_pretty_json_line(&value, newline)
        .with_context(|| format!("encode exact Python {label}"))?;
    if raw != expected {
        bail!("{label} must be exact Python pretty JSON")
    }
    Ok(value)
}

fn parse_publication_policy(value: &Value) -> Result<PublicationPolicy> {
    let fields = value
        .as_object()
        .ok_or_else(|| anyhow!("publication policy must be an object"))?;
    let required_string = |key: &str| {
        fields
            .get(key)
            .and_then(Value::as_str)
            .map(ToOwned::to_owned)
            .ok_or_else(|| anyhow!("publication policy {key} is invalid"))
    };
    Ok(PublicationPolicy {
        qd_version: required_string("qdVersion")?,
        policy_name: required_string("policyName")?,
        policy_sha256: required_string("policySha256")?,
        pair_policy: fields
            .get("pairPolicy")
            .cloned()
            .ok_or_else(|| anyhow!("publication policy lacks pairPolicy"))?,
        operator_implementation_identity: fields
            .get("operatorImplementationIdentity")
            .cloned()
            .ok_or_else(|| anyhow!("publication policy lacks operatorImplementationIdentity"))?,
        predeclared_evidence_context_sha256: fields
            .get("predeclaredEvidenceContextSha256")
            .filter(|value| !value.is_null())
            .and_then(Value::as_str)
            .map(ToOwned::to_owned),
    })
}

fn write_stdout_json(value: &serde_json::Value) -> Result<()> {
    let bytes =
        canonical_json_line(value).map_err(|error| anyhow!("encode version JSON: {error}"))?;
    write_stdout_bytes(&bytes)
}

fn write_stdout_bytes(bytes: &[u8]) -> Result<()> {
    let mut stdout = io::stdout().lock();
    stdout.write_all(bytes)?;
    stdout.flush()?;
    Ok(())
}

/// Resolve an existing absolute file while rejecting POSIX symlinks and
/// Windows reparse points in every path component.
fn safe_existing_file(path: &Path, label: &str) -> Result<PathBuf> {
    if !path.is_absolute() {
        bail!("{label} path must be absolute")
    }
    ensure_safe_components(path, label, false)?;
    Ok(path.to_path_buf())
}

fn ensure_safe_existing_directory(path: &Path, label: &str) -> Result<()> {
    if !path.is_absolute() {
        bail!("{label} path must be absolute")
    }
    ensure_safe_components(path, label, true)
}

fn ensure_safe_directory_tree(path: &Path, label: &str) -> Result<PathBuf> {
    if !path.is_absolute() {
        bail!("{label} path must be absolute")
    }
    let mut current = PathBuf::new();
    for component in path.components() {
        use std::path::Component;
        match component {
            Component::Prefix(_) | Component::RootDir | Component::Normal(_) => {
                current.push(component.as_os_str())
            }
            Component::CurDir | Component::ParentDir => {
                bail!("{label} path contains an unsafe component")
            }
        }
        match fs::symlink_metadata(&current) {
            Ok(metadata) => {
                if is_link_or_reparse(&metadata) || !metadata.is_dir() {
                    bail!(
                        "{label} contains a non-directory or reparse point: {}",
                        current.display()
                    )
                }
            }
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                fs::create_dir(&current)
                    .with_context(|| format!("create {label} component: {}", current.display()))?;
                let metadata = fs::symlink_metadata(&current)
                    .with_context(|| format!("inspect created {label}: {}", current.display()))?;
                if is_link_or_reparse(&metadata) || !metadata.is_dir() {
                    bail!(
                        "created {label} component is not a real directory: {}",
                        current.display()
                    )
                }
            }
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("inspect {label} component: {}", current.display()));
            }
        }
    }
    Ok(path.to_path_buf())
}

fn ensure_safe_components(path: &Path, label: &str, final_is_directory: bool) -> Result<()> {
    let mut current = PathBuf::new();
    let components: Vec<_> = path.components().collect();
    for (index, component) in components.iter().enumerate() {
        use std::path::Component;
        match component {
            Component::Prefix(_) | Component::RootDir | Component::Normal(_) => {
                current.push(component.as_os_str())
            }
            Component::CurDir | Component::ParentDir => {
                bail!("{label} path contains an unsafe component")
            }
        }
        let metadata = fs::symlink_metadata(&current)
            .with_context(|| format!("inspect {label} component: {}", current.display()))?;
        if is_link_or_reparse(&metadata) {
            bail!(
                "{label} contains a symlink or Windows reparse point: {}",
                current.display()
            )
        }
        let is_final = index + 1 == components.len();
        if (!is_final || final_is_directory) && !metadata.is_dir() {
            bail!(
                "{label} parent is not a real directory: {}",
                current.display()
            )
        }
        if is_final && !final_is_directory && !metadata.is_file() {
            bail!("{label} is not a regular file: {}", current.display())
        }
    }
    Ok(())
}

fn is_link_or_reparse(metadata: &fs::Metadata) -> bool {
    if metadata.file_type().is_symlink() {
        return true;
    }
    #[cfg(windows)]
    {
        use std::os::windows::fs::MetadataExt;
        metadata.file_attributes() & 0x0400 != 0
    }
    #[cfg(not(windows))]
    {
        false
    }
}

const SHARING_RETRY_DELAYS_MS: [u64; 5] = [5, 10, 20, 40, 80];
const TEMPORARY_CREATE_ATTEMPTS: usize = 16;

trait PublicationIo {
    fn create_new(&self, path: &Path) -> io::Result<fs::File>;
    fn write_all(&self, file: &mut fs::File, bytes: &[u8]) -> io::Result<()>;
    fn sync_file(&self, file: &fs::File) -> io::Result<()>;
    fn hard_link(&self, source: &Path, target: &Path) -> io::Result<()>;
    fn remove_file(&self, path: &Path) -> io::Result<()>;
    fn sync_directory(&self, path: &Path) -> io::Result<()>;
    fn is_sharing_violation(&self, error: &io::Error) -> bool;
    fn pause(&self, duration: Duration);
}

struct StdPublicationIo;

impl PublicationIo for StdPublicationIo {
    fn create_new(&self, path: &Path) -> io::Result<fs::File> {
        fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
    }

    fn write_all(&self, file: &mut fs::File, bytes: &[u8]) -> io::Result<()> {
        file.write_all(bytes)
    }

    fn sync_file(&self, file: &fs::File) -> io::Result<()> {
        file.sync_all()
    }

    fn hard_link(&self, source: &Path, target: &Path) -> io::Result<()> {
        fs::hard_link(source, target)
    }

    fn remove_file(&self, path: &Path) -> io::Result<()> {
        fs::remove_file(path)
    }

    fn sync_directory(&self, path: &Path) -> io::Result<()> {
        sync_directory_platform(path)
    }

    fn is_sharing_violation(&self, error: &io::Error) -> bool {
        is_windows_sharing_violation(error)
    }

    fn pause(&self, duration: Duration) {
        std::thread::sleep(duration);
    }
}

/// Persist a newly installed directory entry. POSIX exposes directory fsync
/// directly. Windows requires opening a directory with BACKUP_SEMANTICS; the
/// documented unsupported errors mean the already-fsynced payload is the
/// strongest primitive available on that filesystem and are a safe fallback.
#[cfg(not(windows))]
fn sync_directory_platform(path: &Path) -> io::Result<()> {
    fs::File::open(path)?.sync_all()
}

#[cfg(windows)]
fn sync_directory_platform(path: &Path) -> io::Result<()> {
    use std::os::windows::fs::OpenOptionsExt;

    const GENERIC_READ: u32 = 0x8000_0000;
    const GENERIC_WRITE: u32 = 0x4000_0000;
    const FILE_SHARE_READ: u32 = 0x0000_0001;
    const FILE_SHARE_WRITE: u32 = 0x0000_0002;
    const FILE_SHARE_DELETE: u32 = 0x0000_0004;
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;

    let mut options = fs::OpenOptions::new();
    options
        .access_mode(GENERIC_READ | GENERIC_WRITE)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS);
    let directory = match options.open(path) {
        Ok(directory) => directory,
        Err(error) if is_unsupported_windows_directory_sync(&error) => return Ok(()),
        Err(error) => return Err(error),
    };
    match directory.sync_all() {
        Err(error) if is_unsupported_windows_directory_sync(&error) => Ok(()),
        outcome => outcome,
    }
}

#[cfg(windows)]
fn is_unsupported_windows_directory_sync(error: &io::Error) -> bool {
    matches!(error.raw_os_error(), Some(1 | 5 | 50 | 87))
}

fn is_windows_sharing_violation(error: &io::Error) -> bool {
    #[cfg(windows)]
    {
        matches!(error.raw_os_error(), Some(32 | 33))
    }
    #[cfg(not(windows))]
    {
        let _ = error;
        false
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct FileIdentity(u128, u128);

#[cfg(unix)]
fn identity_from_metadata(metadata: &fs::Metadata) -> FileIdentity {
    use std::os::unix::fs::MetadataExt;
    FileIdentity(metadata.dev() as u128, metadata.ino() as u128)
}

#[cfg(unix)]
fn identity_from_file(file: &fs::File) -> io::Result<FileIdentity> {
    file.metadata()
        .map(|metadata| identity_from_metadata(&metadata))
}

#[cfg(unix)]
fn identity_from_path(path: &Path) -> io::Result<FileIdentity> {
    fs::symlink_metadata(path).map(|metadata| identity_from_metadata(&metadata))
}

#[cfg(windows)]
fn identity_from_file(file: &fs::File) -> io::Result<FileIdentity> {
    use std::ffi::c_void;
    use std::mem::MaybeUninit;
    use std::os::windows::io::AsRawHandle;

    #[repr(C)]
    struct FileTime {
        low: u32,
        high: u32,
    }

    #[repr(C)]
    struct ByHandleFileInformation {
        attributes: u32,
        creation_time: FileTime,
        last_access_time: FileTime,
        last_write_time: FileTime,
        volume_serial_number: u32,
        file_size_high: u32,
        file_size_low: u32,
        number_of_links: u32,
        file_index_high: u32,
        file_index_low: u32,
    }

    unsafe extern "system" {
        fn GetFileInformationByHandle(
            file: *mut c_void,
            information: *mut ByHandleFileInformation,
        ) -> i32;
    }

    let mut information = MaybeUninit::<ByHandleFileInformation>::uninit();
    // SAFETY: `file` owns a live Windows handle and `information` points to
    // writable storage with the exact Win32 structure layout.
    if unsafe { GetFileInformationByHandle(file.as_raw_handle(), information.as_mut_ptr()) } == 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: the successful call initialized the full structure.
    let information = unsafe { information.assume_init() };
    let file_index =
        ((information.file_index_high as u64) << 32) | information.file_index_low as u64;
    Ok(FileIdentity(
        information.volume_serial_number as u128,
        file_index as u128,
    ))
}

#[cfg(windows)]
fn identity_from_path(path: &Path) -> io::Result<FileIdentity> {
    use std::os::windows::fs::OpenOptionsExt;

    const FILE_SHARE_READ: u32 = 0x0000_0001;
    const FILE_SHARE_WRITE: u32 = 0x0000_0002;
    const FILE_SHARE_DELETE: u32 = 0x0000_0004;
    const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;

    let mut options = fs::OpenOptions::new();
    options
        .access_mode(0)
        .share_mode(FILE_SHARE_READ | FILE_SHARE_WRITE | FILE_SHARE_DELETE)
        .custom_flags(FILE_FLAG_BACKUP_SEMANTICS);
    identity_from_file(&options.open(path)?)
}

fn require_same_identity(
    current: FileIdentity,
    expected: FileIdentity,
    label: &str,
    path: &Path,
) -> Result<()> {
    if current != expected {
        bail!("{label} changed identity: {}", path.display())
    }
    Ok(())
}

fn require_directory_identity(path: &Path, expected: FileIdentity, label: &str) -> Result<()> {
    ensure_safe_existing_directory(path, label)?;
    let current = identity_from_path(path)
        .with_context(|| format!("re-identify {label}: {}", path.display()))?;
    require_same_identity(current, expected, label, path)
}

fn read_stable_existing_file(path: &Path, label: &str) -> Result<Vec<u8>> {
    let path = safe_existing_file(path, label)?;
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("{label} has no parent directory"))?;
    let parent_identity = identity_from_path(parent)
        .with_context(|| format!("identify {label} parent: {}", parent.display()))?;
    let path_identity = identity_from_path(&path)
        .with_context(|| format!("identify {label}: {}", path.display()))?;
    let mut file = fs::OpenOptions::new()
        .read(true)
        .open(&path)
        .with_context(|| format!("open {label}: {}", path.display()))?;
    let opened_identity = identity_from_file(&file)
        .with_context(|| format!("identify opened {label}: {}", path.display()))?;
    require_same_identity(opened_identity, path_identity, label, &path)?;
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes)
        .with_context(|| format!("read {label}: {}", path.display()))?;
    require_directory_identity(parent, parent_identity, &format!("{label} parent"))?;
    ensure_safe_components(&path, label, false)?;
    let after = identity_from_path(&path)
        .with_context(|| format!("re-identify {label}: {}", path.display()))?;
    require_same_identity(after, opened_identity, label, &path)?;
    Ok(bytes)
}

/// Read an immutable target through a held file handle and compare identities
/// before and after the read. This rejects final-component symlink/reparse and
/// target-swap races rather than following an unknown path.
fn verify_existing(
    path: &Path,
    expected: &[u8],
    parent: FileIdentity,
) -> Result<Option<FileIdentity>> {
    let parent_path = path
        .parent()
        .ok_or_else(|| anyhow!("existing result has no parent"))?;
    require_directory_identity(parent_path, parent, "existing result parent")?;
    match fs::symlink_metadata(path) {
        Ok(_) => {}
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(error) => {
            return Err(error)
                .with_context(|| format!("inspect existing result: {}", path.display()));
        }
    }
    ensure_safe_components(path, "existing result", false)?;
    let before = identity_from_path(path)
        .with_context(|| format!("identify existing result: {}", path.display()))?;
    let mut file = fs::OpenOptions::new()
        .read(true)
        .open(path)
        .with_context(|| format!("open existing result: {}", path.display()))?;
    let opened = identity_from_file(&file)
        .with_context(|| format!("identify opened existing result: {}", path.display()))?;
    require_same_identity(opened, before, "existing result", path)?;
    let mut existing = Vec::new();
    file.read_to_end(&mut existing)
        .with_context(|| format!("read existing result: {}", path.display()))?;
    require_directory_identity(parent_path, parent, "existing result parent")?;
    ensure_safe_components(path, "existing result", false)?;
    let after = identity_from_path(path)
        .with_context(|| format!("re-identify existing result: {}", path.display()))?;
    require_same_identity(after, opened, "existing result", path)?;
    if existing != expected {
        bail!(
            "refusing to overwrite divergent immutable result: {}",
            path.display()
        )
    }
    Ok(Some(opened))
}

fn create_temporary<O: PublicationIo>(
    ops: &O,
    parent: &Path,
    filename: &str,
) -> Result<(PathBuf, fs::File)> {
    for _ in 0..TEMPORARY_CREATE_ATTEMPTS {
        let temporary = parent.join(format!(
            ".{filename}.{}-{}.tmp",
            std::process::id(),
            unique_suffix(),
        ));
        match ops.create_new(&temporary) {
            Ok(file) => return Ok((temporary, file)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(error)
                    .with_context(|| format!("create result temporary: {}", temporary.display()));
            }
        }
    }
    bail!("could not allocate a unique result temporary after bounded attempts")
}

fn owned_temporary_present(
    temporary: &Path,
    temporary_identity: FileIdentity,
    parent_identity: FileIdentity,
) -> Result<bool> {
    let parent = temporary
        .parent()
        .ok_or_else(|| anyhow!("result temporary has no parent"))?;
    require_directory_identity(parent, parent_identity, "result temporary parent")?;
    let current = match fs::symlink_metadata(temporary) {
        Ok(current) => current,
        Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(false),
        Err(error) => {
            return Err(error).with_context(|| {
                format!("inspect owned result temporary: {}", temporary.display())
            });
        }
    };
    if is_link_or_reparse(&current) || !current.is_file() {
        bail!(
            "owned result temporary is not a regular file: {}",
            temporary.display()
        )
    }
    let current_identity = identity_from_path(temporary)
        .with_context(|| format!("identify owned result temporary: {}", temporary.display()))?;
    require_same_identity(
        current_identity,
        temporary_identity,
        "owned result temporary",
        temporary,
    )?;
    Ok(true)
}

fn require_owned_temporary(
    temporary: &Path,
    temporary_identity: FileIdentity,
    parent_identity: FileIdentity,
) -> Result<()> {
    if !owned_temporary_present(temporary, temporary_identity, parent_identity)? {
        bail!("owned result temporary vanished: {}", temporary.display())
    }
    Ok(())
}

fn remove_owned_temporary<O: PublicationIo>(
    ops: &O,
    temporary: &Path,
    temporary_identity: FileIdentity,
    parent_identity: FileIdentity,
) -> Result<()> {
    for delay_ms in SHARING_RETRY_DELAYS_MS
        .iter()
        .copied()
        .map(Some)
        .chain(std::iter::once(None))
    {
        if !owned_temporary_present(temporary, temporary_identity, parent_identity)? {
            return Ok(());
        }
        match ops.remove_file(temporary) {
            Ok(()) => return Ok(()),
            Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(()),
            Err(error) if ops.is_sharing_violation(&error) && delay_ms.is_some() => {
                if let Some(delay_ms) = delay_ms {
                    ops.pause(Duration::from_millis(delay_ms));
                }
            }
            Err(error) => {
                return Err(error).with_context(|| {
                    format!("remove owned result temporary: {}", temporary.display())
                });
            }
        }
    }
    unreachable!("bounded temporary-removal loop always returns")
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum LinkOutcome {
    Published,
    Existing,
}

fn link_immutable<O: PublicationIo>(
    ops: &O,
    temporary: &Path,
    target: &Path,
    expected: &[u8],
    temporary_identity: FileIdentity,
    parent_identity: FileIdentity,
) -> Result<LinkOutcome> {
    for delay_ms in SHARING_RETRY_DELAYS_MS
        .iter()
        .copied()
        .map(Some)
        .chain(std::iter::once(None))
    {
        require_owned_temporary(temporary, temporary_identity, parent_identity)?;
        match ops.hard_link(temporary, target) {
            Ok(()) => {
                let published = verify_existing(target, expected, parent_identity)?
                    .ok_or_else(|| anyhow!("published result vanished: {}", target.display()))?;
                require_same_identity(published, temporary_identity, "published result", target)?;
                return Ok(LinkOutcome::Published);
            }
            Err(error) => {
                if verify_existing(target, expected, parent_identity)?.is_some() {
                    return Ok(LinkOutcome::Existing);
                }
                if ops.is_sharing_violation(&error)
                    && let Some(delay_ms) = delay_ms
                {
                    ops.pause(Duration::from_millis(delay_ms));
                    continue;
                }
                return Err(error).with_context(|| {
                    format!(
                        "publish immutable result without replacing an existing artifact: {}",
                        target.display()
                    )
                });
            }
        }
    }
    unreachable!("bounded immutable-link loop always returns")
}

fn finish_with_cleanup<O: PublicationIo>(
    ops: &O,
    outcome: Result<()>,
    temporary: &Path,
    temporary_identity: FileIdentity,
    parent_identity: FileIdentity,
) -> Result<()> {
    let cleanup = remove_owned_temporary(ops, temporary, temporary_identity, parent_identity);
    match (outcome, cleanup) {
        (Ok(()), Ok(())) => Ok(()),
        (Err(error), Ok(())) => Err(error),
        (Ok(()), Err(cleanup)) => Err(cleanup),
        (Err(error), Err(cleanup)) => Err(error.context(format!(
            "owned result temporary could not be safely removed: {cleanup:#}"
        ))),
    }
}

fn replace_mutable_ledger(
    target: &Path,
    opened_input: &[u8],
    replacement: &[u8],
    expected_input_sha256: &str,
) -> Result<()> {
    let parent = target
        .parent()
        .ok_or_else(|| anyhow!("identity ledger path has no parent"))?;
    ensure_safe_existing_directory(parent, "identity ledger parent")?;
    let current = read_stable_existing_file(target, "caller identity ledger")?;
    if current == replacement {
        return Ok(());
    }
    if current != opened_input {
        bail!("caller identity ledger changed after opening expected {expected_input_sha256}")
    }
    let filename = target
        .file_name()
        .and_then(|name| name.to_str())
        .ok_or_else(|| anyhow!("identity ledger filename is not UTF-8"))?;
    let mut temporary = None;
    let mut file = None;
    for _ in 0..TEMPORARY_CREATE_ATTEMPTS {
        let candidate = parent.join(format!(
            ".{filename}.{}-{}.replace.tmp",
            std::process::id(),
            unique_suffix(),
        ));
        match fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&candidate)
        {
            Ok(created) => {
                temporary = Some(candidate);
                file = Some(created);
                break;
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => continue,
            Err(error) => return Err(error).context("create mutable ledger replacement"),
        }
    }
    let temporary = temporary
        .ok_or_else(|| anyhow!("could not allocate mutable ledger replacement temporary"))?;
    let outcome = (|| -> Result<()> {
        let mut file = file.expect("replacement file accompanies its path");
        file.write_all(replacement)
            .context("write mutable ledger replacement")?;
        file.sync_all().context("sync mutable ledger replacement")?;
        drop(file);
        atomic_replace(&temporary, target).context("atomically replace caller identity ledger")?;
        sync_directory_platform(parent).context("synchronize identity ledger parent")?;
        let persisted = read_stable_existing_file(target, "replaced caller identity ledger")?;
        if persisted != replacement {
            bail!("caller identity ledger drifted after atomic replacement")
        }
        Ok(())
    })();
    if temporary.exists() {
        fs::remove_file(&temporary).with_context(|| {
            format!(
                "remove mutable ledger replacement temporary: {}",
                temporary.display()
            )
        })?;
    }
    outcome
}

#[cfg(not(windows))]
fn atomic_replace(source: &Path, target: &Path) -> io::Result<()> {
    fs::rename(source, target)
}

#[cfg(windows)]
fn atomic_replace(source: &Path, target: &Path) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;

    const MOVEFILE_REPLACE_EXISTING: u32 = 0x0000_0001;
    const MOVEFILE_WRITE_THROUGH: u32 = 0x0000_0008;
    unsafe extern "system" {
        fn MoveFileExW(existing: *const u16, replacement: *const u16, flags: u32) -> i32;
    }
    let source = source
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    let target = target
        .as_os_str()
        .encode_wide()
        .chain(std::iter::once(0))
        .collect::<Vec<_>>();
    // SAFETY: both arguments are owned, NUL-terminated UTF-16 buffers that
    // remain alive for the duration of the Win32 call.
    if unsafe {
        MoveFileExW(
            source.as_ptr(),
            target.as_ptr(),
            MOVEFILE_REPLACE_EXISTING | MOVEFILE_WRITE_THROUGH,
        )
    } == 0
    {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

/// Install a fully-written result without replacing a pre-existing path. A
/// competing writer can only succeed when it produced byte-identical output.
fn publish_once(path: &Path, bytes: &[u8]) -> Result<()> {
    publish_once_with(&StdPublicationIo, path, bytes)
}

fn publish_once_with<O: PublicationIo>(ops: &O, path: &Path, bytes: &[u8]) -> Result<()> {
    let parent = path
        .parent()
        .ok_or_else(|| anyhow!("result has no parent"))?;
    ensure_safe_existing_directory(parent, "result parent")?;
    let parent_identity = identity_from_path(parent)
        .with_context(|| format!("identify result parent: {}", parent.display()))?;
    if verify_existing(path, bytes, parent_identity)?.is_some() {
        ops.sync_directory(parent)
            .with_context(|| format!("synchronize result parent: {}", parent.display()))?;
        require_directory_identity(parent, parent_identity, "result parent")?;
        verify_existing(path, bytes, parent_identity)?
            .ok_or_else(|| anyhow!("existing result vanished after directory sync"))?;
        return Ok(());
    }

    let filename = path
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| anyhow!("result filename is not Unicode"))?;
    let (temporary, mut file) = create_temporary(ops, parent, filename)?;
    let temporary_identity = identity_from_file(&file)
        .with_context(|| format!("identify result temporary: {}", temporary.display()))?;

    let write_outcome = ops
        .write_all(&mut file, bytes)
        .with_context(|| format!("write result temporary: {}", temporary.display()))
        .and_then(|()| {
            ops.sync_file(&file)
                .with_context(|| format!("synchronize result temporary: {}", temporary.display()))
        });
    drop(file);
    if let Err(error) = write_outcome {
        return finish_with_cleanup(
            ops,
            Err(error),
            &temporary,
            temporary_identity,
            parent_identity,
        );
    }

    let publication = (|| -> Result<()> {
        let link_outcome = link_immutable(
            ops,
            &temporary,
            path,
            bytes,
            temporary_identity,
            parent_identity,
        )?;
        ops.sync_directory(parent)
            .with_context(|| format!("synchronize result parent: {}", parent.display()))?;
        require_directory_identity(parent, parent_identity, "result parent")?;
        let after_sync = verify_existing(path, bytes, parent_identity)?
            .ok_or_else(|| anyhow!("published result vanished after directory sync"))?;
        if link_outcome == LinkOutcome::Published {
            require_same_identity(after_sync, temporary_identity, "published result", path)?;
        }
        Ok(())
    })();
    finish_with_cleanup(
        ops,
        publication,
        &temporary,
        temporary_identity,
        parent_identity,
    )
}

fn unique_suffix() -> u128 {
    use std::sync::atomic::{AtomicU64, Ordering};
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let counter = COUNTER.fetch_add(1, Ordering::Relaxed) as u128;
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    now ^ counter
}

#[cfg(test)]
mod tests {
    use std::cell::{Cell, RefCell};

    use super::*;

    const EXACT: &[u8] = b"exact\n";

    #[test]
    fn windows_public_crlf_policy_still_requires_exact_pretty_lf_caller_ledger() {
        let value = serde_json::json!({
            "nested": [true, {"tiny": 1e-7}],
            "text": "\u{007f} 😀",
        });
        let public_newline = generation_final_newline("crlf");
        assert_eq!(public_newline.bytes(), b"\r\n");
        let lf_ledger = python_pretty_json_line(&value, JsonNewline::Lf).unwrap();
        assert!(
            parse_python_pretty_json_document(&lf_ledger, JsonNewline::Lf, "test ledger").is_ok()
        );

        let compact = canonical_json_line(&value).unwrap();
        let error = parse_python_pretty_json_document(&compact, JsonNewline::Lf, "test ledger")
            .unwrap_err();
        assert!(format!("{error:#}").contains("exact Python pretty JSON"));

        let crlf_ledger = python_pretty_json_line(&value, JsonNewline::Crlf).unwrap();
        assert!(
            parse_python_pretty_json_document(&crlf_ledger, JsonNewline::Lf, "test ledger")
                .is_err()
        );
    }

    struct TestDirectory(PathBuf);

    impl TestDirectory {
        fn new(label: &str) -> Self {
            let path = env::temp_dir().join(format!(
                "temporal-qd-batch-{label}-{}-{}",
                std::process::id(),
                unique_suffix(),
            ));
            fs::create_dir(&path).expect("create isolated test directory");
            Self(path)
        }

        fn path(&self) -> &Path {
            &self.0
        }
    }

    impl Drop for TestDirectory {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq)]
    enum FailurePoint {
        Write,
        FileSync,
        Link,
        DirectorySync,
    }

    #[derive(Default)]
    struct InjectedOps {
        failure: Option<FailurePoint>,
        sharing_failures: Cell<usize>,
        classify_sharing: bool,
        create_collisions: Cell<usize>,
        swap_target_during_link: bool,
        swap_target_during_directory_sync: Option<PathBuf>,
        swap_temporary_during_link: bool,
        events: RefCell<Vec<&'static str>>,
        pauses: Cell<usize>,
    }

    impl PublicationIo for InjectedOps {
        fn create_new(&self, path: &Path) -> io::Result<fs::File> {
            let collisions = self.create_collisions.get();
            if collisions > 0 {
                self.create_collisions.set(collisions - 1);
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "injected stale temporary collision",
                ));
            }
            StdPublicationIo.create_new(path)
        }

        fn write_all(&self, file: &mut fs::File, bytes: &[u8]) -> io::Result<()> {
            self.events.borrow_mut().push("payload-write");
            if self.failure == Some(FailurePoint::Write) {
                return Err(io::Error::other("injected write failure"));
            }
            StdPublicationIo.write_all(file, bytes)
        }

        fn sync_file(&self, file: &fs::File) -> io::Result<()> {
            self.events.borrow_mut().push("payload-sync");
            if self.failure == Some(FailurePoint::FileSync) {
                return Err(io::Error::other("injected file sync failure"));
            }
            StdPublicationIo.sync_file(file)
        }

        fn hard_link(&self, source: &Path, target: &Path) -> io::Result<()> {
            self.events.borrow_mut().push("publish-link");
            if self.swap_temporary_during_link {
                fs::remove_file(source)?;
                fs::write(source, b"unknown temporary")?;
                return Err(io::Error::other("injected temporary swap"));
            }
            if self.swap_target_during_link {
                fs::write(target, b"attacker\n")?;
                return Err(io::Error::new(
                    io::ErrorKind::AlreadyExists,
                    "injected target swap",
                ));
            }
            let failures = self.sharing_failures.get();
            if failures > 0 {
                self.sharing_failures.set(failures - 1);
                return Err(io::Error::other("injected sharing violation"));
            }
            if self.failure == Some(FailurePoint::Link) {
                return Err(io::Error::new(
                    io::ErrorKind::PermissionDenied,
                    "injected link failure",
                ));
            }
            StdPublicationIo.hard_link(source, target)
        }

        fn remove_file(&self, path: &Path) -> io::Result<()> {
            self.events.borrow_mut().push("temporary-remove");
            StdPublicationIo.remove_file(path)
        }

        fn sync_directory(&self, path: &Path) -> io::Result<()> {
            self.events.borrow_mut().push("directory-sync");
            if let Some(target) = &self.swap_target_during_directory_sync {
                fs::remove_file(target)?;
                fs::write(target, b"attacker\n")?;
                return Ok(());
            }
            if self.failure == Some(FailurePoint::DirectorySync) {
                return Err(io::Error::other("injected directory sync failure"));
            }
            StdPublicationIo.sync_directory(path)
        }

        fn is_sharing_violation(&self, _error: &io::Error) -> bool {
            self.classify_sharing
        }

        fn pause(&self, _duration: Duration) {
            self.pauses.set(self.pauses.get() + 1);
        }
    }

    fn owned_temporaries(parent: &Path) -> Vec<PathBuf> {
        fs::read_dir(parent)
            .expect("read test directory")
            .map(|entry| entry.expect("read test entry").path())
            .filter(|path| {
                path.file_name()
                    .and_then(|name| name.to_str())
                    .is_some_and(|name| name.starts_with(".result.json.") && name.ends_with(".tmp"))
            })
            .collect()
    }

    #[test]
    fn publication_orders_payload_link_directory_sync_and_cleanup() {
        let directory = TestDirectory::new("ordering");
        let target = directory.path().join("result.json");
        let ops = InjectedOps::default();

        publish_once_with(&ops, &target, EXACT).unwrap();

        assert_eq!(fs::read(&target).unwrap(), EXACT);
        assert_eq!(
            *ops.events.borrow(),
            [
                "payload-write",
                "payload-sync",
                "publish-link",
                "directory-sync",
                "temporary-remove",
            ]
        );
        assert!(owned_temporaries(directory.path()).is_empty());
    }

    #[test]
    fn injected_write_sync_and_link_failures_leave_no_partial_target() {
        for failure in [
            FailurePoint::Write,
            FailurePoint::FileSync,
            FailurePoint::Link,
        ] {
            let directory = TestDirectory::new("prepublication-failure");
            let target = directory.path().join("result.json");
            let ops = InjectedOps {
                failure: Some(failure),
                ..InjectedOps::default()
            };

            assert!(publish_once_with(&ops, &target, EXACT).is_err());
            assert!(!target.exists());
            assert!(owned_temporaries(directory.path()).is_empty());
        }
    }

    #[test]
    fn directory_sync_failure_reports_after_safe_link_and_retry_recovers() {
        let directory = TestDirectory::new("directory-sync-failure");
        let target = directory.path().join("result.json");
        let failing = InjectedOps {
            failure: Some(FailurePoint::DirectorySync),
            ..InjectedOps::default()
        };

        let error = publish_once_with(&failing, &target, EXACT).unwrap_err();
        assert!(format!("{error:#}").contains("synchronize result parent"));
        assert_eq!(fs::read(&target).unwrap(), EXACT);
        assert!(owned_temporaries(directory.path()).is_empty());

        publish_once_with(&InjectedOps::default(), &target, EXACT).unwrap();
        assert_eq!(fs::read(&target).unwrap(), EXACT);
    }

    #[test]
    fn sharing_violation_retries_are_bounded() {
        let directory = TestDirectory::new("sharing-bounded");
        let target = directory.path().join("result.json");
        let ops = InjectedOps {
            sharing_failures: Cell::new(usize::MAX),
            classify_sharing: true,
            ..InjectedOps::default()
        };

        assert!(publish_once_with(&ops, &target, EXACT).is_err());
        let links = ops
            .events
            .borrow()
            .iter()
            .filter(|event| **event == "publish-link")
            .count();
        assert_eq!(links, SHARING_RETRY_DELAYS_MS.len() + 1);
        assert_eq!(ops.pauses.get(), SHARING_RETRY_DELAYS_MS.len());
        assert!(!target.exists());
        assert!(owned_temporaries(directory.path()).is_empty());
    }

    #[test]
    fn transient_sharing_violation_converges_without_replacement() {
        let directory = TestDirectory::new("sharing-transient");
        let target = directory.path().join("result.json");
        let ops = InjectedOps {
            sharing_failures: Cell::new(2),
            classify_sharing: true,
            ..InjectedOps::default()
        };

        publish_once_with(&ops, &target, EXACT).unwrap();
        assert_eq!(fs::read(&target).unwrap(), EXACT);
        assert_eq!(ops.pauses.get(), 2);
    }

    #[test]
    fn divergent_existing_result_is_refused() {
        let directory = TestDirectory::new("divergent-existing");
        let target = directory.path().join("result.json");
        fs::write(&target, b"attacker\n").unwrap();

        let error = publish_once_with(&InjectedOps::default(), &target, EXACT).unwrap_err();
        assert!(format!("{error:#}").contains("divergent immutable result"));
        assert_eq!(fs::read(&target).unwrap(), b"attacker\n");
    }

    #[test]
    fn stale_temporary_is_ignored_and_never_removed() {
        let directory = TestDirectory::new("stale-temp");
        let target = directory.path().join("result.json");
        let stale = directory.path().join(".result.json.stale.tmp");
        fs::write(&stale, b"unowned stale").unwrap();
        let ops = InjectedOps {
            create_collisions: Cell::new(1),
            ..InjectedOps::default()
        };

        publish_once_with(&ops, &target, EXACT).unwrap();

        assert_eq!(fs::read(&target).unwrap(), EXACT);
        assert_eq!(fs::read(&stale).unwrap(), b"unowned stale");
        assert_eq!(owned_temporaries(directory.path()), [stale]);
    }

    #[test]
    fn target_swap_during_link_is_refused_without_replacement() {
        let directory = TestDirectory::new("target-swap-link");
        let target = directory.path().join("result.json");
        let ops = InjectedOps {
            swap_target_during_link: true,
            ..InjectedOps::default()
        };

        let error = publish_once_with(&ops, &target, EXACT).unwrap_err();
        assert!(format!("{error:#}").contains("divergent immutable result"));
        assert_eq!(fs::read(&target).unwrap(), b"attacker\n");
        assert!(owned_temporaries(directory.path()).is_empty());
    }

    #[test]
    fn target_swap_after_link_is_detected_after_directory_sync() {
        let directory = TestDirectory::new("target-swap-sync");
        let target = directory.path().join("result.json");
        let ops = InjectedOps {
            swap_target_during_directory_sync: Some(target.clone()),
            ..InjectedOps::default()
        };

        let error = publish_once_with(&ops, &target, EXACT).unwrap_err();
        assert!(format!("{error:#}").contains("divergent immutable result"));
        assert_eq!(fs::read(&target).unwrap(), b"attacker\n");
        assert!(owned_temporaries(directory.path()).is_empty());
    }

    #[test]
    fn cleanup_refuses_to_remove_a_swapped_unknown_temporary() {
        let directory = TestDirectory::new("temporary-swap");
        let target = directory.path().join("result.json");
        let ops = InjectedOps {
            swap_temporary_during_link: true,
            ..InjectedOps::default()
        };

        let error = publish_once_with(&ops, &target, EXACT).unwrap_err();
        assert!(format!("{error:#}").contains("could not be safely removed"));
        let temporaries = owned_temporaries(directory.path());
        assert_eq!(temporaries.len(), 1);
        assert_eq!(fs::read(&temporaries[0]).unwrap(), b"unknown temporary");
        assert!(!target.exists());
    }

    #[test]
    fn mutable_ledger_replace_is_atomic_idempotent_and_rejects_substitution() {
        let directory = TestDirectory::new("mutable-ledger-replace");
        let target = directory.path().join("identity-ledger.json");
        let old = b"{\"ledgerSha256\":\"old\"}\n";
        let new = b"{\"ledgerSha256\":\"new\"}\n";
        fs::write(&target, old).unwrap();

        replace_mutable_ledger(&target, old, new, "sha256:old").unwrap();
        assert_eq!(fs::read(&target).unwrap(), new);
        // Simulates a crash after the atomic replace but before outer-result
        // publication: repair is recognized without another replacement.
        replace_mutable_ledger(&target, old, new, "sha256:old").unwrap();
        assert_eq!(fs::read(&target).unwrap(), new);

        fs::write(&target, b"attacker\n").unwrap();
        let error = replace_mutable_ledger(&target, old, new, "sha256:old").unwrap_err();
        assert!(format!("{error:#}").contains("changed after opening"));
        assert_eq!(fs::read(&target).unwrap(), b"attacker\n");
    }

    #[cfg(unix)]
    #[test]
    fn symlink_target_and_parent_are_rejected() {
        use std::os::unix::fs::symlink;

        let directory = TestDirectory::new("symlink");
        let victim = directory.path().join("victim.json");
        fs::write(&victim, b"victim\n").unwrap();
        let target = directory.path().join("result.json");
        symlink(&victim, &target).unwrap();
        assert!(publish_once_with(&InjectedOps::default(), &target, EXACT).is_err());
        assert_eq!(fs::read(&victim).unwrap(), b"victim\n");

        let real_parent = directory.path().join("real-parent");
        fs::create_dir(&real_parent).unwrap();
        let linked_parent = directory.path().join("linked-parent");
        symlink(&real_parent, &linked_parent).unwrap();
        assert!(
            publish_once_with(
                &InjectedOps::default(),
                &linked_parent.join("result.json"),
                EXACT,
            )
            .is_err()
        );
        assert!(fs::read_dir(real_parent).unwrap().next().is_none());
    }

    #[cfg(windows)]
    #[test]
    fn symlink_reparse_target_and_parent_are_rejected_when_available() {
        use std::os::windows::fs::{symlink_dir, symlink_file};

        let directory = TestDirectory::new("symlink-reparse");
        let victim = directory.path().join("victim.json");
        fs::write(&victim, b"victim\n").unwrap();
        let target = directory.path().join("result.json");
        if symlink_file(&victim, &target).is_err() {
            return;
        }
        assert!(publish_once_with(&InjectedOps::default(), &target, EXACT).is_err());
        assert_eq!(fs::read(&victim).unwrap(), b"victim\n");

        let real_parent = directory.path().join("real-parent");
        fs::create_dir(&real_parent).unwrap();
        let linked_parent = directory.path().join("linked-parent");
        if symlink_dir(&real_parent, &linked_parent).is_err() {
            return;
        }
        assert!(
            publish_once_with(
                &InjectedOps::default(),
                &linked_parent.join("result.json"),
                EXACT,
            )
            .is_err()
        );
        assert!(fs::read_dir(real_parent).unwrap().next().is_none());
    }
}
