use anyhow::{Context, Result, bail};
use serde::Deserialize;
use serde::de::IgnoredAny;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::time::Instant;

const CONTRACT_VERSION: &str = "temporal_qd_population_finalizer_v1";
const MANIFEST_SCHEMA: &str = "temporal_qd_population_finalizer_manifest_v1";
const SHELL_PLACEHOLDER: &str =
    "sha256:0000000000000000000000000000000000000000000000000000000000000000";

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ManifestCandidate {
    proposal_ordinal: usize,
    candidate_id: String,
    candidate_identity_sha256: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Manifest {
    schema_version: String,
    contract_version: String,
    config_sha256: String,
    generation_index: i64,
    expected_proposal_count: usize,
    expected_entry_sha256s: Vec<String>,
    accepted_candidates: Vec<ManifestCandidate>,
    candidate_count: usize,
    journal_directory: String,
    output_path: String,
    population_shell_path: String,
    population_shell_file_sha256: String,
    final_newline: String,
    manifest_sha256: String,
}

#[derive(Clone, Debug)]
struct Field {
    key: String,
    member_start: usize,
    member_end: usize,
    value: Range<usize>,
    has_following_comma: bool,
}

#[derive(Debug)]
struct CandidateRef {
    path: PathBuf,
    id: String,
    identity_sha256: String,
    range: Range<usize>,
    file_bytes: u64,
    ordinal: usize,
}

fn hex_digest(digest: impl AsRef<[u8]>) -> String {
    let mut result = String::with_capacity(digest.as_ref().len() * 2);
    for byte in digest.as_ref() {
        use std::fmt::Write as _;
        write!(&mut result, "{byte:02x}").unwrap();
    }
    result
}

fn sha256_bytes(bytes: &[u8]) -> String {
    format!("sha256:{}", hex_digest(Sha256::digest(bytes)))
}

fn sha256_file(path: &Path) -> Result<String> {
    let mut reader = BufReader::with_capacity(4 * 1024 * 1024, File::open(path)?);
    let mut digest = Sha256::new();
    let mut buffer = vec![0u8; 4 * 1024 * 1024];
    loop {
        let amount = reader.read(&mut buffer)?;
        if amount == 0 {
            break;
        }
        digest.update(&buffer[..amount]);
    }
    Ok(format!("sha256:{}", hex_digest(digest.finalize())))
}

fn validate_json(bytes: &[u8], name: &str) -> Result<()> {
    let mut deserializer = serde_json::Deserializer::from_slice(bytes);
    IgnoredAny::deserialize(&mut deserializer).with_context(|| format!("parse {name} JSON"))?;
    deserializer
        .end()
        .with_context(|| format!("finish parsing {name} JSON"))?;
    Ok(())
}

fn skip_ws(bytes: &[u8], mut pos: usize) -> usize {
    while pos < bytes.len() && matches!(bytes[pos], b' ' | b'\n' | b'\r' | b'\t') {
        pos += 1;
    }
    pos
}

fn string_end(bytes: &[u8], start: usize) -> Result<usize> {
    if bytes.get(start) != Some(&b'"') {
        bail!("expected JSON string at byte {start}");
    }
    let mut escaped = false;
    for (offset, &byte) in bytes[start + 1..].iter().enumerate() {
        let pos = start + 1 + offset;
        if escaped {
            escaped = false;
        } else if byte == b'\\' {
            escaped = true;
        } else if byte == b'"' {
            return Ok(pos + 1);
        } else if byte < 0x20 {
            bail!("unescaped control byte in JSON string");
        }
    }
    bail!("unterminated JSON string")
}

fn composite_end(bytes: &[u8], start: usize) -> Result<usize> {
    let opening = *bytes.get(start).context("missing JSON composite")?;
    let closing = match opening {
        b'{' => b'}',
        b'[' => b']',
        _ => bail!("expected JSON composite at byte {start}"),
    };
    let mut stack = vec![closing];
    let mut pos = start + 1;
    while pos < bytes.len() {
        match bytes[pos] {
            b'"' => pos = string_end(bytes, pos)?,
            b'{' => {
                stack.push(b'}');
                pos += 1;
            }
            b'[' => {
                stack.push(b']');
                pos += 1;
            }
            b'}' | b']' => {
                let expected = stack.pop().context("unbalanced JSON close")?;
                if bytes[pos] != expected {
                    bail!("mismatched JSON close at byte {pos}");
                }
                pos += 1;
                if stack.is_empty() {
                    return Ok(pos);
                }
            }
            _ => pos += 1,
        }
    }
    bail!("unterminated JSON composite")
}

fn value_end(bytes: &[u8], start: usize) -> Result<usize> {
    match bytes.get(start).copied().context("missing JSON value")? {
        b'"' => string_end(bytes, start),
        b'{' | b'[' => composite_end(bytes, start),
        _ => {
            let mut pos = start;
            while pos < bytes.len()
                && !matches!(
                    bytes[pos],
                    b',' | b'}' | b']' | b' ' | b'\n' | b'\r' | b'\t'
                )
            {
                pos += 1;
            }
            if pos == start {
                bail!("empty JSON primitive at byte {start}");
            }
            Ok(pos)
        }
    }
}

fn object_fields(bytes: &[u8], start: usize) -> Result<(usize, Vec<Field>)> {
    if bytes.get(start) != Some(&b'{') {
        bail!("expected JSON object at byte {start}");
    }
    let mut pos = skip_ws(bytes, start + 1);
    let mut fields = Vec::new();
    let mut previous_key: Option<String> = None;
    if bytes.get(pos) == Some(&b'}') {
        return Ok((pos + 1, fields));
    }
    loop {
        let member_start = pos;
        let key_end = string_end(bytes, pos)?;
        let key: String =
            serde_json::from_slice(&bytes[pos..key_end]).context("invalid JSON key")?;
        if previous_key
            .as_ref()
            .is_some_and(|previous| previous >= &key)
        {
            bail!("object keys are not in strict canonical order at {key}");
        }
        previous_key = Some(key.clone());
        pos = skip_ws(bytes, key_end);
        if bytes.get(pos) != Some(&b':') {
            bail!("missing colon after key {key}");
        }
        pos = skip_ws(bytes, pos + 1);
        let value_start = pos;
        pos = value_end(bytes, value_start)?;
        let value = value_start..pos;
        pos = skip_ws(bytes, pos);
        let delimiter = *bytes.get(pos).context("unterminated JSON object")?;
        if delimiter == b',' {
            fields.push(Field {
                key,
                member_start,
                member_end: pos + 1,
                value,
                has_following_comma: true,
            });
            pos = skip_ws(bytes, pos + 1);
        } else if delimiter == b'}' {
            fields.push(Field {
                key,
                member_start,
                member_end: pos,
                value,
                has_following_comma: false,
            });
            return Ok((pos + 1, fields));
        } else {
            bail!("unexpected object delimiter at byte {pos}");
        }
    }
}

fn field<'a>(fields: &'a [Field], key: &str) -> Result<&'a Field> {
    fields
        .iter()
        .find(|item| item.key == key)
        .with_context(|| format!("missing field {key}"))
}

fn semantic_without_field(bytes: &[u8], field: &Field) -> Result<Vec<u8>> {
    if field.has_following_comma {
        let mut result = Vec::with_capacity(bytes.len() - (field.member_end - field.member_start));
        result.extend_from_slice(&bytes[..field.member_start]);
        result.extend_from_slice(&bytes[field.member_end..]);
        return Ok(result);
    }
    let comma = bytes[..field.member_start]
        .iter()
        .rposition(|byte| *byte == b',')
        .context("last object field has no preceding comma")?;
    let mut result = Vec::with_capacity(bytes.len() - (field.member_end - comma));
    result.extend_from_slice(&bytes[..comma]);
    result.extend_from_slice(&bytes[field.member_end..]);
    Ok(result)
}

fn json_string(bytes: &[u8], range: Range<usize>, name: &str) -> Result<String> {
    serde_json::from_slice(&bytes[range]).with_context(|| format!("invalid {name}"))
}

fn json_usize(bytes: &[u8], range: Range<usize>, name: &str) -> Result<usize> {
    serde_json::from_slice(&bytes[range]).with_context(|| format!("invalid {name}"))
}

fn resolve_relative(manifest_path: &Path, relative: &str) -> Result<PathBuf> {
    let relative_path = Path::new(relative);
    if relative_path.is_absolute() {
        bail!("finalizer manifest paths must be relative: {relative}");
    }
    Ok(manifest_path
        .parent()
        .context("manifest has no parent")?
        .join(relative_path))
}

fn load_manifest(path: &Path) -> Result<Manifest> {
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    let semantic_end = bytes
        .iter()
        .rposition(|byte| !matches!(byte, b'\n' | b'\r'))
        .map(|p| p + 1)
        .unwrap_or(0);
    let semantic = &bytes[..semantic_end];
    validate_json(semantic, "finalizer manifest")?;
    let (end, fields) = object_fields(semantic, 0)?;
    if skip_ws(semantic, end) != semantic.len() {
        bail!("manifest contains trailing bytes");
    }
    let hash_field = field(&fields, "manifestSha256")?;
    let embedded = json_string(semantic, hash_field.value.clone(), "manifest SHA")?;
    let material = semantic_without_field(semantic, hash_field)?;
    let observed = sha256_bytes(&material);
    if embedded != observed {
        bail!("finalizer manifest identity mismatch");
    }
    let manifest: Manifest =
        serde_json::from_slice(semantic).context("parse finalizer manifest")?;
    if manifest.manifest_sha256 != observed {
        bail!("parsed finalizer manifest identity mismatch");
    }
    if manifest.schema_version != MANIFEST_SCHEMA || manifest.contract_version != CONTRACT_VERSION {
        bail!("unsupported finalizer manifest/contract version");
    }
    if manifest.expected_entry_sha256s.len() != manifest.expected_proposal_count {
        bail!("manifest proposal count does not match entry SHA count");
    }
    if manifest.accepted_candidates.len() != manifest.candidate_count {
        bail!("manifest candidate count does not match accepted reference count");
    }
    Ok(manifest)
}

fn scan_journal_entry(
    path: &Path,
    ordinal: usize,
    expected_entry_sha: &str,
    manifest: &Manifest,
) -> Result<Option<CandidateRef>> {
    let bytes = fs::read(path).with_context(|| format!("read {}", path.display()))?;
    let semantic_end = bytes
        .iter()
        .rposition(|byte| !matches!(byte, b'\n' | b'\r'))
        .map(|p| p + 1)
        .unwrap_or(0);
    let semantic = &bytes[..semantic_end];
    validate_json(semantic, "journal entry")?;
    let (end, fields) = object_fields(semantic, 0)?;
    if skip_ws(semantic, end) != semantic.len() {
        bail!("journal entry {ordinal} contains trailing non-newline bytes");
    }
    let entry_sha = field(&fields, "entrySha256")?;
    let embedded = json_string(semantic, entry_sha.value.clone(), "entry SHA")?;
    if embedded != expected_entry_sha {
        bail!("entry SHA manifest mismatch at ordinal {ordinal}");
    }
    let observed = sha256_bytes(&semantic_without_field(semantic, entry_sha)?);
    if observed != embedded {
        bail!("entry semantic SHA mismatch at ordinal {ordinal}");
    }
    let observed_ordinal = json_usize(
        semantic,
        field(&fields, "proposalOrdinal")?.value.clone(),
        "proposal ordinal",
    )?;
    if observed_ordinal != ordinal {
        bail!("journal proposal ordinal mismatch at ordinal {ordinal}");
    }
    let config = json_string(
        semantic,
        field(&fields, "configSha256")?.value.clone(),
        "config SHA",
    )?;
    if config != manifest.config_sha256 {
        bail!("journal config mismatch at ordinal {ordinal}");
    }
    let generation: i64 =
        serde_json::from_slice(&semantic[field(&fields, "generationIndex")?.value.clone()])?;
    if generation != manifest.generation_index {
        bail!("journal generation mismatch at ordinal {ordinal}");
    }
    let disposition = json_string(
        semantic,
        field(&fields, "disposition")?.value.clone(),
        "disposition",
    )?;
    if disposition != "accepted" {
        return Ok(None);
    }
    let candidate = field(&fields, "candidate")?;
    let candidate_bytes = &semantic[candidate.value.clone()];
    let (candidate_end, candidate_fields) = object_fields(candidate_bytes, 0)?;
    if candidate_end != candidate_bytes.len() {
        bail!("candidate has trailing bytes at ordinal {ordinal}");
    }
    let id = json_string(
        candidate_bytes,
        field(&candidate_fields, "candidateId")?.value.clone(),
        "candidate ID",
    )?;
    let identity_sha256 = json_string(
        candidate_bytes,
        field(&candidate_fields, "candidateIdentitySha256")?
            .value
            .clone(),
        "candidate identity SHA",
    )?;
    Ok(Some(CandidateRef {
        path: path.to_path_buf(),
        id,
        identity_sha256,
        range: candidate.value.clone(),
        file_bytes: bytes.len() as u64,
        ordinal,
    }))
}

fn validate_journal_files(directory: &Path, expected_count: usize) -> Result<()> {
    let expected: BTreeSet<String> = (0..expected_count)
        .map(|index| format!("{index:08}.json"))
        .collect();
    let mut observed = BTreeSet::new();
    for entry in fs::read_dir(directory)
        .with_context(|| format!("read journal directory {}", directory.display()))?
    {
        let entry = entry?;
        if !entry.file_type()?.is_file() {
            bail!(
                "journal directory contains a non-file entry: {}",
                entry.path().display()
            );
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if !observed.insert(name.clone()) {
            bail!("duplicate journal filename {name}");
        }
    }
    if observed != expected {
        let missing: Vec<_> = expected.difference(&observed).cloned().collect();
        let extra: Vec<_> = observed.difference(&expected).cloned().collect();
        bail!("journal file set mismatch; missing={missing:?}, extra={extra:?}");
    }
    Ok(())
}

fn copy_candidate(
    reference: &CandidateRef,
    writer: &mut BufWriter<File>,
    semantic_hash: &mut Sha256,
) -> Result<u64> {
    let mut file = File::open(&reference.path)?;
    file.seek(SeekFrom::Start(reference.range.start as u64))?;
    let mut remaining = reference.range.len();
    let mut buffer = vec![0u8; 1024 * 1024];
    let mut total = 0u64;
    while remaining > 0 {
        let amount = remaining.min(buffer.len());
        file.read_exact(&mut buffer[..amount])?;
        writer.write_all(&buffer[..amount])?;
        semantic_hash.update(&buffer[..amount]);
        remaining -= amount;
        total += amount as u64;
    }
    Ok(total)
}

fn write_and_hash(writer: &mut BufWriter<File>, hasher: &mut Sha256, bytes: &[u8]) -> Result<()> {
    writer.write_all(bytes)?;
    hasher.update(bytes);
    Ok(())
}

fn files_exact(left: &Path, right: &Path) -> Result<bool> {
    let left_file = File::open(left)?;
    let right_file = File::open(right)?;
    if left_file.metadata()?.len() != right_file.metadata()?.len() {
        return Ok(false);
    }
    let mut left_reader = BufReader::with_capacity(4 * 1024 * 1024, left_file);
    let mut right_reader = BufReader::with_capacity(4 * 1024 * 1024, right_file);
    let mut left_buffer = vec![0u8; 4 * 1024 * 1024];
    let mut right_buffer = vec![0u8; 4 * 1024 * 1024];
    loop {
        let left_amount = left_reader.read(&mut left_buffer)?;
        let right_amount = right_reader.read(&mut right_buffer)?;
        if left_amount != right_amount || left_buffer[..left_amount] != right_buffer[..right_amount]
        {
            return Ok(false);
        }
        if left_amount == 0 {
            return Ok(true);
        }
    }
}

fn cleanup_stale_temporaries(output: &Path, keep: Option<&Path>) -> Result<()> {
    let parent = output.parent().context("population output has no parent")?;
    let prefix = format!(
        "{}.temporal-finalizer-",
        output
            .file_name()
            .context("population output has no filename")?
            .to_string_lossy()
    );
    for entry in fs::read_dir(parent)? {
        let entry = entry?;
        let path = entry.path();
        if keep.is_some_and(|current| current == path) {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if entry.file_type()?.is_file() && name.starts_with(&prefix) && name.ends_with(".tmp") {
            fs::remove_file(path)?;
        }
    }
    Ok(())
}

fn finalize(manifest_path: &Path) -> Result<Value> {
    let started = Instant::now();
    let manifest = load_manifest(manifest_path)?;
    let journal_dir = resolve_relative(manifest_path, &manifest.journal_directory)?;
    let output = resolve_relative(manifest_path, &manifest.output_path)?;
    let shell_path = resolve_relative(manifest_path, &manifest.population_shell_path)?;
    if sha256_file(&shell_path)? != manifest.population_shell_file_sha256 {
        bail!("population shell file SHA mismatch");
    }
    let shell = fs::read(&shell_path)?;
    let expected_newline: &[u8] = match manifest.final_newline.as_str() {
        "lf" => b"\n",
        "crlf" => b"\r\n",
        _ => bail!("unsupported final newline policy"),
    };
    if !shell.ends_with(expected_newline) {
        bail!("population shell newline does not match manifest policy");
    }
    let semantic_end = shell.len() - expected_newline.len();
    validate_json(&shell[..semantic_end], "population shell")?;
    let (shell_end, shell_fields) = object_fields(&shell[..semantic_end], 0)?;
    if shell_end != semantic_end {
        bail!("population shell contains trailing semantic bytes");
    }
    let candidates_field = field(&shell_fields, "candidates")?.clone();
    if &shell[candidates_field.value.clone()] != b"[]" {
        bail!("population shell candidates must be empty");
    }
    let population_sha_field = field(&shell_fields, "populationSha256")?.clone();
    if !population_sha_field.has_following_comma
        || candidates_field.value.end >= population_sha_field.member_start
    {
        bail!("population shell field order cannot support canonical finalization");
    }
    let placeholder = json_string(
        &shell,
        population_sha_field.value.clone(),
        "population SHA placeholder",
    )?;
    if placeholder != SHELL_PLACEHOLDER {
        bail!("population shell SHA placeholder mismatch");
    }

    validate_journal_files(&journal_dir, manifest.expected_proposal_count)?;
    let scan_started = Instant::now();
    let mut accepted = Vec::with_capacity(manifest.candidate_count);
    let mut journal_bytes = 0u64;
    for (ordinal, expected_sha) in manifest.expected_entry_sha256s.iter().enumerate() {
        let path = journal_dir.join(format!("{ordinal:08}.json"));
        if let Some(reference) = scan_journal_entry(&path, ordinal, expected_sha, &manifest)? {
            journal_bytes += reference.file_bytes;
            accepted.push(reference);
        } else {
            journal_bytes += fs::metadata(path)?.len();
        }
    }
    accepted.sort_by(|left, right| left.id.cmp(&right.id));
    for pair in accepted.windows(2) {
        if pair[0].id == pair[1].id {
            bail!("duplicate candidate ID {}", pair[0].id);
        }
    }
    if accepted.len() != manifest.candidate_count {
        bail!("accepted journal count does not match manifest candidate count");
    }
    let expected_by_ordinal: BTreeMap<usize, (&str, &str)> = manifest
        .accepted_candidates
        .iter()
        .map(|item| {
            (
                item.proposal_ordinal,
                (
                    item.candidate_id.as_str(),
                    item.candidate_identity_sha256.as_str(),
                ),
            )
        })
        .collect();
    if expected_by_ordinal.len() != manifest.accepted_candidates.len() {
        bail!("manifest contains duplicate accepted proposal ordinals");
    }
    for reference in &accepted {
        let expected = expected_by_ordinal
            .get(&reference.ordinal)
            .with_context(|| {
                format!(
                    "accepted journal ordinal {} absent from manifest",
                    reference.ordinal
                )
            })?;
        if reference.id != expected.0 || reference.identity_sha256 != expected.1 {
            bail!(
                "accepted candidate identity mismatch at ordinal {}",
                reference.ordinal
            );
        }
    }
    let scan_ms = scan_started.elapsed().as_secs_f64() * 1000.0;

    let output_parent = output.parent().context("population output has no parent")?;
    fs::create_dir_all(output_parent)?;
    cleanup_stale_temporaries(&output, None)?;
    let temporary = output.with_file_name(format!(
        "{}.temporal-finalizer-{}-{}.tmp",
        output.file_name().unwrap().to_string_lossy(),
        std::process::id(),
        started.elapsed().as_nanos()
    ));
    let assembly_started = Instant::now();
    let file = OpenOptions::new()
        .write(true)
        .read(true)
        .create_new(true)
        .open(&temporary)?;
    let mut writer = BufWriter::with_capacity(4 * 1024 * 1024, file);
    let mut semantic_hash = Sha256::new();

    write_and_hash(
        &mut writer,
        &mut semantic_hash,
        &shell[..candidates_field.value.start],
    )?;
    write_and_hash(&mut writer, &mut semantic_hash, b"[")?;
    let mut candidate_bytes = 0u64;
    for (index, reference) in accepted.iter().enumerate() {
        if index > 0 {
            write_and_hash(&mut writer, &mut semantic_hash, b",")?;
        }
        candidate_bytes += copy_candidate(reference, &mut writer, &mut semantic_hash)?;
    }
    write_and_hash(&mut writer, &mut semantic_hash, b"]")?;
    write_and_hash(
        &mut writer,
        &mut semantic_hash,
        &shell[candidates_field.value.end..population_sha_field.member_start],
    )?;
    writer
        .write_all(&shell[population_sha_field.member_start..population_sha_field.value.start])?;
    let placeholder_offset = writer.stream_position()? + 1;
    writer.write_all(&shell[population_sha_field.value.clone()])?;
    writer.write_all(&shell[population_sha_field.value.end..population_sha_field.member_end])?;
    write_and_hash(
        &mut writer,
        &mut semantic_hash,
        &shell[population_sha_field.member_end..semantic_end],
    )?;
    writer.write_all(expected_newline)?;
    writer.flush()?;
    let population_sha256 = format!("sha256:{}", hex_digest(semantic_hash.finalize()));
    if population_sha256.len() != SHELL_PLACEHOLDER.len() {
        bail!("derived population SHA has an unexpected encoded length");
    }
    {
        let file = writer.get_mut();
        file.seek(SeekFrom::Start(placeholder_offset))?;
        file.write_all(population_sha256.as_bytes())?;
        file.sync_all()?;
    }
    drop(writer);
    let encoded_bytes = fs::metadata(&temporary)?.len();
    let existing = output.exists();
    if existing {
        if !files_exact(&temporary, &output)? {
            fs::remove_file(&temporary)?;
            bail!(
                "refusing to overwrite divergent pair-generation artifact: {}",
                output.display()
            );
        }
        fs::remove_file(&temporary)?;
    } else {
        fs::rename(&temporary, &output)?;
    }
    cleanup_stale_temporaries(&output, None)?;
    let assembly_ms = assembly_started.elapsed().as_secs_f64() * 1000.0;
    Ok(json!({
        "schemaVersion": "temporal_qd_population_finalizer_result_v1",
        "contractVersion": CONTRACT_VERSION,
        "candidateCount": accepted.len(),
        "journalBytesScanned": journal_bytes,
        "candidateBytesCopied": candidate_bytes,
        "encodedBytes": encoded_bytes,
        "populationSha256": population_sha256,
        "journalScanAndVerifyMs": scan_ms,
        "assemblyMs": assembly_ms,
        "totalMs": started.elapsed().as_secs_f64() * 1000.0,
        "existingArtifactVerified": existing,
    }))
}

fn version_json() -> Value {
    json!({
        "schemaVersion": "temporal_qd_population_finalizer_version_v1",
        "contractVersion": CONTRACT_VERSION,
        "crateVersion": env!("CARGO_PKG_VERSION"),
    })
}

fn run() -> Result<Value> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() == 2 && args[1] == "--version-json" {
        return Ok(version_json());
    }
    if args.len() == 3 && args[1] == "--manifest" {
        return finalize(Path::new(&args[2]));
    }
    bail!("usage: temporal-qd-population-finalizer --manifest PATH | --version-json")
}

fn main() {
    match run() {
        Ok(value) => println!("{}", serde_json::to_string(&value).unwrap()),
        Err(error) => {
            eprintln!("ERROR: {error:#}");
            std::process::exit(2);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parser_handles_escaped_nested_json_and_canonical_fields() {
        let bytes = br#"{"a":{"nested":["quote\\\"",-0.0,true,null]},"b":2}"#;
        let (end, fields) = object_fields(bytes, 0).unwrap();
        assert_eq!(end, bytes.len());
        assert_eq!(
            fields
                .iter()
                .map(|field| field.key.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b"]
        );
    }

    #[test]
    fn parser_rejects_noncanonical_field_order() {
        assert!(object_fields(br#"{"b":1,"a":2}"#, 0).is_err());
    }

    #[test]
    fn semantic_field_removal_preserves_canonical_bytes() {
        let bytes = br#"{"a":1,"hash":"x","z":2}"#;
        let (_, fields) = object_fields(bytes, 0).unwrap();
        let material = semantic_without_field(bytes, field(&fields, "hash").unwrap()).unwrap();
        assert_eq!(material, br#"{"a":1,"z":2}"#);
    }
}
