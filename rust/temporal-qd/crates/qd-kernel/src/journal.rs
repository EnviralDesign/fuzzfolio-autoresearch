//! Durable, write-once proposal segments and compact checkpoints.
//!
//! Public compatibility entries and private compact state are deliberately
//! separate.  A segment is the durable receipt; mutable in-memory indexes may
//! advance only after its file is sealed and verified.

#[cfg(debug_assertions)]
use std::cell::RefCell;

use std::{
    fs::{self, File, OpenOptions},
    io::{self, BufReader, Read, Seek, Write},
    path::{Component, Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};

use temporal_qd_contract::{
    ContractError, JsonNewline, Map, Value, canonical_sha256_streaming,
    canonical_sha256_without_object_field, python_pretty_json_line, write_canonical_json,
};

pub const SEGMENT_SCHEMA: &str = "temporal_qd_native_proposal_segment_v1";
pub const CHECKPOINT_SCHEMA: &str = "temporal_qd_native_proposal_checkpoint_v1";
pub const GENERATION_HEAD_SCHEMA: &str = "temporal_qd_native_generation_head_v1";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FinalNewline {
    Lf,
    Crlf,
}

/// Private-temporary streaming writer that may patch a fixed-width region
/// before the normal write-once publication step.  No implementation may use
/// it to mutate an installed artifact: callers receive only the disposable
/// temporary inode created by `ProposalJournal`.
pub trait RewritableTemporaryWrite: Write + Seek {}

impl<T: Write + Seek + ?Sized> RewritableTemporaryWrite for T {}

impl FinalNewline {
    pub const fn bytes(self) -> &'static [u8] {
        match self {
            Self::Lf => b"\n",
            Self::Crlf => b"\r\n",
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum JournalError {
    #[error("filesystem failure: {0}")]
    Io(#[from] io::Error),
    #[error("canonical contract failure: {0}")]
    Canonical(#[from] ContractError),
    #[error("proposal journal contract failure: {0}")]
    Contract(String),
}

pub type Result<T> = std::result::Result<T, JournalError>;

fn contract(message: impl Into<String>) -> JournalError {
    JournalError::Contract(message.into())
}

// A thread-local debug seam lets the integration suite simulate process loss
// after a write-once artifact is durable.  It is unavailable to release
// builds, and thread locality prevents one test's injected loss from leaking
// into another concurrent transaction.
#[cfg(debug_assertions)]
thread_local! {
    static G0_TEST_CRASH_AFTER_ARTIFACT: RefCell<Option<String>> = const { RefCell::new(None) };
}

#[cfg(debug_assertions)]
#[doc(hidden)]
pub fn set_g0_test_crash_after_artifact(relative: Option<&str>) {
    G0_TEST_CRASH_AFTER_ARTIFACT.with(|target| {
        *target.borrow_mut() = relative.map(ToOwned::to_owned);
    });
}

#[cfg(debug_assertions)]
pub(crate) fn g0_test_should_crash_after_artifact(relative: &Path) -> bool {
    G0_TEST_CRASH_AFTER_ARTIFACT
        .with(|target| target.borrow().as_deref() == Some(relative.to_string_lossy().as_ref()))
}

fn object(entries: impl IntoIterator<Item = (&'static str, Value)>) -> Value {
    let mut map = Map::new();
    for (key, value) in entries {
        map.insert(key.to_owned(), value);
    }
    Value::Object(map)
}

fn sha(value: &str, label: &str) -> Result<()> {
    if value.len() != 71
        || !value.starts_with("sha256:")
        || !value.as_bytes()[7..]
            .iter()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(contract(format!(
            "{label} must be a lowercase SHA-256 identity"
        )));
    }
    Ok(())
}

/// A compact accepted reference is enough to publish a population later.  The
/// rich candidate remains in its write-once proposal entry and is reopened at
/// most once while materialising the evaluation sidecar.
#[derive(Clone, Debug)]
pub struct AcceptedReference {
    pub proposal_ordinal: u64,
    pub candidate_id: String,
    pub candidate_identity_sha256: String,
    pub executable_semantic_sha256: String,
    pub entry_sha256: String,
    pub descriptor_projection: Option<Value>,
}

impl AcceptedReference {
    pub fn value(&self) -> Value {
        object([
            ("proposalOrdinal", Value::from(self.proposal_ordinal)),
            ("candidateId", Value::String(self.candidate_id.clone())),
            (
                "candidateIdentitySha256",
                Value::String(self.candidate_identity_sha256.clone()),
            ),
            (
                "executableSemanticSha256",
                Value::String(self.executable_semantic_sha256.clone()),
            ),
            ("entrySha256", Value::String(self.entry_sha256.clone())),
            (
                "descriptorProjection",
                self.descriptor_projection.clone().unwrap_or(Value::Null),
            ),
        ])
    }

    pub fn from_value(value: &Value) -> Result<Self> {
        let fields = value
            .as_object()
            .ok_or_else(|| contract("accepted reference must be an object"))?;
        let string = |field: &str| {
            fields
                .get(field)
                .and_then(Value::as_str)
                .map(ToOwned::to_owned)
                .ok_or_else(|| contract(format!("accepted reference lacks {field}")))
        };
        let candidate_identity_sha256 = string("candidateIdentitySha256")?;
        let executable_semantic_sha256 = string("executableSemanticSha256")?;
        let entry_sha256 = string("entrySha256")?;
        sha(&candidate_identity_sha256, "accepted candidate identity")?;
        sha(
            &executable_semantic_sha256,
            "accepted executable semantic identity",
        )?;
        sha(&entry_sha256, "accepted entry identity")?;
        Ok(Self {
            proposal_ordinal: fields
                .get("proposalOrdinal")
                .and_then(Value::as_u64)
                .ok_or_else(|| contract("accepted reference lacks proposal ordinal"))?,
            candidate_id: string("candidateId")?,
            candidate_identity_sha256,
            executable_semantic_sha256,
            entry_sha256,
            descriptor_projection: fields
                .get("descriptorProjection")
                .cloned()
                .filter(|value| !value.is_null()),
        })
    }
}

#[derive(Clone, Debug)]
pub struct SegmentInput {
    pub proposal_ordinal: u64,
    pub previous_segment_sha256: Option<String>,
    pub entry: Value,
    pub entry_sha256: String,
    pub ledger_delta: Option<Value>,
    pub descriptor_projection: Option<Value>,
    pub accepted_reference: Option<AcceptedReference>,
}

impl SegmentInput {
    pub fn value_without_hash(&self) -> Value {
        object([
            ("schemaVersion", Value::String(SEGMENT_SCHEMA.to_owned())),
            ("proposalOrdinal", Value::from(self.proposal_ordinal)),
            (
                "previousSegmentSha256",
                self.previous_segment_sha256
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Null),
            ),
            ("entry", self.entry.clone()),
            ("entrySha256", Value::String(self.entry_sha256.clone())),
            (
                "ledgerDelta",
                self.ledger_delta.clone().unwrap_or(Value::Null),
            ),
            (
                "descriptorProjection",
                self.descriptor_projection.clone().unwrap_or(Value::Null),
            ),
            (
                "acceptedReference",
                self.accepted_reference
                    .as_ref()
                    .map(AcceptedReference::value)
                    .unwrap_or(Value::Null),
            ),
        ])
    }

    pub fn sealed_value(&self) -> Result<(Value, String)> {
        sha(&self.entry_sha256, "proposal entry SHA-256")?;
        verify_self_hash(
            &self.entry,
            "entrySha256",
            crate::proposal::PROPOSAL_ENTRY_SCHEMA,
            "proposal entry",
        )?;
        if self.entry.get("entrySha256").and_then(Value::as_str) != Some(&self.entry_sha256) {
            return Err(contract(
                "proposal segment entry SHA-256 must bind the supplied rich entry",
            ));
        }
        if let Some(previous) = &self.previous_segment_sha256 {
            sha(previous, "previous segment SHA-256")?;
        }
        if let Some(reference) = &self.accepted_reference {
            sha(
                &reference.candidate_identity_sha256,
                "accepted candidate identity",
            )?;
            sha(
                &reference.executable_semantic_sha256,
                "accepted executable semantic identity",
            )?;
            sha(&reference.entry_sha256, "accepted entry identity")?;
            if reference.entry_sha256 != self.entry_sha256 {
                return Err(contract(
                    "accepted reference must bind its enclosing proposal entry",
                ));
            }
        }
        let mut value = self.value_without_hash();
        let hash = canonical_sha256_streaming(&value)?;
        value
            .as_object_mut()
            .expect("segment is object")
            .insert("segmentSha256".to_owned(), Value::String(hash.clone()));
        Ok((value, hash))
    }
}

#[derive(Clone, Debug)]
pub struct CheckpointInput {
    pub request_sha256: String,
    pub next_proposal_ordinal: u64,
    pub last_segment_sha256: Option<String>,
    pub proposal_state: Value,
    pub parent_selector_state: Value,
    pub ledger_state: Value,
    /// The mutable public Python identity-ledger facade is sealed after each
    /// segment commit.  This identity makes the private checkpoint explicitly
    /// bind the facade a Python continuation must reopen.
    pub public_ledger_sha256: Option<String>,
    pub accepted_references: Vec<AcceptedReference>,
    /// The entries remain in the checkpoint until their idempotent public
    /// publication succeeds.  A crash after the checkpoint therefore repairs
    /// the public compatibility facade without replaying rich history.
    pub pending_public_entries: Vec<(u64, Value)>,
}

impl CheckpointInput {
    pub fn sealed_value(&self) -> Result<(Value, String)> {
        sha(&self.request_sha256, "generation request SHA-256")?;
        if let Some(last) = &self.last_segment_sha256 {
            sha(last, "checkpoint segment SHA-256")?;
        }
        if let Some(ledger) = &self.public_ledger_sha256 {
            sha(ledger, "checkpoint public identity ledger SHA-256")?;
        }
        let mut value = object([
            ("schemaVersion", Value::String(CHECKPOINT_SCHEMA.to_owned())),
            ("requestSha256", Value::String(self.request_sha256.clone())),
            (
                "nextProposalOrdinal",
                Value::from(self.next_proposal_ordinal),
            ),
            (
                "lastSegmentSha256",
                self.last_segment_sha256
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Null),
            ),
            ("proposalState", self.proposal_state.clone()),
            ("parentSelectorState", self.parent_selector_state.clone()),
            ("ledgerState", self.ledger_state.clone()),
            (
                "publicLedgerSha256",
                self.public_ledger_sha256
                    .clone()
                    .map(Value::String)
                    .unwrap_or(Value::Null),
            ),
            (
                "acceptedReferences",
                Value::Array(
                    self.accepted_references
                        .iter()
                        .map(AcceptedReference::value)
                        .collect(),
                ),
            ),
            (
                "pendingPublicEntries",
                Value::Array(
                    self.pending_public_entries
                        .iter()
                        .map(|(ordinal, entry)| {
                            object([
                                ("proposalOrdinal", Value::from(*ordinal)),
                                ("entry", entry.clone()),
                            ])
                        })
                        .collect(),
                ),
            ),
        ]);
        let hash = canonical_sha256_streaming(&value)?;
        value
            .as_object_mut()
            .expect("checkpoint is object")
            .insert("checkpointSha256".to_owned(), Value::String(hash.clone()));
        Ok((value, hash))
    }
}

#[derive(Clone, Debug)]
pub struct WrittenArtifact {
    pub path: PathBuf,
    pub file_sha256: String,
    pub encoded_bytes: u64,
    pub existed: bool,
}

#[derive(Debug)]
pub struct ProposalJournal {
    root: PathBuf,
    canonical_root: PathBuf,
    public_newline: FinalNewline,
}

impl ProposalJournal {
    pub fn open(root: impl AsRef<Path>, newline: FinalNewline) -> Result<Self> {
        // Do not let `create_dir_all` silently follow a caller-controlled
        // link/junction on the way to the generation root.  The batch binary
        // already validates its manifest path, but the kernel is also used
        // directly by tests and library callers, so the write authority must
        // be self-contained here.
        let root = absolute_path(root.as_ref())?;
        let root = ensure_safe_directory_tree(&root, "generation root")?;
        let canonical_root = fs::canonicalize(&root).map_err(|error| {
            contract(format!("canonicalize safe generation root failed: {error}"))
        })?;
        ensure_safe_existing_directory(&root, "generation root")?;
        for relative in [
            Path::new("proposal-journal"),
            Path::new("internal"),
            Path::new("internal/segments"),
            Path::new("internal/checkpoints"),
        ] {
            ensure_safe_relative_directory(&root, relative, "proposal journal directory")?;
        }
        Ok(Self {
            root,
            canonical_root,
            public_newline: newline,
        })
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub const fn public_newline(&self) -> FinalNewline {
        self.public_newline
    }

    pub fn write_segment(&self, input: &SegmentInput) -> Result<(WrittenArtifact, String)> {
        let (value, segment_sha256) = input.sealed_value()?;
        let artifact = self.write_canonical_once_with_newline(
            &PathBuf::from("internal/segments").join(format!("{:08}.json", input.proposal_ordinal)),
            &value,
            FinalNewline::Lf,
        )?;
        Ok((artifact, segment_sha256))
    }

    pub fn write_checkpoint(&self, input: &CheckpointInput) -> Result<(WrittenArtifact, String)> {
        let (value, checkpoint_sha256) = input.sealed_value()?;
        let ordinal = input.next_proposal_ordinal;
        let artifact = self.write_canonical_once_with_newline(
            &PathBuf::from("internal/checkpoints").join(format!("{:08}.json", ordinal)),
            &value,
            FinalNewline::Lf,
        )?;
        Ok((artifact, checkpoint_sha256))
    }

    pub fn write_public_entry(&self, ordinal: u64, entry: &Value) -> Result<WrittenArtifact> {
        verify_self_hash(
            entry,
            "entrySha256",
            crate::proposal::PROPOSAL_ENTRY_SCHEMA,
            "proposal entry",
        )?;
        self.write_canonical_once(
            &PathBuf::from("proposal-journal").join(format!("{:08}.json", ordinal)),
            entry,
        )
    }

    pub fn read_public_entry(&self, ordinal: u64) -> Result<Value> {
        Ok(self.read_public_entry_with_bytes(ordinal)?.0)
    }

    /// Read and authenticate one sealed proposal row, returning the exact
    /// source bytes consumed alongside its decoded value.  The G0 funnel uses
    /// this for untrusted performance diagnostics; the byte count is never
    /// admitted into a semantic artifact or receipt.
    pub fn read_public_entry_with_bytes(&self, ordinal: u64) -> Result<(Value, u64)> {
        let relative = PathBuf::from("proposal-journal").join(format!("{ordinal:08}.json"));
        let path = self.existing_artifact_path(&relative)?;
        let bytes = fs::read(path)?;
        let byte_len = bytes.len() as u64;
        let value = read_canonical_document_bytes(&bytes, self.public_newline)?;
        verify_self_hash(
            &value,
            "entrySha256",
            crate::proposal::PROPOSAL_ENTRY_SCHEMA,
            "proposal entry",
        )?;
        Ok((value, byte_len))
    }

    pub fn read_artifact(&self, relative: &Path) -> Result<Value> {
        read_canonical_document(&self.existing_artifact_path(relative)?, self.public_newline)
    }

    pub fn artifact_file_sha256(&self, relative: &Path) -> Result<String> {
        file_sha256(&self.existing_artifact_path(relative)?)
    }

    /// Read metadata for a sealed artifact without reopening its contents.
    /// Diagnostic callers use this only after the normal write-once or
    /// receipt verification path has already authenticated the artifact.
    pub fn artifact_encoded_bytes(&self, relative: &Path) -> Result<u64> {
        Ok(fs::metadata(self.existing_artifact_path(relative)?)?.len())
    }

    pub fn load_segment(&self, proposal_ordinal: u64) -> Result<Value> {
        let path = self.existing_artifact_path(
            &PathBuf::from("internal/segments").join(format!("{proposal_ordinal:08}.json")),
        )?;
        let value = read_canonical_document(&path, FinalNewline::Lf)?;
        verify_self_hash(&value, "segmentSha256", SEGMENT_SCHEMA, "proposal segment")?;
        Ok(value)
    }

    pub fn write_canonical_once(&self, relative: &Path, value: &Value) -> Result<WrittenArtifact> {
        self.write_canonical_once_with_newline(relative, value, self.public_newline)
    }

    fn write_canonical_once_with_newline(
        &self,
        relative: &Path,
        value: &Value,
        newline: FinalNewline,
    ) -> Result<WrittenArtifact> {
        self.write_once_with_newline(relative, newline, |writer| {
            write_canonical_value(value, writer)
        })
    }

    /// Atomically publish a canonical document assembled incrementally.  The
    /// caller is responsible for emitting one complete canonical object; this
    /// preserves the same write-once, fsync, collision, and file-hash checks
    /// as value-backed publication while avoiding a full rich-population
    /// `serde_json::Value` in memory.
    pub fn write_canonical_once_streaming<F>(
        &self,
        relative: &Path,
        newline: FinalNewline,
        write: F,
    ) -> Result<WrittenArtifact>
    where
        F: FnOnce(&mut dyn Write) -> Result<()>,
    {
        self.write_once_with_newline(relative, newline, write)
    }

    /// Atomically publish a canonical streaming document that needs one
    /// fixed-width placeholder patched in its private temporary file before
    /// hashing, fsync, and write-once installation.  The final component never
    /// observes the placeholder: this reuses the ordinary exact-existing and
    /// reparse-safe publisher after the caller has sealed the final bytes.
    pub fn write_canonical_once_streaming_rewritable<F>(
        &self,
        relative: &Path,
        newline: FinalNewline,
        write: F,
    ) -> Result<WrittenArtifact>
    where
        F: FnOnce(&mut dyn RewritableTemporaryWrite) -> Result<()>,
    {
        self.write_once_with_newline_rewritable(relative, newline, write)
    }

    fn write_once_with_newline<F>(
        &self,
        relative: &Path,
        newline: FinalNewline,
        write: F,
    ) -> Result<WrittenArtifact>
    where
        F: FnOnce(&mut dyn Write) -> Result<()>,
    {
        let destination = self.artifact_path(relative, true)?;
        let temporary = temporary_path(&destination);
        let (encoded_bytes, temporary_sha) = write_streaming_file(&temporary, newline, write)?;
        self.publish_temporary_once(
            relative,
            destination,
            temporary,
            encoded_bytes,
            temporary_sha,
        )
    }

    fn write_once_with_newline_rewritable<F>(
        &self,
        relative: &Path,
        newline: FinalNewline,
        write: F,
    ) -> Result<WrittenArtifact>
    where
        F: FnOnce(&mut dyn RewritableTemporaryWrite) -> Result<()>,
    {
        let destination = self.artifact_path(relative, true)?;
        let temporary = temporary_path(&destination);
        let (encoded_bytes, temporary_sha) =
            write_rewritable_streaming_file(&temporary, newline, write)?;
        self.publish_temporary_once(
            relative,
            destination,
            temporary,
            encoded_bytes,
            temporary_sha,
        )
    }

    fn publish_temporary_once(
        &self,
        relative: &Path,
        destination: PathBuf,
        temporary: PathBuf,
        encoded_bytes: u64,
        temporary_sha: String,
    ) -> Result<WrittenArtifact> {
        let parent = destination
            .parent()
            .expect("validated artifact path has a parent");
        // Recheck every enclosing component after the stream has been sealed
        // and immediately before its inode gains the public name.  This
        // closes the normal link/junction alias case and turns a concurrent
        // path substitution into a fail-closed transaction.
        self.ensure_artifact_parent(parent, "artifact parent")?;
        reject_link_or_non_regular_file(&destination, "write-once artifact")?;
        let installed = match fs::hard_link(&temporary, &destination) {
            Ok(()) => {
                // The temporary inode is sealed before it gains its public
                // name.  A hard link refuses an existing destination, unlike
                // a replace/rename operation which may overwrite it.
                fs::remove_file(&temporary)?;
                false
            }
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {
                self.ensure_artifact_parent(parent, "artifact parent")?;
                reject_link_or_non_regular_file(&destination, "write-once artifact")?;
                let matches = files_equal(&temporary, &destination)?;
                fs::remove_file(&temporary)?;
                if !matches {
                    return Err(contract(format!(
                        "refusing to overwrite divergent write-once artifact: {}",
                        relative.display()
                    )));
                }
                true
            }
            Err(error) => {
                let _ = fs::remove_file(&temporary);
                return Err(error.into());
            }
        };
        self.ensure_artifact_parent(parent, "artifact parent")?;
        ensure_safe_existing_regular_file(&destination, "write-once artifact")?;
        let metadata = fs::metadata(&destination)?;
        if metadata.len() != encoded_bytes {
            return Err(contract(
                "write-once artifact byte count drifted after publication",
            ));
        }
        let destination_sha = file_sha256(&destination)?;
        if destination_sha != temporary_sha {
            return Err(contract(
                "write-once artifact hash drifted after publication",
            ));
        }
        sync_parent_directory(parent)?;
        // This narrowly-scoped debug failpoint models a process loss after a
        // public artifact has reached durable storage but before its caller
        // can publish the next artifact/receipt.  It is deliberately absent
        // from release builds and only exists to prove that G0 can resume an
        // exact write-once prefix without trusting it.
        #[cfg(debug_assertions)]
        if g0_test_should_crash_after_artifact(relative) {
            return Err(contract(format!(
                "injected G0 crash after durable artifact: {}",
                relative.display()
            )));
        }
        Ok(WrittenArtifact {
            path: destination,
            file_sha256: destination_sha,
            encoded_bytes,
            existed: installed,
        })
    }

    /// Atomically advance the intentionally mutable Python identity-ledger
    /// facade.  All proposal/public artifacts remain write-once; this narrow
    /// replacement primitive is for checkpointed campaign identity state.
    pub fn write_public_identity_ledger(&self, value: &Value) -> Result<WrittenArtifact> {
        let relative = Path::new("identity-ledger.json");
        let destination = self.artifact_path(relative, true)?;
        let parent = destination
            .parent()
            .ok_or_else(|| contract("mutable artifact path lacks a parent"))?;
        let temporary = temporary_path(&destination);
        let (encoded_bytes, expected_sha) = write_python_pretty_file(&temporary, value)?;
        self.ensure_artifact_parent(parent, "mutable artifact parent")?;
        reject_link_or_non_regular_file(&destination, "mutable artifact")?;
        let existed = fs::symlink_metadata(&destination).is_ok();
        let installed = replace_file_atomically(&temporary, &destination);
        if let Err(error) = installed {
            let _ = fs::remove_file(&temporary);
            return Err(error.into());
        }
        self.ensure_artifact_parent(parent, "mutable artifact parent")?;
        ensure_safe_existing_regular_file(&destination, "mutable artifact")?;
        let metadata = fs::metadata(&destination)?;
        if metadata.len() != encoded_bytes || file_sha256(&destination)? != expected_sha {
            return Err(contract("mutable artifact bytes drifted after replacement"));
        }
        sync_parent_directory(parent)?;
        Ok(WrittenArtifact {
            path: destination,
            file_sha256: expected_sha,
            encoded_bytes,
            existed,
        })
    }

    pub fn read_public_identity_ledger(&self) -> Result<Value> {
        read_python_pretty_document(
            &self.existing_artifact_path(Path::new("identity-ledger.json"))?,
        )
    }

    pub fn load_checkpoint(&self, next_ordinal: u64) -> Result<Value> {
        let path = self.existing_artifact_path(
            &PathBuf::from("internal/checkpoints").join(format!("{next_ordinal:08}.json")),
        )?;
        let value = read_canonical_document(&path, FinalNewline::Lf)?;
        verify_self_hash(&value, "checkpointSha256", CHECKPOINT_SCHEMA, "checkpoint")?;
        Ok(value)
    }

    pub fn write_generation_head(&self, head: &Value) -> Result<WrittenArtifact> {
        verify_self_hash(
            head,
            "generationHeadSha256",
            GENERATION_HEAD_SCHEMA,
            "generation head",
        )?;
        self.write_canonical_once_with_newline(
            Path::new("internal/generation-head.json"),
            head,
            FinalNewline::Lf,
        )
    }

    pub fn load_generation_head(&self) -> Result<Option<Value>> {
        let path = match self.existing_artifact_path(Path::new("internal/generation-head.json")) {
            Ok(path) => path,
            Err(JournalError::Io(error)) if error.kind() == io::ErrorKind::NotFound => {
                return Ok(None);
            }
            Err(error) => return Err(error),
        };
        match read_canonical_document(&path, FinalNewline::Lf) {
            Ok(value) => {
                verify_self_hash(
                    &value,
                    "generationHeadSha256",
                    GENERATION_HEAD_SCHEMA,
                    "generation head",
                )?;
                Ok(Some(value))
            }
            Err(JournalError::Io(error)) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Resolve a sealed artifact only after checking every component below the
    /// journal root.  `None` represents an absent artifact, not an alias.
    pub fn safe_existing_artifact(&self, relative: &Path) -> Result<Option<PathBuf>> {
        match self.existing_artifact_path(relative) {
            Ok(path) => Ok(Some(path)),
            Err(JournalError::Io(error)) if error.kind() == io::ErrorKind::NotFound => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// Resolve an existing journal directory without following a symlink or a
    /// Windows junction/reparse point in any component below the root.
    pub fn safe_existing_directory(&self, relative: &Path) -> Result<PathBuf> {
        validate_relative(relative)?;
        self.ensure_artifact_parent(&self.root, "generation root")?;
        let path = self.root.join(relative);
        ensure_safe_existing_directory(&path, "journal directory")?;
        self.ensure_artifact_parent(&path, "journal directory")?;
        Ok(path)
    }

    fn existing_artifact_path(&self, relative: &Path) -> Result<PathBuf> {
        let destination = self.artifact_path(relative, false)?;
        ensure_safe_existing_regular_file(&destination, "artifact")?;
        Ok(destination)
    }

    fn artifact_path(&self, relative: &Path, create_parent: bool) -> Result<PathBuf> {
        validate_relative(relative)?;
        let parent_relative = relative
            .parent()
            .filter(|path| !path.as_os_str().is_empty());
        let parent = match parent_relative {
            Some(parent_relative) if create_parent => {
                ensure_safe_relative_directory(&self.root, parent_relative, "artifact parent")?
            }
            Some(parent_relative) => ensure_safe_existing_relative_directory(
                &self.root,
                parent_relative,
                "artifact parent",
            )?,
            None => self.root.clone(),
        };
        self.ensure_artifact_parent(&parent, "artifact parent")?;
        let name = relative
            .file_name()
            .ok_or_else(|| contract("artifact path lacks a final file component"))?;
        let destination = parent.join(name);
        reject_link_or_non_regular_file(&destination, "artifact")?;
        Ok(destination)
    }

    fn ensure_artifact_parent(&self, parent: &Path, label: &str) -> Result<()> {
        ensure_safe_existing_directory(&self.root, "generation root")?;
        ensure_safe_existing_directory(parent, label)?;
        let canonical_root = fs::canonicalize(&self.root)?;
        if canonical_root != self.canonical_root {
            return Err(contract("generation root canonical identity drifted"));
        }
        let canonical_parent = fs::canonicalize(parent)?;
        if !canonical_parent.starts_with(&self.canonical_root) {
            return Err(contract(format!(
                "{label} escapes generation root through a link or reparse alias"
            )));
        }
        Ok(())
    }
}

#[cfg(unix)]
fn sync_parent_directory(path: &Path) -> Result<()> {
    File::open(path)?.sync_all()?;
    Ok(())
}

/// Windows only flushes a directory entry when the directory is opened with
/// `FILE_FLAG_BACKUP_SEMANTICS`.  Keep the same capability-based fallback as
/// qd-batch: on filesystems that explicitly do not implement a directory
/// flush, the payload was already synced before its write-once link install.
#[cfg(windows)]
fn sync_parent_directory(path: &Path) -> Result<()> {
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
        Err(error) => return Err(error.into()),
    };
    match directory.sync_all() {
        Err(error) if is_unsupported_windows_directory_sync(&error) => Ok(()),
        Err(error) => Err(error.into()),
        Ok(()) => Ok(()),
    }
}

#[cfg(windows)]
fn is_unsupported_windows_directory_sync(error: &io::Error) -> bool {
    matches!(error.raw_os_error(), Some(1 | 5 | 50 | 87))
}

#[cfg(not(windows))]
fn replace_file_atomically(source: &Path, destination: &Path) -> io::Result<()> {
    fs::rename(source, destination)
}

#[cfg(windows)]
fn replace_file_atomically(source: &Path, destination: &Path) -> io::Result<()> {
    use std::os::windows::ffi::OsStrExt;

    const REPLACEFILE_WRITE_THROUGH: u32 = 0x0000_0001;

    unsafe extern "system" {
        fn ReplaceFileW(
            replaced_file_name: *const u16,
            replacement_file_name: *const u16,
            backup_file_name: *const u16,
            replace_flags: u32,
            exclude: *mut std::ffi::c_void,
            reserved: *mut std::ffi::c_void,
        ) -> i32;
    }

    fn wide(path: &Path) -> Vec<u16> {
        path.as_os_str()
            .encode_wide()
            .chain(std::iter::once(0))
            .collect()
    }

    if !destination.exists() {
        match fs::rename(source, destination) {
            Ok(()) => return Ok(()),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
    }
    let destination_wide = wide(destination);
    let source_wide = wide(source);
    // `ReplaceFileW` is the Windows atomic replacement primitive.  It also
    // requests write-through so the subsequent directory sync closes the
    // durable publication boundary used by checkpoints.
    let result = unsafe {
        ReplaceFileW(
            destination_wide.as_ptr(),
            source_wide.as_ptr(),
            std::ptr::null(),
            REPLACEFILE_WRITE_THROUGH,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    };
    if result == 0 {
        Err(io::Error::last_os_error())
    } else {
        Ok(())
    }
}

pub fn self_hashed_generation_head(
    generation_index: u64,
    request_sha256: &str,
    checkpoint_sha256: &str,
    population_sha256: &str,
    evaluation_population_sha256: &str,
    journal_sha256: &str,
) -> Result<Value> {
    for (value, label) in [
        (request_sha256, "generation request SHA-256"),
        (checkpoint_sha256, "checkpoint SHA-256"),
        (population_sha256, "population SHA-256"),
        (
            evaluation_population_sha256,
            "evaluation population SHA-256",
        ),
        (journal_sha256, "generation journal SHA-256"),
    ] {
        sha(value, label)?;
    }
    let mut head = object([
        (
            "schemaVersion",
            Value::String(GENERATION_HEAD_SCHEMA.to_owned()),
        ),
        ("generationIndex", Value::from(generation_index)),
        ("requestSha256", Value::String(request_sha256.to_owned())),
        (
            "checkpointSha256",
            Value::String(checkpoint_sha256.to_owned()),
        ),
        (
            "populationSha256",
            Value::String(population_sha256.to_owned()),
        ),
        (
            "evaluationPopulationSha256",
            Value::String(evaluation_population_sha256.to_owned()),
        ),
        ("journalSha256", Value::String(journal_sha256.to_owned())),
    ]);
    let hash = canonical_sha256_streaming(&head)?;
    head.as_object_mut()
        .expect("generation head is object")
        .insert("generationHeadSha256".to_owned(), Value::String(hash));
    Ok(head)
}

pub fn verify_self_hash(value: &Value, field: &str, schema: &str, label: &str) -> Result<()> {
    let fields = value
        .as_object()
        .ok_or_else(|| contract(format!("{label} must be an object")))?;
    if fields.get("schemaVersion").and_then(Value::as_str) != Some(schema) {
        return Err(contract(format!("{label} schema is incompatible")));
    }
    let supplied = fields
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| contract(format!("{label} lacks {field}")))?;
    sha(supplied, field)?;
    if canonical_sha256_without_object_field(value, field)? != supplied {
        return Err(contract(format!("{label} identity mismatch")));
    }
    Ok(())
}

fn validate_relative(relative: &Path) -> Result<()> {
    if relative.as_os_str().is_empty()
        || relative.is_absolute()
        || relative.components().any(|component| {
            matches!(
                component,
                Component::CurDir
                    | Component::ParentDir
                    | Component::RootDir
                    | Component::Prefix(_)
            )
        })
    {
        return Err(contract("artifact path escapes generation root"));
    }
    Ok(())
}

fn absolute_path(path: &Path) -> Result<PathBuf> {
    let absolute = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()?.join(path)
    };
    if absolute
        .components()
        .any(|component| matches!(component, Component::CurDir | Component::ParentDir))
    {
        return Err(contract(
            "generation root path contains an unsafe component",
        ));
    }
    Ok(absolute)
}

/// Component-wise root construction based on the qd-batch executable's
/// authority-path checker.  `create_dir_all` is deliberately avoided because
/// it follows a pre-existing symlink/junction before the caller can inspect
/// it.
fn ensure_safe_directory_tree(path: &Path, label: &str) -> Result<PathBuf> {
    if !path.is_absolute() {
        return Err(contract(format!("{label} path must be absolute")));
    }
    let mut current = PathBuf::new();
    for component in path.components() {
        match component {
            Component::Prefix(_) | Component::RootDir | Component::Normal(_) => {
                current.push(component.as_os_str());
            }
            Component::CurDir | Component::ParentDir => {
                return Err(contract(format!(
                    "{label} path contains an unsafe component"
                )));
            }
        }
        match fs::symlink_metadata(&current) {
            Ok(metadata) => ensure_real_directory(&metadata, label, &current)?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                match fs::create_dir(&current) {
                    Ok(()) => {}
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                    Err(error) => return Err(error.into()),
                }
                let metadata = fs::symlink_metadata(&current)?;
                ensure_real_directory(&metadata, label, &current)?;
            }
            Err(error) => return Err(error.into()),
        }
    }
    Ok(path.to_path_buf())
}

fn ensure_safe_relative_directory(root: &Path, relative: &Path, label: &str) -> Result<PathBuf> {
    validate_relative(relative)?;
    ensure_safe_existing_directory(root, "generation root")?;
    let mut path = root.to_path_buf();
    for component in relative.components() {
        let Component::Normal(component) = component else {
            return Err(contract(format!(
                "{label} path contains an unsafe component"
            )));
        };
        path.push(component);
        match fs::symlink_metadata(&path) {
            Ok(metadata) => ensure_real_directory(&metadata, label, &path)?,
            Err(error) if error.kind() == io::ErrorKind::NotFound => {
                match fs::create_dir(&path) {
                    Ok(()) => {}
                    Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
                    Err(error) => return Err(error.into()),
                }
                let metadata = fs::symlink_metadata(&path)?;
                ensure_real_directory(&metadata, label, &path)?;
            }
            Err(error) => return Err(error.into()),
        }
    }
    ensure_relative_containment(root, &path, label)?;
    Ok(path)
}

fn ensure_safe_existing_relative_directory(
    root: &Path,
    relative: &Path,
    label: &str,
) -> Result<PathBuf> {
    validate_relative(relative)?;
    let path = root.join(relative);
    ensure_safe_existing_directory(&path, label)?;
    ensure_relative_containment(root, &path, label)?;
    Ok(path)
}

fn ensure_safe_existing_directory(path: &Path, label: &str) -> Result<()> {
    ensure_safe_existing_components(path, label, true)
}

fn ensure_safe_existing_regular_file(path: &Path, label: &str) -> Result<()> {
    ensure_safe_existing_components(path, label, false)
}

fn ensure_safe_existing_components(
    path: &Path,
    label: &str,
    final_is_directory: bool,
) -> Result<()> {
    if !path.is_absolute() {
        return Err(contract(format!("{label} path must be absolute")));
    }
    let components: Vec<_> = path.components().collect();
    if components.is_empty() {
        return Err(contract(format!("{label} path is empty")));
    }
    let mut current = PathBuf::new();
    for (index, component) in components.iter().enumerate() {
        match component {
            Component::Prefix(_) | Component::RootDir | Component::Normal(_) => {
                current.push(component.as_os_str());
            }
            Component::CurDir | Component::ParentDir => {
                return Err(contract(format!(
                    "{label} path contains an unsafe component"
                )));
            }
        }
        let metadata = fs::symlink_metadata(&current)?;
        if is_link_or_reparse(&metadata) {
            return Err(contract(format!(
                "{label} contains a symlink or Windows reparse point: {}",
                current.display()
            )));
        }
        let is_final = index + 1 == components.len();
        if (!is_final || final_is_directory) && !metadata.is_dir() {
            return Err(contract(format!(
                "{label} parent is not a real directory: {}",
                current.display()
            )));
        }
        if is_final && !final_is_directory && !metadata.is_file() {
            return Err(contract(format!(
                "{label} is not a regular file: {}",
                current.display()
            )));
        }
    }
    Ok(())
}

fn ensure_real_directory(metadata: &fs::Metadata, label: &str, path: &Path) -> Result<()> {
    if is_link_or_reparse(metadata) || !metadata.is_dir() {
        return Err(contract(format!(
            "{label} contains a non-directory or symlink/reparse point: {}",
            path.display()
        )));
    }
    Ok(())
}

fn ensure_relative_containment(root: &Path, path: &Path, label: &str) -> Result<()> {
    let canonical_root = fs::canonicalize(root)?;
    let canonical_path = fs::canonicalize(path)?;
    if !canonical_path.starts_with(&canonical_root) {
        return Err(contract(format!(
            "{label} escapes generation root through a link or reparse alias"
        )));
    }
    Ok(())
}

/// Reject an existing final component that could cause an otherwise
/// byte-exact write-once comparison to follow an alias.  A missing final
/// component is the only non-file state permitted to a publisher.
fn reject_link_or_non_regular_file(path: &Path, label: &str) -> Result<()> {
    match fs::symlink_metadata(path) {
        Ok(metadata) if is_link_or_reparse(&metadata) || !metadata.is_file() => Err(contract(
            format!("{label} must be a non-symlink, non-reparse regular file"),
        )),
        Ok(_) => Ok(()),
        Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
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

static TEMPORARY_COUNTER: AtomicU64 = AtomicU64::new(0);

fn temporary_path(destination: &Path) -> PathBuf {
    let counter = TEMPORARY_COUNTER.fetch_add(1, Ordering::Relaxed);
    let file_name = destination
        .file_name()
        .and_then(|name| name.to_str())
        .unwrap_or("artifact");
    destination.with_file_name(format!(
        ".{file_name}.native-tmp-{}-{counter}",
        std::process::id()
    ))
}

fn write_streaming_file<F>(path: &Path, newline: FinalNewline, write: F) -> Result<(u64, String)>
where
    F: FnOnce(&mut dyn Write) -> Result<()>,
{
    let file = OpenOptions::new().write(true).create_new(true).open(path)?;
    let mut writer = io::BufWriter::with_capacity(1024 * 1024, file);
    let mut counter = CountingWriter::new(&mut writer);
    write(&mut counter)?;
    counter.write_all(newline.bytes())?;
    counter.flush()?;
    let bytes = counter.written;
    writer.get_ref().sync_all()?;
    drop(writer);
    Ok((bytes, file_sha256(path)?))
}

fn write_rewritable_streaming_file<F>(
    path: &Path,
    newline: FinalNewline,
    write: F,
) -> Result<(u64, String)>
where
    F: FnOnce(&mut dyn RewritableTemporaryWrite) -> Result<()>,
{
    let file = OpenOptions::new().write(true).create_new(true).open(path)?;
    let mut writer = RewritableBufferedWriter {
        inner: io::BufWriter::with_capacity(1024 * 1024, file),
    };
    write(&mut writer)?;
    writer.write_all(newline.bytes())?;
    let bytes = writer.seal()?;
    drop(writer);
    Ok((bytes, file_sha256(path)?))
}

struct RewritableBufferedWriter {
    inner: io::BufWriter<File>,
}

impl RewritableBufferedWriter {
    fn seal(&mut self) -> io::Result<u64> {
        self.inner.flush()?;
        let bytes = self.inner.get_ref().metadata()?.len();
        self.inner.get_ref().sync_all()?;
        Ok(bytes)
    }
}

impl Write for RewritableBufferedWriter {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        self.inner.write(buffer)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

impl Seek for RewritableBufferedWriter {
    fn seek(&mut self, position: io::SeekFrom) -> io::Result<u64> {
        self.inner.flush()?;
        self.inner.seek(position)
    }
}

fn write_python_pretty_file(path: &Path, value: &Value) -> Result<(u64, String)> {
    let encoded = python_pretty_json_line(value, JsonNewline::Lf)?;
    let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
    file.write_all(&encoded)?;
    file.sync_all()?;
    Ok((encoded.len() as u64, file_sha256(path)?))
}

fn read_canonical_document(path: &Path, newline: FinalNewline) -> Result<Value> {
    let bytes = fs::read(path)?;
    read_canonical_document_bytes(&bytes, newline)
}

fn read_canonical_document_bytes(bytes: &[u8], newline: FinalNewline) -> Result<Value> {
    if !bytes.ends_with(newline.bytes()) {
        return Err(contract("artifact newline convention is incompatible"));
    }
    let semantic = &bytes[..bytes.len() - newline.bytes().len()];
    let value: Value = serde_json::from_slice(semantic)
        .map_err(|error| contract(format!("artifact JSON is invalid: {error}")))?;
    let mut expected = Vec::new();
    write_canonical_value(&value, &mut expected)?;
    if expected != semantic {
        return Err(contract("artifact bytes are not canonical JSON"));
    }
    Ok(value)
}

fn read_python_pretty_document(path: &Path) -> Result<Value> {
    let bytes = fs::read(path)?;
    let value: Value = serde_json::from_slice(&bytes)
        .map_err(|error| contract(format!("artifact JSON is invalid: {error}")))?;
    let expected = python_pretty_json_line(&value, JsonNewline::Lf)?;
    if expected != bytes {
        return Err(contract(
            "identity-ledger bytes are not exact Python pretty JSON",
        ));
    }
    Ok(value)
}

fn files_equal(left: &Path, right: &Path) -> Result<bool> {
    let left_meta = fs::metadata(left)?;
    let right_meta = fs::metadata(right)?;
    if left_meta.len() != right_meta.len() {
        return Ok(false);
    }
    let mut left = BufReader::with_capacity(1024 * 1024, File::open(left)?);
    let mut right = BufReader::with_capacity(1024 * 1024, File::open(right)?);
    let mut left_buffer = [0_u8; 8192];
    let mut right_buffer = [0_u8; 8192];
    loop {
        let left_size = left.read(&mut left_buffer)?;
        let right_size = right.read(&mut right_buffer)?;
        if left_size != right_size || left_buffer[..left_size] != right_buffer[..right_size] {
            return Ok(false);
        }
        if left_size == 0 {
            return Ok(true);
        }
    }
}

struct CountingWriter<'a, W> {
    inner: &'a mut W,
    written: u64,
}

impl<'a, W> CountingWriter<'a, W> {
    fn new(inner: &'a mut W) -> Self {
        Self { inner, written: 0 }
    }
}

impl<W: Write> Write for CountingWriter<'_, W> {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        let written = self.inner.write(buffer)?;
        self.written = self.written.saturating_add(written as u64);
        Ok(written)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// Canonical JSON streamed directly to a file.  This matches the shared
/// contract's supported value domain and is used for artifact bytes only; all
/// semantic SHA-256 identities still call `temporal_qd_contract`.
fn write_canonical_value(value: &Value, writer: &mut dyn Write) -> Result<()> {
    write_canonical_json(value, writer)?;
    Ok(())
}

/// Small streaming SHA-256 implementation so this crate can hash on-disk
/// artifact bytes without adding a new direct dependency or buffering a large
/// population in memory.
struct StreamingSha256 {
    state: [u32; 8],
    buffer: [u8; 64],
    buffer_len: usize,
    bit_len: u64,
}

impl StreamingSha256 {
    fn new() -> Self {
        Self {
            state: [
                0x6a09_e667,
                0xbb67_ae85,
                0x3c6e_f372,
                0xa54f_f53a,
                0x510e_527f,
                0x9b05_688c,
                0x1f83_d9ab,
                0x5be0_cd19,
            ],
            buffer: [0; 64],
            buffer_len: 0,
            bit_len: 0,
        }
    }

    fn update(&mut self, mut bytes: &[u8]) {
        self.bit_len = self
            .bit_len
            .wrapping_add((bytes.len() as u64).wrapping_mul(8));
        if self.buffer_len != 0 {
            let take = (64 - self.buffer_len).min(bytes.len());
            self.buffer[self.buffer_len..self.buffer_len + take].copy_from_slice(&bytes[..take]);
            self.buffer_len += take;
            bytes = &bytes[take..];
            if self.buffer_len == 64 {
                self.transform(self.buffer);
                self.buffer_len = 0;
            }
        }
        while bytes.len() >= 64 {
            let mut block = [0_u8; 64];
            block.copy_from_slice(&bytes[..64]);
            self.transform(block);
            bytes = &bytes[64..];
        }
        self.buffer[..bytes.len()].copy_from_slice(bytes);
        self.buffer_len = bytes.len();
    }

    fn finish(mut self) -> [u8; 32] {
        self.buffer[self.buffer_len] = 0x80;
        self.buffer_len += 1;
        if self.buffer_len > 56 {
            self.buffer[self.buffer_len..].fill(0);
            self.transform(self.buffer);
            self.buffer_len = 0;
        }
        self.buffer[self.buffer_len..56].fill(0);
        self.buffer[56..].copy_from_slice(&self.bit_len.to_be_bytes());
        self.transform(self.buffer);
        let mut output = [0_u8; 32];
        for (index, word) in self.state.iter().enumerate() {
            output[index * 4..index * 4 + 4].copy_from_slice(&word.to_be_bytes());
        }
        output
    }

    fn transform(&mut self, block: [u8; 64]) {
        const ROUND: [u32; 64] = [
            0x428a_2f98,
            0x7137_4491,
            0xb5c0_fbcf,
            0xe9b5_dba5,
            0x3956_c25b,
            0x59f1_11f1,
            0x923f_82a4,
            0xab1c_5ed5,
            0xd807_aa98,
            0x1283_5b01,
            0x2431_85be,
            0x550c_7dc3,
            0x72be_5d74,
            0x80de_b1fe,
            0x9bdc_06a7,
            0xc19b_f174,
            0xe49b_69c1,
            0xefbe_4786,
            0x0fc1_9dc6,
            0x240c_a1cc,
            0x2de9_2c6f,
            0x4a74_84aa,
            0x5cb0_a9dc,
            0x76f9_88da,
            0x983e_5152,
            0xa831_c66d,
            0xb003_27c8,
            0xbf59_7fc7,
            0xc6e0_0bf3,
            0xd5a7_9147,
            0x06ca_6351,
            0x1429_2967,
            0x27b7_0a85,
            0x2e1b_2138,
            0x4d2c_6dfc,
            0x5338_0d13,
            0x650a_7354,
            0x766a_0abb,
            0x81c2_c92e,
            0x9272_2c85,
            0xa2bf_e8a1,
            0xa81a_664b,
            0xc24b_8b70,
            0xc76c_51a3,
            0xd192_e819,
            0xd699_0624,
            0xf40e_3585,
            0x106a_a070,
            0x19a4_c116,
            0x1e37_6c08,
            0x2748_774c,
            0x34b0_bcb5,
            0x391c_0cb3,
            0x4ed8_aa4a,
            0x5b9c_ca4f,
            0x682e_6ff3,
            0x748f_82ee,
            0x78a5_636f,
            0x84c8_7814,
            0x8cc7_0208,
            0x90be_fffa,
            0xa450_6ceb,
            0xbef9_a3f7,
            0xc671_78f2,
        ];
        let mut words = [0_u32; 64];
        for (index, bytes) in block.chunks_exact(4).take(16).enumerate() {
            words[index] = u32::from_be_bytes(bytes.try_into().expect("four-byte word"));
        }
        for index in 16..64 {
            let first = words[index - 15].rotate_right(7)
                ^ words[index - 15].rotate_right(18)
                ^ (words[index - 15] >> 3);
            let second = words[index - 2].rotate_right(17)
                ^ words[index - 2].rotate_right(19)
                ^ (words[index - 2] >> 10);
            words[index] = words[index - 16]
                .wrapping_add(first)
                .wrapping_add(words[index - 7])
                .wrapping_add(second);
        }
        let mut current = self.state;
        for index in 0..64 {
            let large1 = current[4].rotate_right(6)
                ^ current[4].rotate_right(11)
                ^ current[4].rotate_right(25);
            let choose = (current[4] & current[5]) ^ ((!current[4]) & current[6]);
            let first = current[7]
                .wrapping_add(large1)
                .wrapping_add(choose)
                .wrapping_add(ROUND[index])
                .wrapping_add(words[index]);
            let large0 = current[0].rotate_right(2)
                ^ current[0].rotate_right(13)
                ^ current[0].rotate_right(22);
            let majority =
                (current[0] & current[1]) ^ (current[0] & current[2]) ^ (current[1] & current[2]);
            let second = large0.wrapping_add(majority);
            current = [
                first.wrapping_add(second),
                current[0],
                current[1],
                current[2],
                first.wrapping_add(current[3]),
                current[4],
                current[5],
                current[6],
            ];
        }
        for (target, source) in self.state.iter_mut().zip(current) {
            *target = target.wrapping_add(source);
        }
    }
}

pub fn file_sha256(path: &Path) -> Result<String> {
    let mut reader = BufReader::with_capacity(1024 * 1024, File::open(path)?);
    let mut hasher = StreamingSha256::new();
    let mut buffer = [0_u8; 8192];
    loop {
        let amount = reader.read(&mut buffer)?;
        if amount == 0 {
            break;
        }
        hasher.update(&buffer[..amount]);
    }
    let digest = hasher.finish();
    let mut output = String::from("sha256:");
    for byte in digest {
        use std::fmt::Write as _;
        write!(output, "{byte:02x}").expect("write to String cannot fail");
    }
    Ok(output)
}
