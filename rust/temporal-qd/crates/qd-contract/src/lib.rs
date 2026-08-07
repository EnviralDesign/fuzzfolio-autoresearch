//! Strict, versioned contracts shared by the Temporal QD native foundation.
//!
//! This crate intentionally contains no proposal or G0 semantics.  It only
//! supplies the byte-level primitives and compact foundation schemas required
//! to establish a safe native boundary.

pub const CONTRACT_VERSION: &str = "temporal_qd_native_foundation_v1";
pub const VERSION_SCHEMA: &str = "temporal_qd_native_version_v1";
pub const MANIFEST_SCHEMA: &str = "temporal_qd_native_manifest_v1";
pub const RESULT_SCHEMA: &str = "temporal_qd_native_result_v1";
pub const FOUNDATION_OPERATION: &str = "foundation_probe";
pub const FOUNDATION_RESULT_PATH: &str = "result.json";

pub mod foundation;

pub use foundation::{
    CanonicalSha256Writer, FoundationManifest, FoundationResult, JsonNewline, NativeContractError,
    NativeVersion, canonical_json, canonical_json_bytes, canonical_json_line, canonical_sha256,
    canonical_sha256_streaming, canonical_sha256_without_object_field, parse_foundation_manifest,
    parse_foundation_result, python_pretty_json_line, sha256_prefixed, write_canonical_json,
    write_python_pretty_json,
};
pub use serde_json::{Map, Value};
pub type ContractError = NativeContractError;
