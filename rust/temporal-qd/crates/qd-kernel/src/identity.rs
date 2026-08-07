//! Canonical identities used by the deterministic proposal kernel.
//!
//! This module owns only byte-level identities that are already part of the
//! Python proposal path.  It deliberately does not know how a grammar module
//! or a profile is constructed.

use temporal_qd_contract::{Value, canonical_sha256};
use thiserror::Error;

pub const PAIR_GENERATION_SCHEMA: &str = "temporal_qd_pair_generation_v2";
pub const BIDIRECTIONAL_GENOME_SCHEMA: &str = "temporal_bidirectional_genome_v1";
pub const PAIR_GENOME_SEMANTIC_SCHEMA: &str = "temporal_qd_pair_genome_semantics_v1";
pub const PAIR_IMMIGRANT_BUILDER_VERSION: &str = "temporal_qd_rich_immigrant_builder_v3";

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Side {
    Long,
    Short,
}

impl Side {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Long => "long",
            Self::Short => "short",
        }
    }
}

#[derive(Debug, Error, Eq, PartialEq)]
pub enum IdentityError {
    #[error("{name} must be a nonempty explicit identifier")]
    EmptyIdentifier { name: &'static str },
    #[error("{name} must be at most 240 Unicode code points")]
    IdentifierTooLong { name: &'static str },
}

/// The compact canonical JSON value domain needed by proposal identities.
/// The shared contract owns its serialization and SHA-256 digest semantics.
#[derive(Clone, Copy, Debug)]
pub(crate) enum CanonicalValue<'a> {
    String(&'a str),
    Boolean(bool),
    Unsigned(u64),
}

pub(crate) fn canonical_sha256_object(fields: &[(&str, CanonicalValue<'_>)]) -> String {
    canonical_sha256(&canonical_value_object(fields))
        .expect("proposal identity fields are in the shared canonical JSON domain")
}

fn canonical_value_object(fields: &[(&str, CanonicalValue<'_>)]) -> Value {
    let mut object = Value::Object(Default::default());
    let fields_out = object
        .as_object_mut()
        .expect("new JSON object must expose its object fields");
    for (key, value) in fields {
        let value = match value {
            CanonicalValue::String(value) => Value::String((*value).to_owned()),
            CanonicalValue::Boolean(value) => Value::Bool(*value),
            CanonicalValue::Unsigned(value) => Value::from(*value),
        };
        fields_out.insert((*key).to_owned(), value);
    }
    object
}

fn python_trimmed_identifier<'a>(
    value: &'a str,
    name: &'static str,
) -> Result<&'a str, IdentityError> {
    // Python `str.strip()` additionally considers U+001C..U+001F whitespace;
    // Unicode's White_Space property (used by Rust) otherwise covers the same
    // identifier-relevant code points.
    let token = value.trim_matches(|character: char| {
        character.is_whitespace() || matches!(character, '\u{001c}'..='\u{001f}')
    });
    if token.is_empty() {
        return Err(IdentityError::EmptyIdentifier { name });
    }
    if token.chars().count() > 240 {
        return Err(IdentityError::IdentifierTooLong { name });
    }
    Ok(token)
}

/// Mirrors `temporal_bidirectional_genome.proposal_side`.
pub fn proposal_side(proposal_seed: impl ToString) -> Result<Side, IdentityError> {
    let rendered = proposal_seed.to_string();
    let seed = python_trimmed_identifier(&rendered, "proposal seed")?;
    let hash = canonical_sha256_object(&[
        (
            "schemaVersion",
            CanonicalValue::String(BIDIRECTIONAL_GENOME_SCHEMA),
        ),
        ("proposalSeed", CanonicalValue::String(seed)),
    ]);
    let nibble = hash
        .as_bytes()
        .last()
        .expect("prefixed SHA-256 must have a final hexadecimal character");
    Ok(if hex_value(*nibble) % 2 == 0 {
        Side::Long
    } else {
        Side::Short
    })
}

/// Stable proposal identity for one journal ordinal.
pub fn proposal_seed(config_sha256: &str, proposal_ordinal: u64) -> String {
    canonical_sha256_object(&[
        (
            "schemaVersion",
            CanonicalValue::String(PAIR_GENERATION_SCHEMA),
        ),
        ("configSha256", CanonicalValue::String(config_sha256)),
        (
            "proposalOrdinal",
            CanonicalValue::Unsigned(proposal_ordinal),
        ),
    ])
}

/// Independent per-side seed used by the rich immigrant builder.
pub fn immigrant_side_seed(proposal_seed: impl ToString, side: Side) -> String {
    let rendered = proposal_seed.to_string();
    canonical_sha256_object(&[
        (
            "schemaVersion",
            CanonicalValue::String(PAIR_IMMIGRANT_BUILDER_VERSION),
        ),
        ("proposalSeed", CanonicalValue::String(&rendered)),
        ("side", CanonicalValue::String(side.as_str())),
    ])
}

/// Seed for the next step in a multi-step structural mutation.
pub fn mutation_step_seed(
    proposal_seed: &str,
    mutation_step: u64,
    parent_pair_identity_sha256: &str,
) -> String {
    canonical_sha256_object(&[
        ("proposalSeed", CanonicalValue::String(proposal_seed)),
        ("mutationStep", CanonicalValue::Unsigned(mutation_step)),
        (
            "parentPairIdentitySha256",
            CanonicalValue::String(parent_pair_identity_sha256),
        ),
    ])
}

/// Candidate identity emitted by a structural mutation before profile
/// materialisation.  This is a candidate identity, not an executable semantic
/// identity.
pub fn mutation_candidate_id(
    proposal_seed: &str,
    parent_identity_sha256: &str,
    operation: &str,
) -> String {
    let hash = canonical_sha256_object(&[
        ("seed", CanonicalValue::String(proposal_seed)),
        ("parent", CanonicalValue::String(parent_identity_sha256)),
        ("operation", CanonicalValue::String(operation)),
    ]);
    format!("qd_pair_{}", &hash[7..35])
}

/// Candidate identity used while compiling the changed side of crossover.
pub fn crossover_side_candidate_id(proposal_seed: &str, side: Side) -> String {
    let hash = canonical_sha256_object(&[
        ("seed", CanonicalValue::String(proposal_seed)),
        ("side", CanonicalValue::String(side.as_str())),
    ]);
    format!("qd_pair_cross_{}", &hash[7..31])
}

/// Candidate identity for the compiled crossover pair.
pub fn crossover_pair_candidate_id(
    proposal_seed: &str,
    parent_identity_sha256: &str,
    mate_identity_sha256: &str,
) -> String {
    let hash = canonical_sha256_object(&[
        ("seed", CanonicalValue::String(proposal_seed)),
        ("parent", CanonicalValue::String(parent_identity_sha256)),
        ("mate", CanonicalValue::String(mate_identity_sha256)),
    ]);
    format!("qd_pair_cross_{}", &hash[7..31])
}

/// Identity of executable authored modules.  Deliberately excludes lineage and
/// materialisation candidate IDs exactly as the Python pair-generation path.
pub fn executable_pair_semantic_sha256(
    long_profile_sha256: &str,
    short_profile_sha256: &str,
) -> String {
    canonical_sha256_object(&[
        (
            "schemaVersion",
            CanonicalValue::String(PAIR_GENOME_SEMANTIC_SCHEMA),
        ),
        (
            "longProfileSha256",
            CanonicalValue::String(long_profile_sha256),
        ),
        (
            "shortProfileSha256",
            CanonicalValue::String(short_profile_sha256),
        ),
    ])
}

pub(crate) fn archive_parent_seed_material(
    generation_seed: &str,
    selection_ordinal: u64,
    label: &str,
) -> String {
    canonical_sha256_object(&[
        ("generationSeed", CanonicalValue::String(generation_seed)),
        (
            "selectionOrdinal",
            CanonicalValue::Unsigned(selection_ordinal),
        ),
        ("label", CanonicalValue::String(label)),
    ])
}

fn hex_value(value: u8) -> u8 {
    match value {
        b'0'..=b'9' => value - b'0',
        b'a'..=b'f' => value - b'a' + 10,
        _ => unreachable!("shared SHA-256 function must return lowercase hexadecimal"),
    }
}
