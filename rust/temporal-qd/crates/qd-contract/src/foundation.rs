//! Canonical JSON and foundation manifest contracts.
//!
//! Foundation schemas use Python-compatible canonical JSON primitives so later
//! compact typed contracts can safely include float-valued policy grids.

use std::collections::BTreeMap;
use std::io::Write;

use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use thiserror::Error;

use crate::{
    CONTRACT_VERSION, FOUNDATION_OPERATION, FOUNDATION_RESULT_PATH, MANIFEST_SCHEMA, RESULT_SCHEMA,
    VERSION_SCHEMA,
};

/// Line ending used by Python-compatible JSON documents written through a
/// text-mode file on the target platform.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum JsonNewline {
    Lf,
    Crlf,
}

impl JsonNewline {
    pub const fn bytes(self) -> &'static [u8] {
        match self {
            Self::Lf => b"\n",
            Self::Crlf => b"\r\n",
        }
    }
}

#[derive(Debug, Error)]
pub enum NativeContractError {
    #[error("JSON is invalid: {0}")]
    Json(#[from] serde_json::Error),
    #[error("native contract must be UTF-8 JSON")]
    Utf8(#[from] std::str::Utf8Error),
    #[error("could not write canonical native contract JSON: {0}")]
    Io(#[from] std::io::Error),
    #[error("native contract must be one canonical JSON document followed by exactly one LF")]
    NonCanonicalDocument,
    #[error("native contract number is not finite or cannot be represented as a finite f64")]
    NonFiniteNumber,
    #[error("native contract schema violation: {0}")]
    Schema(String),
}

/// A small, explicit version handshake.  Extra or missing fields are denied
/// when it is decoded, so callers cannot accidentally accept a different
/// executable protocol.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NativeVersion {
    pub schema_version: String,
    pub contract_version: String,
    pub crate_version: String,
    pub binary_name: String,
}

impl NativeVersion {
    pub fn current() -> Self {
        Self {
            schema_version: VERSION_SCHEMA.to_owned(),
            contract_version: CONTRACT_VERSION.to_owned(),
            crate_version: env!("CARGO_PKG_VERSION").to_owned(),
            binary_name: "temporal-qd-batch".to_owned(),
        }
    }

    pub fn validate(&self) -> Result<(), NativeContractError> {
        if self.schema_version != VERSION_SCHEMA
            || self.contract_version != CONTRACT_VERSION
            || self.crate_version.trim().is_empty()
            || self.binary_name != "temporal-qd-batch"
        {
            return Err(NativeContractError::Schema(
                "version handshake is not compatible with this native foundation".to_owned(),
            ));
        }
        Ok(())
    }
}

/// Immutable input for the one-shot foundation probe.  Future proposal/G0
/// schemas will be new operations and contract versions; they must not be
/// smuggled through this intentionally small manifest.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FoundationManifest {
    pub schema_version: String,
    pub contract_version: String,
    pub operation: String,
    pub authority_sha256: String,
    pub result_path: String,
    pub manifest_sha256: String,
}

impl FoundationManifest {
    pub fn validate(&self) -> Result<(), NativeContractError> {
        if self.schema_version != MANIFEST_SCHEMA {
            return Err(NativeContractError::Schema(
                "manifest schemaVersion is incompatible".to_owned(),
            ));
        }
        if self.contract_version != CONTRACT_VERSION {
            return Err(NativeContractError::Schema(
                "manifest contractVersion is incompatible".to_owned(),
            ));
        }
        if self.operation != FOUNDATION_OPERATION {
            return Err(NativeContractError::Schema(
                "manifest operation is not supported by the foundation binary".to_owned(),
            ));
        }
        require_sha256(&self.authority_sha256, "manifest authoritySha256")?;
        require_sha256(&self.manifest_sha256, "manifest manifestSha256")?;
        if self.result_path != FOUNDATION_RESULT_PATH {
            return Err(NativeContractError::Schema(
                "manifest resultPath is not the fixed foundation result filename".to_owned(),
            ));
        }
        Ok(())
    }
}

/// The one-shot response emitted to stdout and installed as the immutable
/// result document beside its manifest.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct FoundationResult {
    pub schema_version: String,
    pub contract_version: String,
    pub operation: String,
    pub authority_sha256: String,
    pub manifest_sha256: String,
    pub status: String,
}

impl FoundationResult {
    pub fn from_manifest(manifest: &FoundationManifest) -> Self {
        Self {
            schema_version: RESULT_SCHEMA.to_owned(),
            contract_version: CONTRACT_VERSION.to_owned(),
            operation: FOUNDATION_OPERATION.to_owned(),
            authority_sha256: manifest.authority_sha256.clone(),
            manifest_sha256: manifest.manifest_sha256.clone(),
            status: "completed".to_owned(),
        }
    }

    pub fn validate(&self) -> Result<(), NativeContractError> {
        if self.schema_version != RESULT_SCHEMA
            || self.contract_version != CONTRACT_VERSION
            || self.operation != FOUNDATION_OPERATION
            || self.status != "completed"
        {
            return Err(NativeContractError::Schema(
                "foundation result is incompatible".to_owned(),
            ));
        }
        require_sha256(&self.authority_sha256, "result authoritySha256")?;
        require_sha256(&self.manifest_sha256, "result manifestSha256")?;
        Ok(())
    }
}

/// Mirror Python's ``json.dumps(..., sort_keys=True, separators=(",", ":"),
/// ensure_ascii=True, allow_nan=False)`` contract: sorted keys, compact
/// separators, ASCII-only strings, and finite numbers.  The float formatter
/// applies Python's ``repr`` decimal/scientific cutovers and two-digit exponent
/// spelling to a shortest-round-trip Ryu representation. Rust strings cannot
/// represent lone UTF-16 surrogates, so serde_json rejects that out-of-domain
/// input rather than broadening the contract or silently repairing it.
pub fn canonical_json(value: &Value) -> Result<String, NativeContractError> {
    let output = canonical_json_bytes(value)?;
    Ok(String::from_utf8(output).expect("canonical JSON emits only valid UTF-8"))
}

pub fn canonical_json_bytes(value: &Value) -> Result<Vec<u8>, NativeContractError> {
    let mut output = Vec::new();
    write_canonical_json(value, &mut output)?;
    Ok(output)
}

pub fn canonical_json_line(value: &Value) -> Result<Vec<u8>, NativeContractError> {
    let mut output = canonical_json_bytes(value)?;
    output.push(b'\n');
    Ok(output)
}

/// Mirror Python's ``json.dumps(..., indent=2, sort_keys=True,
/// ensure_ascii=True, allow_nan=False)`` and append the selected platform
/// newline. Every structural newline uses the same spelling, matching the
/// translation performed when Python writes the rendered string in text mode.
pub fn python_pretty_json_line(
    value: &Value,
    newline: JsonNewline,
) -> Result<Vec<u8>, NativeContractError> {
    let mut output = Vec::new();
    write_python_pretty_json(value, &mut output, newline)?;
    output.extend_from_slice(newline.bytes());
    Ok(output)
}

/// Stream the Python-compatible two-space-indented representation without its
/// final newline. Callers writing a complete compatibility document should
/// normally use [`python_pretty_json_line`].
pub fn write_python_pretty_json<W: Write + ?Sized>(
    value: &Value,
    output: &mut W,
    newline: JsonNewline,
) -> Result<(), NativeContractError> {
    append_pretty_value(output, value, newline, 0)
}

pub fn sha256_prefixed(value: &[u8]) -> String {
    let digest = Sha256::digest(value);
    let mut output = String::from("sha256:");
    for byte in digest {
        use std::fmt::Write as _;
        write!(output, "{byte:02x}").expect("write to a String cannot fail");
    }
    output
}

pub fn canonical_sha256(value: &Value) -> Result<String, NativeContractError> {
    Ok(sha256_prefixed(&canonical_json_bytes(value)?))
}

/// Hash a canonical value without first allocating its complete encoded JSON.
///
/// This has exactly the same identity as [`canonical_sha256`].  It exists for
/// durable rich artifacts where the transient canonical byte buffer can be as
/// large as the artifact itself.
pub fn canonical_sha256_streaming(value: &Value) -> Result<String, NativeContractError> {
    let mut writer = CanonicalSha256Writer::default();
    write_canonical_json(value, &mut writer)?;
    Ok(writer.finish())
}

/// Hash an object exactly as canonical JSON after omitting one root field,
/// without cloning the rest of the object.  Self-hashed journals use this to
/// validate a rich entry read from disk without momentarily retaining a second
/// copy of that entry merely to remove its identity field.
pub fn canonical_sha256_without_object_field(
    value: &Value,
    omitted_field: &str,
) -> Result<String, NativeContractError> {
    let fields = value.as_object().ok_or_else(|| {
        NativeContractError::Schema("canonical omission requires an object".to_owned())
    })?;
    let ordered: BTreeMap<&str, &Value> = fields
        .iter()
        .filter(|(key, _)| key.as_str() != omitted_field)
        .map(|(key, value)| (key.as_str(), value))
        .collect();
    let mut writer = CanonicalSha256Writer::default();
    writer.write_all(b"{")?;
    let mut first = true;
    for (key, field_value) in ordered {
        if !first {
            writer.write_all(b",")?;
        }
        first = false;
        // Keys are small relative to the retained rich values.  Route their
        // escaping through the same canonical writer rather than duplicate
        // string semantics here.
        write_canonical_json(&Value::String(key.to_owned()), &mut writer)?;
        writer.write_all(b":")?;
        write_canonical_json(field_value, &mut writer)?;
    }
    writer.write_all(b"}")?;
    Ok(writer.finish())
}

pub struct CanonicalSha256Writer(Sha256);

impl CanonicalSha256Writer {
    pub fn finish(self) -> String {
        sha256_digest_prefixed(self.0.finalize())
    }
}

impl Default for CanonicalSha256Writer {
    fn default() -> Self {
        Self(Sha256::new())
    }
}

impl Write for CanonicalSha256Writer {
    fn write(&mut self, buffer: &[u8]) -> std::io::Result<usize> {
        self.0.update(buffer);
        Ok(buffer.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn sha256_digest_prefixed(digest: sha2::digest::Output<Sha256>) -> String {
    let mut output = String::from("sha256:");
    for byte in digest {
        use std::fmt::Write as _;
        write!(output, "{byte:02x}").expect("write to a String cannot fail");
    }
    output
}

/// Stream the exact canonical JSON representation to an arbitrary writer.
/// This is byte-identical to [`canonical_json_bytes`] but does not buffer the
/// complete document, allowing journals to emit large artifacts without
/// duplicating canonical string or number semantics.
pub fn write_canonical_json<W: Write + ?Sized>(
    value: &Value,
    output: &mut W,
) -> Result<(), NativeContractError> {
    append_value(output, value)
}

pub fn parse_foundation_manifest(raw: &[u8]) -> Result<FoundationManifest, NativeContractError> {
    let value = parse_canonical_line(raw)?;
    let manifest: FoundationManifest = serde_json::from_value(value.clone())?;
    manifest.validate()?;

    let mut body = value
        .as_object()
        .cloned()
        .ok_or_else(|| NativeContractError::Schema("manifest root must be an object".to_owned()))?;
    let supplied = body
        .remove("manifestSha256")
        .and_then(|value| value.as_str().map(str::to_owned))
        .ok_or_else(|| NativeContractError::Schema("manifestSha256 must be a string".to_owned()))?;
    let expected = sha256_prefixed(&canonical_json_bytes(&Value::Object(body))?);
    if supplied != expected {
        return Err(NativeContractError::Schema(
            "manifestSha256 does not commit to the exact manifest body".to_owned(),
        ));
    }
    Ok(manifest)
}

pub fn parse_foundation_result(raw: &[u8]) -> Result<FoundationResult, NativeContractError> {
    let value = parse_canonical_line(raw)?;
    let result: FoundationResult = serde_json::from_value(value)?;
    result.validate()?;
    Ok(result)
}

fn parse_canonical_line(raw: &[u8]) -> Result<Value, NativeContractError> {
    let semantic = raw
        .strip_suffix(b"\n")
        .filter(|value| !value.ends_with(b"\r"))
        .ok_or(NativeContractError::NonCanonicalDocument)?;
    if semantic.is_empty() {
        return Err(NativeContractError::NonCanonicalDocument);
    }
    let value: Value = serde_json::from_slice(semantic)?;
    if canonical_json_bytes(&value)? != semantic {
        return Err(NativeContractError::NonCanonicalDocument);
    }
    Ok(value)
}

fn require_sha256(value: &str, label: &str) -> Result<(), NativeContractError> {
    if value.len() != 71
        || !value.starts_with("sha256:")
        || !value[7..]
            .bytes()
            .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
    {
        return Err(NativeContractError::Schema(format!(
            "{label} must be a lowercase sha256 identity"
        )));
    }
    Ok(())
}

fn append_value<W: Write + ?Sized>(
    output: &mut W,
    value: &Value,
) -> Result<(), NativeContractError> {
    match value {
        Value::Null => output.write_all(b"null")?,
        Value::Bool(value) => output.write_all(if *value { b"true" } else { b"false" })?,
        Value::Number(value) => append_number(output, value)?,
        Value::String(value) => append_string(output, value)?,
        Value::Array(values) => {
            output.write_all(b"[")?;
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.write_all(b",")?;
                }
                append_value(output, value)?;
            }
            output.write_all(b"]")?;
        }
        Value::Object(values) => {
            let ordered: BTreeMap<&str, &Value> = values
                .iter()
                .map(|(key, value)| (key.as_str(), value))
                .collect();
            output.write_all(b"{")?;
            for (index, (key, value)) in ordered.into_iter().enumerate() {
                if index > 0 {
                    output.write_all(b",")?;
                }
                append_string(output, key)?;
                output.write_all(b":")?;
                append_value(output, value)?;
            }
            output.write_all(b"}")?;
        }
    }
    Ok(())
}

fn append_pretty_value<W: Write + ?Sized>(
    output: &mut W,
    value: &Value,
    newline: JsonNewline,
    depth: usize,
) -> Result<(), NativeContractError> {
    match value {
        Value::Null => output.write_all(b"null")?,
        Value::Bool(value) => output.write_all(if *value { b"true" } else { b"false" })?,
        Value::Number(value) => append_number(output, value)?,
        Value::String(value) => append_string(output, value)?,
        Value::Array(values) if values.is_empty() => output.write_all(b"[]")?,
        Value::Array(values) => {
            output.write_all(b"[")?;
            output.write_all(newline.bytes())?;
            for (index, value) in values.iter().enumerate() {
                append_indent(output, depth + 1)?;
                append_pretty_value(output, value, newline, depth + 1)?;
                if index + 1 < values.len() {
                    output.write_all(b",")?;
                }
                output.write_all(newline.bytes())?;
            }
            append_indent(output, depth)?;
            output.write_all(b"]")?;
        }
        Value::Object(values) if values.is_empty() => output.write_all(b"{}")?,
        Value::Object(values) => {
            let ordered: BTreeMap<&str, &Value> = values
                .iter()
                .map(|(key, value)| (key.as_str(), value))
                .collect();
            let length = ordered.len();
            output.write_all(b"{")?;
            output.write_all(newline.bytes())?;
            for (index, (key, value)) in ordered.into_iter().enumerate() {
                append_indent(output, depth + 1)?;
                append_string(output, key)?;
                output.write_all(b": ")?;
                append_pretty_value(output, value, newline, depth + 1)?;
                if index + 1 < length {
                    output.write_all(b",")?;
                }
                output.write_all(newline.bytes())?;
            }
            append_indent(output, depth)?;
            output.write_all(b"}")?;
        }
    }
    Ok(())
}

fn append_indent<W: Write + ?Sized>(
    output: &mut W,
    depth: usize,
) -> Result<(), NativeContractError> {
    for _ in 0..depth {
        output.write_all(b"  ")?;
    }
    Ok(())
}

fn append_number<W: Write + ?Sized>(
    output: &mut W,
    value: &serde_json::Number,
) -> Result<(), NativeContractError> {
    // With `arbitrary_precision`, serde_json preserves Python-compatible
    // arbitrary integer JSON lexemes instead of coercing them through f64.
    let lexical = value.to_string();
    if !lexical.contains(['.', 'e', 'E']) {
        // Python decodes integer `-0` as the integer zero and therefore
        // re-encodes it as `0`; preserve arbitrary-sized integer lexemes
        // otherwise.
        output.write_all(if lexical == "-0" {
            b"0"
        } else {
            lexical.as_bytes()
        })?;
        return Ok(());
    }
    let value = value.as_f64().ok_or(NativeContractError::NonFiniteNumber)?;
    if !value.is_finite() {
        return Err(NativeContractError::NonFiniteNumber);
    }
    output.write_all(python_float_repr(value).as_bytes())?;
    Ok(())
}

fn python_float_repr(value: f64) -> String {
    if value == 0.0 {
        return if value.is_sign_negative() {
            "-0.0".to_owned()
        } else {
            "0.0".to_owned()
        };
    }

    let mut buffer = ryu::Buffer::new();
    let rendered = buffer.format_finite(value);
    let (negative, rendered) = rendered
        .strip_prefix('-')
        .map_or((false, rendered), |rest| (true, rest));
    let (coefficient, decimal_exponent) = match rendered.split_once(['e', 'E']) {
        Some((coefficient, exponent)) => (
            coefficient,
            exponent
                .parse::<i32>()
                .expect("Ryu emitted an invalid decimal exponent"),
        ),
        None => (rendered, 0),
    };
    format_python_float_from_ryu(negative, coefficient, decimal_exponent)
}

fn format_python_float_from_ryu(
    negative: bool,
    coefficient: &str,
    decimal_exponent: i32,
) -> String {
    let mut decimal_position = coefficient
        .find('.')
        .map_or(coefficient.len(), |index| index) as i32
        + decimal_exponent;
    let mut digits = coefficient.replace('.', "");
    while digits.starts_with('0') {
        digits.remove(0);
        decimal_position -= 1;
    }
    while digits.ends_with('0') && digits.len() > 1 {
        digits.pop();
    }
    let scientific_exponent = decimal_position - 1;
    let mut output = String::new();
    if negative {
        output.push('-');
    }
    if !(-4..16).contains(&scientific_exponent) {
        output.push(digits.as_bytes()[0] as char);
        if digits.len() > 1 {
            output.push('.');
            output.push_str(&digits[1..]);
        }
        use std::fmt::Write as _;
        if (-9..=9).contains(&scientific_exponent) {
            write!(output, "e{scientific_exponent:+03}").expect("write to a String cannot fail");
        } else {
            write!(output, "e{scientific_exponent:+}").expect("write to a String cannot fail");
        }
    } else if decimal_position <= 0 {
        output.push_str("0.");
        output.push_str(&"0".repeat((-decimal_position) as usize));
        output.push_str(&digits);
    } else if decimal_position as usize >= digits.len() {
        output.push_str(&digits);
        output.push_str(&"0".repeat(decimal_position as usize - digits.len()));
        output.push_str(".0");
    } else {
        output.push_str(&digits[..decimal_position as usize]);
        output.push('.');
        output.push_str(&digits[decimal_position as usize..]);
    }
    output
}

fn append_string<W: Write + ?Sized>(
    output: &mut W,
    value: &str,
) -> Result<(), NativeContractError> {
    output.write_all(b"\"")?;
    for character in value.chars() {
        match character {
            '"' => output.write_all(b"\\\"")?,
            '\\' => output.write_all(b"\\\\")?,
            '\u{08}' => output.write_all(b"\\b")?,
            '\u{0c}' => output.write_all(b"\\f")?,
            '\n' => output.write_all(b"\\n")?,
            '\r' => output.write_all(b"\\r")?,
            '\t' => output.write_all(b"\\t")?,
            value if (value as u32) < 0x20 || value == '\u{007f}' => {
                write!(output, "\\u{:04x}", value as u32)?;
            }
            value if !value.is_ascii() => {
                let mut units = [0_u16; 2];
                for unit in value.encode_utf16(&mut units).iter() {
                    write!(output, "\\u{unit:04x}")?;
                }
            }
            value => output.write_all(&[value as u8])?,
        }
    }
    output.write_all(b"\"")?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use super::*;

    #[test]
    fn canonical_json_matches_python_ascii_sorted_golden() {
        let value = json!({
            "z": "emoji 😀 and slash /",
            "a": [true, "\u{0001}", {"b": 2, "a": -3}],
        });
        assert_eq!(
            canonical_json_bytes(&value).unwrap(),
            br#"{"a":[true,"\u0001",{"a":-3,"b":2}],"z":"emoji \ud83d\ude00 and slash /"}"#,
        );
    }

    #[test]
    fn canonical_json_matches_python_float_golden_corpus() {
        let values = json!([
            0.0,
            -0.0,
            1.0,
            -1.0,
            1e-7,
            1e-6,
            1e-5,
            1e-4,
            1e15,
            1e16,
            1.2e20,
            1e21,
            1.234e-10,
            1.0000000000000002,
            5e-324,
            1.7976931348623157e308,
        ]);
        assert_eq!(
            canonical_json(&values).unwrap(),
            "[0.0,-0.0,1.0,-1.0,1e-07,1e-06,1e-05,0.0001,1000000000000000.0,1e+16,1.2e+20,1e+21,1.234e-10,1.0000000000000002,5e-324,1.7976931348623157e+308]"
        );
    }

    #[test]
    fn canonical_json_normalizes_negative_zero_integer_like_python() {
        let value: Value = serde_json::from_str("-0").unwrap();
        assert_eq!(canonical_json(&value).unwrap(), "0");
    }

    #[test]
    fn canonical_json_escapes_del_with_exact_python_bytes_and_hash() {
        let value = Value::String("\u{007f}".to_owned());
        assert_eq!(canonical_json_bytes(&value).unwrap(), br#""\u007f""#);
        assert_eq!(
            canonical_sha256(&value).unwrap(),
            "sha256:226453a6508483054f1ddc449c26b45077f5a45d993034f0f5f04f3f9801c32c",
        );
    }

    #[test]
    fn canonical_json_del_identity_is_stable_in_nested_keys_and_strings() {
        let value = json!({
            "del": "\u{007f}",
            "nested": ["\u{007f}", {"key\u{007f}": "value\u{007f}"}],
        });
        assert_eq!(
            canonical_json_bytes(&value).unwrap(),
            br#"{"del":"\u007f","nested":["\u007f",{"key\u007f":"value\u007f"}]}"#,
        );
        assert_eq!(
            canonical_sha256(&value).unwrap(),
            "sha256:6f971924b25d06039c447b5f235c8a63f7dc49b16a4eaa294e02209747657da7",
        );
    }

    #[test]
    fn streaming_writer_is_byte_identical_for_all_canonical_primitives() {
        let value: Value = serde_json::from_str(
            r#"{"del":"\u007f","floats":[-0,-0.0,1e-7,1e-6,1e15,1e16],"nested":{"emoji":"😀","controls":"\u0001\t"}}"#,
        )
        .unwrap();
        let expected = br#"{"del":"\u007f","floats":[0,-0.0,1e-07,1e-06,1000000000000000.0,1e+16],"nested":{"controls":"\u0001\t","emoji":"\ud83d\ude00"}}"#;
        let mut streamed = Vec::new();

        write_canonical_json(&value, &mut streamed).unwrap();

        assert_eq!(streamed, expected);
        assert_eq!(streamed, canonical_json_bytes(&value).unwrap());
        assert_eq!(
            String::from_utf8(streamed).unwrap(),
            canonical_json(&value).unwrap()
        );
    }

    #[test]
    fn streaming_hashes_match_buffered_hashes_without_cloning_rich_values() {
        let value = json!({
            "schemaVersion": "rich_fixture_v1",
            "candidate": {
                "payload": ["emoji 😀", "\u{007f}", -0.0, 1e-7],
                "nested": {"z": true, "a": [1, 2, 3]},
            },
            "entrySha256": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
        });
        let mut without = value.clone();
        without.as_object_mut().unwrap().remove("entrySha256");

        assert_eq!(
            canonical_sha256_streaming(&value).unwrap(),
            canonical_sha256(&value).unwrap()
        );
        assert_eq!(
            canonical_sha256_without_object_field(&value, "entrySha256").unwrap(),
            canonical_sha256(&without).unwrap(),
        );
    }

    #[test]
    fn python_pretty_json_matches_python_lf_golden() {
        let value: Value = serde_json::from_str(
            r#"{"z":["\u007f","😀",{"tiny":1e-7,"zero":-0.0}],"a":[true,null,[]]}"#,
        )
        .unwrap();
        let expected = b"{\n  \"a\": [\n    true,\n    null,\n    []\n  ],\n  \"z\": [\n    \"\\u007f\",\n    \"\\ud83d\\ude00\",\n    {\n      \"tiny\": 1e-07,\n      \"zero\": -0.0\n    }\n  ]\n}\n";

        assert_eq!(
            python_pretty_json_line(&value, JsonNewline::Lf).unwrap(),
            expected
        );
    }

    #[test]
    fn python_pretty_json_matches_python_windows_crlf_golden() {
        let value: Value = serde_json::from_str(
            r#"{"z":["\u007f","😀",{"tiny":1e-7,"zero":-0.0}],"a":[true,null,[]]}"#,
        )
        .unwrap();
        let expected = b"{\r\n  \"a\": [\r\n    true,\r\n    null,\r\n    []\r\n  ],\r\n  \"z\": [\r\n    \"\\u007f\",\r\n    \"\\ud83d\\ude00\",\r\n    {\r\n      \"tiny\": 1e-07,\r\n      \"zero\": -0.0\r\n    }\r\n  ]\r\n}\r\n";

        assert_eq!(
            python_pretty_json_line(&value, JsonNewline::Crlf).unwrap(),
            expected
        );
        assert!(expected.ends_with(b"\r\n"));
        assert_eq!(
            expected.windows(2).filter(|pair| *pair == b"\r\n").count(),
            15
        );
    }

    #[test]
    fn manifest_requires_exact_canonical_self_hash() {
        let mut body = serde_json::Map::new();
        body.insert("schemaVersion".to_owned(), json!(MANIFEST_SCHEMA));
        body.insert("contractVersion".to_owned(), json!(CONTRACT_VERSION));
        body.insert("operation".to_owned(), json!(FOUNDATION_OPERATION));
        body.insert(
            "authoritySha256".to_owned(),
            json!("sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"),
        );
        body.insert("resultPath".to_owned(), json!(FOUNDATION_RESULT_PATH));
        let hash = sha256_prefixed(&canonical_json_bytes(&Value::Object(body.clone())).unwrap());
        body.insert("manifestSha256".to_owned(), json!(hash));
        let raw = canonical_json_line(&Value::Object(body)).unwrap();
        assert!(parse_foundation_manifest(&raw).is_ok());
    }
}
