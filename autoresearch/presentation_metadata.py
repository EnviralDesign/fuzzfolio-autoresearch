from __future__ import annotations

import hashlib
import json
import re
import string
import unicodedata
from datetime import datetime
from pathlib import Path
from typing import Any

from .provider import ChatMessage

PRESENTATION_METADATA_VERSION = 1
PRESENTATION_GENERATION_VERSION = 2
DISPLAY_NAME_MAX_CHARS = 40
DISPLAY_NAME_MAX_WORDS = 6
TAGLINE_MAX_CHARS = 60
SHORT_DESCRIPTION_MAX_CHARS = 100
LONG_DESCRIPTION_MIN_CHARS = 110
LONG_DESCRIPTION_MAX_CHARS = 180

_GENERIC_DESCRIPTION_TOKENS = {
    "portable scoring profile scaffolded from live indicator templates.",
    "portable scoring profile scaffolded from live indicator templates",
}
_BANNED_OPERATIONAL_PATTERNS = (
    re.compile(r"\bcand\d+\b", re.IGNORECASE),
    re.compile(r"\bscaffold(?:ed|ing)?\b", re.IGNORECASE),
    re.compile(r"\bseed(?:ed|ing)?\b", re.IGNORECASE),
    re.compile(r"\bv\d+\b", re.IGNORECASE),
)
_VOLATILE_PROFILE_FIELDS = {"name", "description", "executionConfig", "notificationThreshold", "isActive"}


def _normalize_whitespace(text: Any) -> str:
    return " ".join(str(text or "").strip().split())


def _ascii_safe(text: Any) -> str:
    normalized = unicodedata.normalize("NFKD", str(text or ""))
    return normalized.encode("ascii", "ignore").decode("ascii")


def _clean_copy(text: Any) -> str:
    return _normalize_whitespace(_ascii_safe(text))


def _normalize_text_key(text: Any) -> str:
    return _normalize_whitespace(_ascii_safe(text)).lower()


def _normalize_display_name(text: Any) -> str:
    cleaned = _clean_copy(text)
    if cleaned and any(char.isalpha() for char in cleaned) and cleaned == cleaned.upper():
        cleaned = string.capwords(cleaned.lower())
    return cleaned


def _contains_banned_operational_text(text: str) -> bool:
    normalized = _clean_copy(text)
    if not normalized:
        return False
    return any(pattern.search(normalized) for pattern in _BANNED_OPERATIONAL_PATTERNS)


def _sentence_count(text: str) -> int:
    parts = [part for part in re.split(r"[.!?]+", text) if part.strip()]
    return len(parts)


def _word_count(text: str) -> int:
    return len([token for token in text.split() if token.strip()])


def _extract_profile(payload: dict[str, Any] | None) -> dict[str, Any] | None:
    if not isinstance(payload, dict):
        return None
    profile = payload.get("profile")
    if isinstance(profile, dict):
        return profile
    profile_document = payload.get("profile_document")
    if isinstance(profile_document, dict) and isinstance(profile_document.get("profile"), dict):
        return profile_document.get("profile")
    return None


def _canonical_package_inputs(
    package_inputs: dict[str, Any], *, lookback_months: int
) -> dict[str, Any]:
    instruments = [
        str(item).strip().upper()
        for item in list(package_inputs.get("instruments") or [])
        if str(item).strip()
    ]
    return {
        "timeframe": str(package_inputs.get("timeframe") or "").strip().upper() or None,
        "instruments": instruments,
        "lookback_months": int(lookback_months),
    }


def build_package_token(package_inputs: dict[str, Any], *, lookback_months: int) -> str:
    canonical = _canonical_package_inputs(package_inputs, lookback_months=lookback_months)
    instruments = canonical["instruments"]
    if not instruments:
        instruments_part = "no-instruments"
    elif len(instruments) == 1:
        instruments_part = instruments[0].lower()
    else:
        instruments_part = f"{len(instruments)}-assets"
    timeframe = str(canonical.get("timeframe") or "mixed").lower()
    hash_suffix = hashlib.sha1(
        json.dumps(canonical, ensure_ascii=True, sort_keys=True).encode("utf-8")
    ).hexdigest()[:8]
    token = f"{timeframe}-{int(lookback_months)}mo-{instruments_part}-{hash_suffix}"
    return re.sub(r"[^a-z0-9-]+", "-", token).strip("-") or "package"


def presentation_metadata_path(
    run_dir: Path,
    attempt_id: str,
    *,
    package_inputs: dict[str, Any],
    lookback_months: int,
) -> Path:
    safe_attempt_id = re.sub(r"[^A-Za-z0-9._-]+", "-", str(attempt_id or "").strip()).strip("-")
    if not safe_attempt_id:
        safe_attempt_id = "attempt"
    return (
        run_dir
        / "presentation-metadata"
        / f"{safe_attempt_id}--{build_package_token(package_inputs, lookback_months=lookback_months)}.json"
    )


def load_profile_document(bundle_dir: Path) -> tuple[Path, dict[str, Any] | None]:
    profile_document_path = bundle_dir / "profile-document.json"
    if not profile_document_path.exists():
        return profile_document_path, None
    try:
        payload = json.loads(profile_document_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return profile_document_path, None
    return profile_document_path, payload if isinstance(payload, dict) else None


def _stable_profile_document_payload(
    profile_document_payload: dict[str, Any] | None,
) -> dict[str, Any] | None:
    if not isinstance(profile_document_payload, dict):
        return None

    def prune(value: Any, path: tuple[str, ...] = ()) -> Any:
        if isinstance(value, dict):
            result: dict[str, Any] = {}
            for key, child in value.items():
                if path == ("profile",) and key in _VOLATILE_PROFILE_FIELDS:
                    continue
                if key == "instanceId":
                    continue
                result[str(key)] = prune(child, path + (str(key),))
            return result
        if isinstance(value, list):
            return [prune(item, path + ("[]",)) for item in value]
        return value

    return prune(profile_document_payload)


def compute_legacy_presentation_signature(
    profile_document_payload: dict[str, Any] | None,
    *,
    package_inputs: dict[str, Any],
    lookback_months: int,
    writer_profile: str | None,
) -> str:
    canonical = {
        "presentation_generation_version": PRESENTATION_GENERATION_VERSION,
        "writer_profile": str(writer_profile or "").strip() or None,
        "profile_document": profile_document_payload if isinstance(profile_document_payload, dict) else None,
        "package": _canonical_package_inputs(package_inputs, lookback_months=lookback_months),
    }
    serialized = json.dumps(canonical, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def compute_presentation_signature(
    profile_document_payload: dict[str, Any] | None,
    *,
    package_inputs: dict[str, Any],
    lookback_months: int,
    writer_profile: str | None,
) -> str:
    canonical = {
        "presentation_generation_version": PRESENTATION_GENERATION_VERSION,
        "writer_profile": str(writer_profile or "").strip() or None,
        "profile_document": _stable_profile_document_payload(profile_document_payload),
        "package": _canonical_package_inputs(package_inputs, lookback_months=lookback_months),
    }
    serialized = json.dumps(canonical, ensure_ascii=True, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def build_writer_messages(
    *,
    profile_document_payload: dict[str, Any] | None,
    package_inputs: dict[str, Any],
    lookback_months: int,
    row: dict[str, Any],
    attempt: dict[str, Any],
) -> list[ChatMessage]:
    profile = _extract_profile(profile_document_payload) or {}
    context_payload = {
        "package": _canonical_package_inputs(package_inputs, lookback_months=lookback_months),
        "attempt_summary": {
            "candidate_name": str(row.get("candidate_name") or attempt.get("candidate_name") or "").strip() or None,
            "profile_ref": str(row.get("profile_ref") or attempt.get("profile_ref") or "").strip() or None,
            "composite_score": row.get("composite_score") or attempt.get("composite_score"),
            "effective_window_months": row.get("effective_window_months_36m")
            or row.get("effective_window_months")
            or attempt.get("effective_window_months"),
            "requested_horizon_months": row.get("requested_horizon_months")
            or attempt.get("requested_horizon_months"),
            "score_12m": row.get("score_12m"),
            "score_36m": row.get("score_36m"),
            "score_retention_ratio_36m_vs_12m": row.get("score_retention_ratio_36m_vs_12m"),
            "trades_per_month": row.get("trades_per_month_36m")
            or row.get("trades_per_month")
            or attempt.get("trades_per_month"),
            "resolved_trades": row.get("trade_count_36m")
            or row.get("resolved_trades")
            or attempt.get("resolved_trades"),
            "direction_mode": profile.get("directionMode") or row.get("direction_mode") or attempt.get("direction_mode"),
        },
        "profile": profile,
    }
    system_prompt = (
        "You write public-facing naming and copy for a fixed scoring-profile drop card. "
        "Return one JSON object only with keys display_name, tagline, short_description, long_description. "
        "Return display_name in normal title case; the renderer uppercases it. "
        "The display_name is shown as an uppercase two-line title card. "
        "The long_description is shown in a fixed logic-profile body slot. "
        "Be specific, plain-language, and strategy-relevant. Explain how the strategy works. "
        "Do not mention optimization, robustness, advanced tooling, templates, seeds, scaffolds, candidate handles, or version suffixes."
    )
    user_prompt = (
        "Write intentional presentation metadata for this packaged profile.\n\n"
        "Hard constraints:\n"
        f"- display_name: <= {DISPLAY_NAME_MAX_CHARS} chars, <= {DISPLAY_NAME_MAX_WORDS} words\n"
        f"- tagline: <= {TAGLINE_MAX_CHARS} chars\n"
        f"- short_description: <= {SHORT_DESCRIPTION_MAX_CHARS} chars\n"
        f"- long_description: {LONG_DESCRIPTION_MIN_CHARS}-{LONG_DESCRIPTION_MAX_CHARS} chars, <= 2 sentences\n"
        "- No cand/scaffold/seed/v2 or similar operational wording.\n"
        "- No raw metrics in the title or long_description.\n"
        "- Prefer concrete archetypes and plain wording over hype.\n\n"
        "Context JSON:\n"
        f"{json.dumps(context_payload, ensure_ascii=True, indent=2)}"
    )
    return [
        ChatMessage(role="system", content=system_prompt),
        ChatMessage(role="user", content=user_prompt),
    ]


def validate_generated_metadata(payload: dict[str, Any] | None) -> dict[str, str] | None:
    if not isinstance(payload, dict):
        return None
    display_name = _normalize_display_name(payload.get("display_name"))
    tagline = _clean_copy(payload.get("tagline"))
    short_description = _clean_copy(payload.get("short_description"))
    long_description = _clean_copy(payload.get("long_description"))
    if not all((display_name, tagline, short_description, long_description)):
        return None
    if len(display_name) > DISPLAY_NAME_MAX_CHARS or _word_count(display_name) > DISPLAY_NAME_MAX_WORDS:
        return None
    if len(tagline) > TAGLINE_MAX_CHARS:
        return None
    if len(short_description) > SHORT_DESCRIPTION_MAX_CHARS:
        return None
    if not (LONG_DESCRIPTION_MIN_CHARS <= len(long_description) <= LONG_DESCRIPTION_MAX_CHARS):
        return None
    if _sentence_count(long_description) > 2:
        return None
    normalized_long = _normalize_text_key(long_description)
    if normalized_long in _GENERIC_DESCRIPTION_TOKENS:
        return None
    if _contains_banned_operational_text(display_name):
        return None
    if _contains_banned_operational_text(tagline):
        return None
    if _contains_banned_operational_text(short_description):
        return None
    if _contains_banned_operational_text(long_description):
        return None
    return {
        "display_name": display_name,
        "tagline": tagline,
        "short_description": short_description,
        "long_description": long_description,
    }


def load_cached_metadata(
    path: Path,
    *,
    expected_signature: str,
    accepted_signatures: set[str] | None = None,
    fallback_writer_profile: str | None = None,
    fallback_profile_ref: str | None = None,
) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return None
    if not isinstance(payload, dict):
        return None
    accepted = {str(expected_signature)}
    if accepted_signatures:
        accepted.update(str(signature) for signature in accepted_signatures if str(signature).strip())
    payload_signature = str(payload.get("presentation_signature") or "")
    fallback_allowed = (
        str(fallback_writer_profile or "").strip()
        and str(fallback_profile_ref or "").strip()
        and str(payload.get("writer_profile") or "").strip()
        == str(fallback_writer_profile or "").strip()
        and str(payload.get("profile_ref") or "").strip()
        == str(fallback_profile_ref or "").strip()
    )
    if payload_signature not in accepted and not fallback_allowed:
        return None
    normalized = validate_generated_metadata(payload)
    if normalized is None:
        return None
    merged = dict(payload)
    merged.update(normalized)
    return merged


def build_metadata_artifact(
    *,
    run_id: str,
    attempt_id: str,
    candidate_name: str,
    profile_ref: str,
    writer_profile: str,
    presentation_signature: str,
    metadata: dict[str, str],
) -> dict[str, Any]:
    return {
        "version": PRESENTATION_METADATA_VERSION,
        "generated_at": datetime.now().astimezone().isoformat(),
        "run_id": str(run_id),
        "attempt_id": str(attempt_id),
        "candidate_name": str(candidate_name),
        "profile_ref": str(profile_ref),
        "presentation_signature": str(presentation_signature),
        "writer_profile": str(writer_profile),
        **metadata,
    }


def apply_metadata_to_profile_document(
    profile_document_payload: dict[str, Any] | None,
    metadata: dict[str, Any],
) -> dict[str, Any] | None:
    if not isinstance(profile_document_payload, dict):
        return None
    profile = _extract_profile(profile_document_payload)
    if not isinstance(profile, dict):
        return None
    profile["name"] = str(metadata.get("display_name") or profile.get("name") or "")
    profile["description"] = str(
        metadata.get("long_description") or profile.get("description") or ""
    )
    return profile_document_payload
