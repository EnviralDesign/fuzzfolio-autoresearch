"""Stable content fingerprinting for profile JSON (dedupe validate/register)."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any


def _sort_keys(obj: Any) -> Any:
    if isinstance(obj, dict):
        return {k: _sort_keys(obj[k]) for k in sorted(obj.keys())}
    if isinstance(obj, list):
        return [_sort_keys(item) for item in obj]
    return obj


def compute_profile_fingerprint(path: Path) -> tuple[str | None, str | None]:
    """Return (sha256_hex, error_message). Canonical JSON with sorted keys."""
    try:
        raw = path.read_text(encoding="utf-8")
    except OSError as exc:
        return None, str(exc)
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        return None, f"invalid profile JSON: {exc}"
    canonical = json.dumps(
        _sort_keys(data), separators=(",", ":"), ensure_ascii=True
    )
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
    return digest, None


def fingerprint_for_json_object(data: Any) -> str:
    """Deterministic fingerprint for an already-parsed document (tests)."""
    canonical = json.dumps(
        _sort_keys(data), separators=(",", ":"), ensure_ascii=True
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()
