from __future__ import annotations

import hashlib
import json
import os
import threading
import time
from pathlib import Path
from typing import Any

import httpx

from .lake_window import (
    SEMANTIC_DIGEST_CONTRACT_V2,
    LakeWindowBinding,
    LakeWindowRequest,
)

_CACHE: dict[str, LakeWindowBinding] = {}
_CACHE_LOCK = threading.Lock()


def _lake_credentials() -> tuple[str, str]:
    base_url = str(os.environ.get("REMOTE_MARKET_DATA_LAKE_BASE_URL") or "").strip()
    token = str(os.environ.get("REMOTE_MARKET_DATA_LAKE_API_TOKEN") or "").strip()
    if base_url and token:
        return base_url.rstrip("/"), token

    dashboard_root = Path(
        os.environ.get("FUZZFOLIO_TRADING_DASHBOARD_ROOT")
        or r"C:\repos\Trading-Dashboard"
    )
    env_path = Path(
        os.environ.get("FUZZFOLIO_MARKET_DATA_LAKE_ENV_FILE")
        or dashboard_root / "compute-service" / ".env"
    )
    if env_path.is_file():
        values: dict[str, str] = {}
        for raw_line in env_path.read_text(encoding="utf-8-sig").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            key = key.strip()
            if key not in {
                "REMOTE_MARKET_DATA_LAKE_BASE_URL",
                "REMOTE_MARKET_DATA_LAKE_API_TOKEN",
            }:
                continue
            values[key] = value.strip().strip('"').strip("'")
        base_url = base_url or values.get("REMOTE_MARKET_DATA_LAKE_BASE_URL", "")
        token = token or values.get("REMOTE_MARKET_DATA_LAKE_API_TOKEN", "")
    return base_url.rstrip("/"), token


def _canonical_sha256(payload: Any) -> str:
    encoded = json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def _verify_receipt(receipt: dict[str, Any], request: LakeWindowRequest) -> None:
    if receipt.get("request") != request.canonical_payload():
        raise RuntimeError("lake window attestation request mismatch")
    if receipt.get("semantic_contract_id") != SEMANTIC_DIGEST_CONTRACT_V2:
        raise RuntimeError("lake window attestation semantic contract mismatch")

    scopes = receipt.get("scopes")
    included = receipt.get("included_fields")
    excluded = receipt.get("excluded_fields")
    if not isinstance(scopes, list) or not isinstance(included, list) or not isinstance(excluded, list):
        raise RuntimeError("lake window attestation receipt is incomplete")
    normalized_scopes = sorted(
        [
            {
                "pair": item.get("pair"),
                "timeframe": item.get("timeframe"),
                "bar_count": item.get("bar_count"),
                "scope_semantic_sha256": item.get("scope_semantic_sha256"),
            }
            for item in scopes
            if isinstance(item, dict)
        ],
        key=lambda item: (str(item["pair"]), str(item["timeframe"])),
    )
    if len(normalized_scopes) != len(scopes):
        raise RuntimeError("lake window attestation scopes are malformed")
    expected_window = _canonical_sha256(
        {
            "request": request.canonical_payload(),
            "semantic_contract_id": SEMANTIC_DIGEST_CONTRACT_V2,
            "included_fields": included,
            "excluded_fields": excluded,
            "scopes": normalized_scopes,
        }
    )
    if receipt.get("window_semantic_sha256") != expected_window:
        raise RuntimeError("lake window attestation semantic SHA-256 is invalid")
    expected_attestation = _canonical_sha256(
        {
            key: value
            for key, value in receipt.items()
            if key not in {"attestation_sha256", "attested_at"}
        }
    )
    if receipt.get("attestation_sha256") != expected_attestation:
        raise RuntimeError("lake window attestation receipt SHA-256 is invalid")


def resolve_lake_window_binding(
    request: LakeWindowRequest,
    *,
    legacy_selection_manifest_sha256: str | None,
    timeout_seconds: float = 900.0,
) -> LakeWindowBinding:
    """Resolve, verify, and memoize an immutable lake window binding."""

    base_url, token = _lake_credentials()
    if not base_url:
        raise RuntimeError("REMOTE_MARKET_DATA_LAKE_BASE_URL is required for historical replay")
    if not token:
        raise RuntimeError("REMOTE_MARKET_DATA_LAKE_API_TOKEN is required for historical replay")
    request_payload = request.canonical_payload()
    cache_key = _canonical_sha256(
        {
            "base_url": base_url,
            "request": request_payload,
            "legacy_selection_manifest_sha256": legacy_selection_manifest_sha256,
        }
    )
    with _CACHE_LOCK:
        cached = _CACHE.get(cache_key)
    if cached is not None:
        return cached

    endpoint = f"{base_url}/api/lake/window-attestations/resolve"
    headers = {"Authorization": f"Bearer {token}"}
    response: httpx.Response | None = None
    for attempt in range(3):
        response = httpx.post(
            endpoint,
            headers=headers,
            json=request_payload,
            timeout=httpx.Timeout(timeout_seconds),
        )
        if response.status_code != 409:
            break
        if attempt < 2:
            time.sleep(1.0 + attempt)
    assert response is not None
    try:
        response.raise_for_status()
    except httpx.HTTPStatusError as exc:
        detail = response.text[:500]
        raise RuntimeError(
            f"lake window attestation failed ({response.status_code}): {detail}"
        ) from exc
    receipt = response.json()
    if not isinstance(receipt, dict):
        raise RuntimeError("lake window attestation response is not a JSON object")
    _verify_receipt(receipt, request)
    binding = LakeWindowBinding.model_validate(
        {
            "request": request_payload,
            "window_semantic_sha256": receipt["window_semantic_sha256"],
            "semantic_contract_id": receipt["semantic_contract_id"],
            "attestation_sha256": receipt["attestation_sha256"],
            "creation_global_coverage_sha256": receipt.get(
                "observed_global_coverage_sha256"
            ),
            "creation_source_coverage_sha256": receipt.get(
                "observed_source_coverage_sha256"
            ),
            "legacy_selection_manifest_sha256": legacy_selection_manifest_sha256,
        }
    )
    with _CACHE_LOCK:
        existing = _CACHE.setdefault(cache_key, binding)
    return existing
