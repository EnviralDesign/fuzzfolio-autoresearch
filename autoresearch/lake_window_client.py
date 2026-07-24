from __future__ import annotations

from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
import hashlib
import json
import logging
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


logger = logging.getLogger(__name__)

_CACHE: dict[str, LakeWindowBinding] = {}
_CACHE_LOCK = threading.Lock()

LAKE_WINDOW_RETRY_MAX_SECONDS_ENV = "FUZZFOLIO_LAKE_WINDOW_RETRY_MAX_SECONDS"
LAKE_WINDOW_RETRY_BASE_SECONDS_ENV = "FUZZFOLIO_LAKE_WINDOW_RETRY_BASE_SECONDS"
LAKE_WINDOW_RETRY_MAX_DELAY_SECONDS_ENV = "FUZZFOLIO_LAKE_WINDOW_RETRY_MAX_DELAY_SECONDS"
DEFAULT_LAKE_WINDOW_RETRY_MAX_SECONDS = 2 * 60 * 60.0
DEFAULT_LAKE_WINDOW_RETRY_BASE_SECONDS = 2.0
DEFAULT_LAKE_WINDOW_RETRY_MAX_DELAY_SECONDS = 30.0
_RETRYABLE_HTTP_STATUS_CODES = frozenset({425, 429, 502, 503, 504})


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


def _env_nonnegative_float(name: str, default: float) -> float:
    raw = os.environ.get(name)
    if raw is None:
        return max(float(default), 0.0)
    try:
        return max(float(raw), 0.0)
    except (TypeError, ValueError):
        return max(float(default), 0.0)


def _response_detail(response: httpx.Response) -> str:
    try:
        payload = response.json()
    except (ValueError, json.JSONDecodeError):
        return response.text[:500]
    if isinstance(payload, dict) and payload.get("detail") is not None:
        return str(payload["detail"])[:500]
    return response.text[:500]


def _retryable_attestation_response(response: httpx.Response) -> bool:
    status = int(response.status_code)
    if status in _RETRYABLE_HTTP_STATUS_CODES:
        return True
    if status != 409:
        return False
    detail = _response_detail(response).lower()
    return (
        "retryable" in detail
        or "lake mutation" in detail
        or "stable-read snapshot changed" in detail
    )


def _retry_after_seconds(response: httpx.Response) -> float | None:
    raw = str(response.headers.get("Retry-After") or "").strip()
    if not raw:
        return None
    try:
        return max(float(raw), 0.0)
    except ValueError:
        try:
            parsed = parsedate_to_datetime(raw)
        except (TypeError, ValueError, OverflowError):
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=timezone.utc)
        return max((parsed - datetime.now(timezone.utc)).total_seconds(), 0.0)


def _retry_backoff_seconds(attempt: int, *, base_seconds: float, max_delay_seconds: float) -> float:
    if base_seconds <= 0 or max_delay_seconds <= 0:
        return 0.0
    exponent = min(max(int(attempt) - 1, 0), 16)
    return min(base_seconds * (2**exponent), max_delay_seconds)


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
    retry_max_seconds = _env_nonnegative_float(
        LAKE_WINDOW_RETRY_MAX_SECONDS_ENV,
        DEFAULT_LAKE_WINDOW_RETRY_MAX_SECONDS,
    )
    retry_base_seconds = _env_nonnegative_float(
        LAKE_WINDOW_RETRY_BASE_SECONDS_ENV,
        DEFAULT_LAKE_WINDOW_RETRY_BASE_SECONDS,
    )
    retry_max_delay_seconds = _env_nonnegative_float(
        LAKE_WINDOW_RETRY_MAX_DELAY_SECONDS_ENV,
        DEFAULT_LAKE_WINDOW_RETRY_MAX_DELAY_SECONDS,
    )
    retry_started = time.monotonic()
    response: httpx.Response | None = None
    attempt = 0

    while True:
        attempt += 1
        transport_error: httpx.TransportError | None = None
        try:
            response = httpx.post(
                endpoint,
                headers=headers,
                json=request_payload,
                timeout=httpx.Timeout(timeout_seconds),
            )
        except httpx.TransportError as exc:
            transport_error = exc
            retryable = True
            detail = f"{type(exc).__name__}: {exc}"
            status_text = "transport_error"
        else:
            retryable = _retryable_attestation_response(response)
            detail = _response_detail(response)
            status_text = str(response.status_code)
            if not retryable:
                break

        elapsed = max(time.monotonic() - retry_started, 0.0)
        if retry_max_seconds <= 0 or elapsed >= retry_max_seconds:
            message = (
                "lake window attestation retry deadline exceeded "
                f"after {attempt} attempt(s) and {elapsed:.1f}s "
                f"(status={status_text}): {detail}"
            )
            if transport_error is not None:
                raise RuntimeError(message) from transport_error
            raise RuntimeError(message)

        retry_after = (
            _retry_after_seconds(response)
            if response is not None and transport_error is None
            else None
        )
        delay = (
            retry_after
            if retry_after is not None
            else _retry_backoff_seconds(
                attempt,
                base_seconds=retry_base_seconds,
                max_delay_seconds=retry_max_delay_seconds,
            )
        )
        remaining = max(retry_max_seconds - elapsed, 0.0)
        delay = min(max(delay, 0.0), remaining)
        logger.warning(
            "lake_window_attestation_retry attempt=%s status=%s delay_seconds=%.1f "
            "elapsed_seconds=%.1f detail=%s",
            attempt,
            status_text,
            delay,
            elapsed,
            detail,
        )
        if delay > 0:
            time.sleep(delay)

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
