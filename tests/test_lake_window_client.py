from __future__ import annotations

from typing import Any

import httpx
import pytest

from autoresearch import lake_window_client as client
from autoresearch.lake_window import LakeWindowRequest


def _request() -> LakeWindowRequest:
    return LakeWindowRequest(
        pairs=["EURUSD"],
        timeframes=["M5"],
        data_start="2023-01-01T00:00:00Z",
        data_end="2026-01-01T00:00:00Z",
    )


def _receipt(request: LakeWindowRequest) -> dict:
    included = ["pair", "timeframe", "bar_start_s", "open", "high", "low", "close"]
    excluded = ["last_updated"]
    scopes = [
        {
            "pair": "EURUSD",
            "timeframe": "M5",
            "bar_count": 123,
            "scope_semantic_sha256": "sha256:" + "1" * 64,
        }
    ]
    payload = {
        "schema_version": "fuzzfolio.market-data-window-attestation.v1",
        "request": request.canonical_payload(),
        "semantic_contract_id": "fuzzfolio.canonical-bars.semantic-digest.v2",
        "included_fields": included,
        "excluded_fields": excluded,
        "scopes": scopes,
        "window_semantic_sha256": client._canonical_sha256(
            {
                "request": request.canonical_payload(),
                "semantic_contract_id": "fuzzfolio.canonical-bars.semantic-digest.v2",
                "included_fields": included,
                "excluded_fields": excluded,
                "scopes": scopes,
            }
        ),
        "observed_global_coverage_sha256": "sha256:" + "2" * 64,
        "observed_source_coverage_sha256": "sha256:" + "3" * 64,
        "manifest_updated_at": "2026-07-22T00:00:00Z",
        "manifest_promoted_at": "2026-07-22T00:00:00Z",
        "promotion_authority_index_sha256": "sha256:" + "4" * 64,
        "attested_at": "2026-07-22T01:00:00Z",
        "attestation_sha256": "",
    }
    payload["attestation_sha256"] = client._canonical_sha256(
        {
            key: value
            for key, value in payload.items()
            if key not in {"attestation_sha256", "attested_at"}
        }
    )
    return payload


def _response(status_code: int, *, json_payload: Any, headers: dict[str, str] | None = None) -> httpx.Response:
    return httpx.Response(
        status_code,
        json=json_payload,
        headers=headers,
        request=httpx.Request("POST", "http://lake.test/api/lake/window-attestations/resolve"),
    )


def _configure(monkeypatch: pytest.MonkeyPatch) -> None:
    client._CACHE.clear()
    monkeypatch.setenv("REMOTE_MARKET_DATA_LAKE_BASE_URL", "http://lake.test")
    monkeypatch.setenv("REMOTE_MARKET_DATA_LAKE_API_TOKEN", "secret")
    monkeypatch.setenv(client.LAKE_WINDOW_RETRY_MAX_SECONDS_ENV, "60")
    monkeypatch.setenv(client.LAKE_WINDOW_RETRY_BASE_SECONDS_ENV, "1")
    monkeypatch.setenv(client.LAKE_WINDOW_RETRY_MAX_DELAY_SECONDS_ENV, "5")


def test_resolve_lake_window_binding_verifies_receipt_and_memoizes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request()
    receipt = _receipt(request)
    calls = 0

    def fake_post(*args, **kwargs):
        nonlocal calls
        calls += 1
        return _response(200, json_payload=receipt)

    _configure(monkeypatch)
    monkeypatch.setattr(client.httpx, "post", fake_post)

    first = client.resolve_lake_window_binding(
        request,
        legacy_selection_manifest_sha256="sha256:" + "d" * 64,
    )
    second = client.resolve_lake_window_binding(
        request,
        legacy_selection_manifest_sha256="sha256:" + "d" * 64,
    )

    assert first == second
    assert first.window_semantic_sha256 == receipt["window_semantic_sha256"]
    assert first.legacy_selection_manifest_sha256 == "sha256:" + "d" * 64
    assert calls == 1


def test_resolve_lake_window_binding_rejects_tampered_receipt(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request()
    receipt = _receipt(request)
    receipt["scopes"][0]["bar_count"] = 124
    _configure(monkeypatch)
    monkeypatch.setattr(
        client.httpx,
        "post",
        lambda *args, **kwargs: _response(200, json_payload=receipt),
    )

    with pytest.raises(RuntimeError, match="semantic SHA-256 is invalid"):
        client.resolve_lake_window_binding(
            request,
            legacy_selection_manifest_sha256="sha256:" + "d" * 64,
        )


def test_mutation_conflict_retries_until_attestation_succeeds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request()
    receipt = _receipt(request)
    responses = [
        _response(
            409,
            json_payload={
                "detail": "lake mutation is active; window attestation resolve is retryable"
            },
        ),
        _response(
            409,
            json_payload={
                "detail": "lake mutation is active; window attestation resolve is retryable"
            },
        ),
        _response(200, json_payload=receipt),
    ]
    sleeps: list[float] = []
    _configure(monkeypatch)
    monkeypatch.setattr(client.time, "sleep", sleeps.append)
    monkeypatch.setattr(client.httpx, "post", lambda *args, **kwargs: responses.pop(0))

    binding = client.resolve_lake_window_binding(
        request,
        legacy_selection_manifest_sha256="sha256:" + "d" * 64,
    )

    assert binding.window_semantic_sha256 == receipt["window_semantic_sha256"]
    assert sleeps == [1.0, 2.0]
    assert responses == []


def test_retry_after_header_controls_mutation_backoff(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request()
    receipt = _receipt(request)
    responses = [
        _response(
            409,
            json_payload={"detail": "lake mutation is active; retryable"},
            headers={"Retry-After": "7"},
        ),
        _response(200, json_payload=receipt),
    ]
    sleeps: list[float] = []
    _configure(monkeypatch)
    monkeypatch.setattr(client.time, "sleep", sleeps.append)
    monkeypatch.setattr(client.httpx, "post", lambda *args, **kwargs: responses.pop(0))

    client.resolve_lake_window_binding(
        request,
        legacy_selection_manifest_sha256="sha256:" + "d" * 64,
    )

    assert sleeps == [7.0]


def test_transport_failure_retries_idempotent_attestation_request(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request()
    receipt = _receipt(request)
    calls = 0
    sleeps: list[float] = []

    def fake_post(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise httpx.ConnectError(
                "lake temporarily unavailable",
                request=httpx.Request("POST", str(args[0])),
            )
        return _response(200, json_payload=receipt)

    _configure(monkeypatch)
    monkeypatch.setattr(client.time, "sleep", sleeps.append)
    monkeypatch.setattr(client.httpx, "post", fake_post)

    client.resolve_lake_window_binding(
        request,
        legacy_selection_manifest_sha256="sha256:" + "d" * 64,
    )

    assert calls == 2
    assert sleeps == [1.0]


def test_retry_deadline_can_fail_closed_without_waiting(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request()
    calls = 0
    sleeps: list[float] = []

    def fake_post(*args, **kwargs):
        nonlocal calls
        calls += 1
        return _response(
            409,
            json_payload={
                "detail": "lake mutation is active; window attestation resolve is retryable"
            },
        )

    _configure(monkeypatch)
    monkeypatch.setenv(client.LAKE_WINDOW_RETRY_MAX_SECONDS_ENV, "0")
    monkeypatch.setattr(client.time, "sleep", sleeps.append)
    monkeypatch.setattr(client.httpx, "post", fake_post)

    with pytest.raises(RuntimeError, match="retry deadline exceeded after 1 attempt"):
        client.resolve_lake_window_binding(
            request,
            legacy_selection_manifest_sha256="sha256:" + "d" * 64,
        )

    assert calls == 1
    assert sleeps == []


def test_nonretryable_conflict_still_fails_immediately(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request()
    calls = 0
    sleeps: list[float] = []

    def fake_post(*args, **kwargs):
        nonlocal calls
        calls += 1
        return _response(409, json_payload={"detail": "permanent identity conflict"})

    _configure(monkeypatch)
    monkeypatch.setattr(client.time, "sleep", sleeps.append)
    monkeypatch.setattr(client.httpx, "post", fake_post)

    with pytest.raises(RuntimeError, match=r"attestation failed \(409\)"):
        client.resolve_lake_window_binding(
            request,
            legacy_selection_manifest_sha256="sha256:" + "d" * 64,
        )

    assert calls == 1
    assert sleeps == []
