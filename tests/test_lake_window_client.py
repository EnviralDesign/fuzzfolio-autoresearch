from __future__ import annotations

import httpx
import pytest

from autoresearch import lake_window_client as client
from autoresearch.lake_window import LakeWindowRequest


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


def test_resolve_lake_window_binding_verifies_receipt_and_memoizes(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = LakeWindowRequest(
        pairs=["EURUSD"],
        timeframes=["M5"],
        data_start="2023-01-01T00:00:00Z",
        data_end="2026-01-01T00:00:00Z",
    )
    receipt = _receipt(request)
    calls = 0

    def fake_post(*args, **kwargs):
        nonlocal calls
        calls += 1
        return httpx.Response(
            200,
            json=receipt,
            request=httpx.Request("POST", str(args[0])),
        )

    client._CACHE.clear()
    monkeypatch.setenv("REMOTE_MARKET_DATA_LAKE_BASE_URL", "http://lake.test")
    monkeypatch.setenv("REMOTE_MARKET_DATA_LAKE_API_TOKEN", "secret")
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
    request = LakeWindowRequest(
        pairs=["EURUSD"],
        timeframes=["M5"],
        data_start="2023-01-01T00:00:00Z",
        data_end="2026-01-01T00:00:00Z",
    )
    receipt = _receipt(request)
    receipt["scopes"][0]["bar_count"] = 124
    client._CACHE.clear()
    monkeypatch.setenv("REMOTE_MARKET_DATA_LAKE_BASE_URL", "http://lake.test")
    monkeypatch.setenv("REMOTE_MARKET_DATA_LAKE_API_TOKEN", "secret")
    monkeypatch.setattr(
        client.httpx,
        "post",
        lambda *args, **kwargs: httpx.Response(
            200,
            json=receipt,
            request=httpx.Request("POST", str(args[0])),
        ),
    )

    with pytest.raises(RuntimeError, match="semantic SHA-256 is invalid"):
        client.resolve_lake_window_binding(
            request,
            legacy_selection_manifest_sha256="sha256:" + "d" * 64,
        )
