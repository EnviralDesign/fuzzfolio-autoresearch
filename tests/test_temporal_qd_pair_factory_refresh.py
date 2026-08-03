from __future__ import annotations

import copy

import pytest

from autoresearch import temporal_qd_pair_factory as factory
from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError


def _authored() -> dict:
    return {
        "schemaVersion": factory.PAIR_RUN_CONFIG_SCHEMA,
        "longModule": {"seedNames": ["trend"], "context": {}, "catalog": {}, "policy": {}},
        "shortModule": {"seedNames": ["breakout"], "context": {}, "catalog": {}, "policy": {}},
        "nativeJsonlAuthority": {"command": ["python", "validator.py"]},
        "holdOperatorPolicy": {"schemaVersion": factory.PAIR_HOLD_POLICY_SCHEMA},
    }


def _frozen() -> dict:
    context = {
        "instrument": "EURUSD",
        "indicators": [],
        "groups": [],
        "events": [],
        "executionConfig": {},
        "plans": [],
        "budgets": {},
    }
    side = {
        "seedNames": ["trend"],
        "context": context,
        "catalog": {"indicators": [], "timeframes": ["M5"]},
        "catalogSha256": "sha256:" + "1" * 64,
        "indicatorPolicy": {"policySha256": "sha256:" + "2" * 64},
        "policy": {"name": "closed"},
    }
    value = {
        "schemaVersion": factory.PAIR_RUN_CONFIG_SCHEMA,
        "longModule": side,
        "shortModule": copy.deepcopy(side),
        "grammarRegistry": {},
        "holdOperatorPolicy": {"schemaVersion": factory.PAIR_HOLD_POLICY_SCHEMA},
        "nativeJsonlAuthority": {"command": ["python", "validator.py"], "authorityContent": {"old": True}},
        "nativeAuthority": {},
        "pairCompilerAuthority": {},
        "operatorImplementation": {},
    }
    value["pairRunConfigSha256"] = factory.canonical_sha256(value)
    return value


def test_refresh_accepts_authored_input_without_trusting_derived_fields(monkeypatch) -> None:
    raw = _authored()
    monkeypatch.setattr(factory, "freeze_pair_run_config", lambda value: {"captured": value})
    assert factory.refresh_pair_run_config(raw) == {"captured": raw}


def test_refresh_projects_only_authored_fields_from_a_self_valid_frozen_template(monkeypatch) -> None:
    captured: dict = {}

    def freeze(value):
        captured.update(value)
        return {"ok": True}

    monkeypatch.setattr(factory, "freeze_pair_run_config", freeze)
    assert factory.refresh_pair_run_config(_frozen()) == {"ok": True}
    assert set(captured) == {"schemaVersion", "longModule", "shortModule", "nativeJsonlAuthority", "holdOperatorPolicy"}
    assert set(captured["longModule"]) == {"seedNames", "context", "catalog", "policy"}
    assert captured["longModule"]["context"]["evidenceGroups"] == []
    assert captured["longModule"]["context"]["eventBindings"] == []
    assert "authorityContent" not in captured["nativeJsonlAuthority"]


def test_refresh_rejects_a_tampered_frozen_template(monkeypatch) -> None:
    template = _frozen()
    template["longModule"]["seedNames"] = ["breakout"]
    monkeypatch.setattr(factory, "freeze_pair_run_config", lambda value: value)
    with pytest.raises(TemporalDiscoveryContractError, match="identity/schema mismatch"):
        factory.refresh_pair_run_config(template)
