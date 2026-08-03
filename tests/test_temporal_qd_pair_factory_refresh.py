from __future__ import annotations

import copy

import pytest

from autoresearch import temporal_qd_pair_factory as factory
from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError
from autoresearch import temporal_qd_evolution as qd


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


def test_runtime_loader_freezes_authored_input_and_verifies_frozen_input(monkeypatch) -> None:
    raw = _authored()
    monkeypatch.setattr(factory, "freeze_pair_run_config", lambda value: {"frozen": value})
    assert factory.load_pair_run_config(raw) == {"frozen": raw}

    verified: list[dict] = []

    class Bundle:
        def __init__(self, value):
            verified.append(value)

        def __enter__(self):
            return self

        def __exit__(self, *_):
            return None

    frozen = _frozen()
    monkeypatch.setattr(factory, "PairAuthorityBundle", Bundle)
    assert factory.load_pair_run_config(frozen) == frozen
    assert verified == [frozen]


def test_empty_archive_initialization_is_identity_bound_and_rejects_nonempty_templates() -> None:
    archive = {
        "schemaVersion": qd.QD_ARCHIVE_SCHEMA,
        "qdVersion": qd.QD_VERSION,
        "generationIndex": 0,
        "populationSha256": factory.canonical_sha256({"population": "empty"}),
        "resultSetSha256": factory.canonical_sha256({"results": "empty"}),
        "previousArchiveSha256": None,
        "policyName": qd.QD_POLICY_NAME,
        "policySha256": qd.QD_POLICY_SHA256,
        "frozenPolicy": qd.QD_POLICY,
        "cellCapacity": 4,
        "candidateCountSeen": 0,
        "occupiedCellCount": 0,
        "memberCount": 0,
        "qualityMemberCount": 0,
        "observationalMemberCount": 0,
        "negativeNoveltyMemberCount": 0,
        "cells": [],
    }
    archive["archiveSha256"] = factory.canonical_sha256(archive)
    policy = {
        "schemaVersion": "temporal_qd_bidirectional_pair_policy_v1",
        "enabled": True,
        "compilerAuthority": factory.IdentitySnapshot.create(
            kind="pairCompiler", schema_version="pair_compiler_v1", payload={"value": "test"}
        ).canonical_payload(),
    }
    initialized = qd.initialize_empty_bidirectional_archive(archive, policy)
    assert initialized["bidirectionalPairPolicy"] == policy
    supplied = initialized.pop("archiveSha256")
    assert supplied == factory.canonical_sha256(initialized)

    nonempty = copy.deepcopy(archive)
    nonempty["candidateCountSeen"] = 1
    nonempty["archiveSha256"] = factory.canonical_sha256({key: value for key, value in nonempty.items() if key != "archiveSha256"})
    with pytest.raises(TemporalDiscoveryContractError, match="exact empty"):
        qd.initialize_empty_bidirectional_archive(nonempty, policy)
