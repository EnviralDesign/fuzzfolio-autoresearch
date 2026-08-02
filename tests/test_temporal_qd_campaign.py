from __future__ import annotations

import json
from pathlib import Path

import pytest

from autoresearch.temporal_discovery_base import (
    TemporalDiscoveryContractError,
    canonical_sha256,
)
from autoresearch.temporal_qd_campaign import freeze_qd_screening_campaign


def _write(path: Path, value: dict) -> None:
    path.write_text(json.dumps(value, sort_keys=True), encoding="utf-8")


def _profile(*, indicator_timeframe: str | None = None) -> dict:
    profile = {
        "version": "v2",
        "graph": {"kind": "temporal_graph_v1"},
        "instruments": ["EURUSD"],
        "directionMode": "long",
        "isActive": False,
        "executionConfig": {
            "exitPolicy": {
                "selectedCell": {
                    "rewardMultiple": 2.0,
                    "stopLossPercent": 0.5,
                    "takeProfitPercent": 1.0,
                }
            }
        },
    }
    if indicator_timeframe is not None:
        profile["indicators"] = [
            {
                "meta": {
                    "id": "FIXTURE",
                    "instanceId": "signal",
                    "requiredPaddingBars": 10,
                },
                "config": {
                    "isActive": True,
                    "timeframe": indicator_timeframe,
                    "lookbackBars": 14,
                },
            }
        ]
    return profile


def _template() -> dict:
    profile = _profile()
    plan = {
        "schema_version": "fuzzfolio.replay-evidence-plan.v2",
        "profile_snapshot_sha256": canonical_sha256(profile),
        "analysis_window_start": "2024-02-01T00:00:00Z",
        "analysis_window_end": "2024-03-01T00:00:00Z",
        "execution_cell_sha256": canonical_sha256(
            profile["executionConfig"]["exitPolicy"]["selectedCell"]
        ),
        "lake_window_binding": {
            "window_semantic_sha256": "sha256:" + "b" * 64,
            "request": {
                "data_start": "2024-01-01T00:00:00Z",
                "data_end": "2024-03-01T00:00:00Z",
                "pairs": ["EURUSD"],
                "timeframes": ["M5"],
            },
        },
    }
    plan["plan_id"] = canonical_sha256(plan)
    return {
        "schemaVersion": "temporal_graph_candidate_window_preparation_v1",
        "authorityLabel": "frozen-development",
        "workerContract": {
            "workerContractSha256": "sha256:" + "c" * 64,
            "workerContractSchema": "replay-worker-contract-v1",
        },
        "candidates": [
            {
                "candidateId": "template",
                "sourceProfile": profile,
                "sourceProfileSha256": canonical_sha256(profile),
                "instrument": "EURUSD",
                "timeframe": "M5",
                "barLimit": 5000,
                "windowInputs": [{"windowId": "development", "evidencePlan": plan}],
            }
        ],
        "developmentWindows": [
            {
                "windowId": "development",
                "analysisWindowStart": "2024-02-01T00:00:00Z",
                "analysisWindowEnd": "2024-03-01T00:00:00Z",
            }
        ],
        "prohibitedEvidence": [
            {
                "windowId": "reserved",
                "analysisWindowStart": "2024-06-29T00:00:00Z",
                "analysisWindowEnd": "2024-07-01T00:00:00Z",
                "reason": "reserved",
            }
        ],
        "bounds": {
            "maxCandidates": 1,
            "maxDevelopmentWindows": 1,
            "maxTasks": 1,
            "maxAttempts": 2,
            "deadlineSeconds": 60,
        },
    }


def _catalog(*, required_padding_bars: int = 10) -> dict:
    return {
        "timeframes": {"M1": {}, "M5": {}, "M30": {}, "H1": {}},
        "indicators": [
            {
                "meta": {
                    "id": "FIXTURE",
                    "requiredPaddingBars": required_padding_bars,
                },
                "config": {
                    "isActive": True,
                    "timeframe": "M5",
                    "lookbackBars": 14,
                },
            }
        ],
    }


@pytest.mark.parametrize("replacement", ["M1", "M30"])
def test_qd_campaign_refuses_timeframe_child_outside_pre_attested_scope(
    tmp_path: Path, replacement: str
) -> None:
    profile = _profile(indicator_timeframe=replacement)
    population = {
        "schemaVersion": "temporal_discovery_population_v2",
        "candidateCount": 1,
        "candidates": [
            {
                "candidateId": "qd_timeframe_child",
                "sourceProfile": profile,
                "sourceProfileSha256": canonical_sha256(profile),
                "programSha256": "sha256:" + "d" * 64,
            }
        ],
    }
    population["populationSha256"] = canonical_sha256(population)
    population_path = tmp_path / "population.json"
    template_path = tmp_path / "template.json"
    catalog_path = tmp_path / "catalog.json"
    _write(population_path, population)
    _write(template_path, _template())
    _write(catalog_path, _catalog())

    with pytest.raises(
        TemporalDiscoveryContractError,
        match="outside the immutable pre-attested evidence binding",
    ):
        freeze_qd_screening_campaign(
            population_path=population_path,
            template_preparation_path=template_path,
            output_root=tmp_path / "campaign",
            execution_engine_commit="a" * 40,
            construction_catalog_path=catalog_path,
        )


def test_qd_campaign_rejects_abbreviated_profile_when_catalog_padding_exceeds_binding(
    tmp_path: Path,
) -> None:
    """The catalog, not authored indicator meta, sets the required warmup."""

    profile = _profile(indicator_timeframe="H1")
    profile["indicators"][0]["meta"].pop("requiredPaddingBars")
    population = {
        "schemaVersion": "temporal_discovery_population_v2",
        "candidateCount": 1,
        "candidates": [
            {
                "candidateId": "qd_abbreviated_rsi",
                "sourceProfile": profile,
                "sourceProfileSha256": canonical_sha256(profile),
                "programSha256": "sha256:" + "d" * 64,
            }
        ],
    }
    population["populationSha256"] = canonical_sha256(population)
    template = _template()
    # An authored/default-zero calculation needs just one day (14+10 H1 bars)
    # and would accept this.  The frozen catalog requires 260 padding bars,
    # which moves the true day-aligned request back to 2024-01-20.
    template["candidates"][0]["windowInputs"][0]["evidencePlan"][
        "lake_window_binding"
    ]["request"] = {
        "data_start": "2024-01-31T00:00:00Z",
        "data_end": "2024-03-01T00:00:00Z",
        "pairs": ["EURUSD"],
        "timeframes": ["M5", "H1"],
    }
    plan = template["candidates"][0]["windowInputs"][0]["evidencePlan"]
    plan["plan_id"] = canonical_sha256(plan)
    population_path = tmp_path / "population.json"
    template_path = tmp_path / "template.json"
    catalog_path = tmp_path / "catalog.json"
    _write(population_path, population)
    _write(template_path, template)
    _write(catalog_path, _catalog(required_padding_bars=260))

    with pytest.raises(
        TemporalDiscoveryContractError,
        match="outside the immutable pre-attested evidence binding",
    ):
        freeze_qd_screening_campaign(
            population_path=population_path,
            template_preparation_path=template_path,
            output_root=tmp_path / "campaign",
            execution_engine_commit="a" * 40,
            construction_catalog_path=catalog_path,
        )
