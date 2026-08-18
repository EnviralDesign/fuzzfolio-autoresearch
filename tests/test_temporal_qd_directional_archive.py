from __future__ import annotations

from copy import deepcopy
import json
from pathlib import Path

import pytest

import autoresearch.temporal_qd_evolution as qd
from autoresearch.temporal_direction_selection import (
    LANE_BALANCED_BIDIRECTIONAL,
    LANE_HARMFUL_OPPOSITE_SIDE,
    LANE_LONG_SPECIALIST,
    LANE_SHORT_SPECIALIST,
)
from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError, canonical_sha256
from autoresearch.temporal_realized_behavior import REALIZED_BEHAVIOR_SCHEMA


def _behavior(
    *,
    long_net: float,
    short_net: float,
    long_trades: int = 2,
    short_trades: int = 2,
    long_windows: int = 1,
    short_windows: int = 1,
) -> dict:
    def side(net: float, trades: int, windows: int) -> dict:
        gross = net + (0.2 if trades else 0.0)
        return {
            "closedTrades": trades,
            "activeWindowCount": windows,
            "activeWindowFraction": windows / 4,
            "grossR": gross,
            "netR": net,
            "costR": gross - net,
            "active": bool(trades),
            "terminalDirectionCount": 0,
        }

    material = {
        "schemaVersion": REALIZED_BEHAVIOR_SCHEMA,
        "windowCount": 4,
        "sides": {
            "long": side(long_net, long_trades, long_windows),
            "short": side(short_net, short_trades, short_windows),
        },
    }
    material["identityMaterial"] = {"sides": deepcopy(material["sides"])}
    material["identitySha256"] = canonical_sha256(material["identityMaterial"])
    return material


def _member(candidate_id: str, behavior: dict, *, robust: float = 1.0) -> dict:
    descriptor = {
        "operatorFamilies": "one",
        "mutationDepth": "one",
        "entryEvents": "one",
        "managementActions": "one",
        "graphNodes": "medium",
        "tradeFrequency": "moderate",
        "medianHolding": "medium",
    }
    descriptor["cellId"] = "|".join(descriptor[key] for key in qd.QD_DESCRIPTOR_AXES)
    aggregate = {
        "realizedBehavior": behavior,
        "resolvedProgramSha256": "sha256:" + candidate_id.zfill(64)[-64:],
    }
    selection = qd._direction_selection_for_aggregate(aggregate)
    return {
        "candidateId": candidate_id,
        "aggregate": aggregate,
        "descriptor": descriptor,
        "objectives": {
            "worstWindowConservativeNetR": robust,
            "maximumDrawdownR": 1.0,
            "structuralComplexity": 1.0,
        },
        "finiteDataValidity": {
            "isFiniteData": True,
            "passesSupportGate": True,
            "validForQuality": True,
            "totalTrades": 12,
            "capTrades": 20,
        },
        "cappedTradeSupport": 12.0,
        "directionSelection": selection,
        "directionBehaviorLane": selection["lane"],
        "directionBreedingLane": (
            selection["lane"] if selection["selectionEligible"] else None
        ),
    }


def test_directional_cell_reserves_balanced_and_both_specialist_lanes() -> None:
    members = [
        _member("b1", _behavior(long_net=2.0, short_net=1.0), robust=4.0),
        _member("b2", _behavior(long_net=1.5, short_net=1.0), robust=3.0),
        _member("b3", _behavior(long_net=1.0, short_net=1.0), robust=2.0),
        _member("long", _behavior(long_net=2.0, short_net=0.0, short_trades=0, short_windows=0)),
        _member("short", _behavior(long_net=0.0, short_net=2.0, long_trades=0, long_windows=0)),
        _member("harm", _behavior(long_net=4.0, short_net=-1.0), robust=9.0),
    ]
    cells = qd.select_qd_archive(members, cell_capacity=6, direction_aware=True)
    selected = cells[0]["members"]
    lanes = [row.get("directionBreedingLane") for row in selected]
    assert lanes.count(LANE_BALANCED_BIDIRECTIONAL) >= 2
    assert lanes.count(LANE_LONG_SPECIALIST) == 1
    assert lanes.count(LANE_SHORT_SPECIALIST) == 1
    harmful = next(row for row in selected if row["candidateId"] == "harm")
    assert harmful["archiveLane"] == "observational"
    assert harmful["directionBehaviorLane"] == LANE_HARMFUL_OPPOSITE_SIDE
    assert harmful["directionBreedingLane"] is None
    assert cells[0]["breedingEligibleMemberCount"] == 5


def test_directional_fallback_is_deterministic_and_never_promotes_an_ineligible_side() -> None:
    members = [
        _member(f"b{index}", _behavior(long_net=1.0 + index, short_net=1.0), robust=float(index))
        for index in range(5)
    ]
    first = qd.select_qd_archive(members, cell_capacity=4, direction_aware=True)
    second = qd.select_qd_archive(deepcopy(members), cell_capacity=4, direction_aware=True)
    assert canonical_sha256(first) == canonical_sha256(second)
    assert all(
        member["directionBreedingLane"] == LANE_BALANCED_BIDIRECTIONAL
        for member in first[0]["members"]
    )


def test_v5_geometry_recomputes_the_direction_binding_and_old_policies_stay_read_only() -> None:
    member = _member("balanced", _behavior(long_net=1.0, short_net=1.0))
    cell = qd.select_qd_archive([member], direction_aware=True)[0]
    archive = {
        "schemaVersion": qd.QD_ARCHIVE_SCHEMA,
        "qdVersion": qd.QD_VERSION,
        "policyName": qd.DIRECTIONAL_QD_POLICY_NAME,
        "policySha256": qd.DIRECTIONAL_QD_POLICY_SHA256,
        "frozenPolicy": deepcopy(qd.DIRECTIONAL_QD_POLICY),
        "cells": [cell],
    }
    archive["archiveSha256"] = canonical_sha256(archive)
    qd.validate_qd_archive_geometry(archive)

    forged = deepcopy(archive)
    forged["cells"][0]["members"][0]["directionBreedingLane"] = LANE_LONG_SPECIALIST
    with pytest.raises(TemporalDiscoveryContractError, match="breeding lane mismatch"):
        qd.validate_qd_archive_geometry(forged)

    stale_behavior = deepcopy(archive)
    stale_behavior["cells"][0]["members"][0]["aggregate"]["realizedBehavior"][
        "sides"
    ]["short"]["netR"] = -4.0
    with pytest.raises(TemporalDiscoveryContractError, match="side identity drifted"):
        qd.validate_qd_archive_geometry(stale_behavior)

    for name, policy, sha in (
        (qd.LEGACY_QD_POLICY_NAME, qd.LEGACY_QD_POLICY, qd.LEGACY_QD_POLICY_SHA256),
        (qd.CORRECTED_QD_POLICY_NAME, qd.CORRECTED_QD_POLICY, qd.CORRECTED_QD_POLICY_SHA256),
    ):
        old = {
            "schemaVersion": qd.QD_ARCHIVE_SCHEMA,
            "qdVersion": qd.QD_VERSION,
            "policyName": name,
            "policySha256": sha,
            "frozenPolicy": deepcopy(policy),
            "cells": [],
        }
        old["archiveSha256"] = canonical_sha256(old)
        assert qd._archive_policy_kind(old) in {"legacy", "corrected"}
        with pytest.raises(TemporalDiscoveryContractError, match="cannot mix"):
            qd._require_directional_archive_policy(old, context="fresh v5 run")


def test_v5_empty_template_and_ledger_are_fresh_and_cannot_be_confused_with_v4(tmp_path) -> None:
    template = qd.canonical_empty_directional_bidirectional_archive_template()
    assert qd._archive_policy_kind(template) == "directional"
    assert template["cells"] == []
    assert template["archiveSha256"] == canonical_sha256(
        {key: value for key, value in template.items() if key != "archiveSha256"}
    )
    with pytest.raises(TemporalDiscoveryContractError, match="unknown empty direction-aware"):
        qd.initialize_empty_directional_bidirectional_archive(
            qd.canonical_empty_bidirectional_archive_template(),
            {"schemaVersion": "temporal_qd_bidirectional_pair_policy_v1", "enabled": True, "compilerAuthority": {}},
        )
    ledger_path = tmp_path / "identity-ledger.json"
    ledger = qd._load_identity_ledger(
        ledger_path,
        policy_name=qd.DIRECTIONAL_QD_POLICY_NAME,
        policy_sha256=qd.DIRECTIONAL_QD_POLICY_SHA256,
        identity_policy=qd.DIRECTIONAL_QD_POLICY["identity"],
    )
    assert ledger["policyName"] == qd.DIRECTIONAL_QD_POLICY_NAME
    with pytest.raises(TemporalDiscoveryContractError, match="bound to another policy"):
        qd._load_identity_ledger(ledger_path)


def test_v5_writer_authority_is_exact_not_a_feature_toggle() -> None:
    authority = qd.directional_qd_archive_policy_authority()
    name, sha, policy, directional = qd._resolve_archive_policy_authority(authority)
    assert (name, sha, policy, directional) == (
        qd.DIRECTIONAL_QD_POLICY_NAME,
        qd.DIRECTIONAL_QD_POLICY_SHA256,
        qd.DIRECTIONAL_QD_POLICY,
        True,
    )
    authority["policySha256"] = qd.QD_POLICY_SHA256
    with pytest.raises(TemporalDiscoveryContractError, match="unknown QD archive policy authority"):
        qd._resolve_archive_policy_authority(authority)


def test_v5_rotating_frontier_uses_cumulative_direction_behavior(tmp_path) -> None:
    """A negative robust fallback is breedable only when cumulative sides are safe.

    This covers the otherwise subtle lifecycle seam: the current panel's
    direction lane cannot override a harmful side discovered in an earlier
    required panel.
    """
    previous = qd.canonical_empty_directional_bidirectional_archive_template()
    previous_path = tmp_path / "previous-v5.json"
    previous_path.write_text(json.dumps(previous), encoding="utf-8")
    descriptor = _member("seed", _behavior(long_net=1.0, short_net=1.0))["descriptor"]

    def build(*, candidate_id: str, behavior: dict, output_name: str | None = None):
        candidate = {
            "candidateId": candidate_id,
            "candidateIdentitySha256": "sha256:" + candidate_id[0] * 64,
            "programSha256": "sha256:" + candidate_id[-1] * 64,
        }
        selection = qd._direction_selection_for_aggregate(
            {"realizedBehavior": behavior}
        )
        cumulative_member = {
            **candidate,
            "cellId": descriptor["cellId"],
            "currentPanelRank": 0.0,
            "coveredMonths": 24,
            "windowMetrics": [],
            "cumulativeRealizedBehavior": behavior,
            "directionSelection": selection,
            "directionBehaviorLane": selection["lane"],
            "directionBreedingLane": (
                selection["lane"] if selection["selectionEligible"] else None
            ),
            "robustObjectives": {
                "worstWindowConservativeNetR": -0.1,
                "drawdown": 1.0,
                "costDrag": 0.2,
                "novelty": 1.0,
            },
        }
        cumulative = {
            "schemaVersion": "temporal_qd_cumulative_breeder_archive_v1",
            "mode": "replace",
            "rotatingEvidenceSha256": "sha256:" + "c" * 64,
            "generationIndex": 2,
            "requiredPanelIds": ["panel-1", "panel-2"],
            "breederWidth": 1,
            "qualityCandidateIds": [],
            "frontierCandidateIds": [candidate_id],
            "members": [cumulative_member],
        }
        cumulative["archiveSha256"] = canonical_sha256(cumulative)
        current = {
            "candidateId": candidate_id,
            "candidate": candidate,
            # Deliberately benign current-panel behavior: only the cumulative
            # behavior is eligible to decide the rotating parent lane.
            "aggregate": {"realizedBehavior": _behavior(long_net=1.0, short_net=1.0)},
            "descriptor": descriptor,
            "objectives": {"worstWindowConservativeNetR": -0.1, "maximumDrawdownR": 1.0, "structuralComplexity": 1.0},
            "finiteDataValidity": {"isFiniteData": True, "passesSupportGate": True, "validForQuality": True, "totalTrades": 8, "capTrades": 20},
            "cappedTradeSupport": 8.0,
        }
        return qd.build_rotating_qd_parent_archive(
            current_members=[current], cumulative_archive=cumulative,
            output_path=tmp_path / f"{output_name or candidate_id}.json", generation_index=2,
            previous_archive_path=previous_path,
        )

    safe = build(candidate_id="safe", behavior=_behavior(long_net=1.0, short_net=1.0))
    safe_archive, _ = qd._load_archive(tmp_path / "safe.json")
    assert safe["frontierMemberCount"] == 1
    assert qd._reproduction_cells(safe_archive)[0]["members"][0]["directionBreedingLane"] == LANE_BALANCED_BIDIRECTIONAL
    safe_restart = build(
        candidate_id="safe",
        behavior=_behavior(long_net=1.0, short_net=1.0),
        output_name="safe-restart",
    )
    assert safe_restart["archiveSha256"] == safe["archiveSha256"]

    harmful = build(candidate_id="harmful", behavior=_behavior(long_net=3.0, short_net=-1.0))
    harmful_archive, _ = qd._load_archive(tmp_path / "harmful.json")
    member = harmful_archive["cells"][0]["members"][0]
    assert harmful["frontierMemberCount"] == 1
    assert member["directionBehaviorLane"] == LANE_HARMFUL_OPPOSITE_SIDE
    assert member["directionBreedingLane"] is None
    with pytest.raises(TemporalDiscoveryContractError, match="no quality-eligible"):
        qd._reproduction_cells(harmful_archive)


def _directional_archive(members: list[dict], *, omit_projection: bool = False) -> dict:
    cells = qd.select_qd_archive(deepcopy(members), cell_capacity=4, direction_aware=True)
    if omit_projection:
        for cell in cells:
            for member in cell["members"]:
                for key in ("directionSelection", "directionBehaviorLane", "directionBreedingLane"):
                    member.pop(key, None)
    archive = {
        "schemaVersion": qd.QD_ARCHIVE_SCHEMA,
        "qdVersion": qd.QD_VERSION,
        "policyName": qd.DIRECTIONAL_QD_POLICY_NAME,
        "policySha256": qd.DIRECTIONAL_QD_POLICY_SHA256,
        "frozenPolicy": deepcopy(qd.DIRECTIONAL_QD_POLICY),
        "cells": cells,
    }
    archive["archiveSha256"] = canonical_sha256(
        {key: value for key, value in archive.items() if key != "archiveSha256"}
    )
    return archive


def test_native_archive_omits_direction_projection_and_parent_load_derives_it(tmp_path) -> None:
    archive = _directional_archive(
        [_member("balanced", _behavior(long_net=1.0, short_net=1.0))],
        omit_projection=True,
    )
    member = archive["cells"][0]["members"][0]
    assert "directionSelection" not in member
    assert "directionBehaviorLane" not in member
    assert "directionBreedingLane" not in member
    qd.validate_qd_archive_geometry(archive)

    path = tmp_path / "native-parent.json"
    path.write_text(json.dumps(archive), encoding="utf-8")
    loaded, archive_sha = qd._load_archive(path)
    assert archive_sha == archive["archiveSha256"]
    hydrated = loaded["cells"][0]["members"][0]
    assert hydrated["directionBehaviorLane"] == LANE_BALANCED_BIDIRECTIONAL
    assert hydrated["directionBreedingLane"] == LANE_BALANCED_BIDIRECTIONAL
    assert qd._direction_breeding_lane(hydrated) == LANE_BALANCED_BIDIRECTIONAL
    assert "directionSelection" not in member


def test_partial_native_direction_projection_is_still_rejected() -> None:
    archive = _directional_archive(
        [_member("balanced", _behavior(long_net=1.0, short_net=1.0))],
        omit_projection=True,
    )
    archive["cells"][0]["members"][0]["directionBehaviorLane"] = LANE_BALANCED_BIDIRECTIONAL
    archive["archiveSha256"] = canonical_sha256(
        {key: value for key, value in archive.items() if key != "archiveSha256"}
    )
    with pytest.raises(
        TemporalDiscoveryContractError,
        match="lacks bound realized behavior selection",
    ):
        qd.validate_qd_archive_geometry(archive)


_V32_NATIVE_G1_ARCHIVE = Path(
    "runs/temporal-qd-v5-fast-ephemeral-4000x1024x5-20260817-v32"
    "/run/broad-4000x1024x5/generations/generation-0001/native-finalization/archive.json"
)


@pytest.mark.skipif(
    not _V32_NATIVE_G1_ARCHIVE.is_file(),
    reason="v32 native G1 archive is not present",
)
def test_v32_native_g1_archive_loads_as_g2_parent() -> None:
    on_disk = json.loads(_V32_NATIVE_G1_ARCHIVE.read_text(encoding="utf-8"))
    loaded, archive_sha = qd._load_archive(_V32_NATIVE_G1_ARCHIVE)
    assert archive_sha == on_disk["archiveSha256"]
    assert loaded["cells"]
    for cell in loaded["cells"]:
        for member in cell["members"]:
            assert isinstance(member.get("directionSelection"), dict)
            assert member.get("directionBehaviorLane")
            assert "directionBreedingLane" in member
            if member.get("archiveLane") == "quality":
                assert qd._direction_breeding_lane(member) is not None
    cells = qd._reproduction_cells(loaded)
    assert cells
    assert sum(len(cell["members"]) for cell in cells) == 3
    from autoresearch.temporal_qd_pair_generation import select_breeding_confidence

    confidence = select_breeding_confidence(
        parent_archive=loaded, target_unique_candidates=1024
    )
    assert confidence["receipt"]["breedingEligibleParentCount"] == 3
