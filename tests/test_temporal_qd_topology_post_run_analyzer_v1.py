from __future__ import annotations

import copy

from autoresearch.temporal_qd_topology_post_run_analyzer_v1 import analyze_block


def _panel() -> dict:
    return {
        "P": {"conservativeNetR": 1.0, "worstWindowConservativeNetR": -1.0, "tradeCount": 10, "costDragR": 0.1, "support": True, "direction": True, "quality": True},
        "T": {"conservativeNetR": 1.5, "worstWindowConservativeNetR": -0.8, "tradeCount": 10, "costDragR": 0.1, "support": True, "direction": True, "quality": True},
        "E": {"conservativeNetR": 1.25, "worstWindowConservativeNetR": -0.9, "tradeCount": 10, "costDragR": 0.1, "support": True, "direction": True, "quality": True},
        "TE": {"conservativeNetR": 2.0, "worstWindowConservativeNetR": -0.7, "tradeCount": 10, "costDragR": 0.1, "support": True, "direction": True, "quality": True},
    }


def test_same_block_must_pass_all_three_panels() -> None:
    panels = {panel: _panel() for panel in ("panel-1", "panel-2", "panel-3")}
    result = analyze_block(block_id="block-1", panels=panels)
    assert result["replication"]["inspectedPromising"] is True
    panels["panel-2"]["TE"]["conservativeNetR"] = 1.5
    result = analyze_block(block_id="block-1", panels=panels)
    assert result["replication"]["inspectedPromising"] is False
    assert result["replication"]["reportingCategory"] == "development_only_not_replicated"


def test_risk_gate_dust_and_missing_are_fail_closed_but_distinct() -> None:
    panels = {panel: _panel() for panel in ("panel-1", "panel-2", "panel-3")}
    risk = copy.deepcopy(panels)
    risk["panel-1"]["TE"]["worstWindowConservativeNetR"] = -0.85
    assert analyze_block(block_id="risk", panels=risk)["panelReports"]["panel-1"]["nonqualifyingRiskTradeoff"] is True
    gated = copy.deepcopy(panels)
    gated["panel-1"]["TE"]["quality"] = False
    assert analyze_block(block_id="gate", panels=gated)["replication"]["inspectedPromising"] is False
    dust = copy.deepcopy(panels)
    dust["panel-1"]["TE"]["conservativeNetR"] = 1.5 + 1e-13
    assert analyze_block(block_id="dust", panels=dust)["panelReports"]["panel-1"]["usefulProgressiveInnovation"] is False
    missing = dict(panels)
    missing.pop("panel-2")
    assert analyze_block(block_id="missing", panels=missing)["replication"]["reportingCategory"] == "incomplete_invalid"
    invalid = copy.deepcopy(panels)
    invalid["panel-2"]["TE"]["tradeCount"] = -1
    assert analyze_block(block_id="invalid", panels=invalid)["replication"]["reportingCategory"] == "incomplete_invalid"
    extra = dict(panels)
    extra["panel-4"] = _panel()
    assert analyze_block(block_id="extra", panels=extra)["replication"]["reportingCategory"] == "incomplete_invalid"
