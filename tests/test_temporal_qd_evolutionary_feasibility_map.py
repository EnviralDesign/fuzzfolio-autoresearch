from __future__ import annotations

import json
from pathlib import Path

from autoresearch.temporal_qd_evolutionary_feasibility_map import build_feasibility_map


def _write_jsonl(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def _member(candidate_id: str, *, net: list[float], trades: list[int]) -> dict:
    windows = []
    for index, (net_r, trade_count) in enumerate(zip(net, trades, strict=True), start=1):
        gross = net_r + (0.1 * trade_count)
        windows.append(
            {
                "windowId": f"window-{index}",
                "analysisWindowStart": f"2024-0{index}-01T00:00:00Z",
                "analysisWindowEnd": f"2024-0{index}-02T00:00:00Z",
                "grossR": gross,
                "noCostNetR": gross,
                "conservativeNetR": net_r,
                "trades": trade_count,
                "averageHoldingBars": 40.0,
                "medianHoldingBars": 35.0,
                "exposureRatio": 0.1,
                "maxDrawdownR": 0.2,
                "actionCounts": {"enter_next_open": trade_count, "tighten_stop_next_open": trade_count},
                "realizedBehavior": {
                    "sides": {"long": {"netR": net_r}, "short": {"netR": 0.0}}
                },
            }
        )
    total_trades = sum(trades)
    total_gross = sum(window["grossR"] for window in windows)
    return {
        "candidateId": candidate_id,
        "generationIndex": 1,
        "candidate": {
            "candidateId": candidate_id,
            "candidateIdentitySha256": f"sha256:{candidate_id}",
            "sourceMode": "immigrant",
            "structuralOperatorHistory": [{"operation": "seed"}],
        },
        "aggregate": {
            "windowRecords": windows,
            "totalTrades": total_trades,
            "totalNoCostNetR": total_gross,
            "costDragR": 0.1 * total_trades,
            "totalConservativeNetR": sum(net),
            "medianHoldingBars": 35.0,
            "averageHoldingBars": 40.0,
            "averageExposureRatio": 0.1,
        },
        "finiteDataValidity": {"passesSupportGate": total_trades >= 8, "validForQuality": total_trades >= 8},
        "descriptor": {"tradeFrequency": "moderate", "medianHolding": "short"},
    }


def _bundle(member: dict) -> dict:
    return {
        "candidateId": member["candidateId"],
        "panelId": "panel-1",
        "windowEvidence": [
            {
                "windowId": window["windowId"],
                "analysisWindowStart": window["analysisWindowStart"],
                "analysisWindowEnd": window["analysisWindowEnd"],
                "metrics": {
                    "closedTrades": window["trades"],
                    "noCostNetR": window["noCostNetR"],
                    "conservativeNetR": window["conservativeNetR"],
                },
            }
            for window in member["aggregate"]["windowRecords"]
        ],
    }


def test_builds_deterministic_reconciled_map(tmp_path: Path) -> None:
    root = tmp_path / "v37"
    output = root / "run" / "broad" / "generations" / "generation-0001" / "campaign" / "proposal-current-panel" / "campaign-output"
    first = _member("candidate-a", net=[0.1, 0.1, 0.1, 0.1], trades=[2, 2, 2, 2])
    second = _member("candidate-b", net=[-0.2, -0.2, -0.2, -0.2], trades=[1, 1, 1, 1])
    _write_jsonl(output / "evaluated-members.jsonl", [first, second])
    _write_jsonl(output / "candidate-panel-bundles.jsonl", [_bundle(first), _bundle(second)])
    (root / "run" / "broad" / "native-finalization-authority.json").write_text("{}", encoding="utf-8")

    report_a = tmp_path / "report-a"
    result = build_feasibility_map(cohorts=[("v37", root)], references=[], output_dir=report_a)
    assert result["candidateWindowRows"] == 8
    assert result["candidateEvaluations"] == 2
    validation = json.loads((report_a / "validation-results.json").read_text(encoding="utf-8"))
    assert validation["checks"]["grossCostNetReconciliationFailureCount"] == 0
    assert validation["checks"]["nonfiniteMetricCount"] == 0
    assert validation["checks"]["manifestSelfHashValid"] is True
    assert validation["checks"]["binningSensitivityViewsPresent"] is True
    assert validation["checks"]["stabilityMethodSanity"] is True
    feasibility = json.loads((report_a / "feasibility-map.json").read_text(encoding="utf-8"))
    assert feasibility["censusByProtocol"]["temporal_qd_v5_fast_ephemeral_current_panel_v37"]["supportAndAfterCostPositiveCount"] == 1
    candidate_rows = [json.loads(line) for line in (report_a / "candidate-evaluations.jsonl").read_text(encoding="utf-8").splitlines()]
    assert [row["windowResamplePositiveRate"] for row in candidate_rows] == [1.0, 0.0]

    report_b = tmp_path / "report-b"
    build_feasibility_map(cohorts=[("v37", root)], references=[], output_dir=report_b)
    for filename in ("normalized-candidate-windows.jsonl", "candidate-evaluations.jsonl", "feasibility-map.json", "validation-results.json"):
        assert (report_a / filename).read_bytes() == (report_b / filename).read_bytes()


def test_reads_v38_score_as_a_separate_protocol_without_imputing_panel_data(tmp_path: Path) -> None:
    root = tmp_path / "v38"
    member = _member("matrix-child", net=[0.3, -0.1, 0.2, -0.1], trades=[3, 3, 3, 3])
    _write_jsonl(root / "score" / "evaluated-members.jsonl", [member])
    _write_jsonl(
        root / "score" / "compact-metrics.jsonl",
        [
            {
                "candidateId": "matrix-child",
                "parentCandidateId": "parent-a",
                "operatorFamily": "resource",
            }
        ],
    )
    reference = tmp_path / "synthetic-topology"
    reference.mkdir()
    report = tmp_path / "report"

    build_feasibility_map(
        cohorts=[("v38", root)],
        references=[("topology", reference)],
        output_dir=report,
    )

    windows = [json.loads(line) for line in (report / "normalized-candidate-windows.jsonl").read_text(encoding="utf-8").splitlines()]
    assert {row["protocolGroup"] for row in windows} == {"temporal_qd_v5_fast_ephemeral_operator_matrix_v38"}
    assert {row["operatorFamily"] for row in windows} == {"resource"}
    assert {row["parentCandidateId"] for row in windows} == {"parent-a"}
    assert {row["panelId"] for row in windows} == {None}
    manifest = json.loads((report / "corpus-manifest.json").read_text(encoding="utf-8"))
    assert manifest["referenceOnlyExclusions"][0]["inclusion"] == "reference_only_excluded_from_economic_map"
