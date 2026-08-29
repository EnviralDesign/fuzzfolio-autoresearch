from __future__ import annotations

import json
from pathlib import Path

from autoresearch.temporal_qd_v37_archive_terminalization import (
    TRACE_SCHEMA_VERSION,
    _canonical_sha256,
    write_v37_archive_terminalization_report,
)


def _write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _write_jsonl(path: Path, rows: list[dict[str, object]]) -> None:
    path.write_text(
        "".join(
            json.dumps(row, ensure_ascii=True, separators=(",", ":"), sort_keys=True) + "\n"
            for row in rows
        ),
        encoding="utf-8",
    )


def _trace_row(candidate_id: str, lane: str) -> dict[str, object]:
    selected = lane in {"quality", "frontier"}
    return {
        "candidateId": candidate_id,
        "finalLane": lane,
        "preParetoLane": lane if selected else "unsupported",
        "selectedAfterPareto": selected,
        "activeSupportPassed": selected,
        "tradeDensityPassed": selected,
        "directionSelectionEligible": selected,
        "preParetoReasonCodes": ["selected_quality"] if selected else ["no_acceptable_direction"],
    }


def _write_trace(path: Path, generation: int, rows: list[dict[str, object]]) -> None:
    trace: dict[str, object] = {
        "schemaVersion": TRACE_SCHEMA_VERSION,
        "generationIndex": generation,
        "requiredPanelIds": [f"panel-{generation}"],
        "sourceSha256": "sha256:" + f"{generation:064x}",
        "variant0Outputs": {},
        "candidates": rows,
    }
    trace["traceSha256"] = _canonical_sha256(trace)
    _write_json(path, trace)


def _proposal(candidate_id: str, generation: int, *, admitted: bool) -> dict[str, object]:
    return {
        "candidateId": candidate_id,
        "generationIndex": generation,
        "evaluationStateKind": "proposal_current_panel",
        "evaluationStateSha256": f"sha256:state-{candidate_id}",
        "parentArchive": {"admitted": admitted},
        "origin": {"sourceMode": "qd_random_immigrant_bidirectional_pair"},
        "downstream": {"acceptedOffspringCandidateIds": []},
        "currentPanel": {"afterCostNetR": 1.0, "totalTrades": 12, "windows": []},
        "terminalReason": "prefinalizer_newcomer_cap",
        "focusLabels": [],
    }


def _retained(candidate_id: str, generation: int) -> dict[str, object]:
    return {
        "candidateId": candidate_id,
        "generationIndex": generation,
        "evaluationStateKind": "retained_parent_current_panel",
        "evaluationStateSha256": f"sha256:next-{candidate_id}-{generation}",
        "currentPanel": {"afterCostNetR": -1.0, "totalTrades": 12, "windows": []},
    }


def test_terminalization_report_regenerates_and_binds_corrected_forward_rows(tmp_path: Path) -> None:
    proposal_rows: list[dict[str, object]] = []
    phase_rows: list[dict[str, object]] = []
    selected: dict[int, list[str]] = {1: [], 2: []}
    for generation in range(1, 6):
        for ordinal in range(1024):
            candidate_id = f"g{generation}-{ordinal:04d}"
            admitted = generation in selected and ordinal < 3
            proposal_rows.append(_proposal(candidate_id, generation, admitted=admitted))
            if admitted:
                selected[generation].append(candidate_id)
            phase_rows.append(
                {
                    "candidateId": candidate_id,
                    "generationIndex": generation,
                    "totalTrades": ordinal + 1,
                    "medianHoldingBars": ordinal + 1,
                    "windowResamplePositiveRate": float(ordinal % 4) / 4.0,
                    "leaveOneWindowOutPositiveFraction": float(ordinal % 3) / 3.0,
                    "frequencyBand": "moderate_20_95",
                    "holdingBand": "short_25_96_bars",
                }
            )
    ledger_rows = proposal_rows + [
        *[_retained(candidate_id, 2) for candidate_id in selected[1]],
        *[_retained(candidate_id, 3) for candidate_id in selected[2]],
    ]
    ledger_path = tmp_path / "ledger.jsonl"
    phase_path = tmp_path / "phase.jsonl"
    trace_dir = tmp_path / "traces"
    _write_jsonl(ledger_path, ledger_rows)
    _write_jsonl(phase_path, phase_rows)
    _write_trace(trace_dir / "generation-0001-trace.json", 1, [_trace_row(cid, "quality") for cid in selected[1]])
    _write_trace(
        trace_dir / "generation-0002-trace.json",
        2,
        [_trace_row(cid, "quality") for cid in selected[2]]
        + [_trace_row(cid, "unsupported") for cid in selected[1]],
    )
    _write_trace(
        trace_dir / "generation-0003-trace.json",
        3,
        [_trace_row(cid, "unsupported") for cid in selected[2]],
    )
    _write_trace(trace_dir / "generation-0004-trace.json", 4, [_trace_row("g4-0000", "unsupported")])
    _write_trace(trace_dir / "generation-0005-trace.json", 5, [_trace_row("g5-0000", "unsupported")])

    first = tmp_path / "first"
    second = tmp_path / "second"
    report = write_v37_archive_terminalization_report(
        ledger_rows_path=ledger_path,
        trace_dir=trace_dir,
        phase1_evaluations_path=phase_path,
        output_dir=first,
    )
    write_v37_archive_terminalization_report(
        ledger_rows_path=ledger_path,
        trace_dir=trace_dir,
        phase1_evaluations_path=phase_path,
        output_dir=second,
    )

    assert report["forwardEvidence"]["availableRowCount"] == 6
    assert report["forwardEvidence"]["nextPanelNegativeFlipCount"] == 6
    assert report["variant4EvidenceStability"]["label"] == "evidence-stability diagnostic overlay"
    assert report["variant5BehavioralDescriptor"]["label"] == "behavioral-descriptor diagnostic overlay"
    assert (
        (first / "terminalization-report.json").read_bytes()
        == (second / "terminalization-report.json").read_bytes()
    )
