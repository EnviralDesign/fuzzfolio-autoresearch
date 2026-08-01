from __future__ import annotations

from autoresearch.temporal_search_stage5e3 import (
    EXHAUSTED_WINDOWS,
    LEVEL_C_END,
    LEVEL_C_START,
    TIMEFRAMES,
    build_coverage_metadata,
    build_window_selection,
)


def _manifest() -> dict:
    rows = []
    for timeframe in TIMEFRAMES:
        rows.append(
            {
                "dataset": "bars",
                "pair": "EURUSD",
                "timeframe": timeframe,
                "available_from": "2020-07-15T00:00:00Z",
                "available_to": "2026-07-29T00:00:00Z",
                "promoted_through": "2026-07-29T00:00:00Z",
                "status": "promoted",
                "source": "darwinex_mt5",
                "calendar_contract_id": "fx_ny_close",
                "market_structure_hash": "a" * 64,
                "attested_intervals": [
                    {"start_utc": "2020-07-15T00:00:00Z", "end_utc": "2026-07-29T00:00:00Z"}
                ],
                "attested_bucket_intervals": [],
            }
        )
    return {
        "updated_at": "2026-07-31T10:21:01Z",
        "promoted_at": "2026-07-31T10:21:01Z",
        "coverage_sha256": "sha256:" + "b" * 64,
        "source_coverage_sha256": "sha256:" + "c" * 64,
        "coverage": rows,
    }


def _overlap(left: dict, right_start: str, right_end: str) -> bool:
    return left["analysisWindowStart"] < right_end and right_start < left["analysisWindowEnd"]


def test_stage5e3_window_selection_is_metadata_only_disjoint_and_exact() -> None:
    coverage = build_coverage_metadata(_manifest())
    first = build_window_selection(coverage)
    second = build_window_selection(coverage)
    assert first == second
    assert first["eligibleBlockCount"] == 11
    assert len(first["selectedWindows"]) == 4
    assert {row["label"] for row in first["selectedWindows"]} == {"E", "F", "G", "H"}
    assert {row["screeningStage"] for row in first["selectedWindows"]} == {
        "screening",
        "confirmation",
    }
    assert first["priceBarsRead"] is False
    for selected in first["selectedWindows"]:
        assert LEVEL_C_START <= selected["analysisWindowStart"]
        assert selected["analysisWindowEnd"] <= LEVEL_C_END
        assert all(
            not _overlap(selected, old_start, old_end)
            for _name, old_start, old_end in EXHAUSTED_WINDOWS
        )
    for index, left in enumerate(first["selectedWindows"]):
        assert all(
            not _overlap(left, right["analysisWindowStart"], right["analysisWindowEnd"])
            for right in first["selectedWindows"][index + 1 :]
        )


def test_selection_rank_does_not_depend_on_coverage_identity() -> None:
    first = build_window_selection(build_coverage_metadata(_manifest()))
    changed = _manifest()
    changed["coverage_sha256"] = "sha256:" + "d" * 64
    changed["source_coverage_sha256"] = "sha256:" + "e" * 64
    second = build_window_selection(build_coverage_metadata(changed))
    assert [row["windowId"] for row in first["selectedWindows"]] == [
        row["windowId"] for row in second["selectedWindows"]
    ]
    assert [row["selectionRankSha256"] for row in first["selectedWindows"]] == [
        row["selectionRankSha256"] for row in second["selectedWindows"]
    ]
