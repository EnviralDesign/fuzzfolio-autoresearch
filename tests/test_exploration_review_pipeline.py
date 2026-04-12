from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import trainingdatapipeline.build_exploration_judgment_dataset as judgment
from trainingdatapipeline.build_exploration_review_set import _candidate_row, _select_rows


@dataclass
class _FakeValidationResult:
    ok: bool
    normalized_response: dict | None
    errors: list[str]
    warnings: list[str]
    stateful_checks: dict[str, str]


def _target(tool: str, **fields: object) -> dict[str, object]:
    return {
        "reasoning": "short operational rationale",
        "actions": [{"tool": tool, **fields}],
    }


def _base_row(
    tmp_path: Path,
    *,
    example_id: str,
    run_id: str,
    step: int,
    target: dict[str, object],
    split_hint: str = "train",
) -> dict[str, object]:
    return {
        "example_id": example_id,
        "run_id": run_id,
        "step": step,
        "phase": "mid",
        "split_hint": split_hint,
        "prompt_state": {
            "run": {"run_id": run_id, "run_dir": str(tmp_path / run_id)},
            "controller": {"score_target": "first_scored_candidate"},
            "recent_step_window": [],
            "recent_attempts": [],
        },
        "prompt_state_compact_v2": {
            "run": {"run_id": run_id},
            "controller": {"step": step, "phase": "mid", "score_target": "first_scored_candidate"},
            "recent_steps": [],
            "recent_attempts": [],
            "handles": None,
        },
        "target_response_normalized": target,
        "quality_labels": {"keep_for_base_sft": True, "grade": "A"},
        "policy_labels": {"controller_admissible": True},
        "current_result_facts": [],
        "tool_results_summary": [],
        "trace_markers": {"step_guard_triggered": False},
    }


def test_candidate_row_marks_late_reseed_after_scored_attempt(tmp_path: Path) -> None:
    row = _base_row(
        tmp_path,
        example_id="ex-1",
        run_id="run-a",
        step=18,
        target=_target(
            "prepare_profile",
            mode="scaffold_from_seed",
            indicator_ids=["ADX_TREND"],
            instruments=["EURUSD"],
            candidate_name="cand-late",
        ),
    )
    row["prompt_state"]["recent_attempts"] = [
        {"candidate_name": "cand-0", "composite_score": 51.2}
    ]

    candidate = _candidate_row(row)

    assert candidate is not None
    assert candidate["decision_bucket"] == "reseed_scaffold"
    assert candidate["outcome_class"] == "strategically_weak_but_valid"
    assert "reseed_after_scored_attempt" in candidate["interesting_tags"]
    assert "late_reseed" in candidate["interesting_tags"]


def test_candidate_row_prioritizes_mechanical_fail_over_stale_loop(tmp_path: Path) -> None:
    row = _base_row(
        tmp_path,
        example_id="ex-mech",
        run_id="run-mech",
        step=40,
        target=_target(
            "mutate_profile",
            candidate_name="cand-b",
            mutations=["apply broken patch"],
        ),
    )
    row["prompt_state"]["recent_attempts"] = [{"candidate_name": "cand-a", "composite_score": 47.0}]
    row["trace_markers"] = {"step_guard_triggered": True}
    row["tool_results_summary"] = [
        {"tool": "mutate_profile", "error": "Failed to read JSON file: C:\\profiles\\cand-b.json"}
    ]

    candidate = _candidate_row(row)

    assert candidate is not None
    assert candidate["outcome_class"] == "mechanical_fail"


def test_apply_label_rejects_legacy_path_fields(tmp_path: Path) -> None:
    row = _base_row(
        tmp_path,
        example_id="ex-2",
        run_id="run-b",
        step=7,
        target=_target("mutate_profile", candidate_name="cand-1", mutations=["drop indicator X"]),
    )
    row["review_id"] = "REV-TEST"
    row["decision_bucket"] = "mutate_or_simplify"
    row["interest_score"] = 88

    label = {
        "review_id": "REV-TEST",
        "decision": "rewrite_action",
        "corrected_reasoning": "Mutate the current draft.",
        "corrected_action": {
            "tool": "mutate_profile",
            "profile_path": "C:\\profiles\\cand-2.json",
            "mutations": ["drop indicator Y"],
        },
    }

    try:
        judgment._apply_label(row, label)
    except ValueError as exc:
        assert "legacy path fields" in str(exc)
    else:
        raise AssertionError("Expected rewrite_action with legacy path fields to fail.")


def test_select_rows_guarantees_included_example_ids() -> None:
    rows = [
        {
            "example_id": "ex-high",
            "run_id": "run-a",
            "step": 20,
            "review_id": "REV-HIGH",
            "decision_bucket": "reseed_scaffold",
            "outcome_class": "strategically_weak_but_valid",
            "interest_score": 100,
        },
        {
            "example_id": "ex-focus",
            "run_id": "run-a",
            "step": 5,
            "review_id": "REV-FOCUS",
            "decision_bucket": "reseed_scaffold",
            "outcome_class": "ambiguous",
            "interest_score": 10,
        },
        {
            "example_id": "ex-other",
            "run_id": "run-b",
            "step": 9,
            "review_id": "REV-OTHER",
            "decision_bucket": "evaluate",
            "outcome_class": "productive_scored",
            "interest_score": 90,
        },
    ]

    selected = _select_rows(
        rows,
        quotas={"reseed_scaffold": 1, "evaluate": 1},
        max_per_run=1,
        include_example_ids=["ex-focus"],
    )

    selected_ids = [str(row.get("example_id")) for row in selected]
    assert "ex-focus" in selected_ids
    assert "ex-high" not in selected_ids
    assert "ex-other" in selected_ids


def test_build_exploration_judgment_dataset_emits_pathless_train_val_and_benchmark(
    tmp_path: Path, monkeypatch
) -> None:
    def _fake_validate(record: dict, candidate_payload: dict | list) -> _FakeValidationResult:
        if isinstance(candidate_payload, dict):
            normalized = candidate_payload
        else:
            normalized = {"reasoning": "", "actions": []}
        return _FakeValidationResult(
            ok=True,
            normalized_response=normalized,
            errors=[],
            warnings=[],
            stateful_checks={"action_shape": "checked"},
        )

    monkeypatch.setattr(judgment, "validate_candidate_response", _fake_validate)

    review_rows = [
        {
            **_base_row(
                tmp_path,
                example_id="manual-1",
                run_id="run-manual",
                step=9,
                target=_target(
                    "mutate_profile",
                    candidate_name="cand-a",
                    mutations=["drop weakest indicator"],
                ),
            ),
            "review_id": "REV-A",
            "decision_bucket": "mutate_or_simplify",
            "outcome_class": "strategically_weak_but_valid",
            "interest_score": 91,
        },
        {
            **_base_row(
                tmp_path,
                example_id="manual-2",
                run_id="run-manual",
                step=10,
                target=_target(
                    "evaluate_candidate",
                    candidate_name="cand-a",
                    instruments=["EURUSD"],
                ),
            ),
            "review_id": "REV-B",
            "decision_bucket": "evaluate",
            "outcome_class": "productive_unscored",
            "interest_score": 84,
        },
    ]
    labels = [
        {
            "review_id": "REV-A",
            "decision": "keep_gold",
            "notes": "keep",
            "corrected_reasoning": "",
            "corrected_action": None,
        },
        {
            "review_id": "REV-B",
            "decision": "rewrite_action",
            "notes": "rewrite",
            "corrected_reasoning": "Evaluate the best current draft on the known instrument set.",
            "corrected_action": {
                "tool": "evaluate_candidate",
                "candidate_name": "cand-a",
                "instruments": ["EURUSD"],
            },
        },
    ]
    train_source_rows = [
        _base_row(
            tmp_path,
            example_id="open-train",
            run_id="run-open",
            step=1,
            target=_target(
                "prepare_profile",
                mode="scaffold_from_seed",
                indicator_ids=["ADX_TREND"],
                instruments=["EURUSD"],
                candidate_name="cand-open",
            ),
        ),
        _base_row(
            tmp_path,
            example_id="val-train",
            run_id="run-follow",
            step=2,
            target=_target("validate_profile", candidate_name="cand-open"),
        ),
        _base_row(
            tmp_path,
            example_id="reg-train",
            run_id="run-follow",
            step=3,
            target=_target("register_profile", candidate_name="cand-open"),
        ),
        _base_row(
            tmp_path,
            example_id="mut-train",
            run_id="run-follow",
            step=4,
            target=_target(
                "mutate_profile",
                candidate_name="cand-open",
                mutations=["simplify to top 2 indicators"],
            ),
        ),
        _base_row(
            tmp_path,
            example_id="eval-train",
            run_id="run-follow",
            step=5,
            target=_target(
                "evaluate_candidate",
                candidate_name="cand-open",
                instruments=["EURUSD"],
            ),
        ),
    ]
    val_source_rows = [
        _base_row(
            tmp_path,
            example_id="open-val",
            run_id="run-open-val",
            step=1,
            target=_target(
                "prepare_profile",
                mode="scaffold_from_seed",
                indicator_ids=["ADX_TREND"],
                instruments=["GBPUSD"],
                candidate_name="cand-open-val",
            ),
            split_hint="val",
        ),
        _base_row(
            tmp_path,
            example_id="val-val",
            run_id="run-follow-val",
            step=2,
            target=_target("validate_profile", candidate_name="cand-open-val"),
            split_hint="val",
        ),
        _base_row(
            tmp_path,
            example_id="reg-val",
            run_id="run-follow-val",
            step=3,
            target=_target("register_profile", candidate_name="cand-open-val"),
            split_hint="val",
        ),
        _base_row(
            tmp_path,
            example_id="mut-val",
            run_id="run-follow-val",
            step=4,
            target=_target(
                "mutate_profile",
                candidate_name="cand-open-val",
                mutations=["drop weakest indicator"],
            ),
            split_hint="val",
        ),
        _base_row(
            tmp_path,
            example_id="eval-val",
            run_id="run-follow-val",
            step=5,
            target=_target(
                "evaluate_candidate",
                candidate_name="cand-open-val",
                instruments=["GBPUSD"],
            ),
            split_hint="val",
        ),
    ]

    review_path = tmp_path / "review.jsonl"
    labels_path = tmp_path / "labels.jsonl"
    train_source_path = tmp_path / "train_source.jsonl"
    val_source_path = tmp_path / "val_source.jsonl"
    train_out = tmp_path / "train_out.jsonl"
    val_out = tmp_path / "val_out.jsonl"
    benchmark_out = tmp_path / "benchmark_out.jsonl"
    train_chat = tmp_path / "train_chat.jsonl"
    val_chat = tmp_path / "val_chat.jsonl"
    benchmark_chat = tmp_path / "benchmark_chat.jsonl"
    manifest_out = tmp_path / "manifest.json"

    for path, rows in (
        (review_path, review_rows),
        (labels_path, labels),
        (train_source_path, train_source_rows),
        (val_source_path, val_source_rows),
    ):
        path.write_text(
            "".join(json.dumps(row, ensure_ascii=True) + "\n" for row in rows),
            encoding="utf-8",
        )

    manifest = judgment.build_exploration_judgment_dataset(
        review_path=review_path,
        labels_path=labels_path,
        train_source_path=train_source_path,
        val_source_path=val_source_path,
        train_out_path=train_out,
        val_out_path=val_out,
        benchmark_out_path=benchmark_out,
        train_chat_out_path=train_chat,
        val_chat_out_path=val_chat,
        benchmark_chat_out_path=benchmark_chat,
        manifest_path=manifest_out,
        manual_train_target=2,
        manual_benchmark_target=0,
        manual_val_target=0,
        opening_train_target=1,
        opening_val_target=1,
        followup_train_quotas={
            "validate_profile": 1,
            "register_profile": 1,
            "mutate_profile": 1,
            "evaluate_candidate": 1,
        },
        followup_val_quotas={
            "validate_profile": 1,
            "register_profile": 1,
            "mutate_profile": 1,
            "evaluate_candidate": 1,
        },
    )

    train_rows = [json.loads(line) for line in train_out.read_text(encoding="utf-8").splitlines()]
    val_rows = [json.loads(line) for line in val_out.read_text(encoding="utf-8").splitlines()]
    benchmark_rows = [json.loads(line) for line in benchmark_out.read_text(encoding="utf-8").splitlines() if line.strip()]

    assert manifest["output_counts"]["train_rows"] == len(train_rows)
    assert manifest["output_counts"]["val_rows"] == len(val_rows)
    assert manifest["output_counts"]["benchmark_rows"] == len(benchmark_rows)
    assert manifest["manual_split_counts"]["train_after_duplication"] == 2
    assert manifest["source_counts"]["manual_keep_gold"] == 1
    assert manifest["source_counts"]["manual_rewrite_action"] == 1
    assert manifest["source_counts"]["opening_anchor"] == 2
    assert manifest["source_counts"]["followup_anchor"] == 8
    assert train_chat.exists()
    assert val_chat.exists()
    assert benchmark_chat.exists()
    assert not any("profile_path" in row for row in train_out.read_text(encoding="utf-8").splitlines())
