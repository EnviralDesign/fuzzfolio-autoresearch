from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path

import pytest


SCRIPT = Path(__file__).parents[1] / "scripts" / "phase3_survivor_holdout.py"
SPEC = importlib.util.spec_from_file_location("phase3_survivor_holdout", SCRIPT)
assert SPEC and SPEC.loader
holdout = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(holdout)


def _sha(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _dump(path: Path, payload: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")


def _record(*, lane: int, cohort: str = "promoted", recipe: str = "R", indicators: tuple[str, ...] = ("A",), instrument: str = "EURUSD", trades: int = 100, validation: float = 50) -> dict:
    profile = {"instruments": [instrument], "notificationThreshold": 80, "indicators": []}
    return {
        "cohort": cohort, "lane_index": lane, "profile_sha256": f"sha256:{lane:064x}",
        "profile_snapshot": profile, "profile_path": "p.json", "artifact_dir": "source",
        "profile_file_sha256": "sha256:" + "b" * 64,
        "receipt_sha256": "sha256:" + "c" * 64,
        "job_sha256": "sha256:" + "d" * 64,
        "sensitivity_sha256": "sha256:" + "e" * 64,
        "cell": {"reward_multiple": 2.0, "stop_loss_percent": .2}, "cell_sha256": holdout.build_execution_cell_sha256({"reward_multiple": 2.0, "stop_loss_percent": .2}),
        "cell_source": "robust_cell", "resolved_trades": trades, "timeframe": "M5", "instruments": [instrument],
        "recipe_id": recipe, "policy_family": recipe, "indicator_ids": indicators,
        "original_instrument": instrument, "canonical_pair_family": "family", "validation_12mo_score": validation,
    }


def _source_lane(tmp_path: Path, *, robust: bool = True) -> tuple[dict, Path]:
    run = tmp_path / "run"; artifact = run / "evals" / "eval_lab_final_36mo_a"; profile = run / "profiles" / "p.json"
    _dump(profile, {"profile": {"instruments": ["EURUSD"], "notificationThreshold": 80, "indicators": []}})
    _dump(artifact / "deep-replay-job.json", {"request": {"timeframe": "M5", "instruments": ["EURUSD"]}})
    cell = {"reward_multiple": 2, "stop_loss_percent": .2, "resolved_trades": 20}
    aggregate = {"matrix_summary": {"robust_cell": cell} if robust else {}, "recommended_cell": cell}
    _dump(artifact / "sensitivity-response.json", {"data": {"aggregate": aggregate}})
    files = {name: _sha(artifact / name) for name in ("deep-replay-job.json", "sensitivity-response.json")}
    receipt = {"schema_version": "play-hand-lab-task-result-receipt-v2", "task_id": "t", "worker_result_sha256": "sha256:" + "a" * 64, "artifact_receipt": {"root": str(artifact.resolve()), "files": files}, "recorded_result": {"attempt_id": "a-final", "phase": "final_36mo", "artifact_dir": str(artifact.resolve()), "profile_path": str(profile.resolve())}}
    receipt["receipt_sha256"] = holdout.canonical_sha256(receipt)
    _dump(artifact / "task-result-receipt.json", receipt)
    lane = {"lane_index": 2, "lane_id": "lane_2", "run_dir": str(run), "final_attempt_id": "a-final", "phase_scores": {"validation_12mo": 42, "final_36mo": 40}, "policy_assignment": {"candidate_attributes": {"recipe_id": {"kind": "value", "value": "R"}, "instrument": {"kind": "value", "value": "EURUSD"}, "indicator_ids": {"values": [{"kind": "value", "value": "A"}]}}}}
    return lane, artifact


def test_dedup_uses_lowest_lane_and_matching_is_deterministic() -> None:
    left = _record(lane=9); duplicate = _record(lane=2); duplicate["profile_sha256"] = left["profile_sha256"]
    assert holdout._dedupe([left, duplicate])[0]["lane_index"] == 2
    promoted = [_record(lane=1, indicators=("A", "B")), _record(lane=2, recipe="X", indicators=("B",))]
    controls = [_record(lane=8, cohort="control", indicators=("A", "B"), validation=51), _record(lane=4, cohort="control", recipe="X", indicators=("A",), validation=50)]
    pairs, unmatched = holdout.match_pairs(promoted, controls)
    assert [item["match_tier"] for item in pairs] == ["exact_recipe_and_indicators", "same_recipe_indicator_overlap"]
    assert not unmatched
    _, unmatched = holdout.match_pairs(promoted, [])
    assert len(unmatched) == 2 and unmatched[0]["reason"] == "no_unused_control"


def test_resolver_freezes_robust_cell_and_falls_back_to_immediate_eval(tmp_path: Path) -> None:
    lane, artifact = _source_lane(tmp_path)
    resolved = holdout._resolve_lane(lane, "promoted")
    assert resolved["artifact_dir"] == str(artifact.resolve())
    assert resolved["cell_source"] == "robust_cell"
    assert resolved["cell"] == {"reward_multiple": 2.0, "stop_loss_percent": .2}
    fallback_lane, _ = _source_lane(tmp_path / "fallback", robust=False)
    assert holdout._resolve_lane(fallback_lane, "control")["cell_source"] == "recommended_cell"


def test_reserved_tail_and_receipt_drift_fail_closed(tmp_path: Path) -> None:
    with pytest.raises(holdout.HoldoutError, match="reserved tail"):
        holdout._valid_window("2026-01-14T00:00:00Z", "2026-07-14T00:00:00Z")
    lane, artifact = _source_lane(tmp_path)
    (artifact / "deep-replay-job.json").write_text("{}", encoding="utf-8")
    with pytest.raises(holdout.HoldoutError, match="hash drift"):
        holdout._resolve_lane(lane, "promoted")


def test_output_must_be_disjoint_from_source_evidence(tmp_path: Path) -> None:
    source = tmp_path / "source"
    source.mkdir()
    with pytest.raises(holdout.HoldoutError, match="overlaps protected"):
        holdout._require_disjoint_output(source / "derived", [source])


def test_zero_trade_result_is_zero_r_without_path_metrics(tmp_path: Path) -> None:
    result = tmp_path / "result.json"
    curve = tmp_path / "curve.json"
    cell = {"stop_loss_percent": 0.2, "reward_multiple": 2.0}
    _dump(
        result,
        {
            "data": {
                "aggregate": {
                    "tracked_cell_result": {**cell, "resolved_trades": 0}
                }
            }
        },
    )
    _dump(curve, {"cell": cell, "path_metrics": None})
    observed = holdout._tracked_result(result, curve, cell)
    assert observed["no_signal"] is True
    assert observed["net_r"] == 0.0


def test_task_id_and_verdict_are_deterministic() -> None:
    assert holdout.deterministic_task_id("sha256:" + "a" * 64, "pair", "promoted", "h1") == holdout.deterministic_task_id("sha256:" + "a" * 64, "pair", "promoted", "h1")
    records = []
    for cohort, profile, aggregate in (("promoted", "p1", 4), ("promoted", "p2", 3), ("promoted", "p3", 2), ("promoted", "p4", 1), ("promoted", "p5", 1), ("control", "c1", -1), ("control", "c2", -1), ("control", "c3", -1), ("control", "c4", -1), ("control", "c5", -1)):
        pair = "pair" + profile[-1]
        for window, value in (("h1", 1), ("h2", 1), ("h3", -1), ("aggregate", aggregate)):
            records.append({"pair_id": pair, "cohort": cohort, "profile_sha256": profile, "window": window, "result": {"net_r": value}})
    verdict = holdout.calculate_verdict(records)
    assert verdict["verdict"] == "supports_later_tail_test"
    assert verdict["components"]["promoted_pair_win_rate"] == 1.0
    records = [row for row in records if not (row["cohort"] == "control" and row["profile_sha256"] == "c5")]
    assert holdout.calculate_verdict(records)["verdict"] == "no_support_for_tail"


def test_cross_cohort_profile_overlap_does_not_steal_pair_identity() -> None:
    promoted = _record(lane=1)
    overlapping_control = _record(lane=2, cohort="control", recipe="X", indicators=("B",))
    overlapping_control["profile_sha256"] = promoted["profile_sha256"]
    matched_control = _record(lane=3, cohort="control", indicators=("A",))
    pairs, _ = holdout.match_pairs([promoted], [matched_control, overlapping_control])
    pair_by_profile = {
        (role, pair[role]["profile_sha256"]): pair["pair_id"]
        for pair in pairs
        for role in ("promoted", "control")
    }
    assert pair_by_profile[("promoted", promoted["profile_sha256"])] == pairs[0]["pair_id"]
    assert pairs[0]["control"]["profile_sha256"] == matched_control["profile_sha256"]
    assert ("control", overlapping_control["profile_sha256"]) not in pair_by_profile


def test_plan_identity_is_stable_and_missing_chain_is_terminal(tmp_path: Path) -> None:
    lane, _ = _source_lane(tmp_path / "source")
    lane.update({"current_phase": "promoted", "run_promoted": True})
    authority = {"authority_id": "authority", "source_identities": {"lake_manifest_sha256": "sha256:" + "a" * 64}, "bound_contract": {"worker_contract_sha256": "sha256:" + "b" * 64, "worker_contract_id": "replay-worker-contract-v1", "operator_launch_worker_image": "image"}}
    campaign_path = tmp_path / "state.json"; authority_path = tmp_path / "authority.json"
    _dump(campaign_path, {"lanes": [lane, {"lane_index": 99, "lane_id": "gone", "current_phase": "promoted", "final_attempt_id": "gone", "run_dir": str(tmp_path / "gone")} ]}); _dump(authority_path, authority)
    first = holdout.build_plan(campaign_path=campaign_path, authority_path=authority_path, experiment_root=tmp_path / "output")
    second = holdout.build_plan(campaign_path=campaign_path, authority_path=authority_path, experiment_root=tmp_path / "output")
    assert first["plan_id"] == second["plan_id"]
    assert first["unresolved_sources"][0]["terminal"] == "unresolved_source"


def test_run_canary_isolated_and_uses_prebuilt_v2_outer_tasks(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source_file = tmp_path / "frozen-source.json"; source_file.write_text("immutable", encoding="utf-8"); source_hash = _sha(source_file)
    promoted = _record(lane=1); control = _record(lane=2, cohort="control")
    for source in (promoted, control): source["artifact_dir"] = str(source_file.parent); source["profile_path"] = str(source_file)
    pair = {"pair_id": "pair", "promoted": promoted, "control": control, "match_tier": "exact_recipe_and_indicators"}
    requests = []
    for source in (promoted, control):
        for name, start, end, months in holdout.WINDOWS:
            request = holdout.resolve_replay_lake_window_request(pairs=source["instruments"], base_timeframe="M5", profile_snapshot=source["profile_snapshot"], analysis_window_start=start, analysis_window_end=end)
            requests.append({"profile_sha256": source["profile_sha256"], "cohort": source["cohort"], "window": name, "request": request.canonical_payload(), "requested_horizon_months": months})
    plan = {"plan_id": "sha256:" + "b" * 64, "pairs": [pair], "promoted": [promoted], "controls": [control], "lake_window_requests": requests, "unresolved_sources": []}
    authority = {"authority_id": "a", "source_identities": {"lake_manifest_sha256": "sha256:" + "c" * 64}, "bound_contract": {"worker_contract_sha256": "sha256:" + "d" * 64, "worker_contract_id": "replay-worker-contract-v1", "operator_launch_worker_image": "image"}}
    authority_path = tmp_path / "authority.json"; campaign_path = tmp_path / "campaign.json"; _dump(authority_path, authority); _dump(campaign_path, {"lanes": []})
    seen = []
    monkeypatch.setattr(holdout, "_revalidate_plan", lambda *args, **kwargs: None)
    monkeypatch.setattr(holdout, "load_config", lambda **kwargs: object())
    monkeypatch.setattr(holdout, "resolve_lab_backtest_config", lambda **kwargs: type("Lab", (), {"worker_contract_hash": authority["bound_contract"]["worker_contract_sha256"], "worker_contract_schema": "replay-worker-contract-v1"})())
    from autoresearch.lake_window import LakeWindowBinding
    monkeypatch.setattr(holdout, "resolve_lake_window_binding", lambda request, **kwargs: LakeWindowBinding(request=request, window_semantic_sha256="sha256:" + "e" * 64, attestation_sha256="sha256:" + "f" * 64, legacy_selection_manifest_sha256=authority["source_identities"]["lake_manifest_sha256"]))
    def fake_task(**kwargs):
        assert Path(kwargs["attempt"]["artifact_dir"]).is_dir()
        return {"task_id": kwargs["task_id"], "payload": {"evidence_plan": kwargs["evidence_plan"].model_dump(mode="json"), "tracked_cell": kwargs["tracked_cell"]}}
    monkeypatch.setattr(holdout, "build_full_backtest_lab_task", fake_task)
    def fake_run(**kwargs):
        seen.append(kwargs)
        results = []
        for _, attempt, _, _ in kwargs["items"]:
            output = Path(attempt["artifact_dir"]) / "result.json"; _dump(output, {"data": {"aggregate": {"tracked_cell_result": {"stop_loss_percent": .2, "reward_multiple": 2.0, "resolved_trades": 2}}}})
            curve = Path(attempt["artifact_dir"]) / "curve.json"
            _dump(curve, {"cell": {"stop_loss_percent": .2, "reward_multiple": 2.0}, "path_metrics": {"final_equity_r": 1.0, "max_drawdown_r": 0.2}})
            results.append({"attempt_id": attempt["attempt_id"], "status": "calculated", "result_path": str(output), "curve_path": str(curve)})
        return results, len(results), 0
    monkeypatch.setattr(holdout, "run_lab_full_backtests", fake_run)
    report = holdout.run_plan(plan=plan, campaign_path=campaign_path, authority_path=authority_path, experiment_root=tmp_path / "out", gateway_url=None, gateway_token=None, max_workers=2, canary_pairs=1)
    assert "canary-1" in report["execution_root"] and len(seen) == 4
    assert all(task["payload"]["evidence_plan"]["schema_version"].endswith("v2") and task["payload"]["evidence_plan"]["evidence_role"] == "outer_test" for call in seen for task in call["prebuilt_tasks_by_attempt_id"].values())
    assert all(task["payload"]["tracked_cell"] == {"reward_multiple": 2.0, "stop_loss_percent": .2} for call in seen for task in call["prebuilt_tasks_by_attempt_id"].values())
    assert _sha(source_file) == source_hash
