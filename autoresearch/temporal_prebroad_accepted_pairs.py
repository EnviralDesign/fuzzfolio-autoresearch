"""Build the finite, native-validated input for pre-broad admission control.

This is deliberately an artifact adapter, not a search runner.  It combines a
frozen QD population with an already-attested broad evidence preparation, and
only rotates profile-bound plan identities.  Lake requests and their remote
attestation identities are copied byte-for-byte from the trusted preparation.
"""
from __future__ import annotations

import argparse
from copy import deepcopy
import json
from pathlib import Path
from typing import Any, Mapping

from .lake_window import LakeWindowBinding, lake_window_request_contains, resolve_replay_lake_window_request
from .temporal_prebroad_control import (
    ACCEPTED_PAIRS_SCHEMA,
    DEFAULT_DASHBOARD_PYTHON,
    WINDOWS,
    _dashboard_native_reports,
)
from .temporal_search import TemporalSearchContractError, canonical_sha256


WORKER_CONTRACT_SHA256 = "sha256:6c47c1d5b94a65af16e6bb4a7c7f516b33fbac8145154e8b302d31c47830f2e0"
_WORKER_SCHEMA = "replay-worker-contract-v1"


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TemporalSearchContractError(f"could not read JSON file: {path}") from exc
    if not isinstance(value, dict):
        raise TemporalSearchContractError(f"JSON root must be an object: {path}")
    return value


def _immutable_write(path: Path, value: Mapping[str, Any]) -> None:
    encoded = json.dumps(dict(value), indent=2, sort_keys=True, ensure_ascii=True, allow_nan=False) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.read_text(encoding="utf-8") != encoded:
        raise TemporalSearchContractError(f"refusing to overwrite divergent immutable file: {path}")
    path.write_text(encoded, encoding="utf-8")


def _sha(value: Any, *, name: str) -> str:
    token = str(value or "")
    if len(token) != 71 or not token.startswith("sha256:") or any(char not in "0123456789abcdef" for char in token[7:]):
        raise TemporalSearchContractError(f"{name} must be a lower-case sha256 identity")
    return token


def _templates(preparation: Mapping[str, Any]) -> tuple[dict[str, Any], dict[str, dict[str, Any]]]:
    worker = preparation.get("workerContract")
    if not isinstance(worker, Mapping) or dict(worker) != {
        "workerContractSha256": WORKER_CONTRACT_SHA256,
        "workerContractSchema": _WORKER_SCHEMA,
    }:
        raise TemporalSearchContractError("trusted preparation does not bind the required worker contract")
    windows = preparation.get("developmentWindows")
    if not isinstance(windows, list) or [
        (item.get("windowId"), item.get("analysisWindowStart"), item.get("analysisWindowEnd"))
        for item in windows if isinstance(item, Mapping)
    ] != list(WINDOWS):
        raise TemporalSearchContractError("trusted preparation does not bind the fixed development windows")
    candidates = preparation.get("candidates")
    if not isinstance(candidates, list) or not candidates:
        raise TemporalSearchContractError("trusted preparation has no evidence-plan templates")
    first = candidates[0]
    if not isinstance(first, Mapping) or not isinstance(first.get("windowInputs"), list):
        raise TemporalSearchContractError("trusted preparation candidate has no window inputs")
    by_window = {str(row.get("windowId") or ""): row.get("evidencePlan") for row in first["windowInputs"] if isinstance(row, Mapping)}
    if set(by_window) != {row[0] for row in WINDOWS} or not all(isinstance(item, Mapping) for item in by_window.values()):
        raise TemporalSearchContractError("trusted preparation must provide both evidence-plan templates")
    # The source preparation is a broad envelope.  Every listed candidate must
    # reference exactly the same attested binding for each fixed window.
    for candidate in candidates:
        if not isinstance(candidate, Mapping) or not isinstance(candidate.get("windowInputs"), list):
            raise TemporalSearchContractError("trusted preparation candidate window inputs are malformed")
        inputs = {str(row.get("windowId") or ""): row.get("evidencePlan") for row in candidate["windowInputs"] if isinstance(row, Mapping)}
        if set(inputs) != set(by_window):
            raise TemporalSearchContractError("trusted preparation candidate windows drifted")
        for window_id, template in by_window.items():
            current = inputs[window_id]
            if not isinstance(current, Mapping) or current.get("lake_window_binding") != template.get("lake_window_binding"):
                raise TemporalSearchContractError("trusted preparation attested lake binding drifted")
    return dict(worker), {key: deepcopy(dict(value)) for key, value in by_window.items()}


def _rotate_plan(template: Mapping[str, Any], *, profile: Mapping[str, Any], profile_sha: str, timeframe: str, window: tuple[str, str, str]) -> dict[str, Any]:
    window_id, start, end = window
    plan = deepcopy(dict(template))
    if plan.get("schema_version") != "fuzzfolio.replay-evidence-plan.v2" or plan.get("analysis_window_start") != start or plan.get("analysis_window_end") != end:
        raise TemporalSearchContractError(f"trusted {window_id} evidence plan is not the fixed month")
    if plan.get("requested_horizon_months") != 1 or plan.get("selection_data_end") != end:
        raise TemporalSearchContractError(f"trusted {window_id} evidence plan is not exactly one month")
    if plan.get("execution_cell_sha256") is not None:
        raise TemporalSearchContractError(f"trusted {window_id} evidence plan does not use library management")
    binding = plan.get("lake_window_binding")
    try:
        frozen_binding = LakeWindowBinding.model_validate(binding)
        required = resolve_replay_lake_window_request(
            pairs=[str(item) for item in profile.get("instruments", [])],
            base_timeframe=timeframe,
            profile_snapshot=dict(profile),
            analysis_window_start=start,
            analysis_window_end=end,
        )
    except (TypeError, ValueError) as exc:
        raise TemporalSearchContractError(f"candidate evidence plan has malformed lake scope: {window_id}") from exc
    if not lake_window_request_contains(frozen_binding.request, required):
        raise TemporalSearchContractError(f"candidate-derived lake scope is outside the immutable pre-attested evidence binding: {window_id}")
    # Do not reserialize this value: the upstream request/hash attestation is
    # the authority.  Only local profile-bound plan identity rotates.
    plan["profile_snapshot_sha256"] = profile_sha
    plan["execution_cell_sha256"] = None
    plan.pop("plan_id", None)
    identity = dict(plan)
    identity.pop("lake_manifest_sha256", None)
    plan["plan_id"] = canonical_sha256(identity)
    return plan


def build_accepted_pairs(population: Mapping[str, Any], preparation: Mapping[str, Any], *, native_reports: Mapping[str, Mapping[str, Any]] | None = None, dashboard_python: Path = DEFAULT_DASHBOARD_PYTHON) -> dict[str, Any]:
    worker, templates = _templates(preparation)
    candidates = population.get("candidates")
    if not isinstance(candidates, list) or len(candidates) != 8:
        raise TemporalSearchContractError("QD population must contain exactly eight candidates")
    pairs: list[dict[str, Any]] = []
    for raw in candidates:
        if not isinstance(raw, Mapping):
            raise TemporalSearchContractError("QD population candidate is malformed")
        candidate_id = str(raw.get("candidateId") or "").strip().lower()
        profile = raw.get("sourceProfile")
        profile_sha = _sha(raw.get("sourceProfileSha256"), name=f"{candidate_id} sourceProfileSha256")
        if not candidate_id.startswith("qd_") or not isinstance(profile, Mapping) or canonical_sha256(dict(profile)) != profile_sha:
            raise TemporalSearchContractError("QD population must contain canonical qd_* source profiles")
        if profile.get("version") != "v3" or profile.get("directionMode") != "both" or profile.get("instruments") != ["EURUSD"]:
            raise TemporalSearchContractError(f"{candidate_id} is not a v3/both EURUSD candidate")
        pairs.append({
            "candidateId": candidate_id,
            "profile": deepcopy(dict(profile)),
            "profileSha256": profile_sha,
            "validation": {},
            "timeframe": "M5",
            "barLimit": 5000,
            "windowInputs": [
                {"windowId": window[0], "evidencePlan": _rotate_plan(templates[window[0]], profile=profile, profile_sha=profile_sha, timeframe="M5", window=window)}
                for window in WINDOWS
            ],
        })
    if len({pair["candidateId"] for pair in pairs}) != 8:
        raise TemporalSearchContractError("QD population candidates must have distinct identities")
    pairs.sort(key=lambda pair: pair["candidateId"])
    reports = native_reports if native_reports is not None else _dashboard_native_reports(pairs, dashboard_python)
    for pair in pairs:
        report = reports.get(pair["candidateId"])
        if not isinstance(report, Mapping) or report.get("candidateId") != pair["candidateId"] or report.get("candidateAcceptable") is not True or report.get("status") != "valid_evaluable":
            raise TemporalSearchContractError(f"{pair['candidateId']} Dashboard native validation was omitted or rejected")
        if report.get("rawSourceProfileSha256") != pair["profileSha256"]:
            raise TemporalSearchContractError(f"{pair['candidateId']} Dashboard native source profile identity drifted")
        pair["validation"] = deepcopy(dict(report))
    return {"schemaVersion": ACCEPTED_PAIRS_SCHEMA, "workerContract": worker, "pairs": pairs}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Build the immutable eight-pair pre-broad admission artifact.")
    parser.add_argument("--population", type=Path, required=True)
    parser.add_argument("--preparation", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--dashboard-python", type=Path, default=DEFAULT_DASHBOARD_PYTHON)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        accepted = build_accepted_pairs(_read(args.population), _read(args.preparation), dashboard_python=args.dashboard_python)
        _immutable_write(args.output, accepted)
        print(json.dumps({"schemaVersion": "temporal_prebroad_accepted_pairs_result_v1", "candidateCount": 8, "taskCount": 16, "acceptedPairsSha256": canonical_sha256(accepted), "taskDispatchPermitted": False, "marketEvidenceRead": False, "gatewayContacted": False}, indent=2, sort_keys=True))
        return 0
    except Exception as exc:
        print(json.dumps({"schemaVersion": "temporal_prebroad_accepted_pairs_error_v1", "errorType": type(exc).__name__, "message": str(exc)}, indent=2, sort_keys=True), file=__import__("sys").stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
