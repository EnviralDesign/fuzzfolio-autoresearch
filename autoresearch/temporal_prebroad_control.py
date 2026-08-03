"""Closed, no-dispatch authority controls for the pre-broad temporal gate.

This module deliberately only freezes, audits, and materializes the finite
pre-broad matrix.  It contains no Gateway client and cannot start a worker.
The later dispatch owner must consume the immutable manifest and its authority
ID; this boundary keeps an admission-control click from becoming a search.
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import subprocess
import sys
import tempfile
from typing import Any, Callable, Mapping

from .lake_window import LakeWindowBinding, lake_window_request_contains
from .temporal_search import TemporalSearchContractError, canonical_sha256


SCHEMA = "temporal_prebroad_authority_v1"
PREPARATION_SCHEMA = "temporal_prebroad_preparation_v1"
MANIFEST_SCHEMA = "temporal_prebroad_task_manifest_v1"
ACCEPTED_PAIRS_SCHEMA = "temporal_prebroad_accepted_pairs_v1"
WINDOWS = (
    ("window_e_2023_10", "2023-10-01T00:00:00Z", "2023-11-01T00:00:00Z"),
    ("window_f_2021_07", "2021-07-01T00:00:00Z", "2021-08-01T00:00:00Z"),
)
PROHIBITED_START = "2024-06-29T00:00:00Z"
PROHIBITED_END = "2100-01-01T00:00:00Z"
DEFAULT_DASHBOARD_PYTHON = Path("C:/repos/Trading-Dashboard/compute-service/.venv/Scripts/python.exe")


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TemporalSearchContractError(f"could not read JSON file: {path}") from exc
    if not isinstance(value, dict):
        raise TemporalSearchContractError(f"JSON root must be an object: {path}")
    return value


def _write_immutable(path: Path, value: Mapping[str, Any] | str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = value if isinstance(value, str) else json.dumps(
        dict(value), indent=2, sort_keys=True, ensure_ascii=True, allow_nan=False
    ) + "\n"
    if path.exists() and path.read_text(encoding="utf-8") != encoded:
        raise TemporalSearchContractError(f"refusing to overwrite divergent immutable file: {path}")
    path.write_text(encoded, encoding="utf-8")


def _sha(value: Any, *, name: str) -> str:
    token = str(value or "")
    if len(token) != 71 or not token.startswith("sha256:") or any(c not in "0123456789abcdef" for c in token[7:]):
        raise TemporalSearchContractError(f"{name} must be a lower-case sha256 identity")
    return token


def _pair_rows(accepted: Mapping[str, Any]) -> list[dict[str, Any]]:
    if set(accepted) != {"schemaVersion", "workerContract", "pairs"}:
        raise TemporalSearchContractError("accepted pair artifact has a closed schema")
    if accepted.get("schemaVersion") != ACCEPTED_PAIRS_SCHEMA:
        raise TemporalSearchContractError("unknown accepted pair artifact schema")
    worker = accepted.get("workerContract")
    if not isinstance(worker, Mapping) or set(worker) != {"workerContractSha256", "workerContractSchema"}:
        raise TemporalSearchContractError("accepted pair artifact requires one closed worker contract")
    _sha(worker.get("workerContractSha256"), name="workerContractSha256")
    if not str(worker.get("workerContractSchema") or "").strip():
        raise TemporalSearchContractError("workerContractSchema is required")
    pairs = accepted.get("pairs")
    if not isinstance(pairs, list) or len(pairs) != 8:
        raise TemporalSearchContractError("pre-broad admission requires exactly eight accepted pairs")
    output: list[dict[str, Any]] = []
    for index, raw in enumerate(pairs):
        if not isinstance(raw, Mapping) or set(raw) != {"candidateId", "profile", "profileSha256", "validation", "timeframe", "barLimit", "windowInputs"}:
            raise TemporalSearchContractError(f"accepted pair {index} has a closed schema")
        candidate_id = str(raw["candidateId"] or "").strip().lower().replace("-", "_")
        profile = raw["profile"]
        validation = raw["validation"]
        if not candidate_id or not isinstance(profile, Mapping) or not isinstance(validation, Mapping):
            raise TemporalSearchContractError(f"accepted pair {index} is incomplete")
        profile_copy = dict(profile)
        profile_sha = _sha(raw["profileSha256"], name=f"pair {candidate_id} profileSha256")
        if canonical_sha256(profile_copy) != profile_sha:
            raise TemporalSearchContractError(f"pair {candidate_id} profile identity mismatch")
        if profile_copy.get("version") != "v3" or profile_copy.get("directionMode") != "both" or profile_copy.get("instruments") != ["EURUSD"]:
            raise TemporalSearchContractError(f"pair {candidate_id} must be an accepted v3/both EURUSD profile")
        if validation.get("candidateId") != candidate_id or validation.get("candidateAcceptable") is not True or validation.get("status") != "valid_evaluable":
            raise TemporalSearchContractError(f"pair {candidate_id} lacks accepted native validation")
        _sha(validation.get("programSha256"), name=f"pair {candidate_id} validation programSha256")
        _sha(validation.get("validationReportSha256"), name=f"pair {candidate_id} validationReportSha256")
        try:
            bar_limit = int(raw["barLimit"])
        except (TypeError, ValueError) as exc:
            raise TemporalSearchContractError(f"pair {candidate_id} barLimit is invalid") from exc
        if not 10 <= bar_limit <= 100_000 or str(raw["timeframe"] or "").upper() == "":
            raise TemporalSearchContractError(f"pair {candidate_id} has invalid finite task bounds")
        inputs = raw["windowInputs"]
        if not isinstance(inputs, list) or len(inputs) != 2:
            raise TemporalSearchContractError(f"pair {candidate_id} must bind both fixed development windows")
        by_window = {str(item.get("windowId") or ""): item for item in inputs if isinstance(item, Mapping)}
        if len(by_window) != 2 or set(by_window) != {row[0] for row in WINDOWS}:
            raise TemporalSearchContractError(f"pair {candidate_id} does not exactly bind the two development windows")
        normal_inputs = []
        for window_id, start, end in WINDOWS:
            item = by_window[window_id]
            if set(item) != {"windowId", "evidencePlan"} or not isinstance(item["evidencePlan"], Mapping):
                raise TemporalSearchContractError(f"pair {candidate_id}/{window_id} has an invalid evidence binding")
            plan = dict(item["evidencePlan"])
            allowed_plan = {"schema_version", "plan_id", "profile_snapshot_sha256", "analysis_window_start", "analysis_window_end", "execution_cell_sha256", "lake_window_binding", "campaign_plan_id", "coverage_policy", "data_availability_cutoff", "evidence_role", "lake_manifest_sha256", "requested_horizon_months", "selection_data_end"}
            if set(plan) != allowed_plan or plan.get("schema_version") != "fuzzfolio.replay-evidence-plan.v2" or plan.get("analysis_window_start") != start or plan.get("analysis_window_end") != end:
                raise TemporalSearchContractError(f"pair {candidate_id}/{window_id} evidence plan is not the fixed window")
            if plan.get("requested_horizon_months") != 1 or plan.get("selection_data_end") != end:
                raise TemporalSearchContractError(f"pair {candidate_id}/{window_id} evidence plan is not exactly one month")
            if plan.get("profile_snapshot_sha256") != profile_sha or plan.get("execution_cell_sha256") is not None:
                raise TemporalSearchContractError(f"pair {candidate_id}/{window_id} evidence plan does not bind the v3 management profile")
            binding = plan.get("lake_window_binding")
            if not isinstance(binding, Mapping) or set(binding) != {"schema_version", "semantic_contract_id", "request", "window_semantic_sha256", "attestation_sha256", "creation_global_coverage_sha256", "creation_source_coverage_sha256", "legacy_selection_manifest_sha256"}:
                raise TemporalSearchContractError(f"pair {candidate_id}/{window_id} evidence plan lake binding has a closed schema")
            request = binding.get("request")
            if not isinstance(request, Mapping) or set(request) != {"schema_version", "dataset", "pairs", "timeframes", "data_start", "data_end", "coverage_policy"}:
                raise TemporalSearchContractError(f"pair {candidate_id}/{window_id} evidence plan lake binding is not exact")
            try:
                frozen_binding = LakeWindowBinding.model_validate(binding)
                required_request = {
                    "schema_version": "fuzzfolio.market-data-window-request.v1",
                    "dataset": "bars",
                    "pairs": ["EURUSD"],
                    "timeframes": [str(raw["timeframe"]).upper()],
                    "data_start": start,
                    "data_end": end,
                    "coverage_policy": "require_complete",
                }
            except (TypeError, ValueError) as exc:
                raise TemporalSearchContractError(f"pair {candidate_id}/{window_id} evidence plan lake binding is malformed") from exc
            if not lake_window_request_contains(frozen_binding.request, required_request):
                raise TemporalSearchContractError(f"pair {candidate_id}/{window_id} lake request does not contain the fixed task dependency")
            for key in ("window_semantic_sha256", "attestation_sha256", "creation_global_coverage_sha256", "creation_source_coverage_sha256"):
                _sha(binding.get(key), name=f"pair {candidate_id}/{window_id} lake {key}")
            identity = dict(plan); supplied = identity.pop("plan_id", None); identity.pop("lake_manifest_sha256", None)
            if _sha(supplied, name=f"pair {candidate_id}/{window_id} planId") != canonical_sha256(identity):
                raise TemporalSearchContractError(f"pair {candidate_id}/{window_id} evidence plan identity mismatch")
            normal_inputs.append({"windowId": window_id, "evidencePlan": plan})
        output.append({"candidateId": candidate_id, "profile": profile_copy, "profileSha256": profile_sha, "validation": dict(validation), "timeframe": str(raw["timeframe"]).upper(), "barLimit": bar_limit, "windowInputs": normal_inputs})
    if len({row["candidateId"] for row in output}) != 8:
        raise TemporalSearchContractError("pre-broad candidates must have eight distinct identities")
    return sorted(output, key=lambda row: row["candidateId"])


def _dashboard_native_reports(pairs: list[dict[str, Any]], dashboard_python: Path) -> dict[str, dict[str, Any]]:
    if not dashboard_python.is_file():
        raise TemporalSearchContractError(f"Dashboard compute Python is unavailable: {dashboard_python}")
    payload = [{"candidateId": row["candidateId"], "profile": row["profile"]} for row in pairs]
    code = """import json,sys
from fuzzfolio_core.temporal_graph.search_validation import validate_temporal_search_candidate
p=json.load(open(sys.argv[1],encoding='utf-8'))
print(json.dumps({'reports':[validate_temporal_search_candidate(x['profile'],candidate_id=x['candidateId']) for x in p]}))
"""
    with tempfile.TemporaryDirectory(prefix="temporal-prebroad-authority-") as root:
        source = Path(root) / "pairs.json"; source.write_text(json.dumps(payload, sort_keys=True), encoding="utf-8")
        result = subprocess.run([str(dashboard_python), "-c", code, str(source)], text=True, capture_output=True, check=False, timeout=120)
    if result.returncode != 0:
        raise TemporalSearchContractError(f"Dashboard native acceptance validation failed: {result.stderr.strip()[:1000]}")
    try:
        reports = json.loads(result.stdout)["reports"]
    except (KeyError, TypeError, json.JSONDecodeError) as exc:
        raise TemporalSearchContractError("Dashboard native acceptance validation returned invalid JSON") from exc
    return {str(row.get("candidateId")): dict(row) for row in reports if isinstance(row, Mapping)}


def _validate_native_acceptance(pairs: list[dict[str, Any]], reports: Mapping[str, Mapping[str, Any]]) -> None:
    for pair in pairs:
        report = reports.get(pair["candidateId"])
        if not isinstance(report, Mapping):
            raise TemporalSearchContractError(f"pair {pair['candidateId']} native acceptance was omitted")
        supplied = pair["validation"]
        required = ("candidateId", "rawSourceProfileSha256", "profileSnapshotSha256", "programSha256", "validationReportSha256", "evaluatorId", "status", "candidateAcceptable")
        if any(supplied.get(key) != report.get(key) for key in required) or report.get("candidateAcceptable") is not True or report.get("status") != "valid_evaluable":
            raise TemporalSearchContractError(f"pair {pair['candidateId']} native acceptance identity drifted or was forged")


def build_prebroad_authority(accepted: Mapping[str, Any], *, native_reports: Mapping[str, Mapping[str, Any]] | None = None, dashboard_python: Path = DEFAULT_DASHBOARD_PYTHON) -> dict[str, Any]:
    pairs = _pair_rows(accepted)
    _validate_native_acceptance(pairs, native_reports if native_reports is not None else _dashboard_native_reports(pairs, dashboard_python))
    worker = dict(accepted["workerContract"])
    preparation = {
        "schemaVersion": PREPARATION_SCHEMA,
        "candidateCount": 8,
        "taskCount": 16,
        "costViews": ["research_conservative", "none"],
        "workerContract": worker,
        "developmentWindows": [{"windowId": key, "analysisWindowStart": start, "analysisWindowEnd": end} for key, start, end in WINDOWS],
        "prohibitedEvidence": [{"windowId": "reserved_from_2024_06_29", "analysisWindowStart": PROHIBITED_START, "analysisWindowEnd": PROHIBITED_END, "reason": "reserved_and_future_evidence"}],
        "executionPolicy": {"reservedEvidencePermitted": False, "longEconomicSearchPermitted": False, "taskDispatchPermitted": False, "marketEvidenceRead": False, "gatewayContacted": False},
        "pairs": pairs,
    }
    authority = {**preparation, "schemaVersion": SCHEMA, "preparationSha256": canonical_sha256(preparation)}
    authority["authorityId"] = canonical_sha256(authority)
    return authority


def validate_prebroad_authority(authority: Mapping[str, Any], *, native_reports: Mapping[str, Mapping[str, Any]] | None = None, dashboard_python: Path = DEFAULT_DASHBOARD_PYTHON) -> dict[str, Any]:
    current = dict(authority)
    supplied = _sha(current.pop("authorityId", None), name="authorityId")
    # Reuse the closed artifact validator by projecting the only mutable source shape.
    accepted = {"schemaVersion": ACCEPTED_PAIRS_SCHEMA, "workerContract": current.get("workerContract"), "pairs": current.get("pairs")}
    rebuilt = build_prebroad_authority(accepted, native_reports=native_reports, dashboard_python=dashboard_python)
    if current != {key: value for key, value in rebuilt.items() if key != "authorityId"} or supplied != rebuilt["authorityId"]:
        raise TemporalSearchContractError("pre-broad authority identity or semantics mismatch")
    return rebuilt


def _required_id(path: Path) -> str:
    return _sha(path.read_text(encoding="utf-8").strip(), name="required authority ID")


def materialize_prebroad_matrix(authority: Mapping[str, Any], output_root: Path | str, *, required_authority_id: str, resume: bool, native_reports: Mapping[str, Mapping[str, Any]] | None = None, dashboard_python: Path = DEFAULT_DASHBOARD_PYTHON) -> dict[str, Any]:
    frozen = validate_prebroad_authority(authority, native_reports=native_reports, dashboard_python=dashboard_python)
    if frozen["authorityId"] != required_authority_id:
        raise TemporalSearchContractError("frozen authority hash does not match the required authority hash")
    tasks = []
    for pair in frozen["pairs"]:
        inputs = {row["windowId"]: row["evidencePlan"] for row in pair["windowInputs"]}
        for window_id, start, end in WINDOWS:
            identity = {"authorityId": frozen["authorityId"], "candidateId": pair["candidateId"], "windowId": window_id}
            tasks.append({"taskId": "temporal-prebroad-" + canonical_sha256(identity)[7:39], **identity, "instrument": "EURUSD", "timeframe": pair["timeframe"], "barLimit": pair["barLimit"], "analysisWindowStart": start, "analysisWindowEnd": end, "costViews": frozen["costViews"], "evidencePlan": inputs[window_id], "maxAttempts": 1, "deadlineSeconds": 900.0})
    if len(tasks) != 16 or len({row["taskId"] for row in tasks}) != 16:
        raise TemporalSearchContractError("pre-broad matrix must contain exactly sixteen distinct tasks")
    manifest = {"schemaVersion": MANIFEST_SCHEMA, "authorityId": frozen["authorityId"], "taskCount": 16, "tasks": tasks, "taskMatrixSha256": canonical_sha256(tasks), "dispatchPermitted": False, "marketEvidenceRead": False, "gatewayContacted": False}
    root = Path(output_root)
    _write_immutable(root / "authority.json", frozen)
    _write_immutable(root / "task-manifest.json", manifest)
    checkpoint = {"schemaVersion": "temporal_prebroad_checkpoint_v1", "authorityId": frozen["authorityId"], "taskMatrixSha256": manifest["taskMatrixSha256"], "mode": "resume" if resume else "fresh", "dispatchPermitted": False}
    _write_immutable(root / ("resume-checkpoint.json" if resume else "fresh-checkpoint.json"), checkpoint)
    return {"schemaVersion": "temporal_prebroad_matrix_materialization_result_v1", "authorityId": frozen["authorityId"], "taskCount": 16, "taskMatrixSha256": manifest["taskMatrixSha256"], "resume": resume, "taskDispatchPermitted": False}


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Freeze and audit the finite no-dispatch pre-broad authority.")
    commands = parser.add_subparsers(dest="command", required=True)
    prepare = commands.add_parser("prepare")
    prepare.add_argument("--accepted-pairs", type=Path, required=True)
    prepare.add_argument("--output-root", type=Path, required=True)
    audit = commands.add_parser("audit")
    audit.add_argument("--authority-path", type=Path, required=True)
    audit.add_argument("--required-authority-id-path", type=Path, required=True)
    for name in ("fresh", "resume"):
        command = commands.add_parser(name)
        command.add_argument("--authority-path", type=Path, required=True)
        command.add_argument("--required-authority-id-path", type=Path, required=True)
        command.add_argument("--output-root", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
        if args.command == "prepare":
            authority = build_prebroad_authority(_read(args.accepted_pairs))
            preparation = {
                key: value
                for key, value in authority.items()
                if key not in {"authorityId", "preparationSha256"}
            }
            preparation["schemaVersion"] = PREPARATION_SCHEMA
            _write_immutable(args.output_root / "preparation.json", preparation)
            _write_immutable(args.output_root / "authority.json", authority)
            _write_immutable(args.output_root / "authority-id.txt", authority["authorityId"] + "\n")
            result: Mapping[str, Any] = {"schemaVersion": "temporal_prebroad_prepare_result_v1", "authorityId": authority["authorityId"], "taskCount": 16, "taskDispatchPermitted": False, "marketEvidenceRead": False, "gatewayContacted": False}
        else:
            authority = validate_prebroad_authority(_read(args.authority_path))
            required = _required_id(args.required_authority_id_path)
            if authority["authorityId"] != required:
                raise TemporalSearchContractError("authority file does not match required frozen authority hash")
            if args.command == "audit":
                result = {"schemaVersion": "temporal_prebroad_authority_audit_v1", "ok": True, "authorityId": authority["authorityId"], "taskCount": 16, "taskDispatchPermitted": False}
            else:
                result = materialize_prebroad_matrix(authority, args.output_root, required_authority_id=required, resume=args.command == "resume")
        print(json.dumps(dict(result), indent=2, sort_keys=True))
        return 0
    except Exception as exc:
        print(json.dumps({"schemaVersion": "temporal_prebroad_control_error_v1", "errorType": type(exc).__name__, "message": str(exc)}, indent=2, sort_keys=True), file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
