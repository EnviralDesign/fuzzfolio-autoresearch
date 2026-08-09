"""Immutable 12/36-month scrutiny for one completed Temporal-QD generation.

This is deliberately a *post-campaign* controller.  It never mutates the
campaign it reads, never evaluates the outer tail, and delegates every replay
to the existing ``temporal_search`` candidate/window task contract.
"""

from __future__ import annotations

import argparse
import json
import math
import os
import subprocess
import tempfile
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

from .evidence_plan import build_replay_evidence_plan
from .lake_window import LakeWindowBinding, LakeWindowRequest, resolve_replay_lake_window_request
from .lake_window_client import resolve_lake_window_binding
from .play_hand_lab import LabGatewayClient
from .play_hand_lab_auth import load_lab_gateway_token
from .result_codec import read_json_object
from .temporal_discovery_base import TemporalDiscoveryContractError, _clone, canonical_sha256
from .temporal_qd_evaluation_population import load_evaluation_population
from .temporal_qd_rotating_evidence import validate_rotating_evidence_contract
from .temporal_search import (
    TEMPORAL_SEARCH_PREPARATION_SCHEMA,
    build_authority,
    build_task_matrix,
    run_temporal_search_tasks,
    validate_v3_candidate_window_result,
)


SCRUTINY_SCHEMA = "temporal_qd_post_campaign_scrutiny_v1"
PROMOTION_POLICY_SCHEMA = "temporal_qd_post_campaign_promotion_policy_v1"
_SHA_PREFIX = "sha256:"


class TemporalQDScrutinyError(RuntimeError):
    """A completed campaign cannot safely enter post-campaign scrutiny."""


def _read(path: Path, *, name: str) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError) as exc:
        raise TemporalQDScrutinyError(f"could not read {name}: {path}") from exc
    if not isinstance(value, dict):
        raise TemporalQDScrutinyError(f"{name} must be a JSON object")
    return value


def _file_sha(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return _SHA_PREFIX + digest.hexdigest()


def _controller_identity(repo: Path) -> dict[str, Any]:
    """Bind restart semantics to the exact controller and imported machinery."""

    package = Path(__file__).resolve().parent
    dependency_names = (
        "temporal_qd_post_campaign_scrutiny.py",
        "evidence_plan.py",
        "lake_window.py",
        "lake_window_client.py",
        "play_hand_lab.py",
        "result_codec.py",
        "temporal_discovery_base.py",
        "temporal_qd_evaluation_population.py",
        "temporal_qd_rotating_evidence.py",
        "temporal_search.py",
    )
    dependencies = [
        {"path": f"autoresearch/{name}", "sha256": _file_sha(package / name)}
        for name in dependency_names
    ]
    try:
        commit = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=repo, check=True,
            capture_output=True, text=True, timeout=10,
        ).stdout.strip()
        status = subprocess.run(
            ["git", "status", "--porcelain", "--untracked-files=all"],
            cwd=repo, check=True, capture_output=True, text=True, timeout=10,
        ).stdout
    except (OSError, subprocess.SubprocessError) as exc:
        raise TemporalQDScrutinyError("could not bind scrutiny controller Git commit") from exc
    if len(commit) != 40:
        raise TemporalQDScrutinyError("scrutiny controller Git commit is invalid")
    return {
        "gitCommit": commit,
        "gitDirty": bool(status.strip()),
        "dependencies": dependencies,
        "dependencyBundleSha256": canonical_sha256(dependencies),
    }


def _write_once(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = (json.dumps(dict(payload), indent=2, sort_keys=True, ensure_ascii=True, allow_nan=False) + "\n").encode("utf-8")
    if path.exists():
        if path.read_bytes() != encoded:
            raise TemporalQDScrutinyError(f"refusing to overwrite divergent immutable file: {path}")
        return
    temporary = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    try:
        with temporary.open("xb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            if path.read_bytes() != encoded:
                raise TemporalQDScrutinyError(f"refusing to overwrite divergent immutable file: {path}")
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _write_checkpoint(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = json.dumps(dict(payload), indent=2, sort_keys=True, ensure_ascii=True, allow_nan=False) + "\n"
    descriptor, temporary = tempfile.mkstemp(prefix=path.name + ".", suffix=".tmp", dir=path.parent)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except Exception:
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def _inside(child: Path, parent: Path) -> bool:
    try:
        child.resolve().relative_to(parent.resolve())
        return True
    except ValueError:
        return False


def _sha(value: Any, *, name: str) -> str:
    token = str(value or "")
    if not (token.startswith(_SHA_PREFIX) and len(token) == 71):
        raise TemporalQDScrutinyError(f"{name} must be a SHA-256 identity")
    return token


def _execution_cell_sha(profile: Mapping[str, Any]) -> str | None:
    execution = profile.get("executionConfig")
    if not isinstance(execution, Mapping):
        raise TemporalQDScrutinyError("candidate lacks executionConfig")
    if isinstance(execution.get("managementLibrary"), Mapping):
        return None
    policy = execution.get("exitPolicy")
    if not isinstance(policy, Mapping) or not isinstance(policy.get("selectedCell"), Mapping):
        raise TemporalQDScrutinyError("legacy candidate lacks selected execution cell")
    return canonical_sha256(policy["selectedCell"])


def _target_worker_contract(path: Path | str) -> dict[str, Any]:
    """Load a digest-bound v2 worker contract without consulting local state."""
    manifest_path = Path(path).resolve()
    manifest = _read(manifest_path, name="target replay-worker contract manifest")
    if manifest.get("schema_version") != "replay-worker-contract-v2":
        raise TemporalQDScrutinyError("target worker contract must use replay-worker-contract-v2")
    contract_hash = _sha(manifest.get("contract_hash"), name="target worker contract hash")
    if manifest.get("image_identity_mode") != "image_digest":
        raise TemporalQDScrutinyError("scrutiny requires a digest-bound replay worker image")
    image_digest = _sha(manifest.get("image_digest"), name="target worker image digest")
    rust_hash = _sha(manifest.get("rust_core_hash"), name="target worker Rust core hash")
    rust_build = manifest.get("rust_build_info")
    runtime = manifest.get("runtime_platform")
    if not isinstance(rust_build, Mapping) or not isinstance(runtime, Mapping):
        raise TemporalQDScrutinyError("target worker contract requires Rust build and runtime platform identity")
    for key in ("crate_name", "crate_version", "target_arch", "target_os"):
        if not isinstance(rust_build.get(key), str) or not str(rust_build[key]).strip():
            raise TemporalQDScrutinyError(f"target worker Rust build identity lacks {key}")
    for key in ("python_implementation", "python_version", "python_cache_tag", "system", "machine"):
        if not isinstance(runtime.get(key), str) or not str(runtime[key]).strip():
            raise TemporalQDScrutinyError(f"target worker runtime platform identity lacks {key}")
    fields = dict(manifest)
    fields.pop("contract_hash", None)
    fields.pop("git_sha", None)
    fields.pop("git_dirty", None)
    import hashlib
    expected = _SHA_PREFIX + hashlib.sha256(json.dumps(fields, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")).hexdigest()
    if expected != contract_hash:
        raise TemporalQDScrutinyError("target worker contract manifest identity mismatch")
    environment = {
        "worker_contract_hash": contract_hash,
        "worker_contract_schema": "replay-worker-contract-v2",
        "worker_image_digest": image_digest,
        "worker_image_identity_mode": "image_digest",
        "worker_rust_core_hash": rust_hash,
        "worker_rust_build_info": _clone(manifest["rust_build_info"], name="worker Rust build identity"),
        "worker_runtime_platform": _clone(manifest["runtime_platform"], name="worker runtime platform identity"),
    }
    return {"path": str(manifest_path), "fileSha256": _file_sha(manifest_path), "contractHash": contract_hash,
        "contractSchema": "replay-worker-contract-v2", "environment": environment,
        "manifestSha256": canonical_sha256(manifest)}


def _require_worker_environment(
    attribution: Any, *, expected: Mapping[str, Any]
) -> None:
    if not isinstance(attribution, Mapping) or any(
        attribution.get(key) != value for key, value in expected.items()
    ):
        raise TemporalQDScrutinyError(
            "completed scrutiny result worker environment identity mismatch"
        )


def _descriptor(candidate: Mapping[str, Any]) -> str:
    """A deterministic diversity key using existing structural material only."""
    profile = candidate["sourceProfile"]
    indicators = profile.get("indicators") if isinstance(profile, Mapping) else None
    families: list[str] = []
    if isinstance(indicators, list):
        for item in indicators:
            meta = item.get("meta") if isinstance(item, Mapping) else None
            token = str((meta or {}).get("id") or "").strip().upper()
            if token:
                families.append(token)
    graph = profile.get("graph") if isinstance(profile, Mapping) else None
    states = len(graph.get("states") or []) if isinstance(graph, Mapping) else 0
    return "|".join((str(profile.get("directionMode") or "unknown"), str(states), ",".join(sorted(set(families)))))


def _completed_generation_root(campaign_root: Path, generation_index: int) -> Path:
    state = _read(campaign_root / "state.json", name="campaign state")
    if state.get("schemaVersion") != "temporal_qd_supervisor_state_v3":
        raise TemporalQDScrutinyError("campaign state schema is unsupported")
    if int(state.get("currentGenerationIndex") or 0) <= generation_index:
        raise TemporalQDScrutinyError("requested generation is not completed")
    root = campaign_root / "generations" / f"generation-{generation_index:04d}"
    if not (root / "archive.json").is_file():
        raise TemporalQDScrutinyError("requested generation does not have an immutable archive")
    return root


def _load_source(
    *, campaign_root: Path, generation_index: int, expected_cohort_size: int
) -> tuple[list[dict[str, Any]], dict[str, Any], dict[str, Any], dict[str, Any]]:
    generation_root = _completed_generation_root(campaign_root, generation_index)
    cohort_path = generation_root / "evidence" / "cohort.json"
    population_path = generation_root / "proposal" / "population.json"
    cohort = _read(cohort_path, name="generation cohort")
    if cohort.get("schemaVersion") != "temporal_qd_current_panel_evaluation_cohort_v1":
        raise TemporalQDScrutinyError("generation cohort schema is unsupported")
    if cohort.get("generationIndex") != generation_index:
        raise TemporalQDScrutinyError("generation cohort index drifted")
    new_ids = cohort.get("newProposalCandidateIds")
    if not isinstance(new_ids, list) or len(new_ids) != expected_cohort_size or len(set(new_ids)) != expected_cohort_size:
        raise TemporalQDScrutinyError(f"generation cohort must contain exactly {expected_cohort_size} unique new proposal candidates")
    _sha(cohort.get("cohortSha256"), name="generation cohort identity")
    material = dict(cohort)
    supplied = material.pop("cohortSha256")
    if canonical_sha256(material) != supplied:
        raise TemporalQDScrutinyError("generation cohort identity mismatch")
    population = load_evaluation_population(population_path=population_path, verify_population_file=True)
    rows = population.get("candidates")
    if not isinstance(rows, list):
        raise TemporalQDScrutinyError("evaluation population candidates are unavailable")
    selected = {str(item) for item in new_ids}
    by_id = {str(item.get("candidateId")): item for item in rows if isinstance(item, Mapping)}
    if set(by_id) != selected or len(by_id) != expected_cohort_size:
        raise TemporalQDScrutinyError("evaluation population and cohort new-proposal candidate IDs disagree")
    config = _read(campaign_root / "config.json", name="campaign config")
    rotating = validate_rotating_evidence_contract(_read(campaign_root / "rotating-evidence.json", name="rotating evidence"))
    return ([_clone(by_id[item], name="evaluation candidate") for item in sorted(selected)], cohort, config, rotating)


def _common_binding_request(
    *, candidates: Sequence[Mapping[str, Any]], window: Mapping[str, Any], catalog: Mapping[str, Any], base_timeframe: str
) -> LakeWindowRequest:
    requests: list[LakeWindowRequest] = []
    for candidate in candidates:
        profile = candidate["sourceProfile"]
        instruments = profile.get("instruments") if isinstance(profile, Mapping) else None
        if not isinstance(instruments, list) or len(instruments) != 1:
            raise TemporalQDScrutinyError("scrutiny only accepts one-instrument candidate snapshots")
        requests.append(resolve_replay_lake_window_request(
            pairs=[str(instruments[0])], base_timeframe=base_timeframe, profile_snapshot=profile,
            analysis_window_start=str(window["analysisWindowStart"]), analysis_window_end=str(window["analysisWindowEnd"]),
            frozen_catalog=catalog,
        ))
    if not requests:
        raise TemporalQDScrutinyError("scrutiny cohort is empty")
    pairs = requests[0].pairs
    end = requests[0].data_end
    coverage = requests[0].coverage_policy
    if any(item.pairs != pairs or item.data_end != end or item.coverage_policy != coverage for item in requests):
        raise TemporalQDScrutinyError("scrutiny candidates cannot share one attested lake envelope")
    return LakeWindowRequest(
        pairs=pairs,
        timeframes=sorted({frame for item in requests for frame in item.timeframes}),
        data_start=min(item.data_start for item in requests), data_end=end, coverage_policy=coverage,
    )


def _stage_preparation(
    *, stage: str, candidates: Sequence[Mapping[str, Any]], window: Mapping[str, Any], binding: LakeWindowBinding,
    worker_contract: Mapping[str, Any], base_timeframe: str, bar_limit: int, campaign_id: str, outer_tail_start: str,
) -> dict[str, Any]:
    normalized_window = {"windowId": stage, "analysisWindowStart": str(window["analysisWindowStart"]), "analysisWindowEnd": str(window["analysisWindowEnd"])}
    rows: list[dict[str, Any]] = []
    months = 12 if stage == "validation_12m" else 36
    for candidate in candidates:
        profile = _clone(candidate["sourceProfile"], name="scrutiny candidate profile")
        instruments = profile["instruments"]
        plan = build_replay_evidence_plan(
            evidence_role="validation" if stage == "validation_12m" else "scrutiny",
            selection_data_end=normalized_window["analysisWindowEnd"],
            analysis_window_start=normalized_window["analysisWindowStart"], analysis_window_end=normalized_window["analysisWindowEnd"],
            requested_horizon_months=months, profile_snapshot=profile, campaign_plan_id=campaign_id,
            execution_cell_sha256=_execution_cell_sha(profile), lake_window_binding=binding,
            data_availability_cutoff=normalized_window["analysisWindowEnd"], coverage_policy="require_complete",
        )
        rows.append({"candidateId": candidate["candidateId"], "sourceProfile": profile,
            "sourceProfileSha256": candidate["sourceProfileSha256"], "instrument": instruments[0],
            "timeframe": base_timeframe, "barLimit": bar_limit,
            "windowInputs": [{"windowId": stage, "evidencePlan": plan.model_dump(mode="json")} ]})
    preparation = {"schemaVersion": TEMPORAL_SEARCH_PREPARATION_SCHEMA,
        "authorityLabel": f"temporal-qd-{stage}-{campaign_id[-12:]}", "workerContract": dict(worker_contract),
        "candidates": rows, "developmentWindows": [normalized_window],
        "prohibitedEvidence": [{"windowId": "untouched-outer-tail", "analysisWindowStart": outer_tail_start,
            "analysisWindowEnd": "9999-12-31T00:00:00Z", "reason": "sole untouched evidence"}],
        "bounds": {"maxCandidates": len(rows), "maxDevelopmentWindows": 1, "maxTasks": len(rows),
            "maxAttempts": 8, "deadlineSeconds": 86400.0}}
    build_authority(preparation)
    return preparation


def _read_stage_results(stage_root: Path, authority: Mapping[str, Any], *, expected_worker_environment: Mapping[str, Any]) -> list[dict[str, Any]]:
    checkpoint = _read(stage_root / "checkpoint.json", name="stage checkpoint")
    completed = checkpoint.get("completed")
    if not isinstance(completed, Mapping):
        raise TemporalQDScrutinyError("stage checkpoint completed records are invalid")
    tasks = {item["task_id"]: item for item in build_task_matrix(authority)}
    if set(completed) != set(tasks):
        raise TemporalQDScrutinyError("stage is incomplete")
    rows: list[dict[str, Any]] = []
    for task_id in sorted(tasks):
        record = completed[task_id]
        if not isinstance(record, Mapping) or record.get("outcome") == "rejected":
            rows.append({"candidateId": tasks[task_id]["payload"]["candidate_id"], "validReplay": False, "reason": "terminal_rejection"})
            continue
        path = Path(str(record.get("resultPath") or ""))
        try:
            material, _metadata = read_json_object(path)
            validate_v3_candidate_window_result(material, task_payload=tasks[task_id]["payload"])
        except Exception as exc:
            raise TemporalQDScrutinyError(f"invalid completed scrutiny result for {task_id}") from exc
        metrics = material["cost_view_results"]["research_conservative"]["replay_result"]["metrics"]
        terminal = metrics["terminalValuation"]
        _require_worker_environment(
            material.get("worker_attribution"), expected=expected_worker_environment
        )
        rows.append({"candidateId": tasks[task_id]["payload"]["candidate_id"], "validReplay": True,
            "netConservativeR": float(metrics["terminalAdjustedTotalNetR"]), "maxDrawdownR": float(metrics["terminalAdjustedMaxDrawdownR"]),
            "closedTrades": int(metrics["tradesClosed"]), "unresolvedPosition": bool(metrics["unresolvedPosition"]),
            "unresolvedPendingEffect": bool(metrics["unresolvedPendingEffect"]),
            "terminalPositionStatus": terminal["positionStatus"], "observationStreamSha256": material["observation_stream_sha256"],
            "sharedObservationStreamId": material["shared_observation_stream_id"], "taskId": task_id,
            "resultSha256": canonical_sha256(material)})
    return rows


def _rank_validation(*, results: Sequence[Mapping[str, Any]], candidates: Mapping[str, Mapping[str, Any]]) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    ranked: list[dict[str, Any]] = []
    for row in results:
        item = dict(row)
        complete = bool(item.get("validReplay")) and not item.get("unresolvedPosition") and not item.get("unresolvedPendingEffect")
        raw_net = item.get("netConservativeR")
        raw_drawdown = item.get("maxDrawdownR")
        net = float(raw_net) if isinstance(raw_net, (int, float)) and math.isfinite(float(raw_net)) else None
        drawdown = float(raw_drawdown) if isinstance(raw_drawdown, (int, float)) and math.isfinite(float(raw_drawdown)) else None
        trades = int(item.get("closedTrades") or 0)
        passes = complete and trades >= 24 and net is not None and net > 0.0
        item["completeValidReplay"] = complete
        item["promotionEligible"] = passes
        item["descriptor"] = _descriptor(candidates[str(item["candidateId"])])
        # Explicit, deterministic balanced ordering: net gain leads; then less
        # drawdown; then greater trade support; ID is the final non-economic tie.
        # The leading eligibility bucket keeps rejected/malformed rows behind
        # every eligible result.  All persisted values remain strict JSON;
        # infinities would violate the immutable artifact writer's contract.
        item["promotionSortKey"] = [
            0 if passes else 1,
            -net if net is not None else 0.0,
            drawdown if drawdown is not None else 0.0,
            -trades,
            str(item["candidateId"]),
        ]
        ranked.append(item)
    ranked.sort(key=lambda item: tuple(item["promotionSortKey"]))
    passers = [item for item in ranked if item["promotionEligible"]]
    if len(passers) <= 128:
        return ranked, passers
    # Diversity floor: first take each descriptor's strongest passer, then
    # fill remaining slots by the same pre-registered economic ordering.
    selected: list[dict[str, Any]] = []
    seen: set[str] = set()
    for item in passers:
        if item["descriptor"] not in seen:
            selected.append(item); seen.add(item["descriptor"])
            if len(selected) == 128:
                return ranked, selected
    selected_ids = {item["candidateId"] for item in selected}
    selected.extend(item for item in passers if item["candidateId"] not in selected_ids)  # type: ignore[arg-type]
    return ranked, selected[:128]


def _stage_evaluation_identity(*, stage: str, authority: Mapping[str, Any], results: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    records = [{"candidateId": item["candidateId"], "resultSha256": item.get("resultSha256"), "validReplay": item["validReplay"]} for item in sorted(results, key=lambda value: str(value["candidateId"]))]
    payload = {"schemaVersion": "temporal_qd_post_campaign_stage_evaluation_v1", "stage": stage,
        "authorityId": authority["authorityId"], "taskMatrixSha256": canonical_sha256(build_task_matrix(authority)), "results": records}
    payload["evaluationSha256"] = canonical_sha256(payload)
    return payload


def _observation_stream_consistency(results: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    streams: dict[str, set[str]] = {}
    missing = 0
    for item in results:
        if item.get("validReplay") is False:
            continue
        shared_id = item.get("sharedObservationStreamId")
        stream_hash = item.get("observationStreamSha256")
        if not isinstance(shared_id, str) or not isinstance(stream_hash, str):
            missing += 1
            continue
        streams.setdefault(shared_id, set()).add(stream_hash)
    divergent = sorted(key for key, values in streams.items() if len(values) != 1)
    return {
        "checked": len(results),
        "sharedIdentityCount": len(streams),
        "missingIdentityCount": missing,
        "divergentSharedObservationStreamIds": divergent,
        "valid": missing == 0 and not divergent,
    }


def run_qd_post_campaign_scrutiny(
    *, campaign_root: Path | str, generation_index: int, output_root: Path | str,
    target_worker_contract_path: Path | str,
    gateway_url: str = "http://127.0.0.1:8799", gateway_token: str | None = None,
    timeout_seconds: float = 86400.0, poll_interval_seconds: float = 0.25, enqueue_batch_size: int = 128,
    attestor: Callable[..., LakeWindowBinding] = resolve_lake_window_binding,
    client: Any | None = None, expected_cohort_size: int = 1024,
) -> dict[str, Any]:
    """Run 12m for all exact new G5 proposals then 36m for pre-registered passers."""
    campaign = Path(campaign_root).resolve()
    root = Path(output_root).resolve()
    repo = Path(__file__).resolve().parents[1]
    if _inside(root, campaign) or _inside(root, repo):
        raise TemporalQDScrutinyError("scrutiny output must be outside both campaign source and repository")
    candidates, cohort, config, rotating = _load_source(campaign_root=campaign, generation_index=generation_index, expected_cohort_size=expected_cohort_size)
    evidence = config.get("evaluation", {}).get("predeclaredEvidenceContext", {}) if isinstance(config.get("evaluation"), Mapping) else {}
    catalog_record = evidence.get("constructionCatalog") if isinstance(evidence, Mapping) else None
    if not isinstance(catalog_record, Mapping):
        raise TemporalQDScrutinyError("campaign lacks frozen construction catalog binding")
    catalog_path = Path(str(catalog_record.get("path") or ""))
    catalog = _read(catalog_path, name="frozen construction catalog")
    if canonical_sha256(catalog) != catalog_record.get("catalogSha256"):
        raise TemporalQDScrutinyError("frozen construction catalog identity drifted")
    template_path = Path(str(config.get("evaluation", {}).get("templatePreparationPath") or ""))
    template = _read(template_path, name="generation template preparation")
    prototype = (template.get("candidates") or [None])[0]
    if not isinstance(prototype, Mapping):
        raise TemporalQDScrutinyError("generation template lacks a candidate prototype")
    target_worker = _target_worker_contract(target_worker_contract_path)
    worker = {"workerContractSha256": target_worker["contractHash"], "workerContractSchema": target_worker["contractSchema"]}
    base_timeframe = str(prototype.get("timeframe") or "")
    bar_limit = int(prototype.get("barLimit") or 0)
    if not base_timeframe or bar_limit < 10:
        raise TemporalQDScrutinyError("generation template geometry is invalid")
    outer_tail_start = str(rotating["outerTail"]["analysisWindowStart"])
    stages = {"validation_12m": rotating["researchScrutiny"]["validation"], "scrutiny_36m": rotating["researchScrutiny"]["scrutiny"]}
    controller_identity = _controller_identity(repo)
    if controller_identity["gitDirty"]:
        raise TemporalQDScrutinyError(
            "scrutiny controller repository must be clean before freezing evidence"
        )
    source_identity = {"schemaVersion": SCRUTINY_SCHEMA, "campaignRoot": str(campaign), "generationIndex": generation_index,
        "controllerIdentity": controller_identity,
        "campaignConfigSha256": _file_sha(campaign / "config.json"), "rotatingEvidenceSha256": rotating["rotatingEvidenceSha256"],
        "cohortSha256": cohort["cohortSha256"], "evaluationPopulationSha256": candidates and _read(campaign / "generations" / f"generation-{generation_index:04d}" / "proposal" / "evaluation-population.json", name="evaluation population")["evaluationPopulationSha256"],
        "targetWorkerContract": target_worker,
        "newProposalCandidateIds": [row["candidateId"] for row in candidates], "newProposalCandidateIdsSha256": canonical_sha256([row["candidateId"] for row in candidates]),
        "outerTailTouched": False}
    source_identity["sourceIdentitySha256"] = canonical_sha256(source_identity)
    _write_once(root / "source-identity.json", source_identity)
    _write_once(root / "target-worker-contract.json", target_worker)
    policy = {"schemaVersion": PROMOTION_POLICY_SCHEMA, "sourceIdentitySha256": source_identity["sourceIdentitySha256"],
        "validationStage": "validation_12m", "criteria": {"netConservativeR": {"operator": ">", "value": 0.0}, "minimumClosedTrades": 24,
            "completeValidReplay": True, "noUnresolvedState": True}, "maxPromotions": 128,
        "ordering": ["netConservativeR_desc", "maxDrawdownR_asc", "closedTrades_desc", "candidateId_asc"],
        "diversity": {"enabled": True, "descriptor": "direction_mode_state_count_indicator_family_set", "floor": "one strongest passer per descriptor before ranked fill"}}
    policy["promotionPolicySha256"] = canonical_sha256(policy)
    _write_once(root / "promotion-policy.json", policy)
    checkpoint_path = root / "scrutiny-checkpoint.json"
    checkpoint = {"schemaVersion": SCRUTINY_SCHEMA, "sourceIdentitySha256": source_identity["sourceIdentitySha256"], "promotionPolicySha256": policy["promotionPolicySha256"], "outerTailTouched": False, "stages": {}}
    if checkpoint_path.is_file():
        existing = _read(checkpoint_path, name="scrutiny checkpoint")
        if existing.get("sourceIdentitySha256") != checkpoint["sourceIdentitySha256"] or existing.get("promotionPolicySha256") != checkpoint["promotionPolicySha256"]:
            raise TemporalQDScrutinyError("existing scrutiny checkpoint binds different source or promotion policy")
        checkpoint = existing

    # Freeze both research windows before any 12m result is observed.  The
    # 36m cohort is not known yet, but its binding can safely be the exact
    # candidate-universe envelope; the promoted subset is contained by it.
    stage_bindings: dict[str, LakeWindowBinding] = {}
    for stage, stage_contract in stages.items():
        stage_root = root / stage
        binding_path = stage_root / "remote-binding.json"
        request = _common_binding_request(
            candidates=candidates,
            window=stage_contract["window"],
            catalog=catalog,
            base_timeframe=base_timeframe,
        )
        if binding_path.is_file():
            binding = LakeWindowBinding.model_validate(
                _read(binding_path, name=f"{stage} remote binding")
            )
            if binding.request != request or binding.attestation_sha256 is None:
                raise TemporalQDScrutinyError(
                    "persisted scrutiny binding does not match the frozen candidate envelope"
                )
        else:
            binding = attestor(request, legacy_selection_manifest_sha256=None)
            if binding.request != request or binding.attestation_sha256 is None:
                raise TemporalQDScrutinyError(
                    "remote scrutiny attestor returned a forged or unattested binding"
                )
            _write_once(binding_path, binding.model_dump(mode="json"))
        stage_bindings[stage] = binding

    owned_client = client is None
    gateway = client or LabGatewayClient(base_url=gateway_url, token=gateway_token)
    try:
        stage_results: dict[str, list[dict[str, Any]]] = {}
        for stage, stage_contract in stages.items():
            if stage == "scrutiny_36m":
                promotion = _read(root / "promotion-manifest.json", name="promotion manifest")
                promoted_ids = promotion.get("promotedCandidateIds")
                if not isinstance(promoted_ids, list):
                    raise TemporalQDScrutinyError("promotion manifest is malformed")
                selected = [row for row in candidates if row["candidateId"] in set(promoted_ids)]
                if not selected:
                    _write_once(root / stage / "summary.json", {"schemaVersion": SCRUTINY_SCHEMA, "stage": stage, "status": "skipped_no_validation_passers", "candidateCount": 0})
                    checkpoint["stages"][stage] = "skipped_no_validation_passers"
                    _write_checkpoint(checkpoint_path, checkpoint)
                    continue
            else:
                selected = candidates
            stage_root = root / stage
            window = stage_contract["window"]
            binding = stage_bindings[stage]
            preparation = _stage_preparation(stage=stage, candidates=selected, window=window, binding=binding, worker_contract=worker,
                base_timeframe=base_timeframe, bar_limit=bar_limit, campaign_id=source_identity["sourceIdentitySha256"], outer_tail_start=outer_tail_start)
            _write_once(stage_root / "preparation.json", preparation)
            authority = build_authority(preparation)
            _write_once(stage_root / "authority.json", authority)
            _write_once(stage_root / "scrutiny-authority.json", {"schemaVersion": SCRUTINY_SCHEMA,
                "sourceIdentitySha256": source_identity["sourceIdentitySha256"], "targetWorkerContract": target_worker,
                "temporalSearchAuthority": authority})
            run_qd = run_temporal_search_tasks(gateway, authority, output_root=stage_root, timeout_seconds=timeout_seconds,
                poll_interval_seconds=poll_interval_seconds, resume=(stage_root / "checkpoint.json").is_file(), enqueue_batch_size=enqueue_batch_size,
                include_selection_summary=False)
            results = _read_stage_results(stage_root, authority, expected_worker_environment=target_worker["environment"])
            stage_results[stage] = results
            stream_consistency = _observation_stream_consistency(results)
            if not stream_consistency["valid"]:
                raise TemporalQDScrutinyError(
                    f"{stage} observation-stream identities are missing or divergent"
                )
            evaluation = _stage_evaluation_identity(stage=stage, authority=authority, results=results)
            _write_once(stage_root / "evaluation-identity.json", evaluation)
            _write_once(stage_root / "candidate-rankings.json", {"schemaVersion": SCRUTINY_SCHEMA, "stage": stage, "authorityId": authority["authorityId"], "rows": sorted(results, key=lambda row: str(row["candidateId"])), "observationStreamConsistency": stream_consistency})
            _write_once(stage_root / "summary.json", {"schemaVersion": SCRUTINY_SCHEMA, "stage": stage, "authorityId": authority["authorityId"], "run": run_qd,
                "candidateCount": len(selected), "validReplayCount": sum(bool(row.get("validReplay")) for row in results), "observationStreamConsistency": stream_consistency, "evaluationSha256": evaluation["evaluationSha256"]})
            checkpoint["stages"][stage] = evaluation["evaluationSha256"]
            _write_checkpoint(checkpoint_path, checkpoint)
            if stage == "validation_12m":
                by_id = {row["candidateId"]: row for row in candidates}
                rankings, promoted = _rank_validation(results=results, candidates=by_id)
                _write_once(root / "validation-rankings.json", {"schemaVersion": SCRUTINY_SCHEMA, "policySha256": policy["promotionPolicySha256"], "rows": rankings})
                manifest = {"schemaVersion": SCRUTINY_SCHEMA, "sourceIdentitySha256": source_identity["sourceIdentitySha256"], "promotionPolicySha256": policy["promotionPolicySha256"],
                    "validationEvaluationSha256": evaluation["evaluationSha256"], "eligiblePasserCount": sum(bool(row["promotionEligible"]) for row in rankings),
                    "promotedCandidateIds": [row["candidateId"] for row in promoted], "promotionCount": len(promoted), "outerTailTouched": False}
                manifest["promotionManifestSha256"] = canonical_sha256(manifest)
                _write_once(root / "promotion-manifest.json", manifest)
        promotion = _read(root / "promotion-manifest.json", name="promotion manifest")
        validation_summary = _read(root / "validation_12m" / "summary.json", name="validation summary")
        scrutiny_summary = _read(root / "scrutiny_36m" / "summary.json", name="scrutiny summary")
        result = {"schemaVersion": SCRUTINY_SCHEMA, "sourceIdentitySha256": source_identity["sourceIdentitySha256"], "promotionPolicySha256": policy["promotionPolicySha256"],
            "targetWorkerContractHash": target_worker["contractHash"],
            "validationCandidateCount": len(candidates), "validationEvaluationSha256": validation_summary.get("evaluationSha256"),
            "promotionManifest": str(root / "promotion-manifest.json"), "promotionManifestSha256": promotion["promotionManifestSha256"],
            "promotionCount": int(promotion["promotionCount"]), "scrutinyStageOutcome": checkpoint["stages"].get("scrutiny_36m"),
            "scrutinyCandidateCount": int(scrutiny_summary.get("candidateCount") or 0), "outerTailTouched": False,
            "status": "completed"}
        result["resultSha256"] = canonical_sha256(result)
        _write_once(root / "result.json", result)
        return result
    finally:
        if owned_client:
            gateway.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run immutable 12m -> 36m post-campaign Temporal-QD scrutiny.")
    parser.add_argument("--campaign-root", type=Path, required=True)
    parser.add_argument("--generation-index", type=int, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--target-worker-contract", type=Path, required=True,
        help="exact digest-bound replay-worker-contract-v2 JSON emitted for the target worker image")
    parser.add_argument("--gateway-url", default="http://127.0.0.1:8799")
    parser.add_argument("--gateway-token")
    parser.add_argument("--timeout-seconds", type=float, default=86400.0)
    parser.add_argument("--poll-interval-seconds", type=float, default=0.25)
    parser.add_argument("--enqueue-batch-size", type=int, default=128)
    args = parser.parse_args()
    result = run_qd_post_campaign_scrutiny(campaign_root=args.campaign_root, generation_index=args.generation_index,
        output_root=args.output_root, target_worker_contract_path=args.target_worker_contract, gateway_url=args.gateway_url,
        gateway_token=args.gateway_token or load_lab_gateway_token(create=False), timeout_seconds=args.timeout_seconds,
        poll_interval_seconds=args.poll_interval_seconds, enqueue_batch_size=args.enqueue_batch_size)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":  # pragma: no cover
    main()
