"""Freeze remotely-attested Latin-square development evidence for temporal QD.

This is intentionally an evidence-only boundary.  It creates no candidates,
submits no replay work, and does not alter the v1 evidence ladder.  A frozen
four-year curriculum is expanded into sixteen exact calendar quarters, then
into one master development-universe template and four generation-panel
templates.  Every template is immutable and carries the same closed worker,
execution, catalog, and remote lake bindings.
"""

from __future__ import annotations

import argparse
import copy
import json
import os
from collections.abc import Mapping, Sequence
from pathlib import Path
from typing import Any
from uuid import uuid4

from .evidence_plan import build_replay_evidence_plan, canonical_sha256
from .lake_window import LakeWindowBinding, LakeWindowRequest, lake_window_request_contains
from .lake_window_client import resolve_lake_window_binding
from .temporal_discovery_base import TemporalDiscoveryContractError, _clone
from .temporal_qd_evidence_ladder_materializer import (
    _admitted_timeframes, _attest, _candidate_geometry, _capability_envelope,
    _catalog, _envelope_request, _execution_cell_sha, _external_root, _file_sha,
    _pair_config_identity, _read, _seed_members, _variants, _worker_contract,
    _write_once,
)
from .temporal_qd_rotating_evidence import (
    OUTER_TAIL_START, ROTATING_EVIDENCE_INPUT_SCHEMA, build_rotating_evidence_contract,
    validate_generation_template, validate_rotating_evidence_contract,
)
from .temporal_search import TEMPORAL_SEARCH_PREPARATION_SCHEMA, build_authority


MATERIALIZATION_SCHEMA = "temporal_qd_rotating_evidence_materialization_v1"
RESULT_SCHEMA = "temporal_qd_rotating_evidence_materialization_result_v1"
DEFAULT_MAX_ATTEMPTS_PER_TASK = 8
BINDING_CHECKPOINT_SCHEMA = "temporal_qd_rotating_evidence_binding_checkpoint_v1"


def _months(window: Mapping[str, str]) -> int:
    start = str(window["analysisWindowStart"])
    end = str(window["analysisWindowEnd"])
    sy, sm = int(start[:4]), int(start[5:7])
    ey, em = int(end[:4]), int(end[5:7])
    result = (ey - sy) * 12 + em - sm
    if result != 3:
        raise TemporalDiscoveryContractError("rotating QD materialization requires exact three-month quarters")
    return result


def _strict_curriculum(contract: Mapping[str, Any]) -> list[dict[str, Any]]:
    """Reject gaps, altered ordering, or a non-Latin curriculum before I/O."""
    years = list(contract.get("developmentYears") or [])
    quarters = list(contract.get("quarterWindows") or [])
    panels = list(contract.get("panels") or [])
    if len(years) != 4 or len(quarters) != 16 or len(panels) != 4:
        raise TemporalDiscoveryContractError("rotating QD curriculum must contain four years, sixteen quarters, and four panels")
    for index, (left, right) in enumerate(zip(years, years[1:]), start=1):
        if left.get("analysisWindowEnd") != right.get("analysisWindowStart"):
            raise TemporalDiscoveryContractError(f"rotating QD development years have a gap after year {index}")
    expected_ids = [f"year-{year}-q{quarter}" for year in range(1, 5) for quarter in range(1, 5)]
    if [row.get("windowId") for row in quarters] != expected_ids:
        raise TemporalDiscoveryContractError("rotating QD quarter ordering drifted")
    for index, row in enumerate(quarters):
        if int(row.get("yearIndex") or 0) != index // 4 + 1 or int(row.get("quarterIndex") or 0) != index % 4 + 1:
            raise TemporalDiscoveryContractError("rotating QD quarter identity drifted")
        if _months(row) != 3:
            raise TemporalDiscoveryContractError("rotating QD quarter effective horizon drifted")
        if row["analysisWindowEnd"] > OUTER_TAIL_START:
            raise TemporalDiscoveryContractError("rotating QD quarter touches the untouched outer tail")
        if index and quarters[index - 1]["analysisWindowEnd"] != row["analysisWindowStart"]:
            raise TemporalDiscoveryContractError("rotating QD quarter coverage has a gap or overlap")
    expected_panels = [
        [f"year-{year}-q{((year + phase - 1) % 4) + 1}" for year in range(1, 5)]
        for phase in range(4)
    ]
    if [row.get("windowIds") for row in panels] != expected_panels:
        raise TemporalDiscoveryContractError("rotating QD Latin-square panel ordering drifted")
    if {item for panel in expected_panels for item in panel} != set(expected_ids):
        raise TemporalDiscoveryContractError("rotating QD panels do not cover each quarter exactly once")
    return [_clone(row, name="rotating QD quarter") for row in quarters]


def _preparation(*, label: str, windows: Sequence[Mapping[str, Any]], members: Sequence[Mapping[str, Any]],
                 instrument: str, base_timeframe: str, bar_limit: int, worker_contract: Mapping[str, str],
                 bindings: Mapping[str, LakeWindowBinding], curriculum_sha256: str,
                 max_attempts_per_task: int) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    # ``temporal_search`` deliberately has a closed three-field window schema.
    # Year/quarter metadata remains in the immutable curriculum manifest; the
    # task authority gets only the exact executable binding.
    normalized_windows = [{key: str(row[key]) for key in (
        "windowId", "analysisWindowStart", "analysisWindowEnd"
    )} for row in windows]
    for member in members:
        profile = _clone(member["sourceProfile"], name="rotating QD seed profile")
        inputs = []
        for window in normalized_windows:
            window_id = str(window["windowId"])
            plan = build_replay_evidence_plan(
                evidence_role="training", selection_data_end=window["analysisWindowEnd"],
                analysis_window_start=window["analysisWindowStart"], analysis_window_end=window["analysisWindowEnd"],
                requested_horizon_months=3, profile_snapshot=profile,
                campaign_plan_id=f"temporal-qd-rotating-development-{curriculum_sha256[-12:]}",
                execution_cell_sha256=_execution_cell_sha(profile), lake_window_binding=bindings[window_id],
                data_availability_cutoff=window["analysisWindowEnd"], coverage_policy="require_complete",
            )
            inputs.append({"windowId": window_id, "evidencePlan": plan.model_dump(mode="json")})
        candidates.append({"candidateId": member["memberId"], "sourceProfile": profile,
            "sourceProfileSha256": member["sourceProfileSha256"], "instrument": instrument,
            "timeframe": base_timeframe, "barLimit": bar_limit, "windowInputs": inputs})
    preparation = {"schemaVersion": TEMPORAL_SEARCH_PREPARATION_SCHEMA,
        "authorityLabel": label, "workerContract": dict(worker_contract), "candidates": candidates,
        "developmentWindows": normalized_windows,
        "prohibitedEvidence": [{"windowId": "untouched-outer-tail", "analysisWindowStart": OUTER_TAIL_START,
            "analysisWindowEnd": "9999-12-31T00:00:00Z", "reason": "sole untouched evidence"}],
        "bounds": {"maxCandidates": len(candidates), "maxDevelopmentWindows": len(normalized_windows),
            "maxTasks": len(candidates) * len(normalized_windows),
            # This is deliberately a small, frozen per-task retry limit.  It
            # must not scale with population width or task-matrix size.
            "maxAttempts": max_attempts_per_task,
            "deadlineSeconds": 86400.0}}
    build_authority(preparation)
    return preparation


def _template_record(path: Path, preparation: Mapping[str, Any]) -> dict[str, str]:
    return {"path": str(path), "preparationSha256": canonical_sha256(preparation),
            "authorityId": build_authority(preparation)["authorityId"]}


def _valid_sha(value: Any) -> bool:
    return (
        isinstance(value, str) and len(value) == 71 and value.startswith("sha256:")
        and all(char in "0123456789abcdef" for char in value[7:])
    )


def _persisted_bindings(*, root: Path, curriculum_sha256: str,
                        worker: Mapping[str, str], catalog_sha256: str,
                        execution_identities: Sequence[Mapping[str, Any]],
                        quarters: Sequence[Mapping[str, Any]]) -> dict[str, LakeWindowBinding] | None:
    """Reuse verified frozen semantic bindings on an idempotent restart.

    Lake receipts are provenance and can rotate remotely.  The evidence
    authority is instead the persisted request, coverage policy, and semantic
    digest.  We therefore do not call the lake again when a valid immutable
    materialization exists.
    """
    path = root / "materialization.json"
    if not path.exists():
        return None
    manifest = _read(path, name="rotating QD materialization")
    identity_material = dict(manifest)
    supplied = identity_material.pop("materializationSha256", None)
    if (manifest.get("schemaVersion") != MATERIALIZATION_SCHEMA
            or not _valid_sha(supplied)
            or canonical_sha256(identity_material) != supplied):
        raise TemporalDiscoveryContractError("rotating QD persisted materialization identity drifted")
    if (manifest.get("curriculumSha256") != curriculum_sha256
            or manifest.get("workerContract") != dict(worker)):
        raise TemporalDiscoveryContractError("rotating QD persisted curriculum or worker identity drifted")
    catalog = manifest.get("constructionCatalog")
    if not isinstance(catalog, Mapping) or catalog.get("catalogSha256") != catalog_sha256:
        raise TemporalDiscoveryContractError("rotating QD persisted catalog identity drifted")
    if manifest.get("executionIdentities") != list(execution_identities):
        raise TemporalDiscoveryContractError("rotating QD persisted execution identity drifted")
    rows = manifest.get("quarters")
    if not isinstance(rows, list) or len(rows) != len(quarters):
        raise TemporalDiscoveryContractError("rotating QD persisted quarter binding is missing")
    expected = {str(row["windowId"]): row for row in quarters}
    templates = manifest.get("templates")
    master_identity = templates.get("master") if isinstance(templates, Mapping) else None
    if not isinstance(master_identity, Mapping):
        raise TemporalDiscoveryContractError("rotating QD persisted master template identity is missing")
    master_path = Path(str(master_identity.get("path") or ""))
    if not master_path.is_file():
        raise TemporalDiscoveryContractError("rotating QD persisted master template is missing")
    master = _read(master_path, name="rotating QD persisted master template")
    if canonical_sha256(master) != master_identity.get("preparationSha256"):
        raise TemporalDiscoveryContractError("rotating QD persisted master template preparation identity drifted")
    candidates = master.get("candidates")
    first_candidate = candidates[0] if isinstance(candidates, list) and candidates else None
    inputs = first_candidate.get("windowInputs") if isinstance(first_candidate, Mapping) else None
    plan_by_window = {
        str(row.get("windowId")): row.get("evidencePlan")
        for row in inputs or [] if isinstance(row, Mapping)
    }
    if set(plan_by_window) != set(expected):
        raise TemporalDiscoveryContractError("rotating QD persisted master template coverage drifted")
    result: dict[str, LakeWindowBinding] = {}
    for row in rows:
        if not isinstance(row, Mapping):
            raise TemporalDiscoveryContractError("rotating QD persisted quarter binding is malformed")
        window_id = str(row.get("windowId") or "")
        if (window_id not in expected or any(
                row.get(key) != expected[window_id].get(key)
                for key in ("analysisWindowStart", "analysisWindowEnd"))):
            raise TemporalDiscoveryContractError("rotating QD persisted quarter ordering drifted")
        binding_raw = row.get("remoteBinding")
        if isinstance(binding_raw, Mapping) and (
                not _valid_sha(binding_raw.get("window_semantic_sha256"))
                or not _valid_sha(binding_raw.get("attestation_sha256"))):
            raise TemporalDiscoveryContractError("rotating QD persisted lake semantic or attestation identity is invalid")
        try:
            binding = LakeWindowBinding.model_validate(binding_raw)
        except Exception as exc:
            raise TemporalDiscoveryContractError("rotating QD persisted lake binding is missing or malformed") from exc
        if not _valid_sha(binding.window_semantic_sha256) or not _valid_sha(binding.attestation_sha256):
            raise TemporalDiscoveryContractError("rotating QD persisted lake semantic or attestation identity is invalid")
        plan = plan_by_window.get(window_id)
        plan_binding = plan.get("lake_window_binding") if isinstance(plan, Mapping) else None
        if (not isinstance(plan_binding, Mapping)
                or plan_binding.get("window_semantic_sha256") != binding.window_semantic_sha256
                or plan_binding.get("request") != binding.request.canonical_payload()):
            raise TemporalDiscoveryContractError("rotating QD persisted lake semantic or coverage identity drifted")
        result[window_id] = binding
    if set(result) != set(expected):
        raise TemporalDiscoveryContractError("rotating QD persisted quarter binding coverage drifted")
    return result


def _checkpoint_path(root: Path, window_id: str) -> Path:
    return root / "binding-checkpoints" / f"{window_id}.json"


def _canonical_bytes(payload: Mapping[str, Any]) -> bytes:
    return (json.dumps(dict(payload), ensure_ascii=True, indent=2, sort_keys=True,
        allow_nan=False) + "\n").encode("utf-8")


def _fsync_directory(path: Path) -> None:
    """Best-effort directory durability; Windows commonly rejects this mode."""
    try:
        descriptor = os.open(str(path), os.O_RDONLY)
    except OSError:
        return
    try:
        os.fsync(descriptor)
    except OSError:
        pass
    finally:
        os.close(descriptor)


def _cleanup_orphan_checkpoint_temps(root: Path) -> None:
    directory = root / "binding-checkpoints"
    if not directory.is_dir():
        return
    # This exact private filename pattern is produced below.  It cannot match
    # a published checkpoint and is intentionally confined to this directory.
    for path in directory.glob(".*.checkpoint.tmp"):
        try:
            path.unlink()
        except FileNotFoundError:
            pass


def _publish_immutable_checkpoint(path: Path, payload: Mapping[str, Any]) -> None:
    """Atomically publish one checkpoint without ever replacing a final path.

    ``os.link`` is used instead of ``os.replace`` because the latter replaces
    an existing destination on Windows.  A hard-link create is atomic and
    fails if the final name already exists; the existing final is then only
    accepted when its canonical bytes are exactly identical.
    """
    encoded = _canonical_bytes(payload)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.parent / f".{path.name}.{uuid4().hex}.checkpoint.tmp"
    try:
        with temporary.open("xb") as handle:
            handle.write(encoded)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temporary, path)
        except FileExistsError:
            try:
                existing = path.read_bytes()
            except OSError as exc:
                raise TemporalDiscoveryContractError(
                    "could not verify existing immutable lake binding checkpoint"
                ) from exc
            if existing != encoded:
                raise TemporalDiscoveryContractError(
                    "refusing to overwrite divergent immutable lake binding checkpoint"
                )
        _fsync_directory(path.parent)
    finally:
        try:
            temporary.unlink()
        except FileNotFoundError:
            pass


def _checkpoint_payload(*, curriculum_sha256: str, worker: Mapping[str, str],
                        catalog_sha256: str, execution_identities: Sequence[Mapping[str, Any]],
                        window: Mapping[str, Any], binding: LakeWindowBinding) -> dict[str, Any]:
    payload = {
        "schemaVersion": BINDING_CHECKPOINT_SCHEMA,
        "curriculumSha256": curriculum_sha256, "workerContract": dict(worker),
        "catalogSha256": catalog_sha256, "executionIdentities": list(execution_identities),
        "windowId": window["windowId"], "analysisWindowStart": window["analysisWindowStart"],
        "analysisWindowEnd": window["analysisWindowEnd"],
        "remoteBinding": binding.model_dump(mode="json"),
    }
    payload["bindingCheckpointSha256"] = canonical_sha256(payload)
    return payload


def _write_binding_checkpoint(*, root: Path, curriculum_sha256: str,
                              worker: Mapping[str, str], catalog_sha256: str,
                              execution_identities: Sequence[Mapping[str, Any]],
                              window: Mapping[str, Any], binding: LakeWindowBinding) -> None:
    # One immutable file per successful remote attestation means a process
    # crash can never erase or replace a receipt it has already accepted.
    _publish_immutable_checkpoint(_checkpoint_path(root, str(window["windowId"])), _checkpoint_payload(
        curriculum_sha256=curriculum_sha256, worker=worker, catalog_sha256=catalog_sha256,
        execution_identities=execution_identities, window=window, binding=binding,
    ))


def _partial_root_bindings(*, root: Path, curriculum_sha256: str,
                           worker: Mapping[str, str], catalog_sha256: str,
                           execution_identities: Sequence[Mapping[str, Any]],
                           quarters: Sequence[Mapping[str, Any]]) -> dict[str, LakeWindowBinding] | None:
    """Recover only from a complete, immutable pre-template checkpoint set."""
    if not root.exists() or not any(root.iterdir()) or (root / "materialization.json").exists():
        return None
    _cleanup_orphan_checkpoint_temps(root)
    checkpoint_root = root / "binding-checkpoints"
    expected = {str(row["windowId"]): row for row in quarters}
    if not checkpoint_root.is_dir():
        raise TemporalDiscoveryContractError("rotating QD partial root lacks immutable lake binding checkpoints")
    files = {path.stem: path for path in checkpoint_root.glob("*.json")}
    if set(files) != set(expected):
        raise TemporalDiscoveryContractError("rotating QD partial root has incomplete immutable lake binding checkpoints")
    bindings: dict[str, LakeWindowBinding] = {}
    for window_id, window in expected.items():
        payload = _read(files[window_id], name="rotating QD lake binding checkpoint")
        material = dict(payload); supplied = material.pop("bindingCheckpointSha256", None)
        if (payload.get("schemaVersion") != BINDING_CHECKPOINT_SCHEMA or not _valid_sha(supplied)
                or canonical_sha256(material) != supplied):
            raise TemporalDiscoveryContractError("rotating QD immutable lake binding checkpoint identity drifted")
        if (payload.get("curriculumSha256") != curriculum_sha256 or payload.get("workerContract") != dict(worker)
                or payload.get("catalogSha256") != catalog_sha256 or payload.get("executionIdentities") != list(execution_identities)
                or any(payload.get(key) != window.get(key) for key in ("windowId", "analysisWindowStart", "analysisWindowEnd"))):
            raise TemporalDiscoveryContractError("rotating QD immutable lake binding checkpoint authority drifted")
        raw_binding = payload.get("remoteBinding")
        try:
            binding = LakeWindowBinding.model_validate(raw_binding)
        except Exception as exc:
            raise TemporalDiscoveryContractError("rotating QD immutable lake binding checkpoint is malformed") from exc
        if not _valid_sha(binding.window_semantic_sha256) or not _valid_sha(binding.attestation_sha256):
            raise TemporalDiscoveryContractError("rotating QD immutable lake binding checkpoint identity is invalid")
        bindings[window_id] = binding
    return bindings


def materialize_qd_rotating_evidence(*, rotating_evidence_input_path: Path | str,
    seed_population_path: Path | str, construction_catalog_path: Path | str, output_root: Path | str,
    worker_contract_sha256: str, worker_contract_schema: str, base_timeframe: str,
    bar_limit: int = 5000, max_attempts_per_task: int = DEFAULT_MAX_ATTEMPTS_PER_TASK,
    bidirectional_pair_config_path: Path | str | None = None,
    attestor=resolve_lake_window_binding) -> dict[str, Any]:
    """Pre-attest and freeze a four-year/16-quarter rotating development universe."""
    input_path = Path(rotating_evidence_input_path).resolve()
    catalog_path = Path(construction_catalog_path).resolve()
    root = _external_root(output_root)
    _cleanup_orphan_checkpoint_temps(root)
    raw = _read(input_path, name="rotating QD evidence input")
    if raw.get("schemaVersion") != ROTATING_EVIDENCE_INPUT_SCHEMA:
        raise TemporalDiscoveryContractError("unsupported rotating QD evidence input schema")
    core = build_rotating_evidence_contract(raw)
    quarters = _strict_curriculum(core)
    curriculum_sha = core["rotatingEvidenceSha256"]
    members, population_identity, population_payload = _seed_members(seed_population_path)
    execution_identities = [{
        "memberId": str(member["memberId"]),
        "sourceProfileSha256": str(member["sourceProfileSha256"]),
        "executionCellSha256": _execution_cell_sha(member["sourceProfile"]),
    } for member in members]
    catalog, catalog_timeframes = _catalog(_read(catalog_path, name="frozen construction catalog"))
    worker = _worker_contract(worker_contract_sha256, worker_contract_schema)
    if (isinstance(max_attempts_per_task, bool)
            or not isinstance(max_attempts_per_task, int)
            or not 1 <= max_attempts_per_task <= 32):
        raise TemporalDiscoveryContractError(
            "rotating QD max_attempts_per_task must be an integer between 1 and 32"
        )
    pair = _pair_config_identity(bidirectional_pair_config_path)
    admitted, timeframe_policy = _admitted_timeframes(pair_identity=pair, catalog_timeframes=catalog_timeframes)
    pair_identity = {key: value for key, value in pair.items() if key != "payload"} if pair else None
    instrument, timeframe, limit = _candidate_geometry(members, base_timeframe=base_timeframe, bar_limit=bar_limit)
    if timeframe not in catalog_timeframes:
        raise TemporalDiscoveryContractError("base_timeframe is absent from the frozen construction catalog")
    variants = _variants(members, catalog_timeframes=catalog_timeframes, admitted_timeframes=admitted)
    capability, dependencies = _capability_envelope(catalog=catalog, catalog_timeframes=catalog_timeframes,
        admitted_timeframes=admitted, policy=timeframe_policy, members=members)
    catalog_sha256 = canonical_sha256(catalog)
    bindings = _persisted_bindings(
        root=root, curriculum_sha256=curriculum_sha, worker=worker,
        catalog_sha256=catalog_sha256,
        execution_identities=execution_identities, quarters=quarters,
    )
    if bindings is None:
        bindings = _partial_root_bindings(
            root=root, curriculum_sha256=curriculum_sha, worker=worker,
            catalog_sha256=catalog_sha256, execution_identities=execution_identities,
            quarters=quarters,
        )
    bindings = bindings or {}
    records: dict[str, list[dict[str, Any]]] = {}
    for window in quarters:
        request, reachable = _envelope_request(variants=variants, instrument=instrument, base_timeframe=timeframe,
            window=window, catalog=catalog, capability_dependencies=dependencies)
        binding = bindings.get(window["windowId"])
        if binding is None:
            binding = _attest(request, attestor=attestor)
            _write_binding_checkpoint(
                root=root, curriculum_sha256=curriculum_sha, worker=worker,
                catalog_sha256=catalog_sha256, execution_identities=execution_identities,
                window=window, binding=binding,
            )
        elif binding.request != request:
            raise TemporalDiscoveryContractError(
                "rotating QD persisted lake binding request/coverage drifted"
            )
        if not all(lake_window_request_contains(binding.request, row["request"]) for row in reachable):
            raise TemporalDiscoveryContractError("rotating QD attestation does not contain every reachable dependency")
        bindings[window["windowId"]] = binding
        records[window["windowId"]] = reachable
    master_path = root / "development-universe-template-preparation.json"
    panels = {str(panel["panelId"]): list(panel["windows"]) for panel in core["panels"]}
    paths = {"master": master_path, **{panel_id: root / f"{panel_id}-template-preparation.json" for panel_id in panels}}
    preparations = {"master": _preparation(label=f"temporal-qd-rotating-development-universe-{curriculum_sha[-12:]}",
        windows=quarters, members=members, instrument=instrument, base_timeframe=timeframe, bar_limit=limit,
        worker_contract=worker, bindings=bindings, curriculum_sha256=curriculum_sha,
        max_attempts_per_task=max_attempts_per_task)}
    for panel_id, windows in panels.items():
        preparations[panel_id] = _preparation(label=f"temporal-qd-rotating-{panel_id}-{curriculum_sha[-12:]}",
            windows=windows, members=members, instrument=instrument, base_timeframe=timeframe, bar_limit=limit,
            worker_contract=worker, bindings=bindings, curriculum_sha256=curriculum_sha,
            max_attempts_per_task=max_attempts_per_task)
    template_bindings = {panel_id: _template_record(paths[panel_id], preparations[panel_id]) for panel_id in panels}
    materialized_input = copy.deepcopy(raw); materialized_input["panelTemplates"] = template_bindings
    contract = build_rotating_evidence_contract(materialized_input)
    validate_rotating_evidence_contract(contract)
    for generation, panel_id in enumerate(sorted(panels), start=1):
        validate_generation_template(preparations[panel_id], contract, generation)
    manifest = {"schemaVersion": MATERIALIZATION_SCHEMA, "curriculumSha256": curriculum_sha,
        "rotatingEvidence": contract, "rotatingEvidenceInput": {"path": str(input_path), "fileSha256": _file_sha(input_path)},
        "seedPopulation": population_identity,
        "constructionCatalog": {"path": str(catalog_path), "catalogSha256": canonical_sha256(catalog), "fileSha256": _file_sha(catalog_path), "timeframes": list(catalog_timeframes)},
        "workerContract": worker, "bidirectionalPairRunConfig": pair_identity, "catalogCapabilityEnvelope": capability,
        "executionIdentities": execution_identities,
        "coverage": {"developmentYearCount": 4, "quarterCount": 16, "quarterMonths": 3,
            "requestedHorizonMonths": 3, "effectiveHorizonMonths": 3, "allQuarterCoverageExact": True,
            "maxAttemptsPerTask": max_attempts_per_task},
        "templates": {key: {**_template_record(paths[key], preparations[key]), "windowIds": [row["windowId"] for row in (quarters if key == "master" else panels[key])]} for key in preparations},
        "quarters": [{"windowId": row["windowId"], "analysisWindowStart": row["analysisWindowStart"], "analysisWindowEnd": row["analysisWindowEnd"],
            "requestedHorizonMonths": 3, "effectiveHorizonMonths": 3, "remoteBinding": bindings[row["windowId"]].model_dump(mode="json"), "reachableRequests": records[row["windowId"]]} for row in quarters],
        "researchScrutiny": {"validation": {**contract["researchScrutiny"]["validation"], "label": "research_scrutiny_not_untouched"},
            "scrutiny": {**contract["researchScrutiny"]["scrutiny"], "label": "research_scrutiny_not_untouched"}},
        "outerTail": {"analysisWindowStart": OUTER_TAIL_START, "touched": False, "label": "sole_untouched_evidence"},
        "remoteAttestationRequired": True}
    manifest["materializationSha256"] = canonical_sha256(manifest)
    result = {"schemaVersion": RESULT_SCHEMA, "outputRoot": str(root), "curriculumSha256": curriculum_sha,
        "rotatingEvidenceSha256": contract["rotatingEvidenceSha256"], "materializationSha256": manifest["materializationSha256"],
        "masterTemplatePreparationPath": str(master_path), "panelTemplatePreparationPaths": {key: str(paths[key]) for key in panels}}
    _write_once(root / "rotating-evidence-input.json", raw); _write_once(root / "seed-population.json", population_payload)
    _write_once(root / "construction-catalog.json", catalog)
    for key, preparation in preparations.items():
        _write_once(paths[key], preparation); _write_once(root / f"{key}-authority.json", build_authority(preparation))
    _write_once(root / "rotating-evidence-config.json", materialized_input)
    _write_once(root / "rotating-evidence-contract.json", contract); _write_once(root / "materialization.json", manifest)
    _write_once(root / "result.json", result)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Freeze remotely-attested 16-quarter rotating QD evidence.")
    parser.add_argument("--rotating-evidence-input", type=Path, required=True); parser.add_argument("--seed-population", type=Path, required=True)
    parser.add_argument("--construction-catalog", type=Path, required=True); parser.add_argument("--bidirectional-pair-config", type=Path)
    parser.add_argument("--worker-contract-sha256", required=True); parser.add_argument("--worker-contract-schema", required=True)
    parser.add_argument("--base-timeframe", required=True); parser.add_argument("--bar-limit", type=int, default=5000)
    parser.add_argument("--max-attempts-per-task", type=int, default=DEFAULT_MAX_ATTEMPTS_PER_TASK)
    parser.add_argument("--output-root", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    try:
        args = _parser().parse_args(argv)
        print(json.dumps(materialize_qd_rotating_evidence(rotating_evidence_input_path=args.rotating_evidence_input,
            seed_population_path=args.seed_population, construction_catalog_path=args.construction_catalog, output_root=args.output_root,
            worker_contract_sha256=args.worker_contract_sha256, worker_contract_schema=args.worker_contract_schema,
            base_timeframe=args.base_timeframe, bar_limit=args.bar_limit,
            max_attempts_per_task=args.max_attempts_per_task,
            bidirectional_pair_config_path=args.bidirectional_pair_config), indent=2, sort_keys=True))
        return 0
    except Exception as exc:
        print(json.dumps({"schemaVersion": "temporal_qd_rotating_evidence_materialization_error_v1", "errorType": type(exc).__name__, "message": str(exc)}, indent=2, sort_keys=True), flush=True)
        return 1


__all__ = ["MATERIALIZATION_SCHEMA", "materialize_qd_rotating_evidence", "main"]

if __name__ == "__main__":
    raise SystemExit(main())
