"""Prepare a remotely-attested broad evidence envelope for Stage 5E7-v3.

This is deliberately a *freeze* tool, not a replay/search loop.  It takes an
already valid temporal-search template as its structural authority, expands its
lake bindings so they conservatively contain every supplied parent and each
reachable graph-bound timeframe child, and obtains the new bindings only from
the remote lake attestor.  The resulting preparation remains an ordinary
``temporal-search`` preparation, so the existing panel bridge and QD
supervisor need no special execution path.
"""

from __future__ import annotations

import argparse
import copy
import hashlib
import json
import re
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

from .evidence_plan import build_replay_evidence_plan, canonical_sha256
from .lake_window import (
    LakeWindowBinding,
    LakeWindowRequest,
    lake_window_request_contains,
    parse_utc_timestamp,
    resolve_replay_lake_window_request,
)
from .lake_window_client import resolve_lake_window_binding
from .temporal_discovery_base import TemporalDiscoveryContractError, _clone
from .temporal_search import build_authority


EVIDENCE_ENVELOPE_SCHEMA = "stage5e7_v3_broad_evidence_envelope_v1"
EVIDENCE_ENVELOPE_MANIFEST_SCHEMA = "stage5e7_v3_broad_evidence_envelope_manifest_v1"
_SHA = re.compile(r"^sha256:[0-9a-f]{64}$")
_Attestor = Callable[..., LakeWindowBinding]


def _read(path: Path | str, *, name: str) -> dict[str, Any]:
    try:
        value = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TemporalDiscoveryContractError(f"could not read {name}: {path}") from exc
    if not isinstance(value, Mapping):
        raise TemporalDiscoveryContractError(f"{name} root must be an object")
    return _clone(value, name=name)


def _write_once(path: Path, payload: Mapping[str, Any]) -> None:
    encoded = json.dumps(dict(payload), ensure_ascii=True, indent=2, sort_keys=True, allow_nan=False) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.read_text(encoding="utf-8") != encoded:
        raise TemporalDiscoveryContractError(f"refusing to overwrite divergent immutable file: {path}")
    if not path.exists():
        path.write_text(encoded, encoding="utf-8")


def _file_sha(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _external_root(output_root: Path | str) -> Path:
    root = Path(output_root).expanduser().resolve()
    repository = Path(__file__).resolve().parents[1]
    try:
        root.relative_to(repository)
    except ValueError:
        return root
    raise TemporalDiscoveryContractError("evidence-envelope output root must be outside the repository")


def _catalog(payload: Mapping[str, Any]) -> tuple[dict[str, Any], tuple[str, ...]]:
    catalog = _clone(payload, name="frozen construction catalog")
    frames = catalog.get("timeframes")
    if not isinstance(frames, Mapping) or not frames:
        raise TemporalDiscoveryContractError("frozen construction catalog requires non-empty timeframes")
    timeframes = tuple(sorted({str(item).strip().upper() for item in frames if str(item).strip()}))
    if not timeframes:
        raise TemporalDiscoveryContractError("frozen construction catalog timeframes are malformed")
    indicators = catalog.get("indicators")
    if not isinstance(indicators, list) or not indicators:
        raise TemporalDiscoveryContractError("frozen construction catalog requires indicators")
    # Delegate complete active-indicator authority checks to lake_window's
    # catalog hydrator; this check only prevents an empty/minimal impostor from
    # reaching that path.
    return catalog, timeframes


def _population_members(payload: Mapping[str, Any]) -> tuple[list[dict[str, Any]], str]:
    candidates = payload.get("candidates")
    if not isinstance(candidates, list) or not candidates:
        raise TemporalDiscoveryContractError("seed population requires non-empty candidates")
    supplied = payload.get("populationSha256")
    material = _clone(payload, name="seed population")
    if supplied is not None:
        material.pop("populationSha256", None)
        if not isinstance(supplied, str) or not _SHA.fullmatch(supplied) or canonical_sha256(material) != supplied:
            raise TemporalDiscoveryContractError("seed population identity mismatch")
        population_sha = supplied
    else:
        population_sha = canonical_sha256(material)
    members: list[dict[str, Any]] = []
    seen: set[str] = set()
    for index, raw in enumerate(candidates):
        if not isinstance(raw, Mapping):
            raise TemporalDiscoveryContractError(f"seed population candidate {index} is malformed")
        candidate_id = str(raw.get("candidateId") or "").strip()
        profile = raw.get("sourceProfile")
        profile_sha = raw.get("sourceProfileSha256")
        if not candidate_id or candidate_id in seen or not isinstance(profile, Mapping):
            raise TemporalDiscoveryContractError("seed population candidate identities/profiles are invalid")
        if not isinstance(profile_sha, str) or canonical_sha256(profile) != profile_sha:
            raise TemporalDiscoveryContractError("seed population candidate source-profile identity mismatch")
        seen.add(candidate_id)
        members.append({
            "memberId": candidate_id,
            "sourceProfile": _clone(profile, name="seed population source profile"),
            "sourceProfileSha256": profile_sha,
            "memberOrigin": "seed_population",
        })
    return members, population_sha


def _graph_bound_instance_ids(profile: Mapping[str, Any]) -> set[str]:
    graph = profile.get("graph")
    if not isinstance(graph, Mapping):
        return set()
    bound: set[str] = set()
    for group in graph.get("evidenceGroups") or []:
        if isinstance(group, Mapping):
            bound.update(str(value) for value in group.get("indicatorInstanceIds") or [] if str(value))
    for binding in graph.get("eventBindings") or []:
        if isinstance(binding, Mapping) and binding.get("indicatorInstanceId"):
            bound.add(str(binding["indicatorInstanceId"]))
    return bound


def _reachable_profiles(
    member: Mapping[str, Any], *, catalog_timeframes: Sequence[str], admitted_timeframes: Sequence[str]
) -> list[dict[str, Any]]:
    """Return the parent plus every one-step reachable graph-timeframe child.

    Only graph-bound instances are eligible, matching the enabled v3
    construction operator.  Scalar/direction/management constructions do not
    alter lake scope, so including them would duplicate the same request.
    """

    parent = _clone(member["sourceProfile"], name="envelope member profile")
    variants = [{
        "variantId": "parent",
        "sourceProfile": parent,
        "sourceProfileSha256": member["sourceProfileSha256"],
    }]
    bound = _graph_bound_instance_ids(parent)
    indicators = parent.get("indicators") or []
    if not isinstance(indicators, list):
        raise TemporalDiscoveryContractError("envelope member indicators must be an array")
    for index, indicator in enumerate(indicators):
        if not isinstance(indicator, Mapping):
            raise TemporalDiscoveryContractError("envelope member indicator is malformed")
        meta, config = indicator.get("meta"), indicator.get("config")
        instance_id = str(meta.get("instanceId") or "") if isinstance(meta, Mapping) else ""
        current = str(config.get("timeframe") or "").strip().upper() if isinstance(config, Mapping) else ""
        if instance_id not in bound:
            continue
        if not current or current not in catalog_timeframes:
            raise TemporalDiscoveryContractError("graph-bound indicator timeframe is absent from frozen catalog")
        if current not in admitted_timeframes:
            raise TemporalDiscoveryContractError("graph-bound indicator timeframe is outside the admitted evidence allowlist")
        for replacement in admitted_timeframes:
            if replacement == current:
                continue
            child = copy.deepcopy(parent)
            child_indicator = child["indicators"][index]
            child_indicator["config"]["timeframe"] = replacement
            variants.append({
                "variantId": f"graph_timeframe:{instance_id}:{current}->{replacement}",
                "sourceProfile": child,
                "sourceProfileSha256": canonical_sha256(child),
            })
    return variants


def _window_envelope_request(
    *,
    member_variants: Sequence[Mapping[str, Any]],
    template_plan: Mapping[str, Any],
    instrument: str,
    base_timeframe: str,
    frozen_catalog: Mapping[str, Any],
    admitted_timeframes: Sequence[str],
) -> tuple[LakeWindowRequest, list[dict[str, Any]]]:
    plan = dict(template_plan)
    try:
        binding = LakeWindowBinding.model_validate(plan.get("lake_window_binding"))
    except (TypeError, ValueError) as exc:
        raise TemporalDiscoveryContractError("source template requires a valid v2 lake binding") from exc
    requests: list[dict[str, Any]] = []
    for member in member_variants:
        request = resolve_replay_lake_window_request(
            pairs=[instrument],
            base_timeframe=base_timeframe,
            profile_snapshot=member["sourceProfile"],
            analysis_window_start=str(plan.get("analysis_window_start") or ""),
            analysis_window_end=str(plan.get("analysis_window_end") or ""),
            frozen_catalog=frozen_catalog,
        )
        requests.append({
            "memberId": member["memberId"],
            "memberOrigin": member["memberOrigin"],
            "sourceProfileSha256": member["sourceProfileSha256"],
            "variantId": member["variantId"],
            "variantProfileSha256": member["variantProfileSha256"],
            "request": request.canonical_payload(),
        })
    if not requests:
        raise TemporalDiscoveryContractError("evidence envelope has no member requests")
    parsed = [LakeWindowRequest.model_validate(item["request"]) for item in requests]
    first = parsed[0]
    if any(item.dataset != first.dataset or item.pairs != first.pairs or item.data_end != first.data_end or item.coverage_policy != first.coverage_policy for item in parsed):
        raise TemporalDiscoveryContractError("member lake requests do not share one instrument/calendar contract")
    if any(not set(item.timeframes).issubset(admitted_timeframes) for item in parsed):
        raise TemporalDiscoveryContractError("member lake request requires a timeframe outside the admitted evidence allowlist")
    starts = [parse_utc_timestamp(item.data_start, field_name="member request data_start") for item in parsed]
    envelope = LakeWindowRequest(
        dataset=first.dataset,
        pairs=first.pairs,
        timeframes=sorted({timeframe for item in parsed for timeframe in item.timeframes}),
        data_start=min(starts),
        data_end=first.data_end,
        coverage_policy=first.coverage_policy,
    )
    # The old binding is not evidence for the wider envelope.  It may only be
    # used as a containment sanity check for the unmodified template itself.
    template_required = resolve_replay_lake_window_request(
        pairs=[instrument], base_timeframe=base_timeframe,
        profile_snapshot=member_variants[0]["sourceProfile"],
        analysis_window_start=str(plan.get("analysis_window_start") or ""),
        analysis_window_end=str(plan.get("analysis_window_end") or ""),
        frozen_catalog=frozen_catalog,
    )
    if not lake_window_request_contains(envelope, template_required):
        raise TemporalDiscoveryContractError("envelope construction failed to contain its source template dependency")
    _ = binding
    return envelope, requests


def _rebind_plan(plan: Mapping[str, Any], *, profile: Mapping[str, Any], binding: LakeWindowBinding) -> dict[str, Any]:
    if plan.get("schema_version") != "fuzzfolio.replay-evidence-plan.v2":
        raise TemporalDiscoveryContractError("source template requires replay evidence plan v2")
    try:
        rebuilt = build_replay_evidence_plan(
            evidence_role=str(plan["evidence_role"]),
            selection_data_end=plan["selection_data_end"],
            analysis_window_start=plan["analysis_window_start"],
            analysis_window_end=plan["analysis_window_end"],
            requested_horizon_months=int(plan["requested_horizon_months"]),
            profile_snapshot=dict(profile),
            campaign_plan_id=plan.get("campaign_plan_id"),
            execution_cell_sha256=plan.get("execution_cell_sha256"),
            lake_window_binding=binding,
            data_availability_cutoff=plan.get("data_availability_cutoff"),
            coverage_policy=str(plan.get("coverage_policy") or "require_complete"),
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise TemporalDiscoveryContractError("source template evidence plan is malformed") from exc
    return rebuilt.model_dump(mode="json")


def _template_members(template: Mapping[str, Any]) -> list[dict[str, Any]]:
    authority = build_authority(template)
    result = []
    for candidate in authority["candidates"]:
        result.append({
            "memberId": f"template:{candidate['candidateId']}",
            "sourceProfile": _clone(candidate["sourceProfile"], name="template candidate profile"),
            "sourceProfileSha256": candidate["sourceProfileSha256"],
            "memberOrigin": "source_template",
        })
    return result


def build_broad_evidence_envelope(
    *,
    source_preparation_path: Path | str,
    seed_population_path: Path | str,
    construction_catalog_path: Path | str,
    output_root: Path | str,
    worker_contract_sha256: str,
    worker_contract_schema: str,
    admitted_timeframes: Sequence[str],
    attestor: _Attestor = resolve_lake_window_binding,
) -> dict[str, Any]:
    """Freeze one broad v2-binding preparation and immutable evidence manifest."""

    source_path = Path(source_preparation_path).resolve()
    population_path = Path(seed_population_path).resolve()
    catalog_path = Path(construction_catalog_path).resolve()
    source = _read(source_path, name="source template preparation")
    source_authority = build_authority(source)
    if not isinstance(worker_contract_sha256, str) or not _SHA.fullmatch(worker_contract_sha256):
        raise TemporalDiscoveryContractError("worker_contract_sha256 must be a canonical sha256 digest")
    effective_worker_schema = str(worker_contract_schema or "").strip()
    if not effective_worker_schema:
        raise TemporalDiscoveryContractError("worker_contract_schema is required")
    population = _read(population_path, name="seed population")
    seed_members, population_sha = _population_members(population)
    catalog, catalog_timeframes = _catalog(_read(catalog_path, name="frozen construction catalog"))
    if isinstance(admitted_timeframes, (str, bytes)):
        raise TemporalDiscoveryContractError("admitted_timeframes must be a non-empty sequence of timeframe tokens")
    admitted = tuple(sorted({str(value).strip().upper() for value in admitted_timeframes if str(value).strip()}))
    if not admitted:
        raise TemporalDiscoveryContractError("admitted_timeframes must be non-empty")
    if not set(admitted).issubset(catalog_timeframes):
        raise TemporalDiscoveryContractError("admitted_timeframes contains a timeframe absent from frozen catalog")

    members = seed_members + _template_members(source)
    variants: list[dict[str, Any]] = []
    for member in members:
        for item in _reachable_profiles(member, catalog_timeframes=catalog_timeframes, admitted_timeframes=admitted):
            variants.append({
                "memberId": member["memberId"], "memberOrigin": member["memberOrigin"],
                "sourceProfileSha256": member["sourceProfileSha256"], "variantId": item["variantId"],
                "variantProfileSha256": item["sourceProfileSha256"], "sourceProfile": item["sourceProfile"],
            })
    variants.sort(key=lambda item: (item["memberId"], item["variantId"], item["variantProfileSha256"]))
    if len({(item["memberId"], item["variantId"]) for item in variants}) != len(variants):
        raise TemporalDiscoveryContractError("evidence-envelope variant identities collide")

    exemplar = source_authority["candidates"][0]
    if any(candidate["timeframe"] not in admitted for candidate in source_authority["candidates"]):
        raise TemporalDiscoveryContractError("source template decision timeframe is outside the admitted evidence allowlist")
    expected_instruments = [exemplar["instrument"]]
    for member in members:
        if member["sourceProfile"].get("instruments") != expected_instruments:
            raise TemporalDiscoveryContractError(
                "envelope members must exactly bind the source template instrument"
            )
    windows = source_authority["developmentWindows"]
    exemplar_inputs = {item["windowId"]: item["evidencePlan"] for item in exemplar["windowInputs"]}
    bindings: dict[str, LakeWindowBinding] = {}
    request_records: dict[str, list[dict[str, Any]]] = {}
    envelope_requests: dict[str, dict[str, Any]] = {}
    for window in windows:
        window_id = str(window["windowId"])
        envelope, requests = _window_envelope_request(
            member_variants=variants,
            template_plan=exemplar_inputs[window_id], instrument=exemplar["instrument"],
            base_timeframe=exemplar["timeframe"], frozen_catalog=catalog, admitted_timeframes=admitted,
        )
        legacy = LakeWindowBinding.model_validate(exemplar_inputs[window_id]["lake_window_binding"]).legacy_selection_manifest_sha256
        binding = attestor(envelope, legacy_selection_manifest_sha256=legacy)
        if not isinstance(binding, LakeWindowBinding) or binding.request != envelope:
            raise TemporalDiscoveryContractError("remote evidence attestor returned a forged or mismatched binding")
        if binding.attestation_sha256 is None:
            raise TemporalDiscoveryContractError("remote evidence attestor omitted canonical attestation_sha256")
        if binding.legacy_selection_manifest_sha256 != legacy:
            raise TemporalDiscoveryContractError("remote evidence attestor did not preserve legacy_selection_manifest_sha256")
        if not all(lake_window_request_contains(binding.request, request["request"]) for request in requests):
            raise TemporalDiscoveryContractError("remote envelope binding does not contain every member dependency")
        bindings[window_id] = binding
        request_records[window_id] = requests
        envelope_requests[window_id] = envelope.canonical_payload()

    preparation = copy.deepcopy(source)
    # Never inherit a stale worker image/contract merely because its source
    # template had valid evidence.  The caller must explicitly bind the exact
    # contract that will execute this new campaign.
    preparation["workerContract"] = {
        "workerContractSha256": worker_contract_sha256,
        "workerContractSchema": effective_worker_schema,
    }
    for candidate in preparation["candidates"]:
        profile = candidate["sourceProfile"]
        inputs = candidate["windowInputs"]
        for entry in inputs:
            window_id = str(entry["windowId"])
            entry["evidencePlan"] = _rebind_plan(entry["evidencePlan"], profile=profile, binding=bindings[window_id])
    output_authority = build_authority(preparation)
    root = _external_root(output_root)
    source_sha = canonical_sha256(source)
    catalog_sha = canonical_sha256(catalog)
    manifest = {
        "schemaVersion": EVIDENCE_ENVELOPE_MANIFEST_SCHEMA,
        "sourcePreparation": {"path": str(source_path), "preparationSha256": source_sha, "authorityId": source_authority["authorityId"]},
        "outputPreparation": {"preparationSha256": canonical_sha256(preparation), "authorityId": output_authority["authorityId"]},
        "constructionCatalog": {"path": str(catalog_path), "catalogSha256": catalog_sha, "catalogTimeframes": list(catalog_timeframes)},
        "admittedEvidenceTimeframes": list(admitted),
        # This envelope is catalog/evidence authority only.  The existing
        # panel bridge and QD supervisor independently reopen and verify their
        # own frozen construction catalog and generator/QD policy at launch.
        "policyBinding": "bound_separately_by_panel_bridge_and_qd_supervisor",
        "seedPopulation": {"path": str(population_path), "populationSha256": population_sha},
        "members": [
            {key: item[key] for key in ("memberId", "memberOrigin", "sourceProfileSha256")}
            for item in sorted(members, key=lambda item: item["memberId"])
        ],
        "memberVariants": [{key: item[key] for key in ("memberId", "memberOrigin", "sourceProfileSha256", "variantId", "variantProfileSha256")} for item in variants],
        "developmentWindows": [
            {
                "windowId": window_id,
                "envelopeRequest": envelope_requests[window_id],
                "remoteBinding": bindings[window_id].model_dump(mode="json"),
                "memberRequests": request_records[window_id],
            }
            for window_id in sorted(envelope_requests)
        ],
        "remoteAttestationRequired": True,
        "effectiveWorkerContract": _clone(output_authority["workerContract"], name="effective worker contract"),
        "reservedEvidencePermitted": False,
    }
    manifest["envelopeManifestSha256"] = canonical_sha256(manifest)
    result = {
        "schemaVersion": EVIDENCE_ENVELOPE_SCHEMA,
        "outputRoot": str(root),
        "preparationSha256": canonical_sha256(preparation),
        "authorityId": output_authority["authorityId"],
        "envelopeManifestSha256": manifest["envelopeManifestSha256"],
        "windowCount": len(windows),
        "memberCount": len(members),
        "memberVariantCount": len(variants),
        "catalogTimeframes": list(catalog_timeframes),
        "admittedEvidenceTimeframes": list(admitted),
    }
    _write_once(root / "source-preparation.json", source)
    _write_once(root / "seed-population.json", population)
    _write_once(root / "construction-catalog.json", catalog)
    _write_once(root / "preparation.json", preparation)
    _write_once(root / "authority.json", output_authority)
    _write_once(root / "evidence-envelope-manifest.json", manifest)
    _write_once(root / "result.json", result)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Freeze a broad remotely-attested Stage5E7-v3 evidence envelope.")
    parser.add_argument("--source-preparation", type=Path, required=True)
    parser.add_argument("--seed-population", type=Path, required=True)
    parser.add_argument("--construction-catalog", type=Path, required=True)
    parser.add_argument("--worker-contract-sha256", required=True)
    parser.add_argument("--worker-contract-schema", required=True)
    parser.add_argument("--admitted-timeframe", action="append", required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    try:
        args = _parser().parse_args(argv)
        print(json.dumps(build_broad_evidence_envelope(source_preparation_path=args.source_preparation, seed_population_path=args.seed_population, construction_catalog_path=args.construction_catalog, output_root=args.output_root, worker_contract_sha256=args.worker_contract_sha256, worker_contract_schema=args.worker_contract_schema, admitted_timeframes=args.admitted_timeframe), indent=2, sort_keys=True, ensure_ascii=True))
        return 0
    except Exception as exc:
        print(json.dumps({"schemaVersion": "stage5e7_v3_broad_evidence_envelope_error_v1", "errorType": type(exc).__name__, "message": str(exc)}, indent=2, sort_keys=True), flush=True)
        return 1


__all__ = ["EVIDENCE_ENVELOPE_MANIFEST_SCHEMA", "EVIDENCE_ENVELOPE_SCHEMA", "build_broad_evidence_envelope", "main"]


if __name__ == "__main__":
    raise SystemExit(main())
