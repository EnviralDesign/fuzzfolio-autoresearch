"""Materialize the remotely-attested evidence authority for a temporal QD run.

This module intentionally stops at freezing evidence.  It does not submit a
replay, derive a Lake digest, or generate a QD candidate.  A Lake binding is
accepted only when it is returned by ``resolve_lake_window_binding`` (or a
test replacement) for the exact conservative request assembled from the
frozen population/catalog authority.
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

from .evidence_plan import build_execution_cell_sha256, build_replay_evidence_plan, canonical_sha256
from .lake_window import LakeWindowBinding, LakeWindowRequest, lake_window_request_contains, parse_utc_timestamp, resolve_replay_lake_window_request
from .lake_window_client import resolve_lake_window_binding
from .temporal_discovery_base import TemporalDiscoveryContractError, _clone
from .temporal_qd_evidence_ladder import (
    EVIDENCE_LADDER_INPUT_SCHEMA,
    OUTER_TAIL_START,
    build_evidence_ladder,
    validate_template_discovery_windows,
    validate_template_stage_window,
)
from .temporal_stage5e7_v3_evidence_envelope import _catalog, _population_members, _reachable_profiles
from .temporal_indicator_learning_v1 import EVIDENCE_LOOKBACK_CHOICES
from .temporal_search import TEMPORAL_SEARCH_PREPARATION_SCHEMA, build_authority


MATERIALIZATION_SCHEMA = "temporal_qd_evidence_ladder_materialization_v1"
LADDER_CONFIG_SCHEMA = "temporal_qd_evidence_ladder_input_v1"
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
    raise TemporalDiscoveryContractError("QD evidence-ladder output root must be outside the repository")


def _worker_contract(sha256: str, schema: str) -> dict[str, str]:
    if not isinstance(sha256, str) or not _SHA.fullmatch(sha256):
        raise TemporalDiscoveryContractError("worker_contract_sha256 must be a canonical sha256 digest")
    normalized_schema = str(schema or "").strip()
    if not normalized_schema:
        raise TemporalDiscoveryContractError("worker_contract_schema is required")
    return {"workerContractSha256": sha256, "workerContractSchema": normalized_schema}


def _pair_config_identity(path: Path | str | None) -> dict[str, Any] | None:
    """Bind a supplied pair authority without reconstructing its local runtime.

    Reopening a pair run config through its runtime loader would inspect local
    Dashboard code and make an evidence freeze depend on mutable workstation
    state.  Its persisted content hash is the relevant authority here.
    """

    if path is None:
        return None
    resolved = Path(path).resolve()
    payload = _read(resolved, name="bidirectional pair run config")
    supplied = payload.pop("pairRunConfigSha256", None)
    if payload.get("schemaVersion") != "temporal_qd_bidirectional_pair_run_config_v1" or not isinstance(supplied, str) or canonical_sha256(payload) != supplied:
        raise TemporalDiscoveryContractError("bidirectional pair run config identity/schema mismatch")
    return {
        "path": str(resolved), "pairRunConfigSha256": supplied,
        "fileSha256": _file_sha(resolved), "payload": payload,
    }


def _admitted_timeframes(
    *, pair_identity: Mapping[str, Any] | None, catalog_timeframes: Sequence[str]
) -> tuple[tuple[str, ...], dict[str, Any]]:
    """Return the exact pair policy, or a catalog-backed conservative fallback.

    The fallback is safe because indicator-learning policies must themselves be
    catalog-backed.  A materialization without a closed pair authority cannot
    claim a narrower policy, so it deliberately attests the complete frozen
    catalog timeframe domain instead.
    """

    catalog_domain = tuple(sorted({str(value).strip().upper() for value in catalog_timeframes if str(value).strip()}))
    if pair_identity is None:
        return catalog_domain, {
            "source": "frozen_construction_catalog_full_domain_without_pair_config",
            "pairRunConfigSha256": None,
            "timeframes": list(catalog_domain),
        }
    payload = pair_identity.get("payload")
    if not isinstance(payload, Mapping):
        raise TemporalDiscoveryContractError("bidirectional pair run config capability policy is unavailable")

    policies: dict[str, tuple[str, ...]] = {}
    for direction in ("long", "short"):
        module = payload.get(f"{direction}Module")
        policy = module.get("indicatorPolicy") if isinstance(module, Mapping) else None
        raw = policy.get("timeframePolicy") if isinstance(policy, Mapping) else None
        if isinstance(raw, (str, bytes)) or not isinstance(raw, list) or not raw:
            raise TemporalDiscoveryContractError("bidirectional pair indicator timeframe policy is missing or malformed")
        normalized = tuple(str(value).strip().upper() for value in raw)
        if any(not value for value in normalized) or len(set(normalized)) != len(normalized):
            raise TemporalDiscoveryContractError("bidirectional pair indicator timeframe policy is malformed")
        policies[direction] = normalized
    if policies["long"] != policies["short"]:
        raise TemporalDiscoveryContractError("bidirectional pair indicator timeframe policies must be identical")
    admitted = policies["long"]
    if not set(admitted).issubset(catalog_domain):
        raise TemporalDiscoveryContractError("bidirectional pair indicator timeframe policy is not backed by the frozen construction catalog")
    return admitted, {
        "source": "bidirectional_pair_indicator_policy",
        "pairRunConfigSha256": pair_identity["pairRunConfigSha256"],
        "timeframes": list(admitted),
        "longIndicatorPolicySha256": (
            payload["longModule"]["indicatorPolicy"].get("policySha256")
        ),
        "shortIndicatorPolicySha256": (
            payload["shortModule"]["indicatorPolicy"].get("policySha256")
        ),
    }


def _seed_members(path: Path | str) -> tuple[list[dict[str, Any]], dict[str, str], dict[str, Any]]:
    resolved = Path(path).resolve()
    payload = _read(resolved, name="frozen QD seed population")
    members, population_sha = _population_members(payload)
    return members, {"path": str(resolved), "populationSha256": population_sha, "fileSha256": _file_sha(resolved)}, payload


def _candidate_geometry(members: Sequence[Mapping[str, Any]], *, base_timeframe: str, bar_limit: int) -> tuple[str, str, int]:
    if not members:
        raise TemporalDiscoveryContractError("QD evidence materialization requires seed candidates")
    timeframe = str(base_timeframe or "").strip().upper()
    if not timeframe:
        raise TemporalDiscoveryContractError("base_timeframe is required")
    if isinstance(bar_limit, bool) or not 10 <= int(bar_limit) <= 1_000_000:
        raise TemporalDiscoveryContractError("bar_limit is outside admitted bounds")
    instruments: set[str] = set()
    for member in members:
        profile = member["sourceProfile"]
        values = profile.get("instruments") if isinstance(profile, Mapping) else None
        if not isinstance(values, list) or len(values) != 1 or not str(values[0]).strip():
            raise TemporalDiscoveryContractError("every seed profile must bind exactly one instrument")
        instruments.add(str(values[0]).strip().upper())
    if len(instruments) != 1:
        raise TemporalDiscoveryContractError("one QD evidence authority may bind exactly one instrument")
    return next(iter(instruments)), timeframe, int(bar_limit)


def _variants(
    members: Sequence[Mapping[str, Any]], *, catalog_timeframes: Sequence[str],
    admitted_timeframes: Sequence[str],
) -> list[dict[str, Any]]:
    # The frozen catalog is the full reachable timeframe domain.  The helper
    # expands only graph-bound indicators, matching the actual temporal graph
    # construction operator and avoiding unrelated management/scalar scope.
    admitted = tuple(admitted_timeframes)
    result: list[dict[str, Any]] = []
    for member in members:
        for item in _reachable_profiles(member, catalog_timeframes=catalog_timeframes, admitted_timeframes=admitted):
            result.append({
                "memberId": member["memberId"],
                "memberOrigin": member["memberOrigin"],
                "sourceProfileSha256": member["sourceProfileSha256"],
                "variantId": item["variantId"],
                "variantProfileSha256": item["sourceProfileSha256"],
                "sourceProfile": item["sourceProfile"],
            })
    result.sort(key=lambda item: (item["memberId"], item["variantId"], item["variantProfileSha256"]))
    if len({(item["memberId"], item["variantId"]) for item in result}) != len(result):
        raise TemporalDiscoveryContractError("QD evidence variant identities collide")
    return result


def _capability_envelope(
    *, catalog: Mapping[str, Any], catalog_timeframes: Sequence[str],
    admitted_timeframes: Sequence[str], policy: Mapping[str, Any],
    members: Sequence[Mapping[str, Any]],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Enumerate every catalog insertion/substitution lake dependency.

    Candidate construction can retain an arbitrary accepted parent lookback
    during substitution, or create a new catalog-backed fuzzy instance.  The
    conservative bound must therefore cover the frozen policy choices, every
    active catalog default, and every concrete seed instance.
    """

    raw_indicators = catalog.get("indicators")
    if not isinstance(raw_indicators, list):
        raise TemporalDiscoveryContractError("frozen construction catalog requires an indicators array")
    policy_max_lookback = max(EVIDENCE_LOOKBACK_CHOICES)
    catalog_max_lookback = 0
    seed_max_lookback = 0
    dependencies: list[dict[str, Any]] = []
    indicators: list[dict[str, Any]] = []
    for raw in raw_indicators:
        meta = raw.get("meta") if isinstance(raw, Mapping) else None
        config = raw.get("config") if isinstance(raw, Mapping) else None
        indicator_id = str(meta.get("id") or "").strip() if isinstance(meta, Mapping) else ""
        if not indicator_id or not isinstance(config, Mapping) or config.get("isActive") is not True:
            continue
        try:
            required_padding = int(meta.get("requiredPaddingBars"))
        except (TypeError, ValueError) as exc:
            raise TemporalDiscoveryContractError(
                f"active catalog indicator {indicator_id!r} has invalid requiredPaddingBars"
            ) from exc
        if required_padding < 0:
            raise TemporalDiscoveryContractError(
                f"active catalog indicator {indicator_id!r} has negative requiredPaddingBars"
            )
        default_lookback = _nonnegative_lookback(
            config.get("lookbackBars"),
            field_name=f"active catalog indicator {indicator_id!r} lookbackBars",
        )
        catalog_max_lookback = max(catalog_max_lookback, default_lookback)
        indicators.append({
            "indicatorId": indicator_id, "requiredPaddingBars": required_padding,
            "defaultLookbackBars": default_lookback,
        })

    for member in members:
        profile = member.get("sourceProfile")
        indicators_raw = profile.get("indicators") if isinstance(profile, Mapping) else None
        if not isinstance(indicators_raw, list):
            raise TemporalDiscoveryContractError("seed profile indicators must be an array")
        for index, raw in enumerate(indicators_raw):
            config = raw.get("config") if isinstance(raw, Mapping) else None
            if not isinstance(config, Mapping):
                raise TemporalDiscoveryContractError("seed profile indicator config must be an object")
            seed_max_lookback = max(seed_max_lookback, _nonnegative_lookback(
                config.get("lookbackBars"),
                field_name=(
                    f"seed profile {member.get('memberId')!r} indicator {index} lookbackBars"
                ),
            ))

    max_lookback = max(policy_max_lookback, catalog_max_lookback, seed_max_lookback)
    for indicator in indicators:
        indicator_id = indicator["indicatorId"]
        for timeframe in admitted_timeframes:
            dependencies.append({
                "dependencyKind": "catalog_capability",
                "indicatorId": indicator_id,
                "timeframe": timeframe,
                "lookbackBars": max_lookback,
                # Use abbreviated metadata/config intentionally: the lake
                # resolver hydrates from ``catalog`` before computing scope.
                "sourceProfile": {"indicators": [{
                    "meta": {"id": indicator_id},
                    "config": {"isActive": True, "timeframe": timeframe, "lookbackBars": max_lookback},
                }]},
            })
    if not dependencies:
        raise TemporalDiscoveryContractError("frozen construction catalog has no active indicator capabilities")
    dependencies.sort(key=lambda item: (item["indicatorId"], item["timeframe"]))
    details = {
        "schemaVersion": "temporal_qd_catalog_capability_envelope_v1",
        "catalogTimeframes": list(catalog_timeframes),
        "admittedTimeframes": list(admitted_timeframes),
        "timeframePolicy": dict(policy),
        "lookbackBounds": {
            "policyChoicesMax": policy_max_lookback,
            "activeCatalogDefaultsMax": catalog_max_lookback,
            "seedProfilesMax": seed_max_lookback,
            "maxReachable": max_lookback,
        },
        "maxReachableEvidenceLookbackBars": max_lookback,
        "activeIndicators": sorted(indicators, key=lambda item: item["indicatorId"]),
        "dependencyCount": len(dependencies),
    }
    details["capabilityEnvelopeSha256"] = canonical_sha256(details)
    return details, dependencies


def _nonnegative_lookback(value: Any, *, field_name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int):
        raise TemporalDiscoveryContractError(f"{field_name} is invalid")
    if value < 0:
        raise TemporalDiscoveryContractError(f"{field_name} must be non-negative")
    return value


def _envelope_request(
    *, variants: Sequence[Mapping[str, Any]], instrument: str, base_timeframe: str,
    window: Mapping[str, str], catalog: Mapping[str, Any],
    capability_dependencies: Sequence[Mapping[str, Any]],
) -> tuple[LakeWindowRequest, list[dict[str, Any]]]:
    required: list[tuple[Mapping[str, Any], LakeWindowRequest]] = []
    for variant in variants:
        request = resolve_replay_lake_window_request(
            pairs=[instrument], base_timeframe=base_timeframe,
            profile_snapshot=variant["sourceProfile"],
            analysis_window_start=window["analysisWindowStart"],
            analysis_window_end=window["analysisWindowEnd"], frozen_catalog=catalog,
        )
        required.append((variant, request))
    for dependency in capability_dependencies:
        request = resolve_replay_lake_window_request(
            pairs=[instrument], base_timeframe=base_timeframe,
            profile_snapshot=dependency["sourceProfile"],
            analysis_window_start=window["analysisWindowStart"],
            analysis_window_end=window["analysisWindowEnd"], frozen_catalog=catalog,
        )
        required.append((dependency, request))
    if not required:
        raise TemporalDiscoveryContractError("QD evidence envelope has no reachable profile dependencies")
    first = required[0][1]
    if any(item.dataset != first.dataset or item.pairs != first.pairs or item.data_end != first.data_end or item.coverage_policy != first.coverage_policy for _, item in required):
        raise TemporalDiscoveryContractError("QD reachable dependencies do not share a calendar/instrument contract")
    envelope = LakeWindowRequest(
        dataset=first.dataset, pairs=first.pairs,
        timeframes=sorted({frame for _, item in required for frame in item.timeframes}),
        data_start=min(parse_utc_timestamp(item.data_start, field_name="QD dependency data_start") for _, item in required),
        data_end=first.data_end, coverage_policy=first.coverage_policy,
    )
    records = []
    for dependency, request in required:
        if dependency.get("dependencyKind") == "catalog_capability":
            record = {
                "dependencyKind": "catalog_capability",
                **{key: dependency[key] for key in ("indicatorId", "timeframe", "lookbackBars")},
            }
        else:
            record = {
                "dependencyKind": "seed_variant",
                **{key: dependency[key] for key in ("memberId", "memberOrigin", "sourceProfileSha256", "variantId", "variantProfileSha256")},
            }
        records.append({**record, "request": request.canonical_payload()})
    if not all(lake_window_request_contains(envelope, record["request"]) for record in records):
        raise TemporalDiscoveryContractError("QD evidence envelope failed to contain a reachable dependency")
    return envelope, records


def _attest(request: LakeWindowRequest, *, attestor: _Attestor) -> LakeWindowBinding:
    binding = attestor(request, legacy_selection_manifest_sha256=None)
    if not isinstance(binding, LakeWindowBinding) or binding.request != request:
        raise TemporalDiscoveryContractError("remote QD evidence attestor returned a forged or mismatched binding")
    if binding.attestation_sha256 is None:
        raise TemporalDiscoveryContractError("remote QD evidence attestor omitted canonical attestation_sha256")
    if binding.legacy_selection_manifest_sha256 is not None:
        raise TemporalDiscoveryContractError("remote QD evidence attestor unexpectedly supplied a legacy manifest")
    return binding


def _execution_cell_sha(profile: Mapping[str, Any]) -> str | None:
    execution = profile.get("executionConfig")
    if not isinstance(execution, Mapping):
        raise TemporalDiscoveryContractError("seed profile requires executionConfig")
    if execution.get("managementLibrary") is not None:
        return None
    exit_policy = execution.get("exitPolicy")
    if not isinstance(exit_policy, Mapping) or not isinstance(exit_policy.get("selectedCell"), Mapping):
        raise TemporalDiscoveryContractError("seed profile requires exitPolicy.selectedCell")
    return build_execution_cell_sha256(dict(exit_policy["selectedCell"]))


def _preparation(
    *, label: str, stage: str, windows: list[dict[str, str]], members: Sequence[Mapping[str, Any]],
    instrument: str, base_timeframe: str, bar_limit: int, catalog: Mapping[str, Any],
    worker_contract: Mapping[str, str], bindings: Mapping[str, LakeWindowBinding]
) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    for member in members:
        profile = _clone(member["sourceProfile"], name="QD evidence seed profile")
        inputs = []
        for window in windows:
            window_id = window["windowId"]
            role = "training" if stage == "discovery" else stage
            plan = build_replay_evidence_plan(
                evidence_role=role, selection_data_end=window["analysisWindowEnd"],
                analysis_window_start=window["analysisWindowStart"], analysis_window_end=window["analysisWindowEnd"],
                requested_horizon_months=(1 if stage == "discovery" else 12 if stage == "validation" else 36),
                profile_snapshot=profile, campaign_plan_id=f"temporal-qd-evidence-ladder-{stage}-v1",
                execution_cell_sha256=_execution_cell_sha(profile), lake_window_binding=bindings[window_id],
                data_availability_cutoff=window["analysisWindowEnd"], coverage_policy="require_complete",
            )
            inputs.append({"windowId": window_id, "evidencePlan": plan.model_dump(mode="json")})
        candidates.append({
            "candidateId": member["memberId"], "sourceProfile": profile,
            "sourceProfileSha256": member["sourceProfileSha256"], "instrument": instrument,
            "timeframe": base_timeframe, "barLimit": bar_limit, "windowInputs": inputs,
        })
    preparation = {
        "schemaVersion": TEMPORAL_SEARCH_PREPARATION_SCHEMA,
        "authorityLabel": label,
        "workerContract": dict(worker_contract),
        "candidates": candidates,
        "developmentWindows": windows,
        "prohibitedEvidence": [{
            "windowId": "untouched-outer-tail", "analysisWindowStart": OUTER_TAIL_START,
            "analysisWindowEnd": "9999-12-31T00:00:00Z", "reason": "reserved untouched outer tail",
        }],
        "bounds": {
            "maxCandidates": len(candidates), "maxDevelopmentWindows": len(windows),
            "maxTasks": len(candidates) * len(windows), "maxAttempts": max(2, len(candidates) * 2),
            "deadlineSeconds": 86400.0,
        },
    }
    # This confirms the output is the exact closed supervisor schema, rather
    # than merely a JSON envelope that resembles one.
    build_authority(preparation)
    return preparation


def materialize_qd_evidence_ladder(
    *, evidence_ladder_input_path: Path | str, seed_population_path: Path | str,
    construction_catalog_path: Path | str, output_root: Path | str,
    worker_contract_sha256: str, worker_contract_schema: str, base_timeframe: str,
    bar_limit: int = 5000, bidirectional_pair_config_path: Path | str | None = None,
    attestor: _Attestor = resolve_lake_window_binding,
) -> dict[str, Any]:
    """Freeze the three QD evidence preparations and their bound ladder config.

    Concrete seed profiles are mandatory: a pair run config defines operators
    but deliberately contains no compiled candidates, so it can be attached
    as an additional identity only and cannot be used to invent one here.
    """

    ladder_input_path = Path(evidence_ladder_input_path).resolve()
    catalog_path = Path(construction_catalog_path).resolve()
    root = _external_root(output_root)
    raw_ladder = _read(ladder_input_path, name="QD evidence ladder input")
    if raw_ladder.get("schemaVersion") != EVIDENCE_LADDER_INPUT_SCHEMA:
        raise TemporalDiscoveryContractError("unsupported QD evidence ladder input schema")
    ladder = build_evidence_ladder(raw_ladder)
    members, population_identity, population_payload = _seed_members(seed_population_path)
    catalog, catalog_timeframes = _catalog(_read(catalog_path, name="frozen construction catalog"))
    worker = _worker_contract(worker_contract_sha256, worker_contract_schema)
    pair_authority = _pair_config_identity(bidirectional_pair_config_path)
    admitted_timeframes, timeframe_policy = _admitted_timeframes(
        pair_identity=pair_authority, catalog_timeframes=catalog_timeframes,
    )
    pair_identity = (
        {key: value for key, value in pair_authority.items() if key != "payload"}
        if pair_authority is not None else None
    )
    instrument, timeframe, limit = _candidate_geometry(members, base_timeframe=base_timeframe, bar_limit=bar_limit)
    if timeframe not in catalog_timeframes:
        raise TemporalDiscoveryContractError("base_timeframe is absent from the frozen construction catalog")
    variants = _variants(
        members, catalog_timeframes=catalog_timeframes,
        admitted_timeframes=admitted_timeframes,
    )
    capability_envelope, capability_dependencies = _capability_envelope(
        catalog=catalog, catalog_timeframes=catalog_timeframes,
        admitted_timeframes=admitted_timeframes, policy=timeframe_policy,
        members=members,
    )

    stages = {
        "discovery": [
            {"windowId": f"discovery-{index + 1:02d}", **window}
            for index, window in enumerate(ladder["discovery"]["windows"])
        ],
        "validation": [{"windowId": "validation-12m", **ladder["validation"]["window"]}],
        "scrutiny": [{"windowId": "scrutiny-36m", **ladder["scrutiny"]["window"]}],
    }
    # Keep this check local so a future ladder implementation cannot silently
    # widen an evidence request into the protected post-2026-01-01 tail.
    if any(window["analysisWindowEnd"] > OUTER_TAIL_START for rows in stages.values() for window in rows):
        raise TemporalDiscoveryContractError("QD evidence materialization would touch the untouched outer tail")

    bindings: dict[str, dict[str, LakeWindowBinding]] = {}
    request_records: dict[str, dict[str, list[dict[str, Any]]]] = {}
    for stage, windows in stages.items():
        stage_bindings: dict[str, LakeWindowBinding] = {}
        stage_records: dict[str, list[dict[str, Any]]] = {}
        for window in windows:
            request, records = _envelope_request(
                variants=variants, instrument=instrument, base_timeframe=timeframe,
                window=window, catalog=catalog,
                capability_dependencies=capability_dependencies,
            )
            binding = _attest(request, attestor=attestor)
            if not all(lake_window_request_contains(binding.request, record["request"]) for record in records):
                raise TemporalDiscoveryContractError("remote QD evidence binding does not contain every reachable dependency")
            stage_bindings[window["windowId"]] = binding
            stage_records[window["windowId"]] = records
        bindings[stage] = stage_bindings
        request_records[stage] = stage_records

    preparations = {
        stage: _preparation(
            label=f"temporal-qd-{stage}-evidence-ladder-v1", stage=stage, windows=windows,
            members=members, instrument=instrument, base_timeframe=timeframe, bar_limit=limit,
            catalog=catalog, worker_contract=worker, bindings=bindings[stage],
        )
        for stage, windows in stages.items()
    }
    validate_template_discovery_windows(preparations["discovery"], ladder)
    validate_template_stage_window(preparations["validation"], ladder, stage="validation")
    validate_template_stage_window(preparations["scrutiny"], ladder, stage="scrutiny")

    paths = {
        "discovery": root / "discovery-template-preparation.json",
        "validation": root / "validation-12m-template-preparation.json",
        "scrutiny": root / "scrutiny-36m-template-preparation.json",
    }
    # The supervisor consumes this input-schema config directly.  Template
    # paths and contents are separately identity-bound, making replacement or
    # accidental re-materialization fail before launch.
    config = copy.deepcopy(raw_ladder)
    config.update({
        "discoveryTemplatePreparationPath": str(paths["discovery"]),
        "discoveryTemplatePreparationSha256": canonical_sha256(preparations["discovery"]),
        "discoveryTemplateAuthorityId": build_authority(preparations["discovery"])["authorityId"],
        "validationTemplatePreparationPath": str(paths["validation"]),
        "validationTemplatePreparationSha256": canonical_sha256(preparations["validation"]),
        "validationTemplateAuthorityId": build_authority(preparations["validation"])["authorityId"],
        "scrutinyTemplatePreparationPath": str(paths["scrutiny"]),
        "scrutinyTemplatePreparationSha256": canonical_sha256(preparations["scrutiny"]),
        "scrutinyTemplateAuthorityId": build_authority(preparations["scrutiny"])["authorityId"],
        "evidenceLadderSha256": ladder["evidenceLadderSha256"],
        "materializationSchema": MATERIALIZATION_SCHEMA,
    })
    config["evidenceLadderConfigSha256"] = canonical_sha256(config)
    manifest = {
        "schemaVersion": MATERIALIZATION_SCHEMA,
        "evidenceLadder": ladder,
        "evidenceLadderInput": {"path": str(ladder_input_path), "fileSha256": _file_sha(ladder_input_path)},
        "seedPopulation": population_identity,
        "constructionCatalog": {"path": str(catalog_path), "catalogSha256": canonical_sha256(catalog), "fileSha256": _file_sha(catalog_path), "timeframes": list(catalog_timeframes)},
        "workerContract": worker,
        "bidirectionalPairRunConfig": pair_identity,
        "catalogCapabilityEnvelope": capability_envelope,
        "reachableMemberVariants": [{key: item[key] for key in ("memberId", "memberOrigin", "sourceProfileSha256", "variantId", "variantProfileSha256")} for item in variants],
        "stages": {
            stage: {
                "templatePath": str(paths[stage]), "templatePreparationSha256": canonical_sha256(preparations[stage]),
                "templateAuthorityId": build_authority(preparations[stage])["authorityId"],
                "windows": [{
                    "windowId": window["windowId"], "analysisWindowStart": window["analysisWindowStart"], "analysisWindowEnd": window["analysisWindowEnd"],
                    "remoteBinding": bindings[stage][window["windowId"]].model_dump(mode="json"),
                    "reachableRequests": request_records[stage][window["windowId"]],
                } for window in stages[stage]],
            } for stage in stages
        },
        "outerTail": {"analysisWindowStart": OUTER_TAIL_START, "touched": False, "reservedEvidencePermitted": False},
        "remoteAttestationRequired": True,
    }
    manifest["materializationSha256"] = canonical_sha256(manifest)
    result = {
        "schemaVersion": "temporal_qd_evidence_ladder_materialization_result_v1",
        "outputRoot": str(root), "evidenceLadderConfigPath": str(root / "evidence-ladder-config.json"),
        "evidenceLadderConfigSha256": config["evidenceLadderConfigSha256"],
        "materializationSha256": manifest["materializationSha256"],
        "discoveryTemplatePreparationPath": str(paths["discovery"]),
        "validationTemplatePreparationPath": str(paths["validation"]),
        "scrutinyTemplatePreparationPath": str(paths["scrutiny"]),
    }
    _write_once(root / "evidence-ladder-input.json", raw_ladder)
    _write_once(root / "seed-population.json", population_payload)
    _write_once(root / "construction-catalog.json", catalog)
    for stage, path in paths.items():
        _write_once(path, preparations[stage])
        _write_once(root / f"{stage}-authority.json", build_authority(preparations[stage]))
    _write_once(root / "evidence-ladder-config.json", config)
    _write_once(root / "materialization.json", manifest)
    _write_once(root / "result.json", result)
    return result


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Freeze the remotely-attested 3m/12m/36m temporal-QD evidence ladder.")
    parser.add_argument("--evidence-ladder-input", type=Path, required=True)
    parser.add_argument("--seed-population", type=Path, required=True, help="frozen concrete QD seed population; pair authority alone cannot mint profiles")
    parser.add_argument("--construction-catalog", type=Path, required=True)
    parser.add_argument("--bidirectional-pair-config", type=Path, help="optional closed pair-run authority bound into the manifest")
    parser.add_argument("--worker-contract-sha256", required=True)
    parser.add_argument("--worker-contract-schema", required=True)
    parser.add_argument("--base-timeframe", required=True)
    parser.add_argument("--bar-limit", type=int, default=5000)
    parser.add_argument("--output-root", type=Path, required=True)
    return parser


def main(argv: list[str] | None = None) -> int:
    try:
        args = _parser().parse_args(argv)
        print(json.dumps(materialize_qd_evidence_ladder(
            evidence_ladder_input_path=args.evidence_ladder_input, seed_population_path=args.seed_population,
            construction_catalog_path=args.construction_catalog, output_root=args.output_root,
            worker_contract_sha256=args.worker_contract_sha256, worker_contract_schema=args.worker_contract_schema,
            base_timeframe=args.base_timeframe, bar_limit=args.bar_limit,
            bidirectional_pair_config_path=args.bidirectional_pair_config,
        ), indent=2, sort_keys=True, ensure_ascii=True))
        return 0
    except Exception as exc:
        print(json.dumps({"schemaVersion": "temporal_qd_evidence_ladder_materialization_error_v1", "errorType": type(exc).__name__, "message": str(exc)}, indent=2, sort_keys=True), flush=True)
        return 1


__all__ = ["LADDER_CONFIG_SCHEMA", "MATERIALIZATION_SCHEMA", "materialize_qd_evidence_ladder", "main"]


if __name__ == "__main__":
    raise SystemExit(main())
