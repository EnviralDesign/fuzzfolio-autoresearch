"""Deterministic progressive discovery above the immutable temporal worker contract.

The existing ``temporal_search`` module owns a finite candidate/window authority
and the materialization/acknowledgement lifecycle.  This module owns the layer
above it: proposal, authoritative pre-market validation, program deduplication,
progressive window screening, and transparent economic/novelty archives.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
import copy
from datetime import datetime
import json
import math
import os
from pathlib import Path
import random
import re
import subprocess
import tempfile
from typing import Any, Protocol

from .result_codec import ResultCodecError, read_json_object as _read_codec_json_object
from .temporal_search import (
    TEMPORAL_SEARCH_PREPARATION_SCHEMA,
    TemporalSearchContractError,
    build_authority,
    canonical_sha256,
    validate_authority,
)

TEMPORAL_DISCOVERY_PREPARATION_SCHEMA = (
    "temporal_graph_discovery_preparation_v1"
)
TEMPORAL_DISCOVERY_AUTHORITY_SCHEMA = "temporal_graph_discovery_authority_v1"
TEMPORAL_DISCOVERY_POPULATION_SCHEMA = "temporal_graph_discovery_population_v1"
TEMPORAL_DISCOVERY_GENERATION_JOURNAL_SCHEMA = (
    "temporal_graph_discovery_generation_journal_v1"
)
TEMPORAL_DISCOVERY_INITIAL_SELECTION_SCHEMA = (
    "temporal_graph_discovery_initial_selection_v1"
)
TEMPORAL_DISCOVERY_FINAL_REPORT_SCHEMA = (
    "temporal_graph_discovery_final_report_v1"
)
TEMPORAL_DISCOVERY_MANIFEST_SCHEMA = "temporal_graph_discovery_manifest_v1"
TEMPORAL_DISCOVERY_GENERATOR_VERSION = "temporal_discovery_generator_v1"
TEMPORAL_DISCOVERY_SELECTION_VERSION = "temporal_discovery_selection_v1"
TEMPORAL_SEARCH_VALIDATION_SCHEMA = "temporal_search_candidate_validation_v1"

_SAFE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._:-]{0,159}$")
_CANDIDATE = re.compile(r"^[a-z][a-z0-9_]{0,239}$")
_SHA = re.compile(r"^sha256:[0-9a-f]{64}$")

_MUTATION_FAMILIES = (
    "entry_context",
    "graph_structure",
    "management_closure",
)
_THRESHOLD_GRID = (15.0, 25.0, 35.0, 45.0, 55.0, 65.0, 75.0, 85.0)
_EVENT_GRID = (0, 1, 2, 3, 5, 8, 13, 21)
_POS_AGE_GRID = (1, 2, 3, 5, 8, 13, 21, 34)
_R_GRID = (-1.0, -0.5, -0.25, 0.0, 0.5, 1.0, 1.5, 2.0, 3.0)
_STOP_PERCENT_GRID = (0.25, 0.5, 0.75, 1.0, 1.5, 2.0, 3.0)
_TARGET_R_GRID = (0.5, 1.0, 1.5, 2.0, 3.0, 4.0, 6.0)
_TARGET_PERCENT_GRID = (0.5, 1.0, 1.5, 2.0, 3.0, 5.0)
_DISTANCE_MULTIPLE_GRID = (0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 4.0)
_TRAIL_R_GRID = (0.25, 0.5, 0.75, 1.0, 1.5, 2.0)
_TRAIL_PERCENT_GRID = (0.25, 0.5, 0.75, 1.0, 1.5, 2.0)
_MIN_STEP_GRID = (0.0, 0.1, 0.25, 0.5, 1.0)
_TIME_WINDOWS = (
    (0, 360),
    (360, 720),
    (420, 960),
    (720, 1080),
    (780, 1260),
    (1080, 1439),
)


class TemporalDiscoveryError(RuntimeError):
    pass


class TemporalDiscoveryContractError(TemporalDiscoveryError):
    pass


class TemporalDiscoveryInfrastructureError(TemporalDiscoveryContractError):
    """An external authority or transport failed outside candidate semantics.

    Callers may treat an ordinary contract error as a deterministic rejected
    operator.  Infrastructure failures must instead abort the transaction so
    a dead validator cannot become immutable research evidence.
    """

    pass


class TemporalDiscoveryGenerationExhausted(TemporalDiscoveryError):
    pass


class CandidateValidatorProtocol(Protocol):
    def validate(
        self,
        *,
        candidate_id: str,
        source_profile: Mapping[str, Any],
        expected_raw_source_profile_sha256: str,
    ) -> dict[str, Any]: ...


def _clone(value: Any, *, name: str) -> Any:
    try:
        return json.loads(
            json.dumps(
                value,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
                allow_nan=False,
            )
        )
    except (TypeError, ValueError) as exc:
        raise TemporalDiscoveryContractError(
            f"{name} must be finite canonical JSON"
        ) from exc


def _mapping(value: Any, *, name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise TemporalDiscoveryContractError(f"{name} must be an object")
    return _clone(dict(value), name=name)


def _safe(value: Any, *, name: str) -> str:
    token = str(value or "").strip()
    if not _SAFE.fullmatch(token):
        raise TemporalDiscoveryContractError(
            f"{name} must be a safe explicit identifier"
        )
    return token


def _sha(value: Any, *, name: str) -> str:
    token = str(value or "").strip()
    if not _SHA.fullmatch(token):
        raise TemporalDiscoveryContractError(
            f"{name} must be an exact sha256 identity"
        )
    return token


def _integer(
    value: Any,
    *,
    name: str,
    minimum: int,
    maximum: int,
) -> int:
    if isinstance(value, bool):
        raise TemporalDiscoveryContractError(f"{name} must be an integer")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise TemporalDiscoveryContractError(
            f"{name} must be an integer"
        ) from exc
    if not minimum <= result <= maximum:
        raise TemporalDiscoveryContractError(
            f"{name} must be between {minimum} and {maximum}"
        )
    return result


def _number(
    value: Any,
    *,
    name: str,
    minimum: float,
    maximum: float,
) -> float:
    if isinstance(value, bool):
        raise TemporalDiscoveryContractError(f"{name} must be numeric")
    try:
        result = float(value)
    except (TypeError, ValueError) as exc:
        raise TemporalDiscoveryContractError(
            f"{name} must be numeric"
        ) from exc
    if not math.isfinite(result) or not minimum <= result <= maximum:
        raise TemporalDiscoveryContractError(
            f"{name} must be finite and between {minimum} and {maximum}"
        )
    return result


def _write_immutable(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    encoded = (
        json.dumps(
            dict(payload),
            indent=2,
            sort_keys=True,
            ensure_ascii=True,
            allow_nan=False,
        )
        + "\n"
    )
    if path.exists() and path.read_text(encoding="utf-8") != encoded:
        raise TemporalDiscoveryContractError(
            f"refusing to overwrite divergent immutable file: {path}"
        )
    path.write_text(encoded, encoding="utf-8")


def _read_json(path: Path, *, name: str) -> dict[str, Any]:
    try:
        payload, _ = _read_codec_json_object(path)
    except ResultCodecError as exc:
        raise TemporalDiscoveryContractError(
            f"could not read {name}: {path}"
        ) from exc
    return _mapping(payload, name=name)


def _pointer(path: Sequence[str | int]) -> str:
    return "/" + "/".join(
        str(item).replace("~", "~0").replace("/", "~1") for item in path
    )


def _walk(value: Any, path: tuple[str | int, ...] = ()):
    yield path, value
    if isinstance(value, dict):
        for key in sorted(value):
            yield from _walk(value[key], (*path, key))
    elif isinstance(value, list):
        for index, item in enumerate(value):
            yield from _walk(item, (*path, index))


def _get(root: Any, path: Sequence[str | int]) -> Any:
    current = root
    for item in path:
        current = current[item]
    return current


def _set(root: Any, path: Sequence[str | int], value: Any) -> None:
    if not path:
        raise TemporalDiscoveryContractError("cannot replace document root")
    parent = _get(root, path[:-1])
    parent[path[-1]] = _clone(value, name="mutation replacement")


def _mutation(
    *,
    family: str,
    operator: str,
    path: Sequence[str | int],
    replacement: Any,
) -> dict[str, Any]:
    return {
        "family": family,
        "operator": operator,
        "path": _pointer(path),
        "_path": tuple(path),
        "replacement": _clone(replacement, name="mutation replacement"),
    }


def _different(current: Any, candidate: Any) -> bool:
    return canonical_sha256(current) != canonical_sha256(candidate)


def _ensure_explicit_management(profile: dict[str, Any]) -> dict[str, Any]:
    output = _clone(profile, name="seed sourceProfile")
    config = output.get("executionConfig")
    if not isinstance(config, dict):
        raise TemporalDiscoveryContractError(
            "seed profile requires executionConfig"
        )
    if isinstance(config.get("managementLibrary"), dict):
        return output

    selected = (
        config.get("exitPolicy", {}).get("selectedCell")
        if isinstance(config.get("exitPolicy"), dict)
        else None
    )
    if not isinstance(selected, dict):
        raise TemporalDiscoveryContractError(
            "seed profile requires managementLibrary or legacy selectedCell"
        )
    stop = float(selected["stopLossPercent"])
    reward = float(selected["rewardMultiple"])
    upgraded = {
        key: value
        for key, value in config.items()
        if key != "exitPolicy"
    }
    upgraded["managementLibrary"] = {
        "version": "temporal_management_v1",
        "defaultPlanId": "search_plan",
        "plans": [
            {
                "id": "search_plan",
                "initialStop": {
                    "kind": "fixed_percent",
                    "percent": stop,
                },
                "initialTarget": {
                    "kind": "reward_multiple",
                    "multiple": reward,
                },
            }
        ],
    }
    output["executionConfig"] = upgraded
    for _, node in _walk(output.get("graph", {})):
        if isinstance(node, dict) and node.get("kind") == "enter_next_open":
            node.setdefault("managementPlanId", "search_plan")
    return output




__all__ = ['TEMPORAL_DISCOVERY_PREPARATION_SCHEMA', 'TEMPORAL_DISCOVERY_AUTHORITY_SCHEMA', 'TEMPORAL_DISCOVERY_POPULATION_SCHEMA', 'TEMPORAL_DISCOVERY_GENERATION_JOURNAL_SCHEMA', 'TEMPORAL_DISCOVERY_INITIAL_SELECTION_SCHEMA', 'TEMPORAL_DISCOVERY_FINAL_REPORT_SCHEMA', 'TEMPORAL_DISCOVERY_MANIFEST_SCHEMA', 'TEMPORAL_DISCOVERY_GENERATOR_VERSION', 'TEMPORAL_DISCOVERY_SELECTION_VERSION', 'TEMPORAL_SEARCH_VALIDATION_SCHEMA', '_SAFE', '_CANDIDATE', '_SHA', '_MUTATION_FAMILIES', '_THRESHOLD_GRID', '_EVENT_GRID', '_POS_AGE_GRID', '_R_GRID', '_STOP_PERCENT_GRID', '_TARGET_R_GRID', '_TARGET_PERCENT_GRID', '_DISTANCE_MULTIPLE_GRID', '_TRAIL_R_GRID', '_TRAIL_PERCENT_GRID', '_MIN_STEP_GRID', '_TIME_WINDOWS', 'TemporalDiscoveryError', 'TemporalDiscoveryContractError', 'TemporalDiscoveryInfrastructureError', 'TemporalDiscoveryGenerationExhausted', 'CandidateValidatorProtocol', '_clone', '_mapping', '_safe', '_sha', '_integer', '_number', '_write_immutable', '_read_json', '_pointer', '_walk', '_get', '_set', '_mutation', '_different', '_ensure_explicit_management', 'TEMPORAL_SEARCH_PREPARATION_SCHEMA', 'TemporalSearchContractError', 'build_authority', 'canonical_sha256', 'validate_authority']
