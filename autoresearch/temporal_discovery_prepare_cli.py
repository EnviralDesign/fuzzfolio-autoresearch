"""Build the frozen 256-program Stage 5E-0 discovery-pilot preparation."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import re
from typing import Any, Mapping

from .temporal_discovery import (
    TEMPORAL_DISCOVERY_PREPARATION_SCHEMA,
    TEMPORAL_SEARCH_VALIDATION_SCHEMA,
    TemporalDiscoveryContractError,
    _normalize_preparation,
    canonical_sha256,
)

TEMPORAL_DISCOVERY_PILOT_INPUT_SCHEMA = (
    "temporal_graph_discovery_pilot_input_v1"
)
TEMPORAL_DISCOVERY_PILOT_PROFILE = "stage5e0_256_program_pilot_v1"
_SHA40 = re.compile(r"^[0-9a-f]{40}$")


def _read(path: Path, *, name: str) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TemporalDiscoveryContractError(
            f"could not read {name}: {path}"
        ) from exc
    if not isinstance(payload, dict):
        raise TemporalDiscoveryContractError(f"{name} root must be an object")
    return payload


def _mapping(value: Any, *, name: str) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        raise TemporalDiscoveryContractError(f"{name} must be an object")
    return dict(value)


def _timestamp(value: Any, *, name: str) -> str:
    token = str(value or "").strip()
    if not token or "T" not in token or not token.endswith("Z"):
        raise TemporalDiscoveryContractError(
            f"{name} must be an explicit UTC ISO timestamp ending in Z"
        )
    return token


def build_pilot_preparation(
    pilot_input: Mapping[str, Any],
    *,
    base_directory: Path | str,
) -> dict[str, Any]:
    payload = _mapping(pilot_input, name="pilot input")
    required = {
        "schemaVersion",
        "authorityLabel",
        "fuzzfolioCommit",
        "workerContract",
        "instrument",
        "timeframe",
        "barLimit",
        "generatorSeed",
        "seeds",
        "windows",
        "prohibitedEvidence",
    }
    if set(payload) != required:
        raise TemporalDiscoveryContractError(
            f"pilot input must contain exactly {sorted(required)!r}"
        )
    if payload["schemaVersion"] != TEMPORAL_DISCOVERY_PILOT_INPUT_SCHEMA:
        raise TemporalDiscoveryContractError("unknown discovery pilot input schema")
    fuzz_commit = str(payload["fuzzfolioCommit"] or "").strip()
    if not _SHA40.fullmatch(fuzz_commit):
        raise TemporalDiscoveryContractError(
            "fuzzfolioCommit must be an exact lowercase commit SHA"
        )

    root = Path(base_directory).resolve()
    seeds_raw = payload["seeds"]
    if not isinstance(seeds_raw, list) or len(seeds_raw) < 3:
        raise TemporalDiscoveryContractError(
            "pilot requires at least three diverse seed profiles"
        )
    seeds: list[dict[str, Any]] = []
    for index, raw_seed in enumerate(seeds_raw):
        seed = _mapping(raw_seed, name=f"seeds[{index}]")
        if set(seed) != {"seedId", "profilePath"}:
            raise TemporalDiscoveryContractError(
                "seed entries require seedId and profilePath"
            )
        path = (root / str(seed["profilePath"])).resolve()
        seeds.append(
            {
                "seedId": str(seed["seedId"] or "").strip(),
                "sourceProfile": _read(path, name=f"seed profile {path}"),
            }
        )

    windows_raw = payload["windows"]
    if not isinstance(windows_raw, list) or len(windows_raw) != 4:
        raise TemporalDiscoveryContractError(
            "pilot requires exactly four development windows"
        )
    development_windows: list[dict[str, Any]] = []
    templates: list[dict[str, Any]] = []
    initial_ids: list[str] = []
    confirmation_ids: list[str] = []
    seen_ids: set[str] = set()
    for index, raw_window in enumerate(windows_raw):
        window = _mapping(raw_window, name=f"windows[{index}]")
        if set(window) != {
            "windowId",
            "analysisWindowStart",
            "analysisWindowEnd",
            "evidencePlanPath",
            "screeningStage",
        }:
            raise TemporalDiscoveryContractError(
                "window entries have a closed schema"
            )
        window_id = str(window["windowId"] or "").strip()
        if not window_id or window_id in seen_ids:
            raise TemporalDiscoveryContractError(
                "window IDs must be non-empty and unique"
            )
        seen_ids.add(window_id)
        stage = str(window["screeningStage"] or "").strip().lower()
        if stage not in {"initial", "confirmation"}:
            raise TemporalDiscoveryContractError(
                "screeningStage must be initial or confirmation"
            )
        (initial_ids if stage == "initial" else confirmation_ids).append(window_id)
        start = _timestamp(
            window["analysisWindowStart"],
            name=f"windows[{index}].analysisWindowStart",
        )
        end = _timestamp(
            window["analysisWindowEnd"],
            name=f"windows[{index}].analysisWindowEnd",
        )
        development_windows.append(
            {
                "windowId": window_id,
                "analysisWindowStart": start,
                "analysisWindowEnd": end,
            }
        )
        plan_path = (root / str(window["evidencePlanPath"])).resolve()
        plan = _read(plan_path, name=f"evidence plan {plan_path}")
        if plan.get("schema_version") != "fuzzfolio.replay-evidence-plan.v2":
            raise TemporalDiscoveryContractError(
                f"window {window_id!r} requires replay evidence plan v2"
            )
        plan_start = plan.get("analysis_window_start")
        plan_end = plan.get("analysis_window_end")
        if plan_start != start or plan_end != end:
            raise TemporalDiscoveryContractError(
                f"window {window_id!r} does not match its evidence plan dates"
            )
        templates.append({"windowId": window_id, "evidencePlan": plan})
    if len(initial_ids) != 2 or len(confirmation_ids) != 2:
        raise TemporalDiscoveryContractError(
            "pilot requires exactly two initial and two confirmation windows"
        )

    preparation = {
        "schemaVersion": TEMPORAL_DISCOVERY_PREPARATION_SCHEMA,
        "authorityLabel": str(payload["authorityLabel"] or "").strip(),
        "generator": {
            "seed": int(payload["generatorSeed"]),
            "targetUniquePrograms": 256,
            "deNovoFraction": 0.70,
            "maxProposalAttempts": 8192,
            "deNovoMutationCount": {"min": 3, "max": 6},
            "seedMutationCount": {"min": 1, "max": 2},
        },
        "validation": {
            "validatorSchema": TEMPORAL_SEARCH_VALIDATION_SCHEMA,
            "fuzzfolioCommit": fuzz_commit,
        },
        "workerContract": _mapping(
            payload["workerContract"], name="workerContract"
        ),
        "instrument": str(payload["instrument"] or "").strip().upper(),
        "timeframe": str(payload["timeframe"] or "").strip().upper(),
        "barLimit": int(payload["barLimit"]),
        "seeds": seeds,
        "developmentWindows": development_windows,
        "evidencePlanTemplates": templates,
        "prohibitedEvidence": payload["prohibitedEvidence"],
        "screening": {
            "initialWindowIds": initial_ids,
            "confirmationWindowIds": confirmation_ids,
            "economicArchiveSize": 64,
            "noveltyArchiveSize": 64,
            "confirmationCandidateCap": 96,
            "minimumTradesPerInitialWindowEconomic": 20,
            "minimumTotalTradesNovelty": 10,
            "finalEconomicArchiveSize": 32,
            "finalNoveltyArchiveSize": 32,
        },
        "bounds": {
            "maxCandidates": 256,
            "maxInitialTasks": 512,
            "maxConfirmationCandidates": 96,
            "maxConfirmationTasks": 192,
            "maxTotalTasks": 704,
            "maxAttempts": 2,
            "deadlineSeconds": 7200.0,
        },
    }
    # Full normalization is the authoritative pre-write validation.
    _normalize_preparation(preparation)
    return preparation


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Build the frozen Stage 5E-0 256-program discovery pilot."
    )
    parser.add_argument("--pilot-input", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        pilot_input = _read(args.pilot_input, name="pilot input")
        preparation = build_pilot_preparation(
            pilot_input,
            base_directory=args.pilot_input.parent,
        )
        encoded = json.dumps(
            preparation,
            indent=2,
            sort_keys=True,
            ensure_ascii=True,
            allow_nan=False,
        ) + "\n"
        args.output.parent.mkdir(parents=True, exist_ok=True)
        if args.output.exists() and args.output.read_text(encoding="utf-8") != encoded:
            raise TemporalDiscoveryContractError(
                "refusing to overwrite divergent pilot preparation"
            )
        args.output.write_text(encoded, encoding="utf-8")
        normalized = _normalize_preparation(preparation)
        print(
            json.dumps(
                {
                    "schemaVersion": "temporal_graph_discovery_pilot_build_v1",
                    "pilotProfile": TEMPORAL_DISCOVERY_PILOT_PROFILE,
                    "preparationPath": str(args.output.resolve()),
                    "preparationSha256": normalized["preparationSha256"],
                    "targetUniquePrograms": 256,
                    "initialTaskCeiling": 512,
                    "confirmationTaskCeiling": 192,
                    "totalTaskCeiling": 704,
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 0
    except Exception as exc:
        print(
            json.dumps(
                {
                    "schemaVersion": "temporal_graph_discovery_pilot_error_v1",
                    "errorType": type(exc).__name__,
                    "message": str(exc),
                },
                indent=2,
                sort_keys=True,
            )
        )
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
