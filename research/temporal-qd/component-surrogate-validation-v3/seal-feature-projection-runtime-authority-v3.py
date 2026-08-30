"""Seal the V3 component-only projection, runtime, and input-frame authority.

This program deliberately reads only the frozen, outcome-free census plus the
pinned historical engine.  It neither imports a TemporalGraph runtime nor has
an outcome input.  Its output is the compact authority record the later
extractor must verify before it reads an isolated bar file.
"""

from __future__ import annotations

import argparse
import hashlib
import inspect
import json
import platform
import subprocess
import sys
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd
import pyarrow

from fuzzfolio_core.models.common import INDICATORS_CONFIG
from fuzzfolio_core.models.indicator import Indicator, IndicatorConfig, IndicatorMeta
from fuzzfolio_core.rust_runtime import load_rust_core_module, rust_core_build_info
from fuzzfolio_core.scoring_engine.indicators.indicator_factory import get_indicator_instance


HISTORICAL_ENGINE_COMMIT = "2bd50ccb3af1700d286da88cbcaecb4aca24f1a2"
AUTORESEARCH_COMMIT = "51c2f9175f441166e7fc997109e939a9f9103b5d"
FEATURE_PROTOCOL_COMMIT = "0a5a74de695d98dce3b31e896a4c21d748471c04"
FEATURE_PROTOCOL_GIT_BLOB_SHA256 = "sha256:b412209520f0a1b8ea7bacf0d1f0e0bef1eda508f0e8096fdb8e31bc8b8c04cc"
PROJECTION_AUDIT_SHA256 = "sha256:9c3261e460b5a202c4ff31ff5c390cd80e62efc358c669e7daabc637c3c82b52"
AUTHORIZATION_SHA256 = "sha256:19e6db31a7439443ef59775145e4e2b42317e7a5be10af0de5d7b644e6304acf"
EXPECTED_CONTEXT_COUNT = 41
EXPECTED_COMPONENT_COUNT = 19

FEATURE_PROTOCOL_PATH = Path("research/temporal-qd/component-surrogate-validation-v2/feature-protocol-v2.json")
SOURCE_FILES = (
    Path("shared/constants/indicators.json"),
    Path("shared/python/fuzzfolio_core/fuzzfolio_core/models/common.py"),
    Path("shared/python/fuzzfolio_core/fuzzfolio_core/models/indicator.py"),
    Path("shared/python/fuzzfolio_core/fuzzfolio_core/compute/aligned_scoring.py"),
    Path("shared/python/fuzzfolio_core/fuzzfolio_core/compute/indicator_batch_execution.py"),
    Path("shared/python/fuzzfolio_core/fuzzfolio_core/scoring_engine/indicators/base_indicator.py"),
    Path("shared/python/fuzzfolio_core/fuzzfolio_core/scoring_engine/indicators/indicator_factory.py"),
    Path("shared/python/fuzzfolio_core/fuzzfolio_core/scoring_engine/market_utils.py"),
    Path("shared/python/fuzzfolio_core/fuzzfolio_core/temporal_graph/graph_models.py"),
    Path("shared/python/fuzzfolio_core/fuzzfolio_core/temporal_graph/observation_adapter.py"),
    Path("shared/python/fuzzfolio_core/fuzzfolio_core/rust_runtime.py"),
    Path("shared/python/fuzzfolio_core/fuzzfolio_core/talib_runtime.py"),
)


def canonical_bytes(payload: object) -> bytes:
    return json.dumps(
        payload,
        ensure_ascii=True,
        sort_keys=True,
        separators=(",", ":"),
        allow_nan=False,
    ).encode("utf-8")


def sha256_prefixed(payload: bytes) -> str:
    return "sha256:" + hashlib.sha256(payload).hexdigest()


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def under(path: Path, root: Path) -> bool:
    try:
        path.resolve().relative_to(root.resolve())
    except ValueError:
        return False
    return True


def check_json_self_hash(path: Path, field: str) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    expected = payload.pop(field, None)
    actual = sha256_prefixed(canonical_bytes(payload))
    if expected != actual:
        raise RuntimeError(f"{path.name} self-hash mismatch: {expected!r} != {actual!r}")
    payload[field] = expected
    return payload


def git_stdout(study_root: Path, *args: str) -> bytes:
    return subprocess.check_output(["git", "-C", str(study_root), *args])


def git_text(study_root: Path, *args: str) -> str:
    return git_stdout(study_root, *args).decode("utf-8").strip()


def verify_feature_protocol(study_root: Path) -> dict[str, str]:
    commit_blob = git_stdout(
        study_root,
        "show",
        f"{FEATURE_PROTOCOL_COMMIT}:{FEATURE_PROTOCOL_PATH.as_posix()}",
    )
    commit_sha = sha256_prefixed(commit_blob)
    if commit_sha != FEATURE_PROTOCOL_GIT_BLOB_SHA256:
        raise RuntimeError("feature protocol Git blob digest differs from the frozen authority")
    expected_blob_id = git_text(
        study_root,
        "rev-parse",
        f"{FEATURE_PROTOCOL_COMMIT}:{FEATURE_PROTOCOL_PATH.as_posix()}",
    )
    actual_blob_id = git_text(study_root, "hash-object", str(study_root / FEATURE_PROTOCOL_PATH))
    if actual_blob_id != expected_blob_id:
        raise RuntimeError("feature-protocol-v2.json differs from its frozen Git blob")
    return {
        "commit": FEATURE_PROTOCOL_COMMIT,
        "gitBlobObjectId": expected_blob_id,
        "gitBlobSha256": commit_sha,
        "worktreeMatchesFrozenGitBlob": True,
    }


def catalog_by_id() -> dict[str, dict[str, Any]]:
    catalog = INDICATORS_CONFIG.get("indicators")
    if not isinstance(catalog, list):
        raise RuntimeError("historical indicator catalog is malformed")
    items: dict[str, dict[str, Any]] = {}
    for item in catalog:
        meta = item.get("meta") if isinstance(item, dict) else None
        indicator_id = meta.get("id") if isinstance(meta, dict) else None
        if not isinstance(indicator_id, str) or indicator_id in items:
            raise RuntimeError("historical indicator catalog is missing a unique meta.id")
        items[indicator_id] = item
    return items


def component_runtime_pins(manifest: dict[str, Any], engine_root: Path) -> list[dict[str, Any]]:
    catalog = catalog_by_id()
    pins: dict[str, dict[str, Any]] = {}
    for context in manifest["contexts"]:
        component = context["component"]
        indicator_id = component["indicatorId"]
        catalog_item = catalog.get(indicator_id)
        if catalog_item is None:
            raise RuntimeError(f"frozen component is absent from historical catalog: {indicator_id}")
        meta = dict(catalog_item["meta"])
        meta["instanceId"] = component["indicatorInstanceId"]
        model = Indicator(
            meta=IndicatorMeta(**meta),
            docs=catalog_item.get("docs"),
            config=IndicatorConfig(**component["fullConfiguration"]),
        )
        instance = get_indicator_instance(model)
        source = Path(inspect.getsourcefile(type(instance)) or "").resolve()
        if not source.is_file() or not under(source, engine_root):
            raise RuntimeError(f"indicator implementation is outside pinned engine: {indicator_id}: {source}")
        runtime_pin = {
            "indicatorId": indicator_id,
            "implementationClass": type(instance).__name__,
            "implementationSource": source.relative_to(engine_root).as_posix(),
            "implementationSourceSha256": sha256_file(source),
            "requiredColumns": sorted(str(value) for value in getattr(instance, "required_columns", set())),
        }
        prior = pins.setdefault(indicator_id, runtime_pin)
        if prior != runtime_pin:
            raise RuntimeError(f"frozen contexts resolve {indicator_id} inconsistently")
    if len(pins) != EXPECTED_COMPONENT_COUNT:
        raise RuntimeError(f"component resolution count drift: {len(pins)}")
    return [pins[key] for key in sorted(pins)]


def imported_core_modules(engine_root: Path) -> list[dict[str, str]]:
    records: list[dict[str, str]] = []
    for name, module in sorted(sys.modules.items()):
        if not name.startswith("fuzzfolio_core"):
            continue
        source = getattr(module, "__file__", None)
        if source is None:
            continue
        path = Path(source).resolve()
        if not under(path, engine_root):
            raise RuntimeError(f"fuzzfolio_core import escaped pinned historical engine: {name}: {path}")
        records.append({
            "module": name,
            "source": path.relative_to(engine_root).as_posix(),
            "sha256": sha256_file(path),
        })
    if not records:
        raise RuntimeError("no historical fuzzfolio_core modules were imported")
    return records


def rust_runtime_pin() -> dict[str, Any]:
    module = load_rust_core_module()
    module_file = Path(getattr(module, "__file__", "")).resolve() if module is not None else None
    if module is None or module_file is None or not module_file.is_file():
        raise RuntimeError("historical Rust native extension is unavailable")
    package_root = module_file.parent
    artifacts = [
        {
            "relativePath": path.relative_to(package_root).as_posix(),
            "sha256": sha256_file(path),
        }
        for path in sorted(package_root.rglob("*"))
        if path.is_file() and path.suffix.lower() in {".py", ".pyd", ".dll"}
    ]
    if not artifacts:
        raise RuntimeError("Rust native package has no identity-bearing files")
    return {
        "module": module.__name__,
        "modulePath": str(module_file),
        "packageRoot": str(package_root),
        "buildInfo": rust_core_build_info(),
        "artifacts": artifacts,
    }


def build_payload(args: argparse.Namespace) -> dict[str, Any]:
    engine_root = args.engine_root.resolve()
    study_root = args.study_root.resolve()
    if git_text(engine_root, "rev-parse", "HEAD") != HISTORICAL_ENGINE_COMMIT:
        raise RuntimeError("historical engine is not at the frozen commit")
    if not under(Path(__file__).resolve(), study_root):
        raise RuntimeError("authority script must run from the V3 study worktree")
    manifest = check_json_self_hash(args.manifest.resolve(), "manifestCanonicalPayloadSha256")
    if len(manifest.get("contexts", [])) != EXPECTED_CONTEXT_COUNT:
        raise RuntimeError("extraction census context count drift")
    if manifest.get("sourcePins", {}).get("v3HumanDirectiveSha256") != "sha256:97522e780e19f8b399bc465b6358660a11ee4613a3b8ee0e8d1f7e07be588a26":
        raise RuntimeError("extraction census is not bound to its accepted V3 directive")
    source_audit = check_json_self_hash(args.projection_audit.resolve(), "projectionAuditCanonicalPayloadSha256")
    if source_audit["projectionAuditCanonicalPayloadSha256"] != PROJECTION_AUDIT_SHA256:
        raise RuntimeError("historical projection audit digest drift")
    if sha256_file(args.authorization_prompt.resolve()) != AUTHORIZATION_SHA256:
        raise RuntimeError("Pro completion authorization digest drift")
    core_files = []
    for relative_path in SOURCE_FILES:
        source = (engine_root / relative_path).resolve()
        if not source.is_file() or not under(source, engine_root):
            raise RuntimeError(f"missing pinned source: {relative_path}")
        core_files.append({"path": relative_path.as_posix(), "sha256": sha256_file(source)})
    component_pins = component_runtime_pins(manifest, engine_root)
    return {
        "schemaVersion": "temporal_qd_component_surrogate_feature_projection_runtime_authority_addendum_v1",
        "authority": {
            "autoresearchCommit": AUTORESEARCH_COMMIT,
            "historicalExecutionEngineCommit": HISTORICAL_ENGINE_COMMIT,
            "extractionCensusCanonicalPayloadSha256": manifest["manifestCanonicalPayloadSha256"],
            "historicalProjectionAuditCanonicalPayloadSha256": source_audit["projectionAuditCanonicalPayloadSha256"],
            "proCompletionAuthorizationSha256": AUTHORIZATION_SHA256,
        },
        "featureProtocolV2": verify_feature_protocol(study_root),
        "componentOnlyBoundary": {
            "permittedInputs": [
                "outcome-free V3 census",
                "pinned historical engine/catalog/runtime",
                "isolated archive-recovered OHLCV bars",
                "the frozen protocol and this addendum",
            ],
            "forbiddenInputs": [
                "candidate TemporalGraph or profile execution",
                "child outcomes, economics, P&L, costs, or archive values",
                "worker, gateway, market-lake fallback, or network data reads",
            ],
            "extractorContract": "One deterministic component-only extractor has no outcome path argument or outcome reader import; it must be run in an isolated process.",
        },
        "projectionAndFreshness": {
            "primaryEvent": "The named raw EventBinding output after historical warmup coercion and cross-timeframe alignment; processed long/short scores are retained only as diagnostics.",
            "rawDomain": "At the historical visual-alignment boundary, missing/NaN raw event samples become false. A non-missing raw event must be Boolean or exact numeric 0/1; no numeric threshold is permitted.",
            "freshEvent": "At every completed, non-provisional observation where the selected aligned raw binding is active, that binding belongs in facts.freshEvents. There is no inactive-to-active edge filter.",
            "freshFeatures": [
                "freshEventBarCount",
                "freshEventFraction",
                "freshEventAvailability = freshEventFraction",
            ],
            "eventStarts": "An event start remains a separate false-to-true descriptive transition; eventStartShareOfActiveBars must never substitute for freshEventFraction.",
            "eventBindingSelection": "The selected raw output is longOutput for a long-owned binding and shortOutput for a short-owned binding.",
        },
        "clockAndFrameContract": {
            "decisionClock": "completed M5 observation clock",
            "m5": "M5 source raw events expose on the coincident completed M5 observation.",
            "m15": "M15 source raw events first expose at M5 source-start plus 10 minutes; raw events use exact reindexing without forward fill, while processed diagnostic scores use the separate historical forward-fill path.",
            "inputFrame": {
                "requiredColumns": ["open", "high", "low", "close", "volume", "pair"],
                "index": "UTC bar_start datetime index, unique and newest-first before historical indicator execution",
                "analysisInterval": "half-open [start, end) after warmup bars are supplied to the indicator",
                "completeBarsOnly": True,
                "pairMetadata": "pair is supplied as the frozen archive instrument so any historical scale lookup resolves from the input frame.",
                "noLakeFallback": True,
            },
        },
        "pinnedHistoricalSources": core_files,
        "componentImplementations": component_pins,
        "runtime": {
            "python": sys.version,
            "numpy": np.__version__,
            "pandas": pd.__version__,
            "pyarrow": pyarrow.__version__,
            "platform": platform.platform(),
            "rustNative": rust_runtime_pin(),
            "importedFuzzfolioCoreModules": imported_core_modules(engine_root),
            "allImportedFuzzfolioCoreModulesUnderPinnedEngine": True,
        },
        "attestationPolicy": "Every recovered window later records frozenWindowSemanticSha256 plus separate archive/recovery receipt and local raw-file attestations; those attestations are not substituted for one another.",
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--study-root", type=Path, required=True)
    parser.add_argument("--engine-root", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--projection-audit", type=Path, required=True)
    parser.add_argument("--authorization-prompt", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise RuntimeError(f"refusing to overwrite authority output: {args.output}")
    payload = build_payload(args)
    payload["authorityAddendumCanonicalPayloadSha256"] = sha256_prefixed(canonical_bytes(payload))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({
        "componentCount": len(payload["componentImplementations"]),
        "authorityAddendumCanonicalPayloadSha256": payload["authorityAddendumCanonicalPayloadSha256"],
        "output": str(args.output),
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
