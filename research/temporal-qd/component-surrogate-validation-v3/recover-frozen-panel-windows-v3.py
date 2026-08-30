"""Recover and locally verify the twelve frozen V38 P1--P3 window archives.

The remote archive is read through its window-bound delivery surface.  This
tool never opens a current lake root and never falls back to an unbound file
download.  Each downloaded window is separately materialized beneath a fresh
ignored root and must recompute to its original V38 semantic digest.
"""

from __future__ import annotations

import argparse
import hashlib
import importlib.util
import json
import os
import sys
from collections import defaultdict
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq


PANEL_FILE_SHA256 = {
    "panel-1": "sha256:81dd78a4435f3a93c6483463f85e406e679af0ba1a766ddf99c6a7acf6b3f7c1",
    "panel-2": "sha256:b6d0911ac55b4d1496fcfc4ac7da59de76a7f13969353b98a986d561abe92bcd",
    "panel-3": "sha256:6621b534eb62a11dad6b88176a255134e8cf1c06ea8021e8692031cdedc8ef8c",
}
SEMANTIC_DIGEST_SOURCE_SHA256 = "sha256:c6cae40cf63661c17cf71f843801309c687be28c5dc08dac56ff50154bac29a6"
TIMEFRAMES = ("H1", "M15", "M5")
PAIR = "EURUSD"


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


def load_exact_digest_module(path: Path) -> Any:
    spec = importlib.util.spec_from_file_location("v38_window_attestation", path)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"cannot load semantic-digest source: {path}")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def configure_dashboard_imports(dashboard_root: Path) -> None:
    for path in (
        dashboard_root / "shared/python/fuzzfolio_core",
        dashboard_root / "shared/python/fuzzfolio_data",
    ):
        if not path.is_dir():
            raise RuntimeError(f"dashboard source root is missing: {path}")
        sys.path.insert(0, str(path))


def lake_credentials(dashboard_root: Path) -> tuple[str, str]:
    base_url = str(os.environ.get("REMOTE_MARKET_DATA_LAKE_BASE_URL") or "").strip()
    token = str(os.environ.get("REMOTE_MARKET_DATA_LAKE_API_TOKEN") or "").strip()
    if not base_url or not token:
        env_path = dashboard_root / "compute-service/.env"
        if not env_path.is_file():
            raise RuntimeError("market-data lake credentials are unavailable")
        values: dict[str, str] = {}
        for raw_line in env_path.read_text(encoding="utf-8-sig").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue
            key, value = line.split("=", 1)
            if key.strip() in {"REMOTE_MARKET_DATA_LAKE_BASE_URL", "REMOTE_MARKET_DATA_LAKE_API_TOKEN"}:
                values[key.strip()] = value.strip().strip('"').strip("'")
        base_url = base_url or values.get("REMOTE_MARKET_DATA_LAKE_BASE_URL", "")
        token = token or values.get("REMOTE_MARKET_DATA_LAKE_API_TOKEN", "")
    if not base_url or not token:
        raise RuntimeError("market-data lake credentials are incomplete")
    return base_url.rstrip("/"), token


def frozen_windows(authority_root: Path, binding_model: Any) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for panel_id, expected_sha in PANEL_FILE_SHA256.items():
        template_path = authority_root / f"{panel_id}-template-preparation.json"
        if sha256_file(template_path) != expected_sha:
            raise RuntimeError(f"frozen authority template digest drifted: {template_path}")
        template = json.loads(template_path.read_text(encoding="utf-8"))
        candidates = template.get("candidates")
        if not isinstance(candidates, list) or not candidates:
            raise RuntimeError(f"frozen authority has no candidate window inputs: {template_path}")
        inputs = candidates[0].get("windowInputs")
        if not isinstance(inputs, list) or len(inputs) != 4:
            raise RuntimeError(f"frozen authority has unexpected panel width: {template_path}")
        for item in inputs:
            plan = item.get("evidencePlan") if isinstance(item, dict) else None
            raw_binding = plan.get("lake_window_binding") if isinstance(plan, dict) else None
            if not isinstance(raw_binding, dict):
                raise RuntimeError(f"frozen authority binding is missing in {template_path}")
            binding = binding_model.model_validate(raw_binding)
            if binding.request.pairs != [PAIR] or tuple(binding.request.timeframes) != TIMEFRAMES:
                raise RuntimeError(f"frozen authority request drifted for {panel_id}/{item.get('windowId')}")
            rows.append({
                "panelId": panel_id,
                "windowId": str(item.get("windowId") or ""),
                "analysisWindowStart": str(plan.get("analysis_window_start") or ""),
                "analysisWindowEnd": str(plan.get("analysis_window_end") or ""),
                "binding": binding,
                "authorityTemplate": template_path,
                "authorityTemplateSha256": expected_sha,
            })
    rows.sort(key=lambda row: (row["panelId"], row["windowId"]))
    if len(rows) != 12 or len({(row["panelId"], row["windowId"]) for row in rows}) != 12:
        raise RuntimeError("frozen authority must contain exactly twelve distinct P1--P3 windows")
    return rows


def artifact_matches_scope(artifact: Any, scope: Any) -> bool:
    relative = str(artifact.relative_path).replace("\\", "/")
    prefix = (
        f"bars/pair={scope.pair}/timeframe={scope.timeframe}/"
        f"year={scope.year:04d}/month={scope.month:02d}/"
    )
    return relative.startswith(prefix)


def column_values(table: Any, name: str) -> list[Any]:
    if name not in set(table.column_names):
        return [None] * table.num_rows
    values = table.column(name).combine_chunks()
    if hasattr(values, "dictionary") and values.dictionary is not None:
        return values.to_pylist()
    return values.to_pylist()


def load_scope_rows(
    *,
    root: Path,
    artifacts: list[dict[str, Any]],
    timeframe: str,
    start_s: int,
    end_s: int,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    rows: list[dict[str, Any]] = []
    file_checks: list[dict[str, Any]] = []
    for artifact in sorted(
        (item for item in artifacts if item["timeframe"] == timeframe),
        key=lambda item: item["relativePath"],
    ):
        path = (root / artifact["relativePath"]).resolve()
        if root not in path.parents or not path.is_file():
            raise RuntimeError(f"recovered archive artifact is absent or escaped its root: {path}")
        table = pq.ParquetFile(path).read()
        if table.num_rows == 0 or "bar_start_s" not in set(table.column_names):
            raise RuntimeError(f"recovered archive artifact is malformed: {path}")
        values = {name: column_values(table, name) for name in (
            "bar_start_s", "open", "high", "low", "close", "volume", "volume_kind",
            "source", "provider", "ingest_run_id", "calendar_contract_id", "market_structure_hash",
            "pair", "timeframe",
        )}
        for index, stamp in enumerate(values["bar_start_s"]):
            if stamp is None:
                continue
            stamp_i = int(stamp)
            if stamp_i < start_s or stamp_i >= end_s:
                continue
            rows.append({
                "pair": str(values["pair"][index] or PAIR).strip().upper(),
                "timeframe": str(values["timeframe"][index] or timeframe).strip().upper(),
                "bar_start_s": stamp_i,
                "open": float(values["open"][index]),
                "high": float(values["high"][index]),
                "low": float(values["low"][index]),
                "close": float(values["close"][index]),
                "volume": float(values["volume"][index]),
                "volume_kind": values["volume_kind"][index],
                "source": values["source"][index],
                "provider": values["provider"][index],
                "ingest_run_id": values["ingest_run_id"][index],
                "calendar_contract_id": values["calendar_contract_id"][index],
                "market_structure_hash": values["market_structure_hash"][index],
            })
        actual_raw_sha = sha256_file(path)
        if actual_raw_sha != artifact["rawSha256"]:
            raise RuntimeError(f"archive artifact changed after recovery: {path}")
        file_checks.append({**artifact, "localRawSha256": actual_raw_sha, "localRawSha256Matches": True})
    rows.sort(key=lambda row: int(row["bar_start_s"]))
    return rows, file_checks


def utc_days(start: datetime, end: datetime) -> list[datetime]:
    result: list[datetime] = []
    cursor = start
    while cursor < end:
        result.append(cursor)
        cursor += timedelta(days=1)
    return result


def recompute_semantic(
    *,
    digest_module: Any,
    root: Path,
    artifacts: list[dict[str, Any]],
    request: Any,
) -> tuple[str, list[dict[str, Any]], list[dict[str, Any]]]:
    start = datetime.fromisoformat(str(request.data_start).replace("Z", "+00:00")).astimezone(UTC)
    end = datetime.fromisoformat(str(request.data_end).replace("Z", "+00:00")).astimezone(UTC)
    start_s, end_s = int(start.timestamp()), int(end.timestamp())
    scopes: list[dict[str, Any]] = []
    artifact_checks: list[dict[str, Any]] = []
    for timeframe in TIMEFRAMES:
        rows, file_checks = load_scope_rows(
            root=root,
            artifacts=artifacts,
            timeframe=timeframe,
            start_s=start_s,
            end_s=end_s,
        )
        by_day: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for row in rows:
            by_day[datetime.fromtimestamp(int(row["bar_start_s"]), tz=UTC).strftime("%Y-%m-%d")].append(row)
        day_payloads = [
            {
                "day": day.strftime("%Y-%m-%d"),
                "bar_count": len(by_day[day.strftime("%Y-%m-%d")]),
                "day_semantic_sha256": digest_module.compute_scope_semantic_sha256(
                    by_day[day.strftime("%Y-%m-%d")]
                ),
            }
            for day in utc_days(start, end)
        ]
        scopes.append({
            "pair": PAIR,
            "timeframe": timeframe,
            "barCount": len(rows),
            "dayCount": len(day_payloads),
            "localScopeSemanticSha256": digest_module.compute_scope_semantic_sha256_from_days(day_payloads),
        })
        artifact_checks.extend(file_checks)
    window_scopes = [
        {
            "pair": item["pair"],
            "timeframe": item["timeframe"],
            "bar_count": item["barCount"],
            "scope_semantic_sha256": item["localScopeSemanticSha256"],
        }
        for item in scopes
    ]
    return digest_module.compute_window_semantic_sha256(request.model_dump(mode="json"), window_scopes), scopes, artifact_checks


def recover_one(*, row: dict[str, Any], client: Any, required_scopes: Any, digest_module: Any, root: Path) -> dict[str, Any]:
    binding = row["binding"]
    window_root = root / row["panelId"] / row["windowId"]
    window_root.mkdir(parents=True, exist_ok=False)
    artifacts = client.iter_artifacts(dataset="bars", pair=PAIR, window_binding=binding)
    expected_scopes = required_scopes(binding.request)
    recovered: list[dict[str, Any]] = []
    for scope in expected_scopes:
        matching = [artifact for artifact in artifacts if artifact_matches_scope(artifact, scope)]
        if len(matching) != 1:
            raise RuntimeError(
                f"archive scope cardinality failure for {row['panelId']}/{row['windowId']} "
                f"{scope.timeframe}/{scope.year:04d}-{scope.month:02d}: {len(matching)}"
            )
        result = client.download_scope_archive(
            destination_root=window_root,
            dataset=scope.dataset,
            pair=scope.pair,
            timeframe=scope.timeframe,
            year=scope.year,
            month=scope.month,
            expected_artifacts=matching,
            window_binding=binding,
        )
        artifact = matching[0]
        target = (window_root / artifact.relative_path).resolve()
        if not target.is_file() or window_root.resolve() not in target.parents:
            raise RuntimeError(f"scope archive did not materialize its expected target: {target}")
        recovered.append({
            "relativePath": artifact.relative_path.replace("\\", "/"),
            "timeframe": scope.timeframe,
            "remoteEtag": artifact.etag,
            "remoteSizeBytes": artifact.size_bytes,
            "rawSha256": sha256_file(target),
            "scopeArchiveDownloadedCount": result.downloaded_count,
            "scopeArchiveSkippedCount": result.skipped_count,
        })
    semantic, scopes, artifact_checks = recompute_semantic(
        digest_module=digest_module,
        root=window_root,
        artifacts=recovered,
        request=binding.request,
    )
    if semantic != binding.window_semantic_sha256:
        raise RuntimeError(
            f"local semantic mismatch for {row['panelId']}/{row['windowId']}: "
            f"{semantic} != {binding.window_semantic_sha256}"
        )
    return {
        "panelId": row["panelId"],
        "windowId": row["windowId"],
        "analysisWindowStart": row["analysisWindowStart"],
        "analysisWindowEnd": row["analysisWindowEnd"],
        "authorityTemplate": str(row["authorityTemplate"]),
        "authorityTemplateSha256": row["authorityTemplateSha256"],
        "binding": binding.model_dump(mode="json"),
        "isolatedRoot": str(window_root),
        "archiveArtifacts": recovered,
        "semanticScopes": scopes,
        "artifactChecks": artifact_checks,
        "localWindowSemanticSha256": semantic,
        "localWindowSemanticMatchesFrozen": True,
        "archiveScopeCount": len(recovered),
    }


def run(args: argparse.Namespace) -> dict[str, Any]:
    root = args.isolated_root.resolve()
    if root.exists():
        raise RuntimeError(f"fresh isolated recovery root already exists: {root}")
    if sha256_file(args.digest_module.resolve()) != SEMANTIC_DIGEST_SOURCE_SHA256:
        raise RuntimeError("semantic digest source does not match the pinned V2 archive copy")
    configure_dashboard_imports(args.dashboard_root.resolve())
    from fuzzfolio_core.models.lake_window import LakeWindowBinding
    from fuzzfolio_data.market_data_lake.lazy_cache import required_lake_artifact_scopes_for_window_request
    from fuzzfolio_data.market_data_lake.remote_client import MarketDataLakeRemoteClient

    authority_root = args.authority_root.resolve()
    windows = frozen_windows(authority_root, LakeWindowBinding)
    base_url, token = lake_credentials(args.dashboard_root.resolve())
    client = MarketDataLakeRemoteClient(
        base_url=base_url,
        api_token=token,
        timeout_seconds=90.0,
        overload_retry_max_seconds=300.0,
    )
    root.mkdir(parents=True, exist_ok=False)
    digest_module = load_exact_digest_module(args.digest_module.resolve())
    recovered = [
        recover_one(
            row=row,
            client=client,
            required_scopes=required_lake_artifact_scopes_for_window_request,
            digest_module=digest_module,
            root=root,
        )
        for row in windows
    ]
    if len(recovered) != 12 or any(item["archiveScopeCount"] != 15 for item in recovered):
        raise RuntimeError("incomplete frozen-window archive recovery")
    return {
        "schemaVersion": "temporal_qd_component_surrogate_frozen_window_recovery_v3",
        "mode": "remote_archive_read_only_fresh_isolated_roots",
        "semanticDigestSource": {
            "path": str(args.digest_module.resolve()),
            "sha256": SEMANTIC_DIGEST_SOURCE_SHA256,
        },
        "remoteClientSource": {
            "path": str(args.dashboard_root.resolve() / "shared/python/fuzzfolio_data/fuzzfolio_data/market_data_lake/remote_client.py"),
            "sha256": sha256_file(args.dashboard_root.resolve() / "shared/python/fuzzfolio_data/fuzzfolio_data/market_data_lake/remote_client.py"),
        },
        "windows": recovered,
        "allWindowSemanticsMatchFrozen": True,
        "allArchiveArtifactsAreIsolated": True,
        "usedCurrentLakeFallback": False,
        "passes": True,
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--authority-root", type=Path, required=True)
    parser.add_argument("--dashboard-root", type=Path, required=True)
    parser.add_argument("--digest-module", type=Path, required=True)
    parser.add_argument("--isolated-root", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    if args.output.exists():
        raise RuntimeError(f"refusing to overwrite recovery report: {args.output}")
    report = run(args)
    report["recoveryCanonicalPayloadSha256"] = sha256_prefixed(canonical_bytes(report))
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    print(json.dumps({
        "recoveryCanonicalPayloadSha256": report["recoveryCanonicalPayloadSha256"],
        "windowCount": len(report["windows"]),
        "passes": report["passes"],
    }, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
