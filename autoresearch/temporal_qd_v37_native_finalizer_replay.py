"""Replay V37 fast-ephemeral finalization through its retained native authority.

The command accepts only the preserved V37 finalization sources and the
hash-bound historical finalizer binary.  Each replay begins with a fresh
directory containing just ``source.json`` and a manifest whose sole semantic
difference is its absolute source path.  Historical archives are read only
after execution as comparison oracles.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA_VERSION = "temporal_qd_v37_native_finalizer_control_replay_v1"
GENERATION_COUNT = 5


class NativeControlReplayError(RuntimeError):
    """Raised when the native historical-control contract is not satisfied."""


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _sha256_bytes(value: bytes) -> str:
    return "sha256:" + hashlib.sha256(value).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise NativeControlReplayError(f"could not read JSON: {path}") from exc
    if not isinstance(value, dict):
        raise NativeControlReplayError(f"JSON object required: {path}")
    return value


def _run_root(v37_root: Path) -> Path:
    if (v37_root / "generations").is_dir():
        return v37_root
    matches = sorted(v37_root.glob("run/*/generations"))
    if len(matches) != 1:
        raise NativeControlReplayError(
            "V37 root must contain exactly one run/*/generations authority"
        )
    return matches[0].parent


def _native_path(path: Path) -> str:
    resolved = str(path.resolve())
    if os.name == "nt" and not resolved.startswith("\\\\?\\"):
        return "\\\\?\\" + resolved
    return resolved


def _write_new(path: Path, payload: bytes) -> None:
    if path.exists():
        raise NativeControlReplayError(f"refusing to overwrite replay input: {path}")
    path.write_bytes(payload)


def _require_mapping(value: object, *, name: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise NativeControlReplayError(f"{name} must be an object")
    return value


def _members(archive: Mapping[str, Any]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    cells = archive.get("cells")
    if not isinstance(cells, list):
        raise NativeControlReplayError("archive cells must be a list")
    for cell in cells:
        cell_mapping = _require_mapping(cell, name="archive cell")
        members = cell_mapping.get("members")
        if not isinstance(members, list):
            raise NativeControlReplayError("archive cell members must be a list")
        for member in members:
            value = _require_mapping(member, name="archive member")
            descriptor = _require_mapping(value.get("descriptor"), name="archive descriptor")
            rows.append(
                {
                    "candidateId": value.get("candidateId"),
                    "behaviorIdentitySha256": value.get("behaviorIdentitySha256"),
                    "archiveLane": value.get("archiveLane"),
                    "cellId": descriptor.get("cellId"),
                    "robustBreederEligible": value.get("robustBreederEligible"),
                    "retentionReason": value.get("retentionReason"),
                }
            )
    return sorted(rows, key=lambda row: str(row["candidateId"]))


def _cumulative_members(cumulative: Mapping[str, Any]) -> list[dict[str, Any]]:
    members = cumulative.get("members")
    if not isinstance(members, list):
        raise NativeControlReplayError("cumulative archive members must be a list")
    rows: list[dict[str, Any]] = []
    for member in members:
        value = _require_mapping(member, name="cumulative archive member")
        behavior = _require_mapping(
            value.get("cumulativeRealizedBehavior"), name="cumulative realized behavior"
        )
        identity = _require_mapping(behavior.get("identityMaterial"), name="identity material")
        supplied_identity = behavior.get("identitySha256")
        rows.append(
            {
                "candidateId": value.get("candidateId"),
                "cellId": value.get("cellId"),
                "robustBreederEligible": value.get("robustBreederEligible"),
                "robustBreederLane": value.get("robustBreederLane"),
                "aggregateIdentitySha256": supplied_identity,
                "aggregateIdentitySelfConsistent": supplied_identity
                == _sha256_bytes(_canonical_bytes(identity)),
            }
        )
    return sorted(rows, key=lambda row: str(row["candidateId"]))


def _source_window_hashes(source: Mapping[str, Any], candidate_id: str) -> list[str]:
    bundles = source.get("candidatePanelBundles")
    if not isinstance(bundles, list):
        raise NativeControlReplayError("finalization source lacks candidatePanelBundles")
    required_panels = source.get("rotatingEvidence", {}).get("panels")
    if not isinstance(required_panels, list):
        raise NativeControlReplayError("finalization source lacks rotating evidence panels")
    records: list[tuple[str, str, str]] = []
    for bundle in bundles:
        value = _require_mapping(bundle, name="candidate panel bundle")
        if value.get("candidateId") != candidate_id:
            continue
        panel_id = value.get("panelId")
        windows = value.get("windowEvidence")
        if not isinstance(panel_id, str) or not isinstance(windows, list):
            raise NativeControlReplayError("candidate panel bundle lacks panel windows")
        for window in windows:
            record = _require_mapping(window, name="candidate window evidence")
            records.append(
                (
                    panel_id,
                    str(record.get("windowId")),
                    _sha256_bytes(_canonical_bytes(record)),
                )
            )
    return [digest for _panel, _window, digest in sorted(records)]


def _prepare_replay_input(
    *, historical_dir: Path, target_dir: Path
) -> dict[str, Any]:
    if target_dir.exists():
        raise NativeControlReplayError(f"fresh replay directory already exists: {target_dir}")
    source_path = historical_dir / "source.json"
    historical_manifest_path = historical_dir / "manifest.json"
    source_bytes = source_path.read_bytes()
    source = _read_json(source_path)
    historical_manifest = _read_json(historical_manifest_path)
    if source.get("sourceSha256") != historical_manifest.get("sourceSha256"):
        raise NativeControlReplayError("historical manifest/source identity drifted")

    target_dir.mkdir(parents=True)
    replay_source_path = target_dir / "source.json"
    _write_new(replay_source_path, source_bytes)
    replay_manifest = dict(historical_manifest)
    replay_manifest["sourcePath"] = _native_path(replay_source_path)
    unsigned = {
        key: value for key, value in replay_manifest.items() if key != "manifestSha256"
    }
    replay_manifest["manifestSha256"] = _sha256_bytes(_canonical_bytes(unsigned))
    _write_new(target_dir / "manifest.json", _canonical_bytes(replay_manifest) + b"\n")

    pre_execution_files = sorted(
        path.relative_to(target_dir).as_posix()
        for path in target_dir.rglob("*")
        if path.is_file()
    )
    if pre_execution_files != ["manifest.json", "source.json"]:
        raise NativeControlReplayError("replay directory contains a non-input file")
    return {
        "source": source,
        "historicalManifestSha256": historical_manifest.get("manifestSha256"),
        "replayManifestSha256": replay_manifest["manifestSha256"],
        "sourceRawSha256": _sha256_bytes(source_bytes),
        "sourceBytesMatchHistorical": source_bytes == source_path.read_bytes(),
        "preExecutionFiles": pre_execution_files,
    }


def _execute_finalizer(*, binary: Path, manifest_path: Path) -> dict[str, Any]:
    completed = subprocess.run(
        [str(binary), str(manifest_path)],
        cwd=manifest_path.parent,
        capture_output=True,
        check=False,
    )
    (manifest_path.parent / "execution.stdout.log").write_bytes(completed.stdout)
    (manifest_path.parent / "execution.stderr.log").write_bytes(completed.stderr)
    if completed.returncode:
        tail = (completed.stderr or completed.stdout).decode("utf-8", errors="replace")[-1600:]
        raise NativeControlReplayError(
            f"native finalizer rejected replay input (exit {completed.returncode}): {tail}"
        )
    return {
        "exitCode": completed.returncode,
        "stdoutSha256": _sha256_bytes(completed.stdout),
        "stderrSha256": _sha256_bytes(completed.stderr),
    }


def _first_canonical_diff(expected: object, actual: object) -> dict[str, Any] | None:
    if expected == actual:
        return None
    return {
        "expectedSha256": _sha256_bytes(_canonical_bytes(expected)),
        "actualSha256": _sha256_bytes(_canonical_bytes(actual)),
        "expected": expected,
        "actual": actual,
    }


def _compare_generation(
    *, historical_dir: Path, replay_dir: Path, source: Mapping[str, Any]
) -> dict[str, Any]:
    historical_cumulative_path = historical_dir / "evidence" / "cumulative-archive.json"
    replay_cumulative_path = replay_dir / "evidence" / "cumulative-archive.json"
    historical_archive_path = historical_dir / "archive.json"
    replay_archive_path = replay_dir / "archive.json"
    historical_result_path = historical_dir / "fast-ephemeral-result.json"
    replay_result_path = replay_dir / "fast-ephemeral-result.json"
    for path in (
        historical_cumulative_path,
        replay_cumulative_path,
        historical_archive_path,
        replay_archive_path,
        historical_result_path,
        replay_result_path,
    ):
        if not path.is_file():
            raise NativeControlReplayError(f"missing finalization artifact: {path}")

    historical_cumulative = _read_json(historical_cumulative_path)
    replay_cumulative = _read_json(replay_cumulative_path)
    historical_archive = _read_json(historical_archive_path)
    replay_archive = _read_json(replay_archive_path)
    historical_result = _read_json(historical_result_path)
    replay_result = _read_json(replay_result_path)
    expected_result = {
        key: historical_result.get(key)
        for key in (
            "sourceSha256",
            "cumulativeArchive",
            "parentArchive",
            "candidateCount",
            "memberCount",
            "occupiedCellCount",
            "newCellCount",
            "parentSchedule",
        )
    }
    actual_result = {
        key: replay_result.get(key)
        for key in expected_result
    }
    historical_cumulative_members = _cumulative_members(historical_cumulative)
    replay_cumulative_members = _cumulative_members(replay_cumulative)
    identity_rows = []
    historical_by_id = {
        str(row["candidateId"]): row for row in historical_cumulative_members
    }
    for row in replay_cumulative_members:
        candidate_id = str(row["candidateId"])
        historical = historical_by_id.get(candidate_id)
        identity_rows.append(
            {
                "candidateId": candidate_id,
                "sourceWindowRecordSha256s": _source_window_hashes(source, candidate_id),
                "rustAggregateIdentitySha256": row["aggregateIdentitySha256"],
                "historicalAggregateIdentitySha256": (
                    None if historical is None else historical["aggregateIdentitySha256"]
                ),
                "identityEqual": historical == row,
                "selfConsistent": row["aggregateIdentitySelfConsistent"],
            }
        )
    differences = {
        "cumulativeBytes": (
            None
            if replay_cumulative_path.read_bytes() == historical_cumulative_path.read_bytes()
            else {"historical": _sha256_file(historical_cumulative_path), "replay": _sha256_file(replay_cumulative_path)}
        ),
        "archiveBytes": (
            None
            if replay_archive_path.read_bytes() == historical_archive_path.read_bytes()
            else {"historical": _sha256_file(historical_archive_path), "replay": _sha256_file(replay_archive_path)}
        ),
        "cumulativeMembers": _first_canonical_diff(
            historical_cumulative_members, replay_cumulative_members
        ),
        "parentMembers": _first_canonical_diff(
            _members(historical_archive), _members(replay_archive)
        ),
        "resultComparableFields": _first_canonical_diff(expected_result, actual_result),
    }
    exact = all(value is None for value in differences.values()) and all(
        row["identityEqual"] and row["selfConsistent"] for row in identity_rows
    )
    return {
        "exact": exact,
        "historical": {
            "cumulativeArchiveSha256": historical_cumulative.get("archiveSha256"),
            "parentArchiveSha256": historical_archive.get("archiveSha256"),
            "memberCount": historical_archive.get("memberCount"),
            "occupiedCellCount": historical_archive.get("occupiedCellCount"),
        },
        "replay": {
            "cumulativeArchiveSha256": replay_cumulative.get("archiveSha256"),
            "parentArchiveSha256": replay_archive.get("archiveSha256"),
            "memberCount": replay_archive.get("memberCount"),
            "occupiedCellCount": replay_archive.get("occupiedCellCount"),
        },
        "aggregateIdentityReconciliation": identity_rows,
        "differences": differences,
    }


def _replay_once(*, run_root: Path, finalizer: Path, output_root: Path) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for generation_index in range(1, GENERATION_COUNT + 1):
        historical_dir = (
            run_root
            / "generations"
            / f"generation-{generation_index:04d}"
            / "native-finalization"
        )
        replay_dir = (
            output_root
            / f"generation-{generation_index:04d}"
            / "native-finalization"
        )
        prepared = _prepare_replay_input(
            historical_dir=historical_dir, target_dir=replay_dir
        )
        execution = _execute_finalizer(
            binary=finalizer, manifest_path=replay_dir / "manifest.json"
        )
        source_preserved = _sha256_file(replay_dir / "source.json") == prepared[
            "sourceRawSha256"
        ]
        if not source_preserved:
            raise NativeControlReplayError("native finalizer mutated its frozen source input")
        comparison = _compare_generation(
            historical_dir=historical_dir,
            replay_dir=replay_dir,
            source=_require_mapping(prepared["source"], name="finalization source"),
        )
        rows.append(
            {
                "generationIndex": generation_index,
                "input": {
                    **{key: value for key, value in prepared.items() if key != "source"},
                    "sourceContainsPreviousCumulativeArchive": bool(
                        _require_mapping(prepared["source"], name="finalization source").get(
                            "previousCumulativeArchive"
                        )
                    ),
                    "sourceBytesPreservedAfterExecution": source_preserved,
                },
                "execution": execution,
                "comparison": comparison,
            }
        )
    return rows


def run_native_control_replay(
    *, v37_root: Path | str, finalizer: Path | str, output_dir: Path | str
) -> dict[str, Any]:
    """Run two isolated V37 native controls and compare them only after execution."""
    historical_root = Path(v37_root).resolve()
    run_root = _run_root(historical_root)
    finalizer_path = Path(finalizer).resolve()
    target = Path(output_dir)
    if target.exists():
        raise NativeControlReplayError(f"fresh replay root already exists: {target}")
    authority = _read_json(run_root / "native-finalization-authority.json")
    expected_binary_sha = (
        authority.get("binaries", {}).get("generationFinalizer", {}).get("fileSha256")
    )
    if not isinstance(expected_binary_sha, str) or _sha256_file(finalizer_path) != expected_binary_sha:
        raise NativeControlReplayError("finalizer does not match recorded V37 runtime authority")
    target.mkdir(parents=True)
    run_a = _replay_once(run_root=run_root, finalizer=finalizer_path, output_root=target / "run-a")
    run_b = _replay_once(run_root=run_root, finalizer=finalizer_path, output_root=target / "run-b")
    deterministic = []
    for generation_index in range(1, GENERATION_COUNT + 1):
        files = ("archive.json", "evidence/cumulative-archive.json")
        result = {
            "generationIndex": generation_index,
            "files": {},
        }
        for relative in files:
            left = target / "run-a" / f"generation-{generation_index:04d}" / "native-finalization" / relative
            right = target / "run-b" / f"generation-{generation_index:04d}" / "native-finalization" / relative
            result["files"][relative] = {
                "byteEqual": left.read_bytes() == right.read_bytes(),
                "runASha256": _sha256_file(left),
                "runBSha256": _sha256_file(right),
            }
        deterministic.append(result)
    exact = all(row["comparison"]["exact"] for row in run_a + run_b) and all(
        file["byteEqual"]
        for generation in deterministic
        for file in generation["files"].values()
    )
    report = {
        "schemaVersion": SCHEMA_VERSION,
        "status": "exact_native_control_replay" if exact else "native_control_mismatch",
        "v37Root": str(historical_root),
        "runRoot": str(run_root),
        "runtimeAuthority": {
            "generationFinalizer": str(finalizer_path),
            "generationFinalizerSha256": _sha256_file(finalizer_path),
            "recordedGenerationFinalizerSha256": expected_binary_sha,
        },
        "replayInputPolicy": {
            "historicalOutputFilesInjected": False,
            "frozenSourceEmbeddedPriorArchiveState": {
                str(row["generationIndex"]): row["input"][
                    "sourceContainsPreviousCumulativeArchive"
                ]
                for row in run_a
            },
            "preExecutionFiles": ["manifest.json", "source.json"],
            "sourceCopy": "byte_for_byte",
            "manifestDifference": "sourcePath_and_derived_manifestSha256_only",
        },
        "runs": {"runA": run_a, "runB": run_b},
        "determinism": deterministic,
        "observedMemberCountTrajectory": [
            row["comparison"]["replay"]["memberCount"] for row in run_a
        ],
        "safety": {
            "marketEvaluation": False,
            "workerGatewayOrVast": False,
            "generation": False,
            "historicalArchiveMutation": False,
            "policyRewrite": False,
        },
    }
    (target / "native-control-replay.json").write_bytes(_canonical_bytes(report) + b"\n")
    return report


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--v37-root", required=True, type=Path)
    parser.add_argument("--finalizer", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args(argv)
    report = run_native_control_replay(
        v37_root=args.v37_root,
        finalizer=args.finalizer,
        output_dir=args.output_dir,
    )
    print(json.dumps(report, ensure_ascii=True, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
