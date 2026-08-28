"""Fail-closed V37 archive-counterfactual control-replay preflight.

The V37 archive-preservation study may only begin after its frozen historical
control can be reconstructed.  This module deliberately stops at that boundary
when the retained source lacks the direction-behavior identity required by the
recorded cumulative reducer.  It never launches work or synthesizes a variant.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from typing import Any, Mapping, Sequence


SCHEMA_VERSION = "temporal_qd_v37_archive_preservation_counterfactual_v1"
EXPECTED_ARCHIVE_MEMBER_COUNTS = [3, 3, 0, 0, 0]


class V37ControlReplayError(RuntimeError):
    """Raised when the frozen V37 material is malformed or incomplete."""


def _canonical_bytes(value: object) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=True,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _read_json(path: Path) -> Mapping[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise V37ControlReplayError(f"could not read JSON: {path}") from exc
    if not isinstance(value, Mapping):
        raise V37ControlReplayError(f"JSON object required: {path}")
    return value


def _run_root(v37_root: Path) -> Path:
    direct = v37_root / "generations"
    if direct.is_dir():
        return v37_root
    matches = sorted(v37_root.glob("run/*/generations"))
    if len(matches) != 1:
        raise V37ControlReplayError(
            "V37 root must contain exactly one run/*/generations authority"
        )
    return matches[0].parent


def _archive_members(archive: Mapping[str, Any]) -> list[dict[str, str]]:
    members: list[dict[str, str]] = []
    cells = archive.get("cells")
    if not isinstance(cells, list):
        raise V37ControlReplayError("archive cells must be a list")
    for cell in cells:
        if not isinstance(cell, Mapping) or not isinstance(cell.get("members"), list):
            raise V37ControlReplayError("archive cell members must be a list")
        for member in cell["members"]:
            if not isinstance(member, Mapping):
                raise V37ControlReplayError("archive member must be an object")
            candidate_id = member.get("candidateId")
            lane = member.get("archiveLane")
            descriptor = member.get("descriptor")
            if (
                not isinstance(candidate_id, str)
                or not isinstance(lane, str)
                or not isinstance(descriptor, Mapping)
                or not isinstance(descriptor.get("cellId"), str)
            ):
                raise V37ControlReplayError("archive member lacks identity, lane, or cell")
            members.append(
                {
                    "candidateId": candidate_id,
                    "archiveLane": lane,
                    "cellId": str(descriptor["cellId"]),
                }
            )
    return sorted(members, key=lambda row: row["candidateId"])


def _file_binding(path: Path, *, root: Path) -> dict[str, Any]:
    return {
        "path": str(path.resolve()),
        "relativePath": path.resolve().relative_to(root.resolve()).as_posix(),
        "sizeBytes": path.stat().st_size,
        "rawSha256": _sha256_file(path),
    }


def analyze_v37_control_replay(v37_root: Path | str) -> dict[str, Any]:
    """Inspect the frozen V37 control and fail closed on missing replay authority."""
    supplied_root = Path(v37_root).resolve()
    run_root = _run_root(supplied_root)
    launch_path = supplied_root / "launch-identity.json"
    if not launch_path.is_file():
        raise V37ControlReplayError(f"missing V37 launch identity: {launch_path}")
    launch = _read_json(launch_path)
    source = launch.get("source")
    if not isinstance(source, Mapping):
        raise V37ControlReplayError("V37 launch identity lacks source binding")
    source_commit = source.get("autoresearchHead")
    source_worktree = source.get("autoresearchWorktree")
    if not isinstance(source_commit, str) or not source_commit:
        raise V37ControlReplayError("V37 launch identity lacks autoresearchHead")
    if not isinstance(source_worktree, str) or not source_worktree:
        raise V37ControlReplayError("V37 launch identity lacks autoresearchWorktree")

    generations: list[dict[str, Any]] = []
    for generation_index in range(1, 6):
        generation_root = run_root / "generations" / f"generation-{generation_index:04d}"
        archive_path = generation_root / "native-finalization" / "archive.json"
        if not archive_path.is_file():
            raise V37ControlReplayError(f"missing historical archive: {archive_path}")
        archive = _read_json(archive_path)
        members = _archive_members(archive)
        member_count = archive.get("memberCount")
        if member_count != len(members):
            raise V37ControlReplayError(
                f"archive member count disagrees with cells in generation {generation_index}"
            )
        generations.append(
            {
                "generationIndex": generation_index,
                "archiveMemberCount": member_count,
                "archiveSha256": archive.get("archiveSha256"),
                "members": members,
                "archiveBinding": _file_binding(archive_path, root=supplied_root),
            }
        )

    g1_root = run_root / "generations" / "generation-0001" / "native-finalization"
    source_path = g1_root / "source.json"
    cumulative_path = g1_root / "evidence" / "cumulative-archive.json"
    if not source_path.is_file() or not cumulative_path.is_file():
        raise V37ControlReplayError("V37 generation 1 finalization source or archive is missing")
    finalizer_source = _read_json(source_path)
    bundles = finalizer_source.get("candidatePanelBundles")
    if not isinstance(bundles, list) or not bundles:
        raise V37ControlReplayError("V37 generation 1 lacks retained candidate panel bundles")

    behavior_record_count = 0
    missing_identity_records: list[dict[str, str]] = []
    for bundle in bundles:
        if not isinstance(bundle, Mapping):
            raise V37ControlReplayError("candidate panel bundle must be an object")
        candidate_id = bundle.get("candidateId")
        panel_id = bundle.get("panelId")
        records = bundle.get("windowEvidence")
        if not isinstance(candidate_id, str) or not isinstance(panel_id, str) or not isinstance(records, list):
            raise V37ControlReplayError("candidate panel bundle lacks identity or window evidence")
        for record in records:
            if not isinstance(record, Mapping):
                raise V37ControlReplayError("candidate window evidence must be an object")
            metrics = record.get("metrics")
            if not isinstance(metrics, Mapping):
                raise V37ControlReplayError("candidate window evidence lacks metrics")
            behavior = metrics.get("realizedBehavior")
            if not isinstance(behavior, Mapping):
                raise V37ControlReplayError("candidate window evidence lacks realized behavior")
            behavior_record_count += 1
            if not isinstance(behavior.get("identityMaterial"), Mapping) or not isinstance(
                behavior.get("identitySha256"), str
            ):
                missing_identity_records.append(
                    {
                        "candidateId": candidate_id,
                        "panelId": panel_id,
                        "windowId": str(record.get("windowId")),
                    }
                )

    member_counts = [int(row["archiveMemberCount"]) for row in generations]
    observed_control = {
        "memberCounts": member_counts,
        "matchesRequiredTrajectory": member_counts == EXPECTED_ARCHIVE_MEMBER_COUNTS,
        "generationArchives": generations,
        "g1FinalizerSource": _file_binding(source_path, root=supplied_root),
        "g1CumulativeArchive": _file_binding(cumulative_path, root=supplied_root),
    }
    blocker = {
        "stage": "cumulative_direction_behavior_reconstruction",
        "reason": "missing_realized_behavior_identity_authority",
        "recordedCommit": source_commit,
        "recordedWorktree": source_worktree,
        "requiredFields": ["identityMaterial", "identitySha256"],
        "behaviorRecordCount": behavior_record_count,
        "missingIdentityRecordCount": len(missing_identity_records),
        "firstMissingRecords": missing_identity_records[:8],
        "explanation": (
            "The frozen panel metrics retain legacy realizedBehavior objects without the "
            "identity material required to reconstruct cumulative direction selection. "
            "The launch identifies a dirty source worktree, but does not retain that "
            "uncommitted finalizer source."
        ),
    }
    return {
        "schemaVersion": SCHEMA_VERSION,
        "v37Root": str(supplied_root),
        "runRoot": str(run_root),
        "controlReplay": observed_control,
        "status": "blocked_source_drift",
        "blocker": blocker,
        "counterfactualExecution": {
            "state": "not_authorized_without_exact_control_replay",
            "executedVariants": [],
            "marketEvaluation": False,
            "workerGatewayOrVast": False,
            "generation": False,
            "historicalArchiveMutation": False,
        },
    }


def write_control_replay_report(*, v37_root: Path | str, output_dir: Path | str) -> dict[str, Any]:
    """Write only compact deterministic blocker artifacts; never a variant result."""
    output = Path(output_dir)
    output.mkdir(parents=True, exist_ok=True)
    report = analyze_v37_control_replay(v37_root)
    report_path = output / "control-replay-preflight.json"
    report_path.write_bytes(_canonical_bytes(report) + b"\n")
    markdown = "\n".join(
        [
            "# V37 archive-preservation counterfactual — control replay",
            "",
            "Status: **blocked before counterfactual execution**.",
            "",
            f"Historical trajectory: `{report['controlReplay']['memberCounts']}`.",
            f"Recorded source commit: `{report['blocker']['recordedCommit']}`.",
            f"Recorded source worktree: `{report['blocker']['recordedWorktree']}`.",
            "",
            "The retained G1 panel metrics omit `identityMaterial` and `identitySha256` "
            "for realized behavior. Those fields are required to reconstruct the frozen "
            "cumulative direction-selection path, so the exact control cannot be replayed.",
            "",
            "No archive counterfactual, market evaluation, worker/gateway/Vast work, generation, "
            "or archive mutation was executed.",
            "",
        ]
    )
    readme_path = output / "README.md"
    readme_path.write_text(markdown, encoding="utf-8")
    checksum_lines = [
        f"{_sha256_file(path)[len('sha256:'):]}  {path.name}"
        for path in (report_path, readme_path)
    ]
    (output / "CHECKSUMS.sha256").write_text("\n".join(checksum_lines) + "\n", encoding="utf-8")
    return report


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--v37-root", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args(argv)
    report = write_control_replay_report(v37_root=args.v37_root, output_dir=args.output_dir)
    print(json.dumps(report, ensure_ascii=True, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
