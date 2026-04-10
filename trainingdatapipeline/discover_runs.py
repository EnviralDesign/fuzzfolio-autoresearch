"""Discover local run directories and emit a stable manifest."""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from . import PIPELINE_VERSION
from .replay_types import DiscoveredRun, RunArtifactInventory
from .schemas import OPTIONAL_RUN_ARTIFACTS, OPTIONAL_RUN_DIRS, REQUIRED_RUN_ARTIFACTS

RUN_ID_RE = re.compile(r"^(?P<stamp>\d{8}T\d{6}\d{6}Z)-")


def _default_runs_root() -> Path:
    return Path(__file__).resolve().parents[1] / "runs"


def _parse_run_id_timestamp(run_id: str) -> str | None:
    match = RUN_ID_RE.match(run_id)
    if not match:
        return None
    stamp = match.group("stamp")
    try:
        parsed = datetime.strptime(stamp, "%Y%m%dT%H%M%S%fZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError:
        return None
    return parsed.isoformat()


def _count_jsonl_lines(path: Path) -> int | None:
    if not path.exists() or not path.is_file():
        return None
    try:
        with path.open("r", encoding="utf-8") as handle:
            return sum(1 for _ in handle if _.strip())
    except OSError:
        return None


def _discover_run_inventory(run_dir: Path) -> RunArtifactInventory:
    required_present = {
        name: (run_dir / name).exists() for name in REQUIRED_RUN_ARTIFACTS
    }
    optional_present = {
        name: (run_dir / name).exists() for name in OPTIONAL_RUN_ARTIFACTS
    }
    optional_dirs_present = {
        name: (run_dir / name).exists() and (run_dir / name).is_dir()
        for name in OPTIONAL_RUN_DIRS
    }
    return RunArtifactInventory(
        required_present=required_present,
        optional_present=optional_present,
        optional_dirs_present=optional_dirs_present,
    )


def _looks_like_run_dir(path: Path) -> bool:
    if not path.is_dir():
        return False
    if (path / "controller-log.jsonl").exists():
        return True
    if (path / "seed-prompt.json").exists():
        return True
    if (path / "runtime-trace.jsonl").exists():
        return True
    return False


def _iter_candidate_run_dirs(root: Path) -> Iterable[Path]:
    if _looks_like_run_dir(root):
        yield root
        return
    if not root.exists() or not root.is_dir():
        return
    for child in sorted(root.iterdir()):
        if _looks_like_run_dir(child):
            yield child


def discover_runs(roots: Iterable[Path]) -> list[DiscoveredRun]:
    discovered: list[DiscoveredRun] = []
    seen_dirs: set[Path] = set()
    for root in roots:
        resolved_root = root.resolve()
        for run_dir in _iter_candidate_run_dirs(resolved_root):
            resolved_run_dir = run_dir.resolve()
            if resolved_run_dir in seen_dirs:
                continue
            seen_dirs.add(resolved_run_dir)
            inventory = _discover_run_inventory(resolved_run_dir)
            controller_log = resolved_run_dir / "controller-log.jsonl"
            attempts_path = resolved_run_dir / "attempts.jsonl"
            profiles_dir = resolved_run_dir / "profiles"
            evals_dir = resolved_run_dir / "evals"
            discovered.append(
                DiscoveredRun(
                    run_id=resolved_run_dir.name,
                    run_dir=resolved_run_dir,
                    root=resolved_root,
                    artifact_inventory=inventory,
                    parsed_started_at=_parse_run_id_timestamp(resolved_run_dir.name),
                    controller_log_bytes=controller_log.stat().st_size
                    if controller_log.exists()
                    else None,
                    attempts_count_hint=_count_jsonl_lines(attempts_path),
                    profile_count_hint=len(list(profiles_dir.glob("*.json")))
                    if profiles_dir.exists()
                    else None,
                    eval_dir_count_hint=len([p for p in evals_dir.iterdir() if p.is_dir()])
                    if evals_dir.exists()
                    else None,
                )
            )
    discovered.sort(key=lambda item: item.run_id)
    return discovered


def build_manifest(roots: Iterable[Path]) -> dict[str, object]:
    resolved_roots = [path.resolve() for path in roots]
    runs = discover_runs(resolved_roots)
    required_counter = Counter()
    optional_counter = Counter()
    optional_dir_counter = Counter()
    for run in runs:
        for name, present in run.artifact_inventory.required_present.items():
            if present:
                required_counter[name] += 1
        for name, present in run.artifact_inventory.optional_present.items():
            if present:
                optional_counter[name] += 1
        for name, present in run.artifact_inventory.optional_dirs_present.items():
            if present:
                optional_dir_counter[name] += 1
    return {
        "pipeline_version": PIPELINE_VERSION,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "roots": [str(path) for path in resolved_roots],
        "summary": {
            "run_count": len(runs),
            "required_artifact_counts": dict(required_counter),
            "optional_artifact_counts": dict(optional_counter),
            "optional_dir_counts": dict(optional_dir_counter),
        },
        "runs": [run.to_dict() for run in runs],
    }


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Discover autoresearch run directories and emit a manifest."
    )
    parser.add_argument(
        "--root",
        action="append",
        dest="roots",
        help="Run corpus root or a specific run directory. Repeatable.",
    )
    parser.add_argument(
        "--out",
        type=Path,
        help="Optional output JSON path. Defaults to stdout.",
    )
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    roots = [Path(item) for item in args.roots] if args.roots else [_default_runs_root()]
    manifest = build_manifest(roots)
    rendered = json.dumps(manifest, ensure_ascii=True, indent=2)
    if args.out is not None:
        args.out.parent.mkdir(parents=True, exist_ok=True)
        args.out.write_text(rendered + "\n", encoding="utf-8")
    else:
        print(rendered)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
