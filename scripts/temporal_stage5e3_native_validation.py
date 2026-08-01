"""Validate all Stage 5E-3 screening plans/jobs with the frozen Fuzz core."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path

from fuzzfolio_core.contracts.temporal_jobs import TemporalGraphCandidateWindowJob
from fuzzfolio_core.models.evidence_plan import ReplayEvidencePlan
from fuzzfolio_core.temporal_graph.search_validation import canonical_sha256


def _write_immutable(path: Path, payload: dict) -> None:
    encoded = json.dumps(payload, indent=2, sort_keys=True) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.read_text(encoding="utf-8") != encoded:
        raise ValueError(f"refusing divergent overwrite: {path}")
    path.write_text(encoded, encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--task-manifest", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    manifest = json.loads(args.task_manifest.read_text(encoding="utf-8"))
    tasks = list(manifest.get("tasks") or [])
    if len(tasks) != 256:
        raise ValueError("Stage 5E-3 native validation requires exactly 256 tasks")
    plan_ids = set()
    job_ids = set()
    windows = {}
    candidates = set()
    for task in tasks:
        payload = dict(task["payload"])
        plan = ReplayEvidencePlan.model_validate(payload["evidence_plan"])
        job = TemporalGraphCandidateWindowJob.model_validate(payload)
        plan_ids.add(plan.plan_id)
        job_ids.add(job.job_id)
        candidates.add(job.candidate_id)
        windows[job.window_id] = windows.get(job.window_id, 0) + 1
    if len(job_ids) != 256 or len(plan_ids) != 256 or len(candidates) != 128:
        raise ValueError("native validation identities are not one candidate/window job each")
    task_matrix_sha256 = canonical_sha256(tasks)
    if task_matrix_sha256 != manifest.get("taskMatrixSha256"):
        raise ValueError("native task-matrix SHA-256 mismatch")
    report = {
        "schemaVersion": "temporal_search_stage5e3_native_validation_v1",
        "taskManifestFileSha256": "sha256:"
        + hashlib.sha256(args.task_manifest.read_bytes()).hexdigest(),
        "taskMatrixSha256": task_matrix_sha256,
        "taskCount": len(tasks),
        "candidateCount": len(candidates),
        "windowTaskCounts": dict(sorted(windows.items())),
        "evidencePlanValidationCount": len(plan_ids),
        "candidateWindowJobValidationCount": len(job_ids),
        "allChecksPassed": True,
        "gatewayContacted": False,
        "marketBarsRead": False,
    }
    report["reportSha256"] = canonical_sha256(report)
    _write_immutable(args.output, report)
    print(json.dumps(report, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
