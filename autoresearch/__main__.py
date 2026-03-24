from __future__ import annotations

import argparse
import json
from pathlib import Path

from .config import load_config
from .controller import ResearchController
from .fuzzfolio import FuzzfolioCli
from .ledger import append_attempt, load_attempts, make_attempt_record
from .plotting import render_progress_plot
from .scoring import build_attempt_score


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fuzzfolio autoresearch runtime.")
    subparsers = parser.add_subparsers(dest="command", required=True)

    doctor = subparsers.add_parser("doctor", help="Verify config, CLI, auth, and seed prompt.")
    doctor.add_argument("--json", action="store_true", help="Print machine-readable JSON.")

    run = subparsers.add_parser("run", help="Run the autonomous research controller.")
    run.add_argument("--max-steps", type=int, default=None)

    subparsers.add_parser("plot", help="Regenerate the progress plot from the attempts ledger.")

    score = subparsers.add_parser("score", help="Score one sensitivity artifact directory.")
    score.add_argument("artifact_dir", type=Path)

    record = subparsers.add_parser("record-attempt", help="Score and append one artifact directory to the attempts ledger.")
    record.add_argument("artifact_dir", type=Path)
    record.add_argument("--candidate-name", default=None)
    record.add_argument("--run-id", default="manual")
    record.add_argument("--profile-ref", default=None)
    record.add_argument("--note", default=None)

    return parser


def cmd_doctor() -> int:
    config = load_config()
    cli = FuzzfolioCli(config.fuzzfolio)
    cli_path = cli.resolve_executable()
    auth = cli.ensure_login()
    seed = cli.seed_prompt()
    payload = {
        "repo_root": str(config.repo_root),
        "config_path": str(config.config_path),
        "secrets_path": str(config.secrets_path),
        "cli_command": config.fuzzfolio.cli_command,
        "cli_resolved_path": cli_path,
        "provider_model": config.provider.model,
        "provider_api_base": config.provider.api_base,
        "provider_has_api_key": bool(config.provider.api_key),
        "auth_ok": auth.returncode == 0,
        "seed_ok": seed.returncode == 0,
    }
    print(json.dumps(payload, ensure_ascii=True, indent=2))
    return 0


def cmd_run(max_steps: int | None) -> int:
    config = load_config()
    controller = ResearchController(config)
    result = controller.run(max_steps=max_steps)
    print(json.dumps(result, ensure_ascii=True, indent=2))
    return 0


def cmd_plot() -> int:
    config = load_config()
    attempts = load_attempts(config.attempts_path)
    render_progress_plot(
        attempts,
        config.progress_plot_path,
        lower_is_better=config.research.plot_lower_is_better,
    )
    print(json.dumps({"attempts": len(attempts), "plot": str(config.progress_plot_path)}, ensure_ascii=True, indent=2))
    return 0


def cmd_score(artifact_dir: Path) -> int:
    config = load_config()
    cli = FuzzfolioCli(config.fuzzfolio)
    compare_payload = cli.score_artifact(artifact_dir.resolve())
    score = build_attempt_score(compare_payload, config.research.adjustments)
    print(
        json.dumps(
            {
                "artifact_dir": str(artifact_dir.resolve()),
                "primary_score": score.primary_score,
                "composite_score": score.composite_score,
                "adjustments": score.adjustments,
                "best_summary": score.best_summary,
            },
            ensure_ascii=True,
            indent=2,
        )
    )
    return 0


def cmd_record_attempt(
    artifact_dir: Path,
    candidate_name: str | None,
    run_id: str,
    profile_ref: str | None,
    note: str | None,
) -> int:
    config = load_config()
    cli = FuzzfolioCli(config.fuzzfolio)
    compare_payload = cli.score_artifact(artifact_dir.resolve())
    score = build_attempt_score(compare_payload, config.research.adjustments)
    snapshot_path = artifact_dir.resolve() / "sensitivity-response.json"
    record = make_attempt_record(
        config,
        run_id,
        artifact_dir.resolve(),
        score,
        candidate_name=candidate_name,
        profile_ref=profile_ref,
        sensitivity_snapshot_path=snapshot_path if snapshot_path.exists() else None,
        note=note,
    )
    append_attempt(config.attempts_path, record)
    attempts = load_attempts(config.attempts_path)
    render_progress_plot(
        attempts,
        config.progress_plot_path,
        lower_is_better=config.research.plot_lower_is_better,
    )
    print(
        json.dumps(
            {
                "attempt_id": record.attempt_id,
                "sequence": record.sequence,
                "candidate_name": record.candidate_name,
                "composite_score": record.composite_score,
                "progress_plot": str(config.progress_plot_path),
            },
            ensure_ascii=True,
            indent=2,
        )
    )
    return 0


def main() -> int:
    parser = build_parser()
    args = parser.parse_args()
    if args.command == "doctor":
        return cmd_doctor()
    if args.command == "run":
        return cmd_run(max_steps=args.max_steps)
    if args.command == "plot":
        return cmd_plot()
    if args.command == "score":
        return cmd_score(args.artifact_dir)
    if args.command == "record-attempt":
        return cmd_record_attempt(
            args.artifact_dir,
            args.candidate_name,
            args.run_id,
            args.profile_ref,
            args.note,
        )
    parser.error(f"Unknown command: {args.command}")
    return 2


if __name__ == "__main__":
    raise SystemExit(main())
