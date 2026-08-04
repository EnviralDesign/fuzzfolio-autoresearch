"""Run one fresh immigrant-only QD construction smoke without market data."""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import Any

from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError
from autoresearch.temporal_qd_pair_factory import (
    PairAuthorityBundle,
    load_pair_run_config,
    pair_policy_from_config,
)
from autoresearch.temporal_qd_pair_generation import generate_pair_population
from autoresearch.temporal_qd_smoke_report import (
    build_qd_construction_smoke_report,
)


def _read_object(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise TemporalDiscoveryContractError(f"JSON root must be an object: {path}")
    return value


def _fresh_external_root(output_root: Path) -> Path:
    root = output_root.resolve()
    repository = Path(__file__).resolve().parents[1]
    try:
        inside_repository = os.path.commonpath((str(root), str(repository))) == str(
            repository
        )
    except ValueError:
        inside_repository = False
    if inside_repository:
        raise TemporalDiscoveryContractError(
            "--output-root must be external to the autoresearch repository"
        )
    if root.exists() and any(root.iterdir()):
        raise TemporalDiscoveryContractError("--output-root must be absent or empty")
    root.mkdir(parents=True, exist_ok=True)
    return root


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pair-config", required=True, type=Path)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--target-unique-candidates", required=True, type=int)
    parser.add_argument("--autoresearch-commit", required=True)
    parser.add_argument("--run-label", required=True)
    parser.add_argument("--max-proposal-attempts", type=int)
    args = parser.parse_args()
    if args.target_unique_candidates < 1:
        parser.error("--target-unique-candidates must be positive")
    max_attempts = args.max_proposal_attempts or args.target_unique_candidates * 4
    if max_attempts < args.target_unique_candidates:
        parser.error("--max-proposal-attempts cannot be below the target")
    root = _fresh_external_root(args.output_root)
    frozen = load_pair_run_config(_read_object(args.pair_config))
    error: BaseException | None = None
    try:
        with PairAuthorityBundle(frozen) as authority:
            result = generate_pair_population(
                output_root=root,
                generation_index=0,
                target_unique_candidates=args.target_unique_candidates,
                run_config={
                    "schemaVersion": "temporal_qd_immigrant_construction_smoke_run_v1",
                    "autoresearchCommit": str(args.autoresearch_commit),
                    "pairRunConfigSha256": frozen["pairRunConfigSha256"],
                    "mode": "no_market_no_economic_evidence",
                    "runLabel": str(args.run_label),
                    "targetUniqueCandidates": args.target_unique_candidates,
                },
                pair_policy=pair_policy_from_config(frozen),
                pair_factory=authority.factory,
                module_authority=authority.operator,
                native_validator=authority.validator,
                pair_compiler=authority.compiler,
                operator_implementation_identity=frozen["operatorImplementation"],
                max_proposal_attempts=max_attempts,
            )
        print(json.dumps(result, indent=2, sort_keys=True))
    except BaseException as exc:
        error = exc
    finally:
        summary_path = root / "performance" / "latest-summary.json"
        if summary_path.exists():
            report = build_qd_construction_smoke_report(root)
            report_path = root / "performance" / "construction-smoke-report.json"
            report_path.write_text(
                json.dumps(report, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
                newline="\n",
            )
            print(json.dumps(report, indent=2, sort_keys=True))
    if error is not None:
        raise error


if __name__ == "__main__":
    main()
