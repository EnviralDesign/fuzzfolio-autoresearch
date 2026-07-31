from __future__ import annotations

import argparse
import json

from .temporal_search_quality import (
    audit_search_quality_study,
    finalize_search_quality_study,
    freeze_control_study,
    prepare_search_quality_study,
)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=(
            "Build, freeze, or audit a deterministic temporal-search quality "
            "calibration study."
        )
    )
    subcommands = parser.add_subparsers(dest="command", required=True)

    prepare = subcommands.add_parser(
        "prepare",
        help="Build Stage 5E-1 Phase A from immutable existing evidence.",
    )
    prepare.add_argument("--discovery-root", required=True)
    prepare.add_argument("--initial-result-root", required=True)
    prepare.add_argument("--confirmation-result-root", required=True)
    prepare.add_argument("--evidence-path", required=True)
    prepare.add_argument("--output-root", required=True)
    prepare.add_argument("--expected-report-sha256", required=True)
    prepare.add_argument("--expected-manifest-sha256", required=True)
    prepare.add_argument("--expected-evidence-file-sha256", required=True)
    prepare.add_argument("--source-autoresearch-commit", required=True)
    prepare.add_argument("--analysis-autoresearch-commit", required=True)
    prepare.add_argument("--fuzzfolio-commit", required=True)
    prepare.add_argument("--worker-contract-sha256", required=True)

    freeze = subcommands.add_parser(
        "freeze-control",
        help="Freeze the deterministic unselected B/D control authority.",
    )
    freeze.add_argument("--discovery-root", required=True)
    freeze.add_argument("--quality-root", required=True)
    freeze.add_argument("--sample-size", type=int, default=64)

    finalize = subcommands.add_parser(
        "finalize",
        help="Compare the selected cohorts with the completed control study.",
    )
    finalize.add_argument("--discovery-root", required=True)
    finalize.add_argument("--initial-result-root", required=True)
    finalize.add_argument("--confirmation-result-root", required=True)
    finalize.add_argument("--control-result-root", required=True)
    finalize.add_argument("--quality-root", required=True)

    audit = subcommands.add_parser(
        "audit",
        help="Rehash the search-quality binding and immutable artifacts.",
    )
    audit.add_argument("--quality-root", required=True)
    return parser


def main() -> None:
    args = _parser().parse_args()
    if args.command == "prepare":
        result = prepare_search_quality_study(
            discovery_root=args.discovery_root,
            initial_result_root=args.initial_result_root,
            confirmation_result_root=args.confirmation_result_root,
            evidence_path=args.evidence_path,
            output_root=args.output_root,
            expected_report_sha256=args.expected_report_sha256,
            expected_manifest_sha256=args.expected_manifest_sha256,
            expected_evidence_file_sha256=args.expected_evidence_file_sha256,
            source_autoresearch_commit=args.source_autoresearch_commit,
            analysis_autoresearch_commit=args.analysis_autoresearch_commit,
            fuzzfolio_commit=args.fuzzfolio_commit,
            worker_contract_sha256=args.worker_contract_sha256,
        )
    elif args.command == "freeze-control":
        result = freeze_control_study(
            discovery_root=args.discovery_root,
            quality_root=args.quality_root,
            sample_size=args.sample_size,
        )
    elif args.command == "finalize":
        result = finalize_search_quality_study(
            discovery_root=args.discovery_root,
            initial_result_root=args.initial_result_root,
            confirmation_result_root=args.confirmation_result_root,
            control_result_root=args.control_result_root,
            quality_root=args.quality_root,
        )
    else:
        result = audit_search_quality_study(quality_root=args.quality_root)
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
