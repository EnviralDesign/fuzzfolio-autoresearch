"""Profile a bounded, no-market QD proposal-generation slice.

This is a diagnostic harness.  It invokes the normal generation implementation
with ``--max-new-proposals`` and writes its temporary checkpoint/journal under
the caller-supplied output root.  It never contacts the gateway or the lake.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import time

import autoresearch.temporal_qd_evolution as qd
from autoresearch.temporal_qd_pair_factory import PairAuthorityBundle, load_pair_run_config, pair_policy_from_config


def _timed_wrapper(timings: dict[str, dict[str, float]], name: str, function):
    def wrapped(*args, **kwargs):
        started = time.perf_counter()
        try:
            return function(*args, **kwargs)
        finally:
            row = timings.setdefault(name, {"calls": 0, "seconds": 0.0})
            row["calls"] += 1
            row["seconds"] += time.perf_counter() - started

    return wrapped


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--parent-archive", required=True, type=Path)
    parser.add_argument("--source-preparation", type=Path)
    parser.add_argument("--base-generator-root", type=Path)
    parser.add_argument("--confirmed-entry-admission-root", type=Path)
    parser.add_argument("--validator-command-file", type=Path)
    parser.add_argument("--reference-config", required=True, type=Path)
    parser.add_argument("--construction-catalog", required=True, type=Path)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--generation-index", required=True, type=int)
    parser.add_argument("--immigrant-continuation-start", required=True, type=int)
    parser.add_argument("--max-new-proposals", required=True, type=int)
    parser.add_argument("--prewarm-continuation", action="store_true")
    parser.add_argument("--bidirectional-pair-config", type=Path, help="closed pair authority JSON; profiles pair generation without v2 continuation")
    args = parser.parse_args()

    if args.bidirectional_pair_config is None and any(value is None for value in (args.source_preparation, args.base_generator_root, args.confirmed_entry_admission_root, args.validator_command_file)):
        parser.error("legacy profiling requires v2 source paths and --validator-command-file")

    config = json.loads(args.reference_config.read_text(encoding="utf-8"))
    command = json.loads(args.validator_command_file.read_text(encoding="utf-8")) if args.validator_command_file is not None else []
    if not isinstance(command, list) or not all(isinstance(item, str) for item in command):
        raise ValueError("validator command file must contain a string array")

    prewarm_seconds: float | None = None
    if args.prewarm_continuation and args.bidirectional_pair_config is None:
        prewarm_started = time.perf_counter()
        qd.ExactGeneratorV2Continuation(
            source_preparation_path=args.source_preparation or args.output_root / ".pair-mode-unused-source.json",
            base_generator_root=args.base_generator_root or args.output_root / ".pair-mode-unused-generator",
            confirmed_entry_admission_root=args.confirmed_entry_admission_root or args.output_root / ".pair-mode-unused-admission",
            start_continuation_ordinal=args.immigrant_continuation_start,
        )
        prewarm_seconds = time.perf_counter() - prewarm_started

    timings: dict[str, dict[str, float]] = {}
    patched = {
        "_ledger_bootstrap_archive": qd._ledger_bootstrap_archive,
        "_ledger_duplicate_check": qd._ledger_duplicate_check,
        "_ledger_accept": qd._ledger_accept,
        "_ledger_refresh_counts": qd._ledger_refresh_counts,
        "_save_identity_ledger": qd._save_identity_ledger,
        "_proposal_accounting": qd._proposal_accounting,
        "_structural_proposal": qd._structural_proposal,
        "ExactGeneratorV2Continuation": qd.ExactGeneratorV2Continuation,
        "SubprocessCandidateValidator.validate": qd.SubprocessCandidateValidator.validate,
    }
    for name in (
        "_ledger_bootstrap_archive",
        "_ledger_duplicate_check",
        "_ledger_accept",
        "_ledger_refresh_counts",
        "_save_identity_ledger",
        "_proposal_accounting",
        "_structural_proposal",
    ):
        setattr(qd, name, _timed_wrapper(timings, name, patched[name]))
    qd.ExactGeneratorV2Continuation = _timed_wrapper(
        timings, "ExactGeneratorV2Continuation", patched["ExactGeneratorV2Continuation"]
    )
    qd.SubprocessCandidateValidator.validate = _timed_wrapper(
        timings,
        "SubprocessCandidateValidator.validate",
        patched["SubprocessCandidateValidator.validate"],
    )
    started = time.perf_counter()
    try:
        generation_kwargs = dict(
            parent_archive_path=args.parent_archive,
            source_preparation_path=args.source_preparation,
            base_generator_root=args.base_generator_root,
            confirmed_entry_admission_root=args.confirmed_entry_admission_root,
            validator_command=command,
            output_root=args.output_root,
            generation_index=args.generation_index,
            immigrant_continuation_start=args.immigrant_continuation_start,
            parameters=config["parameters"],
            evidence_identity_context=config["predeclaredEvidenceContext"],
            construction_catalog_path=args.construction_catalog,
            max_new_proposals=args.max_new_proposals,
        )
        if args.bidirectional_pair_config is None:
            result = qd.generate_qd_generation(**generation_kwargs)
        else:
            frozen = load_pair_run_config(json.loads(args.bidirectional_pair_config.read_text(encoding="utf-8")))
            with PairAuthorityBundle(frozen) as authority:
                result = qd.generate_qd_generation(
                    **generation_kwargs,
                    bidirectional_pair_policy=pair_policy_from_config(frozen),
                    bidirectional_pair_factory=authority.factory,
                    bidirectional_module_authority=authority.operator,
                    bidirectional_native_validator=authority.validator,
                    bidirectional_pair_compiler=authority.compiler,
                    bidirectional_operator_implementation_identity=frozen["operatorImplementation"],
                )
    finally:
        for name, function in patched.items():
            if name == "SubprocessCandidateValidator.validate":
                qd.SubprocessCandidateValidator.validate = function
            else:
                setattr(qd, name, function)
    timings["generate_qd_generation"] = {"calls": 1, "seconds": time.perf_counter() - started}
    if prewarm_seconds is not None:
        timings["continuation_cache_prewarm"] = {
            "calls": 1,
            "seconds": prewarm_seconds,
        }
    for row in timings.values():
        row["meanMilliseconds"] = round(1000.0 * row["seconds"] / row["calls"], 3)
    args.output_root.mkdir(parents=True, exist_ok=True)
    (args.output_root / "generation-timing.json").write_text(
        json.dumps(timings, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    (args.output_root / "generation-result.json").write_text(
        json.dumps(result, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
