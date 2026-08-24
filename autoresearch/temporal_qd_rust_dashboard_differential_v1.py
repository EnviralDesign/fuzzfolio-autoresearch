"""Machine-generated Rust/Dashboard differential for retained native V38 material.

This audit is read-only with respect to runs and never dispatches evaluation.
Rust reconstruction/validation/compilation and the frozen Dashboard JSONL
authority are executed independently from identical profile inputs.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import subprocess
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Iterable, Mapping

from .evidence_plan import canonical_json, canonical_sha256


ROOT = Path(__file__).resolve().parents[1]
DASHBOARD = Path(r"C:\fuzzfolio-research\authority-checkouts\dashboard-0cb5951c")
V38 = ROOT / "runs/temporal-qd-v5-fast-ephemeral-operator-family-matrix-20260820-v38"
PARENT_MATERIAL = V38 / "run/g2-parents-800/generations/generation-0003/proposal/parent-material.jsonl"
FROZEN_AUTHORITY = V38 / (
    "run/g2-parents-800/generations/generation-0003/proposal/native-batch/v5-proposal/"
    "490cac548bd735945219a8c3d85add4348d476f6190b238b2371255d37391c72/frozen-authority.json"
)
PAIR_RUN_CONFIG = ROOT / "runs/temporal-qd-v5-native-4000x1024x5-20260813-v1/authority/pair-run-config.json"
NATIVE_BIN = ROOT / "rust/temporal-qd/target/debug/temporal-qd-native-authority-jsonl.exe"
OUTPUT = ROOT / "research/temporal-qd/rust-canonical-authority-v1"
TARGET = "qd_001958c8b3288892a458207c9b76"
MATRIX_PARENTS = (
    "qd_ed27f99ba0a8dfd7c76c69687efb",
    "qd_69e5a3407ab21e82d787eb48c8d5",
    "qd_19e9a2130a8f91feea60349066ca",
    "qd_006856a8defe4a0768fd4076f8f1",
    TARGET,
)
RUST_REQUEST_SCHEMA = "temporal_qd_native_authority_jsonl_request_v1"


def _json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def _sha_file(path: Path) -> str:
    return "sha256:" + hashlib.sha256(path.read_bytes()).hexdigest()


def _git(repo: Path, *args: str) -> str:
    return subprocess.run(
        ["git", "-C", str(repo), *args], check=True, text=True, capture_output=True
    ).stdout.strip()


def _git_paths_match(repo: Path, commit: str, paths: Iterable[str]) -> bool:
    return subprocess.run(
        ["git", "-C", str(repo), "diff", "--quiet", commit, "--", *paths],
        check=False,
    ).returncode == 0


def _seal(value: Mapping[str, Any], field: str = "reportSha256") -> dict[str, Any]:
    result = dict(value)
    result[field] = canonical_sha256(result)
    return result


def _write(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(canonical_json(value) + "\n", encoding="utf-8", newline="\n")


class JsonlProcess:
    def __init__(self, command: list[str], *, cwd: Path, env: Mapping[str, str] | None = None):
        self.process = subprocess.Popen(
            command,
            cwd=cwd,
            env=dict(env) if env else None,
            stdin=subprocess.PIPE,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            bufsize=1,
        )

    def request(self, value: Mapping[str, Any]) -> dict[str, Any]:
        assert self.process.stdin is not None and self.process.stdout is not None
        self.process.stdin.write(canonical_json(value) + "\n")
        self.process.stdin.flush()
        line = self.process.stdout.readline()
        if not line:
            error = self.process.stderr.read() if self.process.stderr else ""
            raise RuntimeError(f"JSONL authority exited without response: {error}")
        response = json.loads(line)
        if response.get("status") == "error" or response.get("semanticExitCode") not in (None, 0):
            raise RuntimeError(f"JSONL authority rejected request: {canonical_json(response)}")
        return response

    def close(self) -> None:
        if self.process.stdin:
            self.process.stdin.close()
        self.process.wait(timeout=30)
        if self.process.returncode:
            error = self.process.stderr.read() if self.process.stderr else ""
            raise RuntimeError(f"JSONL authority failed with {self.process.returncode}: {error}")


def _rows() -> list[dict[str, Any]]:
    with PARENT_MATERIAL.open(encoding="utf-8") as handle:
        return [json.loads(line) for line in handle]


def _family_side(row: Mapping[str, Any]) -> tuple[str, str]:
    delta = row["pairPayload"]["proposalDelta"]
    accepted = row["pairPayload"]["acceptedRecord"]
    if accepted["originKind"] == "random_immigrant":
        return "random_immigrant", "both"
    plan = delta.get("terminalOperatorPlan") or {}
    application = delta.get("terminalOperatorApplication") or {}
    program = application.get("childProgram") or {}
    family = str(plan.get("choiceKind") or "unsupported_or_unknown")
    if family == "typed_grammar":
        family = "topology"
    return family, str(program.get("direction") or "unknown")


def select_corpus(rows: Iterable[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    """Freeze selection before compilation: five parents + first two IDs/family/side."""

    rows = list(rows)
    by_id = {row["candidateId"]: row for row in rows}
    missing = sorted(set(MATRIX_PARENTS) - set(by_id))
    if missing:
        raise RuntimeError(f"required matrix parents are missing: {missing}")
    selected = {candidate_id: by_id[candidate_id] for candidate_id in MATRIX_PARENTS}
    strata: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    exact_clones: list[dict[str, Any]] = []
    crossover: list[dict[str, Any]] = []
    for row in rows:
        family, side = _family_side(row)
        strata[(family, side)].append(row)
        delta = row["pairPayload"]["proposalDelta"]
        plan = delta.get("terminalOperatorPlan") or {}
        application = delta.get("terminalOperatorApplication") or {}
        audit = application.get("applicationAudit") or {}
        if audit.get("childProgramSha256") == plan.get("parentProgramSha256"):
            exact_clones.append(row)
        if delta.get("scheduledKind") == "same_side_crossover":
            crossover.append(row)
    for key in sorted(strata):
        for row in sorted(strata[key], key=lambda item: item["candidateId"])[:2]:
            selected[row["candidateId"]] = row
    for row in sorted(exact_clones, key=lambda item: item["candidateId"])[:2]:
        selected[row["candidateId"]] = row
    for row in sorted(crossover, key=lambda item: item["candidateId"])[:2]:
        selected[row["candidateId"]] = row
    chosen = [selected[key] for key in sorted(selected)]
    rule = {
        "schemaVersion": "temporal_qd_v38_differential_corpus_rule_v1",
        "ruleFrozenBeforeOutcomes": True,
        "requiredCandidateIds": list(MATRIX_PARENTS),
        "stratification": "lexicographically_lowest_two_candidate_ids_per_terminal_family_and_mutated_side",
        "exactCloneRule": "lexicographically_lowest_two_when_childProgramSha256_equals_parentProgramSha256",
        "crossoverRule": "lexicographically_lowest_two_when_scheduledKind_is_same_side_crossover",
        "retainedRowCount": len(rows),
        "selectedCount": len(chosen),
        "exactCloneAvailableCount": len(exact_clones),
        "crossoverAvailableCount": len(crossover),
        "selectedCandidateIds": [row["candidateId"] for row in chosen],
    }
    return chosen, _seal(rule, "selectionRuleSha256")


def _native_validate(profile: Mapping[str, Any], candidate_id: str, request_id: str) -> dict[str, Any]:
    return {
        "schemaVersion": RUST_REQUEST_SCHEMA,
        "requestId": request_id,
        "operation": "validate_native_profile",
        "candidateId": candidate_id,
        "profile": profile,
    }


def _dashboard_validate(profile: Mapping[str, Any], candidate_id: str, request_id: str) -> dict[str, Any]:
    return {
        "schemaVersion": "temporal_search_candidate_validation_jsonl_request_v1",
        "operation": "validate_candidate",
        "requestId": request_id,
        "candidateId": candidate_id,
        "expectedRawSourceProfileSha256": canonical_sha256(profile),
        "sourceProfile": profile,
    }


def _classify(path: str, left: Any, right: Any, *, context: str) -> str:
    lower = path.lower()
    if lower.endswith("/holdpolicy/onbreach"):
        # Dashboard materializes its documented exit_next_open default while
        # the native Rust identity deliberately canonicalizes that default to
        # omission.  The semantic decision memo records the equivalence; the
        # representation is still called out because it changes hashes.
        return "normalization_difference_with_possible_semantic_effect"
    if any(token in lower for token in ("/name", "/description", "/candidateid", "/isactive")):
        return "candidate_or_name_metadata_only"
    if context == "validation_report":
        if "sha256" in lower:
            return "identity_material_only"
        return "validation_report_metadata_only"
    if "sha256" in lower or "sourceprofile" in lower or "sourceprogram" in lower:
        return "identity_material_only"
    if any(token in lower for token in ("/transitions", "/guard", "/actions", "/initialstateid", "/eventbindings", "/evidencegroups", "/managementlibrary", "/indicators")):
        return "semantic_executable_difference"
    if left is None or right is None:
        return "normalization_difference_with_possible_semantic_effect"
    return "unsupported_or_unknown"


def _diff(left: Any, right: Any, *, context: str, path: str = "") -> list[dict[str, Any]]:
    if left == right:
        return []
    if isinstance(left, dict) and isinstance(right, dict):
        output: list[dict[str, Any]] = []
        for key in sorted(set(left) | set(right)):
            child = f"{path}/{key}"
            if key not in left or key not in right:
                lvalue = left.get(key)
                rvalue = right.get(key)
                output.append({"path": child, "rust": lvalue, "dashboard": rvalue, "classification": _classify(child, lvalue, rvalue, context=context)})
            else:
                output.extend(_diff(left[key], right[key], context=context, path=child))
        return output
    if isinstance(left, list) and isinstance(right, list):
        if sorted(map(canonical_json, left)) == sorted(map(canonical_json, right)):
            return [{"path": path, "rust": left, "dashboard": right, "classification": "canonical_ordering_or_serialization_only"}]
        output = []
        for index in range(max(len(left), len(right))):
            lvalue = left[index] if index < len(left) else None
            rvalue = right[index] if index < len(right) else None
            output.extend(_diff(lvalue, rvalue, context=context, path=f"{path}/{index}"))
        return output
    return [{"path": path or "/", "rust": left, "dashboard": right, "classification": _classify(path, left, right, context=context)}]


def _profile_features(profile: Mapping[str, Any]) -> dict[str, Any]:
    graph = profile["graph"]
    transitions = graph["transitions"]
    actions = Counter(
        action.get("kind", "unknown")
        for transition in transitions
        for action in transition.get("actions", [])
    )
    guards = Counter(transition.get("guard", {}).get("kind", "implicit") for transition in transitions)
    return {
        "stateCount": len(graph["states"]),
        "transitionCount": len(transitions),
        "eventBindingCount": len(graph["eventBindings"]),
        "evidenceGroupCount": len(graph["evidenceGroups"]),
        "actionPattern": dict(sorted(actions.items())),
        "guardPattern": dict(sorted(guards.items())),
    }


def _dashboard_identity_material(
    interpreter: str, python_path: str, profiles: Mapping[str, Any]
) -> dict[str, Any]:
    code = (
        "import json,sys;"
        "from fuzzfolio_core.temporal_graph.graph_models import TemporalGraphProfile as P;"
        "from fuzzfolio_core.temporal_graph.identity import normalized_profile_snapshot_payload as S,normalized_program_payload as G;"
        "v=json.load(sys.stdin);"
        "json.dump({k:{'profileSnapshot':S(P.model_validate(p)),'program':G(P.model_validate(p))} for k,p in v.items()},sys.stdout,sort_keys=True,separators=(',',':'))"
    )
    env = dict(os.environ)
    env["PYTHONPATH"] = python_path
    result = subprocess.run(
        [interpreter, "-c", code],
        input=canonical_json(profiles),
        text=True,
        capture_output=True,
        env=env,
        cwd=DASHBOARD,
        check=True,
    )
    return json.loads(result.stdout)


def _divergence_summary(
    rust_validation: Mapping[str, Any], dashboard_validation: Mapping[str, Any]
) -> dict[str, Any]:
    rust = rust_validation["result"]["validation"]
    dashboard = dashboard_validation["report"]
    return {
        "rawProfileEqual": rust["rawProfileSha256"] == dashboard["rawSourceProfileSha256"],
        "profileSnapshotEqual": rust["profileSnapshotSha256"] == dashboard["profileSnapshotSha256"],
        "programEqual": rust["programSha256"] == dashboard["programSha256"],
        "validationReportEqual": rust["validationReportSha256"] == dashboard["validationReportSha256"],
    }


def run(output_dir: Path, *, rust_source_commit: str | None = None) -> dict[str, Path]:
    config = _json(PAIR_RUN_CONFIG)
    rust_source_commit = rust_source_commit or _git(ROOT, "rev-parse", "HEAD")
    dashboard_binding = config["nativeJsonlAuthority"]
    command = list(dashboard_binding["command"]) + ["--jsonl-server", "--jsonl-max-line-bytes", str(dashboard_binding["maxLineBytes"])]
    env = dict(os.environ)
    env["PYTHONPATH"] = os.pathsep.join(dashboard_binding["environment"]["PYTHONPATH"])
    shared_authority = _json(FROZEN_AUTHORITY)
    selected, selection_rule = select_corpus(_rows())
    source_manifest = _seal({
        "schemaVersion": "temporal_qd_cross_compiler_source_manifest_v1",
        "rust": {
            "repository": str(ROOT),
            "gitCommit": rust_source_commit,
            "workingTreeDirty": bool(_git(ROOT, "status", "--porcelain")),
            "authoritySourceFilesMatchGitCommit": _git_paths_match(
                ROOT,
                rust_source_commit,
                [
                    "rust/temporal-qd/Cargo.toml",
                    "rust/temporal-qd/Cargo.lock",
                    "rust/temporal-qd/crates/qd-kernel/src/v5.rs",
                    "rust/temporal-qd/crates/qd-kernel/src/v5_operators.rs",
                    "rust/temporal-qd/crates/qd-kernel/src/bin/temporal-qd-native-authority-jsonl.rs",
                    "rust/temporal-qd/crates/qd-campaign-freeze/src/lib.rs",
                ],
            ),
            "cargoLockSha256": _sha_file(ROOT / "rust/temporal-qd/Cargo.lock"),
            "kernelV5Sha256": _sha_file(ROOT / "rust/temporal-qd/crates/qd-kernel/src/v5.rs"),
            "jsonlSeamSha256": _sha_file(ROOT / "rust/temporal-qd/crates/qd-kernel/src/bin/temporal-qd-native-authority-jsonl.rs"),
            "binarySha256": _sha_file(NATIVE_BIN),
        },
        "dashboard": {
            "repository": str(DASHBOARD),
            "gitCommit": _git(DASHBOARD, "rev-parse", "HEAD"),
            "workingTreeDirty": bool(_git(DASHBOARD, "status", "--porcelain")),
            "validatorScriptSha256": _sha_file(Path(dashboard_binding["validatorScriptPath"])),
            "temporalGraphManifestSha256": dashboard_binding["authorityContent"]["dashboardTemporalGraphContentSha256"],
            "authorityIdentity": config["nativeAuthority"],
            "compilerAuthorityIdentity": config["pairCompilerAuthority"],
        },
        "inputs": {
            "pairRunConfigSha256": config["pairRunConfigSha256"],
            "sharedAuthoritySha256": shared_authority["authoritySha256"],
            "parentMaterialRawSha256": _sha_file(PARENT_MATERIAL),
            "selectionRuleSha256": selection_rule["selectionRuleSha256"],
        },
        "canonicalJson": {
            "rust": "temporal_qd_contract::canonical_json_line",
            "dashboard": "fuzzfolio_core.temporal_graph.identity.canonical_json",
            "harness": "autoresearch.evidence_plan.canonical_json",
        },
    }, "sourceManifestSha256")

    native = JsonlProcess([str(NATIVE_BIN)], cwd=ROOT)
    dashboard = JsonlProcess(command, cwd=Path(dashboard_binding["dashboardSourceRoot"]), env=env)
    target_transcript: dict[str, Any] = {}
    cases: list[dict[str, Any]] = []
    try:
        for row in selected:
            candidate_id = row["candidateId"]
            reconstruct_request = {
                "schemaVersion": RUST_REQUEST_SCHEMA,
                "requestId": f"{candidate_id}-reconstruct",
                "operation": "reconstruct_compact_parent",
                "candidateId": candidate_id,
                "sharedAuthority": shared_authority,
                "parentMaterialRow": row,
            }
            reconstruct_response = native.request(reconstruct_request)
            envelope = reconstruct_response["result"]["candidateEnvelope"]
            candidate = envelope["candidate"]
            long_profile, short_profile = candidate["long"]["profile"], candidate["short"]["profile"]
            long_id, short_id = candidate["long"]["nativeReport"]["candidateId"], candidate["short"]["nativeReport"]["candidateId"]
            pair_id = candidate["validation"]["candidateId"]
            requests = {
                "rustLongValidation": _native_validate(long_profile, long_id, f"{candidate_id}-rust-long"),
                "rustShortValidation": _native_validate(short_profile, short_id, f"{candidate_id}-rust-short"),
                "dashboardLongValidation": _dashboard_validate(long_profile, long_id, f"{candidate_id}-dashboard-long"),
                "dashboardShortValidation": _dashboard_validate(short_profile, short_id, f"{candidate_id}-dashboard-short"),
            }
            responses = {
                "rustLongValidation": native.request(requests["rustLongValidation"]),
                "rustShortValidation": native.request(requests["rustShortValidation"]),
                "dashboardLongValidation": dashboard.request(requests["dashboardLongValidation"]),
                "dashboardShortValidation": dashboard.request(requests["dashboardShortValidation"]),
            }
            long_sources = responses["rustLongValidation"]["result"]["validation"]
            short_sources = responses["rustShortValidation"]["result"]["validation"]
            requests["rustPairCompile"] = {
                "schemaVersion": RUST_REQUEST_SCHEMA,
                "requestId": f"{candidate_id}-rust-pair",
                "operation": "compile_bidirectional_profile",
                "candidateId": pair_id,
                "longProfile": long_profile,
                "shortProfile": short_profile,
                "longSourceIdentities": {"profileSnapshotSha256": long_sources["profileSnapshotSha256"], "programSha256": long_sources["programSha256"]},
                "shortSourceIdentities": {"profileSnapshotSha256": short_sources["profileSnapshotSha256"], "programSha256": short_sources["programSha256"]},
            }
            requests["dashboardPairCompile"] = {
                "schemaVersion": "temporal_search_bidirectional_compile_jsonl_request_v1",
                "operation": "compile_bidirectional",
                "requestId": f"{candidate_id}-dashboard-pair",
                "candidateId": pair_id,
                "longProfile": long_profile,
                "shortProfile": short_profile,
                "expectedLongRawSourceProfileSha256": canonical_sha256(long_profile),
                "expectedShortRawSourceProfileSha256": canonical_sha256(short_profile),
            }
            responses["rustPairCompile"] = native.request(requests["rustPairCompile"])
            responses["dashboardPairCompile"] = dashboard.request(requests["dashboardPairCompile"])
            rust_pair = responses["rustPairCompile"]["result"]
            dash_pair = responses["dashboardPairCompile"]["result"]
            accepted = row["pairPayload"]["acceptedRecord"]
            family, side = _family_side(row)
            case = {
                "candidateId": candidate_id,
                "originKind": accepted["originKind"],
                "operatorFamily": family,
                "mutatedSide": side,
                "historicalReconstructionExact": candidate["identities"]["rawPairSha256"] == accepted["compiled"]["rawPairSha256"] and candidate["identities"]["profileSha256"] == accepted["compiled"]["profileSnapshotSha256"] and candidate["identities"]["programSha256"] == accepted["compiled"]["programSha256"],
                "long": _divergence_summary(responses["rustLongValidation"], responses["dashboardLongValidation"]),
                "short": _divergence_summary(responses["rustShortValidation"], responses["dashboardShortValidation"]),
                "pair": {
                    "rawProfileEqual": rust_pair["validation"]["rawProfileSha256"] == dash_pair["rawSourceProfileSha256"],
                    "profileSnapshotEqual": rust_pair["validation"]["profileSnapshotSha256"] == dash_pair["profileSnapshotSha256"],
                    "programEqual": rust_pair["validation"]["programSha256"] == dash_pair["programSha256"],
                    "validationReportEqual": rust_pair["validation"]["validationReportSha256"] == dash_pair["validationReportSha256"],
                },
                "topologySignature": {"long": accepted["long"]["semanticTopologySha256"], "short": accepted["short"]["semanticTopologySha256"]},
                "resourceFingerprint": {"long": accepted["long"]["resourceFingerprintSha256"], "short": accepted["short"]["resourceFingerprintSha256"]},
                "profileFeatures": {"long": _profile_features(long_profile), "short": _profile_features(short_profile)},
                "requestResponseSha256": {key: {"request": canonical_sha256(requests[key]), "response": canonical_sha256(responses[key])} for key in sorted(requests)},
            }
            cases.append(case)
            if candidate_id == TARGET:
                pinned_material = _dashboard_identity_material(
                    dashboard_binding["interpreterPath"],
                    os.pathsep.join(dashboard_binding["environment"]["PYTHONPATH"]),
                    {"long": long_profile, "short": short_profile, "pair": dash_pair["profile"]},
                )
                target_transcript = {
                    "schemaVersion": "temporal_qd_target_cross_compiler_transcript_v1",
                    "candidateId": candidate_id,
                    "sourceManifestSha256": source_manifest["sourceManifestSha256"],
                    "historicalAcceptedCompactRecord": accepted,
                    "reconstruction": {"request": reconstruct_request, "response": reconstruct_response},
                    "requests": requests,
                    "responses": responses,
                    "canonicalDiffs": {
                        "longProfileSnapshot": _diff(long_sources["identityMaterial"]["profileSnapshot"], pinned_material["long"]["profileSnapshot"], context="profile"),
                        "longProgram": _diff(long_sources["identityMaterial"]["program"], pinned_material["long"]["program"], context="program"),
                        "longValidationReport": _diff(long_sources["report"], responses["dashboardLongValidation"]["report"], context="validation_report"),
                        "shortProfileSnapshot": _diff(short_sources["identityMaterial"]["profileSnapshot"], pinned_material["short"]["profileSnapshot"], context="profile"),
                        "shortProgram": _diff(short_sources["identityMaterial"]["program"], pinned_material["short"]["program"], context="program"),
                        "shortValidationReport": _diff(short_sources["report"], responses["dashboardShortValidation"]["report"], context="validation_report"),
                        "compiledPairProfile": _diff(rust_pair["compiledProfile"], dash_pair["profile"], context="profile"),
                        "pairProgram": _diff(rust_pair["validation"]["identityMaterial"]["program"], pinned_material["pair"]["program"], context="program"),
                        "pairValidationReport": _diff(rust_pair["validation"]["report"], dash_pair["report"], context="validation_report"),
                    },
                    "marketEvaluationLaunched": False,
                    "workerDispatched": False,
                    "generationLaunched": False,
                }
                target_transcript = _seal(target_transcript, "transcriptSha256")
    finally:
        native.close()
        dashboard.close()

    def rates(key: str) -> dict[str, Any]:
        groups: dict[str, list[bool]] = defaultdict(list)
        for case in cases:
            for dimension in ("originKind", "operatorFamily", "mutatedSide"):
                groups[f"{dimension}:{case[dimension]}"] += [case[key]["programEqual"]]
        return {name: {"count": len(values), "divergentCount": sum(not value for value in values), "divergenceRate": sum(not value for value in values) / len(values)} for name, values in sorted(groups.items())}

    sweep = _seal({
        "schemaVersion": "temporal_qd_v38_cross_compiler_sweep_v1",
        "sourceManifestSha256": source_manifest["sourceManifestSha256"],
        "selectionRule": selection_rule,
        "cases": cases,
        "divergenceRates": {"longProgram": rates("long"), "shortProgram": rates("short"), "pairProgram": rates("pair")},
        "coverage": {
            "origins": dict(Counter(case["originKind"] for case in cases)),
            "families": dict(Counter(case["operatorFamily"] for case in cases)),
            "sides": dict(Counter(case["mutatedSide"] for case in cases)),
            "allFiveMatrixParentsIncluded": all(candidate in {case["candidateId"] for case in cases} for candidate in MATRIX_PARENTS),
        },
        "marketEvaluationLaunched": False,
        "workerDispatched": False,
        "generationLaunched": False,
    })
    paths = {
        "source": output_dir / "cross-compiler-source-manifest-v1.json",
        "target": output_dir / "target-cross-compiler-transcript-v1.json",
        "sweep": output_dir / "v38-cross-compiler-sweep-v1.json",
    }
    _write(paths["source"], source_manifest)
    _write(paths["target"], target_transcript)
    _write(paths["sweep"], sweep)
    return paths


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", type=Path, default=OUTPUT)
    parser.add_argument("--rust-source-commit")
    args = parser.parse_args()
    for name, path in run(args.output_dir, rust_source_commit=args.rust_source_commit).items():
        print(f"{name}={path}")


if __name__ == "__main__":
    main()
