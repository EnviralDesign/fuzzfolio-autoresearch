"""No-market native activation canary for accepted typed temporal artifacts.

The only evaluator used here is the Dashboard compute environment.  Supplied
observation streams are already immutable temporal observations: this command
does not import a lake, contact a Gateway, or synthesize trading decisions.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from typing import Any, Mapping

from .temporal_search import TemporalSearchContractError, canonical_sha256


INPUT_SCHEMA = "temporal_prebroad_activation_canary_input_v1"
REPORT_SCHEMA = "temporal_prebroad_activation_canary_report_v1"
MANIFEST_SCHEMA = "temporal_prebroad_activation_canary_manifest_v1"
DEFAULT_DASHBOARD_PYTHON = Path("C:/repos/Trading-Dashboard/compute-service/.venv/Scripts/python.exe")

# This is intentionally a small transport program, not a reimplementation of
# the kernel.  It calls the Dashboard compiler, native search validator, and
# sequential replay APIs in the compute-service environment.
_DASHBOARD_DRIVER = r'''
import json, os, sys, types
# The compute-service environment is intentionally only the Dashboard
# authority.  Register the source directory as a namespace package so this
# narrow driver can import the grammar without running autoresearch's optional
# coordinator/bootstrap side effects (and their UI-only dependencies).
if 'autoresearch' not in sys.modules:
    package=types.ModuleType('autoresearch'); package.__path__=[os.environ['AUTORESEARCH_SOURCE_ROOT']]; sys.modules['autoresearch']=package
from fuzzfolio_core.temporal_graph.bidirectional_compiler import compile_bidirectional_profile
from fuzzfolio_core.temporal_graph.graph_models import TemporalGraphProfile
from fuzzfolio_core.temporal_graph.identity import canonical_sha256
from fuzzfolio_core.temporal_graph.observation_models import TemporalObservationStream
from fuzzfolio_core.temporal_graph.replay_checkpoint import TemporalReplayCheckpoint
from fuzzfolio_core.temporal_graph.replay_engine import advance_temporal_replay
from fuzzfolio_core.temporal_graph.replay_metrics import finish_temporal_replay, run_temporal_replay
from fuzzfolio_core.temporal_graph.search_validation import validate_temporal_search_candidate
from autoresearch.temporal_bidirectional_genome import normalize_behaviorally_redundant_transitions
from autoresearch.temporal_typed_motif_grammar import Fragment, GrammarContext, ModuleProgram, TypedFragmentGrammar, compiled_graph_signature

def dump(value):
    return value.model_dump(mode='json', by_alias=True, exclude_none=False)

def outcome(profile, result):
    data=dump(result); graph=data['graphTraces']; conflict=profile.graph.entry_arbitration.conflict_transition_id
    if any(row['transitionId']==conflict for row in graph): return 'conflict_abstention'
    directions={row['direction'] for row in data['trades']}
    position=(data['finalExecutionState'] or {}).get('position') or {}
    if position.get('direction'): directions.add(position['direction'])
    if directions == {'long'}: return 'long'
    if directions == {'short'}: return 'short'
    if not directions: return 'neither'
    raise ValueError('scenario produced more than one entry direction')

class Authority:
    def validate_v2(self, *, profile, candidate_id): return validate_temporal_search_candidate(profile, candidate_id=candidate_id)
    def compile_pair(self, *, long_profile, short_profile, candidate_id):
        profile=dump(compile_bidirectional_profile(TemporalGraphProfile.model_validate(long_profile), TemporalGraphProfile.model_validate(short_profile), name='QD bidirectional '+candidate_id))
        return {'profile':profile, 'validation':validate_temporal_search_candidate(profile, candidate_id=candidate_id)}

def module(module, side, candidate):
    context_raw=module['context']; canonical=module['program']
    context=GrammarContext(instrument=context_raw['instrument'], indicators=tuple(context_raw['indicators']), evidence_groups=tuple(context_raw['evidenceGroups']), event_bindings=tuple(context_raw['eventBindings']), execution_config=context_raw['executionConfig'], budgets=context_raw.get('budgets'))
    fragments=tuple(Fragment(uid='artifact_%d' % index, production_id=item['productionId'], resources=item['resources'], choices=item['choices']) for index,item in enumerate(canonical['fragments']))
    program=ModuleProgram(canonical['direction'], fragments)
    grammar=TypedFragmentGrammar(context, native_authority=Authority())
    # v2 typed-fragment bytes are a sealed historical reader format.  The
    # active compiler writes v3, but the canary must be able to validate a
    # pre-broad v2 artifact against its original immutable program identity
    # before compiling its semantics with the current Dashboard authority.
    program_payload=grammar.canonical_program(program)
    if canonical.get('grammarVersion') == '2': program_payload={**program_payload, 'grammarVersion':'2'}
    if canonical.get('grammarVersion') not in ('2','3') or canonical_sha256(program_payload) != module['programSha256'] or canonical_sha256(grammar.context) != module['contextSha256']:
        raise ValueError('typed module canonical program/context identity drifted')
    canonical_program,_built,authored_profile=grammar._profile_payload(program)
    profile,deduplication=normalize_behaviorally_redundant_transitions(authored_profile)
    report=Authority().validate_v2(profile=profile,candidate_id=candidate+'_'+side)
    compiled=grammar._compiled(program, canonical_program, profile, report, candidate_id=candidate+'_'+side)
    artifact=module['nativeArtifact']; aliases=module.get('transitionAliases')
    exact={'profileSha256':canonical_sha256(profile), **compiled.identities}
    # The legacy module program hash is over the sealed v2 envelope above;
    # the current compiler reports the equivalent v3 writer envelope.  The
    # profile/report identities must still match exactly, while this one
    # historical representation field is checked against its already-verified
    # v2 bytes rather than being silently upgraded.
    if canonical.get('grammarVersion') == '2': exact['programSha256']=module['programSha256']
    identity_drift=[k for k,v in exact.items() if artifact['identities'].get(k) != v]
    expected_aliases=[]
    for group in deduplication['groups']:
        for removed in group['removedTransitionIds']:
            expected_aliases.append({'removedTransitionId':removed,'survivorTransitionId':group['survivorTransitionId'],'semanticTransitionSha256':group['semanticTransitionSha256']})
    expected_aliases.sort(key=lambda row:(row['removedTransitionId'],row['survivorTransitionId'],row['semanticTransitionSha256']))
    if canonical_sha256(profile) != artifact['profileSha256'] or profile != artifact['profile'] or identity_drift or artifact['validation'] != compiled.native_report or (aliases is not None and (aliases.get('schemaVersion') != 'temporal_prebroad_transition_aliases_v1' or aliases.get('profileSha256') != canonical_sha256(profile) or aliases.get('aliases') != expected_aliases)):
        raise ValueError('content-bound native module artifact identity drifted: profile=%s identities=%s report=%s' % (profile != artifact['profile'], identity_drift, artifact['validation'] != compiled.native_report))
    mapping=[]
    transitions_by_id={row['id']:row for row in profile['graph']['transitions']}
    alias_by_removed={row['removedTransitionId']:row['survivorTransitionId'] for row in expected_aliases}
    for index, fragment in enumerate(compiled.program['fragments']):
        prefix='f%d_%s_' % (index, fragment['productionId'])
        authored_rows=[row for row in authored_profile['graph']['transitions'] if row['id'].startswith(prefix)]
        rows=[]
        for authored_row in authored_rows:
            transition_id=authored_row['id']; resolved=transition_id if transition_id in transitions_by_id else alias_by_removed.get(transition_id)
            if resolved is None or resolved not in transitions_by_id: raise ValueError('grammar compilation omitted a typed fragment production')
            if resolved not in {row['id'] for row in rows}: rows.append(transitions_by_id[resolved])
        if not rows: raise ValueError('grammar compilation omitted a typed fragment production')
        mapping.append({'moduleDirection':side, 'fragmentIndex':index, 'productionId':fragment['productionId'], 'lifecycles':[{'lifecycle':row['reasonCode'].split('.')[-1], 'transitionId':side+'_'+row['id'], 'actionKind':(row['actions'][0]['kind'] if row['actions'] else None)} for row in rows]})
    return compiled, mapping

def one(pair):
    candidate=pair['candidateId']; long,long_map=module(pair['longModule'],'long',candidate); short,short_map=module(pair['shortModule'],'short',candidate); accepted=pair['pair']
    compiled_result=TypedFragmentGrammar(GrammarContext(instrument=pair['longModule']['context']['instrument'], indicators=tuple(pair['longModule']['context']['indicators']), evidence_groups=tuple(pair['longModule']['context']['evidenceGroups']), event_bindings=tuple(pair['longModule']['context']['eventBindings']), execution_config=pair['longModule']['context']['executionConfig'], budgets=pair['longModule']['context'].get('budgets')), native_authority=Authority()).compile_pair(long, short, candidate_id=candidate, pair_authority=Authority())
    compiled_raw=compiled_result['profile']; compiled=TemporalGraphProfile.model_validate(compiled_raw); pair_report=compiled_result['validation']
    if canonical_sha256(compiled_raw) != canonical_sha256(accepted['profile']): raise ValueError('canonical bidirectional compiler does not reproduce supplied pair')
    if not pair_report.get('candidateAcceptable') or compiled_raw.get('version') != 'v3' or compiled_raw.get('directionMode') != 'both':
        raise ValueError('compiled pair failed native validation')
    if accepted['validation'].get('programSha256') != pair_report.get('programSha256') or accepted['validation'].get('validationReportSha256') != pair_report.get('validationReportSha256'):
        raise ValueError('accepted pair native identity drifted')
    scenarios=[]
    for scenario in pair['scenarios']:
        stream=TemporalObservationStream.model_validate(scenario['observationStream'])
        full=run_temporal_replay(compiled, stream, cost_model={'mode':'research_conservative'})
        restart_at=int(scenario['restartAfterObservations'])
        partial=advance_temporal_replay(compiled, stream, cost_model={'mode':'research_conservative'}, max_observations=restart_at)
        restarted=TemporalReplayCheckpoint.model_validate(dump(partial))
        resumed=run_temporal_replay(compiled, stream, cost_model={'mode':'research_conservative'}, checkpoint=restarted)
        full_raw=dump(full); resumed_raw=dump(resumed); stream_raw=dump(stream)
        if canonical_sha256(full_raw) != canonical_sha256(resumed_raw):
            raise ValueError('restart parity failed')
        if full_raw['streamSha256'] != stream_raw['streamSha256'] or resumed_raw['streamSha256'] != stream_raw['streamSha256'] or full_raw['profileSnapshotSha256'] != pair_report['profileSnapshotSha256']:
            raise ValueError('Dashboard replay result identity does not bind compiled profile and stream')
        observed=outcome(compiled, full)
        if observed != scenario['expectedOutcome']:
            raise ValueError('scenario outcome mismatch: expected %s got %s' % (scenario['expectedOutcome'], observed))
        scenarios.append({'scenarioId':scenario['scenarioId'], 'expectedOutcome':scenario['expectedOutcome'], 'observedOutcome':observed, 'resultSha256':full_raw['resultSha256'], 'restartResultSha256':resumed_raw['resultSha256'], 'streamSha256':stream_raw['streamSha256'], 'profileSnapshotSha256':full_raw['profileSnapshotSha256'], 'transitionIds':sorted({row['transitionId'] for row in full_raw['graphTraces']}), 'execution':sorted({'%s:%s' % (row['actionKind'],row['status']) for row in full_raw['executionTraces']})})
    return {'candidateId':candidate, 'compiledProfileSha256':pair_report['profileSnapshotSha256'], 'compiledProgramSha256':pair_report['programSha256'], 'compiledValidationReportSha256':pair_report['validationReportSha256'], 'authoritativeProductionMappings':[ *long_map, *short_map ], 'scenarios':scenarios}

payload=json.load(open(sys.argv[1], encoding='utf-8'))
print(json.dumps({'pairs':[one(item) for item in payload['pairs']]}, sort_keys=True, separators=(',', ':')))
'''


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise TemporalSearchContractError(f"could not read JSON file: {path}") from exc
    if not isinstance(value, dict):
        raise TemporalSearchContractError(f"JSON root must be an object: {path}")
    return value


def _write_immutable(path: Path, value: Mapping[str, Any]) -> None:
    encoded = json.dumps(dict(value), indent=2, sort_keys=True, ensure_ascii=True, allow_nan=False) + "\n"
    path.parent.mkdir(parents=True, exist_ok=True)
    if path.exists() and path.read_text(encoding="utf-8") != encoded:
        raise TemporalSearchContractError(f"refusing to overwrite divergent immutable file: {path}")
    path.write_text(encoded, encoding="utf-8")


def _validate_input(payload: Mapping[str, Any]) -> dict[str, Any]:
    if set(payload) != {"schemaVersion", "pairs"} or payload.get("schemaVersion") != INPUT_SCHEMA:
        raise TemporalSearchContractError("canary input has an unknown or open schema")
    pairs = payload.get("pairs")
    if not isinstance(pairs, list) or not 1 <= len(pairs) <= 32:
        raise TemporalSearchContractError("canary requires a finite non-empty accepted pair set")
    seen: set[str] = set()
    normalized = []
    for index, raw in enumerate(pairs):
        if not isinstance(raw, Mapping) or set(raw) != {"candidateId", "longModule", "shortModule", "pair", "scenarios", "productionClaims"}:
            raise TemporalSearchContractError(f"canary pair {index} has a closed schema")
        candidate = str(raw["candidateId"] or "").strip().lower().replace("-", "_")
        if not candidate or candidate in seen:
            raise TemporalSearchContractError("canary candidate identities must be distinct")
        seen.add(candidate)
        modules = {}
        supplied_productions: set[tuple[str, int, str]] = set()
        for side in ("longModule", "shortModule"):
            module = raw[side]
            legacy_module_fields = {"context", "contextSha256", "program", "programSha256", "nativeArtifact"}
            if not isinstance(module, Mapping) or (set(module) != legacy_module_fields and set(module) != {*legacy_module_fields, "transitionAliases"}):
                raise TemporalSearchContractError(f"{candidate} {side} has a closed schema")
            has_transition_aliases = "transitionAliases" in module
            context, program, native = module["context"], module["program"], module["nativeArtifact"]
            aliases = module["transitionAliases"] if has_transition_aliases else None
            if not isinstance(context, Mapping) or not isinstance(program, Mapping) or not isinstance(native, Mapping) or (has_transition_aliases and not isinstance(aliases, Mapping)):
                raise TemporalSearchContractError(f"{candidate} {side} is incomplete")
            expected_direction = "long" if side == "longModule" else "short"
            if set(program) != {"schemaVersion", "grammarVersion", "direction", "fragments"} or program.get("direction") != expected_direction:
                raise TemporalSearchContractError(f"{candidate} {side} is not an exact canonical ModuleProgram")
            for field in ("contextSha256", "programSha256"):
                value = str(module.get(field) or "")
                if len(value) != 71 or not value.startswith("sha256:"):
                    raise TemporalSearchContractError(f"{candidate} {side} {field} is missing")
            fragments = program.get("fragments")
            if not isinstance(fragments, list) or not fragments:
                raise TemporalSearchContractError(f"{candidate} {side} program has no typed fragments")
            for fragment_index, fragment in enumerate(fragments):
                production = str((fragment or {}).get("productionId") or "") if isinstance(fragment, Mapping) else ""
                if not production:
                    raise TemporalSearchContractError(f"{candidate} {side} has an unnamed fragment production")
                supplied_productions.add((expected_direction, fragment_index, production))
            if set(native) != {"schemaVersion", "profile", "profileSha256", "validation", "identities"} or not isinstance(native.get("profile"), Mapping) or not isinstance(native.get("validation"), Mapping) or not isinstance(native.get("identities"), Mapping):
                raise TemporalSearchContractError(f"{candidate} {side} native artifact has a closed schema")
            normalized_module = {"context": dict(context), "contextSha256": str(module["contextSha256"]), "program": dict(program), "programSha256": str(module["programSha256"]), "nativeArtifact": dict(native)}
            if has_transition_aliases:
                alias_material = {
                    "schemaVersion": aliases.get("schemaVersion"),
                    "profileSha256": aliases.get("profileSha256"),
                    "aliases": aliases.get("aliases"),
                }
                if set(aliases) != {"schemaVersion", "profileSha256", "aliases", "manifestSha256"} or aliases.get("schemaVersion") != "temporal_prebroad_transition_aliases_v1" or aliases.get("profileSha256") != native.get("profileSha256") or not isinstance(aliases.get("aliases"), list) or aliases.get("manifestSha256") != canonical_sha256(alias_material):
                    raise TemporalSearchContractError(f"{candidate} {side} transition aliases are unbound or malformed")
                for alias in aliases["aliases"]:
                    if not isinstance(alias, Mapping) or set(alias) != {"removedTransitionId", "survivorTransitionId", "semanticTransitionSha256"} or not all(isinstance(alias.get(field), str) and alias[field] for field in ("removedTransitionId", "survivorTransitionId", "semanticTransitionSha256")):
                        raise TemporalSearchContractError(f"{candidate} {side} transition alias is malformed")
                normalized_module["transitionAliases"] = dict(aliases)
            modules[side] = normalized_module
        pair = raw["pair"]
        if not isinstance(pair, Mapping) or set(pair) != {"profile", "validation"} or not isinstance(pair["profile"], Mapping) or not isinstance(pair["validation"], Mapping):
            raise TemporalSearchContractError(f"{candidate} pair artifact is incomplete")
        if pair["profile"].get("version") != "v3" or pair["profile"].get("directionMode") != "both" or pair["profile"].get("instruments") != ["EURUSD"]:
            raise TemporalSearchContractError(f"{candidate} pair is not v3/both EURUSD")
        validation = pair["validation"]
        if validation.get("candidateId") != candidate or validation.get("candidateAcceptable") is not True or validation.get("status") != "valid_evaluable":
            raise TemporalSearchContractError(f"{candidate} pair lacks accepted native validation")
        for field in ("programSha256", "validationReportSha256"):
            value = str(validation.get(field) or "")
            if len(value) != 71 or not value.startswith("sha256:"):
                raise TemporalSearchContractError(f"{candidate} pair {field} is missing")
        scenarios = raw["scenarios"]
        expected = {"long", "short", "neither", "conflict_abstention"}
        if not isinstance(scenarios, list) or len(scenarios) != 4:
            raise TemporalSearchContractError(f"{candidate} must supply exactly four activation scenarios")
        normalized_scenarios = []
        for item in scenarios:
            if not isinstance(item, Mapping) or set(item) != {"scenarioId", "expectedOutcome", "restartAfterObservations", "observationStream"}:
                raise TemporalSearchContractError(f"{candidate} scenario has a closed schema")
            stream = item["observationStream"]
            count = len(stream.get("observations") or []) if isinstance(stream, Mapping) else 0
            restart = item["restartAfterObservations"]
            if isinstance(restart, bool) or not isinstance(restart, int):
                raise TemporalSearchContractError(f"{candidate} scenario restart checkpoint must be an integer")
            if str(item["expectedOutcome"] or "") not in expected or not str(item["scenarioId"] or "") or not 0 < restart < count:
                raise TemporalSearchContractError(f"{candidate} scenario is not finite or lacks a required outcome")
            normalized_scenarios.append({"scenarioId": str(item["scenarioId"]), "expectedOutcome": str(item["expectedOutcome"]), "restartAfterObservations": restart, "observationStream": dict(stream)})
        if {item["expectedOutcome"] for item in normalized_scenarios} != expected or len({item["scenarioId"] for item in normalized_scenarios}) != 4:
            raise TemporalSearchContractError(f"{candidate} must cover long, short, neither, and conflict-abstention exactly once")
        claims = raw["productionClaims"]
        if not isinstance(claims, list) or not claims:
            raise TemporalSearchContractError(f"{candidate} requires production lifecycle claims")
        claim_set = set()
        for claim in claims:
            if not isinstance(claim, Mapping) or set(claim) != {"moduleDirection", "fragmentIndex", "scenarioId", "lifecycle", "outcome"}:
                raise TemporalSearchContractError(f"{candidate} production claim has a closed schema")
            fragment_index = claim["fragmentIndex"]
            if isinstance(fragment_index, bool) or not isinstance(fragment_index, int):
                raise TemporalSearchContractError(f"{candidate} production claim fragmentIndex must be an integer")
            matches = [key for key in supplied_productions if key[:2] == (str(claim["moduleDirection"]), fragment_index)]
            key = matches[0] if len(matches) == 1 else None
            if key is None or key in claim_set or str(claim["scenarioId"]) not in {x["scenarioId"] for x in normalized_scenarios} or not str(claim["lifecycle"]) or str(claim["outcome"]) not in {"transition_selected", "action_scheduled", "action_applied"}:
                raise TemporalSearchContractError(f"{candidate} production claim is unbound or invalid")
            claim_set.add(key)
        if claim_set != supplied_productions:
            raise TemporalSearchContractError(f"{candidate} has an unclaimed typed fragment production")
        normalized.append({"candidateId": candidate, **modules, "pair": {"profile": dict(pair["profile"]), "validation": dict(pair["validation"])}, "scenarios": normalized_scenarios, "productionClaims": [dict(x) for x in claims]})
    return {"schemaVersion": INPUT_SCHEMA, "pairs": normalized}


def _run_dashboard(payload: Mapping[str, Any], dashboard_python: Path) -> dict[str, Any]:
    if not dashboard_python.is_file():
        raise TemporalSearchContractError(f"Dashboard compute Python is unavailable: {dashboard_python}")
    with tempfile.TemporaryDirectory(prefix="temporal-prebroad-canary-") as root:
        input_path = Path(root) / "input.json"
        input_path.write_text(json.dumps(dict(payload), sort_keys=True, ensure_ascii=True), encoding="utf-8")
        environment = dict(os.environ)
        project_root = str(Path(__file__).resolve().parents[1])
        environment["PYTHONPATH"] = project_root + (os.pathsep + environment["PYTHONPATH"] if environment.get("PYTHONPATH") else "")
        environment["AUTORESEARCH_SOURCE_ROOT"] = str(Path(__file__).resolve().parent)
        completed = subprocess.run([str(dashboard_python), "-c", _DASHBOARD_DRIVER, str(input_path)], text=True, capture_output=True, check=False, timeout=120, env=environment)
    if completed.returncode != 0:
        raise TemporalSearchContractError(f"Dashboard native canary failed: {completed.stderr.strip()[:1000]}")
    try:
        result = json.loads(completed.stdout)
    except json.JSONDecodeError as exc:
        raise TemporalSearchContractError("Dashboard native canary returned non-JSON") from exc
    if not isinstance(result, dict) or not isinstance(result.get("pairs"), list):
        raise TemporalSearchContractError("Dashboard native canary returned an invalid report")
    return result


def _verify_claims(payload: Mapping[str, Any], kernel: Mapping[str, Any]) -> list[dict[str, Any]]:
    by_candidate = {str(row.get("candidateId")): row for row in kernel["pairs"] if isinstance(row, Mapping)}
    outcomes = []
    for pair in payload["pairs"]:
        result = by_candidate.get(pair["candidateId"])
        if result is None:
            raise TemporalSearchContractError("Dashboard native canary omitted an accepted pair")
        scenarios = {str(row.get("scenarioId")): row for row in result.get("scenarios", []) if isinstance(row, Mapping)}
        mappings = {
            (str(row.get("moduleDirection")), row.get("fragmentIndex")): row
            for row in result.get("authoritativeProductionMappings", [])
            if isinstance(row, Mapping)
        }
        for claim in pair["productionClaims"]:
            scenario = scenarios.get(str(claim["scenarioId"]))
            mapping = mappings.get((str(claim["moduleDirection"]), claim["fragmentIndex"]))
            lifecycle = next((row for row in (mapping or {}).get("lifecycles", []) if row.get("lifecycle") == claim["lifecycle"]), None)
            if scenario is None or lifecycle is None or lifecycle["transitionId"] not in set(scenario.get("transitionIds") or []):
                raise TemporalSearchContractError(f"claimed production was not compiled and activated: {pair['candidateId']}/{(mapping or {}).get('productionId', 'unknown')}")
            execution = set(scenario.get("execution") or [])
            action = lifecycle.get("actionKind")
            if claim["outcome"] == "action_scheduled" and f"{action}:scheduled" not in execution:
                raise TemporalSearchContractError(f"claimed production action was not scheduled: {mapping['productionId']}")
            if claim["outcome"] == "action_applied" and not any(item in {f"{action}:applied", f"{action}:filled", f"{action}:closed"} for item in execution):
                raise TemporalSearchContractError(f"claimed production action was not activated: {mapping['productionId']}")
            outcomes.append({"candidateId": pair["candidateId"], "moduleDirection": claim["moduleDirection"], "fragmentIndex": claim["fragmentIndex"], "productionId": mapping["productionId"], "scenarioId": claim["scenarioId"], "lifecycle": claim["lifecycle"], "transitionId": lifecycle["transitionId"], "actionKind": action, "outcome": claim["outcome"]})
    return sorted(outcomes, key=lambda row: (row["candidateId"], row["moduleDirection"], row["fragmentIndex"]))


def run_canary(artifacts_path: Path, output_root: Path, *, dashboard_python: Path = DEFAULT_DASHBOARD_PYTHON) -> dict[str, Any]:
    payload = _validate_input(_read(artifacts_path))
    kernel = _run_dashboard(payload, dashboard_python)
    claims = _verify_claims(payload, kernel)
    report = {"schemaVersion": REPORT_SCHEMA, "inputSha256": canonical_sha256(payload), "pairCount": len(payload["pairs"]), "scenarioCount": 4 * len(payload["pairs"]), "productionOutcomeCount": len(claims), "marketEvidenceRead": False, "lakeRead": False, "gatewayContacted": False, "offlineAuditTrustBoundary": {"streamProvenance": "not_recomputable_without_the_input_artifact", "resultPayloadsEmbedded": False, "runTimeBinding": "Dashboard replay verified result stream/profile identities before reporting"}, "dashboardAuthority": {"bidirectionalCompiler": "fuzzfolio_core.temporal_graph.bidirectional_compiler.compile_bidirectional_profile", "nativeValidator": "fuzzfolio_core.temporal_graph.search_validation.validate_temporal_search_candidate", "replayKernel": "fuzzfolio_core.temporal_graph.sequential_replay.run_temporal_replay"}, "compiledPairs": kernel["pairs"], "productionLifecycleOutcomes": claims}
    report["reportSha256"] = canonical_sha256(report)
    _write_immutable(output_root / "activation-canary.json", report)
    manifest = {"schemaVersion": MANIFEST_SCHEMA, "reportSha256": report["reportSha256"], "fileCount": 1, "files": [{"relativePath": "activation-canary.json", "sha256": __import__("hashlib").sha256((output_root / "activation-canary.json").read_bytes()).hexdigest().upper()}]}
    manifest["manifestSha256"] = canonical_sha256(manifest)
    _write_immutable(output_root / "manifest.json", manifest)
    return {"schemaVersion": "temporal_prebroad_activation_canary_result_v1", "reportSha256": report["reportSha256"], "manifestSha256": manifest["manifestSha256"], "pairCount": report["pairCount"], "scenarioCount": report["scenarioCount"], "marketEvidenceRead": False, "lakeRead": False, "gatewayContacted": False}


def audit_canary(output_root: Path) -> dict[str, Any]:
    report = _read(output_root / "activation-canary.json"); supplied = report.pop("reportSha256", None)
    if canonical_sha256(report) != supplied:
        raise TemporalSearchContractError("activation canary report identity mismatch")
    allowed = {"schemaVersion", "inputSha256", "pairCount", "scenarioCount", "productionOutcomeCount", "marketEvidenceRead", "lakeRead", "gatewayContacted", "offlineAuditTrustBoundary", "dashboardAuthority", "compiledPairs", "productionLifecycleOutcomes"}
    if set(report) != allowed or report.get("schemaVersion") != REPORT_SCHEMA or report.get("marketEvidenceRead") is not False or report.get("lakeRead") is not False or report.get("gatewayContacted") is not False:
        raise TemporalSearchContractError("activation canary report has an open or unsafe schema")
    expected_authority = {"bidirectionalCompiler": "fuzzfolio_core.temporal_graph.bidirectional_compiler.compile_bidirectional_profile", "nativeValidator": "fuzzfolio_core.temporal_graph.search_validation.validate_temporal_search_candidate", "replayKernel": "fuzzfolio_core.temporal_graph.sequential_replay.run_temporal_replay"}
    expected_boundary = {"streamProvenance": "not_recomputable_without_the_input_artifact", "resultPayloadsEmbedded": False, "runTimeBinding": "Dashboard replay verified result stream/profile identities before reporting"}
    pairs, outcomes = report.get("compiledPairs"), report.get("productionLifecycleOutcomes")
    if report.get("dashboardAuthority") != expected_authority or report.get("offlineAuditTrustBoundary") != expected_boundary or not isinstance(pairs, list) or not isinstance(outcomes, list) or report.get("pairCount") != len(pairs) or report.get("scenarioCount") != 4 * len(pairs) or report.get("productionOutcomeCount") != len(outcomes):
        raise TemporalSearchContractError("activation canary report counts or authority drifted")
    sha = lambda value: isinstance(value, str) and len(value) == 71 and value.startswith("sha256:") and all(char in "0123456789abcdef" for char in value[7:])
    expected_outcomes = {"long", "short", "neither", "conflict_abstention"}
    pair_ids: set[str] = set()
    authoritative_keys: set[tuple[Any, Any, Any]] = set()
    for pair in pairs:
        if not isinstance(pair, Mapping) or set(pair) != {"candidateId", "compiledProfileSha256", "compiledProgramSha256", "compiledValidationReportSha256", "authoritativeProductionMappings", "scenarios"}:
            raise TemporalSearchContractError("activation canary compiled pair has an open schema")
        candidate = pair.get("candidateId")
        if not isinstance(candidate, str) or not candidate or candidate in pair_ids or not all(sha(pair.get(field)) for field in ("compiledProfileSha256", "compiledProgramSha256", "compiledValidationReportSha256")):
            raise TemporalSearchContractError("activation canary compiled pair identity is invalid")
        pair_ids.add(candidate)
        scenarios = pair.get("scenarios")
        if not isinstance(scenarios, list) or len(scenarios) != 4:
            raise TemporalSearchContractError("activation canary compiled pair must have exactly four scenarios")
        observed = set(); scenario_ids = set()
        for scenario in scenarios:
            required = {"scenarioId", "expectedOutcome", "observedOutcome", "resultSha256", "restartResultSha256", "streamSha256", "profileSnapshotSha256", "transitionIds", "execution"}
            if not isinstance(scenario, Mapping) or set(scenario) != required or not isinstance(scenario.get("scenarioId"), str) or not scenario["scenarioId"] or scenario["scenarioId"] in scenario_ids or scenario.get("expectedOutcome") not in expected_outcomes or scenario.get("observedOutcome") != scenario.get("expectedOutcome") or scenario["observedOutcome"] in observed or not all(sha(scenario.get(field)) for field in ("resultSha256", "restartResultSha256", "streamSha256", "profileSnapshotSha256")) or scenario["resultSha256"] != scenario["restartResultSha256"] or scenario["profileSnapshotSha256"] != pair["compiledProfileSha256"] or not isinstance(scenario.get("transitionIds"), list) or not isinstance(scenario.get("execution"), list):
                raise TemporalSearchContractError("activation canary scenario report is invalid")
            observed.add(scenario["observedOutcome"]); scenario_ids.add(scenario["scenarioId"])
        if observed != expected_outcomes:
            raise TemporalSearchContractError("activation canary scenario outcomes are incomplete")
        mappings = pair.get("authoritativeProductionMappings")
        if not isinstance(mappings, list) or not mappings:
            raise TemporalSearchContractError("activation canary has no authoritative production mapping")
        by_side: dict[str, set[int]] = {"long": set(), "short": set()}
        for item in mappings:
            required = {"moduleDirection", "fragmentIndex", "productionId", "lifecycles"}
            if not isinstance(item, Mapping) or set(item) != required or item.get("moduleDirection") not in by_side or isinstance(item.get("fragmentIndex"), bool) or not isinstance(item.get("fragmentIndex"), int) or item["fragmentIndex"] < 0 or not isinstance(item.get("productionId"), str) or not item["productionId"] or not isinstance(item.get("lifecycles"), list) or not item["lifecycles"]:
                raise TemporalSearchContractError("activation canary production mapping is invalid")
            key = (candidate, item["moduleDirection"], item["fragmentIndex"])
            if key in authoritative_keys:
                raise TemporalSearchContractError("activation canary production mapping is duplicated")
            authoritative_keys.add(key); by_side[item["moduleDirection"]].add(item["fragmentIndex"])
            for lifecycle in item["lifecycles"]:
                if not isinstance(lifecycle, Mapping) or set(lifecycle) != {"lifecycle", "transitionId", "actionKind"} or not isinstance(lifecycle.get("lifecycle"), str) or not lifecycle["lifecycle"] or not isinstance(lifecycle.get("transitionId"), str) or not lifecycle["transitionId"] or (lifecycle.get("actionKind") is not None and not isinstance(lifecycle.get("actionKind"), str)):
                    raise TemporalSearchContractError("activation canary lifecycle mapping is invalid")
        if any(not indexes or indexes != set(range(len(indexes))) for indexes in by_side.values()):
            raise TemporalSearchContractError("activation canary fragment mapping is incomplete")
    scenario_map = {(pair.get("candidateId"), item.get("scenarioId")): item for pair in pairs if isinstance(pair, Mapping) for item in pair.get("scenarios", []) if isinstance(item, Mapping)}
    mapping = {(pair.get("candidateId"), item.get("moduleDirection"), item.get("fragmentIndex")): item for pair in pairs if isinstance(pair, Mapping) for item in pair.get("authoritativeProductionMappings", []) if isinstance(item, Mapping)}
    outcome_keys = {(row.get("candidateId"), row.get("moduleDirection"), row.get("fragmentIndex")) for row in outcomes if isinstance(row, Mapping)}
    if len(outcome_keys) != len(outcomes) or outcome_keys != authoritative_keys:
        raise TemporalSearchContractError("activation canary report has duplicate production outcomes")
    for row in outcomes:
        required = {"candidateId", "moduleDirection", "fragmentIndex", "productionId", "scenarioId", "lifecycle", "transitionId", "actionKind", "outcome"}
        if not isinstance(row, Mapping) or set(row) != required or row.get("outcome") not in {"transition_selected", "action_scheduled", "action_applied"}: raise TemporalSearchContractError("activation canary outcome is not an object")
        source = mapping.get((row.get("candidateId"), row.get("moduleDirection"), row.get("fragmentIndex")))
        scenario = scenario_map.get((row.get("candidateId"), row.get("scenarioId")))
        lifecycle = next((item for item in (source or {}).get("lifecycles", []) if item.get("lifecycle") == row.get("lifecycle")), None)
        if source is None or scenario is None or lifecycle is None or row.get("productionId") != source.get("productionId") or row.get("transitionId") != lifecycle.get("transitionId") or row.get("transitionId") not in set(scenario.get("transitionIds") or []):
            raise TemporalSearchContractError("activation canary report claim does not bind authoritative grammar output")
        if row.get("outcome") == "action_scheduled" and f"{lifecycle.get('actionKind')}:scheduled" not in set(scenario.get("execution") or []): raise TemporalSearchContractError("activation canary scheduled claim does not match replay")
        if row.get("outcome") == "action_applied" and not any(item in {f"{lifecycle.get('actionKind')}:applied", f"{lifecycle.get('actionKind')}:filled", f"{lifecycle.get('actionKind')}:closed"} for item in scenario.get("execution") or []): raise TemporalSearchContractError("activation canary applied claim does not match replay")
    manifest = _read(output_root / "manifest.json"); manifest_sha = manifest.pop("manifestSha256", None)
    if canonical_sha256(manifest) != manifest_sha or manifest.get("reportSha256") != supplied:
        raise TemporalSearchContractError("activation canary manifest identity mismatch")
    files = manifest.get("files")
    if not isinstance(files, list) or len(files) != 1 or files[0].get("relativePath") != "activation-canary.json":
        raise TemporalSearchContractError("activation canary manifest inventory drift")
    import hashlib
    if hashlib.sha256((output_root / "activation-canary.json").read_bytes()).hexdigest().upper() != files[0].get("sha256"):
        raise TemporalSearchContractError("activation canary report file mismatch")
    return {"schemaVersion": "temporal_prebroad_activation_canary_audit_v1", "ok": True, "reportSha256": supplied, "manifestSha256": manifest_sha}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Run the bounded no-market pre-broad native activation canary.")
    commands = parser.add_subparsers(dest="command", required=True)
    run = commands.add_parser("run"); run.add_argument("--artifacts", type=Path, required=True); run.add_argument("--output-root", type=Path, required=True); run.add_argument("--dashboard-python", type=Path, default=DEFAULT_DASHBOARD_PYTHON)
    audit = commands.add_parser("audit"); audit.add_argument("--output-root", type=Path, required=True)
    args = parser.parse_args(argv)
    try:
        result = run_canary(args.artifacts, args.output_root, dashboard_python=args.dashboard_python) if args.command == "run" else audit_canary(args.output_root)
        print(json.dumps(result, indent=2, sort_keys=True)); return 0
    except Exception as exc:
        print(json.dumps({"schemaVersion": "temporal_prebroad_activation_canary_error_v1", "errorType": type(exc).__name__, "message": str(exc)}, indent=2, sort_keys=True), file=sys.stderr); return 1


if __name__ == "__main__":
    raise SystemExit(main())
