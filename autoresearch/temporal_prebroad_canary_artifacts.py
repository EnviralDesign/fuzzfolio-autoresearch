"""Build bounded, deterministic no-market inputs for the native canary.

This is deliberately an *artifact* builder.  It replays synthetic completed
bars only through the Dashboard kernel; it never supplies position facts,
execution outcomes, lake data, or a Gateway connection.  The small search
alphabet is closed (zero/high evidence and absent/present bound events) and
every stream is capped at :data:`MAX_OBSERVATIONS`.
"""
from __future__ import annotations

import argparse
from collections.abc import Iterable, Mapping, Sequence
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
from typing import Any

from .temporal_bidirectional_genome import FrozenModule, FrozenPair
from .temporal_discovery_validation import SubprocessCandidateValidator
from .temporal_qd_pair_factory import load_pair_run_config
from .temporal_search import TemporalSearchContractError, canonical_sha256
from .temporal_typed_motif_grammar import REGISTRY, Fragment, GrammarContext, ModuleProgram, TypedFragmentGrammar, compiled_graph_signature


ARTIFACT_SCHEMA = "temporal_prebroad_activation_canary_input_v1"
BUILD_REPORT_SCHEMA = "temporal_prebroad_canary_artifact_build_v1"
MAX_OBSERVATIONS = 256
MAX_CORPUS = 1024
MAX_PAIRS_PER_ARTIFACT = 32


class CanaryArtifactError(TemporalSearchContractError):
    """The bounded synthetic trace alphabet cannot prove a required path."""


class GuardSatisfier:
    """Translate the closed grammar guard vocabulary to observation facts.

    Position/state/execution facts intentionally produce no authored fact here:
    they are established by prior real kernel steps.  ``False`` requests are
    used for the neither scenario and remain explicit, rather than relying on
    an unbounded random search.
    """

    def satisfy(self, guard: Mapping[str, Any], *, truth: bool = True) -> dict[str, Any]:
        kind = str(guard.get("kind") or "")
        if kind in {"all", "any"}:
            children = [self.satisfy(item, truth=truth) for item in guard.get("guards") or [] if isinstance(item, Mapping)]
            evidence: dict[str, float] = {}
            events: set[str] = set()
            for child in children:
                evidence.update(child["evidence"]); events.update(child["freshEvents"])
            return {"evidence": evidence, "freshEvents": sorted(events)}
        if kind == "evidence_at_least":
            threshold = float(guard["thresholdPercent"])
            return {"evidence": {str(guard["groupId"]): 100.0 if truth else max(0.0, threshold - 1.0)}, "freshEvents": []}
        if kind == "evidence_below":
            threshold = float(guard["thresholdPercent"])
            return {"evidence": {str(guard["groupId"]): 0.0 if truth else min(100.0, threshold + 1.0)}, "freshEvents": []}
        if kind == "fresh_event":
            return {"evidence": {}, "freshEvents": [str(guard["eventId"])] if truth else []}
        if kind == "event_age_at_most":
            # Age zero is represented by a genuine current decision event.
            return {"evidence": {}, "freshEvents": [str(guard["eventId"])] if truth and int(guard.get("events", 0)) == 0 else []}
        if kind in {"condition_streak_at_least", "predicate_edge", "state_age_at_least", "position_age_at_least", "unrealized_r_at_least", "unrealized_r_at_most", "position_exists", "execution_status_is", "always"}:
            return {"evidence": {}, "freshEvents": []}
        raise CanaryArtifactError(f"unsupported closed guard kind: {kind}")


def greedy_set_cover(candidates: Mapping[str, Iterable[str]], required: Iterable[str], *, maximum: int = MAX_PAIRS_PER_ARTIFACT) -> list[str]:
    """Stable smallest-first greedy cover used by registry planning."""
    remaining = set(required); selected: list[str] = []
    normalized = {str(key): set(values) for key, values in candidates.items()}
    while remaining and len(selected) < maximum:
        ranked = sorted(((len(values & remaining), name) for name, values in normalized.items() if name not in selected), key=lambda item: (-item[0], item[1]))
        if not ranked or ranked[0][0] == 0:
            break
        selected.append(ranked[0][1]); remaining -= normalized[ranked[0][1]]
    if remaining:
        raise CanaryArtifactError("registry production cover is incomplete: " + ", ".join(sorted(remaining)))
    return selected


def _read(path: Path) -> dict[str, Any]:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise CanaryArtifactError(f"could not read JSON: {path}") from exc
    if not isinstance(value, dict):
        raise CanaryArtifactError("artifact source root must be an object")
    return value


def _authority(config: Mapping[str, Any]) -> SubprocessCandidateValidator:
    raw = config.get("nativeJsonlAuthority")
    if not isinstance(raw, Mapping) or raw.get("persistentJsonl") is not True or not isinstance(raw.get("command"), list):
        raise CanaryArtifactError("frozen pair config lacks persistent Dashboard authority")
    environment = dict(os.environ)
    for name, values in (raw.get("environment") or {}).items():
        if isinstance(values, list): environment[str(name)] = os.pathsep.join(str(value) for value in values)
    return SubprocessCandidateValidator(raw["command"], timeout_seconds=float(raw.get("timeoutSeconds", 60)), persistent_jsonl=True, persistent_max_line_bytes=int(raw.get("maxLineBytes", 8 * 1024 * 1024)), persistent_stderr_limit_bytes=int(raw.get("stderrLimitBytes", 64 * 1024)), persistent_environment=environment)


def _context(module: FrozenModule) -> dict[str, Any]:
    raw = _plain(module.grammar_context.payload)
    # Stored grammar context is normalized; canary input owns the public field
    # spelling only.  No lineage/snapshot identity is authored here.
    return {"instrument": raw["instrument"], "indicators": raw["indicators"], "evidenceGroups": raw["groups"], "eventBindings": raw["events"], "executionConfig": raw["executionConfig"], "budgets": raw.get("budgets")}


def _plain(value: Any) -> Any:
    """Thaw frozen genome mapping proxies without changing canonical content."""
    if isinstance(value, Mapping): return {str(key): _plain(child) for key, child in value.items()}
    if isinstance(value, tuple): return [_plain(child) for child in value]
    if isinstance(value, list): return [_plain(child) for child in value]
    return value


def _module_artifact(module: FrozenModule, report: Mapping[str, Any]) -> dict[str, Any]:
    context = _context(module); program = _plain(module.program); profile = _plain(module.profile)
    context_sha = canonical_sha256(_plain(module.grammar_context.payload))
    # The raw source profile hash is emitted by the same Dashboard authority
    # which verifies it inside ``temporal_prebroad_canary``.  Do not recalculate
    # that identity with this repository's compatibility codec.
    profile_sha = str(report["rawSourceProfileSha256"])
    return {"context": context, "contextSha256": context_sha, "program": program, "programSha256": canonical_sha256(program), "nativeArtifact": {"schemaVersion": "temporal_prebroad_frozen_native_module_artifact_v1", "profile": profile, "profileSha256": profile_sha, "validation": dict(report), "identities": {"profileSha256": profile_sha, "contextSha256": context_sha, "programSha256": canonical_sha256(program), "rawModuleSha256": canonical_sha256(profile), "nativeProgramSha256": report["programSha256"], "nativeValidationReportSha256": report["validationReportSha256"], "compiledGraphStructureSha256": compiled_graph_signature(profile)}}}


def _revalidate(pair: FrozenPair, candidate_id: str, authority: SubprocessCandidateValidator) -> dict[str, Any]:
    long_profile, short_profile = _plain(pair.long.profile), _plain(pair.short.profile)
    long_report = authority.validate(candidate_id=candidate_id + "_long", source_profile=long_profile, expected_raw_source_profile_sha256=canonical_sha256(long_profile))
    short_report = authority.validate(candidate_id=candidate_id + "_short", source_profile=short_profile, expected_raw_source_profile_sha256=canonical_sha256(short_profile))
    result = authority.compile_pair(candidate_id=candidate_id, long_profile=long_profile, short_profile=short_profile, expected_long_raw_source_profile_sha256=canonical_sha256(long_profile), expected_short_raw_source_profile_sha256=canonical_sha256(short_profile))
    return {"candidateId": candidate_id, "longModule": _module_artifact(pair.long, long_report), "shortModule": _module_artifact(pair.short, short_report), "pair": {"profile": result["profile"], "validation": result["report"]}}


_NATIVE_SOLVER = r'''
import json, sys
from datetime import datetime, timedelta, UTC
from fuzzfolio_core.temporal_graph.graph_models import TemporalGraphProfile
from fuzzfolio_core.temporal_graph.observation_models import build_completed_bar_observation, build_observation_stream
from fuzzfolio_core.temporal_graph.replay_metrics import run_temporal_replay

MAX=256
def dump(x): return x.model_dump(mode='json',by_alias=True,exclude_none=False)
def main(pair):
 p=TemporalGraphProfile.model_validate(pair['pair']['profile']); validation=pair['pair']['validation']; program=validation['programSha256']; source=validation['rawSourceProfileSha256']; resolved=validation['profileSnapshotSha256']
 transitions=p.graph.model_dump(mode='json',by_alias=True)['transitions']
 profile_text=json.dumps(transitions,sort_keys=True)
 needs_loss='unrealized_r_at_most' in profile_text; needs_profit='unrealized_r_at_least' in profile_text
 def group(side):
  return next((x['id'] for x in p.graph.model_dump(mode='json',by_alias=True)['evidenceGroups'] if x['id'].startswith(side+'_g_')),None)
 def event(side):
  return next((x['id'] for x in p.graph.model_dump(mode='json',by_alias=True)['eventBindings'] if x['id'].startswith(side+'_e_')),None)
 groups={s:group(s) for s in ('long','short')}; events={s:event(s) for s in ('long','short')}
 def stream(name, steps):
  out=[]; start=datetime(2020,1,1,tzinfo=UTC)
  for i,(scores,fresh) in enumerate(steps):
   # +/-0.75R exercises real management guards without touching the 1R stop
   # or 2R target of the sealed base plan.  Only its own scenario marks a side.
   # A single late pulse exercises R-based management/exit guards.  Returning
   # to flat prices matters: a newly applied 1R target must not pre-empt the
   # following exit_on_age witness in the registry set-target canary.
   mark=(99.5 if name=='long' else 100.5) if needs_loss and name in ('long','short') and i==10 else (101.0 if name=='long' else 99.0) if needs_profit and name in ('long','short') and i==10 else 99.25 if name=='short' and i==10 else 100.75 if name=='long' and i==10 else 100.0
   high=max(100.75 if name!='short' else 100.25,mark); low=min(99.25 if name!='long' else 99.75,mark)
   o=build_completed_bar_observation(program_sha256=program,instrument='EURUSD',timeframe='M5',bar_id=name+'-'+str(i),bar_start=start+timedelta(minutes=5*i),bar_close=start+timedelta(minutes=5*(i+1)),sequence=i,clock_index=i,open_price=100.0,high_price=high,low_price=low,close_price=mark,evidence_scores=scores,fresh_events=fresh)
   out.append(o)
  if not 1 < len(out) <= MAX: raise ValueError('bounded solver stream length')
  return dump(build_observation_stream(source_profile_sha256=source,resolved_profile_sha256=resolved,program_sha256=program,instrument='EURUSD',base_timeframe='M5',observations=out,diagnostics={'source':'deterministic_closed_guard_solver_v1'}))
 z={key:0.0 for key in groups.values() if key}; hi=lambda s:{**z,**({groups[s]:100.0} if groups[s] else {})}; lo=lambda s:{**z,**({groups[s]:0.0} if groups[s] else {})}; fe=lambda s:[] if events[s] is None else [events[s]]; edge='predicate_edge' in profile_text; streak='condition_streak' in profile_text; gate=lambda s:lo(s) if edge or 'evidence_below' in profile_text else hi(s)
 # These paths use only current facts.  Pending orders, positions, age, and
 # fill/close execution events are all produced by the replay engine.
 scenarios={
  # One high+event arm, one low reset, then a bounded high+event runway
  # satisfies every named root and the finite gate vocabulary.  In
  # particular, a fresh-event arm followed by gate_delay still receives a
  # later high bar for enter_on_level; the former fixed four-bar prelude did
  # not provide that combination.
  'long': stream('long',[(z,[]),(hi('long'),fe('long')),(lo('long'),fe('long')),*[(hi('long'),fe('long')) for _ in range(10)],(z,fe('long')),*[(z,[]) for _ in range(8)]]),
  'short': stream('short',[(z,[]),(hi('short'),fe('short')),(lo('short'),fe('short')),*[(hi('short'),fe('short')) for _ in range(10)],(z,fe('short')),*[(z,[]) for _ in range(8)]]),
  'neither': stream('neither',[(z,[]),(z,[]),(z,[]),(z,[])]),
  'conflict_abstention': stream('conflict',[(z,[]),({key:100.0 for key in groups.values() if key},fe('long')+fe('short')),(z,[]),(z,[])])}
 results={k:dump(run_temporal_replay(p, __import__('fuzzfolio_core.temporal_graph.observation_models',fromlist=['TemporalObservationStream']).TemporalObservationStream.model_validate(v), cost_model={'mode':'research_conservative'})) for k,v in scenarios.items()}
 conflict=p.graph.entry_arbitration.conflict_transition_id
 def outcome(r):
  if any(x['transitionId']==conflict for x in r['graphTraces']): return 'conflict_abstention'
  dirs={x['direction'] for x in r['trades']}; pos=(r.get('finalExecutionState') or {}).get('position') or {}
  if pos.get('direction'): dirs.add(pos['direction'])
  return 'neither' if not dirs else next(iter(dirs)) if len(dirs)==1 else 'invalid'
 observed={k:outcome(v) for k,v in results.items()}
 if observed != {k:k for k in scenarios}: raise ValueError('bounded guard solver outcomes '+repr(observed))
 mappings=[]; claims=[]
 for side,key in (('long','longModule'),('short','shortModule')):
  for ix,frag in enumerate(pair[key]['program']['fragments']):
   prefix=side+'_f%d_%s_'%(ix,frag['productionId']); rows=[x for x in transitions if x['id'].startswith(prefix)]
   if not rows: raise ValueError('unmapped fragment '+prefix)
   primary=next((x for x in rows if x['actions']), next((x for x in rows if x['id'].endswith('_arm') or x['id'].endswith('_gate')), rows[0]))
   life=primary['reasonCode'].split('.')[-1]; action=primary['actions'][0]['kind'] if primary['actions'] else None
   mappings.append({'moduleDirection':side,'fragmentIndex':ix,'productionId':frag['productionId'],'lifecycles':[{'lifecycle':life,'transitionId':primary['id'],'actionKind':action}]})
   witness=next((name for name,r in results.items() if primary['id'] in {x['transitionId'] for x in r['graphTraces']}),None)
   if witness is None: raise ValueError('unsatisfiable fragment '+primary['id'])
   execution={x['actionKind']+':'+x['status'] for x in results[witness]['executionTraces']}
   if action:
    if action+':scheduled' in execution: claim='action_scheduled'
    elif any(x in execution for x in (action+':applied',action+':filled',action+':closed')): claim='action_applied'
    else: raise ValueError('action not scheduled/applied '+primary['id']+' '+repr(execution))
   else: claim='transition_selected'
   claims.append({'moduleDirection':side,'fragmentIndex':ix,'scenarioId':witness,'lifecycle':life,'outcome':claim})
 packed=[]
 for name in ('long','short','neither','conflict_abstention'):
  packed.append({'scenarioId':name,'expectedOutcome':name,'restartAfterObservations':max(1,len(scenarios[name]['observations'])//2),'observationStream':scenarios[name]})
 return {'scenarios':packed,'productionClaims':claims,'proof':{'mappings':mappings,'outcomes':observed}}
data=json.load(open(sys.argv[1])); solved=[]
for item in data['pairs']:
 try: solved.append(main(item))
 except Exception as exc: raise ValueError(str(item.get('candidateId'))+': '+str(exc))
print(json.dumps({'pairs':solved},sort_keys=True,separators=(',',':')))
'''


def _solve_streams(pairs: Sequence[Mapping[str, Any]], dashboard_python: Path) -> list[dict[str, Any]]:
    if not dashboard_python.is_file(): raise CanaryArtifactError(f"Dashboard Python unavailable: {dashboard_python}")
    with tempfile.TemporaryDirectory(prefix="temporal-canary-solve-") as root:
        input_path = Path(root) / "pairs.json"; input_path.write_text(json.dumps({"pairs": list(pairs)}, sort_keys=True), encoding="utf-8")
        environment = dict(os.environ); environment["PYTHONPATH"] = str(Path(__file__).resolve().parents[1]) + os.pathsep + environment.get("PYTHONPATH", "")
        done = subprocess.run([str(dashboard_python), "-c", _NATIVE_SOLVER, str(input_path)], text=True, capture_output=True, timeout=120, env=environment, check=False)
    if done.returncode: raise CanaryArtifactError("Dashboard bounded stream solver failed: " + done.stderr.strip()[:1000])
    try: value = json.loads(done.stdout)
    except json.JSONDecodeError as exc: raise CanaryArtifactError("Dashboard bounded stream solver returned invalid JSON") from exc
    if not isinstance(value, Mapping) or not isinstance(value.get("pairs"), list) or len(value["pairs"]) != len(pairs): raise CanaryArtifactError("Dashboard bounded stream solver response drift")
    return list(value["pairs"])


def build_artifacts(population_path: Path, config_path: Path, output_path: Path, *, candidate_ids: Sequence[str] | None = None, dashboard_python: Path = Path("C:/repos/Trading-Dashboard/compute-service/.venv/Scripts/python.exe")) -> dict[str, Any]:
    population, config = _read(population_path), load_pair_run_config(_read(config_path))
    raw_candidates = population.get("candidates")
    if not isinstance(raw_candidates, list): raise CanaryArtifactError("population has no frozen candidates")
    requested = set(candidate_ids or [str(item.get("candidateId")) for item in raw_candidates])
    frozen: list[tuple[str, FrozenPair]] = []
    for item in raw_candidates:
        if not isinstance(item, Mapping) or str(item.get("candidateId")) not in requested: continue
        candidate = str(item["candidateId"])
        try: pair = FrozenPair.from_payload(item["bidirectionalGenome"])
        except Exception as exc: raise CanaryArtifactError(f"invalid frozen pair {candidate}: {exc}") from exc
        frozen.append((candidate, pair))
    if not frozen or len(frozen) > MAX_PAIRS_PER_ARTIFACT: raise CanaryArtifactError("selected frozen pair count must be 1..32")
    with _authority(config) as authority:
        built = [_revalidate(pair, candidate, authority) for candidate, pair in sorted(frozen)]
    solved = _solve_streams(built, dashboard_python)
    for artifact, proof in zip(built, solved, strict=True): artifact.update({"scenarios": proof["scenarios"], "productionClaims": proof["productionClaims"]})
    payload = {"schemaVersion": ARTIFACT_SCHEMA, "pairs": built}
    output_path.parent.mkdir(parents=True, exist_ok=True); output_path.write_text(json.dumps(payload, indent=2, sort_keys=True, ensure_ascii=True) + "\n", encoding="utf-8")
    return {"schemaVersion": BUILD_REPORT_SCHEMA, "inputSha256": canonical_sha256(payload), "artifactPath": str(output_path), "pairCount": len(built), "scenarioCount": 4 * len(built), "maxObservations": MAX_OBSERVATIONS, "marketEvidenceRead": False, "lakeRead": False, "gatewayContacted": False}


def registry_plan(population_path: Path) -> dict[str, Any]:
    """Plan a finite selected-pair cover from frozen programs (no compilation)."""
    population = _read(population_path); candidates = population.get("candidates") or []
    required = {side + ":" + production for side in ("long", "short") for production in REGISTRY}
    cover: dict[str, set[str]] = {}
    for row in candidates:
        if not isinstance(row, Mapping): continue
        genome = row.get("bidirectionalGenome") or {}; keys=set()
        for side, module_key in (("long", "long"), ("short", "short")):
            for fragment in ((genome.get(module_key) or {}).get("program") or {}).get("fragments") or []:
                if isinstance(fragment, Mapping): keys.add(side + ":" + str(fragment.get("productionId")))
        cover[str(row.get("candidateId"))] = keys
    return {"schemaVersion": "temporal_prebroad_registry_cover_plan_v1", "registryProductionCount": len(required), "availableProductionCount": len(set().union(*cover.values()) if cover else set()), "selectedCandidateIds": greedy_set_cover(cover, required), "structuralCorpusLimit": MAX_CORPUS, "runtimePairLimit": MAX_PAIRS_PER_ARTIFACT}


class _ModuleAuthority:
    def __init__(self, client: SubprocessCandidateValidator) -> None: self.client = client
    def validate_v2(self, *, profile: Mapping[str, Any], candidate_id: str) -> Mapping[str, Any]:
        return self.client.validate(candidate_id=candidate_id, source_profile=profile, expected_raw_source_profile_sha256=canonical_sha256(profile))


def _public_context(raw: Mapping[str, Any]) -> dict[str, Any]:
    """Return the public grammar-context spelling from authored or frozen input."""
    groups_key = "evidenceGroups" if "evidenceGroups" in raw else "groups"
    events_key = "eventBindings" if "eventBindings" in raw else "events"
    return {
        "instrument": raw["instrument"],
        "indicators": raw["indicators"],
        "evidenceGroups": raw[groups_key],
        "eventBindings": raw[events_key],
        "executionConfig": raw["executionConfig"],
        **({"budgets": raw["budgets"]} if "budgets" in raw else {}),
    }


def _grammar_context(raw: Mapping[str, Any]) -> GrammarContext:
    context = _public_context(raw)
    return GrammarContext(instrument=context["instrument"], indicators=tuple(context["indicators"]), evidence_groups=tuple(context["evidenceGroups"]), event_bindings=tuple(context["eventBindings"]), execution_config=context["executionConfig"])


def _registry_program(grammar: TypedFragmentGrammar, direction: str, target: str) -> ModuleProgram:
    """One finite valid module embedding ``target`` and its ordinary lifecycle."""
    spec = REGISTRY[target]; groups, events, plan = grammar.context["groups"][0]["id"], grammar.context["events"][0]["id"], grammar.context["plans"][0]
    base = ["arm_level", "enter_on_level", "exit_on_age"]
    if spec.family == "arm": base[0] = target
    elif spec.family == "entry": base[1] = target
    elif spec.family == "exit": base[2] = target
    elif spec.family == "gate": base.insert(1, target)
    elif spec.family == "management": base.insert(2, target)
    elif spec.family == "recovery": base.append(target)
    else: raise CanaryArtifactError("unknown registry production family: " + target)
    fragments=[]
    for index, production in enumerate(base):
        item=REGISTRY[production]; resources={slot: {"group": groups, "event": events, "plan": plan}[slot] for slot in item.resource_slots}; choices={name: values[0] for name, values in item.choice_domains.items()}
        # A 1R protective stop necessarily pre-empts the -1.5/-1.0 loss
        # thresholds.  Select the sealed -0.5 witness value for this bounded
        # activation corpus; it remains the same production family.
        if production == "exit_on_loss": choices["r"] = -0.5
        fragments.append(Fragment(uid="registry_%s_%d" % (target, index), production_id=production, resources=resources, choices=choices))
    program=ModuleProgram(direction, tuple(fragments)); grammar.validate(program); return program


def registry_corpus(config_path: Path) -> list[dict[str, Any]]:
    """Deterministically enumerate the 22 sealed lifecycle witnesses per side.

    The construction corpus is intentionally structural only; native admission
    is deferred to the greedy selected set, rather than compiling all 1024
    possible bounded combinations during a runtime canary.
    """
    config=load_pair_run_config(_read(config_path)); output=[]
    for target in sorted(REGISTRY):
        row={"productionId": target}
        for side, key in (("long", "longModule"), ("short", "shortModule")):
            grammar=TypedFragmentGrammar(_grammar_context(config[key]["context"]), native_authority=object())
            row[side]=grammar.canonical_program(_registry_program(grammar, side, target))
        output.append(row)
    if len(output) > MAX_CORPUS: raise CanaryArtifactError("registry corpus exceeded finite structural bound")
    return output


def build_registry_artifacts(config_path: Path, output_path: Path, *, dashboard_python: Path = Path("C:/repos/Trading-Dashboard/compute-service/.venv/Scripts/python.exe")) -> dict[str, Any]:
    """Native-admit the <=32 greedy registry cover, then solve/replay it.

    Each selected pair covers one sealed production on both sides; its ordinary
    arm/entry/exit fragments make the module grammar-valid without claiming
    any uncompiled identity.  The replay solver remains the only source of
    execution/position facts.
    """
    config=load_pair_run_config(_read(config_path)); corpus=registry_corpus(config_path)
    cover={"registry_"+row["productionId"]: {"long:"+row["productionId"], "short:"+row["productionId"]} for row in corpus}
    selected=greedy_set_cover(cover, {side+":"+item for side in ("long", "short") for item in REGISTRY})
    by_id={"registry_"+row["productionId"]: row for row in corpus}; built=[]
    with _authority(config) as client:
        authority=_ModuleAuthority(client)
        for candidate in selected:
            row=by_id[candidate]; modules={}
            profiles={}
            for side,key in (("long", "longModule"),("short", "shortModule")):
                raw_context=config[key]["context"]; grammar=TypedFragmentGrammar(_grammar_context(raw_context), native_authority=authority)
                canonical=row[side]; program=ModuleProgram(side, tuple(Fragment(uid="registry_%d" % index, production_id=item["productionId"], resources=item["resources"], choices=item["choices"]) for index,item in enumerate(canonical["fragments"])))
                compiled=grammar.compile_module(program,candidate_id=candidate+"_"+side); report=compiled.native_report; profile=dict(compiled.profile); profiles[side]=profile
                context=_public_context(raw_context)
                profile_sha=str(report["rawSourceProfileSha256"]); modules[side+"Module"]={"context":context,"contextSha256":grammar.context_sha256,"program":dict(compiled.program),"programSha256":compiled.identities["programSha256"],"nativeArtifact":{"schemaVersion":"temporal_prebroad_frozen_native_module_artifact_v1","profile":profile,"profileSha256":profile_sha,"validation":dict(report),"identities":{"profileSha256":profile_sha,**dict(compiled.identities)}}}
            result=client.compile_pair(candidate_id=candidate,long_profile=profiles["long"],short_profile=profiles["short"],expected_long_raw_source_profile_sha256=canonical_sha256(profiles["long"]),expected_short_raw_source_profile_sha256=canonical_sha256(profiles["short"]))
            built.append({"candidateId":candidate,**modules,"pair":{"profile":result["profile"],"validation":result["report"]}})
    solved=_solve_streams(built,dashboard_python)
    for artifact,proof in zip(built,solved,strict=True): artifact.update({"scenarios":proof["scenarios"],"productionClaims":proof["productionClaims"]})
    payload={"schemaVersion":ARTIFACT_SCHEMA,"pairs":built}; output_path.parent.mkdir(parents=True,exist_ok=True); output_path.write_text(json.dumps(payload,indent=2,sort_keys=True,ensure_ascii=True)+"\n",encoding="utf-8")
    return {"schemaVersion":BUILD_REPORT_SCHEMA,"mode":"registry_cover","inputSha256":canonical_sha256(payload),"artifactPath":str(output_path),"pairCount":len(built),"structuralCorpusCount":len(corpus),"marketEvidenceRead":False,"lakeRead":False,"gatewayContacted":False}


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build deterministic no-market activation canary artifacts.")
    commands = parser.add_subparsers(dest="command", required=True)
    build = commands.add_parser("build"); build.add_argument("--population", type=Path, required=True); build.add_argument("--config", type=Path, required=True); build.add_argument("--output", type=Path, required=True); build.add_argument("--candidate-id", action="append", default=[]); build.add_argument("--dashboard-python", type=Path, default=Path("C:/repos/Trading-Dashboard/compute-service/.venv/Scripts/python.exe"))
    plan = commands.add_parser("registry-plan"); plan.add_argument("--population", type=Path, required=True)
    registry = commands.add_parser("registry-build"); registry.add_argument("--config",type=Path,required=True); registry.add_argument("--output",type=Path,required=True); registry.add_argument("--dashboard-python",type=Path,default=Path("C:/repos/Trading-Dashboard/compute-service/.venv/Scripts/python.exe"))
    args = parser.parse_args(argv)
    try:
        value = registry_plan(args.population) if args.command == "registry-plan" else build_registry_artifacts(args.config,args.output,dashboard_python=args.dashboard_python) if args.command == "registry-build" else build_artifacts(args.population, args.config, args.output, candidate_ids=args.candidate_id or None, dashboard_python=args.dashboard_python)
        print(json.dumps(value, indent=2, sort_keys=True)); return 0
    except Exception as exc:
        print(json.dumps({"schemaVersion": "temporal_prebroad_canary_artifact_error_v1", "errorType": type(exc).__name__, "message": str(exc)}, indent=2, sort_keys=True), file=sys.stderr); return 1

if __name__ == "__main__": raise SystemExit(main())
