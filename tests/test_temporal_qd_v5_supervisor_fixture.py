"""Hermetic fresh-v5 supervisor authority/restart coverage.

The source fixture intentionally resembles the old Dashboard-shaped pair
authority, but fresh v5 must never open it.  The production boundary derives
only pure sealed projections before it gives the one-shot Rust transaction
ownership of construction.
"""

from __future__ import annotations

import copy
import hashlib
import json
import subprocess
import sys
from pathlib import Path

import pytest

import autoresearch.temporal_qd_evolution as qd
import autoresearch.temporal_qd_supervisor as supervisor
from autoresearch.evolvable_module_qd_authority import (
    build_evolvable_module_authority_config,
)
from autoresearch.temporal_discovery_base import (
    TemporalDiscoveryContractError,
    canonical_sha256,
)
from autoresearch.temporal_qd_pair_factory import (
    PAIR_RUN_CONFIG_SCHEMA,
    default_hold_operator_policy,
    freeze_pair_run_config,
)
from autoresearch.temporal_qd_rotating_evidence_materializer import (
    materialize_qd_rotating_evidence,
)
from scripts.build_temporal_pair_authority import RESOURCE_ROLES, _context
from test_evolvable_module_qd_authority import _catalog_row
from test_temporal_qd_rotating_evidence_materializer import (
    _attestor,
    _catalog as _rotating_catalog,
    _curriculum,
    _population,
)


def _write(path: Path, value: dict) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _git_clean_dashboard_root(root: Path) -> tuple[Path, Path]:
    """Make the minimum content-addressable Dashboard authority surface."""

    validator = root / "scripts" / "temporal_search_validate_candidate.py"
    validator.parent.mkdir(parents=True, exist_ok=True)
    validator.write_text(
        # The supervisor's authority reopen does not issue a validator request.
        # A JSONL loop remains production-shaped if a future fixture does.
        "import sys\n"
        "for _line in sys.stdin:\n"
        "    print('{}', flush=True)\n",
        encoding="utf-8",
    )
    temporal_graph = (
        root / "shared" / "python" / "fuzzfolio_core" / "fuzzfolio_core" / "temporal_graph"
    )
    temporal_graph.mkdir(parents=True)
    (temporal_graph / "__init__.py").write_text("# hermetic authority fixture\n", encoding="utf-8")
    subprocess.run(["git", "init", "-q", str(root)], check=True, capture_output=True)
    subprocess.run(["git", "-C", str(root), "config", "user.email", "fixture@example.invalid"], check=True)
    subprocess.run(["git", "-C", str(root), "config", "user.name", "fixture"], check=True)
    subprocess.run(["git", "-C", str(root), "add", "."], check=True)
    subprocess.run(["git", "-C", str(root), "commit", "-qm", "hermetic authority"], check=True)
    return Path(sys.executable).resolve(), validator


def _pair_catalog() -> dict:
    rows: list[dict] = []
    for role, bindings in RESOURCE_ROLES.items():
        setup_id, _setup_instance = bindings["setup"]
        trigger_id, _trigger_instance = bindings["trigger"]
        rows.append(_catalog_row(setup_id, scalar=(role == "trend")))
        rows.append(_catalog_row(trigger_id, event=True))
    return {
        "timeframes": {"M5": {}, "M15": {}, "H1": {}},
        "indicators": rows,
    }


def _fresh_v5_fixture(tmp_path: Path) -> tuple[dict, dict]:
    """Return a real v5 source authority plus valid rotating preparation."""

    dashboard_root = tmp_path / "dashboard"
    interpreter, validator = _git_clean_dashboard_root(dashboard_root)
    catalog = _pair_catalog()
    context = _context(catalog, timeframe="M5")
    side = {
        "seedNames": ["mean_reversion", "breakout", "trend"],
        "context": context,
        "catalog": catalog,
        "policy": {
            "schemaVersion": "temporal_pair_catalog_seed_policy_v1",
            "resourceRoles": RESOURCE_ROLES,
            "resourceRoleDisposition": "seed_priors_only_v1",
        },
    }
    raw_pair = {
        "schemaVersion": PAIR_RUN_CONFIG_SCHEMA,
        "longModule": side,
        "shortModule": copy.deepcopy(side),
        "nativeJsonlAuthority": {
            "command": [str(interpreter), str(validator.resolve())],
            "timeoutSeconds": 60,
            "persistentJsonl": True,
            "maxLineBytes": 8 * 1024 * 1024,
            "stderrLimitBytes": 64 * 1024,
            "interpreterPath": str(interpreter),
            "validatorScriptPath": str(validator.resolve()),
            "dashboardSourceRoot": str(dashboard_root.resolve()),
            "environment": {
                "PYTHONPATH": [str((dashboard_root / "shared" / "python").resolve())]
            },
        },
        "holdOperatorPolicy": default_hold_operator_policy(),
    }
    pair_config = freeze_pair_run_config(raw_pair)
    evolvable = build_evolvable_module_authority_config(
        pair_run_config_sha256=pair_config["pairRunConfigSha256"],
        catalog_sha256=pair_config["longModule"]["catalogSha256"],
    )

    rotating_root = tmp_path / "rotating"
    curriculum = _write(tmp_path / "curriculum.json", _curriculum())
    population = _write(tmp_path / "population.json", _population())
    construction_catalog = _write(tmp_path / "construction-catalog.json", _rotating_catalog())
    materialize_qd_rotating_evidence(
        rotating_evidence_input_path=curriculum,
        seed_population_path=population,
        construction_catalog_path=construction_catalog,
        output_root=rotating_root,
        worker_contract_sha256="sha256:" + "c" * 64,
        worker_contract_schema="replay-worker-contract-v1",
        base_timeframe="M5",
        attestor=_attestor([]),
    )
    rotating_input = json.loads(
        (rotating_root / "rotating-evidence-config.json").read_text(encoding="utf-8")
    )
    archive_path = _write(
        tmp_path / "fresh-v5-initial-archive.json",
        qd.canonical_empty_directional_bidirectional_archive_template(),
    )
    archive_bytes = archive_path.read_bytes()
    initial_archive_transport_descriptor = {
        "schemaVersion": "temporal_qd_archive_transport_descriptor_v1",
        "absolutePath": str(archive_path.resolve()),
        "documentSchemaVersion": "temporal_qd_archive_v3",
        "archiveSha256": qd.canonical_empty_directional_bidirectional_archive_template()[
            "archiveSha256"
        ],
        "fileSha256": "sha256:" + hashlib.sha256(archive_bytes).hexdigest(),
        "sizeBytes": len(archive_bytes),
    }
    initial_archive_transport_descriptor["descriptorSha256"] = canonical_sha256(
        initial_archive_transport_descriptor
    )
    inputs = {
        "initial_archive_path": archive_path,
        "source_preparation_path": tmp_path / "pair-mode-unused-source.json",
        "base_generator_root": tmp_path / "pair-mode-unused-generator",
        "confirmed_entry_admission_root": tmp_path / "pair-mode-unused-admission",
        "template_preparation_path": rotating_root / "panel-1-template-preparation.json",
        "validator_command_file": None,
        "parameters": {
            "version": qd.QD_VERSION,
            "seed": 731,
            "targetUniqueCandidates": 1,
            "immigrantProposalFraction": 0.2,
            "mutationDepthProbabilities": {"1": 0.70, "2": 0.25, "3": 0.05},
            "maxCumulativeStructuralDepth": 16,
            "maxProposalAttempts": 1,
            "minimumTotalTrades": 8,
            "minimumTradesPerWindow": 4,
            "capTrades": 20,
            "cellCapacity": 4,
        },
        "generation_count": 1,
        "first_generation_index": 1,
        "initial_immigrant_continuation_ordinal": 0,
        "autoresearch_commit": "a" * 40,
        "execution_engine_commit": "b" * 40,
        "worker_contract_sha256": "sha256:" + "c" * 64,
        "gateway_url": "http://127.0.0.1:8799",
        "evaluation_timeout_seconds": 60.0,
        "enqueue_batch_size": 1,
        "broad_admission": False,
        "bidirectional_pair_config": pair_config,
        "pair_generation_engine": supervisor.PAIR_GENERATION_RUNTIME_RUST,
        "rotating_evidence_config": rotating_input,
        "evolvable_module_authority_config": evolvable,
        "initial_archive_transport_descriptor": initial_archive_transport_descriptor,
    }
    return inputs, {"pair": pair_config, "evolvable": evolvable, "validator": validator}


def _rehash_config(config: dict) -> dict:
    result = copy.deepcopy(config)
    result.pop("configSha256", None)
    result["configSha256"] = canonical_sha256(result)
    return result


def _rehash_authority(authority: dict) -> dict:
    result = copy.deepcopy(authority)
    result.pop("authoritySha256", None)
    result["authoritySha256"] = canonical_sha256(result)
    return result


def test_fresh_v5_supervisor_reopens_exact_hermetic_authorities_at_phase_and_restart(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    inputs, fixture = _fresh_v5_fixture(tmp_path)
    monkeypatch.setattr(
        supervisor,
        "PairAuthorityBundle",
        lambda *_args, **_kwargs: pytest.fail(
            "fresh v5 must not open a Python pair authority"
        ),
    )
    config, warnings = supervisor._frozen_config(**inputs)
    assert warnings == []
    assert config["initialArchive"]["archiveSha256"] == qd.canonical_empty_directional_bidirectional_archive_template()["archiveSha256"]
    assert config["bidirectionalPairSourceAuthority"] == fixture["pair"]
    assert config["bidirectionalPairGeneration"]["archivePolicyAuthority"] == fixture["evolvable"]["archivePolicyAuthority"]
    assert config["bidirectionalPairGeneration"]["behaviorAttributionRequirement"] == fixture["evolvable"]["behaviorAttributionRequirement"]
    assert config["bidirectionalPairGeneration"]["operatorImplementation"]["authoritySha256"] == fixture["evolvable"]["authoritySha256"]
    assert config["evaluation"]["behaviorAttributionRequirement"] == fixture["evolvable"]["behaviorAttributionRequirement"]
    assert config["pairGenerationRuntime"]["engine"] == supervisor.PAIR_GENERATION_RUNTIME_RUST
    assert config["nativeV5ProposalRuntime"]["engine"] == supervisor.NATIVE_V5_PROPOSAL_ENGINE

    # A phase boundary and a process restart rebuild the same sealed native
    # control plane, without opening a Python authority object.
    assert supervisor._validate_frozen_sources(config) == []
    restarted = json.loads(json.dumps(config))
    assert supervisor._validate_frozen_sources(restarted) == []

    mutated_operator = _rehash_config(config)
    mutated_operator["bidirectionalPairGeneration"]["operatorImplementation"] = {
        "schemaVersion": "forged_operator_v1"
    }
    with pytest.raises(TemporalDiscoveryContractError, match="generation bindings drifted"):
        supervisor._validate_frozen_sources(_rehash_config(mutated_operator))

    mutated_archive = _rehash_config(config)
    mutated_archive["evolvableModuleAuthority"]["archivePolicyAuthority"]["policyName"] = "forged"
    mutated_archive["evolvableModuleAuthority"] = _rehash_authority(
        mutated_archive["evolvableModuleAuthority"]
    )
    with pytest.raises(TemporalDiscoveryContractError, match="archive policy is invalid"):
        supervisor._validate_frozen_sources(_rehash_config(mutated_archive))

    mutated_behavior = _rehash_config(config)
    mutated_behavior["evolvableModuleAuthority"]["behaviorAttributionRequirement"]["required"] = False
    mutated_behavior["evolvableModuleAuthority"] = _rehash_authority(
        mutated_behavior["evolvableModuleAuthority"]
    )
    with pytest.raises(TemporalDiscoveryContractError, match="proposal runtime authority drifted"):
        supervisor._validate_frozen_sources(_rehash_config(mutated_behavior))

    # The requirement has two consumers: generation materialization and the
    # evaluator handoff.  Recomputing only the supervisor envelope cannot
    # make the latter silently optional.
    mutated_evaluation_requirement = _rehash_config(config)
    mutated_evaluation_requirement["evaluation"]["behaviorAttributionRequirement"]["required"] = False
    with pytest.raises(TemporalDiscoveryContractError, match="behavior attribution requirement drifted"):
        supervisor._validate_frozen_sources(_rehash_config(mutated_evaluation_requirement))

    mutated_runtime = _rehash_config(config)
    mutated_runtime["nativeV5ProposalRuntime"]["threadCap"] = 1
    with pytest.raises(TemporalDiscoveryContractError, match="runtime identity drifted"):
        supervisor._validate_frozen_sources(_rehash_config(mutated_runtime))


@pytest.mark.skipif(
    supervisor.os.name != "nt", reason="Windows extended-path archive transport ABI"
)
def test_fresh_v5_supervisor_preserves_rust_extended_initial_archive_transport(
    tmp_path: Path,
) -> None:
    """A self-hashed ``\\\\?\\`` descriptor survives freeze and restart unchanged."""

    inputs, _fixture = _fresh_v5_fixture(tmp_path)
    transport = dict(inputs["initial_archive_transport_descriptor"])
    normal_path = transport["absolutePath"]
    transport["absolutePath"] = "\\\\?\\" + normal_path
    transport["descriptorSha256"] = canonical_sha256(
        {key: value for key, value in transport.items() if key != "descriptorSha256"}
    )
    inputs["initial_archive_transport_descriptor"] = transport

    config, warnings = supervisor._frozen_config(**inputs)
    assert warnings == []
    assert config["initialArchive"]["transportDescriptor"] == transport
    assert supervisor._validate_frozen_sources(config) == []
    assert supervisor._validate_frozen_sources(json.loads(json.dumps(config))) == []

    foreign_inputs = copy.deepcopy(inputs)
    foreign = dict(transport)
    foreign["absolutePath"] = "\\\\?\\" + str(
        (tmp_path / "foreign-initial-archive.json").resolve()
    )
    foreign["descriptorSha256"] = canonical_sha256(
        {key: value for key, value in foreign.items() if key != "descriptorSha256"}
    )
    foreign_inputs["initial_archive_transport_descriptor"] = foreign
    with pytest.raises(
        TemporalDiscoveryContractError,
        match="initial archive transport descriptor binding drifted",
    ):
        supervisor._frozen_config(**foreign_inputs)
