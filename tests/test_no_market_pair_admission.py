from __future__ import annotations

import json
from pathlib import Path

import pytest
from autoresearch.temporal_discovery_base import canonical_sha256
from autoresearch.temporal_bidirectional_genome import IdentitySnapshot
from autoresearch.temporal_indicator_learning_v1 import IndicatorLearningRegistry
from autoresearch.temporal_qd_evolution import (
    canonical_empty_bidirectional_archive_template,
    initialize_empty_bidirectional_archive,
)
from scripts import admit_no_market_pair_generation as admission
from scripts import initialize_qd_pair_archive as initializer


def _pair_policy() -> dict:
    return {
        "schemaVersion": "temporal_qd_bidirectional_pair_policy_v1",
        "enabled": True,
        "compilerAuthority": IdentitySnapshot.create(
            kind="pairCompiler", schema_version="pair_compiler_v1", payload={"local": True}
        ).canonical_payload(),
    }


def test_canonical_empty_archive_template_is_exact_empty_and_pair_bindable() -> None:
    template = canonical_empty_bidirectional_archive_template()
    assert template["generationIndex"] == 0
    assert template["cells"] == []
    assert template["memberCount"] == 0
    supplied = template.pop("archiveSha256")
    assert supplied == canonical_sha256(template)
    template["archiveSha256"] = supplied

    initialized = initialize_empty_bidirectional_archive(template, _pair_policy())
    assert initialized["bidirectionalPairPolicy"] == _pair_policy()
    assert initialized["archiveSha256"] == canonical_sha256(
        {key: value for key, value in initialized.items() if key != "archiveSha256"}
    )


def test_initializer_uses_current_canonical_template_when_legacy_template_is_omitted(monkeypatch, tmp_path: Path) -> None:
    captured: dict = {}
    frozen = {"pairRunConfigSha256": "sha256:config"}

    monkeypatch.setattr(initializer, "load_pair_run_config", lambda _value: frozen)
    monkeypatch.setattr(initializer, "pair_policy_from_config", lambda _value: {"enabled": True})
    monkeypatch.setattr(initializer, "canonical_empty_bidirectional_archive_template", lambda: {"template": "current"})

    def initialize(template, policy):
        captured.update(template=template, policy=policy)
        return {"archiveSha256": "sha256:archive"}

    monkeypatch.setattr(initializer, "initialize_empty_bidirectional_archive", initialize)
    monkeypatch.setattr(initializer, "_write_immutable", lambda path, value: captured.update(output=path, archive=value))
    pair_config = tmp_path / "pair-config.json"
    pair_config.write_text("{}", encoding="utf-8")

    assert initializer.main(["--pair-config", str(pair_config), "--output", str(tmp_path / "archive.json")]) == 0
    assert captured["template"] == {"template": "current"}
    assert captured["policy"] == {"enabled": True}


def test_catalog_audit_proves_current_directional_and_fuzzy_surface() -> None:
    catalog_path = Path(r"C:\repos\Trading-Dashboard\shared\constants\indicators.json")
    catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
    registry = IndicatorLearningRegistry(catalog)
    side = {
        "catalog": catalog,
        "catalogSha256": registry.catalog.catalog_sha256,
        "policy": {"resourceRoleDisposition": "seed_priors_only_v1"},
    }
    audit = admission.audit_construction_catalog(catalog, {"longModule": side, "shortModule": side})

    assert audit["directionalEventSubstitutionCount"] == 21
    assert audit["fuzzyStateRangeCapableIndicatorCount"] == 62
    assert audit["roles"] == "seed_priors_only_not_eligibility"
    assert audit["fuzzyCap"] == {"perSide": 3, "perEvidenceGroup": 3}


def test_offspring_audit_requires_exact_four_to_one_schedule(tmp_path: Path) -> None:
    journal_root = tmp_path / "proposal-journal"
    journal_root.mkdir()
    dispositions = ["accepted", "no_op_proposal", "operation_rejected", "duplicate_pair_genome", "accepted"]
    depths = [1, 2, 3, 1, None]
    for ordinal in range(10):
        origin = "random_immigrant" if ordinal % 5 == 4 else "structural_offspring"
        proposal = {"originKind": origin}
        if origin == "structural_offspring":
            proposal["mutationDepth"] = depths[ordinal % 5]
        if ordinal in {6, 7}:
            proposal.update(proposalKind="temporal_qd_same_side_crossover_v1", disposition="no_op_proposal" if ordinal == 6 else "operation_rejected")
        row = {"proposalOrdinal": ordinal, "originKind": origin, "disposition": dispositions[ordinal % 5], "proposal": proposal}
        (journal_root / f"{ordinal:08d}.json").write_text(json.dumps(row), encoding="utf-8")

    audit = admission.audit_offspring_journal(tmp_path)
    assert audit["originProposalCounts"] == {"random_immigrant": 2, "structural_offspring": 8}
    assert audit["mutationDepthAttemptCounts"] == {"1": 4, "2": 2, "3": 2}
    assert audit["observedMutationDepths"] == [1, 2, 3]
    assert audit["structuralAcceptedCount"] == 2
    assert audit["crossoverDispositionCounts"] == {"no_op_proposal": 1, "operation_rejected": 1}


def test_offspring_audit_keeps_depths_observational_but_requires_structural_acceptance(tmp_path: Path) -> None:
    journal_root = tmp_path / "proposal-journal"
    journal_root.mkdir()
    for ordinal in range(5):
        origin = "random_immigrant" if ordinal == 4 else "structural_offspring"
        disposition = "accepted" if ordinal in {0, 4} else "operation_rejected"
        row = {
            "proposalOrdinal": ordinal,
            "originKind": origin,
            "disposition": disposition,
            "proposal": {
                "originKind": origin,
                **(
                    {"mutationDepth": 1 if ordinal < 2 else 2}
                    if origin == "structural_offspring"
                    else {}
                ),
                **(
                    {"proposalKind": "temporal_qd_same_side_crossover_v1"}
                    if ordinal == 3
                    else {}
                ),
            },
        }
        (journal_root / f"{ordinal:08d}.json").write_text(json.dumps(row), encoding="utf-8")

    observational = admission.audit_offspring_journal(tmp_path)
    assert observational["observedMutationDepths"] == [1, 2]
    assert observational["structuralAcceptedCount"] == 1

    for ordinal in range(4):
        path = journal_root / f"{ordinal:08d}.json"
        row = json.loads(path.read_text(encoding="utf-8"))
        row["disposition"] = "operation_rejected"
        path.write_text(json.dumps(row), encoding="utf-8")
    with pytest.raises(Exception, match="did not accept a structural offspring"):
        admission.audit_offspring_journal(tmp_path)


def test_depth_probe_selects_exact_buckets_and_replays_materialized_pairs(monkeypatch) -> None:
    class Pair:
        identity_sha256 = "sha256:" + "a" * 64
        profile = {"version": "v3", "directionMode": "both"}
        validation = {"candidateAcceptable": True}

        def canonical_payload(self):
            return {"pair": self.identity_sha256}

    parent = Pair()
    seen = []

    monkeypatch.setattr(admission.FrozenPair, "from_payload", lambda payload: parent)

    def propose(**kwargs):
        seen.append(kwargs)
        depth = kwargs["mutation_depth"]
        seed = kwargs["proposal_seed"]
        proposal = {
            "mutationDepth": depth,
            "proposalSha256": f"sha256:{depth:064x}",
        }
        return parent, proposal

    monkeypatch.setattr(admission, "_propose_pair_sequence", propose)
    monkeypatch.setattr(admission, "replay_pair_proposal", lambda **kwargs: parent)
    authority = type("Authority", (), {"operator": object(), "validator": object(), "compiler": object()})()

    probe = admission.probe_structural_mutation_depths(authority, parent)

    assert [item["mutationDepth"] for item in probe["probes"]] == [1, 2, 3]
    assert [item["bucket"] for item in probe["probes"]] == [0, 14, 19]
    assert all(
        admission._unbiased_choice(item["proposalSeed"], size=20) == item["bucket"]
        for item in probe["probes"]
    )
    assert len(seen) == 3
