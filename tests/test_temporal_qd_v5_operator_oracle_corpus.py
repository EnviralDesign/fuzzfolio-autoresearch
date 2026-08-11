"""Hermetic replay and tamper coverage for the v5 Python operator oracle."""

from __future__ import annotations

import copy
import gzip
import json
from pathlib import Path

import pytest

from scripts import temporal_qd_v5_operator_oracle_corpus as corpus


def _load(path: Path) -> dict:
    return json.loads(gzip.decompress(path.read_bytes()))


@pytest.fixture(scope="module")
def generated_corpus() -> dict:
    """Open the expensive frozen JSONL authority exactly once per test run."""

    return corpus.build_corpus()


def test_materializes_current_frozen_authority_with_complete_operator_matrix(generated_corpus: dict) -> None:
    value = generated_corpus
    assert corpus.verify_corpus(value) == value
    assert len(value["cases"]) == 48
    assert value["coverage"] == {
        "evidenceClass": corpus.SYNTHETIC_PER_FAMILY_EVIDENCE,
        "resourceKinds": list(corpus.RESOURCE_KINDS),
        "temporalFamilies": list(corpus.TEMPORAL_FAMILIES),
        "holdKinds": ["none", "market_bars", "elapsed_calendar"],
        "initialProtectionMutationClasses": ["adjacent", "jump", "kind_switch"],
        "initialProtectionDynamicConstruction": True,
        "topologyOperations": list(corpus.TOPOLOGY_OPERATIONS),
        "crossoverPorts": list(corpus.CROSSOVER_PORTS),
        "dispositions": ["accepted", "no_op", "rejected"],
        "mutationDepths": [1, 2, 3],
        "missingRepresentableFamilies": [],
    }
    assert {row["evidenceClass"] for row in value["cases"]} == {
        corpus.SYNTHETIC_PER_FAMILY_EVIDENCE
    }
    selection = value["selection"]
    assert selection["families"] == [
        "indicator_learning", "typed_grammar", "hold", "initial_protection"
    ]
    assert selection["indicatorLearning"]["composes"] == [
        "evolvable_resource_v1", "evolvable_temporal_v1"
    ]
    assert selection["initialProtection"]["classWeights"] == {
        "adjacent": 70,
        "jump": 25,
        "kind_switch": 5,
    }
    assert selection["initialProtection"]["renormalizedWeightTotal"] == 100
    assert selection["prePlanEnumerationFailure"]["reason"] == "no_eligible_operation"
    assert value["execution"]["schemaVersion"] == "temporal_qd_v5_operator_oracle_execution_v1"
    assert value["execution"]["directFrozenModuleValidationCount"] >= 94
    assert value["execution"]["directFrozenPairCompilationCount"] == 3
    assert value["execution"]["persistentJsonlProcessCount"] == 1
    first = value["cases"][0]["parent"]
    assert set(first["frozenModule"]) == {
        "schemaVersion", "direction", "program", "profile", "nativeReport",
        "lineage", "authoritySnapshotSha256s", "identities",
    }
    assert selection["legacyChoiceOrderingSha256"].startswith("sha256:")
    assert len(selection["orderedChoices"]) > 20
    assert all("selectedChoiceSha256" in row for row in selection["transcripts"])
    assert selection["selectionParentFrozenPairIdentitySha256"].startswith("sha256:")
    assert value["freshCurrentPythonPair"]["proposalSeed"] == "sha256:df5f7e2c27a4787c36b5ab29f33d5af411e4afe0d0984944a9bbcfd75b239789"
    assert value["freshCurrentPythonPair"]["pairIdentitySha256"] == selection["selectionParentFrozenPairIdentitySha256"]
    assert value["historicalDrift"]["driftDetected"] is True
    assert value["historicalDrift"]["historicalLiteral"]["semanticTopologySha256"] == corpus.HISTORICAL_SHORT_TOPOLOGY_SHA256
    assert value["historicalDrift"]["currentRecomputation"]["semanticTopologySha256"] == "sha256:661951a3c52ce1327524e25fa042444f899079e1d0c10a24659b1014227621c2"
    transcripts = value["authorityTranscripts"]
    assert [row["kind"] for row in transcripts[:3]] == [
        "proposal_sequence", "proposal_sequence", "proposal_sequence",
    ]
    assert [row["mutationDepth"] for row in transcripts[:3]] == [1, 2, 3]
    assert {row["evidenceClass"] for row in transcripts} == {
        corpus.REAL_AUTHORITY_TRANSCRIPT_EVIDENCE
    }
    search = value["authorityCrossoverSearch"]
    assert search["capturedPorts"] == list(corpus.CROSSOVER_PORTS)
    assert search["missingPorts"] == []
    assert search["matePairsConstructed"] <= corpus.DISTINCT_CROSSOVER_MATE_LIMIT
    assert search["candidateSeedsExamined"] <= (
        corpus.DISTINCT_CROSSOVER_MATE_LIMIT * corpus.DISTINCT_CROSSOVER_SEED_LIMIT
    )
    distinct = [
        row for row in transcripts
        if row["kind"] == "same_side_crossover_distinct"
    ]
    assert [row["port"] for row in distinct] == list(corpus.CROSSOVER_PORTS)
    for row in distinct:
        assert row["parentPairIdentitySha256"] != row["matePairIdentitySha256"]
        assert row["factoryGeneration"]["parent"]["authority"] == "PairAuthorityBundle"
        assert row["mateOrigin"]["kind"] in {
            "factory_pair", "factory_rooted_proposal_sequence",
        }
        assert row["selection"] == row["projection"]["selection"]
        assert row["port"] == row["projection"]["selectedPort"]
        assert row["plan"] == row["projection"]["plan"]
        assert row["application"] == row["projection"]["application"]
        assert row["proposal"]["disposition"] == "materialized"
        assert row["proposal"]["crossoverAudit"] == row["audit"]
        assert row["child"]["programSha256"] == row["projection"]["childProgramSha256"]
        assert row["child"]["pairIdentitySha256"] == row["childPairIdentitySha256"]
    terminal = [
        row for row in transcripts
        if row["kind"] == "same_side_crossover_terminal"
    ]
    assert len(terminal) == 1
    assert terminal[0]["terminalDisposition"] in {"no_op_proposal", "operation_rejected"}
    assert terminal[0]["proposal"]["disposition"] == terminal[0]["terminalDisposition"]
    assert search["realTerminalNoOpOrRejectionSeed"] == terminal[0]["proposalSeed"]
    assert search["realTerminalNoOpOrRejectionDisposition"] == terminal[0]["terminalDisposition"]


def test_checked_in_compressed_fixture_is_canonical_and_replayable(generated_corpus: dict) -> None:
    checked_in = _load(corpus.DEFAULT_OUTPUT)
    assert checked_in == generated_corpus
    assert corpus.verify_corpus(checked_in) == checked_in


@pytest.mark.parametrize(
    ("path", "message"),
    [
        (("cases", 0, "child", "program", "direction"), "case identity mismatch"),
        (("cases", 0, "plan", "construction", "kind"), "case identity mismatch"),
        (("selection", "families", 0), "selection identity mismatch"),
        (("historicalDrift", "driftDetected"), "historical lineage drift fact mismatch"),
        (("cases", 0, "audit", "allChecksPassed"), "case identity mismatch"),
        (("authorityTranscripts", 3, "projection", "selectedPort"), "authority transcript identity drifted"),
    ],
)
def test_tamper_is_detected(
    generated_corpus: dict, path: tuple[object, ...], message: str
) -> None:
    tampered = copy.deepcopy(generated_corpus)
    cursor = tampered
    for part in path[:-1]:
        cursor = cursor[part]  # type: ignore[index]
    cursor[path[-1]] = "tampered"  # type: ignore[index]
    # The corpus hash catches top-level edits; self-rehash it to prove nested
    # case/selection identities remain independently fail closed.
    tampered["corpusSha256"] = corpus._hash(
        {key: item for key, item in tampered.items() if key != "corpusSha256"}
    )
    with pytest.raises(corpus.CorpusError, match=message):
        corpus.verify_corpus(tampered)


def test_write_refuses_divergent_fixture_and_is_byte_stable(
    generated_corpus: dict, tmp_path: Path
) -> None:
    value = generated_corpus
    target = tmp_path / "operator-corpus.json.gz"
    corpus.write_corpus(target, value)
    first = target.read_bytes()
    corpus.write_corpus(target, value)
    assert target.read_bytes() == first
    target.write_bytes(b"foreign")
    with pytest.raises(corpus.CorpusError, match="refusing to overwrite"):
        corpus.write_corpus(target, value)
    corpus.write_corpus(target, value, replace=True)
    assert target.read_bytes() == first
