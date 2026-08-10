"""Bounded admission coverage for the single native G0 transaction.

The Python finalizer is callable here only as an explicitly selected oracle.
Production runs below use the native runtime and must not reach any of the
historical Python G0 replay/selection/publication reductions.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

import autoresearch.temporal_qd_evolution as qd_evolution
import autoresearch.temporal_qd_pair_generation as qd_pair_generation
from autoresearch.temporal_discovery_base import (
    TemporalDiscoveryContractError,
    canonical_sha256,
)
from autoresearch.temporal_qd_native import (
    G0_FINALIZATION_RUNTIME_RUST,
    build_g0_finalization_runtime_config,
)
from autoresearch.temporal_qd_pair_generation import generate_pair_population
from scripts.temporal_qd_front_half_oracle import (
    compare_roots,
    compare_roots_bounded_exact,
    public_semantic_paths,
)
from scripts.temporal_qd_front_half_python_oracle_corpus import (
    UniqueFixtureFactory,
    _arguments,
)


def _g0_oracle_arguments(
    construction_width: int, evaluation_width: int | None = None
) -> dict[str, object]:
    evaluation_width = construction_width if evaluation_width is None else evaluation_width
    return _arguments(
        generation_index=1,
        target_unique_candidates=construction_width,
        pair_factory=UniqueFixtureFactory(),
        g0_evaluation_width=evaluation_width,
    )


def _native_g0_arguments(
    construction_width: int, evaluation_width: int | None = None
) -> dict[str, object]:
    arguments = _g0_oracle_arguments(construction_width, evaluation_width)
    arguments["population_finalizer"] = "rust"
    arguments["g0_finalization_runtime"] = build_g0_finalization_runtime_config(
        engine=G0_FINALIZATION_RUNTIME_RUST
    )
    return arguments


@pytest.mark.parametrize(
    ("construction_width", "evaluation_width"),
    ((64, 64), (128, 128), (64, 16)),
)
def test_native_g0_matches_explicit_python_oracle_full_public_byte_tree(
    tmp_path: Path, construction_width: int, evaluation_width: int
) -> None:
    """Admit bounded 64/128 runs by streaming every public artifact byte."""

    label = f"construction-{construction_width}-evaluation-{evaluation_width}"
    python_root = tmp_path / label / "python-oracle"
    native_root = tmp_path / label / "native"
    python_result = generate_pair_population(
        output_root=python_root,
        **_g0_oracle_arguments(construction_width, evaluation_width),
    )
    native_result = generate_pair_population(
        output_root=native_root,
        **_native_g0_arguments(construction_width, evaluation_width),
    )

    assert native_result == python_result
    assert native_result["proposalCount"] == evaluation_width
    assert native_result["candidateCount"] == evaluation_width
    assert native_result["constructionPoolSize"] == construction_width
    assert native_result["constructedAcceptedCount"] == construction_width
    bounded = compare_roots_bounded_exact(
        python_root, native_root, shape=construction_width
    )
    assert bounded["byteExact"] is True, bounded
    assert bounded["semanticExact"] is True, bounded
    # The bounded digest proves byte identity without materializing large
    # artifacts; this small admission witness additionally produces a JSON
    # pointer if a future semantic contract drifts.
    semantic = compare_roots(python_root, native_root, shape=construction_width)
    assert semantic["byteExact"] is True, semantic
    assert semantic["semanticExact"] is True, semantic
    assert (native_root / "internal" / "g0-funnel" / "receipt.json").is_file()


def test_asymmetric_g0_4_to_2_preserves_selected_and_construction_counts(
    tmp_path: Path,
) -> None:
    """Selected proposal count is intentionally distinct from construction attempts."""

    construction_width = 4
    evaluation_width = 2
    python_root = tmp_path / "python-oracle"
    native_root = tmp_path / "native"
    python_result = generate_pair_population(
        output_root=python_root,
        **_g0_oracle_arguments(construction_width, evaluation_width),
    )
    native_result = generate_pair_population(
        output_root=native_root,
        **_native_g0_arguments(construction_width, evaluation_width),
    )

    assert native_result == python_result
    assert native_result["proposalCount"] == evaluation_width
    assert native_result["candidateCount"] == evaluation_width
    assert native_result["constructionPoolSize"] == construction_width
    assert native_result["constructedAcceptedCount"] == construction_width
    journal = json.loads((native_root / "generation-journal.json").read_text())
    assert journal["proposalCount"] == evaluation_width
    assert journal["constructionProposalCount"] == construction_width
    assert journal["proposalSlots"]["proposalAttempts"] == construction_width
    assert {
        path.relative_to(python_root): path.read_bytes()
        for path in public_semantic_paths(python_root)
    } == {
        path.relative_to(native_root): path.read_bytes()
        for path in public_semantic_paths(native_root)
    }


def test_production_g0_rejects_python_finalizer_without_explicit_oracle(
    tmp_path: Path,
) -> None:
    """The production default may never silently choose the Python path."""

    arguments = _g0_oracle_arguments(1)
    arguments.pop("g0_finalization_runtime")
    with pytest.raises(
        TemporalDiscoveryContractError,
        match="production G0 finalization cannot select the Python population finalizer",
    ):
        generate_pair_population(output_root=tmp_path / "forbidden", **arguments)


def test_native_g0_identity_ledger_binding_never_invokes_python_ledger_loader(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Early dispatch sends Rust only a compact path/policy binding."""

    def unexpected_python_ledger_load(*_: object, **__: object) -> object:
        raise AssertionError("native G0 handoff must not reduce the Python ledger")

    monkeypatch.setattr(
        qd_evolution, "_load_identity_ledger", unexpected_python_ledger_load
    )
    binding = qd_pair_generation._g0_identity_ledger_binding(
        identity_ledger_path=tmp_path / "identity-ledger.json",
        policy_name="native-g0-ledger-tripwire",
        policy_sha256="sha256:" + "a" * 64,
        policy_frozen={"identity": {"fixture": "path-only"}},
    )
    assert binding == {
        "schemaVersion": "temporal_qd_native_g0_identity_ledger_binding_v1",
        "ledgerPath": str((tmp_path / "identity-ledger.json").resolve()),
        "policyName": "native-g0-ledger-tripwire",
        "policySha256": "sha256:" + "a" * 64,
        "identityPolicy": {"fixture": "path-only"},
        "identityPolicySha256": canonical_sha256({"fixture": "path-only"}),
    }


def test_sealed_g0_dispatch_does_not_enumerate_proposal_journal(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Receipt/handoff restart selection is O(compact marker metadata)."""

    root = tmp_path / "proposal"
    marker = root / "internal" / "g0-funnel" / "construction-handoff.json"
    marker.parent.mkdir(parents=True)
    marker.write_text("{}\n", encoding="utf-8")
    journal_root = root / "proposal-journal"
    journal_root.mkdir()
    original_glob = Path.glob

    def forbid_source_enumeration(path: Path, pattern: str):
        if path == journal_root:
            raise AssertionError("sealed native G0 dispatch enumerated proposal journal")
        return original_glob(path, pattern)

    monkeypatch.setattr(Path, "glob", forbid_source_enumeration)
    assert qd_pair_generation._g0_native_dispatch_signal(
        root=root, construction_pool_size=4_000
    ) == (True, True)


def test_native_g0_crash_recovery_adoption_and_corruption_are_fail_closed(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """Recover a handoff natively, then adopt without source rows and reject drift."""

    root = tmp_path / "native-g0"
    arguments = _native_g0_arguments(1)

    def simulated_native_crash(**_: object) -> dict[str, object]:
        raise TemporalDiscoveryContractError("simulated native G0 interruption")

    # Construction is durable before the transaction.  A process interruption
    # at that seam must leave a compact marker, not a Python-only recovery
    # obligation.
    with monkeypatch.context() as crashed:
        crashed.setattr(
            qd_pair_generation, "_run_native_g0_finalization", simulated_native_crash
        )
        with pytest.raises(TemporalDiscoveryContractError, match="simulated native G0 interruption"):
            generate_pair_population(output_root=root, **arguments)
    handoff = root / "internal" / "g0-funnel" / "construction-handoff.json"
    assert handoff.is_file()
    assert not (root / "internal" / "g0-funnel" / "receipt.json").exists()

    def unexpected_python_g0_reduction(*_: object, **__: object) -> object:
        raise AssertionError("production G0 must not fall back to a Python reduction")

    # The restart branch is before journal replay.  These are the major old
    # reductions that would execute if the native transaction ever fell back.
    monkeypatch.setattr(
        qd_pair_generation,
        "_materialize_g0_bootstrap",
        unexpected_python_g0_reduction,
    )
    monkeypatch.setattr(
        qd_pair_generation,
        "_rich_immigrant_distribution_from_journal",
        unexpected_python_g0_reduction,
    )
    monkeypatch.setattr(
        qd_pair_generation,
        "_write_evaluation_population",
        unexpected_python_g0_reduction,
    )
    completed = generate_pair_population(output_root=root, **arguments)
    assert completed["completed"] is True
    receipt = root / "internal" / "g0-funnel" / "receipt.json"
    assert receipt.is_file()

    # Receipt adoption requires no proposal-journal bytes.  Source removal is
    # deliberately stronger than a read counter: any resume scan would fail.
    for source in (root / "proposal-journal").glob("*.json"):
        source.unlink()
    assert generate_pair_population(output_root=root, **arguments) == completed

    # A sealed compact artifact remains part of the receipt trust chain; a
    # corrupt selection must terminate, never trigger the Python oracle.
    selection = root / "g0-bootstrap" / "selection.json"
    selection.write_bytes(b'{"tampered":true}\n')
    with pytest.raises(TemporalDiscoveryContractError):
        generate_pair_population(output_root=root, **arguments)
