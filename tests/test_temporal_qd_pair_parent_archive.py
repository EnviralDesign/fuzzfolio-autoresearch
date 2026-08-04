from __future__ import annotations

import autoresearch.temporal_qd_evolution as qd
import autoresearch.temporal_qd_pair_generation as pair_generation


def test_live_pair_generation_forwards_the_validated_qd_parent_archive(tmp_path, monkeypatch) -> None:
    archive = {"cells": [], "archiveSha256": "sha256:" + "a" * 64}
    policy = {
        "schemaVersion": "temporal_qd_bidirectional_pair_policy_v1",
        "enabled": True,
        "compilerAuthority": {"fixture": True},
        "policySha256": "sha256:" + "b" * 64,
    }
    seen = {}
    monkeypatch.setattr(qd, "_load_archive", lambda _: (archive, archive["archiveSha256"]))
    monkeypatch.setattr(qd, "_bidirectional_pair_policy", lambda _: policy)

    def fake_population(**kwargs):
        seen.update(kwargs)
        return {"completed": True}

    monkeypatch.setattr(pair_generation, "generate_pair_population", fake_population)
    result = qd.generate_qd_generation(
        parent_archive_path=tmp_path / "parent.json",
        output_root=tmp_path / "out",
        generation_index=1,
        parameters={**qd.DEFAULT_QD_PARAMETERS, "targetUniqueCandidates": 1},
        bidirectional_pair_policy=policy,
        bidirectional_pair_factory=object(),
        bidirectional_module_authority=object(),
        bidirectional_native_validator=object(),
        bidirectional_pair_compiler=object(),
        bidirectional_operator_implementation_identity={"fixture": True},
    )
    assert result == {"completed": True}
    assert seen["parent_archive"] is archive
    assert seen["identity_ledger_path"] == tmp_path / "out" / "identity-ledger.json"
    assert seen["max_proposal_attempts"] == qd.DEFAULT_QD_PARAMETERS["maxProposalAttempts"]
