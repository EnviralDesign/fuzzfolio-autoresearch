"""Exact continuation of the admitted temporal generator-v2 proposal stream.

The quality-diversity engine uses this as an independent immigrant source.  It
does not reseed the generator or mutate archive members.  Construction replays
and audits both the admitted generator prefix and every proposal consumed by
the confirmed-entry admission batch, leaving the RNG at the first genuinely
unused proposal.
"""

from __future__ import annotations

import random
from collections.abc import Mapping
from pathlib import Path
from typing import Any

from .temporal_confirmed_entry_admission import (
    CONTINUATION_VERSION,
    _audit_base_generator,
    _base_journal_row,
    _proposal,
    _read,
)
from .temporal_discovery_base import (
    TemporalDiscoveryContractError,
    _clone,
    _sha,
    canonical_sha256,
)
from .temporal_discovery_validation import _normalize_preparation
from .temporal_search_policy_v2 import GENERATOR_V2_VERSION

IMMIGRANT_SOURCE_VERSION = "temporal_generator_v2_qd_immigrant_stream_v1"


def _verify_identity(
    payload: Mapping[str, Any], field: str, *, name: str
) -> tuple[dict[str, Any], str]:
    value = _clone(payload, name=name)
    supplied = _sha(value.pop(field, None), name=f"{name} {field}")
    if canonical_sha256(value) != supplied:
        raise TemporalDiscoveryContractError(f"{name} identity mismatch")
    value[field] = supplied
    return value, supplied


class ExactGeneratorV2Continuation:
    """Sequential, restartable access to unused generator-v2 proposals."""

    def __init__(
        self,
        *,
        source_preparation_path: Path | str,
        base_generator_root: Path | str,
        confirmed_entry_admission_root: Path | str,
        start_continuation_ordinal: int = 0,
    ) -> None:
        if start_continuation_ordinal < 0:
            raise TemporalDiscoveryContractError(
                "immigrant continuation ordinal must be nonnegative"
            )
        source_path = Path(source_preparation_path)
        base_root = Path(base_generator_root)
        admission_root = Path(confirmed_entry_admission_root)

        base_audit = _audit_base_generator(base_root)
        preparation = _normalize_preparation(
            _read(source_path, name="generator-v2 source preparation")
        )
        base_config, base_config_sha = _verify_identity(
            _read(base_root / "config.json", name="generator-v2 config"),
            "configSha256",
            name="generator-v2 config",
        )
        _base_population, base_population_sha = _verify_identity(
            _read(base_root / "population.json", name="generator-v2 population"),
            "populationSha256",
            name="generator-v2 population",
        )
        base_journal, base_journal_sha = _verify_identity(
            _read(base_root / "generation-journal.json", name="generator-v2 journal"),
            "journalSha256",
            name="generator-v2 journal",
        )
        admission_config, admission_config_sha = _verify_identity(
            _read(admission_root / "config.json", name="confirmed-entry config"),
            "configSha256",
            name="confirmed-entry config",
        )
        admission_journal, admission_journal_sha = _verify_identity(
            _read(
                admission_root / "admission-journal.json",
                name="confirmed-entry journal",
            ),
            "journalSha256",
            name="confirmed-entry journal",
        )

        if base_config.get("generatorVersion") != GENERATOR_V2_VERSION:
            raise TemporalDiscoveryContractError("generator-v2 version mismatch")
        expected_bindings = {
            "baseConfigSha256": base_config_sha,
            "basePopulationSha256": base_population_sha,
            "baseJournalSha256": base_journal_sha,
            "baseManifestSha256": base_audit["manifestSha256"],
            "sourcePreparationSha256": preparation["preparationSha256"],
        }
        for key, expected in expected_bindings.items():
            if admission_config.get(key) != expected:
                raise TemporalDiscoveryContractError(
                    f"confirmed-entry continuation {key} mismatch"
                )
        if admission_config.get("continuationVersion") != CONTINUATION_VERSION:
            raise TemporalDiscoveryContractError(
                "confirmed-entry continuation version mismatch"
            )
        if admission_journal.get("configSha256") != admission_config_sha:
            raise TemporalDiscoveryContractError(
                "confirmed-entry journal/config binding mismatch"
            )

        parameters = _clone(
            base_config["parameters"], name="generator-v2 continuation parameters"
        )
        targets = dict(parameters["sourceModeCounts"])
        seeds = sorted(preparation["seeds"], key=lambda item: str(item["seedId"]))
        rng = random.Random(int(parameters["seed"]))
        mode_counts = {key: 0 for key in targets}

        base_entries = base_journal.get("entries") or []
        if len(base_entries) != int(
            admission_journal.get("basePrefixProposalCount", -1)
        ):
            raise TemporalDiscoveryContractError(
                "confirmed-entry base-prefix proposal count mismatch"
            )
        for ordinal, expected in enumerate(base_entries):
            proposal = _proposal(
                rng=rng,
                ordinal=ordinal,
                mode_counts=mode_counts,
                targets=targets,
                seeds=seeds,
                parameters=parameters,
                continuation=False,
            )
            material = _base_journal_row(proposal)
            if any(expected.get(key) != value for key, value in material.items()):
                raise TemporalDiscoveryContractError(
                    f"generator-v2 prefix diverged at proposal {ordinal}"
                )
            if expected.get("disposition") == "accepted":
                mode_counts[str(proposal["sourceMode"])] += 1
        if mode_counts != targets:
            raise TemporalDiscoveryContractError(
                "generator-v2 prefix did not complete its source allocation"
            )

        consumed = sorted(
            (
                item
                for item in admission_journal.get("entries") or []
                if "continuationOrdinal" in item
            ),
            key=lambda item: int(item["continuationOrdinal"]),
        )
        if len(consumed) != int(admission_journal.get("continuationProposalCount", -1)):
            raise TemporalDiscoveryContractError(
                "confirmed-entry continuation proposal count mismatch"
            )
        continuation_start = len(base_entries)
        if (
            int(admission_config.get("continuationStartProposalOrdinal", -1))
            != continuation_start
        ):
            raise TemporalDiscoveryContractError(
                "confirmed-entry continuation start mismatch"
            )
        for offset, expected in enumerate(consumed):
            if int(expected.get("continuationOrdinal", -1)) != offset:
                raise TemporalDiscoveryContractError(
                    "confirmed-entry continuation journal has a gap"
                )
            proposal = _proposal(
                rng=rng,
                ordinal=continuation_start + offset,
                mode_counts=mode_counts,
                targets=targets,
                seeds=seeds,
                parameters=parameters,
                continuation=True,
            )
            material = _base_journal_row(proposal)
            if any(expected.get(key) != value for key, value in material.items()):
                raise TemporalDiscoveryContractError(
                    f"confirmed-entry continuation diverged at offset {offset}"
                )

        source_identity = {
            "schemaVersion": "temporal_generator_v2_qd_immigrant_source_identity_v1",
            "sourceVersion": IMMIGRANT_SOURCE_VERSION,
            "generatorVersion": GENERATOR_V2_VERSION,
            "continuationVersion": CONTINUATION_VERSION,
            "sourcePreparationSha256": preparation["preparationSha256"],
            "baseConfigSha256": base_config_sha,
            "basePopulationSha256": base_population_sha,
            "baseJournalSha256": base_journal_sha,
            "baseManifestSha256": base_audit["manifestSha256"],
            "confirmedEntryConfigSha256": admission_config_sha,
            "confirmedEntryJournalSha256": admission_journal_sha,
            "consumedPrefixProposalCount": len(base_entries) + len(consumed),
        }
        source_identity["sourceIdentitySha256"] = canonical_sha256(source_identity)

        self.source_identity = source_identity
        self._rng = rng
        self._parameters = parameters
        self._targets = targets
        self._mode_counts = mode_counts
        self._seeds = seeds
        self._absolute_start = len(base_entries) + len(consumed)
        self._next_continuation_ordinal = 0
        for _ in range(start_continuation_ordinal):
            self._next_unchecked()

    @property
    def next_continuation_ordinal(self) -> int:
        return self._next_continuation_ordinal

    def _next_unchecked(self) -> dict[str, Any]:
        continuation_ordinal = self._next_continuation_ordinal
        proposal = _proposal(
            rng=self._rng,
            ordinal=self._absolute_start + continuation_ordinal,
            mode_counts=self._mode_counts,
            targets=self._targets,
            seeds=self._seeds,
            parameters=self._parameters,
            continuation=True,
        )
        self._next_continuation_ordinal += 1
        return proposal

    def next_proposal(self) -> dict[str, Any]:
        proposal = self._next_unchecked()
        continuation_ordinal = self._next_continuation_ordinal - 1
        result = {
            "schemaVersion": "temporal_generator_v2_qd_immigrant_proposal_v1",
            "sourceIdentitySha256": self.source_identity["sourceIdentitySha256"],
            "continuationOrdinal": continuation_ordinal,
            "generatorProposalOrdinal": proposal["proposalOrdinal"],
            "sourceMode": proposal["sourceMode"],
            "seedId": proposal["seedId"],
            "rawSourceProfile": proposal["profile"],
            "rawSourceProfileSha256": proposal["rawSourceProfileSha256"],
            "mutations": proposal["mutations"],
            "activationAwareRepairs": proposal["activationAwareRepairs"],
            "managementReachability": proposal["reachability"],
        }
        result["immigrantProposalSha256"] = canonical_sha256(result)
        return result


__all__ = ["IMMIGRANT_SOURCE_VERSION", "ExactGeneratorV2Continuation"]
