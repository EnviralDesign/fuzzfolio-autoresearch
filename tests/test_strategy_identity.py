import json
from pathlib import Path

from autoresearch.strategy_identity import derive_strategy_identity


def _profile(path: Path, *, indicator_id: str, period: int, instance_id: str) -> Path:
    path.write_text(
        json.dumps(
            {
                "format": "fuzzfolio.scoring-profile",
                "formatVersion": 1,
                "profile": {
                    "version": "v1",
                    "directionMode": "both",
                    "instruments": ["EURUSD"],
                    "notificationThreshold": 70,
                    "indicators": [
                        {
                            "meta": {
                                "id": indicator_id,
                                "instanceId": instance_id,
                                "signalRole": "trigger",
                                "signalPersistence": "event",
                            },
                            "config": {
                                "isActive": True,
                                "timeframe": "M5",
                                "normalizationMode": "none",
                                "isTrendFollowing": True,
                                "useFormingBar": False,
                                "talibConfig": [
                                    {"name": "timeperiod", "value": period}
                                ],
                            },
                        }
                    ],
                },
            }
        ),
        encoding="utf-8",
    )
    return path


def test_structural_family_ignores_numeric_tuning_and_instance_labels(tmp_path: Path) -> None:
    first = derive_strategy_identity(
        {
            "attempt_id": "first",
            "profile_path": str(
                _profile(
                    tmp_path / "first.json",
                    indicator_id="RSI_CROSSBACK",
                    period=14,
                    instance_id="instance-a",
                )
            ),
        }
    )
    second = derive_strategy_identity(
        {
            "attempt_id": "second",
            "profile_path": str(
                _profile(
                    tmp_path / "second.json",
                    indicator_id="RSI_CROSSBACK",
                    period=28,
                    instance_id="instance-b",
                )
            ),
        }
    )

    assert first["structural_family_id"] == second["structural_family_id"]
    assert (
        first["structural_family_source"]
        == "indicator_semantic_shape_without_execution"
    )
    assert first["structural_family_signature"]["execution_shape_available"] is False


def test_structural_family_separates_indicator_shapes(tmp_path: Path) -> None:
    first = derive_strategy_identity(
        {
            "attempt_id": "first",
            "profile_path": str(
                _profile(
                    tmp_path / "first.json",
                    indicator_id="RSI_CROSSBACK",
                    period=14,
                    instance_id="instance-a",
                )
            ),
        }
    )
    second = derive_strategy_identity(
        {
            "attempt_id": "second",
            "profile_path": str(
                _profile(
                    tmp_path / "second.json",
                    indicator_id="WILLR_MEAN_REVERSION",
                    period=14,
                    instance_id="instance-a",
                )
            ),
        }
    )

    assert first["structural_family_id"] != second["structural_family_id"]


def test_missing_profiles_use_unique_candidate_fallbacks() -> None:
    first = derive_strategy_identity({"run_id": "run", "attempt_id": "first"})
    second = derive_strategy_identity({"run_id": "run", "attempt_id": "second"})

    assert first["structural_family_source"] == "unique_candidate_fallback"
    assert first["structural_family_id"] != second["structural_family_id"]


def test_identity_keeps_lineage_and_exact_behavior_fingerprint_separate() -> None:
    identity = derive_strategy_identity(
        {
            "run_id": "run-a",
            "strategy_family_id": "lineage-a",
            "attempt_id": "attempt-a",
            "full_backtest_profile_fingerprint_36m": "sha256:exact",
        }
    )

    assert identity["lineage_id"] == "lineage-a"
    assert identity["behavior_fingerprint"] == "sha256:exact"
    assert identity["structural_family_source"] == "behavior_fingerprint_fallback"
