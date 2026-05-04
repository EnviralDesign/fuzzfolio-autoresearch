import random
from pathlib import Path

from autoresearch.play_hand import (
    _best_sweep_parameters,
    _json_payload_from_stdout,
    _normalize_sweep_payload,
    _parameter_importance,
    _play_hand_artifact_commands,
    _top_sweep_score,
    apply_play_hand_profile_defaults,
    build_coarse_axes,
    build_focused_axes,
    build_lookback_axes,
    build_required_lookback_axes,
    deal_indicator_count,
    deal_instruments,
    fit_axes_to_permutation_budget,
    materialize_profile_variant,
)


def test_play_hand_artifact_commands_heal_full_backtests_and_top_drop() -> None:
    commands = _play_hand_artifact_commands(
        run_id="run-123",
        profile_drop_count=1,
        profile_drop_workers=2,
    )

    assert commands[0][-4:] == [
        "calculate-full-backtests",
        "--run-ids",
        "run-123",
        "--json",
    ]
    drop_command = commands[1]
    assert drop_command[:3][-2:] == ["-m", "autoresearch"]
    for expected in (
        "render-corpus-profile-drops",
        "--run-id",
        "run-123",
        "--top-results",
        "1",
        "--lookback-months",
        "36",
        "--profile-drop-workers",
        "2",
        "--json",
    ):
        assert expected in drop_command


def test_play_hand_artifact_commands_can_skip_profile_drops() -> None:
    commands = _play_hand_artifact_commands(
        run_id="run-123",
        profile_drop_count=0,
        profile_drop_workers=1,
    )

    assert len(commands) == 1
    assert "calculate-full-backtests" in commands[0]


def test_json_payload_from_stdout_accepts_trailing_json_payload() -> None:
    payload = _json_payload_from_stdout('noise\\n{"calculated": 2, "failed": 0}\\n')

    assert payload == {"calculated": 2, "failed": 0}


def _profile_payload() -> dict:
    return {
        "format": "fuzzfolio.scoring-profile",
        "profile": {
            "name": "Example",
            "indicators": [
                {
                    "meta": {"id": "RSI_CROSSBACK"},
                    "config": {
                        "timeframe": "M5",
                        "lookbackBars": 1,
                        "talibConfig": [{"name": "timeperiod", "value": 14}],
                    },
                },
                {
                    "meta": {"id": "ADX"},
                    "config": {
                        "timeframe": "H1",
                        "lookbackBars": 1,
                        "talibConfig": [{"name": "timeperiod", "value": 20}],
                    },
                },
            ],
        },
    }


def test_lookback_axes_target_every_active_indicator_with_timeframe_ranges() -> None:
    assert build_lookback_axes(_profile_payload()) == [
        "indicator[0].config.lookbackBars=1,2,3,4,5",
        "indicator[1].config.lookbackBars=1,2,3",
    ]


def test_required_lookback_axes_aliases_universal_timing_axes() -> None:
    assert build_required_lookback_axes(_profile_payload()) == [
        "indicator[0].config.lookbackBars=1,2,3,4,5",
        "indicator[1].config.lookbackBars=1,2,3",
    ]


def test_deal_instruments_honors_pinned_instrument() -> None:
    dealt = deal_instruments(
        instrument=["XAUUSD"],
        instrument_pool=["EURUSD", "GBPUSD"],
        rng=random.Random(123),
    )

    assert dealt["source"] == "pinned"
    assert dealt["primary_instrument"] == "XAUUSD"
    assert dealt["instruments"] == ["XAUUSD"]


def test_deal_instruments_shuffles_from_pool_when_unpinned() -> None:
    dealt = deal_instruments(
        instrument=None,
        instrument_pool=["EURUSD", "GBPUSD", "XAUUSD"],
        rng=random.Random(123),
    )

    assert dealt["source"] == "dealt"
    assert dealt["primary_instrument"] in {"EURUSD", "GBPUSD", "XAUUSD"}
    assert dealt["instruments"] == [dealt["primary_instrument"]]
    assert dealt["instrument_pool"] == ["EURUSD", "GBPUSD", "XAUUSD"]


def test_deal_indicator_count_varies_inside_min_max_range() -> None:
    counts = {
        deal_indicator_count(
            available_count=8,
            min_indicators=1,
            max_indicators=4,
            rng=random.Random(seed),
        )
        for seed in range(20)
    }

    assert counts <= {1, 2, 3, 4}
    assert len(counts) >= 2


def test_deal_indicator_count_supports_exact_hand_size() -> None:
    assert (
        deal_indicator_count(
            available_count=8,
            min_indicators=4,
            max_indicators=4,
            rng=random.Random(1),
        )
        == 4
    )


def test_deal_indicator_count_clamps_to_available_hand() -> None:
    assert (
        deal_indicator_count(
            available_count=3,
            min_indicators=2,
            max_indicators=8,
            rng=random.Random(1),
        )
        <= 3
    )


def test_coarse_axes_uses_numeric_talib_parameters() -> None:
    axes = build_coarse_axes(_profile_payload())

    assert axes[0].startswith("indicator[0].talib.timeperiod=")
    assert axes[1].startswith("indicator[1].talib.timeperiod=")


def test_play_hand_profile_defaults_deals_candlestick_pattern_bundle() -> None:
    payload = {
        "profile": {
            "indicators": [
                {
                    "meta": {"id": "CANDLESTICK_PATTERNS"},
                    "config": {
                        "talibConfig": [
                            {"name": "patterns", "value": []},
                            {"name": "aggregation", "value": "any"},
                        ]
                    },
                }
            ]
        }
    }

    changes = apply_play_hand_profile_defaults(payload, rng=random.Random(1))
    talib = payload["profile"]["indicators"][0]["config"]["talibConfig"]
    patterns = next(item["value"] for item in talib if item["name"] == "patterns")

    assert changes
    assert changes[0]["bundle"]
    assert isinstance(patterns, list)
    assert patterns
    assert all(str(pattern).startswith("CDL") for pattern in patterns)


def test_play_hand_profile_defaults_keeps_existing_candlestick_patterns() -> None:
    payload = {
        "profile": {
            "indicators": [
                {
                    "meta": {"id": "CANDLESTICK_PATTERNS"},
                    "config": {
                        "talibConfig": [
                            {"name": "patterns", "value": ["CDLENGULFING"]},
                        ]
                    },
                }
            ]
        }
    }

    assert apply_play_hand_profile_defaults(payload, rng=random.Random(1)) == []
    talib = payload["profile"]["indicators"][0]["config"]["talibConfig"]
    assert next(item["value"] for item in talib if item["name"] == "patterns") == ["CDLENGULFING"]


def test_fit_axes_to_permutation_budget_preserves_order_and_drops_overflow() -> None:
    selected, dropped, original_count = fit_axes_to_permutation_budget(
        [
            "indicator[1].talib.fastperiod=2,3,4,5",
            "indicator[1].talib.slowperiod=6,8,10,12,16",
            "indicator[3].talib.acceleration=0.01,0.015,0.02,0.025,0.03",
            "indicator[3].talib.maximum=0.1,0.15,0.2,0.25,0.3",
        ],
        max_permutations=256,
    )

    assert original_count == 500
    assert selected == [
        "indicator[1].talib.fastperiod=2,3,4,5",
        "indicator[1].talib.slowperiod=6,8,10,12,16",
        "indicator[3].talib.acceleration=0.01,0.015,0.02,0.025,0.03",
    ]
    assert dropped == ["indicator[3].talib.maximum=0.1,0.15,0.2,0.25,0.3"]


def test_fit_axes_to_permutation_budget_allows_play_hand_sized_grid() -> None:
    selected, dropped, original_count = fit_axes_to_permutation_budget(
        [
            "indicator[1].talib.fastperiod=2,3,4,5",
            "indicator[1].talib.slowperiod=6,8,10,12,16",
            "indicator[3].talib.acceleration=0.01,0.015,0.02,0.025,0.03",
            "indicator[3].talib.maximum=0.1,0.15,0.2,0.25,0.3",
        ],
        max_permutations=625,
    )

    assert original_count == 500
    assert len(selected) == 4
    assert dropped == []


def test_normalize_sweep_payload_maps_backend_axes_to_requested_axes(tmp_path: Path) -> None:
    definition_path = tmp_path / "sweep-definition.json"
    definition_path.write_text(
        __import__("json").dumps(
            {
                "axes": [
                    {"indicator_instance_id": "abc", "param_key": "timeperiod"},
                    {"indicator_instance_id": "def", "param_key": "lookbackBars"},
                ]
            }
        ),
        encoding="utf-8",
    )
    payload = {
        "data": {
            "fitness_metric": "score_lab",
            "parameter_importance": [
                {"axis": "abc.timeperiod", "best_value": 18, "importance_pct": 55.0}
            ],
            "ranked_permutations": [
                {
                    "fitness_value": 42.0,
                    "parameters": {"abc.timeperiod": 18, "def.lookbackBars": 3},
                }
            ],
        }
    }

    _normalize_sweep_payload(
        payload,
        requested_axes=[
            "indicator[0].talib.timeperiod=8,14,18",
            "indicator[1].config.lookbackBars=1,2,3",
        ],
        definition_path=definition_path,
    )

    assert _top_sweep_score(payload) == 42.0
    assert _best_sweep_parameters(payload) == {
        "indicator[0].talib.timeperiod": 18,
        "indicator[1].config.lookbackBars": 3,
    }
    assert _parameter_importance(payload)[0]["axis"] == "indicator[0].talib.timeperiod"
    assert _parameter_importance(payload)[0]["backend_axis"] == "abc.timeperiod"


def test_focused_axes_refines_high_importance_numeric_winners() -> None:
    focused = build_focused_axes(
        [
            {
                "axis": "indicator[0].talib.timeperiod",
                "importance_pct": 42.0,
                "best_value": 14,
            },
            {
                "axis": "indicator[1].talib.timeperiod",
                "importance_pct": 2.0,
                "best_value": 20,
            },
        ],
        [
            "indicator[0].talib.timeperiod=8,11,14,18,22",
            "indicator[1].talib.timeperiod=10,14,20,25,32",
        ],
    )

    assert focused == ["indicator[0].talib.timeperiod=10,12,14,16,18"]


def test_materialize_profile_variant_applies_config_and_talib_params(tmp_path: Path) -> None:
    source = tmp_path / "source.json"
    output = tmp_path / "output.json"
    source.write_text(
        __import__("json").dumps(_profile_payload()),
        encoding="utf-8",
    )

    materialize_profile_variant(
        source,
        output,
        {
            "indicator[0].config.lookbackBars": 3,
            "indicator[1].talib.timeperiod": 28,
        },
        name_suffix="[top]",
    )

    payload = __import__("json").loads(output.read_text(encoding="utf-8"))
    indicators = payload["profile"]["indicators"]
    assert indicators[0]["config"]["lookbackBars"] == 3
    assert indicators[1]["config"]["talibConfig"][0]["value"] == 28
    assert payload["profile"]["name"].endswith("[top]")
