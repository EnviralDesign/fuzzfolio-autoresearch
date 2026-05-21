from __future__ import annotations

import json
from pathlib import Path

from autoresearch.config import (
    AppConfig,
    FuzzfolioConfig,
    LlmConfig,
    ManagerConfig,
    ProviderProfileConfig,
    ResearchConfig,
    SuperviseConfig,
)
from autoresearch.indicator_atlas import build_indicator_atlas


def _app_config(repo_root: Path) -> AppConfig:
    return AppConfig(
        repo_root=repo_root,
        config_path=repo_root / "autoresearch.config.json",
        secrets_path=repo_root / ".agentsecrets",
        llm=LlmConfig(),
        providers={"openai-mini": ProviderProfileConfig()},
        fuzzfolio=FuzzfolioConfig(),
        research=ResearchConfig(),
        supervise=SuperviseConfig(),
        manager=ManagerConfig(),
    )


def _indicator(
    indicator_id: str,
    *,
    signal_role: str,
    strategy_role: str,
    talib_meta: list[dict[str, object]] | None = None,
) -> dict[str, object]:
    return {
        "meta": {
            "id": indicator_id,
            "name": indicator_id.replace("_", " ").title(),
            "namespace": "Signals" if signal_role == "trigger" else "Trend",
            "talibFunction": indicator_id,
            "strategyRole": strategy_role,
            "signalRole": signal_role,
            "signalPersistence": "event-with-lookback"
            if signal_role == "trigger"
            else "state",
            "preferredTimeframeRole": "entry"
            if signal_role == "trigger"
            else "higher-context",
            "talibMeta": talib_meta or [],
        },
        "config": {
            "label": indicator_id,
            "isActive": True,
            "weight": 1,
            "timeframe": "M5",
            "lookbackBars": 1,
            "talibConfig": [
                {"name": item["name"], "value": item.get("default")}
                for item in (talib_meta or [])
            ],
        },
    }


def _write_workspace(root: Path) -> Path:
    workspace = root / "Trading-Dashboard"
    constants_dir = workspace / "shared" / "constants"
    indicators_dir = constants_dir / "indicators"
    factory_dir = (
        workspace
        / "shared"
        / "python"
        / "fuzzfolio_core"
        / "fuzzfolio_core"
        / "scoring_engine"
        / "indicators"
    )
    indicators_dir.mkdir(parents=True)
    factory_dir.mkdir(parents=True)

    rsi = _indicator(
        "RSI_CROSSBACK",
        signal_role="trigger",
        strategy_role="mean-reversion",
        talib_meta=[
            {
                "name": "timeperiod",
                "uiType": "integer_slider",
                "default": 14,
                "min": 2,
                "max": 4,
                "step": 1,
                "marks": [],
            },
            {
                "name": "lower_threshold",
                "uiType": "float_slider",
                "default": 30,
                "min": 5,
                "max": 7,
                "step": 1,
                "marks": [],
            },
        ],
    )
    adx = _indicator("ADX", signal_role="filter", strategy_role="filter")
    unknown = _indicator(
        "UNMAPPED_SIGNAL",
        signal_role="trigger",
        strategy_role="confirm",
    )
    payload = {
        "timeframes": {"M5": {"value": "M5", "label": "5 Minute", "minutes": 5}},
        "indicators": [rsi, adx, unknown],
    }
    (constants_dir / "indicators.json").write_text(
        json.dumps(payload), encoding="utf-8"
    )
    for item in [rsi, adx, unknown]:
        indicator_id = item["meta"]["id"]  # type: ignore[index]
        (indicators_dir / f"{indicator_id}.json").write_text(
            json.dumps(item),
            encoding="utf-8",
        )
    (factory_dir / "indicator_factory.py").write_text(
        "\n".join(
            [
                "INDICATOR_CLASSES: dict[str, object] = {",
                '    "RSI_CROSSBACK": RSICrossbackIndicator,',
                '    "ADX": ADXIndicator,',
                "}",
            ]
        ),
        encoding="utf-8",
    )
    return workspace


def test_build_indicator_atlas_writes_static_artifacts(tmp_path: Path) -> None:
    repo_root = tmp_path / "autoresearch"
    repo_root.mkdir()
    workspace = _write_workspace(tmp_path)
    result = build_indicator_atlas(_app_config(repo_root), workspace_root=workspace)

    assert result.atlas_path.exists()
    assert result.csv_path.exists()
    assert result.dependencies_path.exists()
    assert result.pair_matrix_path.exists()
    assert result.recipe_priors_path.exists()

    atlas = json.loads(result.atlas_path.read_text(encoding="utf-8"))
    rows = {row["id"]: row for row in atlas["indicators"]}
    assert atlas["summary"]["indicator_count"] == 3
    assert atlas["summary"]["generation_eligible_count"] == 2
    assert atlas["summary"]["missing_implementation_ids"] == ["UNMAPPED_SIGNAL"]
    assert rows["RSI_CROSSBACK"]["theoretical_parameter_cardinality"] == 9
    assert rows["RSI_CROSSBACK"]["sweepable_parameters"] == [
        "timeperiod",
        "lower_threshold",
    ]
    assert rows["UNMAPPED_SIGNAL"]["static_prior_bucket"] == "broken_or_unmapped"


def test_indicator_atlas_builds_anchor_pair_and_recipe_priors(tmp_path: Path) -> None:
    repo_root = tmp_path / "autoresearch"
    repo_root.mkdir()
    workspace = _write_workspace(tmp_path)
    result = build_indicator_atlas(_app_config(repo_root), workspace_root=workspace)

    pair_matrix = result.pair_matrix_path.read_text(encoding="utf-8")
    assert "ADX" in pair_matrix
    assert "RSI_CROSSBACK" in pair_matrix

    priors = json.loads(result.recipe_priors_path.read_text(encoding="utf-8"))
    mean_reversion = priors["recipes"]["mean_reversion_reclaim"]["slots"]
    trigger_ids = [item["id"] for item in mean_reversion["trigger"]]
    assert trigger_ids[0] == "RSI_CROSSBACK"
