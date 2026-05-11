from __future__ import annotations

import json
from pathlib import Path
from typing import Any


DEFAULT_EXECUTION_COST_MODE = "research-conservative"
DEFAULT_SPREAD_BPS = 2.0
DEFAULT_SLIPPAGE_BPS = 0.5
DEFAULT_COMMISSION_BPS = 0.1

_VALID_MODES = {"none", "fixed-bps", "research-conservative"}


def normalize_execution_cost_mode(value: Any) -> str:
    mode = str(value or DEFAULT_EXECUTION_COST_MODE).strip().lower().replace("_", "-")
    if mode not in _VALID_MODES:
        return DEFAULT_EXECUTION_COST_MODE
    return mode


def execution_cost_payload(config: Any) -> dict[str, Any]:
    research = getattr(config, "research", None)
    mode = normalize_execution_cost_mode(
        getattr(research, "execution_cost_mode", DEFAULT_EXECUTION_COST_MODE)
    )
    if mode == "research-conservative":
        spread_bps = float(
            getattr(research, "execution_cost_spread_bps", DEFAULT_SPREAD_BPS)
        )
        slippage_bps = float(
            getattr(research, "execution_cost_slippage_bps", DEFAULT_SLIPPAGE_BPS)
        )
        commission_bps = float(
            getattr(research, "execution_cost_commission_bps", DEFAULT_COMMISSION_BPS)
        )
    elif mode == "fixed-bps":
        spread_bps = float(
            getattr(research, "execution_cost_spread_bps", 0.0)
        )
        slippage_bps = float(
            getattr(research, "execution_cost_slippage_bps", 0.0)
        )
        commission_bps = float(
            getattr(research, "execution_cost_commission_bps", 0.0)
        )
    else:
        spread_bps = 0.0
        slippage_bps = 0.0
        commission_bps = 0.0
    return {
        "mode": mode.replace("-", "_"),
        "spread_bps": max(0.0, spread_bps),
        "slippage_bps": max(0.0, slippage_bps),
        "commission_bps": max(0.0, commission_bps),
    }


def execution_cost_cli_args(config: Any) -> list[str]:
    payload = execution_cost_payload(config)
    mode = str(payload["mode"]).replace("_", "-")
    return [
        "--execution-cost-mode",
        mode,
        "--spread-bps",
        f"{float(payload['spread_bps']):g}",
        "--slippage-bps",
        f"{float(payload['slippage_bps']):g}",
        "--commission-bps",
        f"{float(payload['commission_bps']):g}",
    ]


def execution_cost_manifest_payload(config: Any) -> dict[str, Any]:
    payload = execution_cost_payload(config)
    return {
        "execution_cost_model": payload,
        "execution_cost_round_trip_bps": (
            float(payload["spread_bps"])
            + float(payload["slippage_bps"]) * 2.0
            + float(payload["commission_bps"]) * 2.0
        ),
    }


def _load_payload(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _nested_get(payload: Any, keys: list[str]) -> Any:
    current = payload
    for key in keys:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _extract_cost_model(payload: dict[str, Any]) -> dict[str, Any] | None:
    candidates = [
        payload.get("cost_model"),
        _nested_get(payload, ["aggregate", "cost_model"]),
        _nested_get(payload, ["data", "cost_model"]),
        _nested_get(payload, ["data", "aggregate", "cost_model"]),
        _nested_get(payload, ["request", "options", "cost_model"]),
    ]
    for candidate in candidates:
        if isinstance(candidate, dict):
            return candidate
    return None


def _nonzero_cost(payload: dict[str, Any]) -> bool:
    return any(
        float(payload.get(key) or 0.0) > 0.0
        for key in ("spread_bps", "slippage_bps", "commission_bps")
    )


def result_matches_execution_cost_model(path: Path, config: Any) -> bool:
    expected = execution_cost_payload(config)
    expected_mode = normalize_execution_cost_mode(expected.get("mode"))
    payload = _load_payload(path)
    observed = _extract_cost_model(payload)
    if expected_mode == "none" and observed is None:
        return True
    if observed is None:
        return False
    observed_mode = normalize_execution_cost_mode(observed.get("mode"))
    if expected_mode == "none":
        return observed_mode == "none" or not _nonzero_cost(observed)
    if observed_mode != expected_mode:
        return False
    for key in ("spread_bps", "slippage_bps", "commission_bps"):
        if abs(float(observed.get(key) or 0.0) - float(expected.get(key) or 0.0)) > 1e-9:
            return False
    return True
