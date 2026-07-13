from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any, Mapping


def _canonical_json(payload: Any) -> str:
    return json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":"))


def _sha256(payload: Any) -> str:
    return hashlib.sha256(_canonical_json(payload).encode("utf-8")).hexdigest()


def _load_profile(path_value: Any) -> dict[str, Any] | None:
    raw = str(path_value or "").strip()
    if not raw:
        return None
    path = Path(raw)
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    if isinstance(payload.get("profile"), dict):
        return dict(payload["profile"])
    profile_document = payload.get("profile_document")
    if isinstance(profile_document, dict) and isinstance(
        profile_document.get("profile"), dict
    ):
        return dict(profile_document["profile"])
    return payload


def _configuration_shape(value: Any, *, key: str = "") -> Any:
    if isinstance(value, dict):
        return {
            str(child_key): _configuration_shape(child_value, key=str(child_key))
            for child_key, child_value in sorted(value.items(), key=lambda pair: str(pair[0]))
            if str(child_key) not in {"instanceId", "label"}
        }
    if isinstance(value, list):
        shaped = [_configuration_shape(item, key=key) for item in value]
        return sorted(shaped, key=_canonical_json)
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)) and not isinstance(value, bool):
        return "<number>"
    if value is None:
        return None
    token = str(value).strip()
    return token.lower() if key not in {"name"} else token


def structural_family_signature(profile: Mapping[str, Any]) -> dict[str, Any] | None:
    indicators = profile.get("indicators")
    if not isinstance(indicators, list) or not indicators:
        return None
    shaped_indicators: list[dict[str, Any]] = []
    for indicator in indicators:
        if not isinstance(indicator, dict):
            continue
        config = indicator.get("config") if isinstance(indicator.get("config"), dict) else {}
        if config.get("isActive") is False:
            continue
        meta = indicator.get("meta") if isinstance(indicator.get("meta"), dict) else {}
        indicator_id = str(meta.get("id") or "").strip()
        if not indicator_id:
            continue
        shaped_indicators.append(
            {
                "indicator_id": indicator_id,
                "signal_role": str(meta.get("signalRole") or "").strip().lower(),
                "signal_persistence": str(meta.get("signalPersistence") or "").strip().lower(),
                "strategy_role": str(meta.get("strategyRole") or "").strip().lower(),
                "preferred_timeframe_role": str(
                    meta.get("preferredTimeframeRole") or ""
                ).strip().lower(),
                "timeframe": str(config.get("timeframe") or "").strip().upper(),
                "is_trend_following": bool(config.get("isTrendFollowing", False)),
                "use_forming_bar": bool(config.get("useFormingBar", False)),
                "normalization_mode": str(
                    config.get("normalizationMode") or ""
                ).strip().lower(),
                "configuration_shape": _configuration_shape(config),
            }
        )
    if not shaped_indicators:
        return None
    shaped_indicators.sort(key=_canonical_json)
    execution_config = profile.get("executionConfig")
    has_execution_shape = isinstance(execution_config, dict) and bool(execution_config)
    return {
        "schema": "autoresearch-structural-family-v2",
        "profile_version": str(profile.get("version") or "").strip().lower(),
        "direction_mode": str(profile.get("directionMode") or "both").strip().lower(),
        "indicators": shaped_indicators,
        "execution_shape_available": has_execution_shape,
        "execution_shape": (
            _configuration_shape(execution_config) if has_execution_shape else None
        ),
    }


def derive_strategy_identity(row: Mapping[str, Any]) -> dict[str, Any]:
    lineage_id = str(row.get("strategy_family_id") or row.get("run_id") or "").strip() or None
    behavior_fingerprint = str(
        row.get("full_backtest_profile_fingerprint_36m")
        or row.get("profile_fingerprint")
        or ""
    ).strip() or None
    persisted_family_id = str(row.get("structural_family_id") or "").strip()
    persisted_source = str(row.get("structural_family_source") or "").strip()
    persisted_signature = row.get("structural_family_signature")
    if persisted_family_id.startswith("sf2:") and persisted_source:
        return {
            "lineage_id": str(row.get("lineage_id") or lineage_id or "").strip()
            or None,
            "behavior_fingerprint": behavior_fingerprint,
            "structural_family_id": persisted_family_id,
            "structural_family_source": persisted_source,
            "structural_family_signature": (
                dict(persisted_signature)
                if isinstance(persisted_signature, dict)
                else None
            ),
        }
    profile = _load_profile(row.get("profile_path"))
    signature = structural_family_signature(profile or {})
    if signature is not None:
        structural_family_id = f"sf2:{_sha256(signature)}"
        source = (
            "profile_semantic_shape_with_execution"
            if signature.get("execution_shape_available")
            else "indicator_semantic_shape_without_execution"
        )
    elif behavior_fingerprint:
        structural_family_id = f"sf2:opaque-exact:{_sha256(behavior_fingerprint)}"
        source = "behavior_fingerprint_fallback"
    else:
        opaque_identity = {
            "run_id": str(row.get("run_id") or ""),
            "attempt_id": str(row.get("attempt_id") or ""),
            "profile_ref": str(row.get("profile_ref") or ""),
        }
        structural_family_id = f"sf2:opaque-candidate:{_sha256(opaque_identity)}"
        source = "unique_candidate_fallback"
    return {
        "lineage_id": lineage_id,
        "behavior_fingerprint": behavior_fingerprint,
        "structural_family_id": structural_family_id,
        "structural_family_source": source,
        "structural_family_signature": signature,
    }
