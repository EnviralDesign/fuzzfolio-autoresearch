from __future__ import annotations

from datetime import datetime, time, timedelta, timezone
from typing import Any, Literal, Mapping, Sequence

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

LAKE_WINDOW_REQUEST_SCHEMA = "fuzzfolio.market-data-window-request.v1"
LAKE_WINDOW_BINDING_SCHEMA = "fuzzfolio.market-data-window-binding.v1"
SEMANTIC_DIGEST_CONTRACT_V2 = "fuzzfolio.canonical-bars.semantic-digest.v2"
CoveragePolicy = Literal["require_complete", "allow_truncated"]
_SHA256_HEX = r"^sha256:[0-9a-f]{64}$"


def _to_iso_z(value: datetime) -> str:
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def parse_utc_timestamp(value: str | datetime, *, field_name: str) -> datetime:
    if isinstance(value, datetime):
        parsed = value
    else:
        token = str(value or "").strip()
        if not token:
            raise ValueError(f"{field_name} is required")
        try:
            parsed = datetime.fromisoformat(
                token[:-1] + "+00:00" if token.endswith("Z") else token
            )
        except ValueError as exc:
            raise ValueError(f"{field_name} is not a valid ISO-8601 timestamp") from exc
    if parsed.tzinfo is None:
        raise ValueError(f"{field_name} must be timezone-aware")
    return parsed.astimezone(timezone.utc)


def _require_utc_midnight(value: datetime, *, field_name: str) -> datetime:
    utc_value = value.astimezone(timezone.utc)
    if utc_value.time() != time(0, 0, 0) or utc_value.microsecond != 0:
        raise ValueError(f"{field_name} must be aligned to a UTC midnight day boundary")
    return utc_value


def _normalize_symbols(values: Sequence[str], *, field_name: str) -> list[str]:
    normalized = sorted({str(value).strip().upper() for value in values if str(value).strip()})
    if not normalized:
        raise ValueError(f"{field_name} must not be empty")
    return normalized


class LakeWindowRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["fuzzfolio.market-data-window-request.v1"] = (
        LAKE_WINDOW_REQUEST_SCHEMA
    )
    dataset: Literal["bars"] = "bars"
    pairs: list[str] = Field(min_length=1)
    timeframes: list[str] = Field(min_length=1)
    data_start: str | datetime
    data_end: str | datetime
    coverage_policy: CoveragePolicy = "require_complete"

    @model_validator(mode="after")
    def _canonicalize(self) -> "LakeWindowRequest":
        self.pairs = _normalize_symbols(self.pairs, field_name="pairs")
        self.timeframes = _normalize_symbols(self.timeframes, field_name="timeframes")
        start = _require_utc_midnight(
            parse_utc_timestamp(self.data_start, field_name="data_start"),
            field_name="data_start",
        )
        end = _require_utc_midnight(
            parse_utc_timestamp(self.data_end, field_name="data_end"),
            field_name="data_end",
        )
        if start >= end:
            raise ValueError("data_start must be strictly less than data_end")
        self.data_start = _to_iso_z(start)
        self.data_end = _to_iso_z(end)
        return self

    def canonical_payload(self) -> dict[str, Any]:
        return self.model_dump(mode="json")


class LakeWindowBinding(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)

    schema_version: Literal["fuzzfolio.market-data-window-binding.v1"] = (
        LAKE_WINDOW_BINDING_SCHEMA
    )
    request: LakeWindowRequest
    window_semantic_sha256: str = Field(pattern=_SHA256_HEX)
    semantic_contract_id: Literal["fuzzfolio.canonical-bars.semantic-digest.v2"] = (
        SEMANTIC_DIGEST_CONTRACT_V2
    )
    attestation_sha256: str | None = Field(default=None, pattern=_SHA256_HEX)
    creation_global_coverage_sha256: str | None = Field(default=None, pattern=_SHA256_HEX)
    creation_source_coverage_sha256: str | None = Field(default=None, pattern=_SHA256_HEX)
    legacy_selection_manifest_sha256: str | None = Field(default=None, pattern=_SHA256_HEX)

    @field_validator(
        "attestation_sha256",
        "creation_global_coverage_sha256",
        "creation_source_coverage_sha256",
        "legacy_selection_manifest_sha256",
        mode="before",
    )
    @classmethod
    def _empty_optional_hash(cls, value: Any) -> Any:
        token = str(value or "").strip()
        return token or None


def _indicator_value(indicator: Any, area: str, key: str, default: Any = None) -> Any:
    if isinstance(indicator, Mapping):
        nested = indicator.get(area)
        return nested.get(key, default) if isinstance(nested, Mapping) else default
    nested = getattr(indicator, area, None)
    if isinstance(nested, Mapping):
        return nested.get(key, default)
    return getattr(nested, key, default) if nested is not None else default


def _timeframe_minutes(value: str) -> int:
    token = str(value).strip().upper()
    if token.startswith("M"):
        return int(token[1:]) if token[1:].isdigit() else 5
    if token.startswith("H"):
        return int(token[1:]) * 60 if token[1:].isdigit() else 300
    if token.startswith("D"):
        return int(token[1:]) * 1440 if token[1:].isdigit() else 1440
    return 5


def resolve_replay_lake_window_request(
    *,
    pairs: Sequence[str],
    base_timeframe: str,
    profile_snapshot: Any,
    analysis_window_start: str | datetime,
    analysis_window_end: str | datetime,
) -> LakeWindowRequest:
    """Mirror the replay worker's exact day-aligned data dependency scope."""

    base_tf = str(base_timeframe or "").strip().upper()
    if not base_tf:
        raise ValueError("base_timeframe is required")
    indicators = (
        list(profile_snapshot.get("indicators") or [])
        if isinstance(profile_snapshot, Mapping)
        else list(getattr(profile_snapshot, "indicators", None) or [])
    )
    timeframes = [base_tf]
    warmup_minutes = 0
    for indicator in indicators:
        if _indicator_value(indicator, "config", "isActive", True) is False:
            continue
        timeframe = str(_indicator_value(indicator, "config", "timeframe", "") or "").strip().upper()
        if not timeframe:
            continue
        timeframes.append(timeframe)
        required_padding = int(_indicator_value(indicator, "meta", "requiredPaddingBars", 0) or 0)
        lookback = int(_indicator_value(indicator, "config", "lookbackBars", 1) or 1)
        warmup_minutes = max(
            warmup_minutes,
            max(1, required_padding + lookback + 10) * _timeframe_minutes(timeframe),
        )
    start = parse_utc_timestamp(analysis_window_start, field_name="analysis_window_start")
    end = _require_utc_midnight(
        parse_utc_timestamp(analysis_window_end, field_name="analysis_window_end"),
        field_name="analysis_window_end",
    )
    data_start_raw = start - timedelta(minutes=warmup_minutes)
    data_start = datetime(
        data_start_raw.year,
        data_start_raw.month,
        data_start_raw.day,
        tzinfo=timezone.utc,
    )
    return LakeWindowRequest(
        pairs=list(pairs),
        timeframes=timeframes,
        data_start=data_start,
        data_end=end,
    )
