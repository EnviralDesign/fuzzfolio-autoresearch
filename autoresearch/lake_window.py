from __future__ import annotations

from copy import deepcopy
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


def canonicalize_lake_window_request(
    request: LakeWindowRequest | Mapping[str, Any],
) -> LakeWindowRequest:
    """Return a validated canonical request.

    Keep this mirror of the shared-core contract deliberately small: callers
    use it to prove that an immutable, attested request already contains a
    candidate's replay dependency.  It never manufactures a new lake identity.
    """

    if isinstance(request, LakeWindowRequest):
        return LakeWindowRequest.model_validate(request.model_dump())
    return LakeWindowRequest.model_validate(dict(request))


def lake_window_request_contains(
    frozen_request: LakeWindowRequest | Mapping[str, Any],
    derived_request: LakeWindowRequest | Mapping[str, Any],
) -> bool:
    """Return whether a frozen lake request safely contains a dependency.

    This is intentionally the shared-core containment rule: a pre-attested
    request can safely carry extra timeframes and earlier warmup, but it cannot
    change the dataset, instrument universe, coverage policy, or end boundary.
    """

    frozen = canonicalize_lake_window_request(frozen_request)
    derived = canonicalize_lake_window_request(derived_request)
    if frozen.schema_version != derived.schema_version:
        return False
    if frozen.dataset != derived.dataset:
        return False
    if frozen.pairs != derived.pairs:
        return False
    if frozen.coverage_policy != derived.coverage_policy:
        return False
    if frozen.data_end != derived.data_end:
        return False
    if not set(derived.timeframes).issubset(frozen.timeframes):
        return False
    frozen_start = parse_utc_timestamp(
        frozen.data_start, field_name="frozen_request.data_start"
    )
    derived_start = parse_utc_timestamp(
        derived.data_start, field_name="derived_request.data_start"
    )
    return frozen_start <= derived_start


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


def _deep_merge_catalog_config(
    base: Mapping[str, Any], overlay: Mapping[str, Any]
) -> dict[str, Any]:
    """Match the catalog/default merge used by temporal profile hydration."""

    merged = deepcopy(dict(base))
    for key, value in overlay.items():
        if isinstance(value, Mapping) and isinstance(merged.get(key), Mapping):
            merged[key] = _deep_merge_catalog_config(merged[key], value)
        else:
            merged[key] = deepcopy(value)
    return merged


def _catalog_indicator_items(catalog: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    """Return the only catalog view accepted for hydrated lake scope.

    This is deliberately stricter than construction-plan enumeration.  The
    latter can operate on the metadata it needs for an operator, while an
    evidence request must prove the complete active indicator dependency used
    by the canonical FuzzFolio hydration boundary.
    """

    raw_items = catalog.get("indicators")
    if not isinstance(raw_items, list):
        raise ValueError("frozen construction catalog requires an indicators array")
    items: dict[str, dict[str, Any]] = {}
    for raw in raw_items:
        if not isinstance(raw, Mapping):
            raise ValueError("frozen construction catalog indicator is malformed")
        meta = raw.get("meta")
        config = raw.get("config")
        if not isinstance(meta, Mapping) or not isinstance(config, Mapping):
            raise ValueError(
                "frozen construction catalog indicator requires meta and config"
            )
        indicator_id = str(meta.get("id") or "").strip()
        if not indicator_id or indicator_id in items:
            raise ValueError(
                "frozen construction catalog indicator IDs must be non-empty and unique"
            )
        items[indicator_id] = {
            "meta": deepcopy(dict(meta)),
            "config": deepcopy(dict(config)),
        }
    if not items:
        raise ValueError("frozen construction catalog has no indicators")
    return items


def hydrate_profile_for_lake_scope(
    profile_snapshot: Any,
    *,
    frozen_catalog: Mapping[str, Any] | None,
) -> Any:
    """Catalog-hydrate replay dependencies before deriving a lake request.

    FuzzFolio's canonical worker first calls
    ``hydrate_temporal_profile_from_catalog`` and only then derives the lake
    scope.  AutoResearch cannot treat abbreviated authored metadata as an
    alternate authority: doing so lets an omitted ``requiredPaddingBars``
    shrink a pre-attested window.  This small transport-safe mirror therefore
    uses only the supplied, identity-bound construction catalog for the
    dependency fields consumed by the shared lake resolver.

    It intentionally does *not* mint a resolved profile/program identity or a
    lake semantic hash.  The authoritative worker still performs complete
    profile hydration at execution; this helper only makes the freeze-time
    containment calculation conservatively match that same catalog boundary.
    """

    if isinstance(profile_snapshot, Mapping):
        profile = deepcopy(dict(profile_snapshot))
        raw_indicators = profile.get("indicators")
    else:
        # Existing non-mapping callers are retained only for profile objects
        # that already carry fully resolved indicator objects.  New QD/panel
        # callers are JSON mappings and must supply a frozen catalog below.
        raw_indicators = getattr(profile_snapshot, "indicators", None)
        if raw_indicators is None:
            return profile_snapshot
        profile = {"indicators": list(raw_indicators)}

    if raw_indicators is None:
        return profile
    if not isinstance(raw_indicators, list):
        raise ValueError("source profile indicators must be an array")

    # A no-indicator profile has no catalog-backed warmup dependency.  This
    # preserves valid pre-construction fixtures without providing a backdoor
    # for any active indicator to receive a synthetic zero padding value.
    if not raw_indicators:
        return profile
    if frozen_catalog is None:
        raise ValueError(
            "active indicator lake scope requires an identity-bound frozen construction catalog"
        )
    catalog_items = _catalog_indicator_items(frozen_catalog)
    raw_timeframes = frozen_catalog.get("timeframes")
    if not isinstance(raw_timeframes, Mapping) or not raw_timeframes:
        raise ValueError("frozen construction catalog requires timeframes")
    catalog_timeframes = {
        str(value).strip().upper() for value in raw_timeframes if str(value).strip()
    }
    if not catalog_timeframes:
        raise ValueError("frozen construction catalog timeframes are malformed")
    resolved: list[dict[str, Any]] = []
    for index, raw_indicator in enumerate(raw_indicators):
        if not isinstance(raw_indicator, Mapping):
            raise ValueError(f"source profile indicator {index} is malformed")
        authored_meta = raw_indicator.get("meta")
        authored_config = raw_indicator.get("config")
        if not isinstance(authored_meta, Mapping) or not isinstance(authored_config, Mapping):
            raise ValueError(
                f"source profile indicator {index} requires meta and config for catalog hydration"
            )
        indicator_id = str(authored_meta.get("id") or "").strip()
        if not indicator_id:
            raise ValueError(f"source profile indicator {index} lacks a catalog ID")
        catalog_item = catalog_items.get(indicator_id)
        if catalog_item is None:
            raise ValueError(
                f"source profile indicator {indicator_id!r} is absent from the frozen construction catalog"
            )
        catalog_meta = catalog_item["meta"]
        for key, authored_value in authored_meta.items():
            if key in {"instanceId", "instance_id"}:
                continue
            if key not in catalog_meta:
                raise ValueError(
                    f"source profile indicator {indicator_id!r} has unknown catalog metadata {key!r}"
                )
            if authored_value != catalog_meta[key]:
                raise ValueError(
                    f"source profile indicator {indicator_id!r} catalog metadata mismatch for {key!r}"
                )
        if "requiredPaddingBars" not in catalog_meta:
            raise ValueError(
                f"frozen construction catalog indicator {indicator_id!r} lacks requiredPaddingBars"
            )
        try:
            required_padding = int(catalog_meta["requiredPaddingBars"])
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"frozen construction catalog indicator {indicator_id!r} has invalid requiredPaddingBars"
            ) from exc
        if required_padding < 0:
            raise ValueError(
                f"frozen construction catalog indicator {indicator_id!r} has negative requiredPaddingBars"
            )

        # The canonical hydrator overlays authored config onto catalog defaults
        # and forces the complete resolved snapshot active.  Preserve that
        # dependency behavior here so an abbreviated source cannot omit its
        # timeframe or lookback from evidence containment.
        config = _deep_merge_catalog_config(catalog_item["config"], authored_config)
        config["isActive"] = True
        timeframe = str(config.get("timeframe") or "").strip().upper()
        if not timeframe:
            raise ValueError(
                f"catalog-hydrated indicator {indicator_id!r} lacks a timeframe"
            )
        if timeframe not in catalog_timeframes:
            raise ValueError(
                f"catalog-hydrated indicator {indicator_id!r} uses a timeframe absent from the frozen construction catalog"
            )
        try:
            lookback = int(config.get("lookbackBars"))
        except (TypeError, ValueError) as exc:
            raise ValueError(
                f"catalog-hydrated indicator {indicator_id!r} has invalid lookbackBars"
            ) from exc
        if lookback < 0:
            raise ValueError(
                f"catalog-hydrated indicator {indicator_id!r} has negative lookbackBars"
            )
        meta = deepcopy(catalog_meta)
        meta["requiredPaddingBars"] = required_padding
        instance_id = authored_meta.get("instanceId", authored_meta.get("instance_id"))
        if instance_id is not None:
            meta["instanceId"] = deepcopy(instance_id)
        resolved.append({"meta": meta, "config": config})
    profile["indicators"] = resolved
    return profile


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
    frozen_catalog: Mapping[str, Any] | None = None,
) -> LakeWindowRequest:
    """Mirror the replay worker's exact day-aligned data dependency scope.

    When a frozen catalog is supplied, resolve the profile's evidence
    dependencies through that catalog before inspecting any indicator field.
    This is required for new QD/panel work: authored indicator metadata is an
    abbreviation, not authority for warmup or timeframe defaults.
    """

    base_tf = str(base_timeframe or "").strip().upper()
    if not base_tf:
        raise ValueError("base_timeframe is required")
    scope_profile = (
        hydrate_profile_for_lake_scope(
            profile_snapshot,
            frozen_catalog=frozen_catalog,
        )
        if frozen_catalog is not None
        else profile_snapshot
    )
    indicators = (
        list(scope_profile.get("indicators") or [])
        if isinstance(scope_profile, Mapping)
        else list(getattr(scope_profile, "indicators", None) or [])
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
