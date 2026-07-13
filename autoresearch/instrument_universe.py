from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Iterable, Mapping


@dataclass(frozen=True)
class ResearchInstrument:
    canonical_symbol: str
    provider_symbol: str | None
    asset_class: str
    lifecycle_status: str
    portfolio_asset_class: str
    research_eligible: bool


def packaged_universe_path() -> Path:
    return Path(__file__).resolve().parents[1] / "data" / "development_universe.json"


def canonical_universe_hash(payload: dict[str, object]) -> str:
    encoded = json.dumps(payload, ensure_ascii=True, sort_keys=True, separators=(",", ":")).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


def load_instrument_universe(
    path: Path | None = None,
) -> tuple[dict[str, object], Mapping[str, ResearchInstrument]]:
    source = path or packaged_universe_path()
    payload = json.loads(source.read_text(encoding="utf-8"))
    if not isinstance(payload, dict) or payload.get("universe_id") != "fuzzfolio-development-darwinex-zero":
        raise ValueError(f"Invalid development universe contract: {source}")
    raw_instruments = payload.get("instruments")
    if not isinstance(raw_instruments, list) or len(raw_instruments) != 45:
        raise ValueError(f"Development universe must contain 45 lifecycle entries: {source}")

    instruments: dict[str, ResearchInstrument] = {}
    lifecycle_counts = {"enabled": 0, "deferred": 0, "retired": 0}
    for raw in raw_instruments:
        if not isinstance(raw, dict):
            raise ValueError("Development universe instrument entries must be objects")
        canonical = str(raw.get("pair") or "").strip().upper()
        lifecycle = str(raw.get("lifecycle_status") or "").strip().lower()
        if not canonical or canonical in instruments or lifecycle not in lifecycle_counts:
            raise ValueError(f"Invalid development universe entry: {canonical}")
        lifecycle_counts[lifecycle] += 1
        providers = raw.get("providers")
        provider = providers.get("darwinex_zero_mt5") if isinstance(providers, dict) else None
        if not isinstance(provider, dict):
            raise ValueError(f"Instrument {canonical} is missing Darwinex metadata")
        provider_symbol = str(provider.get("symbol") or "").strip().upper() or None
        eligible = bool(raw.get("research_eligible"))
        if eligible != (lifecycle == "enabled"):
            raise ValueError(f"Instrument {canonical} research eligibility conflicts with lifecycle")
        instruments[canonical] = ResearchInstrument(
            canonical_symbol=canonical,
            provider_symbol=provider_symbol,
            asset_class=str(raw.get("asset_class") or "").strip().lower(),
            lifecycle_status=lifecycle,
            portfolio_asset_class=str(raw.get("portfolio_asset_class") or "").strip().lower(),
            research_eligible=eligible,
        )

    if lifecycle_counts != {"enabled": 36, "deferred": 2, "retired": 7}:
        raise ValueError(f"Unexpected development universe lifecycle counts: {lifecycle_counts}")
    return payload, MappingProxyType(instruments)


DEVELOPMENT_UNIVERSE, INSTRUMENT_UNIVERSE = load_instrument_universe()
DEVELOPMENT_UNIVERSE_HASH = canonical_universe_hash(DEVELOPMENT_UNIVERSE)
DEVELOPMENT_UNIVERSE_ID = str(DEVELOPMENT_UNIVERSE["universe_id"])
DEVELOPMENT_UNIVERSE_VERSION = int(DEVELOPMENT_UNIVERSE["universe_version"])
ENABLED_INSTRUMENTS = tuple(
    symbol for symbol, instrument in INSTRUMENT_UNIVERSE.items() if instrument.research_eligible
)
DEFERRED_INSTRUMENTS = tuple(
    symbol for symbol, instrument in INSTRUMENT_UNIVERSE.items() if instrument.lifecycle_status == "deferred"
)
RETIRED_INSTRUMENTS = tuple(
    symbol for symbol, instrument in INSTRUMENT_UNIVERSE.items() if instrument.lifecycle_status == "retired"
)


def instrument_asset_class(symbol: str) -> str:
    instrument = INSTRUMENT_UNIVERSE.get(str(symbol or "").strip().upper())
    return instrument.portfolio_asset_class if instrument else "other"


def research_eligible_instruments(
    *,
    asset_classes: Iterable[str] | None = None,
    source_asset_classes: Iterable[str] | None = None,
) -> tuple[str, ...]:
    """Return the authoritative, ordered active research surface."""
    allowed_classes = {
        str(value or "").strip().lower() for value in (asset_classes or [])
    }
    allowed_source_classes = {
        str(value or "").strip().lower()
        for value in (source_asset_classes or [])
    }
    return tuple(
        symbol
        for symbol, instrument in INSTRUMENT_UNIVERSE.items()
        if instrument.research_eligible
        and (not allowed_classes or instrument.portfolio_asset_class in allowed_classes)
        and (not allowed_source_classes or instrument.asset_class in allowed_source_classes)
    )


def normalize_instruments(values: Iterable[Any] | None) -> list[str]:
    normalized: list[str] = []
    seen: set[str] = set()
    for value in values or []:
        token = str(value or "").strip().upper()
        if token and token not in seen:
            normalized.append(token)
            seen.add(token)
    return normalized


def research_eligibility_report(values: Iterable[Any] | None) -> dict[str, Any]:
    instruments = normalize_instruments(values)
    eligible: list[str] = []
    ineligible: list[str] = []
    unknown: list[str] = []
    lifecycle: dict[str, str] = {}
    for symbol in instruments:
        instrument = INSTRUMENT_UNIVERSE.get(symbol)
        if instrument is None:
            unknown.append(symbol)
            continue
        lifecycle[symbol] = instrument.lifecycle_status
        if instrument.research_eligible:
            eligible.append(symbol)
        else:
            ineligible.append(symbol)
    return {
        "instruments": instruments,
        "eligible": eligible,
        "ineligible": ineligible,
        "unknown": unknown,
        "lifecycle": lifecycle,
        "is_eligible": bool(instruments) and not ineligible and not unknown,
        "is_mixed": bool(eligible) and bool(ineligible or unknown),
    }


def require_research_eligible(
    values: Iterable[Any] | None,
    *,
    context: str = "Instrument input",
    allow_empty: bool = False,
) -> list[str]:
    report = research_eligibility_report(values)
    if not report["instruments"] and allow_empty:
        return []
    if report["is_eligible"]:
        return list(report["instruments"])
    details: list[str] = []
    if report["ineligible"]:
        details.append("ineligible=" + ", ".join(report["ineligible"]))
    if report["unknown"]:
        details.append("unknown=" + ", ".join(report["unknown"]))
    if not details:
        details.append("no instruments supplied")
    raise ValueError(
        f"{context} must contain only research-eligible instruments from "
        f"{DEVELOPMENT_UNIVERSE_ID}: " + "; ".join(details)
    )


def universe_provenance() -> dict[str, object]:
    return {
        "universe_id": DEVELOPMENT_UNIVERSE_ID,
        "universe_version": DEVELOPMENT_UNIVERSE_VERSION,
        "universe_hash": DEVELOPMENT_UNIVERSE_HASH,
        "activation_mode": str(DEVELOPMENT_UNIVERSE["activation_mode"]),
    }


def validation_report() -> dict[str, object]:
    return {
        **universe_provenance(),
        "enabled": list(ENABLED_INSTRUMENTS),
        "deferred": list(DEFERRED_INSTRUMENTS),
        "retired": list(RETIRED_INSTRUMENTS),
    }


if __name__ == "__main__":
    print(json.dumps(validation_report(), indent=2, sort_keys=True))
