from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Mapping


@dataclass(frozen=True)
class ResearchInstrument:
    canonical_symbol: str
    provider_symbol: str | None
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


def validation_report() -> dict[str, object]:
    return {
        "universe_id": DEVELOPMENT_UNIVERSE_ID,
        "universe_version": DEVELOPMENT_UNIVERSE_VERSION,
        "universe_hash": DEVELOPMENT_UNIVERSE_HASH,
        "activation_mode": DEVELOPMENT_UNIVERSE["activation_mode"],
        "enabled": list(ENABLED_INSTRUMENTS),
        "deferred": list(DEFERRED_INSTRUMENTS),
        "retired": list(RETIRED_INSTRUMENTS),
    }


if __name__ == "__main__":
    print(json.dumps(validation_report(), indent=2, sort_keys=True))

