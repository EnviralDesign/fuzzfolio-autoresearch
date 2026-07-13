from __future__ import annotations

from autoresearch.instrument_universe import (
    DEFERRED_INSTRUMENTS,
    ENABLED_INSTRUMENTS,
    INSTRUMENT_UNIVERSE,
    RETIRED_INSTRUMENTS,
    instrument_asset_class,
    validation_report,
)


def test_darwinex_native_universe_contract_is_packaged_and_classified() -> None:
    assert (len(INSTRUMENT_UNIVERSE), len(ENABLED_INSTRUMENTS)) == (45, 36)
    assert set(DEFERRED_INSTRUMENTS) == {"JP225", "US500"}
    assert set(RETIRED_INSTRUMENTS) == {
        "XBRUSD", "HK50", "RUSS2000", "BTCUSD", "ETHUSD", "SOLUSD", "XRPUSD"
    }
    assert INSTRUMENT_UNIVERSE["DE40"].provider_symbol == "GDAXI"
    assert INSTRUMENT_UNIVERSE["US30"].provider_symbol == "WS30"
    assert INSTRUMENT_UNIVERSE["USTECH"].provider_symbol == "NDX"
    assert instrument_asset_class("XTIUSD") == "commodity"
    assert instrument_asset_class("EURUSD") == "fx"
    assert validation_report()["universe_hash"].startswith("sha256:")

