import json
from pathlib import Path

from autoresearch import presentation_metadata as pm


def test_validate_generated_metadata_accepts_constrained_payload() -> None:
    payload = {
        "display_name": "BOLLINGER PULLBACK FILTER",
        "tagline": "Mean reversion entries gated by ATR pressure.",
        "short_description": "Fades stretched Bollinger closes only when ATR confirms meaningful expansion.",
        "long_description": (
            "This profile fades Bollinger overshoots after ATR expands, so it targets forceful pushes that "
            "look exhausted and avoids quieter drift that lacks enough pressure to snap back."
        ),
    }

    validated = pm.validate_generated_metadata(payload)

    assert validated == {
        **payload,
        "display_name": "Bollinger Pullback Filter",
    }


def test_validate_generated_metadata_rejects_operational_or_overlong_copy() -> None:
    assert (
        pm.validate_generated_metadata(
            {
                "display_name": "cand3 v2",
                "tagline": "Seeded retry",
                "short_description": "Scaffold follow-up",
                "long_description": (
                    "This scaffold retries the seed with a slightly adjusted threshold and explains "
                    "the same candidate workflow rather than the actual trading logic in plain language."
                ),
            }
        )
        is None
    )


def test_load_cached_metadata_requires_matching_signature_and_valid_copy(tmp_path: Path) -> None:
    path = tmp_path / "presentation.json"
    payload = {
        "version": 1,
        "presentation_signature": "sig-1",
        "display_name": "Breakout Pressure Fade",
        "tagline": "Fade failed expansion after breakout pressure peaks.",
        "short_description": "Targets exhausted breakouts that reverse after volatility spikes and momentum fades.",
        "long_description": (
            "This profile scores breakout pressure through a channel envelope, then fades moves that extend too "
            "far once volatility peaks and directional thrust stops confirming the push."
        ),
    }
    path.write_text(json.dumps(payload, ensure_ascii=True, indent=2), encoding="utf-8")

    assert pm.load_cached_metadata(path, expected_signature="sig-1") is not None
    assert pm.load_cached_metadata(path, expected_signature="sig-2") is None


def test_apply_metadata_to_profile_document_rewrites_visible_fields() -> None:
    payload = {
        "profile": {
            "name": "cand4",
            "description": "Portable scoring profile scaffolded from live indicator templates.",
        }
    }

    rewritten = pm.apply_metadata_to_profile_document(
        payload,
        {
            "display_name": "Channel Exhaustion Fade",
            "long_description": (
                "This profile fades extended channel breaks after the breakout loses follow-through, "
                "so it focuses on exhaustion reversals instead of trend continuation."
            ),
        },
    )

    assert rewritten is payload
    assert payload["profile"]["name"] == "Channel Exhaustion Fade"
    assert payload["profile"]["description"].startswith("This profile fades extended channel breaks")


def test_compute_presentation_signature_changes_with_writer_profile() -> None:
    payload = {"profile": {"name": "alpha"}}
    package_inputs = {"timeframe": "M5", "instruments": ["EURUSD"]}

    left = pm.compute_presentation_signature(
        payload,
        package_inputs=package_inputs,
        lookback_months=36,
        writer_profile="codex-54-mini",
    )
    right = pm.compute_presentation_signature(
        payload,
        package_inputs=package_inputs,
        lookback_months=36,
        writer_profile="openai-54-mini",
    )

    assert left != right
