from __future__ import annotations

import json
from pathlib import Path

from autoresearch.recipe_priors import (
    build_campaign_policy_manifest,
    validate_campaign_policy_manifest,
)


def test_checked_in_phase3_campaign_policy_matches_the_authoritative_inputs() -> None:
    path = Path(__file__).resolve().parents[1] / "configs" / "phase3-campaign-policy.json"
    stored = json.loads(path.read_text(encoding="utf-8"))
    expected = build_campaign_policy_manifest(
        lane_fractions={"guided": 0.60, "uncertain": 0.25, "wild": 0.15},
        lane_eligible_menus={
            "guided": {
                "recipe_sources": ["curated_recipe_prior", "discovery_recipe_validation"],
                "slot_sampling_lanes": ["high_prior", "medium_prior"],
                "pair_sampling_lanes": ["positive_pair"],
                "allow_generation_eligible_fallback": False,
            },
            "uncertain": {
                "recipe_sources": ["curated_recipe_prior", "discovery_recipe_validation"],
                "slot_sampling_lanes": ["uncertain_prior"],
                "pair_sampling_lanes": ["near_miss_pair"],
                "allow_generation_eligible_fallback": False,
            },
            "wild": {
                "recipe_sources": ["curated_recipe_prior"],
                "slot_sampling_lanes": ["wild_exploration"],
                "pair_sampling_lanes": ["low_pair"],
                "allow_generation_eligible_fallback": True,
            },
        },
        diversity_max_shares={
            "family": 0.05,
            "recipe": 0.30,
            "instrument": 0.10,
            "timeframe": 0.60,
            "indicator": 0.15,
        },
        source_atlas_generation="level-c-v3-phase2-rich-priors",
        source_atlas_run_sequence=4,
    )
    assert stored == expected
    assert validate_campaign_policy_manifest(stored) == expected
