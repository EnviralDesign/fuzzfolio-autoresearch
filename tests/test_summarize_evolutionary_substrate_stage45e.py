from __future__ import annotations

import sys
import unittest
from pathlib import Path


sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
import summarize_evolutionary_substrate_stage45e as stage45e  # noqa: E402


class Stage45EReducerTests(unittest.TestCase):
    def test_locality_rules_prioritize_semantic_construction(self) -> None:
        base = {"childProgramSha256": "child", "parentSideProgramSha256": "parent", "deltaGeometry": {"changedPathCount": 1, "changeCounts": {"indicator": {}}, "mutationTrace": []}}
        self.assertEqual(stage45e.locality_class({**base, "choiceKind": "hold", "constructionKind": "hold"}), "hold_or_protection_policy_only")
        self.assertEqual(stage45e.locality_class({**base, "choiceKind": "resource", "constructionKind": "indicator_period_mutate"}), "scalar_or_parameter_only")
        self.assertEqual(stage45e.locality_class({**base, "choiceKind": "resource", "constructionKind": "directional_event_insert"}), "event_route_add_or_remove")
        self.assertEqual(stage45e.locality_class({**base, "choiceKind": "typed_grammar", "constructionKind": None, "deltaGeometry": {"changedPathCount": 3, "changeCounts": {"state": {}, "transition": {}}, "mutationTrace": {"operation": "insert_setup"}}}), "single_structural_region_change")

    def test_protection_probability_uses_sealed_hierarchy(self) -> None:
        row = {"choiceKind": "initial_protection", "nativePlanConstruction": {"mutationClass": "adjacent"}}
        peer = {"choiceKind": "initial_protection", "nativePlanConstruction": {"mutationClass": "adjacent"}}
        jump = {"choiceKind": "initial_protection", "nativePlanConstruction": {"mutationClass": "jump"}}
        ticket = stage45e.exact_probability(row, [row, peer, jump])
        self.assertEqual(ticket["formula"], "1/4 * 70/95 * 1/2")
        self.assertAlmostEqual(ticket["probability"], 70 / (4 * 95 * 2))


if __name__ == "__main__":
    unittest.main()
