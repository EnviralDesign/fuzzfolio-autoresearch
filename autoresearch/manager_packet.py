"""Compact manager packets for adjudication (not full explorer run state)."""

from __future__ import annotations

import json
from typing import Any

from . import validation_outcome as vo
from .manager_models import ManagerHookEvent, ManagerPacket

MANAGER_SYSTEM_PROMPT = """You are the branch manager for an autonomous Fuzzfolio research run.

You do NOT execute research tools. You do NOT return explorer actions (no evaluate_candidate, sweeps, etc.).
You ONLY return structured branch-control decisions as JSON.

You are the authority for branch-control commands. Be decisive when the packet shows a clear control need.

Your job: given a compact adjudication packet, choose how to steer branch lifecycle state:
leaders (provisional / validated), suppression/collapse policy, reseed windows, budget mode, and short notes.

Return JSON only in this exact shape:
{
  "rationale": "1-3 sentences, concrete",
  "confidence": "low" | "medium" | "high",
  "actions": [
    {
      "kind": "<one of the allowed kinds>",
      ... optional fields per kind ...
    }
  ]
}

Allowed action kinds and fields:
- set_provisional_leader: { "family_id": "<id or null to clear>" }
- set_validated_leader: { "family_id": "<id or null to clear>" }
- clear_provisional_leader: { } (or set_provisional_leader with family_id null)
- demote_family: { "family_id": "<id>" }
- suppress_family: { "family_id": "<id>", "reason": "<short reason>" }
- clear_suppression: { "family_id": "<id>" }  (clears exploit_dead when safe)
- mark_retryable: { "family_id": "<id>" }
- mark_unresolved: { "family_id": "<id>" }
- start_reseed_window: { }  (optional "reason": string)
- stop_reseed_window: { }
- set_budget_mode: { "budget_mode": "scouting" | "exploit" | "validation" | "collapse_recovery" | "wrap_up" }
- attach_manager_note: { "text": "<short note for logs/state>" }

Rules:
- Prefer minimal, high-signal actions. Empty actions is allowed when no change is best.
- When a clear priority exists, issue direct commands instead of vague note-only responses.
- Do not invent family_ids; use only ids present in candidate_families or leader fields in the packet.
- Never declare the run finished.
- Treat validation evidence and phase as primary context; avoid contradicting explicit failed retention without good cause.
- Raw frontier score is supporting evidence, not sole authority. Budget mode, validation evidence, and current leaders should usually dominate.
- Provisional/validated leaders are not assigned by the controller. After each scored eval you usually want set_provisional_leader (and set_validated_leader when evidence supports it) so the run has steering; use suppress_family for clear policy breaks (e.g. digest shows retention_failed, repeated timeframe mismatch, or hopeless coverage), with a reason string.
"""


def build_manager_packet(
    ctrl: Any,
    tool_context: Any,
    hook: ManagerHookEvent,
    step: int,
    step_limit: int,
    policy: Any,
    *,
    extra_issues: list[str] | None = None,
) -> ManagerPacket:
    phase_info = ctrl._run_phase_info(step, step_limit, policy)
    phase = str(phase_info.get("name") or "")
    overlay = ctrl._branch_overlay
    attempts = ctrl._run_attempts(tool_context.run_id)
    best = ctrl._best_attempt(attempts)
    frontier_best: float | None = None
    if isinstance(best, dict):
        raw = best.get("composite_score")
        if raw is not None:
            try:
                frontier_best = float(raw)
            except (TypeError, ValueError):
                frontier_best = None

    cand_limit = int(ctrl.config.manager.max_candidate_families_in_packet)
    ranked: list[tuple[float, str, Any]] = []
    for fid, st in ctrl._family_branches.items():
        if st.best_score is None:
            continue
        ranked.append((float(st.best_score), fid, st))
    ranked.sort(key=lambda x: x[0], reverse=not ctrl.config.research.plot_lower_is_better)
    candidate_families: list[dict[str, Any]] = []
    for sc, fid, st in ranked[:cand_limit]:
        candidate_families.append(
            {
                "family_id": fid,
                "best_score": sc,
                "lifecycle_state": st.lifecycle_state,
                "promotion_level": st.promotion_level,
                "retention_status": st.retention_status,
                "exploit_dead": st.exploit_dead,
                "last_validation_outcome": st.last_validation_outcome,
                "promotability_status": st.promotability_status,
            }
        )

    issues = list(extra_issues or [])
    dig = overlay.last_scored_validation_digest
    if isinstance(dig, dict):
        ev = dig.get("validation_evidence")
        if isinstance(ev, dict):
            if ev.get("outcome") == vo.VALIDATION_UNRESOLVED:
                issues.append("latest_validation_unresolved")
            if ev.get("timeframe_mismatch"):
                issues.append("timeframe_mismatch_in_evidence")
        if dig.get("retention_status") == "failed":
            issues.append("explicit_retention_failed_in_digest")

    return ManagerPacket(
        hook=hook,
        step=step,
        step_limit=step_limit,
        phase=phase,
        budget_mode=overlay.budget_mode,
        reseed_active=overlay.reseed_active,
        validation_stale_steps=int(ctrl._validation_stale_without_validated),
        frontier_best_score=frontier_best,
        frontier_prior_best=float(ctrl._frontier_prior_best)
        if ctrl._frontier_prior_best is not None
        else None,
        provisional_leader_family_id=overlay.provisional_leader_family_id,
        validated_leader_family_id=overlay.validated_leader_family_id,
        last_validation_digest=dig if isinstance(dig, dict) else None,
        candidate_families=candidate_families,
        recent_issues=sorted(set(issues)),
    )


def manager_user_message(packet: ManagerPacket) -> str:
    return (
        "Manager adjudication packet (JSON):\n"
        + json.dumps(packet.to_llm_dict(), ensure_ascii=True, indent=2)
    )
