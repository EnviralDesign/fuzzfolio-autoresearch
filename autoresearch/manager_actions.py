"""Parse manager LLM output and apply branch-control actions deterministically."""

from __future__ import annotations

from typing import Any

from . import branch_lifecycle as bl
from . import branch_mechanics as bmech
from . import validation_outcome as vo
from .manager_models import ManagerAction, ManagerActionKind, ManagerDecision

_BUDGET_ALIASES = {
    "scouting": bl.BUDGET_SCOUTING,
    "exploit": bl.BUDGET_EXPLOIT,
    "validation": bl.BUDGET_VALIDATION,
    "collapse_recovery": bl.BUDGET_COLLAPSE_RECOVERY,
    "wrap_up": bl.BUDGET_WRAP_UP,
}


def parse_manager_decision(raw: dict[str, Any]) -> ManagerDecision | None:
    if not isinstance(raw, dict):
        return None
    rationale = str(raw.get("rationale") or "").strip()
    conf = str(raw.get("confidence") or "medium").strip().lower()
    if conf not in {"low", "medium", "high"}:
        conf = "medium"
    actions_raw = raw.get("actions")
    if actions_raw is None:
        return ManagerDecision(rationale=rationale or "(no rationale)", actions=[], confidence=conf)
    if not isinstance(actions_raw, list):
        return None
    parsed: list[ManagerAction] = []
    for item in actions_raw:
        if not isinstance(item, dict):
            continue
        kind_str = str(item.get("kind") or "").strip()
        if not kind_str:
            continue
        try:
            kind = ManagerActionKind(kind_str)
        except ValueError:
            continue
        payload = {k: v for k, v in item.items() if k != "kind"}
        parsed.append(ManagerAction(kind=kind, payload=payload))
    return ManagerDecision(
        rationale=rationale or "(no rationale)", actions=parsed, confidence=conf
    )


def _valid_family(ctrl: Any, fid: str | None) -> bool:
    if not fid or not str(fid).strip():
        return False
    return str(fid).strip() in ctrl._family_branches


def _clear_overlay_family_refs(ctrl: Any, family_id: str) -> None:
    overlay = ctrl._branch_overlay
    if overlay.provisional_leader_family_id == family_id:
        overlay.provisional_leader_family_id = None
        overlay.provisional_leader_promotability = None
    if overlay.validated_leader_family_id == family_id:
        overlay.validated_leader_family_id = None
    if overlay.shadow_leader_family_id == family_id:
        overlay.shadow_leader_family_id = None
        overlay.shadow_leader_reason = None


def apply_manager_decision(
    ctrl: Any,
    tool_context: Any,
    decision: ManagerDecision,
    step: int,
    step_limit: int,
    policy: Any,
) -> list[dict[str, Any]]:
    """Apply actions in order; return serialized record of applied actions."""
    applied: list[dict[str, Any]] = []
    overlay = ctrl._branch_overlay
    cfg = ctrl.config.research

    for act in decision.actions:
        kind = act.kind
        pl = act.payload
        rec: dict[str, Any] = {"kind": kind.value, "ok": True}

        if kind == ManagerActionKind.set_provisional_leader:
            fid = pl.get("family_id")
            if fid is None or fid == "null":
                overlay.provisional_leader_family_id = None
                overlay.provisional_leader_promotability = None
                rec["family_id"] = None
            elif _valid_family(ctrl, str(fid)):
                overlay.provisional_leader_family_id = str(fid).strip()
                st = ctrl._family_branches[overlay.provisional_leader_family_id]
                overlay.provisional_leader_promotability = st.promotability_status
                rec["family_id"] = overlay.provisional_leader_family_id
            else:
                rec["ok"] = False
                rec["error"] = "unknown family_id"

        elif kind == ManagerActionKind.set_validated_leader:
            fid = pl.get("family_id")
            if fid is None or fid == "null":
                overlay.validated_leader_family_id = None
                rec["family_id"] = None
            elif _valid_family(ctrl, str(fid)):
                overlay.validated_leader_family_id = str(fid).strip()
                rec["family_id"] = overlay.validated_leader_family_id
            else:
                rec["ok"] = False
                rec["error"] = "unknown family_id"

        elif kind == ManagerActionKind.clear_provisional_leader:
            current = overlay.provisional_leader_family_id
            overlay.provisional_leader_family_id = None
            overlay.provisional_leader_promotability = None
            if current and current in ctrl._family_branches:
                st = ctrl._family_branches[current]
                if st.lifecycle_state == bl.LIFECYCLE_PROVISIONAL_LEADER:
                    st.lifecycle_state = bl.LIFECYCLE_PROVISIONAL_CONTENDER

        elif kind == ManagerActionKind.clear_validated_leader:
            current = overlay.validated_leader_family_id
            overlay.validated_leader_family_id = None
            if current and current in ctrl._family_branches:
                st = ctrl._family_branches[current]
                if st.lifecycle_state == bl.LIFECYCLE_VALIDATED_LEADER:
                    st.lifecycle_state = bl.LIFECYCLE_PROVISIONAL_CONTENDER

        elif kind == ManagerActionKind.demote_family:
            fid = str(pl.get("family_id") or "").strip()
            if not _valid_family(ctrl, fid):
                rec["ok"] = False
                rec["error"] = "unknown family_id"
            else:
                _clear_overlay_family_refs(ctrl, fid)
                st = ctrl._family_branches[fid]
                if st.promotion_level != bl.PROMOTION_VALIDATED:
                    st.promotion_level = bl.PROMOTION_PROVISIONAL
                if st.lifecycle_state not in {
                    bl.LIFECYCLE_COLLAPSED,
                    bl.LIFECYCLE_VALIDATED_LEADER,
                }:
                    st.lifecycle_state = bl.LIFECYCLE_PROVISIONAL_CONTENDER
                rec["family_id"] = fid

        elif kind == ManagerActionKind.suppress_family:
            fid = str(pl.get("family_id") or "").strip()
            reason = str(pl.get("reason") or "manager_suppress").strip() or "manager_suppress"
            if not _valid_family(ctrl, fid):
                rec["ok"] = False
                rec["error"] = "unknown family_id"
            else:
                _clear_overlay_family_refs(ctrl, fid)
                bmech.mark_family_collapsed(
                    ctrl, tool_context, fid, reason, step, step_limit
                )
                rec["family_id"] = fid

        elif kind == ManagerActionKind.clear_suppression:
            fid = str(pl.get("family_id") or "").strip()
            if not _valid_family(ctrl, fid):
                rec["ok"] = False
                rec["error"] = "unknown family_id"
            else:
                st = ctrl._family_branches[fid]
                if st.hard_dead:
                    rec["ok"] = False
                    rec["error"] = "hard_dead family"
                else:
                    st.exploit_dead = False
                    st.bankrupt = False
                    if st.lifecycle_state == bl.LIFECYCLE_COLLAPSED:
                        st.lifecycle_state = bl.LIFECYCLE_SCOUT
                    st.collapse_reason = None
                    rec["family_id"] = fid

        elif kind == ManagerActionKind.mark_retryable:
            fid = str(pl.get("family_id") or "").strip()
            if _valid_family(ctrl, fid):
                ctrl._family_branches[fid].promotability_status = (
                    vo.PROMOTABILITY_RETRY_RECOMMENDED
                )
                rec["family_id"] = fid
            else:
                rec["ok"] = False
                rec["error"] = "unknown family_id"

        elif kind == ManagerActionKind.mark_unresolved:
            fid = str(pl.get("family_id") or "").strip()
            if _valid_family(ctrl, fid):
                ctrl._family_branches[fid].last_validation_outcome = (
                    vo.VALIDATION_UNRESOLVED
                )
                rec["family_id"] = fid
            else:
                rec["ok"] = False
                rec["error"] = "unknown family_id"

        elif kind == ManagerActionKind.start_reseed_window:
            remaining = step_limit - step
            if remaining < cfg.reseed_min_remaining_steps:
                rec["ok"] = False
                rec["error"] = "not enough steps remaining"
            elif overlay.validated_leader_family_id:
                rec["ok"] = False
                rec["error"] = "validated leader exists"
            else:
                overlay.reseed_active = True
                if overlay.reseed_started_step is None:
                    overlay.reseed_started_step = step
                overlay.collapse_recovery_remaining = max(
                    overlay.collapse_recovery_remaining,
                    cfg.collapse_recovery_max_steps,
                )

        elif kind == ManagerActionKind.stop_reseed_window:
            overlay.reseed_active = False
            overlay.collapse_recovery_remaining = 0

        elif kind == ManagerActionKind.set_budget_mode:
            raw_mode = str(pl.get("budget_mode") or "").strip().lower()
            norm = _BUDGET_ALIASES.get(raw_mode)
            if not norm:
                rec["ok"] = False
                rec["error"] = "invalid budget_mode"
            else:
                overlay.budget_mode = norm
                rec["budget_mode"] = norm

        elif kind == ManagerActionKind.attach_manager_note:
            text = str(pl.get("text") or "").strip()
            if text:
                ctrl._manager_runtime.manager_notes.append(text[:2000])
            rec["text_len"] = len(text)

        applied.append(rec)

    if decision.actions:
        bmech.apply_overlay_provisional_leadership(ctrl)
    return applied
