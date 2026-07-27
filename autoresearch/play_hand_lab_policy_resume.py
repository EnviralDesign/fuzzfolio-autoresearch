from __future__ import annotations

from pathlib import Path
from typing import Any, Iterator, Mapping

from .durable_execution import DurableExecutionError, DurableExecutionJournal, _record_sha256


_INSTALLED = False
_ORIGINAL_RECOMPUTE_POLICY_STATE: Any = None


class CompactPolicyTaskMapping(Mapping[str, Any]):
    """Overlay compact, verified task evidence onto terminal journal stubs.

    The streaming PlayHand journal loader intentionally drops historical terminal
    payloads. Policy accounting still has to prove that every durable task was bound
    to its lane's frozen assignment. This mapping supplies only the three fields that
    check needs, without putting complete worker payloads back into the journal cache.
    """

    def __init__(
        self,
        source: Mapping[str, Any],
        compact_payloads: Mapping[str, Mapping[str, Any]],
    ) -> None:
        self._source = source
        self._compact_payloads = compact_payloads

    def __getitem__(self, key: str) -> Any:
        value = self._source[key]
        compact_payload = self._compact_payloads.get(str(key))
        if compact_payload is None:
            return value
        if not isinstance(value, dict):
            return value
        restored = dict(value)
        restored["payload"] = dict(compact_payload)
        return restored

    def get(self, key: str, default: Any = None) -> Any:
        try:
            return self[key]
        except KeyError:
            return default

    def __iter__(self) -> Iterator[str]:
        return iter(self._source)

    def __len__(self) -> int:
        return len(self._source)


def _terminal_tasks_needing_policy_payload(
    lanes: list[Any],
    durable_tasks_by_id: Mapping[str, Any],
) -> dict[str, Any]:
    needed: dict[str, Any] = {}
    for lane in lanes:
        task_specs = getattr(lane, "task_specs", {})
        for task_id in getattr(lane, "task_ids", ()):
            task_key = str(task_id)
            task_spec = task_specs.get(task_key) if isinstance(task_specs, dict) else None
            durable_task = durable_tasks_by_id.get(task_key)
            if (
                isinstance(durable_task, dict)
                and durable_task.get("status") == "terminal"
                and not isinstance(durable_task.get("payload"), dict)
                and (
                    not isinstance(task_spec, dict)
                    or not isinstance(task_spec.get("policy_assignment"), dict)
                )
            ):
                needed[task_key] = lane
    return needed


def _compact_policy_payloads_from_journal(
    play_hand_lab: Any,
    *,
    lanes: list[Any],
    durable_tasks_by_id: Mapping[str, Any],
    journal: DurableExecutionJournal | None,
) -> dict[str, dict[str, Any]]:
    """Read terminal register records once and retain only policy proof fields."""

    needed = _terminal_tasks_needing_policy_payload(lanes, durable_tasks_by_id)
    if not needed:
        return {}
    if journal is None:
        raise DurableExecutionError(
            "PlayHand policy resume requires the active durable journal for compacted tasks"
        )

    offsets = getattr(journal, "_play_hand_register_offsets", None)
    if not isinstance(offsets, dict):
        raise DurableExecutionError(
            "PlayHand policy resume has no terminal register offsets"
        )

    ordered: list[tuple[int, str, Any]] = []
    for task_id, lane in needed.items():
        offset = offsets.get(task_id)
        if offset is None:
            raise DurableExecutionError(
                f"durable policy lane has no task spec: {task_id}"
            )
        ordered.append((int(offset), task_id, lane))
    ordered.sort(key=lambda item: item[0])

    compact: dict[str, dict[str, Any]] = {}
    path = Path(journal.path)
    try:
        handle = path.open("rb")
    except OSError as exc:
        raise DurableExecutionError(
            f"execution journal is unreadable: {path}"
        ) from exc

    with handle:
        for offset, task_id, lane in ordered:
            handle.seek(offset)
            raw = handle.readline()
            try:
                record = play_hand_lab.json.loads(raw)
            except (UnicodeDecodeError, play_hand_lab.json.JSONDecodeError) as exc:
                raise DurableExecutionError(
                    f"execution journal is unreadable: {path}"
                ) from exc
            if not isinstance(record, dict):
                raise DurableExecutionError("execution journal record is malformed")
            if record.get("record_sha256") != _record_sha256(record):
                raise DurableExecutionError("execution journal record identity mismatch")

            payload = record.get("payload")
            cached = durable_tasks_by_id.get(task_id)
            expected_payload_sha256 = (
                str(cached.get("payload_sha256") or "")
                if isinstance(cached, dict)
                else ""
            )
            if (
                record.get("record_type") != "register"
                or str(record.get("task_id") or "") != task_id
                or not isinstance(payload, dict)
                or not expected_payload_sha256
                or str(record.get("payload_sha256") or "")
                != expected_payload_sha256
                or str(payload.get("task_id") or "") != task_id
                or str(payload.get("lane_id") or "") != str(lane.lane_id)
            ):
                raise DurableExecutionError(
                    f"durable policy lane has no task spec: {task_id}"
                )

            assignment = play_hand_lab._durable_task_policy_assignment(payload)
            if assignment != lane.policy_assignment:
                raise DurableExecutionError(
                    f"durable task policy assignment mismatch: {task_id}"
                )
            compact[task_id] = {
                "task_id": task_id,
                "lane_id": str(lane.lane_id),
                # Reuse the lane's already-resident immutable assignment rather than
                # retaining another journal-sized copy for every task in that lane.
                "policy_assignment": lane.policy_assignment,
            }
    return compact


def install_play_hand_policy_resume_recovery() -> None:
    """Restore policy verification over compact terminal journal records."""

    global _INSTALLED
    global _ORIGINAL_RECOMPUTE_POLICY_STATE

    if _INSTALLED:
        return

    from . import play_hand_lab
    from . import play_hand_lab_memory_deep as deep_memory

    _ORIGINAL_RECOMPUTE_POLICY_STATE = (
        play_hand_lab._recompute_campaign_policy_state_from_durable_lanes
    )

    def recompute_campaign_policy_state_from_durable_lanes(
        policy_state: dict[str, Any],
        *,
        lanes: list[Any],
        unresolved_tasks: list[dict[str, Any]],
        durable_tasks_by_id: Mapping[str, Any],
        pruned_lane_count: int,
    ) -> dict[str, Any]:
        compact_payloads = _compact_policy_payloads_from_journal(
            play_hand_lab,
            lanes=lanes,
            durable_tasks_by_id=durable_tasks_by_id,
            journal=deep_memory._ACTIVE_PLAY_HAND_JOURNAL,
        )
        return _ORIGINAL_RECOMPUTE_POLICY_STATE(
            policy_state,
            lanes=lanes,
            unresolved_tasks=unresolved_tasks,
            durable_tasks_by_id=CompactPolicyTaskMapping(
                durable_tasks_by_id,
                compact_payloads,
            ),
            pruned_lane_count=pruned_lane_count,
        )

    play_hand_lab._recompute_campaign_policy_state_from_durable_lanes = (
        recompute_campaign_policy_state_from_durable_lanes
    )
    _INSTALLED = True


__all__ = [
    "CompactPolicyTaskMapping",
    "install_play_hand_policy_resume_recovery",
]
