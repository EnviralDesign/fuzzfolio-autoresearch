from __future__ import annotations

import json
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Iterable, Mapping

from .durable_execution import (
    JOURNAL_SCHEMA,
    DurableExecutionError,
    DurableExecutionJournal,
    _record_sha256,
)

try:  # pragma: no cover - optional C extension.
    import orjson as _orjson
except Exception:  # pragma: no cover - stdlib fallback.
    _orjson = None


_LOCK = threading.RLock()
_INSTALLED = False

_ORIGINAL_JOURNAL_LOAD_FROM_DISK: Any = None
_ORIGINAL_HYDRATE_UNRESOLVED_TASK_SPECS: Any = None
_ORIGINAL_WRITE_CAMPAIGN_STATE: Any = None
_ORIGINAL_FORMAT_BARRIER: Any = None

_LOADED_STATE_SHAPES: dict[str, tuple[Any, ...] | None] = {}
_DIAGNOSTICS: dict[str, Any] = {
    "journal_load_seconds": 0.0,
    "terminal_record_hashes_deferred": 0,
    "pending_task_records_validated": 0,
    "campaign_state_load_seconds": 0.0,
    "active_profiles_hydrated": 0,
    "terminal_profiles_skipped": 0,
    "terminal_lanes_skipped_during_task_hydration": 0,
    "policy_task_payload_reads_avoided": 0,
    "policy_metadata_reads_avoided": 0,
    "initial_state_rewrites_skipped": 0,
}
_SHA256_BYTES = rb"sha256:[0-9a-f]{64}"
_REGISTER_SUFFIX_RE = re.compile(
    rb',"payload_sha256":"(?P<payload_sha256>' + _SHA256_BYTES + rb')"'
    rb',"record_sha256":"' + _SHA256_BYTES + rb'"'
    rb',"record_type":"register","task_id":"(?P<task_id>[^"]+)"}\r?\n?$'
)
_COMPLETE_SUFFIX_RE = re.compile(
    rb',"receipt_sha256":"(?P<receipt_sha256>' + _SHA256_BYTES + rb')"'
    rb',"record_sha256":"' + _SHA256_BYTES + rb'"'
    rb',"record_type":"complete","task_id":"(?P<task_id>[^"]+)"}\r?\n?$'
)
_REVOKE_SUFFIX_RE = re.compile(
    rb'{"record_sha256":"' + _SHA256_BYTES + rb'"'
    rb',"record_type":"revoke","task_id":"(?P<task_id>[^"]+)"}\r?\n?$'
)


def startup_diagnostics() -> dict[str, Any]:
    with _LOCK:
        return dict(_DIAGNOSTICS)


def _set_diagnostic(name: str, value: Any) -> None:
    with _LOCK:
        _DIAGNOSTICS[name] = value


def _increment_diagnostic(name: str, amount: int = 1) -> None:
    with _LOCK:
        _DIAGNOSTICS[name] = int(_DIAGNOSTICS.get(name, 0)) + int(amount)


def _json_loads(raw: bytes) -> Any:
    if _orjson is not None:
        return _orjson.loads(raw)
    return json.loads(raw)


def _path_key(path: Path | str) -> str:
    return str(Path(path).resolve(strict=False))


def _fast_read_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = _json_loads(path.read_bytes())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise DurableExecutionError(f"JSON state is unreadable: {path}") from exc
    if not isinstance(payload, dict):
        raise DurableExecutionError(f"JSON state must be an object: {path}")
    return payload


def _load_play_hand_journal_frontier(
    journal: DurableExecutionJournal,
    deep_memory: Any,
) -> dict[str, Any]:
    """Rebuild live work without reparsing immutable terminal payloads.

    Journal records are canonical JSON. For completed history, the small canonical
    suffix contains every field needed to reconstruct status and lazy file offsets.
    Full JSON parsing and SHA verification are reserved for pending/revoked tasks and
    for terminal receipts that are actually reused later.
    """

    path = Path(journal.path)
    tasks: dict[str, dict[str, Any]] = {}
    register_offsets: dict[str, int] = {}
    complete_offsets: dict[str, int] = {}
    compacted_ids: set[str] = set()
    header: dict[str, Any] | None = None
    terminal_hashes_deferred = 0

    try:
        handle = path.open("rb")
    except OSError as exc:
        raise DurableExecutionError(
            f"execution journal is unreadable: {path}"
        ) from exc

    with handle:
        while True:
            offset = handle.tell()
            raw = handle.readline()
            if not raw:
                break
            if not raw.strip():
                continue
            if header is None:
                try:
                    candidate = _json_loads(raw)
                except (
                    UnicodeDecodeError,
                    json.JSONDecodeError,
                    ValueError,
                ) as exc:
                    raise DurableExecutionError(
                        f"execution journal is unreadable: {path}"
                    ) from exc
                if (
                    not isinstance(candidate, dict)
                    or candidate.get("record_type") != "header"
                ):
                    return _ORIGINAL_JOURNAL_LOAD_FROM_DISK(journal)
                if candidate.get("schema_version") != JOURNAL_SCHEMA:
                    return _ORIGINAL_JOURNAL_LOAD_FROM_DISK(journal)
                if candidate.get("header_sha256") != _record_sha256(
                    candidate
                ):
                    raise DurableExecutionError(
                        "execution journal header identity mismatch"
                    )
                header = candidate
                continue

            register_match = _REGISTER_SUFFIX_RE.search(raw)
            if register_match is not None:
                task_id = register_match.group("task_id").decode("ascii")
                payload_sha256 = register_match.group(
                    "payload_sha256"
                ).decode("ascii")
                existing = tasks.get(task_id)
                if existing is not None:
                    if existing.get("payload_sha256") != payload_sha256:
                        raise DurableExecutionError(
                            f"task payload conflicts with durable graph: {task_id}"
                        )
                    continue
                register_offsets[task_id] = int(offset)
                tasks[task_id] = {
                    "task_id": task_id,
                    "payload_sha256": payload_sha256,
                    "status": "pending",
                    "terminal_receipt": None,
                }
                continue

            complete_match = _COMPLETE_SUFFIX_RE.search(raw)
            if complete_match is not None:
                task_id = complete_match.group("task_id").decode("ascii")
                receipt_sha256 = complete_match.group(
                    "receipt_sha256"
                ).decode("ascii")
                task = tasks.get(task_id)
                if not isinstance(task, dict):
                    raise DurableExecutionError(
                        f"terminal receipt references unknown task: {task_id}"
                    )
                if task.get("status") == "terminal":
                    existing = task.get("terminal_receipt")
                    if (
                        not isinstance(existing, dict)
                        or existing.get("receipt_sha256") != receipt_sha256
                    ):
                        raise DurableExecutionError(
                            f"conflicting duplicate terminal receipt: {task_id}"
                        )
                    continue
                task["status"] = "terminal"
                task["terminal_receipt"] = {
                    "receipt_sha256": receipt_sha256
                }
                complete_offsets[task_id] = int(offset)
                compacted_ids.add(task_id)
                terminal_hashes_deferred += 1
                continue

            revoke_match = _REVOKE_SUFFIX_RE.search(raw)
            if revoke_match is not None:
                record = deep_memory._decode_json_record(raw, path=path)
                if record.get("record_sha256") != _record_sha256(record):
                    raise DurableExecutionError(
                        "execution journal record identity mismatch"
                    )
                task_id = revoke_match.group("task_id").decode("ascii")
                task = tasks.get(task_id)
                register_offset = register_offsets.get(task_id)
                if not isinstance(task, dict) or register_offset is None:
                    raise DurableExecutionError(
                        f"revoke references unknown task: {task_id}"
                    )
                register = deep_memory._read_record_at_offset(
                    path,
                    int(register_offset),
                )
                payload = register.get("payload")
                if (
                    register.get("record_type") != "register"
                    or str(register.get("task_id") or "") != task_id
                    or not isinstance(payload, dict)
                    or str(register.get("payload_sha256") or "")
                    != str(task.get("payload_sha256") or "")
                ):
                    raise DurableExecutionError(
                        f"revoke register payload conflicts: {task_id}"
                    )
                task["payload"] = payload
                task["status"] = "pending"
                task["terminal_receipt"] = None
                complete_offsets.pop(task_id, None)
                compacted_ids.discard(task_id)
                continue

            # Preserve compatibility with any noncanonical legacy record shape.
            return _ORIGINAL_JOURNAL_LOAD_FROM_DISK(journal)

    if header is None:
        raise DurableExecutionError("execution journal is missing header")
    if (
        header.get("execution_id") != journal.execution_id
        or header.get("lineage") != journal.lineage
    ):
        raise DurableExecutionError("execution journal lineage mismatch")

    pending_validated = 0
    for task_id, task in tasks.items():
        if task.get("status") == "terminal":
            continue
        offset = register_offsets.get(task_id)
        if offset is None:
            raise DurableExecutionError(
                f"durable task payload is missing: {task_id}"
            )
        register = deep_memory._read_record_at_offset(path, int(offset))
        payload = register.get("payload")
        if (
            register.get("record_type") != "register"
            or str(register.get("task_id") or "") != task_id
            or not isinstance(payload, dict)
            or str(register.get("payload_sha256") or "")
            != str(task.get("payload_sha256") or "")
            or journal.task_payload_sha256(payload)
            != task.get("payload_sha256")
        ):
            raise DurableExecutionError(
                f"durable pending task payload conflicts: {task_id}"
            )
        task["payload"] = payload
        pending_validated += 1

    journal._header_sha256 = str(header["header_sha256"])
    journal._tasks = tasks
    setattr(journal, "_play_hand_register_offsets", register_offsets)
    setattr(journal, "_play_hand_complete_offsets", complete_offsets)
    setattr(journal, "_play_hand_compacted_terminal_ids", compacted_ids)
    setattr(journal, "_play_hand_memory_compaction_enabled", True)
    _set_diagnostic(
        "terminal_record_hashes_deferred",
        terminal_hashes_deferred,
    )
    _set_diagnostic(
        "pending_task_records_validated",
        pending_validated,
    )
    return journal._view()


def _compact_recorded_sweep_result(recorded: Any) -> Any:
    if not isinstance(recorded, dict):
        return recorded
    if not isinstance(recorded.get("sweep_payload"), dict):
        return recorded
    if not str(recorded.get("artifact_dir") or ""):
        return recorded
    compact = dict(recorded)
    compact.pop("sweep_payload", None)
    compact["sweep_payload_artifact"] = str(
        compact.get("sweep_payload_artifact") or "sweep-results.json"
    )
    return compact


def _lane_state_from_payload(
    play_hand_lab: Any,
    payload: dict[str, Any],
) -> tuple[Any, bool]:
    values = dict(payload)
    legacy_heavy = bool(
        values.get("profile_payload")
        or values.get("incumbent_profile_payload")
        or values.get("task_specs")
        or values.get("last_sweep_payload")
    )

    values["run_dir"] = Path(str(values["run_dir"])).resolve(strict=False)
    for key in ("profile_path", "incumbent_profile_path"):
        values[key] = (
            Path(str(values[key])).resolve(strict=False)
            if values.get(key)
            else None
        )
    for key in ("completed_task_ids", "failed_task_ids"):
        values[key] = {str(item) for item in values.get(key) or []}

    terminal = bool(values.get("terminal"))
    if terminal:
        values["profile_payload"] = None
        values["incumbent_profile_payload"] = None
        values["phase_results"] = {}
    else:
        # The profile file is already the durable source. Do not deserialize an
        # embedded legacy copy and then retain a second copy of the same profile.
        if values.get("profile_path") and values["profile_path"].is_file():
            values["profile_payload"] = None
        if (
            values.get("incumbent_profile_path")
            and values["incumbent_profile_path"].is_file()
        ):
            values["incumbent_profile_payload"] = None
        raw_phase_results = values.get("phase_results")
        if isinstance(raw_phase_results, dict):
            compacted: dict[str, list[Any]] = {}
            for phase, rows in raw_phase_results.items():
                if not isinstance(rows, list):
                    continue
                compacted_rows = [
                    _compact_recorded_sweep_result(row) for row in rows
                ]
                legacy_heavy = legacy_heavy or compacted_rows != rows
                compacted[str(phase)] = compacted_rows
            values["phase_results"] = compacted

    # The execution journal is the task-payload authority. Only unfinished task
    # specs are rebuilt after the lanes and journal have both loaded.
    values["task_specs"] = {}
    values["last_sweep_payload"] = None
    return play_hand_lab.LabLaneState(**values), legacy_heavy


def _hydrate_lane_profiles(play_hand_lab: Any, lane: Any) -> None:
    if (
        lane.profile_payload is None
        and lane.profile_path is not None
        and lane.profile_path.is_file()
    ):
        lane.profile_payload = play_hand_lab._inner_profile_payload(
            _fast_read_json_object(lane.profile_path)
        )
    if (
        lane.incumbent_profile_payload is None
        and lane.incumbent_profile_path is not None
        and lane.incumbent_profile_path.is_file()
    ):
        lane.incumbent_profile_payload = play_hand_lab._inner_profile_payload(
            _fast_read_json_object(lane.incumbent_profile_path)
        )


def _state_shape(
    lanes: Iterable[Any],
    *,
    next_lane_index: int,
    reserved_lane_indices: Iterable[int],
    recorded_result_count: int,
) -> tuple[Any, ...]:
    lane_shape = tuple(
        (
            int(getattr(lane, "lane_index", 0)),
            str(getattr(lane, "current_phase", "") or ""),
            bool(getattr(lane, "terminal", False)),
            len(getattr(lane, "task_ids", ()) or ()),
            len(getattr(lane, "completed_task_ids", ()) or ()),
            len(getattr(lane, "failed_task_ids", ()) or ()),
        )
        for lane in lanes
    )
    return (
        int(next_lane_index),
        tuple(sorted(int(item) for item in reserved_lane_indices)),
        int(recorded_result_count),
        lane_shape,
    )


def _load_campaign_state_fast(
    play_hand_lab: Any,
    path: Path,
    *,
    runtime: Any,
    campaign_id: str,
) -> tuple[list[Any], Any, int, list[int], int]:
    started = time.monotonic()
    payload = _fast_read_json_object(path)
    if payload.get("schema_version") != "play-hand-lab-durable-state-v1":
        raise DurableExecutionError("PlayHand durable state schema mismatch")
    if payload.get("lineage") != play_hand_lab._campaign_state_lineage(
        runtime,
        campaign_id,
    ):
        raise DurableExecutionError("PlayHand durable state lineage mismatch")

    lanes: list[Any] = []
    legacy_heavy = False
    for item in payload.get("lanes") or []:
        if not isinstance(item, dict):
            continue
        lane, was_heavy = _lane_state_from_payload(play_hand_lab, dict(item))
        lanes.append(lane)
        legacy_heavy = legacy_heavy or was_heavy

    # Terminal lanes never execute or transition again. Historically loading both
    # profile files for every retained terminal lane made startup scale with total
    # campaign history instead of the active frontier.
    active_lanes = [lane for lane in lanes if not bool(lane.terminal)]
    worker_count = min(len(active_lanes), max(1, min(8, os.cpu_count() or 4)))
    if worker_count > 1:
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            list(
                executor.map(
                    lambda lane: _hydrate_lane_profiles(play_hand_lab, lane),
                    active_lanes,
                )
            )
    else:
        for lane in active_lanes:
            _hydrate_lane_profiles(play_hand_lab, lane)

    history = play_hand_lab.LabCampaignHistory(
        **dict(payload.get("history") or {})
    )
    policy_state = payload.get("campaign_policy_state")
    if policy_state is not None:
        if not isinstance(policy_state, dict):
            raise DurableExecutionError(
                "PlayHand durable campaign policy state is invalid"
            )
        history.campaign_policy_state = dict(policy_state)

    next_lane_index = int(payload.get("next_lane_index") or 0)
    reserved_lane_indices = sorted(
        {int(item) for item in payload.get("reserved_lane_indices") or []}
    )
    recorded_result_count = int(payload.get("recorded_result_count") or 0)
    shape = _state_shape(
        lanes,
        next_lane_index=next_lane_index,
        reserved_lane_indices=reserved_lane_indices,
        recorded_result_count=recorded_result_count,
    )
    with _LOCK:
        _LOADED_STATE_SHAPES[_path_key(path)] = (
            None if legacy_heavy else shape
        )
        _DIAGNOSTICS["campaign_state_load_seconds"] = round(
            time.monotonic() - started,
            3,
        )
        _DIAGNOSTICS["active_profiles_hydrated"] = len(active_lanes)
        _DIAGNOSTICS["terminal_profiles_skipped"] = (
            len(lanes) - len(active_lanes)
        )
    return (
        lanes,
        history,
        next_lane_index,
        reserved_lane_indices,
        recorded_result_count,
    )


def _recompute_policy_state_fast(
    play_hand_lab: Any,
    policy_state: dict[str, Any],
    *,
    lanes: list[Any],
    unresolved_tasks: list[dict[str, Any]],
    durable_tasks_by_id: Mapping[str, Any],
    pruned_lane_count: int,
) -> dict[str, Any]:
    """Rebuild policy accounting from lanes without reopening terminal payloads."""

    if pruned_lane_count:
        raise DurableExecutionError(
            "policy-honest resume cannot verify pruned lane assignments"
        )

    # The static policy identity is immutable and only read below. Replacing the
    # mutable accounting maps avoids copying the complete finite lane plan.
    rebuilt = dict(policy_state)
    lane_plan = rebuilt.get("lane_plan")
    planned = rebuilt.get("planned_lane_counts")
    if not isinstance(lane_plan, list) or not isinstance(planned, dict):
        raise DurableExecutionError("policy state has no durable lane allocation")
    dimensions = ("family", "recipe", "instrument", "timeframe", "indicator")
    rebuilt["assigned_lane_counts"] = {lane: 0 for lane in planned}
    rebuilt["used_lane_counts"] = {lane: 0 for lane in planned}
    rebuilt["exhausted_lane_counts"] = {lane: 0 for lane in planned}
    rebuilt["accounting"] = {dimension: {} for dimension in dimensions}
    rebuilt["exhaustion_outcomes"] = {}

    durable_task_ids = set(durable_tasks_by_id)
    lanes_by_task: dict[str, Any] = {}
    seen_lane_indices: set[int] = set()
    for lane in sorted(lanes, key=lambda item: item.lane_index):
        if lane.lane_index in seen_lane_indices:
            raise DurableExecutionError(
                f"duplicate durable policy lane index: {lane.lane_index}"
            )
        seen_lane_indices.add(lane.lane_index)
        expected_lane = play_hand_lab._policy_lane_for_index(
            rebuilt,
            lane.lane_index,
        )
        assignment = lane.policy_assignment
        if not isinstance(assignment, dict) or not assignment:
            raise DurableExecutionError(
                f"durable policy lane has no assignment: {lane.lane_id}"
            )
        if (
            assignment.get("policy_lane") != expected_lane
            or assignment.get("policy_manifest_sha256")
            != rebuilt.get("policy_manifest_sha256")
        ):
            raise DurableExecutionError(
                f"durable policy lane assignment mismatch: {lane.lane_id}"
            )
        execution = rebuilt.get("execution")
        expected_allocation = {
            "lane_index": lane.lane_index,
            "planned_lane_count": planned.get(expected_lane),
            "algorithm": (execution or {}).get("allocation_algorithm"),
            "algorithm_version": (execution or {}).get(
                "allocation_algorithm_version"
            ),
            "lane_tie_break_order": (execution or {}).get(
                "lane_tie_break_order"
            ),
            "candidate_tie_break_order": (execution or {}).get(
                "candidate_tie_break_order"
            ),
        }
        if (
            not isinstance(execution, dict)
            or assignment.get("allocation") != expected_allocation
        ):
            raise DurableExecutionError(
                f"durable policy lane allocation mismatch: {lane.lane_id}"
            )
        if assignment.get("negative_prior_runtime") != rebuilt.get(
            "negative_prior_runtime"
        ):
            raise DurableExecutionError(
                f"durable policy negative-prior binding mismatch: {lane.lane_id}"
            )

        outcome = str(assignment.get("policy_outcome_type") or "")
        cap_decision = assignment.get("cap_decision")
        if outcome == "policy_lane_selected":
            attributes = assignment.get("candidate_attributes")
            if not isinstance(attributes, dict) or not isinstance(
                cap_decision, dict
            ):
                raise DurableExecutionError(
                    "durable selected policy lane has incomplete accounting: "
                    f"{lane.lane_id}"
                )
            observed_cap = play_hand_lab._policy_cap_decision(
                rebuilt,
                attributes,
            )
            if (
                observed_cap.get("outcome") != "accepted"
                or observed_cap != cap_decision
            ):
                raise DurableExecutionError(
                    f"durable policy cap decision mismatch: {lane.lane_id}"
                )
            play_hand_lab._record_policy_assignment(
                rebuilt,
                lane=expected_lane,
                cap_decision=observed_cap,
            )
        elif outcome in {
            play_hand_lab.POLICY_EXHAUSTION_OUTCOME,
            "policy_cap_exhausted",
        }:
            if cap_decision is not None or lane.task_ids or not lane.terminal:
                raise DurableExecutionError(
                    f"durable exhausted policy lane is contradictory: {lane.lane_id}"
                )
            play_hand_lab._record_policy_assignment(
                rebuilt,
                lane=expected_lane,
                cap_decision=None,
                exhaustion_outcome=outcome,
            )
        else:
            raise DurableExecutionError(
                f"durable policy lane has unsupported outcome: {lane.lane_id}"
            )
        if int(rebuilt["assigned_lane_counts"].get(expected_lane) or 0) > int(
            planned.get(expected_lane) or 0
        ):
            raise DurableExecutionError(
                f"durable policy lane quota exceeded: {expected_lane}"
            )

        for task_id in lane.task_ids:
            task_key = str(task_id)
            if task_key not in durable_task_ids:
                raise DurableExecutionError(
                    f"durable policy lane has no task: {task_key}"
                )
            if task_key in lanes_by_task:
                raise DurableExecutionError(
                    f"duplicate durable policy task id: {task_key}"
                )
            lanes_by_task[task_key] = lane

    # Only unresolved tasks can still affect future work. Their complete envelopes
    # remain in memory, so verify their exact assignment. Terminal tasks were
    # already checked before their sealed completion was appended; rereading every
    # historical register record here duplicated the journal scan and random I/O.
    for task in unresolved_tasks:
        task_id = str(task.get("task_id") or "")
        lane = lanes_by_task.get(task_id)
        if (
            lane is None
            or play_hand_lab._durable_task_policy_assignment(task)
            != lane.policy_assignment
        ):
            raise DurableExecutionError(
                "durable journal task policy assignment mismatch: "
                f"{task_id or '<missing>'}"
            )

    mutable_fields = (
        "assigned_lane_counts",
        "used_lane_counts",
        "exhausted_lane_counts",
        "accounting",
        "exhaustion_outcomes",
    )
    if any(
        policy_state.get(field) != rebuilt.get(field)
        for field in mutable_fields
    ):
        raise DurableExecutionError(
            "durable campaign policy counters do not match persisted lane assignments"
        )
    with _LOCK:
        _DIAGNOSTICS["policy_task_payload_reads_avoided"] = max(
            len(lanes_by_task) - len(unresolved_tasks),
            0,
        )
        _DIAGNOSTICS["policy_metadata_reads_avoided"] = len(lanes)
    return rebuilt


def install_play_hand_startup_bounds() -> None:
    """Install the default bounded resume path for large PlayHand campaigns."""

    global _INSTALLED
    global _ORIGINAL_JOURNAL_LOAD_FROM_DISK
    global _ORIGINAL_HYDRATE_UNRESOLVED_TASK_SPECS
    global _ORIGINAL_WRITE_CAMPAIGN_STATE
    global _ORIGINAL_FORMAT_BARRIER

    with _LOCK:
        if _INSTALLED:
            return

        from . import play_hand_lab
        from . import play_hand_lab_memory_deep as deep_memory

        _ORIGINAL_JOURNAL_LOAD_FROM_DISK = (
            DurableExecutionJournal._load_from_disk
        )
        _ORIGINAL_HYDRATE_UNRESOLVED_TASK_SPECS = (
            play_hand_lab._hydrate_unresolved_lane_task_specs
        )
        _ORIGINAL_WRITE_CAMPAIGN_STATE = play_hand_lab._write_campaign_state
        _ORIGINAL_FORMAT_BARRIER = play_hand_lab._format_lab_barrier_snapshot

        # The streaming journal path previously used stdlib json even when orjson
        # was installed. This changes parsing only; canonical hash verification is
        # unchanged for records that can still influence future work.
        def decode_json_record(raw: bytes, *, path: Path) -> dict[str, Any]:
            try:
                record = _json_loads(raw)
            except (
                UnicodeDecodeError,
                json.JSONDecodeError,
                ValueError,
            ) as exc:
                raise DurableExecutionError(
                    f"execution journal is unreadable: {path}"
                ) from exc
            if not isinstance(record, dict):
                raise DurableExecutionError(
                    "execution journal record is malformed"
                )
            return record

        def load_from_disk(
            journal: DurableExecutionJournal,
        ) -> dict[str, Any]:
            started = time.monotonic()
            if Path(journal.path).name == (
                "play-hand-lab-execution-journal.json"
            ):
                result = _load_play_hand_journal_frontier(
                    journal,
                    deep_memory,
                )
                _set_diagnostic(
                    "journal_load_seconds",
                    round(time.monotonic() - started, 3),
                )
                return result
            return _ORIGINAL_JOURNAL_LOAD_FROM_DISK(journal)

        def load_campaign_state(
            path: Path,
            *,
            runtime: Any,
            campaign_id: str,
        ) -> tuple[list[Any], Any, int, list[int], int]:
            return _load_campaign_state_fast(
                play_hand_lab,
                path,
                runtime=runtime,
                campaign_id=campaign_id,
            )

        def hydrate_unresolved_lane_task_specs(
            lanes: list[Any],
            durable_tasks_by_id: Mapping[str, Any],
        ) -> None:
            active_lanes = [
                lane for lane in lanes if not bool(lane.terminal)
            ]
            _increment_diagnostic(
                "terminal_lanes_skipped_during_task_hydration",
                len(lanes) - len(active_lanes),
            )
            _ORIGINAL_HYDRATE_UNRESOLVED_TASK_SPECS(
                active_lanes,
                durable_tasks_by_id,
            )

        def recompute_policy_state(
            policy_state: dict[str, Any],
            *,
            lanes: list[Any],
            unresolved_tasks: list[dict[str, Any]],
            durable_tasks_by_id: Mapping[str, Any],
            pruned_lane_count: int,
        ) -> dict[str, Any]:
            return _recompute_policy_state_fast(
                play_hand_lab,
                policy_state,
                lanes=lanes,
                unresolved_tasks=unresolved_tasks,
                durable_tasks_by_id=durable_tasks_by_id,
                pruned_lane_count=pruned_lane_count,
            )

        def write_campaign_state(*args: Any, **kwargs: Any) -> Any:
            path = kwargs.get("path") or (args[0] if args else "")
            lanes = kwargs.get("lanes")
            if isinstance(lanes, list):
                current_shape = _state_shape(
                    lanes,
                    next_lane_index=int(
                        kwargs.get("next_lane_index") or 0
                    ),
                    reserved_lane_indices=(
                        kwargs.get("reserved_lane_indices") or ()
                    ),
                    recorded_result_count=int(
                        kwargs.get("recorded_result_count") or 0
                    ),
                )
                with _LOCK:
                    loaded_shape = _LOADED_STATE_SHAPES.pop(
                        _path_key(path),
                        None,
                    )
                if loaded_shape is not None and loaded_shape == current_shape:
                    _increment_diagnostic(
                        "initial_state_rewrites_skipped"
                    )
                    return None
            return _ORIGINAL_WRITE_CAMPAIGN_STATE(*args, **kwargs)

        def format_barrier(*args: Any, **kwargs: Any) -> str:
            rendered = _ORIGINAL_FORMAT_BARRIER(*args, **kwargs)
            diagnostics = startup_diagnostics()
            detail = (
                "resume startup "
                f"journal={float(diagnostics['journal_load_seconds']):.1f}s "
                f"state={float(diagnostics['campaign_state_load_seconds']):.1f}s "
                f"terminal-hash-skips="
                f"{diagnostics['terminal_record_hashes_deferred']} "
                f"active-profiles={diagnostics['active_profiles_hydrated']} "
                f"terminal-profiles-skipped="
                f"{diagnostics['terminal_profiles_skipped']} "
                f"policy-reads-avoided="
                f"{diagnostics['policy_task_payload_reads_avoided']} "
                f"state-rewrite-skips="
                f"{diagnostics['initial_state_rewrites_skipped']}"
            )
            lines = rendered.splitlines()
            if lines:
                lines.insert(-1, play_hand_lab._box_row(detail))
            return "\n".join(lines)

        deep_memory._decode_json_record = decode_json_record
        DurableExecutionJournal._load_from_disk = load_from_disk
        play_hand_lab._load_campaign_state = load_campaign_state
        play_hand_lab._hydrate_unresolved_lane_task_specs = (
            hydrate_unresolved_lane_task_specs
        )
        play_hand_lab._recompute_campaign_policy_state_from_durable_lanes = (
            recompute_policy_state
        )
        play_hand_lab._write_campaign_state = write_campaign_state
        play_hand_lab._format_lab_barrier_snapshot = format_barrier
        _INSTALLED = True


__all__ = [
    "install_play_hand_startup_bounds",
    "startup_diagnostics",
]
