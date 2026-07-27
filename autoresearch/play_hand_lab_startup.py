from __future__ import annotations

import json
import os
import threading
import time
import zlib
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Iterable, Mapping

from .durable_execution import (
    JOURNAL_SCHEMA,
    DurableExecutionError,
    DurableExecutionJournal,
    _record_sha256,
)

try:  # pragma: no cover - exercised when optional C extension is installed.
    import orjson as _orjson
except Exception:  # pragma: no cover - stdlib fallback for unusual environments.
    _orjson = None


_RESUME_CACHE_SCHEMA = "play-hand-journal-resume-cache-v1"
_RESUME_CACHE_SUFFIX = ".resume-cache.zlib"
_CACHE_WRITE_INTERVAL_SECONDS = 60.0

_LOCK = threading.RLock()
_INSTALLED = False

_ORIGINAL_JOURNAL_LOAD_FROM_DISK: Any = None
_ORIGINAL_JOURNAL_APPLY_BATCH: Any = None
_ORIGINAL_LOAD_CAMPAIGN_STATE: Any = None
_ORIGINAL_LANE_STATE_FROM_PAYLOAD: Any = None
_ORIGINAL_HYDRATE_UNRESOLVED_TASK_SPECS: Any = None
_ORIGINAL_RECOMPUTE_POLICY_STATE: Any = None
_ORIGINAL_WRITE_CAMPAIGN_STATE: Any = None
_ORIGINAL_FORMAT_BARRIER: Any = None

_CACHE_LAST_WRITE_AT: dict[str, float] = {}
_LOADED_STATE_SHAPES: dict[str, tuple[Any, ...]] = {}

_DIAGNOSTICS: dict[str, Any] = {
    "journal_load_seconds": 0.0,
    "journal_cache_status": "unused",
    "journal_cache_tail_records": 0,
    "journal_cache_writes": 0,
    "journal_cache_invalid": 0,
    "campaign_state_load_seconds": 0.0,
    "active_profiles_hydrated": 0,
    "terminal_profiles_skipped": 0,
    "terminal_lanes_skipped_during_task_hydration": 0,
    "policy_task_payload_reads_avoided": 0,
    "policy_metadata_reads_avoided": 0,
    "initial_state_rewrites_skipped": 0,
}


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


def _json_dumps(payload: Any) -> bytes:
    if _orjson is not None:
        return _orjson.dumps(payload)
    return json.dumps(payload, ensure_ascii=True, separators=(",", ":")).encode("utf-8")


def _is_play_hand_journal(journal: DurableExecutionJournal) -> bool:
    return Path(journal.path).name == "play-hand-lab-execution-journal.json"


def _resume_cache_path(journal: DurableExecutionJournal) -> Path:
    return Path(str(journal.path) + _RESUME_CACHE_SUFFIX)


def _path_key(path: Path | str) -> str:
    return str(Path(path).resolve(strict=False))


def _atomic_write_transient(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(
        f"{path.name}.{os.getpid()}.{threading.get_ident()}.tmp"
    )
    try:
        with temporary.open("wb") as handle:
            handle.write(payload)
            handle.flush()
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def _journal_header(journal: DurableExecutionJournal) -> dict[str, Any]:
    path = Path(journal.path)
    try:
        with path.open("rb") as handle:
            raw = handle.readline()
    except OSError as exc:
        raise DurableExecutionError(f"execution journal is unreadable: {path}") from exc
    try:
        header = _json_loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise DurableExecutionError(f"execution journal is unreadable: {path}") from exc
    if (
        not isinstance(header, dict)
        or header.get("schema_version") != JOURNAL_SCHEMA
        or header.get("record_type") != "header"
        or header.get("header_sha256") != _record_sha256(header)
        or header.get("execution_id") != journal.execution_id
        or header.get("lineage") != journal.lineage
    ):
        raise DurableExecutionError("execution journal header identity mismatch")
    return header


def _read_record_at_offset(path: Path, offset: int) -> dict[str, Any]:
    try:
        with Path(path).open("rb") as handle:
            handle.seek(int(offset))
            raw = handle.readline()
    except OSError as exc:
        raise DurableExecutionError(f"execution journal is unreadable: {path}") from exc
    if not raw:
        raise DurableExecutionError(f"execution journal record offset is invalid: {path}")
    try:
        record = _json_loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise DurableExecutionError(f"execution journal is unreadable: {path}") from exc
    if not isinstance(record, dict) or record.get("record_sha256") != _record_sha256(record):
        raise DurableExecutionError("execution journal record identity mismatch")
    return record


def _restore_register_payload(
    journal: DurableExecutionJournal,
    task_id: str,
) -> dict[str, Any]:
    offsets = getattr(journal, "_play_hand_register_offsets", None)
    offset = offsets.get(str(task_id)) if isinstance(offsets, dict) else None
    if offset is None:
        raise DurableExecutionError(f"durable task payload is missing: {task_id}")
    record = _read_record_at_offset(Path(journal.path), int(offset))
    payload = record.get("payload")
    if (
        record.get("record_type") != "register"
        or str(record.get("task_id") or "") != str(task_id)
        or not isinstance(payload, dict)
    ):
        raise DurableExecutionError(f"durable task payload conflicts: {task_id}")
    return payload


def _replay_journal_tail(
    journal: DurableExecutionJournal,
    *,
    start_offset: int,
) -> int:
    path = Path(journal.path)
    tasks = getattr(journal, "_tasks", None)
    register_offsets = getattr(journal, "_play_hand_register_offsets", None)
    complete_offsets = getattr(journal, "_play_hand_complete_offsets", None)
    compacted_ids = getattr(journal, "_play_hand_compacted_terminal_ids", None)
    if not isinstance(tasks, dict):
        raise DurableExecutionError("execution journal cache is not loaded")
    if not isinstance(register_offsets, dict):
        register_offsets = {}
        setattr(journal, "_play_hand_register_offsets", register_offsets)
    if not isinstance(complete_offsets, dict):
        complete_offsets = {}
        setattr(journal, "_play_hand_complete_offsets", complete_offsets)
    if not isinstance(compacted_ids, set):
        compacted_ids = set()
        setattr(journal, "_play_hand_compacted_terminal_ids", compacted_ids)

    try:
        handle = path.open("rb")
    except OSError as exc:
        raise DurableExecutionError(f"execution journal is unreadable: {path}") from exc

    record_count = 0
    with handle:
        if start_offset > 0:
            handle.seek(start_offset - 1)
            if handle.read(1) != b"\n":
                raise DurableExecutionError("execution journal cache offset is not line aligned")
        handle.seek(start_offset)
        while True:
            offset = handle.tell()
            raw = handle.readline()
            if not raw:
                break
            if not raw.strip():
                continue
            try:
                record = _json_loads(raw)
            except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
                raise DurableExecutionError(f"execution journal is unreadable: {path}") from exc
            if not isinstance(record, dict):
                raise DurableExecutionError("execution journal record is malformed")
            if record.get("record_type") == "header":
                raise DurableExecutionError("execution journal has duplicate header")
            if record.get("record_sha256") != _record_sha256(record):
                raise DurableExecutionError("execution journal record identity mismatch")
            record_count += 1

            record_type = record.get("record_type")
            task_id = str(record.get("task_id") or "")
            if record_type == "register":
                payload = record.get("payload")
                payload_sha256 = record.get("payload_sha256")
                if not task_id or not isinstance(payload, dict) or not isinstance(payload_sha256, str):
                    raise DurableExecutionError("execution journal register record is malformed")
                existing = tasks.get(task_id)
                if isinstance(existing, dict):
                    if existing.get("payload_sha256") != payload_sha256:
                        raise DurableExecutionError(
                            f"task payload conflicts with durable graph: {task_id}"
                        )
                    if existing.get("status") != "terminal" and not isinstance(
                        existing.get("payload"), dict
                    ):
                        existing["payload"] = payload
                else:
                    tasks[task_id] = {
                        "task_id": task_id,
                        "payload_sha256": payload_sha256,
                        "payload": payload,
                        "status": "pending",
                        "terminal_receipt": None,
                    }
                register_offsets.setdefault(task_id, int(offset))
                continue

            task = tasks.get(task_id)
            if not isinstance(task, dict):
                raise DurableExecutionError(
                    f"journal record references unknown task: {task_id}"
                )

            if record_type == "complete":
                terminal_payload = record.get("payload")
                receipt_sha256 = record.get("receipt_sha256")
                if not isinstance(terminal_payload, dict) or not isinstance(receipt_sha256, str):
                    raise DurableExecutionError("execution journal complete record is malformed")
                if task.get("status") == "terminal":
                    existing = task.get("terminal_receipt")
                    if (
                        not isinstance(existing, dict)
                        or existing.get("receipt_sha256") != receipt_sha256
                    ):
                        raise DurableExecutionError(
                            f"conflicting duplicate terminal receipt: {task_id}"
                        )
                else:
                    task.pop("payload", None)
                    task["status"] = "terminal"
                    task["terminal_receipt"] = {"receipt_sha256": receipt_sha256}
                complete_offsets[task_id] = int(offset)
                compacted_ids.add(task_id)
                continue

            if record_type == "revoke":
                if task.get("status") == "terminal" or not isinstance(task.get("payload"), dict):
                    task["payload"] = _restore_register_payload(journal, task_id)
                task["status"] = "pending"
                task["terminal_receipt"] = None
                complete_offsets.pop(task_id, None)
                compacted_ids.discard(task_id)
                continue

            raise DurableExecutionError(
                f"execution journal has unknown record_type: {record_type!r}"
            )
    return record_count


def _cache_payload(journal: DurableExecutionJournal) -> dict[str, Any]:
    tasks = getattr(journal, "_tasks", None)
    register_offsets = getattr(journal, "_play_hand_register_offsets", None)
    complete_offsets = getattr(journal, "_play_hand_complete_offsets", None)
    compacted_ids = getattr(journal, "_play_hand_compacted_terminal_ids", None)
    if not isinstance(tasks, dict):
        raise DurableExecutionError("execution journal cache is not loaded")
    stat = Path(journal.path).stat()
    return {
        "schema_version": _RESUME_CACHE_SCHEMA,
        "execution_id": journal.execution_id,
        "lineage": journal.lineage,
        "header_sha256": journal._header_sha256,
        "journal_size": int(stat.st_size),
        "journal_mtime_ns": int(stat.st_mtime_ns),
        "tasks": tasks,
        "register_offsets": register_offsets if isinstance(register_offsets, dict) else {},
        "complete_offsets": complete_offsets if isinstance(complete_offsets, dict) else {},
        "compacted_terminal_ids": sorted(compacted_ids) if isinstance(compacted_ids, set) else [],
    }


def _write_resume_cache(
    journal: DurableExecutionJournal,
    *,
    force: bool = False,
) -> None:
    if not _is_play_hand_journal(journal):
        return
    key = _path_key(journal.path)
    now = time.monotonic()
    with _LOCK:
        last_write = _CACHE_LAST_WRITE_AT.get(key)
    if not force and last_write is not None and (now - last_write) < _CACHE_WRITE_INTERVAL_SECONDS:
        return
    try:
        encoded = _json_dumps(_cache_payload(journal))
        packed = zlib.compress(encoded, level=1)
        _atomic_write_transient(_resume_cache_path(journal), packed)
    except Exception:
        return
    with _LOCK:
        _CACHE_LAST_WRITE_AT[key] = time.monotonic()
        _DIAGNOSTICS["journal_cache_writes"] = int(
            _DIAGNOSTICS.get("journal_cache_writes", 0)
        ) + 1


def _load_resume_cache(journal: DurableExecutionJournal) -> dict[str, Any] | None:
    path = _resume_cache_path(journal)
    if not path.is_file():
        return None
    try:
        payload = _json_loads(zlib.decompress(path.read_bytes()))
    except Exception:
        return None
    if not isinstance(payload, dict):
        return None
    if (
        payload.get("schema_version") != _RESUME_CACHE_SCHEMA
        or payload.get("execution_id") != journal.execution_id
        or payload.get("lineage") != journal.lineage
    ):
        return None
    return payload


def _restore_cached_journal(
    journal: DurableExecutionJournal,
    cache: Mapping[str, Any],
) -> int:
    path = Path(journal.path)
    stat = path.stat()
    cached_size = int(cache.get("journal_size") or -1)
    cached_mtime_ns = int(cache.get("journal_mtime_ns") or -1)
    if cached_size < 0 or cached_size > stat.st_size:
        raise DurableExecutionError("execution journal resume cache size mismatch")
    if cached_size == stat.st_size and cached_mtime_ns != int(stat.st_mtime_ns):
        raise DurableExecutionError("execution journal resume cache timestamp mismatch")

    header = _journal_header(journal)
    if cache.get("header_sha256") != header.get("header_sha256"):
        raise DurableExecutionError("execution journal resume cache header mismatch")

    raw_tasks = cache.get("tasks")
    raw_register_offsets = cache.get("register_offsets")
    raw_complete_offsets = cache.get("complete_offsets")
    if not isinstance(raw_tasks, dict) or not isinstance(raw_register_offsets, dict):
        raise DurableExecutionError("execution journal resume cache is malformed")

    tasks: dict[str, dict[str, Any]] = {}
    for raw_task_id, raw_task in raw_tasks.items():
        task_id = str(raw_task_id)
        if not isinstance(raw_task, dict) or str(raw_task.get("task_id") or "") != task_id:
            raise DurableExecutionError("execution journal resume cache task is malformed")
        status = str(raw_task.get("status") or "")
        payload_sha256 = raw_task.get("payload_sha256")
        if status not in {"pending", "terminal"} or not isinstance(payload_sha256, str):
            raise DurableExecutionError("execution journal resume cache task is malformed")
        if status == "pending" and not isinstance(raw_task.get("payload"), dict):
            raise DurableExecutionError("execution journal resume cache pending task has no payload")
        if status == "terminal":
            receipt = raw_task.get("terminal_receipt")
            if not isinstance(receipt, dict) or not isinstance(receipt.get("receipt_sha256"), str):
                raise DurableExecutionError("execution journal resume cache terminal task is malformed")
            raw_task = dict(raw_task)
            raw_task.pop("payload", None)
            raw_task["terminal_receipt"] = {
                "receipt_sha256": receipt["receipt_sha256"]
            }
        tasks[task_id] = dict(raw_task)

    journal._header_sha256 = str(header["header_sha256"])
    journal._tasks = tasks
    setattr(
        journal,
        "_play_hand_register_offsets",
        {str(key): int(value) for key, value in raw_register_offsets.items()},
    )
    setattr(
        journal,
        "_play_hand_complete_offsets",
        {
            str(key): int(value)
            for key, value in (raw_complete_offsets.items() if isinstance(raw_complete_offsets, dict) else ())
        },
    )
    compacted = cache.get("compacted_terminal_ids")
    setattr(
        journal,
        "_play_hand_compacted_terminal_ids",
        {str(item) for item in compacted} if isinstance(compacted, list) else set(),
    )
    setattr(journal, "_play_hand_memory_compaction_enabled", True)

    return _replay_journal_tail(journal, start_offset=cached_size)


def _fast_read_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = _json_loads(path.read_bytes())
    except (OSError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise DurableExecutionError(f"JSON state is unreadable: {path}") from exc
    if not isinstance(payload, dict):
        raise DurableExecutionError(f"JSON state must be an object: {path}")
    return payload


def _lane_state_from_payload(play_hand_lab: Any, payload: dict[str, Any]) -> Any:
    values = dict(payload)
    values["run_dir"] = Path(str(values["run_dir"])).resolve(strict=False)
    for key in ("profile_path", "incumbent_profile_path"):
        values[key] = (
            Path(str(values[key])).resolve(strict=False) if values.get(key) else None
        )
    for key in ("completed_task_ids", "failed_task_ids"):
        values[key] = {str(item) for item in values.get(key) or []}
    return play_hand_lab.LabLaneState(**values)


def _hydrate_lane_profiles(play_hand_lab: Any, lane: Any) -> None:
    if lane.profile_payload is None and lane.profile_path is not None and lane.profile_path.is_file():
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
) -> tuple[Any, ...]:
    lane_list = list(lanes)
    return (
        int(next_lane_index),
        tuple(sorted(int(item) for item in reserved_lane_indices)),
        len(lane_list),
        sum(len(getattr(lane, "task_ids", ()) or ()) for lane in lane_list),
        sum(len(getattr(lane, "completed_task_ids", ()) or ()) for lane in lane_list),
        sum(len(getattr(lane, "failed_task_ids", ()) or ()) for lane in lane_list),
        sum(1 for lane in lane_list if bool(getattr(lane, "terminal", False))),
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
    if payload.get("lineage") != play_hand_lab._campaign_state_lineage(runtime, campaign_id):
        raise DurableExecutionError("PlayHand durable state lineage mismatch")

    lanes = [
        _lane_state_from_payload(play_hand_lab, dict(item))
        for item in payload.get("lanes") or []
        if isinstance(item, dict)
    ]
    active_lanes = [lane for lane in lanes if not bool(lane.terminal)]
    worker_count = min(
        len(active_lanes),
        max(1, min(8, os.cpu_count() or 4)),
    )
    if worker_count > 1:
        with ThreadPoolExecutor(max_workers=worker_count) as executor:
            list(executor.map(lambda lane: _hydrate_lane_profiles(play_hand_lab, lane), active_lanes))
    else:
        for lane in active_lanes:
            _hydrate_lane_profiles(play_hand_lab, lane)

    history = play_hand_lab.LabCampaignHistory(**dict(payload.get("history") or {}))
    policy_state = payload.get("campaign_policy_state")
    if policy_state is not None:
        if not isinstance(policy_state, dict):
            raise DurableExecutionError("PlayHand durable campaign policy state is invalid")
        history.campaign_policy_state = dict(policy_state)

    next_lane_index = int(payload.get("next_lane_index") or 0)
    reserved_lane_indices = sorted(
        {int(item) for item in payload.get("reserved_lane_indices") or []}
    )
    with _LOCK:
        _LOADED_STATE_SHAPES[_path_key(path)] = _state_shape(
            lanes,
            next_lane_index=next_lane_index,
            reserved_lane_indices=reserved_lane_indices,
        )
        _DIAGNOSTICS["campaign_state_load_seconds"] = round(
            time.monotonic() - started,
            3,
        )
        _DIAGNOSTICS["active_profiles_hydrated"] = len(active_lanes)
        _DIAGNOSTICS["terminal_profiles_skipped"] = len(lanes) - len(active_lanes)
    return (
        lanes,
        history,
        next_lane_index,
        reserved_lane_indices,
        int(payload.get("recorded_result_count") or 0),
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
    del durable_tasks_by_id
    if pruned_lane_count:
        raise DurableExecutionError(
            "policy-honest resume cannot verify pruned lane assignments"
        )
    # Identity fields are immutable. Replace only the accounting maps we rebuild;
    # deep-copying the complete finite lane plan and manifest on every resume is waste.
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

    lanes_by_task: dict[str, Any] = {}
    seen_lane_indices: set[int] = set()
    total_task_ids = 0
    for lane in sorted(lanes, key=lambda item: item.lane_index):
        if lane.lane_index in seen_lane_indices:
            raise DurableExecutionError(
                f"duplicate durable policy lane index: {lane.lane_index}"
            )
        seen_lane_indices.add(lane.lane_index)
        expected_lane = play_hand_lab._policy_lane_for_index(rebuilt, lane.lane_index)
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
        allocation = assignment.get("allocation")
        execution = rebuilt.get("execution")
        expected_allocation = {
            "lane_index": lane.lane_index,
            "planned_lane_count": planned.get(expected_lane),
            "algorithm": (execution or {}).get("allocation_algorithm"),
            "algorithm_version": (execution or {}).get("allocation_algorithm_version"),
            "lane_tie_break_order": (execution or {}).get("lane_tie_break_order"),
            "candidate_tie_break_order": (execution or {}).get(
                "candidate_tie_break_order"
            ),
        }
        if not isinstance(execution, dict) or allocation != expected_allocation:
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
            if not isinstance(attributes, dict) or not isinstance(cap_decision, dict):
                raise DurableExecutionError(
                    f"durable selected policy lane has incomplete accounting: {lane.lane_id}"
                )
            recomputed_cap_decision = play_hand_lab._policy_cap_decision(
                rebuilt,
                attributes,
            )
            if (
                recomputed_cap_decision.get("outcome") != "accepted"
                or cap_decision != recomputed_cap_decision
            ):
                raise DurableExecutionError(
                    f"durable policy cap decision mismatch: {lane.lane_id}"
                )
            play_hand_lab._record_policy_assignment(
                rebuilt,
                lane=expected_lane,
                cap_decision=recomputed_cap_decision,
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
            total_task_ids += 1
            if task_key in lanes_by_task:
                raise DurableExecutionError(
                    f"duplicate durable policy task id: {task_key}"
                )
            lanes_by_task[task_key] = lane

    for task in unresolved_tasks:
        task_id = str(task.get("task_id") or "")
        lane = lanes_by_task.get(task_id)
        if (
            lane is None
            or play_hand_lab._durable_task_policy_assignment(task)
            != lane.policy_assignment
        ):
            raise DurableExecutionError(
                f"durable journal task policy assignment mismatch: {task_id or '<missing>'}"
            )

    mutable_fields = (
        "assigned_lane_counts",
        "used_lane_counts",
        "exhausted_lane_counts",
        "accounting",
        "exhaustion_outcomes",
    )
    if any(policy_state.get(field) != rebuilt.get(field) for field in mutable_fields):
        raise DurableExecutionError(
            "durable campaign policy counters do not match persisted lane assignments"
        )
    with _LOCK:
        _DIAGNOSTICS["policy_task_payload_reads_avoided"] = max(
            total_task_ids - len(unresolved_tasks),
            0,
        )
        _DIAGNOSTICS["policy_metadata_reads_avoided"] = len(lanes)
    return rebuilt


def install_play_hand_startup_bounds() -> None:
    """Install one default, bounded resume path for large formal campaigns."""

    global _INSTALLED
    global _ORIGINAL_JOURNAL_LOAD_FROM_DISK
    global _ORIGINAL_JOURNAL_APPLY_BATCH
    global _ORIGINAL_LOAD_CAMPAIGN_STATE
    global _ORIGINAL_LANE_STATE_FROM_PAYLOAD
    global _ORIGINAL_HYDRATE_UNRESOLVED_TASK_SPECS
    global _ORIGINAL_RECOMPUTE_POLICY_STATE
    global _ORIGINAL_WRITE_CAMPAIGN_STATE
    global _ORIGINAL_FORMAT_BARRIER

    with _LOCK:
        if _INSTALLED:
            return

        from . import play_hand_lab

        _ORIGINAL_JOURNAL_LOAD_FROM_DISK = DurableExecutionJournal._load_from_disk
        _ORIGINAL_JOURNAL_APPLY_BATCH = DurableExecutionJournal.apply_batch
        _ORIGINAL_LOAD_CAMPAIGN_STATE = play_hand_lab._load_campaign_state
        _ORIGINAL_LANE_STATE_FROM_PAYLOAD = play_hand_lab._lane_state_from_payload
        _ORIGINAL_HYDRATE_UNRESOLVED_TASK_SPECS = (
            play_hand_lab._hydrate_unresolved_lane_task_specs
        )
        _ORIGINAL_RECOMPUTE_POLICY_STATE = (
            play_hand_lab._recompute_campaign_policy_state_from_durable_lanes
        )
        _ORIGINAL_WRITE_CAMPAIGN_STATE = play_hand_lab._write_campaign_state
        _ORIGINAL_FORMAT_BARRIER = play_hand_lab._format_lab_barrier_snapshot

        def load_from_disk(journal: DurableExecutionJournal) -> dict[str, Any]:
            if not _is_play_hand_journal(journal):
                return _ORIGINAL_JOURNAL_LOAD_FROM_DISK(journal)
            started = time.monotonic()
            cache = _load_resume_cache(journal)
            if cache is not None:
                try:
                    tail_records = _restore_cached_journal(journal, cache)
                    _set_diagnostic("journal_cache_status", "hit")
                    _set_diagnostic("journal_cache_tail_records", tail_records)
                    _set_diagnostic(
                        "journal_load_seconds",
                        round(time.monotonic() - started, 3),
                    )
                    if tail_records:
                        _write_resume_cache(journal, force=True)
                    return journal._view()
                except Exception:
                    _increment_diagnostic("journal_cache_invalid")
            result = _ORIGINAL_JOURNAL_LOAD_FROM_DISK(journal)
            _set_diagnostic("journal_cache_status", "rebuilt")
            _set_diagnostic("journal_cache_tail_records", 0)
            _set_diagnostic(
                "journal_load_seconds",
                round(time.monotonic() - started, 3),
            )
            _write_resume_cache(journal, force=True)
            return result

        def apply_batch(
            journal: DurableExecutionJournal,
            *,
            registrations: Iterable[tuple[str, Mapping[str, Any]]] = (),
            completions: Iterable[tuple[str, Mapping[str, Any]]] = (),
            revocations: Iterable[str] = (),
        ) -> dict[str, Any]:
            registration_rows = list(registrations)
            completion_rows = list(completions)
            revocation_rows = [str(task_id) for task_id in revocations]
            try:
                before_size = Path(journal.path).stat().st_size
            except OSError:
                before_size = 0
            result = _ORIGINAL_JOURNAL_APPLY_BATCH(
                journal,
                registrations=registration_rows,
                completions=completion_rows,
                revocations=revocation_rows,
            )
            if _is_play_hand_journal(journal):
                try:
                    after_size = Path(journal.path).stat().st_size
                except OSError:
                    after_size = before_size
                if after_size > before_size:
                    _replay_journal_tail(journal, start_offset=before_size)
                _write_resume_cache(journal)
            return result

        def lane_state_from_payload(payload: dict[str, Any]) -> Any:
            return _lane_state_from_payload(play_hand_lab, payload)

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
            active_lanes = [lane for lane in lanes if not bool(lane.terminal)]
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
                key = _path_key(path)
                current_shape = _state_shape(
                    lanes,
                    next_lane_index=int(kwargs.get("next_lane_index") or 0),
                    reserved_lane_indices=kwargs.get("reserved_lane_indices") or (),
                )
                with _LOCK:
                    loaded_shape = _LOADED_STATE_SHAPES.pop(key, None)
                if loaded_shape == current_shape:
                    _increment_diagnostic("initial_state_rewrites_skipped")
                    return None
            return _ORIGINAL_WRITE_CAMPAIGN_STATE(*args, **kwargs)

        def format_barrier(*args: Any, **kwargs: Any) -> str:
            rendered = _ORIGINAL_FORMAT_BARRIER(*args, **kwargs)
            diagnostics = startup_diagnostics()
            detail = (
                "resume startup "
                f"journal={float(diagnostics['journal_load_seconds']):.1f}s "
                f"cache={diagnostics['journal_cache_status']} "
                f"tail={diagnostics['journal_cache_tail_records']} "
                f"state={float(diagnostics['campaign_state_load_seconds']):.1f}s "
                f"profiles={diagnostics['active_profiles_hydrated']}/"
                f"-{diagnostics['terminal_profiles_skipped']} "
                f"policy-reads-avoided={diagnostics['policy_task_payload_reads_avoided']} "
                f"state-rewrite-skips={diagnostics['initial_state_rewrites_skipped']}"
            )
            lines = rendered.splitlines()
            if lines:
                lines.insert(-1, play_hand_lab._box_row(detail))
            return "\n".join(lines)

        DurableExecutionJournal._load_from_disk = load_from_disk
        DurableExecutionJournal.apply_batch = apply_batch
        play_hand_lab._lane_state_from_payload = lane_state_from_payload
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
