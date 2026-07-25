from __future__ import annotations

import copy
import itertools
import json
import threading
import weakref
from dataclasses import fields
from pathlib import Path
from typing import Any, Iterable, Mapping
from weakref import WeakValueDictionary

from . import durable_execution as _durable_execution
from .durable_execution import DurableExecutionError, DurableExecutionJournal


PLAY_HAND_JOURNAL_FILENAME = "play-hand-lab-execution-journal.json"

_LOCK = threading.RLock()
_INSTALLED = False
_TASK_LANES: WeakValueDictionary[str, Any] = WeakValueDictionary()

_ORIGINAL_JOURNAL_LOAD_FROM_DISK: Any = None
_ORIGINAL_JOURNAL_APPLY_BATCH: Any = None
_ORIGINAL_MEMORY_COMPACT_TERMINAL_TASK: Any = None
_ORIGINAL_MEMORY_READ_TASK_RECORD_FROM_DISK: Any = None
_ORIGINAL_REGISTER_TASK_SPEC: Any = None
_ORIGINAL_RECOVER_UNRESOLVED_TASK_GRAPH: Any = None
_ORIGINAL_LOAD_CAMPAIGN_STATE: Any = None
_ORIGINAL_LANE_STATE_PAYLOAD: Any = None
_ORIGINAL_MERGE_SWEEP_PARENT_RECEIPTS: Any = None
_ORIGINAL_ADD_RECORDED_RESULT_SAMPLE: Any = None

_LANE_STATE_OMITTED_FIELDS = frozenset(
    {
        "profile_payload",
        "incumbent_profile_payload",
        "task_specs",
    }
)
_LANE_STATE_PATH_FIELDS = frozenset(
    {
        "run_dir",
        "profile_path",
        "incumbent_profile_path",
    }
)
_LANE_STATE_SET_FIELDS = frozenset(
    {
        "completed_task_ids",
        "failed_task_ids",
    }
)
_COMPLETED_SWEEP_SPEC_KEYS = (
    "phase",
    "task_kind",
    "sweep_id",
    "shard_id",
    "axes",
    "axis_key_map",
    "axis_plan",
    "expanded_permutation_count",
    "permutation_start",
    "permutation_count",
    "params_by_index_sha256",
)
_RECORDED_RESULT_SAMPLE_KEYS = (
    "task_id",
    "attempt_id",
    "artifact_dir",
    "score",
    "score_basis",
    "status",
    "phase",
    "task_kind",
    "profile_path",
    "profile_ref",
    "instruments",
    "timeframe",
    "lookback_months",
    "analysis_window_start",
    "analysis_window_end",
    "evidence_plan_id",
    "evidence_role",
)


def _is_play_hand_journal(journal: DurableExecutionJournal) -> bool:
    return Path(journal.path).name == PLAY_HAND_JOURNAL_FILENAME


def _journal_for_lazy_task(task: "_LazyTerminalTask") -> DurableExecutionJournal:
    journal = task._journal_ref()
    if journal is None:
        raise DurableExecutionError(
            f"execution journal is unavailable for compacted task {task.task_id}"
        )
    return journal


class _LazyTerminalReceipt(dict[str, Any]):
    """Hash-only receipt that hydrates its parent task only when payload is requested."""

    def __init__(
        self,
        parent: "_LazyTerminalTask",
        receipt_sha256: str | None,
    ) -> None:
        super().__init__({"receipt_sha256": receipt_sha256})
        self._parent = parent

    def _payload(self, default: Any = None) -> Any:
        self._parent._inflate()
        receipt = dict.get(self._parent, "terminal_receipt")
        if isinstance(receipt, dict):
            return dict.get(receipt, "payload", default)
        return default

    def get(self, key: str, default: Any = None) -> Any:
        if key == "payload":
            return self._payload(default)
        return dict.get(self, key, default)

    def __getitem__(self, key: str) -> Any:
        if key == "payload":
            value = self._payload()
            if value is None:
                raise KeyError(key)
            return value
        return dict.__getitem__(self, key)


class _LazyTerminalTask(dict[str, Any]):
    """Compact terminal journal record with verified on-disk lazy hydration."""

    def __init__(
        self,
        journal: DurableExecutionJournal,
        *,
        task_id: str,
        payload_sha256: str | None,
        receipt_sha256: str | None,
    ) -> None:
        super().__init__(
            {
                "task_id": str(task_id),
                "payload_sha256": payload_sha256,
                "status": "terminal",
                "terminal_receipt": None,
            }
        )
        self.task_id = str(task_id)
        self._journal_ref = weakref.ref(journal)
        dict.__setitem__(
            self,
            "terminal_receipt",
            _LazyTerminalReceipt(self, receipt_sha256),
        )

    @classmethod
    def from_record(
        cls,
        journal: DurableExecutionJournal,
        task_id: str,
        task: Mapping[str, Any],
    ) -> "_LazyTerminalTask":
        receipt = dict.get(task, "terminal_receipt") if isinstance(task, dict) else None
        receipt_sha256 = (
            dict.get(receipt, "receipt_sha256")
            if isinstance(receipt, dict)
            else None
        )
        return cls(
            journal,
            task_id=str(task_id),
            payload_sha256=task.get("payload_sha256"),
            receipt_sha256=receipt_sha256,
        )

    def _inflate(self) -> None:
        if dict.get(self, "payload") is not None:
            return
        with _LOCK:
            if dict.get(self, "payload") is not None:
                return
            journal = _journal_for_lazy_task(self)
            from . import play_hand_lab_memory as _memory

            restored = _memory._read_task_record_from_disk(journal, self.task_id)
            if not isinstance(restored, dict) or restored.get("status") != "terminal":
                raise DurableExecutionError(
                    f"compacted terminal task could not be restored: {self.task_id}"
                )
            self.clear()
            self.update(restored)
            _memory._compacted_terminal_ids(journal).discard(self.task_id)

    def _compact_again(self) -> None:
        journal = _journal_for_lazy_task(self)
        receipt = dict.get(self, "terminal_receipt")
        receipt_sha256 = (
            dict.get(receipt, "receipt_sha256")
            if isinstance(receipt, dict)
            else None
        )
        payload_sha256 = dict.get(self, "payload_sha256")
        self.clear()
        self.update(
            {
                "task_id": self.task_id,
                "payload_sha256": payload_sha256,
                "status": "terminal",
                "terminal_receipt": None,
            }
        )
        dict.__setitem__(
            self,
            "terminal_receipt",
            _LazyTerminalReceipt(self, receipt_sha256),
        )
        from . import play_hand_lab_memory as _memory

        _memory._compacted_terminal_ids(journal).add(self.task_id)

    def get(self, key: str, default: Any = None) -> Any:
        if key == "payload" and dict.get(self, "payload") is None:
            self._inflate()
        return dict.get(self, key, default)

    def __getitem__(self, key: str) -> Any:
        if key == "payload" and dict.get(self, "payload") is None:
            self._inflate()
        return dict.__getitem__(self, key)


def _update_task_record_index(
    index: dict[str, dict[str, Any]],
    *,
    offset: int,
    record: Mapping[str, Any],
) -> None:
    record_type = str(record.get("record_type") or "")
    if record_type not in {"register", "complete", "revoke"}:
        return
    task_id = str(record.get("task_id") or "")
    if not task_id:
        return
    entry = index.setdefault(
        task_id,
        {
            "register_offset": None,
            "terminal_offset": None,
            "status": "pending",
        },
    )
    if record_type == "register":
        if entry.get("register_offset") is None:
            entry["register_offset"] = int(offset)
    elif record_type == "complete":
        entry["terminal_offset"] = int(offset)
        entry["status"] = "terminal"
    elif record_type == "revoke":
        entry["terminal_offset"] = None
        entry["status"] = "pending"


def _iter_binary_lines_with_offsets(handle: Any) -> Iterable[tuple[int, bytes]]:
    while True:
        offset = int(handle.tell())
        raw = handle.readline()
        if not raw:
            return
        yield offset, raw


def _stream_replay_play_hand_journal(
    journal: DurableExecutionJournal,
    lines: Iterable[tuple[int, bytes]],
) -> dict[str, Any]:
    """Replay a PlayHand JSONL journal while retaining only its current live payloads."""

    tasks: dict[str, Any] = {}
    header: dict[str, Any] | None = None
    revoked_pending: set[str] = set()
    compacted_terminal_ids: set[str] = set()
    record_index: dict[str, dict[str, Any]] = {}
    final_offset = 0

    for offset, raw in lines:
        final_offset = max(final_offset, int(offset) + len(raw))
        if not raw.strip():
            continue
        try:
            record = json.loads(raw)
        except json.JSONDecodeError as exc:
            raise DurableExecutionError(
                f"execution journal is unreadable: {journal.path}"
            ) from exc
        if not isinstance(record, dict):
            raise DurableExecutionError("execution journal record is malformed")

        record_type = record.get("record_type")
        if record_type == "header":
            if header is not None:
                raise DurableExecutionError("execution journal has duplicate header")
            if record.get("schema_version") != _durable_execution.JOURNAL_SCHEMA:
                raise DurableExecutionError("execution journal schema mismatch")
            expected = _durable_execution._record_sha256(record)
            if record.get("header_sha256") != expected:
                raise DurableExecutionError(
                    "execution journal header identity mismatch"
                )
            header = record
            continue

        if header is None:
            raise DurableExecutionError("execution journal is missing header")
        if record.get("record_sha256") != _durable_execution._record_sha256(record):
            raise DurableExecutionError("execution journal record identity mismatch")

        _update_task_record_index(record_index, offset=offset, record=record)

        if record_type == "register":
            task_key = str(record.get("task_id") or "")
            payload = record.get("payload")
            payload_sha256 = record.get("payload_sha256")
            if (
                not task_key
                or not isinstance(payload, dict)
                or not isinstance(payload_sha256, str)
            ):
                raise DurableExecutionError(
                    "execution journal register record is malformed"
                )
            existing = tasks.get(task_key)
            if existing is not None:
                if existing.get("payload_sha256") != payload_sha256:
                    raise DurableExecutionError(
                        f"task payload conflicts with durable graph: {task_key}"
                    )
                continue
            tasks[task_key] = {
                "task_id": task_key,
                "payload_sha256": payload_sha256,
                "payload": payload,
                "status": "pending",
                "terminal_receipt": None,
            }
            continue

        if record_type == "complete":
            task_key = str(record.get("task_id") or "")
            terminal_payload = record.get("payload")
            receipt_sha256 = record.get("receipt_sha256")
            task = tasks.get(task_key)
            if not isinstance(task, dict):
                raise DurableExecutionError(
                    f"terminal receipt references unknown task: {task_key}"
                )
            if (
                not isinstance(terminal_payload, dict)
                or not isinstance(receipt_sha256, str)
            ):
                raise DurableExecutionError(
                    "execution journal complete record is malformed"
                )
            if task.get("status") == "terminal":
                existing_receipt = dict.get(task, "terminal_receipt")
                if (
                    not isinstance(existing_receipt, dict)
                    or dict.get(existing_receipt, "receipt_sha256")
                    != receipt_sha256
                ):
                    raise DurableExecutionError(
                        f"conflicting duplicate terminal receipt: {task_key}"
                    )
                continue
            tasks[task_key] = _LazyTerminalTask(
                journal,
                task_id=task_key,
                payload_sha256=task.get("payload_sha256"),
                receipt_sha256=receipt_sha256,
            )
            compacted_terminal_ids.add(task_key)
            revoked_pending.discard(task_key)
            continue

        if record_type == "revoke":
            task_key = str(record.get("task_id") or "")
            task = tasks.get(task_key)
            if not isinstance(task, dict):
                raise DurableExecutionError(
                    f"revoke references unknown task: {task_key}"
                )
            if task.get("status") != "terminal":
                continue
            tasks[task_key] = {
                "task_id": task_key,
                "payload_sha256": task.get("payload_sha256"),
                "status": "pending",
                "terminal_receipt": None,
            }
            compacted_terminal_ids.discard(task_key)
            revoked_pending.add(task_key)
            continue

        raise DurableExecutionError(
            f"execution journal has unknown record_type: {record_type!r}"
        )

    if header is None:
        raise DurableExecutionError("execution journal is missing header")
    if (
        header.get("execution_id") != journal.execution_id
        or header.get("lineage") != journal.lineage
    ):
        raise DurableExecutionError("execution journal lineage mismatch")

    journal._header_sha256 = str(header["header_sha256"])
    journal._tasks = tasks
    setattr(journal, "_play_hand_task_record_index", record_index)
    setattr(journal, "_play_hand_task_record_index_end", final_offset)

    from . import play_hand_lab_memory as _memory

    compacted = _memory._compacted_terminal_ids(journal)
    compacted.clear()
    compacted.update(compacted_terminal_ids)

    # A final revoke leaves the task pending. Restore those rare records after the
    # streaming pass without keeping all historical terminal payloads resident.
    for task_id in revoked_pending:
        restored = _read_indexed_task_record_from_disk(journal, task_id)
        if not isinstance(restored, dict) or restored.get("status") != "pending":
            raise DurableExecutionError(
                f"revoked task could not be restored: {task_id}"
            )
        tasks[task_id] = restored

    return journal._view()


def _stream_play_hand_load_from_disk(
    journal: DurableExecutionJournal,
) -> dict[str, Any]:
    if not _is_play_hand_journal(journal):
        return _ORIGINAL_JOURNAL_LOAD_FROM_DISK(journal)

    try:
        handle = Path(journal.path).open("rb")
    except OSError as exc:
        raise DurableExecutionError(
            f"execution journal is unreadable: {journal.path}"
        ) from exc

    with handle:
        first_offset: int | None = None
        first_raw: bytes | None = None
        for offset, raw in _iter_binary_lines_with_offsets(handle):
            if raw.strip():
                first_offset = offset
                first_raw = raw
                break
        if first_raw is not None and first_offset is not None:
            try:
                first = json.loads(first_raw)
            except json.JSONDecodeError:
                first = None
            if (
                isinstance(first, dict)
                and first.get("schema_version")
                == _durable_execution.JOURNAL_SCHEMA
                and first.get("record_type") == "header"
            ):
                return _stream_replay_play_hand_journal(
                    journal,
                    itertools.chain(
                        ((first_offset, first_raw),),
                        _iter_binary_lines_with_offsets(handle),
                    ),
                )

    # Preserve the generic journal's exact retired-format diagnostics.
    return _ORIGINAL_JOURNAL_LOAD_FROM_DISK(journal)


def _read_record_at_offset(
    journal: DurableExecutionJournal,
    offset: int,
) -> dict[str, Any]:
    try:
        with Path(journal.path).open("rb") as handle:
            handle.seek(int(offset))
            raw = handle.readline()
    except OSError as exc:
        raise DurableExecutionError(
            f"execution journal is unreadable: {journal.path}"
        ) from exc
    try:
        record = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise DurableExecutionError(
            f"execution journal is unreadable: {journal.path}"
        ) from exc
    if (
        not isinstance(record, dict)
        or record.get("record_sha256")
        != _durable_execution._record_sha256(record)
    ):
        raise DurableExecutionError("execution journal record identity mismatch")
    return record


def _read_indexed_task_record_from_disk(
    journal: DurableExecutionJournal,
    task_id: str,
) -> dict[str, Any] | None:
    task_key = str(task_id)
    index = getattr(journal, "_play_hand_task_record_index", None)
    entry = index.get(task_key) if isinstance(index, dict) else None
    if not isinstance(entry, dict):
        return _ORIGINAL_MEMORY_READ_TASK_RECORD_FROM_DISK(journal, task_key)

    register_offset = entry.get("register_offset")
    if not isinstance(register_offset, int):
        return _ORIGINAL_MEMORY_READ_TASK_RECORD_FROM_DISK(journal, task_key)
    register = _read_record_at_offset(journal, register_offset)
    if (
        register.get("record_type") != "register"
        or str(register.get("task_id") or "") != task_key
        or not isinstance(register.get("payload"), dict)
    ):
        raise DurableExecutionError(
            f"indexed task registration conflicts for task {task_key}"
        )
    restored: dict[str, Any] = {
        "task_id": task_key,
        "payload_sha256": register.get("payload_sha256"),
        "payload": register["payload"],
        "status": "pending",
        "terminal_receipt": None,
    }

    if entry.get("status") == "terminal":
        terminal_offset = entry.get("terminal_offset")
        if not isinstance(terminal_offset, int):
            raise DurableExecutionError(
                f"indexed terminal receipt is missing for task {task_key}"
            )
        terminal = _read_record_at_offset(journal, terminal_offset)
        if (
            terminal.get("record_type") != "complete"
            or str(terminal.get("task_id") or "") != task_key
            or not isinstance(terminal.get("payload"), dict)
        ):
            raise DurableExecutionError(
                f"indexed terminal receipt conflicts for task {task_key}"
            )
        restored["status"] = "terminal"
        restored["terminal_receipt"] = {
            "receipt_sha256": terminal.get("receipt_sha256"),
            "payload": terminal["payload"],
        }
    return restored


def _refresh_task_record_index(journal: DurableExecutionJournal) -> None:
    index = getattr(journal, "_play_hand_task_record_index", None)
    if not isinstance(index, dict):
        index = {}
        setattr(journal, "_play_hand_task_record_index", index)
        setattr(journal, "_play_hand_task_record_index_end", 0)
    start = int(getattr(journal, "_play_hand_task_record_index_end", 0) or 0)
    try:
        handle = Path(journal.path).open("rb")
    except OSError as exc:
        raise DurableExecutionError(
            f"execution journal is unreadable: {journal.path}"
        ) from exc
    with handle:
        handle.seek(start)
        end = start
        for offset, raw in _iter_binary_lines_with_offsets(handle):
            end = int(offset) + len(raw)
            if not raw.strip():
                continue
            try:
                record = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise DurableExecutionError(
                    f"execution journal is unreadable: {journal.path}"
                ) from exc
            if not isinstance(record, dict):
                raise DurableExecutionError(
                    "execution journal record identity mismatch"
                )
            if record.get("record_type") == "header":
                if (
                    record.get("schema_version")
                    != _durable_execution.JOURNAL_SCHEMA
                    or record.get("header_sha256")
                    != _durable_execution._record_sha256(record)
                ):
                    raise DurableExecutionError(
                        "execution journal header identity mismatch"
                    )
                continue
            if record.get("record_sha256") != _durable_execution._record_sha256(
                record
            ):
                raise DurableExecutionError(
                    "execution journal record identity mismatch"
                )
            _update_task_record_index(index, offset=offset, record=record)
        setattr(journal, "_play_hand_task_record_index_end", end)


def _compact_terminal_task_without_lazy_inflation(
    journal: DurableExecutionJournal,
    task_id: str,
) -> bool:
    tasks = getattr(journal, "_tasks", None)
    task = tasks.get(str(task_id)) if isinstance(tasks, dict) else None
    if isinstance(task, _LazyTerminalTask):
        if dict.get(task, "payload") is not None:
            task._compact_again()
        else:
            from . import play_hand_lab_memory as _memory

            _memory._compacted_terminal_ids(journal).add(str(task_id))
        return True
    return _ORIGINAL_MEMORY_COMPACT_TERMINAL_TASK(journal, task_id)


def _make_lazy_terminal_record(
    journal: DurableExecutionJournal,
    task_id: str,
) -> None:
    tasks = getattr(journal, "_tasks", None)
    if not isinstance(tasks, dict):
        return
    task = tasks.get(str(task_id))
    if not isinstance(task, dict) or task.get("status") != "terminal":
        return
    if isinstance(task, _LazyTerminalTask):
        task._compact_again()
        return
    tasks[str(task_id)] = _LazyTerminalTask.from_record(
        journal,
        str(task_id),
        task,
    )
    from . import play_hand_lab_memory as _memory

    _memory._compacted_terminal_ids(journal).add(str(task_id))


def _remember_lane_task(lane: Any, task_id: str) -> None:
    task_key = str(task_id or "")
    if not task_key:
        return
    with _LOCK:
        _TASK_LANES[task_key] = lane


def _remember_lane_graph(lanes: Iterable[Any]) -> None:
    for lane in lanes:
        for task_id in getattr(lane, "task_ids", ()) or ():
            _remember_lane_task(lane, str(task_id))


def _phase_for_task(lane: Any, task_id: str, spec: Mapping[str, Any] | None) -> str:
    phase = str((spec or {}).get("phase") or "")
    if phase:
        return phase
    for candidate, task_ids in (getattr(lane, "phase_task_ids", {}) or {}).items():
        if task_id in (task_ids or []):
            return str(candidate)
    return ""


def _compact_policy_assignment(policy: Any) -> dict[str, Any] | None:
    if not isinstance(policy, dict):
        return None
    compact: dict[str, Any] = {}
    for key in (
        "policy_lane",
        "policy_manifest_sha256",
        "policy_outcome_type",
        "candidate_attributes",
        "cap_decision",
        "negative_prior_runtime",
    ):
        if key in policy:
            compact[key] = copy.deepcopy(policy[key])
    return compact or None


def _compact_recorded_result(recorded: Mapping[str, Any]) -> dict[str, Any]:
    compact = {
        key: copy.deepcopy(recorded[key])
        for key in _RECORDED_RESULT_SAMPLE_KEYS
        if key in recorded
    }
    policy = _compact_policy_assignment(recorded.get("policy_assignment"))
    if policy is not None:
        compact["policy_assignment"] = policy
    if str(recorded.get("task_kind") or "") == "sweep_shard":
        sweep_payload = recorded.get("sweep_payload")
        compact["sweep_payload"] = None
        compact["sweep_payload_deferred"] = True
        if isinstance(sweep_payload, dict):
            compact["sweep_summary"] = {
                "sweep_id": sweep_payload.get("sweep_id"),
                "shard_id": sweep_payload.get("shard_id"),
                "outcome": sweep_payload.get("outcome"),
                "permutation_count": len(
                    sweep_payload.get("permutation_indices") or []
                ),
                "scored_count": len(
                    sweep_payload.get("ranked_permutations")
                    or sweep_payload.get("ranked")
                    or []
                ),
                "failed_count": len(
                    sweep_payload.get("failed_permutations") or []
                ),
            }
    return compact


def _compact_completed_sweep_spec(spec: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: copy.deepcopy(spec[key])
        for key in _COMPLETED_SWEEP_SPEC_KEYS
        if key in spec
    }


def _artifact_backed_sweep_result(recorded: Mapping[str, Any]) -> bool:
    artifact_dir = Path(str(recorded.get("artifact_dir") or "")).resolve(
        strict=False
    )
    return (artifact_dir / "sweep-results.json").is_file()


def _compact_loaded_lane_state(lane: Any) -> None:
    phase_results = getattr(lane, "phase_results", None)
    if not isinstance(phase_results, dict):
        return
    for phase, records in list(phase_results.items()):
        if not isinstance(records, list):
            continue
        compacted: list[Any] = []
        for recorded in records:
            if (
                isinstance(recorded, dict)
                and str(recorded.get("task_kind") or "") == "sweep_shard"
                and _artifact_backed_sweep_result(recorded)
            ):
                compacted.append(_compact_recorded_result(recorded))
            else:
                compacted.append(recorded)
        phase_results[phase] = compacted


def _compact_completed_lane_task_state(task_ids: Iterable[str]) -> int:
    compacted_count = 0
    for task_id in {str(item) for item in task_ids if str(item)}:
        with _LOCK:
            lane = _TASK_LANES.get(task_id)
        if lane is None:
            continue
        spec = (getattr(lane, "task_specs", {}) or {}).get(task_id)
        phase = _phase_for_task(lane, task_id, spec)
        if isinstance(spec, dict) and spec.get("task_kind") == "sweep_shard":
            lane.task_specs[task_id] = _compact_completed_sweep_spec(spec)
            compacted_count += 1
        records = (getattr(lane, "phase_results", {}) or {}).get(phase)
        if isinstance(records, list):
            for index, recorded in enumerate(records):
                if (
                    isinstance(recorded, dict)
                    and str(recorded.get("task_id") or "") == task_id
                    and str(recorded.get("task_kind") or "") == "sweep_shard"
                    and _artifact_backed_sweep_result(recorded)
                ):
                    records[index] = _compact_recorded_result(recorded)
                    break
    return compacted_count


def _restore_revoked_lane_task_state(
    journal: DurableExecutionJournal,
    task_ids: Iterable[str],
) -> None:
    tasks = getattr(journal, "_tasks", None)
    if not isinstance(tasks, dict):
        return
    from . import play_hand_lab

    for task_id in {str(item) for item in task_ids if str(item)}:
        with _LOCK:
            lane = _TASK_LANES.get(task_id)
        durable_task = tasks.get(task_id)
        if lane is None or not isinstance(durable_task, dict):
            continue
        if durable_task.get("status") == "terminal":
            continue
        phase = play_hand_lab._task_phase_from_persisted_lane(lane, task_id)
        lane.task_specs[task_id] = play_hand_lab._task_spec_from_durable_payload(
            lane,
            task_id=task_id,
            durable_task=durable_task,
            phase=phase,
        )
        records = (getattr(lane, "phase_results", {}) or {}).get(phase or "")
        if isinstance(records, list):
            records[:] = [
                row
                for row in records
                if not (
                    isinstance(row, dict)
                    and str(row.get("task_id") or "") == task_id
                )
            ]


def _bounded_lane_state_payload(lane: Any) -> dict[str, Any]:
    """Serialize only the durable lane projection without traversing heavy caches."""

    payload: dict[str, Any] = {}
    for field_info in fields(lane):
        name = field_info.name
        if name in _LANE_STATE_OMITTED_FIELDS:
            continue
        value = getattr(lane, name)
        if name in _LANE_STATE_PATH_FIELDS:
            payload[name] = str(value) if value is not None else None
        elif name in _LANE_STATE_SET_FIELDS:
            payload[name] = sorted(value)
        else:
            payload[name] = copy.deepcopy(value)
    payload["profile_payload"] = None
    payload["incumbent_profile_payload"] = None
    payload["task_specs"] = {}
    return payload


def _decode_state_payload(path: Path) -> dict[str, Any]:
    try:
        raw = Path(path).read_bytes()
    except OSError as exc:
        raise DurableExecutionError(
            f"PlayHand durable state is unreadable: {path}"
        ) from exc
    try:
        from . import play_hand_lab

        if play_hand_lab._orjson is not None:
            payload = play_hand_lab._orjson.loads(raw)
        else:
            payload = json.loads(raw)
        del raw
    except (ValueError, json.JSONDecodeError) as exc:
        raise DurableExecutionError(
            f"PlayHand durable state is unreadable: {path}"
        ) from exc
    if not isinstance(payload, dict):
        raise DurableExecutionError(
            f"PlayHand durable state is unreadable: {path}"
        )
    return payload


def _bounded_load_campaign_state(
    path: Path,
    *,
    runtime: Any,
    campaign_id: str,
) -> tuple[list[Any], Any, int, list[int], int]:
    from . import play_hand_lab

    payload = _decode_state_payload(path)
    if payload.get("schema_version") != "play-hand-lab-durable-state-v1":
        raise DurableExecutionError("PlayHand durable state schema mismatch")
    if payload.get("lineage") != play_hand_lab._campaign_state_lineage(
        runtime, campaign_id
    ):
        raise DurableExecutionError("PlayHand durable state lineage mismatch")

    raw_lanes = payload.get("lanes") or []
    lanes: list[Any] = []
    if isinstance(raw_lanes, list):
        for index, item in enumerate(raw_lanes):
            if not isinstance(item, dict):
                continue
            lane = play_hand_lab._lane_state_from_payload(dict(item))
            _compact_loaded_lane_state(lane)
            lanes.append(lane)
            raw_lanes[index] = None

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

    _remember_lane_graph(lanes)
    return (
        lanes,
        history,
        int(payload.get("next_lane_index") or 0),
        sorted(
            {
                int(item)
                for item in payload.get("reserved_lane_indices") or []
            }
        ),
        int(payload.get("recorded_result_count") or 0),
    )


def _bounded_hydrate_unresolved_lane_task_specs(
    lanes: list[Any],
    durable_tasks_by_id: Mapping[str, Any],
) -> None:
    """Hydrate one needed spec at a time and immediately slim completed shards."""

    from . import play_hand_lab

    for lane in lanes:
        for task_id in lane.task_ids:
            _remember_lane_task(lane, task_id)
            if task_id in lane.task_specs:
                continue
            phase = play_hand_lab._task_phase_from_persisted_lane(
                lane, task_id
            )
            if not phase:
                raise DurableExecutionError(
                    f"durable task has no persisted phase: {task_id}"
                )
            task_is_terminal = (
                task_id in lane.completed_task_ids
                or task_id in lane.failed_task_ids
            )
            if task_is_terminal and play_hand_lab._phase_terminal(lane, phase):
                continue
            durable_task = durable_tasks_by_id.get(task_id)
            if not isinstance(durable_task, dict):
                raise DurableExecutionError(
                    f"durable task is missing: {task_id}"
                )
            spec = play_hand_lab._task_spec_from_durable_payload(
                lane,
                task_id=task_id,
                durable_task=durable_task,
            )
            if task_is_terminal and spec.get("task_kind") == "sweep_shard":
                spec = _compact_completed_sweep_spec(spec)
            lane.task_specs[task_id] = spec
            if task_is_terminal and isinstance(
                durable_task, _LazyTerminalTask
            ):
                durable_task._compact_again()


def _load_sweep_payload_from_artifact(
    recorded: Mapping[str, Any],
    *,
    task_id: str,
) -> dict[str, Any]:
    from . import play_hand_lab

    artifact_dir = Path(str(recorded.get("artifact_dir") or "")).resolve(
        strict=False
    )
    path = artifact_dir / "sweep-results.json"
    if not path.is_file():
        raise DurableExecutionError(
            f"sweep task {task_id} is missing its shard receipt"
        )
    payload = play_hand_lab._load_json(path)
    if not isinstance(payload, dict):
        raise DurableExecutionError(
            f"sweep task {task_id} has an unreadable shard receipt"
        )
    return payload


def _hydrate_sweep_parent_inputs(
    *,
    lane: Any,
    task_specs: Mapping[str, tuple[str, Mapping[str, Any]]],
    records_by_task_id: Mapping[str, Mapping[str, Any]],
) -> tuple[
    dict[str, tuple[str, dict[str, Any]]],
    dict[str, dict[str, Any]],
]:
    from . import play_hand_lab

    hydrated_specs: dict[str, tuple[str, dict[str, Any]]] = {}
    for shard_id, (task_id, raw_spec) in task_specs.items():
        spec = dict(raw_spec)
        if not isinstance(spec.get("params_by_index"), dict):
            rebuilt = play_hand_lab._rebuild_sweep_shard_params_by_index(
                lane, spec
            )
            if rebuilt is None:
                raise DurableExecutionError(
                    f"persisted sweep task spec is missing params_by_index: {task_id}"
                )
            expected_sha256 = str(spec.get("params_by_index_sha256") or "")
            observed_sha256 = play_hand_lab.canonical_sha256(
                play_hand_lab._canonical_params(rebuilt)
            )
            if expected_sha256 and observed_sha256 != expected_sha256:
                raise DurableExecutionError(
                    f"rebuilt sweep task params conflict for task {task_id}"
                )
            spec["params_by_index"] = rebuilt
        hydrated_specs[str(shard_id)] = (str(task_id), spec)

    hydrated_records: dict[str, dict[str, Any]] = {}
    for task_id, raw_recorded in records_by_task_id.items():
        recorded = dict(raw_recorded)
        if not isinstance(recorded.get("sweep_payload"), dict):
            recorded["sweep_payload"] = _load_sweep_payload_from_artifact(
                recorded,
                task_id=str(task_id),
            )
        hydrated_records[str(task_id)] = recorded

    return hydrated_specs, hydrated_records


def _bounded_merge_sweep_parent_receipts(
    *,
    lane: Any,
    phase: str,
    sweep_id: str,
    task_specs: dict[str, tuple[str, dict[str, Any]]],
    records_by_task_id: dict[str, dict[str, Any]],
) -> dict[str, Any]:
    hydrated_specs, hydrated_records = _hydrate_sweep_parent_inputs(
        lane=lane,
        task_specs=task_specs,
        records_by_task_id=records_by_task_id,
    )
    return _ORIGINAL_MERGE_SWEEP_PARENT_RECEIPTS(
        lane=lane,
        phase=phase,
        sweep_id=sweep_id,
        task_specs=hydrated_specs,
        records_by_task_id=hydrated_records,
    )


def _bounded_add_recorded_result_sample(
    recorded_results: list[dict[str, Any]],
    recorded: dict[str, Any],
) -> None:
    from . import play_hand_lab

    if len(recorded_results) < max(
        int(play_hand_lab.SUMMARY_RECORDED_RESULTS_SAMPLE_LIMIT), 0
    ):
        recorded_results.append(_compact_recorded_result(recorded))


def install_play_hand_lab_deep_memory_bounds() -> None:
    """Install low-peak journal/state loading and active-lane sweep compaction."""

    global _INSTALLED
    global _ORIGINAL_JOURNAL_LOAD_FROM_DISK
    global _ORIGINAL_JOURNAL_APPLY_BATCH
    global _ORIGINAL_MEMORY_COMPACT_TERMINAL_TASK
    global _ORIGINAL_MEMORY_READ_TASK_RECORD_FROM_DISK
    global _ORIGINAL_REGISTER_TASK_SPEC
    global _ORIGINAL_RECOVER_UNRESOLVED_TASK_GRAPH
    global _ORIGINAL_LOAD_CAMPAIGN_STATE
    global _ORIGINAL_LANE_STATE_PAYLOAD
    global _ORIGINAL_MERGE_SWEEP_PARENT_RECEIPTS
    global _ORIGINAL_ADD_RECORDED_RESULT_SAMPLE

    with _LOCK:
        if _INSTALLED:
            return

        from . import play_hand_lab
        from . import play_hand_lab_memory as _memory

        _ORIGINAL_JOURNAL_LOAD_FROM_DISK = (
            DurableExecutionJournal._load_from_disk
        )
        _ORIGINAL_JOURNAL_APPLY_BATCH = DurableExecutionJournal.apply_batch
        _ORIGINAL_MEMORY_COMPACT_TERMINAL_TASK = (
            _memory._compact_terminal_task
        )
        _ORIGINAL_MEMORY_READ_TASK_RECORD_FROM_DISK = (
            _memory._read_task_record_from_disk
        )
        _ORIGINAL_REGISTER_TASK_SPEC = play_hand_lab._register_task_spec
        _ORIGINAL_RECOVER_UNRESOLVED_TASK_GRAPH = (
            play_hand_lab._recover_unresolved_journal_task_graph
        )
        _ORIGINAL_LOAD_CAMPAIGN_STATE = play_hand_lab._load_campaign_state
        _ORIGINAL_LANE_STATE_PAYLOAD = play_hand_lab._lane_state_payload
        _ORIGINAL_MERGE_SWEEP_PARENT_RECEIPTS = (
            play_hand_lab._merge_sweep_parent_receipts
        )
        _ORIGINAL_ADD_RECORDED_RESULT_SAMPLE = (
            play_hand_lab._add_recorded_result_sample
        )

        def register_task_spec(*args: Any, **kwargs: Any) -> None:
            result = _ORIGINAL_REGISTER_TASK_SPEC(*args, **kwargs)
            lane = args[0] if args else kwargs.get("lane")
            task_id = kwargs.get("task_id")
            if lane is not None and task_id:
                _remember_lane_task(lane, str(task_id))
            return result

        def recover_unresolved_task_graph(
            lanes: list[Any],
            durable_tasks_by_id: Mapping[str, Any],
        ) -> None:
            _ORIGINAL_RECOVER_UNRESOLVED_TASK_GRAPH(
                lanes, durable_tasks_by_id
            )
            _remember_lane_graph(lanes)

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
            result = _ORIGINAL_JOURNAL_APPLY_BATCH(
                journal,
                registrations=registration_rows,
                completions=completion_rows,
                revocations=revocation_rows,
            )
            if not _is_play_hand_journal(journal):
                return result

            _refresh_task_record_index(journal)
            completed_ids = [
                str(task_id) for task_id, _receipt in completion_rows
            ]
            _compact_completed_lane_task_state(completed_ids)
            for task_id in completed_ids:
                _make_lazy_terminal_record(journal, task_id)
            if revocation_rows:
                _restore_revoked_lane_task_state(journal, revocation_rows)
            return journal._view()

        DurableExecutionJournal._load_from_disk = (
            _stream_play_hand_load_from_disk
        )
        DurableExecutionJournal.apply_batch = apply_batch
        _memory._compact_terminal_task = (
            _compact_terminal_task_without_lazy_inflation
        )
        _memory._read_task_record_from_disk = (
            _read_indexed_task_record_from_disk
        )
        play_hand_lab._register_task_spec = register_task_spec
        play_hand_lab._recover_unresolved_journal_task_graph = (
            recover_unresolved_task_graph
        )
        play_hand_lab._hydrate_unresolved_lane_task_specs = (
            _bounded_hydrate_unresolved_lane_task_specs
        )
        play_hand_lab._load_campaign_state = _bounded_load_campaign_state
        play_hand_lab._lane_state_payload = _bounded_lane_state_payload
        play_hand_lab._merge_sweep_parent_receipts = (
            _bounded_merge_sweep_parent_receipts
        )
        play_hand_lab._add_recorded_result_sample = (
            _bounded_add_recorded_result_sample
        )
        _INSTALLED = True


__all__ = [
    "install_play_hand_lab_deep_memory_bounds",
]
