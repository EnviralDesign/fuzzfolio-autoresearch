from __future__ import annotations

import tracemalloc
from pathlib import Path

from autoresearch.durable_execution import DurableExecutionJournal


def _journal(path: Path) -> DurableExecutionJournal:
    return DurableExecutionJournal(
        path,
        execution_id="phase3-streaming-memory-test",
        lineage={"campaign_id": "phase3-streaming-memory-test"},
    )


def test_streaming_loader_peak_is_bounded_by_record_not_whole_journal(tmp_path: Path) -> None:
    path = tmp_path / "play-hand-lab-execution-journal.json"
    writer = _journal(path)
    large = "x" * 100_000
    registrations = []
    completions = []
    for index in range(40):
        task_id = f"task-{index:04d}"
        registrations.append(
            (
                task_id,
                {
                    "task_id": task_id,
                    "lane_id": f"lane-{index:04d}",
                    "payload": {"large": large},
                },
            )
        )
        completions.append(
            (
                task_id,
                {
                    "recorded_result": {
                        "task_id": task_id,
                        "status": "success",
                        "large": large,
                    }
                },
            )
        )
    writer.apply_batch(registrations=registrations, completions=completions)
    journal_size = path.stat().st_size

    tracemalloc.start()
    reader = _journal(path)
    view = reader.load()
    _current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    assert all(
        "payload" not in task and task["terminal_receipt"].keys() == {"receipt_sha256"}
        for task in view["tasks"].values()
    )
    # The old loader held the complete file string, split-line copies, and full task
    # graph concurrently. The streaming path should stay below the journal's own byte
    # size even with tracemalloc bookkeeping and per-record canonical verification.
    assert peak < journal_size, {"peak": peak, "journal_size": journal_size}
