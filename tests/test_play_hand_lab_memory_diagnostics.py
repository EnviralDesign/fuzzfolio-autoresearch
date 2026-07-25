from __future__ import annotations

from autoresearch.play_hand_lab_memory_deep import memory_diagnostics


def test_memory_diagnostics_are_bounded_and_redacted() -> None:
    diagnostics = memory_diagnostics()

    assert set(diagnostics) == {
        "working_set_bytes",
        "private_commit_bytes",
        "tracked_task_ids",
        "tracked_task_copies",
        "profile_cache_entries",
        "attached_cache_entries",
        "compacted_terminal_tasks",
        "recorded_sample_count",
        "recorded_sweep_sample_count",
        "recorded_sample_approx_bytes",
    }
    for key, value in diagnostics.items():
        assert value is None or isinstance(value, int), key
        if isinstance(value, int):
            assert value >= 0, key
