from __future__ import annotations

from autoresearch import play_hand_lab


def test_barrier_includes_coordinator_self_memory() -> None:
    runtime = play_hand_lab.PlayHandLabRuntimeConfig(
        campaign_id="memory-barrier-test",
        target_runs=1,
        active_runs=1,
    )

    rendered = play_hand_lab._format_lab_barrier_snapshot(
        barrier_index=1,
        campaign_id="memory-barrier-test",
        runtime=runtime,
        lanes=[],
        tasks=[],
        snapshot={},
        metric_baseline={},
        recorded_result_count=0,
    )

    assert "coordinator memory" in rendered
    assert "live-task-payloads=" in rendered
    assert "journal-stubs=" in rendered
    assert "sample-bytes=" in rendered
