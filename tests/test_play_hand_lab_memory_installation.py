from __future__ import annotations

from autoresearch import play_hand_lab
from autoresearch import play_hand_lab_enqueue
from autoresearch import play_hand_lab_memory_deep


def test_package_bootstrap_installs_deep_memory_bounds() -> None:
    assert (
        play_hand_lab._enqueue_gateway_tasks_with_retries
        is play_hand_lab_enqueue.enqueue_gateway_tasks_with_retries
    )
    assert play_hand_lab_memory_deep._INSTALLED is True
    assert play_hand_lab._add_recorded_result_sample.__module__ == (
        "autoresearch.play_hand_lab_memory_deep"
    )
    assert play_hand_lab._format_lab_barrier_snapshot.__module__ == (
        "autoresearch.play_hand_lab_memory_deep"
    )
