from __future__ import annotations

from autoresearch import play_hand_lab
from autoresearch import play_hand_lab_enqueue
from autoresearch import play_hand_lab_gateway_runtime
from autoresearch import play_hand_lab_memory_deep
from autoresearch import play_hand_lab_result_fastpath
from autoresearch import play_hand_lab_startup
from autoresearch import play_hand_lab_throughput


def test_package_bootstrap_installs_default_runtime_bounds() -> None:
    assert (
        play_hand_lab._enqueue_gateway_tasks_with_retries
        is play_hand_lab_enqueue.enqueue_gateway_tasks_with_retries
    )
    assert play_hand_lab_memory_deep._INSTALLED is True
    assert play_hand_lab_throughput._INSTALLED is True
    assert play_hand_lab_startup._INSTALLED is True
    assert play_hand_lab_result_fastpath._INSTALLED is True
    assert play_hand_lab_gateway_runtime._INSTALLED is True
    assert play_hand_lab._add_recorded_result_sample.__module__ == (
        "autoresearch.play_hand_lab_memory_deep"
    )
    assert play_hand_lab._format_lab_barrier_snapshot.__module__ == (
        "autoresearch.play_hand_lab_result_fastpath"
    )
