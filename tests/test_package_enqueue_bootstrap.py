from __future__ import annotations

import importlib

import autoresearch
from autoresearch import play_hand_lab
from autoresearch import play_hand_lab_enqueue


def test_package_import_installs_bounded_enqueue_for_direct_launches(monkeypatch) -> None:
    """Direct ``python -m`` and Level-C imports must not depend on script wrappers."""
    sentinel = object()
    monkeypatch.setattr(play_hand_lab, "_enqueue_gateway_tasks_with_retries", sentinel)

    importlib.reload(autoresearch)

    assert (
        play_hand_lab._enqueue_gateway_tasks_with_retries
        is play_hand_lab_enqueue.enqueue_gateway_tasks_with_retries
    )
