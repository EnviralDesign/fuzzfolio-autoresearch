from __future__ import annotations

from pathlib import Path

from autoresearch import temporal_graph_lab as public_lab
from autoresearch import temporal_graph_lab_coordinator as coordinator


class _UnusedClient:
    pass


def test_public_runner_delegates_to_resumable_coordinator(
    monkeypatch,
    tmp_path: Path,
) -> None:
    client = _UnusedClient()
    task = {"task_id": "delegation-only"}
    observed: dict[str, object] = {}
    expected = [{"bundle_path": "delegated"}]

    def fake_run(
        received_client,
        received_tasks,
        *,
        output_root,
        timeout_seconds,
        poll_interval_seconds,
    ):
        observed.update(
            {
                "client": received_client,
                "tasks": received_tasks,
                "output_root": output_root,
                "timeout_seconds": timeout_seconds,
                "poll_interval_seconds": poll_interval_seconds,
            }
        )
        return expected

    monkeypatch.setattr(coordinator, "run_temporal_graph_lab_tasks", fake_run)

    actual = public_lab.run_temporal_graph_lab_tasks(
        client,
        [task],
        output_root=tmp_path,
        timeout_seconds=17.0,
        poll_interval_seconds=0.125,
    )

    assert actual is expected
    assert observed == {
        "client": client,
        "tasks": [task],
        "output_root": tmp_path,
        "timeout_seconds": 17.0,
        "poll_interval_seconds": 0.125,
    }
