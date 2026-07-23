from __future__ import annotations

import sys
import types

from autoresearch import play_hand_lab_enqueue
from autoresearch import play_hand_lab_entrypoint


def test_entrypoint_installs_bounded_enqueue_before_main(monkeypatch) -> None:
    calls: list[str] = []
    fake_main_module = types.ModuleType("autoresearch.__main__")

    def fake_main() -> int:
        calls.append("main")
        return 17

    fake_main_module.main = fake_main
    monkeypatch.setitem(sys.modules, "autoresearch.__main__", fake_main_module)
    monkeypatch.setattr(
        play_hand_lab_enqueue,
        "install_bounded_gateway_enqueue",
        lambda: calls.append("install"),
    )

    assert play_hand_lab_entrypoint.main() == 17
    assert calls == ["install", "main"]
