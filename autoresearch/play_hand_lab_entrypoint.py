from __future__ import annotations


def main() -> int:
    from .play_hand_lab_enqueue import install_bounded_gateway_enqueue

    install_bounded_gateway_enqueue()

    from .__main__ import main as autoresearch_main

    return autoresearch_main()


__all__ = ["main"]
