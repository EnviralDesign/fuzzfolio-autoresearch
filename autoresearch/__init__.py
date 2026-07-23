"""Fuzzfolio autoresearch runtime."""

# Phase 3 can launch the coordinator through ``python -m autoresearch`` or by
# importing the Level-C workflow directly, bypassing project console-script
# wrappers. Install the bounded, 413-adaptive gateway enqueue implementation at
# package import so every coordinator entry path uses the same transport guard.
from .play_hand_lab_enqueue import install_bounded_gateway_enqueue as _install_bounded_gateway_enqueue

_install_bounded_gateway_enqueue()
del _install_bounded_gateway_enqueue

__all__ = ["__version__"]

__version__ = "0.1.0"
