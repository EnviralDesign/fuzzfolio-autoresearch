from __future__ import annotations

import threading
import zlib
from dataclasses import dataclass
from typing import Any


_LOCK = threading.RLock()
_INSTALLED = False
_ORIGINAL_APPEND_RESULT_LOCKED: Any = None
_ORIGINAL_ACK_RESULTS: Any = None


@dataclass(slots=True)
class PackedLabResult:
    """Compressed in-memory representation of one retained gateway result.

    The gateway result queue is a transient delivery buffer, not the durable research
    authority. Keeping thousands of nested Python dictionaries here costs several
    times their serialized size. Store the exact JSON payload once, compressed, and
    decode only the small prefix the coordinator is actively consuming.
    """

    lease_id: str
    payload_zlib: bytes

    def to_payload(self) -> dict[str, Any]:
        from . import play_hand_lab_gateway as gateway_module

        payload = gateway_module._json_loads_bytes(zlib.decompress(self.payload_zlib))
        return dict(payload) if isinstance(payload, dict) else {}


def _result_lease_id(result: Any) -> str:
    return str(getattr(result, "lease_id", "") or "")


def install_play_hand_gateway_runtime_bounds() -> None:
    """Install bounded result retention and O(batch) acknowledgement.

    This preserves the HTTP/WebSocket contract and the uncompressed byte accounting
    used for backpressure. It changes only the gateway's private in-memory shape.
    """

    global _INSTALLED
    global _ORIGINAL_APPEND_RESULT_LOCKED
    global _ORIGINAL_ACK_RESULTS

    with _LOCK:
        if _INSTALLED:
            return

        from . import play_hand_lab_gateway as gateway_module

        _ORIGINAL_APPEND_RESULT_LOCKED = gateway_module.PlayHandLabGateway._append_result_locked
        _ORIGINAL_ACK_RESULTS = gateway_module.PlayHandLabGateway.ack_results

        def append_result_locked(self: Any, result: Any) -> None:
            payload = result.to_payload()
            raw = gateway_module._json_dumps_bytes(payload)
            packed = PackedLabResult(
                lease_id=str(result.lease_id),
                payload_zlib=zlib.compress(raw, level=1),
            )
            self._results.append(packed)
            # Backpressure continues to reflect the amount of JSON the coordinator
            # must receive and process, rather than the compressed resident size.
            self._result_sizes.append(len(raw))
            self._result_backlog_bytes += len(raw)
            self._trim_result_backlog_locked()

        def ack_results(self: Any, lease_ids: list[str]) -> int:
            ordered: list[str] = []
            seen: set[str] = set()
            for raw_lease_id in lease_ids:
                lease_id = str(raw_lease_id or "")
                if lease_id and lease_id not in seen:
                    seen.add(lease_id)
                    ordered.append(lease_id)
            if not ordered:
                return 0

            # The coordinator always acknowledges the prefix returned by /results.
            # Pop that prefix directly. The previous implementation rebuilt the whole
            # backlog for every 25-result acknowledgement, making a 1,400-result
            # backlog quadratic precisely when the gateway was under pressure.
            with self._lock:
                if len(ordered) <= len(self._results) and all(
                    _result_lease_id(self._results[index]) == lease_id
                    for index, lease_id in enumerate(ordered)
                ):
                    released_bytes = 0
                    for _ in ordered:
                        self._results.popleft()
                        released_bytes += self._result_sizes.popleft() if self._result_sizes else 0
                    self._result_backlog_bytes = max(
                        int(self._result_backlog_bytes) - released_bytes,
                        0,
                    )
                    self._metrics["results_acked"] += len(ordered)
                    return len(ordered)

            # Keep arbitrary/non-prefix acknowledgement behavior for operators and
            # compatibility clients. It is rare and correctness matters more than the
            # linear fallback cost in that path.
            return _ORIGINAL_ACK_RESULTS(self, ordered)

        gateway_module.PlayHandLabGateway._append_result_locked = append_result_locked
        gateway_module.PlayHandLabGateway.ack_results = ack_results
        _INSTALLED = True


__all__ = [
    "PackedLabResult",
    "install_play_hand_gateway_runtime_bounds",
]
