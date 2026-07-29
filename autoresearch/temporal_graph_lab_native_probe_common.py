from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import time
from typing import Any, Mapping

from .play_hand_lab import LabGatewayClient
from .temporal_graph_lab import build_temporal_graph_lab_task, canonical_sha256


PREPARATION_SCHEMA = "temporal_graph_lab_preparation_v1"
STATE_SCHEMA = "temporal_graph_lab_native_probe_state_v1"
REPORT_SCHEMA = "temporal_graph_lab_native_probe_report_v1"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def _load_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain one JSON object")
    return payload


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(dict(payload), indent=2, sort_keys=True, ensure_ascii=True) + "\n",
        encoding="utf-8",
    )


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        while chunk := handle.read(1024 * 1024):
            digest.update(chunk)
    return "sha256:" + digest.hexdigest()


def _headers(token: str | None) -> dict[str, str]:
    normalized = str(token or "").strip()
    return {"Authorization": f"Bearer {normalized}"} if normalized else {}


def _post_json(
    client: LabGatewayClient,
    path: str,
    payload: Mapping[str, Any],
    *,
    token: str | None,
) -> dict[str, Any]:
    response = client.session.post(
        f"{client.base_url}{path}",
        json=dict(payload),
        headers=_headers(token),
        timeout=client.timeout_seconds,
    )
    response.raise_for_status()
    decoded = response.json()
    if not isinstance(decoded, dict):
        raise RuntimeError(f"gateway {path} response was not a JSON object")
    return decoded


def _completion_identity(completion: Mapping[str, Any]) -> str:
    stable = dict(completion)
    stable.pop("accepted_at_wall", None)
    stable.pop("read_at", None)
    stable.pop("delivered_at", None)
    stable.pop("delivery_id", None)
    return canonical_sha256(stable)


def _build_task_from_preparation(preparation: Mapping[str, Any]) -> dict[str, Any]:
    if preparation.get("schemaVersion") != PREPARATION_SCHEMA:
        raise ValueError("unknown temporal graph Lab preparation schema")
    builder_inputs = preparation.get("builderInputs")
    if not isinstance(builder_inputs, dict):
        raise ValueError("preparation file has no builderInputs object")
    return build_temporal_graph_lab_task(**builder_inputs)


def _assert_no_unrelated_results(
    results: list[dict[str, Any]],
    *,
    task_id: str,
) -> list[dict[str, Any]]:
    unrelated = [
        str(item.get("task_id") or "<missing>")
        for item in results
        if str(item.get("task_id") or "") != task_id
    ]
    if unrelated:
        raise RuntimeError(
            "unrelated Lab result backlog encountered during isolated native probe: "
            + ", ".join(unrelated)
        )
    return [item for item in results if str(item.get("task_id") or "") == task_id]


def _wait_for_completion(
    client: LabGatewayClient,
    *,
    task_id: str,
    timeout_seconds: float,
    poll_interval_seconds: float,
) -> dict[str, Any]:
    deadline = time.monotonic() + max(float(timeout_seconds), 1.0)
    while time.monotonic() < deadline:
        results = client.read_results(limit=32)
        matching = _assert_no_unrelated_results(results, task_id=task_id)
        if len(matching) > 1:
            raise RuntimeError("gateway delivered more than one result for one task")
        if matching:
            return matching[0]
        time.sleep(max(float(poll_interval_seconds), 0.01))
    raise TimeoutError("timed out waiting for the actual temporal replay worker")
