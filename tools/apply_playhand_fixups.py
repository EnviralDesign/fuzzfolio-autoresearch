from pathlib import Path

path = Path("autoresearch/play_hand_lab_throughput.py")
text = path.read_text(encoding="utf-8")


def replace_once(old: str, new: str) -> None:
    global text
    count = text.count(old)
    if count != 1:
        raise RuntimeError(f"expected one match, found {count}: {old[:100]!r}")
    text = text.replace(old, new, 1)


replace_once(
    '''_ATTEMPT_CACHE_MAX_ENTRIES = 128
_ATTEMPT_CACHE: OrderedDict[
    str, tuple[tuple[int, int], list[dict[str, Any]]]
] = OrderedDict()
_ATTEMPT_CACHE_ROWS = 0
''',
    '''_ATTEMPT_CACHE_MAX_ENTRIES = 128
# The cache stores compact projections for Phase 3, so original file size is
# diagnostic rather than a useful production memory proxy. Keep the legacy
# budget seam for compatibility and tests; the entry cap bounds live objects.
_ATTEMPT_CACHE_MAX_SOURCE_BYTES = 1 << 60
_ATTEMPT_CACHE: OrderedDict[
    str, tuple[tuple[int, int], list[dict[str, Any]]]
] = OrderedDict()
_ATTEMPT_CACHE_ROWS = 0
_ATTEMPT_CACHE_SOURCE_BYTES = 0
''',
)

replace_once(
    '''def _remove_attempt_cache_entry(key: str) -> None:
    global _ATTEMPT_CACHE_ROWS
    removed = _ATTEMPT_CACHE.pop(key, None)
    if removed is not None:
        _ATTEMPT_CACHE_ROWS = max(0, _ATTEMPT_CACHE_ROWS - len(removed[1]))
''',
    '''def _remove_attempt_cache_entry(key: str) -> None:
    global _ATTEMPT_CACHE_ROWS
    global _ATTEMPT_CACHE_SOURCE_BYTES
    removed = _ATTEMPT_CACHE.pop(key, None)
    if removed is not None:
        _ATTEMPT_CACHE_ROWS = max(0, _ATTEMPT_CACHE_ROWS - len(removed[1]))
        _ATTEMPT_CACHE_SOURCE_BYTES = max(
            0,
            _ATTEMPT_CACHE_SOURCE_BYTES - int(removed[0][1]),
        )
''',
)

replace_once(
    '''def _store_attempt_cache_entry(
    key: str,
    signature: tuple[int, int],
    rows: list[dict[str, Any]],
) -> None:
    global _ATTEMPT_CACHE_ROWS
    _remove_attempt_cache_entry(key)
    _ATTEMPT_CACHE[key] = (signature, rows)
    _ATTEMPT_CACHE_ROWS += len(rows)
    while len(_ATTEMPT_CACHE) > _ATTEMPT_CACHE_MAX_ENTRIES:
        _evicted_key, entry = _ATTEMPT_CACHE.popitem(last=False)
        _ATTEMPT_CACHE_ROWS = max(0, _ATTEMPT_CACHE_ROWS - len(entry[1]))
        _COUNTERS["attempt_cache_evictions"] += 1
''',
    '''def _store_attempt_cache_entry(
    key: str,
    signature: tuple[int, int],
    rows: list[dict[str, Any]],
) -> None:
    global _ATTEMPT_CACHE_ROWS
    global _ATTEMPT_CACHE_SOURCE_BYTES
    _remove_attempt_cache_entry(key)
    _ATTEMPT_CACHE[key] = (signature, rows)
    _ATTEMPT_CACHE_ROWS += len(rows)
    _ATTEMPT_CACHE_SOURCE_BYTES += int(signature[1])
    while (
        len(_ATTEMPT_CACHE) > _ATTEMPT_CACHE_MAX_ENTRIES
        or _ATTEMPT_CACHE_SOURCE_BYTES > _ATTEMPT_CACHE_MAX_SOURCE_BYTES
    ):
        _evicted_key, entry = _ATTEMPT_CACHE.popitem(last=False)
        _ATTEMPT_CACHE_ROWS = max(0, _ATTEMPT_CACHE_ROWS - len(entry[1]))
        _ATTEMPT_CACHE_SOURCE_BYTES = max(
            0,
            _ATTEMPT_CACHE_SOURCE_BYTES - int(entry[0][1]),
        )
        _COUNTERS["attempt_cache_evictions"] += 1
''',
)

replace_once(
    '''        diagnostics["attempt_cache_entries"] = len(_ATTEMPT_CACHE)
        diagnostics["attempt_cache_rows"] = _ATTEMPT_CACHE_ROWS
        return diagnostics
''',
    '''        diagnostics["attempt_cache_entries"] = len(_ATTEMPT_CACHE)
        diagnostics["attempt_cache_rows"] = _ATTEMPT_CACHE_ROWS
        diagnostics["attempt_cache_source_bytes"] = _ATTEMPT_CACHE_SOURCE_BYTES
        return diagnostics
''',
)

replace_once(
    '''def _cached_load_attempts(path: Path) -> list[dict[str, Any]]:
    if not _phase3_attempt_path(path):
        return _ORIGINAL_LOAD_ATTEMPTS(path)
    key = _path_key(path)
    signature = _file_signature(path)
    with _LOCK:
        cached = _ATTEMPT_CACHE.get(key)
        if cached is not None and cached[0] == signature:
            _ATTEMPT_CACHE.move_to_end(key)
            _COUNTERS["attempt_cache_hits"] += 1
            return [dict(row) for row in cached[1]]
        if cached is not None:
            _remove_attempt_cache_entry(key)
    rows = _read_compact_phase3_attempts(path)
    with _LOCK:
        _store_attempt_cache_entry(key, signature, rows)
        _COUNTERS["attempt_cache_misses"] += 1
        _COUNTERS["attempt_rows_projected"] += len(rows)
    return [dict(row) for row in rows]
''',
    '''def _cached_load_attempts(path: Path) -> list[dict[str, Any]]:
    key = _path_key(path)
    signature = _file_signature(path)
    with _LOCK:
        cached = _ATTEMPT_CACHE.get(key)
        if cached is not None and cached[0] == signature:
            _ATTEMPT_CACHE.move_to_end(key)
            _COUNTERS["attempt_cache_hits"] += 1
            return [dict(row) for row in cached[1]]
        if cached is not None:
            _remove_attempt_cache_entry(key)
    phase3 = _phase3_attempt_path(path)
    rows = (
        _read_compact_phase3_attempts(path)
        if phase3
        else _ORIGINAL_LOAD_ATTEMPTS(path)
    )
    with _LOCK:
        _store_attempt_cache_entry(key, signature, rows)
        _COUNTERS["attempt_cache_misses"] += 1
        if phase3:
            _COUNTERS["attempt_rows_projected"] += len(rows)
    return [dict(row) for row in rows]
''',
)

replace_once(
    '''def _cached_append_attempt_row(path: Path, row: Mapping[str, Any]) -> None:
    persisted = dict(row)
    if _phase3_attempt_path(path) and isinstance(
        persisted.get("policy_assignment"), Mapping
    ):
        compact = _compact_policy_assignment(persisted["policy_assignment"])
        if compact != persisted["policy_assignment"]:
            persisted["policy_assignment"] = compact
            _counter("attempt_policy_assignments_compacted")
    key = _path_key(path)
    before = _file_signature(path)
    _ORIGINAL_APPEND_ATTEMPT_ROW(path, persisted)
    after = _file_signature(path)
    with _LOCK:
        cached = _ATTEMPT_CACHE.get(key)
        if cached is not None and cached[0] == before:
            cached[1].append(_project_phase3_attempt_row(persisted))
            _store_attempt_cache_entry(key, after, cached[1])
        else:
            _remove_attempt_cache_entry(key)
''',
    '''def _cached_append_attempt_row(path: Path, row: Mapping[str, Any]) -> None:
    persisted = dict(row)
    phase3 = _phase3_attempt_path(path)
    if phase3 and isinstance(persisted.get("policy_assignment"), Mapping):
        compact = _compact_policy_assignment(persisted["policy_assignment"])
        if compact != persisted["policy_assignment"]:
            persisted["policy_assignment"] = compact
            _counter("attempt_policy_assignments_compacted")
    key = _path_key(path)
    before = _file_signature(path)
    _ORIGINAL_APPEND_ATTEMPT_ROW(path, persisted)
    after = _file_signature(path)
    with _LOCK:
        cached = _ATTEMPT_CACHE.get(key)
        if cached is not None and cached[0] == before:
            cached[1].append(
                _project_phase3_attempt_row(persisted)
                if phase3
                else persisted
            )
            _store_attempt_cache_entry(key, after, cached[1])
        else:
            _remove_attempt_cache_entry(key)
''',
)

path.write_text(text, encoding="utf-8")
print("Applied attempt-cache compatibility fixups")
