from __future__ import annotations

import ast
import hashlib
import json
from pathlib import Path
import sys

import pytest

from autoresearch.temporal_discovery_base import TemporalDiscoveryContractError
from autoresearch.temporal_discovery_validation import (
    DashboardBidirectionalPairCompiler,
    SubprocessCandidateValidator,
    validator_provenance,
)


TRADING_DASHBOARD = Path(r"C:\repos\Trading-Dashboard")
VALIDATOR_SCRIPT = TRADING_DASHBOARD / "scripts" / "temporal_search_validate_candidate.py"
CORE_TEST = (
    TRADING_DASHBOARD
    / "shared"
    / "python"
    / "fuzzfolio_core"
    / "tests"
    / "test_temporal_search_candidate_validation.py"
)
CORE_PYTHON = TRADING_DASHBOARD / "compute-service" / ".venv" / "Scripts" / "python.exe"


def _sha(payload: object) -> str:
    encoded = json.dumps(
        payload, sort_keys=True, ensure_ascii=True, allow_nan=False, separators=(",", ":")
    )
    return "sha256:" + hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _valid_profile() -> dict:
    tree = ast.parse(CORE_TEST.read_text(encoding="utf-8"))
    fixtures = [
        node
        for node in tree.body
        if isinstance(node, ast.FunctionDef)
        and node.name in {"_transition", "_candidate_profile"}
    ]
    namespace: dict[str, object] = {}
    exec(compile(ast.Module(body=fixtures, type_ignores=[]), str(CORE_TEST), "exec"), namespace)
    return namespace["_candidate_profile"]()  # type: ignore[operator]


def _real_validator(monkeypatch: pytest.MonkeyPatch) -> SubprocessCandidateValidator:
    monkeypatch.setenv(
        "PYTHONPATH", str(TRADING_DASHBOARD / "shared" / "python")
    )
    return SubprocessCandidateValidator(
        [str(CORE_PYTHON), str(VALIDATOR_SCRIPT)],
        persistent_jsonl=True,
        timeout_seconds=10,
    )


def _fake_server(tmp_path: Path) -> tuple[list[str], Path]:
    starts = tmp_path / "starts.txt"
    program = tmp_path / "server.py"
    program.write_text(
        """
import json
import pathlib
import sys
import time

starts = pathlib.Path(sys.argv[1])
starts.write_text(starts.read_text() + 'x' if starts.exists() else 'x')
for line in sys.stdin:
    request = json.loads(line)
    candidate = request['candidateId']
    if request.get('operation') == 'compile_bidirectional':
        profile = {'version': 'v3', 'directionMode': 'both', 'fixture': candidate}
        def digest(value):
            import hashlib
            return 'sha256:' + hashlib.sha256(json.dumps(value, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        raw = digest(profile)
        report = {
            'schemaVersion': 'temporal_search_candidate_validation_v1',
            'candidateId': candidate,
            'rawSourceProfileSha256': raw,
            'profileSnapshotSha256': digest({'snapshot': profile}),
            'programSha256': digest({'program': profile}),
            'validationReportSha256': digest({'validation': profile}),
            'evaluatorId': 'bar_single_position_v3',
            'status': 'valid_evaluable',
            'candidateAcceptable': True,
        }
        result = {
            'schemaVersion': 'temporal_search_bidirectional_compile_result_v1',
            'candidateId': candidate,
            'longRawSourceProfileSha256': request['expectedLongRawSourceProfileSha256'],
            'shortRawSourceProfileSha256': request['expectedShortRawSourceProfileSha256'],
            'rawSourceProfileSha256': raw,
            'profileSnapshotSha256': report['profileSnapshotSha256'],
            'programSha256': report['programSha256'],
            'validationReportSha256': report['validationReportSha256'],
            'evaluatorId': report['evaluatorId'],
            'profile': profile,
            'report': report,
        }
        sys.stdout.write(json.dumps({'schemaVersion': 'temporal_search_bidirectional_compile_jsonl_response_v1', 'requestId': request['requestId'], 'operation': 'compile_bidirectional', 'semanticExitCode': 0, 'result': result}, separators=(',', ':')) + '\\n')
        sys.stdout.flush()
        continue
    if candidate == 'crash':
        raise SystemExit(7)
    if candidate == 'timeout':
        time.sleep(5)
    if candidate == 'oversize':
        sys.stdout.write('x' * 4096)
        sys.stdout.flush()
        continue
    if candidate in {'nan-exit', 'infinity-exit'}:
        token = 'NaN' if candidate == 'nan-exit' else 'Infinity'
        sys.stdout.write(
            '{"schemaVersion":"temporal_search_candidate_validation_jsonl_response_v1",'
            '"requestId":' + json.dumps(request['requestId']) + ','
            '"operation":"validate_candidate","semanticExitCode":' + token + ','
            '"report":{"schemaVersion":"temporal_search_candidate_validation_v1",'
            '"candidateAcceptable":true}}\\n'
        )
        sys.stdout.flush()
        continue
    request_id = 'wrong' if candidate == 'wrong-id' else request['requestId']
    response = {
        'schemaVersion': 'temporal_search_candidate_validation_jsonl_response_v1',
        'requestId': request_id,
        'operation': request.get('operation'),
        'semanticExitCode': (
            False if candidate == 'bool-exit' else
            2.0 if candidate == 'float-exit' else
            0
        ),
        'report': {
            'schemaVersion': 'temporal_search_candidate_validation_v1',
            'candidateAcceptable': True,
            'fixture': candidate,
        },
    }
    sys.stdout.write(json.dumps(response, separators=(',', ':')) + '\\n')
    sys.stdout.flush()
""".strip(),
        encoding="utf-8",
    )
    return [sys.executable, str(program), str(starts)], starts


def test_persistent_validator_reuses_real_server_and_matches_one_shot_report(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    profile = _valid_profile()
    raw_sha = _sha(profile)
    with _real_validator(monkeypatch) as persistent:
        first = persistent.validate(
            candidate_id="persistent_one",
            source_profile=profile,
            expected_raw_source_profile_sha256=raw_sha,
        )
        second = persistent.validate(
            candidate_id="persistent_two",
            source_profile=profile,
            expected_raw_source_profile_sha256=raw_sha,
        )
    one_shot = SubprocessCandidateValidator(
        [str(CORE_PYTHON), str(VALIDATOR_SCRIPT)], timeout_seconds=10
    ).validate(
        candidate_id="persistent_one",
        source_profile=profile,
        expected_raw_source_profile_sha256=raw_sha,
    )

    assert first == one_shot
    assert second["candidateAcceptable"] is True


@pytest.mark.parametrize(
    ("candidate_id", "match"),
    [
        ("crash", "exited"),
        ("timeout", "timed out"),
        ("wrong-id", "request ID mismatch"),
        ("oversize", "exceeds JSONL line limit"),
        ("nan-exit", "malformed JSONL response"),
        ("infinity-exit", "malformed JSONL response"),
        ("bool-exit", "invalid semantic exit code"),
        ("float-exit", "invalid semantic exit code"),
    ],
)
def test_persistent_validator_fails_closed_on_bad_server_responses(
    tmp_path: Path,
    candidate_id: str,
    match: str,
) -> None:
    command, _starts = _fake_server(tmp_path)
    validator = SubprocessCandidateValidator(
        command,
        persistent_jsonl=True,
        timeout_seconds=1,
        persistent_max_line_bytes=1024,
    )
    with pytest.raises(TemporalDiscoveryContractError, match=match):
        validator.validate(
            candidate_id=candidate_id,
            source_profile={"fixture": candidate_id},
            expected_raw_source_profile_sha256="sha256:" + "a" * 64,
        )
    assert validator._persistent_transport is None


@pytest.mark.parametrize(
    ("candidate_id", "match"),
    [
        ("timeout", "timed out"),
        ("nan-exit", "malformed JSONL response"),
        ("infinity-exit", "malformed JSONL response"),
        ("bool-exit", "invalid semantic exit code"),
        ("float-exit", "invalid semantic exit code"),
    ],
)
def test_persistent_validator_restarts_only_for_a_later_fresh_request(
    tmp_path: Path,
    candidate_id: str,
    match: str,
) -> None:
    command, starts = _fake_server(tmp_path)
    validator = SubprocessCandidateValidator(
        command, persistent_jsonl=True, timeout_seconds=1
    )
    with pytest.raises(TemporalDiscoveryContractError, match=match):
        validator.validate(
            candidate_id=candidate_id,
            source_profile={"fixture": candidate_id},
            expected_raw_source_profile_sha256="sha256:" + "a" * 64,
        )
    report = validator.validate(
        candidate_id="fresh",
        source_profile={"fixture": "fresh"},
        expected_raw_source_profile_sha256="sha256:" + "a" * 64,
    )
    validator.close()

    assert report["fixture"] == "fresh"
    assert starts.read_text(encoding="utf-8") == "xx"


def test_persistent_transport_protocol_is_bound_into_validator_provenance() -> None:
    command = ["validator", "script.py"]
    legacy = SubprocessCandidateValidator(command)
    persistent = SubprocessCandidateValidator(command, persistent_jsonl=True)
    contract = {
        "validatorSchema": "temporal_search_candidate_validation_v1",
        "fuzzfolioCommit": "a" * 40,
    }

    legacy_provenance = validator_provenance(legacy, validation_contract=contract)
    persistent_provenance = validator_provenance(
        persistent, validation_contract=contract
    )

    assert legacy_provenance["validationContractSha256"] != persistent_provenance[
        "validationContractSha256"
    ]
    assert persistent_provenance["validatorCommandSha256"] == legacy_provenance[
        "validatorCommandSha256"
    ]


def test_persistent_bidirectional_compiler_reuses_one_server_and_binds_identities(
    tmp_path: Path,
) -> None:
    command, starts = _fake_server(tmp_path)
    long_profile = {"version": "v2", "directionMode": "long", "fixture": "long"}
    short_profile = {"version": "v2", "directionMode": "short", "fixture": "short"}
    validator = SubprocessCandidateValidator(command, persistent_jsonl=True, timeout_seconds=1)
    first = validator.compile_pair(
        candidate_id="pair_one",
        long_profile=long_profile,
        short_profile=short_profile,
        expected_long_raw_source_profile_sha256=_sha(long_profile),
        expected_short_raw_source_profile_sha256=_sha(short_profile),
    )
    second = validator.compile_pair(
        candidate_id="pair_two",
        long_profile=long_profile,
        short_profile=short_profile,
        expected_long_raw_source_profile_sha256=_sha(long_profile),
        expected_short_raw_source_profile_sha256=_sha(short_profile),
    )
    validator.close()

    assert starts.read_text(encoding="utf-8") == "x"
    assert first["profile"]["directionMode"] == "both"
    assert second["candidateId"] == "pair_two"


def test_pair_compiler_adapter_exposes_only_the_closed_genome_authority_shape(
    tmp_path: Path,
) -> None:
    command, _starts = _fake_server(tmp_path)
    long_profile = {"version": "v2", "directionMode": "long"}
    short_profile = {"version": "v2", "directionMode": "short"}
    client = SubprocessCandidateValidator(command, persistent_jsonl=True, timeout_seconds=1)
    compiler = DashboardBidirectionalPairCompiler(client)
    compiled = compiler.compile_pair(
        candidate_id="pair_adapter",
        long_profile=long_profile,
        short_profile=short_profile,
    )
    client.close()

    assert set(compiled) == {"profile", "validation"}
    assert compiled["validation"]["candidateId"] == "pair_adapter"
