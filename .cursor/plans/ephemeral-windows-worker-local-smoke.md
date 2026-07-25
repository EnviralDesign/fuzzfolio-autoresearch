# Ephemeral Windows Replay Worker — Local Smoke Slice

## Goal
One pasteable local Windows Docker Desktop ephemeral worker session (1–2 workers, 2–5 min) that redeems once, registers against Lab Gateway with the authority contract, and leaves no session-owned Docker/filesystem resources after deadline or Ctrl+C.

## Non-goals
- Public HTTPS / production CDN routing
- Vast / office-PC rollout
- Changing PlayHand task or worker-contract semantics
- Overloading `manage-fuzzfolio-replay-workers.ps1`
- Forensic erasure; market-data-lake scoped child tokens

## Current evidence
| Fact | Source | Confidence |
|---|---|---|
| Spec-only; no impl files | repo-scout | high |
| Lab Gateway HTTP/WS on :8799, durable bearer auth | `play_hand_lab_gateway.py` | high |
| Authority binds image + contract | `phase3-darwin-rich-ab-v3/.../phase3-playhand-authority.json` | high |
| Persistent bootstrap uses `restart: unless-stopped` | TD `worker_gateway.py` | high |
| Docker Desktop installed but daemon not running | `docker info` failed | high |
| Lake creds available via `compute-service/.env` | scout + path exists | high |

## Frozen decisions
- **Product behavior:** Min duration **2m** (smoke); max 12h; workers auto|N with `--max-workers` default 2 for smoke Procman entry; `restart: "no"`; exact-label cleanup only; KeepImage default for first smoke.
- **Architecture:** AutoResearch owns mint/redeem/status/revoke + session principal auth + generator. Trading-Dashboard owns canonical `ephemeral_worker_session.ps1` + static serve route. Local profile: enrollment `http://127.0.0.1:8799/ephemeral-sessions/redeem`; worker `gateway_url` `http://host.docker.internal:8799`; lake URL/token loaded at redeem from env/`compute-service/.env`.
- **Local bootstrap:** Generator supports `--local-bootstrap-script PATH` so smoke need not run TD backend; still add TD serve endpoint for production shape.
- **Compatibility:** Durable admin/worker token behavior unchanged; ephemeral token cannot enqueue/read/ack/snapshot.

## Acceptance criteria
- [ ] `uv run generate-ephemeral-worker-command --duration 3m --workers 1 --max-workers 2 ...` mints and copies/redacts without printing durable secrets
- [ ] Redeem once → compose project `fuzzfolio-ephemeral-*` with labels + `restart: "no"`
- [ ] ≥1 worker registers with expected contract (or clear timeout + cleanup)
- [ ] Deadline or Ctrl+C removes session containers, volumes, `worker.env`, scheduled task, session dir
- [ ] Persistent `fuzzfolio-replay-workers` / unlabeled resources untouched
- [ ] Existing gateway unit tests still pass; new ephemeral session tests pass

## Execution packages
| ID | Objective | Worker | Dependencies | Exclusive write set |
|---|---|---|---|---|
| WP1 | Session registry + AuthPrincipal + mint/redeem/status/revoke + tests | senior-worker | — | `autoresearch/ephemeral_worker_sessions.py`, `autoresearch/play_hand_lab_gateway.py`, `tests/test_ephemeral_worker_sessions.py`, `tests/test_play_hand_lab_gateway.py` |
| WP2 | Generator CLI + pyproject entry + Procman 10m/smoke entries | bounded-worker | WP1 HTTP contracts | `autoresearch/ephemeral_worker_command.py`, `autoresearch/__main__.py`, `pyproject.toml`, `scripts/processes.json`, `tests/test_processes_config.py`, `tests/test_ephemeral_worker_command.py` |
| WP3 | Canonical PS1 lifecycle + TD static endpoint | bounded-worker | WP1 manifest schema | TD: `backend/app/resources/ephemeral_worker_session.ps1`, `backend/app/api/worker_gateway.py`, related tests/packaging only |

## Verification
- Package: `uv run pytest tests/test_ephemeral_worker_sessions.py tests/test_play_hand_lab_gateway.py -q`
- Package: `uv run pytest tests/test_ephemeral_worker_command.py tests/test_processes_config.py -q`
- Integration smoke: Lab Gateway up → generate 3m/1 worker → run PS1 → verify register → wait/Ctrl+C → docker label filter empty
- Independent lean-verifier: **yes** after WP1–WP3

## Risks and rollback
- `host.docker.internal` / lake HTTPS from containers — validate during smoke
- Gateway restart drops in-memory sessions — expected
- Rollback: revert package commits/files; docker `label=com.fuzzfolio.ephemeral=true` manual cleanup if needed
