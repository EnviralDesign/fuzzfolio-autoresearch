# Ephemeral Windows Replay Worker Session Specification

Date: 2026-07-22

Status: proposed implementation specification

Primary owner: `C:\repos\fuzzfolio-autoresearch`

Supporting owner: `C:\repos\Trading-Dashboard`

## 1. Executive Summary

Build a one-command Windows worker session intended for temporary computers reached through TeamViewer or a similar remote desktop tool.

The operator should be able to click a Procman generator entry, paste one command into PowerShell on a Windows computer that already has Docker Desktop, and walk away. The command starts an isolated Fuzzfolio replay-worker pool for a bounded duration, verifies that it joined the expected PlayHand Lab Gateway with the expected immutable worker contract, and removes all session-owned Docker and filesystem resources at the deadline.

The target operator experience is:

```powershell
& ([scriptblock]::Create((irm "https://backend.enviral-design.com/api/worker-gateway/ephemeral-bootstrap.ps1"))) `
  -EnrollmentUrl "https://playhand-lab.enviral-design.com/ephemeral-sessions/redeem" `
  -EnrollmentToken "<short-lived-one-use-token>" `
  -Duration "2h" `
  -Workers auto `
  -MaxWorkers 6
```

The generated command must not contain the durable Lab Gateway token or durable market-data-lake token. The enrollment token may appear in PowerShell history because it expires quickly and can be redeemed only once.

The session must be isolated by a random session ID, Docker Compose project name, Docker labels, volume labels, and a private temporary directory. Cleanup must be idempotent and ownership-scoped. It must never stop or delete ordinary Fuzzfolio workers, unrelated Docker containers, shared Docker volumes, or preexisting images.

"Leave no trace" means operational cleanup, not forensic erasure. The implementation must remove session-owned containers, volumes, generated files, scheduled tasks, and eligible pulled images. It cannot promise to erase Windows Event Log entries, PowerShell history backups, Docker Desktop diagnostics, antivirus telemetry, TeamViewer history, filesystem journal records, or network logs.

## 2. Origin And Product Intent

The original use case is a set of office Windows PCs that are not on the home LAN but are reachable through TeamViewer. The operator wants to temporarily contribute their CPU capacity to PlayHand without installing the repositories, leaving long-running workers behind, or manually cleaning Docker afterward.

The original requested properties were:

- Docker Desktop is assumed to be installed.
- One pasteable PowerShell command is preferred.
- A committed script invoked by a small command is acceptable.
- Duration accepts hours and minutes, including smoke-test durations.
- Worker count should be automatically derived from Docker CPU and memory limits.
- The approved worker image should be pulled automatically.
- The script should connect to the public Lab Gateway and market data lake.
- At expiration, containers, images, temporary files, and session credentials should be removed.
- Procman should generate and copy the command without exposing durable secrets in logs.
- Closing PowerShell, losing TeamViewer, or rebooting should not leave workers configured to restart indefinitely.

This is a private research-operations feature. It is not a Fuzzfolio SaaS feature and must not be exposed in the normal product UI.

## 3. Current Building Blocks

Most of the compute path already exists.

### 3.1 Trading-Dashboard

Current reusable pieces:

- `compute-service/app/workers/bootstrap_command.py`
  - Generates Windows, Linux, macOS, and Vast worker bootstrap instructions.
  - Supports `lab_ws`, `lab_http`, and `gateway_http` transports.
  - Supports immutable image selection, worker pools, gateway/lake URLs, worker counts, and optional inline secrets.
- `backend/app/api/worker_gateway.py`
  - Serves the current `/api/worker-gateway/bootstrap.ps1` script.
  - The current script writes `worker.env` and `compose.yaml`, pulls the image, and starts workers.
- `scripts/manage-fuzzfolio-replay-workers.ps1`
  - Supports `status`, `start`, `stop`, `restart`, `logs`, and `pull` for persistent worker installations.
- The immutable replay-worker image and the `sim-worker-replay` entrypoint.
- Worker contract computation and contract reporting.
- Lab WebSocket transport using:
  - `FUZZFOLIO_WORKER_TRANSPORT=lab_ws`
  - `FUZZFOLIO_LAB_GATEWAY_URL`
  - `FUZZFOLIO_LAB_GATEWAY_TOKEN`
  - `REMOTE_MARKET_DATA_LAKE_BASE_URL`
  - `REMOTE_MARKET_DATA_LAKE_API_TOKEN`

Current gaps:

- The current Windows bootstrap is persistent.
- It uses `restart: unless-stopped`.
- It writes credentials and Compose files to a stable directory.
- It has no duration, deadline, session ownership labels, cleanup watchdog, or image provenance record.
- It does not verify registration against an expected worker contract.
- The existing manager is installation-oriented and must not be overloaded with ephemeral semantics.

### 3.2 AutoResearch

Current reusable pieces:

- `autoresearch/play_hand_lab_gateway.py`
  - Owns the private Lab Gateway worker registry, task queue, leases, completions, and snapshots.
  - Supports HTTP and WebSocket worker authentication using one shared bearer token.
- `autoresearch/play_hand_lab_auth.py`
  - Creates or loads the durable Lab Gateway token from a local file.
- `scripts/processes.json`
  - Owns the Procman `Lab Gateway`, Phase 3 coordinator, and authority-audit entries.
- Phase 3 authority artifacts already bind the approved immutable worker image and worker contract.

Current gaps:

- No ephemeral session model.
- No local-only enrollment-token mint endpoint.
- No public one-time redemption endpoint.
- No session-scoped worker principal or expiry.
- No session-specific status endpoint.
- No Procman command generator entry.

## 4. Scope

### 4.1 Required First Release

The first release must provide:

1. A canonical, versioned Windows PowerShell session script.
2. A public HTTPS endpoint that serves only that non-secret script.
3. A local command generator that mints one short-lived enrollment token.
4. A Procman entry that runs the generator and copies the generated command to the Windows clipboard.
5. One-time credential redemption over HTTPS.
6. Session-scoped Lab Gateway authentication that expires at the worker deadline plus a small grace period.
7. Existing market-data-lake credentials delivered only after redemption, never embedded in Procman configuration or the copied command.
8. Automatic Docker CPU/memory-aware worker sizing.
9. Immutable image and expected worker-contract binding.
10. Registration verification before the session is considered healthy.
11. Deadline-based cleanup with a scheduled-task fallback.
12. Manual and Ctrl+C cleanup.
13. Idempotent recovery and cleanup after partial startup.
14. Redacted logs and a concise final lifecycle summary.
15. Automated tests plus one real 5 to 10 minute Windows smoke.

### 4.2 Explicit Non-Goals

Do not include these in the first implementation:

- Installing Docker Desktop.
- Managing Vast instances.
- Starting or stopping PlayHand coordinators.
- Starting or stopping the Lab Gateway.
- Changing PlayHand task semantics or worker contracts.
- Replacing Sager or Mac persistent worker tooling.
- A Fuzzfolio product UI.
- General remote administration.
- Secure deletion or forensic log erasure.
- Removing unrelated Docker resources to reclaim disk.
- Reusing the persistent `fuzzfolio-replay-workers` Compose project.
- Automatically extending a running session.
- Running workers after the declared deadline because tasks remain active.

### 4.3 Deferred Security Enhancement

The current market data lake uses a durable API token. The first release may return that token only through one-time HTTPS redemption and must delete it locally during cleanup. That avoids placing it in the copied command or PowerShell history, but it does not make the lake credential expire server-side.

True end-to-end session credential expiry requires one of:

- market-data-lake support for scoped child tokens with an expiry and read-only scope; or
- a carefully designed authenticated gateway proxy for lake access.

That is a separate lake-owned enhancement. Do not block the operationally tidy first release on it, and do not falsely describe the static lake token as short-lived.

## 5. Architectural Decisions

### 5.1 Ownership

AutoResearch owns:

- session issuance;
- session-scoped Lab Gateway authentication;
- authority binding;
- Procman command generation;
- worker-session status;
- enrollment and worker-token expiry.

Trading-Dashboard owns:

- the worker image;
- worker contract reporting;
- the canonical PowerShell bootstrap/cleanup implementation;
- the public endpoint that serves the script;
- Windows Docker mechanics.

This keeps private research orchestration out of the product backend while keeping reusable worker bootstrap mechanics beside the worker image.

### 5.2 Separate Ephemeral And Persistent Tooling

Do not add duration and destructive cleanup behavior to `manage-fuzzfolio-replay-workers.ps1`.

Create a dedicated ephemeral script. Persistent Sager/Mac tooling and ordinary worker installations must retain their current behavior.

### 5.3 Authority Is Immutable

The generator must not use `latest`, `main`, or an unqualified `vast` tag.

It must load an authority artifact and bind:

- exact image reference;
- expected worker contract hash;
- Lab Gateway URL;
- market data lake URL;
- transport `lab_ws`;
- any required worker capability/protocol identifiers.

The preferred input is the current Phase 3 PlayHand authority artifact. The generator must fail closed if the image or contract is missing, mutable, malformed, or inconsistent with an explicitly supplied override.

### 5.4 One-Time Enrollment

The copied command contains an opaque enrollment token, not durable credentials.

Enrollment requirements:

- At least 256 random bits.
- Store only a cryptographic hash server-side.
- Default enrollment TTL: 20 minutes.
- Single successful redemption.
- Atomic consume operation.
- Bound to one session ID, authority ID, duration, worker policy, and expected contract.
- A failed malformed redemption does not consume it.
- A successful redemption consumes it before returning credentials.
- Reuse returns a generic expired-or-used response.

### 5.5 Session-Scoped Gateway Token

On redemption, the gateway issues a second random bearer token used by workers.

It must:

- be scoped only to worker registration, heartbeat, claim, completion, and failure operations;
- be associated with one session ID;
- expire at `deadline + cleanup_grace`;
- not authorize coordinator task enqueue, result reads, result acknowledgements, global snapshots, enrollment minting, or other admin operations;
- use constant-time hash comparison;
- never be logged in plaintext;
- be invalid immediately after explicit session revocation.

The existing durable gateway token remains valid for coordinators and existing trusted workers. The new principal model must preserve backward compatibility.

### 5.6 Session Isolation

Each session uses:

- Session ID: `ews-<UTC timestamp>-<12 random lowercase hex>`.
- Pool: `ephemeral-windows-<short session suffix>` unless explicitly set by authority.
- Compose project: `fuzzfolio-ephemeral-<short session suffix>`.
- Work directory: `%LOCALAPPDATA%\Fuzzfolio\EphemeralWorkers\<session-id>`.
- Scheduled task: `Fuzzfolio-Ephemeral-Cleanup-<session-id>`.
- Cache volume: `<compose-project>_lake-cache`.

Every created container and volume must carry:

```text
com.fuzzfolio.ephemeral=true
com.fuzzfolio.ephemeral-session=<session-id>
com.fuzzfolio.ephemeral-deadline=<UTC ISO-8601>
com.fuzzfolio.ephemeral-owner=<current Windows SID>
```

Cleanup must select by exact session label or exact Compose project. Never select only by image name, worker pool, generic `fuzzfolio` text, or container-name substring.

## 6. End-To-End Operator Flow

### 6.1 Preconditions

On the operator PC:

1. Lab Gateway is running and reachable through its public HTTPS endpoint.
2. The market data lake is healthy and reachable through HTTPS.
3. The authority artifact is current and names an immutable image and expected contract.
4. AutoResearch Procman is running locally.

On the temporary Windows PC:

1. Windows 10 or 11 x64.
2. PowerShell 7 preferred; Windows PowerShell 5.1 may be supported only if tests pass.
3. Docker Desktop installed.
4. Docker Linux containers enabled.
5. Outbound HTTPS to the bootstrap endpoint, Lab Gateway, Docker Hub, and market data lake.
6. Enough Docker CPU, RAM, and disk capacity.
7. Permission to create a current-user Scheduled Task.

### 6.2 Generate The Command

The operator starts a Procman entry such as:

```text
Generate Ephemeral Windows Workers - 2h
```

Procman runs a short-lived local command:

```powershell
uv run generate-ephemeral-worker-command `
  --authority-path C:\repos\fuzzfolio-autoresearch\runs\derived\phase3-authorities\<authority>\phase3-playhand-authority.json `
  --duration 2h `
  --workers auto `
  --max-workers 6 `
  --copy `
  --json-redacted
```

The generator:

1. Loads and validates the authority.
2. Confirms the local Lab Gateway is reachable.
3. Confirms the gateway reports the expected protocol generation.
4. Mints an enrollment record.
5. Builds the one-line PowerShell command.
6. Copies it directly to the clipboard.
7. Prints only redacted metadata.
8. Exits zero.

Example redacted output:

```json
{
  "status": "copied",
  "session_id": "ews-20260722T140900Z-a1b2c3d4e5f6",
  "enrollment_expires_at": "2026-07-22T14:29:00Z",
  "duration": "PT2H",
  "workers": "auto",
  "max_workers": 6,
  "image": "lucasmorgan/fuzzfolio-replay-worker:vast-sha-<git>",
  "expected_contract": "sha256:<hash>",
  "command_copied": true
}
```

The actual command and token must not be written to Procman stdout/stderr.

### 6.3 Paste And Redeem

The operator pastes the command into PowerShell on the temporary PC.

The script:

1. Enforces TLS for non-loopback enrollment URLs.
2. Downloads the canonical script.
3. Parses and validates duration and local options.
4. POSTs the one-time token to the redemption endpoint.
5. Receives a bootstrap manifest.
6. Validates manifest schema, session ID, authority, image, contract, endpoints, and deadline.
7. Immediately removes the enrollment token variable from normal script state where practical.

### 6.4 Preflight

Before creating any Docker resource, the script checks:

- OS is Windows x64.
- PowerShell version is supported.
- `docker.exe` exists.
- Docker Desktop engine responds to `docker info`.
- Docker is in Linux-container mode.
- Docker reports supported architecture for the image.
- Docker CPU and memory limits can support at least one worker.
- Host and Docker storage have sufficient free capacity.
- No live session with the same ID exists.
- The session work directory is absent or contains a valid resumable session manifest for this exact session.
- The Scheduled Tasks required for crash cleanup can be created.
- Public Gateway and lake endpoints are HTTPS and syntactically valid.

If Docker Desktop is installed but not running, the script may attempt to start the standard Docker Desktop executable and wait up to five minutes. It must not install or upgrade Docker.

Preflight failure must create no container, volume, Compose project, or persistent secret file. If a session directory was created, cleanup removes it before exit.

### 6.5 Start

After preflight:

1. Create the private session directory.
2. Set directory ACLs to the current Windows user and SYSTEM only where practical.
3. Write a redacted `session.json` manifest.
4. Write `worker.env` containing redeemed credentials.
5. Write `compose.yaml` with session labels and `restart: "no"`.
6. Write `cleanup.ps1` with no embedded durable credentials.
7. Register the cleanup Scheduled Task before starting workers.
8. Record whether the image existed before this session.
9. Pull the exact immutable image.
10. Resolve and record the pulled image ID and repository digest.
11. Start the exact worker count under the unique Compose project.
12. Poll session status until the expected workers register or startup times out.
13. Verify every registered worker reports the expected contract.
14. Enter the bounded foreground status loop.

### 6.6 Run

The foreground output should be deliberately small:

```text
Fuzzfolio ephemeral worker session
Session: ews-20260722T140900Z-a1b2c3d4e5f6
Workers: 6/6 registered, 6 compatible
Image: vast-sha-656f43da9df0
Contract: sha256:0f2e7284...5fec3
Deadline: 2026-07-22 16:09:00Z
Remaining: 01:47:32
Press Ctrl+C to stop and clean up now.
```

Refresh no more frequently than every 15 seconds by default. Do not print credentials, full environment values, raw enrollment responses, or Docker inspect output.

### 6.7 Deadline Or Manual Stop

At deadline, Ctrl+C, normal script error, or explicit cleanup invocation:

1. Mark cleanup started in the redacted session manifest.
2. Best-effort notify the Lab Gateway that the session is ending.
3. Stop and remove exact session containers.
4. Run Compose down with `--remove-orphans --volumes` for the exact project.
5. Remove any remaining exact-labeled session containers and volumes.
6. Remove eligible image data according to the image-removal policy.
7. Delete `worker.env`, `compose.yaml`, transient responses, and session scripts.
8. Remove the Scheduled Task.
9. Revoke the gateway session token.
10. Remove the now-empty session directory.
11. Print a redacted cleanup summary and exit.

If cleanup partly fails, retain only a redacted recovery manifest and cleanup script. Print the exact safe recovery command. Return nonzero.

## 7. Bootstrap Manifest Contract

The redemption response should be a versioned JSON document:

```json
{
  "schema_version": 1,
  "session_id": "ews-20260722T140900Z-a1b2c3d4e5f6",
  "issued_at": "2026-07-22T14:10:00Z",
  "deadline": "2026-07-22T16:10:00Z",
  "cleanup_grace_seconds": 600,
  "authority": {
    "authority_id": "sha256:<authority>",
    "image": "lucasmorgan/fuzzfolio-replay-worker:vast-sha-<git>",
    "expected_worker_contract": "sha256:<contract>",
    "required_capabilities": ["playhand_lab_protocol:playhand-lab-worker-v1"]
  },
  "worker": {
    "transport": "lab_ws",
    "pool": "ephemeral-windows-a1b2c3d4e5f6",
    "gateway_url": "https://playhand-lab.enviral-design.com",
    "gateway_token": "<session-scoped-secret>",
    "lake_url": "https://fuzzfoliodatalake.enviral-design.com/",
    "lake_token": "<current-static-secret>",
    "workers": "auto",
    "max_workers": 6,
    "worker_memory_mb": 768,
    "worker_memory_reserve_mb": 2048,
    "cpu_reserve": 1,
    "startup_jitter_seconds": 30,
    "lake_download_slots": 8
  },
  "bootstrap": {
    "script_sha256": "sha256:<script-content-hash>",
    "minimum_free_disk_gb": 30,
    "registration_timeout_seconds": 300,
    "status_interval_seconds": 15,
    "remove_image_when_safe": true
  }
}
```

Rules:

- Reject unknown `schema_version`.
- Reject a deadline more than the configured maximum session duration.
- Reject mutable image tags.
- Reject non-HTTPS remote URLs.
- Reject a contract that is not `sha256:<64 lowercase hex>`.
- Do not allow client parameters to override authority image, contract, transport, gateway URL, or lake URL.
- Client arguments may lower worker count or duration but may never exceed server-issued maxima.

## 8. CLI Contract

### 8.1 Generator

Add an AutoResearch entry point:

```text
generate-ephemeral-worker-command
```

Required arguments:

```text
--authority-path PATH
--duration DURATION
```

Optional arguments:

```text
--workers auto|N              default: auto
--max-workers N               default: 6
--enrollment-ttl 20m          default: 20m, max: 60m
--minimum-free-disk-gb N      default: 30
--registration-timeout 5m     default: 5m
--bootstrap-url URL           normally configured, HTTPS required
--public-gateway-url URL      normally configured, HTTPS required
--copy                        copy command to clipboard
--print-command               explicit unsafe operator-only option
--json-redacted               structured safe output
--dry-run                     validate without minting or clipboard changes
```

Duration grammar:

```text
15m
90m
2h
1h30m
```

Rules:

- Minimum session duration: 5 minutes.
- Default maximum: 12 hours.
- Parse into seconds, then emit canonical ISO-8601 duration in records.
- Reject ambiguous decimal hours such as `1.5h` unless explicitly implemented and tested.
- `--print-command` must warn that the enrollment token will enter Procman logs if invoked there. Procman entries must never use it.

### 8.2 Remote Script

The script parameters are:

```powershell
param(
  [Parameter(Mandatory)] [uri]$EnrollmentUrl,
  [Parameter(Mandatory)] [string]$EnrollmentToken,
  [string]$Duration = "",
  [string]$Workers = "auto",
  [Nullable[int]]$MaxWorkers = $null,
  [switch]$RemoveImage,
  [switch]$KeepImage,
  [switch]$AllowConcurrent,
  [switch]$NoDockerDesktopAutoStart,
  [ValidateSet("Run", "Cleanup", "Status")] [string]$Action = "Run",
  [string]$SessionId = ""
)
```

Client duration and worker arguments are optional restrictions. They cannot exceed the enrollment manifest.

`-RemoveImage` and `-KeepImage` are mutually exclusive. Default behavior follows the server manifest.

## 9. Gateway API Contract

### 9.1 Local Mint Endpoint

```http
POST /admin/ephemeral-sessions
Host: 127.0.0.1:8799
Authorization: Bearer <durable-admin-token>
Content-Type: application/json
```

Request includes validated authority identity, image, contract, duration, worker bounds, public URLs, and script hash.

Requirements:

- Accept only loopback clients.
- Require the durable admin token in addition to loopback origin.
- Never expose this endpoint through a public reverse proxy route.
- Return the plaintext enrollment token exactly once.
- Store only its hash.

### 9.2 Public Redeem Endpoint

```http
POST /ephemeral-sessions/redeem
Content-Type: application/json

{
  "enrollment_token": "...",
  "client_nonce": "<random>",
  "script_sha256": "sha256:<hash>"
}
```

Response is the bootstrap manifest.

Requirements:

- HTTPS outside loopback.
- Atomic one-use redemption.
- Rate limiting by source and token hash.
- Generic errors for used, expired, and unknown tokens.
- No token in URL query parameters.
- `Cache-Control: no-store`.
- Do not log request bodies.

### 9.3 Session Status Endpoint

```http
GET /ephemeral-sessions/<session-id>/status
Authorization: Bearer <session-worker-token>
```

Response contains only session-local information:

```json
{
  "session_id": "...",
  "status": "issued|active|expired|revoked",
  "deadline": "...",
  "registered_workers": 6,
  "compatible_workers": 6,
  "busy_workers": 4,
  "expected_workers": 6,
  "expected_contract": "sha256:..."
}
```

It must not expose global task counts, other pools, result payloads, or credentials.

### 9.4 Revoke Endpoint

```http
POST /ephemeral-sessions/<session-id>/revoke
Authorization: Bearer <session-worker-token>
```

Revocation is idempotent. Expired or already revoked sessions return success without revealing extra state.

## 10. Gateway State Model

Add bounded in-memory session state beside the existing gateway state:

```python
EphemeralSession(
    session_id,
    enrollment_token_hash,
    enrollment_expires_at,
    redeemed_at,
    worker_token_hash,
    worker_token_expires_at,
    revoked_at,
    authority_id,
    image,
    expected_contract,
    required_capabilities,
    expected_workers,
    max_workers,
    pool,
    deadline,
)
```

State transitions:

```text
issued -> redeemed -> active -> expired
issued -> expired
redeemed|active -> revoked
```

Session records are intentionally in-memory because the Lab Gateway is already ephemeral. A gateway restart invalidates enrollment and session tokens. The remote watchdog still cleans local resources at the deadline. The operator can generate a new session after gateway recovery.

Bound the registry:

- prune unused enrollment records after expiry;
- prune revoked/expired sessions after a diagnostic retention interval, default 60 minutes;
- set a hard maximum active session count;
- expose only aggregate counts in the global snapshot.

## 11. Worker Authentication Integration

Refactor gateway authentication to return a principal instead of a boolean:

```python
AuthPrincipal(
    kind="admin" | "durable_worker" | "ephemeral_worker",
    session_id=None | str,
    expires_at=None | float,
)
```

Authorization rules:

| Operation | Admin/durable token | Ephemeral worker token |
|---|---:|---:|
| enqueue tasks | yes | no |
| read/ack results | yes | no |
| global snapshot | yes | no |
| worker register | yes | yes |
| worker heartbeat | yes | own session only |
| worker claim | yes | own session only |
| worker complete/fail | yes | own session only |
| session-local status | yes | own session only |
| mint enrollment | local admin only | no |
| revoke session | yes | own session only |

When an ephemeral worker registers:

- associate worker ID with the authenticated session ID;
- require the session pool;
- require expected contract and capabilities;
- reject registration after deadline;
- never trust a client-provided session ID without matching auth context.

Existing durable workers and coordinator behavior must remain unchanged.

## 12. Windows Script Design

### 12.1 Canonical Source

Add a committed canonical script in Trading-Dashboard, for example:

```text
backend/app/resources/ephemeral_worker_session.ps1
```

Serve it through:

```text
GET /api/worker-gateway/ephemeral-bootstrap.ps1
```

The endpoint serves static script content only. It must not inject credentials or environment-specific secrets.

Package the resource explicitly so production deployments cannot omit it. A test must compare the served SHA-256 with the packaged file SHA-256.

Do not maintain a second divergent copy in AutoResearch.

### 12.2 Internal Functions

Structure the script into testable functions:

```text
ConvertFrom-FuzzfolioDuration
Get-FuzzfolioDockerCapacity
Get-FuzzfolioWorkerCount
Test-FuzzfolioDockerReady
Start-FuzzfolioDockerDesktop
Invoke-FuzzfolioEnrollment
New-FuzzfolioSessionDirectory
Write-FuzzfolioSessionManifest
Write-FuzzfolioComposeFiles
Register-FuzzfolioCleanupTask
Start-FuzzfolioWorkers
Wait-FuzzfolioWorkerRegistration
Get-FuzzfolioSessionStatus
Stop-FuzzfolioSession
Remove-FuzzfolioSessionImage
Unregister-FuzzfolioCleanupTask
Remove-FuzzfolioSessionFiles
Invoke-FuzzfolioStaleSessionCleanup
```

Keep orchestration at the bottom. Avoid global mutable state except one immutable session context object.

### 12.3 Docker Capacity Calculation

Use Docker's effective resources, not physical host totals:

```powershell
docker info --format '{{json .}}'
```

Derive:

```text
cpu_limit = floor(Docker NCPU)
cpu_workers = max(cpu_limit - cpu_reserve, 0)

usable_memory_mb = max(Docker MemTotal MB - memory_reserve_mb, 0)
memory_workers = floor(usable_memory_mb / worker_memory_mb)

workers = min(cpu_workers, memory_workers, server_max_workers, client_max_workers)
```

Rules:

- Require at least one worker.
- Default CPU reserve: 1.
- Default memory budget: 768 MB per worker.
- Default memory reserve: 2048 MB.
- Default max workers on unknown office PCs: 6.
- An explicit `-Workers N` may lower the count but cannot exceed calculated safe capacity or server max.
- Refuse concurrent ephemeral sessions by default.
- If `-AllowConcurrent` is used, subtract currently running ephemeral worker allocations from available capacity before calculating.

### 12.4 Compose Contract

Generated `compose.yaml` requirements:

```yaml
services:
  replay-worker:
    image: <immutable-image>
    restart: "no"
    command: ["sim-worker-replay"]
    env_file:
      - worker.env
    environment:
      FUZZFOLIO_WORKER_TRANSPORT: lab_ws
      FUZZFOLIO_WORKER_COUNT: "1"
      MARKET_DATA_LAKE_ROOT: /cache/market_data_lake
      LAZY_LAKE_CACHE_ENABLED: "true"
      LAZY_LAKE_SCOPE_ARCHIVE_ENABLED: "true"
    labels:
      com.fuzzfolio.ephemeral: "true"
      com.fuzzfolio.ephemeral-session: "<session-id>"
      com.fuzzfolio.ephemeral-deadline: "<deadline>"
      com.fuzzfolio.ephemeral-owner: "<Windows SID>"
    volumes:
      - lake-cache:/cache/market_data_lake

volumes:
  lake-cache:
    labels:
      com.fuzzfolio.ephemeral: "true"
      com.fuzzfolio.ephemeral-session: "<session-id>"
```

Do not use a keepalive sidecar. This is a normal Windows Docker Desktop session, not a cloud notebook.

### 12.5 Secret File Handling

`worker.env` contains the redeemed session gateway token and lake token.

Requirements:

- Never echo contents.
- Do not pass secrets through Docker command-line `-e` arguments.
- Limit ACLs to current user and SYSTEM where possible.
- Mark file hidden only as a convenience, not a security boundary.
- Delete it during every cleanup path.
- Do not put tokens in `session.json`.
- Do not include secret values in exception messages.
- Clear response variables after writing when practical, recognizing PowerShell/.NET cannot guarantee memory erasure.

### 12.6 Scheduled Cleanup Task

Create the watchdog before starting containers.

Recommended task characteristics:

- Name includes exact session ID.
- Current-user principal with limited privileges by default.
- One trigger at the UTC deadline.
- One trigger at current-user logon for missed-deadline recovery.
- `StartWhenAvailable = true`.
- Multiple-instance policy: ignore new instance.
- Runs the session-owned `cleanup.ps1` with exact session ID and directory.
- Cleanup script polls briefly for Docker availability, then removes exact-labeled resources.
- The task unregisters itself after successful cleanup.

Do not store the Windows account password.

Limitation: current-user tasks cannot run while no user is logged on. `restart: "no"` ensures workers still do not restart after reboot. The logon trigger handles cleanup when the user next signs in.

### 12.7 Image Removal Policy

Before pull, record whether the exact image reference and image ID already exist.

At cleanup, remove the image only when all are true:

1. Server policy or `-RemoveImage` requests removal.
2. The exact image was not present before this session.
3. No container, including stopped containers, references the image ID.
4. No other active Fuzzfolio ephemeral session manifest records ownership of the same newly pulled image.

If any condition is uncertain, preserve the image and report `image_preserved_reason`.

Never run `docker image prune`, `docker system prune`, or any global cleanup command.

### 12.8 Cleanup Idempotency

Cleanup must succeed safely when called:

- twice;
- by both `finally` and Scheduled Task;
- after partial file generation;
- after pull but before Compose start;
- after some containers start;
- when Docker is unavailable;
- after the gateway session expired;
- after the work directory was partly deleted.

Use an exclusive cleanup lock file. If another cleanup owns the lock, wait briefly and then verify terminal state rather than starting a competing destructive path.

Success means no exact session containers, volumes, Compose project, secret files, or Scheduled Task remain. Image preservation may still be a successful cleanup when removal safety cannot be proven.

## 13. Procman Entries

Add one generic generator plus a few operator-friendly presets to AutoResearch `scripts/processes.json`.

Recommended entries:

```text
Generate Ephemeral Windows Workers - 10m Smoke
Generate Ephemeral Windows Workers - 2h
Generate Ephemeral Windows Workers - 6h
```

Properties:

- `auto_restart=false`.
- Excluded from stack start/stop.
- One-shot process that exits after clipboard copy.
- No token or generated command in the static Procman command.
- Uses a fixed authority path or an explicit current-authority resolver.
- Process name clearly says `Generate`; it does not start local workers.

The generic direct CLI remains available for arbitrary durations.

Procman validation must assert:

- entries parse;
- IDs are stable and unique;
- no durable token appears in command text;
- no `--print-command` appears;
- `auto_restart` is false;
- authority path exists;
- the command invokes the intended generator.

## 14. Observability

### 14.1 Local Remote-PC Output

Show:

- session ID;
- effective Docker CPU/RAM;
- selected worker count;
- image tag and shortened digest;
- expected contract shortened hash;
- registration count and compatibility;
- deadline and remaining duration;
- cleanup outcome.

Do not show:

- tokens;
- raw manifest;
- `worker.env`;
- full Docker environment;
- authorization headers;
- one-line command after generation.

### 14.2 Gateway Metrics

Add bounded counters:

- ephemeral enrollments issued;
- redeemed;
- expired unused;
- active sessions;
- revoked sessions;
- session-authenticated registered workers;
- rejected expired-session requests;
- contract mismatches;
- redemption failures by coarse reason.

Do not label global metrics with raw session IDs if that creates unbounded cardinality.

### 14.3 Procman Output

Procman should only say command copied and show redacted session metadata. If clipboard access fails, it should fail nonzero and tell the operator how to rerun from an interactive shell. It must not fall back to printing the secret-bearing command automatically.

## 15. Failure Handling

### 15.1 Enrollment Expired Or Already Used

- Exit before Docker mutation.
- Delete any empty session directory.
- Tell operator to generate a new command.

### 15.2 Docker Missing

- Exit before redemption if practical, or revoke redeemed session immediately.
- Do not install Docker.
- Print concise prerequisite guidance.

### 15.3 Docker Desktop Not Running

- Attempt standard startup unless disabled.
- Wait with bounded timeout.
- On timeout, revoke session and clean local files.

### 15.4 Insufficient Capacity

- Exit before pull/start.
- State effective Docker CPU, RAM, disk, and minimum required.
- Revoke session.

### 15.5 Image Pull Failure

- Retry a small bounded number of times.
- Cleanup generated files and Scheduled Task.
- Do not delete a preexisting image.

### 15.6 Registration Timeout

- Capture only a redacted tail of container logs.
- Stop/remove the session.
- Revoke token.
- Report registered and compatible counts.
- Never leave failed startup containers running until the original deadline.

### 15.7 Contract Mismatch

- Treat as fatal startup failure.
- Stop all exact session workers immediately.
- Preserve no worker containers.
- Report expected and observed hashes, not credentials.

### 15.8 Network Loss During Run

- Workers may retry according to existing transport behavior.
- The local deadline remains authoritative.
- Do not extend deadline based on inactivity.
- Cleanup still occurs offline.

### 15.9 Terminal Closed Or TeamViewer Disconnects

- Foreground process may die.
- Containers continue until the Scheduled Task deadline.
- Scheduled Task performs cleanup.
- Containers use `restart: "no"`.

### 15.10 Reboot

- Containers do not restart.
- Scheduled Task runs at next logon/start-when-available.
- A future ephemeral invocation also scans and removes expired labeled sessions owned by the current user.

### 15.11 Gateway Restart

- Session token becomes invalid because session state is in-memory.
- Workers stop receiving work and retry.
- Local deadline cleanup still succeeds.
- Operator may stop early or let deadline cleanup run.
- Do not silently replace authority or credentials mid-session.

### 15.12 Cleanup Cannot Reach Docker

- Remove secret files immediately if possible.
- Preserve redacted cleanup metadata and Scheduled Task.
- Retry Docker cleanup at logon/start-when-available.
- Never report complete while exact-labeled containers or volumes are known to remain.

## 16. Security Requirements

1. Use `secrets.token_urlsafe(32)` or stronger equivalent for tokens.
2. Store token hashes, not plaintext, in gateway state.
3. Use constant-time comparison.
4. Require HTTPS for all public URLs.
5. Never put durable secrets in Procman JSON, generated command, URL query strings, logs, or redacted manifests.
6. Add `Cache-Control: no-store` to redemption responses.
7. Do not log redemption request or response bodies.
8. Scope ephemeral tokens to worker operations only.
9. Expire/revoke gateway tokens server-side.
10. Bind enrollment to authority, duration, worker bounds, and script hash.
11. Reject client attempts to widen issued authority or capacity.
12. Use exact resource labels for cleanup.
13. Never run global Docker prune commands.
14. Preserve current durable-token authentication behavior.
15. Document the static lake-token residual risk.

## 17. Implementation Map

Exact filenames may adjust after code inspection, but ownership should remain as follows.

### 17.1 AutoResearch

Add:

```text
autoresearch/ephemeral_worker_sessions.py
tests/test_ephemeral_worker_sessions.py
z_docs/EPHEMERAL_WINDOWS_REPLAY_WORKER_SESSION_OPERATOR.md
```

Modify:

```text
autoresearch/play_hand_lab_gateway.py
autoresearch/play_hand_lab_cli.py or the current CLI registration layer
autoresearch/__main__.py
pyproject.toml
scripts/processes.json
tests/test_play_hand_lab_gateway.py
tests/test_processes_config.py
```

Responsibilities:

- authority loading and validation;
- duration parsing;
- enrollment issuance;
- session registry and principal authentication;
- generator CLI and clipboard output;
- Procman presets;
- gateway API and tests.

### 17.2 Trading-Dashboard

Add:

```text
backend/app/resources/ephemeral_worker_session.ps1
```

Modify:

```text
backend/app/api/worker_gateway.py
backend/tests/test_worker_gateway.py
compute-service/app/workers/bootstrap_command.py
compute-service/tests/test_worker_bootstrap_command.py
backend packaging configuration as required for the PowerShell resource
```

Optional testing support:

```text
scripts/tests/ephemeral-worker-session.Tests.ps1
```

Responsibilities:

- serve canonical script;
- generate the command shape;
- PowerShell Docker lifecycle;
- script hashing and packaging;
- mocked lifecycle tests.

### 17.3 Do Not Modify Unless A Test Proves It Necessary

Avoid changes to:

- replay execution semantics;
- worker contract inputs;
- Phase 3 campaign authority schema;
- market data lake API;
- Sager/Mac worker scripts;
- persistent worker Compose behavior.

## 18. Delivery Plan

### Milestone 1: Pure Contracts And Generator

1. Add duration parser and authority loader.
2. Define enrollment/session dataclasses and JSON contracts.
3. Add generator CLI dry-run.
4. Add command rendering with placeholder enrollment token.
5. Add redaction tests.

Exit gate:

- deterministic command shape;
- no durable secrets in output;
- malformed authority/duration fails before mutation.

### Milestone 2: Gateway Enrollment And Session Auth

1. Add bounded session registry.
2. Add local mint endpoint.
3. Add public redemption endpoint.
4. Add principal-based auth.
5. Add session status and revoke endpoints.
6. Preserve legacy durable-token paths.

Exit gate:

- one-time redemption proven under concurrency;
- expired/revoked session cannot register or claim;
- session token cannot enqueue/read/ack results;
- existing gateway tests remain green.

### Milestone 3: PowerShell Lifecycle

1. Add canonical script and static serving endpoint.
2. Implement preflight and capacity calculation.
3. Implement exact session Compose generation.
4. Implement Scheduled Task watchdog.
5. Implement startup registration verification.
6. Implement idempotent cleanup and safe image policy.
7. Add mocked Pester/fake-Docker tests.

Exit gate:

- no global Docker operations;
- every partial-start checkpoint cleans safely;
- exact session resources are gone after cleanup.

### Milestone 4: Procman Integration

1. Add preset entries.
2. Implement clipboard copy.
3. Add process config tests.
4. Reload only after checking live Procman state.

Exit gate:

- clicking entry copies command;
- logs contain no token or command;
- entry exits zero and does not auto-restart.

### Milestone 5: Real Smoke

Run on a Windows Docker Desktop host:

1. 5-minute session, 1 worker.
2. 10-minute session, auto workers capped at 2.
3. Verify actual compatible registration.
4. If a campaign backlog exists, verify at least one accepted completion.
5. Verify deadline cleanup.
6. Repeat while closing the foreground terminal after registration.
7. Verify Scheduled Task cleanup.
8. Verify no exact session containers, volumes, task, env file, or directory remains.
9. Verify preexisting image preservation.
10. Verify newly pulled image removal when safe.

Do not use an office PC for the first destructive lifecycle smoke. Use Sager or another controlled Windows host.

## 19. Test Matrix

### 19.1 Python Unit Tests

- Duration forms: `5m`, `15m`, `90m`, `2h`, `1h30m`.
- Reject zero, negative, excessive, malformed, and ambiguous durations.
- Authority must bind immutable image and valid contract.
- Enrollment token hash and single-use behavior.
- Concurrent redemption yields exactly one success.
- Expired enrollment rejected.
- Session token expiry and revocation.
- Principal authorization matrix.
- Session registry pruning and hard bounds.
- Generator output redacts all secrets.
- Clipboard path does not print command.
- Dry-run mints nothing.
- Procman entries contain no secrets.

### 19.2 Gateway Integration Tests

- Durable worker token still registers/claims/completes.
- Ephemeral token registers only its session.
- Ephemeral token cannot access coordinator endpoints.
- Expired token disconnects/rejects WebSocket operations.
- Wrong contract registration rejected or marked incompatible.
- Status exposes only session-local workers.
- Gateway restart invalidates sessions cleanly.
- Redemption response has `Cache-Control: no-store`.
- Request bodies are absent from logs.

### 19.3 PowerShell Unit Tests

Use Pester and mocked external commands.

- Capacity calculation by CPU and memory.
- Worker cap enforcement.
- Docker missing/not-ready paths.
- Generated Compose uses `restart: "no"` and exact labels.
- Secret values never appear in redacted manifest/output.
- Cleanup command selects exact session labels.
- Cleanup twice succeeds.
- Partial startup cleanup at every checkpoint.
- Scheduled Task definitions contain exact session paths.
- Existing image is preserved.
- Newly pulled unused image is removed.
- Shared/running image is preserved.
- Concurrent session refusal and explicit allowance.
- Stale expired-session cleanup ignores unexpired and unrelated resources.

### 19.4 Real Acceptance Tests

| Scenario | Expected result |
|---|---|
| 5m, 1 worker | registers, remains compatible, cleans at deadline |
| 10m, auto max 2 | capacity selects 1-2 safely, both register |
| Ctrl+C | immediate idempotent cleanup |
| close terminal | Scheduled Task cleans at deadline |
| network disconnect | workers retry, local cleanup still happens |
| gateway restart | session stops being useful, cleanup still happens |
| reboot before deadline | workers do not restart; cleanup occurs at next logon |
| enrollment reuse | rejected before Docker mutation |
| wrong expected contract | startup fails and cleans |
| preexisting persistent Fuzzfolio workers | untouched |
| unrelated Docker workloads | untouched |

## 20. Acceptance Criteria

The feature is complete only when all are true:

1. Procman can generate and copy a command without logging any credential.
2. The copied command contains only a short-lived one-use enrollment token.
3. A clean Windows Docker Desktop host needs no repository checkout.
4. Worker count is derived from effective Docker limits and obeys a maximum.
5. Workers use an immutable image and report the expected contract.
6. Registration is verified before startup is declared successful.
7. Containers use `restart: "no"`.
8. Closing the terminal does not leave workers past the deadline under normal Task Scheduler operation.
9. Cleanup removes exact session containers, volumes, secret/config files, and Scheduled Task.
10. Cleanup never touches persistent Fuzzfolio workers or unrelated Docker resources.
11. Cleanup is idempotent after partial startup and repeated invocation.
12. Image removal follows the safe ownership policy.
13. Gateway session credentials expire or revoke server-side.
14. Existing durable gateway tokens and workers remain compatible.
15. The static lake-token limitation is documented.
16. Automated tests pass in both repositories.
17. A controlled real Windows smoke proves foreground and watchdog cleanup.
18. An operator document provides generation, use, status, early stop, and recovery commands.

## 21. Kill And Escalation Criteria

Stop implementation and reassess before proceeding if:

- Session auth requires changing replay task or worker contract semantics.
- The public endpoint would need to expose the durable gateway token directly.
- Procman cannot avoid logging the generated command.
- Cleanup requires Docker-wide prune operations.
- Scheduled cleanup cannot be made ownership-scoped.
- The implementation modifies persistent worker behavior.
- A worker can use a session token to access coordinator/admin endpoints.
- The script cannot distinguish preexisting images/resources from session-owned resources.
- Real smoke leaves credentials or containers after a successful cleanup.

Escalate the separate market-data-lake token project if office-PC use becomes routine or the risk of exposing the static lake token is no longer acceptable.

## 22. Operator Recovery Commands

The implemented script should support these safe concepts, with exact syntax documented after implementation:

```powershell
# Show one session without exposing credentials
ephemeral-bootstrap.ps1 -Action Status -SessionId <session-id>

# Stop and remove one exact session early
ephemeral-bootstrap.ps1 -Action Cleanup -SessionId <session-id>
```

If the original bootstrap script is gone, recovery must still be possible through exact labels:

```powershell
docker ps -a --filter "label=com.fuzzfolio.ephemeral-session=<session-id>"
docker volume ls --filter "label=com.fuzzfolio.ephemeral-session=<session-id>"
```

The operator guide may provide exact deletion commands only when they include the full validated session ID. It must never recommend generic `docker system prune` or deletion by image name.

## 23. Handoff Prompt For Implementing Agent

Implement the specification in `C:\repos\fuzzfolio-autoresearch\z_docs\EPHEMERAL_WINDOWS_REPLAY_WORKER_SESSION_SPEC_2026-07-22.md`. Treat it as a private research-operations feature. AutoResearch owns session issuance, Procman generation, and session-scoped Lab Gateway auth. Trading-Dashboard owns the canonical Windows Docker lifecycle script and its public static endpoint. Preserve all existing durable gateway and persistent worker behavior. Build in the documented milestones, keep cleanup exact-label-scoped and idempotent, do not add global Docker pruning, and do not print or commit secrets. Stop for review after the pure contracts/generator and gateway-auth milestones before running a real Windows smoke. Use Sager or another controlled Windows host for first lifecycle testing, not an office PC.

