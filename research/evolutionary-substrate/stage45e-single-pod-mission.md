# Stage 4.5E single-pod synthetic mission

## Scope

This is an operational design only; do not launch it during Stage 4.5E. The
pod runs the Ground-Zero synthetic arena from the exact V5 commit, sealed
witness/protocol hashes, and one deterministic seed. It has no market
credentials, worker registration, gateway access, archive write access, or
generation command.

## Envelope

- bind image and repository commit before launch;
- use 4 vCPU, 8 GiB RAM, 20 GiB disk, a 45-minute hard TTL, and a $2 cost
  ceiling;
- emit a heartbeat every two minutes and checkpoint only the compact trace;
- write exactly one source-free result ZIP plus its SHA-256 manifest;
- verify upload checksum and emit a terminal receipt; and
- force-destroy the mother instance on success, failure, missing heartbeat,
  upload failure, or TTL expiry.

A disposable pod is appropriate only after the local protocol is sealed:
independent compute and forced cleanup make a bounded trace job auditable,
while the distributed market-worker fleet is categorically out of scope. A
local execution remains preferable for initial protocol debugging because it
is cheaper and directly inspectable.

The pod decision is therefore deferred until review identifies a synthetic
question that requires more than the local deterministic trace run.
