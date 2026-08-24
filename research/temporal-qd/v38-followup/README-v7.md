# V38 evolve-everything follow-up artifacts v7

Generated locally from frozen v4/v6 artifacts, the original V38 pair-run config, and current source. No new market evaluation. v1-v6 files in this folder are unchanged except the shared README appendix.

| File | Task |
| --- | --- |
| `v38-followup-authority-discovery-v7.json` / `.md` | A/B |
| `canonical-pair-compile-attempt-v7.json` / `.md` | A/B |
| `topology-coadaptation-matrix-spec-v7.json` / `.md` | C/D |
| `topology-coadaptation-materialization-receipts-v7.json` | C |
| `topology-canonical-frozen-pair-payloads-v7.json` | C |
| `v38-directional-event-insert-forensic-v7.json` / `.md` | A |
| `v38-multipanel-suboperation-v7.json` / `.md` | A |
| `v38-cumulative-event-child-archive-forensic-v7.json` / `.md` | F |
| `topology-case-study-inspected-task-authority-v7.json` / `.md` | G |
| `topology-case-study-inspected-task-matrix-v7.json` / `.md` | G |
| `topology-future-untouched-confirmation-authority-v7.json` / `.md` | H |
| `resource-future-untouched-confirmation-authority-v7.json` / `.md` | H |
| `resource-suboperation-selected-pair-receipts-v7.json` / `.md` | I |
| `resource-suboperation-one-plan-design-v7.json` / `.md` | I |
| `resource-suboperation-near-two-plan-design-v7.json` / `.md` | I |
| `topology-coadaptation-python-rust-parity-corpus-v7.json` / `.md` | E |
| `topology-coadaptation-executed-python-rust-parity-report-v7.json` / `.md` | E |
| `v38-followup-v7-go-nogo.json` / `.md` | J |
| `decision-memo-v7.md` | memo |
| `README-v7.md` | index |

Regenerate v7 with:

`uv run python -m autoresearch.temporal_qd_v38_followup_audit_v7 --output-dir research/temporal-qd/v38-followup`

