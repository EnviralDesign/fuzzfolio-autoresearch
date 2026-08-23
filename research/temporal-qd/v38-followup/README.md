# V38 evolve-everything follow-up artifacts

Generated locally from the V38 run and current source. No new market evaluation.

## v1

| File | Task |
| --- | --- |
| `indicator-parameter-evolution-coverage-v1.json` / `.md` | A |
| `v38-resource-suboperation-heritability-v1.json` / `.md` | B |
| `v38-topology-operation-audit-v1.json` / `.md` | C |
| `v38-initial-protection-tail-forensic-v1.json` / `.md` | D |
| `topology-coadaptation-matrix-spec-v1.json` | E |
| `topology-coadaptation-research-plan-v1.md` | E |
| `symmetric-protection-diagnostic-spec-v1.json` | F |
| `symmetric-protection-diagnostic-plan-v1.md` | F |
| `decision-memo-v1.md` | G |

Regenerate A–D with:

`python -m autoresearch.temporal_qd_v38_followup_audit --output-dir research/temporal-qd/v38-followup`

## v2

v1 files above are frozen. v2 corrects labels, metric identity, multi-panel summaries, event-insert and protection forensics, and the co-adaptation contract.

| File | Task |
| --- | --- |
| `indicator-parameter-evolution-coverage-v2.json` / `.md` | D |
| `v38-resource-suboperation-heritability-v2.json` / `.md` | A |
| `v38-multipanel-suboperation-v2.json` / `.md` | B |
| `v38-directional-event-insert-forensic-v2.json` / `.md` | C |
| `v38-topology-operation-audit-v2.json` / `.md` | E |
| `v38-initial-protection-tail-forensic-v2.json` / `.md` | F |
| `topology-coadaptation-matrix-spec-v2.json` | G |
| `topology-coadaptation-research-plan-v2.md` | G |
| `balanced-resource-suboperation-matrix-spec-v1.json` / `.md` | D |
| `decision-memo-v2.md` | memo |
| `README-v2.md` | index |

Regenerate v2 with:

`python -m autoresearch.temporal_qd_v38_followup_audit_v2 --output-dir research/temporal-qd/v38-followup`

## v3

v1 and v2 files above are frozen. v3 corrects event-insert mechanism labels, exact multi-panel economics, qd19 wording, phenotype breadth, the launch-grade resource slot manifest, and parent-bound topology-local co-adaptation.

| File | Task |
| --- | --- |
| `v38-directional-event-insert-forensic-v3.json` / `.md` | A/B |
| `v38-multipanel-suboperation-v3.json` / `.md` | C |
| `resource-suboperation-launch-manifest-v1.json` / `.md` | D |
| `topology-coadaptation-matrix-spec-v3.json` | E |
| `topology-coadaptation-research-plan-v3.md` | E |
| `v38-initial-protection-tail-forensic-v3.json` / `.md` | F |
| `decision-memo-v3.md` | memo |
| `README-v3.md` | index |

Regenerate v3 with:

`python -m autoresearch.temporal_qd_v38_followup_audit_v3 --output-dir research/temporal-qd/v38-followup`
