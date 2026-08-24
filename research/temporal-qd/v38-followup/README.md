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

## v4

v1, v2, and v3 files above are frozen. v4 adds the gross-versus-cost event partition, exact cumulative archive forensic, honest resource inventory, and complete 2x2 topology co-adaptation contract.

| File | Task |
| --- | --- |
| `v38-directional-event-insert-forensic-v4.json` / `.md` | A |
| `v38-multipanel-suboperation-v4.json` / `.md` | A |
| `v38-cumulative-event-child-archive-forensic-v4.json` / `.md` | B |
| `resource-suboperation-candidate-inventory-v1.json` / `.md` | C |
| `resource-suboperation-balanced-design-proposal-v2.json` / `.md` | C |
| `topology-coadaptation-matrix-spec-v4.json` / `.md` | D-G |
| `topology-coadaptation-materialization-receipts-v4.json` | F |
| `decision-memo-v4.md` | memo |
| `README-v4.md` | index |

Regenerate v4 with:

`python -m autoresearch.temporal_qd_v38_followup_audit_v4 --output-dir research/temporal-qd/v38-followup`

## v5

v1-v4 files above are frozen. v5 repairs pair-level receipts, native-validation labeling, receipt chaining, useful-innovation vs interaction, 4-window task math, five pair clones, frozen balanced plan IDs, exact archive gates, and a prepared confirmation-panel authority. No market compute was launched.

| File | Task |
| --- | --- |
| `v38-directional-event-insert-forensic-v5.json` / `.md` | A |
| `v38-multipanel-suboperation-v5.json` / `.md` | A |
| `v38-cumulative-event-child-archive-forensic-v5.json` / `.md` | A/B |
| `resource-suboperation-candidate-inventory-v2.json` / `.md` | F |
| `resource-suboperation-balanced-design-proposal-v3.json` / `.md` | G |
| `topology-coadaptation-matrix-spec-v5.json` / `.md` | B-E |
| `topology-coadaptation-materialization-receipts-v5.json` | B/C |
| `future-untouched-confirmation-panel-authority-v5.json` / `.md` | H |
| `decision-memo-v5.md` | memo |
| `README-v5.md` | index |

Regenerate v5 with:

`python -m autoresearch.temporal_qd_v38_followup_audit_v5 --output-dir research/temporal-qd/v38-followup`

## v6

v1-v5 files above are frozen. v6 is the last no-market contract pass: receiptId=slotId, fail-closed canonical pair compile, useful-innovation worst-window rule, frozen v38 policy pins, and experiment-specific confirmation authorities. No market compute was launched.

Regenerate v6 with:

`python -m autoresearch.temporal_qd_v38_followup_audit_v6 --output-dir research/temporal-qd/v38-followup`

