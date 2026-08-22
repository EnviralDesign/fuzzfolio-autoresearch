# V38 evolve-everything follow-up artifacts

Generated locally from the V38 run and current source. No new market evaluation.

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
