# Temporal QD campaign input

`temporal-qd-campaign-freeze` owns the single durable boundary between a
selected candidate cohort and native gateway dispatch. The current v5 path
accepts a sealed proposal evaluation population or a sealed prefinalizer cohort,
derives the deterministic candidate/window task matrix, and commits one compact
campaign-input checkpoint.

## Invocation

```text
temporal-qd-campaign-freeze --manifest PATH
```

The current manifest schema is
`temporal_qd_v5_native_campaign_freeze_manifest_v2`. There is no production
Python fallback.

## Durable output

A completed current-v5 freeze owns exactly three files:

- `screening-run/tasks.jsonl`
- `cohort-population.json`
- `campaign-input-checkpoint.json`

`campaign-input-checkpoint.json` is receipt-last. It binds the executed
manifest; runtime and semantic authorities; generation, campaign role, and
panel; campaign, evaluation-population, and task-matrix identities; candidate,
window, and task counts; the raw SHA-256 and byte length of both payloads; and
the exact source identities used to derive them.

The checkpoint also records non-semantic artifact measurements, including the
fixed payload-file count and logical bytes for the task pack and cohort
population. Those measurements are excluded from scientific identity.

## Restart behavior

Normal restart calls `open_v5_campaign_input_checkpoint`. It authenticates the
checkpoint and its two payloads and returns their fixed paths and identities. It
does not rebuild a result/transaction/receipt chain, rematerialize candidate
rows, or re-run campaign construction.

The successful artifact count is constant regardless of candidate or task
count. Payload bytes remain O(candidates + tasks), and the task matrix is
streamed rather than duplicated as an in-memory array and a second durable
representation.

## Trust boundaries

For current proposal and evolved publications, `evaluation-population.json` is
the exact qd-batch producer artifact: compact canonical JSON followed by one LF
and bound by its raw SHA-256. Semantically equivalent pretty-printed rewrites
are rejected.

Python-materialized template preparations and construction catalogs retain
their separately fenced pretty-JSON ABI. Their producer bytes are authenticated
at ingress; they are not silently converted into the qd-batch format.

Remote worker results are not handled here. They remain an external trust
boundary owned by gateway dispatch and the campaign-output checkpoint.

## Evidence-ladder compatibility

The explicit evidence-ladder v3 operations remain supported as a separate,
fenced compatibility path. They retain their own authority and receipt-last
artifacts and cannot be substituted for the current v5 campaign-input
checkpoint. The current production supervisor uses the checkpoint path above;
it does not route missing v5 behavior through the historical ladder or Python
campaign freezer.
