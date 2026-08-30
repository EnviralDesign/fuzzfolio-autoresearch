# V37/V38 historical coverage boundary

## What this atlas can establish

The listed historical AutoResearch commits resolve locally and were inspected
as source objects:

| Identity | Commit | Source finding |
| --- | --- | --- |
| V37 AutoResearch | `5fa623b88c641d4d886411bf195ee3ef386d6446` | Typed grammar contains 23 sealed fragments |
| V38 AutoResearch / verified default | `51c2f9175f441166e7fc997109e939a9f9103b5d` | Typed grammar also contains 23 sealed fragments |
| Accepted V4 audit | `38aa9ce00c3214620987883017a8ceffb67f11f5` | Context-free component score rejected; parent/suppression roles remain underidentified |
| FuzzFolio runtime engine | `2bd50ccb3af1700d286da88cbcaecb4aca24f1a2` | Read-only runtime/compiler/validator source pin |

The V37→V38 source diff under the active audit scope changes the generation
quality audit, operator-family report, fast-ephemeral batch support, and a
small native bridge change. It does not change the typed grammar source file
or its 23-fragment count.

## What this atlas intentionally does not establish

No historical run root, reduced outcome table, candidate panel, archive,
market data, worker log, gateway artifact, or economic metric was opened. The
following coverage fields therefore remain deliberately unavailable.

| Requested distinction | V37 | V38 | Why unavailable |
| --- | --- | --- | --- |
| Authored candidates | unavailable | unavailable | Requires proposal/journal corpus read |
| Structurally compiled candidates | unavailable | unavailable | Requires compiler receipt or candidate corpus read |
| Activated runtime behavior | unavailable | unavailable | Requires runtime/worker trace evidence |
| Reduced results | unavailable | unavailable | Requires reduced outcome artifacts |
| Selected / retained candidates | unavailable | unavailable | Requires archive/selection artifacts |
| Guard, indicator, timeframe, action, management use | unavailable | unavailable | Requires candidate plus runtime evidence, not source presence |
| Operator frequency, duplicate rate, never-sampled/dormant state | unavailable | unavailable | Requires proposal/selection telemetry |
| Side activity and side credit | unavailable | unavailable | Requires route/site-conditioned records and predeclared attribution |
| Portfolio contribution | unavailable | unavailable | Requires an authorized portfolio study |

This is not a data-quality failure. It is a hard stage boundary: source
presence can prove language availability; it cannot prove historical use,
behavior, retention, or causal contribution.

## Evidence vocabulary to retain in any later study

If a later authorized study opens the frozen historical corpus, its rows must
keep these states separate rather than derive one from another:

```text
authored → structurally valid → compiled/admitted → runtime activated
  → evaluated/reduced → selected/retained
```

It must additionally retain: parent identity and role, mutation operator and
suboperation, mutation site/route, attempt disposition/reason, child identity,
compiler receipt, runtime transition/action evidence, side, and selection
context. Without those joins, component, parent, suppression, or portfolio
credit is underidentified.

The non-market `Ground-Zero` design in
[ground-zero-delta-atlas.md](ground-zero-delta-atlas.md) is the earliest
allowed next step; it does not retroactively infer any of these fields.
