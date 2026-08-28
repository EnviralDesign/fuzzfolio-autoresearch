# Native control verdict

Two fresh native replay roots were created from G1–G5 copied `source.json`
bytes plus a newly canonical manifest whose only semantic change was the
absolute source path and its derived manifest hash. Before each finalizer
invocation, the replay directory contained exactly `source.json` and
`manifest.json`.

G2–G5's frozen source documents contain their historical prior-state object as
part of the signed finalization source; that is preserved byte-for-byte because
it is an input contract. No historical cumulative/archive *file* was copied
into a replay directory or injected as a freshly produced replay result.

For both runs, all five regenerated cumulative archives and parent archives
were byte-identical to their historical counterparts. The comparable member
IDs, cells, lanes, robust-breeder classifications, archive identities, and
aggregate realized-behavior identities also matched exactly.

| Generation | Parent members | Result |
| --- | ---: | --- |
| G1 | 3 | exact |
| G2 | 3 | exact |
| G3 | 0 | exact |
| G4 | 0 | exact |
| G5 | 0 | exact |

The native control is now Variant 0. No counterfactual variant, market
evaluation, worker/gateway/Vast action, generation, historical archive
mutation, gate weakening, or policy rewrite was performed by this recovery
pass.

Two narrow negative controls also held:

- changing the copied source identity while retaining the historical manifest
  identity was rejected before reduction with `fast-ephemeral manifest/source
  identity drifted`;
- changing one source window's observation count, then correctly rehashing the
  window, bundle, source, and replay manifest, changed that candidate's native
  aggregate identity and the parent archive SHA. The historical comparison
  therefore failed as it should.
