# temporal-qd-batch current v5 handoff

The current native proposal handoff is receipt-last and deliberately bounded.
The G0 result/receipt schemas are `temporal_qd_native_v5_proposal_construction_result_v5`
and `temporal_qd_native_v5_proposal_construction_receipt_v5`; evolved generations use
`temporal_qd_native_v5_evolved_construction_result_v3` and
`temporal_qd_native_v5_evolved_construction_receipt_v3`. Older schemas are historical
formats and are not accepted as alternate shapes of these current schemas.

The receipt embeds `temporal_qd_native_v5_proposal_output_inventory_v2`. Its public
artifact list is fixed by generation kind. Its `objectStore` is the bounded
`temporal_qd_native_v5_proposal_object_store_closure_v2`, containing only:

- `inventory`: a self-hashed `temporal_qd_native_v5_proposal_object_inventory_descriptor_v1`;
- `roots`: three G0 or four evolved role-addressed object descriptors required by the
  control-plane handoff;
- `objectStoreSha256`: the closure self-hash.

The full immutable-object closure is canonical LF JSONL at
`v5-native/object-inventory.jsonl`. Every strictly ordered row uses
`temporal_qd_native_v5_proposal_object_inventory_row_v1` and has exact fields
`schemaVersion`, `ordinal`, `relativePath`, `objectSha256`, `fileSha256`,
`byteLength`, and `rowSha256`. The descriptor seals its fixed path, row schema,
file SHA-256, encoded length, object count, aggregate object bytes, and its own
`descriptorSha256`.

Publication order is immutable objects/public artifacts, object-inventory sidecar,
`internal/v5-proposal/receipt.json`, then the invocation-local
`v5-proposal-result.json`. Fresh validation and sealed adoption stream the sidecar,
require contiguous ordinals and strictly increasing identities, authenticate every
declared object, reject namespace extras, and then run typed kernel replay. A committed
receipt can recreate the invocation result without reopening manifest input documents
or private publication fragments.
