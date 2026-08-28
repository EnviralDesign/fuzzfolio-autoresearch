# Native authority

The V37 run binds its generation finalizer through
`run/broad-4000x1024x5/native-finalization-authority.json`:

- implementation: Rust `temporal-qd-generation-finalizer`;
- retained executable SHA-256:
  `9f622a88ea2de02aac167f9cab380549fb9309cc50eb6d2261d6157d3511c2b3`;
- historical source commit:
  `5fa623b88c641d4d886411bf195ee3ef386d6446`;
- historical tail-reducer source is called through
  `aggregate_realized_behavior` by the generation finalizer.

The exact source workspace built successfully at that commit. Its new binary
is intentionally not used as the control executable because its bytes differ
under the current compiler environment. The retained executable does match the
V37 authority record bit-for-bit, so it is the exact runtime control.

The source inspection shows the finalizer passes each retained window
`realizedBehavior` to the Rust tail reducer, which constructs the cumulative
`identityMaterial` and `identitySha256`. Python's requirement for those fields
on the legacy per-window record is therefore a separate opening defect.
