# Summary Performance and Snapshot Cohort Integrity Design

## Decision

This change improves first-load latency and snapshot consistency without
changing any Fact definition. It deliberately uses the term **bounded-skew
cohort** rather than “same instant” or “atomic observation”: public exchange
REST responses and independent blockchains cannot be observed at one
mathematical instant.

The approved implementation has four parts:

1. replace three whole-payload deep copies with one copy-on-overlay boundary;
2. prewarm the default Screener Summary during dashboard startup;
3. publish each full depth/execution family through one failure-atomic bundle;
4. reject a public execution response when its lineage does not match the
   currently published depth cohort.

Funding Rate is excluded. Fee, gas, transfer-cost, and net-arbitrage Facts are
also excluded from this implementation; they need a separate Fact contract.

## Performance Boundary

`build_market_summary()` needs a selected-window payload and a full-history
catalog. Both payloads receive TVL, CEX-depth, and DEX-depth overlays. The
existing implementation deep-copies the complete payload at each overlay,
including immutable daily `price_points` arrays.

Add one helper that creates an overlay-safe payload:

- deep-copy metadata because every overlay appends sources and snapshot
  metadata;
- shallow-copy every CEX and DEX row because overlays change only top-level
  row fields;
- share nested daily-series arrays as read-only values;
- never mutate the input payload.

The three overlays use this helper instead of `copy.deepcopy(payload)`. Tests
must prove input immutability, nested-series identity sharing, and response
equivalence.

At startup, the server builds the default Summary once before serving normal
traffic. The warmup uses the same public builder, source signature,
generation fence, and freshness bucket as a real request. It creates no
persistent artifact and does not freeze freshness: the existing 60-second
serialized-response key remains authoritative. A warmup failure is logged and
the server continues so `/health` can expose the real data-readiness state.

## Publication Boundary

Depth and execution are derived from the same source observation within each
family:

- one CEX order-book response produces its depth row and ten execution rows;
- one DEX fixed-block pool state produces its depth row and ten execution rows.

Full publication must first validate both candidate inventories and both
coverage reports. It then prepares the depth history, depth latest/current,
and execution latest bytes and passes all public destinations to the existing
`atomic_replace_bundle()` helper. This is **failure-atomic for ordinary I/O
errors**: tests inject a failure at every replacement position and require all
pre-existing bytes to be restored. It is not process-crash atomic and does not
claim to be.

The private processed `current` files remain independently written because
they are not the public read boundary.

## Reader Fail-Closed Boundary

The public catalog already exposes one depth `snapshot_ids` set per family.
The execution snapshot exposes its own `snapshot_ids` and
`source_snapshot_ids`. Before publishing execution or selected-quality Facts,
the server requires, for each loaded family:

- exactly one nonempty depth snapshot ID;
- exactly one execution snapshot ID;
- exactly one execution source snapshot ID;
- all three IDs are equal;
- execution market count equals the published depth inventory row count.

Any mismatch raises a bounded service-unavailable error. The server must not
return a stable-looking response assembled from two publication cohorts. The
release checker independently enforces the same relationship.

Snapshot metadata adds `observation_span_seconds`, derived from canonical
minimum/maximum observation timestamps. This field measures cohort skew; it
does not redefine `snapshot_id` as an observation time.

## Non-Goals and Honest Semantics

This work does not make observations simultaneous. The current full cycle is
sequential across venues and chains; recent production evidence shows about
111 seconds of CEX span, 87 seconds of DEX span, and roughly five minutes from
the first CEX observation to the last DEX observation.

True cross-family reader/crash atomicity needs immutable generation
directories and one atomic manifest pointer. Tighter observation simultaneity
needs concurrent or streaming CEX capture plus per-chain blocks selected
around one declared target timestamp. Those are separate architecture changes,
not claims attached to this release.

## Verification

The release is accepted only when all of the following pass:

- copy-on-overlay immutability and golden-equivalence tests;
- default-warmup success and failure isolation tests;
- CEX and DEX full-bundle fault-injection tests;
- execution/depth lineage match and mismatch endpoint tests;
- release-checker lineage counterexamples;
- full unit suite and Python 3.8 grammar/import checks;
- production cold/warm Summary benchmark with payload size and generation
  equality recorded;
- preflight and post-cutover release checks against one stable generation.
