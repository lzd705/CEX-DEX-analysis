# Historical Foundry Replay Opportunity Design

## Decision

This increment delivers one complete, reproducible historical research loop for
Ethereum UNI/WETH across Uniswap V2 and SushiSwap V2. It scans the exact seven
days ending at a run-frozen `finalized` block, proves candidate round trips with
Foundry and an Anvil transaction receipt, selects the newest fully publishable
block whose ten scenarios all succeed and at least one has positive policy net,
publishes a separately namespaced historical bundle, and exposes that bundle in
the existing Opportunities page.

Each of the selected block's ten scenarios must complete with receipt status
one and is published as a `research_estimate`. Status-zero receipts may close a
non-selected candidate during scanning, but a block containing one is not
publishable. The published results are fixed-block counterfactuals under a
hash-bound state override that models a prefunded, predeployed, preapproved
executor. They are not current quotes, executable candidates, claims that the
edge was capturable in the original block, or permission to broadcast a
transaction.

The existing live Opportunity pointer, 120-second freshness projection, strict
CEX publication firewall, and fail-closed missing-data behavior remain intact.
Historical replay has its own pointer, manifest, API, cache identity, UI mode,
and verification boundary.

## Approved outcome

One accepted run has all of the following:

- chain: Ethereum mainnet, chain ID `1`;
- token pair: UNI/WETH;
- venues: Uniswap V2 and SushiSwap V2;
- routes: Uniswap buy then Sushi sell, and Sushi buy then Uniswap sell;
- notionals: exactly USD `1000`, `5000`, `10000`, `50000`, and `100000`;
- scan anchor: one immutable `finalized` block captured at run start;
- window: exactly 604,800 seconds before the anchor timestamp;
- scan order: block number descending;
- selected block: the newest fully resolved, fully publishable block whose ten
  scenarios all have status-one receipts and at least one has policy-net profit
  strictly greater than zero;
- simulation: one composed WETH to UNI to WETH transaction per scenario;
- publication: exactly ten historical Opportunity rows and ten Foundry proofs;
- classification: all ten rows are numeric `research_estimate`; unavailable,
  strict, executable, and attested counts are zero;
- positive gate: at least one published `research_net_edge_usd` is strictly
  positive and exactly equals the independently replayed policy-net result; and
- public behavior: the historical result is visible only through the historical
  Opportunity namespace and is labelled as a counterfactual replay.

If the complete seven-day window contains no fully publishable policy-positive
block, or any newer candidate remains unresolved, the run exits nonzero and
does not move the historical public pointer. It does not widen the window,
reduce costs, lower the MEV assumption, choose a user-supplied block, or publish
a gross-only winner.

## Baseline and scope boundary

The implementation branch starts at
`143acb344394368d2df1adbec3c9797ab60934bb`. At that baseline:

- the dashboard already reads hash-bound complete Opportunity bundles;
- the UI and API already distinguish strict candidates, research estimates,
  and unavailable routes;
- same-chain DEX-to-DEX routes already map to `atomic_onchain`;
- fixed notionals already use the required five-value grid;
- live Opportunity values become unavailable after the 120-second age SLA;
- strict public promotion accepts only a narrow CEX-to-CEX path;
- no Foundry project, executor, scan CLI, Sushi V2 authority, historical replay
  pointer, or historical Opportunity API exists; and
- no real Opportunity observation is currently published.

This increment does not add CEX Opportunity, flash loans, live execution,
private keys, mainnet broadcasting, multi-hop routing, V3/V4, additional
tokens, additional chains, dynamic notional optimization, route promotion, or
a production scheduler. It does not repair the broader 30-day CEX-volume
selection denominator because the MVP uses an explicit, isolated two-market
research universe rather than production ranking.

## Alternatives considered

### Reuse the live Opportunity feed

Rejected. The finalized anchor is older than the live 120-second SLA, so the
current projector correctly clears its economics. Disabling freshness for all
research estimates would make unrelated stale live estimates appear current.

### Run Foundry for every block

Rejected. Seven Ethereum days are roughly fifty thousand blocks, each with ten
scenarios. Exhaustively starting a fork for all blocks is unnecessarily slow
and creates avoidable RPC load.

### Publish reserve-math results without Foundry

Rejected. Integer V2 math is a useful conservative filter, but the agreed MVP
requires composed-route execution, actual token balance deltas, and a real
local transaction receipt.

### Recommended two-stage historical replay

Use complete header, reserve, feed, and fee coverage to perform exact integer
screening. Only blocks that cannot be safely excluded are replayed in Foundry
and Anvil. Historical publication remains separate from the live feed. This is
the smallest design that satisfies reproducibility, positive-net proof, and
honest product semantics.

## Fixed policy, authority, and toolchain

Three checked-in canonical files form one sealed policy boundary:

```text
config/historical_foundry_replay_policy.json
config/historical_foundry_replay_authority.json
config/historical_foundry_replay_toolchain.json
```

The policy contains the exact SHA-256 of the authority and toolchain files. A
run and every scenario bind all three physical hashes. The files are committed
atomically, use exact schemas, and reject unknown fields.

Their schema values are exactly
`historical_foundry_replay_policy/v1`,
`historical_foundry_replay_authority/v1`, and
`historical_foundry_replay_toolchain/v1`. Each file is canonical JSON with one
trailing LF, and its physical identity is SHA-256 of those exact bytes.
`policy_id` is the closed string `policy:<64hex>`, where the suffix is the typed
`historical_foundry_replay_policy_id/v1` hash of the exact policy bytes; it is a
derived evidence field and is not stored inside the policy file.

The policy fixes:

- chain ID `1`, anchor tag `finalized`, lookback `604800`, descending scan, and
  `newest_publishable_policy_positive` selection;
- the exact five-value notional grid and two venue directions;
- maximum ETH/USD age `3600` seconds with an inclusive boundary;
- post-`B` state basis plus the sealed state-override execution model;
- synthetic timestamp `B.timestamp + 12`, calldata deadline offset 60 seconds,
  router minimum-output values zero, transaction gas limit 2,000,000, empty
  access list, and fixed sender nonce;
- the scan-only closed-revert matrix: an independently prefilter-proved
  zero-output or zero-liquidity leg paired with the exact known V2 revert
  projection resolves that candidate scenario as non-publishable; every other
  status-zero result is a run error;
- exact EIP-1559 next-base-fee calculation, p50 acceptance tip, p90 stress tip,
  and `2 * next_base_fee + tip` max-fee envelope;
- this MVP configuration's 10 bps acceptance MEV and 25/50 bps stress MEV;
- strictly positive research policy net as the sole winner gate; and
- canonical fixed-point serialization with no economic rounding. Values whose
  exact denominator is a power of ten are stored without trailing zeros. Bps
  display fields continue to retain the existing exact numerator/denominator
  alongside the existing rounded display projection and never select a winner.

The authority file fixes the token, router, factory, feed proxy, sender and
executor derivations, V2 `997/1000` rule, executor storage-free runtime model,
UNI/WETH balance and allowance slot descriptors, account nonces, and the exact
state-override derivation. It contains no selected pair address; pairs are
derived from verified factories at runtime.

The toolchain file fixes Foundry archive version and SHA-256, Anvil/Forge/Cast
versions, forge-std full commit, solc version, EVM target, optimizer enabled,
optimizer runs 200, `via_ir=false`, metadata settings, and executor creation and
runtime build identities. No runtime flag may override these values.

The scan CLI does not expose MEV bps, price age, tip percentile, lookback,
notional grid, candidate block, timestamp, gas limit, access list, sender,
executor, direction, token, pool, venue, compiler, optimizer, or hardfork. A
change to any of them creates reviewed config bytes and new hashes.

MEV rates are policy values, not measured MEV. The schema permits any exact
non-negative policy rate; it does not impose a generic "MEV must be positive"
restriction. This checked-in MVP policy selects 10 bps for acceptance and 25/50
bps for stress, and the scan CLI cannot lower them. The two uniquely defined
stress cells reuse the successful baseline receipt's gas units:

```text
baseline  = p50 tip + 10 bps MEV
stress_25 = p90 tip + 25 bps MEV
stress_50 = p90 tip + 50 bps MEV
stress_robust = stress_25_net > 0 and stress_50_net > 0
```

More precisely, baseline gas is
`gasUsed * (child_base_fee + p50_tip)` and both stress cells use
`gasUsed * (child_base_fee + p90_tip)`. Their respective MEV deductions are
`notional * 10/10000`, `notional * 25/10000`, and
`notional * 50/10000`. The stress envelope is
`2 * child_base_fee + p90_tip` and is retained as a projection, not charged as
the paid price.

The executed trace must show that executor, routers, pairs, and tokens did not
execute the `GASPRICE` opcode; otherwise gas units cannot be reused for stress
and the scenario is invalid. Stress never selects a winner. A baseline-positive
but stress-negative result remains eligible and is labelled
`stress_robust=false`.

## Authority contract

The authority file contains these canonical lowercase Ethereum identities,
each encoded as `0x` plus exactly 40 hexadecimal digits. They are verified
against chain state at both the anchor and selected blocks:

| Object | Ethereum address |
| --- | --- |
| UNI | `0x1f9840a85d5af5bf1d1762f925bdaddc4201f984` |
| WETH9 | `0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2` |
| Uniswap V2 Factory | `0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f` |
| Uniswap V2 Router02 | `0x7a250d5630b4cf539739df2c5dacb4c659f2488d` |
| SushiSwap V2 Factory | `0xc0aee478e3658e2610c5f7a4a2e1777ce9e4f2ac` |
| SushiSwap V2 Router02 | `0xd9e1ce17f2641f24ae83637ab66a2cca9c378b9f` |
| Chainlink ETH/USD proxy | `0x5f4ec3df9cbd43714fe2740f5e3616155c5b8419` |

Pair addresses are never accepted from a caller or hard-coded as the primary
authority. At each authority checkpoint the verifier requires:

1. `eth_chainId == 1`;
2. nonempty bytecode for UNI, WETH, both routers, both factories, both derived
   pairs, the Chainlink proxy, and the current Chainlink aggregator;
3. each router's `factory()` and `WETH()` match the fixed identities;
4. each factory returns the same nonzero pair for `getPair(UNI,WETH)` and
   `getPair(WETH,UNI)`;
5. each pair's `factory()`, `token0()`, and `token1()` match the expected
   factory and unordered UNI/WETH set;
6. UNI and WETH decimals are exactly 18;
7. the Chainlink proxy description, decimals, current aggregator, phase, round,
   positive answer, and update timestamp are retained; and
8. the fixed executor account has empty code and the expected pre-overlay nonce
   at the selected state; and
9. every authority runtime byte sequence is retained by SHA-256 and Keccak-256.

The V2 30 bps rule is bound to verified pair/runtime authority and replayed swap
behavior, not inferred from a venue label. UNI balance slot 4 and allowance
slot 3, plus WETH balance slot 3 and allowance slot 4, are accepted only after
the existing retained authority records and live getters reproduce those
locations at the selected block.

## Block semantics

For a selected state block `B`, the fork contains Ethereum state after `B` has
finished executing. Before mining, the harness applies a sealed counterfactual
overlay that injects only:

- the exact hash-bound executor runtime at the fixed empty executor account;
- the fixed sender/executor nonces and required native balances;
- the scenario's prefunded WETH balance at the executor;
- the matching WETH contract native balance adjustment; and
- exact UNI/WETH allowances from the executor to the two fixed routers.

Every changed account, storage key, prior value, and new value is retained in a
canonical override set. Token getters must reproduce the override, both pairs'
balances/reserves must remain byte-for-byte unchanged, and no unrelated state
key may change. The executor has no mutable application storage.

The measured type-2 transaction is the first mined transaction after this
sealed overlay, at synthetic block number `B+1` and timestamp `B.timestamp+12`.
The execution claim is therefore
`historical_counterfactual_state_override_next_block`, not a canonical child of
the original mainnet state root and not a transaction that existed in block
`B`.

The child base fee is calculated exactly from parent `B` according to EIP-1559,
using parent base fee, gas used, gas limit, elasticity, and denominator. The
acceptance priority fee is the p50 reward returned for parent `B` by
`eth_feeHistory`; p90 is retained only for stress analysis. The transaction
envelope uses:

```text
maxPriorityFeePerGas = p50_tip
maxFeePerGas = 2 * child_base_fee + p50_tip
```

The charged gas price must equal the Anvil receipt's `effectiveGasPrice`, which
in this controlled first-mined-transaction model must equal
`child_base_fee + p50_tip`. `maxFeePerGas` is a cap and is never deducted as if
it were the paid price.

## Seven-day scan

### Freeze and coverage

The scanner reads `finalized` once and records its number, hash, parent hash,
timestamp, state root, gas limit, gas used, and base fee. It calculates:

```text
cutoff_timestamp = anchor_timestamp - 604800
```

It binary-searches block timestamps to find the first block whose timestamp is
greater than or equal to the cutoff. The inclusive scan range is that block
through the anchor. At run completion the anchor is read again by number; a
hash change rejects the run.

Every block in the inclusive range has a retained canonical header projection.
Header numbers and parent hashes must form an exact continuous chain. Every
fee-history row, price snapshot, and two-pair reserve snapshot must cover that
same block inventory. Fee history is fetched in deterministic contiguous
chunks, with p50 and p90 rewards, and its base-fee and gas-used projections are
cross-checked against the retained headers. Missing, duplicated, truncated,
reordered, or inconsistent requests or responses make the run `inconclusive`,
never `no_publishable_profitable_block`.

Window implementation is split at authority and retention boundaries in this
exact order:

```text
Task 3a pure/offline plan and projection
  -> Task 4a bounded spool, base handoff types, and test bridge
  -> Task 3b claimed-source bind, production bridges, capture, finalization, reconciliation
  -> Task 4b live-binding consume, immutable chunks, and held staging snapshot
  -> Tasks 5-7 prefilter, replay, and selection
```

Task 3a converts the Task-2a pure anchor fixture to one normalized eight-field
header, projects an ordered binary-search transcript plus a fresh predecessor/
lower witness, and builds one compact gapless-ID plan. It exposes staged
one-shot header then state descriptor factories; only the fully validated
header inventory may provide block hashes to reserve/price rows. Header probes,
witnesses, bulk headers, and the final reread share one request builder and one
success projector. Complete original roots share one semantic projector, so
production 413 children gain no independent typed meaning. For `H=A-L+1` and
`F=ceil(H/1024)`, `1 <= H <= 50_401`, window requests equal `4H+F+1`, and IDs
continue after Task-2a `1..48` and the `S` lower observations. Its output is
explicitly `fixture_only_nonauthorizing`; no pure mapping, copied schema/hash,
or test capability can authorize a spool, writer, chunk, or snapshot.

Task 4a exists before any production window collection. Storage is the sole
owner of the exact `_ProductionArchiveRpcExchangeTransfer`,
`_PendingHistoricalWindowSpoolReceipt`, committed
`_HistoricalWindowSpoolReceipt`, and
`_ProductionHistoricalWindowCapability` classes, including their closure
provenance, inaccessible constructors, private issuers, exact verifiers, and
single-consumer guards. Task 4a freezes only these base classes, the pending
state machine, and a storage-owned private test bridge. The issuer closures are
not exported; no production transfer/mint/consume bridge exists or it rejects
closed-unavailable in this slice. RPC and scan never receive an issuer callable
or define a same-named class. They import storage only function-locally, while
storage imports neither RPC nor scan at module load or runtime. The exact
`_issue_historical_window_exchange_transfer_for_test(*, spool,
exchange_projection, canonical_request_bytes, decoded_response_bytes)` bridge
invokes only the test transfer issuer for Task 4a's positive exact-type KAT and
makes no production-mint claim.

Task 4a owns one no-follow descriptor-held append-only spool. A transfer is only
a spool-bound transport object and never authorizes a historical window.
`append_transfer` writes one successful physical exchange exactly as
`uint64_be(request_len)||canonical_request_bytes||uint64_be(decoded_len)||decoded_response_bytes`,
fsyncs and descriptor-rereads it, and returns a pending receipt without adding
it to committed inventory. At most one pending receipt exists. Task 2b validates
pending receipt against transfer, commits it through
`commit_transfer(*, transfer, pending_receipt)`, validates the exact committed
receipt again, and only then records or returns the exchange. On failure or
cancellation it calls `abort_transfer(*, transfer, pending_receipt)` when
possible; abort truncates the uncommitted tail and reverses its provisional
quota debit, while abort failure terminalizes the spool. Bounded reread accepts
only committed receipts. Task 3b modifies storage after the exact RPC and scan
authority types exist, adding the spool-bound production transfer, receipt
verification, mint, and consume bridges. Final authority exists only after a
sealed spool, exact Task-2b finalization, and exact scan reconciliation.

The RPC-side Task-3b boundary atomically claims its own completely fresh Task-2b
production context and gives the scan scheduler only the claim and same held
opening-config object, without a path reload or raw context/scope exposure.
After that claim, the generic production batch is forbidden before HTTP. The
scan scheduler rebuilds
each complete closed-schema `anchor_stage`, `lower_observation`, or
`window_root` logical root internally; the root contains complete requests,
global logical index, segment/kind/root index, and derived 413 authority, so no
caller can provide a separate boolean or partial root. The claim-scoped wrapper
opens the existing exact explicit Task-2b logical scope over those complete
requests and owns enter/exit; the scan scheduler never accesses the raw context
or internal scope.

Task 3b freezes these exact production bridge contracts. Forward-string and
`Any` annotations keep Python 3.8 import direction; runtime authority still
requires exact types from the held bound module objects:

```python
def _bind_claimed_historical_window_scan_source_module(
    *, claim: "_ProductionHistoricalWindowRunClaim",
) -> None: ...

def _bind_claimed_historical_window_storage_source_module(
    *, claim: "_ProductionHistoricalWindowRunClaim", module: Any,
) -> None: ...

def _bind_claimed_historical_window_sources_to_spool(
    *, claim: "_ProductionHistoricalWindowRunClaim",
    spool: "_HistoricalWindowExchangeSpool",
) -> "_HistoricalWindowSpoolSourceBinding": ...

def _finalize_claimed_production_archive_rpc_run_for_historical_window(
    *, claim: "_ProductionHistoricalWindowRunClaim",
) -> "_ProductionArchiveRpcFinalization": ...

def _verify_claimed_historical_window_finalization(
    *, claim: "_ProductionHistoricalWindowRunClaim",
    finalization: "_ProductionArchiveRpcFinalization",
    expected_receipt_inventory_sha256: str,
) -> None: ...

class _HistoricalWindowSpoolSourceBinding: ...

class _HistoricalWindowExchangeSpool:
    def issue_transfer_from_bound_rpc(
        self, *, claim: Any, exchange_projection: Mapping[str, Any],
        canonical_request_bytes: bytes, decoded_response_bytes: bytes,
    ) -> "_ProductionArchiveRpcExchangeTransfer": ...
    def append_transfer(
        self, *, transfer: "_ProductionArchiveRpcExchangeTransfer",
    ) -> "_PendingHistoricalWindowSpoolReceipt": ...
    def verify_pending_receipt(
        self, *, transfer: "_ProductionArchiveRpcExchangeTransfer",
        pending_receipt: "_PendingHistoricalWindowSpoolReceipt",
    ) -> None: ...
    def commit_transfer(
        self, *, transfer: "_ProductionArchiveRpcExchangeTransfer",
        pending_receipt: "_PendingHistoricalWindowSpoolReceipt",
    ) -> "_HistoricalWindowSpoolReceipt": ...
    def verify_committed_receipt(
        self, *, transfer: "_ProductionArchiveRpcExchangeTransfer",
        receipt: "_HistoricalWindowSpoolReceipt",
    ) -> None: ...
    def abort_transfer(
        self, *, transfer: "_ProductionArchiveRpcExchangeTransfer",
        pending_receipt: "_PendingHistoricalWindowSpoolReceipt",
    ) -> None: ...

class _ProductionHistoricalWindowReconciliation: ...

def _reconcile_production_historical_window(
    *, claim: Any, finalization: Any, sealed_spool: Any,
    frozen_pre_ledger: Sequence[Mapping[str, Any]],
    plan: Mapping[str, Any], compact_projection: Mapping[str, Any],
) -> "_ProductionHistoricalWindowReconciliation": ...

def _verify_production_historical_window_reconciliation(
    *, reconciliation: "_ProductionHistoricalWindowReconciliation",
    expected_spool_identity: Any, expected_finalization_identity: Any,
) -> None: ...

class _SealedHistoricalWindowExchangeSpool:
    def mint_production_historical_window_capability(
        self, *, claim: Any, finalization: Any, reconciliation: Any,
    ) -> "_ProductionHistoricalWindowCapability": ...

class _ConsumedProductionHistoricalWindowCapabilityView: ...

def consume_production_historical_window_capability(
    *, capability: "_ProductionHistoricalWindowCapability",
) -> "_ConsumedProductionHistoricalWindowCapabilityView": ...
```

The reconciliation and consumed-view classes are closure-issued exact private
types, immutable, redacted, noncopyable, nonpickleable, nonserializable, and
lookalike-rejecting. The reconciliation binds the claim, spool and receipt
inventory, finalization identity, compact projection, and post-ledgers. The
consumed view contains only its already-bound Task-4b payload and is issued once.

The source-to-spool binder is the sole Task API that may reach
`claim._context._preflight.sources`. Exactly once after a fresh claim and before
the first logical root or transfer, it requires live held sources and an
active, unbound, nonterminal, empty spool. Within one verification boundary it
applies the fixed scan/storage module resolvers, reverifies every held source,
no-follow duplicates the scan-, storage-, and RPC-related source FDs plus
necessary ancestry descriptors, and rechecks FD identity, path/inode/file
identity, bytes, and SHA-256 before and after duplication. Caller-supplied
module/path/FD/hash/mapping inputs are impossible. The two module bind helpers
are binder-only resolver substeps and do not independently expose the source
authority.

Only that spool's private storage closure slot may accept the exact module
key/spec/object records and duplicated descriptors and issue one storage-owned
`_HistoricalWindowSpoolSourceBinding`; this is not a free issuer or a second
Task API. Acceptance also records the exact spool/binding membership in the
claim's private closure state, allowing the claim-only finalizer to require the
same live binding without accepting a spool or caller proof. The binding is
immutable, redacted, noncopyable, nonpickleable,
nonserializable, exact claim/spool-bound, and one-shot. Failure/cancellation
closes all partial duplicates exactly once and leaves no binding. Second bind,
pre-bind transfer/finalization/mint, cross-spool/claim, closed binding, and
forged module/object/FD/path/hash use fail closed.

Each successful attempt uses only
`spool.issue_transfer_from_bound_rpc(...)`, whose bridge checks the claim
against the live spool source binding's bound RPC module before invoking the
non-exported issuer for at most one transfer. Every issue, append, pending/
committed verification, commit, abort, and seal requires that same live
claim/spool binding. Task 2b appends it, calls `verify_pending_receipt`, commits,
calls `verify_committed_receipt`, then zeroes/releases both raw fields before
another attempt, root, or finalization. Task-2b finalization retains only
compact success records with
exact exchange/logical/attempt indices, request/wire/decoded metadata, and spool
member index/offset/length/hash; it contains no raw request/decoded body and no
`spool_receipt_sha256`.

The production pre-ledger has three ordered segments: three Task-2a anchor
stages at global logical indices `1..3`, `S` lower-observation singletons at
`4..3+S`, and zero-based window roots at `4+S+root_index`. Scheduling uses
`with scope:` and catches literal HTTP 413 inside that active scope only for a
Task-3a-authorized multirow header/reserve/price root. Its pending queue
continues left-first and depth-first under the same cumulative 8-MiB budget; no
child opens a new scope. Only a final successful child with an empty pending
queue permits normal exit and summary completion. Anchor/lower, singleton, fee,
final-anchor, disallowed 413, JSON-RPC text, 429, 5xx, timeout, and transport
failures are terminalized by the wrapper and leave by exception. A recoverable
413 creates no transfer or pending/committed receipt/member.

After the exactly-once source bind and semantic equality of the final normalized
anchor, Task 3b calls
`_finalize_claimed_production_archive_rpc_run_for_historical_window` once; only
that RPC closure can call the existing context finalizer. It then
seals and rereads the spool, reconstructs complete roots, replays the three
anchor stages, the full lower proof, staged headers, and staged state through
the same pure projectors, and verifies gapless global IDs and all compact
physical/typed ledgers and issues the exact
`_ProductionHistoricalWindowReconciliation`. Normal claimed-finalizer cleanup
may close the original RPC preflight source authority: the storage binding owns
independent duplicated source/ancestry descriptors and never dereferences that
closed object. `seal()` atomically moves the binding from active to sealed
spool. Only then can
`sealed_spool.mint_production_historical_window_capability(...)` obtain the
live binding's bound RPC/scan module objects, require exact claim/finalization/
reconciliation classes from them, call both bound verifier seams, verify the
same claim/spool/receipt-inventory/finalization identities and exact
`sealed + finalized + reconciled` state, and internally invoke storage's
non-exported issuer for one exact, single-consumer
production capability bound to the held config/source identities, reconciled
sealed spool, compact projection, post-ledgers, and the still-live source
binding—not merely its closed identity projection. A successful first mint
moves the binding into the capability. A failed first mint terminalizes the
sealed spool and revokes/closes the binding; repeat mint rejects without
copying or transplanting binding ownership already moved to the capability.
Direct issuer calls,
cross-spool/cross-claim inputs, pre-finalization/pre-reconciliation mint, and
repeat mint fail closed. A mapping, copied hash, boolean, lookalike, or forged
bound-module class cannot substitute for an authority object. Task 4b is the
only consumer: its scan bridge function-locally imports storage and calls
`consume_production_historical_window_capability`, using only the exact private
consumed view before materializing immutable raw
exchange and typed role chunks and issuing the held capture snapshot used by
Task 5. Consume moves the live binding into the consumed view and invalidates
the capability. That view is an intermediate private state in one storage-
managed consume/materialize transaction; return of the view alone is not
consume success and it cannot be abandoned. Before the staging snapshot is
issued, Task 4b rechecks actual
module key/spec/object/origin/file and every duplicate source/ancestry FD, then
moves the closed source-identity projection and necessary held-descriptor
ownership into the descriptor-held snapshot. It then revokes/closes the
consumed binding exactly once and closes duplicates not moved. Any failed or
cancelled bind, pre-finalization lifecycle, reconciliation, first mint, or
consume/materialization transaction closes its then-current binding/duplicates
exactly once and issues no downstream authority. After a successful ownership
move, repeat mint/consume and post-consume copy/transplant attempts reject
without altering the new owner's live state.

### Authoritative reserve snapshots

For every retained block hash, the scanner performs two archive `eth_call`
requests, one `getReserves()` call against each verified pair. Calls use the
EIP-1898 `{blockHash, requireCanonical: true}` selector. A provider that cannot
serve hash-bound archive calls cannot satisfy this MVP.

The run inventory therefore contains exactly two reserve results per block,
each bound to the request ID, block number/hash, pair identity, raw result hash,
decoded reserves, and pair timestamp. Responses may arrive out of order, but
their exact ID set must equal the request set. There is no carry-forward state.
`Sync` logs may be retained as diagnostics, but they are never authority for a
safe exclusion or the newest-publishable-positive proof because a range-log
response cannot prove that a provider did not silently omit a log.

### Authoritative ETH/USD snapshots

For every retained block hash, the scanner performs an archive `eth_call` to
the fixed Chainlink proxy's `latestRoundData()` using the same EIP-1898 block
selector. It retains the exact request/result hashes and decoded round ID,
answer, started-at, updated-at, and answered-in-round values. The answer must be
positive, `answeredInRound >= roundId`, and `updatedAt <= block.timestamp`; its
age at the state block must be at most the policy's inclusive 3,600-second
limit. Proxy description and decimals remain fixed, and selected-block
authority rereads the proxy's active aggregator and its runtime identity.

`AnswerUpdated` logs may be diagnostic inputs but never replace the per-block
proxy call. A phase transition is represented by the returned round ID rather
than reconstructed from logs. Missing or invalid price data for any block makes
coverage incomplete; it is not silently forward-filled.

For integer USD notional `N` and feed decimals `d`:

```text
amount_weth_in_wei = floor(N * 10^(18 + d) / eth_usd_answer)
```

UNI/USD is not used. The first swap's actual UNI balance delta is the input to
the second swap.

### Exact prefilter

For every covered block the scanner evaluates the fixed 2-by-5 scenario grid
using integer constant-product arithmetic:

```text
amount_out = floor(
  amount_in * 997 * reserve_out
  / (reserve_in * 1000 + amount_in * 997)
)
```

A block-scenario can be safely excluded before Foundry only when one of these
is proved:

- either swap cannot produce a positive output;
- gross final WETH is not greater than initial WETH; or
- the following exact rational USD upper bound on policy net is non-positive:

```text
gross_edge_usd
  - (21000 * child_base_fee * eth_usd_answer / 10^(18 + feed_decimals))
  - (requested_notional_usd * policy.acceptance_mev_bps / 10000)
  <= 0
```

The prefilter uses the same price record, units, and no-rounding rule as final
economics. The 21,000-gas/zero-tip term is a strict lower bound on actual route
gas cost, so the expression is an upper bound on final policy net. It never
compares WETH and USD amounts or performs a separate MEV conversion.

No heuristic gas estimate may prove a negative. Every scenario not safely
excluded becomes a candidate for exact replay.

### Candidate replay and selection

Every block-scenario has a canonical prefilter row with its header, reserve,
price, fee, notional, direction, integer swap projections, exact USD lower-bound
arithmetic, decision, and reason. Its decision is exactly `safe_excluded` or
`replay_required`. The offline evidence validator recomputes all ten rows for
every block; selection never trusts a stored boolean.

Candidate blocks are the blocks with at least one `replay_required` row and are
processed in descending order. For selection purposes, each block-scenario has
one resolution from `safe_excluded`, `replay_success`, or `closed_revert`.
A block is resolved only when all ten resolutions are closed. An unexpected
transport, fork, authority, receipt, or trace failure leaves the relevant row
and block unresolved and stops selection; an older winner cannot pass it.

The first fully resolved block with at least one baseline policy-positive
scenario and ten `replay_success` resolutions is the newest publishable
policy-positive selection. Every one of its ten scenarios receives an
independent Foundry/Anvil proof even if the prefilter had already proved it
non-positive. For the selected block, `replay_success` replaces `safe_excluded`
as the final resolution while the original prefilter decision remains retained.
A candidate block containing a scan-allowlisted `closed_revert` is fully
resolved but ineligible for publication, so scanning continues to the next
older candidate. Even if another scenario at that block is positive, the block
is retained as `nonpublishable_positive` and is explicitly outside the
`newest_publishable_policy_positive` claim. Unknown revert selectors, missing
revert data, or inconsistent receipts are run failures.

Selection evidence records zero unresolved blocks at or newer than the selected
block. Older candidates are not simulated after selection and are closed as
`not_needed_older_than_selected`; they are not counted as unresolved. If the
range is fully covered and every candidate is resolved without a publishable
positive result, the run records `no_publishable_profitable_block`, zero
unresolved candidates, exits nonzero for this MVP, and leaves the public
historical pointer unchanged.

These checks prove completeness relative to the retained archive-RPC responses.
They detect missing, extra, contradictory, or internally inconsistent
responses, but they are not a cryptographic proof against a malicious provider
that fabricates a self-consistent historical chain. The redacted provider
identity is therefore part of the evidence boundary.

## Foundry and Anvil execution

### Pinned toolchain

The initial immutable toolchain is:

- Foundry `v1.7.1` official `darwin_arm64` release archive, verified against its
  published SHA-256;
- Solidity `0.8.36`;
- forge-std `v1.16.1`, resolved and retained at its full Git commit; and
- an explicit hardfork matching the seven-day window.

Before the checked-in toolchain files are finalized, the bootstrap command
downloads the immutable archives, verifies their published hashes, and proves
that Anvil supports the active Ethereum hardfork and solc 0.8.36 with the fixed
settings. If Foundry `v1.7.1` cannot run the selected window's hardfork exactly,
bootstrap stops and the reviewed toolchain file must select another immutable
release. There is no silent fallback to an older EVM, compiler, or binary.
The first MVP requires the lower bound and anchor to fall in the same
hash-bound mainnet hardfork interval; a window crossing an activation boundary
closes as `fork_window_mixed` rather than simulating different blocks under one
EVM rule set.

The dashboard runtime does not install Foundry. Replay is an offline collection
and publication job. The first connected build downloads the pinned compiler
and dependency. Before evidence collection, two separate sealed bootstrap modes
must pass:

```bash
python3 -m scripts.bootstrap_historical_foundry_toolchain \
  --verify-offline-tests
python3 -m scripts.bootstrap_historical_foundry_toolchain \
  --verify-connected-kat
```

The offline gate covers compilation, constructor/runtime identity, direction,
authorization, residual-balance, and pure arithmetic tests without silently
skipping a fork. The connected gate proves the fixed-block fork behavior. The
wrapper supplies the sealed endpoint without persisting it in output. Both
modes open the reviewed project-local toolchain capability and construct the
fixed forge/Anvil/cast/solc command vectors internally. Ambient `PATH`, direct
binary invocation, caller executable paths, and caller argument suffixes are
forbidden as implementation and completion evidence.

### Solidity boundary

The repository adds:

```text
foundry.toml
foundry.lock
.gitmodules
foundry/src/TwoVenueV2Executor.sol
foundry/test/TwoVenueV2Unit.t.sol
foundry/test/TwoVenueV2Fork.t.sol
lib/forge-std/                         # pinned submodule
```

The executor has fixed immutable UNI, WETH, Uniswap Router02, Sushi Router02,
and authorized sender identities. A constructor KAT verifies each router's
factory and WETH identity and proves the intended approval setup. The exact
constructor arguments, creation bytecode, deployed runtime, and immutable
patches are retained. Historical receipt runs inject that already-built runtime
and the separately derived allowance overlay; they do not execute a deployment
transaction. The measured entrypoint accepts only:

```text
direction
amount_weth_in
```

The closed direction enum selects one of the two fixed router orderings. The
executor performs WETH to UNI on the first router, derives second-leg input from
its actual UNI balance delta, performs UNI to WETH on the second router, and
returns actual intermediate UNI and final WETH deltas. It rejects an unknown
direction, an unauthorized sender, and unexpected residual token or native
asset behavior. Both router calls use the fixed zero minimum output and the
fixed `block.timestamp + 60` deadline so negative but successful scenarios stay
observable instead of being selected away by a caller-controlled slippage
threshold.

### State override and measured transaction

Each scenario starts a fresh Anvil fork fixed at block `B`, disables automining,
and first captures Anvil's fork-base header. Number, hash, parent hash, state
root, timestamp, gas limit, gas used, and base fee must exactly equal the
retained canonical `B` header. Before any override, Anvil also rereads and
matches the selected-block runtime hashes and relevant getters/storage for the
tokens, routers, factories, pairs, and feed authority. Matching only the pair
reserves is insufficient.

The orchestrator then applies the exact policy-derived overlay before any block
is mined. It uses only the sealed Anvil methods needed to set code, balance,
nonce, storage, next timestamp, and next base fee. It injects the verified
executor runtime, prefunded WETH, WETH backing ETH, router allowances, sender
native balance, and fixed nonces. It rereads every changed account and slot and
proves both pools' reserves and token balances still match the authoritative
block-`B` snapshots.

The fixed executor address must have empty code, zero UNI/WETH balances, and
zero UNI/WETH allowances to both routers in canonical state `B`; nonzero prior
values reject the scenario instead of being overwritten. Its new WETH balance
is exactly `amount_weth_in`, and the WETH contract's native balance increases by
that same amount. WETH allowance to the first router is exactly
`amount_weth_in`; UNI allowance to the second router is exactly the independently
predicted first-leg UNI output. The two opposite router/token allowances remain
zero. The actual first-leg UNI delta must equal that prediction before it is
used as the second-leg input. This direction-specific four-slot matrix is
derived internally and no caller may supply an allowance value.

The sender is locally unlocked or impersonated by Anvil; no reusable signing
key is required or retained. The orchestrator submits exactly one type-2 route
transaction and mines exactly one block. Its receipt must be in block `B+1`, at
`B.timestamp+12`, and be that synthetic block's first and only transaction.
The measured gas includes intrinsic gas, executor overhead, both router calls,
token operations, and settlement. Deployment, inventory acquisition, and
approval setup are explicitly excluded counterfactual assumptions, represented
by the hash-bound overlay rather than hidden setup transactions.

Each scenario runs in a fresh process or a cleanly restored pre-mining snapshot.
The overlay set, calldata, transaction envelope, receipt, trace, and pre/post
balances are independently retained. A mutation to an account, slot, prior
value, new value, pool state, nonce, timestamp, or transaction position rejects
the scenario.

The exact preimage is retained at
`foundry/<block>/<scenario>/overlay.json` under schema
`historical_foundry_state_override/v1`. Its closed fields bind scenario key,
block number/hash/state root, executor runtime hash, and a canonically sorted
account-mutation list. Each mutation records address, mutation role, prior and
new code bytes/hashes where applicable, prior and new balance, prior and new
nonce, and a sorted list of storage slot/prior/new 32-byte words. It also binds
the post-write RPC readback and token-getter readback for every changed value.
The overlay typed hash is stored in the member, run manifest, compact scenario,
and ten-scenario overlay-set digest. A hash without this retained preimage is
not valid evidence.

## Economic calculation

All acceptance decisions use integers or exact decimal/rational arithmetic.
Binary floating point is not used.

```text
gross_profit_weth_wei = final_weth_raw - initial_weth_raw

gas_cost_weth_wei = receipt.gasUsed * receipt.effectiveGasPrice

gross_edge_usd =
  gross_profit_weth_wei * eth_usd_answer
  / 10^(18 + feed_decimals)

gas_cost_usd =
  gas_cost_weth_wei * eth_usd_answer
  / 10^(18 + feed_decimals)

mev_buffer_usd =
  requested_notional_usd * policy.acceptance_mev_bps / 10000

policy_net_edge_usd =
  gross_edge_usd - gas_cost_usd - mev_buffer_usd
```

For this checked-in policy, `acceptance_mev_bps` is 10. All economic
denominators are powers of ten, so the stored values are exact terminating
decimals. Canonical serialization uses fixed-point notation, strips trailing
fractional zeros and a trailing decimal point, normalizes zero to `0`, and
never emits an exponent. Publication does not round these authority values.
Any existing rounded bps display is derived afterward and cannot affect
selection. The strict positive comparison uses the exact rational value; exact
zero fails.

The acceptance cell must satisfy all of these:

- receipt status is one;
- route and balance-delta replay succeeds;
- `policy_net_edge_usd > 0`;
- published `research_net_edge_usd > 0`;
- the published and independently replayed policy-net values are equal;
- `opportunity_class == research_estimate`;
- `strict_eligible == false`;
- `strict_ready_for_publication == false`; and
- `publication_attestation_sha256 == null`.

## Cost-component topology

The atomic executor produces one transaction receipt. Gas is therefore one
route-level component and cannot be duplicated across the two legs.

Each DEX-to-DEX scenario has exactly nine cost rows:

| Grain | Component | Status |
| --- | --- | --- |
| buy | `pool_swap_fee` | `bounded_estimate`, embedded |
| buy | `router_or_integrator_fee` | `bounded_estimate` |
| buy | `token_transfer_tax` | `bounded_estimate` |
| sell | `pool_swap_fee` | `bounded_estimate`, embedded |
| sell | `router_or_integrator_fee` | `bounded_estimate` |
| sell | `token_transfer_tax` | `bounded_estimate` |
| route | `network_gas` | `assumed` |
| route | `rebalancing_or_transfer` | `not_applicable`, proved |
| route | `mev_buffer` | `assumed` |

The topology change applies only to `atomic_onchain`; non-atomic DEX legs keep
their existing leg-level gas contract. The selected status-one Phase-2
`result.json` contains one exact
`historical_foundry_cost_proof_inputs/v1` object with exactly `schema`,
`scenario_key`, `policy_sha256`, `receipt_sha256`, `trace_sha256`,
`adapter_proof_sha256`, `rows`, and `proof_inputs_hash`.
`proof_inputs_hash` is the typed hash over the other seven fields. `rows` is an
ordered nine-element array in the table order above. Every proof row has
exactly `grain`, `component`, `value_status`, `embedded`,
`amount_usd_exact`, `rate_bps_exact`, `proof_role`, and `proof_sha256`.
Phase 3 consumes this object and hash directly; it cannot reconstruct a private
variant, reorder/rename fields, or derive expected proof values from the public
cost rows being checked.

One shared module-private historical matrix validator is the sole low-level
source of the nine row identities, statuses, embedded bits, and expected
amount/proof comparisons. It is context-free and accepts no context, manifest,
stage, profile, member list, or source-root policy. In addition to the other
proof expectations it receives exact
`expected_pool_fee_amount_usd_by_leg`; both public embedded pool-fee amounts
must equal the corresponding `amount_usd_exact` values from the validated
Phase-2 object while remaining excluded from additive cost.

Only sealed wrappers in historical publication may invoke that low-level
validator. The producer/complete writer wrapper holds and first validates an
identity-sealed `HistoricalReplayBuildContext`, descriptor-rereads every bound
raw scenario member and validates the exact Phase-2 proof object/hash, then
issues a sentinel-guarded immutable `ValidatedHistoricalScenarioInputs`
capability. The context-free pure bridge consumes only that capability. The
writer rechecks the capability's held descriptor ancestry/currentness after
the pure call, derives scalar expectations from the same capability, and only
then invokes the low-level validator. The published-reader wrapper first
validates its held pointer/report/manifest/core/raw view, then loads and
validates the retained proof object/hash, then invokes the low-level validator.
Dashboard projection and both release paths consume the reader's validated
view and never import or call the low-level validator. Writer call-order tests
require `context_validated`, `raw_descriptors_reread`,
`proof_inputs_validated`, `scenario_capability_issued`, `pure_bridge_called`,
`capability_current_rechecked`, then `low_level_matrix_called`; forged or stale
contexts/capabilities and proof tampering fail before the low-level call. The
live profile keeps its current helper, inventory, and wrapper signatures
byte-for-byte and never accepts historical inputs.

The two 30 bps pool-fee rows are embedded in reserve outputs and are not
deducted again; their exact informational USD amounts remain proof-bound as
described above. Router fee and transfer tax are zero only when the composed
receipt, balance deltas, and verified adapter prove zero; they are not marked
`not_applicable`. The route gas amount equals the receipt gas cost exactly. The
MEV row is bound to the policy hash and equals the requested notional times the
policy rate divided by 10,000. Numeric MEV rates are exact and non-negative;
zero is valid only for a separately reviewed policy that actually specifies
zero. The current MVP policy requires exactly 10 bps. There is no generic
positive-MEV restriction.

Every published scenario has all nine numeric/structural rows above. Candidate
status-zero receipts remain only in raw scan evidence and never enter the
complete historical bundle.

## Immutable run evidence

Every attempt writes to a run-scoped immutable directory before publication:

```text
raw/historical-foundry-replay/<run_id>/
  run_manifest.json
  policy.json
  authority.json
  toolchain.json
  rpc/*.bin
  headers/*.json.gz
  reserves/*.json.gz
  prices/*.json.gz
  fees/*.json.gz
  scan/capture_inventory.json
  scan/prefilter/*.json.gz
  candidate_manifest.json
  typed/<market_key>/dex_pool_state.json
  typed/<market_key>/dex_usd_price_context.json
  typed_manifest.json
  foundry/<block>/<scenario>/overlay.json
  foundry/<block>/<scenario>/receipt.json
  foundry/<block>/<scenario>/trace.json.gz
  foundry/<block>/<scenario>/result.json
  selection.json
```

`run_manifest.json` binds:

- exact schema, run ID, repository HEAD, and policy, authority, and toolchain
  physical hashes;
- Foundry, Anvil, forge-std, solc, compiler settings, executor creation/runtime
  hashes, and active hardfork;
- safe UTC start/finish times;
- chain ID and redacted RPC endpoint identity hash;
- anchor and lower-bound number/hash/timestamp;
- exact inclusive block count and the full `block_count * 10` prefilter-grid
  count;
- chunk inventories with byte size, SHA-256, logical row count, and range;
- candidate, simulated, resolved, unresolved, reverted, and positive counts;
- selected block/scenario identity or a closed run status; and
- every error as a closed reason code without response bodies, URLs, paths, or
  credentials.

Every chunk inventory is a canonical list of safe relative member paths,
physical byte counts, SHA-256 values, logical role/count, and exact block range.
`market_key` is a path-safe typed hash of the canonical market ID, with the
market-ID mapping retained in `typed_manifest.json`; market IDs are never used
as unchecked path components.
Successful RPC records retain the exact canonical request body and the exact
bounded decoded response-body bytes, together with wire/decoded byte counts and
SHA-256 values, before projecting typed fields. This preserves the success
envelope needed for replay without storing the endpoint or HTTP headers.
Non-success response bodies are discarded and represented only by a closed
status/reason code.
The verifier opens members through retained descriptors, rejects links and
unexpected files, rereads all member bytes, and recomputes every logical count
and digest. No absolute filesystem path is evidence.

The private Task-4a spool and its transfer/receipts are staging substrate, not a
final run member or historical-window authority. Task 4b accepts only the exact
storage-owned production capability issued after Task-3b reconciliation,
calls `consume_production_historical_window_capability` once, accepts only its
exact `_ConsumedProductionHistoricalWindowCapabilityView`, rereads every
committed framed spool member, and writes no-replace immutable
`rpc/*.bin` chunks that concatenate whole frames without splitting one. Each raw
chunk is at most 16 MiB; typed header/reserve/price/fee canonical-gzip chunks and
the canonical capture inventory are at most 16 MiB decoded. The capture
inventory joins every raw frame to the exact compact finalization record,
physical leaf/root ledger, typed role/count/digest, and continuous request-ID
range. It also binds the exact three copied config bytes and the finalized source
identity. Consume additionally requires the capability's same live exact
`_HistoricalWindowSpoolSourceBinding`; a copied closed identity projection is
not authority. Before snapshot issue, the writer rechecks the exact bound
module objects and independent duplicate source/ancestry descriptors, transfers
their closed projection and necessary descriptor ownership into the held
staging snapshot, and then revokes/closes the consumed binding exactly once. A
Task-3a projection, fixture, transfer, direct spool, copied
capability properties, caller row mapping, or caller relative path is rejected
before the writer opens or a quota debit occurs.

The writer fsyncs and descriptor-rereads every capture member, independently
reparses/reprojects its contents, freezes the complete `rpc/`, `headers/`,
`reserves/`, `prices/`, `fees/`, configs, and `scan/capture_inventory.json` role
set, then issues a read-only `HistoricalRunStagingSnapshot`. Task 5 may read only
that frozen inventory while the same private writer later appends and freezes
prefilter, scenario, candidate, typed, and selection role sets. `run_manifest.json`
is created exactly once as the final staged member; the writer is then revoked,
all members are reread, and the directory is renamed no-replace. Neither spool
conversion nor a later role freeze weakens an earlier snapshot or resets the
run-wide resource ledger.

`selection.json` proves either:

- `found_publishable_profitable_block`, with the newest fully publishable
  selected block, every newer `nonpublishable_positive` block explicitly
  inventoried, and zero unresolved candidates at or newer than it, plus older
  candidate rows closed as `not_needed_older_than_selected`; or
- `no_publishable_profitable_block`, with 100% coverage and zero unresolved
  candidates.

It binds the full canonical prefilter-grid digest and the candidate-resolution
inventory. Only `found_publishable_profitable_block` can advance this MVP's
historical public pointer.

### Compact replay evidence

The published bundle contains one exact `historical_foundry_replay_evidence/v1`
projection so the dashboard never reads the large raw run directly. Its closed
top-level field set is:

```text
schema
replay_id
route_cohort_id
run_id
policy_id
policy_sha256
authority_sha256
toolchain_sha256
run_manifest_sha256
selection_sha256
temporal_scope
execution_claim
selected_block
overlay_set_sha256
scenario_count
scenarios
scenario_set_sha256
```

`selected_block` is the exact number, hash, parent hash, timestamp, state root,
gas limit, gas used, base fee, synthetic child number/timestamp/base fee, p50 and
p90 tips, and block-bound ETH/USD round projection. The scenario list contains
exactly ten canonical rows sorted by `(route_id, requested_notional_usd)`.
Because prefunding depends on notional, each scenario has its own overlay hash;
the top-level `overlay_set_sha256` is the typed hash of the ordered ten-row
`(scenario_key, overlay_sha256)` inventory, not one overlay reused across
notionals.
Each row binds:

- schema, scenario key, Opportunity ID, route ID, direction, notional, and
  successful execution status;
- policy, authority, toolchain, block, overlay, executor creation/runtime,
  calldata, transaction, receipt, trace, and result hashes;
- exact `proof_inputs_hash`, equal to the typed
  `historical_foundry_cost_proof_inputs/v1` hash reread from that scenario's
  retained result member;
- sender, executor, nonce, type-2 gas limit, empty access list, max priority fee,
  max fee, calldata bytes, transaction hash, and transaction index;
- receipt status, block number/hash, gas used, effective gas price, and exact
  receipt projection hash;
- initial/input/intermediate/final raw token balances and exact gross WETH
  delta;
- gross, gas, MEV, policy-net, stress-25, stress-50, and `stress_robust` values;
  and
- a closed list of source members, each with role, safe run-relative path, byte
  count, and physical SHA-256 for its overlay, receipt, trace, and result.

Every compact row requires receipt status one and classification
`research_estimate`. Candidate-block status-zero evidence remains in the raw
run and cannot enter `replay_evidence.json`. The scenario-set digest, run
manifest, and bundle manifest bind the canonical rows. The local integrity
verifier rereads every referenced run member and recomputes all hashes,
projections, and arithmetic before a bundle may publish; connected replay is a
separate gate described below.

A raw run cannot be deleted while any retained historical bundle references its
run ID and manifest hash. Garbage collection first inventories every retained
historical complete-bundle manifest, every retained historical core-bundle
manifest, and both historical pointers, then deletes only unreferenced runs.
The active historical pointer's hash-bound verification report is retained with
it. An orphan report is not independent authority and does not by itself pin a
raw run; it may be removed only after no retained pointer references its
physical SHA-256. Failure to validate any inventory leaves the run and report
in place. This reference-aware retention rule is part of the MVP, not deferred
operations.

## Bridge into Opportunity economics

A pure module, `scripts/historical_foundry_replay.py`, accepts no filesystem,
network, subprocess, clock, URL, or caller-supplied economic policy. It exposes
narrow functions that:

1. validate canonical run evidence and toolchain/policy authority;
2. replay the selected block's two pools, ten scenario outcomes, gas, MEV, and
   exact positive gate;
3. build an isolated two-leg, two-route research universe and core cohort;
4. construct ten quantity, USD, cost, and atomic mode-evidence inputs; and
5. validate historical bundle closure before returning immutable projections.

The bridge recomputes route IDs, opportunity IDs, pair math, WETH/UNI deltas,
gas, USD conversion, component amounts, hashes, and positive count. It never
accepts serialized `QuantityQuote`, `CommonTarget`, Opportunity rows, profits,
or status flags from Foundry output as authority.

The bridge constructs a fixed two-market research universe from verified
authority, not from the production ranking selector. The existing universe
schema still requires a 30-calendar-day selection-window lineage: its end date
is the UTC date of the frozen anchor and its start date is 29 days earlier. This
field satisfies schema lineage only and is explicitly not a claim that the
seven-day replay has 30 days of volume coverage.

Historical core uses a sealed historical universe/publication profile and does
not invoke the route-shadow universe, baseline-manifest, phase, or joint-shadow
validators. Those contracts require a real 30-day ranking baseline and remain
unchanged for live Shadow. The historical profile validates its explicit two
markets, provenance window, selected-block members, and fixed routes directly.

The two historical DEX legs derive their market IDs, token identities,
collector context, reserves, 100-bps depth, pool TVL projection, side, and
execution capability from the selected-block replay. The maximum successfully
proved notional is the capacity projection; DEX 24-hour volume and route-volume
inputs remain null because this run does not measure them. Candidate generation
binds the policy, authority, raw-run manifest, and selected block. A sanitized,
credential-free endpoint identity is retained; a usable URL or provider secret
is rejected.

Depth and TVL reuse the repository's existing V2 integer projection, token
decimals, and fixed-point serialization. UNI/USD for this schema-only TVL
projection is the selected pool's reserve-implied UNI/WETH ratio multiplied by
the same block-bound ETH/USD answer; it is not an independent UNI price claim
and never enters replay profit or the winner gate.

For each market the bridge also creates the existing canonical
`dex_pool_state` and `dex_usd_price_context` typed members from selected-block
reserve and feed evidence. Their bytes, physical hashes, logical generations,
and descriptor inventory live under the same historical raw run. Historical
core publication rereads those bytes through an explicit historical raw-root
argument; it neither imports nor writes `raw/route-cohort`.

Raw-member replay is owned by the same sealed publication profile: live
publication retains its current `accepted/<market-hash>/response.json` reader,
while historical publication accepts only the manifest-inventoried typed and
replay members under `raw/historical-foundry-replay`. Neither path falls back
to the other's layout.

Core publication is independently namespaced:

```text
routes/historical/core/latest.json
routes/historical/core/bundles/<route_cohort_id>/
  manifest.json
  route_candidates.csv
  route_cohort.sqlite3
  route_legs.csv
  route_timing.csv
raw/historical-foundry-replay/<run_id>/
```

Its exact pointer is `route_historical_replay_core_pointer/v1`, its bundle stage
is `route_historical_replay_core/v1`, and its manifest schema is
`route_historical_replay_core_manifest/v1`. The pointer fields are exactly
`schema`, `bundle_stage`, `route_cohort_id`, and `manifest_sha256`. The bundle
keeps the existing five core filenames and the existing candidate, leg, timing,
and SQLite row schemas, but the historical manifest additionally and exactly
binds temporal scope, execution claim, policy/authority/toolchain, raw-run
manifest, selection, selected block, and historical raw-reader identities.
Those identities must replay before the shared core row/SQLite/hash primitives
are invoked.

Dedicated `publish_historical_replay_core` and
`load_latest_historical_replay_core` wrappers are hard-wired to these schemas,
stage, inventory, and explicit roots. They do not call the live
`publish_route_cohort_bundle` or `load_latest_route_cohort` wrappers and cannot
accept a profile string. The historical entrypoint must pass its roots
explicitly and is forbidden to fall back to the live
`DEFAULT_ROUTE_CORE_ROOT`.

Only two module-private historical-core loaders can construct the frozen
`HistoricalReplayBuildContext`:

- `load_validated_historical_replay_core_at` consumes held descriptors for a
  fully written, fully validated staged core bundle plus its exact prospective
  canonical core-pointer bytes, without reading or moving `latest.json`; and
- `load_latest_historical_replay_core` consumes the committed historical-core
  pointer and immutable bundle.

Both bind the exact core pointer bytes and SHA-256, core manifest/stage,
raw-run manifest, policy/authority/toolchain, selection, selected block, and
historical source reader. Their constructors are guarded by a module-private
sentinel; ordinary callers cannot manufacture a context or choose a profile.
For identical bytes the staged and committed contexts must be byte-equivalent.
Complete-bundle production requires one of these contexts, and the dedicated
writer retains it while validating all ten proof-input objects and cost groups.
The context is never passed into the low-level nine-row validator. Published
bundle readers instead construct a separate sealed validated view only after
pointer/report/manifest/core/raw validation and use that view to load proof
inputs before their wrapper reaches the same context-free validator. Tests
instrument both wrappers and prove that context/view and proof validation
strictly precede the sole low-level call.

Success and failure tests take byte snapshots of both live pointers,
`routes/core/latest.json` and `routes/latest.json`, and require them to remain
unchanged.

The I/O entrypoint, `scripts/run_historical_foundry_replay.py`, owns RPC,
subprocesses, immutable files, staging, publication, and final reread. Its
production commands are:

```bash
python3 -m scripts.run_historical_foundry_replay scan \
  --data-dir "$MARKET_DATA_DIR" \
  --publish

python3 -m scripts.run_historical_foundry_replay scan \
  --data-dir "$MARKET_DATA_DIR" \
  --dry-run

python3 -m scripts.run_historical_foundry_replay verify \
  --data-dir "$MARKET_DATA_DIR" \
  --bundle /absolute/path/to/immutable/historical/bundle
```

`scan` and `verify` require an archive RPC endpoint in `DEX_DEPTH_RPC_ETH`; this
workflow has no default provider and never stores the raw endpoint. Connected
verification is one sealed engine invoked in a new process after the raw run
manifest and complete historical bundle have both been finalized and
descriptor-reread, but before the historical public pointer moves.

The connected verifier does not trust the bundle's
newest-publishable-positive assertion.
It opens a fresh archive-RPC connection, refetches the referenced seven-day
block/reserve/price/fee inventory, recomputes every prefilter row, and derives
an exact `connected_verification_scenario_set`. That set contains:

- all ten scenarios at the selected block; and
- every original `replay_required` scenario at every newer candidate block.

Safe-excluded rows are independently recomputed but do not require a fork. Each
scenario in the verification set is replayed in a fresh fixed-block fork and
must reproduce its original success or allowlisted closed-revert status,
receipt/trace identity, balance deltas, policy net, and final resolution. This
is what proves that the selected publishable positive block is the newest
publishable one relative to the retained provider responses and that every
newer positive block is allowlisted nonpublishable; replaying only the selected
ten is
insufficient. The verifier compares the raw run, private core, staged complete
bundle, replay-evidence projection, and an exact canonical pointer-core object.
The pointer core is the canonical final-pointer JSON object with exactly
`verification_report_sha256` removed. It therefore contains exactly `schema`,
`bundle_stage`, `replay_id`, `route_cohort_id`, and `manifest_sha256`, and its
`schema` remains `route_historical_replay_pointer/v1`.
`route_historical_replay_pointer_core/v1` is only the internal typed-hash domain
label; it is not a JSON field value. One shared remove-one-field canonicalization
helper is used by the verifier, writer, API, and release checker. The verifier
may confirm or reject the winner but cannot choose another block or move a
pointer.

Only a successful verifier may create the immutable report. Its canonical
bytes are hashed first and then stored at:

```text
routes/historical/verifications/by-sha256/<verification_report_sha256>.json
```

The report has exact schema `route_historical_replay_verification/v1` and binds
its verification ID, pointer-core SHA-256, replay/cohort/manifest/run
identities, policy/authority/toolchain hashes, verifier source/toolchain
identity, redacted provider identity, complete coverage and prefilter-grid
digests, candidate-resolution digest, verification-scenario-set digest/count,
selected block, ten published scenario digest, start/finish times, and
`status=verified`. It contains no URL, credential, arbitrary response body,
path, or exception text. The report is written no-replace, descriptor-reread,
and immutable. A pre-existing report is accepted only when the filename equals
its physical SHA-256 and its exact bytes and all referenced identities match
the new verification result.

The final pointer is the pointer core plus exact field
`verification_report_sha256`. Thus the report binds the pointer's economic and
bundle identity while the final pointer binds the report bytes, without a hash
cycle.

After the report is durable, the parent constructs the final pointer,
descriptor-rereads the raw run, core, staged bundle, pointer core, final pointer
bytes, and report, and requires unchanged ancestry, identity, size, and
SHA-256. Only then may it atomically move the historical pointer. The API and
release checker resolve the report only from the SHA-256 in the validated
pointer and fail closed if the file is absent, changed, invalid, or mismatched
to the recomputed pointer core. Thus no unverified pointer becomes publicly
usable and no rollback protocol is needed.

For an already published bundle, public `verify --bundle` reruns the same
connected engine in audit-only mode, compares the retained report, and emits a
canonical completion result without modifying the raw run, bundle, report, or
pointer. It first pins the current historical pointer, requires the explicit
bundle path to identify that pointer's exact `replay_id` directory, and obtains
the report only through the pointer's hash field; it cannot audit an arbitrary
unreferenced directory under weaker semantics. A new process and connection
provide implementation independence, not an assertion that the configured
provider is an independent or cryptographically trustworthy data source.

The local integrity validator is deliberately narrower: it can verify stored
bytes, hashes, schemas, projections, and arithmetic with no network, but it does
not claim to reproduce Ethereum execution from a complete state database.
`--publish` and `--dry-run` are required mutually exclusive scan modes. Dry-run
validates and stages all logical results but does not move any core or complete
pointer. It uses only the staged-core loader/context, runs connected
verification against staged artifacts, validates the would-be report bytes in
the staging area, and does not install that report under
`routes/historical/verifications`.

## Historical publication

Live publication remains:

```text
routes/latest.json
routes/bundles/<cohort_id>/...
```

Historical publication uses:

```text
routes/historical/latest.json
routes/historical/bundles/<replay_id>/
  route_legs.csv
  cost_components.csv
  route_opportunities.csv
  route_cohort.sqlite3
  replay_evidence.json
  manifest.json
```

The historical bundle has six total files; the manifest inventories the other
five. Economic CSV/SQLite rows are validated by extracted, shared complete
bundle validators rather than a copied weaker implementation.

Complete publication has exactly two sealed profiles. Readers select one
internally only after the pointer schema and manifest stage have been
validated. The existing live writer selects the live singleton internally;
the dedicated historical writer requires the module-private identity-sealed
`HistoricalReplayBuildContext` produced by the validated historical core
loader:

- the live profile keeps the existing live filenames, member count, manifest
  schema, raw-member reader, Opportunity fields, timing projection, and cost
  topology byte-for-byte; and
- the historical profile requires exactly the five inventoried members above
  plus `manifest.json`, the historical manifest/stage, the historical raw
  reader, the nine-row atomic topology, and the no-current-clock historical
  timing projection.

The historical writer owns the validated build context and consumes each
scenario's exact `historical_foundry_cost_proof_inputs/v1` object before it can
serialize CSV or SQLite. The historical reader owns the validated published
view and rereads the same retained objects before returning rows. Both compare
all nine public rows, including both embedded pool-fee informational amounts,
to the proof object and `proof_inputs_hash` through the sealed wrappers defined
above. Neither dashboard nor release code reconstructs a parallel proof or
calls the context-free low-level validator directly.

No public function accepts a profile name, profile object, member list, source
root default, or timing policy from its caller. The implementation extracts
the current CSV, SQLite, row-schema, hash, descriptor, and referential-integrity
checks into shared primitives and composes them through the two sealed internal
profiles. It does not call the existing monolithic live validator with relaxed
fields, and it does not copy a smaller historical validator.

Concretely, existing `_validate_complete_route_bundle_at`,
`_complete_manifest_payload`, `_complete_artifact_bytes`, and
`load_latest_complete_route_bundle` remain live-only wrappers with their
current signatures and contracts. Shared descriptor, CSV, SQLite, hash,
foreign-key, row, and identity checks are extracted beneath them. New
historical-only writer/reader wrappers locate bundles by `replay_id`, require
the five manifest-inventoried members plus `manifest.json`, invoke those shared
primitives, and perform the replay-evidence join. A historical wrapper is
forbidden to call a live wrapper, and a live wrapper cannot accept historical
stage, inventory, raw roots, or timing semantics.

The existing `route_legs.csv`, `cost_components.csv`,
`route_opportunities.csv`, and `route_cohort.sqlite3` row schemas remain
unchanged. Foundry block, overlay, receipt, trace, executor, baseline, and
stress fields live only in `replay_evidence.json`. The historical validator
requires an exact one-to-one join from its ten replay scenarios to the ten
Opportunity rows by `opportunity_id`, with equal route ID, direction, notional,
classification, economic amounts, and cost-component references. Missing,
extra, duplicated, or orphan replay rows reject the whole bundle.

The exact pointer is:

```json
{
  "schema": "route_historical_replay_pointer/v1",
  "bundle_stage": "route_historical_foundry_replay/v1",
  "replay_id": "replay:<64hex>",
  "route_cohort_id": "cohort:<64hex>",
  "manifest_sha256": "<64hex>",
  "verification_report_sha256": "<64hex>"
}
```

The exact historical manifest binds:

- schema `route_historical_replay_manifest/v1`;
- stage `route_historical_foundry_replay/v1`;
- replay and cohort IDs;
- `historical_core_manifest_sha256` and
  `historical_core_pointer_sha256` from the identity-sealed build context;
- `temporal_scope=historical_replay`;
- `execution_claim=historical_counterfactual_state_override_next_block`;
- policy, authority, toolchain, selection, and run-manifest SHA-256;
- fixed notional grid;
- route count two, leg count two, scenario count ten, cost count ninety,
  Foundry-verified count ten, research-estimate count ten, unavailable,
  strict, executable, and attested counts zero, and positive count at least
  one; and
- physical and logical identities for all five members.

`Foundry-verified` means the block, overlay, transaction, status-one receipt,
trace, and result proof closed successfully. It does not mean the economics
were positive; verified negative research rows remain in the ten-row bundle.

Publication stages the complete directory, rereads and verifies it, and then
atomically replaces only `routes/historical/latest.json`. Any failure leaves
the previous historical pointer byte-for-byte unchanged. A successfully
published historical private core followed by a complete-bundle failure may
remain under `routes/historical/core` for diagnosis, but it is not reachable
from the public historical pointer and never changes either live pointer.

Historical complete loading does not require the mutable historical-core
`latest.json` still to name the same cohort. It resolves the immutable core
bundle by the complete manifest's `route_cohort_id` and
`historical_core_manifest_sha256`, fully validates that bundle, reconstructs
the canonical historical-core pointer bytes, and requires their SHA-256 to
equal `historical_core_pointer_sha256`. A later historical-core run therefore
cannot detach or reinterpret an older complete replay.

## Historical API

The live endpoint remains unchanged:

```text
GET /api/markets/opportunities
```

Historical replay adds:

```text
GET /api/markets/opportunities/historical
```

It supports the existing token, venue, notional, class, route-type,
availability, sort, and direction filters. `class=strict` is valid and returns
an empty inventory. The historical response has contract version
`opportunity_historical_summary/v1` and includes:

- temporal scope and execution claim;
- replay, cohort, manifest, policy, and run identities;
- selected block and simulation basis;
- route/scenario/verified/research/positive/returned counts and zero
  unavailable/strict/executable/attested counts;
- `freshness.applicable=false`, reason `historical_replay`, and no next wall
  clock deadline; and
- per-row Foundry verification, block, receipt, trace, gas, executor-model,
  baseline-net, and stress projections.

Historical projection verifies state skew and component validity against the
replay simulation time, not the current wall clock. It does not call the live
120-second timing projection or response-deadline retry loop. `available`
is the only valid row-level availability in a published MVP bundle and means a
status-one verified historical replay is present; it never means available now.

Historical response caching keys include the reread pointer identity, the
pointer-bound verification-report SHA-256 and stable descriptor identity,
manifest SHA-256, bundle member identities, contract version, and normalized
filters. Before even consulting a cached payload, the handler descriptor-opens,
rereads, and validates the pointer and report and requires the report's physical
hash and pointer-core binding. It rechecks their stable identities before
returning a cache hit. Deleting, replacing, or mutating a warm-cache report
therefore returns fail-closed rather than old rows. A pointer, report, or member
change invalidates the cache before a response is served; no minute bucket can
keep the previous replay visible after an atomic pointer move.

In addition, the process records a set-if-absent association from each observed
pointer physical SHA-256 to the first fully validated physical publication
signature covering pointer, report, manifest, immutable core, five complete
members, and retained raw members. Ordinary response-cache invalidation never
clears or overwrites this association. If the same pointer bytes/SHA are later
observed with any different descendant descriptor identity, including an
identical-byte inode replacement whose content hash still matches, the handler
returns HTTP 503. Only a genuinely new pointer SHA may establish a new physical
signature. Pre-cache and pre-return probes both enforce this guard.

Missing historical pointer returns HTTP 200 with no inferred route inventory
and reason `historical_replay_pointer_absent`. A malformed, hash-invalid, or
semantically invalid historical bundle returns HTTP 503 and no stale or partial
rows. Historical failure never affects the live endpoint, and live failure
never affects the historical endpoint.

## UI behavior

The Opportunities page gains a page-scoped control:

```text
Current | Historical Replay
```

The URL state is `opportunity_scope=current|historical`. It is distinct from
the market-inventory Current/Historical concept elsewhere in the dashboard.
Late responses from one scope cannot overwrite the other scope's DOM.

Historical mode always displays:

> Historical Foundry Replay. Fixed-block counterfactual simulation under a
> hash-bound state override modelling a prefunded, predeployed, preapproved
> executor. Successful values are research estimates at the displayed Ethereum
> block; they are not current and are not executable candidates.

It hides the Strict section, renames Research estimates to Historical Foundry
Replays, labels net edge as Net result at replay block, and labels age as State
age at replay. It shows block number/hash/time, direction, notional, Foundry
verification, gas, receipt/trace hashes, policy baseline, and stress outcome.
Positive and negative verified scenarios remain visible. Evidence failures
invalidate the historical bundle instead of being mixed into its ten rows; the
Unavailable table is empty for a valid MVP bundle.

The dedicated release flag fetches and retains the exact served bytes from
`/opportunities?opportunity_scope=historical`; it does not substitute the
checkout template or a synthetic DOM fixture. It validates those HTML bytes'
versioned asset references against `/health` application SHA and asset
SHA/version, hashes the HTML bytes separately, and runs the exact served
HTML/navigation/app bytes against the unfiltered ten-scenario API payload. The
DOM result binds application SHA, asset SHA, HTML SHA-256, and API
`data_generation` in one typed surface hash and checks all ten visible rows.
After the probe the checker rereads health, every versioned asset, the same HTML
URL, and the API, requiring unchanged bytes and identities before success.

## Error model

Run-level closed states include:

- `archive_state_unavailable`;
- `anchor_changed`;
- `authority_mismatch`;
- `block_coverage_incomplete`;
- `fee_history_incomplete`;
- `price_snapshot_incomplete`;
- `reserve_snapshot_incomplete`;
- `fork_hardfork_unsupported`;
- `fork_window_mixed`;
- `foundry_replay_failed`;
- `candidate_unresolved`;
- `no_publishable_profitable_block`;
- `positive_gate_failed`;
- `historical_bundle_invalid`; and
- `publication_race`.

Transport errors, timeouts, HTTP statuses, parser/resource limits, and
unexpected or unclassified reverts map to run-level closed reasons without
retaining arbitrary body, header, URL, path, RPC credential, or exception text.
An allowlisted deterministic status-zero receipt may resolve a scanned
candidate as non-publishable, but no selected block or public row may contain
one. Resource-limit failure never becomes no-opportunity. Process cancellation
and exit signals continue to propagate.

## Resource and security boundaries

- RPC and Foundry have no retries that can change the selected anchor or block.
- Collection requires a clean tracked source tree and retains repository HEAD,
  Python runtime identity, and the physical hashes of every policy/toolchain
  and executor input used by the run.
- Header/state/fee requests use deterministic bounded roots. Only a literal HTTP
  413 may bisect a Task-3a descriptor-authorized multirow header/reserve/price
  interval. The claim-scoped historical wrapper owns the existing explicit
  Task-2b scope; the scheduler catches allowed 413 inside `with scope:`, keeps
  that scope active, and drains its pending children left-first/depth-first
  under the same cumulative limits. Normal exit occurs only after the final
  success empties the queue. Fee/final/singleton/anchor/lower/disallowed 413 and
  provider text, 429, 5xx, timeout, or transport failure are terminalized by the
  wrapper and exit exceptionally.
- JSON decoding retains the repository's wire, decompression, header, node,
  scalar, string, depth, duplicate-key, canonical-number, and deadline limits.
- Every pure lower observation or complete logical root is guarded before copy,
  canonicalization, or hash with exactly 1,048,576 nodes, 8,388,608 aggregate
  scalar bytes, 262,144 ordinary-string bytes, depth 128, and 4,096 numeric-token
  bytes. Hostile integers pass an unbound bit-length gate before decimal text;
  Decimal ratios pass the exact 2,048-byte CPython-object preflight and bounded
  tuple/scientific-token contract. System CPython and exact production CPython
  3.8.10 must reproduce the frozen Decimal layout KAT before projection.
- The maximum `H=50_401` plan contains exactly 5,094 roots: 1,261 header, 2,521
  reserve, 1,261 price, 50 fee, and one final reread. A separate 8-MiB decoded
  budget for each root could retain
  `5_094 * 8_388_608 = 42_731_569_152` bytes, exactly `39.796875 GiB`, before
  anchor/lower overhead. That arithmetic is why Task 4a precedes Task 3b; it is
  not a RAM or disk guarantee.
- One monotonic run quota begins when Task 4a opens staging and covers every
  committed spool frame and every later raw/typed/config/scan/scenario/manifest
  member: 8 GiB physical bytes and 200,000 members total. No phase, 413,
  failure, reread, seal, conversion, or deletion resets it or creates a second
  allowance. It is a lifecycle write budget, not a final retained-tree cap:
  bytes are deliberately charged once for the successful spool frame and again
  for its immutable Task-4b chunk. An aborted pending tail reverses only its
  provisional debit and does not reset committed quota.
- At most one Task-2b exchange transfer is live. A successful spool handoff
  leaves the context's resident raw exchange-byte count exactly zero before the
  next attempt/root/finalization; an outstanding transfer is terminal. Append
  allows one pending receipt; Task 2b validates pending against transfer,
  commits, revalidates committed receipt, and only then records/returns.
  Exception or cancellation aborts the pending tail when possible and closes
  transfer/context; failed abort terminalizes the spool. No rows or committed
  receipt escape a spool write/receipt/commit failure.
- Storage alone defines all transfer/pending-receipt/committed-receipt/final-
  capability classes and, in Task 3b, the exact
  `_HistoricalWindowSpoolSourceBinding`, with their closure issuers/verifiers/
  revokers/one-shot guards. It
  imports neither RPC nor scan at module load or runtime; RPC and scan import
  storage only function-locally. Task 4a freezes the base types/state/test bridge
  and leaves production methods absent or closed-unavailable; Task 3b modifies
  storage to add the exact production methods after RPC/scan types exist.
- Task 3b adds exactly
  `("source:historical_foundry_scan", None, "scripts/historical_foundry_scan.py")`
  and
  `("source:historical_foundry_storage", None, "scripts/historical_foundry_storage.py")`
  to Task-2b's held production source inventory. `module_name=None` prevents a
  preflight import. The sole post-claim source-to-spool binder resolves scan
  without importing it. If
  `sys.modules["__main__"].__spec__.name` is exactly
  `scripts.historical_foundry_scan`, it binds actual key `__main__`; a
  simultaneously present canonical key must be the same object. Otherwise it
  binds canonical key `scripts.historical_foundry_scan` only when that object's
  `__spec__.name` is canonical. If neither case matches, reject; there is no
  second import. Before the first logical root, the sole
  `_bind_claimed_historical_window_sources_to_spool(*, claim, spool)` boundary
  accesses the live held source authority and RPC function-locally imports storage,
  which must exist at canonical key `scripts.historical_foundry_storage`, have
  that exact spec name, and be the identical passed object; storage has no
  `__main__` fallback. The binder verifies origin/file and held FD/path/inode/
  file identity/bytes/hash, then retains role, fixed canonical name, actual key,
  and exact module object. In the same boundary it no-follow duplicates every
  scan/storage/RPC-related held source member and necessary ancestry descriptor,
  with pre/post FD/path/inode/bytes/hash checks, into a storage-owned binding.
  Caller supplies no module/path/FD/hash/mapping. Finalization and each later
  authority transition recheck the same key/spec/object/origin/file and the
  independent duplicate FD/path/inode/bytes/hash; any absence, conflicting
  alias, reload, replacement, or drift raises exactly
  `authority_mismatch/final_identity_drift`. Original preflight descriptors may
  close after finalization without weakening the binding. The binding moves
  active spool → sealed spool → capability → consumed view → held staging
  snapshot and is revoked/closed exactly once after descriptor ownership moves
  to the snapshot, or on any failed/cancelled lifecycle. Callers provide no
  path/hash, and no neutral module means no third row.
- Subprocess arguments are fixed; no shell interpolation, FFI, arbitrary test
  selector, environment echo, or caller-supplied executable path is allowed.
- The executor never receives an RPC URL, private key, arbitrary router, token,
  recipient, or calldata from a public caller.
- Production artifacts never contain a usable RPC URL, authorization header,
  cookie, provider key, private key, local absolute path, or error body.
- Staged and published paths reject symlinks, hard links, traversal, unstable
  metadata, ancestry swaps, descriptor races, and unexpected members.
- Toolchain archives, compiler, submodule, policy, executor, and all evidence
  members are hash-bound.

## TDD and verification matrix

Implementation proceeds in independently green slices.

### Policy and pure arithmetic

- exact canonical policy and hash known-answer tests;
- rejection of caller overrides and extra fields;
- EIP-1559 next-base-fee boundary tests;
- USD-to-WETH, V2 swap, gas, MEV, and policy-net integer/Decimal known answers;
- negative/nonfinite/mismatched MEV rejection, current-policy exact 10 bps, and
  a generic policy-schema KAT proving that a hash-bound zero rate is not
  structurally forbidden;
- a zero-rate safe-exclusion known answer proving the scanner reads
  `policy.acceptance_mev_bps` rather than falling back to 10 bps;
- exact-zero net rejection and minimum positive unit acceptance; and
- strict-net-positive but research-net-nonpositive rejection.

### Authority and coverage

- fixed router/factory/WETH/pair/token/feed identity replays;
- reversed getPair identity and token-order cases;
- runtime and address transplant rejection;
- lower-bound timestamp binary search;
- header parent continuity and exact inclusive counts;
- exact two-pair `getReserves()` response inventory for every block hash;
- exact proxy `latestRoundData()` response inventory for every block hash;
- rejection of a missing, extra, wrong-block, duplicate-ID, or silently
  substituted reserve/price response;
- proof that optional Sync/AnswerUpdated logs cannot authorize safe exclusion;
- Chainlink round freshness, answer, phase encoding, and selected aggregator
  checks;
- feeHistory count/range/p50/p90 validation; and
- missing, duplicated, truncated, reordered, or extra chunk rejection.

### Capture and immutable storage split

- Task 3a exact signatures, including the four-input header projector and
  four-input complete-root seam; shared header builder/success projector use for
  lower probes, witnesses, bulk headers, and final anchor;
- pure lower transcript/fresh witness, gapless formula IDs, `H=1/50_400/50_401`
  and `50_402` rejection, staged one-shot descriptors, exact final-anchor typed
  digest without changing the `H` header inventory, and proof that every output
  remains fixture-only and nonauthorizing;
- guard-before-copy exact/+1 tests for nodes, scalar/string/depth/numeric-token
  limits, hostile million-bit integers, strict Decimal preflight ordering,
  exact 1832/2000/2048/2056 layout table, 4,096/4,097 digit/token boundaries,
  signed zero/nonfinite/context invariance, and the strict one-quantum ratio
  enclosure on system and exact CPython 3.8.10;
- Task 4a storage-only ownership and private-test-bridge/issuer KAT for all four
  handoff types; exact append-to-pending, commit, abort, committed-only reread,
  seal, and close APIs; one-pending-only inventory exclusion; validation before
  commit and after committed receipt; frame bytes, contiguous indices/offsets,
  abort truncation/provisional-debit rollback and abort-failure terminalization;
  descriptor/ancestry races, copy/pickle/reuse/transplant rejection, and one
  lifecycle 8-GiB/200,000-member quota that deliberately double-charges spool
  and chunks but never resets; plus proof that production transfer/mint/consume
  methods are absent or closed-unavailable;
- Task 3b fresh-context claim, same held config identity, generic-batch rejection
  after claim; exact logical-root opener/attempt, claimed-finalizer/finalization-
  verifier, spool transfer/pending/committed verifier, scan reconciliation/
  verifier, sealed-spool mint, and one-shot consume APIs; zero resident raw
  bytes, outstanding-transfer rejection, compact finalization field set and
  recursive raw-field absence, three ordered ledger segments and global
  indices, and exact pre/post-finalization cleanup/cancellation;
- Task-3b storage bridge exact bound-module type checks and verifier calls;
  rejection of mapping/hash/bool/lookalike substitutes, direct issuer access,
  cross-spool/cross-claim, forged bound-module classes, pre-finalization,
  pre-reconciliation, repeat mint, and repeat consume;
- exact source-to-spool binder signature and sole-access boundary; pre-bind
  transfer/finalize/mint, second bind, cross-spool/claim, forged module/object/
  FD/path/hash, and drift before/after bind rejection; pre/post-duplication
  descriptor rechecks, no leaked FDs after failed/cancelled bind, successful
  recheck/mint after original preflight-source close, closed-before-mint
  rejection, exact-once close after reconciliation failure, mint-to-consume-to-
  snapshot ownership movement/revocation, and post-consume/repeat-use rejection;
- exact held scan/storage source rows; direct `python -m` `__main__` binding,
  canonical imported-runner binding, dual-key same/different-object cases,
  no-import/no-reload behavior, storage canonical-key-only binding, and
  finalization recheck of key/spec/object/origin/file/FD/path/inode/bytes/hash
  with every missing/drift case closed as `final_identity_drift`;
- existing explicit-scope state-machine REDs for catch location, pending
  left-first/depth-first order, normal exit only after empty queue, exceptional
  exit, cleanup, and cancellation; allowed multirow literal-413 continuation
  versus terminal anchor/lower/fee/final/singleton/other failures, including a
  six-row reserve root split `3+3` whose poisoned half-block leaves never
  receive typed semantics;
- reconciliation reread of every spool frame, complete anchor/lower/root/global
  pure replay, request IDs `1..last_request_id`, physical leaf versus root-only
  typed digests, final-anchor-before-finalize order, mismatch closure, and one
  live transfer at maximum window; and
- Task 4b exact storage one-shot-consumer ingress and exact private consumed
  view, rejection of fixture/transfer/direct-spool/lookalike inputs before
  writer open, immutable raw/typed chunk freeze, decode/reprojection reread,
  lifecycle-quota continuity, snapshot currentness, and no-replace one-final-
  manifest state machine.

### Candidate scan

- full 2-by-5 grid at every block;
- safe-exclusion known answers with no false-negative heuristic;
- descending selection and newer unresolved-candidate rejection;
- newer fully publishable positive block must beat an older one, while a newer
  `nonpublishable_positive` block remains explicit and does not satisfy the
  selection predicate;
- no-publishable-positive complete coverage, including a window containing only
  `nonpublishable_positive` blocks, returns the closed nonpublication state;
- scan rerun produces identical logical evidence hashes.

### Foundry and receipt

- router/factory/WETH constructor checks;
- both directions and all notionals at one fixed fixture block;
- actual first-leg UNI delta feeds the second leg;
- exact overlay account/slot inventory, prior-value reread, and mutation
  rejection;
- zero prior executor inventory/allowances and the exact direction-specific
  WETH-input/UNI-intermediate four-allowance-slot matrix;
- pool state unchanged by the overlay;
- measured receipt is the sole transaction in synthetic block `B+1` at the
  fixed timestamp and base fee;
- receipt status, gas, price, max fee, priority fee, balances, residuals, trace,
  and hash closure;
- reverted, wrong-router, wrong-block, wrong-hardfork, output, calldata, receipt,
  trace, and one-wei delta tampering rejection;
- two independent replays produce identical selected state, token deltas, gas,
  and executor bytecode hash; and
- the unit suite passes offline while the separately invoked fixed-block fork
  suite proves it did not skip connected replay.

### Bridge and economics

- exactly two routes and ten scenarios;
- exact route/opportunity IDs and common-quantity closure;
- Foundry result is rederived rather than trusted;
- atomic route has one route-level gas row and no leg-level duplicate gas;
- exactly nine cost rows per scenario and ninety total;
- exact direct consumption of ten
  `historical_foundry_cost_proof_inputs/v1` objects and their
  `proof_inputs_hash` values, with missing/extra/reordered/renamed/tampered
  proof fields or rows rejected;
- pool fee embedded with both exact proof-bound informational USD amounts,
  router/tax proof, route transfer N/A, receipt gas, and 10 bps MEV arithmetic;
- unique baseline p50+10, stress p90+25, and stress p90+50 arithmetic;
- all ten published rows research-only with status-one receipts, no unavailable
  rows, no attestation, and no strict promotion; and
- at least one published research-net value exactly matches positive policy
  net.

### Historical publication

- exact historical-core pointer/manifest/stage discrimination, five-file core
  inventory, and rejection by every live core wrapper;
- `HistoricalReplayBuildContext` construction only from a validated historical
  staged or committed core, staged/committed byte-equivalence, and rejection of
  forged, transplanted, live-stage, or stale contexts;
- writer-held context and reader-held validated-view call-order tests proving
  context/view validation, then exact proof-input validation, then the sole
  context-free low-level nine-row validator call; direct dashboard/release
  low-level calls are rejected by import-boundary tests;
- exact six-file bundle, member inventory, row counts, hashes, and canonical
  order;
- sealed stage-derived profile selection, rejection of caller-injected profile
  or member lists, and byte-for-byte regression coverage for the unchanged live
  bundle contract;
- CSV/SQLite/logical parity;
- complete-manifest closure over the exact historical core manifest/pointer
  identities, including successful loading after mutable core `latest.json`
  advances to a newer cohort;
- proof that Foundry-only fields remain exclusively in `replay_evidence.json`
  and do not widen existing CSV or SQLite row schemas;
- closed replay-evidence schema, one-to-one linkage with all ten Opportunity
  rows, all ten exact `proof_inputs_hash` values, both pool-fee amount
  parities, and source-run member reread;
- missing, failed, wrong-pointer, wrong-inventory, or TOCTOU-mutated connected
  verification report rejection;
- proof that connected verification replays the selected ten plus every newer
  `replay_required` scenario, and that changing any newer resolution prevents
  publication;
- audit-only `verify --bundle` full-range/scenario-set parity and zero-mutation
  behavior;
- referenced-run retention and unreferenced-only garbage collection;
- missing, extra, duplicate, orphan, wrong-block, wrong-policy, and mutated
  evidence rejection;
- symlink, hardlink, traversal, directory/member race, and pointer-swap tests;
- all-negative preflight and every failure leave the prior historical public
  pointer unchanged, while a newly published private core may remain only as
  unreachable diagnostic evidence;
- dry-run produces and validates all core, context, complete-bundle, and
  connected-report bytes while moving neither core nor complete pointer and
  installing no public verification report;
  and
- both live `routes/core/latest.json` and `routes/latest.json` remain
  byte-for-byte unchanged throughout historical core and complete publication.

### API, UI, and release

- live 120/121-second boundary remains unchanged;
- historical evidence years old retains economics only after historical
  envelope validation;
- missing versus corrupt historical pointer behavior;
- filters, sorting, counts, strict-empty result, and cache invalidation,
  including deletion, mutation, or inode swap of the verification report after
  a warm cache;
- process-lifetime pointer-SHA-to-full-physical-signature guarding, including
  HTTP 503 for identical pointer/member bytes installed under a new descendant
  inode;
- current/history response race isolation;
- URL round trip and fixed disclaimer;
- no Current/Executable language in historical mode;
- actual served historical HTML/JavaScript DOM parity bound to unchanged
  application SHA, asset SHA/version, HTML SHA, API generation, and post-probe
  rereads;
- ten research rows, two directions, five notionals, unavailable,
  strict/executable/attested count zero, Foundry-verified count ten, and
  positive count at least one; and
- a dedicated release-checker flag validates the complete historical MVP.

Python production modules must parse with Python 3.8 grammar and run under real
CPython 3.8.10 as well as the system runtime. Foundry is an independent pinned
binary and does not enter the dashboard Python environment.

## Release checker

The release checker gains an explicit, opt-in flag:

```text
--require-historical-foundry-replay
```

It requires:

- a valid historical pointer and fully reread bundle;
- the pointer-bound verification report at its physical-SHA path, with exact
  pointer-core, run, bundle, coverage, and scenario-set parity;
- policy ID/hash and supported toolchain identities;
- chain ID one and fixed UNI/WETH venues;
- two opposite DEX routes;
- five notionals per route and ten total scenarios;
- ten Foundry-verified scenario proofs;
- ten exact `historical_foundry_cost_proof_inputs/v1` objects and hashes, each
  matching all nine public rows including both embedded pool-fee informational
  amounts;
- all ten rows `research_estimate` with status-one receipts and no unavailable,
  strict, executable, or attested rows;
- at least one available `research_net_edge_usd > 0`;
- coverage 100%, gap count zero, zero unresolved candidates at or newer than the
  selected block, and every skipped older candidate closed as
  `not_needed_older_than_selected`;
- selected block `newest_publishable_policy_positive` proof;
- retained source-run member reread and replay-evidence parity;
- the process-lifetime pointer-SHA-to-physical-signature guard; and
- actual served HTML/application/asset/API-generation-bound DOM parity for all
  ten scenarios, followed by exact post-probe rereads.

The existing `--require-route-opportunities` flag is not redefined and does not
stand in for this stronger MVP gate.

## Operational sequence

1. Implement, test on the system runtime and exact CPython 3.8.10, independently
   review, and commit Task 3a, then Task 4a, then Task 3b, then Task 4b, then
   Tasks 5, 6, and 7. No Phase-2 endpoint or connected Anvil gate runs during
   these slices. Task 4a ends with production bridge methods absent or
   closed-unavailable; Task 3b modifies RPC, scan, and storage together to add
   them.
2. From a clean committed HEAD under exact CPython 3.8.10, load/hash the fixed
   policy and held config/toolchain/source identities, open the Task-4a spool,
   open Task 2b, and atomically claim the still-fresh context for the historical
   window. The claim itself performs no import or source bind. The connected
   runner will use the canonical imported-module resolver branch; direct
   `python -m scripts.historical_foundry_scan` will use the exact `__main__`
   branch inside Step 3's binder.
3. Before the first logical root, call
   `_bind_claimed_historical_window_sources_to_spool(*, claim, spool)` exactly
   once. Inside it, function-locally import storage at its canonical key,
   resolve both exact module bindings, reverify the held source authority, and
   no-follow duplicate/transfer the required source and ancestry descriptors to
   the storage-owned spool binding. Then capture all three Task-2a anchor stages
   into the spool, issue each transfer only through
   `spool.issue_transfer_from_bound_rpc(...)`, and replay the complete 48-row
   anchor authority before backfilling any anchor-stage semantic digest.
4. Acquire the ordered lower-bound probes and fresh boundary witness as
   singleton logical scopes using the shared header seams, then replay the full
   one-pass lower proof and build the compact Task-3a request plan.
5. Execute staged header roots first. After complete header inventory validation,
   execute reserves, prices, fees, and the final anchor. Every successful
   physical exchange follows append, `verify_pending_receipt`, commit,
   `verify_committed_receipt` before releasing resident raw bytes; exception/
   cancellation aborts the pending tail when possible. Each complete root runs
   in one claim-scoped `with scope:`;
   only descriptor-authorized literal 413 intervals are caught inside it and
   bisect without opening a new scope.
6. Semantically compare the final normalized anchor; call the claimed finalizer
   exactly once; seal the spool; issue the exact scan reconciliation by replaying
   all pure projectors; recheck the live spool source binding after the original
   finalizer cleanup; then call
   `mint_production_historical_window_capability`, whose bound RPC/scan exact-
   type and verifier checks validate it before the one-shot issuer is reachable.
7. Task 4b calls `consume_production_historical_window_capability` once, accepts
   only its exact private consumed view, rechecks and moves the source identity/
   required held descriptors into the staging snapshot, writes and
   descriptor-rereads immutable
   raw RPC and typed
   capture chunks under the same lifecycle quota, freezes the capture role set,
   closes the spool, revokes/closes the consumed source binding exactly once,
   and issues the held staging snapshot.
8. Build, persist, freeze, and reread the exact scan inventory and safe candidate
   set only through the held snapshot and validated window/grid capabilities.
9. Replay candidates newest to oldest until a valid publishable winner is
   selected or the fully resolved window proves no publishable positive result.
10. Reread all staged evidence and verify zero gaps/unresolved newer candidates.
    Derive selected-block typed members, create `run_manifest.json` last, revoke
    the writer, rename no-replace, reopen, and reread the immutable raw run.
11. Stage and fully validate the isolated historical private core and its exact
    prospective core-pointer bytes without moving `latest.json`.
12. Construct `HistoricalReplayBuildContext` only through the staged-core loader,
    build the ten Opportunity inputs, and preflight the exact positive
    research-net gate.
13. In publish mode, commit and reread the historical private core, reconstruct
    the context through the committed-core loader, and require byte-equivalence
    with the staged context. In dry-run mode, do not move the core pointer.
14. Rebuild the inputs against the authoritative staged-or-committed context
    and verify economics are unchanged.
15. Stage and validate the historical bundle and compute the exact pointer core
    without moving the pointer.
16. Run connected verification over the complete seven-day inventory, every
    newer replay-required scenario, the selected ten, raw run, core, staged
    bundle, and pointer core. Both modes construct and validate the exact
    would-be report and final report-hash-bound pointer bytes.
17. In publish mode only, install and reread the immutable report, reread all
    verified inputs, and atomically publish the historical pointer. Dry-run
    installs no report and moves no pointer.
18. In publish mode only, reread the historical API, run audit-only
    `verify --bundle`, and run the dedicated release checker. Dry-run ends after
    proving staged parity and reports that publication checks were not run.

No step after a failure publishes a partial or weaker result. Reference-aware
raw-run retention is implemented in this MVP. Automated production scheduling
and long-horizon retention policy are deferred until this manual MVP passes end
to end.

## Completion evidence

The MVP is complete only when a real run, not a fixture, supplies all of the
following:

- pinned toolchain/version/hash output;
- immutable run manifest with complete seven-day coverage;
- selected fixed block and ten Foundry/Anvil receipts;
- at least one positive baseline policy-net scenario;
- the immutable pointer-core/report-hash-closed connected-verification report;
- a separate successful audit-only `verify --bundle` result;
- two routes, ten Opportunity rows, ninety cost rows, at least one successful
  positive research row, and no strict/executable rows;
- historical API and rendered UI showing the same winner and counts;
- dedicated release checker success;
- system and real CPython 3.8.10 focused/full regression evidence;
- Foundry online and offline test success; and
- a clean diff review with no secrets or mutable RPC endpoint retained.

Passing unit tests with synthetic data, producing a reserve-math candidate, or
retaining a status-zero candidate without a ten-success historical bundle does
not satisfy completion.
