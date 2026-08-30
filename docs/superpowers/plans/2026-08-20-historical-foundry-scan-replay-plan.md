# Historical Foundry Scan and Replay Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce one immutable, locally validated seven-day historical raw run whose complete block/reserve/price/fee inventory supports exact safe prefiltering and descending fresh-Anvil candidate resolution.

**Architecture:** Phase-2 window work is deliberately split into four reviewed slices: Task 3a builds a pure/offline request plan and semantic projection; Task 4a adds the minimal bounded descriptor spool, base handoff classes, pending state, and test bridge; Task 3b modifies RPC, scan, and storage together to add exact production transfer/finalization/reconciliation/mint/consume contracts, moves independently duplicated claimed source authority into that spool before logical work, then captures, finalizes, rereads, and reconciles one fresh run; Task 4b alone turns the resulting exact production capability and still-live source binding into immutable typed chunks and a held descriptor snapshot. Tasks 5–7 consume only that snapshot/capability chain, recompute the 2×5 grid, and resolve candidates in fresh Anvil processes without trusting stored booleans. This phase finalizes raw evidence and selection only; it does not publish a historical core or public pointer.

**Tech Stack:** Exact CPython 3.8.10 standard library, bounded HTTP/JSON streaming, Ethereum archive JSON-RPC, pinned Anvil/Cast/Forge from Phase 1, canonical JSON/gzip, `unittest`.

**Spec:** `docs/superpowers/specs/2026-08-20-historical-foundry-replay-opportunity-design.md` sections “Seven-day scan”, “State override and measured transaction”, “Economic calculation”, “Immutable run evidence”, “Resource and security boundaries”, “Authority and coverage”, “Candidate scan”, and “Foundry and receipt”.

## Global Constraints

- Production requires `DEX_DEPTH_RPC_ETH`; no default endpoint exists and no output contains the endpoint or a reversible provider identity.
- `finalized` is read exactly once at start and reread by number/hash at completion. All capture and fallback batches retain that anchor.
- The inclusive range has exact continuous headers and exactly two reserve calls plus one Chainlink call per block. No log, carry-forward, interpolation, or partial-window success is authoritative.
- JSON-RPC success IDs may arrive out of order, but exact request/response ID sets must match. Duplicate, missing, extra, boolean, zero, wrong-method, or wrong-block identities fail closed.
- Each candidate scenario uses a fresh Anvil process and exactly one measured transaction. Setup mutations are retained, not charged as transactions; snapshot reuse is forbidden for this MVP.
- Unexpected transport, fork, authority, receipt, trace, or revert behavior leaves the candidate unresolved and stops older-block selection.
- Phase-2 terminal reasons are closed to `archive_state_unavailable`, `anchor_changed`, `authority_mismatch`, `block_coverage_incomplete`, `fee_history_incomplete`, `price_snapshot_incomplete`, `reserve_snapshot_incomplete`, `fork_hardfork_unsupported`, `fork_window_mixed`, `foundry_replay_failed`, `candidate_unresolved`, and `no_publishable_profitable_block`. Tests freeze the mapping from each failure boundary; no arbitrary exception text replaces a reason.
- This phase writes only under `<validated-data-dir>/raw/historical-foundry-replay/<run_id>` and private staging below the same validated data root. It does not touch any live or historical route pointer.
- The dependency order is exact: Task 3a pure/offline projection → Task 4a bounded descriptor spool/base types/test bridge → Task 3b production RPC/scan/storage bridges and capture/finalize/reconcile → Task 4b immutable chunks/snapshot → Tasks 5–7. A fixture mapping, pure projection, copied visible property set, or test capability cannot authorize Task 4a/4b storage.
- No Phase-2 connected archive collection or Anvil replay gate runs until Tasks 3a, 4a, 3b, 4b, 5, 6, and 7 are all committed, independently review-clean, and green offline on both the system runtime and exact production CPython 3.8.10.

## Frozen Resource, RPC, and Process Policy

These constants are code constants covered by exact known-answer tests; no config file, environment variable, CLI flag, or caller parameter may change them:

| Boundary | Exact limit |
| --- | --- |
| archive HTTP request body | 4 MiB |
| archive HTTP response wire / decoded body | 8 MiB / 8 MiB per logical batch |
| HTTP status/header bytes | status 200 only / 64 KiB total headers |
| JSON shape | 1,048,576 nodes, 8 MiB aggregate scalar bytes, 256 KiB per ordinary string, depth 128, 4 KiB per numeric token |
| archive batch absolute deadline | 30 seconds from pre-connect through final decoded byte |
| full anchor/window/selection collection deadline | 21,600 seconds, checked before and after every batch and scenario |
| fork-relay inbound Anvil request | 64 KiB total request headers and 4 MiB body; exactly one non-batch JSON-RPC object per HTTP request |
| fork-relay upstream archive exchange | 4 MiB request body, 64 KiB response headers, 64 MiB response wire bytes, and 64 MiB decoded response bytes per relayed call |
| fork-relay downstream Anvil response | 4 KiB generated response headers and 64 MiB body; the body is the one validated upstream decoded response, with no second fetch or body substitution |
| fork-relay cumulative response budget | 64 MiB wire and 64 MiB decoded across the entire inbound call; counters start before the sole upstream attempt and cannot reset by chunk, method, parse, or error path |
| fork-relay single-call absolute deadline | 30 seconds from the first inbound header byte through the final downstream response byte, or through abort and closure of both sockets |
| local Anvil RPC request / decoded response | 4 MiB / 64 MiB per call |
| local Anvil RPC absolute deadline | 30 seconds per call |
| Anvil readiness / one-scenario deadline | 30 seconds / 120 seconds including startup and teardown |
| readiness stdout+stderr scan | 64 KiB combined; used only to detect readiness, never retained in evidence or errors |
| stored trace | 64 MiB canonical decoded JSON and 16 MiB deterministic gzip bytes per scenario |
| other scenario JSON member | 8 MiB per overlay, receipt, or result member |
| scan chunk / manifest member | 16 MiB decoded per chunk, 16 MiB per canonical manifest |
| unified staged run, including exchange spool | 8 GiB physical bytes and 200,000 members across spool frames and every later staged/final member; one monotonic ledger, no per-stage reset or second allowance |
| process shutdown | send SIGTERM once, wait 5 seconds, send SIGKILL once if still alive, wait/reap at most 5 more seconds |

Limit/deadline exhaustion maps to the owning closed terminal reason, never retries, never becomes `no_publishable_profitable_block`, and never retains body/output text. The relay applies the global JSON-shape limits as well as its own rows above; it rejects request batching before opening an upstream socket. Tests exercise every archive, relay-inbound, relay-upstream, relay-downstream, local-Anvil, and stored-member byte limit at the exact limit and limit+1; relay cumulative counters before and after decoding; deadline equality and +1 monotonic tick for archive, relay, local Anvil, scenario, and full-run clocks; stdout/stderr overflow; trace compression bombs; total-run exhaustion; SIGTERM success; SIGTERM timeout followed by SIGKILL; and unreaped-child failure.

The per-logical-root archive limits are not a run-wide retention bound. At the maximum `H=50_401`, the pure plan has `1_261` header roots, `2_521` reserve roots, `1_261` price roots, `50` fee roots, and one final-anchor root, for `5_094` roots. Allowing each root to contribute its full 8-MiB decoded budget would permit `5_094 * 8_388_608 = 42_731_569_152` resident bytes, exactly `39.796875 GiB`, before anchor/lower overhead. Task 3a therefore makes no production-memory claim; every successful Task-3b physical exchange must be synchronously transferred to the Task-4a spool, leaving zero resident raw exchange bytes before another attempt/root/finalization. The unified 8-GiB/200,000-member staging quota is independently enforced across committed spool frames and all later evidence writes; it is not inferred from the 39.796875-GiB worst-case arithmetic.

The archive capture client permits exactly `eth_chainId`, `eth_getBlockByNumber`, `eth_getBlockByHash`, `eth_call`, `eth_getCode`, `eth_getBalance`, `eth_getTransactionCount`, `eth_getStorageAt`, and `eth_feeHistory`. The private Anvil fork relay permits exactly `eth_chainId`, `eth_getBlockByNumber`, `eth_getBlockByHash`, `eth_getCode`, `eth_getBalance`, `eth_getTransactionCount`, `eth_getStorageAt`, `eth_call`, and `eth_getProof` upstream. The local Anvil client permits exactly `eth_chainId`, `eth_getBlockByNumber`, `eth_getBlockByHash`, `eth_getCode`, `eth_getBalance`, `eth_getTransactionCount`, `eth_getStorageAt`, `eth_call`, `eth_sendTransaction`, `eth_getTransactionByHash`, `eth_getTransactionReceipt`, `debug_traceTransaction`, `evm_setAutomine`, `evm_setNextBlockTimestamp`, `anvil_impersonateAccount`, `anvil_stopImpersonatingAccount`, `anvil_setCode`, `anvil_setBalance`, `anvil_setNonce`, `anvil_setStorageAt`, `anvil_setNextBlockBaseFeePerGas`, and `anvil_mine`. Any other method, even if supported by the provider, is a programmer error and closes the run.

Before the first anchor request, the production command requires exact CPython 3.8.10, a completely clean `git status --porcelain=v1 --untracked-files=all`, a 40-hex HEAD, stable hashes of every imported tracked source/config/build artifact, and the Phase-1 project-local toolchain capability. It repeats the source/config/artifact stable-hash check before final manifest creation. The run retains HEAD, Python identity, and source/config/artifact digests. A dirty tree or identity drift performs zero archive requests.

Endpoint evidence has exactly the fields `schema`, `scope`, and `endpoint_hmac_sha256`; the first two values are `historical_foundry_rpc_endpoint_identity/v1` and `single_run_nonreversible`, and the digest must match `\A[0-9a-f]{64}\Z`. The collector creates one 32-byte in-memory random HMAC key, computes HMAC-SHA256 over the canonical normalized endpoint origin/path projection for every connection, requires equality throughout the run, and destroys the key without retaining it or its input. Only the digest is written. The URL, hostname, provider name, port, path, query, credential, and an unkeyed URL hash are never persisted.

## Task 1: Extract a Shared Bounded JSON Decoder Without Weakening Route Cost

**Files:**

- Create: `scripts/bounded_json.py`
- Create: `tests/test_bounded_json.py`
- Modify: `scripts/route_cost_collector.py`
- Modify: `tests/test_route_cost_collector.py`

- [ ] **Step 1: Write parity RED tests**

Move no behavior yet. Add a parameterized corpus that sends the same response fixture through the existing route-cost wrapper and the planned shared decoder. Cover identity/gzip, exact limit/+1, headers, Content-Length, canonical bytes, duplicate keys, exact integers/decimals, `NaN`/infinity, surrogate pairs, malformed `\u`, depth 128/+1, node/scalar/string limits, slow body deadline, header accessor failure, and `KeyboardInterrupt`/`SystemExit` propagation.

The shared interface is error-code based so callers can sanitize their own message prefix:

```python
class BoundedJsonError(ValueError):
    reason_code: str

def decode_bounded_json_response(
    response: Any,
    *,
    header_limit: int,
    wire_limit: int,
    decoded_limit: int,
    scalar_limit: int,
    node_limit: int,
    ordinary_string_limit: int,
    require_canonical: bool,
    materialize_exact_floats: bool = False,
    absolute_deadline: Optional[float] = None,
    monotonic: Callable[[], float] = time.monotonic,
    return_decoded_bytes: bool = False,
) -> Any: ...
```

Reason codes are closed: `invalid`, `resource_limit`, `deadline`, `unavailable`, `encoding_unsupported`, and `noncanonical`. They contain no body, URL, path, or exception text. The unchanged route-cost compatibility wrapper always supplies its existing 32-KiB header cap; the historical private boundary always supplies the frozen 64-KiB cap. Tests prove both profiles and reject a caller attempt to alter the historical value.

- [ ] **Step 2: Run RED**

```bash
python3 -m unittest tests.test_bounded_json -v
```

Expected: missing shared decoder module.

- [ ] **Step 3: Move the proven implementation and retain a compatibility wrapper**

Move the lexical preflight, exact string scanner, bounded shape walk, header parser, streaming gzip reader, and deadline logic from `route_cost_collector.py` into the shared module. Keep `_decode_bounded_json_response` in `route_cost_collector.py` with its current signature; it delegates and maps `BoundedJsonError` to the exact existing `RouteCostCollectorError` contract. Do not broaden the route-cost public API.

- [ ] **Step 4: Run parity GREEN on both runtimes**

```bash
python3 -m unittest tests.test_bounded_json tests.test_route_cost_collector -v
/private/tmp/cpython-3.8.10-runtime-20260820/bin/python3.8 -B -m unittest \
  tests.test_bounded_json tests.test_route_cost_collector -v
```

- [ ] **Step 5: Commit the extraction**

```bash
git add scripts/bounded_json.py scripts/route_cost_collector.py \
  tests/test_bounded_json.py tests/test_route_cost_collector.py
git commit -m "refactor(rpc): share bounded JSON decoding"
```

## Task 2: Build the Sealed Archive-RPC Boundary and Anchor Authority Check

**Files:**

- Create: `scripts/historical_foundry_rpc.py`
- Create: `tests/test_historical_foundry_rpc.py`
- Modify: `scripts/historical_foundry_contracts.py`
- Modify: `tests/test_historical_foundry_contracts.py`

- [ ] **Step 1: Write request-byte and response-set RED tests**

Freeze these public pure helpers:

```python
def build_historical_anchor_request_plan(
    policy: Mapping[str, Any],
    authority: Mapping[str, Any],
) -> Mapping[str, Any]: ...

def project_historical_anchor_capture(
    plan: Mapping[str, Any],
    responses: Sequence[Mapping[str, Any]],
) -> Mapping[str, Any]: ...
```

The private production boundary is the only code that reads `DEX_DEPTH_RPC_ETH`:

```python
def _production_archive_rpc_batch(
    context: "_ProductionArchiveRpcRunContext",
    request_rows: Sequence[Mapping[str, Any]],
) -> Tuple[Mapping[str, Any], ...]: ...
```

Known-answer tests freeze canonical JSON-RPC bytes for `eth_chainId`, finalized block, router `factory()`/`WETH()`, factory pair derivation in both token orders, pair `factory()`/`token0()`/`token1()`, decimals, runtime code, Chainlink description/decimals/aggregator/phase/round, executor code/nonce, and token storage/getter authority. Freeze the archive-method allowlist and every archive HTTP/JSON/deadline constant in the resource table. Reject redirect/proxy/cookie/retry behavior, caller URL/client/headers/limits/deadlines/methods, response-ID drift, error body retention, wrong pair order, runtime transplant, and configured chain other than 1. Add a preflight spy proving a dirty tree, non-3.8.10 runtime, source/config hash drift, or invalid project-local executable produces zero HTTP calls.

- [ ] **Step 2: Run RED**

```bash
python3 -m unittest tests.test_historical_foundry_rpc -v
```

Expected: missing request planner/RPC module.

- [ ] **Step 3: Implement sealed HTTP and anchor projection**

Use fixed POST, `ProxyHandler({})`, no redirects, a fixed Accept/Content-Type/User-Agent set, no retries, the frozen resource table, shared bounded decoder, exact ID matching, and the exact single-run HMAC endpoint identity schema above. Generate the HMAC key before reading the endpoint, keep it only in private `_ProductionArchiveRpcRunContext` together with the internally created absolute collection deadline, require the same digest for every connection, and erase it after final source/anchor checks; never write an unkeyed URL/origin hash. A test capability may accept canonical request bytes and return raw bytes, but it is module-private, identity-sealed, omitted from public collector signatures, and `repr`-redacted. Production takes no request limit, deadline, method, URL, or client parameter.

Do not hard-code pair addresses. Derive them from both factories and retain pair/token/router/factory/proxy/aggregator runtime SHA-256 and Keccak-256.

- [ ] **Step 4: Run GREEN**

```bash
python3 -m unittest \
  tests.test_historical_foundry_rpc \
  tests.test_historical_foundry_contracts -v
```

- [ ] **Step 5: Commit the anchor slice**

```bash
git add scripts/historical_foundry_rpc.py \
  scripts/historical_foundry_contracts.py \
  tests/test_historical_foundry_rpc.py \
  tests/test_historical_foundry_contracts.py
git commit -m "feat(opportunity): seal historical archive RPC authority"
```

## Task 3a: Build the Pure/Offline Historical Window Plan and Projection

**Files:**

- Create: `scripts/historical_foundry_scan.py`
- Create: `tests/test_historical_foundry_scan.py`
- Modify: `scripts/historical_foundry_contracts.py` only to add `next_historical_base_fee`
- Modify: `tests/test_historical_foundry_contracts.py` only for that public wrapper

**Interfaces:**

- Consumes: the exact pure `historical_foundry_anchor_capture/v1` mapping from Task 2a; no endpoint, production context, client, raw HTTP bytes, storage path, source inventory, finalization, or capability.
- Produces: a fixture-only `historical_foundry_window_projection/v1`, a temporary validated header inventory, compact deterministic root descriptors, and the shared header/root seams that Task 3b must reuse. Every mapping remains `authority=fixture_only_nonauthorizing`; no mapping from this task may authorize Task 4a or Task 4b.

- [ ] **Step 1: Write the exact public-surface and shared-seam RED tests**

Freeze the public EIP-1559 wrapper and seven pure scan APIs exactly:

```python
def next_historical_base_fee(
    *,
    parent_base_fee: int,
    parent_gas_used: int,
    parent_gas_limit: int,
) -> int: ...

def locate_inclusive_lower_bound(
    *,
    anchor: Mapping[str, Any],
    header_at_number: Callable[[int], Mapping[str, Any]],
    lookback_seconds: int,
) -> int: ...

def project_historical_lower_bound_capture(
    *,
    anchor_capture: Mapping[str, Any],
    lookback_seconds: int,
    search_probes: Iterable[Mapping[str, Any]],
    boundary_witness: Iterable[Mapping[str, Any]],
) -> Mapping[str, Any]: ...

def build_historical_window_request_plan(
    *,
    lower_bound_capture: Mapping[str, Any],
    anchor_capture: Mapping[str, Any],
) -> Mapping[str, Any]: ...

def iter_historical_header_request_batches(
    plan: Mapping[str, Any],
) -> Iterator[Mapping[str, Any]]: ...

def project_historical_header_inventory(
    *,
    plan: Mapping[str, Any],
    anchor_capture: Mapping[str, Any],
    lower_bound_capture: Mapping[str, Any],
    batch_results: Iterable[
        Tuple[Mapping[str, Any], Sequence[Mapping[str, Any]]]
    ],
) -> Mapping[str, Any]: ...

def iter_historical_state_request_batches(
    *,
    plan: Mapping[str, Any],
    header_inventory: Mapping[str, Any],
) -> Iterator[Mapping[str, Any]]: ...

def project_historical_window_projection(
    *,
    plan: Mapping[str, Any],
    anchor_capture: Mapping[str, Any],
    lower_bound_capture: Mapping[str, Any],
    header_inventory: Mapping[str, Any],
    batch_results: Iterable[
        Tuple[Mapping[str, Any], Sequence[Mapping[str, Any]]]
    ],
) -> Mapping[str, Any]: ...
```

Freeze the shared private seams exactly; lower probes, witnesses, bulk headers,
and the final anchor must all use the same builder/projector, and both public
projectors plus later production reconciliation must use the same complete-root
semantic path:

```python
def _build_historical_block_header_request(
    *,
    block_number: int,
    request_id: int,
) -> Mapping[str, Any]: ...

def _project_historical_block_header_success(
    *,
    request: Mapping[str, Any],
    response: Mapping[str, Any],
) -> Mapping[str, Any]: ...

def _project_complete_historical_window_root(
    *,
    plan: Mapping[str, Any],
    descriptor: Mapping[str, Any],
    responses: Sequence[Mapping[str, Any]],
    header_inventory: Optional[Mapping[str, Any]],
) -> Mapping[str, Any]: ...
```

The root seam accepts one reconstructed original descriptor root, never a 413
child. It requires `header_inventory is None` exactly for header roots and the
complete validated inventory for state roots, and returns exactly `kind`,
`root_index`, `block_start`, `block_stop`, `request_ids`, `typed_role`,
`typed_row_count`, `typed_logical_sha256`, and bounded temporary `rows`.

- [ ] **Step 2: Write lower-proof, compact-plan, and one-shot descriptor REDs**

The normalized header has exactly
`number,hash,parent_hash,state_root,timestamp,gas_limit,gas_used,base_fee_per_gas`;
integer fields are exact built-in integers,
hashes are lowercase 32-byte hex, gas and timestamp fields are uint64, and base
fee is uint256. The pure lower-bound helper performs ordinary lower-bound search
over `[0,A]`, while the authoritative projector consumes the ordered search
transcript once and then a fresh predecessor/lower witness once. Probe IDs begin
at 49 after Task-2a IDs `1..48`; for `S` probe/witness observations, the window
starts at `49+S`.

For `H=A-L+1`, `F=ceil(H/1024)`, require `1 <= H <= 50_401` and reject
`H=50_402`. The compact gapless ledger is anchor `1..48`, lower proof next,
headers `H`, reserves `2H` in fixed venue order, prices `H`, fees `F`, and one
final-anchor reread, so window `request_count=4H+F+1` and
`last_request_id=49+S+4H+F`. Store no all-ID, all-request, or all-response list.

Each factory call returns a fresh deterministic iterator; each returned iterator
is one-shot. Header roots contain at most 40 rows. State roots contain at most 20
blocks/two reserves per block, at most 40 prices, one fee request covering at
most 1024 blocks, or one final anchor. `root_index` is zero-based and continuous
across both stages. Literal-HTTP-413 bisection is permitted only by a descriptor
flag for multirow header/reserve/price roots; Task 3a performs no HTTP or
fallback. Recompute the full expected request tuple on every use so mutated
nested dict/list fields, IDs, params, block ranges, and flags reject.

Every descriptor has exactly:

```text
schema = historical_foundry_window_batch/v1
kind, root_index, block_start, block_stop,
request_id_start, request_id_stop, request_count,
requests, allow_http_413_bisection
```

Every request has exactly `jsonrpc,id,method,params`. Header/final rows use
`eth_getBlockByNumber([hex(B),false])`; reserves use selector `0x0902f1ac` and
the exact pair with EIP-1898 `{blockHash,requireCanonical:true}`; prices use
selector `0xfeaf968c` and the fixed proxy with the same EIP-1898 object; fees use
`eth_feeHistory([hex(N),hex(stop),[50,90]])`. Quantities are lowercase minimal
hex. No tag/number substitute, `from`, `value`, gas, caller address/selector/hash,
or extra field is accepted.

- [ ] **Step 3: Write exact semantic, resource, Decimal, and final-anchor REDs**

Before copying, canonicalizing, or hashing each current observation/root, apply
one iterative pure-input guard with exact limits: `1_048_576` nodes,
`8_388_608` aggregate scalar bytes, `262_144` ordinary UTF-8 string bytes,
depth 128, and `4_096` ASCII numeric-token bytes. Count all containers/scalars,
keys, strings, numeric tokens, and literal widths; reject booleans as integers,
subclasses, surrogates, depth 129, and every limit plus one. Guard only the
current item and release it before advancing; do not add a 64-container cap.

For integers, call unbound `int.bit_length` before `str`, encoding, or JSON.
Accept nonnegative magnitudes strictly below `10**4096` and negative magnitudes
strictly below `10**4095`, then require the signed token length `<=4096`. For
ratios, require exact `Decimal` (or exact integer 0/1), call unbound
`Decimal.__sizeof__` before `is_finite` and the single `as_tuple`, and require
the CPython layout KAT `4095/4096/4097 digits -> 1832`, `4500 -> 2000`,
`4617 -> 2048`, `4618 -> 2056`. Object bytes are capped at 2048; coefficient
digits and canonical scientific token are capped at 4096; tuple exponent is
`-8190..4095`; `abs(adjusted) <= 4095`; signed zero, nonfinite, float, string,
subclass, and ambient-context arithmetic reject. For intermediate header gas
ratios, preserve the original tuple and require the strict one-quantum enclosure
`abs(n*scale-m*C) < m`; equality rejects. Endpoint 0/1 agreement with exact
header integers is biconditional. Apply the same structural checks, without
header equality, to optional blob ratios.

Require exact 96-byte reserve ABI, exact two-row venue order, and retain zero
reserves. Require exact 160-byte Chainlink ABI, positive int256 answer, uint80
rounds, nonzero/same phase rules, `0 < started_at <= updated_at <= timestamp`,
age 3600 accepted/3601 rejected, and `valid_until=updated_at+3601`; the anchor
row equals Task-2a authority. Fee roots require exact `N+1/N/N` shapes, p50/p90
ordering, optional Dencun fields both-or-neither, header/base-fee agreement,
adjacent chunk overlap agreement, every interior EIP-1559 child, and the
post-anchor child through `next_historical_base_fee`. Economics never uses
`gasUsedRatio`.

The final descriptor is the singleton anchor reread. Its normalized eight-field
header must equal `lower_bound_capture.anchor_header`; its temporary root is
exactly `typed_role=final_anchor`, `typed_row_count=1`, and a framed digest under
`historical_foundry_final_anchor_inventory/v1`. The final digest remains a
boundary/root-ledger value and never increases the compact header inventory from
`H` to `H+1`.

- [ ] **Step 4: Run focused RED, then implement the minimal pure slice**

```bash
python3 -m unittest \
  tests.test_historical_foundry_scan \
  tests.test_historical_foundry_contracts -v
```

Implement one immutable redacted `HistoricalWindowProjectionError` with this
exact closed reason/failure-kind matrix:

| Reason | Failure kinds |
| --- | --- |
| `authority_mismatch` | `anchor_authority_invalid`, `window_plan_invalid`, `request_ledger_invalid`, `fixture_input_invalid` |
| `anchor_changed` | `final_anchor_mismatch` |
| `block_coverage_incomplete` | `lower_bound_invalid`, `lower_bound_witness_invalid`, `window_resource_limit`, `header_invalid`, `header_continuity_invalid`, `header_coverage_invalid` |
| `reserve_snapshot_incomplete` | `reserve_abi_invalid`, `reserve_coverage_invalid` |
| `price_snapshot_incomplete` | `price_abi_invalid`, `price_round_invalid`, `price_freshness_invalid`, `price_coverage_invalid` |
| `fee_history_incomplete` | `fee_shape_invalid`, `fee_coverage_invalid`, `fee_header_mismatch` |

Sanitize ordinary hostile mapping/iterator/callback and
conversion exceptions; preserve `KeyboardInterrupt`, `SystemExit`,
`GeneratorExit`, and `asyncio.CancelledError`. Canonical typed hashes use sorted
compact UTF-8 JSON, `allow_nan=False`, an eight-byte unsigned big-endian frame,
and exactly these domains:

```text
historical_foundry_scan_request/v1
historical_foundry_scan_result/v1
historical_foundry_scan_response/v1
historical_foundry_anchor_capture/v1
historical_foundry_anchor_stage_inventory/v1
historical_foundry_normalized_header/v1
historical_foundry_lower_bound_capture/v1
historical_foundry_header_inventory/v1
historical_foundry_reserve_inventory/v1
historical_foundry_price_inventory/v1
historical_foundry_fee_inventory/v1
historical_foundry_final_anchor_inventory/v1
historical_foundry_continuous_request_ids/v1
```

The final compact projection has exactly:

```text
schema, authority, chain_id, anchor_capture_sha256,
lower_bound_capture_sha256, range, role_inventories, boundaries, request_ledger
```

`authority=fixture_only_nonauthorizing`; `range` is
`lower_bound_number,anchor_number,cutoff_timestamp,block_count`; role inventories
are exactly `headers,reserves,prices,fees`; boundaries are exactly predecessor,
lower, anchor, and final-anchor headers; and the request ledger contains exact
stage ranges plus the framed continuous-ID digest. It contains no raw bytes,
HTTP identity, path, capability, or full typed row list.

- [ ] **Step 5: Run dual-runtime GREEN, stress, compile, and commit**

Run the full generated `H=50_401` 12-second-slot corpus and a missed-slot
`H=50_400` corpus with staged generators. Assert exact counts
`50_401/100_802/50_401/50_401`, `F=50`, roots
`1_261/2_521/1_261/50/1`, bounded partial roots, gapless IDs, one live raw
observation/root, and releasable temporary header inventory.

```bash
python3 -m unittest \
  tests.test_historical_foundry_scan \
  tests.test_historical_foundry_contracts \
  tests.test_historical_foundry_rpc \
  tests.test_historical_foundry_toolchain \
  tests.test_bounded_json -v
/private/tmp/cpython-3.8.10-runtime-20260820/bin/python3.8 -B -m unittest \
  tests.test_historical_foundry_scan \
  tests.test_historical_foundry_contracts \
  tests.test_historical_foundry_rpc \
  tests.test_historical_foundry_toolchain \
  tests.test_bounded_json -v
python3 -m py_compile \
  scripts/historical_foundry_scan.py \
  scripts/historical_foundry_contracts.py \
  tests/test_historical_foundry_scan.py \
  tests/test_historical_foundry_contracts.py
/private/tmp/cpython-3.8.10-runtime-20260820/bin/python3.8 -B -m py_compile \
  scripts/historical_foundry_scan.py \
  scripts/historical_foundry_contracts.py \
  tests/test_historical_foundry_scan.py \
  tests/test_historical_foundry_contracts.py
git diff --check
```

Run serially because toolchain/contracts tests share repository-local build
state. Then stage exactly the four task files and commit:

```bash
git add scripts/historical_foundry_scan.py \
  tests/test_historical_foundry_scan.py \
  scripts/historical_foundry_contracts.py \
  tests/test_historical_foundry_contracts.py
git commit -m "feat(opportunity): add pure historical window projection"
```

Do not open an RPC run, write evidence, finalize Task 2b, run Foundry, or begin
Task 3b from this slice.

## Task 4a: Add the Minimal Bounded Exchange-Descriptor Spool

**Files:**

- Create: `scripts/historical_foundry_storage.py`
- Create: `tests/test_historical_foundry_storage.py`

**Interfaces:**

- Consumes: only an absolute validated data directory and, in this slice's
  known-answer tests, one storage-owned exchange transfer issued by the private
  storage test bridge. It accepts no Task-3a plan/projection/header inventory,
  request/response mapping, endpoint, caller path, RPC/scan class, production
  claim/finalization/reconciliation, or authority claim.
- Produces: an append-only descriptor-held spool, exact pending and committed
  receipts, a sealed read-only spool, the same run-wide quota capability later
  used by Task 4b, and the storage-owned definitions/closure provenance for all
  base handoff types. A transfer is only a spool-bound transport object and
  never authorizes a historical window. Task 4a freezes only the base classes,
  pending state machine, and private test bridge. Production transfer/mint/
  consume bridge methods do not exist yet or reject closed-unavailable; Task 3b
  modifies storage to add them after the exact RPC/scan types exist.

- [ ] **Step 1: Write REDs for the exact private spool API**

```python
def _open_historical_window_exchange_spool(
    *,
    data_dir: Path,
) -> "_HistoricalWindowExchangeSpool": ...

def _issue_historical_window_exchange_transfer_for_test(
    *,
    spool: "_HistoricalWindowExchangeSpool",
    exchange_projection: Mapping[str, Any],
    canonical_request_bytes: bytes,
    decoded_response_bytes: bytes,
) -> "_ProductionArchiveRpcExchangeTransfer": ...

class _HistoricalWindowExchangeSpool:
    def append_transfer(
        self,
        *,
        transfer: "_ProductionArchiveRpcExchangeTransfer",
    ) -> "_PendingHistoricalWindowSpoolReceipt": ...

    def commit_transfer(
        self,
        *,
        transfer: "_ProductionArchiveRpcExchangeTransfer",
        pending_receipt: "_PendingHistoricalWindowSpoolReceipt",
    ) -> "_HistoricalWindowSpoolReceipt": ...

    def abort_transfer(
        self,
        *,
        transfer: "_ProductionArchiveRpcExchangeTransfer",
        pending_receipt: "_PendingHistoricalWindowSpoolReceipt",
    ) -> None: ...

    def reread_exchange(
        self,
        *,
        receipt: "_HistoricalWindowSpoolReceipt",
    ) -> Tuple[bytes, bytes]: ...

    def seal(self) -> "_SealedHistoricalWindowExchangeSpool": ...
    def close(self) -> None: ...

class _SealedHistoricalWindowExchangeSpool:
    def reread_exchange(
        self,
        *,
        receipt: "_HistoricalWindowSpoolReceipt",
    ) -> Tuple[bytes, bytes]: ...

    def close(self) -> None: ...
```

`scripts/historical_foundry_storage.py` is the sole owner of the exact base classes
`_ProductionArchiveRpcExchangeTransfer`,
`_PendingHistoricalWindowSpoolReceipt`,
`_HistoricalWindowSpoolReceipt`, and
`_ProductionHistoricalWindowCapability`. Their constructors are inaccessible;
the non-exported closure issuers are not callable through any production method
in this slice. RPC and scan never receive an issuer callable and define no
same-named class. Storage imports neither RPC nor scan at module load or runtime;
RPC and scan may import storage only function-locally at the handoff. Task-4a
tests use only `_issue_historical_window_exchange_transfer_for_test(...)` to
invoke the test transfer issuer for the positive exact-transfer/type KAT, so
this slice has no future Task-3b type dependency and makes no production-mint
claim.

`append_transfer` accepts one exact transfer, permits at most one pending append,
and writes one physical member exactly as
`uint64_be(request_len)||canonical_request_bytes||uint64_be(decoded_len)||decoded_response_bytes`.
It reserves a provisional quota debit, fsyncs, descriptor-rereads the complete
frame, verifies both payload hashes and the framed hash, and returns a bound
pending receipt; the frame is not yet in committed inventory and cannot be
reread through `reread_exchange`. Task 2b must validate that exact pending
receipt against the still-live transfer, call `commit_transfer` once, validate
the returned committed receipt again, and only then record or return the
exchange. `abort_transfer` truncates the uncommitted tail frame and rolls back
its provisional byte/member debit. Any abort/truncate/fsync/descriptor failure
terminalizes the spool. The positive contiguous `spool_member_index`, offset of
the first length prefix, complete framed length, and SHA-256 are
descriptor-derived. The committed receipt's exact immutable projection is:

```text
schema = historical_foundry_exchange_spool_receipt/v1
exchange_index, logical_batch_index, attempt_index,
request_byte_count, request_sha256, request_ids,
wire_byte_count, wire_sha256,
decoded_byte_count, decoded_sha256, response_ids,
spool_member_index, spool_offset, spool_length, spool_member_sha256
```

The transfer, spool, and both receipt states are private, redacted, exact-type
checked, noncopyable, nonpickleable, nonserializable, claim/spool/exchange
bound, and single-consumer. Transfer/pending/committed reuse,
duplicate/noncontiguous offsets or indices, multiple simultaneous pending
receipts, transplant, truncation, append-after-seal, reread of pending,
reread-after-close, and unstable descriptor identity reject without returning
bytes.

- [ ] **Step 2: Freeze the unified run-wide quota and filesystem boundary**

Open the absolute pre-existing operator-owned `data_dir` no-follow through stable
ancestry descriptors and create one private no-replace staging/spool file. One
internal monotonic quota ledger starts here and is retained by the sealed spool
for Task 4b: exactly 8 GiB physical bytes and 200,000 members across every
committed framed spool member and every later chunk, config copy, scenario
member, and manifest. This is a lifecycle write budget, not a cap on the final
retained tree: successful exchange bytes are charged once in the spool and
again when Task 4b writes immutable chunks. No 413, failed append, reread, seal,
conversion, deletion, or phase boundary resets the committed ledger or creates
a second allowance. Check the provisional framed byte/member debit before
writing; a successful abort reverses only that uncommitted provisional debit.
On partial write, receipt failure, fsync failure, quota failure, descriptor
drift, or failed abort, terminalize and close the spool with no committed
receipt. Never expose a path or accept a caller relative path.

- [ ] **Step 3: Implement atomic append, bounded reread, seal, and close**

Reuse the proven no-follow descriptor primitives in `scripts/route_publication.py`.
Hold the file descriptor and ancestry snapshot for the spool lifetime. Implement
the exact pending/commit/abort state machine above. `seal()` requires no pending
receipt, fsyncs, revokes append authority, rereads the complete committed
receipt/offset inventory, and returns a read-only exact capability over the same
descriptors and quota. `reread_exchange` accepts only a committed receipt. Close
is idempotent only for cleanup; no read/write/receipt action succeeds afterward.

- [ ] **Step 4: Run offline GREEN on both runtimes and commit**

```bash
python3 -m unittest tests.test_historical_foundry_storage -v
/private/tmp/cpython-3.8.10-runtime-20260820/bin/python3.8 -B -m unittest \
  tests.test_historical_foundry_storage -v
python3 -m py_compile \
  scripts/historical_foundry_storage.py \
  tests/test_historical_foundry_storage.py
/private/tmp/cpython-3.8.10-runtime-20260820/bin/python3.8 -B -m py_compile \
  scripts/historical_foundry_storage.py \
  tests/test_historical_foundry_storage.py
git diff --check
git add scripts/historical_foundry_storage.py \
  tests/test_historical_foundry_storage.py
git commit -m "feat(opportunity): add bounded historical exchange spool"
```

Tests cover only `_issue_historical_window_exchange_transfer_for_test` and its
storage-owned test issuer transfer KAT, and
prove every production transfer/mint/consume bridge is absent or
closed-unavailable; one-pending-only and pending-not-in-inventory; exact
append/commit/abort transitions; validation
before commit and after committed receipt; abort tail truncation/provisional
quota rollback and abort-failure terminalization; committed-only reread; exact
limit/+1 and lifecycle double-debit accounting; frame corruption;
descriptor/ancestry races; symlink/hardlink/traversal; short writes; fsync/read
failures; ordinary exception sanitization; cancellation cleanup; and proof that
arbitrary mappings and every Task-3a output are rejected before a member is
created.

## Task 3b: Capture, Finalize, and Reconcile the Production Window

**Files:**

- Modify: `scripts/historical_foundry_rpc.py`
- Modify: `tests/test_historical_foundry_rpc.py`
- Modify: `scripts/historical_foundry_scan.py`
- Modify: `tests/test_historical_foundry_scan.py`
- Modify: `scripts/historical_foundry_storage.py`
- Modify: `tests/test_historical_foundry_storage.py`

**Interfaces:**

- Consumes: one exact fresh claim produced inside the RPC boundary over its
  internally held production context, that claim's already-held opening config
  identity and live held source authority, the Task-3a pure seams, and one
  active unbound Task-4a spool. The RPC boundary moves independently held
  source descriptors and exact module-object bindings into that spool exactly
  once before the first logical root. The Task-3b scan scheduler never receives
  or passes a raw context, Task-2b scope, source path, descriptor, hash, module,
  or source-authority mapping.
- Produces: one exact storage-owned `_ProductionHistoricalWindowCapability`.
  Only `sealed_spool.mint_production_historical_window_capability(...)` may
  invoke the non-exported Task-4a issuer, and only after it verifies the exact
  Task-2b finalization seal and reconciliation-complete state. The result is
  bound to the reconciled sealed spool, compact projection, three-segment
  post-ledger, held opening identity, final Task-2b source identity, and the
  same still-live storage-owned source binding. This is the sole
  Task-4b ingress; no transfer, mapping, or fixture/test capability is
  equivalent.

- [ ] **Step 1: Write fresh-claim and specialized-spool-handoff REDs**

Task 2b owns these exact claim-scoped seams:

```python
def _claim_fresh_production_archive_rpc_run_for_historical_window(
    *,
    context: "_ProductionArchiveRpcRunContext",
) -> "_ProductionHistoricalWindowRunClaim": ...

def _bind_claimed_historical_window_scan_source_module(
    *,
    claim: "_ProductionHistoricalWindowRunClaim",
) -> None: ...

def _bind_claimed_historical_window_storage_source_module(
    *,
    claim: "_ProductionHistoricalWindowRunClaim",
    module: Any,
) -> None: ...

def _bind_claimed_historical_window_sources_to_spool(
    *,
    claim: "_ProductionHistoricalWindowRunClaim",
    spool: "_HistoricalWindowExchangeSpool",
) -> "_HistoricalWindowSpoolSourceBinding": ...

def _open_production_archive_rpc_historical_window_logical_batch(
    *,
    claim: "_ProductionHistoricalWindowRunClaim",
    logical_root: Mapping[str, Any],
    spool: "_HistoricalWindowExchangeSpool",
) -> "_ProductionHistoricalWindowLogicalBatchScope": ...

def _production_archive_rpc_historical_window_logical_batch_attempt(
    *,
    logical_scope: "_ProductionHistoricalWindowLogicalBatchScope",
    request_rows: Sequence[Mapping[str, Any]],
) -> Tuple[
    Tuple[Mapping[str, Any], ...],
    "_HistoricalWindowSpoolReceipt",
]: ...

def _finalize_claimed_production_archive_rpc_run_for_historical_window(
    *,
    claim: "_ProductionHistoricalWindowRunClaim",
) -> "_ProductionArchiveRpcFinalization": ...

def _verify_claimed_historical_window_finalization(
    *,
    claim: "_ProductionHistoricalWindowRunClaim",
    finalization: "_ProductionArchiveRpcFinalization",
    expected_receipt_inventory_sha256: str,
) -> None: ...
```

Task 3b adds these exact production storage contracts; the spool-bound methods
and one-shot consumer are not module-level issuer functions and were absent or
closed-unavailable in Task 4a. The Task-4a `append_transfer`,
`commit_transfer`, and `abort_transfer` signatures remain unchanged:

```python
class _HistoricalWindowSpoolSourceBinding:
    """Storage-owned live source authority bound to one claim and spool."""

class _HistoricalWindowExchangeSpool:
    def issue_transfer_from_bound_rpc(
        self,
        *,
        claim: Any,
        exchange_projection: Mapping[str, Any],
        canonical_request_bytes: bytes,
        decoded_response_bytes: bytes,
    ) -> "_ProductionArchiveRpcExchangeTransfer": ...

    def verify_pending_receipt(
        self,
        *,
        transfer: "_ProductionArchiveRpcExchangeTransfer",
        pending_receipt: "_PendingHistoricalWindowSpoolReceipt",
    ) -> None: ...

    def verify_committed_receipt(
        self,
        *,
        transfer: "_ProductionArchiveRpcExchangeTransfer",
        receipt: "_HistoricalWindowSpoolReceipt",
    ) -> None: ...

class _SealedHistoricalWindowExchangeSpool:
    def mint_production_historical_window_capability(
        self,
        *,
        claim: Any,
        finalization: Any,
        reconciliation: Any,
    ) -> "_ProductionHistoricalWindowCapability": ...

class _ConsumedProductionHistoricalWindowCapabilityView:
    """Exact storage-private, immutable payload view for Task 4b."""

def consume_production_historical_window_capability(
    *,
    capability: "_ProductionHistoricalWindowCapability",
) -> "_ConsumedProductionHistoricalWindowCapabilityView": ...
```

Task 3b also freezes the scan-owned reconciliation authority and exact seams:

```python
class _ProductionHistoricalWindowReconciliation:
    """Closure-issued exact authority; immutable and nonserializable."""

def _reconcile_production_historical_window(
    *,
    claim: Any,
    finalization: Any,
    sealed_spool: Any,
    frozen_pre_ledger: Sequence[Mapping[str, Any]],
    plan: Mapping[str, Any],
    compact_projection: Mapping[str, Any],
) -> "_ProductionHistoricalWindowReconciliation": ...

def _verify_production_historical_window_reconciliation(
    *,
    reconciliation: "_ProductionHistoricalWindowReconciliation",
    expected_spool_identity: Any,
    expected_finalization_identity: Any,
) -> None: ...
```

`Any` is a Python-3.8-safe annotation boundary, not permission to accept an
arbitrary value. Runtime checks use the exact classes on the already-bound RPC
and scan module objects; mappings, hashes, booleans, lookalike classes, and
objects from another claim/spool are never substitutes for claim,
finalization, reconciliation, sealed-spool identity, or finalization identity.
`_ProductionHistoricalWindowReconciliation` is closure-issued, immutable,
redacted, exact-type checked, noncopyable, nonpickleable, nonserializable, and
bound to the same claim, sealed-spool identity, receipt inventory, finalization
identity, compact projection, and post-ledgers it verified. Its verifier rejects
lookalikes before reading visible fields. The storage-owned consumed view has
the same hardening, contains only the already-bound private payload needed by
Task 4b, and is returned once; repeat consume fails before a writer opens.

`_bind_claimed_historical_window_sources_to_spool(*, claim, spool)` is the sole
Task API that can turn the RPC claim's held source authority into storage
authority. It is called exactly once after the fresh claim succeeds and before
the first logical root, transfer, finalization, or mint. It alone may access
`claim._context._preflight.sources`; the two fixed-module bind helpers above are
resolver substeps reachable only within this binder and never expose that
object. The binder requires a fresh claim with live held sources and an active,
unbound, nonterminal spool with no pending receipt or committed member. It
reverifies every held source member, then resolves the already-running scan
module and function-locally imports/resolves storage under the exact rules in
Step 4. Caller-supplied modules, paths, descriptors, hashes, or mappings are not
accepted.

Within that single verification boundary, RPC no-follow duplicates and moves
ownership of the scan-, storage-, and RPC-related held source descriptors plus
every ancestry descriptor needed to recheck them. It verifies FD identity,
path/inode/file identity, bytes, and SHA-256 both before and after duplication.
Only the current spool's storage-owned private bind slot can invoke storage's
closure issuer and accept the exact canonical/actual module keys, spec names,
exact module objects, and duplicate descriptor set as one
`_HistoricalWindowSpoolSourceBinding`. That slot is an implementation detail,
not another public or free issuer API; the RPC binder above is the unique
entry. On acceptance, the binder also records the exact spool/binding membership
in the claim's private closure state, so the claim-only finalizer can require
that same live binding without receiving a spool or caller proof. The binding
and its private issuer, exact verifier, and exact-once revoker are closure-owned
by storage and are reachable only from the current spool-bound transitions; no
issuer/verifier/revoker callable is exported. The binding is immutable,
redacted, noncopyable, nonpickleable,
nonserializable, exact claim/spool-bound, and one-shot. A failed or cancelled
bind closes every duplicate exactly once and leaves the spool unbound; a second
bind, partial binding, cross-spool/claim binding, or forged module/object/FD/
path/hash rejects.

The RPC-side production boundary invokes the atomic fresh claim before handing
anything to the scan scheduler. It requires exact production context type,
`state=active`,
no prior consumer, no active/reserved logical scope, empty success/logical
collections, and next logical-batch/exchange indices both 1. It records the sole
consumer `historical_foundry_window/v1`. Reuse, partial prior use, race, or held
config identity/digest drift closes once and raises
`authority_mismatch/historical_window_context_not_fresh` before HTTP. The claim
is closure-issued, immutable, noncopyable, nonserializable, redacted, exact-type
checked. Outside the RPC binder/finalizer closures it privately exposes only
the exact `preflight.config` object and frozen identity already held at opening;
only the source-to-spool binder may reach `preflight.sources`. Task 3b never
reloads a config path or substitutes a mapping.

Task 3b internally reconstructs `logical_root` and validates its closed
discriminated schema before opening any scope. The discriminator is exactly
`anchor_stage`, `lower_observation`, or `window_root`; every root carries its
complete original request tuple, global one-based logical index,
segment/kind/root index fields, and an internally derived `allow_413`. A caller
cannot supply a separate boolean, partial root, raw context, existing Task-2b
scope, or index override. The wrapper privately opens the existing
`_open_production_archive_rpc_logical_batch(context, full_root_rows)` exact
explicit scope and owns its enter/exit lifecycle. Task 3b never receives the raw
context or underlying Task-2b scope after the fresh claim.

After claim, whether or not the exactly-once source bind has occurred, generic
`_production_archive_rpc_batch` rejects before HTTP with
`authority_mismatch/historical_window_specialized_batch_required`. The
claim-scoped wrapper reuses Task-2b request/scope/transport/decoder machinery.
On each success RPC calls only
`spool.issue_transfer_from_bound_rpc(...)`, which requires the current spool's
live `_HistoricalWindowSpoolSourceBinding` to contain the exact bound RPC
module object and verifies
`type(claim) is bound_rpc._ProductionHistoricalWindowRunClaim` before its
closure issuer creates at most one exact
`_ProductionArchiveRpcExchangeTransfer` holding the canonical request bytes,
decoded response bytes, and sealed compact metadata. It calls
`append_transfer` once, calls
`verify_pending_receipt(*, transfer, pending_receipt)`, calls
`commit_transfer(*, transfer, pending_receipt)` once, calls
`verify_committed_receipt(*, transfer, receipt)`, zeroes/releases both raw
fields, closes the transfer, appends only the compact record, and only then
returns detached rows plus the committed receipt.
Resident context raw exchange bytes are exactly zero after every success and
before another attempt/root/finalization. An outstanding transfer blocks each
of those actions with
`authority_mismatch/historical_window_transfer_outstanding`.

The scheduler must use `with scope:`. It catches an allowed literal HTTP 413
inside that `with`, while the same explicit underlying logical scope remains
active, and continues its descriptor-derived pending queue left-first and
depth-first. Only the final successful child with an empty pending queue may
leave the `with` normally and complete the one logical summary. A disallowed,
singleton, fee, final-anchor, anchor, or lower 413 is terminalized by the
wrapper and exits by exception. No child opens a new scope or resets the shared
8-MiB logical budget. A recoverable 413 creates no transfer, pending/committed
receipt, success record, or spool member.

For any exception or cancellation after a pending receipt may exist, Task 2b
calls `abort_transfer(*, transfer, pending_receipt)` when possible, then
zeroes/closes the transfer and closes its context according to the lifecycle.
Abort rolls back only the tail frame and provisional quota debit; abort failure
terminalizes the spool. Spool append/validation/commit ordinary failure returns
no rows/receipt and raises only
`authority_mismatch/historical_window_spool_handoff_failed`; cancellation does
the same one-time cleanup and propagates unchanged. RPC and scan perform only
function-local late imports of storage; storage never imports RPC or scan.
Every production transfer issue, pending/committed receipt verification,
commit, abort, and seal requires the same live claim/spool binding. Pre-bind,
closed-binding, cross-spool, and cross-claim use rejects. Closing or
terminalizing the active spool revokes the binding and closes all duplicate
descriptors exactly once.

- [ ] **Step 2: Write anchor/lower/window scheduling and three-segment ledger REDs**

Use the held config identity for the three Task-2a anchor stages, pair/proxy
authority, and every later check. Anchor IDs/stages are exactly `1..2`
`anchor`, `3..39` `fixed_authority`, and `40..48` `derived_authority`. Dynamic
lower probes/witnesses start at 49 and call the shared header builder, send one
singleton through its claim-scoped logical wrapper, then call the shared header
success projector. Bulk headers and final anchor use the identical wrapper; no
callback constructs a wire row or assigns an ID.

The whole-run ledger has exactly three ordered segments: three `anchor_stage`
logical scopes, `S` singleton `lower_observation` scopes, and `R` `window_root`
scopes. Global one-based Task-2b `logical_batch_index` values are `1..3`, then
`4..3+S`, then `4+S+root_index`, for total `3+S+R`. Window `root_index` remains
zero-based. A 413 child stays inside its parent logical scope.

Freeze the exact pre-root schemas/fields:

```text
historical_foundry_anchor_stage_pre_root_ledger/v1:
segment = anchor_stage, stage_index, stage_name, logical_batch_index, request_ids,
request_count, canonical_request_byte_count, canonical_request_sha256,
response_ids, predicted_success_exchange_indices, anchor_capture_sha256,
stage_inventory_row_count, stage_inventory_logical_sha256

historical_foundry_lower_observation_pre_root_ledger/v1:
segment = lower_observation, observation_index, observation_kind, kind_index,
logical_batch_index, block_number, request_id,
canonical_request_byte_count, canonical_request_sha256, response_id,
predicted_success_exchange_index, request_sha256, result_sha256,
response_sha256, lower_bound_capture_sha256

historical_foundry_window_pre_root_ledger/v1:
segment = window_root, root_index, kind, block_start, block_stop, logical_batch_index,
request_ids, request_count, canonical_request_byte_count,
canonical_request_sha256, observed_http_413_intervals,
predicted_success_exchange_indices, typed_role, typed_row_count,
typed_logical_sha256

historical_foundry_pre_leaf_ledger/v1:
segment, segment_local_index, leaf_index, logical_batch_index, request_ids,
request_count, canonical_request_sha256, response_ids,
predicted_success_exchange_index
```

Anchor/lower typed hashes are backfilled only after the complete 48-row anchor
replay or complete one-pass lower proof succeeds. Window typed fields are
backfilled only after left-first children are reassembled by original request
ID, the complete original root passes `_project_complete_historical_window_root`,
and the owning global header/window projector accepts it. Pre-roots/leaves make
no wire/decoded claim. Canonical request size/hash is Task-2b's exact sorted,
compact, UTF-8, no-LF physical batch encoding. Every backfill completes and the
pre-ledger freezes before finalization.

Lower `observation_index` is zero-based over probes then witnesses;
`observation_kind` is exactly `search_probe` or `boundary_witness`, with its own
zero-based `kind_index`. `segment_local_index` means stage, observation, or root
index; anchor/lower leaves use `leaf_index=0`, while window leaves use zero-based
left-first success order. `observed_http_413_intervals` is an ordered tuple of
exact `attempt_index,first_request_id,last_request_id,request_count` mappings,
with fixed reason literal HTTP 413. A leaf is physical only and never has
`typed_role`, `typed_row_count`, or `typed_logical_sha256`.

- [ ] **Step 3: Write compact-finalization, reconciliation, and lifecycle REDs**

Revised Task-2b finalization retains identity, logical summaries, cumulative
counts, and ordered `successful_exchanges`, but each member is exactly:

```text
schema = historical_foundry_archive_rpc_spooled_success_exchange/v1
exchange_index, logical_batch_index, attempt_index,
request_byte_count, request_sha256, request_ids,
wire_byte_count, wire_sha256,
decoded_byte_count, decoded_sha256, response_ids,
spool_member_index, spool_offset, spool_length, spool_member_sha256
```

There is no `spool_receipt_sha256`, `canonical_request_bytes`,
`decoded_response_bytes`, or other raw body field anywhere in finalization.
Finalization requires no outstanding transfer and recomputes logical/global
counts from these compact records.

The lifecycle order is: bind claimed sources to the empty active spool before
any logical work; after all capture succeeds, semantically compare the final
normalized eight-field anchor; call
`_finalize_claimed_production_archive_rpc_run_for_historical_window(*, claim)`
once; seal the Task-4a spool; call
`_reconcile_production_historical_window(...)`; then call
`sealed_spool.mint_production_historical_window_capability(*, claim,
finalization, reconciliation)` once. The claimed finalizer is the only new seam
that can reach the claim's private context and is itself the only caller of the
existing `_finalize_production_archive_rpc_run(context)`; neither context nor
finalizer access escapes the RPC closure.
Reconciliation streams records, summaries, ledgers, receipts, and framed spool
members in lockstep. It bounded-reparses each decoded leaf for physical
request/envelope/ID/size/hash bindings only, then groups by logical scope,
reorders by original IDs, and reconstructs roots of at most 40 envelopes. It
replays all three anchor stages as one Task-2a capture, all lower singletons as
one lower proof, every complete window root through the shared root seam, and
the complete staged header then state projectors. A `3+3` split of a six-row
reserve root is legal only after root reassembly; neither half-block leaf may
claim typed semantics.

Before mint, `sealed_spool.mint_production_historical_window_capability(...)`
obtains the live spool source binding's exact bound RPC and scan module objects.
The original RPC `preflight.sources` may already have been closed by normal
claimed-finalizer cleanup; storage never dereferences it after bind. The spool
binding owns independent duplicate descriptors and module-object identity and
must still be live. Mint requires
`type(claim) is bound_rpc._ProductionHistoricalWindowRunClaim`,
`type(finalization) is bound_rpc._ProductionArchiveRpcFinalization`, and
`type(reconciliation) is bound_scan._ProductionHistoricalWindowReconciliation`.
It calls
`bound_rpc._verify_claimed_historical_window_finalization(...)` with the sealed
spool's exact receipt-inventory SHA-256 and calls
`bound_scan._verify_production_historical_window_reconciliation(...)` with the
exact spool and finalization identity capabilities. Those verifiers close over
issuer provenance and enforce the same claim, spool, receipt inventory,
finalization identity, and reconciliation result. Mint also requires exact
`sealed + finalized + reconciled` state. It accepts no mapping, copied hash,
boolean, lookalike, or alternate-module class in place of those capabilities.
`seal()` atomically moves, rather than copies, the live binding from the active
spool to the sealed spool. A first production mint attempt moves that same
binding into the resulting capability only after all checks succeed; a failed
first attempt terminalizes the sealed spool and revokes/closes the binding.
Repeat mint rejects because the sealed mint slot is consumed and cannot copy or
transplant the binding already owned by the capability.

Post-reconciled roots use schemas
`historical_foundry_anchor_stage_root_ledger/v1`,
`historical_foundry_lower_observation_root_ledger/v1`, or
`historical_foundry_window_root_ledger/v1` and add exactly:

```text
attempt_count, success_exchange_indices, wire_byte_count,
decoded_byte_count, leaf_count, leaf_ledger_sha256
```

Each physical post-leaf is:

```text
schema = historical_foundry_leaf_ledger/v1
segment, segment_local_index, leaf_index,
request_ids, request_count, canonical_request_sha256, response_ids,
exchange_index, logical_batch_index, attempt_index, request_byte_count,
decoded_byte_count, decoded_sha256, wire_byte_count, wire_sha256,
wire_hash_authority, spool_member_index, spool_offset, spool_length,
spool_member_sha256
```

Leaves never gain typed fields. Re-derive request/decoded/member bytes and hashes
from the spool. Because raw wire bytes are not retained,
`wire_hash_authority=task2b_sealed_not_rehashed`. Require exact global request
IDs `1..last_request_id`, logical/attempt/exchange/success indices, 413 schedule,
per-root/global counts, leaf digests, capture hashes, role digests, continuity,
coverage, and final anchor. Any post-finalization divergence raises exactly
`authority_mismatch/historical_window_reconciliation_mismatch`, closes the
spool and its source binding exactly once, and mints nothing.

Handle sealed `_ArchiveRpcError` before pure error translation. Only literal
`archive_state_unavailable/http_413` on a descriptor-authorized multirow
header/reserve/price interval is caught inside the active historical-window
`with scope:` and continues left-first/depth-first inside its existing pending
queue and cumulative budget. The explicit-scope state machine must remain
active across every child attempt and reach normal exit only after the last
success empties the queue. Anchor/lower, singleton, fee, final-anchor,
disallowed 413, JSON-RPC text, 429, 5xx, timeout, and transport errors are
terminalized by the wrapper and leave the scope by exception. Pre-finalization
pure semantic failure preserves its closed pair,
abandons context once, closes spool once, and mints nothing. Other unexpected
ordinary capability/callback/issuer failures sanitize to
`authority_mismatch/historical_window_capability_invalid`. After finalization,
cleanup closes the spool and its attached binding, never the already-closed
original preflight authority. Cancellation performs the same one-time cleanup
for the current lifecycle state and propagates. Finalization is never retried.
Only after all anchor exchanges succeed may complete Task-2a replay failure
translate to `authority_mismatch/anchor_authority_invalid`; no failed exchange
enters a semantic projector. A post-finalization pure replay failure is always
the reconciliation-mismatch pair above, never its earlier pure pair.

- [ ] **Step 4: Add the production source-inventory closure and implement**

Only Task 3b adds these exact rows to Task-2b's held production source inventory:

```python
(
    ("source:historical_foundry_scan", None, "scripts/historical_foundry_scan.py"),
    ("source:historical_foundry_storage", None, "scripts/historical_foundry_storage.py"),
)
```

The `module_name=None` rows cause preflight to descriptor-open and hold each
reviewed path, inode/file identity, exact bytes, and SHA-256 without importing
either module. Callers supply neither path nor digest. The claim itself calls
no source-module binder. Inside the sole source-to-spool binder,
`_bind_claimed_historical_window_scan_source_module(*, claim)` resolves scan
without importing it. Its resolver is exact:

- If `sys.modules["__main__"].__spec__.name == "scripts.historical_foundry_scan"`,
  bind actual key `__main__` and that exact
  object. If canonical key `scripts.historical_foundry_scan` also exists, it
  must be the identical object or the claim rejects.
- Otherwise bind `sys.modules["scripts.historical_foundry_scan"]` only when its
  `__spec__.name` is exactly `scripts.historical_foundry_scan`.
- If neither case matches, reject. Claim resolution never imports or reloads
  scan.

Before the first logical root, RPC calls
`_bind_claimed_historical_window_sources_to_spool(*, claim, spool)` exactly
once. Inside that boundary it function-locally imports storage and calls the
resolver substep
`_bind_claimed_historical_window_storage_source_module(*, claim, module)`.
Storage must exist only at canonical key
`scripts.historical_foundry_storage`, its `__spec__.name` must equal that exact
name, and the passed object must be that exact `sys.modules` object. There is no
`__main__` storage fallback.

Every resolver substep verifies both `module.__spec__.origin` and
`module.__file__` resolve to the held descriptor path, rechecks the held
path/inode/file identity/bytes/hash, and records `role, fixed canonical name,
actual key, exact module object` in the binder's closure-local state. The
atomic binder then duplicates and transfers all required source and ancestry
descriptors into the storage-owned spool binding, with pre/post-duplication
identity/bytes/hash checks. Finalization requires both roles bound in that live
binding and rechecks the same key/spec/object/origin/file plus duplicate
FD/path/inode/bytes/hash.
Any missing, conflicting-alias, replaced, reloaded, or drifted binding raises exactly
`authority_mismatch/final_identity_drift`. No neutral handoff module is added,
so there is no third row.

The binding survives ordinary claimed-finalizer cleanup independently of the
original source-authority object. It moves active spool → sealed spool → minted
capability without copying, and every recheck before mint/consume uses the
actual `sys.modules` key/spec/object/origin/file plus independently held source
and ancestry FDs, inode/file identity, bytes, and SHA-256. The minted capability
binds that still-live source binding as well as its closed source-identity
projection, held opening config identity, sealed spool, compact projection, and
post-ledgers; a closed projection alone is never sufficient. Its class,
constructor provenance, non-exported closure issuer, exact verifier, one-shot
`mint_production_historical_window_capability` method, and
`consume_production_historical_window_capability` function remain storage-owned;
the capability is immutable, redacted, noncopyable, nonpickleable,
nonserializable, exact-type checked, and single-consumer. Direct issuer calls,
cross-spool/cross-claim inputs,
pre-finalization or pre-reconciliation mint attempts, and repeat mint all fail
closed and mint nothing.

On pre-finalization failure, abort, spool close, cancellation, post-finalization
reconciliation failure, or failed first mint, the current owner revokes the
binding and closes every duplicate FD exactly once. It neither closes before
the original RPC finalizer nor survives a failed lifecycle. On successful mint,
Task 4b's exact consume moves the binding into the exact consumed view while
invalidating the capability. Storage treats that view as an intermediate state
inside the one Task-4b consume/materialize transaction, not as a completed
handoff that may be abandoned; the materializer must transfer the closed source
identity projection and ownership of the necessary held descriptors into its
descriptor-held staging snapshot before revoking/closing the consumed binding
and any duplicates not moved. Cancellation or failure during that transfer
revokes/closes exactly once and issues no snapshot. Post-consume use, repeated
consume, copying, and transplanting reject.

- [ ] **Step 5: Run offline dual-runtime GREEN and commit**

The RED/GREEN matrix includes fresh/used/raced claims; held-config identity;
generic-batch rejection; zero resident raw bytes; outstanding transfer;
pending validation, commit revalidation, abort rollback/failure, and
append/receipt/commit failure; compact finalization recursive raw-field absence;
three-segment indices; full anchor/lower replay gates; six-row reserve `3+3`
reassembly with poisoned leaf semantics; the existing explicit-scope state
machine, 413 catch location, pending order, one normal exit, exceptional exit,
cleanup, and cancellation; exact cleanup before/after finalization;
receipt/transfer copy/pickle/reuse/transplant; reconciliation mismatch;
final-anchor-before-finalize order; maximum-window one live transfer; and held
source fixed-name binding and origin/file/inode/bytes/hash/exact-`sys.modules`
identity drift at source bind, first spool handoff, or finalization. Subprocess REDs
cover direct `python -m scripts.historical_foundry_scan` binding through exact
`__main__` spec/key/object, the connected runner's canonical imported-module
binding, dual-key same-object acceptance, dual-key different-object rejection,
no-import rejection, storage canonical-key-only binding, and missing/reloaded/
conflicting-alias module rejection. Bridge REDs cover exact bound-module claim/
finalization/reconciliation classes and verifier calls; mapping/hash/bool/
lookalike substitution; direct issuer access; cross-spool/cross-claim;
pre-finalization/pre-reconciliation; forged bound-module classes; repeated
mint; and repeated consume before writer open.
Source-retention REDs cover transfer/finalize/mint before bind; second bind;
cross-spool/claim and forged module/object/FD/path/hash inputs; drift before and
after bind; successful recheck and mint after the original preflight source
authority closes; closed binding before mint; no leaked duplicate FDs after a
failed or cancelled bind; exact-once close on post-finalization reconciliation
failure; mint-to-consume ownership movement and exact-once revocation; and
rejection of post-consume or repeated binding use.

```bash
python3 -m unittest \
  tests.test_historical_foundry_rpc \
  tests.test_historical_foundry_scan \
  tests.test_historical_foundry_storage \
  tests.test_historical_foundry_contracts \
  tests.test_bounded_json -v
/private/tmp/cpython-3.8.10-runtime-20260820/bin/python3.8 -B -m unittest \
  tests.test_historical_foundry_rpc \
  tests.test_historical_foundry_scan \
  tests.test_historical_foundry_storage \
  tests.test_historical_foundry_contracts \
  tests.test_bounded_json -v
python3 -m py_compile \
  scripts/historical_foundry_rpc.py \
  scripts/historical_foundry_scan.py \
  scripts/historical_foundry_storage.py \
  tests/test_historical_foundry_rpc.py \
  tests/test_historical_foundry_scan.py \
  tests/test_historical_foundry_storage.py
/private/tmp/cpython-3.8.10-runtime-20260820/bin/python3.8 -B -m py_compile \
  scripts/historical_foundry_rpc.py \
  scripts/historical_foundry_scan.py \
  scripts/historical_foundry_storage.py \
  tests/test_historical_foundry_rpc.py \
  tests/test_historical_foundry_scan.py \
  tests/test_historical_foundry_storage.py
git diff --check
git add scripts/historical_foundry_rpc.py \
  tests/test_historical_foundry_rpc.py \
  scripts/historical_foundry_scan.py \
  tests/test_historical_foundry_scan.py \
  scripts/historical_foundry_storage.py \
  tests/test_historical_foundry_storage.py
git commit -m "feat(opportunity): capture and reconcile historical window"
```

Do not run a real endpoint or write typed chunks in this slice. Production
capability issuance is exercised only with sealed offline Task-2b transport
fixtures until Task 4b and Tasks 5–7 are complete.

## Task 4b: Materialize Immutable Capture Chunks and a Held Snapshot

**Files:**

- Modify: `scripts/historical_foundry_storage.py`
- Modify: `tests/test_historical_foundry_storage.py`
- Modify: `scripts/historical_foundry_scan.py`
- Modify: `tests/test_historical_foundry_scan.py`

**Interfaces:**

- Consumes: only the exact single-consumer storage-owned
  `_ProductionHistoricalWindowCapability` whose sealed-spool-bound one-shot
  `mint_production_historical_window_capability` method invoked storage's
  non-exported closure issuer after exact Task-2b
  finalization and Task-3b reconciliation. The private ingress calls
  `consume_production_historical_window_capability(*, capability)` and accepts
  only its exact private consumed view; it rejects Task-3a mappings, copied
  projections/ledgers, direct spools, transfers, caller rows/bytes, lookalike
  classes, and test capabilities before opening a writer.
- Produces: descriptor-reread immutable raw-exchange and typed capture chunks,
  `scan/capture_inventory.json`, and a `HistoricalRunStagingSnapshot` bound to
  the same held staging ancestry, the transferred source descriptor authority,
  and unified Task-4a quota. It does not rerun collection, accept an endpoint,
  or mint production authority.

- [ ] **Step 1: Write production-ingress and filesystem RED tests**

Freeze a held-descriptor snapshot rather than a path-based `Mapping`:

```python
def _materialize_historical_window_staging_snapshot(
    *,
    capability: "_ProductionHistoricalWindowCapability",
) -> "HistoricalRunStagingSnapshot": ...

def open_validated_run(
    *,
    data_dir: Path,
    run_id: str,
    expected_manifest_sha256: str,
) -> "HistoricalRunSnapshot": ...

class HistoricalRunSnapshot:
    def read_member(
        self,
        relative_path: str,
        *,
        expected_sha256: str,
        max_bytes: int,
    ) -> bytes: ...

    def identity_projection(self) -> Mapping[str, Any]: ...
    def reread_unchanged(self) -> None: ...
    def close(self) -> None: ...

class HistoricalRunStagingSnapshot:
    """Read-only descriptor snapshot issued only by the private staging writer."""

    def read_frozen_member(
        self,
        relative_path: str,
        *,
        expected_sha256: str,
        max_bytes: int,
    ) -> bytes: ...

    def frozen_identity_projection(self) -> Mapping[str, Any]: ...
    def reread_frozen_members_unchanged(self) -> None: ...
```

`data_dir` is the one storage-location parameter and was already descriptor-opened
by Task 4a. It must be absolute, pre-existing, operator-owned, and opened
no-follow through stable ancestry; it changes no evidence semantics and is
retained only as a redacted root identity. Task 4b reuses the sealed spool's
exact ancestry and monotonic quota rather than reopening a caller path or
starting a new allowance. `HistoricalRunSnapshot.read_member` may open only a
path/size/hash in the validated final manifest. Logical `run:<64hex>` maps
internally to its safe suffix. `HistoricalRunStagingSnapshot` has no path-opening
constructor.

`_materialize_historical_window_staging_snapshot` function-locally late-imports
storage, calls the exact
`consume_production_historical_window_capability` seam, and uses only the
returned `_ConsumedProductionHistoricalWindowCapabilityView` to stream the
sealed spool once. Storage performs exact type/issuer/currentness verification
and atomically marks the capability consumed before returning that private view.
That transition moves, rather than copies, the live
`_HistoricalWindowSpoolSourceBinding` into the view. Before issuing the staging
snapshot, the materializer rechecks its exact module key/spec/object/origin/file
and duplicate source/ancestry FD identities, then moves the closed source
identity projection and ownership of all necessary held descriptors into the
snapshot. Only after snapshot acceptance does the consumed view revoke/close
its binding exactly once and close any duplicate not moved. A failure or
cancellation during consume/materialization performs the same exact-once
cleanup and produces no snapshot; post-consume use and repeat consume reject.
The returned view is an intermediate private state inside that single
transaction: consume succeeds only when the snapshot accepts the source
transfer, not merely when the view is returned.
It independently rereads and verifies every framed request/decoded payload
against Task-3b finalization and ledgers, and writes no-replace immutable chunks
for `rpc/`, `headers/`, `reserves/`, `prices/`, and `fees/` plus the three copied
configs and `scan/capture_inventory.json`. Raw `rpc/*.bin` chunks concatenate
complete spool frames without splitting a frame and are capped at 16 MiB; typed
canonical JSON gzip chunks and the canonical capture inventory are each capped
at 16 MiB decoded. The inventory binds each raw frame to its compact success
record, committed receipt, physical leaf/root, typed role/count/digest, and
global request-ID range. No endpoint, wire body, HTTP header, path, or exception
text is written; the only raw bodies retained are canonical request bytes and
bounded decoded success-response bytes.

The private writer accepts only the Task-3b bridge's internally derived closed
role enum and canonical bytes, never a caller relative path, row mapping, or
fixture projection. It uses the same quota capability: every new chunk/config/
inventory debit is added to all prior spool debits with no reset or credit. It
fsyncs, descriptor-rereads, decodes, reprojects, and irrevocably freezes the
capture role set before issuing the initial staging snapshot. The private spool
is then closed; the frozen immutable raw chunks, not the spool path, are the
retained evidence.

Implement descriptor operations by reusing
`_open_verified_directory`, `_open_directory_at`, `_write_new_bytes_at`,
`_read_bounded_open_file`, `_read_bounded_bytes_at`,
`_rename_directory_noreplace_at`, `_verify_directory_entry_snapshot`,
`_verify_open_path_snapshot`, and `_fsync_directory` from
`scripts/route_publication.py`, not weaker path logic or replace/backup behavior.

Tests cover exact capability type/issuer/currentness, exact consumed-view type,
one-shot consume/repeat rejection, fixture mapping and direct-spool rejection
before writer open, exact directory/member grammar,
canonical raw framing/gzip, physical size/SHA/logical count/range inventory,
no-replace writes, descriptor reread, stable ancestry,
symlink/hardlink/traversal/unexpected-member rejection, chunk and unified
run-limit exact/+1, quota non-reset across spool/chunks, TOCTOU swaps, failure
cleanup, cancellation, and sanitized exceptions. Decode/reparse bytes to verify
every digest, typed row count, ledger join, and range instead of trusting the
compact projection or future `run_manifest.json`.

The eventual exact run tree includes `run_manifest.json`, copied physical
`policy.json`/`authority.json`/`toolchain.json`, immutable raw `rpc/*.bin`,
chunked `headers/`, `reserves/`, `prices/`, `fees/`, and `scan/`,
`candidate_manifest.json`, selected `typed/<market_key>/...` plus
`typed_manifest.json`, per-scenario
`foundry/<block>/<scenario>/overlay.json|receipt.json|trace.json.gz|result.json`,
and `selection.json`. Task 4b freezes only the capture role set; Tasks 5–7 add
later role sets through the same held writer/quota. Unknown or prematurely
present roles reject.

- [ ] **Step 2: Run RED**

```bash
python3 -m unittest tests.test_historical_foundry_storage -v
```

- [ ] **Step 3: Implement capture-chunk materialization and the staged/final state machine**

Implement the capture freeze now and the writer states needed by Tasks 5–7. The
writer may append only the next closed role set, fsync/freeze it, and issue a new
snapshot whose reads are limited to frozen inventory while the writer adds later
roles. After Task 5 it freezes `scan/prefilter/*.json.gz`; after Task 6 it freezes
scenario quartets; Task 7 alone adds candidates/typed/selection and finalizes.
The state machine proves `run_manifest.json` will be the final and only manifest
write: create once with `O_CREAT|O_EXCL`, revoke the writer, reread manifest and
all inventoried members unchanged, fsync, rename once no-replace to
`<validated-data-dir>/raw/historical-foundry-replay/<run_id>`, reopen with
`open_validated_run`, reread, and close. There is no provisional/hidden manifest,
overwrite, backup/replace, write after manifest, or market-ID path. A schema-valid
closed nonpublication run uses the same final protocol; an earlier failure leaves
no final raw run.

- [ ] **Step 4: Run dual-runtime GREEN and maximum-window streaming corpus**

```bash
python3 -m unittest \
  tests.test_historical_foundry_storage \
  tests.test_historical_foundry_scan -v
/private/tmp/cpython-3.8.10-runtime-20260820/bin/python3.8 -B -m unittest \
  tests.test_historical_foundry_storage \
  tests.test_historical_foundry_scan -v
python3 -m py_compile \
  scripts/historical_foundry_storage.py \
  scripts/historical_foundry_scan.py
/private/tmp/cpython-3.8.10-runtime-20260820/bin/python3.8 -B -m py_compile \
  scripts/historical_foundry_storage.py \
  scripts/historical_foundry_scan.py
git diff --check
```

The maximum corpus proves one current spool frame, one current reconstructed
root of at most 40 envelopes, bounded chunk builders, exact raw/typed joins, and
no in-memory accumulation of the full window. It also proves the production
capability is consumed once and that Task-3a fixture mappings cannot advance the
writer even when every visible schema/hash field is copied.

- [ ] **Step 5: Commit immutable capture materialization**

```bash
git add scripts/historical_foundry_storage.py \
  scripts/historical_foundry_scan.py \
  tests/test_historical_foundry_storage.py \
  tests/test_historical_foundry_scan.py
git commit -m "feat(opportunity): retain immutable historical scan evidence"
```

## Task 5: Build, Persist, and Validate the Exact Prefilter Grid

**Files:**

- Modify: `scripts/historical_foundry_scan.py`
- Modify: `tests/test_historical_foundry_scan.py`

**Dependency/ingress:** Consume only the Task-4b descriptor-held capture snapshot
created from the storage-owned capability issued after Task-3b reconciliation.
Do not accept a Task-3a
projection, spool, plan, caller range/count, or raw row mapping. The prefilter
arithmetic, 2×5 denominator, decisions, persistence, and validation algorithm
below are unchanged.

- [ ] **Step 1: Add full-grid RED tests**

Freeze held, manifest-bound capabilities rather than accepting caller mappings:

```python
class ValidatedHistoricalWindow:
    """Opaque, module-issued capability with read-only identity properties."""

class ValidatedHistoricalPrefilterGrid:
    """Opaque, module-issued capability with read-only count/digest properties."""

def open_validated_historical_window(
    *,
    config: HistoricalFoundryConfigSet,
    staging: HistoricalRunStagingSnapshot,
) -> ValidatedHistoricalWindow: ...

def build_historical_prefilter_grid(
    *,
    config: HistoricalFoundryConfigSet,
    window: ValidatedHistoricalWindow,
) -> Tuple[Mapping[str, Any], ...]: ...

def validate_historical_prefilter_grid(
    *,
    config: HistoricalFoundryConfigSet,
    window: ValidatedHistoricalWindow,
    staging: HistoricalRunStagingSnapshot,
) -> ValidatedHistoricalPrefilterGrid: ...
```

Both validated classes have module-private constructors and carry a private issuer token tied to the held staging descriptors; merely constructing a lookalike object or copying their visible properties fails type/token checks. Their visible properties are exactly the identity/count fields shown in the design (`scan_inventory_sha256`, lower/anchor/count denominators, `coverage_digest`, row/decision counts, and `grid_digest`). Internally, `ValidatedHistoricalWindow` retains the held capture snapshot and exposes only a module-private ordered iterator over validated header/reserve/price/fee records, which is how grid construction reads evidence without accepting caller rows.

`open_validated_historical_window` rereads the staging capability's frozen, inventoried headers/reserves/prices/fees/capture-inventory members, proves the exact inclusive range and zero gaps, and computes the denominator fields itself. It is the only route into grid construction; neither a path nor a caller-provided coverage count/window mapping is accepted. For every block require exactly two directions × five notionals, canonical `(block_number desc, direction, notional)` ordering, exact route/scenario keys, integer swap outputs, USD/gas/MEV fractions, and one decision `safe_excluded|replay_required`. The production controller writes the built rows through the private storage writer to `scan/prefilter/*.json.gz`, fsyncs and freezes that role set, and obtains a new staging snapshot. `validate_historical_prefilter_grid` rereads those frozen bytes; it never accepts caller rows. The validated grid must have exactly `block_count * 10` rows and its two decision counts must sum to that denominator. Recompute everything during validation. Rehashing a forged stored decision, output, rate, reserve, price, block, reason, range, denominator, or digest must still fail.

- [ ] **Step 2: Run RED**

```bash
python3 -m unittest \
  tests.test_historical_foundry_scan.HistoricalPrefilterGridTests -v
```

- [ ] **Step 3: Implement the pure grid**

Call the Phase-1 arithmetic; do not add any scanner-local rate or heuristic. Store exact numerator/denominator where needed and canonical fixed-point display projections separately. Persist/freeze/reread the complete grid through the protocol above before returning `ValidatedHistoricalPrefilterGrid`.

- [ ] **Step 4: Run GREEN including zero-rate KAT**

```bash
python3 -m unittest \
  tests.test_historical_foundry_contracts \
  tests.test_historical_foundry_scan -v
```

- [ ] **Step 5: Commit the prefilter slice**

```bash
git add scripts/historical_foundry_scan.py tests/test_historical_foundry_scan.py
git commit -m "feat(opportunity): derive exact historical prefilter grid"
```

## Task 6: Apply the Sealed Overlay and Capture One Fresh-Fork Scenario

**Files:**

- Create: `scripts/historical_foundry_anvil.py`
- Create: `tests/test_historical_foundry_anvil.py`
- Modify: `scripts/historical_foundry_storage.py`
- Modify: `tests/test_historical_foundry_storage.py`

**Dependency/ingress:** Consume only the Task-4b staging snapshot plus the
Task-5 validated window/grid capabilities issued from its frozen bytes. The
overlay, fresh-process, transaction, receipt, trace, and economic algorithm
below is unchanged. This task runs only offline fixture/KAT tests; its connected
repeat is deferred to Task 7's final connected gate.

- [ ] **Step 1: Write sealed-context overlay/receipt RED tests**

Freeze a capability-producing boundary; state override derivation accepts no runtime bytes, address/value maps, or paths:

```python
def open_historical_replay_context(
    *,
    config: HistoricalFoundryConfigSet,
    staging: HistoricalRunStagingSnapshot,
    window: ValidatedHistoricalWindow,
    grid: ValidatedHistoricalPrefilterGrid,
    executor_artifact: ValidatedExecutorArtifact,
) -> "HistoricalReplayContext": ...

def build_historical_state_override(
    *,
    context: "HistoricalReplayContext",
    scenario: "ValidatedReplayScenario",
) -> Mapping[str, Any]: ...
```

The production orchestrator is private and accepts only validated internal objects:

```python
class ScenarioEvidenceSink:
    def write_member(
        self,
        role: str,
        canonical_bytes: bytes,
    ) -> Mapping[str, Any]: ...

def _replay_historical_scenario(
    context: HistoricalReplayContext,
    scenario: ValidatedReplayScenario,
    sink: ScenarioEvidenceSink,
) -> Mapping[str, Any]: ...
```

`HistoricalReplayContext` can be created only after it rereads the mutually bound config, project-local toolchain, executor artifact, held staging snapshot, complete window, and exact grid. It derives the runtime internally from `ValidatedExecutorArtifact` and requires its creation/runtime/immutable-patch digests to equal the toolchain authority. `ValidatedReplayScenario` is issued only by the validated grid and cannot be constructed from a caller mapping.

RED tests cover exact executor code/nonce/native balance, WETH backing adjustment, WETH/UNI balances, four direction-specific allowance slots, prior zeros, changed-account/slot inventory, getter readbacks, unchanged pair reserves/balances, synthetic B+1/B.timestamp+12/child base fee, type-2 envelope, empty access list, fixed sender nonce, transaction index zero, one transaction, status one/allowlisted status zero, actual first-leg UNI delta, final WETH delta, residuals, and no `GASPRICE` opcode in relevant traces. A decisive test flips one byte in executor runtime after validation and proves context creation/overlay derivation fails before Anvil starts; another places hostile binaries on `PATH` and proves the Phase-1 project-local hashes still govern.

Reject a caller-provided binary path, command flag, endpoint, private key, router, token, pair, sender, executor, slot, value, timestamp, gas, calldata, or direction outside the scenario key.

- [ ] **Step 2: Run RED**

```bash
python3 -m unittest tests.test_historical_foundry_anvil -v
```

- [ ] **Step 3: Implement fixed subprocess orchestration**

Launch Anvil only through the Phase-1 held project-local toolchain capability. The parent first starts a no-log loopback HTTP relay on a privately reserved port; the relay alone holds the sealed archive endpoint context, enforces the exact fork-relay method/byte/deadline allowlist above, disables retries/cookies/redirects/proxies, and has no disk cache. Anvil receives `--fork-url` with only that credential-free loopback relay URL. Its internally fixed launch vector also includes the equivalents of `--fork-block-number B`, `--chain-id 1`, `--hardfork` at the authority's exact `fork_hardfork`, `--host 127.0.0.1`, a privately reserved `--port`, `--no-mining`, `--no-cors`, `--silent`, `--order fifo`, `--steps-tracing`, `--retries 0`, `--timeout 30000`, and `--no-storage-caching`; no IPC flag, public bind, config output, or inherited Anvil/Foundry option is allowed, and each process runs in a new private empty working directory removed after reap.

The retained redacted argv projection has exactly `schema=historical_foundry_anvil_argv/v1`, binary SHA-256, fixed flag/value pairs, selected block, hardfork, and `fork_url_kind=loopback_relay`; it omits both dynamic loopback ports and contains no endpoint string. Capture readiness by bounded `eth_chainId` polling under the exact 30-second/64-KiB limits without retaining stdout/stderr. Verify fork-base header and full selected-block authority before mutations. Invoke only the exact local-method allowlist above, apply the exact overlay, reread it, set next timestamp/base fee, submit one fixed type-2 transaction, mine one block, capture receipt/trace under the frozen limits, and execute the exact 5-second SIGTERM then 5-second SIGKILL/reap sequence on every path. Stop/reap the relay and delete the private work directory on every path, then preserve `KeyboardInterrupt`/`SystemExit`.

Write exact `overlay.json`, `receipt.json`, `trace.json.gz`, and `result.json` members only through the storage-issued sink. The return value contains normalized projections/digests, never raw member bytes or an output path. `result.json` is a projection, never authority over receipt/trace/balances.

- [ ] **Step 4: Run offline GREEN with two-repeat fixture KAT on both runtimes**

```bash
python3 -m unittest \
  tests.test_historical_foundry_anvil \
  tests.test_historical_foundry_storage -v
/private/tmp/cpython-3.8.10-runtime-20260820/bin/python3.8 -B -m unittest \
  tests.test_historical_foundry_anvil \
  tests.test_historical_foundry_storage -v
```

The offline fixture KAT executes the same fixed scenario twice in independent
test-controlled processes and asserts identical selected state, token deltas,
gas used, overlay hash, calldata hash, and executor runtime hash. It does not
read `DEX_DEPTH_RPC_ETH` or make a connected completion claim.

- [ ] **Step 5: Commit the replay slice**

```bash
git add scripts/historical_foundry_anvil.py \
  scripts/historical_foundry_storage.py \
  tests/test_historical_foundry_anvil.py \
  tests/test_historical_foundry_storage.py
git commit -m "feat(opportunity): capture fresh-fork scenario evidence"
```

## Task 7: Resolve Candidates Descending and Finalize Selection

**Files:**

- Modify: `scripts/historical_foundry_scan.py`
- Modify: `scripts/historical_foundry_storage.py`
- Modify: `tests/test_historical_foundry_scan.py`
- Modify: `tests/test_historical_foundry_storage.py`
- Create: `docs/superpowers/reports/2026-08-20-historical-foundry-scan-replay-report.md`

**Dependency/ingress:** Consume only the latest Task-4b staging snapshot and the
Task-5/6 opaque validated window, grid, scan, and replay-ledger capabilities.
Task-3a mappings and Task-4a spools are never accepted directly. Candidate
state transitions, descending selection, economics, and publication eligibility
below are unchanged.

- [ ] **Step 1: Write selection RED tests**

Freeze sealed scan/ledger capabilities and selection:

```python
class ValidatedHistoricalScanSnapshot:
    """Opaque, module-issued capability over frozen window and grid bytes."""

def open_validated_historical_scan_snapshot(
    *,
    config: HistoricalFoundryConfigSet,
    staging: HistoricalRunStagingSnapshot,
) -> ValidatedHistoricalScanSnapshot: ...

def select_historical_replay_block(
    *,
    snapshot: ValidatedHistoricalScanSnapshot,
    replay_ledger: "ValidatedHistoricalReplayLedger",
) -> Mapping[str, Any]: ...

def build_selected_historical_typed_members(
    *,
    config: HistoricalFoundryConfigSet,
    snapshot: ValidatedHistoricalScanSnapshot,
    selection: Mapping[str, Any],
) -> Mapping[str, bytes]: ...
```

`ValidatedHistoricalScanSnapshot` and `ValidatedHistoricalReplayLedger` have module-private constructors and private issuer tokens bound to held descriptors. The scan snapshot exposes only read-only `staging_inventory_sha256`, validated window/grid capabilities, candidate-block count, and candidate-scenario denominator. The denominator is exactly `candidate_block_count * 10`; the separate `initial_replay_required_count` records only prefilter-required rows and may not replace that denominator. `open_validated_historical_scan_snapshot` independently rereads the second-level scan snapshot's frozen window/grid inventory, rebuilds the complete window and `block_count * 10` grid, and derives both counts; callers cannot supply a window, range, coverage flag, grid row, count, or lookalike capability. `ValidatedHistoricalReplayLedger` is issued only by the replay context after it freezes and rereads all four evidence members per attempted scenario and recomputes receipt/trace/overlay/result closure.

Freeze this per-block state machine and no other status strings:

```text
all ten safely excluded -> prefilter_non_candidate -> resolved_nonpositive
at least one replay_required -> candidate -> replaying_required
replaying_required + unexpected failure -> unresolved -> terminal stop
replaying_required + complete/no positive -> resolved_nonpositive -> continue older
replaying_required + policy-positive -> tentative_positive -> completing_full_ten
completing_full_ten + all ten status-one + at least one exact policy-positive -> selected -> terminal winner
completing_full_ten + allowlisted closed revert -> nonpublishable_positive -> continue older
completing_full_ten + all ten status-one/no positive -> resolved_nonpositive -> continue older
completing_full_ten + unexpected failure -> unresolved -> terminal stop
candidate older than selected -> not_needed_older_than_selected
```

Test newest full ten-success positive wins under the exact predicate `all(status == 1 for ten receipts) and any(policy_net_edge_usd > 0 for ten scenarios)`; newer unresolved blocks stop selection; a tentative newer block whose completion pass produces an allowlisted closed revert is retained as `nonpublishable_positive` and scanning demonstrably continues to the next older candidate; unknown revert becomes `unresolved`; the initially replayed successful quartet for each required scenario is frozen as that scenario's final evidence and completion runs only the not-yet-replayed rows, each in its own fresh process, so the selected ledger contains exactly one quartet for each of ten scenario keys; older candidates become `not_needed_older_than_selected`; an all-negative or only-nonpublishable-positive complete window closes `no_publishable_profitable_block`; incomplete coverage, a missing state transition, a forged denominator, duplicate scenario evidence, or an unresolved candidate can never produce that state or a winner.

- [ ] **Step 2: Run RED**

```bash
python3 -m unittest \
  tests.test_historical_foundry_scan.HistoricalCandidateSelectionTests -v
```

- [ ] **Step 3: Implement descending controller and final manifest**

The I/O controller processes candidate blocks in descending order using only the sealed snapshot and ledger. It records every transition above and continues after `nonpublishable_positive`; it never jumps from prefilter output directly to selection. After a tentative winner, retain each already successful required-scenario quartet at its unique no-replace path and run only the remaining scenario keys until exactly ten unique fresh-process quartets exist; never replay or overwrite an existing key. Then derive both selected-block canonical `dex_pool_state` and `dex_usd_price_context` members and write them under manifest-derived market-key directories. Before the one final manifest is created, reread every staged member and recompute coverage denominator, prefilter grid, replay resolution, receipt economics, typed semantics, overlay-set digest, scenario counts, and selection. Reread the anchor by number and require the original hash. Write `candidate_manifest.json`, `typed_manifest.json`, and `selection.json`, then create `run_manifest.json` once as the final staged member and perform the Task-4b single no-replace directory rename. A closed nonpublication run has an exact empty selected typed inventory and can never advance publication.

Phase 2 does not create public cost rows, but each selected status-one `result.json` carries one closed `historical_foundry_cost_proof_inputs/v1` object with exactly `schema`, `scenario_key`, `policy_sha256`, `receipt_sha256`, `trace_sha256`, `adapter_proof_sha256`, `rows`, and `proof_inputs_hash`. `proof_inputs_hash` is the typed hash over the other seven fields. `rows` is an ordered nine-element array; every row has exactly `grain`, `component`, `value_status`, `embedded`, `amount_usd_exact`, `rate_bps_exact`, `proof_role`, and `proof_sha256`. The exact ordered identities/statuses are: buy pool fee `bounded_estimate/embedded=true`; buy router fee `bounded_estimate/false`; buy token tax `bounded_estimate/false`; sell pool fee `bounded_estimate/true`; sell router fee `bounded_estimate/false`; sell token tax `bounded_estimate/false`; route gas `assumed/false`; route transfer `not_applicable/false`; route MEV `assumed/false`.

Both V2 pool rates are exactly 30 bps and their reserve-derived amounts are informational, never deducted again. Router/tax amounts and rates are canonical numeric zero only with receipt/balance/adapter proof. Route gas equals `gasUsed * effectiveGasPrice` converted with the block-bound price. Transfer has null amount/rate and proof that the single transaction has no external rebalance leg. MEV amount/rate are exact and policy-hash bound. `proof_role` and `proof_sha256` bind the corresponding retained evidence member. Phase 3 must accept this typed object directly and may not reconstruct a private variant. The Phase-2 run validator rejects a missing/extra/reordered row or field, wrong typed hash, leg-level gas, unproved router/tax zero, MEV drift, or treating embedded pool fees as an extra deduction.

- [ ] **Step 4: Run the full Phase 2 suite on both runtimes**

```bash
python3 -m unittest \
  tests.test_bounded_json \
  tests.test_historical_foundry_contracts \
  tests.test_historical_foundry_toolchain \
  tests.test_historical_foundry_rpc \
  tests.test_historical_foundry_scan \
  tests.test_historical_foundry_storage \
  tests.test_historical_foundry_anvil \
  tests.test_route_cost_collector \
  tests.test_route_cost_evidence \
  tests.test_route_quantity -v
/private/tmp/cpython-3.8.10-runtime-20260820/bin/python3.8 -B -m unittest \
  tests.test_bounded_json \
  tests.test_historical_foundry_contracts \
  tests.test_historical_foundry_toolchain \
  tests.test_historical_foundry_rpc \
  tests.test_historical_foundry_scan \
  tests.test_historical_foundry_storage \
  tests.test_historical_foundry_anvil \
  tests.test_route_cost_collector \
  tests.test_route_cost_evidence \
  tests.test_route_quantity -v
python3 -m py_compile \
  scripts/bounded_json.py \
  scripts/historical_foundry_rpc.py \
  scripts/historical_foundry_scan.py \
  scripts/historical_foundry_storage.py \
  scripts/historical_foundry_anvil.py
/private/tmp/cpython-3.8.10-runtime-20260820/bin/python3.8 -B -m py_compile \
  scripts/bounded_json.py \
  scripts/historical_foundry_rpc.py \
  scripts/historical_foundry_scan.py \
  scripts/historical_foundry_storage.py \
  scripts/historical_foundry_anvil.py
git diff --check
```

- [ ] **Step 5: Commit the source, run the connected Phase-2 gate, then commit the report**

The production preflight intentionally rejects uncommitted source. Do not run
either connected command until Tasks 3a, 4a, 3b, 4b, 5, 6, and 7 have each
passed their offline system/exact-CPython gates and independent review, and all
source slices are committed. Then commit the tested scanner/controller before
the first Phase-2 anchor request:

```bash
git add scripts/historical_foundry_scan.py \
  scripts/historical_foundry_storage.py \
  tests/test_historical_foundry_scan.py \
  tests/test_historical_foundry_storage.py
git commit -m "feat(opportunity): finalize historical replay selection"
git status --porcelain=v1 --untracked-files=all
/private/tmp/cpython-3.8.10-runtime-20260820/bin/python3.8 -B \
  -m scripts.historical_foundry_anvil --verify-connected-repeat
/private/tmp/cpython-3.8.10-runtime-20260820/bin/python3.8 -B \
  -m scripts.historical_foundry_scan --collect-connected \
  --data-dir "$MARKET_DATA_DIR"
```

The status command must print nothing. The connected repeat reads only
`DEX_DEPTH_RPC_ETH`, the checked-in KAT fixture, tracked authorities, and the
project-local toolchain; it accepts no block/scenario/runtime/limit/flag override
and absence of the variable is a hard failure, never a skip. Only after it
succeeds may `--collect-connected` run. That command requires exactly one
absolute `--data-dir` and accepts no block, window, scenario, policy, rate,
endpoint, tool path, process flag, limit, deadline, or arbitrary member/output-
path argument. The root is validated by the descriptor contract above and is
the same root consumed by Phase 3. It must collect the complete finalized
seven-day window through the Task-3b→4b capability boundary and perform real
fresh-Anvil candidate replay; an absent RPC variable, unavailable exact Python/
toolchain, unresolved candidate, resource failure, or no publishable winner
returns nonzero with the exact closed run state and never skips. Preserve the
immutable raw run even for a schema-valid closed nonpublication result.

Report the maximum synthetic counts (50,401 headers, 100,802 reserve rows, 50,401 price rows) separately from actual connected full-window counts and record the resulting run/manifest/selection identities, candidate-state counts, and fresh-Anvil repeat evidence without endpoint or path. State honestly whether the connected run selected a ten-success block or closed nonpublication; only the former is eligible for Phase 3, and neither is yet a public Opportunity.

```bash
git add docs/superpowers/reports/2026-08-20-historical-foundry-scan-replay-report.md
git commit -m "docs(opportunity): report connected historical replay scan"
git diff --check HEAD^ HEAD
```

## Phase Exit Review

- [ ] Independently recompute the inclusive block count and every per-role row count from member bytes.
- [ ] Require connected exact-CPython-3.8.10 repeat and
  `scripts.historical_foundry_scan --collect-connected --data-dir "$MARKET_DATA_DIR"`
  evidence from a clean committed HEAD only after every Task 3a–7 offline slice
  is committed and review-clean; unit fixtures and the Phase-1 fixed-block KAT
  do not satisfy the Phase-2 gate.
- [ ] Attack exact response-ID sets, EIP-1898 block hashes, header continuity, Chainlink freshness, fee chunks, and anchor reread.
- [ ] Prove optional logs cannot change prefilter or selection.
- [ ] Attack zero-MEV policy behavior and exact-zero net boundary.
- [ ] Verify every selected scenario has a unique overlay preimage and fresh Anvil process.
- [ ] Verify exact archive/Anvil method allowlists, all frozen byte/deadline limits, process cleanup/kill escalation, project-local executable rehashing, and secret-free artifacts.
- [ ] Verify selected results carry the closed proof inputs for the future nine-row topology and do not create or duplicate cost rows in Phase 2.
- [ ] Record pre/post existence, exact bytes, and SHA-256 for both live and historical route pointers and prove neither changed as a consequence of this phase.
- [ ] Request independent code review before building the publication bridge.
