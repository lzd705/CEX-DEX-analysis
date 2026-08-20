# Historical Foundry Scan and Replay Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Produce one immutable, locally validated seven-day historical raw run whose complete block/reserve/price/fee inventory supports exact safe prefiltering and descending fresh-Anvil candidate resolution.

**Architecture:** A sealed archive-RPC client freezes one finalized anchor, binary-searches the exact lower bound, captures every header and three hash-bound state calls per block plus fee history, and writes descriptor-verified immutable chunks. Pure scanner code recomputes the 2×5 grid and chooses replay candidates without trusting stored booleans. A private Anvil orchestrator creates a fresh fork per candidate scenario, applies the policy-derived overlay, mines exactly one transaction, and retains receipt/trace/result evidence. This phase finalizes raw evidence and selection only; it does not publish a historical core or public pointer.

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
| staged run | 8 GiB physical bytes and 200,000 members |
| process shutdown | send SIGTERM once, wait 5 seconds, send SIGKILL once if still alive, wait/reap at most 5 more seconds |

Limit/deadline exhaustion maps to the owning closed terminal reason, never retries, never becomes `no_publishable_profitable_block`, and never retains body/output text. The relay applies the global JSON-shape limits as well as its own rows above; it rejects request batching before opening an upstream socket. Tests exercise every archive, relay-inbound, relay-upstream, relay-downstream, local-Anvil, and stored-member byte limit at the exact limit and limit+1; relay cumulative counters before and after decoding; deadline equality and +1 monotonic tick for archive, relay, local Anvil, scenario, and full-run clocks; stdout/stderr overflow; trace compression bombs; total-run exhaustion; SIGTERM success; SIGTERM timeout followed by SIGKILL; and unreaped-child failure.

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
python3.8 -m unittest \
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
    context: "_ArchiveRpcRunContext",
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

Use fixed POST, `ProxyHandler({})`, no redirects, a fixed Accept/Content-Type/User-Agent set, no retries, the frozen resource table, shared bounded decoder, exact ID matching, and the exact single-run HMAC endpoint identity schema above. Generate the HMAC key before reading the endpoint, keep it only in private `_ArchiveRpcRunContext` together with the internally created absolute collection deadline, require the same digest for every connection, and erase it after final source/anchor checks; never write an unkeyed URL/origin hash. A test capability may accept canonical request bytes and return raw bytes, but it is module-private, identity-sealed, omitted from public collector signatures, and `repr`-redacted. Production takes no request limit, deadline, method, URL, or client parameter.

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

## Task 3: Capture the Exact Inclusive Seven-Day Inventory

**Files:**

- Create: `scripts/historical_foundry_scan.py`
- Create: `tests/test_historical_foundry_scan.py`
- Modify: `scripts/historical_foundry_rpc.py`
- Modify: `tests/test_historical_foundry_rpc.py`

- [ ] **Step 1: Write lower-bound and full-coverage RED tests**

Freeze pure plan/projection functions:

```python
def locate_inclusive_lower_bound(
    *,
    anchor: Mapping[str, Any],
    header_at_number: Callable[[int], Mapping[str, Any]],
    lookback_seconds: int,
) -> int: ...

def build_historical_window_request_plan(
    *,
    lower_bound: int,
    anchor: Mapping[str, Any],
    authority: Mapping[str, Any],
) -> Mapping[str, Any]: ...

def project_historical_window_capture(
    *,
    plan: Mapping[str, Any],
    header_responses: Sequence[Mapping[str, Any]],
    reserve_price_responses: Sequence[Mapping[str, Any]],
    fee_responses: Sequence[Mapping[str, Any]],
) -> Mapping[str, Any]: ...
```

Test cutoff equality, immediately-before/after timestamps, duplicate timestamps, exact inclusive count, parent continuity, anchor reread, two `getReserves()` plus one proxy `latestRoundData()` per block hash with EIP-1898 `requireCanonical:true`, exact request-ID sets, Chainlink phase/round/freshness at 3600/3601 seconds, and p50/p90/base-fee/gas-used fee-history closure. The retained inclusive freshness projection is exactly `valid_until = updated_at + 3601` for integer block timestamps so age 3600 is valid and age 3601 is invalid; it may not reuse the live exclusive boundary unchanged.

Freeze private batches as 40 headers, 20 blocks × two reserve calls, 40 price calls, and 1024 fee-history blocks with percentiles `[50, 90]`. Deterministic fallback may bisect only the same logical interval after HTTP 413 or an exact provider batch-size rejection; it must preserve request IDs, anchor, range, and final logical digest. The original logical batch owns one cumulative 8-MiB wire counter and one cumulative 8-MiB decoded counter shared by its entire fallback tree, so multiple subresponses cannot each consume the limit. Timeout, transport failure, 429, and 5xx are terminal and never retried.

- [ ] **Step 2: Run RED**

```bash
python3 -m unittest \
  tests.test_historical_foundry_scan.HistoricalWindowCaptureTests -v
```

- [ ] **Step 3: Implement capture and projection**

Retain exact canonical request bytes and bounded decoded success-response bytes plus wire/decoded byte counts and hashes. Discard non-success bodies. Optional `Sync`/`AnswerUpdated` diagnostics must not affect coverage, prefilter, or selection.

- [ ] **Step 4: Run GREEN and maximum-count synthetic coverage**

```bash
python3 -m unittest \
  tests.test_historical_foundry_rpc \
  tests.test_historical_foundry_scan -v
```

Include a generated maximum inclusive 50,401-block synthetic week (`604800 / 12 + 1`) asserting exactly 50,401 headers, 100,802 reserve rows, 50,401 price rows, exact fee coverage, no skipped IDs, and bounded chunk sizes without retaining all raw response bodies twice in memory. Also test a missed-slot week with fewer blocks; 50,400 is never treated as the maximum denominator.

- [ ] **Step 5: Commit the coverage slice**

```bash
git add scripts/historical_foundry_rpc.py scripts/historical_foundry_scan.py \
  tests/test_historical_foundry_rpc.py tests/test_historical_foundry_scan.py
git commit -m "feat(opportunity): capture complete historical window"
```

## Task 4: Write and Reread Immutable Run Evidence

**Files:**

- Create: `scripts/historical_foundry_storage.py`
- Create: `tests/test_historical_foundry_storage.py`
- Modify: `scripts/historical_foundry_scan.py`
- Modify: `tests/test_historical_foundry_scan.py`

- [ ] **Step 1: Write filesystem RED tests**

Freeze a held-descriptor snapshot rather than a path-based `Mapping`:

```python
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

`data_dir` is the one storage-location parameter explicitly allowed by the design. It must be an absolute, pre-existing, operator-owned directory opened no-follow through stable ancestry descriptors; it changes no evidence semantics and is retained only as a redacted root identity, never an absolute artifact path. `HistoricalRunSnapshot.read_member` may open only a path/size/hash already present in the validated final manifest; it is not a general relative-path capability. Logical `run:<64hex>` maps internally to its safe 64-hex directory suffix. `HistoricalRunStagingSnapshot` has no path-opening constructor: the private writer first issues a capture snapshot after fsyncing and irrevocably freezing exactly copied configs, `headers/`, `reserves/`, `prices/`, `fees/`, and `scan/capture_inventory.json`; `scan/prefilter/` does not yet exist. After the validated window builds the grid, the writer adds `scan/prefilter/*.json.gz`, fsyncs and irrevocably freezes that second role set, and issues a distinct scan snapshot covering both frozen levels. Each snapshot reads only its frozen inventory while the writer may add later role sets; neither can mutate or authorize a path. The production writer remains private and accepts only a prevalidated run plan and canonical member bytes, never caller relative paths. Implement it by reusing the proven descriptor primitives under `scripts/route_publication.py` (`_open_verified_directory`, `_open_directory_at`, `_write_new_bytes_at`, `_read_bounded_open_file`, `_read_bounded_bytes_at`, `_rename_directory_noreplace_at`, `_verify_directory_entry_snapshot`, `_verify_open_path_snapshot`, and `_fsync_directory`) rather than copying weaker path logic or using the replace/backup semantics in `scripts/atomic_publication.py`.

Tests cover the exact directory/member grammar, path-safe typed `market_key`, canonical gzip members, physical size/SHA/logical count/range inventory, no-replace writes, descriptor reread, stable ancestry, symlink/hardlink/traversal/unexpected-member rejection, member/+1 and total-run limits, TOCTOU swaps, failure cleanup, and sanitized exceptions. Verify every chunk digest and logical row count by decoding its bytes rather than trusting `run_manifest.json`. A state-machine test proves `run_manifest.json` is the final and only manifest write: it is created once with `O_CREAT|O_EXCL`, no member can be created or changed afterward, its held descriptor is reread unchanged, and the staged directory is renamed once with no replacement.

The exact run tree includes `run_manifest.json`, copied physical `policy.json`/`authority.json`/`toolchain.json`, chunked `headers/`, `reserves/`, `prices/`, `fees/`, and `scan/`, `candidate_manifest.json`, selected `typed/<market_key>/...` plus `typed_manifest.json`, per-scenario `foundry/<block>/<scenario>/overlay.json|receipt.json|trace.json.gz|result.json`, and `selection.json`. Unknown roles or extra members reject the run.

- [ ] **Step 2: Run RED**

```bash
python3 -m unittest tests.test_historical_foundry_storage -v
```

- [ ] **Step 3: Implement immutable staging and one final manifest**

Write all evidence members, including `candidate_manifest.json`, `typed_manifest.json`, and `selection.json`, to one private run-scoped staging directory through a held writer capability. Fsync each file and containing directory, decode/reread every member through held descriptors, then construct `run_manifest.json` from those verified bytes and create it exactly once with `O_CREAT|O_EXCL`. Immediately revoke the writer capability, reread the final manifest and every inventoried member unchanged, fsync the staging root, and perform one `_rename_directory_noreplace_at` to `<validated-data-dir>/raw/historical-foundry-replay/<run_id>`. Reopen the final tree with `open_validated_run(data_dir=validated_data_dir, run_id=derived_run_id, expected_manifest_sha256=final_manifest_sha256)`, reread all identities once more, and close it without mutation. There is no provisional manifest, hidden manifest stage, overwrite, backup/replace path, or write after final-manifest creation. A terminal nonpublication attempt uses the same one-final-manifest protocol with its exact closed status; a failure too early to satisfy the closed schema leaves no final raw run. Never use market IDs as paths.

- [ ] **Step 4: Run GREEN and memory-bound corpus**

```bash
python3 -m unittest \
  tests.test_historical_foundry_storage \
  tests.test_historical_foundry_scan -v
```

- [ ] **Step 5: Commit immutable storage**

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

- [ ] **Step 4: Run GREEN with two-repeat KAT**

```bash
python3 -m unittest \
  tests.test_historical_foundry_anvil \
  tests.test_historical_foundry_storage -v
```

Then run the sealed connected command:

```bash
python3 -m scripts.historical_foundry_anvil --verify-connected-repeat
```

It reads only `DEX_DEPTH_RPC_ETH`, the checked-in KAT fixture, tracked authorities, and the project-local toolchain; it has no block/scenario/runtime/limit/flag override. It must replay the same fixed scenario twice in independent Anvil processes and assert identical selected state, token deltas, gas used, overlay hash, calldata hash, and executor runtime hash. Absence of the RPC variable is a hard failure, never a skip.

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

The I/O controller processes candidate blocks in descending order using only the sealed snapshot and ledger. It records every transition above and continues after `nonpublishable_positive`; it never jumps from prefilter output directly to selection. After a tentative winner, retain each already successful required-scenario quartet at its unique no-replace path and run only the remaining scenario keys until exactly ten unique fresh-process quartets exist; never replay or overwrite an existing key. Then derive both selected-block canonical `dex_pool_state` and `dex_usd_price_context` members and write them under manifest-derived market-key directories. Before the one final manifest is created, reread every staged member and recompute coverage denominator, prefilter grid, replay resolution, receipt economics, typed semantics, overlay-set digest, scenario counts, and selection. Reread the anchor by number and require the original hash. Write `candidate_manifest.json`, `typed_manifest.json`, and `selection.json`, then create `run_manifest.json` once as the final staged member and perform the Task-4 single no-replace directory rename. A closed nonpublication run has an exact empty selected typed inventory and can never advance publication.

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
python3.8 -m unittest \
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
python3.8 -m py_compile \
  scripts/bounded_json.py \
  scripts/historical_foundry_rpc.py \
  scripts/historical_foundry_scan.py \
  scripts/historical_foundry_storage.py \
  scripts/historical_foundry_anvil.py
git diff --check
```

- [ ] **Step 5: Commit the source, run the connected Phase-2 gate, then commit the report**

The production preflight intentionally rejects uncommitted source, so commit the tested scanner/controller before the first anchor request:

```bash
git add scripts/historical_foundry_scan.py \
  scripts/historical_foundry_storage.py \
  tests/test_historical_foundry_scan.py \
  tests/test_historical_foundry_storage.py
git commit -m "feat(opportunity): finalize historical replay selection"
git status --porcelain=v1 --untracked-files=all
python3.8 -m scripts.historical_foundry_scan --collect-connected \
  --data-dir "$MARKET_DATA_DIR"
```

The status command must print nothing. `--collect-connected` requires exactly one absolute `--data-dir` and accepts no block, window, scenario, policy, rate, endpoint, tool path, process flag, limit, deadline, or arbitrary member/output-path argument. The root is validated by the descriptor contract above and is the same root consumed by Phase 3. It must collect the complete finalized seven-day window and perform real fresh-Anvil candidate replay; an absent RPC variable, unavailable exact Python/toolchain, unresolved candidate, resource failure, or no publishable winner returns nonzero with the exact closed run state and never skips. Preserve the immutable raw run even for a schema-valid closed nonpublication result.

Report the maximum synthetic counts (50,401 headers, 100,802 reserve rows, 50,401 price rows) separately from actual connected full-window counts and record the resulting run/manifest/selection identities, candidate-state counts, and fresh-Anvil repeat evidence without endpoint or path. State honestly whether the connected run selected a ten-success block or closed nonpublication; only the former is eligible for Phase 3, and neither is yet a public Opportunity.

```bash
git add docs/superpowers/reports/2026-08-20-historical-foundry-scan-replay-report.md
git commit -m "docs(opportunity): report connected historical replay scan"
git diff --check HEAD^ HEAD
```

## Phase Exit Review

- [ ] Independently recompute the inclusive block count and every per-role row count from member bytes.
- [ ] Require connected `python3.8 -m scripts.historical_foundry_scan --collect-connected --data-dir "$MARKET_DATA_DIR"` evidence from a clean committed HEAD; unit fixtures and the Phase-1 fixed-block KAT do not satisfy the Phase-2 gate.
- [ ] Attack exact response-ID sets, EIP-1898 block hashes, header continuity, Chainlink freshness, fee chunks, and anchor reread.
- [ ] Prove optional logs cannot change prefilter or selection.
- [ ] Attack zero-MEV policy behavior and exact-zero net boundary.
- [ ] Verify every selected scenario has a unique overlay preimage and fresh Anvil process.
- [ ] Verify exact archive/Anvil method allowlists, all frozen byte/deadline limits, process cleanup/kill escalation, project-local executable rehashing, and secret-free artifacts.
- [ ] Verify selected results carry the closed proof inputs for the future nine-row topology and do not create or duplicate cost rows in Phase 2.
- [ ] Record pre/post existence, exact bytes, and SHA-256 for both live and historical route pointers and prove neither changed as a consequence of this phase.
- [ ] Request independent code review before building the publication bridge.
