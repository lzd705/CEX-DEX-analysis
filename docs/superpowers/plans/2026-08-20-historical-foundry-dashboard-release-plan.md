# Historical Foundry Dashboard and Release Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Expose the verified historical replay through an isolated API and Current/Historical Replay UI, enforce a dedicated release gate, and complete the first real nonfixture seven-day publication and audit.

**Architecture:** The public historical reader resolves a separate pointer, its SHA-addressed verification report, immutable historical core, complete bundle, raw source members, and one stable publication signature. A historical facts projector reuses filter/sort parsing but never live wall-clock freshness. The server gives historical replay its own route key, pointer-to-physical-signature guard, and cache namespace, probing pointer/report/member identities before lookup and before return. The UI keeps Current and Historical request ownership isolated. The release checker adds one opt-in flag that runs the exact served historical HTML and versioned JavaScript against the complete API denominator under one application/asset/HTML/generation fence, then the operator performs the real run and visual review.

**Tech Stack:** Python 3.8.10+ standard library, dashboard Python server, vanilla JavaScript/CSS, `unittest`, local HTTP/browser verification, pinned Foundry/Anvil and Ethereum archive RPC for the final run.

**Spec:** `docs/superpowers/specs/2026-08-20-historical-foundry-replay-opportunity-design.md` sections “Historical API”, “UI behavior”, “Release checker”, “Operational sequence”, and “Completion evidence”.

## Global Constraints

- Current API/UI behavior and the live 120/121-second boundary remain unchanged.
- Historical economics are valid only at the displayed block. No current wall-clock freshness, availability-now, executable, strict, or attested claim is added.
- Missing historical pointer is HTTP 200 with zero inferred inventory. Any present-but-invalid pointer/report/core/bundle/raw member is HTTP 503 with no cached/partial/stale rows.
- Before a cache lookup and before a cache hit return, reread and validate pointer, report, manifest, and all members. Warm cache cannot survive report deletion, mutation, or inode swap.
- The server permanently associates each observed historical pointer physical SHA with its first validated full publication physical signature for the process lifetime. Reusing identical pointer bytes while any pointer/report/manifest/core/bundle/raw descriptor identity changes is immutable-publication corruption and returns 503; it is never treated as a new cache generation.
- Every public read and release validation groups all ninety costs into ten scenarios and enforces the shared exact nine-row `(leg, component_type, value_status, embedded_in_leg_quote)` matrix; checking only key counts is insufficient.
- Current and Historical requests have separate ownership; a late response from one scope cannot modify the other scope's DOM.
- `--require-historical-foundry-replay` is opt-in and independent. Existing `--require-route-opportunities` semantics and default release behavior do not change.
- When enabled, the historical release flag automatically runs the exact served historical HTML bytes and served JavaScript bytes against the exact unfiltered ten-scenario API payload. The probe is bound to `/health` application SHA, asset SHA/version, historical HTML SHA-256, and historical API `data_generation`, and all four identities are reread after the probe; manual visual inspection cannot substitute for this parity gate.
- Fixture tests prove contracts only. Completion requires one real nonfixture connected run, audit-only verification, API/UI parity, and secret scan.

## Task 1: Add the Strong Published Historical Reader and Signature

**Files:**

- Modify: `scripts/historical_route_publication.py`
- Modify: `tests/test_historical_route_publication.py`
- Create: `tests/historical_replay_fixture.py`

- [ ] **Step 1: Write published-reader RED tests**

Freeze:

```python
HistoricalPublicationSignature = Tuple[Tuple[Any, ...], ...]

def historical_replay_publication_signature(
    historical_root: Path,
    *,
    raw_root: Path,
) -> Optional[HistoricalPublicationSignature]: ...

def load_latest_historical_replay_bundle(
    historical_root: Path,
    *,
    raw_root: Path,
) -> Mapping[str, Any]: ...
```

The loader returns validated normalized objects and identities:

```python
{
    "manifest_sha256": ...,
    "manifest": ...,
    "legs": ...,
    "cost_components": ...,
    "opportunities": ...,
    "replay_evidence": ...,
    "pointer": ...,
    "pointer_core": ...,
    "pointer_sha256": ...,
    "verification_report": ...,
    "verification_report_sha256": ...,
    "publication_signature": ...,
}
```

It resolves immutable historical core from complete-manifest cohort/hash identities, never from mutable `historical/core/latest.json`.

Tests: pointer absent only returns `None`; malformed pointer/report/member raises; signature covers role, stable descriptor metadata, byte size, and physical SHA for pointer, report, manifest, all five members, immutable core, and retained raw members; warm-read delete/mutate/replace/inode-swap fails; core latest advancement does not change the loaded old replay.

After pointer/report/manifest/core/raw validation creates the sealed `ValidatedHistoricalReplayBundleView`, group `cost_components` by all ten `opportunity_id` values. For each group call `_load_historical_cost_proof_inputs_for_published_view()` and require the compact scenario's exact `proof_inputs_hash` to equal the typed hash of the retained `historical_foundry_cost_proof_inputs/v1` object. `_validate_historical_cost_rows_for_published_view()` derives both pool-fee expected amounts/SHAs, four zero-fee proofs, gas, and MEV arguments from that validated capability and calls the module-private context-free matrix validator only after view/proof validation. Dashboard code never imports the low-level topology function.

Add reader RED cases for every status/embedded flip, either public pool-fee amount, proof schema/field/order/row/hash, compact `proof_inputs_hash`, bounded-estimate zero router/tax changed to `not_applicable`, embedded pool fee changed to additive, route gas moved to either leg, route transfer made numeric, and assumed MEV changed to bounded/embedded. All must fail even when the attacker recomputes CSV, SQLite, raw/compact scenario, manifest, pointer, and report hashes. A spy asserts view validation precedes proof validation, which precedes the sole low-level call.

- [ ] **Step 2: Run RED**

```bash
python3 -m unittest \
  tests.test_historical_route_publication.HistoricalPublishedReaderTests -v
```

- [ ] **Step 3: Implement reader and shared fixture**

`tests/historical_replay_fixture.py` builds one canonical 2-route/10-scenario/90-cost bundle and independently calculates expected hashes. It must not import production builders for the values under test. Keep fixture identities visibly synthetic.

- [ ] **Step 4: Run GREEN**

```bash
python3 -m unittest tests.test_historical_route_publication -v
```

- [ ] **Step 5: Commit the public-reader slice**

```bash
git add scripts/historical_route_publication.py \
  tests/test_historical_route_publication.py \
  tests/historical_replay_fixture.py
git commit -m "feat(opportunity): load verified historical replay bundle"
```

## Task 2: Build the Historical Opportunity Facts Projection

**Files:**

- Modify: `dashboard/opportunity_facts.py`
- Create: `tests/test_historical_opportunity_api.py`
- Modify: `tests/test_opportunity_api.py`

- [ ] **Step 1: Write historical facts RED tests**

Freeze:

```python
HISTORICAL_OPPORTUNITY_SUMMARY_CONTRACT = \
    "opportunity_historical_summary/v1"

def load_latest_historical_opportunities(
    routes_root: Optional[Path] = None,
    raw_root: Optional[Path] = None,
) -> Mapping[str, Any]: ...

def historical_opportunity_data_generation(
    loaded: Mapping[str, Any],
) -> str: ...

def build_historical_opportunity_payload(
    loaded: Mapping[str, Any],
    *,
    token: Optional[str] = None,
    venue: Optional[str] = None,
    notional_usd: Optional[str] = None,
    opportunity_class: Optional[str] = None,
    route_type: Optional[str] = None,
    availability: Optional[str] = None,
    sort: Optional[str] = None,
    direction: Optional[str] = None,
) -> Mapping[str, Any]: ...
```

Reuse existing normalization, filter matching, and sorting. Do not reuse live `_timing`, `_cost_component_deadline`, `_next_freshness_deadline_at`, or `_compact_row`.

`historical_opportunity_data_generation()` is the lowercase SHA-256 of canonical JSON containing exactly the historical summary contract, pointer physical SHA, pointer-bound report physical SHA, complete manifest SHA, immutable core manifest/pointer SHA values, and the ordered `(role, physical_sha256, size)` identities for the five bundle members and retained raw members. It contains no path, inode, mtime, current clock, or filter. Every filtered response from one validated publication carries this exact value as `metadata.data_generation`; a different pointer/report/member byte changes it.

Tests require contract, temporal/execution claims, replay/cohort/manifest/policy/run identities, `metadata.data_generation`, selected block/simulation basis, route/scenario/verified/research/positive/returned counts, zero unavailable/strict/executable/attested, and `freshness={"applicable":False,"reason_code":"historical_replay","next_deadline":None}`. Each row carries closed block/direction/Foundry/gas/receipt/trace/executor/baseline/stress fields. Unfiltered projection must contain exactly ten unique scenarios and ninety costs, and must revalidate the exact nine-row status/embedded matrix before projection.

At the dashboard facts boundary, add attacks that replace either embedded pool-fee public amount while preserving topology, replace one compact `proof_inputs_hash`, or replace/reorder one retained proof-input row and then recompute every attacker-controlled bundle/pointer/report hash. `load_latest_historical_opportunities()` must fail before `build_historical_opportunity_payload()` can receive rows. The facts module may inspect the validated proof hash/amount projection returned by the public reader for display/parity, but it must not independently reinterpret raw proof objects or call the low-level matrix validator.

Replay data years old remains available after envelope validation; age is the fixed replay-state age, never `now - block_time`. `class=strict` is valid and returns an empty list/count zero. Negative verified scenarios remain visible.

- [ ] **Step 2: Run RED**

```bash
python3 -m unittest \
  tests.test_historical_opportunity_api.HistoricalOpportunityFactsTests -v
```

- [ ] **Step 3: Implement the isolated projection**

Make roots explicit at the loader boundary. Resolve defaults only through existing trusted dashboard configuration, not arbitrary request/query values. Missing pointer uses a dedicated unavailable payload with reason `historical_replay_pointer_absent`; corrupt artifacts propagate as invalid.

- [ ] **Step 4: Run GREEN and live timing regressions**

```bash
python3 -m unittest \
  tests.test_historical_opportunity_api \
  tests.test_opportunity_api \
  tests.test_freshness -v
```

- [ ] **Step 5: Commit the facts slice**

```bash
git add dashboard/opportunity_facts.py \
  tests/test_historical_opportunity_api.py tests/test_opportunity_api.py
git commit -m "feat(dashboard): project historical replay opportunities"
```

## Task 3: Add the Isolated Historical API and Strong Cache Key

**Files:**

- Modify: `dashboard/server.py`
- Modify: `tests/test_historical_opportunity_api.py`
- Modify: `tests/test_dashboard.py`

- [ ] **Step 1: Write endpoint/cache RED tests**

Add internal route key `opportunities_historical` for:

```text
GET /api/markets/opportunities/historical
```

Tests require all eight existing filters and exact query validation; separate cache namespace; pointer absence => HTTP 200 unavailable; pointer/report/member corruption => 503; Current failure does not affect Historical and vice versa; live 120/121 boundary unchanged; historical never enters live minute bucket/deadline reprojection.

Every available response must expose the path-free generation from `historical_opportunity_data_generation()`. Assert cold, warm, gzip, identity, and every filtered response have the same generation for one publication; mutation followed by a valid republish changes it. The unavailable-pointer payload has `data_generation: null` and cannot be accepted by the DOM parity gate.

The decisive warm-cache test sequence is:

```python
first = self.get_historical()       # 200, ten rows
self.delete_pointer_bound_report()
second = self.get_historical()      # 503, never cached ten rows
```

Repeat for same-size content mutation and inode swap.

Freeze the process-lifetime identity guard:

```python
_HISTORICAL_POINTER_PUBLICATION_IDENTITIES: Dict[
    str, HistoricalPublicationSignature
] = {}

def require_stable_historical_pointer_publication_identity(
    *,
    pointer_sha256: str,
    publication_signature: HistoricalPublicationSignature,
) -> None: ...
```

On first observation of a valid pointer SHA, install its exact full physical signature with set-if-absent semantics. On every later pre-cache and post-build probe for that same pointer SHA, require exact equality; never overwrite the association and never clear it through ordinary serialized-cache/source-generation invalidation. `clear_runtime_caches()` may clear response caches but not this guard; only process initialization and an explicit test-only reset fixture may create an empty guard. A genuinely new pointer SHA may register a new signature.

Add tests for identical pointer bytes plus identical descendant bytes installed under a new inode, report inode swap, manifest inode swap, immutable-core member inode swap, bundle-member inode swap, and retained-raw-member inode swap. Each second request must return 503 even though every physical content SHA still matches and a signature-keyed cache could otherwise rebuild. Also test that a new pointer SHA registers and serves, while returning later to an already observed pointer SHA with changed descendants remains 503.

- [ ] **Step 2: Run RED**

```bash
python3 -m unittest \
  tests.test_historical_opportunity_api.HistoricalOpportunityServerTests -v
```

- [ ] **Step 3: Implement pre/post signature probes**

Before cache lookup, descriptor-reread/validate pointer, report, manifest, five bundle members, immutable core, and retained raw identities, then call `require_stable_historical_pointer_publication_identity()` before consulting a response cache. Key the cache by strong publication signature, `historical_opportunity_data_generation`, contract version, normalized filters, and encoding. Probe again before returning and apply the same pointer-to-signature guard; if signatures differ, the guard rejects the same-pointer case immediately with 503 rather than accepting a retry. If the pointer itself changed, retry the whole assembly at most three times against the new SHA; a recomputed generation mismatch also returns 503. Do not add historical identity to global live `api_source_signature`.

- [ ] **Step 4: Run GREEN**

```bash
python3 -m unittest \
  tests.test_historical_opportunity_api \
  tests.test_opportunity_api \
  tests.test_dashboard -v
```

- [ ] **Step 5: Commit the API slice**

```bash
git add dashboard/server.py \
  tests/test_historical_opportunity_api.py tests/test_dashboard.py
git commit -m "feat(dashboard): serve historical replay opportunities"
```

## Task 4: Add Current/Historical Replay URL State and UI

**Files:**

- Modify: `dashboard/static/index.html`
- Modify: `dashboard/static/navigation.js`
- Modify: `dashboard/static/app.js`
- Modify: `dashboard/static/styles.css`
- Modify: `tests/test_navigation.py`
- Create: `tests/test_historical_opportunity_frontend.py`
- Modify: `tests/test_opportunity_frontend.py`

- [ ] **Step 1: Write navigation/render/race RED tests**

Add page-scoped URL state:

```text
opportunity_scope=current|historical
```

Missing defaults to Current and may be omitted from canonical Current URLs. Reject unknown/duplicate values.

Tests require the segmented control, URL round-trip/back-forward behavior, fixed disclaimer text from the spec, hidden strict section, `Historical Foundry Replays`, `Net result at replay block`, `State age at replay`, block/direction/notional/Foundry/gas/receipt/trace/baseline/stress display, positive and negative rows, and zero Current/Executable positive claims.

The actual historical `<tr>` markup, not a separate audit-only renderer, must expose escaped closed audit attributes used by the release DOM probe:

```text
data-opportunity-id
data-api-generation
data-replay-id
data-block-number
data-direction
data-notional-usd
data-foundry-verified
data-policy-net-edge-usd
data-research-net-edge-usd
data-receipt-sha256
data-trace-sha256
```

The section root also exposes `data-api-generation`, `data-replay-id`, `data-scenario-count`, and `data-selected-block-number`. Add a ten-row fixture test that parses the rendered table bodies and requires a bijection with all ten API rows, including both directions, all five notionals, positive and negative results, and no duplicate/missing opportunity IDs. Do not create a second projection function that could pass while the visible table is wrong.

First add a portability regression that searches the generated Node harness text and rejects any hard-coded `/private/tmp/.../navigation.js`. Replace the seven existing literals in `tests/test_opportunity_frontend.py` with a path derived from `Path(__file__).resolve().parents[1] / "dashboard/static/navigation.js"`; otherwise the frontend suite cannot be a release gate in a new worktree.

Request ownership key includes scope and the existing monotonically increasing request ID. Start a slow Current response, switch to Historical, deliver Historical, then deliver Current; the DOM must remain Historical. Test the reverse order too. Once a historical response wins ownership, write its one validated `metadata.data_generation` to the section and every row atomically; never retain rows from the section's prior generation.

- [ ] **Step 2: Run RED**

```bash
python3 -m unittest \
  tests.test_navigation \
  tests.test_historical_opportunity_frontend -v
```

- [ ] **Step 3: Implement the scoped UI**

Use two explicit context blocks and the existing segmented-control style. Keep all test harness imports repository-relative. Historical mode always renders this exact disclaimer:

> Historical Foundry Replay. Fixed-block counterfactual simulation under a hash-bound state override modelling a prefunded, predeployed, preapproved executor. Successful values are research estimates at the displayed Ethereum block; they are not current and are not executable candidates.

Do not merely hide words; hide strict rows/section from visual and accessibility trees. Frontend response validation must require the correct contract/scope, nonnull lowercase-64-hex `metadata.data_generation`, full-denominator `metadata.coverage.scenario_count == 10`, `metadata.coverage.returned_count == routes.length`, and one generation across section/rows before rendering. Filtered interactive responses may contain fewer rows; the dedicated release probe deliberately requests the unfiltered response and requires all ten.

- [ ] **Step 4: Run GREEN and existing frontend regression**

```bash
python3 -m unittest \
  tests.test_navigation \
  tests.test_historical_opportunity_frontend \
  tests.test_opportunity_frontend \
  tests.test_dashboard_frontend -v
```

- [ ] **Step 5: Commit the UI slice**

```bash
git add dashboard/static/index.html dashboard/static/navigation.js \
  dashboard/static/app.js dashboard/static/styles.css \
  tests/test_navigation.py tests/test_historical_opportunity_frontend.py \
  tests/test_opportunity_frontend.py
git commit -m "feat(dashboard): add historical replay opportunity view"
```

## Task 5: Add the Dedicated Historical Release Gate

**Files:**

- Modify: `scripts/check_dashboard_release.py`
- Create: `scripts/historical_opportunity_dom_probe.js`
- Modify: `tests/test_release_smoke.py`
- Modify: `tests/test_historical_opportunity_api.py`
- Modify: `tests/test_historical_opportunity_frontend.py`
- Create: `tests/test_historical_opportunity_dom_probe.py`

- [ ] **Step 1: Write flag and parity RED tests**

Add only:

```text
--require-historical-foundry-replay
```

Without the flag, the checker must not inspect the historical pointer, request the historical API or historical HTML URL, or start the DOM probe, and existing JSON/exit behavior remains identical. `--require-route-opportunities` does not imply the historical gate.

With the flag, require chain 1, UNI/WETH, Uniswap/Sushi, policy/toolchain support, 100% seven-day coverage, zero gaps/unresolved newer candidates, selected newest-publishable proof, 2 routes, 2 legs, 5 notionals each, 10 scenarios, 90 costs, 10 Foundry-verified, 10 research, zero unavailable/strict/executable/attested, >=1 exact positive baseline/research net, pointer/report/raw/core/bundle parity, and API filter/count/arithmetic parity. Group all ninety costs by opportunity and enforce the exact shared nine-row status/embedded matrix for all ten groups; a count-only check fails the gate.

Shell check fetches the exact nonredirected `/opportunities?opportunity_scope=historical` response, retains its physical bytes, requires scope hooks and the fixed disclaimer in those bytes, and validates every script/style reference against `/health.asset_version` and the served bundle whose bytes hash to `/health.asset_sha`. DOM/API parity is part of this flag, not a separate manual gate.

Freeze these release interfaces:

```python
@dataclass(frozen=True)
class StaticAssetSnapshot:
    asset_sha: str
    asset_version: str
    raw_assets: Mapping[str, bytes]
    metrics: Tuple[ResponseMetrics, ...]

@dataclass(frozen=True)
class HistoricalHtmlSnapshot:
    request_path: str
    raw_html: bytes
    html_sha256: str
    application_sha: str
    asset_sha: str
    asset_version: str
    metrics: ResponseMetrics

def fetch_static_asset_snapshot(
    base_url: str,
    asset_version: str,
    *,
    timeout: float,
) -> StaticAssetSnapshot: ...

def fetch_historical_html_snapshot(
    base_url: str,
    *,
    application_sha: str,
    asset_sha: str,
    asset_version: str,
    timeout: float,
) -> HistoricalHtmlSnapshot: ...

def historical_surface_binding_sha256(
    *,
    application_sha: str,
    asset_sha: str,
    html_sha256: str,
    api_data_generation: str,
) -> str: ...

def run_historical_opportunity_dom_probe(
    *,
    historical_html: bytes,
    navigation_js: bytes,
    app_js: bytes,
    api_payload: Mapping[str, Any],
    expected_application_sha: str,
    expected_asset_sha: str,
    expected_html_sha256: str,
    expected_data_generation: str,
    timeout: float,
) -> Mapping[str, Any]: ...

def validate_historical_dom_api_parity(
    *,
    api_payload: Mapping[str, Any],
    dom_result: Mapping[str, Any],
    expected_application_sha: str,
    expected_asset_sha: str,
    expected_html_sha256: str,
    expected_data_generation: str,
) -> Mapping[str, Any]: ...
```

Keep `fetch_static_asset_bundle()` as a compatibility wrapper over `fetch_static_asset_snapshot()` with its existing return type. The snapshot retains the exact decompressed bytes already included in the health asset digest. `fetch_historical_html_snapshot()` accepts only the exact historical URL above, bounded UTF-8 `text/html`, no redirect, and exact versioned public asset references; it hashes the actual response bytes without reconstructing HTML from the checkout. `historical_surface_binding_sha256()` is the typed canonical hash of exactly the four named identities and is emitted by the release result.

`scripts/historical_opportunity_dom_probe.js` reads one bounded JSON object from stdin containing exact `historical_html_base64`, `navigation_js_base64`, `app_js_base64`, and the already validated unfiltered historical API payload. It strictly decodes and re-hashes each byte field before parsing. It must parse the actual HTML response into the minimal DOM tree used by the application, including real IDs, hidden/ARIA state, data attributes, table bodies, and script/style URLs. It may not create a hard-coded synthetic element inventory or read `dashboard/static/index.html` from disk. It evaluates the exact served JavaScript bytes with Node `vm`, stubs the route fetch to return that payload once, applies the already served historical location, and emits exactly:

```json
{
  "application_sha": "<40-or-64-hex>",
  "asset_sha": "<64hex>",
  "html_sha256": "<64hex>",
  "surface_binding_sha256": "<64hex>",
  "data_generation": "<64hex>",
  "replay_id": "replay:<64hex>",
  "selected_block_number": 0,
  "scenario_count": 10,
  "strict_hidden": true,
  "disclaimer": "<exact fixed disclaimer>",
  "rows": [
    {
      "opportunity_id": "...",
      "direction": "...",
      "notional_usd": "...",
      "foundry_verified": true,
      "policy_net_edge_usd": "...",
      "research_net_edge_usd": "...",
      "receipt_sha256": "...",
      "trace_sha256": "..."
    }
  ]
}
```

The probe reads these values from the actual served section and the `<tr>` attributes created in Task 4. It cannot call a parallel audit renderer. Python supplies expected application/asset/HTML identities only after validating `/health`, recomputing the complete served asset bundle, and hashing the exact HTML response; it independently computes the expected ordered ten-row projection from the API. `validate_historical_dom_api_parity()` requires exact application SHA, asset SHA, HTML SHA, recomputed surface binding, API generation, replay/block identity, fixed disclaimer, hidden strict section, exactly ten unique rows, and a row-for-row bijection over both directions, five notionals, Foundry flag, policy/research net, receipt hash, and trace hash.

The historical flag fails if Node is absent, the probe times out/writes stderr/extra output, served HTML is missing/duplicating a required element or carries a wrong asset URL, the DOM has 9 or 11 rows, any of the ten rows differs, or an application/asset/HTML/generation mismatch occurs. After the probe, reread `/health`, the complete versioned asset bundle, the exact historical HTML URL, and the same unfiltered historical API. Require byte-identical HTML, unchanged application SHA, asset SHA/version, HTML SHA, pointer-bound `data_generation`, surface binding, replay ID, selected block, and ten-row digest before returning success.

- [ ] **Step 2: Run RED**

```bash
python3 -m unittest \
  tests.test_release_smoke.HistoricalFoundryReplayReleaseTests -v
```

- [ ] **Step 3: Implement local/API/shell validation**

Use the historical public reader's sealed validated view; release code must not import the module-private topology validator or reconstruct a weaker manifest/proof/component validator. Require ten proof-input hashes and both embedded pool-fee amount parities from the reader result. Exercise cold and warm API calls plus strict, estimate, unavailable, notional, direction, venue, sort, and invalid-filter cases. Fetch one unfiltered ten-scenario payload for the DOM probe; filtered payloads never substitute for the full parity denominator.

Add exact tests:

- `test_flag_absent_never_fetches_historical_api_or_html_or_runs_dom_probe`
- `test_flag_runs_dom_probe_over_all_ten_api_scenarios`
- `test_dom_probe_uses_exact_served_historical_html_navigation_and_app_bytes`
- `test_dom_probe_rejects_checkout_or_synthetic_html_substitution`
- `test_dom_probe_rejects_missing_duplicate_or_wrong_version_html_assets`
- `test_dom_probe_rejects_application_or_html_sha_mismatch`
- `test_dom_probe_rejects_asset_sha_mismatch`
- `test_dom_probe_rejects_api_generation_mismatch`
- `test_dom_probe_rejects_each_missing_extra_duplicate_or_mutated_scenario`
- `test_dom_probe_rejects_selected_block_replay_or_disclaimer_mismatch`
- `test_dom_probe_rejects_visible_strict_section`
- `test_post_probe_html_asset_application_or_api_generation_change_fails_closed`
- `test_release_rejects_each_proof_hash_pool_fee_amount_status_or_embedded_mutation`

- [ ] **Step 4: Run GREEN**

```bash
python3 -m unittest \
  tests.test_release_smoke \
  tests.test_historical_opportunity_api \
  tests.test_historical_opportunity_frontend \
  tests.test_historical_opportunity_dom_probe -v
```

- [ ] **Step 5: Commit the release gate**

```bash
git add scripts/check_dashboard_release.py \
  scripts/historical_opportunity_dom_probe.js tests/test_release_smoke.py \
  tests/test_historical_opportunity_api.py \
  tests/test_historical_opportunity_frontend.py \
  tests/test_historical_opportunity_dom_probe.py
git commit -m "feat(release): require historical Foundry replay"
```

## Task 6: Run Full Regressions and One Real Nonfixture Publication

**Files:**

- Create: `docs/superpowers/reports/2026-08-20-historical-foundry-real-run-report.md`
- Modify only if a real RED proves a production defect; return to the appropriate earlier TDD task before changing code.

- [ ] **Step 1: Verify environment prerequisites honestly**

```bash
python3 --version
python3.8 --version
python3 -m scripts.bootstrap_historical_foundry_toolchain \
  --print-verified-identity
node --version
```

Require exact CPython 3.8.10, Node for the mandatory DOM probe, and the checked-in Foundry/toolchain identities reported by the sealed project-local bootstrap capability. No final command invokes or resolves ambient `forge`, `anvil`, `cast`, or `solc` through `PATH`. The current planning environment lacks exact Python 3.8.10 and the bootstrapped project-local toolchain; resolve those prerequisites before claiming this step passes.

- [ ] **Step 2: Run all focused and full tests**

```bash
python3 -m unittest \
  tests.test_historical_foundry_contracts \
  tests.test_historical_foundry_toolchain \
  tests.test_historical_foundry_rpc \
  tests.test_historical_foundry_scan \
  tests.test_historical_foundry_storage \
  tests.test_historical_foundry_anvil \
  tests.test_historical_foundry_replay \
  tests.test_historical_route_publication \
  tests.test_historical_foundry_verifier \
  tests.test_run_historical_foundry_replay \
  tests.test_historical_opportunity_api \
  tests.test_historical_opportunity_frontend \
  tests.test_historical_opportunity_dom_probe \
  tests.test_release_smoke -v
python3 -m unittest discover -s tests -v
python3.8 -m unittest discover -s tests -v
```

- [ ] **Step 3: Run Foundry gates**

```bash
python3 -m scripts.bootstrap_historical_foundry_toolchain \
  --verify-offline-tests
python3 -m scripts.bootstrap_historical_foundry_toolchain \
  --verify-connected-kat
```

Both modes open the reviewed project-local toolchain capability and use internally fixed command arrays. Direct `forge test`, `anvil`, `cast`, `solc`, caller executable paths, and caller argument suffixes are forbidden completion evidence.

- [ ] **Step 4: Run the real seven-day publish and audit**

Use an isolated absolute `MARKET_DATA_DIR` and process-only `DEX_DEPTH_RPC_ETH`:

```bash
python3 -m scripts.run_historical_foundry_replay scan \
  --data-dir "$MARKET_DATA_DIR" --publish
python3 -m scripts.run_historical_foundry_replay verify \
  --data-dir "$MARKET_DATA_DIR" \
  --bundle "$MARKET_DATA_DIR/routes/historical/bundles/$REPLAY_ID"
```

Retain the canonical JSON output from both commands. The publish output must contain four exact live-pointer snapshot records: before/after `routes/core/latest.json` and before/after `routes/latest.json`, each with presence, byte size, SHA-256, and exact `bytes_base64`. Decode and re-hash each present snapshot, then require before/after byte equality separately for both pointers; a boolean summary without the bytes and hashes does not satisfy this step.

If the complete window resolves `no_publishable_profitable_block`, that is an honest nonpublication result but does not satisfy the MVP completion gate. Do not select an older/partial/fixture winner or weaken policy.

- [ ] **Step 5: Run API, release, and browser parity**

```bash
python3 dashboard/server.py \
  --data-dir "$MARKET_DATA_DIR" --host 127.0.0.1 --port 8765
```

In another terminal:

```bash
curl -fsS \
  "http://127.0.0.1:8765/api/markets/opportunities/historical?token=UNI&route_type=dex_dex&class=estimate"
python3 scripts/check_dashboard_release.py \
  --base-url http://127.0.0.1:8765 \
  --require-historical-foundry-replay
```

Open:

```text
http://127.0.0.1:8765/opportunities?opportunity_scope=historical&token=UNI&route_type=dex_dex&class=estimate
```

The release command must automatically report `application_sha`, `asset_sha`, `historical_html_sha256`, `historical_surface_binding_sha256`, historical `api_data_generation`, `dom_data_generation`, replay ID, selected block, `api_scenario_count=10`, `dom_scenario_count=10`, and the equal ten-row parity digest. It must also report that post-probe health, assets, HTML bytes, and API generation matched their pre-probe snapshots. Its success is the machine gate. Also open the page for visual layout/accessibility inspection and verify the same selected block/winner/counts, including one positive and all negative rows; manual inspection is additional evidence, not a replacement for the automatic probe.

- [ ] **Step 6: Scan artifacts for secrets and mutable paths**

Search only the new run/core/bundle/report and tracked diff. Assert no usable RPC URL, auth/cookie/provider key, private key, local absolute path, error body, or mutable tool path is present. Record hashes/counts, not secret-bearing matches.

- [ ] **Step 7: Write the real-run report and commit**

The report includes pinned tool identities, repository HEAD, anchor/lower-bound/count/coverage, selected block, candidate counts, ten receipt hashes/statuses, positive scenario/policy net, 2/10/90 counts, report/pointer hashes, audit result, API/UI/release parity, system/3.8/full test counts, sealed bootstrap offline/connected gates, and clean secret/diff review. Copy the four live-pointer before/after snapshot records from the publish output verbatim, including exact `bytes_base64`, size, and SHA-256, and show decoded-byte equality separately for each live pointer. Record the release probe's application SHA, asset SHA, actual served historical HTML SHA, surface binding, API generation, DOM generation, ten API opportunity IDs, ten DOM opportunity IDs, equal parity digest, and post-reread equality. Explicitly call the output historical `research_estimate`, not production executable data.

```bash
git add docs/superpowers/reports/2026-08-20-historical-foundry-real-run-report.md
git commit -m "docs(opportunity): record verified historical replay run"
```

## Phase Exit Review

- [ ] Delete/mutate/swap a report after warm cache; historical API must return 503.
- [ ] Break Current and Historical independently and prove failure isolation.
- [ ] Race scope responses and prove DOM ownership isolation.
- [ ] Verify strict filters return zero and no executable/attested language is asserted.
- [ ] Compare local bundle, all ten API scenarios, all ten automatically probed DOM rows built from the actual served historical HTML, connected report, and release counts/hashes under one unchanged application SHA, asset SHA, HTML SHA, surface binding, and API generation.
- [ ] Verify the real run is nonfixture and complete for the exact seven-day denominator.
- [ ] Verify real CPython 3.8.10 and pinned Foundry online/offline evidence.
- [ ] Verify captured before/after bytes, sizes, and SHA-256 prove neither live pointer changed, and verify no secret/path leaked.
- [ ] Request independent final code and evidence review before merge or deployment.
