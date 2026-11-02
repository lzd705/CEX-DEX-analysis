# Static Delivery and Cache Performance Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce first-load transfer by roughly 77% with deterministic gzip and exact immutable caching while preserving HTML revalidation, API `no-store`, release SHA evidence, and protected admin isolation.

**Architecture:** The dashboard process freezes uncompressed and gzip public representations at startup beside the existing release fingerprint. `MarketMonitorHandler.send_head()` selects an identity/gzip representation and one cache policy from the parsed request; it does not stack headers on `end_headers()`. The release checker downloads the exact versioned bundle, verifies decompressed bytes, and enforces compression and a 220 KiB wire budget.

**Tech Stack:** Python 3.8-compatible standard library, deterministic `gzip`, `http.server`, existing static asset contract, `unittest` HTTP integration fixtures.

## Global Constraints

- Only files in `PUBLIC_STATIC_ASSET_SOURCES` can receive immutable caching.
- Exact version means one query pair and nothing except `v=<static_asset_version()>`.
- `gzip;q=0` and an explicit wildcard exclusion receive identity bytes.
- HTML is optionally gzip but always `no-cache`; APIs remain `no-store`.
- Each response has exactly one `Cache-Control` field.
- HEAD and GET representation headers are identical; HEAD has no body.
- The release asset SHA remains a hash of the uncompressed public bundle.
- External CDN/Nginx and HTTP/1.1 keep-alive changes are outside this plan.

---

### Task 1: Representation and request-policy primitives

**Files:**
- Modify: `dashboard/server.py`
- Create: `tests/test_static_delivery.py`

**Interfaces:**
- Produces: `StaticRepresentation(raw: bytes, gzip: bytes, content_type: str, last_modified: str)`.
- Produces: `client_accepts_gzip(value: str) -> bool`.
- Produces: `exact_static_version(path: str) -> bool`.
- Produces: `static_representation(path: str) -> StaticRepresentation | None`.

- [ ] **Step 1: Write failing Accept-Encoding table tests**

Assert literals: empty/`identity` false; `gzip` true; `br, gzip;q=0.5` true;
`gzip;q=0` false; `*;q=1` true only when gzip is not explicitly excluded;
`gzip;q=0, *;q=1` false; malformed quality values fail to identity.

- [ ] **Step 2: Write failing version-query table tests**

Exact current `?v=<version>` is true. Missing, blank, wrong, duplicated,
percent-decoded mismatch, `v=current&x=1`, `x=1&v=current`, fragments, and
protected admin paths are false.

- [ ] **Step 3: Verify RED**

Run: `python3 -m unittest tests.test_static_delivery -v`

Expected: FAIL because the policy primitives are absent.

- [ ] **Step 4: Implement deterministic startup representations**

Read only public asset sources, render versioned HTML separately, and use
`gzip.compress(raw, compresslevel=5, mtime=0)` or a Python-3.8-equivalent
`GzipFile(mtime=0)`. Store immutable bytes frozen at import/startup. Do not add
admin files or recompute the process release SHA from compressed bytes.

- [ ] **Step 5: Verify GREEN**

Run: `python3 -m unittest tests.test_static_delivery tests.test_dashboard -v`

Expected: PASS; decompression is byte-identical for every public asset.

- [ ] **Step 6: Commit**

```bash
git add dashboard/server.py tests/test_static_delivery.py
git commit -m "feat(web): precompress versioned public assets"
```

Add a GitHub commit comment with raw/gzip byte totals and the content-coding table.

### Task 2: HTTP GET/HEAD and single cache-policy boundary

**Files:**
- Modify: `dashboard/server.py`
- Modify: `tests/test_static_delivery.py`

**Interfaces:**
- Consumes Task 1 primitives.
- Produces one `send_static_representation(head_only: bool) -> BinaryIO | None` handler path.

- [ ] **Step 1: Write real HTTP integration tests before handler changes**

Start `ThreadingHTTPServer` on loopback with a temporary public bundle. For
exact-version gzip GET, assert status 200, one immutable `Cache-Control`,
`Vary: Accept-Encoding`, gzip `Content-Length`, and byte-equal decompression.
Repeat for identity GET, gzip HEAD, identity HEAD, HTML, no/wrong/duplicate
version, missing asset, SPA shell, and admin paths.

- [ ] **Step 2: Verify RED**

Run: `python3 -m unittest tests.test_static_delivery.StaticHttpTests -v`

Expected: FAIL because static responses are uncompressed and always no-cache.

- [ ] **Step 3: Implement one explicit cache header source**

Replace unconditional static `Cache-Control` injection in `end_headers()` with
a handler field set before `end_headers()`. The static sender writes exactly
one policy. Security headers remain centralized. SimpleHTTPRequestHandler
fallbacks and errors receive `no-cache`; API helpers continue to set exactly
one `no-store`.

- [ ] **Step 4: Preserve conditional and method semantics**

GET and HEAD must select the same representation and report identical
Content-Type, Content-Length, Content-Encoding, Vary, Last-Modified, and
Cache-Control. HEAD returns no body. A client without gzip gets raw bytes.

- [ ] **Step 5: Verify GREEN and mutation cases**

Run: `python3 -m unittest tests.test_static_delivery tests.test_dashboard tests.test_admin tests.test_public_actions -v`

Expected: PASS. Removing the exact-version check, q=0 branch, or protected-file
filter must make at least one test fail.

- [ ] **Step 6: Commit**

```bash
git add dashboard/server.py tests/test_static_delivery.py
git commit -m "feat(web): bind immutable caching to exact assets"
```

Add a GitHub commit comment with GET/HEAD, one-header, q=0, and admin-isolation evidence.

### Task 3: Release compression and bundle-budget gates

**Files:**
- Modify: `scripts/check_dashboard_release.py`
- Modify: `tests/test_release_smoke.py`

**Interfaces:**
- Modifies: `fetch_static_asset_bundle(base_url, asset_version, *, timeout, gzip_budget=220_000)`.
- Produces response metrics with raw bytes, wire bytes, compressed status, cache policy, and content length.

- [ ] **Step 1: Write failing checker counterexamples**

For every public asset over 1 KiB, reject identity delivery after requesting
gzip, corrupt gzip, wrong `Content-Length`, missing/duplicate/conflicting cache
policy, non-immutable exact version, missing `Vary`, and total wire bytes above
220,000. Continue to accept a sub-1-KiB identity asset if all other contracts
hold.

- [ ] **Step 2: Verify RED**

Run: `python3 -m unittest tests.test_release_smoke.ReleaseAssetTests -v`

Expected: FAIL because the checker does not enforce these response contracts.

- [ ] **Step 3: Implement wire and decompressed validation**

Hash decompressed bytes in the exact `PUBLIC_STATIC_ASSET_FILENAMES` order.
Compare header length to wire bytes, require exact immutable policy and Vary,
sum wire bytes, and emit both per-asset and bundle metrics. Preserve the
existing uncompressed asset SHA contract.

- [ ] **Step 4: Verify GREEN**

Run: `python3 -m unittest tests.test_release_smoke -v`

Expected: PASS, including protected-admin exclusion and stable asset SHA.

- [ ] **Step 5: Commit**

```bash
git add scripts/check_dashboard_release.py tests/test_release_smoke.py
git commit -m "test(release): enforce static transfer budget"
```

Add a GitHub commit comment with the measured bundle wire total and 220 KiB gate.

### Task 4: Summary warmup observability without cache redesign

**Files:**
- Modify: `dashboard/server.py`
- Modify: `tests/test_dashboard.py`
- Modify: `docs/collection-operations.md`

**Interfaces:**
- Produces health metadata `summary_warmup` with `status`, `generation`, `started_at`, `finished_at`, and `elapsed_ms`.
- Extends the existing encoded-response cache key with the identity encoding so
  the default non-gzip Summary is serialized once per source generation and
  freshness bucket.
- Preserves existing source-core, lifecycle-state, freshness overlay, single-flight, gzip-response, and browser cache boundaries.

- [ ] **Step 1: Write failing warmup-state tests**

Inject success, source-generation change, and exception. Assert health reports
`warming`, `ready`, or `failed` with bounded public fields and no exception,
filesystem path, SQL, or provider detail. Assert a later successful warmup can
replace failed state only for the current generation.

Request the default identity Summary twice and assert one payload build and
identical bytes. Change the source generation and freshness bucket separately;
each must produce a new response. The gzip and identity cache entries must not
alias each other.

- [ ] **Step 2: Verify RED**

Run: `python3 -m unittest tests.test_dashboard -v`

Expected: FAIL because warmup observability is absent.

- [ ] **Step 3: Implement a locked process-local state record**

Measure the already-existing default Summary warmup. Store canonical UTC
timestamps and elapsed milliseconds under one lock. Do not add a Summary
artifact, pointer, minute bucket to the expensive core key, or full-Catalog
preload. Route the default identity response through the existing bounded
encoded-response LRU with an explicit encoding component in its key.

- [ ] **Step 4: Verify cache invariants and GREEN**

Run: `python3 -m unittest tests.test_dashboard tests.test_release_smoke -v`

Expected: PASS, including single-flight, source-generation invalidation, and
freshness rollover tests.

- [ ] **Step 5: Commit**

```bash
git add dashboard/server.py tests/test_dashboard.py docs/collection-operations.md
git commit -m "perf(summary): expose startup warmup readiness"
```

Add a GitHub commit comment with cold/warm evidence and confirmation that no new Summary pointer was introduced.
