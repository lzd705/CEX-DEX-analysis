# Local Opportunity manual refresh implementation plan

**Goal:** Let a local website user fetch a new real UNI/CAKE Binance/Bybit snapshot without returning to the terminal.

**Architecture:** An explicitly enabled local handler wraps the existing read-only dashboard. A synchronous POST runs the existing fixed collector in a fresh one-shot process, because the collector's fork isolation requires a single-threaded caller. A lock prevents overlapping collections; a 30-second cooldown follows every attempt. No scheduler, credentials, arbitrary markets or remote URLs are accepted.

**Tech stack:** Existing Python standard-library HTTP server and plain JavaScript.

## Contract

- Default dashboard and `--serve` remain read-only. `--enable-live-refresh` requires `--serve`.
- Only the established fixed public GET collection on data-api.binance.vision, api.binance.com, api.bybit.com is used.
- Local refresh is `/api/local/opportunity-refresh`; POST requires exact loopback Host, same Origin, `X-Opportunity-Refresh: 1`, no body or query. GET is status-only.
- Valid POST returns 200 succeeded, 502 failed, 409 running, or 429 cooldown, with state and retry_after_seconds. Invalid requests return a fixed 400/403 error without internal state or exception contents.
- Inject one same-origin external script into the local SPA shell; preserve CSP and generic write restrictions.
- Frontend disables repeated clicks, keeps prior results on failure, reloads valid current filters on newly confirmed success (including GET reconciliation after a lost response), and never automatically POSTs.
- CAKE remains unavailable without supported public fee evidence. Expired results remain unavailable.

## Task 1: Local refresh controller and HTTP boundary

Files: `scripts/local_opportunity_refresh.py`, `scripts/run_current_opportunity_dashboard.py`, `tests/test_local_opportunity_refresh.py`.

- [ ] Write and run failing tests for success, failure redaction, single flight, cooldown, Host/Origin/header/body restrictions, script injection and default read-only behavior.
- [ ] Implement `LocalOpportunityRefresh(callback).refresh()` and `.status()` plus handler wrapper.
- [ ] Add optional `refresh_callback` to the isolated server, default None; retain original handler when disabled.
- [ ] Run backend and existing isolated-server tests.

## Task 2: Local frontend

Files: `scripts/local_opportunity_refresh.js`, `tests/test_local_opportunity_refresh_frontend.py`.

- [ ] Write failing Node behavioral tests using the repository's Python test harness pattern.
- [ ] Add button and aria-live status inside current-context container; GET startup state, one POST per click, cooldown timer, safe text updates.
- [ ] On successful collection call `loadOpportunities()` only while the current Opportunity route remains selected.
- [ ] Run frontend tests for success, repeated clicks, failure, cooldown and navigation during collection.

## Task 3: Runner integration and acceptance

Files: `scripts/run_live_cex_opportunity.py`, `tests/test_run_live_cex_opportunity.py`, `docs/collection-operations.md`.

- [ ] Test explicit opt-in and rejection without --serve before implementation.
- [ ] Bind callback only to prevalidated local data directory, schedule and bounded deadline; reuse existing collector.
- [ ] Run relevant Opportunity suites and independent diff review.
- [ ] Start local enabled server, trigger one browser refresh, verify new cohort, current health, honest results and blocked duplicate request.
- [ ] Record acceptance, commit and push the verified changes to the existing authorized branch.

## Decisions

The existing branch/worktree is retained. The user already authorized continued implementation and pushes; no extra confirmation gate is needed. Synchronous collection avoids introducing a job store or background scheduler. Source fee pages are outside the current host allowance, so no new CAKE fee value is inferred. Independent review identified and tests reproduced the thread/fork incompatibility, missing GET success reconciliation, and bypassed invalid-route guard; fixes preserve the existing collector isolation and route validation contracts.
