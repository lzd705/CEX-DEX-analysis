# Local Opportunity manual refresh acceptance — 2026-09-05

## Delivered behavior

The explicitly enabled local runner exposes **Refresh live data** on Current
Opportunity. It collects the fixed UNI/USDT and CAKE/USDT Binance/Bybit markets,
publishes one auditable batch, and reloads the existing valid filters. The
default dashboard remains read-only. No automatic collection, credentials,
orders, configurable remote URLs or additional market hosts were added.

The control rejects overlapping refreshes, enforces a 30-second cooldown,
preserves original errors/N/A evidence, and reconciles completion after a lost
POST response. Local Host/Origin and request-shape checks protect the action.
Collection runs in a fresh one-shot process because the existing fork-based
collector must not run directly inside a threaded HTTP server.

## Browser and HTTP acceptance

The local service used `/private/tmp/cex-dex-live-multi-token.E4INfm` and port
8765. Existing raw files and old bundles were retained; runtime data is not
committed to Git.

At `2026-09-05T08:35:26.395Z` (16:35:26 Hong Kong time), an actual browser click
had completed its collection and publication. The button showed completion
and was still disabled for the remaining 2 seconds of its cooldown. During
collection, GET status reported `running` and `/health` remained HTTP 200.

- Previous cohort:
  `cohort:1532b69170c0a5389acf3729568a5ed99df1c3577f5635a61dde316dcb351d7f`.
- New browser, API and publication cohort:
  `cohort:2e4b248ed690fdc4e213fc13d0e616210dcfc581587c52294729e945967f2906`.
- New manifest SHA-256:
  `9fd685461f4f8e1b6f9ca12a8fdef9ed703a92733ce85d711be917f50186b089`.
- The URL retained the $1,000, all-token, CEX→CEX and descending-net-edge filters.
- The batch contained 4 markets, 4 directional routes and 20 notional scenarios;
  the active filter returned 4 rows.
- All returned rows were unavailable with `route_deadline_exceeded` and null
  net edge. All 20 stored scenarios were unavailable; strict eligibility count
  remained zero. This verifies the refresh workflow, not fresh usable prices.
- A POST with a foreign Origin returned HTTP 403 without collecting. An extra
  URL query on the local status endpoint returned HTTP 400.

## Current source-access boundary

This acceptance did not reproduce the usable four-book collection documented
earlier in `live-cex-multi-token-acceptance-2026-09-05.md`. Fixed public GET
checks returned Binance data-api depth HTTP 200 in 0.6722 seconds, while
`api.binance.com/api/v3/exchangeInfo` and Bybit's public order book did not
respond within their separate 10-second diagnostic bounds. The full collection
reached its 60-second deadline. No alternate hosts, proxies, account APIs or
fee assumptions were used to manufacture a usable result.

CAKE also still lacks exact reviewed fee rows, independently of these access
timeouts. Its fee/net values must remain unavailable until that evidence is
provided and validated. Reloading or refreshing cannot supply missing fees.

## Verification

- 348 tests across the live runner, local server/control, Opportunity API and
  frontend, route pipeline, depth collector and public fees passed in 22.985 s.
- Includes real child-process/fork execution initiated from a parent request
  thread, timeout cleanup, fixed command arguments, duplicate clicks,
  cooldown, cross-site rejection, lost-response reconciliation, invalid-route
  protection, and unchanged read-only default.
- Independent review reproduced and then verified fixes for the thread/fork
  incompatibility and two frontend state/route-validation issues. Final scoped
  review had no outstanding actionable findings.
- The repository-wide suite was not rerun in this increment; the above is the
  focused regression result, not a claim about the entire repository.
