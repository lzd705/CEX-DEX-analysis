# Dashboard Work Status

Updated: 2026-07-11

## Completed

- Local dashboard server backed by the real B-side `factor_panel.csv`.
- Token, date, metric, and normalization controls.
- CEX / DEX KPI strip and daily volume comparison.
- Token-level metric comparison and coverage table.
- CEX 2 / 3 / 10 exchange scope sensitivity snapshot from A-side commit `ae968f7`.
- Candidate-factor bucket comparison for 1d / 3d / 7d / 14d future returns.
- Local JSON persistence for filters, notes, and research checklist.
- Filtered CSV export.
- Reproducible refresh and startup scripts.
- Versioned five-token synthetic sample retained for tests and demos.
- MIT source license and explicit third-party data usage boundary.
- Desktop and mobile release screenshot.
- Append-only local workspace history with data fingerprints.
- Private workspace and public read-only runtime modes.
- Curated 10-token, 30-factor public research snapshot with a manifest.
- Non-root Docker image that excludes private state and review directories.
- GitHub CI and Render Blueprint for checked automatic deployment.

## Data Status

- Current B-side panel: 10 tokens and 1,780 token-day rows.
- Current sample window: 2026-01-09 through 2026-07-05.
- CEX and DEX fields are complete in the merged sample.
- DEX coverage is selected-pool coverage, so the dashboard labels the ratio `Observed DEX Share`.
- DEX buy, sell, and net-buy fields remain unavailable without swap-level data.

## Next

- Cross-check token-level DEX totals against DefiLlama or Dune.
- Add event-study and swap-level views when those datasets become available.
- Authorize the GitHub repository in Render and verify the permanent HTTPS URL.
