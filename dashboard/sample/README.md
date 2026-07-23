# Versioned Sample Dashboard

The dashboard's public sample is the deterministic research fixture in:

```text
data/mock/research/factor_panel.csv
data/mock/research/candidate_factor_forward_returns.csv
data/mock/research/coverage_summary.csv
```

The dataset contains five synthetic tokens and is safe to use for UI testing.
Its version and generation metadata are recorded in `manifest.json`.

Run the sample without observed market data:

```bash
./scripts/run_dashboard.sh --data data/mock/research/factor_panel.csv
```

The files are synthetic and released under CC0 1.0. They do not represent
actual token prices, returns, or trading activity.
