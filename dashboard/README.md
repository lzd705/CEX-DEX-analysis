# CEX / DEX Market Monitor

This frontend presents market facts only. It does not expose candidate factors,
future returns, research notes, or administrator controls.

## Required local data

Place these two detailed daily files in `data/local/`:

- `cex_exchange_volume_daily.csv`
- `dex_pool_volume_daily.csv`

The directory is ignored by Git. Import a reviewed A-side snapshot with:

```bash
python3 scripts/import_local_snapshot.py /path/to/data/processed
```

Run the monitor:

```bash
npm --prefix dashboard install
./scripts/run_dashboard.sh
```

Open `http://127.0.0.1:8765`.

To keep the code and data in separate locations, set `MARKET_DATA_DIR`:

```bash
MARKET_DATA_DIR=/srv/cex-dex/current ./scripts/run_dashboard.sh
```

The Docker image uses the same contract and expects a read-only data volume at
`/app/data/local`.

## Display contract

- The global time window is the first control in the main content.
- Tokens default to descending selected-market USD volume.
- `综合`, `CEX`, and `DEX` change the sorting scope.
- Each token can select one CEX pair and one DEX pool.
- Spread is calculated from those two selected prices and appears only on the
  token summary row.
- Missing values stay `N/A`; CEX-inapplicable TVL and row-level spread use `--`.
