# Collection cycle and freshness operations

## Profiles

All production collection runs enter through:

```bash
python3 scripts/run_collection_cycle.py --profile PROFILE --publish-local
```

| Profile | Ordered steps | Intended cadence |
| --- | --- | --- |
| `full` | incremental daily OHLCV, TVL, CEX depth, DEX depth | manual catch-up and release validation |
| `daily` | incremental daily OHLCV, TVL | daily at 00:30 UTC |
| `tvl` | TVL only | manual retry/recovery |
| `depth` | CEX depth, then DEX depth | hourly at minute 05 UTC |
| `cex_depth` | CEX depth only | manual retry/recovery |
| `dex_depth` | DEX depth only | manual retry/recovery |

The daily step reads the current CEX and DEX end dates, starts from the older
source with a three-day overlap, and ends at the latest completed UTC day. It
passes every configured Token to `run_fact_pipeline.py --append`, so the upsert
preserves older history. `--full-rebuild` is an explicit exception and must not
be used by timers.

## Lock and manifest

Every profile acquires `data/local/collection/collection.lock`. A second run
does not write facts while another profile owns the lock.

Each completed run writes:

```text
data/local/collection/runs/<run_id>/manifest.json
data/local/collection/runs/<run_id>/<step>.log
data/local/collection/latest.json
```

The manifest records exact argument arrays, timestamps, duration, exit status,
full-log SHA-256, a bounded log tail, current file SHA-256 values, coverage,
source-specific date ranges, and freshness.

Scheduled publishing also applies a post-step freshness gate. A collector that
exits zero while its expected source remains stale is recorded as failed. A
rate-limited or empty response therefore cannot masquerade as a successful
refresh.

The separate fact lifecycles remain explicit:

- daily source CSVs are replaced individually, while the server-visible SQLite
  database is staged and atomically replaced last through
  `import_local_snapshot.py`;
- TVL appends normalized history, then atomically replaces its latest snapshot;
- CEX and DEX depth each append normalized history, then atomically replace
  their own latest snapshots.

A full collection manifest coordinates these publications but does not claim
that the CSVs, histories, latest snapshots, and SQLite database form one
multi-file transaction, or that all source APIs were observed at one instant.

## Freshness contract

| Source | Current threshold | Reason |
| --- | --- | --- |
| CEX daily OHLCV | no more than one completed UTC day behind | permits ordinary provider delay |
| DEX daily OHLCV | no more than one completed UTC day behind | permits ordinary provider delay |
| DEX TVL | age no more than 26 hours | matches the initial daily schedule |
| CEX depth | age no more than 2 hours | allows one missed hourly run |
| DEX depth | age no more than 2 hours | keeps CEX/DEX capacity snapshots comparable |

The API reports `cex_daily`, `dex_daily`, `common_comparable_end`, `dex_tvl`,
`cex_depth`, and `dex_depth` separately. A global maximum date must not hide a
lagging source. Missing facts remain unavailable/null and are never replaced
with zero.

Freshness is a data-quality signal, not process liveness. `/health` remains HTTP
200 when the server and data files are readable, while `data_status` reports
`current`, `partial`, or `stale`.

Incremental DEX collection reuses the published token-pool inventory and its
TVL base/quote lineage. It never guesses the OHLCV side. The keyless
GeckoTerminal endpoint is IP-rate-limited, so pool requests are spaced and 429
responses trigger a conservative backoff; a 148-pool refresh is intentionally
slower than the CEX phase.

## Timer installation

The repository includes user-level systemd timer templates. On the production
host, from the deployed checkout:

```bash
chmod +x scripts/install_collection_timers.sh
./scripts/install_collection_timers.sh
systemctl --user list-timers cex-dex-daily.timer cex-dex-depth.timer
```

Logs are available through:

```bash
journalctl --user -u cex-dex-daily.service
journalctl --user -u cex-dex-depth.service
```

The timers use the same lock. A failed collector leaves previously published
facts in place and records a failed run manifest. Diagnose the retained step
log, fix the source/configuration issue, then rerun the relevant profile.
The daily service is intentionally not fail-fast: TVL is still attempted when
the independent daily OHLCV step fails, while the final service status remains
failed and auditable.
The hourly depth service is also not fail-fast: DEX depth is still attempted
when an independent CEX venue fails, and vice versa. The final cycle remains
failed when either supported collection step fails its freshness gate.

## Operational acceptance

- The correct canonical repository and branch are checked before deployment.
- Daily collection uses incremental upsert unless a reviewed rebuild is
  explicitly requested.
- Every configured Token remains present after publication.
- TVL inventory matches every cataloged Token/pool key.
- CEX depth inventory matches every cataloged Token/exchange/pair key.
- DEX depth inventory matches every TVL Token/chain/pool key, including explicit
  unsupported rows.
- Raw responses and collector manifests remain available for audit.
- Collection and publication failures never zero-fill missing values.
- The public API and rendered page display separate source dates and stale
  states.
