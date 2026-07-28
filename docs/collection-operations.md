# Collection cycle and freshness operations

## Profiles

All production collection runs enter through:

```bash
python3 scripts/run_collection_cycle.py --profile PROFILE --publish-local
```

| Profile | Ordered steps | Intended cadence |
| --- | --- | --- |
| `full` | incremental daily OHLCV, CEX depth/cost, published TVL, then DEX depth/cost | manual catch-up and release validation |
| `daily` | incremental daily OHLCV, TVL | daily at 00:30 UTC |
| `tvl` | TVL only | manual retry/recovery |
| `depth` | CEX depth/cost, temporary DEX USD-price refresh, then DEX depth/cost | hourly at minute 05 UTC |
| `cex_depth` | CEX depth and fixed-notional cost from one book snapshot | manual retry/recovery |
| `dex_depth` | temporary DEX USD-price refresh, then DEX depth/cost from one fixed block | manual retry/recovery |

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
  their own latest snapshots. Their matching long-form execution-cost
  latest views reuse the same raw response/fixed-block lineage. Retained raw
  responses and manifests are the execution audit history for this release.

The hourly DEX USD-price refresh reuses the TVL collector's GeckoTerminal
multi-pool response but writes only
`data/processed/dex_pool_tvl_snapshot.csv`. It does not publish a new TVL fact
or append the TVL history. The following DEX collector explicitly reads that
file. If the refresh step fails, DEX depth is recorded as
`skipped_dependency` and the prior published DEX snapshot remains untouched.

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

DEX USD conversion has a stricter dependency-level contract. The fixed-block
pool-state time is compared with the time this project received the
GeckoTerminal price response. A difference of at most 15 minutes is current;
more than 15 minutes and at most 2 hours is an explicit warning; more than
2 hours, a missing timestamp, or an invalid timestamp is unusable. Unusable
inputs cannot publish measured USD depth or execution cost.

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
The hourly depth service is also not fail-fast across independent CEX and DEX
sources: the DEX price refresh is still attempted when a CEX venue fails.
Within the DEX chain, however, the price refresh is a hard dependency. DEX
depth is skipped when that refresh fails. The final cycle remains failed when
either supported collection step fails its freshness or dependency gate.

## Raw CEX depth retention

Hourly order-book responses grow much faster than normalized facts. Keep the
recent JSON snapshots directly inspectable, then compress and eventually
expire them with the dedicated retention command. It is a dry run by default:

```bash
python3 scripts/retain_cex_depth_raw.py
python3 scripts/retain_cex_depth_raw.py --apply
```

The default 7-day raw and 30-day archive periods are operational defaults, not
a data-contract claim. Review the printed actions and the research/audit
retention requirement before applying or enabling
`cex-dex-cex-depth-retention.timer`. See
`docs/production-hardening.md` for the systemd template and safety boundary.

## Operational acceptance

- The correct canonical repository and branch are checked before deployment.
- Daily collection uses incremental upsert unless a reviewed rebuild is
  explicitly requested.
- Every configured Token remains present after publication.
- TVL inventory matches every cataloged Token/pool key.
- CEX depth inventory matches every cataloged Token/exchange/pair key.
- DEX depth inventory matches every TVL Token/chain/pool key, including explicit
  unsupported rows.
- Each CEX and DEX execution-cost inventory contains exactly five notionals by
  two directions for every corresponding source market, with no duplicate
  scenario keys.
- Execution-cost publication passes formula, fill-state, monotonicity, fee
  scope, missing-value, and source-lineage validation. Partial scenarios never
  publish a full-request VWAP or quoted cost.
- A timestamp-fresh TVL/depth snapshot with zero observed or partial rows is a
  failed scheduled step. CEX execution must contain a measured row; DEX
  execution may be wholly `unsupported`, but any wholly failed supported
  adapter set is a failed step.
- Raw responses and collector manifests remain available for audit.
- Collection and publication failures never zero-fill missing values.
- The public API and rendered page display separate source dates and stale
  states.
