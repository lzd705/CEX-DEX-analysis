# Code Reuse Audit

Source reviewed: original repository branch `feature/data-pipeline`, commit
`3f1ffa0`.

## Reused from the original analysis repository

| Module | Decision | Reason |
| --- | --- | --- |
| `fetch_cex.py` | Reused and adapted | Contains tested adapters for up to 12 CEXs and preserves exchange/pair OHLCV rows. Paths now resolve from the project root and CLI token/exchange subsets are supported. |
| `fetch_dex.py` | Reused and adapted | Contains tested GeckoTerminal pool discovery and USD OHLCV retrieval. Paths now resolve from the project root. Current TVL is written only on the latest fetched row instead of being repeated as historical TVL. |
| `config/tokens.csv` | Reused | Defines 30 explicit Token and CEX identities. |
| `config/token_chains.csv` | Reused | Defines chain and contract identities needed to locate DEX pools. |
| Collector unit tests | Reused | Preserve exchange conversion, pool selection, deduplication, and coverage behavior. |

## Deliberately excluded

| Module | Decision | Reason |
| --- | --- | --- |
| `fetch_price.py` | Excluded | It is an unimplemented placeholder. |
| `build_panel.py` | Excluded | It aggregates away venue identity and converts some missing values to zero. |
| `build_research_panel.py` | Excluded | It belongs to the research/factor workflow. |
| `build_factors.py` | Excluded | Factors are outside Market Monitor 1.0. |
| `build_factor_return_panel.py` | Excluded | Future-return evaluation is outside the fact-only boundary. |
| `build_event_table.py` | Deferred | Source-backed events are valid, but events are not part of the first Market Monitor. |
| old `run_pipeline.py` | Excluded | It automatically runs research, factors, forward returns, and events. |

## Known data limitations

- CEX quote volume is normalized to USD by exchange-specific logic; conversion
  methods remain source-dependent.
- DEX OHLCV is requested in USD and for the configured Token side.
- Pool selection uses currently leading pools, which creates survivorship and
  selection bias for historical comparisons.
- DEX TVL is a latest-fetch snapshot. The current collector does not provide
  historical daily TVL.
- CEX order-book depth is a separate latest-fetch snapshot with explicit
  truncation flags; it is not reconstructed tick history.
- Fixed-notional quoted execution cost is present for CEX books and supported
  DEX V2 pools. Supported DEX pool fees are included in pool mechanics; CEX
  account fees, gas, router fees, latency, token taxes, and MEV remain outside
  the quoted-cost scope. DEX V3 execution is explicitly unsupported.
- Funding-rate and event facts are not present in the current Market Monitor
  contract.
