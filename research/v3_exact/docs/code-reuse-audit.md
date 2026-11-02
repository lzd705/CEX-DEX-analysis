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
| `build_event_table.py` | Not reused | Its research table does not meet the official-source evidence, timing-precision, append-only revision, and publication-bundle contract. The current Event Fact layer was implemented separately and does not import that table. |
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
  DEX V2 pools. Every CEX execution row explicitly records
  `excluded_unknown_account_tier`; a numeric account fee is null, not zero.
  Supported DEX V2 pool swap fees are included in pool mechanics. That fee is
  not labeled as protocol treasury or revenue. Gas, router fees, latency,
  token taxes, and MEV remain outside the quoted-cost scope. DEX V3 execution
  is explicitly unsupported.
- Event Facts are present as a separate manually reviewed, official-source,
  append-only bundle and website page. The committed set has 44 latest facts
  and at least one fact for all 30 configured Tokens, but runtime count and
  coverage come from the latest validated bundle. These records are event
  timing/status facts only; 30/30 presence does not imply complete event
  history, and the old research event table and event study were not reused.
- Funding rates, numeric account-specific CEX fees, gas facts, DEX V3
  fixed-notional execution, and event-study returns or impact are not present
  in the current Market Monitor contract.
