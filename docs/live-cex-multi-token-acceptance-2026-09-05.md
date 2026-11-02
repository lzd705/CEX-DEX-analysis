# UNI and CAKE public market acceptance — 2026-09-05

The authorized public collection succeeded for UNI/USDT and CAKE/USDT on
Binance and Bybit. The normal Current Opportunity page served the resulting
four markets, four directional routes, and twenty notional scenarios.

## Source evidence

- Collector application commit: `51f1a41902fd9e3bc6610e8da41fdd90663d6a11`.
- Observed timestamps: `2026-09-05T02:15:29.595475Z` through
  `2026-09-05T02:15:30.023141Z` (10:15:29–10:15:30 Hong Kong time).
- Cohort: `cohort:c45ca54e49f8b1823fc9cd347d7355fa00635555301ce36f3cddcdbc5150066d`.
- Complete manifest SHA-256:
  `35d090a10552a32737e3114d43972380bab0c04257e15368d50a8e7031e2ca3e`.
- Local data directory: `/private/tmp/cex-dex-live-multi-token.E4INfm`.
  This temporary runtime artifact is not committed to Git.
- All four collection legs were `observed`. The four accepted order books
  total 69,699 bytes. Twelve typed evidence files total 71,993 bytes; these
  include order-book copies and must not be counted as independent additional
  market samples. Cold loading, sizes, SHA-256 hashes, market identities, and
  book/rules/conversion roles were verified.
- Requests used the fixed public GET adapters on `data-api.binance.vision`,
  `api.binance.com`, and `api.bybit.com`. No account authentication or orders
  were involved.

## Observed website results

At `2026-09-05T02:15:59Z`, the API returned four rows for the $1,000 filter;
UNI and CAKE filters each returned two, and the strict filter returned zero.
The browser displayed the numeric UNI results and the CAKE missing-fee reasons.

| Token | Buy venue | Sell venue | $1,000 research net edge |
| --- | --- | --- | ---: |
| UNI | Bybit | Binance | -$3.5735828 |
| UNI | Binance | Bybit | -$4.35663 |
| CAKE | Binance | Bybit | N/A |
| CAKE | Bybit | Binance | N/A |

Across all five notionals there were ten UNI research rows and ten CAKE
unavailable rows. UNI used public fee references, with the account rate
unknown, and an explicit prepositioned-inventory assumption. These are
research outputs, not executable or guaranteed trading results. CAKE's
`cex_fee_public_bound_unavailable` components retained null fees and net
economics because the repository schedule has no exact CAKE fee rows.

After the 120-second quote lifetime, a real loopback API check confirmed that
all four displayed rows withheld quantities, gross/net edges, and fee values.
UNI changed to `cohort_stale`; CAKE retained its original missing-fee reason.
This runner performs one collection. Reloading the page does not collect a
new public order book, and this acceptance did not enable automatic refresh.

## Health discrepancy and regression fix

The fresh API correctly showed UNI as available, but health incorrectly
reported `cost_component_stale` because it treated CAKE's absent fee timestamps
as expired evidence. Health now applies the cost-expiry check to rows whose
stored class can supply numeric results; every row still undergoes quote
timestamp and age validation. Known unavailable rows retain their API reasons.

The regression first failed for both a missing-fee-only inventory and a mixed
inventory. Both cases now pass, while genuine fee expiry and quote expiry
remain covered. Re-evaluating the retained real artifact at the original
acceptance time reports `current`; evaluating it after 120 seconds reports
`cohort_stale`. These two fixed-time checks are local replay validation, not
additional live market requests.

After the health change, the Opportunity, collection, publication, frontend,
and release regression suite passed 554 tests in 60.414 seconds. Independent
review found no blocking issue. The preceding full repository run passed
3,126 tests at `51f1a41`; that full run predates this one-line health correction.
