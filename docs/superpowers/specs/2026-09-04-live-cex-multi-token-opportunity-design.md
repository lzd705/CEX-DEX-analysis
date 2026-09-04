# Live CEX Multi-Token Opportunity Design

## Outcome

The live, read-only CEX Opportunity workflow will cover the reviewed pair
inventory `UNI/USDT` and `CAKE/USDT` on Binance and Bybit. One refresh will
produce both directions for each token at the existing five USD notionals, so
the complete inventory contains four markets, four routes, and twenty
scenarios.

The workflow remains research-only. It reads public books and public
instrument rules, retains their exact bytes and hashes, and never accepts API
keys, account state, wallets, orders, transfers, arbitrary hosts, or arbitrary
token input.

## Reviewed fixed inventory

The fixed ordered pair inventory is:

```python
(
    ("UNI", "UNI/USDT"),
    ("CAKE", "CAKE/USDT"),
)
```

The fixed venues remain `("binance", "bybit")`. The universe therefore has:

- `cex:binance:UNI/USDT` and `cex:bybit:UNI/USDT`;
- `cex:binance:CAKE/USDT` and `cex:bybit:CAKE/USDT`;
- two directional, same-token routes for UNI;
- two directional, same-token routes for CAKE;
- no cross-token route;
- notionals `1000`, `5000`, `10000`, `50000`, and `100000` USD.

The generation is the SHA-256 of this full canonical contract. Any change to
the pair inventory, venue inventory, route identity, notional grid, or fixed
selection metadata creates a different generation and prevents an older core
from being finalized as the new scope.

The CLI remains fixed. It does not gain `--token`, `--tokens`, `--venue`, or
endpoint controls. Expanding the reviewed inventory is a code-reviewed release
decision, not caller input.

## Derived inventory contract

No production check may retain literal assumptions that the live workflow has
two legs, two routes, or ten scenarios. For the sealed universe:

```text
market_count      = len(selected_legs)
route_count       = len(routes)
notional_count    = len(requested_notionals_usd)
opportunity_count = route_count * notional_count
```

For this release those values are `4`, `4`, `5`, and `20`. The runner verifies
the exact market and route identities from the sealed universe, not only the
counts.

The success receipt changes to `live_cex_opportunity_refresh/v2` and exposes
`token_pairs`, `venues`, `market_count`, `route_count`,
`opportunity_count`, `strict_eligible_count`, and `served`. The misleading
singular `token_pair` field is removed.

## Per-token failure isolation

The route cohort already retains failed and deadline-exceeded legs and
classifies only their dependent routes as unavailable. The complete
Opportunity publisher must preserve that result instead of rejecting the
entire refresh.

For a route whose retained timing row is not `within_sla`:

- emit one terminal opportunity for every requested notional;
- use the timing row's canonical reason, such as `buy_leg_unavailable`,
  `sell_leg_unavailable`, `route_deadline_exceeded`,
  `execution_adapter_unsupported`, `invalid_state_timestamp`, or
  `snapshot_skew_exceeded`;
- set the target quantity, state IDs, state timestamps, projections, and all
  economic fields to null;
- set `opportunity_class="unavailable"`, both strict flags false, and the
  publication attestation to null;
- emit the exact three CEX topology cost rows, all terminal, non-strict, null
  valued, and bound to the same route reason;
- bind the opportunity and costs to the retained route, legs, timing row,
  cohort ID, and core manifest hash;
- require empty typed-source aliases for that terminal scenario.

No zero or placeholder quantity is allowed. Null target quantity is valid only
when every component is terminal and the matching retained route timing proves
the route unavailable. Normal scenarios keep the existing positive-quantity,
typed-source, quote-replay, and cost-replay contracts unchanged.

This gives the required isolation: if a CAKE leg fails, the ten CAKE scenarios
remain visible as unavailable while the ten UNI scenarios can still be
published from their own valid evidence. A collector-wide exception or corrupt
lineage still fails the entire refresh and preserves the previous pointer.

## Fee evidence boundary

The repository currently has reviewed public-reference rows only for
`UNI/USDT`. It has no retained evidence authorizing an exact CAKE fee row.
This increment therefore does not copy UNI rates or add a wildcard.

When CAKE books and instrument rules are valid but no exact current CAKE fee
row exists, the existing public-fee resolver emits
`cex_fee_public_bound_unavailable`. CAKE scenarios remain unavailable with
null economics. A later source-review increment may add exact Binance and
Bybit `CAKE/USDT` rows; no code change should then be necessary.

## Publication and read path

Standard scenarios continue through the existing common-quantity calculation,
book replay, fee mechanics, USD projection, route classification, immutable
complete-bundle publisher, atomic latest pointer, and cold reload.

Terminal scenarios use a separate explicit build-input kind. The publisher
reconstructs them from the retained core before accepting them, checks that
the route timing is terminal, checks that their source-member map is empty,
and excludes them from the quantity-quote generation while retaining them in
the opportunity and cost generations.

The complete-bundle loader and dashboard validator accept a null target only
for this terminal shape. They reject a terminal input that contains numeric
economics, a nonterminal cost, typed-source aliases, a fabricated state ID, a
different reason, or an attestation.

The existing API and frontend already filter by token and render unavailable
rows without economics. Their tests must prove that a mixed UNI/CAKE payload
preserves token identity and that filtering one token cannot return the other.

## Acceptance contract

Implementation is complete only when:

1. The fixed universe has the exact four markets, four same-token routes,
   five notionals, and literal canonical generation.
2. The runner derives `4/4/20`, emits the v2 receipt, and rejects any core or
   cold-loaded inventory that differs from the sealed universe.
3. A four-market happy-path fixture publishes twenty non-strict scenarios.
4. A single terminal CAKE leg publishes ten valid UNI scenarios plus ten
   terminal CAKE scenarios; no CAKE economic field is numeric.
5. Missing CAKE fee evidence with otherwise usable books withholds only CAKE
   economics and never becomes a zero fee.
6. Adversarial tests reject null targets on normal scenarios and reject
   terminal scenarios with fake quantities, source members, state fields,
   numeric economics, mismatched reasons, or attestations.
7. Existing authenticated CEX, DEX, historical, API, publication, and release
   tests remain unchanged in claim strength.
8. One bounded real refresh uses only the four existing allowlisted Binance
   and Bybit book/rule endpoints, cold-loads the twenty-row bundle, and serves
   it on loopback. If CAKE fee evidence is absent, its rows are explicitly
   unavailable.

