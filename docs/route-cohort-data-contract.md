# Route Cohort Data Contract

## Scope

This contract defines pure route identity and timing evidence. It does not
calculate costs, rank routes, or infer an opportunity from a shared snapshot
identifier. A `snapshot_id` is lineage only; timing is measured from the two
actual `state_observed_at` values.

## Exact timestamp arithmetic

`scripts.timestamp_contract.exact_rfc3339_epoch_seconds(value)` accepts a
timezone-aware RFC 3339 timestamp with an optional arbitrary-length fractional
second and returns a `Decimal` epoch value. `Z` and numeric timezone offsets
refer to the same UTC instant. The existing microsecond parser remains
unchanged.

`exact_timestamp_skew_seconds(left, right)` returns the absolute difference of
those values as a `Decimal`. No float conversion is permitted. For example,
the skew from `2026-08-01T12:00:00.000000000Z` to
`2026-08-01T12:01:00.000000000Z` is exactly `Decimal("60.000000000")` and
passes a 60-second SLA; `60.000000001` does not.

Timestamp arithmetic uses an operation-local Decimal precision derived from
the inputs, not the process-wide Decimal context. Arbitrarily long RFC 3339
fractions remain exact: `60.0000000000000000001` remains distinct from 60 and
does not pass the SLA.

## Candidate and leg identity

A route candidate must provide canonical strings for `token_symbol`,
`buy_market_id`, `sell_market_id`, and `route_mode`; identity is never coerced
with `str()`. Token symbols are upper-case identifier text; market IDs have no
surrounding whitespace and begin `cex:` or `dex:`; route modes are lower-case
underscore identifiers. Missing, empty, non-string, or noncanonical values
raise `ValueError("route candidate identity is invalid")`.
`canonical_route_id(candidate)` returns:

```text
route:{token_symbol}:{buy_market_id}->{sell_market_id}:{route_mode}
```

The arrow is directional: reversing the buy and sell market IDs produces a
different identifier. A candidate may carry `route_id` only when it equals
this canonical value.

Each route leg must provide a non-empty `leg_id` and `market_id`. A leg is
explicitly unavailable only when `available` is `false` or its `status` (or
`collection_status`) states an unavailable terminal condition. Missing state
timestamps are timestamp failures, not an implicit unavailable-leg state.

`validate_route_cohort_rows(candidates, legs)` rejects duplicate directed
candidates, duplicate non-empty `candidate_id` values, same-market routes, a
non-canonical supplied route ID, duplicate leg IDs, and incomplete leg
identity. A supplied `candidate_id` must be a canonical string; including an
unhashable value raises `ValueError("route candidate ID is invalid")` rather
than leaking a Python `TypeError`. The validator never silently deduplicates
data.

## Timing classification

`classify_route_timing(candidate, buy_leg, sell_leg)` returns only these
fields:

```text
route_id       canonical directional route ID
skew_seconds   exact fixed-point decimal text, or null when unavailable
timing_status  within_sla, outside_sla, or unavailable
reason_code    null or one stable reason code
```

`skew_sla_seconds` defaults to `60`; it may be supplied as a non-negative
decimal-text value. A route at the threshold is `within_sla`, and any larger
value is `outside_sla` with `snapshot_skew_exceeded`.

When `validated_at` is supplied, either state timestamp later than that exact
instant is invalid. Missing, timezone-naive, malformed, and future state
timestamps return `timing_status = unavailable`,
`reason_code = invalid_state_timestamp`, and `skew_seconds = null`. Parser
exception text is never returned.

If more than one condition applies, the first matching reason is used in this
fixed priority order:

1. `route_deadline_exceeded`
2. `execution_adapter_unsupported`
3. `buy_leg_unavailable`
4. `sell_leg_unavailable`
5. `invalid_state_timestamp`
6. `snapshot_skew_exceeded`
7. `route_mode_not_executable`

The deadline reason is asserted by `route_deadline_exceeded = true` or a
candidate or leg status of `deadline_exceeded`; it is not inferred from a
historic timestamp. Adapter support is false when
`execution_adapter_supported = false` or `execution_adapter_status = unsupported`
on the candidate or a leg. The final mode reason applies to `route_mode` values `research_only`,
`unsupported`, or `not_executable`, or when
`route_mode_not_executable = true`.
