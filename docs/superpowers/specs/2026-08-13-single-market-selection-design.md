# Single-Market Selection Design

## Decision

The Token Research workspace will support an intentional Market-A-only
selection without adding a visible mode switch. Market B remains optional in
the selector and its first option is exactly:

```text
Market A only — no comparison
```

The command label changes from `Apply pair` to `Apply selection`. Choosing a
real Market B preserves the current two-market experience. Choosing the
Market-A-only option removes every Market B and pair-derived presentation from
the research pages instead of rendering those fields as `N/A`.

This release retains the current line charts. Candlesticks, multi-market
selection, and Benchmark mode are explicitly out of scope.

## Product boundary

Market-A-only is an intentional user selection, not missing data. The UI and
state contract must keep these conditions distinct:

- `Market A only — no comparison`: the user intentionally selected one market;
- `N/A`: a fact for the selected Market A is missing, invalid, unsupported, or
  unavailable;
- incomplete selection: Market A has not been selected, or a shared link has
  invalid market state.

No synthetic `None` market is added to the catalog or sent to a market-data
endpoint. Market A must always be one exact catalog `market_id` for the current
Token.

## Selection and navigation

The Market B label becomes `Market B (optional)`. Its first dropdown option has
an empty value and the user-visible label `Market A only — no comparison`.

Because an absent Market B previously also meant an unfinished pair, the URL
and session state retain `selection=single` as an explicit internal marker.
The dropdown value itself remains empty; `single` is state only and does not
create another visible control or synthetic catalog market. Paired URLs omit
`selection` and remain unchanged for backward compatibility.

The state rules are:

1. an exact Market A plus an exact, distinct Market B is a paired selection;
2. an exact Market A plus `selection=single` and no Market B is a
   valid single-market selection;
3. no Market B without the marker remains an incomplete selection and may use
   the existing validation/default behavior;
4. `selection=single` combined with a Market B, an unknown Market A/B, or equal
   A/B identities is invalid and is never silently repaired into another
   selection.

Applying a valid selection navigates to Compare. The selected state follows
the user through Compare, Liquidity & Execution, Events, and Data Quality,
survives refresh and browser back/forward, and can be shared as a deep link.
The last valid selection is saved per Token. Replacing the empty option with a
real Market B immediately restores the normal A/B experience.

The Markets page continues to show the complete Token market inventory so the
user can inspect coverage and choose a Market B later. Its selector and status
copy clearly identify the active Market-A-only selection.

## Page behavior

### Compare

Single-market Compare displays only Market A's existing daily close-price and
Volume line series, Market A return, Market A daily volatility, event overlay,
and Market A daily observation table.

It hides, rather than fills with placeholders:

- Market B return and volatility;
- latest comparable date and comparable-day count;
- absolute Daily Price Gap and midpoint-relative gap;
- the Daily Price Gap chart control and derived series;
- Market B legend, tooltip values, table columns, and missing-value messages;
- all copy that claims two markets or a same-date comparison.

Actual missing Market A facts still render through the existing structured
`N/A` disclosure and line-gap behavior. No values are interpolated or
forward-filled.

### Liquidity & Execution

Single-market mode displays only Market A depth, directional liquidity,
fixed-notional execution facts, timing, completeness, fee scope, and quality
reasons. It hides Market B cards/columns/series and A/B snapshot skew. The
existing notional and buy/sell controls continue to work for Market A.

### Data Quality

Single-market mode requests and renders the exact selected Market A only. It
hides pair-oriented scope and count copy rather than showing other catalog
markets. Source lineage, freshness, retryability, limitations, and reason codes
remain unchanged for Market A.

### Events

Events remains Token-scoped and is unchanged. Compare may continue to overlay
matching Token Event Facts on the Market A line chart. Event timing never
claims market impact or causality.

## API and data contracts

The existing collectors, SQLite tables, daily data, depth publications,
execution publications, and quality evidence remain unchanged. This feature
changes only bounded API projections, navigation state, and rendering.

The Compare and execution-cost APIs accept one exact Market A only when the
request contains `selection=single` and omits `market_b`. Their paired request
and response contracts remain backward compatible. A single-market response
declares `selection_mode: "single"`, returns top-level `market_b: null`, and
does not expose pair-derived facts as usable numbers. Compare returns
`market_a_statistics`, an explicit latest Market A observation, and daily rows
containing only `date` plus `market_a`; it does not manufacture
`market_b_missing` for every date. Execution returns Market A plus
`snapshot_skew_seconds: null`, because no A/B skew exists.

Selected-scope Quality accepts exactly one `market_a` and no `market_b` when
`selection=single`, and exactly two distinct IDs when `selection` is absent.
Its response declares the selected identity inventory so the browser and
release checker can reject a response for the wrong market.

The server validates the explicit projection and market cardinality together.
A caller cannot obtain a single-market response merely by accidentally omitting
Market B from an otherwise paired request.

## Request ownership and failure behavior

Changing Market A, choosing or restoring Market B, changing Token, navigating,
or applying a new date window invalidates older in-flight Compare, execution,
and Quality requests. Only the latest request owned by the current URL state
may render or commit state.

Failures are isolated by page. A failed single-market execution request does
not erase an already rendered Compare result, and an Event failure does not
invalidate Market A facts. Invalid or unknown market IDs remain visible as a
bounded selection error; the browser does not silently substitute a default
market. A genuine missing Market A fact keeps its existing reason and `N/A`
semantics.

## Accessibility and responsive behavior

The optional Market B label and dropdown option must be available to assistive
technology. When Market B is intentionally absent, hidden B and pair-derived
elements are removed from keyboard navigation and the accessibility tree.
Status text announces either the exact A/B pair or `Market A only` after Apply.

Desktop and mobile layouts collapse to the existing single-column responsive
patterns without reserving blank Market B columns or cards.

## Verification

Automated coverage must prove:

- navigation parse/build, refresh, deep links, and back/forward preserve the
  intentional single selection;
- an absent B without the explicit marker is not misread as intentional single;
- session restoration is per Token and changing Token cannot leak another
  Token's market identity;
- Apply accepts exact A-only or exact A/B selections and rejects all other
  cardinalities;
- Compare renders one Price/Volume series and hides every B/gap/comparable
  element in single mode;
- Liquidity & Execution renders only A and never calculates A/B skew;
- Data Quality requests and renders exactly A in single mode;
- Events remains Token-scoped and its overlay still works;
- returning to a real Market B restores the unchanged pair workflow;
- stale or out-of-order responses cannot overwrite a newer selection;
- keyboard, screen-reader labels, narrow layouts, empty facts, errors, and
  genuine Market A `N/A` states remain usable;
- existing paired API, URL, rendering, and release-gate tests continue to pass.

Browser regression checks must cover the real Apply flow across all four
research pages at desktop and mobile widths. Completion requires focused tests,
the full applicable suite, release validation, and a clean public-browser run.

## Non-goals

- candlestick charts or OHLC API expansion;
- more than two selected markets;
- Benchmark comparison;
- changes to Route Opportunities or Funding Rate;
- order placement, execution guarantees, or trading recommendations;
- changes to collection cadence, raw data, or production deployment.
