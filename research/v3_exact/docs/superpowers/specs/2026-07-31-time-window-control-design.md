# Compact Time Window Control Design

## Goal

Make the global time-window control easier to scan and faster to use without
changing what the selected dates mean. Quick presets and custom dates remain
equally prominent, but the two date inputs no longer occupy the toolbar until
the user needs them.

## Approved layout

The default, collapsed toolbar uses the selected Option A layout:

- the left side keeps the calendar icon, `Time window` title, scope
  information, and available-data range;
- the center shows the currently applied range as a readable summary such as
  `23–29 Jul 2026 · 7 days`;
- the right side keeps the immediate presets `7D`, `30D`, `90D`, and `All`;
- a `Custom…` button opens an inline date editor directly below the toolbar.

The custom editor belongs to the toolbar and pushes the page content down. It
is not a floating modal or tooltip.

## Interaction states

### Collapsed

This is the default state. The applied-range summary is always visible.
Selecting `7D`, `30D`, `90D`, or `All` immediately applies that range using
the existing data refresh path.

### Custom editor open

Selecting `Custom…` expands a row containing:

- a labeled `Start` date input;
- a labeled `End` date input;
- `Cancel`;
- `Apply custom range`.

Opening the editor copies the currently applied dates into the inputs.
Changing an input creates a draft only and does not reload data.

`Cancel` discards the draft, restores the applied dates in the inputs, clears
custom validation errors, and collapses the editor.

`Apply custom range` uses the existing validation and refresh behavior. When
validation succeeds, the range becomes applied, the summary updates, and the
editor collapses. When validation fails, the editor remains open and the
existing inline error is shown next to the custom controls.

### Active selection

- A preset is active only when the applied start and end dates exactly match
  that preset's calculated range.
- `Custom…` is active when the applied range does not exactly match a preset.
- Merely opening or editing the custom form does not change the active
  selection; active styling represents applied data, not an unsubmitted draft.

During refresh, the command that initiated the refresh shows the existing
busy treatment and repeat submission is prevented. The last successfully
applied summary remains visible until the new request succeeds.

## Data and route behavior

The redesign changes presentation and interaction only:

- date values continue to use the existing start/end application logic;
- the selected range continues to be preserved in application routes;
- the available-data bounds and current validation rules remain authoritative;
- downstream price, volume, return, volatility, comparison, and coverage facts
  continue to share the applied daily window;
- independently timestamped TVL, depth, and execution facts remain outside the
  window, as described by the existing scope information.

No API, dataset, calculation, or backend behavior changes are in scope.

## Responsive layout

On wide screens, the summary, presets, and `Custom…` command stay on one row.
The expanded custom editor forms a second row aligned beneath them.

On narrow screens:

- the title and applied summary stack above the controls;
- the preset buttons remain a single, evenly sized segmented row;
- `Custom…` remains a full-width, clearly separate command;
- the expanded Start and End fields stack vertically;
- `Cancel` and `Apply custom range` remain large enough for touch use, with the
  primary Apply command receiving more visual weight.

## Accessibility

- The custom toggle exposes expanded/collapsed state with `aria-expanded` and
  references the editor with `aria-controls`.
- Presets continue to expose selection with `aria-pressed`.
- Start and End retain visible labels and the validation message remains an
  assertive live region.
- The editor can be operated fully by keyboard. Opening it moves focus to
  Start; successful apply or cancel returns focus to `Custom…`.
- Collapsing the editor does not remove or obscure the applied-range summary.
- Focus, hover, active, error, and disabled/busy states remain visually
  distinguishable in the existing dark theme.

## Implementation boundaries

Reuse the current toolbar markup, date inputs, form submission, preset
calculation, validation, loading, route, and refresh mechanisms where
possible. The implementation should introduce only the state needed to
separate applied dates from the open custom-form draft.

The redesign does not alter page navigation, pair selection, chart content,
data fetching contracts, or production deployment configuration.

## Verification and acceptance criteria

Frontend contract tests must verify:

- the collapsed toolbar contains the applied summary, four presets, and the
  `Custom…` toggle;
- a preset applies immediately and updates active-state semantics;
- opening the custom editor starts from the applied dates;
- editing custom dates does not apply or refresh data;
- cancel restores the applied values and closes the editor;
- a valid custom submission applies once, updates the summary, marks the
  correct preset or custom state, and closes the editor;
- invalid custom dates keep the editor open and expose the existing error;
- the applied range remains preserved across routes;
- required accessibility attributes and responsive layout rules are present.

The complete automated test suite must pass. The final UI must also be checked
in a real browser at desktop and narrow mobile widths against the approved
Option A layout.
