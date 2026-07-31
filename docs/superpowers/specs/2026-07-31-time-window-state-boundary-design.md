# Time Window State Boundary Design

## Decision

Use the approved minimal Option B architecture for the compact time-window
control:

- `app.payload.metadata.start_date` and `end_date` are the single source of
  truth for the applied window;
- `#date-start` and `#date-end` hold only the open Custom editor's draft;
- a successful market-summary request is the commit point for a new applied
  window;
- the Token catalog loads after that commit as an independent workspace
  dependency.

This preserves the approved Option A UI without introducing a second global
draft state object.

## Why this boundary is required

Before the compact control, the visible date inputs were treated as both user
input and global applied state. The Custom drawer adds an intentional period
where its inputs differ from the applied data. Any route, API request, link,
or export that reads those inputs during that period leaks an unsubmitted
draft.

The UI is small; the semantic change is that draft and applied dates can now
coexist. They must have different readers.

## State ownership

### Applied window

`appliedTimeWindow()` reads the current payload metadata. When no payload is
available during initial hydration, the parsed route remains the bootstrap
source until the first summary succeeds.

The following consumers always use the applied window unless an explicit
candidate is passed by the date-apply command:

- Screener and Workspace route builders;
- workspace tab and Back links;
- Comparison, Event, and Quality requests;
- CSV export filenames;
- applied-range summary and preset/Custom active styling.

### Draft window

`draftTimeWindow()` is the only business helper that reads the two date
inputs. `setDraftTimeWindow(window)` is the normal writer.

Draft values are used only for:

- Custom input display;
- inline date validation;
- the explicit Custom submit candidate.

Editing a draft cannot update routes, links, APIs, exported filenames,
applied summary, or active styling.

Opening Custom copies the applied window into the draft. Cancel restores the
applied window and closes the editor. A failed summary request keeps the
submitted draft visible for correction or retry.

## Apply flow

`applyWindow(candidate)` receives an explicit `{ start, end }` value.

For Custom, the caller passes `draftTimeWindow()`. For presets, the caller
passes `presetWindow(days)` directly and does not modify the draft first.

The command sequence is:

1. validate the candidate;
2. request the market summary while preserving the currently applied payload;
3. if the summary fails or becomes stale, return `false` without changing the
   applied URL, payload, summary, active state, or editor state;
4. if the summary succeeds and is still current, its metadata becomes the
   applied window;
5. replace the route using that explicit applied candidate;
6. synchronize the draft to the applied window, close the editor, and restore
   focus only for the successful current command;
7. on Workspace routes, start the catalog load for the newly committed
   window.

The market request ID remains the latest-wins authority. An older request
cannot update payload, URL, draft, active state, editor state, or focus after a
newer request starts.

## Catalog boundary

The catalog is a downstream workspace dependency, not part of committing the
daily summary window.

- A catalog success renders the Workspace for the already-applied window.
- A catalog failure displays the existing catalog-unavailable state and keeps
  the successfully applied summary and URL.
- Catalog failure does not reopen Custom or roll back the applied window.
- A stale catalog request returns without mutating newer state.
- Before a data-generation-mismatch retry calls `loadMarket()`, it must verify
  that its route request still owns the operation.

Time-window controls are disabled for the summary request. Workspace catalog
loading uses the existing workspace busy/error presentation after the summary
commit and does not masquerade as an uncommitted date request.

## Hydration rules

Initial route hydration may populate the draft inputs because no user draft
exists yet. Later payload or route rendering must not overwrite an open Custom
draft as an incidental side effect.

After successful apply or Cancel, the draft is explicitly synchronized to the
applied window. When the drawer is closed, background metadata refresh may
also synchronize the hidden draft to the applied window.

## Non-goals

- No backend, API contract, dataset, calculation, or date-validation changes.
- No retry policy or global state-machine framework.
- No rollback of a successfully committed summary because its downstream
  Token catalog failed.
- No change to pair selection, page navigation structure, or chart content.

## Acceptance criteria

- An unsubmitted draft cannot change any URL, link, API query, or CSV filename.
- Custom and every preset submit one explicit candidate window.
- Summary failure keeps the previous applied data and URL and leaves Custom
  open with its candidate.
- Summary success commits payload, summary, active state, and URL exactly once.
- Catalog failure leaves the newly applied window committed and shows the
  catalog error independently.
- Stale summary/catalog work produces no user-visible state mutation.
- Generation-mismatch retry checks route ownership before loading a summary.
- Existing keyboard, focus, ARIA, and responsive Option A behavior remains.
- Focused frontend tests, the complete suite, and desktop/mobile real-browser
  checks pass before publishing.
