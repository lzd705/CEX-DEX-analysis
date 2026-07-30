# Apply Pair Compare Navigation Design

## Goal

Make the `Apply pair` command on a Token Markets page visibly apply the
selected Market A/B pair by navigating to that Token's Compare page.

## Approved behavior

- A valid pair is persisted before navigation.
- Navigation stays inside the single-page application.
- The destination is the current Token's `compare` route.
- The selected Market A/B IDs and current start/end dates remain in the route.
- An invalid or incomplete pair remains on the current page and uses the
  existing route refresh and validation notice behavior.

## Implementation

Add a small `applySelectedPair()` command in `dashboard/static/app.js`.
It delegates validation/persistence to `persistSelectedPair()`. On success it
calls `navigateTo(currentWorkspacePath("compare"))`; on failure it preserves
the existing in-place route refresh behavior. The button event handler calls
this command.

## Verification

Add a frontend contract regression test that verifies the command:

- gates navigation on `persistSelectedPair()`;
- preserves the invalid-pair refresh path; and
- navigates through `currentWorkspacePath("compare")` for a valid pair.

Then run the focused frontend test and the complete test suite.
