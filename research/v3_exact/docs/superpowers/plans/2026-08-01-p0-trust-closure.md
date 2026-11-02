# P0 Trust Closure Implementation Plan

> **Required sub-skill:** Use superpowers:test-driven-development for each
> behavior change and superpowers:verification-before-completion before any
> completion or deployment claim.

1. Add failing collector tests for Crypto.com inventory preflight and Upbit
   legacy-alias migration.
2. Add a validated, bounded CEX current-instrument lifecycle manifest and
   failing server tests for quarantine, ranking exclusion, comparison
   suppression, and daily quality lineage.
3. Add failing API/UI tests for dual quality windows, exact N/A reasons, stale
   drilldown reset, and pending-Summary navigation ownership.
4. Implement the smallest collector, fact-contract, and UI changes that make
   those tests pass.
5. Add application/asset release identities and deployment-evidence tests.
6. Run targeted tests, the full unittest suite, JavaScript syntax checks,
   Python 3.8 grammar/import gates, release checker, and data audits.
7. Commit with explicit messages, push with an explicit release comment,
   deploy from the verified SHA, and verify health, release contract, desktop,
   and mobile behavior before reporting completion.
