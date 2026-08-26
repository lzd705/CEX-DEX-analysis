# Uniswap V3 Two-Pool Production Launch Design

## Decision

Publish exact pool-only execution facts for exactly two reviewed Ethereum
Uniswap V3 markets:

- UNI/WETH: `0x1d42064fc4beb5f8aaf85f4617ae8b3b5b8bd801`
- UNI/USDT: `0x3470447f3cecffac709d3e783a307790b0208d60`

No other V3 market becomes supported. The launch does not claim gas, router
fees, transfer taxes, MEV, account inventory, or realized execution.

## Publication boundary

The existing aggregate DEX coverage gate is necessary but insufficient for
this launch. A candidate may replace published facts for the exact two-market
scope only when all of these conditions hold:

1. the production inventory contains both authority identities with matching
   chain, DEX, pool address, token addresses, fee, and tick spacing;
2. both depth rows are `observed` and complete;
3. each market has exactly ten `observed` execution scenarios forming the
   unique Cartesian product of two directions and five configured notionals;
4. every execution scenario has exact QuoterV2 parity;
5. both markets use the same finalized block number and block hash;
6. every depth and execution row binds to the retained transcript SHA;
7. the transcript, V3 scan manifest, and GeckoTerminal USD source bytes are
   reread from disk and their hashes and identities validate; and
8. the normal aggregate coverage, freshness, schema, and regression gates pass.

Successful validation produces a deterministic
`uniswap_v3_exact_validation/v1` receipt. It binds the authority-file SHA,
sorted market IDs, TVL and depth snapshot IDs, the shared finalized block
number/hash, hashes of the scoped depth and execution facts, the two retained
pool-transcript hashes, the retained GeckoTerminal source hashes, and the
validated 2-by-5 Quoter scenario inventory. It contains no credentialed RPC
URL or absolute local path.

Failure withholds the new exact-scope candidate and preserves the prior
published generation. Missing, partial, unsupported, failed, and stale values
remain explicit and are never changed to zero.

## Public contract

The API must describe the two authority-approved markets as exact pool-only V3
execution and all other V3 markets as unsupported. The validation receipt is
published atomically with the depth and execution facts as
`uniswap_v3_exact_latest.json`. Public health and release validation reread the
facts and receipt and expose whether the exact scope is present, hash-bound,
and current; aggregate DEX freshness alone is not enough.

## Evidence and rollback

Before the first production publication, save a versioned backup of every
published file that can be replaced, plus byte SHA-256s and the deployed
application SHA. Publication evidence must retain the production candidate,
RPC transcripts, scan manifests, GeckoTerminal response, and validation
result. A rollback restores the saved generation and is immediately followed
by the same API, health, and release checks used for forward validation.

## Rollout

1. ship the release-hardening code and pass the GitHub Python 3.8/3.14 and
   Node-backed quality gates;
2. restore the unrelated stale CEX depth cycle so the overall release checker
   is green;
3. deploy the immutable reviewed SHA without changing data;
4. run a full production-inventory candidate with publication disabled;
5. run a UNI/WETH-only no-publish rehearsal, then atomically publish the
   validated combined two-pool scope and validate it;
6. confirm the next scheduled depth cycle reproduces the exact scope; and
7. retain the backup until the post-launch observation window is complete.

Any failed precondition stops the rollout without moving the production data
pointer. A successful one-off canary proves the calculation for that block; it
does not by itself prove ongoing availability.

Production full-inventory collection enables the exact gate explicitly.
Small fixture collections and unapproved bounded recoveries do not enable it.
The two authority markets cannot use bounded `merge-publish`, because updating
one market independently would violate the required shared block/hash. The
single-pool rehearsal is therefore strictly non-publishing.
