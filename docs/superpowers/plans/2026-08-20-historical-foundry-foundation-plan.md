# Historical Foundry Foundation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Freeze the reviewed policy/authority/toolchain boundary and produce one deterministic, hash-bound two-venue Solidity executor that passes separate offline and connected fixed-block gates.

**Architecture:** A new pure Python contracts module validates three canonical checked-in JSON authorities and owns exact arithmetic shared by later scanner and publication phases. A narrow bootstrap script verifies immutable Foundry, solc, and forge-std identities without installing anything at dashboard runtime. The Solidity executor hard-codes UNI, WETH, both Router02 identities, and the authorized sender, while its entrypoint accepts only direction and WETH input. No historical RPC scan or public artifact is produced in this phase.

**Tech Stack:** Python standard library, `unittest`, Solidity 0.8.36, Foundry/Anvil/Forge/Cast v1.7.1, forge-std v1.16.1 pinned to a full commit.

**Spec:** `docs/superpowers/specs/2026-08-20-historical-foundry-replay-opportunity-design.md` sections “Fixed policy, authority, and toolchain”, “Authority contract”, “Block semantics”, “Pinned toolchain”, “Solidity boundary”, and “Policy and pure arithmetic”.

## Global Constraints

- Production loaders have no path, URL, override, environment, or profile parameters. They read the three tracked config files through descriptor-safe, no-follow, stable-reread logic.
- All three config files are exact-schema canonical JSON plus one LF. Policy physically binds authority and toolchain SHA-256; `policy_id` is derived and is not stored in policy bytes.
- The generic policy schema permits exact nonnegative MEV, including zero. The checked-in MVP policy is exactly 10/25/50 bps.
- The executor has no mutable application storage and accepts only `direction` and `amount_weth_in`. Routers, tokens, sender, deadlines, minimum outputs, and approvals are not caller inputs.
- The offline unit gate and connected fork gate are separate commands. Neither may silently skip because an RPC URL is absent.
- This phase does not edit live adapter/canary/connector configuration and does not write data under `raw/` or `routes/`.

## Reviewed Fixed-Identity Gate

This section is the sole source for immutable external specimens. The reviewed toolchain source identities are:

| Toolchain field | Exact reviewed value |
| --- | --- |
| Foundry version | `v1.7.1` |
| Foundry archive URL | `https://github.com/foundry-rs/foundry/releases/download/v1.7.1/foundry_v1.7.1_darwin_arm64.tar.gz` |
| Foundry archive SHA-256 | `eacdc67718fac857cad9e19c7f6729dd80de731d09df81856391d093cfcab547` |
| Foundry release commit | `4072e48705af9d93e3c0f6e29e93b5e9a40caed8` |
| checksum URL | `https://github.com/foundry-rs/foundry/releases/download/v1.7.1/foundry_v1.7.1_darwin_arm64.sha256` |
| checksum-file SHA-256 | `91b21b7f96cfad4e40a0ef18077777c5732e244ed795d476e5bcd153e18e4b5c` |
| Sigstore bundle URL | `https://github.com/foundry-rs/foundry/releases/download/v1.7.1/foundry_v1.7.1_darwin_arm64.sigstore.json` |
| Sigstore bundle SHA-256 | `d5930109b48c43a968ce8c0b2068c7d43e973a2b2604eb590a48c4c74a52159e` |
| SPDX URL | `https://github.com/foundry-rs/foundry/releases/download/v1.7.1/foundry_v1.7.1_darwin_arm64.spdx.json` |
| SPDX SHA-256 | `2a20a6956e75c08ba5b6aa2acbf62d5236b998bf58be00b7561d68af5aa0de0b` |
| Sigstore OIDC issuer | `https://token.actions.githubusercontent.com` |
| Sigstore certificate SAN | `https://github.com/foundry-rs/foundry/.github/workflows/release.yml@refs/tags/v1.7.1` |
| solc version | `0.8.36+commit.8a079791` |
| solc source commit | `8a079791d9cca7a6c03fd6a8429b93aa3bddefed` |
| solc universal macOS URL | `https://binaries.soliditylang.org/macosx-amd64/solc-macosx-amd64-v0.8.36+commit.8a079791` |
| solc artifact SHA-256 | `d4abcf0b3e24b7948ddfd64c374d26c3214648717777790ecb936979054a129d` |
| forge-std version | `v1.16.1` |
| forge-std repository | `https://github.com/foundry-rs/forge-std.git` |
| forge-std commit | `620536fa5277db4e3fd46772d5cbc1ea0696fb43` |
| EVM target / Anvil hardfork | `osaka` |
| sender domain UTF-8 | `historical_foundry_sender/v1` |
| sender Keccak-256 | `ff2670d16598d1a44884a9105ca9e6c3ed27cc0acfb355061fcab6964d4fc444` |
| sender address, low 20 bytes | `0x5ca9e6c3ed27cc0acfb355061fcab6964d4fc444` |
| executor domain UTF-8 | `historical_foundry_executor/v1` |
| executor Keccak-256 | `c7ebd81b3e449255a32b892a68778b870ceee58d82ba9f97cb4219981fdafa72` |
| executor address, low 20 bytes | `0x68778b870ceee58d82ba9f97cb4219981fdafa72` |

Task 3's verified extraction computes the exact SHA-256 of the extracted project-local `forge`, `cast`, and `anvil` files. Those three execution-derived digests must be independently reread, placed into the final toolchain authority, and frozen in known-answer tests before Task 4 starts; absence or disagreement is a hard blocker. Sender/executor derivation is exactly Ethereum Keccak-256 of the listed UTF-8 bytes with no prefix, suffix, NUL, newline, or EIP-191 framing, followed by the low 20 digest bytes. The repository pure Keccak implementation and sealed project-local `cast keccak` must reproduce both full digests and addresses. Before config commit, Task 4 proves at the reviewed KAT block and selected window that executor code/nonce/native/UNI/WETH balances and all four router allowances are zero and that the sender's fixed nonce/state meet the policy; any mismatch blocks the phase and requires a newly reviewed domain version, never a runtime address. No value may be guessed or copied from ambient `PATH`. The committed identities are projected into the three canonical authorities, checked-in KAT fixture, and bootstrap tests; a later change requires new reviewed bytes and hashes rather than a runtime option.

The KAT portion of that single table is already frozen from two independent archive-provider responses:

| KAT field | Exact reviewed value |
| --- | --- |
| block number | `25000000` (`0x17d7840`) |
| block hash | `0xf398976165ca4756c77fc6b61111fa1102d431eb03082417ecce38b36308d728` |
| parent hash | `0xc5a79102dcb47469ef357021c974bbbb92df3a1f3cfbcb5fdc0f9b36fb75e2c7` |
| state root | `0x055eba2b2b3daa967118fe831b0988cb27434e274f97f66cc67dcaa16dbe417f` |
| timestamp | `0x69f497f3` (`2026-05-01T12:09:23Z`) |
| base fee | `0x478d0e7f` |
| gas limit | `0x3938700` |
| gas used | `0x2035c7b` |
| Uniswap V2 UNI/WETH pair | `0xd3d2e2692501a5c9ca623199d38826e513033a17` |
| SushiSwap V2 UNI/WETH pair | `0xdafd66636e2561b0284edde37e42d192f2844d40` |
| Uniswap `getReserves()` raw response | `0x0000000000000000000000000000000000000000000051e38767437fac1d4c0f00000000000000000000000000000000000000000000001d6f8183a4807354760000000000000000000000000000000000000000000000000000000069f49013` |
| Uniswap `getReserves()` response SHA-256 | `204e4b1706f10e75947b770017a684d4c3379a17dbd1ea54851f447544f58461` |
| SushiSwap `getReserves()` raw response | `0x0000000000000000000000000000000000000000000000bd762b5d69a8be9e1700000000000000000000000000000000000000000000000044406e0af95d0c040000000000000000000000000000000000000000000000000000000069f47c0f` |
| SushiSwap `getReserves()` response SHA-256 | `7411473045715ec073ac3cc12a47475135f8de7883f59ec9d49657e083d06e33` |
| Chainlink `latestRoundData()` raw response | `0x000000000000000000000000000000000000000000000007000000000000701e000000000000000000000000000000000000000000000000000000353848f6320000000000000000000000000000000000000000000000000000000069f4963f0000000000000000000000000000000000000000000000000000000069f4964f000000000000000000000000000000000000000000000007000000000000701e` |
| Chainlink `latestRoundData()` response SHA-256 | `e6b59059a6b3440c906a9a24b007a64b965977f2b99e746105f98ed1af5376ad` |

The three response digests are SHA-256 over the complete exact `0x` hex ASCII response value retained in the fixture, with no quotes and no trailing LF. The checked-in fixture retains those exact response values as well as the digests; its validator recomputes each digest before decoding ABI words. Both archive providers matched the complete header and all three raw response values. Provider names/endpoints are not retained.

## Task 1: Freeze the Exact Policy, Authority, and Toolchain Schemas

**Files:**

- Create: `scripts/historical_foundry_contracts.py`
- Create: `tests/test_historical_foundry_contracts.py`

- [ ] **Step 0: Verify the Python runtime prerequisite**

```bash
python3 --version
python3.8 --version
```

The second command must report exactly Python 3.8.10 before Phase 1 can be signed off. It is currently an unresolved environment prerequisite; do not substitute `/usr/bin/python3` 3.9, an AST grammar check, or a nonexistent temporary path. Environment installation is a separate operator action, not part of the dashboard or Foundry bootstrap.

- [ ] **Step 1: Write exact-schema RED tests**

Add tests that import these public functions:

```python
from scripts.historical_foundry_contracts import (
    LoadedHistoricalConfig,
    policy_id_from_bytes,
    validate_historical_foundry_authority,
    validate_historical_foundry_policy,
    validate_historical_foundry_toolchain,
)
```

Freeze these behaviors:

```python
def test_policy_fixture_binds_exact_authority_and_toolchain_bytes(self):
    policy = validate_historical_foundry_policy(
        self.policy_payload(),
        authority_bytes=self.authority_bytes,
        toolchain_bytes=self.toolchain_bytes,
    )
    self.assertEqual(policy["authority_sha256"], sha256(self.authority_bytes).hexdigest())
    self.assertEqual(policy["toolchain_sha256"], sha256(self.toolchain_bytes).hexdigest())
    self.assertNotIn("policy_id", policy)
    self.assertRegex(policy_id_from_bytes(self.policy_bytes), r"\Apolicy:[0-9a-f]{64}\Z")

def test_generic_policy_accepts_hash_bound_zero_mev(self):
    raw = self.policy_payload(acceptance_mev_bps="0")
    normalized = validate_historical_foundry_policy(
        raw,
        authority_bytes=self.authority_bytes,
        toolchain_bytes=self.toolchain_bytes,
    )
    self.assertEqual(normalized["fees"]["acceptance_mev_bps"], "0")
```

Also reject unknown fields, duplicate JSON keys, noncanonical bytes, missing LF, wrong physical cross-hashes, booleans-as-integers, exponent decimals, negative rates, a sixth notional, a third direction, mutable chain/tag/lookback/selection, and policy ID embedded inside policy JSON. Descriptor/path attacks belong to the tracked-loader tests in Task 4 after real immutable identities exist.

Freeze these exact schema field sets in fixtures and validators; every named object is closed and every listed array has fixed order:

- Policy top level: `schema`, `chain_id`, `anchor_tag`, `lookback_seconds`, `selection_rule`, `requested_notionals_usd`, `directions`, `max_eth_usd_age_seconds`, `state_basis`, `execution`, `fees`, `profitability`, `closed_revert_matrix`, `authority_sha256`, `toolchain_sha256`.
- `execution`: `model`, `synthetic_timestamp_offset_seconds`, `calldata_deadline_offset_seconds`, `router_min_output_raw`, `transaction_type`, `transaction_gas_limit`, `access_list`, `sender_nonce`; `router_min_output_raw` has exactly `first_leg` and `second_leg` and `access_list` is exactly empty.
- `fees`: `next_base_fee_rule`, `acceptance_tip_percentile`, `stress_tip_percentile`, `max_fee_multiplier`, `acceptance_mev_bps`, `stress_mev_bps`; `stress_mev_bps` is the ordered two-element array for 25 and 50.
- `profitability`: `winner_comparison`, `exact_zero_result`, `serialization`; the values freeze strict-positive, reject, and canonical-fixed-point behavior.
- Each `closed_revert_matrix` entry: `prefilter_reason`, `leg`, `revert_selector`, `revert_data_sha256`, `terminal_class`; the array is canonical-sorted and accepts only the two reviewed zero-output/zero-liquidity projections.
- Authority top level: `schema`, `chain_id`, `tokens`, `venues`, `price_feed`, `sender`, `executor`, `v2_formula`, `state_override_layout`.
- Each ordered `tokens` entry: `role`, `address`, `decimals`, `balance_descriptor`, `allowance_descriptor`; each descriptor has exactly `kind`, `slot`, `key_order`, and `getter_selector`.
- Each ordered `venues` entry: `venue_id`, `router_address`, `factory_address`, `factory_selector`, `weth_selector`, `pair_getter_selector`, `pair_derivation`; the factory is fixed authority while no pair address field is permitted.
- `price_feed`: `proxy_address`, `description`, `decimals`, `latest_round_selector`, `aggregator_selector`, `phase_selector`; `sender`: `address`, `nonce`; `executor`: `address`, `prior_code`, `prior_nonce`, `prior_token_balances`, `prior_allowances`.
- `v2_formula`: `fee_numerator`, `fee_denominator`; `state_override_layout`: `account_roles`, `storage_roles`, `weth_backing_rule`, `allowance_matrix_rule`, each a closed ordered role list or named rule.
- Toolchain top level: `schema`, `foundry_release`, `binaries`, `solc`, `forge_std`, `compiler_settings`, `executor_build`.
- `foundry_release`: `version`, `archive_url`, `archive_sha256`, `checksum_url`, `checksum_sha256`, `provenance_url`, `provenance_sha256`, `sigstore_issuer`, `sigstore_identity`, `release_commit`; each ordered `binaries` entry: `name`, `version`, `sha256` for exactly forge, cast, and anvil.
- `solc`: `version`, `artifact_url`, `artifact_sha256`; `forge_std`: `repository_url`, `version`, `commit`; `compiler_settings`: `evm_version`, `fork_hardfork`, `optimizer_enabled`, `optimizer_runs`, `via_ir`, `bytecode_hash`, `cbor_metadata`, `append_cbor`. Both target fields are exactly `osaka` for this reviewed run.
- `executor_build`: `source_tree_sha256`, `constructor_args_sha256`, `creation_bytecode_sha256`, `deployed_runtime_sha256`, `immutable_references_sha256`, `artifact_manifest_sha256`.

Tests assert exact top-level and nested key sets, array order, type, canonical serialization, and values required by the design. Any additional metadata belongs in `LoadedHistoricalConfig` or a run/report, never inside these payloads.

- [ ] **Step 2: Run the RED tests**

```bash
python3 -m unittest tests.test_historical_foundry_contracts -v
```

Expected: import failure for `scripts.historical_foundry_contracts`.

- [ ] **Step 3: Implement closed validators and typed physical metadata**

The exact payload and its derived physical metadata stay separate:

```python
@dataclass(frozen=True)
class LoadedHistoricalConfig:
    value: Mapping[str, Any]
    physical_bytes: bytes = field(repr=False)
    physical_sha256: str
    policy_id: Optional[str] = None
```

`value` contains exactly schema fields; `physical_sha256` and `policy_id` are never injected into it. Task 1 implements only pure validators and typed-hash helpers. Tracked loaders and the cross-bound `HistoricalFoundryConfigSet` land in Task 4 after real archive, compiler, dependency, and bytecode identities have been reviewed and fixed.

The pure validators may accept bytes explicitly for unit-test KATs, but may not read paths:

```python
def validate_historical_foundry_policy(
    value: Mapping[str, Any],
    *,
    authority_bytes: bytes,
    toolchain_bytes: bytes,
) -> Dict[str, Any]: ...
```

Return immutable deep-copied payloads inside `LoadedHistoricalConfig`; never mutate caller objects. Reuse the repository's exact canonical JSON/hash conventions, but keep historical schemas closed and independent of live Shadow profile selection.

The eventual checked-in payloads must encode the exact decisions in the design: chain 1, finalized anchor, 604800 seconds, descending newest-publishable selection, five fixed notionals, two directions, 3600-second inclusive feed age, state-override-next-block execution, fixed B+12 timestamp, +60 calldata deadline, 2,000,000 gas limit, fixed nonce, p50 acceptance, p90 stress, 10/25/50 MEV, exact-zero loser, and the closed-revert matrix. Task 1 freezes their schemas and pure validation only; it does not create specimen config bytes before the reviewed fixed-identity gate is complete.

- [ ] **Step 4: Run focused GREEN and syntax checks**

```bash
python3 -m unittest tests.test_historical_foundry_contracts -v
python3 -m py_compile scripts/historical_foundry_contracts.py
python3.8 -m unittest \
  tests.test_historical_foundry_contracts -v
```

- [ ] **Step 5: Commit the contract slice**

```bash
git add scripts/historical_foundry_contracts.py \
  tests/test_historical_foundry_contracts.py
git commit -m "feat(opportunity): freeze historical replay authorities"
```

## Task 2: Implement Separate Exact Prefilter and Receipt-Economics Primitives

**Files:**

- Modify: `scripts/historical_foundry_contracts.py`
- Modify: `tests/test_historical_foundry_contracts.py`
- Reference without widening: `scripts/route_cost_evidence.py`
- Reference without widening: `scripts/route_quantity.py`

- [ ] **Step 1: Add arithmetic RED known-answer tests**

Freeze public pure signatures:

```python
def amount_weth_in_wei(
    requested_notional_usd: int,
    eth_usd_answer: int,
    feed_decimals: int,
) -> int: ...

def quote_v2_exact_in(
    amount_in: int,
    reserve_in: int,
    reserve_out: int,
) -> int: ...

def project_historical_prefilter_math(
    *,
    requested_notional_usd: int,
    direction: str,
    first_reserves: Tuple[int, int],
    second_reserves: Tuple[int, int],
    eth_usd_answer: int,
    feed_decimals: int,
    parent_base_fee: int,
    parent_gas_used: int,
    parent_gas_limit: int,
    acceptance_mev_bps: str,
) -> Dict[str, Any]: ...

def project_historical_receipt_economics(
    *,
    prefilter_projection: Mapping[str, Any],
    actual_intermediate_uni_raw: int,
    actual_final_weth_raw: int,
    receipt_gas_used: int,
    receipt_effective_gas_price: int,
    p50_priority_fee_wei: int,
    p90_priority_fee_wei: int,
    acceptance_mev_bps: str,
    stress_mev_bps: Tuple[str, str],
) -> Dict[str, Any]: ...
```

Test exact floor semantics, reserve/token order reversal, EIP-1559 increase/decrease/equality boundaries, fixed-point serialization, exact-zero prefilter behavior, and integer/rational USD conversion in the prefilter primitive. Separately test receipt delta binding, `receipt_effective_gas_price == child_base_fee + p50_priority_fee_wei`, p50 charged gas, p90 stress gas, `2 * child_base_fee + tip` envelope parity, 10/25/50 MEV projections, exact-zero policy-net rejection, and minimum positive unit acceptance in the receipt primitive. The prefilter function never accepts receipt gas, p50, p90, or a measured-status input; the receipt function never recomputes reserves or accepts a replacement child base fee.

The decisive regression is:

```python
def test_safe_exclusion_uses_policy_zero_mev_without_hidden_ten_bps(self):
    row = project_historical_prefilter_math(
        **self.zero_rate_case(), acceptance_mev_bps="0"
    )
    self.assertEqual(row["decision"], "replay_required")
```

- [ ] **Step 2: Run RED**

```bash
python3 -m unittest \
  tests.test_historical_foundry_contracts.HistoricalFoundryArithmeticTests -v
```

Expected: missing arithmetic functions.

- [ ] **Step 3: Implement exact arithmetic**

Use integers and `Fraction` internally. Canonical economic strings must never contain exponent notation or trailing fractional zeros. The only prefilter exclusions are exact zero output, nonpositive gross WETH, or the design's 21,000-gas/zero-tip upper-bound net `<= 0`. Do not add reserve-size, slippage, gas-estimate, volume, or profitability heuristics. Production callers obtain policy rates and fee rows only from a validated `HistoricalFoundryConfigSet` and validated window member; the explicit scalar parameters above exist for pure KATs and are not exposed as scan CLI options.

- [ ] **Step 4: Run focused GREEN and existing parity tests**

```bash
python3 -m unittest \
  tests.test_historical_foundry_contracts \
  tests.test_route_quantity \
  tests.test_route_cost_evidence -v
```

- [ ] **Step 5: Commit the arithmetic slice**

```bash
git add scripts/historical_foundry_contracts.py \
  tests/test_historical_foundry_contracts.py
git commit -m "feat(opportunity): add exact historical replay arithmetic"
```

## Task 3: Bootstrap and Verify the Pinned Foundry Toolchain

**Files:**

- Create: `scripts/bootstrap_historical_foundry_toolchain.py`
- Create: `tests/test_historical_foundry_toolchain.py`
- Create: `foundry.toml`
- Create: `foundry.lock`
- Create: `.gitmodules`
- Create: `lib/forge-std/` as a pinned submodule
- Modify: `.gitignore`

- [ ] **Step 1: Write RED bootstrap contract tests**

Tests must prove:

- the bootstrap command has no caller-supplied executable path, compiler, optimizer, EVM target, archive URL, checksum, or dependency revision;
- it verifies the official immutable release archive and every downloaded checksum/Sigstore/SPDX sidecar against the exact reviewed SHA-256 table before extraction, then checks that the bounded Sigstore payload's signed message digest, issuer, and SAN projections equal the reviewed values;
- `forge`, `cast`, and `anvil` all report v1.7.1;
- solc is exactly 0.8.36, optimizer enabled with 200 runs, `via_ir=false`, fixed metadata settings, and one explicit EVM version;
- forge-std is a submodule at the reviewed full commit emitted in the candidate identity projection;
- every subprocess resolves to the project-local, lock-digest-scoped binary directory and hashes the held no-follow executable before and after invocation; changing one byte, replacing an inode, using a symlink/hardlink, or placing a different executable first on `PATH` fails before evidence is accepted;
- an unavailable/mixed hardfork closes as `fork_hardfork_unsupported` or `fork_window_mixed` with no fallback; and
- no download or installation happens from a dashboard import or server startup.

Mock download bytes in unit tests. Keep the one real bootstrap as an explicit connected operator command.

Task 3 does not check in a partial toolchain document: the exact schema also binds executor creation/runtime identities, which do not exist until Task 4. The bootstrap emits a canonical candidate identity projection for review; it does not write config files. The KAT block/hash/state-root/timestamp come only from the same reviewed table and later checked-in fixture; no environment or CLI block selector exists.

- [ ] **Step 2: Run RED**

```bash
python3 -m unittest tests.test_historical_foundry_toolchain -v
```

Expected: missing bootstrap module and Foundry files.

- [ ] **Step 3: Implement bootstrap and finalize exact identities**

The production entrypoints are closed:

```python
def bootstrap_historical_foundry_toolchain() -> Mapping[str, Any]: ...
def open_reviewed_historical_toolchain() -> "ReviewedHistoricalToolchain": ...
```

The CLI accepts exactly one of four mutually exclusive no-value modes and rejects every other argument: `--bootstrap-reviewed`, `--print-verified-identity`, `--verify-offline-tests`, or `--verify-connected-kat`. `--verify-offline-tests` always performs a clean build and invokes the fixed equivalent of `forge test --offline --match-path foundry/test/TwoVenueV2Unit.t.sol -vvv`; `--verify-connected-kat` invokes only the fixed fork test and obtains its endpoint and block from the sealed sources described below. Neither verification mode accepts a test selector, forge flag, path, executable, compiler, EVM target, endpoint, or block.

It downloads into a private temporary directory, verifies the official archive and all sidecar physical hashes plus the bounded Sigstore message-digest/issuer/SAN projections against this section, then atomically installs only into `.historical-foundry/toolchains/<source-lock-sha256>/`. `source-lock-sha256` is the typed `historical_foundry_toolchain_source_lock/v1` SHA-256 of the canonical external-source table above; it deliberately excludes executor/build outputs and therefore has no lifecycle cycle. The bootstrap does not claim to cryptographically re-verify the Sigstore certificate/Transparency Log with Python stdlib; acceptance authority is the exact archive SHA independently fixed from the official release/checksum/signed-bundle materials. Adding cryptographic Sigstore verification later requires a separately pinned verifier/trust-root identity.

That generated directory is ignored, private to the current user, and contains exact `bin/forge`, `bin/cast`, `bin/anvil`, and `bin/solc` members. `open_reviewed_historical_toolchain()` opens the directory and binaries through no-follow descriptors, rejects link count other than one and ownership/mode drift, compares exact SHA-256 values with the post-bootstrap reviewed projection, and returns a non-serializable capability whose `repr` contains no path. Every compiler, test, KAT, and replay invocation must go through that capability with an internally fixed argument array, no shell, a minimal allowlisted environment, and stable pre/post inode/size/hash verification. `PATH`, `FOUNDRY_*`, `SOLC_*`, a caller executable, and a caller argument suffix are never consulted.

The bootstrap hashes compiler/dependency/build inputs and prints only a credential-free canonical candidate result. It must not mutate checked-in JSON automatically. Bootstrap known-answer tests compare the exact reviewed-table projection with the final Task-4 toolchain authority rather than maintaining a second internal asset list.

The repository must ignore only generated Foundry output such as `out/`, `cache/`, and `.historical-foundry/toolchains/`; do not ignore `foundry.lock`, Solidity source, tests, or the submodule pointer. Task 5 adds one checked-in fixed-block KAT fixture; its complete header, pair identities, and reserve/Chainlink digest inventory are exact projections of the reviewed table, are not accepted from an environment variable or CLI override, and are not inserted as unrelated fields into the toolchain schema.

- [ ] **Step 4: Run unit and real bootstrap gates**

```bash
python3 -m unittest tests.test_historical_foundry_toolchain -v
python3 -m scripts.bootstrap_historical_foundry_toolchain --bootstrap-reviewed
python3 -m scripts.bootstrap_historical_foundry_toolchain --print-verified-identity
```

`--bootstrap-reviewed` is allowed only against the exact external source rows above. It computes the three extracted-binary digests; record them in the Task-3 tests/identity projection and rerun `--print-verified-identity` only after that projection is exact. If the official archive/sidecar identities, bounded provenance projections, project-local executable identities, or active-window hardfork cannot be verified, stop here and review new toolchain bytes; do not start Task 4, substitute a version, or fall back to an ambient binary.

- [ ] **Step 5: Commit the pinned toolchain**

```bash
git add scripts/bootstrap_historical_foundry_toolchain.py \
  tests/test_historical_foundry_toolchain.py \
  foundry.toml foundry.lock .gitmodules lib/forge-std \
  .gitignore
git commit -m "build(opportunity): pin historical Foundry toolchain"
```

## Task 4: Implement the Storage-Free Two-Venue Executor

**Files:**

- Create: `foundry/src/TwoVenueV2Executor.sol`
- Create: `foundry/test/TwoVenueV2Unit.t.sol`
- Create: `config/historical_foundry_replay_policy.json`
- Create: `config/historical_foundry_replay_authority.json`
- Create: `config/historical_foundry_replay_toolchain.json`
- Modify: `tests/test_historical_foundry_contracts.py`
- Modify: `tests/test_historical_foundry_toolchain.py`

- [ ] **Step 1: Write offline Solidity RED tests**

The contract surface must remain:

```solidity
enum Direction { UniswapToSushi, SushiToUniswap }

function execute(Direction direction, uint256 amountWethIn)
    external
    returns (uint256 intermediateUni, uint256 finalWeth);
```

Unit tests must freeze immutable addresses, authorized sender, direction ordering, exact Router02 paths, zero minimum output, `block.timestamp + 60`, actual first-leg UNI delta as second-leg input, no caller recipient, no arbitrary calldata, unauthorized caller rejection, invalid direction rejection, zero input rejection, residual ETH/token rejection, and zero mutable storage writes. They also replace one byte in the validated runtime artifact and prove both tracked loading and later state-override derivation reject it; callers never supply runtime bytes.

- [ ] **Step 2: Run the offline RED gate**

```bash
python3 -m scripts.bootstrap_historical_foundry_toolchain \
  --verify-offline-tests
```

Expected: contract/test compile failure because the executor does not exist.

- [ ] **Step 3: Implement the minimal executor**

Use immutable constants/constructor values fixed by authority, infinite approvals only in the constructor KAT, and no owner/upgrader/withdraw/receive/fallback surface. Historical execution injects the compiled runtime and separately derived allowance overlay; it does not execute deployment or constructor transactions.

After compilation through `open_reviewed_historical_toolchain()`, hash creation bytecode, deployed runtime, and immutable patches. Persist those bytes only as manifest-inventoried build artifacts loaded through a sealed `ValidatedExecutorArtifact`; never expose a production function taking arbitrary runtime bytes or an artifact path. Create the final exact toolchain JSON with release/compiler/dependency/build identities, the authority JSON, and the policy that binds their physical hashes atomically.

Add the tracked-loader tests now:

```python
def load_historical_foundry_policy() -> LoadedHistoricalConfig: ...
def load_historical_foundry_authority() -> LoadedHistoricalConfig: ...
def load_historical_foundry_toolchain() -> LoadedHistoricalConfig: ...
def load_historical_foundry_config_set() -> "HistoricalFoundryConfigSet": ...
def build_validated_executor_artifact(
    config: "HistoricalFoundryConfigSet",
) -> "ValidatedExecutorArtifact": ...
```

The loaders reject symlink/hardlink members, ancestry swaps, noncanonical bytes, caller paths, physical hash drift, and cross-hash mismatch. `HistoricalFoundryConfigSet` refuses independently valid documents whose exact bytes are not mutually bound. `build_validated_executor_artifact` invokes the sealed compiler/build mode, reads only its internally fixed generated members through held descriptors, verifies the exact artifact manifest/creation/runtime/immutable-reference hashes against the config, and returns a non-serializable capability. Tests alter one artifact byte after validation, swap its inode, and substitute an ambient compiler; all three fail before an artifact capability can reach state-override code.

- [ ] **Step 4: Run offline GREEN twice**

```bash
python3 -m scripts.bootstrap_historical_foundry_toolchain \
  --verify-offline-tests
python3 -m scripts.bootstrap_historical_foundry_toolchain \
  --verify-offline-tests
```

Require identical creation/runtime hashes across the two clean builds.

- [ ] **Step 5: Commit the executor slice**

```bash
git add foundry/src/TwoVenueV2Executor.sol \
  foundry/test/TwoVenueV2Unit.t.sol \
  config/historical_foundry_replay_toolchain.json \
  config/historical_foundry_replay_authority.json \
  config/historical_foundry_replay_policy.json \
  tests/test_historical_foundry_contracts.py \
  tests/test_historical_foundry_toolchain.py
git commit -m "feat(opportunity): add sealed two-venue executor"
```

## Task 5: Add One Connected Fixed-Block Fork KAT

**Files:**

- Create: `foundry/test/TwoVenueV2Fork.t.sol`
- Create: `tests/fixtures/historical_foundry_kat.json`
- Modify: `tests/test_historical_foundry_toolchain.py`
- Create: `docs/superpowers/reports/2026-08-20-historical-foundry-foundation-report.md`

- [ ] **Step 1: Write the connected fork RED**

The test must fail if `DEX_DEPTH_RPC_ETH` is absent; it may not silently skip. The wrapper reads the full reviewed block header, derived pair identities, and reserve/Chainlink request-response digest inventory only from canonical checked-in `tests/fixtures/historical_foundry_kat.json`, whose bytes must project exactly from the reviewed fixed-identity table. It validates number/hash/parent hash/state root/timestamp/base fee/gas limit/gas used and every fixture digest before executing, and offers no block or state override. At that block it must verify both router/factory/WETH identities, derive both pairs in both token orders, verify token ordering/decimals, verify nonempty runtime code, run exactly both directions × all five fixed notionals (ten scenarios), and assert balance-delta closure for every scenario.

- [ ] **Step 2: Run RED against the fixed archive block**

```bash
python3 -m scripts.bootstrap_historical_foundry_toolchain \
  --verify-connected-kat
```

Expected: a real failing assertion for the unimplemented fork setup, not a skipped test.

- [ ] **Step 3: Implement the fixed-block KAT only**

Do not implement the seven-day scanner here. The KAT proves the executor and pinned EVM behavior at one reviewed archive block. It must not broadcast outside the local fork and must not print the RPC endpoint.

- [ ] **Step 4: Run Phase 1 verification**

```bash
python3 -m unittest \
  tests.test_historical_foundry_contracts \
  tests.test_historical_foundry_toolchain \
  tests.test_route_quantity \
  tests.test_route_cost_evidence -v
python3.8 -m unittest \
  tests.test_historical_foundry_contracts \
  tests.test_historical_foundry_toolchain \
  tests.test_route_quantity \
  tests.test_route_cost_evidence -v
python3 -m scripts.bootstrap_historical_foundry_toolchain \
  --verify-offline-tests
python3 -m scripts.bootstrap_historical_foundry_toolchain \
  --verify-connected-kat
python3 -m py_compile \
  scripts/historical_foundry_contracts.py \
  scripts/bootstrap_historical_foundry_toolchain.py
git diff --check
```

- [ ] **Step 5: Write the report and commit**

The report records exact tool versions, archive SHA, forge-std commit, solc identity, EVM target, creation/runtime hashes, the complete KAT header identity, pair identities, reserve/Chainlink response digests, test counts, and any evidence boundary. It must not include either RPC endpoint or provider identity.

```bash
git add foundry/test/TwoVenueV2Fork.t.sol \
  tests/fixtures/historical_foundry_kat.json \
  tests/test_historical_foundry_toolchain.py \
  docs/superpowers/reports/2026-08-20-historical-foundry-foundation-report.md
git commit -m "test(opportunity): verify fixed-block Foundry foundation"
```

## Phase Exit Review

- [ ] Inspect the three canonical JSON files and recompute their physical SHA-256 values independently.
- [ ] Confirm policy ID derivation excludes an embedded `policy_id` field and binds exact policy bytes.
- [ ] Confirm zero-MEV generic KAT passes while checked-in policy remains exactly 10/25/50.
- [ ] Confirm executor ABI contains no arbitrary address, recipient, deadline, slippage, calldata, or signing input.
- [ ] Confirm clean-build bytecode hashes are deterministic and bound into toolchain/policy.
- [ ] Confirm every executed `forge`/`cast`/`anvil`/`solc` came from the project-local lock-digest directory and passed no-follow, single-link, ownership, exact-hash, and stable pre/post checks; an adversarial `PATH` substitution test must stay green.
- [ ] Confirm the KAT fixture exactly matches the reviewed fixed-identity table and offers no block/hash/runtime override.
- [ ] Confirm offline and connected tests are separate and neither can skip.
- [ ] Request an independent code review before starting the seven-day scanner.
