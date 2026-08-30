import copy
import hashlib
import inspect
import json
import os
from pathlib import Path
import pickle
import re
import shutil
import tempfile
import unittest
from unittest import mock

from scripts import historical_foundry_contracts as contracts
from scripts.historical_foundry_contracts import (
    HistoricalFoundryConfigSet,
    LoadedHistoricalConfig,
    ValidatedExecutorArtifact,
    amount_weth_in_wei,
    build_validated_executor_artifact,
    load_historical_foundry_authority,
    load_historical_foundry_config_set,
    load_historical_foundry_policy,
    load_historical_foundry_toolchain,
    next_historical_base_fee,
    policy_id_from_bytes,
    project_historical_prefilter_math,
    project_historical_receipt_economics,
    quote_v2_exact_in,
    validate_historical_foundry_authority,
    validate_historical_foundry_policy,
    validate_historical_foundry_toolchain,
)


REAL_PROJECT_ROOT = Path(__file__).resolve().parents[1]


def canonical_bytes(value):
    return (
        json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        + "\n"
    ).encode("utf-8")


class HistoricalFoundryContractTests(unittest.TestCase):
    def setUp(self):
        self.authority = self.authority_payload()
        self.toolchain = self.toolchain_payload()
        self.authority_bytes = canonical_bytes(self.authority)
        self.toolchain_bytes = canonical_bytes(self.toolchain)
        self.policy = self.policy_payload()
        self.policy_bytes = canonical_bytes(self.policy)

    @staticmethod
    def authority_payload():
        return {
            "schema": "historical_foundry_replay_authority/v1",
            "chain_id": 1,
            "tokens": [
                {
                    "role": "uni",
                    "address": "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984",
                    "decimals": 18,
                    "balance_descriptor": {
                        "kind": "mapping", "slot": 4,
                        "key_order": "address_then_slot",
                        "getter_selector": "0x70a08231",
                    },
                    "allowance_descriptor": {
                        "kind": "mapping", "slot": 3,
                        "key_order": "owner_spender_then_slot",
                        "getter_selector": "0xdd62ed3e",
                    },
                },
                {
                    "role": "weth",
                    "address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                    "decimals": 18,
                    "balance_descriptor": {
                        "kind": "mapping", "slot": 3,
                        "key_order": "address_then_slot",
                        "getter_selector": "0x70a08231",
                    },
                    "allowance_descriptor": {
                        "kind": "mapping", "slot": 4,
                        "key_order": "owner_spender_then_slot",
                        "getter_selector": "0xdd62ed3e",
                    },
                },
            ],
            "venues": [
                {
                    "venue_id": "uniswap_v2",
                    "router_address": "0x7a250d5630b4cf539739df2c5dacb4c659f2488d",
                    "factory_address": "0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f",
                    "factory_selector": "0xc45a0155",
                    "weth_selector": "0xad5c4648",
                    "pair_getter_selector": "0xe6a43905",
                    "pair_derivation": "factory_get_pair_uni_weth",
                },
                {
                    "venue_id": "sushiswap_v2",
                    "router_address": "0xd9e1ce17f2641f24ae83637ab66a2cca9c378b9f",
                    "factory_address": "0xc0aee478e3658e2610c5f7a4a2e1777ce9e4f2ac",
                    "factory_selector": "0xc45a0155",
                    "weth_selector": "0xad5c4648",
                    "pair_getter_selector": "0xe6a43905",
                    "pair_derivation": "factory_get_pair_uni_weth",
                },
            ],
            "price_feed": {
                "proxy_address": "0x5f4ec3df9cbd43714fe2740f5e3616155c5b8419",
                "description": "ETH / USD",
                "decimals": 8,
                "latest_round_selector": "0xfeaf968c",
                "aggregator_selector": "0x245a7bfc",
                "phase_selector": "0x58303b10",
            },
            "sender": {
                "address": "0x5ca9e6c3ed27cc0acfb355061fcab6964d4fc444",
                "nonce": 0,
            },
            "executor": {
                "address": "0x68778b870ceee58d82ba9f97cb4219981fdafa72",
                "prior_code": "empty",
                "prior_nonce": 0,
                "prior_token_balances": ["uni", "weth"],
                "prior_allowances": [
                    "uni_uniswap_v2", "uni_sushiswap_v2",
                    "weth_uniswap_v2", "weth_sushiswap_v2",
                ],
            },
            "v2_formula": {"fee_numerator": 997, "fee_denominator": 1000},
            "state_override_layout": {
                "account_roles": ["sender", "executor", "weth"],
                "storage_roles": [
                    "executor_weth_balance", "weth_native_backing",
                    "executor_uni_allowances", "executor_weth_allowances",
                ],
                "weth_backing_rule": "executor_weth_delta_matches_weth_native_delta",
                "allowance_matrix_rule": "executor_to_each_router_for_each_token",
            },
        }

    @staticmethod
    def toolchain_payload():
        return {
            "schema": "historical_foundry_replay_toolchain/v1",
            "foundry_release": {
                "version": "v1.7.1",
                "archive_url": "https://github.com/foundry-rs/foundry/releases/download/v1.7.1/foundry_v1.7.1_darwin_arm64.tar.gz",
                "archive_sha256": "eacdc67718fac857cad9e19c7f6729dd80de731d09df81856391d093cfcab547",
                "checksum_url": "https://github.com/foundry-rs/foundry/releases/download/v1.7.1/foundry_v1.7.1_darwin_arm64.sha256",
                "checksum_sha256": "91b21b7f96cfad4e40a0ef18077777c5732e244ed795d476e5bcd153e18e4b5c",
                "provenance_url": "https://github.com/foundry-rs/foundry/releases/download/v1.7.1/foundry_v1.7.1_darwin_arm64.sigstore.json",
                "provenance_sha256": "d5930109b48c43a968ce8c0b2068c7d43e973a2b2604eb590a48c4c74a52159e",
                "sigstore_issuer": "https://token.actions.githubusercontent.com",
                "sigstore_identity": "https://github.com/foundry-rs/foundry/.github/workflows/release.yml@refs/tags/v1.7.1",
                "release_commit": "4072e48705af9d93e3c0f6e29e93b5e9a40caed8",
            },
            "binaries": [
                {"name": "forge", "version": "v1.7.1", "sha256": "0" * 64},
                {"name": "cast", "version": "v1.7.1", "sha256": "1" * 64},
                {"name": "anvil", "version": "v1.7.1", "sha256": "2" * 64},
            ],
            "solc": {
                "version": "0.8.36+commit.8a079791",
                "artifact_url": "https://binaries.soliditylang.org/macosx-amd64/solc-macosx-amd64-v0.8.36+commit.8a079791",
                "artifact_sha256": "d4abcf0b3e24b7948ddfd64c374d26c3214648717777790ecb936979054a129d",
            },
            "forge_std": {
                "repository_url": "https://github.com/foundry-rs/forge-std.git",
                "version": "v1.16.1",
                "commit": "620536fa5277db4e3fd46772d5cbc1ea0696fb43",
            },
            "compiler_settings": {
                "evm_version": "osaka", "fork_hardfork": "osaka",
                "optimizer_enabled": True, "optimizer_runs": 200,
                "via_ir": False, "bytecode_hash": "none",
                "cbor_metadata": False, "append_cbor": False,
            },
            "executor_build": {
                "source_tree_sha256": "3" * 64,
                "constructor_args_sha256": "4" * 64,
                "creation_bytecode_sha256": "5" * 64,
                "deployed_runtime_sha256": "6" * 64,
                "immutable_references_sha256": "7" * 64,
                "artifact_manifest_sha256": "8" * 64,
            },
        }

    def policy_payload(self, acceptance_mev_bps="10"):
        authority_sha = hashlib.sha256(self.authority_bytes).hexdigest()
        toolchain_sha = hashlib.sha256(self.toolchain_bytes).hexdigest()
        return {
            "schema": "historical_foundry_replay_policy/v1",
            "chain_id": 1,
            "anchor_tag": "finalized",
            "lookback_seconds": 604800,
            "selection_rule": "newest_publishable_policy_positive",
            "requested_notionals_usd": ["1000", "5000", "10000", "50000", "100000"],
            "directions": ["uniswap_to_sushiswap", "sushiswap_to_uniswap"],
            "max_eth_usd_age_seconds": 3600,
            "state_basis": "post_block_state",
            "execution": {
                "model": "historical_counterfactual_state_override_next_block",
                "synthetic_timestamp_offset_seconds": 12,
                "calldata_deadline_offset_seconds": 60,
                "router_min_output_raw": {"first_leg": "0", "second_leg": "0"},
                "transaction_type": "eip1559_type_2",
                "transaction_gas_limit": 2000000,
                "access_list": [],
                "sender_nonce": 0,
            },
            "fees": {
                "next_base_fee_rule": "eip1559_next_base_fee",
                "acceptance_tip_percentile": 50,
                "stress_tip_percentile": 90,
                "max_fee_multiplier": 2,
                "acceptance_mev_bps": acceptance_mev_bps,
                "stress_mev_bps": ["25", "50"],
            },
            "profitability": {
                "winner_comparison": "strict_positive",
                "exact_zero_result": "reject",
                "serialization": "canonical_fixed_point",
            },
            "closed_revert_matrix": [
                {
                    "prefilter_reason": "first_leg_zero_output",
                    "leg": "first_leg",
                    "revert_selector": "0x08c379a0",
                    "revert_data_sha256": "6798eb314455c46925e230068a2e4849cf2340aefa7480b4aece1cdc6ae36ba7",
                    "terminal_class": "closed_revert",
                },
                {
                    "prefilter_reason": "second_leg_zero_liquidity",
                    "leg": "second_leg",
                    "revert_selector": "0x08c379a0",
                    "revert_data_sha256": "9de19b1bd02b49383b079e33eb28592b7125d02f86cad8e24358a74830d1fe0b",
                    "terminal_class": "closed_revert",
                },
            ],
            "authority_sha256": authority_sha,
            "toolchain_sha256": toolchain_sha,
        }

    def test_policy_fixture_binds_exact_authority_and_toolchain_bytes(self):
        policy = validate_historical_foundry_policy(
            self.policy_payload(),
            authority_bytes=self.authority_bytes,
            toolchain_bytes=self.toolchain_bytes,
        )
        self.assertEqual(
            policy["authority_sha256"], hashlib.sha256(self.authority_bytes).hexdigest()
        )
        self.assertEqual(
            policy["toolchain_sha256"], hashlib.sha256(self.toolchain_bytes).hexdigest()
        )
        self.assertNotIn("policy_id", policy)
        self.assertRegex(policy_id_from_bytes(self.policy_bytes), r"\Apolicy:[0-9a-f]{64}\Z")

    def test_generic_policy_accepts_hash_bound_zero_mev(self):
        normalized = validate_historical_foundry_policy(
            self.policy_payload(acceptance_mev_bps="0"),
            authority_bytes=self.authority_bytes,
            toolchain_bytes=self.toolchain_bytes,
        )
        self.assertEqual(normalized["fees"]["acceptance_mev_bps"], "0")

    def test_authority_and_toolchain_are_closed_ordered_and_detached(self):
        authority = validate_historical_foundry_authority(self.authority_bytes)
        toolchain = validate_historical_foundry_toolchain(self.toolchain_bytes)
        self.assertEqual([item["role"] for item in authority["tokens"]], ["uni", "weth"])
        self.assertEqual(
            [item["venue_id"] for item in authority["venues"]],
            ["uniswap_v2", "sushiswap_v2"],
        )
        self.assertEqual([item["name"] for item in toolchain["binaries"]], ["forge", "cast", "anvil"])
        authority["tokens"][0]["address"] = "0x0000000000000000000000000000000000000000"
        self.assertEqual(
            self.authority["tokens"][0]["address"],
            "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984",
        )

    def test_authority_rejects_malformed_or_noncanonical_addresses_before_identity(self):
        for address in (
            "0x" + "1" * 39,
            "0x1F9840A85D5AF5BF1D1762F925BDADDC4201F984",
        ):
            authority = copy.deepcopy(self.authority)
            authority["tokens"][0]["address"] = address
            with self.subTest(address=address), self.assertRaisesRegex(
                ValueError,
                "authority token address is not a canonical lowercase Ethereum address",
            ):
                validate_historical_foundry_authority(authority)

    def test_loaded_metadata_never_injects_identity_into_schema_value(self):
        value = validate_historical_foundry_authority(self.authority)
        loaded = LoadedHistoricalConfig(
            value=value,
            physical_bytes=self.authority_bytes,
            physical_sha256=hashlib.sha256(self.authority_bytes).hexdigest(),
        )
        self.assertNotIn("physical_sha256", loaded.value)
        self.assertNotIn("policy_id", loaded.value)
        with self.assertRaises(AttributeError):
            loaded.physical_sha256 = "0" * 64
        with self.assertRaises(ValueError):
            LoadedHistoricalConfig(
                value=value,
                physical_bytes=self.authority_bytes,
                physical_sha256="0" * 64,
            )

    def test_loaded_config_revalidates_schema_and_derives_policy_identity(self):
        forbidden_metadata = copy.deepcopy(self.authority)
        forbidden_metadata["physical_sha256"] = "0" * 64
        with self.assertRaises(ValueError):
            LoadedHistoricalConfig(
                value=forbidden_metadata,
                physical_bytes=self.authority_bytes,
                physical_sha256=hashlib.sha256(self.authority_bytes).hexdigest(),
            )

        policy = validate_historical_foundry_policy(
            self.policy,
            authority_bytes=self.authority_bytes,
            toolchain_bytes=self.toolchain_bytes,
        )
        loaded = LoadedHistoricalConfig(
            value=policy,
            physical_bytes=self.policy_bytes,
            physical_sha256=hashlib.sha256(self.policy_bytes).hexdigest(),
        )
        self.assertEqual(loaded.policy_id, policy_id_from_bytes(self.policy_bytes))
        with self.assertRaises(ValueError):
            LoadedHistoricalConfig(
                value=policy,
                physical_bytes=self.policy_bytes,
                physical_sha256=hashlib.sha256(self.policy_bytes).hexdigest(),
                policy_id="policy:" + "0" * 64,
            )
        malformed_policy = copy.deepcopy(policy)
        malformed_policy["authority_sha256"] = "not-a-sha256"
        malformed_policy_bytes = canonical_bytes(malformed_policy)
        with self.assertRaises(ValueError):
            LoadedHistoricalConfig(
                value=malformed_policy,
                physical_bytes=malformed_policy_bytes,
                physical_sha256=hashlib.sha256(malformed_policy_bytes).hexdigest(),
            )

    def test_rejects_noncanonical_or_duplicate_json_bytes(self):
        duplicate = b'{"schema":"historical_foundry_replay_authority/v1","schema":"historical_foundry_replay_authority/v1"}\n'
        noncanonical = b'{ "schema":"historical_foundry_replay_authority/v1"}\n'
        missing_lf = self.authority_bytes[:-1]
        for payload in (duplicate, noncanonical, missing_lf):
            with self.subTest(payload=payload):
                with self.assertRaises(ValueError):
                    validate_historical_foundry_authority(payload)

    def test_rejects_policy_mutations_and_wrong_cross_hashes(self):
        mutations = []
        unknown = copy.deepcopy(self.policy)
        unknown["extra"] = "forbidden"
        mutations.append(unknown)
        policy_id = copy.deepcopy(self.policy)
        policy_id["policy_id"] = "policy:" + "0" * 64
        mutations.append(policy_id)
        sixth_notional = copy.deepcopy(self.policy)
        sixth_notional["requested_notionals_usd"].append("1000000")
        mutations.append(sixth_notional)
        third_direction = copy.deepcopy(self.policy)
        third_direction["directions"].append("other")
        mutations.append(third_direction)
        exponent = copy.deepcopy(self.policy)
        exponent["fees"]["acceptance_mev_bps"] = "1e1"
        mutations.append(exponent)
        negative = copy.deepcopy(self.policy)
        negative["fees"]["acceptance_mev_bps"] = "-1"
        mutations.append(negative)
        boolean = copy.deepcopy(self.policy)
        boolean["chain_id"] = True
        mutations.append(boolean)
        mutable = copy.deepcopy(self.policy)
        mutable["lookback_seconds"] = 1
        mutations.append(mutable)
        for raw in mutations:
            with self.subTest(raw=raw):
                with self.assertRaises(ValueError):
                    validate_historical_foundry_policy(
                        raw,
                        authority_bytes=self.authority_bytes,
                        toolchain_bytes=self.toolchain_bytes,
                    )
        with self.assertRaises(ValueError):
            validate_historical_foundry_policy(
                self.policy,
                authority_bytes=canonical_bytes({"changed": True}),
                toolchain_bytes=self.toolchain_bytes,
            )

    def test_rejects_authority_and_toolchain_schema_or_order_mutations(self):
        bad_authority = copy.deepcopy(self.authority)
        bad_authority["venues"].reverse()
        bad_toolchain = copy.deepcopy(self.toolchain)
        bad_toolchain["compiler_settings"]["evm_version"] = "cancun"
        bad_toolchain["binaries"].reverse()
        for validator, raw in (
            (validate_historical_foundry_authority, bad_authority),
            (validate_historical_foundry_toolchain, bad_toolchain),
        ):
            with self.assertRaises(ValueError):
                validator(raw)

    def test_rejects_unknown_nested_fields(self):
        policy = copy.deepcopy(self.policy)
        policy["execution"]["arbitrary_calldata"] = "forbidden"
        authority = copy.deepcopy(self.authority)
        authority["tokens"][0]["balance_descriptor"]["pair_address"] = "forbidden"
        toolchain = copy.deepcopy(self.toolchain)
        toolchain["executor_build"]["runtime_path"] = "forbidden"
        with self.assertRaises(ValueError):
            validate_historical_foundry_policy(
                policy,
                authority_bytes=self.authority_bytes,
                toolchain_bytes=self.toolchain_bytes,
            )
        with self.assertRaises(ValueError):
            validate_historical_foundry_authority(authority)
        with self.assertRaises(ValueError):
            validate_historical_foundry_toolchain(toolchain)

    def test_rejects_integer_lookalikes_for_compiler_booleans(self):
        for field, invalid_value in (
            ("optimizer_enabled", 1),
            ("via_ir", 0),
            ("cbor_metadata", 0),
            ("append_cbor", 0),
        ):
            toolchain = copy.deepcopy(self.toolchain)
            toolchain["compiler_settings"][field] = invalid_value
            with self.subTest(field=field):
                with self.assertRaises(ValueError):
                    validate_historical_foundry_toolchain(toolchain)


class HistoricalFoundryTrackedLoaderTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.config_dir = self.root / "config"
        self.config_dir.mkdir()
        for name in (
            "historical_foundry_replay_policy.json",
            "historical_foundry_replay_authority.json",
            "historical_foundry_replay_toolchain.json",
        ):
            shutil.copyfile(REAL_PROJECT_ROOT / "config" / name, self.config_dir / name)

    def tearDown(self):
        self.temporary.cleanup()

    def test_public_loader_and_build_surfaces_are_closed(self):
        for function in (
            load_historical_foundry_policy,
            load_historical_foundry_authority,
            load_historical_foundry_toolchain,
            load_historical_foundry_config_set,
        ):
            self.assertEqual(inspect.signature(function).parameters, {})
        self.assertEqual(
            tuple(inspect.signature(build_validated_executor_artifact).parameters),
            ("config",),
        )

    def test_tracked_exact_config_set_loads_and_physically_cross_binds(self):
        config = load_historical_foundry_config_set()
        self.assertIsInstance(config, HistoricalFoundryConfigSet)
        self.assertEqual(load_historical_foundry_policy().physical_sha256, config.policy.physical_sha256)
        self.assertEqual(load_historical_foundry_authority().physical_sha256, config.authority.physical_sha256)
        self.assertEqual(load_historical_foundry_toolchain().physical_sha256, config.toolchain.physical_sha256)
        self.assertEqual(config.policy.value["authority_sha256"], config.authority.physical_sha256)
        self.assertEqual(config.policy.value["toolchain_sha256"], config.toolchain.physical_sha256)
        self.assertEqual(config.policy.physical_sha256, "0f8f604a6c8087ce9e44ac6de4e81b71c65657d4c2dc05f862fda3306e2ba1f8")
        self.assertEqual(config.authority.physical_sha256, "6156c67cedb03dbf21c86028553445118fe41a1732e8da40ac961060a457cd59")
        self.assertEqual(config.toolchain.physical_sha256, "9af543d0b7744d2552d4d65b0bf6c5b9039bcfe65d4296bd82d1c736a577c42a")
        self.assertNotIn(str(REAL_PROJECT_ROOT), repr(config))
        with self.assertRaises((TypeError, pickle.PicklingError)):
            pickle.dumps(config)
        with self.assertRaises(AttributeError):
            config.policy = config.authority

    def test_tracked_authority_loads_canonical_uni_identity(self):
        authority = load_historical_foundry_authority()
        self.assertEqual(
            authority.value["tokens"][0]["address"],
            "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984",
        )

    def test_tracked_uni_identity_matches_fixed_foundry_executor(self):
        authority = load_historical_foundry_authority()
        source = (
            REAL_PROJECT_ROOT / "foundry" / "src" / "TwoVenueV2Executor.sol"
        ).read_text(encoding="utf-8")
        source_identity = re.search(
            r"address private constant UNI = (0x[0-9A-Fa-f]{40});",
            source,
        )
        self.assertIsNotNone(source_identity)
        self.assertEqual(
            authority.value["tokens"][0]["address"],
            source_identity.group(1).lower(),
        )

    def test_tracked_loaders_reject_symlink_and_hardlink_members(self):
        target = self.config_dir / "historical_foundry_replay_authority.json"
        original = target.read_bytes()
        for attack in ("symlink", "hardlink"):
            with self.subTest(attack=attack):
                target.unlink()
                specimen = self.root / ("specimen-" + attack + ".json")
                specimen.write_bytes(original)
                if attack == "symlink":
                    os.symlink(specimen, target)
                else:
                    os.link(specimen, target)
                with mock.patch.object(contracts, "_PROJECT_ROOT", self.root):
                    with self.assertRaises(ValueError):
                        load_historical_foundry_config_set()
                target.unlink()
                specimen.unlink()
                target.write_bytes(original)

    def test_tracked_loader_rejects_ancestor_replacement_during_read(self):
        original_read = contracts.os.read
        displaced = self.root / "displaced-config"
        replaced = []

        def replace_after_first_read(fd, size):
            payload = original_read(fd, size)
            if payload and not replaced:
                replaced.append(True)
                os.rename(self.config_dir, displaced)
                shutil.copytree(displaced, self.config_dir)
            return payload

        with mock.patch.object(contracts, "_PROJECT_ROOT", self.root), mock.patch.object(
            contracts.os, "read", side_effect=replace_after_first_read
        ):
            with self.assertRaises(ValueError):
                load_historical_foundry_config_set()

    def test_tracked_loader_rejects_noncanonical_and_cross_wired_documents(self):
        policy_path = self.config_dir / "historical_foundry_replay_policy.json"
        original_policy = json.loads(policy_path.read_text(encoding="utf-8"))
        policy_path.write_bytes(json.dumps(original_policy, indent=2).encode("utf-8") + b"\n")
        with mock.patch.object(contracts, "_PROJECT_ROOT", self.root):
            with self.assertRaises(ValueError):
                load_historical_foundry_config_set()

        shutil.copyfile(
            REAL_PROJECT_ROOT / "config" / "historical_foundry_replay_policy.json",
            policy_path,
        )
        toolchain_path = self.config_dir / "historical_foundry_replay_toolchain.json"
        changed_toolchain = json.loads(toolchain_path.read_text(encoding="utf-8"))
        changed_toolchain["executor_build"]["deployed_runtime_sha256"] = "0" * 64
        toolchain_path.write_bytes(canonical_bytes(changed_toolchain))
        with mock.patch.object(contracts, "_PROJECT_ROOT", self.root):
            with self.assertRaises(ValueError):
                load_historical_foundry_config_set()

    def test_build_rejects_config_drift_before_opening_toolchain(self):
        with mock.patch.object(contracts, "_PROJECT_ROOT", self.root):
            config = load_historical_foundry_config_set()
            policy_path = self.config_dir / "historical_foundry_replay_policy.json"
            policy_path.write_bytes(policy_path.read_bytes() + b" ")
            with mock.patch(
                "scripts.bootstrap_historical_foundry_toolchain.open_reviewed_historical_toolchain",
                side_effect=AssertionError("toolchain must not open"),
            ):
                with self.assertRaises(ValueError):
                    build_validated_executor_artifact(config)

    def test_direct_artifact_construction_cannot_authorize_arbitrary_runtime(self):
        constructor_args = b""
        creation_bytecode = b"arbitrary creation bytecode"
        deployed_runtime = b"arbitrary runtime"
        immutable_references = b"{}"
        forged_result = {
            "source_tree_sha256": "1" * 64,
            "constructor_args": constructor_args,
            "constructor_args_sha256": hashlib.sha256(constructor_args).hexdigest(),
            "creation_bytecode": creation_bytecode,
            "creation_bytecode_sha256": hashlib.sha256(creation_bytecode).hexdigest(),
            "deployed_runtime": deployed_runtime,
            "deployed_runtime_sha256": hashlib.sha256(deployed_runtime).hexdigest(),
            "immutable_references": immutable_references,
            "immutable_references_sha256": hashlib.sha256(immutable_references).hexdigest(),
            "artifact_manifest_sha256": "2" * 64,
        }
        with self.assertRaises(ValueError):
            ValidatedExecutorArtifact(forged_result)

    def test_module_api_exposes_no_artifact_issuer_for_forged_runtime(self):
        constructor_args = b""
        creation_bytecode = b"forged creation bytecode"
        deployed_runtime = b"forged runtime"
        immutable_references = b"{}"
        forged_result = {
            "source_tree_sha256": "1" * 64,
            "constructor_args": constructor_args,
            "constructor_args_sha256": hashlib.sha256(constructor_args).hexdigest(),
            "creation_bytecode": creation_bytecode,
            "creation_bytecode_sha256": hashlib.sha256(creation_bytecode).hexdigest(),
            "deployed_runtime": deployed_runtime,
            "deployed_runtime_sha256": hashlib.sha256(deployed_runtime).hexdigest(),
            "immutable_references": immutable_references,
            "immutable_references_sha256": hashlib.sha256(immutable_references).hexdigest(),
            "artifact_manifest_sha256": "2" * 64,
        }
        forged_identity = {
            field_name: forged_result[field_name]
            for field_name in (
                "source_tree_sha256",
                "constructor_args_sha256",
                "creation_bytecode_sha256",
                "deployed_runtime_sha256",
                "immutable_references_sha256",
                "artifact_manifest_sha256",
            )
        }
        forged_identity.update({
            "policy_physical_sha256": "3" * 64,
            "authority_physical_sha256": "4" * 64,
            "toolchain_physical_sha256": "5" * 64,
        })
        with self.assertRaises(AttributeError):
            issuer = contracts._create_validated_executor_artifact
            artifact = issuer(forged_result, forged_identity)
            self.assertEqual(
                artifact._deployed_runtime_for_state_override(),
                deployed_runtime,
            )
        with self.assertRaises(AttributeError):
            contracts._executor_artifact_provenance_is_valid

    @classmethod
    def _valid_executor_build_result(cls):
        cached = getattr(cls, "_cached_executor_build_result", None)
        if cached is None:
            cached = dict(contracts._open_and_build_executor())
            cls._cached_executor_build_result = cached
        return dict(cached)

    def test_build_rejects_config_byte_drift_during_toolchain_invocation(self):
        valid_result = self._valid_executor_build_result()
        with mock.patch.object(contracts, "_PROJECT_ROOT", self.root):
            config = load_historical_foundry_config_set()

            def mutate_config_during_build():
                policy_path = self.config_dir / "historical_foundry_replay_policy.json"
                policy_path.write_bytes(policy_path.read_bytes() + b" ")
                return valid_result

            with mock.patch.object(
                contracts,
                "_open_and_build_executor",
                side_effect=mutate_config_during_build,
            ):
                with self.assertRaises(ValueError):
                    build_validated_executor_artifact(config)

    def test_build_rejects_config_inode_drift_during_toolchain_invocation(self):
        valid_result = self._valid_executor_build_result()
        with mock.patch.object(contracts, "_PROJECT_ROOT", self.root):
            config = load_historical_foundry_config_set()

            def replace_config_inode_during_build():
                policy_path = self.config_dir / "historical_foundry_replay_policy.json"
                replacement = self.config_dir / "replacement.json"
                replacement.write_bytes(policy_path.read_bytes())
                os.replace(replacement, policy_path)
                return valid_result

            with mock.patch.object(
                contracts,
                "_open_and_build_executor",
                side_effect=replace_config_inode_during_build,
            ):
                with self.assertRaises(ValueError):
                    build_validated_executor_artifact(config)

    def test_build_rejects_config_ancestor_drift_during_toolchain_invocation(self):
        valid_result = self._valid_executor_build_result()
        with mock.patch.object(contracts, "_PROJECT_ROOT", self.root):
            config = load_historical_foundry_config_set()

            def replace_config_ancestor_during_build():
                displaced = self.root / "displaced-config-during-build"
                os.rename(self.config_dir, displaced)
                shutil.copytree(displaced, self.config_dir)
                return valid_result

            with mock.patch.object(
                contracts,
                "_open_and_build_executor",
                side_effect=replace_config_ancestor_during_build,
            ):
                with self.assertRaises(ValueError):
                    build_validated_executor_artifact(config)

    def test_real_build_returns_sealed_hash_bound_artifact(self):
        config = load_historical_foundry_config_set()
        artifact = build_validated_executor_artifact(config)
        self.assertIsInstance(artifact, ValidatedExecutorArtifact)
        expected_identity = dict(config.toolchain.value["executor_build"])
        expected_identity.update({
            "policy_physical_sha256": config.policy.physical_sha256,
            "authority_physical_sha256": config.authority.physical_sha256,
            "toolchain_physical_sha256": config.toolchain.physical_sha256,
        })
        self.assertEqual(artifact.verified_identity, expected_identity)
        runtime = artifact._deployed_runtime_for_state_override()
        self.assertEqual(hashlib.sha256(runtime).hexdigest(), "0d6af546957603f79734024e8e53a6bf14a01c4c164a4a051295f34a847e4f22")
        self.assertNotIn(str(REAL_PROJECT_ROOT), repr(artifact))
        with self.assertRaises((TypeError, pickle.PicklingError)):
            pickle.dumps(artifact)
        with self.assertRaises(AttributeError):
            artifact._deployed_runtime = b"changed"

    def test_runtime_mutation_is_rejected_before_artifact_capability(self):
        config = load_historical_foundry_config_set()
        expected = dict(config.toolchain.value["executor_build"])
        with mock.patch.object(
            contracts,
            "_open_and_build_executor",
            return_value=dict(expected, deployed_runtime=b"changed"),
        ):
            with self.assertRaises(ValueError):
                build_validated_executor_artifact(config)


class HistoricalFoundryArithmeticTests(unittest.TestCase):
    """Known answers for the pure scanner prefilter and receipt boundary."""

    @staticmethod
    def replay_case():
        unit = 10 ** 18
        return {
            "requested_notional_usd": 1000,
            "direction": "uniswap_to_sushiswap",
            "first_reserves": (4000 * unit, 1000 * unit),
            "second_reserves": (1000 * unit, 1000 * unit),
            "eth_usd_answer": 2000 * 10 ** 8,
            "feed_decimals": 8,
            "parent_base_fee": 100,
            "parent_gas_used": 15,
            "parent_gas_limit": 20,
        }

    @staticmethod
    def zero_rate_case():
        unit = 10 ** 18
        return {
            "requested_notional_usd": 1000,
            "direction": "uniswap_to_sushiswap",
            "first_reserves": (10 ** 24, 10 ** 24),
            "second_reserves": (10 ** 24, 10 ** 24 + 6029 * unit),
            "eth_usd_answer": 2000 * 10 ** 8,
            "feed_decimals": 8,
            "parent_base_fee": 0,
            "parent_gas_used": 10,
            "parent_gas_limit": 20,
        }

    def test_amount_weth_and_v2_quotes_use_integer_floors_and_input_order(self):
        self.assertEqual(amount_weth_in_wei(1, 3, 0), 333333333333333333)
        self.assertEqual(quote_v2_exact_in(1000, 100000, 100000), 987)
        self.assertEqual(quote_v2_exact_in(100, 1000, 2000), 181)
        self.assertEqual(quote_v2_exact_in(100, 2000, 1000), 47)
        self.assertEqual(quote_v2_exact_in(0, 1000, 2000), 0)

    def test_public_next_base_fee_wrapper_is_keyword_only_and_exact(self):
        signature = inspect.signature(next_historical_base_fee)
        self.assertEqual(
            list(signature.parameters),
            ["parent_base_fee", "parent_gas_used", "parent_gas_limit"],
        )
        self.assertTrue(
            all(
                parameter.kind is inspect.Parameter.KEYWORD_ONLY
                for parameter in signature.parameters.values()
            )
        )
        self.assertEqual(
            next_historical_base_fee(
                parent_base_fee=100,
                parent_gas_used=10,
                parent_gas_limit=20,
            ),
            100,
        )
        self.assertEqual(
            next_historical_base_fee(
                parent_base_fee=100,
                parent_gas_used=11,
                parent_gas_limit=20,
            ),
            101,
        )
        self.assertEqual(
            next_historical_base_fee(
                parent_base_fee=100,
                parent_gas_used=9,
                parent_gas_limit=20,
            ),
            99,
        )
        self.assertEqual(
            next_historical_base_fee(
                parent_base_fee=0,
                parent_gas_used=11,
                parent_gas_limit=20,
            ),
            1,
        )

    def test_public_next_base_fee_wrapper_preserves_closed_validation(self):
        invalid = (
            {"parent_base_fee": True, "parent_gas_used": 1, "parent_gas_limit": 2},
            {"parent_base_fee": -1, "parent_gas_used": 1, "parent_gas_limit": 2},
            {"parent_base_fee": 1, "parent_gas_used": True, "parent_gas_limit": 2},
            {"parent_base_fee": 1, "parent_gas_used": -1, "parent_gas_limit": 2},
            {"parent_base_fee": 1, "parent_gas_used": 0, "parent_gas_limit": 0},
            {"parent_base_fee": 1, "parent_gas_used": 1, "parent_gas_limit": 1},
            {"parent_base_fee": 1, "parent_gas_used": 3, "parent_gas_limit": 2},
        )
        for arguments in invalid:
            with self.subTest(arguments=arguments):
                with self.assertRaises(ValueError):
                    next_historical_base_fee(**arguments)
        with self.assertRaises(TypeError):
            next_historical_base_fee(1, 1, 2)

    def test_prefilter_projects_exact_usd_and_eip1559_child_fee(self):
        row = project_historical_prefilter_math(
            **self.replay_case(), acceptance_mev_bps="0"
        )
        self.assertEqual(row["amount_weth_in_wei"], 500000000000000000)
        self.assertEqual(row["first_amount_out_raw"], 1993006486266596101)
        self.assertEqual(row["second_amount_out_raw"], 1983087018433099764)
        self.assertEqual(row["gross_profit_weth_wei"], 1483087018433099764)
        self.assertEqual(row["gross_edge_usd"], "2966.174036866199528")
        self.assertEqual(row["child_base_fee_wei"], 106)
        self.assertEqual(row["prefilter_gas_cost_usd"], "0.000000004452")
        self.assertEqual(
            row["prefilter_policy_net_upper_bound_usd"],
            "2966.174036861747528",
        )
        self.assertEqual(row["decision"], "replay_required")
        self.assertIsNone(row["reason"])

    def test_prefilter_eip1559_increase_decrease_and_equality_boundaries(self):
        common = self.replay_case()
        common["parent_base_fee"] = 100
        common["parent_gas_limit"] = 20
        for gas_used, expected_child in ((9, 99), (10, 100), (11, 101)):
            with self.subTest(gas_used=gas_used):
                case = dict(common, parent_gas_used=gas_used)
                row = project_historical_prefilter_math(
                    **case, acceptance_mev_bps="0"
                )
                self.assertEqual(row["child_base_fee_wei"], expected_child)

    def test_prefilter_excludes_zero_output_without_floating_point_rounding(self):
        case = self.replay_case()
        case["requested_notional_usd"] = 0
        row = project_historical_prefilter_math(
            **case, acceptance_mev_bps="0"
        )
        self.assertEqual(row["amount_weth_in_wei"], 0)
        self.assertEqual(row["decision"], "safe_excluded")
        self.assertEqual(row["reason"], "first_leg_zero_output")
        self.assertEqual(row["gross_edge_usd"], "0")

    def test_safe_exclusion_uses_policy_zero_mev_without_hidden_ten_bps(self):
        row = project_historical_prefilter_math(
            **self.zero_rate_case(), acceptance_mev_bps="0"
        )
        self.assertEqual(row["gross_edge_usd"], "0.00088475561922")
        self.assertEqual(row["decision"], "replay_required")
        excluded = project_historical_prefilter_math(
            **self.zero_rate_case(), acceptance_mev_bps="10"
        )
        self.assertEqual(excluded["decision"], "safe_excluded")
        self.assertEqual(
            excluded["reason"], "nonpositive_prefilter_policy_net_upper_bound"
        )

    def test_receipt_uses_measured_delta_and_charged_p50_gas_with_stress_cells(self):
        prefilter = project_historical_prefilter_math(
            **self.replay_case(), acceptance_mev_bps="10"
        )
        row = project_historical_receipt_economics(
            prefilter_projection=prefilter,
            actual_intermediate_uni_raw=1993006486266596101,
            actual_final_weth_raw=1983087018433099764,
            receipt_gas_used=50000,
            receipt_effective_gas_price=109,
            p50_priority_fee_wei=3,
            p90_priority_fee_wei=9,
            acceptance_mev_bps="10",
            stress_mev_bps=("25", "50"),
        )
        self.assertEqual(row["max_fee_per_gas_wei"], 215)
        self.assertEqual(row["stress_max_fee_per_gas_wei"], 221)
        self.assertEqual(row["charged_gas_cost_usd"], "0.0000000109")
        self.assertEqual(row["stress_gas_cost_usd"], "0.0000000115")
        self.assertEqual(row["policy_net_edge_usd"], "2965.174036855299528")
        self.assertEqual(row["stress_25_net_usd"], "2963.674036854699528")
        self.assertEqual(row["stress_50_net_usd"], "2961.174036854699528")
        self.assertTrue(row["policy_net_positive"])
        self.assertTrue(row["stress_robust"])

    def test_receipt_rejects_delta_or_effective_price_mismatch(self):
        prefilter = project_historical_prefilter_math(
            **self.replay_case(), acceptance_mev_bps="10"
        )
        common = {
            "prefilter_projection": prefilter,
            "actual_final_weth_raw": 1983087018433099764,
            "receipt_gas_used": 50000,
            "receipt_effective_gas_price": 109,
            "p50_priority_fee_wei": 3,
            "p90_priority_fee_wei": 9,
            "acceptance_mev_bps": "10",
            "stress_mev_bps": ("25", "50"),
        }
        with self.assertRaises(ValueError):
            project_historical_receipt_economics(
                **common, actual_intermediate_uni_raw=1993006486266596100
            )
        with self.assertRaises(ValueError):
            project_historical_receipt_economics(
                **dict(common, receipt_effective_gas_price=110),
                actual_intermediate_uni_raw=1993006486266596101,
            )

    def test_receipt_rejects_exact_zero_policy_net_and_accepts_one_positive_unit(self):
        prefilter = project_historical_prefilter_math(
            **self.replay_case(), acceptance_mev_bps="10"
        )
        common = {
            "prefilter_projection": prefilter,
            "actual_intermediate_uni_raw": 1993006486266596101,
            "receipt_gas_used": 0,
            "receipt_effective_gas_price": 106,
            "p50_priority_fee_wei": 0,
            "p90_priority_fee_wei": 0,
            "acceptance_mev_bps": "10",
            "stress_mev_bps": ("25", "50"),
        }
        exact_zero = project_historical_receipt_economics(
            **common, actual_final_weth_raw=500500000000000000
        )
        self.assertEqual(exact_zero["policy_net_edge_usd"], "0")
        self.assertFalse(exact_zero["policy_net_positive"])
        minimum_positive = project_historical_receipt_economics(
            **common, actual_final_weth_raw=500500000000000001
        )
        self.assertEqual(minimum_positive["policy_net_edge_usd"], "0.000000000000002")
        self.assertTrue(minimum_positive["policy_net_positive"])


if __name__ == "__main__":
    unittest.main()
