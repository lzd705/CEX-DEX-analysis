"""Closed pure schemas for historical Foundry replay authorities.

This module intentionally has no filesystem, environment, network, or process
boundary.  Task 4 owns the descriptor-safe tracked loaders; these validators
only accept values (or explicit known-answer bytes) and return detached values.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from fractions import Fraction
import hashlib
import json
import os
from pathlib import Path
import re
import stat
from types import MappingProxyType
from typing import Any, Dict, Mapping, Optional, Tuple


_HASH = re.compile(r"[0-9a-f]{64}\Z", re.ASCII)
_ADDRESS = re.compile(r"0x[0-9a-f]{40}\Z", re.ASCII)
_SELECTOR = re.compile(r"0x[0-9a-f]{8}\Z", re.ASCII)
_DECIMAL = re.compile(r"(?:0|[1-9][0-9]*)(?:\.[0-9]*[1-9])?\Z", re.ASCII)

_POLICY_FIELDS = frozenset((
    "schema", "chain_id", "anchor_tag", "lookback_seconds", "selection_rule",
    "requested_notionals_usd", "directions", "max_eth_usd_age_seconds",
    "state_basis", "execution", "fees", "profitability",
    "closed_revert_matrix", "authority_sha256", "toolchain_sha256",
))
_EXECUTION_FIELDS = frozenset((
    "model", "synthetic_timestamp_offset_seconds", "calldata_deadline_offset_seconds",
    "router_min_output_raw", "transaction_type", "transaction_gas_limit",
    "access_list", "sender_nonce",
))
_MIN_OUTPUT_FIELDS = frozenset(("first_leg", "second_leg"))
_FEE_FIELDS = frozenset((
    "next_base_fee_rule", "acceptance_tip_percentile", "stress_tip_percentile",
    "max_fee_multiplier", "acceptance_mev_bps", "stress_mev_bps",
))
_PROFITABILITY_FIELDS = frozenset((
    "winner_comparison", "exact_zero_result", "serialization",
))
_REVERT_FIELDS = frozenset((
    "prefilter_reason", "leg", "revert_selector", "revert_data_sha256",
    "terminal_class",
))
_AUTHORITY_FIELDS = frozenset((
    "schema", "chain_id", "tokens", "venues", "price_feed", "sender",
    "executor", "v2_formula", "state_override_layout",
))
_TOKEN_FIELDS = frozenset((
    "role", "address", "decimals", "balance_descriptor", "allowance_descriptor",
))
_DESCRIPTOR_FIELDS = frozenset(("kind", "slot", "key_order", "getter_selector"))
_VENUE_FIELDS = frozenset((
    "venue_id", "router_address", "factory_address", "factory_selector",
    "weth_selector", "pair_getter_selector", "pair_derivation",
))
_PRICE_FEED_FIELDS = frozenset((
    "proxy_address", "description", "decimals", "latest_round_selector",
    "aggregator_selector", "phase_selector",
))
_SENDER_FIELDS = frozenset(("address", "nonce"))
_EXECUTOR_FIELDS = frozenset((
    "address", "prior_code", "prior_nonce", "prior_token_balances",
    "prior_allowances",
))
_V2_FORMULA_FIELDS = frozenset(("fee_numerator", "fee_denominator"))
_OVERRIDE_LAYOUT_FIELDS = frozenset((
    "account_roles", "storage_roles", "weth_backing_rule", "allowance_matrix_rule",
))
_TOOLCHAIN_FIELDS = frozenset((
    "schema", "foundry_release", "binaries", "solc", "forge_std",
    "compiler_settings", "executor_build",
))
_FOUNDRY_RELEASE_FIELDS = frozenset((
    "version", "archive_url", "archive_sha256", "checksum_url", "checksum_sha256",
    "provenance_url", "provenance_sha256", "sigstore_issuer",
    "sigstore_identity", "release_commit",
))
_BINARY_FIELDS = frozenset(("name", "version", "sha256"))
_SOLC_FIELDS = frozenset(("version", "artifact_url", "artifact_sha256"))
_FORGE_STD_FIELDS = frozenset(("repository_url", "version", "commit"))
_COMPILER_SETTINGS_FIELDS = frozenset((
    "evm_version", "fork_hardfork", "optimizer_enabled", "optimizer_runs",
    "via_ir", "bytecode_hash", "cbor_metadata", "append_cbor",
))
_EXECUTOR_BUILD_FIELDS = frozenset((
    "source_tree_sha256", "constructor_args_sha256", "creation_bytecode_sha256",
    "deployed_runtime_sha256", "immutable_references_sha256",
    "artifact_manifest_sha256",
))

_EXPECTED_TOKENS = (
    ("uni", "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984", 4, 3),
    ("weth", "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2", 3, 4),
)
_EXPECTED_VENUES = (
    ("uniswap_v2", "0x7a250d5630b4cf539739df2c5dacb4c659f2488d",
     "0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f"),
    ("sushiswap_v2", "0xd9e1ce17f2641f24ae83637ab66a2cca9c378b9f",
     "0xc0aee478e3658e2610c5f7a4a2e1777ce9e4f2ac"),
)
_EXPECTED_REVERTS = (
    (
        "first_leg_zero_output", "first_leg", "0x08c379a0",
        "6798eb314455c46925e230068a2e4849cf2340aefa7480b4aece1cdc6ae36ba7",
    ),
    (
        "second_leg_zero_liquidity", "second_leg", "0x08c379a0",
        "9de19b1bd02b49383b079e33eb28592b7125d02f86cad8e24358a74830d1fe0b",
    ),
)

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_TRACKED_CONFIG_NAMES = (
    "historical_foundry_replay_policy.json",
    "historical_foundry_replay_authority.json",
    "historical_foundry_replay_toolchain.json",
)
_MAX_TRACKED_CONFIG_BYTES = 1024 * 1024
_ARTIFACT_RESULT_FIELDS = frozenset((
    "source_tree_sha256", "constructor_args", "constructor_args_sha256",
    "creation_bytecode", "creation_bytecode_sha256", "deployed_runtime",
    "deployed_runtime_sha256", "immutable_references",
    "immutable_references_sha256", "artifact_manifest_sha256",
))
_ARTIFACT_HASH_FIELDS = (
    "source_tree_sha256", "constructor_args_sha256",
    "creation_bytecode_sha256", "deployed_runtime_sha256",
    "immutable_references_sha256", "artifact_manifest_sha256",
)
_CONFIG_PHYSICAL_HASH_FIELDS = (
    "policy_physical_sha256", "authority_physical_sha256",
    "toolchain_physical_sha256",
)


@dataclass(frozen=True)
class LoadedHistoricalConfig:
    """Exact physical metadata kept outside the closed schema payload."""

    value: Mapping[str, Any]
    physical_bytes: bytes = field(repr=False)
    physical_sha256: str
    policy_id: Optional[str] = None

    def __post_init__(self) -> None:
        if not isinstance(self.physical_bytes, bytes):
            raise ValueError("historical config physical bytes are invalid")
        if not _is_hash(self.physical_sha256):
            raise ValueError("historical config physical hash is invalid")
        if hashlib.sha256(self.physical_bytes).hexdigest() != self.physical_sha256:
            raise ValueError("historical config physical hash does not match bytes")
        physical_value = _parse_canonical_json_bytes(self.physical_bytes)
        if _copy_value(self.value) != physical_value:
            raise ValueError("historical config value does not match physical bytes")
        schema = physical_value.get("schema")
        if schema == "historical_foundry_replay_authority/v1":
            validated = validate_historical_foundry_authority(physical_value)
        elif schema == "historical_foundry_replay_toolchain/v1":
            validated = validate_historical_foundry_toolchain(physical_value)
        elif schema == "historical_foundry_replay_policy/v1":
            _validate_policy_shape(physical_value)
            validated = _copy_value(physical_value)
        else:
            raise ValueError("historical config schema is invalid")
        if schema == "historical_foundry_replay_policy/v1":
            derived_policy_id = policy_id_from_bytes(self.physical_bytes)
            if self.policy_id is not None and self.policy_id != derived_policy_id:
                raise ValueError("historical config policy id does not match bytes")
            object.__setattr__(self, "policy_id", derived_policy_id)
        elif self.policy_id is not None:
            raise ValueError("only a policy config may carry a policy id")
        object.__setattr__(self, "value", _freeze(validated))


class HistoricalFoundryConfigSet:
    """Non-serializable mutually bound tracked-config capability."""

    __slots__ = ("policy", "authority", "toolchain")

    def __init__(
        self,
        policy: LoadedHistoricalConfig,
        authority: LoadedHistoricalConfig,
        toolchain: LoadedHistoricalConfig,
    ) -> None:
        if not all(
            isinstance(item, LoadedHistoricalConfig)
            for item in (policy, authority, toolchain)
        ):
            raise ValueError("historical config set members are invalid")
        if (
            policy.value.get("schema") != "historical_foundry_replay_policy/v1"
            or authority.value.get("schema")
            != "historical_foundry_replay_authority/v1"
            or toolchain.value.get("schema")
            != "historical_foundry_replay_toolchain/v1"
            or policy.value.get("authority_sha256") != authority.physical_sha256
            or policy.value.get("toolchain_sha256") != toolchain.physical_sha256
        ):
            raise ValueError("historical config set physical binding is invalid")
        validate_historical_foundry_policy(
            policy.physical_bytes,
            authority_bytes=authority.physical_bytes,
            toolchain_bytes=toolchain.physical_bytes,
        )
        object.__setattr__(self, "policy", policy)
        object.__setattr__(self, "authority", authority)
        object.__setattr__(self, "toolchain", toolchain)

    def __setattr__(self, _name: str, _value: Any) -> None:
        raise AttributeError("HistoricalFoundryConfigSet is immutable")

    def __repr__(self) -> str:
        return "HistoricalFoundryConfigSet(<sealed>)"

    def __reduce__(self) -> Any:
        raise TypeError("HistoricalFoundryConfigSet is not serializable")


def _initialize_executor_artifact_authority():
    provenance = object()

    class ValidatedExecutorArtifact:
        """Hash-bound in-memory executor bytes with no path or caller input."""

        __slots__ = (
            "_constructor_args", "_creation_bytecode", "_deployed_runtime",
            "_immutable_references", "_verified_identity",
        )

        def __init__(
            self,
            result: Mapping[str, Any],
            *,
            _provenance: object = None,
            _reviewed_identity: Optional[Mapping[str, str]] = None,
        ) -> None:
            if _provenance is not provenance:
                raise ValueError("executor artifact provenance is invalid")
            if set(result) != _ARTIFACT_RESULT_FIELDS:
                raise ValueError("executor artifact result schema is invalid")
            if (
                not isinstance(_reviewed_identity, Mapping)
                or set(_reviewed_identity)
                != set(_ARTIFACT_HASH_FIELDS + _CONFIG_PHYSICAL_HASH_FIELDS)
                or any(
                    not _is_hash(value)
                    for value in _reviewed_identity.values()
                )
            ):
                raise ValueError("executor artifact reviewed identity is invalid")
            for field_name in (
                "constructor_args", "creation_bytecode", "deployed_runtime",
                "immutable_references",
            ):
                if not isinstance(result[field_name], bytes):
                    raise ValueError("executor artifact bytes are invalid")
            for bytes_field, hash_field in (
                ("constructor_args", "constructor_args_sha256"),
                ("creation_bytecode", "creation_bytecode_sha256"),
                ("deployed_runtime", "deployed_runtime_sha256"),
                ("immutable_references", "immutable_references_sha256"),
            ):
                if (
                    hashlib.sha256(result[bytes_field]).hexdigest()
                    != result.get(hash_field)
                ):
                    raise ValueError("executor artifact byte identity is invalid")
            for field_name in _ARTIFACT_HASH_FIELDS:
                if result.get(field_name) != _reviewed_identity[field_name]:
                    raise ValueError(
                        "executor artifact does not match reviewed identity"
                    )
            object.__setattr__(
                self, "_constructor_args", bytes(result["constructor_args"])
            )
            object.__setattr__(
                self, "_creation_bytecode", bytes(result["creation_bytecode"])
            )
            object.__setattr__(
                self, "_deployed_runtime", bytes(result["deployed_runtime"])
            )
            object.__setattr__(
                self,
                "_immutable_references",
                bytes(result["immutable_references"]),
            )
            object.__setattr__(
                self,
                "_verified_identity",
                MappingProxyType(dict(_reviewed_identity)),
            )

        def __setattr__(self, _name: str, _value: Any) -> None:
            raise AttributeError("ValidatedExecutorArtifact is immutable")

        def __repr__(self) -> str:
            return "ValidatedExecutorArtifact(<sealed>)"

        def __reduce__(self) -> Any:
            raise TypeError("ValidatedExecutorArtifact is not serializable")

        @property
        def verified_identity(self) -> Mapping[str, str]:
            return dict(self._verified_identity)

        def _deployed_runtime_for_state_override(self) -> bytes:
            return bytes(self._deployed_runtime)

    def build_validated_executor_artifact(
        config: "HistoricalFoundryConfigSet",
    ) -> "ValidatedExecutorArtifact":
        """Clean-build and validate the fixed executor with the sealed toolchain."""
        if not isinstance(config, HistoricalFoundryConfigSet):
            raise ValueError("historical config capability is invalid")
        pre_build_physical, pre_build_inventory_identity = (
            _read_tracked_config_inventory_with_identity()
        )
        pre_build_config = _config_set_from_physical(pre_build_physical)
        _require_same_config_bytes(config, pre_build_config)
        result = _open_and_build_executor()
        post_build_physical, post_build_inventory_identity = (
            _read_tracked_config_inventory_with_identity()
        )
        post_build_config = _config_set_from_physical(post_build_physical)
        if pre_build_inventory_identity != post_build_inventory_identity:
            raise ValueError("historical config inventory changed during build")
        _require_same_config_bytes(config, post_build_config)
        _require_same_config_bytes(pre_build_config, post_build_config)
        if not isinstance(result, Mapping) or set(result) != _ARTIFACT_RESULT_FIELDS:
            raise ValueError("executor artifact result schema is invalid")
        for bytes_field, hash_field in (
            ("constructor_args", "constructor_args_sha256"),
            ("creation_bytecode", "creation_bytecode_sha256"),
            ("deployed_runtime", "deployed_runtime_sha256"),
            ("immutable_references", "immutable_references_sha256"),
        ):
            payload = result.get(bytes_field)
            if (
                not isinstance(payload, bytes)
                or hashlib.sha256(payload).hexdigest() != result.get(hash_field)
            ):
                raise ValueError("executor artifact byte identity is invalid")
        expected_build = config.toolchain.value["executor_build"]
        for field_name in _ARTIFACT_HASH_FIELDS:
            if result.get(field_name) != expected_build[field_name]:
                raise ValueError("executor artifact does not match toolchain authority")
        reviewed_identity = dict(expected_build)
        reviewed_identity.update({
            "policy_physical_sha256": config.policy.physical_sha256,
            "authority_physical_sha256": config.authority.physical_sha256,
            "toolchain_physical_sha256": config.toolchain.physical_sha256,
        })
        return ValidatedExecutorArtifact(
            result,
            _provenance=provenance,
            _reviewed_identity=reviewed_identity,
        )

    return ValidatedExecutorArtifact, build_validated_executor_artifact


(
    ValidatedExecutorArtifact,
    build_validated_executor_artifact,
) = _initialize_executor_artifact_authority()
del _initialize_executor_artifact_authority


def load_historical_foundry_policy() -> LoadedHistoricalConfig:
    """Load the tracked policy only after validating the complete bound set."""
    return _load_historical_foundry_config_set().policy


def load_historical_foundry_authority() -> LoadedHistoricalConfig:
    """Load the tracked authority only after validating the complete bound set."""
    return _load_historical_foundry_config_set().authority


def load_historical_foundry_toolchain() -> LoadedHistoricalConfig:
    """Load the tracked toolchain only after validating the complete bound set."""
    return _load_historical_foundry_config_set().toolchain


def load_historical_foundry_config_set() -> "HistoricalFoundryConfigSet":
    """Load all exact tracked configs through one descriptor-stable inventory."""
    return _load_historical_foundry_config_set()


def _open_and_build_executor() -> Mapping[str, Any]:
    from scripts.bootstrap_historical_foundry_toolchain import (
        open_reviewed_historical_toolchain,
    )

    with open_reviewed_historical_toolchain() as capability:
        return capability._build_executor_artifact()


def _load_historical_foundry_config_set() -> HistoricalFoundryConfigSet:
    return _config_set_from_physical(_read_tracked_config_inventory())


def _config_set_from_physical(
    physical: Mapping[str, bytes],
) -> HistoricalFoundryConfigSet:
    policy_bytes = physical[_TRACKED_CONFIG_NAMES[0]]
    authority_bytes = physical[_TRACKED_CONFIG_NAMES[1]]
    toolchain_bytes = physical[_TRACKED_CONFIG_NAMES[2]]
    authority_value = validate_historical_foundry_authority(authority_bytes)
    toolchain_value = validate_historical_foundry_toolchain(toolchain_bytes)
    policy_value = validate_historical_foundry_policy(
        policy_bytes,
        authority_bytes=authority_bytes,
        toolchain_bytes=toolchain_bytes,
    )
    return HistoricalFoundryConfigSet(
        LoadedHistoricalConfig(
            value=policy_value,
            physical_bytes=policy_bytes,
            physical_sha256=hashlib.sha256(policy_bytes).hexdigest(),
        ),
        LoadedHistoricalConfig(
            value=authority_value,
            physical_bytes=authority_bytes,
            physical_sha256=hashlib.sha256(authority_bytes).hexdigest(),
        ),
        LoadedHistoricalConfig(
            value=toolchain_value,
            physical_bytes=toolchain_bytes,
            physical_sha256=hashlib.sha256(toolchain_bytes).hexdigest(),
        ),
    )


def _require_same_config_bytes(
    expected: HistoricalFoundryConfigSet,
    observed: HistoricalFoundryConfigSet,
) -> None:
    for expected_member, observed_member in (
        (expected.policy, observed.policy),
        (expected.authority, observed.authority),
        (expected.toolchain, observed.toolchain),
    ):
        if (
            expected_member.physical_sha256 != observed_member.physical_sha256
            or expected_member.physical_bytes != observed_member.physical_bytes
        ):
            raise ValueError("historical config physical bytes changed")


def _config_file_flags() -> int:
    nofollow = getattr(os, "O_NOFOLLOW", None)
    directory = getattr(os, "O_DIRECTORY", None)
    if nofollow is None or directory is None:
        raise ValueError("historical config no-follow support is unavailable")
    return os.O_RDONLY | nofollow | getattr(os, "O_CLOEXEC", 0)


def _config_metadata(metadata: os.stat_result) -> Tuple[int, ...]:
    return (
        metadata.st_dev, metadata.st_ino, metadata.st_size, metadata.st_mode,
        metadata.st_uid, metadata.st_gid, metadata.st_nlink,
        getattr(metadata, "st_mtime_ns", int(metadata.st_mtime * 1000000000)),
        getattr(metadata, "st_ctime_ns", int(metadata.st_ctime * 1000000000)),
    )


def _read_config_descriptor(fd: int) -> bytes:
    chunks = []
    total = 0
    os.lseek(fd, 0, os.SEEK_SET)
    while True:
        chunk = os.read(fd, min(65536, _MAX_TRACKED_CONFIG_BYTES + 1 - total))
        if not chunk:
            return b"".join(chunks)
        chunks.append(chunk)
        total += len(chunk)
        if total > _MAX_TRACKED_CONFIG_BYTES:
            raise ValueError("historical config member is too large")


def _read_tracked_config_inventory() -> Dict[str, bytes]:
    return _read_tracked_config_inventory_with_identity()[0]


def _read_tracked_config_inventory_with_identity(
) -> Tuple[Dict[str, bytes], Tuple[Any, ...]]:
    root_fd = None
    config_fd = None
    opened = []
    try:
        root_path_metadata = os.stat(str(_PROJECT_ROOT), follow_symlinks=False)
        root_fd = os.open(str(_PROJECT_ROOT), _config_file_flags() | os.O_DIRECTORY)
        root_metadata = os.fstat(root_fd)
        if (
            not stat.S_ISDIR(root_metadata.st_mode)
            or _config_metadata(root_path_metadata) != _config_metadata(root_metadata)
        ):
            raise ValueError("historical config root is unsafe")
        config_path_metadata = os.stat(
            "config", dir_fd=root_fd, follow_symlinks=False
        )
        config_fd = os.open(
            "config", _config_file_flags() | os.O_DIRECTORY, dir_fd=root_fd
        )
        config_metadata = os.fstat(config_fd)
        if (
            not stat.S_ISDIR(config_metadata.st_mode)
            or config_metadata.st_uid != os.getuid()
            or stat.S_IMODE(config_metadata.st_mode) & 0o022
            or _config_metadata(config_path_metadata) != _config_metadata(config_metadata)
        ):
            raise ValueError("historical config directory is unsafe")
        result = {}
        member_identities = []
        for name in _TRACKED_CONFIG_NAMES:
            path_metadata = os.stat(name, dir_fd=config_fd, follow_symlinks=False)
            fd = os.open(name, _config_file_flags(), dir_fd=config_fd)
            opened.append(fd)
            descriptor = os.fstat(fd)
            metadata = _config_metadata(descriptor)
            if (
                not stat.S_ISREG(descriptor.st_mode)
                or descriptor.st_nlink != 1
                or descriptor.st_uid != os.getuid()
                or stat.S_IMODE(descriptor.st_mode) & 0o022
                or _config_metadata(path_metadata) != metadata
            ):
                raise ValueError("historical config member is unsafe")
            first = _read_config_descriptor(fd)
            second = _read_config_descriptor(fd)
            if first != second:
                raise ValueError("historical config member changed")
            if (
                _config_metadata(os.fstat(fd)) != metadata
                or _config_metadata(
                    os.stat(name, dir_fd=config_fd, follow_symlinks=False)
                ) != metadata
            ):
                raise ValueError("historical config member changed")
            result[name] = first
            member_identities.append((name, metadata))
        if (
            _config_metadata(os.fstat(config_fd)) != _config_metadata(config_metadata)
            or _config_metadata(
                os.stat("config", dir_fd=root_fd, follow_symlinks=False)
            ) != _config_metadata(config_metadata)
            or _config_metadata(os.fstat(root_fd)) != _config_metadata(root_metadata)
            or _config_metadata(
                os.stat(str(_PROJECT_ROOT), follow_symlinks=False)
            ) != _config_metadata(root_metadata)
        ):
            raise ValueError("historical config ancestry changed")
        ancestry_identity = (
            (
                root_metadata.st_dev, root_metadata.st_ino,
                root_metadata.st_mode, root_metadata.st_uid, root_metadata.st_gid,
            ),
            (
                config_metadata.st_dev, config_metadata.st_ino,
                config_metadata.st_mode, config_metadata.st_uid,
                config_metadata.st_gid,
            ),
            tuple(member_identities),
        )
        return result, ancestry_identity
    except (OSError, ValueError) as error:
        if isinstance(error, ValueError):
            raise
        raise ValueError("historical config inventory is unavailable") from error
    finally:
        for fd in reversed(opened):
            os.close(fd)
        if config_fd is not None:
            os.close(config_fd)
        if root_fd is not None:
            os.close(root_fd)


def policy_id_from_bytes(policy_bytes: bytes) -> str:
    """Return the typed identifier for exact canonical policy bytes."""
    _parse_canonical_json_bytes(policy_bytes)
    return "policy:" + hashlib.sha256(policy_bytes).hexdigest()


def amount_weth_in_wei(
    requested_notional_usd: int,
    eth_usd_answer: int,
    feed_decimals: int,
) -> int:
    """Return the exact floored WETH input for an integer USD notional."""
    notional = _nonnegative_int(requested_notional_usd, "requested notional")
    answer = _positive_int(eth_usd_answer, "ETH/USD answer")
    decimals = _nonnegative_int(feed_decimals, "feed decimals")
    return notional * (10 ** (18 + decimals)) // answer


def quote_v2_exact_in(
    amount_in: int,
    reserve_in: int,
    reserve_out: int,
) -> int:
    """Return the Uniswap V2 exact-input quote using its integer floor."""
    input_amount = _nonnegative_int(amount_in, "amount in")
    input_reserve = _nonnegative_int(reserve_in, "input reserve")
    output_reserve = _nonnegative_int(reserve_out, "output reserve")
    if input_amount == 0 or input_reserve == 0 or output_reserve == 0:
        return 0
    input_with_fee = input_amount * 997
    return input_with_fee * output_reserve // (
        input_reserve * 1000 + input_with_fee
    )


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
) -> Dict[str, Any]:
    """Project conservative reserve-only replay eligibility without receipts."""
    if direction not in (
        "uniswap_to_sushiswap",
        "sushiswap_to_uniswap",
    ):
        raise ValueError("direction is invalid")
    first_uni, first_weth = _reserve_pair(first_reserves, "first reserves")
    second_uni, second_weth = _reserve_pair(second_reserves, "second reserves")
    notional = _nonnegative_int(requested_notional_usd, "requested notional")
    answer = _positive_int(eth_usd_answer, "ETH/USD answer")
    decimals = _nonnegative_int(feed_decimals, "feed decimals")
    amount_in = amount_weth_in_wei(notional, answer, decimals)
    child_base_fee = _next_base_fee(
        parent_base_fee, parent_gas_used, parent_gas_limit
    )
    first_amount_out = quote_v2_exact_in(amount_in, first_weth, first_uni)
    second_amount_out = quote_v2_exact_in(
        first_amount_out, second_uni, second_weth
    )
    gross_profit = second_amount_out - amount_in
    mev_bps = _decimal_fraction(acceptance_mev_bps, "acceptance MEV")
    price_scale = 10 ** (18 + decimals)
    gross_edge_usd = Fraction(gross_profit * answer, price_scale)
    prefilter_gas_cost_usd = Fraction(
        21000 * child_base_fee * answer, price_scale
    )
    mev_buffer_usd = Fraction(notional, 1) * mev_bps / 10000
    policy_net_upper_bound = (
        gross_edge_usd - prefilter_gas_cost_usd - mev_buffer_usd
    )
    if first_amount_out == 0:
        decision, reason = "safe_excluded", "first_leg_zero_output"
    elif second_amount_out == 0:
        decision, reason = "safe_excluded", "second_leg_zero_output"
    elif gross_profit <= 0:
        decision, reason = "safe_excluded", "nonpositive_gross_weth"
    elif policy_net_upper_bound <= 0:
        decision, reason = (
            "safe_excluded",
            "nonpositive_prefilter_policy_net_upper_bound",
        )
    else:
        decision, reason = "replay_required", None
    return {
        "direction": direction,
        "requested_notional_usd": notional,
        "eth_usd_answer": answer,
        "feed_decimals": decimals,
        "acceptance_mev_bps": _canonical_fraction(mev_bps),
        "amount_weth_in_wei": amount_in,
        "first_amount_out_raw": first_amount_out,
        "second_amount_out_raw": second_amount_out,
        "gross_profit_weth_wei": gross_profit,
        "gross_edge_usd": _canonical_fraction(gross_edge_usd),
        "child_base_fee_wei": child_base_fee,
        "prefilter_gas_cost_usd": _canonical_fraction(prefilter_gas_cost_usd),
        "prefilter_mev_buffer_usd": _canonical_fraction(mev_buffer_usd),
        "prefilter_policy_net_upper_bound_usd": _canonical_fraction(
            policy_net_upper_bound
        ),
        "decision": decision,
        "reason": reason,
    }


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
) -> Dict[str, Any]:
    """Derive receipt economics without re-quoting reserves or child base fee."""
    projection = _prefilter_receipt_inputs(prefilter_projection)
    actual_intermediate = _nonnegative_int(
        actual_intermediate_uni_raw, "actual intermediate UNI"
    )
    if actual_intermediate != projection["first_amount_out_raw"]:
        raise ValueError("actual intermediate UNI does not match prefilter")
    actual_final = _nonnegative_int(actual_final_weth_raw, "actual final WETH")
    gas_used = _nonnegative_int(receipt_gas_used, "receipt gas used")
    effective_gas_price = _nonnegative_int(
        receipt_effective_gas_price, "receipt effective gas price"
    )
    p50_tip = _nonnegative_int(p50_priority_fee_wei, "p50 priority fee")
    p90_tip = _nonnegative_int(p90_priority_fee_wei, "p90 priority fee")
    if effective_gas_price != projection["child_base_fee_wei"] + p50_tip:
        raise ValueError("receipt effective gas price does not match p50 charge")
    acceptance_bps = _decimal_fraction(acceptance_mev_bps, "acceptance MEV")
    if _canonical_fraction(acceptance_bps) != projection["acceptance_mev_bps"]:
        raise ValueError("acceptance MEV does not match prefilter")
    stress_bps = _stress_bps(stress_mev_bps)
    price_scale = 10 ** (18 + projection["feed_decimals"])
    price_answer = projection["eth_usd_answer"]
    gross_profit = actual_final - projection["amount_weth_in_wei"]
    gross_edge_usd = Fraction(gross_profit * price_answer, price_scale)
    charged_gas_cost_wei = gas_used * effective_gas_price
    stress_gas_cost_wei = gas_used * (projection["child_base_fee_wei"] + p90_tip)
    charged_gas_cost_usd = Fraction(charged_gas_cost_wei * price_answer, price_scale)
    stress_gas_cost_usd = Fraction(stress_gas_cost_wei * price_answer, price_scale)
    acceptance_mev_usd = (
        Fraction(projection["requested_notional_usd"], 1) * acceptance_bps / 10000
    )
    policy_net = gross_edge_usd - charged_gas_cost_usd - acceptance_mev_usd
    baseline_max_fee_per_gas = 2 * projection["child_base_fee_wei"] + p50_tip
    stress_max_fee_per_gas = 2 * projection["child_base_fee_wei"] + p90_tip
    stress_rows = []
    for mev_bps in stress_bps:
        mev_usd = Fraction(projection["requested_notional_usd"], 1) * mev_bps / 10000
        stress_rows.append((
            mev_bps,
            mev_usd,
            gross_edge_usd - stress_gas_cost_usd - mev_usd,
        ))
    result = {
        "actual_intermediate_uni_raw": actual_intermediate,
        "actual_final_weth_raw": actual_final,
        "gross_profit_weth_wei": gross_profit,
        "gross_edge_usd": _canonical_fraction(gross_edge_usd),
        "child_base_fee_wei": projection["child_base_fee_wei"],
        "receipt_gas_used": gas_used,
        "receipt_effective_gas_price": effective_gas_price,
        "p50_priority_fee_wei": p50_tip,
        "p90_priority_fee_wei": p90_tip,
        "max_fee_per_gas_wei": baseline_max_fee_per_gas,
        "baseline_max_fee_per_gas_wei": baseline_max_fee_per_gas,
        "stress_max_fee_per_gas_wei": stress_max_fee_per_gas,
        "charged_gas_cost_weth_wei": charged_gas_cost_wei,
        "charged_gas_cost_usd": _canonical_fraction(charged_gas_cost_usd),
        "stress_gas_cost_weth_wei": stress_gas_cost_wei,
        "stress_gas_cost_usd": _canonical_fraction(stress_gas_cost_usd),
        "acceptance_mev_buffer_usd": _canonical_fraction(acceptance_mev_usd),
        "policy_net_edge_usd": _canonical_fraction(policy_net),
        "policy_net_positive": policy_net > 0,
        "stress_projections": tuple(
            {
                "mev_bps": _canonical_fraction(mev_bps),
                "mev_buffer_usd": _canonical_fraction(mev_usd),
                "net_usd": _canonical_fraction(net),
                "positive": net > 0,
            }
            for mev_bps, mev_usd, net in stress_rows
        ),
        "stress_robust": all(net > 0 for _mev, _buffer, net in stress_rows),
    }
    for mev_bps, mev_usd, net in stress_rows:
        label = _canonical_fraction(mev_bps).replace(".", "_")
        result["stress_" + label + "_mev_buffer_usd"] = _canonical_fraction(mev_usd)
        result["stress_" + label + "_net_usd"] = _canonical_fraction(net)
    return result


def _nonnegative_int(value: Any, label: str) -> int:
    if type(value) is not int or value < 0:
        raise ValueError(label + " must be a non-negative integer")
    return value


def _positive_int(value: Any, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise ValueError(label + " must be a positive integer")
    return value


def _reserve_pair(value: Any, label: str) -> Tuple[int, int]:
    if not isinstance(value, tuple) or len(value) != 2:
        raise ValueError(label + " must be a (UNI, WETH) tuple")
    return (
        _nonnegative_int(value[0], label + " UNI"),
        _nonnegative_int(value[1], label + " WETH"),
    )


def _next_base_fee(
    parent_base_fee: Any, parent_gas_used: Any, parent_gas_limit: Any
) -> int:
    base_fee = _nonnegative_int(parent_base_fee, "parent base fee")
    gas_used = _nonnegative_int(parent_gas_used, "parent gas used")
    gas_limit = _positive_int(parent_gas_limit, "parent gas limit")
    if gas_limit < 2 or gas_used > gas_limit:
        raise ValueError("parent gas values are invalid")
    target = gas_limit // 2
    if gas_used == target:
        return base_fee
    if gas_used > target:
        increment = base_fee * (gas_used - target) // target // 8
        return base_fee + max(increment, 1)
    decrement = base_fee * (target - gas_used) // target // 8
    return base_fee - decrement


def next_historical_base_fee(
    *,
    parent_base_fee: int,
    parent_gas_used: int,
    parent_gas_limit: int,
) -> int:
    """Project the exact EIP-1559 child base fee from one parent header."""
    return _next_base_fee(parent_base_fee, parent_gas_used, parent_gas_limit)


def _decimal_fraction(value: Any, label: str) -> Fraction:
    if not isinstance(value, str) or _DECIMAL.fullmatch(value) is None:
        raise ValueError(label + " must be a canonical non-negative decimal")
    whole, separator, fractional = value.partition(".")
    numerator = int(whole + fractional)
    denominator = 10 ** len(fractional) if separator else 1
    return Fraction(numerator, denominator)


def _canonical_fraction(value: Fraction) -> str:
    numerator = value.numerator
    denominator = value.denominator
    negative = numerator < 0
    numerator = abs(numerator)
    twos = 0
    fives = 0
    while denominator % 2 == 0:
        denominator //= 2
        twos += 1
    while denominator % 5 == 0:
        denominator //= 5
        fives += 1
    if denominator != 1:
        raise ValueError("economic value is not a terminating decimal")
    scale = max(twos, fives)
    scaled = numerator * (2 ** (scale - twos)) * (5 ** (scale - fives))
    if scale == 0:
        rendered = str(scaled)
    else:
        digits = str(scaled).rjust(scale + 1, "0")
        rendered = (
            digits[:-scale] + "." + digits[-scale:]
        ).rstrip("0").rstrip(".")
    if rendered == "0":
        return "0"
    return "-" + rendered if negative else rendered


def _prefilter_receipt_inputs(value: Any) -> Dict[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError("prefilter projection must be a mapping")
    required = frozenset((
        "requested_notional_usd", "eth_usd_answer", "feed_decimals",
        "acceptance_mev_bps", "amount_weth_in_wei", "first_amount_out_raw",
        "child_base_fee_wei",
    ))
    if not required.issubset(value):
        raise ValueError("prefilter projection is incomplete")
    return {
        "requested_notional_usd": _nonnegative_int(
            value["requested_notional_usd"], "prefilter requested notional"
        ),
        "eth_usd_answer": _positive_int(
            value["eth_usd_answer"], "prefilter ETH/USD answer"
        ),
        "feed_decimals": _nonnegative_int(
            value["feed_decimals"], "prefilter feed decimals"
        ),
        "acceptance_mev_bps": _canonical_fraction(_decimal_fraction(
            value["acceptance_mev_bps"], "prefilter acceptance MEV"
        )),
        "amount_weth_in_wei": _nonnegative_int(
            value["amount_weth_in_wei"], "prefilter WETH input"
        ),
        "first_amount_out_raw": _nonnegative_int(
            value["first_amount_out_raw"], "prefilter first output"
        ),
        "child_base_fee_wei": _nonnegative_int(
            value["child_base_fee_wei"], "prefilter child base fee"
        ),
    }


def _stress_bps(value: Any) -> Tuple[Fraction, Fraction]:
    if not isinstance(value, tuple) or len(value) != 2:
        raise ValueError("stress MEV must contain two rates")
    return (
        _decimal_fraction(value[0], "first stress MEV"),
        _decimal_fraction(value[1], "second stress MEV"),
    )


def validate_historical_foundry_policy(
    value: Mapping[str, Any], *, authority_bytes: bytes, toolchain_bytes: bytes
) -> Dict[str, Any]:
    """Validate one closed policy and bind it to explicit physical KAT bytes."""
    result = validate_historical_foundry_policy_shape(value)
    _require_bytes(authority_bytes, "authority")
    _require_bytes(toolchain_bytes, "toolchain")
    if result["authority_sha256"] != hashlib.sha256(authority_bytes).hexdigest():
        raise ValueError("policy authority physical hash does not match")
    if result["toolchain_sha256"] != hashlib.sha256(toolchain_bytes).hexdigest():
        raise ValueError("policy toolchain physical hash does not match")
    return _copy_value(result)


def validate_historical_foundry_policy_shape(
    value: Mapping[str, Any],
) -> Dict[str, Any]:
    """Validate the complete pure policy payload without physical byte binding."""
    result = _value_from_input(value)
    _validate_policy_shape(result)
    return _copy_value(result)


def _validate_policy_shape(result: Dict[str, Any]) -> None:
    _require_object(result, _POLICY_FIELDS, "policy")
    _require_exact(result, "schema", "historical_foundry_replay_policy/v1", "policy")
    _require_exact_int(result, "chain_id", 1, "policy")
    _require_exact(result, "anchor_tag", "finalized", "policy")
    _require_exact_int(result, "lookback_seconds", 604800, "policy")
    _require_exact(result, "selection_rule", "newest_publishable_policy_positive", "policy")
    _require_exact_list(result, "requested_notionals_usd",
                        ["1000", "5000", "10000", "50000", "100000"], "policy")
    _require_exact_list(result, "directions",
                        ["uniswap_to_sushiswap", "sushiswap_to_uniswap"], "policy")
    _require_exact_int(result, "max_eth_usd_age_seconds", 3600, "policy")
    _require_exact(result, "state_basis", "post_block_state", "policy")
    _validate_execution(result["execution"])
    _validate_fees(result["fees"])
    _validate_profitability(result["profitability"])
    _validate_closed_reverts(result["closed_revert_matrix"])
    if not _is_hash(result["authority_sha256"]) or not _is_hash(result["toolchain_sha256"]):
        raise ValueError("policy physical cross-hash is invalid")


def validate_historical_foundry_authority(value: Mapping[str, Any]) -> Dict[str, Any]:
    """Validate the closed UNI/WETH authority contract without I/O."""
    result = _value_from_input(value)
    _require_object(result, _AUTHORITY_FIELDS, "authority")
    _require_exact(result, "schema", "historical_foundry_replay_authority/v1", "authority")
    _require_exact_int(result, "chain_id", 1, "authority")
    _validate_tokens(result["tokens"])
    _validate_venues(result["venues"])
    _validate_price_feed(result["price_feed"])
    _validate_sender(result["sender"])
    _validate_executor(result["executor"])
    _validate_v2_formula(result["v2_formula"])
    _validate_override_layout(result["state_override_layout"])
    return _copy_value(result)


def validate_historical_foundry_toolchain(value: Mapping[str, Any]) -> Dict[str, Any]:
    """Validate fixed toolchain and compiler/build authority without I/O."""
    result = _value_from_input(value)
    _require_object(result, _TOOLCHAIN_FIELDS, "toolchain")
    _require_exact(result, "schema", "historical_foundry_replay_toolchain/v1", "toolchain")
    _validate_foundry_release(result["foundry_release"])
    _validate_binaries(result["binaries"])
    _validate_solc(result["solc"])
    _validate_forge_std(result["forge_std"])
    _validate_compiler_settings(result["compiler_settings"])
    _validate_executor_build(result["executor_build"])
    return _copy_value(result)


def _validate_execution(value: Any) -> None:
    _require_object(value, _EXECUTION_FIELDS, "policy execution")
    _require_exact(value, "model", "historical_counterfactual_state_override_next_block", "policy execution")
    _require_exact_int(value, "synthetic_timestamp_offset_seconds", 12, "policy execution")
    _require_exact_int(value, "calldata_deadline_offset_seconds", 60, "policy execution")
    _require_object(value["router_min_output_raw"], _MIN_OUTPUT_FIELDS, "router minimum output")
    _require_exact(value["router_min_output_raw"], "first_leg", "0", "router minimum output")
    _require_exact(value["router_min_output_raw"], "second_leg", "0", "router minimum output")
    _require_exact(value, "transaction_type", "eip1559_type_2", "policy execution")
    _require_exact_int(value, "transaction_gas_limit", 2000000, "policy execution")
    _require_exact_list(value, "access_list", [], "policy execution")
    _require_exact_int(value, "sender_nonce", 0, "policy execution")


def _validate_fees(value: Any) -> None:
    _require_object(value, _FEE_FIELDS, "policy fees")
    _require_exact(value, "next_base_fee_rule", "eip1559_next_base_fee", "policy fees")
    _require_exact_int(value, "acceptance_tip_percentile", 50, "policy fees")
    _require_exact_int(value, "stress_tip_percentile", 90, "policy fees")
    _require_exact_int(value, "max_fee_multiplier", 2, "policy fees")
    _require_decimal(value["acceptance_mev_bps"], "acceptance MEV")
    _require_exact_list(value, "stress_mev_bps", ["25", "50"], "policy fees")


def _validate_profitability(value: Any) -> None:
    _require_object(value, _PROFITABILITY_FIELDS, "profitability")
    _require_exact(value, "winner_comparison", "strict_positive", "profitability")
    _require_exact(value, "exact_zero_result", "reject", "profitability")
    _require_exact(value, "serialization", "canonical_fixed_point", "profitability")


def _validate_closed_reverts(value: Any) -> None:
    if not isinstance(value, list) or len(value) != len(_EXPECTED_REVERTS):
        raise ValueError("closed revert matrix is invalid")
    observed = []
    for entry in value:
        _require_object(entry, _REVERT_FIELDS, "closed revert entry")
        if not _SELECTOR.fullmatch(entry["revert_selector"]) or not _is_hash(entry["revert_data_sha256"]):
            raise ValueError("closed revert selector or hash is invalid")
        if entry["terminal_class"] != "closed_revert":
            raise ValueError("closed revert terminal class is invalid")
        observed.append((entry["prefilter_reason"], entry["leg"], entry["revert_selector"], entry["revert_data_sha256"]))
    if tuple(observed) != _EXPECTED_REVERTS:
        raise ValueError("closed revert matrix is not the reviewed canonical order")


def _validate_tokens(value: Any) -> None:
    if not isinstance(value, list) or len(value) != len(_EXPECTED_TOKENS):
        raise ValueError("authority tokens are invalid")
    for entry, expected in zip(value, _EXPECTED_TOKENS):
        role, address, balance_slot, allowance_slot = expected
        _require_object(entry, _TOKEN_FIELDS, "authority token")
        _require_exact(entry, "role", role, "authority token")
        _require_exact_address(entry, "address", address, "authority token")
        _require_exact_int(entry, "decimals", 18, "authority token")
        _validate_descriptor(entry["balance_descriptor"], balance_slot, "address_then_slot")
        _validate_descriptor(entry["allowance_descriptor"], allowance_slot, "owner_spender_then_slot")


def _validate_descriptor(value: Any, slot: int, key_order: str) -> None:
    _require_object(value, _DESCRIPTOR_FIELDS, "token storage descriptor")
    _require_exact(value, "kind", "mapping", "token storage descriptor")
    _require_exact_int(value, "slot", slot, "token storage descriptor")
    _require_exact(value, "key_order", key_order, "token storage descriptor")
    selector = "0x70a08231" if key_order == "address_then_slot" else "0xdd62ed3e"
    _require_exact(value, "getter_selector", selector, "token storage descriptor")


def _validate_venues(value: Any) -> None:
    if not isinstance(value, list) or len(value) != len(_EXPECTED_VENUES):
        raise ValueError("authority venues are invalid")
    for entry, expected in zip(value, _EXPECTED_VENUES):
        venue_id, router, factory = expected
        _require_object(entry, _VENUE_FIELDS, "authority venue")
        _require_exact(entry, "venue_id", venue_id, "authority venue")
        _require_exact_address(
            entry, "router_address", router, "authority venue"
        )
        _require_exact_address(
            entry, "factory_address", factory, "authority venue"
        )
        _require_exact(entry, "factory_selector", "0xc45a0155", "authority venue")
        _require_exact(entry, "weth_selector", "0xad5c4648", "authority venue")
        _require_exact(entry, "pair_getter_selector", "0xe6a43905", "authority venue")
        _require_exact(entry, "pair_derivation", "factory_get_pair_uni_weth", "authority venue")


def _validate_price_feed(value: Any) -> None:
    _require_object(value, _PRICE_FEED_FIELDS, "price feed")
    _require_exact_address(
        value,
        "proxy_address",
        "0x5f4ec3df9cbd43714fe2740f5e3616155c5b8419",
        "price feed",
    )
    _require_exact(value, "description", "ETH / USD", "price feed")
    _require_exact_int(value, "decimals", 8, "price feed")
    _require_exact(value, "latest_round_selector", "0xfeaf968c", "price feed")
    _require_exact(value, "aggregator_selector", "0x245a7bfc", "price feed")
    _require_exact(value, "phase_selector", "0x58303b10", "price feed")


def _validate_sender(value: Any) -> None:
    _require_object(value, _SENDER_FIELDS, "sender")
    _require_exact_address(
        value,
        "address",
        "0x5ca9e6c3ed27cc0acfb355061fcab6964d4fc444",
        "sender",
    )
    _require_exact_int(value, "nonce", 0, "sender")


def _validate_executor(value: Any) -> None:
    _require_object(value, _EXECUTOR_FIELDS, "executor")
    _require_exact_address(
        value,
        "address",
        "0x68778b870ceee58d82ba9f97cb4219981fdafa72",
        "executor",
    )
    _require_exact(value, "prior_code", "empty", "executor")
    _require_exact_int(value, "prior_nonce", 0, "executor")
    _require_exact_list(value, "prior_token_balances", ["uni", "weth"], "executor")
    _require_exact_list(value, "prior_allowances", [
        "uni_uniswap_v2", "uni_sushiswap_v2", "weth_uniswap_v2", "weth_sushiswap_v2",
    ], "executor")


def _validate_v2_formula(value: Any) -> None:
    _require_object(value, _V2_FORMULA_FIELDS, "v2 formula")
    _require_exact_int(value, "fee_numerator", 997, "v2 formula")
    _require_exact_int(value, "fee_denominator", 1000, "v2 formula")


def _validate_override_layout(value: Any) -> None:
    _require_object(value, _OVERRIDE_LAYOUT_FIELDS, "state override layout")
    _require_exact_list(value, "account_roles", ["sender", "executor", "weth"], "state override layout")
    _require_exact_list(value, "storage_roles", [
        "executor_weth_balance", "weth_native_backing", "executor_uni_allowances",
        "executor_weth_allowances",
    ], "state override layout")
    _require_exact(value, "weth_backing_rule", "executor_weth_delta_matches_weth_native_delta", "state override layout")
    _require_exact(value, "allowance_matrix_rule", "executor_to_each_router_for_each_token", "state override layout")


def _validate_foundry_release(value: Any) -> None:
    _require_object(value, _FOUNDRY_RELEASE_FIELDS, "foundry release")
    expected = {
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
    }
    for key, expected_value in expected.items():
        _require_exact(value, key, expected_value, "foundry release")


def _validate_binaries(value: Any) -> None:
    if not isinstance(value, list) or len(value) != 3:
        raise ValueError("toolchain binaries are invalid")
    for entry, name in zip(value, ("forge", "cast", "anvil")):
        _require_object(entry, _BINARY_FIELDS, "toolchain binary")
        _require_exact(entry, "name", name, "toolchain binary")
        _require_exact(entry, "version", "v1.7.1", "toolchain binary")
        if not _is_hash(entry["sha256"]):
            raise ValueError("toolchain binary hash is invalid")


def _validate_solc(value: Any) -> None:
    _require_object(value, _SOLC_FIELDS, "solc")
    _require_exact(value, "version", "0.8.36+commit.8a079791", "solc")
    _require_exact(value, "artifact_url", "https://binaries.soliditylang.org/macosx-amd64/solc-macosx-amd64-v0.8.36+commit.8a079791", "solc")
    _require_exact(value, "artifact_sha256", "d4abcf0b3e24b7948ddfd64c374d26c3214648717777790ecb936979054a129d", "solc")


def _validate_forge_std(value: Any) -> None:
    _require_object(value, _FORGE_STD_FIELDS, "forge std")
    _require_exact(value, "repository_url", "https://github.com/foundry-rs/forge-std.git", "forge std")
    _require_exact(value, "version", "v1.16.1", "forge std")
    _require_exact(value, "commit", "620536fa5277db4e3fd46772d5cbc1ea0696fb43", "forge std")


def _validate_compiler_settings(value: Any) -> None:
    _require_object(value, _COMPILER_SETTINGS_FIELDS, "compiler settings")
    expected = {
        "evm_version": "osaka", "fork_hardfork": "osaka", "optimizer_enabled": True,
        "optimizer_runs": 200, "via_ir": False, "bytecode_hash": "none",
        "cbor_metadata": False, "append_cbor": False,
    }
    for key, expected_value in expected.items():
        _require_exact(value, key, expected_value, "compiler settings")


def _validate_executor_build(value: Any) -> None:
    _require_object(value, _EXECUTOR_BUILD_FIELDS, "executor build")
    for key in _EXECUTOR_BUILD_FIELDS:
        if not _is_hash(value[key]):
            raise ValueError("executor build hash is invalid")


def _parse_canonical_json_bytes(payload: bytes) -> Dict[str, Any]:
    _require_bytes(payload, "historical config")
    if not payload.endswith(b"\n") or payload.endswith(b"\n\n"):
        raise ValueError("historical config must have exactly one trailing LF")
    try:
        value = json.loads(
            payload.decode("utf-8"), object_pairs_hook=_reject_duplicate_json_keys,
            parse_constant=_reject_json_constant,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError) as exc:
        raise ValueError("historical config JSON is invalid") from exc
    if not isinstance(value, dict) or _canonical_json_bytes(value) != payload:
        raise ValueError("historical config JSON is not canonical")
    return value


def _value_from_input(value: Any) -> Dict[str, Any]:
    if isinstance(value, bytes):
        return _parse_canonical_json_bytes(value)
    if not isinstance(value, Mapping):
        raise ValueError("historical config value must be a mapping")
    return _copy_value(value)


def _canonical_json_bytes(value: Mapping[str, Any]) -> bytes:
    try:
        rendered = json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    except (TypeError, ValueError) as exc:
        raise ValueError("historical config value is not JSON") from exc
    return (rendered + "\n").encode("utf-8")


def _copy_value(value: Mapping[str, Any]) -> Dict[str, Any]:
    return json.loads(_canonical_json_bytes(value).decode("utf-8"))


def _freeze(value: Any) -> Any:
    if isinstance(value, dict):
        return MappingProxyType({key: _freeze(item) for key, item in value.items()})
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    return value


def _reject_duplicate_json_keys(pairs: Any) -> Dict[str, Any]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _reject_json_constant(_token: str) -> None:
    raise ValueError("non-finite JSON number")


def _require_object(value: Any, fields: Any, label: str) -> None:
    if not isinstance(value, dict) or set(value) != fields:
        raise ValueError(label + " schema is invalid")


def _require_exact(value: Mapping[str, Any], key: str, expected: Any, label: str) -> None:
    item = value.get(key)
    if isinstance(expected, bool):
        if not isinstance(item, bool) or item is not expected:
            raise ValueError(label + " field " + key + " is invalid")
        return
    if item != expected or (
        isinstance(expected, int) and isinstance(item, bool)
    ):
        raise ValueError(label + " field " + key + " is invalid")


def _require_exact_address(
    value: Mapping[str, Any], key: str, expected: str, label: str
) -> None:
    item = value.get(key)
    if not isinstance(item, str) or _ADDRESS.fullmatch(item) is None:
        raise ValueError(
            label + " address is not a canonical lowercase Ethereum address"
        )
    _require_exact(value, key, expected, label)


def _require_exact_int(value: Mapping[str, Any], key: str, expected: int, label: str) -> None:
    item = value.get(key)
    if isinstance(item, bool) or not isinstance(item, int) or item != expected:
        raise ValueError(label + " integer " + key + " is invalid")


def _require_exact_list(value: Mapping[str, Any], key: str, expected: Any, label: str) -> None:
    if not isinstance(value.get(key), list) or value[key] != expected:
        raise ValueError(label + " list " + key + " is invalid")


def _require_decimal(value: Any, label: str) -> None:
    if not isinstance(value, str) or _DECIMAL.fullmatch(value) is None:
        raise ValueError(label + " is not a canonical non-negative decimal")


def _require_bytes(value: Any, label: str) -> None:
    if not isinstance(value, bytes):
        raise ValueError(label + " bytes are invalid")


def _is_hash(value: Any) -> bool:
    return isinstance(value, str) and _HASH.fullmatch(value) is not None
