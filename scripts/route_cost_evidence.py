"""Pure validation and replay primitives for strict Shadow route-cost evidence.

The public validator in this module performs no file, network, clock, or
subprocess I/O.  Production capture is intentionally a separate boundary: it
must first freeze its registries, profiles, raw RPC results, and connector
attestation, then pass the resulting manifest through this offline replay gate.
"""

from __future__ import annotations

import base64
import binascii
import copy
from collections import Counter
from decimal import Decimal, InvalidOperation, ROUND_CEILING, localcontext
from fractions import Fraction
import hashlib
import json
import math
import os
from pathlib import Path
import re
import stat
import subprocess
import sys
import tempfile
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import urlsplit

try:
    from scripts.timestamp_contract import exact_rfc3339_epoch_seconds
    from scripts.fetch_cex_depth import binance_market_rules_projection
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    from timestamp_contract import exact_rfc3339_epoch_seconds  # type: ignore
    from fetch_cex_depth import binance_market_rules_projection  # type: ignore


ROUTE_COST_EVIDENCE_SCHEMA = "route_cost_evidence_manifest/v1"
ROUTE_COST_ADAPTER_REGISTRY_SCHEMA = "route_cost_adapter_registry/v1"
ROUTE_COST_CONNECTOR_KEY_REGISTRY_SCHEMA = (
    "route_cost_connector_key_registry/v1"
)
ROUTE_COST_SELECTED_MARKETS_SCHEMA = "route_cost_selected_markets/v1"
ROUTE_COST_TRANSCRIPT_SCHEMA = "route_cost_evidence_transcript/v1"
ROUTE_COST_SIMULATION_TARGET_SCHEMA = "route_cost_simulation_target/v1"
ROUTE_COST_CHAIN_EVIDENCE_SCHEMA = "route_cost_chain_evidence/v1"
ROUTE_COST_MARKET_EVIDENCE_SCHEMA = "route_cost_market_evidence/v1"
ROUTE_COST_FACTORY_GET_PAIR_REQUEST_SCHEMA = (
    "route_cost_factory_get_pair_request/v1"
)
ROUTE_COST_FACTORY_GET_PAIR_RESPONSE_SCHEMA = (
    "route_cost_factory_get_pair_response/v1"
)
ROUTE_COST_TOKEN_RUNTIME_CODE_EVIDENCE_SCHEMA = (
    "route_cost_token_runtime_code_evidence/v1"
)
ROUTE_COST_TOKEN_RUNTIME_CODE_REQUEST_SCHEMA = (
    "route_cost_token_runtime_code_request/v1"
)
ROUTE_COST_TOKEN_RUNTIME_CODE_RESPONSE_SCHEMA = (
    "route_cost_token_runtime_code_response/v1"
)
ROUTE_COST_BINDING_SCHEMA = "route_cost_evidence_binding/v1"
ROUTE_COST_BLOCK_EVIDENCE_SCHEMA = "route_cost_block_evidence/v1"
ROUTE_COST_CALL_EVIDENCE_SCHEMA = "route_cost_call_evidence/v1"
ROUTE_COST_GAS_EVIDENCE_SCHEMA = "route_cost_gas_evidence/v1"
ROUTE_COST_ROUTER_FEE_EVIDENCE_SCHEMA = "route_cost_router_fee_evidence/v1"
ROUTE_COST_TRANSFER_TAX_EVIDENCE_SCHEMA = (
    "route_cost_transfer_tax_evidence/v1"
)
ROUTE_COST_RAW_TRANSCRIPT_SCHEMA = "route_cost_raw_transcript/v1"
ROUTE_V2_POOL_STATE_SCHEMA = "route_v2_pool_state/v1"
ROUTE_COST_POLICY_MEMBER_SCHEMA = "route_cost_submission_policy_member/v1"
ROUTE_COST_POLICY_SNAPSHOT_SCHEMA = (
    "route_cost_submission_policy_snapshot/v1"
)
ROUTE_COST_POLICY_REQUEST_SCHEMA = "route_cost_submission_policy_request/v1"
ROUTE_COST_PAIR_AUTHORITY_SCHEMA = "route_cost_pair_authority_record/v1"
ROUTE_COST_TOKEN_FUNDING_AUTHORITY_SCHEMA = (
    "route_cost_token_funding_authority_record/v1"
)

MAX_ROUTE_COST_EVIDENCE_BYTES = 32 * 1024 * 1024
MAX_ADAPTER_REGISTRY_BYTES = 64 * 1024
MAX_AUTHORITY_RECORD_BYTES = 16 * 1024
MAX_CONNECTOR_KEY_REGISTRY_BYTES = 64 * 1024
MAX_PROFILE_BYTES = 64 * 1024
MAX_SELECTED_MARKETS = 8
MAX_TRANSCRIPTS = 80
MAX_BINDINGS = 4096
MAX_CHAIN_EVIDENCE_BYTES = 64 * 1024
MAX_MARKET_EVIDENCE_BYTES = 400 * 1024
MAX_RUNTIME_CODE_BYTES = 128 * 1024
MAX_TRANSCRIPT_BYTES = 16 * 1024
MAX_NATIVE_PRICE_EVIDENCE_BYTES = 4 * 1024 * 1024
MAX_NATIVE_PRICE_RAW_BYTES = 2 * 1024 * 1024
MAX_NATIVE_PRICE_MARKET_RULES_RAW_BYTES = 256 * 1024
MAX_NATIVE_PRICE_JSON_DEPTH = 100
MAX_NATIVE_PRICE_JSON_NODES = 8192
MAX_NATIVE_PRICE_JSON_SCALAR_BYTES = 2 * 1024 * 1024
MAX_NATIVE_PRICE_JSON_STRING_BYTES = 256 * 1024
MAX_NATIVE_PRICE_JSON_NUMBER_TOKEN_BYTES = 256
MAX_CALLDATA_BYTES = 4 * 1024
MAX_SIGNATURE_BYTES = 4 * 1024
MAX_RETAINED_V2_POOL_STATE_BYTES = 1024 * 1024

REQUESTED_NOTIONALS_USD = ("1000", "5000", "10000", "50000", "100000")
ETHEREUM_V2_ADAPTER_ID = "uniswap-v2-router02-ethereum"
ETHEREUM_V2_PROTOCOL_FAMILY = "uniswap_v2_router02"
ETHEREUM_V2_ROUTER_ADDRESS = (
    "0x7a250d5630b4cf539739df2c5dacb4c659f2488d"
)
ETHEREUM_V2_FACTORY_ADDRESS = (
    "0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f"
)
ETHEREUM_V2_ROUTER_RUNTIME_CODE_SHA256 = (
    "ccef50da4af021b09ada39d78db5d281fffff81a57969c7028bccc1f50d37854"
)
ETHEREUM_V2_FACTORY_RUNTIME_CODE_SHA256 = (
    "3abc53f12a9cb8ae37ebfada9efc261c1ab4c2759d161e341a49bf67df3f8321"
)
ETHEREUM_WETH_ADDRESS = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
ETHEREUM_V2_SIMULATION_SENDER = (
    "0x000000000000000000000000000000000000dead"
)
ETHEREUM_V2_SIMULATION_SENDER_NATIVE_BALANCE_WEI = 10 ** 18
ETHEREUM_V2_BUY_SELECTOR = "0x8803dbee"
ETHEREUM_V2_SELL_SELECTOR = "0x38ed1739"
ETHEREUM_V2_ALLOWED_SELECTORS = (
    ETHEREUM_V2_SELL_SELECTOR,
    ETHEREUM_V2_BUY_SELECTOR,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
_ADAPTER_REGISTRY_PATH = PROJECT_ROOT / "config/route_cost_adapters.json"
_AUTHORITY_RECORD_ROOT = PROJECT_ROOT / "config/route_cost_authority"
_CONNECTOR_KEY_REGISTRY_PATH = (
    PROJECT_ROOT / "config/route_cost_connector_keys.json"
)

_TRACKED_ADAPTER_REGISTRY = {
    "schema": ROUTE_COST_ADAPTER_REGISTRY_SCHEMA,
    "registry_version": "2026-08-12-fixed-block-20000000",
    "adapters": [
        {
            "adapter_id": ETHEREUM_V2_ADAPTER_ID,
            "chain_id": 1,
            "protocol_family": ETHEREUM_V2_PROTOCOL_FAMILY,
            "router_address": ETHEREUM_V2_ROUTER_ADDRESS,
            "factory_address": ETHEREUM_V2_FACTORY_ADDRESS,
            "router_runtime_code_sha256": (
                ETHEREUM_V2_ROUTER_RUNTIME_CODE_SHA256
            ),
            "factory_runtime_code_sha256": (
                ETHEREUM_V2_FACTORY_RUNTIME_CODE_SHA256
            ),
            "pair_fee_bps": "30",
            "gas_fee_model": "eip1559_fee_history_v1",
            "allowed_selectors": list(ETHEREUM_V2_ALLOWED_SELECTORS),
            "supports_native": False,
            "supports_multihop": False,
            "supports_fee_on_transfer": False,
            "trace_method": "debug_traceCall_state_override_v1",
            "connector_family": "private_submission_connector/v1",
            "pair_descriptors": [
                {
                    "pair_address": (
                        "0xd3d2e2692501a5c9ca623199d38826e513033a17"
                    ),
                    "token0_address": (
                        "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984"
                    ),
                    "token1_address": ETHEREUM_WETH_ADDRESS,
                    "pair_runtime_code_sha256": (
                        "8b5db55fa9ab3b9527508d4abe0b39eb588bf310270c8e04b3f38214e8ba63b4"
                    ),
                    "source_metadata_sha256": (
                        "cf2697308aa7a9e6f53977c47f63d972b1d3216b001fe97588d16f63c4d3c50b"
                    ),
                }
            ],
            "token_funding_descriptors": [
                {
                    "token_address": (
                        "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984"
                    ),
                    "runtime_code_sha256": (
                        "77ea2b530607db6cb87c7cce18016aa12dd0762c4357355bceee2cb11721bebe"
                    ),
                    "proxy_implementation_address": None,
                    "proxy_implementation_code_sha256": None,
                    "storage_layout": "solidity_mapping_v1",
                    "balance_mapping_slot": "4",
                    "allowance_mapping_slot": "3",
                    "source_metadata_sha256": (
                        "6dc7dbc69c1a1204ebd0fc4fbea7a1ddc51329d24b68339cc7a10003082e9b0f"
                    ),
                },
                {
                    "token_address": ETHEREUM_WETH_ADDRESS,
                    "runtime_code_sha256": (
                        "5566bf50796faf93c9b6f6adacd3b32c70bfe16b48ffc59db6cd144cbdc89739"
                    ),
                    "proxy_implementation_address": None,
                    "proxy_implementation_code_sha256": None,
                    "storage_layout": "solidity_mapping_v1",
                    "balance_mapping_slot": "3",
                    "allowance_mapping_slot": "4",
                    "source_metadata_sha256": (
                        "4336e60bdb78bca45946160dd17bc28e24292b096aebb7b886a756cea15e20a3"
                    ),
                },
            ],
            "native_symbol": "ETH",
            "wrapped_native_address": ETHEREUM_WETH_ADDRESS,
            "simulation_sender_address": ETHEREUM_V2_SIMULATION_SENDER,
            "native_price_reference_market_id": "cex:binance:ETH/USDT",
            "native_price_reference_adapter_id": (
                "binance_public_spot_depth/v1"
            ),
        }
    ],
}
_TRACKED_CONNECTOR_KEY_REGISTRY = {
    "schema": ROUTE_COST_CONNECTOR_KEY_REGISTRY_SCHEMA,
    "registry_version": "2026-08-12-no-production-key",
    "keys": [],
}

ROUTE_COST_EVIDENCE_FIELDS = (
    "schema",
    "run_id",
    "route_cohort_id",
    "phase",
    "candidate_source_generation",
    "route_universe_sha256",
    "adapter_registry",
    "adapter_registry_sha256",
    "connector_key_registry",
    "connector_key_registry_sha256",
    "transcript_count",
    "trace_profile_identity",
    "trace_profile_generation",
    "submission_connector_profile_identity",
    "submission_connector_profile_generation",
    "evaluated_at",
    "selected_market_count",
    "selected_markets",
    "selected_market_set_sha256",
    "native_price_evidence",
    "native_price_evidence_sha256",
    "chain_evidence_count",
    "chain_evidence",
    "chain_evidence_set_sha256",
    "market_evidence_count",
    "market_evidence",
    "market_evidence_set_sha256",
    "transcripts",
    "transcript_set_sha256",
    "binding_count",
    "bindings",
    "binding_set_sha256",
    "submission_policy_snapshot",
    "submission_policy_snapshot_sha256",
    "counts",
)

ADAPTER_REGISTRY_FIELDS = ("schema", "registry_version", "adapters")
ADAPTER_FIELDS = (
    "adapter_id",
    "chain_id",
    "protocol_family",
    "router_address",
    "factory_address",
    "router_runtime_code_sha256",
    "factory_runtime_code_sha256",
    "pair_fee_bps",
    "gas_fee_model",
    "allowed_selectors",
    "supports_native",
    "supports_multihop",
    "supports_fee_on_transfer",
    "trace_method",
    "connector_family",
    "pair_descriptors",
    "token_funding_descriptors",
    "native_symbol",
    "wrapped_native_address",
    "simulation_sender_address",
    "native_price_reference_market_id",
    "native_price_reference_adapter_id",
)
FUNDING_DESCRIPTOR_FIELDS = (
    "token_address",
    "runtime_code_sha256",
    "proxy_implementation_address",
    "proxy_implementation_code_sha256",
    "storage_layout",
    "balance_mapping_slot",
    "allowance_mapping_slot",
    "source_metadata_sha256",
)
PAIR_DESCRIPTOR_FIELDS = (
    "pair_address",
    "token0_address",
    "token1_address",
    "pair_runtime_code_sha256",
    "source_metadata_sha256",
)
AUTHORITY_COMMON_FIELDS = (
    "schema",
    "authority_id",
    "chain_id",
    "block_number",
    "block_hash",
    "state_root",
    "runtime_code_sha256",
    "runtime_code_size",
    "source_repository",
    "source_commit",
    "source_path",
    "source_sha256",
)
PAIR_AUTHORITY_FIELDS = AUTHORITY_COMMON_FIELDS + (
    "factory_address",
    "pair_address",
    "token0_address",
    "token1_address",
    "factory_get_pair_calldata",
    "factory_get_pair_result",
    "token0_result",
    "token1_result",
)
TOKEN_FUNDING_AUTHORITY_FIELDS = AUTHORITY_COMMON_FIELDS + (
    "token_address",
    "storage_layout",
    "balance_mapping_slot",
    "allowance_mapping_slot",
    "balance_probe_owner",
    "balance_probe_storage_key",
    "balance_probe_getter_result",
    "balance_probe_storage_result",
)
CONNECTOR_KEY_REGISTRY_FIELDS = ("schema", "registry_version", "keys")
CONNECTOR_KEY_FIELDS = (
    "key_id",
    "connector_id",
    "algorithm",
    "public_key",
    "valid_from",
    "valid_until",
    "status",
)
SELECTED_MARKET_FIELDS = (
    "market_id",
    "token_rank",
    "selection_rank",
    "best_route_volume_usd",
    "dex_24h_usd",
    "dex_tvl_usd",
    "adapter_id",
    "structural_support_status",
    "structural_reason",
)
TRANSCRIPT_FIELDS = (
    "schema",
    "run_id",
    "route_cohort_id",
    "candidate_source_generation",
    "route_universe_sha256",
    "adapter_registry_sha256",
    "selected_market_set_sha256",
    "trace_profile_generation",
    "submission_connector_profile_generation",
    "market_id",
    "direction",
    "requested_notional_usd",
    "adapter_id",
    "simulation_target_token_address",
    "simulation_target_unit_decimals",
    "simulation_target_raw_quantity",
    "simulation_target_lattice_raw",
    "simulation_target_sha256",
    "core_pool_state_id",
    "core_pool_state_sha256",
    "chain_evidence_sha256",
    "market_evidence_sha256",
    "status",
    "completed_stage",
    "reason_code",
    "block_evidence",
    "call_evidence",
    "gas_evidence",
    "router_fee_evidence",
    "transfer_tax_evidence",
    "raw_transcript",
)
CHAIN_EVIDENCE_FIELDS = (
    "schema",
    "run_id",
    "route_cohort_id",
    "candidate_source_generation",
    "route_universe_sha256",
    "selected_market_set_sha256",
    "chain_id",
    "rpc_source_id",
    "captured_started_at",
    "captured_finished_at",
    "status",
    "reason_code",
    "block_header_result",
    "fee_history_result",
    "native_price_record",
)
MARKET_EVIDENCE_FIELDS = (
    "schema",
    "run_id",
    "route_cohort_id",
    "candidate_source_generation",
    "route_universe_sha256",
    "adapter_registry_sha256",
    "selected_market_set_sha256",
    "market_id",
    "adapter_id",
    "chain_evidence_sha256",
    "core_pool_state_id",
    "core_pool_state_sha256",
    "router_address",
    "router_runtime_code",
    "factory_address",
    "factory_runtime_code",
    "factory_get_pair_request",
    "factory_get_pair_response",
    "pair_address",
    "pair_runtime_code",
    "pair_token0",
    "pair_token1",
    "token_runtime_code_evidence",
    "captured_started_at",
    "captured_finished_at",
)
FACTORY_GET_PAIR_REQUEST_FIELDS = (
    "schema",
    "jsonrpc",
    "id",
    "method",
    "params",
)
FACTORY_GET_PAIR_RESPONSE_FIELDS = (
    "schema",
    "jsonrpc",
    "id",
    "result",
)
FACTORY_GET_PAIR_CALL_FIELDS = ("to", "data")
TOKEN_RUNTIME_CODE_EVIDENCE_FIELDS = (
    "schema",
    "token_address",
    "request",
    "response",
)
TOKEN_RUNTIME_CODE_REQUEST_FIELDS = (
    "schema",
    "jsonrpc",
    "id",
    "method",
    "params",
)
TOKEN_RUNTIME_CODE_RESPONSE_FIELDS = (
    "schema",
    "jsonrpc",
    "id",
    "result",
)
BINDING_FIELDS = (
    "schema",
    "run_id",
    "route_cohort_id",
    "candidate_source_generation",
    "route_universe_sha256",
    "adapter_registry_sha256",
    "selected_market_set_sha256",
    "connector_key_registry_sha256",
    "trace_profile_generation",
    "submission_connector_profile_generation",
    "route_id",
    "requested_notional_usd",
    "buy_transcript_sha256",
    "sell_transcript_sha256",
    "submission_policy_member_sha256",
    "evaluated_at",
    "status",
    "reason_code",
)
COUNT_FIELDS = (
    "transcript_observed",
    "transcript_unavailable",
    "transcript_failed",
    "binding_observed",
    "binding_unavailable",
    "binding_failed",
)
BLOCK_EVIDENCE_FIELDS = (
    "schema",
    "chain_evidence_sha256",
    "market_evidence_sha256",
    "chain_id",
    "block_tag",
    "block_number",
    "block_hash",
    "block_timestamp",
    "core_pool_state_id",
    "router_runtime_code_sha256",
    "factory_runtime_code_sha256",
    "pair_runtime_code_sha256",
    "rpc_transcript_sha256",
)
CALL_EVIDENCE_FIELDS = (
    "schema",
    "selector",
    "path_token_in",
    "path_token_out",
    "recipient_policy",
    "deadline",
    "amount_in_raw",
    "amount_out_raw",
    "calldata_sha256",
    "sender_policy",
    "allowance_basis",
    "submission_loss_bound_bps",
)
GAS_EVIDENCE_FIELDS = (
    "schema",
    "gas_units",
    "max_fee_per_gas_wei",
    "fee_history_sha256",
    "native_symbol",
    "native_price_usd",
    "native_price_sha256",
    "observed_at",
    "valid_until",
)
ROUTER_FEE_EVIDENCE_FIELDS = (
    "schema",
    "status",
    "rate_bps",
    "basis_code",
    "source_record_sha256",
)
TRANSFER_TAX_EVIDENCE_FIELDS = (
    "schema",
    "status",
    "rate_bps",
    "pre_input_balance",
    "post_input_balance",
    "pre_output_balance",
    "post_output_balance",
    "trace_method",
    "trace_sha256",
)
POLICY_MEMBER_FIELDS = (
    "schema",
    "route_id",
    "requested_notional_usd",
    "status",
    "reason_code",
    "submission_mode",
    "policy_id",
    "buy_submission_loss_bps",
    "sell_submission_loss_bps",
)
POLICY_SNAPSHOT_FIELDS = (
    "schema",
    "run_id",
    "route_cohort_id",
    "candidate_source_generation",
    "route_universe_sha256",
    "adapter_registry_sha256",
    "selected_market_set_sha256",
    "connector_key_registry_sha256",
    "trace_profile_generation",
    "submission_connector_profile_generation",
    "connector_id",
    "member_count",
    "members",
    "member_set_sha256",
    "status",
    "reason_code",
    "observed_at",
    "valid_until",
    "issuer_key_id",
    "signature_algorithm",
    "attested_payload_sha256",
    "signature",
)
POLICY_REQUEST_FIELDS = (
    "schema",
    "request_id",
    "run_id",
    "route_cohort_id",
    "candidate_source_generation",
    "route_universe_sha256",
    "selected_market_set_sha256",
    "adapter_registry_sha256",
    "connector_key_registry_sha256",
    "trace_profile_generation",
    "submission_connector_profile_generation",
    "connector_id",
    "members",
)
POLICY_REQUEST_MEMBER_FIELDS = ("route_id", "requested_notional_usd")
RAW_TRANSCRIPT_FIELDS = (
    "schema",
    "chain_evidence_sha256",
    "market_evidence_sha256",
    "captured_started_at",
    "captured_finished_at",
    "calldata_hex",
    "estimate_gas_request",
    "estimate_gas_response",
    "simulation_method",
    "simulation_request",
    "simulation_response",
    "simulation_balance_deltas",
)

# This is deliberately literal rather than reflected from the dataclass.  The
# retained typed member is an external producer/consumer contract; adding a
# dataclass field must not silently widen accepted evidence.
RETAINED_TYPED_SOURCE_DESCRIPTOR_FIELDS = (
    "market_id",
    "role",
    "filename",
    "sha256",
    "size",
    "logical_generation",
    "adapter_id",
    "content_schema",
)
V2_POOL_STATE_FIELDS = (
    "schema",
    "chain",
    "chain_id",
    "dex",
    "pool_address",
    "token0_address",
    "token1_address",
    "token0_decimals",
    "token1_decimals",
    "reserve0_raw",
    "reserve1_raw",
    "reserve_timestamp_last_raw",
    "fee_bps",
    "fee_numerator",
    "fee_denominator",
    "fee_formula",
    "fee_proof_sha256",
    "block_number",
    "block_hash",
    "block_header_sha256",
    "observed_at",
    "raw_response_sha256",
    "state_id",
)

BLOCK_HEADER_FIELDS = (
    "number",
    "hash",
    "parent_hash",
    "timestamp",
    "base_fee_per_gas",
    "gas_used",
    "gas_limit",
)
FEE_HISTORY_FIELDS = (
    "schema",
    "status",
    "reason_code",
    "oldest_block",
    "base_fee_per_gas",
    "reward",
    "gas_used_ratio",
)
NATIVE_PRICE_RECORD_FIELDS = (
    "schema",
    "status",
    "reason_code",
    "native_symbol",
    "wrapped_native_address",
    "price_usd",
    "observed_at",
    "valid_until",
    "native_price_evidence_sha256",
    "source_record_sha256",
)
NATIVE_PRICE_EVIDENCE_FIELDS = (
    "schema",
    "run_id",
    "route_cohort_id",
    "candidate_source_generation",
    "source_market_id",
    "source_adapter_id",
    "source_endpoint_id",
    "book_projection",
    "market_rules_projection",
    "usd_conversion_projection",
    "book_request_receipt",
    "market_rules_request_receipt",
    "raw_response_base64",
    "raw_response_sha256",
    "market_rules_raw_response_base64",
    "market_rules_raw_response_sha256",
    "observed_at",
    "valid_until",
    "source_record_sha256",
    "capture_binding_sha256",
)
NATIVE_PRICE_REQUEST_RECEIPT_FIELDS = (
    "schema",
    "request_role",
    "request_method",
    "source_endpoint_id",
    "request_path",
    "request_query",
    "captured_at",
    "raw_response_sha256",
    "projection_sha256",
)
NATIVE_PRICE_BOOK_FIELDS = (
    "schema",
    "market_id",
    "adapter_id",
    "best_ask_price",
    "best_ask_quantity",
    "observed_at",
    "raw_response_sha256",
)
NATIVE_PRICE_MARKET_RULES_FIELDS = (
    "schema",
    "market_id",
    "price_tick",
    "quantity_step",
    "min_quantity",
    "min_notional",
    "observed_at",
    "source_record_sha256",
)
NATIVE_PRICE_USD_CONVERSION_FIELDS = (
    "schema",
    "quote_asset",
    "usd_asset",
    "rate",
    "observed_at",
    "valid_until",
    "source_record_sha256",
)
ESTIMATE_GAS_REQUEST_FIELDS = ("schema", "jsonrpc", "id", "method", "params")
ESTIMATE_GAS_RESPONSE_FIELDS = ("schema", "jsonrpc", "id", "result")
TRACE_REQUEST_FIELDS = ("schema", "jsonrpc", "id", "method", "params")
TRACE_RESPONSE_FIELDS = ("schema", "jsonrpc", "id", "storage_diffs")
ESTIMATE_CALL_OBJECT_FIELDS = ("from", "to", "data", "value")
TRACE_CALL_OBJECT_FIELDS = ("from", "to", "gas", "data", "value")
TRACE_OPTIONS_FIELDS = ("tracer", "tracerConfig", "stateOverrides")
TRACE_CONFIG_FIELDS = ("diffMode", "disableCode", "disableStorage")
BALANCE_OVERRIDE_FIELDS = ("balance",)
STATE_DIFF_OVERRIDE_FIELDS = ("stateDiff",)
BALANCE_DELTA_FIELDS = (
    "token_address",
    "account_role",
    "pre_balance_raw",
    "post_balance_raw",
)
STORAGE_DIFF_FIELDS = (
    "token_address",
    "account_role",
    "storage_key",
    "pre_present",
    "pre_value",
    "post_present",
    "post_value",
)
POLICY_ATTESTATION_FIELDS = (
    "schema",
    "run_id",
    "route_cohort_id",
    "candidate_source_generation",
    "route_universe_sha256",
    "selected_market_set_sha256",
    "adapter_registry_sha256",
    "connector_key_registry_sha256",
    "trace_profile_generation",
    "submission_connector_profile_generation",
    "connector_id",
    "member_count",
    "member_set_sha256",
    "observed_at",
    "valid_until",
)

_SHA256 = re.compile(r"[0-9a-f]{64}\Z", flags=re.ASCII)
_ADDRESS = re.compile(r"0x[0-9a-f]{40}\Z", flags=re.ASCII)
_HASH32 = re.compile(r"0x[0-9a-f]{64}\Z", flags=re.ASCII)
_HEX_BYTES = re.compile(r"0x(?:[0-9a-f]{2})*\Z", flags=re.ASCII)
_HEX_QUANTITY = re.compile(r"(?:0x0|0x[1-9a-f][0-9a-f]*)\Z", flags=re.ASCII)
_POSITIVE_HEX_QUANTITY = re.compile(r"0x[1-9a-f][0-9a-f]*\Z", flags=re.ASCII)
_DECIMAL = re.compile(r"(?:0|[1-9][0-9]*)(?:\.[0-9]*[1-9])?\Z", flags=re.ASCII)
_POSITIVE_INTEGER_TEXT = re.compile(r"[1-9][0-9]*\Z", flags=re.ASCII)
_NONNEGATIVE_INTEGER_TEXT = re.compile(r"(?:0|[1-9][0-9]*)\Z", flags=re.ASCII)
_SAFE_TEXT = re.compile(r"[A-Za-z0-9][A-Za-z0-9._:/-]{0,255}\Z", flags=re.ASCII)
_SAFE_RUN_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z", flags=re.ASCII)
_COHORT_ID = re.compile(r"cohort:[0-9a-f]{64}\Z", flags=re.ASCII)
_LOWER_ID = re.compile(r"[a-z0-9][a-z0-9._-]{0,127}\Z", flags=re.ASCII)
_CONNECTOR_ID = re.compile(r"[a-z0-9][a-z0-9_-]{0,63}\Z", flags=re.ASCII)
_POLICY_ID = re.compile(r"[a-z0-9][a-z0-9._-]{0,63}\Z", flags=re.ASCII)
_TYPED_MEMBER_FILENAME = re.compile(
    r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z", flags=re.ASCII
)
_DEX_MARKET_ID = re.compile(
    r"dex:([a-z0-9][a-z0-9._-]{0,63}):"
    r"([a-z0-9][a-z0-9._-]{0,127}):"
    r"(0x[0-9a-f]{40}):"
    r"([A-Z0-9][A-Z0-9._-]{0,63})\Z",
    flags=re.ASCII,
)


class RouteCostEvidenceError(ValueError):
    """Raised when an evidence object cannot be replayed exactly."""


_SSHSIG_BEGIN = "-----BEGIN SSH SIGNATURE-----"
_SSHSIG_END = "-----END SSH SIGNATURE-----"


def _sshsig_bytes(value: Any) -> bytes:
    if not isinstance(value, str):
        raise RouteCostEvidenceError("SSHSIG is invalid")
    try:
        encoded = value.encode("ascii")
    except UnicodeEncodeError as error:
        raise RouteCostEvidenceError("SSHSIG is not ASCII") from error
    if len(encoded) > MAX_SIGNATURE_BYTES or "\r" in value:
        raise RouteCostEvidenceError("SSHSIG exceeds its byte limit")
    lines = value.splitlines()
    if (
        len(lines) < 3
        or lines[0] != _SSHSIG_BEGIN
        or lines[-1] != _SSHSIG_END
        or any(not line or len(line) > 76 for line in lines[1:-1])
    ):
        raise RouteCostEvidenceError("SSHSIG armor is invalid")
    try:
        raw = base64.b64decode("".join(lines[1:-1]), validate=True)
    except (ValueError, binascii.Error) as error:
        raise RouteCostEvidenceError("SSHSIG armor is invalid") from error

    def take_string(buffer: bytes, offset: int) -> Tuple[bytes, int]:
        if offset + 4 > len(buffer):
            raise RouteCostEvidenceError("SSHSIG is truncated")
        length = int.from_bytes(buffer[offset:offset + 4], "big")
        offset += 4
        if offset + length > len(buffer):
            raise RouteCostEvidenceError("SSHSIG is truncated")
        return buffer[offset:offset + length], offset + length

    if not raw.startswith(b"SSHSIG") or len(raw) < 10:
        raise RouteCostEvidenceError("SSHSIG magic is invalid")
    offset = 6
    version = int.from_bytes(raw[offset:offset + 4], "big")
    offset += 4
    if version != 1:
        raise RouteCostEvidenceError("SSHSIG version is invalid")
    public_key_blob, offset = take_string(raw, offset)
    namespace, offset = take_string(raw, offset)
    reserved, offset = take_string(raw, offset)
    hash_algorithm, offset = take_string(raw, offset)
    signature_blob, offset = take_string(raw, offset)
    if (
        namespace != b"route-cost-submission-policy-v1"
        or reserved != b""
        or hash_algorithm != b"sha512"
        or offset != len(raw)
    ):
        raise RouteCostEvidenceError("SSHSIG namespace/hash is invalid")
    key_kind, key_offset = take_string(public_key_blob, 0)
    key_material, key_offset = take_string(public_key_blob, key_offset)
    signature_kind, signature_offset = take_string(signature_blob, 0)
    signature_material, signature_offset = take_string(
        signature_blob, signature_offset
    )
    if (
        key_kind != b"ssh-ed25519"
        or len(key_material) != 32
        or key_offset != len(public_key_blob)
        or signature_kind != b"ssh-ed25519"
        or len(signature_material) != 64
        or signature_offset != len(signature_blob)
    ):
        raise RouteCostEvidenceError("SSHSIG is not canonical Ed25519")
    return public_key_blob


def canonical_json_bytes(value: Any) -> bytes:
    """Return the contract's compact sorted-key UTF-8 JSON bytes."""
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError) as error:
        raise RouteCostEvidenceError("value is not canonical JSON") from error


def physical_sha256(value: Any) -> str:
    return hashlib.sha256(canonical_json_bytes(value)).hexdigest()


def typed_sha256(domain: bytes, value: Any) -> str:
    if not isinstance(domain, bytes) or not domain or not domain.endswith(b"\n"):
        raise RouteCostEvidenceError("typed hash domain is invalid")
    return hashlib.sha256(domain + canonical_json_bytes(value) + b"\n").hexdigest()


def _exact_fields(value: Any, fields: Sequence[str], label: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping) or set(value) != set(fields):
        raise RouteCostEvidenceError("{} schema is invalid".format(label))
    return value


def _sha256(value: Any, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise RouteCostEvidenceError("{} is invalid".format(label))
    return value


def _address(value: Any, label: str) -> str:
    if not isinstance(value, str) or _ADDRESS.fullmatch(value) is None:
        raise RouteCostEvidenceError("{} is invalid".format(label))
    return value


def _hash32(value: Any, label: str) -> str:
    if not isinstance(value, str) or _HASH32.fullmatch(value) is None:
        raise RouteCostEvidenceError("{} is invalid".format(label))
    return value


def _timestamp(value: Any, label: str) -> str:
    if not isinstance(value, str):
        raise RouteCostEvidenceError("{} is invalid".format(label))
    try:
        exact_rfc3339_epoch_seconds(value)
    except (TypeError, ValueError) as error:
        raise RouteCostEvidenceError("{} is invalid".format(label)) from error
    return value


def _timestamp_value(value: Any, label: str):
    text = _timestamp(value, label)
    try:
        return exact_rfc3339_epoch_seconds(text)
    except (TypeError, ValueError) as error:  # pragma: no cover - guarded above
        raise RouteCostEvidenceError("{} is invalid".format(label)) from error


def _ordered_timestamps(start: Any, finish: Any, label: str) -> Tuple[str, str]:
    start_text = _timestamp(start, label + " start")
    finish_text = _timestamp(finish, label + " finish")
    if _timestamp_value(start_text, label + " start") > _timestamp_value(
        finish_text, label + " finish"
    ):
        raise RouteCostEvidenceError("{} timestamps are reversed".format(label))
    return start_text, finish_text


def _exact_int(value: Any, label: str, minimum: int = 0, maximum: Optional[int] = None) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
        raise RouteCostEvidenceError("{} is invalid".format(label))
    if maximum is not None and value > maximum:
        raise RouteCostEvidenceError("{} is invalid".format(label))
    return value


def _decimal_text(
    value: Any,
    label: str,
    *,
    positive: bool = False,
    maximum: Optional[Decimal] = None,
) -> str:
    if not isinstance(value, str) or _DECIMAL.fullmatch(value) is None:
        raise RouteCostEvidenceError("{} is invalid".format(label))
    try:
        number = Decimal(value)
    except (InvalidOperation, ValueError) as error:
        raise RouteCostEvidenceError("{} is invalid".format(label)) from error
    if (
        not number.is_finite()
        or number < 0
        or (positive and number <= 0)
        or (number.is_zero() and number.is_signed())
        or (maximum is not None and number > maximum)
    ):
        raise RouteCostEvidenceError("{} is invalid".format(label))
    canonical = format(number, "f")
    if "." in canonical:
        canonical = canonical.rstrip("0").rstrip(".")
    if canonical != value:
        raise RouteCostEvidenceError("{} is not canonical".format(label))
    return value


def _integer_text(value: Any, label: str, *, positive: bool = False) -> str:
    pattern = _POSITIVE_INTEGER_TEXT if positive else _NONNEGATIVE_INTEGER_TEXT
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise RouteCostEvidenceError("{} is invalid".format(label))
    return value


def _quantity(value: Any, label: str, *, positive: bool = False) -> str:
    pattern = _POSITIVE_HEX_QUANTITY if positive else _HEX_QUANTITY
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise RouteCostEvidenceError("{} is invalid".format(label))
    return value


def _required_text(value: Any, label: str, pattern: re.Pattern = _SAFE_TEXT) -> str:
    if not isinstance(value, str) or pattern.fullmatch(value) is None:
        raise RouteCostEvidenceError("{} is invalid".format(label))
    return value


def _canonical_copy(value: Any) -> Any:
    return json.loads(canonical_json_bytes(value).decode("utf-8"))


def _decoded_hex_bytes(value: Any, label: str, maximum: int) -> bytes:
    if not isinstance(value, str) or _HEX_BYTES.fullmatch(value) is None:
        raise RouteCostEvidenceError("{} is invalid".format(label))
    raw = bytes.fromhex(value[2:])
    if len(raw) > maximum:
        raise RouteCostEvidenceError("{} exceeds its byte limit".format(label))
    return raw


def validate_retained_v2_pool_state_member(
    payload: bytes, *, descriptor: Mapping[str, Any]
) -> Dict[str, Any]:
    """Replay one descriptor-reread ``dex_pool_state`` member.

    ``payload`` is the exact bounded byte string already read through the
    retained typed-source descriptor by the publication boundary.  This helper
    accepts neither paths nor a pre-derived projection: it recomputes the
    physical SHA, exact JSON contract, and the dataclass-derived ``state_id``.
    """
    _exact_fields(
        descriptor,
        RETAINED_TYPED_SOURCE_DESCRIPTOR_FIELDS,
        "retained V2 descriptor",
    )
    market_id = descriptor.get("market_id")
    match = _DEX_MARKET_ID.fullmatch(str(market_id))
    if (
        match is None
        or match.group(1) != "eth"
        or match.group(2) != "uniswap_v2"
        or descriptor.get("role") != "dex_pool_state"
        or descriptor.get("adapter_id")
        != "route_quantity_quote_for_v2_pool/v1"
        or descriptor.get("content_schema") != ROUTE_V2_POOL_STATE_SCHEMA
        or not isinstance(descriptor.get("filename"), str)
        or _TYPED_MEMBER_FILENAME.fullmatch(descriptor["filename"]) is None
    ):
        raise RouteCostEvidenceError("retained V2 descriptor identity is invalid")
    if type(payload) is not bytes or not 0 < len(payload) <= MAX_RETAINED_V2_POOL_STATE_BYTES:
        raise RouteCostEvidenceError("retained V2 payload size is invalid")
    size = _exact_int(
        descriptor.get("size"),
        "retained V2 descriptor size",
        1,
        MAX_RETAINED_V2_POOL_STATE_BYTES,
    )
    if size != len(payload) or descriptor.get("sha256") != hashlib.sha256(
        payload
    ).hexdigest():
        raise RouteCostEvidenceError("retained V2 descriptor bytes differ")
    _sha256(
        descriptor.get("logical_generation"),
        "retained V2 logical generation",
    )
    try:
        decoded = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RouteCostEvidenceError("retained V2 payload is invalid JSON") from error
    _exact_fields(decoded, V2_POOL_STATE_FIELDS, "retained V2 pool state")
    if decoded.get("schema") != ROUTE_V2_POOL_STATE_SCHEMA:
        raise RouteCostEvidenceError("retained V2 pool-state schema is invalid")
    if canonical_json_bytes(decoded) != payload:
        raise RouteCostEvidenceError("retained V2 payload is not canonical")
    if (
        decoded.get("chain") != "eth"
        or decoded.get("chain_id") != "1"
        or decoded.get("dex") != "uniswap_v2"
        or decoded.get("pool_address") != match.group(3)
    ):
        raise RouteCostEvidenceError("retained V2 market identity differs")
    try:
        from scripts.route_quantity import V2PoolState
    except ModuleNotFoundError:  # pragma: no cover - direct script execution
        from route_quantity import V2PoolState  # type: ignore
    integer_fields = {
        "chain_id", "token0_decimals", "token1_decimals", "reserve0_raw",
        "reserve1_raw", "reserve_timestamp_last_raw", "fee_bps",
        "fee_numerator", "fee_denominator", "block_number",
    }
    constructor_fields: Dict[str, Any] = {}
    for field in V2_POOL_STATE_FIELDS:
        if field in {"schema", "state_id"}:
            continue
        raw_value = decoded[field]
        if field in integer_fields:
            raw_value = int(_integer_text(
                raw_value, "retained V2 " + field,
                positive=field not in {
                    "token0_decimals", "token1_decimals",
                    "reserve_timestamp_last_raw", "fee_bps",
                },
            ))
        constructor_fields[field] = raw_value
    try:
        state = V2PoolState(**constructor_fields)
    except (TypeError, ValueError) as error:
        raise RouteCostEvidenceError("retained V2 pool state is invalid") from error
    if (
        decoded.get("state_id") != state.state_id
        or descriptor.get("logical_generation")
        != state.state_id.split(":", 1)[1]
    ):
        raise RouteCostEvidenceError("retained V2 state ID differs")
    return _canonical_copy(decoded)


def validate_adapter_registry(value: Mapping[str, Any]) -> Dict[str, Any]:
    _exact_fields(value, ADAPTER_REGISTRY_FIELDS, "adapter registry")
    if value.get("schema") != ROUTE_COST_ADAPTER_REGISTRY_SCHEMA:
        raise RouteCostEvidenceError("adapter registry schema is invalid")
    _required_text(value.get("registry_version"), "adapter registry version")
    adapters = value.get("adapters")
    if not isinstance(adapters, list) or len(adapters) > 64:
        raise RouteCostEvidenceError("adapter registry entries are invalid")
    adapter_ids: List[str] = []
    for item in adapters:
        _exact_fields(item, ADAPTER_FIELDS, "adapter")
        adapter_id = _required_text(item.get("adapter_id"), "adapter ID", _LOWER_ID)
        adapter_ids.append(adapter_id)
        if (
            adapter_id != ETHEREUM_V2_ADAPTER_ID
            or item.get("chain_id") != 1
            or item.get("protocol_family") != ETHEREUM_V2_PROTOCOL_FAMILY
            or item.get("router_address") != ETHEREUM_V2_ROUTER_ADDRESS
            or item.get("factory_address") != ETHEREUM_V2_FACTORY_ADDRESS
            or item.get("router_runtime_code_sha256")
            != ETHEREUM_V2_ROUTER_RUNTIME_CODE_SHA256
            or item.get("factory_runtime_code_sha256")
            != ETHEREUM_V2_FACTORY_RUNTIME_CODE_SHA256
            or item.get("pair_fee_bps") != "30"
            or item.get("gas_fee_model") != "eip1559_fee_history_v1"
            or item.get("allowed_selectors") != list(ETHEREUM_V2_ALLOWED_SELECTORS)
            or item.get("supports_native") is not False
            or item.get("supports_multihop") is not False
            or item.get("supports_fee_on_transfer") is not False
            or item.get("trace_method")
            != "debug_traceCall_state_override_v1"
            or item.get("connector_family")
            != "private_submission_connector/v1"
            or item.get("native_symbol") != "ETH"
            or item.get("wrapped_native_address") != ETHEREUM_WETH_ADDRESS
            or item.get("simulation_sender_address")
            != ETHEREUM_V2_SIMULATION_SENDER
            or item.get("native_price_reference_market_id")
            != "cex:binance:ETH/USDT"
            or item.get("native_price_reference_adapter_id")
            != "binance_public_spot_depth/v1"
        ):
            raise RouteCostEvidenceError("adapter identity is not allowlisted")
        pair_descriptors = item.get("pair_descriptors")
        if not isinstance(pair_descriptors, list) or len(pair_descriptors) > 256:
            raise RouteCostEvidenceError("pair descriptors are invalid")
        pair_addresses: List[str] = []
        for descriptor in pair_descriptors:
            _exact_fields(
                descriptor, PAIR_DESCRIPTOR_FIELDS, "pair descriptor"
            )
            pair = _address(
                descriptor.get("pair_address"), "pair descriptor address"
            )
            token0 = _address(
                descriptor.get("token0_address"), "pair descriptor token0"
            )
            token1 = _address(
                descriptor.get("token1_address"), "pair descriptor token1"
            )
            if token0 == token1:
                raise RouteCostEvidenceError(
                    "pair descriptor tokens are identical"
                )
            pair_addresses.append(pair)
            _sha256(
                descriptor.get("pair_runtime_code_sha256"),
                "pair runtime hash",
            )
            _sha256(
                descriptor.get("source_metadata_sha256"),
                "pair source metadata hash",
            )
        if pair_addresses != sorted(set(pair_addresses)):
            raise RouteCostEvidenceError("pair descriptors are not canonical")
        descriptors = item.get("token_funding_descriptors")
        if not isinstance(descriptors, list) or len(descriptors) > 256:
            raise RouteCostEvidenceError("funding descriptors are invalid")
        addresses: List[str] = []
        for descriptor in descriptors:
            _exact_fields(descriptor, FUNDING_DESCRIPTOR_FIELDS, "funding descriptor")
            token = _address(descriptor.get("token_address"), "funding token")
            addresses.append(token)
            _sha256(descriptor.get("runtime_code_sha256"), "funding runtime hash")
            if (
                descriptor.get("proxy_implementation_address") is not None
                or descriptor.get("proxy_implementation_code_sha256") is not None
                or descriptor.get("storage_layout") != "solidity_mapping_v1"
            ):
                raise RouteCostEvidenceError("funding descriptor proxy/layout is unsupported")
            for field in ("balance_mapping_slot", "allowance_mapping_slot"):
                raw = _integer_text(descriptor.get(field), "funding " + field)
                if int(raw) >= 1 << 256:
                    raise RouteCostEvidenceError("funding mapping slot is too large")
            _sha256(
                descriptor.get("source_metadata_sha256"),
                "funding source metadata hash",
            )
        if addresses != sorted(set(addresses)):
            raise RouteCostEvidenceError("funding descriptors are not canonical")
    if adapter_ids != sorted(set(adapter_ids)):
        raise RouteCostEvidenceError("adapter registry is not canonical")
    if len(canonical_json_bytes(value)) > MAX_ADAPTER_REGISTRY_BYTES:
        raise RouteCostEvidenceError("adapter registry exceeds its byte limit")
    return _canonical_copy(value)


def validate_route_cost_authority_record(
    value: Mapping[str, Any],
) -> Dict[str, Any]:
    """Validate one closed, offline static-descriptor authority record."""
    schema = value.get("schema") if isinstance(value, Mapping) else None
    if schema == ROUTE_COST_PAIR_AUTHORITY_SCHEMA:
        _exact_fields(value, PAIR_AUTHORITY_FIELDS, "pair authority record")
    elif schema == ROUTE_COST_TOKEN_FUNDING_AUTHORITY_SCHEMA:
        _exact_fields(
            value,
            TOKEN_FUNDING_AUTHORITY_FIELDS,
            "token funding authority record",
        )
    else:
        raise RouteCostEvidenceError("authority record schema is invalid")
    _required_text(value.get("authority_id"), "authority record ID", _LOWER_ID)
    if value.get("chain_id") != 1 or value.get("block_number") != 20_000_000:
        raise RouteCostEvidenceError("authority record chain/block is invalid")
    block_hash = _hash32(value.get("block_hash"), "authority block hash")
    if block_hash != (
        "0xd24fd73f794058a3807db926d8898c6481e902b7edb91ce0d479d6760f276183"
    ):
        raise RouteCostEvidenceError("authority block identity is invalid")
    state_root = _hash32(value.get("state_root"), "authority state root")
    if state_root != (
        "0x68421c2c599dc31396a09772a073fb421c4bd25ef1462914ef13e5dfa2d31c23"
    ):
        raise RouteCostEvidenceError("authority state root is invalid")
    _sha256(value.get("runtime_code_sha256"), "authority runtime hash")
    runtime_size = value.get("runtime_code_size")
    if not isinstance(runtime_size, int) or isinstance(runtime_size, bool) or not (
        1 <= runtime_size <= MAX_RUNTIME_CODE_BYTES
    ):
        raise RouteCostEvidenceError("authority runtime size is invalid")
    repository = value.get("source_repository")
    if repository not in {
        "https://github.com/Uniswap/v2-core",
        "https://github.com/Uniswap/governance",
        "https://github.com/gnosis/canonical-weth",
    }:
        raise RouteCostEvidenceError("authority source repository is invalid")
    commit = value.get("source_commit")
    if not isinstance(commit, str) or re.fullmatch(r"[0-9a-f]{40}", commit) is None:
        raise RouteCostEvidenceError("authority source commit is invalid")
    source_path = value.get("source_path")
    if not isinstance(source_path, str) or source_path not in {
        "contracts/UniswapV2Pair.sol", "contracts/Uni.sol", "contracts/WETH9.sol"
    }:
        raise RouteCostEvidenceError("authority source path is invalid")
    _sha256(value.get("source_sha256"), "authority source hash")

    if schema == ROUTE_COST_PAIR_AUTHORITY_SCHEMA:
        if (
            value.get("authority_id")
            != "ethereum-uniswap-v2-uni-weth-pair-block-20000000"
            or value.get("factory_address") != ETHEREUM_V2_FACTORY_ADDRESS
            or value.get("pair_address")
            != "0xd3d2e2692501a5c9ca623199d38826e513033a17"
            or value.get("token0_address")
            != "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984"
            or value.get("token1_address") != ETHEREUM_WETH_ADDRESS
            or value.get("runtime_code_sha256")
            != "8b5db55fa9ab3b9527508d4abe0b39eb588bf310270c8e04b3f38214e8ba63b4"
            or runtime_size != 11_293
            or repository != "https://github.com/Uniswap/v2-core"
            or commit != "6a9e7c97860676e0992f22a49665760444c1cdf5"
            or source_path != "contracts/UniswapV2Pair.sol"
            or value.get("source_sha256")
            != "43a5421b31415868367b62bfa161ca10bcee03778873faad905f5a3e2cce9cbd"
        ):
            raise RouteCostEvidenceError("pair authority identity is invalid")
        calldata = build_factory_get_pair_calldata(
            value["token0_address"], value["token1_address"]
        )
        if value.get("factory_get_pair_calldata") != calldata:
            raise RouteCostEvidenceError("pair authority calldata is invalid")
        for field, address in (
            ("factory_get_pair_result", value["pair_address"]),
            ("token0_result", value["token0_address"]),
            ("token1_result", value["token1_address"]),
        ):
            if value.get(field) != "0x" + "0" * 24 + address[2:]:
                raise RouteCostEvidenceError("pair authority result is invalid")
    else:
        token = _address(value.get("token_address"), "authority token")
        if value.get("storage_layout") != "solidity_mapping_v1":
            raise RouteCostEvidenceError("authority storage layout is invalid")
        balance_slot = _integer_text(
            value.get("balance_mapping_slot"), "authority balance slot"
        )
        allowance_slot = _integer_text(
            value.get("allowance_mapping_slot"), "authority allowance slot"
        )
        owner = _address(value.get("balance_probe_owner"), "authority probe owner")
        if value.get("balance_probe_storage_key") != solidity_balance_storage_key(
            owner, int(balance_slot)
        ):
            raise RouteCostEvidenceError("authority balance probe key is invalid")
        probe_getter = _word(
            value.get("balance_probe_getter_result"), "authority getter result"
        )
        probe_storage = _word(
            value.get("balance_probe_storage_result"), "authority storage result"
        )
        if probe_getter != probe_storage:
            raise RouteCostEvidenceError("authority balance probes differ")
        expected = {
            "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984": (
                "ethereum-uni-token-block-20000000", "4", "3",
                "77ea2b530607db6cb87c7cce18016aa12dd0762c4357355bceee2cb11721bebe",
                12_567, "https://github.com/Uniswap/governance",
                "eabd8c71ad01f61fb54ed6945162021ee419998e", "contracts/Uni.sol",
                "2c5e81aece21281888de638d37783cb9eca11649bbdf310e30ca0f8dbc6eb728",
            ),
            ETHEREUM_WETH_ADDRESS: (
                "ethereum-weth-token-block-20000000", "3", "4",
                "5566bf50796faf93c9b6f6adacd3b32c70bfe16b48ffc59db6cd144cbdc89739",
                3_124, "https://github.com/gnosis/canonical-weth",
                "0dd1ea3e295eef916d0c6223ec63141137d22d67", "contracts/WETH9.sol",
                "097d1a4258c78e1062798419ecb9c4e60b7327de5213be4bedfa4c1fdd04aa95",
            ),
        }.get(token)
        actual = (
            value.get("authority_id"), balance_slot, allowance_slot,
            value.get("runtime_code_sha256"), runtime_size, repository, commit,
            source_path, value.get("source_sha256"),
        )
        if expected is None or actual != expected:
            raise RouteCostEvidenceError("token authority identity is invalid")
    return _canonical_copy(value)


def _decode_openssh_ed25519_public_key(value: str) -> bytes:
    parts = value.split(" ")
    if len(parts) != 2 or parts[0] != "ssh-ed25519" or not parts[1]:
        raise RouteCostEvidenceError("connector public key is invalid")
    try:
        raw = base64.b64decode(parts[1], validate=True)
    except (ValueError, binascii.Error) as error:
        raise RouteCostEvidenceError("connector public key is invalid") from error

    def take_string(buffer: bytes, offset: int) -> Tuple[bytes, int]:
        if offset + 4 > len(buffer):
            raise RouteCostEvidenceError("connector public key is truncated")
        length = int.from_bytes(buffer[offset:offset + 4], "big")
        offset += 4
        if offset + length > len(buffer):
            raise RouteCostEvidenceError("connector public key is truncated")
        return buffer[offset:offset + length], offset + length

    kind, offset = take_string(raw, 0)
    key, offset = take_string(raw, offset)
    if kind != b"ssh-ed25519" or len(key) != 32 or offset != len(raw):
        raise RouteCostEvidenceError("connector public key is not Ed25519")
    return key


def validate_connector_key_registry(value: Mapping[str, Any]) -> Dict[str, Any]:
    _exact_fields(value, CONNECTOR_KEY_REGISTRY_FIELDS, "connector key registry")
    if value.get("schema") != ROUTE_COST_CONNECTOR_KEY_REGISTRY_SCHEMA:
        raise RouteCostEvidenceError("connector key registry schema is invalid")
    _required_text(value.get("registry_version"), "connector registry version")
    keys = value.get("keys")
    if not isinstance(keys, list) or len(keys) > 64:
        raise RouteCostEvidenceError("connector key registry entries are invalid")
    identities: List[Tuple[str, str]] = []
    for item in keys:
        _exact_fields(item, CONNECTOR_KEY_FIELDS, "connector key")
        key_id = _required_text(item.get("key_id"), "connector key ID", _LOWER_ID)
        connector_id = _required_text(
            item.get("connector_id"), "connector ID", _CONNECTOR_ID
        )
        identities.append((key_id, connector_id))
        if item.get("algorithm") != "ssh-ed25519-sshsig-v1":
            raise RouteCostEvidenceError("connector key algorithm is invalid")
        public_key = item.get("public_key")
        if not isinstance(public_key, str):
            raise RouteCostEvidenceError("connector public key is invalid")
        _decode_openssh_ed25519_public_key(public_key)
        start = _timestamp_value(item.get("valid_from"), "connector key valid_from")
        finish = _timestamp_value(item.get("valid_until"), "connector key valid_until")
        if start >= finish:
            raise RouteCostEvidenceError("connector key validity is reversed")
        if item.get("status") not in {"active", "retired"}:
            raise RouteCostEvidenceError("connector key status is invalid")
    if identities != sorted(set(identities)):
        raise RouteCostEvidenceError("connector keys are not canonical")
    if len(canonical_json_bytes(value)) > MAX_CONNECTOR_KEY_REGISTRY_BYTES:
        raise RouteCostEvidenceError("connector key registry exceeds its byte limit")
    return _canonical_copy(value)


def _load_tracked_registry(
    path: Path,
    *,
    maximum_bytes: int,
    expected_value: Optional[Mapping[str, Any]],
    validator: Any,
    label: str,
) -> Dict[str, Any]:
    """Descriptor-load one fixed tracked registry through a held safe ancestry."""
    absolute = Path(os.fspath(path))
    if not absolute.is_absolute() or any(
        component in {"", ".", ".."} for component in absolute.parts[1:]
    ):
        raise RouteCostEvidenceError("{} path is unsafe".format(label))
    nofollow = getattr(os, "O_NOFOLLOW", None)
    directory = getattr(os, "O_DIRECTORY", None)
    if nofollow is None or directory is None:
        raise RouteCostEvidenceError("secure {} open is unavailable".format(label))
    directory_flags = (
        os.O_RDONLY | nofollow | directory | getattr(os, "O_CLOEXEC", 0)
    )
    file_flags = os.O_RDONLY | nofollow | getattr(os, "O_CLOEXEC", 0)
    # Every successful open transfers ownership to this one cleanup stack
    # immediately.  In particular, no metadata probe may run while a freshly
    # opened descriptor is still outside the stack: fstat/stat can fail or be
    # interrupted just as readily as open/read.
    descriptors: List[int] = []
    ancestry: List[Tuple[int, str, int, Tuple[int, int, int, int]]] = []
    try:
        root_descriptor = os.open(os.sep, directory_flags)
        descriptors.append(root_descriptor)
        parent_descriptor = root_descriptor
        for component in absolute.parts[1:-1]:
            child_descriptor = os.open(
                component, directory_flags, dir_fd=parent_descriptor
            )
            descriptors.append(child_descriptor)
            metadata = os.fstat(child_descriptor)
            path_metadata = os.stat(
                component, dir_fd=parent_descriptor, follow_symlinks=False
            )
            if (
                not stat.S_ISDIR(metadata.st_mode)
                or (metadata.st_dev, metadata.st_ino)
                != (path_metadata.st_dev, path_metadata.st_ino)
            ):
                raise RouteCostEvidenceError(
                    "{} ancestor identity is unsafe".format(label)
                )
            snapshot = (
                metadata.st_dev,
                metadata.st_ino,
                metadata.st_ctime_ns,
                metadata.st_mtime_ns,
            )
            ancestry.append(
                (parent_descriptor, component, child_descriptor, snapshot)
            )
            parent_descriptor = child_descriptor

        leaf_name = absolute.parts[-1]
        descriptor = os.open(leaf_name, file_flags, dir_fd=parent_descriptor)
        descriptors.append(descriptor)
        metadata = os.fstat(descriptor)
        path_metadata = os.stat(
            leaf_name, dir_fd=parent_descriptor, follow_symlinks=False
        )
        if (
            not stat.S_ISREG(metadata.st_mode)
            or metadata.st_nlink != 1
            or metadata.st_size <= 0
            or metadata.st_size > maximum_bytes
            or (metadata.st_dev, metadata.st_ino)
            != (path_metadata.st_dev, path_metadata.st_ino)
        ):
            raise RouteCostEvidenceError("{} file identity is unsafe".format(label))
        data = bytearray()
        while True:
            chunk = os.read(descriptor, min(65536, maximum_bytes + 1 - len(data)))
            if not chunk:
                break
            data.extend(chunk)
            if len(data) > maximum_bytes:
                raise RouteCostEvidenceError("{} exceeds its byte limit".format(label))
        final_metadata = os.fstat(descriptor)
        if (
            final_metadata.st_dev != metadata.st_dev
            or final_metadata.st_ino != metadata.st_ino
            or final_metadata.st_size != metadata.st_size
            or final_metadata.st_nlink != metadata.st_nlink
            or final_metadata.st_ctime_ns != metadata.st_ctime_ns
            or final_metadata.st_mtime_ns != metadata.st_mtime_ns
        ):
            raise RouteCostEvidenceError("{} changed while reading".format(label))
        final_path_metadata = os.stat(
            leaf_name, dir_fd=parent_descriptor, follow_symlinks=False
        )
        if (
            not stat.S_ISREG(final_path_metadata.st_mode)
            or (final_path_metadata.st_dev, final_path_metadata.st_ino)
            != (metadata.st_dev, metadata.st_ino)
        ):
            raise RouteCostEvidenceError("{} changed while reading".format(label))
        for (
            ancestor_parent,
            component,
            ancestor_descriptor,
            snapshot,
        ) in ancestry:
            current = os.fstat(ancestor_descriptor)
            current_path = os.stat(
                component,
                dir_fd=ancestor_parent,
                follow_symlinks=False,
            )
            current_snapshot = (
                current.st_dev,
                current.st_ino,
                current.st_ctime_ns,
                current.st_mtime_ns,
            )
            if (
                not stat.S_ISDIR(current.st_mode)
                or current_snapshot != snapshot
                or (current_path.st_dev, current_path.st_ino)
                != (current.st_dev, current.st_ino)
            ):
                raise RouteCostEvidenceError(
                    "{} ancestor changed while reading".format(label)
                )
    except OSError as error:
        raise RouteCostEvidenceError("{} is unavailable or unsafe".format(label)) from error
    finally:
        primary_active = sys.exc_info()[0] is not None
        cleanup_failure: Optional[BaseException] = None
        for held_descriptor in reversed(descriptors):
            try:
                os.close(held_descriptor)
            except BaseException as error:
                # An uncertain close result must never trigger a retry: the FD
                # number may already have been released and reused.  Continue
                # through the remaining owned descriptors and preserve any
                # exception already in flight.
                if cleanup_failure is None:
                    cleanup_failure = error
        if not primary_active and cleanup_failure is not None:
            if isinstance(cleanup_failure, (KeyboardInterrupt, SystemExit)):
                raise cleanup_failure
            raise RouteCostEvidenceError(
                "{} descriptor cleanup failed".format(label)
            )
    try:
        text = bytes(data).decode("utf-8")
        value = json.loads(text)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RouteCostEvidenceError("{} JSON is invalid".format(label)) from error
    if bytes(data) != canonical_json_bytes(value) + b"\n":
        raise RouteCostEvidenceError("{} bytes are not canonical LF JSON".format(label))
    normalized = validator(value)
    if expected_value is not None and normalized != expected_value:
        raise RouteCostEvidenceError("{} differs from the tracked identity".format(label))
    return normalized


def _reread_adapter_authority_records(
    registry: Mapping[str, Any],
) -> None:
    """Bind each production descriptor to one safely reread authority file."""
    for adapter in registry["adapters"]:
        for kind, descriptors in (
            ("pair", adapter["pair_descriptors"]),
            ("token", adapter["token_funding_descriptors"]),
        ):
            address_field = "pair_address" if kind == "pair" else "token_address"
            for descriptor in descriptors:
                address = descriptor[address_field]
                path = _AUTHORITY_RECORD_ROOT / "{}-{}.json".format(kind, address)
                record = _load_tracked_registry(
                    path,
                    maximum_bytes=MAX_AUTHORITY_RECORD_BYTES,
                    expected_value=None,
                    validator=validate_route_cost_authority_record,
                    label="route-cost {} authority record".format(kind),
                )
                record_sha = hashlib.sha256(
                    canonical_json_bytes(record) + b"\n"
                ).hexdigest()
                if record_sha != descriptor["source_metadata_sha256"]:
                    raise RouteCostEvidenceError(
                        "route-cost authority record hash differs"
                    )
                if kind == "pair":
                    expected = {
                        "pair_address": record["pair_address"],
                        "token0_address": record["token0_address"],
                        "token1_address": record["token1_address"],
                        "pair_runtime_code_sha256": record[
                            "runtime_code_sha256"
                        ],
                        "source_metadata_sha256": record_sha,
                    }
                else:
                    expected = {
                        "token_address": record["token_address"],
                        "runtime_code_sha256": record[
                            "runtime_code_sha256"
                        ],
                        "proxy_implementation_address": None,
                        "proxy_implementation_code_sha256": None,
                        "storage_layout": record["storage_layout"],
                        "balance_mapping_slot": record[
                            "balance_mapping_slot"
                        ],
                        "allowance_mapping_slot": record[
                            "allowance_mapping_slot"
                        ],
                        "source_metadata_sha256": record_sha,
                    }
                if descriptor != expected:
                    raise RouteCostEvidenceError(
                        "route-cost descriptor differs from authority record"
                    )


def load_route_cost_adapter_registry() -> Dict[str, Any]:
    """Load the one checked-in production adapter registry."""
    registry = _load_tracked_registry(
        _ADAPTER_REGISTRY_PATH,
        maximum_bytes=MAX_ADAPTER_REGISTRY_BYTES,
        expected_value=_TRACKED_ADAPTER_REGISTRY,
        validator=validate_adapter_registry,
        label="route-cost adapter registry",
    )
    _reread_adapter_authority_records(registry)
    return registry


def load_route_cost_connector_key_registry() -> Dict[str, Any]:
    """Load the one checked-in production connector-key registry."""
    return _load_tracked_registry(
        _CONNECTOR_KEY_REGISTRY_PATH,
        maximum_bytes=MAX_CONNECTOR_KEY_REGISTRY_BYTES,
        expected_value=_TRACKED_CONNECTOR_KEY_REGISTRY,
        validator=validate_connector_key_registry,
        label="route-cost connector-key registry",
    )


def _validated_url(value: Any, label: str, *, origin_only: bool) -> str:
    if not isinstance(value, str) or not value or len(value.encode("utf-8")) > 4096:
        raise RouteCostEvidenceError("{} is invalid".format(label))
    if any(ord(character) < 0x20 or ord(character) == 0x7F for character in value):
        raise RouteCostEvidenceError("{} contains controls".format(label))
    parsed = urlsplit(value)
    if parsed.username is not None or parsed.password is not None or parsed.fragment:
        raise RouteCostEvidenceError("{} authority is unsafe".format(label))
    host = (parsed.hostname or "").lower()
    loopback = host in {"localhost", "127.0.0.1", "::1"}
    if parsed.scheme != "https" and not (parsed.scheme == "http" and loopback):
        raise RouteCostEvidenceError("{} must use HTTPS or loopback".format(label))
    if not host or parsed.query:
        raise RouteCostEvidenceError("{} query/host is invalid".format(label))
    if origin_only and parsed.path not in {"", "/"}:
        raise RouteCostEvidenceError("{} must be an origin URL".format(label))
    return value


def _authorization(value: Any, label: str) -> str:
    if not isinstance(value, str) or not (1 <= len(value.encode("utf-8")) <= 4096):
        raise RouteCostEvidenceError("{} is invalid".format(label))
    if any(ord(character) < 0x20 or ord(character) == 0x7F for character in value):
        raise RouteCostEvidenceError("{} contains controls".format(label))
    return value


def trace_profile_identity(
    profile: Optional[Mapping[str, Any]],
) -> Tuple[Dict[str, Any], str]:
    if profile is None:
        identity = {
            "schema": "route_cost_trace_profile_identity/v1",
            "status": "missing",
            "profile_id": None,
            "endpoint_id": None,
        }
    else:
        fields = ("schema", "profile_id", "endpoint_id", "rpc_url", "authorization")
        _exact_fields(profile, fields, "trace profile")
        if profile.get("schema") != "route_cost_trace_rpc_profile/v1":
            raise RouteCostEvidenceError("trace profile schema is invalid")
        profile_id = _required_text(profile.get("profile_id"), "trace profile ID", _LOWER_ID)
        endpoint_id = _required_text(profile.get("endpoint_id"), "trace endpoint ID", _LOWER_ID)
        _validated_url(profile.get("rpc_url"), "trace RPC URL", origin_only=False)
        _authorization(profile.get("authorization"), "trace authorization")
        if len(canonical_json_bytes(profile)) > MAX_PROFILE_BYTES:
            raise RouteCostEvidenceError("trace profile exceeds its byte limit")
        identity = {
            "schema": "route_cost_trace_profile_identity/v1",
            "status": "available",
            "profile_id": profile_id,
            "endpoint_id": endpoint_id,
        }
    generation = typed_sha256(
        b"route-cost-trace-profile-identity/v1\n", identity
    )
    return identity, generation


def submission_connector_profile_identity(
    profile: Optional[Mapping[str, Any]],
) -> Tuple[Dict[str, Any], str]:
    if profile is None:
        identity = {
            "schema": "route_cost_submission_connector_identity/v1",
            "status": "missing",
            "profile_id": None,
            "connector_id": None,
        }
    else:
        fields = (
            "schema",
            "profile_id",
            "connector_id",
            "endpoint_url",
            "authorization",
        )
        _exact_fields(profile, fields, "submission connector profile")
        if profile.get("schema") != "route_cost_submission_connector_profile/v1":
            raise RouteCostEvidenceError("submission connector profile schema is invalid")
        profile_id = _required_text(
            profile.get("profile_id"), "connector profile ID", _LOWER_ID
        )
        connector_id = _required_text(
            profile.get("connector_id"), "connector ID", _CONNECTOR_ID
        )
        _validated_url(
            profile.get("endpoint_url"), "connector endpoint URL", origin_only=True
        )
        _authorization(profile.get("authorization"), "connector authorization")
        if len(canonical_json_bytes(profile)) > MAX_PROFILE_BYTES:
            raise RouteCostEvidenceError("connector profile exceeds its byte limit")
        identity = {
            "schema": "route_cost_submission_connector_identity/v1",
            "status": "available",
            "profile_id": profile_id,
            "connector_id": connector_id,
        }
    generation = typed_sha256(
        b"route-cost-submission-connector-identity/v1\n", identity
    )
    return identity, generation


def _canonical_optional_decimal(value: Any, label: str) -> Optional[str]:
    if value is None:
        return None
    return _decimal_text(value, label)


def _market_token_addresses(leg: Mapping[str, Any]) -> Optional[Tuple[str, str]]:
    context = leg.get("collector_context")
    if not isinstance(context, Mapping):
        return None
    values = []
    for field in ("base_token_id", "quote_token_id"):
        token_id = context.get(field)
        if not isinstance(token_id, str) or not token_id.startswith("eth_0x"):
            return None
        address = token_id[4:]
        if _ADDRESS.fullmatch(address) is None:
            return None
        values.append(address)
    if values[0] == values[1]:
        return None
    return values[0], values[1]


def _strict_dex_target_identity(
    leg: Mapping[str, Any],
) -> Optional[Tuple[str, str, str, str]]:
    """Return target/other address and USD price from one observed DEX leg."""
    context = leg.get("collector_context")
    pool_tokens = _market_token_addresses(leg)
    target_address = leg.get("target_token_address")
    side = leg.get("target_token_side")
    if (
        not isinstance(context, Mapping)
        or context.get("status") != "observed"
        or pool_tokens is None
        or not isinstance(target_address, str)
        or _ADDRESS.fullmatch(target_address) is None
        or side not in {"base", "quote"}
    ):
        return None
    side_index = 0 if side == "base" else 1
    if target_address != pool_tokens[side_index]:
        raise RouteCostEvidenceError("selected target side/address differs")
    other_index = 1 - side_index
    other_price = _decimal_text(
        context.get(("base" if other_index == 0 else "quote") + "_token_price_usd"),
        "selected other-token price",
        positive=True,
    )
    return target_address, pool_tokens[other_index], other_price, side


def build_simulation_targets(
    universe_legs: Sequence[Mapping[str, Any]],
    selected: Mapping[str, Mapping[str, Any]],
    retained_pool_states: Mapping[str, Mapping[str, Any]],
) -> Dict[Tuple[str, str], Dict[str, str]]:
    """Replay one cohort-global strict DEX target per token/notional.

    A target-token group is emitted only when every selected market in that
    group has observed USD identity and retained state.  An unrelated token
    group cannot poison otherwise complete targets; all transcript inventories
    remain present either way.
    """
    if not isinstance(universe_legs, Sequence) or isinstance(
        universe_legs, (str, bytes, bytearray)
    ):
        raise RouteCostEvidenceError("selected-leg inventory is invalid")
    supported = {
        market_id
        for market_id, row in selected.items()
        if row.get("structural_support_status") == "supported"
    }
    legs_by_market: Dict[str, Mapping[str, Any]] = {}
    for leg in universe_legs:
        if not isinstance(leg, Mapping):
            raise RouteCostEvidenceError("selected DEX leg is invalid")
        market_id = leg.get("market_id")
        if market_id not in supported:
            continue
        if market_id in legs_by_market:
            raise RouteCostEvidenceError("selected DEX leg is duplicated")
        legs_by_market[str(market_id)] = leg
    if set(legs_by_market) != supported:
        raise RouteCostEvidenceError("selected DEX leg inventory differs")
    by_token: Dict[str, List[Fraction]] = {}
    decimals_by_token: Dict[str, int] = {}
    markets_by_token: Dict[str, List[str]] = {}
    failed_tokens: set = set()
    for market_id in sorted(supported):
        declared_target = legs_by_market[market_id].get("target_token_address")
        if not isinstance(declared_target, str) or _ADDRESS.fullmatch(
            declared_target
        ) is None:
            raise RouteCostEvidenceError("selected target identity is invalid")
        identity = _strict_dex_target_identity(legs_by_market[market_id])
        if identity is None or market_id not in retained_pool_states:
            failed_tokens.add(declared_target)
            markets_by_token.setdefault(declared_target, []).append(market_id)
            continue
        target, other, other_price_text, _side = identity
        state = retained_pool_states[market_id]
        if abs(
            _timestamp_value(
                legs_by_market[market_id]["collector_context"].get("observed_at"),
                "simulation USD context observed_at",
            )
            - _timestamp_value(
                state.get("observed_at"), "simulation pool-state observed_at"
            )
        ) > 2 * 60 * 60:
            failed_tokens.add(target)
            markets_by_token.setdefault(target, []).append(market_id)
            continue
        token0 = _address(state.get("token0_address"), "state token0")
        token1 = _address(state.get("token1_address"), "state token1")
        if {target, other} != {token0, token1}:
            raise RouteCostEvidenceError("selected target differs from retained pool")
        target_is_token0 = target == token0
        decimals = int(_integer_text(
            state.get("token0_decimals" if target_is_token0 else "token1_decimals"),
            "simulation target decimals",
        ))
        other_decimals = int(_integer_text(
            state.get("token1_decimals" if target_is_token0 else "token0_decimals"),
            "simulation other-token decimals",
        ))
        reserve_target = int(_integer_text(
            state.get("reserve0_raw" if target_is_token0 else "reserve1_raw"),
            "simulation target reserve",
            positive=True,
        ))
        reserve_other = int(_integer_text(
            state.get("reserve1_raw" if target_is_token0 else "reserve0_raw"),
            "simulation other-token reserve",
            positive=True,
        ))
        prior_decimals = decimals_by_token.setdefault(target, decimals)
        if prior_decimals != decimals:
            raise RouteCostEvidenceError("simulation target decimals differ by pool")
        price = (
            Fraction(reserve_other, 10 ** other_decimals)
            / Fraction(reserve_target, 10 ** decimals)
            * Fraction(other_price_text)
        )
        if price <= 0:
            raise RouteCostEvidenceError("simulation target price is invalid")
        by_token.setdefault(target, []).append(price)
        markets_by_token.setdefault(target, []).append(market_id)

    result: Dict[Tuple[str, str], Dict[str, str]] = {}
    for target in sorted(by_token):
        if target in failed_tokens:
            continue
        maximum_price = max(by_token[target])
        decimals = decimals_by_token[target]
        for notional_text in REQUESTED_NOTIONALS_USD:
            raw_fraction = (
                Fraction(notional_text) * (10 ** decimals) / maximum_price
            )
            raw = raw_fraction.numerator // raw_fraction.denominator
            if raw <= 0:
                raise RouteCostEvidenceError(
                    "simulation target is below one raw unit"
                )
            target_value = {
                "schema": ROUTE_COST_SIMULATION_TARGET_SCHEMA,
                "token_address": target,
                "unit_decimals": str(decimals),
                "raw_quantity": str(raw),
                "lattice_raw": "1",
            }
            projection = {
                "simulation_target_token_address": target,
                "simulation_target_unit_decimals": str(decimals),
                "simulation_target_raw_quantity": str(raw),
                "simulation_target_lattice_raw": "1",
                "simulation_target_sha256": typed_sha256(
                    b"route-cost-simulation-target/v1\n", target_value
                ),
            }
            for market_id in markets_by_token[target]:
                result[(market_id, notional_text)] = dict(projection)
    return result


def _adapter_supports_leg(
    leg: Mapping[str, Any], adapter_registry: Mapping[str, Any]
) -> bool:
    registration = next(
        (
            item
            for item in adapter_registry["adapters"]
            if item["adapter_id"] == ETHEREUM_V2_ADAPTER_ID
        ),
        None,
    )
    if registration is None:
        return False
    funding_descriptors = {
        item["token_address"]: item
        for item in registration["token_funding_descriptors"]
    }
    match = _DEX_MARKET_ID.fullmatch(str(leg.get("market_id")))
    if match is None:
        return False
    pair_matches = [
        item for item in registration["pair_descriptors"]
        if item["pair_address"] == match.group(3)
    ]
    if len(pair_matches) != 1:
        return False
    pair = pair_matches[0]
    target = leg.get("target_token_address")
    if not isinstance(target, str) or _ADDRESS.fullmatch(target) is None:
        return False
    pair_tokens = (pair["token0_address"], pair["token1_address"])
    if target not in pair_tokens or any(
        token not in funding_descriptors for token in pair_tokens
    ):
        return False
    context = leg.get("collector_context")
    if not isinstance(context, Mapping) or context.get("status") != "observed":
        # Static support is fully frozen by the embedded pair and funding
        # descriptors.  A later USD/context failure cannot change it.
        return True
    tokens = _market_token_addresses(leg)
    if tokens is None:
        raise RouteCostEvidenceError("observed selected token identity is invalid")
    if set(tokens) != set(pair_tokens):
        raise RouteCostEvidenceError(
            "observed selected tokens differ from static pair descriptor"
        )
    return True


def build_selected_markets(
    universe: Mapping[str, Any], adapter_registry: Mapping[str, Any]
) -> List[Dict[str, Any]]:
    """Replay the deterministic Ethereum-Uniswap-V2/top-eight coverage cohort."""
    registry = validate_adapter_registry(adapter_registry)
    if not isinstance(universe, Mapping):
        raise RouteCostEvidenceError("route universe is invalid")
    legs = universe.get("selected_legs")
    routes = universe.get("routes")
    if not isinstance(legs, list) or not isinstance(routes, list):
        raise RouteCostEvidenceError("route universe inventory is invalid")
    eligible: Dict[str, Mapping[str, Any]] = {}
    tokens = sorted(
        {
            str(leg.get("token_symbol"))
            for leg in legs
            if isinstance(leg, Mapping) and isinstance(leg.get("token_symbol"), str)
        }
    )
    token_rank = {token: index for index, token in enumerate(tokens, start=1)}
    for leg in legs:
        if not isinstance(leg, Mapping):
            raise RouteCostEvidenceError("route universe leg is invalid")
        market_id = leg.get("market_id")
        if not isinstance(market_id, str):
            raise RouteCostEvidenceError("route universe market ID is invalid")
        match = _DEX_MARKET_ID.fullmatch(market_id)
        if match is None or match.group(1) != "eth" or match.group(2) != "uniswap_v2":
            continue
        if market_id in eligible:
            raise RouteCostEvidenceError("route universe has duplicate eligible markets")
        if leg.get("market_type") != "dex" or leg.get("token_symbol") != match.group(4):
            raise RouteCostEvidenceError("eligible market identity is inconsistent")
        inputs = leg.get("selection_inputs")
        if not isinstance(inputs, Mapping):
            raise RouteCostEvidenceError("eligible market selection inputs are invalid")
        _exact_int(leg.get("selection_rank"), "market selection rank", 1)
        eligible[market_id] = leg

    ranked: List[Tuple[Tuple[Any, ...], Dict[str, Any]]] = []
    for market_id, leg in eligible.items():
        route_volumes: List[Decimal] = []
        for route in routes:
            if not isinstance(route, Mapping):
                raise RouteCostEvidenceError("route universe route is invalid")
            if route.get("route_class") != "candidate":
                continue
            if market_id not in {route.get("buy_market_id"), route.get("sell_market_id")}:
                continue
            raw_volume = route.get("route_volume_usd")
            if raw_volume is not None:
                text = _decimal_text(raw_volume, "route volume")
                route_volumes.append(Decimal(text))
        best_volume = max(route_volumes) if route_volumes else None
        inputs = leg["selection_inputs"]
        dex_volume_text = _canonical_optional_decimal(
            inputs.get("dex_24h_usd"), "DEX 24h volume"
        )
        dex_tvl_text = _canonical_optional_decimal(
            inputs.get("dex_tvl_usd"), "DEX TVL"
        )
        best_text = None if best_volume is None else _format_decimal(best_volume)
        supported = _adapter_supports_leg(leg, registry)
        row = {
            "market_id": market_id,
            "token_rank": token_rank[str(leg["token_symbol"])],
            "selection_rank": leg["selection_rank"],
            "best_route_volume_usd": best_text,
            "dex_24h_usd": dex_volume_text,
            "dex_tvl_usd": dex_tvl_text,
            "adapter_id": ETHEREUM_V2_ADAPTER_ID,
            "structural_support_status": "supported" if supported else "unsupported",
            "structural_reason": None if supported else "strict_cost_adapter_unsupported",
        }

        def descending(value: Optional[str]) -> Tuple[int, Decimal]:
            if value is None:
                return 1, Decimal(0)
            return 0, -Decimal(value)

        key = (
            *descending(best_text),
            *descending(dex_volume_text),
            *descending(dex_tvl_text),
            row["token_rank"],
            row["selection_rank"],
            market_id,
        )
        ranked.append((key, row))
    selected = [row for _key, row in sorted(ranked, key=lambda pair: pair[0])[:8]]
    selected.sort(key=lambda row: row["market_id"])
    return selected


def selected_market_set_sha256(selected_markets: Sequence[Mapping[str, Any]]) -> str:
    if not isinstance(selected_markets, Sequence) or isinstance(
        selected_markets, (str, bytes, bytearray)
    ):
        raise RouteCostEvidenceError("selected markets are invalid")
    value = {
        "schema": ROUTE_COST_SELECTED_MARKETS_SCHEMA,
        "members": list(selected_markets),
    }
    return physical_sha256(value)


def _format_decimal(value: Decimal) -> str:
    if value == 0:
        return "0"
    text = format(value, "f")
    if "." in text:
        text = text.rstrip("0").rstrip(".")
    return text


def next_base_fee_wei(*, base_fee_per_gas: int, gas_used: int, gas_limit: int) -> int:
    base_fee = _exact_int(base_fee_per_gas, "base fee")
    used = _exact_int(gas_used, "gas used")
    limit = _exact_int(gas_limit, "gas limit", 1)
    if used > limit:
        raise RouteCostEvidenceError("gas used exceeds gas limit")
    target = limit // 2
    if target <= 0:
        raise RouteCostEvidenceError("gas target is zero")
    if used == target:
        return base_fee
    if used > target:
        delta = max(base_fee * (used - target) // target // 8, 1)
        return base_fee + delta
    delta = base_fee * (target - used) // target // 8
    return base_fee - delta


def max_fee_per_gas_wei(next_base_fee: int, priority_fee: int) -> int:
    return 2 * _exact_int(next_base_fee, "next base fee") + _exact_int(
        priority_fee, "priority fee"
    )


def network_gas_usd(
    *, gas_units: int, max_fee_per_gas_wei_value: int, native_price_usd: str
) -> str:
    units = _exact_int(gas_units, "gas units", 1)
    fee = _exact_int(max_fee_per_gas_wei_value, "max fee per gas")
    price_text = _decimal_text(native_price_usd, "native price", positive=True)
    digits = max(
        len(str(units)) + len(str(fee)) + len(price_text.replace(".", "")) + 32,
        80,
    )
    with localcontext() as context:
        context.prec = digits
        amount = (
            Decimal(units) * Decimal(fee) * Decimal(price_text) / Decimal(10 ** 18)
        )
        amount = amount.quantize(Decimal("0.000000000000000001"), rounding=ROUND_CEILING)
    return _format_decimal(amount)


def _uint256(value: Any, label: str, *, positive: bool = False) -> int:
    result = _exact_int(value, label, 1 if positive else 0)
    if result >= 1 << 256:
        raise RouteCostEvidenceError("{} exceeds uint256".format(label))
    return result


def _abi_word(value: int) -> str:
    return format(_uint256(value, "ABI integer"), "064x")


def _abi_address_word(value: str) -> str:
    return "0" * 24 + _address(value, "ABI address")[2:]


def build_v2_swap_calldata(
    *,
    direction: str,
    quoted_amount_in_raw: int,
    quoted_amount_out_raw: int,
    submission_loss_bound_bps: int,
    path_token_in: str,
    path_token_out: str,
    recipient: str,
    deadline: int,
) -> str:
    amount_in = _uint256(quoted_amount_in_raw, "quoted amount in", positive=True)
    amount_out = _uint256(quoted_amount_out_raw, "quoted amount out", positive=True)
    bound = _exact_int(submission_loss_bound_bps, "submission bound", 0, 10000)
    token_in = _address(path_token_in, "path token in")
    token_out = _address(path_token_out, "path token out")
    if token_in == token_out:
        raise RouteCostEvidenceError("V2 path tokens must differ")
    recipient_value = _address(recipient, "recipient")
    deadline_value = _uint256(deadline, "deadline", positive=True)
    if direction == "buy":
        selector = ETHEREUM_V2_BUY_SELECTOR[2:]
        first = amount_out
        second = (amount_in * (10000 + bound) + 9999) // 10000
    elif direction == "sell":
        selector = ETHEREUM_V2_SELL_SELECTOR[2:]
        first = amount_in
        second = amount_out * (10000 - bound) // 10000
    else:
        raise RouteCostEvidenceError("V2 direction is invalid")
    words = (
        _abi_word(first),
        _abi_word(second),
        _abi_word(5 * 32),
        _abi_address_word(recipient_value),
        _abi_word(deadline_value),
        _abi_word(2),
        _abi_address_word(token_in),
        _abi_address_word(token_out),
    )
    return "0x" + selector + "".join(words)


def _decode_abi_uint(word: str, label: str) -> int:
    if len(word) != 64 or any(character not in "0123456789abcdef" for character in word):
        raise RouteCostEvidenceError("{} ABI word is invalid".format(label))
    return int(word, 16)


def _decode_abi_address(word: str, label: str) -> str:
    if len(word) != 64 or word[:24] != "0" * 24:
        raise RouteCostEvidenceError("{} ABI address is invalid".format(label))
    return _address("0x" + word[24:], label)


def decode_v2_swap_calldata(calldata: Any) -> Dict[str, Any]:
    raw = _decoded_hex_bytes(calldata, "V2 calldata", MAX_CALLDATA_BYTES)
    if len(raw) != 4 + 8 * 32:
        raise RouteCostEvidenceError("V2 calldata length is invalid")
    selector = "0x" + raw[:4].hex()
    words = [raw[index:index + 32].hex() for index in range(4, len(raw), 32)]
    if _decode_abi_uint(words[2], "path offset") != 5 * 32:
        raise RouteCostEvidenceError("V2 calldata path offset is invalid")
    if _decode_abi_uint(words[5], "path length") != 2:
        raise RouteCostEvidenceError("V2 calldata path is not direct")
    path = [
        _decode_abi_address(words[6], "path token in"),
        _decode_abi_address(words[7], "path token out"),
    ]
    if path[0] == path[1]:
        raise RouteCostEvidenceError("V2 calldata path tokens are identical")
    recipient = _decode_abi_address(words[3], "recipient")
    deadline = _decode_abi_uint(words[4], "deadline")
    if deadline <= 0:
        raise RouteCostEvidenceError("V2 calldata deadline is invalid")
    first = _decode_abi_uint(words[0], "first amount")
    second = _decode_abi_uint(words[1], "second amount")
    if first <= 0 or second <= 0:
        raise RouteCostEvidenceError("V2 calldata amount is invalid")
    if selector == ETHEREUM_V2_BUY_SELECTOR:
        return {
            "selector": selector,
            "direction": "buy",
            "amount_out_raw": first,
            "amount_in_max_raw": second,
            "path": path,
            "recipient": recipient,
            "deadline": deadline,
        }
    if selector == ETHEREUM_V2_SELL_SELECTOR:
        return {
            "selector": selector,
            "direction": "sell",
            "amount_in_raw": first,
            "amount_out_min_raw": second,
            "path": path,
            "recipient": recipient,
            "deadline": deadline,
        }
    raise RouteCostEvidenceError("V2 calldata selector is forbidden")


_KECCAK_ROUND_CONSTANTS = (
    0x0000000000000001,
    0x0000000000008082,
    0x800000000000808A,
    0x8000000080008000,
    0x000000000000808B,
    0x0000000080000001,
    0x8000000080008081,
    0x8000000000008009,
    0x000000000000008A,
    0x0000000000000088,
    0x0000000080008009,
    0x000000008000000A,
    0x000000008000808B,
    0x800000000000008B,
    0x8000000000008089,
    0x8000000000008003,
    0x8000000000008002,
    0x8000000000000080,
    0x000000000000800A,
    0x800000008000000A,
    0x8000000080008081,
    0x8000000000008080,
    0x0000000080000001,
    0x8000000080008008,
)
_KECCAK_ROTATION = (
    (0, 36, 3, 41, 18),
    (1, 44, 10, 45, 2),
    (62, 6, 43, 15, 61),
    (28, 55, 25, 21, 56),
    (27, 20, 39, 8, 14),
)
_MASK64 = (1 << 64) - 1


def _rotate_left_64(value: int, amount: int) -> int:
    if amount == 0:
        return value & _MASK64
    return ((value << amount) | (value >> (64 - amount))) & _MASK64


def _keccak_f1600(state: List[int]) -> None:
    for round_constant in _KECCAK_ROUND_CONSTANTS:
        columns = [
            state[x] ^ state[x + 5] ^ state[x + 10] ^ state[x + 15] ^ state[x + 20]
            for x in range(5)
        ]
        deltas = [
            columns[(x - 1) % 5] ^ _rotate_left_64(columns[(x + 1) % 5], 1)
            for x in range(5)
        ]
        for x in range(5):
            for y in range(5):
                state[x + 5 * y] ^= deltas[x]
        rotated = [0] * 25
        for x in range(5):
            for y in range(5):
                rotated[y + 5 * ((2 * x + 3 * y) % 5)] = _rotate_left_64(
                    state[x + 5 * y], _KECCAK_ROTATION[x][y]
                )
        for x in range(5):
            for y in range(5):
                state[x + 5 * y] = (
                    rotated[x + 5 * y]
                    ^ ((~rotated[(x + 1) % 5 + 5 * y]) & rotated[(x + 2) % 5 + 5 * y])
                ) & _MASK64
        state[0] ^= round_constant


def keccak256(data: bytes) -> bytes:
    if not isinstance(data, bytes):
        raise RouteCostEvidenceError("Keccak input must be bytes")
    rate = 136
    padded = bytearray(data)
    padded.append(0x01)
    while len(padded) % rate != rate - 1:
        padded.append(0)
    padded.append(0x80)
    state = [0] * 25
    for offset in range(0, len(padded), rate):
        block = padded[offset:offset + rate]
        for lane in range(rate // 8):
            state[lane] ^= int.from_bytes(block[lane * 8:(lane + 1) * 8], "little")
        _keccak_f1600(state)
    output = bytearray()
    while len(output) < 32:
        for lane in range(rate // 8):
            output.extend(state[lane].to_bytes(8, "little"))
            if len(output) >= 32:
                break
        if len(output) < 32:
            _keccak_f1600(state)
    return bytes(output[:32])


def build_factory_get_pair_calldata(token_a: str, token_b: str) -> str:
    """Encode the fixed Uniswap V2 ``getPair(address,address)`` call."""
    first = _address(token_a, "factory getPair token A")
    second = _address(token_b, "factory getPair token B")
    if first == second:
        raise RouteCostEvidenceError("factory getPair tokens are identical")
    selector = keccak256(b"getPair(address,address)")[:4].hex()
    return "0x" + selector + _abi_address_word(first) + _abi_address_word(second)


def build_fixed_block_phase_a_request_plan(
    *,
    universe: Mapping[str, Any],
    adapter_registry: Mapping[str, Any],
    retained_typed_pool_state_members: Mapping[str, Mapping[str, Any]],
) -> Dict[str, Any]:
    """Build the deterministic fixed-block identity request inventory.

    This boundary is deliberately pure: its only authorities are the captured
    route universe, captured adapter registry, and descriptor-bound retained
    pool-state bytes.  It accepts no endpoint, credential, clock, network
    client, caller-selected block, or request-count override.
    """
    registry = validate_adapter_registry(adapter_registry)
    selected = build_selected_markets(universe, registry)
    supported = {
        row["market_id"] for row in selected
        if row["structural_support_status"] == "supported"
    }
    if (
        not isinstance(retained_typed_pool_state_members, Mapping)
        or not set(retained_typed_pool_state_members).issubset(supported)
    ):
        raise RouteCostEvidenceError(
            "fixed-block Phase A retained pool-state inventory differs"
        )
    retained = _retained_pool_states_for_terminal_inventory(
        retained_typed_pool_state_members, supported
    )
    if not retained:
        return {
            "block_tag": None,
            "requests": [],
            "request_roles": [],
        }
    adapters = {
        row["adapter_id"]: row for row in registry["adapters"]
    }
    anchors = {
        (
            state["chain_id"], state["block_number"], state["block_hash"],
            state["block_header_sha256"], state["observed_at"],
        )
        for state in retained.values()
    }
    if len(anchors) != 1:
        raise RouteCostEvidenceError(
            "fixed-block Phase A retained anchor differs"
        )
    chain_id, block_number, _block_hash, _header_sha, _observed_at = next(
        iter(anchors)
    )
    if int(chain_id) != 1:
        raise RouteCostEvidenceError("fixed-block Phase A chain differs")
    block_tag = hex(int(block_number))

    requests: List[Dict[str, Any]] = []
    request_roles: List[Dict[str, Any]] = []

    def add_request(
        role: str,
        method: str,
        params: List[Any],
        *,
        market_id: Optional[str] = None,
    ) -> None:
        identifier = len(requests) + 1
        requests.append({
            "jsonrpc": "2.0",
            "id": identifier,
            "method": method,
            "params": params,
        })
        request_roles.append({
            "id": identifier,
            "role": role,
            "market_id": market_id,
        })

    add_request("chain_id", "eth_chainId", [])
    add_request(
        "block_header", "eth_getBlockByNumber", [block_tag, False]
    )
    add_request(
        "fee_history", "eth_feeHistory", ["0x1", block_tag, [50]]
    )
    for market_id in sorted(retained):
        state = retained[market_id]
        adapter = adapters.get(ETHEREUM_V2_ADAPTER_ID)
        if adapter is None:
            raise RouteCostEvidenceError(
                "fixed-block Phase A adapter is absent"
            )
        pair_rows = [
            row for row in adapter["pair_descriptors"]
            if row["pair_address"] == state["pool_address"]
        ]
        if len(pair_rows) != 1:
            raise RouteCostEvidenceError(
                "fixed-block Phase A pair descriptor is absent"
            )
        pair = pair_rows[0]
        token0 = state["token0_address"]
        token1 = state["token1_address"]
        if (
            pair["token0_address"] != token0
            or pair["token1_address"] != token1
        ):
            raise RouteCostEvidenceError(
                "fixed-block Phase A pair identity differs"
            )
        add_request(
            "router_runtime_code", "eth_getCode",
            [adapter["router_address"], block_tag], market_id=market_id,
        )
        add_request(
            "factory_runtime_code", "eth_getCode",
            [adapter["factory_address"], block_tag], market_id=market_id,
        )
        add_request(
            "factory_get_pair", "eth_call", [{
                "to": adapter["factory_address"],
                "data": build_factory_get_pair_calldata(token0, token1),
            }, block_tag], market_id=market_id,
        )
        add_request(
            "pair_runtime_code", "eth_getCode",
            [pair["pair_address"], block_tag], market_id=market_id,
        )
        add_request(
            "pair_token0", "eth_call", [{
                "to": pair["pair_address"], "data": "0x0dfe1681",
            }, block_tag], market_id=market_id,
        )
        add_request(
            "pair_token1", "eth_call", [{
                "to": pair["pair_address"], "data": "0xd21220a7",
            }, block_tag], market_id=market_id,
        )
        add_request(
            "token0_runtime_code", "eth_getCode", [token0, block_tag],
            market_id=market_id,
        )
        add_request(
            "token1_runtime_code", "eth_getCode", [token1, block_tag],
            market_id=market_id,
        )

    if len(requests) != 3 + 8 * len(retained):
        raise RouteCostEvidenceError(
            "fixed-block Phase A request denominator differs"
        )
    return _canonical_copy({
        "block_tag": block_tag,
        "requests": requests,
        "request_roles": request_roles,
    })


def project_fixed_block_phase_a_capture(
    *,
    universe: Mapping[str, Any],
    plan: Mapping[str, Any],
    responses: Sequence[Mapping[str, Any]],
    run_id: str,
    route_cohort_id: str,
    candidate_source_generation: str,
    route_universe_sha256: str,
    trace_profile_identity: Mapping[str, Any],
    adapter_registry: Mapping[str, Any],
    retained_typed_pool_state_members: Mapping[str, Mapping[str, Any]],
    captured_started_at: str,
    captured_finished_at: str,
) -> Dict[str, List[Dict[str, Any]]]:
    """Project one exact successful Phase-A result inventory.

    Response order is intentionally irrelevant; integer JSON-RPC IDs are the
    only join key.  The request plan and every static/retained authority are
    recomputed here before any captured result is trusted.
    """
    _required_text(run_id, "run ID", _SAFE_RUN_ID)
    if _COHORT_ID.fullmatch(route_cohort_id or "") is None:
        raise RouteCostEvidenceError("route cohort ID is invalid")
    _sha256(candidate_source_generation, "candidate source generation")
    _sha256(route_universe_sha256, "route universe hash")
    if physical_sha256(universe) != route_universe_sha256:
        raise RouteCostEvidenceError("route universe physical hash differs")
    if universe.get("candidate_source_generation") != candidate_source_generation:
        raise RouteCostEvidenceError("route universe generation differs")
    _ordered_timestamps(
        captured_started_at, captured_finished_at, "fixed-block Phase A capture"
    )
    registry = validate_adapter_registry(adapter_registry)
    expected_plan = build_fixed_block_phase_a_request_plan(
        universe=universe,
        adapter_registry=registry,
        retained_typed_pool_state_members=retained_typed_pool_state_members,
    )
    if plan != expected_plan:
        raise RouteCostEvidenceError("fixed-block Phase A plan differs")
    trace_identity, _trace_generation = _validate_profile_identity(
        trace_profile_identity, kind="trace"
    )
    if trace_identity["status"] != "available":
        raise RouteCostEvidenceError(
            "fixed-block Phase A trace profile is unavailable"
        )
    selected = build_selected_markets(universe, registry)
    selected_by_id = {row["market_id"]: row for row in selected}
    supported = {
        market_id for market_id, row in selected_by_id.items()
        if row["structural_support_status"] == "supported"
    }
    retained = _retained_pool_states_for_terminal_inventory(
        retained_typed_pool_state_members, supported
    )
    links = {
        "run_id": run_id,
        "route_cohort_id": route_cohort_id,
        "candidate_source_generation": candidate_source_generation,
        "route_universe_sha256": route_universe_sha256,
        "adapter_registry_sha256": physical_sha256(registry),
        "selected_market_set_sha256": selected_market_set_sha256(selected),
    }

    if not retained:
        if responses != []:
            raise RouteCostEvidenceError(
                "fixed-block Phase A empty response inventory differs"
            )
        return {
            "chain_evidence": [],
            "market_evidence": [],
        }

    if not isinstance(responses, Sequence) or isinstance(
        responses, (str, bytes, bytearray)
    ):
        raise RouteCostEvidenceError("fixed-block Phase A responses are invalid")
    expected_ids = [row["id"] for row in expected_plan["requests"]]
    by_id: Dict[int, Mapping[str, Any]] = {}
    for row in responses:
        if (
            not isinstance(row, Mapping)
            or set(row) != {"jsonrpc", "id", "result"}
            or row.get("jsonrpc") != "2.0"
            or type(row.get("id")) is not int
            or row["id"] in by_id
        ):
            raise RouteCostEvidenceError(
                "fixed-block Phase A response inventory is invalid"
            )
        by_id[row["id"]] = row
    if set(by_id) != set(expected_ids) or len(by_id) != len(expected_ids):
        raise RouteCostEvidenceError(
            "fixed-block Phase A response denominator differs"
        )
    role_by_id = {row["id"]: row for row in expected_plan["request_roles"]}

    def result(role: str, market_id: Optional[str] = None) -> Any:
        matches = [
            by_id[identifier]["result"]
            for identifier, row in role_by_id.items()
            if row["role"] == role and row["market_id"] == market_id
        ]
        if len(matches) != 1:
            raise RouteCostEvidenceError(
                "fixed-block Phase A response role differs"
            )
        return matches[0]

    chain_quantity = _quantity(result("chain_id"), "Phase A chain ID", positive=True)
    if int(chain_quantity, 16) != 1:
        raise RouteCostEvidenceError("fixed-block Phase A chain ID differs")
    raw_header = result("block_header")
    _exact_fields(
        raw_header,
        (
            "number", "hash", "parentHash", "timestamp", "baseFeePerGas",
            "gasUsed", "gasLimit",
        ),
        "fixed-block Phase A header result",
    )
    header = {
        "number": raw_header["number"],
        "hash": raw_header["hash"],
        "parent_hash": raw_header["parentHash"],
        "timestamp": raw_header["timestamp"],
        "base_fee_per_gas": raw_header["baseFeePerGas"],
        "gas_used": raw_header["gasUsed"],
        "gas_limit": raw_header["gasLimit"],
    }
    _validate_block_header(header)
    if header["number"] != expected_plan["block_tag"]:
        raise RouteCostEvidenceError("fixed-block Phase A header number differs")
    raw_fee = result("fee_history")
    _exact_fields(
        raw_fee,
        ("oldestBlock", "baseFeePerGas", "reward", "gasUsedRatio"),
        "fixed-block Phase A fee-history result",
    )
    ratios = raw_fee["gasUsedRatio"]
    if not isinstance(ratios, list):
        raise RouteCostEvidenceError("fixed-block Phase A gas ratios are invalid")
    ratio_text: List[str] = []
    for ratio in ratios:
        if isinstance(ratio, bool) or not isinstance(
            ratio, (int, float, Decimal)
        ):
            raise RouteCostEvidenceError(
                "fixed-block Phase A gas ratio is invalid"
            )
        try:
            number = ratio if isinstance(ratio, Decimal) else Decimal(str(ratio))
        except (InvalidOperation, ValueError):
            raise RouteCostEvidenceError(
                "fixed-block Phase A gas ratio is invalid"
            ) from None
        if (
            not number.is_finite()
            or number < 0
            or number > 1
            or (number.is_zero() and number.is_signed())
        ):
            raise RouteCostEvidenceError(
                "fixed-block Phase A gas ratio is invalid"
            )
        ratio_text.append(_format_decimal(number))
    fee_history = {
        "schema": "route_cost_fee_history_result/v1",
        "status": "observed",
        "reason_code": None,
        "oldest_block": raw_fee["oldestBlock"],
        "base_fee_per_gas": raw_fee["baseFeePerGas"],
        "reward": raw_fee["reward"],
        "gas_used_ratio": ratio_text,
    }
    _validate_fee_history(fee_history)
    expected_next = next_base_fee_wei(
        base_fee_per_gas=int(header["base_fee_per_gas"], 16),
        gas_used=int(header["gas_used"], 16),
        gas_limit=int(header["gas_limit"], 16),
    )
    if (
        fee_history["oldest_block"] != header["number"]
        or int(fee_history["base_fee_per_gas"][0], 16)
        != int(header["base_fee_per_gas"], 16)
        or int(fee_history["base_fee_per_gas"][1], 16) != expected_next
    ):
        raise RouteCostEvidenceError(
            "fixed-block Phase A fee history differs from header"
        )
    native = {
        "schema": "route_cost_native_price_record/v1",
        "status": "unavailable",
        "reason_code": "native_price_unavailable",
        "native_symbol": "ETH",
        "wrapped_native_address": ETHEREUM_WETH_ADDRESS,
        "price_usd": None,
        "observed_at": None,
        "valid_until": None,
        "native_price_evidence_sha256": None,
        "source_record_sha256": None,
    }
    _validate_native_price_record(native)
    chain_status = (
        "failed" if native["status"] == "failed" else
        "incomplete" if native["status"] == "unavailable" else "observed"
    )
    chain_reason = None if chain_status == "observed" else native["reason_code"]
    chain = {
        "schema": ROUTE_COST_CHAIN_EVIDENCE_SCHEMA,
        "run_id": run_id,
        "route_cohort_id": route_cohort_id,
        "candidate_source_generation": candidate_source_generation,
        "route_universe_sha256": route_universe_sha256,
        "selected_market_set_sha256": links["selected_market_set_sha256"],
        "chain_id": 1,
        "rpc_source_id": trace_identity["endpoint_id"],
        "captured_started_at": captured_started_at,
        "captured_finished_at": captured_finished_at,
        "status": chain_status,
        "reason_code": chain_reason,
        "block_header_result": header,
        "fee_history_result": fee_history,
        "native_price_record": native,
    }
    chain_sha = _validate_chain_evidence(
        chain, links, trace_profile_identity=trace_identity
    )
    chain_hashes = {chain_sha: chain}
    adapter = next(
        row for row in registry["adapters"]
        if row["adapter_id"] == ETHEREUM_V2_ADAPTER_ID
    )
    markets: List[Dict[str, Any]] = []
    request_by_id = {row["id"]: row for row in expected_plan["requests"]}
    retained_ids = set(retained)
    for market_id in sorted(retained_ids):
        state = retained[market_id]
        pair = next(
            row for row in adapter["pair_descriptors"]
            if row["pair_address"] == state["pool_address"]
        )

        def role_id(role: str) -> int:
            matches = [
                identifier for identifier, row in role_by_id.items()
                if row["role"] == role and row["market_id"] == market_id
            ]
            if len(matches) != 1:
                raise RouteCostEvidenceError(
                    "fixed-block Phase A market role differs"
                )
            return matches[0]

        get_pair_id = role_id("factory_get_pair")
        token_members = []
        for token_role, token in (
            ("token0_runtime_code", state["token0_address"]),
            ("token1_runtime_code", state["token1_address"]),
        ):
            identifier = role_id(token_role)
            token_members.append({
                "schema": ROUTE_COST_TOKEN_RUNTIME_CODE_EVIDENCE_SCHEMA,
                "token_address": token,
                "request": {
                    "schema": ROUTE_COST_TOKEN_RUNTIME_CODE_REQUEST_SCHEMA,
                    **request_by_id[identifier],
                },
                "response": {
                    "schema": ROUTE_COST_TOKEN_RUNTIME_CODE_RESPONSE_SCHEMA,
                    "jsonrpc": "2.0",
                    "id": identifier,
                    "result": by_id[identifier]["result"],
                },
            })
        token_members.sort(key=lambda row: row["token_address"])
        market = {
            "schema": ROUTE_COST_MARKET_EVIDENCE_SCHEMA,
            "run_id": run_id,
            "route_cohort_id": route_cohort_id,
            "candidate_source_generation": candidate_source_generation,
            "route_universe_sha256": route_universe_sha256,
            "adapter_registry_sha256": links["adapter_registry_sha256"],
            "selected_market_set_sha256": links["selected_market_set_sha256"],
            "market_id": market_id,
            "adapter_id": ETHEREUM_V2_ADAPTER_ID,
            "chain_evidence_sha256": chain_sha,
            "core_pool_state_id": state["state_id"],
            "core_pool_state_sha256": state["_physical_sha256"],
            "router_address": adapter["router_address"],
            "router_runtime_code": result("router_runtime_code", market_id),
            "factory_address": adapter["factory_address"],
            "factory_runtime_code": result("factory_runtime_code", market_id),
            "factory_get_pair_request": {
                "schema": ROUTE_COST_FACTORY_GET_PAIR_REQUEST_SCHEMA,
                **request_by_id[get_pair_id],
            },
            "factory_get_pair_response": {
                "schema": ROUTE_COST_FACTORY_GET_PAIR_RESPONSE_SCHEMA,
                "jsonrpc": "2.0",
                "id": get_pair_id,
                "result": by_id[get_pair_id]["result"],
            },
            "pair_address": pair["pair_address"],
            "pair_runtime_code": result("pair_runtime_code", market_id),
            "pair_token0": _decode_address_word(
                result("pair_token0", market_id), "Phase A pair token0"
            ),
            "pair_token1": _decode_address_word(
                result("pair_token1", market_id), "Phase A pair token1"
            ),
            "token_runtime_code_evidence": token_members,
            "captured_started_at": captured_started_at,
            "captured_finished_at": captured_finished_at,
        }
        _validate_market_evidence(
            market,
            links,
            chain_hashes,
            retained_ids,
            adapter,
            retained,
        )
        markets.append(market)
    return _canonical_copy({
        "chain_evidence": [chain],
        "market_evidence": markets,
    })


def project_native_price_terminal_phase_a_capture(
    *,
    universe: Mapping[str, Any],
    phase_a_capture: Mapping[str, Any],
    run_id: str,
    route_cohort_id: str,
    candidate_source_generation: str,
    route_universe_sha256: str,
    trace_profile_identity: Mapping[str, Any],
    submission_connector_profile_identity: Mapping[str, Any],
    adapter_registry: Mapping[str, Any],
    retained_typed_pool_state_members: Mapping[str, Mapping[str, Any]],
    submission_policy_snapshot: Mapping[str, Any],
    reason_code: str,
    terminal_reason_by_market: Optional[Mapping[str, str]] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """Close successful Phase A at the deterministic native-price boundary.

    No estimate or trace request has been released at this boundary.  The
    fixed-block identity graph is replayed in full, then each retained scenario
    derives its calldata and completed call prefix from the same universe,
    retained-state, adapter, and submission-policy authorities as Phase B.
    """
    if reason_code not in {
        "native_price_unavailable", "native_price_invalid",
    }:
        raise RouteCostEvidenceError(
            "native-price terminal reason is invalid"
        )
    (
        registry,
        trace_identity,
        _connector_identity,
        _connector_registry,
        selected,
        selected_by_id,
        links,
    ) = _producer_lineage(
        universe=universe,
        run_id=run_id,
        route_cohort_id=route_cohort_id,
        candidate_source_generation=candidate_source_generation,
        route_universe_sha256=route_universe_sha256,
        adapter_registry=adapter_registry,
        trace_profile_identity=trace_profile_identity,
        submission_connector_profile_identity=(
            submission_connector_profile_identity
        ),
    )
    if trace_identity["status"] != "available":
        raise RouteCostEvidenceError(
            "native-price terminal trace profile is unavailable"
        )
    supported = {
        market_id for market_id, row in selected_by_id.items()
        if row["structural_support_status"] == "supported"
    }
    retained = _retained_pool_states_for_terminal_inventory(
        retained_typed_pool_state_members, supported
    )
    terminal_core_reasons = _phase_b_terminal_core_reasons(
        supported=supported,
        retained=retained,
        terminal_reason_by_market=terminal_reason_by_market,
    )
    _exact_fields(
        phase_a_capture,
        ("chain_evidence", "market_evidence"),
        "native-price terminal Phase A capture",
    )
    chain_rows = phase_a_capture.get("chain_evidence")
    market_rows = phase_a_capture.get("market_evidence")
    if (
        not isinstance(chain_rows, list)
        or len(chain_rows) != 1
        or not isinstance(market_rows, list)
        or [row.get("market_id") for row in market_rows] != sorted(retained)
    ):
        raise RouteCostEvidenceError(
            "native-price terminal Phase A denominator differs"
        )
    old_chain = _canonical_copy(chain_rows[0])
    old_chain_sha = _validate_chain_evidence(
        old_chain, links, trace_profile_identity=trace_identity
    )
    unavailable_record = {
        "schema": "route_cost_native_price_record/v1",
        "status": "unavailable",
        "reason_code": "native_price_unavailable",
        "native_symbol": "ETH",
        "wrapped_native_address": ETHEREUM_WETH_ADDRESS,
        "price_usd": None,
        "observed_at": None,
        "valid_until": None,
        "native_price_evidence_sha256": None,
        "source_record_sha256": None,
    }
    if (
        old_chain.get("status") != "incomplete"
        or old_chain.get("reason_code") != "native_price_unavailable"
        or old_chain.get("fee_history_result", {}).get("status") != "observed"
        or old_chain.get("native_price_record") != unavailable_record
    ):
        raise RouteCostEvidenceError(
            "native-price terminal Phase A chain differs"
        )
    adapter_rows = [
        row for row in registry["adapters"]
        if row["adapter_id"] == ETHEREUM_V2_ADAPTER_ID
    ]
    if len(adapter_rows) != 1:
        raise RouteCostEvidenceError(
            "native-price terminal adapter is absent"
        )
    adapter = adapter_rows[0]
    old_chain_hashes = {old_chain_sha: old_chain}
    old_markets: List[Dict[str, Any]] = []
    for row in market_rows:
        market = _canonical_copy(row)
        _validate_market_evidence(
            market, links, old_chain_hashes, set(retained), adapter, retained
        )
        old_markets.append(market)

    chain = _canonical_copy(old_chain)
    if reason_code == "native_price_invalid":
        chain["status"] = "failed"
        chain["reason_code"] = reason_code
        chain["native_price_record"] = {
            **unavailable_record,
            "status": "failed",
            "reason_code": reason_code,
        }
    chain_sha = _validate_chain_evidence(
        chain, links, trace_profile_identity=trace_identity
    )
    chain_hashes = {chain_sha: chain}
    markets: List[Dict[str, Any]] = []
    market_by_id: Dict[str, Dict[str, Any]] = {}
    market_hashes: Dict[str, Dict[str, Any]] = {}
    for old_market in old_markets:
        market = _canonical_copy(old_market)
        market["chain_evidence_sha256"] = chain_sha
        market_sha = _validate_market_evidence(
            market, links, chain_hashes, set(retained), adapter, retained
        )
        markets.append(market)
        market_by_id[market["market_id"]] = market
        market_hashes[market_sha] = market

    expected_scope, route_sides = _expected_binding_scope(
        universe, selected_by_id
    )
    snapshot = _canonical_copy(submission_policy_snapshot)
    policy_members = _phase_b_structural_policy_members(
        snapshot,
        links=links,
        expected_scope=expected_scope,
        route_sides=route_sides,
    )
    routes_by_id = {
        row["route_id"]: row for row in universe.get("routes", [])
        if isinstance(row, Mapping) and row.get("route_class") == "candidate"
    }
    universe_legs = universe.get("selected_legs")
    if not isinstance(universe_legs, list):
        raise RouteCostEvidenceError(
            "route universe selected legs are invalid"
        )
    targets = build_simulation_targets(
        universe_legs, selected_by_id, retained
    )
    target_complete = {
        market_id for market_id, _notional in targets
    }
    token_identity: Dict[str, Tuple[str, str]] = {}
    for leg in universe_legs:
        market_id = leg.get("market_id") if isinstance(leg, Mapping) else None
        if market_id not in target_complete:
            continue
        identity = _strict_dex_target_identity(leg)
        if identity is None:
            raise RouteCostEvidenceError(
                "native-price terminal token identity is absent"
            )
        token_identity[str(market_id)] = (identity[0], identity[1])
    if set(token_identity) != target_complete:
        raise RouteCostEvidenceError(
            "native-price terminal token denominator differs"
        )

    status = "unavailable" if reason_code.endswith("unavailable") else "failed"
    header = chain["block_header_result"]
    transcript_links = {
        field: links[field] for field in (
            "run_id", "route_cohort_id", "candidate_source_generation",
            "route_universe_sha256", "adapter_registry_sha256",
            "selected_market_set_sha256", "trace_profile_generation",
            "submission_connector_profile_generation",
        )
    }
    rows: List[Dict[str, Any]] = []
    for market_id in sorted(target_complete):
        state = retained[market_id]
        market = market_by_id[market_id]
        market_sha = physical_sha256(market)
        target_token, other_token = token_identity[market_id]
        for direction in ("buy", "sell"):
            for notional in REQUESTED_NOTIONALS_USD:
                target = targets.get((market_id, notional))
                if target is None:
                    raise RouteCostEvidenceError(
                        "native-price terminal simulation target is absent"
                    )
                amount_in, amount_out = _v2_quote_for_target(
                    direction=direction,
                    target_token=target_token,
                    target_raw=int(target["simulation_target_raw_quantity"]),
                    retained_pool_state=state,
                )
                relevant_bounds: List[int] = []
                for route_id, member_notional in expected_scope:
                    if member_notional != notional:
                        continue
                    route = routes_by_id[route_id]
                    if route.get(direction + "_market_id") != market_id:
                        continue
                    member = policy_members[(route_id, notional)]
                    relevant_bounds.append(
                        int(member[direction + "_submission_loss_bps"])
                        if member.get("status") == "observed" else 100
                    )
                if snapshot.get("status") != "authenticated" or not relevant_bounds:
                    relevant_bounds = [100]
                if len(set(relevant_bounds)) != 1:
                    raise RouteCostEvidenceError(
                        "native-price terminal submission bound differs"
                    )
                bound = relevant_bounds[0]
                token_in, token_out = (
                    (other_token, target_token)
                    if direction == "buy"
                    else (target_token, other_token)
                )
                deadline = int(header["timestamp"], 16) + 300
                calldata = build_v2_swap_calldata(
                    direction=direction,
                    quoted_amount_in_raw=amount_in,
                    quoted_amount_out_raw=amount_out,
                    submission_loss_bound_bps=bound,
                    path_token_in=token_in,
                    path_token_out=token_out,
                    recipient=adapter["simulation_sender_address"],
                    deadline=deadline,
                )
                raw = {
                    "schema": ROUTE_COST_RAW_TRANSCRIPT_SCHEMA,
                    "chain_evidence_sha256": chain_sha,
                    "market_evidence_sha256": market_sha,
                    "captured_started_at": chain["captured_finished_at"],
                    "captured_finished_at": chain["captured_finished_at"],
                    "calldata_hex": calldata,
                    "estimate_gas_request": None,
                    "estimate_gas_response": None,
                    "simulation_method": None,
                    "simulation_request": None,
                    "simulation_response": None,
                    "simulation_balance_deltas": None,
                }
                call = {
                    "schema": ROUTE_COST_CALL_EVIDENCE_SCHEMA,
                    "selector": (
                        ETHEREUM_V2_BUY_SELECTOR
                        if direction == "buy" else ETHEREUM_V2_SELL_SELECTOR
                    ),
                    "path_token_in": token_in,
                    "path_token_out": token_out,
                    "recipient_policy": "same_as_registry_sender/v1",
                    "deadline": hex(deadline),
                    "amount_in_raw": str(amount_in),
                    "amount_out_raw": str(amount_out),
                    "calldata_sha256": hashlib.sha256(
                        bytes.fromhex(calldata[2:])
                    ).hexdigest(),
                    "sender_policy": (
                        "registry_fixed_state_override_sender/v1"
                    ),
                    "allowance_basis": "exact_amount_state_override/v1",
                    "submission_loss_bound_bps": str(bound),
                }
                transcript = {
                    "schema": ROUTE_COST_TRANSCRIPT_SCHEMA,
                    **transcript_links,
                    "market_id": market_id,
                    "direction": direction,
                    "requested_notional_usd": notional,
                    "adapter_id": ETHEREUM_V2_ADAPTER_ID,
                    **target,
                    "core_pool_state_id": state["state_id"],
                    "core_pool_state_sha256": state["_physical_sha256"],
                    "chain_evidence_sha256": chain_sha,
                    "market_evidence_sha256": market_sha,
                    "status": status,
                    "completed_stage": "call",
                    "reason_code": reason_code,
                    "block_evidence": {
                        "schema": ROUTE_COST_BLOCK_EVIDENCE_SCHEMA,
                        "chain_evidence_sha256": chain_sha,
                        "market_evidence_sha256": market_sha,
                        "chain_id": 1,
                        "block_tag": header["number"],
                        "block_number": header["number"],
                        "block_hash": header["hash"],
                        "block_timestamp": header["timestamp"],
                        "core_pool_state_id": state["state_id"],
                        "router_runtime_code_sha256": adapter[
                            "router_runtime_code_sha256"
                        ],
                        "factory_runtime_code_sha256": adapter[
                            "factory_runtime_code_sha256"
                        ],
                        "pair_runtime_code_sha256": hashlib.sha256(
                            bytes.fromhex(market["pair_runtime_code"][2:])
                        ).hexdigest(),
                        "rpc_transcript_sha256": _raw_rpc_transcript_sha256(raw),
                    },
                    "call_evidence": call,
                    "gas_evidence": None,
                    "router_fee_evidence": None,
                    "transfer_tax_evidence": None,
                    "raw_transcript": raw,
                }
                _validate_transcript(
                    transcript,
                    links=links,
                    selected=selected_by_id,
                    chain_hashes=chain_hashes,
                    market_hashes=market_hashes,
                    adapter=adapter,
                    native_sha=None,
                    native_evidence=None,
                    retained_pool_states=retained,
                    market_tokens=token_identity,
                    simulation_targets=targets,
                    evaluated_at=chain["captured_finished_at"],
                )
                rows.append(transcript)
    rows.extend(_build_calldata_unavailable_transcripts(
        market_ids=set(retained) - target_complete,
        transcript_links=transcript_links,
        selected_by_id=selected_by_id,
        chain=chain,
        chain_sha=chain_sha,
        market_by_id=market_by_id,
        market_hashes=market_hashes,
        retained=retained,
        adapter=adapter,
        simulation_targets=targets,
        captured_started_at=chain["captured_finished_at"],
        captured_finished_at=chain["captured_finished_at"],
        native_sha=None,
        native_evidence=None,
    ))
    for market in selected:
        market_id = market["market_id"]
        if market_id in retained:
            continue
        if market_id in supported:
            row_status = (
                "failed"
                if terminal_core_reasons[market_id]
                == "core_pool_state_invalid"
                else "unavailable"
            )
            row_reason = terminal_core_reasons[market_id]
        else:
            row_status = "unavailable"
            row_reason = "strict_cost_adapter_unsupported"
        for direction in ("buy", "sell"):
            for notional in REQUESTED_NOTIONALS_USD:
                rows.append({
                    "schema": ROUTE_COST_TRANSCRIPT_SCHEMA,
                    **transcript_links,
                    "market_id": market_id,
                    "direction": direction,
                    "requested_notional_usd": notional,
                    "adapter_id": market["adapter_id"],
                    "simulation_target_token_address": None,
                    "simulation_target_unit_decimals": None,
                    "simulation_target_raw_quantity": None,
                    "simulation_target_lattice_raw": None,
                    "simulation_target_sha256": None,
                    "core_pool_state_id": None,
                    "core_pool_state_sha256": None,
                    "chain_evidence_sha256": None,
                    "market_evidence_sha256": None,
                    "status": row_status,
                    "completed_stage": "none",
                    "reason_code": row_reason,
                    "block_evidence": None,
                    "call_evidence": None,
                    "gas_evidence": None,
                    "router_fee_evidence": None,
                    "transfer_tax_evidence": None,
                    "raw_transcript": None,
                })
    return _canonical_copy({
        "chain_evidence": [chain],
        "market_evidence": markets,
        "transcripts": rows,
    })


def bind_native_price_to_phase_a_capture(
    *,
    universe: Mapping[str, Any],
    phase_a_capture: Mapping[str, Any],
    native_price_evidence: Mapping[str, Any],
    run_id: str,
    route_cohort_id: str,
    candidate_source_generation: str,
    route_universe_sha256: str,
    trace_profile_identity: Mapping[str, Any],
    adapter_registry: Mapping[str, Any],
    retained_typed_pool_state_members: Mapping[str, Mapping[str, Any]],
) -> Dict[str, List[Dict[str, Any]]]:
    """Bind one sealed native-price capture into a successful Phase-A graph.

    The input graph must be the exact successful fixed-block projection whose
    only incomplete component is its native price.  Every authority is replayed
    before the chain hash is replaced in each market row.
    """
    _required_text(run_id, "run ID", _SAFE_RUN_ID)
    if _COHORT_ID.fullmatch(route_cohort_id or "") is None:
        raise RouteCostEvidenceError("route cohort ID is invalid")
    _sha256(candidate_source_generation, "candidate source generation")
    _sha256(route_universe_sha256, "route universe hash")
    if physical_sha256(universe) != route_universe_sha256:
        raise RouteCostEvidenceError("route universe physical hash differs")
    if universe.get("candidate_source_generation") != candidate_source_generation:
        raise RouteCostEvidenceError("route universe generation differs")
    registry = validate_adapter_registry(adapter_registry)
    trace_identity, _trace_generation = _validate_profile_identity(
        trace_profile_identity, kind="trace"
    )
    if trace_identity["status"] != "available":
        raise RouteCostEvidenceError("Phase A trace profile is unavailable")
    selected = build_selected_markets(universe, registry)
    selected_by_id = {row["market_id"]: row for row in selected}
    supported = {
        market_id for market_id, row in selected_by_id.items()
        if row["structural_support_status"] == "supported"
    }
    selected_set_sha = selected_market_set_sha256(selected)
    if not isinstance(retained_typed_pool_state_members, Mapping):
        raise RouteCostEvidenceError("retained pool-state inventory is invalid")
    retained = _retained_pool_states_for_terminal_inventory(
        retained_typed_pool_state_members,
        supported,
    )
    _exact_fields(
        phase_a_capture,
        ("chain_evidence", "market_evidence"),
        "fixed-block Phase A capture",
    )
    chain_rows = phase_a_capture.get("chain_evidence")
    market_rows = phase_a_capture.get("market_evidence")
    if (
        not isinstance(chain_rows, list)
        or len(chain_rows) != 1
        or not isinstance(market_rows, list)
        or [row.get("market_id") for row in market_rows] != sorted(retained)
    ):
        raise RouteCostEvidenceError("fixed-block Phase A capture denominator differs")
    old_chain = _canonical_copy(chain_rows[0])
    if old_chain.get("selected_market_set_sha256") != selected_set_sha:
        raise RouteCostEvidenceError(
            "fixed-block Phase A selected-market authority differs"
        )
    links = {
        "run_id": run_id,
        "route_cohort_id": route_cohort_id,
        "candidate_source_generation": candidate_source_generation,
        "route_universe_sha256": route_universe_sha256,
        "adapter_registry_sha256": physical_sha256(registry),
        "selected_market_set_sha256": selected_set_sha,
    }
    old_chain_sha = _validate_chain_evidence(
        old_chain, links, trace_profile_identity=trace_identity
    )
    unavailable_native = {
        "schema": "route_cost_native_price_record/v1",
        "status": "unavailable",
        "reason_code": "native_price_unavailable",
        "native_symbol": "ETH",
        "wrapped_native_address": ETHEREUM_WETH_ADDRESS,
        "price_usd": None,
        "observed_at": None,
        "valid_until": None,
        "native_price_evidence_sha256": None,
        "source_record_sha256": None,
    }
    if (
        old_chain.get("status") != "incomplete"
        or old_chain.get("reason_code") != "native_price_unavailable"
        or old_chain.get("fee_history_result", {}).get("status") != "observed"
        or old_chain.get("native_price_record") != unavailable_native
    ):
        raise RouteCostEvidenceError("fixed-block Phase A native terminal differs")
    adapter_rows = [
        row for row in registry["adapters"]
        if row["adapter_id"] == ETHEREUM_V2_ADAPTER_ID
    ]
    if len(adapter_rows) != 1:
        raise RouteCostEvidenceError("fixed-block Phase A adapter is absent")
    adapter = adapter_rows[0]
    old_chain_hashes = {old_chain_sha: old_chain}
    old_markets: List[Dict[str, Any]] = []
    for row in market_rows:
        market = _canonical_copy(row)
        if (
            market.get("captured_started_at")
            != old_chain.get("captured_started_at")
            or market.get("captured_finished_at")
            != old_chain.get("captured_finished_at")
        ):
            raise RouteCostEvidenceError("fixed-block Phase A capture window differs")
        _validate_market_evidence(
            market,
            links,
            old_chain_hashes,
            set(retained),
            adapter,
            retained,
        )
        old_markets.append(market)

    native = _canonical_copy(native_price_evidence)
    native_sha = _validate_native_price_evidence(native, links)
    book = native["book_projection"]
    conversion = native["usd_conversion_projection"]
    native_record = {
        "schema": "route_cost_native_price_record/v1",
        "status": "observed",
        "reason_code": None,
        "native_symbol": "ETH",
        "wrapped_native_address": ETHEREUM_WETH_ADDRESS,
        "price_usd": _format_decimal(
            Decimal(book["best_ask_price"]) * Decimal(conversion["rate"])
        ),
        "observed_at": native["observed_at"],
        "valid_until": native["valid_until"],
        "native_price_evidence_sha256": native_sha,
        "source_record_sha256": native["source_record_sha256"],
    }
    _validate_native_price_record(native_record)
    new_chain = _canonical_copy(old_chain)
    new_chain["status"] = "observed"
    new_chain["reason_code"] = None
    new_chain["native_price_record"] = native_record
    new_chain_sha = _validate_chain_evidence(
        new_chain, links, trace_profile_identity=trace_identity
    )
    new_chain_hashes = {new_chain_sha: new_chain}
    new_markets: List[Dict[str, Any]] = []
    for old_market in old_markets:
        market = _canonical_copy(old_market)
        market["chain_evidence_sha256"] = new_chain_sha
        _validate_market_evidence(
            market,
            links,
            new_chain_hashes,
            set(retained),
            adapter,
            retained,
        )
        new_markets.append(market)
    return _canonical_copy({
        "chain_evidence": [new_chain],
        "market_evidence": new_markets,
    })


def _phase_b_structural_policy_members(
    snapshot: Mapping[str, Any],
    *,
    links: Mapping[str, Any],
    expected_scope: Sequence[Tuple[str, str]],
    route_sides: Mapping[str, Tuple[bool, bool]],
) -> Dict[Tuple[str, str], Mapping[str, Any]]:
    """Validate a policy snapshot structurally, without blessing its SSHSIG."""
    _exact_fields(snapshot, POLICY_SNAPSHOT_FIELDS, "policy snapshot")
    if snapshot.get("schema") != ROUTE_COST_POLICY_SNAPSHOT_SCHEMA:
        raise RouteCostEvidenceError("policy snapshot schema is invalid")
    for field in (
        "run_id", "route_cohort_id", "candidate_source_generation",
        "route_universe_sha256", "adapter_registry_sha256",
        "selected_market_set_sha256", "trace_profile_generation",
        "submission_connector_profile_generation",
    ):
        if snapshot.get(field) != links[field]:
            raise RouteCostEvidenceError("policy snapshot lineage differs")
    _sha256(
        snapshot.get("connector_key_registry_sha256"),
        "policy snapshot connector key registry hash",
    )
    members = snapshot.get("members")
    count = _exact_int(
        snapshot.get("member_count"), "policy member count", 0, MAX_BINDINGS
    )
    if not isinstance(members, list) or len(members) != count:
        raise RouteCostEvidenceError("policy member count differs")
    identities: List[Tuple[str, str]] = []
    by_scope: Dict[Tuple[str, str], Mapping[str, Any]] = {}
    for member in members:
        identity = _validate_policy_member(member, route_sides=route_sides)
        identities.append(identity)
        by_scope[identity] = member
    if identities != list(expected_scope) or len(by_scope) != len(identities):
        raise RouteCostEvidenceError("policy member scope differs")
    if snapshot.get("member_set_sha256") != typed_sha256(
        b"route-cost-submission-policy-member-set/v1\n", members
    ):
        raise RouteCostEvidenceError("policy member-set hash differs")
    status = snapshot.get("status")
    if status == "authenticated":
        if snapshot.get("reason_code") is not None:
            raise RouteCostEvidenceError("authenticated snapshot has a reason")
        _required_text(
            snapshot.get("connector_id"), "snapshot connector ID", _CONNECTOR_ID
        )
        observed = _timestamp_value(
            snapshot.get("observed_at"), "snapshot observed_at"
        )
        valid = _timestamp_value(snapshot.get("valid_until"), "snapshot valid_until")
        if observed > valid:
            raise RouteCostEvidenceError("snapshot validity is reversed")
        _required_text(snapshot.get("issuer_key_id"), "snapshot issuer key", _LOWER_ID)
        if snapshot.get("signature_algorithm") != "ssh-ed25519-sshsig-v1":
            raise RouteCostEvidenceError("snapshot signature algorithm is invalid")
        if snapshot.get("attested_payload_sha256") != typed_sha256(
            b"route-cost-submission-policy-attestation/v1\n",
            _policy_attestation(snapshot),
        ):
            raise RouteCostEvidenceError("snapshot attested-payload hash differs")
        _sshsig_bytes(snapshot.get("signature"))
    else:
        policy_links = dict(links)
        policy_links["connector_key_registry_sha256"] = snapshot[
            "connector_key_registry_sha256"
        ]
        _validate_policy_snapshot(
            snapshot,
            policy_links,
            not expected_scope,
            route_sides=route_sides,
        )
    return by_scope


def _build_v2_state_overrides(
    *, calldata: str, adapter: Mapping[str, Any]
) -> Dict[str, Any]:
    decoded = decode_v2_swap_calldata(calldata)
    input_token = decoded["path"][0]
    descriptor = _funding_descriptor(adapter, input_token)
    sender = adapter["simulation_sender_address"]
    router = adapter["router_address"]
    amount_in = decoded.get("amount_in_raw", decoded.get("amount_in_max_raw"))
    word = "0x" + format(amount_in, "064x")
    state_diff = {
        solidity_balance_storage_key(
            sender, int(descriptor["balance_mapping_slot"])
        ): word,
        solidity_allowance_storage_key(
            sender, router, int(descriptor["allowance_mapping_slot"])
        ): word,
    }
    value = {
        sender: {
            "balance": "0x" + format(
                ETHEREUM_V2_SIMULATION_SENDER_NATIVE_BALANCE_WEI, "064x"
            )
        },
        input_token: {
            "stateDiff": {
                key: state_diff[key] for key in sorted(state_diff)
            }
        },
    }
    result = {key: value[key] for key in sorted(value)}
    _validate_state_overrides(result, calldata=calldata, adapter=adapter)
    return result


def _v2_quote_for_target(
    *,
    direction: str,
    target_token: str,
    target_raw: int,
    retained_pool_state: Mapping[str, Any],
) -> Tuple[int, int]:
    token0 = _address(retained_pool_state.get("token0_address"), "retained token0")
    token1 = _address(retained_pool_state.get("token1_address"), "retained token1")
    target = _address(target_token, "simulation target token")
    if target not in {token0, token1}:
        raise RouteCostEvidenceError("call target differs from retained pool")
    target_value = _uint256(target_raw, "simulation target raw quantity", positive=True)
    target_is_token0 = target == token0
    reserve_target = int(_integer_text(
        retained_pool_state.get(
            "reserve0_raw" if target_is_token0 else "reserve1_raw"
        ),
        "retained target reserve",
        positive=True,
    ))
    reserve_other = int(_integer_text(
        retained_pool_state.get(
            "reserve1_raw" if target_is_token0 else "reserve0_raw"
        ),
        "retained other reserve",
        positive=True,
    ))
    fee_numerator = int(_integer_text(
        retained_pool_state.get("fee_numerator"),
        "retained fee numerator",
        positive=True,
    ))
    fee_denominator = int(_integer_text(
        retained_pool_state.get("fee_denominator"),
        "retained fee denominator",
        positive=True,
    ))
    if fee_numerator > fee_denominator:
        raise RouteCostEvidenceError("retained fee fraction is invalid")
    if direction == "sell":
        amount_in = target_value
        amount_with_fee = target_value * fee_numerator
        amount_out = (
            amount_with_fee * reserve_other
            // (reserve_target * fee_denominator + amount_with_fee)
        )
        if amount_out <= 0:
            raise RouteCostEvidenceError("call target output is below one raw unit")
    elif direction == "buy":
        if target_value >= reserve_target:
            raise RouteCostEvidenceError("call target exceeds retained reserve")
        amount_out = target_value
        amount_in = (
            reserve_other * target_value * fee_denominator
            // ((reserve_target - target_value) * fee_numerator)
            + 1
        )
    else:
        raise RouteCostEvidenceError("V2 direction is invalid")
    return amount_in, amount_out


def _build_calldata_unavailable_transcripts(
    *,
    market_ids: Iterable[str],
    transcript_links: Mapping[str, Any],
    selected_by_id: Mapping[str, Mapping[str, Any]],
    chain: Mapping[str, Any],
    chain_sha: str,
    market_by_id: Mapping[str, Mapping[str, Any]],
    market_hashes: Mapping[str, Mapping[str, Any]],
    retained: Mapping[str, Mapping[str, Any]],
    adapter: Mapping[str, Any],
    simulation_targets: Mapping[Tuple[str, str], Mapping[str, str]],
    captured_started_at: str,
    captured_finished_at: str,
    native_sha: Optional[str],
    native_evidence: Optional[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    """Close retained members whose cohort-global calldata target is absent."""
    header = chain["block_header_result"]
    rows: List[Dict[str, Any]] = []
    for market_id in sorted(market_ids):
        state = retained[market_id]
        market = market_by_id[market_id]
        market_sha = physical_sha256(market)
        if market_hashes.get(market_sha) != market:
            raise RouteCostEvidenceError(
                "calldata terminal market evidence differs"
            )
        for direction in ("buy", "sell"):
            for notional in REQUESTED_NOTIONALS_USD:
                if simulation_targets.get((market_id, notional)) is not None:
                    raise RouteCostEvidenceError(
                        "calldata terminal simulation target is present"
                    )
                raw = {
                    "schema": ROUTE_COST_RAW_TRANSCRIPT_SCHEMA,
                    "chain_evidence_sha256": chain_sha,
                    "market_evidence_sha256": market_sha,
                    "captured_started_at": captured_started_at,
                    "captured_finished_at": captured_finished_at,
                    "calldata_hex": None,
                    "estimate_gas_request": None,
                    "estimate_gas_response": None,
                    "simulation_method": None,
                    "simulation_request": None,
                    "simulation_response": None,
                    "simulation_balance_deltas": None,
                }
                transcript = {
                    "schema": ROUTE_COST_TRANSCRIPT_SCHEMA,
                    **transcript_links,
                    "market_id": market_id,
                    "direction": direction,
                    "requested_notional_usd": notional,
                    "adapter_id": ETHEREUM_V2_ADAPTER_ID,
                    "simulation_target_token_address": None,
                    "simulation_target_unit_decimals": None,
                    "simulation_target_raw_quantity": None,
                    "simulation_target_lattice_raw": None,
                    "simulation_target_sha256": None,
                    "core_pool_state_id": state["state_id"],
                    "core_pool_state_sha256": state["_physical_sha256"],
                    "chain_evidence_sha256": chain_sha,
                    "market_evidence_sha256": market_sha,
                    "status": "unavailable",
                    "completed_stage": "block",
                    "reason_code": "calldata_unavailable",
                    "block_evidence": {
                        "schema": ROUTE_COST_BLOCK_EVIDENCE_SCHEMA,
                        "chain_evidence_sha256": chain_sha,
                        "market_evidence_sha256": market_sha,
                        "chain_id": 1,
                        "block_tag": header["number"],
                        "block_number": header["number"],
                        "block_hash": header["hash"],
                        "block_timestamp": header["timestamp"],
                        "core_pool_state_id": state["state_id"],
                        "router_runtime_code_sha256": adapter[
                            "router_runtime_code_sha256"
                        ],
                        "factory_runtime_code_sha256": adapter[
                            "factory_runtime_code_sha256"
                        ],
                        "pair_runtime_code_sha256": hashlib.sha256(
                            bytes.fromhex(market["pair_runtime_code"][2:])
                        ).hexdigest(),
                        "rpc_transcript_sha256": _raw_rpc_transcript_sha256(raw),
                    },
                    "call_evidence": None,
                    "gas_evidence": None,
                    "router_fee_evidence": None,
                    "transfer_tax_evidence": None,
                    "raw_transcript": raw,
                }
                _validate_transcript(
                    transcript,
                    links=transcript_links,
                    selected=selected_by_id,
                    chain_hashes={chain_sha: chain},
                    market_hashes=market_hashes,
                    adapter=adapter,
                    native_sha=native_sha,
                    native_evidence=native_evidence,
                    retained_pool_states=retained,
                    simulation_targets=simulation_targets,
                    evaluated_at=captured_finished_at,
                )
                rows.append(transcript)
    return rows


def _phase_b_terminal_core_reasons(
    *,
    supported: Iterable[str],
    retained: Iterable[str],
    terminal_reason_by_market: Optional[Mapping[str, str]],
) -> Dict[str, str]:
    """Close the supported markets intentionally excluded from Phase B I/O."""
    supported_set = set(supported)
    retained_set = set(retained)
    if terminal_reason_by_market is None:
        reasons: Dict[str, str] = {}
    elif isinstance(terminal_reason_by_market, Mapping):
        reasons = dict(terminal_reason_by_market)
    else:
        raise RouteCostEvidenceError(
            "Phase B terminal core-state inventory is invalid"
        )
    if (
        not retained_set
        or not retained_set.issubset(supported_set)
        or set(reasons) != supported_set - retained_set
    ):
        raise RouteCostEvidenceError("Phase B retained market denominator differs")
    if any(reason not in {
        "core_pool_state_unavailable", "core_pool_state_invalid",
    } for reason in reasons.values()):
        raise RouteCostEvidenceError(
            "Phase B terminal core-state reason is invalid"
        )
    return reasons


def build_phase_b_scenario_request_plan(
    *,
    universe: Mapping[str, Any],
    run_id: str,
    route_cohort_id: str,
    candidate_source_generation: str,
    route_universe_sha256: str,
    adapter_registry: Mapping[str, Any],
    trace_profile_identity: Mapping[str, Any],
    submission_connector_profile_identity: Mapping[str, Any],
    retained_typed_pool_state_members: Mapping[str, Mapping[str, Any]],
    native_price_evidence: Mapping[str, Any],
    submission_policy_snapshot: Mapping[str, Any],
    native_bound_phase_a_capture: Mapping[str, Any],
    terminal_reason_by_market: Optional[Mapping[str, str]] = None,
) -> Dict[str, Any]:
    """Build the closed fixed-block gas-estimate plan for every scenario."""
    (
        registry,
        trace_identity,
        _connector_identity,
        _connector_registry,
        selected,
        selected_by_id,
        links,
    ) = _producer_lineage(
        universe=universe,
        run_id=run_id,
        route_cohort_id=route_cohort_id,
        candidate_source_generation=candidate_source_generation,
        route_universe_sha256=route_universe_sha256,
        adapter_registry=adapter_registry,
        trace_profile_identity=trace_profile_identity,
        submission_connector_profile_identity=(
            submission_connector_profile_identity
        ),
    )
    if trace_identity["status"] != "available":
        raise RouteCostEvidenceError("Phase B trace profile is unavailable")
    supported = {
        market_id for market_id, row in selected_by_id.items()
        if row["structural_support_status"] == "supported"
    }
    retained = _retained_pool_states_for_terminal_inventory(
        retained_typed_pool_state_members, supported
    )
    _phase_b_terminal_core_reasons(
        supported=supported,
        retained=retained,
        terminal_reason_by_market=terminal_reason_by_market,
    )
    native = _canonical_copy(native_price_evidence)
    native_sha = _validate_native_price_evidence(native, links)
    _exact_fields(
        native_bound_phase_a_capture,
        ("chain_evidence", "market_evidence"),
        "native-bound Phase A capture",
    )
    chains = native_bound_phase_a_capture.get("chain_evidence")
    markets = native_bound_phase_a_capture.get("market_evidence")
    if (
        not isinstance(chains, list)
        or len(chains) != 1
        or not isinstance(markets, list)
        or [row.get("market_id") for row in markets] != sorted(retained)
    ):
        raise RouteCostEvidenceError("native-bound Phase A denominator differs")
    chain = _canonical_copy(chains[0])
    chain_sha = _validate_chain_evidence(
        chain, links, trace_profile_identity=trace_identity
    )
    expected_price = _format_decimal(
        Decimal(native["book_projection"]["best_ask_price"])
        * Decimal(native["usd_conversion_projection"]["rate"])
    )
    native_record = chain["native_price_record"]
    if (
        chain.get("status") != "observed"
        or chain.get("reason_code") is not None
        or native_record.get("status") != "observed"
        or native_record.get("price_usd") != expected_price
        or native_record.get("native_price_evidence_sha256") != native_sha
        or native_record.get("source_record_sha256")
        != native.get("source_record_sha256")
        or native_record.get("observed_at") != native.get("observed_at")
        or native_record.get("valid_until") != native.get("valid_until")
    ):
        raise RouteCostEvidenceError("native-bound Phase A price differs")
    adapter_rows = [
        row for row in registry["adapters"]
        if row["adapter_id"] == ETHEREUM_V2_ADAPTER_ID
    ]
    if len(adapter_rows) != 1:
        raise RouteCostEvidenceError("Phase B adapter is absent")
    adapter = adapter_rows[0]
    market_by_id: Dict[str, Dict[str, Any]] = {}
    chain_hashes = {chain_sha: chain}
    for row in markets:
        market = _canonical_copy(row)
        _validate_market_evidence(
            market, links, chain_hashes, set(retained), adapter, retained
        )
        market_by_id[market["market_id"]] = market

    expected_scope, route_sides = _expected_binding_scope(
        universe, selected_by_id
    )
    snapshot = _canonical_copy(submission_policy_snapshot)
    policy_members = _phase_b_structural_policy_members(
        snapshot,
        links=links,
        expected_scope=expected_scope,
        route_sides=route_sides,
    )
    routes_by_id = {
        row["route_id"]: row for row in universe.get("routes", [])
        if isinstance(row, Mapping) and row.get("route_class") == "candidate"
    }
    universe_legs = universe.get("selected_legs")
    if not isinstance(universe_legs, list):
        raise RouteCostEvidenceError("route universe selected legs are invalid")
    targets = build_simulation_targets(
        universe_legs, selected_by_id, retained
    )
    target_complete = {
        market_id for market_id, _notional in targets
    }
    expected_target_keys = {
        (market_id, notional)
        for market_id in target_complete
        for notional in REQUESTED_NOTIONALS_USD
    }
    if set(targets) != expected_target_keys or not target_complete.issubset(
        set(retained)
    ):
        raise RouteCostEvidenceError(
            "Phase B simulation target inventory differs"
        )
    token_identity: Dict[str, Tuple[str, str]] = {}
    for leg in universe_legs:
        market_id = leg.get("market_id") if isinstance(leg, Mapping) else None
        if market_id not in target_complete:
            continue
        identity = _strict_dex_target_identity(leg)
        if identity is None:
            raise RouteCostEvidenceError("Phase B selected token identity is absent")
        target_token, other_token, _price, _side = identity
        token_identity[str(market_id)] = (target_token, other_token)
    if set(token_identity) != target_complete:
        raise RouteCostEvidenceError("Phase B selected token denominator differs")

    phase_a_count = 3 + 8 * len(retained)
    scenario_count = 2 * len(target_complete) * len(REQUESTED_NOTIONALS_USD)
    specs: List[Dict[str, Any]] = []
    estimates: List[Dict[str, Any]] = []
    for market_id in sorted(target_complete):
        state = retained[market_id]
        market = market_by_id[market_id]
        market_sha = physical_sha256(market)
        target_token, other_token = token_identity[market_id]
        for direction in ("buy", "sell"):
            for notional in REQUESTED_NOTIONALS_USD:
                target = targets.get((market_id, notional))
                if target is None:
                    raise RouteCostEvidenceError("Phase B simulation target is absent")
                amount_in, amount_out = _v2_quote_for_target(
                    direction=direction,
                    target_token=target_token,
                    target_raw=int(target["simulation_target_raw_quantity"]),
                    retained_pool_state=state,
                )
                relevant_bounds: List[int] = []
                for route_id, member_notional in expected_scope:
                    if member_notional != notional:
                        continue
                    route = routes_by_id[route_id]
                    side_field = direction + "_submission_loss_bps"
                    if route.get(direction + "_market_id") != market_id:
                        continue
                    member = policy_members[(route_id, notional)]
                    relevant_bounds.append(
                        int(member[side_field])
                        if member.get("status") == "observed"
                        else 100
                    )
                if snapshot.get("status") != "authenticated" or not relevant_bounds:
                    relevant_bounds = [100]
                if len(set(relevant_bounds)) != 1:
                    raise RouteCostEvidenceError(
                        "Phase B submission bound differs across routes"
                    )
                bound = relevant_bounds[0]
                token_in, token_out = (
                    (other_token, target_token)
                    if direction == "buy"
                    else (target_token, other_token)
                )
                deadline = int(chain["block_header_result"]["timestamp"], 16) + 300
                calldata = build_v2_swap_calldata(
                    direction=direction,
                    quoted_amount_in_raw=amount_in,
                    quoted_amount_out_raw=amount_out,
                    submission_loss_bound_bps=bound,
                    path_token_in=token_in,
                    path_token_out=token_out,
                    recipient=adapter["simulation_sender_address"],
                    deadline=deadline,
                )
                overrides = _build_v2_state_overrides(
                    calldata=calldata, adapter=adapter
                )
                index = len(specs)
                estimate_id = phase_a_count + index + 1
                trace_id = phase_a_count + scenario_count + index + 1
                spec = {
                    "schema": "route_cost_phase_b_scenario_spec/v1",
                    "market_id": market_id,
                    "direction": direction,
                    "requested_notional_usd": notional,
                    **target,
                    "core_pool_state_id": state["state_id"],
                    "core_pool_state_sha256": state["_physical_sha256"],
                    "chain_evidence_sha256": chain_sha,
                    "market_evidence_sha256": market_sha,
                    "quoted_amount_in_raw": str(amount_in),
                    "quoted_amount_out_raw": str(amount_out),
                    "submission_loss_bound_bps": str(bound),
                    "calldata_hex": calldata,
                    "state_overrides": overrides,
                    "estimate_request_id": estimate_id,
                    "trace_request_id": trace_id,
                }
                estimate = {
                    "schema": "route_cost_estimate_gas_request/v1",
                    "jsonrpc": "2.0",
                    "id": estimate_id,
                    "method": "eth_estimateGas",
                    "params": [{
                        "from": adapter["simulation_sender_address"],
                        "to": adapter["router_address"],
                        "data": calldata,
                        "value": "0x0",
                    }, chain["block_header_result"]["number"], overrides],
                }
                specs.append(spec)
                estimates.append(estimate)
    return _canonical_copy({
        "schema": "route_cost_phase_b_scenario_plan/v1",
        "phase_a_rpc_call_count": phase_a_count,
        "scenario_specs": specs,
        "estimate_requests": estimates,
    })


def build_phase_b_trace_request_plan(
    *,
    scenario_plan: Mapping[str, Any],
    estimate_responses: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    """Release trace requests only after the complete estimate barrier passes."""
    _exact_fields(
        scenario_plan,
        (
            "schema", "phase_a_rpc_call_count", "scenario_specs",
            "estimate_requests",
        ),
        "Phase B scenario plan",
    )
    if scenario_plan.get("schema") != "route_cost_phase_b_scenario_plan/v1":
        raise RouteCostEvidenceError("Phase B scenario plan schema is invalid")
    phase_a_count = _exact_int(
        scenario_plan.get("phase_a_rpc_call_count"),
        "Phase B Phase-A RPC call count",
        3,
        3 + 8 * MAX_SELECTED_MARKETS,
    )
    specs = scenario_plan.get("scenario_specs")
    requests = scenario_plan.get("estimate_requests")
    if (
        not isinstance(specs, list)
        or not isinstance(requests, list)
        or len(specs) != len(requests)
        or len(specs) > MAX_TRANSCRIPTS
    ):
        raise RouteCostEvidenceError("Phase B scenario denominator differs")
    spec_fields = (
        "schema", "market_id", "direction", "requested_notional_usd",
        "simulation_target_token_address",
        "simulation_target_unit_decimals",
        "simulation_target_raw_quantity", "simulation_target_lattice_raw",
        "simulation_target_sha256", "core_pool_state_id",
        "core_pool_state_sha256", "chain_evidence_sha256",
        "market_evidence_sha256", "quoted_amount_in_raw",
        "quoted_amount_out_raw", "submission_loss_bound_bps",
        "calldata_hex", "state_overrides", "estimate_request_id",
        "trace_request_id",
    )
    expected_estimate_ids = list(
        range(phase_a_count + 1, phase_a_count + len(specs) + 1)
    )
    expected_trace_ids = list(
        range(
            phase_a_count + len(specs) + 1,
            phase_a_count + 2 * len(specs) + 1,
        )
    )
    for index, (spec, request) in enumerate(zip(specs, requests)):
        _exact_fields(spec, spec_fields, "Phase B scenario spec")
        if spec.get("schema") != "route_cost_phase_b_scenario_spec/v1":
            raise RouteCostEvidenceError("Phase B scenario spec schema is invalid")
        if spec.get("direction") not in {"buy", "sell"} or spec.get(
            "requested_notional_usd"
        ) not in REQUESTED_NOTIONALS_USD:
            raise RouteCostEvidenceError("Phase B scenario identity is invalid")
        estimate_id = _exact_int(
            spec.get("estimate_request_id"), "Phase B estimate request ID", 1
        )
        trace_id = _exact_int(
            spec.get("trace_request_id"), "Phase B trace request ID", 1
        )
        if (
            estimate_id != expected_estimate_ids[index]
            or trace_id != expected_trace_ids[index]
        ):
            raise RouteCostEvidenceError("Phase B request ID inventory differs")
        _exact_fields(request, ESTIMATE_GAS_REQUEST_FIELDS, "estimate-gas request")
        if (
            request.get("schema") != "route_cost_estimate_gas_request/v1"
            or request.get("jsonrpc") != "2.0"
            or request.get("id") != estimate_id
            or request.get("method") != "eth_estimateGas"
        ):
            raise RouteCostEvidenceError("Phase B estimate request differs")
        params = request.get("params")
        if not isinstance(params, list) or len(params) != 3:
            raise RouteCostEvidenceError("Phase B estimate params are invalid")
        call = _exact_fields(
            params[0], ESTIMATE_CALL_OBJECT_FIELDS, "estimate call object"
        )
        if (
            call.get("data") != spec.get("calldata_hex")
            or params[2] != spec.get("state_overrides")
            or call.get("value") != "0x0"
        ):
            raise RouteCostEvidenceError("Phase B estimate scenario differs")
        _quantity(params[1], "Phase B estimate block tag", positive=True)
        _address(call.get("from"), "Phase B estimate sender")
        _address(call.get("to"), "Phase B estimate router")
        decoded = decode_v2_swap_calldata(spec.get("calldata_hex"))
        if (
            decoded["selector"]
            != (
                ETHEREUM_V2_BUY_SELECTOR
                if spec["direction"] == "buy"
                else ETHEREUM_V2_SELL_SELECTOR
            )
            or decoded["recipient"] != call["from"]
        ):
            raise RouteCostEvidenceError("Phase B calldata identity differs")

    if not isinstance(estimate_responses, Sequence) or isinstance(
        estimate_responses, (str, bytes, bytearray)
    ):
        raise RouteCostEvidenceError("Phase B estimate responses are invalid")
    by_id: Dict[int, Mapping[str, Any]] = {}
    for row in estimate_responses:
        if (
            not isinstance(row, Mapping)
            or set(row) != {"jsonrpc", "id", "result"}
            or row.get("jsonrpc") != "2.0"
            or type(row.get("id")) is not int
            or row["id"] in by_id
        ):
            raise RouteCostEvidenceError("Phase B estimate response inventory is invalid")
        by_id[row["id"]] = row
    if set(by_id) != set(expected_estimate_ids) or len(by_id) != len(specs):
        raise RouteCostEvidenceError("Phase B estimate response denominator differs")
    normalized: List[Dict[str, Any]] = []
    traces: List[Dict[str, Any]] = []
    for spec, request in zip(specs, requests):
        estimate_id = spec["estimate_request_id"]
        result = _quantity(
            by_id[estimate_id].get("result"),
            "Phase B estimate result",
            positive=True,
        )
        normalized.append({
            "schema": "route_cost_estimate_gas_response/v1",
            "jsonrpc": "2.0",
            "id": estimate_id,
            "result": result,
        })
        call = request["params"][0]
        traces.append({
            "schema": "route_cost_trace_request/v1",
            "jsonrpc": "2.0",
            "id": spec["trace_request_id"],
            "method": "debug_traceCall",
            "params": [{
                "from": call["from"],
                "to": call["to"],
                "gas": result,
                "data": spec["calldata_hex"],
                "value": "0x0",
            }, request["params"][1], {
                "tracer": "prestateTracer",
                "tracerConfig": {
                    "diffMode": True,
                    "disableCode": True,
                    "disableStorage": False,
                },
                "stateOverrides": spec["state_overrides"],
            }],
        })
    return _canonical_copy({
        "schema": "route_cost_phase_b_trace_plan/v1",
        "estimate_responses": normalized,
        "trace_requests": traces,
    })


def project_phase_b_capture(
    *,
    universe: Mapping[str, Any],
    run_id: str,
    route_cohort_id: str,
    candidate_source_generation: str,
    route_universe_sha256: str,
    adapter_registry: Mapping[str, Any],
    trace_profile_identity: Mapping[str, Any],
    submission_connector_profile_identity: Mapping[str, Any],
    retained_typed_pool_state_members: Mapping[str, Mapping[str, Any]],
    native_price_evidence: Mapping[str, Any],
    submission_policy_snapshot: Mapping[str, Any],
    native_bound_phase_a_capture: Mapping[str, Any],
    scenario_plan: Mapping[str, Any],
    trace_plan: Mapping[str, Any],
    trace_responses: Sequence[Mapping[str, Any]],
    captured_started_at: str,
    captured_finished_at: str,
    terminal_reason_by_market: Optional[Mapping[str, str]] = None,
) -> List[Dict[str, Any]]:
    """Project sanitized fixed-tracer results into canonical transcripts.

    ``trace_responses`` starts after the collector's private raw-Geth decoder:
    each row is already the closed ``route_cost_trace_response/v1`` shape.
    This pure boundary replays every authority, plan, response ID, balance
    delta, component hash, and final transcript contract.
    """
    _ordered_timestamps(
        captured_started_at, captured_finished_at, "Phase B capture"
    )
    expected_scenario_plan = build_phase_b_scenario_request_plan(
        universe=universe,
        run_id=run_id,
        route_cohort_id=route_cohort_id,
        candidate_source_generation=candidate_source_generation,
        route_universe_sha256=route_universe_sha256,
        adapter_registry=adapter_registry,
        trace_profile_identity=trace_profile_identity,
        submission_connector_profile_identity=(
            submission_connector_profile_identity
        ),
        retained_typed_pool_state_members=retained_typed_pool_state_members,
        native_price_evidence=native_price_evidence,
        submission_policy_snapshot=submission_policy_snapshot,
        native_bound_phase_a_capture=native_bound_phase_a_capture,
        terminal_reason_by_market=terminal_reason_by_market,
    )
    if scenario_plan != expected_scenario_plan:
        raise RouteCostEvidenceError("Phase B scenario plan differs")
    _exact_fields(
        trace_plan,
        ("schema", "estimate_responses", "trace_requests"),
        "Phase B trace plan",
    )
    if trace_plan.get("schema") != "route_cost_phase_b_trace_plan/v1":
        raise RouteCostEvidenceError("Phase B trace plan schema is invalid")
    normalized_estimates = trace_plan.get("estimate_responses")
    if not isinstance(normalized_estimates, list):
        raise RouteCostEvidenceError("Phase B estimate responses are invalid")
    raw_estimates: List[Dict[str, Any]] = []
    for row in normalized_estimates:
        _exact_fields(
            row, ESTIMATE_GAS_RESPONSE_FIELDS, "estimate-gas response"
        )
        if row.get("schema") != "route_cost_estimate_gas_response/v1":
            raise RouteCostEvidenceError("Phase B estimate response schema is invalid")
        raw_estimates.append({
            "jsonrpc": row.get("jsonrpc"),
            "id": row.get("id"),
            "result": row.get("result"),
        })
    expected_trace_plan = build_phase_b_trace_request_plan(
        scenario_plan=expected_scenario_plan,
        estimate_responses=raw_estimates,
    )
    if trace_plan != expected_trace_plan:
        raise RouteCostEvidenceError("Phase B trace plan differs")

    (
        registry,
        trace_identity,
        _connector_identity,
        _connector_registry,
        selected,
        selected_by_id,
        links,
    ) = _producer_lineage(
        universe=universe,
        run_id=run_id,
        route_cohort_id=route_cohort_id,
        candidate_source_generation=candidate_source_generation,
        route_universe_sha256=route_universe_sha256,
        adapter_registry=adapter_registry,
        trace_profile_identity=trace_profile_identity,
        submission_connector_profile_identity=(
            submission_connector_profile_identity
        ),
    )
    supported = {
        market_id for market_id, row in selected_by_id.items()
        if row["structural_support_status"] == "supported"
    }
    retained = _retained_pool_states_for_terminal_inventory(
        retained_typed_pool_state_members, supported
    )
    _phase_b_terminal_core_reasons(
        supported=supported,
        retained=retained,
        terminal_reason_by_market=terminal_reason_by_market,
    )
    native = _canonical_copy(native_price_evidence)
    native_sha = _validate_native_price_evidence(native, links)
    native_observed = _timestamp_value(
        native.get("observed_at"), "Phase B native observed_at"
    )
    native_valid = _timestamp_value(
        native.get("valid_until"), "Phase B native valid_until"
    )
    phase_b_started = _timestamp_value(
        captured_started_at, "Phase B captured_started_at"
    )
    phase_b_finished = _timestamp_value(
        captured_finished_at, "Phase B captured_finished_at"
    )
    if not (
        native_observed <= phase_b_started <= phase_b_finished <= native_valid
    ):
        raise RouteCostEvidenceError(
            "Phase B native validity does not cover capture window"
        )
    chains = native_bound_phase_a_capture["chain_evidence"]
    markets = native_bound_phase_a_capture["market_evidence"]
    chain = _canonical_copy(chains[0])
    chain_sha = _validate_chain_evidence(
        chain, links, trace_profile_identity=trace_identity
    )
    adapter_rows = [
        row for row in registry["adapters"]
        if row["adapter_id"] == ETHEREUM_V2_ADAPTER_ID
    ]
    if len(adapter_rows) != 1:
        raise RouteCostEvidenceError("Phase B adapter is absent")
    adapter = adapter_rows[0]
    chain_hashes = {chain_sha: chain}
    market_by_id: Dict[str, Dict[str, Any]] = {}
    market_hashes: Dict[str, Dict[str, Any]] = {}
    for row in markets:
        market = _canonical_copy(row)
        market_sha = _validate_market_evidence(
            market, links, chain_hashes, set(retained), adapter, retained
        )
        market_by_id[market["market_id"]] = market
        market_hashes[market_sha] = market
    universe_legs = universe.get("selected_legs")
    if not isinstance(universe_legs, list):
        raise RouteCostEvidenceError("route universe selected legs are invalid")
    simulation_targets = build_simulation_targets(
        universe_legs, selected_by_id, retained
    )
    target_complete = {
        market_id for market_id, _notional in simulation_targets
    }
    market_tokens: Dict[str, Tuple[str, str]] = {}
    for leg in universe_legs:
        market_id = leg.get("market_id") if isinstance(leg, Mapping) else None
        if market_id not in retained:
            continue
        identity = _strict_dex_target_identity(leg)
        if identity is None:
            raise RouteCostEvidenceError("Phase B selected token identity is absent")
        market_tokens[str(market_id)] = (identity[0], identity[1])

    if not isinstance(trace_responses, Sequence) or isinstance(
        trace_responses, (str, bytes, bytearray)
    ):
        raise RouteCostEvidenceError("Phase B trace responses are invalid")
    expected_trace_ids = [
        row["id"] for row in expected_trace_plan["trace_requests"]
    ]
    response_by_id: Dict[int, Dict[str, Any]] = {}
    for row in trace_responses:
        _exact_fields(row, TRACE_RESPONSE_FIELDS, "trace response")
        if (
            row.get("schema") != "route_cost_trace_response/v1"
            or row.get("jsonrpc") != "2.0"
            or type(row.get("id")) is not int
            or row["id"] in response_by_id
        ):
            raise RouteCostEvidenceError("Phase B trace response inventory is invalid")
        response_by_id[row["id"]] = _canonical_copy(row)
    if (
        set(response_by_id) != set(expected_trace_ids)
        or len(response_by_id) != len(expected_trace_ids)
    ):
        raise RouteCostEvidenceError("Phase B trace response denominator differs")

    estimate_by_id = {
        row["id"]: row for row in expected_trace_plan["estimate_responses"]
    }
    estimate_request_by_id = {
        row["id"]: row for row in expected_scenario_plan["estimate_requests"]
    }
    trace_request_by_id = {
        row["id"]: row for row in expected_trace_plan["trace_requests"]
    }
    header = chain["block_header_result"]
    fee_history = chain["fee_history_result"]
    next_base = next_base_fee_wei(
        base_fee_per_gas=int(header["base_fee_per_gas"], 16),
        gas_used=int(header["gas_used"], 16),
        gas_limit=int(header["gas_limit"], 16),
    )
    priority = int(fee_history["reward"][0][0], 16)
    native_price = _format_decimal(
        Decimal(native["book_projection"]["best_ask_price"])
        * Decimal(native["usd_conversion_projection"]["rate"])
    )
    transcript_links = {
        field: links[field] for field in (
            "run_id", "route_cohort_id", "candidate_source_generation",
            "route_universe_sha256", "adapter_registry_sha256",
            "selected_market_set_sha256", "trace_profile_generation",
            "submission_connector_profile_generation",
        )
    }
    rows: List[Dict[str, Any]] = []
    for spec in expected_scenario_plan["scenario_specs"]:
        market_id = spec["market_id"]
        market = market_by_id[market_id]
        state = retained[market_id]
        estimate_request = estimate_request_by_id[spec["estimate_request_id"]]
        estimate_response = estimate_by_id[spec["estimate_request_id"]]
        trace_request = trace_request_by_id[spec["trace_request_id"]]
        trace_response = response_by_id[spec["trace_request_id"]]
        deltas = _validate_storage_diffs(
            trace_response["storage_diffs"],
            adapter=adapter,
            market=market,
            calldata=spec["calldata_hex"],
        )
        raw = {
            "schema": ROUTE_COST_RAW_TRANSCRIPT_SCHEMA,
            "chain_evidence_sha256": spec["chain_evidence_sha256"],
            "market_evidence_sha256": spec["market_evidence_sha256"],
            "captured_started_at": captured_started_at,
            "captured_finished_at": captured_finished_at,
            "calldata_hex": spec["calldata_hex"],
            "estimate_gas_request": estimate_request,
            "estimate_gas_response": estimate_response,
            "simulation_method": adapter["trace_method"],
            "simulation_request": trace_request,
            "simulation_response": trace_response,
            "simulation_balance_deltas": deltas,
        }
        decoded = decode_v2_swap_calldata(spec["calldata_hex"])
        sender_rows = [row for row in deltas if row["account_role"] == "sender"]
        recipient_rows = [
            row for row in deltas if row["account_role"] == "recipient"
        ]
        pair_rows = [row for row in deltas if row["account_role"] == "pair"]
        if len(sender_rows) != 1 or len(recipient_rows) != 1 or len(pair_rows) != 2:
            raise RouteCostEvidenceError("Phase B balance delta roles differ")
        sender_delta = sender_rows[0]
        recipient_delta = recipient_rows[0]
        pair_in = next(
            row for row in pair_rows
            if row["token_address"] == decoded["path"][0]
        )
        pair_out = next(
            row for row in pair_rows
            if row["token_address"] == decoded["path"][1]
        )
        amount_in = int(spec["quoted_amount_in_raw"])
        amount_out = int(spec["quoted_amount_out_raw"])
        zero_tax = (
            int(sender_delta["pre_balance_raw"])
            - int(sender_delta["post_balance_raw"]) == amount_in
            and int(pair_in["post_balance_raw"])
            - int(pair_in["pre_balance_raw"]) == amount_in
            and int(pair_out["pre_balance_raw"])
            - int(pair_out["post_balance_raw"]) == amount_out
            and int(recipient_delta["post_balance_raw"])
            - int(recipient_delta["pre_balance_raw"]) == amount_out
        )
        transfer_status = "not_applicable" if zero_tax else "unavailable"
        transcript_status = "observed" if zero_tax else "unavailable"
        transcript_reason = None if zero_tax else "transfer_tax_present"
        market_sha = spec["market_evidence_sha256"]
        call_evidence = {
            "schema": ROUTE_COST_CALL_EVIDENCE_SCHEMA,
            "selector": decoded["selector"],
            "path_token_in": decoded["path"][0],
            "path_token_out": decoded["path"][1],
            "recipient_policy": "same_as_registry_sender/v1",
            "deadline": hex(decoded["deadline"]),
            "amount_in_raw": spec["quoted_amount_in_raw"],
            "amount_out_raw": spec["quoted_amount_out_raw"],
            "calldata_sha256": hashlib.sha256(
                bytes.fromhex(spec["calldata_hex"][2:])
            ).hexdigest(),
            "sender_policy": "registry_fixed_state_override_sender/v1",
            "allowance_basis": "exact_amount_state_override/v1",
            "submission_loss_bound_bps": spec[
                "submission_loss_bound_bps"
            ],
        }
        transcript = {
            "schema": ROUTE_COST_TRANSCRIPT_SCHEMA,
            **transcript_links,
            "market_id": market_id,
            "direction": spec["direction"],
            "requested_notional_usd": spec["requested_notional_usd"],
            "adapter_id": ETHEREUM_V2_ADAPTER_ID,
            **{
                field: spec[field] for field in (
                    "simulation_target_token_address",
                    "simulation_target_unit_decimals",
                    "simulation_target_raw_quantity",
                    "simulation_target_lattice_raw",
                    "simulation_target_sha256",
                )
            },
            "core_pool_state_id": spec["core_pool_state_id"],
            "core_pool_state_sha256": spec["core_pool_state_sha256"],
            "chain_evidence_sha256": spec["chain_evidence_sha256"],
            "market_evidence_sha256": market_sha,
            "status": transcript_status,
            "completed_stage": "transfer_tax",
            "reason_code": transcript_reason,
            "block_evidence": {
                "schema": ROUTE_COST_BLOCK_EVIDENCE_SCHEMA,
                "chain_evidence_sha256": spec["chain_evidence_sha256"],
                "market_evidence_sha256": market_sha,
                "chain_id": 1,
                "block_tag": header["number"],
                "block_number": header["number"],
                "block_hash": header["hash"],
                "block_timestamp": header["timestamp"],
                "core_pool_state_id": spec["core_pool_state_id"],
                "router_runtime_code_sha256": adapter[
                    "router_runtime_code_sha256"
                ],
                "factory_runtime_code_sha256": adapter[
                    "factory_runtime_code_sha256"
                ],
                "pair_runtime_code_sha256": hashlib.sha256(
                    bytes.fromhex(market["pair_runtime_code"][2:])
                ).hexdigest(),
                "rpc_transcript_sha256": _raw_rpc_transcript_sha256(raw),
            },
            "call_evidence": call_evidence,
            "gas_evidence": {
                "schema": ROUTE_COST_GAS_EVIDENCE_SCHEMA,
                "gas_units": str(int(estimate_response["result"], 16)),
                "max_fee_per_gas_wei": str(
                    max_fee_per_gas_wei(next_base, priority)
                ),
                "fee_history_sha256": physical_sha256(fee_history),
                "native_symbol": "ETH",
                "native_price_usd": native_price,
                "native_price_sha256": native_sha,
                "observed_at": native["observed_at"],
                "valid_until": native["valid_until"],
            },
            "router_fee_evidence": {
                "schema": ROUTE_COST_ROUTER_FEE_EVIDENCE_SCHEMA,
                "status": "not_applicable",
                "rate_bps": None,
                "basis_code": (
                    "verified_uniswap_v2_router02_no_integrator_fee/v1"
                ),
                "source_record_sha256": market_sha,
            },
            "transfer_tax_evidence": {
                "schema": ROUTE_COST_TRANSFER_TAX_EVIDENCE_SCHEMA,
                "status": transfer_status,
                "rate_bps": None,
                "pre_input_balance": sender_delta["pre_balance_raw"],
                "post_input_balance": sender_delta["post_balance_raw"],
                "pre_output_balance": recipient_delta["pre_balance_raw"],
                "post_output_balance": recipient_delta["post_balance_raw"],
                "trace_method": adapter["trace_method"],
                "trace_sha256": _raw_trace_sha256(raw),
            },
            "raw_transcript": raw,
        }
        _validate_transcript(
            transcript,
            links=links,
            selected=selected_by_id,
            chain_hashes=chain_hashes,
            market_hashes=market_hashes,
            adapter=adapter,
            native_sha=native_sha,
            native_evidence=native,
            retained_pool_states=retained,
            market_tokens=market_tokens,
            simulation_targets=simulation_targets,
            evaluated_at=captured_finished_at,
        )
        rows.append(transcript)
    rows.extend(_build_calldata_unavailable_transcripts(
        market_ids=set(retained) - target_complete,
        transcript_links=transcript_links,
        selected_by_id=selected_by_id,
        chain=chain,
        chain_sha=chain_sha,
        market_by_id=market_by_id,
        market_hashes=market_hashes,
        retained=retained,
        adapter=adapter,
        simulation_targets=simulation_targets,
        captured_started_at=captured_started_at,
        captured_finished_at=captured_finished_at,
        native_sha=native_sha,
        native_evidence=native,
    ))
    return _canonical_copy(rows)


def _decode_address_word(value: Any, label: str) -> str:
    raw = _decoded_hex_bytes(value, label, 32)
    if len(raw) != 32 or any(raw[:12]):
        raise RouteCostEvidenceError("{} ABI is invalid".format(label))
    return _address("0x" + raw[12:].hex(), label)


def _validate_factory_get_pair_evidence(
    request: Any,
    response: Any,
    *,
    adapter: Mapping[str, Any],
    chain: Mapping[str, Any],
    token0: str,
    token1: str,
    pair: str,
) -> None:
    """Replay one exact fixed-block factory lookup and its ABI result."""
    _exact_fields(request, FACTORY_GET_PAIR_REQUEST_FIELDS, "factory getPair request")
    _exact_fields(
        response, FACTORY_GET_PAIR_RESPONSE_FIELDS, "factory getPair response"
    )
    if (
        request.get("schema") != ROUTE_COST_FACTORY_GET_PAIR_REQUEST_SCHEMA
        or request.get("jsonrpc") != "2.0"
        or request.get("method") != "eth_call"
        or response.get("schema") != ROUTE_COST_FACTORY_GET_PAIR_RESPONSE_SCHEMA
        or response.get("jsonrpc") != "2.0"
    ):
        raise RouteCostEvidenceError("factory getPair RPC identity is invalid")
    request_id = _exact_int(request.get("id"), "factory getPair request ID", 1)
    if response.get("id") != request_id:
        raise RouteCostEvidenceError("factory getPair response ID differs")
    params = request.get("params")
    if not isinstance(params, list) or len(params) != 2:
        raise RouteCostEvidenceError("factory getPair params are invalid")
    call = params[0]
    _exact_fields(call, FACTORY_GET_PAIR_CALL_FIELDS, "factory getPair call")
    if (
        call.get("to") != adapter.get("factory_address")
        or call.get("data") != build_factory_get_pair_calldata(token0, token1)
        or params[1] != chain.get("block_header_result", {}).get("number")
    ):
        raise RouteCostEvidenceError("factory getPair call differs from fixed market")
    result = _decoded_hex_bytes(
        response.get("result"), "factory getPair result", 32
    )
    if len(result) != 32 or any(result[:12]):
        raise RouteCostEvidenceError("factory getPair result ABI is invalid")
    returned_pair = _address("0x" + result[12:].hex(), "factory getPair result")
    if returned_pair != _address(pair, "factory getPair pair"):
        raise RouteCostEvidenceError("factory getPair returned a different pair")


def _validate_token_runtime_code_evidence(
    value: Any,
    *,
    adapter: Mapping[str, Any],
    chain: Mapping[str, Any],
    token0: str,
    token1: str,
) -> None:
    """Replay the complete fixed-block runtime bytes for both funding tokens."""
    if not isinstance(value, list) or len(value) != 2:
        raise RouteCostEvidenceError("token runtime-code inventory differs")
    expected_tokens = [token0, token1]
    if expected_tokens != sorted(expected_tokens):
        expected_tokens = sorted(expected_tokens)
    identities: List[str] = []
    request_ids: List[int] = []
    for member in value:
        _exact_fields(
            member, TOKEN_RUNTIME_CODE_EVIDENCE_FIELDS,
            "token runtime-code evidence",
        )
        if member.get("schema") != ROUTE_COST_TOKEN_RUNTIME_CODE_EVIDENCE_SCHEMA:
            raise RouteCostEvidenceError("token runtime-code evidence schema is invalid")
        token = _address(member.get("token_address"), "runtime-code token")
        identities.append(token)
        request = member.get("request")
        response = member.get("response")
        _exact_fields(
            request, TOKEN_RUNTIME_CODE_REQUEST_FIELDS,
            "token runtime-code request",
        )
        _exact_fields(
            response, TOKEN_RUNTIME_CODE_RESPONSE_FIELDS,
            "token runtime-code response",
        )
        if (
            request.get("schema") != ROUTE_COST_TOKEN_RUNTIME_CODE_REQUEST_SCHEMA
            or request.get("jsonrpc") != "2.0"
            or request.get("method") != "eth_getCode"
            or response.get("schema")
            != ROUTE_COST_TOKEN_RUNTIME_CODE_RESPONSE_SCHEMA
            or response.get("jsonrpc") != "2.0"
        ):
            raise RouteCostEvidenceError("token runtime-code RPC identity is invalid")
        request_id = _exact_int(
            request.get("id"), "token runtime-code request ID", 1
        )
        request_ids.append(request_id)
        if response.get("id") != request_id:
            raise RouteCostEvidenceError("token runtime-code response ID differs")
        if request.get("params") != [
            token, chain.get("block_header_result", {}).get("number")
        ]:
            raise RouteCostEvidenceError("token runtime-code request differs")
        code = _decoded_hex_bytes(
            response.get("result"), "token runtime code", MAX_RUNTIME_CODE_BYTES
        )
        if not code:
            raise RouteCostEvidenceError("token runtime code is empty")
        descriptor = _funding_descriptor(adapter, token)
        if hashlib.sha256(code).hexdigest() != descriptor.get(
            "runtime_code_sha256"
        ):
            raise RouteCostEvidenceError("token funding runtime-code identity differs")
    if identities != expected_tokens or len(set(request_ids)) != len(request_ids):
        raise RouteCostEvidenceError("token runtime-code inventory is not canonical")


def _pad_address(value: str) -> bytes:
    return bytes.fromhex(_address(value, "storage account")[2:]).rjust(32, b"\0")


def _pad_slot(value: int) -> bytes:
    return _uint256(value, "storage slot").to_bytes(32, "big")


def solidity_balance_storage_key(owner: str, balance_slot: int) -> str:
    return "0x" + keccak256(_pad_address(owner) + _pad_slot(balance_slot)).hex()


def solidity_allowance_storage_key(owner: str, spender: str, allowance_slot: int) -> str:
    inner = keccak256(_pad_address(owner) + _pad_slot(allowance_slot))
    return "0x" + keccak256(_pad_address(spender) + inner).hex()


def _word(value: Any, label: str) -> str:
    if (
        not isinstance(value, str)
        or re.fullmatch(r"0x[0-9a-f]{64}", value, flags=re.ASCII) is None
    ):
        raise RouteCostEvidenceError("{} is invalid".format(label))
    return value


def _funding_descriptor(
    adapter: Mapping[str, Any], token: str
) -> Mapping[str, Any]:
    matches = [
        row for row in adapter.get("token_funding_descriptors", [])
        if row.get("token_address") == token
    ]
    if len(matches) != 1:
        raise RouteCostEvidenceError("input token funding descriptor is absent")
    return matches[0]


def _validate_state_overrides(
    value: Any, *, calldata: Any, adapter: Mapping[str, Any]
) -> None:
    if not isinstance(value, Mapping):
        raise RouteCostEvidenceError("state overrides are invalid")
    decoded = decode_v2_swap_calldata(calldata)
    input_token = decoded["path"][0]
    descriptor = _funding_descriptor(adapter, input_token)
    sender = adapter["simulation_sender_address"]
    router = adapter["router_address"]
    if list(value) != sorted(value) or set(value) != {sender, input_token}:
        raise RouteCostEvidenceError("state override accounts differ")
    sender_override = _exact_fields(
        value[sender], BALANCE_OVERRIDE_FIELDS, "sender balance override"
    )
    sender_balance = _word(
        sender_override.get("balance"), "sender balance override"
    )
    expected_sender_balance = "0x" + format(
        ETHEREUM_V2_SIMULATION_SENDER_NATIVE_BALANCE_WEI, "064x"
    )
    if sender_balance != expected_sender_balance:
        raise RouteCostEvidenceError("sender balance override differs")
    token_override = _exact_fields(
        value[input_token], STATE_DIFF_OVERRIDE_FIELDS, "token state override"
    )
    state_diff = token_override.get("stateDiff")
    if not isinstance(state_diff, Mapping) or list(state_diff) != sorted(state_diff):
        raise RouteCostEvidenceError("token stateDiff is invalid")
    balance_key = solidity_balance_storage_key(
        sender, int(descriptor["balance_mapping_slot"])
    )
    allowance_key = solidity_allowance_storage_key(
        sender, router, int(descriptor["allowance_mapping_slot"])
    )
    if set(state_diff) != {balance_key, allowance_key}:
        raise RouteCostEvidenceError("token stateDiff keys differ")
    for key in sorted(state_diff):
        _word(state_diff[key], "token stateDiff value")
    amount_in = decoded.get("amount_in_raw", decoded.get("amount_in_max_raw"))
    exact_word = "0x" + format(amount_in, "064x")
    if state_diff[balance_key] != exact_word or state_diff[allowance_key] != exact_word:
        raise RouteCostEvidenceError("token stateDiff amount differs")


def _validate_storage_diffs(
    value: Any,
    *,
    adapter: Mapping[str, Any],
    market: Mapping[str, Any],
    calldata: Any,
) -> List[Dict[str, Any]]:
    if not isinstance(value, list):
        raise RouteCostEvidenceError("trace storage diffs are invalid")
    identities = []
    deltas = []
    allowed_roles = {"sender", "router", "pair", "recipient"}
    descriptors = {
        row["token_address"]: row
        for row in adapter.get("token_funding_descriptors", [])
    }
    decoded = decode_v2_swap_calldata(calldata)
    token_in, token_out = decoded["path"]
    sender = adapter["simulation_sender_address"]
    pair = market["pair_address"]
    router = adapter["router_address"]
    planned = {
        (
            token_in,
            "sender",
            solidity_balance_storage_key(
                sender, int(_funding_descriptor(adapter, token_in)["balance_mapping_slot"])
            ),
        ),
        (
            token_in,
            "sender",
            solidity_allowance_storage_key(
                sender,
                router,
                int(_funding_descriptor(adapter, token_in)["allowance_mapping_slot"]),
            ),
        ),
        (
            token_in,
            "pair",
            solidity_balance_storage_key(
                pair, int(_funding_descriptor(adapter, token_in)["balance_mapping_slot"])
            ),
        ),
        (
            token_out,
            "pair",
            solidity_balance_storage_key(
                pair, int(_funding_descriptor(adapter, token_out)["balance_mapping_slot"])
            ),
        ),
        (
            token_out,
            "recipient",
            solidity_balance_storage_key(
                sender, int(_funding_descriptor(adapter, token_out)["balance_mapping_slot"])
            ),
        ),
    }
    for row in value:
        _exact_fields(row, STORAGE_DIFF_FIELDS, "trace storage diff")
        token = _address(row.get("token_address"), "trace diff token")
        role = row.get("account_role")
        if role not in allowed_roles or token not in descriptors:
            raise RouteCostEvidenceError("trace storage diff identity is invalid")
        key = _hash32(row.get("storage_key"), "trace storage key")
        pre_present = row.get("pre_present")
        post_present = row.get("post_present")
        if not isinstance(pre_present, bool) or not isinstance(post_present, bool):
            raise RouteCostEvidenceError("trace storage presence is invalid")
        if not pre_present and not post_present:
            raise RouteCostEvidenceError("trace storage diff is absent on both sides")
        pre_word = _word(row.get("pre_value"), "trace pre value")
        post_word = _word(row.get("post_value"), "trace post value")
        zero = "0x" + "0" * 64
        if (not pre_present and pre_word != zero) or (not post_present and post_word != zero):
            raise RouteCostEvidenceError("trace absent storage is nonzero")
        identities.append((token, role, key))
        descriptor = descriptors[token]
        account = {
            "sender": sender,
            "recipient": sender,
            "pair": pair,
            "router": router,
        }[role]
        balance_key = solidity_balance_storage_key(
            account, int(descriptor["balance_mapping_slot"])
        )
        if key == balance_key:
            deltas.append({
                "token_address": token,
                "account_role": role,
                "pre_balance_raw": str(int(pre_word, 16)),
                "post_balance_raw": str(int(post_word, 16)),
            })
    if identities != sorted(set(identities)) or set(identities) != planned:
        raise RouteCostEvidenceError("trace storage diffs are not canonical")
    return deltas


_UNAVAILABLE_TRANSCRIPT_REASONS = {
    "strict_cost_adapter_unsupported",
    "core_pool_state_unavailable",
    "rpc_unavailable",
    "fixed_block_unavailable",
    "router_identity_unavailable",
    "pair_identity_unavailable",
    "calldata_unavailable",
    "gas_unavailable",
    "native_price_unavailable",
    "trace_profile_missing",
    "trace_unavailable",
    "transfer_tax_present",
    "transfer_behavior_unsupported",
}
_FAILED_TRANSCRIPT_REASONS = {
    "core_pool_state_invalid",
    "rpc_invalid",
    "fixed_block_mismatch",
    "router_identity_mismatch",
    "pair_identity_mismatch",
    "token_funding_code_mismatch",
    "calldata_mismatch",
    "gas_invalid",
    "native_price_invalid",
    "trace_invalid",
    "resource_limit",
}
_BINDING_REASONS = {
    "unavailable": {
        "transcript_unavailable",
        "submission_policy_unavailable",
        "submission_policy_stale",
    },
    "failed": {
        "transcript_failed",
        "transcript_binding_mismatch",
        "submission_policy_invalid",
        "resource_limit",
    },
}
_STAGE_ORDER = {
    "none": 0,
    "block": 1,
    "call": 2,
    "gas": 3,
    "router_fee": 4,
    "transfer_tax": 5,
}


def _validate_profile_identity(
    value: Any, *, kind: str
) -> Tuple[Dict[str, Any], str]:
    if kind == "trace":
        fields = ("schema", "status", "profile_id", "endpoint_id")
        schema = "route_cost_trace_profile_identity/v1"
        identity_field = "endpoint_id"
        domain = b"route-cost-trace-profile-identity/v1\n"
    elif kind == "connector":
        fields = ("schema", "status", "profile_id", "connector_id")
        schema = "route_cost_submission_connector_identity/v1"
        identity_field = "connector_id"
        domain = b"route-cost-submission-connector-identity/v1\n"
    else:  # pragma: no cover - internal invariant
        raise RouteCostEvidenceError("profile identity kind is invalid")
    _exact_fields(value, fields, kind + " profile identity")
    if value.get("schema") != schema:
        raise RouteCostEvidenceError(kind + " profile identity schema is invalid")
    status = value.get("status")
    if status == "available":
        _required_text(value.get("profile_id"), kind + " profile ID", _LOWER_ID)
        pattern = _CONNECTOR_ID if kind == "connector" else _LOWER_ID
        _required_text(
            value.get(identity_field), kind + " public identity", pattern
        )
    elif status == "missing":
        if value.get("profile_id") is not None or value.get(identity_field) is not None:
            raise RouteCostEvidenceError(kind + " missing profile matrix is invalid")
    else:
        raise RouteCostEvidenceError(kind + " profile status is invalid")
    identity = _canonical_copy(value)
    return identity, typed_sha256(domain, identity)


def _derived_binding_status_reason(
    *,
    route_sides: Tuple[bool, bool],
    transcripts: Sequence[Mapping[str, Any]],
    member: Mapping[str, Any],
    snapshot: Mapping[str, Any],
    evaluated_at: Any,
) -> Tuple[str, Optional[str]]:
    """Derive the only legal unsigned binding terminal projection."""
    transcript_statuses = [row.get("status") for row in transcripts]
    if "failed" in transcript_statuses:
        return "failed", "transcript_failed"
    if "unavailable" in transcript_statuses:
        return "unavailable", "transcript_unavailable"
    snapshot_status = snapshot.get("status")
    member_status = member.get("status")
    if snapshot_status == "failed" or member_status == "failed":
        return "failed", "submission_policy_invalid"
    if snapshot_status != "authenticated" or member_status == "unavailable":
        return "unavailable", "submission_policy_unavailable"
    evaluated = _timestamp_value(evaluated_at, "binding evaluated_at")
    observed = _timestamp_value(
        snapshot.get("observed_at"), "snapshot observed_at"
    )
    valid_until = _timestamp_value(
        snapshot.get("valid_until"), "snapshot valid_until"
    )
    if evaluated < observed or evaluated > valid_until:
        return "unavailable", "submission_policy_stale"
    if member_status != "observed":
        raise RouteCostEvidenceError("binding policy member status is invalid")
    if route_sides != (True, True):
        return "unavailable", "submission_policy_unavailable"
    return "observed", None


def _validate_selected_market_row(value: Any) -> None:
    _exact_fields(value, SELECTED_MARKET_FIELDS, "selected market")
    market_id = value.get("market_id")
    if not isinstance(market_id, str) or _DEX_MARKET_ID.fullmatch(market_id) is None:
        raise RouteCostEvidenceError("selected market ID is invalid")
    _exact_int(value.get("token_rank"), "selected market token rank", 1)
    _exact_int(value.get("selection_rank"), "selected market selection rank", 1)
    for field in ("best_route_volume_usd", "dex_24h_usd", "dex_tvl_usd"):
        if value.get(field) is not None:
            _decimal_text(value[field], "selected market " + field)
    if value.get("adapter_id") != ETHEREUM_V2_ADAPTER_ID:
        raise RouteCostEvidenceError("selected market adapter is invalid")
    status = value.get("structural_support_status")
    if status == "supported":
        if value.get("structural_reason") is not None:
            raise RouteCostEvidenceError("supported selected market has a reason")
    elif status == "unsupported":
        if value.get("structural_reason") != "strict_cost_adapter_unsupported":
            raise RouteCostEvidenceError("unsupported selected market reason is invalid")
    else:
        raise RouteCostEvidenceError("selected market support status is invalid")


def _validate_block_header(value: Any) -> None:
    _exact_fields(value, BLOCK_HEADER_FIELDS, "block header")
    _quantity(value.get("number"), "block number", positive=True)
    _hash32(value.get("hash"), "block hash")
    _hash32(value.get("parent_hash"), "block parent hash")
    for field in ("timestamp", "base_fee_per_gas", "gas_used", "gas_limit"):
        _quantity(value.get(field), "block " + field)


def _validate_fee_history(value: Any) -> None:
    _exact_fields(value, FEE_HISTORY_FIELDS, "fee history")
    if value.get("schema") != "route_cost_fee_history_result/v1":
        raise RouteCostEvidenceError("fee history schema is invalid")
    status = value.get("status")
    if status == "observed":
        if value.get("reason_code") is not None:
            raise RouteCostEvidenceError("observed fee history has a reason")
        _quantity(value.get("oldest_block"), "fee-history oldest block", positive=True)
        base_fees = value.get("base_fee_per_gas")
        reward = value.get("reward")
        ratios = value.get("gas_used_ratio")
        if (
            not isinstance(base_fees, list)
            or len(base_fees) != 2
            or not isinstance(reward, list)
            or len(reward) != 1
            or not isinstance(reward[0], list)
            or len(reward[0]) != 1
            or not isinstance(ratios, list)
            or len(ratios) != 1
        ):
            raise RouteCostEvidenceError("fee history shape is invalid")
        for item in base_fees + reward[0]:
            _quantity(item, "fee-history quantity")
        _decimal_text(ratios[0], "fee-history gas ratio")
    elif status in {"unavailable", "failed"}:
        reason = value.get("reason_code")
        expected = "gas_unavailable" if status == "unavailable" else "gas_invalid"
        if reason != expected:
            raise RouteCostEvidenceError("fee history reason is invalid")
        if any(value.get(field) is not None for field in (
            "oldest_block", "base_fee_per_gas", "reward", "gas_used_ratio"
        )):
            raise RouteCostEvidenceError("fee history null matrix is invalid")
    else:
        raise RouteCostEvidenceError("fee history status is invalid")


def _validate_native_price_record(value: Any) -> None:
    _exact_fields(value, NATIVE_PRICE_RECORD_FIELDS, "native price record")
    if value.get("schema") != "route_cost_native_price_record/v1":
        raise RouteCostEvidenceError("native price record schema is invalid")
    status = value.get("status")
    if value.get("native_symbol") != "ETH" or value.get("wrapped_native_address") != ETHEREUM_WETH_ADDRESS:
        raise RouteCostEvidenceError("native price identity is invalid")
    source_fields = (
        "price_usd", "observed_at", "valid_until",
        "native_price_evidence_sha256", "source_record_sha256",
    )
    if status == "observed":
        if value.get("reason_code") is not None:
            raise RouteCostEvidenceError("observed native price has a reason")
        _decimal_text(value.get("price_usd"), "native price", positive=True)
        observed = _timestamp_value(value.get("observed_at"), "native price observed_at")
        valid = _timestamp_value(value.get("valid_until"), "native price valid_until")
        if observed > valid:
            raise RouteCostEvidenceError("native price validity is reversed")
        _sha256(value.get("native_price_evidence_sha256"), "native evidence hash")
        _sha256(value.get("source_record_sha256"), "native source hash")
    elif status == "unavailable":
        if value.get("reason_code") != "native_price_unavailable" or any(
            value.get(field) is not None for field in source_fields
        ):
            raise RouteCostEvidenceError("unavailable native price matrix is invalid")
    elif status == "failed":
        if value.get("reason_code") != "native_price_invalid" or any(
            value.get(field) is not None for field in source_fields
        ):
            raise RouteCostEvidenceError("failed native price matrix is invalid")
    else:
        raise RouteCostEvidenceError("native price status is invalid")


def _validate_chain_evidence(
    value: Any,
    links: Mapping[str, Any],
    *,
    trace_profile_identity: Mapping[str, Any],
) -> str:
    _exact_fields(value, CHAIN_EVIDENCE_FIELDS, "chain evidence")
    if value.get("schema") != ROUTE_COST_CHAIN_EVIDENCE_SCHEMA:
        raise RouteCostEvidenceError("chain evidence schema is invalid")
    for field in (
        "run_id", "route_cohort_id", "candidate_source_generation",
        "route_universe_sha256", "selected_market_set_sha256",
    ):
        if value.get(field) != links[field]:
            raise RouteCostEvidenceError("chain evidence lineage differs")
    if value.get("chain_id") != 1:
        raise RouteCostEvidenceError("chain evidence chain is invalid")
    rpc_source_id = _required_text(
        value.get("rpc_source_id"), "RPC source ID", _LOWER_ID
    )
    if (
        trace_profile_identity.get("status") != "available"
        or rpc_source_id != trace_profile_identity.get("endpoint_id")
    ):
        raise RouteCostEvidenceError("RPC source differs from trace profile")
    _ordered_timestamps(
        value.get("captured_started_at"), value.get("captured_finished_at"), "chain capture"
    )
    _validate_block_header(value.get("block_header_result"))
    _validate_fee_history(value.get("fee_history_result"))
    _validate_native_price_record(value.get("native_price_record"))
    statuses = {
        value["fee_history_result"]["status"],
        value["native_price_record"]["status"],
    }
    expected_status = (
        "failed" if "failed" in statuses else
        "incomplete" if "unavailable" in statuses else "observed"
    )
    expected_reason = (
        None if expected_status == "observed" else
        value["fee_history_result"]["reason_code"]
        if value["fee_history_result"]["status"] != "observed"
        else value["native_price_record"]["reason_code"]
    )
    if value.get("status") != expected_status or value.get("reason_code") != expected_reason:
        raise RouteCostEvidenceError("chain evidence status is inconsistent")
    if len(canonical_json_bytes(value)) > MAX_CHAIN_EVIDENCE_BYTES:
        raise RouteCostEvidenceError("chain evidence exceeds its byte limit")
    return physical_sha256(value)


def _validate_market_evidence(
    value: Any,
    links: Mapping[str, Any],
    chain_hashes: Mapping[str, Mapping[str, Any]],
    selected_ids: set,
    adapter: Mapping[str, Any],
    retained_pool_states: Mapping[str, Mapping[str, Any]],
) -> str:
    _exact_fields(value, MARKET_EVIDENCE_FIELDS, "market evidence")
    if value.get("schema") != ROUTE_COST_MARKET_EVIDENCE_SCHEMA:
        raise RouteCostEvidenceError("market evidence schema is invalid")
    for field in (
        "run_id", "route_cohort_id", "candidate_source_generation",
        "route_universe_sha256", "adapter_registry_sha256",
        "selected_market_set_sha256",
    ):
        if value.get(field) != links[field]:
            raise RouteCostEvidenceError("market evidence lineage differs")
    market_id = value.get("market_id")
    if market_id not in selected_ids or value.get("adapter_id") != ETHEREUM_V2_ADAPTER_ID:
        raise RouteCostEvidenceError("market evidence identity is invalid")
    chain_sha = _sha256(value.get("chain_evidence_sha256"), "market chain hash")
    if chain_sha not in chain_hashes:
        raise RouteCostEvidenceError("market evidence chain is missing")
    core_id = value.get("core_pool_state_id")
    if not isinstance(core_id, str) or not core_id:
        raise RouteCostEvidenceError("market core pool state ID is invalid")
    _sha256(value.get("core_pool_state_sha256"), "market core pool state hash")
    retained = retained_pool_states.get(market_id)
    if retained is None:
        raise RouteCostEvidenceError("market retained core pool state is absent")
    if (
        core_id != retained.get("state_id")
        or value.get("core_pool_state_sha256")
        != retained.get("_physical_sha256")
    ):
        raise RouteCostEvidenceError("market retained core pool-state lineage differs")
    if value.get("router_address") != adapter["router_address"] or value.get("factory_address") != adapter["factory_address"]:
        raise RouteCostEvidenceError("market router/factory address mismatch")
    market_match = _DEX_MARKET_ID.fullmatch(str(market_id))
    if market_match is None or value.get("pair_address") != market_match.group(3):
        raise RouteCostEvidenceError("market pair address mismatch")
    pair_descriptors = [
        row for row in adapter.get("pair_descriptors", [])
        if row.get("pair_address") == value.get("pair_address")
    ]
    if len(pair_descriptors) != 1:
        raise RouteCostEvidenceError("market pair descriptor is absent")
    pair_descriptor = pair_descriptors[0]
    code_fields = (
        ("router_runtime_code", adapter["router_runtime_code_sha256"]),
        ("factory_runtime_code", adapter["factory_runtime_code_sha256"]),
        (
            "pair_runtime_code",
            pair_descriptor["pair_runtime_code_sha256"],
        ),
    )
    for field, expected_hash in code_fields:
        raw = _decoded_hex_bytes(value.get(field), field, MAX_RUNTIME_CODE_BYTES)
        if not raw:
            raise RouteCostEvidenceError("{} is empty".format(field))
        if expected_hash is not None and hashlib.sha256(raw).hexdigest() != expected_hash:
            raise RouteCostEvidenceError("{} identity mismatch".format(field))
    token0 = _address(value.get("pair_token0"), "pair token0")
    token1 = _address(value.get("pair_token1"), "pair token1")
    if token0 == token1:
        raise RouteCostEvidenceError("market pair tokens are identical")
    if (
        token0 != pair_descriptor["token0_address"]
        or token1 != pair_descriptor["token1_address"]
    ):
        raise RouteCostEvidenceError(
            "market pair tokens differ from static pair descriptor"
        )
    chain = chain_hashes[chain_sha]
    _validate_factory_get_pair_evidence(
        value.get("factory_get_pair_request"),
        value.get("factory_get_pair_response"),
        adapter=adapter,
        chain=chain,
        token0=token0,
        token1=token1,
        pair=value.get("pair_address"),
    )
    _validate_token_runtime_code_evidence(
        value.get("token_runtime_code_evidence"),
        adapter=adapter,
        chain=chain,
        token0=token0,
        token1=token1,
    )
    if (
        value.get("pair_address") != retained.get("pool_address")
        or token0 != retained.get("token0_address")
        or token1 != retained.get("token1_address")
        or int(retained.get("fee_bps")) != int(adapter["pair_fee_bps"])
    ):
        raise RouteCostEvidenceError("market retained pool-state identity differs")
    header = chain["block_header_result"]
    if (
        retained.get("chain") != "eth"
        or int(retained.get("chain_id")) != chain.get("chain_id")
        or retained.get("dex") != "uniswap_v2"
        or int(retained.get("block_number")) != int(header["number"], 16)
        or retained.get("block_hash") != header["hash"]
        or retained.get("block_header_sha256") != physical_sha256(header)
        or _timestamp_value(
            retained.get("observed_at"), "retained state observed_at"
        ) != int(header["timestamp"], 16)
    ):
        raise RouteCostEvidenceError("market retained fixed-block state differs")
    _ordered_timestamps(
        value.get("captured_started_at"), value.get("captured_finished_at"), "market capture"
    )
    if (
        value.get("captured_started_at") != chain.get("captured_started_at")
        or value.get("captured_finished_at")
        != chain.get("captured_finished_at")
    ):
        raise RouteCostEvidenceError(
            "market capture window differs from referenced chain"
        )
    if len(canonical_json_bytes(value)) > MAX_MARKET_EVIDENCE_BYTES:
        raise RouteCostEvidenceError("market evidence exceeds its byte limit")
    return physical_sha256(value)


def _validate_block_evidence(
    value: Any,
    *,
    transcript: Mapping[str, Any],
    chain: Mapping[str, Any],
    market: Mapping[str, Any],
    adapter: Mapping[str, Any],
    retained_pool_state: Mapping[str, Any],
) -> None:
    _exact_fields(value, BLOCK_EVIDENCE_FIELDS, "block evidence")
    if value.get("schema") != ROUTE_COST_BLOCK_EVIDENCE_SCHEMA:
        raise RouteCostEvidenceError("block evidence schema is invalid")
    if (
        value.get("chain_evidence_sha256") != transcript["chain_evidence_sha256"]
        or value.get("market_evidence_sha256") != transcript["market_evidence_sha256"]
        or value.get("chain_id") != 1
        or value.get("core_pool_state_id") != transcript["core_pool_state_id"]
        or value.get("core_pool_state_id") != retained_pool_state.get("state_id")
    ):
        raise RouteCostEvidenceError("block evidence lineage differs")
    header = chain["block_header_result"]
    if (
        value.get("block_tag") != header["number"]
        or value.get("block_number") != header["number"]
        or value.get("block_hash") != header["hash"]
        or value.get("block_timestamp") != header["timestamp"]
        or value.get("block_number")
        != hex(int(retained_pool_state["block_number"]))
        or value.get("block_hash") != retained_pool_state["block_hash"]
        or value.get("block_timestamp")
        != hex(int(_timestamp_value(
            retained_pool_state["observed_at"], "retained state observed_at"
        )))
        or value.get("router_runtime_code_sha256")
        != adapter["router_runtime_code_sha256"]
        or value.get("factory_runtime_code_sha256")
        != adapter["factory_runtime_code_sha256"]
        or value.get("pair_runtime_code_sha256")
        != hashlib.sha256(bytes.fromhex(market["pair_runtime_code"][2:])).hexdigest()
    ):
        raise RouteCostEvidenceError("block evidence fixed-block identity differs")
    _sha256(value.get("rpc_transcript_sha256"), "RPC transcript hash")


def _raw_rpc_transcript_sha256(raw: Mapping[str, Any]) -> str:
    projection = {
        "estimate_request": raw.get("estimate_gas_request"),
        "estimate_response": raw.get("estimate_gas_response"),
        "trace_request": raw.get("simulation_request"),
        "trace_response": raw.get("simulation_response"),
    }
    return typed_sha256(b"route-cost-rpc-transcript/v1\n", projection)


def _raw_trace_sha256(raw: Mapping[str, Any]) -> str:
    return typed_sha256(
        b"route-cost-trace/v1\n",
        {
            "request": raw.get("simulation_request"),
            "response": raw.get("simulation_response"),
        },
    )


def _validate_call_evidence(
    value: Any,
    *,
    transcript: Mapping[str, Any],
    raw: Mapping[str, Any],
    adapter: Mapping[str, Any],
    chain: Mapping[str, Any],
    market: Mapping[str, Any],
    market_tokens: Tuple[str, str],
    simulation_target: Mapping[str, str],
    retained_pool_state: Mapping[str, Any],
) -> None:
    _exact_fields(value, CALL_EVIDENCE_FIELDS, "call evidence")
    if value.get("schema") != ROUTE_COST_CALL_EVIDENCE_SCHEMA:
        raise RouteCostEvidenceError("call evidence schema is invalid")
    direction = transcript["direction"]
    expected_selector = ETHEREUM_V2_BUY_SELECTOR if direction == "buy" else ETHEREUM_V2_SELL_SELECTOR
    if value.get("selector") != expected_selector:
        raise RouteCostEvidenceError("call evidence selector differs")
    token_in = _address(value.get("path_token_in"), "call path token in")
    token_out = _address(value.get("path_token_out"), "call path token out")
    if token_in == token_out:
        raise RouteCostEvidenceError("call path tokens are identical")
    pair_tokens = {
        _address(market.get("pair_token0"), "market pair token0"),
        _address(market.get("pair_token1"), "market pair token1"),
    }
    if {token_in, token_out} != pair_tokens:
        raise RouteCostEvidenceError("call path differs from selected pair")
    target_token, other_token = (
        _address(market_tokens[0], "selected target token"),
        _address(market_tokens[1], "selected other token"),
    )
    expected_path = (
        (other_token, target_token)
        if direction == "buy"
        else (target_token, other_token)
    )
    if (token_in, token_out) != expected_path:
        raise RouteCostEvidenceError("call path direction differs from selected market")
    if (
        value.get("recipient_policy") != "same_as_registry_sender/v1"
        or value.get("sender_policy") != "registry_fixed_state_override_sender/v1"
        or value.get("allowance_basis") != "exact_amount_state_override/v1"
    ):
        raise RouteCostEvidenceError("call policy is invalid")
    deadline = _quantity(value.get("deadline"), "call deadline", positive=True)
    if int(deadline, 16) != int(
        chain["block_header_result"]["timestamp"], 16
    ) + 300:
        raise RouteCostEvidenceError("call deadline differs from fixed block")
    amount_in = int(_integer_text(
        value.get("amount_in_raw"), "call amount in", positive=True
    ))
    amount_out = int(_integer_text(
        value.get("amount_out_raw"), "call amount out", positive=True
    ))
    if not isinstance(simulation_target, Mapping):
        raise RouteCostEvidenceError("call simulation target is absent")
    target_raw = int(_integer_text(
        simulation_target.get("simulation_target_raw_quantity"),
        "simulation target raw quantity",
        positive=True,
    ))
    if (
        simulation_target.get("simulation_target_token_address")
        != target_token
        or simulation_target.get("simulation_target_lattice_raw") != "1"
    ):
        raise RouteCostEvidenceError("call simulation target identity differs")
    token0 = _address(
        retained_pool_state.get("token0_address"), "retained token0"
    )
    token1 = _address(
        retained_pool_state.get("token1_address"), "retained token1"
    )
    if {target_token, other_token} != {token0, token1}:
        raise RouteCostEvidenceError("call target differs from retained pool")
    target_is_token0 = target_token == token0
    target_decimals = int(_integer_text(
        retained_pool_state.get(
            "token0_decimals" if target_is_token0 else "token1_decimals"
        ),
        "retained target decimals",
    ))
    if simulation_target.get("simulation_target_unit_decimals") != str(
        target_decimals
    ):
        raise RouteCostEvidenceError("call target decimals differ")
    reserve_target = int(_integer_text(
        retained_pool_state.get(
            "reserve0_raw" if target_is_token0 else "reserve1_raw"
        ),
        "retained target reserve",
        positive=True,
    ))
    reserve_other = int(_integer_text(
        retained_pool_state.get(
            "reserve1_raw" if target_is_token0 else "reserve0_raw"
        ),
        "retained other reserve",
        positive=True,
    ))
    fee_numerator = int(_integer_text(
        retained_pool_state.get("fee_numerator"),
        "retained fee numerator",
        positive=True,
    ))
    fee_denominator = int(_integer_text(
        retained_pool_state.get("fee_denominator"),
        "retained fee denominator",
        positive=True,
    ))
    if fee_numerator > fee_denominator:
        raise RouteCostEvidenceError("retained fee fraction is invalid")
    if direction == "sell":
        expected_amount_in = target_raw
        amount_with_fee = target_raw * fee_numerator
        expected_amount_out = (
            amount_with_fee
            * reserve_other
            // (reserve_target * fee_denominator + amount_with_fee)
        )
        if expected_amount_out <= 0:
            raise RouteCostEvidenceError("call target output is below one raw unit")
    else:
        if target_raw >= reserve_target:
            raise RouteCostEvidenceError("call target exceeds retained reserve")
        expected_amount_out = target_raw
        expected_amount_in = (
            reserve_other
            * target_raw
            * fee_denominator
            // ((reserve_target - target_raw) * fee_numerator)
            + 1
        )
    if (amount_in, amount_out) != (expected_amount_in, expected_amount_out):
        raise RouteCostEvidenceError("call amount differs from retained target quote")
    bound = _integer_text(value.get("submission_loss_bound_bps"), "call bound")
    if int(bound) > 10000:
        raise RouteCostEvidenceError("call bound exceeds 10000")
    calldata = raw.get("calldata_hex")
    if hashlib.sha256(bytes.fromhex(str(calldata)[2:])).hexdigest() != value.get("calldata_sha256"):
        raise RouteCostEvidenceError("call calldata hash differs")
    decoded = decode_v2_swap_calldata(calldata)
    if (
        decoded["selector"] != expected_selector
        or decoded["path"] != [token_in, token_out]
        or decoded["recipient"] != adapter["simulation_sender_address"]
        or decoded["deadline"] != int(deadline, 16)
    ):
        raise RouteCostEvidenceError("call calldata identity differs")
    if direction == "buy":
        expected_bound_amount = (amount_in * (10000 + int(bound)) + 9999) // 10000
        if (
            decoded.get("amount_out_raw") != amount_out
            or decoded.get("amount_in_max_raw") != expected_bound_amount
        ):
            raise RouteCostEvidenceError("buy calldata amount/bound differs")
    else:
        expected_bound_amount = amount_out * (10000 - int(bound)) // 10000
        if (
            decoded.get("amount_in_raw") != amount_in
            or decoded.get("amount_out_min_raw") != expected_bound_amount
        ):
            raise RouteCostEvidenceError("sell calldata amount/bound differs")


def _validate_gas_evidence(
    value: Any,
    *,
    chain: Mapping[str, Any],
    native_sha: Optional[str],
    native_evidence: Optional[Mapping[str, Any]],
    raw: Mapping[str, Any],
) -> None:
    _exact_fields(value, GAS_EVIDENCE_FIELDS, "gas evidence")
    if value.get("schema") != ROUTE_COST_GAS_EVIDENCE_SCHEMA:
        raise RouteCostEvidenceError("gas evidence schema is invalid")
    gas_units = int(_integer_text(value.get("gas_units"), "gas units", positive=True))
    estimate_result = raw.get("estimate_gas_response", {}).get("result")
    if not isinstance(estimate_result, str) or gas_units != int(
        _quantity(estimate_result, "estimate-gas result", positive=True), 16
    ):
        raise RouteCostEvidenceError("gas units differ from estimate")
    max_fee = int(_integer_text(value.get("max_fee_per_gas_wei"), "max fee per gas"))
    fee_history_sha = physical_sha256(chain["fee_history_result"])
    if value.get("fee_history_sha256") != fee_history_sha:
        raise RouteCostEvidenceError("gas fee-history hash differs")
    if value.get("native_symbol") != "ETH":
        raise RouteCostEvidenceError("gas native symbol is invalid")
    native_price = _decimal_text(
        value.get("native_price_usd"), "gas native price", positive=True
    )
    if native_sha is None or value.get("native_price_sha256") != native_sha:
        raise RouteCostEvidenceError("gas native-price hash differs")
    if native_evidence is None:
        raise RouteCostEvidenceError("gas native-price evidence is absent")
    book = native_evidence["book_projection"]
    conversion = native_evidence["usd_conversion_projection"]
    expected_native_price = _format_decimal(
        Decimal(book["best_ask_price"]) * Decimal(conversion["rate"])
    )
    if native_price != expected_native_price:
        raise RouteCostEvidenceError("gas native price differs")
    header = chain["block_header_result"]
    fee_history = chain["fee_history_result"]
    if fee_history.get("status") != "observed":
        raise RouteCostEvidenceError("gas fee history is not observed")
    if fee_history.get("oldest_block") != header.get("number"):
        raise RouteCostEvidenceError("fee-history fixed block differs")
    expected_next = next_base_fee_wei(
        base_fee_per_gas=int(header["base_fee_per_gas"], 16),
        gas_used=int(header["gas_used"], 16),
        gas_limit=int(header["gas_limit"], 16),
    )
    returned_next = int(fee_history["base_fee_per_gas"][1], 16)
    if expected_next != returned_next:
        raise RouteCostEvidenceError("fee-history next base fee differs")
    priority = int(fee_history["reward"][0][0], 16)
    if max_fee != max_fee_per_gas_wei(expected_next, priority):
        raise RouteCostEvidenceError("gas max fee differs")
    observed = _timestamp_value(value.get("observed_at"), "gas observed_at")
    valid = _timestamp_value(value.get("valid_until"), "gas valid_until")
    if (
        observed > valid
        or value.get("observed_at") != native_evidence.get("observed_at")
        or value.get("valid_until") != native_evidence.get("valid_until")
    ):
        raise RouteCostEvidenceError("gas validity is reversed")


def _validate_component(
    value: Any, *, component: str, raw: Optional[Mapping[str, Any]] = None,
    call_evidence: Optional[Mapping[str, Any]] = None,
    expected_source_sha: Optional[str] = None,
) -> None:
    if component == "router":
        fields = ROUTER_FEE_EVIDENCE_FIELDS
        schema = ROUTE_COST_ROUTER_FEE_EVIDENCE_SCHEMA
        allowed_basis = "verified_uniswap_v2_router02_no_integrator_fee/v1"
    else:
        fields = TRANSFER_TAX_EVIDENCE_FIELDS
        schema = ROUTE_COST_TRANSFER_TAX_EVIDENCE_SCHEMA
        allowed_basis = None
    _exact_fields(value, fields, component + " evidence")
    if value.get("schema") != schema:
        raise RouteCostEvidenceError("{} schema is invalid".format(component))
    status = value.get("status")
    if status not in {"authenticated", "not_applicable", "unavailable", "failed"}:
        raise RouteCostEvidenceError("{} status is invalid".format(component))
    rate = value.get("rate_bps")
    if status == "authenticated":
        _decimal_text(rate, component + " rate")
    elif rate is not None:
        raise RouteCostEvidenceError("{} nonnumeric status has a rate".format(component))
    if component == "router":
        if value.get("basis_code") != allowed_basis or status != "not_applicable":
            raise RouteCostEvidenceError("router fee must be verified not-applicable")
        source_sha = _sha256(
            value.get("source_record_sha256"), "router fee source hash"
        )
        if source_sha != expected_source_sha:
            raise RouteCostEvidenceError("router fee source differs")
    else:
        if raw is None or call_evidence is None:
            raise RouteCostEvidenceError(
                "transfer-tax raw transcript or quoted call is absent"
            )
        for field in (
            "pre_input_balance", "post_input_balance", "pre_output_balance", "post_output_balance"
        ):
            _integer_text(value.get(field), "transfer-tax " + field)
        if value.get("trace_method") != "debug_traceCall_state_override_v1":
            raise RouteCostEvidenceError("transfer-tax trace method is invalid")
        if value.get("trace_sha256") != _raw_trace_sha256(raw):
            raise RouteCostEvidenceError("transfer-tax trace hash differs")
        if status not in {"not_applicable", "unavailable"}:
            raise RouteCostEvidenceError("transfer-tax status is invalid")
        deltas = raw.get("simulation_balance_deltas")
        if not isinstance(deltas, list) or len(deltas) != 4:
            raise RouteCostEvidenceError("transfer-tax balance delta inventory differs")
        by_role = {
            row["account_role"]: row
            for row in deltas
            if row["account_role"] != "pair"
        }
        if set(by_role) != {"sender", "recipient"}:
            raise RouteCostEvidenceError("transfer-tax balance roles differ")
        expected_balances = (
            by_role["sender"]["pre_balance_raw"],
            by_role["sender"]["post_balance_raw"],
            by_role["recipient"]["pre_balance_raw"],
            by_role["recipient"]["post_balance_raw"],
        )
        actual_balances = tuple(
            value[field] for field in (
                "pre_input_balance", "post_input_balance",
                "pre_output_balance", "post_output_balance",
            )
        )
        if actual_balances != expected_balances:
            raise RouteCostEvidenceError("transfer-tax balances differ")
        input_spent = int(actual_balances[0]) - int(actual_balances[1])
        output_received = int(actual_balances[3]) - int(actual_balances[2])
        pair_rows = [row for row in deltas if row["account_role"] == "pair"]
        if len(pair_rows) != 2:
            raise RouteCostEvidenceError("transfer-tax pair deltas differ")
        calldata = decode_v2_swap_calldata(raw["calldata_hex"])
        token_in, token_out = calldata["path"]
        pair_in = next(row for row in pair_rows if row["token_address"] == token_in)
        pair_out = next(row for row in pair_rows if row["token_address"] == token_out)
        pair_received = int(pair_in["post_balance_raw"]) - int(pair_in["pre_balance_raw"])
        pair_sent = int(pair_out["pre_balance_raw"]) - int(pair_out["post_balance_raw"])
        quoted_input = int(_integer_text(
            call_evidence.get("amount_in_raw"),
            "transfer-tax quoted input", positive=True,
        ))
        quoted_output = int(_integer_text(
            call_evidence.get("amount_out_raw"),
            "transfer-tax quoted output", positive=True,
        ))
        zero_tax = (
            input_spent == quoted_input
            and pair_received == quoted_input
            and pair_sent == quoted_output
            and output_received == quoted_output
        )
        if status == "not_applicable":
            if rate is not None or not zero_tax:
                raise RouteCostEvidenceError("zero-tax semantics differ")
        elif rate is not None or zero_tax:
            raise RouteCostEvidenceError("positive-tax semantics differ")


def _validate_raw_transcript(
    value: Any,
    *,
    transcript: Mapping[str, Any],
    adapter: Optional[Mapping[str, Any]] = None,
    market: Optional[Mapping[str, Any]] = None,
    chain: Optional[Mapping[str, Any]] = None,
    completed_stage: Optional[str] = None,
    status: Optional[str] = None,
    reason_code: Optional[str] = None,
) -> None:
    _exact_fields(value, RAW_TRANSCRIPT_FIELDS, "raw transcript")
    if value.get("schema") != ROUTE_COST_RAW_TRANSCRIPT_SCHEMA:
        raise RouteCostEvidenceError("raw transcript schema is invalid")
    if (
        value.get("chain_evidence_sha256") != transcript["chain_evidence_sha256"]
        or value.get("market_evidence_sha256") != transcript["market_evidence_sha256"]
    ):
        raise RouteCostEvidenceError("raw transcript lineage differs")
    _ordered_timestamps(
        value.get("captured_started_at"), value.get("captured_finished_at"), "raw transcript"
    )
    if value.get("calldata_hex") is not None:
        decode_v2_swap_calldata(value["calldata_hex"])
    captured = (
        "calldata_hex", "estimate_gas_request", "estimate_gas_response",
        "simulation_method", "simulation_request", "simulation_response",
        "simulation_balance_deltas",
    )
    if completed_stage is not None and completed_stage not in _STAGE_ORDER:
        raise RouteCostEvidenceError("raw transcript stage is invalid")
    if status is None and reason_code is None:
        if completed_stage is None:
            populated = [value.get(field) is not None for field in captured]
            if any(populated) and not all(populated):
                raise RouteCostEvidenceError("raw transcript capture is partial")
        else:
            stage_index = _STAGE_ORDER[completed_stage]
            required = {"calldata_hex"}
            if stage_index >= _STAGE_ORDER["call"]:
                required.update(("estimate_gas_request", "estimate_gas_response"))
            if stage_index >= _STAGE_ORDER["router_fee"]:
                required.update((
                    "simulation_method", "simulation_request",
                    "simulation_response", "simulation_balance_deltas",
                ))
            forbidden = set(captured) - required
            if any(value.get(field) is None for field in required) or any(
                value.get(field) is not None for field in forbidden
            ):
                raise RouteCostEvidenceError("raw transcript stage capture differs")
    else:
        if completed_stage is None or status not in {
            "observed", "unavailable", "failed"
        }:
            raise RouteCostEvidenceError("raw transcript terminal identity is invalid")
        expected_reason = {
            ("unavailable", "calldata_unavailable", "block"),
            ("failed", "calldata_mismatch", "block"),
            ("unavailable", "native_price_unavailable", "call"),
            ("failed", "native_price_invalid", "call"),
            ("unavailable", "gas_unavailable", "call"),
            ("failed", "gas_invalid", "call"),
            ("unavailable", "trace_unavailable", "router_fee"),
            ("failed", "trace_invalid", "router_fee"),
            ("unavailable", "transfer_tax_present", "transfer_tax"),
            (
                "unavailable", "transfer_behavior_unsupported",
                "transfer_tax",
            ),
            ("observed", None, "transfer_tax"),
        }
        terminal = (status, reason_code, completed_stage)
        if reason_code != "resource_limit" and terminal not in expected_reason:
            raise RouteCostEvidenceError("raw transcript terminal matrix is invalid")

        estimate_request_present = value.get("estimate_gas_request") is not None
        estimate_response_present = value.get("estimate_gas_response") is not None
        trace_method_present = value.get("simulation_method") is not None
        trace_request_present = value.get("simulation_request") is not None
        trace_response_present = value.get("simulation_response") is not None
        deltas_present = value.get("simulation_balance_deltas") is not None
        if estimate_response_present and not estimate_request_present:
            raise RouteCostEvidenceError("estimate response exists without request")
        if trace_response_present and not trace_request_present:
            raise RouteCostEvidenceError("trace response exists without request")
        if trace_method_present != trace_request_present:
            raise RouteCostEvidenceError("trace method/request matrix differs")
        if deltas_present != trace_response_present:
            raise RouteCostEvidenceError("trace response/delta matrix differs")
        if trace_request_present and not estimate_response_present:
            raise RouteCostEvidenceError("raw trace exists without successful gas estimate")

        if reason_code in {
            "calldata_unavailable", "calldata_mismatch",
            "native_price_unavailable", "native_price_invalid",
        }:
            expected_presence = (False, False, False, False)
        elif reason_code == "gas_unavailable":
            expected_presence = (
                estimate_request_present, False, False, False
            )
        elif reason_code == "gas_invalid":
            expected_presence = (True, False, False, False)
        elif reason_code == "trace_unavailable":
            expected_presence = (
                True, True, trace_request_present, False
            )
        elif reason_code == "trace_invalid":
            expected_presence = (True, True, True, False)
        elif status == "observed" or reason_code in {
            "transfer_tax_present", "transfer_behavior_unsupported",
        }:
            expected_presence = (True, True, True, True)
        else:
            expected_presence = (
                estimate_request_present, estimate_response_present,
                trace_request_present, trace_response_present,
            )
            resource_limit_prefixes = {
                "block": {(False, False, False, False)},
                "call": {
                    (False, False, False, False),
                    (True, False, False, False),
                },
                "gas": {(True, True, False, False)},
                "router_fee": {
                    (True, True, False, False),
                    (True, True, True, False),
                },
                "transfer_tax": {(True, True, True, True)},
            }
            if expected_presence not in resource_limit_prefixes.get(
                completed_stage, set()
            ):
                raise RouteCostEvidenceError(
                    "resource-limit stage capture prefix differs"
                )
        if (
            estimate_request_present, estimate_response_present,
            trace_request_present, trace_response_present,
        ) != expected_presence:
            raise RouteCostEvidenceError("raw transcript terminal capture differs")
        if reason_code == "calldata_unavailable":
            if value.get("calldata_hex") is not None:
                raise RouteCostEvidenceError(
                    "unavailable calldata terminal contains calldata"
                )
        elif value.get("calldata_hex") is None:
            raise RouteCostEvidenceError("raw transcript calldata is absent")

    has_estimate_request = value.get("estimate_gas_request") is not None
    has_estimate_response = value.get("estimate_gas_response") is not None
    has_trace_request = value.get("simulation_request") is not None
    has_trace_response = value.get("simulation_response") is not None
    if has_estimate_request:
        if adapter is None or market is None or chain is None:
            raise RouteCostEvidenceError("raw transcript adapter/market/chain is absent")
        estimate_request = _exact_fields(
            value.get("estimate_gas_request"), ESTIMATE_GAS_REQUEST_FIELDS,
            "estimate-gas request",
        )
        if (
            estimate_request.get("schema") != "route_cost_estimate_gas_request/v1"
            or estimate_request.get("jsonrpc") != "2.0"
            or estimate_request.get("method") != "eth_estimateGas"
        ):
            raise RouteCostEvidenceError("raw transcript RPC schema differs")
        estimate_id = _exact_int(
            estimate_request.get("id"), "estimate-gas request ID", 1
        )
        estimate_params = estimate_request.get("params")
        if not isinstance(estimate_params, list) or len(estimate_params) != 3:
            raise RouteCostEvidenceError("estimate-gas params are invalid")
        estimate_call = _exact_fields(
            estimate_params[0], ESTIMATE_CALL_OBJECT_FIELDS, "estimate call object"
        )
        block_tag = _quantity(estimate_params[1], "estimate block tag", positive=True)
        header = chain.get("block_header_result")
        if not isinstance(header, Mapping) or block_tag != header.get("number"):
            raise RouteCostEvidenceError("raw transcript block tag differs from fixed block")
        expected_common = {
            "from": adapter.get("simulation_sender_address"),
            "to": adapter.get("router_address"),
            "data": value.get("calldata_hex"),
            "value": "0x0",
        }
        if dict(estimate_call) != expected_common:
            raise RouteCostEvidenceError("raw transcript call object differs")
        _validate_state_overrides(
            estimate_params[2], calldata=value.get("calldata_hex"), adapter=adapter
        )
    if has_estimate_response:
        estimate_response = _exact_fields(
            value.get("estimate_gas_response"), ESTIMATE_GAS_RESPONSE_FIELDS,
            "estimate-gas response",
        )
        if (
            estimate_response.get("schema")
            != "route_cost_estimate_gas_response/v1"
            or estimate_response.get("jsonrpc") != "2.0"
        ):
            raise RouteCostEvidenceError("raw transcript RPC schema differs")
        estimate_response_id = _exact_int(
            estimate_response.get("id"), "estimate-gas response ID", 1
        )
        if estimate_response_id != estimate_id:
            raise RouteCostEvidenceError("raw transcript response ID differs")
        gas_result = _quantity(
            estimate_response.get("result"), "estimate-gas result", positive=True
        )
    if has_trace_request:
        if not has_estimate_response:
            raise RouteCostEvidenceError("raw trace exists without successful gas estimate")
        trace_request = _exact_fields(
            value.get("simulation_request"), TRACE_REQUEST_FIELDS, "trace request"
        )
        if value.get("simulation_method") != adapter.get("trace_method"):
            raise RouteCostEvidenceError("raw transcript trace method differs")
        if (
            trace_request.get("schema") != "route_cost_trace_request/v1"
            or trace_request.get("jsonrpc") != "2.0"
            or trace_request.get("method") != "debug_traceCall"
        ):
            raise RouteCostEvidenceError("raw transcript RPC schema differs")
        trace_id = _exact_int(trace_request.get("id"), "trace request ID", 1)
        if trace_id == estimate_id:
            raise RouteCostEvidenceError(
                "cross-request ID must differ"
            )
        trace_params = trace_request.get("params")
        if not isinstance(trace_params, list) or len(trace_params) != 3:
            raise RouteCostEvidenceError("trace params are invalid")
        trace_call = _exact_fields(
            trace_params[0], TRACE_CALL_OBJECT_FIELDS, "trace call object"
        )
        if trace_params[1] != block_tag:
            raise RouteCostEvidenceError("trace/estimate block tag differs")
        if any(
            trace_call.get(field) != expected
            for field, expected in expected_common.items()
        ) or trace_call.get("gas") != gas_result:
            raise RouteCostEvidenceError("raw transcript call object differs")
        options = _exact_fields(trace_params[2], TRACE_OPTIONS_FIELDS, "trace options")
        if estimate_params[2] != options.get("stateOverrides"):
            raise RouteCostEvidenceError("trace/estimate state overrides differ")
        if options.get("tracer") != "prestateTracer":
            raise RouteCostEvidenceError("trace tracer is invalid")
        config = _exact_fields(
            options.get("tracerConfig"), TRACE_CONFIG_FIELDS, "trace config"
        )
        if dict(config) != {
            "diffMode": True,
            "disableCode": True,
            "disableStorage": False,
        }:
            raise RouteCostEvidenceError("trace config differs")
    if has_trace_response:
        trace_response = _exact_fields(
            value.get("simulation_response"), TRACE_RESPONSE_FIELDS, "trace response"
        )
        if (
            trace_response.get("schema") != "route_cost_trace_response/v1"
            or trace_response.get("jsonrpc") != "2.0"
        ):
            raise RouteCostEvidenceError("raw transcript RPC schema differs")
        trace_response_id = _exact_int(
            trace_response.get("id"), "trace response ID", 1
        )
        if trace_response_id != trace_id:
            raise RouteCostEvidenceError("raw transcript response ID differs")
        deltas = _validate_storage_diffs(
            trace_response.get("storage_diffs"),
            adapter=adapter,
            market=market,
            calldata=value.get("calldata_hex"),
        )
        if value.get("simulation_balance_deltas") != deltas:
            raise RouteCostEvidenceError("raw transcript balance deltas differ")
    if len(canonical_json_bytes(value)) > MAX_TRANSCRIPT_BYTES:
        raise RouteCostEvidenceError("raw transcript exceeds its byte limit")


def _validate_transcript(
    value: Any,
    *,
    links: Mapping[str, Any],
    selected: Mapping[str, Mapping[str, Any]],
    chain_hashes: Mapping[str, Mapping[str, Any]],
    market_hashes: Mapping[str, Mapping[str, Any]],
    adapter: Mapping[str, Any],
    native_sha: Optional[str],
    native_evidence: Optional[Mapping[str, Any]],
    retained_pool_states: Mapping[str, Mapping[str, Any]],
    market_tokens: Optional[Mapping[str, Tuple[str, str]]] = None,
    simulation_targets: Optional[
        Mapping[Tuple[str, str], Mapping[str, str]]
    ] = None,
    evaluated_at: Optional[str] = None,
) -> Tuple[str, str, str]:
    _exact_fields(value, TRANSCRIPT_FIELDS, "transcript")
    if value.get("schema") != ROUTE_COST_TRANSCRIPT_SCHEMA:
        raise RouteCostEvidenceError("transcript schema is invalid")
    for field in (
        "run_id", "route_cohort_id", "candidate_source_generation",
        "route_universe_sha256", "adapter_registry_sha256",
        "selected_market_set_sha256", "trace_profile_generation",
        "submission_connector_profile_generation",
    ):
        if value.get(field) != links[field]:
            raise RouteCostEvidenceError("transcript lineage differs")
    market_id = value.get("market_id")
    if market_id not in selected or value.get("adapter_id") != ETHEREUM_V2_ADAPTER_ID:
        raise RouteCostEvidenceError("transcript market identity is invalid")
    direction = value.get("direction")
    notional = value.get("requested_notional_usd")
    if direction not in {"buy", "sell"} or notional not in REQUESTED_NOTIONALS_USD:
        raise RouteCostEvidenceError("transcript scenario identity is invalid")
    target_fields = (
        "simulation_target_token_address",
        "simulation_target_unit_decimals",
        "simulation_target_raw_quantity",
        "simulation_target_lattice_raw",
        "simulation_target_sha256",
    )
    expected_target = (
        None
        if simulation_targets is None
        else simulation_targets.get((market_id, notional))
    )
    if expected_target is None:
        if any(value.get(field) is not None for field in target_fields):
            raise RouteCostEvidenceError("transcript simulation target must be null")
    elif any(value.get(field) != expected_target.get(field) for field in target_fields):
        raise RouteCostEvidenceError("transcript simulation target differs")
    status = value.get("status")
    stage = value.get("completed_stage")
    reason = value.get("reason_code")
    if status not in {"observed", "unavailable", "failed"} or stage not in _STAGE_ORDER:
        raise RouteCostEvidenceError("transcript status/stage is invalid")
    core_pair = (value.get("core_pool_state_id"), value.get("core_pool_state_sha256"))
    if (core_pair[0] is None) != (core_pair[1] is None):
        raise RouteCostEvidenceError("transcript core-state null matrix is invalid")
    if core_pair[0] is not None:
        if not isinstance(core_pair[0], str) or not core_pair[0]:
            raise RouteCostEvidenceError("transcript core state ID is invalid")
        _sha256(core_pair[1], "transcript core state hash")
        retained = retained_pool_states.get(market_id)
        if retained is None:
            raise RouteCostEvidenceError("transcript retained core pool state is absent")
        if (
            core_pair[0] != retained.get("state_id")
            or core_pair[1] != retained.get("_physical_sha256")
        ):
            raise RouteCostEvidenceError("transcript retained core state differs")
    elif market_id in retained_pool_states:
        raise RouteCostEvidenceError("retained core state is unreferenced by transcript")
    chain = None
    market = None
    if value.get("chain_evidence_sha256") is not None:
        chain_sha = _sha256(value["chain_evidence_sha256"], "transcript chain hash")
        chain = chain_hashes.get(chain_sha)
        if chain is None:
            raise RouteCostEvidenceError("transcript chain evidence is missing")
    if value.get("market_evidence_sha256") is not None:
        market_sha = _sha256(value["market_evidence_sha256"], "transcript market hash")
        market = market_hashes.get(market_sha)
        if market is None or market.get("market_id") != market_id:
            raise RouteCostEvidenceError("transcript market evidence is missing")
        if (
            market.get("core_pool_state_id") != core_pair[0]
            or market.get("core_pool_state_sha256") != core_pair[1]
        ):
            raise RouteCostEvidenceError("transcript/market core state differs")

    nested_fields = (
        "block_evidence", "call_evidence", "gas_evidence",
        "router_fee_evidence", "transfer_tax_evidence", "raw_transcript",
    )
    if status == "observed":
        if stage != "transfer_tax" or reason is not None or any(value.get(field) is None for field in nested_fields):
            raise RouteCostEvidenceError("observed transcript presence matrix is invalid")
        if chain is None or market is None or core_pair[0] is None:
            raise RouteCostEvidenceError("observed transcript shared evidence is absent")
    else:
        allowed = _UNAVAILABLE_TRANSCRIPT_REASONS if status == "unavailable" else _FAILED_TRANSCRIPT_REASONS
        if reason not in allowed:
            raise RouteCostEvidenceError("transcript reason is invalid")
        expected_stage = {
            "strict_cost_adapter_unsupported": "none",
            "core_pool_state_unavailable": "none",
            "core_pool_state_invalid": "none",
            "trace_profile_missing": "none",
            "rpc_unavailable": "none",
            "rpc_invalid": "none",
            "fixed_block_unavailable": "none",
            "fixed_block_mismatch": "none",
            "router_identity_unavailable": "none",
            "router_identity_mismatch": "none",
            "pair_identity_unavailable": "none",
            "pair_identity_mismatch": "none",
            "token_funding_code_mismatch": "none",
            "calldata_unavailable": "block",
            "calldata_mismatch": "block",
            "gas_unavailable": "call",
            "gas_invalid": "call",
            "native_price_unavailable": "call",
            "native_price_invalid": "call",
            "trace_unavailable": "router_fee",
            "trace_invalid": "router_fee",
            "transfer_tax_present": "transfer_tax",
            "transfer_behavior_unsupported": "transfer_tax",
        }.get(reason)
        if reason != "resource_limit" and stage != expected_stage:
            raise RouteCostEvidenceError("transcript stage does not match reason")
        if reason == "strict_cost_adapter_unsupported":
            if selected[market_id]["structural_support_status"] != "unsupported" or any(
                value.get(field) is not None for field in (
                    "core_pool_state_id", "core_pool_state_sha256",
                    "chain_evidence_sha256", "market_evidence_sha256",
                ) + nested_fields
            ):
                raise RouteCostEvidenceError("unsupported transcript null matrix is invalid")
        elif reason.startswith("core_pool_state_"):
            if any(value.get(field) is not None for field in (
                "core_pool_state_id", "core_pool_state_sha256",
                "chain_evidence_sha256", "market_evidence_sha256",
            ) + nested_fields):
                raise RouteCostEvidenceError("core-state transcript matrix is invalid")
        elif stage == "none":
            if any(value.get(field) is not None for field in nested_fields):
                raise RouteCostEvidenceError("stage-none transcript has nested evidence")
        else:
            prefix_by_stage = {
                "block": ("block_evidence", "raw_transcript"),
                "call": ("block_evidence", "call_evidence", "raw_transcript"),
                "gas": (
                    "block_evidence", "call_evidence", "gas_evidence",
                    "raw_transcript",
                ),
                "router_fee": (
                    "block_evidence", "call_evidence", "gas_evidence",
                    "router_fee_evidence", "raw_transcript",
                ),
                "transfer_tax": nested_fields,
            }
            required = set(prefix_by_stage[stage])
            if chain is None or market is None or core_pair[0] is None:
                raise RouteCostEvidenceError("completed stage lacks shared evidence")
            if any(value.get(field) is None for field in required) or any(
                value.get(field) is not None
                for field in set(nested_fields) - required
            ):
                raise RouteCostEvidenceError("completed-stage presence matrix differs")

    raw = value.get("raw_transcript")
    if raw is not None:
        if evaluated_at is None:
            raise RouteCostEvidenceError(
                "raw transcript evaluated_at is absent"
            )
        raw_started = _timestamp_value(
            raw.get("captured_started_at"), "raw captured_started_at"
        )
        raw_finished = _timestamp_value(
            raw.get("captured_finished_at"), "raw captured_finished_at"
        )
        evaluated = _timestamp_value(evaluated_at, "route-cost evaluated_at")
        if (
            raw_finished - raw_started > Decimal(35)
            or raw_finished > evaluated
        ):
            raise RouteCostEvidenceError(
                "raw transcript lies outside the run window"
            )
        if chain is not None:
            chain_started = _timestamp_value(
                chain.get("captured_started_at"), "chain captured_started_at"
            )
            chain_finished = _timestamp_value(
                chain.get("captured_finished_at"), "chain captured_finished_at"
            )
            if (
                raw_started < chain_finished
                or evaluated - chain_started > Decimal(60)
            ):
                raise RouteCostEvidenceError(
                    "raw transcript lies outside the run window"
                )
        if native_evidence is not None:
            native_observed = _timestamp_value(
                native_evidence.get("observed_at"),
                "native evidence observed_at",
            )
            native_valid = _timestamp_value(
                native_evidence.get("valid_until"),
                "native evidence valid_until",
            )
            if not (
                native_observed <= raw_started <= raw_finished <= native_valid
                and evaluated <= native_valid
            ):
                raise RouteCostEvidenceError(
                    "native validity does not cover transcript run window"
                )
        _validate_raw_transcript(
            raw, transcript=value, adapter=adapter, market=market,
            chain=chain,
            completed_stage=stage,
            status=status,
            reason_code=reason,
        )
    if value.get("block_evidence") is not None:
        if chain is None or market is None:
            raise RouteCostEvidenceError("block evidence lacks shared evidence")
        _validate_block_evidence(
            value["block_evidence"], transcript=value, chain=chain,
            market=market, adapter=adapter,
            retained_pool_state=retained_pool_states[market_id],
        )
        if raw is None or value["block_evidence"]["rpc_transcript_sha256"] != _raw_rpc_transcript_sha256(raw):
            raise RouteCostEvidenceError("block RPC transcript hash differs")
    if value.get("call_evidence") is not None:
        if (
            raw is None
            or market_tokens is None
            or market_id not in market_tokens
            or market_id not in retained_pool_states
            or expected_target is None
        ):
            raise RouteCostEvidenceError(
                "call evidence lacks raw transcript or selected token identity"
            )
        _validate_call_evidence(
            value["call_evidence"],
            transcript=value,
            raw=raw,
            adapter=adapter,
            chain=chain,
            market=market,
            market_tokens=market_tokens[market_id],
            simulation_target=expected_target,
            retained_pool_state=retained_pool_states[market_id],
        )
    if value.get("gas_evidence") is not None:
        if chain is None or raw is None:
            raise RouteCostEvidenceError("gas evidence lacks chain evidence")
        _validate_gas_evidence(
            value["gas_evidence"],
            chain=chain,
            native_sha=native_sha,
            native_evidence=native_evidence,
            raw=raw,
        )
    if value.get("router_fee_evidence") is not None:
        _validate_component(
            value["router_fee_evidence"], component="router",
            expected_source_sha=value.get("market_evidence_sha256"),
        )
    if value.get("transfer_tax_evidence") is not None:
        _validate_component(
            value["transfer_tax_evidence"], component="transfer", raw=raw,
            call_evidence=value.get("call_evidence"),
        )
        if status == "observed" and value["transfer_tax_evidence"]["status"] not in {
            "authenticated", "not_applicable"
        }:
            raise RouteCostEvidenceError("observed transfer tax is not authenticated")
    if len(canonical_json_bytes(value)) > MAX_TRANSCRIPT_BYTES:
        raise RouteCostEvidenceError("transcript exceeds its byte limit")
    return str(market_id), str(direction), str(notional)


def _validate_policy_member(
    value: Any,
    *,
    route_sides: Optional[Mapping[str, Tuple[bool, bool]]] = None,
) -> Tuple[str, str]:
    _exact_fields(value, POLICY_MEMBER_FIELDS, "policy member")
    if value.get("schema") != ROUTE_COST_POLICY_MEMBER_SCHEMA:
        raise RouteCostEvidenceError("policy member schema is invalid")
    route_id = value.get("route_id")
    if not isinstance(route_id, str) or not route_id:
        raise RouteCostEvidenceError("policy member route ID is invalid")
    notional = value.get("requested_notional_usd")
    if notional not in REQUESTED_NOTIONALS_USD:
        raise RouteCostEvidenceError("policy member notional is invalid")
    status = value.get("status")
    bound_fields = ("buy_submission_loss_bps", "sell_submission_loss_bps")
    if status == "observed":
        if value.get("reason_code") is not None or value.get("submission_mode") != "private_relay":
            raise RouteCostEvidenceError("observed policy member is invalid")
        _required_text(value.get("policy_id"), "policy ID", _POLICY_ID)
        for field in bound_fields:
            if value.get(field) is not None:
                text = _integer_text(value[field], "policy " + field)
                if int(text) > 10000:
                    raise RouteCostEvidenceError("policy bound exceeds 10000")
        if route_sides is not None:
            sides = route_sides.get(route_id)
            if sides is None:
                raise RouteCostEvidenceError("policy route is outside binding scope")
            for field, is_dex in zip(bound_fields, sides):
                if (value.get(field) is not None) != is_dex:
                    raise RouteCostEvidenceError("policy DEX/CEX bound matrix differs")
    elif status in {"unavailable", "failed"}:
        allowed = (
            {"submission_connector_missing", "submission_connector_unavailable"}
            if status == "unavailable" else
            {"submission_connector_invalid", "submission_policy_invalid"}
        )
        if value.get("reason_code") not in allowed or any(
            value.get(field) is not None
            for field in ("submission_mode", "policy_id") + bound_fields
        ):
            raise RouteCostEvidenceError("terminal policy member matrix is invalid")
    else:
        raise RouteCostEvidenceError("policy member status is invalid")
    return route_id, notional


def _validate_policy_snapshot(
    value: Any,
    links: Mapping[str, Any],
    binding_scope_empty: bool,
    *,
    connector_registry: Optional[Mapping[str, Any]] = None,
    route_sides: Optional[Mapping[str, Tuple[bool, bool]]] = None,
    permit_authenticated: bool = False,
    verified_signature: bool = False,
) -> Dict[Tuple[str, str], Mapping[str, Any]]:
    _exact_fields(value, POLICY_SNAPSHOT_FIELDS, "policy snapshot")
    if value.get("schema") != ROUTE_COST_POLICY_SNAPSHOT_SCHEMA:
        raise RouteCostEvidenceError("policy snapshot schema is invalid")
    for field in (
        "run_id", "route_cohort_id", "candidate_source_generation",
        "route_universe_sha256", "adapter_registry_sha256",
        "selected_market_set_sha256", "connector_key_registry_sha256",
        "trace_profile_generation", "submission_connector_profile_generation",
    ):
        if value.get(field) != links[field]:
            raise RouteCostEvidenceError("policy snapshot lineage differs")
    members = value.get("members")
    count = _exact_int(value.get("member_count"), "policy member count", 0, MAX_BINDINGS)
    if not isinstance(members, list) or len(members) != count:
        raise RouteCostEvidenceError("policy member count differs")
    identities: List[Tuple[str, str]] = []
    by_id = {}
    for member in members:
        identity = _validate_policy_member(member, route_sides=route_sides)
        identities.append(identity)
        by_id[identity] = member
    expected_order = sorted(identities, key=lambda item: (item[0], Decimal(item[1])))
    if identities != expected_order or len(identities) != len(set(identities)):
        raise RouteCostEvidenceError("policy members are not canonical")
    if typed_sha256(b"route-cost-submission-policy-member-set/v1\n", members) != value.get("member_set_sha256"):
        raise RouteCostEvidenceError("policy member-set hash differs")
    status = value.get("status")
    if binding_scope_empty:
        null_fields = (
            "observed_at", "valid_until", "issuer_key_id", "signature_algorithm",
            "attested_payload_sha256", "signature",
        )
        if (
            members != []
            or status != "not_applicable"
            or value.get("reason_code") != "scope_empty"
            or any(value.get(field) is not None for field in null_fields)
        ):
            raise RouteCostEvidenceError("empty policy snapshot matrix is invalid")
    elif status == "authenticated":
        if not permit_authenticated or not verified_signature:
            raise RouteCostEvidenceError(
                "authenticated policy snapshot requires sealed SSHSIG verification"
            )
        if value.get("reason_code") is not None:
            raise RouteCostEvidenceError("authenticated snapshot has a reason")
        _required_text(value.get("connector_id"), "snapshot connector ID", _CONNECTOR_ID)
        observed = _timestamp_value(value.get("observed_at"), "snapshot observed_at")
        valid = _timestamp_value(value.get("valid_until"), "snapshot valid_until")
        if observed > valid:
            raise RouteCostEvidenceError("snapshot validity is reversed")
        issuer_key_id = _required_text(
            value.get("issuer_key_id"), "snapshot issuer key", _LOWER_ID
        )
        if value.get("signature_algorithm") != "ssh-ed25519-sshsig-v1":
            raise RouteCostEvidenceError("snapshot signature algorithm is invalid")
        attestation = {
            "schema": "route_cost_submission_policy_snapshot_attestation/v1",
            "run_id": value["run_id"],
            "route_cohort_id": value["route_cohort_id"],
            "candidate_source_generation": value["candidate_source_generation"],
            "route_universe_sha256": value["route_universe_sha256"],
            "selected_market_set_sha256": value["selected_market_set_sha256"],
            "adapter_registry_sha256": value["adapter_registry_sha256"],
            "connector_key_registry_sha256": value["connector_key_registry_sha256"],
            "trace_profile_generation": value["trace_profile_generation"],
            "submission_connector_profile_generation": value[
                "submission_connector_profile_generation"
            ],
            "connector_id": value["connector_id"],
            "member_count": value["member_count"],
            "member_set_sha256": value["member_set_sha256"],
            "observed_at": value["observed_at"],
            "valid_until": value["valid_until"],
        }
        expected_attestation_sha = typed_sha256(
            b"route-cost-submission-policy-attestation/v1\n", attestation
        )
        if value.get("attested_payload_sha256") != expected_attestation_sha:
            raise RouteCostEvidenceError("snapshot attested-payload hash differs")
        signature = value.get("signature")
        key_blob = _sshsig_bytes(signature)
        if connector_registry is None:
            raise RouteCostEvidenceError("connector key registry is absent")
        matching_keys = [
            row for row in connector_registry["keys"]
            if row["key_id"] == issuer_key_id
            and row["connector_id"] == value["connector_id"]
        ]
        if len(matching_keys) != 1:
            raise RouteCostEvidenceError("snapshot issuer key is unknown")
        key = matching_keys[0]
        key_text = key["public_key"].split(" ", 1)[1]
        if base64.b64decode(key_text, validate=True) != key_blob:
            raise RouteCostEvidenceError("snapshot signature key differs")
        if (
            _timestamp_value(key["valid_from"], "connector key valid_from") > observed
            or _timestamp_value(key["valid_until"], "connector key valid_until") < valid
        ):
            raise RouteCostEvidenceError("snapshot issuer key is outside validity")
    elif status in {"unavailable", "failed"}:
        allowed_reason = (
            {"submission_connector_missing", "submission_connector_unavailable"}
            if status == "unavailable"
            else {"submission_connector_invalid", "submission_policy_invalid"}
        )
        if value.get("reason_code") not in allowed_reason:
            raise RouteCostEvidenceError("policy snapshot reason is invalid")
        connector_id = value.get("connector_id")
        if connector_id is not None:
            _required_text(connector_id, "snapshot connector ID", _CONNECTOR_ID)
        if any(value.get(field) is not None for field in (
            "observed_at", "valid_until", "issuer_key_id", "signature_algorithm",
            "attested_payload_sha256", "signature",
        )):
            raise RouteCostEvidenceError("terminal policy snapshot matrix is invalid")
        if any(member.get("status") != status for member in members):
            raise RouteCostEvidenceError("terminal policy members differ from snapshot")
    elif status != "not_applicable":
        raise RouteCostEvidenceError("policy snapshot status is invalid")
    elif not binding_scope_empty:
        raise RouteCostEvidenceError("not-applicable policy has nonempty scope")
    return by_id


def _policy_attestation(snapshot: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "schema": "route_cost_submission_policy_snapshot_attestation/v1",
        "run_id": snapshot["run_id"],
        "route_cohort_id": snapshot["route_cohort_id"],
        "candidate_source_generation": snapshot["candidate_source_generation"],
        "route_universe_sha256": snapshot["route_universe_sha256"],
        "selected_market_set_sha256": snapshot["selected_market_set_sha256"],
        "adapter_registry_sha256": snapshot["adapter_registry_sha256"],
        "connector_key_registry_sha256": snapshot["connector_key_registry_sha256"],
        "trace_profile_generation": snapshot["trace_profile_generation"],
        "submission_connector_profile_generation": snapshot[
            "submission_connector_profile_generation"
        ],
        "connector_id": snapshot["connector_id"],
        "member_count": snapshot["member_count"],
        "member_set_sha256": snapshot["member_set_sha256"],
        "observed_at": snapshot["observed_at"],
        "valid_until": snapshot["valid_until"],
    }


def _verify_snapshot_sshsig_fixed(
    snapshot: Mapping[str, Any], connector_registry: Mapping[str, Any]
) -> None:
    """Verify one aggregate SSHSIG through the fixed system binary and FDs."""
    signature = snapshot.get("signature")
    _sshsig_bytes(signature)
    connector_id = _required_text(
        snapshot.get("connector_id"), "snapshot connector ID", _CONNECTOR_ID
    )
    issuer_key_id = _required_text(
        snapshot.get("issuer_key_id"), "snapshot issuer key", _LOWER_ID
    )
    keys = [
        row for row in connector_registry.get("keys", [])
        if row.get("key_id") == issuer_key_id
        and row.get("connector_id") == connector_id
    ]
    if len(keys) != 1:
        raise RouteCostEvidenceError("snapshot issuer key is unknown")
    public_key = keys[0]["public_key"]
    allowed = "{} {}\n".format(connector_id, public_key).encode("ascii")
    signature_bytes = signature.encode("ascii")
    payload = canonical_json_bytes(_policy_attestation(snapshot))
    allowed_file = tempfile.TemporaryFile()
    signature_file = tempfile.TemporaryFile()
    try:
        allowed_file.write(allowed)
        allowed_file.flush()
        allowed_file.seek(0)
        signature_file.write(signature_bytes)
        signature_file.flush()
        signature_file.seek(0)
        allowed_fd = allowed_file.fileno()
        signature_fd = signature_file.fileno()
        if sys.platform == "darwin":
            allowed_path = "/dev/fd/{}".format(allowed_fd)
            signature_path = "/dev/fd/{}".format(signature_fd)
        elif sys.platform.startswith("linux"):
            allowed_path = "/proc/self/fd/{}".format(allowed_fd)
            signature_path = "/proc/self/fd/{}".format(signature_fd)
        else:
            raise RouteCostEvidenceError("SSHSIG descriptor paths are unsupported")
        argv = (
            "/usr/bin/ssh-keygen",
            "-Y",
            "verify",
            "-f",
            allowed_path,
            "-I",
            connector_id,
            "-n",
            "route-cost-submission-policy-v1",
            "-s",
            signature_path,
        )
        try:
            completed = subprocess.run(
                argv,
                input=payload,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=2,
                check=False,
                close_fds=True,
                pass_fds=(allowed_fd, signature_fd),
                env={"LC_ALL": "C", "PATH": "/usr/bin:/bin"},
            )
        except (OSError, subprocess.SubprocessError) as error:
            raise RouteCostEvidenceError("SSHSIG verification is unavailable") from error
        if (
            completed.returncode != 0
            or len(completed.stdout) > 8192
            or len(completed.stderr) > 8192
        ):
            raise RouteCostEvidenceError("SSHSIG verification failed")
    finally:
        allowed_file.close()
        signature_file.close()


def _expected_binding_scope(
    universe: Mapping[str, Any], selected: Mapping[str, Mapping[str, Any]]
) -> Tuple[List[Tuple[str, str]], Dict[str, Tuple[bool, bool]]]:
    supported = {
        market_id for market_id, row in selected.items()
        if row["structural_support_status"] == "supported"
    }
    expected = []
    route_sides: Dict[str, Tuple[bool, bool]] = {}
    for route in universe.get("routes", []):
        if not isinstance(route, Mapping) or route.get("route_class") != "candidate":
            continue
        if not ({route.get("buy_market_id"), route.get("sell_market_id")} & supported):
            continue
        route_id = route.get("route_id")
        if not isinstance(route_id, str) or not route_id:
            raise RouteCostEvidenceError("candidate route ID is invalid")
        route_sides[route_id] = (
            route.get("buy_market_id") in supported,
            route.get("sell_market_id") in supported,
        )
        for notional in REQUESTED_NOTIONALS_USD:
            expected.append((route_id, notional))
    expected.sort(key=lambda item: (item[0], Decimal(item[1])))
    if len(expected) > MAX_BINDINGS:
        raise RouteCostEvidenceError("binding scope exceeds its limit")
    return expected, route_sides


def _expected_bindings(
    universe: Mapping[str, Any], selected: Mapping[str, Mapping[str, Any]]
) -> List[Tuple[str, str]]:
    return _expected_binding_scope(universe, selected)[0]


def _producer_lineage(
    *,
    universe: Mapping[str, Any],
    run_id: str,
    route_cohort_id: str,
    candidate_source_generation: str,
    route_universe_sha256: str,
    adapter_registry: Mapping[str, Any],
    trace_profile_identity: Mapping[str, Any],
    submission_connector_profile_identity: Mapping[str, Any],
    connector_key_registry: Optional[Mapping[str, Any]] = None,
) -> Tuple[
    Dict[str, Any], Dict[str, Any], Dict[str, Any], Optional[Dict[str, Any]],
    List[Dict[str, Any]], Dict[str, Dict[str, Any]], Dict[str, Any],
]:
    """Validate the deterministic, secret-free inputs shared by producers."""
    _required_text(run_id, "run ID", _SAFE_RUN_ID)
    if _COHORT_ID.fullmatch(route_cohort_id or "") is None:
        raise RouteCostEvidenceError("route cohort ID is invalid")
    _sha256(candidate_source_generation, "candidate source generation")
    _sha256(route_universe_sha256, "route universe hash")
    if physical_sha256(universe) != route_universe_sha256:
        raise RouteCostEvidenceError("route universe physical hash differs")
    if universe.get("candidate_source_generation") != candidate_source_generation:
        raise RouteCostEvidenceError("route universe generation differs")
    adapter_snapshot = validate_adapter_registry(adapter_registry)
    connector_snapshot = (
        None if connector_key_registry is None
        else validate_connector_key_registry(connector_key_registry)
    )
    trace_identity, trace_generation = _validate_profile_identity(
        trace_profile_identity, kind="trace"
    )
    connector_identity, connector_generation = _validate_profile_identity(
        submission_connector_profile_identity, kind="connector"
    )
    selected = build_selected_markets(universe, adapter_snapshot)
    selected_by_id = {row["market_id"]: row for row in selected}
    links = {
        "run_id": run_id,
        "route_cohort_id": route_cohort_id,
        "candidate_source_generation": candidate_source_generation,
        "route_universe_sha256": route_universe_sha256,
        "selected_market_set_sha256": selected_market_set_sha256(selected),
        "adapter_registry_sha256": physical_sha256(adapter_snapshot),
        "trace_profile_generation": trace_generation,
        "submission_connector_profile_generation": connector_generation,
    }
    if connector_snapshot is not None:
        links["connector_key_registry_sha256"] = physical_sha256(
            connector_snapshot
        )
    return (
        adapter_snapshot,
        trace_identity,
        connector_identity,
        connector_snapshot,
        selected,
        selected_by_id,
        links,
    )


def _retained_pool_states_for_terminal_inventory(
    members: Mapping[str, Mapping[str, Any]],
    supported: Iterable[str],
) -> Dict[str, Dict[str, Any]]:
    if not isinstance(members, Mapping):
        raise RouteCostEvidenceError("retained pool-state inventory is invalid")
    supported_set = set(supported)
    if not set(members).issubset(supported_set):
        raise RouteCostEvidenceError("retained pool-state inventory differs")
    states: Dict[str, Dict[str, Any]] = {}
    for market_id in sorted(members):
        member = members[market_id]
        if (
            not isinstance(member, Mapping)
            or set(member) != {"descriptor", "payload"}
            or not isinstance(member.get("descriptor"), Mapping)
            or member["descriptor"].get("market_id") != market_id
        ):
            raise RouteCostEvidenceError("retained pool-state member differs")
        state = validate_retained_v2_pool_state_member(
            member.get("payload"), descriptor=member.get("descriptor")
        )
        state["_physical_sha256"] = member["descriptor"]["sha256"]
        states[market_id] = state
    return states


def build_terminal_transcript_inventory(
    *,
    universe: Mapping[str, Any],
    run_id: str,
    route_cohort_id: str,
    candidate_source_generation: str,
    route_universe_sha256: str,
    adapter_registry: Mapping[str, Any],
    trace_profile_identity: Mapping[str, Any],
    submission_connector_profile_identity: Mapping[str, Any],
    retained_typed_pool_state_members: Mapping[str, Mapping[str, Any]],
    terminal_reason_by_market: Mapping[str, str],
) -> List[Dict[str, Any]]:
    """Build every deterministic terminal scenario for a selected cohort.

    Unsupported rows always close as structural unsupportedness.  Supported
    rows use one closed caller-reported capture outcome, while all status,
    stage, core-state, simulation-target, and null fields are derived here.
    This first producer package intentionally covers only terminal reasons
    whose exact matrix needs no captured chain/market/transcript object.
    """
    (
        _adapter_snapshot,
        trace_identity,
        _connector_identity,
        _connector_snapshot,
        selected,
        selected_by_id,
        links,
    ) = _producer_lineage(
        universe=universe,
        run_id=run_id,
        route_cohort_id=route_cohort_id,
        candidate_source_generation=candidate_source_generation,
        route_universe_sha256=route_universe_sha256,
        adapter_registry=adapter_registry,
        trace_profile_identity=trace_profile_identity,
        submission_connector_profile_identity=(
            submission_connector_profile_identity
        ),
    )
    supported = {
        market_id for market_id, row in selected_by_id.items()
        if row["structural_support_status"] == "supported"
    }
    if (
        not isinstance(terminal_reason_by_market, Mapping)
        or set(terminal_reason_by_market) != supported
    ):
        raise RouteCostEvidenceError("terminal transcript reason inventory differs")
    first_package_reasons = {
        "core_pool_state_unavailable",
        "core_pool_state_invalid",
        "trace_profile_missing",
        "rpc_unavailable",
        "rpc_invalid",
        "fixed_block_unavailable",
        "fixed_block_mismatch",
    }
    for reason in terminal_reason_by_market.values():
        if reason not in first_package_reasons:
            raise RouteCostEvidenceError("terminal transcript reason is invalid")
    if trace_identity["status"] == "missing" and any(
        reason not in {
            "core_pool_state_unavailable",
            "core_pool_state_invalid",
            "trace_profile_missing",
        }
        for reason in terminal_reason_by_market.values()
    ):
        raise RouteCostEvidenceError("missing trace profile reason differs")
    if trace_identity["status"] == "available" and any(
        reason == "trace_profile_missing"
        for reason in terminal_reason_by_market.values()
    ):
        raise RouteCostEvidenceError("available trace profile reason differs")

    retained_states = _retained_pool_states_for_terminal_inventory(
        retained_typed_pool_state_members, supported
    )
    universe_legs = universe.get("selected_legs")
    if not isinstance(universe_legs, list):
        raise RouteCostEvidenceError("route universe selected legs are invalid")
    simulation_targets = build_simulation_targets(
        universe_legs, selected_by_id, retained_states
    )
    rows: List[Dict[str, Any]] = []
    for market in selected:
        market_id = market["market_id"]
        supported_market = market_id in supported
        reason = (
            terminal_reason_by_market[market_id]
            if supported_market else "strict_cost_adapter_unsupported"
        )
        status = "failed" if reason in _FAILED_TRANSCRIPT_REASONS else "unavailable"
        retained = retained_states.get(market_id)
        retains_core = retained is not None and not reason.startswith(
            "core_pool_state_"
        )
        for direction in ("buy", "sell"):
            for notional in REQUESTED_NOTIONALS_USD:
                target = (
                    simulation_targets.get((market_id, notional))
                    if retains_core else None
                )
                target_fields = {
                    "simulation_target_token_address": None,
                    "simulation_target_unit_decimals": None,
                    "simulation_target_raw_quantity": None,
                    "simulation_target_lattice_raw": None,
                    "simulation_target_sha256": None,
                }
                if target is not None:
                    target_fields.update(target)
                rows.append({
                    "schema": ROUTE_COST_TRANSCRIPT_SCHEMA,
                    "run_id": links["run_id"],
                    "route_cohort_id": links["route_cohort_id"],
                    "candidate_source_generation": links[
                        "candidate_source_generation"
                    ],
                    "route_universe_sha256": links["route_universe_sha256"],
                    "adapter_registry_sha256": links[
                        "adapter_registry_sha256"
                    ],
                    "selected_market_set_sha256": links[
                        "selected_market_set_sha256"
                    ],
                    "trace_profile_generation": links[
                        "trace_profile_generation"
                    ],
                    "submission_connector_profile_generation": links[
                        "submission_connector_profile_generation"
                    ],
                    "market_id": market_id,
                    "direction": direction,
                    "requested_notional_usd": notional,
                    "adapter_id": market["adapter_id"],
                    **target_fields,
                    "core_pool_state_id": (
                        retained["state_id"] if retains_core else None
                    ),
                    "core_pool_state_sha256": (
                        retained["_physical_sha256"] if retains_core else None
                    ),
                    "chain_evidence_sha256": None,
                    "market_evidence_sha256": None,
                    "status": status,
                    "completed_stage": "none",
                    "reason_code": reason,
                    "block_evidence": None,
                    "call_evidence": None,
                    "gas_evidence": None,
                    "router_fee_evidence": None,
                    "transfer_tax_evidence": None,
                    "raw_transcript": None,
                })
    return _canonical_copy(rows)


def build_submission_policy_scope(
    *, universe: Mapping[str, Any], adapter_registry: Mapping[str, Any]
) -> List[Dict[str, str]]:
    """Derive the canonical nonempty connector request member inventory."""
    adapter_snapshot = validate_adapter_registry(adapter_registry)
    selected = build_selected_markets(universe, adapter_snapshot)
    expected, _route_sides = _expected_binding_scope(
        universe, {row["market_id"]: row for row in selected}
    )
    return [
        {"route_id": route_id, "requested_notional_usd": notional}
        for route_id, notional in expected
    ]


def build_terminal_submission_policy_snapshot(
    *,
    universe: Mapping[str, Any],
    run_id: str,
    route_cohort_id: str,
    candidate_source_generation: str,
    route_universe_sha256: str,
    adapter_registry: Mapping[str, Any],
    connector_key_registry: Mapping[str, Any],
    trace_profile_identity: Mapping[str, Any],
    submission_connector_profile_identity: Mapping[str, Any],
    reason_code: str,
) -> Dict[str, Any]:
    """Build one exact local terminal policy snapshot from a closed outcome."""
    (
        adapter_snapshot,
        _trace_identity,
        connector_identity,
        _connector_snapshot,
        _selected,
        _selected_by_id,
        links,
    ) = _producer_lineage(
        universe=universe,
        run_id=run_id,
        route_cohort_id=route_cohort_id,
        candidate_source_generation=candidate_source_generation,
        route_universe_sha256=route_universe_sha256,
        adapter_registry=adapter_registry,
        connector_key_registry=connector_key_registry,
        trace_profile_identity=trace_profile_identity,
        submission_connector_profile_identity=(
            submission_connector_profile_identity
        ),
    )
    scope = build_submission_policy_scope(
        universe=universe, adapter_registry=adapter_snapshot
    )
    if not scope:
        if reason_code != "scope_empty":
            raise RouteCostEvidenceError("empty policy scope reason is invalid")
        status = "not_applicable"
        members: List[Dict[str, Any]] = []
    else:
        if reason_code == "submission_connector_missing":
            if connector_identity["status"] != "missing":
                raise RouteCostEvidenceError("connector missing reason differs")
            status = "unavailable"
        elif reason_code == "submission_connector_unavailable":
            if connector_identity["status"] != "available":
                raise RouteCostEvidenceError("connector unavailable reason differs")
            status = "unavailable"
        elif reason_code == "submission_connector_invalid":
            if connector_identity["status"] != "available":
                raise RouteCostEvidenceError("connector invalid reason differs")
            status = "failed"
        else:
            raise RouteCostEvidenceError("terminal policy reason is invalid")
        members = [{
            "schema": ROUTE_COST_POLICY_MEMBER_SCHEMA,
            "route_id": row["route_id"],
            "requested_notional_usd": row["requested_notional_usd"],
            "status": status,
            "reason_code": reason_code,
            "submission_mode": None,
            "policy_id": None,
            "buy_submission_loss_bps": None,
            "sell_submission_loss_bps": None,
        } for row in scope]
    snapshot = {
        "schema": ROUTE_COST_POLICY_SNAPSHOT_SCHEMA,
        "run_id": links["run_id"],
        "route_cohort_id": links["route_cohort_id"],
        "candidate_source_generation": links["candidate_source_generation"],
        "route_universe_sha256": links["route_universe_sha256"],
        "adapter_registry_sha256": links["adapter_registry_sha256"],
        "selected_market_set_sha256": links["selected_market_set_sha256"],
        "connector_key_registry_sha256": links[
            "connector_key_registry_sha256"
        ],
        "trace_profile_generation": links["trace_profile_generation"],
        "submission_connector_profile_generation": links[
            "submission_connector_profile_generation"
        ],
        "connector_id": connector_identity["connector_id"],
        "member_count": len(members),
        "members": members,
        "member_set_sha256": typed_sha256(
            b"route-cost-submission-policy-member-set/v1\n", members
        ),
        "status": status,
        "reason_code": reason_code,
        "observed_at": None,
        "valid_until": None,
        "issuer_key_id": None,
        "signature_algorithm": None,
        "attested_payload_sha256": None,
        "signature": None,
    }
    _validate_policy_snapshot(
        snapshot,
        links,
        not scope,
        route_sides=_expected_binding_scope(
            universe,
            {row["market_id"]: row for row in build_selected_markets(
                universe, adapter_snapshot
            )},
        )[1],
    )
    return _canonical_copy(snapshot)


def build_submission_policy_request(
    *,
    universe: Mapping[str, Any],
    run_id: str,
    route_cohort_id: str,
    candidate_source_generation: str,
    route_universe_sha256: str,
    adapter_registry: Mapping[str, Any],
    connector_key_registry: Mapping[str, Any],
    trace_profile_identity: Mapping[str, Any],
    submission_connector_profile_identity: Mapping[str, Any],
) -> Dict[str, Any]:
    """Build the exact nonempty connector request and its typed request ID."""
    (
        adapter_snapshot,
        _trace_identity,
        connector_identity,
        _connector_snapshot,
        _selected,
        _selected_by_id,
        links,
    ) = _producer_lineage(
        universe=universe,
        run_id=run_id,
        route_cohort_id=route_cohort_id,
        candidate_source_generation=candidate_source_generation,
        route_universe_sha256=route_universe_sha256,
        adapter_registry=adapter_registry,
        connector_key_registry=connector_key_registry,
        trace_profile_identity=trace_profile_identity,
        submission_connector_profile_identity=(
            submission_connector_profile_identity
        ),
    )
    if connector_identity["status"] != "available":
        raise RouteCostEvidenceError("policy request connector is missing")
    members = build_submission_policy_scope(
        universe=universe, adapter_registry=adapter_snapshot
    )
    if not members:
        raise RouteCostEvidenceError("policy request scope is empty")
    request = {
        "schema": ROUTE_COST_POLICY_REQUEST_SCHEMA,
        "run_id": links["run_id"],
        "route_cohort_id": links["route_cohort_id"],
        "candidate_source_generation": links["candidate_source_generation"],
        "route_universe_sha256": links["route_universe_sha256"],
        "selected_market_set_sha256": links["selected_market_set_sha256"],
        "adapter_registry_sha256": links["adapter_registry_sha256"],
        "connector_key_registry_sha256": links[
            "connector_key_registry_sha256"
        ],
        "trace_profile_generation": links["trace_profile_generation"],
        "submission_connector_profile_generation": links[
            "submission_connector_profile_generation"
        ],
        "connector_id": connector_identity["connector_id"],
        "members": members,
    }
    request_id = typed_sha256(
        b"route-cost-submission-policy-request/v1\n", request
    )
    return _canonical_copy({**request, "request_id": request_id})


def _validate_submission_policy_request(
    request: Mapping[str, Any], connector_key_registry: Mapping[str, Any]
) -> Dict[str, Any]:
    _exact_fields(request, POLICY_REQUEST_FIELDS, "policy request")
    if request.get("schema") != ROUTE_COST_POLICY_REQUEST_SCHEMA:
        raise RouteCostEvidenceError("policy request schema is invalid")
    _required_text(request.get("run_id"), "policy request run ID", _SAFE_RUN_ID)
    if _COHORT_ID.fullmatch(str(request.get("route_cohort_id", ""))) is None:
        raise RouteCostEvidenceError("policy request cohort ID is invalid")
    for field in (
        "candidate_source_generation", "route_universe_sha256",
        "selected_market_set_sha256", "adapter_registry_sha256",
        "connector_key_registry_sha256", "trace_profile_generation",
        "submission_connector_profile_generation",
    ):
        _sha256(request.get(field), "policy request " + field)
    registry = validate_connector_key_registry(connector_key_registry)
    if physical_sha256(registry) != request["connector_key_registry_sha256"]:
        raise RouteCostEvidenceError("policy request key registry differs")
    _required_text(request.get("connector_id"), "policy request connector", _CONNECTOR_ID)
    members = request.get("members")
    if not isinstance(members, list) or not 0 < len(members) <= MAX_BINDINGS:
        raise RouteCostEvidenceError("policy request members are invalid")
    identities = []
    for member in members:
        _exact_fields(member, POLICY_REQUEST_MEMBER_FIELDS, "policy request member")
        route_id = member.get("route_id")
        if not isinstance(route_id, str) or not route_id:
            raise RouteCostEvidenceError("policy request route ID is invalid")
        notional = member.get("requested_notional_usd")
        if notional not in REQUESTED_NOTIONALS_USD:
            raise RouteCostEvidenceError("policy request notional is invalid")
        identities.append((route_id, notional))
    if identities != sorted(
        set(identities), key=lambda item: (item[0], Decimal(item[1]))
    ):
        raise RouteCostEvidenceError("policy request members are not canonical")
    unhashed = {field: request[field] for field in POLICY_REQUEST_FIELDS if field != "request_id"}
    if request.get("request_id") != typed_sha256(
        b"route-cost-submission-policy-request/v1\n", unhashed
    ):
        raise RouteCostEvidenceError("policy request ID differs")
    return _canonical_copy(request)


def validate_captured_submission_policy_response(
    value: Mapping[str, Any],
    *,
    request: Mapping[str, Any],
    connector_key_registry: Mapping[str, Any],
) -> Dict[str, Any]:
    """Validate captured response structure without I/O or signature blessing."""
    normalized_request = _validate_submission_policy_request(
        request, connector_key_registry
    )
    links = {
        field: normalized_request[field]
        for field in (
            "run_id", "route_cohort_id", "candidate_source_generation",
            "route_universe_sha256", "selected_market_set_sha256",
            "adapter_registry_sha256", "connector_key_registry_sha256",
            "trace_profile_generation",
            "submission_connector_profile_generation",
        )
    }
    snapshot = _canonical_copy(value)
    _exact_fields(snapshot, POLICY_SNAPSHOT_FIELDS, "policy snapshot")
    if snapshot.get("schema") != ROUTE_COST_POLICY_SNAPSHOT_SCHEMA:
        raise RouteCostEvidenceError("policy snapshot schema is invalid")
    response_members = snapshot.get("members")
    if not isinstance(response_members, list):
        raise RouteCostEvidenceError("policy response members are invalid")
    expected_scope = [
        (member["route_id"], member["requested_notional_usd"])
        for member in normalized_request["members"]
    ]
    captured_scope = [
        (member.get("route_id"), member.get("requested_notional_usd"))
        if isinstance(member, Mapping) else (None, None)
        for member in response_members
    ]
    if captured_scope != expected_scope:
        raise RouteCostEvidenceError("policy response scope differs")
    members = _validate_policy_snapshot(
        snapshot,
        links,
        False,
        connector_registry=validate_connector_key_registry(
            connector_key_registry
        ),
        permit_authenticated=True,
        # This is deliberately structural only. The publication boundary must
        # still invoke the fixed SSHSIG verifier before accepting evidence.
        verified_signature=True,
    )
    if snapshot.get("status") != "authenticated":
        raise RouteCostEvidenceError("captured policy response is not authenticated")
    if snapshot.get("connector_id") != normalized_request["connector_id"]:
        raise RouteCostEvidenceError("policy response connector differs")
    if sorted(members, key=lambda item: (item[0], Decimal(item[1]))) != expected_scope:
        raise RouteCostEvidenceError("policy response scope differs")
    return snapshot


class _NativeJSONPreflightState:
    def __init__(self, *, node_limit: int, scalar_limit: int) -> None:
        self.node_limit = node_limit
        self.scalar_limit = scalar_limit
        self.nodes = 0
        self.scalars = 0

    def add_node(self, label: str) -> None:
        self.nodes += 1
        if self.nodes > self.node_limit:
            raise RouteCostEvidenceError(
                "{} JSON exceeds its node limit".format(label)
            )

    def add_scalar(self, amount: int, label: str) -> None:
        self.scalars += amount
        if self.scalars > self.scalar_limit:
            raise RouteCostEvidenceError(
                "{} JSON exceeds its scalar limit".format(label)
            )


def _preflight_native_json_bytes(
    data: bytes,
    *,
    label: str,
    maximum_depth: int = MAX_NATIVE_PRICE_JSON_DEPTH,
    node_limit: int = MAX_NATIVE_PRICE_JSON_NODES,
    ordinary_string_limit: int = MAX_NATIVE_PRICE_JSON_STRING_BYTES,
    scalar_limit: int = MAX_NATIVE_PRICE_JSON_SCALAR_BYTES,
    number_token_limit: int = MAX_NATIVE_PRICE_JSON_NUMBER_TOKEN_BYTES,
) -> None:
    """Lexically bound native JSON before ``json.loads`` materializes it."""
    if not isinstance(data, bytes) or not data:
        raise RouteCostEvidenceError("{} raw response is invalid".format(label))
    state = _NativeJSONPreflightState(
        node_limit=node_limit, scalar_limit=scalar_limit
    )
    whitespace = {0x09, 0x0A, 0x0D, 0x20}

    def invalid() -> RouteCostEvidenceError:
        return RouteCostEvidenceError("{} raw response is invalid".format(label))

    def resource(kind: str) -> RouteCostEvidenceError:
        return RouteCostEvidenceError(
            "{} JSON exceeds its {} limit".format(label, kind)
        )

    def skip(index: int) -> int:
        while index < len(data) and data[index] in whitespace:
            index += 1
        return index

    def scan_string(index: int, *, maximum: int) -> int:
        if index >= len(data) or data[index] != 0x22:
            raise invalid()
        index += 1
        decoded_bytes = 0

        def add(width: int) -> None:
            nonlocal decoded_bytes
            decoded_bytes += width
            if decoded_bytes > maximum:
                raise resource("string")
            state.add_scalar(width, label)

        while index < len(data):
            byte = data[index]
            if byte == 0x22:
                return index + 1
            if byte < 0x20:
                raise invalid()
            if byte == 0x5C:
                if index + 1 >= len(data):
                    raise invalid()
                escaped = data[index + 1]
                if escaped in b'"\\/bfnrt':
                    add(1)
                    index += 2
                    continue
                if escaped != 0x75 or index + 6 > len(data):
                    raise invalid()
                token = data[index + 2:index + 6]
                if re.fullmatch(b"[0-9A-Fa-f]{4}", token) is None:
                    raise invalid()
                unit = int(token, 16)
                if 0xD800 <= unit <= 0xDBFF:
                    if (
                        index + 12 > len(data)
                        or data[index + 6:index + 8] != b"\\u"
                    ):
                        raise invalid()
                    low_token = data[index + 8:index + 12]
                    if re.fullmatch(b"[0-9A-Fa-f]{4}", low_token) is None:
                        raise invalid()
                    low = int(low_token, 16)
                    if not 0xDC00 <= low <= 0xDFFF:
                        raise invalid()
                    add(4)
                    index += 12
                    continue
                if 0xDC00 <= unit <= 0xDFFF:
                    raise invalid()
                add(1 if unit <= 0x7F else 2 if unit <= 0x7FF else 3)
                index += 6
                continue
            if byte < 0x80:
                add(1)
                index += 1
                continue
            width = (
                2 if 0xC2 <= byte <= 0xDF
                else 3 if 0xE0 <= byte <= 0xEF
                else 4 if 0xF0 <= byte <= 0xF4
                else 0
            )
            if width == 0 or index + width > len(data):
                raise invalid()
            try:
                data[index:index + width].decode("utf-8")
            except UnicodeDecodeError:
                raise invalid() from None
            add(width)
            index += width
        raise invalid()

    def parse_number(index: int) -> int:
        start = index
        if data[index] == 0x2D:
            index += 1
        if index >= len(data):
            raise invalid()
        if data[index] == 0x30:
            index += 1
        elif 0x31 <= data[index] <= 0x39:
            index += 1
            while index < len(data) and 0x30 <= data[index] <= 0x39:
                index += 1
        else:
            raise invalid()
        if index < len(data) and data[index] == 0x2E:
            index += 1
            fraction_start = index
            while index < len(data) and 0x30 <= data[index] <= 0x39:
                index += 1
            if index == fraction_start:
                raise invalid()
        if index < len(data) and data[index] in {0x45, 0x65}:
            index += 1
            if index < len(data) and data[index] in {0x2B, 0x2D}:
                index += 1
            exponent_start = index
            while index < len(data) and 0x30 <= data[index] <= 0x39:
                index += 1
            if index == exponent_start:
                raise invalid()
        width = index - start
        if width > number_token_limit:
            raise resource("number token")
        state.add_scalar(width, label)
        return index

    def parse_value(index: int, depth: int) -> int:
        index = skip(index)
        if index >= len(data):
            raise invalid()
        state.add_node(label)
        byte = data[index]
        if byte == 0x22:
            return scan_string(index, maximum=ordinary_string_limit)
        if byte == 0x7B:
            if depth + 1 > maximum_depth:
                raise resource("depth")
            index = skip(index + 1)
            if index < len(data) and data[index] == 0x7D:
                return index + 1
            while True:
                index = scan_string(index, maximum=ordinary_string_limit)
                index = skip(index)
                if index >= len(data) or data[index] != 0x3A:
                    raise invalid()
                index = parse_value(index + 1, depth + 1)
                index = skip(index)
                if index < len(data) and data[index] == 0x7D:
                    return index + 1
                if index >= len(data) or data[index] != 0x2C:
                    raise invalid()
                index = skip(index + 1)
        if byte == 0x5B:
            if depth + 1 > maximum_depth:
                raise resource("depth")
            index = skip(index + 1)
            if index < len(data) and data[index] == 0x5D:
                return index + 1
            while True:
                index = parse_value(index, depth + 1)
                index = skip(index)
                if index < len(data) and data[index] == 0x5D:
                    return index + 1
                if index >= len(data) or data[index] != 0x2C:
                    raise invalid()
                index = skip(index + 1)
        for literal in (b"true", b"false", b"null"):
            if data.startswith(literal, index):
                state.add_scalar(len(literal), label)
                return index + len(literal)
        if byte == 0x2D or 0x30 <= byte <= 0x39:
            return parse_number(index)
        raise invalid()

    final = skip(parse_value(0, 0))
    if final != len(data):
        raise invalid()


def _strict_json_object(raw: bytes, label: str) -> Mapping[str, Any]:
    if not isinstance(raw, bytes) or not raw:
        raise RouteCostEvidenceError("{} raw response is invalid".format(label))

    _preflight_native_json_bytes(raw, label=label)

    def pairs(items: Sequence[Tuple[str, Any]]) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        for key, item in items:
            if key in result:
                raise RouteCostEvidenceError(
                    "{} JSON key is duplicated".format(label)
                )
            result[key] = item
        return result

    try:
        decoded = json.loads(
            raw.decode("utf-8"),
            object_pairs_hook=pairs,
            parse_constant=lambda token: (_ for _ in ()).throw(
                ValueError("non-finite JSON number: " + token)
            ),
        )
    except RouteCostEvidenceError:
        raise
    except (UnicodeDecodeError, json.JSONDecodeError, ValueError, RecursionError) as error:
        raise RouteCostEvidenceError(
            "{} raw response is invalid".format(label)
        ) from error
    if not isinstance(decoded, Mapping):
        raise RouteCostEvidenceError("{} raw response is invalid".format(label))
    return decoded


def _native_price_levels(rows: Any, label: str) -> List[Tuple[Decimal, Decimal]]:
    if not isinstance(rows, list) or not rows:
        raise RouteCostEvidenceError("{} levels are invalid".format(label))
    result = []
    for row in rows:
        if not isinstance(row, list) or len(row) != 2:
            raise RouteCostEvidenceError("{} level is invalid".format(label))
        price_text = _bounded_native_fixed_decimal(row[0], label + " price")
        quantity_text = _bounded_native_fixed_decimal(
            row[1], label + " quantity"
        )
        price = Decimal(price_text)
        quantity = Decimal(quantity_text)
        result.append((price, quantity))
    return result


def _bounded_native_fixed_decimal(value: Any, label: str) -> str:
    if (
        not isinstance(value, str)
        or len(value) > MAX_NATIVE_PRICE_JSON_NUMBER_TOKEN_BYTES
        or re.fullmatch(
            r"(?:0|[1-9][0-9]*)(?:\.[0-9]+)?\Z", value, flags=re.ASCII
        ) is None
    ):
        raise RouteCostEvidenceError("{} is invalid".format(label))
    places = len(value.rsplit(".", 1)[1]) if "." in value else 0
    if places > 255:
        raise RouteCostEvidenceError("{} precision is invalid".format(label))
    try:
        number = Decimal(value)
    except InvalidOperation as error:  # pragma: no cover - fixed grammar
        raise RouteCostEvidenceError("{} is invalid".format(label)) from error
    if number <= 0:
        raise RouteCostEvidenceError("{} is invalid".format(label))
    return value


def _native_price_book_projection(
    raw: bytes, observed_at: str
) -> Dict[str, Any]:
    raw_book = _strict_json_object(raw, "native price book")
    if set(raw_book) != {"asks", "bids", "lastUpdateId"}:
        raise RouteCostEvidenceError("native price Binance book schema is invalid")
    update_id = raw_book.get("lastUpdateId")
    if isinstance(update_id, bool) or not isinstance(update_id, int) or update_id < 0:
        raise RouteCostEvidenceError("native price Binance book identity is invalid")
    asks = _native_price_levels(raw_book.get("asks"), "native ask")
    bids = _native_price_levels(raw_book.get("bids"), "native bid")
    best_ask = min(asks, key=lambda row: row[0])
    best_bid = max(bids, key=lambda row: row[0])
    if best_bid[0] >= best_ask[0]:
        raise RouteCostEvidenceError("native price book is crossed")
    raw_sha = hashlib.sha256(raw).hexdigest()
    return {
        "schema": "route_cost_native_price_book/v1",
        "market_id": "cex:binance:ETH/USDT",
        "adapter_id": "binance_public_spot_depth/v1",
        "best_ask_price": _format_decimal(best_ask[0]),
        "best_ask_quantity": _format_decimal(best_ask[1]),
        "observed_at": _timestamp(observed_at, "native book observed_at"),
        "raw_response_sha256": raw_sha,
    }


def _native_price_rules_projection(
    raw: bytes, observed_at: str
) -> Dict[str, Any]:
    decoded = _strict_json_object(raw, "native price market rules")
    if (
        not isinstance(decoded.get("symbols"), list)
        or len(decoded["symbols"]) != 1
        or not isinstance(decoded["symbols"][0], Mapping)
        or decoded["symbols"][0].get("symbol") != "ETHUSDT"
    ):
        raise RouteCostEvidenceError(
            "native price market-rules closed identity is invalid"
        )
    try:
        projection = binance_market_rules_projection(
            decoded,
            base_asset="ETH",
            quote_asset="USDT",
            source_instrument="ETHUSDT",
        )
    except ValueError as error:
        raise RouteCostEvidenceError(
            "native price market-rules identity is invalid"
        ) from error
    if projection["min_quantity"] == "0" or projection["min_notional"] == "0":
        raise RouteCostEvidenceError("native price market-rules minimum is invalid")
    return {
        "schema": "route_cost_native_price_market_rules/v1",
        "market_id": "cex:binance:ETH/USDT",
        "price_tick": projection["price_tick"],
        "quantity_step": projection["quantity_step"],
        "min_quantity": projection["min_quantity"],
        "min_notional": projection["min_notional"],
        "observed_at": _timestamp(
            observed_at, "native market rules observed_at"
        ),
        "source_record_sha256": hashlib.sha256(raw).hexdigest(),
    }


def _native_price_usd_conversion_projection(
    *, book_observed_at: str, rules_observed_at: str
) -> Dict[str, Any]:
    observed = max(
        (
            _timestamp_value(book_observed_at, "native book observed_at"),
            book_observed_at,
        ),
        (
            _timestamp_value(rules_observed_at, "native rules observed_at"),
            rules_observed_at,
        ),
    )[1]
    earliest = min(
        _timestamp_value(book_observed_at, "native book observed_at"),
        _timestamp_value(rules_observed_at, "native rules observed_at"),
    )
    validity = earliest + Decimal(60)
    # Source timestamps are whole-second evidence in this initial sealed adapter.
    valid_until = _timestamp_from_epoch_seconds(validity)
    projection = {
        "schema": "route_cost_native_price_usd_conversion/v1",
        "quote_asset": "USDT",
        "usd_asset": "USD",
        "rate": "1",
        "observed_at": observed,
        "valid_until": valid_until,
    }
    projection["source_record_sha256"] = typed_sha256(
        b"route-cost-native-price-usd-conversion-source/v1\n",
        projection,
    )
    return projection


def _validate_native_best_ask_rules(
    book: Mapping[str, Any], rules: Mapping[str, Any]
) -> None:
    price = Decimal(book["best_ask_price"])
    quantity = Decimal(book["best_ask_quantity"])
    tick = Decimal(rules["price_tick"])
    step = Decimal(rules["quantity_step"])
    minimum_quantity = Decimal(rules["min_quantity"])
    minimum_notional = Decimal(rules["min_notional"])
    if price % tick != 0:
        raise RouteCostEvidenceError("native best ask does not obey price tick")
    if quantity % step != 0:
        raise RouteCostEvidenceError("native best ask does not obey quantity step")
    if quantity < minimum_quantity:
        raise RouteCostEvidenceError("native best ask is below minimum quantity")
    if price * quantity < minimum_notional:
        raise RouteCostEvidenceError("native best ask is below minimum notional")


def _timestamp_from_epoch_seconds(value: Decimal) -> str:
    # Convert exact Decimal epoch seconds without float rounding. Native
    # receipts admit at most microsecond precision, matching RFC3339 producers.
    from datetime import datetime, timezone

    try:
        whole = int(value // Decimal(1))
        fraction = value - Decimal(whole)
        microseconds = fraction * Decimal(1_000_000)
        if microseconds != microseconds.to_integral_value():
            raise RouteCostEvidenceError(
                "native source timestamp precision is invalid"
            )
        parsed = datetime.fromtimestamp(whole, tz=timezone.utc)
        base = parsed.strftime("%Y-%m-%dT%H:%M:%S")
        microsecond_value = int(microseconds)
        return (
            base + "Z" if microsecond_value == 0
            else base + ".{:06d}Z".format(microsecond_value)
        )
    except RouteCostEvidenceError:
        raise
    except (OverflowError, OSError, ValueError, InvalidOperation) as error:
        raise RouteCostEvidenceError("native source timestamp is invalid") from error


def _native_price_request_receipt(
    *,
    role: str,
    captured_at: str,
    raw_sha256: str,
    projection: Mapping[str, Any],
) -> Dict[str, Any]:
    if role == "book":
        endpoint_id = "binance-public-spot-depth-v1"
        path = "/api/v3/depth"
        query = "symbol=ETHUSDT&limit=100"
        projection_domain = b"route-cost-native-price-book-projection/v1\n"
    elif role == "market_rules":
        endpoint_id = "binance-public-spot-exchange-info-v1"
        path = "/api/v3/exchangeInfo"
        query = "symbol=ETHUSDT"
        projection_domain = (
            b"route-cost-native-price-market-rules-projection/v1\n"
        )
    else:  # pragma: no cover - closed internal caller set
        raise RouteCostEvidenceError("native request receipt role is invalid")
    return {
        "schema": "route_cost_native_price_request_receipt/v1",
        "request_role": role,
        "request_method": "GET",
        "source_endpoint_id": endpoint_id,
        "request_path": path,
        "request_query": query,
        "captured_at": _timestamp(captured_at, "native request captured_at"),
        "raw_response_sha256": raw_sha256,
        "projection_sha256": typed_sha256(projection_domain, projection),
    }


def _validate_native_price_request_receipt(
    value: Any,
    *,
    expected_role: str,
    expected_captured_at: str,
    raw_sha256: str,
    projection: Mapping[str, Any],
) -> str:
    _exact_fields(
        value, NATIVE_PRICE_REQUEST_RECEIPT_FIELDS,
        "native price request receipt",
    )
    expected = _native_price_request_receipt(
        role=expected_role,
        captured_at=expected_captured_at,
        raw_sha256=raw_sha256,
        projection=projection,
    )
    if value != expected:
        raise RouteCostEvidenceError("native price request receipt differs")
    return typed_sha256(
        b"route-cost-native-price-request-receipt/v1\n", value
    )


def build_native_price_evidence_from_captured(
    *,
    run_id: str,
    route_cohort_id: str,
    candidate_source_generation: str,
    book_raw_response: bytes,
    book_observed_at: str,
    market_rules_raw_response: bytes,
    market_rules_observed_at: str,
) -> Dict[str, Any]:
    """Build sealed ETH/USDT evidence from two already-captured raw bodies."""
    if not isinstance(book_raw_response, bytes) or not book_raw_response:
        raise RouteCostEvidenceError("native price book raw response is invalid")
    if len(book_raw_response) > MAX_NATIVE_PRICE_RAW_BYTES:
        raise RouteCostEvidenceError("native price book exceeds its byte limit")
    if (
        not isinstance(market_rules_raw_response, bytes)
        or not market_rules_raw_response
    ):
        raise RouteCostEvidenceError("native price rules raw response is invalid")
    if len(market_rules_raw_response) > MAX_NATIVE_PRICE_MARKET_RULES_RAW_BYTES:
        raise RouteCostEvidenceError("native price rules exceeds its byte limit")
    links = {
        "run_id": _required_text(run_id, "native price run_id", _SAFE_RUN_ID),
        "route_cohort_id": route_cohort_id,
        "candidate_source_generation": _sha256(
            candidate_source_generation, "native price source generation"
        ),
    }
    if _COHORT_ID.fullmatch(route_cohort_id or "") is None:
        raise RouteCostEvidenceError("native price route cohort ID is invalid")
    book = _native_price_book_projection(book_raw_response, book_observed_at)
    rules = _native_price_rules_projection(
        market_rules_raw_response, market_rules_observed_at
    )
    _validate_native_best_ask_rules(book, rules)
    conversion = _native_price_usd_conversion_projection(
        book_observed_at=book["observed_at"],
        rules_observed_at=rules["observed_at"],
    )
    book_raw_sha = hashlib.sha256(book_raw_response).hexdigest()
    rules_raw_sha = hashlib.sha256(market_rules_raw_response).hexdigest()
    book_receipt = _native_price_request_receipt(
        role="book",
        captured_at=book["observed_at"],
        raw_sha256=book_raw_sha,
        projection=book,
    )
    rules_receipt = _native_price_request_receipt(
        role="market_rules",
        captured_at=rules["observed_at"],
        raw_sha256=rules_raw_sha,
        projection=rules,
    )
    observed_at = max(
        (book["observed_at"], rules["observed_at"], conversion["observed_at"]),
        key=lambda item: _timestamp_value(item, "native evidence observed_at"),
    )
    valid_until = conversion["valid_until"]
    source_projection = {
        "book": book,
        "book_request_receipt": book_receipt,
        "market_rules": rules,
        "market_rules_request_receipt": rules_receipt,
        "usd_conversion": conversion,
    }
    source_record_sha = typed_sha256(
        b"route-cost-native-price-source/v1\n", source_projection
    )
    capture_projection = {
        "schema": "route_cost_native_price_capture_binding/v1",
        **links,
        "source_market_id": "cex:binance:ETH/USDT",
        "source_adapter_id": "binance_public_spot_depth/v1",
        "book_request_receipt_sha256": typed_sha256(
            b"route-cost-native-price-request-receipt/v1\n", book_receipt
        ),
        "market_rules_request_receipt_sha256": typed_sha256(
            b"route-cost-native-price-request-receipt/v1\n", rules_receipt
        ),
        "source_record_sha256": source_record_sha,
    }
    value = {
        "schema": "route_cost_native_price_evidence/v1",
        **links,
        "source_market_id": "cex:binance:ETH/USDT",
        "source_adapter_id": "binance_public_spot_depth/v1",
        "source_endpoint_id": "binance-public-spot-depth-v1",
        "book_projection": book,
        "market_rules_projection": rules,
        "usd_conversion_projection": conversion,
        "book_request_receipt": book_receipt,
        "market_rules_request_receipt": rules_receipt,
        "raw_response_base64": base64.b64encode(book_raw_response).decode("ascii"),
        "raw_response_sha256": book_raw_sha,
        "market_rules_raw_response_base64": base64.b64encode(
            market_rules_raw_response
        ).decode("ascii"),
        "market_rules_raw_response_sha256": rules_raw_sha,
        "observed_at": observed_at,
        "valid_until": valid_until,
        "source_record_sha256": source_record_sha,
        "capture_binding_sha256": typed_sha256(
            b"route-cost-native-price-capture-binding/v1\n",
            capture_projection,
        ),
    }
    _validate_native_price_evidence(value, links)
    return value


def _validate_native_price_evidence(value: Any, links: Mapping[str, Any]) -> str:
    _exact_fields(value, NATIVE_PRICE_EVIDENCE_FIELDS, "native price evidence")
    if value.get("schema") != "route_cost_native_price_evidence/v1":
        raise RouteCostEvidenceError("native price evidence schema is invalid")
    for field in ("run_id", "route_cohort_id", "candidate_source_generation"):
        if value.get(field) != links[field]:
            raise RouteCostEvidenceError("native price lineage differs")
    if (
        value.get("source_market_id") != "cex:binance:ETH/USDT"
        or value.get("source_adapter_id") != "binance_public_spot_depth/v1"
        or value.get("source_endpoint_id") != "binance-public-spot-depth-v1"
    ):
        raise RouteCostEvidenceError("native price source identity differs")
    book = value.get("book_projection")
    rules = value.get("market_rules_projection")
    conversion = value.get("usd_conversion_projection")
    _exact_fields(book, NATIVE_PRICE_BOOK_FIELDS, "native price book")
    _exact_fields(rules, NATIVE_PRICE_MARKET_RULES_FIELDS, "native market rules")
    _exact_fields(conversion, NATIVE_PRICE_USD_CONVERSION_FIELDS, "native USD conversion")
    if book.get("schema") != "route_cost_native_price_book/v1" or rules.get("schema") != "route_cost_native_price_market_rules/v1" or conversion.get("schema") != "route_cost_native_price_usd_conversion/v1":
        raise RouteCostEvidenceError("native price projection schema is invalid")
    if (
        book.get("market_id") != value.get("source_market_id")
        or book.get("adapter_id") != value.get("source_adapter_id")
        or rules.get("market_id") != value.get("source_market_id")
        or conversion.get("quote_asset") != "USDT"
        or conversion.get("usd_asset") != "USD"
    ):
        raise RouteCostEvidenceError("native price projection identity differs")
    for field in ("best_ask_price", "best_ask_quantity"):
        _decimal_text(book.get(field), "native book " + field, positive=True)
    for field in ("price_tick", "quantity_step", "min_quantity", "min_notional"):
        _decimal_text(rules.get(field), "native rules " + field, positive=True)
    _decimal_text(conversion.get("rate"), "native conversion rate", positive=True)
    book_observed = _timestamp_value(book.get("observed_at"), "native book observed_at")
    rules_observed = _timestamp_value(rules.get("observed_at"), "native rules observed_at")
    conversion_observed = _timestamp_value(
        conversion.get("observed_at"), "native conversion observed_at"
    )
    conversion_valid = _timestamp_value(
        conversion.get("valid_until"), "native conversion valid_until"
    )
    if conversion_observed > conversion_valid:
        raise RouteCostEvidenceError("native conversion validity is reversed")
    _sha256(rules.get("source_record_sha256"), "native rules source hash")
    _sha256(conversion.get("source_record_sha256"), "native conversion source hash")
    raw_text = value.get("raw_response_base64")
    if not isinstance(raw_text, str):
        raise RouteCostEvidenceError("native price raw response is invalid")
    try:
        raw = base64.b64decode(raw_text, validate=True)
    except (ValueError, binascii.Error) as error:
        raise RouteCostEvidenceError("native price raw base64 is invalid") from error
    if base64.b64encode(raw).decode("ascii") != raw_text or len(raw) > MAX_NATIVE_PRICE_RAW_BYTES:
        raise RouteCostEvidenceError("native price raw response is noncanonical/oversized")
    raw_sha = hashlib.sha256(raw).hexdigest()
    if value.get("raw_response_sha256") != raw_sha or book.get("raw_response_sha256") != raw_sha:
        raise RouteCostEvidenceError("native price raw hash differs")
    replayed_book = _native_price_book_projection(raw, book.get("observed_at"))
    if replayed_book != book:
        raise RouteCostEvidenceError("native price book projection differs")
    rules_text = value.get("market_rules_raw_response_base64")
    if not isinstance(rules_text, str):
        raise RouteCostEvidenceError("native price rules raw response is invalid")
    try:
        rules_raw = base64.b64decode(rules_text, validate=True)
    except (ValueError, binascii.Error) as error:
        raise RouteCostEvidenceError("native price rules raw base64 is invalid") from error
    if (
        base64.b64encode(rules_raw).decode("ascii") != rules_text
        or len(rules_raw) > MAX_NATIVE_PRICE_MARKET_RULES_RAW_BYTES
    ):
        raise RouteCostEvidenceError(
            "native price rules raw response is noncanonical/oversized"
        )
    rules_raw_sha = hashlib.sha256(rules_raw).hexdigest()
    if value.get("market_rules_raw_response_sha256") != rules_raw_sha:
        raise RouteCostEvidenceError("native price rules raw hash differs")
    replayed_rules = _native_price_rules_projection(
        rules_raw, rules.get("observed_at")
    )
    if replayed_rules != rules:
        raise RouteCostEvidenceError("native price market-rules projection differs")
    _validate_native_best_ask_rules(book, rules)
    replayed_conversion = _native_price_usd_conversion_projection(
        book_observed_at=book.get("observed_at"),
        rules_observed_at=rules.get("observed_at"),
    )
    if replayed_conversion != conversion:
        raise RouteCostEvidenceError("native price USD conversion projection differs")
    book_receipt = value.get("book_request_receipt")
    rules_receipt = value.get("market_rules_request_receipt")
    book_receipt_sha = _validate_native_price_request_receipt(
        book_receipt,
        expected_role="book",
        expected_captured_at=book.get("observed_at"),
        raw_sha256=raw_sha,
        projection=book,
    )
    rules_receipt_sha = _validate_native_price_request_receipt(
        rules_receipt,
        expected_role="market_rules",
        expected_captured_at=rules.get("observed_at"),
        raw_sha256=rules_raw_sha,
        projection=rules,
    )
    source_projection = {
        "book": book,
        "book_request_receipt": book_receipt,
        "market_rules": rules,
        "market_rules_request_receipt": rules_receipt,
        "usd_conversion": conversion,
    }
    source_sha = typed_sha256(b"route-cost-native-price-source/v1\n", source_projection)
    if value.get("source_record_sha256") != source_sha:
        raise RouteCostEvidenceError("native price source hash differs")
    capture_projection = {
        "schema": "route_cost_native_price_capture_binding/v1",
        "run_id": value.get("run_id"),
        "route_cohort_id": value.get("route_cohort_id"),
        "candidate_source_generation": value.get(
            "candidate_source_generation"
        ),
        "source_market_id": value.get("source_market_id"),
        "source_adapter_id": value.get("source_adapter_id"),
        "book_request_receipt_sha256": book_receipt_sha,
        "market_rules_request_receipt_sha256": rules_receipt_sha,
        "source_record_sha256": source_sha,
    }
    capture_sha = typed_sha256(
        b"route-cost-native-price-capture-binding/v1\n",
        capture_projection,
    )
    if value.get("capture_binding_sha256") != capture_sha:
        raise RouteCostEvidenceError("native price capture binding differs")
    observed = _timestamp_value(value.get("observed_at"), "native evidence observed_at")
    valid = _timestamp_value(value.get("valid_until"), "native evidence valid_until")
    if (
        observed > valid
        or observed != max(book_observed, rules_observed, conversion_observed)
        or valid != conversion_valid
    ):
        raise RouteCostEvidenceError("native evidence validity is reversed")
    if len(canonical_json_bytes(value)) > MAX_NATIVE_PRICE_EVIDENCE_BYTES:
        raise RouteCostEvidenceError("native price evidence exceeds its byte limit")
    return typed_sha256(b"route-cost-native-price-evidence/v1\n", value)


def build_route_cost_evidence_manifest_from_captured(
    *,
    universe: Mapping[str, Any],
    run_id: str,
    route_cohort_id: str,
    phase: str,
    candidate_source_generation: str,
    route_universe_sha256: str,
    evaluated_at: str,
    adapter_registry: Mapping[str, Any],
    connector_key_registry: Mapping[str, Any],
    trace_profile_identity: Mapping[str, Any],
    submission_connector_profile_identity: Mapping[str, Any],
    native_price_evidence: Optional[Mapping[str, Any]],
    chain_evidence: Sequence[Mapping[str, Any]],
    market_evidence: Sequence[Mapping[str, Any]],
    transcripts: Sequence[Mapping[str, Any]],
    submission_policy_snapshot: Mapping[str, Any],
) -> Dict[str, Any]:
    """Assemble captured projections into one deterministic full-v1 graph.

    This is deliberately a pure composition boundary.  It accepts no profile
    path, URL, credential, clock, verifier, network client, or filesystem
    handle.  It recomputes every outer set/count and every unsigned binding;
    the publication validator remains the only authority that verifies the
    captured SSHSIG and retained typed pool-state bytes.
    """
    _required_text(run_id, "run ID", _SAFE_RUN_ID)
    if _COHORT_ID.fullmatch(route_cohort_id or "") is None:
        raise RouteCostEvidenceError("route cohort ID is invalid")
    if phase not in {"canary", "full"}:
        raise RouteCostEvidenceError("phase is invalid")
    _sha256(candidate_source_generation, "candidate source generation")
    _sha256(route_universe_sha256, "route universe hash")
    _timestamp(evaluated_at, "evaluated_at")
    if physical_sha256(universe) != route_universe_sha256:
        raise RouteCostEvidenceError("route universe physical hash differs")
    if universe.get("candidate_source_generation") != candidate_source_generation:
        raise RouteCostEvidenceError("route universe generation differs")

    adapter_snapshot = validate_adapter_registry(adapter_registry)
    connector_snapshot = validate_connector_key_registry(
        connector_key_registry
    )
    adapter_sha = physical_sha256(adapter_snapshot)
    connector_sha = physical_sha256(connector_snapshot)
    trace_identity, trace_generation = _validate_profile_identity(
        trace_profile_identity, kind="trace"
    )
    connector_identity, connector_generation = _validate_profile_identity(
        submission_connector_profile_identity, kind="connector"
    )
    selected = build_selected_markets(universe, adapter_snapshot)
    selected_sha = selected_market_set_sha256(selected)
    selected_by_id = {row["market_id"]: row for row in selected}

    links = {
        "run_id": run_id,
        "route_cohort_id": route_cohort_id,
        "candidate_source_generation": candidate_source_generation,
        "route_universe_sha256": route_universe_sha256,
        "adapter_registry_sha256": adapter_sha,
        "selected_market_set_sha256": selected_sha,
        "connector_key_registry_sha256": connector_sha,
        "trace_profile_generation": trace_generation,
        "submission_connector_profile_generation": connector_generation,
    }

    chain_rows = [_canonical_copy(row) for row in chain_evidence]
    market_rows = [_canonical_copy(row) for row in market_evidence]
    transcript_rows = [_canonical_copy(row) for row in transcripts]
    chain_rows.sort(key=lambda row: row.get("chain_id", -1))
    market_rows.sort(key=lambda row: str(row.get("market_id", "")))
    direction_order = {"buy": 0, "sell": 1}
    try:
        transcript_rows.sort(key=lambda row: (
            str(row.get("market_id", "")),
            direction_order.get(str(row.get("direction")), 2),
            Decimal(str(row.get("requested_notional_usd"))),
        ))
    except (InvalidOperation, ValueError) as error:
        raise RouteCostEvidenceError("captured transcript identity is invalid") from error

    expected_transcript_scope = [
        (row["market_id"], direction, notional)
        for row in selected
        for direction in ("buy", "sell")
        for notional in REQUESTED_NOTIONALS_USD
    ]
    transcript_scope = [
        (
            row.get("market_id"),
            row.get("direction"),
            row.get("requested_notional_usd"),
        )
        for row in transcript_rows
    ]
    if transcript_scope != expected_transcript_scope:
        raise RouteCostEvidenceError("captured transcript denominator/sort differs")
    transcript_by_scope = {
        scope: row for scope, row in zip(transcript_scope, transcript_rows)
    }

    snapshot = _canonical_copy(submission_policy_snapshot)
    _exact_fields(snapshot, POLICY_SNAPSHOT_FIELDS, "policy snapshot")
    members = snapshot.get("members")
    if not isinstance(members, list):
        raise RouteCostEvidenceError("policy members are invalid")
    members = [_canonical_copy(member) for member in members]
    try:
        members.sort(key=lambda row: (
            str(row.get("route_id", "")),
            Decimal(str(row.get("requested_notional_usd"))),
        ))
    except (InvalidOperation, ValueError) as error:
        raise RouteCostEvidenceError("policy member identity is invalid") from error
    snapshot["members"] = members
    snapshot["member_count"] = len(members)
    snapshot["member_set_sha256"] = typed_sha256(
        b"route-cost-submission-policy-member-set/v1\n", members
    )
    if snapshot.get("status") == "authenticated":
        snapshot["attested_payload_sha256"] = typed_sha256(
            b"route-cost-submission-policy-attestation/v1\n",
            _policy_attestation(snapshot),
        )

    expected_binding_scope, route_sides = _expected_binding_scope(
        universe, selected_by_id
    )
    policy_members = _validate_policy_snapshot(
        snapshot,
        links,
        len(expected_binding_scope) == 0,
        connector_registry=connector_snapshot,
        route_sides=route_sides,
        permit_authenticated=True,
        # Assembly checks all deterministic signed structure.  Cryptographic
        # authentication is intentionally deferred to the sealed publication
        # entry point and cannot be injected here.
        verified_signature=True,
    )
    if sorted(policy_members, key=lambda item: (item[0], Decimal(item[1]))) != (
        expected_binding_scope
    ):
        raise RouteCostEvidenceError("policy member denominator differs")
    snapshot_connector = snapshot.get("connector_id")
    if connector_identity["status"] == "available":
        if snapshot_connector != connector_identity["connector_id"]:
            raise RouteCostEvidenceError(
                "policy snapshot connector differs from profile identity"
            )
    elif snapshot_connector is not None:
        raise RouteCostEvidenceError(
            "missing connector profile cannot name a connector"
        )

    routes = {
        row.get("route_id"): row
        for row in universe.get("routes", [])
        if isinstance(row, Mapping)
    }
    bindings: List[Dict[str, Any]] = []
    for route_id, notional in expected_binding_scope:
        route = routes.get(route_id)
        member = policy_members[(route_id, notional)]
        if route is None:
            raise RouteCostEvidenceError("binding route does not resolve")
        resolved: List[Mapping[str, Any]] = []
        transcript_hashes: Dict[str, Optional[str]] = {}
        for direction in ("buy", "sell"):
            market_id = route.get(direction + "_market_id")
            is_supported = (
                market_id in selected_by_id
                and selected_by_id[market_id]["structural_support_status"]
                == "supported"
            )
            if is_supported:
                transcript = transcript_by_scope.get(
                    (market_id, direction, notional)
                )
                if transcript is None:
                    raise RouteCostEvidenceError(
                        "binding transcript scenario does not resolve"
                    )
                resolved.append(transcript)
                transcript_hashes[direction] = typed_sha256(
                    b"route-cost-evidence-transcript/v1\n", transcript
                )
            else:
                transcript_hashes[direction] = None
        status, reason = _derived_binding_status_reason(
            route_sides=route_sides[route_id],
            transcripts=resolved,
            member=member,
            snapshot=snapshot,
            evaluated_at=evaluated_at,
        )
        bindings.append({
            "schema": ROUTE_COST_BINDING_SCHEMA,
            **links,
            "route_id": route_id,
            "requested_notional_usd": notional,
            "buy_transcript_sha256": transcript_hashes["buy"],
            "sell_transcript_sha256": transcript_hashes["sell"],
            "submission_policy_member_sha256": typed_sha256(
                b"route-cost-submission-policy-member/v1\n", member
            ),
            "evaluated_at": evaluated_at,
            "status": status,
            "reason_code": reason,
        })

    native = (
        None if native_price_evidence is None
        else _canonical_copy(native_price_evidence)
    )
    counts = {
        "transcript_observed": 0,
        "transcript_unavailable": 0,
        "transcript_failed": 0,
        "binding_observed": 0,
        "binding_unavailable": 0,
        "binding_failed": 0,
    }
    for prefix, rows in (("transcript", transcript_rows), ("binding", bindings)):
        actual = Counter(row.get("status") for row in rows)
        for status in ("observed", "unavailable", "failed"):
            counts["{}_{}".format(prefix, status)] = actual[status]

    manifest = {
        "schema": ROUTE_COST_EVIDENCE_SCHEMA,
        "run_id": run_id,
        "route_cohort_id": route_cohort_id,
        "phase": phase,
        "candidate_source_generation": candidate_source_generation,
        "route_universe_sha256": route_universe_sha256,
        "adapter_registry": adapter_snapshot,
        "adapter_registry_sha256": adapter_sha,
        "connector_key_registry": connector_snapshot,
        "connector_key_registry_sha256": connector_sha,
        "transcript_count": len(transcript_rows),
        "trace_profile_identity": trace_identity,
        "trace_profile_generation": trace_generation,
        "submission_connector_profile_identity": connector_identity,
        "submission_connector_profile_generation": connector_generation,
        "evaluated_at": evaluated_at,
        "selected_market_count": len(selected),
        "selected_markets": selected,
        "selected_market_set_sha256": selected_sha,
        "native_price_evidence": native,
        "native_price_evidence_sha256": (
            None if native is None else typed_sha256(
                b"route-cost-native-price-evidence/v1\n", native
            )
        ),
        "chain_evidence_count": len(chain_rows),
        "chain_evidence": chain_rows,
        "chain_evidence_set_sha256": typed_sha256(
            b"route-cost-chain-evidence-set/v1\n", chain_rows
        ),
        "market_evidence_count": len(market_rows),
        "market_evidence": market_rows,
        "market_evidence_set_sha256": typed_sha256(
            b"route-cost-market-evidence-set/v1\n", market_rows
        ),
        "transcripts": transcript_rows,
        "transcript_set_sha256": typed_sha256(
            b"route-cost-evidence-transcript-set/v1\n", transcript_rows
        ),
        "binding_count": len(bindings),
        "bindings": bindings,
        "binding_set_sha256": typed_sha256(
            b"route-cost-evidence-binding-set/v1\n", bindings
        ),
        "submission_policy_snapshot": snapshot,
        "submission_policy_snapshot_sha256": typed_sha256(
            b"route-cost-submission-policy-snapshot/v1\n", snapshot
        ),
        "counts": counts,
    }
    if len(canonical_json_bytes(manifest)) > MAX_ROUTE_COST_EVIDENCE_BYTES:
        raise RouteCostEvidenceError("route-cost sidecar exceeds its byte limit")
    return _canonical_copy(manifest)


def build_unavailable_route_cost_evidence_manifest(
    *,
    universe: Mapping[str, Any],
    run_id: str,
    route_cohort_id: str,
    phase: str,
    candidate_source_generation: str,
    route_universe_sha256: str,
    evaluated_at: str,
    reason_code: str = "strict_cost_adapter_unsupported",
) -> Dict[str, Any]:
    """Build the exact fail-closed full-v1 sidecar for an unsupported scope.

    This builder deliberately refuses supported selected markets.  A production
    caller cannot use it to turn an observation failure into structural
    unsupportedness or to fabricate observed strict evidence.
    """
    registry = load_route_cost_adapter_registry()
    key_registry = load_route_cost_connector_key_registry()
    selected_markets = build_selected_markets(universe, registry)
    if any(row["structural_support_status"] != "unsupported" for row in selected_markets):
        raise RouteCostEvidenceError("unavailable builder cannot cover supported markets")
    if reason_code != "strict_cost_adapter_unsupported":
        raise RouteCostEvidenceError("unavailable builder reason is invalid")
    _required_text(run_id, "run ID", _SAFE_RUN_ID)
    if _COHORT_ID.fullmatch(route_cohort_id or "") is None:
        raise RouteCostEvidenceError("route cohort ID is invalid")
    if phase not in {"canary", "full"}:
        raise RouteCostEvidenceError("phase is invalid")
    _sha256(candidate_source_generation, "candidate source generation")
    _sha256(route_universe_sha256, "route universe hash")
    _timestamp(evaluated_at, "evaluated_at")
    selected_sha = selected_market_set_sha256(selected_markets)
    adapter_sha = physical_sha256(registry)
    key_sha = physical_sha256(key_registry)
    trace_identity, trace_generation = trace_profile_identity(None)
    connector_identity, connector_generation = (
        submission_connector_profile_identity(None)
    )
    transcripts = build_terminal_transcript_inventory(
        universe=universe,
        run_id=run_id,
        route_cohort_id=route_cohort_id,
        candidate_source_generation=candidate_source_generation,
        route_universe_sha256=route_universe_sha256,
        adapter_registry=registry,
        trace_profile_identity=trace_identity,
        submission_connector_profile_identity=connector_identity,
        retained_typed_pool_state_members={},
        terminal_reason_by_market={},
    )
    snapshot = build_terminal_submission_policy_snapshot(
        universe=universe,
        run_id=run_id,
        route_cohort_id=route_cohort_id,
        candidate_source_generation=candidate_source_generation,
        route_universe_sha256=route_universe_sha256,
        adapter_registry=registry,
        connector_key_registry=key_registry,
        trace_profile_identity=trace_identity,
        submission_connector_profile_identity=connector_identity,
        reason_code="scope_empty",
    )
    manifest = {
        "schema": ROUTE_COST_EVIDENCE_SCHEMA,
        "run_id": run_id,
        "route_cohort_id": route_cohort_id,
        "phase": phase,
        "candidate_source_generation": candidate_source_generation,
        "route_universe_sha256": route_universe_sha256,
        "adapter_registry": registry,
        "adapter_registry_sha256": adapter_sha,
        "connector_key_registry": key_registry,
        "connector_key_registry_sha256": key_sha,
        "transcript_count": len(transcripts),
        "trace_profile_identity": trace_identity,
        "trace_profile_generation": trace_generation,
        "submission_connector_profile_identity": connector_identity,
        "submission_connector_profile_generation": connector_generation,
        "evaluated_at": evaluated_at,
        "selected_market_count": len(selected_markets),
        "selected_markets": selected_markets,
        "selected_market_set_sha256": selected_sha,
        "native_price_evidence": None,
        "native_price_evidence_sha256": None,
        "chain_evidence_count": 0,
        "chain_evidence": [],
        "chain_evidence_set_sha256": typed_sha256(
            b"route-cost-chain-evidence-set/v1\n", []
        ),
        "market_evidence_count": 0,
        "market_evidence": [],
        "market_evidence_set_sha256": typed_sha256(
            b"route-cost-market-evidence-set/v1\n", []
        ),
        "transcripts": transcripts,
        "transcript_set_sha256": typed_sha256(
            b"route-cost-evidence-transcript-set/v1\n", transcripts
        ),
        "binding_count": 0,
        "bindings": [],
        "binding_set_sha256": typed_sha256(
            b"route-cost-evidence-binding-set/v1\n", []
        ),
        "submission_policy_snapshot": snapshot,
        "submission_policy_snapshot_sha256": typed_sha256(
            b"route-cost-submission-policy-snapshot/v1\n", snapshot
        ),
        "counts": {
            "transcript_observed": 0,
            "transcript_unavailable": len(transcripts),
            "transcript_failed": 0,
            "binding_observed": 0,
            "binding_unavailable": 0,
            "binding_failed": 0,
        },
    }
    return validate_route_cost_evidence_manifest(
        manifest,
        universe=universe,
        expected_run_id=run_id,
        expected_route_cohort_id=route_cohort_id,
        expected_phase=phase,
        expected_candidate_source_generation=candidate_source_generation,
        expected_route_universe_sha256=route_universe_sha256,
    )


def build_trace_profile_missing_route_cost_evidence_manifest(
    *,
    universe: Mapping[str, Any],
    run_id: str,
    route_cohort_id: str,
    phase: str,
    candidate_source_generation: str,
    route_universe_sha256: str,
    evaluated_at: str,
    adapter_registry: Mapping[str, Any],
    connector_key_registry: Mapping[str, Any],
    retained_typed_pool_state_members: Mapping[
        str, Mapping[str, Any]
    ],
) -> Dict[str, Any]:
    """Build supported, core-retained terminal rows for a missing trace profile.

    No trace profile means no RPC call: chain/market shared inventories remain
    empty, while each supported market retains its validated typed pool-state
    identity and all ten expected terminal transcript rows.
    """
    adapter_snapshot = validate_adapter_registry(adapter_registry)
    connector_snapshot = validate_connector_key_registry(
        connector_key_registry
    )
    selected = build_selected_markets(universe, adapter_snapshot)
    selected_by_id = {row["market_id"]: row for row in selected}
    supported = {
        market_id for market_id, row in selected_by_id.items()
        if row["structural_support_status"] == "supported"
    }
    if not supported:
        raise RouteCostEvidenceError(
            "trace-profile-missing builder requires supported markets"
        )
    if not isinstance(retained_typed_pool_state_members, Mapping) or set(
        retained_typed_pool_state_members
    ) != supported:
        raise RouteCostEvidenceError(
            "trace-profile-missing retained pool-state inventory differs"
        )
    retained_states: Dict[str, Dict[str, Any]] = {}
    for market_id in sorted(supported):
        member = retained_typed_pool_state_members[market_id]
        if (
            not isinstance(member, Mapping)
            or set(member) != {"descriptor", "payload"}
            or not isinstance(member.get("descriptor"), Mapping)
            or member["descriptor"].get("market_id") != market_id
        ):
            raise RouteCostEvidenceError(
                "trace-profile-missing retained pool-state member differs"
            )
        state = validate_retained_v2_pool_state_member(
            member.get("payload"), descriptor=member.get("descriptor")
        )
        state["_physical_sha256"] = member["descriptor"]["sha256"]
        retained_states[market_id] = state

    universe_legs = universe.get("selected_legs")
    if not isinstance(universe_legs, list):
        raise RouteCostEvidenceError("route universe selected legs are invalid")
    simulation_targets = build_simulation_targets(
        universe_legs, selected_by_id, retained_states
    )
    if {
        market_id for market_id, _notional in simulation_targets
    } != supported:
        raise RouteCostEvidenceError(
            "trace-profile-missing simulation target inventory is incomplete"
        )

    trace_identity, _trace_generation = trace_profile_identity(None)
    connector_identity, _connector_generation = (
        submission_connector_profile_identity(None)
    )
    transcripts = build_terminal_transcript_inventory(
        universe=universe,
        run_id=run_id,
        route_cohort_id=route_cohort_id,
        candidate_source_generation=candidate_source_generation,
        route_universe_sha256=route_universe_sha256,
        adapter_registry=adapter_snapshot,
        trace_profile_identity=trace_identity,
        submission_connector_profile_identity=connector_identity,
        retained_typed_pool_state_members=retained_typed_pool_state_members,
        terminal_reason_by_market={
            market_id: "trace_profile_missing" for market_id in supported
        },
    )
    snapshot = build_terminal_submission_policy_snapshot(
        universe=universe,
        run_id=run_id,
        route_cohort_id=route_cohort_id,
        candidate_source_generation=candidate_source_generation,
        route_universe_sha256=route_universe_sha256,
        adapter_registry=adapter_snapshot,
        connector_key_registry=connector_snapshot,
        trace_profile_identity=trace_identity,
        submission_connector_profile_identity=connector_identity,
        reason_code="submission_connector_missing",
    )
    return build_route_cost_evidence_manifest_from_captured(
        universe=universe,
        run_id=run_id,
        route_cohort_id=route_cohort_id,
        phase=phase,
        candidate_source_generation=candidate_source_generation,
        route_universe_sha256=route_universe_sha256,
        evaluated_at=evaluated_at,
        adapter_registry=adapter_snapshot,
        connector_key_registry=connector_snapshot,
        trace_profile_identity=trace_identity,
        submission_connector_profile_identity=connector_identity,
        native_price_evidence=None,
        chain_evidence=[],
        market_evidence=[],
        transcripts=transcripts,
        submission_policy_snapshot=snapshot,
    )


def _validate_route_cost_evidence_manifest(
    value: Mapping[str, Any],
    *,
    universe: Mapping[str, Any],
    expected_run_id: str,
    expected_route_cohort_id: str,
    expected_phase: str,
    expected_candidate_source_generation: str,
    expected_route_universe_sha256: str,
    _authenticated_snapshot_verified: bool = False,
    _retained_typed_pool_state_members: Optional[
        Mapping[str, Mapping[str, Any]]
    ] = None,
) -> Dict[str, Any]:
    """Replay one complete full-v1 route-cost sidecar without external I/O."""
    _exact_fields(value, ROUTE_COST_EVIDENCE_FIELDS, "route-cost evidence")
    links = {
        "run_id": expected_run_id,
        "route_cohort_id": expected_route_cohort_id,
        "candidate_source_generation": expected_candidate_source_generation,
        "route_universe_sha256": expected_route_universe_sha256,
    }
    if (
        value.get("schema") != ROUTE_COST_EVIDENCE_SCHEMA
        or value.get("run_id") != expected_run_id
        or value.get("route_cohort_id") != expected_route_cohort_id
        or value.get("phase") != expected_phase
        or value.get("candidate_source_generation")
        != expected_candidate_source_generation
        or value.get("route_universe_sha256") != expected_route_universe_sha256
    ):
        raise RouteCostEvidenceError("route-cost outer lineage differs")
    _required_text(expected_run_id, "run ID", _SAFE_RUN_ID)
    if _COHORT_ID.fullmatch(expected_route_cohort_id or "") is None:
        raise RouteCostEvidenceError("route cohort ID is invalid")
    if expected_phase not in {"canary", "full"}:
        raise RouteCostEvidenceError("phase is invalid")
    _sha256(expected_candidate_source_generation, "candidate source generation")
    _sha256(expected_route_universe_sha256, "route universe hash")
    if physical_sha256(universe) != expected_route_universe_sha256:
        raise RouteCostEvidenceError("route universe physical hash differs")
    if universe.get("candidate_source_generation") != expected_candidate_source_generation:
        raise RouteCostEvidenceError("route universe generation differs")

    adapter_registry = validate_adapter_registry(value.get("adapter_registry"))
    connector_registry = validate_connector_key_registry(
        value.get("connector_key_registry")
    )
    # Historical replay is authoritative from these embedded exact bytes.  It
    # must not consult or compare to the moving checked-in registries: a later
    # funding/key rotation cannot rewrite an already published sidecar.  The
    # production capture builder, separately, uses the fixed tracked loaders.
    adapter_sha = physical_sha256(adapter_registry)
    connector_sha = physical_sha256(connector_registry)
    if value.get("adapter_registry_sha256") != adapter_sha or value.get("connector_key_registry_sha256") != connector_sha:
        raise RouteCostEvidenceError("registry physical hash differs")
    links.update({
        "adapter_registry_sha256": adapter_sha,
        "connector_key_registry_sha256": connector_sha,
    })
    trace_identity, trace_generation = _validate_profile_identity(
        value.get("trace_profile_identity"), kind="trace"
    )
    connector_identity, connector_generation = _validate_profile_identity(
        value.get("submission_connector_profile_identity"), kind="connector"
    )
    if value.get("trace_profile_generation") != trace_generation:
        raise RouteCostEvidenceError("trace profile generation differs")
    if value.get("submission_connector_profile_generation") != connector_generation:
        raise RouteCostEvidenceError("connector profile generation differs")
    links["trace_profile_generation"] = trace_generation
    links["submission_connector_profile_generation"] = connector_generation
    _timestamp(value.get("evaluated_at"), "route-cost evaluated_at")

    expected_selected = build_selected_markets(universe, adapter_registry)
    selected = value.get("selected_markets")
    selected_count = _exact_int(
        value.get("selected_market_count"), "selected market count", 0, MAX_SELECTED_MARKETS
    )
    if not isinstance(selected, list) or len(selected) != selected_count:
        raise RouteCostEvidenceError("selected market count differs")
    for row in selected:
        _validate_selected_market_row(row)
    if selected != expected_selected:
        raise RouteCostEvidenceError("selected market replay differs")
    selected_sha = selected_market_set_sha256(selected)
    if value.get("selected_market_set_sha256") != selected_sha:
        raise RouteCostEvidenceError("selected-market set hash differs")
    links["selected_market_set_sha256"] = selected_sha
    selected_by_id = {row["market_id"]: row for row in selected}
    universe_legs = universe.get("selected_legs")
    if not isinstance(universe_legs, list):
        raise RouteCostEvidenceError("route universe selected legs are invalid")
    supported_selected_ids = {
        market_id for market_id, row in selected_by_id.items()
        if row["structural_support_status"] == "supported"
    }
    selected_market_tokens: Dict[str, Tuple[str, str]] = {}
    selected_legs_by_id: Dict[str, Mapping[str, Any]] = {}
    for leg in universe_legs:
        if not isinstance(leg, Mapping):
            raise RouteCostEvidenceError("route universe selected leg is invalid")
        market_id = leg.get("market_id")
        if market_id not in supported_selected_ids:
            continue
        if market_id in selected_legs_by_id:
            raise RouteCostEvidenceError(
                "selected market token identity is duplicated"
            )
        selected_legs_by_id[market_id] = leg
        target_identity = _strict_dex_target_identity(leg)
        if target_identity is not None:
            selected_market_tokens[market_id] = target_identity[:2]
    if set(selected_legs_by_id) != supported_selected_ids:
        raise RouteCostEvidenceError("selected market token inventory differs")

    supplied_members = _retained_typed_pool_state_members
    if supplied_members is None:
        supplied_members = {}
    if not isinstance(supplied_members, Mapping):
        raise RouteCostEvidenceError("retained pool-state inventory is invalid")
    retained_pool_states: Dict[str, Dict[str, Any]] = {}
    for market_id, item in supplied_members.items():
        if (
            not isinstance(market_id, str)
            or market_id not in selected_by_id
            or selected_by_id[market_id]["structural_support_status"]
            != "supported"
            or not isinstance(item, Mapping)
            or set(item) != {"descriptor", "payload"}
            or item.get("descriptor", {}).get("market_id") != market_id
        ):
            raise RouteCostEvidenceError("retained pool-state inventory differs")
        replayed = validate_retained_v2_pool_state_member(
            item.get("payload"), descriptor=item.get("descriptor")
        )
        # The descriptor is only a byte/identity envelope.  Replay still binds
        # the state to the fixed chain header encoded in this cost manifest.
        matching_chain_headers = [
            row.get("block_header_result")
            for row in value.get("chain_evidence", [])
            if isinstance(row, Mapping) and row.get("chain_id") == 1
        ]
        if len(matching_chain_headers) == 0:
            # Pre-chain terminal outcomes retain the canonical V2 member but
            # deliberately publish no partial cost-side chain object.  Permit
            # only the frozen stage-none/null-shared-evidence reasons; the
            # complete transcript validator below still enforces cardinality,
            # status, reason and the rest of the exact presence matrix.
            market_rows = [
                row for row in value.get("transcripts", [])
                if isinstance(row, Mapping)
                and row.get("market_id") == market_id
            ]
            pre_chain_reasons = {
                "rpc_unavailable", "rpc_invalid",
                "fixed_block_unavailable", "fixed_block_mismatch",
            }
            if (
                value.get("trace_profile_identity", {}).get("status")
                != "missing"
                and (
                    not market_rows
                    or any(
                        row.get("completed_stage") != "none"
                        or row.get("reason_code") not in pre_chain_reasons
                        or row.get("chain_evidence_sha256") is not None
                        or row.get("market_evidence_sha256") is not None
                        for row in market_rows
                    )
                )
            ):
                raise RouteCostEvidenceError(
                    "retained pool state lacks one fixed chain anchor"
                )
        elif len(matching_chain_headers) == 1:
            header = matching_chain_headers[0]
            if (
                not isinstance(header, Mapping)
                or int(replayed["chain_id"]) != 1
                or int(replayed["block_number"]) != int(header.get("number"), 16)
                or replayed["block_hash"] != header.get("hash")
                or replayed["block_header_sha256"] != physical_sha256(header)
                or _timestamp_value(
                    replayed["observed_at"], "retained state observed_at"
                ) != int(header.get("timestamp"), 16)
            ):
                raise RouteCostEvidenceError(
                    "retained pool state differs from fixed chain anchor"
                )
        else:
            raise RouteCostEvidenceError(
                "retained pool state lacks one fixed chain anchor"
            )
        replayed["_physical_sha256"] = item["descriptor"]["sha256"]
        retained_pool_states[market_id] = replayed
    supported_ids = {
        market_id for market_id, row in selected_by_id.items()
        if row["structural_support_status"] == "supported"
    }
    referenced_core_ids = {
        row.get("market_id") for row in value.get("transcripts", [])
        if isinstance(row, Mapping) and row.get("core_pool_state_id") is not None
    }
    if referenced_core_ids != set(retained_pool_states):
        raise RouteCostEvidenceError("retained pool-state referenced inventory differs")
    if set(retained_pool_states) - supported_ids:
        raise RouteCostEvidenceError("retained pool state belongs to unsupported market")
    simulation_targets = build_simulation_targets(
        universe_legs, selected_by_id, retained_pool_states
    )

    native_evidence = value.get("native_price_evidence")
    native_sha = value.get("native_price_evidence_sha256")
    if native_evidence is None:
        if native_sha is not None:
            raise RouteCostEvidenceError("native-price null matrix is invalid")
    else:
        expected_native_sha = _validate_native_price_evidence(native_evidence, links)
        if native_sha != expected_native_sha:
            raise RouteCostEvidenceError("native-price evidence hash differs")

    set_specs = (
        ("chain_evidence", "chain_evidence_count", 64, "chain_evidence_set_sha256", b"route-cost-chain-evidence-set/v1\n"),
        ("market_evidence", "market_evidence_count", 8, "market_evidence_set_sha256", b"route-cost-market-evidence-set/v1\n"),
        ("transcripts", "transcript_count", MAX_TRANSCRIPTS, "transcript_set_sha256", b"route-cost-evidence-transcript-set/v1\n"),
        ("bindings", "binding_count", MAX_BINDINGS, "binding_set_sha256", b"route-cost-evidence-binding-set/v1\n"),
    )
    for rows_field, count_field, maximum, sha_field, domain in set_specs:
        rows = value.get(rows_field)
        count = _exact_int(value.get(count_field), count_field, 0, maximum)
        if not isinstance(rows, list) or len(rows) != count:
            raise RouteCostEvidenceError("{} count differs".format(rows_field))
        if typed_sha256(domain, rows) != value.get(sha_field):
            raise RouteCostEvidenceError("{} set hash differs".format(rows_field))

    chain_hashes: Dict[str, Mapping[str, Any]] = {}
    chain_ids = []
    for row in value["chain_evidence"]:
        row_sha = _validate_chain_evidence(
            row, links, trace_profile_identity=trace_identity
        )
        native_record = row["native_price_record"]
        if native_record.get("status") == "observed":
            if native_evidence is None or native_sha is None:
                raise RouteCostEvidenceError(
                    "observed chain native price lacks top-level evidence"
                )
            expected_price = _format_decimal(
                Decimal(native_evidence["book_projection"]["best_ask_price"])
                * Decimal(native_evidence["usd_conversion_projection"]["rate"])
            )
            if (
                native_record.get("native_price_evidence_sha256") != native_sha
                or native_record.get("source_record_sha256")
                != native_evidence.get("source_record_sha256")
                or native_record.get("price_usd") != expected_price
                or native_record.get("observed_at")
                != native_evidence.get("observed_at")
                or native_record.get("valid_until")
                != native_evidence.get("valid_until")
            ):
                raise RouteCostEvidenceError(
                    "chain native price differs from top-level evidence"
                )
        elif native_evidence is not None:
            raise RouteCostEvidenceError(
                "top-level native evidence contradicts terminal chain record"
            )
        chain_hashes[row_sha] = row
        chain_ids.append(row["chain_id"])
    if chain_ids != sorted(set(chain_ids)):
        raise RouteCostEvidenceError("chain evidence is not canonical")

    adapter = next(
        (
            row for row in adapter_registry["adapters"]
            if row["adapter_id"] == ETHEREUM_V2_ADAPTER_ID
        ),
        None,
    )
    market_hashes: Dict[str, Mapping[str, Any]] = {}
    market_ids = []
    for row in value["market_evidence"]:
        if adapter is None:
            raise RouteCostEvidenceError(
                "market evidence exists without a captured adapter"
            )
        row_sha = _validate_market_evidence(
            row, links, chain_hashes, set(selected_by_id), adapter,
            retained_pool_states,
        )
        market_hashes[row_sha] = row
        market_ids.append(row["market_id"])
    if market_ids != sorted(set(market_ids)):
        raise RouteCostEvidenceError("market evidence is not canonical")

    expected_transcript_scope = [
        (row["market_id"], direction, notional)
        for row in selected
        for direction in ("buy", "sell")
        for notional in REQUESTED_NOTIONALS_USD
    ]
    transcript_scope = []
    transcript_by_sha = {}
    for row in value["transcripts"]:
        scope = _validate_transcript(
            row,
            links=links,
            selected=selected_by_id,
            chain_hashes=chain_hashes,
            market_hashes=market_hashes,
            adapter=adapter,
            native_sha=native_sha,
            native_evidence=native_evidence,
            retained_pool_states=retained_pool_states,
            market_tokens=selected_market_tokens,
            simulation_targets=simulation_targets,
            evaluated_at=value["evaluated_at"],
        )
        transcript_scope.append(scope)
        transcript_by_sha[typed_sha256(b"route-cost-evidence-transcript/v1\n", row)] = row
    if transcript_scope != expected_transcript_scope:
        raise RouteCostEvidenceError("transcript denominator/sort differs")
    if native_evidence is not None and _timestamp_value(
        value.get("evaluated_at"), "route-cost evaluated_at"
    ) > _timestamp_value(
        native_evidence.get("valid_until"), "native evidence valid_until"
    ):
        raise RouteCostEvidenceError(
            "native validity does not cover route-cost evaluated_at"
        )
    if trace_identity["status"] == "missing" and any(
        row.get("reason_code") not in {
            "core_pool_state_unavailable",
            "core_pool_state_invalid",
            "trace_profile_missing",
        }
        for row in value["transcripts"]
        if selected_by_id[row["market_id"]]["structural_support_status"]
        == "supported"
    ):
        raise RouteCostEvidenceError(
            "missing trace profile has a contradictory transcript reason"
        )

    expected_bindings, route_sides = _expected_binding_scope(
        universe, selected_by_id
    )
    binding_scope_empty = len(expected_bindings) == 0
    policy_members = _validate_policy_snapshot(
        value.get("submission_policy_snapshot"),
        links,
        binding_scope_empty,
        connector_registry=connector_registry,
        route_sides=route_sides,
        permit_authenticated=_authenticated_snapshot_verified,
        verified_signature=_authenticated_snapshot_verified,
    )
    snapshot_connector = value["submission_policy_snapshot"].get("connector_id")
    if connector_identity["status"] == "available":
        if snapshot_connector != connector_identity["connector_id"]:
            raise RouteCostEvidenceError(
                "policy snapshot connector differs from profile identity"
            )
    elif snapshot_connector is not None:
        raise RouteCostEvidenceError(
            "missing connector profile cannot name a connector"
        )
    if connector_identity["status"] == "missing" and not binding_scope_empty:
        snapshot = value["submission_policy_snapshot"]
        if (
            snapshot.get("status") != "unavailable"
            or snapshot.get("reason_code") != "submission_connector_missing"
            or any(
                member.get("status") != "unavailable"
                or member.get("reason_code") != "submission_connector_missing"
                for member in snapshot.get("members", [])
            )
        ):
            raise RouteCostEvidenceError(
                "missing connector profile has a contradictory policy reason"
            )
    if typed_sha256(
        b"route-cost-submission-policy-snapshot/v1\n",
        value["submission_policy_snapshot"],
    ) != value.get("submission_policy_snapshot_sha256"):
        raise RouteCostEvidenceError("policy snapshot hash differs")

    binding_scope = []
    for binding in value["bindings"]:
        _exact_fields(binding, BINDING_FIELDS, "binding")
        if binding.get("schema") != ROUTE_COST_BINDING_SCHEMA:
            raise RouteCostEvidenceError("binding schema is invalid")
        for field in (
            "run_id", "route_cohort_id", "candidate_source_generation",
            "route_universe_sha256", "adapter_registry_sha256",
            "selected_market_set_sha256", "connector_key_registry_sha256",
            "trace_profile_generation", "submission_connector_profile_generation",
        ):
            if binding.get(field) != links[field]:
                raise RouteCostEvidenceError("binding lineage differs")
        route_id = binding.get("route_id")
        notional = binding.get("requested_notional_usd")
        if not isinstance(route_id, str) or notional not in REQUESTED_NOTIONALS_USD:
            raise RouteCostEvidenceError("binding identity is invalid")
        binding_scope.append((route_id, notional))
        for field in ("buy_transcript_sha256", "sell_transcript_sha256"):
            sha = binding.get(field)
            if sha is not None and _sha256(sha, "binding transcript hash") not in transcript_by_sha:
                raise RouteCostEvidenceError("binding transcript does not resolve")
        member_sha = _sha256(
            binding.get("submission_policy_member_sha256"), "binding policy-member hash"
        )
        member = policy_members.get((route_id, notional))
        if member is None or typed_sha256(
            b"route-cost-submission-policy-member/v1\n", member
        ) != member_sha:
            raise RouteCostEvidenceError("binding policy member does not resolve")
        route = next(
            row for row in universe["routes"] if row.get("route_id") == route_id
        )
        resolved_transcripts: List[Mapping[str, Any]] = []
        for leg, transcript_field, member_field in (
            ("buy", "buy_transcript_sha256", "buy_submission_loss_bps"),
            ("sell", "sell_transcript_sha256", "sell_submission_loss_bps"),
        ):
            market_id = route.get(leg + "_market_id")
            transcript_sha = binding.get(transcript_field)
            if market_id in selected_by_id and selected_by_id[market_id]["structural_support_status"] == "supported":
                if transcript_sha is None:
                    raise RouteCostEvidenceError("DEX binding transcript is absent")
                transcript = transcript_by_sha[transcript_sha]
                resolved_transcripts.append(transcript)
                if (
                    transcript.get("market_id") != market_id
                    or transcript.get("direction") != leg
                    or transcript.get("requested_notional_usd") != notional
                ):
                    raise RouteCostEvidenceError("binding transcript scenario differs")
                if member.get("status") == "observed" and (
                    transcript.get("call_evidence") is None
                    or member.get(member_field)
                    != transcript["call_evidence"].get("submission_loss_bound_bps")
                ):
                    raise RouteCostEvidenceError("signed policy bound differs from calldata")
            elif transcript_sha is not None or member.get(member_field) is not None:
                raise RouteCostEvidenceError("CEX binding side must be null")
        if binding.get("evaluated_at") != value.get("evaluated_at"):
            raise RouteCostEvidenceError("binding evaluation time differs")
        status = binding.get("status")
        reason = binding.get("reason_code")
        if status not in {"observed", "unavailable", "failed"}:
            raise RouteCostEvidenceError("binding status is invalid")
        expected_status, expected_reason = _derived_binding_status_reason(
            route_sides=route_sides[route_id],
            transcripts=resolved_transcripts,
            member=member,
            snapshot=value["submission_policy_snapshot"],
            evaluated_at=value["evaluated_at"],
        )
        if (status, reason) != (expected_status, expected_reason):
            raise RouteCostEvidenceError("binding status/reason differs")
    if binding_scope != expected_bindings:
        raise RouteCostEvidenceError("binding denominator/sort differs")

    counts = value.get("counts")
    _exact_fields(counts, COUNT_FIELDS, "route-cost counts")
    for prefix, rows in (("transcript", value["transcripts"]), ("binding", value["bindings"])):
        actual = Counter(row["status"] for row in rows)
        for status in ("observed", "unavailable", "failed"):
            field = "{}_{}".format(prefix, status)
            if _exact_int(counts.get(field), field, 0, MAX_BINDINGS) != actual[status]:
                raise RouteCostEvidenceError("route-cost counts differ")

    referenced_chain = {
        row["chain_evidence_sha256"] for row in value["transcripts"]
        if row.get("chain_evidence_sha256") is not None
    }
    referenced_market = {
        row["market_evidence_sha256"] for row in value["transcripts"]
        if row.get("market_evidence_sha256") is not None
    }
    if set(chain_hashes) != referenced_chain or set(market_hashes) != referenced_market:
        raise RouteCostEvidenceError("shared evidence is orphaned or missing")
    if len(canonical_json_bytes(value)) > MAX_ROUTE_COST_EVIDENCE_BYTES:
        raise RouteCostEvidenceError("route-cost sidecar exceeds its byte limit")
    return _canonical_copy(value)


def validate_route_cost_evidence_manifest(
    value: Mapping[str, Any],
    *,
    universe: Mapping[str, Any],
    expected_run_id: str,
    expected_route_cohort_id: str,
    expected_phase: str,
    expected_candidate_source_generation: str,
    expected_route_universe_sha256: str,
) -> Dict[str, Any]:
    """Pure replay; authenticated policy snapshots fail closed."""
    return _validate_route_cost_evidence_manifest(
        value,
        universe=universe,
        expected_run_id=expected_run_id,
        expected_route_cohort_id=expected_route_cohort_id,
        expected_phase=expected_phase,
        expected_candidate_source_generation=expected_candidate_source_generation,
        expected_route_universe_sha256=expected_route_universe_sha256,
        _authenticated_snapshot_verified=False,
    )


def validate_route_cost_evidence_manifest_for_publication(
    value: Mapping[str, Any],
    *,
    universe: Mapping[str, Any],
    expected_run_id: str,
    expected_route_cohort_id: str,
    expected_phase: str,
    expected_candidate_source_generation: str,
    expected_route_universe_sha256: str,
    retained_typed_pool_state_members: Optional[
        Mapping[str, Mapping[str, Any]]
    ] = None,
) -> Dict[str, Any]:
    """Replay evidence and cryptographically verify an authenticated snapshot.

    The caller cannot inject a verifier, key, path, argv, or boolean result.
    Unsupported/not-applicable/unavailable evidence remains a pure replay and
    launches no subprocess.  Only an authenticated snapshot reaches the fixed
    bounded `/usr/bin/ssh-keygen` adapter.
    """
    snapshot = value.get("submission_policy_snapshot")
    authenticated = (
        isinstance(snapshot, Mapping)
        and snapshot.get("status") == "authenticated"
    )
    if authenticated:
        registry = validate_connector_key_registry(
            value.get("connector_key_registry")
        )
        _verify_snapshot_sshsig_fixed(snapshot, registry)
    return _validate_route_cost_evidence_manifest(
        value,
        universe=universe,
        expected_run_id=expected_run_id,
        expected_route_cohort_id=expected_route_cohort_id,
        expected_phase=expected_phase,
        expected_candidate_source_generation=expected_candidate_source_generation,
        expected_route_universe_sha256=expected_route_universe_sha256,
        _authenticated_snapshot_verified=authenticated,
        _retained_typed_pool_state_members=retained_typed_pool_state_members,
    )
