"""Strict route-cost evidence contract tests.

These fixtures deliberately derive expected hashes without importing helpers
from the implementation under test.  They are the producer/consumer boundary
for the private Shadow sidecar, not tests of route-publication internals.
"""

from __future__ import annotations

import base64
import copy
from decimal import Decimal
import gzip
import hashlib
import inspect
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest
from unittest import mock

import scripts.route_cost_evidence as route_cost_evidence


RUN_ID = "shadow-run-cost-1"
COHORT_ID = "cohort:" + "c" * 64
GENERATION = "a" * 64
PHASE = "canary"
EVALUATED_AT = "2026-08-01T12:00:03Z"
TOKEN_A = "0x" + "1" * 40
TOKEN_B = "0x" + "2" * 40
POOL = "0x" + "3" * 40
ROUTER = "0x7a250d5630b4cf539739df2c5dacb4c659f2488d"
FACTORY = "0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f"
SENDER = "0x000000000000000000000000000000000000dead"
ADAPTER_ID = "uniswap-v2-router02-ethereum"
MARKET_ID = "dex:eth:uniswap_v2:{}:AAA".format(POOL)
NOTIONALS = [1000, 5000, 10000, 50000, 100000]
SSHSIG_KAT_PUBLIC_KEY = (
    "ssh-ed25519 "
    "AAAAC3NzaC1lZDI1NTE5AAAAIPr40BZ7LJsVDPkf5cn0cfUOJ3Zi8hxfvJo+QHtKFDX7"
)
SSHSIG_KAT_SIGNATURE = """-----BEGIN SSH SIGNATURE-----
U1NIU0lHAAAAAQAAADMAAAALc3NoLWVkMjU1MTkAAAAg+vjQFnssmxUM+R/lyfRx9Q4ndm
LyHF+8mj5Ae0oUNfsAAAAfcm91dGUtY29zdC1zdWJtaXNzaW9uLXBvbGljeS12MQAAAAAA
AAAGc2hhNTEyAAAAUwAAAAtzc2gtZWQyNTUxOQAAAECPuOjfvclnRSDZT0Htma+aqaxsmB
aQOZ9dSwzaLspVbURohsl53yGHzrCSbJ8jtWFOioLm3wzgNuNuxKYx0JMO
-----END SSH SIGNATURE-----"""
V2_STATE_FIELDS = (
    "schema", "chain", "chain_id", "dex", "pool_address",
    "token0_address", "token1_address", "token0_decimals",
    "token1_decimals", "reserve0_raw", "reserve1_raw",
    "reserve_timestamp_last_raw", "fee_bps", "fee_numerator",
    "fee_denominator", "fee_formula", "fee_proof_sha256",
    "block_number", "block_hash", "block_header_sha256", "observed_at",
    "raw_response_sha256", "state_id",
)


def canonical_bytes(value):
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def physical_sha(value):
    return hashlib.sha256(canonical_bytes(value)).hexdigest()


def typed_sha(domain, value):
    return hashlib.sha256(domain + canonical_bytes(value) + b"\n").hexdigest()


def pinned_runtime_code(name):
    fixture = (
        Path(__file__).resolve().parent
        / "fixtures"
        / "route_cost_runtime_code"
        / "{}.bin.gz.b64".format(name)
    )
    encoded = fixture.read_text(encoding="ascii").strip()
    return gzip.decompress(base64.b64decode(encoded, validate=True))


def pinned_pair_runtime_code():
    response = (
        Path(__file__).resolve().parent
        / "fixtures"
        / "route_cost_runtime_code"
        / "uniswap-v2-pair-runtime.bin.gz.b64"
    )
    encoded = response.read_text(encoding="ascii").strip()
    return gzip.decompress(base64.b64decode(encoded, validate=True))


def runtime_code_evidence(tokens, *, block_tag="0x64", code=b"\x01"):
    return [
        {
            "schema": "route_cost_token_runtime_code_evidence/v1",
            "token_address": token,
            "request": {
                "schema": "route_cost_token_runtime_code_request/v1",
                "jsonrpc": "2.0",
                "id": request_id,
                "method": "eth_getCode",
                "params": [token, block_tag],
            },
            "response": {
                "schema": "route_cost_token_runtime_code_response/v1",
                "jsonrpc": "2.0",
                "id": request_id,
                "result": "0x" + code.hex(),
            },
        }
        for request_id, token in enumerate(sorted(tokens), start=8)
    ]


def context(token_a=TOKEN_A, token_b=TOKEN_B):
    return {
        "schema": "route_collector_context/v1",
        "snapshot_id": "tvl-1",
        "request_started_at": "2026-08-01T11:59:58+00:00",
        "observed_at": "2026-08-01T12:00:00+00:00",
        "response_received_at": "2026-08-01T12:00:01+00:00",
        "status": "observed",
        "reason_code": "observed",
        "pool_name": "AAA/WETH",
        "base_token_id": "eth_{}".format(token_a),
        "quote_token_id": "eth_{}".format(token_b),
        "base_token_price_usd": "1",
        "quote_token_price_usd": "3000",
        "tvl_method": "locked_token_balance_times_synchronized_usd_price",
        "source": "defillama",
        "source_endpoint": "https://coins.llama.fi/prices/current/...",
        "raw_response_sha256": "9" * 64,
    }


def universe_for(markets=None, routes=None):
    if markets is None:
        markets = [
            {
                "market_id": MARKET_ID,
                "market_type": "dex",
                "token_symbol": "AAA",
                "candidate_source_generation": GENERATION,
                "selection_window": {
                    "start": "2026-07-03",
                    "end": "2026-08-01",
                },
                "selection_inputs": {
                    "execution_capability": "proved",
                    "proved_execution_capacity_usd": "100000",
                    "observed_100bps_depth_usd": "200000",
                    "cex_selected_window_usd": None,
                    "dex_24h_usd": "900",
                    "dex_tvl_usd": "1000",
                },
                "selection_rank": 1,
                "collector_context": context(),
                "target_token_address": TOKEN_A,
                "target_token_side": "base",
            }
        ]
    if routes is None:
        routes = [
            {
                "route_id": "route:AAA:{}->cex:x:AAA/USDT:prepositioned_inventory".format(
                    MARKET_ID
                ),
                "token_symbol": "AAA",
                "buy_market_id": MARKET_ID,
                "sell_market_id": "cex:x:AAA/USDT",
                "route_mode": "prepositioned_inventory",
                "route_class": "candidate",
                "settlement_reason": None,
                "requested_notionals_usd": list(NOTIONALS),
                "candidate_source_generation": GENERATION,
                "buy_reference_volume_usd": "900",
                "sell_reference_volume_usd": "2000",
                "route_volume_usd": "900",
                "route_volume_basis": "minimum_leg_source_horizon_usd",
            }
        ]
    return {
        "schema": "route_universe/v1",
        "candidate_source_generation": GENERATION,
        "selection_window": {
            "start": "2026-07-03",
            "end": "2026-08-01",
        },
        "requested_notionals_usd": list(NOTIONALS),
        "selected_legs": markets,
        "routes": routes,
    }


def funding_descriptor(address, *, balance_slot="0", allowance_slot="1"):
    return {
        "token_address": address,
        "runtime_code_sha256": hashlib.sha256(b"\x01").hexdigest(),
        "proxy_implementation_address": None,
        "proxy_implementation_code_sha256": None,
        "storage_layout": "solidity_mapping_v1",
        "balance_mapping_slot": balance_slot,
        "allowance_mapping_slot": allowance_slot,
        "source_metadata_sha256": "5" * 64,
    }


def retained_v2_pool_state(*, market_id=MARKET_ID, block_number=100,
                           block_timestamp=1785585600, pool_address=POOL,
                           token0=TOKEN_A, token1=TOKEN_B,
                           reserve0_raw=1_000_000,
                           reserve1_raw=2_000_000):
    from scripts.route_quantity import V2PoolState

    block_hash = "0x" + "7" * 64
    header = {
        "number": hex(block_number),
        "hash": block_hash,
        "parent_hash": "0x" + "a" * 64,
        "timestamp": hex(block_timestamp),
        "base_fee_per_gas": "0x64",
        "gas_used": "0x1",
        "gas_limit": "0x2",
    }
    state = V2PoolState(
        chain="eth",
        chain_id=1,
        dex="uniswap_v2",
        pool_address=pool_address,
        token0_address=token0,
        token1_address=token1,
        token0_decimals=18,
        token1_decimals=18,
        reserve0_raw=reserve0_raw,
        reserve1_raw=reserve1_raw,
        reserve_timestamp_last_raw=12_000,
        fee_bps=30,
        fee_numerator=9_970,
        fee_denominator=10_000,
        fee_formula=(
            "amount_in_with_fee=amount_in*fee_numerator;"
            "denominator=reserve_in*fee_denominator+amount_in_with_fee"
        ),
        fee_proof_sha256="6" * 64,
        block_number=block_number,
        block_hash=block_hash,
        block_header_sha256=physical_sha(header),
        observed_at="2026-08-01T12:00:00Z",
        raw_response_sha256="9" * 64,
    )
    payload = {
        "schema": "route_v2_pool_state/v1",
        **{
            field: (
                str(getattr(state, field))
                if field in {
                    "chain_id", "token0_decimals", "token1_decimals",
                    "reserve0_raw", "reserve1_raw",
                    "reserve_timestamp_last_raw", "fee_bps",
                    "fee_numerator", "fee_denominator", "block_number",
                }
                else getattr(state, field)
            )
            for field in V2_STATE_FIELDS
            if field != "schema"
        },
    }
    payload_bytes = canonical_bytes(payload)
    descriptor = {
        "market_id": market_id,
        "role": "dex_pool_state",
        "filename": "0000-dex_pool_state.json",
        "sha256": hashlib.sha256(payload_bytes).hexdigest(),
        "size": len(payload_bytes),
        "logical_generation": state.state_id.split(":", 1)[1],
        "adapter_id": "route_quantity_quote_for_v2_pool/v1",
        "content_schema": "route_v2_pool_state/v1",
    }
    return {
        "descriptor": descriptor,
        "payload": payload_bytes,
    }


def phase_a_rpc_responses(plan):
    """Return one exact successful Phase-A response inventory, in reverse order."""
    router_code = "0x" + pinned_runtime_code(
        "uniswap-v2-router02-runtime"
    ).hex()
    factory_code = "0x" + pinned_runtime_code(
        "uniswap-v2-factory-runtime"
    ).hex()
    pair_code = "0x" + pinned_pair_runtime_code().hex()
    results = {
        "chain_id": "0x1",
        "block_header": {
            "number": "0x64",
            "hash": "0x" + "7" * 64,
            "parentHash": "0x" + "a" * 64,
            "timestamp": hex(1785585600),
            "baseFeePerGas": "0x64",
            "gasUsed": "0x1",
            "gasLimit": "0x2",
        },
        "fee_history": {
            "oldestBlock": "0x64",
            "baseFeePerGas": ["0x64", "0x64"],
            "reward": [["0x3"]],
            "gasUsedRatio": [0.5],
        },
        "router_runtime_code": router_code,
        "factory_runtime_code": factory_code,
        "factory_get_pair": "0x" + "0" * 24 + POOL[2:],
        "pair_runtime_code": pair_code,
        "pair_token0": "0x" + "0" * 24 + TOKEN_A[2:],
        "pair_token1": "0x" + "0" * 24 + TOKEN_B[2:],
        "token0_runtime_code": "0x01",
        "token1_runtime_code": "0x01",
    }
    return [
        {
            "jsonrpc": "2.0",
            "id": role["id"],
            "result": results[role["role"]],
        }
        for role in reversed(plan["request_roles"])
    ]


def supported_core_manifest():
    value = unsupported_manifest(tracked=False)
    trace_identity = {
        "schema": "route_cost_trace_profile_identity/v1",
        "status": "available",
        "profile_id": "test-trace",
        "endpoint_id": "rpc-mainnet-a",
    }
    value["trace_profile_identity"] = trace_identity
    value["trace_profile_generation"] = typed_sha(
        b"route-cost-trace-profile-identity/v1\n", trace_identity
    )
    for transcript in value["transcripts"]:
        transcript["trace_profile_generation"] = value[
            "trace_profile_generation"
        ]
    value["transcript_set_sha256"] = typed_sha(
        b"route-cost-evidence-transcript-set/v1\n", value["transcripts"]
    )
    value["submission_policy_snapshot"]["trace_profile_generation"] = value[
        "trace_profile_generation"
    ]
    registry = adapter_registry(supported=True)
    registry_sha = physical_sha(registry)
    value["adapter_registry"] = registry
    value["adapter_registry_sha256"] = registry_sha
    value["selected_markets"][0].update({
        "structural_support_status": "supported",
        "structural_reason": None,
    })
    selected_sha = physical_sha({
        "schema": "route_cost_selected_markets/v1",
        "members": value["selected_markets"],
    })
    value["selected_market_set_sha256"] = selected_sha

    retained = retained_v2_pool_state()
    state = json.loads(retained["payload"])
    header = {
        "number": hex(int(state["block_number"])),
        "hash": state["block_hash"],
        "parent_hash": "0x" + "a" * 64,
        "timestamp": hex(1785585600),
        "base_fee_per_gas": "0x64",
        "gas_used": "0x1",
        "gas_limit": "0x2",
    }
    chain = {
        "schema": "route_cost_chain_evidence/v1",
        "run_id": RUN_ID,
        "route_cohort_id": COHORT_ID,
        "candidate_source_generation": GENERATION,
        "route_universe_sha256": value["route_universe_sha256"],
        "selected_market_set_sha256": selected_sha,
        "chain_id": 1,
        "rpc_source_id": "rpc-mainnet-a",
        "captured_started_at": state["observed_at"],
        "captured_finished_at": state["observed_at"],
        "status": "incomplete",
        "reason_code": "gas_unavailable",
        "block_header_result": header,
        "fee_history_result": {
            "schema": "route_cost_fee_history_result/v1",
            "status": "unavailable",
            "reason_code": "gas_unavailable",
            "oldest_block": None,
            "base_fee_per_gas": None,
            "reward": None,
            "gas_used_ratio": None,
        },
        "native_price_record": {
            "schema": "route_cost_native_price_record/v1",
            "status": "unavailable",
            "reason_code": "native_price_unavailable",
            "native_symbol": "ETH",
            "wrapped_native_address": (
                "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
            ),
            "price_usd": None,
            "observed_at": None,
            "valid_until": None,
            "native_price_evidence_sha256": None,
            "source_record_sha256": None,
        },
    }
    # Chain status reason deterministically selects fee history first.
    chain_sha = physical_sha(chain)
    value["chain_evidence"] = [chain]
    value["chain_evidence_count"] = 1
    value["chain_evidence_set_sha256"] = typed_sha(
        b"route-cost-chain-evidence-set/v1\n", [chain]
    )

    # Runtime bytes are only grammar/size relevant for this core-state-stage
    # fixture; the pinned router/factory hashes are intentionally not bypassed.
    # A stage-none transcript may reference the chain without a market object.
    for transcript in value["transcripts"]:
        target = route_cost_evidence.build_simulation_targets(
            universe_for()["selected_legs"],
            {MARKET_ID: {"structural_support_status": "supported"}},
            {MARKET_ID: state},
        )[(MARKET_ID, transcript["requested_notional_usd"])]
        transcript.update({
            "adapter_registry_sha256": registry_sha,
            "selected_market_set_sha256": selected_sha,
            **target,
            "core_pool_state_id": state["state_id"],
            "core_pool_state_sha256": retained["descriptor"]["sha256"],
            "chain_evidence_sha256": chain_sha,
            "status": "unavailable",
            "completed_stage": "none",
            "reason_code": "router_identity_unavailable",
        })
    value["transcript_set_sha256"] = typed_sha(
        b"route-cost-evidence-transcript-set/v1\n", value["transcripts"]
    )
    members = []
    bindings = []
    transcript_by_scope = {
        (row["direction"], row["requested_notional_usd"]): row
        for row in value["transcripts"]
    }
    route_id = universe_for()["routes"][0]["route_id"]
    for notional in map(str, NOTIONALS):
        member = {
            "schema": "route_cost_submission_policy_member/v1",
            "route_id": route_id,
            "requested_notional_usd": notional,
            "status": "unavailable",
            "reason_code": "submission_connector_missing",
            "submission_mode": None,
            "policy_id": None,
            "buy_submission_loss_bps": None,
            "sell_submission_loss_bps": None,
        }
        members.append(member)
        bindings.append({
            "schema": "route_cost_evidence_binding/v1",
            "run_id": RUN_ID,
            "route_cohort_id": COHORT_ID,
            "candidate_source_generation": GENERATION,
            "route_universe_sha256": value["route_universe_sha256"],
            "adapter_registry_sha256": registry_sha,
            "selected_market_set_sha256": selected_sha,
            "connector_key_registry_sha256": value[
                "connector_key_registry_sha256"
            ],
            "trace_profile_generation": value["trace_profile_generation"],
            "submission_connector_profile_generation": value[
                "submission_connector_profile_generation"
            ],
            "route_id": route_id,
            "requested_notional_usd": notional,
            "buy_transcript_sha256": typed_sha(
                b"route-cost-evidence-transcript/v1\n",
                transcript_by_scope[("buy", notional)],
            ),
            "sell_transcript_sha256": None,
            "submission_policy_member_sha256": typed_sha(
                b"route-cost-submission-policy-member/v1\n", member
            ),
            "evaluated_at": EVALUATED_AT,
            "status": "unavailable",
            "reason_code": "transcript_unavailable",
        })
    value["bindings"] = bindings
    value["binding_count"] = len(bindings)
    value["binding_set_sha256"] = typed_sha(
        b"route-cost-evidence-binding-set/v1\n", bindings
    )
    value["counts"]["binding_unavailable"] = len(bindings)
    snapshot = value["submission_policy_snapshot"]
    snapshot.update({
        "adapter_registry_sha256": registry_sha,
        "selected_market_set_sha256": selected_sha,
        "member_count": len(members),
        "members": members,
        "member_set_sha256": typed_sha(
            b"route-cost-submission-policy-member-set/v1\n", members
        ),
        "status": "unavailable",
        "reason_code": "submission_connector_missing",
    })
    value["submission_policy_snapshot_sha256"] = typed_sha(
        b"route-cost-submission-policy-snapshot/v1\n", snapshot
    )
    return value, retained


def pair_descriptor(pair=POOL, *, token0=TOKEN_A, token1=TOKEN_B):
    return {
        "pair_address": pair,
        "token0_address": token0,
        "token1_address": token1,
        "pair_runtime_code_sha256": (
            "8b5db55fa9ab3b9527508d4abe0b39eb588bf310270c8e04b3f38214e8ba63b4"
        ),
        "source_metadata_sha256": "6" * 64,
    }


def adapter(*, with_funding=True, pairs=None):
    descriptors = (
        [funding_descriptor(TOKEN_A), funding_descriptor(TOKEN_B)]
        if with_funding
        else []
    )
    return {
        "adapter_id": ADAPTER_ID,
        "chain_id": 1,
        "protocol_family": "uniswap_v2_router02",
        "router_address": ROUTER,
        "factory_address": FACTORY,
        "router_runtime_code_sha256": (
            "ccef50da4af021b09ada39d78db5d281fffff81a57969c7028bccc1f50d37854"
        ),
        "factory_runtime_code_sha256": (
            "3abc53f12a9cb8ae37ebfada9efc261c1ab4c2759d161e341a49bf67df3f8321"
        ),
        "pair_fee_bps": "30",
        "gas_fee_model": "eip1559_fee_history_v1",
        "allowed_selectors": ["0x38ed1739", "0x8803dbee"],
        "supports_native": False,
        "supports_multihop": False,
        "supports_fee_on_transfer": False,
        "trace_method": "debug_traceCall_state_override_v1",
        "connector_family": "private_submission_connector/v1",
        "pair_descriptors": list(pairs or [pair_descriptor()]),
        "token_funding_descriptors": descriptors,
        "native_symbol": "ETH",
        "wrapped_native_address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
        "simulation_sender_address": SENDER,
        "native_price_reference_market_id": "cex:binance:ETH/USDT",
        "native_price_reference_adapter_id": "binance_public_spot_depth/v1",
    }


def adapter_registry(*, supported=False):
    return {
        "schema": "route_cost_adapter_registry/v1",
        "registry_version": "test-v1",
        "adapters": [adapter()] if supported else [],
    }


def connector_registry():
    return {
        "schema": "route_cost_connector_key_registry/v1",
        "registry_version": "test-v1",
        "keys": [],
    }


def signed_snapshot_kat():
    keys = {
        "schema": "route_cost_connector_key_registry/v1",
        "registry_version": "historical-kat-v1",
        "keys": [
            {
                "key_id": "kat-key-1",
                "connector_id": "connector-a",
                "algorithm": "ssh-ed25519-sshsig-v1",
                "public_key": SSHSIG_KAT_PUBLIC_KEY,
                "valid_from": "2026-07-01T00:00:00Z",
                "valid_until": "2026-09-01T00:00:00Z",
                "status": "active",
            }
        ],
    }
    links = {
        "run_id": RUN_ID,
        "route_cohort_id": COHORT_ID,
        "candidate_source_generation": GENERATION,
        "route_universe_sha256": "1" * 64,
        "adapter_registry_sha256": "2" * 64,
        "selected_market_set_sha256": "3" * 64,
        "connector_key_registry_sha256": physical_sha(keys),
        "trace_profile_generation": "4" * 64,
        "submission_connector_profile_generation": "5" * 64,
    }
    member = {
        "schema": "route_cost_submission_policy_member/v1",
        "route_id": "route-a",
        "requested_notional_usd": "1000",
        "status": "unavailable",
        "reason_code": "submission_connector_unavailable",
        "submission_mode": None,
        "policy_id": None,
        "buy_submission_loss_bps": None,
        "sell_submission_loss_bps": None,
    }
    snapshot = {
        "schema": "route_cost_submission_policy_snapshot/v1",
        **links,
        "connector_id": "connector-a",
        "member_count": 1,
        "members": [member],
        "member_set_sha256": typed_sha(
            b"route-cost-submission-policy-member-set/v1\n", [member]
        ),
        "status": "authenticated",
        "reason_code": None,
        "observed_at": "2026-08-01T12:00:00Z",
        "valid_until": "2026-08-01T12:01:00Z",
        "issuer_key_id": "kat-key-1",
        "signature_algorithm": "ssh-ed25519-sshsig-v1",
        "attested_payload_sha256": None,
        "signature": SSHSIG_KAT_SIGNATURE,
    }
    attestation = route_cost_evidence._policy_attestation(snapshot)
    snapshot["attested_payload_sha256"] = typed_sha(
        b"route-cost-submission-policy-attestation/v1\n", attestation
    )
    return snapshot, keys


def _word(number):
    return "0x{:064x}".format(number)


def _complete_raw_transcript(*, arbitrary_keys=False, short_pair_output=False):
    current_adapter = adapter()
    calldata = route_cost_evidence.build_v2_swap_calldata(
        direction="sell",
        quoted_amount_in_raw=100,
        quoted_amount_out_raw=50,
        submission_loss_bound_bps=0,
        path_token_in=TOKEN_A,
        path_token_out=TOKEN_B,
        recipient=SENDER,
        deadline=12345,
    )
    balance_a_sender = route_cost_evidence.solidity_balance_storage_key(
        SENDER, 0
    )
    allowance_a = route_cost_evidence.solidity_allowance_storage_key(
        SENDER, ROUTER, 1
    )
    balance_a_pair = route_cost_evidence.solidity_balance_storage_key(POOL, 0)
    balance_b_pair = route_cost_evidence.solidity_balance_storage_key(POOL, 0)
    balance_b_recipient = route_cost_evidence.solidity_balance_storage_key(
        SENDER, 0
    )
    storage_keys = [
        balance_a_sender,
        allowance_a,
        balance_a_pair,
        balance_b_pair,
        balance_b_recipient,
    ]
    if arbitrary_keys:
        storage_keys = ["0x" + character * 64 for character in "abcde"]
    storage_diffs = [
        {
            "token_address": TOKEN_A,
            "account_role": "sender",
            "storage_key": storage_keys[0],
            "pre_present": True,
            "pre_value": _word(1000),
            "post_present": True,
            "post_value": _word(900),
        },
        {
            "token_address": TOKEN_A,
            "account_role": "sender",
            "storage_key": storage_keys[1],
            "pre_present": True,
            "pre_value": _word(100),
            "post_present": True,
            "post_value": _word(0),
        },
        {
            "token_address": TOKEN_A,
            "account_role": "pair",
            "storage_key": storage_keys[2],
            "pre_present": True,
            "pre_value": _word(1000),
            "post_present": True,
            "post_value": _word(1100),
        },
        {
            "token_address": TOKEN_B,
            "account_role": "pair",
            "storage_key": storage_keys[3],
            "pre_present": True,
            "pre_value": _word(1000),
            "post_present": True,
            "post_value": _word(900 if short_pair_output else 950),
        },
        {
            "token_address": TOKEN_B,
            "account_role": "recipient",
            "storage_key": storage_keys[4],
            "pre_present": True,
            "pre_value": _word(0),
            "post_present": True,
            "post_value": _word(50),
        },
    ]
    overrides = {
        SENDER: {"balance": _word(10 ** 18)},
        TOKEN_A: {
            "stateDiff": {
                balance_a_sender: _word(100),
                allowance_a: _word(100),
            }
        },
    }
    estimate_request = {
        "schema": "route_cost_estimate_gas_request/v1",
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_estimateGas",
        "params": [
            {"from": SENDER, "to": ROUTER, "data": calldata, "value": "0x0"},
            "0x64",
            overrides,
        ],
    }
    estimate_response = {
        "schema": "route_cost_estimate_gas_response/v1",
        "jsonrpc": "2.0",
        "id": 1,
        "result": "0x5208",
    }
    trace_request = {
        "schema": "route_cost_trace_request/v1",
        "jsonrpc": "2.0",
        "id": 2,
        "method": "debug_traceCall",
        "params": [
            {
                "from": SENDER,
                "to": ROUTER,
                "gas": "0x5208",
                "data": calldata,
                "value": "0x0",
            },
            "0x64",
            {
                "tracer": "prestateTracer",
                "tracerConfig": {
                    "diffMode": True,
                    "disableCode": True,
                    "disableStorage": False,
                },
                "stateOverrides": overrides,
            },
        ],
    }
    trace_response = {
        "schema": "route_cost_trace_response/v1",
        "jsonrpc": "2.0",
        "id": 2,
        "storage_diffs": sorted(
            storage_diffs,
            key=lambda row: (
                row["token_address"], row["account_role"], row["storage_key"]
            ),
        ),
    }
    deltas = [
        {
            "token_address": row["token_address"],
            "account_role": row["account_role"],
            "pre_balance_raw": str(int(row["pre_value"], 16)),
            "post_balance_raw": str(int(row["post_value"], 16)),
        }
        for row in trace_response["storage_diffs"]
        if not (
            row["account_role"] == "sender"
            and row["storage_key"] == allowance_a
        )
    ]
    return {
        "schema": "route_cost_raw_transcript/v1",
        "chain_evidence_sha256": "1" * 64,
        "market_evidence_sha256": "2" * 64,
        "captured_started_at": "2026-08-01T12:00:00Z",
        "captured_finished_at": "2026-08-01T12:00:01Z",
        "calldata_hex": calldata,
        "estimate_gas_request": estimate_request,
        "estimate_gas_response": estimate_response,
        "simulation_method": current_adapter["trace_method"],
        "simulation_request": trace_request,
        "simulation_response": trace_response,
        "simulation_balance_deltas": deltas,
    }


def observed_raw_transcript(*, market_id, pool, direction, amount_in,
                            amount_out, bound_bps, deadline, block_tag,
                            chain_sha, market_sha):
    current_adapter = adapter()
    token_in, token_out = (
        (TOKEN_B, TOKEN_A) if direction == "buy" else (TOKEN_A, TOKEN_B)
    )
    calldata = route_cost_evidence.build_v2_swap_calldata(
        direction=direction,
        quoted_amount_in_raw=amount_in,
        quoted_amount_out_raw=amount_out,
        submission_loss_bound_bps=bound_bps,
        path_token_in=token_in,
        path_token_out=token_out,
        recipient=SENDER,
        deadline=deadline,
    )
    decoded = route_cost_evidence.decode_v2_swap_calldata(calldata)
    override_input = decoded.get("amount_in_raw", decoded.get("amount_in_max_raw"))
    descriptor_in = next(
        row for row in current_adapter["token_funding_descriptors"]
        if row["token_address"] == token_in
    )
    descriptor_out = next(
        row for row in current_adapter["token_funding_descriptors"]
        if row["token_address"] == token_out
    )
    sender_balance = route_cost_evidence.solidity_balance_storage_key(
        SENDER, int(descriptor_in["balance_mapping_slot"])
    )
    allowance = route_cost_evidence.solidity_allowance_storage_key(
        SENDER, ROUTER, int(descriptor_in["allowance_mapping_slot"])
    )
    pair_input = route_cost_evidence.solidity_balance_storage_key(
        pool, int(descriptor_in["balance_mapping_slot"])
    )
    pair_output = route_cost_evidence.solidity_balance_storage_key(
        pool, int(descriptor_out["balance_mapping_slot"])
    )
    recipient_output = route_cost_evidence.solidity_balance_storage_key(
        SENDER, int(descriptor_out["balance_mapping_slot"])
    )
    storage_diffs = [
        {
            "token_address": token_in,
            "account_role": "sender",
            "storage_key": sender_balance,
            "pre_present": True,
            "pre_value": _word(amount_in),
            "post_present": True,
            "post_value": _word(0),
        },
        {
            "token_address": token_in,
            "account_role": "sender",
            "storage_key": allowance,
            "pre_present": True,
            "pre_value": _word(override_input),
            "post_present": True,
            "post_value": _word(override_input - amount_in),
        },
        {
            "token_address": token_in,
            "account_role": "pair",
            "storage_key": pair_input,
            "pre_present": True,
            "pre_value": _word(10 ** 30),
            "post_present": True,
            "post_value": _word(10 ** 30 + amount_in),
        },
        {
            "token_address": token_out,
            "account_role": "pair",
            "storage_key": pair_output,
            "pre_present": True,
            "pre_value": _word(10 ** 30),
            "post_present": True,
            "post_value": _word(10 ** 30 - amount_out),
        },
        {
            "token_address": token_out,
            "account_role": "recipient",
            "storage_key": recipient_output,
            "pre_present": True,
            "pre_value": _word(0),
            "post_present": True,
            "post_value": _word(amount_out),
        },
    ]
    storage_diffs.sort(key=lambda row: (
        row["token_address"], row["account_role"], row["storage_key"]
    ))
    state_overrides = {
        SENDER: {"balance": _word(10 ** 18)},
        token_in: {
            "stateDiff": {
                sender_balance: _word(override_input),
                allowance: _word(override_input),
            }
        },
    }
    state_overrides = {
        key: state_overrides[key] for key in sorted(state_overrides)
    }
    estimate_request = {
        "schema": "route_cost_estimate_gas_request/v1",
        "jsonrpc": "2.0",
        "id": 1,
        "method": "eth_estimateGas",
        "params": [{
            "from": SENDER, "to": ROUTER, "data": calldata, "value": "0x0",
        }, block_tag, state_overrides],
    }
    estimate_response = {
        "schema": "route_cost_estimate_gas_response/v1",
        "jsonrpc": "2.0",
        "id": 1,
        "result": "0x5208",
    }
    trace_request = {
        "schema": "route_cost_trace_request/v1",
        "jsonrpc": "2.0",
        "id": 2,
        "method": "debug_traceCall",
        "params": [{
            "from": SENDER, "to": ROUTER, "gas": "0x5208",
            "data": calldata, "value": "0x0",
        }, block_tag, {
            "tracer": "prestateTracer",
            "tracerConfig": {
                "diffMode": True,
                "disableCode": True,
                "disableStorage": False,
            },
            "stateOverrides": state_overrides,
        }],
    }
    trace_response = {
        "schema": "route_cost_trace_response/v1",
        "jsonrpc": "2.0",
        "id": 2,
        "storage_diffs": storage_diffs,
    }
    balance_deltas = [
        {
            "token_address": row["token_address"],
            "account_role": row["account_role"],
            "pre_balance_raw": str(int(row["pre_value"], 16)),
            "post_balance_raw": str(int(row["post_value"], 16)),
        }
        for row in storage_diffs
        if not (
            row["account_role"] == "sender"
            and row["storage_key"] == allowance
        )
    ]
    return {
        "schema": "route_cost_raw_transcript/v1",
        "chain_evidence_sha256": chain_sha,
        "market_evidence_sha256": market_sha,
        "captured_started_at": "2026-08-01T12:00:01Z",
        "captured_finished_at": "2026-08-01T12:00:02Z",
        "calldata_hex": calldata,
        "estimate_gas_request": estimate_request,
        "estimate_gas_response": estimate_response,
        "simulation_method": current_adapter["trace_method"],
        "simulation_request": trace_request,
        "simulation_response": trace_response,
        "simulation_balance_deltas": balance_deltas,
    }


def native_price_evidence(universe_sha=None):
    book_raw, rules_raw = native_price_captured_bytes()
    return route_cost_evidence.build_native_price_evidence_from_captured(
        run_id=RUN_ID,
        route_cohort_id=COHORT_ID,
        candidate_source_generation=GENERATION,
        book_raw_response=book_raw,
        book_observed_at="2026-08-01T12:00:00Z",
        market_rules_raw_response=rules_raw,
        market_rules_observed_at="2026-08-01T11:59:59Z",
    )


def native_price_captured_bytes():
    book_raw = (
        b'{"asks":[["3000.00","10.0000"],["3001","2"]],'
        b'"bids":[["2999","10"]],"lastUpdateId":1}'
    )
    rules_raw = (
        b'{"serverTime":1785585600000,"symbols":[{"baseAsset":"ETH",'
        b'"baseAssetPrecision":8,"filters":[{"filterType":"PRICE_FILTER",'
        b'"tickSize":"0.01000000"},{"filterType":"LOT_SIZE",'
        b'"minQty":"0.00010000","stepSize":"0.00010000"},'
        b'{"filterType":"MIN_NOTIONAL","minNotional":"5.00000000"}],'
        b'"quoteAsset":"USDT","quoteAssetPrecision":8,"status":"TRADING",'
        b'"symbol":"ETHUSDT"}]}'
    )
    return book_raw, rules_raw


class NativePricePureProducerTests(unittest.TestCase):
    def _build(self, **overrides):
        book_raw, rules_raw = native_price_captured_bytes()
        arguments = {
            "run_id": RUN_ID,
            "route_cohort_id": COHORT_ID,
            "candidate_source_generation": GENERATION,
            "book_raw_response": book_raw,
            "book_observed_at": "2026-08-01T12:00:00Z",
            "market_rules_raw_response": rules_raw,
            "market_rules_observed_at": "2026-08-01T11:59:59Z",
        }
        arguments.update(overrides)
        return route_cost_evidence.build_native_price_evidence_from_captured(
            **arguments
        )

    def test_builds_exact_two_raw_sealed_native_evidence(self):
        book_raw, rules_raw = native_price_captured_bytes()
        value = self._build()
        self.assertEqual(set(value), set(route_cost_evidence.NATIVE_PRICE_EVIDENCE_FIELDS))
        self.assertEqual(value["source_market_id"], "cex:binance:ETH/USDT")
        self.assertEqual(value["source_adapter_id"], "binance_public_spot_depth/v1")
        self.assertEqual(value["source_endpoint_id"], "binance-public-spot-depth-v1")
        self.assertEqual(value["book_projection"], {
            "schema": "route_cost_native_price_book/v1",
            "market_id": "cex:binance:ETH/USDT",
            "adapter_id": "binance_public_spot_depth/v1",
            "best_ask_price": "3000",
            "best_ask_quantity": "10",
            "observed_at": "2026-08-01T12:00:00Z",
            "raw_response_sha256": hashlib.sha256(book_raw).hexdigest(),
        })
        self.assertEqual(value["market_rules_projection"], {
            "schema": "route_cost_native_price_market_rules/v1",
            "market_id": "cex:binance:ETH/USDT",
            "price_tick": "0.01",
            "quantity_step": "0.0001",
            "min_quantity": "0.0001",
            "min_notional": "5",
            "observed_at": "2026-08-01T11:59:59Z",
            "source_record_sha256": hashlib.sha256(rules_raw).hexdigest(),
        })
        conversion = value["usd_conversion_projection"]
        self.assertEqual(conversion["quote_asset"], "USDT")
        self.assertEqual(conversion["usd_asset"], "USD")
        self.assertEqual(conversion["rate"], "1")
        self.assertEqual(conversion["observed_at"], "2026-08-01T12:00:00Z")
        self.assertEqual(conversion["valid_until"], "2026-08-01T12:00:59Z")
        self.assertEqual(
            value["market_rules_raw_response_base64"],
            base64.b64encode(rules_raw).decode("ascii"),
        )
        self.assertEqual(
            value["market_rules_raw_response_sha256"],
            hashlib.sha256(rules_raw).hexdigest(),
        )
        self.assertEqual(value["book_request_receipt"], {
            "schema": "route_cost_native_price_request_receipt/v1",
            "request_role": "book",
            "request_method": "GET",
            "source_endpoint_id": "binance-public-spot-depth-v1",
            "request_path": "/api/v3/depth",
            "request_query": "symbol=ETHUSDT&limit=100",
            "captured_at": "2026-08-01T12:00:00Z",
            "raw_response_sha256": hashlib.sha256(book_raw).hexdigest(),
            "projection_sha256": typed_sha(
                b"route-cost-native-price-book-projection/v1\n",
                value["book_projection"],
            ),
        })
        self.assertEqual(value["market_rules_request_receipt"], {
            "schema": "route_cost_native_price_request_receipt/v1",
            "request_role": "market_rules",
            "request_method": "GET",
            "source_endpoint_id": "binance-public-spot-exchange-info-v1",
            "request_path": "/api/v3/exchangeInfo",
            "request_query": "symbol=ETHUSDT",
            "captured_at": "2026-08-01T11:59:59Z",
            "raw_response_sha256": hashlib.sha256(rules_raw).hexdigest(),
            "projection_sha256": typed_sha(
                b"route-cost-native-price-market-rules-projection/v1\n",
                value["market_rules_projection"],
            ),
        })
        links = {
            "run_id": RUN_ID,
            "route_cohort_id": COHORT_ID,
            "candidate_source_generation": GENERATION,
        }
        self.assertEqual(
            route_cost_evidence._validate_native_price_evidence(value, links),
            typed_sha(b"route-cost-native-price-evidence/v1\n", value),
        )
        transplant = copy.deepcopy(value)
        transplant["book_request_receipt"] = copy.deepcopy(
            value["market_rules_request_receipt"]
        )
        with self.assertRaisesRegex(
            route_cost_evidence.RouteCostEvidenceError, "request receipt"
        ):
            route_cost_evidence._validate_native_price_evidence(
                transplant, links
            )

    def test_public_signature_exposes_only_lineage_two_raw_and_timestamps(self):
        signature = inspect.signature(
            route_cost_evidence.build_native_price_evidence_from_captured
        )
        self.assertEqual(set(signature.parameters), {
            "run_id", "route_cohort_id", "candidate_source_generation",
            "book_raw_response", "book_observed_at",
            "market_rules_raw_response", "market_rules_observed_at",
        })

    def test_duplicate_json_keys_are_rejected_in_both_raw_sources(self):
        book_raw, rules_raw = native_price_captured_bytes()
        duplicate_book = book_raw[:-1] + b',"asks":[["3002","1"]]}'
        duplicate_rules = rules_raw[:-1] + b',"symbols":[]}'
        for arguments in (
            {"book_raw_response": duplicate_book},
            {"market_rules_raw_response": duplicate_rules},
        ):
            with self.subTest(arguments=sorted(arguments)):
                with self.assertRaisesRegex(
                    route_cost_evidence.RouteCostEvidenceError, "duplicate"
                ):
                    self._build(**arguments)

    def test_raw_transport_bytes_need_not_be_canonical_json(self):
        book_raw, rules_raw = native_price_captured_bytes()
        pretty_book = json.dumps(
            json.loads(book_raw), indent=2
        ).encode("utf-8")
        pretty_rules = json.dumps(
            json.loads(rules_raw), indent=1
        ).encode("utf-8")
        value = self._build(
            book_raw_response=pretty_book,
            market_rules_raw_response=pretty_rules,
        )
        self.assertEqual(
            value["raw_response_sha256"], hashlib.sha256(pretty_book).hexdigest()
        )
        self.assertEqual(
            value["market_rules_raw_response_sha256"],
            hashlib.sha256(pretty_rules).hexdigest(),
        )

    def test_rejects_crossed_book_and_wrong_rules_identity(self):
        book_raw, rules_raw = native_price_captured_bytes()
        crossed = book_raw.replace(b'"2999"', b'"3000"')
        transplanted = rules_raw.replace(b'"ETHUSDT"', b'"BTCUSDT"')
        mixed = json.loads(rules_raw)
        extra = copy.deepcopy(mixed["symbols"][0])
        extra.update({
            "symbol": "BTCUSDT",
            "baseAsset": "BTC",
        })
        mixed["symbols"].append(extra)
        for expected, arguments in (
            ("crossed", {"book_raw_response": crossed}),
            ("identity|instrument", {"market_rules_raw_response": transplanted}),
            (
                "identity|instrument|closed",
                {"market_rules_raw_response": canonical_bytes(mixed)},
            ),
        ):
            with self.subTest(expected=expected):
                with self.assertRaisesRegex(
                    route_cost_evidence.RouteCostEvidenceError, expected
                ):
                    self._build(**arguments)

    def test_rejects_rule_filter_drift_and_duplicate_filters(self):
        _book_raw, rules_raw = native_price_captured_bytes()
        parsed = json.loads(rules_raw)
        cases = []
        for filter_type, field, value in (
            ("PRICE_FILTER", "tickSize", "0"),
            ("LOT_SIZE", "stepSize", "0"),
            ("LOT_SIZE", "minQty", "-0"),
            ("MIN_NOTIONAL", "minNotional", "0"),
        ):
            mutated = copy.deepcopy(parsed)
            target = next(
                item for item in mutated["symbols"][0]["filters"]
                if item["filterType"] == filter_type
            )
            target[field] = value
            cases.append((filter_type + "." + field, canonical_bytes(mutated)))
        duplicated = copy.deepcopy(parsed)
        duplicated["symbols"][0]["filters"].append(
            copy.deepcopy(duplicated["symbols"][0]["filters"][0])
        )
        cases.append(("duplicate", canonical_bytes(duplicated)))
        for label, raw in cases:
            with self.subTest(label=label):
                with self.assertRaises(route_cost_evidence.RouteCostEvidenceError):
                    self._build(market_rules_raw_response=raw)

    def test_best_ask_must_obey_tick_step_and_minima(self):
        book_raw, _rules_raw = native_price_captured_bytes()
        parsed = json.loads(book_raw)
        cases = (
            ("tick", "3000.005", "10"),
            ("step", "3000", "10.00005"),
            ("min quantity", "3000", "0.00005"),
            ("min notional", "3000", "0.001"),
        )
        for label, price, quantity in cases:
            with self.subTest(label=label):
                mutated = copy.deepcopy(parsed)
                mutated["asks"][0] = [price, quantity]
                with self.assertRaisesRegex(
                    route_cost_evidence.RouteCostEvidenceError,
                    "tick|step|min",
                ):
                    self._build(book_raw_response=canonical_bytes(mutated))

    def test_exact_field_contract_is_literal(self):
        self.assertEqual(
            route_cost_evidence.NATIVE_PRICE_EVIDENCE_FIELDS,
            (
                "schema", "run_id", "route_cohort_id",
                "candidate_source_generation", "source_market_id",
                "source_adapter_id", "source_endpoint_id", "book_projection",
                "market_rules_projection", "usd_conversion_projection",
                "book_request_receipt", "market_rules_request_receipt",
                "raw_response_base64", "raw_response_sha256",
                "market_rules_raw_response_base64",
                "market_rules_raw_response_sha256", "observed_at",
                "valid_until", "source_record_sha256",
                "capture_binding_sha256",
            ),
        )

    def test_native_json_preflight_limits_are_exact_and_pre_materialization(self):
        preflight = route_cost_evidence._preflight_native_json_bytes
        passing_and_overflow = (
            (
                {"maximum_depth": 3},
                b"[[[0]]]",
                b"[[[[0]]]]",
            ),
            (
                {"node_limit": 3},
                b"[0,0]",
                b"[0,0,0]",
            ),
            (
                {"scalar_limit": 2},
                b'["ab"]',
                b'["abc"]',
            ),
            (
                {"ordinary_string_limit": 2, "scalar_limit": 10},
                b'["ab"]',
                b'["abc"]',
            ),
            (
                {"number_token_limit": 4, "scalar_limit": 10},
                b"[1234]",
                b"[12345]",
            ),
        )
        for limits, exact, overflow in passing_and_overflow:
            with self.subTest(limits=limits):
                preflight(exact, label="native test", **limits)
                with self.assertRaises(route_cost_evidence.RouteCostEvidenceError):
                    preflight(overflow, label="native test", **limits)
        preflight(b'["\\ud83d\\ude00"]', label="native test")
        with self.assertRaises(route_cost_evidence.RouteCostEvidenceError):
            preflight(b'["\\ud800"]', label="native test")

    def test_deep_native_json_closes_as_typed_error_before_json_loads(self):
        _book_raw, rules_raw = native_price_captured_bytes()
        parsed = json.loads(rules_raw)
        nested = 0
        for _index in range(route_cost_evidence.MAX_NATIVE_PRICE_JSON_DEPTH + 1):
            nested = [nested]
        parsed["ignored"] = nested
        with self.assertRaises(route_cost_evidence.RouteCostEvidenceError) as raised:
            self._build(market_rules_raw_response=canonical_bytes(parsed))
        self.assertNotIsInstance(raised.exception.__cause__, RecursionError)

    def test_native_book_decimal_inputs_reject_exponents_numbers_and_signed_zero(self):
        book_raw, _rules_raw = native_price_captured_bytes()
        parsed = json.loads(book_raw)
        for value in ("1e-1000000", "-0", 3000):
            with self.subTest(value=value):
                mutated = copy.deepcopy(parsed)
                mutated["asks"][0][0] = value
                with self.assertRaises(route_cost_evidence.RouteCostEvidenceError):
                    self._build(book_raw_response=canonical_bytes(mutated))

    def test_microsecond_receipts_retain_exact_plus_sixty_validity(self):
        value = self._build(
            book_observed_at="2026-08-01T12:00:00.123456Z",
            market_rules_observed_at="2026-08-01T11:59:59.654321Z",
        )
        self.assertEqual(value["observed_at"], "2026-08-01T12:00:00.123456Z")
        self.assertEqual(value["valid_until"], "2026-08-01T12:00:59.654321Z")
        self.assertEqual(
            value["book_request_receipt"]["captured_at"],
            "2026-08-01T12:00:00.123456Z",
        )

    def test_full_rehash_cannot_turn_a_btc_receipt_into_eth_evidence(self):
        value = self._build()
        forged = copy.deepcopy(value)
        forged["book_request_receipt"]["request_query"] = (
            "symbol=BTCUSDT&limit=100"
        )
        source_projection = {
            "book": forged["book_projection"],
            "book_request_receipt": forged["book_request_receipt"],
            "market_rules": forged["market_rules_projection"],
            "market_rules_request_receipt": forged[
                "market_rules_request_receipt"
            ],
            "usd_conversion": forged["usd_conversion_projection"],
        }
        forged["source_record_sha256"] = typed_sha(
            b"route-cost-native-price-source/v1\n", source_projection
        )
        capture_projection = {
            "schema": "route_cost_native_price_capture_binding/v1",
            "run_id": forged["run_id"],
            "route_cohort_id": forged["route_cohort_id"],
            "candidate_source_generation": forged[
                "candidate_source_generation"
            ],
            "source_market_id": forged["source_market_id"],
            "source_adapter_id": forged["source_adapter_id"],
            "book_request_receipt_sha256": typed_sha(
                b"route-cost-native-price-request-receipt/v1\n",
                forged["book_request_receipt"],
            ),
            "market_rules_request_receipt_sha256": typed_sha(
                b"route-cost-native-price-request-receipt/v1\n",
                forged["market_rules_request_receipt"],
            ),
            "source_record_sha256": forged["source_record_sha256"],
        }
        forged["capture_binding_sha256"] = typed_sha(
            b"route-cost-native-price-capture-binding/v1\n",
            capture_projection,
        )
        links = {
            "run_id": RUN_ID,
            "route_cohort_id": COHORT_ID,
            "candidate_source_generation": GENERATION,
        }
        with self.assertRaisesRegex(
            route_cost_evidence.RouteCostEvidenceError, "request receipt"
        ):
            route_cost_evidence._validate_native_price_evidence(forged, links)

    def test_rejects_book_and_rules_one_byte_over_limits(self):
        for expected, arguments in (
            (
                "book.*byte limit",
                {"book_raw_response": b" " * (2 * 1024 * 1024 + 1)},
            ),
            (
                "rules.*byte limit",
                {"market_rules_raw_response": b" " * (256 * 1024 + 1)},
            ),
        ):
            with self.subTest(expected=expected):
                with self.assertRaisesRegex(
                    route_cost_evidence.RouteCostEvidenceError, expected
                ):
                    self._build(**arguments)

    def test_accepts_raw_sources_at_exact_byte_limits(self):
        book_raw, rules_raw = native_price_captured_bytes()
        book_at_limit = book_raw[:-1] + (
            b" " * (route_cost_evidence.MAX_NATIVE_PRICE_RAW_BYTES - len(book_raw))
        ) + b"}"
        rules_at_limit = rules_raw[:-1] + (
            b" " * (
                route_cost_evidence.MAX_NATIVE_PRICE_MARKET_RULES_RAW_BYTES
                - len(rules_raw)
            )
        ) + b"}"
        value = self._build(
            book_raw_response=book_at_limit,
            market_rules_raw_response=rules_at_limit,
        )
        self.assertEqual(
            len(base64.b64decode(value["raw_response_base64"])),
            route_cost_evidence.MAX_NATIVE_PRICE_RAW_BYTES,
        )
        self.assertEqual(
            len(base64.b64decode(value["market_rules_raw_response_base64"])),
            route_cost_evidence.MAX_NATIVE_PRICE_MARKET_RULES_RAW_BYTES,
        )

    def test_validator_replays_every_projection_and_raw_binding(self):
        original = self._build()
        links = {
            "run_id": RUN_ID,
            "route_cohort_id": COHORT_ID,
            "candidate_source_generation": GENERATION,
        }
        mutations = (
            ("lineage", lambda value: value.__setitem__("run_id", "other-run")),
            (
                "endpoint",
                lambda value: value.__setitem__("source_endpoint_id", "other"),
            ),
            (
                "book projection",
                lambda value: value["book_projection"].__setitem__(
                    "best_ask_price", "3001"
                ),
            ),
            (
                "market-rules projection",
                lambda value: value["market_rules_projection"].__setitem__(
                    "quantity_step", "0.001"
                ),
            ),
            (
                "USD conversion projection",
                lambda value: value["usd_conversion_projection"].__setitem__(
                    "rate", "2"
                ),
            ),
            (
                "rules raw hash",
                lambda value: value.__setitem__(
                    "market_rules_raw_response_sha256", "0" * 64
                ),
            ),
            (
                "source hash",
                lambda value: value.__setitem__("source_record_sha256", "0" * 64),
            ),
        )
        for label, mutate in mutations:
            with self.subTest(label=label):
                forged = copy.deepcopy(original)
                mutate(forged)
                with self.assertRaises(route_cost_evidence.RouteCostEvidenceError):
                    route_cost_evidence._validate_native_price_evidence(
                        forged, links
                    )

    def test_validator_rejects_noncanonical_base64(self):
        value = self._build()
        links = {
            "run_id": RUN_ID,
            "route_cohort_id": COHORT_ID,
            "candidate_source_generation": GENERATION,
        }
        for field in (
            "raw_response_base64",
            "market_rules_raw_response_base64",
        ):
            with self.subTest(field=field):
                forged = copy.deepcopy(value)
                forged[field] = forged[field] + "\n"
                with self.assertRaises(route_cost_evidence.RouteCostEvidenceError):
                    route_cost_evidence._validate_native_price_evidence(
                        forged, links
                    )

    def test_rejects_invalid_timestamps_and_collapsed_validity(self):
        for arguments in (
            {"book_observed_at": "not-a-time"},
            {
                "book_observed_at": "2026-08-01T12:01:00Z",
                "market_rules_observed_at": "2026-08-01T11:59:59Z",
            },
        ):
            with self.subTest(arguments=arguments):
                with self.assertRaises(route_cost_evidence.RouteCostEvidenceError):
                    self._build(**arguments)


def _generate_ephemeral_signing_key(directory):
    key_path = Path(directory) / "kat"
    subprocess.run(
        [
            "/usr/bin/ssh-keygen", "-q", "-t", "ed25519", "-N", "",
            "-f", str(key_path),
        ],
        check=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env={"LC_ALL": "C", "PATH": "/usr/bin:/bin"},
    )
    public_key = " ".join(
        key_path.with_suffix(".pub").read_text(
            encoding="ascii"
        ).strip().split(" ")[:2]
    )
    return key_path, public_key


def _write_ephemeral_sshsig(snapshot, *, key_path=None):
    with tempfile.TemporaryDirectory() as directory:
        generated_key = key_path is None
        if generated_key:
            key_path, _public_key = _generate_ephemeral_signing_key(directory)
        else:
            key_path = Path(key_path)
        public_key = key_path.with_suffix(".pub").read_text(
            encoding="ascii"
        ).strip().split(" ")[:2]
        public_key_text = " ".join(public_key)
        payload_path = Path(directory) / "attestation.json"
        payload_path.write_bytes(canonical_bytes(
            route_cost_evidence._policy_attestation(snapshot)
        ))
        subprocess.run(
            [
                "/usr/bin/ssh-keygen", "-Y", "sign", "-q", "-f",
                str(key_path), "-n", "route-cost-submission-policy-v1",
                str(payload_path),
            ],
            check=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env={"LC_ALL": "C", "PATH": "/usr/bin:/bin"},
        )
        signature = Path(str(payload_path) + ".sig").read_text(
            encoding="ascii"
        ).strip()
    return public_key_text, signature


def supported_observed_manifest(*, signing_key_path=None,
                                signing_public_key=None):
    pool2 = "0x" + "4" * 40
    market2 = "dex:eth:uniswap_v2:{}:AAA".format(pool2)
    leg1 = copy.deepcopy(universe_for()["selected_legs"][0])
    leg1["selection_inputs"]["dex_24h_usd"] = "900"
    leg1["selection_inputs"]["dex_tvl_usd"] = "1000"
    leg2 = copy.deepcopy(leg1)
    leg2["market_id"] = market2
    leg2["selection_rank"] = 2
    leg2["selection_inputs"]["dex_24h_usd"] = "800"
    leg2["selection_inputs"]["dex_tvl_usd"] = "900"
    route_id = "route:AAA:{}->{}:atomic_onchain".format(
        MARKET_ID, market2
    )
    universe = universe_for(
        markets=[leg1, leg2],
        routes=[{
            "route_id": route_id,
            "token_symbol": "AAA",
            "buy_market_id": MARKET_ID,
            "sell_market_id": market2,
            "route_mode": "atomic_onchain",
            "route_class": "candidate",
            "settlement_reason": None,
            "requested_notionals_usd": list(NOTIONALS),
            "candidate_source_generation": GENERATION,
            "buy_reference_volume_usd": "900",
            "sell_reference_volume_usd": "800",
            "route_volume_usd": "800",
            "route_volume_basis": "minimum_leg_source_horizon_usd",
        }],
    )
    universe_sha = physical_sha(universe)
    registry = adapter_registry(supported=True)
    registry["adapters"][0]["pair_descriptors"] = [
        pair_descriptor(),
        pair_descriptor(pool2),
    ]
    registry_sha = physical_sha(registry)
    signing_directory = None
    if signing_key_path is None:
        signing_directory = tempfile.TemporaryDirectory()
        signing_key, public_key = _generate_ephemeral_signing_key(
            signing_directory.name
        )
    else:
        signing_key = Path(signing_key_path)
        public_key = signing_public_key
        if not isinstance(public_key, str):
            raise AssertionError("supported KAT signing public key is absent")
    selected = route_cost_evidence.build_selected_markets(universe, registry)
    selected_sha = physical_sha({
        "schema": "route_cost_selected_markets/v1", "members": selected,
    })
    retained = {
        MARKET_ID: retained_v2_pool_state(
            reserve0_raw=10 ** 24, reserve1_raw=2 * 10 ** 24
        ),
        market2: retained_v2_pool_state(
            market_id=market2, pool_address=pool2,
            reserve0_raw=2 * 10 ** 24, reserve1_raw=3 * 10 ** 24,
        ),
    }
    states = {
        market_id: json.loads(member["payload"])
        for market_id, member in retained.items()
    }
    targets = route_cost_evidence.build_simulation_targets(
        universe["selected_legs"],
        {row["market_id"]: row for row in selected},
        states,
    )
    trace_identity, trace_generation = route_cost_evidence.trace_profile_identity({
        "schema": "route_cost_trace_rpc_profile/v1",
        "profile_id": "kat-trace",
        "endpoint_id": "kat-rpc",
        "rpc_url": "https://rpc.example.invalid/v1",
        "authorization": "Bearer private-not-serialized",
    })
    connector_identity, connector_generation = route_cost_evidence.submission_connector_profile_identity({
        "schema": "route_cost_submission_connector_profile/v1",
        "profile_id": "kat-connector",
        "connector_id": "kat_connector",
        "endpoint_url": "https://connector.example.invalid",
        "authorization": "Bearer private-not-serialized",
    })
    keys = {
        "schema": "route_cost_connector_key_registry/v1",
        "registry_version": "supported-kat-v1",
        "keys": [{
            "key_id": "kat-key",
            "connector_id": "kat_connector",
            "algorithm": "ssh-ed25519-sshsig-v1",
            "public_key": public_key,
            "valid_from": "2026-07-01T00:00:00Z",
            "valid_until": "2026-09-01T00:00:00Z",
            "status": "active",
        }],
    }
    key_sha = physical_sha(keys)
    native = native_price_evidence()
    native_sha = typed_sha(b"route-cost-native-price-evidence/v1\n", native)
    header = {
        "number": "0x64",
        "hash": "0x" + "7" * 64,
        "parent_hash": "0x" + "a" * 64,
        "timestamp": hex(1785585600),
        "base_fee_per_gas": "0x64",
        "gas_used": "0x1",
        "gas_limit": "0x2",
    }
    fee_history = {
        "schema": "route_cost_fee_history_result/v1",
        "status": "observed",
        "reason_code": None,
        "oldest_block": "0x64",
        "base_fee_per_gas": ["0x64", "0x64"],
        "reward": [["0x3"]],
        "gas_used_ratio": ["0.5"],
    }
    links = {
        "run_id": RUN_ID,
        "route_cohort_id": COHORT_ID,
        "candidate_source_generation": GENERATION,
        "route_universe_sha256": universe_sha,
        "adapter_registry_sha256": registry_sha,
        "selected_market_set_sha256": selected_sha,
        "connector_key_registry_sha256": key_sha,
        "trace_profile_generation": trace_generation,
        "submission_connector_profile_generation": connector_generation,
    }
    transcript_links = {
        field: links[field]
        for field in (
            "run_id", "route_cohort_id", "candidate_source_generation",
            "route_universe_sha256", "adapter_registry_sha256",
            "selected_market_set_sha256", "trace_profile_generation",
            "submission_connector_profile_generation",
        )
    }
    chain = {
        "schema": "route_cost_chain_evidence/v1",
        "run_id": RUN_ID,
        "route_cohort_id": COHORT_ID,
        "candidate_source_generation": GENERATION,
        "route_universe_sha256": universe_sha,
        "selected_market_set_sha256": selected_sha,
        "chain_id": 1,
        "rpc_source_id": "kat-rpc",
        "captured_started_at": "2026-08-01T12:00:00Z",
        "captured_finished_at": "2026-08-01T12:00:01Z",
        "status": "observed",
        "reason_code": None,
        "block_header_result": header,
        "fee_history_result": fee_history,
        "native_price_record": {
            "schema": "route_cost_native_price_record/v1",
            "status": "observed",
            "reason_code": None,
            "native_symbol": "ETH",
            "wrapped_native_address": (
                "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
            ),
            "price_usd": "3000",
            "observed_at": native["observed_at"],
            "valid_until": native["valid_until"],
            "native_price_evidence_sha256": native_sha,
            "source_record_sha256": native["source_record_sha256"],
        },
    }
    chain_sha = physical_sha(chain)
    router_hex = "0x" + pinned_runtime_code(
        "uniswap-v2-router02-runtime"
    ).hex()
    factory_hex = "0x" + pinned_runtime_code(
        "uniswap-v2-factory-runtime"
    ).hex()
    pair_hex = "0x" + pinned_pair_runtime_code().hex()
    markets = []
    for index, market_id in enumerate((MARKET_ID, market2), start=1):
        state = states[market_id]
        pool = state["pool_address"]
        market = {
            "schema": "route_cost_market_evidence/v1",
            "run_id": RUN_ID,
            "route_cohort_id": COHORT_ID,
            "candidate_source_generation": GENERATION,
            "route_universe_sha256": universe_sha,
            "adapter_registry_sha256": registry_sha,
            "selected_market_set_sha256": selected_sha,
            "market_id": market_id,
            "adapter_id": ADAPTER_ID,
            "chain_evidence_sha256": chain_sha,
            "core_pool_state_id": state["state_id"],
            "core_pool_state_sha256": retained[market_id]["descriptor"]["sha256"],
            "router_address": ROUTER,
            "router_runtime_code": router_hex,
            "factory_address": FACTORY,
            "factory_runtime_code": factory_hex,
            "factory_get_pair_request": {
                "schema": "route_cost_factory_get_pair_request/v1",
                "jsonrpc": "2.0",
                "id": index,
                "method": "eth_call",
                "params": [{
                    "to": FACTORY,
                    "data": route_cost_evidence.build_factory_get_pair_calldata(
                        TOKEN_A, TOKEN_B
                    ),
                }, "0x64"],
            },
            "factory_get_pair_response": {
                "schema": "route_cost_factory_get_pair_response/v1",
                "jsonrpc": "2.0",
                "id": index,
                "result": "0x" + "0" * 24 + pool[2:],
            },
            "pair_address": pool,
            "pair_runtime_code": pair_hex,
            "pair_token0": TOKEN_A,
            "pair_token1": TOKEN_B,
            "token_runtime_code_evidence": runtime_code_evidence(
                (TOKEN_A, TOKEN_B)
            ),
            "captured_started_at": "2026-08-01T12:00:00Z",
            "captured_finished_at": "2026-08-01T12:00:01Z",
        }
        markets.append(market)
    markets.sort(key=lambda row: row["market_id"])
    market_by_id = {row["market_id"]: row for row in markets}
    market_sha_by_id = {
        market_id: physical_sha(row) for market_id, row in market_by_id.items()
    }
    transcripts = []
    transcript_by_scope = {}
    for market_id in sorted(market_by_id):
        pool = states[market_id]["pool_address"]
        reserve_target = int(states[market_id]["reserve0_raw"])
        reserve_other = int(states[market_id]["reserve1_raw"])
        for direction in ("buy", "sell"):
            for notional in map(str, NOTIONALS):
                target = targets[(market_id, notional)]
                target_raw = int(target["simulation_target_raw_quantity"])
                if direction == "sell":
                    amount_in = target_raw
                    amount_with_fee = target_raw * 9970
                    amount_out = (
                        amount_with_fee * reserve_other
                        // (reserve_target * 10000 + amount_with_fee)
                    )
                else:
                    amount_out = target_raw
                    amount_in = (
                        reserve_other * target_raw * 10000
                        // ((reserve_target - target_raw) * 9970)
                        + 1
                    )
                raw = observed_raw_transcript(
                    market_id=market_id,
                    pool=pool,
                    direction=direction,
                    amount_in=amount_in,
                    amount_out=amount_out,
                    bound_bps=0,
                    deadline=1785585900,
                    block_tag="0x64",
                    chain_sha=chain_sha,
                    market_sha=market_sha_by_id[market_id],
                )
                transcript = {
                    "schema": "route_cost_evidence_transcript/v1",
                    **transcript_links,
                    "market_id": market_id,
                    "direction": direction,
                    "requested_notional_usd": notional,
                    "adapter_id": ADAPTER_ID,
                    **target,
                    "core_pool_state_id": states[market_id]["state_id"],
                    "core_pool_state_sha256": retained[market_id][
                        "descriptor"
                    ]["sha256"],
                    "chain_evidence_sha256": chain_sha,
                    "market_evidence_sha256": market_sha_by_id[market_id],
                    "status": "observed",
                    "completed_stage": "transfer_tax",
                    "reason_code": None,
                    "block_evidence": {
                        "schema": "route_cost_block_evidence/v1",
                        "chain_evidence_sha256": chain_sha,
                        "market_evidence_sha256": market_sha_by_id[market_id],
                        "chain_id": 1,
                        "block_tag": "0x64",
                        "block_number": "0x64",
                        "block_hash": header["hash"],
                        "block_timestamp": header["timestamp"],
                        "core_pool_state_id": states[market_id]["state_id"],
                        "router_runtime_code_sha256": (
                            route_cost_evidence.ETHEREUM_V2_ROUTER_RUNTIME_CODE_SHA256
                        ),
                        "factory_runtime_code_sha256": (
                            route_cost_evidence.ETHEREUM_V2_FACTORY_RUNTIME_CODE_SHA256
                        ),
                        "pair_runtime_code_sha256": hashlib.sha256(
                            pinned_pair_runtime_code()
                        ).hexdigest(),
                        "rpc_transcript_sha256": typed_sha(
                            b"route-cost-rpc-transcript/v1\n",
                            {
                                "estimate_request": raw["estimate_gas_request"],
                                "estimate_response": raw["estimate_gas_response"],
                                "trace_request": raw["simulation_request"],
                                "trace_response": raw["simulation_response"],
                            },
                        ),
                    },
                    "call_evidence": {
                        "schema": "route_cost_call_evidence/v1",
                        "selector": (
                            "0x8803dbee" if direction == "buy" else "0x38ed1739"
                        ),
                        "path_token_in": TOKEN_B if direction == "buy" else TOKEN_A,
                        "path_token_out": TOKEN_A if direction == "buy" else TOKEN_B,
                        "recipient_policy": "same_as_registry_sender/v1",
                        "deadline": hex(1785585900),
                        "amount_in_raw": str(amount_in),
                        "amount_out_raw": str(amount_out),
                        "calldata_sha256": hashlib.sha256(
                            bytes.fromhex(raw["calldata_hex"][2:])
                        ).hexdigest(),
                        "sender_policy": "registry_fixed_state_override_sender/v1",
                        "allowance_basis": "exact_amount_state_override/v1",
                        "submission_loss_bound_bps": "0",
                    },
                    "gas_evidence": {
                        "schema": "route_cost_gas_evidence/v1",
                        "gas_units": str(int("0x5208", 16)),
                        "max_fee_per_gas_wei": "203",
                        "fee_history_sha256": physical_sha(fee_history),
                        "native_symbol": "ETH",
                        "native_price_usd": "3000",
                        "native_price_sha256": native_sha,
                        "observed_at": native["observed_at"],
                        "valid_until": native["valid_until"],
                    },
                    "router_fee_evidence": {
                        "schema": "route_cost_router_fee_evidence/v1",
                        "status": "not_applicable",
                        "rate_bps": None,
                        "basis_code": "verified_uniswap_v2_router02_no_integrator_fee/v1",
                        "source_record_sha256": market_sha_by_id[market_id],
                    },
                    "transfer_tax_evidence": {
                        "schema": "route_cost_transfer_tax_evidence/v1",
                        "status": "not_applicable",
                        "rate_bps": None,
                        "pre_input_balance": str(amount_in),
                        "post_input_balance": "0",
                        "pre_output_balance": "0",
                        "post_output_balance": str(amount_out),
                        "trace_method": "debug_traceCall_state_override_v1",
                        "trace_sha256": typed_sha(
                            b"route-cost-trace/v1\n",
                            {
                                "request": raw["simulation_request"],
                                "response": raw["simulation_response"],
                            },
                        ),
                    },
                    "raw_transcript": raw,
                }
                transcripts.append(transcript)
                transcript_by_scope[(market_id, direction, notional)] = transcript
    members = [
        {
            "schema": "route_cost_submission_policy_member/v1",
            "route_id": route_id,
            "requested_notional_usd": notional,
            "status": "observed",
            "reason_code": None,
            "submission_mode": "private_relay",
            "policy_id": "kat-policy",
            "buy_submission_loss_bps": "0",
            "sell_submission_loss_bps": "0",
        }
        for notional in map(str, NOTIONALS)
    ]
    snapshot = {
        "schema": "route_cost_submission_policy_snapshot/v1",
        **links,
        "connector_id": "kat_connector",
        "member_count": len(members),
        "members": members,
        "member_set_sha256": typed_sha(
            b"route-cost-submission-policy-member-set/v1\n", members
        ),
        "status": "authenticated",
        "reason_code": None,
        "observed_at": "2026-08-01T12:00:00Z",
        "valid_until": "2026-08-01T12:05:00Z",
        "issuer_key_id": "kat-key",
        "signature_algorithm": "ssh-ed25519-sshsig-v1",
        "attested_payload_sha256": None,
        "signature": None,
    }
    snapshot["attested_payload_sha256"] = typed_sha(
        b"route-cost-submission-policy-attestation/v1\n",
        route_cost_evidence._policy_attestation(snapshot),
    )
    signed_public_key, signature = _write_ephemeral_sshsig(
        snapshot, key_path=signing_key
    )
    if signed_public_key != public_key:
        raise AssertionError("ephemeral signer public key changed")
    snapshot["signature"] = signature
    if signing_directory is not None:
        signing_directory.cleanup()
    bindings = []
    for member in members:
        notional = member["requested_notional_usd"]
        bindings.append({
            "schema": "route_cost_evidence_binding/v1",
            **links,
            "route_id": route_id,
            "requested_notional_usd": notional,
            "buy_transcript_sha256": typed_sha(
                b"route-cost-evidence-transcript/v1\n",
                transcript_by_scope[(MARKET_ID, "buy", notional)],
            ),
            "sell_transcript_sha256": typed_sha(
                b"route-cost-evidence-transcript/v1\n",
                transcript_by_scope[(market2, "sell", notional)],
            ),
            "submission_policy_member_sha256": typed_sha(
                b"route-cost-submission-policy-member/v1\n", member
            ),
            "evaluated_at": EVALUATED_AT,
            "status": "observed",
            "reason_code": None,
        })
    manifest = {
        "schema": "route_cost_evidence_manifest/v1",
        "run_id": RUN_ID,
        "route_cohort_id": COHORT_ID,
        "phase": PHASE,
        "candidate_source_generation": GENERATION,
        "route_universe_sha256": universe_sha,
        "adapter_registry": registry,
        "adapter_registry_sha256": registry_sha,
        "connector_key_registry": keys,
        "connector_key_registry_sha256": physical_sha(keys),
        "transcript_count": len(transcripts),
        "trace_profile_identity": trace_identity,
        "trace_profile_generation": trace_generation,
        "submission_connector_profile_identity": connector_identity,
        "submission_connector_profile_generation": connector_generation,
        "evaluated_at": EVALUATED_AT,
        "selected_market_count": len(selected),
        "selected_markets": selected,
        "selected_market_set_sha256": selected_sha,
        "native_price_evidence": native,
        "native_price_evidence_sha256": native_sha,
        "chain_evidence_count": 1,
        "chain_evidence": [chain],
        "chain_evidence_set_sha256": typed_sha(
            b"route-cost-chain-evidence-set/v1\n", [chain]
        ),
        "market_evidence_count": len(markets),
        "market_evidence": markets,
        "market_evidence_set_sha256": typed_sha(
            b"route-cost-market-evidence-set/v1\n", markets
        ),
        "transcripts": transcripts,
        "transcript_set_sha256": typed_sha(
            b"route-cost-evidence-transcript-set/v1\n", transcripts
        ),
        "binding_count": len(bindings),
        "bindings": bindings,
        "binding_set_sha256": typed_sha(
            b"route-cost-evidence-binding-set/v1\n", bindings
        ),
        "submission_policy_snapshot": snapshot,
        "submission_policy_snapshot_sha256": typed_sha(
            b"route-cost-submission-policy-snapshot/v1\n", snapshot
        ),
        "counts": {
            "transcript_observed": len(transcripts),
            "transcript_unavailable": 0,
            "transcript_failed": 0,
            "binding_observed": len(bindings),
            "binding_unavailable": 0,
            "binding_failed": 0,
        },
    }
    return manifest, universe, retained


def unsupported_manifest(universe=None, *, tracked=False):
    universe = universe or universe_for()
    if tracked:
        registry = route_cost_evidence.load_route_cost_adapter_registry()
        keys = route_cost_evidence.load_route_cost_connector_key_registry()
    else:
        registry = adapter_registry(supported=False)
        keys = connector_registry()
    universe_sha = physical_sha(universe)
    selected = [
        {
            "market_id": MARKET_ID,
            "token_rank": 1,
            "selection_rank": 1,
            "best_route_volume_usd": "900",
            "dex_24h_usd": "900",
            "dex_tvl_usd": "1000",
            "adapter_id": ADAPTER_ID,
            "structural_support_status": "unsupported",
            "structural_reason": "strict_cost_adapter_unsupported",
        }
    ]
    selected_sha = physical_sha(
        {"schema": "route_cost_selected_markets/v1", "members": selected}
    )
    trace_identity = {
        "schema": "route_cost_trace_profile_identity/v1",
        "status": "missing",
        "profile_id": None,
        "endpoint_id": None,
    }
    connector_identity = {
        "schema": "route_cost_submission_connector_identity/v1",
        "status": "missing",
        "profile_id": None,
        "connector_id": None,
    }
    trace_generation = typed_sha(
        b"route-cost-trace-profile-identity/v1\n", trace_identity
    )
    connector_generation = typed_sha(
        b"route-cost-submission-connector-identity/v1\n", connector_identity
    )
    registry_sha = physical_sha(registry)
    key_sha = physical_sha(keys)
    transcripts = []
    for direction in ("buy", "sell"):
        for notional in NOTIONALS:
            transcripts.append(
                {
                    "schema": "route_cost_evidence_transcript/v1",
                    "run_id": RUN_ID,
                    "route_cohort_id": COHORT_ID,
                    "candidate_source_generation": GENERATION,
                    "route_universe_sha256": universe_sha,
                    "adapter_registry_sha256": registry_sha,
                    "selected_market_set_sha256": selected_sha,
                    "trace_profile_generation": trace_generation,
                    "submission_connector_profile_generation": connector_generation,
                    "market_id": MARKET_ID,
                    "direction": direction,
                    "requested_notional_usd": str(notional),
                    "adapter_id": ADAPTER_ID,
                    "simulation_target_token_address": None,
                    "simulation_target_unit_decimals": None,
                    "simulation_target_raw_quantity": None,
                    "simulation_target_lattice_raw": None,
                    "simulation_target_sha256": None,
                    "core_pool_state_id": None,
                    "core_pool_state_sha256": None,
                    "chain_evidence_sha256": None,
                    "market_evidence_sha256": None,
                    "status": "unavailable",
                    "completed_stage": "none",
                    "reason_code": "strict_cost_adapter_unsupported",
                    "block_evidence": None,
                    "call_evidence": None,
                    "gas_evidence": None,
                    "router_fee_evidence": None,
                    "transfer_tax_evidence": None,
                    "raw_transcript": None,
                }
            )
    empty_member_sha = typed_sha(
        b"route-cost-submission-policy-member-set/v1\n", []
    )
    snapshot = {
        "schema": "route_cost_submission_policy_snapshot/v1",
        "run_id": RUN_ID,
        "route_cohort_id": COHORT_ID,
        "candidate_source_generation": GENERATION,
        "route_universe_sha256": universe_sha,
        "adapter_registry_sha256": registry_sha,
        "selected_market_set_sha256": selected_sha,
        "connector_key_registry_sha256": key_sha,
        "trace_profile_generation": trace_generation,
        "submission_connector_profile_generation": connector_generation,
        "connector_id": None,
        "member_count": 0,
        "members": [],
        "member_set_sha256": empty_member_sha,
        "status": "not_applicable",
        "reason_code": "scope_empty",
        "observed_at": None,
        "valid_until": None,
        "issuer_key_id": None,
        "signature_algorithm": None,
        "attested_payload_sha256": None,
        "signature": None,
    }
    return {
        "schema": "route_cost_evidence_manifest/v1",
        "run_id": RUN_ID,
        "route_cohort_id": COHORT_ID,
        "phase": PHASE,
        "candidate_source_generation": GENERATION,
        "route_universe_sha256": universe_sha,
        "adapter_registry": registry,
        "adapter_registry_sha256": registry_sha,
        "connector_key_registry": keys,
        "connector_key_registry_sha256": key_sha,
        "transcript_count": 10,
        "trace_profile_identity": trace_identity,
        "trace_profile_generation": trace_generation,
        "submission_connector_profile_identity": connector_identity,
        "submission_connector_profile_generation": connector_generation,
        "evaluated_at": EVALUATED_AT,
        "selected_market_count": 1,
        "selected_markets": selected,
        "selected_market_set_sha256": selected_sha,
        "native_price_evidence": None,
        "native_price_evidence_sha256": None,
        "chain_evidence_count": 0,
        "chain_evidence": [],
        "chain_evidence_set_sha256": typed_sha(
            b"route-cost-chain-evidence-set/v1\n", []
        ),
        "market_evidence_count": 0,
        "market_evidence": [],
        "market_evidence_set_sha256": typed_sha(
            b"route-cost-market-evidence-set/v1\n", []
        ),
        "transcripts": transcripts,
        "transcript_set_sha256": typed_sha(
            b"route-cost-evidence-transcript-set/v1\n", transcripts
        ),
        "binding_count": 0,
        "bindings": [],
        "binding_set_sha256": typed_sha(
            b"route-cost-evidence-binding-set/v1\n", []
        ),
        "submission_policy_snapshot": snapshot,
        "submission_policy_snapshot_sha256": typed_sha(
            b"route-cost-submission-policy-snapshot/v1\n", snapshot
        ),
        "counts": {
            "transcript_observed": 0,
            "transcript_unavailable": 10,
            "transcript_failed": 0,
            "binding_observed": 0,
            "binding_unavailable": 0,
            "binding_failed": 0,
        },
    }


class FixedBlockPhaseAPureProducerTests(unittest.TestCase):
    def _fixture(self):
        universe = universe_for()
        registry = adapter_registry(supported=True)
        retained = {MARKET_ID: retained_v2_pool_state()}
        plan = route_cost_evidence.build_fixed_block_phase_a_request_plan(
            universe=universe,
            adapter_registry=registry,
            retained_typed_pool_state_members=retained,
        )
        return universe, registry, retained, plan

    def test_request_plan_is_exact_one_market_known_answer(self):
        _universe, registry, _retained, plan = self._fixture()
        adapter_row = registry["adapters"][0]
        block_tag = "0x64"
        self.assertEqual(set(plan), {"block_tag", "requests", "request_roles"})
        self.assertEqual([row["id"] for row in plan["requests"]], list(range(1, 12)))
        self.assertEqual(
            [(row["method"], row["params"]) for row in plan["requests"]],
            [
                ("eth_chainId", []),
                ("eth_getBlockByNumber", [block_tag, False]),
                ("eth_feeHistory", ["0x1", block_tag, [50]]),
                ("eth_getCode", [ROUTER, block_tag]),
                ("eth_getCode", [FACTORY, block_tag]),
                ("eth_call", [{
                    "to": FACTORY,
                    "data": route_cost_evidence.build_factory_get_pair_calldata(
                        TOKEN_A, TOKEN_B
                    ),
                }, block_tag]),
                ("eth_getCode", [POOL, block_tag]),
                ("eth_call", [{"to": POOL, "data": "0x0dfe1681"}, block_tag]),
                ("eth_call", [{"to": POOL, "data": "0xd21220a7"}, block_tag]),
                ("eth_getCode", [TOKEN_A, block_tag]),
                ("eth_getCode", [TOKEN_B, block_tag]),
            ],
        )
        self.assertEqual(
            [row["role"] for row in plan["request_roles"]],
            [
                "chain_id", "block_header", "fee_history",
                "router_runtime_code", "factory_runtime_code",
                "factory_get_pair", "pair_runtime_code", "pair_token0",
                "pair_token1", "token0_runtime_code", "token1_runtime_code",
            ],
        )
        self.assertEqual(
            {row.get("market_id") for row in plan["request_roles"][3:]},
            {MARKET_ID},
        )
        self.assertTrue(adapter_row)

    def test_permuted_successful_results_project_to_valid_incomplete_chain(self):
        universe, registry, retained, plan = self._fixture()
        projected = route_cost_evidence.project_fixed_block_phase_a_capture(
            universe=universe,
            plan=plan,
            responses=phase_a_rpc_responses(plan),
            run_id=RUN_ID,
            route_cohort_id=COHORT_ID,
            candidate_source_generation=GENERATION,
            route_universe_sha256=physical_sha(universe),
            trace_profile_identity={
                "schema": "route_cost_trace_profile_identity/v1",
                "status": "available",
                "profile_id": "kat-trace",
                "endpoint_id": "kat-rpc",
            },
            adapter_registry=registry,
            retained_typed_pool_state_members=retained,
            captured_started_at="2026-08-01T12:00:00Z",
            captured_finished_at="2026-08-01T12:00:01Z",
        )
        self.assertEqual(set(projected), {"chain_evidence", "market_evidence"})
        self.assertEqual(len(projected["chain_evidence"]), 1)
        self.assertEqual(len(projected["market_evidence"]), 1)
        chain = projected["chain_evidence"][0]
        market = projected["market_evidence"][0]
        self.assertEqual(
            (chain["status"], chain["reason_code"]),
            ("incomplete", "native_price_unavailable"),
        )
        self.assertEqual(chain["rpc_source_id"], "kat-rpc")
        self.assertEqual(market["chain_evidence_sha256"], physical_sha(chain))
        self.assertEqual(market["factory_get_pair_request"]["id"], 6)
        self.assertEqual(
            [row["token_address"] for row in market["token_runtime_code_evidence"]],
            [TOKEN_A, TOKEN_B],
        )

    def test_projection_rejects_fee_history_parent_base_fee_drift(self):
        universe, registry, retained, plan = self._fixture()
        responses = phase_a_rpc_responses(plan)
        fee_id = next(
            row["id"] for row in plan["request_roles"]
            if row["role"] == "fee_history"
        )
        fee_response = next(row for row in responses if row["id"] == fee_id)
        fee_response["result"]["baseFeePerGas"][0] = "0x63"
        with self.assertRaisesRegex(
            route_cost_evidence.RouteCostEvidenceError,
            "fee history differs from header",
        ):
            self._project(universe, registry, retained, plan, responses)

    def test_fee_history_gas_used_ratio_is_finite_unit_interval_without_negative_zero(self):
        universe, registry, retained, plan = self._fixture()
        fee_id = next(
            row["id"] for row in plan["request_roles"]
            if row["role"] == "fee_history"
        )
        for ratio in (2.0, 1.1, -0.0, float("nan"), float("inf"), -float("inf")):
            responses = phase_a_rpc_responses(plan)
            fee_response = next(row for row in responses if row["id"] == fee_id)
            fee_response["result"]["gasUsedRatio"] = [ratio]
            with self.subTest(invalid=repr(ratio)), self.assertRaisesRegex(
                route_cost_evidence.RouteCostEvidenceError,
                "gas ratio",
            ):
                self._project(universe, registry, retained, plan, responses)

        for ratio, expected in ((0, "0"), (1, "1"), (0.5, "0.5")):
            responses = phase_a_rpc_responses(plan)
            fee_response = next(row for row in responses if row["id"] == fee_id)
            fee_response["result"]["gasUsedRatio"] = [ratio]
            projected = self._project(
                universe, registry, retained, plan, responses
            )
            self.assertEqual(
                projected["chain_evidence"][0]["fee_history_result"][
                    "gas_used_ratio"
                ],
                [expected],
            )

    def test_fee_history_preserves_high_precision_decimal_ratio(self):
        universe, registry, retained, plan = self._fixture()
        responses = phase_a_rpc_responses(plan)
        fee_id = next(
            row["id"] for row in plan["request_roles"]
            if row["role"] == "fee_history"
        )
        fee_response = next(row for row in responses if row["id"] == fee_id)
        ratio = Decimal("0.123456789012345678901234567890123456789")
        fee_response["result"]["gasUsedRatio"] = [ratio]
        projected = self._project(
            universe, registry, retained, plan, responses
        )
        self.assertEqual(
            projected["chain_evidence"][0]["fee_history_result"][
                "gas_used_ratio"
            ],
            [str(ratio)],
        )

    def test_projection_rejects_duplicate_missing_extra_and_wrong_ids(self):
        universe, registry, retained, plan = self._fixture()
        canonical = phase_a_rpc_responses(plan)
        cases = {
            "duplicate": canonical[:-1] + [copy.deepcopy(canonical[0])],
            "missing": canonical[:-1],
            "extra": canonical + [{
                "jsonrpc": "2.0", "id": 12, "result": "0x1",
            }],
            "wrong": [
                dict(row, id=12) if index == 0 else row
                for index, row in enumerate(canonical)
            ],
        }
        for label, responses in cases.items():
            with self.subTest(case=label), self.assertRaises(
                route_cost_evidence.RouteCostEvidenceError
            ):
                self._project(universe, registry, retained, plan, responses)

    def test_projection_rejects_chain_block_and_identity_drift(self):
        universe, registry, retained, plan = self._fixture()
        role_ids = {row["role"]: row["id"] for row in plan["request_roles"]}
        cases = {
            "chain": ("chain_id", lambda value: "0x2"),
            "block": ("block_header", lambda value: dict(value, number="0x65")),
            "header": (
                "block_header",
                lambda value: dict(value, hash="0x" + "8" * 64),
            ),
            "get_pair": (
                "factory_get_pair",
                lambda value: "0x" + "0" * 64,
            ),
            "token": (
                "pair_token0",
                lambda value: "0x" + "0" * 24 + TOKEN_B[2:],
            ),
            "router_code": ("router_runtime_code", lambda value: "0x01"),
            "pair_code": ("pair_runtime_code", lambda value: "0x01"),
            "token_code": ("token0_runtime_code", lambda value: "0x02"),
        }
        for label, (role, mutate) in cases.items():
            responses = phase_a_rpc_responses(plan)
            response = next(row for row in responses if row["id"] == role_ids[role])
            response["result"] = mutate(response["result"])
            with self.subTest(case=label), self.assertRaises(
                route_cost_evidence.RouteCostEvidenceError
            ):
                self._project(universe, registry, retained, plan, responses)

    def test_projection_rejects_anchor_transplant(self):
        universe, registry, retained, plan = self._fixture()
        transplanted = {MARKET_ID: retained_v2_pool_state(block_number=101)}
        with self.assertRaisesRegex(
            route_cost_evidence.RouteCostEvidenceError, "plan differs"
        ):
            self._project(
                universe, registry, transplanted, plan,
                phase_a_rpc_responses(plan),
            )

    def test_request_plan_rejects_multi_market_anchor_drift(self):
        pool2 = "0x" + "4" * 40
        market2 = "dex:eth:uniswap_v2:{}:AAA".format(pool2)
        leg1 = copy.deepcopy(universe_for()["selected_legs"][0])
        leg2 = copy.deepcopy(leg1)
        leg2["market_id"] = market2
        leg2["selection_rank"] = 2
        universe = universe_for(markets=[leg1, leg2])
        registry = adapter_registry(supported=True)
        registry["adapters"][0]["pair_descriptors"].append(
            pair_descriptor(pool2)
        )
        retained = {
            MARKET_ID: retained_v2_pool_state(),
            market2: retained_v2_pool_state(
                market_id=market2,
                pool_address=pool2,
                block_timestamp=1785585601,
            ),
        }
        with self.assertRaisesRegex(
            route_cost_evidence.RouteCostEvidenceError,
            "retained anchor differs",
        ):
            route_cost_evidence.build_fixed_block_phase_a_request_plan(
                universe=universe,
                adapter_registry=registry,
                retained_typed_pool_state_members=retained,
            )

    def test_mixed_supported_scope_requests_and_projects_only_retained_market(self):
        pool2 = "0x" + "4" * 40
        market2 = "dex:eth:uniswap_v2:{}:AAA".format(pool2)
        leg1 = copy.deepcopy(universe_for()["selected_legs"][0])
        leg2 = copy.deepcopy(leg1)
        leg2["market_id"] = market2
        leg2["selection_rank"] = 2
        universe = universe_for(markets=[leg1, leg2])
        registry = adapter_registry(supported=True)
        registry["adapters"][0]["pair_descriptors"].append(
            pair_descriptor(pool2)
        )
        retained = {MARKET_ID: retained_v2_pool_state()}
        plan = route_cost_evidence.build_fixed_block_phase_a_request_plan(
            universe=universe,
            adapter_registry=registry,
            retained_typed_pool_state_members=retained,
        )
        self.assertEqual(len(plan["requests"]), 11)
        self.assertEqual(
            {row["market_id"] for row in plan["request_roles"][3:]},
            {MARKET_ID},
        )
        projection = route_cost_evidence.project_fixed_block_phase_a_capture(
            universe=universe,
            plan=plan,
            responses=phase_a_rpc_responses(plan),
            run_id=RUN_ID,
            route_cohort_id=COHORT_ID,
            candidate_source_generation=GENERATION,
            route_universe_sha256=physical_sha(universe),
            trace_profile_identity={
                "schema": "route_cost_trace_profile_identity/v1",
                "status": "available",
                "profile_id": "kat-trace",
                "endpoint_id": "kat-rpc",
            },
            adapter_registry=registry,
            retained_typed_pool_state_members=retained,
            captured_started_at="2026-08-01T12:00:00Z",
            captured_finished_at="2026-08-01T12:00:01Z",
        )
        self.assertEqual(
            [row["market_id"] for row in projection["market_evidence"]],
            [MARKET_ID],
        )

    def test_empty_retained_subset_has_no_fixed_block_or_rpc_projection(self):
        universe = universe_for()
        registry = adapter_registry(supported=True)
        plan = route_cost_evidence.build_fixed_block_phase_a_request_plan(
            universe=universe,
            adapter_registry=registry,
            retained_typed_pool_state_members={},
        )
        self.assertEqual(plan, {
            "block_tag": None,
            "requests": [],
            "request_roles": [],
        })
        projected = route_cost_evidence.project_fixed_block_phase_a_capture(
            universe=universe,
            plan=plan,
            responses=[],
            run_id=RUN_ID,
            route_cohort_id=COHORT_ID,
            candidate_source_generation=GENERATION,
            route_universe_sha256=physical_sha(universe),
            trace_profile_identity={
                "schema": "route_cost_trace_profile_identity/v1",
                "status": "available",
                "profile_id": "kat-trace",
                "endpoint_id": "kat-rpc",
            },
            adapter_registry=registry,
            retained_typed_pool_state_members={},
            captured_started_at="2026-08-01T12:00:00Z",
            captured_finished_at="2026-08-01T12:00:01Z",
        )
        self.assertEqual(projected, {
            "chain_evidence": [],
            "market_evidence": [],
        })

    def _project(self, universe, registry, retained, plan, responses):
        return route_cost_evidence.project_fixed_block_phase_a_capture(
            universe=universe,
            plan=plan,
            responses=responses,
            run_id=RUN_ID,
            route_cohort_id=COHORT_ID,
            candidate_source_generation=GENERATION,
            route_universe_sha256=physical_sha(universe),
            trace_profile_identity={
                "schema": "route_cost_trace_profile_identity/v1",
                "status": "available",
                "profile_id": "kat-trace",
                "endpoint_id": "kat-rpc",
            },
            adapter_registry=registry,
            retained_typed_pool_state_members=retained,
            captured_started_at="2026-08-01T12:00:00Z",
            captured_finished_at="2026-08-01T12:00:01Z",
        )


class NativePricePhaseABindingPureProducerTests(unittest.TestCase):
    def _fixture(self):
        universe = universe_for()
        registry = adapter_registry(supported=True)
        retained = {MARKET_ID: retained_v2_pool_state()}
        plan = route_cost_evidence.build_fixed_block_phase_a_request_plan(
            universe=universe,
            adapter_registry=registry,
            retained_typed_pool_state_members=retained,
        )
        trace_identity = {
            "schema": "route_cost_trace_profile_identity/v1",
            "status": "available",
            "profile_id": "kat-trace",
            "endpoint_id": "kat-rpc",
        }
        phase_a = route_cost_evidence.project_fixed_block_phase_a_capture(
            universe=universe,
            plan=plan,
            responses=phase_a_rpc_responses(plan),
            run_id=RUN_ID,
            route_cohort_id=COHORT_ID,
            candidate_source_generation=GENERATION,
            route_universe_sha256=physical_sha(universe),
            trace_profile_identity=trace_identity,
            adapter_registry=registry,
            retained_typed_pool_state_members=retained,
            captured_started_at="2026-08-01T12:00:00Z",
            captured_finished_at="2026-08-01T12:00:01Z",
        )
        return universe, registry, retained, trace_identity, phase_a

    def _bind(self, fixture, native=None):
        universe, registry, retained, trace_identity, phase_a = fixture
        return route_cost_evidence.bind_native_price_to_phase_a_capture(
            universe=universe,
            phase_a_capture=phase_a,
            native_price_evidence=(
                native_price_evidence() if native is None else native
            ),
            run_id=RUN_ID,
            route_cohort_id=COHORT_ID,
            candidate_source_generation=GENERATION,
            route_universe_sha256=physical_sha(universe),
            trace_profile_identity=trace_identity,
            adapter_registry=registry,
            retained_typed_pool_state_members=retained,
        )

    def test_binding_rebuilds_chain_and_every_market_without_mutating_phase_a(self):
        fixture = self._fixture()
        original = copy.deepcopy(fixture[-1])
        bound = self._bind(fixture)
        self.assertEqual(fixture[-1], original)
        self.assertEqual(set(bound), {"chain_evidence", "market_evidence"})
        self.assertEqual(len(bound["chain_evidence"]), 1)
        self.assertEqual(len(bound["market_evidence"]), 1)
        old_chain = original["chain_evidence"][0]
        new_chain = bound["chain_evidence"][0]
        self.assertEqual(
            (old_chain["status"], old_chain["reason_code"]),
            ("incomplete", "native_price_unavailable"),
        )
        self.assertEqual(
            (new_chain["status"], new_chain["reason_code"]),
            ("observed", None),
        )
        native = native_price_evidence()
        native_sha = typed_sha(
            b"route-cost-native-price-evidence/v1\n", native
        )
        self.assertEqual(
            new_chain["native_price_record"],
            {
                "schema": "route_cost_native_price_record/v1",
                "status": "observed",
                "reason_code": None,
                "native_symbol": "ETH",
                "wrapped_native_address": (
                    "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
                ),
                "price_usd": "3000",
                "observed_at": native["observed_at"],
                "valid_until": native["valid_until"],
                "native_price_evidence_sha256": native_sha,
                "source_record_sha256": native["source_record_sha256"],
            },
        )
        self.assertNotEqual(physical_sha(old_chain), physical_sha(new_chain))
        self.assertEqual(
            bound["market_evidence"][0]["chain_evidence_sha256"],
            physical_sha(new_chain),
        )

    def test_binding_rejects_foreign_native_and_noncanonical_phase_a_terminal(self):
        fixture = self._fixture()
        foreign = native_price_evidence()
        foreign["run_id"] = "foreign-run"
        with self.assertRaises(route_cost_evidence.RouteCostEvidenceError):
            self._bind(fixture, native=foreign)

        tampered_fixture = list(fixture)
        tampered_fixture[-1] = copy.deepcopy(fixture[-1])
        tampered_fixture[-1]["chain_evidence"][0]["status"] = "observed"
        with self.assertRaises(route_cost_evidence.RouteCostEvidenceError):
            self._bind(tuple(tampered_fixture))

    def test_binding_rejects_full_rehash_selected_market_hash_transplant(self):
        fixture = list(self._fixture())
        phase_a = copy.deepcopy(fixture[-1])
        chain = phase_a["chain_evidence"][0]
        chain["selected_market_set_sha256"] = "e" * 64
        transplanted_chain_sha = physical_sha(chain)
        for market in phase_a["market_evidence"]:
            market["selected_market_set_sha256"] = "e" * 64
            market["chain_evidence_sha256"] = transplanted_chain_sha
        fixture[-1] = phase_a
        with self.assertRaisesRegex(
            route_cost_evidence.RouteCostEvidenceError,
            "selected-market",
        ):
            self._bind(tuple(fixture))


class NativePriceTerminalPureProducerTests(unittest.TestCase):
    def _fixture(self):
        universe = universe_for()
        registry = adapter_registry(supported=True)
        retained = {MARKET_ID: retained_v2_pool_state(
            reserve0_raw=10 ** 24,
            reserve1_raw=2 * 10 ** 24,
        )}
        trace_identity = {
            "schema": "route_cost_trace_profile_identity/v1",
            "status": "available",
            "profile_id": "kat-trace",
            "endpoint_id": "kat-rpc",
        }
        connector_identity = {
            "schema": "route_cost_submission_connector_identity/v1",
            "status": "missing",
            "profile_id": None,
            "connector_id": None,
        }
        plan = route_cost_evidence.build_fixed_block_phase_a_request_plan(
            universe=universe,
            adapter_registry=registry,
            retained_typed_pool_state_members=retained,
        )
        phase_a = route_cost_evidence.project_fixed_block_phase_a_capture(
            universe=universe,
            plan=plan,
            responses=phase_a_rpc_responses(plan),
            run_id=RUN_ID,
            route_cohort_id=COHORT_ID,
            candidate_source_generation=GENERATION,
            route_universe_sha256=physical_sha(universe),
            trace_profile_identity=trace_identity,
            adapter_registry=registry,
            retained_typed_pool_state_members=retained,
            captured_started_at="2026-08-01T12:00:00Z",
            captured_finished_at="2026-08-01T12:00:01Z",
        )
        snapshot = route_cost_evidence.build_terminal_submission_policy_snapshot(
            universe=universe,
            run_id=RUN_ID,
            route_cohort_id=COHORT_ID,
            candidate_source_generation=GENERATION,
            route_universe_sha256=physical_sha(universe),
            adapter_registry=registry,
            connector_key_registry=connector_registry(),
            trace_profile_identity=trace_identity,
            submission_connector_profile_identity=connector_identity,
            reason_code="submission_connector_missing",
        )
        return {
            "universe": universe,
            "registry": registry,
            "retained": retained,
            "trace_identity": trace_identity,
            "connector_identity": connector_identity,
            "phase_a": phase_a,
            "snapshot": snapshot,
        }

    def _project(self, fixture, reason, terminal_reason_by_market=None):
        return route_cost_evidence.project_native_price_terminal_phase_a_capture(
            universe=fixture["universe"],
            phase_a_capture=fixture["phase_a"],
            run_id=RUN_ID,
            route_cohort_id=COHORT_ID,
            candidate_source_generation=GENERATION,
            route_universe_sha256=physical_sha(fixture["universe"]),
            trace_profile_identity=fixture["trace_identity"],
            submission_connector_profile_identity=fixture[
                "connector_identity"
            ],
            adapter_registry=fixture["registry"],
            retained_typed_pool_state_members=fixture["retained"],
            submission_policy_snapshot=fixture["snapshot"],
            reason_code=reason,
            terminal_reason_by_market=terminal_reason_by_market,
        )

    def test_native_terminal_replays_phase_a_and_derives_call_prefix(self):
        fixture = self._fixture()
        original = copy.deepcopy(fixture["phase_a"])
        for reason, status, chain_status in (
            ("native_price_unavailable", "unavailable", "incomplete"),
            ("native_price_invalid", "failed", "failed"),
        ):
            with self.subTest(reason=reason):
                projected = self._project(fixture, reason)
                self.assertEqual(fixture["phase_a"], original)
                self.assertEqual(
                    set(projected),
                    {"chain_evidence", "market_evidence", "transcripts"},
                )
                self.assertEqual(len(projected["chain_evidence"]), 1)
                self.assertEqual(len(projected["market_evidence"]), 1)
                self.assertEqual(len(projected["transcripts"]), 10)
                chain = projected["chain_evidence"][0]
                self.assertEqual(chain["status"], chain_status)
                self.assertEqual(chain["reason_code"], reason)
                self.assertEqual(chain["native_price_record"]["status"], status)
                chain_sha = physical_sha(chain)
                market = projected["market_evidence"][0]
                market_sha = physical_sha(market)
                self.assertEqual(market["chain_evidence_sha256"], chain_sha)
                for row in projected["transcripts"]:
                    self.assertEqual(
                        (row["status"], row["completed_stage"], row["reason_code"]),
                        (status, "call", reason),
                    )
                    self.assertEqual(row["chain_evidence_sha256"], chain_sha)
                    self.assertEqual(row["market_evidence_sha256"], market_sha)
                    self.assertIsNotNone(row["simulation_target_sha256"])
                    self.assertIsNotNone(row["block_evidence"])
                    self.assertIsNotNone(row["call_evidence"])
                    self.assertIsNotNone(row["raw_transcript"])
                    self.assertIsNone(row["raw_transcript"]["estimate_gas_request"])
                    self.assertIsNone(row["raw_transcript"]["estimate_gas_response"])
                    self.assertIsNone(row["raw_transcript"]["simulation_request"])
                    self.assertIsNone(row["raw_transcript"]["simulation_response"])
                    self.assertIsNone(row["gas_evidence"])
                    self.assertIsNone(row["router_fee_evidence"])
                    self.assertIsNone(row["transfer_tax_evidence"])

                manifest = route_cost_evidence.build_route_cost_evidence_manifest_from_captured(
                    universe=fixture["universe"],
                    run_id=RUN_ID,
                    route_cohort_id=COHORT_ID,
                    phase=PHASE,
                    candidate_source_generation=GENERATION,
                    route_universe_sha256=physical_sha(fixture["universe"]),
                    evaluated_at=EVALUATED_AT,
                    adapter_registry=fixture["registry"],
                    connector_key_registry=connector_registry(),
                    trace_profile_identity=fixture["trace_identity"],
                    submission_connector_profile_identity=fixture[
                        "connector_identity"
                    ],
                    native_price_evidence=None,
                    chain_evidence=projected["chain_evidence"],
                    market_evidence=projected["market_evidence"],
                    transcripts=projected["transcripts"],
                    submission_policy_snapshot=fixture["snapshot"],
                )
                self.assertEqual(
                    route_cost_evidence.validate_route_cost_evidence_manifest_for_publication(
                        manifest,
                        universe=fixture["universe"],
                        expected_run_id=RUN_ID,
                        expected_route_cohort_id=COHORT_ID,
                        expected_phase=PHASE,
                        expected_candidate_source_generation=GENERATION,
                        expected_route_universe_sha256=physical_sha(
                            fixture["universe"]
                        ),
                        retained_typed_pool_state_members=fixture["retained"],
                    ),
                    manifest,
                )

    def test_native_terminal_rejects_reason_and_authority_transplants(self):
        fixture = self._fixture()
        for reason in ("caller_chosen", "gas_invalid", None):
            with self.subTest(reason=reason):
                with self.assertRaises(route_cost_evidence.RouteCostEvidenceError):
                    self._project(fixture, reason)

        tampered = copy.deepcopy(fixture)
        tampered["phase_a"]["market_evidence"][0][
            "core_pool_state_sha256"
        ] = "e" * 64
        with self.assertRaises(route_cost_evidence.RouteCostEvidenceError):
            self._project(tampered, "native_price_unavailable")

    def test_native_terminal_keeps_unsupported_rows_in_full_denominator(self):
        fixture = self._fixture()
        unsupported_pool = "0x" + "4" * 40
        unsupported_market = (
            "dex:eth:uniswap_v2:{}:BBB".format(unsupported_pool)
        )
        unsupported_leg = copy.deepcopy(
            fixture["universe"]["selected_legs"][0]
        )
        unsupported_leg.update({
            "market_id": unsupported_market,
            "token_symbol": "BBB",
            "selection_rank": 2,
        })
        fixture["universe"]["selected_legs"].append(unsupported_leg)
        fixture["snapshot"] = route_cost_evidence.build_terminal_submission_policy_snapshot(
            universe=fixture["universe"],
            run_id=RUN_ID,
            route_cohort_id=COHORT_ID,
            candidate_source_generation=GENERATION,
            route_universe_sha256=physical_sha(fixture["universe"]),
            adapter_registry=fixture["registry"],
            connector_key_registry=connector_registry(),
            trace_profile_identity=fixture["trace_identity"],
            submission_connector_profile_identity=fixture[
                "connector_identity"
            ],
            reason_code="submission_connector_missing",
        )
        plan = route_cost_evidence.build_fixed_block_phase_a_request_plan(
            universe=fixture["universe"],
            adapter_registry=fixture["registry"],
            retained_typed_pool_state_members=fixture["retained"],
        )
        fixture["phase_a"] = route_cost_evidence.project_fixed_block_phase_a_capture(
            universe=fixture["universe"],
            plan=plan,
            responses=phase_a_rpc_responses(plan),
            run_id=RUN_ID,
            route_cohort_id=COHORT_ID,
            candidate_source_generation=GENERATION,
            route_universe_sha256=physical_sha(fixture["universe"]),
            trace_profile_identity=fixture["trace_identity"],
            adapter_registry=fixture["registry"],
            retained_typed_pool_state_members=fixture["retained"],
            captured_started_at="2026-08-01T12:00:00Z",
            captured_finished_at="2026-08-01T12:00:01Z",
        )

        projected = self._project(fixture, "native_price_unavailable")
        self.assertEqual(len(projected["transcripts"]), 20)
        self.assertEqual(
            {
                (row["status"], row["completed_stage"], row["reason_code"])
                for row in projected["transcripts"]
                if row["market_id"] == unsupported_market
            },
            {("unavailable", "none", "strict_cost_adapter_unsupported")},
        )

        tampered = copy.deepcopy(fixture)
        tampered["snapshot"]["members"][0][
            "requested_notional_usd"
        ] = "1001"
        with self.assertRaises(route_cost_evidence.RouteCostEvidenceError):
            self._project(tampered, "native_price_unavailable")

    def test_native_terminal_keeps_missing_core_supported_rows(self):
        fixture = self._fixture()
        pool2 = "0x" + "4" * 40
        token2 = "0x" + "5" * 40
        token3 = "0x" + "6" * 40
        missing_market = "dex:eth:uniswap_v2:{}:BBB".format(pool2)
        missing_leg = copy.deepcopy(
            fixture["universe"]["selected_legs"][0]
        )
        missing_leg.update({
            "market_id": missing_market,
            "token_symbol": "BBB",
            "selection_rank": 2,
            "target_token_address": token2,
        })
        missing_leg["collector_context"] = copy.deepcopy(
            missing_leg["collector_context"]
        )
        missing_leg["collector_context"].update({
            "base_token_id": "eth_" + token2,
            "quote_token_id": "eth_" + token3,
        })
        fixture["universe"]["selected_legs"].append(missing_leg)
        fixture["registry"]["adapters"][0]["pair_descriptors"].append(
            pair_descriptor(pool2, token0=token2, token1=token3)
        )
        fixture["registry"]["adapters"][0][
            "token_funding_descriptors"
        ].extend([
            funding_descriptor(token2), funding_descriptor(token3),
        ])
        fixture["snapshot"] = route_cost_evidence.build_terminal_submission_policy_snapshot(
            universe=fixture["universe"],
            run_id=RUN_ID,
            route_cohort_id=COHORT_ID,
            candidate_source_generation=GENERATION,
            route_universe_sha256=physical_sha(fixture["universe"]),
            adapter_registry=fixture["registry"],
            connector_key_registry=connector_registry(),
            trace_profile_identity=fixture["trace_identity"],
            submission_connector_profile_identity=fixture[
                "connector_identity"
            ],
            reason_code="submission_connector_missing",
        )
        plan = route_cost_evidence.build_fixed_block_phase_a_request_plan(
            universe=fixture["universe"],
            adapter_registry=fixture["registry"],
            retained_typed_pool_state_members=fixture["retained"],
        )
        fixture["phase_a"] = route_cost_evidence.project_fixed_block_phase_a_capture(
            universe=fixture["universe"],
            plan=plan,
            responses=phase_a_rpc_responses(plan),
            run_id=RUN_ID,
            route_cohort_id=COHORT_ID,
            candidate_source_generation=GENERATION,
            route_universe_sha256=physical_sha(fixture["universe"]),
            trace_profile_identity=fixture["trace_identity"],
            adapter_registry=fixture["registry"],
            retained_typed_pool_state_members=fixture["retained"],
            captured_started_at="2026-08-01T12:00:00Z",
            captured_finished_at="2026-08-01T12:00:01Z",
        )

        projected = self._project(
            fixture,
            "native_price_unavailable",
            {missing_market: "core_pool_state_unavailable"},
        )
        self.assertEqual(len(projected["transcripts"]), 20)
        self.assertEqual(
            {
                (row["status"], row["completed_stage"], row["reason_code"])
                for row in projected["transcripts"]
                if row["market_id"] == MARKET_ID
            },
            {("unavailable", "call", "native_price_unavailable")},
        )
        missing_rows = [
            row for row in projected["transcripts"]
            if row["market_id"] == missing_market
        ]
        self.assertEqual(
            {
                (row["status"], row["completed_stage"], row["reason_code"])
                for row in missing_rows
            },
            {("unavailable", "none", "core_pool_state_unavailable")},
        )
        self.assertTrue(all(
            row["core_pool_state_id"] is None
            and row["chain_evidence_sha256"] is None
            and row["market_evidence_sha256"] is None
            and row["raw_transcript"] is None
            for row in missing_rows
        ))

        with self.assertRaises(route_cost_evidence.RouteCostEvidenceError):
            self._project(
                fixture,
                "native_price_unavailable",
                {missing_market: "rpc_unavailable"},
            )

    def test_native_terminal_same_target_sibling_closes_at_block(self):
        fixture = self._fixture()
        pool2 = "0x" + "4" * 40
        missing_market = "dex:eth:uniswap_v2:{}:AAA".format(pool2)
        missing_leg = copy.deepcopy(
            fixture["universe"]["selected_legs"][0]
        )
        missing_leg.update({
            "market_id": missing_market,
            "selection_rank": 2,
        })
        fixture["universe"]["selected_legs"].append(missing_leg)
        fixture["registry"]["adapters"][0]["pair_descriptors"].append(
            pair_descriptor(pool2, token0=TOKEN_A, token1=TOKEN_B)
        )
        fixture["snapshot"] = route_cost_evidence.build_terminal_submission_policy_snapshot(
            universe=fixture["universe"],
            run_id=RUN_ID,
            route_cohort_id=COHORT_ID,
            candidate_source_generation=GENERATION,
            route_universe_sha256=physical_sha(fixture["universe"]),
            adapter_registry=fixture["registry"],
            connector_key_registry=connector_registry(),
            trace_profile_identity=fixture["trace_identity"],
            submission_connector_profile_identity=fixture[
                "connector_identity"
            ],
            reason_code="submission_connector_missing",
        )
        plan = route_cost_evidence.build_fixed_block_phase_a_request_plan(
            universe=fixture["universe"],
            adapter_registry=fixture["registry"],
            retained_typed_pool_state_members=fixture["retained"],
        )
        fixture["phase_a"] = route_cost_evidence.project_fixed_block_phase_a_capture(
            universe=fixture["universe"],
            plan=plan,
            responses=phase_a_rpc_responses(plan),
            run_id=RUN_ID,
            route_cohort_id=COHORT_ID,
            candidate_source_generation=GENERATION,
            route_universe_sha256=physical_sha(fixture["universe"]),
            trace_profile_identity=fixture["trace_identity"],
            adapter_registry=fixture["registry"],
            retained_typed_pool_state_members=fixture["retained"],
            captured_started_at="2026-08-01T12:00:00Z",
            captured_finished_at="2026-08-01T12:00:01Z",
        )

        for reason in ("native_price_unavailable", "native_price_invalid"):
            with self.subTest(reason=reason):
                projected = self._project(
                    fixture, reason,
                    {missing_market: "core_pool_state_unavailable"},
                )
                retained_rows = [
                    row for row in projected["transcripts"]
                    if row["market_id"] == MARKET_ID
                ]
                self.assertEqual(len(retained_rows), 10)
                self.assertEqual(
                    {
                        (row["status"], row["completed_stage"], row["reason_code"])
                        for row in retained_rows
                    },
                    {("unavailable", "block", "calldata_unavailable")},
                )
                self.assertTrue(all(
                    row["simulation_target_sha256"] is None
                    and row["raw_transcript"]["calldata_hex"] is None
                    and row["call_evidence"] is None
                    for row in retained_rows
                ))


class PhaseBScenarioPureProducerTests(unittest.TestCase):
    def _fixture(self, *, routes=None, connector_available=False,
                 native=None, missing_supported=False,
                 same_target_missing=False):
        universe = universe_for(routes=routes)
        registry = adapter_registry(supported=True)
        missing_market_id = None
        if missing_supported:
            pool2 = "0x" + "4" * 40
            token2 = TOKEN_A if same_target_missing else "0x" + "5" * 40
            token3 = TOKEN_B if same_target_missing else "0x" + "6" * 40
            missing_market_id = (
                "dex:eth:uniswap_v2:{}:BBB".format(pool2)
            )
            leg2 = copy.deepcopy(universe["selected_legs"][0])
            leg2.update({
                "market_id": missing_market_id,
                "token_symbol": "BBB",
                "selection_rank": 2,
                "target_token_address": token2,
            })
            leg2["collector_context"] = copy.deepcopy(
                leg2["collector_context"]
            )
            leg2["collector_context"].update({
                "base_token_id": "eth_" + token2,
                "quote_token_id": "eth_" + token3,
            })
            universe["selected_legs"].append(leg2)
            registry["adapters"][0]["pair_descriptors"].append(
                pair_descriptor(pool2, token0=token2, token1=token3)
            )
            if not same_target_missing:
                registry["adapters"][0]["token_funding_descriptors"].extend([
                    funding_descriptor(token2), funding_descriptor(token3),
                ])
        retained = {MARKET_ID: retained_v2_pool_state(
            reserve0_raw=10 ** 24,
            reserve1_raw=2 * 10 ** 24,
        )}
        trace_identity = {
            "schema": "route_cost_trace_profile_identity/v1",
            "status": "available",
            "profile_id": "kat-trace",
            "endpoint_id": "kat-rpc",
        }
        phase_a_plan = route_cost_evidence.build_fixed_block_phase_a_request_plan(
            universe=universe,
            adapter_registry=registry,
            retained_typed_pool_state_members=retained,
        )
        phase_a = route_cost_evidence.project_fixed_block_phase_a_capture(
            universe=universe,
            plan=phase_a_plan,
            responses=phase_a_rpc_responses(phase_a_plan),
            run_id=RUN_ID,
            route_cohort_id=COHORT_ID,
            candidate_source_generation=GENERATION,
            route_universe_sha256=physical_sha(universe),
            trace_profile_identity=trace_identity,
            adapter_registry=registry,
            retained_typed_pool_state_members=retained,
            captured_started_at="2026-08-01T12:00:00Z",
            captured_finished_at="2026-08-01T12:00:01Z",
        )
        native = native_price_evidence() if native is None else native
        bound = route_cost_evidence.bind_native_price_to_phase_a_capture(
            universe=universe,
            phase_a_capture=phase_a,
            native_price_evidence=native,
            run_id=RUN_ID,
            route_cohort_id=COHORT_ID,
            candidate_source_generation=GENERATION,
            route_universe_sha256=physical_sha(universe),
            trace_profile_identity=trace_identity,
            adapter_registry=registry,
            retained_typed_pool_state_members=retained,
        )
        connector_identity = (
            {
                "schema": "route_cost_submission_connector_identity/v1",
                "status": "available",
                "profile_id": "kat-connector",
                "connector_id": "kat_connector",
            }
            if connector_available else
            {
                "schema": "route_cost_submission_connector_identity/v1",
                "status": "missing",
                "profile_id": None,
                "connector_id": None,
            }
        )
        keys = {
            "schema": "route_cost_connector_key_registry/v1",
            "registry_version": "test-empty-v1",
            "keys": [],
        }
        snapshot = route_cost_evidence.build_terminal_submission_policy_snapshot(
            universe=universe,
            run_id=RUN_ID,
            route_cohort_id=COHORT_ID,
            candidate_source_generation=GENERATION,
            route_universe_sha256=physical_sha(universe),
            adapter_registry=registry,
            connector_key_registry=keys,
            trace_profile_identity=trace_identity,
            submission_connector_profile_identity=connector_identity,
            reason_code=(
                "submission_connector_unavailable"
                if connector_available else "submission_connector_missing"
            ),
        )
        return {
            "universe": universe,
            "registry": registry,
            "retained": retained,
            "trace_identity": trace_identity,
            "connector_identity": connector_identity,
            "native": native,
            "snapshot": snapshot,
            "phase_a": phase_a,
            "bound": bound,
            "missing_market_id": missing_market_id,
        }

    def _authenticate(self, fixture, bounds_by_route):
        snapshot = fixture["snapshot"]
        for member in snapshot["members"]:
            buy_bound, sell_bound = bounds_by_route[member["route_id"]]
            member.update({
                "status": "observed",
                "reason_code": None,
                "submission_mode": "private_relay",
                "policy_id": "kat-policy",
                "buy_submission_loss_bps": buy_bound,
                "sell_submission_loss_bps": sell_bound,
            })
        snapshot["member_set_sha256"] = typed_sha(
            b"route-cost-submission-policy-member-set/v1\n",
            snapshot["members"],
        )
        snapshot.update({
            "status": "authenticated",
            "reason_code": None,
            "connector_id": "kat_connector",
            "observed_at": "2026-08-01T12:00:00Z",
            "valid_until": "2026-08-01T12:05:00Z",
            "issuer_key_id": "kat-key",
            "signature_algorithm": "ssh-ed25519-sshsig-v1",
            "signature": SSHSIG_KAT_SIGNATURE,
        })
        snapshot["attested_payload_sha256"] = typed_sha(
            b"route-cost-submission-policy-attestation/v1\n",
            route_cost_evidence._policy_attestation(snapshot),
        )
        return fixture

    def _plan(self, fixture, terminal_reason_by_market=None):
        kwargs = {}
        if terminal_reason_by_market is not None:
            kwargs["terminal_reason_by_market"] = terminal_reason_by_market
        return route_cost_evidence.build_phase_b_scenario_request_plan(
            universe=fixture["universe"],
            run_id=RUN_ID,
            route_cohort_id=COHORT_ID,
            candidate_source_generation=GENERATION,
            route_universe_sha256=physical_sha(fixture["universe"]),
            adapter_registry=fixture["registry"],
            trace_profile_identity=fixture["trace_identity"],
            submission_connector_profile_identity=fixture[
                "connector_identity"
            ],
            retained_typed_pool_state_members=fixture["retained"],
            native_price_evidence=fixture["native"],
            submission_policy_snapshot=fixture["snapshot"],
            native_bound_phase_a_capture=fixture["bound"],
            **kwargs
        )

    def test_missing_supported_core_state_keeps_full_authority_denominator(self):
        fixture = self._fixture(missing_supported=True)
        plan = self._plan(fixture, {
            fixture["missing_market_id"]: "core_pool_state_unavailable",
        })
        self.assertEqual(plan["phase_a_rpc_call_count"], 11)
        self.assertEqual(len(plan["scenario_specs"]), 10)
        self.assertEqual(
            {row["market_id"] for row in plan["scenario_specs"]},
            {MARKET_ID},
        )
        self.assertEqual(
            [row["id"] for row in plan["estimate_requests"]],
            list(range(12, 22)),
        )

    def test_same_target_missing_core_suppresses_entire_phase_b_group(self):
        fixture = self._fixture(
            missing_supported=True, same_target_missing=True
        )
        plan = self._plan(fixture, {
            fixture["missing_market_id"]: "core_pool_state_unavailable",
        })
        self.assertEqual(plan["phase_a_rpc_call_count"], 11)
        self.assertEqual(plan["scenario_specs"], [])
        self.assertEqual(plan["estimate_requests"], [])
        self.assertEqual(
            route_cost_evidence.build_phase_b_trace_request_plan(
                scenario_plan=plan, estimate_responses=[]
            ),
            {
                "schema": "route_cost_phase_b_trace_plan/v1",
                "estimate_responses": [],
                "trace_requests": [],
            },
        )

    def test_one_market_plan_has_exact_ten_scenarios_and_reserved_ids(self):
        fixture = self._fixture()
        plan = self._plan(fixture)
        self.assertEqual(
            set(plan),
            {
                "schema", "phase_a_rpc_call_count", "scenario_specs",
                "estimate_requests",
            },
        )
        self.assertEqual(plan["schema"], "route_cost_phase_b_scenario_plan/v1")
        self.assertEqual(plan["phase_a_rpc_call_count"], 11)
        self.assertEqual(len(plan["scenario_specs"]), 10)
        self.assertEqual(len(plan["estimate_requests"]), 10)
        self.assertEqual(
            [row["estimate_request_id"] for row in plan["scenario_specs"]],
            list(range(12, 22)),
        )
        self.assertEqual(
            [row["trace_request_id"] for row in plan["scenario_specs"]],
            list(range(22, 32)),
        )
        self.assertEqual(
            [row["id"] for row in plan["estimate_requests"]],
            list(range(12, 22)),
        )
        self.assertEqual(
            [
                (row["direction"], row["requested_notional_usd"])
                for row in plan["scenario_specs"]
            ],
            [
                (direction, str(notional))
                for direction in ("buy", "sell")
                for notional in NOTIONALS
            ],
        )
        self.assertEqual(
            {row["submission_loss_bound_bps"] for row in plan["scenario_specs"]},
            {"100"},
        )
        for spec, request in zip(
            plan["scenario_specs"], plan["estimate_requests"]
        ):
            self.assertEqual(
                set(spec),
                {
                    "schema", "market_id", "direction",
                    "requested_notional_usd",
                    "simulation_target_token_address",
                    "simulation_target_unit_decimals",
                    "simulation_target_raw_quantity",
                    "simulation_target_lattice_raw",
                    "simulation_target_sha256", "core_pool_state_id",
                    "core_pool_state_sha256", "chain_evidence_sha256",
                    "market_evidence_sha256", "quoted_amount_in_raw",
                    "quoted_amount_out_raw", "submission_loss_bound_bps",
                    "calldata_hex", "state_overrides",
                    "estimate_request_id", "trace_request_id",
                },
            )
            self.assertEqual(
                request,
                {
                    "schema": "route_cost_estimate_gas_request/v1",
                    "jsonrpc": "2.0",
                    "id": spec["estimate_request_id"],
                    "method": "eth_estimateGas",
                    "params": [{
                        "from": SENDER,
                        "to": ROUTER,
                        "data": spec["calldata_hex"],
                        "value": "0x0",
                    }, "0x64", spec["state_overrides"]],
                },
            )

    def test_plan_rejects_phase_a_and_policy_lineage_transplants(self):
        fixture = self._fixture()
        bad_capture = copy.deepcopy(fixture)
        bad_capture["bound"]["market_evidence"][0][
            "chain_evidence_sha256"
        ] = "0" * 64
        with self.assertRaises(route_cost_evidence.RouteCostEvidenceError):
            self._plan(bad_capture)
        bad_policy = self._fixture()
        bad_policy["snapshot"]["run_id"] = "foreign-run"
        with self.assertRaises(route_cost_evidence.RouteCostEvidenceError):
            self._plan(bad_policy)

    def test_authenticated_bounds_are_direction_local(self):
        base_route = universe_for()["routes"][0]
        fixture = self._fixture(
            routes=[base_route], connector_available=True
        )
        self._authenticate(
            fixture, {base_route["route_id"]: ("250", None)}
        )
        plan = self._plan(fixture)
        by_direction = {
            direction: {
                row["submission_loss_bound_bps"]
                for row in plan["scenario_specs"]
                if row["direction"] == direction
            }
            for direction in ("buy", "sell")
        }
        self.assertEqual(by_direction, {"buy": {"250"}, "sell": {"100"}})

    def test_authenticated_buy_and_sell_routes_may_use_different_bounds(self):
        buy_route = copy.deepcopy(universe_for()["routes"][0])
        sell_route = copy.deepcopy(buy_route)
        sell_route.update({
            "route_id": "route:AAA:cex:x:AAA/USDT->{}:prepositioned_inventory".format(
                MARKET_ID
            ),
            "buy_market_id": "cex:x:AAA/USDT",
            "sell_market_id": MARKET_ID,
        })
        fixture = self._fixture(
            routes=[buy_route, sell_route], connector_available=True
        )
        self._authenticate(fixture, {
            buy_route["route_id"]: ("250", None),
            sell_route["route_id"]: (None, "400"),
        })
        plan = self._plan(fixture)
        by_direction = {
            direction: {
                row["submission_loss_bound_bps"]
                for row in plan["scenario_specs"]
                if row["direction"] == direction
            }
            for direction in ("buy", "sell")
        }
        self.assertEqual(by_direction, {"buy": {"250"}, "sell": {"400"}})


class PhaseBTraceBarrierPureProducerTests(unittest.TestCase):
    def _fixture(self):
        producer = PhaseBScenarioPureProducerTests()
        fixture = producer._fixture()
        plan = producer._plan(fixture)
        responses = [
            {"jsonrpc": "2.0", "id": request["id"], "result": "0x5208"}
            for request in reversed(plan["estimate_requests"])
        ]
        return plan, responses

    def test_complete_permuted_estimates_release_exact_trace_plan(self):
        scenario_plan, responses = self._fixture()
        trace_plan = route_cost_evidence.build_phase_b_trace_request_plan(
            scenario_plan=scenario_plan,
            estimate_responses=responses,
        )
        self.assertEqual(
            set(trace_plan),
            {"schema", "estimate_responses", "trace_requests"},
        )
        self.assertEqual(
            trace_plan["schema"], "route_cost_phase_b_trace_plan/v1"
        )
        self.assertEqual(
            [row["id"] for row in trace_plan["estimate_responses"]],
            list(range(12, 22)),
        )
        self.assertEqual(
            [row["id"] for row in trace_plan["trace_requests"]],
            list(range(22, 32)),
        )
        for spec, estimate, trace in zip(
            scenario_plan["scenario_specs"],
            trace_plan["estimate_responses"],
            trace_plan["trace_requests"],
        ):
            self.assertEqual(
                estimate,
                {
                    "schema": "route_cost_estimate_gas_response/v1",
                    "jsonrpc": "2.0",
                    "id": spec["estimate_request_id"],
                    "result": "0x5208",
                },
            )
            estimate_request = scenario_plan["estimate_requests"][
                spec["estimate_request_id"] - 12
            ]
            self.assertEqual(
                trace,
                {
                    "schema": "route_cost_trace_request/v1",
                    "jsonrpc": "2.0",
                    "id": spec["trace_request_id"],
                    "method": "debug_traceCall",
                    "params": [{
                        "from": SENDER,
                        "to": ROUTER,
                        "gas": "0x5208",
                        "data": spec["calldata_hex"],
                        "value": "0x0",
                    }, estimate_request["params"][1], {
                        "tracer": "prestateTracer",
                        "tracerConfig": {
                            "diffMode": True,
                            "disableCode": True,
                            "disableStorage": False,
                        },
                        "stateOverrides": spec["state_overrides"],
                    }],
                },
            )

    def test_barrier_rejects_partial_duplicate_wrong_type_and_nonminimal_result(self):
        scenario_plan, responses = self._fixture()
        cases = {
            "partial": responses[:-1],
            "duplicate": responses[:-1] + [copy.deepcopy(responses[0])],
            "bool-id": [
                dict(row, id=True) if index == 0 else row
                for index, row in enumerate(responses)
            ],
            "float-id": [
                dict(row, id=float(row["id"])) if index == 0 else row
                for index, row in enumerate(responses)
            ],
            "zero": [
                dict(row, result="0x0") if index == 0 else row
                for index, row in enumerate(responses)
            ],
            "nonminimal": [
                dict(row, result="0x05208") if index == 0 else row
                for index, row in enumerate(responses)
            ],
        }
        for label, candidate in cases.items():
            with self.subTest(case=label), self.assertRaises(
                route_cost_evidence.RouteCostEvidenceError
            ):
                route_cost_evidence.build_phase_b_trace_request_plan(
                    scenario_plan=scenario_plan,
                    estimate_responses=candidate,
                )


class PhaseBCaptureProjectorPureProducerTests(unittest.TestCase):
    def _fixture(self, *, missing_supported=False,
                 same_target_missing=False):
        producer = PhaseBScenarioPureProducerTests()
        fixture = producer._fixture(
            missing_supported=missing_supported,
            same_target_missing=same_target_missing,
        )
        terminal_reasons = (
            {
                fixture["missing_market_id"]:
                "core_pool_state_unavailable",
            }
            if missing_supported else None
        )
        scenario_plan = producer._plan(fixture, terminal_reasons)
        estimate_responses = [
            {"jsonrpc": "2.0", "id": request["id"], "result": "0x5208"}
            for request in scenario_plan["estimate_requests"]
        ]
        trace_plan = route_cost_evidence.build_phase_b_trace_request_plan(
            scenario_plan=scenario_plan,
            estimate_responses=estimate_responses,
        )
        state = json.loads(fixture["retained"][MARKET_ID]["payload"])
        current_adapter = fixture["registry"]["adapters"][0]
        responses = []
        for spec, request in zip(
            scenario_plan["scenario_specs"], trace_plan["trace_requests"]
        ):
            decoded = route_cost_evidence.decode_v2_swap_calldata(
                spec["calldata_hex"]
            )
            token_in, token_out = decoded["path"]
            amount_in = int(spec["quoted_amount_in_raw"])
            amount_out = int(spec["quoted_amount_out_raw"])
            descriptor_in = next(
                row for row in current_adapter["token_funding_descriptors"]
                if row["token_address"] == token_in
            )
            descriptor_out = next(
                row for row in current_adapter["token_funding_descriptors"]
                if row["token_address"] == token_out
            )
            allowance = route_cost_evidence.solidity_allowance_storage_key(
                SENDER, ROUTER, int(descriptor_in["allowance_mapping_slot"])
            )
            storage_diffs = [
                {
                    "token_address": token_in,
                    "account_role": "sender",
                    "storage_key": route_cost_evidence.solidity_balance_storage_key(
                        SENDER, int(descriptor_in["balance_mapping_slot"])
                    ),
                    "pre_present": True,
                    "pre_value": _word(amount_in),
                    "post_present": True,
                    "post_value": _word(0),
                },
                {
                    "token_address": token_in,
                    "account_role": "sender",
                    "storage_key": allowance,
                    "pre_present": True,
                    "pre_value": _word(decoded.get(
                        "amount_in_raw", decoded.get("amount_in_max_raw")
                    )),
                    "post_present": True,
                    "post_value": _word(
                        decoded.get(
                            "amount_in_raw", decoded.get("amount_in_max_raw")
                        ) - amount_in
                    ),
                },
                {
                    "token_address": token_in,
                    "account_role": "pair",
                    "storage_key": route_cost_evidence.solidity_balance_storage_key(
                        state["pool_address"],
                        int(descriptor_in["balance_mapping_slot"]),
                    ),
                    "pre_present": True,
                    "pre_value": _word(10 ** 30),
                    "post_present": True,
                    "post_value": _word(10 ** 30 + amount_in),
                },
                {
                    "token_address": token_out,
                    "account_role": "pair",
                    "storage_key": route_cost_evidence.solidity_balance_storage_key(
                        state["pool_address"],
                        int(descriptor_out["balance_mapping_slot"]),
                    ),
                    "pre_present": True,
                    "pre_value": _word(10 ** 30),
                    "post_present": True,
                    "post_value": _word(10 ** 30 - amount_out),
                },
                {
                    "token_address": token_out,
                    "account_role": "recipient",
                    "storage_key": route_cost_evidence.solidity_balance_storage_key(
                        SENDER, int(descriptor_out["balance_mapping_slot"])
                    ),
                    "pre_present": True,
                    "pre_value": _word(0),
                    "post_present": True,
                    "post_value": _word(amount_out),
                },
            ]
            storage_diffs.sort(key=lambda row: (
                row["token_address"], row["account_role"], row["storage_key"]
            ))
            responses.append({
                "schema": "route_cost_trace_response/v1",
                "jsonrpc": "2.0",
                "id": request["id"],
                "storage_diffs": storage_diffs,
            })
        return (
            fixture, scenario_plan, trace_plan, list(reversed(responses)),
            terminal_reasons,
        )

    def _project(self, fixture, scenario_plan, trace_plan, responses,
                 terminal_reason_by_market=None):
        kwargs = {}
        if terminal_reason_by_market is not None:
            kwargs["terminal_reason_by_market"] = terminal_reason_by_market
        return route_cost_evidence.project_phase_b_capture(
            universe=fixture["universe"],
            run_id=RUN_ID,
            route_cohort_id=COHORT_ID,
            candidate_source_generation=GENERATION,
            route_universe_sha256=physical_sha(fixture["universe"]),
            adapter_registry=fixture["registry"],
            trace_profile_identity=fixture["trace_identity"],
            submission_connector_profile_identity=fixture[
                "connector_identity"
            ],
            retained_typed_pool_state_members=fixture["retained"],
            native_price_evidence=fixture["native"],
            submission_policy_snapshot=fixture["snapshot"],
            native_bound_phase_a_capture=fixture["bound"],
            scenario_plan=scenario_plan,
            trace_plan=trace_plan,
            trace_responses=responses,
            captured_started_at="2026-08-01T12:00:01Z",
            captured_finished_at="2026-08-01T12:00:02Z",
            **kwargs
        )

    def test_projects_ten_observed_transcripts_from_permuted_trace_results(self):
        fixture, scenario_plan, trace_plan, responses, reasons = self._fixture()
        rows = self._project(fixture, scenario_plan, trace_plan, responses)
        self.assertEqual(len(rows), 10)
        self.assertEqual(
            {(row["status"], row["completed_stage"], row["reason_code"])
             for row in rows},
            {("observed", "transfer_tax", None)},
        )
        self.assertEqual(
            [(row["direction"], row["requested_notional_usd"]) for row in rows],
            [(direction, str(notional)) for direction in ("buy", "sell")
             for notional in NOTIONALS],
        )
        for row in rows:
            self.assertEqual(row["transfer_tax_evidence"]["status"], "not_applicable")
            self.assertEqual(row["router_fee_evidence"]["status"], "not_applicable")
            self.assertEqual(row["gas_evidence"]["gas_units"], "21000")

    def test_projector_replays_missing_supported_core_state_closed_set(self):
        fixture, scenario_plan, trace_plan, responses, reasons = self._fixture(
            missing_supported=True
        )
        rows = self._project(
            fixture, scenario_plan, trace_plan, responses, reasons
        )
        self.assertEqual(len(rows), 10)
        self.assertEqual({row["market_id"] for row in rows}, {MARKET_ID})

    def test_projector_closes_same_target_retained_sibling_at_block(self):
        fixture, scenario_plan, trace_plan, responses, reasons = self._fixture(
            missing_supported=True, same_target_missing=True
        )
        rows = self._project(
            fixture, scenario_plan, trace_plan, responses, reasons
        )
        self.assertEqual(len(rows), 10)
        self.assertEqual({row["market_id"] for row in rows}, {MARKET_ID})
        for row in rows:
            self.assertEqual(
                (row["status"], row["completed_stage"], row["reason_code"]),
                ("unavailable", "block", "calldata_unavailable"),
            )
            self.assertTrue(all(
                row[field] is None for field in (
                    "simulation_target_token_address",
                    "simulation_target_unit_decimals",
                    "simulation_target_raw_quantity",
                    "simulation_target_lattice_raw",
                    "simulation_target_sha256",
                )
            ))
            self.assertIsNotNone(row["core_pool_state_id"])
            self.assertIsNotNone(row["core_pool_state_sha256"])
            self.assertIsNotNone(row["chain_evidence_sha256"])
            self.assertIsNotNone(row["market_evidence_sha256"])
            self.assertIsNotNone(row["block_evidence"])
            self.assertIsNotNone(row["raw_transcript"])
            self.assertIsNone(row["raw_transcript"]["calldata_hex"])
            self.assertTrue(all(
                row["raw_transcript"][field] is None for field in (
                    "estimate_gas_request", "estimate_gas_response",
                    "simulation_method", "simulation_request",
                    "simulation_response", "simulation_balance_deltas",
                )
            ))
            self.assertTrue(all(
                row[field] is None for field in (
                    "call_evidence", "gas_evidence",
                    "router_fee_evidence", "transfer_tax_evidence",
                )
            ))

    def test_projector_rejects_response_and_plan_tampering(self):
        fixture, scenario_plan, trace_plan, responses, reasons = self._fixture()
        wrong_id = copy.deepcopy(responses)
        wrong_id[0]["id"] += 1
        with self.assertRaises(route_cost_evidence.RouteCostEvidenceError):
            self._project(fixture, scenario_plan, trace_plan, wrong_id)
        wrong_amount = copy.deepcopy(scenario_plan)
        wrong_amount["scenario_specs"][0]["quoted_amount_in_raw"] = str(
            int(wrong_amount["scenario_specs"][0]["quoted_amount_in_raw"]) + 1
        )
        with self.assertRaises(route_cost_evidence.RouteCostEvidenceError):
            self._project(fixture, wrong_amount, trace_plan, responses)

    def test_projector_requires_native_validity_to_cover_capture_window(self):
        book_raw, rules_raw = native_price_captured_bytes()
        late_native = route_cost_evidence.build_native_price_evidence_from_captured(
            run_id=RUN_ID,
            route_cohort_id=COHORT_ID,
            candidate_source_generation=GENERATION,
            book_raw_response=book_raw,
            book_observed_at="2026-08-01T12:00:02Z",
            market_rules_raw_response=rules_raw,
            market_rules_observed_at="2026-08-01T12:00:01Z",
        )
        producer = PhaseBScenarioPureProducerTests()
        fixture = producer._fixture(native=late_native)
        scenario_plan = producer._plan(fixture)
        estimate_responses = [
            {"jsonrpc": "2.0", "id": request["id"], "result": "0x5208"}
            for request in scenario_plan["estimate_requests"]
        ]
        trace_plan = route_cost_evidence.build_phase_b_trace_request_plan(
            scenario_plan=scenario_plan,
            estimate_responses=estimate_responses,
        )
        (
            _base_fixture, _base_scenarios, _base_traces, responses,
            _base_reasons,
        ) = self._fixture()
        with self.assertRaisesRegex(
            route_cost_evidence.RouteCostEvidenceError,
            "native validity",
        ):
            self._project(fixture, scenario_plan, trace_plan, responses)


class FieldContractTests(unittest.TestCase):
    def test_exported_field_tuples_are_literal_and_non_reflective(self):
        self.assertEqual(
            route_cost_evidence.ROUTE_COST_EVIDENCE_FIELDS,
            (
                "schema", "run_id", "route_cohort_id", "phase",
                "candidate_source_generation", "route_universe_sha256",
                "adapter_registry", "adapter_registry_sha256",
                "connector_key_registry", "connector_key_registry_sha256",
                "transcript_count", "trace_profile_identity",
                "trace_profile_generation",
                "submission_connector_profile_identity",
                "submission_connector_profile_generation", "evaluated_at",
                "selected_market_count", "selected_markets",
                "selected_market_set_sha256", "native_price_evidence",
                "native_price_evidence_sha256", "chain_evidence_count",
                "chain_evidence", "chain_evidence_set_sha256",
                "market_evidence_count", "market_evidence",
                "market_evidence_set_sha256", "transcripts",
                "transcript_set_sha256", "binding_count", "bindings",
                "binding_set_sha256", "submission_policy_snapshot",
                "submission_policy_snapshot_sha256", "counts",
            ),
        )
        self.assertEqual(
            route_cost_evidence.BLOCK_EVIDENCE_FIELDS,
            (
                "schema", "chain_evidence_sha256", "market_evidence_sha256",
                "chain_id", "block_tag", "block_number", "block_hash",
                "block_timestamp", "core_pool_state_id",
                "router_runtime_code_sha256", "factory_runtime_code_sha256",
                "pair_runtime_code_sha256", "rpc_transcript_sha256",
            ),
        )
        self.assertEqual(
            route_cost_evidence.RAW_TRANSCRIPT_FIELDS,
            (
                "schema", "chain_evidence_sha256", "market_evidence_sha256",
                "captured_started_at", "captured_finished_at", "calldata_hex",
                "estimate_gas_request", "estimate_gas_response",
                "simulation_method", "simulation_request",
                "simulation_response", "simulation_balance_deltas",
            ),
        )
        self.assertEqual(
            route_cost_evidence.MARKET_EVIDENCE_FIELDS,
            (
                "schema", "run_id", "route_cohort_id",
                "candidate_source_generation", "route_universe_sha256",
                "adapter_registry_sha256", "selected_market_set_sha256",
                "market_id", "adapter_id", "chain_evidence_sha256",
                "core_pool_state_id", "core_pool_state_sha256",
                "router_address", "router_runtime_code", "factory_address",
                "factory_runtime_code", "factory_get_pair_request",
                "factory_get_pair_response", "pair_address",
                "pair_runtime_code", "pair_token0", "pair_token1",
                "token_runtime_code_evidence",
                "captured_started_at", "captured_finished_at",
            ),
        )
        self.assertEqual(
            route_cost_evidence.TOKEN_RUNTIME_CODE_EVIDENCE_FIELDS,
            ("schema", "token_address", "request", "response"),
        )
        self.assertEqual(
            route_cost_evidence.TOKEN_RUNTIME_CODE_REQUEST_FIELDS,
            ("schema", "jsonrpc", "id", "method", "params"),
        )
        self.assertEqual(
            route_cost_evidence.TOKEN_RUNTIME_CODE_RESPONSE_FIELDS,
            ("schema", "jsonrpc", "id", "result"),
        )
        self.assertEqual(
            route_cost_evidence.TRANSCRIPT_FIELDS,
            (
                "schema", "run_id", "route_cohort_id",
                "candidate_source_generation", "route_universe_sha256",
                "adapter_registry_sha256", "selected_market_set_sha256",
                "trace_profile_generation",
                "submission_connector_profile_generation", "market_id",
                "direction", "requested_notional_usd", "adapter_id",
                "simulation_target_token_address",
                "simulation_target_unit_decimals",
                "simulation_target_raw_quantity",
                "simulation_target_lattice_raw",
                "simulation_target_sha256", "core_pool_state_id",
                "core_pool_state_sha256", "chain_evidence_sha256",
                "market_evidence_sha256", "status", "completed_stage",
                "reason_code", "block_evidence", "call_evidence",
                "gas_evidence", "router_fee_evidence",
                "transfer_tax_evidence", "raw_transcript",
            ),
        )
        self.assertEqual(
            route_cost_evidence.POLICY_REQUEST_FIELDS,
            (
                "schema", "request_id", "run_id", "route_cohort_id",
                "candidate_source_generation", "route_universe_sha256",
                "selected_market_set_sha256", "adapter_registry_sha256",
                "connector_key_registry_sha256", "trace_profile_generation",
                "submission_connector_profile_generation", "connector_id",
                "members",
            ),
        )
        self.assertEqual(
            route_cost_evidence.POLICY_REQUEST_MEMBER_FIELDS,
            ("route_id", "requested_notional_usd"),
        )

    def test_factory_get_pair_replays_fixed_block_tokens_and_pair(self):
        request = {
            "schema": "route_cost_factory_get_pair_request/v1",
            "jsonrpc": "2.0",
            "id": 7,
            "method": "eth_call",
            "params": [
                {
                    "to": FACTORY,
                    "data": route_cost_evidence.build_factory_get_pair_calldata(
                        TOKEN_A, TOKEN_B
                    ),
                },
                "0x64",
            ],
        }
        response = {
            "schema": "route_cost_factory_get_pair_response/v1",
            "jsonrpc": "2.0",
            "id": 7,
            "result": "0x" + "0" * 24 + POOL[2:],
        }
        route_cost_evidence._validate_factory_get_pair_evidence(
            request,
            response,
            adapter=adapter(),
            chain={"block_header_result": {"number": "0x64"}},
            token0=TOKEN_A,
            token1=TOKEN_B,
            pair=POOL,
        )
        mutations = (
            ("block", lambda req, res: req["params"].__setitem__(1, "0x65")),
            (
                "token",
                lambda req, res: req["params"][0].__setitem__(
                    "data",
                    route_cost_evidence.build_factory_get_pair_calldata(
                        TOKEN_A, "0x" + "4" * 40
                    ),
                ),
            ),
            (
                "pair",
                lambda req, res: res.__setitem__(
                    "result", "0x" + "0" * 24 + "4" * 40
                ),
            ),
        )
        for label, mutate in mutations:
            forged_request = copy.deepcopy(request)
            forged_response = copy.deepcopy(response)
            mutate(forged_request, forged_response)
            with self.subTest(label=label):
                with self.assertRaises(route_cost_evidence.RouteCostEvidenceError):
                    route_cost_evidence._validate_factory_get_pair_evidence(
                        forged_request,
                        forged_response,
                        adapter=adapter(),
                        chain={"block_header_result": {"number": "0x64"}},
                        token0=TOKEN_A,
                        token1=TOKEN_B,
                        pair=POOL,
                    )

    def test_pinned_runtime_code_known_answers_match_tracked_identities(self):
        router = pinned_runtime_code("uniswap-v2-router02-runtime")
        factory = pinned_runtime_code("uniswap-v2-factory-runtime")
        pair = pinned_pair_runtime_code()
        self.assertEqual(len(router), 21_943)
        self.assertEqual(len(factory), 13_859)
        self.assertEqual(len(pair), 11_293)
        self.assertEqual(
            hashlib.sha256(router).hexdigest(),
            route_cost_evidence.ETHEREUM_V2_ROUTER_RUNTIME_CODE_SHA256,
        )
        self.assertEqual(
            hashlib.sha256(factory).hexdigest(),
            route_cost_evidence.ETHEREUM_V2_FACTORY_RUNTIME_CODE_SHA256,
        )
        self.assertEqual(
            hashlib.sha256(pair).hexdigest(),
            "8b5db55fa9ab3b9527508d4abe0b39eb588bf310270c8e04b3f38214e8ba63b4",
        )

    def test_tracked_registries_are_canonical_and_strictly_validated(self):
        root = Path(__file__).resolve().parents[1]
        adapter_bytes = (root / "config/route_cost_adapters.json").read_bytes()
        key_bytes = (root / "config/route_cost_connector_keys.json").read_bytes()
        self.assertTrue(adapter_bytes.endswith(b"\n"))
        self.assertTrue(key_bytes.endswith(b"\n"))
        adapter_value = json.loads(adapter_bytes)
        key_value = json.loads(key_bytes)
        self.assertEqual(adapter_bytes, canonical_bytes(adapter_value) + b"\n")
        self.assertEqual(key_bytes, canonical_bytes(key_value) + b"\n")
        route_cost_evidence.validate_adapter_registry(adapter_value)
        route_cost_evidence.validate_connector_key_registry(key_value)
        self.assertEqual(
            route_cost_evidence.load_route_cost_adapter_registry(),
            adapter_value,
        )
        self.assertEqual(
            route_cost_evidence.load_route_cost_connector_key_registry(),
            key_value,
        )

    def test_production_uni_weth_authority_records_exactly_bind_descriptors(self):
        root = Path(__file__).resolve().parents[1]
        authority_root = root / "config" / "route_cost_authority"
        expected_names = [
            "pair-0xd3d2e2692501a5c9ca623199d38826e513033a17.json",
            "token-0x1f9840a85d5af5bf1d1762f925bdaddc4201f984.json",
            "token-0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2.json",
        ]
        self.assertEqual(
            sorted(path.name for path in authority_root.glob("*.json")),
            expected_names,
        )
        records = {}
        for name in expected_names:
            raw = (authority_root / name).read_bytes()
            value = json.loads(raw)
            self.assertEqual(raw, canonical_bytes(value) + b"\n")
            records[name] = (
                route_cost_evidence.validate_route_cost_authority_record(value),
                hashlib.sha256(raw).hexdigest(),
            )

        registry = route_cost_evidence.load_route_cost_adapter_registry()
        adapter_value = registry["adapters"][0]
        self.assertEqual(adapter_value["connector_family"],
                         "private_submission_connector/v1")
        self.assertEqual(
            route_cost_evidence.load_route_cost_connector_key_registry()["keys"],
            [],
        )
        self.assertEqual(adapter_value["pair_descriptors"], [{
            "pair_address": "0xd3d2e2692501a5c9ca623199d38826e513033a17",
            "token0_address": "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984",
            "token1_address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
            "pair_runtime_code_sha256": (
                "8b5db55fa9ab3b9527508d4abe0b39eb588bf310270c8e04b3f38214e8ba63b4"
            ),
            "source_metadata_sha256": records[expected_names[0]][1],
        }])
        self.assertEqual(adapter_value["token_funding_descriptors"], [
            {
                "token_address": "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984",
                "runtime_code_sha256": (
                    "77ea2b530607db6cb87c7cce18016aa12dd0762c4357355bceee2cb11721bebe"
                ),
                "proxy_implementation_address": None,
                "proxy_implementation_code_sha256": None,
                "storage_layout": "solidity_mapping_v1",
                "balance_mapping_slot": "4",
                "allowance_mapping_slot": "3",
                "source_metadata_sha256": records[expected_names[1]][1],
            },
            {
                "token_address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
                "runtime_code_sha256": (
                    "5566bf50796faf93c9b6f6adacd3b32c70bfe16b48ffc59db6cd144cbdc89739"
                ),
                "proxy_implementation_address": None,
                "proxy_implementation_code_sha256": None,
                "storage_layout": "solidity_mapping_v1",
                "balance_mapping_slot": "3",
                "allowance_mapping_slot": "4",
                "source_metadata_sha256": records[expected_names[2]][1],
            },
        ])
        pair_record = records[expected_names[0]][0]
        self.assertEqual(pair_record["block_number"], 20_000_000)
        self.assertEqual(
            pair_record["block_hash"],
            "0xd24fd73f794058a3807db926d8898c6481e902b7edb91ce0d479d6760f276183",
        )
        self.assertEqual(pair_record["runtime_code_size"], 11_293)
        self.assertEqual(records[expected_names[1]][0]["runtime_code_size"], 12_567)
        self.assertEqual(records[expected_names[2]][0]["runtime_code_size"], 3_124)

    def test_authority_record_schema_is_closed_and_semantically_validated(self):
        root = Path(__file__).resolve().parents[1]
        path = (
            root / "config" / "route_cost_authority"
            / "token-0x1f9840a85d5af5bf1d1762f925bdaddc4201f984.json"
        )
        value = json.loads(path.read_bytes())
        mutations = []
        extra = copy.deepcopy(value)
        extra["unexpected"] = True
        mutations.append(extra)
        wrong_slot_key = copy.deepcopy(value)
        wrong_slot_key["balance_probe_storage_key"] = "0x" + "0" * 64
        mutations.append(wrong_slot_key)
        wrong_probe = copy.deepcopy(value)
        wrong_probe["balance_probe_storage_result"] = "0x" + "0" * 64
        mutations.append(wrong_probe)
        wrong_block = copy.deepcopy(value)
        wrong_block["block_number"] = 20_000_001
        mutations.append(wrong_block)
        for mutated in mutations:
            with self.subTest(field=next(
                key for key in mutated if mutated.get(key) != value.get(key)
            )):
                with self.assertRaises(route_cost_evidence.RouteCostEvidenceError):
                    route_cost_evidence.validate_route_cost_authority_record(mutated)

    def test_production_loader_rejects_authority_hash_symlink_and_hardlink(self):
        root = Path(__file__).resolve().parents[1]
        source = root / "config" / "route_cost_authority"
        uni_name = "token-0x1f9840a85d5af5bf1d1762f925bdaddc4201f984.json"
        for attack in ("hash", "leaf_symlink", "hardlink", "ancestor_symlink"):
            with self.subTest(attack=attack), tempfile.TemporaryDirectory() as temporary:
                temporary_root = Path(temporary).resolve()
                authority = temporary_root / "authority"
                shutil.copytree(str(source), str(authority))
                path = authority / uni_name
                if attack == "hash":
                    value = json.loads(path.read_bytes())
                    value["source_sha256"] = "0" * 64
                    path.write_bytes(canonical_bytes(value) + b"\n")
                elif attack == "leaf_symlink":
                    target = temporary_root / "target.json"
                    shutil.copyfile(str(path), str(target))
                    path.unlink()
                    path.symlink_to(target)
                elif attack == "hardlink":
                    os.link(str(path), str(temporary_root / "second-link.json"))
                else:
                    alias = temporary_root / "authority-alias"
                    alias.symlink_to(authority, target_is_directory=True)
                    authority = alias
                with mock.patch.object(
                    route_cost_evidence, "_AUTHORITY_RECORD_ROOT", authority
                ):
                    with self.assertRaisesRegex(
                        route_cost_evidence.RouteCostEvidenceError,
                        "authority|unsafe|unavailable|differs",
                    ):
                        route_cost_evidence.load_route_cost_adapter_registry()

    def test_production_loader_rejects_authority_ancestor_swap(self):
        root = Path(__file__).resolve().parents[1]
        source = root / "config" / "route_cost_authority"
        with tempfile.TemporaryDirectory() as temporary:
            temporary_root = Path(temporary).resolve()
            authority = temporary_root / "authority"
            shutil.copytree(str(source), str(authority))
            original_read = os.read
            swapped = [False]

            def swap_during_authority_read(descriptor, size):
                chunk = original_read(descriptor, size)
                if not swapped[0] and b'"authority_id"' in chunk:
                    authority.rename(temporary_root / "authority-old")
                    shutil.copytree(str(source), str(authority))
                    swapped[0] = True
                return chunk

            with mock.patch.object(
                route_cost_evidence, "_AUTHORITY_RECORD_ROOT", authority
            ), mock.patch.object(
                route_cost_evidence.os,
                "read",
                side_effect=swap_during_authority_read,
            ):
                with self.assertRaisesRegex(
                    route_cost_evidence.RouteCostEvidenceError,
                    "changed|unsafe|ancestor|unavailable",
                ):
                    route_cost_evidence.load_route_cost_adapter_registry()
            self.assertTrue(swapped[0])

    def test_tracked_registry_loader_rejects_symlinked_ancestor(self):
        value = {"registry": "test"}
        data = canonical_bytes(value) + b"\n"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            tracked = root / "tracked"
            (tracked / "config").mkdir(parents=True)
            (tracked / "config" / "registry.json").write_bytes(data)
            alias = root / "tracked-alias"
            alias.symlink_to(tracked, target_is_directory=True)
            with self.assertRaisesRegex(
                route_cost_evidence.RouteCostEvidenceError,
                "unsafe|ancestor|unavailable",
            ):
                route_cost_evidence._load_tracked_registry(
                    alias / "config" / "registry.json",
                    maximum_bytes=1024,
                    expected_value=value,
                    validator=lambda item: item,
                    label="test registry",
                )

    def test_tracked_registry_loader_rejects_ancestor_swap_during_read(self):
        value = {"payload": "x" * 70000}
        data = canonical_bytes(value) + b"\n"
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve()
            config = root / "config"
            config.mkdir()
            path = config / "registry.json"
            path.write_bytes(data)
            original_read = os.read
            swapped = [False]

            def swap_after_first_read(descriptor, size):
                chunk = original_read(descriptor, size)
                if not swapped[0]:
                    config.rename(root / "config-old")
                    config.mkdir()
                    (config / "registry.json").write_bytes(data)
                    swapped[0] = True
                return chunk

            with mock.patch.object(
                route_cost_evidence.os, "read", side_effect=swap_after_first_read
            ):
                with self.assertRaisesRegex(
                    route_cost_evidence.RouteCostEvidenceError,
                    "changed|unsafe|ancestor|unavailable",
                ):
                    route_cost_evidence._load_tracked_registry(
                        path,
                        maximum_bytes=128 * 1024,
                        expected_value=value,
                        validator=lambda item: item,
                        label="test registry",
                    )
            self.assertTrue(swapped[0])

    def test_tracked_registry_loader_closes_owned_child_when_ancestor_probe_fails(self):
        value = {"registry": "test"}
        data = canonical_bytes(value) + b"\n"
        for failing_probe in ("fstat", "stat"):
            with self.subTest(failing_probe=failing_probe), tempfile.TemporaryDirectory() as temporary:
                path = Path(temporary).resolve() / "registry.json"
                path.write_bytes(data)
                original_open = os.open
                original_close = os.close
                original_fstat = os.fstat
                original_stat = os.stat
                opened = []
                closed = []

                def recording_open(*args, **kwargs):
                    descriptor = original_open(*args, **kwargs)
                    opened.append(descriptor)
                    return descriptor

                def recording_close(descriptor):
                    closed.append(descriptor)
                    return original_close(descriptor)

                def failing_fstat(descriptor):
                    raise OSError("ancestor probe failed")

                def failing_stat(*args, **kwargs):
                    raise OSError("ancestor probe failed")

                patches = [
                    mock.patch.object(route_cost_evidence.os, "open", side_effect=recording_open),
                    mock.patch.object(route_cost_evidence.os, "close", side_effect=recording_close),
                ]
                if failing_probe == "fstat":
                    patches.append(mock.patch.object(
                        route_cost_evidence.os, "fstat", side_effect=failing_fstat
                    ))
                else:
                    patches.append(mock.patch.object(
                        route_cost_evidence.os, "fstat", side_effect=original_fstat
                    ))
                    patches.append(mock.patch.object(
                        route_cost_evidence.os, "stat", side_effect=failing_stat
                    ))
                with patches[0], patches[1], patches[2]:
                    if len(patches) == 4:
                        patches[3].start()
                    try:
                        with self.assertRaisesRegex(
                            route_cost_evidence.RouteCostEvidenceError,
                            "unavailable|unsafe",
                        ):
                            route_cost_evidence._load_tracked_registry(
                                path,
                                maximum_bytes=1024,
                                expected_value=value,
                                validator=lambda item: item,
                                label="test registry",
                            )
                    finally:
                        if len(patches) == 4:
                            patches[3].stop()
                        for descriptor in opened:
                            try:
                                original_close(descriptor)
                            except OSError:
                                pass
                self.assertGreaterEqual(len(opened), 2)
                self.assertEqual(closed, list(reversed(opened)))

    def test_tracked_registry_loader_preserves_base_exceptions_and_attempts_all_closes(self):
        value = {"registry": "test"}
        data = canonical_bytes(value) + b"\n"
        for primary in (KeyboardInterrupt, SystemExit):
            with self.subTest(primary=primary.__name__), tempfile.TemporaryDirectory() as temporary:
                path = Path(temporary).resolve() / "registry.json"
                path.write_bytes(data)
                original_open = os.open
                original_close = os.close
                opened = []
                close_attempts = []

                def recording_open(*args, **kwargs):
                    descriptor = original_open(*args, **kwargs)
                    opened.append(descriptor)
                    return descriptor

                def interrupting_fstat(_descriptor):
                    raise primary("primary interruption")

                def first_close_fails(descriptor):
                    close_attempts.append(descriptor)
                    original_close(descriptor)
                    if len(close_attempts) == 1:
                        raise OSError("/private/secret/cleanup-path")

                with mock.patch.object(
                    route_cost_evidence.os, "open", side_effect=recording_open
                ), mock.patch.object(
                    route_cost_evidence.os, "fstat", side_effect=interrupting_fstat
                ), mock.patch.object(
                    route_cost_evidence.os, "close", side_effect=first_close_fails
                ):
                    with self.assertRaises(primary) as caught:
                        route_cost_evidence._load_tracked_registry(
                            path,
                            maximum_bytes=1024,
                            expected_value=value,
                            validator=lambda item: item,
                            label="test registry",
                        )
                for descriptor in opened:
                    try:
                        original_close(descriptor)
                    except OSError:
                        pass
                self.assertEqual(str(caught.exception), "primary interruption")
                self.assertGreaterEqual(len(opened), 2)
                self.assertEqual(close_attempts, list(reversed(opened)))

    def test_tracked_registry_loader_close_failure_is_sanitized_and_never_retried(self):
        value = {"registry": "test"}
        data = canonical_bytes(value) + b"\n"
        secret = "/private/secret/trace-profile.json"
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary).resolve() / "registry.json"
            path.write_bytes(data)
            original_open = os.open
            original_close = os.close
            opened = []
            close_attempts = []

            def recording_open(*args, **kwargs):
                descriptor = original_open(*args, **kwargs)
                opened.append(descriptor)
                return descriptor

            def first_close_fails(descriptor):
                close_attempts.append(descriptor)
                original_close(descriptor)
                if len(close_attempts) == 1:
                    raise OSError(secret)

            with mock.patch.object(
                route_cost_evidence.os, "open", side_effect=recording_open
            ), mock.patch.object(
                route_cost_evidence.os, "close", side_effect=first_close_fails
            ):
                with self.assertRaises(
                    route_cost_evidence.RouteCostEvidenceError
                ) as caught:
                    route_cost_evidence._load_tracked_registry(
                        path,
                        maximum_bytes=1024,
                        expected_value=value,
                        validator=lambda item: item,
                        label="test registry",
                    )
            for descriptor in opened:
                try:
                    original_close(descriptor)
                except OSError:
                    pass
            self.assertNotIn(secret, str(caught.exception))
            self.assertIn("cleanup", str(caught.exception))
            self.assertEqual(close_attempts, list(reversed(opened)))
            self.assertEqual(len(close_attempts), len(set(close_attempts)))


class ProfileIdentityTests(unittest.TestCase):
    def test_profile_generation_omits_url_authorization_and_secret_bytes(self):
        first = {
            "schema": "route_cost_trace_rpc_profile/v1",
            "profile_id": "trace-primary",
            "endpoint_id": "rpc-mainnet-a",
            "rpc_url": "https://rpc.example.test/SECRET_ONE",
            "authorization": "Bearer SECRET_ONE",
        }
        second = dict(first)
        second["rpc_url"] = "https://other.example.test/hidden"
        second["authorization"] = "Bearer SECRET_TWO"
        identity_a, generation_a = route_cost_evidence.trace_profile_identity(first)
        identity_b, generation_b = route_cost_evidence.trace_profile_identity(second)
        self.assertEqual(identity_a, identity_b)
        self.assertEqual(generation_a, generation_b)
        rendered = canonical_bytes(identity_a) + generation_a.encode("ascii")
        self.assertNotIn(b"SECRET", rendered)
        self.assertNotIn(b"https://", rendered)

    def test_missing_profile_has_fixed_typed_generation_and_exact_nulls(self):
        identity, generation = route_cost_evidence.trace_profile_identity(None)
        expected = {
            "schema": "route_cost_trace_profile_identity/v1",
            "status": "missing",
            "profile_id": None,
            "endpoint_id": None,
        }
        self.assertEqual(identity, expected)
        self.assertEqual(
            generation,
            typed_sha(b"route-cost-trace-profile-identity/v1\n", expected),
        )


class SelectedMarketTests(unittest.TestCase):
    def test_production_token_ids_replay_and_colon_alias_is_rejected(self):
        leg = universe_for()["selected_legs"][0]
        self.assertEqual(
            route_cost_evidence._market_token_addresses(leg),
            (TOKEN_A, TOKEN_B),
        )
        forged = copy.deepcopy(leg)
        forged["collector_context"]["base_token_id"] = "eth:" + TOKEN_A
        self.assertIsNone(route_cost_evidence._market_token_addresses(forged))

    def test_real_eth_uniswap_v2_selector_keeps_unsupported_and_is_permutation_stable(self):
        markets = []
        routes = []
        for index in range(10):
            token = "T{:02d}".format(index)
            pool = "0x{:040x}".format(index + 10)
            market_id = "dex:eth:uniswap_v2:{}:{}".format(pool, token)
            leg = copy.deepcopy(universe_for()["selected_legs"][0])
            leg["market_id"] = market_id
            leg["token_symbol"] = token
            leg["selection_rank"] = 1
            leg["selection_inputs"]["dex_24h_usd"] = str(100 - index)
            leg["selection_inputs"]["dex_tvl_usd"] = str(200 - index)
            leg["collector_context"] = context(
                "0x{:040x}".format(index + 100),
                "0x{:040x}".format(index + 200),
            )
            markets.append(leg)
            routes.append(
                {
                    "route_id": "route-{:02d}".format(index),
                    "route_class": "candidate",
                    "buy_market_id": market_id,
                    "sell_market_id": "cex:x:{}/USDT".format(token),
                    "route_volume_usd": None if index == 0 else str(1000 - index),
                }
            )
        # Non-Ethereum V2 and Ethereum V3 are never eligible for this adapter.
        ignored = copy.deepcopy(markets[0])
        ignored["market_id"] = "dex:bsc:uniswap_v2:{}:IGN".format("0x" + "f" * 40)
        ignored["token_symbol"] = "IGN"
        markets.append(ignored)
        ignored_v3 = copy.deepcopy(markets[0])
        ignored_v3["market_id"] = "dex:eth:uniswap_v3:{}:V3".format("0x" + "e" * 40)
        ignored_v3["token_symbol"] = "V3"
        markets.append(ignored_v3)
        universe = universe_for(markets=markets, routes=routes)

        expected_ids = [
            "dex:eth:uniswap_v2:0x{:040x}:T{:02d}".format(index + 10, index)
            for index in range(1, 9)
        ]
        actual = route_cost_evidence.build_selected_markets(
            universe, adapter_registry(supported=False)
        )
        self.assertEqual([row["market_id"] for row in actual], expected_ids)
        self.assertTrue(all(row["structural_support_status"] == "unsupported" for row in actual))
        self.assertTrue(all(row["structural_reason"] == "strict_cost_adapter_unsupported" for row in actual))

        shuffled = copy.deepcopy(universe)
        shuffled["selected_legs"].reverse()
        shuffled["routes"].reverse()
        self.assertEqual(
            route_cost_evidence.build_selected_markets(
                shuffled, adapter_registry(supported=False)
            ),
            actual,
        )

    def test_funding_descriptors_are_frozen_static_support_not_rpc_results(self):
        supported = route_cost_evidence.build_selected_markets(
            universe_for(), adapter_registry(supported=True)
        )
        self.assertEqual(supported[0]["structural_support_status"], "supported")
        self.assertIsNone(supported[0]["structural_reason"])
        missing_funding = adapter_registry(supported=True)
        missing_funding["adapters"][0]["token_funding_descriptors"] = []
        unsupported = route_cost_evidence.build_selected_markets(
            universe_for(), missing_funding
        )
        self.assertEqual(unsupported[0]["structural_support_status"], "unsupported")

    def test_pair_descriptor_and_both_funding_tokens_define_static_support(self):
        registry = adapter_registry(supported=True)
        observed = route_cost_evidence.build_selected_markets(
            universe_for(), registry
        )
        self.assertEqual(observed[0]["structural_support_status"], "supported")

        failed = copy.deepcopy(universe_for())
        failed_context = failed["selected_legs"][0]["collector_context"]
        failed_context.update({
            "status": "failed",
            "reason_code": "collection_failed",
            "base_token_id": None,
            "quote_token_id": None,
            "base_token_price_usd": None,
            "quote_token_price_usd": None,
        })
        failed["selected_legs"][0]["target_token_side"] = None
        self.assertEqual(
            route_cost_evidence.build_selected_markets(failed, registry),
            observed,
        )

        for mutate in (
            lambda item: item["pair_descriptors"].clear(),
            lambda item: item["token_funding_descriptors"].pop(),
            lambda item: item["pair_descriptors"][0].__setitem__(
                "token1_address", "0x" + "9" * 40
            ),
        ):
            malformed = adapter_registry(supported=True)
            mutate(malformed["adapters"][0])
            with self.subTest(registry=malformed):
                result = route_cost_evidence.build_selected_markets(
                    failed, malformed
                )
                self.assertEqual(
                    result[0]["structural_support_status"], "unsupported"
                )

    def test_context_failure_cannot_shrink_structural_supported_denominator(self):
        universe = universe_for()
        observed = route_cost_evidence.build_selected_markets(
            universe, adapter_registry(supported=True)
        )
        failed = copy.deepcopy(universe)
        failed_context = failed["selected_legs"][0]["collector_context"]
        failed_context.update({
            "status": "failed",
            "reason_code": "collection_failed",
            "base_token_id": None,
            "quote_token_id": None,
            "base_token_price_usd": None,
            "quote_token_price_usd": None,
        })
        failed["selected_legs"][0]["target_token_side"] = None
        self.assertEqual(
            route_cost_evidence.build_selected_markets(
                failed, adapter_registry(supported=True)
            ),
            observed,
        )


class ManifestValidationTests(unittest.TestCase):
    def test_pure_producer_helpers_have_closed_public_capability_surfaces(self):
        forbidden = {
            "path", "url", "credential", "authorization", "verifier",
            "clock", "expected_count", "network", "client",
        }
        names = (
            "build_fixed_block_phase_a_request_plan",
            "project_fixed_block_phase_a_capture",
            "build_terminal_transcript_inventory",
            "build_submission_policy_scope",
            "build_terminal_submission_policy_snapshot",
            "build_submission_policy_request",
            "validate_captured_submission_policy_response",
        )
        for name in names:
            with self.subTest(name=name):
                helper = getattr(route_cost_evidence, name)
                parameters = set(inspect.signature(helper).parameters)
                self.assertFalse(parameters & forbidden)

    def test_terminal_inventory_derives_mixed_supported_and_unsupported_rows(self):
        unsupported_pool = "0x" + "4" * 40
        unsupported_market = "dex:eth:uniswap_v2:{}:AAA".format(
            unsupported_pool
        )
        supported_leg = copy.deepcopy(universe_for()["selected_legs"][0])
        unsupported_leg = copy.deepcopy(supported_leg)
        unsupported_leg.update({
            "market_id": unsupported_market,
            "selection_rank": 2,
        })
        unsupported_leg["selection_inputs"] = copy.deepcopy(
            supported_leg["selection_inputs"]
        )
        unsupported_leg["selection_inputs"].update({
            "dex_24h_usd": "800",
            "dex_tvl_usd": "900",
        })
        universe = universe_for(markets=[unsupported_leg, supported_leg])
        registry = adapter_registry(supported=True)
        trace_identity, _trace_generation = (
            route_cost_evidence.trace_profile_identity(None)
        )
        connector_identity, _connector_generation = (
            route_cost_evidence.submission_connector_profile_identity(None)
        )
        retained = retained_v2_pool_state()

        rows = route_cost_evidence.build_terminal_transcript_inventory(
            universe=universe,
            run_id=RUN_ID,
            route_cohort_id=COHORT_ID,
            candidate_source_generation=GENERATION,
            route_universe_sha256=physical_sha(universe),
            adapter_registry=registry,
            trace_profile_identity=trace_identity,
            submission_connector_profile_identity=connector_identity,
            retained_typed_pool_state_members={MARKET_ID: retained},
            terminal_reason_by_market={MARKET_ID: "trace_profile_missing"},
        )

        self.assertEqual(len(rows), 20)
        self.assertEqual(
            [
                (row["market_id"], row["direction"], row["requested_notional_usd"])
                for row in rows
            ],
            sorted(
                [
                    (market, direction, str(notional))
                    for market in (MARKET_ID, unsupported_market)
                    for direction in ("buy", "sell")
                    for notional in NOTIONALS
                ],
                key=lambda item: (
                    item[0], 0 if item[1] == "buy" else 1,
                    Decimal(item[2]),
                ),
            ),
        )
        state = json.loads(retained["payload"])
        supported_rows = [row for row in rows if row["market_id"] == MARKET_ID]
        unsupported_rows = [
            row for row in rows if row["market_id"] == unsupported_market
        ]
        self.assertEqual(
            {(row["status"], row["completed_stage"], row["reason_code"])
             for row in supported_rows},
            {("unavailable", "none", "trace_profile_missing")},
        )
        self.assertEqual(
            {(row["core_pool_state_id"], row["core_pool_state_sha256"])
             for row in supported_rows},
            {(state["state_id"], retained["descriptor"]["sha256"])},
        )
        self.assertTrue(all(
            row["simulation_target_sha256"] is not None
            for row in supported_rows
        ))
        self.assertEqual(
            {(row["status"], row["completed_stage"], row["reason_code"])
             for row in unsupported_rows},
            {("unavailable", "none", "strict_cost_adapter_unsupported")},
        )
        for row in unsupported_rows:
            self.assertTrue(all(row[field] is None for field in (
                "simulation_target_token_address",
                "simulation_target_unit_decimals",
                "simulation_target_raw_quantity",
                "simulation_target_lattice_raw",
                "simulation_target_sha256",
                "core_pool_state_id", "core_pool_state_sha256",
                "chain_evidence_sha256", "market_evidence_sha256",
                "block_evidence", "call_evidence", "gas_evidence",
                "router_fee_evidence", "transfer_tax_evidence",
                "raw_transcript",
            )))

        with self.assertRaisesRegex(
            route_cost_evidence.RouteCostEvidenceError,
            "terminal transcript reason",
        ):
            route_cost_evidence.build_terminal_transcript_inventory(
                universe=universe,
                run_id=RUN_ID,
                route_cohort_id=COHORT_ID,
                candidate_source_generation=GENERATION,
                route_universe_sha256=physical_sha(universe),
                adapter_registry=registry,
                trace_profile_identity=trace_identity,
                submission_connector_profile_identity=connector_identity,
                retained_typed_pool_state_members={MARKET_ID: retained},
                terminal_reason_by_market={MARKET_ID: "caller_chosen_reason"},
            )

    def test_terminal_inventory_freezes_early_failure_status_core_and_target_matrix(self):
        universe = universe_for()
        registry = adapter_registry(supported=True)
        retained = retained_v2_pool_state()
        missing_trace, _missing_generation = (
            route_cost_evidence.trace_profile_identity(None)
        )
        available_trace, _available_generation = (
            route_cost_evidence.trace_profile_identity({
                "schema": "route_cost_trace_rpc_profile/v1",
                "profile_id": "test-trace",
                "endpoint_id": "rpc-mainnet-a",
                "rpc_url": "https://rpc.example.invalid",
                "authorization": "Bearer secret",
            })
        )
        connector_identity, _connector_generation = (
            route_cost_evidence.submission_connector_profile_identity(None)
        )
        cases = (
            ("core_pool_state_unavailable", "unavailable", False, available_trace),
            ("core_pool_state_invalid", "failed", False, available_trace),
            ("core_pool_state_unavailable", "unavailable", False, missing_trace),
            ("core_pool_state_invalid", "failed", False, missing_trace),
            ("trace_profile_missing", "unavailable", True, missing_trace),
            ("rpc_unavailable", "unavailable", True, available_trace),
            ("rpc_invalid", "failed", True, available_trace),
            ("fixed_block_unavailable", "unavailable", True, available_trace),
            ("fixed_block_mismatch", "failed", True, available_trace),
        )
        for reason, status, retains_core, trace_identity in cases:
            with self.subTest(reason=reason):
                members = {MARKET_ID: retained} if retains_core else {}
                rows = route_cost_evidence.build_terminal_transcript_inventory(
                    universe=universe,
                    run_id=RUN_ID,
                    route_cohort_id=COHORT_ID,
                    candidate_source_generation=GENERATION,
                    route_universe_sha256=physical_sha(universe),
                    adapter_registry=registry,
                    trace_profile_identity=trace_identity,
                    submission_connector_profile_identity=connector_identity,
                    retained_typed_pool_state_members=members,
                    terminal_reason_by_market={MARKET_ID: reason},
                )
                self.assertEqual(
                    {(row["status"], row["completed_stage"], row["reason_code"])
                     for row in rows},
                    {(status, "none", reason)},
                )
                self.assertEqual(
                    {row["core_pool_state_id"] is not None for row in rows},
                    {retains_core},
                )
                self.assertEqual(
                    {row["simulation_target_sha256"] is not None for row in rows},
                    {retains_core},
                )
                for row in rows:
                    self.assertTrue(all(row[field] is None for field in (
                        "chain_evidence_sha256", "market_evidence_sha256",
                        "block_evidence", "call_evidence", "gas_evidence",
                        "router_fee_evidence", "transfer_tax_evidence",
                        "raw_transcript",
                    )))

    def test_missing_trace_core_unavailable_manifest_passes_publication(self):
        universe = universe_for()
        registry = adapter_registry(supported=True)
        keys = connector_registry()
        trace_identity, _trace_generation = (
            route_cost_evidence.trace_profile_identity(None)
        )
        connector_identity, _connector_generation = (
            route_cost_evidence.submission_connector_profile_identity(None)
        )
        transcripts = route_cost_evidence.build_terminal_transcript_inventory(
            universe=universe,
            run_id=RUN_ID,
            route_cohort_id=COHORT_ID,
            candidate_source_generation=GENERATION,
            route_universe_sha256=physical_sha(universe),
            adapter_registry=registry,
            trace_profile_identity=trace_identity,
            submission_connector_profile_identity=connector_identity,
            retained_typed_pool_state_members={},
            terminal_reason_by_market={
                MARKET_ID: "core_pool_state_unavailable"
            },
        )
        snapshot = route_cost_evidence.build_terminal_submission_policy_snapshot(
            universe=universe,
            run_id=RUN_ID,
            route_cohort_id=COHORT_ID,
            candidate_source_generation=GENERATION,
            route_universe_sha256=physical_sha(universe),
            adapter_registry=registry,
            connector_key_registry=keys,
            trace_profile_identity=trace_identity,
            submission_connector_profile_identity=connector_identity,
            reason_code="submission_connector_missing",
        )
        value = route_cost_evidence.build_route_cost_evidence_manifest_from_captured(
            universe=universe,
            run_id=RUN_ID,
            route_cohort_id=COHORT_ID,
            phase=PHASE,
            candidate_source_generation=GENERATION,
            route_universe_sha256=physical_sha(universe),
            evaluated_at=EVALUATED_AT,
            adapter_registry=registry,
            connector_key_registry=keys,
            trace_profile_identity=trace_identity,
            submission_connector_profile_identity=connector_identity,
            native_price_evidence=None,
            chain_evidence=[],
            market_evidence=[],
            transcripts=transcripts,
            submission_policy_snapshot=snapshot,
        )
        self.assertEqual(
            route_cost_evidence.validate_route_cost_evidence_manifest_for_publication(
                value,
                universe=universe,
                expected_run_id=RUN_ID,
                expected_route_cohort_id=COHORT_ID,
                expected_phase=PHASE,
                expected_candidate_source_generation=GENERATION,
                expected_route_universe_sha256=physical_sha(universe),
                retained_typed_pool_state_members={},
            ),
            value,
        )

    def test_retained_pre_chain_terminal_reasons_pass_without_chain_evidence(self):
        universe = universe_for()
        registry = adapter_registry(supported=True)
        keys = connector_registry()
        retained = retained_v2_pool_state(block_number=20_000_000)
        trace_identity, _trace_generation = (
            route_cost_evidence.trace_profile_identity({
                "schema": "route_cost_trace_rpc_profile/v1",
                "profile_id": "test-trace",
                "endpoint_id": "rpc-mainnet-a",
                "rpc_url": "https://rpc.example.invalid",
                "authorization": "Bearer secret",
            })
        )
        connector_identity, _connector_generation = (
            route_cost_evidence.submission_connector_profile_identity(None)
        )
        for reason in (
            "rpc_unavailable", "rpc_invalid", "fixed_block_unavailable",
            "fixed_block_mismatch",
        ):
            with self.subTest(reason=reason):
                transcripts = route_cost_evidence.build_terminal_transcript_inventory(
                    universe=universe,
                    run_id=RUN_ID,
                    route_cohort_id=COHORT_ID,
                    candidate_source_generation=GENERATION,
                    route_universe_sha256=physical_sha(universe),
                    adapter_registry=registry,
                    trace_profile_identity=trace_identity,
                    submission_connector_profile_identity=connector_identity,
                    retained_typed_pool_state_members={MARKET_ID: retained},
                    terminal_reason_by_market={MARKET_ID: reason},
                )
                snapshot = route_cost_evidence.build_terminal_submission_policy_snapshot(
                    universe=universe,
                    run_id=RUN_ID,
                    route_cohort_id=COHORT_ID,
                    candidate_source_generation=GENERATION,
                    route_universe_sha256=physical_sha(universe),
                    adapter_registry=registry,
                    connector_key_registry=keys,
                    trace_profile_identity=trace_identity,
                    submission_connector_profile_identity=connector_identity,
                    reason_code="submission_connector_missing",
                )
                value = route_cost_evidence.build_route_cost_evidence_manifest_from_captured(
                    universe=universe,
                    run_id=RUN_ID,
                    route_cohort_id=COHORT_ID,
                    phase=PHASE,
                    candidate_source_generation=GENERATION,
                    route_universe_sha256=physical_sha(universe),
                    evaluated_at=EVALUATED_AT,
                    adapter_registry=registry,
                    connector_key_registry=keys,
                    trace_profile_identity=trace_identity,
                    submission_connector_profile_identity=connector_identity,
                    native_price_evidence=None,
                    chain_evidence=[],
                    market_evidence=[],
                    transcripts=transcripts,
                    submission_policy_snapshot=snapshot,
                )
                self.assertEqual(
                    route_cost_evidence.validate_route_cost_evidence_manifest_for_publication(
                        value,
                        universe=universe,
                        expected_run_id=RUN_ID,
                        expected_route_cohort_id=COHORT_ID,
                        expected_phase=PHASE,
                        expected_candidate_source_generation=GENERATION,
                        expected_route_universe_sha256=physical_sha(universe),
                        retained_typed_pool_state_members={MARKET_ID: retained},
                    ),
                    value,
                )

    def test_policy_scope_terminal_snapshots_and_request_are_derived(self):
        universe = universe_for()
        registry = adapter_registry(supported=True)
        keys = connector_registry()
        trace_identity, trace_generation = (
            route_cost_evidence.trace_profile_identity(None)
        )
        missing_connector, missing_connector_generation = (
            route_cost_evidence.submission_connector_profile_identity(None)
        )
        connector_identity, connector_generation = (
            route_cost_evidence.submission_connector_profile_identity({
                "schema": "route_cost_submission_connector_profile/v1",
                "profile_id": "test-connector",
                "connector_id": "connector_a",
                "endpoint_url": "https://connector.example.invalid",
                "authorization": "Bearer secret",
            })
        )
        route_id = universe["routes"][0]["route_id"]
        expected_scope = [
            {"route_id": route_id, "requested_notional_usd": str(notional)}
            for notional in NOTIONALS
        ]
        self.assertEqual(
            route_cost_evidence.build_submission_policy_scope(
                universe=universe, adapter_registry=registry
            ),
            expected_scope,
        )

        common = {
            "universe": universe,
            "run_id": RUN_ID,
            "route_cohort_id": COHORT_ID,
            "candidate_source_generation": GENERATION,
            "route_universe_sha256": physical_sha(universe),
            "adapter_registry": registry,
            "connector_key_registry": keys,
            "trace_profile_identity": trace_identity,
        }
        missing = route_cost_evidence.build_terminal_submission_policy_snapshot(
            **common,
            submission_connector_profile_identity=missing_connector,
            reason_code="submission_connector_missing",
        )
        self.assertEqual(missing["status"], "unavailable")
        self.assertIsNone(missing["connector_id"])
        self.assertEqual(missing["trace_profile_generation"], trace_generation)
        self.assertEqual(
            missing["submission_connector_profile_generation"],
            missing_connector_generation,
        )
        self.assertEqual(
            [(row["route_id"], row["requested_notional_usd"])
             for row in missing["members"]],
            [(row["route_id"], row["requested_notional_usd"])
             for row in expected_scope],
        )
        self.assertEqual(
            {(row["status"], row["reason_code"]) for row in missing["members"]},
            {("unavailable", "submission_connector_missing")},
        )

        unavailable = route_cost_evidence.build_terminal_submission_policy_snapshot(
            **common,
            submission_connector_profile_identity=connector_identity,
            reason_code="submission_connector_unavailable",
        )
        self.assertEqual(unavailable["connector_id"], "connector_a")
        self.assertEqual(unavailable["status"], "unavailable")
        self.assertEqual(
            unavailable["submission_connector_profile_generation"],
            connector_generation,
        )
        invalid = route_cost_evidence.build_terminal_submission_policy_snapshot(
            **common,
            submission_connector_profile_identity=connector_identity,
            reason_code="submission_connector_invalid",
        )
        self.assertEqual(invalid["status"], "failed")
        self.assertEqual(
            {(row["status"], row["reason_code"]) for row in invalid["members"]},
            {("failed", "submission_connector_invalid")},
        )

        empty = route_cost_evidence.build_terminal_submission_policy_snapshot(
            **dict(common, adapter_registry=adapter_registry(supported=False)),
            submission_connector_profile_identity=connector_identity,
            reason_code="scope_empty",
        )
        self.assertEqual(
            (empty["status"], empty["reason_code"], empty["connector_id"],
             empty["members"]),
            ("not_applicable", "scope_empty", "connector_a", []),
        )

        request = route_cost_evidence.build_submission_policy_request(
            **common,
            submission_connector_profile_identity=connector_identity,
        )
        self.assertEqual(
            set(request),
            {
                "schema", "request_id", "run_id", "route_cohort_id",
                "candidate_source_generation", "route_universe_sha256",
                "selected_market_set_sha256", "adapter_registry_sha256",
                "connector_key_registry_sha256", "trace_profile_generation",
                "submission_connector_profile_generation", "connector_id",
                "members",
            },
        )
        unhashed = copy.deepcopy(request)
        del unhashed["request_id"]
        self.assertEqual(
            request["request_id"],
            typed_sha(b"route-cost-submission-policy-request/v1\n", unhashed),
        )
        self.assertEqual(request["members"], expected_scope)

        for bad_identity, bad_reason in (
            (missing_connector, "submission_connector_unavailable"),
            (connector_identity, "submission_connector_missing"),
        ):
            with self.subTest(reason=bad_reason):
                with self.assertRaises(route_cost_evidence.RouteCostEvidenceError):
                    route_cost_evidence.build_terminal_submission_policy_snapshot(
                        **common,
                        submission_connector_profile_identity=bad_identity,
                        reason_code=bad_reason,
                    )

    def test_captured_policy_response_validator_is_structural_and_request_bound(self):
        value, universe, _retained = supported_observed_manifest()
        request = route_cost_evidence.build_submission_policy_request(
            universe=universe,
            run_id=RUN_ID,
            route_cohort_id=COHORT_ID,
            candidate_source_generation=GENERATION,
            route_universe_sha256=physical_sha(universe),
            adapter_registry=value["adapter_registry"],
            connector_key_registry=value["connector_key_registry"],
            trace_profile_identity=value["trace_profile_identity"],
            submission_connector_profile_identity=value[
                "submission_connector_profile_identity"
            ],
        )
        with mock.patch.object(
            route_cost_evidence.subprocess,
            "run",
            side_effect=AssertionError("structural validation performed I/O"),
        ):
            self.assertEqual(
                route_cost_evidence.validate_captured_submission_policy_response(
                    value["submission_policy_snapshot"],
                    request=request,
                    connector_key_registry=value["connector_key_registry"],
                ),
                value["submission_policy_snapshot"],
            )

        omitted = copy.deepcopy(value["submission_policy_snapshot"])
        omitted["members"].pop()
        omitted["member_count"] -= 1
        omitted["member_set_sha256"] = typed_sha(
            b"route-cost-submission-policy-member-set/v1\n",
            omitted["members"],
        )
        omitted["attested_payload_sha256"] = typed_sha(
            b"route-cost-submission-policy-attestation/v1\n",
            route_cost_evidence._policy_attestation(omitted),
        )
        with self.assertRaisesRegex(
            route_cost_evidence.RouteCostEvidenceError,
            "policy response scope differs",
        ):
            route_cost_evidence.validate_captured_submission_policy_response(
                omitted,
                request=request,
                connector_key_registry=value["connector_key_registry"],
            )

    def test_captured_assembler_rebuilds_supported_outer_sets_and_bindings(self):
        expected, universe, retained = supported_observed_manifest()
        assembled = route_cost_evidence.build_route_cost_evidence_manifest_from_captured(
            universe=universe,
            run_id=RUN_ID,
            route_cohort_id=COHORT_ID,
            phase=PHASE,
            candidate_source_generation=GENERATION,
            route_universe_sha256=physical_sha(universe),
            evaluated_at=EVALUATED_AT,
            adapter_registry=expected["adapter_registry"],
            connector_key_registry=expected["connector_key_registry"],
            trace_profile_identity=expected["trace_profile_identity"],
            submission_connector_profile_identity=expected[
                "submission_connector_profile_identity"
            ],
            native_price_evidence=expected["native_price_evidence"],
            chain_evidence=expected["chain_evidence"],
            market_evidence=expected["market_evidence"],
            transcripts=expected["transcripts"],
            submission_policy_snapshot=expected[
                "submission_policy_snapshot"
            ],
        )
        self.assertEqual(assembled, expected)
        self.assertEqual(
            route_cost_evidence.validate_route_cost_evidence_manifest_for_publication(
                assembled,
                universe=universe,
                expected_run_id=RUN_ID,
                expected_route_cohort_id=COHORT_ID,
                expected_phase=PHASE,
                expected_candidate_source_generation=GENERATION,
                expected_route_universe_sha256=physical_sha(universe),
                retained_typed_pool_state_members=retained,
            ),
            expected,
        )

    def test_captured_assembler_derives_terminal_binding_reason(self):
        captured, _retained = supported_core_manifest()
        assembled = route_cost_evidence.build_route_cost_evidence_manifest_from_captured(
            universe=universe_for(),
            run_id=RUN_ID,
            route_cohort_id=COHORT_ID,
            phase=PHASE,
            candidate_source_generation=GENERATION,
            route_universe_sha256=physical_sha(universe_for()),
            evaluated_at=EVALUATED_AT,
            adapter_registry=captured["adapter_registry"],
            connector_key_registry=captured["connector_key_registry"],
            trace_profile_identity=captured["trace_profile_identity"],
            submission_connector_profile_identity=captured[
                "submission_connector_profile_identity"
            ],
            native_price_evidence=None,
            chain_evidence=captured["chain_evidence"],
            market_evidence=[],
            transcripts=captured["transcripts"],
            submission_policy_snapshot=captured[
                "submission_policy_snapshot"
            ],
        )
        self.assertEqual(
            {row["reason_code"] for row in assembled["bindings"]},
            {"transcript_unavailable"},
        )
        self.assertEqual(assembled["counts"]["binding_unavailable"], 5)

    def test_trace_profile_missing_builder_retains_core_without_rpc(self):
        universe = universe_for()
        retained = retained_v2_pool_state()
        value = route_cost_evidence.build_trace_profile_missing_route_cost_evidence_manifest(
            universe=universe,
            run_id=RUN_ID,
            route_cohort_id=COHORT_ID,
            phase=PHASE,
            candidate_source_generation=GENERATION,
            route_universe_sha256=physical_sha(universe),
            evaluated_at=EVALUATED_AT,
            adapter_registry=adapter_registry(supported=True),
            connector_key_registry=connector_registry(),
            retained_typed_pool_state_members={MARKET_ID: retained},
        )
        self.assertEqual(value["chain_evidence"], [])
        self.assertEqual(value["market_evidence"], [])
        self.assertEqual(value["transcript_count"], 10)
        self.assertEqual(
            {
                (row["status"], row["completed_stage"], row["reason_code"])
                for row in value["transcripts"]
            },
            {("unavailable", "none", "trace_profile_missing")},
        )
        state = json.loads(retained["payload"])
        for row in value["transcripts"]:
            self.assertEqual(row["core_pool_state_id"], state["state_id"])
            self.assertEqual(
                row["core_pool_state_sha256"], retained["descriptor"]["sha256"]
            )
            self.assertIsNotNone(row["simulation_target_sha256"])
            self.assertIsNone(row["chain_evidence_sha256"])
            self.assertIsNone(row["market_evidence_sha256"])
            for field in (
                "block_evidence", "call_evidence", "gas_evidence",
                "router_fee_evidence", "transfer_tax_evidence",
                "raw_transcript",
            ):
                self.assertIsNone(row[field])
        self.assertEqual(value["counts"]["binding_unavailable"], 5)
        self.assertEqual(
            {row["reason_code"] for row in value["bindings"]},
            {"transcript_unavailable"},
        )
        self.assertEqual(
            route_cost_evidence.validate_route_cost_evidence_manifest_for_publication(
                value,
                universe=universe,
                expected_run_id=RUN_ID,
                expected_route_cohort_id=COHORT_ID,
                expected_phase=PHASE,
                expected_candidate_source_generation=GENERATION,
                expected_route_universe_sha256=physical_sha(universe),
                retained_typed_pool_state_members={MARKET_ID: retained},
            ),
            value,
        )

        wrong_trace_reason = copy.deepcopy(value)
        for row in wrong_trace_reason["transcripts"]:
            row["reason_code"] = "router_identity_unavailable"
        wrong_trace_reason["transcript_set_sha256"] = typed_sha(
            b"route-cost-evidence-transcript-set/v1\n",
            wrong_trace_reason["transcripts"],
        )
        by_scope = {
            (row["direction"], row["requested_notional_usd"]): row
            for row in wrong_trace_reason["transcripts"]
        }
        for binding in wrong_trace_reason["bindings"]:
            binding["buy_transcript_sha256"] = typed_sha(
                b"route-cost-evidence-transcript/v1\n",
                by_scope[("buy", binding["requested_notional_usd"])],
            )
        wrong_trace_reason["binding_set_sha256"] = typed_sha(
            b"route-cost-evidence-binding-set/v1\n",
            wrong_trace_reason["bindings"],
        )
        with self.assertRaisesRegex(
            route_cost_evidence.RouteCostEvidenceError,
            "missing trace profile",
        ):
            route_cost_evidence.validate_route_cost_evidence_manifest_for_publication(
                wrong_trace_reason,
                universe=universe,
                expected_run_id=RUN_ID,
                expected_route_cohort_id=COHORT_ID,
                expected_phase=PHASE,
                expected_candidate_source_generation=GENERATION,
                expected_route_universe_sha256=physical_sha(universe),
                retained_typed_pool_state_members={MARKET_ID: retained},
            )

        wrong_connector_reason = copy.deepcopy(value)
        snapshot = wrong_connector_reason["submission_policy_snapshot"]
        snapshot["reason_code"] = "submission_connector_unavailable"
        for member in snapshot["members"]:
            member["reason_code"] = "submission_connector_unavailable"
        snapshot["member_set_sha256"] = typed_sha(
            b"route-cost-submission-policy-member-set/v1\n",
            snapshot["members"],
        )
        wrong_connector_reason["submission_policy_snapshot_sha256"] = typed_sha(
            b"route-cost-submission-policy-snapshot/v1\n", snapshot
        )
        member_by_scope = {
            (member["route_id"], member["requested_notional_usd"]): member
            for member in snapshot["members"]
        }
        for binding in wrong_connector_reason["bindings"]:
            binding["submission_policy_member_sha256"] = typed_sha(
                b"route-cost-submission-policy-member/v1\n",
                member_by_scope[
                    (binding["route_id"], binding["requested_notional_usd"])
                ],
            )
        wrong_connector_reason["binding_set_sha256"] = typed_sha(
            b"route-cost-evidence-binding-set/v1\n",
            wrong_connector_reason["bindings"],
        )
        with self.assertRaisesRegex(
            route_cost_evidence.RouteCostEvidenceError,
            "missing connector profile",
        ):
            route_cost_evidence.validate_route_cost_evidence_manifest_for_publication(
                wrong_connector_reason,
                universe=universe,
                expected_run_id=RUN_ID,
                expected_route_cohort_id=COHORT_ID,
                expected_phase=PHASE,
                expected_candidate_source_generation=GENERATION,
                expected_route_universe_sha256=physical_sha(universe),
                retained_typed_pool_state_members={MARKET_ID: retained},
            )

    def validate(self, value, universe=None):
        universe = universe or universe_for()
        return route_cost_evidence.validate_route_cost_evidence_manifest(
            value,
            universe=universe,
            expected_run_id=RUN_ID,
            expected_route_cohort_id=COHORT_ID,
            expected_phase=PHASE,
            expected_candidate_source_generation=GENERATION,
            expected_route_universe_sha256=physical_sha(universe),
        )

    def test_unsupported_scope_does_not_require_call_direction_tokens(self):
        universe = universe_for()
        universe["selected_legs"][0].pop("collector_context")
        self.assertEqual(
            self.validate(unsupported_manifest(universe), universe),
            unsupported_manifest(universe),
        )

    def test_full_v1_unsupported_manifest_round_trips_as_canonical_deep_copy(self):
        value = unsupported_manifest(tracked=True)
        validated = self.validate(value)
        self.assertEqual(validated, value)
        self.assertIsNot(validated, value)
        self.assertIsNot(validated["transcripts"], value["transcripts"])

    def test_full_supported_observed_manifest_round_trips_with_real_sshsig(self):
        value, universe, retained = supported_observed_manifest()
        from scripts.route_publication import _validate_route_universe_payload

        self.assertEqual(
            _validate_route_universe_payload(universe),
            universe,
        )
        validated = route_cost_evidence.validate_route_cost_evidence_manifest_for_publication(
            value,
            universe=universe,
            expected_run_id=RUN_ID,
            expected_route_cohort_id=COHORT_ID,
            expected_phase=PHASE,
            expected_candidate_source_generation=GENERATION,
            expected_route_universe_sha256=physical_sha(universe),
            retained_typed_pool_state_members=retained,
        )
        self.assertEqual(validated, value)

    def test_market_capture_window_must_equal_its_referenced_chain_window(self):
        value, universe, retained = supported_observed_manifest()
        candidate = copy.deepcopy(value)
        market = candidate["market_evidence"][0]
        old_market_sha = physical_sha(market)
        market["captured_finished_at"] = "2026-08-01T12:00:00.5Z"
        new_market_sha = physical_sha(market)
        for transcript in candidate["transcripts"]:
            if transcript["market_evidence_sha256"] != old_market_sha:
                continue
            transcript["market_evidence_sha256"] = new_market_sha
            if transcript["block_evidence"] is not None:
                transcript["block_evidence"][
                    "market_evidence_sha256"
                ] = new_market_sha
            if transcript["raw_transcript"] is not None:
                transcript["raw_transcript"][
                    "market_evidence_sha256"
                ] = new_market_sha
        candidate["market_evidence_set_sha256"] = typed_sha(
            b"route-cost-market-evidence-set/v1\n",
            candidate["market_evidence"],
        )
        candidate["transcript_set_sha256"] = typed_sha(
            b"route-cost-evidence-transcript-set/v1\n",
            candidate["transcripts"],
        )
        by_scope = {
            (
                row["market_id"], row["direction"],
                row["requested_notional_usd"],
            ): row
            for row in candidate["transcripts"]
        }
        route = universe["routes"][0]
        for binding in candidate["bindings"]:
            if route["buy_market_id"] == MARKET_ID:
                binding["buy_transcript_sha256"] = typed_sha(
                    b"route-cost-evidence-transcript/v1\n",
                    by_scope[(MARKET_ID, "buy", binding[
                        "requested_notional_usd"
                    ])],
                )
            if route["sell_market_id"] == MARKET_ID:
                binding["sell_transcript_sha256"] = typed_sha(
                    b"route-cost-evidence-transcript/v1\n",
                    by_scope[(MARKET_ID, "sell", binding[
                        "requested_notional_usd"
                    ])],
                )
        candidate["binding_set_sha256"] = typed_sha(
            b"route-cost-evidence-binding-set/v1\n", candidate["bindings"]
        )
        with self.assertRaisesRegex(
            route_cost_evidence.RouteCostEvidenceError,
            "capture window",
        ):
            route_cost_evidence.validate_route_cost_evidence_manifest_for_publication(
                candidate,
                universe=universe,
                expected_run_id=RUN_ID,
                expected_route_cohort_id=COHORT_ID,
                expected_phase=PHASE,
                expected_candidate_source_generation=GENERATION,
                expected_route_universe_sha256=physical_sha(universe),
                retained_typed_pool_state_members=retained,
            )

    def test_router_fee_source_must_equal_transcript_market_evidence(self):
        value, universe, retained = supported_observed_manifest()
        candidate = copy.deepcopy(value)
        transcript = candidate["transcripts"][0]
        transcript["router_fee_evidence"]["source_record_sha256"] = "f" * 64
        candidate["transcript_set_sha256"] = typed_sha(
            b"route-cost-evidence-transcript-set/v1\n",
            candidate["transcripts"],
        )
        route = next(
            row for row in universe["routes"]
            if transcript["market_id"] in {
                row["buy_market_id"], row["sell_market_id"]
            }
        )
        binding = next(
            row for row in candidate["bindings"]
            if row["route_id"] == route["route_id"]
            and row["requested_notional_usd"]
            == transcript["requested_notional_usd"]
        )
        side = (
            "buy_transcript_sha256"
            if route["buy_market_id"] == transcript["market_id"]
            else "sell_transcript_sha256"
        )
        binding[side] = typed_sha(
            b"route-cost-evidence-transcript/v1\n", transcript
        )
        candidate["binding_set_sha256"] = typed_sha(
            b"route-cost-evidence-binding-set/v1\n", candidate["bindings"]
        )
        with self.assertRaisesRegex(
            route_cost_evidence.RouteCostEvidenceError,
            "router fee source",
        ):
            route_cost_evidence.validate_route_cost_evidence_manifest_for_publication(
                candidate,
                universe=universe,
                expected_run_id=RUN_ID,
                expected_route_cohort_id=COHORT_ID,
                expected_phase=PHASE,
                expected_candidate_source_generation=GENERATION,
                expected_route_universe_sha256=physical_sha(universe),
                retained_typed_pool_state_members=retained,
            )

    def test_supported_observed_manifest_rejects_deep_tampering(self):
        with tempfile.TemporaryDirectory() as signing_directory:
            signing_key, signing_public_key = _generate_ephemeral_signing_key(
                signing_directory
            )
            value, universe, retained = supported_observed_manifest(
                signing_key_path=signing_key,
                signing_public_key=signing_public_key,
            )

        def validate(candidate):
            return route_cost_evidence.validate_route_cost_evidence_manifest_for_publication(
                candidate,
                universe=universe,
                expected_run_id=RUN_ID,
                expected_route_cohort_id=COHORT_ID,
                expected_phase=PHASE,
                expected_candidate_source_generation=GENERATION,
                expected_route_universe_sha256=physical_sha(universe),
                retained_typed_pool_state_members=retained,
            )

        cases = []
        raw = copy.deepcopy(value)
        raw["transcripts"][0]["raw_transcript"]["estimate_gas_request"][
            "params"
        ][1] = "0x65"
        cases.append(raw)

        token_code = copy.deepcopy(value)
        token_code["market_evidence"][0]["token_runtime_code_evidence"][0][
            "response"
        ]["result"] = "0x02"
        cases.append(token_code)

        amount = copy.deepcopy(value)
        amount["transcripts"][0]["call_evidence"]["amount_in_raw"] = "1"
        cases.append(amount)

        native = copy.deepcopy(value)
        native["native_price_evidence"]["book_projection"][
            "best_ask_price"
        ] = "2999"
        cases.append(native)

        tax = copy.deepcopy(value)
        tax["transcripts"][0]["transfer_tax_evidence"][
            "post_output_balance"
        ] = "0"
        cases.append(tax)

        signature = copy.deepcopy(value)
        signature["submission_policy_snapshot"]["signature"] = (
            SSHSIG_KAT_SIGNATURE
        )
        cases.append(signature)

        extra = copy.deepcopy(value)
        extra["transcripts"][0]["raw_transcript"]["extra"] = True
        cases.append(extra)

        missing = copy.deepcopy(value)
        del missing["transcripts"][0]["raw_transcript"]["simulation_response"]
        cases.append(missing)

        null = copy.deepcopy(value)
        null["transcripts"][0]["gas_evidence"] = None
        cases.append(null)

        stage = copy.deepcopy(value)
        stage["transcripts"][0]["completed_stage"] = "call"
        cases.append(stage)

        reason = copy.deepcopy(value)
        reason["transcripts"][0]["reason_code"] = "gas_unavailable"
        cases.append(reason)

        for index, candidate in enumerate(cases):
            with self.subTest(index=index):
                with self.assertRaises(route_cost_evidence.RouteCostEvidenceError):
                    validate(candidate)

    def test_transfer_tax_trace_quantities_bind_to_quoted_buy_and_sell(self):
        value, universe, retained = supported_observed_manifest()

        def rehash(candidate, transcript):
            raw = transcript["raw_transcript"]
            transcript["block_evidence"]["rpc_transcript_sha256"] = typed_sha(
                b"route-cost-rpc-transcript/v1\n",
                {
                    "estimate_request": raw["estimate_gas_request"],
                    "estimate_response": raw["estimate_gas_response"],
                    "trace_request": raw["simulation_request"],
                    "trace_response": raw["simulation_response"],
                },
            )
            transcript["transfer_tax_evidence"]["trace_sha256"] = typed_sha(
                b"route-cost-trace/v1\n",
                {
                    "request": raw["simulation_request"],
                    "response": raw["simulation_response"],
                },
            )
            candidate["transcript_set_sha256"] = typed_sha(
                b"route-cost-evidence-transcript-set/v1\n",
                candidate["transcripts"],
            )
            route = universe["routes"][0]
            binding = next(
                row for row in candidate["bindings"]
                if row["requested_notional_usd"]
                == transcript["requested_notional_usd"]
            )
            side = (
                "buy_transcript_sha256"
                if transcript["market_id"] == route["buy_market_id"]
                else "sell_transcript_sha256"
            )
            binding[side] = typed_sha(
                b"route-cost-evidence-transcript/v1\n", transcript
            )
            candidate["binding_set_sha256"] = typed_sha(
                b"route-cost-evidence-binding-set/v1\n",
                candidate["bindings"],
            )

        def validate(candidate):
            return route_cost_evidence.validate_route_cost_evidence_manifest_for_publication(
                candidate,
                universe=universe,
                expected_run_id=RUN_ID,
                expected_route_cohort_id=COHORT_ID,
                expected_phase=PHASE,
                expected_candidate_source_generation=GENERATION,
                expected_route_universe_sha256=physical_sha(universe),
                retained_typed_pool_state_members=retained,
            )

        route = universe["routes"][0]
        current_adapter = adapter()
        for direction, market_id, changed_role in (
            ("buy", route["buy_market_id"], "input"),
            ("sell", route["sell_market_id"], "output"),
        ):
            candidate = copy.deepcopy(value)
            transcript = next(
                row for row in candidate["transcripts"]
                if row["market_id"] == market_id
                and row["direction"] == direction
            )
            call = transcript["call_evidence"]
            raw = transcript["raw_transcript"]
            token_in = call["path_token_in"]
            token_out = call["path_token_out"]
            descriptor = next(
                row for row in current_adapter["token_funding_descriptors"]
                if row["token_address"] == token_in
            )
            allowance_key = route_cost_evidence.solidity_allowance_storage_key(
                SENDER, ROUTER, int(descriptor["allowance_mapping_slot"])
            )
            if changed_role == "input":
                forged = int(call["amount_in_raw"]) - 1
                for row in raw["simulation_response"]["storage_diffs"]:
                    if row["token_address"] != token_in:
                        continue
                    if row["account_role"] == "sender":
                        row["post_value"] = _word(1)
                    elif row["account_role"] == "pair":
                        row["post_value"] = _word(
                            int(row["pre_value"], 16) + forged
                        )
                transcript["transfer_tax_evidence"].update({
                    "pre_input_balance": call["amount_in_raw"],
                    "post_input_balance": "1",
                })
            else:
                forged = int(call["amount_out_raw"]) - 1
                for row in raw["simulation_response"]["storage_diffs"]:
                    if row["token_address"] != token_out:
                        continue
                    if row["account_role"] == "pair":
                        row["post_value"] = _word(
                            int(row["pre_value"], 16) - forged
                        )
                    elif row["account_role"] == "recipient":
                        row["post_value"] = _word(forged)
                transcript["transfer_tax_evidence"].update({
                    "pre_output_balance": "0",
                    "post_output_balance": str(forged),
                })
            raw["simulation_balance_deltas"] = [
                {
                    "token_address": row["token_address"],
                    "account_role": row["account_role"],
                    "pre_balance_raw": str(int(row["pre_value"], 16)),
                    "post_balance_raw": str(int(row["post_value"], 16)),
                }
                for row in raw["simulation_response"]["storage_diffs"]
                if not (
                    row["account_role"] == "sender"
                    and row["storage_key"] == allowance_key
                )
            ]
            rehash(candidate, transcript)
            with self.subTest(direction=direction), self.assertRaisesRegex(
                route_cost_evidence.RouteCostEvidenceError,
                "quoted|zero-tax",
            ):
                validate(candidate)

    def test_binding_status_reason_and_snapshot_freshness_are_derived(self):
        with tempfile.TemporaryDirectory() as signing_directory:
            signing_key, signing_public_key = _generate_ephemeral_signing_key(
                signing_directory
            )
            value, universe, retained = supported_observed_manifest(
                signing_key_path=signing_key,
                signing_public_key=signing_public_key,
            )

        def validate(candidate):
            return route_cost_evidence.validate_route_cost_evidence_manifest_for_publication(
                candidate,
                universe=universe,
                expected_run_id=RUN_ID,
                expected_route_cohort_id=COHORT_ID,
                expected_phase=PHASE,
                expected_candidate_source_generation=GENERATION,
                expected_route_universe_sha256=physical_sha(universe),
                retained_typed_pool_state_members=retained,
            )

        downgraded = copy.deepcopy(value)
        downgraded["bindings"][0].update({
            "status": "unavailable",
            "reason_code": "transcript_unavailable",
        })
        downgraded["binding_set_sha256"] = typed_sha(
            b"route-cost-evidence-binding-set/v1\n",
            downgraded["bindings"],
        )
        downgraded["counts"].update({
            "binding_observed": 4,
            "binding_unavailable": 1,
        })
        with self.assertRaisesRegex(
            route_cost_evidence.RouteCostEvidenceError,
            "binding status/reason differs",
        ):
            validate(downgraded)

        for instant, accepted in (
            ("2026-08-01T11:59:59Z", False),
            ("2026-08-01T12:00:00Z", True),
            ("2026-08-01T12:05:00Z", True),
            ("2026-08-01T12:05:01Z", False),
        ):
            derived = route_cost_evidence._derived_binding_status_reason(
                route_sides=(True, True),
                transcripts=({"status": "observed"},),
                member=value["submission_policy_snapshot"]["members"][0],
                snapshot=value["submission_policy_snapshot"],
                evaluated_at=instant,
            )
            with self.subTest(instant=instant):
                if accepted:
                    self.assertEqual(derived, ("observed", None))
                else:
                    self.assertEqual(
                        derived,
                        ("unavailable", "submission_policy_stale"),
                    )

    def test_phase_b_capture_timestamps_are_bound_to_the_run_window(self):
        value, universe, retained = supported_observed_manifest()

        def rehash(candidate):
            by_scope = {
                (
                    row["market_id"], row["direction"],
                    row["requested_notional_usd"],
                ): typed_sha(b"route-cost-evidence-transcript/v1\n", row)
                for row in candidate["transcripts"]
            }
            candidate["transcript_set_sha256"] = typed_sha(
                b"route-cost-evidence-transcript-set/v1\n",
                candidate["transcripts"],
            )
            for binding in candidate["bindings"]:
                route = next(
                    row for row in universe["routes"]
                    if row["route_id"] == binding["route_id"]
                )
                notional = binding["requested_notional_usd"]
                binding["buy_transcript_sha256"] = by_scope[
                    (route["buy_market_id"], "buy", notional)
                ]
                binding["sell_transcript_sha256"] = by_scope[
                    (route["sell_market_id"], "sell", notional)
                ]
                binding["evaluated_at"] = candidate["evaluated_at"]
            candidate["binding_set_sha256"] = typed_sha(
                b"route-cost-evidence-binding-set/v1\n",
                candidate["bindings"],
            )

        def validate(candidate):
            return route_cost_evidence.validate_route_cost_evidence_manifest_for_publication(
                candidate,
                universe=universe,
                expected_run_id=RUN_ID,
                expected_route_cohort_id=COHORT_ID,
                expected_phase=PHASE,
                expected_candidate_source_generation=GENERATION,
                expected_route_universe_sha256=physical_sha(universe),
                retained_typed_pool_state_members=retained,
            )

        exact = copy.deepcopy(value)
        exact["evaluated_at"] = "2026-08-01T12:00:37Z"
        for row in exact["transcripts"]:
            row["raw_transcript"]["captured_started_at"] = (
                "2026-08-01T12:00:01Z"
            )
            row["raw_transcript"]["captured_finished_at"] = (
                "2026-08-01T12:00:36Z"
            )
        rehash(exact)
        self.assertEqual(validate(exact), exact)

        cases = (
            ("before-phase-a", "2026-08-01T12:00:00Z", "2026-08-01T12:00:01Z", "2026-08-01T12:00:03Z"),
            ("after-evaluation", "2026-08-01T12:00:02Z", "2026-08-01T12:00:04Z", "2026-08-01T12:00:03Z"),
            ("phase-b-over-35", "2026-08-01T12:00:01Z", "2026-08-01T12:00:36.000001Z", "2026-08-01T12:00:37Z"),
            ("run-over-60", "2026-08-01T12:00:30Z", "2026-08-01T12:00:31Z", "2026-08-01T12:01:00.000001Z"),
        )
        for label, started, finished, evaluated in cases:
            candidate = copy.deepcopy(value)
            candidate["evaluated_at"] = evaluated
            for row in candidate["transcripts"]:
                row["raw_transcript"]["captured_started_at"] = started
                row["raw_transcript"]["captured_finished_at"] = finished
            rehash(candidate)
            with self.subTest(case=label), self.assertRaisesRegex(
                route_cost_evidence.RouteCostEvidenceError,
                "outside the run window",
            ):
                validate(candidate)

    def test_native_validity_covers_raw_capture_and_final_evaluation(self):
        value, universe, retained = supported_observed_manifest()

        def rehash(candidate):
            by_scope = {
                (
                    row["market_id"], row["direction"],
                    row["requested_notional_usd"],
                ): typed_sha(b"route-cost-evidence-transcript/v1\n", row)
                for row in candidate["transcripts"]
            }
            candidate["transcript_set_sha256"] = typed_sha(
                b"route-cost-evidence-transcript-set/v1\n",
                candidate["transcripts"],
            )
            for binding in candidate["bindings"]:
                route = next(
                    row for row in universe["routes"]
                    if row["route_id"] == binding["route_id"]
                )
                notional = binding["requested_notional_usd"]
                binding["buy_transcript_sha256"] = by_scope[
                    (route["buy_market_id"], "buy", notional)
                ]
                binding["sell_transcript_sha256"] = by_scope[
                    (route["sell_market_id"], "sell", notional)
                ]
                binding["evaluated_at"] = candidate["evaluated_at"]
            candidate["binding_set_sha256"] = typed_sha(
                b"route-cost-evidence-binding-set/v1\n",
                candidate["bindings"],
            )

        def validate(candidate):
            return route_cost_evidence.validate_route_cost_evidence_manifest_for_publication(
                candidate,
                universe=universe,
                expected_run_id=RUN_ID,
                expected_route_cohort_id=COHORT_ID,
                expected_phase=PHASE,
                expected_candidate_source_generation=GENERATION,
                expected_route_universe_sha256=physical_sha(universe),
                retained_typed_pool_state_members=retained,
            )

        valid_until = value["native_price_evidence"]["valid_until"]
        equality = copy.deepcopy(value)
        equality["evaluated_at"] = valid_until
        for row in equality["transcripts"]:
            row["raw_transcript"]["captured_started_at"] = (
                "2026-08-01T12:00:24Z"
            )
            row["raw_transcript"]["captured_finished_at"] = valid_until
        rehash(equality)
        self.assertEqual(validate(equality), equality)

        cases = (
            (
                "raw-after-valid",
                "2026-08-01T12:00:30Z",
                "2026-08-01T12:00:59.000001Z",
                "2026-08-01T12:00:59.000001Z",
            ),
            (
                "evaluated-after-valid",
                "2026-08-01T12:00:01Z",
                "2026-08-01T12:00:02Z",
                "2026-08-01T12:00:59.000001Z",
            ),
        )
        for label, started, finished, evaluated in cases:
            candidate = copy.deepcopy(value)
            candidate["evaluated_at"] = evaluated
            for row in candidate["transcripts"]:
                row["raw_transcript"]["captured_started_at"] = started
                row["raw_transcript"]["captured_finished_at"] = finished
            rehash(candidate)
            with self.subTest(case=label), self.assertRaisesRegex(
                route_cost_evidence.RouteCostEvidenceError,
                "native validity",
            ):
                validate(candidate)

    def test_profile_identities_bind_generation_rpc_source_and_connector(self):
        value, universe, retained = supported_observed_manifest()
        self.assertEqual(
            value["trace_profile_identity"],
            {
                "schema": "route_cost_trace_profile_identity/v1",
                "status": "available",
                "profile_id": "kat-trace",
                "endpoint_id": "kat-rpc",
            },
        )
        self.assertEqual(
            value["submission_connector_profile_identity"],
            {
                "schema": "route_cost_submission_connector_identity/v1",
                "status": "available",
                "profile_id": "kat-connector",
                "connector_id": "kat_connector",
            },
        )

        def validate(candidate):
            return route_cost_evidence.validate_route_cost_evidence_manifest_for_publication(
                candidate,
                universe=universe,
                expected_run_id=RUN_ID,
                expected_route_cohort_id=COHORT_ID,
                expected_phase=PHASE,
                expected_candidate_source_generation=GENERATION,
                expected_route_universe_sha256=physical_sha(universe),
                retained_typed_pool_state_members=retained,
            )

        for field, mutate in (
            (
                "rpc source",
                lambda item: item["chain_evidence"][0].__setitem__(
                    "rpc_source_id", "attacker-rpc"
                ),
            ),
            (
                "trace identity",
                lambda item: item["trace_profile_identity"].__setitem__(
                    "endpoint_id", "attacker-rpc"
                ),
            ),
            (
                "connector identity",
                lambda item: item[
                    "submission_connector_profile_identity"
                ].__setitem__("connector_id", "attacker_connector"),
            ),
        ):
            forged = copy.deepcopy(value)
            mutate(forged)
            with self.subTest(field=field):
                with self.assertRaises(route_cost_evidence.RouteCostEvidenceError):
                    validate(forged)

    def test_historical_embedded_registry_replays_after_current_registry_rotation(self):
        historical = unsupported_manifest(tracked=False)
        self.assertNotEqual(
            historical["adapter_registry"],
            route_cost_evidence.load_route_cost_adapter_registry(),
        )
        self.assertNotEqual(
            historical["connector_key_registry"]["registry_version"],
            route_cost_evidence.load_route_cost_connector_key_registry()[
                "registry_version"
            ],
        )
        self.assertEqual(self.validate(historical), historical)

    def test_retained_v2_member_replays_exact_descriptor_bytes_and_derived_id(self):
        retained = retained_v2_pool_state()
        expected = json.loads(retained["payload"].decode("utf-8"))
        self.assertEqual(
            route_cost_evidence.validate_retained_v2_pool_state_member(
                retained["payload"], descriptor=retained["descriptor"]
            ),
            expected,
        )

        cases = []
        extra_descriptor = copy.deepcopy(retained)
        extra_descriptor["descriptor"]["extra"] = True
        cases.append(("descriptor extra", extra_descriptor))

        wrong_role = copy.deepcopy(retained)
        wrong_role["descriptor"]["role"] = "dex_usd_price_context"
        cases.append(("descriptor role", wrong_role))

        one_byte = copy.deepcopy(retained)
        one_byte["payload"] += b"\n"
        one_byte["descriptor"]["size"] += 1
        one_byte["descriptor"]["sha256"] = hashlib.sha256(
            one_byte["payload"]
        ).hexdigest()
        cases.append(("noncanonical payload byte", one_byte))

        derived = copy.deepcopy(retained)
        derived_value = json.loads(derived["payload"])
        derived_value["state_id"] = "dex-v2-quantity:" + "0" * 64
        derived["payload"] = canonical_bytes(derived_value)
        derived["descriptor"]["size"] = len(derived["payload"])
        derived["descriptor"]["sha256"] = hashlib.sha256(
            derived["payload"]
        ).hexdigest()
        cases.append(("derived state id", derived))

        for label, item in cases:
            with self.subTest(label=label):
                with self.assertRaises(route_cost_evidence.RouteCostEvidenceError):
                    route_cost_evidence.validate_retained_v2_pool_state_member(
                        item["payload"], descriptor=item["descriptor"]
                    )

    def test_pure_manifest_refuses_core_state_without_retained_bytes(self):
        value, _retained = supported_core_manifest()
        with self.assertRaises(route_cost_evidence.RouteCostEvidenceError):
            self.validate(value)

    def test_publication_replays_historical_retained_state_and_rejects_transplants(self):
        value, retained = supported_core_manifest()
        validated = route_cost_evidence.validate_route_cost_evidence_manifest_for_publication(
            value,
            universe=universe_for(),
            expected_run_id=RUN_ID,
            expected_route_cohort_id=COHORT_ID,
            expected_phase=PHASE,
            expected_candidate_source_generation=GENERATION,
            expected_route_universe_sha256=physical_sha(universe_for()),
            retained_typed_pool_state_members={MARKET_ID: retained},
        )
        self.assertEqual(validated, value)

        missing = {}
        extra = {
            MARKET_ID: retained,
            "dex:eth:uniswap_v2:0x4444444444444444444444444444444444444444:BBB": (
                retained_v2_pool_state(
                    market_id=(
                        "dex:eth:uniswap_v2:"
                        "0x4444444444444444444444444444444444444444:BBB"
                    ),
                    pool_address="0x4444444444444444444444444444444444444444",
                )
            ),
        }
        cross_market = retained_v2_pool_state(
            market_id=(
                "dex:eth:uniswap_v2:"
                "0x4444444444444444444444444444444444444444:BBB"
            ),
            pool_address="0x4444444444444444444444444444444444444444",
        )
        block_plus_one = retained_v2_pool_state(
            block_number=101, block_timestamp=1785585601
        )
        cases = (
            ("missing", missing),
            ("extra", extra),
            ("cross market", {MARKET_ID: cross_market}),
            ("cross block", {MARKET_ID: block_plus_one}),
        )
        for label, retained_members in cases:
            with self.subTest(label=label):
                with self.assertRaises(route_cost_evidence.RouteCostEvidenceError):
                    route_cost_evidence.validate_route_cost_evidence_manifest_for_publication(
                        value,
                        universe=universe_for(),
                        expected_run_id=RUN_ID,
                        expected_route_cohort_id=COHORT_ID,
                        expected_phase=PHASE,
                        expected_candidate_source_generation=GENERATION,
                        expected_route_universe_sha256=physical_sha(universe_for()),
                        retained_typed_pool_state_members=retained_members,
                    )

        # Rebind every cost-side core ID/SHA and all dependent transcript and
        # binding hashes to B+1.  This proves rejection comes from replaying the
        # retained state against the fixed chain anchor, not an opaque hash.
        transplanted = copy.deepcopy(value)
        new_state = json.loads(block_plus_one["payload"])
        for transcript in transplanted["transcripts"]:
            transcript["core_pool_state_id"] = new_state["state_id"]
            transcript["core_pool_state_sha256"] = block_plus_one[
                "descriptor"
            ]["sha256"]
        transplanted["transcript_set_sha256"] = typed_sha(
            b"route-cost-evidence-transcript-set/v1\n",
            transplanted["transcripts"],
        )
        transcript_by_scope = {
            (row["direction"], row["requested_notional_usd"]): row
            for row in transplanted["transcripts"]
        }
        for binding in transplanted["bindings"]:
            binding["buy_transcript_sha256"] = typed_sha(
                b"route-cost-evidence-transcript/v1\n",
                transcript_by_scope[
                    ("buy", binding["requested_notional_usd"])
                ],
            )
        transplanted["binding_set_sha256"] = typed_sha(
            b"route-cost-evidence-binding-set/v1\n",
            transplanted["bindings"],
        )
        with self.assertRaises(route_cost_evidence.RouteCostEvidenceError):
            route_cost_evidence.validate_route_cost_evidence_manifest_for_publication(
                transplanted,
                universe=universe_for(),
                expected_run_id=RUN_ID,
                expected_route_cohort_id=COHORT_ID,
                expected_phase=PHASE,
                expected_candidate_source_generation=GENERATION,
                expected_route_universe_sha256=physical_sha(universe_for()),
                retained_typed_pool_state_members={MARKET_ID: block_plus_one},
            )

    def test_cross_notional_target_transplant_fails_after_full_rehash(self):
        value, retained = supported_core_manifest()
        forged = copy.deepcopy(value)
        by_scope = {
            (row["direction"], row["requested_notional_usd"]): row
            for row in forged["transcripts"]
        }
        source = by_scope[("buy", "5000")]
        destination = by_scope[("buy", "1000")]
        for field in (
            "simulation_target_token_address",
            "simulation_target_unit_decimals",
            "simulation_target_raw_quantity",
            "simulation_target_lattice_raw",
            "simulation_target_sha256",
        ):
            destination[field] = source[field]
        forged["transcript_set_sha256"] = typed_sha(
            b"route-cost-evidence-transcript-set/v1\n", forged["transcripts"]
        )
        for binding in forged["bindings"]:
            if binding["requested_notional_usd"] == "1000":
                binding["buy_transcript_sha256"] = typed_sha(
                    b"route-cost-evidence-transcript/v1\n", destination
                )
        forged["binding_set_sha256"] = typed_sha(
            b"route-cost-evidence-binding-set/v1\n", forged["bindings"]
        )
        with self.assertRaisesRegex(
            route_cost_evidence.RouteCostEvidenceError,
            "simulation target",
        ):
            route_cost_evidence.validate_route_cost_evidence_manifest_for_publication(
                forged,
                universe=universe_for(),
                expected_run_id=RUN_ID,
                expected_route_cohort_id=COHORT_ID,
                expected_phase=PHASE,
                expected_candidate_source_generation=GENERATION,
                expected_route_universe_sha256=physical_sha(universe_for()),
                retained_typed_pool_state_members={MARKET_ID: retained},
            )

    def test_market_token0_token1_order_is_bound_to_retained_state(self):
        value, retained = supported_core_manifest()
        state = json.loads(retained["payload"])
        chain = value["chain_evidence"][0]
        links = {
            field: value[field]
            for field in (
                "run_id", "route_cohort_id", "candidate_source_generation",
                "route_universe_sha256", "adapter_registry_sha256",
                "selected_market_set_sha256",
            )
        }
        market = {
            "schema": "route_cost_market_evidence/v1",
            **links,
            "market_id": MARKET_ID,
            "adapter_id": ADAPTER_ID,
            "chain_evidence_sha256": physical_sha(chain),
            "core_pool_state_id": state["state_id"],
            "core_pool_state_sha256": retained["descriptor"]["sha256"],
            "router_address": ROUTER,
            "router_runtime_code": "0x" + pinned_runtime_code(
                "uniswap-v2-router02-runtime"
            ).hex(),
            "factory_address": FACTORY,
            "factory_runtime_code": "0x" + pinned_runtime_code(
                "uniswap-v2-factory-runtime"
            ).hex(),
            "factory_get_pair_request": {
                "schema": "route_cost_factory_get_pair_request/v1",
                "jsonrpc": "2.0",
                "id": 7,
                "method": "eth_call",
                "params": [{
                    "to": FACTORY,
                    "data": route_cost_evidence.build_factory_get_pair_calldata(
                        TOKEN_A, TOKEN_B
                    ),
                }, chain["block_header_result"]["number"]],
            },
            "factory_get_pair_response": {
                "schema": "route_cost_factory_get_pair_response/v1",
                "jsonrpc": "2.0",
                "id": 7,
                "result": "0x" + "0" * 24 + POOL[2:],
            },
            "pair_address": POOL,
            "pair_runtime_code": "0x" + pinned_pair_runtime_code().hex(),
            "pair_token0": TOKEN_A,
            "pair_token1": TOKEN_B,
            "token_runtime_code_evidence": runtime_code_evidence(
                (TOKEN_A, TOKEN_B),
                block_tag=chain["block_header_result"]["number"],
            ),
            "captured_started_at": "2026-08-01T12:00:00Z",
            "captured_finished_at": "2026-08-01T12:00:00Z",
        }
        for descriptor in value["adapter_registry"]["adapters"][0][
            "token_funding_descriptors"
        ]:
            descriptor["runtime_code_sha256"] = hashlib.sha256(b"\x01").hexdigest()
        retained_replayed = copy.deepcopy(state)
        retained_replayed["_physical_sha256"] = retained["descriptor"]["sha256"]
        route_cost_evidence._validate_market_evidence(
            market,
            links,
            {physical_sha(chain): chain},
            {MARKET_ID},
            value["adapter_registry"]["adapters"][0],
            {MARKET_ID: retained_replayed},
        )
        forged = copy.deepcopy(market)
        forged["pair_token0"], forged["pair_token1"] = (
            forged["pair_token1"], forged["pair_token0"]
        )
        forged["factory_get_pair_request"]["params"][0]["data"] = (
            route_cost_evidence.build_factory_get_pair_calldata(
                TOKEN_B, TOKEN_A
            )
        )
        with self.assertRaisesRegex(
            route_cost_evidence.RouteCostEvidenceError,
            "static pair descriptor",
        ):
            route_cost_evidence._validate_market_evidence(
                forged,
                links,
                {physical_sha(chain): chain},
                {MARKET_ID},
                value["adapter_registry"]["adapters"][0],
                {MARKET_ID: retained_replayed},
            )
        wrong_descriptor = copy.deepcopy(value["adapter_registry"]["adapters"][0])
        wrong_descriptor["token_funding_descriptors"][0][
            "runtime_code_sha256"
        ] = "0" * 64
        with self.assertRaisesRegex(
            route_cost_evidence.RouteCostEvidenceError,
            "token funding runtime-code identity",
        ):
            route_cost_evidence._validate_market_evidence(
                market,
                links,
                {physical_sha(chain): chain},
                {MARKET_ID},
                wrong_descriptor,
                {MARKET_ID: retained_replayed},
            )
        wrong_block = copy.deepcopy(market)
        wrong_block["token_runtime_code_evidence"][0]["request"]["params"][1] = (
            "0x65"
        )
        with self.assertRaisesRegex(
            route_cost_evidence.RouteCostEvidenceError,
            "runtime-code request differs",
        ):
            route_cost_evidence._validate_market_evidence(
                wrong_block,
                links,
                {physical_sha(chain): chain},
                {MARKET_ID},
                value["adapter_registry"]["adapters"][0],
                {MARKET_ID: retained_replayed},
            )

    def test_market_pair_runtime_code_must_match_static_pair_descriptor(self):
        value, retained = supported_core_manifest()
        state = json.loads(retained["payload"])
        chain = value["chain_evidence"][0]
        links = {
            field: value[field]
            for field in (
                "run_id", "route_cohort_id", "candidate_source_generation",
                "route_universe_sha256", "adapter_registry_sha256",
                "selected_market_set_sha256",
            )
        }
        market = {
            "schema": "route_cost_market_evidence/v1",
            **links,
            "market_id": MARKET_ID,
            "adapter_id": ADAPTER_ID,
            "chain_evidence_sha256": physical_sha(chain),
            "core_pool_state_id": state["state_id"],
            "core_pool_state_sha256": retained["descriptor"]["sha256"],
            "router_address": ROUTER,
            "router_runtime_code": "0x" + pinned_runtime_code(
                "uniswap-v2-router02-runtime"
            ).hex(),
            "factory_address": FACTORY,
            "factory_runtime_code": "0x" + pinned_runtime_code(
                "uniswap-v2-factory-runtime"
            ).hex(),
            "factory_get_pair_request": {
                "schema": "route_cost_factory_get_pair_request/v1",
                "jsonrpc": "2.0", "id": 7, "method": "eth_call",
                "params": [{
                    "to": FACTORY,
                    "data": route_cost_evidence.build_factory_get_pair_calldata(
                        TOKEN_A, TOKEN_B
                    ),
                }, chain["block_header_result"]["number"]],
            },
            "factory_get_pair_response": {
                "schema": "route_cost_factory_get_pair_response/v1",
                "jsonrpc": "2.0", "id": 7,
                "result": "0x" + "0" * 24 + POOL[2:],
            },
            "pair_address": POOL,
            "pair_runtime_code": "0x01",
            "pair_token0": TOKEN_A,
            "pair_token1": TOKEN_B,
            "token_runtime_code_evidence": runtime_code_evidence(
                (TOKEN_A, TOKEN_B),
                block_tag=chain["block_header_result"]["number"],
            ),
            "captured_started_at": "2026-08-01T12:00:00Z",
            "captured_finished_at": "2026-08-01T12:00:00Z",
        }
        adapter_value = value["adapter_registry"]["adapters"][0]
        for descriptor in adapter_value["token_funding_descriptors"]:
            descriptor["runtime_code_sha256"] = hashlib.sha256(b"\x01").hexdigest()
        replayed = copy.deepcopy(state)
        replayed["_physical_sha256"] = retained["descriptor"]["sha256"]

        with self.assertRaisesRegex(
            route_cost_evidence.RouteCostEvidenceError,
            "pair_runtime_code identity mismatch",
        ):
            route_cost_evidence._validate_market_evidence(
                market,
                links,
                {physical_sha(chain): chain},
                {MARKET_ID},
                adapter_value,
                {MARKET_ID: replayed},
            )

        market["pair_runtime_code"] = "0x" + pinned_pair_runtime_code().hex()
        route_cost_evidence._validate_market_evidence(
            market,
            links,
            {physical_sha(chain): chain},
            {MARKET_ID},
            adapter_value,
            {MARKET_ID: replayed},
        )

    def test_scope_hash_status_and_null_matrix_are_replayed_not_trusted(self):
        cases = []
        extra = unsupported_manifest(tracked=True)
        extra["extra"] = True
        cases.append(("extra top field", extra))

        shrink = unsupported_manifest(tracked=True)
        shrink["transcripts"] = shrink["transcripts"][:-1]
        shrink["transcript_count"] = len(shrink["transcripts"])
        shrink["transcript_set_sha256"] = typed_sha(
            b"route-cost-evidence-transcript-set/v1\n", shrink["transcripts"]
        )
        shrink["counts"]["transcript_unavailable"] -= 1
        cases.append(("denominator shrink", shrink))

        forged = unsupported_manifest(tracked=True)
        forged["transcripts"][0]["status"] = "observed"
        forged["transcripts"][0]["reason_code"] = None
        forged["transcript_set_sha256"] = typed_sha(
            b"route-cost-evidence-transcript-set/v1\n", forged["transcripts"]
        )
        forged["counts"]["transcript_unavailable"] -= 1
        forged["counts"]["transcript_observed"] += 1
        cases.append(("unsupported observed", forged))

        bad_nulls = unsupported_manifest(tracked=True)
        bad_nulls["transcripts"][0]["core_pool_state_sha256"] = "6" * 64
        bad_nulls["transcript_set_sha256"] = typed_sha(
            b"route-cost-evidence-transcript-set/v1\n", bad_nulls["transcripts"]
        )
        cases.append(("contradictory null matrix", bad_nulls))

        for label, value in cases:
            with self.subTest(label=label):
                with self.assertRaises(route_cost_evidence.RouteCostEvidenceError):
                    self.validate(value)

    def test_forged_raw_rpc_nested_objects_are_rejected(self):
        transcript = {
            "chain_evidence_sha256": "1" * 64,
            "market_evidence_sha256": "2" * 64,
        }
        forged = {
            "schema": "route_cost_raw_transcript/v1",
            "chain_evidence_sha256": "1" * 64,
            "market_evidence_sha256": "2" * 64,
            "captured_started_at": "2026-08-01T12:00:00Z",
            "captured_finished_at": "2026-08-01T12:00:01Z",
            "calldata_hex": None,
            "estimate_gas_request": {"forged": "accepted"},
            "estimate_gas_response": ["anything"],
            "simulation_method": "not-debug-trace",
            "simulation_request": "opaque",
            "simulation_response": 123,
            "simulation_balance_deltas": {"unplanned": "slot"},
        }
        with self.assertRaises(route_cost_evidence.RouteCostEvidenceError):
            route_cost_evidence._validate_raw_transcript(
                forged,
                transcript=transcript,
                adapter=adapter(),
                market={"pair_address": POOL},
                chain={"block_header_result": {"number": "0x64"}},
            )

    def test_forged_authenticated_snapshot_is_never_structurally_trusted(self):
        historical_keys = connector_registry()
        links = {
            "run_id": RUN_ID,
            "route_cohort_id": COHORT_ID,
            "candidate_source_generation": GENERATION,
            "route_universe_sha256": "1" * 64,
            "adapter_registry_sha256": "2" * 64,
            "selected_market_set_sha256": "3" * 64,
            "connector_key_registry_sha256": physical_sha(historical_keys),
            "trace_profile_generation": "4" * 64,
            "submission_connector_profile_generation": "5" * 64,
        }
        member = {
            "schema": "route_cost_submission_policy_member/v1",
            "route_id": "route-a",
            "requested_notional_usd": "1000",
            "status": "observed",
            "reason_code": None,
            "submission_mode": "private_relay",
            "policy_id": "policy-a",
            "buy_submission_loss_bps": None,
            "sell_submission_loss_bps": None,
        }
        snapshot = {
            "schema": "route_cost_submission_policy_snapshot/v1",
            **links,
            "connector_id": "connector-a",
            "member_count": 1,
            "members": [member],
            "member_set_sha256": typed_sha(
                b"route-cost-submission-policy-member-set/v1\n", [member]
            ),
            "status": "authenticated",
            "reason_code": None,
            "observed_at": "2026-08-01T12:00:00Z",
            "valid_until": "2026-08-01T12:01:00Z",
            "issuer_key_id": "nonexistent-key",
            "signature_algorithm": "ssh-ed25519-sshsig-v1",
            "attested_payload_sha256": "0" * 64,
            "signature": "definitely-not-an-sshsig",
        }
        with self.assertRaises(route_cost_evidence.RouteCostEvidenceError):
            route_cost_evidence._validate_policy_snapshot(
                snapshot,
                links,
                binding_scope_empty=False,
                connector_registry=historical_keys,
                route_sides={"route-a": (True, False)},
                permit_authenticated=False,
            )

    def test_fixed_sshsig_known_answer_verifies_and_tamper_fails(self):
        snapshot, keys = signed_snapshot_kat()
        route_cost_evidence._verify_snapshot_sshsig_fixed(snapshot, keys)
        forged = copy.deepcopy(snapshot)
        forged["valid_until"] = "2026-08-01T12:00:59Z"
        forged["attested_payload_sha256"] = typed_sha(
            b"route-cost-submission-policy-attestation/v1\n",
            route_cost_evidence._policy_attestation(forged),
        )
        with self.assertRaises(route_cost_evidence.RouteCostEvidenceError):
            route_cost_evidence._verify_snapshot_sshsig_fixed(forged, keys)

    def test_arbitrary_storage_keys_and_pair_recipient_shortfall_are_rejected(self):
        transcript = {
            "chain_evidence_sha256": "1" * 64,
            "market_evidence_sha256": "2" * 64,
        }
        raw = _complete_raw_transcript()
        route_cost_evidence._validate_raw_transcript(
            raw,
            transcript=transcript,
            adapter=adapter(),
            market={"pair_address": POOL},
            chain={"block_header_result": {"number": "0x64"}},
        )
        forged_keys = _complete_raw_transcript(arbitrary_keys=True)
        with self.assertRaises(route_cost_evidence.RouteCostEvidenceError):
            route_cost_evidence._validate_raw_transcript(
                forged_keys,
                transcript=transcript,
                adapter=adapter(),
                market={"pair_address": POOL},
                chain={"block_header_result": {"number": "0x64"}},
            )

        short_raw = _complete_raw_transcript(short_pair_output=True)
        # Re-derive the stored balance projection from the internally
        # self-consistent short trace, so this targets conservation rather than
        # an outer hash/list mismatch.
        short_raw["simulation_balance_deltas"] = [
            {
                "token_address": row["token_address"],
                "account_role": row["account_role"],
                "pre_balance_raw": str(int(row["pre_value"], 16)),
                "post_balance_raw": str(int(row["post_value"], 16)),
            }
            for row in short_raw["simulation_response"]["storage_diffs"]
            if not (
                row["account_role"] == "sender"
                and row["storage_key"]
                == route_cost_evidence.solidity_allowance_storage_key(
                    SENDER, ROUTER, 1
                )
            )
        ]
        transfer = {
            "schema": "route_cost_transfer_tax_evidence/v1",
            "status": "not_applicable",
            "rate_bps": None,
            "pre_input_balance": "1000",
            "post_input_balance": "900",
            "pre_output_balance": "0",
            "post_output_balance": "50",
            "trace_method": "debug_traceCall_state_override_v1",
            "trace_sha256": typed_sha(
                b"route-cost-trace/v1\n",
                {
                    "request": short_raw["simulation_request"],
                    "response": short_raw["simulation_response"],
                },
            ),
        }
        with self.assertRaises(route_cost_evidence.RouteCostEvidenceError):
            route_cost_evidence._validate_component(
                transfer, component="transfer", raw=short_raw,
                call_evidence={
                    "amount_in_raw": "100",
                    "amount_out_raw": "50",
                },
            )

    def test_raw_rpc_block_tags_bind_to_fixed_chain_header(self):
        transcript = {
            "chain_evidence_sha256": "1" * 64,
            "market_evidence_sha256": "2" * 64,
        }
        chain = {"block_header_result": {"number": "0x64"}}
        raw = _complete_raw_transcript()
        route_cost_evidence._validate_raw_transcript(
            raw,
            transcript=transcript,
            adapter=adapter(),
            market={"pair_address": POOL},
            chain=chain,
        )
        forged = copy.deepcopy(raw)
        forged["estimate_gas_request"]["params"][1] = "0x65"
        forged["simulation_request"]["params"][1] = "0x65"
        with self.assertRaisesRegex(
            route_cost_evidence.RouteCostEvidenceError,
            "block tag|fixed block",
        ):
            route_cost_evidence._validate_raw_transcript(
                forged,
                transcript=transcript,
                adapter=adapter(),
                market={"pair_address": POOL},
                chain=chain,
            )

    def test_completed_stage_requires_exact_evidence_prefix(self):
        selected = {
            MARKET_ID: {
                "structural_support_status": "supported",
            }
        }
        links = {
            "run_id": RUN_ID,
            "route_cohort_id": COHORT_ID,
            "candidate_source_generation": GENERATION,
            "route_universe_sha256": "1" * 64,
            "adapter_registry_sha256": "2" * 64,
            "selected_market_set_sha256": "3" * 64,
            "trace_profile_generation": "4" * 64,
            "submission_connector_profile_generation": "5" * 64,
        }
        transcript = {
            "schema": "route_cost_evidence_transcript/v1",
            **links,
            "market_id": MARKET_ID,
            "direction": "buy",
            "requested_notional_usd": "1000",
            "adapter_id": ADAPTER_ID,
            "simulation_target_token_address": None,
            "simulation_target_unit_decimals": None,
            "simulation_target_raw_quantity": None,
            "simulation_target_lattice_raw": None,
            "simulation_target_sha256": None,
            "core_pool_state_id": None,
            "core_pool_state_sha256": None,
            "chain_evidence_sha256": None,
            "market_evidence_sha256": None,
            "status": "unavailable",
            "completed_stage": "call",
            "reason_code": "gas_unavailable",
            "block_evidence": None,
            "call_evidence": None,
            "gas_evidence": None,
            "router_fee_evidence": None,
            "transfer_tax_evidence": None,
            "raw_transcript": None,
        }
        with self.assertRaises(route_cost_evidence.RouteCostEvidenceError):
            route_cost_evidence._validate_transcript(
                transcript,
                links=links,
                selected=selected,
                chain_hashes={},
                market_hashes={},
                adapter=adapter(),
                native_sha=None,
                native_evidence=None,
                retained_pool_states={},
            )

    def test_raw_transcript_stage_prefix_accepts_only_exact_captured_fields(self):
        transcript = {
            "chain_evidence_sha256": "1" * 64,
            "market_evidence_sha256": "2" * 64,
        }
        raw = _complete_raw_transcript()
        calldata_only = {
            **raw,
            "estimate_gas_request": None,
            "estimate_gas_response": None,
            "simulation_method": None,
            "simulation_request": None,
            "simulation_response": None,
            "simulation_balance_deltas": None,
        }
        route_cost_evidence._validate_raw_transcript(
            calldata_only,
            transcript=transcript,
            adapter=adapter(),
            market={"pair_address": POOL},
            completed_stage="block",
        )

        call_complete = {
            **raw,
            "simulation_method": None,
            "simulation_request": None,
            "simulation_response": None,
            "simulation_balance_deltas": None,
        }
        route_cost_evidence._validate_raw_transcript(
            call_complete,
            transcript=transcript,
            adapter=adapter(),
            market={"pair_address": POOL},
            chain={"block_header_result": {"number": "0x64"}},
            completed_stage="call",
        )
        forged = copy.deepcopy(call_complete)
        forged["simulation_method"] = adapter()["trace_method"]
        with self.assertRaises(route_cost_evidence.RouteCostEvidenceError):
            route_cost_evidence._validate_raw_transcript(
                forged,
                transcript=transcript,
                adapter=adapter(),
                market={"pair_address": POOL},
                chain={"block_header_result": {"number": "0x64"}},
                completed_stage="call",
            )

    def test_phase_b_request_only_failure_shapes_are_truthfully_accepted(self):
        transcript = {
            "chain_evidence_sha256": "1" * 64,
            "market_evidence_sha256": "2" * 64,
        }
        common = {
            "transcript": transcript,
            "adapter": adapter(),
            "market": {"pair_address": POOL},
            "chain": {"block_header_result": {"number": "0x64"}},
        }
        complete = _complete_raw_transcript()
        cases = []
        for status, reason in (
            ("unavailable", "native_price_unavailable"),
            ("failed", "native_price_invalid"),
        ):
            raw = copy.deepcopy(complete)
            for field in (
                "estimate_gas_request", "estimate_gas_response",
                "simulation_method", "simulation_request",
                "simulation_response", "simulation_balance_deltas",
            ):
                raw[field] = None
            cases.append((status, reason, "call", raw))
        for status, reason in (
            ("unavailable", "gas_unavailable"),
            ("failed", "gas_invalid"),
        ):
            raw = copy.deepcopy(complete)
            raw["estimate_gas_response"] = None
            for field in (
                "simulation_method", "simulation_request",
                "simulation_response", "simulation_balance_deltas",
            ):
                raw[field] = None
            cases.append((status, reason, "call", raw))
        for status, reason in (
            ("unavailable", "trace_unavailable"),
            ("failed", "trace_invalid"),
        ):
            raw = copy.deepcopy(complete)
            raw["simulation_response"] = None
            raw["simulation_balance_deltas"] = None
            cases.append((status, reason, "router_fee", raw))

        for status, reason, stage, raw in cases:
            with self.subTest(reason=reason):
                route_cost_evidence._validate_raw_transcript(
                    raw,
                    completed_stage=stage,
                    status=status,
                    reason_code=reason,
                    **common
                )

    def test_gas_unavailable_deadline_before_send_keeps_request_null(self):
        raw = copy.deepcopy(_complete_raw_transcript())
        for field in (
            "estimate_gas_request", "estimate_gas_response",
            "simulation_method", "simulation_request",
            "simulation_response", "simulation_balance_deltas",
        ):
            raw[field] = None
        route_cost_evidence._validate_raw_transcript(
            raw,
            transcript={
                "chain_evidence_sha256": "1" * 64,
                "market_evidence_sha256": "2" * 64,
            },
            adapter=adapter(),
            market={"pair_address": POOL},
            chain={"block_header_result": {"number": "0x64"}},
            completed_stage="call",
            status="unavailable",
            reason_code="gas_unavailable",
        )

    def test_phase_b_failure_dependencies_and_request_identity_are_closed(self):
        transcript = {
            "chain_evidence_sha256": "1" * 64,
            "market_evidence_sha256": "2" * 64,
        }
        common = {
            "transcript": transcript,
            "adapter": adapter(),
            "market": {"pair_address": POOL},
            "chain": {"block_header_result": {"number": "0x64"}},
        }

        response_without_request = _complete_raw_transcript()
        response_without_request["estimate_gas_request"] = None
        for field in (
            "simulation_method", "simulation_request",
            "simulation_response", "simulation_balance_deltas",
        ):
            response_without_request[field] = None
        with self.assertRaises(route_cost_evidence.RouteCostEvidenceError):
            route_cost_evidence._validate_raw_transcript(
                response_without_request,
                completed_stage="call",
                status="unavailable",
                reason_code="gas_unavailable",
                **common
            )

        trace_without_successful_estimate = _complete_raw_transcript()
        trace_without_successful_estimate["estimate_gas_response"] = None
        trace_without_successful_estimate["simulation_response"] = None
        trace_without_successful_estimate["simulation_balance_deltas"] = None
        with self.assertRaises(route_cost_evidence.RouteCostEvidenceError):
            route_cost_evidence._validate_raw_transcript(
                trace_without_successful_estimate,
                completed_stage="router_fee",
                status="unavailable",
                reason_code="trace_unavailable",
                **common
            )

        bad_requests = []
        for mutate in (
            lambda raw: raw["estimate_gas_request"].__setitem__("method", "eth_call"),
            lambda raw: raw["estimate_gas_request"].__setitem__("id", 0),
            lambda raw: raw["estimate_gas_request"]["params"].__setitem__(1, "0x65"),
            lambda raw: raw["estimate_gas_request"]["params"][0].__setitem__("data", "0x"),
        ):
            raw = copy.deepcopy(_complete_raw_transcript())
            raw["estimate_gas_response"] = None
            for field in (
                "simulation_method", "simulation_request",
                "simulation_response", "simulation_balance_deltas",
            ):
                raw[field] = None
            mutate(raw)
            bad_requests.append(raw)
        for raw in bad_requests:
            with self.assertRaises(route_cost_evidence.RouteCostEvidenceError):
                route_cost_evidence._validate_raw_transcript(
                    raw,
                    completed_stage="call",
                    status="failed",
                    reason_code="gas_invalid",
                    **common
                )

        estimate_response_only = copy.deepcopy(_complete_raw_transcript())
        estimate_response_only["estimate_gas_request"] = None
        for field in (
            "simulation_method", "simulation_request",
            "simulation_response", "simulation_balance_deltas",
        ):
            estimate_response_only[field] = None
        trace_response_only = copy.deepcopy(_complete_raw_transcript())
        trace_response_only["simulation_request"] = None
        for label, raw, stage, status, reason in (
            (
                "estimate response without request", estimate_response_only,
                "call", "unavailable", "gas_unavailable",
            ),
            (
                "trace response without request", trace_response_only,
                "router_fee", "unavailable", "trace_unavailable",
            ),
        ):
            with self.subTest(label=label):
                with self.assertRaises(route_cost_evidence.RouteCostEvidenceError):
                    route_cost_evidence._validate_raw_transcript(
                        raw, completed_stage=stage, status=status,
                        reason_code=reason, **common
                    )

        for field, value in (
            ("method", "debug_traceTransaction"),
            ("id", 0),
        ):
            raw = copy.deepcopy(_complete_raw_transcript())
            raw["simulation_response"] = None
            raw["simulation_balance_deltas"] = None
            raw["simulation_request"][field] = value
            with self.subTest(trace_field=field):
                with self.assertRaises(route_cost_evidence.RouteCostEvidenceError):
                    route_cost_evidence._validate_raw_transcript(
                        raw, completed_stage="router_fee", status="failed",
                        reason_code="trace_invalid", **common
                    )

    def test_unavailable_failures_may_omit_only_a_not_issued_request(self):
        transcript = {
            "chain_evidence_sha256": "1" * 64,
            "market_evidence_sha256": "2" * 64,
        }
        common = {
            "transcript": transcript,
            "adapter": adapter(),
            "market": {"pair_address": POOL},
            "chain": {"block_header_result": {"number": "0x64"}},
        }
        raw = copy.deepcopy(_complete_raw_transcript())
        raw["simulation_method"] = None
        raw["simulation_request"] = None
        raw["simulation_response"] = None
        raw["simulation_balance_deltas"] = None
        route_cost_evidence._validate_raw_transcript(
            raw, completed_stage="router_fee", status="unavailable",
            reason_code="trace_unavailable", **common
        )

        invalid = copy.deepcopy(raw)
        with self.assertRaises(route_cost_evidence.RouteCostEvidenceError):
            route_cost_evidence._validate_raw_transcript(
                invalid, completed_stage="router_fee", status="failed",
                reason_code="trace_invalid", **common
            )

        for label, mutate in (
            (
                "trace block",
                lambda raw: raw["simulation_request"]["params"].__setitem__(
                    1, "0x65"
                ),
            ),
            (
                "trace calldata",
                lambda raw: raw["simulation_request"]["params"][0].__setitem__(
                    "data", "0x"
                ),
            ),
            (
                "trace override",
                lambda raw: raw["simulation_request"]["params"][2][
                    "stateOverrides"
                ][SENDER].__setitem__("balance", _word(10 ** 18 + 1)),
            ),
        ):
            raw = copy.deepcopy(_complete_raw_transcript())
            raw["simulation_response"] = None
            raw["simulation_balance_deltas"] = None
            mutate(raw)
            with self.subTest(label=label):
                with self.assertRaises(route_cost_evidence.RouteCostEvidenceError):
                    route_cost_evidence._validate_raw_transcript(
                        raw, completed_stage="router_fee", status="failed",
                        reason_code="trace_invalid", **common
                    )

    def test_resource_limit_accepts_only_real_boundary_prefixes(self):
        transcript = {
            "chain_evidence_sha256": "1" * 64,
            "market_evidence_sha256": "2" * 64,
        }
        common = {
            "transcript": transcript,
            "adapter": adapter(),
            "market": {"pair_address": POOL},
            "chain": {"block_header_result": {"number": "0x64"}},
            "status": "failed",
            "reason_code": "resource_limit",
        }
        raw = copy.deepcopy(_complete_raw_transcript())
        raw["estimate_gas_response"] = None
        for field in (
            "simulation_method", "simulation_request",
            "simulation_response", "simulation_balance_deltas",
        ):
            raw[field] = None
        route_cost_evidence._validate_raw_transcript(
            raw, completed_stage="call", **common
        )
        forged = copy.deepcopy(raw)
        forged["simulation_method"] = adapter()["trace_method"]
        with self.assertRaises(route_cost_evidence.RouteCostEvidenceError):
            route_cost_evidence._validate_raw_transcript(
                forged, completed_stage="call", **common
            )

    def test_resource_limit_prefix_is_bound_to_the_declared_stage(self):
        transcript = {
            "chain_evidence_sha256": "1" * 64,
            "market_evidence_sha256": "2" * 64,
        }
        common = {
            "transcript": transcript,
            "adapter": adapter(),
            "market": {"pair_address": POOL},
            "chain": {"block_header_result": {"number": "0x64"}},
            "status": "failed",
            "reason_code": "resource_limit",
        }

        def raw_for(prefix):
            raw = copy.deepcopy(_complete_raw_transcript())
            fields = (
                "estimate_gas_request", "estimate_gas_response",
                "simulation_request", "simulation_response",
            )
            for field, present in zip(fields, prefix):
                if not present:
                    raw[field] = None
            if raw["simulation_request"] is None:
                raw["simulation_method"] = None
            if raw["simulation_response"] is None:
                raw["simulation_balance_deltas"] = None
            return raw

        accepted = {
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
        prefixes = {
            (False, False, False, False),
            (True, False, False, False),
            (True, True, False, False),
            (True, True, True, False),
            (True, True, True, True),
        }
        for stage, legal in accepted.items():
            for prefix in prefixes:
                with self.subTest(stage=stage, prefix=prefix):
                    if prefix in legal:
                        route_cost_evidence._validate_raw_transcript(
                            raw_for(prefix), completed_stage=stage, **common
                        )
                    else:
                        with self.assertRaises(
                            route_cost_evidence.RouteCostEvidenceError
                        ):
                            route_cost_evidence._validate_raw_transcript(
                                raw_for(prefix), completed_stage=stage,
                                **common
                            )

    def test_trace_request_id_must_differ_from_estimate_request_id(self):
        raw = copy.deepcopy(_complete_raw_transcript())
        raw["simulation_request"]["id"] = raw["estimate_gas_request"]["id"]
        raw["simulation_response"]["id"] = raw["estimate_gas_request"]["id"]
        with self.assertRaisesRegex(
            route_cost_evidence.RouteCostEvidenceError,
            "request ID|cross-request",
        ):
            route_cost_evidence._validate_raw_transcript(
                raw,
                transcript={
                    "chain_evidence_sha256": "1" * 64,
                    "market_evidence_sha256": "2" * 64,
                },
                adapter=adapter(),
                market={"pair_address": POOL},
                chain={"block_header_result": {"number": "0x64"}},
                completed_stage="transfer_tax",
                status="observed",
                reason_code=None,
            )

    def test_terminal_raw_matrix_accepts_closed_calldata_and_transfer_reasons(self):
        common = {
            "transcript": {
                "chain_evidence_sha256": "1" * 64,
                "market_evidence_sha256": "2" * 64,
            },
            "adapter": adapter(),
            "market": {"pair_address": POOL},
            "chain": {"block_header_result": {"number": "0x64"}},
        }
        for status, reason in (
            ("unavailable", "calldata_unavailable"),
            ("failed", "calldata_mismatch"),
        ):
            raw = copy.deepcopy(_complete_raw_transcript())
            for field in (
                "estimate_gas_request", "estimate_gas_response",
                "simulation_method", "simulation_request",
                "simulation_response", "simulation_balance_deltas",
            ):
                raw[field] = None
            if reason == "calldata_unavailable":
                raw["calldata_hex"] = None
            with self.subTest(reason=reason):
                route_cost_evidence._validate_raw_transcript(
                    raw, completed_stage="block", status=status,
                    reason_code=reason, **common
                )
        for reason in (
            "transfer_tax_present", "transfer_behavior_unsupported"
        ):
            with self.subTest(reason=reason):
                route_cost_evidence._validate_raw_transcript(
                    copy.deepcopy(_complete_raw_transcript()),
                    completed_stage="transfer_tax", status="unavailable",
                    reason_code=reason, **common
                )

    def test_rpc_response_ids_are_exact_positive_integers_not_booleans(self):
        common = {
            "transcript": {
                "chain_evidence_sha256": "1" * 64,
                "market_evidence_sha256": "2" * 64,
            },
            "adapter": adapter(),
            "market": {"pair_address": POOL},
            "chain": {"block_header_result": {"number": "0x64"}},
            "completed_stage": "transfer_tax",
            "status": "observed",
            "reason_code": None,
        }
        for field in ("estimate_gas_response", "simulation_response"):
            raw = copy.deepcopy(_complete_raw_transcript())
            raw[field]["id"] = True
            with self.subTest(field=field), self.assertRaisesRegex(
                route_cost_evidence.RouteCostEvidenceError, "response ID"
            ):
                route_cost_evidence._validate_raw_transcript(raw, **common)

    def test_full_rehash_cannot_hide_stage_downgrade_or_same_rpc_id(self):
        value, universe, retained = supported_observed_manifest()

        def rehash(candidate, transcript):
            candidate["transcript_set_sha256"] = typed_sha(
                b"route-cost-evidence-transcript-set/v1\n",
                candidate["transcripts"],
            )
            binding = next(
                row for row in candidate["bindings"]
                if row["requested_notional_usd"]
                == transcript["requested_notional_usd"]
            )
            binding["buy_transcript_sha256"] = typed_sha(
                b"route-cost-evidence-transcript/v1\n", transcript
            )
            candidate["binding_set_sha256"] = typed_sha(
                b"route-cost-evidence-binding-set/v1\n",
                candidate["bindings"],
            )

        def validate(candidate):
            return route_cost_evidence.validate_route_cost_evidence_manifest_for_publication(
                candidate,
                universe=universe,
                expected_run_id=RUN_ID,
                expected_route_cohort_id=COHORT_ID,
                expected_phase=PHASE,
                expected_candidate_source_generation=GENERATION,
                expected_route_universe_sha256=physical_sha(universe),
                retained_typed_pool_state_members=retained,
            )

        stage_attack = copy.deepcopy(value)
        stage_target = stage_attack["transcripts"][0]
        self.assertEqual(stage_target["direction"], "buy")
        stage_target.update({
            "status": "failed",
            "completed_stage": "call",
            "reason_code": "resource_limit",
            "gas_evidence": None,
            "router_fee_evidence": None,
            "transfer_tax_evidence": None,
        })
        stage_binding = next(
            row for row in stage_attack["bindings"]
            if row["requested_notional_usd"]
            == stage_target["requested_notional_usd"]
        )
        stage_binding.update({
            "status": "failed",
            "reason_code": "transcript_failed",
        })
        stage_attack["counts"].update({
            "transcript_observed": stage_attack["counts"][
                "transcript_observed"
            ] - 1,
            "transcript_failed": 1,
            "binding_observed": stage_attack["counts"]["binding_observed"] - 1,
            "binding_failed": 1,
        })
        rehash(stage_attack, stage_target)
        with self.assertRaisesRegex(
            route_cost_evidence.RouteCostEvidenceError,
            "resource-limit|stage.*prefix",
        ):
            validate(stage_attack)

        id_attack = copy.deepcopy(value)
        id_target = id_attack["transcripts"][0]
        raw = id_target["raw_transcript"]
        common_id = raw["estimate_gas_request"]["id"]
        raw["simulation_request"]["id"] = common_id
        raw["simulation_response"]["id"] = common_id
        id_target["block_evidence"]["rpc_transcript_sha256"] = typed_sha(
            b"route-cost-rpc-transcript/v1\n",
            {
                "estimate_request": raw["estimate_gas_request"],
                "estimate_response": raw["estimate_gas_response"],
                "trace_request": raw["simulation_request"],
                "trace_response": raw["simulation_response"],
            },
        )
        id_target["transfer_tax_evidence"]["trace_sha256"] = typed_sha(
            b"route-cost-trace/v1\n",
            {
                "request": raw["simulation_request"],
                "response": raw["simulation_response"],
            },
        )
        rehash(id_attack, id_target)
        with self.assertRaisesRegex(
            route_cost_evidence.RouteCostEvidenceError,
            "request ID|cross-request",
        ):
            validate(id_attack)

    def test_sender_native_balance_override_is_one_fixed_exported_word(self):
        self.assertEqual(
            route_cost_evidence.ETHEREUM_V2_SIMULATION_SENDER_NATIVE_BALANCE_WEI,
            10 ** 18,
        )
        transcript = {
            "chain_evidence_sha256": "1" * 64,
            "market_evidence_sha256": "2" * 64,
        }
        common = {
            "transcript": transcript,
            "adapter": adapter(),
            "market": {"pair_address": POOL},
            "chain": {"block_header_result": {"number": "0x64"}},
            "completed_stage": "transfer_tax",
            "status": "observed",
            "reason_code": None,
        }
        for request_field, override_index in (
            ("estimate_gas_request", 2),
            ("simulation_request", 2),
        ):
            forged = copy.deepcopy(_complete_raw_transcript())
            state_overrides = forged[request_field]["params"][override_index]
            if request_field == "simulation_request":
                state_overrides = state_overrides["stateOverrides"]
            state_overrides[SENDER]["balance"] = _word(10 ** 18 + 1)
            with self.subTest(request_field=request_field):
                with self.assertRaises(route_cost_evidence.RouteCostEvidenceError):
                    route_cost_evidence._validate_raw_transcript(
                        forged, **common
                    )


class RouteCostCoverageOutcomeTests(unittest.TestCase):
    @staticmethod
    def _replay(manifest, universe, retained=None, **expected_overrides):
        expected = {
            "expected_run_id": RUN_ID,
            "expected_route_cohort_id": COHORT_ID,
            "expected_phase": PHASE,
            "expected_candidate_source_generation": GENERATION,
            "expected_route_universe_sha256": physical_sha(universe),
        }
        expected.update(expected_overrides)
        return route_cost_evidence.replay_route_cost_coverage_outcomes(
            manifest,
            universe=universe,
            retained_typed_pool_state_members=retained,
            **expected,
        )

    @staticmethod
    def _partial_scope_fixture():
        markets = []
        routes = []
        for index in range(9):
            pool = "0x{:040x}".format(index + 100)
            market_id = "dex:eth:uniswap_v2:{}:AAA".format(pool)
            leg = copy.deepcopy(universe_for()["selected_legs"][0])
            leg["market_id"] = market_id
            leg["selection_rank"] = index + 1
            leg["selection_inputs"]["dex_24h_usd"] = str(1000 - index)
            leg["selection_inputs"]["dex_tvl_usd"] = str(2000 - index)
            markets.append(leg)
            routes.append({
                "route_id": "route-scope-{:02d}".format(index),
                "token_symbol": "AAA",
                "buy_market_id": market_id,
                "sell_market_id": "cex:x:AAA/USDT",
                "route_mode": "prepositioned_inventory",
                "route_class": "candidate",
                "settlement_reason": None,
                "requested_notionals_usd": list(NOTIONALS),
                "candidate_source_generation": GENERATION,
                "buy_reference_volume_usd": str(1000 - index),
                "sell_reference_volume_usd": "3000",
                "route_volume_usd": str(1000 - index),
                "route_volume_basis": "minimum_leg_source_horizon_usd",
            })

        selected_supported = markets[0]["market_id"]
        unselected_supported = markets[-1]["market_id"]
        partial_route_id = "route-scope-partial"
        routes.append({
            "route_id": partial_route_id,
            "token_symbol": "AAA",
            "buy_market_id": selected_supported,
            "sell_market_id": unselected_supported,
            "route_mode": "atomic_onchain",
            "route_class": "candidate",
            "settlement_reason": None,
            "requested_notionals_usd": list(NOTIONALS),
            "candidate_source_generation": GENERATION,
            "buy_reference_volume_usd": "1000",
            "sell_reference_volume_usd": "992",
            "route_volume_usd": "992",
            "route_volume_basis": "minimum_leg_source_horizon_usd",
        })
        routes.sort(key=lambda row: row["route_id"])
        universe = universe_for(markets=markets, routes=routes)
        universe_sha = physical_sha(universe)
        registry = {
            "schema": "route_cost_adapter_registry/v1",
            "registry_version": "scope-gap-v1",
            "adapters": [adapter(pairs=[
                pair_descriptor(markets[0]["market_id"].split(":")[3]),
                pair_descriptor(markets[-1]["market_id"].split(":")[3]),
            ])],
        }
        retained = {
            selected_supported: retained_v2_pool_state(
                market_id=selected_supported,
                pool_address=markets[0]["market_id"].split(":")[3],
            )
        }
        manifest = (
            route_cost_evidence.build_trace_profile_missing_route_cost_evidence_manifest(
                universe=universe,
                run_id=RUN_ID,
                route_cohort_id=COHORT_ID,
                phase=PHASE,
                candidate_source_generation=GENERATION,
                route_universe_sha256=universe_sha,
                evaluated_at=EVALUATED_AT,
                adapter_registry=registry,
                connector_key_registry=connector_registry(),
                retained_typed_pool_state_members=retained,
            )
        )
        return manifest, universe, retained, partial_route_id, unselected_supported

    def test_replays_selected_binding_status_for_every_notional(self):
        manifest, retained = supported_core_manifest()
        universe = universe_for()

        outcomes = self._replay(
            manifest,
            universe,
            {MARKET_ID: retained},
        )

        self.assertEqual(len(outcomes), 5)
        self.assertEqual(
            [row["requested_notional_usd"] for row in outcomes],
            [str(value) for value in NOTIONALS],
        )
        self.assertTrue(all(row["status"] == "unavailable" for row in outcomes))
        self.assertTrue(
            all(row["reason_code"] == "transcript_unavailable" for row in outcomes)
        )
        self.assertTrue(all(row["coverage_kind"] == "binding" for row in outcomes))
        self.assertTrue(all(row["uncovered_dex_market_ids"] == [] for row in outcomes))
        self.assertTrue(
            all(row["route_cost_evidence_sha256"] == physical_sha(manifest) for row in outcomes)
        )
        expected_binding_hashes = [
            typed_sha(b"route-cost-evidence-binding/v1\n", row)
            for row in manifest["bindings"]
        ]
        self.assertEqual(
            [row["scoped_binding_sha256"] for row in outcomes],
            expected_binding_hashes,
        )

    def test_partial_binding_cannot_masquerade_as_complete_route_coverage(self):
        (
            manifest,
            universe,
            retained,
            partial_route_id,
            unselected_supported,
        ) = self._partial_scope_fixture()

        outcomes = self._replay(
            manifest,
            universe,
            retained,
        )
        partial = [row for row in outcomes if row["route_id"] == partial_route_id]

        self.assertEqual(len(partial), 5)
        self.assertTrue(all(row["status"] == "unavailable" for row in partial))
        self.assertTrue(
            all(row["reason_code"] == "not_collected_by_cost_scope" for row in partial)
        )
        self.assertTrue(
            all(row["coverage_kind"] == "terminal_scope_replay" for row in partial)
        )
        self.assertTrue(
            all(row["uncovered_dex_market_ids"] == [unselected_supported] for row in partial)
        )
        # The legacy v1 sidecar does contain a one-sided scoped binding here;
        # the full-route outcome must retain its hash without inheriting status.
        self.assertTrue(all(row["scoped_binding_sha256"] is not None for row in partial))

    def test_structurally_unsupported_dex_route_is_terminal_not_absent(self):
        manifest = unsupported_manifest()
        universe = universe_for()

        outcomes = self._replay(
            manifest,
            universe,
        )

        self.assertEqual(len(outcomes), 5)
        self.assertTrue(all(row["status"] == "unavailable" for row in outcomes))
        self.assertTrue(
            all(row["reason_code"] == "strict_cost_adapter_unsupported" for row in outcomes)
        )
        self.assertTrue(all(row["uncovered_dex_market_ids"] == [MARKET_ID] for row in outcomes))

    def test_research_only_route_marks_selected_dex_leg_binding_uncovered(self):
        candidate = copy.deepcopy(universe_for()["routes"][0])
        route = copy.deepcopy(candidate)
        route.update({
            "route_id": "route-research-only",
            "route_mode": "research_only",
            "route_class": "research_only",
            "settlement_reason": "unsupported_cross_chain_settlement",
        })
        universe = universe_for(routes=[candidate, route])
        retained = {MARKET_ID: retained_v2_pool_state()}
        manifest = (
            route_cost_evidence.build_trace_profile_missing_route_cost_evidence_manifest(
                universe=universe,
                run_id=RUN_ID,
                route_cohort_id=COHORT_ID,
                phase=PHASE,
                candidate_source_generation=GENERATION,
                route_universe_sha256=physical_sha(universe),
                evaluated_at=EVALUATED_AT,
                adapter_registry=adapter_registry(supported=True),
                connector_key_registry=connector_registry(),
                retained_typed_pool_state_members=retained,
            )
        )

        outcomes = [
            row for row in self._replay(manifest, universe, retained)
            if row["route_id"] == route["route_id"]
        ]

        self.assertEqual(len(outcomes), 5)
        self.assertTrue(all(row["status"] == "unavailable" for row in outcomes))
        self.assertTrue(
            all(row["reason_code"] == "not_collected_by_cost_scope" for row in outcomes)
        )
        self.assertTrue(
            all(row["covered_dex_market_ids"] == [MARKET_ID] for row in outcomes)
        )
        self.assertTrue(
            all(row["uncovered_dex_market_ids"] == [MARKET_ID] for row in outcomes)
        )
        self.assertTrue(all(row["scoped_binding_sha256"] is None for row in outcomes))

    def test_cex_only_route_is_not_applicable_for_all_five_notionals(self):
        candidate = copy.deepcopy(universe_for()["routes"][0])
        route = copy.deepcopy(candidate)
        route.update({
            "route_id": "route-cex-only",
            "buy_market_id": "cex:x:AAA/USDT",
            "sell_market_id": "cex:y:AAA/USDT",
        })
        universe = universe_for(routes=[candidate, route])
        manifest = unsupported_manifest(universe)

        outcomes = [
            row for row in self._replay(manifest, universe)
            if row["route_id"] == route["route_id"]
        ]

        self.assertEqual(len(outcomes), 5)
        self.assertEqual(
            [row["requested_notional_usd"] for row in outcomes],
            [str(value) for value in NOTIONALS],
        )
        self.assertTrue(all(row["status"] == "not_applicable" for row in outcomes))
        self.assertTrue(all(row["reason_code"] is None for row in outcomes))
        self.assertTrue(all(row["coverage_kind"] == "not_applicable" for row in outcomes))

    def test_external_run_anchor_is_not_taken_from_sidecar(self):
        manifest = unsupported_manifest()
        universe = universe_for()

        with self.assertRaisesRegex(
            route_cost_evidence.RouteCostEvidenceError,
            "route-cost outer lineage differs",
        ):
            self._replay(
                manifest,
                universe,
                expected_run_id="shadow-run-cost-other",
            )

    def test_top_level_notional_denominator_tampering_is_rejected(self):
        universe = universe_for()
        universe["requested_notionals_usd"][-1] = 99999
        manifest = unsupported_manifest(universe)

        with self.assertRaisesRegex(
            route_cost_evidence.RouteCostEvidenceError,
            "route-cost coverage top-level notional denominator differs",
        ):
            self._replay(manifest, universe)

    def test_route_notional_grid_type_tampering_is_rejected(self):
        universe = universe_for()
        universe["routes"][0]["requested_notionals_usd"] = [
            str(value) for value in NOTIONALS
        ]
        manifest = unsupported_manifest(universe)

        with self.assertRaisesRegex(
            route_cost_evidence.RouteCostEvidenceError,
            "route-cost coverage route notional denominator differs",
        ):
            self._replay(manifest, universe)


class V2PrimitiveTests(unittest.TestCase):
    @staticmethod
    def _target_and_state(raw_quantity="100"):
        state = {
            "token0_address": TOKEN_A,
            "token1_address": TOKEN_B,
            "token0_decimals": "0",
            "token1_decimals": "0",
            "reserve0_raw": "1000",
            "reserve1_raw": "552",
            "fee_numerator": "9970",
            "fee_denominator": "10000",
        }
        target_value = {
            "schema": "route_cost_simulation_target/v1",
            "token_address": TOKEN_A,
            "unit_decimals": "0",
            "raw_quantity": raw_quantity,
            "lattice_raw": "1",
        }
        target = {
            "simulation_target_token_address": TOKEN_A,
            "simulation_target_unit_decimals": "0",
            "simulation_target_raw_quantity": raw_quantity,
            "simulation_target_lattice_raw": "1",
            "simulation_target_sha256": typed_sha(
                b"route-cost-simulation-target/v1\n", target_value
            ),
        }
        return target, state

    def test_global_simulation_target_handles_base_and_quote_side_exactly(self):
        other_c = "0x" + "4" * 40
        market_c = "dex:eth:uniswap_v2:{}:AAA".format("0x" + "5" * 40)
        leg_base = copy.deepcopy(universe_for()["selected_legs"][0])
        leg_base["collector_context"]["quote_token_price_usd"] = "10"
        leg_quote = copy.deepcopy(leg_base)
        leg_quote["market_id"] = market_c
        leg_quote["collector_context"] = context(other_c, TOKEN_A)
        leg_quote["collector_context"]["base_token_price_usd"] = "20"
        leg_quote["collector_context"]["quote_token_price_usd"] = "1"
        leg_quote["target_token_side"] = "quote"
        selected = {
            MARKET_ID: {"structural_support_status": "supported"},
            market_c: {"structural_support_status": "supported"},
        }
        states = {
            MARKET_ID: {
                "token0_address": TOKEN_A,
                "token1_address": TOKEN_B,
                "token0_decimals": "0",
                "token1_decimals": "0",
                "reserve0_raw": "101",
                "reserve1_raw": "101",
                "observed_at": "2026-08-01T12:00:00Z",
            },
            market_c: {
                "token0_address": other_c,
                "token1_address": TOKEN_A,
                "token0_decimals": "0",
                "token1_decimals": "0",
                "reserve0_raw": "202",
                "reserve1_raw": "101",
                "observed_at": "2026-08-01T12:00:00Z",
            },
        }
        targets = route_cost_evidence.build_simulation_targets(
            [leg_base, leg_quote], selected, states
        )
        expected = {
            "simulation_target_token_address": TOKEN_A,
            "simulation_target_unit_decimals": "0",
            "simulation_target_raw_quantity": "25",
            "simulation_target_lattice_raw": "1",
        }
        expected["simulation_target_sha256"] = typed_sha(
            b"route-cost-simulation-target/v1\n",
            {
                "schema": "route_cost_simulation_target/v1",
                "token_address": TOKEN_A,
                "unit_decimals": "0",
                "raw_quantity": "25",
                "lattice_raw": "1",
            },
        )
        self.assertEqual(targets[(MARKET_ID, "1000")], expected)
        self.assertEqual(targets[(market_c, "1000")], expected)
        self.assertEqual(
            targets[(MARKET_ID, "100000")][
                "simulation_target_raw_quantity"
            ],
            "2500",
        )

        unavailable = copy.deepcopy(leg_quote)
        unavailable["collector_context"]["status"] = "failed"
        unavailable["collector_context"]["base_token_id"] = None
        unavailable["collector_context"]["quote_token_id"] = None
        unavailable["collector_context"]["base_token_price_usd"] = None
        unavailable["collector_context"]["quote_token_price_usd"] = None
        unavailable["target_token_side"] = None
        self.assertEqual(
            route_cost_evidence.build_simulation_targets(
                [leg_base, unavailable], selected, states
            ),
            {},
        )
        within_two_hours = copy.deepcopy(states)
        within_two_hours[market_c]["observed_at"] = "2026-08-01T10:00:00Z"
        self.assertEqual(
            route_cost_evidence.build_simulation_targets(
                [leg_base, leg_quote], selected, within_two_hours
            ),
            targets,
        )
        over_route_age_but_valid = copy.deepcopy(states)
        over_route_age_but_valid[market_c]["observed_at"] = (
            "2026-08-01T11:57:59Z"
        )
        self.assertEqual(
            route_cost_evidence.build_simulation_targets(
                [leg_base, leg_quote], selected, over_route_age_but_valid
            ),
            targets,
        )
        stale_states = copy.deepcopy(states)
        stale_states[market_c]["observed_at"] = "2026-08-01T09:59:59Z"
        self.assertEqual(
            route_cost_evidence.build_simulation_targets(
                [leg_base, leg_quote], selected, stale_states
            ),
            {},
        )

    def test_missing_state_poisoning_is_isolated_per_target_token(self):
        target_b = "0x" + "6" * 40
        other_b = "0x" + "7" * 40
        market_b = "dex:eth:uniswap_v2:{}:BBB".format("0x" + "8" * 40)
        leg_a = copy.deepcopy(universe_for()["selected_legs"][0])
        leg_b = copy.deepcopy(leg_a)
        leg_b.update({
            "market_id": market_b,
            "token_symbol": "BBB",
            "target_token_address": target_b,
            "target_token_side": "base",
        })
        leg_b["collector_context"] = context(target_b, other_b)
        selected = {
            MARKET_ID: {"structural_support_status": "supported"},
            market_b: {"structural_support_status": "supported"},
        }
        state_a = {
            "token0_address": TOKEN_A,
            "token1_address": TOKEN_B,
            "token0_decimals": "18",
            "token1_decimals": "18",
            "reserve0_raw": str(100 * 10 ** 18),
            "reserve1_raw": str(200 * 10 ** 18),
            "observed_at": "2026-08-01T12:00:00Z",
        }
        targets = route_cost_evidence.build_simulation_targets(
            [leg_a, leg_b], selected, {MARKET_ID: state_a}
        )
        self.assertEqual(
            set(targets),
            {(MARKET_ID, str(notional)) for notional in NOTIONALS},
        )

    def test_quote_side_target_replays_exact_buy_and_sell_amounts(self):
        target = {
            "simulation_target_token_address": TOKEN_B,
            "simulation_target_unit_decimals": "0",
            "simulation_target_raw_quantity": "100",
            "simulation_target_lattice_raw": "1",
            "simulation_target_sha256": "a" * 64,
        }
        state = {
            "token0_address": TOKEN_A,
            "token1_address": TOKEN_B,
            "token0_decimals": "0",
            "token1_decimals": "0",
            "reserve0_raw": "552",
            "reserve1_raw": "1000",
            "fee_numerator": "9970",
            "fee_denominator": "10000",
        }
        chain = {"block_header_result": {"timestamp": hex(12345 - 300)}}
        market = {"pair_token0": TOKEN_A, "pair_token1": TOKEN_B}
        for direction, amount_in, amount_out, path in (
            ("sell", 100, 50, (TOKEN_B, TOKEN_A)),
            ("buy", 62, 100, (TOKEN_A, TOKEN_B)),
        ):
            raw = _complete_raw_transcript()
            raw["calldata_hex"] = route_cost_evidence.build_v2_swap_calldata(
                direction=direction,
                quoted_amount_in_raw=amount_in,
                quoted_amount_out_raw=amount_out,
                submission_loss_bound_bps=0,
                path_token_in=path[0],
                path_token_out=path[1],
                recipient=SENDER,
                deadline=12345,
            )
            call = {
                "schema": "route_cost_call_evidence/v1",
                "selector": (
                    "0x38ed1739" if direction == "sell" else "0x8803dbee"
                ),
                "path_token_in": path[0],
                "path_token_out": path[1],
                "recipient_policy": "same_as_registry_sender/v1",
                "deadline": "0x3039",
                "amount_in_raw": str(amount_in),
                "amount_out_raw": str(amount_out),
                "calldata_sha256": hashlib.sha256(
                    bytes.fromhex(raw["calldata_hex"][2:])
                ).hexdigest(),
                "sender_policy": "registry_fixed_state_override_sender/v1",
                "allowance_basis": "exact_amount_state_override/v1",
                "submission_loss_bound_bps": "0",
            }
            route_cost_evidence._validate_call_evidence(
                call,
                transcript={"direction": direction},
                raw=raw,
                adapter=adapter(),
                chain=chain,
                market=market,
                market_tokens=(TOKEN_B, TOKEN_A),
                simulation_target=target,
                retained_pool_state=state,
            )
            forged = copy.deepcopy(call)
            forged["amount_in_raw"] = str(amount_in + 1)
            with self.assertRaisesRegex(
                route_cost_evidence.RouteCostEvidenceError,
                "retained target quote",
            ):
                route_cost_evidence._validate_call_evidence(
                    forged,
                    transcript={"direction": direction},
                    raw=raw,
                    adapter=adapter(),
                    chain=chain,
                    market=market,
                    market_tokens=(TOKEN_B, TOKEN_A),
                    simulation_target=target,
                    retained_pool_state=state,
                )

    def test_eip1559_next_base_fee_and_decimal18_gas_cost_are_exact(self):
        self.assertEqual(
            route_cost_evidence.next_base_fee_wei(
                base_fee_per_gas=100, gas_used=750, gas_limit=1000
            ),
            106,
        )
        self.assertEqual(
            route_cost_evidence.next_base_fee_wei(
                base_fee_per_gas=100, gas_used=500, gas_limit=1000
            ),
            100,
        )
        self.assertEqual(
            route_cost_evidence.next_base_fee_wei(
                base_fee_per_gas=100, gas_used=250, gas_limit=1000
            ),
            94,
        )
        self.assertEqual(route_cost_evidence.max_fee_per_gas_wei(106, 3), 215)
        self.assertEqual(
            route_cost_evidence.network_gas_usd(
                gas_units=21000,
                max_fee_per_gas_wei_value=100_000_000_000,
                native_price_usd="3000",
            ),
            "6.3",
        )

    def test_v2_buy_and_sell_calldata_round_trip_exact_bounds_and_path(self):
        buy = route_cost_evidence.build_v2_swap_calldata(
            direction="buy",
            quoted_amount_in_raw=10001,
            quoted_amount_out_raw=5000,
            submission_loss_bound_bps=100,
            path_token_in=TOKEN_A,
            path_token_out=TOKEN_B,
            recipient=SENDER,
            deadline=12345,
        )
        sell = route_cost_evidence.build_v2_swap_calldata(
            direction="sell",
            quoted_amount_in_raw=5000,
            quoted_amount_out_raw=10001,
            submission_loss_bound_bps=100,
            path_token_in=TOKEN_B,
            path_token_out=TOKEN_A,
            recipient=SENDER,
            deadline=12345,
        )
        self.assertTrue(buy.startswith("0x8803dbee"))
        self.assertTrue(sell.startswith("0x38ed1739"))
        self.assertEqual(
            route_cost_evidence.decode_v2_swap_calldata(buy),
            {
                "selector": "0x8803dbee",
                "direction": "buy",
                "amount_out_raw": 5000,
                "amount_in_max_raw": 10102,
                "path": [TOKEN_A, TOKEN_B],
                "recipient": SENDER,
                "deadline": 12345,
            },
        )
        self.assertEqual(
            route_cost_evidence.decode_v2_swap_calldata(sell),
            {
                "selector": "0x38ed1739",
                "direction": "sell",
                "amount_in_raw": 5000,
                "amount_out_min_raw": 9900,
                "path": [TOKEN_B, TOKEN_A],
                "recipient": SENDER,
                "deadline": 12345,
            },
        )

    def test_keccak_and_mapping_slots_use_ethereum_not_nist_sha3(self):
        self.assertEqual(
            route_cost_evidence.keccak256(b"").hex(),
            "c5d2460186f7233c927e7db2dcc703c0e500b653ca82273b7bfad8045d85a470",
        )
        balance = route_cost_evidence.solidity_balance_storage_key(
            "0x" + "1" * 40, 3
        )
        allowance = route_cost_evidence.solidity_allowance_storage_key(
            "0x" + "1" * 40, "0x" + "2" * 40, 4
        )
        self.assertRegex(balance, r"^0x[0-9a-f]{64}$")
        self.assertRegex(allowance, r"^0x[0-9a-f]{64}$")
        self.assertNotEqual(balance, allowance)

    def test_call_deadline_is_exactly_fixed_block_timestamp_plus_300(self):
        target, state = self._target_and_state()
        raw = _complete_raw_transcript()
        call = {
            "schema": "route_cost_call_evidence/v1",
            "selector": "0x38ed1739",
            "path_token_in": TOKEN_A,
            "path_token_out": TOKEN_B,
            "recipient_policy": "same_as_registry_sender/v1",
            "deadline": "0x3039",
            "amount_in_raw": "100",
            "amount_out_raw": "50",
            "calldata_sha256": hashlib.sha256(
                bytes.fromhex(raw["calldata_hex"][2:])
            ).hexdigest(),
            "sender_policy": "registry_fixed_state_override_sender/v1",
            "allowance_basis": "exact_amount_state_override/v1",
            "submission_loss_bound_bps": "0",
        }
        transcript = {"direction": "sell"}
        chain = {"block_header_result": {"timestamp": hex(12345 - 300)}}
        route_cost_evidence._validate_call_evidence(
            call,
            transcript=transcript,
            raw=raw,
            adapter=adapter(),
            chain=chain,
            market={"pair_token0": TOKEN_A, "pair_token1": TOKEN_B},
            market_tokens=(TOKEN_A, TOKEN_B),
            simulation_target=target,
            retained_pool_state=state,
        )
        for offset in (-1, 1):
            forged = copy.deepcopy(call)
            forged_deadline = 12345 + offset
            forged["deadline"] = hex(forged_deadline)
            forged_raw = copy.deepcopy(raw)
            forged_raw["calldata_hex"] = route_cost_evidence.build_v2_swap_calldata(
                direction="sell",
                quoted_amount_in_raw=100,
                quoted_amount_out_raw=50,
                submission_loss_bound_bps=0,
                path_token_in=TOKEN_A,
                path_token_out=TOKEN_B,
                recipient=SENDER,
                deadline=forged_deadline,
            )
            forged["calldata_sha256"] = hashlib.sha256(
                bytes.fromhex(forged_raw["calldata_hex"][2:])
            ).hexdigest()
            with self.assertRaises(route_cost_evidence.RouteCostEvidenceError):
                route_cost_evidence._validate_call_evidence(
                    forged,
                    transcript=transcript,
                    raw=forged_raw,
                    adapter=adapter(),
                    chain=chain,
                    market={"pair_token0": TOKEN_A, "pair_token1": TOKEN_B},
                    market_tokens=(TOKEN_A, TOKEN_B),
                    simulation_target=target,
                    retained_pool_state=state,
                )

    def test_call_path_is_exact_selected_pair_and_direction(self):
        target, state = self._target_and_state()
        wrong_a = "0x" + "3" * 40
        wrong_b = "0x" + "4" * 40
        raw = _complete_raw_transcript()
        raw["calldata_hex"] = route_cost_evidence.build_v2_swap_calldata(
            direction="sell",
            quoted_amount_in_raw=100,
            quoted_amount_out_raw=50,
            submission_loss_bound_bps=0,
            path_token_in=wrong_a,
            path_token_out=wrong_b,
            recipient=SENDER,
            deadline=12345,
        )
        call = {
            "schema": "route_cost_call_evidence/v1",
            "selector": "0x38ed1739",
            "path_token_in": wrong_a,
            "path_token_out": wrong_b,
            "recipient_policy": "same_as_registry_sender/v1",
            "deadline": "0x3039",
            "amount_in_raw": "100",
            "amount_out_raw": "50",
            "calldata_sha256": hashlib.sha256(
                bytes.fromhex(raw["calldata_hex"][2:])
            ).hexdigest(),
            "sender_policy": "registry_fixed_state_override_sender/v1",
            "allowance_basis": "exact_amount_state_override/v1",
            "submission_loss_bound_bps": "0",
        }
        cases = ((raw, call),)
        reversed_raw = _complete_raw_transcript()
        reversed_raw["calldata_hex"] = route_cost_evidence.build_v2_swap_calldata(
            direction="sell",
            quoted_amount_in_raw=100,
            quoted_amount_out_raw=50,
            submission_loss_bound_bps=0,
            path_token_in=TOKEN_B,
            path_token_out=TOKEN_A,
            recipient=SENDER,
            deadline=12345,
        )
        reversed_call = dict(
            call,
            path_token_in=TOKEN_B,
            path_token_out=TOKEN_A,
            calldata_sha256=hashlib.sha256(
                bytes.fromhex(reversed_raw["calldata_hex"][2:])
            ).hexdigest(),
        )
        cases += ((reversed_raw, reversed_call),)
        for forged_raw, forged_call in cases:
            with self.subTest(path=(forged_call["path_token_in"], forged_call["path_token_out"])):
                with self.assertRaisesRegex(
                    route_cost_evidence.RouteCostEvidenceError, "pair|path|direction"
                ):
                    route_cost_evidence._validate_call_evidence(
                        forged_call,
                        transcript={"direction": "sell"},
                        raw=forged_raw,
                        adapter=adapter(),
                        chain={"block_header_result": {"timestamp": hex(12345 - 300)}},
                        market={"pair_token0": TOKEN_A, "pair_token1": TOKEN_B},
                        market_tokens=(TOKEN_A, TOKEN_B),
                        simulation_target=target,
                        retained_pool_state=state,
                    )


if __name__ == "__main__":
    unittest.main()
