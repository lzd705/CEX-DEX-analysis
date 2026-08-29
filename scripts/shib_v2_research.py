"""Pure contracts for SHIB V2/V2 historical research evidence."""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timezone
from decimal import Decimal
from fractions import Fraction
from typing import Dict, List, Sequence

from scripts.route_quantity import (
    CommonTarget,
    MarketRules,
    QuantityQuote,
    V2PoolState,
    quote_v2_pool_quantity,
    validate_v2_quantity_quote_against_state,
)


REGISTRY_SCHEMA = "shib_v2_research_registry/v1"
EVIDENCE_SCHEMA = "shib_v2_research_evidence/v1"
SNAPSHOT_SCHEMA = "shib_v2_research_snapshot/v1"
_ADDRESS = re.compile(r"0x[0-9a-f]{40}$")
_SHA256 = re.compile(r"[0-9a-f]{64}$")
_GIT_SHA = re.compile(r"[0-9a-f]{40}$")
_HASH32 = re.compile(r"0x[0-9a-f]{64}$")
_V2_STATE_ID = re.compile(r"dex-v2-quantity:[0-9a-f]{64}$")
_CALL_ID = re.compile(r"call:[0-9a-f]{64}$")
_HEX = re.compile(r"0x(?:[0-9a-f]{2})*$")
_MAX_CALLDATA_BYTES = 512
_MAX_RESULT_BYTES = 65536
_MAX_ABI_STRING_BYTES = 4096
_FUNCTION_SELECTORS = {
    "getPair(address,address)": "e6a43905",
    "factory()": "c45a0155",
    "WETH()": "ad5c4648",
    "token0()": "0dfe1681",
    "token1()": "d21220a7",
    "getReserves()": "0902f1ac",
    "decimals()": "313ce567",
    "balanceOf(address)": "70a08231",
    "totalFee()": "1df4ccfc",
    "alpha()": "db1d0fd5",
    "beta()": "9faa3c91",
    "description()": "7284e416",
    "latestRoundData()": "feaf968c",
}
_FORBIDDEN_KEY_NORMALIZED = {
    "apikey",
    "authorization",
    "endpoint",
    "headers",
    "privatekey",
    "privatepath",
    "providererror",
    "rawpayload",
    "rawresponse",
    "rawrpc",
    "rpcurl",
    "secret",
    "url",
}
_UNSAFE_TEXT = re.compile(
    r"(?:https?|wss?)://|(?:^|[^a-z0-9])(?:sk|rk|pk)[_-][a-z0-9_-]{8,}|"
    r"(?:^|[^a-z0-9])gh[pous]_[a-z0-9]{16,}|github_pat_[a-z0-9_]{16,}|"
    r"(?:^|[^a-z0-9])(?:secret|password|credential)[ _:=.-]+[a-z0-9_-]{6,}|"
    r"(?:^|[\s\"'])/(?:root|etc|Users|home|private)(?:/|$)|"
    r"[A-Za-z]:[\\/](?:root|etc|Users|home|private)(?:[\\/]|$)",
    re.IGNORECASE,
)
_AUTHORITY_TRUST_ANCHOR = (
    (
        (
            "SHIB",
            "0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce",
            18,
            4852,
            "5c813da8be193a1a33a7533edc758e3ad29f1fa1730cbf2d8c9fc8a7f31c78f3",
        ),
        (
            "WETH",
            "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
            18,
            3124,
            "5566bf50796faf93c9b6f6adacd3b32c70bfe16b48ffc59db6cd144cbdc89739",
        ),
    ),
    (
        (
            "uniswap_v2",
            (
                (
                    "0x5c69bee701ef814a2b6a3edd4b1652cb9cc5aa6f",
                    13859,
                    "3abc53f12a9cb8ae37ebfada9efc261c1ab4c2759d161e341a49bf67df3f8321",
                ),
                (
                    "0x7a250d5630b4cf539739df2c5dacb4c659f2488d",
                    21943,
                    "ccef50da4af021b09ada39d78db5d281fffff81a57969c7028bccc1f50d37854",
                ),
                (
                    "0x811beed0119b4afce20d2583eb608c6f7af1954f",
                    11293,
                    "8b5db55fa9ab3b9527508d4abe0b39eb588bf310270c8e04b3f38214e8ba63b4",
                ),
            ),
            (("kind", "runtime_code_bound"),),
        ),
        (
            "shibaswap_v1",
            (
                (
                    "0x115934131916c8b277dd010ee02de363c09d037c",
                    15527,
                    "bccd00fecc8d072c7635ef40bd5b7721057975123aa8639d62a37f90f6a45b53",
                ),
                (
                    "0x03f7724180aa6b939894b5ca4314783b0b36b329",
                    18469,
                    "bb5f84ee54eacd3a273b2a3942ad904f8194a999f32394682cda2080b14b0423",
                ),
                (
                    "0xcf6daab95c476106eca715d48de4b13287ffdeaa",
                    10654,
                    "83589060885cd6b139ce4b4ed723653d124a00b50c0fa203dbd5a425cb272bc7",
                ),
            ),
            (
                ("alpha", 1),
                ("beta", 3),
                ("kind", "pair_native_parameters"),
                ("native_fee_denominator", 1000),
                ("target", "pair"),
                ("total_fee", 3),
            ),
        ),
    ),
    (
        "0x5f4ec3df9cbd43714fe2740f5e3616155c5b8419",
        9571,
        "ed698309290de3517c7201fcad9a9dbd4b8cde4a72c9add23129201f299c6f2b",
        8,
        3600,
    ),
)
_FIXED_POOLS = (
    ("uniswap_v2", "0x811beed0119b4afce20d2583eb608c6f7af1954f"),
    ("shibaswap_v1", "0xcf6daab95c476106eca715d48de4b13287ffdeaa"),
)
_FIXED_ROUTES = (
    (
        "shib-v2v2:eth:uniswap_v2:"
        "0x811beed0119b4afce20d2583eb608c6f7af1954f:to:shibaswap_v1:"
        "0xcf6daab95c476106eca715d48de4b13287ffdeaa",
        _FIXED_POOLS[0],
        _FIXED_POOLS[1],
    ),
    (
        "shib-v2v2:eth:shibaswap_v1:"
        "0xcf6daab95c476106eca715d48de4b13287ffdeaa:to:uniswap_v2:"
        "0x811beed0119b4afce20d2583eb608c6f7af1954f",
        _FIXED_POOLS[1],
        _FIXED_POOLS[0],
    ),
)
_REQUESTED_NOTIONALS = ("1000", "5000", "10000", "50000", "100000")
_COMPLETE_QUOTE_REASON = "fixed_block_fee_proof_not_authenticated"
_UNAVAILABLE_QUOTE_REASONS = frozenset({
    "pool_state_binding_mismatch",
    "pool_state_not_current",
    "market_rules_binding_mismatch",
    "market_rules_not_current",
    "pool_state_market_mismatch",
    "target_asset_mismatch",
    "pool_state_token_address_mismatch",
    "pool_state_token_decimals_mismatch",
    "target_base_unit_misaligned",
    "target_lot_misaligned",
    "minimum_base_quantity_not_met",
    "pool_output_below_one_raw",
    "pool_reserve_insufficient",
    "minimum_notional_not_met",
})
MISSING_COST_FIELDS = (
    "network_gas_usd",
    "router_or_integrator_fee_usd",
    "token_transfer_tax_usd",
    "mev_cost_usd",
    "atomic_execution_cost_usd",
    "net_edge_usd",
    "net_edge_bps",
)
LIMITATIONS = (
    "network_gas_not_evaluated",
    "router_fee_not_evaluated",
    "token_transfer_tax_not_evaluated",
    "mev_not_evaluated",
    "atomic_route_simulation_unavailable",
)
_SCENARIO_FIELDS = (
    "route_id", "buy_dex", "buy_pair_address", "sell_dex",
    "sell_pair_address", "requested_notional_usd",
    "buy_pool_reference_shib_usd", "sell_pool_reference_shib_usd",
    "common_target_reference_shib_usd", "common_shib_raw",
    "buy_weth_raw", "sell_weth_raw", "gross_edge_weth_raw",
    "buy_cost_usd", "sell_proceeds_usd", "gross_edge_usd",
    "gross_edge_bps", "buy_pool_state_id", "sell_pool_state_id",
    "buy_quote_status", "sell_quote_status", "buy_quote_reason",
    "sell_quote_reason", "classification", "reason_codes",
    "strict_eligible", "executable", "limitations",
) + MISSING_COST_FIELDS


class ResearchContractError(ValueError):
    """The supplied value crosses a research-contract boundary unsafely."""


def canonical_json_bytes(value: object) -> bytes:
    """Return compact, sorted UTF-8 JSON bytes or fail closed."""
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError) as error:
        raise ResearchContractError("value is not canonical JSON") from error


def _exact_fields(value: object, fields: Sequence[str], label: str) -> dict:
    if not isinstance(value, dict) or set(value) != set(fields):
        raise ResearchContractError("{} schema is invalid".format(label))
    return value


def _address(value: object, label: str) -> str:
    if not isinstance(value, str) or _ADDRESS.fullmatch(value) is None:
        raise ResearchContractError("{} is invalid".format(label))
    return value


def _sha256(value: object, label: str) -> str:
    if not isinstance(value, str) or _SHA256.fullmatch(value) is None:
        raise ResearchContractError("{} is invalid".format(label))
    return value


def _positive_int(value: object, label: str) -> int:
    if type(value) is not int or value <= 0:
        raise ResearchContractError("{} is invalid".format(label))
    return value


def _runtime_code(value: object, label: str) -> dict:
    record = _exact_fields(
        value,
        ("address", "runtime_code_size_bytes", "runtime_code_sha256"),
        label,
    )
    _address(record["address"], label + " address")
    _positive_int(record["runtime_code_size_bytes"], label + " code size")
    _sha256(record["runtime_code_sha256"], label + " code hash")
    return record


def _token(value: object, symbol: str) -> dict:
    record = _exact_fields(
        value,
        ("address", "decimals", "runtime_code_size_bytes", "runtime_code_sha256"),
        "{} token".format(symbol),
    )
    _address(record["address"], symbol + " address")
    decimals = _positive_int(record["decimals"], symbol + " decimals")
    if decimals > 255:
        raise ResearchContractError(symbol + " decimals is invalid")
    _positive_int(record["runtime_code_size_bytes"], symbol + " code size")
    _sha256(record["runtime_code_sha256"], symbol + " code hash")
    return record


def _fee_model(value: object, dex: str) -> dict:
    record = _exact_fields(
        value,
        ("formula", "fee_bps", "fee_numerator", "fee_denominator", "evidence"),
        dex + " fee model",
    )
    if record["formula"] != (
        "amount_in_with_fee=amount_in*fee_numerator;"
        "denominator=reserve_in*fee_denominator+amount_in_with_fee"
    ):
        raise ResearchContractError(dex + " fee formula is invalid")
    fee_bps = _positive_int(record["fee_bps"], dex + " fee bps")
    numerator = _positive_int(record["fee_numerator"], dex + " fee numerator")
    denominator = _positive_int(record["fee_denominator"], dex + " fee denominator")
    if numerator >= denominator:
        raise ResearchContractError(dex + " fee fraction is invalid")
    if (fee_bps, numerator, denominator) != (30, 997, 1000):
        raise ResearchContractError(dex + " fee parameters are invalid")

    evidence = record["evidence"]
    if dex == "uniswap_v2":
        _exact_fields(evidence, ("kind",), dex + " fee evidence")
        if evidence["kind"] != "runtime_code_bound":
            raise ResearchContractError(dex + " fee evidence is invalid")
    elif dex == "shibaswap_v1":
        evidence = _exact_fields(
            evidence,
            (
                "kind",
                "target",
                "native_fee_denominator",
                "total_fee",
                "alpha",
                "beta",
            ),
            dex + " fee evidence",
        )
        if evidence["kind"] != "pair_native_parameters" or evidence["target"] != "pair":
            raise ResearchContractError(dex + " fee evidence is invalid")
        native_denominator = _positive_int(
            evidence["native_fee_denominator"], dex + " native fee denominator"
        )
        total_fee = _positive_int(evidence["total_fee"], dex + " total fee")
        _positive_int(evidence["alpha"], dex + " alpha")
        _positive_int(evidence["beta"], dex + " beta")
        if (
            numerator != native_denominator - total_fee
            or fee_bps * denominator != total_fee * 10000
        ):
            raise ResearchContractError(dex + " fee parameters are inconsistent")
    else:
        raise ResearchContractError("pool dex is invalid")
    return record


def _pool(value: object) -> dict:
    record = _exact_fields(
        value,
        ("dex", "factory", "router", "pair", "token0", "token1", "fee_model"),
        "pool",
    )
    dex = record["dex"]
    if dex not in {"uniswap_v2", "shibaswap_v1"}:
        raise ResearchContractError("pool dex is invalid")
    _runtime_code(record["factory"], dex + " factory")
    _runtime_code(record["router"], dex + " router")
    _runtime_code(record["pair"], dex + " pair")
    if record["token0"] != "SHIB" or record["token1"] != "WETH":
        raise ResearchContractError("pool token ordering is invalid")
    _fee_model(record["fee_model"], dex)
    return record


def _usd_reference(value: object) -> dict:
    record = _exact_fields(
        value,
        (
            "kind",
            "proxy_address",
            "runtime_code_size_bytes",
            "runtime_code_sha256",
            "description",
            "decimals",
            "max_age_seconds",
        ),
        "USD reference",
    )
    if record["kind"] != "chainlink_aggregator_v3":
        raise ResearchContractError("USD reference kind is invalid")
    _address(record["proxy_address"], "USD reference proxy address")
    _positive_int(record["runtime_code_size_bytes"], "USD reference code size")
    _sha256(record["runtime_code_sha256"], "USD reference code hash")
    if record["description"] != "ETH / USD":
        raise ResearchContractError("USD reference description is invalid")
    _positive_int(record["decimals"], "USD reference decimals")
    _positive_int(record["max_age_seconds"], "USD reference max age")
    return record


def _require_exact_authorities(registry: dict) -> None:
    actual = (
        tuple(
            (
                symbol,
                registry["tokens"][symbol]["address"],
                registry["tokens"][symbol]["decimals"],
                registry["tokens"][symbol]["runtime_code_size_bytes"],
                registry["tokens"][symbol]["runtime_code_sha256"],
            )
            for symbol in ("SHIB", "WETH")
        ),
        tuple(
        (
            pool["dex"],
            tuple(
                (
                    pool[role]["address"],
                    pool[role]["runtime_code_size_bytes"],
                    pool[role]["runtime_code_sha256"],
                )
                for role in ("factory", "router", "pair")
            ),
            tuple(sorted(pool["fee_model"]["evidence"].items())),
        )
        for pool in registry["pools"]
        ),
        (
            registry["usd_reference"]["proxy_address"],
            registry["usd_reference"]["runtime_code_size_bytes"],
            registry["usd_reference"]["runtime_code_sha256"],
            registry["usd_reference"]["decimals"],
            registry["usd_reference"]["max_age_seconds"],
        ),
    )
    if actual != _AUTHORITY_TRUST_ANCHOR:
        raise ResearchContractError("registry authorities are invalid")


def load_research_registry(payload: object) -> dict:
    """Validate the closed two-pool authority registry and return a copy."""
    registry = _exact_fields(
        payload,
        ("schema", "chain", "tokens", "pools", "usd_reference", "requested_notionals_usd"),
        "research registry",
    )
    if registry["schema"] != REGISTRY_SCHEMA:
        raise ResearchContractError("research registry schema is invalid")
    chain = _exact_fields(registry["chain"], ("name", "chain_id"), "chain")
    if chain["name"] != "eth" or type(chain["chain_id"]) is not int or chain["chain_id"] != 1:
        raise ResearchContractError("chain is invalid")
    tokens = _exact_fields(registry["tokens"], ("SHIB", "WETH"), "tokens")
    _token(tokens["SHIB"], "SHIB")
    _token(tokens["WETH"], "WETH")
    pools = registry["pools"]
    if not isinstance(pools, list) or len(pools) != 2:
        raise ResearchContractError("pool collection is invalid")
    for pool in pools:
        _pool(pool)
    if len({pool["dex"] for pool in pools}) != 2 or len(
        {pool["pair"]["address"] for pool in pools}
    ) != 2:
        raise ResearchContractError("pool collection contains duplicates")
    _usd_reference(registry["usd_reference"])
    if registry["requested_notionals_usd"] != [
        "1000", "5000", "10000", "50000", "100000"
    ]:
        raise ResearchContractError("requested notionals are invalid")
    _require_exact_authorities(registry)
    return json.loads(canonical_json_bytes(registry).decode("utf-8"))


def registry_sha256(registry: dict) -> str:
    normalized = load_research_registry(registry)
    return hashlib.sha256(canonical_json_bytes(normalized)).hexdigest()


def _bounded_hex(value: object, label: str, maximum_bytes: int) -> bytes:
    if (
        not isinstance(value, str)
        or _HEX.fullmatch(value) is None
        or (len(value) - 2) // 2 > maximum_bytes
    ):
        raise ResearchContractError("{} is invalid".format(label))
    return bytes.fromhex(value[2:])


def _hash32(value: object, label: str) -> str:
    if not isinstance(value, str) or _HASH32.fullmatch(value) is None:
        raise ResearchContractError("{} is invalid".format(label))
    return value


def _nonnegative_int(value: object, label: str) -> int:
    if type(value) is not int or value < 0:
        raise ResearchContractError("{} is invalid".format(label))
    return value


def _uint(value: object, bits: int, label: str, positive: bool = False) -> int:
    _nonnegative_int(value, label)
    if value >= 1 << bits or (positive and value == 0):
        raise ResearchContractError("{} is invalid".format(label))
    return value


def abi_encode_call(signature: str, arguments: Sequence[object]) -> str:
    """Encode only the reviewed calls used by the closed SHIB inventory."""
    if signature not in _FUNCTION_SELECTORS:
        raise ResearchContractError("ABI signature is not reviewed")
    if not isinstance(arguments, (list, tuple)):
        raise ResearchContractError("ABI arguments are invalid")
    expected_arguments = 2 if signature == "getPair(address,address)" else (
        1 if signature == "balanceOf(address)" else 0
    )
    if len(arguments) != expected_arguments:
        raise ResearchContractError("ABI argument count is invalid")
    encoded = bytearray.fromhex(_FUNCTION_SELECTORS[signature])
    for index, argument in enumerate(arguments):
        address = _address(argument, "ABI address argument {}".format(index))
        encoded.extend(b"\x00" * 12)
        encoded.extend(bytes.fromhex(address[2:]))
    return "0x" + bytes(encoded).hex()


def _decode_word(data: bytes, index: int) -> int:
    start = index * 32
    return int.from_bytes(data[start:start + 32], "big")


def abi_decode_result(kind: str, result_hex: str) -> object:
    """Decode the small reviewed ABI result grammar and reject ambiguity."""
    data = _bounded_hex(result_hex, "ABI result", _MAX_RESULT_BYTES)
    if kind == "address":
        if len(data) != 32 or data[:12] != b"\x00" * 12:
            raise ResearchContractError("ABI address result is invalid")
        return "0x" + data[12:].hex()
    if kind in {"uint8", "uint32", "uint256", "int256"}:
        if len(data) != 32:
            raise ResearchContractError("ABI integer result is invalid")
        unsigned = _decode_word(data, 0)
        if kind == "uint8" and unsigned >= 1 << 8:
            raise ResearchContractError("ABI uint8 result is invalid")
        if kind == "uint32" and unsigned >= 1 << 32:
            raise ResearchContractError("ABI uint32 result is invalid")
        if kind == "int256" and unsigned >= 1 << 255:
            return unsigned - (1 << 256)
        return unsigned
    if kind == "uint112_tuple":
        if len(data) != 96:
            raise ResearchContractError("ABI reserves result is invalid")
        values = (_decode_word(data, 0), _decode_word(data, 1), _decode_word(data, 2))
        if values[0] >= 1 << 112 or values[1] >= 1 << 112 or values[2] >= 1 << 32:
            raise ResearchContractError("ABI reserves result is invalid")
        return values
    if kind == "string":
        if len(data) < 64 or len(data) % 32 != 0 or _decode_word(data, 0) != 32:
            raise ResearchContractError("ABI string result is invalid")
        length = _decode_word(data, 1)
        if length > _MAX_ABI_STRING_BYTES:
            raise ResearchContractError("ABI string result is oversized")
        padded_length = ((length + 31) // 32) * 32
        if len(data) != 64 + padded_length:
            raise ResearchContractError("ABI string result has trailing words")
        raw = data[64:64 + length]
        if data[64 + length:] != b"\x00" * (padded_length - length):
            raise ResearchContractError("ABI string padding is invalid")
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError as error:
            raise ResearchContractError("ABI string is not UTF-8") from error
    if kind == "chainlink_round":
        if len(data) != 160:
            raise ResearchContractError("ABI Chainlink result is invalid")
        round_id = _decode_word(data, 0)
        answer_unsigned = _decode_word(data, 1)
        answer = (
            answer_unsigned - (1 << 256)
            if answer_unsigned >= 1 << 255 else answer_unsigned
        )
        started_at = _decode_word(data, 2)
        updated_at = _decode_word(data, 3)
        answered_in_round = _decode_word(data, 4)
        if round_id >= 1 << 80 or answered_in_round >= 1 << 80:
            raise ResearchContractError("ABI Chainlink round ID is invalid")
        return (round_id, answer, started_at, updated_at, answered_in_round)
    raise ResearchContractError("ABI result kind is not reviewed")


def _inventory_call(method: str, target: str, calldata: str) -> dict:
    _address(target, "inventory target")
    calldata_bytes = _bounded_hex(calldata, "inventory calldata", _MAX_CALLDATA_BYTES)
    identity = {
        "method": method,
        "target": target,
        "calldata_sha256": hashlib.sha256(calldata_bytes).hexdigest(),
        "block_selector": "eip1898_block_hash_require_canonical",
    }
    return dict(
        identity,
        logical_call_id="call:" + hashlib.sha256(
            b"shib-v2-logical-call/v1\n" + canonical_json_bytes(identity)
        ).hexdigest(),
        calldata=calldata,
    )


def build_logical_call_inventory(registry: dict) -> List[dict]:
    """Expand the strict registry into the closed 35-call state ledger."""
    registry = load_research_registry(registry)
    calls = []
    unique_code_targets = []
    for pool in registry["pools"]:
        unique_code_targets.extend(
            pool[role]["address"] for role in ("factory", "router", "pair")
        )
    unique_code_targets.extend(
        registry["tokens"][symbol]["address"] for symbol in ("SHIB", "WETH")
    )
    unique_code_targets.append(registry["usd_reference"]["proxy_address"])
    for target in unique_code_targets:
        calls.append(_inventory_call("eth_getCode", target, "0x"))
    shib = registry["tokens"]["SHIB"]["address"]
    weth = registry["tokens"]["WETH"]["address"]
    for pool in registry["pools"]:
        factory = pool["factory"]["address"]
        router = pool["router"]["address"]
        pair = pool["pair"]["address"]
        calls.extend((
            _inventory_call(
                "factory.getPair",
                factory,
                abi_encode_call("getPair(address,address)", (shib, weth)),
            ),
            _inventory_call("router.factory", router, abi_encode_call("factory()", ())),
            _inventory_call("router.weth", router, abi_encode_call("WETH()", ())),
            _inventory_call("pair.factory", pair, abi_encode_call("factory()", ())),
            _inventory_call("pair.token0", pair, abi_encode_call("token0()", ())),
            _inventory_call("pair.token1", pair, abi_encode_call("token1()", ())),
            _inventory_call(
                "pair.getReserves", pair, abi_encode_call("getReserves()", ())
            ),
            _inventory_call(
                "erc20.balanceOf", shib, abi_encode_call("balanceOf(address)", (pair,))
            ),
            _inventory_call(
                "erc20.balanceOf", weth, abi_encode_call("balanceOf(address)", (pair,))
            ),
        ))
    for symbol in ("SHIB", "WETH"):
        calls.append(_inventory_call(
            "erc20.decimals",
            registry["tokens"][symbol]["address"],
            abi_encode_call("decimals()", ()),
        ))
    shibaswap_pair = registry["pools"][1]["pair"]["address"]
    calls.extend((
        _inventory_call(
            "fee.totalFee", shibaswap_pair, abi_encode_call("totalFee()", ())
        ),
        _inventory_call("fee.alpha", shibaswap_pair, abi_encode_call("alpha()", ())),
        _inventory_call("fee.beta", shibaswap_pair, abi_encode_call("beta()", ())),
    ))
    feed = registry["usd_reference"]["proxy_address"]
    calls.extend((
        _inventory_call("feed.decimals", feed, abi_encode_call("decimals()", ())),
        _inventory_call(
            "feed.description", feed, abi_encode_call("description()", ())
        ),
        _inventory_call(
            "feed.latestRoundData", feed, abi_encode_call("latestRoundData()", ())
        ),
    ))
    calls.sort(key=lambda call: call["logical_call_id"])
    if len(calls) != 35 or len({call["logical_call_id"] for call in calls}) != 35:
        raise ResearchContractError("logical call inventory is not closed")
    return calls


def _persisted_logical_call(inventory_call: dict, result_hex: str) -> dict:
    result = _bounded_hex(result_hex, "call result", _MAX_RESULT_BYTES)
    record = {
        key: inventory_call[key]
        for key in (
            "logical_call_id", "method", "target", "calldata", "calldata_sha256"
        )
    }
    record.update({
        "result_hex": result_hex,
        "result_sha256": hashlib.sha256(result).hexdigest(),
    })
    return record


def _call_results_sha256(calls: Sequence[dict]) -> str:
    members = [
        {
            "logical_call_id": call["logical_call_id"],
            "result_sha256": call["result_sha256"],
        }
        for call in sorted(calls, key=lambda item: item["logical_call_id"])
    ]
    return hashlib.sha256(
        b"shib-v2-call-results/v1\n" + canonical_json_bytes(members)
    ).hexdigest()


def evidence_identity(payload: dict) -> str:
    if not isinstance(payload, dict):
        raise ResearchContractError("evidence identity payload is invalid")
    body = dict(payload)
    body.pop("evidence_identity", None)
    return hashlib.sha256(
        b"shib-v2-research-evidence/v1\n" + canonical_json_bytes(body)
    ).hexdigest()


_BLOCK_FIELDS = (
    "number", "hash", "parent_hash", "timestamp", "timestamp_utc",
    "state_root", "base_fee_per_gas", "canonical_header_sha256",
    "provider_header_observations",
)
_LOGICAL_CALL_FIELDS = (
    "logical_call_id", "method", "target", "calldata", "calldata_sha256",
    "result_hex", "result_sha256",
)
_PROVIDER_OBSERVATION_FIELDS = (
    "provider_label", "logical_call_id", "block_hash", "result_sha256", "status",
)
_TOKEN_OBSERVATION_FIELDS = (
    "symbol", "address", "decimals", "runtime_code_size_bytes",
    "runtime_code_sha256", "call_results_sha256",
)
_POOL_OBSERVATION_FIELDS = (
    "dex", "factory_address", "router_address", "pair_address",
    "factory_runtime_code_size_bytes", "factory_runtime_code_sha256",
    "router_runtime_code_size_bytes", "router_runtime_code_sha256",
    "pair_runtime_code_size_bytes", "pair_runtime_code_sha256",
    "factory_get_pair_result", "router_factory_result", "router_weth_result",
    "pair_factory_result", "token0_address", "token1_address",
    "token0_decimals", "token1_decimals", "reserve0_raw", "reserve1_raw",
    "reserve_timestamp_last_raw", "token0_balance_raw", "token1_balance_raw",
    "reserve_lag_seconds", "fee_bps", "fee_numerator", "fee_denominator",
    "fee_formula", "fee_parameters", "fee_evidence_sha256",
    "call_results_sha256",
)
_USD_REFERENCE_FIELDS = (
    "kind", "proxy_address", "proxy_runtime_code_size_bytes",
    "proxy_runtime_code_sha256", "description", "decimals", "round_id", "answer",
    "started_at", "updated_at", "answered_in_round", "freshness_lag_seconds",
    "call_results_sha256",
)
_QUALITY_FIELDS = (
    "state", "expected_logical_call_count", "observed_logical_call_count",
    "usable_logical_call_count", "expected_provider_observation_count",
    "observed_provider_observation_count", "usable_provider_observation_count",
    "duplicate_logical_call_key_count", "duplicate_provider_observation_key_count",
    "required_field_null_count", "measured_zero_count", "missing_null_count",
    "provider_agreement_count", "provider_disagreement_count", "status_counts",
)


def _validate_evidence_shape(payload: object) -> dict:
    evidence = _exact_fields(
        payload,
        (
            "schema", "registry_sha256", "chain", "block", "logical_calls",
            "provider_observations", "tokens", "usd_reference", "pools",
            "collection_quality", "evidence_identity",
        ),
        "research evidence",
    )
    if evidence["schema"] != EVIDENCE_SCHEMA:
        raise ResearchContractError("research evidence schema is invalid")
    _sha256(evidence["registry_sha256"], "evidence registry hash")
    _sha256(evidence["evidence_identity"], "evidence identity")
    chain = _exact_fields(evidence["chain"], ("name", "chain_id"), "evidence chain")
    if chain["name"] != "eth" or chain["chain_id"] != 1 or type(chain["chain_id"]) is not int:
        raise ResearchContractError("evidence chain is invalid")
    block = _exact_fields(evidence["block"], _BLOCK_FIELDS, "evidence block")
    _positive_int(block["number"], "block number")
    _hash32(block["hash"], "block hash")
    if block["hash"] == "0x" + "00" * 32:
        raise ResearchContractError("canonical block hash must be nonzero")
    _hash32(block["parent_hash"], "parent block hash")
    _uint(block["timestamp"], 256, "block timestamp", positive=True)
    if not isinstance(block["timestamp_utc"], str):
        raise ResearchContractError("block timestamp UTC is invalid")
    _hash32(block["state_root"], "block state root")
    _uint(block["base_fee_per_gas"], 256, "block base fee")
    _sha256(block["canonical_header_sha256"], "canonical header hash")
    if not isinstance(block["provider_header_observations"], list):
        raise ResearchContractError("header observations are invalid")
    for observation in block["provider_header_observations"]:
        record = _exact_fields(
            observation,
            ("provider_label", "canonical_header_sha256", "status"),
            "header observation",
        )
        _sha256(record["canonical_header_sha256"], "header observation hash")
    if not isinstance(evidence["logical_calls"], list):
        raise ResearchContractError("logical calls are invalid")
    for call in evidence["logical_calls"]:
        record = _exact_fields(call, _LOGICAL_CALL_FIELDS, "logical call")
        if not isinstance(record["logical_call_id"], str) or _CALL_ID.fullmatch(record["logical_call_id"]) is None:
            raise ResearchContractError("logical call ID is invalid")
        if not isinstance(record["method"], str):
            raise ResearchContractError("logical call method is invalid")
        _address(record["target"], "logical call target")
        _bounded_hex(record["calldata"], "logical call calldata", _MAX_CALLDATA_BYTES)
        _sha256(record["calldata_sha256"], "logical call calldata hash")
        _bounded_hex(record["result_hex"], "logical call result", _MAX_RESULT_BYTES)
        _sha256(record["result_sha256"], "logical call result hash")
    if not isinstance(evidence["provider_observations"], list):
        raise ResearchContractError("provider observations are invalid")
    for observation in evidence["provider_observations"]:
        record = _exact_fields(
            observation, _PROVIDER_OBSERVATION_FIELDS, "provider observation"
        )
        if not isinstance(record["logical_call_id"], str) or _CALL_ID.fullmatch(record["logical_call_id"]) is None:
            raise ResearchContractError("provider logical call ID is invalid")
        _hash32(record["block_hash"], "provider block hash")
        _sha256(record["result_sha256"], "provider result hash")
    if not isinstance(evidence["tokens"], list):
        raise ResearchContractError("token observations are invalid")
    for token in evidence["tokens"]:
        record = _exact_fields(token, _TOKEN_OBSERVATION_FIELDS, "token observation")
        _address(record["address"], "token observation address")
        _uint(record["decimals"], 8, "token observation decimals")
        _positive_int(record["runtime_code_size_bytes"], "token code size")
        _sha256(record["runtime_code_sha256"], "token code hash")
        _sha256(record["call_results_sha256"], "token call-results hash")
    if not isinstance(evidence["pools"], list):
        raise ResearchContractError("pool observations are invalid")
    for pool in evidence["pools"]:
        record = _exact_fields(pool, _POOL_OBSERVATION_FIELDS, "pool observation")
        for field in (
            "factory_address", "router_address", "pair_address",
            "factory_get_pair_result", "router_factory_result", "router_weth_result",
            "pair_factory_result", "token0_address", "token1_address",
        ):
            _address(record[field], "pool {}".format(field))
        for field in (
            "factory_runtime_code_size_bytes", "router_runtime_code_size_bytes",
            "pair_runtime_code_size_bytes",
        ):
            _positive_int(record[field], "pool {}".format(field))
        for field in (
            "factory_runtime_code_sha256", "router_runtime_code_sha256",
            "pair_runtime_code_sha256", "fee_evidence_sha256",
            "call_results_sha256",
        ):
            _sha256(record[field], "pool {}".format(field))
        _uint(record["token0_decimals"], 8, "pool token0 decimals")
        _uint(record["token1_decimals"], 8, "pool token1 decimals")
        _uint(record["reserve0_raw"], 112, "pool reserve0", positive=True)
        _uint(record["reserve1_raw"], 112, "pool reserve1", positive=True)
        _uint(record["reserve_timestamp_last_raw"], 32, "pool reserve timestamp")
        _uint(record["token0_balance_raw"], 256, "pool token0 balance", positive=True)
        _uint(record["token1_balance_raw"], 256, "pool token1 balance", positive=True)
        _uint(record["reserve_lag_seconds"], 32, "pool reserve lag")
        _uint(record["fee_bps"], 16, "pool fee bps")
        _positive_int(record["fee_numerator"], "pool fee numerator")
        _positive_int(record["fee_denominator"], "pool fee denominator")
        parameters = record["fee_parameters"]
        if isinstance(parameters, dict) and parameters.get("kind") == "runtime_code_bound":
            _exact_fields(parameters, ("kind",), "runtime fee parameters")
        else:
            parameters = _exact_fields(
                parameters,
                (
                    "kind", "native_fee_denominator", "total_fee", "alpha", "beta",
                ),
                "native fee parameters",
            )
            if parameters["kind"] != "pair_native_parameters":
                raise ResearchContractError("native fee kind is invalid")
            _positive_int(parameters["native_fee_denominator"], "native fee denominator")
            for field in ("total_fee", "alpha", "beta"):
                _uint(parameters[field], 256, "native fee {}".format(field))
    reference = _exact_fields(
        evidence["usd_reference"], _USD_REFERENCE_FIELDS, "USD reference observation"
    )
    _address(reference["proxy_address"], "USD reference proxy")
    _positive_int(reference["proxy_runtime_code_size_bytes"], "USD reference code size")
    _sha256(reference["proxy_runtime_code_sha256"], "USD reference code hash")
    _uint(reference["decimals"], 8, "USD reference decimals")
    _uint(reference["round_id"], 80, "USD reference round", positive=True)
    if type(reference["answer"]) is not int or not (-(1 << 255) <= reference["answer"] < 1 << 255):
        raise ResearchContractError("USD reference answer is invalid")
    _uint(reference["started_at"], 256, "USD reference started at", positive=True)
    _uint(reference["updated_at"], 256, "USD reference updated at", positive=True)
    _uint(reference["answered_in_round"], 80, "USD reference answered round", positive=True)
    _uint(reference["freshness_lag_seconds"], 256, "USD reference freshness lag")
    _sha256(reference["call_results_sha256"], "USD reference call-results hash")
    quality = _exact_fields(
        evidence["collection_quality"], _QUALITY_FIELDS, "collection quality"
    )
    if quality["state"] != "evaluated":
        raise ResearchContractError("collection quality state is invalid")
    for field in _QUALITY_FIELDS[1:-1]:
        _nonnegative_int(quality[field], "collection quality {}".format(field))
    status_counts = _exact_fields(quality["status_counts"], ("observed",), "status counts")
    _nonnegative_int(status_counts["observed"], "observed status count")
    return evidence


def _require_exact_call_set(logical_calls: Sequence[dict], expected: Sequence[dict]) -> None:
    expected_ids = [call["logical_call_id"] for call in expected]
    actual_ids = [call["logical_call_id"] for call in logical_calls]
    if actual_ids != expected_ids:
        raise ResearchContractError("logical call set is not exact")
    for actual, inventory_call in zip(logical_calls, expected):
        for field in ("logical_call_id", "method", "target", "calldata", "calldata_sha256"):
            if actual[field] != inventory_call[field]:
                raise ResearchContractError("logical call identity is invalid")
        calldata = _bounded_hex(actual["calldata"], "logical calldata", _MAX_CALLDATA_BYTES)
        result = _bounded_hex(actual["result_hex"], "logical result", _MAX_RESULT_BYTES)
        if hashlib.sha256(calldata).hexdigest() != actual["calldata_sha256"]:
            raise ResearchContractError("calldata hash does not recompute")
        if hashlib.sha256(result).hexdigest() != actual["result_sha256"]:
            raise ResearchContractError("result hash does not recompute")
        if actual["method"] == "eth_getCode" and not result:
            raise ResearchContractError("runtime code is empty")


def _require_two_agreed_observations(evidence: dict) -> None:
    calls = {call["logical_call_id"]: call for call in evidence["logical_calls"]}
    expected_order = [
        (call["logical_call_id"], label)
        for call in evidence["logical_calls"]
        for label in ("provider_a", "provider_b")
    ]
    actual_order = [
        (observation["logical_call_id"], observation["provider_label"])
        for observation in evidence["provider_observations"]
    ]
    if actual_order != expected_order:
        raise ResearchContractError("provider observation set is not exact")
    for observation in evidence["provider_observations"]:
        call = calls[observation["logical_call_id"]]
        if (
            observation["status"] != "observed"
            or observation["result_sha256"] != call["result_sha256"]
        ):
            raise ResearchContractError("provider results do not agree")


def _validate_header_and_every_block_binding(evidence: dict) -> None:
    block = evidence["block"]
    header = {
        field: block[field]
        for field in (
            "number", "hash", "parent_hash", "timestamp", "state_root",
            "base_fee_per_gas",
        )
    }
    header_hash = hashlib.sha256(canonical_json_bytes(header)).hexdigest()
    if block["canonical_header_sha256"] != header_hash:
        raise ResearchContractError("canonical header hash does not recompute")
    try:
        expected_utc = datetime.fromtimestamp(
            block["timestamp"], timezone.utc
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
    except (OverflowError, OSError, ValueError) as error:
        raise ResearchContractError("block timestamp is not representable") from error
    if block["timestamp_utc"] != expected_utc:
        raise ResearchContractError("block timestamp UTC does not recompute")
    expected_headers = [
        {
            "provider_label": label,
            "canonical_header_sha256": header_hash,
            "status": "observed",
        }
        for label in ("provider_a", "provider_b")
    ]
    if block["provider_header_observations"] != expected_headers:
        raise ResearchContractError("header observations do not agree")
    if any(
        observation["block_hash"] != block["hash"]
        for observation in evidence["provider_observations"]
    ):
        raise ResearchContractError("provider observation block binding is invalid")


def _find_call(
    calls: Sequence[dict], method: str, target: str, calldata: str = ""
) -> dict:
    matches = [
        call for call in calls
        if call["method"] == method
        and call["target"] == target
        and (not calldata or call["calldata"] == calldata)
    ]
    if len(matches) != 1:
        raise ResearchContractError("evidence call lookup is ambiguous")
    return matches[0]


def _validate_code_call(call: dict, size: int, code_hash: str) -> None:
    code = _bounded_hex(call["result_hex"], "runtime code", _MAX_RESULT_BYTES)
    if not code or len(code) != size or hashlib.sha256(code).hexdigest() != code_hash:
        raise ResearchContractError("runtime code authority is invalid")


def _validate_tokens_pools_fees_and_feed(evidence: dict, registry: dict) -> None:
    calls = evidence["logical_calls"]
    if evidence["registry_sha256"] != registry_sha256(registry):
        raise ResearchContractError("registry hash does not recompute")
    if evidence["chain"] != registry["chain"]:
        raise ResearchContractError("evidence chain does not match registry")
    if [token["symbol"] for token in evidence["tokens"]] != ["SHIB", "WETH"]:
        raise ResearchContractError("token observation order is invalid")
    for token_record, symbol in zip(evidence["tokens"], ("SHIB", "WETH")):
        authority = registry["tokens"][symbol]
        code_call = _find_call(calls, "eth_getCode", authority["address"])
        decimals_call = _find_call(calls, "erc20.decimals", authority["address"])
        _validate_code_call(
            code_call,
            authority["runtime_code_size_bytes"],
            authority["runtime_code_sha256"],
        )
        decimals = abi_decode_result("uint8", decimals_call["result_hex"])
        expected_token = {
            "symbol": symbol,
            "address": authority["address"],
            "decimals": decimals,
            "runtime_code_size_bytes": len(bytes.fromhex(code_call["result_hex"][2:])),
            "runtime_code_sha256": hashlib.sha256(
                bytes.fromhex(code_call["result_hex"][2:])
            ).hexdigest(),
            "call_results_sha256": _call_results_sha256((code_call, decimals_call)),
        }
        if token_record != expected_token or decimals != authority["decimals"]:
            raise ResearchContractError("token authority is invalid")
    if [pool["dex"] for pool in evidence["pools"]] != [
        pool["dex"] for pool in registry["pools"]
    ]:
        raise ResearchContractError("pool observation order is invalid")
    shib = registry["tokens"]["SHIB"]
    weth = registry["tokens"]["WETH"]
    block_timestamp = evidence["block"]["timestamp"]
    for pool_record, authority in zip(evidence["pools"], registry["pools"]):
        factory = authority["factory"]["address"]
        router = authority["router"]["address"]
        pair = authority["pair"]["address"]
        code_calls = {
            role: _find_call(calls, "eth_getCode", authority[role]["address"])
            for role in ("factory", "router", "pair")
        }
        for role in ("factory", "router", "pair"):
            _validate_code_call(
                code_calls[role],
                authority[role]["runtime_code_size_bytes"],
                authority[role]["runtime_code_sha256"],
            )
        get_pair = _find_call(
            calls,
            "factory.getPair",
            factory,
            abi_encode_call("getPair(address,address)", (shib["address"], weth["address"])),
        )
        router_factory = _find_call(calls, "router.factory", router)
        router_weth = _find_call(calls, "router.weth", router)
        pair_factory = _find_call(calls, "pair.factory", pair)
        token0 = _find_call(calls, "pair.token0", pair)
        token1 = _find_call(calls, "pair.token1", pair)
        reserves_call = _find_call(calls, "pair.getReserves", pair)
        shib_decimals = _find_call(calls, "erc20.decimals", shib["address"])
        weth_decimals = _find_call(calls, "erc20.decimals", weth["address"])
        shib_balance = _find_call(
            calls,
            "erc20.balanceOf",
            shib["address"],
            abi_encode_call("balanceOf(address)", (pair,)),
        )
        weth_balance = _find_call(
            calls,
            "erc20.balanceOf",
            weth["address"],
            abi_encode_call("balanceOf(address)", (pair,)),
        )
        decoded_addresses = (
            abi_decode_result("address", get_pair["result_hex"]),
            abi_decode_result("address", router_factory["result_hex"]),
            abi_decode_result("address", router_weth["result_hex"]),
            abi_decode_result("address", pair_factory["result_hex"]),
            abi_decode_result("address", token0["result_hex"]),
            abi_decode_result("address", token1["result_hex"]),
        )
        if decoded_addresses != (
            pair, factory, weth["address"], factory, shib["address"], weth["address"]
        ):
            raise ResearchContractError("pool identity round trip is invalid")
        reserve0, reserve1, reserve_timestamp = abi_decode_result(
            "uint112_tuple", reserves_call["result_hex"]
        )
        if reserve0 <= 0 or reserve1 <= 0 or reserve_timestamp > block_timestamp:
            raise ResearchContractError("pool reserves are invalid")
        token0_decimals = abi_decode_result("uint8", shib_decimals["result_hex"])
        token1_decimals = abi_decode_result("uint8", weth_decimals["result_hex"])
        token0_balance = abi_decode_result("uint256", shib_balance["result_hex"])
        token1_balance = abi_decode_result("uint256", weth_balance["result_hex"])
        if token0_balance != reserve0 or token1_balance != reserve1:
            raise ResearchContractError("pool balances do not equal reserves")
        pool_calls = [
            call for call in calls
            if call["target"] in {factory, router, pair}
            or call["method"] == "erc20.decimals"
            or (
                call["method"] == "erc20.balanceOf"
                and call["calldata"].endswith(pair[2:])
            )
        ]
        fee_model = authority["fee_model"]
        if authority["dex"] == "uniswap_v2":
            fee_parameters = {"kind": "runtime_code_bound"}
            fee_calls = [code_calls["pair"]]
        else:
            total_fee_call = _find_call(calls, "fee.totalFee", pair)
            alpha_call = _find_call(calls, "fee.alpha", pair)
            beta_call = _find_call(calls, "fee.beta", pair)
            total_fee = abi_decode_result("uint256", total_fee_call["result_hex"])
            alpha = abi_decode_result("uint256", alpha_call["result_hex"])
            beta = abi_decode_result("uint256", beta_call["result_hex"])
            native = fee_model["evidence"]
            if (
                total_fee != native["total_fee"]
                or alpha != native["alpha"]
                or beta != native["beta"]
                or fee_model["fee_numerator"] != native["native_fee_denominator"] - total_fee
                or fee_model["fee_bps"] * fee_model["fee_denominator"] != total_fee * 10000
            ):
                raise ResearchContractError("ShibaSwap fee authority is invalid")
            fee_parameters = {
                "kind": "pair_native_parameters",
                "native_fee_denominator": native["native_fee_denominator"],
                "total_fee": total_fee,
                "alpha": alpha,
                "beta": beta,
            }
            fee_calls = [total_fee_call, alpha_call, beta_call]
        expected_pool = {
            "dex": authority["dex"],
            "factory_address": factory,
            "router_address": router,
            "pair_address": pair,
            "factory_runtime_code_size_bytes": authority["factory"]["runtime_code_size_bytes"],
            "factory_runtime_code_sha256": authority["factory"]["runtime_code_sha256"],
            "router_runtime_code_size_bytes": authority["router"]["runtime_code_size_bytes"],
            "router_runtime_code_sha256": authority["router"]["runtime_code_sha256"],
            "pair_runtime_code_size_bytes": authority["pair"]["runtime_code_size_bytes"],
            "pair_runtime_code_sha256": authority["pair"]["runtime_code_sha256"],
            "factory_get_pair_result": decoded_addresses[0],
            "router_factory_result": decoded_addresses[1],
            "router_weth_result": decoded_addresses[2],
            "pair_factory_result": decoded_addresses[3],
            "token0_address": decoded_addresses[4],
            "token1_address": decoded_addresses[5],
            "token0_decimals": token0_decimals,
            "token1_decimals": token1_decimals,
            "reserve0_raw": reserve0,
            "reserve1_raw": reserve1,
            "reserve_timestamp_last_raw": reserve_timestamp,
            "token0_balance_raw": token0_balance,
            "token1_balance_raw": token1_balance,
            "reserve_lag_seconds": block_timestamp - reserve_timestamp,
            "fee_bps": fee_model["fee_bps"],
            "fee_numerator": fee_model["fee_numerator"],
            "fee_denominator": fee_model["fee_denominator"],
            "fee_formula": fee_model["formula"],
            "fee_parameters": fee_parameters,
            "fee_evidence_sha256": _call_results_sha256(fee_calls),
            "call_results_sha256": _call_results_sha256(pool_calls),
        }
        if pool_record != expected_pool:
            raise ResearchContractError("pool observation does not recompute")
    reference_authority = registry["usd_reference"]
    proxy = reference_authority["proxy_address"]
    code_call = _find_call(calls, "eth_getCode", proxy)
    decimals_call = _find_call(calls, "feed.decimals", proxy)
    description_call = _find_call(calls, "feed.description", proxy)
    round_call = _find_call(calls, "feed.latestRoundData", proxy)
    _validate_code_call(
        code_call,
        reference_authority["runtime_code_size_bytes"],
        reference_authority["runtime_code_sha256"],
    )
    decimals = abi_decode_result("uint8", decimals_call["result_hex"])
    description = abi_decode_result("string", description_call["result_hex"])
    round_id, answer, started_at, updated_at, answered_in_round = abi_decode_result(
        "chainlink_round", round_call["result_hex"]
    )
    if (
        decimals != reference_authority["decimals"]
        or description != reference_authority["description"]
        or answer <= 0
        or started_at > updated_at
        or updated_at > block_timestamp
        or block_timestamp - updated_at > reference_authority["max_age_seconds"]
        or answered_in_round < round_id
    ):
        raise ResearchContractError("USD reference authority is invalid")
    expected_reference = {
        "kind": reference_authority["kind"],
        "proxy_address": proxy,
        "proxy_runtime_code_size_bytes": reference_authority["runtime_code_size_bytes"],
        "proxy_runtime_code_sha256": reference_authority["runtime_code_sha256"],
        "description": description,
        "decimals": decimals,
        "round_id": round_id,
        "answer": answer,
        "started_at": started_at,
        "updated_at": updated_at,
        "answered_in_round": answered_in_round,
        "freshness_lag_seconds": block_timestamp - updated_at,
        "call_results_sha256": _call_results_sha256(
            (code_call, decimals_call, description_call, round_call)
        ),
    }
    if evidence["usd_reference"] != expected_reference:
        raise ResearchContractError("USD reference observation does not recompute")


def _count_nulls(value: object) -> int:
    if value is None:
        return 1
    if isinstance(value, dict):
        return sum(_count_nulls(child) for child in value.values())
    if isinstance(value, list):
        return sum(_count_nulls(child) for child in value)
    return 0


def _count_numeric_zeroes(value: object) -> int:
    if type(value) is int:
        return int(value == 0)
    if isinstance(value, dict):
        return sum(_count_numeric_zeroes(child) for child in value.values())
    if isinstance(value, list):
        return sum(_count_numeric_zeroes(child) for child in value)
    return 0


def _recompute_collection_quality(evidence: dict, expected: Sequence[dict]) -> dict:
    logical_calls = evidence["logical_calls"]
    observations = evidence["provider_observations"]
    logical_ids = [call["logical_call_id"] for call in logical_calls]
    provider_keys = [
        (observation["provider_label"], observation["logical_call_id"])
        for observation in observations
    ]
    call_by_id = {call["logical_call_id"]: call for call in logical_calls}
    agreement_count = 0
    disagreement_count = 0
    for logical_call_id in set(logical_ids):
        hashes = [
            observation["result_sha256"] for observation in observations
            if observation["logical_call_id"] == logical_call_id
            and observation["status"] == "observed"
        ]
        if len(hashes) == 2 and len(set(hashes)) == 1 and hashes[0] == call_by_id[logical_call_id]["result_sha256"]:
            agreement_count += 1
        elif hashes:
            disagreement_count += 1
    observation_values = {
        "block": evidence["block"],
        "tokens": evidence["tokens"],
        "pools": evidence["pools"],
        "usd_reference": evidence["usd_reference"],
    }
    return {
        "state": "evaluated",
        "expected_logical_call_count": len(expected),
        "observed_logical_call_count": len(logical_calls),
        "usable_logical_call_count": sum(
            bool(call["result_hex"] != "0x") for call in logical_calls
        ),
        "expected_provider_observation_count": len(expected) * 2,
        "observed_provider_observation_count": len(observations),
        "usable_provider_observation_count": sum(
            observation["status"] == "observed" for observation in observations
        ),
        "duplicate_logical_call_key_count": len(logical_ids) - len(set(logical_ids)),
        "duplicate_provider_observation_key_count": len(provider_keys) - len(set(provider_keys)),
        "required_field_null_count": _count_nulls({
            key: value for key, value in evidence.items()
            if key not in {"collection_quality", "evidence_identity"}
        }),
        "measured_zero_count": _count_numeric_zeroes(observation_values),
        "missing_null_count": 0,
        "provider_agreement_count": agreement_count,
        "provider_disagreement_count": disagreement_count,
        "status_counts": {
            "observed": sum(
                observation["status"] == "observed" for observation in observations
            )
        },
    }


def validate_research_evidence(payload: object, registry: dict) -> dict:
    """Validate and canonicalize one complete dual-provider evidence ledger."""
    registry = load_research_registry(registry)
    evidence = _validate_evidence_shape(payload)
    expected = build_logical_call_inventory(registry)
    _require_exact_call_set(evidence["logical_calls"], expected)
    _require_two_agreed_observations(evidence)
    _validate_header_and_every_block_binding(evidence)
    _validate_tokens_pools_fees_and_feed(evidence, registry)
    recomputed = _recompute_collection_quality(evidence, expected)
    if evidence["collection_quality"] != recomputed:
        raise ResearchContractError("collection quality does not recompute")
    if evidence["evidence_identity"] != evidence_identity(evidence):
        raise ResearchContractError("evidence identity does not recompute")
    scan_public_payload(evidence)
    return json.loads(canonical_json_bytes(evidence).decode("utf-8"))


def _ratio(value: Fraction) -> dict:
    exact = Fraction(value)
    return {"numerator": exact.numerator, "denominator": exact.denominator}


def _validated_ratio(value: object, label: str, *, positive: bool = False) -> Fraction:
    record = _exact_fields(value, ("numerator", "denominator"), label)
    if type(record["numerator"]) is not int:
        raise ResearchContractError("{} numerator is invalid".format(label))
    denominator = _positive_int(record["denominator"], label + " denominator")
    exact = Fraction(record["numerator"], denominator)
    if (
        exact.numerator != record["numerator"]
        or exact.denominator != denominator
        or (positive and exact <= 0)
    ):
        raise ResearchContractError("{} is not a canonical ratio".format(label))
    return exact


def _pool_state(pool: dict, block: dict) -> V2PoolState:
    try:
        return V2PoolState(
            chain="eth",
            chain_id=1,
            dex=pool["dex"],
            pool_address=pool["pair_address"],
            token0_address=pool["token0_address"],
            token1_address=pool["token1_address"],
            token0_decimals=pool["token0_decimals"],
            token1_decimals=pool["token1_decimals"],
            reserve0_raw=pool["reserve0_raw"],
            reserve1_raw=pool["reserve1_raw"],
            reserve_timestamp_last_raw=pool["reserve_timestamp_last_raw"],
            fee_bps=pool["fee_bps"],
            fee_numerator=pool["fee_numerator"],
            fee_denominator=pool["fee_denominator"],
            fee_formula=pool["fee_formula"],
            fee_proof_sha256=pool["fee_evidence_sha256"],
            block_number=block["number"],
            block_hash=block["hash"],
            block_header_sha256=block["canonical_header_sha256"],
            observed_at=block["timestamp_utc"],
            raw_response_sha256=pool["call_results_sha256"],
        )
    except (KeyError, TypeError, ValueError) as error:
        raise ResearchContractError("pool state is invalid") from error


def _market_rules(pool: dict, block: dict) -> MarketRules:
    try:
        valid_until = datetime.fromtimestamp(
            block["timestamp"] + 1, timezone.utc
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
        return MarketRules(
            market_id="dex:eth:{}:{}:SHIB".format(
                pool["dex"], pool["pair_address"]
            ),
            base_asset="SHIB",
            quote_asset="WETH",
            base_unit_decimals=18,
            quote_unit_decimals=18,
            base_increment=Decimal("0.000000000000000001"),
            quote_increment=Decimal("0.000000000000000001"),
            min_base_quantity=Decimal(0),
            min_quote_notional=Decimal(0),
            observed_at=block["timestamp_utc"],
            valid_until=valid_until,
            source_record_sha256=pool["call_results_sha256"],
        )
    except (KeyError, TypeError, ValueError, OverflowError, OSError) as error:
        raise ResearchContractError("market rules are invalid") from error


def _common_target(notional_usd: int, reference_price: Fraction) -> CommonTarget:
    theoretical_raw = Fraction(notional_usd * 10**18, 1) / reference_price
    raw = theoretical_raw.numerator // theoretical_raw.denominator
    if raw <= 0:
        raise ResearchContractError("common target is below one SHIB wei")
    try:
        return CommonTarget(
            asset="SHIB", unit_decimals=18, raw_quantity=raw, lattice_raw=1
        )
    except ValueError as error:
        raise ResearchContractError("common target is invalid") from error


def _quote_weth_raw(quote: QuantityQuote) -> int:
    if quote.gross_quote_quantity is None:
        raise ResearchContractError("quote has no WETH amount")
    raw = Fraction(quote.gross_quote_quantity) * 10**18
    if raw.denominator != 1:
        raise ResearchContractError("quote WETH amount is not raw-unit exact")
    return raw.numerator


def _pool_reference_shib_usd(
    pool: dict,
    shib_address: str,
    weth_address: str,
    eth_usd: Fraction,
) -> Fraction:
    if (pool["token0_address"], pool["token1_address"]) == (
        shib_address,
        weth_address,
    ):
        reserve_shib = pool["reserve0_raw"]
        reserve_weth = pool["reserve1_raw"]
        shib_decimals = pool["token0_decimals"]
        weth_decimals = pool["token1_decimals"]
    elif (pool["token0_address"], pool["token1_address"]) == (
        weth_address,
        shib_address,
    ):
        reserve_shib = pool["reserve1_raw"]
        reserve_weth = pool["reserve0_raw"]
        shib_decimals = pool["token1_decimals"]
        weth_decimals = pool["token0_decimals"]
    else:
        raise ResearchContractError("pool token order is invalid")
    reference = (
        Fraction(reserve_weth * 10**shib_decimals, reserve_shib * 10**weth_decimals)
        * eth_usd
    )
    if reference <= 0:
        raise ResearchContractError("pool SHIB/USD reference is invalid")
    return reference


def _quote_for_scenario(
    state: V2PoolState,
    target: CommonTarget,
    rules: MarketRules,
    direction: str,
    shib_address: str,
    weth_address: str,
    block_time: str,
) -> QuantityQuote:
    try:
        quote = quote_v2_pool_quantity(
            state,
            target,
            rules,
            direction=direction,
            target_token_address=shib_address,
            quote_token_address=weth_address,
            cohort_now=block_time,
        )
        return validate_v2_quantity_quote_against_state(
            quote,
            state,
            target,
            rules,
            direction=direction,
            target_token_address=shib_address,
            quote_token_address=weth_address,
            cohort_now=block_time,
        )
    except ValueError as error:
        raise ResearchContractError("V2 quantity quote is invalid") from error


def _scenario_reason_codes(
    buy_quote: QuantityQuote,
    sell_quote: QuantityQuote,
    classification: str,
) -> List[str]:
    if classification == "unavailable":
        reasons = []
        for quote in (buy_quote, sell_quote):
            if quote.status == "unavailable" and quote.reason_code not in reasons:
                reasons.append(quote.reason_code)
        return reasons
    reasons = [_COMPLETE_QUOTE_REASON]
    if classification == "positive_pool_edge_costs_incomplete":
        reasons.append("route_costs_not_evaluated")
    return reasons


def _snapshot_summary(scenarios: Sequence[dict]) -> dict:
    classifications = (
        "non_positive_pool_edge",
        "positive_pool_edge_costs_incomplete",
        "unavailable",
    )
    return {
        "expected_scenario_count": 10,
        "observed_scenario_count": len(scenarios),
        "usable_scenario_count": sum(
            row["classification"] != "unavailable" for row in scenarios
        ),
        "classification_counts": {
            classification: sum(
                row["classification"] == classification for row in scenarios
            )
            for classification in classifications
        },
        "strict_eligible_count": sum(row["strict_eligible"] for row in scenarios),
        "executable_count": sum(row["executable"] for row in scenarios),
        "missing_cost_field_count": sum(
            row[field] is None
            for row in scenarios
            for field in MISSING_COST_FIELDS
        ),
    }


def snapshot_sha256(payload: dict) -> str:
    if not isinstance(payload, dict):
        raise ResearchContractError("research snapshot is invalid")
    body = dict(payload)
    body.pop("snapshot_sha256", None)
    return hashlib.sha256(
        b"shib-v2-research-snapshot/v1\n" + canonical_json_bytes(body)
    ).hexdigest()


def build_research_snapshot(
    evidence: dict,
    registry: dict,
    application_sha: str,
) -> dict:
    """Replay the two fixed SHIB V2/V2 routes from authenticated evidence."""
    registry = load_research_registry(registry)
    evidence = validate_research_evidence(evidence, registry)
    if (
        not isinstance(application_sha, str)
        or _GIT_SHA.fullmatch(application_sha) is None
    ):
        raise ResearchContractError("application SHA is invalid")

    block = evidence["block"]
    block_time = block["timestamp_utc"]
    token_by_symbol = {token["symbol"]: token for token in evidence["tokens"]}
    shib = token_by_symbol["SHIB"]
    weth = token_by_symbol["WETH"]
    pool_by_identity = {
        (pool["dex"], pool["pair_address"]): pool for pool in evidence["pools"]
    }
    states = {
        identity: _pool_state(pool, block)
        for identity, pool in pool_by_identity.items()
    }
    rules = {
        identity: _market_rules(pool, block)
        for identity, pool in pool_by_identity.items()
    }
    eth_usd = Fraction(
        evidence["usd_reference"]["answer"],
        10 ** evidence["usd_reference"]["decimals"],
    )
    if eth_usd <= 0:
        raise ResearchContractError("ETH/USD reference is invalid")
    references = {
        identity: _pool_reference_shib_usd(
            pool, shib["address"], weth["address"], eth_usd
        )
        for identity, pool in pool_by_identity.items()
    }

    scenarios = []
    for route_id, buy_identity, sell_identity in _FIXED_ROUTES:
        buy_reference = references[buy_identity]
        sell_reference = references[sell_identity]
        common_reference = max(buy_reference, sell_reference)
        for notional_text in _REQUESTED_NOTIONALS:
            target = _common_target(int(notional_text), common_reference)
            buy_quote = _quote_for_scenario(
                states[buy_identity], target, rules[buy_identity], "buy",
                shib["address"], weth["address"], block_time,
            )
            sell_quote = _quote_for_scenario(
                states[sell_identity], target, rules[sell_identity], "sell",
                shib["address"], weth["address"], block_time,
            )
            both_complete = (
                buy_quote.status == "calculation_complete"
                and sell_quote.status == "calculation_complete"
            )
            if both_complete:
                buy_weth_raw = _quote_weth_raw(buy_quote)
                sell_weth_raw = _quote_weth_raw(sell_quote)
                gross_edge_weth_raw = sell_weth_raw - buy_weth_raw
                buy_cost_usd = Fraction(buy_weth_raw, 10**18) * eth_usd
                sell_proceeds_usd = Fraction(sell_weth_raw, 10**18) * eth_usd
                gross_edge_usd = Fraction(gross_edge_weth_raw, 10**18) * eth_usd
                gross_edge_bps = Fraction(
                    gross_edge_weth_raw * 10000, buy_weth_raw
                )
                classification = (
                    "positive_pool_edge_costs_incomplete"
                    if gross_edge_weth_raw > 0
                    else "non_positive_pool_edge"
                )
                quote_values = {
                    "buy_weth_raw": buy_weth_raw,
                    "sell_weth_raw": sell_weth_raw,
                    "gross_edge_weth_raw": gross_edge_weth_raw,
                    "buy_cost_usd": _ratio(buy_cost_usd),
                    "sell_proceeds_usd": _ratio(sell_proceeds_usd),
                    "gross_edge_usd": _ratio(gross_edge_usd),
                    "gross_edge_bps": _ratio(gross_edge_bps),
                }
            else:
                classification = "unavailable"
                quote_values = {
                    "buy_weth_raw": None,
                    "sell_weth_raw": None,
                    "gross_edge_weth_raw": None,
                    "buy_cost_usd": None,
                    "sell_proceeds_usd": None,
                    "gross_edge_usd": None,
                    "gross_edge_bps": None,
                }
            scenario = {
                "route_id": route_id,
                "buy_dex": buy_identity[0],
                "buy_pair_address": buy_identity[1],
                "sell_dex": sell_identity[0],
                "sell_pair_address": sell_identity[1],
                "requested_notional_usd": notional_text,
                "buy_pool_reference_shib_usd": _ratio(buy_reference),
                "sell_pool_reference_shib_usd": _ratio(sell_reference),
                "common_target_reference_shib_usd": _ratio(common_reference),
                "common_shib_raw": target.raw_quantity,
                "buy_pool_state_id": states[buy_identity].state_id,
                "sell_pool_state_id": states[sell_identity].state_id,
                "buy_quote_status": buy_quote.status,
                "sell_quote_status": sell_quote.status,
                "buy_quote_reason": buy_quote.reason_code,
                "sell_quote_reason": sell_quote.reason_code,
                "classification": classification,
                "reason_codes": _scenario_reason_codes(
                    buy_quote, sell_quote, classification
                ),
                "strict_eligible": False,
                "executable": False,
                "limitations": list(LIMITATIONS),
            }
            scenario.update(quote_values)
            scenario.update({field: None for field in MISSING_COST_FIELDS})
            scenarios.append(scenario)

    pool_identities = []
    for identity in _FIXED_POOLS:
        pool = pool_by_identity[identity]
        pool_identities.append({
            "dex": pool["dex"],
            "pair_address": pool["pair_address"],
            "state_id": states[identity].state_id,
            "call_results_sha256": pool["call_results_sha256"],
            "fee_evidence_sha256": pool["fee_evidence_sha256"],
            "reserve_timestamp_last_raw": pool["reserve_timestamp_last_raw"],
            "reserve_lag_seconds": pool["reserve_lag_seconds"],
        })
    snapshot = {
        "schema": SNAPSHOT_SCHEMA,
        "application_sha": application_sha,
        "registry_sha256": registry_sha256(registry),
        "evidence_identity": evidence["evidence_identity"],
        "as_of_block_number": block["number"],
        "as_of_block_hash": block["hash"],
        "as_of_utc": block_time,
        "mode": "historical_replay",
        "token": {
            "symbol": "SHIB",
            "address": shib["address"],
            "decimals": shib["decimals"],
        },
        "quote_asset": {
            "symbol": "WETH",
            "address": weth["address"],
            "decimals": weth["decimals"],
        },
        "requested_notionals_usd": list(_REQUESTED_NOTIONALS),
        "pool_identities": pool_identities,
        "scenario_count": len(scenarios),
        "summary": _snapshot_summary(scenarios),
        "scenarios": scenarios,
    }
    snapshot["snapshot_sha256"] = snapshot_sha256(snapshot)
    return validate_research_snapshot(snapshot)


def _validate_quote_status_reason(status: object, reason: object, label: str) -> None:
    if status == "calculation_complete":
        if reason != _COMPLETE_QUOTE_REASON:
            raise ResearchContractError("{} complete reason is invalid".format(label))
    elif status == "unavailable":
        if reason not in _UNAVAILABLE_QUOTE_REASONS:
            raise ResearchContractError("{} unavailable reason is invalid".format(label))
    else:
        raise ResearchContractError("{} status is invalid".format(label))


def _validate_scenario(
    value: object,
    expected_route: tuple,
    expected_notional: str,
    state_ids: Dict[tuple, str],
) -> dict:
    row = _exact_fields(value, _SCENARIO_FIELDS, "research scenario")
    route_id, buy_identity, sell_identity = expected_route
    exact_identity = (
        row["route_id"],
        (row["buy_dex"], row["buy_pair_address"]),
        (row["sell_dex"], row["sell_pair_address"]),
    )
    if exact_identity != (route_id, buy_identity, sell_identity):
        raise ResearchContractError("research scenario route is invalid")
    if row["requested_notional_usd"] != expected_notional:
        raise ResearchContractError("research scenario notional is invalid")
    buy_reference = _validated_ratio(
        row["buy_pool_reference_shib_usd"], "buy pool reference", positive=True
    )
    sell_reference = _validated_ratio(
        row["sell_pool_reference_shib_usd"], "sell pool reference", positive=True
    )
    common_reference = _validated_ratio(
        row["common_target_reference_shib_usd"],
        "common target reference",
        positive=True,
    )
    if common_reference != max(buy_reference, sell_reference):
        raise ResearchContractError("common target reference does not recompute")
    expected_target = _common_target(int(expected_notional), common_reference)
    if row["common_shib_raw"] != expected_target.raw_quantity:
        raise ResearchContractError("common SHIB quantity does not recompute")
    if (
        row["buy_pool_state_id"] != state_ids[buy_identity]
        or row["sell_pool_state_id"] != state_ids[sell_identity]
    ):
        raise ResearchContractError("scenario pool state identity is invalid")
    _validate_quote_status_reason(
        row["buy_quote_status"], row["buy_quote_reason"], "buy quote"
    )
    _validate_quote_status_reason(
        row["sell_quote_status"], row["sell_quote_reason"], "sell quote"
    )
    if row["strict_eligible"] is not False or row["executable"] is not False:
        raise ResearchContractError("research scenario cannot be executable")
    if row["limitations"] != list(LIMITATIONS):
        raise ResearchContractError("research scenario limitations are invalid")
    if any(row[field] is not None for field in MISSING_COST_FIELDS):
        raise ResearchContractError("research scenario missing costs must be null")

    both_complete = (
        row["buy_quote_status"] == "calculation_complete"
        and row["sell_quote_status"] == "calculation_complete"
    )
    if both_complete:
        buy_raw = _positive_int(row["buy_weth_raw"], "buy WETH raw")
        sell_raw = _nonnegative_int(row["sell_weth_raw"], "sell WETH raw")
        if row["gross_edge_weth_raw"] != sell_raw - buy_raw:
            raise ResearchContractError("gross WETH edge does not recompute")
        gross_raw = row["gross_edge_weth_raw"]
        if type(gross_raw) is not int:
            raise ResearchContractError("gross WETH edge is invalid")
        buy_cost = _validated_ratio(row["buy_cost_usd"], "buy cost", positive=True)
        sell_proceeds = _validated_ratio(
            row["sell_proceeds_usd"], "sell proceeds", positive=True
        )
        gross_edge = _validated_ratio(row["gross_edge_usd"], "gross USD edge")
        gross_bps = _validated_ratio(row["gross_edge_bps"], "gross edge bps")
        usd_per_weth_raw = buy_cost / buy_raw
        if (
            sell_proceeds != sell_raw * usd_per_weth_raw
            or gross_edge != gross_raw * usd_per_weth_raw
            or gross_bps != Fraction(gross_raw * 10000, buy_raw)
        ):
            raise ResearchContractError("research scenario USD arithmetic is invalid")
        expected_classification = (
            "positive_pool_edge_costs_incomplete"
            if gross_raw > 0
            else "non_positive_pool_edge"
        )
        expected_reasons = [_COMPLETE_QUOTE_REASON]
        if gross_raw > 0:
            expected_reasons.append("route_costs_not_evaluated")
    else:
        for field in (
            "buy_weth_raw", "sell_weth_raw", "gross_edge_weth_raw",
            "buy_cost_usd", "sell_proceeds_usd", "gross_edge_usd",
            "gross_edge_bps",
        ):
            if row[field] is not None:
                raise ResearchContractError("unavailable quote values must be null")
        expected_classification = "unavailable"
        expected_reasons = []
        for status_field, reason_field in (
            ("buy_quote_status", "buy_quote_reason"),
            ("sell_quote_status", "sell_quote_reason"),
        ):
            if row[status_field] == "unavailable" and row[reason_field] not in expected_reasons:
                expected_reasons.append(row[reason_field])
    if row["classification"] != expected_classification:
        raise ResearchContractError("research scenario classification is invalid")
    if row["reason_codes"] != expected_reasons:
        raise ResearchContractError("research scenario reason codes are invalid")
    return row


def validate_research_snapshot(payload: object) -> dict:
    """Validate one deterministic, non-executable historical replay snapshot."""
    snapshot = _exact_fields(
        payload,
        (
            "schema", "application_sha", "registry_sha256", "evidence_identity",
            "as_of_block_number", "as_of_block_hash", "as_of_utc", "mode",
            "token", "quote_asset", "requested_notionals_usd",
            "pool_identities", "scenario_count", "summary", "scenarios",
            "snapshot_sha256",
        ),
        "research snapshot",
    )
    if snapshot["schema"] != SNAPSHOT_SCHEMA:
        raise ResearchContractError("research snapshot schema is invalid")
    if (
        not isinstance(snapshot["application_sha"], str)
        or _GIT_SHA.fullmatch(snapshot["application_sha"]) is None
    ):
        raise ResearchContractError("application SHA is invalid")
    _sha256(snapshot["registry_sha256"], "snapshot registry hash")
    _sha256(snapshot["evidence_identity"], "snapshot evidence identity")
    _positive_int(snapshot["as_of_block_number"], "snapshot block number")
    _hash32(snapshot["as_of_block_hash"], "snapshot block hash")
    if snapshot["as_of_block_hash"] == "0x" + "00" * 32:
        raise ResearchContractError("snapshot block hash must be nonzero")
    if not isinstance(snapshot["as_of_utc"], str):
        raise ResearchContractError("snapshot UTC is invalid")
    try:
        parsed_utc = datetime.strptime(
            snapshot["as_of_utc"], "%Y-%m-%dT%H:%M:%SZ"
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
    except ValueError as error:
        raise ResearchContractError("snapshot UTC is invalid") from error
    if parsed_utc != snapshot["as_of_utc"] or snapshot["mode"] != "historical_replay":
        raise ResearchContractError("snapshot time or mode is invalid")
    token = _exact_fields(
        snapshot["token"], ("symbol", "address", "decimals"), "snapshot token"
    )
    quote_asset = _exact_fields(
        snapshot["quote_asset"], ("symbol", "address", "decimals"),
        "snapshot quote asset",
    )
    expected_token = {
        "symbol": "SHIB",
        "address": "0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce",
        "decimals": 18,
    }
    expected_quote = {
        "symbol": "WETH",
        "address": "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
        "decimals": 18,
    }
    if token != expected_token or quote_asset != expected_quote:
        raise ResearchContractError("snapshot asset identity is invalid")
    if snapshot["requested_notionals_usd"] != list(_REQUESTED_NOTIONALS):
        raise ResearchContractError("snapshot notionals are invalid")
    if (
        not isinstance(snapshot["pool_identities"], list)
        or len(snapshot["pool_identities"]) != 2
    ):
        raise ResearchContractError("snapshot pool identities are invalid")
    state_ids = {}
    for value, expected_identity in zip(snapshot["pool_identities"], _FIXED_POOLS):
        pool = _exact_fields(
            value,
            (
                "dex", "pair_address", "state_id", "call_results_sha256",
                "fee_evidence_sha256", "reserve_timestamp_last_raw",
                "reserve_lag_seconds",
            ),
            "snapshot pool identity",
        )
        if (pool["dex"], pool["pair_address"]) != expected_identity:
            raise ResearchContractError("snapshot pool identity is invalid")
        if (
            not isinstance(pool["state_id"], str)
            or _V2_STATE_ID.fullmatch(pool["state_id"]) is None
        ):
            raise ResearchContractError("snapshot pool state ID is invalid")
        _sha256(pool["call_results_sha256"], "pool call-results hash")
        _sha256(pool["fee_evidence_sha256"], "pool fee-evidence hash")
        _uint(pool["reserve_timestamp_last_raw"], 32, "pool reserve timestamp")
        _uint(pool["reserve_lag_seconds"], 32, "pool reserve lag")
        state_ids[expected_identity] = pool["state_id"]
    if snapshot["scenario_count"] != 10 or type(snapshot["scenario_count"]) is not int:
        raise ResearchContractError("snapshot scenario count is invalid")
    if (
        not isinstance(snapshot["scenarios"], list)
        or len(snapshot["scenarios"]) != 10
    ):
        raise ResearchContractError("snapshot scenarios are invalid")
    index = 0
    scenarios = []
    pool_references = {}
    usd_per_weth_raw = None
    for route in _FIXED_ROUTES:
        for notional in _REQUESTED_NOTIONALS:
            scenario = _validate_scenario(
                snapshot["scenarios"][index], route, notional, state_ids
            )
            for identity, field in (
                (route[1], "buy_pool_reference_shib_usd"),
                (route[2], "sell_pool_reference_shib_usd"),
            ):
                reference = _validated_ratio(
                    scenario[field], "scenario pool reference", positive=True
                )
                prior = pool_references.setdefault(identity, reference)
                if reference != prior:
                    raise ResearchContractError(
                        "scenario pool reference is inconsistent"
                    )
            if scenario["classification"] != "unavailable":
                scenario_conversion = _validated_ratio(
                    scenario["buy_cost_usd"], "scenario buy cost", positive=True
                ) / scenario["buy_weth_raw"]
                if usd_per_weth_raw is None:
                    usd_per_weth_raw = scenario_conversion
                elif scenario_conversion != usd_per_weth_raw:
                    raise ResearchContractError(
                        "scenario USD conversion is inconsistent"
                    )
            scenarios.append(scenario)
            index += 1
    summary = _exact_fields(
        snapshot["summary"],
        (
            "expected_scenario_count", "observed_scenario_count",
            "usable_scenario_count", "classification_counts",
            "strict_eligible_count", "executable_count",
            "missing_cost_field_count",
        ),
        "snapshot summary",
    )
    counts = _exact_fields(
        summary["classification_counts"],
        (
            "non_positive_pool_edge",
            "positive_pool_edge_costs_incomplete",
            "unavailable",
        ),
        "snapshot classification counts",
    )
    for field in (
        "expected_scenario_count", "observed_scenario_count",
        "usable_scenario_count", "strict_eligible_count", "executable_count",
        "missing_cost_field_count",
    ):
        _nonnegative_int(summary[field], "snapshot summary " + field)
    for classification, count in counts.items():
        _nonnegative_int(count, "snapshot classification " + classification)
    if summary != _snapshot_summary(scenarios):
        raise ResearchContractError("snapshot summary does not recompute")
    _sha256(snapshot["snapshot_sha256"], "snapshot self-hash")
    if snapshot["snapshot_sha256"] != snapshot_sha256(snapshot):
        raise ResearchContractError("snapshot self-hash does not recompute")
    scan_public_payload(snapshot)
    return json.loads(canonical_json_bytes(snapshot).decode("utf-8"))


def scan_public_payload(payload: object) -> None:
    """Reject keys and free text that are unsafe to publish in research data."""
    def scan(value: object) -> None:
        if isinstance(value, dict):
            for key, child in value.items():
                if not isinstance(key, str):
                    raise ResearchContractError("public payload key is invalid")
                normalized_key = re.sub(r"[^a-z0-9]", "", key.lower())
                if normalized_key in _FORBIDDEN_KEY_NORMALIZED:
                    raise ResearchContractError("public payload contains forbidden key")
                scan(child)
        elif isinstance(value, list):
            for child in value:
                scan(child)
        elif isinstance(value, str):
            if _UNSAFE_TEXT.search(value):
                raise ResearchContractError("public payload contains unsafe text")
            return
        elif value is None or isinstance(value, (bool, int)):
            return
        elif isinstance(value, float):
            raise ResearchContractError("public payload contains binary float")
        else:
            raise ResearchContractError("public payload value is invalid")

    scan(payload)
