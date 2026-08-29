"""Pure contracts for SHIB V2/V2 historical research evidence."""

from __future__ import annotations

import hashlib
import json
import re
from typing import Dict, Sequence


REGISTRY_SCHEMA = "shib_v2_research_registry/v1"
_ADDRESS = re.compile(r"0x[0-9a-f]{40}$")
_SHA256 = re.compile(r"[0-9a-f]{64}$")
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
    expected = {
        "SHIB": (
            "0x95ad61b0a150d79219dcf64e1e6cc01f0b64c4ce",
            4852,
            "5c813da8be193a1a33a7533edc758e3ad29f1fa1730cbf2d8c9fc8a7f31c78f3",
        ),
        "WETH": (
            "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
            3124,
            "5566bf50796faf93c9b6f6adacd3b32c70bfe16b48ffc59db6cd144cbdc89739",
        ),
    }
    for symbol, (address, size, code_hash) in expected.items():
        token = registry["tokens"][symbol]
        if (
            token["address"],
            token["decimals"],
            token["runtime_code_size_bytes"],
            token["runtime_code_sha256"],
        ) != (address, 18, size, code_hash):
            raise ResearchContractError(symbol + " authority is invalid")

    expected_pools = (
        (
            "uniswap_v2",
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
            {"kind": "runtime_code_bound"},
        ),
        (
            "shibaswap_v1",
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
            {
                "kind": "pair_native_parameters",
                "target": "pair",
                "native_fee_denominator": 1000,
                "total_fee": 3,
                "alpha": 1,
                "beta": 3,
            },
        ),
    )
    actual_pools = tuple(
        (
            pool["dex"],
            (
                pool["factory"]["address"],
                pool["factory"]["runtime_code_size_bytes"],
                pool["factory"]["runtime_code_sha256"],
            ),
            (
                pool["router"]["address"],
                pool["router"]["runtime_code_size_bytes"],
                pool["router"]["runtime_code_sha256"],
            ),
            (
                pool["pair"]["address"],
                pool["pair"]["runtime_code_size_bytes"],
                pool["pair"]["runtime_code_sha256"],
            ),
            pool["fee_model"]["evidence"],
        )
        for pool in registry["pools"]
    )
    if actual_pools != expected_pools:
        raise ResearchContractError("pool authorities are invalid")

    reference = registry["usd_reference"]
    if (
        reference["proxy_address"],
        reference["runtime_code_size_bytes"],
        reference["runtime_code_sha256"],
        reference["decimals"],
        reference["max_age_seconds"],
    ) != (
        "0x5f4ec3df9cbd43714fe2740f5e3616155c5b8419",
        9571,
        "ed698309290de3517c7201fcad9a9dbd4b8cde4a72c9add23129201f299c6f2b",
        8,
        3600,
    ):
        raise ResearchContractError("USD reference authority is invalid")


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
