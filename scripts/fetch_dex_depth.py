"""Collect auditable point-in-time DEX pool-state depth snapshots.

The collector measures how much quote notional can trade against one pool
before its *marginal* price reaches 10/25/50/100 bps from the starting price.
That makes the result analogous to consuming a CEX order book up to a price
band.  It never derives depth from TVL or historical volume.

Supported pool-state models in this first release:

- Uniswap V2 and SushiSwap V2 constant-product pools;
- Uniswap V3-compatible concentrated-liquidity pools whose contracts expose
  ``slot0``, ``liquidity``, ``fee``, ``tickSpacing``, ``tickBitmap`` and
  ``ticks``.

Every inventory pool receives an explicit observed/partial/unsupported/failed
row.  All EVM calls for a chain use one fixed block tag, and the raw JSON-RPC
request/response transcript is retained with a SHA-256 hash.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import os
import shutil
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from collections import Counter
from decimal import Decimal, InvalidOperation, ROUND_FLOOR, localcontext
from pathlib import Path
from typing import Any, Callable, Iterable

try:
    import certifi
except ImportError:  # pragma: no cover - system trust remains the safe fallback
    certifi = None


PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_TVL_CSV = PROJECT_ROOT / "data/local/dex_pool_tvl_latest.csv"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "data/processed"
DEFAULT_PUBLISH_DIR = PROJECT_ROOT / "data/local"
DEFAULT_RAW_ROOT = PROJECT_ROOT / "data/raw/dex-depth"

CURRENT_FILENAME = "dex_depth_snapshot.csv"
LATEST_FILENAME = "dex_depth_latest.csv"
HISTORY_FILENAME = "dex_depth_history.csv"

DEPTH_BANDS_BPS = (10, 25, 50, 100)
DEX_DEPTH_METHOD = "fixed_block_pool_state_marginal_price_band"
REQUEST_SLEEP_SECONDS = 0.15
MAX_RETRIES = 4
Q96 = Decimal(2**96)
ONE_MILLION = Decimal(1_000_000)
TLS_CONTEXT = (
    ssl.create_default_context(cafile=certifi.where())
    if certifi
    else ssl.create_default_context()
)

DEFAULT_RPC_URLS = {
    "eth": "https://ethereum-rpc.publicnode.com",
    "arbitrum": "https://arb1.arbitrum.io/rpc",
    "optimism": "https://mainnet.optimism.io",
    # Base documents mainnet.base.org as rate-limited and not suitable for
    # production systems. PublicNode is the keyless default; operators can
    # override it with DEX_DEPTH_RPC_BASE.
    "base": "https://base-rpc.publicnode.com",
    "bsc": "https://bsc-dataseed.bnbchain.org",
    "zksync": "https://mainnet.era.zksync.io",
}
RPC_ENV_KEYS = {
    chain: f"DEX_DEPTH_RPC_{chain.upper()}"
    for chain in DEFAULT_RPC_URLS
}

V2_FEE_BPS = {
    "uniswap_v2": Decimal("30"),
    "sushiswap": Decimal("30"),
    "shibaswap": Decimal("30"),
    "pancakeswap_v2": Decimal("25"),
    "pancakeswap-v2-zksync": Decimal("25"),
}
V3_DEXES = {
    "aerodrome-slipstream",
    "uniswap_v3",
    "uniswap_v3_arbitrum",
    "uniswap_v3_optimism",
    "uniswap-v3-base",
    "uniswap-v3-zksync",
    "sushiswap-v3-ethereum",
    "pancakeswap-v3-bsc",
    "velodrome-finance-slipstream",
}

# Ethereum ABI selectors.  The signatures are documented in the protocol
# interfaces cited by docs/dex-depth-data-contract.md.
SELECTOR_TOKEN0 = "0x0dfe1681"
SELECTOR_TOKEN1 = "0xd21220a7"
SELECTOR_GET_RESERVES = "0x0902f1ac"
SELECTOR_DECIMALS = "0x313ce567"
SELECTOR_SYMBOL = "0x95d89b41"
SELECTOR_SLOT0 = "0x3850c7bd"
SELECTOR_LIQUIDITY = "0x1a686502"
SELECTOR_FEE = "0xddca3f43"
SELECTOR_TICK_SPACING = "0xd0c93a7c"
SELECTOR_TICK_BITMAP = "0x5339c296"
SELECTOR_TICKS = "0xf30dba93"

BASE_COLUMNS = [
    "snapshot_id",
    "observed_at",
    "request_started_at",
    "response_received_at",
    "token_symbol",
    "chain",
    "dex",
    "pool_address",
    "pool_name",
    "protocol_model",
    "block_number",
    "target_token_address",
    "target_token_position",
    "token0_address",
    "token0_symbol",
    "token0_decimals",
    "token0_price_usd",
    "token1_address",
    "token1_symbol",
    "token1_decimals",
    "token1_price_usd",
    "fee_bps",
    "pool_state_price_usd",
    "source_target_price_usd",
    "price_difference_bps",
]
DEPTH_COLUMNS = [
    field
    for band in DEPTH_BANDS_BPS
    for field in (
        f"sell_depth_{band}bps_usd",
        f"buy_depth_{band}bps_usd",
        f"total_depth_{band}bps_usd",
        f"depth_{band}bps_complete",
    )
]
TRAILING_COLUMNS = [
    "depth_method",
    "source",
    "source_endpoint",
    "raw_response_sha256",
    "status",
    "error",
]
DEX_DEPTH_COLUMNS = BASE_COLUMNS + DEPTH_COLUMNS + TRAILING_COLUMNS


class RpcError(RuntimeError):
    """Raised when a JSON-RPC endpoint returns no usable result."""


def utc_now_text() -> str:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc).isoformat()


def pool_key(chain: str, pool_address: str) -> tuple[str, str]:
    address = pool_address.strip()
    if address.startswith("0x"):
        address = address.lower()
    return chain.strip().lower(), address


def finite_decimal(value: Any, *, positive: bool = False) -> Decimal:
    try:
        number = Decimal(str(value))
    except (InvalidOperation, ValueError) as error:
        raise ValueError(f"Invalid decimal value: {value}") from error
    if not number.is_finite() or (positive and number <= 0):
        raise ValueError(f"Invalid {'positive ' if positive else ''}decimal value: {value}")
    return number


def decimal_text(value: Decimal | int | float | str | None) -> str:
    if value is None:
        return ""
    number = finite_decimal(value)
    if number == 0:
        return "0"
    return format(number.normalize(), "f")


def bool_text(value: bool) -> str:
    return "1" if value else "0"


def address_from_token_id(value: str) -> str:
    token_id = value.strip()
    if "_" not in token_id:
        return ""
    address = token_id.split("_", 1)[1]
    return address.lower() if address.startswith("0x") else address


def sanitize_endpoint(url: str) -> str:
    parsed = urllib.parse.urlsplit(url)
    host = parsed.hostname or ""
    port = f":{parsed.port}" if parsed.port else ""
    path = parsed.path or ""
    return urllib.parse.urlunsplit((parsed.scheme, host + port, path, "", ""))


def rpc_url_for_chain(chain: str) -> str | None:
    normalized = chain.lower()
    configured = os.environ.get(RPC_ENV_KEYS.get(normalized, ""))
    return configured or DEFAULT_RPC_URLS.get(normalized)


def protocol_model(dex: str, chain: str, pool_address: str) -> tuple[str, str]:
    normalized = dex.lower()
    if chain.lower() not in DEFAULT_RPC_URLS:
        return "unsupported", f"unsupported_chain:{chain.lower()}"
    if not (
        pool_address.startswith("0x")
        and len(pool_address) == 42
        and all(character in "0123456789abcdefABCDEF" for character in pool_address[2:])
    ):
        return "unsupported", "pool_is_not_an_evm_contract_address"
    if normalized in V2_FEE_BPS:
        return "constant_product_v2", ""
    if normalized in V3_DEXES:
        return "concentrated_liquidity_v3", ""
    return "unsupported", f"unsupported_pool_model:{normalized}"


def load_pool_inventory(path: Path = DEFAULT_TVL_CSV) -> list[dict[str, str]]:
    if not path.exists():
        raise FileNotFoundError(f"TVL inventory does not exist: {path}")
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        required = {
            "snapshot_id",
            "observed_at",
            "token_symbol",
            "chain",
            "dex",
            "pool_address",
            "pool_name",
            "base_token_id",
            "quote_token_id",
            "base_token_price_usd",
            "quote_token_price_usd",
            "status",
        }
        missing = sorted(required - set(reader.fieldnames or []))
        if missing:
            raise ValueError(f"{path.name} is missing columns: {', '.join(missing)}")
        rows = list(reader)
    if not rows:
        raise ValueError(f"{path.name} contains no pool rows")

    latest: dict[tuple[str, str, str], dict[str, str]] = {}
    for row in rows:
        key = (
            row["token_symbol"].upper(),
            *pool_key(row["chain"], row["pool_address"]),
        )
        timestamp = row.get("response_received_at") or row.get("observed_at") or ""
        previous = latest.get(key)
        previous_timestamp = (
            previous.get("response_received_at") or previous.get("observed_at") or ""
            if previous
            else ""
        )
        if previous is None or timestamp >= previous_timestamp:
            latest[key] = row
    return sorted(
        latest.values(),
        key=lambda row: (
            row["chain"].lower(),
            row["token_symbol"].upper(),
            row["dex"].lower(),
            row["pool_address"].lower(),
        ),
    )


def http_json_rpc(url: str, payload: Any) -> tuple[Any, bytes]:
    encoded = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    request = urllib.request.Request(
        url,
        data=encoded,
        headers={
            "Content-Type": "application/json",
            "Accept": "application/json",
            "User-Agent": "CEX-DEX-Market-Monitor/2.0",
        },
        method="POST",
    )
    for attempt in range(MAX_RETRIES):
        try:
            with urllib.request.urlopen(
                request,
                timeout=30,
                context=TLS_CONTEXT,
            ) as response:
                raw = response.read()
                return json.loads(raw.decode("utf-8")), raw
        except urllib.error.HTTPError as error:
            retryable = error.code == 429 or 500 <= error.code < 600
            if not retryable or attempt + 1 >= MAX_RETRIES:
                raise
            retry_after = float(error.headers.get("Retry-After") or 0)
            time.sleep(max(retry_after, 2 ** attempt))
        except urllib.error.URLError:
            if attempt + 1 >= MAX_RETRIES:
                raise
            time.sleep(max(1.0, 2 ** attempt))
    raise RpcError(f"JSON-RPC request failed after retries: {sanitize_endpoint(url)}")


class RpcClient:
    def __init__(
        self,
        chain: str,
        url: str,
        *,
        request: Callable[[str, Any], tuple[Any, bytes]] = http_json_rpc,
    ) -> None:
        self.chain = chain
        self.url = url
        self.endpoint = sanitize_endpoint(url)
        self.request = request
        self.records: list[dict[str, Any]] = []
        self._next_id = 1

    def _send(self, payload: Any) -> Any:
        response, raw = self.request(self.url, payload)
        self.records.append(
            {
                "request": payload,
                "response": response,
                "response_sha256": hashlib.sha256(raw).hexdigest(),
            }
        )
        return response

    def method(self, method: str, params: list[Any]) -> Any:
        request_id = self._next_id
        self._next_id += 1
        payload = {
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": params,
        }
        response = self._send(payload)
        if not isinstance(response, dict):
            raise RpcError(f"{method} returned a non-object response")
        if response.get("error"):
            raise RpcError(f"{method}: {response['error']}")
        if "result" not in response:
            raise RpcError(f"{method} returned no result")
        return response["result"]

    def block_number(self) -> int:
        return int(self.method("eth_blockNumber", []), 16)

    def block(self, block_tag: str) -> dict[str, Any]:
        result = self.method("eth_getBlockByNumber", [block_tag, False])
        if not isinstance(result, dict):
            raise RpcError("eth_getBlockByNumber returned no block")
        return result

    def eth_calls(self, to: str, data_values: list[str], block_tag: str) -> list[str]:
        requests = []
        for data in data_values:
            request_id = self._next_id
            self._next_id += 1
            requests.append(
                {
                    "jsonrpc": "2.0",
                    "id": request_id,
                    "method": "eth_call",
                    "params": [{"to": to, "data": data}, block_tag],
                }
            )
        try:
            response = self._send(requests)
            if not isinstance(response, list):
                raise RpcError("Batch eth_call returned a non-list response")
            by_id = {item.get("id"): item for item in response if isinstance(item, dict)}
            results = []
            for request_item in requests:
                item = by_id.get(request_item["id"])
                if not item or item.get("error") or "result" not in item:
                    raise RpcError(f"eth_call failed: {item}")
                results.append(item["result"])
            return results
        except (RpcError, ValueError):
            results = []
            for request_item in requests:
                response_item = self._send(request_item)
                if (
                    not isinstance(response_item, dict)
                    or response_item.get("error")
                    or "result" not in response_item
                ):
                    raise RpcError(f"eth_call failed: {response_item}")
                results.append(response_item["result"])
            return results


def words(hex_data: str) -> list[str]:
    value = hex_data[2:] if hex_data.startswith("0x") else hex_data
    if len(value) % 64:
        raise ValueError("ABI response is not aligned to 32-byte words")
    return [value[index:index + 64] for index in range(0, len(value), 64)]


def decode_uint(hex_data: str, index: int = 0) -> int:
    values = words(hex_data)
    if index >= len(values):
        raise ValueError("ABI response contains too few words")
    return int(values[index], 16)


def decode_int(hex_data: str, index: int = 0, bits: int = 256) -> int:
    value = decode_uint(hex_data, index)
    sign_bit = 1 << (bits - 1)
    mask = (1 << bits) - 1
    value &= mask
    return value - (1 << bits) if value & sign_bit else value


def decode_address(hex_data: str) -> str:
    value = words(hex_data)[0][-40:]
    return "0x" + value.lower()


def decode_symbol(hex_data: str) -> str:
    values = words(hex_data)
    if not values:
        return ""
    if int(values[0], 16) == 32 and len(values) >= 2:
        length = int(values[1], 16)
        raw_hex = "".join(values[2:])[: length * 2]
    else:
        raw_hex = values[0].rstrip("0")
    try:
        return bytes.fromhex(raw_hex).decode("utf-8", errors="strict").strip("\x00")
    except (ValueError, UnicodeDecodeError):
        return ""


def encode_signed_word(value: int, bits: int) -> str:
    minimum = -(1 << (bits - 1))
    maximum = (1 << (bits - 1)) - 1
    if not minimum <= value <= maximum:
        raise ValueError(f"value is outside int{bits}")
    encoded = value if value >= 0 else (1 << 256) + value
    return f"{encoded:064x}"


def call_with_int(selector: str, value: int, bits: int) -> str:
    return selector + encode_signed_word(value, bits)


def price_map_from_inventory(row: dict[str, str]) -> dict[str, Decimal]:
    result: dict[str, Decimal] = {}
    for side in ("base", "quote"):
        address = address_from_token_id(row.get(f"{side}_token_id", ""))
        raw_price = row.get(f"{side}_token_price_usd", "")
        if address and raw_price:
            result[address.lower()] = finite_decimal(raw_price, positive=True)
    return result


def target_position(
    target_symbol: str,
    token0_symbol: str,
    token1_symbol: str,
) -> int:
    target = target_symbol.strip().upper()
    matches = [
        index
        for index, symbol in enumerate((token0_symbol, token1_symbol))
        if symbol.strip().upper() == target
    ]
    if len(matches) != 1:
        raise ValueError(
            f"target_token_not_identified:{target}:"
            f"{token0_symbol or '?'}:{token1_symbol or '?'}"
        )
    return matches[0]


def v2_band_amounts(
    reserve0: Decimal,
    reserve1: Decimal,
    fee_bps: Decimal,
    band_bps: int,
) -> dict[str, Decimal]:
    if reserve0 <= 0 or reserve1 <= 0:
        raise ValueError("V2 reserves must be positive")
    fee_fraction = fee_bps / Decimal(10_000)
    down_factor = Decimal(1) - Decimal(band_bps) / Decimal(10_000)
    up_factor = Decimal(1) + Decimal(band_bps) / Decimal(10_000)
    with localcontext() as context:
        context.prec = 90
        net0_in = reserve0 * (Decimal(1) / down_factor.sqrt() - Decimal(1))
        gross0_in = net0_in / (Decimal(1) - fee_fraction)
        new_reserve0 = reserve0 + net0_in
        token1_out = reserve1 - reserve0 * reserve1 / new_reserve0

        net1_in = reserve1 * (up_factor.sqrt() - Decimal(1))
        gross1_in = net1_in / (Decimal(1) - fee_fraction)
        new_reserve1 = reserve1 + net1_in
        token0_out = reserve0 - reserve0 * reserve1 / new_reserve1
    return {
        "zero_for_one_gross_input": gross0_in,
        "zero_for_one_output": token1_out,
        "one_for_zero_gross_input": gross1_in,
        "one_for_zero_output": token0_out,
    }


def tick_sqrt_ratio_x96(tick: int) -> Decimal:
    if not -887272 <= tick <= 887272:
        raise ValueError("tick outside Uniswap V3 bounds")
    with localcontext() as context:
        context.prec = 100
        return (
            Decimal("1.0001") ** (Decimal(tick) / Decimal(2)) * Q96
        ).to_integral_value(rounding=ROUND_FLOOR)


def v3_segment_amounts(
    sqrt_start: Decimal,
    sqrt_end: Decimal,
    liquidity: Decimal,
    fee_pips: int,
    *,
    zero_for_one: bool,
) -> tuple[Decimal, Decimal]:
    if liquidity <= 0:
        raise ValueError("V3 active liquidity is zero")
    fee_fraction = Decimal(fee_pips) / ONE_MILLION
    if not Decimal(0) <= fee_fraction < Decimal(1):
        raise ValueError("V3 fee is invalid")
    with localcontext() as context:
        context.prec = 100
        if zero_for_one:
            if sqrt_end > sqrt_start:
                raise ValueError("zero-for-one target price must be lower")
            net_input = (
                liquidity * Q96 * (sqrt_start - sqrt_end)
                / (sqrt_start * sqrt_end)
            )
            output = liquidity * (sqrt_start - sqrt_end) / Q96
        else:
            if sqrt_end < sqrt_start:
                raise ValueError("one-for-zero target price must be higher")
            net_input = liquidity * (sqrt_end - sqrt_start) / Q96
            output = (
                liquidity * Q96 * (sqrt_end - sqrt_start)
                / (sqrt_end * sqrt_start)
            )
        gross_input = net_input / (Decimal(1) - fee_fraction)
    return gross_input, output


def v3_move_to_price(
    sqrt_start: int,
    target_sqrt: Decimal,
    liquidity: int,
    fee_pips: int,
    initialized_ticks: dict[int, int],
    *,
    zero_for_one: bool,
) -> tuple[Decimal, Decimal, bool]:
    current_sqrt = Decimal(sqrt_start)
    current_liquidity = Decimal(liquidity)
    if zero_for_one:
        boundaries = sorted(
            (
                (tick, tick_sqrt_ratio_x96(tick))
                for tick in initialized_ticks
                if target_sqrt < tick_sqrt_ratio_x96(tick) <= current_sqrt
            ),
            reverse=True,
        )
    else:
        boundaries = sorted(
            (
                (tick, tick_sqrt_ratio_x96(tick))
                for tick in initialized_ticks
                if current_sqrt < tick_sqrt_ratio_x96(tick) < target_sqrt
            )
        )

    total_input = Decimal(0)
    total_output = Decimal(0)
    for tick, boundary_sqrt in boundaries:
        gross_input, output = v3_segment_amounts(
            current_sqrt,
            boundary_sqrt,
            current_liquidity,
            fee_pips,
            zero_for_one=zero_for_one,
        )
        total_input += gross_input
        total_output += output
        liquidity_net = Decimal(initialized_ticks[tick])
        current_liquidity += -liquidity_net if zero_for_one else liquidity_net
        current_sqrt = boundary_sqrt
        if current_liquidity <= 0:
            return total_input, total_output, False

    gross_input, output = v3_segment_amounts(
        current_sqrt,
        target_sqrt,
        current_liquidity,
        fee_pips,
        zero_for_one=zero_for_one,
    )
    return total_input + gross_input, total_output + output, True


def initialized_tick_range(
    current_tick: int,
    tick_spacing: int,
    max_band_bps: int = max(DEPTH_BANDS_BPS),
) -> tuple[int, int]:
    if tick_spacing <= 0:
        raise ValueError("tick spacing must be positive")
    down = abs(math.log1p(-max_band_bps / 10_000) / math.log(1.0001))
    up = abs(math.log1p(max_band_bps / 10_000) / math.log(1.0001))
    margin = 2 * tick_spacing
    return (
        math.floor(current_tick - down - margin),
        math.ceil(current_tick + up + margin),
    )


def collect_initialized_ticks(
    client: RpcClient,
    pool_address: str,
    block_tag: str,
    current_tick: int,
    tick_spacing: int,
) -> dict[int, int]:
    minimum_tick, maximum_tick = initialized_tick_range(current_tick, tick_spacing)
    minimum_word = (minimum_tick // tick_spacing) >> 8
    maximum_word = (maximum_tick // tick_spacing) >> 8
    word_positions = list(range(minimum_word, maximum_word + 1))
    bitmap_data = [
        call_with_int(SELECTOR_TICK_BITMAP, position, 16)
        for position in word_positions
    ]
    bitmap_results = client.eth_calls(pool_address, bitmap_data, block_tag)
    ticks: list[int] = []
    for word_position, result in zip(word_positions, bitmap_results):
        bitmap = decode_uint(result)
        for bit in range(256):
            if bitmap & (1 << bit):
                tick = (word_position * 256 + bit) * tick_spacing
                if minimum_tick <= tick <= maximum_tick:
                    ticks.append(tick)
    if not ticks:
        return {}
    tick_results = client.eth_calls(
        pool_address,
        [call_with_int(SELECTOR_TICKS, tick, 24) for tick in ticks],
        block_tag,
    )
    return {
        tick: decode_int(result, 1, 128)
        for tick, result in zip(ticks, tick_results)
        if decode_uint(result, 0) > 0
    }


def base_row(
    pool: dict[str, str],
    *,
    snapshot_id: str,
    request_started_at: str,
    response_received_at: str,
) -> dict[str, str]:
    row = {column: "" for column in DEX_DEPTH_COLUMNS}
    row.update(
        {
            "snapshot_id": snapshot_id,
            "observed_at": response_received_at,
            "request_started_at": request_started_at,
            "response_received_at": response_received_at,
            "token_symbol": pool["token_symbol"].upper(),
            "chain": pool["chain"].lower(),
            "dex": pool["dex"].lower(),
            "pool_address": pool["pool_address"],
            "pool_name": pool.get("pool_name", ""),
            "depth_method": DEX_DEPTH_METHOD,
            "source": "fixed-block EVM JSON-RPC eth_call",
        }
    )
    return row


def unsupported_row(
    pool: dict[str, str],
    *,
    snapshot_id: str,
    request_started_at: str,
    response_received_at: str,
    reason: str,
) -> dict[str, str]:
    row = base_row(
        pool,
        snapshot_id=snapshot_id,
        request_started_at=request_started_at,
        response_received_at=response_received_at,
    )
    row.update(
        {
            "protocol_model": "unsupported",
            "status": "unsupported",
            "error": reason,
        }
    )
    return row


def token_metadata(
    client: RpcClient,
    token_addresses: tuple[str, str],
    block_tag: str,
) -> tuple[tuple[str, int], tuple[str, int]]:
    result = []
    for address in token_addresses:
        decimals_result, symbol_result = client.eth_calls(
            address,
            [SELECTOR_DECIMALS, SELECTOR_SYMBOL],
            block_tag,
        )
        decimals = decode_uint(decimals_result)
        symbol = decode_symbol(symbol_result)
        if not 0 <= decimals <= 255:
            raise ValueError(f"invalid token decimals for {address}")
        if not symbol:
            raise ValueError(f"missing token symbol for {address}")
        result.append((symbol, decimals))
    return result[0], result[1]


def depth_fields(
    *,
    target_position_index: int,
    token0_decimals: int,
    token1_decimals: int,
    token0_price: Decimal,
    token1_price: Decimal,
    band_amounts: dict[int, dict[str, Any]],
) -> dict[str, str]:
    fields: dict[str, str] = {}
    scale0 = Decimal(10) ** token0_decimals
    scale1 = Decimal(10) ** token1_decimals
    for band, amounts in band_amounts.items():
        zero_input = amounts["zero_input"] / scale0
        zero_output = amounts["zero_output"] / scale1
        one_input = amounts["one_input"] / scale1
        one_output = amounts["one_output"] / scale0
        if target_position_index == 0:
            sell_usd = zero_output * token1_price
            buy_usd = one_input * token1_price
        else:
            sell_usd = one_output * token0_price
            buy_usd = zero_input * token0_price
        complete = bool(amounts["zero_complete"] and amounts["one_complete"])
        fields[f"sell_depth_{band}bps_usd"] = decimal_text(sell_usd)
        fields[f"buy_depth_{band}bps_usd"] = decimal_text(buy_usd)
        fields[f"total_depth_{band}bps_usd"] = decimal_text(sell_usd + buy_usd)
        fields[f"depth_{band}bps_complete"] = bool_text(complete)
    return fields


def pool_state_price_usd(
    *,
    target_position_index: int,
    raw_token1_per_token0: Decimal,
    token0_price: Decimal,
    token1_price: Decimal,
) -> Decimal:
    if raw_token1_per_token0 <= 0:
        raise ValueError("pool state price must be positive")
    return (
        raw_token1_per_token0 * token1_price
        if target_position_index == 0
        else token0_price / raw_token1_per_token0
    )


def observed_pool_row(
    pool: dict[str, str],
    *,
    snapshot_id: str,
    block_number: int,
    client: RpcClient,
    request_started_at: str,
    raw_response_sha256: str,
    protocol: str,
) -> dict[str, str]:
    block_tag = hex(block_number)
    pool_address = pool["pool_address"].lower()
    price_map = price_map_from_inventory(pool)
    if len(price_map) < 2:
        raise ValueError("TVL inventory is missing one or both token USD prices")

    if protocol == "constant_product_v2":
        token0_result, token1_result, reserves_result = client.eth_calls(
            pool_address,
            [SELECTOR_TOKEN0, SELECTOR_TOKEN1, SELECTOR_GET_RESERVES],
            block_tag,
        )
        token0 = decode_address(token0_result)
        token1 = decode_address(token1_result)
        reserve0 = Decimal(decode_uint(reserves_result, 0))
        reserve1 = Decimal(decode_uint(reserves_result, 1))
        fee_bps = V2_FEE_BPS[pool["dex"].lower()]
        sqrt_price_x96 = None
        current_tick = None
        active_liquidity = None
        initialized_ticks: dict[int, int] = {}
    else:
        (
            token0_result,
            token1_result,
            slot0_result,
            liquidity_result,
            fee_result,
            spacing_result,
        ) = client.eth_calls(
            pool_address,
            [
                SELECTOR_TOKEN0,
                SELECTOR_TOKEN1,
                SELECTOR_SLOT0,
                SELECTOR_LIQUIDITY,
                SELECTOR_FEE,
                SELECTOR_TICK_SPACING,
            ],
            block_tag,
        )
        token0 = decode_address(token0_result)
        token1 = decode_address(token1_result)
        sqrt_price_x96 = decode_uint(slot0_result, 0)
        current_tick = decode_int(slot0_result, 1, 24)
        active_liquidity = decode_uint(liquidity_result)
        fee_pips = decode_uint(fee_result)
        fee_bps = Decimal(fee_pips) / Decimal(100)
        tick_spacing = decode_int(spacing_result, 0, 24)
        if sqrt_price_x96 <= 0 or active_liquidity <= 0:
            raise ValueError("V3 pool is uninitialized or has zero active liquidity")
        initialized_ticks = collect_initialized_ticks(
            client,
            pool_address,
            block_tag,
            current_tick,
            tick_spacing,
        )
        reserve0 = reserve1 = Decimal(0)

    (token0_symbol, token0_decimals), (token1_symbol, token1_decimals) = token_metadata(
        client,
        (token0, token1),
        block_tag,
    )
    target_index = target_position(
        pool["token_symbol"],
        token0_symbol,
        token1_symbol,
    )
    if token0 not in price_map or token1 not in price_map:
        raise ValueError(
            f"pool token addresses do not match TVL inventory:{token0}:{token1}"
        )
    token0_price = price_map[token0]
    token1_price = price_map[token1]

    band_amounts: dict[int, dict[str, Any]] = {}
    if protocol == "constant_product_v2":
        for band in DEPTH_BANDS_BPS:
            amounts = v2_band_amounts(reserve0, reserve1, fee_bps, band)
            band_amounts[band] = {
                "zero_input": amounts["zero_for_one_gross_input"],
                "zero_output": amounts["zero_for_one_output"],
                "one_input": amounts["one_for_zero_gross_input"],
                "one_output": amounts["one_for_zero_output"],
                "zero_complete": True,
                "one_complete": True,
            }
        raw_ratio = (
            reserve1 / (Decimal(10) ** token1_decimals)
            / (reserve0 / (Decimal(10) ** token0_decimals))
        )
    else:
        assert sqrt_price_x96 is not None
        assert active_liquidity is not None
        assert current_tick is not None
        fee_pips = int(fee_bps * Decimal(100))
        for band in DEPTH_BANDS_BPS:
            with localcontext() as context:
                context.prec = 100
                down_target = (
                    Decimal(sqrt_price_x96)
                    * (
                        Decimal(1) - Decimal(band) / Decimal(10_000)
                    ).sqrt()
                )
                up_target = (
                    Decimal(sqrt_price_x96)
                    * (
                        Decimal(1) + Decimal(band) / Decimal(10_000)
                    ).sqrt()
                )
            zero_input, zero_output, zero_complete = v3_move_to_price(
                sqrt_price_x96,
                down_target,
                active_liquidity,
                fee_pips,
                initialized_ticks,
                zero_for_one=True,
            )
            one_input, one_output, one_complete = v3_move_to_price(
                sqrt_price_x96,
                up_target,
                active_liquidity,
                fee_pips,
                initialized_ticks,
                zero_for_one=False,
            )
            band_amounts[band] = {
                "zero_input": zero_input,
                "zero_output": zero_output,
                "one_input": one_input,
                "one_output": one_output,
                "zero_complete": zero_complete,
                "one_complete": one_complete,
            }
        with localcontext() as context:
            context.prec = 100
            raw_ratio = (
                (Decimal(sqrt_price_x96) / Q96) ** 2
                * (Decimal(10) ** (token0_decimals - token1_decimals))
            )

    response_received_at = utc_now_text()
    row = base_row(
        pool,
        snapshot_id=snapshot_id,
        request_started_at=request_started_at,
        response_received_at=response_received_at,
    )
    source_target_price = (
        token0_price if target_index == 0 else token1_price
    )
    state_price = pool_state_price_usd(
        target_position_index=target_index,
        raw_token1_per_token0=raw_ratio,
        token0_price=token0_price,
        token1_price=token1_price,
    )
    price_difference_bps = (
        abs(state_price - source_target_price)
        / ((state_price + source_target_price) / Decimal(2))
        * Decimal(10_000)
    )
    row.update(
        {
            "protocol_model": protocol,
            "block_number": str(block_number),
            "target_token_address": token0 if target_index == 0 else token1,
            "target_token_position": f"token{target_index}",
            "token0_address": token0,
            "token0_symbol": token0_symbol,
            "token0_decimals": str(token0_decimals),
            "token0_price_usd": decimal_text(token0_price),
            "token1_address": token1,
            "token1_symbol": token1_symbol,
            "token1_decimals": str(token1_decimals),
            "token1_price_usd": decimal_text(token1_price),
            "fee_bps": decimal_text(fee_bps),
            "pool_state_price_usd": decimal_text(state_price),
            "source_target_price_usd": decimal_text(source_target_price),
            "price_difference_bps": decimal_text(price_difference_bps),
            "source_endpoint": client.endpoint,
            "raw_response_sha256": raw_response_sha256,
        }
    )
    row.update(
        depth_fields(
            target_position_index=target_index,
            token0_decimals=token0_decimals,
            token1_decimals=token1_decimals,
            token0_price=token0_price,
            token1_price=token1_price,
            band_amounts=band_amounts,
        )
    )
    row["status"] = (
        "observed"
        if all(row[f"depth_{band}bps_complete"] == "1" for band in DEPTH_BANDS_BPS)
        else "partial"
    )
    return row


def raw_transcript_bytes(
    *,
    pool: dict[str, str],
    block_number: int | None,
    endpoint: str,
    records: list[dict[str, Any]],
    error: Exception | None = None,
) -> bytes:
    payload: dict[str, Any] = {
        "pool": {
            "token_symbol": pool["token_symbol"],
            "chain": pool["chain"],
            "dex": pool["dex"],
            "pool_address": pool["pool_address"],
        },
        "block_number": block_number,
        "source_endpoint": endpoint,
        "records": records,
    }
    if error is not None:
        payload["error_type"] = type(error).__name__
        payload["error"] = str(error)
    return (
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n"
    ).encode("utf-8")


def collect_dex_depth(
    pools: list[dict[str, str]],
    *,
    raw_root: Path = DEFAULT_RAW_ROOT,
    sleep_seconds: float = REQUEST_SLEEP_SECONDS,
    rpc_factory: Callable[[str, str], RpcClient] = RpcClient,
) -> tuple[str, list[dict[str, str]]]:
    from datetime import datetime, timezone

    snapshot_id = (
        datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        + "-"
        + uuid.uuid4().hex[:8]
    )
    snapshot_raw_dir = raw_root / snapshot_id
    snapshot_raw_dir.mkdir(parents=True, exist_ok=False)
    clients: dict[str, RpcClient] = {}
    blocks: dict[str, int] = {}
    rows: list[dict[str, str]] = []

    for index, pool in enumerate(pools, start=1):
        request_started_at = utc_now_text()
        protocol, unsupported_reason = protocol_model(
            pool["dex"],
            pool["chain"],
            pool["pool_address"],
        )
        if protocol == "unsupported":
            row = unsupported_row(
                pool,
                snapshot_id=snapshot_id,
                request_started_at=request_started_at,
                response_received_at=utc_now_text(),
                reason=unsupported_reason,
            )
            rows.append(row)
            print(
                f"[{index}/{len(pools)}] {pool['token_symbol']} "
                f"{pool['chain']} {pool['dex']}: unsupported",
                flush=True,
            )
            continue

        chain = pool["chain"].lower()
        rpc_url = rpc_url_for_chain(chain)
        if not rpc_url:
            row = unsupported_row(
                pool,
                snapshot_id=snapshot_id,
                request_started_at=request_started_at,
                response_received_at=utc_now_text(),
                reason=f"missing_rpc_endpoint:{chain}",
            )
            rows.append(row)
            continue
        client = clients.setdefault(chain, rpc_factory(chain, rpc_url))
        if chain not in blocks:
            blocks[chain] = client.block_number()
        block_number = blocks[chain]
        record_start = len(client.records)
        raw_path = (
            snapshot_raw_dir
            / f"{index:03d}-{chain}-{pool['token_symbol']}-{pool['dex']}.json"
        )
        try:
            row = observed_pool_row(
                pool,
                snapshot_id=snapshot_id,
                block_number=block_number,
                client=client,
                request_started_at=request_started_at,
                raw_response_sha256="",
                protocol=protocol,
            )
            transcript = raw_transcript_bytes(
                pool=pool,
                block_number=block_number,
                endpoint=client.endpoint,
                records=client.records[record_start:],
            )
            raw_path.write_bytes(transcript)
            row["raw_response_sha256"] = hashlib.sha256(transcript).hexdigest()
        except Exception as error:
            transcript = raw_transcript_bytes(
                pool=pool,
                block_number=block_number,
                endpoint=client.endpoint,
                records=client.records[record_start:],
                error=error,
            )
            raw_path.write_bytes(transcript)
            row = base_row(
                pool,
                snapshot_id=snapshot_id,
                request_started_at=request_started_at,
                response_received_at=utc_now_text(),
            )
            row.update(
                {
                    "protocol_model": protocol,
                    "block_number": str(block_number),
                    "source_endpoint": client.endpoint,
                    "raw_response_sha256": hashlib.sha256(transcript).hexdigest(),
                    "status": "failed",
                    "error": f"{type(error).__name__}: {error}",
                }
            )
        rows.append(row)
        print(
            f"[{index}/{len(pools)}] {pool['token_symbol']} "
            f"{pool['chain']} {pool['dex']}: {row['status']}",
            flush=True,
        )
        if index < len(pools) and sleep_seconds > 0:
            time.sleep(sleep_seconds)

    manifest = {
        "snapshot_id": snapshot_id,
        "generated_at": utc_now_text(),
        "pool_count": len(rows),
        "token_count": len({row["token_symbol"] for row in rows}),
        "chain_blocks": blocks,
        "depth_bands_bps": list(DEPTH_BANDS_BPS),
        "status_counts": dict(Counter(row["status"] for row in rows)),
        "raw_files": sorted(path.name for path in snapshot_raw_dir.glob("*.json")),
    }
    (snapshot_raw_dir / "manifest.json").write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    validate_snapshot(pools, rows)
    return snapshot_id, rows


def validate_snapshot(
    inventory: list[dict[str, str]],
    rows: list[dict[str, str]],
) -> None:
    expected = {
        (row["token_symbol"].upper(), *pool_key(row["chain"], row["pool_address"]))
        for row in inventory
    }
    actual = {
        (row["token_symbol"].upper(), *pool_key(row["chain"], row["pool_address"]))
        for row in rows
    }
    if len(rows) != len(actual):
        raise ValueError("DEX depth snapshot contains duplicate Token/pool rows")
    if expected != actual:
        raise ValueError("DEX depth snapshot coverage does not match the TVL inventory")
    accepted = {"observed", "partial", "unsupported", "failed"}
    if any(row["status"] not in accepted for row in rows):
        raise ValueError("DEX depth snapshot contains an invalid status")
    if not any(row["status"] in {"observed", "partial"} for row in rows):
        raise ValueError("DEX depth snapshot contains no measured pools")
    for row in rows:
        if row["status"] not in {"observed", "partial"}:
            continue
        previous = Decimal("-1")
        for band in DEPTH_BANDS_BPS:
            total = finite_decimal(row[f"total_depth_{band}bps_usd"])
            sell = finite_decimal(row[f"sell_depth_{band}bps_usd"])
            buy = finite_decimal(row[f"buy_depth_{band}bps_usd"])
            if total < 0 or sell < 0 or buy < 0:
                raise ValueError("DEX depth snapshot contains negative depth")
            if total + Decimal("1e-12") < previous:
                raise ValueError("DEX depth must be monotonic across wider bands")
            previous = total


def read_csv_rows(path: Path) -> list[dict[str, str]]:
    if not path.exists():
        return []
    with path.open("r", newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def atomic_write_csv(path: Path, rows: list[dict[str, str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=DEX_DEPTH_COLUMNS,
                lineterminator="\n",
            )
            writer.writeheader()
            writer.writerows(
                {field: row.get(field, "") for field in DEX_DEPTH_COLUMNS}
                for row in rows
            )
        os.replace(temporary, path)
    finally:
        temporary.unlink(missing_ok=True)


def publish_snapshot(
    rows: list[dict[str, str]],
    *,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    publish_dir: Path | None = None,
) -> dict[str, Any]:
    output_dir.mkdir(parents=True, exist_ok=True)
    current_path = output_dir / CURRENT_FILENAME
    atomic_write_csv(current_path, rows)
    result: dict[str, Any] = {"current_path": str(current_path), "row_count": len(rows)}
    if publish_dir is None:
        return result

    publish_dir.mkdir(parents=True, exist_ok=True)
    history_path = publish_dir / HISTORY_FILENAME
    merged = {
        (
            row.get("snapshot_id", ""),
            row.get("token_symbol", ""),
            *pool_key(row.get("chain", ""), row.get("pool_address", "")),
        ): row
        for row in read_csv_rows(history_path)
    }
    for row in rows:
        merged[
            (
                row["snapshot_id"],
                row["token_symbol"],
                *pool_key(row["chain"], row["pool_address"]),
            )
        ] = row
    history_rows = sorted(
        merged.values(),
        key=lambda row: (
            row.get("observed_at", ""),
            row.get("token_symbol", ""),
            row.get("chain", ""),
            row.get("pool_address", ""),
        ),
    )
    atomic_write_csv(history_path, history_rows)
    atomic_write_csv(publish_dir / LATEST_FILENAME, rows)
    shutil.copyfile(current_path, publish_dir / CURRENT_FILENAME)
    result.update(
        {
            "latest_path": str(publish_dir / LATEST_FILENAME),
            "history_path": str(history_path),
            "history_row_count": len(history_rows),
        }
    )
    return result


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Collect fixed-block DEX pool-state depth"
    )
    parser.add_argument("--tvl-csv", type=Path, default=DEFAULT_TVL_CSV)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    parser.add_argument("--raw-root", type=Path, default=DEFAULT_RAW_ROOT)
    parser.add_argument("--publish-local", action="store_true")
    parser.add_argument("--sleep-seconds", type=float, default=REQUEST_SLEEP_SECONDS)
    parser.add_argument("--tokens", help="Comma-separated token symbols")
    parser.add_argument("--chains", help="Comma-separated chain names")
    return parser.parse_args()


def parse_filter(value: str | None, *, upper: bool) -> set[str]:
    if not value:
        return set()
    transform = str.upper if upper else str.lower
    return {transform(item.strip()) for item in value.split(",") if item.strip()}


def main() -> None:
    args = parse_args()
    pools = load_pool_inventory(args.tvl_csv)
    tokens = parse_filter(args.tokens, upper=True)
    chains = parse_filter(args.chains, upper=False)
    if tokens:
        pools = [row for row in pools if row["token_symbol"].upper() in tokens]
    if chains:
        pools = [row for row in pools if row["chain"].lower() in chains]
    if not pools:
        raise ValueError("No DEX pools match the requested filters")

    snapshot_id, rows = collect_dex_depth(
        pools,
        raw_root=args.raw_root,
        sleep_seconds=max(0.0, args.sleep_seconds),
    )
    result = publish_snapshot(
        rows,
        output_dir=args.output_dir,
        publish_dir=DEFAULT_PUBLISH_DIR if args.publish_local else None,
    )
    counts = Counter(row["status"] for row in rows)
    result.update(
        {
            "snapshot_id": snapshot_id,
            "token_count": len({row["token_symbol"] for row in rows}),
            "pool_count": len(rows),
            "status_counts": dict(counts),
        }
    )
    print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
