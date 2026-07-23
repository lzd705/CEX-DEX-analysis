"""Fetch daily DEX volume data from GeckoTerminal.

This first version is intentionally simple:
    - Read token config from config/tokens.csv
    - Read token-chain config from config/token_chains.csv
    - Find the global top DEX pools across configured chains for each token
    - Fetch daily OHLCV for those pools
    - Write data/processed/dex_pools.csv
    - Write data/processed/dex_pool_volume_daily.csv
    - Write data/processed/dex_volume_daily.csv
"""

import argparse
import csv
import json
import time
import urllib.parse
import urllib.request
import urllib.error
from datetime import datetime
from datetime import timezone
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
TOKEN_CONFIG_PATH = PROJECT_ROOT / "config/tokens.csv"
TOKEN_CHAIN_CONFIG_PATH = PROJECT_ROOT / "config/token_chains.csv"
DEX_POOLS_OUTPUT_PATH = PROJECT_ROOT / "data/processed/dex_pools.csv"
DEX_POOL_VOLUME_OUTPUT_PATH = PROJECT_ROOT / "data/processed/dex_pool_volume_daily.csv"
DEX_VOLUME_OUTPUT_PATH = PROJECT_ROOT / "data/processed/dex_volume_daily.csv"

GECKOTERMINAL_BASE_URL = "https://api.geckoterminal.com/api/v2"
LIMIT_DAYS = 180
REQUEST_SLEEP_SECONDS = 15.0
MAX_RETRIES = 3
MIN_HISTORY_DAYS = 120
MAX_POOL_CANDIDATES = 8
TOP_POOL_COUNT = 5


def safe_float(value) -> float:
    """Convert a value to float for internal ranking, treating missing as zero."""
    if value is None:
        return 0.0
    if value == "":
        return 0.0
    return float(value)


def optional_float(value):
    """Convert a source fact to float without inventing a missing zero."""
    if value is None or value == "":
        return None
    return float(value)


def get_retry_wait_seconds(status_code, retry_after) -> int:
    """Return wait seconds for rate-limited requests."""
    if status_code != 429:
        return 0

    if retry_after is None:
        return 65

    try:
        return int(retry_after)
    except ValueError:
        return 65


def get_status_code(error):
    """Extract an HTTP status code from urllib errors or error text."""
    status_code = getattr(error, "code", None)

    if status_code is not None:
        return status_code

    if "HTTP Error 429" in str(error):
        return 429

    return None


def get_token_side(pool, chain: str, contract_address: str) -> str:
    """Return base or quote, depending on where the target token is in the pool."""
    target_id = (chain + "_" + contract_address).lower()

    if pool.get("base_token_id", "").lower() == target_id:
        return "base"

    if pool.get("quote_token_id", "").lower() == target_id:
        return "quote"

    return "base"


def sort_pools_by_volume(pools):
    """Sort GeckoTerminal pools by 24h volume, highest first."""
    def get_volume(pool):
        attributes = pool.get("attributes", {})
        volume_usd = attributes.get("volume_usd", {})
        return safe_float(volume_usd.get("h24"))

    return sorted(pools, key=get_volume, reverse=True)


def request_json(url: str):
    """Request JSON from GeckoTerminal."""
    request = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})

    attempt = 0

    while attempt < MAX_RETRIES:
        try:
            with urllib.request.urlopen(request, timeout=30) as response:
                text = response.read().decode("utf-8")
                data = json.loads(text)
                return data
        except Exception as error:
            status_code = get_status_code(error)
            retry_after = None

            if hasattr(error, "headers"):
                retry_after = error.headers.get("Retry-After")

            wait_seconds = get_retry_wait_seconds(status_code, retry_after)

            if wait_seconds <= 0:
                raise

            attempt = attempt + 1
            print("Rate limited. Waiting %s seconds before retry." % wait_seconds)
            time.sleep(wait_seconds)

    raise RuntimeError("Failed after retries: %s" % url)


def read_token_config(path: Path):
    """Read token config rows."""
    with path.open("r", newline="") as file:
        reader = csv.DictReader(file)
        rows = list(reader)

    return rows


def filter_token_rows(rows, token_symbols):
    """Keep configured tokens requested by the caller."""
    if token_symbols is None:
        return rows

    requested = set()
    for token_symbol in token_symbols:
        requested.add(token_symbol.upper())

    result = []
    for row in rows:
        if row["token_symbol"].upper() in requested:
            result.append(row)

    return result


def read_csv_rows(path):
    """Read an existing pipeline CSV if it exists."""
    if not path.exists():
        return []

    with path.open("r", newline="") as file:
        return list(csv.DictReader(file))


def replace_token_rows(existing_rows, new_rows, token_symbols):
    """Replace selected-token rows while preserving other tokens."""
    selected = set()
    for token_symbol in token_symbols:
        selected.add(token_symbol.upper())

    result = []
    for row in existing_rows:
        if row["token_symbol"].upper() not in selected:
            result.append(row)

    result.extend(new_rows)
    return result


def deduplicate_pool_volume_rows(rows):
    """Keep one row for each token-pool-date combination."""
    seen = set()
    result = []

    for row in rows:
        key = (
            row["date"],
            row["token_symbol"],
            row["chain"],
            row["pool_address"],
        )

        if key in seen:
            continue

        seen.add(key)
        result.append(row)

    return result


def group_chain_rows_by_token(rows):
    """Group token-chain config rows by token symbol."""
    grouped = {}

    for row in rows:
        token_symbol = row["token_symbol"]

        if token_symbol not in grouped:
            grouped[token_symbol] = []

        grouped[token_symbol].append(row)

    return grouped


def read_token_chain_config(path: Path, token_rows):
    """Read token-chain config, falling back to config/tokens.csv chains."""
    if path.exists():
        with path.open("r", newline="") as file:
            reader = csv.DictReader(file)
            rows = list(reader)

        return rows

    rows = []
    for token in token_rows:
        rows.append(
            {
                "token_symbol": token["token_symbol"],
                "chain": token["chain"],
                "contract_address": token["contract_address"],
                "notes": "fallback from tokens.csv",
            }
        )

    return rows


def build_pool_result(pool_data):
    """Convert GeckoTerminal pool data into our pool row."""
    attributes = pool_data.get("attributes", {})
    relationships = pool_data.get("relationships", {})
    dex_data = relationships.get("dex", {}).get("data", {})
    base_token_data = relationships.get("base_token", {}).get("data", {})
    quote_token_data = relationships.get("quote_token", {}).get("data", {})
    volume_usd = attributes.get("volume_usd", {})

    result = {
        "pool_address": attributes.get("address", ""),
        "dex": dex_data.get("id", ""),
        "pool_name": attributes.get("name", ""),
        "pool_tvl_usd": optional_float(attributes.get("reserve_in_usd")),
        "volume_24h_usd": optional_float(volume_usd.get("h24")),
        "base_token_id": base_token_data.get("id", ""),
        "quote_token_id": quote_token_data.get("id", ""),
    }

    return result


def choose_main_pool(pools):
    """Choose the highest-volume pool from GeckoTerminal pool data."""
    sorted_pools = sort_pools_by_volume(pools)

    if len(sorted_pools) == 0:
        return None

    return build_pool_result(sorted_pools[0])


def choose_top_pools(pools, pool_count):
    """Choose top pools by 24h volume from GeckoTerminal pool data."""
    sorted_pools = sort_pools_by_volume(pools)
    selected_pool_data = sorted_pools[:pool_count]

    selected_pools = []
    for pool_data in selected_pool_data:
        selected_pools.append(build_pool_result(pool_data))

    return selected_pools


def add_token_fields(pool, token, pool_rank):
    """Add token metadata to a selected pool row."""
    chain = token["chain"]
    contract_address = token["contract_address"]

    pool["token_symbol"] = token["token_symbol"]
    pool["chain"] = chain
    pool["contract_address"] = contract_address
    pool["pool_rank"] = pool_rank
    pool["ohlcv_token"] = get_token_side(pool, chain, contract_address)

    return pool


def fetch_pool_candidates_for_chain(chain_row):
    """Fetch candidate pools for one token on one chain."""
    chain = chain_row["chain"]
    contract_address = chain_row["contract_address"]

    path = "/networks/%s/tokens/%s/pools" % (chain, contract_address)
    url = GECKOTERMINAL_BASE_URL + path

    data = request_json(url)
    pools = data.get("data", [])
    sorted_pools = sort_pools_by_volume(pools)
    candidates = sorted_pools[:MAX_POOL_CANDIDATES]

    results = []
    for pool_data in candidates:
        pool = build_pool_result(pool_data)
        add_token_fields(pool, chain_row, 0)
        results.append(pool)

    return results


def find_main_pool(token):
    """Find one main pool for a token."""
    chain = token["chain"]
    contract_address = token["contract_address"]

    path = "/networks/%s/tokens/%s/pools" % (chain, contract_address)
    url = GECKOTERMINAL_BASE_URL + path

    data = request_json(url)
    pools = data.get("data", [])
    pool = choose_main_pool(pools)

    if pool is None:
        return None

    add_token_fields(pool, token, 1)

    return pool


def find_pool_with_ohlcv(token):
    """Find a pool with enough daily OHLCV history."""
    chain = token["chain"]
    contract_address = token["contract_address"]

    path = "/networks/%s/tokens/%s/pools" % (chain, contract_address)
    url = GECKOTERMINAL_BASE_URL + path

    data = request_json(url)
    pools = data.get("data", [])
    sorted_pools = sort_pools_by_volume(pools)
    candidates = sorted_pools[:MAX_POOL_CANDIDATES]

    fallback_pool = None
    fallback_ohlcv_list = []

    time.sleep(REQUEST_SLEEP_SECONDS)

    for pool_data in candidates:
        pool = build_pool_result(pool_data)
        pool["token_symbol"] = token["token_symbol"]
        pool["chain"] = chain
        pool["contract_address"] = contract_address
        pool["ohlcv_token"] = get_token_side(pool, chain, contract_address)

        try:
            ohlcv_list = fetch_pool_ohlcv(pool)
        except Exception as error:
            print("Candidate failed %s: %s" % (pool["pool_name"], error))
            time.sleep(REQUEST_SLEEP_SECONDS)
            continue

        row_count = len(ohlcv_list)

        if row_count > len(fallback_ohlcv_list):
            fallback_pool = pool
            fallback_ohlcv_list = ohlcv_list

        if row_count >= MIN_HISTORY_DAYS:
            time.sleep(REQUEST_SLEEP_SECONDS)
            return pool, ohlcv_list

        print(
            "Candidate has short history for %s: %s rows (%s)"
            % (token["token_symbol"], row_count, pool["pool_name"])
        )
        time.sleep(REQUEST_SLEEP_SECONDS)

    if fallback_pool is None:
        return None, []

    return fallback_pool, fallback_ohlcv_list


def find_top_pools_with_ohlcv(token, chain_rows):
    """Find global top pools with enough daily OHLCV history for one token."""
    candidates = []

    for chain_row in chain_rows:
        try:
            chain_candidates = fetch_pool_candidates_for_chain(chain_row)
        except Exception as error:
            print(
                "Failed candidate list for %s on %s: %s"
                % (token["token_symbol"], chain_row["chain"], error)
            )
            time.sleep(REQUEST_SLEEP_SECONDS)
            continue

        candidates.extend(chain_candidates)
        time.sleep(REQUEST_SLEEP_SECONDS)

    candidates = sorted(candidates, key=lambda pool: pool["volume_24h_usd"], reverse=True)

    selected = []
    fallback = []

    for pool in candidates:
        pool_rank = len(selected) + 1
        pool["pool_rank"] = pool_rank

        try:
            ohlcv_list = fetch_pool_ohlcv(pool)
        except Exception as error:
            print("Candidate failed %s: %s" % (pool["pool_name"], error))
            time.sleep(REQUEST_SLEEP_SECONDS)
            continue

        row_count = len(ohlcv_list)

        if row_count > 0:
            fallback.append((pool, ohlcv_list))

        if row_count >= MIN_HISTORY_DAYS:
            selected.append((pool, ohlcv_list))
            print(
                "Selected %s pool %s: %s"
                % (token["token_symbol"], len(selected), pool["pool_name"])
            )

        if len(selected) >= TOP_POOL_COUNT:
            time.sleep(REQUEST_SLEEP_SECONDS)
            return selected

        if row_count < MIN_HISTORY_DAYS:
            print(
                "Candidate has short history for %s: %s rows (%s)"
                % (token["token_symbol"], row_count, pool["pool_name"])
            )

        time.sleep(REQUEST_SLEEP_SECONDS)

    if len(selected) > 0:
        return selected

    return fallback[:TOP_POOL_COUNT]


def convert_ohlcv_row(ohlcv, pool, include_tvl_snapshot=True):
    """Convert one GeckoTerminal OHLCV list into one output CSV row."""
    timestamp = int(ohlcv[0])
    date = datetime.fromtimestamp(timestamp, timezone.utc).strftime("%Y-%m-%d")

    row = {
        "date": date,
        "token_symbol": pool["token_symbol"],
        "chain": pool["chain"],
        "dex": pool["dex"],
        "pool_address": pool["pool_address"],
        "pool_name": pool["pool_name"],
        "open": float(ohlcv[1]),
        "high": float(ohlcv[2]),
        "low": float(ohlcv[3]),
        "close": float(ohlcv[4]),
        "dex_volume_usd": float(ohlcv[5]),
        "pool_tvl_usd": pool["pool_tvl_usd"] if include_tvl_snapshot else None,
    }

    return row


def aggregate_dex_pool_rows(rows):
    """Aggregate pool-level DEX volume rows into token-date rows."""
    groups = {}

    for row in rows:
        key = (row["date"], row["token_symbol"])

        if key not in groups:
            groups[key] = {
                "date": row["date"],
                "token_symbol": row["token_symbol"],
                "dex_volume_usd": 0.0,
                "pool_addresses": set(),
                "dexes": set(),
                "chains": set(),
            }

        groups[key]["dex_volume_usd"] += float(row["dex_volume_usd"])
        groups[key]["pool_addresses"].add(row["pool_address"])
        groups[key]["dexes"].add(row["dex"])
        groups[key]["chains"].add(row["chain"])

    result = []
    for item in groups.values():
        pool_addresses = sorted(item["pool_addresses"])
        dexes = sorted(item["dexes"])
        chains = sorted(item["chains"])
        selected_chains = ";".join(chains)

        result.append(
            {
                "date": item["date"],
                "token_symbol": item["token_symbol"],
                "chain": selected_chains,
                "selected_chains": selected_chains,
                "dex_volume_usd": item["dex_volume_usd"],
                "pool_count": len(pool_addresses),
                "included_dexes": ";".join(dexes),
                "included_pool_addresses": ";".join(pool_addresses),
            }
        )

    return sorted(result, key=lambda row: (row["token_symbol"], row["date"]))


def filter_complete_dates(rows, expected_token_count):
    """Keep only dates that have all expected tokens."""
    tokens_by_date = {}

    for row in rows:
        date = row["date"]
        token_symbol = row["token_symbol"]

        if date not in tokens_by_date:
            tokens_by_date[date] = set()

        tokens_by_date[date].add(token_symbol)

    complete_dates = set()
    for date, token_symbols in tokens_by_date.items():
        if len(token_symbols) == expected_token_count:
            complete_dates.add(date)

    result = []
    for row in rows:
        if row["date"] in complete_dates:
            result.append(row)

    return sorted(result, key=lambda row: (row["token_symbol"], row["date"]))


def fetch_pool_ohlcv(pool):
    """Fetch daily OHLCV for one pool."""
    chain = pool["chain"]
    pool_address = pool["pool_address"]

    query = {
        "aggregate": "1",
        "limit": str(LIMIT_DAYS),
        "currency": "usd",
        "token": pool.get("ohlcv_token", "base"),
    }

    encoded_query = urllib.parse.urlencode(query)
    path = "/networks/%s/pools/%s/ohlcv/day" % (chain, pool_address)
    url = GECKOTERMINAL_BASE_URL + path + "?" + encoded_query

    data = request_json(url)
    attributes = data.get("data", {}).get("attributes", {})
    ohlcv_list = attributes.get("ohlcv_list", [])

    return ohlcv_list


def write_pool_rows(pools, output_path: Path):
    """Write selected pools to CSV."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "token_symbol",
        "chain",
        "contract_address",
        "pool_rank",
        "dex",
        "pool_address",
        "pool_name",
        "pool_tvl_usd",
        "volume_24h_usd",
        "ohlcv_token",
        "base_token_id",
        "quote_token_id",
    ]

    with output_path.open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(pools)


def write_pool_volume_rows(rows, output_path: Path):
    """Write pool-level DEX volume rows to CSV."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "date",
        "token_symbol",
        "chain",
        "dex",
        "pool_address",
        "pool_name",
        "open",
        "high",
        "low",
        "close",
        "dex_volume_usd",
        "pool_tvl_usd",
    ]

    rows = sorted(rows, key=lambda row: (row["token_symbol"], row["date"]))

    with output_path.open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def write_volume_rows(rows, output_path: Path):
    """Write aggregated DEX volume rows to CSV."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = [
        "date",
        "token_symbol",
        "chain",
        "selected_chains",
        "dex_volume_usd",
        "pool_count",
        "included_dexes",
        "included_pool_addresses",
    ]

    rows = sorted(rows, key=lambda row: (row["token_symbol"], row["date"]))

    with output_path.open("w", newline="") as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def fetch_selected_tokens(token_rows, chain_rows_by_token):
    """Fetch selected pools and pool-level rows for configured tokens."""
    selected_pools = []
    pool_volume_rows = []

    for token in token_rows:
        token_symbol = token["token_symbol"]
        token_chain_rows = chain_rows_by_token.get(token_symbol, [])

        if len(token_chain_rows) == 0:
            print("No token-chain config found for %s" % token_symbol)
            continue

        try:
            pool_results = find_top_pools_with_ohlcv(token, token_chain_rows)
        except Exception as error:
            print("Failed %s: %s" % (token_symbol, error))
            continue

        if len(pool_results) == 0:
            print("No usable pool found for %s" % token_symbol)
            continue

        for pool_result in pool_results:
            pool = pool_result[0]
            ohlcv_list = pool_result[1]
            selected_pools.append(pool)

            print(
                "Using %s pool: %s (%s)"
                % (token_symbol, pool["pool_name"], pool["pool_address"])
            )

            latest_timestamp = max(int(ohlcv[0]) for ohlcv in ohlcv_list)
            for ohlcv in ohlcv_list:
                row = convert_ohlcv_row(
                    ohlcv,
                    pool,
                    include_tvl_snapshot=int(ohlcv[0]) == latest_timestamp,
                )
                pool_volume_rows.append(row)

        print("Fetched %s DEX pools: %s" % (token_symbol, len(pool_results)))
        time.sleep(REQUEST_SLEEP_SECONDS)

    return selected_pools, pool_volume_rows


def main(token_symbols=None, append=False) -> None:
    """Fetch DEX data into processed CSV files."""
    all_token_rows = read_token_config(TOKEN_CONFIG_PATH)
    token_rows = filter_token_rows(all_token_rows, token_symbols)
    chain_rows = read_token_chain_config(TOKEN_CHAIN_CONFIG_PATH, all_token_rows)
    chain_rows_by_token = group_chain_rows_by_token(chain_rows)

    selected_pools, pool_volume_rows = fetch_selected_tokens(
        token_rows,
        chain_rows_by_token,
    )

    expected_token_count = len(token_rows)

    if append:
        if token_symbols is None:
            raise ValueError("--append requires --tokens")

        existing_pools = read_csv_rows(DEX_POOLS_OUTPUT_PATH)
        existing_pool_volume_rows = read_csv_rows(DEX_POOL_VOLUME_OUTPUT_PATH)
        selected_pools = replace_token_rows(
            existing_pools,
            selected_pools,
            token_symbols,
        )
        pool_volume_rows = replace_token_rows(
            existing_pool_volume_rows,
            pool_volume_rows,
            token_symbols,
        )
        expected_token_count = len(all_token_rows)

    pool_volume_rows = deduplicate_pool_volume_rows(pool_volume_rows)
    volume_rows = aggregate_dex_pool_rows(pool_volume_rows)
    volume_rows = filter_complete_dates(volume_rows, expected_token_count)

    write_pool_rows(selected_pools, DEX_POOLS_OUTPUT_PATH)
    write_pool_volume_rows(pool_volume_rows, DEX_POOL_VOLUME_OUTPUT_PATH)
    write_volume_rows(volume_rows, DEX_VOLUME_OUTPUT_PATH)

    print("Wrote %s pools to %s" % (len(selected_pools), DEX_POOLS_OUTPUT_PATH))
    print("Wrote %s pool rows to %s" % (len(pool_volume_rows), DEX_POOL_VOLUME_OUTPUT_PATH))
    print("Wrote %s rows to %s" % (len(volume_rows), DEX_VOLUME_OUTPUT_PATH))


def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Fetch GeckoTerminal DEX data")
    parser.add_argument(
        "--tokens",
        help="Comma-separated token symbols to refresh",
    )
    parser.add_argument(
        "--append",
        action="store_true",
        help="Replace selected tokens and preserve existing token rows",
    )
    args = parser.parse_args()

    token_symbols = None
    if args.tokens:
        token_symbols = []
        for token_symbol in args.tokens.split(","):
            cleaned = token_symbol.strip().upper()
            if cleaned:
                token_symbols.append(cleaned)

    return token_symbols, args.append


if __name__ == "__main__":
    selected_tokens, append_rows = parse_args()
    main(selected_tokens, append_rows)
