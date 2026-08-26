"""Run the two-pool Uniswap V3 depth/execution canary without publishing."""

from __future__ import annotations

import argparse
import hashlib
import json
import re
from collections import Counter
from pathlib import Path

try:
    from scripts.fetch_dex_depth import (
        collect_dex_depth_with_execution,
        dex_market_id,
        load_uniswap_v3_execution_authority,
    )
    from scripts.fetch_tvl import collect_tvl
    from scripts.execution_cost import (
        EXECUTION_DIRECTIONS,
        EXECUTION_NOTIONALS_USD,
    )
except ModuleNotFoundError:
    from fetch_dex_depth import (
        collect_dex_depth_with_execution,
        dex_market_id,
        load_uniswap_v3_execution_authority,
    )
    from fetch_tvl import collect_tvl
    from execution_cost import EXECUTION_DIRECTIONS, EXECUTION_NOTIONALS_USD


BLOCK_HASH_PATTERN = re.compile(r"0x[0-9a-f]{64}\Z", flags=re.ASCII)
EXPECTED_EXECUTION_SCENARIOS = {
    (direction, str(notional))
    for direction in EXECUTION_DIRECTIONS
    for notional in EXECUTION_NOTIONALS_USD
}


def authority_pools() -> list[dict[str, str]]:
    pools = []
    for market_id, market in sorted(
        load_uniswap_v3_execution_authority().items()
    ):
        pools.append(
            {
                "market_id": market_id,
                "token_symbol": market_id.rsplit(":", 1)[-1],
                "chain": market["chain"],
                "dex": market["dex"],
                "pool_address": market["pool_address"],
                "pool_name": "",
            }
        )
    return pools


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def validate_raw_evidence(
    evidence_root: Path,
    *,
    tvl_snapshot_id: str,
    depth_snapshot_id: str,
    expected_market_ids: set[str],
) -> dict[str, dict[str, object]]:
    tvl_directory = evidence_root / "tvl" / tvl_snapshot_id
    tvl_raw_files = sorted(
        path
        for path in tvl_directory.glob("*.json")
        if path.name != "manifest.json"
    )
    if not tvl_raw_files:
        raise ValueError("canary has no retained GeckoTerminal raw response")
    tvl_raw_hashes = {_sha256(path) for path in tvl_raw_files}

    depth_directory = evidence_root / "depth" / depth_snapshot_id
    depth_raw_files = sorted(
        path
        for path in depth_directory.glob("*.json")
        if path.name != "manifest.json"
    )
    if len(depth_raw_files) != 2:
        raise ValueError("canary must retain exactly two pool transcripts")

    evidence: dict[str, dict[str, object]] = {}
    for path in depth_raw_files:
        try:
            payload = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as error:
            raise ValueError("canary pool transcript is unreadable") from error
        manifest = payload.get("v3_tick_scan_manifest")
        usd_evidence = payload.get("usd_price_evidence")
        if not isinstance(manifest, dict) or not isinstance(usd_evidence, dict):
            raise ValueError("canary transcript is missing V3 or USD evidence")
        market_id = str(manifest.get("market_id") or "")
        if market_id not in expected_market_ids or market_id in evidence:
            raise ValueError("canary transcript market identity is invalid")
        block = manifest.get("block")
        block_final = manifest.get("block_final")
        if not isinstance(block, dict) or block_final != block:
            raise ValueError("canary transcript final block identity is invalid")
        block_hash = str(block.get("hash") or "")
        if BLOCK_HASH_PATTERN.fullmatch(block_hash) is None:
            raise ValueError("canary transcript block hash is invalid")
        block_number = int(str(block.get("number") or ""))
        parity = manifest.get("quoter_v2_parity")
        if not isinstance(parity, list) or len(parity) != len(
            EXPECTED_EXECUTION_SCENARIOS
        ):
            raise ValueError("canary transcript lacks ten exact Quoter matches")
        parity_scenarios = []
        for item in parity:
            if not isinstance(item, dict) or item.get("status") != "exact_match":
                raise ValueError("canary transcript lacks ten exact Quoter matches")
            parity_scenarios.append(
                (
                    str(item.get("direction") or ""),
                    str(item.get("requested_notional_usd") or ""),
                )
            )
        if (
            len(parity_scenarios) != len(set(parity_scenarios))
            or set(parity_scenarios) != EXPECTED_EXECUTION_SCENARIOS
        ):
            raise ValueError("canary raw Quoter scenario inventory is invalid")
        usd_hash = str(usd_evidence.get("raw_response_sha256") or "")
        if usd_hash not in tvl_raw_hashes:
            raise ValueError("canary USD lineage does not match retained raw input")
        if (
            usd_evidence.get("source") != "GeckoTerminal API v2"
            or not str(usd_evidence.get("source_endpoint") or "").startswith(
                "https://api.geckoterminal.com/api/v2/"
            )
            or not str(usd_evidence.get("observed_at") or "")
        ):
            raise ValueError("canary USD lineage is incomplete")
        evidence[market_id] = {
            "block_number": block_number,
            "block_hash": block_hash,
            "raw_transcript_sha256": _sha256(path),
            "usd_price_raw_response_sha256": usd_hash,
        }
    if set(evidence) != expected_market_ids:
        raise ValueError("canary transcript coverage is incomplete")
    return evidence


def run_canary(evidence_root: Path) -> dict[str, object]:
    evidence_root.mkdir(parents=True, exist_ok=False)
    pools = authority_pools()
    expected_market_ids = {pool["market_id"] for pool in pools}
    if len(pools) != 2 or len(expected_market_ids) != 2:
        raise ValueError("canary authority must contain exactly two markets")
    tvl_snapshot_id, tvl_rows = collect_tvl(
        pools,
        raw_root=evidence_root / "tvl",
        sleep_seconds=0,
    )
    if len(tvl_rows) != len(pools) or any(
        row.get("status") != "observed" for row in tvl_rows
    ):
        raise ValueError("canary USD source collection is incomplete")
    depth_snapshot_id, depth_rows, execution_rows = (
        collect_dex_depth_with_execution(
            tvl_rows,
            raw_root=evidence_root / "depth",
            sleep_seconds=0,
        )
    )
    depth_market_ids = [dex_market_id(row) for row in depth_rows]
    execution_market_ids = [row["market_id"] for row in execution_rows]
    if (
        len(depth_rows) != 2
        or set(depth_market_ids) != expected_market_ids
        or len(execution_rows) != 20
        or set(execution_market_ids) != expected_market_ids
        or any(
            execution_market_ids.count(market_id) != 10
            for market_id in expected_market_ids
        )
    ):
        raise ValueError("canary result does not contain the exact two-pool scenario inventory")
    depth_blocks = {row["block_number"] for row in depth_rows}
    execution_blocks = {row["block_number"] for row in execution_rows}
    if depth_blocks != execution_blocks:
        raise ValueError("canary depth/execution block lineage is inconsistent")
    if len(depth_blocks) != 1:
        raise ValueError("canary pools must use one shared fixed block")
    if any(row["status"] != "observed" for row in depth_rows):
        raise ValueError("canary requires both complete observed depth facts")
    if any(row["status"] != "observed" for row in execution_rows):
        raise ValueError("canary requires all 20 exact scenarios to be observed")
    for market_id in expected_market_ids:
        scenarios = [
            (row.get("direction"), row.get("requested_notional_usd"))
            for row in execution_rows
            if row["market_id"] == market_id
        ]
        if (
            len(scenarios) != len(set(scenarios))
            or set(scenarios) != EXPECTED_EXECUTION_SCENARIOS
        ):
            raise ValueError("canary execution scenario inventory is invalid")

    raw_evidence = validate_raw_evidence(
        evidence_root,
        tvl_snapshot_id=tvl_snapshot_id,
        depth_snapshot_id=depth_snapshot_id,
        expected_market_ids=expected_market_ids,
    )
    raw_block_identities = {
        (
            int(item["block_number"]),
            str(item["block_hash"]),
        )
        for item in raw_evidence.values()
    }
    if len(raw_block_identities) != 1:
        raise ValueError("canary pools must share one block number and hash")
    raw_block_number, raw_block_hash = next(iter(raw_block_identities))
    if depth_blocks != {str(raw_block_number)}:
        raise ValueError("canary row block does not match raw evidence")

    result = {
        "schema": "uniswap_v3_canary_result/v1",
        "published": False,
        "tvl_snapshot_id": tvl_snapshot_id,
        "depth_snapshot_id": depth_snapshot_id,
        "market_ids": sorted(expected_market_ids),
        "block_numbers": sorted(int(block_number) for block_number in depth_blocks),
        "block_hashes": [raw_block_hash],
        "depth_status_counts": dict(Counter(row["status"] for row in depth_rows)),
        "execution_status_counts": dict(
            Counter(row["status"] for row in execution_rows)
        ),
        "execution_scenario_count": len(execution_rows),
        "raw_evidence_root": str(evidence_root.resolve()),
        "raw_evidence": raw_evidence,
    }
    result_path = evidence_root / "canary_result.json"
    with result_path.open("x", encoding="utf-8") as destination:
        json.dump(result, destination, ensure_ascii=False, indent=2, sort_keys=True)
        destination.write("\n")
    return result


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--evidence-root", type=Path, required=True)
    arguments = parser.parse_args()
    print(
        json.dumps(
            run_canary(arguments.evidence_root),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
