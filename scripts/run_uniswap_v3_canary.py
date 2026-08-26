"""Run the two-pool Uniswap V3 depth/execution canary without publishing."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

try:
    from scripts.fetch_dex_depth import (
        V3_EXECUTION_AUTHORITY_PATH,
        collect_dex_depth_with_execution,
        load_uniswap_v3_execution_authority,
        validate_uniswap_v3_exact_candidate,
        write_uniswap_v3_exact_raw_receipt,
    )
    from scripts.fetch_tvl import collect_tvl
except ModuleNotFoundError:
    from fetch_dex_depth import (
        V3_EXECUTION_AUTHORITY_PATH,
        collect_dex_depth_with_execution,
        load_uniswap_v3_execution_authority,
        validate_uniswap_v3_exact_candidate,
        write_uniswap_v3_exact_raw_receipt,
    )
    from fetch_tvl import collect_tvl


def authority_pools(
    authority_path: Path = V3_EXECUTION_AUTHORITY_PATH,
) -> list[dict[str, str]]:
    pools = []
    for market_id, market in sorted(
        load_uniswap_v3_execution_authority(authority_path).items()
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


def run_canary(
    evidence_root: Path,
    *,
    authority_path: Path = V3_EXECUTION_AUTHORITY_PATH,
) -> dict[str, object]:
    evidence_root.mkdir(parents=True, exist_ok=False)
    pools = authority_pools(authority_path)
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
    receipt = validate_uniswap_v3_exact_candidate(
        tvl_rows,
        depth_rows,
        execution_rows,
        tvl_raw_root=evidence_root / "tvl",
        depth_raw_root=evidence_root / "depth",
        authority_path=authority_path,
    )
    write_uniswap_v3_exact_raw_receipt(evidence_root / "depth", receipt)
    block = receipt["shared_finalized_block"]

    result = {
        "schema": "uniswap_v3_canary_result/v1",
        "published": False,
        "tvl_snapshot_id": tvl_snapshot_id,
        "depth_snapshot_id": depth_snapshot_id,
        "market_ids": receipt["market_ids"],
        "block_numbers": [block["number"]],
        "block_hashes": [block["hash"]],
        "depth_status_counts": {
            "observed": receipt["depth_observed_count"]
        },
        "execution_status_counts": {
            "observed": receipt["execution_observed_scenario_count"]
        },
        "execution_scenario_count": receipt[
            "execution_observed_scenario_count"
        ],
        "raw_evidence_root": str(evidence_root.resolve()),
        "uniswap_v3_exact_validation": receipt,
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
