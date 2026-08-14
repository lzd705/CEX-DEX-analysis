"""Contract tests for the publish-safe observed data-quality snapshot."""

from __future__ import annotations

import csv
import hashlib
import json
import sqlite3
import subprocess
import sys
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path


from scripts.data_quality_snapshot import build_snapshot, canonical_snapshot_bytes
from scripts.execution_cost import (
    EXECUTION_COST_COLUMNS,
    EXECUTION_NOTIONALS_USD,
    NOTIONAL_DEFINITION,
    RESULT_NUMERIC_COLUMNS,
)
from scripts.event_facts import CURATED_COLUMNS
from scripts.fetch_cex_depth import DEPTH_COLUMNS_ALL
from scripts.fetch_dex_depth import DEX_DEPTH_COLUMNS
from scripts.market_database import build_database


PROJECT_ROOT = Path(__file__).resolve().parents[1]
FAMILY_NAMES = [
    "cex_daily_ohlcv",
    "cex_depth",
    "cex_execution_cost",
    "cex_instrument_lifecycle",
    "dex_daily_ohlcv",
    "dex_depth",
    "dex_execution_cost",
    "event_facts",
    "market_lifecycle_reviews",
    "route_cohort_opportunity",
    "route_shadow_route_cost_evidence",
    "tvl",
]

CEX_HEADER = [
    "date",
    "token_symbol",
    "exchange",
    "cex_symbol",
    "open",
    "high",
    "low",
    "close",
    "base_volume",
    "quote_volume_usd",
]
DEX_HEADER = [
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
TVL_HEADER = [
    "snapshot_id",
    "observed_at",
    "request_started_at",
    "response_received_at",
    "token_symbol",
    "chain",
    "dex",
    "pool_address",
    "pool_name",
    "source_dex",
    "source_pool_name",
    "base_token_id",
    "quote_token_id",
    "tvl_usd",
    "base_token_price_usd",
    "quote_token_price_usd",
    "volume_24h_usd",
    "pool_created_at",
    "tvl_method",
    "source",
    "source_endpoint",
    "raw_response_sha256",
    "status",
    "reason_code",
    "error",
]
CEX_DEPTH_HEADER = list(DEPTH_COLUMNS_ALL)
DEX_DEPTH_HEADER = list(DEX_DEPTH_COLUMNS)
CEX_DEPTH_MEASUREMENT_FIELDS = [
    "best_bid",
    "best_ask",
    "midpoint",
    "spread_quote",
    "spread_bps",
] + [
    f"{side}_depth_{band}bps_usd"
    for band in (10, 25, 50, 100)
    for side in ("bid", "ask", "total")
]
DEX_DEPTH_MEASUREMENT_FIELDS = [
    f"{side}_depth_{band}bps_usd"
    for band in (10, 25, 50, 100)
    for side in ("sell", "buy", "total")
]
EXECUTION_HEADER = list(EXECUTION_COST_COLUMNS)
EXECUTION_NOTIONAL_TEXT = [str(int(value)) for value in EXECUTION_NOTIONALS_USD]
EVENT_HEADER = list(CURATED_COLUMNS)


def _write_csv(path, header, rows):
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=header)
        writer.writeheader()
        writer.writerows(rows)


def _write_json(path, value):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(value, ensure_ascii=False, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _window_dates():
    first = date(2026, 7, 15)
    return [(first + timedelta(days=offset)).isoformat() for offset in range(30)]


def _cex_row(day, **overrides):
    row = {
        "date": day,
        "token_symbol": "BTC",
        "exchange": "Binance",
        "cex_symbol": "BTCUSDT",
        "open": "100",
        "high": "110",
        "low": "90",
        "close": "105",
        "base_volume": "2",
        "quote_volume_usd": "200",
    }
    row.update(overrides)
    return row


def _dex_row(day, **overrides):
    row = {
        "date": day,
        "token_symbol": "BTC",
        "chain": "ethereum",
        "dex": "uniswap_v3",
        "pool_address": "0x1111111111111111111111111111111111111111",
        "pool_name": "WBTC-USDC",
        "open": "100",
        "high": "110",
        "low": "90",
        "close": "105",
        "dex_volume_usd": "50",
        "pool_tvl_usd": "500",
    }
    row.update(overrides)
    return row


def _tvl_row(**overrides):
    row = {
        "snapshot_id": "snapshot-001",
        "observed_at": "2026-08-13T12:00:00Z",
        "request_started_at": "2026-08-13T11:59:58Z",
        "response_received_at": "2026-08-13T11:59:59Z",
        "token_symbol": "BTC",
        "chain": "ethereum",
        "dex": "uniswap_v3",
        "pool_address": "0x1111111111111111111111111111111111111111",
        "pool_name": "WBTC-USDC",
        "source_dex": "uniswap_v3",
        "source_pool_name": "WBTC-USDC",
        "base_token_id": "wbtc",
        "quote_token_id": "usdc",
        "tvl_usd": "5000000",
        "base_token_price_usd": "60000",
        "quote_token_price_usd": "1",
        "volume_24h_usd": "1000000",
        "pool_created_at": "2021-01-01T00:00:00Z",
        "tvl_method": "source_reported",
        "source": "geckoterminal",
        "source_endpoint": "https://example.invalid/pool",
        "raw_response_sha256": "b" * 64,
        "status": "observed",
        "reason_code": "observed",
        "error": "",
    }
    row.update(overrides)
    return row


def _cex_depth_row(**overrides):
    row = {field: "" for field in CEX_DEPTH_HEADER}
    row.update(
        {
            "snapshot_id": "cex-depth-001",
            "observed_at": "2026-08-13T12:00:00+00:00",
            "request_started_at": "2026-08-13T11:59:58Z",
            "response_received_at": "2026-08-13T12:00:01Z",
            "token_symbol": "BTC",
            "exchange": "binance",
            "cex_symbol": "BTC/USDT",
            "source_instrument": "BTCUSDT",
            "base_asset": "BTC",
            "source_quote_asset": "USDT",
            "quote_to_usd": "1",
            "quote_conversion_method": "stablecoin_parity",
            "best_bid": "99",
            "best_ask": "101",
            "midpoint": "100",
            "spread_quote": "2",
            "spread_bps": "200",
            "bid_levels_returned": "10",
            "ask_levels_returned": "10",
            "requested_level_limit": "100",
            "full_book_reported": "1",
            "depth_method": "midpoint_symmetric_quote_notional",
            "source": "binance public spot order-book API",
            "source_endpoint": "https://example.invalid/depth",
            "raw_response_sha256": "c" * 64,
            "status": "observed",
            "reason_code": "observed",
        }
    )
    for band in (10, 25, 50, 100):
        row[f"bid_depth_{band}bps_usd"] = "0" if band == 10 else str(band)
        row[f"ask_depth_{band}bps_usd"] = str(band)
        row[f"total_depth_{band}bps_usd"] = str(band if band == 10 else band * 2)
        row[f"depth_{band}bps_complete"] = "1"
    row.update(overrides)
    return row


def _dex_depth_row(**overrides):
    row = {field: "" for field in DEX_DEPTH_HEADER}
    row.update(
        {
            "snapshot_id": "dex-depth-001",
            "observed_at": "2026-08-13T12:00:02+00:00",
            "request_started_at": "2026-08-13T11:59:58Z",
            "response_received_at": "2026-08-13T12:00:02Z",
            "token_symbol": "BTC",
            "chain": "eth",
            "dex": "uniswap_v2",
            "pool_address": "0x1111111111111111111111111111111111111111",
            "pool_name": "WBTC-USDC",
            "protocol_model": "constant_product_v2",
            "block_number": "12345678",
            "block_timestamp": "2026-08-13T12:00:00Z",
            "target_token_address": "0x2222222222222222222222222222222222222222",
            "target_token_position": "0",
            "token0_address": "0x2222222222222222222222222222222222222222",
            "token0_symbol": "WBTC",
            "token0_decimals": "8",
            "token0_price_usd": "60000",
            "token1_address": "0x3333333333333333333333333333333333333333",
            "token1_symbol": "USDC",
            "token1_decimals": "6",
            "token1_price_usd": "1",
            "fee_bps": "30",
            "pool_state_price_usd": "60000",
            "source_target_price_usd": "60000",
            "price_difference_bps": "0",
            "usd_price_source_snapshot_id": "tvl-snapshot-001",
            "usd_price_observed_at": "2026-08-13T11:59:59Z",
            "usd_price_skew_seconds": "1",
            "usd_price_freshness_status": "current",
            "usd_price_source": "geckoterminal",
            "usd_price_source_endpoint": "https://example.invalid/pool",
            "usd_price_raw_response_sha256": "d" * 64,
            "depth_method": "fixed_block_pool_state",
            "source": "fixed-block EVM JSON-RPC eth_call",
            "source_endpoint": "https://example.invalid/rpc",
            "raw_response_sha256": "e" * 64,
            "status": "observed",
            "reason_code": "observed",
        }
    )
    for band in (10, 25, 50, 100):
        row[f"sell_depth_{band}bps_usd"] = "0" if band == 10 else str(band)
        row[f"buy_depth_{band}bps_usd"] = str(band)
        row[f"total_depth_{band}bps_usd"] = str(band if band == 10 else band * 2)
        row[f"depth_{band}bps_complete"] = "1"
    row.update(overrides)
    return row


def _execution_row(market_type, direction, notional, **overrides):
    row = {field: "" for field in EXECUTION_HEADER}
    target_quantity = str(int(notional) // 100)
    row.update(
        {
            "snapshot_id": f"{market_type}-execution-001",
            "source_snapshot_id": f"{market_type}-source-001",
            "contract_version": "1",
            "calculation_method": "fixed_notional_from_observed_state",
            "observed_at": "2026-08-13T12:00:02+00:00",
            "state_observed_at": "2026-08-13T12:00:00Z",
            "request_started_at": "2026-08-13T11:59:58Z",
            "response_received_at": "2026-08-13T12:00:02Z",
            "market_id": "cex:binance:BTC/USDT",
            "market_type": market_type,
            "token_symbol": "BTC",
            "exchange": "binance",
            "cex_symbol": "BTC/USDT",
            "source_instrument": "BTCUSDT",
            "base_asset": "BTC",
            "source_quote_asset": "USDT",
            "direction": direction,
            "requested_notional_usd": str(notional),
            "notional_definition": NOTIONAL_DEFINITION,
            "reference_price_method": "pre_trade_midpoint",
            "reference_price_quote_per_token": "100",
            "quote_to_usd": "1",
            "reference_price_usd_per_token": "100",
            "reference_notional_usd": str(notional),
            "target_token_quantity": target_quantity,
            "filled_token_quantity": target_quantity,
            "fill_ratio": "1",
            "quote_amount": str(notional),
            "quote_amount_usd": str(notional),
            "filled_vwap_quote_per_token": "100",
            "filled_vwap_usd_per_token": "100",
            "quoted_execution_cost_usd": "0",
            "quoted_execution_cost_bps": "0",
            "levels_or_ticks_consumed": "1",
            "ending_marginal_price_quote_per_token": "100",
            "fee_status": "excluded_unknown_account_tier",
            "usd_conversion_status": "observed_stablecoin_parity",
            "excluded_costs": "taker_fee,gas,mev",
            "status": "observed",
            "status_reason": "target_filled",
            "source": "observed market state",
            "source_endpoint": "https://example.invalid/state",
            "source_sequence": "12345678",
            "raw_response_sha256": "f" * 64,
        }
    )
    if market_type == "dex":
        row.update(
            {
                "market_id": (
                    "dex:eth:uniswap_v2:"
                    "0x1111111111111111111111111111111111111111:BTC"
                ),
                "exchange": "",
                "cex_symbol": "",
                "source_instrument": "",
                "base_asset": "",
                "source_quote_asset": "",
                "chain": "eth",
                "dex": "uniswap_v2",
                "pool_address": "0x1111111111111111111111111111111111111111",
                "block_number": "12345678",
                "block_timestamp": "2026-08-13T12:00:00Z",
                "protocol_model": "constant_product_v2",
                "target_token_address": "0x2222222222222222222222222222222222222222",
                "target_token_decimals": "8",
                "quote_token_address": "0x3333333333333333333333333333333333333333",
                "quote_token_decimals": "6",
                "usd_price_source_snapshot_id": "tvl-snapshot-001",
                "usd_price_observed_at": "2026-08-13T11:59:59Z",
                "fee_status": "included_protocol_fee",
                "fee_rate_bps": "30",
                "usd_conversion_status": "observed_inventory_token_price",
                "excluded_costs": "gas,mev",
                "status_reason": "full_target_quantity_filled",
                "source": "fixed-block EVM JSON-RPC pool state",
            }
        )
    row.update(overrides)
    return row


def _execution_rows(market_type, *, terminal_buy=False):
    rows = []
    for direction in ("sell_token", "buy_token"):
        for notional in EXECUTION_NOTIONAL_TEXT:
            row = _execution_row(market_type, direction, notional)
            if terminal_buy and direction == "buy_token":
                for field in RESULT_NUMERIC_COLUMNS:
                    row[field] = ""
                row.update(
                    {
                        "status": "unsupported",
                        "status_reason": "source_range_unavailable",
                    }
                )
            rows.append(row)
    return rows


def _event_row(**overrides):
    row = {field: "" for field in EVENT_HEADER}
    row.update(
        {
            "event_id": "btc-scheduled-release",
            "revision": "1",
            "token_symbol": "BTC",
            "event_type": "unlock",
            "event_subtype": "scheduled_release",
            "event_name": "BTC scheduled release",
            "lifecycle": "scheduled",
            "effective_at": "2026-08-15",
            "effective_at_precision": "day",
            "source_kind": "official_project",
            "evidence_status": "primary_confirmed",
            "source_url": "https://example.invalid/events/btc-release",
            "source_checked_at_utc": "2026-08-13T12:00:00Z",
            "source_record_file": "btc-release-v1.json",
            "record_locator": "facts.release",
            "recorded_at_utc": "2026-08-13T12:00:00Z",
            "revision_reason": "initial",
            "notes": "operator-private-note",
        }
    )
    row.update(overrides)
    return row


def _event_record(*, checked_at="2026-08-13T12:00:00Z", lifecycle="scheduled"):
    return {
        "record_schema": "source_check/v1",
        "source_url": "https://example.invalid/events/btc-release",
        "checked_at_utc": checked_at,
        "source_title": "private source title",
        "publisher": "private publisher",
        "facts": {
            "release": {
                "statement": "private source-backed statement",
                "supported_lifecycle": lifecycle,
            }
        },
        "limitations": ["private limitation"],
    }


def _write_event_fixture(directory, rows, records):
    data_dir = Path(directory)
    (data_dir / "curated").mkdir(parents=True, exist_ok=True)
    _write_csv(data_dir / "curated" / "event_facts.csv", EVENT_HEADER, rows)
    for filename, record in records.items():
        _write_json(data_dir / "evidence" / "events" / filename, record)


def _cex_lifecycle_payload(*, checked_at="2026-08-13T12:00:00+00:00"):
    response_hash = "a" * 64
    configured_hash = "b" * 64
    review = {
        "market_id": "cex:crypto_com:BTC/USDT",
        "market_type": "cex",
        "token_symbol": "BTC",
        "exchange": "crypto_com",
        "instrument": "BTC/USDT",
        "current_listing_status": "absent_from_official_current_catalog",
        "reason_code": "instrument_absent_from_current_catalog",
        "checked_at_utc": checked_at,
        "source_url": "https://api.crypto.com/exchange/v1/public/get-instruments",
        "http_status": 200,
        "response_sha256": response_hash,
        "inventory_count": 919,
        "instrument_present": False,
    }
    return {
        "schema": "cex_instrument_lifecycle/v1",
        "generated_at_utc": checked_at,
        "checked_at_utc": checked_at,
        "response_sha256": response_hash,
        "inventory_count": 919,
        "configured_market_count": 30,
        "configured_market_ids_sha256": configured_hash,
        "review_count": 1,
        "reviews": [review],
    }


def _market_review_revision(
    *,
    revision=1,
    review_status="disposed",
    checked_at="2026-08-13T12:00:00Z",
    reviewed_at="2026-08-13T12:00:00Z",
):
    disposed = review_status == "disposed"
    return {
        "review_id": "upbit-ldo-usdt-listed-no-recent-candle",
        "revision": revision,
        "supersedes_revision": None if revision == 1 else revision - 1,
        "review_status": review_status,
        "reviewed_issue_id": "a" * 20,
        "original_category": "stale_market_unknown",
        "original_reason_code": "stale_market_lifecycle_unknown",
        "market_id": "cex:upbit:LDO/USDT",
        "market_type": "cex",
        "token_symbol": "LDO",
        "issue_date": "2026-08-12",
        "disposition_status": "source_no_observation" if disposed else None,
        "disposition_reason_code": "no_candles" if disposed else None,
        "market_lifecycle": "listed_quote_market_dormant" if disposed else None,
        "evidence_status": "primary_confirmed",
        "review_method": "manual_primary_source_cross_check",
        "review_actor": "private-review-actor",
        "reviewed_at_utc": reviewed_at,
        "disposition_note": "private lifecycle disposition note",
        "source_checks": [
            {
                "source_kind": "official_exchange_ticker",
                "url": "https://api.upbit.com/v1/ticker?markets=USDT-LDO",
                "http_status": 200,
                "response_sha256": "c" * 64,
                "checked_at_utc": checked_at,
                "observations": {
                    "market": "USDT-LDO",
                    "last_trade_date_utc": "2026-08-11",
                },
            }
        ],
    }


def _market_lifecycle_payload(reviews, *, generated_at="2026-08-13T13:00:00Z"):
    return {
        "schema": "market_lifecycle_reviews/v1",
        "generated_at_utc": generated_at,
        "review_count": len(reviews),
        "reviews": reviews,
    }


def _publish_route_opportunity_fixture(data_dir):
    import scripts.route_publication as route_publication
    from tests.test_route_publication import _task7_cex_inputs

    data_path = Path(data_dir)
    fixture_root = data_path.parent
    core_root = fixture_root / "route-core"
    raw_root = fixture_root / "route-raw"
    source_root = fixture_root / "route-sources"
    private_root = fixture_root / "route-private"
    fixture = _task7_cex_inputs(
        core_root,
        raw_root,
        source_root,
        private_root,
    )
    pointer = route_publication.publish_complete_route_bundle(
        core_root=core_root,
        routes_root=data_path / "routes",
        raw_root=raw_root,
        source_root=fixture["source_root"],
        fee_profile_path=fixture["fee_profile_path"],
        fee_profile_id=fixture["fee_profile_id"],
        inventory_profile_path=fixture["inventory_profile_path"],
        opportunity_inputs=fixture["opportunity_inputs"],
    )
    return pointer


def _publish_route_shadow_fixture():
    from tests.test_route_publication import JointShadowPublicationTests

    fixture = JointShadowPublicationTests()
    fixture.setUp()
    fixture._publish()
    return fixture


def _build_bound_database(directory, cex_rows):
    data_dir = Path(directory)
    _write_csv(data_dir / "cex_exchange_volume_daily.csv", CEX_HEADER, cex_rows)
    _write_csv(data_dir / "dex_pool_volume_daily.csv", DEX_HEADER, [_dex_row(_window_dates()[0])])
    build_database(data_dir, data_dir / "market_facts.sqlite3")


def _build_dex_bound_database(directory, dex_rows):
    data_dir = Path(directory)
    _write_csv(
        data_dir / "cex_exchange_volume_daily.csv",
        CEX_HEADER,
        [_cex_row(_window_dates()[0])],
    )
    _write_csv(data_dir / "dex_pool_volume_daily.csv", DEX_HEADER, dex_rows)
    build_database(data_dir, data_dir / "market_facts.sqlite3")


def _rebind_cex_source(directory, rows):
    data_dir = Path(directory)
    path = data_dir / "cex_exchange_volume_daily.csv"
    _write_csv(path, CEX_HEADER, rows)
    digest = hashlib.sha256(path.read_bytes()).hexdigest()
    connection = sqlite3.connect(data_dir / "market_facts.sqlite3")
    try:
        connection.execute(
            """
            UPDATE dataset_snapshots
            SET cex_source_bytes = ?, cex_sha256 = ?, cex_row_count = ?
            WHERE snapshot_id = (SELECT snapshot_id FROM dataset_state WHERE singleton_id = 1)
            """,
            (path.stat().st_size, digest, len(rows)),
        )
        connection.commit()
    finally:
        connection.close()


def _update_database(directory, statements):
    connection = sqlite3.connect(Path(directory) / "market_facts.sqlite3")
    try:
        connection.execute("PRAGMA foreign_keys = OFF")
        for statement, parameters in statements:
            connection.execute(statement, parameters)
        connection.commit()
    finally:
        connection.close()


def _snapshot(directory):
    return build_snapshot(
        Path(directory),
        "2026-08-14T00:00:00Z",
        date(2026, 8, 13),
        30,
        "a" * 40,
    )


def _family(snapshot, name):
    return next(family for family in snapshot["families"] if family["name"] == name)


class DataQualitySnapshotCoreTests(unittest.TestCase):
    def test_empty_directory_is_explicitly_not_evaluated(self):
        with tempfile.TemporaryDirectory() as directory:
            snapshot = build_snapshot(
                Path(directory),
                "2026-08-14T00:00:00Z",
                date(2026, 8, 13),
                30,
                "a" * 40,
            )

            self.assertEqual(snapshot["schema_version"], "data_quality_snapshot/v1")
            self.assertEqual(snapshot["generated_at_utc"], "2026-08-14T00:00:00Z")
            self.assertEqual(snapshot["application"], {"build_sha": "a" * 40})
            self.assertEqual(snapshot["window"], {
                "start_date": "2026-07-15",
                "end_date": "2026-08-13",
                "expected_days": 30,
                "timezone": "UTC",
            })
            self.assertEqual(
                [family["name"] for family in snapshot["families"]],
                FAMILY_NAMES,
            )
            self.assertEqual(snapshot["summary"], {
                "evaluated_family_count": 0,
                "failed_family_count": 0,
                "not_evaluated_family_count": 12,
                "total_family_count": 12,
            })
            for family in snapshot["families"]:
                self.assertEqual(family["state"], "not_evaluated")
                self.assertIsNone(family["failure_reason"])
                expected_reason = (
                    "route_pointer_missing"
                    if family["name"].startswith("route_")
                    else "source_file_missing"
                )
                self.assertEqual(family["not_evaluated_reason"], expected_reason)
                self.assertEqual(family["counts"], {
                    "expected": None,
                    "observed": None,
                    "usable": None,
                    "expected_basis": None,
                })
                self.assertIsNone(family["coverage_bps"])
                self.assertEqual(family["duplicate_primary_key"], {
                    "count": None,
                    "rate_bps": None,
                })
                self.assertEqual(family["required_field_null"], {
                    "count": None,
                    "rate_bps": None,
                })
                self.assertEqual(family["measurements"], {
                    "null_count": None,
                    "zero_count": None,
                    "fields": {},
                })
                self.assertEqual(family["status_counts"], {})
                self.assertEqual(family["reason_counts"], {})
                self.assertEqual(family["observation_time"], {
                    "min": None,
                    "max": None,
                    "freshness_lag_seconds": None,
                })
                self.assertIsNone(family["source"])

            self.assertRegex(snapshot["publication"]["identity"], r"^[0-9a-f]{64}$")
            self.assertRegex(snapshot["snapshot_sha256"], r"^[0-9a-f]{64}$")
            self.assertNotIn(str(Path(directory)), json.dumps(snapshot, sort_keys=True))


class DataQualitySnapshotDailyTests(unittest.TestCase):
    def test_daily_source_without_authoritative_inventory_is_not_evaluated(self):
        with tempfile.TemporaryDirectory() as directory:
            _write_csv(
                Path(directory) / "cex_exchange_volume_daily.csv",
                CEX_HEADER,
                [_cex_row(_window_dates()[0])],
            )

            family = _family(_snapshot(directory), "cex_daily_ohlcv")

            self.assertEqual(family["state"], "not_evaluated")
            self.assertEqual(family["not_evaluated_reason"], "authoritative_inventory_missing")
            self.assertEqual(family["counts"]["expected"], None)
            self.assertEqual(family["coverage_bps"], None)

    def test_duplicate_primary_key_fails_with_measured_rate(self):
        with tempfile.TemporaryDirectory() as directory:
            rows = [_cex_row(day) for day in _window_dates()]
            _build_bound_database(directory, rows)
            duplicate_rows = rows + [dict(rows[0])]
            _rebind_cex_source(directory, duplicate_rows)

            family = _family(_snapshot(directory), "cex_daily_ohlcv")

            self.assertEqual(family["state"], "failed")
            self.assertEqual(family["failure_reason"], "duplicate_primary_key")
            self.assertEqual(family["duplicate_primary_key"], {"count": 1, "rate_bps": 323})

    def test_blank_required_identity_fails_without_echoing_value(self):
        with tempfile.TemporaryDirectory() as directory:
            rows = [_cex_row(day) for day in _window_dates()]
            _build_bound_database(directory, rows)
            broken_rows = [dict(row) for row in rows]
            broken_rows[0]["cex_symbol"] = ""
            _rebind_cex_source(directory, broken_rows)

            family = _family(_snapshot(directory), "cex_daily_ohlcv")

            self.assertEqual(family["state"], "failed")
            self.assertEqual(family["failure_reason"], "required_field_null")
            self.assertEqual(family["required_field_null"]["count"], 1)
            self.assertNotIn(str(Path(directory)), json.dumps(family, sort_keys=True))

    def test_null_measurement_and_real_zero_are_distinct(self):
        with tempfile.TemporaryDirectory() as directory:
            rows = [_cex_row(day) for day in _window_dates()]
            rows[3]["quote_volume_usd"] = "0"
            rows[7]["quote_volume_usd"] = ""
            _build_bound_database(directory, rows)

            family = _family(_snapshot(directory), "cex_daily_ohlcv")

            self.assertEqual(family["state"], "evaluated")
            self.assertEqual(family["counts"], {
                "expected": 30,
                "observed": 30,
                "usable": 29,
                "expected_basis": family["counts"]["expected_basis"],
            })
            self.assertEqual(family["measurements"]["fields"]["quote_volume_usd"], {
                "null_count": 1,
                "zero_count": 1,
            })
            self.assertEqual(family["measurements"]["null_count"], 1)
            self.assertEqual(family["measurements"]["zero_count"], 1)
            self.assertEqual(family["daily_coverage"]["ranking_eligible_market_count"], 0)

    def test_interior_missing_date_is_incomplete_even_with_both_endpoints(self):
        with tempfile.TemporaryDirectory() as directory:
            rows = [_cex_row(day) for day in _window_dates() if day != "2026-07-29"]
            _build_bound_database(directory, rows)

            family = _family(_snapshot(directory), "cex_daily_ohlcv")

            self.assertEqual(family["state"], "evaluated")
            self.assertEqual(family["counts"]["expected"], 30)
            self.assertEqual(family["counts"]["observed"], 29)
            self.assertEqual(family["coverage_bps"], 9667)
            self.assertEqual(family["daily_coverage"]["completeness_state"], "incomplete")
            self.assertEqual(family["daily_coverage"]["incomplete_market_count"], 1)
            self.assertEqual(family["daily_coverage"]["ranking_eligible_market_count"], 0)
            self.assertEqual(
                family["daily_coverage"]["disposition_counts"]["missing_unexplained"],
                1,
            )
            incomplete = family["daily_coverage"]["incomplete_markets"]
            self.assertEqual(len(incomplete), 1)
            self.assertRegex(incomplete[0]["market_identity_sha256"], r"^[0-9a-f]{64}$")
            self.assertEqual(incomplete[0]["missing_date_count"], 1)
            self.assertNotIn("BTCUSDT", json.dumps(incomplete, sort_keys=True))

    def test_one_day_does_not_pass_a_thirty_day_window(self):
        with tempfile.TemporaryDirectory() as directory:
            _build_bound_database(directory, [_cex_row("2026-08-13")])

            family = _family(_snapshot(directory), "cex_daily_ohlcv")

            self.assertEqual(family["counts"]["expected"], 30)
            self.assertEqual(family["counts"]["observed"], 1)
            self.assertEqual(family["coverage_bps"], 333)
            self.assertEqual(family["daily_coverage"]["completeness_state"], "incomplete")
            self.assertEqual(family["daily_coverage"]["ranking_eligible_market_count"], 0)
            self.assertEqual(
                family["daily_coverage"]["disposition_counts"]["missing_unexplained"],
                29,
            )

    def test_complete_thirty_day_window_accepts_measured_zero(self):
        with tempfile.TemporaryDirectory() as directory:
            rows = [_cex_row(day) for day in _window_dates()]
            rows[10]["quote_volume_usd"] = "0"
            _build_bound_database(directory, rows)

            family = _family(_snapshot(directory), "cex_daily_ohlcv")

            self.assertEqual(family["state"], "evaluated")
            self.assertEqual(family["counts"]["expected"], 30)
            self.assertEqual(family["counts"]["observed"], 30)
            self.assertEqual(family["counts"]["usable"], 30)
            self.assertEqual(family["coverage_bps"], 10000)
            self.assertEqual(family["daily_coverage"]["completeness_state"], "complete")
            self.assertEqual(family["daily_coverage"]["complete_market_count"], 1)
            self.assertEqual(family["daily_coverage"]["ranking_eligible_market_count"], 1)
            self.assertEqual(
                family["daily_coverage"]["disposition_counts"],
                {
                    "collection_failed": 0,
                    "missing_unexplained": 0,
                    "observed": 30,
                    "post_delisting": 0,
                    "pre_listing": 0,
                    "source_no_observation": 0,
                    "structurally_unsupported": 0,
                },
            )

    def test_window_external_null_does_not_pollute_window_metrics(self):
        with tempfile.TemporaryDirectory() as directory:
            rows = [_cex_row(day) for day in _window_dates()]
            rows.append(_cex_row("2026-07-14", quote_volume_usd=""))
            _build_bound_database(directory, rows)

            family = _family(_snapshot(directory), "cex_daily_ohlcv")

            self.assertEqual(family["state"], "evaluated")
            self.assertEqual(family["counts"]["observed"], 30)
            self.assertEqual(family["counts"]["usable"], 30)
            self.assertEqual(family["measurements"]["null_count"], 0)
            self.assertEqual(family["required_field_null"], {"count": 0, "rate_bps": 0})

    def test_window_external_duplicate_primary_key_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            rows = [_cex_row(day) for day in _window_dates()]
            rows.append(_cex_row("2026-07-14"))
            _build_bound_database(directory, rows)
            _rebind_cex_source(directory, rows + [_cex_row("2026-07-14")])

            family = _family(_snapshot(directory), "cex_daily_ohlcv")

            self.assertEqual(family["state"], "failed")
            self.assertEqual(family["failure_reason"], "duplicate_primary_key")

    def test_csv_extra_trailing_column_fails_before_database_binding(self):
        with tempfile.TemporaryDirectory() as directory:
            rows = [_cex_row(day) for day in _window_dates()]
            _build_bound_database(directory, rows)
            path = Path(directory) / "cex_exchange_volume_daily.csv"
            path.write_text(
                path.read_text(encoding="utf-8").replace(
                    "2026-07-15,BTC,Binance,BTCUSDT,100,110,90,105,2,200\n",
                    "2026-07-15,BTC,Binance,BTCUSDT,100,110,90,105,2,200,tail\n",
                ),
                encoding="utf-8",
            )

            family = _family(_snapshot(directory), "cex_daily_ohlcv")

            self.assertEqual(family["state"], "failed")
            self.assertEqual(family["failure_reason"], "schema_mismatch")

    def test_database_run_must_bind_to_current_snapshot(self):
        with tempfile.TemporaryDirectory() as directory:
            rows = [_cex_row(day) for day in _window_dates()]
            _build_bound_database(directory, rows)
            _update_database(
                directory,
                [
                    (
                        "UPDATE import_runs SET snapshot_id = ? WHERE run_id = "
                        "(SELECT import_run_id FROM dataset_state WHERE singleton_id = 1)",
                        ("f" * 24,),
                    )
                ],
            )

            family = _family(_snapshot(directory), "cex_daily_ohlcv")

            self.assertEqual(family["state"], "failed")
            self.assertEqual(family["failure_reason"], "authoritative_inventory_invalid")

    def test_database_secret_snapshot_id_is_never_published(self):
        with tempfile.TemporaryDirectory() as directory:
            rows = [_cex_row(day) for day in _window_dates()]
            _build_bound_database(directory, rows)
            secret = "api_key=top-secret/private/operator"
            _update_database(
                directory,
                [
                    (
                        "UPDATE dataset_snapshots SET snapshot_id = ? WHERE snapshot_id = "
                        "(SELECT snapshot_id FROM dataset_state WHERE singleton_id = 1)",
                        (secret,),
                    ),
                    (
                        "UPDATE import_runs SET snapshot_id = ? WHERE run_id = "
                        "(SELECT import_run_id FROM dataset_state WHERE singleton_id = 1)",
                        (secret,),
                    ),
                    ("UPDATE dataset_state SET snapshot_id = ? WHERE singleton_id = 1", (secret,)),
                ],
            )

            snapshot = _snapshot(directory)
            family = _family(snapshot, "cex_daily_ohlcv")

            self.assertEqual(family["state"], "failed")
            self.assertEqual(family["failure_reason"], "authoritative_inventory_invalid")
            self.assertNotIn(secret, canonical_snapshot_bytes(snapshot).decode("utf-8"))

    def test_database_secret_import_run_id_is_never_published(self):
        with tempfile.TemporaryDirectory() as directory:
            rows = [_cex_row(day) for day in _window_dates()]
            _build_bound_database(directory, rows)
            secret = "api_key=top-secret/private/operator"
            _update_database(
                directory,
                [
                    (
                        "UPDATE import_runs SET run_id = ? WHERE run_id = "
                        "(SELECT import_run_id FROM dataset_state WHERE singleton_id = 1)",
                        (secret,),
                    ),
                    ("UPDATE dataset_state SET import_run_id = ? WHERE singleton_id = 1", (secret,)),
                ],
            )

            snapshot = _snapshot(directory)
            family = _family(snapshot, "cex_daily_ohlcv")

            self.assertEqual(family["state"], "failed")
            self.assertEqual(family["failure_reason"], "authoritative_inventory_invalid")
            self.assertNotIn(secret, canonical_snapshot_bytes(snapshot).decode("utf-8"))

    def test_database_declared_row_count_must_match_csv_and_table(self):
        with tempfile.TemporaryDirectory() as directory:
            rows = [_cex_row(day) for day in _window_dates()]
            _build_bound_database(directory, rows)
            _update_database(
                directory,
                [
                    (
                        "UPDATE dataset_snapshots SET cex_row_count = cex_row_count + 1 "
                        "WHERE snapshot_id = (SELECT snapshot_id FROM dataset_state WHERE singleton_id = 1)",
                        (),
                    )
                ],
            )

            family = _family(_snapshot(directory), "cex_daily_ohlcv")

            self.assertEqual(family["state"], "failed")
            self.assertEqual(family["failure_reason"], "authoritative_inventory_invalid")

    def test_database_market_inventory_must_match_bound_csv(self):
        with tempfile.TemporaryDirectory() as directory:
            rows = [_cex_row(day) for day in _window_dates()]
            _build_bound_database(directory, rows)
            changed_rows = [dict(row, cex_symbol="BTCUSD") for row in rows]
            _rebind_cex_source(directory, changed_rows)

            family = _family(_snapshot(directory), "cex_daily_ohlcv")

            self.assertEqual(family["state"], "failed")
            self.assertEqual(family["failure_reason"], "authoritative_inventory_market_mismatch")


class DataQualitySnapshotDexDailyAdapterTests(unittest.TestCase):
    def test_dex_daily_source_without_authoritative_inventory_is_not_evaluated(self):
        with tempfile.TemporaryDirectory() as directory:
            _write_csv(
                Path(directory) / "dex_pool_volume_daily.csv",
                DEX_HEADER,
                [_dex_row(_window_dates()[0])],
            )

            family = _family(_snapshot(directory), "dex_daily_ohlcv")

            self.assertEqual(family["state"], "not_evaluated")
            self.assertEqual(
                family["not_evaluated_reason"],
                "authoritative_inventory_missing",
            )
            self.assertIsNone(family["counts"]["expected"])
            self.assertIsNone(family["coverage_bps"])

    def test_dex_daily_uses_sqlite_market_inventory_for_the_full_window_grid(self):
        with tempfile.TemporaryDirectory() as directory:
            _build_dex_bound_database(directory, [_dex_row("2026-08-13")])

            family = _family(_snapshot(directory), "dex_daily_ohlcv")

            self.assertEqual(family["state"], "evaluated")
            self.assertEqual(family["counts"]["expected"], 30)
            self.assertEqual(family["counts"]["observed"], 1)
            self.assertEqual(family["counts"]["usable"], 1)
            self.assertEqual(family["coverage_bps"], 333)
            self.assertEqual(
                family["counts"]["expected_basis"]["market_count"],
                1,
            )
            self.assertRegex(
                family["counts"]["expected_basis"]["market_inventory_sha256"],
                r"^[0-9a-f]{64}$",
            )
            self.assertEqual(
                family["daily_coverage"]["disposition_counts"]["missing_unexplained"],
                29,
            )
            self.assertEqual(
                family["daily_coverage"]["ranking_eligible_market_count"],
                0,
            )
            self.assertNotIn(
                "0x1111111111111111111111111111111111111111",
                json.dumps(family["daily_coverage"]["incomplete_markets"]),
            )

    def test_dex_daily_keeps_optional_null_and_real_zero_distinct(self):
        with tempfile.TemporaryDirectory() as directory:
            rows = [_dex_row(day) for day in _window_dates()]
            rows[3]["pool_tvl_usd"] = "0"
            rows[7]["pool_tvl_usd"] = ""
            rows[11]["dex_volume_usd"] = "0"
            _build_dex_bound_database(directory, rows)

            family = _family(_snapshot(directory), "dex_daily_ohlcv")

            self.assertEqual(family["state"], "evaluated")
            self.assertEqual(family["counts"]["expected"], 30)
            self.assertEqual(family["counts"]["observed"], 30)
            self.assertEqual(family["counts"]["usable"], 30)
            self.assertEqual(
                family["measurements"]["fields"]["pool_tvl_usd"],
                {"null_count": 1, "zero_count": 1},
            )
            self.assertEqual(
                family["measurements"]["fields"]["dex_volume_usd"],
                {"null_count": 0, "zero_count": 1},
            )
            self.assertEqual(family["measurements"]["null_count"], 1)
            self.assertEqual(family["measurements"]["zero_count"], 2)
            self.assertEqual(
                family["daily_coverage"]["ranking_eligible_market_count"],
                1,
            )


class DataQualitySnapshotDepthAdapterTests(unittest.TestCase):
    def test_depth_rows_bind_exact_retained_market_identity(self):
        cases = (
            (
                "cex_depth",
                "cex_depth_latest.csv",
                CEX_DEPTH_HEADER,
                [_cex_depth_row(token_symbol="ETH")],
            ),
            (
                "dex_depth",
                "dex_depth_latest.csv",
                DEX_DEPTH_HEADER,
                [_dex_depth_row(target_token_position="1")],
            ),
        )
        for family_name, filename, header, rows in cases:
            with self.subTest(family=family_name):
                with tempfile.TemporaryDirectory() as directory:
                    _write_csv(Path(directory) / filename, header, rows)

                    family = _family(_snapshot(directory), family_name)

                    self.assertEqual(family["state"], "failed")
                    self.assertEqual(
                        family["failure_reason"], "invalid_depth_contract"
                    )

    def test_header_only_latest_depth_and_tvl_files_fail_empty_inventory(self):
        cases = (
            ("tvl", "dex_pool_tvl_latest.csv", TVL_HEADER),
            ("cex_depth", "cex_depth_latest.csv", CEX_DEPTH_HEADER),
            ("dex_depth", "dex_depth_latest.csv", DEX_DEPTH_HEADER),
        )
        for family_name, filename, header in cases:
            with self.subTest(family=family_name):
                with tempfile.TemporaryDirectory() as directory:
                    _write_csv(Path(directory) / filename, header, [])

                    family = _family(_snapshot(directory), family_name)

                    self.assertEqual(family["state"], "failed")
                    self.assertEqual(family["failure_reason"], "empty_inventory")
                    self.assertIsNone(family["counts"]["expected"])
                    self.assertIsNone(family["counts"]["observed"])
                    self.assertIsNone(family["counts"]["usable"])

    def test_depth_adapters_report_exact_status_reason_clocks_and_measurements(self):
        cases = []

        cex_failed = _cex_depth_row(
            token_symbol="ETH",
            cex_symbol="ETH/USDT",
            source_instrument="ETHUSDT",
            base_asset="ETH",
            observed_at="2026-08-13T13:00:00+00:00",
            status="failed",
            reason_code="network",
            error="api_key=top-secret /private/operator/cex.json",
        )
        for field in CEX_DEPTH_MEASUREMENT_FIELDS:
            cex_failed[field] = ""
        cases.append(
            (
                "cex_depth",
                "cex_depth_latest.csv",
                CEX_DEPTH_HEADER,
                [_cex_depth_row(), cex_failed],
                "bid_depth_10bps_usd",
                {"observed": 1, "failed": 1},
                {"observed": 1, "network": 1},
                "2026-08-13T12:00:00Z",
                "2026-08-13T13:00:00Z",
            )
        )

        dex_unsupported = _dex_depth_row(
            token_symbol="ETH",
            pool_address="0x4444444444444444444444444444444444444444",
            observed_at="2026-08-13T13:00:00+00:00",
            block_number="",
            block_timestamp="",
            status="unsupported",
            reason_code="unsupported_protocol",
            error="cookie=session-secret /private/operator/dex.json",
        )
        for field in DEX_DEPTH_MEASUREMENT_FIELDS:
            dex_unsupported[field] = ""
        cases.append(
            (
                "dex_depth",
                "dex_depth_latest.csv",
                DEX_DEPTH_HEADER,
                [_dex_depth_row(), dex_unsupported],
                "sell_depth_10bps_usd",
                {"observed": 1, "unsupported": 1},
                {"observed": 1, "unsupported_protocol": 1},
                "2026-08-13T12:00:00Z",
                "2026-08-13T12:00:00Z",
            )
        )

        for (
            family_name,
            filename,
            header,
            rows,
            zero_field,
            status_counts,
            reason_counts,
            minimum,
            maximum,
        ) in cases:
            with self.subTest(family=family_name):
                with tempfile.TemporaryDirectory() as directory:
                    _write_csv(Path(directory) / filename, header, rows)

                    snapshot = _snapshot(directory)
                    family = _family(snapshot, family_name)

                    self.assertEqual(family["state"], "evaluated")
                    self.assertEqual(family["counts"]["expected"], 2)
                    self.assertEqual(family["counts"]["observed"], 2)
                    self.assertEqual(family["counts"]["usable"], 1)
                    self.assertEqual(family["coverage_bps"], 5000)
                    self.assertEqual(family["status_counts"], status_counts)
                    self.assertEqual(family["reason_counts"], reason_counts)
                    self.assertEqual(
                        family["measurements"]["fields"][zero_field],
                        {"null_count": 1, "zero_count": 1},
                    )
                    self.assertEqual(family["required_field_null"], {
                        "count": 0,
                        "rate_bps": 0,
                    })
                    self.assertEqual(family["observation_time"]["min"], minimum)
                    self.assertEqual(family["observation_time"]["max"], maximum)
                    serialized = canonical_snapshot_bytes(snapshot).decode("utf-8")
                    self.assertNotIn("top-secret", serialized)
                    self.assertNotIn("session-secret", serialized)
                    self.assertNotIn("/private/operator", serialized)

    def test_depth_latest_files_reject_mixed_snapshot_ids(self):
        cases = [
            (
                "cex_depth",
                "cex_depth_latest.csv",
                CEX_DEPTH_HEADER,
                [
                    _cex_depth_row(),
                    _cex_depth_row(
                        snapshot_id="cex-depth-002",
                        token_symbol="ETH",
                        cex_symbol="ETH/USDT",
                        source_instrument="ETHUSDT",
                        base_asset="ETH",
                    ),
                ],
            ),
            (
                "dex_depth",
                "dex_depth_latest.csv",
                DEX_DEPTH_HEADER,
                [
                    _dex_depth_row(),
                    _dex_depth_row(
                        snapshot_id="dex-depth-002",
                        token_symbol="ETH",
                        pool_address="0x4444444444444444444444444444444444444444",
                    ),
                ],
            ),
        ]
        for family_name, filename, header, rows in cases:
            with self.subTest(family=family_name):
                with tempfile.TemporaryDirectory() as directory:
                    _write_csv(Path(directory) / filename, header, rows)

                    family = _family(_snapshot(directory), family_name)

                    self.assertEqual(family["state"], "failed")
                    self.assertEqual(family["failure_reason"], "mixed_snapshot_id")

    def test_depth_adapters_reject_invalid_status_and_timestamp(self):
        cases = [
            (
                "cex_depth",
                "cex_depth_latest.csv",
                CEX_DEPTH_HEADER,
                [_cex_depth_row(status="complete")],
                "invalid_status",
            ),
            (
                "dex_depth",
                "dex_depth_latest.csv",
                DEX_DEPTH_HEADER,
                [_dex_depth_row(block_timestamp="2026-08-13T12:00:00")],
                "timezone_naive_timestamp",
            ),
            (
                "dex_depth",
                "dex_depth_latest.csv",
                DEX_DEPTH_HEADER,
                [_dex_depth_row(block_timestamp="2026-08-14T00:00:01Z")],
                "future_observation_timestamp",
            ),
        ]
        for family_name, filename, header, rows, failure_reason in cases:
            with self.subTest(family=family_name, reason=failure_reason):
                with tempfile.TemporaryDirectory() as directory:
                    _write_csv(Path(directory) / filename, header, rows)

                    family = _family(_snapshot(directory), family_name)

                    self.assertEqual(family["state"], "failed")
                    self.assertEqual(family["failure_reason"], failure_reason)

    def test_stale_depth_partition_remains_evaluated(self):
        with tempfile.TemporaryDirectory() as directory:
            _write_csv(
                Path(directory) / "cex_depth_latest.csv",
                CEX_DEPTH_HEADER,
                [_cex_depth_row(observed_at="2026-08-12T00:00:00+00:00")],
            )

            family = _family(_snapshot(directory), "cex_depth")

            self.assertEqual(family["state"], "evaluated")
            self.assertEqual(
                family["observation_time"]["freshness_lag_seconds"],
                172800,
            )
            self.assertEqual(
                family["reason_counts"],
                {"observed": 1, "stale_partition": 1},
            )

    def test_depth_adapters_apply_production_book_shape_rules(self):
        cases = [
            (
                "cex_depth",
                "cex_depth_latest.csv",
                CEX_DEPTH_HEADER,
                [_cex_depth_row(best_bid="101", best_ask="100")],
            ),
            (
                "dex_depth",
                "dex_depth_latest.csv",
                DEX_DEPTH_HEADER,
                [_dex_depth_row(total_depth_25bps_usd="5")],
            ),
        ]
        for family_name, filename, header, rows in cases:
            with self.subTest(family=family_name):
                with tempfile.TemporaryDirectory() as directory:
                    _write_csv(Path(directory) / filename, header, rows)

                    family = _family(_snapshot(directory), family_name)

                    self.assertEqual(family["state"], "failed")
                    self.assertEqual(family["failure_reason"], "invalid_measurement")


class DataQualitySnapshotExecutionAdapterTests(unittest.TestCase):
    def test_execution_rows_bind_exact_retained_market_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            rows = _execution_rows("cex")
            rows[0]["token_symbol"] = "ETH"
            _write_csv(
                Path(directory) / "cex_execution_cost_latest.csv",
                EXECUTION_HEADER,
                rows,
            )

            family = _family(_snapshot(directory), "cex_execution_cost")

            self.assertEqual(family["state"], "failed")
            self.assertEqual(
                family["failure_reason"], "invalid_execution_contract"
            )

    def test_execution_adapters_require_the_exact_two_by_five_inventory(self):
        cases = [
            (
                "cex_execution_cost",
                "cex_execution_cost_latest.csv",
                _execution_rows("cex", terminal_buy=True),
                5,
                {"observed": 5, "unsupported": 5},
                {"target_filled": 5, "source_range_unavailable": 5},
                {"null_count": 5, "zero_count": 0},
                {"null_count": 5, "zero_count": 5},
            ),
            (
                "dex_execution_cost",
                "dex_execution_cost_latest.csv",
                _execution_rows("dex"),
                10,
                {"observed": 10},
                {"full_target_quantity_filled": 10},
                {"null_count": 0, "zero_count": 0},
                {"null_count": 0, "zero_count": 10},
            ),
        ]
        for (
            family_name,
            filename,
            rows,
            usable,
            status_counts,
            reason_counts,
            filled_counts,
            cost_counts,
        ) in cases:
            with self.subTest(family=family_name):
                with tempfile.TemporaryDirectory() as directory:
                    for row in rows:
                        row["source_endpoint"] = (
                            "api_key=top-secret /private/operator/state.json"
                        )
                    rows[0]["error"] = "cookie=session-secret"
                    _write_csv(Path(directory) / filename, EXECUTION_HEADER, rows)

                    snapshot = _snapshot(directory)
                    family = _family(snapshot, family_name)

                    self.assertEqual(family["state"], "evaluated")
                    self.assertEqual(family["counts"]["expected"], 10)
                    self.assertEqual(family["counts"]["observed"], 10)
                    self.assertEqual(family["counts"]["usable"], usable)
                    self.assertEqual(family["coverage_bps"], usable * 1000)
                    self.assertEqual(
                        family["counts"]["expected_basis"]["directions"],
                        ["sell_token", "buy_token"],
                    )
                    self.assertEqual(
                        family["counts"]["expected_basis"]["notionals_usd"],
                        [1000, 5000, 10000, 50000, 100000],
                    )
                    self.assertEqual(family["status_counts"], status_counts)
                    self.assertEqual(family["reason_counts"], reason_counts)
                    self.assertEqual(
                        family["measurements"]["fields"]["filled_token_quantity"],
                        filled_counts,
                    )
                    self.assertEqual(
                        family["measurements"]["fields"]["quoted_execution_cost_usd"],
                        cost_counts,
                    )
                    self.assertEqual(family["observation_time"], {
                        "min": "2026-08-13T12:00:00Z",
                        "max": "2026-08-13T12:00:00Z",
                        "freshness_lag_seconds": 43200,
                    })
                    serialized = canonical_snapshot_bytes(snapshot).decode("utf-8")
                    self.assertNotIn("top-secret", serialized)
                    self.assertNotIn("session-secret", serialized)
                    self.assertNotIn("/private/operator", serialized)

    def test_missing_execution_scenario_is_not_synthesized(self):
        with tempfile.TemporaryDirectory() as directory:
            rows = _execution_rows("cex")[:-1]
            _write_csv(
                Path(directory) / "cex_execution_cost_latest.csv",
                EXECUTION_HEADER,
                rows,
            )

            family = _family(_snapshot(directory), "cex_execution_cost")

            self.assertEqual(family["state"], "evaluated")
            self.assertEqual(family["counts"]["expected"], 10)
            self.assertEqual(family["counts"]["observed"], 9)
            self.assertEqual(family["counts"]["usable"], 9)
            self.assertEqual(family["coverage_bps"], 9000)
            self.assertEqual(sum(family["status_counts"].values()), 9)

    def test_execution_duplicate_and_mixed_snapshot_fail_closed(self):
        cases = []
        duplicate = _execution_rows("cex")
        duplicate.append(dict(duplicate[0]))
        cases.append(("cex", duplicate, "duplicate_primary_key"))
        mixed = _execution_rows("dex")
        mixed[0]["snapshot_id"] = "dex-execution-002"
        cases.append(("dex", mixed, "mixed_snapshot_id"))
        for market_type, rows, failure_reason in cases:
            with self.subTest(market_type=market_type, reason=failure_reason):
                with tempfile.TemporaryDirectory() as directory:
                    _write_csv(
                        Path(directory) / f"{market_type}_execution_cost_latest.csv",
                        EXECUTION_HEADER,
                        rows,
                    )

                    family = _family(
                        _snapshot(directory), f"{market_type}_execution_cost"
                    )

                    self.assertEqual(family["state"], "failed")
                    self.assertEqual(family["failure_reason"], failure_reason)

    def test_execution_rejects_invalid_status_and_unsafe_public_reason(self):
        cases = [
            ("complete", "target_filled", "invalid_status", None),
            (
                "observed",
                "api_key=top-secret /private/operator/token",
                "unsafe_public_value",
                "top-secret",
            ),
        ]
        for status, reason, failure_reason, secret in cases:
            with self.subTest(reason=failure_reason):
                with tempfile.TemporaryDirectory() as directory:
                    rows = _execution_rows("cex")
                    rows[0]["status"] = status
                    rows[0]["status_reason"] = reason
                    _write_csv(
                        Path(directory) / "cex_execution_cost_latest.csv",
                        EXECUTION_HEADER,
                        rows,
                    )

                    snapshot = _snapshot(directory)
                    family = _family(snapshot, "cex_execution_cost")

                    self.assertEqual(family["state"], "failed")
                    self.assertEqual(family["failure_reason"], failure_reason)
                    if secret is not None:
                        self.assertNotIn(
                            secret,
                            canonical_snapshot_bytes(snapshot).decode("utf-8"),
                        )

    def test_unknown_execution_reason_is_bucketed_without_disclosure(self):
        secret = "sk-live-51ABCDEFprivatecredential"
        with tempfile.TemporaryDirectory() as directory:
            rows = _execution_rows("cex")
            rows[0]["status_reason"] = secret
            _write_csv(
                Path(directory) / "cex_execution_cost_latest.csv",
                EXECUTION_HEADER,
                rows,
            )

            snapshot = _snapshot(directory)
            family = _family(snapshot, "cex_execution_cost")

            self.assertEqual(family["state"], "evaluated")
            self.assertEqual(
                family["reason_counts"],
                {"other_status_reason": 1, "target_filled": 9},
            )
            self.assertNotIn(
                secret, canonical_snapshot_bytes(snapshot).decode("utf-8")
            )

    def test_dex_execution_requires_fixed_block_and_usd_lineage(self):
        cases = [
            ("usd_price_source_snapshot_id", "", "required_field_null"),
            (
                "block_timestamp",
                "2026-08-13T12:00:01Z",
                "invalid_fixed_block_lineage",
            ),
        ]
        for field, value, failure_reason in cases:
            with self.subTest(field=field):
                with tempfile.TemporaryDirectory() as directory:
                    rows = _execution_rows("dex")
                    rows[0][field] = value
                    _write_csv(
                        Path(directory) / "dex_execution_cost_latest.csv",
                        EXECUTION_HEADER,
                        rows,
                    )

                    family = _family(_snapshot(directory), "dex_execution_cost")

                    self.assertEqual(family["state"], "failed")
                    self.assertEqual(family["failure_reason"], failure_reason)

    def test_execution_timestamp_rules_and_stale_partition(self):
        cases = [
            (
                "cex",
                {"state_observed_at": "2026-08-13T12:00:00"},
                "timezone_naive_timestamp",
            ),
            (
                "dex",
                {
                    "state_observed_at": "2026-08-14T00:00:01Z",
                    "block_timestamp": "2026-08-14T00:00:01Z",
                },
                "future_observation_timestamp",
            ),
        ]
        for market_type, changes, failure_reason in cases:
            with self.subTest(market_type=market_type):
                with tempfile.TemporaryDirectory() as directory:
                    rows = _execution_rows(market_type)
                    for row in rows:
                        row.update(changes)
                    _write_csv(
                        Path(directory) / f"{market_type}_execution_cost_latest.csv",
                        EXECUTION_HEADER,
                        rows,
                    )

                    family = _family(
                        _snapshot(directory), f"{market_type}_execution_cost"
                    )

                    self.assertEqual(family["state"], "failed")
                    self.assertEqual(family["failure_reason"], failure_reason)

        with tempfile.TemporaryDirectory() as directory:
            rows = _execution_rows("cex")
            for row in rows:
                row["state_observed_at"] = "2026-08-12T00:00:00Z"
            _write_csv(
                Path(directory) / "cex_execution_cost_latest.csv",
                EXECUTION_HEADER,
                rows,
            )

            family = _family(_snapshot(directory), "cex_execution_cost")

            self.assertEqual(family["state"], "evaluated")
            self.assertEqual(
                family["observation_time"]["freshness_lag_seconds"],
                172800,
            )
            self.assertEqual(family["reason_counts"]["stale_partition"], 1)


class DataQualitySnapshotEventFactsAdapterTests(unittest.TestCase):
    def test_event_effective_time_and_precision_use_production_contract(self):
        cases = (
            {"effective_at": "not-a-time"},
            {"effective_at_precision": "banana"},
        )
        for changes in cases:
            with self.subTest(changes=changes):
                with tempfile.TemporaryDirectory() as directory:
                    _write_event_fixture(
                        directory,
                        [_event_row(**changes)],
                        {"btc-release-v1.json": _event_record()},
                    )

                    family = _family(_snapshot(directory), "event_facts")

                    self.assertEqual(family["state"], "failed")
                    self.assertEqual(
                        family["failure_reason"], "invalid_event_contract"
                    )

    def test_failed_event_evidence_bytes_remain_publication_inputs(self):
        snapshots = []
        evidence_hashes = []
        for malformed_bytes in (b"{", b"[not-json"):
            with tempfile.TemporaryDirectory() as directory:
                _write_event_fixture(
                    directory,
                    [_event_row()],
                    {"btc-release-v1.json": _event_record()},
                )
                evidence_path = (
                    Path(directory)
                    / "evidence"
                    / "events"
                    / "btc-release-v1.json"
                )
                evidence_path.write_bytes(malformed_bytes)
                evidence_hashes.append(hashlib.sha256(malformed_bytes).hexdigest())

                snapshot = _snapshot(directory)
                family = _family(snapshot, "event_facts")

                self.assertEqual(family["state"], "failed")
                self.assertEqual(family["failure_reason"], "invalid_evidence_record")
                self.assertIn(
                    evidence_hashes[-1],
                    {item["sha256"] for item in family["source"]["inputs"]},
                )
                snapshots.append(snapshot)

        self.assertNotEqual(
            snapshots[0]["publication"]["identity"],
            snapshots[1]["publication"]["identity"],
        )
        self.assertNotEqual(
            snapshots[0]["snapshot_sha256"], snapshots[1]["snapshot_sha256"]
        )

    def test_event_revisions_bind_evidence_and_count_only_latest_as_usable(self):
        with tempfile.TemporaryDirectory() as directory:
            first = _event_row()
            second = _event_row(
                revision="2",
                lifecycle="postponed",
                source_checked_at_utc="2026-08-13T13:00:00Z",
                source_record_file="btc-release-v2.json",
                recorded_at_utc="2026-08-13T13:00:00Z",
                revision_reason="Official schedule postponed",
            )
            _write_event_fixture(
                directory,
                [first, second],
                {
                    "btc-release-v1.json": _event_record(),
                    "btc-release-v2.json": _event_record(
                        checked_at="2026-08-13T13:00:00Z",
                        lifecycle="postponed",
                    ),
                },
            )

            snapshot = _snapshot(directory)
            family = _family(snapshot, "event_facts")

            self.assertEqual(family["state"], "evaluated")
            self.assertEqual(family["counts"]["expected"], 2)
            self.assertEqual(family["counts"]["observed"], 2)
            self.assertEqual(family["counts"]["usable"], 1)
            self.assertEqual(family["coverage_bps"], 5000)
            self.assertEqual(
                family["status_counts"],
                {"postponed": 1, "scheduled": 1},
            )
            self.assertEqual(
                family["reason_counts"],
                {"primary_confirmed": 2},
            )
            self.assertEqual(family["observation_time"], {
                "min": "2026-08-13T12:00:00Z",
                "max": "2026-08-13T13:00:00Z",
                "freshness_lag_seconds": 39600,
            })
            inputs = family["source"]["inputs"]
            self.assertEqual(len(inputs), 3)
            evidence_hashes = {
                hashlib.sha256(
                    (Path(directory) / "evidence" / "events" / filename).read_bytes()
                ).hexdigest()
                for filename in ("btc-release-v1.json", "btc-release-v2.json")
            }
            self.assertTrue(evidence_hashes <= {item["sha256"] for item in inputs})
            serialized = canonical_snapshot_bytes(snapshot).decode("utf-8")
            for private_value in (
                "https://example.invalid/events/btc-release",
                "btc-release-v1.json",
                "btc-release-v2.json",
                "private source-backed statement",
                "operator-private-note",
                str(Path(directory)),
            ):
                self.assertNotIn(private_value, serialized)

    def test_event_revision_keys_must_be_unique_and_contiguous(self):
        cases = [
            (
                [_event_row(), _event_row()],
                "duplicate_primary_key",
            ),
            (
                [
                    _event_row(
                        revision="2",
                        revision_reason="late first revision",
                    )
                ],
                "noncontiguous_revision",
            ),
        ]
        for rows, failure_reason in cases:
            with self.subTest(reason=failure_reason):
                with tempfile.TemporaryDirectory() as directory:
                    _write_event_fixture(
                        directory,
                        rows,
                        {"btc-release-v1.json": _event_record()},
                    )

                    family = _family(_snapshot(directory), "event_facts")

                    self.assertEqual(family["state"], "failed")
                    self.assertEqual(family["failure_reason"], failure_reason)

    def test_event_status_evidence_and_clocks_fail_closed(self):
        cases = [
            (
                _event_row(lifecycle="complete"),
                _event_record(),
                "invalid_status",
            ),
            (
                _event_row(evidence_status="rumored"),
                _event_record(),
                "invalid_evidence_status",
            ),
            (
                _event_row(
                    source_checked_at_utc="2026-08-13T12:00:00",
                    recorded_at_utc="2026-08-13T12:00:00",
                ),
                _event_record(checked_at="2026-08-13T12:00:00"),
                "timezone_naive_timestamp",
            ),
            (
                _event_row(
                    source_checked_at_utc="2026-08-14T00:00:01Z",
                    recorded_at_utc="2026-08-14T00:00:01Z",
                ),
                _event_record(checked_at="2026-08-14T00:00:01Z"),
                "future_observation_timestamp",
            ),
        ]
        for row, record, failure_reason in cases:
            with self.subTest(reason=failure_reason):
                with tempfile.TemporaryDirectory() as directory:
                    _write_event_fixture(
                        directory,
                        [row],
                        {"btc-release-v1.json": record},
                    )

                    family = _family(_snapshot(directory), "event_facts")

                    self.assertEqual(family["state"], "failed")
                    self.assertEqual(family["failure_reason"], failure_reason)

    def test_malformed_event_url_is_a_family_failure_not_a_build_exception(self):
        malformed_url = "https://[bad"
        record = _event_record()
        record["source_url"] = malformed_url
        with tempfile.TemporaryDirectory() as directory:
            _write_event_fixture(
                directory,
                [_event_row(source_url=malformed_url)],
                {"btc-release-v1.json": record},
            )

            snapshot = _snapshot(directory)
            family = _family(snapshot, "event_facts")

            self.assertEqual(family["state"], "failed")
            self.assertEqual(family["failure_reason"], "invalid_source_url")
            self.assertNotIn(malformed_url, json.dumps(family, sort_keys=True))

    def test_event_evidence_must_be_contained_regular_and_bound(self):
        cases = [
            (
                _event_row(source_record_file="../outside.json"),
                {"../outside.json": _event_record()},
                "unsafe_evidence_path",
            ),
            (
                _event_row(),
                {
                    "btc-release-v1.json": {
                        **_event_record(),
                        "source_url": "https://example.invalid/another-event",
                    }
                },
                "evidence_binding_mismatch",
            ),
            (
                _event_row(record_locator="facts.missing"),
                {"btc-release-v1.json": _event_record()},
                "invalid_record_locator",
            ),
        ]
        for row, records, failure_reason in cases:
            with self.subTest(reason=failure_reason):
                with tempfile.TemporaryDirectory() as directory:
                    _write_event_fixture(directory, [row], records)

                    family = _family(_snapshot(directory), "event_facts")

                    self.assertEqual(family["state"], "failed")
                    self.assertEqual(family["failure_reason"], failure_reason)

        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory)
            _write_event_fixture(data_dir, [_event_row()], {})
            outside = data_dir / "outside.json"
            _write_json(outside, _event_record())
            evidence_path = data_dir / "evidence" / "events" / "btc-release-v1.json"
            evidence_path.parent.mkdir(parents=True, exist_ok=True)
            evidence_path.symlink_to(outside)

            family = _family(_snapshot(data_dir), "event_facts")

            self.assertEqual(family["state"], "failed")
            self.assertEqual(family["failure_reason"], "unsafe_source_file")

    def test_event_curated_header_is_exact(self):
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory)
            (data_dir / "curated").mkdir(parents=True)
            _write_csv(
                data_dir / "curated" / "event_facts.csv",
                EVENT_HEADER + ["unexpected"],
                [{**_event_row(), "unexpected": "value"}],
            )

            family = _family(_snapshot(data_dir), "event_facts")

            self.assertEqual(family["state"], "failed")
            self.assertEqual(family["failure_reason"], "schema_mismatch")


class DataQualitySnapshotCexLifecycleAdapterTests(unittest.TestCase):
    def test_instrument_uses_exact_canonical_base_quote_shape(self):
        payload = _cex_lifecycle_payload()
        review = payload["reviews"][0]
        review["instrument"] = "BTC/USDT/EXTRA"
        review["market_id"] = "cex:crypto_com:BTC/USDT/EXTRA"
        with tempfile.TemporaryDirectory() as directory:
            _write_json(
                Path(directory) / "curated" / "cex_instrument_lifecycle.json",
                payload,
            )

            family = _family(_snapshot(directory), "cex_instrument_lifecycle")

            self.assertEqual(family["state"], "failed")
            self.assertEqual(family["failure_reason"], "invalid_market_identity")

    def test_complete_manifest_is_evaluated_with_configured_inventory_context(self):
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory)
            payload = _cex_lifecycle_payload()
            _write_json(data_dir / "curated" / "cex_instrument_lifecycle.json", payload)

            snapshot = _snapshot(data_dir)
            family = _family(snapshot, "cex_instrument_lifecycle")

            self.assertEqual(family["state"], "evaluated")
            self.assertEqual(family["counts"]["expected"], 1)
            self.assertEqual(family["counts"]["observed"], 1)
            self.assertEqual(family["counts"]["usable"], 1)
            self.assertEqual(family["coverage_bps"], 10000)
            self.assertEqual(
                family["counts"]["expected_basis"],
                {
                    "kind": "configured_market_inventory_context",
                    "configured_market_count": 30,
                    "configured_market_ids_sha256": "b" * 64,
                    "catalog_inventory_count": 919,
                },
            )
            self.assertEqual(
                family["status_counts"],
                {"absent_from_official_current_catalog": 1},
            )
            self.assertEqual(
                family["reason_counts"],
                {"instrument_absent_from_current_catalog": 1},
            )
            serialized = canonical_snapshot_bytes(snapshot).decode("utf-8")
            self.assertNotIn(payload["reviews"][0]["source_url"], serialized)
            self.assertNotIn(payload["reviews"][0]["market_id"], serialized)

    def test_manifest_structure_counts_and_market_keys_fail_closed(self):
        cases = []
        extra_root = _cex_lifecycle_payload()
        extra_root["unexpected"] = True
        cases.append((extra_root, "schema_mismatch"))
        count_mismatch = _cex_lifecycle_payload()
        count_mismatch["review_count"] = 2
        cases.append((count_mismatch, "count_mismatch"))
        duplicate = _cex_lifecycle_payload()
        duplicate["reviews"].append(dict(duplicate["reviews"][0]))
        duplicate["review_count"] = 2
        cases.append((duplicate, "duplicate_primary_key"))
        for payload, failure_reason in cases:
            with self.subTest(reason=failure_reason):
                with tempfile.TemporaryDirectory() as directory:
                    _write_json(
                        Path(directory) / "cex_instrument_lifecycle.json", payload
                    )
                    family = _family(_snapshot(directory), "cex_instrument_lifecycle")
                    self.assertEqual(family["state"], "failed")
                    self.assertEqual(family["failure_reason"], failure_reason)

    def test_manifest_status_reason_hash_and_clocks_fail_closed(self):
        cases = []
        bad_status = _cex_lifecycle_payload()
        bad_status["reviews"][0]["current_listing_status"] = "delisted"
        cases.append((bad_status, "invalid_status"))
        bad_reason = _cex_lifecycle_payload()
        bad_reason["reviews"][0]["reason_code"] = "unknown"
        cases.append((bad_reason, "status_reason_conflict"))
        bad_hash = _cex_lifecycle_payload()
        bad_hash["reviews"][0]["response_sha256"] = "not-a-hash"
        cases.append((bad_hash, "invalid_source_lineage"))
        naive = _cex_lifecycle_payload(checked_at="2026-08-13T12:00:00")
        cases.append((naive, "timezone_naive_timestamp"))
        future = _cex_lifecycle_payload(checked_at="2026-08-14T00:00:01Z")
        cases.append((future, "future_observation_timestamp"))
        for payload, failure_reason in cases:
            with self.subTest(reason=failure_reason):
                with tempfile.TemporaryDirectory() as directory:
                    _write_json(
                        Path(directory) / "cex_instrument_lifecycle.json", payload
                    )
                    family = _family(_snapshot(directory), "cex_instrument_lifecycle")
                    self.assertEqual(family["state"], "failed")
                    self.assertEqual(family["failure_reason"], failure_reason)

    def test_stale_manifest_remains_evaluated(self):
        with tempfile.TemporaryDirectory() as directory:
            _write_json(
                Path(directory) / "cex_instrument_lifecycle.json",
                _cex_lifecycle_payload(checked_at="2026-08-12T00:00:00+00:00"),
            )
            family = _family(_snapshot(directory), "cex_instrument_lifecycle")
            self.assertEqual(family["state"], "evaluated")
            self.assertEqual(family["reason_counts"]["stale_partition"], 1)
            self.assertEqual(
                family["observation_time"]["freshness_lag_seconds"], 172800
            )


class DataQualitySnapshotMarketLifecycleAdapterTests(unittest.TestCase):
    def test_cex_evidence_must_bind_the_exact_reviewed_market(self):
        review = _market_review_revision()
        review["source_checks"][0]["url"] = (
            "https://api.upbit.com/v1/ticker?markets=USDT-BTC"
        )
        review["source_checks"][0]["observations"] = {
            "market": "USDT-BTC",
            "last_trade_date_utc": "2026-08-11",
        }
        with tempfile.TemporaryDirectory() as directory:
            _write_json(
                Path(directory) / "curated" / "market_lifecycle_reviews.json",
                _market_lifecycle_payload([review]),
            )

            snapshot = _snapshot(directory)
            family = _family(snapshot, "market_lifecycle_reviews")

            self.assertEqual(family["state"], "failed")
            self.assertEqual(
                family["failure_reason"], "invalid_source_lineage"
            )
            serialized = canonical_snapshot_bytes(snapshot).decode("utf-8")
            self.assertNotIn("USDT-BTC", serialized)

    def test_revision_inventory_counts_only_latest_as_usable(self):
        with tempfile.TemporaryDirectory() as directory:
            first = _market_review_revision()
            latest = _market_review_revision(
                revision=2,
                review_status="withdrawn",
                checked_at="2026-08-13T13:00:00Z",
                reviewed_at="2026-08-13T13:00:00Z",
            )
            payload = _market_lifecycle_payload([first, latest])
            _write_json(
                Path(directory) / "curated" / "market_lifecycle_reviews.json",
                payload,
            )

            snapshot = _snapshot(directory)
            family = _family(snapshot, "market_lifecycle_reviews")

            self.assertEqual(family["state"], "evaluated")
            self.assertEqual(family["counts"]["expected"], 2)
            self.assertEqual(family["counts"]["observed"], 2)
            self.assertEqual(family["counts"]["usable"], 1)
            self.assertEqual(family["coverage_bps"], 5000)
            self.assertEqual(
                family["counts"]["expected_basis"],
                {
                    "kind": "declared_revision_inventory",
                    "revision_count": 2,
                    "review_id_count": 1,
                    "active_disposition_count": 0,
                },
            )
            self.assertEqual(family["status_counts"], {"disposed": 1, "withdrawn": 1})
            self.assertEqual(family["reason_counts"], {"no_candles": 1, "withdrawn": 1})
            self.assertEqual(
                family["observation_time"],
                {
                    "min": "2026-08-13T12:00:00Z",
                    "max": "2026-08-13T13:00:00Z",
                    "freshness_lag_seconds": 39600,
                },
            )
            serialized = canonical_snapshot_bytes(snapshot).decode("utf-8")
            for private_value in (
                first["market_id"],
                first["review_id"],
                first["review_actor"],
                first["disposition_note"],
                first["source_checks"][0]["url"],
                first["source_checks"][0]["observations"]["market"],
            ):
                self.assertNotIn(private_value, serialized)

    def test_root_count_and_revision_keys_fail_closed(self):
        count_mismatch = _market_lifecycle_payload([_market_review_revision()])
        count_mismatch["review_count"] = 2
        duplicate = _market_lifecycle_payload(
            [_market_review_revision(), _market_review_revision()]
        )
        noncontiguous = _market_lifecycle_payload(
            [_market_review_revision(revision=2)]
        )
        for payload, failure_reason in (
            (count_mismatch, "count_mismatch"),
            (duplicate, "duplicate_primary_key"),
            (noncontiguous, "noncontiguous_revision"),
        ):
            with self.subTest(reason=failure_reason):
                with tempfile.TemporaryDirectory() as directory:
                    _write_json(
                        Path(directory) / "market_lifecycle_reviews.json", payload
                    )
                    family = _family(_snapshot(directory), "market_lifecycle_reviews")
                    self.assertEqual(family["state"], "failed")
                    self.assertEqual(family["failure_reason"], failure_reason)

    def test_disposed_and_withdrawn_rules_fail_closed(self):
        disposed = _market_review_revision()
        disposed["disposition_reason_code"] = None
        withdrawn = _market_review_revision(review_status="withdrawn")
        withdrawn["market_lifecycle"] = "listed_quote_market_dormant"
        for review in (disposed, withdrawn):
            with self.subTest(status=review["review_status"]):
                with tempfile.TemporaryDirectory() as directory:
                    _write_json(
                        Path(directory) / "market_lifecycle_reviews.json",
                        _market_lifecycle_payload([review]),
                    )
                    family = _family(_snapshot(directory), "market_lifecycle_reviews")
                    self.assertEqual(family["state"], "failed")
                    self.assertEqual(family["failure_reason"], "invalid_disposition")

    def test_review_source_check_hash_status_and_clocks_fail_closed(self):
        cases = []
        bad_hash = _market_review_revision()
        bad_hash["source_checks"][0]["response_sha256"] = "bad"
        cases.append((bad_hash, "2026-08-13T13:00:00Z", "invalid_source_lineage"))
        bad_status = _market_review_revision()
        bad_status["source_checks"][0]["http_status"] = 500
        cases.append((bad_status, "2026-08-13T13:00:00Z", "invalid_source_status"))
        naive = _market_review_revision(
            checked_at="2026-08-13T12:00:00",
            reviewed_at="2026-08-13T12:00:00",
        )
        cases.append((naive, "2026-08-13T13:00:00", "timezone_naive_timestamp"))
        future = _market_review_revision(
            checked_at="2026-08-14T00:00:01Z",
            reviewed_at="2026-08-14T00:00:01Z",
        )
        cases.append((future, "2026-08-14T00:00:01Z", "future_observation_timestamp"))
        for review, generated_at, failure_reason in cases:
            with self.subTest(reason=failure_reason):
                with tempfile.TemporaryDirectory() as directory:
                    _write_json(
                        Path(directory) / "market_lifecycle_reviews.json",
                        _market_lifecycle_payload([review], generated_at=generated_at),
                    )
                    family = _family(_snapshot(directory), "market_lifecycle_reviews")
                    self.assertEqual(family["state"], "failed")
                    self.assertEqual(family["failure_reason"], failure_reason)


class DataQualitySnapshotTrackedRepositoryDataTests(unittest.TestCase):
    def test_only_tracked_event_and_lifecycle_families_are_evaluated(self):
        snapshot = build_snapshot(
            data_dir=PROJECT_ROOT / "data",
            generated_at_utc="2026-08-14T00:00:00Z",
            window_end=date(2026, 8, 13),
            window_days=30,
            application_sha="a" * 40,
        )
        states = {family["name"]: family["state"] for family in snapshot["families"]}
        self.assertEqual(
            {name for name, state in states.items() if state == "evaluated"},
            {
                "event_facts",
                "cex_instrument_lifecycle",
                "market_lifecycle_reviews",
            },
        )
        self.assertEqual(
            {name for name, state in states.items() if state == "not_evaluated"},
            set(FAMILY_NAMES)
            - {
                "event_facts",
                "cex_instrument_lifecycle",
                "market_lifecycle_reviews",
            },
        )
        self.assertEqual(
            snapshot["summary"],
            {
                "evaluated_family_count": 3,
                "failed_family_count": 0,
                "not_evaluated_family_count": 9,
                "total_family_count": 12,
            },
        )
        for name in (
            "event_facts",
            "cex_instrument_lifecycle",
            "market_lifecycle_reviews",
        ):
            family = _family(snapshot, name)
            self.assertGreater(family["counts"]["observed"], 0)
            self.assertGreater(family["counts"]["usable"], 0)


class DataQualitySnapshotRoutePointerTests(unittest.TestCase):
    def test_route_entities_use_uniform_contract_shape(self):
        required_fields = {
            "grain",
            "primary_key",
            "counts",
            "coverage_bps",
            "duplicate_primary_key",
            "status_counts",
            "reason_counts",
        }
        cases = []
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory) / "data"
            _publish_route_opportunity_fixture(data_dir)
            cases.append(
                (
                    _family(_snapshot(data_dir), "route_cohort_opportunity"),
                    {
                        "opportunities": (
                            "route_x_requested_notional_opportunity",
                            ["opportunity_id"],
                        ),
                        "routes": ("directed_route", ["route_id"]),
                        "markets": ("route_leg_market", ["market_id"]),
                        "legs": ("route_leg", ["leg_id"]),
                        "cost_components": (
                            "route_opportunity_cost_component",
                            ["opportunity_id", "component_type", "leg"],
                        ),
                    },
                )
            )
            for family, contracts in cases:
                for name, (grain, primary_key) in contracts.items():
                    with self.subTest(family=family["name"], entity=name):
                        entity = family["entities"][name]
                        self.assertEqual(set(entity), required_fields)
                        self.assertEqual(entity["grain"], grain)
                        self.assertEqual(entity["primary_key"], primary_key)
                        self.assertEqual(
                            set(entity["counts"]),
                            {"expected", "observed", "usable", "expected_basis"},
                        )
                        self.assertEqual(
                            set(entity["duplicate_primary_key"]),
                            {"count", "rate_bps"},
                        )

        fixture = _publish_route_shadow_fixture()
        try:
            data_dir = Path(fixture.temporary.name) / "data"
            family = _family(
                _snapshot(data_dir), "route_shadow_route_cost_evidence"
            )
            contracts = {
                "bindings": ("route_notional_cost_binding", ["binding_key"]),
                "runs": ("shadow_run", ["run_id"]),
                "markets": ("selected_market", ["market_id"]),
                "transcripts": ("route_cost_transcript", ["transcript_key"]),
            }
            for name, (grain, primary_key) in contracts.items():
                with self.subTest(family=family["name"], entity=name):
                    entity = family["entities"][name]
                    self.assertEqual(set(entity), required_fields)
                    self.assertEqual(entity["grain"], grain)
                    self.assertEqual(entity["primary_key"], primary_key)
                    self.assertEqual(
                        set(entity["counts"]),
                        {"expected", "observed", "usable", "expected_basis"},
                    )
                    self.assertEqual(
                        set(entity["duplicate_primary_key"]),
                        {"count", "rate_bps"},
                    )
        finally:
            fixture.doCleanups()

    def test_failed_route_artifact_bytes_remain_publication_inputs(self):
        snapshots = []
        artifact_hashes = []
        for suffix in (b"broken-one", b"broken-two"):
            with tempfile.TemporaryDirectory() as directory:
                data_dir = Path(directory) / "data"
                pointer = _publish_route_opportunity_fixture(data_dir)
                artifact_path = (
                    data_dir
                    / "routes"
                    / "bundles"
                    / pointer["route_cohort_id"]
                    / "route_legs.csv"
                )
                artifact_path.write_bytes(artifact_path.read_bytes() + suffix)
                artifact_hashes.append(
                    hashlib.sha256(artifact_path.read_bytes()).hexdigest()
                )

                snapshot = _snapshot(data_dir)
                family = _family(snapshot, "route_cohort_opportunity")

                self.assertEqual(family["state"], "failed")
                self.assertEqual(family["failure_reason"], "route_bundle_invalid")
                self.assertIn(
                    artifact_hashes[-1],
                    {item["sha256"] for item in family["source"]["inputs"]},
                )
                snapshots.append(snapshot)

        self.assertNotEqual(
            snapshots[0]["publication"]["identity"],
            snapshots[1]["publication"]["identity"],
        )
        self.assertNotEqual(
            snapshots[0]["snapshot_sha256"], snapshots[1]["snapshot_sha256"]
        )

    def test_failed_shadow_sidecar_bytes_remain_publication_inputs(self):
        snapshots = []
        sidecar_hashes = []
        for replacement in (b"{", b"[invalid-shadow"):
            fixture = _publish_route_shadow_fixture()
            try:
                data_dir = Path(fixture.temporary.name) / "data"
                pointer_path = (
                    data_dir / "local" / "routes" / "shadow" / "latest.json"
                )
                pointer = json.loads(pointer_path.read_text())
                sidecar_path = (
                    pointer_path.parent
                    / "runs"
                    / pointer["run_id"]
                    / "route-cost-evidence.json"
                )
                sidecar_path.write_bytes(replacement)
                sidecar_hashes.append(hashlib.sha256(replacement).hexdigest())

                snapshot = _snapshot(data_dir)
                family = _family(
                    snapshot, "route_shadow_route_cost_evidence"
                )

                self.assertEqual(family["state"], "failed")
                self.assertEqual(family["failure_reason"], "route_bundle_invalid")
                self.assertIn(
                    sidecar_hashes[-1],
                    {item["sha256"] for item in family["source"]["inputs"]},
                )
                snapshots.append(snapshot)
            finally:
                fixture.doCleanups()

        self.assertNotEqual(
            snapshots[0]["publication"]["identity"],
            snapshots[1]["publication"]["identity"],
        )
        self.assertNotEqual(
            snapshots[0]["snapshot_sha256"], snapshots[1]["snapshot_sha256"]
        )

    def test_missing_route_pointers_remain_not_evaluated(self):
        with tempfile.TemporaryDirectory() as directory:
            snapshot = _snapshot(directory)
            for name in (
                "route_cohort_opportunity",
                "route_shadow_route_cost_evidence",
            ):
                family = _family(snapshot, name)
                self.assertEqual(family["state"], "not_evaluated")
                self.assertEqual(
                    family["not_evaluated_reason"], "route_pointer_missing"
                )
                self.assertIsNone(family["counts"]["expected"])
                self.assertIsNone(family["counts"]["observed"])
                self.assertIsNone(family["counts"]["usable"])

    def test_present_path_unsafe_route_pointers_fail_closed(self):
        public_pointer = {
            "schema": "route_opportunity_pointer/v1",
            "bundle_stage": "route_opportunity/v1",
            "route_cohort_id": "../../private-cohort",
            "manifest_sha256": "a" * 64,
            "core_manifest_sha256": "b" * 64,
            "core_pointer_sha256": "c" * 64,
        }
        shadow_pointer = {
            "schema": "route_shadow_pointer/v1",
            "run_id": "../private-run",
            "phase": "canary",
            "route_cohort_id": "cohort:" + "d" * 64,
            "phase_state_sha256": "e" * 64,
            "phase_transition_id": None,
            "core_pointer_sha256": "f" * 64,
            "core_manifest_sha256": "1" * 64,
            "route_universe_sha256": "2" * 64,
            "route_cost_evidence_sha256": "3" * 64,
            "baseline_manifest_sha256": "4" * 64,
            "candidate_source_generation": "5" * 64,
            "audit_sha256": "6" * 64,
        }
        cases = (
            (
                "route_cohort_opportunity",
                Path("routes") / "latest.json",
                public_pointer,
            ),
            (
                "route_shadow_route_cost_evidence",
                Path("routes") / "shadow" / "latest.json",
                shadow_pointer,
            ),
        )
        for family_name, relative_path, pointer in cases:
            with self.subTest(family=family_name):
                with tempfile.TemporaryDirectory() as directory:
                    _write_json(Path(directory) / relative_path, pointer)
                    family = _family(_snapshot(directory), family_name)
                    self.assertEqual(family["state"], "failed")
                    self.assertEqual(family["failure_reason"], "unsafe_route_pointer")
                    serialized = json.dumps(family, sort_keys=True)
                    self.assertNotIn("private-cohort", serialized)
                    self.assertNotIn("private-run", serialized)
                    self.assertNotIn(str(Path(directory)), serialized)

    def test_malformed_route_pointer_fails_with_public_reason(self):
        pointer = {
            "schema": "route_opportunity_pointer/v1",
            "bundle_stage": "route_opportunity/v1",
            "route_cohort_id": "cohort:" + "a" * 64,
            "manifest_sha256": "b" * 64,
            "core_manifest_sha256": "c" * 64,
            "core_pointer_sha256": "d" * 64,
            "unexpected_private_payload": "credential=do-not-project",
        }
        with tempfile.TemporaryDirectory() as directory:
            _write_json(Path(directory) / "routes" / "latest.json", pointer)
            snapshot = _snapshot(directory)
            family = _family(snapshot, "route_cohort_opportunity")

            self.assertEqual(family["state"], "failed")
            self.assertEqual(family["failure_reason"], "invalid_route_pointer")
            self.assertNotIn(
                "credential=do-not-project",
                canonical_snapshot_bytes(snapshot).decode("utf-8"),
            )

    def test_route_pointer_intermediate_symlink_fails_containment(self):
        pointer = {
            "schema": "route_opportunity_pointer/v1",
            "bundle_stage": "route_opportunity/v1",
            "route_cohort_id": "cohort:" + "a" * 64,
            "manifest_sha256": "b" * 64,
            "core_manifest_sha256": "c" * 64,
            "core_pointer_sha256": "d" * 64,
        }
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data_dir = root / "data"
            escaped_routes = root / "escaped-routes"
            data_dir.mkdir()
            _write_json(escaped_routes / "latest.json", pointer)
            (data_dir / "routes").symlink_to(
                escaped_routes, target_is_directory=True
            )

            snapshot = _snapshot(data_dir)
            family = _family(snapshot, "route_cohort_opportunity")

            self.assertEqual(family["state"], "failed")
            self.assertEqual(family["failure_reason"], "unsafe_source_file")
            self.assertNotIn(str(escaped_routes), json.dumps(family, sort_keys=True))

    def test_valid_actual_schema_route_opportunity_bundle_is_evaluated(self):
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory) / "data"
            pointer = _publish_route_opportunity_fixture(data_dir)
            bundle_root = (
                data_dir
                / "routes"
                / "bundles"
                / pointer["route_cohort_id"]
            )
            manifest = json.loads((bundle_root / "manifest.json").read_text())

            snapshot = _snapshot(data_dir)
            family = _family(snapshot, "route_cohort_opportunity")

            self.assertEqual(family["state"], "evaluated")
            self.assertEqual(family["counts"]["expected"], 5)
            self.assertEqual(family["counts"]["observed"], 5)
            self.assertEqual(family["counts"]["usable"], 5)
            self.assertEqual(family["coverage_bps"], 10000)
            self.assertEqual(
                family["counts"]["expected_basis"],
                {
                    "kind": "sealed_route_opportunity_manifest",
                    "manifest_sha256": pointer["manifest_sha256"],
                    "core_manifest_sha256": pointer["core_manifest_sha256"],
                    "core_pointer_sha256": pointer["core_pointer_sha256"],
                    "route_cohort_identity_sha256": hashlib.sha256(
                        (
                            "data_quality_snapshot/v1/route_cohort\0"
                            + pointer["route_cohort_id"]
                        ).encode("utf-8")
                    ).hexdigest(),
                },
            )
            self.assertEqual(
                {
                    name: entity["counts"]
                    for name, entity in family["entities"].items()
                },
                {
                    "opportunities": {
                        "expected": 5,
                        "observed": 5,
                        "usable": 5,
                        "expected_basis": family["entities"]["opportunities"]["counts"]["expected_basis"],
                    },
                    "routes": {"expected": 1, "observed": 1, "usable": 1, "expected_basis": family["entities"]["routes"]["counts"]["expected_basis"]},
                    "markets": {"expected": 2, "observed": 2, "usable": 2, "expected_basis": family["entities"]["markets"]["counts"]["expected_basis"]},
                    "legs": {"expected": 2, "observed": 2, "usable": 2, "expected_basis": family["entities"]["legs"]["counts"]["expected_basis"]},
                    "cost_components": {"expected": 15, "observed": 15, "usable": 15, "expected_basis": family["entities"]["cost_components"]["counts"]["expected_basis"]},
                },
            )
            self.assertEqual(
                family["status_counts"], {"executable_candidate": 5}
            )
            self.assertEqual(
                family["reason_counts"],
                {"positive_strict_net_edge": 5, "stale_partition": 1},
            )
            self.assertEqual(
                family["observation_time"],
                {
                    "min": "2026-08-01T12:00:00Z",
                    "max": "2026-08-01T12:01:00Z",
                    "freshness_lag_seconds": 1079940,
                },
            )
            source_hashes = {
                item["sha256"] for item in family["source"]["inputs"]
            }
            self.assertEqual(len(family["source"]["inputs"]), 6)
            self.assertIn(
                hashlib.sha256(
                    (data_dir / "routes" / "latest.json").read_bytes()
                ).hexdigest(),
                source_hashes,
            )
            self.assertTrue(
                {item["sha256"] for item in manifest["files"].values()}
                <= source_hashes
            )
            self.assertIn(pointer["manifest_sha256"], source_hashes)
            serialized = canonical_snapshot_bytes(snapshot).decode("utf-8")
            private_values = (
                pointer["route_cohort_id"],
                "task7-source-run",
                "cex:binance:AAVE/USDT",
                "cex:bybit:AAVE/USDT",
                "route:AAVE:",
                "api.binance.com",
                str(Path(directory)),
            )
            for value in private_values:
                self.assertNotIn(value, serialized)

    def test_valid_actual_schema_shadow_bundle_is_evaluated(self):
        fixture = _publish_route_shadow_fixture()
        try:
            data_dir = Path(fixture.temporary.name) / "data"
            pointer_path = data_dir / "local" / "routes" / "shadow" / "latest.json"
            pointer = json.loads(pointer_path.read_text())

            snapshot = _snapshot(data_dir)
            family = _family(snapshot, "route_shadow_route_cost_evidence")

            self.assertEqual(family["state"], "evaluated")
            self.assertEqual(
                family["counts"],
                {
                    "expected": 0,
                    "observed": 0,
                    "usable": 0,
                    "expected_basis": {
                        "kind": "sealed_route_cost_evidence",
                        "phase": "canary",
                        "phase_state_sha256": pointer["phase_state_sha256"],
                        "phase_transition_id": None,
                        "audit_sha256": pointer["audit_sha256"],
                        "core_pointer_sha256": pointer["core_pointer_sha256"],
                        "core_manifest_sha256": pointer["core_manifest_sha256"],
                        "route_universe_sha256": pointer["route_universe_sha256"],
                        "route_cost_evidence_sha256": pointer[
                            "route_cost_evidence_sha256"
                        ],
                        "baseline_manifest_sha256": pointer[
                            "baseline_manifest_sha256"
                        ],
                        "candidate_source_generation": pointer[
                            "candidate_source_generation"
                        ],
                        "route_cohort_identity_sha256": hashlib.sha256(
                            (
                                "data_quality_snapshot/v1/route_cohort\0"
                                + pointer["route_cohort_id"]
                            ).encode("utf-8")
                        ).hexdigest(),
                        "run_identity_sha256": hashlib.sha256(
                            (
                                "data_quality_snapshot/v1/route_shadow_run\0"
                                + pointer["run_id"]
                            ).encode("utf-8")
                        ).hexdigest(),
                    },
                },
            )
            self.assertIsNone(family["coverage_bps"])
            self.assertEqual(family["status_counts"], {})
            self.assertEqual(family["reason_counts"], {"stale_partition": 1})
            self.assertEqual(
                family["observation_time"],
                {
                    "min": "2026-08-01T12:00:01.000000000Z",
                    "max": "2026-08-01T12:00:04Z",
                    "freshness_lag_seconds": 1079996,
                },
            )
            self.assertEqual(
                {
                    name: entity["counts"]
                    for name, entity in family["entities"].items()
                },
                {
                    "bindings": {"expected": 0, "observed": 0, "usable": 0, "expected_basis": family["entities"]["bindings"]["counts"]["expected_basis"]},
                    "runs": {"expected": 1, "observed": 1, "usable": 1, "expected_basis": family["entities"]["runs"]["counts"]["expected_basis"]},
                    "markets": {"expected": 0, "observed": 0, "usable": 0, "expected_basis": family["entities"]["markets"]["counts"]["expected_basis"]},
                    "transcripts": {"expected": 0, "observed": 0, "usable": 0, "expected_basis": family["entities"]["transcripts"]["counts"]["expected_basis"]},
                },
            )
            self.assertEqual(len(family["source"]["inputs"]), 10)
            source_hashes = {
                item["sha256"] for item in family["source"]["inputs"]
            }
            self.assertIn(hashlib.sha256(pointer_path.read_bytes()).hexdigest(), source_hashes)
            for field in (
                "audit_sha256",
                "core_manifest_sha256",
                "route_cost_evidence_sha256",
                "baseline_manifest_sha256",
            ):
                self.assertIn(pointer[field], source_hashes)

            serialized = canonical_snapshot_bytes(snapshot).decode("utf-8")
            private_values = (
                pointer["route_cohort_id"],
                pointer["run_id"],
                "cex:binance:AAVE/USDT",
                "cex:bybit:AAVE/USDT",
                "api.binance.com",
                str(data_dir),
            )
            for value in private_values:
                self.assertNotIn(value, serialized)
        finally:
            fixture.doCleanups()

    def test_missing_cost_sidecar_fails_only_shadow_without_zero_success(self):
        fixture = _publish_route_shadow_fixture()
        try:
            data_dir = Path(fixture.temporary.name) / "data"
            pointer_path = data_dir / "local" / "routes" / "shadow" / "latest.json"
            pointer = json.loads(pointer_path.read_text())
            (
                data_dir
                / "local"
                / "routes"
                / "shadow"
                / "runs"
                / pointer["run_id"]
                / "route-cost-evidence.json"
            ).unlink()

            snapshot = _snapshot(data_dir)
            public_family = _family(snapshot, "route_cohort_opportunity")
            shadow_family = _family(
                snapshot, "route_shadow_route_cost_evidence"
            )

            self.assertEqual(public_family["state"], "not_evaluated")
            self.assertEqual(
                public_family["not_evaluated_reason"], "route_pointer_missing"
            )
            self.assertEqual(shadow_family["state"], "failed")
            self.assertEqual(
                shadow_family["failure_reason"], "route_sidecar_missing"
            )
            self.assertIsNone(shadow_family["counts"]["expected"])
            self.assertIsNone(shadow_family["counts"]["observed"])
            self.assertIsNone(shadow_family["counts"]["usable"])
            self.assertIsNone(shadow_family["coverage_bps"])
        finally:
            fixture.doCleanups()


class DataQualitySnapshotPointInTimeTests(unittest.TestCase):
    def test_timezone_naive_timestamp_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            _write_csv(
                Path(directory) / "dex_pool_tvl_latest.csv",
                TVL_HEADER,
                [_tvl_row(observed_at="2026-08-13T12:00:00")],
            )

            family = _family(_snapshot(directory), "tvl")

            self.assertEqual(family["state"], "failed")
            self.assertEqual(family["failure_reason"], "timezone_naive_timestamp")

    def test_future_timestamp_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            _write_csv(
                Path(directory) / "dex_pool_tvl_latest.csv",
                TVL_HEADER,
                [_tvl_row(observed_at="2026-08-14T00:00:01Z")],
            )

            family = _family(_snapshot(directory), "tvl")

            self.assertEqual(family["state"], "failed")
            self.assertEqual(family["failure_reason"], "future_observation_timestamp")

    def test_latest_file_with_two_snapshot_ids_is_mixed_grain(self):
        with tempfile.TemporaryDirectory() as directory:
            _write_csv(
                Path(directory) / "dex_pool_tvl_latest.csv",
                TVL_HEADER,
                [_tvl_row(), _tvl_row(snapshot_id="snapshot-002", pool_address="0x" + "2" * 40)],
            )

            family = _family(_snapshot(directory), "tvl")

            self.assertEqual(family["state"], "failed")
            self.assertEqual(family["failure_reason"], "mixed_snapshot_id")

    def test_tvl_case_variant_pool_identity_is_a_duplicate(self):
        lower_pool = "0x" + "a" * 40
        upper_pool = "0X" + "A" * 40
        with tempfile.TemporaryDirectory() as directory:
            _write_csv(
                Path(directory) / "dex_pool_tvl_latest.csv",
                TVL_HEADER,
                [
                    _tvl_row(chain="ethereum", pool_address=lower_pool),
                    _tvl_row(chain="Ethereum", pool_address=upper_pool),
                ],
            )

            family = _family(_snapshot(directory), "tvl")

            self.assertEqual(family["state"], "failed")
            self.assertEqual(family["failure_reason"], "duplicate_primary_key")
            self.assertEqual(
                family["duplicate_primary_key"],
                {"count": 1, "rate_bps": 5000},
            )

    def test_invalid_tvl_snapshot_id_fails_without_projecting_secret(self):
        with tempfile.TemporaryDirectory() as directory:
            secret = "api_key=top-secret /private/operator/token"
            _write_csv(
                Path(directory) / "dex_pool_tvl_latest.csv",
                TVL_HEADER,
                [_tvl_row(snapshot_id=secret)],
            )

            snapshot = _snapshot(directory)
            family = _family(snapshot, "tvl")

            self.assertEqual(family["state"], "failed")
            self.assertEqual(family["failure_reason"], "invalid_snapshot_id")
            self.assertNotIn(secret, canonical_snapshot_bytes(snapshot).decode("utf-8"))

    def test_valid_tvl_snapshot_id_is_published_only_as_an_opaque_hash(self):
        snapshot_id = "customer_secret_abcdef"
        expected_hash = hashlib.sha256(
            (
                "data_quality_snapshot/v1/tvl_snapshot\0" + snapshot_id
            ).encode("utf-8")
        ).hexdigest()
        with tempfile.TemporaryDirectory() as directory:
            _write_csv(
                Path(directory) / "dex_pool_tvl_latest.csv",
                TVL_HEADER,
                [_tvl_row(snapshot_id=snapshot_id)],
            )

            snapshot = _snapshot(directory)
            family = _family(snapshot, "tvl")

            self.assertEqual(family["state"], "evaluated")
            self.assertEqual(
                family["counts"]["expected_basis"],
                {
                    "kind": "latest_file_inventory",
                    "snapshot_id_sha256": expected_hash,
                },
            )
            self.assertNotIn(
                snapshot_id, canonical_snapshot_bytes(snapshot).decode("utf-8")
            )

    def test_tvl_extra_trailing_column_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "dex_pool_tvl_latest.csv"
            _write_csv(path, TVL_HEADER, [_tvl_row()])
            path.write_text(
                path.read_text(encoding="utf-8").replace(
                    ",observed,observed,\n", ",observed,observed,,tail\n"
                ),
                encoding="utf-8",
            )

            family = _family(_snapshot(directory), "tvl")

            self.assertEqual(family["state"], "failed")
            self.assertEqual(family["failure_reason"], "schema_mismatch")

    def test_stale_partition_remains_evaluated_and_visible(self):
        with tempfile.TemporaryDirectory() as directory:
            _write_csv(
                Path(directory) / "dex_pool_tvl_latest.csv",
                TVL_HEADER,
                [_tvl_row(observed_at="2026-08-12T00:00:00Z")],
            )

            family = _family(_snapshot(directory), "tvl")

            self.assertEqual(family["state"], "evaluated")
            self.assertEqual(family["counts"]["expected"], 1)
            self.assertEqual(family["counts"]["observed"], 1)
            self.assertEqual(family["counts"]["usable"], 1)
            self.assertEqual(family["coverage_bps"], 10000)
            self.assertEqual(family["observation_time"], {
                "min": "2026-08-12T00:00:00Z",
                "max": "2026-08-12T00:00:00Z",
                "freshness_lag_seconds": 172800,
            })
            self.assertEqual(
                family["reason_counts"],
                {"observed": 1, "stale_partition": 1},
            )


class DataQualitySnapshotDeterminismTests(unittest.TestCase):
    def test_identical_inputs_produce_identical_canonical_content_and_hash(self):
        with tempfile.TemporaryDirectory() as directory:
            first = _snapshot(directory)
            second = _snapshot(directory)

            self.assertEqual(first, second)
            self.assertEqual(canonical_snapshot_bytes(first), canonical_snapshot_bytes(second))
            self.assertEqual(first["snapshot_sha256"], second["snapshot_sha256"])
            unsigned = dict(first)
            del unsigned["snapshot_sha256"]
            expected = hashlib.sha256(
                (
                    json.dumps(
                        unsigned,
                        ensure_ascii=False,
                        allow_nan=False,
                        sort_keys=True,
                        separators=(",", ":"),
                    )
                    + "\n"
                ).encode("utf-8")
            ).hexdigest()
            self.assertEqual(first["snapshot_sha256"], expected)

    def test_publication_identity_changes_with_input_identity(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "dex_pool_tvl_latest.csv"
            _write_csv(path, TVL_HEADER, [_tvl_row(source_endpoint="https://example.invalid/one")])
            first = _snapshot(directory)
            _write_csv(path, TVL_HEADER, [_tvl_row(source_endpoint="https://example.invalid/two")])
            second = _snapshot(directory)

            self.assertNotEqual(first["publication"]["identity"], second["publication"]["identity"])

    def test_output_never_projects_private_path_cookie_or_api_key(self):
        with tempfile.TemporaryDirectory() as directory:
            private_path = str(Path(directory) / "operator" / "secret.json")
            secret = "api_key=top-secret cookie=session-secret " + private_path
            _write_csv(
                Path(directory) / "dex_pool_tvl_latest.csv",
                TVL_HEADER,
                [_tvl_row(error=secret, source_endpoint=secret)],
            )

            serialized = canonical_snapshot_bytes(_snapshot(directory)).decode("utf-8")

            self.assertNotIn("top-secret", serialized)
            self.assertNotIn("session-secret", serialized)
            self.assertNotIn(private_path, serialized)
            self.assertNotIn(str(Path(directory)), serialized)
            self.assertNotIn("api_key", serialized)
            self.assertNotIn("cookie", serialized)

    def test_cli_writes_the_exact_canonical_bytes(self):
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory) / "input"
            data_dir.mkdir()
            output = Path(directory) / "quality" / "latest.json"

            completed = subprocess.run(
                [
                    sys.executable,
                    "scripts/build_data_quality_snapshot.py",
                    "--data-dir",
                    str(data_dir),
                    "--generated-at-utc",
                    "2026-08-14T00:00:00Z",
                    "--window-end",
                    "2026-08-13",
                    "--window-days",
                    "30",
                    "--application-sha",
                    "a" * 40,
                    "--output",
                    str(output),
                ],
                cwd=Path(__file__).resolve().parents[1],
                check=False,
                capture_output=True,
                text=True,
            )

            self.assertEqual(completed.returncode, 0, completed.stderr)
            expected = canonical_snapshot_bytes(_snapshot(data_dir))
            self.assertEqual(output.read_bytes(), expected)
            self.assertNotIn(str(data_dir), output.read_text(encoding="utf-8"))


if __name__ == "__main__":
    unittest.main()
