import csv
import hashlib
import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal
import multiprocessing
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import textwrap
import unittest
import urllib.error
from concurrent.futures import ThreadPoolExecutor
from threading import Event, Lock, Thread as TestThread, enumerate as enumerate_threads
import time
from unittest.mock import patch

from scripts.collect_route_cohort import (
    _ForkProcessExecutor,
    _RouteCollectionResult,
    _accepted_evidence_for_attachment,
    _attachment_authority_bytes,
    _final_route_leg_projection,
    _validated_attachment_authority,
    _safe_leg_projection,
    _validated_universe,
    collect_route_cohort as _collect_route_cohort,
    collect_unique_route_legs,
    attach_typed_source_lineage,
    finalize_route_opportunity_bundle,
    main,
    materialize_route_leg_rows,
    publish_typed_source_manifest,
    _default_dex_block_resolver,
)
from scripts.fetch_dex_depth import (
    ROUTE_V2_FEE_PROOF_SHA256,
    freeze_v2_pool_state,
)
from scripts.fetch_cex_depth import collect_cex_market_observation, observed_row
from scripts.live_cex_research import build_live_cex_research_universe
from scripts.route_opportunity_pipeline import (
    _load_cex_sources,
    finalize_public_cex_research_opportunities,
)
from scripts.route_publication import (
    load_latest_complete_route_bundle,
    load_latest_route_cohort,
    publish_route_cohort_bundle,
)
from scripts.route_quantity import V2PoolState, V2_FEE_FORMULA


_TEST_RAW_DIRECTORY = tempfile.TemporaryDirectory(prefix="route-cohort-tests-")


def _install_attachment_authority_fixture(
    accepted: Path,
    cohort: _RouteCollectionResult,
    market_id: str,
) -> None:
    """Give hand-built attachment tests the same persisted trust boundary."""
    cohort.setdefault("candidate_source_generation", "fixture-candidate")
    cohort.setdefault("collection_input_generation", "fixture-input")
    leg = next(
        row for row in cohort["legs"] if row["market_id"] == market_id
    )
    leg.setdefault("leg_id", market_id)
    leg.setdefault(
        "candidate_source_generation",
        cohort["candidate_source_generation"],
    )
    raw = (accepted / "response.json").read_bytes()
    authority = _attachment_authority_bytes(
        market_id=market_id,
        trusted_leg=leg,
        collector_row=leg,
        accepted_raw_sha256=hashlib.sha256(raw).hexdigest(),
        collection_input_generation=cohort["collection_input_generation"],
        validated_specs=cohort._typed_source_payloads[market_id],
    )
    (accepted / "attachment-authority.json").write_bytes(authority)


def _child_reports_closed_descriptor(descriptor):
    try:
        os.fstat(descriptor)
    except OSError:
        return True
    return False


def _typed_v2_pool_payload(
    *,
    pool_address="0x" + "3" * 40,
    token0_address="0x" + "1" * 40,
    token1_address="0x" + "2" * 40,
    token0_decimals=18,
    token1_decimals=6,
    observed_at="2024-01-01T00:00:00+00:00",
    raw_response_sha256="d" * 64,
):
    state = V2PoolState(
        chain="eth",
        chain_id=1,
        dex="uniswap_v2",
        pool_address=pool_address,
        token0_address=token0_address,
        token1_address=token1_address,
        token0_decimals=token0_decimals,
        token1_decimals=token1_decimals,
        reserve0_raw=100 * 10**18,
        reserve1_raw=10_000 * 10**6,
        reserve_timestamp_last_raw=1_704_067_200,
        fee_bps=30,
        fee_numerator=9_970,
        fee_denominator=10_000,
        fee_formula=V2_FEE_FORMULA,
        fee_proof_sha256=ROUTE_V2_FEE_PROOF_SHA256,
        block_number=123,
        block_hash="0x" + "a" * 64,
        block_header_sha256="b" * 64,
        observed_at=observed_at,
        raw_response_sha256=raw_response_sha256,
    )
    integer_fields = {
        "chain_id", "token0_decimals", "token1_decimals", "reserve0_raw",
        "reserve1_raw", "reserve_timestamp_last_raw", "fee_bps",
        "fee_numerator", "fee_denominator", "block_number",
    }
    return {
        "schema": "route_v2_pool_state/v1",
        **{
            field: str(getattr(state, field)) if field in integer_fields
            else getattr(state, field)
            for field in (
                "chain", "chain_id", "dex", "pool_address",
                "token0_address", "token1_address", "token0_decimals",
                "token1_decimals", "reserve0_raw", "reserve1_raw",
                "reserve_timestamp_last_raw", "fee_bps", "fee_numerator",
                "fee_denominator", "fee_formula", "fee_proof_sha256",
                "block_number", "block_hash", "block_header_sha256",
                "observed_at", "raw_response_sha256", "state_id",
            )
        },
    }


def _typed_dex_inventory_fixture():
    market_id = (
        "dex:eth:uniswap_v2:"
        "0x3333333333333333333333333333333333333333:AAVE"
    )
    target = "0x" + "1" * 40
    quote = "0x" + "2" * 40
    state_hash = "d" * 64
    source_hash = "e" * 64
    context = {
        "schema": "route_collector_context/v1",
        "snapshot_id": "tvl-1",
        "request_started_at": "2023-12-31T23:59:59+00:00",
        "observed_at": "2024-01-01T00:00:01+00:00",
        "response_received_at": "2024-01-01T00:00:01+00:00",
        "status": "observed",
        "reason_code": "observed",
        "pool_name": "AAVE / USDC",
        "base_token_id": "eth_" + target,
        "quote_token_id": "eth_" + quote,
        "base_token_price_usd": "100",
        "quote_token_price_usd": "1",
        "tvl_method": "geckoterminal_reserve_in_usd",
        "source": "GeckoTerminal API v2",
        "source_endpoint": "https://api.example.test/pools",
        "raw_response_sha256": source_hash,
    }
    leg = {
        "market_id": market_id,
        "market_type": "dex",
        "token_symbol": "AAVE",
        "target_token_address": target,
        "target_token_side": "base",
        "collector_context": context,
    }
    row = {
        "market_id": market_id,
        "market_type": "dex",
        "status": "observed",
        "token_symbol": "AAVE",
        "chain": "eth",
        "dex": "uniswap_v2",
        "pool_address": "0x" + "3" * 40,
        "block_timestamp": "2024-01-01T00:00:00+00:00",
        "target_token_position": "token0",
        "target_token_address": target,
        "token0_address": target,
        "token0_symbol": "AAVE",
        "token0_decimals": "18",
        "token0_price_usd": "100",
        "token1_address": quote,
        "token1_symbol": "USDC",
        "token1_decimals": "6",
        "token1_price_usd": "1",
        "usd_price_source_snapshot_id": "tvl-1",
        "usd_price_observed_at": "2024-01-01T00:00:01+00:00",
        "usd_price_source": "GeckoTerminal API v2",
        "usd_price_source_endpoint": "https://api.example.test/pools",
        "usd_price_raw_response_sha256": source_hash,
        "raw_response_sha256": state_hash,
    }
    rules = {
        "schema": "route_dex_market_rules_source/v1",
        "market_id": market_id,
        "base_asset": "AAVE",
        "quote_asset": "USDC",
        "base_token_address": target,
        "quote_token_address": quote,
        "base_unit_decimals": 18,
        "quote_unit_decimals": 6,
        "base_increment": "0.000000000000000001",
        "quote_increment": "0.000001",
        "min_base_quantity": "0",
        "min_quote_notional": "0",
        "increment_source": "fixed_block_token_decimals",
        "minimum_source": "dex_protocol_no_additional_order_minimum",
        "observed_at": "2024-01-01T00:00:00+00:00",
        "valid_until": "2024-01-01T00:02:00+00:00",
        "raw_response_sha256": state_hash,
    }
    conversion = {
        "schema": "route_dex_usd_conversion_source/v1",
        "market_id": market_id,
        "target_asset": "AAVE",
        "target_token_address": target,
        "quote_asset": "USDC",
        "quote_token_address": quote,
        "usd_per_quote": "1",
        "value_status": "measured",
        "observed_at": "2024-01-01T00:00:01+00:00",
        "valid_until": "2024-01-01T02:00:01+00:00",
        "state_observed_at": "2024-01-01T00:00:00+00:00",
        "source": "GeckoTerminal API v2",
        "source_snapshot_id": "tvl-1",
        "source_raw_response_sha256": source_hash,
        "state_raw_response_sha256": state_hash,
    }
    return leg, row, _typed_v2_pool_payload(), rules, conversion


def _typed_payload_values(pool, rules, conversion):
    return [
        {
            "role": role,
            "payload": json.dumps(
                payload, sort_keys=True, separators=(",", ":")
            ).encode("utf-8"),
        }
        for role, payload in (
            ("dex_pool_state", pool),
            ("dex_market_rules", rules),
            ("dex_usd_conversion", conversion),
        )
    ]


def _validate_cex_typed_payloads(market_id, values):
    from scripts.collect_route_cohort import _validated_typed_payload_inventory

    raw_sha256 = "a" * 64
    return _validated_typed_payload_inventory(
        trusted_leg={"market_id": market_id, "market_type": "cex"},
        collector_row={
            "status": "observed",
            "raw_response_sha256": raw_sha256,
        },
        accepted_raw_sha256=raw_sha256,
        values=values,
    )


class ForkProcessCloseFdContractTests(unittest.TestCase):
    def test_child_closes_inherited_collection_lock_descriptor_before_callable(self):
        descriptor = os.open(os.devnull, os.O_RDONLY)
        executor = None
        try:
            executor = _ForkProcessExecutor(
                max_workers=1, child_close_fds=(descriptor,)
            )
            future = executor.submit(
                _child_reports_closed_descriptor, descriptor
            )
            done = executor.wait_for_any([future], timeout=5)
            self.assertEqual(done, {future})
            self.assertTrue(future.result())
        finally:
            if executor is not None:
                executor.shutdown(wait=False)
            os.close(descriptor)

    def test_child_close_fd_inventory_is_exact_and_process_counts_are_observed(self):
        for invalid in ((-1,), (True,), (3, 3), ("3",)):
            with self.subTest(invalid=invalid):
                with self.assertRaisesRegex(ValueError, "child_close_fds"):
                    _ForkProcessExecutor(
                        max_workers=1, child_close_fds=invalid
                    )

        executor = _ForkProcessExecutor(max_workers=1, child_close_fds=())
        try:
            future = executor.submit(lambda: "ok")
            executor.wait_for_any([future], timeout=5)
            self.assertEqual(future.result(), "ok")
        finally:
            executor.shutdown(wait=False)
        self.assertEqual(
            executor.process_evidence(),
            {
                "collector_process_started_count": 1,
                "collector_process_reaped_count": 1,
                "orphan_process_count": 0,
            },
        )


class TypedSourceProducerTests(unittest.TestCase):
    def test_attachment_evidence_close_attempts_every_descriptor(self):
        market_id = "cex:okx:UNI/USDT"
        raw = b'{"book":1}'
        cohort = _RouteCollectionResult({
            "raw_evidence_run_id": "close-run",
            "legs": [{
                "market_id": market_id,
                "market_type": "cex",
                "status": "observed",
                "raw_response_sha256": hashlib.sha256(raw).hexdigest(),
            }],
        }, {market_id: ()})

        with tempfile.TemporaryDirectory() as temporary:
            raw_run_root = Path(temporary).resolve() / "close-run"
            accepted = (
                raw_run_root / "accepted"
                / hashlib.sha256(market_id.encode("utf-8")).hexdigest()
            )
            accepted.mkdir(parents=True)
            (accepted / "response.json").write_bytes(raw)
            _install_attachment_authority_fixture(
                accepted, cohort, market_id
            )
            evidence = _accepted_evidence_for_attachment(
                raw_run_root, market_id
            )
            descriptors = (
                evidence.authority_descriptor,
                evidence.response_descriptor,
                evidence.entry_descriptor,
                evidence.accepted_descriptor,
            )
            failed_descriptor = descriptors[0]
            calls = []
            actual_close = os.close

            def fail_first_close(descriptor):
                calls.append(descriptor)
                if descriptor == failed_descriptor:
                    raise OSError("injected close failure")
                actual_close(descriptor)

            try:
                with patch(
                    "scripts.collect_route_cohort.os.close",
                    side_effect=fail_first_close,
                ), self.assertRaisesRegex(OSError, "injected close"):
                    evidence.close()
                self.assertEqual(tuple(calls), descriptors)
                self.assertFalse(evidence.closed)
                evidence.close()
                self.assertTrue(evidence.closed)
            finally:
                for descriptor in descriptors:
                    try:
                        actual_close(descriptor)
                    except OSError:
                        pass

    def test_attachment_authority_never_retains_endpoint_credentials(self):
        market_id = "cex:okx:UNI/USDT"
        raw_sha = "d" * 64
        endpoint = "https://example.test/v2/APIKEY-very-secret"
        leg = {
            "market_id": market_id,
            "market_type": "cex",
            "source_endpoint": endpoint,
        }
        row = {
            "market_id": market_id,
            "market_type": "cex",
            "status": "observed",
            "raw_response_sha256": raw_sha,
            "source_endpoint": endpoint,
        }

        encoded = _attachment_authority_bytes(
            market_id=market_id,
            trusted_leg=leg,
            collector_row=row,
            accepted_raw_sha256=raw_sha,
            collection_input_generation="input-a",
            validated_specs=(),
        )
        authority = json.loads(encoded)

        self.assertNotIn(b"secret", encoded)
        self.assertEqual(
            authority["collector_row"]["source_endpoint"],
            "https://example.test",
        )

    def test_shadow_collector_context_reduces_endpoint_path_to_origin(self):
        from tests.test_route_shadow_inputs import ProductionInputFixture

        with tempfile.TemporaryDirectory() as temporary:
            fixture = ProductionInputFixture(temporary)
            path = fixture.data_dir / "dex_pool_tvl_latest.csv"
            with path.open(newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                fields = list(reader.fieldnames or [])
                rows = list(reader)
            rows[0]["source_endpoint"] = (
                "https://api.example.test/v2/APIKEY-very-secret"
            )
            with path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle, fieldnames=fields, lineterminator="\n"
                )
                writer.writeheader()
                writer.writerows(rows)
            universe, _manifest = fixture.build()

        dex_leg = next(
            leg for leg in universe["selected_legs"]
            if leg["market_type"] == "dex"
        )
        self.assertEqual(
            dex_leg["collector_context"]["source_endpoint"],
            "https://api.example.test",
        )
        self.assertNotIn(
            "APIKEY-very-secret", json.dumps(universe, sort_keys=True)
        )

    def test_attach_rejects_in_place_response_rewrite_after_authority_read(self):
        market_id = "cex:okx:UNI/USDT"
        cohort = _RouteCollectionResult({
            "raw_evidence_run_id": "response-rewrite-run",
            "legs": [{
                "market_id": market_id,
                "market_type": "cex",
                "status": "observed",
                "raw_response_sha256": hashlib.sha256(
                    b'{"book":1}'
                ).hexdigest(),
            }],
        }, {market_id: ()})

        with tempfile.TemporaryDirectory() as temporary:
            raw_root = Path(temporary)
            accepted = (
                raw_root / "response-rewrite-run" / "accepted"
                / hashlib.sha256(market_id.encode("utf-8")).hexdigest()
            )
            accepted.mkdir(parents=True)
            response_path = accepted / "response.json"
            response_path.write_bytes(b'{"book":1}')
            _install_attachment_authority_fixture(
                accepted, cohort, market_id
            )
            response_inode = response_path.stat().st_ino

            def rewrite_after_authority_read(payload, **kwargs):
                validated = _validated_attachment_authority(
                    payload, **kwargs
                )
                response_path.write_bytes(b'{"book":2}')
                self.assertEqual(response_path.stat().st_ino, response_inode)
                return validated

            with patch(
                "scripts.collect_route_cohort._validated_attachment_authority",
                side_effect=rewrite_after_authority_read,
            ), self.assertRaisesRegex(
                ValueError, "accepted|source|changed|invalid"
            ):
                attach_typed_source_lineage(cohort, raw_root=raw_root)

            self.assertFalse((raw_root / "response-rewrite-run/typed").exists())
            self.assertFalse(
                (raw_root / "response-rewrite-run/typed-manifest.json").exists()
            )

    def test_cex_producer_flows_through_fork_capability_and_attach(self):
        left = "cex:binance:UNI/USDT"
        right = "cex:bybit:UNI/USDT"
        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [
                {
                    "market_id": left,
                    "market_type": "cex",
                    "exchange": "binance",
                    "cex_symbol": "UNI/USDT",
                    "token_symbol": "UNI",
                },
                {
                    "market_id": right,
                    "market_type": "cex",
                    "exchange": "bybit",
                    "cex_symbol": "UNI/USDT",
                    "token_symbol": "UNI",
                },
            ],
            "routes": [_strict_route(
                "UNI", left, right, "prepositioned_inventory"
            )],
        }

        binance_book = b'{"bids":[["100","2"]],"asks":[["100.01","3"]]}'
        bybit_book = (
            b'{"retCode":0,"result":{"s":"UNIUSDT",'
            b'"b":[["100","2"]],"a":[["100.01","3"]]}}'
        )
        binance_rules = json.dumps({
            "symbols": [{
                "symbol": "UNIUSDT",
                "status": "TRADING",
                "baseAsset": "UNI",
                "quoteAsset": "USDT",
                "baseAssetPrecision": 8,
                "quoteAssetPrecision": 4,
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.01"},
                    {
                        "filterType": "LOT_SIZE",
                        "minQty": "0.1",
                        "stepSize": "0.01",
                    },
                    {"filterType": "MIN_NOTIONAL", "minNotional": "5"},
                ],
            }],
        }, sort_keys=True, separators=(",", ":")).encode("utf-8")
        bybit_rules = json.dumps({
            "retCode": 0,
            "result": {
                "category": "spot",
                "list": [{
                    "symbol": "UNIUSDT",
                    "status": "Trading",
                    "baseCoin": "UNI",
                    "quoteCoin": "USDT",
                    "lotSizeFilter": {
                        "basePrecision": "0.001",
                        "quotePrecision": "0.0001",
                        "minOrderQty": "0.01",
                        "minOrderAmt": "1",
                    },
                    "priceFilter": {"tickSize": "0.01"},
                }],
            },
        }, sort_keys=True, separators=(",", ":")).encode("utf-8")

        def cex_source(
            leg,
            *,
            snapshot_id,
            raw_path,
            deadline,
            typed_source_payload_sink,
        ):
            def request(url, *, deadline):
                deadline.require_remaining()
                if "data-api.binance.vision" in url:
                    return json.loads(binance_book), binance_book
                if "api.binance.com" in url:
                    return json.loads(binance_rules), binance_rules
                if "/v5/market/orderbook" in url:
                    return json.loads(bybit_book), bybit_book
                if "/v5/market/instruments-info" in url:
                    return json.loads(bybit_rules), bybit_rules
                raise AssertionError("unexpected CEX request: " + url)

            return collect_cex_market_observation(
                dict(leg),
                snapshot_id=snapshot_id,
                raw_path=raw_path,
                request=request,
                deadline=deadline,
                typed_source_payload_sink=typed_source_payload_sink,
            )

        with tempfile.TemporaryDirectory() as temporary:
            raw_root = Path(temporary) / "raw"
            wall_start = datetime(2026, 8, 1, 12, 0, 0, tzinfo=timezone.utc)
            book_response_time = wall_start + timedelta(seconds=1)
            rules_response_time = wall_start + timedelta(seconds=2)
            cohort_now = wall_start + timedelta(seconds=30)
            wall = iter([
                wall_start,
                cohort_now,
            ])
            source_times = iter([
                wall_start.isoformat(),
                book_response_time.isoformat(),
                rules_response_time.isoformat(),
            ] * 2)
            with patch(
                "scripts.fetch_cex_depth.utc_now_text",
                side_effect=lambda: next(source_times),
            ):
                cohort = _collect_route_cohort(
                    universe,
                    cex_collector=cex_source,
                    dex_collector=lambda *_args, **_kwargs: None,
                    raw_root=raw_root,
                    snapshot_id="typed-cex-run",
                    source_generation_reader=lambda: "input-a",
                    expected_source_generation="input-a",
                    wall_clock=lambda: next(wall),
                )
            normalized, publication = attach_typed_source_lineage(
                cohort, raw_root=raw_root
            )

            typed_root = raw_root / "typed-cex-run" / "typed"
            typed_payloads = {}
            for member in publication["manifest"]["members"]:
                if member["role"] in {
                    "cex_market_rules", "quote_usd_conversion"
                }:
                    typed_payloads[(member["market_id"], member["role"])] = (
                        json.loads(
                            (typed_root / member["filename"])
                            .read_text(encoding="utf-8")
                        )
                    )

        self.assertEqual(
            publication["manifest"]["member_count"],
            6,
            msg=json.dumps(normalized["legs"], sort_keys=True),
        )
        self.assertEqual(
            [(leg["status"], leg["reason_code"]) for leg in normalized["legs"]],
            [
                ("partial", "source_level_limit"),
                ("partial", "source_level_limit"),
            ],
        )
        self.assertTrue(all(
            [member["status"] for member in leg["typed_source_lineage"]["members"]]
            == ["observed", "observed", "observed"]
            for leg in normalized["legs"]
        ))
        self.assertEqual(
            {value["observed_at"] for (market_id, role), value in typed_payloads.items()
             if role == "quote_usd_conversion"},
            {book_response_time.isoformat()},
        )
        self.assertEqual(
            {value["observed_at"] for (market_id, role), value in typed_payloads.items()
             if role == "cex_market_rules"},
            {rules_response_time.isoformat()},
        )
        for value in typed_payloads.values():
            observed = datetime.fromisoformat(value["observed_at"])
            valid_until = datetime.fromisoformat(value["valid_until"])
            self.assertLessEqual(observed, cohort_now)
            self.assertLess(cohort_now, valid_until)

    def test_built_universe_dex_leg_reaches_default_collector_from_context(self):
        from scripts.fetch_dex_depth import (
            SELECTOR_DECIMALS,
            SELECTOR_GET_RESERVES,
            SELECTOR_SYMBOL,
            SELECTOR_TOKEN0,
            SELECTOR_TOKEN1,
            collect_dex_pool_observation,
        )
        from tests.test_route_shadow_inputs import (
            BLOCK_TIME,
            POOL,
            ProductionInputFixture,
        )

        target = "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984"
        quote = "0x" + "2" * 40
        case = self

        def word(value):
            return "{:064x}".format(value % (1 << 256))

        def address_result(address):
            return "0x" + ("0" * 24) + address[2:]

        def uint_result(*values):
            return "0x" + "".join(word(value) for value in values)

        def string_result(value):
            encoded = value.encode("utf-8")
            padded = encoded.hex().ljust(
                ((len(encoded) + 31) // 32) * 64, "0"
            )
            return "0x" + word(32) + word(len(encoded)) + padded

        class FixtureRpc:
            endpoint = "https://rpc.example.test"

            def __init__(self):
                self.records = []

            def eth_calls(self, to, data_values, block_tag):
                self.records.append({
                    "request": {
                        "to": to,
                        "data": data_values,
                        "block": block_tag,
                    },
                    "response": "fixture",
                })
                if data_values == [
                    SELECTOR_TOKEN0,
                    SELECTOR_TOKEN1,
                    SELECTOR_GET_RESERVES,
                ]:
                    return [
                        address_result(target),
                        address_result(quote),
                        uint_result(100 * 10**18, 10_000 * 10**6, 0),
                    ]
                if to == target:
                    case.assertEqual(
                        data_values, [SELECTOR_DECIMALS, SELECTOR_SYMBOL]
                    )
                    return [uint_result(18), string_result("UNI")]
                if to == quote:
                    case.assertEqual(
                        data_values, [SELECTOR_DECIMALS, SELECTOR_SYMBOL]
                    )
                    return [uint_result(6), string_result("USDC")]
                case.fail("unexpected RPC target: {}".format(to))

        with tempfile.TemporaryDirectory() as temporary:
            fixture = ProductionInputFixture(temporary)
            universe, _manifest = fixture.build()
            for leg in universe["selected_legs"]:
                volume_field = (
                    "dex_24h_usd"
                    if leg["market_type"] == "dex"
                    else "cex_selected_window_usd"
                )
                leg["selection_inputs"][volume_field] = "1"
            for route in universe["routes"]:
                route["buy_reference_volume_usd"] = "1"
                route["sell_reference_volume_usd"] = "1"
                route["route_volume_usd"] = "1"
            raw_root = Path(temporary) / "route-raw"
            captured = {}
            rpc = FixtureRpc()

            def default_dex(
                leg,
                *,
                typed_source_payload_sink,
                **kwargs
            ):
                captured.update(leg)
                return collect_dex_pool_observation(
                    leg,
                    client=rpc,
                    typed_source_payload_sink=typed_source_payload_sink,
                    **kwargs
                )

            def collect_cex(leg, *, raw_path, **_kwargs):
                payload = b'{"fixture":"cex"}'
                raw_path.write_bytes(payload)
                return {
                    "market_id": leg["market_id"],
                    "status": "observed",
                    "observed_at": "2026-08-02T12:00:00+00:00",
                    "state_observed_at": "2026-08-02T12:00:00+00:00",
                    "exchange": "binance",
                    "cex_symbol": "UNI/USDT",
                    "token_symbol": "UNI",
                    "raw_response_sha256": hashlib.sha256(payload).hexdigest(),
                }

            block_epoch = int(datetime.fromisoformat(BLOCK_TIME).timestamp())
            header = {
                "number": "0x7b",
                "hash": "0x" + "a" * 64,
                "parent_hash": "0x" + "b" * 64,
                "timestamp": hex(block_epoch),
                "base_fee_per_gas": "0x3b9aca00",
                "gas_used": "0xe4e1c0",
                "gas_limit": "0x1c9c380",
            }
            wall = iter([
                datetime(2026, 8, 2, 12, 0, 1, tzinfo=timezone.utc),
                datetime(2026, 8, 2, 12, 0, 2, tzinfo=timezone.utc),
            ])
            result = _collect_route_cohort(
                universe,
                cex_collector=collect_cex,
                dex_collector=default_dex,
                dex_block_resolver=lambda *_args, **_kwargs: {
                    "block_number": 123,
                    "block_timestamp": BLOCK_TIME,
                    "chain_id": "0x1",
                    "block_header": header,
                },
                raw_root=raw_root,
                snapshot_id="built-universe-default-dex",
                executor_factory=ThreadPoolExecutor,
                source_generation_reader=lambda: universe[
                    "candidate_source_generation"
                ],
                expected_source_generation=universe[
                    "candidate_source_generation"
                ],
                wall_clock=lambda: next(wall),
            )

            dex_id = "dex:eth:uniswap_v2:{}:UNI".format(POOL)
            original_legs = json.loads(json.dumps(result["legs"]))
            original_payloads = result._typed_source_payloads
            attacked_legs = []
            for leg in result["legs"]:
                if leg["market_id"] != dex_id:
                    attacked_legs.append(leg)
                    continue
                context = {
                    **leg["collector_context"],
                    "pool_name": "forged pool",
                    "quote_token_price_usd": "999",
                    "raw_response_sha256": "f" * 64,
                }
                attacked_legs.append({
                    **leg,
                    "collector_context": context,
                    "pool_name": "forged pool",
                    "token1_price_usd": "999",
                    "usd_price_raw_response_sha256": "f" * 64,
                })
            attacked_members = []
            for member in original_payloads[dex_id]:
                if member["role"] != "dex_usd_conversion":
                    attacked_members.append(member)
                    continue
                conversion = {
                    **json.loads(member["payload"]),
                    "usd_per_quote": "999",
                    "source_raw_response_sha256": "f" * 64,
                }
                payload = json.dumps(
                    conversion, ensure_ascii=False, sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
                attacked_members.append({
                    **member,
                    "payload": payload,
                    "logical_generation": hashlib.sha256(payload).hexdigest(),
                })
            self.assertIn(
                "dex_usd_conversion",
                {member["role"] for member in attacked_members},
            )
            result["legs"] = attacked_legs
            result._typed_source_payloads = {
                **original_payloads,
                dex_id: tuple(attacked_members),
            }
            with self.assertRaisesRegex(
                ValueError, "typed-source.*invalid|authority"
            ):
                attach_typed_source_lineage(result, raw_root=raw_root)
            result["legs"] = original_legs
            result._typed_source_payloads = original_payloads

        dex_row = next(
            row for row in result["legs"] if row["market_id"] == dex_id
        )
        self.assertEqual(
            dex_row["status"], "observed", msg=json.dumps(dex_row, sort_keys=True)
        )
        self.assertEqual(captured["token_symbol"], "UNI")
        self.assertEqual(captured["chain"], "eth")
        self.assertEqual(captured["dex"], "uniswap_v2")
        self.assertEqual(captured["pool_address"], POOL)
        self.assertEqual(captured["base_token_id"], "eth_" + target)
        self.assertEqual(captured["quote_token_id"], "eth_" + quote)
        self.assertEqual(captured["base_token_price_usd"], "100")
        self.assertEqual(captured["quote_token_price_usd"], "1")

    def test_built_universe_rejects_zero_dex_collector_token_address(self):
        from tests.test_route_shadow_inputs import ProductionInputFixture

        with tempfile.TemporaryDirectory() as temporary:
            fixture = ProductionInputFixture(temporary)
            path = fixture.data_dir / "dex_pool_tvl_latest.csv"
            with path.open(newline="", encoding="utf-8") as handle:
                reader = csv.DictReader(handle)
                fieldnames = reader.fieldnames
                rows = list(reader)
            rows[0]["quote_token_id"] = "eth_0x" + "0" * 40
            with path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle, fieldnames=fieldnames, lineterminator="\n"
                )
                writer.writeheader()
                writer.writerows(rows)

            with self.assertRaisesRegex(ValueError, "zero address"):
                fixture.build()

    def test_missing_and_stale_usd_context_retain_fixed_block_rules_only(self):
        from scripts.fetch_dex_depth import collect_dex_pool_observation
        from tests.test_fetch_dex_depth import FakeV2Rpc

        target = "0x" + "1" * 40
        quote = "0x" + "2" * 40
        left = "dex:eth:uniswap_v2:0x" + "3" * 40 + ":AAVE"
        right = "dex:eth:uniswap_v2:0x" + "4" * 40 + ":AAVE"

        def missing_context(pool_name):
            return {
                "schema": "route_collector_context/v1",
                "snapshot_id": "tvl-missing",
                "request_started_at": "2023-12-31T23:59:58+00:00",
                "observed_at": "2023-12-31T23:59:59+00:00",
                "response_received_at": "2024-01-01T00:00:00+00:00",
                "status": "missing",
                "reason_code": "source_no_tvl_observation",
                "pool_name": pool_name,
                "base_token_id": None,
                "quote_token_id": None,
                "base_token_price_usd": None,
                "quote_token_price_usd": None,
                "tvl_method": "geckoterminal_reserve_in_usd",
                "source": "GeckoTerminal API v2",
                "source_endpoint": "https://api.example.test/pools",
                "raw_response_sha256": "e" * 64,
            }

        def stale_context(pool_name):
            return {
                **missing_context(pool_name),
                "request_started_at": "2023-12-31T20:59:58+00:00",
                "observed_at": "2023-12-31T20:59:59+00:00",
                "response_received_at": "2023-12-31T21:00:00+00:00",
                "status": "observed",
                "reason_code": "observed",
                "base_token_id": "eth_" + target,
                "quote_token_id": "eth_" + quote,
                "base_token_price_usd": "100",
                "quote_token_price_usd": "1",
            }

        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [
                {
                    "market_id": market_id,
                    "market_type": "dex",
                    "token_symbol": "AAVE",
                    "target_token_address": target,
                    "target_token_side": (
                        None if market_id == left else "base"
                    ),
                    "collector_context": (
                        missing_context("AAVE / unknown")
                        if market_id == left
                        else stale_context("AAVE / USDC")
                    ),
                }
                for market_id in (left, right)
            ],
            "routes": [_strict_route(
                "AAVE", left, right, "atomic_onchain"
            )],
        }
        header = {
            "number": "0x7b",
            "hash": "0x" + "a" * 64,
            "parent_hash": "0x" + "b" * 64,
            "timestamp": "0x65920080",
            "base_fee_per_gas": "0x3b9aca00",
            "gas_used": "0xe4e1c0",
            "gas_limit": "0x1c9c380",
        }

        def default_dex(
            leg,
            *,
            typed_source_payload_sink,
            allow_degraded_usd_context=False,
            **kwargs
        ):
            self.assertTrue(allow_degraded_usd_context)
            return collect_dex_pool_observation(
                leg,
                rpc_factory=lambda chain, url, **_kwargs: FakeV2Rpc(
                    chain, url
                ),
                typed_source_payload_sink=typed_source_payload_sink,
                allow_degraded_usd_context=allow_degraded_usd_context,
                **kwargs,
            )

        with tempfile.TemporaryDirectory() as temporary:
            raw_root = Path(temporary) / "raw"
            wall = iter([
                datetime(2024, 1, 1, 0, 0, 1, tzinfo=timezone.utc),
                datetime(2024, 1, 1, 0, 0, 2, tzinfo=timezone.utc),
            ])
            cohort = _collect_route_cohort(
                universe,
                cex_collector=lambda *_args, **_kwargs: self.fail(
                    "unexpected CEX"
                ),
                dex_collector=default_dex,
                dex_block_resolver=lambda *_args, **_kwargs: {
                    "block_number": 123,
                    "block_timestamp": "2024-01-01T00:00:00+00:00",
                    "chain_id": "0x1",
                    "block_header": header,
                },
                raw_root=raw_root,
                snapshot_id="missing-usd-context",
                executor_factory=ThreadPoolExecutor,
                source_generation_reader=lambda: "generation-a",
                expected_source_generation="generation-a",
                wall_clock=lambda: next(wall),
            )

            original_legs = [dict(leg) for leg in cohort["legs"]]
            original_payloads = cohort._typed_source_payloads
            attacked_market = left
            attacked_members = []
            for member in original_payloads[attacked_market]:
                payload = json.loads(member["payload"])
                if member["role"] == "dex_pool_state":
                    payload["token1_decimals"] = "8"
                    state = freeze_v2_pool_state({
                        **payload,
                        **{
                            field: int(payload[field])
                            for field in (
                                "chain_id", "token0_decimals",
                                "token1_decimals", "reserve0_raw",
                                "reserve1_raw", "reserve_timestamp_last_raw",
                                "fee_bps", "fee_numerator",
                                "fee_denominator", "block_number",
                            )
                        },
                    })
                    payload["state_id"] = state.state_id
                elif member["role"] == "dex_market_rules":
                    payload["quote_unit_decimals"] = 8
                    payload["quote_increment"] = "0.00000001"
                encoded = json.dumps(
                    payload, ensure_ascii=False, sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
                attacked_members.append({
                    **member,
                    "payload": encoded,
                    "logical_generation": (
                        payload["state_id"].split(":", 1)[1]
                        if member["role"] == "dex_pool_state"
                        else hashlib.sha256(encoded).hexdigest()
                    ),
                })
            cohort["legs"] = [
                {
                    **leg,
                    **(
                        {"token1_decimals": "8"}
                        if leg["market_id"] == attacked_market
                        else {}
                    ),
                }
                for leg in cohort["legs"]
            ]
            cohort._typed_source_payloads = {
                **original_payloads,
                attacked_market: tuple(attacked_members),
            }
            with self.assertRaisesRegex(
                ValueError, "typed-source.*invalid|authority"
            ):
                attach_typed_source_lineage(cohort, raw_root=raw_root)

            cohort["legs"] = original_legs
            cohort._typed_source_payloads = original_payloads
            normalized, publication = attach_typed_source_lineage(
                cohort, raw_root=raw_root
            )

        self.assertTrue(all(
            leg["status"] == "partial" and leg["available"] is False
            for leg in cohort["legs"]
        ), msg=cohort["legs"])
        self.assertTrue(all(
            leg["token0_price_usd"] is None
            and leg["token1_price_usd"] is None
            and leg["total_depth_100bps_usd"] == ""
            for leg in cohort["legs"]
        ))
        self.assertEqual(
            cohort["route_rows"][0]["timing_status"], "unavailable"
        )
        self.assertEqual(
            cohort["route_rows"][0]["reason_code"], "buy_leg_unavailable"
        )
        self.assertEqual(publication["manifest"]["member_count"], 6)
        for leg in normalized["legs"]:
            by_role = {
                member["role"]: member
                for member in leg["typed_source_lineage"]["members"]
            }
            self.assertEqual(by_role["dex_pool_state"]["status"], "observed")
            self.assertEqual(by_role["dex_market_rules"]["status"], "observed")
            self.assertEqual(by_role["dex_usd_conversion"]["status"], "unavailable")

    def test_pipeline_installs_typed_inventory_and_attaches_exact_core_lineage(self):
        with tempfile.TemporaryDirectory() as temporary:
            raw_root = Path(temporary) / "raw"
            run_root = raw_root / "run-1"
            market_id = "cex:binance:UNI/USDT"
            accepted = (
                run_root / "accepted"
                / hashlib.sha256(market_id.encode("utf-8")).hexdigest()
            )
            accepted.mkdir(parents=True)
            raw = b'{"book":1}'
            (accepted / "response.json").write_bytes(raw)
            rules_payload = json.dumps({
                "schema": "route_market_rules_source/v1",
                "market_id": market_id,
                "base_asset": "UNI",
                "quote_asset": "USDT",
                "base_unit_decimals": 8,
                "quote_unit_decimals": 8,
                "base_increment": "0.01",
                "quote_increment": "0.01",
                "min_base_quantity": "0.1",
                "min_quote_notional": "5",
                "observed_at": "2026-08-02T12:00:00Z",
                "valid_until": "2026-08-02T12:01:00Z",
            }, sort_keys=True, separators=(",", ":")).encode("utf-8")
            conversion_payload = json.dumps({
                "schema": "route_usd_conversion_source/v1",
                "quote_asset": "USDT",
                "usd_per_quote": "1",
                "observed_at": "2026-08-02T12:00:00Z",
                "valid_until": "2026-08-02T12:01:00Z",
                "source": "USDT=USD proxy",
            }, sort_keys=True, separators=(",", ":")).encode("utf-8")
            cohort = _RouteCollectionResult({
                "raw_evidence_run_id": "run-1",
                "legs": [{
                    "market_id": market_id,
                    "market_type": "cex",
                    "status": "observed",
                    "available": True,
                    "state_observed_at": "2026-08-02T12:00:00Z",
                    "raw_response_sha256": hashlib.sha256(raw).hexdigest(),
                    "source_quote_asset": "USDT",
                    "quote_to_usd": "1",
                    "quote_conversion_method": "USDT=USD proxy",
                }],
            }, {market_id: ({
                "market_id": market_id,
                "role": "cex_market_rules",
                "payload": rules_payload,
                "logical_generation": hashlib.sha256(rules_payload).hexdigest(),
                "adapter_id": "route_quantity_quote_for_book/v1",
                "content_schema": "route_market_rules_source/v1",
            }, {
                "market_id": market_id,
                "role": "quote_usd_conversion",
                "payload": conversion_payload,
                "logical_generation": hashlib.sha256(conversion_payload).hexdigest(),
                "adapter_id": "route_usd_conversion_source/v1",
                "content_schema": "route_usd_conversion_source/v1",
            })})
            _install_attachment_authority_fixture(
                accepted, cohort, market_id
            )
            normalized, publication = attach_typed_source_lineage(
                cohort, raw_root=raw_root
            )
            lineage = normalized["legs"][0]["typed_source_lineage"]
            observed = [
                row for row in lineage["members"]
                if row["status"] == "observed"
            ]
            self.assertEqual(
                [row["role"] for row in observed],
                [
                    "cex_market_rules",
                    "cex_raw_book_response",
                    "quote_usd_conversion",
                ],
            )
            self.assertEqual(
                publication["manifest"]["member_count"], 3
            )
            self.assertEqual(
                (run_root / "typed-manifest.json").is_file(), True
            )

    def test_attach_never_infers_conversion_from_public_leg_columns(self):
        with tempfile.TemporaryDirectory() as temporary:
            raw_root = Path(temporary) / "raw"
            run_root = raw_root / "run-inference"
            market_id = "cex:okx:UNI/USDT"
            accepted = (
                run_root / "accepted"
                / hashlib.sha256(market_id.encode("utf-8")).hexdigest()
            )
            accepted.mkdir(parents=True)
            raw = b'{"book":1}'
            (accepted / "response.json").write_bytes(raw)
            cohort = _RouteCollectionResult({
                "raw_evidence_run_id": "run-inference",
                "legs": [{
                    "market_id": market_id,
                    "market_type": "cex",
                    "status": "observed",
                    "available": True,
                    "state_observed_at": "2026-08-02T12:00:00Z",
                    "raw_response_sha256": hashlib.sha256(raw).hexdigest(),
                    "source_quote_asset": "USDT",
                    "quote_to_usd": "999",
                    "quote_conversion_method": "caller supplied",
                }],
            }, {market_id: ()})
            _install_attachment_authority_fixture(
                accepted, cohort, market_id
            )

            normalized, publication = attach_typed_source_lineage(
                cohort, raw_root=raw_root
            )

        lineage = normalized["legs"][0]["typed_source_lineage"]
        by_role = {member["role"]: member for member in lineage["members"]}
        self.assertEqual(publication["manifest"]["member_count"], 1)
        self.assertEqual(by_role["cex_raw_book_response"]["status"], "observed")
        self.assertEqual(by_role["cex_market_rules"]["status"], "unavailable")
        self.assertEqual(
            by_role["cex_market_rules"]["reason_code"],
            "typed_source_adapter_unsupported",
        )
        self.assertEqual(by_role["quote_usd_conversion"]["status"], "unavailable")
        self.assertEqual(
            by_role["quote_usd_conversion"]["reason_code"],
            "typed_source_adapter_unsupported",
        )

    def test_terminal_unsupported_cex_still_declares_adapter_unsupported(self):
        market_id = "cex:okx:UNI/USDT"
        cohort = _RouteCollectionResult({
            "raw_evidence_run_id": "run-terminal-unsupported",
            "legs": [{
                "market_id": market_id,
                "market_type": "cex",
                "status": "failed",
                "available": False,
                "reason_code": "collection_failed",
            }],
        }, {})
        with tempfile.TemporaryDirectory() as temporary:
            normalized, publication = attach_typed_source_lineage(
                cohort, raw_root=Path(temporary)
            )

        self.assertEqual(publication["manifest"]["member_count"], 0)
        by_role = {
            member["role"]: member
            for member in normalized["legs"][0]["typed_source_lineage"]["members"]
        }
        self.assertEqual(
            by_role["cex_market_rules"]["reason_code"],
            "typed_source_adapter_unsupported",
        )
        self.assertEqual(
            by_role["quote_usd_conversion"]["reason_code"],
            "typed_source_adapter_unsupported",
        )

    def test_unavailable_dex_leg_never_publishes_mutable_collector_context(self):
        market_id = (
            "dex:eth:uniswap_v2:"
            "0x3333333333333333333333333333333333333333:AAVE"
        )
        cohort = _RouteCollectionResult({
            "raw_evidence_run_id": "run-terminal-dex-context",
            "legs": [{
                "market_id": market_id,
                "market_type": "dex",
                "status": "failed",
                "available": False,
                "reason_code": "collection_failed",
                "collector_context": {
                    "schema": "route_collector_context/v1",
                    "snapshot_id": "forged",
                    "request_started_at": "2024-01-01T00:00:00Z",
                    "observed_at": "2024-01-01T00:00:00Z",
                    "response_received_at": "2024-01-01T00:00:00Z",
                    "status": "observed",
                    "reason_code": "observed",
                    "pool_name": "forged pool",
                    "base_token_id": "eth_0x" + "1" * 40,
                    "quote_token_id": "eth_0x" + "2" * 40,
                    "base_token_price_usd": "999",
                    "quote_token_price_usd": "999",
                    "tvl_method": "forged",
                    "source": "forged",
                    "source_endpoint": "https://example.test/forged",
                    "raw_response_sha256": "f" * 64,
                },
            }],
        }, {})

        with tempfile.TemporaryDirectory() as temporary:
            normalized, publication = attach_typed_source_lineage(
                cohort, raw_root=Path(temporary)
            )

        self.assertEqual(publication["manifest"]["member_count"], 0)
        self.assertFalse(any(
            member["status"] == "observed"
            for member in normalized["legs"][0]["typed_source_lineage"]["members"]
        ))

    def test_supported_cex_missing_one_typed_member_marks_only_that_role_failed(self):
        with tempfile.TemporaryDirectory() as temporary:
            raw_root = Path(temporary) / "raw"
            run_root = raw_root / "run-missing"
            market_id = "cex:binance:UNI/USDT"
            accepted = (
                run_root / "accepted"
                / hashlib.sha256(market_id.encode("utf-8")).hexdigest()
            )
            accepted.mkdir(parents=True)
            raw = b'{"book":1}'
            (accepted / "response.json").write_bytes(raw)
            rules_payload = json.dumps({
                "schema": "route_market_rules_source/v1",
                "market_id": market_id,
                "base_asset": "UNI",
                "quote_asset": "USDT",
                "base_unit_decimals": 8,
                "quote_unit_decimals": 8,
                "base_increment": "0.01",
                "quote_increment": "0.01",
                "min_base_quantity": "0.1",
                "min_quote_notional": "5",
                "observed_at": "2026-08-02T12:00:00Z",
                "valid_until": "2026-08-02T12:01:00Z",
            }, sort_keys=True, separators=(",", ":")).encode("utf-8")
            cohort = _RouteCollectionResult({
                "raw_evidence_run_id": "run-missing",
                "legs": [{
                    "market_id": market_id,
                    "market_type": "cex",
                    "status": "observed",
                    "available": True,
                    "state_observed_at": "2026-08-02T12:00:00Z",
                    "raw_response_sha256": hashlib.sha256(raw).hexdigest(),
                }],
            }, {market_id: ({
                "market_id": market_id,
                "role": "cex_market_rules",
                "payload": rules_payload,
                "logical_generation": hashlib.sha256(rules_payload).hexdigest(),
                "adapter_id": "route_quantity_quote_for_book/v1",
                "content_schema": "route_market_rules_source/v1",
            },)})
            _install_attachment_authority_fixture(
                accepted, cohort, market_id
            )

            normalized, publication = attach_typed_source_lineage(
                cohort, raw_root=raw_root
            )

        by_role = {
            member["role"]: member
            for member in normalized["legs"][0]["typed_source_lineage"]["members"]
        }
        self.assertEqual(publication["manifest"]["member_count"], 2)
        self.assertEqual(by_role["cex_market_rules"]["status"], "observed")
        self.assertEqual(by_role["quote_usd_conversion"]["status"], "unavailable")
        self.assertEqual(
            by_role["quote_usd_conversion"]["reason_code"],
            "typed_source_failed",
        )

    def test_malformed_cex_typed_payload_is_rejected_before_raw_promotion(self):
        universe = _strict_cex_universe()

        def malformed_cex(leg, *, raw_path, typed_source_payload_sink, **_kwargs):
            raw = ("raw:" + leg["market_id"]).encode("utf-8")
            raw_path.write_bytes(raw)
            typed_source_payload_sink({
                "role": "cex_market_rules",
                "payload": b'{"schema":"wrong"}',
            })
            return {
                "status": "observed",
                "state_observed_at": "2026-08-01T12:00:00Z",
                "raw_response_sha256": hashlib.sha256(raw).hexdigest(),
            }

        with tempfile.TemporaryDirectory() as temporary:
            raw_root = Path(temporary)
            cohort = _collect_route_cohort(
                universe,
                cex_collector=malformed_cex,
                dex_collector=lambda *_args, **_kwargs: None,
                raw_root=raw_root,
                snapshot_id="malformed-cex-typed",
                executor_factory=ThreadPoolExecutor,
                source_generation_reader=lambda: "input-a",
                expected_source_generation="input-a",
            )

        self.assertEqual(cohort._typed_source_payloads, {})
        self.assertTrue(all(
            leg["status"] == "failed"
            and leg["reason_code"] == "collection_failed"
            for leg in cohort["legs"]
        ))

    def test_cex_typed_payloads_require_exact_positive_windowed_values(self):
        market_id = "cex:binance:UNI/USDT"
        rules = {
            "schema": "route_market_rules_source/v1",
            "market_id": market_id,
            "base_asset": "UNI",
            "quote_asset": "USDT",
            "base_unit_decimals": 8,
            "quote_unit_decimals": 4,
            "base_increment": "0.01",
            "quote_increment": "0.0001",
            "min_base_quantity": "0.1",
            "min_quote_notional": "5",
            "observed_at": "2026-08-01T12:00:00Z",
            "valid_until": "2026-08-01T12:01:00Z",
        }
        conversion = {
            "schema": "route_usd_conversion_source/v1",
            "quote_asset": "USDT",
            "usd_per_quote": "1",
            "observed_at": "2026-08-01T12:00:00Z",
            "valid_until": "2026-08-01T12:01:00Z",
            "source": "USDT=USD proxy",
        }
        mutations = (
            ("rules-extra", "cex_market_rules", {**rules, "extra": True}),
            ("rules-zero", "cex_market_rules", {**rules, "base_increment": "0"}),
            ("rules-noncanonical", "cex_market_rules", {**rules, "quote_increment": "0.00010"}),
            ("rules-unit-misaligned", "cex_market_rules", {
                **rules,
                "base_unit_decimals": 2,
                "base_increment": "0.001",
            }),
            ("rules-signed-zero-base-minimum", "cex_market_rules", {
                **rules,
                "min_base_quantity": "-0",
            }),
            ("rules-signed-zero-quote-minimum", "cex_market_rules", {
                **rules,
                "min_quote_notional": "-0",
            }),
            ("conversion-wrong-quote", "quote_usd_conversion", {**conversion, "quote_asset": "USD"}),
            ("conversion-zero-window", "quote_usd_conversion", {**conversion, "valid_until": conversion["observed_at"]}),
        )
        for name, role, value in mutations:
            with self.subTest(name=name), self.assertRaisesRegex(
                ValueError, "typed-source worker inventory"
            ):
                _validate_cex_typed_payloads(
                    market_id,
                    [{
                        "role": role,
                        "payload": json.dumps(
                            value, sort_keys=True, separators=(",", ":")
                        ).encode("utf-8"),
                    }],
                )

        zero_minima = {
            **rules,
            "min_base_quantity": "0",
            "min_quote_notional": "0",
        }
        inventory = _validate_cex_typed_payloads(
            market_id,
            [{
                "role": "cex_market_rules",
                "payload": json.dumps(
                    zero_minima, sort_keys=True, separators=(",", ":")
                ).encode("utf-8"),
            }],
        )
        self.assertEqual(len(inventory), 1)

        unsupported_market_id = "cex:okx:UNI/USDT"
        with self.assertRaisesRegex(
            ValueError, "typed-source worker inventory"
        ):
            _validate_cex_typed_payloads(
                unsupported_market_id,
                [{
                    "role": "cex_market_rules",
                    "payload": json.dumps(
                        {**rules, "market_id": unsupported_market_id},
                        sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8"),
                }],
            )

    def test_dex_typed_payloads_require_exact_directional_identity_and_lineage(self):
        from scripts.collect_route_cohort import _validated_typed_payload_inventory

        market_id = (
            "dex:eth:uniswap_v2:"
            "0x3333333333333333333333333333333333333333:AAVE"
        )
        trusted_leg, collector_row, _pool, _rules, _conversion = (
            _typed_dex_inventory_fixture()
        )
        target = "0x" + "1" * 40
        quote = "0x" + "2" * 40
        state_hash = "d" * 64
        rules = {
            "schema": "route_dex_market_rules_source/v1",
            "market_id": market_id,
            "base_asset": "AAVE",
            "quote_asset": "USDC",
            "base_token_address": target,
            "quote_token_address": quote,
            "base_unit_decimals": 18,
            "quote_unit_decimals": 6,
            "base_increment": "0.000000000000000001",
            "quote_increment": "0.000001",
            "min_base_quantity": "0",
            "min_quote_notional": "0",
            "increment_source": "fixed_block_token_decimals",
            "minimum_source": "dex_protocol_no_additional_order_minimum",
            "observed_at": "2024-01-01T00:00:00+00:00",
            "valid_until": "2024-01-01T00:02:00+00:00",
            "raw_response_sha256": state_hash,
        }
        conversion = {
            "schema": "route_dex_usd_conversion_source/v1",
            "market_id": market_id,
            "target_asset": "AAVE",
            "target_token_address": target,
            "quote_asset": "USDC",
            "quote_token_address": quote,
            "usd_per_quote": "1",
            "value_status": "measured",
            "observed_at": "2024-01-01T00:00:01+00:00",
            "valid_until": "2024-01-01T02:00:01+00:00",
            "state_observed_at": "2024-01-01T00:00:00+00:00",
            "source": "GeckoTerminal API v2",
            "source_snapshot_id": "tvl-1",
            "source_raw_response_sha256": "e" * 64,
            "state_raw_response_sha256": state_hash,
        }

        inventory = _validated_typed_payload_inventory(
            trusted_leg=trusted_leg,
            collector_row=collector_row,
            accepted_raw_sha256=state_hash,
            values=[
                {
                    "role": "dex_market_rules",
                    "payload": json.dumps(
                        rules, sort_keys=True, separators=(",", ":")
                    ).encode("utf-8"),
                },
                {
                    "role": "dex_usd_conversion",
                    "payload": json.dumps(
                        conversion, sort_keys=True, separators=(",", ":")
                    ).encode("utf-8"),
                },
            ],
        )
        self.assertEqual(
            [member["role"] for member in inventory],
            ["dex_market_rules", "dex_usd_conversion"],
        )

        mutations = (
            ("rules-wrong-address", {**rules, "quote_token_address": "0x" + "3" * 40}, conversion),
            ("rules-guessed-symbol", {**rules, "quote_asset": "QUOTE"}, conversion),
            ("rules-fake-minimum", {**rules, "min_base_quantity": "1"}, conversion),
            ("rules-window", {**rules, "valid_until": "2024-01-01T00:03:00+00:00"}, conversion),
            ("conversion-wrong-address", rules, {**conversion, "quote_token_address": "0x" + "3" * 40}),
            ("conversion-wrong-state-hash", rules, {**conversion, "state_raw_response_sha256": "f" * 64}),
            ("conversion-window", rules, {**conversion, "valid_until": "2024-01-01T01:00:01+00:00"}),
        )
        for name, candidate_rules, candidate_conversion in mutations:
            with self.subTest(name=name), self.assertRaisesRegex(
                ValueError, "typed-source worker inventory"
            ):
                _validated_typed_payload_inventory(
                    trusted_leg=trusted_leg,
                    collector_row=collector_row,
                    accepted_raw_sha256=state_hash,
                    values=[
                        {
                            "role": "dex_market_rules",
                            "payload": json.dumps(
                                candidate_rules,
                                sort_keys=True,
                                separators=(",", ":"),
                            ).encode("utf-8"),
                        },
                        {
                            "role": "dex_usd_conversion",
                            "payload": json.dumps(
                                candidate_conversion,
                                sort_keys=True,
                                separators=(",", ":"),
                            ).encode("utf-8"),
                        },
                    ],
                )

    def test_dex_typed_inventory_binds_trusted_leg_row_context_and_raw(self):
        from scripts.collect_route_cohort import _validated_typed_payload_inventory

        leg, row, pool, rules, conversion = _typed_dex_inventory_fixture()

        def validate(
            candidate_leg=leg,
            candidate_row=row,
            accepted_sha="d" * 64,
            candidate_pool=pool,
            candidate_rules=rules,
            candidate_conversion=conversion,
        ):
            return _validated_typed_payload_inventory(
                trusted_leg=candidate_leg,
                collector_row=candidate_row,
                accepted_raw_sha256=accepted_sha,
                values=_typed_payload_values(
                    candidate_pool, candidate_rules, candidate_conversion
                ),
            )

        self.assertEqual(
            [member["role"] for member in validate()],
            ["dex_market_rules", "dex_pool_state", "dex_usd_conversion"],
        )
        other = "0x" + "4" * 40
        binding_mismatches = {
            "accepted-raw": {"accepted_sha": "f" * 64},
            "target-address": {
                "candidate_pool": _typed_v2_pool_payload(
                    token0_address=other
                ),
                "candidate_rules": {
                    **rules,
                    "base_token_address": other,
                },
                "candidate_conversion": {
                    **conversion,
                    "target_token_address": other,
                },
            },
            "quote-address": {
                "candidate_pool": _typed_v2_pool_payload(
                    token1_address=other
                ),
                "candidate_rules": {
                    **rules,
                    "quote_token_address": other,
                },
                "candidate_conversion": {
                    **conversion,
                    "quote_token_address": other,
                },
            },
            "quote-decimals": {
                "candidate_pool": _typed_v2_pool_payload(token1_decimals=8),
                "candidate_rules": {
                    **rules,
                    "quote_unit_decimals": 8,
                    "quote_increment": "0.00000001",
                },
            },
            "state-time": {
                "candidate_pool": _typed_v2_pool_payload(
                    observed_at="2024-01-01T00:00:30+00:00"
                ),
                "candidate_rules": {
                    **rules,
                    "observed_at": "2024-01-01T00:00:30+00:00",
                    "valid_until": "2024-01-01T00:02:30+00:00",
                },
                "candidate_conversion": {
                    **conversion,
                    "state_observed_at": "2024-01-01T00:00:30+00:00",
                },
            },
            "state-hash": {
                "candidate_pool": _typed_v2_pool_payload(
                    raw_response_sha256="f" * 64
                ),
                "candidate_rules": {
                    **rules,
                    "raw_response_sha256": "f" * 64,
                },
                "candidate_conversion": {
                    **conversion,
                    "state_raw_response_sha256": "f" * 64,
                },
            },
            "usd-rate": {
                "candidate_conversion": {
                    **conversion,
                    "usd_per_quote": "2",
                },
            },
            "usd-source": {
                "candidate_conversion": {
                    **conversion,
                    "source": "other source",
                },
            },
            "usd-snapshot": {
                "candidate_conversion": {
                    **conversion,
                    "source_snapshot_id": "other-snapshot",
                },
            },
            "usd-source-hash": {
                "candidate_conversion": {
                    **conversion,
                    "source_raw_response_sha256": "f" * 64,
                },
            },
            "usd-time": {
                "candidate_conversion": {
                    **conversion,
                    "observed_at": "2024-01-01T00:00:02+00:00",
                    "valid_until": "2024-01-01T02:00:02+00:00",
                },
            },
            "row-context-price": {
                "candidate_row": {**row, "token1_price_usd": "2"},
                "candidate_conversion": {
                    **conversion,
                    "usd_per_quote": "2",
                },
            },
        }
        for name, arguments in binding_mismatches.items():
            with self.subTest(name=name), self.assertRaisesRegex(
                ValueError, "typed-source worker inventory"
            ):
                validate(**arguments)

    def test_missing_dex_usd_lineage_keeps_independently_valid_rules(self):
        from scripts.collect_route_cohort import _validated_typed_payload_inventory

        leg, row, pool, rules, conversion = _typed_dex_inventory_fixture()
        row_without_usd_lineage = {
            **row,
            "usd_price_source_snapshot_id": "",
            "usd_price_observed_at": "",
            "usd_price_source": "",
            "usd_price_source_endpoint": "",
            "usd_price_raw_response_sha256": "",
        }

        inventory = _validated_typed_payload_inventory(
            trusted_leg=leg,
            collector_row=row_without_usd_lineage,
            accepted_raw_sha256="d" * 64,
            values=_typed_payload_values(pool, rules, conversion)[:2],
        )

        self.assertEqual(
            [member["role"] for member in inventory],
            ["dex_market_rules", "dex_pool_state"],
        )

    def test_directional_dex_sources_without_pool_state_require_supported_evm(self):
        from scripts.collect_route_cohort import _validated_typed_payload_inventory

        leg, row, _pool, rules, conversion = _typed_dex_inventory_fixture()
        unsupported_id = (
            "dex:solana:orca:"
            "0x3333333333333333333333333333333333333333:AAVE"
        )
        unsupported_context = {
            **leg["collector_context"],
            "base_token_id": "solana_" + "0x" + "1" * 40,
            "quote_token_id": "solana_" + "0x" + "2" * 40,
        }
        unsupported_leg = {
            **leg,
            "market_id": unsupported_id,
            "collector_context": unsupported_context,
        }
        unsupported_row = {
            **row,
            "market_id": unsupported_id,
            "chain": "solana",
            "dex": "orca",
        }
        values = _typed_payload_values(
            _typed_v2_pool_payload(),
            {**rules, "market_id": unsupported_id},
            {**conversion, "market_id": unsupported_id},
        )[1:]
        with self.assertRaisesRegex(
            ValueError, "typed-source worker inventory"
        ):
            _validated_typed_payload_inventory(
                trusted_leg=unsupported_leg,
                collector_row=unsupported_row,
                accepted_raw_sha256="d" * 64,
                values=values,
            )

        zero_target_rules = {
            **rules,
            "base_token_address": "0x" + "0" * 40,
        }
        zero_target_conversion = {
            **conversion,
            "target_token_address": "0x" + "0" * 40,
        }
        with self.assertRaisesRegex(
            ValueError, "typed-source worker inventory"
        ):
            _validated_typed_payload_inventory(
                trusted_leg=leg,
                collector_row=row,
                accepted_raw_sha256="d" * 64,
                values=_typed_payload_values(
                    _pool, zero_target_rules, zero_target_conversion
                )[1:],
            )

    def test_dex_directional_payloads_cannot_mix_with_another_valid_pool_state(self):
        from scripts.collect_route_cohort import _validated_typed_payload_inventory

        market_id = (
            "dex:eth:uniswap_v2:"
            "0x3333333333333333333333333333333333333333:AAVE"
        )
        trusted_leg, collector_row, _pool, _rules, _conversion = (
            _typed_dex_inventory_fixture()
        )
        rules = {
            "schema": "route_dex_market_rules_source/v1",
            "market_id": market_id,
            "base_asset": "AAVE",
            "quote_asset": "USDC",
            "base_token_address": "0x" + "1" * 40,
            "quote_token_address": "0x" + "2" * 40,
            "base_unit_decimals": 18,
            "quote_unit_decimals": 6,
            "base_increment": "0.000000000000000001",
            "quote_increment": "0.000001",
            "min_base_quantity": "0",
            "min_quote_notional": "0",
            "increment_source": "fixed_block_token_decimals",
            "minimum_source": "dex_protocol_no_additional_order_minimum",
            "observed_at": "2024-01-01T00:00:00+00:00",
            "valid_until": "2024-01-01T00:02:00+00:00",
            "raw_response_sha256": "d" * 64,
        }
        conversion = {
            "schema": "route_dex_usd_conversion_source/v1",
            "market_id": market_id,
            "target_asset": "AAVE",
            "target_token_address": "0x" + "1" * 40,
            "quote_asset": "USDC",
            "quote_token_address": "0x" + "2" * 40,
            "usd_per_quote": "1",
            "value_status": "measured",
            "observed_at": "2024-01-01T00:00:01+00:00",
            "valid_until": "2024-01-01T02:00:01+00:00",
            "state_observed_at": "2024-01-01T00:00:00+00:00",
            "source": "GeckoTerminal API v2",
            "source_snapshot_id": "tvl-1",
            "source_raw_response_sha256": "e" * 64,
            "state_raw_response_sha256": "d" * 64,
        }

        def validate(pool):
            return _validated_typed_payload_inventory(
                trusted_leg=trusted_leg,
                collector_row=collector_row,
                accepted_raw_sha256="d" * 64,
                values=[
                    {
                        "role": "dex_pool_state",
                        "payload": json.dumps(
                            pool, sort_keys=True, separators=(",", ":")
                        ).encode("utf-8"),
                    },
                    {
                        "role": "dex_market_rules",
                        "payload": json.dumps(
                            rules, sort_keys=True, separators=(",", ":")
                        ).encode("utf-8"),
                    },
                    {
                        "role": "dex_usd_conversion",
                        "payload": json.dumps(
                            conversion, sort_keys=True, separators=(",", ":")
                        ).encode("utf-8"),
                    },
                ],
            )

        self.assertEqual(
            [member["role"] for member in validate(_typed_v2_pool_payload())],
            ["dex_market_rules", "dex_pool_state", "dex_usd_conversion"],
        )
        mismatched_states = {
            "pool-market": _typed_v2_pool_payload(
                pool_address="0x" + "4" * 40
            ),
            "target-address": _typed_v2_pool_payload(
                token0_address="0x" + "4" * 40
            ),
            "quote-decimals": _typed_v2_pool_payload(token1_decimals=8),
            "observed-at": _typed_v2_pool_payload(
                observed_at="2024-01-01T00:00:30+00:00"
            ),
            "raw-hash": _typed_v2_pool_payload(
                raw_response_sha256="f" * 64
            ),
        }
        for name, pool in mismatched_states.items():
            with self.subTest(name=name), self.assertRaisesRegex(
                ValueError, "typed-source worker inventory"
            ):
                validate(pool)

    def test_attach_marks_missing_dex_directional_sources_unavailable_without_inference(self):
        market_id = (
            "dex:eth:uniswap_v2:"
            "0x3333333333333333333333333333333333333333:AAVE"
        )
        raw = b'{"pool":1}'
        cohort = _RouteCollectionResult({
            "raw_evidence_run_id": "run-dex-missing-directional",
            "legs": [{
                "market_id": market_id,
                "market_type": "dex",
                "status": "observed",
                "token_symbol": "AAVE",
                "target_token_address": "0x" + "1" * 40,
                "quote_token_address": "0x" + "2" * 40,
                "quote_asset": "USDC",
                "quote_to_usd": "999",
                "raw_response_sha256": hashlib.sha256(raw).hexdigest(),
            }],
        }, {market_id: ()})

        with tempfile.TemporaryDirectory() as temporary:
            accepted = (
                Path(temporary) / "run-dex-missing-directional" / "accepted"
                / hashlib.sha256(market_id.encode("utf-8")).hexdigest()
            )
            accepted.mkdir(parents=True)
            (accepted / "response.json").write_bytes(raw)
            _install_attachment_authority_fixture(
                accepted, cohort, market_id
            )
            normalized, publication = attach_typed_source_lineage(
                cohort, raw_root=Path(temporary)
            )

        lineage = normalized["legs"][0]["typed_source_lineage"]
        by_role = {member["role"]: member for member in lineage["members"]}
        self.assertEqual(lineage["schema"], "route_leg_typed_source_lineage/v2")
        self.assertEqual(publication["manifest"]["member_count"], 0)
        self.assertEqual(by_role["dex_market_rules"]["status"], "unavailable")
        self.assertEqual(by_role["dex_usd_conversion"]["status"], "unavailable")

    def test_fork_returned_dex_typed_inventory_is_sealed_then_physically_attached(self):
        left = (
            "dex:eth:uniswap_v2:"
            "0x3333333333333333333333333333333333333333:UNI"
        )
        right = (
            "dex:eth:uniswap_v2:"
            "0x4444444444444444444444444444444444444444:UNI"
        )
        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [
                {
                    "market_id": left,
                    "market_type": "dex",
                    "chain": "eth",
                    "token_symbol": "UNI",
                    "target_token_address": "0x" + "1" * 40,
                },
                {
                    "market_id": right,
                    "market_type": "dex",
                    "chain": "eth",
                    "token_symbol": "UNI",
                    "target_token_address": "0x" + "1" * 40,
                },
            ],
            "routes": [{
                "route_id": "route:UNI:{}->{}:atomic_onchain".format(left, right),
                "token_symbol": "UNI",
                "buy_market_id": left,
                "sell_market_id": right,
                "route_mode": "atomic_onchain",
            }],
        }
        header = {
            "number": "0x7b",
            "hash": "0x" + "a" * 64,
            "parent_hash": "0x" + "b" * 64,
            "timestamp": "0x65920080",
            "base_fee_per_gas": "0x3b9aca00",
            "gas_used": "0xe4e1c0",
            "gas_limit": "0x1c9c380",
        }

        def collect_dex(leg, *, raw_path, fixed_block_number,
                        fixed_block_timestamp, fixed_chain_id,
                        fixed_block_header,
                        typed_source_payload_sink, **_kwargs):
            self.assertEqual(fixed_chain_id, "0x1")
            self.assertEqual(fixed_block_header, header)
            raw_path.write_bytes(b"dex raw evidence")
            raw_sha = hashlib.sha256(raw_path.read_bytes()).hexdigest()
            pool_address = leg["market_id"].split(":", 4)[3]
            state = V2PoolState(
                chain="eth", chain_id=1, dex="uniswap_v2",
                pool_address=pool_address,
                token0_address="0x" + "1" * 40,
                token1_address="0x" + "2" * 40,
                token0_decimals=18, token1_decimals=6,
                reserve0_raw=100 * 10**18,
                reserve1_raw=10_000 * 10**6,
                reserve_timestamp_last_raw=1_704_067_200,
                fee_bps=30, fee_numerator=9_970, fee_denominator=10_000,
                fee_formula=V2_FEE_FORMULA,
                fee_proof_sha256=ROUTE_V2_FEE_PROOF_SHA256,
                block_number=123, block_hash=header["hash"],
                block_header_sha256=hashlib.sha256(
                    json.dumps(
                        header, ensure_ascii=False, sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8")
                ).hexdigest(),
                observed_at=fixed_block_timestamp,
                raw_response_sha256=raw_sha,
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
                    for field in (
                        "chain", "chain_id", "dex", "pool_address",
                        "token0_address", "token1_address", "token0_decimals",
                        "token1_decimals", "reserve0_raw", "reserve1_raw",
                        "reserve_timestamp_last_raw", "fee_bps",
                        "fee_numerator", "fee_denominator", "fee_formula",
                        "fee_proof_sha256", "block_number", "block_hash",
                        "block_header_sha256", "observed_at",
                        "raw_response_sha256", "state_id",
                    )
                },
            }
            typed_source_payload_sink({
                "role": "dex_pool_state",
                "payload": json.dumps(
                    payload, ensure_ascii=False, sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8"),
            })
            return {
                "market_id": leg["market_id"],
                "market_type": "dex",
                "status": "observed",
                "token_symbol": "UNI",
                "chain": "eth",
                "dex": "uniswap_v2",
                "pool_address": pool_address,
                "state_observed_at": fixed_block_timestamp,
                "block_number": str(fixed_block_number),
                "block_timestamp": fixed_block_timestamp,
                "target_token_position": "token0",
                "target_token_address": "0x" + "1" * 40,
                "token0_address": "0x" + "1" * 40,
                "token0_decimals": "18",
                "token1_address": "0x" + "2" * 40,
                "token1_decimals": "6",
                "raw_response_sha256": raw_sha,
            }

        with tempfile.TemporaryDirectory() as temporary:
            raw_root = Path(temporary) / "raw"
            wall = iter([
                datetime(2024, 1, 1, 0, 0, 1, tzinfo=timezone.utc),
                datetime(2024, 1, 1, 0, 0, 2, tzinfo=timezone.utc),
            ])
            cohort = _collect_route_cohort(
                universe,
                cex_collector=lambda *_args, **_kwargs: self.fail("unexpected CEX"),
                dex_collector=collect_dex,
                dex_block_resolver=lambda *_args, **_kwargs: {
                    "block_number": 123,
                    "block_timestamp": "2024-01-01T00:00:00Z",
                    "chain_id": "0x1",
                    "block_header": header,
                },
                raw_root=raw_root,
                snapshot_id="typed-dex-run",
                executor_factory=ThreadPoolExecutor,
                source_generation_reader=lambda: "input-a",
                expected_source_generation="input-a",
                wall_clock=lambda: next(wall),
            )
            self.assertNotIn("payload", json.dumps(cohort["legs"]))
            accepted = raw_root / "typed-dex-run" / "accepted"
            self.assertEqual(
                sorted(path.name for path in accepted.iterdir()),
                sorted(
                    hashlib.sha256(value.encode("utf-8")).hexdigest()
                    for value in (left, right)
                ),
            )
            for directory in accepted.iterdir():
                self.assertEqual(
                    sorted(path.name for path in directory.iterdir()),
                    ["attachment-authority.json", "response.json"],
                )

            original_payloads = cohort._typed_source_payloads
            cohort._typed_source_payloads = {
                market_id: tuple(
                    {
                        **member,
                        "payload": (
                            b"{}"
                            if market_id == left
                            else member["payload"]
                        ),
                        "logical_generation": (
                            hashlib.sha256(b"{}").hexdigest()
                            if market_id == left
                            else member["logical_generation"]
                        ),
                    }
                    for member in members
                )
                for market_id, members in original_payloads.items()
            }
            with self.assertRaisesRegex(
                ValueError, "typed-source.*invalid|payload capability"
            ):
                attach_typed_source_lineage(cohort, raw_root=raw_root)
            cohort._typed_source_payloads = original_payloads

            normalized, publication = attach_typed_source_lineage(
                cohort, raw_root=raw_root
            )
            self.assertNotIn("_typed_source_payload_capability", normalized)
            self.assertEqual(publication["manifest"]["member_count"], 2)
            self.assertEqual(
                [
                    member["role"]
                    for leg in normalized["legs"]
                    for member in leg["typed_source_lineage"]["members"]
                    if member["status"] == "observed"
                ],
                ["dex_pool_state", "dex_pool_state"],
            )
            for record in publication["manifest"]["members"]:
                payload = (
                    raw_root / "typed-dex-run" / "typed" / record["filename"]
                ).read_bytes()
                self.assertEqual(hashlib.sha256(payload).hexdigest(), record["sha256"])

    def test_default_resolver_projects_one_complete_header_from_the_same_block(self):
        header = {
            "number": "0x7b",
            "hash": "0x" + "a" * 64,
            "parentHash": "0x" + "b" * 64,
            "timestamp": "0x65920080",
            "baseFeePerGas": "0x3b9aca00",
            "gasUsed": "0xe4e1c0",
            "gasLimit": "0x1c9c380",
        }

        class ResolverClient:
            def __init__(self, chain, url, *, deadline):
                self.chain = chain
                self.url = url
                self.deadline = deadline

            def block_number(self):
                return 123

            def chain_id(self):
                return "0x1"

            def block(self, tag):
                self.assertEqual(tag, "0x7b")
                return header

            def assertEqual(self, left, right):
                if left != right:
                    raise AssertionError((left, right))

        from scripts.collection_deadline import CollectionDeadline

        with patch("scripts.collect_route_cohort.RpcClient", ResolverClient), patch(
            "scripts.collect_route_cohort.rpc_url_for_chain",
            return_value="https://rpc.example.test",
        ):
            result = _default_dex_block_resolver(
                "eth", deadline=CollectionDeadline.for_duration(5)
            )
        self.assertEqual(result["block_number"], 123)
        self.assertEqual(result["chain_id"], "0x1")
        self.assertEqual(result["block_timestamp"], "2024-01-01T00:00:00+00:00")
        self.assertEqual(
            result["block_header"],
            {
                "number": "0x7b",
                "hash": "0x" + "a" * 64,
                "parent_hash": "0x" + "b" * 64,
                "timestamp": "0x65920080",
                "base_fee_per_gas": "0x3b9aca00",
                "gas_used": "0xe4e1c0",
                "gas_limit": "0x1c9c380",
            },
        )

    def test_default_resolver_rejects_a_non_ethereum_chain_id_from_same_client(self):
        header = {
            "number": "0x7b",
            "hash": "0x" + "a" * 64,
            "parentHash": "0x" + "b" * 64,
            "timestamp": "0x65920080",
            "baseFeePerGas": "0x3b9aca00",
            "gasUsed": "0xe4e1c0",
            "gasLimit": "0x1c9c380",
        }

        class WrongChainClient:
            def __init__(self, _chain, _url, *, deadline):
                self.deadline = deadline

            def chain_id(self):
                return "0x38"

            def block_number(self):
                raise AssertionError("wrong chain must fail before block reads")

            def block(self, _tag):
                return header

        from scripts.collection_deadline import CollectionDeadline

        with patch(
            "scripts.collect_route_cohort.RpcClient", WrongChainClient
        ), patch(
            "scripts.collect_route_cohort.rpc_url_for_chain",
            return_value="https://rpc.example.test",
        ):
            with self.assertRaisesRegex(ValueError, "chain ID"):
                _default_dex_block_resolver(
                    "eth", deadline=CollectionDeadline.for_duration(5)
                )

    def test_typed_capability_requires_exact_final_eligible_market_inventory(self):
        market_id = "dex:eth:uniswap_v2:0x" + "3" * 40 + ":UNI"
        cohort_value = {
            "raw_evidence_run_id": "typed-capability-run",
            "legs": [{
                "market_id": market_id,
                "market_type": "dex",
                "status": "observed",
            }],
        }
        orphan_id = "dex:eth:uniswap_v2:0x" + "4" * 40 + ":UNI"
        with tempfile.TemporaryDirectory() as temporary:
            raw_root = Path(temporary) / "raw"
            for capability in ({}, {market_id: (), orphan_id: ()}):
                with self.subTest(keys=sorted(capability)):
                    cohort = _RouteCollectionResult(cohort_value, capability)
                    with self.assertRaisesRegex(
                        ValueError, "typed-source payload capability"
                    ):
                        attach_typed_source_lineage(cohort, raw_root=raw_root)

    def test_typed_capability_is_discarded_after_raw_and_promotion_failures(self):
        universe = _strict_cex_universe()

        def typed_cex(leg, *, raw_path, typed_source_payload_sink, **_kwargs):
            raw = ("raw:" + leg["market_id"]).encode("utf-8")
            raw_path.write_bytes(raw)
            typed_source_payload_sink({
                "role": "cex_market_rules",
                "payload": b'{"schema":"test-market-rules"}',
            })
            return {
                "status": "observed",
                "state_observed_at": "2026-08-01T12:00:00Z",
                "raw_response_sha256": hashlib.sha256(raw).hexdigest(),
            }

        def mismatched_raw(leg, **kwargs):
            row = typed_cex(leg, **kwargs)
            return {**row, "raw_response_sha256": "0" * 64}

        scenarios = (
            ("raw", mismatched_raw, None),
            ("promotion", typed_cex, OSError("promotion failed")),
            ("post-promotion", typed_cex, "raw_evidence_hash_mismatch"),
        )
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for name, collector, injected in scenarios:
                with self.subTest(name=name):
                    patches = []
                    if name == "promotion":
                        patches.append(patch(
                            "scripts.collect_route_cohort._rename_directory_entry",
                            side_effect=injected,
                        ))
                    elif name == "post-promotion":
                        patches.append(patch(
                            "scripts.collect_route_cohort._post_promotion_failure",
                            return_value=injected,
                        ))
                    for active_patch in patches:
                        active_patch.start()
                    try:
                        cohort = _collect_route_cohort(
                            universe,
                            cex_collector=collector,
                            dex_collector=lambda *_args, **_kwargs: None,
                            raw_root=root / name,
                            snapshot_id=name + "-run",
                            executor_factory=ThreadPoolExecutor,
                            source_generation_reader=lambda: "input-a",
                            expected_source_generation="input-a",
                        )
                    finally:
                        for active_patch in reversed(patches):
                            active_patch.stop()
                    self.assertEqual(cohort._typed_source_payloads, {})
                    self.assertTrue(all(
                        row["status"] == "failed" for row in cohort["legs"]
                    ))
                    normalized, publication = attach_typed_source_lineage(
                        cohort, raw_root=root / name
                    )
                    self.assertEqual(publication["manifest"]["member_count"], 0)
                    self.assertTrue(all(
                        not any(
                            member["status"] == "observed"
                            for member in row["typed_source_lineage"]["members"]
                        )
                        for row in normalized["legs"]
                    ))

    def test_attachment_authority_write_failure_never_promotes_half_entry(self):
        universe = _strict_cex_universe()

        def observed_cex(leg, *, raw_path, **_kwargs):
            payload = ("raw:" + leg["market_id"]).encode("utf-8")
            raw_path.write_bytes(payload)
            return {
                "status": "observed",
                "state_observed_at": "2026-08-01T12:00:00Z",
                "raw_response_sha256": hashlib.sha256(payload).hexdigest(),
            }

        with tempfile.TemporaryDirectory() as temporary, patch(
            "scripts.collect_route_cohort._write_regular_file_at",
            side_effect=OSError("authority fsync failed"),
        ):
            raw_root = Path(temporary)
            cohort = _collect_route_cohort(
                universe,
                cex_collector=observed_cex,
                dex_collector=lambda *_args, **_kwargs: None,
                raw_root=raw_root,
                snapshot_id="authority-write-failure",
                executor_factory=ThreadPoolExecutor,
                source_generation_reader=lambda: "input-a",
                expected_source_generation="input-a",
            )
            accepted = (
                raw_root / cohort["raw_evidence_run_id"] / "accepted"
            )
            self.assertEqual(list(accepted.iterdir()), [])

        self.assertEqual(cohort._typed_source_payloads, {})
        self.assertTrue(all(
            row["status"] == "failed"
            and row["reason_code"] == "raw_evidence_path_unsafe"
            for row in cohort["legs"]
        ))

    def test_authority_write_reread_swap_never_deletes_foreign_bytes(self):
        from scripts.collect_route_cohort import (
            _read_regular_file_at as real_read,
            _write_regular_file_at,
        )

        name = "attachment-authority.json"
        attempt_bytes = b'{"authority":"attempt"}'
        foreign_bytes = b'{"authority":"foreign"}'
        swapped = False

        with tempfile.TemporaryDirectory() as temporary:
            directory = Path(temporary)
            directory_descriptor = os.open(
                str(directory),
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
            )

            def swap_before_reread(
                descriptor, candidate_name, *, max_bytes=None
            ):
                nonlocal swapped
                if not swapped and candidate_name == name:
                    swapped = True
                    os.rename(
                        name,
                        "rescued-attempt-authority.json",
                        src_dir_fd=descriptor,
                        dst_dir_fd=descriptor,
                    )
                    foreign_descriptor = os.open(
                        name,
                        os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                        0o600,
                        dir_fd=descriptor,
                    )
                    try:
                        os.write(foreign_descriptor, foreign_bytes)
                        os.fsync(foreign_descriptor)
                    finally:
                        os.close(foreign_descriptor)
                return real_read(
                    descriptor, candidate_name, max_bytes=max_bytes
                )

            try:
                with patch(
                    "scripts.collect_route_cohort._read_regular_file_at",
                    side_effect=swap_before_reread,
                ), self.assertRaisesRegex(
                    ValueError, "write changed|quarantine|unsafe"
                ):
                    _write_regular_file_at(
                        directory_descriptor,
                        name,
                        attempt_bytes,
                        max_bytes=1024,
                    )
            finally:
                os.close(directory_descriptor)

            self.assertTrue(swapped)
            self.assertFalse((directory / name).exists())
            self.assertEqual(
                (directory / "rescued-attempt-authority.json").read_bytes(),
                attempt_bytes,
            )
            quarantined = list(directory.glob(
                ".raw-write-quarantine-*-attachment-authority.json"
            ))
            self.assertEqual(len(quarantined), 1)
            self.assertEqual(quarantined[0].read_bytes(), foreign_bytes)

    def test_fixed_block_mismatch_discards_worker_typed_capability(self):
        left = "dex:eth:uniswap_v2:0x" + "3" * 40 + ":UNI"
        right = "dex:eth:uniswap_v2:0x" + "4" * 40 + ":UNI"
        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [
                {"market_id": left, "market_type": "dex", "chain": "eth"},
                {"market_id": right, "market_type": "dex", "chain": "eth"},
            ],
            "routes": [_strict_route("UNI", left, right, "atomic_onchain")],
        }

        def mismatched_dex(_leg, *, raw_path, typed_source_payload_sink, **_kwargs):
            raw_path.write_bytes(b"mismatched fixed block")
            typed_source_payload_sink({
                "role": "dex_pool_state", "payload": b"must-not-survive"
            })
            return {
                "status": "observed",
                "state_observed_at": "2026-08-01T12:00:00Z",
                "block_number": "124",
                "block_timestamp": "2026-08-01T12:00:00Z",
            }

        with tempfile.TemporaryDirectory() as temporary:
            cohort = _collect_route_cohort(
                universe,
                cex_collector=lambda *_args, **_kwargs: None,
                dex_collector=mismatched_dex,
                dex_block_resolver=lambda *_args, **_kwargs: {
                    "block_number": 123,
                    "block_timestamp": "2026-08-01T12:00:00Z",
                    "chain_id": "0x1",
                },
                raw_root=Path(temporary),
                snapshot_id="fixed-mismatch-run",
                executor_factory=ThreadPoolExecutor,
                source_generation_reader=lambda: "input-a",
                expected_source_generation="input-a",
            )
        self.assertEqual(cohort._typed_source_payloads, {})
        self.assertTrue(all(
            row["reason_code"] == "fixed_block_lineage_mismatch"
            for row in cohort["legs"]
        ))

    def test_wrong_resolved_chain_is_terminal_and_never_calls_dex_or_keeps_typed(self):
        left = "dex:eth:uniswap_v2:0x" + "3" * 40 + ":UNI"
        right = "dex:eth:uniswap_v2:0x" + "4" * 40 + ":UNI"
        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [
                {"market_id": left, "market_type": "dex", "chain": "eth"},
                {"market_id": right, "market_type": "dex", "chain": "eth"},
            ],
            "routes": [_strict_route("UNI", left, right, "atomic_onchain")],
        }
        calls = []
        with tempfile.TemporaryDirectory() as temporary:
            cohort = _collect_route_cohort(
                universe,
                cex_collector=lambda *_args, **_kwargs: None,
                dex_collector=lambda *_args, **_kwargs: calls.append("dex"),
                dex_block_resolver=lambda *_args, **_kwargs: {
                    "block_number": 123,
                    "block_timestamp": "2026-08-01T12:00:00Z",
                    "chain_id": "0x38",
                },
                raw_root=Path(temporary),
                snapshot_id="wrong-chain-run",
                executor_factory=ThreadPoolExecutor,
                wall_clock=lambda: datetime(
                    2026, 8, 1, 12, 0, 1, tzinfo=timezone.utc
                ),
                source_generation_reader=lambda: "input-a",
                expected_source_generation="input-a",
            )
        self.assertEqual(calls, [])
        self.assertEqual(cohort._typed_source_payloads, {})
        self.assertTrue(all(
            row["status"] == "failed"
            and row["reason_code"] == "fixed_block_unavailable"
            for row in cohort["legs"]
        ))

    def test_logical_generation_requires_a_lowercase_hex_string(self):
        member = {
            "market_id": "cex:binance:UNI/USDT",
            "role": "cex_raw_book_response",
            "payload": b'{"book":1}',
            "logical_generation": int("1" * 64),
            "adapter_id": "fetch_cex_depth/parse_book/v1",
            "content_schema": "route_bytes/v1",
        }
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaisesRegex(ValueError, "typed-source.*invalid"):
                publish_typed_source_manifest(
                    Path(temporary), raw_evidence_run_id="run-1",
                    members=[member],
                )

    def test_exact_observed_inventory_is_installed_and_manifest_hash_bound(self):
        with tempfile.TemporaryDirectory() as temporary:
            run_root = Path(temporary) / "raw/run-1"
            members = [
                {
                    "market_id": "cex:binance:UNI/USDT",
                    "role": "cex_raw_book_response",
                    "payload": b'{"book":1}',
                    "logical_generation": "a" * 64,
                    "adapter_id": "fetch_cex_depth/parse_book/v1",
                    "content_schema": "route_bytes/v1",
                },
                {
                    "market_id": "cex:binance:UNI/USDT",
                    "role": "cex_market_rules",
                    "payload": b'{"rules":1}',
                    "logical_generation": "b" * 64,
                    "adapter_id": "route_quantity_quote_for_book/v1",
                    "content_schema": "route_market_rules_source/v1",
                },
                {
                    "market_id": "cex:binance:UNI/USDT",
                    "role": "quote_usd_conversion",
                    "payload": b'{"usd":1}',
                    "logical_generation": "c" * 64,
                    "adapter_id": "route_usd_conversion_source/v1",
                    "content_schema": "route_usd_conversion_source/v1",
                },
            ]
            result = publish_typed_source_manifest(
                run_root, raw_evidence_run_id="run-1", members=members
            )
            manifest = json.loads(
                (run_root / "typed-manifest.json").read_text(encoding="utf-8")
            )
            self.assertEqual(manifest["member_count"], 3)
            self.assertEqual(
                result["typed_source_manifest_sha256"],
                hashlib.sha256(
                    (run_root / "typed-manifest.json").read_bytes()
                ).hexdigest(),
            )
            self.assertEqual(
                sorted(path.name for path in (run_root / "typed").iterdir()),
                [row["filename"] for row in manifest["members"]],
            )
            for row in manifest["members"]:
                payload = (run_root / "typed" / row["filename"]).read_bytes()
                self.assertEqual(hashlib.sha256(payload).hexdigest(), row["sha256"])

    def test_postcommit_source_drift_rolls_back_new_typed_publication(self):
        member = {
            "market_id": "cex:okx:UNI/USDT",
            "role": "cex_raw_book_response",
            "payload": b'{"book":1}',
            "logical_generation": "a" * 64,
            "adapter_id": "fetch_cex_depth/parse_book/v1",
            "content_schema": "route_bytes/v1",
        }
        validations = 0

        def source_validator():
            nonlocal validations
            validations += 1
            if validations == 2:
                raise ValueError("accepted response changed")

        with tempfile.TemporaryDirectory() as temporary:
            run_root = Path(temporary) / "run"
            with self.assertRaisesRegex(ValueError, "response changed"):
                publish_typed_source_manifest(
                    run_root,
                    raw_evidence_run_id="postcommit-drift",
                    members=[member],
                    source_validator=source_validator,
                )
            self.assertEqual(validations, 2)
            self.assertFalse((run_root / "typed").exists())
            self.assertFalse((run_root / "typed-manifest.json").exists())
            quarantined_typed = list(
                run_root.glob(".typed-quarantine-*-typed")
            )
            quarantined_manifests = list(
                run_root.glob(
                    ".typed-quarantine-*-typed-manifest.json"
                )
            )
            self.assertEqual(len(quarantined_typed), 1)
            self.assertEqual(len(quarantined_manifests), 1)
            self.assertEqual(
                (
                    quarantined_typed[0]
                    / "0000-cex_raw_book_response.json"
                ).read_bytes(),
                member["payload"],
            )
            self.assertEqual(
                json.loads(quarantined_manifests[0].read_bytes())[
                    "raw_evidence_run_id"
                ],
                "postcommit-drift",
            )
            self.assertEqual(
                len(list(run_root.glob(".typed-stage-*"))), 1
            )

    def test_rollback_never_unlinks_same_name_foreign_member(self):
        member = {
            "market_id": "cex:okx:UNI/USDT",
            "role": "cex_raw_book_response",
            "payload": b'{"book":1}',
            "logical_generation": "a" * 64,
            "adapter_id": "fetch_cex_depth/parse_book/v1",
            "content_schema": "route_bytes/v1",
        }
        validations = 0
        foreign_bytes = b"FOREIGN-MEMBER-MUST-SURVIVE"

        with tempfile.TemporaryDirectory() as temporary:
            run_root = Path(temporary) / "run"

            def source_validator():
                nonlocal validations
                validations += 1
                if validations != 2:
                    return
                member_path = (
                    run_root / "typed/0000-cex_raw_book_response.json"
                )
                member_path.rename(run_root / "rescued-attempt-member.json")
                foreign = run_root / "foreign-member.json"
                foreign.write_bytes(foreign_bytes)
                foreign.rename(member_path)
                raise ValueError("accepted response changed")

            with self.assertRaisesRegex(ValueError, "response changed"):
                publish_typed_source_manifest(
                    run_root,
                    raw_evidence_run_id="foreign-member-rollback",
                    members=[member],
                    source_validator=source_validator,
                )

            self.assertFalse((run_root / "typed").exists())
            self.assertFalse((run_root / "typed-manifest.json").exists())
            quarantined_typed = list(
                run_root.glob(".typed-quarantine-*-typed")
            )
            self.assertEqual(len(quarantined_typed), 1)
            self.assertEqual(
                (
                    quarantined_typed[0]
                    / "0000-cex_raw_book_response.json"
                ).read_bytes(),
                foreign_bytes,
            )
            self.assertEqual(
                (run_root / "rescued-attempt-member.json").read_bytes(),
                member["payload"],
            )
            self.assertEqual(
                len(list(run_root.glob(
                    ".typed-quarantine-*-typed-manifest.json"
                ))),
                1,
            )

    def test_stage_cleanup_never_follows_replaced_stage_path(self):
        member = {
            "market_id": "cex:okx:UNI/USDT",
            "role": "cex_raw_book_response",
            "payload": b'{"book":1}',
            "logical_generation": "a" * 64,
            "adapter_id": "fetch_cex_depth/parse_book/v1",
            "content_schema": "route_bytes/v1",
        }
        valuable_bytes = b"FOREIGN-STAGE-DATA-MUST-SURVIVE"

        with tempfile.TemporaryDirectory() as temporary:
            run_root = Path(temporary) / "run"

            def replace_stage_then_fail():
                stage = next(run_root.glob(".typed-stage-*"))
                stage.rename(run_root / ".saved-attempt-stage")
                stage.mkdir()
                (stage / "typed").mkdir()
                (stage / "typed/valuable.bin").write_bytes(valuable_bytes)
                (stage / "typed-manifest.json").write_bytes(
                    b"FOREIGN-MANIFEST-MUST-SURVIVE"
                )
                raise ValueError("accepted response changed")

            with self.assertRaisesRegex(ValueError, "response changed"):
                publish_typed_source_manifest(
                    run_root,
                    raw_evidence_run_id="foreign-stage-cleanup",
                    members=[member],
                    source_validator=replace_stage_then_fail,
                )

            foreign_stage = next(run_root.glob(".typed-stage-*"))
            self.assertEqual(
                (foreign_stage / "typed/valuable.bin").read_bytes(),
                valuable_bytes,
            )
            self.assertEqual(
                (foreign_stage / "typed-manifest.json").read_bytes(),
                b"FOREIGN-MANIFEST-MUST-SURVIVE",
            )
            saved_stage = run_root / ".saved-attempt-stage"
            self.assertEqual(
                (
                    saved_stage
                    / "typed/0000-cex_raw_book_response.json"
                ).read_bytes(),
                member["payload"],
            )
            self.assertEqual(
                json.loads(
                    (saved_stage / "typed-manifest.json").read_bytes()
                )["raw_evidence_run_id"],
                "foreign-stage-cleanup",
            )
            self.assertFalse((run_root / "typed").exists())
            self.assertFalse((run_root / "typed-manifest.json").exists())

    def test_quarantine_detach_boundary_swap_preserves_all_bytes(self):
        from scripts.collect_route_cohort import (
            _rename_directory_entry as real_rename,
        )

        member_bytes = b'{"book":1}'
        foreign_bytes = b"FOREIGN-TYPED-DIRECTORY-MUST-SURVIVE"
        member = {
            "market_id": "cex:okx:UNI/USDT",
            "role": "cex_raw_book_response",
            "payload": member_bytes,
            "logical_generation": "a" * 64,
            "adapter_id": "fetch_cex_depth/parse_book/v1",
            "content_schema": "route_bytes/v1",
        }
        validations = 0
        swapped = False
        attempt_manifest_bytes = None

        with tempfile.TemporaryDirectory() as temporary:
            run_root = Path(temporary) / "run"

            def fail_after_commit():
                nonlocal validations, attempt_manifest_bytes
                validations += 1
                if validations == 2:
                    attempt_manifest_bytes = (
                        run_root / "typed-manifest.json"
                    ).read_bytes()
                    raise ValueError("accepted response changed")

            def swap_at_detach(
                source_name,
                destination_name,
                *,
                source_directory_fd,
                destination_directory_fd,
            ):
                nonlocal swapped
                if (
                    not swapped
                    and source_name == "typed"
                    and destination_name.startswith(".typed-quarantine-")
                ):
                    swapped = True
                    real_rename(
                        "typed",
                        ".rescued-attempt-typed",
                        source_directory_fd=source_directory_fd,
                        destination_directory_fd=destination_directory_fd,
                    )
                    os.mkdir(
                        "typed", mode=0o700, dir_fd=source_directory_fd
                    )
                    typed_descriptor = os.open(
                        "typed",
                        os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
                        dir_fd=source_directory_fd,
                    )
                    try:
                        foreign_descriptor = os.open(
                            "valuable.bin",
                            os.O_WRONLY | os.O_CREAT | os.O_EXCL,
                            0o600,
                            dir_fd=typed_descriptor,
                        )
                        try:
                            os.write(foreign_descriptor, foreign_bytes)
                            os.fsync(foreign_descriptor)
                        finally:
                            os.close(foreign_descriptor)
                    finally:
                        os.close(typed_descriptor)
                return real_rename(
                    source_name,
                    destination_name,
                    source_directory_fd=source_directory_fd,
                    destination_directory_fd=destination_directory_fd,
                )

            with patch(
                "scripts.collect_route_cohort._rename_directory_entry",
                side_effect=swap_at_detach,
            ), self.assertRaisesRegex(
                ValueError, "quarantine|detach|changed|unsafe"
            ):
                publish_typed_source_manifest(
                    run_root,
                    raw_evidence_run_id="detach-boundary-swap",
                    members=[member],
                    source_validator=fail_after_commit,
                )

            self.assertTrue(swapped)
            self.assertFalse((run_root / "typed-manifest.json").exists())
            self.assertEqual(
                (run_root / "typed/valuable.bin").read_bytes(),
                foreign_bytes,
            )
            self.assertEqual(
                (
                    run_root
                    / ".rescued-attempt-typed"
                    / "0000-cex_raw_book_response.json"
                ).read_bytes(),
                member_bytes,
            )
            self.assertIsNotNone(attempt_manifest_bytes)
            self.assertTrue(any(
                path.is_file()
                and path.read_bytes() == attempt_manifest_bytes
                for path in run_root.glob(
                    ".typed-quarantine-*-typed-manifest.json"
                )
            ))

    def test_success_never_returns_after_run_root_path_is_replaced(self):
        member_bytes = b'{"book":1}'
        member = {
            "market_id": "cex:okx:UNI/USDT",
            "role": "cex_raw_book_response",
            "payload": member_bytes,
            "logical_generation": "a" * 64,
            "adapter_id": "fetch_cex_depth/parse_book/v1",
            "content_schema": "route_bytes/v1",
        }
        validations = 0

        with tempfile.TemporaryDirectory() as temporary:
            parent = Path(temporary)
            run_root = parent / "run"
            moved_root = parent / "moved-run"

            def replace_root_after_commit():
                nonlocal validations
                validations += 1
                if validations == 2:
                    run_root.rename(moved_root)
                    run_root.mkdir()

            with self.assertRaisesRegex(
                ValueError, "root|path|identity|unsafe|quarantine"
            ):
                publish_typed_source_manifest(
                    run_root,
                    raw_evidence_run_id="replaced-return-root",
                    members=[member],
                    source_validator=replace_root_after_commit,
                )

            self.assertEqual(validations, 2)
            self.assertFalse((run_root / "typed-manifest.json").exists())
            self.assertFalse((run_root / "typed").exists())
            self.assertTrue(any(
                path.is_file() and path.read_bytes() == member_bytes
                for path in moved_root.rglob("*")
            ))

    def test_foreign_equal_bytes_manifest_is_never_restored_as_commit_marker(self):
        member_bytes = b'{"book":1}'
        member = {
            "market_id": "cex:okx:UNI/USDT",
            "role": "cex_raw_book_response",
            "payload": member_bytes,
            "logical_generation": "a" * 64,
            "adapter_id": "fetch_cex_depth/parse_book/v1",
            "content_schema": "route_bytes/v1",
        }
        validations = 0
        manifest_bytes = None

        with tempfile.TemporaryDirectory() as temporary:
            run_root = Path(temporary) / "run"

            def replace_publication_with_equal_bytes():
                nonlocal validations, manifest_bytes
                validations += 1
                if validations != 2:
                    return
                manifest_path = run_root / "typed-manifest.json"
                manifest_bytes = manifest_path.read_bytes()
                manifest_path.rename(
                    run_root / ".rescued-attempt-manifest.json"
                )
                (run_root / "typed").rename(
                    run_root / ".rescued-attempt-typed"
                )
                (run_root / "typed").mkdir()
                (
                    run_root / "typed/0000-cex_raw_book_response.json"
                ).write_bytes(member_bytes)
                manifest_path.write_bytes(manifest_bytes)
                raise ValueError("accepted response changed")

            with self.assertRaisesRegex(
                ValueError, "quarantine|foreign|changed|unsafe"
            ):
                publish_typed_source_manifest(
                    run_root,
                    raw_evidence_run_id="foreign-equal-bytes",
                    members=[member],
                    source_validator=replace_publication_with_equal_bytes,
                )

            self.assertIsNotNone(manifest_bytes)
            self.assertFalse((run_root / "typed-manifest.json").exists())
            self.assertEqual(
                (
                    run_root
                    / ".rescued-attempt-typed"
                    / "0000-cex_raw_book_response.json"
                ).read_bytes(),
                member_bytes,
            )
            self.assertEqual(
                (run_root / ".rescued-attempt-manifest.json").read_bytes(),
                manifest_bytes,
            )
            foreign_manifests = list(run_root.glob(
                ".typed-quarantine-*-typed-manifest.json"
            ))
            self.assertEqual(len(foreign_manifests), 1)
            self.assertEqual(
                foreign_manifests[0].read_bytes(), manifest_bytes
            )
            self.assertEqual(
                (
                    run_root / "typed/0000-cex_raw_book_response.json"
                ).read_bytes(),
                member_bytes,
            )

    def test_duplicate_role_market_and_postinstall_replacement_fail_closed(self):
        member = {
            "market_id": "dex:eth:uniswap_v2:0x1111111111111111111111111111111111111111:UNI",
            "role": "dex_pool_state",
            "payload": b'{"pool":1}',
            "logical_generation": "d" * 64,
            "adapter_id": "route_quantity_quote_for_v2_pool/v1",
            "content_schema": "route_v2_pool_state/v1",
        }
        with tempfile.TemporaryDirectory() as temporary:
            with self.assertRaisesRegex(ValueError, "duplicate|unique"):
                publish_typed_source_manifest(
                    Path(temporary), raw_evidence_run_id="run-1",
                    members=[member, member],
                )


class CompleteFinalizationOrchestrationTest(unittest.TestCase):
    def test_finalization_forwards_existing_artifacts_without_recollecting(self):
        data_dir = Path("/tmp/task7-existing-data")
        inputs = [{"already": "classified"}]
        expected = {"schema": "route_opportunity_pointer/v1"}
        with patch(
            "scripts.collect_route_cohort.publish_complete_route_bundle",
            return_value=expected,
        ) as publisher, patch(
            "scripts.collect_route_cohort.collect_route_cohort"
        ) as collector:
            actual = finalize_route_opportunity_bundle(
                data_dir=data_dir,
                opportunity_inputs=inputs,
                source_root=Path("/tmp/task7-sources"),
                fee_profile_path=Path("/private/tmp/task7-fees.csv"),
                fee_profile_id="9" * 64,
                inventory_profile_path=Path("/private/tmp/task7-inventory.csv"),
            )

        self.assertEqual(actual, expected)
        collector.assert_not_called()
        publisher.assert_called_once_with(
            core_root=data_dir / "routes/core",
            routes_root=data_dir / "routes",
            raw_root=data_dir / "raw/route-cohort",
            opportunity_inputs=inputs,
            source_root=Path("/tmp/task7-sources"),
            fee_profile_path=Path("/private/tmp/task7-fees.csv"),
            fee_profile_id="9" * 64,
            inventory_profile_path=Path("/private/tmp/task7-inventory.csv"),
        )


def _complete_test_routes(universe):
    normalized = dict(universe)
    route_volume_by_market = {}
    for route in universe.get("routes", []):
        for side in ("buy", "sell"):
            market_id = route.get(side + "_market_id")
            volume = route.get(side + "_reference_volume_usd")
            if isinstance(market_id, str) and volume is not None:
                route_volume_by_market.setdefault(market_id, volume)
    normalized["selected_legs"] = []
    for source in universe.get("selected_legs", []):
        leg = dict(source)
        market_id = leg.get("market_id")
        reference_volume = route_volume_by_market.get(market_id, "10000")
        inputs = dict(leg.get("selection_inputs") or {})
        if leg.get("market_type") == "dex":
            inputs.setdefault("dex_24h_usd", reference_volume)
            inputs.setdefault("cex_selected_window_usd", None)
        else:
            inputs.setdefault("cex_selected_window_usd", reference_volume)
            inputs.setdefault("dex_24h_usd", None)
        leg["selection_inputs"] = inputs
        normalized["selected_legs"].append(leg)
    selected_by_id = {
        leg.get("market_id"): leg for leg in normalized["selected_legs"]
    }
    normalized["routes"] = []
    for source in universe.get("routes", []):
        route = dict(source)
        if not route.get("route_id") and all(
            isinstance(route.get(key), str)
            for key in (
                "token_symbol", "buy_market_id", "sell_market_id", "route_mode"
            )
        ):
            route["route_id"] = "route:{}:{}->{}:{}".format(
                route["token_symbol"],
                route["buy_market_id"],
                route["sell_market_id"],
                route["route_mode"],
            )
        buy_leg = selected_by_id.get(route.get("buy_market_id"), {})
        sell_leg = selected_by_id.get(route.get("sell_market_id"), {})
        buy_inputs = buy_leg.get("selection_inputs", {})
        sell_inputs = sell_leg.get("selection_inputs", {})
        buy_field = (
            "dex_24h_usd"
            if buy_leg.get("market_type") == "dex"
            else "cex_selected_window_usd"
        )
        sell_field = (
            "dex_24h_usd"
            if sell_leg.get("market_type") == "dex"
            else "cex_selected_window_usd"
        )
        buy_volume = buy_inputs.get(buy_field)
        sell_volume = sell_inputs.get(sell_field)
        route.setdefault("buy_reference_volume_usd", buy_volume)
        route.setdefault("sell_reference_volume_usd", sell_volume)
        route.setdefault(
            "route_volume_usd",
            str(min(int(buy_volume), int(sell_volume)))
            if buy_volume is not None and sell_volume is not None
            else None,
        )
        route.setdefault(
            "route_volume_basis", "minimum_leg_source_horizon_usd"
        )
        normalized["routes"].append(route)
    return normalized


def _raw_writing_fake(collector):
    def wrapped(*args, **kwargs):
        value = collector(*args, **kwargs)
        row = value[0] if isinstance(value, tuple) and len(value) == 2 else value
        raw_path = kwargs.get("raw_path")
        if (
            isinstance(row, dict)
            and row.get("status") in {"observed", "partial"}
            and isinstance(raw_path, Path)
            and not raw_path.exists()
        ):
            raw_path.write_bytes(b"test raw evidence")
        return value

    return wrapped


def collect_route_cohort(universe, *args, **kwargs):
    """Keep stateful unit fakes in-process and give every test isolated raw."""
    kwargs.setdefault("raw_root", Path(_TEST_RAW_DIRECTORY.name))
    kwargs.setdefault("executor_factory", ThreadPoolExecutor)
    if "cex_collector" in kwargs:
        kwargs["cex_collector"] = _raw_writing_fake(kwargs["cex_collector"])
    if "dex_collector" in kwargs:
        kwargs["dex_collector"] = _raw_writing_fake(kwargs["dex_collector"])
    return _collect_route_cohort(_complete_test_routes(universe), *args, **kwargs)


def _strict_route(token, buy_market_id, sell_market_id, route_mode):
    return {
        "route_id": "route:{}:{}->{}:{}".format(
            token, buy_market_id, sell_market_id, route_mode
        ),
        "token_symbol": token,
        "buy_market_id": buy_market_id,
        "sell_market_id": sell_market_id,
        "route_mode": route_mode,
    }


def _strict_cex_universe():
    alpha = "cex:alpha:UNI/USDT"
    beta = "cex:beta:UNI/USDT"
    return {
        "candidate_source_generation": "generation-a",
        "selected_legs": [
            {"market_id": alpha, "market_type": "cex", "exchange": "alpha"},
            {"market_id": beta, "market_type": "cex", "exchange": "beta"},
        ],
        "routes": [
            _strict_route("UNI", alpha, beta, "prepositioned_inventory")
        ],
    }


def _write_observed_raw(_leg, *, raw_path, **_kwargs):
    raw_path.write_bytes(b"observed raw")
    return {
        "status": "observed",
        "state_observed_at": "2026-08-01T12:00:00Z",
    }


def _public_cex_depth_row(
    observed_at="2026-08-01T12:00:02+00:00",
    response_received_at="2026-08-01T12:00:00+00:00",
    exchange="binance",
    snapshot_id="live-cex-test",
    leg=None,
):
    token_symbol = leg["token_symbol"] if leg is not None else "UNI"
    cex_symbol = leg["cex_symbol"] if leg is not None else "UNI/USDT"
    endpoint = {
        "binance": "https://data-api.binance.vision/api/v3/depth",
        "bybit": "https://api.bybit.com/v5/market/orderbook",
    }[exchange]
    raw = '{{"exchange":"{}"}}'.format(exchange).encode("ascii")
    return observed_row(
        {
            "token_symbol": token_symbol,
            "exchange": exchange,
            "cex_symbol": cex_symbol,
        },
        {
            "bids": [(Decimal("99.99"), Decimal("1000"))],
            "asks": [(Decimal("100.01"), Decimal("1000"))],
            "source_instrument": cex_symbol.replace("/", ""),
            "source_sequence": "123",
            "source_observed_at": observed_at,
            "source_endpoint": endpoint,
            "raw": raw,
            "source_quote_asset": "USDT",
            "quote_to_usd": Decimal("1"),
            "quote_conversion_method": "USDT=USD proxy",
            "quote_conversion_endpoint": "",
            "quote_conversion_response_sha256": "",
            "full_book_reported": True,
        },
        snapshot_id=snapshot_id,
        request_started_at="2026-08-01T11:59:59Z",
        response_received_at=response_received_at,
    )


class CexRouteStateTimestampProjectionTests(unittest.TestCase):
    def test_attachment_authority_binds_candidate_source_generation(self):
        market_id = "cex:okx:UNI/USDT"
        raw_sha256 = hashlib.sha256(b"book").hexdigest()
        authority = _attachment_authority_bytes(
            market_id=market_id,
            trusted_leg={
                "market_id": market_id,
                "market_type": "cex",
                "candidate_source_generation": "candidate-a",
            },
            collector_row={
                "market_id": market_id,
                "market_type": "cex",
                "status": "observed",
                "raw_response_sha256": raw_sha256,
            },
            accepted_raw_sha256=raw_sha256,
            collection_input_generation="collection-a",
            validated_specs=(),
        )

        with self.assertRaisesRegex(ValueError, "authority is invalid"):
            _validated_attachment_authority(
                authority,
                market_id=market_id,
                market_type="cex",
                accepted_raw_sha256=raw_sha256,
                candidate_source_generation="candidate-b",
                collection_input_generation="collection-a",
            )

    def test_real_cex_depth_uses_canonical_local_receive_time_for_route_state(self):
        market_id = "cex:binance:UNI/USDT"
        for status in ("observed", "partial"):
            with self.subTest(status=status):
                collector_row = _public_cex_depth_row()
                collector_row["status"] = status

                projected = _final_route_leg_projection(
                    {"market_id": market_id, "market_type": "cex"},
                    collector_row,
                    market_id=market_id,
                )

                self.assertNotIn("state_observed_at", collector_row)
                self.assertEqual(
                    projected["state_observed_at"],
                    "2026-08-01T12:00:00Z",
                )
                self.assertEqual(
                    projected["observed_at"],
                    "2026-08-01T12:00:02+00:00",
                )

    def test_real_observed_cex_depth_defaults_route_availability_to_true(self):
        market_id = "cex:binance:UNI/USDT"
        collector_row = _public_cex_depth_row()

        projected = _final_route_leg_projection(
            {"market_id": market_id, "market_type": "cex"},
            collector_row,
            market_id=market_id,
        )

        self.assertNotIn("available", collector_row)
        self.assertIs(projected["available"], True)

    def test_explicit_false_cex_availability_is_not_promoted(self):
        market_id = "cex:binance:UNI/USDT"
        collector_row = _public_cex_depth_row()
        collector_row["available"] = False

        projected = _final_route_leg_projection(
            {"market_id": market_id, "market_type": "cex"},
            collector_row,
            market_id=market_id,
        )

        self.assertIs(projected["available"], False)

    def test_non_observed_cex_rows_do_not_promote_route_availability(self):
        market_id = "cex:binance:UNI/USDT"
        for status in ("partial", "failed", "deadline_exceeded"):
            with self.subTest(status=status):
                collector_row = _public_cex_depth_row()
                collector_row["status"] = status

                projected = _final_route_leg_projection(
                    {"market_id": market_id, "market_type": "cex"},
                    collector_row,
                    market_id=market_id,
                )

                self.assertIsNot(projected.get("available"), True)

    def test_terminal_cex_rows_do_not_promote_book_timestamp(self):
        market_id = "cex:binance:UNI/USDT"
        for status in ("failed", "unavailable"):
            with self.subTest(status=status):
                collector_row = _public_cex_depth_row()
                collector_row["status"] = status

                projected = _final_route_leg_projection(
                    {"market_id": market_id, "market_type": "cex"},
                    collector_row,
                    market_id=market_id,
                )

                self.assertNotIn("state_observed_at", projected)

    def test_missing_cex_response_receive_time_is_not_promoted(self):
        market_id = "cex:binance:UNI/USDT"
        for response_received_at in (None, ""):
            with self.subTest(response_received_at=response_received_at):
                collector_row = _public_cex_depth_row()
                if response_received_at is None:
                    collector_row.pop("response_received_at")
                else:
                    collector_row["response_received_at"] = (
                        response_received_at
                    )

                projected = _final_route_leg_projection(
                    {"market_id": market_id, "market_type": "cex"},
                    collector_row,
                    market_id=market_id,
                )

                self.assertNotIn("state_observed_at", projected)

    def test_invalid_cex_response_receive_time_is_not_promoted(self):
        market_id = "cex:binance:UNI/USDT"
        collector_row = _public_cex_depth_row()
        collector_row["response_received_at"] = "not-a-timestamp"

        with self.assertRaisesRegex(
            ValueError, "CEX response_received_at is invalid"
        ):
            _final_route_leg_projection(
                {"market_id": market_id, "market_type": "cex"},
                collector_row,
                market_id=market_id,
            )

    def test_existing_route_state_timestamp_remains_authoritative(self):
        market_id = "cex:binance:UNI/USDT"
        collector_row = _public_cex_depth_row()
        collector_row["state_observed_at"] = "2026-08-01T12:00:00Z"

        projected = _final_route_leg_projection(
            {"market_id": market_id, "market_type": "cex"},
            collector_row,
            market_id=market_id,
        )

        self.assertEqual(
            projected["state_observed_at"], "2026-08-01T12:00:00Z"
        )

    def test_invalid_explicit_route_state_timestamp_fails_closed(self):
        market_id = "cex:binance:UNI/USDT"
        collector_row = _public_cex_depth_row()
        collector_row["state_observed_at"] = "not-a-timestamp"

        with self.assertRaisesRegex(
            ValueError, "CEX state_observed_at is invalid"
        ):
            _final_route_leg_projection(
                {"market_id": market_id, "market_type": "cex"},
                collector_row,
                market_id=market_id,
            )

    def test_canonical_receive_time_reaches_core_publisher_validation(self):
        universe = build_live_cex_research_universe()
        generation = universe["candidate_source_generation"]
        wall_times = iter([
            datetime(2026, 8, 1, 11, 59, 59, tzinfo=timezone.utc),
            datetime(2026, 8, 1, 12, 0, 1, tzinfo=timezone.utc),
        ])

        def collect(leg, *, raw_path, snapshot_id, **_kwargs):
            exchange = leg["exchange"]
            row = _public_cex_depth_row(
                observed_at="2026-08-01T12:00:00+00:00",
                response_received_at="2026-08-01T12:00:00+00:00",
                exchange=exchange,
                snapshot_id=snapshot_id,
                leg=leg,
            )
            raw_path.write_bytes(
                '{{"exchange":"{}"}}'.format(exchange).encode("ascii")
            )
            return row, []

        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            cohort = _collect_route_cohort(
                universe,
                cex_collector=collect,
                dex_collector=lambda *_args, **_kwargs: None,
                raw_root=root / "raw",
                executor_factory=ThreadPoolExecutor,
                wall_clock=lambda: next(wall_times),
                source_generation_reader=lambda: generation,
                expected_source_generation=generation,
            )
            pointer = publish_route_cohort_bundle(
                cohort, core_root=root / "core"
            )

        self.assertTrue(all(
            row["state_observed_at"] == "2026-08-01T12:00:00Z"
            for row in cohort["legs"]
        ))
        self.assertEqual(pointer["route_cohort_id"], cohort["route_cohort_id"])

    def test_realistic_public_cex_chain_publishes_and_cold_reloads(self):
        universe = build_live_cex_research_universe()
        generation = universe["candidate_source_generation"]
        binance_book = json.dumps({
            "lastUpdateId": 1001,
            "bids": [
                ["100", "1000000"], ["99.9", "1000000"],
                ["99", "1000000"], ["98", "1000000"],
            ],
            "asks": [
                ["100.1", "1000000"], ["100.2", "1000000"],
                ["101.2", "1000000"], ["102", "1000000"],
            ],
        }, sort_keys=True, separators=(",", ":")).encode("utf-8")
        bybit_books = {
            instrument: json.dumps({
                "retCode": 0,
                "result": {
                    "s": instrument,
                    "u": 2002,
                    "cts": 1788518846632,
                    "b": [
                        ["100.2", "1000000"], ["100.1", "1000000"],
                        ["99.2", "1000000"], ["98", "1000000"],
                    ],
                    "a": [
                        ["100.3", "1000000"], ["100.4", "1000000"],
                        ["101.4", "1000000"], ["102", "1000000"],
                    ],
                },
            }, sort_keys=True, separators=(",", ":")).encode("utf-8")
            for instrument in ("UNIUSDT", "CAKEUSDT")
        }
        binance_rules = json.dumps({
            "symbols": [{
                "symbol": token_symbol + "USDT",
                "status": "TRADING",
                "baseAsset": token_symbol,
                "quoteAsset": "USDT",
                "baseAssetPrecision": 8,
                "quoteAssetPrecision": 8,
                "filters": [
                    {"filterType": "PRICE_FILTER", "tickSize": "0.0001"},
                    {
                        "filterType": "LOT_SIZE",
                        "minQty": "0.0001",
                        "stepSize": "0.0001",
                    },
                    {"filterType": "MIN_NOTIONAL", "minNotional": "1"},
                ],
            } for token_symbol in ("UNI", "CAKE")],
        }, sort_keys=True, separators=(",", ":")).encode("utf-8")
        bybit_rules = json.dumps({
            "retCode": 0,
            "result": {
                "category": "spot",
                "list": [{
                    "symbol": token_symbol + "USDT",
                    "status": "Trading",
                    "baseCoin": token_symbol,
                    "quoteCoin": "USDT",
                    "lotSizeFilter": {
                        "basePrecision": "0.0001",
                        "quotePrecision": "0.0001",
                        "minOrderQty": "0.0001",
                        "minOrderAmt": "1",
                    },
                    "priceFilter": {"tickSize": "0.0001"},
                } for token_symbol in ("UNI", "CAKE")],
            },
        }, sort_keys=True, separators=(",", ":")).encode("utf-8")

        def request(url, **_kwargs):
            if "data-api.binance.vision" in url:
                raw = binance_book
            elif "api.binance.com" in url:
                raw = binance_rules
            elif "/v5/market/orderbook" in url:
                instrument = (
                    "CAKEUSDT" if "symbol=CAKEUSDT" in url else "UNIUSDT"
                )
                raw = bybit_books[instrument]
            elif "/v5/market/instruments-info" in url:
                raw = bybit_rules
            else:
                raise AssertionError("unexpected CEX request: " + url)
            return json.loads(raw.decode("utf-8")), raw

        def collect(leg, *, typed_source_payload_sink, **kwargs):
            return collect_cex_market_observation(
                dict(leg),
                request=request,
                typed_source_payload_sink=typed_source_payload_sink,
                **kwargs
            )

        wall_times = iter([
            datetime(2026, 9, 4, 10, 47, 24, 203353,
                     tzinfo=timezone.utc),
            datetime(2026, 9, 4, 10, 47, 25, tzinfo=timezone.utc),
        ])
        with tempfile.TemporaryDirectory() as directory_name, patch(
            "scripts.fetch_cex_depth.utc_now_text",
            return_value="2026-09-04T10:47:24.681982+00:00",
        ):
            root = Path(directory_name)
            raw_root = root / "raw/route-cohort"
            cohort = _collect_route_cohort(
                universe,
                cex_collector=collect,
                dex_collector=lambda *_args, **_kwargs: None,
                raw_root=raw_root,
                executor_factory=ThreadPoolExecutor,
                wall_clock=lambda: next(wall_times),
                source_generation_reader=lambda: generation,
                expected_source_generation=generation,
            )
            cohort, _typed = attach_typed_source_lineage(
                cohort, raw_root=raw_root
            )
            self.assertEqual(
                {leg["market_id"] for leg in cohort["legs"]},
                {
                    "cex:binance:UNI/USDT",
                    "cex:bybit:UNI/USDT",
                    "cex:binance:CAKE/USDT",
                    "cex:bybit:CAKE/USDT",
                },
            )
            self.assertTrue(all(
                {
                    member["role"]
                    for member in leg["typed_source_lineage"]["members"]
                    if member["status"] == "observed"
                } == {
                    "cex_market_rules",
                    "cex_raw_book_response",
                    "quote_usd_conversion",
                }
                for leg in cohort["legs"]
            ), msg=json.dumps(cohort["legs"], sort_keys=True))
            legacy_sources, _legacy_legs = _load_cex_sources(
                root=root,
                cohort=cohort,
                source_root=raw_root / cohort["raw_evidence_run_id"] / "typed",
                now=cohort["collection_completed_at"],
            )
            self.assertEqual(set(legacy_sources), {
                "cex:binance:UNI/USDT",
                "cex:bybit:UNI/USDT",
                "cex:binance:CAKE/USDT",
                "cex:bybit:CAKE/USDT",
            })
            core_pointer = publish_route_cohort_bundle(
                cohort, core_root=root / "routes/core"
            )
            schedule = root / "public-fees.csv"
            schedule.write_bytes(
                (Path(__file__).resolve().parents[1]
                 / "config/cex_public_fee_schedules.csv").read_bytes()
            )
            pointer = finalize_public_cex_research_opportunities(
                data_dir=root,
                public_fee_schedule_path=schedule,
                expected_route_cohort_id=core_pointer["route_cohort_id"],
                expected_core_manifest_sha256=core_pointer[
                    "manifest_sha256"
                ],
            )
            loaded = load_latest_complete_route_bundle(
                root / "routes", core_root=root / "routes/core"
            )

        self.assertEqual(loaded["pointer"], pointer)
        self.assertEqual(len(loaded["bundle"]["opportunities"]), 20)
        self.assertEqual(
            {
                token: sum(
                    row["token_symbol"] == token
                    for row in loaded["bundle"]["opportunities"]
                )
                for token in ("UNI", "CAKE")
            },
            {"UNI": 10, "CAKE": 10},
        )
        covered = [
            row for row in loaded["bundle"]["opportunities"]
            if row["requested_notional_usd"] == "1000"
        ]
        self.assertEqual(len(covered), 4)
        for row in covered:
            self.assertEqual(
                row["opportunity_class"],
                "research_estimate"
                if row["token_symbol"] == "UNI" else "unavailable",
            )
            for field in (
                "gross_buy_cost_usd",
                "gross_sell_proceeds_usd",
                "gross_edge_usd",
                "research_net_edge_usd",
                "research_net_edge_bps",
            ):
                if row["token_symbol"] == "UNI":
                    self.assertIsNotNone(row[field])
                else:
                    self.assertIsNone(row[field])
        self.assertTrue(all(
            row["strict_eligible"] is False
            for row in loaded["bundle"]["opportunities"]
        ))

    def test_dex_projection_does_not_derive_state_from_observed_at(self):
        market_id = "dex:eth:uniswap_v2:0x" + "1" * 40 + ":UNI"

        projected = _final_route_leg_projection(
            {"market_id": market_id, "market_type": "dex"},
            {
                "status": "observed",
                "observed_at": "2026-08-01T12:00:00Z",
            },
            market_id=market_id,
        )

        self.assertNotIn("state_observed_at", projected)


class FakeClock:
    def __init__(self, now=0.0):
        self.now = now
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def read(self):
        return self.payload


class CollectionDeadlineTest(unittest.TestCase):
    def test_expired_deadline_uses_one_stable_exception_without_http_request(self):
        from scripts.collection_deadline import (
            CollectionDeadline,
            CollectionDeadlineExceeded,
        )
        from scripts.fetch_cex_depth import request_json
        from scripts.fetch_dex_depth import http_json_rpc

        clock = FakeClock()
        deadline = CollectionDeadline.for_duration(
            0,
            clock=clock.monotonic,
            sleeper=clock.sleep,
        )
        with patch("urllib.request.urlopen") as urlopen:
            with self.assertRaises(CollectionDeadlineExceeded) as cex_error:
                request_json("https://example.test/book", deadline=deadline)
            with self.assertRaises(CollectionDeadlineExceeded) as dex_error:
                http_json_rpc(
                    "https://example.test/rpc",
                    {"jsonrpc": "2.0", "id": 1, "method": "test", "params": []},
                    deadline=deadline,
                )

        urlopen.assert_not_called()
        self.assertEqual(str(cex_error.exception), "collection deadline exceeded")
        self.assertEqual(type(cex_error.exception), type(dex_error.exception))
        self.assertEqual(str(cex_error.exception), str(dex_error.exception))

    def test_request_timeout_never_exceeds_remaining_deadline(self):
        from scripts.collection_deadline import CollectionDeadline
        from scripts.fetch_cex_depth import request_json

        clock = FakeClock()
        deadline = CollectionDeadline.for_duration(
            2.5,
            clock=clock.monotonic,
            sleeper=clock.sleep,
        )
        raw = json.dumps({"ok": True}).encode("utf-8")
        with patch(
            "scripts.fetch_cex_depth.open_public_json_request",
            return_value=FakeResponse(raw),
        ) as open_public_json_request:
            payload, returned_raw = request_json(
                "https://example.test/book",
                deadline=deadline,
                timeout_seconds=30,
                max_retries=1,
            )

        self.assertEqual(payload, {"ok": True})
        self.assertEqual(returned_raw, raw)
        self.assertEqual(
            open_public_json_request.call_args.kwargs["timeout"], 2.5
        )

    def test_retry_sleep_is_clamped_and_exhaustion_raises_stable_exception(self):
        from scripts.collection_deadline import (
            CollectionDeadline,
            CollectionDeadlineExceeded,
        )

        clock = FakeClock(now=10.0)
        deadline = CollectionDeadline.for_duration(
            1.25,
            clock=clock.monotonic,
            sleeper=clock.sleep,
        )
        with self.assertRaisesRegex(
            CollectionDeadlineExceeded,
            "^collection deadline exceeded$",
        ):
            deadline.sleep_before_retry(30)

        self.assertEqual(clock.sleeps, [1.25])
        self.assertEqual(deadline.remaining_seconds(), 0.0)

    def test_final_transport_failure_is_replaced_by_deadline_exhaustion(self):
        from scripts.collection_deadline import (
            CollectionDeadline,
            CollectionDeadlineExceeded,
        )
        from scripts.fetch_cex_depth import request_json
        from scripts.fetch_dex_depth import http_json_rpc

        for transport_target, request_call in (
            (
                "scripts.fetch_cex_depth.open_public_json_request",
                lambda deadline: request_json(
                    "https://example.test/book",
                    deadline=deadline,
                    max_retries=1,
                ),
            ),
            (
                "urllib.request.urlopen",
                lambda deadline: http_json_rpc(
                    "https://example.test/rpc",
                    {"jsonrpc": "2.0", "id": 1, "method": "test", "params": []},
                    deadline=deadline,
                    max_retries=1,
                ),
            ),
        ):
            clock = FakeClock()
            deadline = CollectionDeadline.for_duration(
                1,
                clock=clock.monotonic,
                sleeper=clock.sleep,
            )

            def expire_then_fail(*_args, **_kwargs):
                clock.now = 2
                raise urllib.error.URLError("transport timed out")

            with patch(transport_target, side_effect=expire_then_fail):
                with self.assertRaisesRegex(
                    CollectionDeadlineExceeded,
                    "^collection deadline exceeded$",
                ):
                    request_call(deadline)


class RpcClientIsolationTest(unittest.TestCase):
    def test_production_clients_start_at_one_and_keep_independent_records(self):
        from scripts.fetch_dex_depth import RpcClient

        def transport(_url, payload):
            response = {
                "jsonrpc": "2.0",
                "id": payload["id"],
                "result": "0x1",
            }
            return response, json.dumps(response, sort_keys=True).encode("utf-8")

        first = RpcClient("eth", "https://rpc.example.test", request=transport)
        second = RpcClient("eth", "https://rpc.example.test", request=transport)

        self.assertEqual(first.method("test_first", []), "0x1")
        self.assertEqual(second.method("test_second", []), "0x1")
        self.assertEqual(first.records[0]["request"]["id"], 1)
        self.assertEqual(second.records[0]["request"]["id"], 1)
        self.assertIsNot(first.records, second.records)

        self.assertEqual(first.method("test_first_again", []), "0x1")
        self.assertEqual(first.records[1]["request"]["id"], 2)
        self.assertEqual(len(second.records), 1)


class RouteLegCollectionTests(unittest.TestCase):
    def test_route_volume_lineage_is_bound_before_cli_or_direct_collection(self):
        alpha = "cex:alpha:UNI/USDT"
        beta = "cex:beta:UNI/USDT"
        route_id = "route:UNI:{}->{}:prepositioned_inventory".format(
            alpha, beta
        )
        forged = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [
                {
                    "market_id": alpha,
                    "market_type": "cex",
                    "selection_inputs": {
                        "cex_selected_window_usd": "100",
                        "dex_24h_usd": None,
                    },
                },
                {
                    "market_id": beta,
                    "market_type": "cex",
                    "selection_inputs": {
                        "cex_selected_window_usd": "200",
                        "dex_24h_usd": None,
                    },
                },
            ],
            "routes": [{
                "route_id": route_id,
                "token_symbol": "UNI",
                "buy_market_id": alpha,
                "sell_market_id": beta,
                "route_mode": "prepositioned_inventory",
                "buy_reference_volume_usd": "999999",
                "sell_reference_volume_usd": "999999",
                "route_volume_usd": "999999",
                "route_volume_basis": "minimum_leg_source_horizon_usd",
            }],
        }

        with self.assertRaisesRegex(ValueError, "route volume lineage"):
            _validated_universe(forged, None)

        with tempfile.TemporaryDirectory() as directory_name:
            with self.assertRaisesRegex(ValueError, "route volume lineage"):
                _collect_route_cohort(
                    forged,
                    cex_collector=_write_observed_raw,
                    source_generation_reader=lambda: "generation-a",
                    expected_source_generation="generation-a",
                    raw_root=Path(directory_name),
                    executor_factory=ThreadPoolExecutor,
                )

    def test_completion_time_rejects_future_observations_and_is_retained(self):
        universe = _strict_cex_universe()
        wall_times = iter([
            datetime(2026, 8, 1, 12, tzinfo=timezone.utc),
            datetime(2026, 8, 1, 12, 0, 1, tzinfo=timezone.utc),
        ])

        def future_observation(_leg, *, raw_path, **_kwargs):
            raw_path.write_bytes(b"future")
            return {
                "status": "observed",
                "state_observed_at": "2099-01-01T00:00:00Z",
            }

        with tempfile.TemporaryDirectory() as directory_name:
            result = _collect_route_cohort(
                universe,
                cex_collector=future_observation,
                dex_collector=lambda *_args, **_kwargs: None,
                raw_root=Path(directory_name),
                executor_factory=ThreadPoolExecutor,
                wall_clock=lambda: next(wall_times),
                source_generation_reader=lambda: "input-a",
                expected_source_generation="input-a",
            )

        self.assertEqual(
            result["collection_completed_at"], "2026-08-01T12:00:01Z"
        )
        route = result["route_rows"][0]
        self.assertEqual(route["validated_at"], result["collection_completed_at"])
        self.assertEqual(route["timing_status"], "unavailable")
        self.assertEqual(route["reason_code"], "invalid_state_timestamp")

    def test_collection_completion_time_is_bound_into_both_hashes(self):
        from scripts.collection_deadline import CollectionDeadline

        universe = _strict_cex_universe()
        start = datetime(2026, 8, 1, 12, tzinfo=timezone.utc)

        def collect_once(raw_root, completed_at):
            wall_times = iter([start, completed_at])
            monotonic = FakeClock()
            return _collect_route_cohort(
                universe,
                cex_collector=_write_observed_raw,
                dex_collector=lambda *_args, **_kwargs: None,
                raw_root=raw_root,
                snapshot_id="same-run",
                executor_factory=ThreadPoolExecutor,
                deadline=CollectionDeadline.for_duration(
                    1, clock=monotonic.monotonic, sleeper=monotonic.sleep
                ),
                wall_clock=lambda: next(wall_times),
                source_generation_reader=lambda: "input-a",
                expected_source_generation="input-a",
            )

        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            first = collect_once(
                root / "first",
                datetime(2026, 8, 1, 12, 0, 1, tzinfo=timezone.utc),
            )
            second = collect_once(
                root / "second",
                datetime(2026, 8, 1, 12, 0, 2, tzinfo=timezone.utc),
            )

        self.assertNotEqual(first["route_cohort_id"], second["route_cohort_id"])
        self.assertNotEqual(first["fingerprint"], second["fingerprint"])

    def test_exact_route_id_is_required_before_source_reads_or_raw_work(self):
        canonical = _strict_cex_universe()
        invalid_routes = [
            {key: value for key, value in canonical["routes"][0].items() if key != "route_id"},
            {**canonical["routes"][0], "route_id": ""},
            {**canonical["routes"][0], "route_id": "route:not-canonical"},
        ]
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            for index, route in enumerate(invalid_routes):
                source_reads = []
                raw_root = root / str(index)
                with self.subTest(route_id=route.get("route_id")):
                    with self.assertRaisesRegex(
                        ValueError, "route_id must be canonical"
                    ):
                        _collect_route_cohort(
                            {**canonical, "routes": [route]},
                            cex_collector=_write_observed_raw,
                            dex_collector=lambda *_args, **_kwargs: None,
                            raw_root=raw_root,
                            executor_factory=ThreadPoolExecutor,
                            source_generation_reader=lambda: (
                                source_reads.append("read") or "input-a"
                            ),
                            expected_source_generation="input-a",
                        )
                    self.assertEqual(source_reads, [])
                    self.assertFalse(raw_root.exists())

    def test_selected_market_type_must_match_market_id_before_source_read(self):
        universe = _strict_cex_universe()
        universe["selected_legs"][0]["market_type"] = "dex"
        source_reads = []
        with tempfile.TemporaryDirectory() as directory_name:
            raw_root = Path(directory_name) / "raw"
            with self.assertRaisesRegex(ValueError, "market type.*market_id"):
                _collect_route_cohort(
                    universe,
                    cex_collector=_write_observed_raw,
                    dex_collector=lambda *_args, **_kwargs: None,
                    dex_block_resolver=lambda *_args, **_kwargs: {
                        "block_number": 1,
                        "block_timestamp": "2026-08-01T11:59:59Z",
                    },
                    raw_root=raw_root,
                    executor_factory=ThreadPoolExecutor,
                    source_generation_reader=lambda: (
                        source_reads.append("read") or "input-a"
                    ),
                    expected_source_generation="input-a",
                )
            self.assertEqual(source_reads, [])
            self.assertFalse(raw_root.exists())

    def test_malformed_market_ids_fail_before_source_read_or_raw_creation(self):
        valid_cex = "cex:beta:UNI/USDT"
        invalid_ids = (
            "cex::UNI/USDT",
            "cex:alpha:UNI",
            "cex:alpha:/USDT",
            "cex:alpha:UNI/",
            "cex:alpha:UNI//USDT",
            "cex:alpha:UNI/US DT",
            "cex:alpha:../USDT",
            "dex::swap:0xpool:UNI",
            "dex:eth::0xpool:UNI",
            "dex:eth:swap::UNI",
            "dex:eth:swap:0xpool:",
            "dex:eth:swap:0xpool",
            "dex:eth:swap:..:UNI",
            "dex:eth:swap:0xpool:UNI/USDT",
            "dex:eth:swap:0xAbC:UNI",
        )
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            for index, market_id in enumerate(invalid_ids):
                source_reads = []
                raw_root = root / str(index)
                market_type = "dex" if market_id.startswith("dex:") else "cex"
                selected_leg = {
                    "market_id": market_id,
                    "market_type": market_type,
                }
                universe = {
                    "candidate_source_generation": "generation-a",
                    "selected_legs": [
                        selected_leg,
                        {
                            "market_id": valid_cex,
                            "market_type": "cex",
                        },
                    ],
                    "routes": [
                        _strict_route(
                            "UNI",
                            market_id,
                            valid_cex,
                            "prepositioned_inventory",
                        )
                    ],
                }
                with self.subTest(market_id=market_id):
                    with self.assertRaisesRegex(
                        ValueError, "route leg identity is invalid"
                    ):
                        _collect_route_cohort(
                            universe,
                            cex_collector=lambda *_args, **_kwargs: self.fail(
                                "invalid identity must not collect"
                            ),
                            dex_collector=lambda *_args, **_kwargs: self.fail(
                                "invalid identity must not collect"
                            ),
                            dex_block_resolver=lambda *_args, **_kwargs: self.fail(
                                "invalid identity must not resolve"
                            ),
                            raw_root=raw_root,
                            executor_factory=ThreadPoolExecutor,
                            source_generation_reader=lambda: (
                                source_reads.append("read") or "input-a"
                            ),
                            expected_source_generation="input-a",
                        )
                    self.assertEqual(source_reads, [])
                    self.assertFalse(raw_root.exists())

    def test_selected_identity_fields_must_match_canonical_market_id(self):
        left = "cex:alpha:UNI/USDT"
        right = "dex:eth:swap:0xpool:UNI"
        cases = (
            (left, "cex", {"exchange": "beta"}),
            (left, "cex", {"exchange": " alpha "}),
            (left, "cex", {"cex_symbol": "AAVE/USDT"}),
            (left, "cex", {"cex_symbol": " UNI/USDT "}),
            (left, "cex", {"token_symbol": "AAVE"}),
            (left, "cex", {"token_symbol": " UNI "}),
            (right, "dex", {"chain": "arb"}),
            (right, "dex", {"chain": " eth "}),
            (right, "dex", {"dex": "other"}),
            (right, "dex", {"dex": " swap "}),
            (right, "dex", {"pool_address": "0xother"}),
            (right, "dex", {"pool_address": " 0xpool "}),
            (right, "dex", {"token_symbol": "AAVE"}),
            (right, "dex", {"token_symbol": " UNI "}),
        )
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            for index, (market_id, market_type, conflicting) in enumerate(cases):
                source_reads = []
                raw_root = root / str(index)
                universe = {
                    "candidate_source_generation": "generation-a",
                    "selected_legs": [
                        {
                            "market_id": market_id,
                            "market_type": market_type,
                            **conflicting,
                        },
                        {
                            "market_id": "cex:beta:UNI/USDT",
                            "market_type": "cex",
                        },
                    ],
                    "routes": [
                        _strict_route(
                            "UNI",
                            market_id,
                            "cex:beta:UNI/USDT",
                            "prepositioned_inventory",
                        )
                    ],
                }
                with self.subTest(market_id=market_id, conflicting=conflicting):
                    with self.assertRaisesRegex(
                        ValueError, "route leg identity is invalid"
                    ):
                        _collect_route_cohort(
                            universe,
                            cex_collector=lambda *_args, **_kwargs: self.fail(
                                "identity conflict must not collect"
                            ),
                            dex_collector=lambda *_args, **_kwargs: self.fail(
                                "identity conflict must not collect"
                            ),
                            dex_block_resolver=lambda *_args, **_kwargs: self.fail(
                                "identity conflict must not resolve"
                            ),
                            raw_root=raw_root,
                            executor_factory=ThreadPoolExecutor,
                            source_generation_reader=lambda: (
                                source_reads.append("read") or "input-a"
                            ),
                            expected_source_generation="input-a",
                        )
                    self.assertEqual(source_reads, [])
                    self.assertFalse(raw_root.exists())

    def test_dex_pool_identity_accepts_publication_maximum_length(self):
        pool = "P" + ("a" * 255)
        dex_market = "dex:eth:swap:{}:UNI".format(pool)
        cex_market = "cex:beta:UNI/USDT"
        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [
                {
                    "market_id": dex_market,
                    "market_type": "dex",
                    "chain": "eth",
                    "dex": "swap",
                    "pool_address": pool,
                    "token_symbol": "UNI",
                },
                {"market_id": cex_market, "market_type": "cex"},
            ],
            "routes": [
                _strict_route(
                    "UNI", dex_market, cex_market, "prepositioned_inventory"
                )
            ],
        }

        def dex_observation(_leg, *, raw_path, fixed_block_number,
                            fixed_block_timestamp, **_kwargs):
            raw_path.write_bytes(b"long pool raw")
            return {
                "status": "observed",
                "state_observed_at": "2026-08-01T12:00:00Z",
                "block_number": str(fixed_block_number),
                "block_timestamp": fixed_block_timestamp,
            }

        with tempfile.TemporaryDirectory() as directory_name:
            try:
                result = _collect_route_cohort(
                    universe,
                    cex_collector=_write_observed_raw,
                    dex_collector=dex_observation,
                    dex_block_resolver=lambda *_args, **_kwargs: {
                        "block_number": 123,
                        "block_timestamp": "2026-08-01T12:00:00Z",
                    },
                    raw_root=Path(directory_name),
                    executor_factory=ThreadPoolExecutor,
                    source_generation_reader=lambda: "input-a",
                    expected_source_generation="input-a",
                )
            except ValueError as error:
                self.fail("maximum-length canonical pool was rejected: {}".format(error))

        self.assertTrue(all(row["status"] == "observed" for row in result["legs"]))

    def test_fixed_block_lineage_is_strict_and_future_safe(self):
        left = "dex:eth:swap:0xone:UNI"
        right = "dex:eth:swap:0xtwo:UNI"
        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [
                {"market_id": left, "market_type": "dex", "chain": "eth"},
                {"market_id": right, "market_type": "dex", "chain": "eth"},
            ],
            "routes": [_strict_route("UNI", left, right, "atomic_onchain")],
        }
        invalid = [
            {"block_number": 0, "block_timestamp": "2026-08-01T11:59:59Z"},
            {"block_number": -1, "block_timestamp": "2026-08-01T11:59:59Z"},
            {"block_number": True, "block_timestamp": "2026-08-01T11:59:59Z"},
            {"block_number": 1, "block_timestamp": ""},
            {"block_number": 1, "block_timestamp": "not-a-time"},
            {"block_number": 1, "block_timestamp": "2099-01-01T00:00:00Z"},
        ]

        def dex_observation(_leg, *, raw_path, fixed_block_number,
                            fixed_block_timestamp, **_kwargs):
            raw_path.write_bytes(b"dex raw")
            return {
                "status": "observed",
                "state_observed_at": "2026-08-01T12:00:00Z",
                "block_number": str(fixed_block_number),
                "block_timestamp": fixed_block_timestamp,
            }

        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            for index, lineage in enumerate(invalid):
                calls = []
                with self.subTest(lineage=lineage):
                    result = _collect_route_cohort(
                        universe,
                        cex_collector=lambda *_args, **_kwargs: None,
                        dex_collector=lambda *args, **kwargs: (
                            calls.append("called")
                            or dex_observation(*args, **kwargs)
                        ),
                        dex_block_resolver=lambda *_args, value=lineage, **_kwargs: value,
                        raw_root=root / str(index),
                        executor_factory=ThreadPoolExecutor,
                        wall_clock=lambda: datetime(
                            2026, 8, 1, 12, tzinfo=timezone.utc
                        ),
                        source_generation_reader=lambda: "input-a",
                        expected_source_generation="input-a",
                    )
                    self.assertEqual(calls, [])
                    self.assertTrue(all(
                        row["reason_code"] == "fixed_block_unavailable"
                        for row in result["legs"]
                    ))

    def test_terminal_dex_leg_retains_normalized_resolved_block_lineage(self):
        left = "dex:eth:swap:0xone:UNI"
        right = "dex:eth:swap:0xtwo:UNI"
        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [
                {"market_id": left, "market_type": "dex", "chain": "eth"},
                {"market_id": right, "market_type": "dex", "chain": "eth"},
            ],
            "routes": [_strict_route("UNI", left, right, "atomic_onchain")],
        }
        with tempfile.TemporaryDirectory() as directory_name:
            result = _collect_route_cohort(
                universe,
                cex_collector=lambda *_args, **_kwargs: None,
                dex_collector=lambda *_args, **_kwargs: (_ for _ in ()).throw(
                    ValueError("collection failed")
                ),
                dex_block_resolver=lambda *_args, **_kwargs: {
                    "block_number": 123,
                    "block_timestamp": "2026-08-01T20:00:00+08:00",
                },
                raw_root=Path(directory_name),
                executor_factory=ThreadPoolExecutor,
                wall_clock=lambda: datetime(
                    2026, 8, 1, 12, 0, 1, tzinfo=timezone.utc
                ),
                source_generation_reader=lambda: "input-a",
                expected_source_generation="input-a",
            )

        self.assertTrue(all(row["status"] == "failed" for row in result["legs"]))
        self.assertTrue(all(
            row["fixed_block_number"] == "123"
            and row["fixed_block_timestamp"] == "2026-08-01T12:00:00Z"
            for row in result["legs"]
        ))

    def test_hung_resolvers_cannot_consume_reserved_cex_capacity(self):
        cex_one = "cex:alpha:UNI/USDT"
        cex_two = "cex:alpha:UNI/USDC"
        dex_one = "dex:arb:swap:0xone:UNI"
        dex_two = "dex:eth:swap:0xtwo:UNI"
        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [
                {"market_id": cex_one, "market_type": "cex", "exchange": "alpha"},
                {"market_id": cex_two, "market_type": "cex", "exchange": "alpha"},
                {"market_id": dex_one, "market_type": "dex", "chain": "arb"},
                {"market_id": dex_two, "market_type": "dex", "chain": "eth"},
            ],
            "routes": [
                _strict_route(
                    "UNI", cex_one, cex_two, "prepositioned_inventory"
                ),
                _strict_route("UNI", dex_one, dex_two, "research_only"),
            ],
        }
        gates = {"arb": Event(), "eth": Event()}
        started = []

        def hung_resolver(chain, **_kwargs):
            gates[chain].wait()

        def cex_observation(leg, **_kwargs):
            started.append(leg["market_id"])
            return {
                "status": "observed",
                "state_observed_at": "2026-08-01T12:00:00Z",
            }

        result = collect_route_cohort(
            universe,
            cex_collector=cex_observation,
            dex_collector=lambda *_args, **_kwargs: None,
            dex_block_resolver=hung_resolver,
            max_workers=2,
            cex_workers_per_venue=1,
            deadline_seconds=0.15,
            source_generation_reader=lambda: "input-a",
            expected_source_generation="input-a",
        )
        for gate in gates.values():
            gate.set()

        self.assertEqual(len(started), 2)
        self.assertEqual(set(started), {cex_one, cex_two})
        self.assertTrue(all(
            row["status"] == "observed"
            for row in result["legs"] if row["market_id"].startswith("cex:")
        ))

    def test_one_worker_finishes_all_cex_work_before_any_resolver(self):
        cex_one = "cex:alpha:UNI/USDT"
        cex_two = "cex:alpha:UNI/USDC"
        dex_one = "dex:eth:swap:0xone:UNI"
        dex_two = "dex:eth:swap:0xtwo:UNI"
        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [
                {"market_id": cex_one, "market_type": "cex", "exchange": "alpha"},
                {"market_id": cex_two, "market_type": "cex", "exchange": "alpha"},
                {"market_id": dex_one, "market_type": "dex", "chain": "eth"},
                {"market_id": dex_two, "market_type": "dex", "chain": "eth"},
            ],
            "routes": [
                _strict_route(
                    "UNI", cex_one, cex_two, "prepositioned_inventory"
                ),
                _strict_route("UNI", dex_one, dex_two, "atomic_onchain"),
            ],
        }
        gate = Event()
        starts = []

        def cex_observation(leg, **_kwargs):
            starts.append(leg["market_id"])
            return {
                "status": "observed",
                "state_observed_at": "2026-08-01T12:00:00Z",
            }

        result = collect_route_cohort(
            universe,
            cex_collector=cex_observation,
            dex_collector=lambda *_args, **_kwargs: None,
            dex_block_resolver=lambda *_args, **_kwargs: gate.wait(),
            max_workers=1,
            cex_workers_per_venue=1,
            deadline_seconds=0.05,
            source_generation_reader=lambda: "input-a",
            expected_source_generation="input-a",
        )
        gate.set()

        self.assertEqual(len(starts), 2)
        self.assertEqual(set(starts), {cex_one, cex_two})
        self.assertTrue(all(
            row["status"] == "observed"
            for row in result["legs"] if row["market_id"].startswith("cex:")
        ))

    def test_repeated_blocked_default_calls_leave_no_workers_or_processes(self):
        universe = _strict_cex_universe()
        baseline_processes = {process.pid for process in multiprocessing.active_children()}
        baseline_threads = {
            thread.ident
            for thread in enumerate_threads()
            if thread.name.startswith("route-cohort-")
        }
        with tempfile.TemporaryDirectory() as directory_name:
            raw_root = Path(directory_name)
            for _index in range(3):
                gate = multiprocessing.get_context("fork").Event()

                def blocked_collector(*_args, **_kwargs):
                    gate.wait()

                result = _collect_route_cohort(
                    universe,
                    cex_collector=blocked_collector,
                    dex_collector=lambda *_args, **_kwargs: None,
                    raw_root=raw_root,
                    max_workers=1,
                    deadline_seconds=0.03,
                    source_generation_reader=lambda: "input-a",
                    expected_source_generation="input-a",
                )
                self.assertTrue(all(
                    row["status"] == "deadline_exceeded"
                    for row in result["legs"]
                ))

        time.sleep(0.05)
        self.assertEqual(
            {process.pid for process in multiprocessing.active_children()},
            baseline_processes,
        )
        self.assertEqual(
            {
                thread.ident
                for thread in enumerate_threads()
                if thread.name.startswith("route-cohort-")
            },
            baseline_threads,
        )

    def test_default_process_executor_creates_no_monitor_threads(self):
        universe = _strict_cex_universe()
        with tempfile.TemporaryDirectory() as directory_name:
            with patch(
                "scripts.collect_route_cohort.Thread",
                side_effect=AssertionError("process executor created a thread"),
            ):
                result = _collect_route_cohort(
                    universe,
                    cex_collector=_write_observed_raw,
                    dex_collector=lambda *_args, **_kwargs: None,
                    raw_root=Path(directory_name),
                    source_generation_reader=lambda: "input-a",
                    expected_source_generation="input-a",
                )

        self.assertTrue(all(row["status"] == "observed" for row in result["legs"]))

    def test_multithreaded_caller_fails_before_source_read_raw_or_fork(self):
        universe = _strict_cex_universe()
        gate = Event()
        caller_thread = TestThread(
            target=gate.wait,
            name="unrelated-caller-thread",
        )
        caller_thread.start()
        source_reads = []
        baseline_processes = {
            process.pid for process in multiprocessing.active_children()
        }
        try:
            with tempfile.TemporaryDirectory() as directory_name:
                raw_root = Path(directory_name) / "raw"
                with self.assertRaisesRegex(RuntimeError, "single-threaded"):
                    _collect_route_cohort(
                        universe,
                        cex_collector=_write_observed_raw,
                        dex_collector=lambda *_args, **_kwargs: None,
                        raw_root=raw_root,
                        source_generation_reader=lambda: (
                            source_reads.append("read") or "input-a"
                        ),
                        expected_source_generation="input-a",
                    )
                self.assertFalse(raw_root.exists())
        finally:
            gate.set()
            caller_thread.join(timeout=1)

        self.assertEqual(source_reads, [])
        self.assertEqual(
            {process.pid for process in multiprocessing.active_children()},
            baseline_processes,
        )

    def test_default_fork_path_is_clean_under_deprecation_warnings_as_errors(self):
        code = textwrap.dedent(
            """
            from pathlib import Path
            import tempfile
            import time
            from scripts.collect_route_cohort import collect_route_cohort

            left = 'cex:alpha:UNI/USDT'
            right = 'cex:beta:UNI/USDT'
            universe = {
                'candidate_source_generation': 'generation-a',
                'selected_legs': [
                    {'market_id': left, 'market_type': 'cex'},
                    {'market_id': right, 'market_type': 'cex'},
                ],
                'routes': [{
                    'route_id': 'route:UNI:{}->{}:prepositioned_inventory'.format(left, right),
                    'token_symbol': 'UNI',
                    'buy_market_id': left,
                    'sell_market_id': right,
                    'route_mode': 'prepositioned_inventory',
                }],
            }

            def collect(_leg, *, raw_path, **_kwargs):
                time.sleep(0.1)
                raw_path.write_bytes(b'raw')
                return {
                    'status': 'observed',
                    'state_observed_at': '2026-08-01T12:00:00Z',
                }

            result = collect_route_cohort(
                universe,
                cex_collector=collect,
                dex_collector=lambda *_args, **_kwargs: None,
                raw_root=Path(tempfile.mkdtemp()),
                source_generation_reader=lambda: 'input-a',
                expected_source_generation='input-a',
            )
            assert all(row['status'] == 'observed' for row in result['legs'])
            print('ok')
            """
        )
        completed = subprocess.run(
            [
                sys.executable,
                "-W",
                "error::DeprecationWarning",
                "-c",
                code,
            ],
            cwd=str(Path(__file__).resolve().parents[1]),
            text=True,
            capture_output=True,
            timeout=5,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(completed.stdout.strip(), "ok")

    def test_direct_collection_requires_explicit_raw_root_without_artifacts(self):
        universe = _strict_cex_universe()
        source_reads = []
        previous_cwd = Path.cwd()
        with tempfile.TemporaryDirectory() as directory_name:
            temporary_cwd = Path(directory_name)
            os.chdir(str(temporary_cwd))
            try:
                with self.assertRaisesRegex(ValueError, "raw_root is required"):
                    _collect_route_cohort(
                        universe,
                        cex_collector=_write_observed_raw,
                        dex_collector=lambda *_args, **_kwargs: None,
                        executor_factory=ThreadPoolExecutor,
                        source_generation_reader=lambda: (
                            source_reads.append("read") or "input-a"
                        ),
                        expected_source_generation="input-a",
                    )
            finally:
                os.chdir(str(previous_cwd))
            self.assertFalse((temporary_cwd / "data").exists())
        self.assertEqual(source_reads, [])

    def test_raw_root_rejects_existing_and_broken_symlinks(self):
        universe = _strict_cex_universe()
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            target = root / "target"
            target.mkdir()
            existing_link = root / "existing-link"
            broken_link = root / "broken-link"
            existing_link.symlink_to(target, target_is_directory=True)
            broken_link.symlink_to(root / "missing", target_is_directory=True)
            for raw_root in (existing_link, broken_link):
                with self.subTest(raw_root=raw_root.name):
                    with self.assertRaisesRegex(ValueError, "raw_root.*symlink"):
                        _collect_route_cohort(
                            universe,
                            cex_collector=_write_observed_raw,
                            dex_collector=lambda *_args, **_kwargs: None,
                            raw_root=raw_root,
                            executor_factory=ThreadPoolExecutor,
                            source_generation_reader=lambda: "input-a",
                            expected_source_generation="input-a",
                        )
            self.assertEqual(list(target.iterdir()), [])

    def test_raw_root_rejects_caller_controlled_symlink_ancestor(self):
        universe = _strict_cex_universe()
        source_reads = []
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            target = root / "target"
            target.mkdir()
            alias = root / "caller-alias"
            alias.symlink_to(target, target_is_directory=True)
            raw_root = alias / "raw"
            with self.assertRaisesRegex(ValueError, "raw_root.*symlink"):
                _collect_route_cohort(
                    universe,
                    cex_collector=_write_observed_raw,
                    dex_collector=lambda *_args, **_kwargs: None,
                    raw_root=raw_root,
                    executor_factory=ThreadPoolExecutor,
                    source_generation_reader=lambda: (
                        source_reads.append("read") or "input-a"
                    ),
                    expected_source_generation="input-a",
                )
            self.assertFalse((target / "raw").exists())
        self.assertEqual(source_reads, [])

    def test_symlinked_stage_directory_cannot_import_external_evidence(self):
        universe = _strict_cex_universe()
        external_raw = b"EXTERNAL_STAGE_EVIDENCE_SENTINEL"
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            external = root / "external"
            external.mkdir()
            (external / "response.json").write_bytes(external_raw)
            displaced = root / "displaced-original-stage"

            def collector(leg, *, raw_path, **_kwargs):
                if leg["market_id"].startswith("cex:alpha:"):
                    raw_path.parent.rename(displaced)
                    raw_path.parent.symlink_to(
                        external, target_is_directory=True
                    )
                    return {
                        "status": "observed",
                        "state_observed_at": "2026-08-01T12:00:00Z",
                        "raw_response_sha256": hashlib.sha256(
                            external_raw
                        ).hexdigest(),
                    }
                return _write_observed_raw(leg, raw_path=raw_path)

            result = _collect_route_cohort(
                universe,
                cex_collector=collector,
                dex_collector=lambda *_args, **_kwargs: None,
                raw_root=root / "raw",
                max_workers=1,
                executor_factory=ThreadPoolExecutor,
                source_generation_reader=lambda: "input-a",
                expected_source_generation="input-a",
            )

            alpha = next(
                row for row in result["legs"]
                if row["market_id"].startswith("cex:alpha:")
            )
            self.assertEqual(alpha["status"], "failed")
            self.assertEqual(
                alpha["reason_code"], "raw_evidence_path_unsafe"
            )
            self.assertEqual(
                (external / "response.json").read_bytes(), external_raw
            )
            accepted = root / "raw" / result["raw_evidence_run_id"] / "accepted"
            self.assertEqual(len(list(accepted.iterdir())), 1)
            self.assertTrue(all(not path.is_symlink() for path in accepted.iterdir()))

    def test_swapped_real_stage_directory_is_not_promoted(self):
        universe = _strict_cex_universe()
        replacement_raw = b"REAL_DIRECTORY_SWAP_SENTINEL"
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            replacement = root / "replacement-stage"
            replacement.mkdir()
            (replacement / "response.json").write_bytes(replacement_raw)
            displaced = root / "original-stage"

            def collector(leg, *, raw_path, **_kwargs):
                if leg["market_id"].startswith("cex:alpha:"):
                    raw_path.parent.rename(displaced)
                    replacement.rename(raw_path.parent)
                    return {
                        "status": "observed",
                        "state_observed_at": "2026-08-01T12:00:00Z",
                        "raw_response_sha256": hashlib.sha256(
                            replacement_raw
                        ).hexdigest(),
                    }
                return _write_observed_raw(leg, raw_path=raw_path)

            result = _collect_route_cohort(
                universe,
                cex_collector=collector,
                dex_collector=lambda *_args, **_kwargs: None,
                raw_root=root / "raw",
                max_workers=1,
                executor_factory=ThreadPoolExecutor,
                source_generation_reader=lambda: "input-a",
                expected_source_generation="input-a",
            )

            alpha = next(
                row for row in result["legs"]
                if row["market_id"].startswith("cex:alpha:")
            )
            self.assertEqual(alpha["status"], "failed")
            self.assertEqual(
                alpha["reason_code"], "raw_evidence_path_unsafe"
            )
            accepted = root / "raw" / result["raw_evidence_run_id"] / "accepted"
            self.assertEqual(len(list(accepted.iterdir())), 1)

    def test_swapped_accepted_root_cannot_export_staged_evidence(self):
        universe = _strict_cex_universe()
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            external = root / "external-accepted"
            external.mkdir()
            displaced = root / "original-accepted"

            def collector(leg, *, raw_path, **_kwargs):
                if leg["market_id"].startswith("cex:alpha:"):
                    accepted = raw_path.parents[2] / "accepted"
                    accepted.rename(displaced)
                    accepted.symlink_to(external, target_is_directory=True)
                return _write_observed_raw(leg, raw_path=raw_path)

            result = _collect_route_cohort(
                universe,
                cex_collector=collector,
                dex_collector=lambda *_args, **_kwargs: None,
                raw_root=root / "raw",
                max_workers=1,
                executor_factory=ThreadPoolExecutor,
                source_generation_reader=lambda: "input-a",
                expected_source_generation="input-a",
            )

            self.assertTrue(all(
                row["status"] == "failed"
                and row["reason_code"] == "raw_evidence_path_unsafe"
                for row in result["legs"]
            ))
            self.assertEqual(list(external.iterdir()), [])

    def test_accepted_root_swap_between_guard_and_rename_cannot_export_evidence(self):
        universe = _strict_cex_universe()
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            raw_root = root / "raw"
            external = root / "external-accepted"
            external.mkdir()
            displaced = root / "original-accepted"
            swapped = []

            def swap_then_rename(
                source_name,
                destination_name,
                *,
                source_directory_fd,
                destination_directory_fd,
            ):
                if not swapped:
                    accepted = raw_root / "stable-run" / "accepted"
                    accepted.rename(displaced)
                    accepted.symlink_to(external, target_is_directory=True)
                    swapped.append(True)
                return os.rename(
                    source_name,
                    destination_name,
                    src_dir_fd=source_directory_fd,
                    dst_dir_fd=destination_directory_fd,
                )

            with patch(
                "scripts.collect_route_cohort._rename_directory_entry",
                side_effect=swap_then_rename,
            ):
                result = _collect_route_cohort(
                    universe,
                    cex_collector=_write_observed_raw,
                    dex_collector=lambda *_args, **_kwargs: None,
                    raw_root=raw_root,
                    snapshot_id="stable-run",
                    max_workers=1,
                    executor_factory=ThreadPoolExecutor,
                    source_generation_reader=lambda: "input-a",
                    expected_source_generation="input-a",
                )

            self.assertTrue(swapped)
            self.assertEqual(list(external.iterdir()), [])
            self.assertFalse(
                (raw_root / "stable-run" / "accepted").is_symlink()
            )
            self.assertTrue(all(
                row["status"] == "failed"
                and row["reason_code"] == "raw_evidence_path_unsafe"
                for row in result["legs"]
            ))

    def test_rollback_exchanges_staging_collision_before_clearing_swapped_accepted(self):
        universe = _strict_cex_universe()
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            raw_root = root / "raw"
            external_accepted = root / "external-accepted"
            attacked = []

            def promote_then_swap_and_collide(
                source_name,
                destination_name,
                *,
                source_directory_fd,
                destination_directory_fd,
            ):
                result = os.rename(
                    source_name,
                    destination_name,
                    src_dir_fd=source_directory_fd,
                    dst_dir_fd=destination_directory_fd,
                )
                if not attacked:
                    accepted = raw_root / "stable-run" / "accepted"
                    accepted.rename(external_accepted)
                    accepted.symlink_to(
                        external_accepted,
                        target_is_directory=True,
                    )
                    os.symlink(
                        str(root / "attacker-controlled"),
                        source_name,
                        dir_fd=source_directory_fd,
                    )
                    attacked.append(source_name)
                return result

            with patch(
                "scripts.collect_route_cohort._rename_directory_entry",
                side_effect=promote_then_swap_and_collide,
            ):
                result = _collect_route_cohort(
                    universe,
                    cex_collector=_write_observed_raw,
                    dex_collector=lambda *_args, **_kwargs: None,
                    raw_root=raw_root,
                    snapshot_id="stable-run",
                    max_workers=1,
                    executor_factory=ThreadPoolExecutor,
                    source_generation_reader=lambda: "input-a",
                    expected_source_generation="input-a",
                )

            self.assertTrue(attacked)
            self.assertEqual(list(external_accepted.iterdir()), [])
            run_dir = raw_root / "stable-run"
            self.assertFalse((run_dir / "accepted").is_symlink())
            recovered = run_dir / "staging" / attacked[0]
            self.assertTrue(recovered.is_dir())
            self.assertFalse(recovered.is_symlink())
            self.assertEqual(
                (recovered / "response.json").read_bytes(),
                b"observed raw",
            )
            self.assertTrue(all(
                row["status"] == "failed"
                and row["reason_code"] == "raw_evidence_path_unsafe"
                for row in result["legs"]
            ))

    def test_rollback_exchange_failure_hard_fails_after_descriptor_cleanup(self):
        universe = _strict_cex_universe()
        returned = []
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            raw_root = root / "raw"
            external_accepted = root / "external-accepted"
            attacked = []

            def promote_then_swap_and_collide(
                source_name,
                destination_name,
                *,
                source_directory_fd,
                destination_directory_fd,
            ):
                result = os.rename(
                    source_name,
                    destination_name,
                    src_dir_fd=source_directory_fd,
                    dst_dir_fd=destination_directory_fd,
                )
                if not attacked:
                    accepted = raw_root / "stable-run" / "accepted"
                    accepted.rename(external_accepted)
                    accepted.symlink_to(
                        external_accepted,
                        target_is_directory=True,
                    )
                    os.symlink(
                        str(root / "attacker-controlled"),
                        source_name,
                        dir_fd=source_directory_fd,
                    )
                    attacked.append(source_name)
                return result

            descriptors_before = len(os.listdir("/dev/fd"))
            with patch(
                "scripts.collect_route_cohort._rename_directory_entry",
                side_effect=promote_then_swap_and_collide,
            ), patch(
                "scripts.collect_route_cohort._exchange_directory_entries",
                side_effect=OSError("injected exchange failure"),
            ):
                with self.assertRaisesRegex(
                    ValueError,
                    "^raw evidence rollback could not be verified$",
                ):
                    returned.append(_collect_route_cohort(
                        universe,
                        cex_collector=_write_observed_raw,
                        dex_collector=lambda *_args, **_kwargs: None,
                        raw_root=raw_root,
                        snapshot_id="stable-run",
                        max_workers=1,
                        executor_factory=ThreadPoolExecutor,
                        source_generation_reader=lambda: "input-a",
                        expected_source_generation="input-a",
                    ))
            self.assertEqual(len(os.listdir("/dev/fd")), descriptors_before)
            self.assertEqual(returned, [])

    def test_rollback_quarantine_failure_hard_fails_after_descriptor_cleanup(self):
        universe = _strict_cex_universe()
        returned = []
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            raw_root = root / "raw"
            external_accepted = root / "external-accepted"
            attacked = []

            def fail_quarantine_after_collision(
                source_name,
                destination_name,
                *,
                source_directory_fd,
                destination_directory_fd,
            ):
                if destination_name.startswith(".rejected-"):
                    raise OSError("injected quarantine failure")
                result = os.rename(
                    source_name,
                    destination_name,
                    src_dir_fd=source_directory_fd,
                    dst_dir_fd=destination_directory_fd,
                )
                if not attacked:
                    accepted = raw_root / "stable-run" / "accepted"
                    accepted.rename(external_accepted)
                    accepted.symlink_to(
                        external_accepted,
                        target_is_directory=True,
                    )
                    os.symlink(
                        str(root / "attacker-controlled"),
                        source_name,
                        dir_fd=source_directory_fd,
                    )
                    attacked.append(source_name)
                return result

            descriptors_before = len(os.listdir("/dev/fd"))
            with patch(
                "scripts.collect_route_cohort._rename_directory_entry",
                side_effect=fail_quarantine_after_collision,
            ):
                with self.assertRaisesRegex(
                    ValueError,
                    "^raw evidence rollback could not be verified$",
                ):
                    returned.append(_collect_route_cohort(
                        universe,
                        cex_collector=_write_observed_raw,
                        dex_collector=lambda *_args, **_kwargs: None,
                        raw_root=raw_root,
                        snapshot_id="stable-run",
                        max_workers=1,
                        executor_factory=ThreadPoolExecutor,
                        source_generation_reader=lambda: "input-a",
                        expected_source_generation="input-a",
                    ))
            self.assertEqual(len(os.listdir("/dev/fd")), descriptors_before)
            self.assertEqual(returned, [])

    def test_rollback_state_verification_failure_hard_fails_after_descriptor_cleanup(self):
        universe = _strict_cex_universe()
        returned = []
        with tempfile.TemporaryDirectory() as directory_name:
            descriptors_before = len(os.listdir("/dev/fd"))
            with patch(
                "scripts.collect_route_cohort._post_promotion_failure",
                return_value="raw_evidence_hash_mismatch",
            ), patch(
                "scripts.collect_route_cohort._rollback_state_is_safe",
                return_value=False,
            ):
                with self.assertRaisesRegex(
                    ValueError,
                    "^raw evidence rollback could not be verified$",
                ):
                    returned.append(_collect_route_cohort(
                        universe,
                        cex_collector=_write_observed_raw,
                        dex_collector=lambda *_args, **_kwargs: None,
                        raw_root=Path(directory_name),
                        snapshot_id="stable-run",
                        max_workers=1,
                        executor_factory=ThreadPoolExecutor,
                        source_generation_reader=lambda: "input-a",
                        expected_source_generation="input-a",
                    ))
            self.assertEqual(len(os.listdir("/dev/fd")), descriptors_before)
            self.assertEqual(returned, [])

    def test_post_promotion_raw_tamper_is_rejected_and_rolled_back(self):
        universe = _strict_cex_universe()

        def tampering_rename(
            source_name,
            destination_name,
            *,
            source_directory_fd,
            destination_directory_fd,
        ):
            result = os.rename(
                source_name,
                destination_name,
                src_dir_fd=source_directory_fd,
                dst_dir_fd=destination_directory_fd,
            )
            promoted_descriptor = os.open(
                destination_name,
                os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                dir_fd=destination_directory_fd,
            )
            try:
                response_descriptor = os.open(
                    "response.json",
                    os.O_WRONLY | os.O_TRUNC | os.O_NOFOLLOW,
                    dir_fd=promoted_descriptor,
                )
                try:
                    os.write(
                        response_descriptor,
                        b"POST_PROMOTION_TAMPER_SENTINEL",
                    )
                finally:
                    os.close(response_descriptor)
            finally:
                os.close(promoted_descriptor)
            return result

        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            with patch(
                "scripts.collect_route_cohort._rename_directory_entry",
                side_effect=tampering_rename,
            ):
                result = _collect_route_cohort(
                    universe,
                    cex_collector=_write_observed_raw,
                    dex_collector=lambda *_args, **_kwargs: None,
                    raw_root=root,
                    max_workers=1,
                    executor_factory=ThreadPoolExecutor,
                    source_generation_reader=lambda: "input-a",
                    expected_source_generation="input-a",
                )

            self.assertTrue(all(
                row["status"] == "failed"
                and row["reason_code"] == "raw_evidence_hash_mismatch"
                for row in result["legs"]
            ))
            run_dir = root / result["raw_evidence_run_id"]
            self.assertEqual(list((run_dir / "accepted").iterdir()), [])
            self.assertEqual(len(list((run_dir / "staging").iterdir())), 2)

    def test_missing_or_mismatched_raw_evidence_cannot_be_accepted(self):
        universe = _strict_cex_universe()

        def missing_raw(_leg, **_kwargs):
            return {
                "status": "observed",
                "state_observed_at": "2026-08-01T12:00:00Z",
            }

        def mismatched_raw(_leg, *, raw_path, **_kwargs):
            raw_path.write_bytes(b"actual")
            return {
                "status": "observed",
                "state_observed_at": "2026-08-01T12:00:00Z",
                "raw_response_sha256": "0" * 64,
            }

        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            for name, collector, reason in (
                ("missing", missing_raw, "raw_evidence_missing"),
                ("mismatch", mismatched_raw, "raw_evidence_hash_mismatch"),
            ):
                with self.subTest(name=name):
                    result = _collect_route_cohort(
                        universe,
                        cex_collector=collector,
                        dex_collector=lambda *_args, **_kwargs: None,
                        raw_root=root / name,
                        executor_factory=ThreadPoolExecutor,
                        source_generation_reader=lambda: "input-a",
                        expected_source_generation="input-a",
                    )
                    self.assertTrue(all(
                        row["status"] == "failed" and row["reason_code"] == reason
                        for row in result["legs"]
                    ))
                    run_dir = root / name / result["raw_evidence_run_id"]
                    self.assertEqual(list((run_dir / "accepted").iterdir()), [])

    def test_live_main_returns_only_the_fingerprint_bound_cohort(self):
        universe = _strict_cex_universe()
        inventory = [
            {"token_symbol": "UNI", "exchange": "alpha", "cex_symbol": "UNI/USDT"},
            {"token_symbol": "UNI", "exchange": "beta", "cex_symbol": "UNI/USDT"},
        ]

        def cex_observation(_market, *, raw_path, **_kwargs):
            raw = b"cli raw"
            raw_path.write_bytes(raw)
            return {
                "status": "observed",
                "state_observed_at": "2026-08-01T12:00:00Z",
                "raw_response_sha256": hashlib.sha256(raw).hexdigest(),
            }, []

        with tempfile.TemporaryDirectory() as directory_name:
            data_dir = Path(directory_name)
            (data_dir / "route_universe.json").write_text(
                json.dumps(_complete_test_routes(universe)), encoding="utf-8"
            )
            with patch(
                "scripts.collect_route_cohort.load_cataloged_markets",
                return_value=inventory,
            ):
                result = main(
                    ["--data-dir", str(data_dir), "--deadline-seconds", "1"],
                    cex_collector=cex_observation,
                    executor_factory=ThreadPoolExecutor,
                )

        self.assertNotIn("dry_run", result)
        self.assertNotIn("universe_path", result)
        without_hashes = {
            key: value
            for key, value in result.items()
            if key not in {"route_cohort_id", "fingerprint"}
        }
        expected_id = "cohort:" + hashlib.sha256(json.dumps(
            without_hashes,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")).hexdigest()
        self.assertEqual(result["route_cohort_id"], expected_id)
        expected_fingerprint = hashlib.sha256(json.dumps(
            {**without_hashes, "route_cohort_id": expected_id},
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")).hexdigest()
        self.assertEqual(result["fingerprint"], expected_fingerprint)

    def test_cli_recomputes_full_input_generation_and_rejects_inventory_mutation(self):
        universe = {
            "candidate_source_generation": "candidate-a",
            "selection_window": {"start": "2026-08-01", "end": "2026-08-01"},
            "selected_legs": [
                {"market_id": "cex:alpha:UNI/USDT", "market_type": "cex", "token_symbol": "UNI"},
                {"market_id": "cex:beta:UNI/USDT", "market_type": "cex", "token_symbol": "UNI"},
            ],
            "routes": [{"token_symbol": "UNI", "buy_market_id": "cex:alpha:UNI/USDT", "sell_market_id": "cex:beta:UNI/USDT", "route_mode": "prepositioned_inventory"}],
        }
        first = [
            {"token_symbol": "UNI", "exchange": "alpha", "cex_symbol": "UNI/USDT", "observed_at": "2026-08-01T00:00:00Z"},
            {"token_symbol": "UNI", "exchange": "beta", "cex_symbol": "UNI/USDT", "observed_at": "2026-08-01T00:00:00Z"},
        ]
        mutated = [{**row, "observed_at": "2026-08-01T00:00:01Z"} for row in first]
        with tempfile.TemporaryDirectory() as directory_name:
            data_dir = Path(directory_name)
            (data_dir / "route_universe.json").write_text(
                json.dumps(_complete_test_routes(universe)), encoding="utf-8"
            )
            with patch(
                "scripts.collect_route_cohort.load_cataloged_markets",
                side_effect=[first, mutated],
            ):
                with self.assertRaisesRegex(ValueError, "collection input generation changed"):
                    main(
                        ["--data-dir", str(data_dir), "--start", "2026-08-01", "--end", "2026-08-01", "--deadline-seconds", "1"],
                        cex_collector=lambda *_args, **_kwargs: ({"status": "observed", "state_observed_at": "2026-08-01T12:00:00Z"}, []),
                    )

    def test_cli_generation_includes_unfiltered_route_universe(self):
        universe = {
            "candidate_source_generation": "candidate-a",
            "selected_legs": [
                {"market_id": "cex:alpha:UNI/USDT", "market_type": "cex", "token_symbol": "UNI"},
                {"market_id": "cex:beta:UNI/USDT", "market_type": "cex", "token_symbol": "UNI"},
                {"market_id": "cex:alpha:AAVE/USDT", "market_type": "cex", "token_symbol": "AAVE"},
                {"market_id": "cex:beta:AAVE/USDT", "market_type": "cex", "token_symbol": "AAVE"},
            ],
            "routes": [
                {"token_symbol": "UNI", "buy_market_id": "cex:alpha:UNI/USDT", "sell_market_id": "cex:beta:UNI/USDT", "route_mode": "prepositioned_inventory"},
                {"token_symbol": "AAVE", "buy_market_id": "cex:alpha:AAVE/USDT", "sell_market_id": "cex:beta:AAVE/USDT", "route_mode": "prepositioned_inventory"},
            ],
        }
        mutated = {
            **universe,
            "routes": [universe["routes"][0], {
                **universe["routes"][1],
                "route_class": "mutated-unselected-input",
            }],
        }
        inventory = [
            {"token_symbol": "UNI", "exchange": "alpha", "cex_symbol": "UNI/USDT"},
            {"token_symbol": "UNI", "exchange": "beta", "cex_symbol": "UNI/USDT"},
        ]
        with tempfile.TemporaryDirectory() as directory_name:
            data_dir = Path(directory_name)
            with patch(
                "scripts.collect_route_cohort._load_universe_for_cli",
                side_effect=[
                    _complete_test_routes(universe),
                    _complete_test_routes(mutated),
                ],
            ), patch(
                "scripts.collect_route_cohort.load_cataloged_markets",
                return_value=inventory,
            ):
                with self.assertRaisesRegex(
                    ValueError, "collection input generation changed"
                ):
                    main(
                        ["--data-dir", str(data_dir), "--tokens", "UNI"],
                        cex_collector=lambda *_args, **_kwargs: self.fail(
                            "must reject before collection"
                        ),
                    )
            self.assertFalse((data_dir / "raw").exists())

    def test_selected_identity_conflict_and_returned_identity_mismatch_fail_closed(self):
        universe = {
            "candidate_source_generation": "candidate-a",
            "selected_legs": [
                {"market_id": "cex:alpha:UNI/USDT", "market_type": "cex", "token_symbol": "AAVE"},
                {"market_id": "cex:beta:UNI/USDT", "market_type": "cex", "token_symbol": "UNI"},
            ],
            "routes": [{"token_symbol": "UNI", "buy_market_id": "cex:alpha:UNI/USDT", "sell_market_id": "cex:beta:UNI/USDT", "route_mode": "prepositioned_inventory"}],
        }
        inventory = [
            {"token_symbol": "UNI", "exchange": "alpha", "cex_symbol": "UNI/USDT"},
            {"token_symbol": "UNI", "exchange": "beta", "cex_symbol": "UNI/USDT"},
        ]
        with tempfile.TemporaryDirectory() as directory_name:
            data_dir = Path(directory_name)
            (data_dir / "route_universe.json").write_text(
                json.dumps(_complete_test_routes(universe)), encoding="utf-8"
            )
            with patch(
                "scripts.collect_route_cohort.load_cataloged_markets",
                return_value=inventory,
            ) as load_catalog:
                with self.assertRaisesRegex(ValueError, "route leg identity is invalid"):
                    main(["--data-dir", str(data_dir)], cex_collector=lambda *_args, **_kwargs: self.fail("must not collect"))
                load_catalog.assert_not_called()

        direct = {**universe, "selected_legs": [{**row, "token_symbol": "UNI"} for row in universe["selected_legs"]]}
        result = collect_route_cohort(
            direct,
            cex_collector=lambda leg, **_kwargs: ({"status": "observed", "state_observed_at": "2026-08-01T12:00:00Z", "token_symbol": "AAVE"}, []),
            dex_collector=lambda *_args, **_kwargs: self.fail("unexpected DEX"),
            source_generation_reader=lambda: "input-a",
            expected_source_generation="input-a",
        )
        self.assertTrue(all(row["reason_code"] == "collector_identity_mismatch" for row in result["legs"]))

    def test_partial_dex_collector_pool_identity_is_case_sensitive(self):
        cex_market = "cex:alpha:UNI/USDT"
        dex_market = "dex:sol:orca:PoolCase:UNI"
        universe = {
            "candidate_source_generation": "candidate-a",
            "selected_legs": [
                {
                    "market_id": cex_market,
                    "market_type": "cex",
                    "exchange": "alpha",
                },
                {
                    "market_id": dex_market,
                    "market_type": "dex",
                    "chain": "sol",
                    "dex": "orca",
                    "pool_address": "PoolCase",
                    "token_symbol": "UNI",
                },
            ],
            "routes": [
                _strict_route(
                    "UNI",
                    cex_market,
                    dex_market,
                    "prepositioned_inventory",
                )
            ],
        }
        fixed_timestamp = "2026-08-01T12:00:00Z"

        result = collect_route_cohort(
            universe,
            cex_collector=_write_observed_raw,
            dex_collector=lambda _leg, **_kwargs: {
                "status": "partial",
                "state_observed_at": fixed_timestamp,
                "block_number": 123,
                "block_timestamp": fixed_timestamp,
                "pool_address": "poolcase",
            },
            dex_block_resolver=lambda *_args, **_kwargs: {
                "block_number": 123,
                "block_timestamp": fixed_timestamp,
            },
            source_generation_reader=lambda: "input-a",
            expected_source_generation="input-a",
        )

        dex_leg = next(
            row for row in result["legs"] if row["market_id"] == dex_market
        )
        self.assertEqual(dex_leg["status"], "failed")
        self.assertEqual(
            dex_leg["reason_code"],
            "collector_identity_mismatch",
        )

    def test_blocked_worker_does_not_keep_subprocess_alive_past_deadline(self):
        code = textwrap.dedent(
            """
            from pathlib import Path
            from threading import Event
            import tempfile
            from scripts.collect_route_cohort import collect_route_cohort
            universe = {
                'candidate_source_generation': 'candidate-a',
                'selected_legs': [
                    {'market_id': 'cex:alpha:UNI/USDT', 'market_type': 'cex'},
                    {'market_id': 'cex:beta:UNI/USDT', 'market_type': 'cex'},
                ],
                'routes': [{'route_id': 'route:UNI:cex:alpha:UNI/USDT->cex:beta:UNI/USDT:prepositioned_inventory', 'token_symbol': 'UNI', 'buy_market_id': 'cex:alpha:UNI/USDT', 'sell_market_id': 'cex:beta:UNI/USDT', 'route_mode': 'prepositioned_inventory'}],
            }
            gate = Event()
            result = collect_route_cohort(
                universe, deadline_seconds=0.05,
                cex_collector=lambda *_args, **_kwargs: gate.wait(),
                dex_collector=lambda *_args, **_kwargs: None,
                raw_root=Path(tempfile.mkdtemp()),
                source_generation_reader=lambda: 'input-a',
                expected_source_generation='input-a',
            )
            print([row['status'] for row in result['legs']])
            """
        )
        completed = subprocess.run(
            [sys.executable, "-c", code], cwd=str(Path(__file__).resolve().parents[1]),
            text=True, capture_output=True, timeout=5,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertIn("deadline_exceeded", completed.stdout)

    def test_late_raw_write_remains_staging_and_never_becomes_accepted(self):
        started = Event()
        release = Event()
        finished = Event()
        universe = {
            "candidate_source_generation": "candidate-a",
            "selected_legs": [
                {"market_id": "cex:alpha:UNI/USDT", "market_type": "cex"},
                {"market_id": "cex:beta:UNI/USDT", "market_type": "cex"},
            ],
            "routes": [{
                "token_symbol": "UNI",
                "buy_market_id": "cex:alpha:UNI/USDT",
                "sell_market_id": "cex:beta:UNI/USDT",
                "route_mode": "prepositioned_inventory",
            }],
        }

        def late_collector(_leg, *, raw_path, **_kwargs):
            started.set()
            release.wait()
            raw_path.write_text("late", encoding="utf-8")
            finished.set()
            return {
                "status": "observed",
                "state_observed_at": "2026-08-01T12:00:00Z",
            }

        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            result = collect_route_cohort(
                universe,
                cex_collector=late_collector,
                dex_collector=lambda *_args, **_kwargs: None,
                deadline_seconds=0.05,
                max_workers=1,
                raw_root=root,
                source_generation_reader=lambda: "input-a",
                expected_source_generation="input-a",
            )
            self.assertTrue(started.is_set())
            release.set()
            self.assertTrue(finished.wait(timeout=0.5))
            run_dir = root / result["raw_evidence_run_id"]
            self.assertEqual(list((run_dir / "accepted").iterdir()), [])
            self.assertEqual(
                [path.read_text(encoding="utf-8") for path in (run_dir / "staging").glob("*/response.json")],
                ["late"],
            )

    def test_supplied_executor_fairness_regression_exits_cleanly(self):
        target = (
            "tests.test_route_collection.RouteLegCollectionTests."
            "test_resolver_is_scheduled_fairly_with_cex_collection"
        )
        completed = subprocess.run(
            [sys.executable, "-m", "unittest", target],
            cwd=str(Path(__file__).resolve().parents[1]),
            text=True,
            capture_output=True,
            timeout=5,
        )

        self.assertEqual(completed.returncode, 0, completed.stderr)

    def test_resolver_is_scheduled_fairly_with_cex_collection(self):
        cex_started = Event()
        resolver_started = Event()
        resolver_gate = Event()
        resolver_finished = Event()

        def release_resolver():
            resolver_gate.set()
            self.assertTrue(resolver_finished.wait(timeout=0.5))

        self.addCleanup(release_resolver)
        cex_saw_resolver = []
        universe = {
            "candidate_source_generation": "candidate-a",
            "selected_legs": [
                {"market_id": "cex:alpha:UNI/USDT", "market_type": "cex"},
                {"market_id": "cex:beta:UNI/USDT", "market_type": "cex"},
                {"market_id": "dex:eth:swap:0xone:UNI", "market_type": "dex", "chain": "eth"},
            ],
            "routes": [
                {"token_symbol": "UNI", "buy_market_id": "cex:alpha:UNI/USDT", "sell_market_id": "cex:beta:UNI/USDT", "route_mode": "prepositioned_inventory"},
                {"token_symbol": "UNI", "buy_market_id": "cex:alpha:UNI/USDT", "sell_market_id": "dex:eth:swap:0xone:UNI", "route_mode": "prepositioned_inventory"},
            ],
        }

        def resolver(_chain, **_kwargs):
            resolver_started.set()
            resolver_gate.wait()
            resolver_finished.set()

        def collect_cex(*_args, **_kwargs):
            cex_started.set()
            cex_saw_resolver.append(resolver_started.wait(timeout=0.2))
            return {"status": "observed", "state_observed_at": "2026-08-01T12:00:00Z"}

        result = collect_route_cohort(
            universe,
            cex_collector=collect_cex,
            dex_collector=lambda *_args, **_kwargs: self.fail("unresolved DEX must not collect"),
            dex_block_resolver=resolver, max_workers=2, deadline_seconds=0.15,
            source_generation_reader=lambda: "input-a", expected_source_generation="input-a",
        )
        self.assertTrue(cex_started.is_set())
        self.assertTrue(all(cex_saw_resolver))
        self.assertTrue(all(
            row["status"] == "observed"
            for row in result["legs"] if row["market_id"].startswith("cex:")
        ))
        self.assertEqual(
            next(row for row in result["legs"] if row["market_id"].startswith("dex:"))["status"],
            "deadline_exceeded",
        )

    def test_snapshot_id_traversal_and_raw_run_collision_are_rejected(self):
        universe = {
            "candidate_source_generation": "candidate-a",
            "selected_legs": [
                {"market_id": "cex:alpha:UNI/USDT", "market_type": "cex"},
                {"market_id": "cex:beta:UNI/USDT", "market_type": "cex"},
            ],
            "routes": [{"token_symbol": "UNI", "buy_market_id": "cex:alpha:UNI/USDT", "sell_market_id": "cex:beta:UNI/USDT", "route_mode": "prepositioned_inventory"}],
        }
        kwargs = {
            "cex_collector": lambda *_args, **_kwargs: {"status": "observed", "state_observed_at": "2026-08-01T12:00:00Z"},
            "dex_collector": lambda *_args, **_kwargs: None,
            "source_generation_reader": lambda: "input-a",
            "expected_source_generation": "input-a",
        }
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            with self.assertRaisesRegex(ValueError, "snapshot_id"):
                collect_route_cohort(universe, raw_root=root, snapshot_id="../escape", **kwargs)
            explicit = collect_route_cohort(
                universe, raw_root=root, snapshot_id="same-run", **kwargs
            )
            with self.assertRaisesRegex(FileExistsError, "same-run"):
                collect_route_cohort(universe, raw_root=root, snapshot_id="same-run", **kwargs)
            wall_time = datetime(2026, 8, 1, 12, tzinfo=timezone.utc)
            first = collect_route_cohort(
                universe, raw_root=root, wall_clock=lambda: wall_time, **kwargs
            )
            second = collect_route_cohort(
                universe, raw_root=root, wall_clock=lambda: wall_time, **kwargs
            )

            self.assertNotEqual(
                first["raw_evidence_run_id"], second["raw_evidence_run_id"]
            )
            accepted_names = [
                path.name
                for path in (root / explicit["raw_evidence_run_id"] / "accepted").iterdir()
            ]
            self.assertEqual(len(accepted_names), 2)
            self.assertTrue(all(
                len(name) == 64 and set(name) <= set("0123456789abcdef")
                for name in accepted_names
            ))

    def test_task3_cex_adapter_receives_only_its_declared_arguments(self):
        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [
                {
                    "market_id": "cex:alpha:UNI/USDT",
                    "market_type": "cex",
                    "token_symbol": "UNI",
                    "exchange": "alpha",
                    "cex_symbol": "UNI/USDT",
                },
                {
                    "market_id": "cex:beta:UNI/USDT",
                    "market_type": "cex",
                    "token_symbol": "UNI",
                    "exchange": "beta",
                    "cex_symbol": "UNI/USDT",
                },
            ],
            "routes": [
                {"token_symbol": "UNI", "buy_market_id": "cex:alpha:UNI/USDT", "sell_market_id": "cex:beta:UNI/USDT", "route_mode": "prepositioned_inventory"},
            ],
        }
        calls = []

        def cex_primitive(market, *, snapshot_id, raw_path, deadline):
            calls.append((market["market_id"], snapshot_id, raw_path.name, deadline))
            return ({"status": "observed", "state_observed_at": "2026-08-01T12:00:00Z", "source_endpoint": "https://user:pass@example.test/depth?api_key=private", "credential": "private"}, [])

        with tempfile.TemporaryDirectory() as directory_name:
            result = collect_route_cohort(
                universe,
                cex_collector=cex_primitive,
                dex_collector=lambda *_args, **_kwargs: self.fail("unexpected DEX collection"),
                raw_root=Path(directory_name),
                snapshot_id="cohort-test",
                source_generation_reader=lambda: "generation-a",
                expected_source_generation="generation-a",
            )

        self.assertCountEqual(
            [call[0] for call in calls],
            ["cex:alpha:UNI/USDT", "cex:beta:UNI/USDT"],
        )
        self.assertTrue(all(call[1] == "cohort-test" for call in calls))
        self.assertEqual([row["status"] for row in result["legs"]], ["observed", "observed"])
        self.assertEqual(
            result["legs"][0]["source_endpoint"],
            "https://example.test",
        )
        self.assertNotIn("credential", result["legs"][0])

    def test_safe_leg_projection_recursively_drops_non_json_objects(self):
        value = {
            "safe": {
                "items": [
                    "keep",
                    Path("/PRIVATE_PATH_SENTINEL"),
                    ValueError("PRIVATE_EXCEPTION_SENTINEL"),
                    object(),
                    float("nan"),
                    ("tuple-value",),
                ],
                "url": (
                    "https://user:pass@example.test/path?"
                    "api_key=PRIVATE_QUERY_SENTINEL#fragment"
                ),
            }
        }

        projected = _safe_leg_projection(value)

        self.assertEqual(projected, {
            "safe": {
                "items": ["keep", ["tuple-value"]],
                "url": "https://example.test/path",
            }
        })
        json.dumps(projected, allow_nan=False)

    def test_safe_leg_projection_drops_local_paths_and_path_like_keys(self):
        value = {
            "safe": "retained",
            "absolute": "/private/tmp/PRIVATE_ABSOLUTE_PATH_SENTINEL.json",
            "home": "~/PRIVATE_HOME_PATH_SENTINEL.json",
            "unc": r"\\server\share\PRIVATE_UNC_PATH_SENTINEL.json",
            "drive": r"C:\Users\name\PRIVATE_DRIVE_PATH_SENTINEL.json",
            "file_uri": "file:///private/tmp/PRIVATE_FILE_URI_SENTINEL.json",
            "artifact_path": "relative/PRIVATE_PATH_KEY_SENTINEL.json",
            "nested": [{"cachePath": "PRIVATE_PATH_KEY_SENTINEL"}],
        }

        projected = _safe_leg_projection(value)

        self.assertEqual(projected, {"safe": "retained", "nested": [{}]})
        encoded = json.dumps(projected, allow_nan=False)
        for sentinel in (
            "PRIVATE_ABSOLUTE_PATH_SENTINEL",
            "PRIVATE_HOME_PATH_SENTINEL",
            "PRIVATE_UNC_PATH_SENTINEL",
            "PRIVATE_DRIVE_PATH_SENTINEL",
            "PRIVATE_FILE_URI_SENTINEL",
            "PRIVATE_PATH_KEY_SENTINEL",
        ):
            self.assertNotIn(sentinel, encoded)

    def test_safe_leg_projection_rejects_non_http_credentials_and_parent_paths(self):
        projected = _safe_leg_projection({
            "endpoint": (
                "wss://user:PRIVATE_PASS@example.test/ws?"
                "token=PRIVATE_TOKEN#PRIVATE_FRAGMENT"
            ),
            "database": (
                "postgres://user:PRIVATE_DB_PASS@example.test/market?"
                "sslkey=PRIVATE_SSL_KEY"
            ),
            "private_key": "PRIVATE_KEY_SENTINEL",
            "provenance": "../PRIVATE_PARENT/secret.json",
            "windows_parent": r"..\PRIVATE_WINDOWS_PARENT\secret.json",
            "middle_parent": "safe/../PRIVATE_MIDDLE_PARENT/secret.json",
            "middle_dot": r"safe\.\PRIVATE_MIDDLE_DOT\secret.json",
            "opaque_wss": (
                "wss:user:PRIVATE_OPAQUE_WSS@example.test/ws?"
                "token=PRIVATE_OPAQUE_WSS_TOKEN"
            ),
            "opaque_postgres": (
                "postgres:user:PRIVATE_OPAQUE_DB@example.test/market?"
                "sslkey=PRIVATE_OPAQUE_DB_KEY"
            ),
            "opaque_https": (
                "https:user:PRIVATE_OPAQUE_HTTPS@example.test/depth?"
                "token=PRIVATE_OPAQUE_HTTPS_TOKEN"
            ),
            "market_symbol": "UNI/USDT",
            "market_id": "dex:sol:orca:PoolCase:UNI",
        })

        self.assertEqual(projected, {
            "market_symbol": "UNI/USDT",
            "market_id": "dex:sol:orca:PoolCase:UNI",
        })
        encoded = json.dumps(projected, allow_nan=False)
        for sentinel in (
            "PRIVATE_PASS",
            "PRIVATE_TOKEN",
            "PRIVATE_FRAGMENT",
            "PRIVATE_DB_PASS",
            "PRIVATE_SSL_KEY",
            "PRIVATE_KEY_SENTINEL",
            "PRIVATE_PARENT",
            "PRIVATE_WINDOWS_PARENT",
            "PRIVATE_MIDDLE_PARENT",
            "PRIVATE_MIDDLE_DOT",
            "PRIVATE_OPAQUE_WSS",
            "PRIVATE_OPAQUE_WSS_TOKEN",
            "PRIVATE_OPAQUE_DB",
            "PRIVATE_OPAQUE_DB_KEY",
            "PRIVATE_OPAQUE_HTTPS",
            "PRIVATE_OPAQUE_HTTPS_TOKEN",
        ):
            self.assertNotIn(sentinel, encoded)

    def test_nested_leg_provenance_is_secret_free_and_json_safe(self):
        universe = _strict_cex_universe()

        def collector(_leg, **_kwargs):
            return {
                "status": "observed",
                "state_observed_at": "2026-08-01T12:00:00Z",
                "provenance": {
                    "api_key": "PRIVATE_API_KEY_SENTINEL",
                    "Authorization": "Bearer PRIVATE_AUTH_SENTINEL",
                    "safe_label": "retained",
                    "cache_path": "relative/PRIVATE_PATH_KEY_SENTINEL.json",
                    "local_source": "/private/tmp/PRIVATE_PATH_VALUE_SENTINEL.json",
                    "nested": [
                        {
                            "endpoint": (
                                "https://user:pass@example.test/depth?"
                                "token=PRIVATE_QUERY_SENTINEL"
                            ),
                            "token": "PRIVATE_TOKEN_SENTINEL",
                        },
                        (
                            "https://user:pass@example.test/tuple?"
                            "api_key=PRIVATE_TUPLE_SENTINEL",
                            {"safe": "value", "secret": "PRIVATE_SECRET_SENTINEL"},
                        ),
                    ],
                },
            }

        with tempfile.TemporaryDirectory() as directory_name:
            result = collect_route_cohort(
                universe,
                cex_collector=collector,
                dex_collector=lambda *_args, **_kwargs: None,
                raw_root=Path(directory_name),
                source_generation_reader=lambda: "input-a",
                expected_source_generation="input-a",
            )

        self.assertEqual(result["legs"][0]["provenance"], {
            "safe_label": "retained",
            "nested": [
                {"endpoint": "https://example.test/depth"},
                [
                    "https://example.test/tuple",
                    {"safe": "value"},
                ],
            ],
        })
        encoded = json.dumps(
            result,
            ensure_ascii=False,
            sort_keys=True,
            allow_nan=False,
        )
        for sentinel in (
            "PRIVATE_API_KEY_SENTINEL",
            "PRIVATE_AUTH_SENTINEL",
            "PRIVATE_QUERY_SENTINEL",
            "PRIVATE_TOKEN_SENTINEL",
            "PRIVATE_TUPLE_SENTINEL",
            "PRIVATE_SECRET_SENTINEL",
            "PRIVATE_PATH_KEY_SENTINEL",
            "PRIVATE_PATH_VALUE_SENTINEL",
            "user:pass@",
        ):
            self.assertNotIn(sentinel, encoded)

    def test_live_cli_resolves_exact_cex_inventory_and_runs_without_publish(self):
        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [
                {"market_id": "cex:alpha:UNI/USDT", "market_type": "cex", "token_symbol": "UNI"},
                {"market_id": "cex:beta:UNI/USDT", "market_type": "cex", "token_symbol": "UNI"},
            ],
            "routes": [
                {"token_symbol": "UNI", "buy_market_id": "cex:alpha:UNI/USDT", "sell_market_id": "cex:beta:UNI/USDT", "route_mode": "prepositioned_inventory"},
            ],
        }
        inventory = [
            {"token_symbol": "UNI", "exchange": "alpha", "cex_symbol": "UNI/USDT"},
            {"token_symbol": "UNI", "exchange": "beta", "cex_symbol": "UNI/USDT"},
        ]
        with tempfile.TemporaryDirectory() as directory_name:
            data_dir = Path(directory_name)
            (data_dir / "route_universe.json").write_text(
                json.dumps(_complete_test_routes(universe)), encoding="utf-8"
            )
            calls = []

            def cex_primitive(market, *, snapshot_id, raw_path, deadline):
                calls.append(market)
                raw_path.write_bytes(b"cli inventory raw")
                return ({"status": "observed", "state_observed_at": "2026-08-01T12:00:00Z"}, [])

            with patch("scripts.collect_route_cohort.load_cataloged_markets", return_value=inventory):
                result = main(
                    ["--data-dir", str(data_dir), "--deadline-seconds", "1"],
                    cex_collector=cex_primitive,
                    executor_factory=ThreadPoolExecutor,
                )

        self.assertNotIn("dry_run", result)
        self.assertEqual(
            sorted(
                ({key: row[key] for key in ("token_symbol", "exchange", "cex_symbol")} for row in calls),
                key=lambda row: row["exchange"],
            ),
            sorted(inventory, key=lambda row: row["exchange"]),
        )

    def test_live_cli_publish_writes_only_validated_core_pointer(self):
        universe = _strict_cex_universe()
        universe["selection_window"] = {
            "start": "2026-07-25",
            "end": "2026-08-01",
        }
        universe["requested_notionals_usd"] = [
            1000,
            5000,
            10000,
            50000,
            100000,
        ]
        for route in universe["routes"]:
            route.update(
                {
                    "route_class": "candidate",
                    "settlement_reason": None,
                    "requested_notionals_usd": [
                        1000,
                        5000,
                        10000,
                        50000,
                        100000,
                    ],
                    "candidate_source_generation": "generation-a",
                    "buy_reference_volume_usd": "9000",
                    "sell_reference_volume_usd": "7000",
                    "route_volume_usd": "7000",
                    "route_volume_basis": "minimum_leg_source_horizon_usd",
                }
            )
        inventory = [
            {
                "token_symbol": "UNI",
                "exchange": exchange,
                "cex_symbol": "UNI/USDT",
            }
            for exchange in ("alpha", "beta")
        ]

        def observed_with_snapshot(_leg, *, snapshot_id, raw_path, **_kwargs):
            raw_path.write_bytes(b"published route raw")
            return {
                "status": "observed",
                "snapshot_id": snapshot_id,
                "state_observed_at": "2026-08-01T12:00:00Z",
            }

        with tempfile.TemporaryDirectory() as directory_name:
            data_dir = Path(directory_name)
            (data_dir / "route_universe.json").write_text(
                json.dumps(_complete_test_routes(universe)), encoding="utf-8"
            )

            with patch(
                "scripts.collect_route_cohort.load_cataloged_markets",
                return_value=inventory,
            ):
                result = main(
                    [
                        "--data-dir",
                        str(data_dir),
                        "--deadline-seconds",
                        "1",
                        "--publish",
                    ],
                    cex_collector=observed_with_snapshot,
                    executor_factory=ThreadPoolExecutor,
                )

            loaded = load_latest_route_cohort(data_dir / "routes/core")
            self.assertEqual(
                loaded["manifest"]["route_cohort_id"],
                result["route_cohort_id"],
            )
            self.assertFalse((data_dir / "routes/latest.json").exists())

    def test_dry_run_applies_tokens_and_rejects_invalid_worker_values(self):
        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [
                {"market_id": "cex:alpha:UNI/USDT", "market_type": "cex", "token_symbol": "UNI"},
                {"market_id": "cex:beta:UNI/USDT", "market_type": "cex", "token_symbol": "UNI"},
                {"market_id": "cex:alpha:AAVE/USDT", "market_type": "cex", "token_symbol": "AAVE"},
                {"market_id": "cex:beta:AAVE/USDT", "market_type": "cex", "token_symbol": "AAVE"},
            ],
            "routes": [
                {"token_symbol": "UNI", "buy_market_id": "cex:alpha:UNI/USDT", "sell_market_id": "cex:beta:UNI/USDT", "route_mode": "prepositioned_inventory"},
                {"token_symbol": "AAVE", "buy_market_id": "cex:alpha:AAVE/USDT", "sell_market_id": "cex:beta:AAVE/USDT", "route_mode": "prepositioned_inventory"},
            ],
        }
        with tempfile.TemporaryDirectory() as directory_name:
            data_dir = Path(directory_name)
            (data_dir / "route_universe.json").write_text(
                json.dumps(_complete_test_routes(universe)), encoding="utf-8"
            )
            inventory = [
                {
                    "token_symbol": leg["token_symbol"],
                    "exchange": leg["market_id"].split(":", 2)[1],
                    "cex_symbol": leg["market_id"].split(":", 2)[2],
                }
                for leg in universe["selected_legs"]
            ]
            with patch(
                "scripts.collect_route_cohort.load_cataloged_markets",
                return_value=inventory,
            ):
                result = main([
                    "--data-dir", str(data_dir),
                    "--tokens", "UNI",
                    "--dry-run", "--publish",
                ])
            self.assertEqual((result["selected_leg_count"], result["route_count"]), (2, 1))
            self.assertEqual(len(result["collection_input_generation"]), 64)
            self.assertFalse((data_dir / "raw").exists())
            self.assertFalse((data_dir / "routes/core/latest.json").exists())
            with self.assertRaisesRegex(ValueError, "worker limits"):
                main(["--data-dir", str(data_dir), "--max-workers", "0", "--dry-run"])

    def test_dry_run_binds_authoritative_inventory_and_fails_closed(self):
        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [
                {"market_id": "cex:alpha:UNI/USDT", "market_type": "cex", "token_symbol": "AAVE"},
                {"market_id": "cex:beta:UNI/USDT", "market_type": "cex", "token_symbol": "UNI"},
            ],
            "routes": [
                {"token_symbol": "UNI", "buy_market_id": "cex:alpha:UNI/USDT", "sell_market_id": "cex:beta:UNI/USDT", "route_mode": "prepositioned_inventory"},
            ],
        }
        inventory = [
            {"token_symbol": "UNI", "exchange": "alpha", "cex_symbol": "UNI/USDT"},
            {"token_symbol": "UNI", "exchange": "beta", "cex_symbol": "UNI/USDT"},
        ]
        with tempfile.TemporaryDirectory() as directory_name:
            data_dir = Path(directory_name)
            universe_path = data_dir / "route_universe.json"
            universe_path.write_text(
                json.dumps(_complete_test_routes(universe)), encoding="utf-8"
            )
            with patch(
                "scripts.collect_route_cohort.load_cataloged_markets",
                return_value=inventory,
            ) as load_catalog:
                with self.assertRaisesRegex(
                    ValueError, "route leg identity is invalid"
                ):
                    main(["--data-dir", str(data_dir), "--dry-run"])
                load_catalog.assert_not_called()

            universe["selected_legs"][0]["token_symbol"] = "UNI"
            universe_path.write_text(
                json.dumps(_complete_test_routes(universe)), encoding="utf-8"
            )
            with patch(
                "scripts.collect_route_cohort.load_cataloged_markets",
                return_value=inventory[:1],
            ):
                with self.assertRaisesRegex(
                    ValueError, "absent from authoritative inventory"
                ):
                    main(["--data-dir", str(data_dir), "--dry-run"])
            self.assertFalse((data_dir / "raw").exists())

    def test_cli_dates_routes_and_nonfinite_deadlines_fail_closed(self):
        universe = {
            "candidate_source_generation": "generation-a",
            "selection_window": {"start": "2026-08-01", "end": "2026-08-02"},
            "selected_legs": [
                {"market_id": "cex:alpha:UNI/USDT", "market_type": "cex"},
                {"market_id": "cex:beta:UNI/USDT", "market_type": "cex"},
            ],
            "routes": [
                {"token_symbol": "UNI", "buy_market_id": "cex:alpha:UNI/USDT", "sell_market_id": "cex:beta:UNI/USDT", "route_mode": "prepositioned_inventory"},
            ],
        }
        with tempfile.TemporaryDirectory() as directory_name:
            data_dir = Path(directory_name)
            universe_path = data_dir / "route_universe.json"
            universe_path.write_text(
                json.dumps(_complete_test_routes(universe)), encoding="utf-8"
            )
            with self.assertRaisesRegex(ValueError, "selection_window"):
                main([
                    "--data-dir", str(data_dir), "--start", "2026-07-31",
                    "--end", "2026-08-02", "--dry-run",
                ])
            malformed = {**universe, "routes": [{
                "token_symbol": "UNI",
                "buy_market_id": "cex:alpha:UNI/USDT",
                "sell_market_id": "cex:beta:UNI/USDT",
            }]}
            universe_path.write_text(json.dumps(malformed), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "route candidate is invalid"):
                main(["--data-dir", str(data_dir), "--dry-run"])
            for value in ("nan", "inf", "-inf"):
                with self.subTest(deadline_seconds=value):
                    with self.assertRaisesRegex(ValueError, "must be positive"):
                        main([
                            "--data-dir", str(data_dir),
                            "--deadline-seconds={}".format(value), "--dry-run",
                        ])
        valid = {
            **universe,
            "routes": [{
                "token_symbol": "UNI",
                "buy_market_id": "cex:alpha:UNI/USDT",
                "sell_market_id": "cex:beta:UNI/USDT",
                "route_mode": "prepositioned_inventory",
            }],
        }
        for value in (float("nan"), float("inf"), float("-inf")):
            with self.subTest(direct_deadline_seconds=value):
                with self.assertRaisesRegex(ValueError, "finite and positive"):
                    collect_route_cohort(
                        valid,
                        deadline_seconds=value,
                        cex_collector=lambda *_args, **_kwargs: self.fail(
                            "must reject before collection"
                        ),
                        dex_collector=lambda *_args, **_kwargs: None,
                        source_generation_reader=lambda: "input-a",
                        expected_source_generation="input-a",
                    )

    def test_direct_collection_rejects_malformed_route_before_raw_or_work(self):
        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [
                {"market_id": "cex:alpha:UNI/USDT", "market_type": "cex"},
                {"market_id": "cex:beta:UNI/USDT", "market_type": "cex"},
            ],
            "routes": [{
                "token_symbol": "UNI",
                "buy_market_id": "cex:alpha:UNI/USDT",
                "sell_market_id": "cex:beta:UNI/USDT",
            }],
        }
        calls = []
        with tempfile.TemporaryDirectory() as directory_name:
            raw_root = Path(directory_name) / "raw"
            with self.assertRaisesRegex(ValueError, "route candidate is invalid"):
                collect_route_cohort(
                    universe,
                    cex_collector=lambda *_args, **_kwargs: calls.append("called"),
                    dex_collector=lambda *_args, **_kwargs: None,
                    raw_root=raw_root,
                    source_generation_reader=lambda: "input-a",
                    expected_source_generation="input-a",
                )
            self.assertEqual(calls, [])
            self.assertFalse(raw_root.exists())

    def test_cohort_metadata_and_late_success_are_stable_and_deadline_terminal(self):
        from scripts.collection_deadline import CollectionDeadline

        clock = FakeClock()
        deadline = CollectionDeadline.for_duration(1, clock=clock.monotonic, sleeper=clock.sleep)
        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [
                {"market_id": "cex:alpha:UNI/USDT", "market_type": "cex"},
                {"market_id": "cex:beta:UNI/USDT", "market_type": "cex"},
            ],
            "routes": [{"token_symbol": "UNI", "buy_market_id": "cex:alpha:UNI/USDT", "sell_market_id": "cex:beta:UNI/USDT", "route_mode": "prepositioned_inventory"}],
        }

        def cex_collector(leg, **_kwargs):
            if leg["market_id"] == "cex:beta:UNI/USDT":
                clock.now = 2
            return {"status": "observed", "state_observed_at": "2026-08-01T12:00:00Z"}

        result = collect_route_cohort(
            universe, cex_collector=cex_collector,
            dex_collector=lambda *_args, **_kwargs: self.fail("unexpected DEX collection"),
            deadline=deadline, target_observed_at="2026-08-01T12:00:00+08:00",
            source_generation_reader=lambda: "generation-a",
            expected_source_generation="generation-a",
            wall_clock=lambda: datetime(
                2026, 8, 1, 12, tzinfo=timezone(timedelta(hours=8))
            ),
        )

        self.assertTrue(result["route_cohort_id"].startswith("cohort:"))
        self.assertEqual(result["target_observed_at"], "2026-08-01T04:00:00Z")
        self.assertEqual(result["collection_started_at"], "2026-08-01T04:00:00Z")
        self.assertEqual(result["collection_deadline_at"], "2026-08-01T04:00:01Z")
        terminal = next(row for row in result["legs"] if row["market_id"] == "cex:beta:UNI/USDT")
        self.assertEqual(terminal["status"], "deadline_exceeded")
        self.assertEqual(result["skew_sla_seconds"], "60")
        self.assertEqual(result["route_age_sla_seconds"], "120")
        self.assertEqual(result["candidate_source_generation"], "generation-a")
        self.assertEqual(result["collection_input_generation"], "generation-a")
        self.assertEqual(result["source_state"], {
            "candidate_source_generation": "generation-a",
            "collection_input_generation": "generation-a",
        })

    def test_resolver_timeout_and_fixed_block_mismatch_are_isolated(self):
        from scripts.collection_deadline import CollectionDeadlineExceeded

        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [
                {"market_id": "cex:alpha:UNI/USDT", "market_type": "cex"},
                {"market_id": "cex:beta:UNI/USDT", "market_type": "cex"},
                {"market_id": "dex:eth:swap:0xone:UNI", "market_type": "dex", "chain": "eth"},
            ],
            "routes": [
                {"token_symbol": "UNI", "buy_market_id": "cex:alpha:UNI/USDT", "sell_market_id": "cex:beta:UNI/USDT", "route_mode": "prepositioned_inventory"},
                {"token_symbol": "UNI", "buy_market_id": "cex:alpha:UNI/USDT", "sell_market_id": "dex:eth:swap:0xone:UNI", "route_mode": "prepositioned_inventory"},
            ],
        }
        cex = lambda *_args, **_kwargs: ({"status": "observed", "state_observed_at": "2026-08-01T12:00:00Z"}, [])
        timed_out = collect_route_cohort(
            universe, cex_collector=cex,
            dex_collector=lambda *_args, **_kwargs: self.fail("unresolved chain must not collect"),
            dex_block_resolver=lambda *_args, **_kwargs: (_ for _ in ()).throw(CollectionDeadlineExceeded("collection deadline exceeded")),
            source_generation_reader=lambda: "generation-a",
            expected_source_generation="generation-a",
        )
        timed_leg = next(row for row in timed_out["legs"] if row["market_id"].startswith("dex:"))
        self.assertEqual(timed_leg["status"], "deadline_exceeded")
        self.assertEqual(timed_leg["reason_code"], "route_deadline_exceeded")
        self.assertEqual(
            next(row for row in timed_out["route_rows"] if row["sell_market_id"].startswith("dex:"))["reason_code"],
            "route_deadline_exceeded",
        )
        self.assertEqual(
            next(row for row in timed_out["route_rows"] if row["sell_market_id"] == "cex:beta:UNI/USDT")["timing_status"],
            "within_sla",
        )

        mismatch = collect_route_cohort(
            universe, cex_collector=cex,
            dex_collector=lambda *_args, **_kwargs: ({"status": "observed", "state_observed_at": "2026-08-01T12:00:00Z", "block_number": "999", "block_timestamp": "wrong"}, []),
            dex_block_resolver=lambda *_args, **_kwargs: {"block_number": 123, "block_timestamp": "2026-08-01T12:00:00Z"},
            source_generation_reader=lambda: "generation-a",
            expected_source_generation="generation-a",
        )
        mismatch_leg = next(row for row in mismatch["legs"] if row["market_id"].startswith("dex:"))
        self.assertEqual(mismatch_leg["reason_code"], "fixed_block_lineage_mismatch")

    def test_generation_reader_is_mandatory(self):
        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [
                {"market_id": "cex:alpha:UNI/USDT", "market_type": "cex"},
                {"market_id": "cex:beta:UNI/USDT", "market_type": "cex"},
            ],
            "routes": [{"token_symbol": "UNI", "buy_market_id": "cex:alpha:UNI/USDT", "sell_market_id": "cex:beta:UNI/USDT", "route_mode": "prepositioned_inventory"}],
        }
        for extra in ({}, {"source_generation_reader": lambda: "generation-a"}):
            with self.subTest(extra=sorted(extra)):
                with self.assertRaisesRegex(ValueError, "generation reader is required"):
                    collect_route_cohort(
                        universe,
                        cex_collector=lambda *_args, **_kwargs: self.fail("must not collect"),
                        dex_collector=lambda *_args, **_kwargs: self.fail("must not collect"),
                        **extra,
                    )

    def test_fair_scheduler_respects_global_venue_and_chain_caps(self):
        from threading import Barrier

        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [
                {"market_id": "cex:alpha:UNI/USDT", "market_type": "cex", "exchange": "alpha"},
                {"market_id": "cex:alpha:UNI/USDC", "market_type": "cex", "exchange": "alpha"},
                {"market_id": "dex:eth:swap:0xone:UNI", "market_type": "dex", "chain": "eth"},
                {"market_id": "dex:eth:swap:0xtwo:UNI", "market_type": "dex", "chain": "eth"},
            ],
            "routes": [
                {"token_symbol": "UNI", "buy_market_id": "cex:alpha:UNI/USDT", "sell_market_id": "cex:alpha:UNI/USDC", "route_mode": "prepositioned_inventory"},
                {"token_symbol": "UNI", "buy_market_id": "dex:eth:swap:0xone:UNI", "sell_market_id": "dex:eth:swap:0xtwo:UNI", "route_mode": "atomic_onchain"},
            ],
        }
        barrier = Barrier(2)
        lock = Lock()
        active = {"all": 0, "cex": 0, "dex": 0}
        maximum = dict(active)
        starts = []

        def observe(kind, leg, **kwargs):
            with lock:
                active["all"] += 1
                active[kind] += 1
                maximum["all"] = max(maximum["all"], active["all"])
                maximum[kind] = max(maximum[kind], active[kind])
                starts.append((kind, leg["market_id"]))
                first_pair = len(starts) <= 2
            if first_pair:
                barrier.wait(timeout=1)
            with lock:
                active["all"] -= 1
                active[kind] -= 1
            if kind == "dex":
                return {"status": "observed", "state_observed_at": "2026-08-01T12:00:00Z", "block_number": str(kwargs["fixed_block_number"]), "block_timestamp": kwargs["fixed_block_timestamp"]}
            return {"status": "observed", "state_observed_at": "2026-08-01T12:00:00Z"}

        collect_route_cohort(
            universe,
            cex_collector=lambda leg, **kwargs: observe("cex", leg, **kwargs),
            dex_collector=lambda leg, **kwargs: observe("dex", leg, **kwargs),
            dex_block_resolver=lambda *_args, **_kwargs: {"block_number": 123, "block_timestamp": "2026-08-01T12:00:00Z"},
            source_generation_reader=lambda: "generation-a",
            expected_source_generation="generation-a",
            max_workers=2, cex_workers_per_venue=1, dex_workers_per_chain=1,
        )
        self.assertEqual(maximum, {"all": 2, "cex": 1, "dex": 1})
        self.assertEqual({kind for kind, _market_id in starts[:2]}, {"cex", "dex"})

    def test_cli_dry_run_reads_and_validates_universe_without_collection_or_publication(self):
        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [],
            "routes": [],
        }
        with tempfile.TemporaryDirectory() as directory_name:
            data_dir = Path(directory_name)
            (data_dir / "route_universe.json").write_text(json.dumps(universe), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "selected legs and routes"):
                main([
                    "--data-dir", str(data_dir), "--start", "2026-08-01",
                    "--end", "2026-08-01", "--tokens", "UNI", "--dry-run",
                ])
            self.assertEqual(sorted(path.name for path in data_dir.iterdir()), ["route_universe.json"])

    def test_unique_route_legs_deduplicates_directional_route_references(self):
        routes = [
            {
                "route_id": "route:UNI:cex:alpha:UNI/USDT->dex:eth:swap:0xpool:UNI:prepositioned_inventory",
                "buy_market_id": "cex:alpha:UNI/USDT",
                "sell_market_id": "dex:eth:swap:0xpool:UNI",
            },
            {
                "route_id": "route:UNI:dex:eth:swap:0xpool:UNI->cex:alpha:UNI/USDT:prepositioned_inventory",
                "buy_market_id": "dex:eth:swap:0xpool:UNI",
                "sell_market_id": "cex:alpha:UNI/USDT",
            },
        ]

        self.assertEqual(
            collect_unique_route_legs(routes),
            ["cex:alpha:UNI/USDT", "dex:eth:swap:0xpool:UNI"],
        )

    def test_materialize_route_leg_rows_retains_terminal_deadline_leg(self):
        rows = materialize_route_leg_rows(
            ["cex:alpha:UNI/USDT"],
            {},
            deadline_exceeded={"cex:alpha:UNI/USDT"},
        )

        self.assertEqual(
            rows,
            [{
                "leg_id": "cex:alpha:UNI/USDT",
                "market_id": "cex:alpha:UNI/USDT",
                "status": "deadline_exceeded",
                "available": False,
                "reason_code": "route_deadline_exceeded",
            }],
        )

    def test_collects_each_shared_leg_once_with_per_venue_limit(self):
        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [
                {"market_id": "cex:alpha:UNI/USDT", "market_type": "cex", "exchange": "alpha"},
                {"market_id": "cex:alpha:UNI/USDC", "market_type": "cex", "exchange": "alpha"},
                {"market_id": "cex:beta:UNI/USDT", "market_type": "cex", "exchange": "beta"},
            ],
            "routes": [
                {"token_symbol": "UNI", "buy_market_id": "cex:alpha:UNI/USDT", "sell_market_id": "cex:alpha:UNI/USDC", "route_mode": "prepositioned_inventory"},
                {"token_symbol": "UNI", "buy_market_id": "cex:alpha:UNI/USDC", "sell_market_id": "cex:beta:UNI/USDT", "route_mode": "prepositioned_inventory"},
            ],
        }
        active = {"alpha": 0, "beta": 0}
        maximum = {"alpha": 0, "beta": 0}
        started = []
        lock = Lock()

        def cex_collector(leg, **_kwargs):
            venue = leg["exchange"]
            with lock:
                active[venue] += 1
                maximum[venue] = max(maximum[venue], active[venue])
                started.append(leg["market_id"])
            try:
                return {"status": "observed", "state_observed_at": "2026-08-01T12:00:00Z"}
            finally:
                with lock:
                    active[venue] -= 1

        result = collect_route_cohort(
            universe,
            cex_collector=cex_collector,
            dex_collector=lambda *_args, **_kwargs: self.fail("unexpected DEX collection"),
            max_workers=3,
            cex_workers_per_venue=1,
            source_generation_reader=lambda: "generation-a",
            expected_source_generation="generation-a",
            executor_factory=ThreadPoolExecutor,
        )

        self.assertCountEqual(
            started,
            ["cex:alpha:UNI/USDC", "cex:alpha:UNI/USDT", "cex:beta:UNI/USDT"],
        )
        self.assertEqual(maximum, {"alpha": 1, "beta": 1})
        self.assertEqual(
            [row["market_id"] for row in result["legs"]],
            ["cex:alpha:UNI/USDC", "cex:alpha:UNI/USDT", "cex:beta:UNI/USDT"],
        )

    def test_same_chain_dex_legs_receive_one_resolved_fixed_block(self):
        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [
                {"market_id": "dex:eth:swap:0xone:UNI", "market_type": "dex", "chain": "eth"},
                {"market_id": "dex:eth:swap:0xtwo:UNI", "market_type": "dex", "chain": "eth"},
                {"market_id": "dex:arb:swap:0xthree:UNI", "market_type": "dex", "chain": "arb"},
            ],
            "routes": [
                {"token_symbol": "UNI", "buy_market_id": "dex:eth:swap:0xone:UNI", "sell_market_id": "dex:eth:swap:0xtwo:UNI", "route_mode": "atomic_onchain"},
            ],
        }
        resolved = []
        received = {}

        def resolve_block(chain, **_kwargs):
            resolved.append(chain)
            return {"block_number": 101 if chain == "eth" else 202, "block_timestamp": "2026-08-01T12:00:00Z"}

        def dex_collector(leg, **kwargs):
            received[leg["market_id"]] = (
                kwargs["fixed_block_number"], kwargs["fixed_block_timestamp"],
            )
            return {"status": "observed", "state_observed_at": "2026-08-01T12:00:00Z"}

        collect_route_cohort(
            universe,
            cex_collector=lambda *_args, **_kwargs: self.fail("unexpected CEX collection"),
            dex_collector=dex_collector,
            dex_block_resolver=resolve_block,
            source_generation_reader=lambda: "generation-a",
            expected_source_generation="generation-a",
            max_workers=3,
        )

        self.assertEqual(resolved, ["eth"])
        self.assertEqual(received["dex:eth:swap:0xone:UNI"], (101, "2026-08-01T12:00:00Z"))
        self.assertEqual(received["dex:eth:swap:0xtwo:UNI"], (101, "2026-08-01T12:00:00Z"))
        self.assertNotIn("dex:arb:swap:0xthree:UNI", received)

    def test_dex_collection_fails_closed_without_fixed_block_resolver(self):
        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [
                {"market_id": "dex:eth:swap:0xone:UNI", "market_type": "dex"},
                {"market_id": "dex:eth:swap:0xtwo:UNI", "market_type": "dex"},
            ],
            "routes": [
                {"token_symbol": "UNI", "buy_market_id": "dex:eth:swap:0xone:UNI", "sell_market_id": "dex:eth:swap:0xtwo:UNI", "route_mode": "atomic_onchain"},
            ],
        }

        with self.assertRaisesRegex(ValueError, "fixed block resolver"):
            collect_route_cohort(
                universe,
                cex_collector=lambda *_args, **_kwargs: self.fail("unexpected CEX collection"),
                dex_collector=lambda *_args, **_kwargs: self.fail("must not collect without a fixed block"),
                source_generation_reader=lambda: "generation-a",
                expected_source_generation="generation-a",
            )

    def test_deadline_terminal_leg_only_makes_its_routes_unavailable(self):
        from scripts.collection_deadline import CollectionDeadlineExceeded

        good = "2026-08-01T12:00:00Z"
        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [
                {"market_id": "cex:alpha:UNI/USDT", "market_type": "cex"},
                {"market_id": "cex:beta:UNI/USDT", "market_type": "cex"},
                {"market_id": "cex:gamma:UNI/USDT", "market_type": "cex"},
            ],
            "routes": [
                {"token_symbol": "UNI", "buy_market_id": "cex:alpha:UNI/USDT", "sell_market_id": "cex:beta:UNI/USDT", "route_mode": "prepositioned_inventory"},
                {"token_symbol": "UNI", "buy_market_id": "cex:alpha:UNI/USDT", "sell_market_id": "cex:gamma:UNI/USDT", "route_mode": "prepositioned_inventory"},
            ],
        }

        def cex_collector(leg, **_kwargs):
            if leg["market_id"] == "cex:beta:UNI/USDT":
                raise CollectionDeadlineExceeded("collection deadline exceeded")
            return {"status": "observed", "state_observed_at": good}

        result = collect_route_cohort(
            universe,
            cex_collector=cex_collector,
            dex_collector=lambda *_args, **_kwargs: self.fail("unexpected DEX collection"),
            source_generation_reader=lambda: "generation-a",
            expected_source_generation="generation-a",
        )

        timing = {row["route_id"]: row for row in result["route_rows"]}
        self.assertEqual(
            timing["route:UNI:cex:alpha:UNI/USDT->cex:beta:UNI/USDT:prepositioned_inventory"]["reason_code"],
            "route_deadline_exceeded",
        )
        self.assertEqual(
            timing["route:UNI:cex:alpha:UNI/USDT->cex:gamma:UNI/USDT:prepositioned_inventory"]["timing_status"],
            "within_sla",
        )

    def test_reverse_completion_orders_produce_identical_normalized_cohort(self):
        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [
                {"market_id": "cex:alpha:UNI/USDT", "market_type": "cex"},
                {"market_id": "cex:beta:UNI/USDT", "market_type": "cex"},
            ],
            "routes": [
                {"token_symbol": "UNI", "buy_market_id": "cex:alpha:UNI/USDT", "sell_market_id": "cex:beta:UNI/USDT", "route_mode": "prepositioned_inventory"},
            ],
        }

        def collect_with_delays(delays):
            def cex_collector(leg, **_kwargs):
                time.sleep(delays[leg["market_id"]])
                return {"status": "observed", "state_observed_at": "2026-08-01T12:00:00Z"}

            with tempfile.TemporaryDirectory() as directory_name:
                return collect_route_cohort(
                    universe,
                    cex_collector=cex_collector,
                    dex_collector=lambda *_args, **_kwargs: self.fail("unexpected DEX collection"),
                    max_workers=2,
                    target_observed_at="2026-08-01T12:00:00Z",
                    source_generation_reader=lambda: "generation-a",
                    expected_source_generation="generation-a",
                    raw_root=Path(directory_name), snapshot_id="stable-run",
                    wall_clock=lambda: datetime(
                        2026, 8, 1, 12, tzinfo=timezone.utc
                    ),
                )

        alpha_first = collect_with_delays({"cex:alpha:UNI/USDT": 0, "cex:beta:UNI/USDT": 0.02})
        beta_first = collect_with_delays({"cex:alpha:UNI/USDT": 0.02, "cex:beta:UNI/USDT": 0})

        self.assertEqual(alpha_first["legs"], beta_first["legs"])
        self.assertEqual(alpha_first["route_rows"], beta_first["route_rows"])
        self.assertEqual(alpha_first["fingerprint"], beta_first["fingerprint"])

    def test_source_generation_change_during_collection_fails_closed(self):
        universe = {
            "candidate_source_generation": "generation-a",
            "selected_legs": [
                {"market_id": "cex:alpha:UNI/USDT", "market_type": "cex"},
                {"market_id": "cex:beta:UNI/USDT", "market_type": "cex"},
            ],
            "routes": [
                {"token_symbol": "UNI", "buy_market_id": "cex:alpha:UNI/USDT", "sell_market_id": "cex:beta:UNI/USDT", "route_mode": "prepositioned_inventory"},
            ],
        }
        current_generation = ["generation-a"]

        def cex_collector(_leg, **_kwargs):
            current_generation[0] = "generation-b"
            return {"status": "observed", "state_observed_at": "2026-08-01T12:00:00Z"}

        with tempfile.TemporaryDirectory() as directory_name:
            raw_root = Path(directory_name)
            with self.assertRaisesRegex(ValueError, "collection input generation changed"):
                collect_route_cohort(
                    universe,
                    cex_collector=cex_collector,
                    dex_collector=lambda *_args, **_kwargs: self.fail("unexpected DEX collection"),
                    source_generation_reader=lambda: current_generation[0],
                    expected_source_generation="generation-a",
                    raw_root=raw_root,
                )
            accepted = list(raw_root.glob("*/accepted/*"))
            self.assertEqual(accepted, [])


if __name__ == "__main__":
    unittest.main()
