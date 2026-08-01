"""Tests for immutable publication of normalized route-cohort bundles."""

from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path
import shutil
import sqlite3
import tempfile
import unittest
from unittest.mock import patch

from scripts.route_publication import (
    build_route_cohort_sqlite,
    load_latest_route_cohort,
    publish_route_cohort_bundle,
    validate_route_cohort_bundle,
)
import scripts.route_publication as route_publication


def _canonical_sha256(value):
    return hashlib.sha256(json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")).hexdigest()


def _route(token_symbol, buy_market_id, sell_market_id):
    route_mode = "prepositioned_inventory"
    route_id = "route:{}:{}->{}:{}".format(
        token_symbol, buy_market_id, sell_market_id, route_mode
    )
    return {
        "token_symbol": token_symbol,
        "buy_market_id": buy_market_id,
        "sell_market_id": sell_market_id,
        "route_mode": route_mode,
        "route_id": route_id,
        "route_class": "candidate",
        "settlement_reason": None,
        "requested_notionals_usd": [1000, 5000, 10000, 50000, 100000],
        "candidate_source_generation": "candidate-generation-a",
    }


def _cohort():
    alpha = "cex:alpha:UNI/USDT"
    beta = "cex:beta:UNI/USDT"
    routes = [
        _route("UNI", alpha, beta),
        _route("UNI", beta, alpha),
    ]
    legs = [
        {
            "leg_id": alpha,
            "market_id": alpha,
            "market_type": "cex",
            "token_symbol": "UNI",
            "status": "observed",
            "available": True,
            "reason_code": None,
            "state_observed_at": "2026-08-01T12:00:01.000000000Z",
            "snapshot_id": "snapshot-a",
            "source_endpoint": "https://api.alpha.example/orderbook",
            "raw_response_sha256": "a" * 64,
        },
        {
            "leg_id": beta,
            "market_id": beta,
            "market_type": "cex",
            "token_symbol": "UNI",
            "status": "observed",
            "available": True,
            "reason_code": None,
            "state_observed_at": "2026-08-01T12:00:02.000000000Z",
            "snapshot_id": "snapshot-a",
            "source_endpoint": "https://api.beta.example/orderbook",
            "raw_response_sha256": "b" * 64,
        },
    ]
    route_rows = [
        {
            **route,
            "validated_at": "2026-08-01T12:00:03Z",
            "skew_seconds": "1.000000000",
            "timing_status": "within_sla",
            "reason_code": None,
        }
        for route in routes
    ]
    cohort = {
        "schema": "route_cohort_collection/v1",
        "candidate_source_generation": "candidate-generation-a",
        "collection_input_generation": "collection-generation-a",
        "source_state": {
            "candidate_source_generation": "candidate-generation-a",
            "collection_input_generation": "collection-generation-a",
        },
        "raw_evidence_run_id": "snapshot-a",
        "target_observed_at": "2026-08-01T12:00:00Z",
        "collection_started_at": "2026-08-01T12:00:00Z",
        "collection_completed_at": "2026-08-01T12:00:03Z",
        "collection_deadline_at": "2026-08-01T12:01:00Z",
        "skew_sla_seconds": "60",
        "route_age_sla_seconds": "120",
        "selection_window": {
            "start": "2026-07-25",
            "end": "2026-08-01",
        },
        "requested_notionals_usd": [1000, 5000, 10000, 50000, 100000],
        "legs": sorted(legs, key=lambda row: row["market_id"]),
        "routes": sorted(routes, key=lambda row: row["route_id"]),
        "route_rows": sorted(route_rows, key=lambda row: row["route_id"]),
    }
    cohort["route_cohort_id"] = "cohort:" + _canonical_sha256(cohort)
    cohort["fingerprint"] = _canonical_sha256(cohort)
    return cohort


def _rehash(cohort):
    value = copy.deepcopy(cohort)
    for field, key in (
        ("routes", "route_id"),
        ("legs", "market_id"),
        ("route_rows", "route_id"),
    ):
        value[field] = sorted(value[field], key=lambda row: row[key])
    value.pop("route_cohort_id", None)
    value.pop("fingerprint", None)
    value["route_cohort_id"] = "cohort:" + _canonical_sha256(value)
    value["fingerprint"] = _canonical_sha256(value)
    return value


def _second_cohort():
    cohort = _cohort()
    cohort["raw_evidence_run_id"] = "snapshot-b"
    for leg in cohort["legs"]:
        leg["snapshot_id"] = "snapshot-b"
    return _rehash(cohort)


def _third_cohort():
    cohort = _cohort()
    cohort["raw_evidence_run_id"] = "snapshot-c"
    for leg in cohort["legs"]:
        leg["snapshot_id"] = "snapshot-c"
    return _rehash(cohort)


def _refresh_database_hash_in_manifest(bundle):
    database = bundle / "route_cohort.sqlite3"
    manifest_path = bundle / "manifest.json"
    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    manifest["files"]["route_cohort.sqlite3"]["sha256"] = hashlib.sha256(
        database.read_bytes()
    ).hexdigest()
    manifest_path.write_text(
        json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _rewrite_route_legs_schema(
    bundle,
    *,
    market_definition="market_id TEXT PRIMARY KEY NOT NULL",
    status_definition="status TEXT NOT NULL",
    table_primary_key=None,
    without_rowid=True,
):
    database = bundle / "route_cohort.sqlite3"
    connection = sqlite3.connect(str(database))
    try:
        connection.execute("PRAGMA foreign_keys = OFF")
        connection.execute("DROP INDEX route_legs_token_idx")
        connection.execute("ALTER TABLE route_legs RENAME TO route_legs_old")
        definitions = [
            "route_cohort_id TEXT NOT NULL",
            "leg_id TEXT NOT NULL",
            market_definition,
            "market_type TEXT NOT NULL",
            "token_symbol TEXT NOT NULL",
            status_definition,
            "available INTEGER",
            "reason_code TEXT NOT NULL",
            "state_observed_at TEXT NOT NULL",
            "snapshot_id TEXT NOT NULL",
            "source_endpoint TEXT NOT NULL",
            "raw_response_sha256 TEXT NOT NULL",
            "fixed_block_number TEXT NOT NULL",
            "fixed_block_timestamp TEXT NOT NULL",
            "row_json TEXT NOT NULL",
        ]
        if table_primary_key is not None:
            definitions.append(table_primary_key)
        connection.execute(
            "CREATE TABLE route_legs ({}){}".format(
                ",".join(definitions),
                " WITHOUT ROWID" if without_rowid else "",
            )
        )
        columns = ",".join(route_publication.LEG_COLUMNS)
        connection.execute(
            "INSERT INTO route_legs ({0}) SELECT {0} FROM route_legs_old".format(
                columns
            )
        )
        connection.execute("DROP TABLE route_legs_old")
        connection.execute(
            "CREATE INDEX route_legs_token_idx "
            "ON route_legs(token_symbol, market_id)"
        )
        connection.commit()
    finally:
        connection.close()


def _rewrite_route_timing_without_foreign_key(bundle):
    database = bundle / "route_cohort.sqlite3"
    connection = sqlite3.connect(str(database))
    try:
        connection.execute("PRAGMA foreign_keys = OFF")
        connection.execute("DROP INDEX route_timing_status_idx")
        connection.execute("ALTER TABLE route_timing RENAME TO route_timing_old")
        connection.execute(
            """
            CREATE TABLE route_timing (
                route_cohort_id TEXT NOT NULL,
                route_id TEXT PRIMARY KEY NOT NULL,
                skew_seconds TEXT NOT NULL,
                timing_status TEXT NOT NULL,
                reason_code TEXT NOT NULL,
                row_json TEXT NOT NULL
            ) WITHOUT ROWID
            """
        )
        columns = ",".join(route_publication.TIMING_COLUMNS)
        connection.execute(
            "INSERT INTO route_timing ({0}) "
            "SELECT {0} FROM route_timing_old".format(columns)
        )
        connection.execute("DROP TABLE route_timing_old")
        connection.execute(
            "CREATE INDEX route_timing_status_idx "
            "ON route_timing(timing_status, route_id)"
        )
        connection.commit()
    finally:
        connection.close()


def _dex_cohort(block_numbers=("100", "100")):
    cohort = _cohort()
    first = "dex:eth:uniswap:0xaaa:UNI"
    second = "dex:eth:uniswap:0xbbb:UNI"

    def route(buy, sell):
        identity = {
            "token_symbol": "UNI",
            "buy_market_id": buy,
            "sell_market_id": sell,
            "route_mode": "atomic_onchain",
        }
        return {
            **identity,
            "route_id": "route:UNI:{}->{}:atomic_onchain".format(buy, sell),
            "route_class": "candidate",
            "settlement_reason": None,
            "requested_notionals_usd": [1000, 5000, 10000, 50000, 100000],
            "candidate_source_generation": "candidate-generation-a",
        }

    routes = [route(first, second), route(second, first)]
    cohort["routes"] = routes
    cohort["legs"] = [
        {
            "leg_id": market_id,
            "market_id": market_id,
            "market_type": "dex",
            "token_symbol": "UNI",
            "status": "observed",
            "available": True,
            "reason_code": None,
            "state_observed_at": observed_at,
            "snapshot_id": "snapshot-a",
            "source_endpoint": "https://rpc.example/eth",
            "raw_response_sha256": raw_hash * 64,
            "fixed_block_number": block_number,
            "fixed_block_timestamp": "2026-08-01T11:59:59Z",
        }
        for market_id, observed_at, raw_hash, block_number in (
            (first, "2026-08-01T12:00:01Z", "a", block_numbers[0]),
            (second, "2026-08-01T12:00:02Z", "b", block_numbers[1]),
        )
    ]
    cohort["route_rows"] = [
        {
            **candidate,
            "validated_at": "2026-08-01T12:00:03Z",
            "skew_seconds": "1",
            "timing_status": "within_sla",
            "reason_code": None,
        }
        for candidate in routes
    ]
    return _rehash(cohort)


def _dex_cohort_with_lineage_conflict():
    return _dex_cohort(("100", "101"))


class TemporaryRouteRootTestCase(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / "data/local/routes/core"


class RoutePublicationInterfaceTests(unittest.TestCase):
    def test_task_five_publication_interfaces_exist(self):
        from scripts.route_publication import (
            build_route_cohort_sqlite,
            load_latest_route_cohort,
            publish_route_cohort_bundle,
            validate_route_cohort_bundle,
        )

        self.assertTrue(callable(build_route_cohort_sqlite))
        self.assertTrue(callable(validate_route_cohort_bundle))
        self.assertTrue(callable(publish_route_cohort_bundle))
        self.assertTrue(callable(load_latest_route_cohort))

    def test_private_tmp_alias_normalization_is_darwin_only(self):
        with patch("scripts.route_publication.sys.platform", "linux"):
            self.assertEqual(
                route_publication._absolute_without_symlink_resolution(
                    Path("/tmp/route-core")
                ),
                Path("/tmp/route-core"),
            )


class DeterministicRoutePublicationTests(TemporaryRouteRootTestCase):
    def test_shuffled_rows_publish_identical_five_file_bundles(self):
        first_root = self.root / "first"
        second_root = self.root / "second"
        cohort = _cohort()
        shuffled = copy.deepcopy(cohort)
        for field in ("routes", "legs", "route_rows"):
            shuffled[field].reverse()

        first_pointer = publish_route_cohort_bundle(cohort, core_root=first_root)
        second_pointer = publish_route_cohort_bundle(
            shuffled, core_root=second_root
        )

        self.assertEqual(first_pointer, second_pointer)
        cohort_id = cohort["route_cohort_id"]
        first_bundle = first_root / "bundles" / cohort_id
        second_bundle = second_root / "bundles" / cohort_id
        expected_files = {
            "manifest.json",
            "route_candidates.csv",
            "route_cohort.sqlite3",
            "route_legs.csv",
            "route_timing.csv",
        }
        self.assertEqual(
            {path.name for path in first_bundle.iterdir()}, expected_files
        )
        self.assertEqual(
            {path.name for path in second_bundle.iterdir()}, expected_files
        )
        for filename in expected_files:
            self.assertEqual(
                (first_bundle / filename).read_bytes(),
                (second_bundle / filename).read_bytes(),
                filename,
            )

        first = validate_route_cohort_bundle(first_bundle)
        second = validate_route_cohort_bundle(second_bundle)
        self.assertEqual(first["manifest"], second["manifest"])
        self.assertEqual(first["candidates"], second["candidates"])
        self.assertEqual(first["legs"], second["legs"])
        self.assertEqual(first["timing"], second["timing"])
        self.assertEqual(
            first["manifest"]["files"]["route_cohort.sqlite3"][
                "logical_sha256"
            ],
            second["manifest"]["files"]["route_cohort.sqlite3"][
                "logical_sha256"
            ],
        )

    def test_latest_loader_cross_validates_csv_sqlite_manifest_and_pointer(self):
        cohort = _cohort()
        pointer = publish_route_cohort_bundle(cohort, core_root=self.root)

        loaded = load_latest_route_cohort(self.root)

        self.assertEqual(loaded["pointer"], pointer)
        self.assertEqual(loaded["manifest"]["bundle_stage"], "route_cohort_core/v1")
        self.assertEqual(
            {row["route_id"] for row in loaded["candidates"]},
            {row["route_id"] for row in cohort["routes"]},
        )
        self.assertEqual(
            {row["market_id"] for row in loaded["legs"]},
            {row["market_id"] for row in cohort["legs"]},
        )
        self.assertEqual(
            {row["route_id"] for row in loaded["timing"]},
            {row["route_id"] for row in cohort["route_rows"]},
        )

    def test_public_complete_pointer_is_never_created_or_replaced(self):
        public_pointer = self.root.parent / "latest.json"
        public_pointer.parent.mkdir(parents=True)
        sentinel = b'{"schema":"complete-sentinel"}\n'
        public_pointer.write_bytes(sentinel)
        public_bundles = self.root.parent / "bundles"
        public_bundles.mkdir()
        bundle_sentinel = public_bundles / "complete-sentinel"
        bundle_sentinel.write_bytes(b"complete bundle sentinel\n")

        publish_route_cohort_bundle(_cohort(), core_root=self.root)

        self.assertEqual(public_pointer.read_bytes(), sentinel)
        self.assertEqual(
            bundle_sentinel.read_bytes(), b"complete bundle sentinel\n"
        )
        self.assertEqual(list(public_bundles.iterdir()), [bundle_sentinel])

    def test_build_sqlite_returns_stable_logical_fingerprint(self):
        cohort = _cohort()
        shuffled = copy.deepcopy(cohort)
        for field in ("routes", "legs", "route_rows"):
            shuffled[field].reverse()
        first = Path(self.temporary.name) / "first.sqlite3"
        second = Path(self.temporary.name) / "second.sqlite3"

        first_logical = build_route_cohort_sqlite(first, cohort)
        second_logical = build_route_cohort_sqlite(second, shuffled)

        self.assertEqual(first_logical, second_logical)
        self.assertEqual(first.read_bytes(), second.read_bytes())

    def test_canonical_utc_nanoseconds_are_preserved_exactly(self):
        cohort = _dex_cohort()
        cohort["target_observed_at"] = "2026-08-01T11:59:58.123456789Z"
        cohort["legs"][0][
            "state_observed_at"
        ] = "2026-08-01T12:00:01.123456789Z"
        cohort["legs"][1][
            "state_observed_at"
        ] = "2026-08-01T12:00:02.123456789Z"
        for leg in cohort["legs"]:
            leg["fixed_block_timestamp"] = "2026-08-01T11:59:59.123456789Z"
        for row in cohort["route_rows"]:
            row["skew_seconds"] = "1.000000000"
        cohort = _rehash(cohort)

        publish_route_cohort_bundle(cohort, core_root=self.root)
        loaded = load_latest_route_cohort(self.root)["cohort"]

        self.assertEqual(
            loaded["target_observed_at"],
            "2026-08-01T11:59:58.123456789Z",
        )
        self.assertEqual(
            loaded["legs"][0]["state_observed_at"],
            "2026-08-01T12:00:01.123456789Z",
        )
        self.assertEqual(
            loaded["legs"][0]["fixed_block_timestamp"],
            "2026-08-01T11:59:59.123456789Z",
        )

    def test_raw_evidence_path_unsafe_terminal_reason_round_trips(self):
        cohort = _cohort()
        failed_market = cohort["legs"][0]["market_id"]
        cohort["legs"][0]["status"] = "failed"
        cohort["legs"][0]["available"] = False
        cohort["legs"][0]["reason_code"] = "raw_evidence_path_unsafe"
        for row in cohort["route_rows"]:
            row["skew_seconds"] = None
            row["timing_status"] = "unavailable"
            row["reason_code"] = (
                "buy_leg_unavailable"
                if row["buy_market_id"] == failed_market
                else "sell_leg_unavailable"
            )
        cohort = _rehash(cohort)

        publish_route_cohort_bundle(cohort, core_root=self.root)
        loaded = load_latest_route_cohort(self.root)["cohort"]

        failed = next(
            leg for leg in loaded["legs"] if leg["market_id"] == failed_market
        )
        self.assertEqual(failed["reason_code"], "raw_evidence_path_unsafe")


class RoutePublicationFailureTests(TemporaryRouteRootTestCase):
    def test_duplicate_route_identity_fails_closed(self):
        cohort = _cohort()
        cohort["routes"].append(copy.deepcopy(cohort["routes"][0]))

        with self.assertRaisesRegex(ValueError, "duplicate route candidate"):
            publish_route_cohort_bundle(cohort, core_root=self.root)

        self.assertFalse(self.root.exists())

    def test_route_and_timing_enum_drift_fail_closed(self):
        route_drift = _cohort()
        route_drift["routes"][0]["route_class"] = "strict"
        with self.assertRaisesRegex(ValueError, "enum"):
            publish_route_cohort_bundle(route_drift, core_root=self.root)

        timing_drift = _cohort()
        timing_drift["route_rows"][0]["timing_status"] = "stale"
        with self.assertRaisesRegex(ValueError, "timing status enum"):
            publish_route_cohort_bundle(timing_drift, core_root=self.root)

        leg_drift = _cohort()
        leg_drift["legs"][0]["reason_code"] = "future_reason_code"
        with self.assertRaisesRegex(ValueError, "leg reason enum"):
            publish_route_cohort_bundle(leg_drift, core_root=self.root)

        status_drift = _cohort()
        status_drift["legs"][0]["status"] = "future_status"
        with self.assertRaisesRegex(ValueError, "leg status enum"):
            publish_route_cohort_bundle(status_drift, core_root=self.root)

    def test_forged_exact_skew_and_future_state_time_fail_closed(self):
        forged_skew = _cohort()
        forged_skew["legs"][0][
            "state_observed_at"
        ] = "2026-08-01T12:00:00.000000000Z"
        forged_skew["legs"][1][
            "state_observed_at"
        ] = "2026-08-01T12:01:00.000000001Z"
        forged_skew["collection_completed_at"] = "2026-08-01T12:01:01Z"
        forged_skew["collection_deadline_at"] = "2026-08-01T12:02:00Z"
        for row in forged_skew["route_rows"]:
            row["validated_at"] = "2026-08-01T12:01:01Z"
            row["skew_seconds"] = "60.000000001"
            row["timing_status"] = "within_sla"
            row["reason_code"] = None
        forged_skew = _rehash(forged_skew)
        with self.assertRaisesRegex(ValueError, "timing classification"):
            publish_route_cohort_bundle(forged_skew, core_root=self.root)

        future_state = _cohort()
        future_state["collection_completed_at"] = "2026-08-01T12:00:00.5Z"
        for row in future_state["route_rows"]:
            row["validated_at"] = "2026-08-01T12:00:00.5Z"
        future_state = _rehash(future_state)
        with self.assertRaisesRegex(ValueError, "state timestamp is in the future"):
            publish_route_cohort_bundle(future_state, core_root=self.root)

    def test_candidate_and_collection_source_generation_conflicts_fail_closed(self):
        candidate_conflict = _cohort()
        candidate_conflict["routes"][0][
            "candidate_source_generation"
        ] = "candidate-generation-b"
        with self.assertRaisesRegex(ValueError, "candidate source lineage"):
            publish_route_cohort_bundle(candidate_conflict, core_root=self.root)

        collection_conflict = _cohort()
        collection_conflict["source_state"][
            "collection_input_generation"
        ] = "collection-generation-b"
        with self.assertRaisesRegex(ValueError, "source lineage"):
            publish_route_cohort_bundle(collection_conflict, core_root=self.root)

    def test_incomplete_route_pair_fails_closed(self):
        cohort = _cohort()
        cohort["legs"].pop()

        with self.assertRaisesRegex(ValueError, "route pair is incomplete"):
            publish_route_cohort_bundle(cohort, core_root=self.root)

    def test_malformed_market_identity_is_rejected_before_bundle_writes(self):
        cohort = _cohort()
        original = "cex:alpha:UNI/USDT"
        malformed = "cex::"
        for leg in cohort["legs"]:
            if leg["market_id"] == original:
                leg["market_id"] = malformed
                leg["leg_id"] = malformed
        for row in cohort["routes"] + cohort["route_rows"]:
            if row["buy_market_id"] == original:
                row["buy_market_id"] = malformed
            if row["sell_market_id"] == original:
                row["sell_market_id"] = malformed
            row["route_id"] = "route:{}:{}->{}:{}".format(
                row["token_symbol"],
                row["buy_market_id"],
                row["sell_market_id"],
                row["route_mode"],
            )
        cohort = _rehash(cohort)

        with self.assertRaisesRegex(ValueError, "market identity"):
            publish_route_cohort_bundle(cohort, core_root=self.root)

        self.assertFalse(self.root.exists())

    def test_float_coercion_cannot_satisfy_the_exact_notional_grid(self):
        cohort = _cohort()
        cohort["requested_notionals_usd"][0] = 1000.0
        for row in cohort["routes"] + cohort["route_rows"]:
            row["requested_notionals_usd"][0] = 1000.0
        cohort = _rehash(cohort)

        with self.assertRaisesRegex(ValueError, "notional grid"):
            publish_route_cohort_bundle(cohort, core_root=self.root)

    def test_fixed_block_lineage_conflict_fails_closed(self):
        with self.assertRaisesRegex(ValueError, "fixed block lineage conflict"):
            publish_route_cohort_bundle(
                _dex_cohort_with_lineage_conflict(), core_root=self.root
            )

    def test_observed_dex_leg_requires_complete_fixed_block_lineage(self):
        cohort = _dex_cohort()
        cohort["legs"][0].pop("fixed_block_number")
        cohort["legs"][0].pop("fixed_block_timestamp")
        cohort = _rehash(cohort)

        with self.assertRaisesRegex(ValueError, "observed DEX.*fixed block"):
            publish_route_cohort_bundle(cohort, core_root=self.root)

    def test_terminal_dex_leg_cannot_drop_chain_fixed_block_lineage(self):
        cohort = _dex_cohort()
        terminal = cohort["legs"][0]
        terminal["status"] = "failed"
        terminal["available"] = False
        terminal["reason_code"] = "collection_failed"
        terminal.pop("fixed_block_number")
        terminal.pop("fixed_block_timestamp")
        cohort = _rehash(cohort)

        with self.assertRaisesRegex(
            ValueError, "DEX chain fixed block lineage is incomplete"
        ):
            publish_route_cohort_bundle(cohort, core_root=self.root)

    def test_fixed_block_timestamp_cannot_exceed_collection_bound(self):
        after_completion = _dex_cohort()
        for leg in after_completion["legs"]:
            leg["fixed_block_timestamp"] = "2026-08-01T12:00:04.000000001Z"

        after_earlier_deadline = _dex_cohort()
        after_earlier_deadline[
            "collection_deadline_at"
        ] = "2026-08-01T12:00:00Z"
        for leg in after_earlier_deadline["legs"]:
            leg["fixed_block_timestamp"] = "2026-08-01T12:00:00.000000001Z"

        for label, cohort in (
            ("completion-bound", _rehash(after_completion)),
            ("deadline-bound", _rehash(after_earlier_deadline)),
        ):
            with self.subTest(label=label):
                with self.assertRaisesRegex(
                    ValueError, "fixed block timestamp exceeds collection bound"
                ):
                    publish_route_cohort_bundle(cohort, core_root=self.root)

    def test_all_metadata_timestamps_require_canonical_utc_z(self):
        cases = []

        top_level = _cohort()
        top_level["target_observed_at"] = "2026-08-01T20:00:00+08:00"
        cases.append(("top-level", _rehash(top_level)))

        leg_level = _cohort()
        leg_level["legs"][0][
            "state_observed_at"
        ] = "2026-08-01T20:00:01.000000000+08:00"
        cases.append(("leg", _rehash(leg_level)))

        fixed_block = _dex_cohort()
        for leg in fixed_block["legs"]:
            leg["fixed_block_timestamp"] = "2026-08-01T19:59:59+08:00"
        cases.append(("fixed-block", _rehash(fixed_block)))

        for label, cohort in cases:
            with self.subTest(label=label):
                with self.assertRaisesRegex(ValueError, "canonical UTC timestamp"):
                    publish_route_cohort_bundle(cohort, core_root=self.root)

    def test_collection_timeline_bounds_fail_closed(self):
        deadline_before_start = _cohort()
        deadline_before_start[
            "target_observed_at"
        ] = "2026-08-01T11:59:58Z"
        deadline_before_start[
            "collection_deadline_at"
        ] = "2026-08-01T11:59:59Z"

        target_after_deadline = _cohort()
        target_after_deadline[
            "target_observed_at"
        ] = "2026-08-01T12:01:00.000000001Z"

        cases = (
            ("deadline-before-start", _rehash(deadline_before_start)),
            ("target-after-deadline", _rehash(target_after_deadline)),
        )
        for label, cohort in cases:
            with self.subTest(label=label):
                with self.assertRaisesRegex(
                    ValueError, "route cohort collection timeline is invalid"
                ):
                    publish_route_cohort_bundle(cohort, core_root=self.root)

    def test_selection_window_requires_strict_ordered_iso_dates(self):
        malformed = _cohort()
        malformed["selection_window"]["start"] = "2026-7-25"
        reversed_window = _cohort()
        reversed_window["selection_window"] = {
            "start": "2026-08-02",
            "end": "2026-08-01",
        }

        for label, cohort in (
            ("malformed", _rehash(malformed)),
            ("reversed", _rehash(reversed_window)),
        ):
            with self.subTest(label=label):
                with self.assertRaisesRegex(ValueError, "selection window"):
                    publish_route_cohort_bundle(cohort, core_root=self.root)

    def test_nested_endpoint_credentials_and_local_paths_fail_closed(self):
        unsafe_values = (
            {"endpoint": "https://user:pass@example.test/rpc?api_key=secret"},
            {"cache_path": "/private/tmp/raw-response.json"},
        )
        for provenance in unsafe_values:
            with self.subTest(provenance=provenance):
                cohort = _cohort()
                cohort["legs"][0]["provenance"] = provenance
                cohort = _rehash(cohort)
                with self.assertRaisesRegex(ValueError, "unsafe evidence"):
                    publish_route_cohort_bundle(cohort, core_root=self.root)

    def test_task4_unsafe_evidence_forms_fail_closed(self):
        unsafe_mutations = {
            "file-uri": lambda leg: leg.update(
                {"provenance": {"uri": "file:///private/tmp/raw.json"}}
            ),
            "relative-parent": lambda leg: leg.update(
                {"provenance": "../private/raw.json"}
            ),
            "private-key": lambda leg: leg.update({"private_key": "secret"}),
            "cookie": lambda leg: leg.update({"cookie": "session=secret"}),
            "session": lambda leg: leg.update({"session_id": "secret"}),
            "credential-uri": lambda leg: leg.update(
                {"provenance": "ssh://user:secret@example.test/private"}
            ),
            "invalid-port": lambda leg: leg.update(
                {"source_endpoint": "https://example.test:99999/orderbook"}
            ),
        }
        for label, mutate in unsafe_mutations.items():
            with self.subTest(label=label):
                cohort = _cohort()
                mutate(cohort["legs"][0])
                cohort = _rehash(cohort)
                with self.assertRaisesRegex(ValueError, "unsafe evidence"):
                    publish_route_cohort_bundle(cohort, core_root=self.root)

    def test_terminal_nonempty_raw_hash_must_be_lowercase_sha256(self):
        for label, invalid_hash in (
            ("uppercase", "A" * 64),
            ("short", "a" * 63),
            ("nonhex", "g" * 64),
        ):
            with self.subTest(label=label):
                cohort = _cohort()
                failed_market = cohort["legs"][0]["market_id"]
                cohort["legs"][0]["status"] = "failed"
                cohort["legs"][0]["available"] = False
                cohort["legs"][0]["reason_code"] = "collection_failed"
                cohort["legs"][0]["raw_response_sha256"] = invalid_hash
                for row in cohort["route_rows"]:
                    row["skew_seconds"] = None
                    row["timing_status"] = "unavailable"
                    row["reason_code"] = (
                        "buy_leg_unavailable"
                        if row["buy_market_id"] == failed_market
                        else "sell_leg_unavailable"
                    )
                cohort = _rehash(cohort)

                with self.assertRaisesRegex(ValueError, "raw evidence hash"):
                    publish_route_cohort_bundle(cohort, core_root=self.root)

    def test_dex_pool_identity_rejects_path_like_components(self):
        cohort = _dex_cohort()
        original = "dex:eth:uniswap:0xaaa:UNI"
        malformed = "dex:eth:uniswap:../x:UNI"
        for leg in cohort["legs"]:
            if leg["market_id"] == original:
                leg["market_id"] = malformed
                leg["leg_id"] = malformed
        for row in cohort["routes"] + cohort["route_rows"]:
            if row["buy_market_id"] == original:
                row["buy_market_id"] = malformed
            if row["sell_market_id"] == original:
                row["sell_market_id"] = malformed
            row["route_id"] = "route:{}:{}->{}:{}".format(
                row["token_symbol"],
                row["buy_market_id"],
                row["sell_market_id"],
                row["route_mode"],
            )
        cohort = _rehash(cohort)

        with self.assertRaisesRegex(ValueError, "market identity"):
            publish_route_cohort_bundle(cohort, core_root=self.root)

    def test_market_id_component_lengths_exactly_match_task4(self):
        invalid_ids = (
            "cex:{}:UNI/USDT".format("x" * 65),
            "cex:alpha:{}/USDT".format("U" * 65),
            "cex:alpha:UNI/{}".format("T" * 65),
            "dex:{}:uniswap:pool:UNI".format("c" * 65),
            "dex:eth:{}:pool:UNI".format("d" * 129),
            "dex:eth:uniswap:{}:UNI".format("p" * 257),
            "dex:eth:uniswap:pool:{}".format("U" * 65),
        )
        for market_id in invalid_ids:
            with self.subTest(market_id=market_id[:80]):
                with self.assertRaisesRegex(ValueError, "market identity"):
                    route_publication._canonical_market_token(market_id)

        self.assertEqual(
            route_publication._canonical_market_token(
                "cex:{}:{}/{}".format(
                    "x" * 64,
                    "U" * 64,
                    "T" * 64,
                )
            ),
            "U" * 64,
        )
        self.assertEqual(
            route_publication._canonical_market_token(
                "dex:{}:{}:{}:{}".format(
                    "c" * 64,
                    "d" * 128,
                    "p" * 256,
                    "U" * 64,
                )
            ),
            "U" * 64,
        )

    def test_tampered_file_hash_is_rejected(self):
        cohort = _cohort()
        publish_route_cohort_bundle(cohort, core_root=self.root)
        bundle = self.root / "bundles" / cohort["route_cohort_id"]
        with (bundle / "route_legs.csv").open("ab") as handle:
            handle.write(b"\n")

        with self.assertRaisesRegex(ValueError, "checksum"):
            validate_route_cohort_bundle(bundle)

    def test_csv_sqlite_inventory_mismatch_is_rejected_even_with_new_file_hash(self):
        cohort = _cohort()
        publish_route_cohort_bundle(cohort, core_root=self.root)
        bundle = self.root / "bundles" / cohort["route_cohort_id"]
        database = bundle / "route_cohort.sqlite3"
        connection = sqlite3.connect(str(database))
        try:
            connection.execute(
                "DELETE FROM route_timing WHERE route_id = ?",
                (cohort["route_rows"][0]["route_id"],),
            )
            connection.commit()
        finally:
            connection.close()
        _refresh_database_hash_in_manifest(bundle)

        with self.assertRaisesRegex(ValueError, "inventories do not match"):
            validate_route_cohort_bundle(bundle)

    def test_existing_immutable_id_is_never_overwritten(self):
        cohort = _cohort()
        publish_route_cohort_bundle(cohort, core_root=self.root)
        pointer_path = self.root / "latest.json"
        pointer_before = pointer_path.read_bytes()
        bundle = self.root / "bundles" / cohort["route_cohort_id"]
        manifest_before = (bundle / "manifest.json").read_bytes()

        with self.assertRaisesRegex(ValueError, "already exists"):
            publish_route_cohort_bundle(cohort, core_root=self.root)

        self.assertEqual(pointer_path.read_bytes(), pointer_before)
        self.assertEqual((bundle / "manifest.json").read_bytes(), manifest_before)

    def test_no_replace_rename_rejects_file_empty_nonempty_and_symlink_targets(self):
        for target_kind in ("file", "empty", "nonempty", "symlink"):
            with self.subTest(target_kind=target_kind):
                case_root = Path(self.temporary.name) / ("rename-" + target_kind)
                case_root.mkdir()
                stage = case_root / "stage"
                stage.mkdir()
                (stage / "payload").write_text("new", encoding="utf-8")
                target = case_root / "target"
                if target_kind == "file":
                    target.write_text("old", encoding="utf-8")
                elif target_kind == "empty":
                    target.mkdir()
                elif target_kind == "nonempty":
                    target.mkdir()
                    (target / "sentinel").write_text("old", encoding="utf-8")
                else:
                    external = case_root / "external"
                    external.mkdir()
                    target.symlink_to(external, target_is_directory=True)

                with self.assertRaisesRegex(ValueError, "already exists"):
                    route_publication._rename_directory_noreplace(stage, target)

                self.assertTrue(stage.is_dir())
                self.assertEqual((stage / "payload").read_text(encoding="utf-8"), "new")
                self.assertTrue(os.path.lexists(str(target)))

    def test_darwin_and_linux_no_replace_rename_use_verified_dirfds(self):
        class FakeOperation:
            def __init__(self):
                self.calls = []
                self.argtypes = None
                self.restype = None

            def __call__(self, *arguments):
                self.calls.append(arguments)
                return 0

        case_root = Path(self.temporary.name) / "rename-dirfd"
        case_root.mkdir()
        flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
        directory_fd = os.open(str(case_root), flags)
        self.addCleanup(os.close, directory_fd)

        for platform, operation_name, expected_flag in (
            ("darwin", "renameatx_np", 0x00000004),
            ("linux", "renameat2", 1),
        ):
            with self.subTest(platform=platform):
                operation = FakeOperation()
                library = type("FakeLibrary", (), {})()
                setattr(library, operation_name, operation)
                with patch("scripts.route_publication.sys.platform", platform), patch(
                    "scripts.route_publication.ctypes.CDLL",
                    return_value=library,
                ):
                    route_publication._rename_directory_noreplace_at(
                        directory_fd,
                        "stage",
                        directory_fd,
                        "cohort",
                        destination_display=case_root / "cohort",
                    )

                self.assertEqual(
                    operation.calls,
                    [
                        (
                            directory_fd,
                            b"stage",
                            directory_fd,
                            b"cohort",
                            expected_flag,
                        )
                    ],
                )

    def test_destination_created_in_the_final_rename_race_is_not_replaced(self):
        cohort = _cohort()
        original = route_publication._rename_directory_noreplace

        def race(source, destination, **kwargs):
            destination.mkdir()
            return original(source, destination, **kwargs)

        with patch(
            "scripts.route_publication._rename_directory_noreplace",
            side_effect=race,
        ):
            with self.assertRaisesRegex(ValueError, "already exists"):
                publish_route_cohort_bundle(cohort, core_root=self.root)

        final = self.root / "bundles" / cohort["route_cohort_id"]
        self.assertTrue(final.is_dir())
        self.assertEqual(list(final.iterdir()), [])
        self.assertFalse((self.root / "latest.json").exists())

    def test_stage_entry_swapped_before_rename_cannot_be_published(self):
        cohort = _cohort()
        original = route_publication._rename_directory_noreplace
        detached = self.root / "bundles" / ".detached-stage"

        def swap_stage(source, destination, **kwargs):
            os.rename(str(source), str(detached))
            shutil.copytree(str(detached), str(source))
            return original(source, destination, **kwargs)

        with patch(
            "scripts.route_publication._rename_directory_noreplace",
            side_effect=swap_stage,
        ):
            with self.assertRaisesRegex(ValueError, "changed during validation"):
                publish_route_cohort_bundle(cohort, core_root=self.root)

        self.assertFalse((self.root / "latest.json").exists())
        final = self.root / "bundles" / cohort["route_cohort_id"]
        self.assertEqual(
            validate_route_cohort_bundle(final)["cohort"]["route_cohort_id"],
            cohort["route_cohort_id"],
        )

    def test_pointer_replace_failure_preserves_exact_old_pointer(self):
        first = _cohort()
        second = _second_cohort()
        publish_route_cohort_bundle(first, core_root=self.root)
        pointer_path = self.root / "latest.json"
        pointer_before = pointer_path.read_bytes()

        with patch(
            "scripts.route_publication.os.replace",
            side_effect=OSError("injected pointer failure"),
        ):
            with self.assertRaisesRegex(OSError, "injected pointer failure"):
                publish_route_cohort_bundle(second, core_root=self.root)

        self.assertEqual(pointer_path.read_bytes(), pointer_before)
        second_bundle = self.root / "bundles" / second["route_cohort_id"]
        self.assertEqual(
            validate_route_cohort_bundle(second_bundle)["cohort"]["route_cohort_id"],
            second["route_cohort_id"],
        )

    def test_post_replace_fsync_failure_keeps_new_pointer_over_old_pointer(self):
        first = _cohort()
        second = _second_cohort()
        publish_route_cohort_bundle(first, core_root=self.root)
        pointer_path = self.root / "latest.json"
        pointer_before = pointer_path.read_bytes()
        original_fsync_directory = route_publication._fsync_directory
        injected = {"failed": False}

        def fail_first_core_fsync(path, **kwargs):
            if Path(path).resolve() == self.root.resolve() and not injected["failed"]:
                injected["failed"] = True
                raise OSError("injected post-replace fsync failure")
            return original_fsync_directory(path, **kwargs)

        with patch(
            "scripts.route_publication._fsync_directory",
            side_effect=fail_first_core_fsync,
        ):
            with self.assertRaisesRegex(
                ValueError, "pointer state uncertain"
            ):
                publish_route_cohort_bundle(second, core_root=self.root)

        self.assertNotEqual(pointer_path.read_bytes(), pointer_before)
        self.assertEqual(
            load_latest_route_cohort(self.root)["cohort"]["route_cohort_id"],
            second["route_cohort_id"],
        )

    def test_post_replace_fsync_failure_keeps_new_pointer_when_none_existed(self):
        cohort = _cohort()
        original_fsync_directory = route_publication._fsync_directory
        injected = {"failed": False}

        def fail_first_core_fsync(path, **kwargs):
            if Path(path).resolve() == self.root.resolve() and not injected["failed"]:
                injected["failed"] = True
                raise OSError("injected post-replace fsync failure")
            return original_fsync_directory(path, **kwargs)

        with patch(
            "scripts.route_publication._fsync_directory",
            side_effect=fail_first_core_fsync,
        ):
            with self.assertRaisesRegex(
                ValueError, "pointer state uncertain"
            ):
                publish_route_cohort_bundle(cohort, core_root=self.root)

        self.assertTrue((self.root / "latest.json").is_file())
        self.assertEqual(
            load_latest_route_cohort(self.root)["cohort"]["route_cohort_id"],
            cohort["route_cohort_id"],
        )
        bundle = self.root / "bundles" / cohort["route_cohort_id"]
        self.assertEqual(
            validate_route_cohort_bundle(bundle)["cohort"]["route_cohort_id"],
            cohort["route_cohort_id"],
        )

    def test_concurrent_pointer_during_failed_fsync_is_preserved(self):
        first = _cohort()
        second = _second_cohort()
        third = _third_cohort()
        publish_route_cohort_bundle(first, core_root=self.root)
        pointer_path = self.root / "latest.json"
        first_pointer = pointer_path.read_bytes()
        publish_route_cohort_bundle(third, core_root=self.root)
        third_pointer = pointer_path.read_bytes()

        restore_first = self.root / ".restore-first-pointer"
        restore_first.write_bytes(first_pointer)
        os.replace(str(restore_first), str(pointer_path))
        original_fsync_directory = route_publication._fsync_directory
        injected = {"done": False}

        def install_concurrent_pointer_then_fail(path, **kwargs):
            if Path(path).resolve() == self.root.resolve() and not injected["done"]:
                injected["done"] = True
                concurrent = self.root / ".concurrent-pointer"
                concurrent.write_bytes(third_pointer)
                with concurrent.open("rb") as handle:
                    os.fsync(handle.fileno())
                os.replace(str(concurrent), str(pointer_path))
                os.fsync(kwargs["directory_fd"])
                raise OSError("injected pointer fsync failure after concurrent C")
            return original_fsync_directory(path, **kwargs)

        with patch(
            "scripts.route_publication._fsync_directory",
            side_effect=install_concurrent_pointer_then_fail,
        ):
            with self.assertRaisesRegex(
                Exception,
                "pointer state uncertain",
            ):
                publish_route_cohort_bundle(second, core_root=self.root)

        self.assertEqual(pointer_path.read_bytes(), third_pointer)
        self.assertEqual(
            load_latest_route_cohort(self.root)["cohort"]["route_cohort_id"],
            third["route_cohort_id"],
        )

    def test_concurrent_pointer_after_post_fsync_diagnostic_is_preserved(self):
        first = _cohort()
        second = _second_cohort()
        third = _third_cohort()
        publish_route_cohort_bundle(first, core_root=self.root)
        pointer_path = self.root / "latest.json"
        first_pointer = pointer_path.read_bytes()
        publish_route_cohort_bundle(third, core_root=self.root)
        third_pointer = pointer_path.read_bytes()

        restore_first = self.root / ".restore-first-pointer"
        restore_first.write_bytes(first_pointer)
        os.replace(str(restore_first), str(pointer_path))
        original_snapshot = route_publication._optional_pointer_snapshot_at
        calls = {"count": 0}

        def install_concurrent_pointer_after_read(core_fd):
            calls["count"] += 1
            snapshot = original_snapshot(core_fd)
            if calls["count"] == 2:
                concurrent = self.root / ".concurrent-after-diagnostic"
                concurrent.write_bytes(third_pointer)
                os.replace(str(concurrent), str(pointer_path))
                raise OSError("injected diagnostic failure after concurrent C")
            return snapshot

        with patch(
            "scripts.route_publication._optional_pointer_snapshot_at",
            side_effect=install_concurrent_pointer_after_read,
        ):
            with self.assertRaisesRegex(ValueError, "pointer state uncertain"):
                publish_route_cohort_bundle(second, core_root=self.root)

        self.assertEqual(calls["count"], 2)
        self.assertEqual(pointer_path.read_bytes(), third_pointer)
        self.assertEqual(
            load_latest_route_cohort(self.root)["cohort"]["route_cohort_id"],
            third["route_cohort_id"],
        )

    def test_concurrent_pointer_during_successful_fsync_is_detected_and_preserved(self):
        first = _cohort()
        second = _second_cohort()
        third = _third_cohort()
        publish_route_cohort_bundle(first, core_root=self.root)
        pointer_path = self.root / "latest.json"
        first_pointer = pointer_path.read_bytes()
        publish_route_cohort_bundle(third, core_root=self.root)
        third_pointer = pointer_path.read_bytes()

        restore_first = self.root / ".restore-first-pointer"
        restore_first.write_bytes(first_pointer)
        os.replace(str(restore_first), str(pointer_path))
        original_fsync_directory = route_publication._fsync_directory
        injected = {"done": False}

        def install_concurrent_pointer_and_succeed(path, **kwargs):
            if Path(path).resolve() == self.root.resolve() and not injected["done"]:
                injected["done"] = True
                concurrent = self.root / ".concurrent-during-fsync"
                concurrent.write_bytes(third_pointer)
                os.replace(str(concurrent), str(pointer_path))
                os.fsync(kwargs["directory_fd"])
                return None
            return original_fsync_directory(path, **kwargs)

        with patch(
            "scripts.route_publication._fsync_directory",
            side_effect=install_concurrent_pointer_and_succeed,
        ):
            with self.assertRaisesRegex(ValueError, "pointer state uncertain"):
                publish_route_cohort_bundle(second, core_root=self.root)

        self.assertEqual(pointer_path.read_bytes(), third_pointer)
        self.assertEqual(
            load_latest_route_cohort(self.root)["cohort"]["route_cohort_id"],
            third["route_cohort_id"],
        )

    def test_pointer_lock_acquisition_failure_is_clear_and_preserves_old_pointer(self):
        first = _cohort()
        second = _second_cohort()
        publish_route_cohort_bundle(first, core_root=self.root)
        pointer_path = self.root / "latest.json"
        pointer_before = pointer_path.read_bytes()

        with patch(
            "scripts.route_publication.fcntl.flock",
            side_effect=OSError("injected lock acquisition failure"),
        ):
            with self.assertRaisesRegex(ValueError, "lock acquisition failed"):
                publish_route_cohort_bundle(second, core_root=self.root)

        self.assertEqual(pointer_path.read_bytes(), pointer_before)

    def test_pointer_lock_release_failure_after_commit_is_clear_and_fd_close_releases(self):
        first = _cohort()
        second = _second_cohort()
        original_flock = route_publication.fcntl.flock

        def fail_unlock(fd, operation):
            if operation == route_publication.fcntl.LOCK_UN:
                raise OSError("injected lock release failure")
            return original_flock(fd, operation)

        with patch(
            "scripts.route_publication.fcntl.flock",
            side_effect=fail_unlock,
        ):
            with self.assertRaisesRegex(
                ValueError,
                "lock release failed after pointer commit",
            ):
                publish_route_cohort_bundle(first, core_root=self.root)

        self.assertEqual(
            load_latest_route_cohort(self.root)["cohort"]["route_cohort_id"],
            first["route_cohort_id"],
        )
        publish_route_cohort_bundle(second, core_root=self.root)
        self.assertEqual(
            load_latest_route_cohort(self.root)["cohort"]["route_cohort_id"],
            second["route_cohort_id"],
        )

    def test_pointer_lock_release_failure_does_not_mask_post_replace_failure(self):
        cohort = _cohort()
        original_fsync_directory = route_publication._fsync_directory
        original_flock = route_publication.fcntl.flock

        def fail_core_fsync(path, **kwargs):
            if Path(path).resolve() == self.root.resolve():
                raise OSError("injected post-replace fsync failure")
            return original_fsync_directory(path, **kwargs)

        def fail_unlock(fd, operation):
            if operation == route_publication.fcntl.LOCK_UN:
                raise OSError("injected lock release failure")
            return original_flock(fd, operation)

        with patch(
            "scripts.route_publication._fsync_directory",
            side_effect=fail_core_fsync,
        ), patch(
            "scripts.route_publication.fcntl.flock",
            side_effect=fail_unlock,
        ):
            with self.assertRaisesRegex(
                ValueError,
                "pointer state uncertain",
            ) as raised:
                publish_route_cohort_bundle(cohort, core_root=self.root)

        self.assertIn(
            "injected post-replace fsync failure",
            str(raised.exception.__cause__),
        )
        self.assertEqual(
            load_latest_route_cohort(self.root)["cohort"]["route_cohort_id"],
            cohort["route_cohort_id"],
        )

    def test_bundle_directory_swap_during_validation_is_rejected(self):
        cohort = _cohort()
        publish_route_cohort_bundle(cohort, core_root=self.root)
        bundles = self.root / "bundles"
        bundle = bundles / cohort["route_cohort_id"]
        replacement = bundles / ".replacement"
        detached = bundles / ".detached"
        shutil.copytree(str(bundle), str(replacement))
        original_listdir = route_publication.os.listdir
        swapped = {"done": False}

        def swap_after_list(directory):
            entries = original_listdir(directory)
            if not swapped["done"]:
                swapped["done"] = True
                os.rename(str(bundle), str(detached))
                os.rename(str(replacement), str(bundle))
            return entries

        with patch(
            "scripts.route_publication.os.listdir",
            side_effect=swap_after_list,
        ):
            with self.assertRaisesRegex(ValueError, "changed during validation"):
                validate_route_cohort_bundle(bundle)

    def test_csv_replaced_after_read_during_final_validation_is_rejected(self):
        cohort = _cohort()
        bundle = self.root / "bundles" / cohort["route_cohort_id"]
        original_sqlite_validation = route_publication._read_and_validate_sqlite_at
        calls = {"count": 0}

        def replace_csv_after_its_read(*args, **kwargs):
            calls["count"] += 1
            if calls["count"] == 2:
                replacement = bundle.parent / ".replacement-candidates.csv"
                replacement.write_bytes(b"attacker replacement\n")
                os.replace(
                    str(replacement),
                    str(bundle / "route_candidates.csv"),
                )
            return original_sqlite_validation(*args, **kwargs)

        with patch(
            "scripts.route_publication._read_and_validate_sqlite_at",
            side_effect=replace_csv_after_its_read,
        ):
            with self.assertRaisesRegex(
                ValueError,
                "route candidate CSV changed during validation",
            ):
                publish_route_cohort_bundle(cohort, core_root=self.root)

        self.assertFalse((self.root / "latest.json").exists())

    def test_pre_rename_failures_leave_no_final_bundle_or_pointer(self):
        original_fsync_directory = route_publication._fsync_directory

        def fail_stage_fsync(path, **kwargs):
            if path.name.startswith(".route-cohort-"):
                raise OSError("injected stage fsync failure")
            return original_fsync_directory(path, **kwargs)

        cases = {
            "write": patch(
                "scripts.route_publication._write_bundle_artifacts",
                side_effect=OSError("injected stage write failure"),
            ),
            "validate": patch(
                "scripts.route_publication._validate_route_cohort_bundle",
                side_effect=ValueError("injected stage validation failure"),
            ),
            "fsync": patch(
                "scripts.route_publication._fsync_directory",
                side_effect=fail_stage_fsync,
            ),
            "rename": patch(
                "scripts.route_publication._rename_directory_noreplace",
                side_effect=OSError("injected final rename failure"),
            ),
        }
        for phase, failure in cases.items():
            with self.subTest(phase=phase):
                core_root = Path(self.temporary.name) / ("failure-" + phase)
                cohort = _cohort()
                with failure:
                    with self.assertRaisesRegex(Exception, "injected"):
                        publish_route_cohort_bundle(cohort, core_root=core_root)
                final = core_root / "bundles" / cohort["route_cohort_id"]
                self.assertFalse(os.path.lexists(str(final)))
                self.assertFalse((core_root / "latest.json").exists())
                self.assertEqual(list((core_root / "bundles").iterdir()), [])

    def test_final_reread_failure_leaves_only_a_valid_unpointed_orphan(self):
        cohort = _cohort()
        original_validate = route_publication._validate_route_cohort_bundle

        def fail_final_reread(bundle_path, **kwargs):
            if kwargs.get("require_directory_identity"):
                raise ValueError("injected final reread failure")
            return original_validate(bundle_path, **kwargs)

        with patch(
            "scripts.route_publication._validate_route_cohort_bundle",
            side_effect=fail_final_reread,
        ):
            with self.assertRaisesRegex(ValueError, "injected final reread failure"):
                publish_route_cohort_bundle(cohort, core_root=self.root)

        final = self.root / "bundles" / cohort["route_cohort_id"]
        self.assertFalse((self.root / "latest.json").exists())
        self.assertEqual(
            validate_route_cohort_bundle(final)["cohort"]["route_cohort_id"],
            cohort["route_cohort_id"],
        )

    def test_pointer_manifest_hash_tamper_is_rejected(self):
        cohort = _cohort()
        publish_route_cohort_bundle(cohort, core_root=self.root)
        pointer_path = self.root / "latest.json"
        pointer = json.loads(pointer_path.read_text(encoding="utf-8"))
        pointer["manifest_sha256"] = "f" * 64
        pointer_path.write_text(
            json.dumps(pointer, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        with self.assertRaisesRegex(ValueError, "manifest hash"):
            load_latest_route_cohort(self.root)

    def test_symlink_roots_files_and_existing_broken_bundle_are_rejected(self):
        real = Path(self.temporary.name) / "real"
        real.mkdir()
        symlink_root = Path(self.temporary.name) / "core-link"
        symlink_root.symlink_to(real, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, "real directory"):
            publish_route_cohort_bundle(_cohort(), core_root=symlink_root)

        cohort = _cohort()
        publish_route_cohort_bundle(cohort, core_root=self.root)
        bundle = self.root / "bundles" / cohort["route_cohort_id"]
        manifest = bundle / "manifest.json"
        manifest_bytes = manifest.read_bytes()
        manifest.unlink()
        external = Path(self.temporary.name) / "external-manifest.json"
        external.write_bytes(manifest_bytes)
        manifest.symlink_to(external)
        with self.assertRaisesRegex(ValueError, "non-symlink"):
            validate_route_cohort_bundle(bundle)

        broken_root = Path(self.temporary.name) / "broken-core"
        bundles = broken_root / "bundles"
        bundles.mkdir(parents=True)
        broken = bundles / cohort["route_cohort_id"]
        broken.symlink_to(Path(self.temporary.name) / "missing")
        with self.assertRaisesRegex(ValueError, "already exists"):
            publish_route_cohort_bundle(cohort, core_root=broken_root)

    def test_symlinked_bundle_ancestor_is_rejected(self):
        real_parent = Path(self.temporary.name) / "real-parent"
        real_core = real_parent / "core"
        cohort = _cohort()
        publish_route_cohort_bundle(cohort, core_root=real_core)
        linked_parent = Path(self.temporary.name) / "linked-parent"
        linked_parent.symlink_to(real_parent, target_is_directory=True)

        linked_bundle = (
            linked_parent / "core/bundles" / cohort["route_cohort_id"]
        )
        with self.assertRaisesRegex(ValueError, "symlink"):
            validate_route_cohort_bundle(linked_bundle)
        with self.assertRaisesRegex(ValueError, "symlink"):
            load_latest_route_cohort(linked_parent / "core")

    def test_extra_sqlite_index_is_rejected_even_when_manifest_hash_is_updated(self):
        cohort = _cohort()
        publish_route_cohort_bundle(cohort, core_root=self.root)
        bundle = self.root / "bundles" / cohort["route_cohort_id"]
        database = bundle / "route_cohort.sqlite3"
        connection = sqlite3.connect(str(database))
        try:
            connection.execute(
                "CREATE INDEX attacker_extra_idx ON route_legs(status)"
            )
            connection.commit()
        finally:
            connection.close()
        _refresh_database_hash_in_manifest(bundle)

        with self.assertRaisesRegex(ValueError, "SQLite schema"):
            validate_route_cohort_bundle(bundle)

    def test_sqlite_exact_column_and_table_semantics_are_enforced(self):
        cases = {
            "type": lambda bundle: _rewrite_route_legs_schema(
                bundle,
                status_definition="status BLOB NOT NULL",
            ),
            "not-null": lambda bundle: _rewrite_route_legs_schema(
                bundle,
                status_definition="status TEXT",
            ),
            "primary-key": lambda bundle: _rewrite_route_legs_schema(
                bundle,
                market_definition="market_id TEXT NOT NULL",
                table_primary_key="PRIMARY KEY (market_id, leg_id)",
            ),
            "without-rowid": lambda bundle: _rewrite_route_legs_schema(
                bundle,
                without_rowid=False,
            ),
            "foreign-key": _rewrite_route_timing_without_foreign_key,
        }
        for label, mutate in cases.items():
            with self.subTest(label=label):
                core_root = Path(self.temporary.name) / ("schema-" + label)
                cohort = _cohort()
                publish_route_cohort_bundle(cohort, core_root=core_root)
                bundle = core_root / "bundles" / cohort["route_cohort_id"]
                mutate(bundle)
                _refresh_database_hash_in_manifest(bundle)

                with self.assertRaisesRegex(ValueError, "SQLite schema"):
                    validate_route_cohort_bundle(bundle)

    def test_sqlite_foreign_key_or_file_corruption_is_rejected(self):
        for corruption in ("foreign-key", "file"):
            with self.subTest(corruption=corruption):
                core_root = Path(self.temporary.name) / ("sqlite-" + corruption)
                cohort = _cohort()
                publish_route_cohort_bundle(cohort, core_root=core_root)
                bundle = core_root / "bundles" / cohort["route_cohort_id"]
                database = bundle / "route_cohort.sqlite3"
                if corruption == "foreign-key":
                    connection = sqlite3.connect(str(database))
                    try:
                        connection.execute(
                            "DELETE FROM route_candidates WHERE route_id = ?",
                            (cohort["routes"][0]["route_id"],),
                        )
                        connection.commit()
                    finally:
                        connection.close()
                    expected = "foreign keys"
                else:
                    value = bytearray(database.read_bytes())
                    value[:16] = b"not-a-sqlite-db!"
                    database.write_bytes(bytes(value))
                    expected = "SQLite"
                _refresh_database_hash_in_manifest(bundle)

                with self.assertRaisesRegex(ValueError, expected):
                    validate_route_cohort_bundle(bundle)

    def test_direct_sqlite_builder_cleans_up_logical_and_fsync_failures(self):
        failures = {
            "logical": patch(
                "scripts.route_publication._sqlite_candidate_values",
                side_effect=ValueError("injected SQLite logical failure"),
            ),
            "fsync": patch(
                "scripts.route_publication._fsync_file",
                side_effect=OSError("injected SQLite fsync failure"),
            ),
        }
        for phase, failure in failures.items():
            with self.subTest(phase=phase):
                database = Path(self.temporary.name) / (phase + ".sqlite3")
                with failure:
                    with self.assertRaisesRegex(Exception, "injected SQLite"):
                        build_route_cohort_sqlite(database, _cohort())
                self.assertFalse(os.path.lexists(str(database)))
                for suffix in ("-journal", "-wal", "-shm"):
                    self.assertFalse(os.path.lexists(str(database) + suffix))

    def test_pointer_path_traversal_is_rejected_without_reading_outside_bundle_root(self):
        bundles = self.root / "bundles"
        bundles.mkdir(parents=True)
        outside = self.root / "outside-marker"
        outside.write_text("do not read", encoding="utf-8")
        (self.root / "latest.json").write_text(json.dumps({
            "schema": "route_cohort_core_pointer/v1",
            "bundle_stage": "route_cohort_core/v1",
            "route_cohort_id": "../outside-marker",
            "manifest_sha256": "a" * 64,
        }), encoding="utf-8")

        with self.assertRaisesRegex(ValueError, "path-unsafe"):
            load_latest_route_cohort(self.root)

        self.assertEqual(outside.read_text(encoding="utf-8"), "do not read")

    def test_extra_or_missing_bundle_files_are_rejected_by_exact_allowlist(self):
        cohort = _cohort()
        publish_route_cohort_bundle(cohort, core_root=self.root)
        bundle = self.root / "bundles" / cohort["route_cohort_id"]
        (bundle / "cost_components.csv").write_text("\n", encoding="utf-8")

        with self.assertRaisesRegex(ValueError, "file inventory"):
            validate_route_cohort_bundle(bundle)


if __name__ == "__main__":
    unittest.main()
