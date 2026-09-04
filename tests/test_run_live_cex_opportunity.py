"""Tests for the fixed, read-only live CEX Opportunity entrypoint."""

from __future__ import annotations

import copy
from datetime import datetime, timezone
import io
import json
from pathlib import Path
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest.mock import patch

from scripts import run_live_cex_opportunity as runner
from scripts.route_shadow_inputs import TYPED_SOURCE_ROLE_CONTRACTS


COHORT_ID = "cohort:" + "c" * 64
CORE_MANIFEST_SHA256 = "a" * 64
COMPLETE_MANIFEST_SHA256 = "b" * 64
NOW = datetime(2026, 9, 4, 12, 0, tzinfo=timezone.utc)
ROUTE_IDS = (
    "route:UNI:cex:binance:UNI/USDT->cex:bybit:UNI/USDT:"
    "prepositioned_inventory",
    "route:UNI:cex:bybit:UNI/USDT->cex:binance:UNI/USDT:"
    "prepositioned_inventory",
    "route:CAKE:cex:binance:CAKE/USDT->cex:bybit:CAKE/USDT:"
    "prepositioned_inventory",
    "route:CAKE:cex:bybit:CAKE/USDT->cex:binance:CAKE/USDT:"
    "prepositioned_inventory",
)
ROUTES = (
    {
        "route_id": ROUTE_IDS[0],
        "token_symbol": "UNI",
        "buy_market_id": "cex:binance:UNI/USDT",
        "sell_market_id": "cex:bybit:UNI/USDT",
        "route_mode": "prepositioned_inventory",
    },
    {
        "route_id": ROUTE_IDS[1],
        "token_symbol": "UNI",
        "buy_market_id": "cex:bybit:UNI/USDT",
        "sell_market_id": "cex:binance:UNI/USDT",
        "route_mode": "prepositioned_inventory",
    },
    {
        "route_id": ROUTE_IDS[2],
        "token_symbol": "CAKE",
        "buy_market_id": "cex:binance:CAKE/USDT",
        "sell_market_id": "cex:bybit:CAKE/USDT",
        "route_mode": "prepositioned_inventory",
    },
    {
        "route_id": ROUTE_IDS[3],
        "token_symbol": "CAKE",
        "buy_market_id": "cex:bybit:CAKE/USDT",
        "sell_market_id": "cex:binance:CAKE/USDT",
        "route_mode": "prepositioned_inventory",
    },
)


def _cohort(*, second_status="observed", second_timing="within_sla"):
    markets = (
        "cex:binance:UNI/USDT",
        "cex:bybit:UNI/USDT",
        "cex:binance:CAKE/USDT",
        "cex:bybit:CAKE/USDT",
    )
    legs = []
    for index, market_id in enumerate(markets):
        status = second_status if index == 1 else "observed"
        observed = status in {"observed", "partial"}
        legs.append({
            "leg_id": market_id,
            "market_id": market_id,
            "status": status,
            "available": observed,
            "reason_code": "observed" if observed else "source_unavailable",
            "state_observed_at": (
                "2026-09-04T12:00:00Z" if observed else None
            ),
        })
    route_rows = []
    for index, route in enumerate(ROUTES):
        timing_status = second_timing if index == 1 else "within_sla"
        route_rows.append({
            **route,
            "validated_at": "2026-09-04T12:00:00Z",
            "skew_seconds": "0" if timing_status == "within_sla" else None,
            "timing_status": timing_status,
            "reason_code": (
                None if timing_status == "within_sla"
                else "buy_leg_unavailable"
            ),
        })
    return {
        "route_cohort_id": COHORT_ID,
        "skew_sla_seconds": "60",
        "routes": [dict(route) for route in ROUTES],
        "legs": legs,
        "route_rows": route_rows,
    }


def _terminal_cake_cohort():
    cohort = _cohort()
    failed = next(
        row for row in cohort["legs"]
        if row["market_id"] == "cex:bybit:CAKE/USDT"
    )
    market_id = failed["market_id"]
    failed.clear()
    failed.update({
        "leg_id": market_id,
        "market_id": market_id,
        "market_type": "cex",
        "status": "failed",
        "available": False,
        "reason_code": "source_unavailable",
        "execution_adapter_status": "supported",
        "execution_adapter_supported": True,
    })
    for row in cohort["route_rows"]:
        if row["token_symbol"] != "CAKE":
            continue
        failed_is_buy = row["buy_market_id"] == failed["market_id"]
        row.update({
            "skew_seconds": None,
            "timing_status": "unavailable",
            "reason_code": (
                "buy_leg_unavailable" if failed_is_buy
                else "sell_leg_unavailable"
            ),
        })
    return cohort


def _observed_cex_typed_lineage():
    members = []
    for index, role in enumerate((
        "cex_market_rules",
        "cex_raw_book_response",
        "quote_usd_conversion",
    ), start=1):
        contract = TYPED_SOURCE_ROLE_CONTRACTS[role]
        members.append({
            "role": role,
            "status": "observed",
            "reason_code": None,
            "filename": "terminal-residue-{}.json".format(index),
            "sha256": str(index) * 64,
            "size": index,
            "logical_generation": str(index) * 64,
            "adapter_id": contract["adapter_id"],
            "content_schema": contract["content_schema"],
        })
    return {
        "schema": "route_leg_typed_source_lineage/v1",
        "members": members,
    }


def _complete_pointer():
    return {
        "route_cohort_id": COHORT_ID,
        "manifest_sha256": COMPLETE_MANIFEST_SHA256,
    }


def _loaded_bundle(*, pointer=None, count=20, strict=False):
    return {
        "pointer": dict(pointer or _complete_pointer()),
        "bundle": {
            "opportunities": [
                {
                    "opportunity_id": "opportunity:{}".format(index),
                    "strict_eligible": strict,
                }
                for index in range(count)
            ],
        },
    }


class LiveCexOpportunityParserTests(unittest.TestCase):
    def test_parser_exposes_only_fixed_safe_controls(self):
        parsed = runner.parse_args([
            "--data-dir", "/tmp/live-cex-data",
            "--public-fee-schedule", "/tmp/fees.csv",
            "--deadline-seconds", "10",
            "--serve",
            "--port", "65535",
        ])

        self.assertEqual(set(vars(parsed)), {
            "data_dir",
            "public_fee_schedule",
            "deadline_seconds",
            "serve",
            "port",
        })
        self.assertEqual(parsed.data_dir, Path("/tmp/live-cex-data"))
        self.assertEqual(parsed.public_fee_schedule, Path("/tmp/fees.csv"))
        self.assertEqual(parsed.deadline_seconds, 10)
        self.assertTrue(parsed.serve)
        self.assertEqual(parsed.port, 65535)

        defaults = runner.parse_args([
            "--data-dir", "/tmp/live-cex-data",
        ])
        self.assertTrue(defaults.public_fee_schedule.is_absolute())
        self.assertEqual(
            defaults.public_fee_schedule.name,
            "cex_public_fee_schedules.csv",
        )
        self.assertEqual(defaults.deadline_seconds, 60)
        self.assertFalse(defaults.serve)
        self.assertEqual(defaults.port, 8765)

    def test_parser_rejects_relative_data_deadline_and_port_bounds(self):
        invalid_arguments = (
            ["--data-dir", "relative/data"],
            ["--data-dir", "/tmp/data", "--deadline-seconds", "9"],
            ["--data-dir", "/tmp/data", "--deadline-seconds", "61"],
            ["--data-dir", "/tmp/data", "--deadline-seconds", "10.5"],
            ["--data-dir", "/tmp/data", "--port", "0"],
            ["--data-dir", "/tmp/data", "--port", "65536"],
        )
        for arguments in invalid_arguments:
            with self.subTest(arguments=arguments):
                with redirect_stderr(io.StringIO()):
                    with self.assertRaises(SystemExit):
                        runner.parse_args(arguments)

    def test_parser_rejects_all_dynamic_market_and_execution_controls(self):
        forbidden = (
            "--token", "--tokens", "--venue", "--venues", "--url",
            "--endpoint", "--profile", "--run-id", "--finalizer",
            "--host", "--rpc", "--api-key",
        )
        for option in forbidden:
            with self.subTest(option=option):
                with redirect_stderr(io.StringIO()):
                    with self.assertRaises(SystemExit):
                        runner.parse_args([
                            "--data-dir", "/tmp/data", option, "value",
                        ])


class LiveCexOpportunityOrchestrationTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.data_dir = Path(self.temporary.name) / "market-data"
        self.data_dir.mkdir()
        self.schedule = Path(self.temporary.name) / "fees.csv"
        self.schedule.write_text("schedule\n", encoding="utf-8")
        self.universe = runner.build_live_cex_research_universe()
        self.core_pointer = {
            "route_cohort_id": COHORT_ID,
            "manifest_sha256": CORE_MANIFEST_SHA256,
        }
        self.attach_typed = patch.object(
            runner,
            "attach_typed_source_lineage",
            create=True,
            side_effect=lambda cohort, **_kwargs: (
                cohort,
                {"typed_source_manifest_sha256": "d" * 64},
            ),
        )
        self.attach_typed_mock = self.attach_typed.start()
        self.addCleanup(self.attach_typed.stop)

    def _patch_happy_path(self, events):
        def build():
            events.append("build fixed universe")
            return self.universe

        def collect(universe, **kwargs):
            events.append("collect_route_cohort")
            self.assertIs(universe, self.universe)
            self.assertEqual(kwargs["deadline_seconds"], 30)
            self.assertEqual(kwargs["max_workers"], 4)
            self.assertEqual(kwargs["cex_workers_per_venue"], 2)
            self.assertEqual(
                kwargs["raw_root"], self.data_dir / "raw/route-cohort"
            )
            self.assertIs(kwargs["cex_collector"], runner.collect_cex_market_observation)
            self.assertIs(kwargs["source_generation_reader"], runner.live_cex_research_generation)
            self.assertEqual(
                kwargs["expected_source_generation"],
                runner.live_cex_research_generation(),
            )
            self.assertIs(kwargs["wall_clock"], self.wall_clock)
            return _cohort()

        def publish(cohort, **kwargs):
            events.append("publish_route_cohort_bundle")
            self.assertEqual(cohort, _cohort())
            self.assertEqual(kwargs["core_root"], self.data_dir / "routes/core")
            return self.core_pointer

        def attach(cohort, **kwargs):
            events.append("attach_typed_source_lineage")
            self.assertEqual(cohort, _cohort())
            self.assertEqual(
                kwargs, {"raw_root": self.data_dir / "raw/route-cohort"}
            )
            return cohort, {"typed_source_manifest_sha256": "d" * 64}

        def finalize(**kwargs):
            events.append("finalize_public_cex_research_opportunities")
            validator = kwargs.pop("_postcommit_validator")
            self.assertEqual(kwargs, {
                "data_dir": self.data_dir,
                "public_fee_schedule_path": self.schedule,
                "expected_route_cohort_id": COHORT_ID,
                "expected_core_manifest_sha256": CORE_MANIFEST_SHA256,
            })
            validator(_complete_pointer())
            return _complete_pointer()

        def load(routes_root, **kwargs):
            events.append("load_latest_complete_route_bundle")
            self.assertEqual(routes_root, self.data_dir / "routes")
            self.assertEqual(kwargs["core_root"], self.data_dir / "routes/core")
            return _loaded_bundle()

        self.wall_clock = lambda: NOW
        return (
            patch.object(runner, "build_live_cex_research_universe", side_effect=build),
            patch.object(runner, "collect_route_cohort", side_effect=collect),
            patch.object(
                runner,
                "attach_typed_source_lineage",
                side_effect=attach,
            ),
            patch.object(runner, "publish_route_cohort_bundle", side_effect=publish),
            patch.object(
                runner,
                "finalize_public_cex_research_opportunities",
                side_effect=finalize,
            ),
            patch.object(runner, "load_latest_complete_route_bundle", side_effect=load),
        )

    def test_exact_pipeline_order_identity_binding_and_minimal_receipt(self):
        events = []
        patches = self._patch_happy_path(events)
        with patches[0], patches[1], patches[2], \
                patches[3], patches[4], patches[5]:
            receipt = runner.collect_and_publish_live_cex_research(
                data_dir=self.data_dir,
                public_fee_schedule_path=self.schedule,
                deadline_seconds=30,
                wall_clock=self.wall_clock,
            )

        self.assertEqual(events, [
            "build fixed universe",
            "collect_route_cohort",
            "attach_typed_source_lineage",
            "publish_route_cohort_bundle",
            "finalize_public_cex_research_opportunities",
            "load_latest_complete_route_bundle",
        ])
        self.assertEqual(receipt, {
            "schema": "live_cex_opportunity_refresh/v2",
            "status": "published",
            "token_pairs": ["UNI/USDT", "CAKE/USDT"],
            "venues": ["binance", "bybit"],
            "market_count": 4,
            "route_count": 4,
            "route_cohort_id": COHORT_ID,
            "manifest_sha256": COMPLETE_MANIFEST_SHA256,
            "opportunity_count": 20,
            "strict_eligible_count": 0,
            "served": False,
        })

    def test_terminal_cake_cohort_still_publishes_twenty_row_receipt(self):
        cohort = _terminal_cake_cohort()

        def finalize(**kwargs):
            validator = kwargs["_postcommit_validator"]
            validator(_complete_pointer())
            return _complete_pointer()

        with patch.object(
            runner,
            "build_live_cex_research_universe",
            return_value=self.universe,
        ), patch.object(
            runner, "collect_route_cohort", return_value=cohort,
        ), patch.object(
            runner,
            "attach_typed_source_lineage",
            return_value=(cohort, {"typed_source_manifest_sha256": "d" * 64}),
        ), patch.object(
            runner,
            "publish_route_cohort_bundle",
            return_value=self.core_pointer,
        ) as publish, patch.object(
            runner,
            "finalize_public_cex_research_opportunities",
            side_effect=finalize,
        ) as finalize_mock, patch.object(
            runner,
            "load_latest_complete_route_bundle",
            return_value=_loaded_bundle(),
        ):
            receipt = runner.collect_and_publish_live_cex_research(
                data_dir=self.data_dir,
                public_fee_schedule_path=self.schedule,
                deadline_seconds=30,
                wall_clock=lambda: NOW,
            )

        publish.assert_called_once_with(
            cohort, core_root=self.data_dir / "routes/core"
        )
        finalize_mock.assert_called_once()
        self.assertEqual(receipt["status"], "published")
        self.assertEqual(receipt["market_count"], 4)
        self.assertEqual(receipt["route_count"], 4)
        self.assertEqual(receipt["opportunity_count"], 20)

    def test_typed_lineage_attachment_failure_never_publishes(self):
        with patch.object(
            runner,
            "build_live_cex_research_universe",
            return_value=self.universe,
        ), patch.object(
            runner, "collect_route_cohort", return_value=_cohort(),
        ), patch.object(
            runner,
            "attach_typed_source_lineage",
            side_effect=ValueError("invalid typed evidence"),
        ), patch.object(
            runner, "publish_route_cohort_bundle"
        ) as publish:
            with self.assertRaisesRegex(
                runner.LiveCexOpportunityRefreshError,
                "collection_failed",
            ):
                runner.collect_and_publish_live_cex_research(
                    data_dir=self.data_dir,
                    public_fee_schedule_path=self.schedule,
                    deadline_seconds=30,
                    wall_clock=lambda: NOW,
                )

        publish.assert_not_called()

    def test_typed_lineage_normalization_is_rechecked_before_publish(self):
        with patch.object(
            runner,
            "build_live_cex_research_universe",
            return_value=self.universe,
        ), patch.object(
            runner, "collect_route_cohort", return_value=_cohort(),
        ), patch.object(
            runner,
            "attach_typed_source_lineage",
            return_value=(
                _cohort(second_status="failed"),
                {"typed_source_manifest_sha256": "d" * 64},
            ),
        ), patch.object(
            runner, "publish_route_cohort_bundle"
        ) as publish:
            with self.assertRaisesRegex(
                runner.LiveCexOpportunityRefreshError,
                "collection_failed",
            ):
                runner.collect_and_publish_live_cex_research(
                    data_dir=self.data_dir,
                    public_fee_schedule_path=self.schedule,
                    deadline_seconds=30,
                    wall_clock=lambda: NOW,
                )

        publish.assert_not_called()

    def test_incomplete_collection_never_publishes_or_finalizes(self):
        terminal = _terminal_cake_cohort()
        wrong_reason = copy.deepcopy(terminal)
        next(
            row for row in wrong_reason["route_rows"]
            if row["timing_status"] != "within_sla"
        )["reason_code"] = "route_deadline_exceeded"
        missing_terminal_route = copy.deepcopy(terminal)
        missing_terminal_route["route_rows"].pop()
        missing_route_candidate = copy.deepcopy(terminal)
        missing_route_candidate["routes"].pop()
        missing_terminal_leg = copy.deepcopy(terminal)
        missing_terminal_leg["legs"].pop()
        duplicate_terminal_route = copy.deepcopy(terminal)
        duplicate_terminal_route["route_rows"][3]["route_id"] = (
            duplicate_terminal_route["route_rows"][2]["route_id"]
        )
        unknown_terminal_status = copy.deepcopy(terminal)
        unknown_terminal_status["legs"][-1]["status"] = "missing"
        terminal_missing_leg_id = copy.deepcopy(terminal)
        terminal_missing_leg_id["legs"][-1].pop("leg_id")
        terminal_with_state = copy.deepcopy(terminal)
        terminal_with_state["legs"][-1][
            "state_observed_at"
        ] = "2026-09-04T12:00:00Z"
        terminal_with_snapshot = copy.deepcopy(terminal)
        terminal_with_snapshot["legs"][-1]["snapshot_id"] = "source-run"
        terminal_with_raw_hash = copy.deepcopy(terminal)
        terminal_with_raw_hash["legs"][-1][
            "raw_response_sha256"
        ] = "a" * 64
        terminal_with_endpoint = copy.deepcopy(terminal)
        terminal_with_endpoint["legs"][-1][
            "source_endpoint"
        ] = "https://api.bybit.com/v5/market/orderbook"
        terminal_with_observed_typed_member = copy.deepcopy(terminal)
        terminal_with_observed_typed_member["legs"][-1][
            "typed_source_lineage"
        ] = _observed_cex_typed_lineage()
        invalid_cohorts = (
            _cohort(second_status="failed"),
            _cohort(second_timing="outside_sla"),
            {
                **_cohort(),
                "legs": _cohort()["legs"][:1],
            },
            {
                **_cohort(),
                "route_rows": _cohort()["route_rows"][:1],
            },
            wrong_reason,
            missing_terminal_route,
            missing_route_candidate,
            missing_terminal_leg,
            duplicate_terminal_route,
            unknown_terminal_status,
            terminal_missing_leg_id,
            terminal_with_state,
            terminal_with_snapshot,
            terminal_with_raw_hash,
            terminal_with_endpoint,
            terminal_with_observed_typed_member,
        )
        prior = b'{"prior":"complete-pointer"}\n'
        pointer_path = self.data_dir / "routes/latest.json"
        pointer_path.parent.mkdir(exist_ok=True)
        for cohort in invalid_cohorts:
            with self.subTest(cohort=cohort):
                pointer_path.write_bytes(prior)
                with patch.object(
                    runner,
                    "build_live_cex_research_universe",
                    return_value=self.universe,
                ), patch.object(
                    runner, "collect_route_cohort", return_value=cohort,
                ), patch.object(
                    runner, "publish_route_cohort_bundle"
                ) as publish, patch.object(
                    runner, "finalize_public_cex_research_opportunities"
                ) as finalize:
                    with self.assertRaisesRegex(
                        runner.LiveCexOpportunityRefreshError,
                        "collection_failed",
                    ):
                        runner.collect_and_publish_live_cex_research(
                            data_dir=self.data_dir,
                            public_fee_schedule_path=self.schedule,
                            deadline_seconds=30,
                            wall_clock=lambda: NOW,
                        )
                publish.assert_not_called()
                finalize.assert_not_called()
                self.assertEqual(pointer_path.read_bytes(), prior)

    def test_collector_exception_never_calls_publication(self):
        prior = b'{"prior":"complete-pointer"}\n'
        pointer_path = self.data_dir / "routes/latest.json"
        pointer_path.parent.mkdir()
        pointer_path.write_bytes(prior)
        with patch.object(
            runner,
            "build_live_cex_research_universe",
            return_value=self.universe,
        ), patch.object(
            runner, "collect_route_cohort", side_effect=TimeoutError("secret")
        ), patch.object(
            runner, "publish_route_cohort_bundle"
        ) as publish, patch.object(
            runner, "finalize_public_cex_research_opportunities"
        ) as finalize:
            with self.assertRaisesRegex(
                runner.LiveCexOpportunityRefreshError, "collection_failed"
            ):
                runner.collect_and_publish_live_cex_research(
                    data_dir=self.data_dir,
                    public_fee_schedule_path=self.schedule,
                    deadline_seconds=30,
                    wall_clock=lambda: NOW,
                )
        publish.assert_not_called()
        finalize.assert_not_called()
        self.assertEqual(pointer_path.read_bytes(), prior)

    def test_finalizer_failure_preserves_prior_complete_pointer(self):
        prior = b'{"prior":"complete-pointer"}\n'
        pointer_path = self.data_dir / "routes/latest.json"
        pointer_path.parent.mkdir()
        pointer_path.write_bytes(prior)
        with patch.object(
            runner,
            "build_live_cex_research_universe",
            return_value=self.universe,
        ), patch.object(
            runner, "collect_route_cohort", return_value=_cohort(),
        ), patch.object(
            runner, "publish_route_cohort_bundle", return_value=self.core_pointer,
        ), patch.object(
            runner,
            "finalize_public_cex_research_opportunities",
            side_effect=ValueError("private details"),
        ), patch.object(
            runner, "load_latest_complete_route_bundle"
        ) as load:
            with self.assertRaisesRegex(
                runner.LiveCexOpportunityRefreshError, "publication_failed"
            ):
                runner.collect_and_publish_live_cex_research(
                    data_dir=self.data_dir,
                    public_fee_schedule_path=self.schedule,
                    deadline_seconds=30,
                    wall_clock=lambda: NOW,
                )
        load.assert_not_called()
        self.assertEqual(pointer_path.read_bytes(), prior)

    def test_reload_mismatch_or_invalid_rows_is_terminal(self):
        prior = b'{"prior":"complete-pointer"}\n'
        attempted = b'{"attempted":"complete-pointer"}\n'
        pointer_path = self.data_dir / "routes/latest.json"
        pointer_path.parent.mkdir()
        bad_loaded = (
            _loaded_bundle(pointer={
                "route_cohort_id": COHORT_ID,
                "manifest_sha256": "d" * 64,
            }),
            _loaded_bundle(count=19),
            _loaded_bundle(strict=True),
        )
        for loaded in bad_loaded:
            with self.subTest(loaded=loaded):
                pointer_path.write_bytes(prior)

                def finalize(**kwargs):
                    pointer_path.write_bytes(attempted)
                    validator = kwargs.get("_postcommit_validator")
                    if validator is None:
                        return _complete_pointer()
                    try:
                        validator(_complete_pointer())
                    except BaseException:
                        if pointer_path.read_bytes() == attempted:
                            pointer_path.write_bytes(prior)
                        raise
                    return _complete_pointer()

                with patch.object(
                    runner,
                    "build_live_cex_research_universe",
                    return_value=self.universe,
                ), patch.object(
                    runner, "collect_route_cohort", return_value=_cohort(),
                ), patch.object(
                    runner,
                    "publish_route_cohort_bundle",
                    return_value=self.core_pointer,
                ), patch.object(
                    runner,
                    "finalize_public_cex_research_opportunities",
                    side_effect=finalize,
                ), patch.object(
                    runner,
                    "load_latest_complete_route_bundle",
                    return_value=loaded,
                ):
                    with self.assertRaisesRegex(
                        runner.LiveCexOpportunityRefreshError,
                        "reload_failed",
                    ):
                        runner.collect_and_publish_live_cex_research(
                            data_dir=self.data_dir,
                            public_fee_schedule_path=self.schedule,
                            deadline_seconds=30,
                            wall_clock=lambda: NOW,
                        )
                self.assertEqual(pointer_path.read_bytes(), prior)

    def test_direct_api_rejects_unsafe_paths_before_collection(self):
        symlink_schedule = Path(self.temporary.name) / "fee-link.csv"
        symlink_schedule.symlink_to(self.schedule)
        with patch.object(runner, "collect_route_cohort") as collect:
            with self.assertRaisesRegex(
                runner.LiveCexOpportunityRefreshError,
                "preflight_failed",
            ):
                runner.collect_and_publish_live_cex_research(
                    data_dir=self.data_dir,
                    public_fee_schedule_path=symlink_schedule,
                    deadline_seconds=30,
                    wall_clock=lambda: NOW,
                )
        collect.assert_not_called()


class LiveCexOpportunityMainTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.parent = Path(self.temporary.name)
        self.data_dir = self.parent / "new-data"
        self.schedule = self.parent / "fees.csv"
        self.schedule.write_text("schedule\n", encoding="utf-8")
        self.arguments = [
            "--data-dir", str(self.data_dir),
            "--public-fee-schedule", str(self.schedule),
        ]

    def test_main_creates_data_dir_and_prints_compact_receipt(self):
        receipt = {
            "schema": "live_cex_opportunity_refresh/v2",
            "status": "published",
            "token_pairs": ["UNI/USDT", "CAKE/USDT"],
            "venues": ["binance", "bybit"],
            "market_count": 4,
            "route_count": 4,
            "route_cohort_id": COHORT_ID,
            "manifest_sha256": COMPLETE_MANIFEST_SHA256,
            "opportunity_count": 20,
            "strict_eligible_count": 0,
            "served": False,
        }
        output = io.StringIO()
        with patch.object(
            runner,
            "collect_and_publish_live_cex_research",
            return_value=receipt,
        ) as collect, redirect_stdout(output):
            result = runner.main(self.arguments)

        self.assertEqual(result, 0)
        self.assertTrue(self.data_dir.is_dir())
        collect.assert_called_once()
        call = collect.call_args.kwargs
        self.assertEqual(call["data_dir"], self.data_dir)
        self.assertEqual(call["public_fee_schedule_path"], self.schedule)
        self.assertEqual(call["deadline_seconds"], 60)
        self.assertTrue(callable(call["wall_clock"]))
        self.assertEqual(json.loads(output.getvalue()), receipt)
        self.assertEqual(len(output.getvalue().splitlines()), 1)

    def test_main_prints_before_loopback_server_and_serves_only_after_success(self):
        receipt = {
            "schema": "live_cex_opportunity_refresh/v2",
            "status": "published",
            "token_pairs": ["UNI/USDT", "CAKE/USDT"],
            "venues": ["binance", "bybit"],
            "market_count": 4,
            "route_count": 4,
            "route_cohort_id": COHORT_ID,
            "manifest_sha256": COMPLETE_MANIFEST_SHA256,
            "opportunity_count": 20,
            "strict_eligible_count": 0,
            "served": False,
        }
        output = io.StringIO()

        def serve(**kwargs):
            self.assertTrue(output.getvalue())
            published = json.loads(output.getvalue())
            self.assertTrue(published["served"])
            self.assertEqual(kwargs, {"data_dir": self.data_dir, "port": 8765})

        with patch.object(
            runner,
            "collect_and_publish_live_cex_research",
            return_value=receipt,
        ), patch.object(
            runner, "serve_current_dashboard", side_effect=serve,
        ) as server, redirect_stdout(output):
            result = runner.main(self.arguments + ["--serve"])

        self.assertEqual(result, 0)
        server.assert_called_once()

    def test_reload_failure_never_serves_and_does_not_leak_exception(self):
        output = io.StringIO()
        errors = io.StringIO()
        failure = runner.LiveCexOpportunityRefreshError("reload_failed")
        with patch.object(
            runner,
            "collect_and_publish_live_cex_research",
            side_effect=failure,
        ), patch.object(
            runner, "serve_current_dashboard"
        ) as server, redirect_stdout(output), redirect_stderr(errors):
            result = runner.main(self.arguments + ["--serve"])

        self.assertEqual(result, 1)
        self.assertEqual(output.getvalue(), "")
        self.assertEqual(errors.getvalue(), "reload_failed\n")
        self.assertNotIn("exception", errors.getvalue())
        server.assert_not_called()

    def test_stable_failure_codes_and_interrupt_exit(self):
        for code in (
            "collection_failed", "publication_failed", "reload_failed",
        ):
            with self.subTest(code=code):
                errors = io.StringIO()
                with patch.object(
                    runner,
                    "collect_and_publish_live_cex_research",
                    side_effect=runner.LiveCexOpportunityRefreshError(code),
                ), redirect_stderr(errors):
                    result = runner.main(self.arguments)
                self.assertEqual(result, 1)
                self.assertEqual(errors.getvalue(), code + "\n")

        errors = io.StringIO()
        with patch.object(
            runner,
            "collect_and_publish_live_cex_research",
            side_effect=KeyboardInterrupt,
        ), redirect_stderr(errors):
            result = runner.main(self.arguments)
        self.assertEqual(result, 130)
        self.assertEqual(errors.getvalue(), "interrupted\n")

    def test_unsafe_paths_fail_preflight_before_collection(self):
        missing_parent_data = self.parent / "missing" / "data"
        symlink_parent = self.parent / "symlink-parent"
        real_parent = self.parent / "real-parent"
        real_parent.mkdir()
        symlink_parent.symlink_to(real_parent, target_is_directory=True)
        symlink_schedule = self.parent / "fees-link.csv"
        symlink_schedule.symlink_to(self.schedule)
        cases = (
            [
                "--data-dir", str(missing_parent_data),
                "--public-fee-schedule", str(self.schedule),
            ],
            [
                "--data-dir", str(symlink_parent / "data"),
                "--public-fee-schedule", str(self.schedule),
            ],
            [
                "--data-dir", str(self.data_dir),
                "--public-fee-schedule", str(symlink_schedule),
            ],
        )
        for arguments in cases:
            with self.subTest(arguments=arguments):
                errors = io.StringIO()
                with patch.object(
                    runner, "collect_and_publish_live_cex_research"
                ) as collect, redirect_stderr(errors):
                    result = runner.main(arguments)
                self.assertEqual(result, 1)
                self.assertEqual(errors.getvalue(), "preflight_failed\n")
                collect.assert_not_called()

    def test_main_parse_failures_never_echo_values_and_help_returns_zero(self):
        cases = (
            ([
                "--data-dir", str(self.data_dir),
                "--api-key", "TOPSECRET-API-KEY",
            ], "TOPSECRET-API-KEY"),
            (["--data-dir", "RELATIVE-SECRET-PATH"], "RELATIVE-SECRET-PATH"),
            ([
                "--data-dir", str(self.data_dir),
                "--deadline-seconds", "SECRET-DEADLINE",
            ], "SECRET-DEADLINE"),
            ([
                "--data-dir", str(self.data_dir),
                "--port", "SECRET-PORT",
            ], "SECRET-PORT"),
        )
        for arguments, secret in cases:
            with self.subTest(arguments=arguments):
                output = io.StringIO()
                errors = io.StringIO()
                with redirect_stdout(output), redirect_stderr(errors):
                    result = runner.main(arguments)
                self.assertEqual(result, 1)
                self.assertEqual(output.getvalue(), "")
                self.assertEqual(errors.getvalue(), "preflight_failed\n")
                self.assertNotIn(secret, errors.getvalue())

        output = io.StringIO()
        errors = io.StringIO()
        with redirect_stdout(output), redirect_stderr(errors):
            result = runner.main(["--help"])
        self.assertEqual(result, 0)
        self.assertIn("usage:", output.getvalue())
        self.assertEqual(errors.getvalue(), "")

    def test_serve_failure_uses_stable_code_without_payload(self):
        receipt = {
            "schema": "live_cex_opportunity_refresh/v2",
            "status": "published",
            "token_pairs": ["UNI/USDT", "CAKE/USDT"],
            "venues": ["binance", "bybit"],
            "market_count": 4,
            "route_count": 4,
            "route_cohort_id": COHORT_ID,
            "manifest_sha256": COMPLETE_MANIFEST_SHA256,
            "opportunity_count": 20,
            "strict_eligible_count": 0,
            "served": False,
        }
        output = io.StringIO()
        errors = io.StringIO()
        with patch.object(
            runner,
            "collect_and_publish_live_cex_research",
            return_value=receipt,
        ), patch.object(
            runner,
            "serve_current_dashboard",
            side_effect=RuntimeError("private server detail"),
        ), redirect_stdout(output), redirect_stderr(errors):
            result = runner.main(self.arguments + ["--serve"])

        self.assertEqual(result, 1)
        self.assertTrue(json.loads(output.getvalue())["served"])
        self.assertEqual(errors.getvalue(), "serve_failed\n")
        self.assertNotIn("private server detail", errors.getvalue())


if __name__ == "__main__":  # pragma: no cover
    unittest.main()
