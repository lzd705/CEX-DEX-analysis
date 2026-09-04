from __future__ import annotations

import copy
import inspect
from pathlib import Path
import subprocess
import sys
import unittest

from dashboard import opportunity_facts
from scripts import check_dashboard_release, route_cost_topology
from scripts import route_opportunity, route_opportunity_pipeline, route_publication
from scripts.route_cost_topology import (
    _validate_historical_atomic_cost_component_matrix,
    live_complete_cost_component_keys,
)


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SCRIPTS_ROOT = PROJECT_ROOT / "scripts"

COHORT_ID = "cohort:" + "1" * 64
OPPORTUNITY_ID = "opportunity:" + "2" * 64
BUY_MARKET_ID = "dex:eth:uniswap_v2:" + "0x" + "3" * 40 + ":UNI"
SELL_MARKET_ID = "dex:eth:sushiswap_v2:" + "0x" + "4" * 40 + ":UNI"
ROUTE = {
    "route_mode": "atomic_onchain",
    "buy_market_id": BUY_MARKET_ID,
    "sell_market_id": SELL_MARKET_ID,
}

LIVE_DEX_DEX_KEYS = frozenset({
    ("buy", "pool_swap_fee"),
    ("buy", "network_gas"),
    ("buy", "router_or_integrator_fee"),
    ("buy", "token_transfer_tax"),
    ("sell", "pool_swap_fee"),
    ("sell", "network_gas"),
    ("sell", "router_or_integrator_fee"),
    ("sell", "token_transfer_tax"),
    ("route", "rebalancing_or_transfer"),
    ("route", "mev_buffer"),
})

HISTORICAL_ATOMIC_COMPONENT_MATRIX = (
    ("buy", "pool_swap_fee", "bounded_estimate", True),
    ("buy", "router_or_integrator_fee", "bounded_estimate", False),
    ("buy", "token_transfer_tax", "bounded_estimate", False),
    ("sell", "pool_swap_fee", "bounded_estimate", True),
    ("sell", "router_or_integrator_fee", "bounded_estimate", False),
    ("sell", "token_transfer_tax", "bounded_estimate", False),
    ("route", "network_gas", "assumed", False),
    ("route", "rebalancing_or_transfer", "not_applicable", False),
    ("route", "mev_buffer", "assumed", False),
)

RECEIPT_SOURCE = "5" * 64
TRACE_SOURCE = "6" * 64
POOL_FEE_SOURCE_BY_LEG = {"buy": RECEIPT_SOURCE, "sell": RECEIPT_SOURCE}
POOL_FEE_AMOUNT_BY_LEG = {"buy": "3", "sell": "4"}
ZERO_PROOF_BY_KEY = {
    ("buy", "router_or_integrator_fee"): RECEIPT_SOURCE,
    ("buy", "token_transfer_tax"): RECEIPT_SOURCE,
    ("sell", "router_or_integrator_fee"): RECEIPT_SOURCE,
    ("sell", "token_transfer_tax"): RECEIPT_SOURCE,
}
GAS_SOURCE = RECEIPT_SOURCE
POLICY_SOURCE = "c" * 64


def _row(
    leg,
    component_type,
    value_status,
    embedded,
    *,
    amount,
    rate,
    source,
    source_hash,
):
    if leg == "buy":
        market_id = BUY_MARKET_ID
        direction = "buy_token"
    elif leg == "sell":
        market_id = SELL_MARKET_ID
        direction = "sell_token"
    else:
        market_id = ""
        direction = "route"
    return {
        "contract_version": "1",
        "cohort_id": COHORT_ID,
        "opportunity_id": OPPORTUNITY_ID,
        "leg": leg,
        "market_id": market_id,
        "direction": direction,
        "requested_notional_usd": "1000",
        "target_token_quantity": "10",
        "component_type": component_type,
        "value_status": value_status,
        "amount_usd": amount,
        "rate_bps": rate,
        "basis": "historical replay proof",
        "strict_eligible": False,
        "embedded_in_leg_quote": embedded,
        "observed_at": None,
        "valid_until": None,
        "source": source,
        "source_record_sha256": source_hash,
        "reason_code": None,
    }


def historical_rows():
    return [
        _row(
            "buy", "pool_swap_fee", "bounded_estimate", True,
            amount="3", rate="30", source="receipt",
            source_hash=POOL_FEE_SOURCE_BY_LEG["buy"],
        ),
        _row(
            "buy", "router_or_integrator_fee", "bounded_estimate", False,
            amount="0", rate="0", source="receipt",
            source_hash=ZERO_PROOF_BY_KEY[("buy", "router_or_integrator_fee")],
        ),
        _row(
            "buy", "token_transfer_tax", "bounded_estimate", False,
            amount="0", rate="0", source="receipt",
            source_hash=ZERO_PROOF_BY_KEY[("buy", "token_transfer_tax")],
        ),
        _row(
            "sell", "pool_swap_fee", "bounded_estimate", True,
            amount="4", rate="30", source="receipt",
            source_hash=POOL_FEE_SOURCE_BY_LEG["sell"],
        ),
        _row(
            "sell", "router_or_integrator_fee", "bounded_estimate", False,
            amount="0", rate="0", source="receipt",
            source_hash=ZERO_PROOF_BY_KEY[("sell", "router_or_integrator_fee")],
        ),
        _row(
            "sell", "token_transfer_tax", "bounded_estimate", False,
            amount="0", rate="0", source="receipt",
            source_hash=ZERO_PROOF_BY_KEY[("sell", "token_transfer_tax")],
        ),
        _row(
            "route", "network_gas", "assumed", False,
            amount="2", rate=None, source="receipt",
            source_hash=GAS_SOURCE,
        ),
        _row(
            "route", "rebalancing_or_transfer", "not_applicable", False,
            amount=None, rate=None, source="trace",
            source_hash=TRACE_SOURCE,
        ),
        _row(
            "route", "mev_buffer", "assumed", False,
            amount="1", rate="10", source="policy",
            source_hash=POLICY_SOURCE,
        ),
    ]


def validate(rows, **changes):
    arguments = {
        "expected_cohort_id": COHORT_ID,
        "expected_opportunity_id": OPPORTUNITY_ID,
        "expected_pool_fee_source_sha256_by_leg": POOL_FEE_SOURCE_BY_LEG,
        "expected_pool_fee_amount_usd_by_leg": POOL_FEE_AMOUNT_BY_LEG,
        "expected_zero_fee_proof_sha256_by_key": ZERO_PROOF_BY_KEY,
        "expected_gas_amount_usd": "2",
        "expected_gas_source_sha256": GAS_SOURCE,
        "expected_transfer_source_sha256": TRACE_SOURCE,
        "expected_mev_amount_usd": "1",
        "expected_policy_sha256": POLICY_SOURCE,
    }
    arguments.update(changes)
    return _validate_historical_atomic_cost_component_matrix(
        ROUTE, rows, **arguments
    )


class LiveCostTopologyTests(unittest.TestCase):
    def test_terminal_cex_cost_rows_have_one_canonical_literal_shape(self):
        self.assertTrue(hasattr(
            route_cost_topology,
            "build_terminal_cex_cost_components",
        ))
        route = {
            "buy_market_id": "cex:binance:CAKE/USDT",
            "sell_market_id": "cex:bybit:CAKE/USDT",
        }
        rows = route_cost_topology.build_terminal_cex_cost_components(
            cohort_id=COHORT_ID,
            opportunity_id=OPPORTUNITY_ID,
            route=route,
            requested_notional_usd="1000",
            reason_code="invalid_state_timestamp",
        )

        self.assertEqual(
            [
                (
                    row["leg"], row["market_id"], row["direction"],
                    row["component_type"], row["value_status"],
                    row["basis"], row["source"], row["reason_code"],
                )
                for row in rows
            ],
            [
                (
                    "buy", "cex:binance:CAKE/USDT", "buy_token",
                    "venue_taker_fee", "unavailable",
                    "retained route timing proves route unavailable",
                    "retained route timing", "invalid_state_timestamp",
                ),
                (
                    "route", "", "route", "rebalancing_or_transfer",
                    "unavailable",
                    "retained route timing proves route unavailable",
                    "retained route timing", "invalid_state_timestamp",
                ),
                (
                    "sell", "cex:bybit:CAKE/USDT", "sell_token",
                    "venue_taker_fee", "unavailable",
                    "retained route timing proves route unavailable",
                    "retained route timing", "invalid_state_timestamp",
                ),
            ],
        )
        self.assertTrue(all(
            row["target_token_quantity"] is None
            and row["amount_usd"] is None
            and row["rate_bps"] is None
            and row["strict_eligible"] is False
            and row["embedded_in_leg_quote"] is False
            and row["observed_at"] is None
            and row["valid_until"] is None
            and row["source_record_sha256"] is None
            for row in rows
        ))

    def test_terminal_cex_cost_validator_rejects_semantic_mutations(self):
        self.assertTrue(hasattr(
            route_cost_topology,
            "validate_terminal_cex_cost_components",
        ))
        route = {
            "buy_market_id": "cex:binance:CAKE/USDT",
            "sell_market_id": "cex:bybit:CAKE/USDT",
        }
        arguments = {
            "cohort_id": COHORT_ID,
            "opportunity_id": OPPORTUNITY_ID,
            "route": route,
            "requested_notional_usd": "1000",
            "reason_code": "invalid_state_timestamp",
        }
        baseline = route_cost_topology.build_terminal_cex_cost_components(
            **arguments
        )
        for label, field, value in (
            ("basis", "basis", "caller asserted terminal basis"),
            ("source", "source", "caller asserted terminal source"),
            ("status", "value_status", "failed"),
        ):
            with self.subTest(label=label):
                mutated = copy.deepcopy(baseline)
                mutated[0][field] = value
                with self.assertRaises(ValueError):
                    route_cost_topology.validate_terminal_cex_cost_components(
                        mutated,
                        **arguments,
                    )

    def test_dex_dex_inventory_is_the_existing_ten_key_contract(self):
        self.assertEqual(
            live_complete_cost_component_keys(ROUTE), LIVE_DEX_DEX_KEYS
        )
        self.assertIsInstance(live_complete_cost_component_keys(ROUTE), frozenset)

    def test_cex_and_mixed_routes_preserve_existing_component_inventories(self):
        cex = "cex:binance:UNI/USDT"
        self.assertEqual(
            live_complete_cost_component_keys({
                "buy_market_id": cex,
                "sell_market_id": "cex:bybit:UNI/USDT",
            }),
            frozenset({
                ("buy", "venue_taker_fee"),
                ("sell", "venue_taker_fee"),
                ("route", "rebalancing_or_transfer"),
            }),
        )
        self.assertEqual(
            live_complete_cost_component_keys({
                "buy_market_id": cex,
                "sell_market_id": SELL_MARKET_ID,
            }),
            frozenset({
                ("buy", "venue_taker_fee"),
                ("sell", "pool_swap_fee"),
                ("sell", "network_gas"),
                ("sell", "router_or_integrator_fee"),
                ("sell", "token_transfer_tax"),
                ("route", "rebalancing_or_transfer"),
                ("route", "mev_buffer"),
            }),
        )

    def test_unknown_or_incomplete_market_identity_rejects(self):
        for route in (
            {"buy_market_id": "unknown", "sell_market_id": SELL_MARKET_ID},
            {"buy_market_id": BUY_MARKET_ID},
        ):
            with self.subTest(route=route):
                with self.assertRaises((KeyError, TypeError, ValueError)):
                    live_complete_cost_component_keys(route)


class HistoricalAtomicCostTopologyTests(unittest.TestCase):
    def test_valid_closed_nine_row_matrix_passes(self):
        self.assertIsNone(validate(historical_rows()))
        self.assertEqual(
            tuple(
                (
                    row["leg"], row["component_type"], row["value_status"],
                    row["embedded_in_leg_quote"],
                )
                for row in historical_rows()
            ),
            HISTORICAL_ATOMIC_COMPONENT_MATRIX,
        )

    def test_missing_duplicate_extra_and_live_dex_inventory_reject(self):
        rows = historical_rows()
        variants = (
            rows[:-1],
            rows + [copy.deepcopy(rows[0])],
            rows + [_row(
                "buy", "network_gas", "assumed", False,
                amount="2", rate=None, source="forbidden leg gas",
                source_hash=GAS_SOURCE,
            )],
        )
        for candidate in variants:
            with self.subTest(size=len(candidate)):
                with self.assertRaises((TypeError, ValueError)):
                    validate(candidate)
        self.assertNotEqual(
            frozenset((row[0], row[1]) for row in HISTORICAL_ATOMIC_COMPONENT_MATRIX),
            LIVE_DEX_DEX_KEYS,
        )

    def test_every_amount_status_and_embedded_mutation_rejects(self):
        for index in range(len(HISTORICAL_ATOMIC_COMPONENT_MATRIX)):
            for field in ("amount_usd", "value_status", "embedded_in_leg_quote"):
                rows = historical_rows()
                if field == "amount_usd":
                    rows[index][field] = (
                        "0" if rows[index][field] is None else "999"
                    )
                elif field == "value_status":
                    rows[index][field] = "failed"
                    rows[index]["reason_code"] = "mutated"
                    rows[index]["amount_usd"] = None
                    rows[index]["rate_bps"] = None
                else:
                    rows[index][field] = not rows[index][field]
                with self.subTest(index=index, field=field):
                    with self.assertRaises((TypeError, ValueError)):
                        validate(rows)

    def test_lineage_route_and_strict_eligibility_mutations_reject(self):
        mutations = (
            (0, "cohort_id", "cohort:" + "d" * 64),
            (1, "opportunity_id", "opportunity:" + "e" * 64),
            (0, "market_id", SELL_MARKET_ID),
            (3, "direction", "buy_token"),
            (8, "strict_eligible", True),
        )
        for index, field, value in mutations:
            rows = historical_rows()
            rows[index][field] = value
            with self.subTest(index=index, field=field):
                with self.assertRaises((TypeError, ValueError)):
                    validate(rows)

        wrong_mode = dict(ROUTE, route_mode="prepositioned_inventory")
        with self.assertRaises((TypeError, ValueError)):
            _validate_historical_atomic_cost_component_matrix(
                wrong_mode,
                historical_rows(),
                expected_cohort_id=COHORT_ID,
                expected_opportunity_id=OPPORTUNITY_ID,
                expected_pool_fee_source_sha256_by_leg=POOL_FEE_SOURCE_BY_LEG,
                expected_pool_fee_amount_usd_by_leg=POOL_FEE_AMOUNT_BY_LEG,
                expected_zero_fee_proof_sha256_by_key=ZERO_PROOF_BY_KEY,
                expected_gas_amount_usd="2",
                expected_gas_source_sha256=GAS_SOURCE,
                expected_transfer_source_sha256=TRACE_SOURCE,
                expected_mev_amount_usd="1",
                expected_policy_sha256=POLICY_SOURCE,
            )

    def test_pool_zero_fee_gas_transfer_and_policy_bindings_reject_drift(self):
        row_mutations = (
            (0, "rate_bps", "29"),
            (0, "source_record_sha256", "d" * 64),
            (1, "amount_usd", "0.1"),
            (1, "rate_bps", "1"),
            (2, "source_record_sha256", "d" * 64),
            (6, "source_record_sha256", "d" * 64),
            (7, "source", "caller asserted topology"),
            (7, "source_record_sha256", "d" * 64),
            (8, "rate_bps", "11"),
            (8, "source_record_sha256", "d" * 64),
        )
        for index, field, value in row_mutations:
            rows = historical_rows()
            rows[index][field] = value
            with self.subTest(index=index, field=field):
                with self.assertRaises((TypeError, ValueError)):
                    validate(rows)

        expectation_mutations = (
            {"expected_pool_fee_amount_usd_by_leg": {"buy": "30", "sell": "4"}},
            {"expected_gas_amount_usd": "20"},
            {"expected_mev_amount_usd": "2"},
            {"expected_policy_sha256": "d" * 64},
        )
        for changes in expectation_mutations:
            with self.subTest(changes=changes):
                with self.assertRaises((TypeError, ValueError)):
                    validate(historical_rows(), **changes)

    def test_phase_two_proof_roles_are_exact(self):
        expected_roles = (
            "receipt", "receipt", "receipt", "receipt", "receipt",
            "receipt", "receipt", "trace", "policy",
        )
        for index, expected_role in enumerate(expected_roles):
            rows = historical_rows()
            rows[index]["source"] = expected_role + "-forged"
            with self.subTest(index=index, field="source"):
                with self.assertRaises((TypeError, ValueError)):
                    validate(rows)

    def test_historical_proof_rows_are_timeless(self):
        for index in range(len(HISTORICAL_ATOMIC_COMPONENT_MATRIX)):
            for field in ("observed_at", "valid_until"):
                rows = historical_rows()
                rows[index][field] = "2026-08-01T12:00:00Z"
                with self.subTest(index=index, field=field):
                    with self.assertRaises((TypeError, ValueError)):
                        validate(rows)

    def test_transfer_trace_sha_is_exact(self):
        rows = historical_rows()
        rows[7]["source_record_sha256"] = "d" * 64
        with self.assertRaises((TypeError, ValueError)):
            validate(rows)
        with self.assertRaises((TypeError, ValueError)):
            validate(
                historical_rows(),
                expected_transfer_source_sha256="d" * 64,
            )

    def test_mev_amount_is_independently_recomputed_from_notional_and_ten_bps(self):
        rows = historical_rows()
        rows[8]["amount_usd"] = "2"
        with self.assertRaises((TypeError, ValueError)):
            validate(rows, expected_mev_amount_usd="2")

    def test_arbitrary_profile_or_context_parameters_are_not_accepted(self):
        with self.assertRaises(TypeError):
            validate(historical_rows(), profile="historical")
        with self.assertRaises(TypeError):
            validate(historical_rows(), context=object())


class CostTopologyBoundaryTests(unittest.TestCase):
    def test_consumers_expose_no_divergent_topology_copies(self):
        copied_names = (
            (route_opportunity, "_expected_component_keys"),
            (route_opportunity_pipeline, "_terminal_cex_cost_components"),
            (route_publication, "_expected_component_keys_for_complete_route"),
            (opportunity_facts, "_expected_component_keys"),
            (check_dashboard_release, "_route_expected_component_keys"),
            (check_dashboard_release, "_opportunity_expected_component_keys"),
        )
        for module, name in copied_names:
            with self.subTest(module=module.__name__, name=name):
                self.assertFalse(hasattr(module, name))

    def test_live_wrapper_signatures_remain_frozen(self):
        expected = {
            route_cost_topology.build_terminal_cex_cost_components: (
                "cohort_id", "opportunity_id", "route",
                "requested_notional_usd", "reason_code",
            ),
            route_cost_topology.validate_terminal_cex_cost_components: (
                "rows", "cohort_id", "opportunity_id", "route",
                "requested_notional_usd", "reason_code",
            ),
            route_opportunity.build_route_opportunity: (
                "cohort_id", "route", "requested_notional_usd", "common_target",
                "buy_leg", "sell_leg", "buy_quote", "sell_quote",
                "buy_quote_evidence", "sell_quote_evidence",
                "buy_usd_projection", "sell_usd_projection", "cost_components",
                "mode_evidence", "now", "publication_attestation",
            ),
            route_publication._validate_complete_route_bundle_at: (
                "parent_fd", "bundle_name", "bundle_path",
                "expected_route_cohort_id", "expected_manifest_sha256",
                "require_directory_identity",
            ),
            route_publication._complete_manifest_payload: ("bundle", "files"),
            route_publication._complete_artifact_bytes: ("bundle",),
            route_publication.load_latest_complete_route_bundle: (
                "routes_root", "core_root",
            ),
        }
        for function, names in expected.items():
            with self.subTest(function=function.__name__):
                self.assertEqual(tuple(inspect.signature(function).parameters), names)
        self.assertEqual(
            tuple(inspect.signature(
                route_cost_topology._validate_historical_atomic_cost_component_matrix
            ).parameters),
            (
                "route", "rows", "expected_cohort_id",
                "expected_opportunity_id",
                "expected_pool_fee_source_sha256_by_leg",
                "expected_pool_fee_amount_usd_by_leg",
                "expected_zero_fee_proof_sha256_by_key",
                "expected_gas_amount_usd", "expected_gas_source_sha256",
                "expected_transfer_source_sha256", "expected_mev_amount_usd",
                "expected_policy_sha256",
            ),
        )
        self.assertEqual(
            inspect.signature(
                route_publication.load_latest_complete_route_bundle
            ).parameters["routes_root"].default,
            route_publication.DEFAULT_ROUTE_ROOT,
        )

    def test_low_level_historical_validator_stays_out_of_dashboard_and_release(self):
        historical_name = "scripts.historical_route_publication"
        missing = object()
        previous_module = sys.modules.get(historical_name, missing)
        completed = subprocess.run(
            [
                sys.executable,
                "-B",
                "-c",
                (
                    "import sys; "
                    "from dashboard import opportunity_facts; "
                    "from scripts import check_dashboard_release, "
                    "route_cost_topology; "
                    "private_name = "
                    "'_validate_historical_atomic_cost_component_matrix'; "
                    "violations = (private_name in "
                    "opportunity_facts.__dict__, private_name in "
                    "check_dashboard_release.__dict__, hasattr("
                    "route_cost_topology, 'HistoricalReplayBuildContext'), "
                    "'scripts.historical_route_publication' in "
                    "sys.modules); "
                    "raise SystemExit(any(violations))"
                ),
            ],
            cwd=str(PROJECT_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr.decode("utf-8"))
        self.assertIs(sys.modules.get(historical_name, missing), previous_module)

    def test_package_and_direct_script_import_modes_work(self):
        completed = subprocess.run(
            [
                sys.executable,
                "-B",
                "-c",
                (
                    "import route_cost_topology; import route_opportunity; "
                    "import route_opportunity_pipeline; "
                    "import route_publication; import check_dashboard_release"
                ),
            ],
            cwd=str(SCRIPTS_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr.decode("utf-8"))

    def test_modified_production_modules_compile(self):
        completed = subprocess.run(
            [
                sys.executable,
                "-B",
                "-m",
                "py_compile",
                "scripts/route_cost_topology.py",
                "scripts/route_opportunity.py",
                "scripts/route_opportunity_pipeline.py",
                "scripts/route_publication.py",
                "dashboard/opportunity_facts.py",
                "scripts/check_dashboard_release.py",
            ],
            cwd=str(PROJECT_ROOT),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
        )
        self.assertEqual(completed.returncode, 0, completed.stderr.decode("utf-8"))


if __name__ == "__main__":
    unittest.main()
