import copy
import json
import random
import unittest

from scripts.route_universe import (
    build_route_universe,
    execution_capability_by_market,
    route_universe_sha256,
    select_route_legs,
)


NOTIONALS = [1000, 5000, 10000, 50000, 100000]
OBSERVED_AT = "2026-08-01T12:00:00Z"


def market(market_id, market_type, **overrides):
    row = {
        "market_id": market_id,
        "market_type": market_type,
        "token_symbol": "UNI",
        "observed_at": OBSERVED_AT,
        "lifecycle_status": "active",
        "execution_adapter_status": "supported",
    }
    row.update(overrides)
    return row


def depth(market_id, amount, **overrides):
    row = {
        "market_id": market_id,
        "status": "observed",
        "state_observed_at": OBSERVED_AT,
        "total_depth_100bps_usd": str(amount),
    }
    if market_id.startswith("cex:"):
        row.update({
            "bid_depth_100bps_usd": str(amount),
            "ask_depth_100bps_usd": str(amount),
        })
    else:
        row.update({
            "buy_depth_100bps_usd": str(amount),
            "sell_depth_100bps_usd": str(amount),
        })
    row.update(overrides)
    return row


def execution(market_id, capacity, **overrides):
    rows = []
    for direction in ("buy_token", "sell_token"):
        row = {
            "market_id": market_id,
            "direction": direction,
            "requested_notional_usd": str(capacity),
            "status": "observed",
            "state_observed_at": OBSERVED_AT,
        }
        row.update(overrides)
        rows.append(row)
    return rows


def cex_volume(market_id, value, **overrides):
    row = {
        "market_id": market_id,
        "selected_window_usd": str(value),
        "observed_at": OBSERVED_AT,
    }
    row.update(overrides)
    return row


def dex_volume(market_id, value, **overrides):
    row = {
        "market_id": market_id,
        "volume_24h_usd": str(value),
        "observed_at": OBSERVED_AT,
    }
    row.update(overrides)
    return row


def tvl(market_id, value, **overrides):
    row = {
        "market_id": market_id,
        "tvl_usd": str(value),
        "observed_at": OBSERVED_AT,
    }
    row.update(overrides)
    return row


class RouteUniverseSelectionTests(unittest.TestCase):
    def test_selection_is_bounded_excludes_unusable_rows_and_retains_priority_inputs(self):
        cex_ids = ["cex:venue:{}".format(letter) for letter in "ABCDEFG"]
        dex_ids = ["dex:eth:swap:0x{}:UNI".format(letter.lower()) for letter in "ABCDEFG"]
        catalog = [market(identifier, "cex") for identifier in cex_ids]
        catalog.extend(market(identifier, "dex") for identifier in dex_ids)
        catalog[1]["lifecycle_status"] = "withheld"
        catalog[2]["execution_adapter_status"] = "unsupported"
        depths = [depth(identifier, 1000 + index) for index, identifier in enumerate(cex_ids + dex_ids)]
        depths[3]["status"] = "failed"
        depths[4]["state_observed_at"] = "not-a-time"
        execution_rows = []
        for index, identifier in enumerate(cex_ids + dex_ids):
            execution_rows.extend(execution(identifier, 10000 + index * 1000))
        cex_volumes = [cex_volume(identifier, 100 + index) for index, identifier in enumerate(cex_ids)]
        dex_volumes = [dex_volume(identifier, 200 + index) for index, identifier in enumerate(dex_ids)]
        tvl_rows = [tvl(identifier, 300 + index) for index, identifier in enumerate(dex_ids)]

        selected = select_route_legs(
            catalog, depths, execution_rows, cex_volumes, dex_volumes, tvl_rows,
            selection_window={"start": "2026-08-01", "end": "2026-08-01"},
            candidate_source_generation="catalog-sha-1",
        )

        self.assertEqual(
            [row["market_id"] for row in selected if row["market_type"] == "cex"],
            [cex_ids[6], cex_ids[5], cex_ids[0]],
        )
        self.assertEqual(
            [row["market_id"] for row in selected if row["market_type"] == "dex"],
            [dex_ids[6], dex_ids[5], dex_ids[4]],
        )
        for row in selected:
            self.assertIn("selection_rank", row)
            self.assertEqual(row["candidate_source_generation"], "catalog-sha-1")
            self.assertEqual(row["selection_window"], {"end": "2026-08-01", "start": "2026-08-01"})
            self.assertEqual(
                set(row["selection_inputs"]),
                {
                    "execution_capability", "proved_execution_capacity_usd",
                    "observed_100bps_depth_usd", "cex_selected_window_usd",
                    "dex_24h_usd", "dex_tvl_usd",
                },
            )

    def test_selection_requires_positive_two_sided_depth_for_each_market_type(self):
        cex_valid = "cex:valid:UNI/USDT"
        dex_valid = "dex:eth:swap:0xvalid:UNI"
        excluded = (
            (
                "cex:zero-ask:UNI/USDT",
                "cex",
                {"bid_depth_100bps_usd": "50", "ask_depth_100bps_usd": "0"},
            ),
            (
                "cex:zero-bid:UNI/USDT",
                "cex",
                {"bid_depth_100bps_usd": "0", "ask_depth_100bps_usd": "50"},
            ),
            (
                "dex:eth:swap:0xzero-sell:UNI",
                "dex",
                {"buy_depth_100bps_usd": "50", "sell_depth_100bps_usd": "0"},
            ),
            (
                "dex:eth:swap:0xzero-buy:UNI",
                "dex",
                {"buy_depth_100bps_usd": "0", "sell_depth_100bps_usd": "50"},
            ),
        )
        catalog = [market(cex_valid, "cex"), market(dex_valid, "dex")]
        catalog.extend(market(market_id, market_type) for market_id, market_type, _ in excluded)
        depths = [depth(cex_valid, 100), depth(dex_valid, 100)]
        depths.extend(depth(market_id, 100, **overrides) for market_id, _market_type, overrides in excluded)
        execution_rows = []
        for row in catalog:
            execution_rows.extend(execution(row["market_id"], 1000))

        selected = select_route_legs(
            catalog, depths, execution_rows, [], [], [],
            selection_window={"start": "2026-08-01", "end": "2026-08-01"},
            candidate_source_generation="catalog-sha-two-sided",
        )

        self.assertEqual(
            [row["market_id"] for row in selected], [cex_valid, dex_valid]
        )

    def test_conflicting_duplicate_catalog_market_ids_fail_closed_independent_of_order(self):
        market_id = "cex:duplicate:UNI/USDT"
        catalog = [
            market(market_id, "cex", token_symbol="UNI"),
            market(market_id, "cex", token_symbol="AAVE"),
        ]
        depths = [depth(market_id, 100)]
        execution_rows = execution(market_id, 1000)

        for seed in range(10):
            shuffled = copy.deepcopy(catalog)
            random.Random(seed).shuffle(shuffled)
            with self.subTest(seed=seed):
                with self.assertRaisesRegex(ValueError, "duplicate canonical market ID"):
                    select_route_legs(
                        shuffled, depths, execution_rows, [], [], [],
                        selection_window={"start": "2026-08-01", "end": "2026-08-01"},
                        candidate_source_generation="catalog-sha-duplicate",
                    )

    def test_execution_capability_requires_current_two_direction_observed_capacity(self):
        market_id = "cex:venue:UNI/USDT"
        rows = execution(market_id, 10000)
        rows.append({
            "market_id": "cex:unsupported:UNI/USDT",
            "direction": "buy_token",
            "requested_notional_usd": "100000",
            "status": "unsupported",
            "state_observed_at": OBSERVED_AT,
        })
        rows.append({
            "market_id": market_id,
            "direction": "buy_token",
            "requested_notional_usd": "100000",
            "status": "observed",
            "state_observed_at": "invalid",
        })

        capabilities = execution_capability_by_market(rows)

        self.assertEqual(
            capabilities[market_id],
            {"execution_capability": "proved", "proved_execution_capacity_usd": "10000"},
        )
        self.assertEqual(
            capabilities["cex:unsupported:UNI/USDT"],
            {"execution_capability": "unsupported", "proved_execution_capacity_usd": None},
        )

    def test_current_unsupported_execution_row_overrides_conflicting_observed_rows(self):
        market_id = "cex:conflicted:UNI/USDT"
        rows = execution(market_id, 10000)
        rows.append({
            "market_id": market_id,
            "direction": "sell_token",
            "requested_notional_usd": "50000",
            "status": "unsupported",
            "state_observed_at": OBSERVED_AT,
        })

        capabilities = execution_capability_by_market(rows)

        self.assertEqual(
            capabilities[market_id],
            {"execution_capability": "unsupported", "proved_execution_capacity_usd": None},
        )


class RouteUniverseDeterminismTests(unittest.TestCase):
    def test_shuffled_inputs_produce_identical_legs_routes_json_bytes_and_sha256(self):
        catalog = [
            market("cex:zeta:UNI/USDT", "cex"),
            market("cex:alpha:UNI/USDT", "cex"),
            market("dex:eth:swap:0xpool1:UNI", "dex"),
            market("dex:arb:swap:0xpool2:UNI", "dex"),
        ]
        depths = [depth(row["market_id"], 5000) for row in catalog]
        execution_rows = []
        for row in catalog:
            execution_rows.extend(execution(row["market_id"], 10000))
        cex_volumes = [cex_volume(row["market_id"], 500) for row in catalog if row["market_type"] == "cex"]
        dex_volumes = [dex_volume(row["market_id"], 500) for row in catalog if row["market_type"] == "dex"]
        tvl_rows = [tvl(row["market_id"], 500) for row in catalog if row["market_type"] == "dex"]
        baseline = build_route_universe(
            catalog, depths, execution_rows, cex_volumes, dex_volumes, tvl_rows,
            selection_window={"end": "2026-08-01", "start": "2026-08-01"},
            candidate_source_generation="catalog-sha-2",
        )
        baseline_bytes = json.dumps(
            baseline, ensure_ascii=False, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")

        for seed in range(10):
            inputs = [
                copy.deepcopy(catalog), copy.deepcopy(depths), copy.deepcopy(execution_rows),
                copy.deepcopy(cex_volumes), copy.deepcopy(dex_volumes), copy.deepcopy(tvl_rows),
            ]
            random.Random(seed).shuffle(inputs[0])
            random.Random(seed + 10).shuffle(inputs[1])
            random.Random(seed + 20).shuffle(inputs[2])
            random.Random(seed + 30).shuffle(inputs[3])
            random.Random(seed + 40).shuffle(inputs[4])
            random.Random(seed + 50).shuffle(inputs[5])
            actual = build_route_universe(
                *inputs,
                selection_window={"start": "2026-08-01", "end": "2026-08-01"},
                candidate_source_generation="catalog-sha-2",
            )
            actual_bytes = json.dumps(
                actual, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            ).encode("utf-8")
            self.assertEqual(actual["selected_legs"], baseline["selected_legs"])
            self.assertEqual(actual["routes"], baseline["routes"])
            self.assertEqual(actual_bytes, baseline_bytes)
            self.assertEqual(route_universe_sha256(actual), route_universe_sha256(baseline))

        self.assertEqual(
            [row["market_id"] for row in baseline["selected_legs"][:2]],
            ["cex:alpha:UNI/USDT", "cex:zeta:UNI/USDT"],
        )

    def test_routes_are_directed_use_the_five_notional_grid_and_mark_cross_chain_dex_research_only(self):
        catalog = [
            market("cex:alpha:UNI/USDT", "cex"),
            market("dex:eth:swap:0xpool1:UNI", "dex"),
            market("dex:arb:swap:0xpool2:UNI", "dex"),
        ]
        depths = [depth(row["market_id"], 5000) for row in catalog]
        execution_rows = []
        for row in catalog:
            execution_rows.extend(execution(row["market_id"], 10000))
        universe = build_route_universe(
            catalog, depths, execution_rows,
            [cex_volume("cex:alpha:UNI/USDT", 1)],
            [dex_volume("dex:eth:swap:0xpool1:UNI", 1), dex_volume("dex:arb:swap:0xpool2:UNI", 1)],
            [tvl("dex:eth:swap:0xpool1:UNI", 1), tvl("dex:arb:swap:0xpool2:UNI", 1)],
            selection_window={"start": "2026-08-01", "end": "2026-08-01"},
            candidate_source_generation="catalog-sha-3",
        )

        routes = {row["route_id"]: row for row in universe["routes"]}
        forward = "route:UNI:cex:alpha:UNI/USDT->dex:eth:swap:0xpool1:UNI:prepositioned_inventory"
        reverse = "route:UNI:dex:eth:swap:0xpool1:UNI->cex:alpha:UNI/USDT:prepositioned_inventory"
        cross_chain = "route:UNI:dex:arb:swap:0xpool2:UNI->dex:eth:swap:0xpool1:UNI:research_only"
        self.assertIn(forward, routes)
        self.assertIn(reverse, routes)
        self.assertNotEqual(forward, reverse)
        self.assertEqual(routes[forward]["requested_notionals_usd"], NOTIONALS)
        self.assertEqual(routes[cross_chain]["route_class"], "research_only")
        self.assertEqual(routes[cross_chain]["settlement_reason"], "unsupported_cross_chain_settlement")


if __name__ == "__main__":
    unittest.main()
