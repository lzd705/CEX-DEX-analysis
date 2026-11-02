import unittest
from decimal import Decimal

from scripts.uni_experiments import eligible_pair, first_delayed_pair, evaluate_quantity, make_rules, make_target
from scripts.analyze_uni_experiments import analyze


class UniExperimentsTests(unittest.TestCase):
    def book(self, venue, bid="6.03", ask="6.04", size="100000"):
        return {"venue": venue, "status": "observed", "received_mono": 10,
                "request_seconds": 0.2, "source_age_seconds": None,
                "bids": [[bid, size]], "asks": [[ask, size]]}

    def rules(self, venue):
        return make_rules({"market_id": "cex:" + venue + ":UNI/USDT", "base_asset": "UNI",
            "quote_asset": "USDT", "base_unit_decimals": 8, "quote_unit_decimals": 8,
            "base_increment": "0.01", "quote_increment": "0.00000001",
            "min_base_quantity": "0.01", "min_quote_notional": "5",
            "observed_at": "2026-11-01T00:00:00Z", "valid_until": "2026-11-02T00:00:00Z"}, "a" * 64)

    def test_zero_fee_and_paid_fee_cashflows_are_different_and_match_quantity(self):
        buy = self.book("binance", "5.99", "6")
        sell = self.book("bybit", "6.03", "6.04")
        rules = {v: self.rules(v) for v in ("binance", "bybit")}
        target = make_target(buy, sell, rules, "1000")
        free = evaluate_quantity(buy, sell, rules, target, ("0", "0"))
        paid = evaluate_quantity(buy, sell, rules, target, ("10", "20"))
        self.assertEqual(Decimal(free["net"]), target.quantity * Decimal("0.03"))
        self.assertLess(Decimal(paid["net"]), Decimal(free["net"]))
        self.assertEqual(Decimal(paid["buy_net_quantity"]), target.quantity)
        self.assertEqual(Decimal(paid["sell_debit_quantity"]), target.quantity)

    def test_partial_depth_never_becomes_a_complete_pnl(self):
        rules = {v: self.rules(v) for v in ("binance", "bybit")}
        buy, sell = self.book("binance", ask="6"), self.book("bybit")
        target = make_target(buy, sell, rules, "1000")
        buy["asks"] = [["6", "1"]]
        result = evaluate_quantity(buy, sell, rules, target, ("0", "0"))
        self.assertIsNone(result["net"])

    def test_pair_excludes_slow_skewed_and_stale_records(self):
        a, b = self.book("binance"), self.book("bybit")
        self.assertTrue(eligible_pair({"books": [a, b]})[0])
        b["source_age_seconds"] = 10
        self.assertFalse(eligible_pair({"books": [a, b]})[0])
        b["source_age_seconds"] = None
        b["received_mono"] = 20
        self.assertFalse(eligible_pair({"books": [a, b]})[0])

    def test_delay_uses_first_pair_after_decision_plus_horizon_without_skipping_failure(self):
        pairs = [{"decision_mono": n, "id": i} for i, n in enumerate([10, 14.9, 15.2, 20.5])]
        self.assertEqual(first_delayed_pair(pairs, 0, 5)["id"], 2)
        self.assertIsNone(first_delayed_pair(pairs, 3, 5))

    def test_analysis_keeps_original_quantity_and_missing_tail_denominator(self):
        pairs = [
            {"index": 0, "decision_mono": 0, "decision_at": "start", "books": [
                self.book("binance", "5.99", "6"), self.book("bybit", "6.03", "6.04")]},
            {"index": 1, "decision_mono": 5, "decision_at": "end", "books": [
                self.book("binance", "11.99", "12"), self.book("bybit", "12.03", "12.04")]},
        ]
        rules = {v: self.rules(v) for v in ("binance", "bybit")}
        rows, delays, summary = analyze(pairs, rules)
        initial = next(r for r in rows if r["sample"] == 0 and r["buy"] == "binance"
                       and r["notional"] == "1000" and r["fee_scenario"] == "zero_counterfactual")
        future = next(r for r in delays if r["sample"] == 0 and r["buy"] == "binance"
                      and r["notional"] == "1000" and r["fee_scenario"] == "zero_counterfactual"
                      and r["horizon_seconds"] == 5)
        self.assertEqual(future["quantity"], initial["quantity"])
        self.assertEqual(Decimal(future["future_net"]), Decimal(initial["quantity"]) * Decimal("0.03"))
        group = next(r for r in summary["delay_summary"] if r["buy"] == "binance"
                     and r["notional"] == "1000" and r["fee_scenario"] == "zero_counterfactual"
                     and r["horizon"] == 5)
        self.assertEqual((group["all_origins"], group["paired"], group["initial_positive"],
                          group["positive_with_future"], group["positive_at_future"]), (2, 1, 2, 1, 1))

    def test_fee_scenarios_are_monotone_without_changing_net_quantity(self):
        buy, sell = self.book("binance", "5.99", "6"), self.book("bybit", "6.03", "6.04")
        rules = {v: self.rules(v) for v in ("binance", "bybit")}
        target = make_target(buy, sell, rules, "1000")
        values = [evaluate_quantity(buy, sell, rules, target, (str(f), str(f)))
                  for f in (0, 1, 2, 5, 10)]
        self.assertEqual([Decimal(v["net"]) for v in values],
                         sorted((Decimal(v["net"]) for v in values), reverse=True))
        self.assertTrue(all(Decimal(v["buy_net_quantity"]) == target.quantity
                            == Decimal(v["sell_debit_quantity"]) for v in values))
