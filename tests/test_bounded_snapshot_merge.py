import unittest

from scripts.bounded_snapshot_merge import (
    merge_exact_market_snapshot,
    require_aligned_depth_execution_lineage,
)


def row(
    market_id,
    *,
    snapshot_id="baseline-1",
    direction="",
    notional="",
    value="old",
    source_snapshot_id=None,
):
    result = {
        "snapshot_id": snapshot_id,
        "market_id": market_id,
        "direction": direction,
        "requested_notional_usd": notional,
        "value": value,
    }
    if source_snapshot_id is not None:
        result["source_snapshot_id"] = source_snapshot_id
    return result


class BoundedSnapshotMergeTest(unittest.TestCase):
    def test_depth_execution_bundle_requires_one_shared_source_publication(self):
        depth = [row("cex:binance:AAVE/USDT")]
        execution = [
            row(
                "cex:binance:AAVE/USDT",
                snapshot_id="execution-other",
                direction="buy_token",
                notional="1000",
                source_snapshot_id="execution-other",
            )
        ]

        with self.assertRaisesRegex(ValueError, "same source publication"):
            require_aligned_depth_execution_lineage(depth, execution)

    def test_replaces_only_exact_target_and_rebinds_publication_identity(self):
        baseline = [
            row("cex:binance:AAVE/USDT"),
            row("cex:coinbase:AAVE/USD"),
        ]
        candidate = [
            row(
                "cex:binance:AAVE/USDT",
                snapshot_id="candidate-2",
                value="new",
            )
        ]

        merged = merge_exact_market_snapshot(
            baseline,
            candidate,
            target_market_id="cex:binance:AAVE/USDT",
            market_id_for_row=lambda item: item["market_id"],
            row_identity=lambda item: item["market_id"],
        )

        self.assertEqual(len(merged), len(baseline))
        self.assertEqual(
            {item["snapshot_id"] for item in merged},
            {"candidate-2"},
        )
        by_market = {item["market_id"]: item for item in merged}
        self.assertEqual(by_market["cex:binance:AAVE/USDT"]["value"], "new")
        self.assertEqual(by_market["cex:coinbase:AAVE/USD"]["value"], "old")

    def test_target_insertion_is_explicit_and_preserves_existing_facts(self):
        baseline = [row("dex:eth:uniswap_v3:0xold:UNI")]
        candidate = [
            row(
                "dex:eth:uniswap_v3:0xnew:AAVE",
                snapshot_id="candidate-2",
                value="new",
            )
        ]

        with self.assertRaisesRegex(ValueError, "absent"):
            merge_exact_market_snapshot(
                baseline,
                candidate,
                target_market_id="dex:eth:uniswap_v3:0xnew:AAVE",
                market_id_for_row=lambda item: item["market_id"],
                row_identity=lambda item: item["market_id"],
            )

        merged = merge_exact_market_snapshot(
            baseline,
            candidate,
            target_market_id="dex:eth:uniswap_v3:0xnew:AAVE",
            market_id_for_row=lambda item: item["market_id"],
            row_identity=lambda item: item["market_id"],
            allow_target_insert=True,
        )

        self.assertEqual(len(merged), 2)
        self.assertEqual(
            {item["snapshot_id"] for item in merged},
            {"candidate-2"},
        )
        by_market = {item["market_id"]: item for item in merged}
        self.assertEqual(
            by_market["dex:eth:uniswap_v3:0xold:UNI"]["value"],
            "old",
        )
        self.assertEqual(
            by_market["dex:eth:uniswap_v3:0xnew:AAVE"]["value"],
            "new",
        )

    def test_execution_merge_requires_the_exact_scenario_key_set(self):
        target = "cex:binance:AAVE/USDT"
        other = "cex:coinbase:AAVE/USD"
        baseline = [
            row(
                target,
                direction=direction,
                notional=notional,
                source_snapshot_id="baseline-1",
            )
            for direction in ("buy_token", "sell_token")
            for notional in ("1000", "10000")
        ] + [
            row(
                other,
                direction="buy_token",
                notional="1000",
                source_snapshot_id="baseline-1",
            )
        ]
        incomplete_candidate = [
            row(
                target,
                snapshot_id="candidate-2",
                direction="buy_token",
                notional="1000",
                value="new",
                source_snapshot_id="candidate-2",
            )
        ]

        with self.assertRaisesRegex(ValueError, "scenario coverage"):
            merge_exact_market_snapshot(
                baseline,
                incomplete_candidate,
                target_market_id=target,
                market_id_for_row=lambda item: item["market_id"],
                row_identity=lambda item: (
                    item["market_id"],
                    item["direction"],
                    item["requested_notional_usd"],
                ),
                rebind_source_snapshot_id=True,
            )

    def test_execution_merge_rebinds_source_to_the_merged_depth_publication(self):
        target = "cex:binance:AAVE/USDT"
        other = "cex:coinbase:AAVE/USD"
        baseline = [
            row(
                target,
                direction="buy_token",
                notional="1000",
                source_snapshot_id="baseline-1",
            ),
            row(
                other,
                direction="buy_token",
                notional="1000",
                source_snapshot_id="baseline-1",
            ),
        ]
        candidate = [
            row(
                target,
                snapshot_id="candidate-2",
                direction="buy_token",
                notional="1000",
                value="new",
                source_snapshot_id="candidate-2",
            )
        ]

        merged = merge_exact_market_snapshot(
            baseline,
            candidate,
            target_market_id=target,
            market_id_for_row=lambda item: item["market_id"],
            row_identity=lambda item: (
                item["market_id"],
                item["direction"],
                item["requested_notional_usd"],
            ),
            rebind_source_snapshot_id=True,
        )

        self.assertEqual(
            {item["source_snapshot_id"] for item in merged},
            {"candidate-2"},
        )
        self.assertEqual(
            next(item for item in merged if item["market_id"] == other)["value"],
            "old",
        )

    def test_rejects_cross_market_candidate_and_missing_baseline(self):
        target = "cex:binance:AAVE/USDT"
        with self.assertRaisesRegex(ValueError, "exact target"):
            merge_exact_market_snapshot(
                [row(target)],
                [row("cex:coinbase:AAVE/USD", snapshot_id="candidate-2")],
                target_market_id=target,
                market_id_for_row=lambda item: item["market_id"],
                row_identity=lambda item: item["market_id"],
            )
        with self.assertRaisesRegex(ValueError, "baseline"):
            merge_exact_market_snapshot(
                [],
                [row(target, snapshot_id="candidate-2")],
                target_market_id=target,
                market_id_for_row=lambda item: item["market_id"],
                row_identity=lambda item: item["market_id"],
            )

    def test_rejects_same_generation_and_schema_drift(self):
        target = "cex:binance:AAVE/USDT"
        with self.assertRaisesRegex(ValueError, "new publication"):
            merge_exact_market_snapshot(
                [row(target)],
                [row(target)],
                target_market_id=target,
                market_id_for_row=lambda item: item["market_id"],
                row_identity=lambda item: item["market_id"],
            )
        candidate = row(target, snapshot_id="candidate-2")
        candidate["unexpected"] = "schema drift"
        with self.assertRaisesRegex(ValueError, "schema"):
            merge_exact_market_snapshot(
                [row(target)],
                [candidate],
                target_market_id=target,
                market_id_for_row=lambda item: item["market_id"],
                row_identity=lambda item: item["market_id"],
            )

    def test_execution_merge_rejects_incoherent_source_snapshot_lineage(self):
        target = "cex:binance:AAVE/USDT"
        baseline = [
            row(
                target,
                direction="buy_token",
                notional="1000",
                source_snapshot_id="different-baseline",
            )
        ]
        candidate = [
            row(
                target,
                snapshot_id="candidate-2",
                direction="buy_token",
                notional="1000",
                source_snapshot_id="candidate-2",
            )
        ]

        with self.assertRaisesRegex(ValueError, "source snapshot lineage"):
            merge_exact_market_snapshot(
                baseline,
                candidate,
                target_market_id=target,
                market_id_for_row=lambda item: item["market_id"],
                row_identity=lambda item: (
                    item["market_id"],
                    item["direction"],
                    item["requested_notional_usd"],
                ),
                rebind_source_snapshot_id=True,
            )


if __name__ == "__main__":
    unittest.main()
