import unittest

from scripts.uniswap_v3_math import (
    MAX_SQRT_RATIO,
    MAX_TICK,
    MAX_UINT256,
    MIN_SQRT_RATIO,
    MIN_TICK,
    Q96,
    count_initialized_ticks_crossed,
    compute_swap_step,
    get_amount0_delta,
    get_next_sqrt_price_from_input,
    get_next_sqrt_price_from_output,
    get_sqrt_ratio_at_tick,
    get_tick_at_sqrt_ratio,
    simulate_swap,
    sqrt_price_limit_for_bps,
)


class UniswapV3TickMathTest(unittest.TestCase):
    def test_tick_boundaries_match_uniswap_core_literals(self):
        self.assertEqual(get_sqrt_ratio_at_tick(MIN_TICK), 4_295_128_739)
        self.assertEqual(
            get_sqrt_ratio_at_tick(MAX_TICK),
            1_461_446_703_485_210_103_287_273_052_203_988_822_378_723_970_342,
        )
        self.assertEqual(get_sqrt_ratio_at_tick(0), 79_228_162_514_264_337_593_543_950_336)
        self.assertEqual(MIN_SQRT_RATIO, 4_295_128_739)
        self.assertEqual(MAX_SQRT_RATIO, get_sqrt_ratio_at_tick(MAX_TICK))

    def test_inverse_tick_is_greatest_tick_not_above_ratio(self):
        for tick in (-887_271, -60, -1, 0, 1, 60, 887_271):
            ratio = get_sqrt_ratio_at_tick(tick)
            self.assertEqual(get_tick_at_sqrt_ratio(ratio), tick)
            if tick < MAX_TICK:
                self.assertEqual(get_tick_at_sqrt_ratio(ratio + 1), tick)

    def test_price_band_limits_are_conservative_integer_boundaries(self):
        down = sqrt_price_limit_for_bps(Q96, 100, zero_for_one=True)
        up = sqrt_price_limit_for_bps(Q96, 100, zero_for_one=False)

        self.assertEqual(down, 78_831_026_366_734_652_303_669_917_532)
        self.assertEqual(up, 79_623_317_895_830_914_510_639_640_423)
        self.assertGreaterEqual(down * down * 10_000, Q96 * Q96 * 9_900)
        self.assertLessEqual(up * up * 10_000, Q96 * Q96 * 10_100)


class UniswapV3SqrtPriceMathTest(unittest.TestCase):
    def test_token1_input_vector_matches_uniswap_core(self):
        self.assertEqual(
            get_next_sqrt_price_from_input(
                Q96,
                10**18,
                10**17,
                False,
            ),
            87_150_978_765_690_771_352_898_345_369,
        )

    def test_token0_input_vector_matches_uniswap_core(self):
        self.assertEqual(
            get_next_sqrt_price_from_input(
                Q96,
                10**18,
                10**17,
                True,
            ),
            72_025_602_285_694_852_357_767_227_579,
        )

    def test_token0_output_vector_matches_uniswap_core(self):
        self.assertEqual(
            get_next_sqrt_price_from_output(
                Q96,
                10**18,
                10**17,
                False,
            ),
            88_031_291_682_515_930_659_493_278_152,
        )

    def test_token1_output_vector_matches_uniswap_core(self):
        self.assertEqual(
            get_next_sqrt_price_from_output(
                Q96,
                10**18,
                10**17,
                True,
            ),
            71_305_346_262_837_903_834_189_555_302,
        )

    def test_token0_input_uses_uint256_overflow_fallback(self):
        self.assertEqual(
            get_next_sqrt_price_from_input(
                Q96,
                1,
                MAX_UINT256 // 2,
                True,
            ),
            1,
        )

    def test_overflow_regression_round_trips_exact_token0_input(self):
        sqrt_price = 1_025_574_284_609_383_690_408_304_870_162_715_216_695_788_925_244
        liquidity = 50_015_962_439_936_049_619_261_659_728_067_971_248
        sqrt_price_next = get_next_sqrt_price_from_input(
            sqrt_price,
            liquidity,
            406,
            True,
        )

        self.assertEqual(
            sqrt_price_next,
            1_025_574_284_609_383_582_644_711_336_373_707_553_698_163_132_913,
        )
        self.assertEqual(
            get_amount0_delta(
                sqrt_price_next,
                sqrt_price,
                liquidity,
                True,
            ),
            406,
        )


class UniswapV3SwapMathTest(unittest.TestCase):
    def test_exact_output_is_capped_to_one_base_unit(self):
        step = compute_swap_step(
            417_332_158_212_080_721_273_783_715_441_582,
            1_452_870_262_520_218_020_823_638_996,
            159_344_665_391_607_089_467_575_320_103,
            -1,
            1,
        )

        self.assertEqual(step.sqrt_price_next_x96, 417_332_158_212_080_721_273_783_715_441_581)
        self.assertEqual(step.amount_in, 1)
        self.assertEqual(step.amount_out, 1)
        self.assertEqual(step.fee_amount, 1)

    def test_exact_input_capped_at_target_matches_uniswap_core_vector(self):
        step = compute_swap_step(
            Q96,
            79_623_317_895_830_914_510_639_640_423,
            2_000_000_000_000_000_000,
            1_000_000_000_000_000_000,
            600,
        )

        self.assertEqual(step.amount_in, 9_975_124_224_178_055)
        self.assertEqual(step.amount_out, 9_925_619_580_021_728)
        self.assertEqual(step.fee_amount, 5_988_667_735_148)
        self.assertEqual(step.sqrt_price_next_x96, 79_623_317_895_830_914_510_639_640_423)

    def test_exact_input_fully_spent_matches_uniswap_core_vector(self):
        step = compute_swap_step(
            Q96,
            250_541_448_375_047_931_186_413_801_569,
            2_000_000_000_000_000_000,
            1_000_000_000_000_000_000,
            600,
        )

        self.assertEqual(step.amount_in, 999_400_000_000_000_000)
        self.assertEqual(step.amount_out, 666_399_946_655_997_866)
        self.assertEqual(step.fee_amount, 600_000_000_000_000)
        self.assertEqual(step.amount_in + step.fee_amount, 1_000_000_000_000_000_000)

    def test_exact_output_fully_received_matches_uniswap_core_vector(self):
        step = compute_swap_step(
            Q96,
            10 * Q96,
            2_000_000_000_000_000_000,
            -1_000_000_000_000_000_000,
            600,
        )

        self.assertEqual(step.amount_in, 2_000_000_000_000_000_000)
        self.assertEqual(step.amount_out, 1_000_000_000_000_000_000)
        self.assertEqual(step.fee_amount, 1_200_720_432_259_356)
        self.assertEqual(step.sqrt_price_next_x96, 2 * Q96)

    def test_entire_small_input_is_taken_as_fee(self):
        step = compute_swap_step(
            2_413,
            79_887_613_182_836_312,
            1_985_041_575_832_132_834_610_021_537_970,
            10,
            1_872,
        )

        self.assertEqual(step.sqrt_price_next_x96, 2_413)
        self.assertEqual(step.amount_in, 0)
        self.assertEqual(step.amount_out, 0)
        self.assertEqual(step.fee_amount, 10)


class UniswapV3SimulationTest(unittest.TestCase):
    def test_frozen_mainnet_uni_weth_quoter_vectors_match_base_units(self):
        # Official QuoterV2 results observed at Ethereum block 25,840,333 for
        # the canonical UNI/WETH 0.3% pool. The literals are independent of the
        # Python collector and cover both exact-input and exact-output paths.
        state = {
            "sqrt_price_x96": 3_284_550_905_803_876_574_905_503_962,
            "current_tick": -63_666,
            "liquidity": 67_012_279_185_301_941_712_032,
            "fee_pips": 3_000,
            "initialized_ticks": {
                -63_720: -484_557_911_860_154_763_132_306,
                -63_660: 2_004_523_190_906_485_959_326,
            },
        }
        sell = simulate_swap(
            **state,
            amount_specified=237_442_131_959_897_433_784,
            zero_for_one=True,
            sqrt_price_limit_x96=1_703_225_506_495_531_874_124_623_042,
        )
        buy = simulate_swap(
            **state,
            amount_specified=-237_442_131_959_897_433_784,
            zero_for_one=False,
            sqrt_price_limit_x96=3_660_082_040_745_495_354_352_498_852,
        )

        self.assertEqual(sell.amount_out, 406_801_147_882_411_655)
        self.assertEqual(
            sell.sqrt_price_x96,
            3_284_069_947_569_844_652_251_334_788,
        )
        self.assertEqual(buy.amount_in, 409_373_052_227_973_579)
        self.assertEqual(
            buy.sqrt_price_x96,
            3_285_033_452_786_009_935_649_823_985,
        )
        self.assertEqual(
            count_initialized_ticks_crossed(
                tick_before=state["current_tick"],
                tick_after=sell.tick,
                tick_spacing=60,
                initialized_ticks=state["initialized_ticks"],
            ),
            1,
        )
        self.assertEqual(
            count_initialized_ticks_crossed(
                tick_before=state["current_tick"],
                tick_after=buy.tick,
                tick_spacing=60,
                initialized_ticks=state["initialized_ticks"],
            ),
            1,
        )

    def test_quoter_tick_counter_includes_the_compressed_start_bucket(self):
        self.assertEqual(
            count_initialized_ticks_crossed(
                tick_before=-63_666,
                tick_after=-63_669,
                tick_spacing=60,
                initialized_ticks={-63_660: 1},
            ),
            1,
        )

    def test_quoter_tick_counter_applies_directional_endpoint_rules(self):
        initialized_ticks = {-60: 1, 0: 1, 60: 1}
        self.assertEqual(
            count_initialized_ticks_crossed(
                tick_before=0,
                tick_after=60,
                tick_spacing=60,
                initialized_ticks=initialized_ticks,
            ),
            1,
        )
        self.assertEqual(
            count_initialized_ticks_crossed(
                tick_before=60,
                tick_after=0,
                tick_spacing=60,
                initialized_ticks=initialized_ticks,
            ),
            1,
        )

    def test_current_initialized_boundary_crosses_without_price_motion(self):
        result = simulate_swap(
            sqrt_price_x96=Q96,
            current_tick=0,
            liquidity=1_000_000_000_000_000_000,
            fee_pips=3_000,
            initialized_ticks={0: 200_000_000_000_000_000},
            amount_specified=1_000_000_000_000_000,
            zero_for_one=True,
            sqrt_price_limit_x96=get_sqrt_ratio_at_tick(-120),
        )

        self.assertTrue(result.complete)
        self.assertEqual(result.initialized_ticks_crossed, 1)
        self.assertEqual(result.liquidity, 800_000_000_000_000_000)

    def test_zero_liquidity_gap_advances_to_next_initialized_tick(self):
        result = simulate_swap(
            sqrt_price_x96=Q96,
            current_tick=0,
            liquidity=0,
            fee_pips=3_000,
            initialized_ticks={60: 1_000_000_000_000_000_000},
            amount_specified=1_000_000_000_000_000,
            zero_for_one=False,
            sqrt_price_limit_x96=get_sqrt_ratio_at_tick(120),
        )

        self.assertTrue(result.complete)
        self.assertEqual(result.initialized_ticks_crossed, 1)
        self.assertEqual(result.liquidity, 1_000_000_000_000_000_000)
        self.assertGreater(result.amount_out, 0)

    def test_one_for_zero_crosses_tick_and_applies_liquidity_net(self):
        result = simulate_swap(
            sqrt_price_x96=Q96,
            current_tick=0,
            liquidity=1_000_000_000_000_000_000,
            fee_pips=3_000,
            initialized_ticks={60: 250_000_000_000_000_000},
            amount_specified=5_000_000_000_000_000,
            zero_for_one=False,
            sqrt_price_limit_x96=get_sqrt_ratio_at_tick(120),
        )

        self.assertTrue(result.complete)
        self.assertEqual(result.terminal_reason, "amount_resolved")
        self.assertEqual(result.initialized_ticks_crossed, 1)
        self.assertEqual(result.liquidity, 1_250_000_000_000_000_000)
        self.assertGreaterEqual(result.tick, 60)
        self.assertEqual(result.amount_in, 5_000_000_000_000_000)
        self.assertGreater(result.amount_out, 0)
        self.assertGreater(result.fee_amount, 0)

    def test_scan_price_limit_returns_auditable_partial(self):
        limit = get_sqrt_ratio_at_tick(120)
        result = simulate_swap(
            sqrt_price_x96=Q96,
            current_tick=0,
            liquidity=1_000_000_000_000_000_000,
            fee_pips=3_000,
            initialized_ticks={60: 250_000_000_000_000_000},
            amount_specified=10**30,
            zero_for_one=False,
            sqrt_price_limit_x96=limit,
        )

        self.assertFalse(result.complete)
        self.assertEqual(result.terminal_reason, "sqrt_price_limit_reached")
        self.assertEqual(result.sqrt_price_x96, limit)
        self.assertEqual(result.initialized_ticks_crossed, 1)
        self.assertLess(result.amount_in, 10**30)

    def test_zero_for_one_crossing_subtracts_liquidity_net(self):
        result = simulate_swap(
            sqrt_price_x96=get_sqrt_ratio_at_tick(1),
            current_tick=1,
            liquidity=1_000_000_000_000_000_000,
            fee_pips=3_000,
            initialized_ticks={0: 200_000_000_000_000_000},
            amount_specified=1_000_000_000_000_000,
            zero_for_one=True,
            sqrt_price_limit_x96=get_sqrt_ratio_at_tick(-120),
        )

        self.assertTrue(result.complete)
        self.assertEqual(result.initialized_ticks_crossed, 1)
        self.assertEqual(result.liquidity, 800_000_000_000_000_000)
        self.assertLess(result.tick, 0)

    def test_exact_output_stops_before_tick_outside_proven_scan_window(self):
        requested_output = 10**30
        limit = get_sqrt_ratio_at_tick(120)
        result = simulate_swap(
            sqrt_price_x96=Q96,
            current_tick=0,
            liquidity=1_000_000_000_000_000_000,
            fee_pips=3_000,
            initialized_ticks={180: 250_000_000_000_000_000},
            amount_specified=-requested_output,
            zero_for_one=False,
            sqrt_price_limit_x96=limit,
        )

        self.assertFalse(result.complete)
        self.assertEqual(result.terminal_reason, "sqrt_price_limit_reached")
        self.assertEqual(result.sqrt_price_x96, limit)
        self.assertEqual(result.initialized_ticks_crossed, 0)
        self.assertEqual(result.liquidity, 1_000_000_000_000_000_000)
        self.assertLess(result.amount_specified_remaining, 0)
        self.assertEqual(
            result.amount_out - result.amount_specified_remaining,
            requested_output,
        )


if __name__ == "__main__":
    unittest.main()
