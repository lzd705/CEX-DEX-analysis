import unittest

from scripts.uniswap_v3_math import (
    MAX_SQRT_RATIO,
    MAX_TICK,
    MAX_UINT256,
    MIN_SQRT_RATIO,
    MIN_TICK,
    Q96,
    compute_swap_step,
    count_initialized_ticks_crossed,
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
        self.assertEqual(get_sqrt_ratio_at_tick(0), Q96)
        self.assertEqual(MIN_SQRT_RATIO, 4_295_128_739)
        self.assertEqual(MAX_SQRT_RATIO, get_sqrt_ratio_at_tick(MAX_TICK))

    def test_inverse_tick_is_greatest_tick_not_above_ratio(self):
        for tick in (-887_271, -60, -1, 0, 1, 60, 887_271):
            ratio = get_sqrt_ratio_at_tick(tick)
            self.assertEqual(get_tick_at_sqrt_ratio(ratio), tick)
            self.assertEqual(get_tick_at_sqrt_ratio(ratio + 1), tick)

    def test_price_band_limits_are_conservative_integer_boundaries(self):
        self.assertEqual(
            sqrt_price_limit_for_bps(Q96, 100, zero_for_one=True),
            78_831_026_366_734_652_303_669_917_532,
        )
        self.assertEqual(
            sqrt_price_limit_for_bps(Q96, 100, zero_for_one=False),
            79_623_317_895_830_914_510_639_640_423,
        )


class UniswapV3SqrtPriceMathTest(unittest.TestCase):
    def test_input_and_output_vectors_match_uniswap_core(self):
        self.assertEqual(
            get_next_sqrt_price_from_input(Q96, 10**18, 10**17, False),
            87_150_978_765_690_771_352_898_345_369,
        )
        self.assertEqual(
            get_next_sqrt_price_from_input(Q96, 10**18, 10**17, True),
            72_025_602_285_694_852_357_767_227_579,
        )
        self.assertEqual(
            get_next_sqrt_price_from_output(Q96, 10**18, 10**17, False),
            88_031_291_682_515_930_659_493_278_152,
        )
        self.assertEqual(
            get_next_sqrt_price_from_output(Q96, 10**18, 10**17, True),
            71_305_346_262_837_903_834_189_555_302,
        )

    def test_token0_input_uses_uint256_overflow_fallback(self):
        self.assertEqual(
            get_next_sqrt_price_from_input(Q96, 1, MAX_UINT256 // 2, True), 1
        )

    def test_overflow_fallback_preserves_exact_input_rounding(self):
        current = 1_025_574_284_609_383_690_408_304_870_162_715_216_695_788_925_244
        next_price = get_next_sqrt_price_from_input(
            current, 50_015_962_439_936_049_619_261_659_728_067_971_248, 406, True
        )
        self.assertEqual(
            next_price,
            1_025_574_284_609_383_582_644_711_336_373_707_553_698_163_132_913,
        )
        self.assertEqual(
            get_amount0_delta(next_price, current, 50_015_962_439_936_049_619_261_659_728_067_971_248, True),
            406,
        )


class UniswapV3SwapMathTest(unittest.TestCase):
    def test_exact_output_rounding_is_capped_to_one_base_unit(self):
        step = compute_swap_step(
            417_332_158_212_080_721_273_783_715_441_582,
            1_452_870_262_520_218_020_823_638_996,
            159_344_665_391_607_089_467_575_320_103,
            -1,
            1,
        )
        self.assertEqual(step.sqrt_price_next_x96, 417_332_158_212_080_721_273_783_715_441_581)
        self.assertEqual((step.amount_in, step.amount_out, step.fee_amount), (1, 1, 1))

    def test_exact_input_fully_spent_matches_uniswap_core_vector(self):
        step = compute_swap_step(
            Q96, 250_541_448_375_047_931_186_413_801_569,
            2_000_000_000_000_000_000, 1_000_000_000_000_000_000, 600,
        )
        self.assertEqual((step.amount_in, step.amount_out, step.fee_amount), (
            999_400_000_000_000_000, 666_399_946_655_997_866, 600_000_000_000_000,
        ))


class UniswapV3SimulationTest(unittest.TestCase):
    def test_tick_crossing_applies_liquidity_net_by_direction(self):
        up = simulate_swap(
            sqrt_price_x96=Q96, current_tick=0, liquidity=10**18, fee_pips=3000,
            initialized_ticks={60: 250_000_000_000_000_000}, amount_specified=5_000_000_000_000_000,
            zero_for_one=False, sqrt_price_limit_x96=get_sqrt_ratio_at_tick(120),
        )
        down = simulate_swap(
            sqrt_price_x96=get_sqrt_ratio_at_tick(1), current_tick=1, liquidity=10**18,
            fee_pips=3000, initialized_ticks={0: 200_000_000_000_000_000},
            amount_specified=1_000_000_000_000_000, zero_for_one=True,
            sqrt_price_limit_x96=get_sqrt_ratio_at_tick(-120),
        )
        self.assertEqual((up.initialized_ticks_crossed, up.liquidity), (1, 1_250_000_000_000_000_000))
        self.assertEqual((down.initialized_ticks_crossed, down.liquidity), (1, 800_000_000_000_000_000))

    def test_scan_price_limit_returns_auditable_partial(self):
        limit = get_sqrt_ratio_at_tick(120)
        result = simulate_swap(
            sqrt_price_x96=Q96, current_tick=0, liquidity=10**18, fee_pips=3000,
            initialized_ticks={60: 250_000_000_000_000_000}, amount_specified=10**30,
            zero_for_one=False, sqrt_price_limit_x96=limit,
        )
        self.assertFalse(result.complete)
        self.assertEqual((result.terminal_reason, result.sqrt_price_x96), ("sqrt_price_limit_reached", limit))

    def test_exact_output_at_scan_bound_preserves_unresolved_amount(self):
        requested = 10**30
        result = simulate_swap(
            sqrt_price_x96=Q96, current_tick=0, liquidity=10**18, fee_pips=3000,
            initialized_ticks={180: 250_000_000_000_000_000}, amount_specified=-requested,
            zero_for_one=False, sqrt_price_limit_x96=get_sqrt_ratio_at_tick(120),
        )
        self.assertFalse(result.complete)
        self.assertLess(result.amount_specified_remaining, 0)
        self.assertEqual(result.amount_out - result.amount_specified_remaining, requested)

    def test_quoter_tick_counter_uses_directional_endpoints(self):
        ticks = {-60: 1, 0: 1, 60: 1}
        self.assertEqual(count_initialized_ticks_crossed(tick_before=0, tick_after=60, tick_spacing=60, initialized_ticks=ticks), 1)
        self.assertEqual(count_initialized_ticks_crossed(tick_before=60, tick_after=0, tick_spacing=60, initialized_ticks=ticks), 1)


if __name__ == "__main__":
    unittest.main()
