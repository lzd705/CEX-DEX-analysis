"""Protocol-exact integer math for canonical Uniswap V3 pool quotes."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Mapping


Q96 = 1 << 96
Q128 = 1 << 128
MAX_UINT128 = (1 << 128) - 1
MAX_UINT160 = (1 << 160) - 1
MAX_UINT256 = (1 << 256) - 1
MIN_TICK = -887_272
MAX_TICK = 887_272
MIN_SQRT_RATIO = 4_295_128_739
MAX_SQRT_RATIO = 1_461_446_703_485_210_103_287_273_052_203_988_822_378_723_970_342
FEE_DENOMINATOR = 1_000_000

_TICK_MULTIPLIERS = (
    0xFFFcb933BD6FAD37AA2D162D1A594001, 0xFFF97272373D413259A46990580E213A,
    0xFFF2E50F5F656932EF12357CF3C7FDCC, 0xFFE5CACA7E10E4E61C3624EAA0941CD0,
    0xFFCB9843D60F6159C9DB58835C926644, 0xFF973B41FA98C081472E6896DFB254C0,
    0xFF2EA16466C96A3843EC78B326B52861, 0xFE5DEE046A99A2A811C461F1969C3053,
    0xFCBE86C7900A88AEDCFFC83B479AA3A4, 0xF987A7253AC413176F2B074CF7815E54,
    0xF3392B0822B70005940C7A398E4B70F3, 0xE7159475A2C29B7443B29C7FA6E889D9,
    0xD097F3BDFD2022B8845AD8F792AA5825, 0xA9F746462D870FDF8A65DC1F90E061E5,
    0x70D869A156D2A1B890BB3DF62BAF32F7, 0x31BE135F97D08FD981231505542FCFA6,
    0x09AA508B5B7A84E1C677DE54F3E99BC9, 0x005D6AF8DEDB81196699C329225EE604,
    0x0002216E584F5FA1EA926041BEDFE98, 0x000048A170391F7DC42444E8FA2,
)


@dataclass(frozen=True)
class SwapStep:
    sqrt_price_next_x96: int
    amount_in: int
    amount_out: int
    fee_amount: int


@dataclass(frozen=True)
class SwapResult:
    amount_in: int
    amount_out: int
    fee_amount: int
    amount_specified_remaining: int
    sqrt_price_x96: int
    tick: int
    liquidity: int
    steps: int
    initialized_ticks_crossed: int
    complete: bool
    terminal_reason: str


def _require_uint(value, bits, label):
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError("{} must be an integer".format(label))
    if value < 0 or value >= 1 << bits:
        raise ValueError("{} does not fit uint{}".format(label, bits))
    return value


def div_rounding_up(numerator, denominator):
    if numerator < 0 or denominator <= 0:
        raise ValueError("rounding division inputs are invalid")
    quotient, remainder = divmod(numerator, denominator)
    return quotient + (1 if remainder else 0)


def mul_div(a, b, denominator):
    if a < 0 or b < 0 or denominator <= 0:
        raise ValueError("mulDiv inputs are invalid")
    result = a * b // denominator
    if result > MAX_UINT256:
        raise ValueError("mulDiv result exceeds uint256")
    return result


def mul_div_rounding_up(a, b, denominator):
    result = mul_div(a, b, denominator)
    if (a * b) % denominator:
        if result == MAX_UINT256:
            raise ValueError("rounded mulDiv result exceeds uint256")
        result += 1
    return result


def get_sqrt_ratio_at_tick(tick):
    """Return ``sqrt(1.0001**tick) * 2**96`` with core rounding."""
    if not isinstance(tick, int) or isinstance(tick, bool):
        raise ValueError("tick must be an integer")
    absolute_tick = abs(tick)
    if absolute_tick > MAX_TICK:
        raise ValueError("tick outside Uniswap V3 bounds")
    ratio = Q128
    for bit, multiplier in enumerate(_TICK_MULTIPLIERS):
        if absolute_tick & (1 << bit):
            ratio = ratio * multiplier >> 128
    if tick > 0:
        ratio = MAX_UINT256 // ratio
    quotient, remainder = divmod(ratio, 1 << 32)
    return quotient + (1 if remainder else 0)


def get_tick_at_sqrt_ratio(sqrt_price_x96):
    _require_uint(sqrt_price_x96, 160, "sqrt_price_x96")
    if not MIN_SQRT_RATIO <= sqrt_price_x96 < MAX_SQRT_RATIO:
        raise ValueError("sqrt ratio outside inverse TickMath bounds")
    low, high = MIN_TICK, MAX_TICK
    while low < high:
        middle = (low + high + 1) // 2
        if get_sqrt_ratio_at_tick(middle) <= sqrt_price_x96:
            low = middle
        else:
            high = middle - 1
    return low


def get_amount0_delta(sqrt_ratio_a_x96, sqrt_ratio_b_x96, liquidity, round_up):
    _require_uint(sqrt_ratio_a_x96, 160, "sqrt_ratio_a_x96")
    _require_uint(sqrt_ratio_b_x96, 160, "sqrt_ratio_b_x96")
    _require_uint(liquidity, 128, "liquidity")
    if sqrt_ratio_a_x96 > sqrt_ratio_b_x96:
        sqrt_ratio_a_x96, sqrt_ratio_b_x96 = sqrt_ratio_b_x96, sqrt_ratio_a_x96
    if sqrt_ratio_a_x96 == 0:
        raise ValueError("lower sqrt ratio must be positive")
    numerator1 = liquidity << 96
    numerator2 = sqrt_ratio_b_x96 - sqrt_ratio_a_x96
    if round_up:
        return div_rounding_up(mul_div_rounding_up(numerator1, numerator2, sqrt_ratio_b_x96), sqrt_ratio_a_x96)
    return mul_div(numerator1, numerator2, sqrt_ratio_b_x96) // sqrt_ratio_a_x96


def get_amount1_delta(sqrt_ratio_a_x96, sqrt_ratio_b_x96, liquidity, round_up):
    _require_uint(sqrt_ratio_a_x96, 160, "sqrt_ratio_a_x96")
    _require_uint(sqrt_ratio_b_x96, 160, "sqrt_ratio_b_x96")
    _require_uint(liquidity, 128, "liquidity")
    if sqrt_ratio_a_x96 > sqrt_ratio_b_x96:
        sqrt_ratio_a_x96, sqrt_ratio_b_x96 = sqrt_ratio_b_x96, sqrt_ratio_a_x96
    difference = sqrt_ratio_b_x96 - sqrt_ratio_a_x96
    if round_up:
        return mul_div_rounding_up(liquidity, difference, Q96)
    return mul_div(liquidity, difference, Q96)


def get_next_sqrt_price_from_amount0_rounding_up(sqrt_price_x96, liquidity, amount, add):
    _require_uint(sqrt_price_x96, 160, "sqrt_price_x96")
    _require_uint(liquidity, 128, "liquidity")
    _require_uint(amount, 256, "amount")
    if amount == 0:
        return sqrt_price_x96
    numerator1 = liquidity << 96
    product = amount * sqrt_price_x96
    if add:
        if product <= MAX_UINT256:
            denominator = numerator1 + product
            if denominator <= MAX_UINT256 and denominator >= numerator1:
                result = mul_div_rounding_up(numerator1, sqrt_price_x96, denominator)
                return _require_uint(result, 160, "next_sqrt_price_x96")
        result = div_rounding_up(numerator1, numerator1 // sqrt_price_x96 + amount)
        return _require_uint(result, 160, "next_sqrt_price_x96")
    if product > MAX_UINT256 or numerator1 <= product:
        raise ValueError("token0 output exceeds virtual reserves")
    return _require_uint(mul_div_rounding_up(numerator1, sqrt_price_x96, numerator1 - product), 160, "next_sqrt_price_x96")


def get_next_sqrt_price_from_amount1_rounding_down(sqrt_price_x96, liquidity, amount, add):
    _require_uint(sqrt_price_x96, 160, "sqrt_price_x96")
    _require_uint(liquidity, 128, "liquidity")
    _require_uint(amount, 256, "amount")
    if liquidity == 0:
        raise ValueError("active liquidity is zero")
    if add:
        quotient = (amount << 96) // liquidity if amount <= MAX_UINT160 else mul_div(amount, Q96, liquidity)
        return _require_uint(sqrt_price_x96 + quotient, 160, "next_sqrt_price_x96")
    quotient = div_rounding_up(amount << 96, liquidity) if amount <= MAX_UINT160 else mul_div_rounding_up(amount, Q96, liquidity)
    if sqrt_price_x96 <= quotient:
        raise ValueError("token1 output exceeds virtual reserves")
    return sqrt_price_x96 - quotient


def get_next_sqrt_price_from_input(sqrt_price_x96, liquidity, amount_in, zero_for_one):
    if sqrt_price_x96 <= 0 or liquidity <= 0:
        raise ValueError("price and active liquidity must be positive")
    if zero_for_one:
        return get_next_sqrt_price_from_amount0_rounding_up(sqrt_price_x96, liquidity, amount_in, True)
    return get_next_sqrt_price_from_amount1_rounding_down(sqrt_price_x96, liquidity, amount_in, True)


def get_next_sqrt_price_from_output(sqrt_price_x96, liquidity, amount_out, zero_for_one):
    if sqrt_price_x96 <= 0 or liquidity <= 0:
        raise ValueError("price and active liquidity must be positive")
    if zero_for_one:
        return get_next_sqrt_price_from_amount1_rounding_down(sqrt_price_x96, liquidity, amount_out, False)
    return get_next_sqrt_price_from_amount0_rounding_up(sqrt_price_x96, liquidity, amount_out, False)


def compute_swap_step(sqrt_ratio_current_x96, sqrt_ratio_target_x96, liquidity, amount_remaining, fee_pips):
    _require_uint(sqrt_ratio_current_x96, 160, "sqrt_ratio_current_x96")
    _require_uint(sqrt_ratio_target_x96, 160, "sqrt_ratio_target_x96")
    _require_uint(liquidity, 128, "liquidity")
    if liquidity == 0:
        raise ValueError("active liquidity is zero")
    if not isinstance(amount_remaining, int) or amount_remaining == 0:
        raise ValueError("amount_remaining must be a non-zero integer")
    if not isinstance(fee_pips, int) or not 0 <= fee_pips < FEE_DENOMINATOR:
        raise ValueError("fee_pips is invalid")
    zero_for_one = sqrt_ratio_current_x96 >= sqrt_ratio_target_x96
    exact_input = amount_remaining >= 0
    amount_in = amount_out = 0
    if exact_input:
        amount_remaining_less_fee = mul_div(amount_remaining, FEE_DENOMINATOR - fee_pips, FEE_DENOMINATOR)
        amount_in = get_amount0_delta(sqrt_ratio_target_x96, sqrt_ratio_current_x96, liquidity, True) if zero_for_one else get_amount1_delta(sqrt_ratio_current_x96, sqrt_ratio_target_x96, liquidity, True)
        sqrt_ratio_next_x96 = sqrt_ratio_target_x96 if amount_remaining_less_fee >= amount_in else get_next_sqrt_price_from_input(sqrt_ratio_current_x96, liquidity, amount_remaining_less_fee, zero_for_one)
    else:
        amount_out = get_amount1_delta(sqrt_ratio_target_x96, sqrt_ratio_current_x96, liquidity, False) if zero_for_one else get_amount0_delta(sqrt_ratio_current_x96, sqrt_ratio_target_x96, liquidity, False)
        sqrt_ratio_next_x96 = sqrt_ratio_target_x96 if -amount_remaining >= amount_out else get_next_sqrt_price_from_output(sqrt_ratio_current_x96, liquidity, -amount_remaining, zero_for_one)
    reached_target = sqrt_ratio_target_x96 == sqrt_ratio_next_x96
    if zero_for_one:
        if not (reached_target and exact_input):
            amount_in = get_amount0_delta(sqrt_ratio_next_x96, sqrt_ratio_current_x96, liquidity, True)
        if not (reached_target and not exact_input):
            amount_out = get_amount1_delta(sqrt_ratio_next_x96, sqrt_ratio_current_x96, liquidity, False)
    else:
        if not (reached_target and exact_input):
            amount_in = get_amount1_delta(sqrt_ratio_current_x96, sqrt_ratio_next_x96, liquidity, True)
        if not (reached_target and not exact_input):
            amount_out = get_amount0_delta(sqrt_ratio_current_x96, sqrt_ratio_next_x96, liquidity, False)
    if not exact_input and amount_out > -amount_remaining:
        amount_out = -amount_remaining
    fee_amount = amount_remaining - amount_in if exact_input and not reached_target else mul_div_rounding_up(amount_in, fee_pips, FEE_DENOMINATOR - fee_pips)
    return SwapStep(sqrt_ratio_next_x96, amount_in, amount_out, fee_amount)


def sqrt_price_limit_for_bps(sqrt_price_x96, band_bps, *, zero_for_one):
    _require_uint(sqrt_price_x96, 160, "sqrt_price_x96")
    if not isinstance(band_bps, int) or not 0 < band_bps < 10_000:
        raise ValueError("band_bps must be an integer inside (0, 10000)")
    factor = 10_000 - band_bps if zero_for_one else 10_000 + band_bps
    numerator = sqrt_price_x96 * sqrt_price_x96 * factor
    result = math.isqrt(numerator // 10_000)
    if zero_for_one and result * result * 10_000 < numerator:
        result += 1
    if zero_for_one:
        if not MIN_SQRT_RATIO < result < sqrt_price_x96:
            raise ValueError("downward price band is outside protocol bounds")
    elif not sqrt_price_x96 < result < MAX_SQRT_RATIO:
        raise ValueError("upward price band is outside protocol bounds")
    return result


def _validated_ticks(initialized_ticks):
    result = {}
    for tick, liquidity_net in initialized_ticks.items():
        if not isinstance(tick, int) or not MIN_TICK <= tick <= MAX_TICK:
            raise ValueError("initialized tick is invalid")
        if not isinstance(liquidity_net, int) or not -(1 << 127) <= liquidity_net < 1 << 127:
            raise ValueError("liquidity_net is invalid")
        result[tick] = liquidity_net
    return result


def _signed_div_toward_zero(numerator, denominator):
    quotient = abs(numerator) // denominator
    return -quotient if numerator < 0 else quotient


def count_initialized_ticks_crossed(*, tick_before, tick_after, tick_spacing, initialized_ticks):
    if not isinstance(tick_before, int) or not MIN_TICK <= tick_before <= MAX_TICK:
        raise ValueError("tick_before is invalid")
    if not isinstance(tick_after, int) or not MIN_TICK <= tick_after <= MAX_TICK:
        raise ValueError("tick_after is invalid")
    if not isinstance(tick_spacing, int) or isinstance(tick_spacing, bool) or tick_spacing <= 0:
        raise ValueError("tick_spacing must be a positive integer")
    ticks = _validated_ticks(initialized_ticks)
    if any(tick % tick_spacing for tick in ticks):
        raise ValueError("initialized tick is not aligned to tick spacing")
    before = _signed_div_toward_zero(tick_before, tick_spacing)
    after = _signed_div_toward_zero(tick_after, tick_spacing)
    count = sum(1 for tick in ticks if min(before, after) <= tick // tick_spacing <= max(before, after))
    if tick_before > tick_after and tick_after in ticks:
        count -= 1
    if tick_before < tick_after and tick_before in ticks:
        count -= 1
    if count < 0:
        raise ValueError("initialized tick counter underflow")
    return count


def simulate_swap(*, sqrt_price_x96, current_tick, liquidity, fee_pips, initialized_ticks, amount_specified, zero_for_one, sqrt_price_limit_x96):
    _require_uint(sqrt_price_x96, 160, "sqrt_price_x96")
    _require_uint(sqrt_price_limit_x96, 160, "sqrt_price_limit_x96")
    _require_uint(liquidity, 128, "liquidity")
    if not isinstance(current_tick, int) or not MIN_TICK <= current_tick <= MAX_TICK:
        raise ValueError("current_tick is invalid")
    if not isinstance(amount_specified, int) or amount_specified == 0:
        raise ValueError("amount_specified must be a non-zero integer")
    if not isinstance(fee_pips, int) or not 0 <= fee_pips < FEE_DENOMINATOR:
        raise ValueError("fee_pips is invalid")
    if zero_for_one:
        if not MIN_SQRT_RATIO <= sqrt_price_limit_x96 < sqrt_price_x96:
            raise ValueError("zero-for-one price limit is invalid")
    elif not sqrt_price_x96 < sqrt_price_limit_x96 <= MAX_SQRT_RATIO:
        raise ValueError("one-for-zero price limit is invalid")
    ticks = _validated_ticks(initialized_ticks)
    sorted_ticks = sorted(ticks)
    amount_remaining, current_sqrt, active_liquidity, state_tick = amount_specified, sqrt_price_x96, liquidity, current_tick
    total_input = total_output = total_fee = steps = crossed = 0
    while amount_remaining != 0 and current_sqrt != sqrt_price_limit_x96:
        if steps >= len(sorted_ticks) + 2:
            raise ValueError("swap iteration guard exceeded")
        candidates = [tick for tick in sorted_ticks if tick <= state_tick] if zero_for_one else [tick for tick in sorted_ticks if tick > state_tick]
        next_tick = max(candidates) if zero_for_one and candidates else (min(candidates) if candidates else None)
        next_sqrt = get_sqrt_ratio_at_tick(next_tick) if next_tick is not None else sqrt_price_limit_x96
        target = max(next_sqrt, sqrt_price_limit_x96) if zero_for_one else min(next_sqrt, sqrt_price_limit_x96)
        reaches_tick = next_tick is not None and target == next_sqrt
        previous = current_sqrt
        crossed_this_step = False
        steps += 1
        if active_liquidity == 0:
            current_sqrt = target
        else:
            step = compute_swap_step(current_sqrt, target, active_liquidity, amount_remaining, fee_pips)
            current_sqrt = step.sqrt_price_next_x96
            gross = step.amount_in + step.fee_amount
            total_input += gross
            total_output += step.amount_out
            total_fee += step.fee_amount
            amount_remaining = amount_remaining - gross if amount_specified > 0 else amount_remaining + step.amount_out
        if current_sqrt == next_sqrt and reaches_tick:
            delta = -ticks[next_tick] if zero_for_one else ticks[next_tick]
            active_liquidity += delta
            if not 0 <= active_liquidity <= MAX_UINT128:
                raise ValueError("tick crossing produces invalid active liquidity")
            state_tick = next_tick - 1 if zero_for_one else next_tick
            crossed += 1
            crossed_this_step = True
        elif current_sqrt != previous:
            state_tick = MAX_TICK if current_sqrt == MAX_SQRT_RATIO else get_tick_at_sqrt_ratio(current_sqrt)
        if current_sqrt == previous and amount_remaining != 0 and not crossed_this_step:
            raise ValueError("swap step made no progress")
    complete = amount_remaining == 0
    return SwapResult(total_input, total_output, total_fee, amount_remaining, current_sqrt, state_tick, active_liquidity, steps, crossed, complete, "amount_resolved" if complete else "sqrt_price_limit_reached")
