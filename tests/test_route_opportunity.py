"""Exact, fail-closed route-opportunity calculation tests."""

from __future__ import annotations

import unittest
from dataclasses import replace
from decimal import Decimal, localcontext
import hashlib
import json
from typing import Optional
from unittest.mock import patch

import scripts.route_opportunity as route_opportunity
from scripts.execution_cost_components import cost_component_row
from scripts.fetch_cex_depth import (
    cex_quantity_state_id,
    route_quantity_quote_for_book,
)
from scripts.route_cohort import canonical_route_id
from scripts.route_opportunity import (
    build_route_opportunity,
    common_target_quantity,
    route_opportunity_id,
    usd_projection_evidence,
    validate_route_opportunity,
)
from scripts.route_quantity import CommonTarget, FeeSemantics, MarketRules
from scripts.route_quantity import V2PoolState, quote_v2_pool_quantity


COHORT_ID = "cohort:" + "c" * 64
CORE_HASH = "d" * 64
COHORT_NOW = "2026-08-01T12:01:00Z"
NOW = "2026-08-01T12:02:00Z"
RULES_OBSERVED = "2026-08-01T11:55:00Z"
VALID_UNTIL = "2026-08-01T13:00:00Z"
POOL = "0x3333333333333333333333333333333333333333"
TOKEN_ADDRESS = "0x1111111111111111111111111111111111111111"
QUOTE_ADDRESS = "0x2222222222222222222222222222222222222222"
V2_FEE_FORMULA = (
    "amount_in_with_fee=amount_in*fee_numerator;"
    "denominator=reserve_in*fee_denominator+amount_in_with_fee"
)


def market_rules(market_id: str, *, source_hash: str) -> MarketRules:
    base, quote = market_id.rsplit(":", 1)[1].split("/")
    return MarketRules(
        market_id=market_id,
        base_asset=base,
        quote_asset=quote,
        base_unit_decimals=2,
        quote_unit_decimals=2,
        base_increment=Decimal("0.01"),
        quote_increment=Decimal("0.01"),
        min_base_quantity=Decimal("0.01"),
        min_quote_notional=Decimal("1"),
        observed_at=RULES_OBSERVED,
        valid_until=VALID_UNTIL,
        source_record_sha256=source_hash,
    )


def fee_semantics(
    *,
    direction: str,
    source_hash: str,
    rate_bps: str = "10",
    fee_asset: str = "USDT",
    charge_basis=None,
) -> FeeSemantics:
    return FeeSemantics(
        rate_bps=Decimal(rate_bps),
        fee_asset=fee_asset,
        charge_basis=(
            charge_basis
            if charge_basis is not None
            else ("spent_quote" if direction == "buy" else "received_quote")
        ),
        fee_increment=Decimal("0.01"),
        rounding_mode="ceiling",
        third_asset_quote_price=None,
        observed_at=RULES_OBSERVED,
        valid_until=VALID_UNTIL,
        source_record_sha256=source_hash,
        conversion_source_record_sha256=None,
    )


def common_target(raw: int = 10_000) -> CommonTarget:
    return CommonTarget(
        asset="AAVE",
        unit_decimals=2,
        raw_quantity=raw,
        lattice_raw=1,
    )


def cex_leg(
    *,
    venue: str,
    direction: str,
    price: str,
    state_observed_at: str,
    target: CommonTarget,
    cohort_now: str = COHORT_NOW,
    fee_asset: str = "USDT",
    charge_basis=None,
):
    market_id = f"cex:{venue}:AAVE/USDT"
    market = {
        "token_symbol": "AAVE",
        "exchange": venue,
        "cex_symbol": "AAVE/USDT",
    }
    levels = [(Decimal(price), Decimal("1000"))]
    book = {
        "bids": levels,
        "asks": levels,
        "source_instrument": "AAVEUSDT",
        "source_quote_asset": "USDT",
        "source_sequence": venue + "-sequence-1",
        "source_observed_at": state_observed_at,
        "source_endpoint": "https://example.test/" + venue + "/depth",
        "full_book_reported": False,
        "raw": (venue + ":raw-book").encode("utf-8"),
    }
    rules_hash = ("1" if venue == "binance" else "2") * 64
    fee_hash = ("3" if venue == "binance" else "4") * 64
    rules = market_rules(market_id, source_hash=rules_hash)
    fee = fee_semantics(
        direction=direction,
        source_hash=fee_hash,
        fee_asset=fee_asset,
        charge_basis=charge_basis,
    )
    snapshot_id = "snapshot-" + venue
    state_id = cex_quantity_state_id(
        market,
        book,
        snapshot_id=snapshot_id,
        observed_at=state_observed_at,
        cohort_now=cohort_now,
        market_rules=rules,
        fee_semantics=fee,
    )
    quote = route_quantity_quote_for_book(
        market,
        book,
        direction=direction,
        target_token_quantity=target,
        market_rules=rules,
        fee_semantics=fee,
        snapshot_id=snapshot_id,
        observed_at=state_observed_at,
        cohort_now=cohort_now,
        expected_state_id=state_id,
    )
    evidence = {
        "kind": "cex_book",
        "market": market,
        "book": book,
        "market_rules": rules,
        "fee_semantics": fee,
        "snapshot_id": snapshot_id,
        "observed_at": state_observed_at,
        "cohort_now": cohort_now,
        "expected_state_id": state_id,
        "assurance_status": "route_bundle_validated",
        "core_manifest_sha256": CORE_HASH,
    }
    leg = {
        "market_id": market_id,
        "status": "observed",
        "available": True,
        "reason_code": "observed",
        "state_id": quote.state_id,
        "state_observed_at": state_observed_at,
        "snapshot_id": snapshot_id,
        "raw_response_sha256": quote.raw_response_sha256,
    }
    cash_quantity = (
        quote.quote_debit_quantity
        if direction == "buy"
        else quote.quote_received_quantity
    )
    projection = usd_projection_evidence(
        market_id=market_id,
        state_id=state_id,
        direction=direction,
        quote_asset="USDT",
        quote_cash_quantity=cash_quantity,
        usd_per_quote=Decimal("1"),
        value_status="authenticated",
        observed_at=state_observed_at,
        valid_until=VALID_UNTIL,
        source="synchronized USDT/USD conversion",
        source_record_sha256=("5" if venue == "binance" else "6") * 64,
        core_manifest_sha256=CORE_HASH,
    )
    return quote, evidence, leg, projection, fee


def dex_target(raw: int = 100 * 10**18) -> CommonTarget:
    return CommonTarget(
        asset="AAVE",
        unit_decimals=18,
        raw_quantity=raw,
        lattice_raw=10**16,
    )


def v2_leg(
    *,
    direction: str,
    state_observed_at: str,
    target: CommonTarget,
    chain: str = "eth",
    cohort_now: str = COHORT_NOW,
    pool_address: Optional[str] = None,
    dex: str = "uniswap_v2",
    fee_proof_sha256: str = "a" * 64,
    raw_response_sha256: Optional[str] = None,
):
    chain_id = {"eth": 1, "arb": 42161}[chain]
    pool = pool_address or (
        POOL if chain == "eth" else "0x4444444444444444444444444444444444444444"
    )
    market_id = f"dex:{chain}:{dex}:{pool}:AAVE"
    state = V2PoolState(
        chain=chain,
        chain_id=chain_id,
        dex=dex,
        pool_address=pool,
        token0_address=TOKEN_ADDRESS,
        token1_address=QUOTE_ADDRESS,
        token0_decimals=18,
        token1_decimals=6,
        reserve0_raw=1_000 * 10**18,
        reserve1_raw=100_000 * 10**6,
        reserve_timestamp_last_raw=1_704_067_200,
        fee_bps=30,
        fee_numerator=9_970,
        fee_denominator=10_000,
        fee_formula=V2_FEE_FORMULA,
        fee_proof_sha256=fee_proof_sha256,
        block_number=123,
        block_hash="0x" + "b" * 64,
        block_header_sha256="c" * 64,
        observed_at=state_observed_at,
        raw_response_sha256=(
            raw_response_sha256
            or (("d" if chain == "eth" else "e") * 64)
        ),
    )
    rules = MarketRules(
        market_id=market_id,
        base_asset="AAVE",
        quote_asset="USDC",
        base_unit_decimals=18,
        quote_unit_decimals=6,
        base_increment=Decimal("0.000000000000000001"),
        quote_increment=Decimal("0.000001"),
        min_base_quantity=Decimal("0"),
        min_quote_notional=Decimal("0"),
        observed_at=RULES_OBSERVED,
        valid_until=VALID_UNTIL,
        source_record_sha256="f" * 64,
    )
    quote = quote_v2_pool_quantity(
        state,
        target,
        rules,
        direction=direction,
        target_token_address=TOKEN_ADDRESS,
        quote_token_address=QUOTE_ADDRESS,
        cohort_now=cohort_now,
    )
    evidence = {
        "kind": "dex_v2",
        "pool_state": state,
        "market_rules": rules,
        "target_token_address": TOKEN_ADDRESS,
        "quote_token_address": QUOTE_ADDRESS,
        "cohort_now": cohort_now,
        "assurance_status": "authenticated_fixed_block",
        "core_manifest_sha256": CORE_HASH,
    }
    leg = {
        "market_id": market_id,
        "status": "observed",
        "available": True,
        "reason_code": "observed",
        "state_id": quote.state_id,
        "state_observed_at": state_observed_at,
        "snapshot_id": "",
        "raw_response_sha256": quote.raw_response_sha256,
    }
    cash_quantity = (
        quote.quote_debit_quantity
        if direction == "buy"
        else quote.quote_received_quantity
    )
    projection = usd_projection_evidence(
        market_id=market_id,
        state_id=quote.state_id,
        direction=direction,
        quote_asset="USDC",
        quote_cash_quantity=cash_quantity,
        usd_per_quote=Decimal("1"),
        value_status="authenticated",
        observed_at=state_observed_at,
        valid_until=VALID_UNTIL,
        source="synchronized USDC/USD conversion",
        source_record_sha256="9" * 64,
        core_manifest_sha256=CORE_HASH,
    )
    return quote, evidence, leg, projection, state


def dex_leg_costs(*, route, opportunity_id, target, leg, state):
    market_id = route[f"{leg}_market_id"]
    direction = "buy_token" if leg == "buy" else "sell_token"
    shared = {
        "cohort_id": COHORT_ID,
        "opportunity_id": opportunity_id,
        "leg": leg,
        "market_id": market_id,
        "direction": direction,
        "requested_notional_usd": Decimal("10000"),
        "target_token_quantity": target.quantity,
    }
    return [
        cost_component_row(
            **shared,
            component_type="pool_swap_fee",
            value_status="measured",
            amount_usd=Decimal("30"),
            rate_bps=Decimal("30"),
            basis="fixed-block V2 pool fee embedded in quote",
            strict_eligible=True,
            embedded_in_leg_quote=True,
            observed_at=state.observed_at,
            valid_until=None,
            source="fixed-block pool state",
            source_record_sha256=state.fee_proof_sha256,
        ),
        cost_component_row(
            **shared,
            component_type="network_gas",
            value_status="quoted",
            amount_usd=Decimal("2"),
            rate_bps=Decimal("2"),
            basis="same-call gas quote and native/USD conversion",
            strict_eligible=True,
            observed_at=state.observed_at,
            valid_until=VALID_UNTIL,
            source="validated route gas quote",
            source_record_sha256="8" * 64,
        ),
        cost_component_row(
            **shared,
            component_type="router_or_integrator_fee",
            value_status="not_applicable",
            amount_usd=None,
            rate_bps=None,
            basis="selected adapter proves no router surcharge",
            strict_eligible=True,
            observed_at=None,
            valid_until=None,
            source="validated route adapter contract",
            source_record_sha256="7" * 64,
        ),
        cost_component_row(
            **shared,
            component_type="token_transfer_tax",
            value_status="not_applicable",
            amount_usd=None,
            rate_bps=None,
            basis="selected adapter proves ordinary ERC-20 transfer behavior",
            strict_eligible=True,
            observed_at=None,
            valid_until=None,
            source="validated route adapter contract",
            source_record_sha256="6" * 64,
        ),
    ]


def route_and_mode(buy_market_id: str, sell_market_id: str):
    identity = {
        "token_symbol": "AAVE",
        "buy_market_id": buy_market_id,
        "sell_market_id": sell_market_id,
        "route_mode": "prepositioned_inventory",
    }
    route = {
        **identity,
        "route_id": canonical_route_id(identity),
        "route_class": "candidate",
        "settlement_reason": None,
    }
    mode = {
        "route_id": route["route_id"],
        "route_mode": route["route_mode"],
        "classification": "mode_evidence_eligible",
        "mode_evidence_eligible": True,
        "reason_code": None,
        "reason_codes": [],
        "inventory_profile_hash": "7" * 64,
        "maximum_proved_capacity_quantity": "100",
    }
    return route, mode


def cex_costs(
    *,
    route,
    opportunity_id,
    target,
    requested_notional="10000",
    buy_fee_hash="3" * 64,
    sell_fee_hash="4" * 64,
):
    rows = []
    for leg, market_id, fee_hash in (
        ("buy", route["buy_market_id"], buy_fee_hash),
        ("sell", route["sell_market_id"], sell_fee_hash),
    ):
        rows.append(
            cost_component_row(
                cohort_id=COHORT_ID,
                opportunity_id=opportunity_id,
                leg=leg,
                market_id=market_id,
                direction="buy_token" if leg == "buy" else "sell_token",
                requested_notional_usd=Decimal(requested_notional),
                target_token_quantity=target.quantity,
                component_type="venue_taker_fee",
                value_status="authenticated",
                amount_usd=Decimal("10"),
                rate_bps=Decimal("10"),
                basis="authenticated received-asset taker fee; fee_asset=USDT",
                strict_eligible=True,
                observed_at=RULES_OBSERVED,
                valid_until=VALID_UNTIL,
                source="redacted authenticated fee response",
                source_record_sha256=fee_hash,
            )
        )
    rows.append(
        cost_component_row(
            cohort_id=COHORT_ID,
            opportunity_id=opportunity_id,
            leg="route",
            market_id="",
            direction="route",
            requested_notional_usd=Decimal(requested_notional),
            target_token_quantity=target.quantity,
            component_type="rebalancing_or_transfer",
            value_status="not_applicable",
            amount_usd=None,
            rate_bps=None,
            basis="prepositioned inventory proves no immediate transfer",
            strict_eligible=True,
            observed_at=None,
            valid_until=None,
            source="validated route topology",
            source_record_sha256=None,
        )
    )
    return rows


def terminal_route_fixture(
    *,
    cohort_id=COHORT_ID,
    core_hash=CORE_HASH,
    requested_notional="10000",
    token_symbol="CAKE",
):
    identity = {
        "token_symbol": token_symbol,
        "buy_market_id": "cex:binance:{}/USDT".format(token_symbol),
        "sell_market_id": "cex:bybit:{}/USDT".format(token_symbol),
        "route_mode": "prepositioned_inventory",
    }
    route = {
        **identity,
        "route_id": canonical_route_id(identity),
        "route_class": "candidate",
        "settlement_reason": None,
    }
    buy_leg = {
        "market_id": route["buy_market_id"],
        "status": "observed",
        "available": True,
        "reason_code": "observed",
        "state_observed_at": "2026-08-01T12:00:00Z",
    }
    sell_leg = {
        "market_id": route["sell_market_id"],
        "status": "failed",
        "available": False,
        "reason_code": "source_unavailable",
        "state_observed_at": None,
    }
    route_timing = {
        "route_id": route["route_id"],
        "skew_seconds": None,
        "timing_status": "unavailable",
        "reason_code": "sell_leg_unavailable",
    }
    opportunity_id = route_opportunity_id(route["route_id"], requested_notional)
    shared = {
        "cohort_id": cohort_id,
        "opportunity_id": opportunity_id,
        "requested_notional_usd": Decimal(requested_notional),
        "target_token_quantity": None,
        "value_status": "unavailable",
        "amount_usd": None,
        "rate_bps": None,
        "basis": "retained route timing proves route unavailable",
        "strict_eligible": False,
        "observed_at": None,
        "valid_until": None,
        "source": "retained route timing",
        "source_record_sha256": None,
        "reason_code": route_timing["reason_code"],
    }
    costs = [
        cost_component_row(
            **shared,
            leg=leg,
            market_id=market_id,
            direction=direction,
            component_type=component_type,
        )
        for leg, market_id, direction, component_type in (
            ("buy", route["buy_market_id"], "buy_token", "venue_taker_fee"),
            ("sell", route["sell_market_id"], "sell_token", "venue_taker_fee"),
            ("route", "", "route", "rebalancing_or_transfer"),
        )
    ]
    mode = {
        "route_id": route["route_id"],
        "route_mode": route["route_mode"],
        "classification": "research_estimate",
        "mode_evidence_eligible": False,
        "reason_code": "mode_expected_request_unavailable",
        "reason_codes": ["mode_expected_request_unavailable"],
        "inventory_profile_hash": None,
        "maximum_proved_capacity_quantity": None,
    }
    return {
        "cohort_id": cohort_id,
        "route": route,
        "requested_notional_usd": Decimal(requested_notional),
        "buy_leg": buy_leg,
        "sell_leg": sell_leg,
        "route_timing": route_timing,
        "cost_components": costs,
        "mode_evidence": mode,
        "now": NOW,
        "core_manifest_sha256": core_hash,
    }


def strict_fixture(
    *,
    now=NOW,
    sell_observed_at="2026-08-01T12:01:00Z",
    cohort_now=COHORT_NOW,
    sell_price="102",
    sell_fee_asset="USDT",
    sell_charge_basis=None,
):
    target = common_target()
    buy_quote, buy_evidence, buy_leg, buy_usd, buy_fee = cex_leg(
        venue="binance",
        direction="buy",
        price="100",
        state_observed_at="2026-08-01T12:00:00Z",
        target=target,
        cohort_now=cohort_now,
    )
    sell_quote, sell_evidence, sell_leg, sell_usd, sell_fee = cex_leg(
        venue="bybit",
        direction="sell",
        price=sell_price,
        state_observed_at=sell_observed_at,
        target=target,
        cohort_now=cohort_now,
        fee_asset=sell_fee_asset,
        charge_basis=sell_charge_basis,
    )
    route, mode = route_and_mode(buy_quote.market_id, sell_quote.market_id)
    opportunity_id = route_opportunity_id(route["route_id"], Decimal("10000"))
    return {
        "cohort_id": COHORT_ID,
        "route": route,
        "requested_notional_usd": Decimal("10000"),
        "common_target": target,
        "buy_leg": buy_leg,
        "sell_leg": sell_leg,
        "buy_quote": buy_quote,
        "sell_quote": sell_quote,
        "buy_quote_evidence": buy_evidence,
        "sell_quote_evidence": sell_evidence,
        "buy_usd_projection": buy_usd,
        "sell_usd_projection": sell_usd,
        "cost_components": cex_costs(
            route=route,
            opportunity_id=opportunity_id,
            target=target,
        ),
        "mode_evidence": mode,
        "now": now,
    }


def cex_v2_fixture(
    *,
    buy_observed_at="2026-08-01T12:00:00Z",
    sell_observed_at="2026-08-01T12:01:00Z",
    cohort_now=COHORT_NOW,
    now=NOW,
):
    target = dex_target()
    buy_quote, buy_evidence, buy_leg, buy_usd, pool_state = v2_leg(
        direction="buy",
        state_observed_at=buy_observed_at,
        target=target,
        cohort_now=cohort_now,
    )
    sell_quote, sell_evidence, sell_leg, sell_usd, sell_fee = cex_leg(
        venue="bybit",
        direction="sell",
        price="120",
        state_observed_at=sell_observed_at,
        target=target,
        cohort_now=cohort_now,
    )
    route, mode = route_and_mode(buy_quote.market_id, sell_quote.market_id)
    opportunity_id = route_opportunity_id(route["route_id"], Decimal("10000"))
    costs = dex_leg_costs(
        route=route,
        opportunity_id=opportunity_id,
        target=target,
        leg="buy",
        state=pool_state,
    )
    costs.extend(
        [
            cost_component_row(
                cohort_id=COHORT_ID,
                opportunity_id=opportunity_id,
                leg="sell",
                market_id=route["sell_market_id"],
                direction="sell_token",
                requested_notional_usd=Decimal("10000"),
                target_token_quantity=target.quantity,
                component_type="venue_taker_fee",
                value_status="authenticated",
                amount_usd=Decimal("10"),
                rate_bps=Decimal("10"),
                basis="authenticated received-asset taker fee; fee_asset=USDT",
                strict_eligible=True,
                observed_at=RULES_OBSERVED,
                valid_until=VALID_UNTIL,
                source="redacted authenticated fee response",
                source_record_sha256=sell_fee.source_record_sha256,
            ),
            cost_component_row(
                cohort_id=COHORT_ID,
                opportunity_id=opportunity_id,
                leg="route",
                market_id="",
                direction="route",
                requested_notional_usd=Decimal("10000"),
                target_token_quantity=target.quantity,
                component_type="rebalancing_or_transfer",
                value_status="not_applicable",
                amount_usd=None,
                rate_bps=None,
                basis="prepositioned inventory proves no immediate transfer",
                strict_eligible=True,
                observed_at=None,
                valid_until=None,
                source="validated route topology",
                source_record_sha256=None,
            ),
        ]
    )
    return {
        "cohort_id": COHORT_ID,
        "route": route,
        "requested_notional_usd": Decimal("10000"),
        "common_target": target,
        "buy_leg": buy_leg,
        "sell_leg": sell_leg,
        "buy_quote": buy_quote,
        "sell_quote": sell_quote,
        "buy_quote_evidence": buy_evidence,
        "sell_quote_evidence": sell_evidence,
        "buy_usd_projection": buy_usd,
        "sell_usd_projection": sell_usd,
        "cost_components": costs,
        "mode_evidence": mode,
        "now": now,
    }


def mev_scenario_row(kwargs, *, value_status, amount_usd=None, reason_code=None):
    exemplar = kwargs["cost_components"][0]
    return cost_component_row(
        cohort_id=exemplar["cohort_id"],
        opportunity_id=exemplar["opportunity_id"],
        leg="route",
        market_id="",
        direction="route",
        requested_notional_usd=exemplar["requested_notional_usd"],
        target_token_quantity=exemplar["target_token_quantity"],
        component_type="mev_buffer",
        value_status=value_status,
        amount_usd=amount_usd,
        rate_bps=amount_usd,
        basis="explicit route-level MEV protection scenario",
        strict_eligible=False,
        observed_at=None,
        valid_until=None,
        source="route submission policy",
        source_record_sha256=None,
        reason_code=reason_code,
    )


def atomic_v2_fixture():
    target = dex_target()
    buy_quote, buy_evidence, buy_leg, buy_usd, buy_state = v2_leg(
        direction="buy",
        state_observed_at="2026-08-01T12:00:00Z",
        target=target,
    )
    sell_quote, sell_evidence, sell_leg, sell_usd, sell_state = v2_leg(
        direction="sell",
        state_observed_at="2026-08-01T12:01:00Z",
        target=target,
        pool_address="0x4444444444444444444444444444444444444444",
        dex="sushiswap_v2",
        fee_proof_sha256="5" * 64,
        raw_response_sha256="e" * 64,
    )
    identity = {
        "token_symbol": "AAVE",
        "buy_market_id": buy_quote.market_id,
        "sell_market_id": sell_quote.market_id,
        "route_mode": "atomic_onchain",
    }
    route = {
        **identity,
        "route_id": canonical_route_id(identity),
        "route_class": "candidate",
        "settlement_reason": None,
    }
    mode = {
        "route_id": route["route_id"],
        "route_mode": route["route_mode"],
        "classification": "mode_evidence_eligible",
        "mode_evidence_eligible": True,
        "reason_code": None,
        "reason_codes": [],
        "inventory_profile_hash": None,
        "maximum_proved_capacity_quantity": str(target.quantity),
    }
    opportunity_id = route_opportunity_id(route["route_id"], Decimal("10000"))
    costs = dex_leg_costs(
        route=route,
        opportunity_id=opportunity_id,
        target=target,
        leg="buy",
        state=buy_state,
    )
    costs.extend(dex_leg_costs(
        route=route,
        opportunity_id=opportunity_id,
        target=target,
        leg="sell",
        state=sell_state,
    ))
    costs.append(cost_component_row(
        cohort_id=COHORT_ID,
        opportunity_id=opportunity_id,
        leg="route",
        market_id="",
        direction="route",
        requested_notional_usd=Decimal("10000"),
        target_token_quantity=target.quantity,
        component_type="rebalancing_or_transfer",
        value_status="not_applicable",
        amount_usd=None,
        rate_bps=None,
        basis="atomic route proves no intermediate transfer",
        strict_eligible=True,
        observed_at=None,
        valid_until=None,
        source="validated route topology",
        source_record_sha256=None,
    ))
    kwargs = {
        "cohort_id": COHORT_ID,
        "route": route,
        "requested_notional_usd": Decimal("10000"),
        "common_target": target,
        "buy_leg": buy_leg,
        "sell_leg": sell_leg,
        "buy_quote": buy_quote,
        "sell_quote": sell_quote,
        "buy_quote_evidence": buy_evidence,
        "sell_quote_evidence": sell_evidence,
        "buy_usd_projection": buy_usd,
        "sell_usd_projection": sell_usd,
        "cost_components": costs,
        "mode_evidence": mode,
        "now": NOW,
    }
    kwargs["cost_components"].append(mev_scenario_row(
        kwargs,
        value_status="assumed",
        amount_usd=Decimal("5"),
    ))
    return kwargs


def collapsed_atomic_gas_costs(kwargs):
    costs = [
        row for row in kwargs["cost_components"]
        if not (
            row["leg"] in {"buy", "sell"}
            and row["component_type"] == "network_gas"
        )
    ]
    exemplar = kwargs["cost_components"][0]
    costs.append(cost_component_row(
        cohort_id=exemplar["cohort_id"],
        opportunity_id=exemplar["opportunity_id"],
        leg="route",
        market_id="",
        direction="route",
        requested_notional_usd=exemplar["requested_notional_usd"],
        target_token_quantity=exemplar["target_token_quantity"],
        component_type="network_gas",
        value_status="quoted",
        amount_usd=Decimal("4"),
        rate_bps=Decimal("4"),
        basis="one atomic route gas quote covering both swap legs",
        strict_eligible=True,
        observed_at="2026-08-01T12:01:00Z",
        valid_until=VALID_UNTIL,
        source="validated atomic route gas quote",
        source_record_sha256="8" * 64,
    ))
    return sorted(costs, key=lambda row: (row["leg"], row["component_type"]))


class CommonQuantityTests(unittest.TestCase):
    def test_known_answer_uses_one_common_lattice_quantity(self):
        buy = market_rules("cex:binance:AAVE/USDT", source_hash="1" * 64)
        sell = market_rules("cex:bybit:AAVE/USDT", source_hash="2" * 64)

        result = common_target_quantity(
            requested_notional_usd=Decimal("10000"),
            buy_reference_price_usd=Decimal("101"),
            sell_reference_price_usd=Decimal("100"),
            buy_market_rules=buy,
            sell_market_rules=sell,
        )

        self.assertEqual(result.quantity, Decimal("99"))
        self.assertEqual(result.raw_quantity, 9900)
        self.assertEqual(result.asset, "AAVE")

    def test_common_quantity_bounds_reference_exposure_on_both_legs(self):
        buy = market_rules("cex:binance:AAVE/USDT", source_hash="1" * 64)
        sell = market_rules("cex:bybit:AAVE/USDT", source_hash="2" * 64)

        result = common_target_quantity(
            requested_notional_usd=Decimal("10000"),
            buy_reference_price_usd=Decimal("100"),
            sell_reference_price_usd=Decimal("200"),
            buy_market_rules=buy,
            sell_market_rules=sell,
        )

        self.assertEqual(result.quantity, Decimal("50"))
        self.assertLessEqual(result.quantity * Decimal("100"), Decimal("10000"))
        self.assertLessEqual(result.quantity * Decimal("200"), Decimal("10000"))


class _RouteOpportunityTopologyTests:
    def test_live_builder_rejects_collapsed_atomic_nine_row_topology(self):
        kwargs = atomic_v2_fixture()
        collapsed_costs = collapsed_atomic_gas_costs(kwargs)
        collapsed_keys = frozenset(
            (row["leg"], row["component_type"])
            for row in collapsed_costs
        )
        live_keys = route_opportunity.live_complete_cost_component_keys(
            kwargs["route"]
        )
        self.assertEqual(len(live_keys), 10)
        self.assertEqual(len(collapsed_costs), 9)
        self.assertEqual(len(collapsed_keys), 9)
        self.assertEqual(
            live_keys - collapsed_keys,
            frozenset({
                ("buy", "network_gas"),
                ("sell", "network_gas"),
            }),
        )
        self.assertEqual(
            collapsed_keys - live_keys,
            frozenset({("route", "network_gas")}),
        )
        self.assertEqual(
            sum(
                Decimal(row["amount_usd"])
                for row in kwargs["cost_components"]
                if row["component_type"] == "network_gas"
            ),
            Decimal(next(
                row["amount_usd"]
                for row in collapsed_costs
                if row["component_type"] == "network_gas"
            )),
        )

        with patch(
            "scripts.route_opportunity.live_complete_cost_component_keys",
            wraps=route_opportunity.live_complete_cost_component_keys,
        ) as topology:
            with self.assertRaisesRegex(
                ValueError,
                "cost component is incompatible with route topology",
            ):
                build_route_opportunity(
                    **{**kwargs, "cost_components": collapsed_costs}
                )

        topology.assert_called_once()
        self.assertEqual(
            topology.call_args.args[0]["route_id"],
            kwargs["route"]["route_id"],
        )

        with patch(
            "scripts.route_opportunity.live_complete_cost_component_keys",
            return_value=collapsed_keys,
        ) as mutant:
            result = build_route_opportunity(
                **{**kwargs, "cost_components": collapsed_costs}
            )

        mutant.assert_called_once()
        self.assertEqual(
            mutant.call_args.args[0]["route_id"],
            kwargs["route"]["route_id"],
        )
        self.assertEqual(result["opportunity_id"], collapsed_costs[0]["opportunity_id"])


class _TerminalRouteOpportunityContractTests:
    def test_terminal_builder_emits_exact_null_contract_and_rejects_mutations(self):
        self.assertTrue(
            hasattr(route_opportunity, "build_terminal_route_opportunity"),
            "terminal route builder is not implemented",
        )
        kwargs = terminal_route_fixture()
        result = route_opportunity.build_terminal_route_opportunity(**kwargs)

        expected_id = (
            "route:CAKE:cex:binance:CAKE/USDT->cex:bybit:CAKE/USDT:"
            "prepositioned_inventory:10000"
        )
        self.assertEqual(result["opportunity_id"], expected_id)
        self.assertEqual(result["primary_reason"], "sell_leg_unavailable")
        self.assertEqual(result["reason_codes"], ["sell_leg_unavailable"])
        self.assertEqual(result["opportunity_class"], "unavailable")
        self.assertIs(result["strict_eligible"], False)
        self.assertIs(result["strict_ready_for_publication"], False)
        self.assertEqual(result["buy_core_manifest_sha256"], CORE_HASH)
        self.assertEqual(result["sell_core_manifest_sha256"], CORE_HASH)
        for field in (
            "target_token_quantity",
            "target_base_raw",
            "target_base_unit_decimals",
            "target_lattice_raw",
            "buy_state_id",
            "sell_state_id",
            "buy_state_observed_at",
            "sell_state_observed_at",
            "route_age_seconds",
            "gross_buy_cost_usd",
            "gross_sell_proceeds_usd",
            "gross_edge_usd",
            "gross_edge_bps",
            "gross_edge_bps_numerator",
            "gross_edge_bps_denominator",
            "strict_nonembedded_cost_usd",
            "research_bounded_cost_usd",
            "research_assumed_cost_usd",
            "strict_net_edge_usd",
            "strict_net_edge_bps",
            "strict_net_edge_bps_numerator",
            "strict_net_edge_bps_denominator",
            "research_net_edge_usd",
            "research_net_edge_bps",
            "research_net_edge_bps_numerator",
            "research_net_edge_bps_denominator",
            "edge_bps_denominator_basis",
            "inventory_profile_hash",
            "maximum_proved_capacity_quantity",
            "publication_attestation_sha256",
            "buy_usd_projection_sha256",
            "sell_usd_projection_sha256",
        ):
            with self.subTest(field=field):
                self.assertIsNone(result[field])

        canonical_costs = sorted(
            (dict(row) for row in kwargs["cost_components"]),
            key=lambda row: (row["leg"], row["component_type"]),
        )
        expected_cost_hash = hashlib.sha256(json.dumps(
            canonical_costs,
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")).hexdigest()
        expected_mode_hash = hashlib.sha256(json.dumps(
            kwargs["mode_evidence"],
            ensure_ascii=True,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")).hexdigest()
        self.assertEqual(result["cost_component_set_sha256"], expected_cost_hash)
        self.assertEqual(result["mode_evidence_sha256"], expected_mode_hash)
        self.assertEqual(
            result["evidence_binding_sha256"],
            hashlib.sha256(json.dumps(
                {
                    key: value for key, value in result.items()
                    if key != "evidence_binding_sha256"
                },
                ensure_ascii=True,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")).hexdigest(),
        )
        self.assertEqual(len(kwargs["cost_components"]), 3)
        self.assertTrue(all(
            row["target_token_quantity"] is None
            and row["value_status"] == "unavailable"
            and row["amount_usd"] is None
            and row["rate_bps"] is None
            and row["strict_eligible"] is False
            and row["reason_code"] == "sell_leg_unavailable"
            for row in kwargs["cost_components"]
        ))

        route_opportunity.validate_route_opportunity(result, **kwargs)

        wrong_timing = {**kwargs, "route_timing": {
            **kwargs["route_timing"],
            "reason_code": "buy_leg_unavailable",
        }}
        wrong_route = {**kwargs, "route": {
            **kwargs["route"],
            "buy_market_id": "cex:bybit:CAKE/USDT",
        }}
        for label, mutated in (
            ("timing reason", wrong_timing),
            ("route identity", wrong_route),
        ):
            with self.subTest(label=label):
                with self.assertRaises(ValueError):
                    route_opportunity.build_terminal_route_opportunity(**mutated)

        for label, field, value in (
            ("target", "target_token_quantity", "1"),
            ("state", "sell_state_id", "fabricated-state"),
            ("economics", "gross_edge_usd", "1"),
            ("attestation", "publication_attestation_sha256", "f" * 64),
        ):
            with self.subTest(label=label):
                mutated = {**result, field: value}
                mutated["evidence_binding_sha256"] = hashlib.sha256(json.dumps(
                    {
                        key: item for key, item in mutated.items()
                        if key != "evidence_binding_sha256"
                    },
                    ensure_ascii=True,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")).hexdigest()
                with self.assertRaises(ValueError):
                    route_opportunity.validate_route_opportunity(
                        mutated,
                        **kwargs,
                    )

class RouteOpportunityTests(_RouteOpportunityTopologyTests, unittest.TestCase):
    def test_public_reason_registry_covers_every_mode_reason(self):
        expected_mode_reasons = frozenset().union(
            *route_opportunity._MODE_REASON_CODES_BY_MODE.values()
        )

        self.assertLessEqual(
            expected_mode_reasons,
            route_opportunity.ROUTE_OPPORTUNITY_REASON_CODES,
        )

    def test_complete_positive_route_is_locally_ready_but_not_published_without_attestation(self):
        kwargs = strict_fixture()

        result = build_route_opportunity(**kwargs)

        self.assertEqual(result["opportunity_class"], "research_estimate")
        self.assertEqual(result["primary_reason"], "publication_evidence_unverified")
        self.assertEqual(result["gross_buy_cost_usd"], "10010")
        self.assertEqual(result["gross_sell_proceeds_usd"], "10189.8")
        self.assertEqual(result["gross_edge_usd"], "179.8")
        self.assertEqual(result["strict_nonembedded_cost_usd"], "0")
        self.assertEqual(result["strict_net_edge_usd"], "179.8")
        self.assertEqual(
            result["reflected_or_embedded_component_keys"],
            ["buy:venue_taker_fee", "sell:venue_taker_fee"],
        )
        self.assertEqual(result["skew_seconds"], "60")
        self.assertEqual(result["route_age_seconds"], "60")
        self.assertEqual(result["edge_bps_denominator_basis"], "gross_buy_cost_usd")
        self.assertTrue(result["strict_ready_for_publication"])
        self.assertFalse(result["strict_eligible"])
        self.assertIsNone(result["publication_attestation_sha256"])

    def test_mapping_malformed_and_mismatched_publication_attestations_are_rejected(self):
        kwargs = strict_fixture()
        locally_ready = build_route_opportunity(**kwargs)

        for malformed in ({}, object()):
            with self.subTest(kind=type(malformed).__name__):
                with self.assertRaisesRegex(ValueError, "publication attestation"):
                    build_route_opportunity(
                        **kwargs,
                        publication_attestation=malformed,
                    )

        malformed_internal = object.__new__(
            route_opportunity._PublicationAttestation
        )
        with self.assertRaisesRegex(ValueError, "publication attestation is malformed"):
            build_route_opportunity(
                **kwargs,
                publication_attestation=malformed_internal,
            )

        mismatched = object.__new__(route_opportunity._PublicationAttestation)
        object.__setattr__(mismatched, "_binding_sha256", "0" * 64)
        with self.assertRaisesRegex(ValueError, "publication attestation binding"):
            build_route_opportunity(
                **kwargs,
                publication_attestation=mismatched,
            )

        binding = {
            "cohort_id": locally_ready["cohort_id"],
            "opportunity_id": locally_ready["opportunity_id"],
            "route_id": locally_ready["route_id"],
            "target_token_quantity": locally_ready["target_token_quantity"],
            "buy_state_id": locally_ready["buy_state_id"],
            "sell_state_id": locally_ready["sell_state_id"],
            "buy_usd_projection_sha256": locally_ready[
                "buy_usd_projection_sha256"
            ],
            "sell_usd_projection_sha256": locally_ready[
                "sell_usd_projection_sha256"
            ],
            "cost_component_set_sha256": locally_ready[
                "cost_component_set_sha256"
            ],
            "mode_evidence_sha256": locally_ready["mode_evidence_sha256"],
            "core_manifest_sha256": locally_ready["buy_core_manifest_sha256"],
        }
        exact_hash_forgery = object.__new__(
            route_opportunity._PublicationAttestation
        )
        object.__setattr__(
            exact_hash_forgery,
            "_binding_sha256",
            hashlib.sha256(
                json.dumps(
                    binding,
                    sort_keys=True,
                    separators=(",", ":"),
                    ensure_ascii=True,
                    allow_nan=False,
                ).encode("utf-8")
            ).hexdigest(),
        )
        with self.assertRaisesRegex(ValueError, "not issued"):
            build_route_opportunity(
                **kwargs,
                publication_attestation=exact_hash_forgery,
            )

    def test_exact_60_second_skew_and_120_second_age_pass_but_next_unit_does_not(self):
        boundary = build_route_opportunity(
            **strict_fixture(now="2026-08-01T12:03:00Z")
        )
        stale = build_route_opportunity(
            **strict_fixture(now="2026-08-01T12:03:00.0000001Z")
        )

        self.assertEqual(boundary["route_age_seconds"], "120")
        self.assertEqual(boundary["opportunity_class"], "research_estimate")
        self.assertTrue(boundary["strict_ready_for_publication"])
        self.assertEqual(stale["opportunity_class"], "research_estimate")
        self.assertEqual(stale["primary_reason"], "cohort_stale")
        self.assertEqual(stale["route_age_seconds"], "120.0000001")

    def test_real_v2_replay_enforces_exact_60_second_skew_boundary(self):
        zero = build_route_opportunity(
            **cex_v2_fixture(sell_observed_at="2026-08-01T12:00:00Z")
        )
        boundary = build_route_opportunity(**cex_v2_fixture())
        over = build_route_opportunity(
            **cex_v2_fixture(
                sell_observed_at="2026-08-01T12:01:00.0000001Z",
                cohort_now="2026-08-01T12:01:00.0000001Z",
            )
        )

        self.assertEqual(zero["skew_seconds"], "0")
        self.assertEqual(zero["opportunity_class"], "research_estimate")
        self.assertEqual(boundary["skew_seconds"], "60")
        self.assertEqual(boundary["opportunity_class"], "research_estimate")
        self.assertEqual(over["skew_seconds"], "60.0000001")
        self.assertEqual(over["opportunity_class"], "unavailable")
        self.assertEqual(over["primary_reason"], "snapshot_skew_exceeded")
        self.assertIsNone(over["gross_edge_usd"])
        self.assertIsNone(over["strict_net_edge_usd"])

    def test_cex_v2_cost_topology_embeds_pool_and_cex_fees_exactly_once(self):
        kwargs = cex_v2_fixture()

        result = build_route_opportunity(**kwargs)

        self.assertEqual(result["opportunity_class"], "research_estimate")
        self.assertFalse(result["strict_ready_for_publication"])
        self.assertEqual(result["strict_nonembedded_cost_usd"], "2")
        self.assertEqual(
            result["reflected_or_embedded_component_keys"],
            ["buy:pool_swap_fee", "sell:venue_taker_fee"],
        )
        self.assertEqual(
            Decimal(result["strict_net_edge_usd"]),
            Decimal(result["gross_edge_usd"]) - Decimal("2"),
        )

        for field, value, message in (
            ("source_record_sha256", "0" * 64, "pool fee source"),
            ("rate_bps", "31", "pool fee rate"),
        ):
            forged = cex_v2_fixture()
            pool_row = forged["cost_components"][0]
            if field == "rate_bps":
                pool_row = {
                    **pool_row,
                    "amount_usd": "31",
                    field: value,
                }
            else:
                pool_row = {**pool_row, field: value}
            forged["cost_components"] = [
                pool_row,
                *forged["cost_components"][1:],
            ]
            with self.subTest(field=field):
                with self.assertRaisesRegex(ValueError, message):
                    build_route_opportunity(**forged)

    def test_v2_quote_is_replayed_against_the_exact_pool_state(self):
        kwargs = cex_v2_fixture()
        evidence = kwargs["buy_quote_evidence"]
        kwargs["buy_quote_evidence"] = {
            **evidence,
            "pool_state": replace(
                evidence["pool_state"],
                reserve1_raw=evidence["pool_state"].reserve1_raw + 1,
            ),
        }

        result = build_route_opportunity(**kwargs)

        self.assertEqual(result["opportunity_class"], "unavailable")
        self.assertEqual(
            result["primary_reason"],
            "quantity_quote_evidence_mismatch",
        )

    def test_two_legs_must_declare_the_same_validated_core_lineage(self):
        kwargs = strict_fixture()
        kwargs["sell_quote_evidence"] = {
            **kwargs["sell_quote_evidence"],
            "core_manifest_sha256": "e" * 64,
        }

        result = build_route_opportunity(**kwargs)

        self.assertEqual(result["opportunity_class"], "unavailable")
        self.assertEqual(
            result["primary_reason"],
            "quantity_quote_evidence_mismatch",
        )

    def test_leg_timestamp_mismatch_is_unavailable_and_has_no_edge_residue(self):
        kwargs = strict_fixture()
        kwargs["sell_leg"] = {
            **kwargs["sell_leg"],
            "state_observed_at": "2026-08-01T12:01:00.0000001Z",
        }

        result = build_route_opportunity(**kwargs)

        self.assertEqual(result["opportunity_class"], "unavailable")
        self.assertEqual(result["primary_reason"], "quantity_quote_evidence_mismatch")
        self.assertIsNone(result["gross_edge_usd"])
        self.assertIsNone(result["strict_net_edge_usd"])

    def test_partial_leg_is_unavailable_even_when_other_evidence_is_complete(self):
        kwargs = strict_fixture()
        kwargs["buy_leg"] = {
            **kwargs["buy_leg"],
            "status": "partial",
            "available": False,
            "reason_code": "partial_fill",
        }

        result = build_route_opportunity(**kwargs)

        self.assertEqual(result["opportunity_class"], "unavailable")
        self.assertEqual(result["primary_reason"], "leg_not_completely_filled")
        self.assertIsNone(result["gross_edge_usd"])

    def test_required_estimate_and_optional_mev_have_different_effects(self):
        estimated = strict_fixture()
        original = estimated["cost_components"][0]
        estimated["cost_components"][0] = cost_component_row(
            cohort_id=original["cohort_id"],
            opportunity_id=original["opportunity_id"],
            leg=original["leg"],
            market_id=original["market_id"],
            direction=original["direction"],
            requested_notional_usd=original["requested_notional_usd"],
            target_token_quantity=original["target_token_quantity"],
            component_type="venue_taker_fee",
            value_status="bounded_estimate",
            amount_usd=Decimal("10"),
            rate_bps=Decimal("10"),
            basis="public fee schedule upper bound",
            strict_eligible=False,
            observed_at=None,
            valid_until=None,
            source="official public fee schedule",
            source_record_sha256="3" * 64,
        )
        estimate_result = build_route_opportunity(**estimated)

        self.assertEqual(estimate_result["opportunity_class"], "research_estimate")
        self.assertEqual(estimate_result["cost_completeness"], "incomplete")
        self.assertEqual(estimate_result["scenario_cost_completeness"], "complete")
        self.assertIn("cost_component_estimated", estimate_result["reason_codes"])
        self.assertEqual(estimate_result["research_net_edge_usd"], "179.8")

        optional = strict_fixture()
        exemplar = optional["cost_components"][0]
        optional["cost_components"].append(
            cost_component_row(
                cohort_id=exemplar["cohort_id"],
                opportunity_id=exemplar["opportunity_id"],
                leg="route",
                market_id="",
                direction="route",
                requested_notional_usd=exemplar["requested_notional_usd"],
                target_token_quantity=exemplar["target_token_quantity"],
                component_type="mev_buffer",
                value_status="assumed",
                amount_usd=Decimal("5"),
                rate_bps=Decimal("5"),
                basis="user-selected adverse-selection buffer",
                strict_eligible=False,
                observed_at=None,
                valid_until=None,
                source="explicit research scenario",
                source_record_sha256=None,
            )
        )
        optional_result = build_route_opportunity(**optional)

        self.assertEqual(optional_result["opportunity_class"], "research_estimate")
        self.assertTrue(optional_result["strict_ready_for_publication"])
        self.assertEqual(
            optional_result["primary_reason"],
            "publication_evidence_unverified",
        )
        self.assertEqual(optional_result["strict_net_edge_usd"], "179.8")
        self.assertEqual(optional_result["research_net_edge_usd"], "174.8")

    def test_nonembedded_required_estimates_are_subtracted_by_status(self):
        kwargs = cex_v2_fixture()
        gas = kwargs["cost_components"][1]
        router = kwargs["cost_components"][2]
        kwargs["cost_components"][1] = cost_component_row(
            cohort_id=gas["cohort_id"],
            opportunity_id=gas["opportunity_id"],
            leg=gas["leg"],
            market_id=gas["market_id"],
            direction=gas["direction"],
            requested_notional_usd=gas["requested_notional_usd"],
            target_token_quantity=gas["target_token_quantity"],
            component_type="network_gas",
            value_status="bounded_estimate",
            amount_usd=Decimal("7"),
            rate_bps=Decimal("7"),
            basis="bounded gas scenario",
            strict_eligible=False,
            observed_at=None,
            valid_until=None,
            source="explicit research bound",
            source_record_sha256=None,
        )
        kwargs["cost_components"][2] = cost_component_row(
            cohort_id=router["cohort_id"],
            opportunity_id=router["opportunity_id"],
            leg=router["leg"],
            market_id=router["market_id"],
            direction=router["direction"],
            requested_notional_usd=router["requested_notional_usd"],
            target_token_quantity=router["target_token_quantity"],
            component_type="router_or_integrator_fee",
            value_status="assumed",
            amount_usd=Decimal("2"),
            rate_bps=Decimal("2"),
            basis="explicit router scenario",
            strict_eligible=False,
            observed_at=None,
            valid_until=None,
            source="user research assumption",
            source_record_sha256=None,
        )
        kwargs["cost_components"].append(
            mev_scenario_row(
                kwargs,
                value_status="bounded_estimate",
                amount_usd=Decimal("5"),
            )
        )

        result = build_route_opportunity(**kwargs)

        self.assertEqual(result["opportunity_class"], "research_estimate")
        self.assertEqual(result["research_bounded_cost_usd"], "12")
        self.assertEqual(result["research_assumed_cost_usd"], "2")
        self.assertEqual(
            Decimal(result["research_net_edge_usd"]),
            Decimal(result["gross_edge_usd"]) - Decimal("14"),
        )

    def test_cex_only_terminal_optional_mev_keeps_local_readiness_but_hides_research_net(self):
        kwargs = strict_fixture()
        exemplar = kwargs["cost_components"][0]
        kwargs["cost_components"].append(
            cost_component_row(
                cohort_id=exemplar["cohort_id"],
                opportunity_id=exemplar["opportunity_id"],
                leg="route",
                market_id="",
                direction="route",
                requested_notional_usd=exemplar["requested_notional_usd"],
                target_token_quantity=exemplar["target_token_quantity"],
                component_type="mev_buffer",
                value_status="unavailable",
                amount_usd=None,
                rate_bps=None,
                basis="public-mempool adverse selection cannot be bounded",
                strict_eligible=False,
                observed_at=None,
                valid_until=None,
                source="route submission policy",
                source_record_sha256=None,
                reason_code="mev_protection_unavailable",
            )
        )

        result = build_route_opportunity(**kwargs)

        self.assertEqual(result["opportunity_class"], "research_estimate")
        self.assertTrue(result["strict_ready_for_publication"])
        self.assertEqual(result["primary_reason"], "publication_evidence_unverified")
        self.assertEqual(result["scenario_cost_completeness"], "incomplete")
        self.assertIsNone(result["research_net_edge_usd"])
        self.assertIn(
            "mev_scenario_unavailable:route:mev_buffer:mev_protection_unavailable",
            result["component_reasons"],
        )

    def test_dex_mev_missing_terminal_and_scenario_states_fail_closed(self):
        missing = cex_v2_fixture()
        missing_result = build_route_opportunity(**missing)
        self.assertEqual(missing_result["opportunity_class"], "research_estimate")
        self.assertFalse(missing_result["strict_ready_for_publication"])
        self.assertEqual(missing_result["scenario_cost_completeness"], "incomplete")
        self.assertIn(
            "mev_protection_unavailable:route",
            missing_result["component_reasons"],
        )

        terminal = cex_v2_fixture()
        terminal["cost_components"].append(
            mev_scenario_row(
                terminal,
                value_status="unavailable",
                reason_code="mev_protection_unavailable",
            )
        )
        terminal_result = build_route_opportunity(**terminal)
        self.assertFalse(terminal_result["strict_ready_for_publication"])
        self.assertEqual(terminal_result["scenario_cost_completeness"], "incomplete")
        self.assertIsNone(terminal_result["research_net_edge_usd"])

        for status in ("bounded_estimate", "assumed"):
            scenario = cex_v2_fixture()
            scenario["cost_components"].append(
                mev_scenario_row(
                    scenario,
                    value_status=status,
                    amount_usd=Decimal("5"),
                )
            )
            with self.subTest(status=status):
                result = build_route_opportunity(**scenario)
                self.assertEqual(result["opportunity_class"], "research_estimate")
                self.assertFalse(result["strict_ready_for_publication"])
                self.assertEqual(result["scenario_cost_completeness"], "complete")
                self.assertEqual(
                    Decimal(result["research_net_edge_usd"]),
                    Decimal(result["gross_edge_usd"]) - Decimal("7"),
                )

    def test_non_positive_edge_never_enters_executable_ranking(self):
        result = build_route_opportunity(**strict_fixture(sell_price="99"))

        self.assertEqual(result["opportunity_class"], "research_estimate")
        self.assertEqual(result["primary_reason"], "non_positive_net_edge")
        self.assertLess(Decimal(result["strict_net_edge_usd"]), Decimal("0"))
        self.assertFalse(result["strict_eligible"])

    def test_nonreflected_base_asset_cex_fee_needs_exact_conversion_evidence(self):
        kwargs = strict_fixture(
            sell_fee_asset="AAVE",
            sell_charge_basis="sold_base",
        )

        result = build_route_opportunity(**kwargs)

        self.assertEqual(result["opportunity_class"], "unavailable")
        self.assertIn(
            "fee_debit_conversion_unproved:sell:venue_taker_fee",
            result["component_reasons"],
        )
        self.assertIsNone(result["strict_net_edge_usd"])

    def test_route_mode_capacity_below_target_is_not_executable(self):
        kwargs = strict_fixture()
        kwargs["mode_evidence"] = {
            **kwargs["mode_evidence"],
            "maximum_proved_capacity_quantity": "1",
        }

        result = build_route_opportunity(**kwargs)

        self.assertEqual(result["opportunity_class"], "research_estimate")
        self.assertEqual(result["primary_reason"], "inventory_insufficient")
        self.assertFalse(result["strict_eligible"])

    def test_route_mode_projection_rejects_contradictions_and_missing_capacity(self):
        contradictory = strict_fixture()
        contradictory["mode_evidence"] = {
            **contradictory["mode_evidence"],
            "classification": "research_estimate",
            "reason_code": "inventory_insufficient",
            "reason_codes": ["inventory_insufficient"],
        }
        with self.assertRaisesRegex(ValueError, "classification"):
            build_route_opportunity(**contradictory)

        missing = strict_fixture()
        missing["mode_evidence"] = {
            key: value
            for key, value in missing["mode_evidence"].items()
            if key != "maximum_proved_capacity_quantity"
        }
        result = build_route_opportunity(**missing)
        self.assertEqual(result["opportunity_class"], "research_estimate")
        self.assertEqual(result["primary_reason"], "inventory_unavailable")

    def test_route_mode_reasons_are_allowlisted_and_mode_consistent(self):
        for reason in ("banana", "atomic_route_simulation_unavailable"):
            kwargs = strict_fixture()
            kwargs["mode_evidence"] = {
                **kwargs["mode_evidence"],
                "classification": "research_estimate",
                "mode_evidence_eligible": False,
                "reason_code": reason,
                "reason_codes": [reason],
                "maximum_proved_capacity_quantity": None,
            }
            with self.subTest(reason=reason):
                with self.assertRaisesRegex(ValueError, "mode evidence reason"):
                    build_route_opportunity(**kwargs)

        with self.assertRaisesRegex(ValueError, "mode evidence reason"):
            route_opportunity._validated_mode_evidence(
                {"route_id": "route:cross-chain", "route_mode": "research_only"},
                {"reason_code": "banana", "reason_codes": ["banana"]},
                common_target(),
            )

    def test_cross_chain_dex_cannot_masquerade_as_rebalance_required(self):
        kwargs = cex_v2_fixture()
        identity = {
            "token_symbol": "AAVE",
            "buy_market_id": kwargs["route"]["buy_market_id"],
            "sell_market_id": (
                "dex:arb:uniswap_v2:"
                "0x4444444444444444444444444444444444444444:AAVE"
            ),
            "route_mode": "rebalance_required",
        }
        kwargs["route"] = {
            **identity,
            "route_id": canonical_route_id(identity),
            "route_class": "candidate",
            "settlement_reason": None,
        }

        with self.assertRaisesRegex(ValueError, "cross-chain"):
            build_route_opportunity(**kwargs)

    def test_unproved_not_applicable_cost_is_unavailable_with_specific_reason(self):
        kwargs = strict_fixture()
        route_row = kwargs["cost_components"][-1]
        kwargs["cost_components"][-1] = {
            **route_row,
            "source": "operator claim",
        }

        result = build_route_opportunity(**kwargs)

        self.assertEqual(result["opportunity_class"], "unavailable")
        self.assertIn(
            "cost_not_applicable_unproved:route:rebalancing_or_transfer",
            result["component_reasons"],
        )

    def test_terminal_unavailable_cost_is_not_mislabeled_stale(self):
        kwargs = strict_fixture()
        row = kwargs["cost_components"][0]
        kwargs["cost_components"][0] = cost_component_row(
            cohort_id=row["cohort_id"],
            opportunity_id=row["opportunity_id"],
            leg=row["leg"],
            market_id=row["market_id"],
            direction=row["direction"],
            requested_notional_usd=row["requested_notional_usd"],
            target_token_quantity=row["target_token_quantity"],
            component_type=row["component_type"],
            value_status="unavailable",
            amount_usd=None,
            rate_bps=None,
            basis="pinned evidence did not establish this component",
            strict_eligible=False,
            observed_at=None,
            valid_until=None,
            source="pinned route-cost evidence",
            source_record_sha256=None,
            reason_code="strict_cost_adapter_unsupported",
        )

        result = build_route_opportunity(**kwargs)

        self.assertEqual(result["opportunity_class"], "unavailable")
        self.assertEqual(result["reason_codes"], ["cost_components_incomplete"])
        self.assertEqual(result["component_reasons"], [])

    def test_explicit_terminal_stale_cost_keeps_stale_reason(self):
        kwargs = strict_fixture()
        row = kwargs["cost_components"][0]
        kwargs["cost_components"][0] = cost_component_row(
            cohort_id=row["cohort_id"],
            opportunity_id=row["opportunity_id"],
            leg=row["leg"],
            market_id=row["market_id"],
            direction=row["direction"],
            requested_notional_usd=row["requested_notional_usd"],
            target_token_quantity=row["target_token_quantity"],
            component_type=row["component_type"],
            value_status="stale",
            amount_usd=None,
            rate_bps=None,
            basis="pinned evidence is stale",
            strict_eligible=False,
            observed_at=None,
            valid_until=None,
            source="pinned route-cost evidence",
            source_record_sha256=None,
            reason_code="source_expired",
        )

        result = build_route_opportunity(**kwargs)

        self.assertEqual(
            result["component_reasons"],
            ["cost_component_stale:buy:venue_taker_fee"],
        )

    def test_expired_not_applicable_proof_does_not_bypass_currentness(self):
        kwargs = strict_fixture()
        route_row = kwargs["cost_components"][-1]
        kwargs["cost_components"][-1] = cost_component_row(
            cohort_id=route_row["cohort_id"],
            opportunity_id=route_row["opportunity_id"],
            leg=route_row["leg"],
            market_id=route_row["market_id"],
            direction=route_row["direction"],
            requested_notional_usd=route_row["requested_notional_usd"],
            target_token_quantity=route_row["target_token_quantity"],
            component_type=route_row["component_type"],
            value_status="not_applicable",
            amount_usd=None,
            rate_bps=None,
            basis=route_row["basis"],
            strict_eligible=True,
            observed_at="2026-08-01T11:00:00Z",
            valid_until=NOW,
            source="validated route topology",
            source_record_sha256=None,
        )

        result = build_route_opportunity(**kwargs)

        self.assertEqual(result["opportunity_class"], "unavailable")
        self.assertIn(
            "cost_component_stale:route:rebalancing_or_transfer",
            result["component_reasons"],
        )

    def test_stale_reflected_fee_preserves_research_cashflow_but_not_strict(self):
        kwargs = strict_fixture()
        row = kwargs["cost_components"][0]
        kwargs["cost_components"][0] = cost_component_row(
            cohort_id=row["cohort_id"],
            opportunity_id=row["opportunity_id"],
            leg=row["leg"],
            market_id=row["market_id"],
            direction=row["direction"],
            requested_notional_usd=row["requested_notional_usd"],
            target_token_quantity=row["target_token_quantity"],
            component_type=row["component_type"],
            value_status="authenticated",
            amount_usd=row["amount_usd"],
            rate_bps=row["rate_bps"],
            basis=row["basis"],
            strict_eligible=True,
            observed_at=RULES_OBSERVED,
            valid_until=NOW,
            source=row["source"],
            source_record_sha256=row["source_record_sha256"],
        )

        result = build_route_opportunity(**kwargs)

        self.assertEqual(result["opportunity_class"], "research_estimate")
        self.assertEqual(result["gross_edge_usd"], "179.8")
        self.assertEqual(result["research_net_edge_usd"], "179.8")
        self.assertIn(
            "cost_component_stale:buy:venue_taker_fee",
            result["component_reasons"],
        )

    def test_missing_or_expired_usd_conversion_never_defaults_stablecoin_to_usd(self):
        missing = strict_fixture()
        missing["buy_usd_projection"] = None
        expired = strict_fixture()
        expired["sell_usd_projection"] = usd_projection_evidence(
            market_id=expired["sell_quote"].market_id,
            state_id=expired["sell_quote"].state_id,
            direction="sell",
            quote_asset="USDT",
            quote_cash_quantity=expired["sell_quote"].quote_received_quantity,
            usd_per_quote=Decimal("1"),
            value_status="authenticated",
            observed_at="2026-08-01T11:00:00Z",
            valid_until=NOW,
            source="expired USDT/USD conversion",
            source_record_sha256="6" * 64,
            core_manifest_sha256=CORE_HASH,
        )

        for kwargs in (missing, expired):
            with self.subTest(kind="missing" if kwargs is missing else "expired"):
                result = build_route_opportunity(**kwargs)
                self.assertEqual(result["opportunity_class"], "unavailable")
                self.assertEqual(result["primary_reason"], "usd_conversion_unavailable")
                self.assertIsNone(result["gross_edge_usd"])

    def test_usd_projection_binding_and_declared_core_lineage_are_enforced(self):
        broken_binding = strict_fixture()
        broken_binding["buy_usd_projection"] = {
            **broken_binding["buy_usd_projection"],
            "usd_per_quote": "2",
            "usd_amount": "20020",
        }
        with self.assertRaisesRegex(ValueError, "evidence binding"):
            build_route_opportunity(**broken_binding)

        wrong_state = strict_fixture()
        projection = wrong_state["buy_usd_projection"]
        wrong_state["buy_usd_projection"] = usd_projection_evidence(
            market_id=projection["market_id"],
            state_id="other-state",
            direction=projection["direction"],
            quote_asset=projection["quote_asset"],
            quote_cash_quantity=projection["quote_cash_quantity"],
            usd_per_quote=projection["usd_per_quote"],
            value_status=projection["value_status"],
            observed_at=projection["observed_at"],
            valid_until=projection["valid_until"],
            source=projection["source"],
            source_record_sha256=projection["source_record_sha256"],
            core_manifest_sha256=projection["core_manifest_sha256"],
        )
        with self.assertRaisesRegex(ValueError, "quote lineage"):
            build_route_opportunity(**wrong_state)

        wrong_core = strict_fixture()
        projection = wrong_core["buy_usd_projection"]
        wrong_core["buy_usd_projection"] = usd_projection_evidence(
            market_id=projection["market_id"],
            state_id=projection["state_id"],
            direction=projection["direction"],
            quote_asset=projection["quote_asset"],
            quote_cash_quantity=projection["quote_cash_quantity"],
            usd_per_quote=projection["usd_per_quote"],
            value_status=projection["value_status"],
            observed_at=projection["observed_at"],
            valid_until=projection["valid_until"],
            source=projection["source"],
            source_record_sha256=projection["source_record_sha256"],
            core_manifest_sha256="e" * 64,
        )
        result = build_route_opportunity(**wrong_core)
        self.assertEqual(result["opportunity_class"], "unavailable")
        self.assertEqual(result["primary_reason"], "usd_conversion_unavailable")

    def test_integrity_only_leg_assurance_cannot_upgrade_to_executable(self):
        kwargs = strict_fixture()
        kwargs["buy_quote_evidence"] = {
            **kwargs["buy_quote_evidence"],
            "assurance_status": "integrity_only",
        }

        result = build_route_opportunity(**kwargs)

        self.assertEqual(result["opportunity_class"], "research_estimate")
        self.assertEqual(
            result["primary_reason"],
            "quantity_quote_evidence_not_strict",
        )
        self.assertFalse(result["strict_eligible"])
        self.assertEqual(result["research_net_edge_usd"], "179.8")

    def test_self_consistent_output_mutation_is_rejected_by_evidence_recompute(self):
        kwargs = strict_fixture()
        quote = kwargs["sell_quote"]
        kwargs["sell_quote"] = replace(
            quote,
            gross_quote_quantity=Decimal("20400"),
            net_quote_quantity=Decimal("20379.6"),
            quote_received_quantity=Decimal("20379.6"),
            fee_debit_quantity=Decimal("20.4"),
            ending_price=Decimal("204"),
            ending_price_numerator=204,
            ending_price_denominator=1,
            vwap_quote_per_base=Decimal("204"),
            vwap_quote_numerator=204,
            vwap_quote_denominator=1,
        )
        kwargs["sell_usd_projection"] = usd_projection_evidence(
            market_id=kwargs["sell_quote"].market_id,
            state_id=kwargs["sell_quote"].state_id,
            direction="sell",
            quote_asset="USDT",
            quote_cash_quantity=kwargs["sell_quote"].quote_received_quantity,
            usd_per_quote=Decimal("1"),
            value_status="authenticated",
            observed_at=kwargs["sell_quote"].state_observed_at,
            valid_until=VALID_UNTIL,
            source="synchronized USDT/USD conversion",
            source_record_sha256="6" * 64,
            core_manifest_sha256=CORE_HASH,
        )

        result = build_route_opportunity(**kwargs)

        self.assertEqual(result["opportunity_class"], "unavailable")
        self.assertEqual(result["primary_reason"], "quantity_quote_evidence_mismatch")
        self.assertIsNone(result["gross_sell_proceeds_usd"])

    def test_opportunity_validator_rebuilds_from_evidence_and_rejects_mutation(self):
        kwargs = strict_fixture()
        result = build_route_opportunity(**kwargs)

        self.assertIs(validate_route_opportunity(result, **kwargs), result)
        forged = {**result, "strict_net_edge_usd": "999999"}
        with self.assertRaisesRegex(ValueError, "opportunity evidence"):
            validate_route_opportunity(forged, **kwargs)

    def test_opportunity_validator_rejects_rehashed_output_forgery(self):
        kwargs = strict_fixture()
        result = build_route_opportunity(**kwargs)
        forged = {**result, "strict_net_edge_usd": "999999"}
        unsigned = {
            key: value
            for key, value in forged.items()
            if key != "evidence_binding_sha256"
        }
        forged["evidence_binding_sha256"] = hashlib.sha256(
            json.dumps(
                unsigned,
                sort_keys=True,
                separators=(",", ":"),
                ensure_ascii=True,
                allow_nan=False,
            ).encode("utf-8")
        ).hexdigest()

        with self.assertRaisesRegex(ValueError, "does not reproduce"):
            validate_route_opportunity(forged, **kwargs)

    def test_exact_arithmetic_is_independent_of_decimal_context(self):
        kwargs = strict_fixture()
        baseline = build_route_opportunity(**kwargs)
        with localcontext() as context:
            context.prec = 3
            result = build_route_opportunity(**kwargs)

        self.assertEqual(result, baseline)


class TerminalRouteOpportunityTests(
    _TerminalRouteOpportunityContractTests,
    unittest.TestCase,
):
    pass


if __name__ == "__main__":
    unittest.main()
