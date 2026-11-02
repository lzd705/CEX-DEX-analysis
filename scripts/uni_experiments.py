#!/usr/bin/env python3
"""Bounded public UNI sampling and exact same-quantity research calculations."""
import argparse
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from decimal import Decimal
from functools import partial
import hashlib
import json
from pathlib import Path
import sys
import time

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.fetch_cex_depth import (
    fetch_source_book, request_json, _typed_market_rules_request,
    _binance_market_rules_source, _bybit_market_rules_source,
)
from scripts.route_opportunity import common_target_quantity
from scripts.route_quantity import MarketRules, FeeSemantics, quote_cex_book_quantity

VENUES = ("binance", "bybit")
NOTIONALS = ("1000", "5000", "10000", "50000", "100000")
FEE_SCENARIOS = {
    "zero_counterfactual": {"binance": "0", "bybit": "0"},
    "one_each_counterfactual": {"binance": "1", "bybit": "1"},
    "two_each_counterfactual": {"binance": "2", "bybit": "2"},
    "five_each_counterfactual": {"binance": "5", "bybit": "5"},
    "ten_each_counterfactual": {"binance": "10", "bybit": "10"},
    "reviewed_public_reference": {"binance": "10", "bybit": "20"},
}


def utc():
    return datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def make_rules(source, source_hash):
    decimals = {"base_increment", "quote_increment", "min_base_quantity", "min_quote_notional"}
    return MarketRules(**{k: Decimal(v) if k in decimals else v for k, v in source.items()
                          if k != "schema"}, source_record_sha256=source_hash)


def make_target(buy, sell, rules, notional):
    return common_target_quantity(requested_notional_usd=notional,
        buy_reference_price_usd=buy["asks"][0][0], sell_reference_price_usd=sell["bids"][0][0],
        buy_market_rules=rules[buy["venue"]], sell_market_rules=rules[sell["venue"]])


def evaluate_quantity(buy, sell, rules, target, rates):
    quotes = []
    for book, direction, rate in ((buy, "buy", rates[0]), (sell, "sell", rates[1])):
        rule = rules[book["venue"]]
        fee = FeeSemantics(rate_bps=Decimal(rate),
            fee_asset="UNI" if direction == "buy" else "USDT",
            charge_basis="received_base" if direction == "buy" else "received_quote",
            fee_increment=rule.base_increment if direction == "buy" else rule.quote_increment,
            rounding_mode="ceiling", third_asset_quote_price=None,
            observed_at=rule.observed_at, valid_until=rule.valid_until,
            source_record_sha256=digest(("hypothetical-fee:" + rate).encode()),
            conversion_source_record_sha256=None)
        quote = quote_cex_book_quantity(
            [(Decimal(p), Decimal(q)) for p, q in book["asks" if direction == "buy" else "bids"]],
            target, rule, fee, direction=direction, source_quote_asset="USDT",
            full_book_reported=False, state_id="uni-experiment:" + book["venue"])
        quotes.append(quote)
    b, s = quotes
    if not b.complete or not s.complete:
        return {"net": None, "bps": None,
                "reason": b.reason_code or s.reason_code or "depth_or_quantity_incomplete"}
    cost, proceeds = b.quote_debit_quantity, s.quote_received_quantity
    net = proceeds - cost
    return {"net": str(net), "bps": str(net / cost * 10000), "reason": None,
            "buy_cost": str(cost), "sell_proceeds": str(proceeds),
            "buy_fee_asset": b.fee_debit_asset, "buy_fee": str(b.fee_debit_quantity),
            "sell_fee_asset": s.fee_debit_asset, "sell_fee": str(s.fee_debit_quantity),
            "buy_net_quantity": str(b.net_base_received_quantity),
            "sell_debit_quantity": str(s.base_debit_quantity),
            "buy_levels": b.levels_or_ticks_consumed, "sell_levels": s.levels_or_ticks_consumed}


def eligible_pair(pair):
    books = pair["books"]
    if len(books) != 2 or {b["venue"] for b in books} != set(VENUES):
        return False, "market_inventory"
    if any(b.get("status") != "observed" for b in books):
        return False, "source_failure"
    if any(b["request_seconds"] > 2 for b in books):
        return False, "request_over_2s"
    if abs(books[0]["received_mono"] - books[1]["received_mono"]) > 1.5:
        return False, "receive_skew_over_1_5s"
    if any(b.get("source_age_seconds") is not None and
           not -1 <= b["source_age_seconds"] <= 2 for b in books):
        return False, "source_age_outside_bounds"
    return True, None


def first_delayed_pair(pairs, index, delay):
    target = pairs[index]["decision_mono"] + delay
    return next((p for p in pairs[index + 1:] if p["decision_mono"] >= target), None)


def capture_rules(root, label):
    records = {}
    for venue in VENUES:
        url = _typed_market_rules_request(venue, "UNIUSDT")
        payload, raw = request_json(url, timeout_seconds=6, max_retries=1)
        observed = utc()
        filename = "raw/" + label + "-" + venue + "-rules.json"
        (root / filename).write_bytes(raw)
        project = _binance_market_rules_source if venue == "binance" else _bybit_market_rules_source
        projection = project(payload, market={"exchange": venue, "cex_symbol": "UNI/USDT"},
                             source_instrument="UNIUSDT", observed_at=observed)
        records[venue] = {"url": url, "sha256": digest(raw), "path": filename,
                          "observed_at": observed, "projection": projection}
    return records


def capture_book(root, index, venue):
    started, wall_start = time.monotonic(), utc()
    try:
        book = fetch_source_book(venue, "UNI/USDT",
            request=partial(request_json, timeout_seconds=4, max_retries=1))
        received, wall_receive = time.monotonic(), utc()
        filename = "raw/%04d-%s.json" % (index, venue)
        (root / filename).write_bytes(book["raw"])
        source_time = book.get("source_observed_at") or None
        source_age = None if source_time is None else (
            datetime.fromisoformat(wall_receive.replace("Z", "+00:00")) -
            datetime.fromisoformat(source_time.replace("Z", "+00:00"))).total_seconds()
        return {"venue": venue, "status": "observed", "started_at": wall_start,
                "received_at": wall_receive, "received_mono": received,
                "request_seconds": received - started,
                "source_observed_at": source_time, "source_age_seconds": source_age,
                "sequence": book["source_sequence"], "url": book["source_endpoint"],
                "path": filename, "sha256": digest(book["raw"]),
                "bids": [[str(p), str(q)] for p, q in book["bids"]],
                "asks": [[str(p), str(q)] for p, q in book["asks"]]}
    except Exception as error:
        return {"venue": venue, "status": "failed", "started_at": wall_start,
                "received_at": utc(), "received_mono": time.monotonic(),
                "error_type": type(error).__name__}


def collect(root, samples, interval):
    root.mkdir(parents=True, exist_ok=False)
    (root / "raw").mkdir()
    manifest = {"schema": "uni_experiment_capture/v1", "started_at": utc(),
                "planned_samples": samples, "interval_seconds": interval,
                "sampling": "REST concurrent snapshots, no orders", "rules_start": capture_rules(root, "start")}
    (root / "manifest.json").write_text(json.dumps(manifest, indent=2))
    start = time.monotonic()
    with (root / "samples.jsonl").open("x") as handle, ThreadPoolExecutor(max_workers=2) as pool:
        for index in range(samples):
            wait = start + index * interval - time.monotonic()
            if wait > 0:
                time.sleep(wait)
            futures = [pool.submit(capture_book, root, index, venue) for venue in VENUES]
            books = [future.result() for future in futures]
            pair = {"index": index, "decision_mono": max(b["received_mono"] for b in books),
                    "decision_at": max(b["received_at"] for b in books), "books": books}
            handle.write(json.dumps(pair, separators=(",", ":")) + "\n")
            handle.flush()
            if index % 6 == 0:
                print(json.dumps({"sample": index + 1, "planned": samples,
                                  "eligible": eligible_pair(pair)[0], "at": pair["decision_at"]}), flush=True)
    try:
        manifest["rules_end"] = capture_rules(root, "end")
    except Exception as error:
        manifest["rules_end_error"] = type(error).__name__
    manifest["completed_at"] = utc()
    manifest["samples_sha256"] = digest((root / "samples.jsonl").read_bytes())
    (root / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    print(json.dumps({"completed": str(root), "samples": samples}), flush=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--samples", type=int, default=120, choices=range(2, 181))
    parser.add_argument("--interval", type=float, default=5, choices=(5, 10))
    args = parser.parse_args()
    collect(args.output, args.samples, args.interval)
