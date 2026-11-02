#!/usr/bin/env python3
"""Replay retained UNI samples into fee, time-series and delay experiments."""
import argparse
from collections import Counter, defaultdict
import csv
from decimal import Decimal
import json
from pathlib import Path
from statistics import median
import sys

if __package__ in {None, ""}:
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.fetch_cex_depth import parse_book
from scripts.fetch_cex_depth import _binance_market_rules_source, _bybit_market_rules_source
from scripts.uni_experiments import (VENUES, NOTIONALS, FEE_SCENARIOS, digest,
    make_rules, make_target, evaluate_quantity, eligible_pair, first_delayed_pair)


def stats(values):
    values = sorted(values)
    return {"n": len(values), "min": min(values) if values else None,
            "median": median(values) if values else None,
            "max": max(values) if values else None,
            "positive": sum(x > 0 for x in values)}


def verify_capture(root):
    manifest = json.loads((root / "manifest.json").read_text())
    raw = (root / "samples.jsonl").read_bytes()
    if digest(raw) != manifest.get("samples_sha256"):
        raise ValueError("capture is incomplete or its samples hash differs")
    pairs = [json.loads(line) for line in raw.splitlines()]
    if [p["index"] for p in pairs] != list(range(manifest["planned_samples"])):
        raise ValueError("missing or duplicate sampling slots")
    rules = {}
    for venue in VENUES:
        for role in ("rules_start", "rules_end"):
            member = manifest[role][venue]
            rule_raw = (root / member["path"]).read_bytes()
            if digest(rule_raw) != member["sha256"]:
                raise ValueError("rule raw hash differs")
            project = _binance_market_rules_source if venue == "binance" else _bybit_market_rules_source
            if project(json.loads(rule_raw), market={"exchange": venue, "cex_symbol": "UNI/USDT"},
                       source_instrument="UNIUSDT", observed_at=member["observed_at"]) != member["projection"]:
                raise ValueError("rule projection differs from raw response")
        before = manifest["rules_start"][venue]
        after = manifest["rules_end"][venue]
        semantic = lambda r: {k: v for k, v in r.items() if k not in {"observed_at", "valid_until"}}
        if semantic(before["projection"]) != semantic(after["projection"]):
            raise ValueError("market rules changed during experiment")
        rules[venue] = make_rules(before["projection"], before["sha256"])
    for pair in pairs:
        for book in pair["books"]:
            if book["status"] != "observed":
                continue
            raw = (root / book["path"]).read_bytes()
            if digest(raw) != book["sha256"]:
                raise ValueError("book raw hash differs")
            parsed = parse_book(book["venue"], json.loads(raw), requested_instrument="UNIUSDT")
            for side in ("bids", "asks"):
                if [[str(p), str(q)] for p, q in parsed[side]] != book[side]:
                    raise ValueError("normalized book differs from raw response")
    return manifest, pairs, rules


def analyze(pairs, rules):
    rows, delays = [], []
    by_key, targets = {}, {}
    rejection = Counter()
    for pair in pairs:
        accepted, reason = eligible_pair(pair)
        if not accepted:
            rejection[reason] += 1
            continue
        books = {b["venue"]: b for b in pair["books"]}
        for buy_venue, sell_venue in (VENUES, tuple(reversed(VENUES))):
            buy, sell = books[buy_venue], books[sell_venue]
            for notional in NOTIONALS:
                target = make_target(buy, sell, rules, notional)
                targets[pair["index"], buy_venue, notional] = target
                for scenario, fees in FEE_SCENARIOS.items():
                    result = evaluate_quantity(buy, sell, rules, target,
                                               (fees[buy_venue], fees[sell_venue]))
                    row = {"sample": pair["index"], "at": pair["decision_at"],
                           "buy": buy_venue, "sell": sell_venue, "notional": notional,
                           "quantity": str(target.quantity), "fee_scenario": scenario,
                           "buy_fee_bps": fees[buy_venue], "sell_fee_bps": fees[sell_venue], **result}
                    rows.append(row)
                    by_key[pair["index"], buy_venue, notional, scenario] = row
    # Full distribution is reported, not just surviving positive signals.
    for i, origin in enumerate(pairs):
        if not eligible_pair(origin)[0]:
            continue
        for horizon in (5, 10, 30):
            later = first_delayed_pair(pairs, i, horizon)
            future_reason = "future_tail" if later is None else None
            if later is not None and not eligible_pair(later)[0]:
                future_reason = "future_quality:" + eligible_pair(later)[1]
            actual_delay = None if later is None else later["decision_mono"] - origin["decision_mono"]
            if later is not None and actual_delay > horizon + 6:
                future_reason = "future_timing_gap"
            usable = future_reason is None
            books = {} if later is None else {b["venue"]: b for b in later["books"]}
            for buy, sell in (VENUES, tuple(reversed(VENUES))):
                for notional in NOTIONALS:
                    target = targets[i, buy, notional]
                    for scenario in ("zero_counterfactual", "one_each_counterfactual", "reviewed_public_reference"):
                        initial = by_key[i, buy, notional, scenario]
                        result = {"net": None, "bps": None, "reason": future_reason}
                        if usable:
                            fees = FEE_SCENARIOS[scenario]
                            result = evaluate_quantity(books[buy], books[sell], rules, target,
                                                       (fees[buy], fees[sell]))
                        delays.append({"sample": i, "buy": buy, "sell": sell,
                            "notional": notional, "fee_scenario": scenario,
                            "horizon_seconds": horizon, "actual_delay_seconds": actual_delay,
                            "initial_net": initial["net"], "future_net": result["net"],
                            "reason": result["reason"], "quantity": str(target.quantity)})

    groups = defaultdict(list)
    for row in rows:
        groups[row["buy"], row["notional"], row["fee_scenario"]].append(row)
    summaries = []
    for (buy, notional, scenario), records in groups.items():
        values = [float(r["bps"]) for r in records if r["bps"] is not None]
        summaries.append({"buy": buy, "sell": next(v for v in VENUES if v != buy),
            "notional": notional, "fee_scenario": scenario,
            "eligible_pairs": len(records), "unquotable": len(records) - len(values), **stats(values)})

    delay_groups = defaultdict(list)
    for row in delays:
        delay_groups[row["fee_scenario"], row["horizon_seconds"], row["notional"], row["buy"]].append(row)
    delay_summary = []
    for (scenario, horizon, notional, buy), records in delay_groups.items():
        initial_positive = [r for r in records if r["initial_net"] is not None and Decimal(r["initial_net"]) > 0]
        paired_positive = [r for r in initial_positive if r["future_net"] is not None]
        differences = [float(Decimal(r["future_net"]) - Decimal(r["initial_net"]))
                       for r in records if r["initial_net"] is not None and r["future_net"] is not None]
        delay_summary.append({"fee_scenario": scenario, "horizon": horizon,
            "notional": notional, "buy": buy, "sell": next(v for v in VENUES if v != buy),
            "all_origins": len(records), "paired": len(differences),
            "missing_reasons": dict(Counter(r["reason"] for r in records if r["reason"])),
            "initial_positive": len(initial_positive), "positive_with_future": len(paired_positive),
            # A positive endpoint can have disappeared and reappeared between
            # samples: this does not establish continuous signal survival.
            "positive_at_future": sum(Decimal(r["future_net"]) > 0 for r in paired_positive),
            "actual_delay": stats([r["actual_delay_seconds"] for r in records if r["future_net"] is not None]),
            "net_change_usdt": stats(differences)})

    # Search every positive no-fee sample, not only the best gross spread. Fee
    # rounding can change their ranking. This is a post-hoc in-window maximum.
    thresholds = []
    for buy in VENUES:
        for notional in NOTIONALS:
            candidates = [r for r in rows if r["buy"] == buy and r["notional"] == notional
                          and r["fee_scenario"] == "zero_counterfactual" and r["net"] is not None]
            if not candidates:
                continue
            best_gross = max(candidates, key=lambda r: Decimal(r["bps"]))
            bounds = []
            for candidate in candidates:
                if Decimal(candidate["net"]) <= 0:
                    continue
                lo, hi = Decimal(0), Decimal(50)
                books = {b["venue"]: b for b in pairs[candidate["sample"]]["books"]}
                for _ in range(15):
                    middle = (lo + hi) / 2
                    value = evaluate_quantity(books[buy], books[candidate["sell"]], rules,
                        targets[candidate["sample"], buy, notional], (str(middle), str(middle)))
                    if value["net"] is not None and Decimal(value["net"]) > 0:
                        lo = middle
                    else:
                        hi = middle
                bounds.append((lo, hi, candidate))
            lo, hi, best = max(bounds, key=lambda item: item[0]) if bounds else (None, None, best_gross)
            thresholds.append({"buy": buy, "sell": best["sell"], "notional": notional,
                "sample": best["sample"], "at": best["at"], "best_zero_fee_bps": best["bps"],
                "maximum_zero_fee_bps": best_gross["bps"],
                "largest_positive_equal_leg_fee_bps": None if lo is None else str(lo),
                "first_nonpositive_equal_leg_fee_bps": None if hi is None else str(hi)})
    return rows, delays, {"rejected_pairs": dict(rejection), "summaries": summaries,
                          "delay_summary": delay_summary, "thresholds": thresholds}


def write_csv(path, rows):
    fields = list(dict.fromkeys(key for row in rows for key in row))
    with path.open("w", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--capture", type=Path, required=True)
    args = parser.parse_args()
    manifest, pairs, rules = verify_capture(args.capture)
    rows, delays, summary = analyze(pairs, rules)
    summary["capture"] = {"started_at": manifest["started_at"], "completed_at": manifest["completed_at"],
                          "planned": manifest["planned_samples"], "samples_sha256": manifest["samples_sha256"]}
    summary["pair_count"] = len(pairs)
    summary["eligible_pair_count"] = len(pairs) - sum(summary["rejected_pairs"].values())
    summary["request_seconds"] = stats([b["request_seconds"] for p in pairs for b in p["books"]
                                        if b["status"] == "observed"])
    summary["receive_skew_seconds"] = stats([abs(p["books"][0]["received_mono"] - p["books"][1]["received_mono"])
                                             for p in pairs])
    summary["pair_interval_seconds"] = stats([b["decision_mono"] - a["decision_mono"]
                                              for a, b in zip(pairs, pairs[1:])])
    summary["sources"] = {venue: {
        "responses": sum(b["status"] == "observed" for p in pairs for b in p["books"] if b["venue"] == venue),
        "unique_raw_hashes": len({b["sha256"] for p in pairs for b in p["books"]
                                 if b["venue"] == venue and b["status"] == "observed"}),
        "with_source_timestamp": sum(bool(b.get("source_observed_at")) for p in pairs for b in p["books"]
                                     if b["venue"] == venue),
    } for venue in VENUES}
    fee_path = Path(__file__).resolve().parents[1] / "config/cex_public_fee_schedules_interview_2026-11-01.csv"
    fee_raw = fee_path.read_bytes()
    fee_records = list(csv.DictReader(fee_raw.decode().splitlines()))
    from datetime import datetime
    parse_time = lambda value: datetime.fromisoformat(value.replace("Z", "+00:00"))
    for venue in VENUES:
        record = next(r for r in fee_records if r["venue"] == venue and r["instrument_pattern"] == "UNI/USDT")
        if not (parse_time(record["checked_at"]) <= parse_time(manifest["started_at"])
                <= parse_time(manifest["completed_at"]) <= parse_time(record["valid_until"])):
            raise ValueError("public fee evidence does not cover capture window")
        if record["max_taker_fee_bps"] != FEE_SCENARIOS["reviewed_public_reference"][venue]:
            raise ValueError("public reference scenario differs from reviewed fee evidence")
    summary["public_fee_evidence"] = {"path": str(fee_path), "sha256": digest(fee_raw), "records": fee_records}
    out = args.capture / "analysis"
    out.mkdir(exist_ok=True)
    write_csv(out / "fee_timeseries.csv", rows)
    write_csv(out / "delay_pairs.csv", delays)
    write_csv(out / "fee_summary.csv", summary["summaries"])
    write_csv(out / "break_even.csv", summary["thresholds"])
    (out / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    print(json.dumps({"output": str(out), "rows": len(rows), "delay_pairs": len(delays)}))


if __name__ == "__main__":
    main()
