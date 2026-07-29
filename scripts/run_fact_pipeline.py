"""Run only the source-backed CEX and DEX data collectors."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import shutil
import sqlite3
from collections import Counter
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple

if __package__:
    from . import fetch_cex, fetch_dex
    from .fact_quality import (
        REPORT_SCHEMA,
        _attempt_source,
        build_report,
        normalize_collection_attempts,
        sha256_file,
    )
    from .import_local_snapshot import import_snapshot
    from .market_database import CEX_COLUMNS, DATABASE_FILENAME, DEX_COLUMNS
else:
    import fetch_cex
    import fetch_dex
    from fact_quality import (
        REPORT_SCHEMA,
        _attempt_source,
        build_report,
        normalize_collection_attempts,
        sha256_file,
    )
    from import_local_snapshot import import_snapshot
    from market_database import CEX_COLUMNS, DATABASE_FILENAME, DEX_COLUMNS


PROJECT_ROOT = Path(__file__).resolve().parents[1]
PROCESSED_DIR = PROJECT_ROOT / "data/processed"
LOCAL_DIR = PROJECT_ROOT / "data/local"
DETAILED_FILES = [
    "cex_exchange_volume_daily.csv",
    "dex_pool_volume_daily.csv",
]
ATTEMPT_FILES = [
    "cex_daily_collection_attempts.json",
    "dex_daily_collection_attempts.json",
]
DATABASE_EXPORTS = {
    "cex_exchange_volume_daily.csv": (
        "cex_market_daily",
        CEX_COLUMNS,
        ("date", "token_symbol", "exchange", "cex_symbol"),
    ),
    "dex_pool_volume_daily.csv": (
        "dex_pool_daily",
        DEX_COLUMNS,
        ("date", "token_symbol", "chain", "pool_address"),
    ),
}
QUALITY_REPORT_PATH = Path("quality/daily-latest.json")
SOURCE_FILES = {
    "cex": "cex_exchange_volume_daily.csv",
    "dex": "dex_pool_volume_daily.csv",
}


class CarryForwardEvidenceError(ValueError):
    """Raised when append evidence cannot be tied to the published commit."""


def _counter_dict(values: Sequence[str]) -> Dict[str, int]:
    return dict(sorted(Counter(values).items()))


def _published_lineage(data_dir: Path) -> Dict[str, Any]:
    database_path = data_dir / DATABASE_FILENAME
    if not database_path.exists():
        raise CarryForwardEvidenceError(
            "published quality exists but the SQLite commit point is missing"
        )
    connection = sqlite3.connect(
        "{}?mode=ro".format(database_path.resolve().as_uri()),
        uri=True,
    )
    connection.row_factory = sqlite3.Row
    try:
        row = connection.execute(
            """
            SELECT
                state.snapshot_id,
                state.import_run_id,
                snapshot.cex_source_name,
                snapshot.dex_source_name,
                snapshot.cex_sha256,
                snapshot.dex_sha256,
                snapshot.cex_row_count,
                snapshot.dex_row_count
            FROM dataset_state AS state
            JOIN dataset_snapshots AS snapshot
              ON snapshot.snapshot_id = state.snapshot_id
            WHERE state.singleton_id = 1
            """
        ).fetchone()
    finally:
        connection.close()
    if row is None:
        raise CarryForwardEvidenceError(
            "SQLite does not contain a published dataset_state row"
        )
    return dict(row)


def load_append_attempt_evidence(data_dir: Path) -> Dict[str, list[Dict[str, Any]]]:
    """Load trusted non-success evidence from the current publication.

    A missing report is treated as a legacy snapshot with no evidence to
    preserve. Once a report exists, every publication, source, and attempt
    lineage check is fail-closed so an append cannot silently relabel known
    source outcomes as unexplained gaps.
    """

    report_path = data_dir / QUALITY_REPORT_PATH
    if not report_path.exists():
        return {"cex": [], "dex": []}
    try:
        report = json.loads(report_path.read_text(encoding="utf-8"))
        if not isinstance(report, dict) or report.get("schema") != REPORT_SCHEMA:
            raise ValueError("unsupported quality-report schema")
        lineage = _published_lineage(data_dir)
        publication = report.get("publication")
        if not isinstance(publication, dict):
            raise ValueError("quality publication metadata is missing")
        if publication.get("import_run_id") != lineage["import_run_id"]:
            raise ValueError("quality import_run_id does not match SQLite")
        if publication.get("dataset_snapshot_id") != lineage["snapshot_id"]:
            raise ValueError("quality snapshot_id does not match SQLite")

        raw_sources = report.get("sources")
        if not isinstance(raw_sources, list) or len(raw_sources) != 2:
            raise ValueError("quality sources must contain exactly CEX and DEX")
        sources: Dict[str, Mapping[str, Any]] = {}
        for source in raw_sources:
            if not isinstance(source, dict):
                raise ValueError("quality source is not an object")
            market_type = source.get("market_type")
            if market_type not in SOURCE_FILES or market_type in sources:
                raise ValueError("quality sources contain an invalid market type")
            sources[str(market_type)] = source
        if set(sources) != set(SOURCE_FILES):
            raise ValueError("quality sources do not cover CEX and DEX")

        for market_type, filename in SOURCE_FILES.items():
            source = sources[market_type]
            sha_column = "{}_sha256".format(market_type)
            count_column = "{}_row_count".format(market_type)
            name_column = "{}_source_name".format(market_type)
            if source.get("name") != filename:
                raise ValueError("{} quality source name is invalid".format(market_type))
            if lineage[name_column] != filename:
                raise ValueError("{} SQLite source name is invalid".format(market_type))
            if source.get("sha256") != lineage[sha_column]:
                raise ValueError(
                    "{} quality source hash does not match SQLite".format(market_type)
                )
            if source.get("row_count") != lineage[count_column]:
                raise ValueError(
                    "{} quality row count does not match SQLite".format(market_type)
                )
            published_csv = data_dir / filename
            if (
                not published_csv.exists()
                or sha256_file(published_csv) != lineage[sha_column]
            ):
                raise ValueError(
                    "{} published CSV does not match the commit lineage".format(
                        market_type
                    )
                )

        raw_attempts = report.get("collection_attempts")
        if not isinstance(raw_attempts, list):
            raise ValueError("quality collection_attempts is not a list")
        grouped_raw: Dict[str, list[Mapping[str, Any]]] = {
            "cex": [],
            "dex": [],
        }
        for attempt in raw_attempts:
            if not isinstance(attempt, dict):
                raise ValueError("quality collection attempt is not an object")
            market_type = attempt.get("market_type")
            if market_type not in grouped_raw:
                raise ValueError("quality collection attempt has invalid market type")
            grouped_raw[str(market_type)].append(attempt)
        normalized = {
            market_type: normalize_collection_attempts(
                attempts,
                market_type=market_type,
            )
            for market_type, attempts in grouped_raw.items()
        }

        raw_attempt_sources = report.get("attempt_sources")
        if (
            not isinstance(raw_attempt_sources, list)
            or len(raw_attempt_sources) != 2
        ):
            raise ValueError("quality attempt_sources must contain CEX and DEX")
        attempt_sources: Dict[str, Mapping[str, Any]] = {}
        for source in raw_attempt_sources:
            if not isinstance(source, dict):
                raise ValueError("quality attempt source is not an object")
            market_type = source.get("market_type")
            if market_type not in SOURCE_FILES or market_type in attempt_sources:
                raise ValueError("quality attempt_sources contain an invalid market type")
            attempt_sources[str(market_type)] = source
        if set(attempt_sources) != set(SOURCE_FILES):
            raise ValueError("quality attempt_sources do not cover CEX and DEX")
        for market_type in SOURCE_FILES:
            source = attempt_sources[market_type]
            status = source.get("status")
            attempts = normalized[market_type]
            if status not in {"accepted", "absent"}:
                raise ValueError(
                    "{} prior attempt source is not trusted".format(market_type)
                )
            if source.get("attempt_count") != len(attempts):
                raise ValueError(
                    "{} attempt source count does not match".format(market_type)
                )
            if status == "absent" and attempts:
                raise ValueError(
                    "{} absent attempt source contains attempts".format(market_type)
                )
            if status == "accepted" and source.get("reason_counts") != _counter_dict(
                [str(item["reason_code"]) for item in attempts]
            ):
                raise ValueError(
                    "{} attempt source reasons do not match".format(market_type)
                )

        attempt_summary = report.get("collection_attempt_summary")
        if not isinstance(attempt_summary, dict):
            raise ValueError("quality collection_attempt_summary is missing")
        all_attempts = [*normalized["cex"], *normalized["dex"]]
        if attempt_summary.get("attempt_count") != len(all_attempts):
            raise ValueError("quality attempt summary count does not match")
        if attempt_summary.get("status_counts") != _counter_dict(
            [str(item["status"]) for item in all_attempts]
        ):
            raise ValueError("quality attempt status summary does not match")
        if attempt_summary.get("reason_code_counts") != _counter_dict(
            [str(item["reason_code"]) for item in all_attempts]
        ):
            raise ValueError("quality attempt reason summary does not match")

        return {
            market_type: [
                item
                for item in attempts
                if item["status"] != "succeeded"
                and item["reason_code"] != "observed"
            ]
            for market_type, attempts in normalized.items()
        }
    except CarryForwardEvidenceError:
        raise
    except (OSError, ValueError, TypeError, json.JSONDecodeError, sqlite3.Error) as error:
        raise CarryForwardEvidenceError(
            "cannot safely carry prior collection attempts: {}".format(error)
        ) from error


def _stable_market_key(attempt_or_market: Mapping[str, Any]) -> Optional[Tuple[str, ...]]:
    market_type = attempt_or_market.get("market_type")
    token = str(attempt_or_market.get("token_symbol") or "").strip().upper()
    if market_type == "cex":
        exchange = str(attempt_or_market.get("exchange") or "").strip().lower()
        if token and exchange:
            return ("cex", token, exchange)
        return None
    if market_type == "dex":
        chain = str(attempt_or_market.get("chain") or "").strip().lower()
        address = str(attempt_or_market.get("pool_address") or "").strip()
        if address.startswith("0x"):
            address = address.lower()
        if token and chain and address:
            return ("dex", token, chain, address)
    return None


def _attempt_window(
    attempt: Mapping[str, Any],
) -> Optional[Tuple[date, date]]:
    start_text = attempt.get("requested_start_date")
    end_text = attempt.get("requested_end_date")
    if not start_text or not end_text:
        return None
    return date.fromisoformat(str(start_text)), date.fromisoformat(str(end_text))


def _date_segments(days: Sequence[date]) -> list[Tuple[date, date]]:
    if not days:
        return []
    ordered = sorted(set(days))
    segments: list[Tuple[date, date]] = []
    start = ordered[0]
    end = ordered[0]
    for day_value in ordered[1:]:
        if day_value == end + timedelta(days=1):
            end = day_value
            continue
        segments.append((start, end))
        start = end = day_value
    segments.append((start, end))
    return segments


def _attempt_with_window(
    attempt: Mapping[str, Any],
    start_day: date,
    end_day: date,
) -> Dict[str, Any]:
    start_text = start_day.isoformat()
    end_text = end_day.isoformat()
    observed_dates = [
        day_text
        for day_text in attempt.get("observed_dates") or []
        if start_text <= str(day_text) <= end_text
    ]
    result = dict(attempt)
    result["requested_start_date"] = start_text
    result["requested_end_date"] = end_text
    result["observed_dates"] = observed_dates
    result["observed_day_count"] = len(observed_dates)
    identity = {
        key: result.get(key)
        for key in (
            "market_type",
            "token_symbol",
            "exchange",
            "instrument",
            "chain",
            "dex",
            "pool_address",
        )
    }
    id_material = {
        **identity,
        "requested_start_date": start_text,
        "requested_end_date": end_text,
        "status": result.get("status"),
        "reason_code": result.get("reason_code"),
    }
    result["attempt_id"] = hashlib.sha256(
        json.dumps(
            id_material,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()[:20]
    return result


def _dedupe_key(attempt: Mapping[str, Any]) -> Tuple[Any, ...]:
    return tuple(
        attempt.get(key)
        for key in (
            "market_type",
            "token_symbol",
            "exchange",
            "instrument",
            "chain",
            "dex",
            "pool_address",
            "requested_start_date",
            "requested_end_date",
        )
    )


def _candidate_gap_keys(processed_dir: Path) -> set[Tuple[Tuple[str, ...], str]]:
    report = build_report(
        processed_dir / SOURCE_FILES["cex"],
        processed_dir / SOURCE_FILES["dex"],
    )
    gaps = set()
    for issue in report.get("issues") or []:
        if issue.get("category") not in {"historical_gap", "d1_active_gap"}:
            continue
        market = issue.get("market")
        if not isinstance(market, dict):
            continue
        market_key = _stable_market_key(market)
        day_text = issue.get("date")
        if market_key is not None and day_text:
            gaps.add((market_key, str(day_text)))
    return gaps


def _carried_segments(
    prior_attempts: Sequence[Mapping[str, Any]],
    new_attempts: Sequence[Mapping[str, Any]],
    *,
    gap_keys: set[Tuple[Tuple[str, ...], str]],
) -> list[Dict[str, Any]]:
    new_windows: Dict[Tuple[str, ...], list[Tuple[date, date]]] = {}
    for attempt in new_attempts:
        market_key = _stable_market_key(attempt)
        window = _attempt_window(attempt)
        if market_key is not None and window is not None:
            new_windows.setdefault(market_key, []).append(window)

    carried: list[Dict[str, Any]] = []
    for attempt in prior_attempts:
        if (
            attempt.get("status") == "succeeded"
            or attempt.get("reason_code") == "observed"
        ):
            continue
        market_key = _stable_market_key(attempt)
        window = _attempt_window(attempt)
        if market_key is None or window is None:
            continue
        start_day, end_day = window
        remaining_days = {
            start_day + timedelta(days=offset)
            for offset in range((end_day - start_day).days + 1)
        }
        for replacement_start, replacement_end in new_windows.get(
            market_key,
            [],
        ):
            remaining_days = {
                day_value
                for day_value in remaining_days
                if not replacement_start <= day_value <= replacement_end
            }
        observed_dates = set(attempt.get("observed_dates") or [])
        for segment_start, segment_end in _date_segments(list(remaining_days)):
            explains_gap = any(
                key == market_key
                and segment_start.isoformat() <= day_text <= segment_end.isoformat()
                and day_text not in observed_dates
                for key, day_text in gap_keys
            )
            if explains_gap:
                carried.append(
                    _attempt_with_window(
                        attempt,
                        segment_start,
                        segment_end,
                    )
                )
    return carried


def merge_append_attempt_evidence(
    *,
    processed_dir: Path,
    prior_attempts: Mapping[str, Sequence[Mapping[str, Any]]],
    collected_market_types: Sequence[str],
) -> None:
    """Rebind trusted prior failures to the new complete candidate CSVs."""

    gap_keys = _candidate_gap_keys(processed_dir)
    collected = set(collected_market_types)
    for market_type, filename in SOURCE_FILES.items():
        csv_path = processed_dir / filename
        ledger_path = processed_dir / (
            "cex_daily_collection_attempts.json"
            if market_type == "cex"
            else "dex_daily_collection_attempts.json"
        )
        new_attempts: list[Dict[str, Any]] = []
        if market_type in collected:
            loaded, metadata = _attempt_source(
                path=ledger_path,
                market_type=market_type,
                source_csv_sha256=sha256_file(csv_path),
            )
            if metadata.get("status") != "accepted":
                raise CarryForwardEvidenceError(
                    "{} collector produced an invalid attempt ledger".format(
                        market_type
                    )
                )
            new_attempts = loaded
        elif ledger_path.exists():
            raise CarryForwardEvidenceError(
                "unselected {} collector left unexpected attempt evidence".format(
                    market_type
                )
            )

        carried = _carried_segments(
            prior_attempts.get(market_type) or [],
            new_attempts,
            gap_keys=gap_keys,
        )
        merged_by_key: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
        for attempt in sorted(
            carried,
            key=lambda item: (
                str(item.get("finished_at_utc") or ""),
                str(item.get("attempt_id") or ""),
            ),
        ):
            merged_by_key[_dedupe_key(attempt)] = attempt
        # The current collector always wins an exact market/window identity,
        # independent of wall-clock skew.
        for attempt in new_attempts:
            merged_by_key[_dedupe_key(attempt)] = attempt
        merged = list(merged_by_key.values())

        if not merged and market_type not in collected:
            ledger_path.unlink(missing_ok=True)
            continue
        ledger_windows = {
            (
                item.get("requested_start_date"),
                item.get("requested_end_date"),
            )
            for item in merged
        }
        if len(ledger_windows) == 1:
            ledger_start, ledger_end = next(iter(ledger_windows))
        else:
            ledger_start = ledger_end = None
        writer = (
            fetch_cex.write_attempt_ledger
            if market_type == "cex"
            else fetch_dex.write_attempt_ledger
        )
        writer(
            ledger_path,
            merged,
            source_csv=csv_path,
            start_date=ledger_start,
            end_date=ledger_end,
        )


def parse_list(value: str | None, *, upper: bool) -> list[str] | None:
    if not value:
        return None
    transform = str.upper if upper else str.lower
    values = [transform(item.strip()) for item in value.split(",") if item.strip()]
    return values or None


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Refresh fact-only market data")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--cex-only", action="store_true")
    mode.add_argument("--dex-only", action="store_true")
    parser.add_argument("--tokens", help="Comma-separated token symbols")
    parser.add_argument("--exchanges", help="Comma-separated CEX names")
    parser.add_argument(
        "--append",
        action="store_true",
        help="Upsert selected Token dates while preserving the current snapshot",
    )
    parser.add_argument("--start", help="Inclusive UTC date")
    parser.add_argument("--end", help="Inclusive UTC date")
    parser.add_argument("--publish-local", action="store_true")
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=LOCAL_DIR,
        help="Runtime snapshot directory to read and atomically publish",
    )
    parser.add_argument(
        "--processed-dir",
        type=Path,
        help="Collector staging directory (defaults beside --data-dir)",
    )
    return parser.parse_args()


def resolve_limit_days(start: str | None, end: str | None) -> int:
    if not start and not end:
        return 180
    if not start or not end:
        raise ValueError("--start and --end must be provided together")
    start_date = date.fromisoformat(start)
    end_date = date.fromisoformat(end)
    days = (end_date - start_date).days + 1
    if days < 1 or days > 180:
        raise ValueError("Refresh window must contain between 1 and 180 days")
    return min(180, days + 3)


def seed_processed_from_local(local_dir: Path, processed_dir: Path) -> None:
    """Start an incremental refresh from the currently published snapshot."""
    processed_dir.mkdir(parents=True, exist_ok=True)
    database_path = local_dir / DATABASE_FILENAME
    if database_path.exists():
        connection = sqlite3.connect(
            f"{database_path.resolve().as_uri()}?mode=ro",
            uri=True,
        )
        connection.row_factory = sqlite3.Row
        try:
            for filename, (
                table,
                columns,
                order_columns,
            ) in DATABASE_EXPORTS.items():
                query = "SELECT {} FROM {} ORDER BY {}".format(
                    ", ".join(columns),
                    table,
                    ", ".join(order_columns),
                )
                rows = connection.execute(query)
                target = processed_dir / filename
                temporary = target.with_name(f".{target.name}.seed.tmp")
                try:
                    with temporary.open(
                        "w",
                        newline="",
                        encoding="utf-8",
                    ) as handle:
                        writer = csv.DictWriter(
                            handle,
                            fieldnames=columns,
                            lineterminator="\n",
                        )
                        writer.writeheader()
                        writer.writerows(dict(row) for row in rows)
                    temporary.replace(target)
                finally:
                    temporary.unlink(missing_ok=True)
        finally:
            connection.close()
        return
    for filename in DETAILED_FILES:
        source = local_dir / filename
        target = processed_dir / filename
        if not source.exists():
            raise FileNotFoundError(f"Cannot append without current local snapshot: {source}")
        shutil.copyfile(source, target)


def resolve_processed_dir(
    data_dir: Path,
    configured_processed_dir: Optional[Path],
) -> Path:
    if configured_processed_dir is not None:
        return configured_processed_dir.expanduser().resolve()
    if data_dir == LOCAL_DIR.resolve():
        return PROCESSED_DIR.resolve()
    return (data_dir.parent / f".{data_dir.name}-processed").resolve()


def main() -> None:
    args = parse_args()
    tokens = parse_list(args.tokens, upper=True)
    exchanges = parse_list(args.exchanges, upper=False)
    limit_days = resolve_limit_days(args.start, args.end)
    data_dir = args.data_dir.expanduser().resolve()
    processed_dir = resolve_processed_dir(data_dir, args.processed_dir)
    processed_dir.mkdir(parents=True, exist_ok=True)
    # Attempt evidence is valid only for the collector invocations in this
    # pipeline run or a lineage-validated prior publication. Never let an
    # arbitrary staging artifact explain a new publication.
    for filename in ATTEMPT_FILES:
        (processed_dir / filename).unlink(missing_ok=True)

    prior_attempts: Dict[str, list[Dict[str, Any]]] = {
        "cex": [],
        "dex": [],
    }
    if args.append:
        if tokens is None:
            raise ValueError("--append requires --tokens")
        prior_attempts = load_append_attempt_evidence(data_dir)
        seed_processed_from_local(data_dir, processed_dir)

    collected_market_types = []
    if not args.dex_only:
        fetch_cex.main(
            token_symbols=tokens,
            exchanges=exchanges,
            append=args.append,
            start_date=args.start,
            end_date=args.end,
            limit_days=limit_days,
            output_dir=processed_dir,
        )
        collected_market_types.append("cex")
    if not args.cex_only:
        fetch_dex.main(
            token_symbols=tokens,
            append=args.append,
            start_date=args.start,
            end_date=args.end,
            limit_days=limit_days,
            output_dir=processed_dir,
            local_dir=data_dir,
        )
        collected_market_types.append("dex")
    if args.append:
        merge_append_attempt_evidence(
            processed_dir=processed_dir,
            prior_attempts=prior_attempts,
            collected_market_types=collected_market_types,
        )
    if args.publish_local:
        import_snapshot(processed_dir, target_dir=data_dir)


if __name__ == "__main__":
    main()
