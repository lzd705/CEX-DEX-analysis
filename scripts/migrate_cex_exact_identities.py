"""Atomically migrate historical CEX rows to exact venue identities.

The runner intentionally separates collection from publication.  Every
bounded source window is appended to one private staging snapshot and its
attempt ledger is validated against the just-written CEX CSV hash.  Only the
fully merged, lineage-bound candidate may reach ``import_snapshot`` and that
commit point is invoked at most once.
"""

from __future__ import annotations

import argparse
import csv
import fcntl
import json
from collections import Counter
from datetime import date, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

if __package__:
    from . import fetch_cex
    from .fact_quality import _attempt_source, sha256_file
    from .import_local_snapshot import import_snapshot
    from .market_database import CEX_COLUMNS
    from .run_fact_pipeline import (
        LOCAL_DIR,
        load_append_attempt_evidence,
        merge_append_attempt_evidence,
        seed_processed_from_local,
    )
else:  # pragma: no cover - exercised by the direct CLI smoke test
    import fetch_cex
    from fact_quality import _attempt_source, sha256_file
    from import_local_snapshot import import_snapshot
    from market_database import CEX_COLUMNS
    from run_fact_pipeline import (
        LOCAL_DIR,
        load_append_attempt_evidence,
        merge_append_attempt_evidence,
        seed_processed_from_local,
    )


MAX_WINDOW_DAYS = 180
TARGET_EXCHANGES = ("upbit", "coinbase", "kraken")
CEX_FILENAME = "cex_exchange_volume_daily.csv"
DEX_FILENAME = "dex_pool_volume_daily.csv"
CEX_ATTEMPT_FILENAME = "cex_daily_collection_attempts.json"
NUMERIC_CEX_FIELDS = {
    "open",
    "high",
    "low",
    "close",
    "base_volume",
    "quote_volume_usd",
}


class MigrationPreflightError(ValueError):
    """Raised before publication when exact-identity evidence is incomplete."""


def split_date_windows(
    start_date: str,
    end_date: str,
    *,
    maximum_days: int = MAX_WINDOW_DAYS,
) -> List[Tuple[str, str]]:
    """Split one inclusive UTC date range into bounded contiguous windows."""

    start_day = date.fromisoformat(start_date)
    end_day = date.fromisoformat(end_date)
    if start_day > end_day:
        raise ValueError("migration start date must not be after end date")
    if maximum_days < 1 or maximum_days > MAX_WINDOW_DAYS:
        raise ValueError("migration windows must contain between 1 and 180 days")

    windows: List[Tuple[str, str]] = []
    current_start = start_day
    while current_start <= end_day:
        current_end = min(
            end_day,
            current_start + timedelta(days=maximum_days - 1),
        )
        windows.append((current_start.isoformat(), current_end.isoformat()))
        current_start = current_end + timedelta(days=1)
    return windows


def _read_csv_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        raise MigrationPreflightError("missing staging input: {}".format(path.name))
    with path.open("r", newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        if not reader.fieldnames:
            raise MigrationPreflightError(
                "staging input has no CSV header: {}".format(path.name)
            )
    if not rows:
        raise MigrationPreflightError(
            "staging input has no data rows: {}".format(path.name)
        )
    return rows


def _normalize_decimal(value: Any) -> Optional[Decimal]:
    if value in (None, ""):
        return None
    try:
        return Decimal(str(value)).normalize()
    except (InvalidOperation, ValueError) as error:
        raise MigrationPreflightError(
            "CEX candidate contains a non-numeric fact"
        ) from error


def _cex_row_signature(row: Mapping[str, Any]) -> Tuple[Any, ...]:
    values: List[Any] = []
    for field in CEX_COLUMNS:
        value = row.get(field)
        if field in NUMERIC_CEX_FIELDS:
            values.append(_normalize_decimal(value))
        elif field == "token_symbol":
            values.append(str(value or "").strip().upper())
        elif field == "exchange":
            values.append(str(value or "").strip().lower())
        elif field == "cex_symbol":
            values.append(str(value or "").strip().upper())
        else:
            values.append(str(value or "").strip())
    return tuple(values)


def _grain_duplicates(
    rows: Sequence[Mapping[str, Any]],
    fields: Sequence[str],
) -> int:
    counts = Counter(
        tuple(str(row.get(field) or "").strip() for field in fields)
        for row in rows
    )
    return sum(count - 1 for count in counts.values() if count > 1)


def _date_bounds(rows: Sequence[Mapping[str, Any]]) -> Tuple[str, str]:
    days = [str(row.get("date") or "").strip() for row in rows]
    if not days or any(not value for value in days):
        raise MigrationPreflightError("candidate contains an empty fact date")
    try:
        parsed = sorted(date.fromisoformat(value) for value in days)
    except ValueError as error:
        raise MigrationPreflightError("candidate contains an invalid fact date") from error
    return parsed[0].isoformat(), parsed[-1].isoformat()


def _configured_instruments(
    requested_tokens: Optional[Sequence[str]],
) -> Dict[str, str]:
    configured: Dict[str, str] = {}
    for row in fetch_cex.read_token_config(fetch_cex.TOKEN_CONFIG_PATH):
        token = str(row.get("token_symbol") or "").strip().upper()
        instrument = str(row.get("cex_symbol") or "").strip().upper()
        if not token or not instrument:
            raise MigrationPreflightError(
                "configured CEX migration identity is incomplete"
            )
        if token in configured:
            raise MigrationPreflightError(
                "configured Token is duplicated: {}".format(token)
            )
        configured[token] = instrument

    if requested_tokens is None:
        selected = sorted(configured)
    else:
        selected = []
        seen = set()
        for raw_token in requested_tokens:
            token = str(raw_token or "").strip().upper()
            if not token or token in seen:
                raise MigrationPreflightError(
                    "migration Tokens must be nonempty and unique"
                )
            seen.add(token)
            selected.append(token)
    if not selected:
        raise MigrationPreflightError("migration requires at least one Token")
    missing = sorted(set(selected) - set(configured))
    if missing:
        raise MigrationPreflightError(
            "migration Tokens are absent from config/tokens.csv: {}".format(
                ", ".join(missing)
            )
        )
    return {token: configured[token] for token in selected}


def _expected_attempt_keys(
    configured: Mapping[str, str],
    start_date: str,
    end_date: str,
) -> set[Tuple[str, str, str, str, str]]:
    return {
        (
            token,
            exchange,
            fetch_cex.canonical_collected_instrument(exchange, instrument),
            start_date,
            end_date,
        )
        for token, instrument in configured.items()
        for exchange in TARGET_EXCHANGES
    }


def _attempt_key(attempt: Mapping[str, Any]) -> Tuple[str, str, str, str, str]:
    return (
        str(attempt.get("token_symbol") or "").strip().upper(),
        str(attempt.get("exchange") or "").strip().lower(),
        str(attempt.get("instrument") or "").strip().upper(),
        str(attempt.get("requested_start_date") or ""),
        str(attempt.get("requested_end_date") or ""),
    )


def _is_conclusive_attempt(attempt: Mapping[str, Any]) -> bool:
    status = str(attempt.get("status") or "")
    reason = str(attempt.get("reason_code") or "")
    return status in {"succeeded", "no_data"} or (
        status == "failed" and reason == "not_listed"
    )


def _validate_window_attempts(
    attempts: Sequence[Mapping[str, Any]],
    *,
    configured: Mapping[str, str],
    start_date: str,
    end_date: str,
) -> None:
    expected = _expected_attempt_keys(configured, start_date, end_date)
    actual = [_attempt_key(attempt) for attempt in attempts]
    if len(actual) != len(set(actual)) or set(actual) != expected:
        missing = sorted(expected - set(actual))
        unexpected = sorted(set(actual) - expected)
        raise MigrationPreflightError(
            "attempt scope is incomplete for {}..{}; missing={} unexpected={}".format(
                start_date,
                end_date,
                len(missing),
                len(unexpected),
            )
        )
    for attempt in attempts:
        if str(attempt.get("status") or "") == "partial":
            raise MigrationPreflightError(
                "partial exact-identity window blocks migration because it "
                "cannot replace unobserved baseline dates"
            )
        if not _is_conclusive_attempt(attempt):
            raise MigrationPreflightError(
                "technical collection outcome blocks exact identity migration: "
                "{}/{}/{}".format(
                    attempt.get("exchange"),
                    attempt.get("status"),
                    attempt.get("reason_code"),
                )
            )
        instrument = str(attempt.get("instrument") or "").strip().upper()
        source_instrument = attempt.get("source_instrument")
        if source_instrument not in (None, "") and (
            str(source_instrument).strip().upper() != instrument
        ):
            raise MigrationPreflightError(
                "attempt source instrument differs from the exact market identity"
            )
        if bool(attempt.get("source_instrument_alias_validated")):
            raise MigrationPreflightError(
                "attempt still contains a legacy source-instrument alias"
            )
        observed_dates = list(attempt.get("observed_dates") or [])
        if any(
            str(day_text) < start_date or str(day_text) > end_date
            for day_text in observed_dates
        ):
            raise MigrationPreflightError(
                "attempt observation falls outside its requested window"
            )


def _load_hash_bound_attempts(staging_dir: Path) -> List[Dict[str, Any]]:
    cex_path = staging_dir / CEX_FILENAME
    attempts, metadata = _attempt_source(
        path=staging_dir / CEX_ATTEMPT_FILENAME,
        market_type="cex",
        source_csv_sha256=sha256_file(cex_path),
    )
    if metadata.get("status") != "accepted":
        raise MigrationPreflightError(
            "CEX attempt ledger is not bound to the current staging CSV hash"
        )
    return attempts


def _mutable_target_row(
    row: Mapping[str, Any],
    *,
    configured: Mapping[str, str],
    start_date: str,
    end_date: str,
) -> bool:
    token = str(row.get("token_symbol") or "").strip().upper()
    exchange = str(row.get("exchange") or "").strip().lower()
    instrument = str(row.get("cex_symbol") or "").strip().upper()
    day_text = str(row.get("date") or "").strip()
    if (
        token not in configured
        or exchange not in TARGET_EXCHANGES
        or not start_date <= day_text <= end_date
    ):
        return False
    expected = fetch_cex.canonical_collected_instrument(
        exchange,
        configured[token],
    )
    candidates = {expected}
    base_asset = expected.split("/", 1)[0]
    if exchange == "upbit" and expected.endswith("/USDT"):
        candidates.add(base_asset + "/KRW")
    elif exchange in {"coinbase", "kraken"} and expected.endswith("/USD"):
        candidates.add(base_asset + "/USDT")
    return instrument in candidates


def _temporary_residues(staging_dir: Path) -> List[str]:
    return sorted(
        path.name
        for path in staging_dir.iterdir()
        if path.name.startswith(".") or path.name.endswith(".tmp")
    )


def _is_legacy_upbit_krw_fallback(
    row: Mapping[str, Any],
    configured: Mapping[str, str],
) -> bool:
    """Identify only KRW rows that conflict with configured TOKEN/USDT."""
    if str(row.get("exchange") or "").strip().lower() != "upbit":
        return False
    token = str(row.get("token_symbol") or "").strip().upper()
    configured_instrument = configured.get(token)
    if configured_instrument is None:
        return False
    expected = fetch_cex.canonical_collected_instrument(
        "upbit",
        configured_instrument,
    )
    if not expected.endswith("/USDT"):
        return False
    observed = str(row.get("cex_symbol") or "").strip().upper()
    base_asset = expected.split("/", 1)[0]
    return observed == base_asset + "/KRW"


def production_rebuild_preflight(
    *,
    staging_dir: Path,
    baseline_cex_rows: Sequence[Mapping[str, Any]],
    baseline_dex_bytes: bytes,
    configured: Mapping[str, str],
    start_date: str,
    end_date: str,
    expected_attempts: Sequence[Mapping[str, Any]],
) -> Dict[str, Any]:
    """Fail closed unless the complete staged migration is publication-safe."""

    cex_path = staging_dir / CEX_FILENAME
    dex_path = staging_dir / DEX_FILENAME
    final_cex_rows = _read_csv_rows(cex_path)
    final_dex_rows = _read_csv_rows(dex_path)

    cex_duplicate_count = _grain_duplicates(
        final_cex_rows,
        ("date", "token_symbol", "exchange", "cex_symbol"),
    )
    dex_duplicate_count = _grain_duplicates(
        final_dex_rows,
        ("date", "token_symbol", "chain", "pool_address"),
    )
    if cex_duplicate_count or dex_duplicate_count:
        raise MigrationPreflightError(
            "duplicate daily fact grain remains in the migration candidate"
        )

    legacy_upbit_krw = sum(
        1
        for row in final_cex_rows
        if _is_legacy_upbit_krw_fallback(row, configured)
    )
    legacy_usdt_usd_adapters = sum(
        1
        for row in final_cex_rows
        if str(row.get("exchange") or "").strip().lower()
        in {"coinbase", "kraken"}
        and str(row.get("cex_symbol") or "").strip().upper().endswith("/USDT")
    )
    if legacy_upbit_krw or legacy_usdt_usd_adapters:
        raise MigrationPreflightError(
            "legacy exact-identity rows remain: upbit_krw={} usd_adapter_usdt={}".format(
                legacy_upbit_krw,
                legacy_usdt_usd_adapters,
            )
        )

    final_market_dates = {
        (
            str(row.get("date") or "").strip(),
            str(row.get("token_symbol") or "").strip().upper(),
            str(row.get("exchange") or "").strip().lower(),
            str(row.get("cex_symbol") or "").strip().upper(),
        )
        for row in final_cex_rows
    }
    required_target_market_dates = set()
    for row in baseline_cex_rows:
        if not _mutable_target_row(
            row,
            configured=configured,
            start_date=start_date,
            end_date=end_date,
        ):
            continue
        token = str(row.get("token_symbol") or "").strip().upper()
        exchange = str(row.get("exchange") or "").strip().lower()
        expected_instrument = fetch_cex.canonical_collected_instrument(
            exchange,
            configured[token],
        )
        required_target_market_dates.add(
            (
                str(row.get("date") or "").strip(),
                token,
                exchange,
                expected_instrument,
            )
        )
    missing_target_market_dates = sorted(
        required_target_market_dates - final_market_dates
    )
    if missing_target_market_dates:
        raise MigrationPreflightError(
            "historical target observation would be lost during exact "
            "identity migration: {} market-date rows".format(
                len(missing_target_market_dates)
            )
        )

    baseline_bounds = _date_bounds(baseline_cex_rows)
    final_bounds = _date_bounds(final_cex_rows)
    if final_bounds[0] > baseline_bounds[0] or final_bounds[1] < baseline_bounds[1]:
        raise MigrationPreflightError("CEX publication date range would shrink")

    baseline_immutable = Counter(
        _cex_row_signature(row)
        for row in baseline_cex_rows
        if not _mutable_target_row(
            row,
            configured=configured,
            start_date=start_date,
            end_date=end_date,
        )
    )
    final_immutable = Counter(
        _cex_row_signature(row)
        for row in final_cex_rows
        if not _mutable_target_row(
            row,
            configured=configured,
            start_date=start_date,
            end_date=end_date,
        )
    )
    if final_immutable != baseline_immutable:
        raise MigrationPreflightError(
            "non-target CEX facts changed during exact identity migration"
        )

    if dex_path.read_bytes() != baseline_dex_bytes:
        raise MigrationPreflightError(
            "DEX candidate changed during CEX identity migration"
        )

    final_attempts = _load_hash_bound_attempts(staging_dir)
    final_by_id = {
        str(attempt.get("attempt_id") or ""): attempt
        for attempt in final_attempts
    }
    if len(final_by_id) != len(final_attempts):
        raise MigrationPreflightError("final CEX attempt ledger has duplicate IDs")
    for expected in expected_attempts:
        attempt_id = str(expected.get("attempt_id") or "")
        if attempt_id not in final_by_id or final_by_id[attempt_id] != expected:
            raise MigrationPreflightError(
                "final CEX attempt ledger lost current-window evidence"
            )

    residues = _temporary_residues(staging_dir)
    if residues:
        raise MigrationPreflightError(
            "temporary staging residues block publication: {}".format(
                ", ".join(residues)
            )
        )

    return {
        "status": "passed",
        "cex_row_count": len(final_cex_rows),
        "dex_row_count": len(final_dex_rows),
        "cex_duplicate_grain_count": cex_duplicate_count,
        "dex_duplicate_grain_count": dex_duplicate_count,
        "legacy_upbit_krw_row_count": legacy_upbit_krw,
        "legacy_coinbase_kraken_usdt_row_count": legacy_usdt_usd_adapters,
        "baseline_date_min": baseline_bounds[0],
        "baseline_date_max": baseline_bounds[1],
        "candidate_date_min": final_bounds[0],
        "candidate_date_max": final_bounds[1],
        "non_target_cex_unchanged": True,
        "preserved_target_market_date_count": len(
            required_target_market_dates
        ),
        "dex_bytes_unchanged": True,
        "current_attempt_count": len(expected_attempts),
        "final_attempt_count": len(final_attempts),
        "cex_sha256": sha256_file(cex_path),
        "dex_sha256": sha256_file(dex_path),
        "attempt_ledger_sha256": sha256_file(
            staging_dir / CEX_ATTEMPT_FILENAME
        ),
    }


def _run_migration_under_lock(
    *,
    data_dir: Path,
    staging_dir: Path,
    start_date: str,
    end_date: str,
    tokens: Optional[Sequence[str]] = None,
    apply: bool = False,
    remove_legacy_upbit_krw_fallback: bool = False,
) -> Dict[str, Any]:
    """Build, validate, and optionally publish one exact-identity snapshot."""

    data_dir = Path(data_dir).expanduser().resolve()
    staging_dir = Path(staging_dir).expanduser().resolve()
    if not data_dir.is_dir():
        raise MigrationPreflightError(
            "runtime data directory does not exist: {}".format(data_dir)
        )
    windows = split_date_windows(start_date, end_date)
    configured = _configured_instruments(tokens)
    if staging_dir.exists():
        raise MigrationPreflightError(
            "migration staging directory already exists; inspect or remove it: {}".format(
                staging_dir
            )
        )
    staging_dir.mkdir(parents=True, exist_ok=False)

    prior_attempts = load_append_attempt_evidence(data_dir)
    seed_processed_from_local(data_dir, staging_dir)

    cex_path = staging_dir / CEX_FILENAME
    dex_path = staging_dir / DEX_FILENAME
    baseline_cex_rows = _read_csv_rows(cex_path)
    baseline_dex_bytes = dex_path.read_bytes()
    accumulated_attempts: List[Dict[str, Any]] = []
    accumulated_ids = set()

    for window_start, window_end in windows:
        window_days = (
            date.fromisoformat(window_end) - date.fromisoformat(window_start)
        ).days + 1
        fetch_cex.main(
            token_symbols=list(configured),
            exchanges=list(TARGET_EXCHANGES),
            append=True,
            start_date=window_start,
            end_date=window_end,
            limit_days=min(MAX_WINDOW_DAYS, window_days + 3),
            output_dir=staging_dir,
            remove_legacy_upbit_krw_fallback=(
                remove_legacy_upbit_krw_fallback
            ),
        )
        if dex_path.read_bytes() != baseline_dex_bytes:
            raise MigrationPreflightError(
                "DEX candidate changed during CEX identity migration"
            )
        window_attempts = _load_hash_bound_attempts(staging_dir)
        _validate_window_attempts(
            window_attempts,
            configured=configured,
            start_date=window_start,
            end_date=window_end,
        )
        for attempt in window_attempts:
            attempt_id = str(attempt.get("attempt_id") or "")
            if not attempt_id or attempt_id in accumulated_ids:
                raise MigrationPreflightError(
                    "window attempt IDs are empty or duplicated"
                )
            accumulated_ids.add(attempt_id)
            accumulated_attempts.append(dict(attempt))

    fetch_cex.write_attempt_ledger(
        staging_dir / CEX_ATTEMPT_FILENAME,
        accumulated_attempts,
        source_csv=cex_path,
        start_date=start_date if len(windows) == 1 else None,
        end_date=end_date if len(windows) == 1 else None,
    )
    merge_append_attempt_evidence(
        processed_dir=staging_dir,
        prior_attempts=prior_attempts,
        collected_market_types=["cex"],
    )
    preflight = production_rebuild_preflight(
        staging_dir=staging_dir,
        baseline_cex_rows=baseline_cex_rows,
        baseline_dex_bytes=baseline_dex_bytes,
        configured=configured,
        start_date=start_date,
        end_date=end_date,
        expected_attempts=accumulated_attempts,
    )

    import_counts = None
    if apply:
        import_counts = import_snapshot(staging_dir, target_dir=data_dir)
    return {
        "schema": "cex_exact_identity_migration/v1",
        "status": "applied" if apply else "dry_run_validated",
        "applied": bool(apply),
        "data_dir": str(data_dir),
        "staging_dir": str(staging_dir),
        "start_date": start_date,
        "end_date": end_date,
        "tokens": list(configured),
        "exchanges": list(TARGET_EXCHANGES),
        "window_count": len(windows),
        "windows": [
            {"start_date": start, "end_date": end}
            for start, end in windows
        ],
        "preflight": preflight,
        "import_counts": import_counts,
    }


def run_migration(
    *,
    data_dir: Path,
    staging_dir: Path,
    start_date: str,
    end_date: str,
    tokens: Optional[Sequence[str]] = None,
    apply: bool = False,
    remove_legacy_upbit_krw_fallback: bool = False,
) -> Dict[str, Any]:
    """Hold the shared collection lock across seed, collection, and import."""

    resolved_data_dir = Path(data_dir).expanduser().resolve()
    if not resolved_data_dir.is_dir():
        raise MigrationPreflightError(
            "runtime data directory does not exist: {}".format(
                resolved_data_dir
            )
        )
    lock_path = resolved_data_dir / "collection" / "collection.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    with lock_path.open("a+", encoding="utf-8") as lock_handle:
        try:
            fcntl.flock(
                lock_handle.fileno(),
                fcntl.LOCK_EX | fcntl.LOCK_NB,
            )
        except BlockingIOError as error:
            raise MigrationPreflightError(
                "collection lock is already held: {}".format(lock_path)
            ) from error
        report = _run_migration_under_lock(
            data_dir=resolved_data_dir,
            staging_dir=staging_dir,
            start_date=start_date,
            end_date=end_date,
            tokens=tokens,
            apply=apply,
            remove_legacy_upbit_krw_fallback=(
                remove_legacy_upbit_krw_fallback
            ),
        )
        report["lock_path"] = str(lock_path)
        return report


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Rebuild Upbit, Coinbase, and Kraken daily history with exact "
            "venue identities; dry-run unless --apply is supplied"
        )
    )
    parser.add_argument("--start", required=True, help="Inclusive UTC date")
    parser.add_argument("--end", required=True, help="Inclusive UTC date")
    parser.add_argument(
        "--tokens",
        help="Comma-separated Tokens; defaults to all config/tokens.csv rows",
    )
    parser.add_argument(
        "--data-dir",
        type=Path,
        default=LOCAL_DIR,
        help="Published runtime snapshot directory",
    )
    parser.add_argument(
        "--staging-dir",
        type=Path,
        help="New, non-existing private staging directory",
    )
    parser.add_argument(
        "--remove-legacy-upbit-krw-fallback",
        action="store_true",
        help=(
            "Opt in to bounded removal of historical TOKEN/KRW fallback rows "
            "after exact TOKEN/USDT attempts"
        ),
    )
    parser.add_argument(
        "--apply",
        action="store_true",
        help="Publish once after all windows and preflight checks pass",
    )
    return parser.parse_args(argv)


def main(argv: Optional[Sequence[str]] = None) -> int:
    args = parse_args(argv)
    data_dir = args.data_dir.expanduser().resolve()
    staging_dir = args.staging_dir
    if staging_dir is None:
        staging_dir = data_dir.parent / (
            ".{}-cex-exact-identity-migration".format(data_dir.name)
        )
    tokens = None
    if args.tokens:
        tokens = [
            item.strip().upper()
            for item in args.tokens.split(",")
            if item.strip()
        ]
    report = run_migration(
        data_dir=data_dir,
        staging_dir=staging_dir,
        start_date=args.start,
        end_date=args.end,
        tokens=tokens,
        apply=args.apply,
        remove_legacy_upbit_krw_fallback=(
            args.remove_legacy_upbit_krw_fallback
        ),
    )
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
