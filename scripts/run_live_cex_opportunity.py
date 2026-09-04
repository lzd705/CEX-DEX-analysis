#!/usr/bin/env python3
"""Collect and publish the fixed public UNI/USDT CEX research workflow."""

from __future__ import annotations

import argparse
from contextlib import redirect_stderr
from datetime import datetime, timezone
import io
import json
import os
from pathlib import Path
import re
import stat
import sys
from typing import Any, Callable, Dict, Mapping, Optional, Sequence


if __package__ in {None, ""}:  # pragma: no cover - direct script bootstrap
    _PROJECT_ROOT_TEXT = str(Path(__file__).resolve().parents[1])
    if _PROJECT_ROOT_TEXT not in sys.path:
        sys.path.insert(0, _PROJECT_ROOT_TEXT)

try:
    from scripts.collect_route_cohort import collect_route_cohort
    from scripts.fetch_cex_depth import collect_cex_market_observation
    from scripts.live_cex_research import (
        LIVE_CEX_TOKEN_PAIR,
        LIVE_CEX_VENUES,
        build_live_cex_research_universe,
        live_cex_research_generation,
    )
    from scripts.route_opportunity_pipeline import (
        finalize_public_cex_research_opportunities,
    )
    from scripts.route_publication import (
        load_latest_complete_route_bundle,
        publish_route_cohort_bundle,
    )
    from scripts.run_current_opportunity_dashboard import (
        DEFAULT_PORT,
        serve_current_dashboard,
    )
except ModuleNotFoundError:  # pragma: no cover - direct script execution
    from collect_route_cohort import collect_route_cohort  # type: ignore
    from fetch_cex_depth import collect_cex_market_observation  # type: ignore
    from live_cex_research import (  # type: ignore
        LIVE_CEX_TOKEN_PAIR,
        LIVE_CEX_VENUES,
        build_live_cex_research_universe,
        live_cex_research_generation,
    )
    from route_opportunity_pipeline import (  # type: ignore
        finalize_public_cex_research_opportunities,
    )
    from route_publication import (  # type: ignore
        load_latest_complete_route_bundle,
        publish_route_cohort_bundle,
    )
    from run_current_opportunity_dashboard import (  # type: ignore
        DEFAULT_PORT,
        serve_current_dashboard,
    )


_PROJECT_ROOT = Path(__file__).resolve().parents[1]
_REPOSITORY_SCHEDULE = Path("config/cex_public_fee_schedules.csv")
DEFAULT_PUBLIC_FEE_SCHEDULE = _PROJECT_ROOT / _REPOSITORY_SCHEDULE
_EXPECTED_MARKET_IDS = frozenset(
    "cex:{}:{}".format(venue, LIVE_CEX_TOKEN_PAIR)
    for venue in LIVE_CEX_VENUES
)
_COHORT_ID = re.compile(r"cohort:[0-9a-f]{64}\Z", re.ASCII)
_SHA256 = re.compile(r"[0-9a-f]{64}\Z", re.ASCII)
_FAILURE_CODES = frozenset({
    "preflight_failed",
    "collection_failed",
    "publication_failed",
    "reload_failed",
    "serve_failed",
})


class LiveCexOpportunityRefreshError(RuntimeError):
    """Stable public failure category with no source or environment payload."""

    def __init__(self, code: str) -> None:
        if code not in _FAILURE_CODES:
            raise ValueError("live CEX refresh failure code is invalid")
        self.code = code
        super().__init__(code)


def _absolute_data_path(value: str) -> Path:
    path = Path(value)
    if not path.is_absolute():
        raise argparse.ArgumentTypeError("data directory must be absolute")
    return Path(os.path.abspath(os.fspath(path)))


def _bounded_integer(
    value: str,
    *,
    label: str,
    minimum: int,
    maximum: int,
) -> int:
    try:
        parsed = int(value)
    except ValueError as error:
        raise argparse.ArgumentTypeError(
            "{} must be an integer".format(label)
        ) from error
    if not minimum <= parsed <= maximum:
        raise argparse.ArgumentTypeError(
            "{} must be between {} and {}".format(
                label, minimum, maximum
            )
        )
    return parsed


def _deadline_seconds(value: str) -> int:
    return _bounded_integer(
        value,
        label="deadline-seconds",
        minimum=10,
        maximum=60,
    )


def _port(value: str) -> int:
    return _bounded_integer(
        value,
        label="port",
        minimum=1,
        maximum=65535,
    )


def parse_args(argv: Optional[Sequence[str]] = None) -> argparse.Namespace:
    """Parse only local storage, bounded timing, and loopback serving controls."""
    parser = argparse.ArgumentParser(
        description=(
            "Collect Binance and Bybit UNI/USDT public books, publish ten "
            "read-only research scenarios, and optionally serve the normal "
            "Current Opportunity page on loopback"
        )
    )
    parser.add_argument(
        "--data-dir",
        required=True,
        type=_absolute_data_path,
        help="absolute local market-data directory",
    )
    parser.add_argument(
        "--public-fee-schedule",
        type=Path,
        default=DEFAULT_PUBLIC_FEE_SCHEDULE,
        help=(
            "absolute schedule or the repository schedule "
            "config/cex_public_fee_schedules.csv"
        ),
    )
    parser.add_argument(
        "--deadline-seconds",
        type=_deadline_seconds,
        default=60,
        help="bounded collection deadline from 10 through 60 seconds",
    )
    parser.add_argument(
        "--serve",
        action="store_true",
        help="serve the verified result through the loopback-only dashboard",
    )
    parser.add_argument(
        "--port",
        type=_port,
        default=DEFAULT_PORT,
        help="loopback dashboard port",
    )
    return parser.parse_args(argv)


def _canonical_system_alias(path: Path) -> Path:
    """Canonicalize only stable macOS /var and /tmp aliases for lstat checks."""
    text = os.path.abspath(os.fspath(path))
    if sys.platform == "darwin":
        if text == "/var" or text.startswith("/var/"):
            text = "/private" + text
        elif text == "/tmp" or text.startswith("/tmp/"):
            text = "/private" + text
    return Path(text)


def _require_real_directory_chain(path: Path) -> None:
    canonical = _canonical_system_alias(path)
    for component in list(reversed(canonical.parents)) + [canonical]:
        try:
            details = os.lstat(str(component))
        except OSError as error:
            raise ValueError("directory chain is unavailable") from error
        if stat.S_ISLNK(details.st_mode) or not stat.S_ISDIR(details.st_mode):
            raise ValueError("directory chain is not real")


def _prepare_data_dir(path: Path) -> Path:
    requested = Path(path)
    if not requested.is_absolute():
        raise ValueError("data directory must be absolute")
    requested = Path(os.path.abspath(os.fspath(requested)))
    canonical = _canonical_system_alias(requested)
    _require_real_directory_chain(canonical.parent)
    try:
        details = os.lstat(str(canonical))
    except FileNotFoundError:
        try:
            os.mkdir(str(canonical), 0o700)
        except OSError as error:
            raise ValueError("data directory could not be created") from error
    except OSError as error:
        raise ValueError("data directory is unavailable") from error
    _require_real_directory_chain(canonical)
    return requested


def _prepare_schedule_path(path: Path) -> Path:
    requested = Path(path)
    if not requested.is_absolute():
        if requested != _REPOSITORY_SCHEDULE:
            raise ValueError("relative public fee schedule is not repository-tracked")
        requested = DEFAULT_PUBLIC_FEE_SCHEDULE
    requested = Path(os.path.abspath(os.fspath(requested)))
    canonical = _canonical_system_alias(requested)
    _require_real_directory_chain(canonical.parent)
    try:
        details = os.lstat(str(canonical))
    except OSError as error:
        raise ValueError("public fee schedule is unavailable") from error
    if stat.S_ISLNK(details.st_mode) or not stat.S_ISREG(details.st_mode):
        raise ValueError("public fee schedule must be a real regular file")
    return requested


def _collection_is_publishable(
    cohort: Mapping[str, Any],
    *,
    expected_route_ids: Sequence[str],
) -> bool:
    legs = cohort.get("legs")
    timing = cohort.get("route_rows")
    if not isinstance(legs, list) or len(legs) != 2:
        return False
    if not isinstance(timing, list) or len(timing) != 2:
        return False
    if any(not isinstance(row, Mapping) for row in legs + timing):
        return False
    leg_ids = [row.get("market_id") for row in legs]
    route_ids = [row.get("route_id") for row in timing]
    return (
        len(set(leg_ids)) == 2
        and set(leg_ids) == _EXPECTED_MARKET_IDS
        and all(row.get("status") == "observed" for row in legs)
        and len(set(route_ids)) == 2
        and set(route_ids) == set(expected_route_ids)
        and all(row.get("timing_status") == "within_sla" for row in timing)
    )


def collect_and_publish_live_cex_research(
    *,
    data_dir: Path,
    public_fee_schedule_path: Path,
    deadline_seconds: int,
    wall_clock: Callable[[], datetime],
) -> Dict[str, Any]:
    """Collect, publish, and cold-load one fixed public CEX research cohort."""
    root = Path(data_dir)
    schedule_path = Path(public_fee_schedule_path)
    if (
        not root.is_absolute()
        or type(deadline_seconds) is not int
        or not 10 <= deadline_seconds <= 60
        or not callable(wall_clock)
    ):
        raise LiveCexOpportunityRefreshError("preflight_failed")
    try:
        root = _prepare_data_dir(root)
        schedule_path = _prepare_schedule_path(schedule_path)
    except KeyboardInterrupt:
        raise
    except Exception as error:
        raise LiveCexOpportunityRefreshError(
            "preflight_failed"
        ) from error

    try:
        universe = build_live_cex_research_universe()
        generation = live_cex_research_generation()
        if (
            not isinstance(universe, Mapping)
            or universe.get("candidate_source_generation") != generation
        ):
            raise ValueError("fixed generation differs")
        routes = universe.get("routes")
        if not isinstance(routes, list) or len(routes) != 2:
            raise ValueError("fixed route inventory differs")
        expected_route_ids = [route["route_id"] for route in routes]
        cohort = collect_route_cohort(
            universe,
            cex_collector=collect_cex_market_observation,
            deadline_seconds=deadline_seconds,
            max_workers=4,
            cex_workers_per_venue=2,
            source_generation_reader=live_cex_research_generation,
            expected_source_generation=generation,
            raw_root=root / "raw/route-cohort",
            wall_clock=wall_clock,
        )
        if (
            not isinstance(cohort, Mapping)
            or not _collection_is_publishable(
                cohort,
                expected_route_ids=expected_route_ids,
            )
        ):
            raise ValueError("fixed collection is incomplete")
    except KeyboardInterrupt:
        raise
    except Exception as error:
        raise LiveCexOpportunityRefreshError(
            "collection_failed"
        ) from error

    try:
        core_pointer = publish_route_cohort_bundle(
            cohort,
            core_root=root / "routes/core",
        )
        if (
            not isinstance(core_pointer, Mapping)
            or core_pointer.get("route_cohort_id")
            != cohort.get("route_cohort_id")
            or not isinstance(core_pointer.get("route_cohort_id"), str)
            or _COHORT_ID.fullmatch(core_pointer["route_cohort_id"]) is None
            or not isinstance(core_pointer.get("manifest_sha256"), str)
            or _SHA256.fullmatch(core_pointer["manifest_sha256"]) is None
        ):
            raise ValueError("published core identity differs")

        runner_cold_loaded: Dict[str, Any] = {}

        def validate_runner_cold_reload(
            committed_pointer: Mapping[str, Any],
        ) -> None:
            try:
                loaded = load_latest_complete_route_bundle(
                    root / "routes",
                    core_root=root / "routes/core",
                )
                bundle = loaded.get("bundle")
                opportunities = (
                    bundle.get("opportunities")
                    if isinstance(bundle, Mapping) else None
                )
                if (
                    loaded.get("pointer") != committed_pointer
                    or not isinstance(opportunities, list)
                    or len(opportunities) != 10
                    or any(
                        not isinstance(row, Mapping)
                        or row.get("strict_eligible") is not False
                        for row in opportunities
                    )
                ):
                    raise ValueError("cold-loaded complete result differs")
                runner_cold_loaded["pointer"] = dict(committed_pointer)
            except KeyboardInterrupt:
                raise
            except Exception as error:
                raise LiveCexOpportunityRefreshError(
                    "reload_failed"
                ) from error

        pointer = finalize_public_cex_research_opportunities(
            data_dir=root,
            public_fee_schedule_path=schedule_path,
            expected_route_cohort_id=core_pointer["route_cohort_id"],
            expected_core_manifest_sha256=core_pointer["manifest_sha256"],
            _postcommit_validator=validate_runner_cold_reload,
        )
        if (
            not isinstance(pointer, Mapping)
            or pointer.get("route_cohort_id")
            != core_pointer.get("route_cohort_id")
            or not isinstance(pointer.get("manifest_sha256"), str)
            or _SHA256.fullmatch(pointer["manifest_sha256"]) is None
            or runner_cold_loaded.get("pointer") != pointer
        ):
            raise ValueError("published complete identity differs")
        pointer = dict(pointer)
    except KeyboardInterrupt:
        raise
    except LiveCexOpportunityRefreshError:
        raise
    except Exception as error:
        raise LiveCexOpportunityRefreshError(
            "publication_failed"
        ) from error

    return {
        "schema": "live_cex_opportunity_refresh/v1",
        "status": "published",
        "token_pair": LIVE_CEX_TOKEN_PAIR,
        "venues": list(LIVE_CEX_VENUES),
        "route_cohort_id": pointer["route_cohort_id"],
        "manifest_sha256": pointer["manifest_sha256"],
        "opportunity_count": 10,
        "strict_eligible_count": 0,
        "served": False,
    }


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def main(argv: Optional[Sequence[str]] = None) -> int:
    try:
        with redirect_stderr(io.StringIO()):
            arguments = parse_args(argv)
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr, flush=True)
        return 130
    except SystemExit as error:
        if error.code == 0:
            return 0
        print("preflight_failed", file=sys.stderr, flush=True)
        return 1
    except Exception:
        print("preflight_failed", file=sys.stderr, flush=True)
        return 1
    try:
        data_dir = _prepare_data_dir(arguments.data_dir)
        schedule_path = _prepare_schedule_path(
            arguments.public_fee_schedule
        )
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr, flush=True)
        return 130
    except Exception:
        print("preflight_failed", file=sys.stderr, flush=True)
        return 1

    try:
        receipt = collect_and_publish_live_cex_research(
            data_dir=data_dir,
            public_fee_schedule_path=schedule_path,
            deadline_seconds=arguments.deadline_seconds,
            wall_clock=_utc_now,
        )
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr, flush=True)
        return 130
    except LiveCexOpportunityRefreshError as error:
        print(error.code, file=sys.stderr, flush=True)
        return 1
    except Exception:
        print("collection_failed", file=sys.stderr, flush=True)
        return 1

    if arguments.serve:
        receipt = dict(receipt)
        receipt["served"] = True
    print(
        json.dumps(
            receipt,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        ),
        flush=True,
    )
    if not arguments.serve:
        return 0
    try:
        serve_current_dashboard(data_dir=data_dir, port=arguments.port)
    except KeyboardInterrupt:
        print("interrupted", file=sys.stderr, flush=True)
        return 130
    except Exception:
        print("serve_failed", file=sys.stderr, flush=True)
        return 1
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
