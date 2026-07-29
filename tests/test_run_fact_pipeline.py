import csv
import json
import sqlite3
import tempfile
import unittest
from datetime import date
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from scripts import run_fact_pipeline
from scripts.fact_quality import build_report
from scripts.import_local_snapshot import import_snapshot


def cex_row(day, token="AAVE"):
    return {
        "date": day,
        "token_symbol": token,
        "exchange": "binance",
        "cex_symbol": "{}/USDT".format(token),
        "open": "100",
        "high": "105",
        "low": "95",
        "close": "102",
        "base_volume": "10",
        "quote_volume_usd": "1020",
    }


def dex_row(day, token="AAVE"):
    return {
        "date": day,
        "token_symbol": token,
        "chain": "eth",
        "dex": "uniswap_v3",
        "pool_address": "0x{}pool".format(token.lower()),
        "pool_name": "{} / WETH".format(token),
        "open": "100",
        "high": "105",
        "low": "95",
        "close": "101",
        "dex_volume_usd": "500",
        "pool_tvl_usd": "1000000",
    }


def write_csv(path, columns, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def cex_attempt(
    token,
    start,
    end,
    *,
    reason="not_listed",
    finished_at="2026-07-04T00:00:00+00:00",
):
    status = "no_data" if reason == "no_candles" else "failed"
    outcome = "no_candles" if reason == "no_candles" else "request_failed"
    error = (
        "The source returned no daily candles inside the requested window."
        if reason == "no_candles"
        else "The source reported that the requested market was unavailable."
    )
    return {
        "attempt_id": "cex-{}-{}".format(token.lower(), reason),
        "market_type": "cex",
        "token_symbol": token,
        "exchange": "binance",
        "instrument": "{}/USDT".format(token),
        "chain": None,
        "dex": None,
        "pool_address": None,
        "requested_start_date": start,
        "requested_end_date": end,
        "observed_dates": [],
        "observed_day_count": 0,
        "status": status,
        "outcome": outcome,
        "reason_code": reason,
        "http_status": 404 if reason == "not_listed" else None,
        "error": error,
        "finished_at_utc": finished_at,
    }


def dex_attempt(
    token,
    start,
    end,
    *,
    reason="no_candles",
    finished_at="2026-07-04T00:00:00+00:00",
):
    if reason == "no_candles":
        status = "no_data"
        outcome = "no_candles"
        error = "The source returned no daily candles inside the requested window."
    else:
        status = "failed"
        outcome = "request_failed"
        error = "The source rejected the request because its rate limit was reached."
    return {
        "attempt_id": "dex-{}-{}".format(token.lower(), reason),
        "market_type": "dex",
        "token_symbol": token,
        "exchange": None,
        "instrument": None,
        "chain": "eth",
        "dex": "uniswap_v3",
        "pool_address": "0x{}pool".format(token.lower()),
        "requested_start_date": start,
        "requested_end_date": end,
        "observed_dates": [],
        "observed_day_count": 0,
        "status": status,
        "outcome": outcome,
        "reason_code": reason,
        "http_status": 429 if reason == "rate_limit" else None,
        "error": error,
        "finished_at_utc": finished_at,
    }


class RunFactPipelineTest(unittest.TestCase):
    def publish_gap_fixture(
        self,
        root,
        *,
        cex_attempts=None,
        dex_attempts=None,
    ):
        source = root / "source"
        target = root / "runtime"
        source.mkdir(parents=True)
        write_csv(
            source / run_fact_pipeline.SOURCE_FILES["cex"],
            run_fact_pipeline.CEX_COLUMNS,
            [
                cex_row("2026-07-01", "AAVE"),
                cex_row("2026-07-02", "AAVE"),
                cex_row("2026-07-03", "AAVE"),
                cex_row("2026-07-01", "UNI"),
                cex_row("2026-07-03", "UNI"),
            ],
        )
        write_csv(
            source / run_fact_pipeline.SOURCE_FILES["dex"],
            run_fact_pipeline.DEX_COLUMNS,
            [
                dex_row("2026-07-01", "AAVE"),
                dex_row("2026-07-02", "AAVE"),
                dex_row("2026-07-03", "AAVE"),
                dex_row("2026-07-01", "UNI"),
                dex_row("2026-07-03", "UNI"),
            ],
        )
        run_fact_pipeline.fetch_cex.write_attempt_ledger(
            source / "cex_daily_collection_attempts.json",
            cex_attempts or [cex_attempt("UNI", "2026-07-02", "2026-07-02")],
            source_csv=source / run_fact_pipeline.SOURCE_FILES["cex"],
            start_date="2026-07-02",
            end_date="2026-07-02",
        )
        run_fact_pipeline.fetch_dex.write_attempt_ledger(
            source / "dex_daily_collection_attempts.json",
            dex_attempts or [dex_attempt("UNI", "2026-07-02", "2026-07-02")],
            source_csv=source / run_fact_pipeline.SOURCE_FILES["dex"],
            start_date="2026-07-02",
            end_date="2026-07-02",
        )
        import_snapshot(
            source,
            target_dir=target,
            quality_today=date(2026, 7, 4),
        )
        return source, target

    def test_pipeline_removes_stale_attempt_evidence_before_collecting(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data_dir = root / "runtime"
            processed_dir = root / "staging"
            processed_dir.mkdir()
            for filename in run_fact_pipeline.ATTEMPT_FILES:
                (processed_dir / filename).write_text(
                    "stale-attempt-evidence\n",
                    encoding="utf-8",
                )
            arguments = SimpleNamespace(
                cex_only=True,
                dex_only=False,
                tokens="xyz",
                exchanges=None,
                append=False,
                start="2026-07-28",
                end="2026-07-28",
                publish_local=False,
                data_dir=data_dir,
                processed_dir=processed_dir,
            )

            with patch.object(
                run_fact_pipeline,
                "parse_args",
                return_value=arguments,
            ):
                with patch.object(
                    run_fact_pipeline.fetch_cex,
                    "main",
                ) as fetch_cex:
                    run_fact_pipeline.main()

            fetch_cex.assert_called_once()
            self.assertTrue(
                all(
                    not (processed_dir / filename).exists()
                    for filename in run_fact_pipeline.ATTEMPT_FILES
                )
            )

    def test_custom_data_dir_routes_collection_and_publication_together(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data_dir = root / "runtime"
            processed_dir = root / "staging"
            arguments = SimpleNamespace(
                cex_only=False,
                dex_only=False,
                tokens="xyz",
                exchanges=None,
                append=False,
                start="2026-07-28",
                end="2026-07-28",
                publish_local=True,
                data_dir=data_dir,
                processed_dir=processed_dir,
            )
            with patch.object(
                run_fact_pipeline,
                "parse_args",
                return_value=arguments,
            ):
                with patch.object(
                    run_fact_pipeline.fetch_cex,
                    "main",
                ) as fetch_cex:
                    with patch.object(
                        run_fact_pipeline.fetch_dex,
                        "main",
                    ) as fetch_dex:
                        with patch.object(
                            run_fact_pipeline,
                            "import_snapshot",
                        ) as publish:
                            run_fact_pipeline.main()

            fetch_cex.assert_called_once_with(
                token_symbols=["XYZ"],
                exchanges=None,
                append=False,
                start_date="2026-07-28",
                end_date="2026-07-28",
                limit_days=4,
                output_dir=processed_dir.resolve(),
            )
            fetch_dex.assert_called_once_with(
                token_symbols=["XYZ"],
                append=False,
                start_date="2026-07-28",
                end_date="2026-07-28",
                limit_days=4,
                output_dir=processed_dir.resolve(),
                local_dir=data_dir.resolve(),
            )
            publish.assert_called_once_with(
                processed_dir.resolve(),
                target_dir=data_dir.resolve(),
            )

    def test_append_seed_uses_the_selected_runtime_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            local_dir = root / "runtime"
            processed_dir = root / "staging"
            local_dir.mkdir()
            for filename in run_fact_pipeline.DETAILED_FILES:
                (local_dir / filename).write_text(
                    f"fixture for {filename}\n",
                    encoding="utf-8",
                )
            run_fact_pipeline.seed_processed_from_local(
                local_dir,
                processed_dir,
            )

            for filename in run_fact_pipeline.DETAILED_FILES:
                self.assertEqual(
                    (processed_dir / filename).read_text(encoding="utf-8"),
                    f"fixture for {filename}\n",
                )

    def test_append_seed_prefers_sqlite_commit_over_flat_csv_candidates(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            local_dir = root / "runtime"
            processed_dir = root / "staging"
            local_dir.mkdir()
            (local_dir / "cex_exchange_volume_daily.csv").write_text(
                "candidate-not-committed\n",
                encoding="utf-8",
            )
            (local_dir / "dex_pool_volume_daily.csv").write_text(
                "candidate-not-committed\n",
                encoding="utf-8",
            )
            database_path = local_dir / run_fact_pipeline.DATABASE_FILENAME
            connection = sqlite3.connect(database_path)
            connection.execute(
                """
                CREATE TABLE cex_market_daily (
                    date TEXT, token_symbol TEXT, exchange TEXT, cex_symbol TEXT,
                    open REAL, high REAL, low REAL, close REAL,
                    base_volume REAL, quote_volume_usd REAL
                )
                """
            )
            connection.execute(
                """
                CREATE TABLE dex_pool_daily (
                    date TEXT, token_symbol TEXT, chain TEXT, dex TEXT,
                    pool_address TEXT, pool_name TEXT,
                    open REAL, high REAL, low REAL, close REAL,
                    dex_volume_usd REAL, pool_tvl_usd REAL
                )
                """
            )
            connection.execute(
                """
                INSERT INTO cex_market_daily
                VALUES ('2026-07-28', 'AAVE', 'binance', 'AAVE/USDT',
                        1, 2, 0.5, 1.5, 10, 15)
                """
            )
            connection.execute(
                """
                INSERT INTO dex_pool_daily
                VALUES ('2026-07-28', 'AAVE', 'eth', 'uniswap_v3',
                        '0xpool', 'AAVE / USDC',
                        1, 2, 0.5, 1.5, 20, 100)
                """
            )
            connection.commit()
            connection.close()

            run_fact_pipeline.seed_processed_from_local(
                local_dir,
                processed_dir,
            )

            with (
                processed_dir / "cex_exchange_volume_daily.csv"
            ).open(newline="", encoding="utf-8") as handle:
                cex_rows = list(csv.DictReader(handle))
            with (
                processed_dir / "dex_pool_volume_daily.csv"
            ).open(newline="", encoding="utf-8") as handle:
                dex_rows = list(csv.DictReader(handle))
            self.assertEqual(cex_rows[0]["token_symbol"], "AAVE")
            self.assertEqual(dex_rows[0]["pool_address"], "0xpool")

    def test_custom_processed_default_is_isolated_beside_runtime(self):
        data_dir = Path("/tmp/market-runtime")
        self.assertEqual(
            run_fact_pipeline.resolve_processed_dir(data_dir, None),
            Path("/tmp/.market-runtime-processed").resolve(),
        )

    def test_append_preserves_other_token_no_candles_and_not_listed_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _source, data_dir = self.publish_gap_fixture(root)
            processed_dir = root / "processed"
            prior = run_fact_pipeline.load_append_attempt_evidence(data_dir)
            run_fact_pipeline.seed_processed_from_local(data_dir, processed_dir)
            run_fact_pipeline.fetch_cex.write_attempt_ledger(
                processed_dir / "cex_daily_collection_attempts.json",
                [
                    run_fact_pipeline.fetch_cex.cex_attempt_record(
                        "AAVE",
                        "binance",
                        "AAVE/USDT",
                        rows=[cex_row("2026-07-02", "AAVE")],
                        start_date="2026-07-02",
                        end_date="2026-07-02",
                    )
                ],
                source_csv=processed_dir
                / run_fact_pipeline.SOURCE_FILES["cex"],
                start_date="2026-07-02",
                end_date="2026-07-02",
            )
            run_fact_pipeline.fetch_dex.write_attempt_ledger(
                processed_dir / "dex_daily_collection_attempts.json",
                [
                    run_fact_pipeline.fetch_dex.dex_attempt_record(
                        "AAVE",
                        "eth",
                        "uniswap_v3",
                        "0xaavepool",
                        rows=[dex_row("2026-07-02", "AAVE")],
                        start_date="2026-07-02",
                        end_date="2026-07-02",
                    )
                ],
                source_csv=processed_dir
                / run_fact_pipeline.SOURCE_FILES["dex"],
                start_date="2026-07-02",
                end_date="2026-07-02",
            )

            run_fact_pipeline.merge_append_attempt_evidence(
                processed_dir=processed_dir,
                prior_attempts=prior,
                collected_market_types=["cex", "dex"],
            )

            report = build_report(
                processed_dir / run_fact_pipeline.SOURCE_FILES["cex"],
                processed_dir / run_fact_pipeline.SOURCE_FILES["dex"],
                cex_attempts=processed_dir / "cex_daily_collection_attempts.json",
                dex_attempts=processed_dir / "dex_daily_collection_attempts.json",
                today=date(2026, 7, 4),
            )
            uni_gaps = {
                (
                    issue["market"]["market_type"],
                    issue["reason_code"],
                    issue["status"],
                )
                for issue in report["issues"]
                if issue["market"]["token_symbol"] == "UNI"
                and issue["date"] == "2026-07-02"
            }
            self.assertEqual(
                uni_gaps,
                {
                    ("cex", "not_listed", "needs_review"),
                    ("dex", "no_candles", "source_no_observation"),
                },
            )

    def test_overlapping_new_attempt_replaces_only_the_intersecting_old_window(self):
        old_attempt = cex_attempt(
            "UNI",
            "2026-07-01",
            "2026-07-05",
            reason="no_candles",
        )
        old_attempt["observed_dates"] = ["2026-07-01", "2026-07-05"]
        old_attempt["observed_day_count"] = 2
        new_attempt = cex_attempt(
            "UNI",
            "2026-07-03",
            "2026-07-03",
            reason="not_listed",
            finished_at="2026-07-05T00:00:00+00:00",
        )
        gap_keys = {
            (("cex", "UNI", "binance"), "2026-07-02"),
            (("cex", "UNI", "binance"), "2026-07-03"),
            (("cex", "UNI", "binance"), "2026-07-04"),
        }

        carried = run_fact_pipeline._carried_segments(
            [old_attempt],
            [new_attempt],
            gap_keys=gap_keys,
        )

        self.assertEqual(
            [
                (
                    item["requested_start_date"],
                    item["requested_end_date"],
                    item["observed_dates"],
                    item["observed_day_count"],
                )
                for item in carried
            ],
            [
                ("2026-07-01", "2026-07-02", ["2026-07-01"], 1),
                ("2026-07-04", "2026-07-05", ["2026-07-05"], 1),
            ],
        )
        self.assertTrue(all(len(item["attempt_id"]) == 20 for item in carried))
        self.assertEqual(len({item["attempt_id"] for item in carried}), 2)

    def test_append_quality_lineage_or_hash_mismatch_fails_closed(self):
        for mismatch in ("import_run_id", "source_hash", "invalid_attempt"):
            with self.subTest(mismatch=mismatch), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                _source, data_dir = self.publish_gap_fixture(root)
                report_path = data_dir / run_fact_pipeline.QUALITY_REPORT_PATH
                report = json.loads(report_path.read_text(encoding="utf-8"))
                if mismatch == "import_run_id":
                    report["publication"]["import_run_id"] = "wrong-run"
                elif mismatch == "source_hash":
                    report["sources"][0]["sha256"] = "0" * 64
                else:
                    report["collection_attempts"][0]["error"] = (
                        "unsafe path /Users/example/private"
                    )
                report_path.write_text(
                    json.dumps(report) + "\n",
                    encoding="utf-8",
                )

                with self.assertRaises(run_fact_pipeline.CarryForwardEvidenceError):
                    run_fact_pipeline.load_append_attempt_evidence(data_dir)

    def test_append_invalid_quality_stops_before_collection_or_publication(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            _source, data_dir = self.publish_gap_fixture(root)
            (data_dir / run_fact_pipeline.QUALITY_REPORT_PATH).write_text(
                "{not-json\n",
                encoding="utf-8",
            )
            arguments = SimpleNamespace(
                cex_only=False,
                dex_only=False,
                tokens="aave",
                exchanges=None,
                append=True,
                start="2026-07-02",
                end="2026-07-02",
                publish_local=True,
                data_dir=data_dir,
                processed_dir=root / "processed",
            )
            with patch.object(
                run_fact_pipeline,
                "parse_args",
                return_value=arguments,
            ):
                with patch.object(
                    run_fact_pipeline.fetch_cex,
                    "main",
                ) as fetch_cex:
                    with patch.object(
                        run_fact_pipeline.fetch_dex,
                        "main",
                    ) as fetch_dex:
                        with patch.object(
                            run_fact_pipeline,
                            "import_snapshot",
                        ) as publish:
                            with self.assertRaises(
                                run_fact_pipeline.CarryForwardEvidenceError
                            ):
                                run_fact_pipeline.main()

            fetch_cex.assert_not_called()
            fetch_dex.assert_not_called()
            publish.assert_not_called()


if __name__ == "__main__":
    unittest.main()
