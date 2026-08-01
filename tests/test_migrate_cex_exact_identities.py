import csv
import fcntl
import importlib
import json
import tempfile
import unittest
from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch

from scripts import fetch_cex
from scripts.fact_quality import sha256_file
from scripts.market_database import CEX_COLUMNS, DEX_COLUMNS


class ExactCexIdentityMigrationTest(unittest.TestCase):
    def _migration(self, *required_names):
        migration = importlib.import_module(
            "scripts.migrate_cex_exact_identities"
        )
        for name in required_names:
            self.assertTrue(
                hasattr(migration, name),
                "migration runner must expose {}".format(name),
            )
        return migration

    @staticmethod
    def _cex_row(day, token, exchange, instrument, close="1.0"):
        return {
            "date": day,
            "token_symbol": token,
            "exchange": exchange,
            "cex_symbol": instrument,
            "open": close,
            "high": close,
            "low": close,
            "close": close,
            "base_volume": "10.0",
            "quote_volume_usd": "10.0",
        }

    @staticmethod
    def _dex_row(day):
        return {
            "date": day,
            "token_symbol": "AAVE",
            "chain": "eth",
            "dex": "uniswap_v3",
            "pool_address": "0xaavepool",
            "pool_name": "AAVE / USDC",
            "open": "1",
            "high": "1",
            "low": "1",
            "close": "1",
            "dex_volume_usd": "10",
            "pool_tvl_usd": "100",
        }

    @staticmethod
    def _write_csv(path, fieldnames, rows):
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("w", newline="", encoding="utf-8") as handle:
            writer = csv.DictWriter(
                handle,
                fieldnames=fieldnames,
                lineterminator="\n",
            )
            writer.writeheader()
            writer.writerows(rows)

    def _runtime_fixture(self, root):
        data_dir = root / "runtime"
        cex_rows = [
            self._cex_row("2026-01-16", "AAVE", "upbit", "AAVE/KRW"),
            self._cex_row("2026-07-31", "AAVE", "upbit", "AAVE/KRW"),
            self._cex_row("2026-01-16", "AAVE", "coinbase", "AAVE/USDT"),
            self._cex_row("2026-07-31", "AAVE", "coinbase", "AAVE/USDT"),
            self._cex_row("2026-01-16", "AAVE", "kraken", "AAVE/USDT"),
            self._cex_row("2026-07-31", "AAVE", "kraken", "AAVE/USDT"),
            self._cex_row("2026-01-16", "AAVE", "binance", "AAVE/USDT"),
            self._cex_row("2026-07-31", "AAVE", "binance", "AAVE/USDT"),
            self._cex_row("2026-01-16", "UNI", "binance", "UNI/USDT"),
            self._cex_row("2026-01-18", "UNI", "binance", "UNI/USDT"),
        ]
        dex_rows = [
            self._dex_row("2026-01-16"),
            self._dex_row("2026-07-31"),
        ]
        self._write_csv(
            data_dir / "cex_exchange_volume_daily.csv",
            CEX_COLUMNS,
            cex_rows,
        )
        self._write_csv(
            data_dir / "dex_pool_volume_daily.csv",
            DEX_COLUMNS,
            dex_rows,
        )
        return data_dir, cex_rows

    def _fake_fetch(
        self,
        *,
        fail_window_number=None,
        no_data_exchange=None,
        mutate_dex=False,
        mutate_non_target_cex=False,
        upbit_quote_asset="USDT",
    ):
        calls = []

        def fake_fetch(**kwargs):
            calls.append(dict(kwargs))
            output_dir = Path(kwargs["output_dir"])
            start_text = kwargs["start_date"]
            end_text = kwargs["end_date"]
            window_number = len(calls)
            existing_rows = fetch_cex.read_exchange_rows(
                output_dir / "cex_exchange_volume_daily.csv"
            )
            new_rows = []
            attempts = []
            start_day = date.fromisoformat(start_text)
            end_day = date.fromisoformat(end_text)
            days = [
                (start_day + timedelta(days=offset)).isoformat()
                for offset in range((end_day - start_day).days + 1)
            ]
            for token in kwargs["token_symbols"]:
                for exchange in kwargs["exchanges"]:
                    instrument = (
                        token + "/USD"
                        if exchange in {"coinbase", "kraken"}
                        else token + "/" + upbit_quote_asset
                        if exchange == "upbit"
                        else token + "/USDT"
                    )
                    if fail_window_number == window_number and exchange == "kraken":
                        attempts.append(
                            fetch_cex.cex_attempt_record(
                                token,
                                exchange,
                                instrument,
                                error=RuntimeError("temporary network failure"),
                                start_date=start_text,
                                end_date=end_text,
                            )
                        )
                        continue
                    if exchange == no_data_exchange:
                        attempts.append(
                            fetch_cex.cex_attempt_record(
                                token,
                                exchange,
                                instrument,
                                rows=[],
                                start_date=start_text,
                                end_date=end_text,
                            )
                        )
                        continue
                    market_rows = []
                    for day_text in days:
                        row = self._cex_row(
                            day_text,
                            token,
                            exchange,
                            instrument,
                        )
                        row["source_instrument"] = instrument
                        market_rows.append(row)
                    new_rows.extend(market_rows)
                    attempts.append(
                        fetch_cex.cex_attempt_record(
                            token,
                            exchange,
                            instrument,
                            rows=market_rows,
                            start_date=start_text,
                            end_date=end_text,
                        )
                    )
            merged = fetch_cex.merge_conclusive_attempt_windows(
                existing_rows,
                new_rows,
                attempts,
                remove_legacy_upbit_krw_fallback=kwargs[
                    "remove_legacy_upbit_krw_fallback"
                ],
            )
            if mutate_non_target_cex:
                for row in merged:
                    if row["exchange"] == "binance":
                        row["close"] = 2.0
                        break
            cex_path = output_dir / "cex_exchange_volume_daily.csv"
            fetch_cex.write_exchange_rows(merged, cex_path)
            fetch_cex.write_attempt_ledger(
                output_dir / "cex_daily_collection_attempts.json",
                attempts,
                source_csv=cex_path,
                start_date=start_text,
                end_date=end_text,
            )
            if mutate_dex:
                dex_path = output_dir / "dex_pool_volume_daily.csv"
                dex_path.write_bytes(dex_path.read_bytes() + b"\n")

        return fake_fetch, calls

    @staticmethod
    def _prior_attempt():
        return fetch_cex.cex_attempt_record(
            "UNI",
            "binance",
            "UNI/USDT",
            rows=[],
            start_date="2026-01-17",
            end_date="2026-01-17",
        )

    def test_197_day_range_splits_into_180_and_17_day_windows(self):
        try:
            migration = importlib.import_module(
                "scripts.migrate_cex_exact_identities"
            )
        except ModuleNotFoundError:
            self.fail("exact CEX identity migration runner is missing")
        self.assertTrue(
            hasattr(migration, "split_date_windows"),
            "migration runner must expose split_date_windows",
        )

        windows = migration.split_date_windows(
            "2026-01-16",
            "2026-07-31",
        )

        self.assertEqual(
            windows,
            [
                ("2026-01-16", "2026-07-14"),
                ("2026-07-15", "2026-07-31"),
            ],
        )

    def test_partial_window_is_not_conclusive_for_exact_identity_migration(self):
        migration = self._migration(
            "MigrationPreflightError",
            "_validate_window_attempts",
        )
        start_date = "2026-07-01"
        end_date = "2026-07-02"
        configured = {"AAVE": "AAVE/USDT"}
        attempts = []
        for exchange in ("upbit", "coinbase", "kraken"):
            instrument = (
                "AAVE/USD"
                if exchange in {"coinbase", "kraken"}
                else "AAVE/USDT"
            )
            observed_dates = (
                [end_date]
                if exchange == "coinbase"
                else [start_date, end_date]
            )
            rows = []
            for day_text in observed_dates:
                row = self._cex_row(
                    day_text,
                    "AAVE",
                    exchange,
                    instrument,
                )
                row["source_instrument"] = instrument
                rows.append(row)
            attempts.append(
                fetch_cex.cex_attempt_record(
                    "AAVE",
                    exchange,
                    instrument,
                    rows=rows,
                    start_date=start_date,
                    end_date=end_date,
                )
            )

        with self.assertRaisesRegex(
            migration.MigrationPreflightError,
            "partial exact-identity window",
        ):
            migration._validate_window_attempts(
                attempts,
                configured=configured,
                start_date=start_date,
                end_date=end_date,
            )

    def test_preflight_preserves_explicitly_configured_upbit_krw_market(self):
        migration = self._migration(
            "load_append_attempt_evidence",
            "run_migration",
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data_dir, _baseline_rows = self._runtime_fixture(root)
            staging_dir = root / "staging"
            fake_fetch, calls = self._fake_fetch(upbit_quote_asset="KRW")

            with patch.object(
                migration,
                "_configured_instruments",
                return_value={"AAVE": "AAVE/KRW"},
            ), patch.object(
                migration,
                "load_append_attempt_evidence",
                return_value={"cex": [], "dex": []},
            ), patch.object(
                migration.fetch_cex,
                "main",
                side_effect=fake_fetch,
            ), patch.object(
                migration,
                "import_snapshot",
            ) as publish:
                try:
                    report = migration.run_migration(
                        data_dir=data_dir,
                        staging_dir=staging_dir,
                        start_date="2026-01-16",
                        end_date="2026-07-31",
                        tokens=["AAVE"],
                        remove_legacy_upbit_krw_fallback=False,
                    )
                except migration.MigrationPreflightError as error:
                    self.fail(
                        "Explicitly configured Upbit KRW was treated as a "
                        "legacy fallback: {}".format(error)
                    )

            self.assertEqual(len(calls), 2)
            self.assertEqual(report["status"], "dry_run_validated")
            self.assertEqual(
                report["preflight"]["legacy_upbit_krw_row_count"],
                0,
            )
            publish.assert_not_called()

    def test_preflight_rejects_krw_fallback_when_upbit_is_configured_usdt(self):
        migration = self._migration(
            "MigrationPreflightError",
            "load_append_attempt_evidence",
            "run_migration",
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data_dir, _baseline_rows = self._runtime_fixture(root)
            staging_dir = root / "staging"
            fake_fetch, calls = self._fake_fetch(upbit_quote_asset="USDT")

            with patch.object(
                migration,
                "load_append_attempt_evidence",
                return_value={"cex": [], "dex": []},
            ), patch.object(
                migration.fetch_cex,
                "main",
                side_effect=fake_fetch,
            ), patch.object(
                migration,
                "import_snapshot",
            ) as publish:
                with self.assertRaisesRegex(
                    migration.MigrationPreflightError,
                    "legacy exact-identity rows remain: upbit_krw=2",
                ):
                    migration.run_migration(
                        data_dir=data_dir,
                        staging_dir=staging_dir,
                        start_date="2026-01-16",
                        end_date="2026-07-31",
                        tokens=["AAVE"],
                        remove_legacy_upbit_krw_fallback=False,
                    )

            self.assertEqual(len(calls), 2)
            publish.assert_not_called()

    def test_apply_uses_one_seed_one_prior_load_and_one_final_import(self):
        migration = self._migration(
            "load_append_attempt_evidence",
            "seed_processed_from_local",
            "import_snapshot",
            "run_migration",
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data_dir, baseline_rows = self._runtime_fixture(root)
            staging_dir = root / "staging"
            prior = self._prior_attempt()
            fake_fetch, calls = self._fake_fetch()
            imported = []

            def record_import(source_dir, *, target_dir):
                imported.append((Path(source_dir), Path(target_dir)))
                return {"cex_exchange_volume_daily.csv": 1}

            with patch.object(
                migration,
                "load_append_attempt_evidence",
                return_value={"cex": [prior], "dex": []},
            ) as load_prior, patch.object(
                migration,
                "seed_processed_from_local",
                wraps=migration.seed_processed_from_local,
            ) as seed, patch.object(
                migration.fetch_cex,
                "main",
                side_effect=fake_fetch,
            ), patch.object(
                migration,
                "import_snapshot",
                side_effect=record_import,
            ):
                report = migration.run_migration(
                    data_dir=data_dir,
                    staging_dir=staging_dir,
                    start_date="2026-01-16",
                    end_date="2026-07-31",
                    tokens=["AAVE"],
                    apply=True,
                    remove_legacy_upbit_krw_fallback=True,
                )

            self.assertEqual(load_prior.call_count, 1)
            self.assertEqual(seed.call_count, 1)
            self.assertEqual(len(calls), 2)
            self.assertEqual(
                [
                    (call["start_date"], call["end_date"])
                    for call in calls
                ],
                [
                    ("2026-01-16", "2026-07-14"),
                    ("2026-07-15", "2026-07-31"),
                ],
            )
            self.assertTrue(
                all(call["append"] for call in calls),
            )
            self.assertTrue(
                all(
                    call["exchanges"] == ["upbit", "coinbase", "kraken"]
                    for call in calls
                )
            )
            self.assertTrue(
                all(
                    call["remove_legacy_upbit_krw_fallback"]
                    for call in calls
                )
            )
            self.assertEqual(
                imported,
                [(staging_dir.resolve(), data_dir.resolve())],
            )
            self.assertTrue(report["applied"])
            self.assertEqual(report["window_count"], 2)

            final_cex_path = staging_dir / "cex_exchange_volume_daily.csv"
            ledger = json.loads(
                (staging_dir / "cex_daily_collection_attempts.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertEqual(
                ledger["source_csv_sha256"],
                sha256_file(final_cex_path),
            )
            self.assertTrue(
                any(
                    item["token_symbol"] == "UNI"
                    and item["exchange"] == "binance"
                    and item["instrument"] == "UNI/USDT"
                    and item["requested_start_date"] == "2026-01-17"
                    and item["requested_end_date"] == "2026-01-17"
                    and item["reason_code"] == "no_candles"
                    for item in ledger["attempts"]
                ),
                "prior gap evidence must survive with its semantic identity",
            )

            with final_cex_path.open(newline="", encoding="utf-8") as handle:
                final_rows = list(csv.DictReader(handle))
            expected_non_target = [
                row for row in baseline_rows if row["exchange"] == "binance"
            ]
            actual_non_target = [
                row for row in final_rows if row["exchange"] == "binance"
            ]
            self.assertEqual(actual_non_target, expected_non_target)
            self.assertEqual(
                (staging_dir / "dex_pool_volume_daily.csv").read_bytes(),
                (data_dir / "dex_pool_volume_daily.csv").read_bytes(),
            )

    def test_second_window_technical_failure_never_imports(self):
        migration = self._migration(
            "MigrationPreflightError",
            "load_append_attempt_evidence",
            "run_migration",
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data_dir, _baseline_rows = self._runtime_fixture(root)
            staging_dir = root / "staging"
            fake_fetch, calls = self._fake_fetch(fail_window_number=2)

            with patch.object(
                migration,
                "load_append_attempt_evidence",
                return_value={"cex": [], "dex": []},
            ), patch.object(
                migration.fetch_cex,
                "main",
                side_effect=fake_fetch,
            ), patch.object(
                migration,
                "import_snapshot",
            ) as publish:
                with self.assertRaisesRegex(
                    migration.MigrationPreflightError,
                    "technical collection outcome",
                ):
                    migration.run_migration(
                        data_dir=data_dir,
                        staging_dir=staging_dir,
                        start_date="2026-01-16",
                        end_date="2026-07-31",
                        tokens=["AAVE"],
                        apply=True,
                        remove_legacy_upbit_krw_fallback=True,
                    )

            self.assertEqual(len(calls), 2)
            publish.assert_not_called()

    def test_no_data_cannot_delete_existing_target_market_history(self):
        migration = self._migration(
            "MigrationPreflightError",
            "load_append_attempt_evidence",
            "run_migration",
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data_dir, _baseline_rows = self._runtime_fixture(root)
            staging_dir = root / "staging"
            fake_fetch, calls = self._fake_fetch(
                no_data_exchange="coinbase",
            )

            with patch.object(
                migration,
                "load_append_attempt_evidence",
                return_value={"cex": [], "dex": []},
            ), patch.object(
                migration.fetch_cex,
                "main",
                side_effect=fake_fetch,
            ), patch.object(
                migration,
                "import_snapshot",
            ) as publish:
                with self.assertRaisesRegex(
                    migration.MigrationPreflightError,
                    "historical target observation would be lost",
                ):
                    migration.run_migration(
                        data_dir=data_dir,
                        staging_dir=staging_dir,
                        start_date="2026-01-16",
                        end_date="2026-07-31",
                        tokens=["AAVE"],
                        apply=True,
                        remove_legacy_upbit_krw_fallback=True,
                    )

            self.assertEqual(len(calls), 2)
            publish.assert_not_called()

    def test_dry_run_is_default_and_never_imports(self):
        migration = self._migration("parse_args", "run_migration")
        arguments = migration.parse_args(
            [
                "--start",
                "2026-01-16",
                "--end",
                "2026-07-31",
            ]
        )
        self.assertFalse(arguments.apply)

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data_dir, _baseline_rows = self._runtime_fixture(root)
            staging_dir = root / "staging"
            fake_fetch, calls = self._fake_fetch()
            with patch.object(
                migration,
                "load_append_attempt_evidence",
                return_value={"cex": [], "dex": []},
            ), patch.object(
                migration.fetch_cex,
                "main",
                side_effect=fake_fetch,
            ), patch.object(
                migration,
                "import_snapshot",
            ) as publish:
                report = migration.run_migration(
                    data_dir=data_dir,
                    staging_dir=staging_dir,
                    start_date="2026-01-16",
                    end_date="2026-07-31",
                    tokens=["AAVE"],
                    remove_legacy_upbit_krw_fallback=True,
                )

            self.assertEqual(len(calls), 2)
            self.assertEqual(report["status"], "dry_run_validated")
            self.assertFalse(report["applied"])
            self.assertIsNone(report["import_counts"])
            publish.assert_not_called()

    def test_dex_mutation_is_rejected_before_import(self):
        migration = self._migration(
            "MigrationPreflightError",
            "load_append_attempt_evidence",
            "run_migration",
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data_dir, _baseline_rows = self._runtime_fixture(root)
            staging_dir = root / "staging"
            fake_fetch, _calls = self._fake_fetch(mutate_dex=True)
            with patch.object(
                migration,
                "load_append_attempt_evidence",
                return_value={"cex": [], "dex": []},
            ), patch.object(
                migration.fetch_cex,
                "main",
                side_effect=fake_fetch,
            ), patch.object(
                migration,
                "import_snapshot",
            ) as publish:
                with self.assertRaisesRegex(
                    migration.MigrationPreflightError,
                    "DEX candidate changed",
                ):
                    migration.run_migration(
                        data_dir=data_dir,
                        staging_dir=staging_dir,
                        start_date="2026-01-16",
                        end_date="2026-07-31",
                        tokens=["AAVE"],
                        apply=True,
                        remove_legacy_upbit_krw_fallback=True,
                    )
            publish.assert_not_called()

    def test_non_target_cex_mutation_is_rejected_before_import(self):
        migration = self._migration(
            "MigrationPreflightError",
            "load_append_attempt_evidence",
            "run_migration",
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data_dir, _baseline_rows = self._runtime_fixture(root)
            staging_dir = root / "staging"
            fake_fetch, _calls = self._fake_fetch(mutate_non_target_cex=True)
            with patch.object(
                migration,
                "load_append_attempt_evidence",
                return_value={"cex": [], "dex": []},
            ), patch.object(
                migration.fetch_cex,
                "main",
                side_effect=fake_fetch,
            ), patch.object(
                migration,
                "import_snapshot",
            ) as publish:
                with self.assertRaisesRegex(
                    migration.MigrationPreflightError,
                    "non-target CEX facts changed",
                ):
                    migration.run_migration(
                        data_dir=data_dir,
                        staging_dir=staging_dir,
                        start_date="2026-01-16",
                        end_date="2026-07-31",
                        tokens=["AAVE"],
                        apply=True,
                        remove_legacy_upbit_krw_fallback=True,
                    )
            publish.assert_not_called()

    def test_existing_collection_lock_blocks_before_staging_or_collection(self):
        migration = self._migration(
            "MigrationPreflightError",
            "run_migration",
        )
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data_dir, _baseline_rows = self._runtime_fixture(root)
            staging_dir = root / "staging"
            lock_path = data_dir / "collection" / "collection.lock"
            lock_path.parent.mkdir(parents=True, exist_ok=True)

            with lock_path.open("a+", encoding="utf-8") as lock_handle:
                fcntl.flock(
                    lock_handle.fileno(),
                    fcntl.LOCK_EX | fcntl.LOCK_NB,
                )
                with patch.object(
                    migration.fetch_cex,
                    "main",
                ) as collect, patch.object(
                    migration,
                    "import_snapshot",
                ) as publish:
                    with self.assertRaisesRegex(
                        migration.MigrationPreflightError,
                        "collection lock is already held",
                    ):
                        migration.run_migration(
                            data_dir=data_dir,
                            staging_dir=staging_dir,
                            start_date="2026-01-16",
                            end_date="2026-07-31",
                            tokens=["AAVE"],
                            apply=True,
                            remove_legacy_upbit_krw_fallback=True,
                        )

            self.assertFalse(staging_dir.exists())
            collect.assert_not_called()
            publish.assert_not_called()


if __name__ == "__main__":
    unittest.main()
