import csv
import sqlite3
import tempfile
import unittest
from pathlib import Path

from scripts.market_database import (
    CEX_COLUMNS,
    CEX_FILENAME,
    DATABASE_FILENAME,
    DEX_COLUMNS,
    DEX_FILENAME,
    build_database,
    database_status,
)


def write_rows(path, columns, rows):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=columns, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


class MarketDatabaseTest(unittest.TestCase):
    def create_snapshot(self, directory: Path, *, extra_cex=False):
        cex_rows = [
            {
                "date": "2026-01-01",
                "token_symbol": "BTC",
                "exchange": "binance",
                "cex_symbol": "BTC/USDT",
                "open": "99",
                "high": "101",
                "low": "98",
                "close": "100",
                "base_volume": "10",
                "quote_volume_usd": "1000",
            }
        ]
        if extra_cex:
            cex_rows.append(
                {
                    "date": "2026-01-02",
                    "token_symbol": "BTC",
                    "exchange": "binance",
                    "cex_symbol": "BTC/USDT",
                    "open": "100",
                    "high": "103",
                    "low": "99",
                    "close": "102",
                    "base_volume": "12",
                    "quote_volume_usd": "1200",
                }
            )
        dex_rows = [
            {
                "date": "2026-01-01",
                "token_symbol": "BTC",
                "chain": "eth",
                "dex": "uniswap",
                "pool_address": "0xpool",
                "pool_name": "WBTC / USDC",
                "open": "100",
                "high": "102",
                "low": "99",
                "close": "101",
                "dex_volume_usd": "300",
                "pool_tvl_usd": "",
            }
        ]
        write_rows(directory / CEX_FILENAME, CEX_COLUMNS, cex_rows)
        write_rows(directory / DEX_FILENAME, DEX_COLUMNS, dex_rows)

    def test_builds_indexed_database_with_nulls_and_state(self):
        with tempfile.TemporaryDirectory() as directory_name:
            directory = Path(directory_name)
            self.create_snapshot(directory)
            database = directory / DATABASE_FILENAME

            result = build_database(directory, database)
            status = database_status(database)

            self.assertEqual(result["token_count"], 1)
            self.assertEqual(status["cex_row_count"], 1)
            self.assertEqual(status["dex_row_count"], 1)
            connection = sqlite3.connect(database)
            try:
                self.assertIsNone(
                    connection.execute("SELECT pool_tvl_usd FROM dex_pool_daily").fetchone()[0]
                )
                indexes = {
                    row[1]
                    for row in connection.execute("PRAGMA index_list('cex_market_daily')")
                }
                self.assertIn("idx_cex_token_date", indexes)
            finally:
                connection.close()

    def test_rebuild_preserves_import_history(self):
        with tempfile.TemporaryDirectory() as directory_name:
            directory = Path(directory_name)
            database = directory / DATABASE_FILENAME
            self.create_snapshot(directory)
            first = build_database(directory, database)
            self.create_snapshot(directory, extra_cex=True)

            second = build_database(directory, database)

            self.assertNotEqual(first["snapshot_id"], second["snapshot_id"])
            connection = sqlite3.connect(database)
            try:
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM dataset_snapshots").fetchone()[0], 2)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM import_runs").fetchone()[0], 2)
                self.assertEqual(connection.execute("SELECT COUNT(*) FROM cex_market_daily").fetchone()[0], 2)
            finally:
                connection.close()

    def test_failed_rebuild_keeps_previous_database(self):
        with tempfile.TemporaryDirectory() as directory_name:
            directory = Path(directory_name)
            database = directory / DATABASE_FILENAME
            self.create_snapshot(directory)
            first = build_database(directory, database)
            with (directory / CEX_FILENAME).open("a", encoding="utf-8") as handle:
                handle.write(
                    "2026-01-01,BTC,binance,BTC/USDT,99,101,98,100,10,1000\n"
                )

            with self.assertRaises(sqlite3.IntegrityError):
                build_database(directory, database)

            self.assertEqual(database_status(database)["snapshot_id"], first["snapshot_id"])


if __name__ == "__main__":
    unittest.main()
