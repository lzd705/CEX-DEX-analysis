import csv
import tempfile
import unittest
from pathlib import Path

from scripts.import_local_snapshot import FILES, import_snapshot


class ImportLocalSnapshotTest(unittest.TestCase):
    def test_import_validates_and_copies_both_fact_files(self):
        with tempfile.TemporaryDirectory() as source_name, tempfile.TemporaryDirectory() as target_name:
            source = Path(source_name)
            target = Path(target_name)
            for filename, columns in FILES.items():
                ordered_columns = sorted(columns)
                row = {column: "value" for column in ordered_columns}
                row["date"] = "2026-01-01"
                row["token_symbol"] = "BTC"
                with (source / filename).open("w", newline="", encoding="utf-8") as handle:
                    writer = csv.DictWriter(handle, fieldnames=ordered_columns, lineterminator="\n")
                    writer.writeheader()
                    writer.writerow(row)

            counts = import_snapshot(source, target)

            self.assertEqual(set(counts), set(FILES))
            self.assertTrue((target / "cex_exchange_volume_daily.csv").exists())
            self.assertTrue((target / "dex_pool_volume_daily.csv").exists())


if __name__ == "__main__":
    unittest.main()
