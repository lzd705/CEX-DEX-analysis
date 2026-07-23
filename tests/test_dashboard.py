import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dashboard import server


class DashboardServerTest(unittest.TestCase):
    def setUp(self):
        server.RUNTIME["public"] = False

    def tearDown(self):
        server.RUNTIME["public"] = False

    def test_parse_value_preserves_missing_values(self):
        self.assertIsNone(server.parse_value(""))
        self.assertIsNone(server.parse_value("nan"))
        self.assertEqual(server.parse_value("12"), 12)
        self.assertEqual(server.parse_value("AAVE"), "AAVE")

    def test_public_mode_uses_curated_ten_token_snapshot(self):
        server.RUNTIME["public"] = True
        with patch.dict(server.os.environ, {}, clear=True):
            payload = server.build_dashboard_payload()
        self.assertEqual(payload["metadata"]["access_mode"], "public_read_only")
        self.assertFalse(payload["metadata"]["synthetic"])
        self.assertEqual(payload["metadata"]["row_count"], 1780)
        self.assertEqual(payload["metadata"]["token_count"], 10)
        self.assertEqual(len(payload["scope_sensitivity"]), 3)
        self.assertEqual(len(payload["factor_results"]), 147)
        self.assertGreaterEqual(len({row["factor_name"] for row in payload["factor_results"]}), 30)

    def test_public_mode_rejects_private_data_and_state_writes(self):
        server.RUNTIME["public"] = True
        with tempfile.TemporaryDirectory() as temporary_directory:
            private_path = Path(temporary_directory) / "private.csv"
            private_path.write_text("date,token_symbol\n2026-01-01,AAVE\n", encoding="utf-8")
            with patch.dict(server.os.environ, {"DASHBOARD_DATA": str(private_path)}):
                with self.assertRaises(PermissionError):
                    server.resolve_panel_path()
        with self.assertRaises(PermissionError):
            server.save_state({})


if __name__ == "__main__":
    unittest.main()
