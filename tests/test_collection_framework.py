import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class RouteCollectionFrameworkTests(unittest.TestCase):
    def test_route_collection_profile_remains_manual_only(self):
        scheduled_units = sorted(
            (PROJECT_ROOT / "deploy/systemd").glob("*.service.in")
        ) + sorted((PROJECT_ROOT / "deploy/systemd").glob("*.timer"))

        self.assertTrue(scheduled_units)
        for path in scheduled_units:
            with self.subTest(unit=path.name):
                self.assertNotIn(
                    "--profile routes",
                    path.read_text(encoding="utf-8"),
                )


if __name__ == "__main__":
    unittest.main()
