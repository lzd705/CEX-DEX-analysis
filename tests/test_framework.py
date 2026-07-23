import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


class FrameworkStructureTest(unittest.TestCase):
    def test_portable_deployment_files_exist(self):
        required_paths = [
            "Dockerfile",
            ".dockerignore",
            ".gitignore",
            "README.md",
            "scripts/run_dashboard.sh",
        ]

        for relative_path in required_paths:
            with self.subTest(path=relative_path):
                self.assertTrue((PROJECT_ROOT / relative_path).exists())

    def test_container_mounts_runtime_data_instead_of_baking_csvs_into_image(self):
        dockerfile = (PROJECT_ROOT / "Dockerfile").read_text(encoding="utf-8")

        self.assertIn("COPY --chown=dashboard:dashboard dashboard ./dashboard", dockerfile)
        self.assertIn('VOLUME ["/app/data/local"]', dockerfile)
        self.assertNotIn("COPY --chown=dashboard:dashboard data/public", dockerfile)
        self.assertNotIn("data/a_review", dockerfile)
        self.assertNotIn("data/raw", dockerfile)

    def test_framework_is_not_bound_to_render(self):
        self.assertFalse((PROJECT_ROOT / "render.yaml").exists())

    def test_market_monitor_has_no_factor_or_admin_surface(self):
        html = (PROJECT_ROOT / "dashboard/static/index.html").read_text(encoding="utf-8")
        javascript = (PROJECT_ROOT / "dashboard/static/app.js").read_text(encoding="utf-8")

        self.assertIn('id="date-start"', html)
        self.assertIn('id="date-end"', html)
        self.assertIn('data-scope="cex"', html)
        self.assertIn('data-scope="dex"', html)
        self.assertNotIn("factor", (html + javascript).lower())
        self.assertNotIn("admin", (html + javascript).lower())


if __name__ == "__main__":
    unittest.main()
