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
            "data/schema/001_market_facts.sql",
            "scripts/market_database.py",
            "scripts/run_collection_cycle.py",
            "scripts/run_dashboard.sh",
            "deploy/systemd/cex-dex-daily.timer",
            "deploy/systemd/cex-dex-depth.timer",
        ]

        for relative_path in required_paths:
            with self.subTest(path=relative_path):
                self.assertTrue((PROJECT_ROOT / relative_path).exists())

    def test_container_mounts_runtime_data_instead_of_baking_csvs_into_image(self):
        dockerfile = (PROJECT_ROOT / "Dockerfile").read_text(encoding="utf-8")

        self.assertIn("COPY --chown=dashboard:dashboard dashboard ./dashboard", dockerfile)
        self.assertIn("COPY --chown=dashboard:dashboard scripts ./scripts", dockerfile)
        self.assertIn("COPY --chown=dashboard:dashboard data/schema ./data/schema", dockerfile)
        self.assertIn('VOLUME ["/app/data/local"]', dockerfile)
        self.assertNotIn("COPY --chown=dashboard:dashboard data/public", dockerfile)
        self.assertNotIn("data/a_review", dockerfile)
        self.assertNotIn("data/raw", dockerfile)

    def test_collection_timers_use_coordinated_profiles_and_realistic_timeout(self):
        daily_service = (
            PROJECT_ROOT / "deploy/systemd/cex-dex-daily.service.in"
        ).read_text(encoding="utf-8")
        depth_service = (
            PROJECT_ROOT / "deploy/systemd/cex-dex-depth.service.in"
        ).read_text(encoding="utf-8")

        self.assertIn("--profile daily --publish-local", daily_service)
        self.assertNotIn("--fail-fast", daily_service)
        self.assertIn("TimeoutStartSec=75min", daily_service)
        self.assertIn("--profile depth --publish-local --fail-fast", depth_service)

    def test_framework_is_not_bound_to_render(self):
        self.assertFalse((PROJECT_ROOT / "render.yaml").exists())

    def test_local_runner_defaults_to_loopback(self):
        runner = (PROJECT_ROOT / "scripts/run_dashboard.sh").read_text(encoding="utf-8")

        self.assertIn('host="${HOST:-127.0.0.1}"', runner)

    def test_market_monitor_has_no_factor_or_admin_surface(self):
        html = (PROJECT_ROOT / "dashboard/static/index.html").read_text(encoding="utf-8")
        javascript = (PROJECT_ROOT / "dashboard/static/app.js").read_text(encoding="utf-8")
        styles = (PROJECT_ROOT / "dashboard/static/styles.css").read_text(encoding="utf-8")

        self.assertIn('id="date-start"', html)
        self.assertIn('id="date-end"', html)
        self.assertIn('data-scope="cex"', html)
        self.assertIn('data-scope="dex"', html)
        self.assertIn('id="search-token"', html)
        self.assertIn('id="tvl-source-status"', html)
        self.assertIn('id="depth-source-status"', html)
        self.assertIn('id="daily-source-status"', html)
        self.assertIn("DEFAULT_MARKET_CACHE_KEY", javascript)
        self.assertIn("Cached through", javascript)
        self.assertIn("common comparable end", javascript)
        self.assertIn("TVL snapshot", javascript)
        self.assertIn("CEX depth", javascript)
        self.assertIn('class="price-cell"', javascript)
        self.assertIn("td.price-cell", styles)
        self.assertNotIn("factor", (html + javascript).lower())
        self.assertNotIn("admin", (html + javascript).lower())

    def test_administrator_is_a_separate_server_controlled_page(self):
        admin_html = (PROJECT_ROOT / "dashboard/static/admin.html").read_text(encoding="utf-8")
        admin_javascript = (PROJECT_ROOT / "dashboard/static/admin.js").read_text(encoding="utf-8")
        admin_backend = (PROJECT_ROOT / "dashboard/admin.py").read_text(encoding="utf-8")
        server = (PROJECT_ROOT / "dashboard/server.py").read_text(encoding="utf-8")

        self.assertIn('id="login-form"', admin_html)
        self.assertIn('id="refresh-form"', admin_html)
        self.assertIn("/api/admin/login", admin_javascript)
        self.assertIn("require_admin(csrf=True)", server)
        self.assertIn("ADMIN_LOGIN_REQUIRED", admin_backend)
        self.assertNotIn("ADMIN_PASSWORD_HASH=", admin_html + admin_javascript)


if __name__ == "__main__":
    unittest.main()
