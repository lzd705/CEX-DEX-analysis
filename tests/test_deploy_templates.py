import importlib.util
import stat
import tempfile
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
RENDERER_PATH = PROJECT_ROOT / "deploy/render_runtime_templates.py"
SPEC = importlib.util.spec_from_file_location("render_runtime_templates", RENDERER_PATH)
assert SPEC is not None and SPEC.loader is not None
renderer = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(renderer)


class DeployTemplateTests(unittest.TestCase):
    def test_renderer_keeps_environment_and_systemd_write_paths_identical(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output_dir = root / "rendered"
            project_root = root / "app/current"
            market_data_dir = root / "runtime/published"
            admin_job_dir = root / "operator/jobs"

            written = renderer.render_templates(
                output_dir=output_dir,
                project_root=project_root,
                service_user="market-monitor",
                service_group="market-monitor",
                market_data_dir=market_data_dir,
                admin_job_dir=admin_job_dir,
            )

            self.assertEqual(len(written), 7)
            environment = (output_dir / "dashboard.env").read_text(encoding="utf-8")
            service = (output_dir / "cex-dex-dashboard.service").read_text(
                encoding="utf-8"
            )
            daily = (output_dir / "cex-dex-daily.service").read_text(
                encoding="utf-8"
            )
            depth = (output_dir / "cex-dex-depth.service").read_text(
                encoding="utf-8"
            )
            daily_user = (
                output_dir / "cex-dex-daily-user.service"
            ).read_text(encoding="utf-8")
            depth_user = (
                output_dir / "cex-dex-depth-user.service"
            ).read_text(encoding="utf-8")
            work_dir = market_data_dir.parent / ".published-processed"
            self.assertIn(f"MARKET_DATA_DIR={market_data_dir}", environment)
            self.assertIn(f"ADMIN_JOB_DIR={admin_job_dir}", environment)
            self.assertIn(f"ReadWritePaths={market_data_dir}", service)
            self.assertIn(f"ReadWritePaths={work_dir}", service)
            self.assertIn(f"ReadWritePaths={admin_job_dir}", service)
            self.assertIn(f"ReadOnlyPaths={project_root}", service)
            for collection in (daily, depth):
                self.assertIn("User=market-monitor", collection)
                self.assertIn("Group=market-monitor", collection)
                self.assertIn(f"--data-dir {market_data_dir}", collection)
                self.assertIn(f"ReadOnlyPaths={project_root}", collection)
                self.assertIn(f"ReadWritePaths={market_data_dir}", collection)
                self.assertIn(f"ReadWritePaths={work_dir}", collection)
                self.assertIn("ProtectSystem=strict", collection)
                self.assertNotRegex(collection, renderer.PLACEHOLDER)
            for collection in (daily_user, depth_user):
                self.assertIn(
                    f"Environment=MARKET_DATA_DIR={market_data_dir}",
                    collection,
                )
                self.assertIn(f"--data-dir {market_data_dir}", collection)
                self.assertNotIn("/etc/cex-dex", collection)
                self.assertNotIn("User=", collection)
                self.assertNotRegex(collection, renderer.PLACEHOLDER)
            self.assertNotRegex(service, renderer.PLACEHOLDER)
            self.assertNotRegex(environment, renderer.PLACEHOLDER)
            self.assertEqual(
                stat.S_IMODE((output_dir / "dashboard.env").stat().st_mode),
                0o600,
            )

    def test_retention_uses_the_rendered_external_market_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            output_dir = root / "rendered"
            market_data_dir = root / "external-data"
            renderer.render_templates(
                output_dir=output_dir,
                project_root=root / "app",
                service_user="collector",
                service_group="collector",
                market_data_dir=market_data_dir,
                admin_job_dir=root / "jobs",
            )
            retention = (
                output_dir / "cex-dex-cex-depth-retention.service"
            ).read_text(encoding="utf-8")
            raw_root = market_data_dir / "raw/cex-depth"
            self.assertIn(f"ConditionPathIsDirectory={raw_root}", retention)
            self.assertIn(f"--root {raw_root}", retention)
            self.assertIn(f"ReadWritePaths={raw_root}", retention)
            self.assertNotIn("/srv/", retention)

    def test_default_checkout_data_uses_the_checkout_processed_directory(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            project_root = root / "app"
            output_dir = root / "rendered"
            market_data_dir = project_root / "data/local"
            renderer.render_templates(
                output_dir=output_dir,
                project_root=project_root,
                service_user="collector",
                service_group="collector",
                market_data_dir=market_data_dir,
                admin_job_dir=market_data_dir / "admin/jobs",
            )

            for filename in (
                "cex-dex-dashboard.service",
                "cex-dex-daily.service",
                "cex-dex-depth.service",
            ):
                service = (output_dir / filename).read_text(encoding="utf-8")
                self.assertIn(
                    f"ReadWritePaths={project_root / 'data/processed'}",
                    service,
                )
                self.assertNotIn(
                    f"ReadWritePaths={project_root / 'data/.local-processed'}",
                    service,
                )

    def test_user_timer_installer_renders_dedicated_units_without_system_config(self):
        script = (
            PROJECT_ROOT / "scripts/install_collection_timers.sh"
        ).read_text(encoding="utf-8")

        self.assertIn("deploy/render_runtime_templates.py", script)
        self.assertIn('market_data_dir=\"${MARKET_DATA_DIR:-', script)
        self.assertIn('$service-user.service', script)
        self.assertNotIn("sed ", script)
        self.assertNotIn("/etc/cex-dex", script)

    def test_path_validation_rejects_unsafe_or_unrenderable_paths(self):
        for value in (
            "/",
            "relative/path",
            "/tmp/path with spaces",
            "/tmp/path#tag",
            "/tmp/path%specifier",
            "/tmp/path@placeholder",
        ):
            with self.subTest(value=value):
                with self.assertRaises(ValueError):
                    renderer.validated_absolute_path(value, "test-path")


if __name__ == "__main__":
    unittest.main()
