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

    def test_container_includes_only_public_runtime_data(self):
        dockerfile = (PROJECT_ROOT / "Dockerfile").read_text(encoding="utf-8")

        self.assertIn("COPY --chown=dashboard:dashboard dashboard ./dashboard", dockerfile)
        self.assertIn("COPY --chown=dashboard:dashboard data/public ./data/public", dockerfile)
        self.assertNotIn("data/a_review", dockerfile)
        self.assertNotIn("data/raw", dockerfile)

    def test_framework_is_not_bound_to_render(self):
        self.assertFalse((PROJECT_ROOT / "render.yaml").exists())


if __name__ == "__main__":
    unittest.main()
