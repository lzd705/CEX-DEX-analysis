import shutil
import subprocess
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
STATIC_ROOT = PROJECT_ROOT / "dashboard" / "static"


class PublicActionsFrontendTest(unittest.TestCase):
    def test_public_page_exposes_only_bounded_collection_contracts(self):
        html = (STATIC_ROOT / "actions.html").read_text(encoding="utf-8")
        javascript = (STATIC_ROOT / "actions.js").read_text(encoding="utf-8")
        dashboard = (STATIC_ROOT / "index.html").read_text(encoding="utf-8")

        self.assertIn('href="/actions.html">Data Actions</a>', dashboard)
        self.assertIn("Smart-contract address", html)
        self.assertIn("Audited windows only", html)
        self.assertIn("Add &amp; collect 30D", html)
        self.assertNotIn('type="date"', html)
        self.assertNotIn("/api/admin/", javascript)
        self.assertIn('"/api/actions/tokens/resolve"', javascript)
        self.assertIn('"/api/actions/tokens"', javascript)
        self.assertIn('"/api/actions/quality/retryable"', javascript)
        self.assertIn('"/api/actions/quality/retry"', javascript)
        self.assertIn("/api/actions/jobs/", javascript)
        self.assertIn("expected_token_symbol", javascript)
        self.assertIn("queue_type: window.queue_type", javascript)
        self.assertIn("capabilities.cex", javascript)
        self.assertIn("Existing catalog record", javascript)
        self.assertIn("slice(-10)", javascript)
        self.assertIn("/^[0-9a-f]{32}$/.test(value)", javascript)

    def test_public_page_has_keyboard_and_live_status_contracts(self):
        html = (STATIC_ROOT / "actions.html").read_text(encoding="utf-8")
        styles = (STATIC_ROOT / "actions.css").read_text(encoding="utf-8")

        self.assertIn('role="tooltip"', html)
        self.assertIn('aria-live="polite"', html)
        self.assertIn('tabindex="0"', html)
        self.assertIn("@media (max-width: 700px)", styles)
        self.assertIn(".public-retry-table th", styles)

    def test_public_javascript_parses(self):
        node = shutil.which("node")
        if node is None:
            raise unittest.SkipTest("Node.js is not installed")
        subprocess.run(
            [node, "--check", str(STATIC_ROOT / "actions.js")],
            cwd=PROJECT_ROOT,
            check=True,
            capture_output=True,
            text=True,
        )


if __name__ == "__main__":
    unittest.main()
