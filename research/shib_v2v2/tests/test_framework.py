import ast
import io
import subprocess
import sys
import tokenize
import unittest
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]


def parenthesized_multi_context_with_lines(source):
    """Return lines using the Python 3.10+ ``with (cm1, cm2)`` syntax."""
    tokens = list(tokenize.generate_tokens(io.StringIO(source).readline))
    lines = []

    for index, token in enumerate(tokens):
        if token.type != tokenize.NAME or token.string != "with":
            continue

        next_index = index + 1
        while tokens[next_index].type in {
            tokenize.COMMENT,
            tokenize.NL,
            tokenize.NEWLINE,
            tokenize.INDENT,
            tokenize.DEDENT,
        }:
            next_index += 1
        if tokens[next_index].string != "(":
            continue

        brackets = {"(": ")", "[": "]", "{": "}"}
        bracket_stack = []
        has_top_level_comma = False
        for candidate in tokens[next_index:]:
            if candidate.string in brackets:
                bracket_stack.append(brackets[candidate.string])
            elif bracket_stack and candidate.string == bracket_stack[-1]:
                bracket_stack.pop()
                if not bracket_stack:
                    if has_top_level_comma:
                        lines.append(token.start[0])
                    break
            elif candidate.string == "," and len(bracket_stack) == 1:
                has_top_level_comma = True

    return lines


class FrameworkStructureTest(unittest.TestCase):
    def test_release_checker_supports_documented_direct_script_execution(self):
        result = subprocess.run(
            [
                sys.executable,
                str(PROJECT_ROOT / "scripts/check_dashboard_release.py"),
                "--help",
            ],
            cwd=str(PROJECT_ROOT),
            capture_output=True,
            text=True,
            timeout=15,
            check=False,
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("--base-url", result.stdout)

    def test_parenthesized_multi_context_guard_distinguishes_single_context_expressions(self):
        unsupported = "with (first_context, second_context):\n    pass\n"
        supported = (
            "with (path / filename).open() as stream:\n    pass\n"
            "with (resources[first, second]).open() as stream:\n    pass\n"
        )

        self.assertEqual(parenthesized_multi_context_with_lines(unsupported), [1])
        self.assertEqual(parenthesized_multi_context_with_lines(supported), [])

    def test_python_sources_avoid_parenthesized_multi_context_with_syntax(self):
        source_roots = ("dashboard", "deploy", "scripts", "tests")
        violations = []
        for source_root in source_roots:
            for path in (PROJECT_ROOT / source_root).rglob("*.py"):
                for line in parenthesized_multi_context_with_lines(
                    path.read_text(encoding="utf-8")
                ):
                    violations.append(f"{path.relative_to(PROJECT_ROOT)}:{line}")

        self.assertEqual(violations, [])

    def test_all_python_sources_parse_with_python_38_grammar(self):
        source_roots = ("dashboard", "deploy", "scripts", "tests")
        source_paths = sorted(
            path
            for source_root in source_roots
            for path in (PROJECT_ROOT / source_root).rglob("*.py")
        )

        self.assertTrue(source_paths)
        for path in source_paths:
            with self.subTest(path=str(path.relative_to(PROJECT_ROOT))):
                ast.parse(
                    path.read_text(encoding="utf-8"),
                    filename=str(path),
                    feature_version=8,
                )

    def test_portable_deployment_files_exist(self):
        required_paths = [
            "Dockerfile",
            ".dockerignore",
            ".gitignore",
            "README.md",
            "data/schema/001_market_facts.sql",
            "docs/dex-depth-data-contract.md",
            "scripts/fetch_dex_depth.py",
            "scripts/market_database.py",
            "scripts/run_collection_cycle.py",
            "scripts/run_dashboard.sh",
            "deploy/systemd/cex-dex-daily.timer",
            "deploy/systemd/cex-dex-depth.timer",
            "deploy/systemd/cex-dex-daily-user.service.in",
            "deploy/systemd/cex-dex-depth-user.service.in",
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
        self.assertIn("User=@SERVICE_USER@", daily_service)
        self.assertIn("--data-dir @MARKET_DATA_DIR@", daily_service)
        self.assertIn("ReadWritePaths=@MARKET_DATA_DIR@", daily_service)
        self.assertIn("ReadWritePaths=@MARKET_WORK_DIR@", daily_service)
        self.assertIn("--profile depth --publish-local", depth_service)
        self.assertNotIn("--fail-fast", depth_service)
        self.assertIn("TimeoutStartSec=30min", depth_service)
        self.assertIn("User=@SERVICE_USER@", depth_service)
        self.assertIn("--data-dir @MARKET_DATA_DIR@", depth_service)
        self.assertIn("ReadWritePaths=@MARKET_DATA_DIR@", depth_service)
        self.assertIn("ReadWritePaths=@MARKET_WORK_DIR@", depth_service)

    def test_framework_is_not_bound_to_render(self):
        self.assertFalse((PROJECT_ROOT / "render.yaml").exists())

    def test_local_runner_defaults_to_loopback(self):
        runner = (PROJECT_ROOT / "scripts/run_dashboard.sh").read_text(encoding="utf-8")

        self.assertIn('host="${HOST:-127.0.0.1}"', runner)

    def test_production_runbook_requires_python38_import_preflight(self):
        runbook = (
            PROJECT_ROOT / "docs/production-hardening.md"
        ).read_text(encoding="utf-8")

        self.assertIn("supported production baseline is Python 3.8.10", runbook)
        self.assertIn(
            'python3 -c "import dashboard.server; import dashboard.market_facts"',
            runbook,
        )
        self.assertIn("Keep the old process running during this preflight", runbook)
        self.assertEqual(runbook.count("--expected-application-sha"), 2)
        self.assertEqual(runbook.count("--expected-asset-sha"), 2)
        self.assertIn("from dashboard.server import static_asset_sha", runbook)

    def test_opportunity_release_gate_is_bounded_and_has_no_collection_side_effect(self):
        checker = (
            PROJECT_ROOT / "scripts/check_dashboard_release.py"
        ).read_text(encoding="utf-8")
        operations = (
            PROJECT_ROOT / "docs/collection-operations.md"
        ).read_text(encoding="utf-8")
        design = (
            PROJECT_ROOT / "docs/market-monitor-design.md"
        ).read_text(encoding="utf-8")

        self.assertIn("/api/markets/opportunities", checker)
        self.assertIn("--opportunity-raw-max", checker)
        self.assertIn("--opportunity-gzip-max", checker)
        self.assertIn("cold", operations.lower())
        self.assertIn("warm", operations.lower())
        self.assertIn("filter parity", operations.lower())
        self.assertIn("does not start collection", operations.lower())
        self.assertIn("does not install or enable a timer", operations.lower())
        self.assertIn("opportunity API", design)
        self.assertIn("complete_pointer_absent", design)

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
        self.assertIn('id="dex-depth-source-status"', html)
        self.assertIn('id="daily-source-status"', html)
        self.assertIn("DEFAULT_MARKET_CACHE_KEY", javascript)
        self.assertIn("Cached through", javascript)
        self.assertIn("common comparable end", javascript)
        self.assertIn("TVL snapshot", javascript)
        self.assertIn("CEX depth", javascript)
        self.assertIn("DEX depth", javascript)
        self.assertIn('class="token-row screener-token-row"', javascript)
        self.assertIn(".workspace-market-table", styles)
        self.assertNotIn("factor", (html + javascript).lower())
        self.assertNotIn('href="/admin', html.lower())
        self.assertNotIn('data-app-view="admin"', html.lower())
        self.assertNotIn("/api/admin/", javascript.lower())

    def test_administrator_is_a_separate_server_controlled_page(self):
        admin_html = (PROJECT_ROOT / "dashboard/static/admin.html").read_text(encoding="utf-8")
        admin_javascript = (PROJECT_ROOT / "dashboard/static/admin.js").read_text(encoding="utf-8")
        admin_backend = (PROJECT_ROOT / "dashboard/admin.py").read_text(encoding="utf-8")
        server = (PROJECT_ROOT / "dashboard/server.py").read_text(encoding="utf-8")

        self.assertIn('id="login-form"', admin_html)
        self.assertIn('id="refresh-form"', admin_html)
        self.assertIn("/api/admin/login", admin_javascript)
        self.assertIn("/api/admin/quality/manual-review", admin_javascript)
        self.assertIn('id="manual-review-body"', admin_html)
        self.assertIn("Manual primary-source check", admin_javascript)
        self.assertIn("queue_type: window.queue_type", admin_javascript)
        self.assertIn("require_admin(csrf=True)", server)
        self.assertIn("ADMIN_LOGIN_REQUIRED", admin_backend)
        self.assertNotIn("ADMIN_PASSWORD_HASH=", admin_html + admin_javascript)


if __name__ == "__main__":
    unittest.main()
