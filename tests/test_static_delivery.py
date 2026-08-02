import gzip
import http.client
import json
import shutil
import tempfile
import threading
import unittest
from email.utils import formatdate
from http.server import ThreadingHTTPServer
from pathlib import Path

from dashboard import server
from scripts.static_asset_contract import PUBLIC_STATIC_ASSET_SOURCES


class StaticDeliveryPolicyTest(unittest.TestCase):
    def test_accept_encoding_policy_accepts_only_usable_gzip(self):
        # A broken quality parser that treats q=0, malformed quality values, or
        # an explicit gzip exclusion as compression-capable must fail this table.
        cases = (
            ("", False),
            ("identity", False),
            ("gzip", True),
            ("br, gzip;q=0.5", True),
            ("gzip;q=0", False),
            ("*;q=1", True),
            ("*;q=0", False),
            ("gzip;q=0, *;q=1", False),
            ("gzip;q=wat", False),
            ("gzip;q=1.1", False),
        )

        for value, expected in cases:
            with self.subTest(value=value):
                self.assertEqual(server.client_accepts_gzip(value), expected)

    def test_exact_static_version_requires_one_unescaped_current_query_value(self):
        # A version check that accepts extra parameters, duplicate keys, escaped
        # keys, fragments, or protected admin routes must fail this table.
        version = server.static_asset_version()
        cases = (
            (f"/app.js?v={version}", True),
            ("/app.js", False),
            ("/app.js?v=", False),
            ("/app.js?v=wrong", False),
            (f"/app.js?v={version}&v={version}", False),
            (f"/app.js?%76={version}", False),
            (f"/app.js?v={version}&x=1", False),
            (f"/app.js?x=1&v={version}", False),
            (f"/app.js;ignored?v={version}", False),
            (f"/app.js?v={version}#fragment", False),
            (f"/admin.js?v={version}", False),
        )

        for path, expected in cases:
            with self.subTest(path=path):
                self.assertEqual(server.exact_static_version(path), expected)


class StaticRepresentationTest(unittest.TestCase):
    def test_public_representations_are_frozen_and_gzip_round_trips_exactly(self):
        expected_content_types = {
            "actions.css": "text/css; charset=utf-8",
            "actions.js": "text/javascript; charset=utf-8",
            "app.js": "text/javascript; charset=utf-8",
            "navigation.js": "text/javascript; charset=utf-8",
            "styles.css": "text/css; charset=utf-8",
            "vendor/lucide.js": "text/javascript; charset=utf-8",
        }

        for served_name, source_name in PUBLIC_STATIC_ASSET_SOURCES:
            with self.subTest(asset=served_name):
                source_path = server.STATIC_ROOT / source_name
                representation = server.static_representation(f"/{served_name}")
                self.assertIsNotNone(representation)
                assert representation is not None
                self.assertEqual(representation.raw, source_path.read_bytes())
                self.assertEqual(gzip.decompress(representation.gzip), representation.raw)
                self.assertEqual(
                    representation.content_type,
                    expected_content_types[served_name],
                )
                self.assertEqual(
                    representation.last_modified,
                    formatdate(source_path.stat().st_mtime, usegmt=True),
                )

    def test_protected_and_unknown_paths_have_no_public_representation(self):
        for path in (
            "/admin.js",
            "/admin.css",
            "/app.js;ignored",
            "/missing.js",
        ):
            with self.subTest(path=path):
                self.assertIsNone(server.static_representation(path))


class StaticHttpTests(unittest.TestCase):
    IMMUTABLE_CACHE_CONTROL = "public, max-age=31536000, immutable"

    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.bundle = Path(self.temporary_directory.name) / "public"
        self.original_static_root = server.STATIC_ROOT
        self.original_vendor_files = server.VENDOR_FILES
        self.original_representations = server._STATIC_REPRESENTATIONS
        shutil.copytree(self.original_static_root, self.bundle)
        server.STATIC_ROOT = self.bundle
        server.VENDOR_FILES = {
            "/vendor/lucide.js": self.bundle / "vendor/lucide.min.js",
        }
        server._STATIC_REPRESENTATIONS = server._build_static_representations()
        self.app_raw = (self.bundle / "app.js").read_bytes()
        self.app_last_modified = formatdate(
            (self.bundle / "app.js").stat().st_mtime,
            usegmt=True,
        )
        self.http_server = ThreadingHTTPServer(
            ("127.0.0.1", 0), server.MarketMonitorHandler
        )
        self.server_thread = threading.Thread(
            target=self.http_server.serve_forever,
            daemon=True,
        )
        self.server_thread.start()

    def tearDown(self):
        self.http_server.shutdown()
        self.http_server.server_close()
        self.server_thread.join()
        server.STATIC_ROOT = self.original_static_root
        server.VENDOR_FILES = self.original_vendor_files
        server._STATIC_REPRESENTATIONS = self.original_representations
        self.temporary_directory.cleanup()

    def request(self, method, path, accept_encoding=None, extra_headers=None):
        connection = http.client.HTTPConnection(*self.http_server.server_address)
        headers = dict(extra_headers or {})
        if accept_encoding is not None:
            headers["Accept-Encoding"] = accept_encoding
        connection.request(method, path, headers=headers)
        response = connection.getresponse()
        status, response_headers, body = (
            response.status,
            response.headers,
            response.read(),
        )
        connection.close()
        return status, response_headers, body

    def assert_single_cache_control(self, headers, value):
        self.assertEqual(headers.get_all("Cache-Control"), [value])

    def assert_exact_asset_headers(self, headers, body, encoded, *, includes_body=True):
        self.assertEqual(headers.get("Content-Type"), "text/javascript; charset=utf-8")
        self.assertEqual(headers.get("Last-Modified"), self.app_last_modified)
        self.assertEqual(headers.get_all("Vary"), ["Accept-Encoding"])
        self.assert_single_cache_control(headers, self.IMMUTABLE_CACHE_CONTROL)
        if not includes_body:
            return
        self.assertEqual(headers.get("Content-Length"), str(len(body)))
        if encoded:
            self.assertEqual(headers.get_all("Content-Encoding"), ["gzip"])
            self.assertEqual(gzip.decompress(body), self.app_raw)
        else:
            self.assertIsNone(headers.get("Content-Encoding"))
            self.assertEqual(body, self.app_raw)

    def test_exact_version_gzip_get_is_immutable_and_round_trips(self):
        # Removing the exact-version route or gzip selection must fail this
        # boundary assertion rather than merely changing an implementation detail.
        version = server.static_asset_version()
        status, headers, body = self.request(
            "GET", f"/app.js?v={version}", "gzip"
        )

        self.assertEqual(status, 200)
        self.assert_exact_asset_headers(headers, body, encoded=True)

    def test_exact_version_identity_get_serves_raw_immutable_bytes(self):
        version = server.static_asset_version()
        status, headers, body = self.request(
            "GET", f"/app.js?v={version}", "identity"
        )

        self.assertEqual(status, 200)
        self.assert_exact_asset_headers(headers, body, encoded=False)

    def test_exact_version_head_matches_get_for_gzip_and_identity(self):
        version = server.static_asset_version()
        fields = (
            "Content-Type",
            "Content-Length",
            "Content-Encoding",
            "Vary",
            "Last-Modified",
            "Cache-Control",
        )

        for accept_encoding, encoded in (("gzip", True), ("identity", False)):
            with self.subTest(accept_encoding=accept_encoding):
                get_status, get_headers, get_body = self.request(
                    "GET", f"/app.js?v={version}", accept_encoding
                )
                head_status, head_headers, head_body = self.request(
                    "HEAD", f"/app.js?v={version}", accept_encoding
                )

                self.assertEqual((get_status, head_status), (200, 200))
                self.assert_exact_asset_headers(get_headers, get_body, encoded)
                self.assert_exact_asset_headers(
                    head_headers, head_body, encoded, includes_body=False
                )
                self.assertEqual(head_body, b"")
                self.assertEqual(
                    {field: get_headers.get_all(field) for field in fields},
                    {field: head_headers.get_all(field) for field in fields},
                )

    def test_q_zero_does_not_select_gzip(self):
        version = server.static_asset_version()
        status, headers, body = self.request(
            "GET", f"/app.js?v={version}", "gzip;q=0, *;q=1"
        )

        self.assertEqual(status, 200)
        self.assert_exact_asset_headers(headers, body, encoded=False)

    def test_wildcard_q_zero_serves_raw_immutable_bytes(self):
        # Replacing the real quality negotiation with a truthy-header check
        # would incorrectly compress this client-visible response.
        version = server.static_asset_version()
        status, headers, body = self.request(
            "GET", f"/app.js?v={version}", "*;q=0"
        )

        self.assertEqual(status, 200)
        self.assert_exact_asset_headers(headers, body, encoded=False)

    def test_exact_version_conditional_get_is_not_modified(self):
        version = server.static_asset_version()
        _, initial_headers, _ = self.request(
            "GET", f"/app.js?v={version}", "gzip"
        )
        status, headers, body = self.request(
            "GET",
            f"/app.js?v={version}",
            "gzip",
            {"If-Modified-Since": initial_headers["Last-Modified"]},
        )

        self.assertEqual(status, 304)
        self.assertEqual(body, b"")
        self.assertEqual(headers.get("Last-Modified"), initial_headers["Last-Modified"])
        self.assertEqual(headers.get_all("Vary"), ["Accept-Encoding"])
        self.assert_single_cache_control(headers, self.IMMUTABLE_CACHE_CONTROL)

    def test_html_and_spa_shell_are_no_cache(self):
        for path in ("/index.html", "/screener"):
            with self.subTest(path=path):
                status, headers, body = self.request("GET", path)

                self.assertEqual(status, 200)
                self.assertTrue(body)
                self.assertEqual(
                    headers.get("Content-Type"), "text/html; charset=utf-8"
                )
                self.assert_single_cache_control(headers, "no-cache")
                self.assertIsNone(headers.get("Content-Encoding"))

    def test_admin_api_error_has_exactly_one_no_store_cache_policy(self):
        # A direct header from an API helper plus the shared boundary default
        # would produce multiple policies; this route is data-independent.
        status, headers, body = self.request("GET", "/api/admin/session")

        self.assertIn(status, (200, 404))
        self.assertEqual(headers.get("Content-Type"), "application/json; charset=utf-8")
        self.assertIsInstance(json.loads(body), dict)
        self.assert_single_cache_control(headers, "no-store")

    def test_unversioned_wrong_and_duplicate_asset_versions_are_no_cache(self):
        version = server.static_asset_version()
        for path in (
            "/app.js",
            "/app.js?v=wrong",
            f"/app.js?v={version}&v={version}",
        ):
            with self.subTest(path=path):
                status, headers, body = self.request("GET", path, "gzip")

                self.assertEqual(status, 200)
                self.assertTrue(body)
                self.assert_single_cache_control(headers, "no-cache")
                self.assertIsNone(headers.get("Content-Encoding"))

    def test_missing_and_admin_assets_remain_no_cache_and_unavailable(self):
        version = server.static_asset_version()
        for path in ("/missing.js", f"/admin.js?v={version}"):
            with self.subTest(path=path):
                status, headers, body = self.request("GET", path, "gzip")

                self.assertEqual(status, 404)
                self.assertTrue(body)
                self.assert_single_cache_control(headers, "no-cache")
                self.assertIsNone(headers.get("Content-Encoding"))
