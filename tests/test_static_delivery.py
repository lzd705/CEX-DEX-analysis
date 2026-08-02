import gzip
import unittest
from email.utils import formatdate

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
