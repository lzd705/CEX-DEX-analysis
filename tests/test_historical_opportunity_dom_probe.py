import hashlib
import json
import subprocess
import unittest
from pathlib import Path
from unittest.mock import patch

import scripts.check_dashboard_release as checker


DISCLAIMER = (
    "Historical Foundry Replay. Fixed-block counterfactual simulation under "
    "a hash-bound state override modelling a prefunded, predeployed, "
    "preapproved executor. Successful values are research estimates at the "
    "displayed Ethereum block; they are not current and are not executable "
    "candidates."
)
GENERATION = "1" * 64
REPLAY_ID = "replay:" + "2" * 64
APPLICATION_SHA = "a" * 40
ASSET_SHA = "b" * 64
ASSET_VERSION = APPLICATION_SHA[:12] + "-" + ASSET_SHA[:12]
PROJECT_ROOT = Path(__file__).resolve().parents[1]
STATIC_ROOT = PROJECT_ROOT / "dashboard" / "static"


def api_payload():
    routes = []
    index = 0
    for direction in (
        "sushiswap_to_uniswap", "uniswap_to_sushiswap",
    ):
        for notional in ("1000", "5000", "10000", "50000", "100000"):
            routes.append({
                "opportunity_id": "historical-opportunity-{}".format(index),
                "route_id": "historical-route-{}".format(direction),
                "token_symbol": "UNI",
                "buy_market_id": "dex:ethereum:uniswap_v2:UNI/WETH",
                "sell_market_id": "dex:ethereum:sushiswap_v2:UNI/WETH",
                "direction": direction,
                "requested_notional_usd": notional,
                "selected_block_number": 12345678,
                "selected_block_hash": "0x" + "3" * 64,
                "selected_block_timestamp": "2023-01-01T00:00:00Z",
                "route_mode": (
                    "historical_counterfactual_state_override_next_block"
                ),
                "opportunity_class": "research_estimate",
                "availability": {"status": "available"},
                "foundry_verified": True,
                "gas_used": 123456,
                "policy_net_edge_usd": "1.25",
                "research_net_edge_usd": "1.00",
                "baseline_net_edge_usd": "1.25",
                "stress_25_net_edge_usd": "0.75",
                "stress_50_net_edge_usd": "0.50",
                "stress_robust": True,
                "state_age_seconds": 12,
                "receipt_sha256": format(index + 10, "064x"),
                "trace_sha256": format(index + 30, "064x"),
                "executor_model": "prefunded_predeployed_preapproved",
            })
            index += 1
    return {
        "availability": {"status": "available", "reason": None},
        "metadata": {
            "contract_version": "opportunity_historical_summary/v1",
            "temporal_scope": "historical_replay",
            "execution_claim": (
                "historical_counterfactual_state_override_next_block"
            ),
            "data_generation": GENERATION,
            "replay_id": REPLAY_ID,
            "selected_block_number": 12345678,
            "coverage": {"scenario_count": 10, "returned_count": 10},
        },
        "filters": {
            "token": None,
            "venue": None,
            "notional_usd": None,
            "opportunity_class": "all",
            "route_type": "all",
            "availability": "all",
            "sort": "net_edge_usd",
            "direction": "desc",
        },
        "routes": routes,
    }


def dom_projection():
    payload = api_payload()
    rows = [{
        "opportunity_id": row["opportunity_id"],
        "direction": row["direction"],
        "notional_usd": row["requested_notional_usd"],
        "foundry_verified": row["foundry_verified"],
        "policy_net_edge_usd": row["policy_net_edge_usd"],
        "research_net_edge_usd": row["research_net_edge_usd"],
        "receipt_sha256": row["receipt_sha256"],
        "trace_sha256": row["trace_sha256"],
    } for row in payload["routes"]]
    html_sha = "c" * 64
    return {
        "application_sha": APPLICATION_SHA,
        "asset_sha": ASSET_SHA,
        "html_sha256": html_sha,
        "surface_binding_sha256": checker.historical_surface_binding_sha256(
            application_sha=APPLICATION_SHA,
            asset_sha=ASSET_SHA,
            html_sha256=html_sha,
            api_data_generation=GENERATION,
        ),
        "data_generation": GENERATION,
        "replay_id": REPLAY_ID,
        "selected_block_number": 12345678,
        "scenario_count": 10,
        "strict_hidden": True,
        "visible_value_row_count": 10,
        "disclaimer": DISCLAIMER,
        "rows": rows,
    }


def probe_html():
    return (STATIC_ROOT / "index.html").read_bytes().replace(
        b"__ASSET_VERSION__", ASSET_VERSION.encode("ascii")
    )


def probe_app_js():
    return (STATIC_ROOT / "app.js").read_bytes()


def probe_navigation_js():
    return (STATIC_ROOT / "navigation.js").read_bytes()


class HistoricalOpportunityDomProbeTests(unittest.TestCase):
    class Headers:
        def __init__(self, pairs):
            self.pairs = pairs

        def get_all(self, name):
            return [value for key, value in self.pairs if key == name]

    class Response:
        status = 200

        def __init__(self, body, headers, url):
            self.body = body
            self.headers = HistoricalOpportunityDomProbeTests.Headers(headers)
            self.url = url

        def __enter__(self):
            return self

        def __exit__(self, *_args):
            return False

        def read(self, limit):
            return self.body[:limit]

        def geturl(self):
            return self.url

    def test_static_snapshot_preserves_exact_served_asset_bytes(self):
        bodies = {
            name: ("served-" + name).encode()
            for name in checker.STATIC_ASSET_FILENAMES
        }
        expected = hashlib.sha256()
        for name in checker.STATIC_ASSET_FILENAMES:
            expected.update(name.encode())
            expected.update(b"\0")
            expected.update(bodies[name])
            expected.update(b"\0")

        def open_asset(request, timeout):
            self.assertEqual(timeout, 2.0)
            name = request.full_url.split("/", 3)[-1].split("?", 1)[0]
            body = bodies[name]
            return self.Response(body, [
                ("Content-Length", str(len(body))),
                ("Cache-Control", checker.IMMUTABLE_STATIC_CACHE_CONTROL),
                ("Vary", "Accept-Encoding"),
            ], request.full_url)

        with patch.object(checker, "urlopen", side_effect=open_asset):
            snapshot = checker.fetch_static_asset_snapshot(
                "https://dashboard.test", ASSET_VERSION, timeout=2.0,
            )
        self.assertEqual(snapshot.asset_sha, expected.hexdigest())
        self.assertEqual(snapshot.asset_version, ASSET_VERSION)
        self.assertEqual(dict(snapshot.raw_assets), bodies)
        self.assertEqual(len(snapshot.metrics), len(bodies))

    def test_historical_html_snapshot_hashes_exact_nonredirected_response(self):
        body = (
            '<link rel="stylesheet" href="/styles.css?v={0}">'
            '<script src="/vendor/lucide.js?v={0}"></script>'
            '<script src="/navigation.js?v={0}"></script>'
            '<script src="/app.js?v={0}"></script>'
            '<button data-opportunity-scope="current"></button>'
            '<button data-opportunity-scope="historical"></button>'
            '<section id="historical-opportunity-inventory"></section>'
            '<tbody id="historical-opportunity-body"></tbody>{1}'
        ).format(ASSET_VERSION, DISCLAIMER).encode()
        url = (
            "https://dashboard.test/opportunities?"
            "opportunity_scope=historical"
        )
        response = self.Response(body, [
            ("Content-Length", str(len(body))),
            ("Content-Type", "text/html; charset=utf-8"),
        ], url)
        with patch.object(checker, "urlopen", return_value=response):
            snapshot = checker.fetch_historical_html_snapshot(
                "https://dashboard.test",
                application_sha=APPLICATION_SHA,
                asset_sha=ASSET_SHA,
                asset_version=ASSET_VERSION,
                timeout=2.0,
            )
        self.assertEqual(snapshot.raw_html, body)
        self.assertEqual(snapshot.html_sha256, hashlib.sha256(body).hexdigest())
        self.assertEqual(snapshot.request_path,
                         "/opportunities?opportunity_scope=historical")

    def test_dom_api_parity_accepts_exact_ten_row_bijection(self):
        result = checker.validate_historical_dom_api_parity(
            api_payload=api_payload(),
            dom_result=dom_projection(),
            expected_application_sha=APPLICATION_SHA,
            expected_asset_sha=ASSET_SHA,
            expected_html_sha256="c" * 64,
            expected_data_generation=GENERATION,
        )
        self.assertEqual(result["scenario_count"], 10)
        self.assertEqual(len(result["rows"]), 10)
        self.assertTrue(result["strict_hidden"])
        self.assertEqual(result["disclaimer"], DISCLAIMER)

    def test_dom_probe_uses_actual_html_and_served_javascript_bytes(self):
        html = probe_html()
        result = checker.run_historical_opportunity_dom_probe(
            historical_html=html,
            navigation_js=probe_navigation_js(),
            app_js=probe_app_js(),
            api_payload=api_payload(),
            expected_application_sha=APPLICATION_SHA,
            expected_asset_sha=ASSET_SHA,
            expected_html_sha256=hashlib.sha256(html).hexdigest(),
            expected_data_generation=GENERATION,
            timeout=3.0,
        )
        self.assertEqual(len(result["rows"]), 10)
        self.assertEqual(result["replay_id"], REPLAY_ID)
        self.assertTrue(result["strict_hidden"])
        self.assertEqual(result.get("visible_value_row_count"), 10)

        replaced = html.replace(
            b'id="historical-opportunity-body"',
            b'id="historical-opportunity-body-replaced"',
        )
        with self.assertRaises(checker.ReleaseCheckError):
            checker.run_historical_opportunity_dom_probe(
                historical_html=replaced,
                navigation_js=probe_navigation_js(),
                app_js=probe_app_js(),
                api_payload=api_payload(),
                expected_application_sha=APPLICATION_SHA,
                expected_asset_sha=ASSET_SHA,
                expected_html_sha256=hashlib.sha256(replaced).hexdigest(),
                expected_data_generation=GENERATION,
                timeout=3.0,
            )

        for label, attacked_html in (
            ("commented", b"<!--" + html + b"-->"),
            ("duplicate", html.replace(
                b'<tbody id="historical-opportunity-body"></tbody>',
                b'<tbody id="historical-opportunity-body"></tbody>' * 2,
            )),
            ("wrong_asset", html.replace(
                ASSET_VERSION.encode("ascii"), b"0" * len(ASSET_VERSION), 1,
            )),
        ):
            with self.subTest(label=label), self.assertRaises(
                checker.ReleaseCheckError
            ):
                checker.run_historical_opportunity_dom_probe(
                    historical_html=attacked_html,
                    navigation_js=probe_navigation_js(),
                    app_js=probe_app_js(),
                    api_payload=api_payload(),
                    expected_application_sha=APPLICATION_SHA,
                    expected_asset_sha=ASSET_SHA,
                    expected_html_sha256=hashlib.sha256(attacked_html).hexdigest(),
                    expected_data_generation=GENERATION,
                    timeout=3.0,
                )

        disabled_bootstrap = probe_app_js().replace(
            b'if (typeof document !== "undefined") initialize();',
            b'if (false) initialize();',
        )
        self.assertNotEqual(disabled_bootstrap, probe_app_js())
        with self.assertRaises(checker.ReleaseCheckError):
            checker.run_historical_opportunity_dom_probe(
                historical_html=html,
                navigation_js=probe_navigation_js(),
                app_js=disabled_bootstrap,
                api_payload=api_payload(),
                expected_application_sha=APPLICATION_SHA,
                expected_asset_sha=ASSET_SHA,
                expected_html_sha256=hashlib.sha256(html).hexdigest(),
                expected_data_generation=GENERATION,
                timeout=3.0,
            )

    def test_dom_probe_fails_closed_when_node_is_unavailable(self):
        html = probe_html()
        with patch.object(checker.shutil, "which", return_value=None):
            with self.assertRaisesRegex(checker.ReleaseCheckError, "Node"):
                checker.run_historical_opportunity_dom_probe(
                    historical_html=html,
                    navigation_js=probe_navigation_js(),
                    app_js=probe_app_js(),
                    api_payload=api_payload(),
                    expected_application_sha=APPLICATION_SHA,
                    expected_asset_sha=ASSET_SHA,
                    expected_html_sha256=hashlib.sha256(html).hexdigest(),
                    expected_data_generation=GENERATION,
                    timeout=3.0,
                )

    def test_dom_probe_requires_one_exact_json_frame_and_empty_stderr(self):
        html = probe_html()
        encoded = json.dumps(
            dom_projection(), separators=(",", ":"), sort_keys=True,
        ).encode("utf-8")
        outputs = (
            (b" " + encoded, b""),
            (encoded + b" ", b""),
            (b"\t" + encoded, b""),
            (encoded + b"\n", b""),
            (encoded + encoded, b""),
            (encoded, b"diagnostic"),
        )
        for stdout, stderr in outputs:
            with self.subTest(stdout=stdout[:10], stderr=stderr), patch.object(
                checker.shutil, "which", return_value="/usr/bin/node"
            ), patch.object(
                checker.subprocess,
                "run",
                return_value=subprocess.CompletedProcess(
                    ["node"], 0, stdout=stdout, stderr=stderr,
                ),
            ):
                with self.assertRaises(checker.ReleaseCheckError):
                    checker.run_historical_opportunity_dom_probe(
                        historical_html=html,
                        navigation_js=probe_navigation_js(),
                        app_js=probe_app_js(),
                        api_payload=api_payload(),
                        expected_application_sha=APPLICATION_SHA,
                        expected_asset_sha=ASSET_SHA,
                        expected_html_sha256=hashlib.sha256(html).hexdigest(),
                        expected_data_generation=GENERATION,
                        timeout=3.0,
                    )

    def test_dom_api_parity_rejects_identity_row_and_visibility_mutations(self):
        mutations = []
        for field, value in (
            ("application_sha", "d" * 40),
            ("asset_sha", "e" * 64),
            ("data_generation", "f" * 64),
            ("strict_hidden", False),
            ("visible_value_row_count", 9),
            ("disclaimer", DISCLAIMER + " altered"),
        ):
            mutation = json.loads(json.dumps(dom_projection()))
            mutation[field] = value
            mutations.append((field, mutation))
        missing = json.loads(json.dumps(dom_projection()))
        missing["rows"].pop()
        mutations.append(("missing", missing))
        duplicate = json.loads(json.dumps(dom_projection()))
        duplicate["rows"][-1] = duplicate["rows"][0]
        mutations.append(("duplicate", duplicate))
        changed = json.loads(json.dumps(dom_projection()))
        changed["rows"][0]["trace_sha256"] = "9" * 64
        mutations.append(("trace", changed))
        for label, mutation in mutations:
            with self.subTest(label=label), self.assertRaises(
                checker.ReleaseCheckError
            ):
                checker.validate_historical_dom_api_parity(
                    api_payload=api_payload(),
                    dom_result=mutation,
                    expected_application_sha=APPLICATION_SHA,
                    expected_asset_sha=ASSET_SHA,
                    expected_html_sha256="c" * 64,
                    expected_data_generation=GENERATION,
                )


if __name__ == "__main__":
    unittest.main()
