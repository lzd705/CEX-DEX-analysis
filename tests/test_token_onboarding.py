import unittest
import urllib.error
from unittest.mock import patch

from dashboard.token_onboarding import (
    MAX_SOURCE_RESPONSE_BYTES,
    TokenOnboardingError,
    build_registry_record,
    request_json,
    resolve_token_candidate,
    resolve_token_identity,
)


ADDRESS = "0x" + "12" * 20
QUOTE_ADDRESS = "0x" + "34" * 20
POOL_ADDRESS = "0x" + "56" * 20
V4_POOL_KEY = "0x" + "ab" * 32


def identity_payload(address=ADDRESS, symbol="TEST"):
    return {
        "data": {
            "id": "eth_%s" % address.lower(),
            "type": "token",
            "attributes": {
                "address": address,
                "name": "Test Token",
                "symbol": symbol,
                "decimals": 18,
                "coingecko_coin_id": None,
            },
        }
    }


def pool_payload(
    *,
    base_token_id=None,
    quote_token_id=None,
    pool_address=POOL_ADDRESS,
):
    return {
        "data": [
            {
                "id": "eth_%s" % pool_address.lower(),
                "type": "pool",
                "attributes": {
                    "address": pool_address,
                    "name": "TEST / USD",
                    "reserve_in_usd": "1000000.5",
                    "volume_usd": {"h24": "250000.25"},
                },
                "relationships": {
                    "base_token": {
                        "data": {
                            "id": base_token_id or "eth_%s" % ADDRESS.lower(),
                        }
                    },
                    "quote_token": {
                        "data": {
                            "id": quote_token_id or "eth_%s" % QUOTE_ADDRESS.lower(),
                        }
                    },
                    "dex": {"data": {"id": "uniswap_v3"}},
                },
            }
        ]
    }


class FakeSource:
    def __init__(self, token=None, pools=None):
        self.token = token if token is not None else identity_payload()
        self.pools = pools if pools is not None else pool_payload()
        self.urls = []

    def __call__(self, url):
        self.urls.append(url)
        return self.pools if url.endswith("/pools") else self.token


class TokenOnboardingTest(unittest.TestCase):
    def test_request_json_rejects_oversized_response_with_bounded_read(self):
        class OversizedResponse:
            requested_size = None

            def __enter__(self):
                return self

            def __exit__(self, _error_type, _error, _traceback):
                return False

            def read(self, size=-1):
                self.requested_size = size
                return b"x" * size

        response = OversizedResponse()
        private_url = "https://source.test/api/v2/token?credential=private"
        with patch(
            "dashboard.token_onboarding.urllib.request.urlopen",
            return_value=response,
        ), self.assertRaises(TokenOnboardingError) as context:
            request_json(private_url)

        self.assertEqual(
            response.requested_size,
            MAX_SOURCE_RESPONSE_BYTES + 1,
        )
        self.assertEqual(context.exception.code, "source_invalid_response")
        self.assertEqual(
            context.exception.message,
            "GeckoTerminal response exceeded the allowed size",
        )
        self.assertTrue(context.exception.retryable)
        self.assertEqual(context.exception.details, {})
        self.assertNotIn(private_url, str(context.exception))

    def test_resolve_candidate_validates_identity_and_pool_without_cex_guess(self):
        source = FakeSource()

        candidate = resolve_token_candidate(
            " ETH ",
            ADDRESS.upper().replace("0X", "0x"),
            fetch_json=source,
            base_url="https://source.test/api/v2",
        )

        self.assertEqual(candidate["identity"]["contract_address"], ADDRESS)
        self.assertEqual(candidate["identity"]["token_symbol"], "TEST")
        pool = candidate["discovery"]["top_pools"][0]
        self.assertEqual(pool["target_side"], "base")
        self.assertEqual(pool["tvl_usd"], 1000000.5)
        self.assertEqual(
            candidate["capabilities"]["cex"],
            "requires_manual_mapping",
        )
        self.assertNotIn("cex_symbol", candidate["identity"])
        self.assertEqual(
            source.urls,
            [
                "https://source.test/api/v2/networks/eth/tokens/%s" % ADDRESS,
                "https://source.test/api/v2/networks/eth/tokens/%s/pools" % ADDRESS,
            ],
        )

    def test_quote_side_is_detected_exactly(self):
        source = FakeSource(
            pools=pool_payload(
                base_token_id="eth_%s" % QUOTE_ADDRESS,
                quote_token_id="eth_%s" % ADDRESS,
            )
        )

        candidate = resolve_token_candidate("eth", ADDRESS, fetch_json=source)

        self.assertEqual(
            candidate["discovery"]["top_pools"][0]["target_side"],
            "quote",
        )

    def test_evm_protocol_native_32_byte_pool_key_is_accepted(self):
        source = FakeSource(pools=pool_payload(pool_address=V4_POOL_KEY))

        candidate = resolve_token_candidate("eth", ADDRESS, fetch_json=source)

        self.assertEqual(
            candidate["discovery"]["top_pools"][0]["pool_address"],
            V4_POOL_KEY,
        )

    def test_pool_source_identity_must_match_attributes(self):
        pools = pool_payload()
        pools["data"][0]["id"] = "eth_%s" % ("0x" + "78" * 20)
        source = FakeSource(pools=pools)

        with self.assertRaises(TokenOnboardingError) as context:
            resolve_token_candidate("eth", ADDRESS, fetch_json=source)

        self.assertEqual(context.exception.code, "source_invalid_response")
        self.assertTrue(context.exception.retryable)

    def test_identity_address_mismatch_is_rejected(self):
        source = FakeSource(token=identity_payload(address=QUOTE_ADDRESS))

        with self.assertRaises(TokenOnboardingError) as context:
            resolve_token_identity("eth", ADDRESS, fetch_json=source)

        self.assertEqual(context.exception.code, "identity_mismatch")
        self.assertFalse(context.exception.retryable)

    def test_pool_without_target_token_is_rejected(self):
        source = FakeSource(
            pools=pool_payload(
                base_token_id="eth_%s" % QUOTE_ADDRESS,
                quote_token_id="eth_%s" % ("0x" + "78" * 20),
            )
        )

        with self.assertRaises(TokenOnboardingError) as context:
            resolve_token_candidate("eth", ADDRESS, fetch_json=source)

        self.assertEqual(context.exception.code, "pool_token_mismatch")
        self.assertFalse(context.exception.retryable)

    def test_empty_pool_set_is_a_non_retryable_explicit_error(self):
        source = FakeSource(pools={"data": []})

        with self.assertRaises(TokenOnboardingError) as context:
            resolve_token_candidate("eth", ADDRESS, fetch_json=source)

        self.assertEqual(context.exception.code, "no_usable_pool")
        self.assertFalse(context.exception.retryable)

    def test_source_rate_limit_is_retryable(self):
        def rate_limited(_url):
            raise urllib.error.HTTPError(
                "https://source.test",
                429,
                "rate limited",
                {},
                None,
            )

        with self.assertRaises(TokenOnboardingError) as context:
            resolve_token_identity("eth", ADDRESS, fetch_json=rate_limited)

        self.assertEqual(context.exception.code, "source_rate_limited")
        self.assertTrue(context.exception.retryable)

    def test_invalid_source_payload_is_not_accepted(self):
        source = FakeSource(token={"data": []})

        with self.assertRaises(TokenOnboardingError) as context:
            resolve_token_identity("eth", ADDRESS, fetch_json=source)

        self.assertEqual(context.exception.code, "source_invalid_response")

    def test_non_object_pool_is_a_typed_source_error(self):
        source = FakeSource(pools={"data": ["not-a-pool"]})

        with self.assertRaises(TokenOnboardingError) as context:
            resolve_token_candidate("eth", ADDRESS, fetch_json=source)

        self.assertEqual(context.exception.code, "source_invalid_response")
        self.assertTrue(context.exception.retryable)

    def test_registry_record_keeps_cex_unmapped(self):
        candidate = resolve_token_candidate(
            "eth",
            ADDRESS,
            fetch_json=FakeSource(),
        )

        record = build_registry_record(
            candidate,
            created_by="research-admin",
            job_id="job-123",
            created_at="2026-07-29T00:00:00+00:00",
        )

        self.assertEqual(record["status"], "pending")
        self.assertEqual(
            record["cex_mapping"],
            {
                "status": "requires_manual_review",
                "cex_symbol": None,
                "exchanges": [],
            },
        )
        self.assertEqual(record["last_job_id"], "job-123")


if __name__ == "__main__":
    unittest.main()
