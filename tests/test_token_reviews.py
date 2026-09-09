import sqlite3
import tempfile
import unittest
from concurrent.futures import ThreadPoolExecutor
from datetime import date
from pathlib import Path

from dashboard.token_reviews import TokenReviewError, TokenReviewStore
from dashboard.token_review_email import (
    SmtpTokenReviewMailer,
    TokenReviewEmailSettings,
    build_token_review_message,
)


ADDRESS = "0x" + "12" * 20


def candidate(*, chain="ETH", address=ADDRESS.upper().replace("0X", "0x"), symbol="test"):
    return {
        "identity": {
            "chain": chain,
            "contract_address": address,
            "token_symbol": symbol,
            "token_name": "Test Token",
            "decimals": 18,
            "coingecko_id": None,
            "source": "geckoterminal",
            "source_token_id": "eth_%s" % ADDRESS,
        },
        "discovery": {"usable_pool_count": 1, "top_pools": []},
        "capabilities": {"dex_daily": "available"},
    }


class TokenReviewStoreTest(unittest.TestCase):
    def test_create_persists_canonical_identity_digest_notification_and_audit(self):
        with tempfile.TemporaryDirectory() as directory:
            store = TokenReviewStore(Path(directory) / "reviews.sqlite3")

            review, deduplicated = store.create_or_get(
                candidate(),
                requested_history_days=30,
                submitter="public:add_token",
                created_at="2026-09-09T01:02:03+00:00",
            )

            self.assertFalse(deduplicated)
            self.assertEqual(review["chain"], "eth")
            self.assertEqual(review["contract_address"], ADDRESS)
            self.assertEqual(review["token_symbol"], "TEST")
            self.assertEqual(len(review["candidate_sha256"]), 64)
            self.assertEqual(review["status"], "pending_review")
            self.assertEqual(review["notification"]["status"], "pending")
            self.assertEqual(review["revision"], 1)
            self.assertEqual(
                review["audit"],
                [{
                    "event": "created",
                    "at": "2026-09-09T01:02:03+00:00",
                    "actor": "public:add_token",
                }],
            )

    def test_concurrent_identical_creates_return_one_canonical_request(self):
        with tempfile.TemporaryDirectory() as directory:
            store = TokenReviewStore(Path(directory) / "reviews.sqlite3")

            def create():
                return store.create_or_get(
                    candidate(),
                    requested_history_days=30,
                    submitter="public:add_token",
                )

            with ThreadPoolExecutor(max_workers=8) as executor:
                results = list(executor.map(lambda _ignored: create(), range(8)))

            request_ids = {review["request_id"] for review, _duplicate in results}
            self.assertEqual(len(request_ids), 1)
            self.assertEqual(sum(duplicate for _review, duplicate in results), 7)
            self.assertEqual(len(store.list_reviews()), 1)

    def test_malformed_database_fails_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "reviews.sqlite3"
            connection = sqlite3.connect(path)
            connection.execute("CREATE TABLE unrelated (value TEXT)")
            connection.commit()
            connection.close()

            with self.assertRaises(TokenReviewError) as context:
                TokenReviewStore(path)

            self.assertEqual(context.exception.code, "invalid_review_database")

    def test_malformed_persisted_row_fails_closed_when_loaded(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "reviews.sqlite3"
            store = TokenReviewStore(path)
            review, _duplicate = store.create_or_get(
                candidate(), requested_history_days=30, submitter="public:add_token"
            )
            connection = sqlite3.connect(path)
            connection.execute(
                "UPDATE token_reviews SET notification_error_code = ? WHERE request_id = ?",
                ("not a safe error", review["request_id"]),
            )
            connection.commit()
            connection.close()

            with self.assertRaises(TokenReviewError) as context:
                store.get(review["request_id"])

            self.assertEqual(context.exception.code, "invalid_review_database")

    def test_stale_revision_cannot_mutate_review(self):
        with tempfile.TemporaryDirectory() as directory:
            store = TokenReviewStore(Path(directory) / "reviews.sqlite3")
            review, _duplicate = store.create_or_get(
                candidate(),
                requested_history_days=30,
                submitter="public:add_token",
            )

            updated = store.transition(
                review["request_id"],
                "approved",
                expected_revision=1,
                actor="research-admin",
            )
            self.assertEqual(updated["revision"], 2)
            with self.assertRaises(TokenReviewError) as context:
                store.transition(
                    review["request_id"],
                    "rejected",
                    expected_revision=1,
                    actor="research-admin",
                )

            self.assertEqual(context.exception.code, "stale_revision")
            self.assertEqual(store.get(review["request_id"])["status"], "approved")

    def test_daily_count_reads_persisted_requests_in_utc(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "reviews.sqlite3"
            store = TokenReviewStore(path)
            store.create_or_get(
                candidate(),
                requested_history_days=30,
                submitter="public:add_token",
                created_at="2026-09-09T23:59:59+00:00",
            )
            store.create_or_get(
                candidate(address="0x" + "34" * 20),
                requested_history_days=30,
                submitter="public:add_token",
                created_at="2026-09-10T00:00:00+00:00",
            )

            self.assertEqual(store.count_created_on(date(2026, 9, 9)), 1)
            self.assertEqual(store.count_created_on("2026-09-10"), 1)

    def test_notification_attempt_and_completion_are_revision_checked_and_audited(self):
        with tempfile.TemporaryDirectory() as directory:
            store = TokenReviewStore(Path(directory) / "reviews.sqlite3")
            review, _duplicate = store.create_or_get(
                candidate(),
                requested_history_days=30,
                submitter="public:add_token",
            )

            sending = store.claim_notification_attempt(
                review["request_id"], expected_revision=1, actor="system:mailer"
            )
            sent = store.record_notification(
                review["request_id"],
                expected_revision=sending["revision"],
                status="sent",
                actor="system:mailer",
                at="2026-09-09T01:02:03+00:00",
            )

            self.assertEqual(sent["notification"], {
                "status": "sent",
                "attempts": 1,
                "error_code": None,
                "retry_at": None,
                "sent_at": "2026-09-09T01:02:03+00:00",
            })
            self.assertEqual(
                [event["event"] for event in sent["audit"]],
                ["created", "notification_claimed", "notification_sent"],
            )
            with self.assertRaises(TokenReviewError) as context:
                store.record_notification(
                    review["request_id"],
                    expected_revision=sending["revision"],
                    status="sent",
                    actor="system:mailer",
                )
            self.assertEqual(context.exception.code, "stale_revision")


class TokenReviewEmailTest(unittest.TestCase):
    @staticmethod
    def environment(**overrides):
        values = {
            "TOKEN_REVIEW_EMAIL_ENABLED": "true",
            "TOKEN_REVIEWER_EMAIL": "reviewer@example.com",
            "TOKEN_REVIEW_BASE_URL": "https://admin.example/portal/",
            "TOKEN_REVIEW_SMTP_HOST": "smtp.example",
            "TOKEN_REVIEW_SMTP_PORT": "587",
            "TOKEN_REVIEW_SMTP_STARTTLS": "true",
            "TOKEN_REVIEW_SMTP_FROM": "notices@example.com",
        }
        values.update(overrides)
        return values

    @staticmethod
    def review():
        with tempfile.TemporaryDirectory() as directory:
            store = TokenReviewStore(Path(directory) / "reviews.sqlite3")
            value, _duplicate = store.create_or_get(
                candidate(),
                requested_history_days=30,
                submitter="public:add_token",
                created_at="2026-09-09T01:02:03+00:00",
            )
            return value

    def test_enabled_email_requires_complete_strict_server_configuration(self):
        settings = TokenReviewEmailSettings.from_environment(self.environment())
        self.assertTrue(settings.enabled)
        self.assertEqual(settings.reviewer_email, "reviewer@example.com")
        self.assertEqual(settings.smtp_port, 587)

        for name, value in (
            ("TOKEN_REVIEWER_EMAIL", ""),
            ("TOKEN_REVIEW_BASE_URL", "https://admin.example/?private=1"),
            ("TOKEN_REVIEW_SMTP_PORT", "0"),
            ("TOKEN_REVIEW_SMTP_USERNAME", "username"),
        ):
            with self.subTest(name=name), self.assertRaises(TokenReviewError) as context:
                TokenReviewEmailSettings.from_environment(self.environment(**{name: value}))
            self.assertEqual(context.exception.code, "invalid_token_review_email_config")

    def test_message_uses_only_fixed_server_headers_and_plain_text_body(self):
        settings = TokenReviewEmailSettings.from_environment(self.environment())
        review = self.review()

        message = build_token_review_message(settings, review)

        self.assertEqual(message["To"], "reviewer@example.com")
        self.assertEqual(message["From"], "notices@example.com")
        self.assertEqual(message["Subject"], "Token contract review requested")
        self.assertEqual(message.get_content_type(), "text/plain")
        self.assertNotIn("<a ", message.get_content().lower())
        expected_url = "https://admin.example/portal/admin.html#token-review=%s" % review["request_id"]
        self.assertIn(expected_url, message.get_content())
        self.assertIn(ADDRESS, message.get_content())
        self.assertNotIn(ADDRESS, expected_url)

    def test_configuration_rejects_crlf_and_address_cannot_form_url(self):
        for name in (
            "TOKEN_REVIEWER_EMAIL",
            "TOKEN_REVIEW_BASE_URL",
            "TOKEN_REVIEW_SMTP_HOST",
            "TOKEN_REVIEW_SMTP_FROM",
        ):
            with self.subTest(name=name), self.assertRaises(TokenReviewError) as context:
                TokenReviewEmailSettings.from_environment(
                    self.environment(**{name: "safe\r\nBcc: victim@example.com"})
                )
            self.assertEqual(context.exception.code, "invalid_token_review_email_config")

        settings = TokenReviewEmailSettings.from_environment(self.environment())
        review = self.review()
        review["contract_address"] = "0x" + "ab" * 20
        message = build_token_review_message(settings, review)
        urls = [line for line in message.get_content().splitlines() if line.startswith("Review URL: ")]
        self.assertEqual(len(urls), 1)
        self.assertNotIn(review["contract_address"], urls[0])

    def test_mailer_uses_optional_starttls_and_optional_credentials(self):
        class FakeSmtp:
            def __init__(self, host, port, timeout):
                self.host = host
                self.port = port
                self.timeout = timeout
                self.starttls_called = False
                self.login_values = None
                self.message = None

            def __enter__(self):
                return self

            def __exit__(self, _type, _value, _traceback):
                return False

            def starttls(self):
                self.starttls_called = True

            def login(self, username, password):
                self.login_values = (username, password)

            def send_message(self, message):
                self.message = message

        settings = TokenReviewEmailSettings.from_environment(
            self.environment(
                TOKEN_REVIEW_SMTP_USERNAME="mailer",
                TOKEN_REVIEW_SMTP_PASSWORD="secret",
            )
        )
        created = []

        def smtp_factory(*args):
            instance = FakeSmtp(*args)
            created.append(instance)
            return instance

        mailer = SmtpTokenReviewMailer(settings, smtp_factory=smtp_factory)
        mailer.send(self.review())

        self.assertEqual(len(created), 1)
        self.assertTrue(created[0].starttls_called)
        self.assertEqual(created[0].login_values, ("mailer", "secret"))
        self.assertEqual(created[0].message["To"], "reviewer@example.com")


if __name__ == "__main__":
    unittest.main()
