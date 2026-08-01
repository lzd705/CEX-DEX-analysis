import json
import unittest
from unittest.mock import patch


class FakeClock:
    def __init__(self, now=0.0):
        self.now = now
        self.sleeps = []

    def monotonic(self):
        return self.now

    def sleep(self, seconds):
        self.sleeps.append(seconds)
        self.now += seconds


class FakeResponse:
    def __init__(self, payload):
        self.payload = payload

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        return False

    def read(self):
        return self.payload


class CollectionDeadlineTest(unittest.TestCase):
    def test_expired_deadline_uses_one_stable_exception_without_http_request(self):
        from scripts.collection_deadline import (
            CollectionDeadline,
            CollectionDeadlineExceeded,
        )
        from scripts.fetch_cex_depth import request_json
        from scripts.fetch_dex_depth import http_json_rpc

        clock = FakeClock()
        deadline = CollectionDeadline.for_duration(
            0,
            clock=clock.monotonic,
            sleeper=clock.sleep,
        )
        with patch("urllib.request.urlopen") as urlopen:
            with self.assertRaises(CollectionDeadlineExceeded) as cex_error:
                request_json("https://example.test/book", deadline=deadline)
            with self.assertRaises(CollectionDeadlineExceeded) as dex_error:
                http_json_rpc(
                    "https://example.test/rpc",
                    {"jsonrpc": "2.0", "id": 1, "method": "test", "params": []},
                    deadline=deadline,
                )

        urlopen.assert_not_called()
        self.assertEqual(str(cex_error.exception), "collection deadline exceeded")
        self.assertEqual(type(cex_error.exception), type(dex_error.exception))
        self.assertEqual(str(cex_error.exception), str(dex_error.exception))

    def test_request_timeout_never_exceeds_remaining_deadline(self):
        from scripts.collection_deadline import CollectionDeadline
        from scripts.fetch_cex_depth import request_json

        clock = FakeClock()
        deadline = CollectionDeadline.for_duration(
            2.5,
            clock=clock.monotonic,
            sleeper=clock.sleep,
        )
        raw = json.dumps({"ok": True}).encode("utf-8")
        with patch(
            "urllib.request.urlopen",
            return_value=FakeResponse(raw),
        ) as urlopen:
            payload, returned_raw = request_json(
                "https://example.test/book",
                deadline=deadline,
                timeout_seconds=30,
                max_retries=1,
            )

        self.assertEqual(payload, {"ok": True})
        self.assertEqual(returned_raw, raw)
        self.assertEqual(urlopen.call_args.kwargs["timeout"], 2.5)

    def test_retry_sleep_is_clamped_and_exhaustion_raises_stable_exception(self):
        from scripts.collection_deadline import (
            CollectionDeadline,
            CollectionDeadlineExceeded,
        )

        clock = FakeClock(now=10.0)
        deadline = CollectionDeadline.for_duration(
            1.25,
            clock=clock.monotonic,
            sleeper=clock.sleep,
        )
        with self.assertRaisesRegex(
            CollectionDeadlineExceeded,
            "^collection deadline exceeded$",
        ):
            deadline.sleep_before_retry(30)

        self.assertEqual(clock.sleeps, [1.25])
        self.assertEqual(deadline.remaining_seconds(), 0.0)


if __name__ == "__main__":
    unittest.main()
