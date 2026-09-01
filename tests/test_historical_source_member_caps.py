"""Raw member size contracts for historical publication."""

from __future__ import annotations

import hashlib
import unittest


class _ExactSource:
    def __init__(self, payload):
        self.payload = payload
        self.calls = []

    def read_member(self, path, *, expected_sha256, max_bytes):
        self.calls.append((path, expected_sha256, max_bytes))
        return self.payload


class HistoricalSourceMemberCapTests(unittest.TestCase):
    def test_compressed_json_member_may_exceed_plain_json_limit(self):
        import scripts.historical_route_publication as publication

        payload = b"x" * publication._MAX_GZIP_MEMBER_BYTES
        digest = hashlib.sha256(payload).hexdigest()
        path = "scan/prefilter/00000001.json.gz"
        descriptor = {
            "path": path,
            "byte_count": len(payload),
            "sha256": digest,
        }
        source = _ExactSource(payload)

        self.assertEqual(
            publication._read_source_member(
                source, {path: descriptor}, path
            ),
            payload,
        )
        self.assertEqual(source.calls, [(path, digest, len(payload))])

        trace_path = "foundry/1/scenario/trace.json.gz"
        oversized_trace = dict(
            descriptor,
            path=trace_path,
            byte_count=publication._MAX_SCENARIO_TRACE_BYTES + 1,
        )
        with self.assertRaises(
            publication.HistoricalRoutePublicationError
        ):
            publication._read_source_member(
                source, {trace_path: oversized_trace}, trace_path
            )

        plain_path = "foundry/1/scenario/result.json"
        plain = dict(descriptor, path=plain_path)
        with self.assertRaises(
            publication.HistoricalRoutePublicationError
        ):
            publication._read_source_member(
                source, {plain_path: plain}, plain_path
            )


if __name__ == "__main__":
    unittest.main()
