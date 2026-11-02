"""Adversarial digest-lineage tests for historical complete bundles."""

from __future__ import annotations

import unittest
from unittest import mock


_SET_DIGEST_DOMAINS = frozenset({
    "historical_foundry_overlay_set/v1",
    "historical_foundry_scenario_set/v1",
})


class HistoricalDigestLineageTests(unittest.TestCase):
    @staticmethod
    def _open_published_core(publication):
        from tests.test_historical_complete_bundle import (
            HistoricalCompleteBundleTests,
        )

        return HistoricalCompleteBundleTests._open_published_core(publication)

    @staticmethod
    def _close_published_core(run, finalized, context):
        from tests.test_historical_complete_bundle import (
            HistoricalCompleteBundleTests,
        )

        HistoricalCompleteBundleTests._close_published_core(
            run, finalized, context
        )

    def test_writer_rejects_replaced_overlay_and_scenario_set_digest_oracle(
        self,
    ):
        import scripts.historical_route_publication as publication

        run, finalized, context = self._open_published_core(publication)
        real_typed_digest = publication._historical_replay._typed_digest

        def replaced_typed_digest(domain, value):
            authentic = real_typed_digest(domain, value)
            if domain not in _SET_DIGEST_DOMAINS:
                return authentic
            forged = "f" * 64
            return forged if forged != authentic else "e" * 64

        try:
            with mock.patch.object(
                publication._historical_replay,
                "_typed_digest",
                side_effect=replaced_typed_digest,
            ):
                with self.assertRaises(
                    publication.HistoricalRoutePublicationError
                ):
                    publication._build_historical_complete_payload(
                        context=context
                    )
        finally:
            self._close_published_core(run, finalized, context)


if __name__ == "__main__":
    unittest.main()
