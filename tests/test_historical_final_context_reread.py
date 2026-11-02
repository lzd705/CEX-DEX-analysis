"""Regression for final Task-6 build-context currentness."""

from __future__ import annotations

import os
import unittest
from unittest import mock


class HistoricalFinalContextRereadTests(unittest.TestCase):
    @staticmethod
    def _open_published_core(publication):
        from tests.test_historical_complete_bundle import (
            HistoricalCompleteBundleTests,
        )

        return HistoricalCompleteBundleTests._open_published_core(
            publication
        )

    @staticmethod
    def _close_published_core(run, finalized, context):
        from tests.test_historical_complete_bundle import (
            HistoricalCompleteBundleTests,
        )

        HistoricalCompleteBundleTests._close_published_core(
            run, finalized, context
        )

    def test_stage_rejects_same_byte_raw_inode_replacement_after_final_validation(
        self,
    ):
        import scripts.historical_route_publication as publication

        run, finalized, context = self._open_published_core(publication)
        replacement_path = None
        try:
            data_dir = run["fixture"].data_dir
            raw_root = data_dir / "raw" / "historical-foundry-replay"
            run_id = context.identity_projection()["run_id"]
            retained_member = raw_root / run_id[4:] / "policy.json"
            original_bytes = retained_member.read_bytes()
            original_inode = retained_member.stat().st_ino
            real_validate = publication._validate_historical_replay_bundle
            calls = [0]

            def replace_after_final_validation(**kwargs):
                nonlocal replacement_path
                calls[0] += 1
                validated = real_validate(**kwargs)
                if calls[0] == 3:
                    replacement_path = retained_member.with_name(
                        ".policy.same-bytes-replacement"
                    )
                    replacement_path.write_bytes(original_bytes)
                    os.chmod(replacement_path, 0o600)
                    os.replace(replacement_path, retained_member)
                    replacement_path = None
                    self.assertEqual(
                        retained_member.read_bytes(), original_bytes
                    )
                    self.assertNotEqual(
                        retained_member.stat().st_ino, original_inode
                    )
                return validated

            with mock.patch.object(
                publication,
                "_validate_historical_replay_bundle",
                side_effect=replace_after_final_validation,
            ):
                with self.assertRaises(
                    publication.HistoricalRoutePublicationError
                ):
                    publication.stage_historical_replay_bundle(
                        data_dir=data_dir,
                        raw_root=raw_root,
                        context=context,
                    )

            self.assertEqual(calls[0], 3)
            bundles = data_dir / "routes" / "historical" / "bundles"
            self.assertEqual(list(bundles.iterdir()), [])
        finally:
            if replacement_path is not None:
                try:
                    replacement_path.unlink()
                except FileNotFoundError:
                    pass
            self._close_published_core(run, finalized, context)


if __name__ == "__main__":
    unittest.main()
