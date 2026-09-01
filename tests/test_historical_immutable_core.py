"""Tests for immutable historical-core lineage loading."""

from __future__ import annotations

import json
import os
from pathlib import Path
import unittest
from unittest import mock

class HistoricalImmutableCoreTests(unittest.TestCase):
    def _publish_core(self):
        import scripts.historical_route_publication as publication
        from tests.test_historical_route_publication import (
            HistoricalCorePublicationTests,
        )

        run, finalized, lease, _identity = (
            HistoricalCorePublicationTests._open_real_task7_lease()
        )
        stage = published = None
        try:
            stage = publication.stage_historical_replay_core(
                data_dir=run["fixture"].data_dir,
                config=run["config"],
                publication_lease=lease,
            )
            lease = None
            published = publication.publish_historical_replay_core(
                data_dir=run["fixture"].data_dir,
                staged_core=stage,
            )
            stage = None
            projection = dict(published.identity_projection())
            published.close()
            published = None
            return run, finalized, projection
        except BaseException:
            if published is not None:
                published.close()
            if stage is not None:
                stage.close()
            if lease is not None:
                lease.close()
            HistoricalCorePublicationTests._close_real_task7_run(
                run, finalized
            )
            raise

    @staticmethod
    def _load_immutable(publication, data_dir: Path, projection):
        return publication._load_immutable_historical_replay_core(
            data_dir=data_dir,
            route_cohort_id=projection["core_pointer"]["route_cohort_id"],
            expected_manifest_sha256=projection["core_manifest_sha256"],
            expected_pointer_sha256=projection["core_pointer_sha256"],
        )

    def test_immutable_loader_never_reads_latest_and_survives_latest_advance(self):
        import scripts.historical_route_publication as publication
        from tests.test_historical_route_publication import (
            HistoricalCorePublicationTests,
        )

        run, finalized, projection = self._publish_core()
        context = None
        try:
            data_dir = run["fixture"].data_dir
            with mock.patch.object(
                publication._route_publication,
                "_optional_pointer_snapshot_at",
                side_effect=AssertionError("immutable loader read latest"),
            ):
                context = self._load_immutable(
                    publication, data_dir, projection
                )

            core_root = data_dir / "routes" / "historical" / "core"
            sibling_id = "cohort:" + "f" * 64
            (core_root / "bundles" / sibling_id).mkdir()
            advanced_pointer = {
                "schema": "route_historical_replay_core_pointer/v1",
                "bundle_stage": "route_historical_replay_core/v1",
                "route_cohort_id": sibling_id,
                "manifest_sha256": "e" * 64,
            }
            (core_root / "latest.json").write_bytes(
                json.dumps(
                    advanced_pointer,
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                ).encode("utf-8") + b"\n"
            )

            with mock.patch.object(
                publication._route_publication,
                "_optional_pointer_snapshot_at",
                side_effect=AssertionError("immutable reread read latest"),
            ):
                context.reread_unchanged()
            self.assertEqual(
                dict(context.identity_projection()), projection
            )
        finally:
            if context is not None:
                context.close()
            HistoricalCorePublicationTests._close_real_task7_run(
                run, finalized
            )

    def test_immutable_context_rejects_same_byte_member_rewrite(self):
        import scripts.historical_route_publication as publication
        from tests.test_historical_route_publication import (
            HistoricalCorePublicationTests,
        )

        run, finalized, projection = self._publish_core()
        context = None
        try:
            data_dir = run["fixture"].data_dir
            context = self._load_immutable(
                publication, data_dir, projection
            )
            bundle = (
                data_dir / "routes" / "historical" / "core" / "bundles"
                / projection["core_pointer"]["route_cohort_id"]
            )
            route_legs = bundle / "route_legs.csv"
            original = route_legs.read_bytes()
            route_legs.write_bytes(original)
            self.assertEqual(route_legs.read_bytes(), original)

            with self.assertRaises(
                publication.HistoricalRoutePublicationError
            ):
                context.reread_unchanged()
        finally:
            if context is not None:
                context.close()
            HistoricalCorePublicationTests._close_real_task7_run(
                run, finalized
            )

    def test_immutable_loader_rejects_hardlinked_retained_raw_member(self):
        import scripts.historical_route_publication as publication
        from tests.test_historical_route_publication import (
            HistoricalCorePublicationTests,
        )

        run, finalized, projection = self._publish_core()
        context = None
        try:
            data_dir = run["fixture"].data_dir
            run_root = (
                data_dir / "raw" / "historical-foundry-replay"
                / projection["run_id"][4:]
            )
            result_member = next(run_root.glob("foundry/*/*/result.json"))
            os.link(
                str(result_member),
                str(data_dir / "retained-result-hardlink.json"),
            )
            self.assertEqual(result_member.stat().st_nlink, 2)

            with self.assertRaises(
                publication.HistoricalRoutePublicationError
            ):
                context = self._load_immutable(
                    publication, data_dir, projection
                )
        finally:
            if context is not None:
                context.close()
            HistoricalCorePublicationTests._close_real_task7_run(
                run, finalized
            )

    def test_immutable_context_rejects_same_byte_retained_raw_rewrite(self):
        import scripts.historical_route_publication as publication
        from tests.test_historical_route_publication import (
            HistoricalCorePublicationTests,
        )

        run, finalized, projection = self._publish_core()
        context = None
        try:
            data_dir = run["fixture"].data_dir
            context = self._load_immutable(
                publication, data_dir, projection
            )
            run_root = (
                data_dir / "raw" / "historical-foundry-replay"
                / projection["run_id"][4:]
            )
            result_member = next(run_root.glob("foundry/*/*/result.json"))
            original = result_member.read_bytes()
            before = result_member.stat()
            result_member.write_bytes(original)
            after = result_member.stat()
            self.assertNotEqual(
                (before.st_mtime_ns, before.st_ctime_ns),
                (after.st_mtime_ns, after.st_ctime_ns),
            )

            with self.assertRaises(
                publication.HistoricalRoutePublicationError
            ):
                context.reread_unchanged()
        finally:
            if context is not None:
                context.close()
            HistoricalCorePublicationTests._close_real_task7_run(
                run, finalized
            )

    def test_immutable_context_rejects_renamed_retained_raw_run_root(self):
        import scripts.historical_route_publication as publication
        from tests.test_historical_route_publication import (
            HistoricalCorePublicationTests,
        )

        run, finalized, projection = self._publish_core()
        context = None
        run_root = renamed_root = None
        try:
            data_dir = run["fixture"].data_dir
            context = self._load_immutable(
                publication, data_dir, projection
            )
            run_root = (
                data_dir / "raw" / "historical-foundry-replay"
                / projection["run_id"][4:]
            )
            renamed_root = run_root.with_name(run_root.name + ".renamed")
            os.rename(str(run_root), str(renamed_root))

            with self.assertRaises(
                publication.HistoricalRoutePublicationError
            ):
                context.reread_unchanged()
        finally:
            if (
                run_root is not None and renamed_root is not None
                and renamed_root.exists() and not run_root.exists()
            ):
                os.rename(str(renamed_root), str(run_root))
            if context is not None:
                context.close()
            HistoricalCorePublicationTests._close_real_task7_run(
                run, finalized
            )

    def test_immutable_context_allows_new_sibling_raw_run(self):
        import scripts.historical_route_publication as publication
        from tests.test_historical_route_publication import (
            HistoricalCorePublicationTests,
        )

        run, finalized, projection = self._publish_core()
        context = None
        try:
            data_dir = run["fixture"].data_dir
            context = self._load_immutable(
                publication, data_dir, projection
            )
            raw_root = data_dir / "raw" / "historical-foundry-replay"
            (raw_root / ("f" * 64)).mkdir()

            context.reread_unchanged()
            self.assertEqual(
                dict(context.identity_projection()), projection
            )
        finally:
            if context is not None:
                context.close()
            HistoricalCorePublicationTests._close_real_task7_run(
                run, finalized
            )


if __name__ == "__main__":
    unittest.main()
