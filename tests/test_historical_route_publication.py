"""Tests for the isolated historical replay private-core publication path."""

from __future__ import annotations

import importlib
import inspect
import json
import pickle
import gc
from pathlib import Path
import shutil
import unittest
import weakref
from unittest import mock


class HistoricalCorePublicationInterfaceTests(unittest.TestCase):
    def _publication_module(self):
        try:
            return importlib.import_module("scripts.historical_route_publication")
        except ModuleNotFoundError as error:
            self.fail(
                "scripts.historical_route_publication is not implemented"
            )

    def test_stage_signature_accepts_only_data_config_and_one_shot_lease(self):
        """Catch a caller-controlled projection, root, profile, or pointer seam."""
        publication = self._publication_module()

        self.assertEqual(
            inspect.signature(publication.stage_historical_replay_core),
            inspect.Signature(
                parameters=(
                    inspect.Parameter(
                        "data_dir",
                        inspect.Parameter.KEYWORD_ONLY,
                        annotation=Path,
                    ),
                    inspect.Parameter(
                        "config",
                        inspect.Parameter.KEYWORD_ONLY,
                        annotation=publication.HistoricalFoundryConfigSet,
                    ),
                    inspect.Parameter(
                        "publication_lease",
                        inspect.Parameter.KEYWORD_ONLY,
                        annotation=object,
                    ),
                ),
                return_annotation=publication._StagedHistoricalReplayCore,
            ),
        )

    def test_staged_loader_accepts_only_the_issued_stage_handle(self):
        """Catch caller injection of a staged path or prospective pointer bytes."""
        publication = self._publication_module()

        self.assertEqual(
            tuple(inspect.signature(
                publication.load_validated_historical_replay_core_at
            ).parameters),
            ("staged_core",),
        )

    def test_publish_and_latest_have_no_live_default_or_raw_root(self):
        """Catch fallback to the live root or a caller-selected raw reader."""
        publication = self._publication_module()

        publish = inspect.signature(publication.publish_historical_replay_core)
        latest = inspect.signature(publication.load_latest_historical_replay_core)
        self.assertEqual(tuple(publish.parameters), ("data_dir", "staged_core"))
        self.assertEqual(tuple(latest.parameters), ("data_dir",))
        self.assertTrue(all(
            parameter.default is inspect.Parameter.empty
            for parameter in (*publish.parameters.values(), *latest.parameters.values())
        ))

    def test_unissued_stage_and_context_are_redacted_and_nonserializable(self):
        """Catch capability data disclosure or serialization-based forgery."""
        publication = self._publication_module()
        stage = object.__new__(publication._StagedHistoricalReplayCore)
        context = object.__new__(publication.HistoricalReplayBuildContext)

        self.assertEqual(repr(stage), "_StagedHistoricalReplayCore(<redacted>)")
        self.assertEqual(repr(context), "HistoricalReplayBuildContext(<redacted>)")
        for value in (stage, context):
            with self.subTest(type=type(value).__name__):
                with self.assertRaises(TypeError):
                    pickle.dumps(value)

    def test_unissued_stage_and_context_cannot_cross_validation_boundary(self):
        """Catch object.__new__ objects being mistaken for issued capabilities."""
        publication = self._publication_module()
        stage = object.__new__(publication._StagedHistoricalReplayCore)
        context = object.__new__(publication.HistoricalReplayBuildContext)

        with self.assertRaises(publication.HistoricalRoutePublicationError):
            publication.load_validated_historical_replay_core_at(
                staged_core=stage
            )
        with self.assertRaises(publication.HistoricalRoutePublicationError):
            publication._require_historical_replay_build_context(
                context=context
            )
        self.assertTrue(
            hasattr(stage, "close")
            and hasattr(context, "identity_projection")
            and hasattr(context, "reread_unchanged")
            and hasattr(context, "close"),
            "sentinel lifecycle methods are not implemented",
        )
        with self.assertRaises(publication.HistoricalRoutePublicationError):
            stage.close()
        with self.assertRaises(publication.HistoricalRoutePublicationError):
            context.identity_projection()
        with self.assertRaises(publication.HistoricalRoutePublicationError):
            context.reread_unchanged()
        with self.assertRaises(publication.HistoricalRoutePublicationError):
            context.close()


class HistoricalCorePublicationTask7SeamTests(unittest.TestCase):
    def test_task7_one_shot_publication_authority_seam_exists(self):
        """Catch publication code landing before the real Task7 authority seam."""
        import scripts.historical_foundry_storage as storage

        required = (
            "_HistoricalRunPublicationLease",
            "_HistoricalRunPublicationSource",
            "_consume_historical_run_publication_lease",
            "_validate_historical_run_publication_source",
            "_close_historical_run_publication_source",
        )
        missing = [name for name in required if not hasattr(storage, name)]
        self.assertEqual(
            missing,
            [],
            "Task7 publication authority seam has not been cherry-picked",
        )


class HistoricalCorePublicationTests(unittest.TestCase):
    @staticmethod
    def _open_real_task7_lease():
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_storage as storage
        from tests.historical_foundry_task7_fixture import (
            historical_phase3_mainnet_pair_by_venue,
        )
        from tests.test_historical_foundry_scan import (
            HistoricalCandidateSelectionTests,
            HistoricalPrefilterGridTests,
        )

        fixture = HistoricalPrefilterGridTests._new_fixture(
            pair_by_venue=historical_phase3_mainnet_pair_by_venue()
        )
        run = HistoricalCandidateSelectionTests._open_replay_fixture(fixture)
        finalized = None
        try:
            selection = HistoricalCandidateSelectionTests._complete_winner(run)
            finalized = scan._finalize_historical_replay_run(
                config=run["config"], snapshot=run["snapshot"],
                selection=selection,
            )
            identity = dict(finalized.identity_projection())
            lease = storage._acquire_historical_run_publication_lease(
                run_id=identity["run_id"],
                expected_manifest_sha256=identity[
                    "run_manifest_sha256"
                ],
            )
            return run, finalized, lease, identity
        except BaseException:
            if finalized is not None:
                finalized.close()
            HistoricalCandidateSelectionTests._close_replay_fixture(run)
            raise

    @staticmethod
    def _close_real_task7_run(run, finalized):
        from tests.test_historical_foundry_scan import (
            HistoricalCandidateSelectionTests,
        )

        try:
            if finalized is not None:
                finalized.close()
        finally:
            HistoricalCandidateSelectionTests._close_replay_fixture(run)

    def test_real_task7_lease_stages_publishes_and_reopens_isolated_core(self):
        import scripts.historical_foundry_storage as storage
        import scripts.historical_route_publication as publication

        run, finalized, lease, run_identity = self._open_real_task7_lease()
        stage = staged_context = published = latest = None
        data_dir = run["fixture"].data_dir
        live_core_pointer = data_dir / "routes" / "core" / "latest.json"
        live_complete_pointer = data_dir / "routes" / "latest.json"
        live_core_pointer.parent.mkdir(parents=True, exist_ok=True)
        live_complete_pointer.parent.mkdir(parents=True, exist_ok=True)
        live_core_pointer.write_bytes(b"live-core-byte-sentinel\n")
        live_complete_pointer.write_bytes(b"live-complete-byte-sentinel\n")
        before_live = (
            live_core_pointer.read_bytes(), live_complete_pointer.read_bytes()
        )
        try:
            consumed_lease = lease
            stage = publication.stage_historical_replay_core(
                data_dir=data_dir, config=run["config"],
                publication_lease=lease,
            )
            lease = None
            with self.assertRaises(storage.HistoricalFoundryStorageError):
                storage._validate_historical_run_publication_lease(
                    lease=consumed_lease
                )
            self.assertFalse(
                (data_dir / "routes" / "historical" / "core"
                 / "latest.json").exists()
            )
            staged_context = (
                None
            )
            with mock.patch.object(
                publication._route_publication,
                "_optional_pointer_snapshot_at",
                side_effect=AssertionError("staged loader read latest"),
            ):
                staged_context = (
                    publication.load_validated_historical_replay_core_at(
                        staged_core=stage
                    )
                )
            staged_projection = dict(staged_context.identity_projection())
            self.assertEqual(staged_projection["run_id"], run_identity["run_id"])
            self.assertEqual(
                staged_projection["run_manifest_sha256"],
                run_identity["run_manifest_sha256"],
            )
            self.assertEqual(
                staged_projection["core_pointer"]["schema"],
                "route_historical_replay_core_pointer/v1",
            )
            attacker_copy = staged_context.identity_projection()
            attacker_copy["core_pointer"]["schema"] = "forged"
            attacker_copy["selected_block"]["number"] = -1
            self.assertEqual(
                dict(staged_context.identity_projection()), staged_projection
            )
            self.assertEqual(
                dict(publication._require_historical_replay_build_context(
                    context=staged_context
                )),
                staged_projection,
            )
            with self.assertRaises(publication.HistoricalRoutePublicationError):
                stage.close()
            staged_context.close()
            staged_context = None

            published = publication.publish_historical_replay_core(
                data_dir=data_dir, staged_core=stage
            )
            stage = None
            published_projection = dict(published.identity_projection())
            self.assertEqual(published_projection, staged_projection)
            pointer_path = (
                data_dir / "routes" / "historical" / "core" / "latest.json"
            )
            pointer = json.loads(pointer_path.read_bytes())
            self.assertEqual(set(pointer), {
                "schema", "bundle_stage", "route_cohort_id",
                "manifest_sha256",
            })
            self.assertEqual(
                pointer["bundle_stage"], "route_historical_replay_core/v1"
            )
            bundle = pointer_path.parent / "bundles" / pointer["route_cohort_id"]
            self.assertEqual(
                {path.name for path in bundle.iterdir()},
                {
                    "manifest.json", "route_candidates.csv",
                    "route_cohort.sqlite3", "route_legs.csv",
                    "route_timing.csv",
                },
            )
            manifest = json.loads((bundle / "manifest.json").read_bytes())
            self.assertEqual(
                manifest["schema"],
                "route_historical_replay_core_manifest/v1",
            )
            published.close()
            published = None
            latest = publication.load_latest_historical_replay_core(
                data_dir=data_dir
            )
            self.assertEqual(dict(latest.identity_projection()), staged_projection)
            latest.close()
            latest = None
            original_pointer_read = (
                publication._route_publication._optional_pointer_snapshot_at
            )
            pointer_reads = [0]

            def race_pointer(core_fd):
                pointer_reads[0] += 1
                value = original_pointer_read(core_fd)
                return (
                    value if pointer_reads[0] == 1
                    else (value[0] + b" ", value[1])
                )

            with mock.patch.object(
                publication._route_publication,
                "_optional_pointer_snapshot_at",
                side_effect=race_pointer,
            ):
                with self.assertRaises(
                    publication.HistoricalRoutePublicationError
                ):
                    publication.load_latest_historical_replay_core(
                        data_dir=data_dir
                    )
            self.assertEqual(
                (live_core_pointer.read_bytes(),
                 live_complete_pointer.read_bytes()),
                before_live,
            )
        finally:
            if latest is not None:
                latest.close()
            if published is not None:
                published.close()
            if staged_context is not None:
                staged_context.close()
            if stage is not None:
                stage.close()
            if lease is not None:
                try:
                    lease.close()
                except storage.HistoricalFoundryStorageError:
                    pass
            self._close_real_task7_run(run, finalized)

    def test_stage_failure_consumes_lease_closes_source_and_leaves_live_bytes(self):
        import scripts.historical_foundry_storage as storage
        import scripts.historical_route_publication as publication

        run, finalized, lease, _identity = self._open_real_task7_lease()
        data_dir = run["fixture"].data_dir
        live = data_dir / "routes" / "core" / "latest.json"
        live.parent.mkdir(parents=True, exist_ok=True)
        live.write_bytes(b"unchanged-live-pointer\n")
        try:
            with mock.patch.object(
                publication._route_publication,
                "_core_representation_artifact_bytes_from_validated_cohort",
                side_effect=RuntimeError("controlled serializer failure"),
            ):
                with self.assertRaises(
                    publication.HistoricalRoutePublicationError
                ):
                    publication.stage_historical_replay_core(
                        data_dir=data_dir, config=run["config"],
                        publication_lease=lease,
                    )
            with self.assertRaises(storage.HistoricalFoundryStorageError):
                lease.close()
            lease = None
            self.assertEqual(live.read_bytes(), b"unchanged-live-pointer\n")
            finalized.close()
            finalized = None
        finally:
            if lease is not None:
                try:
                    lease.close()
                except storage.HistoricalFoundryStorageError:
                    pass
            self._close_real_task7_run(run, finalized)

    def test_gc_retires_stage_borrow_and_owned_latest_source(self):
        import scripts.historical_foundry_storage as storage
        import scripts.historical_route_publication as publication

        run, finalized, lease, _identity = self._open_real_task7_lease()
        stage = staged_context = published = None
        try:
            stage = publication.stage_historical_replay_core(
                data_dir=run["fixture"].data_dir, config=run["config"],
                publication_lease=lease,
            )
            lease = None
            retired_stage = publication._stage_record(stage)
            retired_stage_path = retired_stage["stage_path"]
            retired_stage_source = retired_stage["source"]
            stage_reference = weakref.ref(stage)
            stage = None
            gc.collect()
            self.assertIsNone(stage_reference())
            self.assertEqual(retired_stage["state"], "gc_closed")
            self.assertIsNone(retired_stage["source"])
            self.assertFalse(retired_stage_path.exists())
            with self.assertRaises(storage.HistoricalFoundryStorageError):
                storage._validate_historical_run_publication_source(
                    source=retired_stage_source
                )

            finalized.close()
            finalized = None
            self._close_real_task7_run(run, finalized)
            run = None

            run, finalized, lease, _identity = self._open_real_task7_lease()
            stage = publication.stage_historical_replay_core(
                data_dir=run["fixture"].data_dir, config=run["config"],
                publication_lease=lease,
            )
            lease = None
            active_stage = publication._stage_record(stage)
            staged_context = (
                publication.load_validated_historical_replay_core_at(
                    staged_core=stage
                )
            )
            staged_record = publication._context_record(staged_context)
            context_reference = weakref.ref(staged_context)
            self.assertEqual(active_stage["borrow_count"], 1)
            staged_context = None
            gc.collect()
            self.assertIsNone(context_reference())
            self.assertEqual(staged_record["state"], "gc_closed")
            self.assertEqual(active_stage["borrow_count"], 0)

            published = publication.publish_historical_replay_core(
                data_dir=run["fixture"].data_dir, staged_core=stage
            )
            stage = None
            published_record = publication._context_record(published)
            published_source = published_record["source"]
            published_reference = weakref.ref(published)
            published = None
            gc.collect()
            self.assertIsNone(published_reference())
            self.assertEqual(published_record["state"], "gc_closed")
            self.assertIsNone(published_record["source"])
            with self.assertRaises(storage.HistoricalFoundryStorageError):
                storage._validate_historical_run_publication_source(
                    source=published_source
                )
            finalized.close()
            finalized = None
        finally:
            if published is not None:
                published.close()
            if staged_context is not None:
                staged_context.close()
            if stage is not None:
                stage.close()
            if lease is not None:
                try:
                    lease.close()
                except storage.HistoricalFoundryStorageError:
                    pass
            if run is not None:
                self._close_real_task7_run(run, finalized)

    def test_staged_loader_rejects_symlinked_member_and_stage_can_clean_up(self):
        import scripts.historical_route_publication as publication

        run, finalized, lease, _identity = self._open_real_task7_lease()
        stage = None
        try:
            stage = publication.stage_historical_replay_core(
                data_dir=run["fixture"].data_dir, config=run["config"],
                publication_lease=lease,
            )
            lease = None
            bundles = (
                run["fixture"].data_dir / "routes" / "historical"
                / "core" / "bundles"
            )
            staged = [
                path for path in bundles.iterdir()
                if path.name.startswith(".historical-core-")
            ]
            self.assertEqual(len(staged), 1)
            victim = staged[0] / "route_timing.csv"
            victim.unlink()
            victim.symlink_to("route_legs.csv")
            with self.assertRaises(
                publication.HistoricalRoutePublicationError
            ):
                publication.load_validated_historical_replay_core_at(
                    staged_core=stage
                )
            stage.close()
            stage = None
            finalized.close()
            finalized = None
        finally:
            if stage is not None:
                stage.close()
            if lease is not None:
                lease.close()
            self._close_real_task7_run(run, finalized)

    def test_duplicate_final_bundle_fails_closed_without_pointer_move(self):
        import scripts.historical_route_publication as publication

        run, finalized, lease, _identity = self._open_real_task7_lease()
        stage = context = None
        try:
            stage = publication.stage_historical_replay_core(
                data_dir=run["fixture"].data_dir, config=run["config"],
                publication_lease=lease,
            )
            lease = None
            context = publication.load_validated_historical_replay_core_at(
                staged_core=stage
            )
            route_cohort_id = context.identity_projection()[
                "core_pointer"
            ]["route_cohort_id"]
            context.close()
            context = None
            core = (
                run["fixture"].data_dir / "routes" / "historical" / "core"
            )
            (core / "bundles" / route_cohort_id).mkdir()
            with self.assertRaises(
                publication.HistoricalRoutePublicationError
            ):
                publication.publish_historical_replay_core(
                    data_dir=run["fixture"].data_dir, staged_core=stage
                )
            with self.assertRaises(
                publication.HistoricalRoutePublicationError
            ):
                stage.close()
            stage = None
            self.assertFalse((core / "latest.json").exists())
            finalized.close()
            finalized = None
        finally:
            if context is not None:
                context.close()
            if stage is not None:
                try:
                    stage.close()
                except publication.HistoricalRoutePublicationError:
                    pass
            if lease is not None:
                lease.close()
            self._close_real_task7_run(run, finalized)

    def test_pointer_changed_after_stage_is_not_overwritten(self):
        import scripts.historical_route_publication as publication

        run, finalized, lease, _identity = self._open_real_task7_lease()
        stage = published = None
        try:
            stage = publication.stage_historical_replay_core(
                data_dir=run["fixture"].data_dir, config=run["config"],
                publication_lease=lease,
            )
            lease = None
            pointer = (
                run["fixture"].data_dir / "routes" / "historical"
                / "core" / "latest.json"
            )
            concurrent = b"third-party-historical-pointer\n"
            pointer.write_bytes(concurrent)
            try:
                published = publication.publish_historical_replay_core(
                    data_dir=run["fixture"].data_dir, staged_core=stage
                )
            except publication.HistoricalRoutePublicationError:
                stage = None
            else:
                self.fail("concurrent historical pointer was overwritten")
            self.assertEqual(pointer.read_bytes(), concurrent)
            finalized.close()
            finalized = None
        finally:
            if published is not None:
                published.close()
            if stage is not None:
                try:
                    stage.close()
                except publication.HistoricalRoutePublicationError:
                    pass
            if lease is not None:
                lease.close()
            self._close_real_task7_run(run, finalized)

    def test_post_pointer_failure_restores_prior_absence(self):
        import scripts.historical_route_publication as publication

        run, finalized, lease, _identity = self._open_real_task7_lease()
        stage = None
        try:
            stage = publication.stage_historical_replay_core(
                data_dir=run["fixture"].data_dir, config=run["config"],
                publication_lease=lease,
            )
            lease = None
            source = publication._stage_record(stage)["source"]
            pointer = (
                run["fixture"].data_dir / "routes" / "historical"
                / "core" / "latest.json"
            )
            with mock.patch.object(
                type(source), "close",
                side_effect=RuntimeError("controlled post-pointer failure"),
            ):
                with self.assertRaises(
                    publication.HistoricalRoutePublicationError
                ):
                    publication.publish_historical_replay_core(
                        data_dir=run["fixture"].data_dir, staged_core=stage
                    )
            stage = None
            self.assertFalse(pointer.exists())
        finally:
            if stage is not None:
                try:
                    stage.close()
                except publication.HistoricalRoutePublicationError:
                    pass
            if lease is not None:
                lease.close()
            # The controlled close failure leaves the Task-7 source live, so
            # release it after the patch before closing the finalizer.
            if finalized is not None:
                try:
                    source.close()
                except Exception:
                    pass
            self._close_real_task7_run(run, finalized)

    def test_same_byte_stage_directory_replacement_is_rejected(self):
        import scripts.historical_route_publication as publication

        run, finalized, lease, _identity = self._open_real_task7_lease()
        stage = None
        try:
            stage = publication.stage_historical_replay_core(
                data_dir=run["fixture"].data_dir, config=run["config"],
                publication_lease=lease,
            )
            lease = None
            bundles = (
                run["fixture"].data_dir / "routes" / "historical"
                / "core" / "bundles"
            )
            original = next(
                path for path in bundles.iterdir()
                if path.name.startswith(".historical-core-")
            )
            displaced = bundles / ".displaced-stage"
            original.rename(displaced)
            shutil.copytree(displaced, original)
            unexpected = None
            try:
                unexpected = publication.load_validated_historical_replay_core_at(
                    staged_core=stage
                )
            except publication.HistoricalRoutePublicationError:
                pass
            else:
                unexpected.close()
                self.fail("same-byte replacement stage was accepted")
        finally:
            if stage is not None:
                try:
                    stage.close()
                except publication.HistoricalRoutePublicationError:
                    pass
            if lease is not None:
                lease.close()
            self._close_real_task7_run(run, finalized)


if __name__ == "__main__":
    unittest.main()
