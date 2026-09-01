"""Capability and idempotence contracts for historical complete bundles."""

from __future__ import annotations

import gc
import hashlib
import types
import unittest
import weakref


_COMPLETE_FILENAMES = {
    "cost_components.csv",
    "manifest.json",
    "replay_evidence.json",
    "route_cohort.sqlite3",
    "route_legs.csv",
    "route_opportunities.csv",
}


class HistoricalCompleteCapabilityTests(unittest.TestCase):
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

    def test_view_authority_storage_is_not_exposed_as_module_state(self):
        import scripts.historical_route_publication as publication

        exposed = tuple(
            sorted(
                name
                for name in vars(publication)
                if "view" in name.lower()
                and any(
                    marker in name.lower()
                    for marker in ("issuer", "registry", "sentinel")
                )
            )
        )
        self.assertEqual(exposed, ())

    def test_view_methods_do_not_close_over_a_mutable_registry(self):
        import scripts.historical_route_publication as publication

        def closure_values(function):
            pending = [function]
            seen = set()
            values = []
            while pending:
                current = pending.pop()
                if id(current) in seen:
                    continue
                seen.add(id(current))
                for cell in current.__closure__ or ():
                    value = cell.cell_contents
                    values.append(value)
                    if isinstance(value, types.FunctionType):
                        pending.append(value)
            return values

        view_type = publication.ValidatedHistoricalReplayBundleView
        values = []
        for name in ("close", "reread_unchanged", "__enter__"):
            values.extend(closure_values(getattr(view_type, name)))
        self.assertFalse(any(type(value) is dict for value in values))

    def test_view_rejects_public_construction_and_uninitialized_instance(
        self,
    ):
        import scripts.historical_route_publication as publication

        view_type = publication.ValidatedHistoricalReplayBundleView
        with self.assertRaises(publication.HistoricalRoutePublicationError):
            view_type()

        forged = object.__new__(view_type)
        with self.assertRaises(publication.HistoricalRoutePublicationError):
            forged.reread_unchanged()

    def test_published_cost_proof_is_bound_to_one_validated_view(self):
        import scripts.historical_route_publication as publication

        run, finalized, context = self._open_published_core(publication)
        first = second = None
        try:
            data_dir = run["fixture"].data_dir
            raw_root = data_dir / "raw" / "historical-foundry-replay"
            staged = publication.stage_historical_replay_bundle(
                data_dir=data_dir, raw_root=raw_root, context=context
            )
            first = publication.validate_historical_replay_bundle(
                data_dir=data_dir,
                raw_root=raw_root,
                bundle_path=staged["path"],
            )
            second = publication.validate_historical_replay_bundle(
                data_dir=data_dir,
                raw_root=raw_root,
                bundle_path=staged["path"],
            )
            first_view = first["validated_view"]
            second_view = second["validated_view"]
            self.assertIsNot(first_view, second_view)
            scenario_key = first["replay_evidence"]["scenarios"][0][
                "scenario_key"
            ]
            first_proof = (
                publication
                ._load_historical_cost_proof_inputs_for_published_view(
                    validated_view=first_view, scenario_key=scenario_key
                )
            )
            second_proof = (
                publication
                ._load_historical_cost_proof_inputs_for_published_view(
                    validated_view=second_view, scenario_key=scenario_key
                )
            )
            publication._require_historical_cost_proof_owner(
                first_proof, first_view
            )
            publication._require_historical_cost_proof_owner(
                second_proof, second_view
            )
            with self.assertRaises(
                publication.HistoricalRoutePublicationError
            ):
                publication._require_historical_cost_proof_owner(
                    first_proof, second_view
                )
            with self.assertRaises(
                publication.HistoricalRoutePublicationError
            ):
                publication._require_historical_cost_proof_owner(
                    second_proof, first_view
                )
        finally:
            for validated in (second, first):
                if validated is not None:
                    validated["validated_view"].close()
            self._close_published_core(run, finalized, context)

    def test_view_close_and_gc_release_the_owned_immutable_context(self):
        import scripts.historical_route_publication as publication

        run, finalized, context = self._open_published_core(publication)
        try:
            data_dir = run["fixture"].data_dir
            raw_root = data_dir / "raw" / "historical-foundry-replay"
            staged = publication.stage_historical_replay_bundle(
                data_dir=data_dir, raw_root=raw_root, context=context
            )
            baseline = set(publication._CONTEXT_REGISTRY)

            validated = publication.validate_historical_replay_bundle(
                data_dir=data_dir,
                raw_root=raw_root,
                bundle_path=staged["path"],
            )
            view = validated["validated_view"]
            owner = object.__getattribute__(view, "_validated_record")
            inner = owner["payload"]["immutable_context"]
            inner_id = id(inner)
            finalizer = object.__getattribute__(
                view, "_context_finalizer"
            )
            self.assertTrue(finalizer.alive)
            self.assertIn(inner_id, publication._CONTEXT_REGISTRY)
            self.assertIsNone(view.close())
            self.assertFalse(finalizer.alive)
            self.assertNotIn(inner_id, publication._CONTEXT_REGISTRY)
            with self.assertRaises(
                publication.HistoricalRoutePublicationError
            ):
                view.reread_unchanged()
            validated = view = owner = inner = finalizer = None
            gc.collect()
            self.assertEqual(set(publication._CONTEXT_REGISTRY), baseline)

            validated = publication.validate_historical_replay_bundle(
                data_dir=data_dir,
                raw_root=raw_root,
                bundle_path=staged["path"],
            )
            view = validated["validated_view"]
            owner = object.__getattribute__(view, "_validated_record")
            inner = owner["payload"]["immutable_context"]
            view_reference = weakref.ref(view)
            inner_reference = weakref.ref(inner)
            inner_id = id(inner)
            self.assertIn(inner_id, publication._CONTEXT_REGISTRY)
            validated = view = owner = inner = None
            gc.collect()
            self.assertIsNone(view_reference())
            self.assertIsNone(inner_reference())
            self.assertEqual(set(publication._CONTEXT_REGISTRY), baseline)
        finally:
            self._close_published_core(run, finalized, context)

    def test_repeated_stage_is_byte_identical_and_reuses_one_replay(self):
        import scripts.historical_route_publication as publication

        run, finalized, context = self._open_published_core(publication)
        try:
            data_dir = run["fixture"].data_dir
            raw_root = data_dir / "raw" / "historical-foundry-replay"
            first = publication.stage_historical_replay_bundle(
                data_dir=data_dir, raw_root=raw_root, context=context
            )
            first_hashes = {
                member.name: hashlib.sha256(member.read_bytes()).hexdigest()
                for member in first["path"].iterdir()
            }

            second = publication.stage_historical_replay_bundle(
                data_dir=data_dir, raw_root=raw_root, context=context
            )
            second_hashes = {
                member.name: hashlib.sha256(member.read_bytes()).hexdigest()
                for member in second["path"].iterdir()
            }

            self.assertEqual(first["replay_id"], second["replay_id"])
            self.assertEqual(
                first["manifest_sha256"], second["manifest_sha256"]
            )
            self.assertEqual(first["path"], second["path"])
            self.assertEqual(set(first_hashes), _COMPLETE_FILENAMES)
            self.assertEqual(first_hashes, second_hashes)
            bundles = data_dir / "routes" / "historical" / "bundles"
            self.assertEqual(
                {entry.name for entry in bundles.iterdir()},
                {first["replay_id"]},
            )
        finally:
            self._close_published_core(run, finalized, context)


if __name__ == "__main__":
    unittest.main()
