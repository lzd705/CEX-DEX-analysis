"""Task-6 contract tests for the immutable historical complete bundle."""

from __future__ import annotations

from collections.abc import Mapping
import hashlib
import inspect
import json
from pathlib import Path
from types import MappingProxyType
import unittest
from unittest import mock


_COMPLETE_FILENAMES = {
    "route_legs.csv",
    "cost_components.csv",
    "route_opportunities.csv",
    "route_cohort.sqlite3",
    "replay_evidence.json",
    "manifest.json",
}


class HistoricalCompleteBundleTests(unittest.TestCase):
    """Use one real Task-7 fixture per end-to-end contract, not per assertion."""

    def _task6_api(self, publication):
        required = (
            "stage_historical_replay_bundle",
            "validate_historical_replay_bundle",
            "ValidatedHistoricalReplayBundleView",
        )
        missing = tuple(name for name in required if not hasattr(publication, name))
        self.assertEqual(
            missing,
            (),
            "Task-6 historical complete-bundle API is missing: {}".format(
                ", ".join(missing)
            ),
        )
        stage = publication.stage_historical_replay_bundle
        validate = publication.validate_historical_replay_bundle
        self.assertEqual(
            tuple(inspect.signature(stage).parameters),
            ("data_dir", "raw_root", "context"),
        )
        self.assertEqual(
            tuple(inspect.signature(validate).parameters),
            ("data_dir", "raw_root", "bundle_path", "expected_pointer_core"),
        )
        self.assertTrue(all(
            parameter.kind is inspect.Parameter.KEYWORD_ONLY
            for parameter in inspect.signature(stage).parameters.values()
        ))
        self.assertTrue(all(
            parameter.kind is inspect.Parameter.KEYWORD_ONLY
            for parameter in inspect.signature(validate).parameters.values()
        ))
        return stage, validate, publication.ValidatedHistoricalReplayBundleView

    def test_expected_pointer_core_accepts_read_only_mapping(self):
        import scripts.historical_route_publication as publication

        manifest = {
            "replay_id": "replay:" + "1" * 64,
            "route_cohort_id": "cohort:" + "2" * 64,
        }
        manifest_sha256 = "3" * 64
        expected = MappingProxyType({
            "schema": "route_historical_replay_pointer/v1",
            "bundle_stage": "route_historical_foundry_replay/v1",
            "replay_id": manifest["replay_id"],
            "route_cohort_id": manifest["route_cohort_id"],
            "manifest_sha256": manifest_sha256,
        })

        self.assertIsNone(
            publication._validate_historical_expected_pointer_core(
                expected=expected,
                manifest=manifest,
                manifest_sha256=manifest_sha256,
            )
        )

    @staticmethod
    def _open_published_core(publication):
        from tests.test_historical_route_publication import (
            HistoricalCorePublicationTests,
        )

        run, finalized, lease, _identity = (
            HistoricalCorePublicationTests._open_real_task7_lease()
        )
        core_stage = context = None
        try:
            core_stage = publication.stage_historical_replay_core(
                data_dir=run["fixture"].data_dir,
                config=run["config"],
                publication_lease=lease,
            )
            lease = None
            context = publication.publish_historical_replay_core(
                data_dir=run["fixture"].data_dir,
                staged_core=core_stage,
            )
            core_stage = None
            return run, finalized, context
        except BaseException:
            if context is not None:
                context.close()
            if core_stage is not None:
                try:
                    core_stage.close()
                except publication.HistoricalRoutePublicationError:
                    pass
            if lease is not None:
                lease.close()
            HistoricalCorePublicationTests._close_real_task7_run(
                run, finalized
            )
            raise

    @staticmethod
    def _close_published_core(run, finalized, context):
        from tests.test_historical_route_publication import (
            HistoricalCorePublicationTests,
        )

        try:
            if context is not None:
                context.close()
        finally:
            HistoricalCorePublicationTests._close_real_task7_run(
                run, finalized
            )

    def _stage_bundle(self, publication, stage, run, context):
        data_dir = run["fixture"].data_dir
        raw_root = data_dir / "raw" / "historical-foundry-replay"
        latest = data_dir / "routes" / "historical" / "latest.json"
        latest_before = latest.read_bytes() if latest.exists() else None

        result = stage(
            data_dir=data_dir,
            raw_root=raw_root,
            context=context,
        )

        self.assertIsInstance(result, Mapping)
        self.assertTrue(
            {"path", "replay_id", "manifest_sha256"}.issubset(result)
        )
        self.assertIsInstance(result["path"], Path)
        bundle_path = result["path"]
        self.assertEqual(
            {path.name for path in bundle_path.iterdir()},
            _COMPLETE_FILENAMES,
        )
        manifest_bytes = (bundle_path / "manifest.json").read_bytes()
        self.assertEqual(
            hashlib.sha256(manifest_bytes).hexdigest(),
            result["manifest_sha256"],
        )
        manifest = json.loads(manifest_bytes)
        self.assertEqual(manifest["replay_id"], result["replay_id"])
        self.assertEqual(
            set(manifest["files"]),
            _COMPLETE_FILENAMES - {"manifest.json"},
        )
        self.assertEqual(
            latest.read_bytes() if latest.exists() else None,
            latest_before,
            "staging a historical complete bundle moved historical latest",
        )
        return result, manifest, raw_root

    def _validate_bundle(
        self, publication, validate, view_type, *, data_dir, raw_root,
        staged,
    ):
        result = validate(
            data_dir=data_dir,
            raw_root=raw_root,
            bundle_path=staged["path"],
        )
        self.assertIsInstance(result, Mapping)
        self.assertTrue({
            "path", "manifest", "opportunities", "cost_components",
            "replay_evidence", "validated_view",
        }.issubset(result))
        self.assertEqual(result["path"], staged["path"])
        view = result["validated_view"]
        self.assertIs(type(view), view_type)
        with self.assertRaises((TypeError, ValueError)):
            view_type(
                replay_id=staged["replay_id"],
                route_cohort_id=result["manifest"]["route_cohort_id"],
                manifest_sha256=staged["manifest_sha256"],
            )
        self.assertEqual(view.replay_id, staged["replay_id"])
        self.assertEqual(
            view.route_cohort_id, result["manifest"]["route_cohort_id"]
        )
        self.assertEqual(view.manifest_sha256, staged["manifest_sha256"])
        return result

    @staticmethod
    def _close_validated_view(validated):
        if validated is None:
            return
        close = getattr(validated.get("validated_view"), "close", None)
        if callable(close):
            close()

    def test_stage_and_validate_six_file_bundle_without_moving_historical_latest(self):
        import scripts.historical_route_publication as publication

        stage, validate, view_type = self._task6_api(publication)
        run, finalized, context = self._open_published_core(publication)
        validated = None
        try:
            staged, _manifest, raw_root = self._stage_bundle(
                publication, stage, run, context
            )
            validated = self._validate_bundle(
                publication,
                validate,
                view_type,
                data_dir=run["fixture"].data_dir,
                raw_root=raw_root,
                staged=staged,
            )
            self.assertEqual(len(validated["opportunities"]), 10)
            self.assertEqual(len(validated["cost_components"]), 90)
            self.assertEqual(validated["replay_evidence"]["scenario_count"], 10)
            self.assertEqual(len(validated["replay_evidence"]["scenarios"]), 10)
            view = validated["validated_view"]
            view.reread_unchanged()
            scenario_key = validated["replay_evidence"]["scenarios"][0][
                "scenario_key"
            ]
            proof = (
                publication
                ._load_historical_cost_proof_inputs_for_published_view(
                    validated_view=view, scenario_key=scenario_key
                )
            )
            self.assertEqual(proof.scenario_key, scenario_key)
            self.assertEqual(
                proof.proof_inputs_hash,
                validated["replay_evidence"]["scenarios"][0][
                    "proof_inputs_hash"
                ],
            )
            publication._require_historical_cost_proof_owner(proof, view)
            scenario = validated["replay_evidence"]["scenarios"][0]
            opportunity = next(
                row for row in validated["opportunities"]
                if row["opportunity_id"] == scenario["opportunity_id"]
            )
            route = next(
                row for row in validated["bundle"]["routes"]
                if row["route_id"] == opportunity["route_id"]
            )
            cost_rows = tuple(
                publication._plain(row)
                for row in validated["cost_components"]
                if row["opportunity_id"] == opportunity["opportunity_id"]
            )
            tampered_rows = [dict(row) for row in cost_rows]
            tampered_rows[0]["source_record_sha256"] = "0" * 64
            with self.assertRaises(
                publication.HistoricalRoutePublicationError
            ):
                publication._validate_historical_cost_rows_for_published_view(
                    validated_view=view,
                    scenario_key=scenario_key,
                    route=publication._plain(route),
                    rows=tuple(tampered_rows),
                )
            forged = object.__new__(view_type)
            with self.assertRaises(
                publication.HistoricalRoutePublicationError
            ):
                publication._load_historical_cost_proof_inputs_for_published_view(
                    validated_view=forged, scenario_key=scenario_key
                )
        finally:
            self._close_validated_view(validated)
            self._close_published_core(run, finalized, context)

    def test_old_bundle_validates_after_historical_core_latest_advances(self):
        import scripts.historical_route_publication as publication

        stage, validate, view_type = self._task6_api(publication)
        run, finalized, context = self._open_published_core(publication)
        validated = None
        try:
            staged, manifest, raw_root = self._stage_bundle(
                publication, stage, run, context
            )
            data_dir = run["fixture"].data_dir
            core_latest = (
                data_dir / "routes" / "historical" / "core" / "latest.json"
            )
            old_core_pointer = json.loads(core_latest.read_bytes())

            context.close()
            context = None
            advanced_pointer = dict(old_core_pointer)
            replacement_cohort = "cohort:" + "f" * 64
            if replacement_cohort == old_core_pointer["route_cohort_id"]:
                replacement_cohort = "cohort:" + "0" * 64
            replacement_manifest = "e" * 64
            if replacement_manifest == old_core_pointer["manifest_sha256"]:
                replacement_manifest = "1" * 64
            advanced_pointer["route_cohort_id"] = replacement_cohort
            advanced_pointer["manifest_sha256"] = replacement_manifest
            self.assertNotEqual(advanced_pointer, old_core_pointer)
            core_latest.write_bytes(
                json.dumps(
                    advanced_pointer,
                    ensure_ascii=False,
                    indent=2,
                    sort_keys=True,
                ).encode("utf-8") + b"\n"
            )

            validated = self._validate_bundle(
                publication,
                validate,
                view_type,
                data_dir=data_dir,
                raw_root=raw_root,
                staged=staged,
            )
            self.assertEqual(
                validated["validated_view"].route_cohort_id,
                old_core_pointer["route_cohort_id"],
            )
            self.assertEqual(
                validated["manifest"]["historical_core_manifest_sha256"],
                manifest["historical_core_manifest_sha256"],
            )
        finally:
            self._close_validated_view(validated)
            self._close_published_core(run, finalized, context)

    def test_pre_rename_validation_failure_cleans_stage_and_retry_succeeds(self):
        import scripts.historical_route_publication as publication

        stage, validate, view_type = self._task6_api(publication)
        run, finalized, context = self._open_published_core(publication)
        validated = None
        try:
            data_dir = run["fixture"].data_dir
            raw_root = data_dir / "raw" / "historical-foundry-replay"
            latest = data_dir / "routes" / "historical" / "latest.json"
            latest_before = latest.read_bytes() if latest.exists() else None
            real_validate = publication._validate_historical_replay_bundle

            def fail_only_staged(**kwargs):
                if kwargs["bundle_path"].name.startswith(
                    ".historical-replay-"
                ):
                    raise publication.HistoricalRoutePublicationError(
                        "controlled staged reread failure"
                    )
                return real_validate(**kwargs)

            with mock.patch.object(
                publication, "_validate_historical_replay_bundle",
                side_effect=fail_only_staged,
            ):
                with self.assertRaises(
                    publication.HistoricalRoutePublicationError
                ):
                    stage(
                        data_dir=data_dir, raw_root=raw_root,
                        context=context,
                    )
            bundles = data_dir / "routes" / "historical" / "bundles"
            self.assertEqual(list(bundles.iterdir()), [])
            self.assertEqual(
                latest.read_bytes() if latest.exists() else None,
                latest_before,
            )

            staged = stage(
                data_dir=data_dir, raw_root=raw_root, context=context
            )
            validated = self._validate_bundle(
                publication, validate, view_type,
                data_dir=data_dir, raw_root=raw_root, staged=staged,
            )
        finally:
            self._close_validated_view(validated)
            self._close_published_core(run, finalized, context)

    def test_post_rename_public_validation_failure_rolls_back_and_retries(self):
        import scripts.historical_route_publication as publication

        stage, validate, view_type = self._task6_api(publication)
        run, finalized, context = self._open_published_core(publication)
        validated = None
        try:
            data_dir = run["fixture"].data_dir
            raw_root = data_dir / "raw" / "historical-foundry-replay"
            real_validate = publication._validate_historical_replay_bundle
            call_count = [0]

            def fail_public_reread(**kwargs):
                call_count[0] += 1
                if call_count[0] == 3:
                    raise publication.HistoricalRoutePublicationError(
                        "controlled public reread failure"
                    )
                return real_validate(**kwargs)

            with mock.patch.object(
                publication, "_validate_historical_replay_bundle",
                side_effect=fail_public_reread,
            ):
                with self.assertRaises(
                    publication.HistoricalRoutePublicationError
                ):
                    stage(
                        data_dir=data_dir, raw_root=raw_root,
                        context=context,
                    )
            self.assertEqual(call_count[0], 3)
            bundles = data_dir / "routes" / "historical" / "bundles"
            self.assertEqual(list(bundles.iterdir()), [])

            staged = stage(
                data_dir=data_dir, raw_root=raw_root, context=context
            )
            validated = self._validate_bundle(
                publication, validate, view_type,
                data_dir=data_dir, raw_root=raw_root, staged=staged,
            )
        finally:
            self._close_validated_view(validated)
            self._close_published_core(run, finalized, context)

    def test_evidence_join_rejects_rehashed_noncanonical_scenario_facts(self):
        import scripts.historical_route_publication as publication

        run, finalized, context = self._open_published_core(publication)
        try:
            payload = publication._build_historical_complete_payload(
                context=context
            )
            bundle = payload["bundle"]
            original = payload["replay_evidence"]

            def rehashed_evidence(mutator):
                evidence = json.loads(publication._canonical_bytes(original))
                mutator(evidence)
                evidence["scenario_set_sha256"] = (
                    publication._historical_replay._typed_digest(
                        "historical_foundry_scenario_set/v1",
                        evidence["scenarios"],
                    )
                )
                return evidence

            def add_nested_eth_usd_field(evidence):
                evidence["selected_block"]["eth_usd"]["attacker"] = "field"
                for row in evidence["scenarios"]:
                    row["selected_block"]["eth_usd"][
                        "attacker"
                    ] = "field"

            def replace_receipt_status_with_boolean(evidence):
                evidence["scenarios"][0]["receipt_status"] = True
                evidence["scenarios"][0]["receipt"]["status"] = True

            mutations = {
                "scenario schema": lambda evidence: evidence[
                    "scenarios"
                ][0].__setitem__("schema", "attacker/v1"),
                "nested ETH/USD field": add_nested_eth_usd_field,
                "boolean receipt status": replace_receipt_status_with_boolean,
            }
            for label, mutator in mutations.items():
                with self.subTest(label=label):
                    evidence = rehashed_evidence(mutator)
                    with self.assertRaises(
                        publication.HistoricalRoutePublicationError
                    ):
                        publication._validate_historical_replay_evidence_join(
                            bundle=bundle, evidence=evidence
                        )
        finally:
            self._close_published_core(run, finalized, context)

    def test_writer_rejects_rehashed_selected_block_outside_sealed_context(self):
        import scripts.historical_route_publication as publication

        run, finalized, context = self._open_published_core(publication)
        real_builder = (
            publication._historical_replay
            .build_historical_replay_publication_facts
        )

        def rebound_facts(*args):
            facts = json.loads(real_builder(*args))
            false_hash = "0x" + "f" * 64
            for row in facts["scenarios"]:
                row["selected_block"]["hash"] = false_hash
                row["selected_block"]["eth_usd"]["block_hash"] = false_hash
            facts["scenario_set_sha256"] = (
                publication._historical_replay._typed_digest(
                    "historical_foundry_scenario_set/v1",
                    facts["scenarios"],
                )
            )
            return publication._canonical_bytes(facts)

        try:
            with mock.patch.object(
                publication._historical_replay,
                "build_historical_replay_publication_facts",
                side_effect=rebound_facts,
            ):
                with self.assertRaises(
                    publication.HistoricalRoutePublicationError
                ):
                    publication._build_historical_complete_payload(
                        context=context
                    )
        finally:
            self._close_published_core(run, finalized, context)

    def test_writer_rejects_rehashed_block_derivatives_outside_sealed_input(self):
        import scripts.historical_route_publication as publication

        run, finalized, context = self._open_published_core(publication)
        real_builder = (
            publication._historical_replay
            .build_historical_replay_publication_facts
        )

        def rebound_facts(*args):
            facts = json.loads(real_builder(*args))
            for row in facts["scenarios"]:
                answer = int(row["selected_block"]["eth_usd"]["answer"])
                row["selected_block"]["eth_usd"]["answer"] = str(
                    answer + 1
                )
                row["selected_block"]["synthetic_child_timestamp"] += 1
            facts["scenario_set_sha256"] = (
                publication._historical_replay._typed_digest(
                    "historical_foundry_scenario_set/v1",
                    facts["scenarios"],
                )
            )
            return publication._canonical_bytes(facts)

        try:
            with mock.patch.object(
                publication._historical_replay,
                "build_historical_replay_publication_facts",
                side_effect=rebound_facts,
            ):
                with self.assertRaises(
                    publication.HistoricalRoutePublicationError
                ):
                    publication._build_historical_complete_payload(
                        context=context
                    )
        finally:
            self._close_published_core(run, finalized, context)

    def test_writer_rejects_rehashed_source_facts_outside_sealed_input(self):
        import scripts.historical_route_publication as publication

        run, finalized, context = self._open_published_core(publication)
        real_builder = (
            publication._historical_replay
            .build_historical_replay_publication_facts
        )

        def rebound_facts(*args):
            facts = json.loads(real_builder(*args))
            scenario = facts["scenarios"][0]
            scenario["source_members"][0]["byte_count"] += 1
            scenario["receipt"]["block_hash"] = "0x" + "f" * 64
            facts["scenario_set_sha256"] = (
                publication._historical_replay._typed_digest(
                    "historical_foundry_scenario_set/v1",
                    facts["scenarios"],
                )
            )
            return publication._canonical_bytes(facts)

        try:
            with mock.patch.object(
                publication._historical_replay,
                "build_historical_replay_publication_facts",
                side_effect=rebound_facts,
            ):
                with self.assertRaises(
                    publication.HistoricalRoutePublicationError
                ):
                    publication._build_historical_complete_payload(
                        context=context
                    )
        finally:
            self._close_published_core(run, finalized, context)

    def test_writer_rejects_replaced_per_scenario_fact_oracle(self):
        import scripts.historical_route_publication as publication

        run, finalized, context = self._open_published_core(publication)
        real_oracle = (
            publication._historical_replay
            ._historical_publication_scenario_facts
        )

        def replaced_oracle(*args, **kwargs):
            row = publication._plain(real_oracle(*args, **kwargs))
            row["source_members"][0]["byte_count"] += 1
            return row

        try:
            with mock.patch.object(
                publication._historical_replay,
                "_historical_publication_scenario_facts",
                side_effect=replaced_oracle,
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
