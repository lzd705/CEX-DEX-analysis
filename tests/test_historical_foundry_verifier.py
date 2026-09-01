"""Tests for historical connected verification and report installation."""

from __future__ import annotations

import errno
import hashlib
import inspect
import json
import multiprocessing
import os
from pathlib import Path
import stat
import tempfile
import unittest


def _connected_observation_worker(request, connection):
    try:
        import scripts.historical_foundry_verifier as verifier

        connection.send(
            verifier._build_connected_observation_for_retained_fixture(
                request
            )
        )
    except BaseException as error:
        connection.send(("error", type(error).__name__, str(error)))
    finally:
        connection.close()


def _local_connected_engine(request):
    context = multiprocessing.get_context("spawn")
    parent, child = context.Pipe(duplex=False)
    process = context.Process(
        target=_connected_observation_worker, args=(request, child)
    )
    process.start()
    child.close()
    try:
        observation = parent.recv()
    finally:
        parent.close()
        process.join(timeout=60)
    if process.exitcode != 0:
        raise AssertionError("local connected process failed")
    if isinstance(observation, tuple) and observation[:1] == ("error",):
        raise AssertionError(repr(observation))
    return observation


def _install_report_worker(root_text, payload, queue):
    from scripts.historical_foundry_verifier import (
        install_historical_verification_report,
    )

    try:
        result = install_historical_verification_report(
            verification_root=Path(root_text), report_bytes=payload,
        )
        queue.put(("ok", result.disposition, result.sha256, result.size))
    except BaseException as error:
        queue.put(("error", type(error).__name__, str(error)))


def _unbound_connected_worker(queue):
    import scripts.historical_foundry_verifier as verifier

    rejected_binding = None
    try:
        verifier._bind_connected_historical_verification_engine(
            _local_connected_engine
        )
    except BaseException as error:
        rejected_binding = (type(error).__name__, str(error))
    try:
        verifier._invoke_connected_historical_verification_engine({})
    except BaseException as error:
        queue.put((
            rejected_binding,
            (type(error).__name__, str(error)),
        ))
    else:
        queue.put((rejected_binding, ("unexpected_success", "")))


class HistoricalVerificationInterfaceTests(unittest.TestCase):
    def test_public_interfaces_are_exact_and_subject_is_private(self):
        import scripts.historical_foundry_verifier as verifier

        self.assertEqual(
            tuple(inspect.signature(
                verifier.run_connected_historical_verification
            ).parameters),
            ("subject", "mode"),
        )
        self.assertEqual(
            tuple(inspect.signature(
                verifier.historical_replay_pointer_core
            ).parameters),
            ("pointer",),
        )
        self.assertEqual(
            tuple(inspect.signature(
                verifier.install_historical_verification_report
            ).parameters),
            ("verification_root", "report_bytes"),
        )
        with self.assertRaises(verifier.HistoricalVerificationError):
            verifier.HistoricalVerificationSubject()
        forged = object.__new__(verifier.HistoricalVerificationSubject)
        self.assertEqual(
            repr(forged), "HistoricalVerificationSubject(<redacted>)"
        )
        with self.assertRaises(verifier.HistoricalVerificationError):
            verifier.run_connected_historical_verification(
                forged, mode="staged"
            )

    def test_fresh_production_import_fails_closed_without_authority(self):
        context = multiprocessing.get_context("spawn")
        queue = context.Queue()
        process = context.Process(
            target=_unbound_connected_worker, args=(queue,)
        )
        process.start()
        result = queue.get(timeout=30)
        process.join(timeout=30)
        self.assertEqual(process.exitcode, 0)
        self.assertEqual(result, (
            (
                "HistoricalVerificationError",
                "historical connected engine binder is invalid",
            ),
            (
                "HistoricalVerificationError",
                "historical connected authority is unavailable",
            ),
        ))

    def test_pointer_core_removes_only_report_hash_and_keeps_schema(self):
        from scripts.historical_foundry_verifier import (
            historical_replay_pointer_core,
        )

        pointer = {
            "schema": "route_historical_replay_pointer/v1",
            "bundle_stage": "route_historical_foundry_replay/v1",
            "replay_id": "replay:" + "1" * 64,
            "route_cohort_id": "cohort:" + "2" * 64,
            "manifest_sha256": "3" * 64,
            "verification_report_sha256": "4" * 64,
        }
        core = historical_replay_pointer_core(pointer)
        self.assertEqual(dict(core), {
            key: value for key, value in pointer.items()
            if key != "verification_report_sha256"
        })
        self.assertEqual(
            core["schema"], "route_historical_replay_pointer/v1"
        )
        for mutate in (
            lambda value: value.pop("verification_report_sha256"),
            lambda value: value.__setitem__("extra", "attacker"),
            lambda value: value.__setitem__(
                "schema", "route_historical_replay_pointer_core/v1"
            ),
        ):
            with self.subTest(mutate=mutate):
                forged = dict(pointer)
                mutate(forged)
                with self.assertRaises(ValueError):
                    historical_replay_pointer_core(forged)


class HistoricalVerificationReportInstallTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name) / "routes" / "historical" / "verifications"

    def tearDown(self):
        self.temporary.cleanup()

    @staticmethod
    def _report_bytes(*, marker="baseline", whitespace=False):
        value = {
            "schema": "route_historical_replay_verification/v1",
            "status": "verified",
            "marker": marker,
        }
        if whitespace:
            return json.dumps(value, sort_keys=True, indent=2).encode("utf-8")
        return json.dumps(
            value, sort_keys=True, separators=(",", ":")
        ).encode("utf-8")

    def _target(self, payload):
        import scripts.route_publication as route_publication

        digest = hashlib.sha256(payload).hexdigest()
        root = route_publication._absolute_without_symlink_resolution(
            self.root
        )
        return root / "by-sha256" / (digest + ".json")

    def _assert_physical(self, result, payload):
        digest = hashlib.sha256(payload).hexdigest()
        self.assertEqual(result.path, self._target(payload))
        self.assertEqual(result.sha256, digest)
        self.assertEqual(result.size, len(payload))
        self.assertIn(result.disposition, ("created", "matched_existing"))
        self.assertTrue(hasattr(os, "O_CLOEXEC"))
        self.assertTrue(hasattr(os, "O_NOFOLLOW"))
        flags = os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW
        descriptor = os.open(str(result.path), flags)
        try:
            details = os.fstat(descriptor)
            self.assertTrue(stat.S_ISREG(details.st_mode))
            self.assertEqual(details.st_nlink, 1)
            self.assertEqual(details.st_uid, os.geteuid())
            self.assertEqual(stat.S_IMODE(details.st_mode), 0o600)
            self.assertFalse(os.get_inheritable(descriptor))
            chunks = []
            while True:
                chunk = os.read(descriptor, 65536)
                if not chunk:
                    break
                chunks.append(chunk)
        finally:
            os.close(descriptor)
        actual = b"".join(chunks)
        self.assertEqual(actual, payload)
        self.assertEqual(len(actual), result.size)
        self.assertEqual(hashlib.sha256(actual).hexdigest(), result.sha256)
        self.assertEqual(result.path.stem, result.sha256)

    def test_report_eexist_exact_bytes_is_idempotent(self):
        from scripts.historical_foundry_verifier import (
            install_historical_verification_report,
        )

        payload = self._report_bytes()
        created = install_historical_verification_report(
            verification_root=self.root, report_bytes=payload,
        )
        matched = install_historical_verification_report(
            verification_root=self.root, report_bytes=payload,
        )
        self.assertEqual(created.disposition, "created")
        self.assertEqual(matched.disposition, "matched_existing")
        self._assert_physical(created, payload)
        self._assert_physical(matched, payload)

    def test_report_eexist_different_bytes_rejects(self):
        from scripts.historical_foundry_verifier import (
            HistoricalVerificationError,
            install_historical_verification_report,
        )

        requested = self._report_bytes(marker="requested")
        target = self._target(requested)
        target.parent.mkdir(parents=True)
        target.write_bytes(self._report_bytes(marker="attacker"))
        before = target.read_bytes()
        with self.assertRaisesRegex(
            HistoricalVerificationError, "historical_bundle_invalid"
        ):
            install_historical_verification_report(
                verification_root=self.root, report_bytes=requested,
            )
        self.assertEqual(target.read_bytes(), before)

    def test_report_eexist_matching_identities_but_different_bytes_rejects(self):
        from scripts.historical_foundry_verifier import (
            HistoricalVerificationError,
            install_historical_verification_report,
        )

        requested = self._report_bytes(whitespace=False)
        target = self._target(requested)
        target.parent.mkdir(parents=True)
        target.write_bytes(self._report_bytes(whitespace=True))
        with self.assertRaisesRegex(
            HistoricalVerificationError, "historical_bundle_invalid"
        ):
            install_historical_verification_report(
                verification_root=self.root, report_bytes=requested,
            )
        self.assertEqual(target.read_bytes(), self._report_bytes(whitespace=True))

    def test_report_eexist_symlink_or_nonregular_rejects(self):
        from scripts.historical_foundry_verifier import (
            HistoricalVerificationError,
            install_historical_verification_report,
        )

        for kind in ("symlink", "directory"):
            with self.subTest(kind=kind):
                payload = self._report_bytes(marker=kind)
                target = self._target(payload)
                target.parent.mkdir(parents=True, exist_ok=True)
                if kind == "symlink":
                    victim = target.parent / "victim.json"
                    victim.write_bytes(b"victim")
                    target.symlink_to(victim.name)
                else:
                    target.mkdir()
                with self.assertRaisesRegex(
                    HistoricalVerificationError,
                    "historical_bundle_invalid",
                ):
                    install_historical_verification_report(
                        verification_root=self.root,
                        report_bytes=payload,
                    )
                if kind == "symlink":
                    self.assertTrue(target.is_symlink())
                    self.assertEqual(victim.read_bytes(), b"victim")
                else:
                    self.assertTrue(target.is_dir())

    def test_report_eexist_wrong_mode_or_owner_rejects(self):
        import scripts.historical_foundry_verifier as verifier
        from unittest import mock

        payload = self._report_bytes(marker="physical-identity")
        created = verifier.install_historical_verification_report(
            verification_root=self.root, report_bytes=payload,
        )
        created.path.chmod(0o644)
        with self.assertRaisesRegex(
            verifier.HistoricalVerificationError,
            "historical_bundle_invalid",
        ):
            verifier.install_historical_verification_report(
                verification_root=self.root, report_bytes=payload,
            )
        created.path.chmod(0o600)
        with mock.patch.object(
            verifier.os, "geteuid", return_value=os.geteuid() + 1,
        ):
            with self.assertRaisesRegex(
                verifier.HistoricalVerificationError,
                "historical_bundle_invalid",
            ):
                verifier.install_historical_verification_report(
                    verification_root=self.root, report_bytes=payload,
                )
        self._assert_physical(created, payload)

    def test_concurrent_report_install_accepts_only_exact_winner_bytes(self):
        payload = self._report_bytes(marker="concurrent")
        context = multiprocessing.get_context("spawn")
        queue = context.Queue()
        processes = [
            context.Process(
                target=_install_report_worker,
                args=(str(self.root), payload, queue),
            )
            for _index in range(4)
        ]
        for process in processes:
            process.start()
        results = [queue.get(timeout=30) for _process in processes]
        for process in processes:
            process.join(timeout=30)
            self.assertEqual(process.exitcode, 0)
        self.assertTrue(all(row[0] == "ok" for row in results), results)
        self.assertEqual(
            sum(row[1] == "created" for row in results), 1, results
        )
        self.assertEqual(
            sum(row[1] == "matched_existing" for row in results), 3,
            results,
        )
        from scripts.historical_foundry_verifier import (
            install_historical_verification_report,
        )
        final = install_historical_verification_report(
            verification_root=self.root, report_bytes=payload,
        )
        self.assertEqual(final.disposition, "matched_existing")
        self._assert_physical(final, payload)


class HistoricalConnectedVerificationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import scripts.historical_foundry_verifier as verifier
        import scripts.historical_route_publication as publication
        from tests.test_historical_complete_bundle import (
            HistoricalCompleteBundleTests,
        )

        verifier._bind_connected_historical_verification_engine(
            _local_connected_engine
        )
        cls.publication = publication
        cls.helper = HistoricalCompleteBundleTests
        cls.run_fixture = cls.finalized = None
        cls.context = cls.subject = None
        try:
            cls.run_fixture, cls.finalized, cls.context = (
                cls.helper._open_published_core(publication)
            )
            staged = publication.stage_historical_replay_bundle(
                data_dir=cls.run_fixture["fixture"].data_dir,
                raw_root=(
                    cls.run_fixture["fixture"].data_dir / "raw"
                    / "historical-foundry-replay"
                ),
                context=cls.context,
            )
            cls.subject = staged["verification_subject"]
            cls.pointer_core = dict(staged["pointer_core"])
            cls.verification_root = (
                cls.run_fixture["fixture"].data_dir / "routes" / "historical"
                / "verifications"
            )
        except BaseException:
            cls.tearDownClass()
            raise

    @classmethod
    def tearDownClass(cls):
        if cls.subject is not None:
            try:
                cls.subject.close()
            except Exception:
                pass
            cls.subject = None
        if cls.context is not None:
            try:
                cls.context.close()
            except Exception:
                pass
            cls.context = None
        if cls.run_fixture is not None:
            cls.helper._close_published_core(
                cls.run_fixture, cls.finalized, cls.context
            )
            cls.run_fixture = cls.finalized = None

    def test_local_fresh_process_verifies_without_implying_external_rpc(self):
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_verifier as verifier
        from unittest import mock

        with mock.patch.object(
            scan, "select_historical_replay_block",
            side_effect=AssertionError("verifier must not select"),
        ):
            staged = verifier.run_connected_historical_verification(
                self.subject, mode="staged"
            )
        self.assertEqual(staged["report"]["status"], "verified")
        self.assertEqual(
            staged["report"]["schema"],
            "route_historical_replay_verification/v1",
        )
        self.assertEqual(
            staged["report"]["evidence_mode"], "offline_test_fixture"
        )
        self.assertEqual(
            staged["report"]["verification_scenario_count"], 10
        )
        self.assertFalse(self.verification_root.exists())
        self.assertIsNone(staged["install_result"])
        self.assertEqual(
            dict(verifier.historical_replay_pointer_core(
                staged["final_pointer"]
            )),
            self.pointer_core,
        )

        published = verifier.run_connected_historical_verification(
            self.subject, mode="publish"
        )
        self.assertEqual(published["install_result"].disposition, "created")
        self.assertEqual(
            published["install_result"].sha256,
            published["report_sha256"],
        )
        self.assertEqual(
            published["final_pointer"]["verification_report_sha256"],
            published["report_sha256"],
        )
        self.assertEqual(
            published["install_result"].path.read_bytes(),
            published["report_bytes"],
        )
        forbidden = ("url", "path", "body", "credential", "exception")

        def inspect_value(value):
            if isinstance(value, dict):
                for key, nested in value.items():
                    self.assertFalse(any(word in key.lower() for word in forbidden))
                    inspect_value(nested)
            elif isinstance(value, (list, tuple)):
                for nested in value:
                    inspect_value(nested)

        inspect_value(dict(published["report"]))

    def test_wrong_scenario_set_and_resolution_transplant_reject(self):
        import scripts.historical_foundry_verifier as verifier

        request = verifier._connected_request_for_subject(self.subject)
        observation = _local_connected_engine(request)
        baseline = json.loads(json.dumps(observation))
        attacks = []
        wrong_set = json.loads(json.dumps(baseline))
        wrong_set["projection"]["verification_scenario_keys"].pop()
        attacks.append(wrong_set)
        transplanted = json.loads(json.dumps(baseline))
        results = transplanted["projection"]["scenario_results"]
        results[0]["resolution"] = results[-1]["resolution"]
        attacks.append(transplanted)
        for attack in attacks:
            with self.subTest(attack=attacks.index(attack)):
                with self.assertRaisesRegex(
                    verifier.HistoricalVerificationError,
                    "historical_bundle_invalid",
                ):
                    verifier._validate_connected_historical_observation(
                        subject=self.subject, observation=attack,
                    )

    def test_report_deletion_or_mutation_before_pointer_construction_rejects(self):
        import scripts.historical_foundry_verifier as verifier
        from unittest import mock

        pointer = (
            self.run_fixture["fixture"].data_dir / "routes"
            / "historical" / "latest.json"
        )
        self.assertFalse(pointer.exists())
        real_install = verifier.install_historical_verification_report

        for attack in ("delete", "mutate"):
            with self.subTest(attack=attack):
                def attacked_install(**kwargs):
                    installed = real_install(**kwargs)
                    if attack == "delete":
                        installed.path.unlink()
                    else:
                        installed.path.write_bytes(b"attacker report bytes")
                    return installed

                with mock.patch.object(
                    verifier,
                    "install_historical_verification_report",
                    side_effect=attacked_install,
                ):
                    with self.assertRaisesRegex(
                        verifier.HistoricalVerificationError,
                        "historical_bundle_invalid",
                    ):
                        verifier.run_connected_historical_verification(
                            self.subject, mode="publish"
                        )
                self.assertFalse(pointer.exists())

    def test_report_install_precedes_final_pointer_construction(self):
        import scripts.historical_foundry_verifier as verifier
        from unittest import mock

        events = []
        real_install = verifier.install_historical_verification_report
        real_pointer_core = verifier.historical_replay_pointer_core

        def recorded_install(**kwargs):
            events.append("report_installed")
            return real_install(**kwargs)

        def recorded_pointer(pointer):
            events.append("final_pointer_constructed")
            return real_pointer_core(pointer)

        with mock.patch.object(
            verifier, "install_historical_verification_report",
            side_effect=recorded_install,
        ), mock.patch.object(
            verifier, "historical_replay_pointer_core",
            side_effect=recorded_pointer,
        ):
            result = verifier.run_connected_historical_verification(
                self.subject, mode="publish"
            )
        self.assertEqual(
            events, ["report_installed", "final_pointer_constructed"]
        )
        self.assertEqual(
            result["install_result"].path.read_bytes(),
            result["report_bytes"],
        )

    def test_report_bytes_are_not_reusable_across_pointer_cores(self):
        import scripts.historical_foundry_verifier as verifier

        observation = _local_connected_engine(
            verifier._connected_request_for_subject(self.subject)
        )
        baseline = verifier._verification_report(observation)
        transplanted = json.loads(json.dumps(observation))
        transplanted["projection"]["pointer_core_sha256"] = "f" * 64
        changed = verifier._verification_report(transplanted)
        self.assertNotEqual(
            json.dumps(baseline, sort_keys=True),
            json.dumps(changed, sort_keys=True),
        )
        self.assertNotEqual(
            baseline["verification_id"], changed["verification_id"]
        )


if __name__ == "__main__":
    unittest.main()
