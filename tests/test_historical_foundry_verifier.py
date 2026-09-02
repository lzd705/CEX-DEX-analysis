"""Tests for historical connected verification and report installation."""

from __future__ import annotations

import errno
import hashlib
from importlib.machinery import SourceFileLoader
import inspect
import json
import multiprocessing
import os
from pathlib import Path
import stat
import tempfile
import types
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
        if not parent.poll(60):
            process.terminate()
            process.join(timeout=5)
            if process.is_alive():
                process.kill()
                process.join(timeout=5)
            raise AssertionError("local connected process timed out")
        observation = parent.recv()
    finally:
        parent.close()
        process.join(timeout=5)
        if process.is_alive():
            process.terminate()
            process.join(timeout=5)
        if process.is_alive():
            process.kill()
            process.join(timeout=5)
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


def _forged_same_name_test_module_worker(queue):
    import scripts.historical_foundry_verifier as verifier
    import sys

    module_name = "tests.test_historical_foundry_verifier"
    genuine = sys.modules.get(module_name)
    fake = types.ModuleType(module_name)
    fake.__file__ = str(Path(__file__).resolve())
    fake.__spec__ = type(genuine.__spec__)(
        module_name,
        SourceFileLoader(module_name, fake.__file__),
        origin=fake.__file__,
    )
    exec(
        "def _local_connected_engine(request):\n"
        "    return {'attacker': True}\n"
        "\n"
        "class HistoricalConnectedVerificationTests:\n"
        "    @classmethod\n"
        "    def setUpClass(cls):\n"
        "        import scripts.historical_foundry_verifier as verifier\n"
        "        verifier._bind_connected_historical_verification_engine(\n"
        "            _local_connected_engine\n"
        "        )\n",
        fake.__dict__,
    )
    sys.modules[module_name] = fake
    try:
        try:
            fake.HistoricalConnectedVerificationTests.setUpClass()
        except BaseException as error:
            rejected = (type(error).__name__, str(error))
        else:
            rejected = None
        try:
            invoked = verifier._invoke_connected_historical_verification_engine(
                {}
            )
        except BaseException as error:
            invocation = (type(error).__name__, str(error))
        else:
            invocation = ("unexpected_success", invoked)
        queue.put((rejected, invocation))
    finally:
        if genuine is None:
            sys.modules.pop(module_name, None)
        else:
            sys.modules[module_name] = genuine


def _mutable_connected_loader_worker(queue):
    import scripts.historical_foundry_verifier as verifier
    import sys

    module_name = "scripts.run_historical_foundry_replay"
    expected_file = Path(verifier.__file__).with_name(
        "run_historical_foundry_replay.py"
    ).resolve()
    module = types.ModuleType(module_name)
    loader = SourceFileLoader(module_name, str(expected_file))
    module.__file__ = str(expected_file)
    module.__spec__ = type(verifier.__spec__)(
        module_name, loader, origin=str(expected_file),
    )
    source = (
        "replacement_calls = 0\n"
        "def _replacement_engine(request):\n"
        "    global replacement_calls\n"
        "    replacement_calls += 1\n"
        "    return {'replacement': True}\n"
        "try:\n"
        "    verifier._bind_connected_historical_verification_engine(\n"
        "        _replacement_engine\n"
        "    )\n"
        "except BaseException as error:\n"
        "    binding = (type(error).__name__, str(error))\n"
        "else:\n"
        "    binding = None\n"
    )
    replacement_code = compile(source, str(expected_file), "exec")
    loader_calls = []

    def replacement_get_filename(name):
        loader_calls.append(("get_filename", name))
        return str(expected_file)

    def replacement_get_code(name):
        loader_calls.append(("get_code", name))
        return replacement_code

    loader.get_filename = replacement_get_filename
    loader.get_code = replacement_get_code
    genuine = sys.modules.get(module_name)
    sys.modules[module_name] = module
    try:
        module.verifier = verifier
        exec(replacement_code, module.__dict__)
        try:
            verifier._invoke_connected_historical_verification_engine({})
        except BaseException as error:
            invocation = (type(error).__name__, str(error))
        else:
            invocation = ("unexpected_success", "")
        queue.put((
            module.binding,
            invocation,
            module.replacement_calls,
            tuple(loader_calls),
        ))
    finally:
        if genuine is None:
            sys.modules.pop(module_name, None)
        else:
            sys.modules[module_name] = genuine


def _mutable_material_loader_worker(queue):
    import scripts.historical_foundry_verifier as verifier
    import sys

    module_name = "scripts.historical_route_publication"
    expected_file = Path(verifier.__file__).with_name(
        "historical_route_publication.py"
    ).resolve()
    module = types.ModuleType(module_name)
    loader = SourceFileLoader(module_name, str(expected_file))
    module.__file__ = str(expected_file)
    module.__spec__ = type(verifier.__spec__)(
        module_name, loader, origin=str(expected_file),
    )
    source = (
        "replacement_calls = 0\n"
        "def _historical_verification_subject_material(*, validated_view):\n"
        "    global replacement_calls\n"
        "    replacement_calls += 1\n"
        "    return {\n"
        "        'validated_view': validated_view,\n"
        "        'data_dir': 'replacement',\n"
        "        'raw_root': 'replacement',\n"
        "        'bundle_path': 'replacement',\n"
        "        'manifest': {},\n"
        "        'bundle': {},\n"
        "        'replay_evidence': {},\n"
        "        'pointer_core': {},\n"
        "    }\n"
        "try:\n"
        "    issue = verifier._bind_historical_verification_subject_material(\n"
        "        _historical_verification_subject_material\n"
        "    )\n"
        "except BaseException as error:\n"
        "    binding = (type(error).__name__, str(error))\n"
        "else:\n"
        "    binding = None\n"
        "    class View:\n"
        "        def close(self):\n"
        "            pass\n"
        "    subject = issue(View())\n"
        "    subject.close()\n"
    )
    replacement_code = compile(source, str(expected_file), "exec")
    loader_calls = []

    def replacement_get_filename(name):
        loader_calls.append(("get_filename", name))
        return str(expected_file)

    def replacement_get_code(name):
        loader_calls.append(("get_code", name))
        return replacement_code

    loader.get_filename = replacement_get_filename
    loader.get_code = replacement_get_code
    genuine = sys.modules.get(module_name)
    sys.modules[module_name] = module
    try:
        module.verifier = verifier
        exec(replacement_code, module.__dict__)
        queue.put((
            module.binding,
            module.replacement_calls,
            tuple(loader_calls),
        ))
    finally:
        if genuine is None:
            sys.modules.pop(module_name, None)
        else:
            sys.modules[module_name] = genuine


class HistoricalVerificationInterfaceTests(unittest.TestCase):
    def test_mode_is_exactly_closed_to_three_plain_strings(self):
        import scripts.historical_foundry_verifier as verifier

        class StringSubclass(str):
            pass

        class EqualityLookalike:
            def __eq__(self, other):
                return other == "publish"

        for mode in ("staged", "publish", "audit"):
            with self.subTest(valid=mode):
                self.assertIs(
                    verifier._require_historical_verification_mode(mode),
                    mode,
                )
        for mode in (
            StringSubclass("publish"), EqualityLookalike(), None, 1,
        ):
            with self.subTest(invalid=repr(mode)):
                with self.assertRaisesRegex(
                    verifier.HistoricalVerificationError,
                    "historical verification mode is invalid",
                ):
                    verifier._require_historical_verification_mode(mode)

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

    def test_forged_same_name_test_module_cannot_bind_connected_engine(self):
        context = multiprocessing.get_context("spawn")
        queue = context.Queue()
        process = context.Process(
            target=_forged_same_name_test_module_worker, args=(queue,)
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

    def test_mutable_loader_methods_cannot_bind_connected_engine(self):
        context = multiprocessing.get_context("spawn")
        queue = context.Queue()
        process = context.Process(
            target=_mutable_connected_loader_worker, args=(queue,)
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
            0,
            (),
        ))

    def test_mutable_loader_methods_cannot_bind_material_reader(self):
        context = multiprocessing.get_context("spawn")
        queue = context.Queue()
        process = context.Process(
            target=_mutable_material_loader_worker, args=(queue,)
        )
        process.start()
        result = queue.get(timeout=30)
        process.join(timeout=30)
        self.assertEqual(process.exitcode, 0)
        self.assertEqual(result, (
            (
                "HistoricalVerificationError",
                "historical verification subject binder is invalid",
            ),
            0,
            (),
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

    def _assert_preexisting_policy(self, path):
        details = os.lstat(str(path))
        self.assertTrue(stat.S_ISREG(details.st_mode))
        self.assertEqual(details.st_nlink, 1)
        self.assertEqual(details.st_uid, os.geteuid())
        self.assertEqual(stat.S_IMODE(details.st_mode), 0o600)

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
        target.chmod(0o600)
        self._assert_preexisting_policy(target)
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
        target.chmod(0o600)
        self._assert_preexisting_policy(target)
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

    def test_report_eexist_hardlink_rejects(self):
        import scripts.historical_foundry_verifier as verifier

        payload = self._report_bytes(marker="hardlink")
        target = self._target(payload)
        target.parent.mkdir(parents=True)
        source = target.parent / "attacker-source.json"
        source.write_bytes(payload)
        source.chmod(0o600)
        os.link(str(source), str(target))
        self.assertEqual(os.lstat(str(source)).st_nlink, 2)
        self.assertEqual(os.lstat(str(target)).st_nlink, 2)
        with self.assertRaisesRegex(
            verifier.HistoricalVerificationError,
            "historical_bundle_invalid",
        ):
            verifier.install_historical_verification_report(
                verification_root=self.root, report_bytes=payload,
            )
        self.assertEqual(source.read_bytes(), payload)
        self.assertEqual(target.read_bytes(), payload)
        self.assertEqual(os.lstat(str(target)).st_nlink, 2)

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
        from tests.test_historical_route_publication import (
            HistoricalCorePublicationTests,
        )

        cls.publication = publication
        cls.helper = HistoricalCorePublicationTests
        cls.run_fixture = cls.finalized = None
        cls.context = cls.subject = None
        lease = core_stage = None
        try:
            cls.run_fixture, cls.finalized, lease, _identity = (
                cls.helper._open_real_task7_lease(
                    include_newer_mixed_rows=True
                )
            )
            core_stage = publication.stage_historical_replay_core(
                data_dir=cls.run_fixture["fixture"].data_dir,
                config=cls.run_fixture["config"],
                publication_lease=lease,
            )
            lease = None
            cls.context = publication.publish_historical_replay_core(
                data_dir=cls.run_fixture["fixture"].data_dir,
                staged_core=core_stage,
            )
            core_stage = None
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
            cls.offline_observation = _local_connected_engine(
                verifier._connected_request_for_subject(cls.subject)
            )
        except BaseException:
            if core_stage is not None:
                core_stage.close()
            if lease is not None:
                lease.close()
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
            cls.helper._close_real_task7_run(
                cls.run_fixture, cls.finalized
            )
            cls.run_fixture = cls.finalized = None

    def _run_with_observation(self, *, mode, observation=None):
        import scripts.historical_foundry_verifier as verifier
        from unittest import mock

        if observation is None:
            observation = self.offline_observation
        detached = json.loads(json.dumps(observation))
        with mock.patch.object(
            verifier, "_invoke_connected_historical_verification_engine",
            return_value=(detached["evidence_mode"], detached),
        ):
            return verifier.run_connected_historical_verification(
                self.subject, mode=mode,
            )

    def _simulated_production_observation(self, marker=None):
        """Exercise post-authority flow under a mock; never bind this fixture."""
        observation = json.loads(json.dumps(self.offline_observation))
        observation["evidence_mode"] = "production_connected"
        if marker is not None:
            observation["provider_identity_sha256"] = hashlib.sha256(
                marker.encode("utf-8")
            ).hexdigest()
        return observation

    @staticmethod
    def _tree_snapshot(root):
        if not root.exists():
            return ()
        rows = []
        for path in sorted(root.rglob("*"), key=lambda value: str(value)):
            details = os.lstat(str(path))
            relative = str(path.relative_to(root))
            if stat.S_ISREG(details.st_mode):
                payload = path.read_bytes()
                digest = hashlib.sha256(payload).hexdigest()
            else:
                digest = None
            rows.append((
                relative, details.st_mode, details.st_nlink,
                details.st_uid, details.st_size,
                getattr(details, "st_mtime_ns", None), digest,
            ))
        return tuple(rows)

    def test_audit_returns_canonical_retained_reference_with_zero_mutation(self):
        import scripts.historical_foundry_verifier as verifier
        from unittest import mock

        historical_root = (
            self.run_fixture["fixture"].data_dir / "routes" / "historical"
        )
        before = self._tree_snapshot(historical_root)
        with mock.patch.object(
            verifier, "_install_historical_verification_report_held",
            side_effect=AssertionError("audit called installer"),
        ):
            result = self._run_with_observation(
                mode="audit",
                observation=self._simulated_production_observation(),
            )
        after = self._tree_snapshot(historical_root)
        self.assertEqual(after, before)
        self.assertEqual(result["mode"], "audit")
        self.assertIsNone(result["install_result"])
        self.assertEqual(result["report"]["status"], "verified")
        self.assertEqual(
            json.dumps(
                json.loads(result["report_bytes"]), sort_keys=True,
                separators=(",", ":"), allow_nan=False,
            ).encode("utf-8"),
            result["report_bytes"],
        )
        retained = (
            verifier._validate_retained_historical_verification_report(
                report_bytes=result["report_bytes"],
                pointer_core=result["pointer_core"],
            )
        )
        self.assertEqual(dict(retained), dict(result["report"]))

    def test_by_sha_directory_same_byte_swap_before_final_pointer_rejects(self):
        import scripts.historical_foundry_verifier as verifier
        from unittest import mock

        real_pointer_core = verifier.historical_replay_pointer_core
        pointer_path = (
            self.run_fixture["fixture"].data_dir / "routes"
            / "historical" / "latest.json"
        )

        def swap_directory(pointer):
            checked = real_pointer_core(pointer)
            by_sha = self.verification_root / "by-sha256"
            target = by_sha / (
                pointer["verification_report_sha256"] + ".json"
            )
            payload = target.read_bytes()
            backup = self.verification_root / ".swapped-by-sha256"
            by_sha.rename(backup)
            by_sha.mkdir(mode=0o700)
            replacement = by_sha / target.name
            replacement.write_bytes(payload)
            replacement.chmod(0o600)
            return checked

        with mock.patch.object(
            verifier, "historical_replay_pointer_core",
            side_effect=swap_directory,
        ):
            with self.assertRaisesRegex(
                verifier.HistoricalVerificationError,
                "historical_bundle_invalid",
            ):
                self._run_with_observation(
                    mode="publish",
                    observation=self._simulated_production_observation(
                        "directory-swap"
                    ),
                )
        self.assertFalse(pointer_path.exists())

    def test_report_same_byte_inode_swap_before_final_pointer_rejects(self):
        import scripts.historical_foundry_verifier as verifier
        from unittest import mock

        real_pointer_core = verifier.historical_replay_pointer_core
        pointer_path = (
            self.run_fixture["fixture"].data_dir / "routes"
            / "historical" / "latest.json"
        )

        def swap_file(pointer):
            checked = real_pointer_core(pointer)
            target = (
                self.verification_root / "by-sha256"
                / (pointer["verification_report_sha256"] + ".json")
            )
            payload = target.read_bytes()
            target.unlink()
            target.write_bytes(payload)
            target.chmod(0o600)
            return checked

        with mock.patch.object(
            verifier, "historical_replay_pointer_core",
            side_effect=swap_file,
        ):
            with self.assertRaisesRegex(
                verifier.HistoricalVerificationError,
                "historical_bundle_invalid",
            ):
                self._run_with_observation(
                    mode="publish",
                    observation=self._simulated_production_observation(
                        "file-swap"
                    ),
                )
        self.assertFalse(pointer_path.exists())

    def test_local_fresh_process_verifies_without_implying_external_rpc(self):
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_verifier as verifier
        from unittest import mock

        verification_before = self._tree_snapshot(self.verification_root)
        with mock.patch.object(
            scan, "select_historical_replay_block",
            side_effect=AssertionError("verifier must not select"),
        ):
            staged = self._run_with_observation(mode="staged")
        self.assertEqual(
            staged["report"]["status"], "structurally_validated"
        )
        self.assertEqual(
            staged["report"]["schema"],
            "route_historical_replay_verification/v1",
        )
        self.assertEqual(
            staged["report"]["evidence_mode"], "offline_test_fixture"
        )
        self.assertEqual(
            staged["report"]["verification_scenario_count"], 12
        )
        self.assertEqual(
            self._tree_snapshot(self.verification_root),
            verification_before,
        )
        self.assertIsNone(staged["install_result"])
        self.assertEqual(
            dict(verifier.historical_replay_pointer_core(
                staged["final_pointer"]
            )),
            self.pointer_core,
        )

        published = self._run_with_observation(
            mode="publish",
            observation=self._simulated_production_observation(),
        )
        self.assertEqual(published["report"]["status"], "verified")
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

    def test_offline_fixture_evidence_is_rejected_for_publish_and_audit(self):
        import scripts.historical_foundry_verifier as verifier

        for mode in ("publish", "audit"):
            with self.subTest(mode=mode):
                with self.assertRaisesRegex(
                    verifier.HistoricalVerificationError,
                    "historical_bundle_invalid",
                ):
                    self._run_with_observation(mode=mode)

    def test_engine_kind_cannot_relabel_fixture_or_production_evidence(self):
        import scripts.historical_foundry_verifier as verifier
        from unittest import mock

        cases = (
            (
                "production_connected",
                json.loads(json.dumps(self.offline_observation)),
            ),
            (
                "offline_test_fixture",
                self._simulated_production_observation(),
            ),
        )
        for engine_kind, observation in cases:
            with self.subTest(engine_kind=engine_kind):
                with mock.patch.object(
                    verifier,
                    "_invoke_connected_historical_verification_engine",
                    return_value=(engine_kind, observation),
                ):
                    with self.assertRaisesRegex(
                        verifier.HistoricalVerificationError,
                        "historical_bundle_invalid",
                    ):
                        verifier.run_connected_historical_verification(
                            self.subject, mode="staged",
                        )

    def test_wrong_scenario_set_and_resolution_transplant_reject(self):
        import scripts.historical_foundry_verifier as verifier

        baseline = json.loads(json.dumps(self.offline_observation))
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

    def test_newer_required_and_safe_exclusion_tamper_matrix(self):
        import scripts.historical_foundry_verifier as verifier

        baseline = json.loads(json.dumps(self.offline_observation))
        projection = baseline["projection"]
        selected_number = projection["selected_block"]["number"]
        newer_required = [
            row for row in projection["prefilter_rows"]
            if row["block_number"] > selected_number
            and row["decision"] == "replay_required"
        ]
        newer_safe = [
            row for row in projection["safe_exclusions"]
            if row["block_number"] > selected_number
        ]
        self.assertEqual(len(newer_required), 2)
        self.assertEqual(len(newer_safe), 8)
        scenario_keys = projection["verification_scenario_keys"]
        result_keys = [
            row["scenario_key"] for row in projection["scenario_results"]
        ]
        self.assertEqual(len(scenario_keys), 12)
        self.assertEqual(result_keys, scenario_keys)
        self.assertTrue({
            row["scenario_key"] for row in newer_required
        }.issubset(set(scenario_keys)))
        self.assertTrue({
            row["scenario_key"] for row in newer_safe
        }.isdisjoint(set(scenario_keys)))

        attacks = {}
        changed_required = json.loads(json.dumps(baseline))
        next(
            row for row in changed_required["projection"]["prefilter_rows"]
            if row["scenario_key"] == newer_required[0]["scenario_key"]
        )["decision"] = "safe_excluded"
        attacks["changed_required_decision"] = changed_required
        changed_safe = json.loads(json.dumps(baseline))
        next(
            row for row in changed_safe["projection"]["prefilter_rows"]
            if row["scenario_key"] == newer_safe[0]["scenario_key"]
        )["decision"] = "replay_required"
        attacks["changed_safe_decision"] = changed_safe
        omitted_key = json.loads(json.dumps(baseline))
        omitted_key["projection"]["verification_scenario_keys"].remove(
            newer_required[0]["scenario_key"]
        )
        attacks["omitted_newer_key"] = omitted_key
        omitted_result = json.loads(json.dumps(baseline))
        omitted_result["projection"]["scenario_results"] = [
            row for row in omitted_result["projection"]["scenario_results"]
            if row["scenario_key"] != newer_required[0]["scenario_key"]
        ]
        attacks["omitted_newer_result"] = omitted_result
        transplanted = json.loads(json.dumps(baseline))
        transplanted_results = transplanted["projection"][
            "scenario_results"
        ]
        next(
            row for row in transplanted_results
            if row["scenario_key"] == newer_required[0]["scenario_key"]
        )["resolution"] = transplanted_results[-1]["resolution"]
        attacks["newer_resolution_transplant"] = transplanted
        typed_mutation = json.loads(json.dumps(baseline))
        next(
            row for row in typed_mutation["projection"]["scenario_results"]
            if row["scenario_key"] == newer_required[0]["scenario_key"]
        )["resolution"]["result_typed_sha256"] = "f" * 64
        attacks["newer_typed_result"] = typed_mutation

        for label, attack in attacks.items():
            with self.subTest(label=label):
                with self.assertRaisesRegex(
                    verifier.HistoricalVerificationError,
                    "historical_bundle_invalid",
                ):
                    verifier._validate_connected_historical_observation(
                        subject=self.subject, observation=attack,
                    )

    def test_connected_raw_source_close_failure_is_not_silenced(self):
        import scripts.historical_foundry_verifier as verifier
        from unittest import mock

        def fail_after_close(source):
            source.close()
            raise RuntimeError("injected connected source close failure")

        with mock.patch.object(
            verifier, "_close_connected_source",
            side_effect=fail_after_close,
        ):
            with self.assertRaisesRegex(
                verifier.HistoricalVerificationError,
                "historical_bundle_invalid",
            ):
                self._run_with_observation(mode="staged")

    def test_report_deletion_or_mutation_before_pointer_construction_rejects(self):
        import scripts.historical_foundry_verifier as verifier
        from unittest import mock

        pointer = (
            self.run_fixture["fixture"].data_dir / "routes"
            / "historical" / "latest.json"
        )
        self.assertFalse(pointer.exists())
        real_install = verifier._install_historical_verification_report_held

        for attack in ("delete", "mutate"):
            with self.subTest(attack=attack):
                def attacked_install(**kwargs):
                    installed, held = real_install(**kwargs)
                    if attack == "delete":
                        installed.path.unlink()
                    else:
                        installed.path.write_bytes(b"attacker report bytes")
                    return installed, held

                with mock.patch.object(
                    verifier,
                    "_install_historical_verification_report_held",
                    side_effect=attacked_install,
                ):
                    with self.assertRaisesRegex(
                        verifier.HistoricalVerificationError,
                        "historical_bundle_invalid",
                    ):
                        self._run_with_observation(
                            mode="publish",
                            observation=self._simulated_production_observation(
                                "report-{}".format(attack)
                            ),
                        )
                self.assertFalse(pointer.exists())

    def test_publish_holds_created_and_matched_report_through_pointer_bytes(self):
        import scripts.historical_foundry_verifier as verifier
        from unittest import mock

        rereads = []
        real_reread = (
            verifier._HeldVerificationReportInstall.reread_unchanged
        )

        def record_reread(held, expected):
            rereads.append(held.filename)
            return real_reread(held, expected)

        observation = self._simulated_production_observation(
            "held-both-dispositions"
        )
        with mock.patch.object(
            verifier._HeldVerificationReportInstall,
            "reread_unchanged", new=record_reread,
        ):
            created = self._run_with_observation(
                mode="publish", observation=observation,
            )
            matched = self._run_with_observation(
                mode="publish", observation=observation,
            )
        self.assertEqual(created["install_result"].disposition, "created")
        self.assertEqual(
            matched["install_result"].disposition, "matched_existing"
        )
        self.assertEqual(len(rereads), 4)
        self.assertTrue(all(name == rereads[0] for name in rereads))

    def test_report_install_precedes_final_pointer_construction(self):
        import scripts.historical_foundry_verifier as verifier
        from unittest import mock

        events = []
        real_install = verifier._install_historical_verification_report_held
        real_pointer_core = verifier.historical_replay_pointer_core
        real_reread = (
            verifier._HeldVerificationReportInstall.reread_unchanged
        )

        def recorded_install(**kwargs):
            events.append("report_installed")
            return real_install(**kwargs)

        def recorded_pointer(pointer):
            events.append("final_pointer_constructed")
            return real_pointer_core(pointer)

        def recorded_reread(held, expected):
            events.append("held_report_reread")
            return real_reread(held, expected)

        with mock.patch.object(
            verifier, "_install_historical_verification_report_held",
            side_effect=recorded_install,
        ), mock.patch.object(
            verifier, "historical_replay_pointer_core",
            side_effect=recorded_pointer,
        ), mock.patch.object(
            verifier._HeldVerificationReportInstall,
            "reread_unchanged", new=recorded_reread,
        ):
            result = self._run_with_observation(
                mode="publish",
                observation=self._simulated_production_observation(
                    "event-order"
                ),
            )
        self.assertEqual(
            events,
            [
                "report_installed", "held_report_reread",
                "final_pointer_constructed", "held_report_reread",
            ],
        )
        self.assertEqual(
            result["install_result"].path.read_bytes(),
            result["report_bytes"],
        )

    def test_report_bytes_are_not_reusable_across_pointer_cores(self):
        import scripts.historical_foundry_verifier as verifier

        observation = self._simulated_production_observation()
        report = verifier._verification_report(observation)
        report_bytes = json.dumps(
            report, sort_keys=True, separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
        retained = verifier._validate_retained_historical_verification_report(
            report_bytes=report_bytes, pointer_core=self.pointer_core,
        )
        self.assertEqual(dict(retained), report)
        transplanted_core = dict(self.pointer_core)
        transplanted_core["replay_id"] = "replay:" + "f" * 64
        with self.assertRaisesRegex(
            verifier.HistoricalVerificationError,
            "historical_bundle_invalid",
        ):
            verifier._validate_retained_historical_verification_report(
                report_bytes=report_bytes,
                pointer_core=transplanted_core,
            )

    def test_retained_report_rejects_added_or_missing_stable_fields(self):
        import scripts.historical_foundry_verifier as verifier

        baseline = verifier._verification_report(
            self._simulated_production_observation()
        )
        attacks = []
        added = dict(baseline)
        added["unexpected"] = "field"
        attacks.append(added)
        missing = dict(baseline)
        missing.pop("coverage_sha256")
        attacks.append(missing)
        for attack in attacks:
            attack.pop("verification_id", None)
            attack["verification_id"] = (
                "verification:" + hashlib.sha256(
                    verifier._canonical_bytes(attack)
                ).hexdigest()
            )
            with self.subTest(fields=tuple(sorted(attack))):
                with self.assertRaisesRegex(
                    verifier.HistoricalVerificationError,
                    "historical_bundle_invalid",
                ):
                    verifier._validate_retained_historical_verification_report(
                        report_bytes=verifier._canonical_bytes(attack),
                        pointer_core=self.pointer_core,
                    )

    def test_audit_report_parity_allows_only_fresh_process_fields(self):
        import scripts.historical_foundry_verifier as verifier

        retained = verifier._verification_report(
            self._simulated_production_observation("same-provider")
        )
        audit = dict(retained)
        audit.update({
            "process_identity_sha256": "7" * 64,
            "connection_identity_sha256": "8" * 64,
            "started_at": "2026-09-02T00:00:00.000000Z",
            "finished_at": "2026-09-02T00:00:01.000000Z",
        })
        audit.pop("verification_id")
        audit["verification_id"] = (
            "verification:" + hashlib.sha256(
                verifier._canonical_bytes(audit)
            ).hexdigest()
        )
        self.assertIsNone(
            verifier._require_historical_audit_report_parity(
                retained_report_bytes=verifier._canonical_bytes(retained),
                audit_report=audit,
            )
        )
        audit["coverage_sha256"] = "9" * 64
        audit.pop("verification_id")
        audit["verification_id"] = (
            "verification:" + hashlib.sha256(
                verifier._canonical_bytes(audit)
            ).hexdigest()
        )
        with self.assertRaisesRegex(
            verifier.HistoricalVerificationError,
            "historical_bundle_invalid",
        ):
            verifier._require_historical_audit_report_parity(
                retained_report_bytes=verifier._canonical_bytes(retained),
                audit_report=audit,
            )


if __name__ == "__main__":
    unittest.main()
