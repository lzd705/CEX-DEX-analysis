"""Tests for the sealed historical replay command entrypoint."""

from __future__ import annotations

import base64
import copy
import contextlib
import hashlib
import importlib
import inspect
import io
import json
import os
import pickle
from pathlib import Path
import py_compile
import socket
import subprocess
import sys
import tempfile
import types
import unittest
from unittest import mock


_EXACT_PYTHON38 = (
    Path(__file__).resolve().parents[2]
    / "cpython-3.8.10-runtime"
    / "bin"
    / "python3.8"
)
_EXPECTED_SAFE_STARTUP_FLAGS = (
    "-E",
    "-s",
    "-S",
    "-B",
    "-X",
    "pycache_prefix=/dev/null",
)
_UNSAFE_STARTUP_MESSAGE = "historical replay startup is unsafe\n"


def _fixed_subprocess_environment():
    return {
        "LANG": "C",
        "LC_ALL": "C",
        "PATH": "/usr/bin:/bin",
    }


def _entrypoint():
    return importlib.import_module("scripts.run_historical_foundry_replay")


class HistoricalReplayPycStartupTests(unittest.TestCase):
    def _compile_ignored_cache(
        self, python, source_path, mode, *, cfile=None
    ):
        cfile_argument = (
            ",cfile={!r}".format(str(cfile)) if cfile is not None else ""
        )
        compiler = (
            "import py_compile;py_compile.compile({!r}{}"
            ",doraise=True,invalidation_mode="
            "py_compile.PycInvalidationMode.{})"
        ).format(str(source_path), cfile_argument, mode)
        completed = subprocess.run(
            [str(python), "-E", "-s", "-S", "-c", compiler],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            text=True,
            env=_fixed_subprocess_environment(),
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)

    def _pyc_attack_fixture(self, root, *, kind):
        scripts = root / "scripts"
        scripts.mkdir()
        (scripts / "__init__.py").write_bytes(b"")
        (root / ".gitignore").write_text(
            "__pycache__/\n", encoding="ascii"
        )
        sentinel = root / "harmless-pyc-sentinel"
        if kind == "timestamp-entrypoint":
            target = scripts / "run_historical_foundry_replay.py"
            malicious = (
                "from pathlib import Path\n"
                "Path({!r}).write_text('timestamp-pyc',encoding='ascii')\n"
            ).format(str(sentinel)).encode("utf-8")
            safe = (b"#" * (len(malicious) - 1)) + b"\n"
            mode = "TIMESTAMP"
        elif kind == "unchecked-lazy":
            (scripts / "run_historical_foundry_replay.py").write_bytes(
                b"import scripts.lazy_module\n"
            )
            target = scripts / "lazy_module.py"
            malicious = (
                "from pathlib import Path\n"
                "Path({!r}).write_text('unchecked-pyc',encoding='ascii')\n"
            ).format(str(sentinel)).encode("utf-8")
            safe = (b"#" * (len(malicious) - 1)) + b"\n"
            mode = "UNCHECKED_HASH"
        else:
            raise AssertionError("unknown local pyc fixture")

        target.write_bytes(malicious)
        fixed_ns = 1_700_000_000_000_000_000
        os.utime(str(target), ns=(fixed_ns, fixed_ns))
        for python in (Path(sys.executable), _EXACT_PYTHON38):
            self._compile_ignored_cache(python, target, mode)
        metadata = target.stat()
        target.write_bytes(safe)
        os.utime(
            str(target),
            ns=(metadata.st_atime_ns, metadata.st_mtime_ns),
        )
        self.assertEqual(target.stat().st_size, len(malicious))

        _git(root, "init", "-q")
        _git(root, "config", "user.name", "Local Test")
        _git(root, "config", "user.email", "local@example.invalid")
        _git(root, "add", ".")
        _git(root, "commit", "-q", "-m", "clean source fixture")
        self.assertEqual(
            _git(root, "status", "--porcelain=v1", "--untracked-files=all"),
            b"",
        )
        return sentinel

    def _run_fixture(self, python, root, *, safe):
        flags = _EXPECTED_SAFE_STARTUP_FLAGS if safe else ("-B",)
        return subprocess.run(
            [str(python)] + list(flags) + [
                "-m", "scripts.run_historical_foundry_replay"
            ],
            cwd=str(root),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            text=True,
            env=_fixed_subprocess_environment(),
        )

    def test_ignored_timestamp_and_unchecked_pyc_attack_and_safe_bypass(self):
        for kind in ("timestamp-entrypoint", "unchecked-lazy"):
            with self.subTest(kind=kind), tempfile.TemporaryDirectory() as name:
                root = Path(name)
                sentinel = self._pyc_attack_fixture(root, kind=kind)
                for python in (Path(sys.executable), _EXACT_PYTHON38):
                    with self.subTest(kind=kind, python=str(python), safe=False):
                        completed = self._run_fixture(
                            python, root, safe=False
                        )
                        self.assertEqual(
                            completed.returncode, 0, completed.stderr
                        )
                        self.assertTrue(sentinel.is_file())
                        sentinel.unlink()
                    with self.subTest(kind=kind, python=str(python), safe=True):
                        completed = self._run_fixture(
                            python, root, safe=True
                        )
                        self.assertEqual(
                            completed.returncode, 0, completed.stderr
                        )
                        self.assertFalse(sentinel.exists())

    def test_tracked_package_marker_preempts_legacy_adjacent_init_pyc(self):
        tracked_init = Path(__file__).resolve().parents[1] / "scripts/__init__.py"
        for python in (Path(sys.executable), _EXACT_PYTHON38):
            with self.subTest(
                python=str(python)
            ), tempfile.TemporaryDirectory() as name:
                root = Path(name)
                scripts = root / "scripts"
                scripts.mkdir()
                sentinel = root / "harmless-legacy-init-pyc-sentinel"
                init_source = scripts / "__init__.py"
                init_source.write_text(
                    "from pathlib import Path\n"
                    "Path({!r}).write_text('legacy-init-pyc',encoding='ascii')\n".format(
                        str(sentinel)
                    ),
                    encoding="utf-8",
                )
                adjacent_cache = scripts / "__init__.pyc"
                self._compile_ignored_cache(
                    python,
                    init_source,
                    "UNCHECKED_HASH",
                    cfile=adjacent_cache,
                )
                init_source.unlink()
                (scripts / "run_historical_foundry_replay.py").write_text(
                    "import scripts\nprint(scripts.__file__)\n",
                    encoding="ascii",
                )

                ordinary = self._run_fixture(python, root, safe=False)
                self.assertEqual(ordinary.returncode, 0, ordinary.stderr)
                self.assertTrue(sentinel.is_file())
                self.assertEqual(
                    Path(ordinary.stdout.strip()).resolve(),
                    adjacent_cache.resolve(),
                )
                sentinel.unlink()

                if tracked_init.is_file():
                    init_source.write_bytes(tracked_init.read_bytes())
                safe = self._run_fixture(python, root, safe=True)
                self.assertEqual(safe.returncode, 0, safe.stderr)
                self.assertFalse(sentinel.exists())
                self.assertEqual(
                    Path(safe.stdout.strip()).resolve(),
                    init_source.resolve(),
                )

    def test_production_startup_contract_is_exact_and_immutable(self):
        module = _entrypoint()
        self.assertEqual(
            module.SAFE_HISTORICAL_REPLAY_STARTUP_FLAGS,
            _EXPECTED_SAFE_STARTUP_FLAGS,
        )
        self.assertIsInstance(
            module.SAFE_HISTORICAL_REPLAY_STARTUP_FLAGS, tuple
        )
        self.assertEqual(module._SAFE_CPYTHON_VERSION, (3, 8, 10))
        self.assertEqual(
            tuple(inspect.signature(
                module._require_safe_historical_startup
            ).parameters),
            (),
        )

    def test_startup_guard_checks_full_exact_python_projection(self):
        module = _entrypoint()
        flags = types.SimpleNamespace(
            debug=0,
            inspect=0,
            interactive=0,
            optimize=0,
            dont_write_bytecode=1,
            no_user_site=1,
            no_site=1,
            ignore_environment=1,
            verbose=0,
            bytes_warning=0,
            quiet=0,
            hash_randomization=1,
            isolated=0,
            dev_mode=False,
            utf8_mode=0,
        )
        version = types.SimpleNamespace(
            major=3,
            minor=8,
            micro=10,
            releaselevel="final",
            serial=0,
        )
        implementation = types.SimpleNamespace(
            name="cpython", cache_tag="cpython-38"
        )
        project_root = str(Path(module.__file__).resolve().parents[1])
        prefix = str(
            Path(project_root).parent / "cpython-3.8.10-runtime"
        )
        expected_path = [
            project_root,
            prefix + "/lib/python38.zip",
            prefix + "/lib/python3.8",
            prefix + "/lib/python3.8/lib-dynload",
        ]
        exact = types.SimpleNamespace(
            flags=flags,
            version_info=version,
            implementation=implementation,
            pycache_prefix="/dev/null",
            _xoptions={"pycache_prefix": "/dev/null"},
            warnoptions=[],
            prefix=prefix,
            base_prefix=prefix,
            exec_prefix=prefix,
            base_exec_prefix=prefix,
            executable=prefix + "/bin/python3.8",
            path=expected_path,
        )
        with mock.patch.object(module, "_startup_sys", exact):
            self.assertTrue(module._trusted_launch_is_exact())
        variants = (
            ("releaselevel", "candidate"),
            ("serial", 1),
        )
        for field, value in variants:
            with self.subTest(field=field):
                changed_version = types.SimpleNamespace(**vars(version))
                setattr(changed_version, field, value)
                changed = types.SimpleNamespace(**vars(exact))
                changed.version_info = changed_version
                with mock.patch.object(module, "_startup_sys", changed):
                    self.assertFalse(module._trusted_launch_is_exact())
        changed_implementation = types.SimpleNamespace(
            name="cpython", cache_tag="cpython-313"
        )
        changed = types.SimpleNamespace(**vars(exact))
        changed.implementation = changed_implementation
        with mock.patch.object(module, "_startup_sys", changed):
            self.assertFalse(module._trusted_launch_is_exact())

        runtime_variants = (
            ("prefix", "/portable/other-runtime"),
            ("base_prefix", "/portable/other-runtime"),
            ("exec_prefix", "/portable/other-runtime"),
            ("base_exec_prefix", "/portable/other-runtime"),
            ("executable", prefix + "/bin/python"),
            ("path", expected_path + ["/injected"]),
            ("path", list(reversed(expected_path))),
        )
        for field, value in runtime_variants:
            with self.subTest(runtime_field=field, value=value):
                changed = types.SimpleNamespace(**vars(exact))
                setattr(changed, field, value)
                with mock.patch.object(module, "_startup_sys", changed):
                    self.assertFalse(module._trusted_launch_is_exact())

        wrong_prefix = "/portable/python-runtime"
        coherent_wrong_runtime = types.SimpleNamespace(**vars(exact))
        for field in (
            "prefix",
            "base_prefix",
            "exec_prefix",
            "base_exec_prefix",
        ):
            setattr(coherent_wrong_runtime, field, wrong_prefix)
        coherent_wrong_runtime.executable = wrong_prefix + "/bin/python3.8"
        coherent_wrong_runtime.path = [
            project_root,
            wrong_prefix + "/lib/python38.zip",
            wrong_prefix + "/lib/python3.8",
            wrong_prefix + "/lib/python3.8/lib-dynload",
        ]
        with mock.patch.object(
            module, "_startup_sys", coherent_wrong_runtime
        ):
            self.assertFalse(module._trusted_launch_is_exact())

        unrelated_prefix = "/portable/cpython-3.8.10-runtime"
        coherent_unrelated_runtime = types.SimpleNamespace(**vars(exact))
        for field in (
            "prefix",
            "base_prefix",
            "exec_prefix",
            "base_exec_prefix",
        ):
            setattr(coherent_unrelated_runtime, field, unrelated_prefix)
        coherent_unrelated_runtime.executable = (
            unrelated_prefix + "/bin/python3.8"
        )
        coherent_unrelated_runtime.path = [
            project_root,
            unrelated_prefix + "/lib/python38.zip",
            unrelated_prefix + "/lib/python3.8",
            unrelated_prefix + "/lib/python3.8/lib-dynload",
        ]
        with mock.patch.object(
            module, "_startup_sys", coherent_unrelated_runtime
        ):
            self.assertFalse(module._trusted_launch_is_exact())

    def test_exact_project_local_runtime_reaches_argparse(self):
        completed = subprocess.run(
            [str(_EXACT_PYTHON38)]
            + list(_EXPECTED_SAFE_STARTUP_FLAGS)
            + [
                "-m",
                "scripts.run_historical_foundry_replay",
                "scan",
                "--data-dir",
                "/tmp/example",
            ],
            cwd=str(Path(__file__).resolve().parents[1]),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            text=True,
            env=_fixed_subprocess_environment(),
        )
        self.assertEqual(completed.returncode, 2)
        self.assertEqual(completed.stdout, "")
        self.assertIn("usage:", completed.stderr)
        self.assertNotIn(_UNSAFE_STARTUP_MESSAGE.strip(), completed.stderr)

    def test_unsafe_clean_module_rejects_before_canonical_import(self):
        module = _entrypoint()
        source_bytes = Path(module.__file__).read_bytes()
        with tempfile.TemporaryDirectory() as name:
            root = Path(name)
            scripts = root / "scripts"
            scripts.mkdir()
            (scripts / "__init__.py").write_bytes(b"")
            (scripts / "run_historical_foundry_replay.py").write_bytes(
                source_bytes
            )
            (root / ".gitignore").write_text(
                "__pycache__/\n", encoding="ascii"
            )
            _git(root, "init", "-q")
            _git(root, "config", "user.name", "Local Test")
            _git(root, "config", "user.email", "local@example.invalid")
            _git(root, "add", ".")
            _git(root, "commit", "-q", "-m", "clean module")
            self.assertEqual(
                _git(
                    root,
                    "status",
                    "--porcelain=v1",
                    "--untracked-files=all",
                ),
                b"",
            )
            unsafe_flag_sets = (
                ("-B",),
                *(
                    tuple(
                        flag
                        for flag in _EXPECTED_SAFE_STARTUP_FLAGS
                        if flag != omitted
                    )
                    for omitted in ("-E", "-s", "-S", "-B")
                ),
                _EXPECTED_SAFE_STARTUP_FLAGS[:-2],
                _EXPECTED_SAFE_STARTUP_FLAGS[:-1] + ("/dev",),
                _EXPECTED_SAFE_STARTUP_FLAGS + ("-O",),
                _EXPECTED_SAFE_STARTUP_FLAGS + ("-X", "dev"),
            )
            for python in (Path(sys.executable), _EXACT_PYTHON38):
                for flags in unsafe_flag_sets:
                    with self.subTest(python=str(python), flags=flags):
                        completed = subprocess.run(
                            [str(python)] + list(flags) + [
                                "-m",
                                "scripts.run_historical_foundry_replay",
                                "scan",
                                "--data-dir",
                                str(root),
                                "--dry-run",
                            ],
                            cwd=str(root),
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            check=False,
                            text=True,
                            env=_fixed_subprocess_environment(),
                        )
                        self.assertEqual(completed.returncode, 1)
                        self.assertEqual(completed.stdout, "")
                        self.assertEqual(
                            completed.stderr, _UNSAFE_STARTUP_MESSAGE
                        )


class HistoricalReplayEntrypointImportAndParserTests(unittest.TestCase):
    def test_canonical_import_binds_the_one_shot_production_engine(self):
        module = _entrypoint()
        import scripts.historical_foundry_verifier as verifier

        self.assertEqual(
            module._production_connected_historical_verification_engine.__module__,
            "scripts.run_historical_foundry_replay",
        )
        self.assertFalse(
            hasattr(verifier, "_bind_connected_historical_verification_engine")
        )
        self.assertFalse(
            hasattr(module, "_bind_connected_historical_verification_engine")
        )
        with self.assertRaisesRegex(
            module.HistoricalReplayEntrypointError,
            "historical production controller is unavailable",
        ):
            with mock.patch.object(
                module, "_trusted_launch_is_exact", return_value=True
            ):
                verifier._invoke_connected_historical_verification_engine({})

    def test_fresh_canonical_import_has_no_operational_io(self):
        source = r'''
import argparse
import base64
import dataclasses
import hashlib
import pathlib
import socket
import subprocess
import scripts.historical_foundry_verifier as verifier
import os

def forbidden(*args, **kwargs):
    raise AssertionError("operational I/O during import")

os.open = forbidden
os.getenv = forbidden
subprocess.run = forbidden
subprocess.Popen = forbidden
socket.socket = forbidden
import scripts.run_historical_foundry_replay as entrypoint
assert entrypoint.__name__ == "scripts.run_historical_foundry_replay"
assert not hasattr(verifier, "_bind_connected_historical_verification_engine")
entrypoint._trusted_launch_is_exact = lambda: True
try:
    verifier._invoke_connected_historical_verification_engine({})
except entrypoint.HistoricalReplayEntrypointError as error:
    assert str(error) == "historical production controller is unavailable"
else:
    raise AssertionError("bound engine did not fail closed")
print("canonical-import-ok")
'''
        completed = subprocess.run(
            [str(_EXACT_PYTHON38)]
            + list(_EXPECTED_SAFE_STARTUP_FLAGS)
            + ["-c", source],
            cwd=str(Path(__file__).resolve().parents[1]),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            text=True,
            env=_fixed_subprocess_environment(),
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(completed.stdout, "canonical-import-ok\n")

    def test_python_m_uses_canonical_trampoline_and_rejects_invalid_cli(self):
        completed = subprocess.run(
            [
                str(_EXACT_PYTHON38),
            ]
            + list(_EXPECTED_SAFE_STARTUP_FLAGS)
            + [
                "-m",
                "scripts.run_historical_foundry_replay",
                "scan",
                "--data-dir",
                "/tmp/example",
            ],
            cwd=str(Path(__file__).resolve().parents[1]),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            text=True,
            env=_fixed_subprocess_environment(),
        )
        self.assertEqual(completed.returncode, 2)
        self.assertEqual(completed.stdout, "")
        self.assertIn("usage:", completed.stderr)

    def test_runpy_main_branch_installs_the_canonical_binding(self):
        source = r'''
import runpy
import sys
project_root = sys.argv[1]
sys.path[0] = project_root
sys.argv = ["scripts.run_historical_foundry_replay", "scan"]
try:
    runpy.run_module("scripts.run_historical_foundry_replay", run_name="__main__")
except SystemExit as error:
    assert error.code == 2
else:
    raise AssertionError("invalid CLI unexpectedly succeeded")
canonical = sys.modules.get("scripts.run_historical_foundry_replay")
assert canonical is not None
import scripts.historical_foundry_verifier as verifier
assert not hasattr(verifier, "_bind_connected_historical_verification_engine")
try:
    verifier._invoke_connected_historical_verification_engine({})
except canonical.HistoricalReplayEntrypointError:
    pass
else:
    raise AssertionError("canonical engine was not bound")
print("module-trampoline-ok")
'''
        completed = subprocess.run(
            [str(_EXACT_PYTHON38)]
            + list(_EXPECTED_SAFE_STARTUP_FLAGS)
            + [
                "-c",
                source,
                str(Path(__file__).resolve().parents[1]),
            ],
            cwd=str(Path(__file__).resolve().parents[1]),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            check=False,
            text=True,
            env=_fixed_subprocess_environment(),
        )
        self.assertEqual(completed.returncode, 0, completed.stderr)
        self.assertEqual(completed.stdout, "module-trampoline-ok\n")

    def test_parser_accepts_only_the_three_exact_forms(self):
        module = _entrypoint()
        cases = (
            (
                ["scan", "--data-dir", "/data", "--publish"],
                {
                    "command": "scan",
                    "data_dir": Path("/data"),
                    "publish": True,
                    "dry_run": False,
                },
            ),
            (
                ["scan", "--data-dir", "/data", "--dry-run"],
                {
                    "command": "scan",
                    "data_dir": Path("/data"),
                    "publish": False,
                    "dry_run": True,
                },
            ),
            (
                ["verify", "--data-dir", "/data", "--bundle", "/immutable"],
                {
                    "command": "verify",
                    "data_dir": Path("/data"),
                    "bundle": Path("/immutable"),
                },
            ),
        )
        for arguments, expected in cases:
            with self.subTest(arguments=arguments):
                parsed = module._parse_arguments(arguments)
                self.assertEqual(vars(parsed), expected)

    def test_parser_rejects_missing_modes_relative_bundle_and_all_overrides(self):
        module = _entrypoint()
        invalid = (
            [],
            ["scan", "--data-dir", "/data"],
            ["scan", "--data-dir", "/data", "--publish", "--dry-run"],
            ["scan", "--data", "/data", "--publish"],
            ["scan", "--data-dir", "/data", "--publish", "--rpc", "x"],
            ["scan", "--data-dir", "/data", "--publish", "--block", "1"],
            ["scan", "--data-dir", "/data", "--publish", "extra"],
            ["verify", "--data-dir", "/data", "--bundle", "relative"],
            ["verify", "--data-dir", "/data", "--bundl", "/immutable"],
            ["verify", "--data-dir", "/data", "--bundle", "/immutable", "--publish"],
            ["verify", "--data-dir", "/data", "--bundle", "/immutable", "--member", "x"],
            ["scan", "--data-dir", "/data", "--data-dir", "/other", "--publish"],
            ["scan", "--data-dir", "/data", "--publish", "--publish"],
            ["scan", "--data-dir=/data", "--publish"],
            ["scan", "--data-dir", "/data", "--publish=true"],
            [
                "verify", "--data-dir", "/data", "--bundle", "/immutable",
                "--bundle", "/other",
            ],
            ["verify", "--data-dir=/data", "--bundle", "/immutable"],
            ["verify", "--data-dir", "/data", "--bundle=/immutable"],
        )
        for arguments in invalid:
            with self.subTest(arguments=arguments):
                with contextlib.redirect_stderr(io.StringIO()):
                    with self.assertRaises(SystemExit) as raised:
                        module._parse_arguments(arguments)
                self.assertEqual(raised.exception.code, 2)

    def test_invalid_cli_does_not_reach_environment_process_or_filesystem(self):
        module = _entrypoint()
        forbidden = mock.Mock(side_effect=AssertionError("operational I/O"))
        with mock.patch.object(module, "verify_clean_tracked_historical_source", forbidden), mock.patch.object(
            module, "capture_live_pointer_snapshots", forbidden
        ), mock.patch.object(module, "_invoke_production_controller", forbidden), mock.patch.object(
            module.os, "open", forbidden
        ), mock.patch.object(module.os, "getenv", forbidden), mock.patch.object(
            module.subprocess, "run", forbidden
        ), mock.patch.object(socket, "socket", forbidden), contextlib.redirect_stderr(
            io.StringIO()
        ):
            with self.assertRaises(SystemExit) as raised:
                module._parse_arguments(["scan", "--data-dir", "/data"])
        self.assertEqual(raised.exception.code, 2)
        forbidden.assert_not_called()

    def test_valid_cli_fails_closed_without_production_evidence(self):
        module = _entrypoint()

        class HeldSource:
            @property
            def identity_projection(self):
                return {"schema": "test-only-preflight"}

            def reread_unchanged(self):
                return None

            def close(self):
                return None

        with tempfile.TemporaryDirectory() as name, mock.patch.object(
            module,
            "verify_clean_tracked_historical_source",
            return_value=HeldSource(),
        ), mock.patch.object(
            module, "_trusted_launch_is_exact", return_value=True
        ), contextlib.redirect_stdout(io.StringIO()) as stdout, contextlib.redirect_stderr(
            io.StringIO()
        ) as stderr:
            result = module.main(
                ["scan", "--data-dir", name, "--dry-run"]
            )
        self.assertEqual(result, 1)
        self.assertEqual(stdout.getvalue(), "")
        self.assertEqual(
            stderr.getvalue(),
            "historical production controller is unavailable\n",
        )


class HistoricalReplayLivePointerTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.data_dir = Path(self.temporary.name) / "market-data"
        self.data_dir.mkdir()
        self.module = _entrypoint()

    def tearDown(self):
        self.temporary.cleanup()

    def _write(self, relative_path, payload):
        target = self.data_dir / relative_path
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(payload)
        return target

    def test_live_pointer_dataclass_contract_is_exact_and_frozen(self):
        fields = tuple(self.module.LivePointerSnapshot.__dataclass_fields__)
        self.assertEqual(
            fields,
            ("relative_path", "present", "size", "sha256", "bytes_value"),
        )
        snapshot = self.module.LivePointerSnapshot(
            "routes/latest.json", False, None, None, None
        )
        with self.assertRaises(Exception):
            snapshot.present = True
        self.assertEqual(
            tuple(inspect.signature(
                self.module.capture_live_pointer_snapshots
            ).parameters),
            ("data_dir",),
        )

    def test_both_absent_are_ordered_and_missing_parents_are_not_created(self):
        snapshots = self.module.capture_live_pointer_snapshots(
            data_dir=self.data_dir
        )
        self.assertEqual(
            snapshots,
            (
                self.module.LivePointerSnapshot(
                    "routes/core/latest.json", False, None, None, None
                ),
                self.module.LivePointerSnapshot(
                    "routes/latest.json", False, None, None, None
                ),
            ),
        )
        self.assertFalse((self.data_dir / "routes").exists())

    def test_both_present_each_one_absent_and_invalid_bytes_are_captured_raw(self):
        invalid = b"\xffnot canonical json\x00"
        core = self._write("routes/core/latest.json", invalid)
        complete = self._write("routes/latest.json", b"{}\n")
        both = self.module.capture_live_pointer_snapshots(data_dir=self.data_dir)
        self.assertEqual(both[0].bytes_value, invalid)
        self.assertEqual(both[0].size, len(invalid))
        self.assertEqual(both[0].sha256, hashlib.sha256(invalid).hexdigest())
        self.assertEqual(both[1].bytes_value, b"{}\n")

        core.unlink()
        only_complete = self.module.capture_live_pointer_snapshots(
            data_dir=self.data_dir
        )
        self.assertFalse(only_complete[0].present)
        self.assertTrue(only_complete[1].present)

        core.write_bytes(b"core")
        complete.unlink()
        only_core = self.module.capture_live_pointer_snapshots(
            data_dir=self.data_dir
        )
        self.assertTrue(only_core[0].present)
        self.assertFalse(only_core[1].present)

    def test_projection_has_exact_fields_and_round_trips_bytes(self):
        payload = b"\x00\xffpointer"
        self._write("routes/core/latest.json", payload)
        snapshots = self.module.capture_live_pointer_snapshots(
            data_dir=self.data_dir
        )
        projection = self.module.project_live_pointer_snapshots(snapshots)
        self.assertEqual(len(projection), 2)
        self.assertEqual(
            tuple(projection[0]),
            ("relative_path", "present", "size", "sha256", "bytes_base64"),
        )
        self.assertEqual(base64.b64decode(projection[0]["bytes_base64"]), payload)
        self.assertEqual(
            hashlib.sha256(base64.b64decode(projection[0]["bytes_base64"])).hexdigest(),
            projection[0]["sha256"],
        )
        self.assertEqual(
            projection[1],
            {
                "relative_path": "routes/latest.json",
                "present": False,
                "size": None,
                "sha256": None,
                "bytes_base64": None,
            },
        )

    def test_same_size_replacement_between_snapshots_is_rejected(self):
        target = self._write("routes/latest.json", b"before")
        before = self.module.capture_live_pointer_snapshots(
            data_dir=self.data_dir
        )
        replacement = target.with_name("replacement")
        replacement.write_bytes(b"after!")
        os.replace(str(replacement), str(target))
        after = self.module.capture_live_pointer_snapshots(data_dir=self.data_dir)
        with self.assertRaisesRegex(
            self.module.HistoricalReplayEntrypointError,
            "live pointer changed",
        ):
            self.module.require_live_pointer_snapshots_unchanged(before, after)

    def test_capture_rejects_byte_identical_inode_swap_during_read(self):
        target = self._write("routes/latest.json", b"same bytes")
        replacement = target.with_name("replacement")
        replacement.write_bytes(b"same bytes")
        original = self.module._read_descriptor_bounded
        swapped = {"done": False}

        def swap_after_read(descriptor, maximum):
            payload = original(descriptor, maximum)
            if not swapped["done"]:
                swapped["done"] = True
                os.replace(str(replacement), str(target))
            return payload

        with mock.patch.object(
            self.module, "_read_descriptor_bounded", side_effect=swap_after_read
        ):
            with self.assertRaises(self.module.HistoricalReplayEntrypointError):
                self.module.capture_live_pointer_snapshots(data_dir=self.data_dir)

    def test_batch_capture_rejects_first_pointer_swap_during_second_read(self):
        core = self._write("routes/core/latest.json", b"core-before")
        self._write("routes/latest.json", b"complete")
        replacement = core.with_name("replacement")
        replacement.write_bytes(b"core-after!")
        original = self.module._read_descriptor_bounded
        reads = {"count": 0}

        def replace_first_after_second_read(descriptor, maximum):
            payload = original(descriptor, maximum)
            reads["count"] += 1
            if reads["count"] == 2:
                os.replace(str(replacement), str(core))
            return payload

        with mock.patch.object(
            self.module,
            "_read_descriptor_bounded",
            side_effect=replace_first_after_second_read,
        ):
            with self.assertRaises(self.module.HistoricalReplayEntrypointError):
                self.module.capture_live_pointer_snapshots(data_dir=self.data_dir)

    def test_batch_capture_rejects_first_absence_becoming_present(self):
        core = self.data_dir / "routes" / "core" / "latest.json"
        core.parent.mkdir(parents=True)
        self._write("routes/latest.json", b"complete")
        original = self.module._read_descriptor_bounded

        def create_absent_first_after_second_read(descriptor, maximum):
            payload = original(descriptor, maximum)
            core.write_bytes(b"appeared")
            return payload

        with mock.patch.object(
            self.module,
            "_read_descriptor_bounded",
            side_effect=create_absent_first_after_second_read,
        ):
            with self.assertRaises(self.module.HistoricalReplayEntrypointError):
                self.module.capture_live_pointer_snapshots(data_dir=self.data_dir)

    def test_missing_routes_create_delete_is_detected_by_parent_generation(self):
        real_stat = self.module.os.stat
        observed = {"routes": 0, "mutated": False}

        def create_delete_before_absence_recheck(path, *args, **kwargs):
            if path == "routes" and kwargs.get("dir_fd") is not None:
                observed["routes"] += 1
                if observed["routes"] == 2:
                    routes = self.data_dir / "routes"
                    routes.mkdir()
                    routes.rmdir()
                    observed["mutated"] = True
            return real_stat(path, *args, **kwargs)

        with mock.patch.object(
            self.module.os,
            "stat",
            side_effect=create_delete_before_absence_recheck,
        ):
            with self.assertRaises(self.module.HistoricalReplayEntrypointError):
                self.module.capture_live_pointer_snapshots(
                    data_dir=self.data_dir
                )
        self.assertTrue(observed["mutated"])

    def test_symlink_hardlink_and_nonregular_leaf_are_rejected(self):
        outside = Path(self.temporary.name) / "outside"
        outside.write_bytes(b"outside")
        cases = ("symlink", "hardlink", "directory")
        for case in cases:
            with self.subTest(case=case):
                with tempfile.TemporaryDirectory() as name:
                    data_dir = Path(name)
                    routes = data_dir / "routes"
                    routes.mkdir()
                    target = routes / "latest.json"
                    if case == "symlink":
                        target.symlink_to(outside)
                    elif case == "hardlink":
                        os.link(str(outside), str(target))
                    else:
                        target.mkdir()
                    with self.assertRaises(
                        self.module.HistoricalReplayEntrypointError
                    ):
                        self.module.capture_live_pointer_snapshots(data_dir=data_dir)

    def test_pointer_read_is_bounded(self):
        self._write(
            "routes/latest.json",
            b"x" * (self.module._MAX_LIVE_POINTER_BYTES + 1),
        )
        with self.assertRaises(self.module.HistoricalReplayEntrypointError):
            self.module.capture_live_pointer_snapshots(data_dir=self.data_dir)

    def test_ancestor_symlink_is_rejected(self):
        real_root = Path(self.temporary.name) / "real"
        data_dir = real_root / "data"
        data_dir.mkdir(parents=True)
        alias = Path(self.temporary.name) / "alias"
        alias.symlink_to(real_root, target_is_directory=True)
        with self.assertRaises(self.module.HistoricalReplayEntrypointError):
            self.module.capture_live_pointer_snapshots(
                data_dir=alias / "data"
            )

    def test_cloexec_is_mandatory_and_every_descriptor_is_noninheritable(self):
        with mock.patch.object(self.module.os, "O_CLOEXEC", None):
            with self.assertRaises(self.module.HistoricalReplayEntrypointError):
                self.module.capture_live_pointer_snapshots(data_dir=self.data_dir)
        with mock.patch.object(
            self.module.os, "get_inheritable", return_value=True
        ) as inheritable:
            with self.assertRaises(self.module.HistoricalReplayEntrypointError):
                self.module.capture_live_pointer_snapshots(data_dir=self.data_dir)
        self.assertTrue(inheritable.called)

    def test_early_descriptor_cleanup_is_best_effort_and_preserves_control(self):
        nested = self.data_dir / "one" / "two" / "three"
        nested.mkdir(parents=True)
        real_open = self.module.os.open
        real_close = self.module.os.close
        opened = []
        close_attempts = []
        control = KeyboardInterrupt("open control")

        def fail_after_three_opens(name, flags, *args, **kwargs):
            if len(opened) == 3:
                raise control
            descriptor = real_open(name, flags, *args, **kwargs)
            opened.append(descriptor)
            return descriptor

        def close_all_but_raise_on_first(descriptor):
            close_attempts.append(descriptor)
            real_close(descriptor)
            if len(close_attempts) == 1:
                raise RuntimeError("first close failed")

        try:
            with mock.patch.object(
                self.module.os, "open", side_effect=fail_after_three_opens
            ), mock.patch.object(
                self.module.os,
                "close",
                side_effect=close_all_but_raise_on_first,
            ):
                with self.assertRaises(KeyboardInterrupt) as raised:
                    self.module._open_absolute_directory_chain(nested)
            self.assertIs(raised.exception, control)
            self.assertCountEqual(close_attempts, opened)
        finally:
            for descriptor in opened:
                try:
                    real_close(descriptor)
                except OSError:
                    pass

    def test_held_pointer_cleanup_does_not_mask_control_exception(self):
        routes = self.data_dir / "routes"
        routes.mkdir()
        (routes / "latest.json").write_bytes(b"pointer")
        parent_descriptor = os.open(
            str(routes), self.module._secure_directory_flags()
        )
        real_close = self.module.os.close
        close_attempts = []
        control = GeneratorExit("read control")

        def close_then_fail(descriptor):
            close_attempts.append(descriptor)
            real_close(descriptor)
            raise RuntimeError("close failed")

        try:
            with mock.patch.object(
                self.module,
                "_read_descriptor_bounded",
                side_effect=control,
            ), mock.patch.object(
                self.module.os, "close", side_effect=close_then_fail
            ):
                with self.assertRaises(GeneratorExit) as raised:
                    self.module._open_held_pointer(
                        relative_path="routes/latest.json",
                        parent_descriptor=parent_descriptor,
                        leaf_name="latest.json",
                    )
            self.assertIs(raised.exception, control)
            self.assertEqual(len(close_attempts), 1)
        finally:
            real_close(parent_descriptor)


class HistoricalReplayOutermostGuardTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.data_dir = Path(self.temporary.name)
        self.module = _entrypoint()

    def tearDown(self):
        self.temporary.cleanup()

    def test_guard_recaptures_and_compares_on_success(self):
        real_capture = self.module.capture_live_pointer_snapshots
        with mock.patch.object(
            self.module,
            "capture_live_pointer_snapshots",
            wraps=real_capture,
        ) as capture:
            guard = self.module._LivePointerGuard(data_dir=self.data_dir)
            with guard:
                pass
        self.assertEqual(capture.call_count, 2)
        self.assertEqual(guard.before, guard.after)

    def test_guard_recaptures_and_compares_in_exception_finally_path(self):
        real_capture = self.module.capture_live_pointer_snapshots
        with mock.patch.object(
            self.module,
            "capture_live_pointer_snapshots",
            wraps=real_capture,
        ) as capture:
            with self.assertRaisesRegex(RuntimeError, "controller failed"):
                with self.module._LivePointerGuard(data_dir=self.data_dir):
                    raise RuntimeError("controller failed")
        self.assertEqual(capture.call_count, 2)

    def test_guard_rejects_pointer_mutation_even_while_unwinding_failure(self):
        target = self.data_dir / "routes" / "latest.json"
        target.parent.mkdir(parents=True)
        target.write_bytes(b"before")
        with self.assertRaisesRegex(
            self.module.HistoricalReplayEntrypointError,
            "live pointer changed",
        ):
            with self.module._LivePointerGuard(data_dir=self.data_dir):
                target.write_bytes(b"after!")
                raise RuntimeError("controller failed")

    def test_guard_rejects_stale_after_batch_capture(self):
        core = self.data_dir / "routes" / "core" / "latest.json"
        complete = self.data_dir / "routes" / "latest.json"
        core.parent.mkdir(parents=True)
        core.write_bytes(b"core-before")
        complete.write_bytes(b"complete")
        replacement = core.with_name("replacement")
        replacement.write_bytes(b"core-after!")
        original = self.module._read_descriptor_bounded
        reads = {"count": 0}

        def attack_only_after_snapshot(descriptor, maximum):
            payload = original(descriptor, maximum)
            reads["count"] += 1
            if reads["count"] == 4:
                os.replace(str(replacement), str(core))
            return payload

        with mock.patch.object(
            self.module,
            "_read_descriptor_bounded",
            side_effect=attack_only_after_snapshot,
        ):
            with self.assertRaises(self.module.HistoricalReplayEntrypointError):
                with self.module._LivePointerGuard(data_dir=self.data_dir):
                    pass

    def test_guard_preserves_ordinary_and_control_flow_matrix(self):
        controls = (KeyboardInterrupt, SystemExit, GeneratorExit)
        for error_type in controls:
            with self.subTest(control=error_type.__name__, guard="success"):
                with self.assertRaises(error_type):
                    with self.module._LivePointerGuard(data_dir=self.data_dir):
                        raise error_type()

        for error_type in controls:
            with self.subTest(control=error_type.__name__, guard="failure"):
                target = self.data_dir / "routes" / "latest.json"
                target.parent.mkdir(parents=True, exist_ok=True)
                target.write_bytes(b"before")
                with self.assertRaises(error_type):
                    with self.module._LivePointerGuard(data_dir=self.data_dir):
                        target.write_bytes(b"after!")
                        raise error_type()
                target.write_bytes(b"before")

        with self.subTest(ordinary="guard-success"):
            with self.assertRaisesRegex(RuntimeError, "ordinary"):
                with self.module._LivePointerGuard(data_dir=self.data_dir):
                    raise RuntimeError("ordinary")
        with self.subTest(ordinary="guard-failure"):
            target = self.data_dir / "routes" / "latest.json"
            target.write_bytes(b"changed")
            with self.assertRaises(self.module.HistoricalReplayEntrypointError):
                with self.module._LivePointerGuard(data_dir=self.data_dir):
                    target.write_bytes(b"changed-again")
                    raise RuntimeError("ordinary")


_TRACKED_FIXTURE_BYTES = {
    "config/historical_foundry_replay_policy.json": b'{"policy":1}\n',
    "config/historical_foundry_replay_authority.json": b'{"authority":1}\n',
    "config/historical_foundry_replay_toolchain.json": b'{"toolchain":1}\n',
    "foundry/src/TwoVenueV2Executor.sol": b"contract Executor {}\n",
    "foundry/test/TwoVenueV2Unit.t.sol": b"contract UnitTest {}\n",
    "foundry/test/TwoVenueV2Fork.t.sol": b"contract ForkTest {}\n",
    "foundry.toml": b"[profile.default]\n",
    "foundry.lock": b'{"lock":1}\n',
    ".gitmodules": b'[submodule "lib/forge-std"]\npath = lib/forge-std\n',
}

_EXPECTED_PRODUCTION_PYTHON_PATHS = (
    "scripts/__init__.py",
    "scripts/atomic_publication.py",
    "scripts/bootstrap_historical_foundry_toolchain.py",
    "scripts/bounded_json.py",
    "scripts/bounded_snapshot_merge.py",
    "scripts/cex_fee_facts.py",
    "scripts/cex_instrument_lifecycle.py",
    "scripts/collection_deadline.py",
    "scripts/execution_cost.py",
    "scripts/execution_cost_components.py",
    "scripts/fact_quality.py",
    "scripts/fetch_cex.py",
    "scripts/fetch_cex_depth.py",
    "scripts/fetch_dex_depth.py",
    "scripts/fetch_tvl.py",
    "scripts/historical_foundry_anvil.py",
    "scripts/historical_foundry_contracts.py",
    "scripts/historical_foundry_replay.py",
    "scripts/historical_foundry_rpc.py",
    "scripts/historical_foundry_scan.py",
    "scripts/historical_foundry_storage.py",
    "scripts/historical_foundry_verifier.py",
    "scripts/historical_route_publication.py",
    "scripts/market_lifecycle_reviews.py",
    "scripts/publication_gate.py",
    "scripts/quality_outcomes.py",
    "scripts/route_cohort.py",
    "scripts/route_cost_evidence.py",
    "scripts/route_cost_topology.py",
    "scripts/route_inventory.py",
    "scripts/route_opportunity.py",
    "scripts/route_publication.py",
    "scripts/route_quantity.py",
    "scripts/route_shadow_audit.py",
    "scripts/route_shadow_inputs.py",
    "scripts/route_universe.py",
    "scripts/run_historical_foundry_replay.py",
    "scripts/timestamp_contract.py",
    "scripts/token_registry.py",
)

_TRACKED_FIXTURE_BYTES.update({
    relative_path: (
        "# fixed local fixture for {}\n".format(relative_path)
    ).encode("ascii")
    for relative_path in _EXPECTED_PRODUCTION_PYTHON_PATHS
})


def _git(repository, *arguments):
    completed = subprocess.run(
        ["/usr/bin/git", "-C", str(repository)] + list(arguments),
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=False,
    )
    if completed.returncode != 0:
        raise AssertionError(
            "git failed: {}".format(completed.stderr.decode("utf-8", "replace"))
        )
    return completed.stdout


class _FakeReviewedToolchain:
    def __init__(
        self,
        project_root,
        forge_std_commit,
        *,
        fail_at=None,
        after_project_identity=None,
        exit_failure=None,
    ):
        self._project_root = project_root
        self._forge_std_commit = forge_std_commit
        self.versions_checked = 0
        self.events = []
        self.fail_at = fail_at
        self.after_project_identity = after_project_identity
        self.exit_failure = exit_failure

    def _record(self, event):
        self.events.append(event)
        if self.fail_at == event:
            raise RuntimeError("injected toolchain {} failure".format(event))

    def __enter__(self):
        self._record("enter")
        return self

    def __exit__(self, _type, _value, _traceback):
        self.events.append("exit")
        if self.exit_failure is not None:
            raise self.exit_failure
        return None

    def _verify_versions_and_hardfork(self):
        self._record("versions")
        self.versions_checked += 1
        return None

    @property
    def verified_identity(self):
        self._record("identity")
        return {
            "schema": "historical_foundry_toolchain_candidate/v1",
            "source_lock_sha256": "9" * 64,
            "foundry_release": {
                "archive_url": "https://must-not-escape.invalid/secret",
            },
            "binaries": [
                {"name": "forge", "sha256": "1" * 64, "version": "v1.7.1"},
                {"name": "cast", "sha256": "2" * 64, "version": "v1.7.1"},
                {"name": "anvil", "sha256": "3" * 64, "version": "v1.7.1"},
            ],
            "solc": {
                "artifact_url": "https://must-not-escape.invalid/secret",
                "sha256": "4" * 64,
                "version": "0.8.36+commit.8a079791",
            },
        }

    def verified_project_input_identity(self):
        self._record("project_identity")
        result = {
            "schema": "historical_foundry_project_input_identity/v1",
            "foundry_toml_sha256": hashlib.sha256(
                _TRACKED_FIXTURE_BYTES["foundry.toml"]
            ).hexdigest(),
            "foundry_lock_sha256": hashlib.sha256(
                _TRACKED_FIXTURE_BYTES["foundry.lock"]
            ).hexdigest(),
            "gitmodules_sha256": hashlib.sha256(
                _TRACKED_FIXTURE_BYTES[".gitmodules"]
            ).hexdigest(),
            "forge_std_commit": self._forge_std_commit,
            "forge_std_tree_sha256": "5" * 64,
        }
        if self.after_project_identity is not None:
            callback = self.after_project_identity
            self.after_project_identity = None
            callback()
        return result


class _ProcessProxy:
    def __init__(self, process):
        self._process = process
        self.communicate_called = False

    def __getattr__(self, name):
        return getattr(self._process, name)

    def __enter__(self):
        self._process.__enter__()
        return self

    def __exit__(self, exc_type, value, traceback):
        return self._process.__exit__(exc_type, value, traceback)

    def communicate(self, *args, **kwargs):
        self.communicate_called = True
        return self._process.communicate(*args, **kwargs)


def _plain_json_value(value):
    if isinstance(value, dict) or hasattr(value, "items"):
        return {key: _plain_json_value(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_plain_json_value(item) for item in value]
    return value


class HistoricalReplayCleanSourcePreflightTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.project_root = Path(self.temporary.name) / "project"
        self.project_root.mkdir()
        self.forge_std = self.project_root / "lib" / "forge-std"
        self.forge_std.mkdir(parents=True)

        _git(self.forge_std, "init", "-q")
        _git(self.forge_std, "config", "user.name", "Local Test")
        _git(self.forge_std, "config", "user.email", "local@example.invalid")
        (self.forge_std / "README.md").write_bytes(b"forge std fixture\n")
        _git(self.forge_std, "add", "README.md")
        _git(self.forge_std, "commit", "-q", "-m", "fixture")
        self.forge_std_commit = _git(
            self.forge_std, "rev-parse", "HEAD"
        ).decode("ascii").strip()

        for relative_path, payload in _TRACKED_FIXTURE_BYTES.items():
            target = self.project_root / relative_path
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(payload)
        _git(self.project_root, "init", "-q")
        _git(self.project_root, "config", "user.name", "Local Test")
        _git(self.project_root, "config", "user.email", "local@example.invalid")
        _git(self.project_root, "add", ".")
        _git(self.project_root, "commit", "-q", "-m", "fixture")
        self.repository_head = _git(
            self.project_root, "rev-parse", "HEAD"
        ).decode("ascii").strip()
        self.module = _entrypoint()
        self.toolchain = _FakeReviewedToolchain(
            self.project_root, self.forge_std_commit
        )
        self.patches = (
            mock.patch.object(self.module, "_PROJECT_ROOT", self.project_root),
            mock.patch.object(
                self.module,
                "_FORGE_STD_COMMIT",
                self.forge_std_commit,
            ),
            mock.patch.object(
                self.module,
                "_open_reviewed_historical_toolchain",
                return_value=self.toolchain,
            ),
            mock.patch.object(
                self.module,
                "_trusted_launch_is_exact",
                return_value=True,
            ),
        )
        for patch in self.patches:
            patch.start()

    def tearDown(self):
        for patch in reversed(self.patches):
            patch.stop()
        self.temporary.cleanup()

    def _real_git_with_popen(self, popen, *arguments):
        process = popen(
            ("/usr/bin/git", "-C", str(self.project_root)) + arguments,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        stdout, stderr = process.communicate()
        if process.returncode != 0:
            raise AssertionError(stderr.decode("utf-8", "replace"))
        return stdout

    def test_clean_preflight_is_sealed_and_returns_only_exact_safe_identity(self):
        self.assertEqual(
            tuple(inspect.signature(
                self.module.verify_clean_tracked_historical_source
            ).parameters),
            (),
        )
        source = self.module.verify_clean_tracked_historical_source()
        self.addCleanup(source.close)
        result = source.identity_projection
        self.assertEqual(self.toolchain.versions_checked, 1)
        self.assertEqual(
            self.toolchain.events,
            ["enter", "versions", "identity", "project_identity"],
        )
        self.assertEqual(
            tuple(result),
            (
                "schema",
                "repository_head",
                "tracked_source",
                "forge_std",
                "toolchain",
            ),
        )
        self.assertEqual(
            result["schema"],
            "historical_foundry_clean_source_preflight/v1",
        )
        self.assertEqual(result["repository_head"], self.repository_head)
        self.assertEqual(
            tuple(row["name"] for row in result["tracked_source"]),
            (
                "policy",
                "authority",
                "toolchain_config",
                "executor",
                "unit_test",
                "fork_test",
                "foundry_toml",
                "foundry_lock",
                "gitmodules",
                "source:scripts_package",
            ) + tuple(
                "source:" + Path(relative_path).stem
                for relative_path in _EXPECTED_PRODUCTION_PYTHON_PATHS[1:]
            ),
        )
        self.assertTrue(all(tuple(row) == ("name", "size", "sha256") for row in result["tracked_source"]))
        self.assertEqual(
            result["forge_std"],
            {
                "commit": self.forge_std_commit,
                "tree_sha256": "5" * 64,
            },
        )
        self.assertEqual(
            result["toolchain"],
            (
                {"name": "forge", "sha256": "1" * 64, "version": "v1.7.1"},
                {"name": "anvil", "sha256": "3" * 64, "version": "v1.7.1"},
                {"name": "cast", "sha256": "2" * 64, "version": "v1.7.1"},
                {
                    "name": "solc",
                    "sha256": "4" * 64,
                    "version": "0.8.36+commit.8a079791",
                },
            ),
        )
        with self.assertRaises(TypeError):
            result["repository_head"] = "0" * 40
        serialized = json.dumps(_plain_json_value(result))
        self.assertNotIn(str(self.project_root), serialized)
        self.assertNotIn("must-not-escape", serialized)
        self.assertNotIn("secret", serialized.lower())
        self.assertNotIn("url", serialized.lower())
        self.assertNotIn('"path"', serialized.lower())
        source.reread_unchanged()
        source.close()
        self.assertEqual(self.toolchain.events[-1], "exit")
        with self.assertRaises(self.module.HistoricalReplayEntrypointError):
            source.reread_unchanged()

    def test_production_python_source_inventory_is_fixed_and_complete(self):
        self.assertEqual(
            self.module._PRODUCTION_PYTHON_SOURCE_PATHS,
            _EXPECTED_PRODUCTION_PYTHON_PATHS,
        )
        self.assertEqual(
            tuple(
                relative_path
                for role, relative_path in self.module._TRACKED_SOURCE_INVENTORY
                if role.startswith("source:")
            ),
            _EXPECTED_PRODUCTION_PYTHON_PATHS,
        )
        self.assertEqual(
            len(_EXPECTED_PRODUCTION_PYTHON_PATHS),
            len(set(_EXPECTED_PRODUCTION_PYTHON_PATHS)),
        )
        for required in (
            "scripts/__init__.py",
            "scripts/run_historical_foundry_replay.py",
            "scripts/historical_foundry_scan.py",
            "scripts/historical_foundry_rpc.py",
            "scripts/historical_foundry_storage.py",
            "scripts/historical_foundry_replay.py",
            "scripts/historical_route_publication.py",
            "scripts/historical_foundry_verifier.py",
        ):
            self.assertIn(required, _EXPECTED_PRODUCTION_PYTHON_PATHS)

    def test_lazy_source_inode_swap_is_rejected_before_controller_runs(self):
        target = self.project_root / "scripts/historical_foundry_scan.py"
        original = target.read_bytes()
        replacement = b"LAZY_LOADED = True\n".ljust(len(original), b" ")
        self.assertEqual(len(replacement), len(original))
        self.assertNotEqual(replacement, original)
        backup = target.with_name("historical_foundry_scan.py.held-original")
        real_verify = self.module.verify_clean_tracked_historical_source
        controller = mock.Mock()

        def verify_then_swap():
            source = real_verify()
            os.replace(str(target), str(backup))
            target.write_bytes(replacement)
            return source

        def lazy_load_then_restore(_arguments, _source):
            compiled = compile(target.read_bytes(), str(target), "exec")
            exec(compiled, {})
            target.unlink()
            os.replace(str(backup), str(target))
            return {"status": "must-not-run"}

        controller.side_effect = lazy_load_then_restore
        arguments = self.module._parse_arguments(
            ["scan", "--data-dir", str(self.project_root), "--dry-run"]
        )
        try:
            with mock.patch.object(
                self.module,
                "verify_clean_tracked_historical_source",
                side_effect=verify_then_swap,
            ), mock.patch.object(
                self.module,
                "_invoke_production_controller",
                controller,
            ):
                with self.assertRaises(
                    self.module.HistoricalReplayEntrypointError
                ):
                    self.module._execute(arguments)
            controller.assert_not_called()
        finally:
            if backup.exists():
                if target.exists():
                    target.unlink()
                os.replace(str(backup), str(target))
        self.assertEqual(
            _git(
                self.project_root,
                "status",
                "--porcelain=v1",
                "--untracked-files=all",
            ),
            b"",
        )

    def test_assume_unchanged_same_size_worktree_head_mismatch_is_rejected(self):
        relative_path = "foundry/src/TwoVenueV2Executor.sol"
        target = self.project_root / relative_path
        original = target.read_bytes()
        replacement = b"X" * len(original)
        self.assertNotEqual(original, replacement)
        _git(self.project_root, "update-index", "--assume-unchanged", relative_path)
        target.write_bytes(replacement)
        self.assertEqual(
            _git(
                self.project_root,
                "status",
                "--porcelain=v1",
                "--untracked-files=all",
            ),
            b"",
        )
        with self.assertRaises(self.module.HistoricalReplayEntrypointError):
            self.module.verify_clean_tracked_historical_source()

    def test_git_uses_frozen_head_sha_and_stage_zero_blob_oids(self):
        commands = []
        real_popen = self.module.subprocess.Popen

        def record(command, *positional, **keywords):
            commands.append(tuple(command))
            return real_popen(command, *positional, **keywords)

        with mock.patch.object(
            self.module.subprocess, "Popen", side_effect=record
        ):
            source = self.module.verify_clean_tracked_historical_source()
            source.close()
        flattened = [argument for command in commands for argument in command]
        self.assertIn(
            self.repository_head
            + ":config/historical_foundry_replay_policy.json",
            flattened,
        )
        self.assertNotIn(
            "HEAD:config/historical_foundry_replay_policy.json",
            flattened,
        )
        self.assertTrue(any(
            "ls-files" in command
            and "--stage" in command
            and "config/historical_foundry_replay_policy.json" in command
            for command in commands
        ))
        self.assertTrue(any(
            "ls-tree" in command
            and self.repository_head in command
            and "lib/forge-std" in command
            for command in commands
        ))

    def test_checkout_after_head_freeze_is_rejected(self):
        real_popen = self.module.subprocess.Popen
        state = {"head_started": False, "mutated": False}

        def intercept(command, *positional, **keywords):
            arguments = tuple(command)
            if state["head_started"] and not state["mutated"]:
                state["mutated"] = True
                target = (
                    self.project_root
                    / "config/historical_foundry_replay_policy.json"
                )
                target.write_bytes(b'{"policy":2}\n')
                self._real_git_with_popen(
                    real_popen,
                    "add",
                    "config/historical_foundry_replay_policy.json",
                )
                self._real_git_with_popen(
                    real_popen, "commit", "-q", "-m", "race"
                )
            process = real_popen(command, *positional, **keywords)
            if arguments[-3:] == (
                "rev-parse", "--verify", "HEAD^{commit}",
            ) and "lib/forge-std" not in arguments:
                state["head_started"] = True
            return process

        with mock.patch.object(
            self.module.subprocess, "Popen", side_effect=intercept
        ):
            with self.assertRaises(self.module.HistoricalReplayEntrypointError):
                self.module.verify_clean_tracked_historical_source()
        self.assertTrue(state["mutated"])

    def test_already_checked_index_and_worktree_drift_is_rejected(self):
        real_popen = self.module.subprocess.Popen
        state = {"mutated": False}

        def intercept(command, *positional, **keywords):
            arguments = tuple(command)
            if (
                not state["mutated"]
                and "config/historical_foundry_replay_authority.json"
                in arguments
            ):
                state["mutated"] = True
                target = (
                    self.project_root
                    / "config/historical_foundry_replay_policy.json"
                )
                target.write_bytes(b'{"policy":2}\n')
                self._real_git_with_popen(
                    real_popen,
                    "add",
                    "config/historical_foundry_replay_policy.json",
                )
            return real_popen(command, *positional, **keywords)

        with mock.patch.object(
            self.module.subprocess, "Popen", side_effect=intercept
        ):
            with self.assertRaises(self.module.HistoricalReplayEntrypointError):
                self.module.verify_clean_tracked_historical_source()
        self.assertTrue(state["mutated"])

    def test_toolchain_identity_followed_by_source_drift_is_rejected(self):
        def mutate_after_identity():
            target = (
                self.project_root
                / "config/historical_foundry_replay_policy.json"
            )
            target.write_bytes(b'{"policy":2}\n')
            _git(
                self.project_root,
                "add",
                "config/historical_foundry_replay_policy.json",
            )

        toolchain = _FakeReviewedToolchain(
            self.project_root,
            self.forge_std_commit,
            after_project_identity=mutate_after_identity,
        )
        with mock.patch.object(
            self.module,
            "_open_reviewed_historical_toolchain",
            return_value=toolchain,
        ):
            with self.assertRaises(self.module.HistoricalReplayEntrypointError):
                self.module.verify_clean_tracked_historical_source()
        self.assertEqual(
            toolchain.events,
            ["enter", "versions", "identity", "project_identity", "exit"],
        )

    def test_held_source_reread_detects_post_preflight_drift_and_closes(self):
        source = self.module.verify_clean_tracked_historical_source()
        target = (
            self.project_root
            / "config/historical_foundry_replay_policy.json"
        )
        target.write_bytes(b'{"policy":2}\n')
        _git(
            self.project_root,
            "add",
            "config/historical_foundry_replay_policy.json",
        )
        with self.assertRaises(self.module.HistoricalReplayEntrypointError):
            source.reread_unchanged()
        source.close()
        source.close()
        self.assertEqual(self.toolchain.events.count("exit"), 1)
        with self.assertRaises(TypeError):
            source.__reduce__()

    def test_held_source_authority_is_closure_only_and_cannot_be_forged(self):
        source = self.module.verify_clean_tracked_historical_source()
        source_type = type(source)
        self.addCleanup(source.close)

        self.assertEqual(source_type.__slots__, ("__weakref__",))
        self.assertFalse(hasattr(source, "__dict__"))
        for name in (
            "_token",
            "_members",
            "_toolchain",
            "_frozen_git",
            "_project_identity",
            "_record_digests",
            "_identity_projection",
        ):
            with self.subTest(authority_attribute=name):
                self.assertFalse(hasattr(source, name))
                with self.assertRaises(AttributeError):
                    setattr(source, name, object())
                with self.assertRaises(AttributeError):
                    object.__setattr__(source, name, object())
                with self.assertRaises(AttributeError):
                    delattr(source, name)

        with self.assertRaises(self.module.HistoricalReplayEntrypointError):
            source_type(object())
        forged = object.__new__(source_type)
        with self.assertRaises(self.module.HistoricalReplayEntrypointError):
            _ = forged.identity_projection
        with self.assertRaises(self.module.HistoricalReplayEntrypointError):
            forged.reread_unchanged()
        with self.assertRaises(self.module.HistoricalReplayEntrypointError):
            forged.close()
        with self.assertRaises(TypeError):
            type("ForgedHeldSource", (source_type,), {})

        for operation in (
            copy.copy,
            copy.deepcopy,
            pickle.dumps,
        ):
            with self.subTest(operation=operation.__name__):
                with self.assertRaises(TypeError):
                    operation(source)

        class Lookalike:
            pass

        lookalike = Lookalike()
        projection_property = inspect.getattr_static(
            source_type, "identity_projection"
        )
        with self.assertRaises(self.module.HistoricalReplayEntrypointError):
            projection_property.fget(lookalike)
        with self.assertRaises(self.module.HistoricalReplayEntrypointError):
            source_type.reread_unchanged(lookalike)
        with self.assertRaises(self.module.HistoricalReplayEntrypointError):
            source_type.close(lookalike)

        projection = source.identity_projection
        with self.assertRaises(TypeError):
            projection["repository_head"] = "0" * 40
        source.close()
        source.close()
        with self.assertRaises(self.module.HistoricalReplayEntrypointError):
            _ = source.identity_projection
        with self.assertRaises(self.module.HistoricalReplayEntrypointError):
            source.reread_unchanged()
        self.assertEqual(self.toolchain.events.count("exit"), 1)

    def test_toolchain_is_cleanup_owned_before_enter_returns(self):
        enter_failure = _FakeReviewedToolchain(
            self.project_root,
            self.forge_std_commit,
            fail_at="enter",
        )

        class WrongEnterToolchain(_FakeReviewedToolchain):
            def __enter__(self):
                self._record("enter")
                return object()

        wrong_enter = WrongEnterToolchain(
            self.project_root,
            self.forge_std_commit,
        )
        for label, toolchain in (
            ("raises", enter_failure),
            ("returns-foreign-object", wrong_enter),
        ):
            with self.subTest(case=label), mock.patch.object(
                self.module,
                "_open_reviewed_historical_toolchain",
                return_value=toolchain,
            ):
                with self.assertRaises(
                    self.module.HistoricalReplayEntrypointError
                ):
                    self.module.verify_clean_tracked_historical_source()
            self.assertEqual(toolchain.events, ["enter", "exit"])

    def test_toolchain_failure_paths_always_exit(self):
        for phase in ("versions", "identity", "project_identity"):
            with self.subTest(phase=phase):
                toolchain = _FakeReviewedToolchain(
                    self.project_root,
                    self.forge_std_commit,
                    fail_at=phase,
                )
                with mock.patch.object(
                    self.module,
                    "_open_reviewed_historical_toolchain",
                    return_value=toolchain,
                ):
                    with self.assertRaises(
                        self.module.HistoricalReplayEntrypointError
                    ):
                        self.module.verify_clean_tracked_historical_source()
                self.assertEqual(toolchain.events[-1], "exit")

    def test_held_source_exit_preserves_control_flow_cleanup_matrix(self):
        cases = (
            (
                "success-close-ordinary",
                None,
                ValueError("close"),
                "wrapped-close",
            ),
            (
                "ordinary-close-ordinary",
                RuntimeError("body"),
                ValueError("close"),
                "wrapped-close",
            ),
            (
                "ordinary-close-control",
                RuntimeError("body"),
                KeyboardInterrupt("close"),
                "close",
            ),
            (
                "keyboard-close-ordinary",
                KeyboardInterrupt("body"),
                ValueError("close"),
                "body",
            ),
            (
                "system-exit-close-control",
                SystemExit("body"),
                GeneratorExit("close"),
                "body",
            ),
            (
                "generator-close-success",
                GeneratorExit("body"),
                None,
                "body",
            ),
        )
        for label, body_error, close_error, expected in cases:
            with self.subTest(case=label):
                toolchain = _FakeReviewedToolchain(
                    self.project_root,
                    self.forge_std_commit,
                    exit_failure=close_error,
                )
                with mock.patch.object(
                    self.module,
                    "_open_reviewed_historical_toolchain",
                    return_value=toolchain,
                ):
                    source = (
                        self.module.verify_clean_tracked_historical_source()
                    )
                expected_error = body_error if expected == "body" else close_error
                expected_type = (
                    self.module.HistoricalReplayEntrypointError
                    if expected == "wrapped-close"
                    else type(expected_error)
                )
                with self.assertRaises(expected_type) as raised:
                    with source:
                        if body_error is not None:
                            raise body_error
                if expected != "wrapped-close":
                    self.assertIs(raised.exception, expected_error)
                source.close()
                self.assertEqual(toolchain.events.count("exit"), 1)

    def test_final_reread_rechecks_versions_and_full_toolchain_identity(self):
        class DriftingIdentityToolchain(_FakeReviewedToolchain):
            @property
            def verified_identity(self):
                identity = super().verified_identity
                if self.events.count("identity") >= 2:
                    identity["source_lock_sha256"] = "8" * 64
                return identity

        toolchain = DriftingIdentityToolchain(
            self.project_root,
            self.forge_std_commit,
        )
        with mock.patch.object(
            self.module,
            "_open_reviewed_historical_toolchain",
            return_value=toolchain,
        ):
            source = self.module.verify_clean_tracked_historical_source()
        with self.assertRaises(self.module.HistoricalReplayEntrypointError):
            source.reread_unchanged()
        source.close()
        self.assertEqual(toolchain.events.count("versions"), 2)
        self.assertEqual(toolchain.events.count("identity"), 2)
        self.assertEqual(toolchain.events.count("exit"), 1)

    def test_project_root_ancestor_symlink_is_rejected(self):
        alias = Path(self.temporary.name) / "project-alias"
        alias.symlink_to(self.project_root, target_is_directory=True)
        with mock.patch.object(self.module, "_PROJECT_ROOT", alias):
            with self.assertRaises(self.module.HistoricalReplayEntrypointError):
                self.module.verify_clean_tracked_historical_source()

    def test_preflight_requires_noninheritable_descriptors(self):
        with mock.patch.object(
            self.module.os, "get_inheritable", return_value=True
        ):
            with self.assertRaises(self.module.HistoricalReplayEntrypointError):
                self.module.verify_clean_tracked_historical_source()

    def test_index_head_blob_mismatch_is_rejected_even_if_status_is_hidden(self):
        relative_path = "config/historical_foundry_replay_policy.json"
        target = self.project_root / relative_path
        target.write_bytes(b'{"policy":2}\n')
        _git(self.project_root, "add", relative_path)
        real_popen = self.module.subprocess.Popen

        def hide_status(command, *positional, **keywords):
            if tuple(command[-3:]) == (
                "status",
                "--porcelain=v1",
                "--untracked-files=all",
            ):
                command = ("/usr/bin/true",)
            return real_popen(command, *positional, **keywords)

        with mock.patch.object(
            self.module.subprocess,
            "Popen",
            side_effect=hide_status,
        ):
            with self.assertRaises(self.module.HistoricalReplayEntrypointError):
                self.module.verify_clean_tracked_historical_source()

    def test_head_gitlink_must_be_mode_160000_at_the_fixed_commit(self):
        with mock.patch.object(self.module, "_FORGE_STD_COMMIT", "f" * 40):
            with self.assertRaises(self.module.HistoricalReplayEntrypointError):
                self.module.verify_clean_tracked_historical_source()

    def test_checked_out_forge_std_commit_must_match_head_gitlink(self):
        (self.forge_std / "README.md").write_bytes(b"second commit\n")
        _git(self.forge_std, "add", "README.md")
        _git(self.forge_std, "commit", "-q", "-m", "second")
        real_popen = self.module.subprocess.Popen

        def hide_status(command, *positional, **keywords):
            if tuple(command[-3:]) == (
                "status",
                "--porcelain=v1",
                "--untracked-files=all",
            ):
                command = ("/usr/bin/true",)
            return real_popen(command, *positional, **keywords)

        with mock.patch.object(
            self.module.subprocess,
            "Popen",
            side_effect=hide_status,
        ):
            with self.assertRaises(self.module.HistoricalReplayEntrypointError):
                self.module.verify_clean_tracked_historical_source()

    def _hostile_process_factory(self, *, stream, payload, sleep_seconds=0):
        real_popen = self.module.subprocess.Popen
        proxies = []

        def spawn(_command, *positional, **keywords):
            script = (
                "import sys,time;"
                "time.sleep({!r});"
                "sys.{}.buffer.write({!r});"
                "sys.{}.buffer.flush()"
            ).format(sleep_seconds, stream, payload, stream)
            process = real_popen(
                (sys.executable, "-c", script),
                *positional,
                **keywords
            )
            proxy = _ProcessProxy(process)
            proxies.append(proxy)
            return proxy

        return spawn, proxies

    def test_git_stdout_cap_is_streaming_reaped_and_blocks_controller(self):
        spawn, proxies = self._hostile_process_factory(
            stream="stdout", payload=b"x" * 4096
        )
        arguments = self.module._parse_arguments(
            ["scan", "--data-dir", str(self.project_root), "--dry-run"]
        )
        controller = mock.Mock(
            side_effect=AssertionError("controller must not run")
        )
        with mock.patch.object(
            self.module, "_MAX_GIT_OUTPUT_BYTES", 128
        ), mock.patch.object(
            self.module.subprocess, "Popen", side_effect=spawn
        ), mock.patch.object(
            self.module, "_invoke_production_controller", controller
        ):
            with self.assertRaises(self.module.HistoricalReplayEntrypointError):
                self.module._execute(arguments)
        controller.assert_not_called()
        self.assertTrue(proxies)
        self.assertFalse(proxies[0].communicate_called)
        self.assertIsNotNone(proxies[0].poll())

    def test_git_stderr_cap_is_streaming_and_reaped(self):
        spawn, proxies = self._hostile_process_factory(
            stream="stderr", payload=b"x" * 4096
        )
        with mock.patch.object(
            self.module, "_MAX_GIT_OUTPUT_BYTES", 128
        ), mock.patch.object(
            self.module.subprocess, "Popen", side_effect=spawn
        ):
            with self.assertRaises(self.module.HistoricalReplayEntrypointError):
                self.module.verify_clean_tracked_historical_source()
        self.assertTrue(proxies)
        self.assertFalse(proxies[0].communicate_called)
        self.assertIsNotNone(proxies[0].poll())

    def test_git_timeout_terminates_and_reaps_without_communicate(self):
        spawn, proxies = self._hostile_process_factory(
            stream="stdout", payload=b"late", sleep_seconds=0.3
        )
        with mock.patch.object(
            self.module, "_GIT_TIMEOUT_SECONDS", 0.05, create=True
        ), mock.patch.object(
            self.module.subprocess, "Popen", side_effect=spawn
        ):
            with self.assertRaises(self.module.HistoricalReplayEntrypointError):
                self.module.verify_clean_tracked_historical_source()
        self.assertTrue(proxies)
        self.assertFalse(proxies[0].communicate_called)
        self.assertIsNotNone(proxies[0].poll())

    def test_git_cleanup_continues_after_terminate_and_kill_errors(self):
        real_popen = self.module.subprocess.Popen
        proxies = []

        class CleanupFaultProcess(_ProcessProxy):
            def __init__(self, process):
                super().__init__(process)
                self.terminate_calls = 0
                self.kill_calls = 0

            def terminate(self):
                self.terminate_calls += 1
                raise RuntimeError("terminate failed")

            def kill(self):
                self.kill_calls += 1
                self._process.kill()
                raise RuntimeError("kill reported failure after signal")

        def spawn(_command, *positional, **keywords):
            process = real_popen(
                (sys.executable, "-c", "import time; time.sleep(5)"),
                *positional,
                **keywords
            )
            proxy = CleanupFaultProcess(process)
            proxies.append(proxy)
            return proxy

        try:
            with mock.patch.object(
                self.module, "_GIT_TIMEOUT_SECONDS", 0.05
            ), mock.patch.object(
                self.module, "_PROCESS_CLEANUP_SECONDS", 0.05
            ), mock.patch.object(
                self.module.subprocess, "Popen", side_effect=spawn
            ):
                with self.assertRaises(
                    self.module.HistoricalReplayEntrypointError
                ):
                    self.module.verify_clean_tracked_historical_source()
            self.assertTrue(proxies)
            self.assertEqual(proxies[0].terminate_calls, 1)
            self.assertEqual(proxies[0].kill_calls, 1)
            self.assertIsNotNone(proxies[0].poll())
        finally:
            for proxy in proxies:
                if proxy.poll() is None:
                    proxy._process.kill()
                proxy._process.wait()

    def test_execute_closes_held_source_on_success_ordinary_and_control_exit(self):
        class HeldSource:
            def __init__(self):
                self.closed = 0
                self.reread = 0

            @property
            def identity_projection(self):
                return {"schema": "held-test-source"}

            def close(self):
                self.closed += 1

            def reread_unchanged(self):
                self.reread += 1

        arguments = self.module._parse_arguments(
            ["scan", "--data-dir", str(self.project_root), "--dry-run"]
        )
        cases = (
            ("success", None),
            ("ordinary", RuntimeError("ordinary")),
            ("control", KeyboardInterrupt()),
        )
        for label, failure in cases:
            with self.subTest(case=label):
                source = HeldSource()

                def controller(_arguments, received):
                    self.assertIs(received, source)
                    if failure is not None:
                        raise failure
                    return {"status": "test-only"}

                with mock.patch.object(
                    self.module,
                    "verify_clean_tracked_historical_source",
                    return_value=source,
                ), mock.patch.object(
                    self.module,
                    "_invoke_production_controller",
                    side_effect=controller,
                ):
                    if failure is None:
                        self.module._execute(arguments)
                    else:
                        with self.assertRaises(type(failure)):
                            self.module._execute(arguments)
                self.assertEqual(source.closed, 1)
                self.assertEqual(source.reread, 2)

    def test_preflight_failure_occurs_before_controller_capability(self):
        arguments = self.module._parse_arguments(
            ["scan", "--data-dir", str(self.project_root), "--dry-run"]
        )
        controller = mock.Mock(
            side_effect=AssertionError("controller must not run")
        )
        with mock.patch.object(
            self.module,
            "verify_clean_tracked_historical_source",
            side_effect=self.module.HistoricalReplayEntrypointError(
                "preflight failed"
            ),
        ), mock.patch.object(
            self.module, "_invoke_production_controller", controller
        ):
            with self.assertRaisesRegex(
                self.module.HistoricalReplayEntrypointError,
                "preflight failed",
            ):
                self.module._execute(arguments)
        controller.assert_not_called()


class HistoricalReplayCandidateDriverTests(unittest.TestCase):
    def setUp(self):
        self.module = _entrypoint()

    def test_drives_each_action_to_the_exact_terminal_selection(self):
        import scripts.historical_foundry_anvil as anvil
        import scripts.historical_foundry_scan as scan

        snapshot = object()
        context = object()
        first_action = object()
        second_action = object()
        first_ledger = object()
        second_ledger = object()
        terminal = {
            "status": "found_publishable_profitable_block",
            "selected_scenario_count": 10,
            "unresolved_candidate_count": 0,
        }
        events = []

        class Sink:
            def __init__(self, ledger):
                self._ledger = ledger

            def validated_ledger(self):
                events.append(("ledger", self._ledger))
                return self._ledger

        def advance(*, snapshot: object, replay_ledger: object):
            events.append(("advance", snapshot, replay_ledger))
            if replay_ledger is None:
                return first_action
            if replay_ledger is first_ledger:
                return second_action
            if replay_ledger is second_ledger:
                return terminal
            raise AssertionError("unexpected replay ledger")

        def consume(*, action: object, context: object):
            events.append(("consume", action, context))
            return "scenario-one" if action is first_action else "scenario-two"

        def open_sink(*, context: object, scenario: object):
            events.append(("open", context, scenario))
            return Sink(
                first_ledger if scenario == "scenario-one" else second_ledger
            )

        def replay(*, context: object, scenario: object, sink: object):
            events.append(("replay", context, scenario, sink._ledger))

        with mock.patch.object(
            scan,
            "_advance_historical_selection_controller",
            side_effect=advance,
        ), mock.patch.object(
            scan,
            "_consume_historical_selection_action",
            side_effect=consume,
        ), mock.patch.object(
            anvil, "_open_scenario_evidence_sink", side_effect=open_sink
        ), mock.patch.object(
            anvil, "_replay_historical_scenario", side_effect=replay
        ):
            result = self.module._drive_historical_candidate_replay(
                snapshot=snapshot, replay_context=context
            )

        self.assertIs(result, terminal)
        self.assertEqual(
            events,
            [
                ("advance", snapshot, None),
                ("consume", first_action, context),
                ("open", context, "scenario-one"),
                ("replay", context, "scenario-one", first_ledger),
                ("ledger", first_ledger),
                ("advance", snapshot, first_ledger),
                ("consume", second_action, context),
                ("open", context, "scenario-two"),
                ("replay", context, "scenario-two", second_ledger),
                ("ledger", second_ledger),
                ("advance", snapshot, second_ledger),
            ],
        )

    def test_typed_replay_failure_is_recorded_and_never_becomes_no_opportunity(self):
        import scripts.historical_foundry_anvil as anvil
        import scripts.historical_foundry_scan as scan

        snapshot = object()
        context = object()
        action = object()
        scenario = object()
        sink = mock.Mock()
        failure = anvil.HistoricalReplayError("archive")
        unresolved = {
            "status": "candidate_unresolved",
            "closed_reason": "archive",
            "unresolved_candidate_count": 1,
        }
        advance = mock.Mock(side_effect=(action, unresolved))
        record = mock.Mock()

        with mock.patch.object(
            scan,
            "_advance_historical_selection_controller",
            advance,
        ), mock.patch.object(
            scan,
            "_consume_historical_selection_action",
            return_value=scenario,
        ), mock.patch.object(
            scan, "_record_historical_selection_failure", record
        ), mock.patch.object(
            anvil, "_open_scenario_evidence_sink", return_value=sink
        ), mock.patch.object(
            anvil, "_replay_historical_scenario", side_effect=failure
        ):
            with self.assertRaisesRegex(
                self.module.HistoricalReplayEntrypointError,
                "historical replay candidate is unresolved",
            ):
                self.module._drive_historical_candidate_replay(
                    snapshot=snapshot, replay_context=context
                )

        self.assertEqual(
            advance.call_args_list,
            [
                mock.call(snapshot=snapshot, replay_ledger=None),
                mock.call(snapshot=snapshot, replay_ledger=None),
            ],
        )
        record.assert_called_once_with(action=action, error=failure)
        sink.validated_ledger.assert_not_called()

    def test_rejects_any_unrecognized_terminal_state(self):
        import scripts.historical_foundry_scan as scan

        terminal = {"status": "weaker_partial_result"}
        with mock.patch.object(
            scan,
            "_advance_historical_selection_controller",
            return_value=terminal,
        ):
            with self.assertRaisesRegex(
                self.module.HistoricalReplayEntrypointError,
                "historical replay selection is invalid",
            ):
                self.module._drive_historical_candidate_replay(
                    snapshot=object(), replay_context=object()
                )


if __name__ == "__main__":
    unittest.main()
