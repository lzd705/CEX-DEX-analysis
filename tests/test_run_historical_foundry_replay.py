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

    def test_valid_cli_enters_controller_and_fails_without_production_evidence(self):
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
            "historical production scan failed\n",
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

    def test_execute_result_retains_exact_live_pointer_before_and_after(self):
        class Source:
            def __init__(self):
                self.rereads = 0
                self.closed = 0

            def reread_unchanged(self):
                self.rereads += 1

            def close(self):
                self.closed += 1

        source = Source()
        routes = self.data_dir / "routes"
        (routes / "core").mkdir(parents=True)
        (routes / "core" / "latest.json").write_bytes(b"core")
        (routes / "latest.json").write_bytes(b"complete")
        arguments = self.module._parse_arguments([
            "scan", "--data-dir", str(self.data_dir), "--dry-run",
        ])
        with mock.patch.object(
            self.module,
            "_require_safe_historical_startup",
        ), mock.patch.object(
            self.module,
            "verify_clean_tracked_historical_source",
            return_value=source,
        ), mock.patch.object(
            self.module,
            "_invoke_production_controller",
            return_value={"schema": "test-result/v1", "status": "ok"},
        ):
            result = self.module._execute(arguments)

        self.assertEqual(result["schema"], "test-result/v1")
        self.assertEqual(
            result["live_pointers_before"],
            result["live_pointers_after"],
        )
        self.assertEqual(
            tuple(row["relative_path"] for row in result[
                "live_pointers_before"
            ]),
            ("routes/core/latest.json", "routes/latest.json"),
        )
        self.assertEqual(
            tuple(
                base64.b64decode(row["bytes_base64"])
                for row in result["live_pointers_before"]
            ),
            (b"core", b"complete"),
        )
        self.assertEqual(source.rereads, 2)
        self.assertEqual(source.closed, 1)

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


class HistoricalReplayRawRunControllerTests(unittest.TestCase):
    class Resource:
        def __init__(self, name, events):
            self.name = name
            self.events = events
            self.closed = 0

        def close(self):
            self.closed += 1
            self.events.append(("close", self.name))

    class Finalized(Resource):
        def identity_projection(self):
            self.events.append(("identity", self.name))
            return {
                "run_id": "run:" + "a" * 64,
                "run_manifest_sha256": "b" * 64,
                "stage": "complete",
            }

    def setUp(self):
        self.module = _entrypoint()

    def _patched_controller(self, *, selection_status):
        import scripts.historical_foundry_anvil as anvil
        import scripts.historical_foundry_contracts as contracts
        import scripts.historical_foundry_rpc as rpc
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_storage as storage

        events = []
        resources = {
            name: self.Resource(name, events)
            for name in (
                "rpc_context", "claim", "spool", "capability",
                "capture", "prefilter", "replay_context", "lease",
            )
        }
        resources["finalized"] = self.Finalized("finalized", events)
        config = object()
        window = object()
        rows = (object(),)
        grid = object()
        snapshot = types.SimpleNamespace(
            validated_window=window, validated_grid=grid
        )
        artifact = object()
        selection = {
            "status": selection_status,
            "selected_scenario_count": (
                10
                if selection_status
                == "found_publishable_profitable_block"
                else 0
            ),
            "unresolved_candidate_count": 0,
        }

        def operation(name, value):
            def call(*args, **kwargs):
                events.append((name, args, kwargs))
                return value

            return call

        patches = contextlib.ExitStack()
        patches.enter_context(mock.patch.object(
            contracts,
            "load_historical_foundry_config_set",
            side_effect=operation("load_config", config),
        ))
        patches.enter_context(mock.patch.object(
            storage,
            "_open_historical_window_exchange_spool",
            side_effect=operation("open_spool", resources["spool"]),
        ))
        patches.enter_context(mock.patch.object(
            rpc,
            "_open_production_archive_rpc_run",
            side_effect=operation("open_rpc", resources["rpc_context"]),
        ))
        patches.enter_context(mock.patch.object(
            rpc,
            "_claim_fresh_production_archive_rpc_run_for_historical_window",
            side_effect=operation("claim", resources["claim"]),
        ))
        patches.enter_context(mock.patch.object(
            scan,
            "_capture_production_historical_window",
            side_effect=operation("capture", resources["capability"]),
        ))
        patches.enter_context(mock.patch.object(
            scan,
            "_materialize_historical_window_staging_snapshot",
            side_effect=operation("materialize", resources["capture"]),
        ))
        patches.enter_context(mock.patch.object(
            scan,
            "open_validated_historical_window",
            side_effect=operation("open_window", window),
        ))
        patches.enter_context(mock.patch.object(
            scan,
            "build_historical_prefilter_grid",
            side_effect=operation("build_grid", rows),
        ))
        patches.enter_context(mock.patch.object(
            storage,
            "_freeze_historical_prefilter_grid",
            side_effect=operation("freeze_grid", resources["prefilter"]),
        ))
        patches.enter_context(mock.patch.object(
            scan,
            "open_validated_historical_scan_snapshot",
            side_effect=operation("open_snapshot", snapshot),
        ))
        patches.enter_context(mock.patch.object(
            contracts,
            "build_validated_executor_artifact",
            side_effect=operation("build_artifact", artifact),
        ))
        patches.enter_context(mock.patch.object(
            anvil,
            "open_historical_replay_context",
            side_effect=operation(
                "open_replay", resources["replay_context"]
            ),
        ))
        patches.enter_context(mock.patch.object(
            self.module,
            "_drive_historical_candidate_replay",
            side_effect=operation("drive", selection),
        ))
        patches.enter_context(mock.patch.object(
            scan,
            "_finalize_historical_replay_run",
            side_effect=operation("finalize", resources["finalized"]),
        ))
        acquire = patches.enter_context(mock.patch.object(
            storage,
            "_acquire_historical_run_publication_lease",
            side_effect=operation("acquire_lease", resources["lease"]),
        ))
        return patches, events, resources, config, selection, acquire

    def test_positive_scan_runs_steps_three_through_ten_in_order(self):
        (
            patches, events, resources, config, selection, acquire,
        ) = self._patched_controller(
            selection_status="found_publishable_profitable_block"
        )
        with patches:
            result = self.module._produce_historical_raw_run(
                data_dir=Path("/immutable-data")
            )

        self.assertIs(result["config"], config)
        self.assertEqual(result["selection"], selection)
        self.assertIs(result["run"], resources["finalized"])
        self.assertIs(result["publication_lease"], resources["lease"])
        self.assertEqual(
            [event[0] for event in events],
            [
                "load_config", "open_spool", "open_rpc", "claim",
                "capture", "materialize", "open_window", "build_grid",
                "freeze_grid", "open_snapshot", "build_artifact",
                "open_replay", "drive", "finalize", "identity",
                "close", "acquire_lease",
            ],
        )
        self.assertEqual(events[-2], ("close", "replay_context"))
        acquire.assert_called_once_with(
            run_id="run:" + "a" * 64,
            expected_manifest_sha256="b" * 64,
        )

    def test_resolved_no_opportunity_closes_run_and_never_requests_lease(self):
        (
            patches, events, resources, _config, selection, acquire,
        ) = self._patched_controller(
            selection_status="no_publishable_profitable_block"
        )
        with patches:
            result = self.module._produce_historical_raw_run(
                data_dir=Path("/immutable-data")
            )

        self.assertEqual(result["selection"], selection)
        self.assertIsNone(result["run"])
        self.assertIsNone(result["publication_lease"])
        acquire.assert_not_called()
        self.assertEqual(resources["replay_context"].closed, 1)
        self.assertEqual(resources["finalized"].closed, 1)
        self.assertEqual(
            events[-2:],
            [("close", "replay_context"), ("close", "finalized")],
        )

    def test_failure_before_grid_freeze_closes_the_last_owned_snapshot(self):
        import scripts.historical_foundry_scan as scan

        (
            patches, events, resources, _config, _selection, acquire,
        ) = self._patched_controller(
            selection_status="found_publishable_profitable_block"
        )
        with patches, mock.patch.object(
            scan,
            "build_historical_prefilter_grid",
            side_effect=ValueError("invalid grid"),
        ):
            with self.assertRaisesRegex(
                self.module.HistoricalReplayEntrypointError,
                "historical production scan failed",
            ):
                self.module._produce_historical_raw_run(
                    data_dir=Path("/immutable-data")
                )

        acquire.assert_not_called()
        self.assertEqual(resources["capture"].closed, 1)
        self.assertEqual(resources["prefilter"].closed, 0)
        self.assertEqual(resources["replay_context"].closed, 0)
        self.assertEqual(events[-1], ("close", "capture"))


class HistoricalReplayBundlePreparationTests(unittest.TestCase):
    class Resource(HistoricalReplayRawRunControllerTests.Resource):
        pass

    class Context(Resource):
        def __init__(self, name, events, projection):
            super().__init__(name, events)
            self.projection = copy.deepcopy(projection)

        def identity_projection(self):
            self.events.append(("identity", self.name))
            return copy.deepcopy(self.projection)

    def setUp(self):
        self.module = _entrypoint()

    def _fixture(self):
        events = []
        finalized = self.Resource("finalized", events)
        lease = self.Resource("lease", events)
        stage = self.Resource("core_stage", events)
        subject = self.Resource("subject", events)
        pointer_publication = self.Resource(
            "pointer_publication", events
        )
        staged_projection = {
            "schema": "historical_replay_build_context/v1",
            "run_id": "run:" + "a" * 64,
            "core_manifest_sha256": "b" * 64,
            "core_pointer": {"route_cohort_id": "cohort:" + "c" * 64},
        }
        staged_context = self.Context(
            "staged_context", events, staged_projection
        )
        committed_context = self.Context(
            "committed_context", events, staged_projection
        )
        payload = {
            "replay_id": "replay:" + "d" * 64,
            "bundle": {"route_cohort_id": "cohort:" + "c" * 64},
            "opportunities": tuple(range(10)),
        }
        bundle = {
            "path": Path("/immutable-data/routes/historical/bundles")
            / ("replay:" + "d" * 64),
            "replay_id": "replay:" + "d" * 64,
            "manifest_sha256": "e" * 64,
            "pointer_core": {"schema": "route_historical_replay_pointer/v1"},
            "verification_subject": subject,
            "pointer_publication": pointer_publication,
        }
        raw = {
            "config": object(),
            "selection": {
                "status": "found_publishable_profitable_block",
                "selected_scenario_count": 10,
                "unresolved_candidate_count": 0,
            },
            "run": finalized,
            "run_identity": {
                "run_id": "run:" + "a" * 64,
                "run_manifest_sha256": "f" * 64,
            },
            "publication_lease": lease,
        }
        return {
            "events": events,
            "finalized": finalized,
            "lease": lease,
            "stage": stage,
            "subject": subject,
            "pointer_publication": pointer_publication,
            "staged_context": staged_context,
            "committed_context": committed_context,
            "payload": payload,
            "bundle": bundle,
            "raw": raw,
        }

    def _patch_publication(self, fixture):
        import scripts.historical_route_publication as publication

        events = fixture["events"]

        def stage_core(**keywords):
            events.append(("stage_core", keywords))
            return fixture["stage"]

        def load_staged(**keywords):
            events.append(("load_staged", keywords))
            return fixture["staged_context"]

        def publish_core(**keywords):
            events.append(("publish_core", keywords))
            return fixture["committed_context"]

        def build_payload(*, context):
            events.append(("build_payload", context.name))
            return copy.deepcopy(fixture["payload"])

        def stage_bundle(**keywords):
            events.append(("stage_bundle", keywords))
            return fixture["bundle"]

        stack = contextlib.ExitStack()
        stack.enter_context(mock.patch.object(
            publication,
            "stage_historical_replay_core",
            side_effect=stage_core,
        ))
        stack.enter_context(mock.patch.object(
            publication,
            "load_validated_historical_replay_core_at",
            side_effect=load_staged,
        ))
        publish = stack.enter_context(mock.patch.object(
            publication,
            "publish_historical_replay_core",
            side_effect=publish_core,
        ))
        stack.enter_context(mock.patch.object(
            publication,
            "_build_historical_complete_payload",
            side_effect=build_payload,
        ))
        stack.enter_context(mock.patch.object(
            publication,
            "stage_historical_replay_bundle",
            side_effect=stage_bundle,
        ))
        return stack, publish

    def test_dry_run_keeps_staged_core_and_never_moves_its_pointer(self):
        fixture = self._fixture()
        patches, publish = self._patch_publication(fixture)
        with patches:
            prepared = self.module._prepare_historical_replay_bundle(
                data_dir=Path("/immutable-data"),
                raw_state=fixture["raw"],
                publish=False,
            )

        publish.assert_not_called()
        self.assertIs(prepared["core_stage"], fixture["stage"])
        self.assertIs(prepared["context"], fixture["staged_context"])
        self.assertIs(prepared["verification_subject"], fixture["subject"])
        self.assertIs(
            prepared["pointer_publication"],
            fixture["pointer_publication"],
        )
        self.assertIsNone(fixture["raw"]["run"])
        self.assertIsNone(fixture["raw"]["publication_lease"])
        self.assertEqual(
            [event[0] for event in fixture["events"]],
            [
                "stage_core", "load_staged", "identity",
                "build_payload", "build_payload", "stage_bundle",
            ],
        )
        self.module._close_prepared_historical_bundle(prepared)
        self.assertEqual(
            fixture["events"][-5:],
            [
                ("close", "pointer_publication"),
                ("close", "subject"),
                ("close", "staged_context"),
                ("close", "core_stage"),
                ("close", "finalized"),
            ],
        )

    def test_publish_reloads_equal_committed_context_before_bundle(self):
        fixture = self._fixture()
        patches, publish = self._patch_publication(fixture)
        with patches:
            prepared = self.module._prepare_historical_replay_bundle(
                data_dir=Path("/immutable-data"),
                raw_state=fixture["raw"],
                publish=True,
            )

        publish.assert_called_once_with(
            data_dir=Path("/immutable-data"),
            staged_core=fixture["stage"],
        )
        self.assertIsNone(prepared["core_stage"])
        self.assertIs(prepared["context"], fixture["committed_context"])
        self.assertEqual(
            [event[0] for event in fixture["events"]],
            [
                "stage_core", "load_staged", "identity",
                "build_payload", "close", "publish_core", "identity",
                "build_payload", "stage_bundle",
            ],
        )
        self.assertEqual(
            fixture["events"][4], ("close", "staged_context")
        )
        self.module._close_prepared_historical_bundle(prepared)
        self.assertEqual(
            fixture["events"][-4:],
            [
                ("close", "pointer_publication"),
                ("close", "subject"),
                ("close", "committed_context"),
                ("close", "finalized"),
            ],
        )

    def test_committed_context_drift_fails_before_complete_bundle(self):
        fixture = self._fixture()
        fixture["committed_context"].projection[
            "core_manifest_sha256"
        ] = "0" * 64
        patches, _publish = self._patch_publication(fixture)
        with patches:
            with self.assertRaisesRegex(
                self.module.HistoricalReplayEntrypointError,
                "historical committed core differs from staged core",
            ):
                self.module._prepare_historical_replay_bundle(
                    data_dir=Path("/immutable-data"),
                    raw_state=fixture["raw"],
                    publish=True,
                )

        self.assertNotIn(
            "stage_bundle",
            [event[0] for event in fixture["events"]],
        )
        self.assertEqual(fixture["subject"].closed, 0)
        self.assertEqual(fixture["committed_context"].closed, 1)
        self.assertEqual(fixture["finalized"].closed, 1)


class HistoricalReplayConnectedVerificationControllerTests(unittest.TestCase):
    class Subject:
        def __init__(self):
            self.rereads = 0

        def reread_unchanged(self):
            self.rereads += 1

    def setUp(self):
        self.module = _entrypoint()

    @staticmethod
    def _fixture(*, mode, evidence_mode="production_connected"):
        import scripts.historical_foundry_verifier as verifier

        pointer_core = {
            "schema": "route_historical_replay_pointer/v1",
            "bundle_stage": "route_historical_foundry_replay/v1",
            "replay_id": "replay:" + "1" * 64,
            "route_cohort_id": "cohort:" + "2" * 64,
            "manifest_sha256": "3" * 64,
        }
        report = {
            "schema": "route_historical_replay_verification/v1",
            "evidence_mode": evidence_mode,
            "status": (
                "verified"
                if evidence_mode == "production_connected"
                else "structurally_validated"
            ),
        }
        report_bytes = verifier._canonical_bytes(report)
        report_sha256 = hashlib.sha256(report_bytes).hexdigest()
        final_pointer = {
            **pointer_core,
            "verification_report_sha256": report_sha256,
        }
        subject = HistoricalReplayConnectedVerificationControllerTests.Subject()
        prepared = {
            "mode": mode,
            "data_dir": Path("/immutable-data"),
            "bundle": {"pointer_core": copy.deepcopy(pointer_core)},
            "verification_subject": subject,
            "pointer_publication": object(),
        }
        result = {
            "schema": "historical_connected_verification_result/v1",
            "mode": "publish" if mode == "publish" else "staged",
            "report": report,
            "report_bytes": report_bytes,
            "report_sha256": report_sha256,
            "pointer_core": copy.deepcopy(pointer_core),
            "final_pointer": final_pointer,
            "final_pointer_bytes": verifier._canonical_bytes(final_pointer),
            "install_result": object() if mode == "publish" else None,
        }
        return prepared, result

    def test_dry_run_requires_connected_evidence_but_installs_no_report(self):
        import scripts.historical_foundry_verifier as verifier

        prepared, result = self._fixture(mode="dry-run")
        with mock.patch.object(
            verifier,
            "run_connected_historical_verification",
            return_value=result,
        ) as run:
            observed = self.module._verify_prepared_historical_bundle(
                prepared=prepared, publish=False
            )
        run.assert_called_once_with(
            prepared["verification_subject"], mode="staged"
        )
        self.assertIs(observed, result)
        self.assertIs(prepared["verification"], result)
        self.assertEqual(prepared["verification_subject"].rereads, 2)

    def test_publish_requires_installed_report_and_exact_final_pointer(self):
        import scripts.historical_foundry_verifier as verifier

        prepared, result = self._fixture(mode="publish")
        with mock.patch.object(
            verifier,
            "run_connected_historical_verification",
            return_value=result,
        ):
            observed = self.module._verify_prepared_historical_bundle(
                prepared=prepared, publish=True
            )
        self.assertIs(observed, result)
        self.assertIsNotNone(result["install_result"])
        self.assertEqual(
            dict(verifier.historical_replay_pointer_core(
                result["final_pointer"]
            )),
            result["pointer_core"],
        )

    def test_staged_offline_fixture_cannot_satisfy_production_command(self):
        import scripts.historical_foundry_verifier as verifier

        prepared, result = self._fixture(
            mode="dry-run", evidence_mode="offline_test_fixture"
        )
        with mock.patch.object(
            verifier,
            "run_connected_historical_verification",
            return_value=result,
        ):
            with self.assertRaisesRegex(
                self.module.HistoricalReplayEntrypointError,
                "production-connected evidence is required",
            ):
                self.module._verify_prepared_historical_bundle(
                    prepared=prepared, publish=False
                )

    def test_pointer_core_mismatch_is_rejected_before_publication(self):
        import scripts.historical_foundry_verifier as verifier

        prepared, result = self._fixture(mode="publish")
        prepared["bundle"]["pointer_core"]["manifest_sha256"] = "0" * 64
        with mock.patch.object(
            verifier,
            "run_connected_historical_verification",
            return_value=result,
        ):
            with self.assertRaisesRegex(
                self.module.HistoricalReplayEntrypointError,
                "connected verification result is invalid",
            ):
                self.module._verify_prepared_historical_bundle(
                    prepared=prepared, publish=True
                )

    def test_verified_publish_handoff_installs_exact_pointer_once(self):
        import scripts.historical_route_publication as publication

        prepared, result = self._fixture(mode="publish")
        prepared["verification"] = result
        installed = copy.deepcopy(result["final_pointer"])
        with mock.patch.object(
            publication,
            "publish_historical_replay_bundle",
            return_value=installed,
        ) as publish_pointer:
            observed = self.module._publish_verified_historical_bundle(
                prepared=prepared,
                verification=result,
                publish=True,
            )
        publish_pointer.assert_called_once_with(
            data_dir=prepared["data_dir"],
            pointer_publication=prepared["pointer_publication"],
            final_pointer_bytes=result["final_pointer_bytes"],
        )
        self.assertIs(observed, installed)
        self.assertIs(prepared["complete_pointer"], installed)
        self.assertEqual(prepared["verification_subject"].rereads, 2)

    def test_verified_dry_run_handoff_never_installs_pointer(self):
        import scripts.historical_route_publication as publication

        prepared, result = self._fixture(mode="dry-run")
        prepared["verification"] = result
        with mock.patch.object(
            publication,
            "publish_historical_replay_bundle",
        ) as publish_pointer:
            observed = self.module._publish_verified_historical_bundle(
                prepared=prepared,
                verification=result,
                publish=False,
            )
        self.assertIsNone(observed)
        publish_pointer.assert_not_called()
        self.assertNotIn("complete_pointer", prepared)

    def test_verified_publish_handoff_rejects_substituted_result(self):
        import scripts.historical_route_publication as publication

        prepared, result = self._fixture(mode="publish")
        prepared["verification"] = result
        substituted = dict(result)
        with mock.patch.object(
            publication,
            "publish_historical_replay_bundle",
        ) as publish_pointer:
            with self.assertRaisesRegex(
                self.module.HistoricalReplayEntrypointError,
                "publication handoff is invalid",
            ):
                self.module._publish_verified_historical_bundle(
                    prepared=prepared,
                    verification=substituted,
                    publish=True,
                )
        publish_pointer.assert_not_called()


class HistoricalReplayProductionControllerTests(unittest.TestCase):
    class Resource:
        def __init__(self, name, events):
            self.name = name
            self.events = events
            self.closed = 0

        def close(self):
            self.closed += 1
            self.events.append(("close", self.name))

    class Preflight:
        @property
        def identity_projection(self):
            return {
                "schema": "historical_clean_source_identity/v1",
                "head": "a" * 40,
            }

    def setUp(self):
        self.module = _entrypoint()

    def _fixture(self, *, publish, status):
        events = []
        run = self.Resource("run", events)
        lease = self.Resource("lease", events)
        pointer_core = {
            "schema": "route_historical_replay_pointer/v1",
            "bundle_stage": "route_historical_foundry_replay/v1",
            "replay_id": "replay:" + "1" * 64,
            "route_cohort_id": "cohort:" + "2" * 64,
            "manifest_sha256": "3" * 64,
        }
        selection = {
            "status": status,
            "selected_scenario_count": (
                10
                if status == "found_publishable_profitable_block"
                else 0
            ),
            "unresolved_candidate_count": 0,
        }
        raw = {
            "config": object(),
            "selection": selection,
            "run": (
                run
                if status == "found_publishable_profitable_block"
                else None
            ),
            "run_identity": {
                "run_id": "run:" + "4" * 64,
                "run_manifest_sha256": "5" * 64,
            },
            "publication_lease": (
                lease
                if status == "found_publishable_profitable_block"
                else None
            ),
        }
        prepared = {
            "mode": "publish" if publish else "dry-run",
            "bundle": {
                "replay_id": pointer_core["replay_id"],
                "manifest_sha256": pointer_core["manifest_sha256"],
                "pointer_core": pointer_core,
            },
        }
        final_pointer = {
            **pointer_core,
            "verification_report_sha256": "6" * 64,
        }
        verification = {
            "report_sha256": "6" * 64,
            "final_pointer": final_pointer,
        }
        published_pointer = final_pointer if publish else None
        arguments = self.module._parse_arguments([
            "scan", "--data-dir", "/immutable-data",
            "--publish" if publish else "--dry-run",
        ])
        return {
            "events": events,
            "run": run,
            "lease": lease,
            "raw": raw,
            "prepared": prepared,
            "verification": verification,
            "published_pointer": published_pointer,
            "arguments": arguments,
        }

    def _patch_pipeline(self, fixture):
        events = fixture["events"]

        def produce(**keywords):
            events.append(("produce", keywords))
            return fixture["raw"]

        def prepare(**keywords):
            events.append(("prepare", keywords))
            fixture["raw"]["run"] = None
            fixture["raw"]["publication_lease"] = None
            return fixture["prepared"]

        def verify(**keywords):
            events.append(("verify", keywords))
            return fixture["verification"]

        def publish_bundle(**keywords):
            events.append(("publish", keywords))
            return fixture["published_pointer"]

        def close(prepared):
            events.append(("close_prepared", prepared))

        stack = contextlib.ExitStack()
        stack.enter_context(mock.patch.object(
            self.module,
            "_produce_historical_raw_run",
            side_effect=produce,
        ))
        stack.enter_context(mock.patch.object(
            self.module,
            "_prepare_historical_replay_bundle",
            side_effect=prepare,
        ))
        verify_mock = stack.enter_context(mock.patch.object(
            self.module,
            "_verify_prepared_historical_bundle",
            side_effect=verify,
        ))
        publish_mock = stack.enter_context(mock.patch.object(
            self.module,
            "_publish_verified_historical_bundle",
            side_effect=publish_bundle,
        ))
        stack.enter_context(mock.patch.object(
            self.module,
            "_close_prepared_historical_bundle",
            side_effect=close,
        ))
        return stack, verify_mock, publish_mock

    def test_publish_scan_runs_full_pipeline_then_closes_prepared_state(self):
        fixture = self._fixture(
            publish=True,
            status="found_publishable_profitable_block",
        )
        patches, verify, publish_bundle = self._patch_pipeline(fixture)
        with patches:
            result = self.module._invoke_production_controller(
                fixture["arguments"], self.Preflight()
            )

        self.assertEqual(
            [event[0] for event in fixture["events"]],
            ["produce", "prepare", "verify", "publish", "close_prepared"],
        )
        verify.assert_called_once_with(
            prepared=fixture["prepared"], publish=True
        )
        publish_bundle.assert_called_once_with(
            prepared=fixture["prepared"],
            verification=fixture["verification"],
            publish=True,
        )
        self.assertEqual(result["status"], fixture["raw"]["selection"]["status"])
        self.assertEqual(
            result["published_pointer"], fixture["published_pointer"]
        )
        self.assertEqual(result["bundle"]["replay_id"], "replay:" + "1" * 64)

    def test_dry_run_validates_full_pipeline_without_publishing_pointer(self):
        fixture = self._fixture(
            publish=False,
            status="found_publishable_profitable_block",
        )
        patches, _verify, publish_bundle = self._patch_pipeline(fixture)
        with patches:
            result = self.module._invoke_production_controller(
                fixture["arguments"], self.Preflight()
            )

        publish_bundle.assert_called_once_with(
            prepared=fixture["prepared"],
            verification=fixture["verification"],
            publish=False,
        )
        self.assertIsNone(result["published_pointer"])
        self.assertEqual(result["mode"], "dry-run")

    def test_resolved_no_opportunity_stops_before_publication_pipeline(self):
        fixture = self._fixture(
            publish=True,
            status="no_publishable_profitable_block",
        )
        patches, verify, publish_bundle = self._patch_pipeline(fixture)
        with patches:
            result = self.module._invoke_production_controller(
                fixture["arguments"], self.Preflight()
            )

        self.assertEqual(
            [event[0] for event in fixture["events"]], ["produce"]
        )
        verify.assert_not_called()
        publish_bundle.assert_not_called()
        self.assertEqual(result["status"], "no_publishable_profitable_block")
        self.assertIsNone(result["bundle"])
        self.assertIsNone(result["verification"])

    def test_verification_failure_closes_prepared_state_and_never_publishes(self):
        fixture = self._fixture(
            publish=True,
            status="found_publishable_profitable_block",
        )
        patches, _verify, publish_bundle = self._patch_pipeline(fixture)
        with patches, mock.patch.object(
            self.module,
            "_verify_prepared_historical_bundle",
            side_effect=self.module.HistoricalReplayEntrypointError(
                "verification failed"
            ),
        ):
            with self.assertRaisesRegex(
                self.module.HistoricalReplayEntrypointError,
                "verification failed",
            ):
                self.module._invoke_production_controller(
                    fixture["arguments"], self.Preflight()
                )

        publish_bundle.assert_not_called()
        self.assertEqual(fixture["events"][-1][0], "close_prepared")

    def test_preparation_failure_closes_untransferred_raw_authority(self):
        fixture = self._fixture(
            publish=True,
            status="found_publishable_profitable_block",
        )
        with mock.patch.object(
            self.module,
            "_produce_historical_raw_run",
            return_value=fixture["raw"],
        ), mock.patch.object(
            self.module,
            "_prepare_historical_replay_bundle",
            side_effect=self.module.HistoricalReplayEntrypointError(
                "preparation failed"
            ),
        ):
            with self.assertRaisesRegex(
                self.module.HistoricalReplayEntrypointError,
                "preparation failed",
            ):
                self.module._invoke_production_controller(
                    fixture["arguments"], self.Preflight()
                )

        self.assertEqual(
            fixture["events"],
            [("close", "lease"), ("close", "run")],
        )


class HistoricalReplayAuditControllerTests(unittest.TestCase):
    class Subject:
        def __init__(self):
            self.rereads = 0
            self.closed = 0

        def reread_unchanged(self):
            self.rereads += 1

        def close(self):
            self.closed += 1

    class Preflight:
        @property
        def identity_projection(self):
            return {
                "schema": "historical_clean_source_identity/v1",
                "head": "a" * 40,
            }

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.data_dir = Path(self.temporary.name).resolve()
        self.module = _entrypoint()
        self.replay_id = "replay:" + "1" * 64
        self.pointer_core = {
            "schema": "route_historical_replay_pointer/v1",
            "bundle_stage": "route_historical_foundry_replay/v1",
            "replay_id": self.replay_id,
            "route_cohort_id": "cohort:" + "2" * 64,
            "manifest_sha256": "3" * 64,
        }
        self.retained_report = b"retained-report"
        self.pointer = {
            **self.pointer_core,
            "verification_report_sha256": hashlib.sha256(
                self.retained_report
            ).hexdigest(),
        }
        self.historical_root = (
            self.data_dir / "routes" / "historical"
        )
        self.bundle_path = (
            self.historical_root / "bundles" / self.replay_id
        )
        self.bundle_path.mkdir(parents=True)
        self.latest = self.historical_root / "latest.json"
        self.latest.write_bytes(json.dumps(
            self.pointer,
            allow_nan=False,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8"))

    def tearDown(self):
        self.temporary.cleanup()

    def _tree_snapshot(self):
        rows = []
        for path in sorted(
            self.historical_root.rglob("*"), key=lambda item: str(item)
        ):
            if path.is_file():
                rows.append((str(path.relative_to(self.historical_root)), path.read_bytes()))
            else:
                rows.append((str(path.relative_to(self.historical_root)), None))
        return tuple(rows)

    def _patched_audit_dependencies(self, *, run_side_effect=None):
        import scripts.historical_foundry_verifier as verifier
        import scripts.historical_route_publication as publication

        subject = self.Subject()
        validated = {
            "path": self.bundle_path,
            "replay_id": self.replay_id,
            "manifest_sha256": self.pointer_core["manifest_sha256"],
            "pointer_core": copy.deepcopy(self.pointer_core),
            "manifest": {
                "run_id": "run:" + "4" * 64,
                "run_manifest_sha256": "5" * 64,
            },
            "verification_subject": subject,
        }
        audit_report = {
            "schema": "route_historical_replay_verification/v1",
            "status": "verified",
            "evidence_mode": "production_connected",
        }
        audit_report_bytes = json.dumps(
            audit_report,
            allow_nan=False,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        audit_sha256 = hashlib.sha256(audit_report_bytes).hexdigest()
        audit_pointer = {
            **self.pointer_core,
            "verification_report_sha256": audit_sha256,
        }
        verification = {
            "schema": "historical_connected_verification_result/v1",
            "mode": "audit",
            "report": audit_report,
            "report_bytes": audit_report_bytes,
            "report_sha256": audit_sha256,
            "pointer_core": copy.deepcopy(self.pointer_core),
            "final_pointer": audit_pointer,
            "final_pointer_bytes": json.dumps(
                audit_pointer,
                allow_nan=False,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8"),
            "install_result": None,
        }
        stack = contextlib.ExitStack()
        validate = stack.enter_context(mock.patch.object(
            publication,
            "validate_historical_replay_bundle",
            return_value=validated,
        ))
        reread = stack.enter_context(mock.patch.object(
            publication,
            "_reread_historical_verification_report",
            return_value=self.retained_report,
        ))
        run = stack.enter_context(mock.patch.object(
            verifier,
            "run_connected_historical_verification",
            side_effect=(
                run_side_effect
                if run_side_effect is not None
                else lambda _subject, *, mode: verification
            ),
        ))
        stack.enter_context(mock.patch.object(
            verifier,
            "_require_historical_audit_report_parity",
            return_value=None,
            create=True,
        ))
        return stack, subject, verification, validate, reread, run

    def test_audit_pins_current_bundle_and_retains_zero_mutation(self):
        before = self._tree_snapshot()
        (
            patches, subject, verification, validate, reread, run,
        ) = self._patched_audit_dependencies()
        with patches:
            result = self.module._audit_latest_historical_replay_bundle(
                data_dir=self.data_dir,
                bundle_path=self.bundle_path,
            )

        self.assertEqual(self._tree_snapshot(), before)
        validate.assert_called_once_with(
            data_dir=self.data_dir,
            raw_root=(
                self.data_dir / "raw" / "historical-foundry-replay"
            ),
            bundle_path=self.bundle_path,
            expected_pointer_core=self.pointer_core,
        )
        run.assert_called_once_with(subject, mode="audit")
        self.assertGreaterEqual(reread.call_count, 2)
        self.assertGreaterEqual(subject.rereads, 2)
        self.assertEqual(subject.closed, 1)
        self.assertEqual(result["status"], "verified")
        self.assertEqual(
            result["verification"]["audit_report_sha256"],
            verification["report_sha256"],
        )
        self.assertEqual(result["published_pointer"], self.pointer)

    def test_audit_rejects_bundle_that_is_not_current_pointer_directory(self):
        other = self.historical_root / "bundles" / ("replay:" + "9" * 64)
        other.mkdir()
        patches, subject, _verification, validate, _reread, run = (
            self._patched_audit_dependencies()
        )
        with patches:
            with self.assertRaisesRegex(
                self.module.HistoricalReplayEntrypointError,
                "current historical pointer",
            ):
                self.module._audit_latest_historical_replay_bundle(
                    data_dir=self.data_dir, bundle_path=other,
                )
        validate.assert_not_called()
        run.assert_not_called()
        self.assertEqual(subject.closed, 0)

    def test_audit_detects_intervening_pointer_writer_without_rollback(self):
        attacker_bytes = b'{"attacker":"won"}'

        def replace_pointer(_subject, *, mode):
            self.assertEqual(mode, "audit")
            self.latest.write_bytes(attacker_bytes)
            return verification

        patches, subject, verification, _validate, _reread, _run = (
            self._patched_audit_dependencies(
                run_side_effect=replace_pointer
            )
        )
        with patches:
            with self.assertRaisesRegex(
                self.module.HistoricalReplayEntrypointError,
                "historical audit changed",
            ):
                self.module._audit_latest_historical_replay_bundle(
                    data_dir=self.data_dir,
                    bundle_path=self.bundle_path,
                )
        self.assertEqual(self.latest.read_bytes(), attacker_bytes)
        self.assertEqual(subject.closed, 1)

    def test_audit_rejects_noncanonical_report_handoff(self):
        patches, subject, verification, _validate, _reread, _run = (
            self._patched_audit_dependencies()
        )
        verification["report_bytes"] += b"\n"
        replacement_sha256 = hashlib.sha256(
            verification["report_bytes"]
        ).hexdigest()
        verification["report_sha256"] = replacement_sha256
        verification["final_pointer"][
            "verification_report_sha256"
        ] = replacement_sha256
        verification["final_pointer_bytes"] = json.dumps(
            verification["final_pointer"],
            allow_nan=False,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        with patches:
            with self.assertRaisesRegex(
                self.module.HistoricalReplayEntrypointError,
                "audit verification result is invalid",
            ):
                self.module._audit_latest_historical_replay_bundle(
                    data_dir=self.data_dir,
                    bundle_path=self.bundle_path,
                )
        self.assertEqual(subject.closed, 1)

    def test_verify_command_routes_only_to_audit_controller(self):
        arguments = self.module._parse_arguments([
            "verify", "--data-dir", str(self.data_dir),
            "--bundle", str(self.bundle_path),
        ])
        audit_result = {
            "status": "verified",
            "run_identity": {
                "run_id": "run:" + "4" * 64,
                "run_manifest_sha256": "5" * 64,
            },
            "bundle": {"replay_id": self.replay_id},
            "verification": {"audit_report_sha256": "6" * 64},
            "published_pointer": self.pointer,
        }
        with mock.patch.object(
            self.module,
            "_audit_latest_historical_replay_bundle",
            return_value=audit_result,
        ) as audit, mock.patch.object(
            self.module,
            "_produce_historical_raw_run",
            side_effect=AssertionError("verify entered scan"),
        ):
            result = self.module._invoke_production_controller(
                arguments, self.Preflight()
            )
        audit.assert_called_once_with(
            data_dir=self.data_dir, bundle_path=self.bundle_path
        )
        self.assertEqual(result["command"], "verify")
        self.assertEqual(result["mode"], "audit")
        self.assertEqual(result["source_identity"]["head"], "a" * 40)


class HistoricalReplayReferenceGcPlannerTests(unittest.TestCase):
    def setUp(self):
        self.module = _entrypoint()
        self.run_a = {
            "run_id": "run:" + "1" * 64,
            "run_manifest_sha256": "2" * 64,
        }
        self.run_b = {
            "run_id": "run:" + "3" * 64,
            "run_manifest_sha256": "4" * 64,
        }
        self.orphan_run = {
            "run_id": "run:" + "5" * 64,
            "run_manifest_sha256": "6" * 64,
        }
        self.replay_a = "replay:" + "7" * 64
        self.replay_b = "replay:" + "8" * 64
        self.cohort_a = "cohort:" + "9" * 64
        self.cohort_b = "cohort:" + "a" * 64
        self.report_a = "b" * 64
        self.orphan_report = "c" * 64

    def _inventory(self):
        return {
            "schema": "historical_reference_gc_validated_inventory/v1",
            "status": "validated",
            "complete_bundles": (
                {
                    "replay_id": self.replay_a,
                    **self.run_a,
                },
                {
                    "replay_id": self.replay_b,
                    **self.run_b,
                },
            ),
            "core_bundles": (
                {
                    "route_cohort_id": self.cohort_a,
                    **self.run_a,
                },
                {
                    "route_cohort_id": self.cohort_b,
                    **self.run_b,
                },
            ),
            "historical_pointers": {
                "core": {"route_cohort_id": self.cohort_a},
                "complete": {
                    "replay_id": self.replay_a,
                    "verification_report_sha256": self.report_a,
                },
            },
            "raw_runs": (self.run_a, self.run_b, self.orphan_run),
            "verification_reports": (
                {"sha256": self.report_a},
                {"sha256": self.orphan_report},
            ),
        }

    def test_only_unreferenced_raw_runs_and_reports_are_candidates(self):
        plan = self.module._plan_historical_reference_gc_inventory(
            self._inventory()
        )
        self.assertEqual(plan["status"], "planned")
        self.assertEqual(plan["delete_runs"], (self.orphan_run,))
        self.assertEqual(
            plan["protected_runs"], (self.run_a, self.run_b)
        )
        self.assertEqual(
            plan["delete_reports"], ({"sha256": self.orphan_report},)
        )
        self.assertEqual(
            plan["protected_reports"], ({"sha256": self.report_a},)
        )

    def test_retained_noncurrent_bundles_still_pin_their_raw_runs(self):
        inventory = self._inventory()
        inventory["historical_pointers"] = {
            "core": None,
            "complete": None,
        }
        plan = self.module._plan_historical_reference_gc_inventory(
            inventory
        )
        self.assertEqual(plan["status"], "planned")
        self.assertEqual(plan["protected_runs"], (self.run_a, self.run_b))
        self.assertEqual(plan["delete_runs"], (self.orphan_run,))
        self.assertEqual(plan["protected_reports"], ())
        self.assertEqual(
            plan["delete_reports"],
            ({"sha256": self.report_a}, {"sha256": self.orphan_report}),
        )

    def test_any_invalid_inventory_produces_an_empty_deletion_set(self):
        attacks = []
        invalid_status = self._inventory()
        invalid_status["status"] = "invalid"
        attacks.append(invalid_status)
        missing_pointer_target = self._inventory()
        missing_pointer_target["historical_pointers"]["core"] = {
            "route_cohort_id": "cohort:" + "f" * 64
        }
        attacks.append(missing_pointer_target)
        missing_raw = self._inventory()
        missing_raw["raw_runs"] = (
            self.run_a, self.orphan_run
        )
        attacks.append(missing_raw)
        conflicting_duplicate = self._inventory()
        conflicting_duplicate["raw_runs"] = (
            *conflicting_duplicate["raw_runs"],
            {
                "run_id": self.run_a["run_id"],
                "run_manifest_sha256": "d" * 64,
            },
        )
        attacks.append(conflicting_duplicate)
        missing_report = self._inventory()
        missing_report["verification_reports"] = (
            {"sha256": self.orphan_report},
        )
        attacks.append(missing_report)
        extra_field = self._inventory()
        extra_field["raw_runs"] = (
            {**self.run_a, "path": "/must-not-be-trusted"},
            self.run_b,
            self.orphan_run,
        )
        attacks.append(extra_field)

        for inventory in attacks:
            with self.subTest(attack=attacks.index(inventory)):
                plan = self.module._plan_historical_reference_gc_inventory(
                    inventory
                )
                self.assertEqual(
                    plan["status"], "blocked_invalid_inventory"
                )
                self.assertEqual(plan["delete_runs"], ())
                self.assertEqual(plan["delete_reports"], ())


class HistoricalReplayReferenceGcInventoryAdapterTests(unittest.TestCase):
    class Handle:
        def __init__(self, projection=None):
            self.projection = projection
            self.reread_count = 0
            self.closed = False

        def identity_projection(self):
            if self.projection is None:
                raise AssertionError("projection was not configured")
            return dict(self.projection)

        def reread_unchanged(self):
            if self.closed:
                raise AssertionError("closed inventory handle was reread")
            self.reread_count += 1

        def close(self):
            if self.closed:
                raise AssertionError("inventory handle was closed twice")
            self.closed = True

    def setUp(self):
        self.module = _entrypoint()
        self.publication = importlib.import_module(
            "scripts.historical_route_publication"
        )
        self.verifier = importlib.import_module(
            "scripts.historical_foundry_verifier"
        )
        self.storage = importlib.import_module(
            "scripts.historical_foundry_storage"
        )
        self.route_publication = importlib.import_module(
            "scripts.route_publication"
        )
        self.temporary = tempfile.TemporaryDirectory()
        self.data_dir = Path(self.temporary.name)
        self.canonical_data_dir = (
            self.route_publication._absolute_without_symlink_resolution(
                self.data_dir
            )
        )
        self.raw_root = (
            self.data_dir / "raw" / "historical-foundry-replay"
        )
        self.historical_root = (
            self.data_dir / "routes" / "historical"
        )
        self.complete_root = self.historical_root / "bundles"
        self.core_root = self.historical_root / "core"
        self.core_bundles = self.core_root / "bundles"
        self.report_root = (
            self.historical_root / "verifications" / "by-sha256"
        )
        for directory in (
            self.raw_root, self.complete_root, self.core_bundles,
            self.report_root,
        ):
            directory.mkdir(parents=True)

        self.run_id = "run:" + "1" * 64
        self.orphan_run_id = "run:" + "2" * 64
        self.run_manifest_bytes = b"validated-run-manifest"
        self.orphan_manifest_bytes = b"validated-orphan-manifest"
        self.run_manifest_sha256 = hashlib.sha256(
            self.run_manifest_bytes
        ).hexdigest()
        self.orphan_manifest_sha256 = hashlib.sha256(
            self.orphan_manifest_bytes
        ).hexdigest()
        for run_id, payload in (
            (self.run_id, self.run_manifest_bytes),
            (self.orphan_run_id, self.orphan_manifest_bytes),
        ):
            directory = self.raw_root / run_id[4:]
            directory.mkdir()
            directory.chmod(0o700)
            manifest_path = directory / "run_manifest.json"
            manifest_path.write_bytes(payload)
            manifest_path.chmod(0o600)

        self.replay_id = "replay:" + "3" * 64
        self.route_cohort_id = "cohort:" + "4" * 64
        (self.complete_root / self.replay_id).mkdir()
        (self.core_bundles / self.route_cohort_id).mkdir()
        self.core_manifest = {
            "schema": "route_historical_replay_core_manifest/v1",
            "bundle_stage": "route_historical_replay_core/v1",
            "route_cohort_id": self.route_cohort_id,
            "raw_evidence_run_id": self.run_id,
            "raw_run_manifest_sha256": self.run_manifest_sha256,
        }
        self.core_manifest_bytes = self.publication._json_file_bytes(
            self.core_manifest
        )
        self.core_manifest_sha256 = hashlib.sha256(
            self.core_manifest_bytes
        ).hexdigest()
        (
            self.core_bundles / self.route_cohort_id / "manifest.json"
        ).write_bytes(self.core_manifest_bytes)
        self.core_pointer = self.publication._pointer(
            self.core_manifest, self.core_manifest_sha256
        )
        self.core_pointer_bytes = self.publication._json_file_bytes(
            self.core_pointer
        )
        self.core_pointer_sha256 = hashlib.sha256(
            self.core_pointer_bytes
        ).hexdigest()
        (self.core_root / "latest.json").write_bytes(
            self.core_pointer_bytes
        )

        self.complete_manifest_sha256 = "5" * 64
        self.current_report_bytes = b"validated-current-report"
        self.orphan_report_bytes = b"validated-orphan-report"
        self.current_report_sha256 = hashlib.sha256(
            self.current_report_bytes
        ).hexdigest()
        self.orphan_report_sha256 = hashlib.sha256(
            self.orphan_report_bytes
        ).hexdigest()
        for digest, payload in (
            (self.current_report_sha256, self.current_report_bytes),
            (self.orphan_report_sha256, self.orphan_report_bytes),
        ):
            report_path = self.report_root / (digest + ".json")
            report_path.write_bytes(payload)
            report_path.chmod(0o600)
        self.complete_pointer_core = {
            "schema": "route_historical_replay_pointer/v1",
            "bundle_stage": "route_historical_foundry_replay/v1",
            "replay_id": self.replay_id,
            "route_cohort_id": self.route_cohort_id,
            "manifest_sha256": self.complete_manifest_sha256,
        }
        self.complete_pointer = {
            **self.complete_pointer_core,
            "verification_report_sha256": self.current_report_sha256,
        }
        self.complete_pointer_bytes = self.verifier._canonical_bytes(
            self.complete_pointer
        )
        (self.historical_root / "latest.json").write_bytes(
            self.complete_pointer_bytes
        )
        self.handles = []

    def tearDown(self):
        self.temporary.cleanup()

    def _handle(self, projection=None):
        handle = self.Handle(projection)
        self.handles.append(handle)
        return handle

    def _run_with_validators(self, operation, *, complete_callback=None):
        raw_identities = {
            self.run_id: self.run_manifest_sha256,
            self.orphan_run_id: self.orphan_manifest_sha256,
        }

        def open_run(*, data_dir, run_id, expected_manifest_sha256):
            self.assertEqual(data_dir, self.canonical_data_dir)
            self.assertEqual(
                raw_identities[run_id], expected_manifest_sha256
            )
            return self._handle({
                "run_id": run_id,
                "run_manifest_sha256": expected_manifest_sha256,
            })

        def validate_complete(
            *, data_dir, raw_root, bundle_path,
            expected_pointer_core=None,
        ):
            self.assertEqual(data_dir, self.canonical_data_dir)
            self.assertEqual(
                raw_root,
                self.canonical_data_dir
                / "raw" / "historical-foundry-replay",
            )
            self.assertEqual(
                bundle_path,
                self.canonical_data_dir
                / "routes" / "historical" / "bundles" / self.replay_id,
            )
            self.assertIsNone(expected_pointer_core)
            if complete_callback is not None:
                complete_callback()
            return {
                "path": bundle_path,
                "manifest_sha256": self.complete_manifest_sha256,
                "manifest": {
                    "replay_id": self.replay_id,
                    "route_cohort_id": self.route_cohort_id,
                    "run_id": self.run_id,
                    "run_manifest_sha256": self.run_manifest_sha256,
                },
                "pointer_core": dict(self.complete_pointer_core),
                "verification_subject": self._handle(),
            }

        core_projection = {
            "run_id": self.run_id,
            "run_manifest_sha256": self.run_manifest_sha256,
            "core_manifest_sha256": self.core_manifest_sha256,
            "core_pointer_sha256": self.core_pointer_sha256,
            "core_pointer": dict(self.core_pointer),
        }

        def load_core(**kwargs):
            self.assertEqual(kwargs, {
                "data_dir": self.canonical_data_dir,
                "route_cohort_id": self.route_cohort_id,
                "expected_manifest_sha256": self.core_manifest_sha256,
                "expected_pointer_sha256": self.core_pointer_sha256,
            })
            return self._handle(core_projection)

        def load_latest_core(*, data_dir):
            self.assertEqual(data_dir, self.canonical_data_dir)
            return self._handle(core_projection)

        report_records = {
            self.current_report_bytes: {
                "replay_id": self.replay_id,
                "route_cohort_id": self.route_cohort_id,
                "manifest_sha256": self.complete_manifest_sha256,
            },
            self.orphan_report_bytes: {
                "replay_id": "replay:" + "6" * 64,
                "route_cohort_id": "cohort:" + "7" * 64,
                "manifest_sha256": "8" * 64,
            },
        }

        def validate_report(report_bytes):
            return dict(report_records[report_bytes])

        def validate_retained(*, report_bytes, pointer_core):
            report = report_records[report_bytes]
            self.assertEqual(pointer_core, {
                "schema": "route_historical_replay_pointer/v1",
                "bundle_stage": "route_historical_foundry_replay/v1",
                **report,
            })
            return dict(report)

        with contextlib.ExitStack() as stack:
            stack.enter_context(mock.patch.object(
                self.storage, "open_validated_run", side_effect=open_run
            ))
            stack.enter_context(mock.patch.object(
                self.publication, "validate_historical_replay_bundle",
                side_effect=validate_complete,
            ))
            stack.enter_context(mock.patch.object(
                self.publication,
                "_load_immutable_historical_replay_core",
                side_effect=load_core,
            ))
            stack.enter_context(mock.patch.object(
                self.publication, "load_latest_historical_replay_core",
                side_effect=load_latest_core,
            ))
            stack.enter_context(mock.patch.object(
                self.verifier,
                "_validate_exact_historical_verification_report",
                side_effect=validate_report,
            ))
            stack.enter_context(mock.patch.object(
                self.verifier,
                "_validate_retained_historical_verification_report",
                side_effect=validate_retained,
            ))
            stack.enter_context(mock.patch.object(
                self.publication,
                "_reread_historical_verification_report",
                return_value=self.current_report_bytes,
            ))
            return operation()

    def _run_adapter(self, *, complete_callback=None):
        return self._run_with_validators(
            lambda: self.module
            ._build_validated_historical_reference_gc_inventory(
                data_dir=self.data_dir
            ),
            complete_callback=complete_callback,
        )

    def _apply_gc(self, *, complete_callback=None):
        return self._run_with_validators(
            lambda: self.module._apply_historical_reference_gc(
                data_dir=self.data_dir
            ),
            complete_callback=complete_callback,
        )

    def test_descriptor_inventory_drives_only_unreferenced_candidates(self):
        inventory = self._run_adapter()
        self.assertEqual(inventory["status"], "validated")
        self.assertEqual(inventory["complete_bundles"], ({
            "replay_id": self.replay_id,
            "run_id": self.run_id,
            "run_manifest_sha256": self.run_manifest_sha256,
        },))
        self.assertEqual(inventory["core_bundles"], ({
            "route_cohort_id": self.route_cohort_id,
            "run_id": self.run_id,
            "run_manifest_sha256": self.run_manifest_sha256,
        },))
        plan = self.module._plan_historical_reference_gc_inventory(
            inventory
        )
        self.assertEqual(plan["status"], "planned")
        self.assertEqual(plan["delete_runs"], ({
            "run_id": self.orphan_run_id,
            "run_manifest_sha256": self.orphan_manifest_sha256,
        },))
        self.assertEqual(plan["delete_reports"], ({
            "sha256": self.orphan_report_sha256,
        },))
        self.assertTrue(self.handles)
        self.assertTrue(all(handle.closed for handle in self.handles))
        self.assertTrue(all(
            handle.reread_count >= 2 for handle in self.handles
        ))

    def test_symlinked_run_directory_blocks_every_deletion(self):
        os.symlink(
            str(self.raw_root / self.run_id[4:]),
            str(self.raw_root / ("0" * 64)),
        )
        inventory = self._run_adapter()
        self.assertEqual(inventory["status"], "invalid")
        plan = self.module._plan_historical_reference_gc_inventory(
            inventory
        )
        self.assertEqual(plan["status"], "blocked_invalid_inventory")
        self.assertEqual(plan["delete_runs"], ())
        self.assertEqual(plan["delete_reports"], ())

    def test_same_byte_pointer_replacement_blocks_every_deletion(self):
        def replace_pointer():
            temporary = self.historical_root / ".same-byte-race"
            temporary.write_bytes(self.complete_pointer_bytes)
            os.replace(
                str(temporary),
                str(self.historical_root / "latest.json"),
            )

        inventory = self._run_adapter(
            complete_callback=replace_pointer
        )
        self.assertEqual(inventory["status"], "invalid")
        plan = self.module._plan_historical_reference_gc_inventory(
            inventory
        )
        self.assertEqual(plan["status"], "blocked_invalid_inventory")
        self.assertEqual(plan["delete_runs"], ())
        self.assertEqual(plan["delete_reports"], ())

    def test_one_shot_gc_removes_only_planned_orphans(self):
        nested = self.raw_root / self.orphan_run_id[4:] / "nested"
        nested.mkdir(mode=0o700)
        nested_member = nested / "evidence.json"
        nested_member.write_bytes(b"orphan-evidence")
        nested_member.chmod(0o600)
        pointer_before = (
            self.historical_root / "latest.json"
        ).read_bytes()

        result = self._apply_gc()

        self.assertEqual(result, {
            "schema": "historical_reference_gc_result/v1",
            "status": "applied",
            "deleted_runs": ({
                "run_id": self.orphan_run_id,
                "run_manifest_sha256": self.orphan_manifest_sha256,
            },),
            "deleted_reports": ({
                "sha256": self.orphan_report_sha256,
            },),
        })
        self.assertTrue((self.raw_root / self.run_id[4:]).is_dir())
        self.assertFalse(
            (self.raw_root / self.orphan_run_id[4:]).exists()
        )
        self.assertTrue((
            self.report_root / (self.current_report_sha256 + ".json")
        ).is_file())
        self.assertFalse((
            self.report_root / (self.orphan_report_sha256 + ".json")
        ).exists())
        self.assertEqual(
            (self.historical_root / "latest.json").read_bytes(),
            pointer_before,
        )
        self.assertFalse(any(
            name.startswith(".historical-gc-")
            for name in os.listdir(self.raw_root)
        ))
        self.assertFalse(any(
            name.startswith(".historical-gc-")
            for name in os.listdir(self.report_root)
        ))

    def test_gc_preflight_hardlink_failure_leaves_every_orphan(self):
        orphan = self.raw_root / self.orphan_run_id[4:]
        first = orphan / "linked-evidence.json"
        second = orphan / "linked-evidence-copy.json"
        first.write_bytes(b"must-remain")
        first.chmod(0o600)
        os.link(str(first), str(second))

        result = self._apply_gc()

        self.assertEqual(result["status"], "blocked_validation")
        self.assertEqual(result["deleted_runs"], ())
        self.assertEqual(result["deleted_reports"], ())
        self.assertTrue(orphan.is_dir())
        self.assertTrue(first.is_file())
        self.assertTrue(second.is_file())
        self.assertTrue((
            self.report_root / (self.orphan_report_sha256 + ".json")
        ).is_file())

    def test_gc_pointer_race_leaves_every_orphan(self):
        def replace_pointer():
            temporary = self.historical_root / ".gc-pointer-race"
            temporary.write_bytes(self.complete_pointer_bytes)
            os.replace(
                str(temporary),
                str(self.historical_root / "latest.json"),
            )

        result = self._apply_gc(complete_callback=replace_pointer)

        self.assertEqual(result["status"], "blocked_invalid_inventory")
        self.assertEqual(result["deleted_runs"], ())
        self.assertEqual(result["deleted_reports"], ())
        self.assertTrue(
            (self.raw_root / self.orphan_run_id[4:]).is_dir()
        )
        self.assertTrue((
            self.report_root / (self.orphan_report_sha256 + ".json")
        ).is_file())

    def test_gc_post_isolation_pointer_race_rolls_candidates_back(self):
        real_rename = (
            self.route_publication._rename_directory_noreplace_at
        )

        def rename_then_race(*args, **kwargs):
            result = real_rename(*args, **kwargs)
            if args[1] == self.orphan_report_sha256 + ".json":
                temporary = self.historical_root / ".post-isolation-race"
                temporary.write_bytes(self.complete_pointer_bytes)
                os.replace(
                    str(temporary),
                    str(self.historical_root / "latest.json"),
                )
            return result

        with mock.patch.object(
            self.route_publication,
            "_rename_directory_noreplace_at",
            side_effect=rename_then_race,
        ):
            result = self._apply_gc()

        self.assertEqual(result["status"], "blocked_validation")
        self.assertEqual(result["deleted_runs"], ())
        self.assertEqual(result["deleted_reports"], ())
        self.assertTrue(
            (self.raw_root / self.orphan_run_id[4:]).is_dir()
        )
        self.assertTrue((
            self.report_root / (self.orphan_report_sha256 + ".json")
        ).is_file())
        self.assertFalse(any(
            name.startswith(".historical-gc-")
            for name in os.listdir(self.raw_root)
        ))
        self.assertFalse(any(
            name.startswith(".historical-gc-")
            for name in os.listdir(self.report_root)
        ))

    def test_gc_post_rename_stat_failure_rolls_candidate_back(self):
        real_stat = self.module.os.stat
        injected = [False]

        def fail_first_isolated_stat(path, *args, **kwargs):
            if (
                not injected[0]
                and type(path) is str
                and path.startswith(".historical-gc-run-")
            ):
                injected[0] = True
                raise OSError("controlled post-rename stat failure")
            return real_stat(path, *args, **kwargs)

        with mock.patch.object(
            self.module.os, "stat", side_effect=fail_first_isolated_stat
        ):
            result = self._apply_gc()

        self.assertTrue(injected[0])
        self.assertEqual(result["status"], "blocked_validation")
        self.assertTrue(
            (self.raw_root / self.orphan_run_id[4:]).is_dir()
        )
        self.assertTrue((
            self.report_root / (self.orphan_report_sha256 + ".json")
        ).is_file())
        self.assertFalse(any(
            name.startswith(".historical-gc-")
            for name in os.listdir(self.raw_root)
        ))

    def test_isolated_tree_inventory_drift_prevents_recursive_delete(self):
        candidate = self.raw_root / self.orphan_run_id[4:]
        parent_fd = os.open(
            str(self.raw_root),
            os.O_RDONLY | getattr(os, "O_DIRECTORY", 0),
        )
        try:
            details, inventory = (
                self.module._preflight_historical_gc_tree_at(
                    parent_fd=parent_fd,
                    name=self.orphan_run_id[4:],
                )
            )
            added = candidate / "late-private-file.json"
            added.write_bytes(b"must-not-be-deleted")
            added.chmod(0o600)
            with self.assertRaises(
                self.module.HistoricalReplayEntrypointError
            ):
                self.module._remove_historical_gc_tree_at(
                    parent_fd=parent_fd,
                    name=self.orphan_run_id[4:],
                    expected=details,
                    expected_inventory=inventory,
                )
        finally:
            os.close(parent_fd)
        self.assertTrue(candidate.is_dir())
        self.assertTrue(added.is_file())
        self.assertTrue((candidate / "run_manifest.json").is_file())


if __name__ == "__main__":
    unittest.main()
