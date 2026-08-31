import base64
from contextlib import redirect_stderr
import hashlib
import importlib
import inspect
import io
import json
import os
from pathlib import Path
import shutil
import stat
import sys
import tarfile
import tempfile
import threading
import time
import traceback
import unittest
from unittest import mock

from scripts import bootstrap_historical_foundry_toolchain as toolchain


FORGE_STD_COMMIT = "620536fa5277db4e3fd46772d5cbc1ea0696fb43"
FOUNDRY_COMMIT = "4072e48705af9d93e3c0f6e29e93b5e9a40caed8"
REAL_PROJECT_ROOT = Path(__file__).resolve().parents[1]

KAT_FIXTURE_BYTES = (
    b'{"archive_calls":[{"block_reference":"0x17d7840","calldata":"0x0902f1ac","method":"getReserves()","raw_response":"0x0000000000000000000000000000000000000000000051e38767437fac1d4c0f00000000000000000000000000000000000000000000001d6f8183a4807354760000000000000000000000000000000000000000000000000000000069f49013","response_sha256":"204e4b1706f10e75947b770017a684d4c3379a17dbd1ea54851f447544f58461","role":"uniswap_v2_uni_weth_reserves","target":"0xd3d2e2692501a5c9ca623199d38826e513033a17"},'
    b'{"block_reference":"0x17d7840","calldata":"0x0902f1ac","method":"getReserves()","raw_response":"0x0000000000000000000000000000000000000000000000bd762b5d69a8be9e1700000000000000000000000000000000000000000000000044406e0af95d0c040000000000000000000000000000000000000000000000000000000069f47c0f","response_sha256":"7411473045715ec073ac3cc12a47475135f8de7883f59ec9d49657e083d06e33","role":"sushiswap_v2_uni_weth_reserves","target":"0xdafd66636e2561b0284edde37e42d192f2844d40"},'
    b'{"block_reference":"0x17d7840","calldata":"0xfeaf968c","method":"latestRoundData()","raw_response":"0x000000000000000000000000000000000000000000000007000000000000701e000000000000000000000000000000000000000000000000000000353848f6320000000000000000000000000000000000000000000000000000000069f4963f0000000000000000000000000000000000000000000000000000000069f4964f000000000000000000000000000000000000000000000007000000000000701e","response_sha256":"e6b59059a6b3440c906a9a24b007a64b965977f2b99e746105f98ed1af5376ad","role":"chainlink_eth_usd_latest_round","target":"0x5f4ec3df9cbd43714fe2740f5e3616155c5b8419"}],'
    b'"block_header":{"base_fee":"0x478d0e7f","gas_limit":"0x3938700","gas_used":"0x2035c7b","hash":"0xf398976165ca4756c77fc6b61111fa1102d431eb03082417ecce38b36308d728","number_decimal":25000000,"number_hex":"0x17d7840","parent_hash":"0xc5a79102dcb47469ef357021c974bbbb92df3a1f3cfbcb5fdc0f9b36fb75e2c7","state_root":"0x055eba2b2b3daa967118fe831b0988cb27434e274f97f66cc67dcaa16dbe417f","timestamp_hex":"0x69f497f3","timestamp_utc":"2026-05-01T12:09:23Z"},'
    b'"chain_id":1,"pair_identities":[{"pair_address":"0xd3d2e2692501a5c9ca623199d38826e513033a17","venue_id":"uniswap_v2"},{"pair_address":"0xdafd66636e2561b0284edde37e42d192f2844d40","venue_id":"sushiswap_v2"}],"schema":"historical_foundry_kat/v1"}\n'
)
KAT_FIXTURE_VALUE = json.loads(KAT_FIXTURE_BYTES.decode("ascii"))
KAT_UNISWAP_RESPONSE, KAT_SUSHISWAP_RESPONSE, KAT_CHAINLINK_RESPONSE = (
    row["raw_response"] for row in KAT_FIXTURE_VALUE["archive_calls"]
)


def _sha256(payload):
    return hashlib.sha256(payload).hexdigest()


def _script(stdout, *, delay=False):
    pause = "sleep 0.25\n" if delay else ""
    return ("#!/bin/sh\n" + pause + "printf '%s\\n' '" + stdout + "'\n").encode("ascii")


def _archive(members):
    payload = io.BytesIO()
    with tarfile.open(fileobj=payload, mode="w:gz") as archive:
        for name, body in members.items():
            info = tarfile.TarInfo(name)
            info.mode = 0o755
            info.size = len(body)
            archive.addfile(info, io.BytesIO(body))
    return payload.getvalue()


def _sigstore_bundle(archive_digest, issuer, san):
    certificate_projection = (issuer + "\n" + san).encode("utf-8")
    value = {
        "mediaType": "application/vnd.dev.sigstore.bundle+json;version=0.3",
        "verificationMaterial": {
            "certificate": {
                "rawBytes": base64.b64encode(certificate_projection).decode("ascii")
            },
            "tlogEntries": [],
        },
        "messageSignature": {
            "messageDigest": {
                "algorithm": "SHA2_256",
                "digest": base64.b64encode(bytes.fromhex(archive_digest)).decode("ascii"),
            },
            "signature": "AA==",
        },
    }
    return json.dumps(value, sort_keys=True, separators=(",", ":")).encode("utf-8")


class HistoricalFoundryToolchainTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self):
        self.temporary.cleanup()

    def _write_project_inputs(self):
        forge_std = self.root / "lib" / "forge-std"
        forge_std.parent.mkdir(parents=True)
        shutil.copytree(
            REAL_PROJECT_ROOT / "lib" / "forge-std",
            forge_std,
            ignore=shutil.ignore_patterns(".git"),
        )
        gitdir = self.root / "submodule-git"
        gitdir.mkdir()
        (gitdir / "HEAD").write_text(FORGE_STD_COMMIT + "\n", encoding="ascii")
        (forge_std / ".git").write_text(
            "gitdir: ../../submodule-git\n", encoding="ascii"
        )
        for name, payload in toolchain._REVIEWED_PROJECT_FILES.items():
            path = self.root / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_bytes(payload)

    def _mock_assets(self):
        binaries = {
            "forge": _script("forge Version: 1.7.1\nCommit SHA: " + FOUNDRY_COMMIT),
            "cast": _script("cast Version: 1.7.1\nCommit SHA: " + FOUNDRY_COMMIT),
            "anvil": _script("anvil Version: 1.7.1\nCommit SHA: " + FOUNDRY_COMMIT + "\nosaka"),
        }
        archive = _archive(dict(binaries, chisel=_script("unused")))
        archive_digest = _sha256(archive)
        checksum = (
            archive_digest + "  foundry_v1.7.1_darwin_arm64.tar.gz\n"
        ).encode("ascii")
        sigstore = _sigstore_bundle(
            archive_digest,
            toolchain._SIGSTORE_ISSUER,
            toolchain._SIGSTORE_SAN,
        )
        spdx = b'{"spdxVersion":"SPDX-2.3"}\n'
        solc = _script("solc, the solidity compiler commandline interface\nVersion: 0.8.36+commit.8a079791.Darwin.appleclang")
        assets = {
            toolchain._FOUNDRY_ARCHIVE_URL: archive,
            toolchain._FOUNDRY_CHECKSUM_URL: checksum,
            toolchain._FOUNDRY_SIGSTORE_URL: sigstore,
            toolchain._FOUNDRY_SPDX_URL: spdx,
            toolchain._SOLC_URL: solc,
        }
        hashes = {url: _sha256(payload) for url, payload in assets.items()}
        return binaries, assets, hashes

    def _install_fake_toolchain(self, *, delayed_forge=False):
        if not (self.root / "foundry.toml").exists():
            self._write_project_inputs()
        source_lock = toolchain._SOURCE_LOCK_SHA256
        binary_dir = (
            self.root / ".historical-foundry" / "toolchains" / source_lock / "bin"
        )
        binary_dir.mkdir(parents=True)
        bodies = {
            "forge": _script("forge Version: 1.7.1\nCommit SHA: " + FOUNDRY_COMMIT, delay=delayed_forge),
            "cast": _script("cast Version: 1.7.1\nCommit SHA: " + FOUNDRY_COMMIT),
            "anvil": _script("anvil Version: 1.7.1\nCommit SHA: " + FOUNDRY_COMMIT + "\nosaka"),
            "solc": _script("Version: 0.8.36+commit.8a079791.Darwin.appleclang"),
        }
        for name, body in bodies.items():
            path = binary_dir / name
            path.write_bytes(body)
            path.chmod(0o700)
        for parent in (
            self.root / ".historical-foundry",
            self.root / ".historical-foundry" / "toolchains",
            binary_dir.parent,
            binary_dir,
        ):
            parent.chmod(0o700)
        return binary_dir, {name: _sha256(body) for name, body in bodies.items()}

    def _write_executor_build_fixture(self):
        source = self.root / "foundry" / "src" / "TwoVenueV2Executor.sol"
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_text("contract TwoVenueV2Executor {}\n", encoding="ascii")
        artifact = self.root / "out" / "TwoVenueV2Executor.sol" / "TwoVenueV2Executor.json"
        artifact.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "abi": [],
            "bytecode": {"object": "0x6000"},
            "deployedBytecode": {
                "immutableReferences": {},
                "object": "0x6001",
            },
        }
        artifact.write_bytes(
            json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("ascii")
        )
        return source, artifact

    def _open_descriptor_capability(self):
        expected = dict(toolchain._EXPECTED_BINARY_SHA256)
        expected["solc"] = toolchain._SOLC_SHA256
        binary_fds = tuple(
            os.open(os.devnull, os.O_RDONLY) for _name in toolchain._BINARY_NAMES
        )
        directory_fds = tuple(os.open(os.devnull, os.O_RDONLY) for _index in range(3))
        binaries = {
            name: (fd, (), expected[name])
            for name, fd in zip(toolchain._BINARY_NAMES, binary_fds)
        }
        directories = tuple((fd, None, None, ()) for fd in directory_fds)
        capability = toolchain.ReviewedHistoricalToolchain(
            directories,
            binaries,
            expected,
        )
        return capability, binary_fds + tuple(reversed(directory_fds))

    def test_close_preserves_first_control_and_drains_every_descriptor(self):
        real_close = os.close
        real_fstat = os.fstat
        scenarios = (
            (
                KeyboardInterrupt("first keyboard control"),
                SystemExit("later system control"),
            ),
            (
                SystemExit("first system control"),
                KeyboardInterrupt("later keyboard control"),
            ),
        )
        for first_control, later_control in scenarios:
            with self.subTest(control=type(first_control).__name__):
                capability, close_order = self._open_descriptor_capability()
                attempts = []

                def controlled_close(fd):
                    attempts.append(fd)
                    real_close(fd)
                    if len(attempts) == 2:
                        raise first_control
                    if len(attempts) == 3:
                        raise OSError("ordinary cleanup secret")
                    if len(attempts) == 4:
                        raise later_control

                caught = None
                try:
                    with mock.patch.object(toolchain.os, "close", new=controlled_close):
                        try:
                            capability._close()
                        except BaseException as error:
                            caught = error
                        capability._close()
                    self.assertIs(caught, first_control)
                    self.assertEqual(attempts, list(close_order))
                    for fd in close_order:
                        with self.assertRaises(OSError):
                            real_fstat(fd)
                finally:
                    for fd in close_order:
                        try:
                            real_close(fd)
                        except OSError:
                            pass
                caught = None
                first_control = first_control.with_traceback(None)
                later_control = later_control.with_traceback(None)

    def test_close_later_control_outweighs_ordinary_failure(self):
        real_close = os.close
        real_fstat = os.fstat
        capability, close_order = self._open_descriptor_capability()
        control = KeyboardInterrupt("later control sentinel")
        attempts = []

        def controlled_close(fd):
            attempts.append(fd)
            real_close(fd)
            if len(attempts) == 1:
                raise OSError("ordinary cleanup marker")
            if len(attempts) == 2:
                raise control

        caught = None
        try:
            with mock.patch.object(toolchain.os, "close", new=controlled_close):
                try:
                    capability._close()
                except BaseException as error:
                    caught = error
            self.assertIs(caught, control)
            self.assertEqual(attempts, list(close_order))
            for fd in close_order:
                with self.assertRaises(OSError):
                    real_fstat(fd)
        finally:
            for fd in close_order:
                try:
                    real_close(fd)
                except OSError:
                    pass
        caught = None
        control = control.with_traceback(None)

    def test_close_sanitizes_ordinary_failure_after_full_drain(self):
        real_close = os.close
        real_fstat = os.fstat
        capability, close_order = self._open_descriptor_capability()
        attempts = []
        secret = "PRIVATE-CLOSE-FAILURE-MARKER"

        def controlled_close(fd):
            attempts.append(fd)
            real_close(fd)
            if len(attempts) == 2:
                raise OSError(secret)

        caught = None
        try:
            with mock.patch.object(toolchain.os, "close", new=controlled_close):
                try:
                    capability._close()
                except BaseException as error:
                    caught = error
                capability._close()
            self.assertIs(type(caught), toolchain.HistoricalFoundryToolchainError)
            self.assertEqual(str(caught), "toolchain_descriptor_cleanup_failed")
            self.assertIsNone(caught.__cause__)
            self.assertIsNone(caught.__context__)
            self.assertNotIn(secret, repr(caught))
            self.assertEqual(attempts, list(close_order))
            for fd in close_order:
                with self.assertRaises(OSError):
                    real_fstat(fd)
        finally:
            for fd in close_order:
                try:
                    real_close(fd)
                except OSError:
                    pass

    def test_close_resumes_when_trace_interrupts_before_an_attempt(self):
        real_close = os.close
        real_fstat = os.fstat
        capability, close_order = self._open_descriptor_capability()
        attempts = []
        marker = KeyboardInterrupt("before close attempt")
        fired = []

        def recording_close(fd):
            attempts.append(fd)
            real_close(fd)

        def tracer(frame, event, _argument):
            if (
                not fired
                and event == "line"
                and frame.f_code.co_name == "_close"
                and attempts == [close_order[0]]
                and frame.f_locals.get("fd") == close_order[1]
            ):
                fired.append(True)
                raise marker
            return tracer

        caught = None
        prior_trace = sys.gettrace()
        try:
            with mock.patch.object(toolchain.os, "close", new=recording_close):
                try:
                    sys.settrace(tracer)
                    capability._close()
                except BaseException as error:
                    caught = error
                finally:
                    sys.settrace(prior_trace)
            self.assertEqual(fired, [True])
            self.assertIs(caught, marker)
            self.assertEqual(attempts, list(close_order))
            for fd in close_order:
                with self.assertRaises(OSError):
                    real_fstat(fd)
        finally:
            sys.settrace(prior_trace)
            for fd in close_order:
                try:
                    real_close(fd)
                except OSError:
                    pass
        caught = None
        marker = marker.with_traceback(None)

    def test_close_finishes_after_trace_control_at_finalization(self):
        real_close = os.close
        real_fstat = os.fstat
        capability, close_order = self._open_descriptor_capability()
        attempts = []
        marker = SystemExit("cleanup finalization")
        observed_closed = []

        def recording_close(fd):
            attempts.append(fd)
            real_close(fd)

        def tracer(frame, event, _argument):
            if (
                not observed_closed
                and event == "line"
                and frame.f_code.co_name == "_close"
                and attempts == list(close_order)
            ):
                observed_closed.append(capability._closed)
                raise marker
            return tracer

        caught = None
        prior_trace = sys.gettrace()
        try:
            with mock.patch.object(toolchain.os, "close", new=recording_close):
                try:
                    sys.settrace(tracer)
                    capability._close()
                except BaseException as error:
                    caught = error
                finally:
                    sys.settrace(prior_trace)
                capability._close()
            self.assertEqual(observed_closed, [False])
            self.assertIs(caught, marker)
            self.assertTrue(capability._closed)
            self.assertEqual(attempts, list(close_order))
            for fd in close_order:
                with self.assertRaises(OSError):
                    real_fstat(fd)
        finally:
            sys.settrace(prior_trace)
            for fd in close_order:
                try:
                    real_close(fd)
                except OSError:
                    pass
        caught = None
        marker = marker.with_traceback(None)

    def test_close_never_retries_a_number_reused_after_physical_close(self):
        real_open = os.open
        real_close = os.close
        real_fstat = os.fstat
        capability, close_order = self._open_descriptor_capability()
        attempts = []
        replacement = []
        marker = SystemExit("post-close control")

        def close_then_reuse(fd):
            attempts.append(fd)
            real_close(fd)
            if len(attempts) == 2:
                replacement.append(real_open(os.devnull, os.O_RDONLY))
                raise marker

        caught = None
        try:
            with mock.patch.object(toolchain.os, "close", new=close_then_reuse):
                try:
                    capability._close()
                except BaseException as error:
                    caught = error
                capability._close()
            self.assertIs(caught, marker)
            self.assertEqual(attempts, list(close_order))
            self.assertEqual(len(set(attempts)), len(attempts))
            self.assertEqual(len(replacement), 1)
            real_fstat(replacement[0])
        finally:
            for fd in close_order + tuple(replacement):
                try:
                    real_close(fd)
                except OSError:
                    pass
        caught = None
        marker = marker.with_traceback(None)

    def test_exit_preserves_body_exception_over_cleanup_failures(self):
        real_close = os.close
        real_fstat = os.fstat
        scenarios = (
            (
                KeyboardInterrupt("body keyboard control"),
                OSError("PRIVATE-CLEANUP-ORDINARY"),
            ),
            (
                SystemExit("body system control"),
                GeneratorExit("cleanup generator control"),
            ),
            (
                RuntimeError("body ordinary sentinel"),
                OSError("PRIVATE-CLEANUP-ORDINARY"),
            ),
            (
                RuntimeError("body ordinary sentinel"),
                SystemExit("cleanup system control"),
            ),
        )
        for body_error, cleanup_error in scenarios:
            with self.subTest(
                body=type(body_error).__name__,
                cleanup=type(cleanup_error).__name__,
            ):
                capability, close_order = self._open_descriptor_capability()
                attempts = []

                def controlled_close(fd):
                    attempts.append(fd)
                    real_close(fd)
                    if len(attempts) == 2:
                        raise cleanup_error

                caught = None
                try:
                    with mock.patch.object(toolchain.os, "close", new=controlled_close):
                        try:
                            with capability:
                                raise body_error
                        except BaseException as error:
                            caught = error
                        capability._close()
                    self.assertIs(caught, body_error)
                    self.assertIsNone(caught.__cause__)
                    self.assertIsNone(caught.__context__)
                    self.assertNotIn("PRIVATE-CLEANUP", repr(caught))
                    self.assertEqual(attempts, list(close_order))
                    for fd in close_order:
                        with self.assertRaises(OSError):
                            real_fstat(fd)
                finally:
                    for fd in close_order:
                        try:
                            real_close(fd)
                        except OSError:
                            pass
                caught = None
                body_error = body_error.with_traceback(None)
                cleanup_error = cleanup_error.with_traceback(None)

    def test_public_entrypoints_and_cli_are_closed(self):
        self.assertEqual(inspect.signature(toolchain.bootstrap_historical_foundry_toolchain).parameters, {})
        self.assertEqual(inspect.signature(toolchain.open_reviewed_historical_toolchain).parameters, {})
        allowed = (
            "--bootstrap-reviewed",
            "--print-verified-identity",
            "--verify-offline-tests",
            "--verify-connected-kat",
        )
        for mode in allowed:
            parsed = toolchain._parse_cli([mode])
            self.assertEqual(parsed, mode)
        for rejected in ([], [allowed[0], allowed[1]], [allowed[0], "value"], ["--solc", "/tmp/solc"], ["--help=x"], ["--help"], ["-h"]):
            with redirect_stderr(io.StringIO()), self.assertRaises(SystemExit) as raised:
                toolchain._parse_cli(rejected)
            self.assertEqual(raised.exception.code, 2)

    def test_executor_build_mode_has_no_caller_path_runtime_or_flags(self):
        self.assertEqual(
            tuple(inspect.signature(toolchain.ReviewedHistoricalToolchain._build_executor_artifact).parameters),
            ("self",),
        )
        self.assertEqual(
            tuple(inspect.signature(toolchain.ReviewedHistoricalToolchain._clean_project_outputs).parameters),
            ("self",),
        )
        _binary_dir, digests = self._install_fake_toolchain()
        self._write_executor_build_fixture()
        calls = []

        def invoke(_capability, _name, arguments, timeout=30):
            calls.append((arguments, timeout))
            return toolchain.subprocess.CompletedProcess(arguments, 0, b"", b"")

        with mock.patch.object(toolchain, "_PROJECT_ROOT", self.root), mock.patch.object(
            toolchain, "_EXPECTED_BINARY_SHA256", digests
        ):
            with toolchain.open_reviewed_historical_toolchain() as capability:
                with mock.patch.object(
                    toolchain.ReviewedHistoricalToolchain, "_invoke", new=invoke
                ), mock.patch.object(
                    toolchain.ReviewedHistoricalToolchain,
                    "_clean_project_outputs",
                    new=lambda _capability: None,
                ):
                    result = capability._build_executor_artifact()
        self.assertEqual(calls, [
            ((
                "build", "--offline", "--root", str(self.root),
                "--use", str(
                    self.root / ".historical-foundry" / "toolchains"
                    / toolchain._SOURCE_LOCK_SHA256 / "bin" / "solc"
                ),
                "--contracts", "foundry/src/TwoVenueV2Executor.sol",
                "--skip", "TwoVenueV2Fork.t.sol",
            ), 300),
        ])
        self.assertEqual(result["creation_bytecode"], bytes.fromhex("6000"))
        self.assertEqual(result["deployed_runtime"], bytes.fromhex("6001"))
        self.assertEqual(result["constructor_args"], b"")
        self.assertRegex(result["source_tree_sha256"], r"\A[0-9a-f]{64}\Z")
        self.assertRegex(result["artifact_manifest_sha256"], r"\A[0-9a-f]{64}\Z")

    def test_executor_artifact_reader_rejects_links_inventory_and_read_races(self):
        for attack in ("symlink", "hardlink", "extra", "mutation", "inode_swap"):
            with self.subTest(attack=attack):
                for stale in (self.root / ".historical-foundry", self.root / "out", self.root / "foundry"):
                    if stale.exists():
                        shutil.rmtree(stale)
                _binary_dir, digests = self._install_fake_toolchain()
                _source, artifact = self._write_executor_build_fixture()
                artifact_payload = artifact.read_bytes()
                artifact_inode = artifact.stat().st_ino
                if attack == "symlink":
                    specimen = self.root / "artifact-specimen.json"
                    specimen.write_bytes(artifact.read_bytes())
                    artifact.unlink()
                    os.symlink(specimen, artifact)
                elif attack == "hardlink":
                    specimen = self.root / "artifact-specimen.json"
                    specimen.write_bytes(artifact.read_bytes())
                    artifact.unlink()
                    os.link(specimen, artifact)
                elif attack == "extra":
                    (artifact.parent / "unexpected.json").write_text("{}", encoding="ascii")

                original_pread = toolchain.os.pread
                attacked = []

                def attack_during_read(fd, size, offset):
                    payload = original_pread(fd, size, offset)
                    if (
                        payload
                        and not attacked
                        and attack in ("mutation", "inode_swap")
                        and os.fstat(fd).st_ino == artifact_inode
                    ):
                        attacked.append(True)
                        if attack == "mutation":
                            with artifact.open("ab") as handle:
                                handle.write(b" ")
                        else:
                            replacement = artifact.parent / "replacement.json"
                            replacement.write_bytes(artifact_payload)
                            os.replace(replacement, artifact)
                    return payload

                def successful_invoke(_capability, _name, arguments, timeout=30):
                    return toolchain.subprocess.CompletedProcess(arguments, 0, b"", b"")

                with mock.patch.object(toolchain, "_PROJECT_ROOT", self.root), mock.patch.object(
                    toolchain, "_EXPECTED_BINARY_SHA256", digests
                ):
                    with toolchain.open_reviewed_historical_toolchain() as capability:
                        with mock.patch.object(
                            toolchain.ReviewedHistoricalToolchain,
                            "_invoke",
                            new=successful_invoke,
                        ), mock.patch.object(
                            toolchain.ReviewedHistoricalToolchain,
                            "_clean_project_outputs",
                            new=lambda _capability: None,
                        ), mock.patch.object(toolchain.os, "pread", side_effect=attack_during_read):
                            with self.assertRaises(toolchain.HistoricalFoundryToolchainError):
                                capability._build_executor_artifact()
                shutil.rmtree(self.root / ".historical-foundry")
                shutil.rmtree(self.root / "out")
                shutil.rmtree(self.root / "foundry")
                if (self.root / "artifact-specimen.json").exists():
                    (self.root / "artifact-specimen.json").unlink()

    def test_executor_artifact_normalizes_omitted_empty_immutable_references(self):
        payload = {
            "bytecode": {"object": "0x6000"},
            "deployedBytecode": {"object": "0x6001"},
        }
        artifact = json.dumps(payload, sort_keys=True, separators=(",", ":")).encode("ascii")
        creation, runtime, immutable_references = toolchain._parse_executor_artifact(artifact)
        self.assertEqual(creation, bytes.fromhex("6000"))
        self.assertEqual(runtime, bytes.fromhex("6001"))
        self.assertEqual(immutable_references, b"{}\n")

    def test_bootstrap_cli_maps_boundary_failure_without_traceback(self):
        stderr = io.StringIO()
        with mock.patch.object(
            toolchain,
            "bootstrap_historical_foundry_toolchain",
            side_effect=toolchain.HistoricalFoundryToolchainError("toolchain_download_failed"),
        ), redirect_stderr(stderr):
            self.assertEqual(toolchain._main(["--bootstrap-reviewed"]), 1)
        self.assertEqual(stderr.getvalue(), "toolchain_download_failed\n")

    def test_downloader_uses_the_verified_system_ca_bundle(self):
        response = mock.MagicMock()
        response.status = 200
        response.read.return_value = b"asset"
        response.__enter__.return_value = response
        context = object()
        with mock.patch(
            "scripts.bootstrap_historical_foundry_toolchain.ssl.create_default_context",
            return_value=context,
        ) as create_context, mock.patch(
            "scripts.bootstrap_historical_foundry_toolchain.urllib.request.urlopen",
            return_value=response,
        ) as urlopen:
            self.assertEqual(toolchain._download_bytes(toolchain._FOUNDRY_CHECKSUM_URL), b"asset")
        create_context.assert_called_once_with(cafile="/etc/ssl/cert.pem")
        self.assertIs(urlopen.call_args.kwargs["context"], context)

    def test_downloaded_assets_are_private_physical_files_before_use(self):
        _binaries, assets, hashes = self._mock_assets()
        download_dir = self.root / "private-download"
        download_dir.mkdir(mode=0o700)
        with mock.patch.object(
            toolchain, "_REVIEWED_ASSET_SHA256", hashes
        ), mock.patch.object(
            toolchain, "_download_bytes", side_effect=lambda url: assets[url]
        ):
            reread = toolchain._download_reviewed_assets(download_dir)
        self.assertEqual(reread, assets)
        self.assertEqual(stat.S_IMODE(download_dir.stat().st_mode), 0o700)
        expected_files = {
            "foundry.tar.gz",
            "foundry.sha256",
            "foundry.sigstore.json",
            "foundry.spdx.json",
            "solc",
        }
        self.assertEqual({path.name for path in download_dir.iterdir()}, expected_files)
        for path in download_dir.iterdir():
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)

    def test_reviewed_source_table_and_project_configuration_are_exact(self):
        self.assertEqual(toolchain._FOUNDRY_VERSION, "v1.7.1")
        self.assertEqual(toolchain._FOUNDRY_ARCHIVE_SHA256, "eacdc67718fac857cad9e19c7f6729dd80de731d09df81856391d093cfcab547")
        self.assertEqual(toolchain._FOUNDRY_CHECKSUM_SHA256, "91b21b7f96cfad4e40a0ef18077777c5732e244ed795d476e5bcd153e18e4b5c")
        self.assertEqual(toolchain._FOUNDRY_SIGSTORE_SHA256, "d5930109b48c43a968ce8c0b2068c7d43e973a2b2604eb590a48c4c74a52159e")
        self.assertEqual(toolchain._FOUNDRY_SPDX_SHA256, "2a20a6956e75c08ba5b6aa2acbf62d5236b998bf58be00b7561d68af5aa0de0b")
        self.assertEqual(toolchain._SOLC_SHA256, "d4abcf0b3e24b7948ddfd64c374d26c3214648717777790ecb936979054a129d")
        self.assertEqual(toolchain._FORGE_STD_COMMIT, FORGE_STD_COMMIT)
        self.assertEqual(
            toolchain._FORGE_STD_TREE_SHA256,
            "b20e3e90b1aab4acb1295e9d107c95a224441d272e6e479e9de153a9f3f64ab5",
        )
        self.assertEqual(dict(toolchain._EXPECTED_BINARY_SHA256), {
            "anvil": "5c9f9aad323062b1c0421a63595741430acaea150da3611e38c45071e4cf4e28",
            "cast": "eb9a9dc730a0f178556b90d39a30212375ee6e7c754fee96fa95b2723878e220",
            "forge": "e729589084ca2f1479354353d1ec3d4789451b577f4cdee4e7dc57cae64a38fa",
        })
        self.assertEqual(toolchain._COMPILER_SETTINGS, {
            "append_cbor": False,
            "bytecode_hash": "none",
            "cbor_metadata": False,
            "evm_version": "osaka",
            "fork_hardfork": "osaka",
            "optimizer_enabled": True,
            "optimizer_runs": 200,
            "via_ir": False,
        })
        self._write_project_inputs()
        with mock.patch.object(toolchain, "_PROJECT_ROOT", self.root):
            projection = toolchain._verify_project_inputs()
        self.assertEqual(projection["forge_std_commit"], FORGE_STD_COMMIT)
        self.assertEqual(
            projection["forge_std_tree_sha256"],
            toolchain._FORGE_STD_TREE_SHA256,
        )
        self.assertEqual(set(projection["physical_sha256"]), {"foundry.toml", "foundry.lock", ".gitmodules"})

    def test_reviewed_capability_exposes_only_fixed_project_input_identity(self):
        self.assertEqual(
            tuple(
                inspect.signature(
                    toolchain.ReviewedHistoricalToolchain
                    .verified_project_input_identity
                ).parameters
            ),
            ("self",),
        )
        _binary_dir, digests = self._install_fake_toolchain()
        with mock.patch.object(
            toolchain, "_PROJECT_ROOT", self.root
        ), mock.patch.object(
            toolchain, "_EXPECTED_BINARY_SHA256", digests
        ):
            with toolchain.open_reviewed_historical_toolchain() as capability:
                projection = capability.verified_project_input_identity()
                with self.assertRaises(TypeError):
                    capability.verified_project_input_identity("foundry.toml")
        self.assertEqual(
            projection,
            {
                "schema": "historical_foundry_project_input_identity/v1",
                "foundry_toml_sha256": _sha256(
                    toolchain._REVIEWED_PROJECT_FILES["foundry.toml"]
                ),
                "foundry_lock_sha256": _sha256(
                    toolchain._REVIEWED_PROJECT_FILES["foundry.lock"]
                ),
                "gitmodules_sha256": _sha256(
                    toolchain._REVIEWED_PROJECT_FILES[".gitmodules"]
                ),
                "forge_std_commit": FORGE_STD_COMMIT,
                "forge_std_tree_sha256": toolchain._FORGE_STD_TREE_SHA256,
            },
        )
        self.assertNotIn("url", json.dumps(projection, sort_keys=True))

    def test_final_toolchain_authority_projects_reviewed_task3_identities(self):
        from scripts.historical_foundry_contracts import load_historical_foundry_toolchain

        tracked = load_historical_foundry_toolchain().value
        with toolchain.open_reviewed_historical_toolchain() as capability:
            candidate = capability.verified_identity
        self.assertEqual(
            [dict(row) for row in tracked["binaries"]], candidate["binaries"]
        )
        self.assertEqual(tracked["solc"]["version"], candidate["solc"]["version"])
        self.assertEqual(tracked["solc"]["artifact_sha256"], candidate["solc"]["artifact_sha256"])
        self.assertEqual(dict(tracked["forge_std"]), candidate["forge_std"])
        self.assertEqual(dict(tracked["compiler_settings"]), candidate["compiler_settings"])

    def test_bootstrap_verifies_every_physical_hash_and_bounded_sigstore_projection(self):
        self._write_project_inputs()
        binaries, assets, hashes = self._mock_assets()
        foundry_hashes = {name: _sha256(body) for name, body in binaries.items()}
        with mock.patch.object(toolchain, "_PROJECT_ROOT", self.root), mock.patch.object(
            toolchain, "_REVIEWED_ASSET_SHA256", hashes
        ), mock.patch.object(
            toolchain, "_EXPECTED_BINARY_SHA256", foundry_hashes
        ), mock.patch.object(toolchain, "_download_bytes", side_effect=lambda url: assets[url]):
            candidate = toolchain.bootstrap_historical_foundry_toolchain()
        self.assertEqual(candidate["source_lock_sha256"], toolchain._SOURCE_LOCK_SHA256)
        self.assertEqual(
            {row["name"]: row["sha256"] for row in candidate["binaries"]},
            foundry_hashes,
        )
        self.assertEqual(candidate["solc"]["version"], "0.8.36+commit.8a079791")
        self.assertEqual(candidate["forge_std"]["commit"], FORGE_STD_COMMIT)
        self.assertFalse((self.root / "config" / "historical_foundry_replay_toolchain.json").exists())

    def test_bootstrap_rejects_each_changed_asset_before_install(self):
        self._write_project_inputs()
        _binaries, assets, hashes = self._mock_assets()
        for changed_url in tuple(assets):
            changed = dict(assets)
            changed[changed_url] += b"x"
            with self.subTest(url=changed_url), mock.patch.object(
                toolchain, "_PROJECT_ROOT", self.root
            ), mock.patch.object(toolchain, "_REVIEWED_ASSET_SHA256", hashes), mock.patch.object(
                toolchain, "_download_bytes", side_effect=lambda url, rows=changed: rows[url]
            ):
                with self.assertRaises(toolchain.HistoricalFoundryToolchainError):
                    toolchain.bootstrap_historical_foundry_toolchain()
            self.assertFalse((self.root / ".historical-foundry" / "toolchains").exists())

    def test_sigstore_message_digest_issuer_and_san_each_fail_closed(self):
        self._write_project_inputs()
        _binaries, assets, hashes = self._mock_assets()
        wrong_values = (
            ("digest", "0" * 64, toolchain._SIGSTORE_ISSUER, toolchain._SIGSTORE_SAN),
            ("issuer", _sha256(assets[toolchain._FOUNDRY_ARCHIVE_URL]), "https://issuer.invalid", toolchain._SIGSTORE_SAN),
            ("san", _sha256(assets[toolchain._FOUNDRY_ARCHIVE_URL]), toolchain._SIGSTORE_ISSUER, "https://identity.invalid"),
        )
        for label, digest, issuer, san in wrong_values:
            changed = dict(assets)
            changed[toolchain._FOUNDRY_SIGSTORE_URL] = _sigstore_bundle(digest, issuer, san)
            changed_hashes = dict(hashes)
            changed_hashes[toolchain._FOUNDRY_SIGSTORE_URL] = _sha256(changed[toolchain._FOUNDRY_SIGSTORE_URL])
            with self.subTest(field=label), mock.patch.object(toolchain, "_PROJECT_ROOT", self.root), mock.patch.object(
                toolchain, "_REVIEWED_ASSET_SHA256", changed_hashes
            ), mock.patch.object(toolchain, "_download_bytes", side_effect=lambda url, rows=changed: rows[url]):
                with self.assertRaises(toolchain.HistoricalFoundryToolchainError):
                    toolchain.bootstrap_historical_foundry_toolchain()

    def test_sigstore_allows_repeated_exact_certificate_projections(self):
        digest = "a" * 64
        payload = _sigstore_bundle(
            digest,
            toolchain._SIGSTORE_ISSUER + "\n" + toolchain._SIGSTORE_ISSUER,
            toolchain._SIGSTORE_SAN + "\n" + toolchain._SIGSTORE_SAN,
        )
        toolchain._verify_sigstore_projection(payload, digest)

    def test_dirty_forge_std_blocks_candidate_before_download(self):
        self._write_project_inputs()
        source = self.root / "lib" / "forge-std" / "src" / "Test.sol"
        source.write_bytes(source.read_bytes() + b"\n// dirty\n")
        with mock.patch.object(toolchain, "_PROJECT_ROOT", self.root), mock.patch.object(
            toolchain,
            "_download_bytes",
            side_effect=AssertionError("download must not start"),
        ):
            with self.assertRaisesRegex(
                toolchain.HistoricalFoundryToolchainError,
                "forge_std_tree_invalid",
            ):
                toolchain.bootstrap_historical_foundry_toolchain()

    def test_dirty_forge_std_blocks_open(self):
        self._write_project_inputs()
        _binary_dir, digests = self._install_fake_toolchain()
        source = self.root / "lib" / "forge-std" / "src" / "Test.sol"
        source.write_bytes(source.read_bytes() + b"\n// dirty\n")
        with mock.patch.object(toolchain, "_PROJECT_ROOT", self.root), mock.patch.object(
            toolchain, "_EXPECTED_BINARY_SHA256", digests
        ):
            with self.assertRaisesRegex(
                toolchain.HistoricalFoundryToolchainError,
                "forge_std_tree_invalid",
            ):
                toolchain.open_reviewed_historical_toolchain()

    def test_dirty_forge_std_after_open_blocks_offline_invocation(self):
        self._write_project_inputs()
        _binary_dir, digests = self._install_fake_toolchain()
        source = self.root / "lib" / "forge-std" / "src" / "Test.sol"
        with mock.patch.object(toolchain, "_PROJECT_ROOT", self.root), mock.patch.object(
            toolchain, "_EXPECTED_BINARY_SHA256", digests
        ):
            with toolchain.open_reviewed_historical_toolchain() as capability:
                source.write_bytes(source.read_bytes() + b"\n// dirty\n")
                with mock.patch.object(
                    toolchain.subprocess,
                    "run",
                    side_effect=AssertionError("forge must not start"),
                ), self.assertRaisesRegex(
                    toolchain.HistoricalFoundryToolchainError,
                    "forge_std_tree_invalid",
                ):
                    capability._verify_offline_tests()

    def test_open_uses_digest_scoped_nofollow_single_link_binaries(self):
        binary_dir, digests = self._install_fake_toolchain()
        with mock.patch.object(toolchain, "_PROJECT_ROOT", self.root), mock.patch.object(
            toolchain, "_EXPECTED_BINARY_SHA256", digests
        ):
            with toolchain.open_reviewed_historical_toolchain() as capability:
                self.assertNotIn(str(self.root), repr(capability))
                self.assertEqual(capability.verified_identity["binaries"][0]["name"], "forge")
            (binary_dir / "forge").unlink()
            os.symlink(binary_dir / "cast", binary_dir / "forge")
            with self.assertRaises(toolchain.HistoricalFoundryToolchainError):
                toolchain.open_reviewed_historical_toolchain()
            (binary_dir / "forge").unlink()
            os.link(binary_dir / "cast", binary_dir / "forge")
            with self.assertRaises(toolchain.HistoricalFoundryToolchainError):
                toolchain.open_reviewed_historical_toolchain()

    def test_ambient_path_is_ignored_and_preexisting_byte_change_is_rejected(self):
        binary_dir, digests = self._install_fake_toolchain()
        ambient = self.root / "ambient"
        ambient.mkdir()
        (ambient / "forge").write_bytes(_script("forge Version: 9.9.9-ambient"))
        (ambient / "forge").chmod(0o700)
        with mock.patch.object(toolchain, "_PROJECT_ROOT", self.root), mock.patch.object(
            toolchain, "_EXPECTED_BINARY_SHA256", digests
        ), mock.patch.dict(os.environ, {"PATH": str(ambient), "FOUNDRY_PROFILE": "attacker"}, clear=False):
            with toolchain.open_reviewed_historical_toolchain() as capability:
                self.assertEqual(capability._verified_version("forge"), "v1.7.1")
            (binary_dir / "cast").write_bytes(b"changed")
            with self.assertRaises(toolchain.HistoricalFoundryToolchainError):
                toolchain.open_reviewed_historical_toolchain()

    def test_invocation_rejects_same_inode_mutation_and_inode_replacement(self):
        for replacement in (False, True):
            with self.subTest(replacement=replacement):
                binary_dir, digests = self._install_fake_toolchain(delayed_forge=True)
                with mock.patch.object(toolchain, "_PROJECT_ROOT", self.root), mock.patch.object(
                    toolchain, "_EXPECTED_BINARY_SHA256", digests
                ):
                    with toolchain.open_reviewed_historical_toolchain() as capability:
                        path = binary_dir / "forge"
                        def mutate():
                            time.sleep(0.08)
                            if replacement:
                                other = binary_dir / "replacement"
                                other.write_bytes(_script("forge Version: 1.7.1\nCommit SHA: " + FOUNDRY_COMMIT))
                                other.chmod(0o700)
                                os.replace(other, path)
                            else:
                                with path.open("ab") as handle:
                                    handle.write(b"\n# changed")
                        worker = threading.Thread(target=mutate)
                        worker.start()
                        try:
                            with self.assertRaises(toolchain.HistoricalFoundryToolchainError):
                                capability._verified_version("forge")
                        finally:
                            worker.join()
                if replacement:
                    (binary_dir / "forge").unlink()
                else:
                    (binary_dir / "forge").write_bytes(_script("forge Version: 1.7.1\nCommit SHA: " + FOUNDRY_COMMIT, delay=True))
                    (binary_dir / "forge").chmod(0o700)
                import shutil
                shutil.rmtree(binary_dir.parent)

    def test_forge_invocation_holds_and_rechecks_the_pinned_compiler(self):
        binary_dir, digests = self._install_fake_toolchain(delayed_forge=True)
        with mock.patch.object(toolchain, "_PROJECT_ROOT", self.root), mock.patch.object(
            toolchain, "_EXPECTED_BINARY_SHA256", digests
        ):
            with toolchain.open_reviewed_historical_toolchain() as capability:
                def mutate_solc():
                    time.sleep(0.08)
                    with (binary_dir / "solc").open("ab") as handle:
                        handle.write(b"\n# changed")
                worker = threading.Thread(target=mutate_solc)
                worker.start()
                try:
                    with self.assertRaises(toolchain.HistoricalFoundryToolchainError):
                        capability._verified_version("forge")
                finally:
                    worker.join()

    def test_ancestor_replacement_during_invocation_rejects_evidence(self):
        _binary_dir, digests = self._install_fake_toolchain()
        historical = self.root / ".historical-foundry"
        displaced = self.root / "displaced-historical-foundry"

        def replace_ancestor(*args, **kwargs):
            os.rename(historical, displaced)
            self._install_fake_toolchain()
            return toolchain.subprocess.CompletedProcess(
                args[0],
                0,
                stdout=(
                    "forge Version: 1.7.1\nCommit SHA: "
                    + FOUNDRY_COMMIT
                    + "\n"
                ).encode("ascii"),
                stderr=b"",
            )

        with mock.patch.object(toolchain, "_PROJECT_ROOT", self.root), mock.patch.object(
            toolchain, "_EXPECTED_BINARY_SHA256", digests
        ):
            with toolchain.open_reviewed_historical_toolchain() as capability:
                with mock.patch.object(
                    toolchain.subprocess, "run", side_effect=replace_ancestor
                ), self.assertRaisesRegex(
                    toolchain.HistoricalFoundryToolchainError,
                    "toolchain_directory_changed",
                ):
                    capability._verified_version("forge")

    def test_version_and_hardfork_checks_have_no_fallback(self):
        self.assertEqual(
            toolchain._parse_foundry_version(
                "forge", "forge Version: 1.7.1\nCommit SHA: " + FOUNDRY_COMMIT + "\n"
            ),
            "v1.7.1",
        )
        for text in ("forge Version: 1.7.0", "forge 1.7.1 and 1.7.0"):
            with self.assertRaises(toolchain.HistoricalFoundryToolchainError):
                toolchain._parse_foundry_version("forge", text)
        with self.assertRaisesRegex(toolchain.HistoricalFoundryToolchainError, "fork_hardfork_unsupported"):
            toolchain._require_hardfork_support("cancun")
        toolchain._require_single_hardfork(
            toolchain._COMPILER_SETTINGS,
            toolchain._REVIEWED_FIXED_WINDOW_HARDFORK_PROJECTION,
        )
        mismatched_settings = dict(toolchain._COMPILER_SETTINGS)
        mismatched_settings["evm_version"] = "cancun"
        with self.assertRaisesRegex(toolchain.HistoricalFoundryToolchainError, "fork_window_mixed"):
            toolchain._require_single_hardfork(
                mismatched_settings,
                toolchain._REVIEWED_FIXED_WINDOW_HARDFORK_PROJECTION,
            )
        mixed_projection = dict(
            toolchain._REVIEWED_FIXED_WINDOW_HARDFORK_PROJECTION
        )
        mixed_projection["lower_bound_hardfork"] = "prague"
        with self.assertRaisesRegex(toolchain.HistoricalFoundryToolchainError, "fork_window_mixed"):
            toolchain._require_single_hardfork(
                toolchain._COMPILER_SETTINGS,
                mixed_projection,
            )

    def test_offline_gate_rejects_zero_test_success(self):
        _binary_dir, digests = self._install_fake_toolchain()
        with mock.patch.object(toolchain, "_PROJECT_ROOT", self.root), mock.patch.object(
            toolchain, "_EXPECTED_BINARY_SHA256", digests
        ):
            with toolchain.open_reviewed_historical_toolchain() as capability:
                with self.assertRaisesRegex(
                    toolchain.HistoricalFoundryToolchainError,
                    "foundry_offline_tests_failed",
                ):
                    capability._verify_offline_tests()

    def test_connected_gate_rejects_zero_test_success(self):
        _binary_dir, digests = self._install_fake_toolchain()
        fork_source = self.root / "foundry" / "test" / "TwoVenueV2Fork.t.sol"
        fork_source.parent.mkdir(parents=True)
        fork_source.write_bytes(
            (REAL_PROJECT_ROOT / toolchain._KAT_FORK_SOURCE).read_bytes()
        )
        kat_fixture = (
            self.root / "tests" / "fixtures" / "historical_foundry_kat.json"
        )
        kat_fixture.parent.mkdir(parents=True)
        kat_fixture.write_bytes(KAT_FIXTURE_BYTES)
        preflight = [
            json.dumps({
                "baseFeePerGas": "0x478d0e7f",
                "gasLimit": "0x3938700",
                "gasUsed": "0x2035c7b",
                "hash": "0xf398976165ca4756c77fc6b61111fa1102d431eb03082417ecce38b36308d728",
                "number": "0x17d7840",
                "parentHash": "0xc5a79102dcb47469ef357021c974bbbb92df3a1f3cfbcb5fdc0f9b36fb75e2c7",
                "stateRoot": "0x055eba2b2b3daa967118fe831b0988cb27434e274f97f66cc67dcaa16dbe417f",
                "timestamp": "0x69f497f3",
            }).encode("ascii"),
            KAT_UNISWAP_RESPONSE.encode("ascii"),
            KAT_SUSHISWAP_RESPONSE.encode("ascii"),
            KAT_CHAINLINK_RESPONSE.encode("ascii"),
        ]
        calls = []

        def zero_test_invoke(_capability, name, arguments, timeout=30):
            calls.append(name)
            if name == "cast":
                return toolchain.subprocess.CompletedProcess(
                    arguments, 0, preflight[len(calls) - 1], b""
                )
            return toolchain.subprocess.CompletedProcess(arguments, 0, b"", b"")

        with mock.patch.object(toolchain, "_PROJECT_ROOT", self.root), mock.patch.object(
            toolchain, "_EXPECTED_BINARY_SHA256", digests
        ), mock.patch.dict(
            os.environ, {"DEX_DEPTH_RPC_ETH": "https://example.invalid"}, clear=True
        ):
            with toolchain.open_reviewed_historical_toolchain() as capability:
                with mock.patch.object(
                    toolchain.ReviewedHistoricalToolchain,
                    "_invoke",
                    new=zero_test_invoke,
                ), mock.patch.object(
                    toolchain.ReviewedHistoricalToolchain,
                    "_verify_versions_and_hardfork",
                    return_value=None,
                ), self.assertRaisesRegex(
                    toolchain.HistoricalFoundryToolchainError,
                    "foundry_replay_failed",
                ):
                    capability._verify_connected_kat()

    def test_dashboard_import_performs_no_download_or_install(self):
        target = self.root / ".historical-foundry"
        with mock.patch("urllib.request.urlopen", side_effect=AssertionError("download attempted")):
            import dashboard.server
            importlib.reload(dashboard.server)
        self.assertFalse(target.exists())


class HistoricalFoundryKatFixtureTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)

    def tearDown(self):
        self.temporary.cleanup()

    def _write_project_inputs(self):
        HistoricalFoundryToolchainTests._write_project_inputs(self)

    def _install_fake_toolchain(self):
        _binary_dir, digests = HistoricalFoundryToolchainTests._install_fake_toolchain(self)
        return digests

    def _write_fixture(self, root=None):
        root = self.root if root is None else root
        fixture = root / "tests" / "fixtures" / "historical_foundry_kat.json"
        fixture.parent.mkdir(parents=True)
        fixture.write_bytes(KAT_FIXTURE_BYTES)
        fixture.chmod(0o600)
        return fixture

    def _write_fork_source(self):
        source = self.root / "foundry" / "test" / "TwoVenueV2Fork.t.sol"
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_bytes(
            (REAL_PROJECT_ROOT / toolchain._KAT_FORK_SOURCE).read_bytes()
        )
        return source

    def _value_copy(self):
        return json.loads(KAT_FIXTURE_BYTES.decode("ascii"))

    def _bytes(self, value):
        return (
            json.dumps(value, sort_keys=True, separators=(",", ":")) + "\n"
        ).encode("ascii")

    def _live_header(self):
        return {
            "baseFeePerGas": "0x478d0e7f",
            "gasLimit": "0x3938700",
            "gasUsed": "0x2035c7b",
            "hash": "0xf398976165ca4756c77fc6b61111fa1102d431eb03082417ecce38b36308d728",
            "number": "0x17d7840",
            "parentHash": "0xc5a79102dcb47469ef357021c974bbbb92df3a1f3cfbcb5fdc0f9b36fb75e2c7",
            "stateRoot": "0x055eba2b2b3daa967118fe831b0988cb27434e274f97f66cc67dcaa16dbe417f",
            "timestamp": "0x69f497f3",
        }

    def _preflight_outputs(self):
        return [
            json.dumps(
                self._live_header(), sort_keys=True, separators=(",", ":")
            ).encode("ascii")
            + b"\n",
            KAT_UNISWAP_RESPONSE.encode("ascii") + b"\n",
            KAT_SUSHISWAP_RESPONSE.encode("ascii") + b"\n",
            KAT_CHAINLINK_RESPONSE.encode("ascii") + b"\n",
        ]

    def _expected_vectors(self, endpoint):
        sealed_solc = str(
            self.root
            / ".historical-foundry"
            / "toolchains"
            / toolchain._SOURCE_LOCK_SHA256
            / "bin"
            / "solc"
        )
        vectors = [
            (
                "cast",
                (
                    "rpc",
                    "--rpc-url",
                    endpoint,
                    "eth_getBlockByNumber",
                    "0x17d7840",
                    "false",
                ),
            ),
        ]
        for row in KAT_FIXTURE_VALUE["archive_calls"]:
            call = json.dumps(
                {"data": row["calldata"], "to": row["target"]},
                sort_keys=True,
                separators=(",", ":"),
            )
            vectors.append(("cast", (
                "rpc", "--rpc-url", endpoint, "eth_call", call,
                row["block_reference"],
            )))
        vectors.append(
            (
                "forge",
                (
                    "test",
                    "--root",
                    str(self.root),
                    "--use",
                    sealed_solc,
                    "--match-path",
                    "foundry/test/TwoVenueV2Fork.t.sol",
                    "--fork-url",
                    endpoint,
                    "--fork-block-number",
                    "25000000",
                    "-vvv",
                ),
            ),
        )
        return vectors

    def _assert_sanitized_error(self, error, *sentinels):
        rendered = "".join(
            traceback.format_exception(type(error), error, error.__traceback__)
        )
        retained = repr(getattr(error, "__dict__", {}))
        self.assertIsNone(error.__cause__)
        self.assertIsNone(error.__context__)
        for sentinel in sentinels:
            self.assertNotIn(sentinel, rendered)
            self.assertNotIn(sentinel, retained)

    def test_checked_in_fixture_and_private_apis_match_the_reviewed_table(self):
        self.assertEqual(
            tuple(
                inspect.signature(
                    toolchain._validate_reviewed_historical_foundry_kat_bytes
                ).parameters
            ),
            ("payload",),
        )
        self.assertEqual(
            inspect.signature(
                toolchain._load_reviewed_historical_foundry_kat
            ).parameters,
            {},
        )
        fixture = (
            REAL_PROJECT_ROOT
            / "tests"
            / "fixtures"
            / "historical_foundry_kat.json"
        )
        payload = fixture.read_bytes()
        self.assertEqual(payload, KAT_FIXTURE_BYTES)
        value = toolchain._validate_reviewed_historical_foundry_kat_bytes(payload)
        self.assertEqual(value, KAT_FIXTURE_VALUE)
        decoded = []
        for row in value["archive_calls"]:
            raw = bytes.fromhex(row["raw_response"][2:])
            decoded.append(
                [
                    int.from_bytes(raw[offset : offset + 32], "big")
                    for offset in range(0, len(raw), 32)
                ]
            )
        self.assertEqual(
            decoded,
            [
                [386708852858506679503887, 542990426090335589494, 1777635347],
                [3494949632159963323927, 4918051786500934660, 1777630223],
                [
                    129127208515966890014,
                    228577572402,
                    1777636927,
                    1777636943,
                    129127208515966890014,
                ],
            ],
        )

    def test_cli_accepts_only_one_exact_mode_without_echoing_rejected_values(self):
        modes = (
            "--bootstrap-reviewed",
            "--print-verified-identity",
            "--verify-offline-tests",
            "--verify-connected-kat",
        )
        for mode in modes:
            with self.subTest(valid=mode):
                self.assertEqual(toolchain._parse_cli((mode,)), mode)

        secret = "CLI-SECRET-SENTINEL"
        rejected = (
            (),
            ("--bootstrap-r",),
            ("--verify-connected-k",),
            ("--verify-connected-kat", "--verify-connected-kat"),
            ("--verify-connected-kat", secret),
            ("--verify-connected-kat=" + secret,),
        )
        for arguments in rejected:
            with self.subTest(rejected=arguments):
                stderr = io.StringIO()
                with redirect_stderr(stderr), self.assertRaises(SystemExit) as raised:
                    toolchain._parse_cli(arguments)
                self.assertEqual(raised.exception.code, 2)
                self.assertEqual(stderr.getvalue(), "invalid_cli_arguments\n")
                self.assertNotIn(secret, stderr.getvalue())

    def test_fixture_rejects_noncanonical_schema_type_and_identity_mutations(self):
        invalid_payloads = [
            KAT_FIXTURE_BYTES[:-1],
            KAT_FIXTURE_BYTES + b"\n",
            json.dumps(KAT_FIXTURE_VALUE, indent=2).encode("ascii") + b"\n",
            KAT_FIXTURE_BYTES.replace(
                b'{"archive_calls":', b'{"archive_calls":[],"archive_calls":', 1
            ),
        ]
        for payload in invalid_payloads:
            with self.subTest(payload_sha256=_sha256(payload)):
                with self.assertRaises(toolchain.HistoricalFoundryToolchainError):
                    toolchain._validate_reviewed_historical_foundry_kat_bytes(payload)

        mutations = []
        value = self._value_copy()
        value["unknown"] = "no"
        mutations.append(value)
        value = self._value_copy()
        del value["schema"]
        mutations.append(value)
        value = self._value_copy()
        value["chain_id"] = True
        mutations.append(value)
        value = self._value_copy()
        value["block_header"]["number_decimal"] = True
        mutations.append(value)
        value = self._value_copy()
        value["block_header"]["hash"] = "0x" + "00" * 32
        mutations.append(value)
        value = self._value_copy()
        value["block_header"]["state_override"] = {}
        mutations.append(value)
        value = self._value_copy()
        value["pair_identities"][0]["pair_address"] = "0x" + "00" * 20
        mutations.append(value)
        value = self._value_copy()
        value["pair_identities"].reverse()
        mutations.append(value)
        value = self._value_copy()
        value["archive_calls"].reverse()
        mutations.append(value)
        value = self._value_copy()
        value["archive_calls"][0]["block_reference"] = "latest"
        mutations.append(value)
        for forbidden in ("endpoint", "provider", "runtime_override", "block_override"):
            value = self._value_copy()
            value[forbidden] = "forbidden"
            mutations.append(value)
        for value in mutations:
            with self.subTest(value_sha256=_sha256(self._bytes(value))):
                with self.assertRaises(toolchain.HistoricalFoundryToolchainError):
                    toolchain._validate_reviewed_historical_foundry_kat_bytes(
                        self._bytes(value)
                    )

    def test_fixture_rejects_response_digest_and_abi_length_mutations(self):
        mutations = []
        value = self._value_copy()
        value["archive_calls"][0]["raw_response"] = (
            "0x1" + value["archive_calls"][0]["raw_response"][3:]
        )
        mutations.append(value)
        value = self._value_copy()
        value["archive_calls"][1]["response_sha256"] = "0" * 64
        mutations.append(value)
        for index in range(3):
            value = self._value_copy()
            raw = value["archive_calls"][index]["raw_response"][:-2]
            value["archive_calls"][index]["raw_response"] = raw
            value["archive_calls"][index]["response_sha256"] = _sha256(
                raw.encode("ascii")
            )
            mutations.append(value)
        for value in mutations:
            with self.subTest(value_sha256=_sha256(self._bytes(value))):
                with self.assertRaises(toolchain.HistoricalFoundryToolchainError):
                    toolchain._validate_reviewed_historical_foundry_kat_bytes(
                        self._bytes(value)
                    )

    def test_fixture_loader_rejects_symlink_and_hardlink_members(self):
        for attack in ("symlink", "hardlink"):
            with self.subTest(attack=attack):
                root = self.root / attack
                fixture = self._write_fixture(root)
                specimen = root / "specimen.json"
                specimen.write_bytes(KAT_FIXTURE_BYTES)
                fixture.unlink()
                if attack == "symlink":
                    os.symlink(specimen, fixture)
                else:
                    os.link(specimen, fixture)
                with mock.patch.object(toolchain, "_PROJECT_ROOT", root):
                    with self.assertRaises(
                        toolchain.HistoricalFoundryToolchainError
                    ):
                        toolchain._load_reviewed_historical_foundry_kat()

    def test_fixture_loader_rejects_reread_inode_and_ancestor_attacks(self):
        for attack in ("reread", "inode", "ancestor"):
            with self.subTest(attack=attack):
                root = self.root / attack
                fixture = self._write_fixture(root)
                fixture_inode = fixture.stat().st_ino
                original_pread = toolchain.os.pread
                attacked = []

                def attack_after_first_read(fd, size, offset):
                    payload = original_pread(fd, size, offset)
                    if (
                        not payload
                        and offset == len(KAT_FIXTURE_BYTES)
                        and not attacked
                        and os.fstat(fd).st_ino == fixture_inode
                    ):
                        attacked.append(True)
                        if attack == "reread":
                            changed = bytearray(KAT_FIXTURE_BYTES)
                            changed[-2] = ord(" ")
                            fixture.write_bytes(bytes(changed))
                        elif attack == "inode":
                            replacement = fixture.parent / "replacement.json"
                            replacement.write_bytes(KAT_FIXTURE_BYTES)
                            os.replace(replacement, fixture)
                        else:
                            displaced = root / "displaced-tests"
                            os.rename(root / "tests", displaced)
                            replacement = self._write_fixture(root)
                            replacement.chmod(0o600)
                    return payload

                with mock.patch.object(
                    toolchain, "_PROJECT_ROOT", root
                ), mock.patch.object(
                    toolchain.os, "pread", side_effect=attack_after_first_read
                ):
                    with self.assertRaises(
                        toolchain.HistoricalFoundryToolchainError
                    ):
                        toolchain._load_reviewed_historical_foundry_kat()
                self.assertTrue(attacked)

    def test_missing_rpc_precedes_missing_fork_source_and_invokes_nothing(self):
        digests = self._install_fake_toolchain()
        self._write_fixture()
        with mock.patch.object(
            toolchain, "_PROJECT_ROOT", self.root
        ), mock.patch.object(
            toolchain, "_EXPECTED_BINARY_SHA256", digests
        ), mock.patch.dict(
            os.environ, {}, clear=True
        ):
            stderr = io.StringIO()
            with mock.patch.object(
                toolchain.ReviewedHistoricalToolchain,
                "_invoke",
                side_effect=AssertionError("subprocess must not start"),
            ) as invoke, redirect_stderr(stderr):
                self.assertEqual(
                    toolchain._main(["--verify-connected-kat"]), 1
                )
        self.assertEqual(stderr.getvalue(), "archive_state_unavailable\n")
        invoke.assert_not_called()

    def test_cli_header_mismatch_precedes_every_forge_process(self):
        digests = self._install_fake_toolchain()
        self._write_fixture()
        self._write_fork_source()
        endpoint = "https://rpc.invalid/header-order"
        header = self._live_header()
        header["stateRoot"] = "0x" + "00" * 32
        calls = []

        def invoke(_capability, name, arguments, timeout=30):
            calls.append((name, arguments))
            if arguments == ("--version",):
                if name == "solc":
                    output = "Version: 0.8.36+commit.8a079791.Darwin.appleclang"
                else:
                    output = "{} Version: 1.7.1\nCommit SHA: {}".format(
                        name, FOUNDRY_COMMIT
                    )
                return toolchain.subprocess.CompletedProcess(
                    arguments, 0, output.encode("ascii"), b""
                )
            if name == "anvil" and arguments == ("--help",):
                return toolchain.subprocess.CompletedProcess(
                    arguments, 0, b"osaka\n", b""
                )
            return toolchain.subprocess.CompletedProcess(
                arguments, 0, json.dumps(header).encode("ascii"), b""
            )

        stderr = io.StringIO()
        with mock.patch.object(
            toolchain, "_PROJECT_ROOT", self.root
        ), mock.patch.object(
            toolchain, "_EXPECTED_BINARY_SHA256", digests
        ), mock.patch.dict(
            os.environ, {"DEX_DEPTH_RPC_ETH": endpoint}, clear=True
        ), mock.patch.object(
            toolchain.ReviewedHistoricalToolchain, "_invoke", new=invoke
        ), redirect_stderr(stderr):
            self.assertEqual(toolchain._main(["--verify-connected-kat"]), 1)
        self.assertEqual(stderr.getvalue(), "authority_mismatch\n")
        self.assertNotIn("forge", [name for name, _arguments in calls])

    def test_mocked_preflight_and_forge_vectors_are_exact_and_ordered(self):
        digests = self._install_fake_toolchain()
        self._write_fixture()
        self._write_fork_source()
        (self.root / "out" / "generated").mkdir(parents=True)
        (self.root / "cache").mkdir()
        endpoint = "https://rpc.invalid/fixed-sentinel"
        outputs = self._preflight_outputs()
        calls = []

        def invoke(_capability, name, arguments, timeout=30):
            calls.append((name, arguments))
            if name == "cast":
                return toolchain.subprocess.CompletedProcess(
                    arguments, 0, outputs[len(calls) - 1], b""
                )
            return toolchain.subprocess.CompletedProcess(
                arguments,
                0,
                b"Suite result: ok. 10 passed; 0 failed; 0 skipped; finished in 1ms\n",
                b"",
            )

        with mock.patch.object(
            toolchain, "_PROJECT_ROOT", self.root
        ), mock.patch.object(
            toolchain, "_EXPECTED_BINARY_SHA256", digests
        ), mock.patch.dict(
            os.environ, {"DEX_DEPTH_RPC_ETH": endpoint}, clear=True
        ):
            with toolchain.open_reviewed_historical_toolchain() as capability:
                with mock.patch.object(
                    toolchain.ReviewedHistoricalToolchain, "_invoke", new=invoke
                ), mock.patch.object(
                    toolchain.ReviewedHistoricalToolchain,
                    "_verify_versions_and_hardfork",
                    return_value=None,
                ):
                    capability._verify_connected_kat()
        self.assertEqual(calls, self._expected_vectors(endpoint))
        self.assertFalse((self.root / "out").exists())
        self.assertFalse((self.root / "cache").exists())

    def test_checked_in_fork_source_is_accepted_by_the_connected_gate(self):
        source = REAL_PROJECT_ROOT / "foundry" / "test" / "TwoVenueV2Fork.t.sol"
        self.assertTrue(source.is_file(), str(source))
        endpoint = "https://rpc.invalid/checked-in-source"
        outputs = self._preflight_outputs()
        calls = []

        def invoke(_capability, name, arguments, timeout=30):
            calls.append((name, arguments))
            if name == "cast":
                return toolchain.subprocess.CompletedProcess(
                    arguments, 0, outputs[len(calls) - 1], b""
                )
            return toolchain.subprocess.CompletedProcess(
                arguments,
                0,
                b"Suite result: ok. 10 passed; 0 failed; 0 skipped;\n",
                b"",
            )

        with mock.patch.dict(
            os.environ, {"DEX_DEPTH_RPC_ETH": endpoint}, clear=True
        ), mock.patch.object(
            toolchain.ReviewedHistoricalToolchain, "_invoke", new=invoke
        ), mock.patch.object(
            toolchain.ReviewedHistoricalToolchain,
            "_verify_versions_and_hardfork",
            return_value=None,
        ):
            with toolchain.open_reviewed_historical_toolchain() as capability:
                capability._verify_connected_kat()
        expected = self._expected_vectors(endpoint)
        temporary_prefix = str(self.root)
        project_prefix = str(REAL_PROJECT_ROOT)
        expected[-1] = (
            expected[-1][0],
            tuple(
                value.replace(temporary_prefix, project_prefix)
                for value in expected[-1][1]
            ),
        )
        self.assertEqual(calls, expected)

    def test_unreviewed_fork_source_is_rejected_before_any_process(self):
        digests = self._install_fake_toolchain()
        self._write_fixture()
        source = self.root / "foundry" / "test" / "TwoVenueV2Fork.t.sol"
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_text(
            "contract UnreviewedTenPassingTests {}\n", encoding="ascii"
        )

        with mock.patch.object(
            toolchain, "_PROJECT_ROOT", self.root
        ), mock.patch.object(
            toolchain, "_EXPECTED_BINARY_SHA256", digests
        ), mock.patch.dict(
            os.environ,
            {"DEX_DEPTH_RPC_ETH": "https://rpc.invalid/unreviewed-source"},
            clear=True,
        ):
            with toolchain.open_reviewed_historical_toolchain() as capability:
                with mock.patch.object(
                    toolchain.ReviewedHistoricalToolchain,
                    "_invoke",
                    side_effect=AssertionError("process must not start"),
                ) as invoke, self.assertRaisesRegex(
                    toolchain.HistoricalFoundryToolchainError,
                    r"\Aconnected_kat_fixture_invalid\Z",
                ):
                    capability._verify_connected_kat()
        invoke.assert_not_called()

    def test_fork_source_inode_swap_during_first_cast_is_rejected(self):
        digests = self._install_fake_toolchain()
        self._write_fixture()
        source = self._write_fork_source()
        replacement = source.with_name("replacement.t.sol")
        replacement.write_bytes(source.read_bytes())
        endpoint = "https://rpc.invalid/source-stability"
        outputs = self._preflight_outputs()
        calls = []
        cast_count = 0

        def invoke(_capability, name, arguments, timeout=30):
            nonlocal cast_count
            calls.append(name)
            if name == "cast":
                if cast_count == 0:
                    os.replace(replacement, source)
                output = outputs[cast_count]
                cast_count += 1
                return toolchain.subprocess.CompletedProcess(
                    arguments, 0, output, b""
                )
            return toolchain.subprocess.CompletedProcess(
                arguments,
                0,
                b"Suite result: ok. 10 passed; 0 failed; 0 skipped;\n",
                b"",
            )

        with mock.patch.object(
            toolchain, "_PROJECT_ROOT", self.root
        ), mock.patch.object(
            toolchain, "_EXPECTED_BINARY_SHA256", digests
        ), mock.patch.dict(
            os.environ, {"DEX_DEPTH_RPC_ETH": endpoint}, clear=True
        ):
            with toolchain.open_reviewed_historical_toolchain() as capability:
                with mock.patch.object(
                    toolchain.ReviewedHistoricalToolchain,
                    "_invoke",
                    new=invoke,
                ), mock.patch.object(
                    toolchain.ReviewedHistoricalToolchain,
                    "_verify_versions_and_hardfork",
                    return_value=None,
                ), self.assertRaises(
                    toolchain.HistoricalFoundryToolchainError
                ):
                    capability._verify_connected_kat()
        self.assertNotIn("forge", calls)

    def test_any_preflight_mismatch_or_error_prevents_forge(self):
        digests = self._install_fake_toolchain()
        self._write_fixture()
        self._write_fork_source()
        endpoint = "https://rpc.invalid/preflight"
        for attack in ("header", "raw", "rpc_error"):
            with self.subTest(attack=attack):
                outputs = self._preflight_outputs()
                if attack == "header":
                    header = self._live_header()
                    header["stateRoot"] = "0x" + "00" * 32
                    outputs[0] = json.dumps(header).encode("ascii") + b"\n"
                elif attack == "raw":
                    outputs[2] = ("0x" + "00" * 96).encode("ascii") + b"\n"
                calls = []

                def invoke(_capability, name, arguments, timeout=30):
                    calls.append((name, arguments))
                    index = len(calls) - 1
                    if attack == "rpc_error" and index == 0:
                        return toolchain.subprocess.CompletedProcess(
                            arguments, 1, b"", b"private-rpc-error-sentinel"
                        )
                    return toolchain.subprocess.CompletedProcess(
                        arguments, 0, outputs[index], b""
                    )

                with mock.patch.object(
                    toolchain, "_PROJECT_ROOT", self.root
                ), mock.patch.object(
                    toolchain, "_EXPECTED_BINARY_SHA256", digests
                ), mock.patch.dict(
                    os.environ, {"DEX_DEPTH_RPC_ETH": endpoint}, clear=True
                ):
                    with toolchain.open_reviewed_historical_toolchain() as capability:
                        with mock.patch.object(
                            toolchain.ReviewedHistoricalToolchain,
                            "_invoke",
                            new=invoke,
                        ), self.assertRaises(
                            toolchain.HistoricalFoundryToolchainError
                        ):
                            capability._verify_connected_kat()
                self.assertNotIn("forge", [name for name, _arguments in calls])

    def test_connected_gate_accepts_only_exact_ten_scenario_summary(self):
        digests = self._install_fake_toolchain()
        self._write_fixture()
        self._write_fork_source()
        endpoint = "https://rpc.invalid/counts"
        summaries = (
            ("9", b"Suite result: ok. 9 passed; 0 failed; 0 skipped;\n"),
            ("11", b"Suite result: ok. 11 passed; 0 failed; 0 skipped;\n"),
            ("failed", b"Suite result: FAILED. 10 passed; 1 failed; 0 skipped;\n"),
            ("skipped", b"Suite result: ok. 10 passed; 0 failed; 1 skipped;\n"),
            ("generic", b"1 test passed\n"),
        )
        for label, summary in summaries:
            with self.subTest(summary=label):
                outputs = self._preflight_outputs()
                calls = []

                def invoke(_capability, name, arguments, timeout=30):
                    calls.append(name)
                    if name == "cast":
                        return toolchain.subprocess.CompletedProcess(
                            arguments, 0, outputs[len(calls) - 1], b""
                        )
                    return toolchain.subprocess.CompletedProcess(
                        arguments, 0, summary, b""
                    )

                with mock.patch.object(
                    toolchain, "_PROJECT_ROOT", self.root
                ), mock.patch.object(
                    toolchain, "_EXPECTED_BINARY_SHA256", digests
                ), mock.patch.dict(
                    os.environ, {"DEX_DEPTH_RPC_ETH": endpoint}, clear=True
                ):
                    with toolchain.open_reviewed_historical_toolchain() as capability:
                        with mock.patch.object(
                            toolchain.ReviewedHistoricalToolchain,
                            "_invoke",
                            new=invoke,
                        ), mock.patch.object(
                            toolchain.ReviewedHistoricalToolchain,
                            "_verify_versions_and_hardfork",
                            return_value=None,
                        ), self.assertRaisesRegex(
                            toolchain.HistoricalFoundryToolchainError,
                            r"\Afoundry_replay_failed\Z",
                        ):
                            capability._verify_connected_kat()

    def test_endpoint_and_rpc_error_sentinels_are_never_emitted_or_retained(self):
        digests = self._install_fake_toolchain()
        fixture = self._write_fixture()
        self._write_fork_source()
        endpoint = "https://user:SECRET-ENDPOINT-SENTINEL@rpc.invalid/key"
        rpc_error = b"SECRET-RPC-ERROR-SENTINEL"

        def invoke(_capability, _name, arguments, timeout=30):
            return toolchain.subprocess.CompletedProcess(
                arguments, 1, b"", rpc_error
            )

        stdout = io.StringIO()
        stderr = io.StringIO()
        with mock.patch.object(
            toolchain, "_PROJECT_ROOT", self.root
        ), mock.patch.object(
            toolchain, "_EXPECTED_BINARY_SHA256", digests
        ), mock.patch.dict(
            os.environ, {"DEX_DEPTH_RPC_ETH": endpoint}, clear=True
        ), mock.patch.object(
            toolchain.ReviewedHistoricalToolchain, "_invoke", new=invoke
        ), redirect_stderr(stderr), mock.patch("sys.stdout", stdout):
            with toolchain.open_reviewed_historical_toolchain() as capability:
                with self.assertRaises(
                    toolchain.HistoricalFoundryToolchainError
                ) as raised:
                    capability._verify_connected_kat()
        combined = (
            str(raised.exception)
            + stdout.getvalue()
            + stderr.getvalue()
            + fixture.read_text(encoding="ascii")
        )
        self.assertNotIn("SECRET-ENDPOINT-SENTINEL", combined)
        self.assertNotIn("SECRET-RPC-ERROR-SENTINEL", combined)

    def test_process_parse_and_forge_errors_discard_sensitive_exception_state(self):
        digests = self._install_fake_toolchain()
        self._write_fixture()
        self._write_fork_source()
        endpoint = "https://credential-sentinel.invalid/private"
        rpc_body = "RPC-BODY-SENTINEL"
        local_path = str(self.root / "PRIVATE-LOCAL-PATH")
        dirty_argv = ("cast", "--rpc-url", endpoint, local_path)
        with mock.patch.object(
            toolchain, "_PROJECT_ROOT", self.root
        ), mock.patch.object(
            toolchain, "_EXPECTED_BINARY_SHA256", digests
        ), mock.patch.dict(
            os.environ, {"DEX_DEPTH_RPC_ETH": endpoint}, clear=True
        ):
            with toolchain.open_reviewed_historical_toolchain() as capability:
                timeout = toolchain.subprocess.TimeoutExpired(
                    dirty_argv,
                    1,
                    output=rpc_body.encode("ascii"),
                    stderr=local_path.encode("utf-8"),
                )
                with mock.patch.object(
                    toolchain.subprocess, "run", side_effect=timeout
                ), self.assertRaises(
                    toolchain.HistoricalFoundryToolchainError
                ) as raised:
                    capability._invoke("cast", ("--version",))
                self._assert_sanitized_error(
                    raised.exception, endpoint, rpc_body, local_path
                )

                invalid_json = (rpc_body + "{").encode("ascii")
                with mock.patch.object(
                    toolchain.ReviewedHistoricalToolchain,
                    "_invoke",
                    return_value=toolchain.subprocess.CompletedProcess(
                        (), 0, invalid_json, b""
                    ),
                ), self.assertRaises(
                    toolchain.HistoricalFoundryToolchainError
                ) as raised:
                    capability._verify_connected_kat()
                self._assert_sanitized_error(
                    raised.exception, endpoint, rpc_body, local_path
                )

                malformed_call_outputs = [
                    self._preflight_outputs()[0],
                    ('"' + rpc_body + local_path).encode("utf-8"),
                ]
                malformed_call_count = 0

                def malformed_call(_capability, _name, arguments, timeout=30):
                    nonlocal malformed_call_count
                    output = malformed_call_outputs[malformed_call_count]
                    malformed_call_count += 1
                    return toolchain.subprocess.CompletedProcess(
                        arguments, 0, output, b""
                    )

                with mock.patch.object(
                    toolchain.ReviewedHistoricalToolchain,
                    "_invoke",
                    new=malformed_call,
                ), self.assertRaises(
                    toolchain.HistoricalFoundryToolchainError
                ) as raised:
                    capability._verify_connected_kat()
                self._assert_sanitized_error(
                    raised.exception, endpoint, rpc_body, local_path
                )

                outputs = self._preflight_outputs()
                calls = []

                def fail_at_forge(_capability, name, arguments, timeout=30):
                    calls.append(name)
                    if name == "cast":
                        return toolchain.subprocess.CompletedProcess(
                            arguments, 0, outputs[len(calls) - 1], b""
                        )
                    try:
                        raise OSError(endpoint + rpc_body + local_path)
                    except OSError as dirty:
                        raise toolchain.HistoricalFoundryToolchainError(
                            "toolchain_process_failed"
                        ) from dirty

                with mock.patch.object(
                    toolchain.ReviewedHistoricalToolchain,
                    "_invoke",
                    new=fail_at_forge,
                ), mock.patch.object(
                    toolchain.ReviewedHistoricalToolchain,
                    "_verify_versions_and_hardfork",
                    return_value=None,
                ), self.assertRaises(
                    toolchain.HistoricalFoundryToolchainError
                ) as raised:
                    capability._verify_connected_kat()
                self._assert_sanitized_error(
                    raised.exception, endpoint, rpc_body, local_path
                )


class HistoricalFoundryAnvilProcessLeaseNativeTests(unittest.TestCase):
    def test_task6_spawn_surface_and_test_lease_are_sealed(self):
        self.assertIsNone(toolchain._validate_historical_process_output_counts(
            stdout_bytes=32_768, stderr_bytes=32_768
        ))
        with self.assertRaises(ValueError):
            toolchain._validate_historical_process_output_counts(
                stdout_bytes=32_769, stderr_bytes=32_768
            )
        signature = inspect.signature(
            toolchain.ReviewedHistoricalToolchain._spawn_historical_anvil_process
        )
        self.assertEqual(
            tuple(signature.parameters),
            ("self", "selected_block", "hardfork", "relay_port", "anvil_port"),
        )

        class Process:
            returncode = None

            def __init__(self):
                self.calls = []

            def terminate(self):
                self.calls.append("term")

            def kill(self):
                self.calls.append("kill")

            def wait(self, timeout):
                self.calls.append(("wait", timeout))
                self.returncode = 0
                return 0

        process = Process()
        cleanup = mock.Mock()
        lease = toolchain._issue_historical_process_lease_for_test(
            process=process, cleanup=cleanup,
            binary_sha256="1" * 64, selected_block=7, hardfork="osaka",
        )
        self.assertNotIn("object at", repr(lease))
        self.assertEqual(lease._close_with_budget(lambda cap: min(cap, 2.0)), None)
        self.assertEqual(process.calls, ["term", ("wait", 2.0)])
        cleanup.assert_called_once_with()


if __name__ == "__main__":
    unittest.main()
