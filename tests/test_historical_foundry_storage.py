from __future__ import annotations

import importlib
import importlib.util
import ast
import asyncio
import copy
from decimal import Decimal
import dis
import gc
import gzip
import hashlib
import io
import inspect
import json
import os
from pathlib import Path
import pickle
import stat
import sys
import tempfile
import unittest
import weakref
from types import MappingProxyType
from unittest import mock


def _valid_transfer_arguments(
    *,
    exchange_index=1,
    logical_batch_index=1,
    attempt_index=1,
    request_ids=(1,),
    response_ids=None,
    request_bytes=b'{"id":1}',
    decoded_bytes=b'[{"id":1,"result":"0x1"}]',
):
    if response_ids is None:
        response_ids = tuple(reversed(request_ids))
    wire_bytes = b"sealed-wire-metadata"
    return {
        "exchange_projection": {
            "exchange_index": exchange_index,
            "logical_batch_index": logical_batch_index,
            "attempt_index": attempt_index,
            "request_byte_count": len(request_bytes),
            "request_sha256": hashlib.sha256(request_bytes).hexdigest(),
            "request_ids": request_ids,
            "wire_byte_count": len(wire_bytes),
            "wire_sha256": hashlib.sha256(wire_bytes).hexdigest(),
            "decoded_byte_count": len(decoded_bytes),
            "decoded_sha256": hashlib.sha256(decoded_bytes).hexdigest(),
            "response_ids": response_ids,
        },
        "canonical_request_bytes": request_bytes,
        "decoded_response_bytes": decoded_bytes,
    }


class HistoricalFoundryStorageSurfaceTests(unittest.TestCase):
    def test_task3b_storage_surfaces_are_exact(self):
        storage = importlib.import_module("scripts.historical_foundry_storage")
        method_parameters = {
            (storage._HistoricalWindowExchangeSpool,
             "_bind_claimed_source_authority_from_rpc"): (
                "self", "claim", "bound_rpc_module", "bound_scan_module",
                "bound_storage_module", "source_capsule",
            ),
            (storage._HistoricalWindowExchangeSpool,
             "_verify_bound_source_authority_for_claimed_finalization"): (
                "self", "claim", "prefinalization",
            ),
            (storage._HistoricalWindowExchangeSpool,
             "issue_transfer_from_bound_rpc"): (
                "self", "claim", "exchange_projection",
                "canonical_request_bytes", "decoded_response_bytes",
            ),
            (storage._HistoricalWindowExchangeSpool,
             "verify_pending_receipt"): (
                "self", "transfer", "pending_receipt",
            ),
            (storage._HistoricalWindowExchangeSpool,
             "verify_committed_receipt"): ("self", "transfer", "receipt"),
            (storage._HistoricalWindowExchangeSpool,
             "release_verified_transfer"): ("self", "transfer", "receipt"),
            (storage._SealedHistoricalWindowExchangeSpool,
             "_open_reconciliation_cursor_from_bound_scan"): (
                "self", "claim", "finalization",
            ),
            (storage._SealedHistoricalWindowExchangeSpool,
             "mint_production_historical_window_capability"): (
                "self", "claim", "finalization", "reconciliation",
            ),
        }
        for (authority_class, name), parameters in method_parameters.items():
            signature = inspect.signature(getattr(authority_class, name))
            self.assertEqual(tuple(signature.parameters), parameters)
            self.assertTrue(all(
                parameter.kind is inspect.Parameter.KEYWORD_ONLY
                for parameter in tuple(signature.parameters.values())[1:]
            ))

        cursor_methods = {
            "__enter__": ("self",), "__iter__": ("self",),
            "__next__": ("self",),
            "__exit__": ("self", "error_type", "error", "traceback"),
            "close": ("self",),
        }
        for name, parameters in cursor_methods.items():
            self.assertEqual(
                tuple(inspect.signature(getattr(
                    storage._HistoricalWindowSpoolReconciliationCursor, name
                )).parameters),
                parameters,
            )
        self.assertEqual(
            tuple(inspect.signature(
                storage.consume_production_historical_window_capability
            ).parameters),
            ("capability",),
        )

    def test_task3b_storage_authorities_are_closed(self):
        storage = importlib.import_module("scripts.historical_foundry_storage")
        for authority_class in (
            storage._HistoricalWindowSpoolReconciliationCursor,
            storage._ConsumedProductionHistoricalWindowCapabilityView,
        ):
            with self.subTest(authority=authority_class.__name__):
                with self.assertRaises(storage.HistoricalFoundryStorageError):
                    authority_class()
                with self.assertRaises(TypeError):
                    type("Forbidden", (authority_class,), {})
                clone = object.__new__(authority_class)
                self.assertFalse(hasattr(clone, "__dict__"))
                self.assertEqual(
                    repr(clone), authority_class.__name__ + "(<redacted>)"
                )
                with self.assertRaises(TypeError):
                    copy.copy(clone)
                with self.assertRaises(TypeError):
                    copy.deepcopy(clone)
                with self.assertRaises(TypeError):
                    pickle.dumps(clone)
                with self.assertRaises(TypeError):
                    json.dumps(clone)

    def test_task4a_surface_and_production_bridges_are_closed(self):
        storage = importlib.import_module("scripts.historical_foundry_storage")

        expected_classes = (
            "HistoricalFoundryStorageError",
            "_ProductionArchiveRpcExchangeTransfer",
            "_PendingHistoricalWindowSpoolReceipt",
            "_HistoricalWindowSpoolReceipt",
            "_ProductionHistoricalWindowCapability",
            "_HistoricalWindowSpoolSourceBinding",
            "_HistoricalWindowRunQuota",
            "_HistoricalWindowExchangeSpool",
            "_SealedHistoricalWindowExchangeSpool",
        )
        expected_functions = {
            "_open_historical_window_exchange_spool": "(*, data_dir: 'Path') -> \"'_HistoricalWindowExchangeSpool'\"",
            "_issue_historical_window_exchange_transfer_for_test": "(*, spool: \"'_HistoricalWindowExchangeSpool'\", exchange_projection: 'Mapping[str, Any]', canonical_request_bytes: 'bytes', decoded_response_bytes: 'bytes') -> \"'_ProductionArchiveRpcExchangeTransfer'\"",
            "_get_historical_window_run_quota_for_test": "(*, spool: \"'_HistoricalWindowExchangeSpool'\") -> \"'_HistoricalWindowRunQuota'\"",
            "_project_historical_window_exchange_spool_for_test": "(*, spool_or_sealed: 'object') -> 'Mapping[str, Any]'",
        }
        for name in expected_classes:
            self.assertTrue(hasattr(storage, name), name)
        for name, signature in expected_functions.items():
            self.assertEqual(str(inspect.signature(getattr(storage, name))), signature)

        self.assertTrue(
            hasattr(storage, "consume_production_historical_window_capability")
        )
        self.assertTrue(hasattr(
            storage._HistoricalWindowExchangeSpool,
            "_bind_claimed_source_authority_from_rpc",
        ))
        self.assertTrue(hasattr(
            storage._HistoricalWindowExchangeSpool,
            "issue_transfer_from_bound_rpc",
        ))
        self.assertTrue(hasattr(
            storage._HistoricalWindowExchangeSpool, "verify_pending_receipt"
        ))
        self.assertTrue(hasattr(
            storage._HistoricalWindowExchangeSpool, "verify_committed_receipt"
        ))
        self.assertTrue(hasattr(
            storage._SealedHistoricalWindowExchangeSpool,
            "mint_production_historical_window_capability",
        ))
        forbidden_module_seams = (
            "issuer",
            "registry",
            "register",
            "retire",
            "tombstone",
            "lane_token",
            "lineage_token",
            "constructor_provenance",
        )
        self.assertEqual(
            [
                name
                for name in vars(storage)
                if any(marker in name for marker in forbidden_module_seams)
            ],
            [],
        )

    def test_authority_handles_are_slotless_redacted_and_nonconstructible(self):
        storage = importlib.import_module("scripts.historical_foundry_storage")
        authority_classes = (
            storage._ProductionArchiveRpcExchangeTransfer,
            storage._PendingHistoricalWindowSpoolReceipt,
            storage._HistoricalWindowSpoolReceipt,
            storage._ProductionHistoricalWindowCapability,
            storage._HistoricalWindowSpoolSourceBinding,
            storage._HistoricalWindowRunQuota,
            storage._HistoricalWindowExchangeSpool,
            storage._SealedHistoricalWindowExchangeSpool,
        )
        for index, authority_class in enumerate(authority_classes):
            with self.subTest(authority_class=authority_class.__name__):
                with self.assertRaises(storage.HistoricalFoundryStorageError):
                    authority_class()
                with self.assertRaises(TypeError):
                    type("ForbiddenSubclass", (authority_class,), {})
                clone = object.__new__(authority_class)
                self.assertFalse(hasattr(clone, "__dict__"))
                self.assertIs(weakref.ref(clone)(), clone)
                with self.assertRaises(TypeError):
                    object.__setattr__(
                        clone,
                        "__class__",
                        authority_classes[(index + 1) % len(authority_classes)],
                    )
                self.assertEqual(
                    repr(clone),
                    "{}(<redacted>)".format(authority_class.__name__),
                )
                with self.assertRaises(TypeError):
                    copy.copy(clone)
                with self.assertRaises(TypeError):
                    copy.deepcopy(clone)
                with self.assertRaises(TypeError):
                    pickle.dumps(clone)
                with self.assertRaises(TypeError):
                    json.dumps(clone)

        error = storage.HistoricalFoundryStorageError()
        self.assertEqual(str(error), "historical foundry storage failed")
        self.assertEqual(repr(error), "HistoricalFoundryStorageError(<redacted>)")
        with self.assertRaises(TypeError):
            type("ForbiddenStorageError", (type(error),), {})
        with self.assertRaises(AttributeError):
            error.marker = "secret"
        with self.assertRaises(TypeError):
            copy.copy(error)
        with self.assertRaises(TypeError):
            copy.deepcopy(error)
        with self.assertRaises(TypeError):
            pickle.dumps(error)

    def test_module_runtime_ast_and_import_graph_are_offline_storage_only(self):
        storage = importlib.import_module("scripts.historical_foundry_storage")
        source_path = Path(__file__).parents[1] / "scripts" / "historical_foundry_storage.py"
        tree = ast.parse(source_path.read_text(encoding="utf-8"))
        forbidden = {
            "scripts.route_publication",
            "scripts.historical_foundry_rpc",
            "scripts.historical_foundry_scan",
            "urllib",
            "http.client",
            "ssl",
            "socket",
            "subprocess",
        }
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    self.assertFalse(
                        any(alias.name == name or alias.name.startswith(name + ".") for name in forbidden),
                        alias.name,
                    )
            elif isinstance(node, ast.ImportFrom):
                module_name = node.module or ""
                self.assertFalse(
                    any(module_name == name or module_name.startswith(name + ".") for name in forbidden),
                    module_name,
                )
            elif isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name):
                self.assertFalse(
                    node.value.id == "os" and node.attr in {"environ", "getenv"}
                )

        before = set(sys.modules)
        spec = importlib.util.spec_from_file_location(
            "_historical_foundry_storage_surface_probe", source_path
        )
        self.assertIsNotNone(spec)
        self.assertIsNotNone(spec.loader)
        module = importlib.util.module_from_spec(spec)

        class RejectEnvironment(dict):
            def __iter__(self):
                raise AssertionError("environment read")

            def __getitem__(self, _key):
                raise AssertionError("environment read")

            def get(self, _key, _default=None):
                raise AssertionError("environment read")

        with mock.patch.object(os, "environ", RejectEnvironment()):
            spec.loader.exec_module(module)
        imported = set(sys.modules) - before
        self.assertFalse(
            any(
                name == item or name.startswith(item + ".")
                for name in forbidden
                for item in imported
            ),
            imported,
        )
        self.assertTrue(hasattr(module, "_open_historical_window_exchange_spool"))
        self.assertEqual(storage._HistoricalWindowExchangeSpool.__module__, storage.__name__)


class HistoricalFoundryStorageFilesystemTests(unittest.TestCase):
    def setUp(self):
        self.storage = importlib.import_module("scripts.historical_foundry_storage")
        self.temporary = tempfile.TemporaryDirectory(dir="/private/tmp")
        self.data_dir = Path(self.temporary.name)
        os.chmod(str(self.data_dir), 0o700)

    def tearDown(self):
        self.temporary.cleanup()

    def _open(self):
        return self.storage._open_historical_window_exchange_spool(
            data_dir=self.data_dir
        )

    def _member_names(self):
        return tuple(sorted(path.name for path in self.data_dir.iterdir()))

    def test_data_dir_requires_exact_absolute_preexisting_private_operator_path(self):
        invalid = (
            str(self.data_dir),
            Path("relative"),
            Path("/private/tmp/left/../right"),
            self.data_dir / "missing",
        )
        for value in invalid:
            with self.subTest(value=repr(value)):
                with self.assertRaises(self.storage.HistoricalFoundryStorageError):
                    self.storage._open_historical_window_exchange_spool(data_dir=value)
                self.assertEqual(self._member_names(), ())

        os.chmod(str(self.data_dir), 0o722)
        try:
            with self.assertRaises(self.storage.HistoricalFoundryStorageError):
                self._open()
        finally:
            os.chmod(str(self.data_dir), 0o700)
        self.assertEqual(self._member_names(), ())

        with mock.patch.object(
            self.storage.os, "geteuid", return_value=os.geteuid() + 1
        ):
            with self.assertRaises(self.storage.HistoricalFoundryStorageError):
                self._open()
        self.assertEqual(self._member_names(), ())

    def test_data_dir_path_resource_grammar_exact_and_plus_one(self):
        base = "/private/tmp"
        exact_component = Path(base + "/" + "a" * 255)
        oversized_component = Path(base + "/" + "a" * 256)
        exact_components = Path(base + "/" + "/".join("a" for _ in range(62)))
        oversized_components = Path(base + "/" + "/".join("a" for _ in range(63)))
        exact_total = Path(base + "/" + "/".join(("a" * 255, "b" * 255, "c" * 255, "d" * 243)))
        oversized_total = Path(base + "/" + "/".join(("a" * 255, "b" * 255, "c" * 255, "d" * 244)))
        self.assertEqual(len(os.fsencode(str(exact_total))), 1024)
        self.assertEqual(len(os.fsencode(str(oversized_total))), 1025)

        for value in (exact_component, exact_components, exact_total):
            with self.subTest(inclusive=str(value)[-12:]):
                supported = set(os.supports_dir_fd)
                original_open = os.open
                with mock.patch.object(
                    self.storage.os, "open", side_effect=OSError("probe")
                ) as opener:
                    supported.discard(original_open)
                    supported.add(opener)
                    with mock.patch.object(
                        self.storage.os, "supports_dir_fd", supported
                    ):
                        with self.assertRaises(self.storage.HistoricalFoundryStorageError):
                            self.storage._open_historical_window_exchange_spool(data_dir=value)
                self.assertEqual(opener.call_count, 1)

        for value in (oversized_component, oversized_components, oversized_total):
            with self.subTest(rejected=str(value)[-12:]):
                with mock.patch.object(
                    self.storage.os, "open", side_effect=AssertionError("opened")
                ) as opener:
                    with self.assertRaises(self.storage.HistoricalFoundryStorageError):
                        self.storage._open_historical_window_exchange_spool(data_dir=value)
                self.assertEqual(opener.call_count, 0)

    def test_held_ancestry_rejects_symlink_and_component_identity_races(self):
        real = self.data_dir / "real"
        real.mkdir(mode=0o700)
        link = self.data_dir / "link"
        link.symlink_to(real, target_is_directory=True)
        with self.assertRaises(self.storage.HistoricalFoundryStorageError):
            self.storage._open_historical_window_exchange_spool(data_dir=link)
        self.assertEqual(tuple(real.iterdir()), ())

        parent = self.data_dir / "parent"
        leaf = parent / "leaf"
        leaf.mkdir(parents=True, mode=0o700)
        spool = self.storage._open_historical_window_exchange_spool(data_dir=leaf)
        moved = self.data_dir / "moved"
        parent.rename(moved)
        leaf.mkdir(parents=True, mode=0o700)
        with self.assertRaises(self.storage.HistoricalFoundryStorageError):
            spool.close()
        self.assertEqual(tuple(leaf.iterdir()), ())
        self.assertEqual(len(tuple((moved / "leaf").iterdir())), 1)
        self.assertIsNone(spool.close())

    def test_spool_create_is_noreplace_private_0600_and_parent_fsynced(self):
        real_fsync = os.fsync
        fsynced_modes = []

        def recording_fsync(fd):
            fsynced_modes.append(stat.S_IFMT(os.fstat(fd).st_mode))
            return real_fsync(fd)

        with mock.patch.object(self.storage.os, "fsync", side_effect=recording_fsync):
            spool = self._open()
        names = self._member_names()
        self.assertEqual(len(names), 1)
        self.assertRegex(
            names[0], r"^\.historical-foundry-exchange-spool-[0-9a-f]{32}\.bin$"
        )
        details = os.lstat(str(self.data_dir / names[0]))
        self.assertTrue(stat.S_ISREG(details.st_mode))
        self.assertEqual(details.st_nlink, 1)
        self.assertEqual(stat.S_IMODE(details.st_mode), 0o600)
        self.assertEqual(details.st_size, 0)
        self.assertIn(stat.S_IFDIR, fsynced_modes)
        spool.close()
        self.assertEqual(self._member_names(), ())

    def test_create_failure_unlinks_only_this_attempts_verified_entry(self):
        basename = ".historical-foundry-exchange-spool-" + "11" * 16 + ".bin"
        collision = self.data_dir / basename
        collision.write_bytes(b"untouched")
        os.chmod(str(collision), 0o600)
        with mock.patch.object(self.storage.os, "urandom", return_value=b"\x11" * 16):
            with self.assertRaises(self.storage.HistoricalFoundryStorageError):
                self._open()
        self.assertEqual(collision.read_bytes(), b"untouched")

        collision.unlink()
        real_fsync = os.fsync
        calls = 0

        def fail_first_directory_fsync(fd):
            nonlocal calls
            if stat.S_ISDIR(os.fstat(fd).st_mode) and calls == 0:
                calls += 1
                raise OSError("secret create failure")
            return real_fsync(fd)

        with mock.patch.object(self.storage.os, "urandom", return_value=b"\x11" * 16), mock.patch.object(
            self.storage.os, "fsync", side_effect=fail_first_directory_fsync
        ):
            with self.assertRaises(self.storage.HistoricalFoundryStorageError) as caught:
                self._open()
        self.assertEqual(str(caught.exception), "historical foundry storage failed")
        self.assertEqual(self._member_names(), ())

    def test_spool_entry_rejects_hardlink_symlink_rename_and_replacement(self):
        spool = self._open()
        name = self._member_names()[0]
        original = self.data_dir / name
        extra = self.data_dir / "extra-link"
        os.link(str(original), str(extra))
        with self.assertRaises(self.storage.HistoricalFoundryStorageError):
            spool.close()
        self.assertTrue(original.exists())
        self.assertTrue(extra.exists())
        original.unlink()
        extra.unlink()

        spool = self._open()
        name = self._member_names()[0]
        original = self.data_dir / name
        renamed = self.data_dir / "renamed"
        original.rename(renamed)
        original.symlink_to(renamed.name)
        with self.assertRaises(self.storage.HistoricalFoundryStorageError):
            spool.close()
        self.assertTrue(original.is_symlink())
        self.assertEqual(renamed.read_bytes(), b"")
        original.unlink()
        renamed.unlink()

    def test_review_acquisition_cleanup_owns_root_and_new_member_immediately(self):
        real_open = os.open
        real_fstat = os.fstat
        real_stat = os.stat
        real_close = os.close

        for root_boundary in ("fstat", "stat"):
            with self.subTest(root_boundary=root_boundary):
                opened = []
                failed = [False]

                def recording_open(path, flags, *args, **kwargs):
                    fd = real_open(path, flags, *args, **kwargs)
                    opened.append(fd)
                    return fd

                def failing_fstat(fd):
                    if root_boundary == "fstat" and not failed[0]:
                        failed[0] = True
                        raise OSError("root metadata sentinel")
                    return real_fstat(fd)

                def failing_stat(path, *args, **kwargs):
                    if (
                        root_boundary == "stat"
                        and path == os.sep
                        and "dir_fd" not in kwargs
                        and not failed[0]
                    ):
                        failed[0] = True
                        raise OSError("root entry sentinel")
                    return real_stat(path, *args, **kwargs)

                dir_fd_support = set(os.supports_dir_fd)
                dir_fd_support.discard(real_open)
                dir_fd_support.add(recording_open)
                dir_fd_support.discard(real_stat)
                dir_fd_support.add(failing_stat)
                follow_support = set(os.supports_follow_symlinks)
                follow_support.discard(real_stat)
                follow_support.add(failing_stat)
                with mock.patch.object(
                    self.storage.os, "open", new=recording_open
                ), mock.patch.object(
                    self.storage.os, "fstat", new=failing_fstat
                ), mock.patch.object(
                    self.storage.os, "stat", new=failing_stat
                ), mock.patch.object(
                    self.storage.os, "supports_dir_fd", dir_fd_support
                ), mock.patch.object(
                    self.storage.os,
                    "supports_follow_symlinks",
                    follow_support,
                ):
                    with self.assertRaises(
                        self.storage.HistoricalFoundryStorageError
                    ):
                        self._open()
                live = []
                for fd in opened:
                    try:
                        real_fstat(fd)
                    except OSError:
                        continue
                    live.append(fd)
                    real_close(fd)
                self.assertEqual(live, [])

        for member_boundary in ("fstat", "stat"):
            with self.subTest(member_boundary=member_boundary):
                opened = []
                file_fd = [None]
                failed = [False]

                def recording_open(path, flags, *args, **kwargs):
                    fd = real_open(path, flags, *args, **kwargs)
                    opened.append(fd)
                    if flags & os.O_CREAT:
                        file_fd[0] = fd
                    return fd

                def failing_fstat(fd):
                    if (
                        member_boundary == "fstat"
                        and fd == file_fd[0]
                        and not failed[0]
                    ):
                        failed[0] = True
                        raise OSError("member metadata sentinel")
                    return real_fstat(fd)

                def failing_stat(path, *args, **kwargs):
                    if (
                        member_boundary == "stat"
                        and type(path) is str
                        and path.startswith(
                            ".historical-foundry-exchange-spool-"
                        )
                        and kwargs.get("dir_fd") is not None
                        and not failed[0]
                    ):
                        failed[0] = True
                        raise OSError("member entry sentinel")
                    return real_stat(path, *args, **kwargs)

                dir_fd_support = set(os.supports_dir_fd)
                dir_fd_support.discard(real_open)
                dir_fd_support.add(recording_open)
                dir_fd_support.discard(real_stat)
                dir_fd_support.add(failing_stat)
                follow_support = set(os.supports_follow_symlinks)
                follow_support.discard(real_stat)
                follow_support.add(failing_stat)
                with mock.patch.object(
                    self.storage.os, "open", new=recording_open
                ), mock.patch.object(
                    self.storage.os, "fstat", new=failing_fstat
                ), mock.patch.object(
                    self.storage.os, "stat", new=failing_stat
                ), mock.patch.object(
                    self.storage.os, "supports_dir_fd", dir_fd_support
                ), mock.patch.object(
                    self.storage.os,
                    "supports_follow_symlinks",
                    follow_support,
                ):
                    with self.assertRaises(
                        self.storage.HistoricalFoundryStorageError
                    ):
                        self._open()
                members = tuple(self.data_dir.iterdir())
                all_closed = True
                for fd in opened:
                    try:
                        real_fstat(fd)
                    except OSError:
                        continue
                    all_closed = False
                    real_close(fd)
                for member in members:
                    member.unlink()
                self.assertEqual(members, ())
                self.assertTrue(all_closed)

        opened = []

        def restrictive_create(path, flags, *args, **kwargs):
            fd = real_open(path, flags, *args, **kwargs)
            opened.append(fd)
            if flags & os.O_CREAT:
                os.fchmod(fd, 0o400)
            return fd

        dir_fd_support = set(os.supports_dir_fd)
        dir_fd_support.discard(real_open)
        dir_fd_support.add(restrictive_create)
        with mock.patch.object(
            self.storage.os, "open", new=restrictive_create
        ), mock.patch.object(
            self.storage.os, "supports_dir_fd", dir_fd_support
        ):
            with self.assertRaises(
                self.storage.HistoricalFoundryStorageError
            ):
                self._open()
        members = tuple(self.data_dir.iterdir())
        all_closed = True
        for fd in opened:
            try:
                real_fstat(fd)
            except OSError:
                continue
            all_closed = False
            real_close(fd)
        for member in members:
            member.unlink()
        self.assertEqual(members, ())
        self.assertTrue(all_closed)

    def test_review_round2_open_return_cancellation_preserves_descriptor_ownership(self):
        real_open = os.open
        real_fstat = os.fstat
        real_close = os.close

        for label in ("root", "child", "member"):
            with self.subTest(label=label):
                case_directory = self.data_dir / (label + "-return")
                if label == "child":
                    case_directory = case_directory / "target-child"
                    case_directory.mkdir(parents=True, mode=0o700)
                else:
                    case_directory.mkdir(mode=0o700)
                os.chmod(str(case_directory), 0o700)
                opened = []
                armed = [False]
                fired = [False]
                marker = asyncio.CancelledError(label + " open return")

                def recording_open(path, flags, *args, **kwargs):
                    fd = real_open(path, flags, *args, **kwargs)
                    opened.append(fd)
                    target = (
                        (label == "root" and path == os.sep)
                        or (
                            label == "child"
                            and path == case_directory.name
                            and bool(flags & os.O_DIRECTORY)
                        )
                        or (label == "member" and bool(flags & os.O_CREAT))
                    )
                    if target and not armed[0]:
                        armed[0] = True
                    return fd

                def tracer(frame, event, _argument):
                    expected = (
                        "_open_historical_window_exchange_spool"
                        if label == "member"
                        else "_open_ancestry"
                    )
                    if (
                        armed[0]
                        and not fired[0]
                        and frame.f_code.co_name == expected
                        and event == "line"
                    ):
                        fired[0] = True
                        raise marker
                    return tracer

                dir_fd_support = set(os.supports_dir_fd)
                dir_fd_support.discard(real_open)
                dir_fd_support.add(recording_open)
                caught = None
                prior_trace = sys.gettrace()
                try:
                    with mock.patch.object(
                        self.storage.os, "open", new=recording_open
                    ), mock.patch.object(
                        self.storage.os, "supports_dir_fd", dir_fd_support
                    ):
                        sys.settrace(tracer)
                        self.storage._open_historical_window_exchange_spool(
                            data_dir=case_directory
                        )
                except BaseException as error:
                    caught = error
                finally:
                    sys.settrace(prior_trace)
                live = []
                for fd in set(opened):
                    try:
                        real_fstat(fd)
                    except OSError:
                        continue
                    live.append(fd)
                    real_close(fd)
                members = tuple(case_directory.iterdir())
                for member in members:
                    member.unlink()
                self.assertTrue(armed[0])
                self.assertTrue(fired[0])
                self.assertIs(caught, marker)
                self.assertEqual(live, [])
                self.assertEqual(members, ())
                marker = marker.with_traceback(None)

    def test_review_round2_open_slot_return_is_cleanup_owned(self):
        real_open = os.open
        real_fstat = os.fstat
        real_close = os.close

        for label in ("root", "child"):
            with self.subTest(label=label):
                directory = self.data_dir / ("slot-return-" + label)
                if label == "child":
                    directory = directory / "target-child"
                    directory.mkdir(parents=True, mode=0o700)
                else:
                    directory.mkdir(mode=0o700)
                os.chmod(str(directory), 0o700)
                opened = []
                armed = [False]
                fired = [False]
                marker = asyncio.CancelledError(label + " slot return")

                def recording_open(path, flags, *args, **kwargs):
                    fd = real_open(path, flags, *args, **kwargs)
                    opened.append(fd)
                    if (
                        (label == "root" and path == os.sep)
                        or (
                            label == "child"
                            and path == directory.name
                            and bool(flags & os.O_DIRECTORY)
                        )
                    ):
                        armed[0] = True
                    return fd

                def tracer(frame, event, _argument):
                    if (
                        armed[0]
                        and not fired[0]
                        and frame.f_code.co_name == "_open_into_slot"
                        and event == "return"
                    ):
                        fired[0] = True
                        raise marker
                    return tracer

                dir_fd_support = set(os.supports_dir_fd)
                dir_fd_support.discard(real_open)
                dir_fd_support.add(recording_open)
                caught = None
                prior_trace = sys.gettrace()
                try:
                    with mock.patch.object(
                        self.storage.os, "open", new=recording_open
                    ), mock.patch.object(
                        self.storage.os, "supports_dir_fd", dir_fd_support
                    ):
                        sys.settrace(tracer)
                        self.storage._open_historical_window_exchange_spool(
                            data_dir=directory
                        )
                except BaseException as error:
                    caught = error
                finally:
                    sys.settrace(prior_trace)
                live = []
                for fd in set(opened):
                    try:
                        real_fstat(fd)
                    except OSError:
                        continue
                    live.append(fd)
                    real_close(fd)
                self.assertTrue(armed[0])
                self.assertTrue(fired[0])
                self.assertIs(caught, marker)
                self.assertEqual(live, [])
                self.assertEqual(tuple(directory.iterdir()), ())
                marker = marker.with_traceback(None)

    def test_review_round2_open_handle_install_cancellation_retires_unpublished_handles(self):
        helper_names = ("_prepare_handle", "_install_open_handles")
        authority_names = {
            "_HistoricalWindowRunQuota",
            "_HistoricalWindowExchangeSpool",
        }
        trace_points = []
        outer_points = []
        installer_returned = [False]

        def helper_key(frame, event):
            if frame.f_code.co_name == "_prepare_handle":
                authority_class = frame.f_locals.get("authority_class")
                authority_name = getattr(authority_class, "__name__", None)
                if authority_name not in authority_names:
                    return None
                return (frame.f_code.co_name, authority_name, event, frame.f_lineno)
            if frame.f_code.co_name == "_install_open_handles":
                return (frame.f_code.co_name, "open", event, frame.f_lineno)
            return None

        def discover(frame, event, _argument):
            if event in ("line", "return") and frame.f_code.co_name in helper_names:
                key = helper_key(frame, event)
                if key is not None and key not in trace_points:
                    trace_points.append(key)
                if frame.f_code.co_name == "_install_open_handles" and event == "return":
                    installer_returned[0] = True
            elif (
                installer_returned[0]
                and frame.f_code.co_name
                == "_open_historical_window_exchange_spool"
                and event == "line"
            ):
                point = (event, frame.f_lineno)
                if point not in outer_points:
                    outer_points.append(point)
            return discover

        probe_directory = self.data_dir / "open-install-probe"
        probe_directory.mkdir(mode=0o700)
        prior_trace = sys.gettrace()
        try:
            sys.settrace(discover)
            probe = self.storage._open_historical_window_exchange_spool(
                data_dir=probe_directory
            )
        finally:
            sys.settrace(prior_trace)
        probe.close()
        self.assertGreater(len(trace_points), 0)
        self.assertGreater(len(outer_points), 0)

        authority_types = (
            self.storage._HistoricalWindowRunQuota,
            self.storage._HistoricalWindowExchangeSpool,
        )
        targets = tuple(("helper", point) for point in trace_points) + tuple(
            ("outer", point) for point in outer_points
        )
        for index, (scope, point) in enumerate(targets):
            with self.subTest(scope=scope, trace_point=point):
                directory = self.data_dir / "open-install-{}".format(index)
                directory.mkdir(mode=0o700)
                marker = asyncio.CancelledError(
                    "open install {}".format(point)
                )
                fired = [False]
                returned = [False]
                references = []

                def tracer(frame, event, _argument):
                    key = helper_key(frame, event)
                    if frame.f_code.co_name == "_install_open_handles" and event == "return":
                        returned[0] = True
                    helper_match = scope == "helper" and key == point
                    outer_match = (
                        scope == "outer"
                        and returned[0]
                        and frame.f_code.co_name
                        == "_open_historical_window_exchange_spool"
                        and (event, frame.f_lineno) == point
                    )
                    if not fired[0] and (helper_match or outer_match):
                        for value in frame.f_locals.values():
                            if type(value) in authority_types:
                                references.append(weakref.ref(value))
                        fired[0] = True
                        raise marker
                    return tracer

                caught = None
                prior_trace = sys.gettrace()
                try:
                    sys.settrace(tracer)
                    self.storage._open_historical_window_exchange_spool(
                        data_dir=directory
                    )
                except BaseException as error:
                    caught = error
                finally:
                    sys.settrace(prior_trace)
                self.assertTrue(fired[0])
                self.assertIs(caught, marker)
                self.assertEqual(tuple(directory.iterdir()), ())
                caught = None
                marker = marker.with_traceback(None)
                gc.collect()
                self.assertTrue(all(reference() is None for reference in references))

    def test_review_partial_ancestry_cleanup_preserves_control_priority(self):
        first = self.data_dir / "first"
        second = first / "second"
        second.mkdir(parents=True, mode=0o700)
        real_open = os.open
        real_close = os.close
        real_fstat = os.fstat

        scenarios = (
            (
                OSError("ordinary open sentinel"),
                KeyboardInterrupt("cleanup control sentinel"),
                "cleanup",
            ),
            (
                GeneratorExit("primary control sentinel"),
                SystemExit("secondary control sentinel"),
                "primary",
            ),
            (
                OSError("ordinary open sentinel"),
                OSError("ordinary cleanup sentinel"),
                "sanitized",
            ),
        )
        for primary, cleanup, expected in scenarios:
            with self.subTest(expected=expected):
                opened = []
                cleanup_raised = [False]

                def failing_open(path, flags, *args, **kwargs):
                    if path == "second":
                        raise primary
                    fd = real_open(path, flags, *args, **kwargs)
                    opened.append(fd)
                    return fd

                def closing_with_failure(fd):
                    real_close(fd)
                    if not cleanup_raised[0]:
                        cleanup_raised[0] = True
                        raise cleanup

                dir_fd_support = set(os.supports_dir_fd)
                dir_fd_support.discard(real_open)
                dir_fd_support.add(failing_open)
                caught = None
                with mock.patch.object(
                    self.storage.os, "open", new=failing_open
                ), mock.patch.object(
                    self.storage.os, "close", new=closing_with_failure
                ), mock.patch.object(
                    self.storage.os, "supports_dir_fd", dir_fd_support
                ):
                    try:
                        self.storage._open_historical_window_exchange_spool(
                            data_dir=second
                        )
                    except BaseException as error:
                        caught = error
                self.assertIsNotNone(caught)
                if expected == "cleanup":
                    self.assertIs(caught, cleanup)
                elif expected == "primary":
                    self.assertIs(caught, primary)
                else:
                    self.assertIs(
                        type(caught), self.storage.HistoricalFoundryStorageError
                    )
                    self.assertIsNone(caught.__cause__)
                    self.assertIsNone(caught.__context__)
                for fd in opened:
                    with self.assertRaises(OSError):
                        real_fstat(fd)

    def test_review_leaf_resnapshot_rejects_completed_metadata_drift(self):
        real_open = os.open
        real_fstat = os.fstat
        real_close = os.close
        leaf_identity = (
            os.stat(str(self.data_dir)).st_dev,
            os.stat(str(self.data_dir)).st_ino,
        )
        opened = []
        leaf_fstats = [0]

        def recording_open(path, flags, *args, **kwargs):
            fd = real_open(path, flags, *args, **kwargs)
            opened.append(fd)
            return fd

        def drifting_fstat(fd):
            details = real_fstat(fd)
            if (details.st_dev, details.st_ino) == leaf_identity:
                leaf_fstats[0] += 1
                if leaf_fstats[0] == 5:
                    os.chmod(str(self.data_dir), 0o500)
                    details = real_fstat(fd)
            return details

        dir_fd_support = set(os.supports_dir_fd)
        dir_fd_support.discard(real_open)
        dir_fd_support.add(recording_open)
        spool = None
        caught = None
        with mock.patch.object(
            self.storage.os, "open", new=recording_open
        ), mock.patch.object(
            self.storage.os, "fstat", new=drifting_fstat
        ), mock.patch.object(
            self.storage.os, "supports_dir_fd", dir_fd_support
        ):
            try:
                spool = self._open()
            except BaseException as error:
                caught = error
        if spool is not None:
            try:
                spool.close()
            except self.storage.HistoricalFoundryStorageError:
                pass
        members = tuple(self.data_dir.iterdir())
        all_closed = True
        for fd in opened:
            try:
                real_fstat(fd)
            except OSError:
                continue
            all_closed = False
            real_close(fd)
        os.chmod(str(self.data_dir), 0o700)
        for member in members:
            member.unlink()
        self.assertIs(type(caught), self.storage.HistoricalFoundryStorageError)
        self.assertEqual(len(members), 1)
        self.assertTrue(all_closed)

        spool = self._open()
        name = self._member_names()[0]
        original = self.data_dir / name
        renamed = self.data_dir / "held-original"
        original.rename(renamed)
        original.write_bytes(b"replacement")
        os.chmod(str(original), 0o600)
        with self.assertRaises(self.storage.HistoricalFoundryStorageError):
            spool.close()
        self.assertEqual(original.read_bytes(), b"replacement")
        self.assertEqual(renamed.read_bytes(), b"")
        original.unlink()
        renamed.unlink()


class HistoricalFoundryStorageTransferTests(unittest.TestCase):
    def setUp(self):
        self.storage = importlib.import_module("scripts.historical_foundry_storage")
        self.temporary = tempfile.TemporaryDirectory(dir="/private/tmp")
        self.data_dir = Path(self.temporary.name)
        os.chmod(str(self.data_dir), 0o700)

    def tearDown(self):
        self.temporary.cleanup()

    def _open(self):
        return self.storage._open_historical_window_exchange_spool(
            data_dir=self.data_dir
        )

    def _issue(self, spool, **overrides):
        arguments = _valid_transfer_arguments(**overrides)
        return self.storage._issue_historical_window_exchange_transfer_for_test(
            spool=spool, **arguments
        )

    def test_test_bridge_claims_a_sticky_test_lane_and_rejects_unusable_spools_first(self):
        class HostileDict(dict):
            touched = False

            def __iter__(self):
                type(self).touched = True
                raise AssertionError("hostile projection inspected")

        closed = self._open()
        closed.close()
        with self.assertRaises(self.storage.HistoricalFoundryStorageError):
            self.storage._issue_historical_window_exchange_transfer_for_test(
                spool=closed,
                exchange_projection=HostileDict(),
                canonical_request_bytes=b"request",
                decoded_response_bytes=b"response",
            )
        self.assertFalse(HostileDict.touched)

        clone = object.__new__(self.storage._HistoricalWindowExchangeSpool)
        with self.assertRaises(self.storage.HistoricalFoundryStorageError):
            self.storage._issue_historical_window_exchange_transfer_for_test(
                spool=clone,
                exchange_projection=HostileDict(),
                canonical_request_bytes=b"request",
                decoded_response_bytes=b"response",
            )
        self.assertFalse(HostileDict.touched)

        spool = self._open()
        transfer = self._issue(spool)
        quota = self.storage._get_historical_window_run_quota_for_test(spool=spool)
        self.assertIs(type(transfer), self.storage._ProductionArchiveRpcExchangeTransfer)
        self.assertIs(type(quota), self.storage._HistoricalWindowRunQuota)
        self.assertEqual(
            self.storage._project_historical_window_exchange_spool_for_test(
                spool_or_sealed=spool
            )["state"],
            "active",
        )
        spool.close()
    def test_exchange_projection_is_exact_bounded_and_payload_hash_bound(self):
        spool = self._open()
        valid = _valid_transfer_arguments()
        projection = valid["exchange_projection"]
        invalid_projections = []
        extra = dict(projection)
        extra["extra"] = 1
        invalid_projections.append(extra)
        missing = dict(projection)
        missing.pop("wire_sha256")
        invalid_projections.append(missing)

        class DictSubclass(dict):
            pass

        invalid_projections.append(DictSubclass(projection))
        for key, value in (
            ("exchange_index", True),
            ("logical_batch_index", 0),
            ("attempt_index", -1),
            ("request_byte_count", len(valid["canonical_request_bytes"]) + 1),
            ("wire_byte_count", 0),
            ("decoded_byte_count", 8_388_609),
            ("request_sha256", "A" * 64),
            ("wire_sha256", "g" * 64),
            ("decoded_sha256", "0" * 64),
            ("request_ids", (1, 1)),
            ("response_ids", (2,)),
        ):
            candidate = dict(projection)
            candidate[key] = value
            invalid_projections.append(candidate)

        for candidate in invalid_projections:
            with self.subTest(candidate=tuple(candidate)):
                with self.assertRaises(self.storage.HistoricalFoundryStorageError):
                    self.storage._issue_historical_window_exchange_transfer_for_test(
                        spool=spool,
                        exchange_projection=candidate,
                        canonical_request_bytes=valid["canonical_request_bytes"],
                        decoded_response_bytes=valid["decoded_response_bytes"],
                    )

        with self.assertRaises(self.storage.HistoricalFoundryStorageError):
            self.storage._issue_historical_window_exchange_transfer_for_test(
                spool=spool,
                exchange_projection=projection,
                canonical_request_bytes=bytearray(valid["canonical_request_bytes"]),
                decoded_response_bytes=valid["decoded_response_bytes"],
            )
        transfer = self.storage._issue_historical_window_exchange_transfer_for_test(
            spool=spool, **valid
        )
        self.assertIs(type(transfer), self.storage._ProductionArchiveRpcExchangeTransfer)
        spool.close()

    def test_exchange_ids_cap_at_40_and_uint64_before_allocation(self):
        maximum_ids = tuple(range(1, 40)) + (18_446_744_073_709_551_615,)
        spool = self._open()
        transfer = self._issue(
            spool, request_ids=maximum_ids, response_ids=tuple(reversed(maximum_ids))
        )
        self.assertIs(type(transfer), self.storage._ProductionArchiveRpcExchangeTransfer)
        spool.close()

        for rejected_ids in (
            tuple(range(1, 42)),
            (18_446_744_073_709_551_616,),
            (1 << 1_000_000,),
            (True,),
        ):
            spool = self._open()
            with self.subTest(cardinality=len(rejected_ids)):
                with self.assertRaises(self.storage.HistoricalFoundryStorageError):
                    self._issue(
                        spool,
                        request_ids=rejected_ids,
                        response_ids=rejected_ids,
                    )
                replacement = self._issue(spool)
                self.assertIs(
                    type(replacement),
                    self.storage._ProductionArchiveRpcExchangeTransfer,
                )
            spool.close()

    def test_task3a_mappings_cannot_issue_a_transfer(self):
        from scripts.historical_foundry_scan import (
            _build_historical_block_header_request,
        )

        task3a_output = _build_historical_block_header_request(
            block_number=123, request_id=49
        )
        for candidate in (task3a_output, dict(task3a_output)):
            spool = self._open()
            with self.subTest(candidate_type=type(candidate).__name__):
                with self.assertRaises(self.storage.HistoricalFoundryStorageError):
                    self.storage._issue_historical_window_exchange_transfer_for_test(
                        spool=spool,
                        exchange_projection=candidate,
                        canonical_request_bytes=b"request",
                        decoded_response_bytes=b"response",
                    )
                self._issue(spool)
            spool.close()

    def test_only_one_live_transfer_exists(self):
        spool = self._open()
        first = self._issue(spool)
        with self.assertRaises(self.storage.HistoricalFoundryStorageError):
            self._issue(spool)
        self.assertIs(type(first), self.storage._ProductionArchiveRpcExchangeTransfer)
        self.assertIs(
            type(self.storage._get_historical_window_run_quota_for_test(spool=spool)),
            self.storage._HistoricalWindowRunQuota,
        )
        spool.close()

    def test_test_hooks_hide_records_and_bind_live_or_exact_retired_identity(self):
        spool = self._open()
        with self.assertRaises(self.storage.HistoricalFoundryStorageError):
            self.storage._get_historical_window_run_quota_for_test(spool=spool)
        self._issue(spool)
        quota = self.storage._get_historical_window_run_quota_for_test(spool=spool)
        self.assertFalse(hasattr(quota, "__dict__"))
        projection = self.storage._project_historical_window_exchange_spool_for_test(
            spool_or_sealed=spool
        )
        self.assertIs(type(projection), MappingProxyType)
        self.assertEqual(
            tuple(projection),
            (
                "state",
                "committed_physical_bytes",
                "committed_members",
                "provisional_physical_bytes",
                "provisional_members",
                "committed_receipt_count",
                "committed_eof",
                "receipt_inventory_sha256",
            ),
        )
        self.assertEqual(
            tuple(projection.values()),
            ("active", 0, 0, 0, 0, 0, 0, None),
        )
        with self.assertRaises(TypeError):
            projection["state"] = "forged"
        clone = object.__new__(self.storage._HistoricalWindowExchangeSpool)
        with self.assertRaises(self.storage.HistoricalFoundryStorageError):
            self.storage._project_historical_window_exchange_spool_for_test(
                spool_or_sealed=clone
            )
        spool.close()
        self.assertEqual(
            tuple(
                self.storage._project_historical_window_exchange_spool_for_test(
                    spool_or_sealed=spool
                ).values()
            ),
            ("closed", 0, 0, 0, 0, 0, 0, None),
        )

        unclaimed = self._open()
        unclaimed.close()
        with self.assertRaises(self.storage.HistoricalFoundryStorageError):
            self.storage._project_historical_window_exchange_spool_for_test(
                spool_or_sealed=unclaimed
            )

    def test_review_round2_transfer_install_cancellation_rolls_back_unpublished_binding(self):
        helper_names = ("_prepare_handle", "_install_transfer_transition")
        outer_name = "_issue_historical_window_exchange_transfer_for_test"
        transition_points = []
        outer_points = []
        helper_returned = [False]

        def helper_key(frame, event):
            if frame.f_code.co_name == "_prepare_handle":
                authority_class = frame.f_locals.get("authority_class")
                if authority_class is not self.storage._ProductionArchiveRpcExchangeTransfer:
                    return None
                return (frame.f_code.co_name, event, frame.f_lineno)
            if frame.f_code.co_name == "_install_transfer_transition":
                return (frame.f_code.co_name, event, frame.f_lineno)
            return None

        def discover(frame, event, _argument):
            if event in ("line", "return") and frame.f_code.co_name in helper_names:
                key = helper_key(frame, event)
                if key is not None and key not in transition_points:
                    transition_points.append(key)
                if key is not None and event == "return":
                    helper_returned[0] = True
            elif (
                helper_returned[0]
                and frame.f_code.co_name == outer_name
                and event == "line"
            ):
                point = (event, frame.f_lineno)
                if point not in outer_points:
                    outer_points.append(point)
            return discover

        probe = self._open()
        prior_trace = sys.gettrace()
        try:
            sys.settrace(discover)
            probe_transfer = self._issue(probe)
        finally:
            sys.settrace(prior_trace)
        probe_pending = probe.append_transfer(transfer=probe_transfer)
        probe.abort_transfer(
            transfer=probe_transfer, pending_receipt=probe_pending
        )
        probe.close()
        self.assertGreater(len(transition_points), 0)
        self.assertGreater(len(outer_points), 0)

        targets = tuple(("transition", point) for point in transition_points) + tuple(
            ("outer", point) for point in outer_points
        )
        for sticky in (False, True):
            for index, (scope, point) in enumerate(targets):
                with self.subTest(sticky=sticky, scope=scope, trace_point=point):
                    directory = self.data_dir / "transfer-install-{}-{}".format(
                        int(sticky), index
                    )
                    directory.mkdir(mode=0o700)
                    spool = self.storage._open_historical_window_exchange_spool(
                        data_dir=directory
                    )
                    if sticky:
                        baseline = self._issue(spool)
                        baseline_pending = spool.append_transfer(transfer=baseline)
                        spool.abort_transfer(
                            transfer=baseline, pending_receipt=baseline_pending
                        )
                    marker = asyncio.CancelledError(
                        "transfer install {} {}".format(scope, point)
                    )
                    fired = [False]
                    returned = [False]
                    references = []

                    def tracer(frame, event, _argument):
                        key = helper_key(frame, event)
                        if key is not None and event == "return":
                            returned[0] = True
                        transition_match = scope == "transition" and key == point
                        outer_match = (
                            scope == "outer"
                            and returned[0]
                            and frame.f_code.co_name == outer_name
                            and (event, frame.f_lineno) == point
                        )
                        if not fired[0] and (transition_match or outer_match):
                            for value in frame.f_locals.values():
                                if type(value) is self.storage._ProductionArchiveRpcExchangeTransfer:
                                    references.append(weakref.ref(value))
                            fired[0] = True
                            raise marker
                        return tracer

                    caught = None
                    prior_trace = sys.gettrace()
                    try:
                        sys.settrace(tracer)
                        self._issue(spool)
                    except BaseException as error:
                        caught = error
                    finally:
                        sys.settrace(prior_trace)
                    self.assertTrue(fired[0])
                    self.assertIs(caught, marker)
                    caught = None
                    marker = marker.with_traceback(None)
                    gc.collect()
                    dead_before_reissue = all(
                        reference() is None for reference in references
                    )
                    if sticky:
                        self.assertIs(
                            type(
                                self.storage._get_historical_window_run_quota_for_test(
                                    spool=spool
                                )
                            ),
                            self.storage._HistoricalWindowRunQuota,
                        )
                    else:
                        with self.assertRaises(
                            self.storage.HistoricalFoundryStorageError
                        ):
                            self.storage._get_historical_window_run_quota_for_test(
                                spool=spool
                            )
                    replacement_succeeded = False
                    try:
                        replacement = self._issue(spool)
                        replacement_pending = spool.append_transfer(
                            transfer=replacement
                        )
                        spool.abort_transfer(
                            transfer=replacement,
                            pending_receipt=replacement_pending,
                        )
                        replacement_succeeded = True
                    except self.storage.HistoricalFoundryStorageError:
                        pass
                    spool.close()
                    self.assertTrue(dead_before_reissue)
                    self.assertTrue(replacement_succeeded)
                    self.assertEqual(tuple(directory.iterdir()), ())

    def test_closure_records_defeat_clone_and_original_attribute_transplants(self):
        def opened(label):
            directory = self.data_dir / label
            directory.mkdir(mode=0o700)
            return self.storage._open_historical_window_exchange_spool(
                data_dir=directory
            )

        def attack(donor):
            with self.subTest(donor=type(donor).__name__):
                clone = object.__new__(type(donor))
                for target in (clone, donor):
                    for name, value in (
                        ("_state", "active"),
                        ("_lineage", object()),
                        ("_file_fd", 0),
                        ("_basename", "forged"),
                        ("_quota", object()),
                        ("_raw_bytes", b"forged"),
                    ):
                        with self.assertRaises((AttributeError, TypeError)):
                            object.__setattr__(target, name, value)
                with self.assertRaises(TypeError):
                    object.__setattr__(
                        clone,
                        "__class__",
                        self.storage._ProductionHistoricalWindowCapability,
                    )

        active = opened("active")
        attack(active)
        self.assertEqual(len(tuple((self.data_dir / "active").iterdir())), 1)
        active.close()
        self.assertEqual(tuple((self.data_dir / "active").iterdir()), ())

        issued_owner = opened("issued")
        issued = self._issue(issued_owner)
        attack(issued)
        pending = issued_owner.append_transfer(transfer=issued)
        issued_owner.abort_transfer(transfer=issued, pending_receipt=pending)
        issued_owner.close()

        pending_owner = opened("pending")
        pending_transfer = self._issue(pending_owner)
        pending_donor = pending_owner.append_transfer(transfer=pending_transfer)
        attack(pending_donor)
        pending_owner.abort_transfer(
            transfer=pending_transfer, pending_receipt=pending_donor
        )
        pending_owner.close()

        receipt_owner = opened("receipt")
        receipt_transfer = self._issue(receipt_owner)
        receipt_pending = receipt_owner.append_transfer(transfer=receipt_transfer)
        receipt = receipt_owner.commit_transfer(
            transfer=receipt_transfer, pending_receipt=receipt_pending
        )
        attack(receipt)
        self.assertEqual(receipt_owner.reread_exchange(receipt=receipt)[0], b'{"id":1}')
        receipt_owner.close()

        quota_owner = opened("quota")
        self._issue(quota_owner)
        quota = self.storage._get_historical_window_run_quota_for_test(
            spool=quota_owner
        )
        attack(quota)
        quota._reserve_for_test(physical_bytes=7, members=2)
        quota._abort_reservation_for_test()
        quota_owner.close()

        sealed_owner = opened("sealed")
        sealed_transfer = self._issue(sealed_owner)
        sealed_pending = sealed_owner.append_transfer(transfer=sealed_transfer)
        sealed_receipt = sealed_owner.commit_transfer(
            transfer=sealed_transfer, pending_receipt=sealed_pending
        )
        sealed = sealed_owner.seal()
        attack(sealed)
        self.assertEqual(sealed.reread_exchange(receipt=sealed_receipt)[0], b'{"id":1}')
        sealed.close()


class HistoricalFoundryStorageTask3bBindingTests(unittest.TestCase):
    def test_private_bind_slot_rejects_direct_call_before_reading_arguments(self):
        storage = importlib.import_module("scripts.historical_foundry_storage")

        class Hostile:
            def __getattribute__(self, _name):
                raise AssertionError("hostile argument inspected")

        temporary = tempfile.TemporaryDirectory(dir="/private/tmp")
        try:
            data_dir = Path(temporary.name)
            os.chmod(str(data_dir), 0o700)
            spool = storage._open_historical_window_exchange_spool(
                data_dir=data_dir
            )
            hostile = Hostile()
            with self.assertRaises(storage.HistoricalFoundryStorageError):
                spool._bind_claimed_source_authority_from_rpc(
                    claim=hostile,
                    bound_rpc_module=hostile,
                    bound_scan_module=hostile,
                    bound_storage_module=hostile,
                    source_capsule=hostile,
                )
            transfer = storage._issue_historical_window_exchange_transfer_for_test(
                spool=spool, **_valid_transfer_arguments()
            )
            pending = spool.append_transfer(transfer=transfer)
            spool.abort_transfer(transfer=transfer, pending_receipt=pending)
            spool.close()
        finally:
            temporary.cleanup()


class HistoricalFoundryStorageTask3bProductionTransferTests(unittest.TestCase):
    def setUp(self):
        self.storage = importlib.import_module(
            "scripts.historical_foundry_storage"
        )
        self.rpc = importlib.import_module("scripts.historical_foundry_rpc")
        importlib.import_module("scripts.historical_foundry_scan")
        self.temporary = tempfile.TemporaryDirectory(dir="/private/tmp")
        self.data_dir = Path(self.temporary.name)
        os.chmod(str(self.data_dir), 0o700)

    def tearDown(self):
        self.temporary.cleanup()

    def _bound(self):
        rpc_module = self.rpc
        sources = rpc_module._HeldArchiveSourceAuthority(
            Path(rpc_module.__file__).resolve().parents[1]
        )
        sources.open_members()

        class Preflight:
            def __init__(self):
                self.identity = rpc_module._test_preflight_identity()
                self.config = object()
                self.sources = sources
                self.closed = False

            def close(self):
                if not self.closed:
                    self.closed = True
                    self.sources.close()

        class Environment(dict):
            def get(self, key, default=None):
                if key != "DEX_DEPTH_RPC_ETH" or default is not None:
                    raise AssertionError("unexpected environment read")
                return "https://rpc.example.invalid/archive"

        class Opener:
            addheaders = []

            def open(self, *_args, **_kwargs):
                raise AssertionError("transport entered")

        preflight = Preflight()
        with mock.patch.object(
            rpc_module, "_perform_production_preflight", return_value=preflight
        ), mock.patch.object(
            rpc_module.time, "monotonic", return_value=10.0
        ), mock.patch.object(
            rpc_module.os, "urandom", return_value=b"z" * 32
        ), mock.patch.object(
            rpc_module.os, "environ", Environment()
        ), mock.patch.object(
            rpc_module.urllib.request, "build_opener", return_value=Opener()
        ):
            context = rpc_module._open_production_archive_rpc_run()
        claim = rpc_module._claim_fresh_production_archive_rpc_run_for_historical_window(
            context=context
        )
        spool = self.storage._open_historical_window_exchange_spool(
            data_dir=self.data_dir
        )
        rpc_module._bind_claimed_historical_window_sources_to_spool(
            claim=claim, spool=spool
        )
        return claim, spool

    def test_production_transfer_retains_authority_until_verified_release(self):
        claim, spool = self._bound()
        arguments = _valid_transfer_arguments()
        transfer = spool.issue_transfer_from_bound_rpc(
            claim=claim, **arguments
        )
        self.assertIs(
            type(transfer), self.storage._ProductionArchiveRpcExchangeTransfer
        )
        pending = spool.append_transfer(transfer=transfer)
        self.assertIsNone(spool.verify_pending_receipt(
            transfer=transfer, pending_receipt=pending
        ))
        receipt = spool.commit_transfer(
            transfer=transfer, pending_receipt=pending
        )
        self.assertIsNone(spool.verify_committed_receipt(
            transfer=transfer, receipt=receipt
        ))
        self.assertEqual(
            spool.reread_exchange(receipt=receipt),
            (
                arguments["canonical_request_bytes"],
                arguments["decoded_response_bytes"],
            ),
        )
        self.assertIsNone(spool.release_verified_transfer(
            transfer=transfer, receipt=receipt
        ))
        with self.assertRaises(self.storage.HistoricalFoundryStorageError):
            spool.release_verified_transfer(
                transfer=transfer, receipt=receipt
            )
        replacement = spool.issue_transfer_from_bound_rpc(
            claim=claim,
            **_valid_transfer_arguments(
                exchange_index=2,
                request_ids=(2,),
                response_ids=(2,),
            )
        )
        replacement_pending = spool.append_transfer(transfer=replacement)
        spool.abort_transfer(
            transfer=replacement, pending_receipt=replacement_pending
        )
        claim.close()
        spool.close()

    def test_production_transfer_internal_delivery_control_terminalizes_owner(self):
        stages = (
            "issue_transfer_from_bound_rpc",
            "verify_pending_receipt",
            "commit_transfer",
            "verify_committed_receipt",
            "release_verified_transfer",
        )
        internal_names = {
            "issue_transfer_from_bound_rpc": "_issue_transfer_from_bound_rpc",
            "verify_pending_receipt": "_verify_pending_production_receipt",
            "commit_transfer": "_commit_transfer",
            "verify_committed_receipt": "_verify_committed_production_receipt",
            "release_verified_transfer": "_release_verified_production_transfer",
        }
        arguments = _valid_transfer_arguments()

        for stage in stages:
            with self.subTest(stage=stage):
                claim, spool = self._bound()
                transfer = None
                pending = None
                receipt = None
                if stage != "issue_transfer_from_bound_rpc":
                    transfer = spool.issue_transfer_from_bound_rpc(
                        claim=claim, **arguments
                    )
                    pending = spool.append_transfer(transfer=transfer)
                if stage in (
                    "commit_transfer", "verify_committed_receipt",
                    "release_verified_transfer",
                ):
                    spool.verify_pending_receipt(
                        transfer=transfer, pending_receipt=pending
                    )
                if stage in (
                    "verify_committed_receipt", "release_verified_transfer",
                ):
                    receipt = spool.commit_transfer(
                        transfer=transfer, pending_receipt=pending
                    )
                if stage == "release_verified_transfer":
                    spool.verify_committed_receipt(
                        transfer=transfer, receipt=receipt
                    )

                method = getattr(
                    self.storage._HistoricalWindowExchangeSpool, stage
                )
                internal = next(
                    cell.cell_contents
                    for cell in (method.__closure__ or ())
                    if callable(cell.cell_contents)
                    and getattr(cell.cell_contents, "__name__", "")
                    == internal_names[stage]
                )
                cancellation = GeneratorExit(
                    "production-transfer-delivery-{}".format(stage)
                )
                prior_trace = sys.gettrace()

                def tracer(frame, event, _arg):
                    if frame.f_code is internal.__code__ and event == "return":
                        sys.settrace(prior_trace)
                        raise cancellation
                    return tracer

                try:
                    sys.settrace(tracer)
                    with self.assertRaises(GeneratorExit) as caught:
                        if stage == "issue_transfer_from_bound_rpc":
                            spool.issue_transfer_from_bound_rpc(
                                claim=claim, **arguments
                            )
                        elif stage == "verify_pending_receipt":
                            spool.verify_pending_receipt(
                                transfer=transfer, pending_receipt=pending
                            )
                        elif stage == "commit_transfer":
                            spool.commit_transfer(
                                transfer=transfer, pending_receipt=pending
                            )
                        elif stage == "verify_committed_receipt":
                            spool.verify_committed_receipt(
                                transfer=transfer, receipt=receipt
                            )
                        else:
                            spool.release_verified_transfer(
                                transfer=transfer, receipt=receipt
                            )
                finally:
                    sys.settrace(prior_trace)
                self.assertIs(caught.exception, cancellation)
                observed_files = tuple(self.data_dir.iterdir())
                self.assertIsNone(spool.close())
                claim.close()
                self.assertEqual(observed_files, ())
                self.assertEqual(tuple(self.data_dir.iterdir()), ())

    def test_bound_module_generation_drift_raises_exact_rpc_error_before_issue(self):
        claim, spool = self._bound()
        generation = self.rpc._HISTORICAL_WINDOW_MODULE_GENERATION
        try:
            self.rpc._HISTORICAL_WINDOW_MODULE_GENERATION = object()
            with self.assertRaises(self.rpc._ArchiveRpcError) as caught:
                spool.issue_transfer_from_bound_rpc(
                    claim=claim, **_valid_transfer_arguments()
                )
            self.assertEqual(
                (caught.exception.reason_code, caught.exception.failure_kind),
                ("authority_mismatch", "final_identity_drift"),
            )
        finally:
            self.rpc._HISTORICAL_WINDOW_MODULE_GENERATION = generation
            claim.close()
            spool.close()

    def test_bound_currentness_ordinary_helper_fault_stays_storage_error(self):
        for error in (
            RuntimeError("ordinary currentness runtime fault"),
            ValueError("ordinary currentness value fault"),
            AttributeError("ordinary currentness attribute fault"),
        ):
            with self.subTest(error=type(error).__name__):
                claim, spool = self._bound()
                try:
                    with mock.patch.object(
                        self.storage.os, "pread", side_effect=error
                    ):
                        with self.assertRaises(
                            self.storage.HistoricalFoundryStorageError
                        ) as caught:
                            spool.issue_transfer_from_bound_rpc(
                                claim=claim, **_valid_transfer_arguments()
                            )
                    self.assertNotIsInstance(
                        caught.exception, self.rpc._ArchiveRpcError
                    )
                finally:
                    claim.close()
                    spool.close()


class HistoricalFoundryStorageTask3bSealBindingTests(
    HistoricalFoundryStorageTask3bProductionTransferTests
):
    def test_bound_spool_cannot_seal_before_claimed_prefinalization(self):
        claim, spool = self._bound()
        with self.assertRaises(self.storage.HistoricalFoundryStorageError):
            spool.seal()
        claim.close()
        spool.close()


class HistoricalFoundryStorageTask3bReconciliationCursorTests(unittest.TestCase):
    def setUp(self):
        self.storage = importlib.import_module(
            "scripts.historical_foundry_storage"
        )
        self.rpc = importlib.import_module("scripts.historical_foundry_rpc")
        self.scan = importlib.import_module("scripts.historical_foundry_scan")
        self.temporary = tempfile.TemporaryDirectory(dir="/private/tmp")
        self.data_dir = Path(self.temporary.name)
        os.chmod(str(self.data_dir), 0o700)

    def tearDown(self):
        self.temporary.cleanup()

    def test_cursor_streams_exact_receipt_and_requires_eof_normal_exit(self):
        from tests.test_historical_foundry_scan import (
            HistoricalFoundryScanTask3bIntegratedTests,
        )

        case = HistoricalFoundryScanTask3bIntegratedTests(
            methodName=(
                "test_scheduler_owns_complete_offline_run_through_capability_delivery"
            )
        )
        case.test_scheduler_owns_complete_offline_run_through_capability_delivery()


class HistoricalFoundryStorageTask3bCursorDeliveryTests(unittest.TestCase):
    def _case(self):
        from tests.test_historical_foundry_scan import (
            HistoricalFoundryScanTask3bIntegratedTests,
        )

        return HistoricalFoundryScanTask3bIntegratedTests(
            methodName=(
                "test_scheduler_owns_complete_offline_run_through_capability_delivery"
            )
        )

    def test_cursor_open_delivery_failure_leaves_only_weak_tombstone(self):
        storage = importlib.import_module("scripts.historical_foundry_storage")
        open_core = next(
            cell.cell_contents
            for cell in (
                storage._SealedHistoricalWindowExchangeSpool
                ._open_reconciliation_cursor_from_bound_scan.__closure__ or ()
            )
            if callable(cell.cell_contents)
            and getattr(cell.cell_contents, "__name__", "")
            == "_open_reconciliation_cursor_core"
        )
        cursor_registry = dict(zip(
            storage._HistoricalWindowSpoolReconciliationCursor
            .close.__code__.co_freevars,
            storage._HistoricalWindowSpoolReconciliationCursor
            .close.__closure__ or (),
        ))["cursor_registry"].cell_contents
        baseline = set(cursor_registry)
        cancellation = GeneratorExit("cursor-open-weak-terminal")
        cursor_reference = [None]
        cursor_id = [None]
        prior_trace = sys.gettrace()

        def tracer(frame, event, argument):
            if (
                cursor_reference[0] is None
                and frame.f_code is open_core.__code__
                and event == "return"
            ):
                cursor_reference[0] = weakref.ref(argument)
                cursor_id[0] = id(argument)
                sys.settrace(prior_trace)
                raise cancellation
            return tracer

        caught = None
        try:
            sys.settrace(tracer)
            try:
                self._case().test_scheduler_owns_complete_offline_run_through_capability_delivery()
            except GeneratorExit as error:
                caught = error
        finally:
            sys.settrace(prior_trace)
        self.assertIs(caught, cancellation)
        self.assertIsNotNone(cursor_reference[0])
        cancellation.__traceback__ = None
        caught = None
        gc.collect()
        try:
            self.assertIsNone(cursor_reference[0]())
            self.assertEqual(set(cursor_registry), baseline)
        finally:
            if cursor_id[0] is not None:
                cursor_registry.pop(cursor_id[0], None)
            gc.collect()

    def test_cursor_read_failure_leaves_only_weak_tombstone(self):
        storage = importlib.import_module("scripts.historical_foundry_storage")
        rpc = importlib.import_module("scripts.historical_foundry_rpc")
        scan = importlib.import_module("scripts.historical_foundry_scan")
        cursor_registry = dict(zip(
            storage._HistoricalWindowSpoolReconciliationCursor
            .close.__code__.co_freevars,
            storage._HistoricalWindowSpoolReconciliationCursor
            .close.__closure__ or (),
        ))["cursor_registry"].cell_contents
        baseline = set(cursor_registry)
        lines, start = inspect.getsourcelines(
            scan._reconcile_production_historical_window
        )
        target = start + next(
            index for index, line in enumerate(lines)
            if "with cursor as stream:" in line
        )
        cursor_reference = [None]
        cursor_id = [None]
        read_patch = [None]
        prior_trace = sys.gettrace()

        def tracer(frame, event, _argument):
            if (
                cursor_reference[0] is None
                and frame.f_code.co_filename == scan.__file__
                and frame.f_code.co_name == "replay_all"
                and event == "line"
                and frame.f_lineno == target
            ):
                cursor = frame.f_locals["cursor"]
                cursor_reference[0] = weakref.ref(cursor)
                cursor_id[0] = id(cursor)
                read_patch[0] = mock.patch.object(
                    storage.os,
                    "pread",
                    side_effect=OSError("forced cursor read failure"),
                )
                read_patch[0].start()
                sys.settrace(prior_trace)
            return tracer

        caught = None
        try:
            sys.settrace(tracer)
            try:
                self._case().test_scheduler_owns_complete_offline_run_through_capability_delivery()
            except rpc._ArchiveRpcError as error:
                caught = error
        finally:
            sys.settrace(prior_trace)
            if read_patch[0] is not None:
                read_patch[0].stop()
                read_patch[0] = None
        self.assertIsNotNone(caught)
        self.assertEqual(
            (caught.reason_code, caught.failure_kind),
            (
                "authority_mismatch",
                "historical_window_reconciliation_mismatch",
            ),
        )
        self.assertIsNotNone(cursor_reference[0])
        caught = None
        gc.collect()
        try:
            self.assertIsNone(cursor_reference[0]())
            self.assertEqual(set(cursor_registry), baseline)
        finally:
            if cursor_id[0] is not None:
                cursor_registry.pop(cursor_id[0], None)
            gc.collect()

    def test_cursor_open_assignment_control_retires_cursor_authority(self):
        storage = importlib.import_module("scripts.historical_foundry_storage")
        scan = importlib.import_module("scripts.historical_foundry_scan")
        lines, start = inspect.getsourcelines(
            scan._reconcile_production_historical_window
        )
        target = start + next(
            index for index, line in enumerate(lines)
            if "with cursor as stream:" in line
        )
        cancellation = GeneratorExit("cursor-open-assignment-control")
        captured = [None]
        prior_trace = sys.gettrace()

        def tracer(frame, event, _argument):
            if (
                frame.f_code.co_filename == scan.__file__
                and frame.f_code.co_name == "replay_all"
                and event == "line"
                and frame.f_lineno == target
            ):
                captured[0] = frame.f_locals["cursor"]
                sys.settrace(prior_trace)
                raise cancellation
            return tracer

        try:
            sys.settrace(tracer)
            with self.assertRaises(GeneratorExit) as caught:
                self._case().test_scheduler_owns_complete_offline_run_through_capability_delivery()
        finally:
            sys.settrace(prior_trace)
        self.assertIs(caught.exception, cancellation)
        self.assertIsNotNone(captured[0])
        try:
            with self.assertRaises(storage.HistoricalFoundryStorageError):
                captured[0].__enter__()
        finally:
            try:
                captured[0].close()
            except storage.HistoricalFoundryStorageError:
                pass

    def test_cursor_open_internal_return_is_guarded_before_scan_cleanup(self):
        storage = importlib.import_module("scripts.historical_foundry_storage")
        scan = importlib.import_module("scripts.historical_foundry_scan")
        cancellation = GeneratorExit("cursor-open-internal-return-control")
        captured = [None]
        observed_live_before_scan_cleanup = [None]
        prior_trace = sys.gettrace()
        prior_profile = sys.getprofile()

        def tracer(frame, event, argument):
            if (
                frame.f_code.co_filename == storage.__file__
                and frame.f_code.co_name in (
                    "_open_reconciliation_cursor_from_bound_scan",
                    "_open_reconciliation_cursor_core",
                )
                and event == "return"
            ):
                captured[0] = argument
                raise cancellation
            return tracer

        def profiler(frame, event, _argument):
            if (
                captured[0] is not None
                and observed_live_before_scan_cleanup[0] is None
                and frame.f_code
                is scan._reconcile_production_historical_window.__code__
                and event == "return"
            ):
                try:
                    captured[0].__enter__()
                except storage.HistoricalFoundryStorageError:
                    observed_live_before_scan_cleanup[0] = False
                else:
                    observed_live_before_scan_cleanup[0] = True

        try:
            sys.setprofile(profiler)
            sys.settrace(tracer)
            with self.assertRaises(GeneratorExit) as caught:
                self._case().test_scheduler_owns_complete_offline_run_through_capability_delivery()
        finally:
            sys.settrace(prior_trace)
            sys.setprofile(prior_profile)
        self.assertIs(caught.exception, cancellation)
        self.assertIsNotNone(captured[0])
        self.assertIs(observed_live_before_scan_cleanup[0], False)

    def test_cursor_row_return_control_terminalizes_before_next_use(self):
        storage = importlib.import_module("scripts.historical_foundry_storage")
        scan = importlib.import_module("scripts.historical_foundry_scan")
        lines, start = inspect.getsourcelines(
            scan._reconcile_production_historical_window
        )
        target = start + next(
            index for index, line in enumerate(lines)
            if "with cursor as stream:" in line
        )
        original_next = storage._HistoricalWindowSpoolReconciliationCursor.__next__
        cancellation = asyncio.CancelledError("cursor-row-return-control")
        rejected = [False]
        prior_trace = sys.gettrace()
        method_patch = [None]

        def wrapped_next(cursor):
            def interrupt(frame, event, _argument):
                if (
                    frame.f_code.co_filename == storage.__file__
                    and frame.f_code.co_name in (
                        "__next__", "_next_reconciliation_cursor_core"
                    )
                    and event == "return"
                ):
                    sys.settrace(prior_trace)
                    raise cancellation
                return interrupt

            try:
                sys.settrace(interrupt)
                original_next(cursor)
            except asyncio.CancelledError as caught:
                self.assertIs(caught, cancellation)
            finally:
                sys.settrace(prior_trace)
            try:
                original_next(cursor)
            except storage.HistoricalFoundryStorageError:
                rejected[0] = True
            raise cancellation

        def install(frame, event, _argument):
            if (
                frame.f_code.co_filename == scan.__file__
                and frame.f_code.co_name == "replay_all"
                and event == "line"
                and frame.f_lineno == target
            ):
                method_patch[0] = mock.patch.object(
                    storage._HistoricalWindowSpoolReconciliationCursor,
                    "__next__",
                    wrapped_next,
                )
                method_patch[0].start()
                sys.settrace(prior_trace)
            return install

        try:
            sys.settrace(install)
            with self.assertRaises(asyncio.CancelledError) as caught:
                self._case().test_scheduler_owns_complete_offline_run_through_capability_delivery()
        finally:
            sys.settrace(prior_trace)
            if method_patch[0] is not None:
                method_patch[0].stop()
        self.assertIs(caught.exception, cancellation)
        self.assertTrue(rejected[0])

    def test_cursor_exit_cleanup_control_overrides_ordinary_body_error(self):
        storage = importlib.import_module("scripts.historical_foundry_storage")
        scan = importlib.import_module("scripts.historical_foundry_scan")
        lines, start = inspect.getsourcelines(
            scan._reconcile_production_historical_window
        )
        target = start + next(
            index for index, line in enumerate(lines)
            if "with cursor as stream:" in line
        )
        cleanup_control = GeneratorExit("cursor-exit-cleanup-control")
        original_close = storage.os.close
        close_fired = [False]
        prior_trace = sys.gettrace()
        patchers = []

        def close_then_control(fd):
            original_close(fd)
            if not close_fired[0]:
                close_fired[0] = True
                raise cleanup_control

        def ordinary_next(_cursor):
            raise ValueError("cursor-body-ordinary")

        def install(frame, event, _argument):
            if (
                frame.f_code.co_filename == scan.__file__
                and frame.f_code.co_name == "replay_all"
                and event == "line"
                and frame.f_lineno == target
            ):
                patchers.extend((
                    mock.patch.object(
                        storage._HistoricalWindowSpoolReconciliationCursor,
                        "__next__",
                        ordinary_next,
                    ),
                    mock.patch.object(storage.os, "close", close_then_control),
                ))
                for patcher in patchers:
                    patcher.start()
                sys.settrace(prior_trace)
            return install

        try:
            sys.settrace(install)
            with self.assertRaises(GeneratorExit) as caught:
                self._case().test_scheduler_owns_complete_offline_run_through_capability_delivery()
        finally:
            sys.settrace(prior_trace)
            for patcher in reversed(patchers):
                patcher.stop()
        self.assertTrue(close_fired[0])
        self.assertIs(caught.exception, cleanup_control)


class HistoricalFoundryStorageTask3bCapabilityTests(
    HistoricalFoundryStorageTask3bReconciliationCursorTests
):
    def test_mint_and_consume_move_one_owner_then_view_close_revokes(self):
        self.test_cursor_streams_exact_receipt_and_requires_eof_normal_exit()

    def test_consume_rechecks_bound_source_generation_before_view_move(self):
        from tests.test_historical_foundry_scan import (
            HistoricalFoundryScanTask3bIntegratedTests,
        )

        storage = importlib.import_module("scripts.historical_foundry_storage")
        rpc = importlib.import_module("scripts.historical_foundry_rpc")
        case_method = (
            HistoricalFoundryScanTask3bIntegratedTests
            .test_scheduler_owns_complete_offline_run_through_capability_delivery
        )
        method_lines, method_start = inspect.getsourcelines(case_method)
        consume_line = method_start + next(
            index for index, line in enumerate(method_lines)
            if "view = storage.consume_production_historical_window_capability(" in line
        )
        setup_control = GeneratorExit("capture-capability-before-drift")
        capability = [None]
        prior_trace = sys.gettrace()

        def capture_capability(frame, event, _argument):
            if (
                frame.f_code is case_method.__code__
                and event == "line"
                and frame.f_lineno == consume_line
            ):
                capability[0] = frame.f_locals["capability"]
                sys.settrace(prior_trace)
                raise setup_control
            return capture_capability

        case = HistoricalFoundryScanTask3bIntegratedTests(
            methodName=case_method.__name__
        )
        try:
            sys.settrace(capture_capability)
            with self.assertRaises(GeneratorExit) as caught:
                case.test_scheduler_owns_complete_offline_run_through_capability_delivery()
        finally:
            sys.settrace(prior_trace)
        self.assertIs(caught.exception, setup_control)
        self.assertIsNotNone(capability[0])

        original_generation = rpc._HISTORICAL_WINDOW_MODULE_GENERATION
        view = None
        try:
            rpc._HISTORICAL_WINDOW_MODULE_GENERATION = object()
            with self.assertRaises(rpc._ArchiveRpcError) as caught:
                view = storage.consume_production_historical_window_capability(
                    capability=capability[0]
                )
        finally:
            rpc._HISTORICAL_WINDOW_MODULE_GENERATION = original_generation
            if view is not None:
                try:
                    view.close()
                except storage.HistoricalFoundryStorageError:
                    pass
        self.assertEqual(
            (caught.exception.reason_code, caught.exception.failure_kind),
            ("authority_mismatch", "final_identity_drift"),
        )
        self.assertIsNone(capability[0].close())

    def test_mint_return_control_does_not_strand_moved_capability_owner(self):
        from tests.test_historical_foundry_scan import (
            HistoricalFoundryScanTask3bIntegratedTests,
        )

        storage = importlib.import_module("scripts.historical_foundry_storage")
        cancellation = GeneratorExit("mint-owner-delivery-control")
        captured = [None]
        prior_trace = sys.gettrace()

        def tracer(frame, event, argument):
            if (
                frame.f_code.co_filename == storage.__file__
                and frame.f_code.co_name in (
                    "mint_production_historical_window_capability",
                    "_mint_production_historical_window_capability_core",
                )
                and event == "return"
            ):
                captured[0] = argument
                sys.settrace(prior_trace)
                raise cancellation
            return tracer

        case = HistoricalFoundryScanTask3bIntegratedTests(
            methodName=(
                "test_scheduler_owns_complete_offline_run_through_capability_delivery"
            )
        )
        try:
            sys.settrace(tracer)
            with self.assertRaises(GeneratorExit) as caught:
                case.test_scheduler_owns_complete_offline_run_through_capability_delivery()
        finally:
            sys.settrace(prior_trace)
        self.assertIs(caught.exception, cancellation)
        self.assertIsNotNone(captured[0])
        leaked_view = None
        try:
            leaked_view = storage.consume_production_historical_window_capability(
                capability=captured[0]
            )
        except storage.HistoricalFoundryStorageError:
            pass
        else:
            self.fail("mint return control left a consumable capability owner")
        finally:
            if leaked_view is not None:
                try:
                    leaked_view.close()
                except storage.HistoricalFoundryStorageError:
                    pass
            try:
                captured[0].close()
            except storage.HistoricalFoundryStorageError:
                pass

    def test_consume_return_control_does_not_strand_moved_view_owner(self):
        from tests.test_historical_foundry_scan import (
            HistoricalFoundryScanTask3bIntegratedTests,
        )

        storage = importlib.import_module("scripts.historical_foundry_storage")
        case_method = (
            HistoricalFoundryScanTask3bIntegratedTests
            .test_scheduler_owns_complete_offline_run_through_capability_delivery
        )
        method_lines, method_start = inspect.getsourcelines(case_method)
        consume_line = method_start + next(
            index for index, line in enumerate(method_lines)
            if "view = storage.consume_production_historical_window_capability(" in line
        )
        setup_control = GeneratorExit("capture-delivered-capability")
        capability = [None]
        prior_trace = sys.gettrace()

        def capture_capability(frame, event, _argument):
            if (
                frame.f_code is case_method.__code__
                and event == "line"
                and frame.f_lineno == consume_line
            ):
                capability[0] = frame.f_locals["capability"]
                sys.settrace(prior_trace)
                raise setup_control
            return capture_capability

        case = HistoricalFoundryScanTask3bIntegratedTests(
            methodName=case_method.__name__
        )
        try:
            sys.settrace(capture_capability)
            with self.assertRaises(GeneratorExit) as caught:
                case.test_scheduler_owns_complete_offline_run_through_capability_delivery()
        finally:
            sys.settrace(prior_trace)
        self.assertIs(caught.exception, setup_control)
        self.assertIsNotNone(capability[0])

        cancellation = asyncio.CancelledError("consume-owner-delivery-control")
        view = [None]

        def interrupt_consume(frame, event, argument):
            if (
                frame.f_code.co_filename == storage.__file__
                and frame.f_code.co_name in (
                    "consume_production_historical_window_capability",
                    "_consume_production_historical_window_capability_core",
                )
                and event == "return"
            ):
                view[0] = argument
                sys.settrace(prior_trace)
                raise cancellation
            return interrupt_consume

        try:
            sys.settrace(interrupt_consume)
            with self.assertRaises(asyncio.CancelledError) as caught:
                storage.consume_production_historical_window_capability(
                    capability=capability[0]
                )
        finally:
            sys.settrace(prior_trace)
        self.assertIs(caught.exception, cancellation)
        self.assertIsNotNone(view[0])
        entered = None
        try:
            entered = view[0].__enter__()
        except storage.HistoricalFoundryStorageError:
            pass
        else:
            self.fail("consume return control left an enterable view owner")
        finally:
            if entered is not None:
                try:
                    view[0].close()
                except storage.HistoricalFoundryStorageError:
                    pass
            try:
                capability[0].close()
            except storage.HistoricalFoundryStorageError:
                pass


class HistoricalFoundryStorageTask3bMovedOwnerTerminalizerTests(unittest.TestCase):
    def _delivered_capability(self, storage):
        from tests.test_historical_foundry_scan import (
            HistoricalFoundryScanTask3bIntegratedTests,
        )

        case_method = (
            HistoricalFoundryScanTask3bIntegratedTests
            .test_scheduler_owns_complete_offline_run_through_capability_delivery
        )
        method_lines, method_start = inspect.getsourcelines(case_method)
        consume_line = method_start + next(
            index for index, line in enumerate(method_lines)
            if "view = storage.consume_production_historical_window_capability(" in line
        )
        setup_control = GeneratorExit("capture-capability-for-close")
        captured = {"capability": None, "temporary": None, "data_dir": None}
        prior_trace = sys.gettrace()

        def tracer(frame, event, _argument):
            if (
                frame.f_code is case_method.__code__
                and event == "line"
                and frame.f_lineno == consume_line
            ):
                captured["capability"] = frame.f_locals["capability"]
                captured["temporary"] = frame.f_locals["temporary"]
                captured["data_dir"] = frame.f_locals["data_dir"]
                sys.settrace(prior_trace)
                raise setup_control
            return tracer

        original_cleanup = tempfile.TemporaryDirectory.cleanup
        case = HistoricalFoundryScanTask3bIntegratedTests(
            methodName=case_method.__name__
        )
        with mock.patch.object(
            tempfile.TemporaryDirectory,
            "cleanup",
            autospec=True,
            return_value=None,
        ):
            try:
                sys.settrace(tracer)
                with self.assertRaises(GeneratorExit) as caught:
                    case.test_scheduler_owns_complete_offline_run_through_capability_delivery()
            finally:
                sys.settrace(prior_trace)
        self.assertIs(caught.exception, setup_control)
        self.assertIsNotNone(captured["capability"])
        captured["original_cleanup"] = original_cleanup
        return captured

    def test_moved_owner_close_resumes_after_lineage_retirement(self):
        storage = importlib.import_module("scripts.historical_foundry_storage")
        public_close = storage._ProductionHistoricalWindowCapability.close
        internal = next(
            cell.cell_contents
            for cell in (public_close.__closure__ or ())
            if callable(cell.cell_contents)
            and getattr(cell.cell_contents, "__name__", "")
            == "_close_moved_owner"
        )
        cleanup_core = next(
            cell.cell_contents
            for cell in (internal.__closure__ or ())
            if callable(cell.cell_contents)
            and getattr(cell.cell_contents, "__name__", "")
            == "_cleanup_resources"
        )
        lines, start = inspect.getsourcelines(internal)
        targets = {
            "before_resource_cleanup": start + next(
                index for index, line in enumerate(lines)
                if "cleanup_control, cleanup_ordinary = _cleanup_resources(" in line
            ),
            "before_closed_publication": start + next(
                index for index, line in enumerate(lines)
                if "_retire_nonowner_handle(handle, registry, tombstones)" in line
            ),
        }
        for stage, target in targets.items():
            with self.subTest(stage=stage):
                captured = self._delivered_capability(storage)
                capability = captured["capability"]
                cancellation = asyncio.CancelledError(
                    "moved-owner-close-{}".format(stage)
                )
                owner = [None]
                prior_trace = sys.gettrace()

                def tracer(frame, event, _argument):
                    if (
                        frame.f_code is internal.__code__
                        and event == "line"
                        and frame.f_lineno == target
                    ):
                        owner[0] = frame.f_locals["owner"]
                        sys.settrace(prior_trace)
                        raise cancellation
                    return tracer

                try:
                    try:
                        sys.settrace(tracer)
                        with self.assertRaises(asyncio.CancelledError) as caught:
                            capability.close()
                    finally:
                        sys.settrace(prior_trace)
                    self.assertIs(caught.exception, cancellation)
                    self.assertIsNone(capability.close())
                    self.assertEqual(tuple(captured["data_dir"].iterdir()), ())
                finally:
                    if owner[0] is not None:
                        cleanup_core(owner[0], created=True)
                    captured["original_cleanup"](captured["temporary"])


class HistoricalFoundryStorageTask3bIntegratedTests(unittest.TestCase):
    def test_direct_capability_and_view_construction_remain_closed(self):
        storage = importlib.import_module("scripts.historical_foundry_storage")
        for authority in (
            storage._ProductionHistoricalWindowCapability,
            storage._ConsumedProductionHistoricalWindowCapabilityView,
        ):
            with self.assertRaises(storage.HistoricalFoundryStorageError):
                authority()


class HistoricalFoundryStorageQuotaTests(unittest.TestCase):
    BYTE_LIMIT = 8_589_934_592
    MEMBER_LIMIT = 200_000

    def setUp(self):
        self.storage = importlib.import_module("scripts.historical_foundry_storage")
        self.temporary = tempfile.TemporaryDirectory(dir="/private/tmp")
        self.data_dir = Path(self.temporary.name)
        os.chmod(str(self.data_dir), 0o700)

    def tearDown(self):
        self.temporary.cleanup()

    def _claimed(self):
        spool = self.storage._open_historical_window_exchange_spool(
            data_dir=self.data_dir
        )
        transfer = self.storage._issue_historical_window_exchange_transfer_for_test(
            spool=spool, **_valid_transfer_arguments()
        )
        quota = self.storage._get_historical_window_run_quota_for_test(spool=spool)
        return spool, transfer, quota

    def _projection(self, spool):
        return self.storage._project_historical_window_exchange_spool_for_test(
            spool_or_sealed=spool
        )

    def test_first_quota_reserve_isolates_fresh_lineage_and_retires_claim_transfer(self):
        spool, transfer, quota = self._claimed()
        self.assertEqual(self._projection(spool)["state"], "active")
        transfer_reference = weakref.ref(transfer)
        quota._reserve_for_test(physical_bytes=17, members=2)
        self.assertEqual(
            tuple(self._projection(spool).values()),
            ("quota_test_only", 0, 0, 17, 2, 0, 0, None),
        )
        with self.assertRaises(self.storage.HistoricalFoundryStorageError):
            self.storage._get_historical_window_run_quota_for_test(spool=spool)
        del transfer
        gc.collect()
        self.assertIsNone(transfer_reference())
        spool.close()

    def test_quota_test_only_rejects_normal_apis_and_never_exits_after_abort(self):
        spool, _transfer, quota = self._claimed()
        quota._reserve_for_test(physical_bytes=5, members=1)

        class Hostile:
            def __getattribute__(self, _name):
                raise AssertionError("hostile argument inspected")

        hostile = Hostile()
        normal_calls = (
            lambda: self.storage._issue_historical_window_exchange_transfer_for_test(
                spool=spool,
                exchange_projection=hostile,
                canonical_request_bytes=hostile,
                decoded_response_bytes=hostile,
            ),
            lambda: spool.append_transfer(transfer=hostile),
            lambda: spool.commit_transfer(transfer=hostile, pending_receipt=hostile),
            lambda: spool.abort_transfer(transfer=hostile, pending_receipt=hostile),
            lambda: spool.reread_exchange(receipt=hostile),
            lambda: spool.seal(),
        )
        for call in normal_calls:
            with self.assertRaises(self.storage.HistoricalFoundryStorageError):
                call()
        quota._abort_reservation_for_test()
        self.assertEqual(self._projection(spool)["state"], "quota_test_only")
        self.assertEqual(
            (
                self._projection(spool)["provisional_physical_bytes"],
                self._projection(spool)["provisional_members"],
            ),
            (0, 0),
        )
        with self.assertRaises(self.storage.HistoricalFoundryStorageError):
            spool.seal()
        self.assertFalse(
            hasattr(self.storage, "_bind_claimed_historical_window_sources_to_spool")
        )
        spool.close()

    def test_quota_byte_and_member_limits_are_inclusive(self):
        for physical_bytes, members, field, expected in (
            (self.BYTE_LIMIT, 1, "provisional_physical_bytes", self.BYTE_LIMIT),
            (1, self.MEMBER_LIMIT, "provisional_members", self.MEMBER_LIMIT),
        ):
            spool, _transfer, quota = self._claimed()
            quota._reserve_for_test(physical_bytes=physical_bytes, members=members)
            self.assertEqual(self._projection(spool)[field], expected)
            spool.close()

        for physical_bytes, members in (
            (self.BYTE_LIMIT + 1, 1),
            (1, self.MEMBER_LIMIT + 1),
            (1 << 1_000_000, 1),
        ):
            spool, transfer, quota = self._claimed()
            transfer_reference = weakref.ref(transfer)
            with self.assertRaises(self.storage.HistoricalFoundryStorageError):
                quota._reserve_for_test(physical_bytes=physical_bytes, members=members)
            self.assertEqual(
                tuple(self._projection(spool).values()),
                ("closed", 0, 0, 0, 0, 0, 0, None),
            )
            del transfer
            gc.collect()
            self.assertIsNone(transfer_reference())
            self.assertIsNone(spool.close())

    def test_quota_reservation_reclassifies_once_without_second_debit(self):
        spool, _transfer, quota = self._claimed()
        for physical_bytes, members in (
            (True, 1),
            (1, False),
            (0, 1),
            (1, 0),
            (1.0, 1),
        ):
            with self.subTest(units=(physical_bytes, members)):
                with self.assertRaises(self.storage.HistoricalFoundryStorageError):
                    quota._reserve_for_test(
                        physical_bytes=physical_bytes, members=members
                    )
                self.assertEqual(self._projection(spool)["state"], "active")

        quota._reserve_for_test(physical_bytes=19, members=3)
        self.assertEqual(
            tuple(self._projection(spool).values()),
            ("quota_test_only", 0, 0, 19, 3, 0, 0, None),
        )
        quota._commit_reservation_for_test()
        self.assertEqual(
            tuple(self._projection(spool).values()),
            ("quota_test_only", 19, 3, 0, 0, 0, 0, None),
        )
        with self.assertRaises(self.storage.HistoricalFoundryStorageError):
            quota._commit_reservation_for_test()
        self.assertEqual(
            (
                self._projection(spool)["committed_physical_bytes"],
                self._projection(spool)["committed_members"],
            ),
            (19, 3),
        )
        spool.close()

    def test_quota_test_reservations_charge_exact_units_and_duplicates_twice(self):
        spool, _transfer, quota = self._claimed()
        for _ in range(2):
            quota._reserve_for_test(physical_bytes=23, members=4)
            quota._commit_reservation_for_test()
        self.assertEqual(
            tuple(self._projection(spool).values()),
            ("quota_test_only", 46, 8, 0, 0, 0, 0, None),
        )
        with self.assertRaises(self.storage.HistoricalFoundryStorageError):
            quota._abort_reservation_for_test()
        self.assertEqual(self._projection(spool)["committed_members"], 8)
        spool.close()

    def test_review_round2_quota_transition_and_delivery_cancellation_are_coherent(self):
        operations = (
            (
                "reserve",
                "_reserve_quota_for_test",
                "_reserve_for_test",
                "_install_quota_reserve_transition",
            ),
            (
                "commit",
                "_commit_quota_reservation_for_test",
                "_commit_reservation_for_test",
                "_install_quota_commit_transition",
            ),
            (
                "abort",
                "_abort_quota_reservation_for_test",
                "_abort_reservation_for_test",
                "_install_quota_abort_transition",
            ),
        )
        for operation, internal_name, public_name, helper_name in operations:
            helper_points = []
            internal_returns = []
            public_points = []
            internal_returned = [False]

            def discover(frame, event, _argument):
                if frame.f_code.co_name == helper_name and event in ("line", "return"):
                    point = (event, frame.f_lineno)
                    if point not in helper_points:
                        helper_points.append(point)
                if frame.f_code.co_name == internal_name and event == "return":
                    point = (event, frame.f_lineno)
                    if point not in internal_returns:
                        internal_returns.append(point)
                    internal_returned[0] = True
                elif (
                    internal_returned[0]
                    and frame.f_code.co_name == public_name
                    and event == "line"
                ):
                    point = (event, frame.f_lineno)
                    if point not in public_points:
                        public_points.append(point)
                return discover

            probe_directory = self.data_dir / (operation + "-quota-probe")
            probe_directory.mkdir(mode=0o700)
            probe = self.storage._open_historical_window_exchange_spool(
                data_dir=probe_directory
            )
            self.storage._issue_historical_window_exchange_transfer_for_test(
                spool=probe, **_valid_transfer_arguments()
            )
            probe_quota = self.storage._get_historical_window_run_quota_for_test(
                spool=probe
            )
            if operation != "reserve":
                probe_quota._reserve_for_test(physical_bytes=7, members=2)
            prior_trace = sys.gettrace()
            try:
                sys.settrace(discover)
                if operation == "reserve":
                    probe_quota._reserve_for_test(physical_bytes=7, members=2)
                elif operation == "commit":
                    probe_quota._commit_reservation_for_test()
                else:
                    probe_quota._abort_reservation_for_test()
            finally:
                sys.settrace(prior_trace)
            probe.close()

            targets = tuple(("helper", point) for point in helper_points) + tuple(
                ("internal_return", point) for point in internal_returns
            ) + tuple(("public_delivery", point) for point in public_points)
            for index, (scope, point) in enumerate(targets):
                with self.subTest(
                    operation=operation, scope=scope, trace_point=point
                ):
                    directory = self.data_dir / "quota-{}-{}".format(
                        operation, index
                    )
                    directory.mkdir(mode=0o700)
                    spool = self.storage._open_historical_window_exchange_spool(
                        data_dir=directory
                    )
                    self.storage._issue_historical_window_exchange_transfer_for_test(
                        spool=spool, **_valid_transfer_arguments()
                    )
                    quota = self.storage._get_historical_window_run_quota_for_test(
                        spool=spool
                    )
                    if operation != "reserve":
                        quota._reserve_for_test(physical_bytes=7, members=2)
                    marker = asyncio.CancelledError(
                        "quota {} {}".format(operation, point)
                    )
                    fired = [False]
                    returned = [False]

                    def tracer(frame, event, _argument):
                        if frame.f_code.co_name == internal_name and event == "return":
                            returned[0] = True
                        helper_match = (
                            scope == "helper"
                            and frame.f_code.co_name == helper_name
                            and (event, frame.f_lineno) == point
                        )
                        internal_match = (
                            scope == "internal_return"
                            and frame.f_code.co_name == internal_name
                            and (event, frame.f_lineno) == point
                        )
                        public_match = (
                            scope == "public_delivery"
                            and returned[0]
                            and frame.f_code.co_name == public_name
                            and (event, frame.f_lineno) == point
                        )
                        if not fired[0] and (
                            helper_match or internal_match or public_match
                        ):
                            fired[0] = True
                            raise marker
                        return tracer

                    caught = None
                    prior_trace = sys.gettrace()
                    try:
                        sys.settrace(tracer)
                        if operation == "reserve":
                            quota._reserve_for_test(physical_bytes=7, members=2)
                        elif operation == "commit":
                            quota._commit_reservation_for_test()
                        else:
                            quota._abort_reservation_for_test()
                    except BaseException as error:
                        caught = error
                    finally:
                        sys.settrace(prior_trace)
                    self.assertTrue(fired[0])
                    self.assertIs(caught, marker)
                    projection = tuple(self._projection(spool).values())
                    if operation == "reserve":
                        expected = (
                            ("closed", 0, 0, 0, 0, 0, 0, None),
                            ("closed", 0, 0, 7, 2, 0, 0, None),
                        )
                    elif operation == "commit":
                        expected = (
                            ("closed", 0, 0, 7, 2, 0, 0, None),
                            ("closed", 7, 2, 0, 0, 0, 0, None),
                        )
                    else:
                        expected = (
                            ("closed", 0, 0, 7, 2, 0, 0, None),
                            ("closed", 0, 0, 0, 0, 0, 0, None),
                        )
                    self.assertIn(projection, expected)
                    self.assertEqual(tuple(directory.iterdir()), ())
                    self.assertIsNone(spool.close())
                    marker = marker.with_traceback(None)
            self.assertGreater(len(internal_returns), 0)
            self.assertGreater(len(public_points), 0)


class HistoricalFoundryStorageStateTests(unittest.TestCase):
    RECEIPT_KEYS = (
        "schema",
        "exchange_index",
        "logical_batch_index",
        "attempt_index",
        "request_byte_count",
        "request_sha256",
        "request_ids",
        "wire_byte_count",
        "wire_sha256",
        "decoded_byte_count",
        "decoded_sha256",
        "response_ids",
        "spool_member_index",
        "spool_offset",
        "spool_length",
        "spool_member_sha256",
    )

    def setUp(self):
        self.storage = importlib.import_module("scripts.historical_foundry_storage")
        self.temporary = tempfile.TemporaryDirectory(dir="/private/tmp")
        self.data_dir = Path(self.temporary.name)
        os.chmod(str(self.data_dir), 0o700)

    def tearDown(self):
        self.temporary.cleanup()

    def _open(self):
        return self.storage._open_historical_window_exchange_spool(
            data_dir=self.data_dir
        )

    def _member_path(self):
        members = tuple(self.data_dir.iterdir())
        self.assertEqual(len(members), 1)
        return members[0]

    def _issue(self, spool, **overrides):
        return self.storage._issue_historical_window_exchange_transfer_for_test(
            spool=spool, **_valid_transfer_arguments(**overrides)
        )

    def _append(self, spool, **overrides):
        transfer = self._issue(spool, **overrides)
        pending = spool.append_transfer(transfer=transfer)
        return transfer, pending

    def _commit(self, spool, **overrides):
        transfer, pending = self._append(spool, **overrides)
        receipt = spool.commit_transfer(
            transfer=transfer, pending_receipt=pending
        )
        return transfer, pending, receipt

    def test_append_writes_the_exact_uint64_frame_and_returns_pending(self):
        spool = self._open()
        arguments = _valid_transfer_arguments()
        transfer = self.storage._issue_historical_window_exchange_transfer_for_test(
            spool=spool, **arguments
        )
        pending = spool.append_transfer(transfer=transfer)
        expected_frame = (
            len(arguments["canonical_request_bytes"]).to_bytes(8, "big")
            + arguments["canonical_request_bytes"]
            + len(arguments["decoded_response_bytes"]).to_bytes(8, "big")
            + arguments["decoded_response_bytes"]
        )
        self.assertEqual(self._member_path().read_bytes(), expected_frame)
        self.assertIs(type(pending), self.storage._PendingHistoricalWindowSpoolReceipt)
        projection = self.storage._project_historical_window_exchange_spool_for_test(
            spool_or_sealed=spool
        )
        self.assertEqual(
            tuple(projection.values()),
            ("active", 0, 0, len(expected_frame), 1, 0, 0, None),
        )
        spool.abort_transfer(transfer=transfer, pending_receipt=pending)
        spool.close()

    def test_pending_is_not_rereadable_or_committed_inventory(self):
        spool = self._open()
        transfer, pending = self._append(spool)
        before = dict(
            self.storage._project_historical_window_exchange_spool_for_test(
                spool_or_sealed=spool
            )
        )
        with self.assertRaises(self.storage.HistoricalFoundryStorageError):
            spool.reread_exchange(receipt=pending)
        after = dict(
            self.storage._project_historical_window_exchange_spool_for_test(
                spool_or_sealed=spool
            )
        )
        self.assertEqual(after, before)
        self.assertEqual(after["committed_receipt_count"], 0)
        spool.abort_transfer(transfer=transfer, pending_receipt=pending)
        spool.close()

    def test_commit_rereads_and_returns_exact_receipt(self):
        spool = self._open()
        arguments = _valid_transfer_arguments()
        transfer = self.storage._issue_historical_window_exchange_transfer_for_test(
            spool=spool, **arguments
        )
        pending = spool.append_transfer(transfer=transfer)
        real_pread = os.pread
        reads = []

        def recording_pread(fd, length, offset):
            reads.append((length, offset))
            return real_pread(fd, length, offset)

        with mock.patch.object(self.storage.os, "pread", side_effect=recording_pread):
            receipt = spool.commit_transfer(
                transfer=transfer, pending_receipt=pending
            )
        self.assertGreaterEqual(len(reads), 2)
        detached = dict(receipt)
        self.assertEqual(tuple(detached), self.RECEIPT_KEYS)
        self.assertEqual(detached["schema"], "historical_foundry_exchange_spool_receipt/v1")
        self.assertEqual(detached["spool_member_index"], 1)
        self.assertEqual(detached["spool_offset"], 0)
        self.assertEqual(detached["spool_length"], len(self._member_path().read_bytes()))
        self.assertEqual(
            detached["spool_member_sha256"],
            hashlib.sha256(self._member_path().read_bytes()).hexdigest(),
        )
        self.assertEqual(
            spool.reread_exchange(receipt=receipt),
            (
                arguments["canonical_request_bytes"],
                arguments["decoded_response_bytes"],
            ),
        )
        spool.close()

    def test_test_commit_self_finalizes_and_retires_raw_binding(self):
        spool = self._open()
        transfer, pending, receipt = self._commit(spool)
        transfer_reference = weakref.ref(transfer)
        pending_reference = weakref.ref(pending)
        del transfer
        del pending
        gc.collect()
        self.assertIsNone(transfer_reference())
        self.assertIsNone(pending_reference())
        self.assertEqual(dict(receipt)["exchange_index"], 1)
        spool.close()

    def test_two_commits_have_contiguous_exchange_member_indices_and_offsets(self):
        spool = self._open()
        _transfer1, _pending1, first = self._commit(spool)
        _transfer2, _pending2, second = self._commit(
            spool,
            exchange_index=2,
            logical_batch_index=9,
            attempt_index=4,
            request_ids=(7, 8),
            response_ids=(8, 7),
            request_bytes=b"second-request",
            decoded_bytes=b"second-response",
        )
        first_row = dict(first)
        second_row = dict(second)
        self.assertEqual(
            (first_row["exchange_index"], second_row["exchange_index"]),
            (1, 2),
        )
        self.assertEqual(
            (first_row["spool_member_index"], second_row["spool_member_index"]),
            (1, 2),
        )
        self.assertEqual(
            second_row["spool_offset"],
            first_row["spool_offset"] + first_row["spool_length"],
        )
        self.assertEqual(
            (second_row["logical_batch_index"], second_row["attempt_index"]),
            (9, 4),
        )
        spool.close()

    def test_abort_durably_truncates_and_rolls_back_only_provisional_quota(self):
        spool = self._open()
        _transfer1, _pending1, first = self._commit(spool)
        committed_eof = dict(first)["spool_length"]
        transfer, pending = self._append(
            spool,
            exchange_index=2,
            request_ids=(2,),
            response_ids=(2,),
            request_bytes=b"aborted-request",
            decoded_bytes=b"aborted-response",
        )
        self.assertGreater(self._member_path().stat().st_size, committed_eof)
        spool.abort_transfer(transfer=transfer, pending_receipt=pending)
        self.assertEqual(self._member_path().stat().st_size, committed_eof)
        projection = self.storage._project_historical_window_exchange_spool_for_test(
            spool_or_sealed=spool
        )
        self.assertEqual(
            (
                projection["committed_physical_bytes"],
                projection["committed_members"],
                projection["provisional_physical_bytes"],
                projection["provisional_members"],
            ),
            (committed_eof, 1, 0, 0),
        )
        _transfer2, _pending2, second = self._commit(
            spool,
            exchange_index=2,
            request_ids=(3,),
            response_ids=(3,),
            request_bytes=b"replacement-request",
            decoded_bytes=b"replacement-response",
        )
        self.assertEqual(dict(second)["spool_member_index"], 2)
        spool.close()

    def test_transfer_pending_and_receipt_transplants_reject_without_terminalizing(self):
        first = self._open()
        second_dir = self.data_dir / "second"
        second_dir.mkdir(mode=0o700)
        second = self.storage._open_historical_window_exchange_spool(data_dir=second_dir)
        first_transfer, first_pending = self._append(first)
        second_transfer = self.storage._issue_historical_window_exchange_transfer_for_test(
            spool=second, **_valid_transfer_arguments()
        )
        second_pending = second.append_transfer(transfer=second_transfer)
        for call in (
            lambda: first.commit_transfer(
                transfer=second_transfer, pending_receipt=first_pending
            ),
            lambda: first.abort_transfer(
                transfer=first_transfer, pending_receipt=second_pending
            ),
        ):
            with self.assertRaises(self.storage.HistoricalFoundryStorageError):
                call()
        first_receipt = first.commit_transfer(
            transfer=first_transfer, pending_receipt=first_pending
        )
        second_receipt = second.commit_transfer(
            transfer=second_transfer, pending_receipt=second_pending
        )
        with self.assertRaises(self.storage.HistoricalFoundryStorageError):
            first.reread_exchange(receipt=second_receipt)
        self.assertEqual(first.reread_exchange(receipt=first_receipt)[0], b'{"id":1}')
        first.close()
        second.close()

    def test_reuse_after_commit_or_abort_rejects_without_mutation(self):
        spool = self._open()
        transfer, pending, receipt = self._commit(spool)
        before = dict(
            self.storage._project_historical_window_exchange_spool_for_test(
                spool_or_sealed=spool
            )
        )
        for call in (
            lambda: spool.commit_transfer(transfer=transfer, pending_receipt=pending),
            lambda: spool.abort_transfer(transfer=transfer, pending_receipt=pending),
            lambda: spool.append_transfer(transfer=transfer),
        ):
            with self.assertRaises(self.storage.HistoricalFoundryStorageError):
                call()
        self.assertEqual(
            dict(
                self.storage._project_historical_window_exchange_spool_for_test(
                    spool_or_sealed=spool
                )
            ),
            before,
        )
        self.assertEqual(dict(receipt)["spool_member_index"], 1)

        aborted_transfer, aborted_pending = self._append(
            spool,
            exchange_index=2,
            request_ids=(2,),
            response_ids=(2,),
        )
        spool.abort_transfer(
            transfer=aborted_transfer, pending_receipt=aborted_pending
        )
        for call in (
            lambda: spool.abort_transfer(
                transfer=aborted_transfer, pending_receipt=aborted_pending
            ),
            lambda: spool.append_transfer(transfer=aborted_transfer),
        ):
            with self.assertRaises(self.storage.HistoricalFoundryStorageError):
                call()
        spool.close()

    def test_committed_reread_detects_corruption_and_truncation(self):
        for mutation in ("corrupt", "truncate"):
            spool = self._open()
            _transfer, _pending, receipt = self._commit(spool)
            row = dict(receipt)
            path = self._member_path()
            fd = os.open(str(path), os.O_RDWR)
            try:
                if mutation == "corrupt":
                    os.pwrite(fd, b"X", row["spool_offset"] + 8)
                    os.fsync(fd)
                else:
                    os.ftruncate(fd, row["spool_length"] - 1)
                    os.fsync(fd)
            finally:
                os.close(fd)
            with self.subTest(mutation=mutation):
                with self.assertRaises(self.storage.HistoricalFoundryStorageError):
                    spool.reread_exchange(receipt=receipt)
                projection = self.storage._project_historical_window_exchange_spool_for_test(
                    spool_or_sealed=spool
                )
                self.assertEqual(
                    tuple(projection.values()),
                    (
                        "closed",
                        row["spool_length"],
                        1,
                        0,
                        0,
                        1,
                        row["spool_length"],
                        None,
                    ),
                )
                self.assertEqual(tuple(self.data_dir.iterdir()), ())

    def test_review_commit_rechecks_currentness_after_complete_reread(self):
        real_pread = os.pread
        for mutation in ("replacement", "hardlink"):
            with self.subTest(mutation=mutation):
                spool = self._open()
                transfer, pending = self._append(spool)
                projection = self.storage._project_historical_window_exchange_spool_for_test(
                    spool_or_sealed=spool
                )
                frame_length = projection["provisional_physical_bytes"]
                member = self._member_path()
                moved = self.data_dir / "moved-commit-member"
                extra = self.data_dir / "extra-commit-link"
                reads = [0]

                def racing_pread(fd, length, offset):
                    value = real_pread(fd, length, offset)
                    reads[0] += 1
                    if reads[0] == 5:
                        if mutation == "replacement":
                            member.rename(moved)
                            member.write_bytes(b"replacement")
                            os.chmod(str(member), 0o600)
                        else:
                            os.link(str(member), str(extra))
                    return value

                receipt = None
                caught = None
                with mock.patch.object(
                    self.storage.os, "pread", side_effect=racing_pread
                ):
                    try:
                        receipt = spool.commit_transfer(
                            transfer=transfer, pending_receipt=pending
                        )
                    except BaseException as error:
                        caught = error
                if receipt is not None:
                    try:
                        spool.close()
                    except self.storage.HistoricalFoundryStorageError:
                        pass
                closed = dict(
                    self.storage._project_historical_window_exchange_spool_for_test(
                        spool_or_sealed=spool
                    )
                )
                members = tuple(self.data_dir.iterdir())
                for path in members:
                    path.unlink()
                self.assertIs(
                    type(caught), self.storage.HistoricalFoundryStorageError
                )
                self.assertIsNone(receipt)
                self.assertEqual(reads[0], 5)
                self.assertEqual(
                    tuple(closed.values()),
                    ("closed", 0, 0, frame_length, 1, 0, 0, None),
                )

    def test_review_commit_cancellation_never_exposes_partial_authoritative_state(self):
        critical_name = "_install_test_commit_transition"
        trace_points = []
        outer_points = []
        internal_return_points = []
        public_points = []
        transition_returned = [False]
        internal_returned = [False]

        def discover(frame, event, _argument):
            if (
                frame.f_code.co_name == critical_name
                and event in ("line", "return")
            ):
                point = (event, frame.f_lineno)
                if point not in trace_points:
                    trace_points.append(point)
                if event == "return":
                    transition_returned[0] = True
            elif (
                transition_returned[0]
                and frame.f_code.co_name == "_commit_transfer"
                and event == "line"
            ):
                point = (event, frame.f_lineno)
                if point not in outer_points:
                    outer_points.append(point)
            if frame.f_code.co_name == "_commit_transfer" and event == "return":
                point = (event, frame.f_lineno)
                if point not in internal_return_points:
                    internal_return_points.append(point)
                internal_returned[0] = True
            elif (
                internal_returned[0]
                and frame.f_code.co_name == "commit_transfer"
                and event == "line"
            ):
                point = (event, frame.f_lineno)
                if point not in public_points:
                    public_points.append(point)
            return discover

        probe_directory = self.data_dir / "commit-boundary-probe"
        probe_directory.mkdir(mode=0o700)
        probe = self.storage._open_historical_window_exchange_spool(
            data_dir=probe_directory
        )
        probe_transfer = self.storage._issue_historical_window_exchange_transfer_for_test(
            spool=probe, **_valid_transfer_arguments()
        )
        probe_pending = probe.append_transfer(transfer=probe_transfer)
        prior_trace = sys.gettrace()
        try:
            sys.settrace(discover)
            probe.commit_transfer(
                transfer=probe_transfer, pending_receipt=probe_pending
            )
        finally:
            sys.settrace(prior_trace)
            probe.close()
        self.assertGreater(len(trace_points), 0)
        self.assertGreater(len(outer_points), 0)

        targets = tuple(("transition", point) for point in trace_points) + tuple(
            ("post_transition", point) for point in outer_points
        ) + tuple(
            ("internal_return", point) for point in internal_return_points
        ) + tuple(
            ("public_delivery", point) for point in public_points
        )
        for index, (scope, point) in enumerate(targets):
            with self.subTest(scope=scope, trace_point=point):
                directory = self.data_dir / "commit-boundary-{}".format(index)
                directory.mkdir(mode=0o700)
                spool = self.storage._open_historical_window_exchange_spool(
                    data_dir=directory
                )
                transfer = self.storage._issue_historical_window_exchange_transfer_for_test(
                    spool=spool, **_valid_transfer_arguments()
                )
                pending = spool.append_transfer(transfer=transfer)
                frame_length = self.storage._project_historical_window_exchange_spool_for_test(
                    spool_or_sealed=spool
                )["provisional_physical_bytes"]
                marker = asyncio.CancelledError(
                    "commit trace point {}".format(point)
                )
                fired = [False]
                helper_returned = [False]
                commit_returned = [False]

                def tracer(frame, event, _argument):
                    if frame.f_code.co_name == critical_name and event == "return":
                        helper_returned[0] = True
                    if frame.f_code.co_name == "_commit_transfer" and event == "return":
                        commit_returned[0] = True
                    transition_match = (
                        scope == "transition"
                        and frame.f_code.co_name == critical_name
                        and (event, frame.f_lineno) == point
                    )
                    outer_match = (
                        scope == "post_transition"
                        and helper_returned[0]
                        and frame.f_code.co_name == "_commit_transfer"
                        and (event, frame.f_lineno) == point
                    )
                    internal_return_match = (
                        scope == "internal_return"
                        and frame.f_code.co_name == "_commit_transfer"
                        and (event, frame.f_lineno) == point
                    )
                    public_match = (
                        scope == "public_delivery"
                        and commit_returned[0]
                        and frame.f_code.co_name == "commit_transfer"
                        and (event, frame.f_lineno) == point
                    )
                    if not fired[0] and (
                        transition_match
                        or outer_match
                        or internal_return_match
                        or public_match
                    ):
                        fired[0] = True
                        raise marker
                    return tracer

                caught = None
                prior_trace = sys.gettrace()
                try:
                    sys.settrace(tracer)
                    spool.commit_transfer(
                        transfer=transfer, pending_receipt=pending
                    )
                except BaseException as error:
                    caught = error
                finally:
                    sys.settrace(prior_trace)
                self.assertTrue(fired[0])
                self.assertIs(caught, marker)
                projection = tuple(
                    self.storage._project_historical_window_exchange_spool_for_test(
                        spool_or_sealed=spool
                    ).values()
                )
                self.assertIn(
                    projection,
                    (
                        (
                            "closed",
                            0,
                            0,
                            frame_length,
                            1,
                            0,
                            0,
                            None,
                        ),
                        (
                            "closed",
                            frame_length,
                            1,
                            0,
                            0,
                            1,
                            frame_length,
                            None,
                        ),
                    ),
                )
                self.assertEqual(tuple(directory.iterdir()), ())
                self.assertIsNone(spool.close())
                marker = marker.with_traceback(None)
        self.assertGreater(len(internal_return_points), 0)
        self.assertGreater(len(public_points), 0)

    def test_review_round2_commit_retains_inventory_and_appends_once(self):
        spool = self._open()
        append_calls = [0]
        detached_receipts = []

        def profiler(frame, event, argument):
            if (
                frame.f_code.co_name == "_install_test_commit_transition"
                and event == "c_call"
                and getattr(argument, "__name__", None) == "append"
            ):
                append_calls[0] += 1

        prior_profile = sys.getprofile()
        try:
            sys.setprofile(profiler)
            for exchange_index in range(1, 65):
                transfer = self._issue(
                    spool,
                    exchange_index=exchange_index,
                    request_ids=(exchange_index,),
                    response_ids=(exchange_index,),
                    request_bytes=b"r",
                    decoded_bytes=b"d",
                )
                pending = spool.append_transfer(transfer=transfer)
                receipt = spool.commit_transfer(
                    transfer=transfer, pending_receipt=pending
                )
                detached_receipts.append(dict(receipt))
        finally:
            sys.setprofile(prior_profile)
        projection = dict(
            self.storage._project_historical_window_exchange_spool_for_test(
                spool_or_sealed=spool
            )
        )
        expected_eof = 64 * (8 + 1 + 8 + 1)
        self.assertEqual(projection["committed_physical_bytes"], expected_eof)
        self.assertEqual(projection["committed_members"], 64)
        self.assertEqual(projection["committed_receipt_count"], 64)
        self.assertEqual(projection["committed_eof"], expected_eof)
        self.assertEqual(append_calls[0], 64)

        expected_digest = hashlib.sha256()
        expected_digest.update(
            b"historical_foundry_exchange_spool_receipt_inventory/v1\0"
        )
        for receipt_projection in detached_receipts:
            payload = json.dumps(
                receipt_projection,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
            expected_digest.update(len(payload).to_bytes(8, "big"))
            expected_digest.update(payload)
        sealed = spool.seal()
        sealed_projection = dict(
            self.storage._project_historical_window_exchange_spool_for_test(
                spool_or_sealed=sealed
            )
        )
        self.assertEqual(sealed_projection["committed_receipt_count"], 64)
        self.assertEqual(sealed_projection["committed_eof"], expected_eof)
        self.assertEqual(
            sealed_projection["receipt_inventory_sha256"],
            expected_digest.hexdigest(),
        )
        sealed.close()

    def test_review_round2_append_and_abort_delivery_cancellation_closes_lineage(self):
        operations = (
            (
                "append",
                "_append_transfer",
                "append_transfer",
                (
                    "_install_append_quota_transition",
                    "_install_append_transition",
                ),
            ),
            (
                "abort",
                "_abort_transfer",
                "abort_transfer",
                ("_install_abort_transition",),
            ),
        )
        for operation, internal_name, public_name, helper_names in operations:
            helper_points = []
            internal_returns = []
            public_points = []
            internal_returned = [False]

            def discover(frame, event, _argument):
                if frame.f_code.co_name in helper_names and event in ("line", "return"):
                    point = (frame.f_code.co_name, event, frame.f_lineno)
                    if point not in helper_points:
                        helper_points.append(point)
                if frame.f_code.co_name == internal_name and event == "return":
                    point = (event, frame.f_lineno)
                    if point not in internal_returns:
                        internal_returns.append(point)
                    internal_returned[0] = True
                elif (
                    internal_returned[0]
                    and frame.f_code.co_name == public_name
                    and event == "line"
                ):
                    point = (event, frame.f_lineno)
                    if point not in public_points:
                        public_points.append(point)
                return discover

            probe_directory = self.data_dir / (operation + "-delivery-probe")
            probe_directory.mkdir(mode=0o700)
            probe = self.storage._open_historical_window_exchange_spool(
                data_dir=probe_directory
            )
            probe_transfer = self._issue(probe)
            probe_pending = None
            prior_trace = sys.gettrace()
            try:
                sys.settrace(discover)
                probe_pending = probe.append_transfer(transfer=probe_transfer)
                if operation == "abort":
                    probe.abort_transfer(
                        transfer=probe_transfer,
                        pending_receipt=probe_pending,
                    )
            finally:
                sys.settrace(prior_trace)
            if operation == "append":
                probe.abort_transfer(
                    transfer=probe_transfer, pending_receipt=probe_pending
                )
            probe.close()

            targets = tuple(("helper", point) for point in helper_points) + tuple(
                ("internal_return", point) for point in internal_returns
            ) + tuple(("public_delivery", point) for point in public_points)
            for index, (scope, point) in enumerate(targets):
                with self.subTest(
                    operation=operation, scope=scope, trace_point=point
                ):
                    directory = self.data_dir / "{}-delivery-{}".format(
                        operation, index
                    )
                    directory.mkdir(mode=0o700)
                    spool = self.storage._open_historical_window_exchange_spool(
                        data_dir=directory
                    )
                    transfer = self.storage._issue_historical_window_exchange_transfer_for_test(
                        spool=spool, **_valid_transfer_arguments()
                    )
                    pending = None
                    if operation == "abort":
                        pending = spool.append_transfer(transfer=transfer)
                    frame_length = 16 + len(b'{"id":1}') + len(
                        b'[{"id":1,"result":"0x1"}]'
                    )
                    marker = asyncio.CancelledError(
                        "{} delivery {}".format(operation, point)
                    )
                    fired = [False]
                    returned = [False]
                    references = []

                    def tracer(frame, event, _argument):
                        if frame.f_code.co_name == internal_name and event == "return":
                            returned[0] = True
                        helper_match = (
                            scope == "helper"
                            and frame.f_code.co_name in helper_names
                            and (frame.f_code.co_name, event, frame.f_lineno)
                            == point
                        )
                        internal_match = (
                            scope == "internal_return"
                            and frame.f_code.co_name == internal_name
                            and (event, frame.f_lineno) == point
                        )
                        public_match = (
                            scope == "public_delivery"
                            and returned[0]
                            and frame.f_code.co_name == public_name
                            and (event, frame.f_lineno) == point
                        )
                        if not fired[0] and (
                            helper_match or internal_match or public_match
                        ):
                            if operation == "append":
                                for value in frame.f_locals.values():
                                    if type(value) is self.storage._PendingHistoricalWindowSpoolReceipt:
                                        references.append(weakref.ref(value))
                            fired[0] = True
                            raise marker
                        return tracer

                    caught = None
                    prior_trace = sys.gettrace()
                    try:
                        sys.settrace(tracer)
                        if operation == "append":
                            spool.append_transfer(transfer=transfer)
                        else:
                            spool.abort_transfer(
                                transfer=transfer,
                                pending_receipt=pending,
                            )
                    except BaseException as error:
                        caught = error
                    finally:
                        sys.settrace(prior_trace)
                    self.assertTrue(fired[0])
                    self.assertIs(caught, marker)
                    caught = None
                    marker = marker.with_traceback(None)
                    gc.collect()
                    self.assertTrue(
                        all(reference() is None for reference in references)
                    )
                    projection = tuple(
                        self.storage._project_historical_window_exchange_spool_for_test(
                            spool_or_sealed=spool
                        ).values()
                    )
                    if operation == "append":
                        self.assertEqual(
                            projection,
                            ("closed", 0, 0, 0, 0, 0, 0, None),
                        )
                    else:
                        self.assertIn(
                            projection,
                            (
                                ("closed", 0, 0, frame_length, 1, 0, 0, None),
                                ("closed", 0, 0, 0, 0, 0, 0, None),
                            ),
                        )
                    self.assertEqual(tuple(directory.iterdir()), ())
                    self.assertIsNone(spool.close())
            self.assertGreater(len(internal_returns), 0)
            self.assertGreater(len(public_points), 0)

    def test_review_receipt_key_gate_is_exact_and_sanitized(self):
        spool = self._open()
        _transfer, _pending, receipt = self._commit(spool)

        class HostileKey:
            touched = False

            def __hash__(self):
                type(self).touched = True
                raise RuntimeError("SECRET HOSTILE KEY")

        class StrSubclass(str):
            def __hash__(self):
                raise RuntimeError("SECRET STR SUBCLASS")

        for key in (HostileKey(), StrSubclass("schema"), "missing"):
            with self.subTest(key_type=type(key).__name__):
                caught = None
                try:
                    receipt[key]
                except BaseException as error:
                    caught = error
                self.assertIs(
                    type(caught), self.storage.HistoricalFoundryStorageError
                )
                self.assertEqual(
                    str(caught), "historical foundry storage failed"
                )
                self.assertIsNone(caught.__cause__)
                self.assertIsNone(caught.__context__)
        self.assertFalse(HostileKey.touched)
        self.assertEqual(dict(receipt)["schema"], "historical_foundry_exchange_spool_receipt/v1")
        spool.close()


class HistoricalFoundryStorageSealTests(unittest.TestCase):
    def setUp(self):
        self.storage = importlib.import_module("scripts.historical_foundry_storage")
        self.temporary = tempfile.TemporaryDirectory(dir="/private/tmp")
        self.data_dir = Path(self.temporary.name)
        os.chmod(str(self.data_dir), 0o700)

    def tearDown(self):
        self.temporary.cleanup()

    def _open(self):
        return self.storage._open_historical_window_exchange_spool(
            data_dir=self.data_dir
        )

    def _commit(self, spool, **overrides):
        arguments = _valid_transfer_arguments(**overrides)
        transfer = self.storage._issue_historical_window_exchange_transfer_for_test(
            spool=spool, **arguments
        )
        pending = spool.append_transfer(transfer=transfer)
        receipt = spool.commit_transfer(
            transfer=transfer, pending_receipt=pending
        )
        return arguments, receipt

    def _projection(self, value):
        return self.storage._project_historical_window_exchange_spool_for_test(
            spool_or_sealed=value
        )

    def test_seal_streams_inventory_quota_parity_and_known_answer_digest(self):
        spool = self._open()
        _first_arguments, first = self._commit(spool)
        _second_arguments, second = self._commit(
            spool,
            exchange_index=2,
            logical_batch_index=5,
            attempt_index=3,
            request_ids=(7, 8),
            response_ids=(8, 7),
            request_bytes=b"seal-request-two",
            decoded_bytes=b"seal-response-two",
        )
        expected = hashlib.sha256()
        expected.update(
            b"historical_foundry_exchange_spool_receipt_inventory/v1\0"
        )
        for receipt in (first, second):
            payload = json.dumps(
                dict(receipt),
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                allow_nan=False,
            ).encode("utf-8")
            expected.update(len(payload).to_bytes(8, "big"))
            expected.update(payload)
        sealed = spool.seal()
        projection = self._projection(sealed)
        self.assertEqual(projection["state"], "sealed")
        self.assertEqual(projection["committed_receipt_count"], 2)
        self.assertEqual(
            projection["committed_physical_bytes"], projection["committed_eof"]
        )
        self.assertEqual(projection["committed_members"], 2)
        self.assertEqual(
            projection["receipt_inventory_sha256"], expected.hexdigest()
        )
        sealed.close()

    def test_seal_moves_read_fd_ancestry_and_same_quota_without_shared_ownership(self):
        spool = self._open()
        arguments, receipt = self._commit(spool)
        before = dict(self._projection(spool))
        real_open = os.open
        reopened = []

        def recording_open(path, flags, *args, **kwargs):
            fd = real_open(path, flags, *args, **kwargs)
            reopened.append((path, flags, kwargs.get("dir_fd"), fd))
            return fd

        with mock.patch.object(self.storage.os, "open", side_effect=recording_open):
            sealed = spool.seal()
        self.assertEqual(len(reopened), 1)
        _path, flags, directory_fd, read_fd = reopened[0]
        self.assertIsNotNone(directory_fd)
        self.assertEqual(flags & (os.O_WRONLY | os.O_RDWR), 0)
        self.assertTrue(flags & os.O_NOFOLLOW)
        self.assertTrue(flags & os.O_CLOEXEC)
        self.assertEqual(
            sealed.reread_exchange(receipt=receipt),
            (
                arguments["canonical_request_bytes"],
                arguments["decoded_response_bytes"],
            ),
        )
        after = dict(self._projection(sealed))
        for key in (
            "committed_physical_bytes",
            "committed_members",
            "committed_receipt_count",
            "committed_eof",
        ):
            self.assertEqual(after[key], before[key])
        self.assertIsNone(spool.close())
        self.assertEqual(sealed.reread_exchange(receipt=receipt)[0], b'{"id":1}')
        sealed.close()
        with self.assertRaises(OSError):
            os.fstat(read_fd)

    def test_active_apis_reject_after_seal_without_harming_sealed_owner(self):
        spool = self._open()
        arguments, receipt = self._commit(spool)
        sealed = spool.seal()

        class Hostile:
            def __getattribute__(self, _name):
                raise AssertionError("moved active inspected hostile argument")

        hostile = Hostile()
        for call in (
            lambda: self.storage._issue_historical_window_exchange_transfer_for_test(
                spool=spool,
                exchange_projection=hostile,
                canonical_request_bytes=hostile,
                decoded_response_bytes=hostile,
            ),
            lambda: spool.append_transfer(transfer=hostile),
            lambda: spool.commit_transfer(transfer=hostile, pending_receipt=hostile),
            lambda: spool.abort_transfer(transfer=hostile, pending_receipt=hostile),
            lambda: spool.reread_exchange(receipt=hostile),
            lambda: spool.seal(),
            lambda: self._projection(spool),
        ):
            with self.assertRaises(self.storage.HistoricalFoundryStorageError):
                call()
        self.assertIsNone(spool.close())
        self.assertEqual(
            sealed.reread_exchange(receipt=receipt),
            (
                arguments["canonical_request_bytes"],
                arguments["decoded_response_bytes"],
            ),
        )
        sealed.close()

    def test_close_tombstone_and_scalar_audit_are_idempotent_and_weak(self):
        active = self._open()
        self._commit(active)
        active.close()
        closed_active = self._projection(active)
        self.assertEqual(closed_active["state"], "closed")
        self.assertTrue(
            all(
                type(value) in (str, int, type(None))
                for value in closed_active.values()
            )
        )
        self.assertIsNone(active.close())
        active_reference = weakref.ref(active)
        del active
        gc.collect()
        self.assertIsNone(active_reference())

        moved_active = self._open()
        self._commit(moved_active)
        sealed = moved_active.seal()
        sealed.close()
        self.assertEqual(self._projection(moved_active)["state"], "closed")
        self.assertEqual(self._projection(sealed)["state"], "closed")
        self.assertIsNone(moved_active.close())
        self.assertIsNone(sealed.close())
        moved_reference = weakref.ref(moved_active)
        sealed_reference = weakref.ref(sealed)
        del moved_active
        del sealed
        gc.collect()
        self.assertIsNone(moved_reference())
        self.assertIsNone(sealed_reference())

    def test_unregistered_active_and_sealed_clone_close_rejects_without_effect(self):
        spool = self._open()
        arguments, receipt = self._commit(spool)
        sealed = spool.seal()
        for authority_class in (
            self.storage._HistoricalWindowExchangeSpool,
            self.storage._SealedHistoricalWindowExchangeSpool,
        ):
            clone = object.__new__(authority_class)
            with self.assertRaises(self.storage.HistoricalFoundryStorageError):
                clone.close()
        self.assertEqual(
            sealed.reread_exchange(receipt=receipt),
            (
                arguments["canonical_request_bytes"],
                arguments["decoded_response_bytes"],
            ),
        )
        sealed.close()

    def test_close_rejects_completed_entry_replacement_and_closes_fds(self):
        real_open = os.open
        opened_fds = []

        def recording_open(path, flags, *args, **kwargs):
            fd = real_open(path, flags, *args, **kwargs)
            opened_fds.append(fd)
            return fd

        supported = set(os.supports_dir_fd)
        supported.discard(real_open)
        supported.add(recording_open)
        with mock.patch.object(self.storage.os, "open", new=recording_open), mock.patch.object(
            self.storage.os, "supports_dir_fd", supported
        ):
            spool = self._open()
        member = tuple(self.data_dir.iterdir())[0]
        moved = self.data_dir / "moved-spool"
        member.rename(moved)
        member.write_bytes(b"replacement")
        os.chmod(str(member), 0o600)
        with self.assertRaises(self.storage.HistoricalFoundryStorageError):
            spool.close()
        self.assertEqual(member.read_bytes(), b"replacement")
        self.assertEqual(moved.read_bytes(), b"")
        self.assertIsNone(spool.close())
        for fd in opened_fds:
            with self.assertRaises(OSError):
                os.fstat(fd)
        member.unlink()
        moved.unlink()

    def test_review_seal_rechecks_entry_and_ancestry_after_readonly_reopen(self):
        real_open = os.open
        real_stat = os.stat
        real_fstat = os.fstat
        for mutation in ("entry", "ancestry"):
            with self.subTest(mutation=mutation):
                case_directory = self.data_dir / mutation
                parent = case_directory / "parent"
                leaf = parent / "leaf"
                leaf.mkdir(parents=True, mode=0o700)
                spool = self.storage._open_historical_window_exchange_spool(
                    data_dir=leaf
                )
                _arguments, receipt = self._commit(spool)
                row = dict(receipt)
                member = tuple(leaf.iterdir())[0]
                moved_member = leaf / "moved-seal-member"
                moved_parent = case_directory / "moved-parent"
                opened = []
                reopened = [False]
                raced = [False]

                def recording_open(path, flags, *args, **kwargs):
                    fd = real_open(path, flags, *args, **kwargs)
                    opened.append(fd)
                    if (
                        type(path) is str
                        and path.startswith(
                            ".historical-foundry-exchange-spool-"
                        )
                        and flags & (os.O_WRONLY | os.O_RDWR) == 0
                    ):
                        reopened[0] = True
                    return fd

                def racing_stat(path, *args, **kwargs):
                    details = real_stat(path, *args, **kwargs)
                    if (
                        reopened[0]
                        and not raced[0]
                        and type(path) is str
                        and path.startswith(
                            ".historical-foundry-exchange-spool-"
                        )
                        and kwargs.get("dir_fd") is not None
                    ):
                        raced[0] = True
                        if mutation == "entry":
                            member.rename(moved_member)
                            member.write_bytes(b"replacement")
                            os.chmod(str(member), 0o600)
                        else:
                            parent.rename(moved_parent)
                            leaf.mkdir(parents=True, mode=0o700)
                    return details

                dir_fd_support = set(os.supports_dir_fd)
                dir_fd_support.discard(real_open)
                dir_fd_support.add(recording_open)
                dir_fd_support.discard(real_stat)
                dir_fd_support.add(racing_stat)
                follow_support = set(os.supports_follow_symlinks)
                follow_support.discard(real_stat)
                follow_support.add(racing_stat)
                sealed = None
                caught = None
                with mock.patch.object(
                    self.storage.os, "open", new=recording_open
                ), mock.patch.object(
                    self.storage.os, "stat", new=racing_stat
                ), mock.patch.object(
                    self.storage.os, "supports_dir_fd", dir_fd_support
                ), mock.patch.object(
                    self.storage.os,
                    "supports_follow_symlinks",
                    follow_support,
                ):
                    try:
                        sealed = spool.seal()
                    except BaseException as error:
                        caught = error
                if sealed is not None:
                    try:
                        sealed.close()
                    except self.storage.HistoricalFoundryStorageError:
                        pass
                closed = tuple(self._projection(spool).values())
                all_closed = True
                for fd in opened:
                    try:
                        real_fstat(fd)
                    except OSError:
                        continue
                    all_closed = False
                    os.close(fd)
                self.assertTrue(raced[0])
                self.assertIs(
                    type(caught), self.storage.HistoricalFoundryStorageError
                )
                self.assertIsNone(sealed)
                self.assertEqual(
                    closed,
                    (
                        "closed",
                        row["spool_length"],
                        1,
                        0,
                        0,
                        1,
                        row["spool_length"],
                        None,
                    ),
                )
                self.assertTrue(all_closed)
                if mutation == "entry":
                    self.assertEqual(member.read_bytes(), b"replacement")
                    self.assertEqual(
                        moved_member.stat().st_size, row["spool_length"]
                    )

    def test_review_round2_postseal_mutator_control_preserves_moved_owner(self):
        probe_directory = self.data_dir / "postseal-control-probe"
        probe_directory.mkdir(mode=0o700)
        probe = self.storage._open_historical_window_exchange_spool(
            data_dir=probe_directory
        )
        probe_transfer = self.storage._issue_historical_window_exchange_transfer_for_test(
            spool=probe, **_valid_transfer_arguments()
        )
        probe_quota = self.storage._get_historical_window_run_quota_for_test(
            spool=probe
        )
        probe_pending = probe.append_transfer(transfer=probe_transfer)
        probe.commit_transfer(
            transfer=probe_transfer, pending_receipt=probe_pending
        )
        probe_sealed = probe.seal()
        targets = {}

        def discover(frame, event, _argument):
            if event == "line" and frame.f_code.co_name in (
                "_normal_active_record",
                "_active_owner_for_quota",
            ):
                targets.setdefault(frame.f_code.co_name, frame.f_lineno)
            return discover

        prior_trace = sys.gettrace()
        try:
            sys.settrace(discover)
            with self.assertRaises(self.storage.HistoricalFoundryStorageError):
                probe.append_transfer(transfer=object())
            with self.assertRaises(self.storage.HistoricalFoundryStorageError):
                probe_quota._abort_reservation_for_test()
        finally:
            sys.settrace(prior_trace)
        probe_sealed.close()
        self.assertEqual(
            set(targets), {"_normal_active_record", "_active_owner_for_quota"}
        )

        for index, (operation, target_name) in enumerate(
            (
                ("append", "_normal_active_record"),
                ("seal", "_normal_active_record"),
                ("quota", "_active_owner_for_quota"),
            )
        ):
            with self.subTest(operation=operation):
                directory = self.data_dir / "postseal-control-{}".format(index)
                directory.mkdir(mode=0o700)
                spool = self.storage._open_historical_window_exchange_spool(
                    data_dir=directory
                )
                transfer = self.storage._issue_historical_window_exchange_transfer_for_test(
                    spool=spool, **_valid_transfer_arguments()
                )
                quota = self.storage._get_historical_window_run_quota_for_test(
                    spool=spool
                )
                pending = spool.append_transfer(transfer=transfer)
                receipt = spool.commit_transfer(
                    transfer=transfer, pending_receipt=pending
                )
                row = dict(receipt)
                sealed = spool.seal()
                digest = self._projection(sealed)["receipt_inventory_sha256"]
                expected = (
                    "sealed",
                    row["spool_length"],
                    1,
                    0,
                    0,
                    1,
                    row["spool_length"],
                    digest,
                )
                marker = asyncio.CancelledError(
                    "postseal {} control".format(operation)
                )
                fired = [False]

                def tracer(frame, event, _argument):
                    if (
                        not fired[0]
                        and frame.f_code.co_name == target_name
                        and event == "line"
                        and frame.f_lineno == targets[target_name]
                    ):
                        fired[0] = True
                        raise marker
                    return tracer

                caught = None
                prior_trace = sys.gettrace()
                try:
                    sys.settrace(tracer)
                    if operation == "append":
                        spool.append_transfer(transfer=object())
                    elif operation == "seal":
                        spool.seal()
                    else:
                        quota._abort_reservation_for_test()
                except BaseException as error:
                    caught = error
                finally:
                    sys.settrace(prior_trace)
                self.assertTrue(fired[0])
                self.assertIs(caught, marker)
                self.assertEqual(len(tuple(directory.iterdir())), 1)
                with self.assertRaises(
                    self.storage.HistoricalFoundryStorageError
                ):
                    self._projection(spool)
                self.assertEqual(tuple(self._projection(sealed).values()), expected)
                self.assertEqual(
                    sealed.reread_exchange(receipt=receipt)[0], b'{"id":1}'
                )
                self.assertIsNone(spool.close())
                self.assertIsNone(sealed.close())
                self.assertEqual(tuple(directory.iterdir()), ())
                marker = marker.with_traceback(None)

    def test_review_round3_terminal_close_interruptions_finish_cleanup(self):
        terminalizers = (
            ("active", "_terminalize_active"),
            ("sealed", "_terminalize_sealed"),
        )

        def prepare(kind, label):
            directory = self.data_dir / label
            directory.mkdir(mode=0o700)
            opened_fds = []
            real_open = os.open

            def recording_open(*args, **kwargs):
                fd = real_open(*args, **kwargs)
                opened_fds.append(fd)
                return fd

            supported_open = set(os.supports_dir_fd)
            supported_open.add(recording_open)
            with mock.patch.object(
                self.storage.os, "open", new=recording_open
            ), mock.patch.object(
                self.storage.os, "supports_dir_fd", new=supported_open
            ):
                spool = self.storage._open_historical_window_exchange_spool(
                    data_dir=directory
                )
                _arguments, receipt = self._commit(spool)
                row = dict(receipt)
                owner = spool
                digest = None
                if kind == "sealed":
                    owner = spool.seal()
                    digest = self._projection(owner)[
                        "receipt_inventory_sha256"
                    ]
            expected = (
                "closed",
                row["spool_length"],
                1,
                0,
                0,
                1,
                row["spool_length"],
                digest,
            )
            live_fds = []
            for fd in dict.fromkeys(opened_fds):
                try:
                    os.fstat(fd)
                except OSError:
                    continue
                live_fds.append(fd)
            return directory, spool, owner, receipt, expected, tuple(live_fds)

        for kind, terminalizer_name in terminalizers:
            probe = prepare(kind, "round3-close-probe-{}".format(kind))
            probe_directory, probe_spool, probe_owner = probe[:3]
            points = []
            terminalizer_returned = [False]

            def discover(frame, event, _argument):
                name = frame.f_code.co_name
                if name in (terminalizer_name, "_cleanup_resources") and event in (
                    "line",
                    "return",
                ):
                    point = (name, event, frame.f_lineno)
                    if point not in points:
                        points.append(point)
                    if name == terminalizer_name and event == "return":
                        terminalizer_returned[0] = True
                elif (
                    terminalizer_returned[0]
                    and name == "close"
                    and event == "line"
                ):
                    point = (name, event, frame.f_lineno)
                    if point not in points:
                        points.append(point)
                return discover

            prior_trace = sys.gettrace()
            try:
                sys.settrace(discover)
                probe_owner.close()
            finally:
                sys.settrace(prior_trace)
            self.assertEqual(tuple(probe_directory.iterdir()), ())
            self.assertGreater(len(points), 0)

            for index, point in enumerate(points):
                with self.subTest(kind=kind, point=point):
                    prepared = prepare(
                        kind, "round3-close-{}-{}".format(kind, index)
                    )
                    directory, spool, owner, receipt, expected, live_fds = prepared
                    marker = asyncio.CancelledError(
                        "round3 {} close {}".format(kind, point)
                    )
                    fired = [False]
                    close_counts = {}
                    unlink_count = [0]
                    fsync_count = [0]
                    real_close = os.close
                    real_unlink = os.unlink
                    real_fsync = os.fsync

                    def recording_close(fd):
                        close_counts[fd] = close_counts.get(fd, 0) + 1
                        return real_close(fd)

                    def recording_unlink(*args, **kwargs):
                        unlink_count[0] += 1
                        return real_unlink(*args, **kwargs)

                    def recording_fsync(fd):
                        fsync_count[0] += 1
                        return real_fsync(fd)

                    def tracer(frame, event, _argument):
                        current = (
                            frame.f_code.co_name,
                            event,
                            frame.f_lineno,
                        )
                        if not fired[0] and current == point:
                            fired[0] = True
                            raise marker
                        return tracer

                    caught = None
                    prior_trace = sys.gettrace()
                    try:
                        with mock.patch.object(
                            self.storage.os, "close", new=recording_close
                        ), mock.patch.object(
                            self.storage.os, "unlink", new=recording_unlink
                        ), mock.patch.object(
                            self.storage.os, "fsync", new=recording_fsync
                        ):
                            sys.settrace(tracer)
                            try:
                                owner.close()
                            except BaseException as error:
                                caught = error
                    finally:
                        sys.settrace(prior_trace)
                    self.assertTrue(fired[0])
                    self.assertIs(caught, marker)
                    for fd in live_fds:
                        with self.assertRaises(OSError):
                            os.fstat(fd)
                        self.assertEqual(close_counts.get(fd), 1)
                    self.assertTrue(
                        all(count == 1 for count in close_counts.values())
                    )
                    self.assertEqual(unlink_count[0], 1)
                    self.assertEqual(fsync_count[0], 1)
                    self.assertEqual(tuple(directory.iterdir()), ())
                    self.assertEqual(
                        tuple(self._projection(owner).values()), expected
                    )
                    if kind == "sealed":
                        self.assertEqual(
                            tuple(self._projection(spool).values()), expected
                        )
                    self.assertIsNone(owner.close())
                    self.assertIsNone(spool.close())
                    with self.assertRaises(
                        self.storage.HistoricalFoundryStorageError
                    ):
                        dict(receipt)
                    marker = marker.with_traceback(None)

    def test_review_round3_recursive_close_during_cleanup_is_idempotent(self):
        for kind in ("active", "sealed"):
            with self.subTest(kind=kind):
                directory = self.data_dir / "round3-recursive-close-{}".format(
                    kind
                )
                directory.mkdir(mode=0o700)
                opened_fds = []
                real_open = os.open

                def recording_open(*args, **kwargs):
                    fd = real_open(*args, **kwargs)
                    opened_fds.append(fd)
                    return fd

                supported_open = set(os.supports_dir_fd)
                supported_open.add(recording_open)
                with mock.patch.object(
                    self.storage.os, "open", new=recording_open
                ), mock.patch.object(
                    self.storage.os, "supports_dir_fd", new=supported_open
                ):
                    spool = self.storage._open_historical_window_exchange_spool(
                        data_dir=directory
                    )
                    _arguments, receipt = self._commit(spool)
                    row = dict(receipt)
                    owner = spool
                    digest = None
                    if kind == "sealed":
                        owner = spool.seal()
                        digest = self._projection(owner)[
                            "receipt_inventory_sha256"
                        ]

                live_fds = []
                for fd in dict.fromkeys(opened_fds):
                    try:
                        os.fstat(fd)
                    except OSError:
                        continue
                    live_fds.append(fd)

                real_close = os.close
                real_unlink = os.unlink
                real_fsync = os.fsync
                close_counts = {}
                unlink_count = [0]
                fsync_count = [0]
                recursive_results = []

                def recording_close(fd):
                    close_counts[fd] = close_counts.get(fd, 0) + 1
                    return real_close(fd)

                def recursive_unlink(*args, **kwargs):
                    unlink_count[0] += 1
                    recursive_results.append(owner.close())
                    return real_unlink(*args, **kwargs)

                def recording_fsync(fd):
                    fsync_count[0] += 1
                    return real_fsync(fd)

                with mock.patch.object(
                    self.storage.os, "close", new=recording_close
                ), mock.patch.object(
                    self.storage.os, "unlink", new=recursive_unlink
                ), mock.patch.object(
                    self.storage.os, "fsync", new=recording_fsync
                ):
                    self.assertIsNone(owner.close())

                self.assertEqual(recursive_results, [None])
                self.assertEqual(unlink_count[0], 1)
                self.assertEqual(fsync_count[0], 1)
                for fd in live_fds:
                    with self.assertRaises(OSError):
                        os.fstat(fd)
                    self.assertEqual(close_counts.get(fd), 1)
                self.assertTrue(
                    all(count == 1 for count in close_counts.values())
                )
                self.assertEqual(tuple(directory.iterdir()), ())
                expected = (
                    "closed",
                    row["spool_length"],
                    1,
                    0,
                    0,
                    1,
                    row["spool_length"],
                    digest,
                )
                self.assertEqual(tuple(self._projection(owner).values()), expected)
                if kind == "sealed":
                    self.assertEqual(
                        tuple(self._projection(spool).values()), expected
                    )
                self.assertIsNone(owner.close())
                self.assertIsNone(spool.close())
                with self.assertRaises(
                    self.storage.HistoricalFoundryStorageError
                ):
                    dict(receipt)

    def test_review_round3_reread_delivery_control_closes_current_owner(self):
        operations = (
            ("active", "_reread_exchange"),
            ("sealed", "_sealed_reread_exchange"),
        )

        def prepare(kind, label):
            directory = self.data_dir / label
            directory.mkdir(mode=0o700)
            spool = self.storage._open_historical_window_exchange_spool(
                data_dir=directory
            )
            arguments, receipt = self._commit(spool)
            row = dict(receipt)
            owner = spool
            digest = None
            if kind == "sealed":
                owner = spool.seal()
                digest = self._projection(owner)["receipt_inventory_sha256"]
            expected = (
                "closed",
                row["spool_length"],
                1,
                0,
                0,
                1,
                row["spool_length"],
                digest,
            )
            return directory, spool, owner, receipt, arguments, expected

        for kind, internal_name in operations:
            probe = prepare(kind, "round3-reread-probe-{}".format(kind))
            probe_directory, probe_spool, probe_owner, probe_receipt = probe[:4]
            internal_points = []
            public_points = []
            internal_returned = [False]

            def discover(frame, event, _argument):
                name = frame.f_code.co_name
                if name == internal_name and event == "return":
                    point = (name, event, frame.f_lineno)
                    if point not in internal_points:
                        internal_points.append(point)
                    internal_returned[0] = True
                elif (
                    internal_returned[0]
                    and name == "reread_exchange"
                    and event == "line"
                ):
                    point = (name, event, frame.f_lineno)
                    if point not in public_points:
                        public_points.append(point)
                return discover

            prior_trace = sys.gettrace()
            try:
                sys.settrace(discover)
                probe_owner.reread_exchange(receipt=probe_receipt)
            finally:
                sys.settrace(prior_trace)
            probe_owner.close()
            probe_spool.close()
            self.assertEqual(tuple(probe_directory.iterdir()), ())
            self.assertGreater(len(internal_points), 0)

            points = internal_points + public_points
            for index, point in enumerate(points):
                with self.subTest(kind=kind, point=point):
                    prepared = prepare(
                        kind, "round3-reread-{}-{}".format(kind, index)
                    )
                    directory, spool, owner, receipt, _arguments, expected = prepared
                    marker = asyncio.CancelledError(
                        "round3 {} reread {}".format(kind, point)
                    )
                    fired = [False]
                    internal_returned = [False]

                    def tracer(frame, event, _argument):
                        name = frame.f_code.co_name
                        if name == internal_name and event == "return":
                            internal_returned[0] = True
                        internal_match = (
                            point[0] == internal_name
                            and (name, event, frame.f_lineno) == point
                        )
                        public_match = (
                            point[0] == "reread_exchange"
                            and internal_returned[0]
                            and (name, event, frame.f_lineno) == point
                        )
                        if not fired[0] and (internal_match or public_match):
                            fired[0] = True
                            raise marker
                        return tracer

                    caught = None
                    prior_trace = sys.gettrace()
                    try:
                        sys.settrace(tracer)
                        owner.reread_exchange(receipt=receipt)
                    except BaseException as error:
                        caught = error
                    finally:
                        sys.settrace(prior_trace)
                    self.assertTrue(fired[0])
                    self.assertIs(caught, marker)
                    self.assertEqual(tuple(directory.iterdir()), ())
                    self.assertEqual(
                        tuple(self._projection(owner).values()), expected
                    )
                    if kind == "sealed":
                        self.assertEqual(
                            tuple(self._projection(spool).values()), expected
                        )
                    self.assertIsNone(owner.close())
                    self.assertIsNone(spool.close())
                    with self.assertRaises(
                        self.storage.HistoricalFoundryStorageError
                    ):
                        dict(receipt)
                    marker = marker.with_traceback(None)
            self.assertGreater(len(public_points), 0)

    def test_review_round3_seal_consumes_writer_fd_before_close_control(self):
        directory = self.data_dir / "round3-seal-writer-close"
        directory.mkdir(mode=0o700)
        spool = self.storage._open_historical_window_exchange_spool(
            data_dir=directory
        )
        _arguments, receipt = self._commit(spool)
        row = dict(receipt)
        marker = asyncio.CancelledError("round3 retiring writer close")
        real_close = os.close
        real_open = os.open
        close_counts = {}
        replacement_fd = [None]
        first_closed_fd = [None]

        def close_then_reuse_and_cancel(fd):
            close_counts[fd] = close_counts.get(fd, 0) + 1
            real_close(fd)
            if replacement_fd[0] is None:
                first_closed_fd[0] = fd
                replacement_fd[0] = real_open("/dev/null", os.O_RDONLY)
                raise marker

        caught = None
        with mock.patch.object(
            self.storage.os, "close", new=close_then_reuse_and_cancel
        ):
            try:
                spool.seal()
            except BaseException as error:
                caught = error
        self.assertIs(caught, marker)
        self.assertEqual(replacement_fd[0], first_closed_fd[0])
        replacement_is_open = True
        try:
            os.fstat(replacement_fd[0])
        except OSError:
            replacement_is_open = False
        if replacement_is_open:
            real_close(replacement_fd[0])
        self.assertTrue(replacement_is_open)
        self.assertEqual(close_counts[first_closed_fd[0]], 1)
        self.assertTrue(all(count == 1 for count in close_counts.values()))
        self.assertEqual(tuple(directory.iterdir()), ())
        self.assertEqual(
            tuple(self._projection(spool).values()),
            (
                "closed",
                row["spool_length"],
                1,
                0,
                0,
                1,
                row["spool_length"],
                None,
            ),
        )
        self.assertIsNone(spool.close())
        marker = marker.with_traceback(None)

    def test_review_seal_cancellation_never_strands_or_duplicates_ownership(self):
        critical_name = "_install_seal_transition"
        trace_points = []
        outer_points = []
        internal_return_points = []
        public_points = []
        transition_returned = [False]
        internal_returned = [False]

        close_directory = self.data_dir / "seal-close-control"
        close_directory.mkdir(mode=0o700)
        close_spool = self.storage._open_historical_window_exchange_spool(
            data_dir=close_directory
        )
        _close_arguments, close_receipt = self._commit(close_spool)
        close_length = dict(close_receipt)["spool_length"]
        close_marker = asyncio.CancelledError("seal writer close control")
        real_close = os.close
        close_raised = [False]

        def closing_then_cancelling(fd):
            real_close(fd)
            if not close_raised[0]:
                close_raised[0] = True
                raise close_marker

        close_caught = None
        with mock.patch.object(
            self.storage.os, "close", new=closing_then_cancelling
        ):
            try:
                close_spool.seal()
            except BaseException as error:
                close_caught = error
        close_projection = tuple(self._projection(close_spool).values())
        close_members = tuple(close_directory.iterdir())
        for member in close_members:
            member.unlink()
        self.assertIs(close_caught, close_marker)
        self.assertEqual(
            close_projection,
            ("closed", close_length, 1, 0, 0, 1, close_length, None),
        )
        self.assertEqual(close_members, ())
        self.assertIsNone(close_spool.close())
        close_marker = close_marker.with_traceback(None)

        def discover(frame, event, _argument):
            if (
                frame.f_code.co_name == critical_name
                and event in ("line", "return")
            ):
                point = (event, frame.f_lineno)
                if point not in trace_points:
                    trace_points.append(point)
                if event == "return":
                    transition_returned[0] = True
            elif (
                transition_returned[0]
                and frame.f_code.co_name == "_seal_spool"
                and event == "line"
            ):
                point = (event, frame.f_lineno)
                if point not in outer_points:
                    outer_points.append(point)
            if frame.f_code.co_name == "_seal_spool" and event == "return":
                point = (event, frame.f_lineno)
                if point not in internal_return_points:
                    internal_return_points.append(point)
                internal_returned[0] = True
            elif (
                internal_returned[0]
                and frame.f_code.co_name == "seal"
                and event == "line"
            ):
                point = (event, frame.f_lineno)
                if point not in public_points:
                    public_points.append(point)
            return discover

        probe_directory = self.data_dir / "seal-boundary-probe"
        probe_directory.mkdir(mode=0o700)
        probe = self.storage._open_historical_window_exchange_spool(
            data_dir=probe_directory
        )
        _probe_arguments, probe_receipt = self._commit(probe)
        probe_length = dict(probe_receipt)["spool_length"]
        prior_trace = sys.gettrace()
        try:
            sys.settrace(discover)
            probe_sealed = probe.seal()
        finally:
            sys.settrace(prior_trace)
        expected_digest = self._projection(probe_sealed)[
            "receipt_inventory_sha256"
        ]
        probe_sealed.close()
        self.assertGreater(len(trace_points), 0)
        self.assertGreater(len(outer_points), 0)

        targets = tuple(("transition", point) for point in trace_points) + tuple(
            ("post_transition", point) for point in outer_points
        ) + tuple(
            ("internal_return", point) for point in internal_return_points
        ) + tuple(
            ("public_delivery", point) for point in public_points
        )
        for index, (scope, point) in enumerate(targets):
            with self.subTest(scope=scope, trace_point=point):
                directory = self.data_dir / "seal-boundary-{}".format(index)
                directory.mkdir(mode=0o700)
                spool = self.storage._open_historical_window_exchange_spool(
                    data_dir=directory
                )
                arguments = _valid_transfer_arguments()
                transfer = self.storage._issue_historical_window_exchange_transfer_for_test(
                    spool=spool, **arguments
                )
                pending = spool.append_transfer(transfer=transfer)
                receipt = spool.commit_transfer(
                    transfer=transfer, pending_receipt=pending
                )
                marker = asyncio.CancelledError(
                    "seal trace point {}".format(point)
                )
                fired = [False]
                helper_returned = [False]
                seal_returned = [False]

                def tracer(frame, event, _argument):
                    if frame.f_code.co_name == critical_name and event == "return":
                        helper_returned[0] = True
                    if frame.f_code.co_name == "_seal_spool" and event == "return":
                        seal_returned[0] = True
                    transition_match = (
                        scope == "transition"
                        and frame.f_code.co_name == critical_name
                        and (event, frame.f_lineno) == point
                    )
                    outer_match = (
                        scope == "post_transition"
                        and helper_returned[0]
                        and frame.f_code.co_name == "_seal_spool"
                        and (event, frame.f_lineno) == point
                    )
                    internal_return_match = (
                        scope == "internal_return"
                        and frame.f_code.co_name == "_seal_spool"
                        and (event, frame.f_lineno) == point
                    )
                    public_match = (
                        scope == "public_delivery"
                        and seal_returned[0]
                        and frame.f_code.co_name == "seal"
                        and (event, frame.f_lineno) == point
                    )
                    if not fired[0] and (
                        transition_match
                        or outer_match
                        or internal_return_match
                        or public_match
                    ):
                        fired[0] = True
                        raise marker
                    return tracer

                sealed = None
                caught = None
                prior_trace = sys.gettrace()
                try:
                    sys.settrace(tracer)
                    sealed = spool.seal()
                except BaseException as error:
                    caught = error
                finally:
                    sys.settrace(prior_trace)
                if sealed is not None:
                    sealed.close()
                self.assertTrue(fired[0])
                self.assertIs(caught, marker)
                self.assertIsNone(sealed)
                projection = tuple(self._projection(spool).values())
                self.assertIn(
                    projection,
                    (
                        (
                            "closed",
                            probe_length,
                            1,
                            0,
                            0,
                            1,
                            probe_length,
                            None,
                        ),
                        (
                            "closed",
                            probe_length,
                            1,
                            0,
                            0,
                            1,
                            probe_length,
                            expected_digest,
                        ),
                    ),
                )
                self.assertEqual(tuple(directory.iterdir()), ())
                self.assertIsNone(spool.close())
                with self.assertRaises(
                    self.storage.HistoricalFoundryStorageError
                ):
                    dict(receipt)
                marker = marker.with_traceback(None)
        self.assertGreater(len(internal_return_points), 0)
        self.assertGreater(len(public_points), 0)


class HistoricalFoundryStorageFailureTests(unittest.TestCase):
    def setUp(self):
        self.storage = importlib.import_module("scripts.historical_foundry_storage")
        self.temporary = tempfile.TemporaryDirectory(dir="/private/tmp")
        self.data_dir = Path(self.temporary.name)
        os.chmod(str(self.data_dir), 0o700)

    def tearDown(self):
        self.temporary.cleanup()

    def _new_directory(self, label):
        directory = self.data_dir / label
        directory.mkdir(mode=0o700)
        return directory

    def _open(self, label="spool"):
        directory = self._new_directory(label)
        spool = self.storage._open_historical_window_exchange_spool(
            data_dir=directory
        )
        return directory, spool

    def _issue(self, spool, **overrides):
        return self.storage._issue_historical_window_exchange_transfer_for_test(
            spool=spool, **_valid_transfer_arguments(**overrides)
        )

    def _append(self, spool, **overrides):
        transfer = self._issue(spool, **overrides)
        pending = spool.append_transfer(transfer=transfer)
        return transfer, pending

    def _commit(self, spool, **overrides):
        transfer, pending = self._append(spool, **overrides)
        return spool.commit_transfer(
            transfer=transfer, pending_receipt=pending
        )

    def _projection(self, spool):
        return dict(
            self.storage._project_historical_window_exchange_spool_for_test(
                spool_or_sealed=spool
            )
        )

    def _assert_closed(
        self,
        spool,
        *,
        committed_bytes=0,
        committed_members=0,
        provisional_bytes=0,
        provisional_members=0,
        receipts=0,
        committed_eof=0,
    ):
        self.assertEqual(
            tuple(self._projection(spool).values()),
            (
                "closed",
                committed_bytes,
                committed_members,
                provisional_bytes,
                provisional_members,
                receipts,
                committed_eof,
                None,
            ),
        )
        self.assertIsNone(spool.close())

    def test_append_postwrite_failure_rolls_back_tail_then_terminalizes(self):
        directory, spool = self._open()
        transfer = self._issue(spool)
        real_pread = os.pread
        failed = [False]

        def fail_first_read(fd, length, offset):
            if not failed[0]:
                failed[0] = True
                raise OSError("postwrite reread sentinel")
            return real_pread(fd, length, offset)

        with mock.patch.object(
            self.storage.os, "pread", side_effect=fail_first_read
        ):
            with self.assertRaises(self.storage.HistoricalFoundryStorageError):
                spool.append_transfer(transfer=transfer)
        self.assertEqual(tuple(directory.iterdir()), ())
        self._assert_closed(spool)

    def test_failed_rollback_partial_tail_is_size_agnostic_unlinked_without_credit(self):
        directory, spool = self._open()
        transfer = self._issue(spool)
        frame_length = 16 + len(b'{"id":1}') + len(b'[{"id":1,"result":"0x1"}]')
        real_pwrite = os.pwrite
        wrote_partial = [False]

        def partial_then_fail(fd, value, offset):
            if not wrote_partial[0]:
                wrote_partial[0] = True
                count = min(3, len(value))
                return real_pwrite(fd, value[:count], offset)
            raise OSError("partial write sentinel")

        with mock.patch.object(
            self.storage.os, "pwrite", side_effect=partial_then_fail
        ), mock.patch.object(
            self.storage.os, "ftruncate", side_effect=OSError("rollback sentinel")
        ):
            with self.assertRaises(self.storage.HistoricalFoundryStorageError):
                spool.append_transfer(transfer=transfer)
        self.assertEqual(tuple(directory.iterdir()), ())
        self._assert_closed(
            spool,
            provisional_bytes=frame_length,
            provisional_members=1,
        )

    def test_abort_failure_terminalizes_without_credit(self):
        directory, spool = self._open()
        committed = self._commit(spool)
        committed_row = dict(committed)
        transfer, pending = self._append(
            spool,
            exchange_index=2,
            request_ids=(2,),
            response_ids=(2,),
            request_bytes=b"abort-failure-request",
            decoded_bytes=b"abort-failure-response",
        )
        pending_row = self._projection(spool)
        with mock.patch.object(
            self.storage.os, "ftruncate", side_effect=OSError("abort sentinel")
        ):
            with self.assertRaises(self.storage.HistoricalFoundryStorageError):
                spool.abort_transfer(
                    transfer=transfer, pending_receipt=pending
                )
        self.assertEqual(tuple(directory.iterdir()), ())
        self._assert_closed(
            spool,
            committed_bytes=committed_row["spool_length"],
            committed_members=1,
            provisional_bytes=pending_row["provisional_physical_bytes"],
            provisional_members=1,
            receipts=1,
            committed_eof=committed_row["spool_length"],
        )

    def test_short_write_short_read_and_fsync_failures_are_terminal(self):
        scenarios = (
            ("write", "pwrite", lambda *_args, **_kwargs: 0, True),
            ("read", "pread", lambda *_args, **_kwargs: b"", True),
            (
                "fsync",
                "fsync",
                lambda *_args, **_kwargs: (_ for _ in ()).throw(
                    OSError("fsync sentinel")
                ),
                False,
            ),
        )
        frame_length = 16 + len(b'{"id":1}') + len(b'[{"id":1,"result":"0x1"}]')
        for label, boundary, failure, rollback_succeeds in scenarios:
            with self.subTest(label=label):
                directory, spool = self._open(label)
                transfer = self._issue(spool)
                with mock.patch.object(
                    self.storage.os, boundary, side_effect=failure
                ):
                    with self.assertRaises(
                        self.storage.HistoricalFoundryStorageError
                    ):
                        spool.append_transfer(transfer=transfer)
                self.assertEqual(tuple(directory.iterdir()), ())
                self._assert_closed(
                    spool,
                    provisional_bytes=(0 if rollback_succeeds else frame_length),
                    provisional_members=(0 if rollback_succeeds else 1),
                )

    def test_ordinary_exception_text_cause_context_and_paths_are_sanitized(self):
        directory, spool = self._open()
        member = tuple(directory.iterdir())[0]
        transfer = self._issue(spool)
        secret = "SECRET_SENTINEL_/private/tmp/provider?token=credential"
        with mock.patch.object(
            self.storage.os, "pwrite", side_effect=OSError(secret)
        ):
            with self.assertRaises(
                self.storage.HistoricalFoundryStorageError
            ) as caught:
                spool.append_transfer(transfer=transfer)
        error = caught.exception
        self.assertEqual(str(error), "historical foundry storage failed")
        self.assertEqual(repr(error), "HistoricalFoundryStorageError(<redacted>)")
        self.assertIsNone(error.__cause__)
        self.assertIsNone(error.__context__)
        rendered = " ".join((str(error), repr(error), repr(error.args)))
        self.assertNotIn(secret, rendered)
        self.assertNotIn(str(member), rendered)
        self.assertNotIn(member.name, rendered)
        self.assertEqual(tuple(directory.iterdir()), ())
        self._assert_closed(spool)

        close_directory, close_spool = self._open("close")
        self._issue(close_spool)
        close_member = tuple(close_directory.iterdir())[0]
        with mock.patch.object(
            self.storage.os, "unlink", side_effect=OSError(secret)
        ):
            with self.assertRaises(self.storage.HistoricalFoundryStorageError):
                close_spool.close()
        self.assertTrue(close_member.exists())
        self._assert_closed(close_spool)
        close_member.unlink()

    def test_control_flow_baseexceptions_cleanup_then_propagate_identically(self):
        controls = (
            KeyboardInterrupt("keyboard sentinel"),
            SystemExit("system sentinel"),
            GeneratorExit("generator sentinel"),
            asyncio.CancelledError("cancelled sentinel"),
        )
        for index, control in enumerate(controls):
            with self.subTest(control=type(control).__name__):
                directory, spool = self._open("control{}".format(index))
                transfer = self._issue(spool)
                with mock.patch.object(
                    self.storage.os, "pwrite", side_effect=control
                ):
                    with self.assertRaises(type(control)) as caught:
                        spool.append_transfer(transfer=transfer)
                self.assertIs(caught.exception, control)
                self.assertEqual(tuple(directory.iterdir()), ())
                self._assert_closed(spool)

class HistoricalFoundryStorageTask4bOwnerMoveTests(unittest.TestCase):
    def test_task4b_post_consume_surface_drift_terminalizes_before_source_issue(self):
        from tests.test_historical_foundry_scan import (
            _Task4bOfflineCapabilityFixture,
        )

        storage = importlib.import_module(
            "scripts.historical_foundry_storage"
        )
        scan = importlib.import_module("scripts.historical_foundry_scan")
        rpc = importlib.import_module("scripts.historical_foundry_rpc")
        fixture = _Task4bOfflineCapabilityFixture()
        view = None
        original = storage._HistoricalWindowCaptureReplaySource.__next__
        original_exported = storage._TASK4B_STORAGE_LOCAL_SURFACE_OBJECTS

        def replacement(self):
            return original(self)

        replacement_exported = list(original_exported)
        replacement_exported[4] = replacement
        source_bind_calls = [0]
        prior_trace = sys.gettrace()

        def tracer(frame, event, _argument):
            if (
                frame.f_code.co_filename == storage.__file__
                and frame.f_code.co_name
                == "_bind_reconciliation_from_bound_scan"
                and event == "call"
            ):
                source_bind_calls[0] += 1
            return tracer

        try:
            capability = fixture.mint()
            view = storage.consume_production_historical_window_capability(
                capability=capability
            )
            fixture.capability = None
            storage._HistoricalWindowCaptureReplaySource.__next__ = replacement
            storage._TASK4B_STORAGE_LOCAL_SURFACE_OBJECTS = tuple(
                replacement_exported
            )
            sys.settrace(tracer)
            with self.assertRaises(rpc._ArchiveRpcError) as caught:
                view._materialize_staging_snapshot_from_bound_scan()
        finally:
            sys.settrace(prior_trace)
            storage._HistoricalWindowCaptureReplaySource.__next__ = original
            storage._TASK4B_STORAGE_LOCAL_SURFACE_OBJECTS = original_exported
        try:
            self.assertEqual(
                (caught.exception.reason_code, caught.exception.failure_kind),
                ("authority_mismatch", "final_identity_drift"),
            )
            self.assertEqual(source_bind_calls[0], 0)
            self.assertIsNone(view.close())
            self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
        finally:
            fixture.close()

    def test_task4b_cross_owner_source_bind_consumes_first_attempt(self):
        from tests.test_historical_foundry_scan import (
            _Task4bOfflineCapabilityFixture,
        )

        storage = importlib.import_module(
            "scripts.historical_foundry_storage"
        )
        rpc = importlib.import_module("scripts.historical_foundry_rpc")
        scan = importlib.import_module("scripts.historical_foundry_scan")
        first = _Task4bOfflineCapabilityFixture()
        second = _Task4bOfflineCapabilityFixture()
        second_view = None
        source_class = storage._HistoricalWindowCaptureReplaySource
        original_method = source_class._bind_reconciliation_from_bound_scan
        original_exported = storage._TASK4B_STORAGE_LOCAL_SURFACE_OBJECTS
        observation = {
            "interception_calls": 0,
            "cross_pair": None,
            "association_calls": 0,
        }
        prior_trace = sys.gettrace()

        def interception(
            self, *, expected_view, expected_reconciliation
        ):
            observation["interception_calls"] += 1
            source_class._bind_reconciliation_from_bound_scan = (
                original_method
            )
            storage._TASK4B_STORAGE_LOCAL_SURFACE_OBJECTS = original_exported
            try:
                original_method(
                    self,
                    expected_view=second_view,
                    expected_reconciliation=expected_reconciliation,
                )
            except rpc._ArchiveRpcError as caught:
                observation["cross_pair"] = (
                    caught.reason_code, caught.failure_kind
                )
                raise
            raise AssertionError("cross-owner source bind was accepted")

        interception_exported = list(original_exported)
        interception_exported[2] = interception
        interception_exported = tuple(interception_exported)
        armed = [False]

        def tracer(frame, event, argument):
            if (
                frame.f_code.co_filename == scan.__file__
                and frame.f_code.co_name
                == "_install_task4b_capture_replay_association"
                and event == "call"
            ):
                observation["association_calls"] += 1
            if (
                not armed[0]
                and frame.f_code.co_filename == storage.__file__
                and frame.f_code.co_name == "_prepare_handle"
                and event == "return"
                and type(argument) is source_class
            ):
                armed[0] = True
                source_class._bind_reconciliation_from_bound_scan = (
                    interception
                )
                storage._TASK4B_STORAGE_LOCAL_SURFACE_OBJECTS = (
                    interception_exported
                )
            return tracer

        snapshot = None
        error = None
        try:
            first_capability = first.mint()
            second_capability = second.mint()
            second_view = (
                storage.consume_production_historical_window_capability(
                    capability=second_capability
                )
            )
            second.capability = None
            sys.settrace(tracer)
            try:
                scan._materialize_historical_window_staging_snapshot(
                    capability=first_capability
                )
            except BaseException as caught:
                error = caught
        finally:
            sys.settrace(prior_trace)
            source_class._bind_reconciliation_from_bound_scan = (
                original_method
            )
            storage._TASK4B_STORAGE_LOCAL_SURFACE_OBJECTS = original_exported
        try:
            self.assertEqual(observation["interception_calls"], 1)
            self.assertEqual(
                observation["cross_pair"],
                (
                    "authority_mismatch",
                    "historical_window_capability_invalid",
                ),
            )
            self.assertEqual(observation["association_calls"], 0)
            self.assertIs(type(error), rpc._ArchiveRpcError)
            self.assertEqual(
                (error.reason_code, error.failure_kind),
                (
                    "authority_mismatch",
                    "historical_window_capability_invalid",
                ),
            )
            self.assertIsNone(second_view.close())
            self.assertEqual(tuple(first.data_dir.iterdir()), ())
            self.assertEqual(tuple(second.data_dir.iterdir()), ())
            error = None
            first_capability = None
            first.capability = None
            second_view = None
        finally:
            first.close()
            second.close()

    def test_task4b_source_protocol_misuse_is_sticky_before_bind(self):
        from tests.test_historical_foundry_scan import (
            _Task4bOfflineCapabilityFixture,
        )

        storage = importlib.import_module(
            "scripts.historical_foundry_storage"
        )
        rpc = importlib.import_module("scripts.historical_foundry_rpc")
        scan = importlib.import_module("scripts.historical_foundry_scan")
        operations = (
            ("enter", lambda source: source.__enter__()),
            ("iter", lambda source: iter(source)),
            ("next", lambda source: next(source)),
        )
        source_class = storage._HistoricalWindowCaptureReplaySource
        original_method = source_class._bind_reconciliation_from_bound_scan
        original_exported = storage._TASK4B_STORAGE_LOCAL_SURFACE_OBJECTS
        invalid_pair = (
            "authority_mismatch",
            "historical_window_capability_invalid",
        )
        for label, operation in operations:
            with self.subTest(operation=label):
                fixture = _Task4bOfflineCapabilityFixture()
                observation = {
                    "interception_calls": 0,
                    "misuse_pair": None,
                    "bind_after_misuse_pair": None,
                }
                prior_trace = sys.gettrace()

                def interception(
                    self, *, expected_view, expected_reconciliation
                ):
                    observation["interception_calls"] += 1
                    source_class._bind_reconciliation_from_bound_scan = (
                        original_method
                    )
                    storage._TASK4B_STORAGE_LOCAL_SURFACE_OBJECTS = (
                        original_exported
                    )
                    try:
                        operation(self)
                    except rpc._ArchiveRpcError as caught:
                        observation["misuse_pair"] = (
                            caught.reason_code, caught.failure_kind
                        )
                    try:
                        original_method(
                            self,
                            expected_view=expected_view,
                            expected_reconciliation=expected_reconciliation,
                        )
                    except rpc._ArchiveRpcError as caught:
                        observation["bind_after_misuse_pair"] = (
                            caught.reason_code, caught.failure_kind
                        )
                        raise
                    raise AssertionError(
                        "misused source unexpectedly bound"
                    )

                interception_exported = list(original_exported)
                interception_exported[2] = interception
                interception_exported = tuple(interception_exported)
                armed = [False]

                def tracer(frame, event, argument):
                    if (
                        not armed[0]
                        and frame.f_code.co_filename == storage.__file__
                        and frame.f_code.co_name == "_prepare_handle"
                        and event == "return"
                        and type(argument) is source_class
                    ):
                        armed[0] = True
                        source_class._bind_reconciliation_from_bound_scan = (
                            interception
                        )
                        storage._TASK4B_STORAGE_LOCAL_SURFACE_OBJECTS = (
                            interception_exported
                        )
                    return tracer

                error = None
                capability = None
                try:
                    capability = fixture.mint()
                    sys.settrace(tracer)
                    try:
                        snapshot = scan._materialize_historical_window_staging_snapshot(
                            capability=capability
                        )
                        self.assertIsNone(snapshot.close())
                        self.assertIsNone(snapshot.close())
                    except BaseException as caught:
                        error = caught
                finally:
                    sys.settrace(prior_trace)
                    source_class._bind_reconciliation_from_bound_scan = (
                        original_method
                    )
                    storage._TASK4B_STORAGE_LOCAL_SURFACE_OBJECTS = (
                        original_exported
                    )
                try:
                    self.assertEqual(observation["interception_calls"], 1)
                    self.assertEqual(
                        observation["misuse_pair"], invalid_pair
                    )
                    self.assertEqual(
                        observation["bind_after_misuse_pair"], invalid_pair
                    )
                    self.assertIs(type(error), rpc._ArchiveRpcError)
                    self.assertEqual(
                        (error.reason_code, error.failure_kind), invalid_pair
                    )
                    self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
                    error = None
                    capability = None
                    fixture.capability = None
                finally:
                    fixture.close()

    def test_task4b_view_moves_whole_owner_binds_source_once_and_terminalizes(self):
        from tests.test_historical_foundry_scan import (
            _Task4bOfflineCapabilityFixture,
        )

        storage = importlib.import_module(
            "scripts.historical_foundry_storage"
        )
        rpc = importlib.import_module("scripts.historical_foundry_rpc")
        scan = importlib.import_module("scripts.historical_foundry_scan")
        signature = inspect.signature(
            storage._ConsumedProductionHistoricalWindowCapabilityView
            ._materialize_staging_snapshot_from_bound_scan
        )
        self.assertEqual(tuple(signature.parameters), ("self",))
        self.assertIs(
            signature.parameters["self"].kind,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        )

        fixture = _Task4bOfflineCapabilityFixture()
        source_class = storage._HistoricalWindowCaptureReplaySource
        original_method = source_class._bind_reconciliation_from_bound_scan
        original_exported = storage._TASK4B_STORAGE_LOCAL_SURFACE_OBJECTS
        observation = {
            "consume_calls": 0,
            "materialize_calls": 0,
            "consumed_owner_id": None,
            "consumed_quota_id": None,
            "consumed_generation": None,
            "materializing_owner_id": None,
            "materializing_quota_id": None,
            "materializing_generation": None,
            "capture_generation": None,
            "checker_calls": 0,
            "source_bind_calls": 0,
            "source_allocations": 0,
            "successful_source_binds": 0,
            "interception_calls": 0,
            "repeat_pair": None,
            "clone_pair": None,
            "snapshot_allocations": 0,
            "snapshot_type": None,
        }
        prior_trace = sys.gettrace()

        def interception(
            self, *, expected_view, expected_reconciliation
        ):
            observation["interception_calls"] += 1
            source_class._bind_reconciliation_from_bound_scan = (
                original_method
            )
            storage._TASK4B_STORAGE_LOCAL_SURFACE_OBJECTS = original_exported
            original_method(
                self,
                expected_view=expected_view,
                expected_reconciliation=expected_reconciliation,
            )
            observation["successful_source_binds"] += 1
            try:
                original_method(
                    self,
                    expected_view=expected_view,
                    expected_reconciliation=expected_reconciliation,
                )
            except rpc._ArchiveRpcError as caught:
                observation["repeat_pair"] = (
                    caught.reason_code, caught.failure_kind
                )
            clone = object.__new__(source_class)
            try:
                original_method(
                    clone,
                    expected_view=expected_view,
                    expected_reconciliation=expected_reconciliation,
                )
            except rpc._ArchiveRpcError as caught:
                observation["clone_pair"] = (
                    caught.reason_code, caught.failure_kind
                )
            clone = None
            raise RuntimeError("stop after source one-shot probes")

        interception_exported = list(original_exported)
        interception_exported[2] = interception
        interception_exported = tuple(interception_exported)
        armed = [False]

        def tracer(frame, event, argument):
            if frame.f_code.co_filename != storage.__file__:
                return tracer
            name = frame.f_code.co_name
            if (
                name == "consume_production_historical_window_capability"
                and event == "call"
            ):
                observation["consume_calls"] += 1
            elif (
                name == "_materialize_staging_snapshot_from_bound_scan"
                and event == "call"
            ):
                observation["materialize_calls"] += 1
            elif (
                name == "_verify_task4b_bound_source_current"
                and event == "call"
            ):
                observation["checker_calls"] += 1
            if name == "_prepare_handle" and event == "return":
                if type(argument) is source_class:
                    observation["source_allocations"] += 1
                    if not armed[0]:
                        armed[0] = True
                        source_class._bind_reconciliation_from_bound_scan = (
                            interception
                        )
                        storage._TASK4B_STORAGE_LOCAL_SURFACE_OBJECTS = (
                            interception_exported
                        )
                elif type(argument) is storage.HistoricalRunStagingSnapshot:
                    observation["snapshot_allocations"] += 1
            owner = frame.f_locals.get("owner")
            if type(owner) is dict:
                if (
                    owner.get("state") == "consumed_view"
                    and name
                    == "_consume_production_historical_window_capability_core"
                    and event == "return"
                ):
                    observation["consumed_owner_id"] = id(owner)
                    observation["consumed_quota_id"] = id(
                        owner.get("quota")
                    )
                    observation["consumed_generation"] = owner.get(
                        "owner_generation"
                    )
                if owner.get("state") == "capture_materializing":
                    observation["materializing_owner_id"] = id(owner)
                    observation["materializing_quota_id"] = id(
                        owner.get("quota")
                    )
                    observation["materializing_generation"] = owner.get(
                        "owner_generation"
                    )
                    observation["capture_generation"] = owner.get(
                        "capture_generation"
                    )
            if (
                name == "_bind_reconciliation_from_bound_scan"
                and event == "call"
            ):
                observation["source_bind_calls"] += 1
            return tracer

        error = None
        result = None
        capability = None
        try:
            capability = fixture.mint()
            sys.settrace(tracer)
            try:
                result = scan._materialize_historical_window_staging_snapshot(
                    capability=capability
                )
            except BaseException as caught:
                error = caught
        finally:
            sys.settrace(prior_trace)
            source_class._bind_reconciliation_from_bound_scan = (
                original_method
            )
            storage._TASK4B_STORAGE_LOCAL_SURFACE_OBJECTS = original_exported
        try:
            invalid_pair = (
                "authority_mismatch",
                "historical_window_capability_invalid",
            )
            self.assertIsNone(result)
            self.assertIs(type(error), rpc._ArchiveRpcError)
            self.assertEqual(
                (error.reason_code, error.failure_kind),
                (
                    "authority_mismatch",
                    "historical_window_spool_handoff_failed",
                ),
            )
            self.assertIsNone(error.__context__)
            self.assertIsNone(error.__cause__)
            self.assertEqual(observation["consume_calls"], 1)
            self.assertEqual(observation["materialize_calls"], 1)
            self.assertIs(type(observation["consumed_generation"]), int)
            self.assertEqual(
                observation["materializing_generation"],
                observation["consumed_generation"] + 1,
            )
            self.assertEqual(observation["capture_generation"], 0)
            self.assertEqual(
                observation["materializing_owner_id"],
                observation["consumed_owner_id"],
            )
            self.assertEqual(
                observation["materializing_quota_id"],
                observation["consumed_quota_id"],
            )
            self.assertNotEqual(
                observation["consumed_quota_id"], id(None)
            )
            self.assertGreaterEqual(observation["checker_calls"], 3)
            self.assertEqual(observation["source_allocations"], 1)
            self.assertEqual(observation["interception_calls"], 1)
            self.assertEqual(observation["source_bind_calls"], 3)
            self.assertEqual(observation["successful_source_binds"], 1)
            self.assertEqual(observation["repeat_pair"], invalid_pair)
            self.assertEqual(observation["clone_pair"], invalid_pair)
            self.assertEqual(observation["snapshot_allocations"], 0)
            self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
            error = None
            capability = None
            fixture.capability = None
        finally:
            fixture.close()


class _HistoricalFoundryStorageTask4bSlice3Mixin:
    def _new_capability(self):
        from tests.test_historical_foundry_scan import (
            _Task4bOfflineCapabilityFixture,
        )

        fixture = _Task4bOfflineCapabilityFixture()
        capability = fixture.mint()
        return fixture, capability

    def _materialize(self, capability):
        try:
            return self.scan._materialize_historical_window_staging_snapshot(
                capability=capability
            )
        finally:
            capability = None

    def _assert_spool_handoff_failure(self, operation):
        return self._assert_pair(
            operation, "historical_window_spool_handoff_failed"
        )

    def _assert_pair(self, operation, failure_kind):
        with self.assertRaises(self.rpc._ArchiveRpcError) as caught:
            operation()
        self.assertEqual(
            (caught.exception.reason_code, caught.exception.failure_kind),
            (
                "authority_mismatch",
                failure_kind,
            ),
        )

    def _run_detached_config_validator_case(
        self, source_name, transform
    ):
        fixture, capability = self._new_capability()
        original_open = self.storage.os.open
        original_pread = self.storage.os.pread
        fd_names = {}
        observation = {
            "mutated_reads": 0,
            "mutated_digest": None,
            "decode_calls": 0,
            "decode_exceptions": 0,
            "validation_exceptions": 0,
            "finalization_calls": 0,
            "output_opens": 0,
        }
        prior_trace = sys.gettrace()

        def observed_open(path, flags, mode=0o777, *, dir_fd=None):
            fd = original_open(path, flags, mode, dir_fd=dir_fd)
            fd_names[fd] = path
            if path in ("policy.json", "authority.json", "toolchain.json"):
                observation["output_opens"] += 1
            return fd

        def detached_pread(fd, length, offset):
            payload = original_pread(fd, length, offset)
            if fd_names.get(fd) == source_name and offset == 0 and payload:
                mutated = transform(payload)
                self.assertIs(type(mutated), bytes)
                self.assertEqual(len(mutated), len(payload))
                observation["mutated_reads"] += 1
                observation["mutated_digest"] = hashlib.sha256(
                    mutated
                ).hexdigest()
                return mutated
            return payload

        def tracer(frame, event, argument):
            if frame.f_code.co_filename != self.storage.__file__:
                return tracer
            name = frame.f_code.co_name
            if name == "_task4b_decode_canonical_config":
                if event == "call":
                    observation["decode_calls"] += 1
                elif event == "exception":
                    observation["decode_exceptions"] += 1
            elif name == "_task4b_validate_config_set" and event == "exception":
                observation["validation_exceptions"] += 1
            elif (
                name == "_task4b_finalization_config_identity"
                and event == "call"
            ):
                observation["finalization_calls"] += 1
            return tracer

        try:
            with mock.patch.object(
                self.storage.os, "open", side_effect=observed_open
            ) as patched_open:
                supported = set(self.storage.os.supports_dir_fd)
                supported.discard(original_open)
                supported.add(patched_open)
                with mock.patch.object(
                    self.storage.os, "supports_dir_fd", supported
                ), mock.patch.object(
                    self.storage.os, "pread", side_effect=detached_pread
                ):
                    sys.settrace(tracer)
                    self._assert_pair(
                        lambda: self._materialize(capability),
                        "final_identity_drift",
                    )
            fixture.capability = None
        finally:
            sys.settrace(prior_trace)
        try:
            self.assertGreaterEqual(observation["mutated_reads"], 1)
            self.assertEqual(observation["output_opens"], 0)
            self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
            return observation
        finally:
            fixture.close()


class HistoricalFoundryStorageTask4bConfigTests(
    _HistoricalFoundryStorageTask4bSlice3Mixin,
    unittest.TestCase,
):
    def setUp(self):
        self.storage = importlib.import_module(
            "scripts.historical_foundry_storage"
        )
        self.rpc = importlib.import_module("scripts.historical_foundry_rpc")
        self.scan = importlib.import_module("scripts.historical_foundry_scan")

    def test_actual_held_root_configs_copy_exact_bytes_before_slice4_stop(self):
        fixture, capability = self._new_capability()
        snapshot = None
        original_open = self.storage.os.open
        original_pwrite = self.storage.os.pwrite
        opened = []
        fd_names = {}
        written = {}
        config_sources = (
            "historical_foundry_replay_policy.json",
            "historical_foundry_replay_authority.json",
            "historical_foundry_replay_toolchain.json",
        )
        staging_names = ("policy.json", "authority.json", "toolchain.json")

        def observed_open(path, flags, mode=0o777, *, dir_fd=None):
            fd = original_open(path, flags, mode, dir_fd=dir_fd)
            if path == "config" or path in config_sources + staging_names:
                opened.append(
                    (
                        path,
                        flags,
                        mode,
                        dir_fd,
                        self.storage.os.get_inheritable(fd),
                    )
                )
                fd_names[fd] = path
            return fd

        def observed_pwrite(fd, payload, offset):
            count = original_pwrite(fd, payload, offset)
            name = fd_names.get(fd)
            if name in staging_names:
                current = bytearray(written.get(name, b""))
                if len(current) < offset + count:
                    current.extend(b"\0" * (offset + count - len(current)))
                current[offset:offset + count] = payload[:count]
                written[name] = bytes(current)
            return count

        try:
            with mock.patch.object(
                self.storage.os, "open", side_effect=observed_open
            ) as patched_open:
                supported = set(self.storage.os.supports_dir_fd)
                supported.discard(original_open)
                supported.add(patched_open)
                with mock.patch.object(
                    self.storage.os, "supports_dir_fd", supported
                ), mock.patch.object(
                    self.storage.os, "pwrite", side_effect=observed_pwrite
                ):
                    snapshot = self._materialize(capability)
            fixture.capability = None
            source_rows = [row for row in opened if row[0] in config_sources]
            self.assertEqual(tuple(row[0] for row in source_rows), config_sources)
            source_flags = (
                self.storage.os.O_RDONLY
                | self.storage.os.O_NOFOLLOW
                | self.storage.os.O_CLOEXEC
            )
            self.assertTrue(all(
                row[1] == source_flags and row[4] is False
                for row in source_rows
            ))
            config_row = next(row for row in opened if row[0] == "config")
            self.assertEqual(
                config_row[1],
                source_flags | self.storage.os.O_DIRECTORY,
            )
            self.assertIs(config_row[4], False)
            repo_root = Path(self.rpc.__file__).resolve().parents[1]
            expected = {
                "policy.json": (
                    repo_root / "config/historical_foundry_replay_policy.json"
                ).read_bytes(),
                "authority.json": (
                    repo_root / "config/historical_foundry_replay_authority.json"
                ).read_bytes(),
                "toolchain.json": (
                    repo_root / "config/historical_foundry_replay_toolchain.json"
                ).read_bytes(),
            }
            self.assertEqual(written, expected)
            output_rows = [row for row in opened if row[0] in staging_names]
            output_flags = (
                self.storage.os.O_RDWR
                | self.storage.os.O_CREAT
                | self.storage.os.O_EXCL
                | self.storage.os.O_NOFOLLOW
                | self.storage.os.O_CLOEXEC
            )
            create_rows = [
                row for row in output_rows if row[1] == output_flags
            ]
            freeze_flags = (
                self.storage.os.O_RDONLY
                | self.storage.os.O_NOFOLLOW
                | self.storage.os.O_CLOEXEC
            )
            freeze_reread_rows = [
                row for row in output_rows if row[1] == freeze_flags
            ]
            self.assertEqual(
                tuple(row[0] for row in create_rows), staging_names
            )
            self.assertTrue(all(
                row[1] == output_flags
                and row[2] == 0o600
                and row[4] is False
                for row in create_rows
            ))
            self.assertEqual(
                tuple(row[0] for row in freeze_reread_rows), staging_names
            )
            self.assertTrue(all(
                row[1] == freeze_flags and row[4] is False
                for row in freeze_reread_rows
            ))
            self.assertEqual(
                len(create_rows) + len(freeze_reread_rows),
                len(output_rows),
            )
            self.assertIsNone(snapshot.close())
            snapshot = None
            self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
        finally:
            if snapshot is not None:
                snapshot.close()
            fixture.close()

    def test_duplicate_nonfinite_and_noncanonical_bytes_reach_decoder(self):
        mutations = (
            (
                "duplicate",
                b'"lookback_seconds"',
                b'"authority_sha256"',
            ),
            (
                "nonfinite",
                b'"lookback_seconds":604800',
                b'"lookback_seconds":NaN   ',
            ),
            ("noncanonical_lf", b"\n", b" "),
        )
        for label, original_token, replacement_token in mutations:
            with self.subTest(case=label):
                def transform(payload):
                    self.assertIn(original_token, payload)
                    return payload.replace(
                        original_token, replacement_token, 1
                    )

                observation = self._run_detached_config_validator_case(
                    "historical_foundry_replay_policy.json", transform
                )
                self.assertEqual(observation["decode_calls"], 1)
                self.assertEqual(observation["decode_exceptions"], 1)
                self.assertEqual(observation["finalization_calls"], 0)
                self.assertGreaterEqual(
                    observation["validation_exceptions"], 1
                )

    def test_schema_mutation_passes_decoder_and_rejects_before_hash_check(self):
        def transform(payload):
            original = b"historical_foundry_replay_policy/v1"
            replacement = b"historical_foundry_replay_policy/v2"
            self.assertIn(original, payload)
            return payload.replace(original, replacement, 1)

        observation = self._run_detached_config_validator_case(
            "historical_foundry_replay_policy.json", transform
        )
        self.assertEqual(observation["decode_calls"], 3)
        self.assertEqual(observation["decode_exceptions"], 0)
        self.assertEqual(observation["finalization_calls"], 0)
        self.assertGreaterEqual(observation["validation_exceptions"], 1)

    def test_cross_bind_rejects_after_real_hashes_match_finalization(self):
        fixture, capability = self._new_capability()
        observation = {
            "decode_calls": 0,
            "detached_mutations": 0,
            "finalization_calls": 0,
            "validation_exceptions": 0,
            "output_opens": 0,
        }
        original_open = self.storage.os.open
        prior_trace = sys.gettrace()

        def observed_open(path, flags, mode=0o777, *, dir_fd=None):
            fd = original_open(path, flags, mode, dir_fd=dir_fd)
            if path in ("policy.json", "authority.json", "toolchain.json"):
                observation["output_opens"] += 1
            return fd

        def tracer(frame, event, _argument):
            if frame.f_code.co_filename != self.storage.__file__:
                return tracer
            name = frame.f_code.co_name
            if name == "_task4b_decode_canonical_config" and event == "call":
                observation["decode_calls"] += 1
            elif (
                name == "_task4b_validate_config_set"
                and event == "line"
                and observation["detached_mutations"] == 0
            ):
                values = frame.f_locals.get("values")
                if type(values) is dict and type(values.get("policy")) is dict:
                    observed = values["policy"].get("authority_sha256")
                    if type(observed) is str and len(observed) == 64:
                        values["policy"]["authority_sha256"] = (
                            "0" * 64 if observed != "0" * 64 else "1" * 64
                        )
                        observation["detached_mutations"] += 1
            elif (
                name == "_task4b_finalization_config_identity"
                and event == "call"
            ):
                observation["finalization_calls"] += 1
            elif name == "_task4b_validate_config_set" and event == "exception":
                observation["validation_exceptions"] += 1
            return tracer

        try:
            with mock.patch.object(
                self.storage.os, "open", side_effect=observed_open
            ) as patched_open:
                supported = set(self.storage.os.supports_dir_fd)
                supported.discard(original_open)
                supported.add(patched_open)
                with mock.patch.object(
                    self.storage.os, "supports_dir_fd", supported
                ):
                    sys.settrace(tracer)
                    self._assert_pair(
                        lambda: self._materialize(capability),
                        "final_identity_drift",
                    )
            fixture.capability = None
        finally:
            sys.settrace(prior_trace)
        try:
            self.assertEqual(observation["decode_calls"], 3)
            self.assertEqual(observation["detached_mutations"], 1)
            self.assertEqual(observation["finalization_calls"], 1)
            self.assertGreaterEqual(observation["validation_exceptions"], 1)
            self.assertEqual(observation["output_opens"], 0)
            self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
        finally:
            fixture.close()

    def test_config_directory_identity_is_rechecked_during_source_reads(self):
        fixture, capability = self._new_capability()
        original_stat = self.storage.os.stat
        config_stats = [0]
        injected = [False]

        class StatProxy:
            def __init__(self, value, *, inode):
                self._value = value
                self.st_ino = inode

            def __getattr__(self, name):
                return getattr(self._value, name)

        def drifting_stat(path, *args, **kwargs):
            value = original_stat(path, *args, **kwargs)
            if path == "config" and type(kwargs.get("dir_fd")) is int:
                config_stats[0] += 1
                if config_stats[0] == 3:
                    injected[0] = True
                    return StatProxy(value, inode=value.st_ino + 1)
            return value

        try:
            with mock.patch.object(
                self.storage.os, "stat", side_effect=drifting_stat
            ) as patched_stat:
                dir_fd = set(self.storage.os.supports_dir_fd)
                dir_fd.discard(original_stat)
                dir_fd.add(patched_stat)
                nofollow = set(self.storage.os.supports_follow_symlinks)
                nofollow.discard(original_stat)
                nofollow.add(patched_stat)
                with mock.patch.object(
                    self.storage.os, "supports_dir_fd", dir_fd
                ), mock.patch.object(
                    self.storage.os,
                    "supports_follow_symlinks",
                    nofollow,
                ):
                    self._assert_pair(
                        lambda: self._materialize(capability),
                        "final_identity_drift",
                    )
            fixture.capability = None
            self.assertTrue(injected[0])
            self.assertGreaterEqual(config_stats[0], 3)
            self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
        finally:
            fixture.close()

    def test_source_size_link_owner_and_write_mode_are_rejected(self):
        scenarios = (
            ("hardlink", {"st_nlink": 2}),
            ("wrong_owner", {"st_uid": os.geteuid() + 1}),
            ("group_write", {"st_mode": stat.S_IFREG | 0o664}),
            ("nonregular", {"st_mode": stat.S_IFDIR | 0o755}),
        )
        for label, replacements in scenarios:
            with self.subTest(case=label):
                fixture, capability = self._new_capability()
                original_open = self.storage.os.open
                original_stat = self.storage.os.stat
                original_fstat = self.storage.os.fstat
                fd_names = {}
                injected = [0]

                class StatProxy:
                    def __init__(self, value):
                        self._value = value
                        for key, replacement in replacements.items():
                            setattr(self, key, replacement)

                    def __getattr__(self, name):
                        return getattr(self._value, name)

                def observed_open(path, flags, mode=0o777, *, dir_fd=None):
                    fd = original_open(path, flags, mode, dir_fd=dir_fd)
                    fd_names[fd] = path
                    return fd

                def detached_stat(path, *args, **kwargs):
                    value = original_stat(path, *args, **kwargs)
                    if path == "historical_foundry_replay_policy.json":
                        injected[0] += 1
                        return StatProxy(value)
                    return value

                def detached_fstat(fd):
                    value = original_fstat(fd)
                    if fd_names.get(fd) == (
                        "historical_foundry_replay_policy.json"
                    ):
                        injected[0] += 1
                        return StatProxy(value)
                    return value

                try:
                    with mock.patch.object(
                        self.storage.os, "open", side_effect=observed_open
                    ) as patched_open, mock.patch.object(
                        self.storage.os, "stat", side_effect=detached_stat
                    ) as patched_stat, mock.patch.object(
                        self.storage.os, "fstat", side_effect=detached_fstat
                    ), mock.patch.object(
                        self.storage.os, "urandom", wraps=self.storage.os.urandom
                    ) as entropy:
                        dir_fd = set(self.storage.os.supports_dir_fd)
                        dir_fd.discard(original_open)
                        dir_fd.discard(original_stat)
                        dir_fd.update((patched_open, patched_stat))
                        nofollow = set(
                            self.storage.os.supports_follow_symlinks
                        )
                        nofollow.discard(original_stat)
                        nofollow.add(patched_stat)
                        with mock.patch.object(
                            self.storage.os, "supports_dir_fd", dir_fd
                        ), mock.patch.object(
                            self.storage.os,
                            "supports_follow_symlinks",
                            nofollow,
                        ):
                            self._assert_pair(
                                lambda: self._materialize(capability),
                                "final_identity_drift",
                            )
                    fixture.capability = None
                    self.assertGreaterEqual(injected[0], 2)
                    entropy.assert_not_called()
                    self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
                finally:
                    fixture.close()

    def test_source_size_cap_accepts_exact_and_rejects_plus_one_before_pread(self):
        for label, detached_size, expected_pread in (
            ("exact_cap", 1_048_576, ((1_048_576, 0),)),
            ("plus_one", 1_048_577, ()),
        ):
            with self.subTest(case=label):
                fixture, capability = self._new_capability()
                original_open = self.storage.os.open
                original_stat = self.storage.os.stat
                original_fstat = self.storage.os.fstat
                original_pread = self.storage.os.pread
                fd_names = {}
                pread_calls = []

                class StatProxy:
                    def __init__(self, value):
                        self._value = value
                        self.st_size = detached_size

                    def __getattr__(self, name):
                        return getattr(self._value, name)

                def observed_open(path, flags, mode=0o777, *, dir_fd=None):
                    fd = original_open(path, flags, mode, dir_fd=dir_fd)
                    fd_names[fd] = path
                    return fd

                def detached_stat(path, *args, **kwargs):
                    value = original_stat(path, *args, **kwargs)
                    if path == "historical_foundry_replay_policy.json":
                        return StatProxy(value)
                    return value

                def detached_fstat(fd):
                    value = original_fstat(fd)
                    if fd_names.get(fd) == (
                        "historical_foundry_replay_policy.json"
                    ):
                        return StatProxy(value)
                    return value

                def observed_pread(fd, length, offset):
                    if fd_names.get(fd) == (
                        "historical_foundry_replay_policy.json"
                    ):
                        pread_calls.append((length, offset))
                        return b""
                    return original_pread(fd, length, offset)

                try:
                    with mock.patch.object(
                        self.storage.os, "open", side_effect=observed_open
                    ) as patched_open, mock.patch.object(
                        self.storage.os, "stat", side_effect=detached_stat
                    ) as patched_stat, mock.patch.object(
                        self.storage.os, "fstat", side_effect=detached_fstat
                    ), mock.patch.object(
                        self.storage.os, "pread", side_effect=observed_pread
                    ):
                        dir_fd = set(self.storage.os.supports_dir_fd)
                        dir_fd.discard(original_open)
                        dir_fd.discard(original_stat)
                        dir_fd.update((patched_open, patched_stat))
                        nofollow = set(
                            self.storage.os.supports_follow_symlinks
                        )
                        nofollow.discard(original_stat)
                        nofollow.add(patched_stat)
                        with mock.patch.object(
                            self.storage.os, "supports_dir_fd", dir_fd
                        ), mock.patch.object(
                            self.storage.os,
                            "supports_follow_symlinks",
                            nofollow,
                        ):
                            self._assert_pair(
                                lambda: self._materialize(capability),
                                "final_identity_drift",
                            )
                    fixture.capability = None
                    self.assertEqual(tuple(pread_calls), expected_pread)
                    self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
                finally:
                    fixture.close()

    def test_each_initial_and_independent_reread_identity_point_rejects(self):
        points = tuple(("fstat", count) for count in range(2, 6)) + tuple(
            ("stat", count) for count in range(3, 7)
        )
        for operation, target_count in points:
            with self.subTest(operation=operation, count=target_count):
                fixture, capability = self._new_capability()
                original_open = self.storage.os.open
                original_stat = self.storage.os.stat
                original_fstat = self.storage.os.fstat
                fd_names = {}
                counts = {"stat": 0, "fstat": 0}
                injected = [False]

                class StatProxy:
                    def __init__(self, value):
                        self._value = value
                        self.st_ino = value.st_ino + 1

                    def __getattr__(self, name):
                        return getattr(self._value, name)

                def observed_open(path, flags, mode=0o777, *, dir_fd=None):
                    fd = original_open(path, flags, mode, dir_fd=dir_fd)
                    fd_names[fd] = path
                    return fd

                def temporal_stat(path, *args, **kwargs):
                    value = original_stat(path, *args, **kwargs)
                    if path == "historical_foundry_replay_policy.json":
                        counts["stat"] += 1
                        if operation == "stat" and counts["stat"] == target_count:
                            injected[0] = True
                            return StatProxy(value)
                    return value

                def temporal_fstat(fd):
                    value = original_fstat(fd)
                    if fd_names.get(fd) == (
                        "historical_foundry_replay_policy.json"
                    ):
                        counts["fstat"] += 1
                        if (
                            operation == "fstat"
                            and counts["fstat"] == target_count
                        ):
                            injected[0] = True
                            return StatProxy(value)
                    return value

                try:
                    with mock.patch.object(
                        self.storage.os, "open", side_effect=observed_open
                    ) as patched_open, mock.patch.object(
                        self.storage.os, "stat", side_effect=temporal_stat
                    ) as patched_stat, mock.patch.object(
                        self.storage.os, "fstat", side_effect=temporal_fstat
                    ):
                        dir_fd = set(self.storage.os.supports_dir_fd)
                        dir_fd.discard(original_open)
                        dir_fd.discard(original_stat)
                        dir_fd.update((patched_open, patched_stat))
                        nofollow = set(
                            self.storage.os.supports_follow_symlinks
                        )
                        nofollow.discard(original_stat)
                        nofollow.add(patched_stat)
                        with mock.patch.object(
                            self.storage.os, "supports_dir_fd", dir_fd
                        ), mock.patch.object(
                            self.storage.os,
                            "supports_follow_symlinks",
                            nofollow,
                        ):
                            self._assert_pair(
                                lambda: self._materialize(capability),
                                "final_identity_drift",
                            )
                    fixture.capability = None
                    self.assertTrue(injected[0])
                    self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
                finally:
                    fixture.close()

    def test_repository_root_identity_is_rechecked_after_config_open(self):
        fixture, capability = self._new_capability()
        original_stat = self.storage.os.stat
        root_stats = [0]
        injected = [False]

        class StatProxy:
            def __init__(self, value):
                self._value = value
                self.st_ino = value.st_ino + 1

            def __getattr__(self, name):
                return getattr(self._value, name)

        def drifting_stat(path, *args, **kwargs):
            value = original_stat(path, *args, **kwargs)
            if path == "." and type(kwargs.get("dir_fd")) is int:
                root_stats[0] += 1
                if root_stats[0] == 3:
                    injected[0] = True
                    return StatProxy(value)
            return value

        try:
            with mock.patch.object(
                self.storage.os, "stat", side_effect=drifting_stat
            ) as patched_stat:
                dir_fd = set(self.storage.os.supports_dir_fd)
                dir_fd.discard(original_stat)
                dir_fd.add(patched_stat)
                nofollow = set(self.storage.os.supports_follow_symlinks)
                nofollow.discard(original_stat)
                nofollow.add(patched_stat)
                with mock.patch.object(
                    self.storage.os, "supports_dir_fd", dir_fd
                ), mock.patch.object(
                    self.storage.os,
                    "supports_follow_symlinks",
                    nofollow,
                ):
                    self._assert_pair(
                        lambda: self._materialize(capability),
                        "final_identity_drift",
                    )
            fixture.capability = None
            self.assertTrue(injected[0])
            self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
        finally:
            fixture.close()

    def test_materialization_never_uses_loader_cwd_or_path_reads(self):
        import scripts.historical_foundry_contracts as contracts

        fixture, capability = self._new_capability()
        snapshot = None
        try:
            with mock.patch(
                "builtins.open",
                side_effect=AssertionError("generic open reached Slice3"),
            ) as generic_open, mock.patch.object(
                Path,
                "read_bytes",
                side_effect=AssertionError("Path.read_bytes reached Slice3"),
            ) as path_read, mock.patch.object(
                self.storage.os,
                "getcwd",
                side_effect=AssertionError("cwd reached Slice3"),
            ) as getcwd, mock.patch.object(
                contracts,
                "load_historical_foundry_config_set",
                side_effect=AssertionError("config loader reached Slice3"),
            ) as loader:
                snapshot = self._materialize(capability)
                self.assertIsNone(snapshot.close())
                snapshot = None
            fixture.capability = None
            generic_open.assert_not_called()
            path_read.assert_not_called()
            getcwd.assert_not_called()
            loader.assert_not_called()
            self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
        finally:
            if snapshot is not None:
                snapshot.close()
            fixture.close()


class HistoricalFoundryStorageTask4bFilesystemTests(
    _HistoricalFoundryStorageTask4bSlice3Mixin,
    unittest.TestCase,
):
    def setUp(self):
        self.storage = importlib.import_module(
            "scripts.historical_foundry_storage"
        )
        self.rpc = importlib.import_module("scripts.historical_foundry_rpc")
        self.scan = importlib.import_module("scripts.historical_foundry_scan")

    def test_private_tree_is_no_replace_exact_mode_and_fully_cleaned(self):
        fixture, capability = self._new_capability()
        snapshot = None
        original_mkdir = self.storage.os.mkdir
        original_open = self.storage.os.open
        mkdir_rows = []
        directory_open_rows = []
        entropy_calls = []
        expected_names = (
            "raw",
            "historical-foundry-replay",
            ".staging-" + "12" * 16,
            "rpc",
            "headers",
            "reserves",
            "prices",
            "fees",
            "scan",
        )

        def observed_mkdir(path, mode=0o777, *, dir_fd=None):
            mkdir_rows.append((path, mode, dir_fd))
            return original_mkdir(path, mode, dir_fd=dir_fd)

        def observed_open(path, flags, mode=0o777, *, dir_fd=None):
            fd = original_open(path, flags, mode, dir_fd=dir_fd)
            if path in expected_names:
                directory_open_rows.append(
                    (
                        path,
                        flags,
                        dir_fd,
                        self.storage.os.get_inheritable(fd),
                    )
                )
            return fd

        def fixed_entropy(size):
            entropy_calls.append(size)
            return b"\x12" * size

        try:
            with mock.patch.object(
                self.storage.os, "mkdir", side_effect=observed_mkdir
            ) as patched_mkdir, mock.patch.object(
                self.storage.os, "open", side_effect=observed_open
            ) as patched_open:
                supported = set(self.storage.os.supports_dir_fd)
                supported.discard(original_mkdir)
                supported.discard(original_open)
                supported.update((patched_mkdir, patched_open))
                with mock.patch.object(
                    self.storage.os, "supports_dir_fd", supported
                ), mock.patch.object(
                    self.storage.os, "urandom", side_effect=fixed_entropy
                ):
                    snapshot = self._materialize(capability)
            fixture.capability = None
            self.assertEqual(tuple(row[0] for row in mkdir_rows), expected_names)
            self.assertTrue(all(row[1] == 0o700 for row in mkdir_rows))
            self.assertTrue(all(type(row[2]) is int for row in mkdir_rows))
            self.assertEqual(
                tuple(row[0] for row in directory_open_rows), expected_names
            )
            directory_flags = (
                self.storage.os.O_RDONLY
                | self.storage.os.O_DIRECTORY
                | self.storage.os.O_NOFOLLOW
                | self.storage.os.O_CLOEXEC
            )
            self.assertTrue(all(
                row[1] == directory_flags
                and type(row[2]) is int
                and row[3] is False
                for row in directory_open_rows
            ))
            self.assertEqual(entropy_calls, [16])
            self.assertIsNone(snapshot.close())
            snapshot = None
            self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
        finally:
            if snapshot is not None:
                snapshot.close()
            fixture.close()

    def test_private_ledger_binds_lineage_and_hidden_basename_only(self):
        fixture, capability = self._new_capability()
        snapshot = None
        observed = {
            "lineage_exact": False,
            "private_basename": None,
            "directory_count": None,
            "file_count": None,
        }
        prior_trace = sys.gettrace()

        def tracer(frame, event, _argument):
            if (
                event == "return"
                and frame.f_code.co_filename == self.storage.__file__
                and frame.f_code.co_name == "_task4b_prepare_config_staging"
            ):
                owner = frame.f_locals.get("owner")
                ledger = (
                    owner.get("_task4b_staging")
                    if type(owner) is dict else None
                )
                if type(ledger) is dict:
                    observed["lineage_exact"] = (
                        ledger.get("lineage") is owner.get("lineage")
                    )
                    observed["private_basename"] = ledger.get(
                        "private_basename"
                    )
                    directories = ledger.get("directories")
                    files = ledger.get("files")
                    observed["directory_count"] = (
                        len(directories) if type(directories) is list else None
                    )
                    observed["file_count"] = (
                        len(files) if type(files) is list else None
                    )
            return tracer

        try:
            sys.settrace(tracer)
            with mock.patch.object(
                self.storage.os, "urandom", return_value=b"\xab" * 16
            ):
                snapshot = self._materialize(capability)
            fixture.capability = None
        finally:
            sys.settrace(prior_trace)
        try:
            self.assertTrue(observed["lineage_exact"])
            self.assertEqual(
                observed["private_basename"],
                ".staging-" + "ab" * 16,
            )
            self.assertEqual(observed["directory_count"], 9)
            self.assertEqual(observed["file_count"], 3)
            self.assertIsNone(snapshot.close())
            snapshot = None
            self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
        finally:
            if snapshot is not None:
                snapshot.close()
            fixture.close()

    def test_staging_collision_is_not_replaced_or_deleted(self):
        fixture, capability = self._new_capability()
        raw = fixture.data_dir / "raw"
        replay = raw / "historical-foundry-replay"
        collision = replay / (".staging-" + "34" * 16)
        collision.mkdir(parents=True, mode=0o700)
        marker = collision / "attacker-marker"
        marker.write_bytes(b"retain")
        os.chmod(str(raw), 0o700)
        os.chmod(str(replay), 0o700)
        os.chmod(str(collision), 0o700)
        try:
            with mock.patch.object(
                self.storage.os, "urandom", return_value=b"\x34" * 16
            ) as entropy:
                self._assert_spool_handoff_failure(
                    lambda: self._materialize(capability)
                )
            fixture.capability = None
            entropy.assert_called_once_with(16)
            self.assertEqual(marker.read_bytes(), b"retain")
        finally:
            fixture.close()

    def test_safe_preexisting_raw_and_replay_roots_are_reused_not_removed(self):
        fixture, capability = self._new_capability()
        snapshot = None
        raw = fixture.data_dir / "raw"
        replay = raw / "historical-foundry-replay"
        replay.mkdir(parents=True, mode=0o700)
        os.chmod(str(raw), 0o700)
        os.chmod(str(replay), 0o700)
        try:
            with mock.patch.object(
                self.storage.os, "urandom", return_value=b"\x9a" * 16
            ) as entropy:
                snapshot = self._materialize(capability)
            fixture.capability = None
            entropy.assert_called_once_with(16)
            self.assertIsNone(snapshot.close())
            snapshot = None
            self.assertTrue(raw.is_dir())
            self.assertTrue(replay.is_dir())
            self.assertEqual(tuple(replay.iterdir()), ())
            self.assertEqual(tuple(raw.iterdir()), (replay,))
        finally:
            if snapshot is not None:
                snapshot.close()
            fixture.close()

    def test_unsafe_or_symlink_raw_root_rejects_before_entropy(self):
        for kind in ("group_write", "symlink"):
            with self.subTest(kind=kind):
                fixture, capability = self._new_capability()
                raw = fixture.data_dir / "raw"
                external = fixture.data_dir / "external"
                if kind == "group_write":
                    raw.mkdir(mode=0o700)
                    os.chmod(str(raw), 0o720)
                else:
                    external.mkdir(mode=0o700)
                    raw.symlink_to(external, target_is_directory=True)
                try:
                    with mock.patch.object(
                        self.storage.os,
                        "urandom",
                        wraps=self.storage.os.urandom,
                    ) as entropy:
                        self._assert_spool_handoff_failure(
                            lambda: self._materialize(capability)
                        )
                    fixture.capability = None
                    entropy.assert_not_called()
                    if kind == "group_write":
                        self.assertTrue(raw.is_dir())
                    else:
                        self.assertTrue(raw.is_symlink())
                        self.assertTrue(external.is_dir())
                finally:
                    fixture.close()

    def test_body_control_survives_config_cleanup_with_exact_identity(self):
        controls = (
            KeyboardInterrupt("slice3 keyboard sentinel"),
            SystemExit("slice3 system sentinel"),
            GeneratorExit("slice3 generator sentinel"),
            asyncio.CancelledError("slice3 cancelled sentinel"),
        )
        for control in controls:
            with self.subTest(control=type(control).__name__):
                fixture, capability = self._new_capability()
                original_open = self.storage.os.open
                original_fsync = self.storage.os.fsync
                fd_names = {}
                raised = [False]

                def observed_open(path, flags, mode=0o777, *, dir_fd=None):
                    fd = original_open(path, flags, mode, dir_fd=dir_fd)
                    fd_names[fd] = path
                    return fd

                def controlled_fsync(fd):
                    if fd_names.get(fd) == "policy.json" and not raised[0]:
                        raised[0] = True
                        raise control
                    return original_fsync(fd)

                try:
                    with mock.patch.object(
                        self.storage.os, "open", side_effect=observed_open
                    ) as patched_open:
                        supported = set(self.storage.os.supports_dir_fd)
                        supported.discard(original_open)
                        supported.add(patched_open)
                        with mock.patch.object(
                            self.storage.os, "supports_dir_fd", supported
                        ), mock.patch.object(
                            self.storage.os,
                            "fsync",
                            side_effect=controlled_fsync,
                        ):
                            with self.assertRaises(type(control)) as caught:
                                self._materialize(capability)
                    fixture.capability = None
                    self.assertIs(caught.exception, control)
                    self.assertTrue(raised[0])
                    self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
                finally:
                    fixture.close()

    def test_unexpected_role_member_rejects_before_source_issue_and_is_retained(self):
        fixture, capability = self._new_capability()
        original_open = self.storage.os.open
        source_allocations = [0]
        inserted = [False]
        prior_trace = sys.gettrace()

        def observed_open(path, flags, mode=0o777, *, dir_fd=None):
            fd = original_open(path, flags, mode, dir_fd=dir_fd)
            if path == "scan" and flags & self.storage.os.O_DIRECTORY:
                attacker_fd = original_open(
                    "unexpected.bin",
                    self.storage.os.O_RDWR
                    | self.storage.os.O_CREAT
                    | self.storage.os.O_EXCL
                    | self.storage.os.O_NOFOLLOW
                    | self.storage.os.O_CLOEXEC,
                    0o600,
                    dir_fd=fd,
                )
                self.storage.os.close(attacker_fd)
                inserted[0] = True
            return fd

        def tracer(frame, event, argument):
            if (
                event == "return"
                and frame.f_code.co_filename == self.storage.__file__
                and frame.f_code.co_name == "_prepare_handle"
                and type(argument)
                is self.storage._HistoricalWindowCaptureReplaySource
            ):
                source_allocations[0] += 1
            return tracer

        try:
            with mock.patch.object(
                self.storage.os, "open", side_effect=observed_open
            ) as patched_open, mock.patch.object(
                self.storage.os, "urandom", return_value=b"\x56" * 16
            ):
                supported = set(self.storage.os.supports_dir_fd)
                supported.discard(original_open)
                supported.add(patched_open)
                with mock.patch.object(
                    self.storage.os, "supports_dir_fd", supported
                ):
                    sys.settrace(tracer)
                    self._assert_spool_handoff_failure(
                        lambda: self._materialize(capability)
                    )
            fixture.capability = None
        finally:
            sys.settrace(prior_trace)
        try:
            unexpected = (
                fixture.data_dir
                / "raw"
                / "historical-foundry-replay"
                / (".staging-" + "56" * 16)
                / "scan"
                / "unexpected.bin"
            )
            self.assertTrue(inserted[0])
            self.assertEqual(source_allocations[0], 0)
            self.assertTrue(unexpected.is_file())
        finally:
            fixture.close()

    def test_partial_config_write_failure_removes_known_private_tree(self):
        fixture, capability = self._new_capability()
        original_open = self.storage.os.open
        original_pwrite = self.storage.os.pwrite
        fd_names = {}
        partial = [False]
        failed = [False]

        def observed_open(path, flags, mode=0o777, *, dir_fd=None):
            fd = original_open(path, flags, mode, dir_fd=dir_fd)
            fd_names[fd] = path
            return fd

        def failing_pwrite(fd, payload, offset):
            if fd_names.get(fd) == "policy.json":
                if not partial[0]:
                    partial[0] = True
                    prefix = max(1, len(payload) // 2)
                    return original_pwrite(fd, payload[:prefix], offset)
                failed[0] = True
                raise OSError("slice3 injected partial write failure")
            return original_pwrite(fd, payload, offset)

        try:
            with mock.patch.object(
                self.storage.os, "open", side_effect=observed_open
            ) as patched_open:
                supported = set(self.storage.os.supports_dir_fd)
                supported.discard(original_open)
                supported.add(patched_open)
                with mock.patch.object(
                    self.storage.os, "supports_dir_fd", supported
                ), mock.patch.object(
                    self.storage.os, "pwrite", side_effect=failing_pwrite
                ):
                    self._assert_spool_handoff_failure(
                        lambda: self._materialize(capability)
                    )
            fixture.capability = None
            self.assertTrue(partial[0])
            self.assertTrue(failed[0])
            self.assertEqual(tuple(fixture.data_dir.iterdir()), ())

            opens = [0]

            def repeat_open(*args, **kwargs):
                opens[0] += 1
                return original_open(*args, **kwargs)

            with mock.patch.object(
                self.storage.os, "open", side_effect=repeat_open
            ) as patched_open:
                supported = set(self.storage.os.supports_dir_fd)
                supported.discard(original_open)
                supported.add(patched_open)
                with mock.patch.object(
                    self.storage.os, "supports_dir_fd", supported
                ):
                    self._assert_pair(
                        lambda: self._materialize(capability),
                        "historical_window_capability_invalid",
                    )
            self.assertEqual(opens[0], 0)
        finally:
            fixture.close()

    def test_natural_descriptor_reuse_leaves_zero_fd_delta(self):
        before = frozenset(
            int(name) for name in os.listdir("/dev/fd")
            if name.isdigit()
        )
        fixture, capability = self._new_capability()
        snapshot = None
        try:
            snapshot = self._materialize(capability)
            self.assertIsNone(snapshot.close())
            snapshot = None
            self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
        finally:
            if snapshot is not None:
                snapshot.close()
            fixture.close()
            capability = None
        gc.collect()
        after = frozenset(
            int(name) for name in os.listdir("/dev/fd")
            if name.isdigit()
        )
        self.assertEqual(after, before)

    def test_line_control_after_capture_file_open_closes_and_cleans(self):
        before = frozenset(
            int(name) for name in os.listdir("/dev/fd")
            if name.isdigit()
        )
        fixture, capability = self._new_capability()
        control = KeyboardInterrupt("slice3 post-file-open sentinel")
        fired = [False]
        prior_trace = sys.gettrace()

        def tracer(frame, event, _argument):
            if (
                not fired[0]
                and event == "line"
                and frame.f_code.co_filename == self.storage.__file__
                and frame.f_code.co_name == "_task4b_write_capture_config"
                and type(frame.f_locals.get("fd")) is int
            ):
                fired[0] = True
                raise control
            return tracer

        try:
            sys.settrace(tracer)
            with self.assertRaises(KeyboardInterrupt) as caught:
                self._materialize(capability)
        finally:
            sys.settrace(prior_trace)
        try:
            self.assertTrue(fired[0])
            self.assertIs(caught.exception, control)
            self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
            capability.close()
            capability.close()
        finally:
            fixture.close()
            capability = None
        gc.collect()
        after = frozenset(
            int(name) for name in os.listdir("/dev/fd")
            if name.isdigit()
        )
        self.assertEqual(after, before)

    def test_each_slice3_open_return_opcode_gap_retains_cleanup_authority(
        self,
    ):
        cases = (
            ("repository_config", "config", 1, "directory"),
            (
                "config_source",
                "historical_foundry_replay_policy.json",
                1,
                "source",
            ),
            ("created_directory", None, 1, "directory"),
            ("existing_directory", "raw", 1, "directory"),
            ("output_config", "policy.json", 1, "output"),
            ("cleanup_reopen", None, 2, "directory"),
        )
        for index, (label, literal_path, occurrence, flag_kind) in enumerate(
            cases
        ):
            with self.subTest(site=label):
                fixture, capability = self._new_capability()
                original_open = self.storage.os.open
                original_fstat = self.storage.os.fstat
                entropy = bytes((0xe0 + index,)) * 16
                staging_name = ".staging-" + entropy.hex()
                target_path = (
                    staging_name if literal_path is None else literal_path
                )
                control = KeyboardInterrupt(
                    "slice3 open-return opcode sentinel " + label
                )
                observation = {
                    "target_calls": 0,
                    "returned_fd": None,
                    "call_args": None,
                    "call_kwargs": None,
                    "armed": False,
                    "opcode_fired": False,
                }
                prior_trace = sys.gettrace()

                if label == "existing_directory":
                    raw = fixture.data_dir / "raw"
                    replay = raw / "historical-foundry-replay"
                    replay.mkdir(parents=True, mode=0o700)
                    os.chmod(str(raw), 0o700)
                    os.chmod(str(replay), 0o700)

                def observed_open(*args, **kwargs):
                    path = args[0] if args else None
                    if path == target_path:
                        observation["target_calls"] += 1
                        if (
                            label == "cleanup_reopen"
                            and observation["target_calls"] == 1
                        ):
                            raise OSError(
                                "slice3 initial directory open failure"
                            )
                    fd = original_open(*args, **kwargs)
                    if (
                        path == target_path
                        and observation["target_calls"] == occurrence
                        and observation["returned_fd"] is None
                    ):
                        observation["returned_fd"] = fd
                        observation["call_args"] = tuple(args)
                        observation["call_kwargs"] = dict(kwargs)
                        observation["armed"] = True
                    return fd

                def tracer(frame, event, _argument):
                    if frame.f_code.co_filename == self.storage.__file__:
                        frame.f_trace_opcodes = True
                        if (
                            event == "opcode"
                            and observation["armed"]
                            and not observation["opcode_fired"]
                        ):
                            observation["opcode_fired"] = True
                            raise control
                    return tracer

                escaped = None
                fd_was_live = False
                try:
                    with mock.patch.object(
                        self.storage.os,
                        "open",
                        side_effect=observed_open,
                    ) as patched_open:
                        supported = set(self.storage.os.supports_dir_fd)
                        supported.discard(original_open)
                        supported.add(patched_open)
                        with mock.patch.object(
                            self.storage.os, "supports_dir_fd", supported
                        ), mock.patch.object(
                            self.storage.os,
                            "urandom",
                            return_value=entropy,
                        ):
                            sys.settrace(tracer)
                            try:
                                self._materialize(capability)
                            except BaseException as error:
                                escaped = error
                            finally:
                                sys.settrace(prior_trace)

                    returned_fd = observation["returned_fd"]
                    self.assertIs(type(returned_fd), int)
                    try:
                        original_fstat(returned_fd)
                    except OSError:
                        fd_was_live = False
                    else:
                        fd_was_live = True
                    self.assertFalse(
                        fd_was_live,
                        "open-return fd escaped the registered cleanup slot",
                    )
                    self.assertTrue(observation["opcode_fired"])
                    self.assertIs(escaped, control)
                    self.assertIsNone(control.__context__)

                    call_args = observation["call_args"]
                    call_kwargs = observation["call_kwargs"]
                    self.assertEqual(call_args[0], target_path)
                    self.assertIs(type(call_kwargs.get("dir_fd")), int)
                    directory_flags = (
                        self.storage.os.O_RDONLY
                        | self.storage.os.O_DIRECTORY
                        | self.storage.os.O_NOFOLLOW
                        | self.storage.os.O_CLOEXEC
                    )
                    source_flags = (
                        self.storage.os.O_RDONLY
                        | self.storage.os.O_NOFOLLOW
                        | self.storage.os.O_CLOEXEC
                    )
                    output_flags = (
                        self.storage.os.O_RDWR
                        | self.storage.os.O_CREAT
                        | self.storage.os.O_EXCL
                        | self.storage.os.O_NOFOLLOW
                        | self.storage.os.O_CLOEXEC
                    )
                    expected_flags = {
                        "directory": directory_flags,
                        "source": source_flags,
                        "output": output_flags,
                    }[flag_kind]
                    self.assertEqual(call_args[1], expected_flags)
                    if flag_kind == "output":
                        self.assertEqual(call_args[2:], (0o600,))
                    else:
                        self.assertEqual(call_args[2:], ())

                    if label == "existing_directory":
                        raw = fixture.data_dir / "raw"
                        replay = raw / "historical-foundry-replay"
                        self.assertEqual(
                            tuple(fixture.data_dir.iterdir()), (raw,)
                        )
                        self.assertEqual(tuple(raw.iterdir()), (replay,))
                        self.assertEqual(tuple(replay.iterdir()), ())
                    else:
                        self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
                finally:
                    sys.settrace(prior_trace)
                    returned_fd = observation["returned_fd"]
                    if type(returned_fd) is int:
                        try:
                            original_fstat(returned_fd)
                        except OSError:
                            pass
                        else:
                            os.close(returned_fd)
                    fixture.close()

    def test_slice3_close_pre_call_opcode_gap_closes_exact_fd_once(self):
        fixture, capability = self._new_capability()
        original_open = self.storage.os.open
        original_close = self.storage.os.close
        original_fstat = self.storage.os.fstat
        target = {
            "fd": None,
            "close_calls": [],
            "opcode_fired": False,
        }
        control = KeyboardInterrupt("slice3 close opcode sentinel")
        prior_trace = sys.gettrace()

        def observed_open(*args, **kwargs):
            fd = original_open(*args, **kwargs)
            if args and args[0] == "historical_foundry_replay_policy.json":
                target["fd"] = fd
            return fd

        def observed_close(fd):
            if fd == target["fd"]:
                target["close_calls"].append(fd)
            return original_close(fd)

        def tracer(frame, event, _argument):
            if frame.f_code.co_filename == self.storage.__file__:
                frame.f_trace_opcodes = True
                if (
                    event == "opcode"
                    and not target["opcode_fired"]
                    and frame.f_code.co_name == "_task4b_close_fd_slot"
                    and frame.f_locals.get("fd") == target["fd"]
                    and frame.f_locals.get("slot", {}).get("close_state")
                    == "attempting"
                ):
                    target["opcode_fired"] = True
                    raise control
            return tracer

        escaped = None
        fd_was_live = False
        try:
            with mock.patch.object(
                self.storage.os, "open", side_effect=observed_open
            ) as patched_open, mock.patch.object(
                self.storage.os, "close", side_effect=observed_close
            ) as patched_close:
                supported = set(self.storage.os.supports_dir_fd)
                supported.discard(original_open)
                supported.add(patched_open)
                with mock.patch.object(
                    self.storage.os, "supports_dir_fd", supported
                ):
                    sys.settrace(tracer)
                    try:
                        self._materialize(capability)
                    except BaseException as error:
                        escaped = error
                    finally:
                        sys.settrace(prior_trace)

            target_fd = target["fd"]
            self.assertIs(type(target_fd), int)
            try:
                original_fstat(target_fd)
            except OSError:
                fd_was_live = False
            else:
                fd_was_live = True
            self.assertFalse(
                fd_was_live,
                "close pre-call opcode control leaked the exact registered fd",
            )
            self.assertTrue(target["opcode_fired"])
            self.assertIs(escaped, control)
            self.assertIsNone(control.__context__)
            self.assertEqual(target["close_calls"], [target_fd])
            self.assertEqual(
                patched_close.call_args_list.count(mock.call(target_fd)), 1
            )
            self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
            capability.close()
            capability.close()
        finally:
            sys.settrace(prior_trace)
            target_fd = target["fd"]
            if type(target_fd) is int:
                try:
                    original_fstat(target_fd)
                except OSError:
                    pass
                else:
                    original_close(target_fd)
            fixture.close()

    def test_slice3_close_entered_control_never_retries_numeric_fd(self):
        fixture, capability = self._new_capability()
        original_open = self.storage.os.open
        original_close = self.storage.os.close
        original_fstat = self.storage.os.fstat
        target = {"fd": None, "close_calls": []}
        control = KeyboardInterrupt("slice3 entered-close sentinel")

        def observed_open(*args, **kwargs):
            fd = original_open(*args, **kwargs)
            if args and args[0] == "historical_foundry_replay_policy.json":
                target["fd"] = fd
            return fd

        def close_then_raise(fd):
            if fd == target["fd"]:
                target["close_calls"].append(fd)
                original_close(fd)
                raise control
            return original_close(fd)

        escaped = None
        try:
            with mock.patch.object(
                self.storage.os, "open", side_effect=observed_open
            ) as patched_open, mock.patch.object(
                self.storage.os, "close", side_effect=close_then_raise
            ):
                supported = set(self.storage.os.supports_dir_fd)
                supported.discard(original_open)
                supported.add(patched_open)
                with mock.patch.object(
                    self.storage.os, "supports_dir_fd", supported
                ):
                    try:
                        self._materialize(capability)
                    except BaseException as error:
                        escaped = error

            target_fd = target["fd"]
            self.assertIs(type(target_fd), int)
            with self.assertRaises(OSError):
                original_fstat(target_fd)
            self.assertEqual(target["close_calls"], [target_fd])
            self.assertIs(escaped, control)
            self.assertIsNone(control.__context__)
            self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
            capability.close()
            capability.close()
        finally:
            target_fd = target["fd"]
            if type(target_fd) is int:
                try:
                    original_fstat(target_fd)
                except OSError:
                    pass
                else:
                    original_close(target_fd)
            fixture.close()

    def test_slice3_close_call_line_is_atomic_at_every_opcode(self):
        original_open = self.storage.os.open
        original_close = self.storage.os.close
        original_fstat = self.storage.os.fstat

        def run_materializer(*, injected_offset=None):
            fixture, capability = self._new_capability()
            snapshot = None
            observation = {
                "fd": None,
                "close_entered": False,
                "close_calls": [],
                "events": [],
                "fired": False,
            }
            control = KeyboardInterrupt(
                "slice3 exhaustive close opcode sentinel"
            )
            prior_trace = sys.gettrace()

            def observed_open(*args, **kwargs):
                fd = original_open(*args, **kwargs)
                if (
                    args
                    and args[0]
                    == "historical_foundry_replay_policy.json"
                ):
                    observation["fd"] = fd
                return fd

            def observed_close(*args, **kwargs):
                fd = args[0] if args else kwargs.get("fd")
                if fd == observation["fd"]:
                    observation["close_calls"].append(
                        (tuple(args), dict(kwargs))
                    )
                    observation["close_entered"] = True
                return original_close(*args, **kwargs)

            def tracer(frame, event, _argument):
                if frame.f_code.co_filename == self.storage.__file__:
                    frame.f_trace_opcodes = True
                    if (
                        event == "opcode"
                        and frame.f_code.co_name
                        == "_task4b_close_fd_slot"
                        and frame.f_locals.get("fd") == observation["fd"]
                    ):
                        row = (
                            frame.f_lineno,
                            frame.f_lasti,
                            observation["close_entered"],
                        )
                        observation["events"].append(row)
                        if (
                            injected_offset is not None
                            and not observation["fired"]
                            and frame.f_lineno == injected_offset[0]
                            and frame.f_lasti == injected_offset[1]
                        ):
                            observation["fired"] = True
                            raise control
                return tracer

            escaped = None
            try:
                with mock.patch.object(
                    self.storage.os, "open", side_effect=observed_open
                ) as patched_open, mock.patch.object(
                    self.storage.os, "close", side_effect=observed_close
                ):
                    supported = set(self.storage.os.supports_dir_fd)
                    supported.discard(original_open)
                    supported.add(patched_open)
                    with mock.patch.object(
                        self.storage.os, "supports_dir_fd", supported
                    ):
                        sys.settrace(tracer)
                        try:
                            snapshot = self._materialize(capability)
                            self.assertIsNone(snapshot.close())
                            snapshot = None
                        except BaseException as error:
                            escaped = error
                        finally:
                            sys.settrace(prior_trace)

                target_fd = observation["fd"]
                self.assertIs(type(target_fd), int)
                with self.assertRaises(OSError):
                    original_fstat(target_fd)
                self.assertEqual(
                    observation["close_calls"], [((target_fd,), {})]
                )
                self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
                if injected_offset is not None:
                    self.assertTrue(observation["fired"])
                    self.assertIs(escaped, control)
                    self.assertIsNone(control.__context__)
                return tuple(observation["events"])
            finally:
                sys.settrace(prior_trace)
                if snapshot is not None:
                    snapshot.close()
                target_fd = observation["fd"]
                if type(target_fd) is int:
                    try:
                        original_fstat(target_fd)
                    except OSError:
                        pass
                    else:
                        original_close(target_fd)
                fixture.close()

        baseline = run_materializer()
        post_call_rows = tuple(row for row in baseline if row[2])
        self.assertTrue(post_call_rows)
        close_call_line = post_call_rows[0][0]
        close_call_offsets = tuple(dict.fromkeys(
            (line, offset)
            for line, offset, _entered in baseline
            if line == close_call_line
        ))
        self.assertGreaterEqual(len(close_call_offsets), 3)
        for close_call_offset in close_call_offsets:
            with self.subTest(close_call_offset=close_call_offset):
                run_materializer(injected_offset=close_call_offset)

    def test_slice3_destructive_cleanup_boundaries_resume_safely(self):
        cases = (
            ("unlink", "before", 2, 1),
            ("unlink", "after", 1, 1),
            ("rmdir", "before", 2, 1),
            ("rmdir", "after", 1, 1),
            ("fsync", "before", 2, 1),
            ("fsync", "after", 2, 2),
        )
        for syscall_name, boundary, expected_calls, expected_completions in cases:
            with self.subTest(syscall=syscall_name, boundary=boundary):
                fixture, capability = self._new_capability()
                original_unlink = self.storage.os.unlink
                original_rmdir = self.storage.os.rmdir
                original_fsync = self.storage.os.fsync
                observation = {
                    "calls": 0,
                    "completions": 0,
                    "fsync_armed": False,
                    "fsync_target_fd": None,
                }
                control = KeyboardInterrupt(
                    "slice3 destructive boundary sentinel"
                )

                def controlled_unlink(path, *args, **kwargs):
                    if syscall_name == "unlink" and path == "policy.json":
                        observation["calls"] += 1
                        if boundary == "before" and observation["calls"] == 1:
                            raise control
                        result = original_unlink(path, *args, **kwargs)
                        observation["completions"] += 1
                        if boundary == "after" and observation["calls"] == 1:
                            raise control
                        return result
                    result = original_unlink(path, *args, **kwargs)
                    if syscall_name == "fsync" and path == "policy.json":
                        observation["fsync_armed"] = True
                        observation["fsync_target_fd"] = kwargs.get(
                            "dir_fd"
                        )
                    return result

                def controlled_rmdir(path, *args, **kwargs):
                    if syscall_name == "rmdir" and path == "scan":
                        observation["calls"] += 1
                        if boundary == "before" and observation["calls"] == 1:
                            raise control
                        result = original_rmdir(path, *args, **kwargs)
                        observation["completions"] += 1
                        if boundary == "after" and observation["calls"] == 1:
                            raise control
                        return result
                    return original_rmdir(path, *args, **kwargs)

                def controlled_fsync(fd):
                    if (
                        syscall_name == "fsync"
                        and observation["fsync_armed"]
                        and fd == observation["fsync_target_fd"]
                    ):
                        observation["calls"] += 1
                        if boundary == "before" and observation["calls"] == 1:
                            raise control
                        result = original_fsync(fd)
                        observation["completions"] += 1
                        if boundary == "after" and observation["calls"] == 1:
                            raise control
                        observation["fsync_armed"] = False
                        return result
                    return original_fsync(fd)

                escaped = None
                snapshot = None
                try:
                    with mock.patch.object(
                        self.storage.os,
                        "unlink",
                        side_effect=controlled_unlink,
                    ) as patched_unlink, mock.patch.object(
                        self.storage.os,
                        "rmdir",
                        side_effect=controlled_rmdir,
                    ) as patched_rmdir, mock.patch.object(
                        self.storage.os,
                        "fsync",
                        side_effect=controlled_fsync,
                    ):
                        supported = set(self.storage.os.supports_dir_fd)
                        supported.discard(original_unlink)
                        supported.discard(original_rmdir)
                        supported.update((patched_unlink, patched_rmdir))
                        with mock.patch.object(
                            self.storage.os,
                            "supports_dir_fd",
                            supported,
                        ):
                            try:
                                snapshot = self._materialize(capability)
                                snapshot.close()
                            except BaseException as error:
                                escaped = error

                    self.assertIs(escaped, control)
                    self.assertIsNone(control.__context__)
                    self.assertEqual(observation["calls"], expected_calls)
                    self.assertEqual(
                        observation["completions"], expected_completions
                    )
                    self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
                    self.assertIsNone(snapshot.close())
                    self.assertIsNone(snapshot.close())
                finally:
                    if snapshot is not None:
                        snapshot.close()
                    fixture.close()

    def test_slice3_ordinary_post_unlink_and_rmdir_reconcile_parent_fsync(
        self,
    ):
        cases = (
            ("unlink", "policy.json", RuntimeError("post-unlink sentinel")),
            ("rmdir", "scan", OSError("post-rmdir sentinel")),
        )
        for syscall_name, target_name, ordinary in cases:
            with self.subTest(syscall=syscall_name):
                fixture, capability = self._new_capability()
                snapshot = None
                original_unlink = self.storage.os.unlink
                original_rmdir = self.storage.os.rmdir
                original_fsync = self.storage.os.fsync
                observation = {
                    "calls": 0,
                    "effects": 0,
                    "target_parent_fd": None,
                    "entry_active": False,
                    "parent_fsyncs_before_close": 0,
                    "phase_finished": False,
                }
                prior_trace = sys.gettrace()

                def controlled_unlink(path, *args, **kwargs):
                    if syscall_name == "unlink" and path == target_name:
                        observation["calls"] += 1
                        result = original_unlink(path, *args, **kwargs)
                        observation["effects"] += 1
                        observation["target_parent_fd"] = kwargs.get(
                            "dir_fd"
                        )
                        observation["entry_active"] = True
                        raise ordinary
                    return original_unlink(path, *args, **kwargs)

                def controlled_rmdir(path, *args, **kwargs):
                    if syscall_name == "rmdir" and path == target_name:
                        observation["calls"] += 1
                        result = original_rmdir(path, *args, **kwargs)
                        observation["effects"] += 1
                        observation["target_parent_fd"] = kwargs.get(
                            "dir_fd"
                        )
                        observation["entry_active"] = True
                        raise ordinary
                    return original_rmdir(path, *args, **kwargs)

                def controlled_fsync(fd):
                    if (
                        observation["entry_active"]
                        and fd == observation["target_parent_fd"]
                    ):
                        observation["parent_fsyncs_before_close"] += 1
                    return original_fsync(fd)

                def tracer(frame, event, _argument):
                    if (
                        observation["entry_active"]
                        and event == "line"
                        and frame.f_code.co_filename == self.storage.__file__
                        and frame.f_code.co_name
                        == "_cleanup_task4b_capture_staging"
                    ):
                        entry = frame.f_locals.get("entry")
                        if (
                            type(entry) is dict
                            and entry.get("name") == target_name
                            and entry.get("cleanup_phase") == "close"
                        ):
                            observation["phase_finished"] = True
                            observation["entry_active"] = False
                    return tracer

                try:
                    with mock.patch.object(
                        self.storage.os,
                        "unlink",
                        side_effect=controlled_unlink,
                    ) as patched_unlink, mock.patch.object(
                        self.storage.os,
                        "rmdir",
                        side_effect=controlled_rmdir,
                    ) as patched_rmdir, mock.patch.object(
                        self.storage.os,
                        "fsync",
                        side_effect=controlled_fsync,
                    ):
                        supported = set(self.storage.os.supports_dir_fd)
                        supported.discard(original_unlink)
                        supported.discard(original_rmdir)
                        supported.update((patched_unlink, patched_rmdir))
                        with mock.patch.object(
                            self.storage.os,
                            "supports_dir_fd",
                            supported,
                        ):
                            sys.settrace(tracer)
                            with self.assertRaises(
                                self.storage.HistoricalFoundryStorageError
                            ) as caught:
                                snapshot = self._materialize(capability)
                                snapshot.close()
                            sys.settrace(prior_trace)

                    self.assertIsNone(caught.exception.__context__)
                    self.assertIsNone(caught.exception.__cause__)
                    self.assertEqual(observation["calls"], 1)
                    self.assertEqual(observation["effects"], 1)
                    self.assertTrue(observation["phase_finished"])
                    self.assertEqual(
                        observation["parent_fsyncs_before_close"], 1
                    )
                    self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
                    self.assertIsNone(snapshot.close())
                    self.assertIsNone(snapshot.close())
                    self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
                finally:
                    sys.settrace(prior_trace)
                    if snapshot is not None:
                        snapshot.close()
                    fixture.close()

    def test_created_directory_open_failure_reopens_safely_for_cleanup(self):
        fixture, capability = self._new_capability()
        original_open = self.storage.os.open
        staging_name = ".staging-" + "6d" * 16
        failed = [False]

        def failing_open(path, flags, mode=0o777, *, dir_fd=None):
            if path == staging_name and not failed[0]:
                failed[0] = True
                raise OSError("slice3 injected directory open failure")
            return original_open(path, flags, mode, dir_fd=dir_fd)

        try:
            with mock.patch.object(
                self.storage.os, "open", side_effect=failing_open
            ) as patched_open:
                supported = set(self.storage.os.supports_dir_fd)
                supported.discard(original_open)
                supported.add(patched_open)
                with mock.patch.object(
                    self.storage.os, "supports_dir_fd", supported
                ), mock.patch.object(
                    self.storage.os,
                    "urandom",
                    return_value=b"\x6d" * 16,
                ):
                    self._assert_spool_handoff_failure(
                        lambda: self._materialize(capability)
                    )
            self.assertTrue(failed[0])
            self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
        finally:
            fixture.close()

    def test_created_directory_open_body_controls_win_identity_freeze_control(
        self,
    ):
        controls = (
            KeyboardInterrupt("slice3 directory-open keyboard sentinel"),
            SystemExit("slice3 directory-open system sentinel"),
            GeneratorExit("slice3 directory-open generator sentinel"),
            asyncio.CancelledError(
                "slice3 directory-open cancellation sentinel"
            ),
        )
        for index, control in enumerate(controls):
            with self.subTest(control=type(control).__name__):
                fixture, capability = self._new_capability()
                original_open = self.storage.os.open
                original_stat = self.storage.os.stat
                entropy = bytes((0xa0 + index,)) * 16
                staging_name = ".staging-" + entropy.hex()
                secondary = SystemExit(
                    "slice3 identity-freeze secondary sentinel"
                )
                open_failed = [False]
                stat_failed = [False]

                def controlled_open(
                    path, flags, mode=0o777, *, dir_fd=None
                ):
                    if path == staging_name and not open_failed[0]:
                        open_failed[0] = True
                        raise control
                    return original_open(
                        path, flags, mode, dir_fd=dir_fd
                    )

                def controlled_stat(path, *args, **kwargs):
                    if (
                        open_failed[0]
                        and path == staging_name
                        and type(kwargs.get("dir_fd")) is int
                        and not stat_failed[0]
                    ):
                        stat_failed[0] = True
                        raise secondary
                    return original_stat(path, *args, **kwargs)

                escaped = None
                try:
                    with mock.patch.object(
                        self.storage.os,
                        "open",
                        side_effect=controlled_open,
                    ) as patched_open, mock.patch.object(
                        self.storage.os,
                        "stat",
                        side_effect=controlled_stat,
                    ) as patched_stat:
                        dir_fd = set(self.storage.os.supports_dir_fd)
                        dir_fd.discard(original_open)
                        dir_fd.discard(original_stat)
                        dir_fd.update((patched_open, patched_stat))
                        nofollow = set(
                            self.storage.os.supports_follow_symlinks
                        )
                        nofollow.discard(original_stat)
                        nofollow.add(patched_stat)
                        with mock.patch.object(
                            self.storage.os, "supports_dir_fd", dir_fd
                        ), mock.patch.object(
                            self.storage.os,
                            "supports_follow_symlinks",
                            nofollow,
                        ), mock.patch.object(
                            self.storage.os,
                            "urandom",
                            return_value=entropy,
                        ):
                            try:
                                self._materialize(capability)
                            except BaseException as error:
                                escaped = error
                    self.assertTrue(open_failed[0])
                    self.assertTrue(stat_failed[0])
                    self.assertIs(escaped, control)
                    self.assertIsNone(control.__context__)
                    self.assertIsNone(secondary.__context__)
                    self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
                finally:
                    fixture.close()

    def test_created_directory_open_ordinary_failure_priority_is_sanitized(
        self,
    ):
        cases = (
            (
                "secondary_control",
                OSError("slice3 directory-open ordinary sentinel"),
                KeyboardInterrupt(
                    "slice3 identity-freeze cleanup sentinel"
                ),
            ),
            (
                "secondary_ordinary",
                OSError("slice3 directory-open ordinary sentinel"),
                RuntimeError("slice3 identity-freeze ordinary sentinel"),
            ),
        )
        for index, (label, original, secondary) in enumerate(cases):
            with self.subTest(case=label):
                fixture, capability = self._new_capability()
                original_open = self.storage.os.open
                original_stat = self.storage.os.stat
                entropy = bytes((0xb0 + index,)) * 16
                staging_name = ".staging-" + entropy.hex()
                open_failed = [False]
                stat_failed = [False]

                def controlled_open(
                    path, flags, mode=0o777, *, dir_fd=None
                ):
                    if path == staging_name and not open_failed[0]:
                        open_failed[0] = True
                        raise original
                    return original_open(
                        path, flags, mode, dir_fd=dir_fd
                    )

                def controlled_stat(path, *args, **kwargs):
                    if (
                        open_failed[0]
                        and path == staging_name
                        and type(kwargs.get("dir_fd")) is int
                        and not stat_failed[0]
                    ):
                        stat_failed[0] = True
                        raise secondary
                    return original_stat(path, *args, **kwargs)

                escaped = None
                try:
                    with mock.patch.object(
                        self.storage.os,
                        "open",
                        side_effect=controlled_open,
                    ) as patched_open, mock.patch.object(
                        self.storage.os,
                        "stat",
                        side_effect=controlled_stat,
                    ) as patched_stat:
                        dir_fd = set(self.storage.os.supports_dir_fd)
                        dir_fd.discard(original_open)
                        dir_fd.discard(original_stat)
                        dir_fd.update((patched_open, patched_stat))
                        nofollow = set(
                            self.storage.os.supports_follow_symlinks
                        )
                        nofollow.discard(original_stat)
                        nofollow.add(patched_stat)
                        with mock.patch.object(
                            self.storage.os, "supports_dir_fd", dir_fd
                        ), mock.patch.object(
                            self.storage.os,
                            "supports_follow_symlinks",
                            nofollow,
                        ), mock.patch.object(
                            self.storage.os,
                            "urandom",
                            return_value=entropy,
                        ):
                            try:
                                self._materialize(capability)
                            except BaseException as error:
                                escaped = error
                    self.assertTrue(open_failed[0])
                    self.assertTrue(stat_failed[0])
                    if label == "secondary_control":
                        self.assertIs(escaped, secondary)
                        self.assertIsNone(secondary.__context__)
                    else:
                        self.assertIs(type(escaped), self.rpc._ArchiveRpcError)
                        self.assertEqual(
                            (escaped.reason_code, escaped.failure_kind),
                            (
                                "authority_mismatch",
                                "historical_window_spool_handoff_failed",
                            ),
                        )
                        self.assertIsNone(escaped.__context__)
                    self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
                finally:
                    fixture.close()

    def test_ambiguous_mkdir_outcome_is_observed_terminalized_and_retained(
        self,
    ):
        for index, label in enumerate(("body_control", "ordinary")):
            with self.subTest(case=label):
                if label == "body_control":
                    body_error = KeyboardInterrupt(
                        "slice3 ambiguous mkdir body sentinel"
                    )
                    observer_control = SystemExit(
                        "slice3 ambiguous mkdir observer sentinel"
                    )
                else:
                    body_error = OSError(
                        "slice3 ambiguous mkdir ordinary sentinel"
                    )
                    observer_control = None
                before_fds = frozenset(
                    int(name) for name in os.listdir("/dev/fd")
                    if name.isdigit()
                )
                fixture, capability = self._new_capability()
                capability_reference = weakref.ref(capability)
                original_mkdir = self.storage.os.mkdir
                original_stat = self.storage.os.stat
                entropy = bytes((0xc0 + index,)) * 16
                staging_name = ".staging-" + entropy.hex()
                mkdir_fired = [False]
                observer_calls = [0]
                observer_fired = [False]
                observation = {
                    "source_allocations": 0,
                    "snapshot_allocations": 0,
                    "event_issuer_calls": 0,
                    "quota_transition_calls": 0,
                }
                prior_trace = sys.gettrace()

                def ambiguous_mkdir(path, mode=0o777, *, dir_fd=None):
                    if path == staging_name and not mkdir_fired[0]:
                        original_mkdir(path, mode, dir_fd=dir_fd)
                        mkdir_fired[0] = True
                        raise body_error
                    return original_mkdir(path, mode, dir_fd=dir_fd)

                def observed_stat(path, *args, **kwargs):
                    if (
                        mkdir_fired[0]
                        and path == staging_name
                        and type(kwargs.get("dir_fd")) is int
                    ):
                        observer_calls[0] += 1
                        if (
                            observer_control is not None
                            and not observer_fired[0]
                        ):
                            observer_fired[0] = True
                            raise observer_control
                    return original_stat(path, *args, **kwargs)

                def tracer(frame, event, argument):
                    name = frame.f_code.co_name
                    if frame.f_code.co_filename == self.storage.__file__:
                        if name == "_prepare_handle" and event == "return":
                            if type(argument) is self.storage._HistoricalWindowCaptureReplaySource:
                                observation["source_allocations"] += 1
                            elif type(argument) is self.storage.HistoricalRunStagingSnapshot:
                                observation["snapshot_allocations"] += 1
                        if event == "call" and name in (
                            "_install_append_quota_transition",
                            "_install_quota_reserve_transition",
                            "_install_quota_commit_transition",
                        ):
                            observation["quota_transition_calls"] += 1
                    elif (
                        frame.f_code.co_filename == self.scan.__file__
                        and event == "call"
                        and name == "_issue_task4b_capture_replay_event"
                    ):
                        observation["event_issuer_calls"] += 1
                    return tracer

                escaped = None
                try:
                    with mock.patch.object(
                        self.storage.os,
                        "mkdir",
                        side_effect=ambiguous_mkdir,
                    ) as patched_mkdir, mock.patch.object(
                        self.storage.os,
                        "stat",
                        side_effect=observed_stat,
                    ) as patched_stat:
                        dir_fd = set(self.storage.os.supports_dir_fd)
                        dir_fd.discard(original_mkdir)
                        dir_fd.discard(original_stat)
                        dir_fd.update((patched_mkdir, patched_stat))
                        nofollow = set(
                            self.storage.os.supports_follow_symlinks
                        )
                        nofollow.discard(original_stat)
                        nofollow.add(patched_stat)
                        with mock.patch.object(
                            self.storage.os, "supports_dir_fd", dir_fd
                        ), mock.patch.object(
                            self.storage.os,
                            "supports_follow_symlinks",
                            nofollow,
                        ), mock.patch.object(
                            self.storage.os,
                            "urandom",
                            return_value=entropy,
                        ):
                            sys.settrace(tracer)
                            try:
                                self._materialize(capability)
                            except BaseException as error:
                                escaped = error
                finally:
                    sys.settrace(prior_trace)
                try:
                    staging = (
                        fixture.data_dir
                        / "raw"
                        / "historical-foundry-replay"
                        / staging_name
                    )
                    self.assertTrue(mkdir_fired[0])
                    self.assertGreaterEqual(observer_calls[0], 1)
                    self.assertEqual(
                        observer_fired[0], observer_control is not None
                    )
                    if label == "body_control":
                        self.assertIs(escaped, body_error)
                        self.assertIsNone(body_error.__context__)
                        self.assertIsNone(observer_control.__context__)
                    else:
                        self.assertIs(type(escaped), self.rpc._ArchiveRpcError)
                        self.assertEqual(
                            (escaped.reason_code, escaped.failure_kind),
                            (
                                "authority_mismatch",
                                "historical_window_spool_handoff_failed",
                            ),
                        )
                        self.assertIsNone(escaped.__context__)
                    self.assertTrue(staging.is_dir())
                    self.assertEqual(tuple(staging.iterdir()), ())
                    self.assertEqual(
                        observation,
                        {
                            "source_allocations": 0,
                            "snapshot_allocations": 0,
                            "event_issuer_calls": 0,
                            "quota_transition_calls": 0,
                        },
                    )
                    self.assertIsNone(capability.close())
                    self.assertIsNone(capability.close())
                finally:
                    fixture.capability = None
                    capability = None
                    escaped = None
                    body_error = None
                    observer_control = None
                    fixture.close()
                gc.collect()
                self.assertIsNone(capability_reference())
                after_fds = frozenset(
                    int(name) for name in os.listdir("/dev/fd")
                    if name.isdigit()
                )
                self.assertEqual(after_fds, before_fds)

    def test_ambiguous_mkdir_same_mode_empty_replacement_is_retained(self):
        before_fds = frozenset(
            int(name) for name in os.listdir("/dev/fd") if name.isdigit()
        )
        fixture, capability = self._new_capability()
        original_mkdir = self.storage.os.mkdir
        original_stat = self.storage.os.stat
        entropy = b"\xd2" * 16
        staging_name = ".staging-" + entropy.hex()
        retired_name = ".retired-" + entropy.hex()
        control = KeyboardInterrupt(
            "slice3 ambiguous mkdir replacement sentinel"
        )
        observation = {
            "injected": False,
            "replacement_inode": None,
            "reconciliation_stats": 0,
        }

        def replacing_mkdir(path, mode=0o777, *, dir_fd=None):
            if path == staging_name and not observation["injected"]:
                original_mkdir(path, mode, dir_fd=dir_fd)
                os.rename(
                    staging_name,
                    retired_name,
                    src_dir_fd=dir_fd,
                    dst_dir_fd=dir_fd,
                )
                original_mkdir(staging_name, 0o700, dir_fd=dir_fd)
                replacement = original_stat(
                    staging_name,
                    dir_fd=dir_fd,
                    follow_symlinks=False,
                )
                observation["replacement_inode"] = replacement.st_ino
                os.rmdir(retired_name, dir_fd=dir_fd)
                observation["injected"] = True
                raise control
            return original_mkdir(path, mode, dir_fd=dir_fd)

        def observed_stat(path, *args, **kwargs):
            if (
                observation["injected"]
                and path == staging_name
                and type(kwargs.get("dir_fd")) is int
            ):
                observation["reconciliation_stats"] += 1
            return original_stat(path, *args, **kwargs)

        escaped = None
        try:
            with mock.patch.object(
                self.storage.os, "mkdir", side_effect=replacing_mkdir
            ) as patched_mkdir, mock.patch.object(
                self.storage.os, "stat", side_effect=observed_stat
            ) as patched_stat:
                dir_fd = set(self.storage.os.supports_dir_fd)
                dir_fd.discard(original_mkdir)
                dir_fd.discard(original_stat)
                dir_fd.update((patched_mkdir, patched_stat))
                nofollow = set(self.storage.os.supports_follow_symlinks)
                nofollow.discard(original_stat)
                nofollow.add(patched_stat)
                with mock.patch.object(
                    self.storage.os, "supports_dir_fd", dir_fd
                ), mock.patch.object(
                    self.storage.os,
                    "supports_follow_symlinks",
                    nofollow,
                ), mock.patch.object(
                    self.storage.os, "urandom", return_value=entropy
                ):
                    try:
                        self._materialize(capability)
                    except BaseException as error:
                        escaped = error
            staging = (
                fixture.data_dir
                / "raw"
                / "historical-foundry-replay"
                / staging_name
            )
            self.assertTrue(observation["injected"])
            self.assertGreaterEqual(observation["reconciliation_stats"], 1)
            self.assertIs(escaped, control)
            self.assertIsNone(control.__context__)
            self.assertTrue(staging.is_dir())
            self.assertEqual(
                os.stat(staging, follow_symlinks=False).st_ino,
                observation["replacement_inode"],
            )
            self.assertEqual(tuple(staging.iterdir()), ())
        finally:
            fixture.close()
            capability = None
            escaped = None
        gc.collect()
        after_fds = frozenset(
            int(name) for name in os.listdir("/dev/fd") if name.isdigit()
        )
        self.assertEqual(after_fds, before_fds)

    def test_created_directory_first_identity_stat_failure_is_cleaned(self):
        fixture, capability = self._new_capability()
        original_stat = self.storage.os.stat
        staging_name = ".staging-" + "7e" * 16
        failed = [False]

        def failing_stat(path, *args, **kwargs):
            if (
                path == staging_name
                and type(kwargs.get("dir_fd")) is int
                and not failed[0]
            ):
                failed[0] = True
                raise OSError("slice3 injected first identity stat failure")
            return original_stat(path, *args, **kwargs)

        try:
            with mock.patch.object(
                self.storage.os, "stat", side_effect=failing_stat
            ) as patched_stat:
                supported = set(self.storage.os.supports_dir_fd)
                supported.discard(original_stat)
                supported.add(patched_stat)
                nofollow = set(self.storage.os.supports_follow_symlinks)
                nofollow.discard(original_stat)
                nofollow.add(patched_stat)
                with mock.patch.object(
                    self.storage.os, "supports_dir_fd", supported
                ), mock.patch.object(
                    self.storage.os,
                    "supports_follow_symlinks",
                    nofollow,
                ), mock.patch.object(
                    self.storage.os,
                    "urandom",
                    return_value=b"\x7e" * 16,
                ):
                    self._assert_spool_handoff_failure(
                        lambda: self._materialize(capability)
                    )
            self.assertTrue(failed[0])
            self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
        finally:
            fixture.close()

    def test_post_identity_stat_line_control_keeps_cleanup_authority(self):
        fixture, capability = self._new_capability()
        staging_name = ".staging-" + "8f" * 16
        control = KeyboardInterrupt("slice3 post-identity-stat sentinel")
        fired = [False]
        prior_trace = sys.gettrace()

        def tracer(frame, event, _argument):
            entry = frame.f_locals.get("entry")
            if (
                not fired[0]
                and event == "line"
                and frame.f_code.co_filename == self.storage.__file__
                and frame.f_code.co_name == "_task4b_open_capture_directory"
                and type(entry) is dict
                and entry.get("name") == staging_name
                and entry.get("created") is True
                and entry.get("identity") is None
                and isinstance(frame.f_locals.get("before"), os.stat_result)
            ):
                fired[0] = True
                raise control
            return tracer

        try:
            with mock.patch.object(
                self.storage.os,
                "urandom",
                return_value=b"\x8f" * 16,
            ):
                sys.settrace(tracer)
                with self.assertRaises(KeyboardInterrupt) as caught:
                    self._materialize(capability)
        finally:
            sys.settrace(prior_trace)
        try:
            self.assertTrue(fired[0])
            self.assertIs(caught.exception, control)
            self.assertIsNone(control.__context__)
            self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
        finally:
            fixture.close()

    def test_identityless_same_basename_replacement_is_retained(self):
        fixture, capability = self._new_capability()
        staging_name = ".staging-" + "9c" * 16
        retired_name = ".retired-" + "9c" * 16
        control = KeyboardInterrupt("slice3 replacement sentinel")
        observation = {"fired": False, "attacker_inode": None}
        prior_trace = sys.gettrace()

        def tracer(frame, event, _argument):
            entry = frame.f_locals.get("entry")
            if (
                not observation["fired"]
                and event == "line"
                and frame.f_code.co_filename == self.storage.__file__
                and frame.f_code.co_name == "_task4b_open_capture_directory"
                and type(entry) is dict
                and entry.get("name") == staging_name
                and entry.get("created") is True
                and entry.get("identity") is None
                and isinstance(frame.f_locals.get("before"), os.stat_result)
            ):
                parent_fd = entry.get("parent_fd")
                self.assertIs(type(parent_fd), int)
                os.rename(
                    staging_name,
                    retired_name,
                    src_dir_fd=parent_fd,
                    dst_dir_fd=parent_fd,
                )
                os.mkdir(staging_name, 0o700, dir_fd=parent_fd)
                attacker = os.stat(
                    staging_name,
                    dir_fd=parent_fd,
                    follow_symlinks=False,
                )
                observation["attacker_inode"] = attacker.st_ino
                observation["fired"] = True
                raise control
            return tracer

        try:
            with mock.patch.object(
                self.storage.os,
                "urandom",
                return_value=b"\x9c" * 16,
            ):
                sys.settrace(tracer)
                with self.assertRaises(KeyboardInterrupt) as caught:
                    self._materialize(capability)
        finally:
            sys.settrace(prior_trace)
        try:
            replay = (
                fixture.data_dir / "raw" / "historical-foundry-replay"
            )
            attacker_path = replay / staging_name
            retired_path = replay / retired_name
            self.assertTrue(observation["fired"])
            self.assertIs(caught.exception, control)
            self.assertTrue(attacker_path.is_dir())
            self.assertEqual(
                os.stat(attacker_path, follow_symlinks=False).st_ino,
                observation["attacker_inode"],
            )
            self.assertTrue(retired_path.is_dir())
        finally:
            fixture.close()

    def test_cleanup_control_resumes_entry_and_terminal_close_is_idempotent(self):
        fixture, capability = self._new_capability()
        original_open = self.storage.os.open
        original_pwrite = self.storage.os.pwrite
        original_unlink = self.storage.os.unlink
        fd_names = {}
        body_failed = [False]
        unlink_calls = [0]
        control = KeyboardInterrupt("slice3 resumable cleanup sentinel")

        def observed_open(path, flags, mode=0o777, *, dir_fd=None):
            fd = original_open(path, flags, mode, dir_fd=dir_fd)
            fd_names[fd] = path
            return fd

        def failing_pwrite(fd, payload, offset):
            if fd_names.get(fd) == "policy.json":
                body_failed[0] = True
                raise RuntimeError("slice3 ordinary body failure")
            return original_pwrite(fd, payload, offset)

        def controlled_unlink(path, *args, **kwargs):
            if path == "policy.json":
                unlink_calls[0] += 1
                if unlink_calls[0] == 1:
                    raise control
            return original_unlink(path, *args, **kwargs)

        try:
            with mock.patch.object(
                self.storage.os, "open", side_effect=observed_open
            ) as patched_open, mock.patch.object(
                self.storage.os, "unlink", side_effect=controlled_unlink
            ) as patched_unlink:
                supported = set(self.storage.os.supports_dir_fd)
                supported.discard(original_open)
                supported.discard(original_unlink)
                supported.update((patched_open, patched_unlink))
                with mock.patch.object(
                    self.storage.os, "supports_dir_fd", supported
                ), mock.patch.object(
                    self.storage.os, "pwrite", side_effect=failing_pwrite
                ):
                    with self.assertRaises(KeyboardInterrupt) as caught:
                        self._materialize(capability)
            self.assertIs(caught.exception, control)
            self.assertTrue(body_failed[0])
            self.assertEqual(unlink_calls[0], 2)
            self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
            capability.close()
            capability.close()
            self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
        finally:
            fixture.close()

    def test_output_config_short_reread_is_sanitized_and_cleaned(self):
        fixture, capability = self._new_capability()
        original_open = self.storage.os.open
        original_pread = self.storage.os.pread
        fd_names = {}
        injected = [False]

        def observed_open(path, flags, mode=0o777, *, dir_fd=None):
            fd = original_open(path, flags, mode, dir_fd=dir_fd)
            fd_names[fd] = path
            return fd

        def short_pread(fd, length, offset):
            if fd_names.get(fd) == "policy.json" and not injected[0]:
                injected[0] = True
                return b""
            return original_pread(fd, length, offset)

        try:
            with mock.patch.object(
                self.storage.os, "open", side_effect=observed_open
            ) as patched_open:
                supported = set(self.storage.os.supports_dir_fd)
                supported.discard(original_open)
                supported.add(patched_open)
                with mock.patch.object(
                    self.storage.os, "supports_dir_fd", supported
                ), mock.patch.object(
                    self.storage.os, "pread", side_effect=short_pread
                ):
                    self._assert_spool_handoff_failure(
                        lambda: self._materialize(capability)
                    )
            fixture.capability = None
            self.assertTrue(injected[0])
            self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
        finally:
            fixture.close()

    def test_cleanup_control_wins_over_ordinary_config_body_failure(self):
        fixture, capability = self._new_capability()
        original_open = self.storage.os.open
        original_unlink = self.storage.os.unlink
        fd_names = {}
        control = KeyboardInterrupt("slice3 cleanup sentinel")
        body_failed = [False]
        cleanup_failed = [False]

        def observed_open(path, flags, mode=0o777, *, dir_fd=None):
            fd = original_open(path, flags, mode, dir_fd=dir_fd)
            fd_names[fd] = path
            return fd

        def controlled_unlink(path, *args, **kwargs):
            if path == "policy.json" and not cleanup_failed[0]:
                cleanup_failed[0] = True
                raise control
            return original_unlink(path, *args, **kwargs)

        original_pwrite = self.storage.os.pwrite

        def bounded_pwrite(fd, payload, offset):
            if fd_names.get(fd) == "policy.json":
                body_failed[0] = True
                raise RuntimeError("slice3 ordinary body sentinel")
            return original_pwrite(fd, payload, offset)

        try:
            with mock.patch.object(
                self.storage.os, "open", side_effect=observed_open
            ) as patched_open, mock.patch.object(
                self.storage.os, "unlink", side_effect=controlled_unlink
            ) as patched_unlink:
                supported = set(self.storage.os.supports_dir_fd)
                supported.discard(original_open)
                supported.discard(original_unlink)
                supported.update((patched_open, patched_unlink))
                with mock.patch.object(
                    self.storage.os, "supports_dir_fd", supported
                ), mock.patch.object(
                    self.storage.os, "pwrite", side_effect=bounded_pwrite
                ), mock.patch.object(
                    self.storage.os, "urandom", return_value=b"\x78" * 16
                ):
                    with self.assertRaises(KeyboardInterrupt) as caught:
                        self._materialize(capability)
            fixture.capability = None
            self.assertIs(caught.exception, control)
            self.assertTrue(body_failed[0])
            self.assertTrue(cleanup_failed[0])
        finally:
            fixture.close()

    def test_staging_cleanup_precedes_binding_and_spool_retirement(self):
        fixture, capability = self._new_capability()
        observed = []
        snapshot = None
        preclose_spools = None
        preclose_entries = None
        original_unlink = self.storage.os.unlink
        prior_trace = sys.gettrace()

        def observed_unlink(path, *args, **kwargs):
            if (
                type(path) is str
                and path.startswith(
                    ".historical-foundry-exchange-spool-"
                )
                and path.endswith(".bin")
            ):
                observed.append("spool_unlink")
            return original_unlink(path, *args, **kwargs)

        def tracer(frame, event, _argument):
            if (
                event == "call"
                and frame.f_code.co_filename == self.storage.__file__
                and frame.f_code.co_name in (
                    "_task4b_move_bound_source_authority",
                    "_prepare_handle",
                    "_cleanup_task4b_capture_staging",
                    "_task4b_close_snapshot_source_authority",
                    "_revoke_bound_source",
                    "_retire_lineage",
                    "_cleanup_resources",
                    "_retire_nonowner_handle",
                )
            ):
                name = frame.f_code.co_name
                if (
                    (
                        name != "_prepare_handle"
                        or frame.f_locals.get("authority_class")
                        is self.storage.HistoricalRunStagingSnapshot
                    )
                    and (
                        name != "_retire_lineage"
                        or frame.f_locals.get("preserve_handle") is snapshot
                    )
                    and (
                        name != "_retire_nonowner_handle"
                        or frame.f_locals.get("handle") is snapshot
                    )
                ):
                    observed.append(name)
            return tracer

        try:
            with mock.patch.object(
                self.storage.os,
                "unlink",
                side_effect=observed_unlink,
            ) as patched_unlink:
                supported = set(self.storage.os.supports_dir_fd)
                supported.discard(original_unlink)
                supported.add(patched_unlink)
                with mock.patch.object(
                    self.storage.os, "supports_dir_fd", supported
                ):
                    sys.settrace(tracer)
                    snapshot = self._materialize(capability)
                    fixture.capability = None
                    preclose_entries = tuple(fixture.data_dir.iterdir())
                    preclose_spools = tuple(
                        path.name
                        for path in preclose_entries
                        if path.name.startswith(
                            ".historical-foundry-exchange-spool-"
                        )
                        and path.name.endswith(".bin")
                    )
                    snapshot.close()
                    snapshot.close()
        finally:
            sys.settrace(prior_trace)
        try:
            self.assertIs(
                type(snapshot),
                self.storage.HistoricalRunStagingSnapshot,
            )
            self.assertTrue(any(path.name == "raw" for path in preclose_entries))
            self.assertIn("_cleanup_task4b_capture_staging", observed)
            self.assertIn(
                "_task4b_close_snapshot_source_authority", observed
            )
            self.assertIn("_revoke_bound_source", observed)
            self.assertIn("_retire_lineage", observed)
            self.assertIn("_cleanup_resources", observed)
            self.assertIn("_retire_nonowner_handle", observed)
            self.assertLess(
                observed.index("_cleanup_task4b_capture_staging"),
                observed.index(
                    "_task4b_close_snapshot_source_authority"
                ),
            )
            self.assertLess(
                observed.index(
                    "_task4b_close_snapshot_source_authority"
                ),
                observed.index("_revoke_bound_source"),
            )
            self.assertLess(
                observed.index("_revoke_bound_source"),
                observed.index("_retire_lineage"),
            )
            self.assertLess(
                observed.index("_retire_lineage"),
                observed.index("_cleanup_resources"),
            )
            self.assertLess(
                observed.index("_cleanup_resources"),
                observed.index("_retire_nonowner_handle"),
            )
            self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
            with self.subTest(contract="step6-spool-retired-before-delivery"):
                self.assertEqual(preclose_spools, ())
                self.assertEqual(observed.count("spool_unlink"), 1)
                self.assertLess(
                    observed.index("spool_unlink"),
                    observed.index("_task4b_move_bound_source_authority"),
                )
                self.assertLess(
                    observed.index("_task4b_move_bound_source_authority"),
                    observed.index("_prepare_handle"),
                )
        finally:
            if snapshot is not None:
                snapshot.close()
            fixture.close()

    def test_slice4_stop_has_raw_exchange_replay_but_no_later_surface(self):
        fixture, capability = self._new_capability()
        original_open = self.storage.os.open
        fd_names = {}
        observation = {
            "source_allocations": 0,
            "snapshot_allocations": 0,
            "event_issuer_calls": 0,
            "event_registry_sizes": [],
            "snapshot_registry_sizes": [],
            "quota_transition_calls": [],
            "final_surface_members": [],
            "member_open_order": [],
            "raw_members": [],
            "raw_exchange_join_count_at_stop": 0,
            "config_members": [],
        }
        member_read_flags = (
            self.storage.os.O_RDONLY
            | self.storage.os.O_NOFOLLOW
            | self.storage.os.O_CLOEXEC
        )
        prior_trace = sys.gettrace()
        stop = KeyboardInterrupt("slice4 exact post-raw stop")
        stop_fired = [False]

        def observed_open(path, flags, mode=0o777, *, dir_fd=None):
            fd = original_open(path, flags, mode, dir_fd=dir_fd)
            fd_names[fd] = path
            if (
                path in ("policy.json", "authority.json", "toolchain.json")
                and flags & self.storage.os.O_CREAT
            ):
                observation["config_members"].append(path)
            if type(path) is str:
                if (
                    len(path) == 12
                    and path[:8].isdigit()
                    and path[8:] == ".bin"
                ):
                    observation["raw_members"].append(path)
                    if flags == member_read_flags:
                        observation["member_open_order"].append(
                            ("raw", path)
                        )
                elif path == "run_manifest.json":
                    observation["final_surface_members"].append(path)
                elif (
                    path.endswith(".json.gz")
                    or path == "capture_inventory.json"
                ):
                    if flags == member_read_flags:
                        observation["member_open_order"].append(
                            ("slice5", path)
                        )
            return fd

        def tracer(frame, event, argument):
            filename = frame.f_code.co_filename
            name = frame.f_code.co_name
            if filename == self.storage.__file__:
                if name == "_prepare_handle" and event == "return":
                    if type(argument) is self.storage._HistoricalWindowCaptureReplaySource:
                        observation["source_allocations"] += 1
                    elif type(argument) is self.storage.HistoricalRunStagingSnapshot:
                        observation["snapshot_allocations"] += 1
                if (
                    event == "call"
                    and name in (
                        "_install_append_quota_transition",
                        "_install_quota_reserve_transition",
                        "_install_quota_commit_transition",
                    )
                ):
                    caller = frame.f_back
                    entry = (
                        caller.f_locals.get("entry")
                        if caller is not None else None
                    )
                    observation["quota_transition_calls"].append((
                        name,
                        entry.get("name") if type(entry) is dict else None,
                    ))
                if (
                    not stop_fired[0]
                    and name == "_task4b_consume_root_payload"
                    and event == "call"
                    and type(frame.f_locals.get("record")) is dict
                    and bool(frame.f_locals["record"].get("exchange_joins"))
                ):
                    observation["raw_exchange_join_count_at_stop"] = len(
                        frame.f_locals["record"]["exchange_joins"]
                    )
                    stop_fired[0] = True
                    raise stop
                for key, value in frame.f_locals.items():
                    if (
                        type(key) is str
                        and "snapshot" in key
                        and "registry" in key
                        and type(value) is dict
                    ):
                        observation["snapshot_registry_sizes"].append(
                            len(value)
                        )
                return tracer
            if filename == self.scan.__file__:
                if (
                    name == "_issue_task4b_capture_replay_event"
                    and event == "call"
                ):
                    observation["event_issuer_calls"] += 1
                event_registry = frame.f_locals.get("event_registry")
                if type(event_registry) is dict:
                    observation["event_registry_sizes"].append(
                        len(event_registry)
                    )
            return tracer

        try:
            with mock.patch.object(
                self.storage.os, "open", side_effect=observed_open
            ) as patched_open:
                supported = set(self.storage.os.supports_dir_fd)
                supported.discard(original_open)
                supported.add(patched_open)
                with mock.patch.object(
                    self.storage.os, "supports_dir_fd", supported
                ):
                    sys.settrace(tracer)
                    escaped = None
                    try:
                        self._materialize(capability)
                    except BaseException as error:
                        escaped = error
            fixture.capability = None
        finally:
            sys.settrace(prior_trace)
        try:
            self.assertTrue(stop_fired[0])
            self.assertIs(escaped, stop)
            self.assertIsNone(stop.__context__)
            self.assertEqual(observation["source_allocations"], 1)
            self.assertEqual(observation["snapshot_allocations"], 0)
            self.assertGreater(observation["event_issuer_calls"], 0)
            self.assertTrue(observation["event_registry_sizes"])
            self.assertEqual(set(observation["event_registry_sizes"]), {0})
            self.assertNotIn(
                True,
                tuple(
                    size != 0
                    for size in observation["snapshot_registry_sizes"]
                ),
            )
            quota_pairs = [
                observation["quota_transition_calls"][index:index + 2]
                for index in range(
                    0, len(observation["quota_transition_calls"]), 2
                )
            ]
            self.assertEqual(len(quota_pairs), 3)
            self.assertTrue(all(
                pair == [
                    ("_install_quota_reserve_transition", pair[0][1]),
                    ("_install_quota_commit_transition", pair[0][1]),
                ]
                and pair[0][1] is not None
                for pair in quota_pairs
            ))
            self.assertEqual(
                tuple(pair[0][1] for pair in quota_pairs[:3]),
                ("policy.json", "authority.json", "toolchain.json"),
            )
            self.assertEqual(observation["final_surface_members"], [])
            self.assertGreater(
                observation["raw_exchange_join_count_at_stop"], 0
            )
            self.assertEqual(observation["raw_members"], [])
            self.assertFalse(any(
                kind == "slice5"
                for kind, _name in observation["member_open_order"]
            ))
            self.assertEqual(
                observation["config_members"],
                ["policy.json", "authority.json", "toolchain.json"],
            )
            self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
            stop.__traceback__ = None
            escaped = None
            capability = None
            gc.collect()
            self.assertFalse(any(
                type(value) is self.scan._ProductionHistoricalWindowCaptureReplayEvent
                or type(value) is self.storage.HistoricalRunStagingSnapshot
                for value in gc.get_objects()
            ))
        finally:
            fixture.close()


class HistoricalFoundryStorageTask4bRawChunkTests(unittest.TestCase):
    def test_slice4_raw_planner_literal_boundaries(self):
        storage = importlib.import_module("scripts.historical_foundry_storage")

        self.assertEqual(
            storage._plan_historical_raw_chunk_append(
                current_chunk_byte_count=16_777_200,
                request_byte_count=0,
                decoded_byte_count=0,
            ),
            ("append_current", 16_777_216),
        )
        self.assertEqual(
            storage._plan_historical_raw_chunk_append(
                current_chunk_byte_count=16_777_201,
                request_byte_count=0,
                decoded_byte_count=0,
            ),
            ("flush_then_append", 16),
        )
        self.assertEqual(
            storage._plan_historical_raw_chunk_append(
                current_chunk_byte_count=0,
                request_byte_count=4_194_304,
                decoded_byte_count=8_388_608,
            ),
            ("append_current", 12_582_928),
        )

        class IntSubclass(int):
            pass

        invalid_rows = (
            (0, 4_194_305, 0),
            (0, 0, 8_388_609),
            (16_777_217, 0, 0),
            (-1, 0, 0),
            (0, -1, 0),
            (0, 0, -1),
            (False, 0, 0),
            (0, False, 0),
            (0, 0, False),
            (IntSubclass(0), 0, 0),
            (0, IntSubclass(0), 0),
            (0, 0, IntSubclass(0)),
        )
        for current, request, decoded in invalid_rows:
            with self.subTest(
                current=current, request=request, decoded=decoded
            ):
                with self.assertRaises(ValueError):
                    storage._plan_historical_raw_chunk_append(
                        current_chunk_byte_count=current,
                        request_byte_count=request,
                        decoded_byte_count=decoded,
                    )

    def test_slice4_natural_frames_are_staged_as_unsplit_raw_members(self):
        from tests.test_historical_foundry_scan import (
            _Task4bOfflineCapabilityFixture,
        )

        rpc = importlib.import_module("scripts.historical_foundry_rpc")
        scan = importlib.import_module("scripts.historical_foundry_scan")
        storage = importlib.import_module("scripts.historical_foundry_storage")
        fixture = _Task4bOfflineCapabilityFixture()
        source_rows = []
        raw_members = []
        raw_inventory_rows = []
        provisional_exchange_joins = []
        duplicate_ledger_names = []
        finalization_success_reads = []
        original_open = storage.os.open
        original_close = storage.os.close
        original_fstat = storage.os.fstat
        original_pread = storage.os.pread
        original_unlink = storage.os.unlink
        prior_trace = sys.gettrace()

        def read_member(name, dir_fd):
            fd = original_open(
                name,
                os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=dir_fd,
            )
            try:
                size = original_fstat(fd).st_size
                chunks = []
                offset = 0
                while offset < size:
                    chunk = original_pread(fd, size - offset, offset)
                    if not chunk:
                        raise AssertionError("raw member reread was short")
                    chunks.append(chunk)
                    offset += len(chunk)
                return b"".join(chunks)
            finally:
                original_close(fd)

        def observed_unlink(path, *args, **kwargs):
            if (
                type(path) is str
                and len(path) == 12
                and path[:8].isdigit()
                and path[8:] == ".bin"
            ):
                raw_members.append((
                    path,
                    read_member(path, kwargs.get("dir_fd")),
                ))
            return original_unlink(path, *args, **kwargs)

        def tracer(frame, event, argument):
            if (
                frame.f_code.co_filename == rpc.__file__
                and frame.f_code.co_name == "__getitem__"
                and event == "call"
                and frame.f_locals.get("key") == "successful_exchanges"
            ):
                finalization_success_reads.append(1)
            elif (
                frame.f_code.co_filename == storage.__file__
                and frame.f_code.co_name == "__next__"
                and event == "return"
                and type(argument) is tuple
                and len(argument) == 3
                and type(argument[0]) is dict
                and type(argument[1]) is bytes
                and type(argument[2]) is bytes
            ):
                source_rows.append((
                    copy.deepcopy(argument[0]),
                    bytes(argument[1]),
                    bytes(argument[2]),
                ))
            elif (
                frame.f_code.co_filename == storage.__file__
                and frame.f_code.co_name == "_task4b_write_raw_chunk"
                and event == "return"
                and type(argument) is dict
            ):
                raw_inventory_rows.append(copy.deepcopy(argument))
            elif (
                frame.f_code.co_filename == storage.__file__
                and frame.f_code.co_name == "_task4b_consume_root_payload"
                and event == "call"
            ):
                record = frame.f_locals["record"]
                payload = frame.f_locals["payload"]
                root = payload[1]
                success_indices = root["success_exchange_indices"]
                joins = record["exchange_joins"]
                provisional_exchange_joins.extend(copy.deepcopy([
                    joins[exchange_index - 1]
                    for exchange_index in success_indices
                ]))
            elif (
                frame.f_code.co_filename == storage.__file__
                and frame.f_code.co_name == "_retire_lineage"
                and event == "call"
            ):
                record = frame.f_locals.get("record")
                if type(record) is dict:
                    duplicate_ledger_names.extend(
                        key for key in record
                        if "post_root" in key or "post_leaf" in key
                    )
            return tracer

        snapshot = None
        error = None
        try:
            capability = fixture.mint()
            with mock.patch.object(
                storage.os, "unlink", side_effect=observed_unlink
            ) as patched_unlink:
                supported = set(storage.os.supports_dir_fd)
                supported.discard(original_unlink)
                supported.add(patched_unlink)
                with mock.patch.object(
                    storage.os, "supports_dir_fd", supported
                ):
                    sys.settrace(tracer)
                    try:
                        snapshot = scan._materialize_historical_window_staging_snapshot(
                            capability=capability
                        )
                        self.assertIsNone(snapshot.close())
                        self.assertIsNone(snapshot.close())
                    except BaseException as caught:
                        error = caught
        finally:
            sys.settrace(prior_trace)
        try:
            self.assertIsNone(error)
            self.assertIs(
                type(snapshot), storage.HistoricalRunStagingSnapshot
            )
            self.assertTrue(source_rows)
            self.assertTrue(raw_members)
            self.assertTrue(raw_inventory_rows)
            self.assertGreater(len(source_rows), 2)
            self.assertEqual(len(finalization_success_reads), 2)
            self.assertEqual(
                len(provisional_exchange_joins), len(source_rows)
            )
            self.assertEqual(duplicate_ledger_names, [])
            self.assertEqual(
                tuple(name for name, _payload in raw_members),
                tuple(
                    "{:08d}.bin".format(index)
                    for index in range(1, len(raw_members) + 1)
                ),
            )
            expected_frames = tuple(
                len(request).to_bytes(8, "big")
                + request
                + len(decoded).to_bytes(8, "big")
                + decoded
                for _compact, request, decoded in source_rows
            )
            self.assertTrue(all(expected_frames))
            self.assertTrue(all(
                0 < len(payload) < 16_777_216
                for _name, payload in raw_members
            ))
            self.assertEqual(
                b"".join(payload for _name, payload in raw_members),
                b"".join(expected_frames),
            )
            raw_payload_by_path = {
                "rpc/" + name: payload for name, payload in raw_members
            }
            self.assertEqual(
                tuple(row["path"] for row in raw_inventory_rows),
                tuple(raw_payload_by_path),
            )
            next_exchange_index = 1
            next_request_id = 1
            for row in raw_inventory_rows:
                self.assertEqual(
                    tuple(row),
                    (
                        "path", "byte_count", "sha256",
                        "exchange_index_start", "exchange_index_stop",
                        "exchange_count", "request_id_start",
                        "request_id_stop",
                    ),
                )
                payload = raw_payload_by_path[row["path"]]
                self.assertIs(type(row["byte_count"]), int)
                self.assertGreater(row["byte_count"], 0)
                self.assertLessEqual(row["byte_count"], 16_777_216)
                self.assertEqual(row["byte_count"], len(payload))
                self.assertEqual(
                    row["sha256"], hashlib.sha256(payload).hexdigest()
                )
                self.assertEqual(
                    row["exchange_count"],
                    row["exchange_index_stop"]
                    - row["exchange_index_start"] + 1,
                )
                self.assertIs(type(row["exchange_index_start"]), int)
                self.assertIs(type(row["exchange_index_stop"]), int)
                self.assertIs(type(row["exchange_count"]), int)
                self.assertIs(type(row["request_id_start"]), int)
                self.assertIs(type(row["request_id_stop"]), int)
                self.assertGreater(row["exchange_count"], 0)
                self.assertEqual(
                    row["exchange_index_start"], next_exchange_index
                )
                covered = source_rows[
                    row["exchange_index_start"] - 1:
                    row["exchange_index_stop"]
                ]
                request_ids = tuple(
                    request_id
                    for compact, _request, _decoded in covered
                    for request_id in compact["request_ids"]
                )
                self.assertEqual(
                    request_ids,
                    tuple(range(
                        next_request_id,
                        next_request_id + len(request_ids),
                    )),
                )
                self.assertEqual(row["request_id_start"], next_request_id)
                self.assertEqual(row["request_id_stop"], request_ids[-1])
                next_exchange_index = row["exchange_index_stop"] + 1
                next_request_id = row["request_id_stop"] + 1
            self.assertEqual(next_exchange_index, len(source_rows) + 1)
            self.assertEqual(
                next_request_id,
                source_rows[-1][0]["request_ids"][-1] + 1,
            )
            compact_keys = tuple(source_rows[0][0])
            expected_join_keys = compact_keys + (
                "segment", "segment_local_index", "leaf_index",
                "wire_hash_authority", "raw_chunk_path",
                "raw_chunk_offset",
            )
            self.assertEqual(len(expected_join_keys), 22)
            self.assertNotEqual(
                expected_join_keys, tuple(sorted(expected_join_keys))
            )
            for source_index, join in enumerate(provisional_exchange_joins):
                self.assertEqual(tuple(join), expected_join_keys)
                self.assertNotIn("typed_role", join)
                self.assertNotIn("typed_chunk_refs", join)
                self.assertEqual(
                    {key: join[key] for key in compact_keys},
                    source_rows[source_index][0],
                )
                compact = source_rows[source_index][0]
                receipt = dict(compact)
                receipt["schema"] = (
                    "historical_foundry_exchange_spool_receipt/v1"
                )
                self.assertEqual(tuple(receipt), compact_keys)
                self.assertEqual(
                    {
                        key: receipt[key]
                        for key in compact_keys[1:]
                    },
                    {
                        key: compact[key]
                        for key in compact_keys[1:]
                    },
                )
                canonical = json.dumps(
                    join,
                    ensure_ascii=False,
                    allow_nan=False,
                    sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8")
                physical_key_order = json.loads(
                    canonical.decode("utf-8"),
                    object_pairs_hook=lambda pairs: tuple(
                        key for key, _value in pairs
                    ),
                )
                self.assertEqual(
                    physical_key_order, tuple(sorted(expected_join_keys))
                )
                self.assertNotEqual(physical_key_order, expected_join_keys)
                frame = expected_frames[source_index]
                raw_payload = raw_payload_by_path[join["raw_chunk_path"]]
                raw_offset = join["raw_chunk_offset"]
                self.assertIs(type(raw_offset), int)
                self.assertGreaterEqual(raw_offset, 0)
                self.assertEqual(
                    raw_payload[raw_offset:raw_offset + len(frame)], frame
                )
            frame_boundaries = set()
            offset = 0
            for frame in expected_frames:
                offset += len(frame)
                frame_boundaries.add(offset)
            offset = 0
            for _name, payload in raw_members:
                offset += len(payload)
                self.assertIn(offset, frame_boundaries)
            self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
        finally:
            if snapshot is not None:
                snapshot.close()
            fixture.capability = None
            fixture.close()

    def test_slice4_production_uses_captured_raw_planner_after_global_rebind(
        self,
    ):
        from tests.test_historical_foundry_scan import (
            _Task4bOfflineCapabilityFixture,
        )

        rpc = importlib.import_module("scripts.historical_foundry_rpc")
        scan = importlib.import_module("scripts.historical_foundry_scan")
        storage = importlib.import_module("scripts.historical_foundry_storage")
        fixture = _Task4bOfflineCapabilityFixture()
        replacement_calls = []
        raw_writer_calls = []
        prior_trace = sys.gettrace()

        def rebound_planner(**kwargs):
            replacement_calls.append(dict(kwargs))
            raise AssertionError("module-global raw planner was invoked")

        def tracer(frame, event, _argument):
            if (
                frame.f_code.co_filename == storage.__file__
                and frame.f_code.co_name == "_task4b_write_raw_chunk"
                and event == "call"
            ):
                raw_writer_calls.append(1)
            return tracer

        snapshot = None
        error = None
        try:
            capability = fixture.mint()
            with mock.patch.object(
                storage,
                "_plan_historical_raw_chunk_append",
                side_effect=rebound_planner,
            ):
                sys.settrace(tracer)
                try:
                    snapshot = scan._materialize_historical_window_staging_snapshot(
                        capability=capability
                    )
                    self.assertIsNone(snapshot.close())
                    self.assertIsNone(snapshot.close())
                except BaseException as caught:
                    error = caught
        finally:
            sys.settrace(prior_trace)
        try:
            self.assertEqual(replacement_calls, [])
            self.assertTrue(raw_writer_calls)
            self.assertIsNone(error)
            self.assertIs(
                type(snapshot), storage.HistoricalRunStagingSnapshot
            )
            self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
        finally:
            if snapshot is not None:
                snapshot.close()
            fixture.capability = None
            fixture.close()

    def test_slice4_raw_member_short_io_corruption_and_path_race_fail_closed(
        self,
    ):
        from tests.test_historical_foundry_scan import (
            _Task4bOfflineCapabilityFixture,
        )

        rpc = importlib.import_module("scripts.historical_foundry_rpc")
        scan = importlib.import_module("scripts.historical_foundry_scan")
        storage = importlib.import_module("scripts.historical_foundry_storage")

        for case in (
            "short_write",
            "short_read",
            "premature_eof",
            "corrupt_prefix",
            "path_identity",
        ):
            with self.subTest(case=case):
                fixture = _Task4bOfflineCapabilityFixture()
                original_open = storage.os.open
                original_pwrite = storage.os.pwrite
                original_pread = storage.os.pread
                original_stat = storage.os.stat
                observation = {
                    "raw_fd": None,
                    "raw_pwrite_calls": 0,
                    "raw_pread_calls": 0,
                    "raw_stat_calls": 0,
                    "mutation_fired": False,
                    "writer_returns": 0,
                }
                prior_trace = sys.gettrace()

                def is_raw_name(path):
                    return (
                        type(path) is str
                        and len(path) == 12
                        and path[:8].isdigit()
                        and path[8:] == ".bin"
                    )

                def observed_open(*args, **kwargs):
                    fd = original_open(*args, **kwargs)
                    if args and is_raw_name(args[0]):
                        observation["raw_fd"] = fd
                    return fd

                def observed_pwrite(fd, data, offset):
                    if fd == observation["raw_fd"]:
                        observation["raw_pwrite_calls"] += 1
                        if case == "short_write" and len(data) > 7:
                            return original_pwrite(fd, data[:7], offset)
                    return original_pwrite(fd, data, offset)

                def observed_pread(fd, byte_count, offset):
                    if fd == observation["raw_fd"]:
                        observation["raw_pread_calls"] += 1
                        if case == "short_read" and byte_count > 5:
                            return original_pread(fd, 5, offset)
                        if (
                            case == "premature_eof"
                            and not observation["mutation_fired"]
                            and offset == 0
                        ):
                            observation["mutation_fired"] = True
                            return b""
                        if (
                            case == "corrupt_prefix"
                            and not observation["mutation_fired"]
                            and offset == 0
                        ):
                            payload = original_pread(
                                fd, byte_count, offset
                            )
                            if payload:
                                observation["mutation_fired"] = True
                                replacement = (
                                    b"\x01" if payload[:1] != b"\x01"
                                    else b"\x02"
                                )
                                return replacement + payload[1:]
                            return payload
                    return original_pread(fd, byte_count, offset)

                def observed_stat(path, *args, **kwargs):
                    details = original_stat(path, *args, **kwargs)
                    if is_raw_name(path):
                        observation["raw_stat_calls"] += 1
                        if (
                            case == "path_identity"
                            and not observation["mutation_fired"]
                            and observation["raw_stat_calls"] == 2
                        ):
                            observation["mutation_fired"] = True
                            fields = list(details)
                            fields[1] += 1
                            return os.stat_result(fields)
                    return details

                def tracer(frame, event, argument):
                    if (
                        frame.f_code.co_filename == storage.__file__
                        and frame.f_code.co_name
                        == "_task4b_write_raw_chunk"
                        and event == "return"
                        and type(argument) is dict
                    ):
                        observation["writer_returns"] += 1
                    return tracer

                snapshot = None
                error = None
                try:
                    capability = fixture.mint()
                    with mock.patch.object(
                        storage.os, "open", side_effect=observed_open
                    ) as patched_open, mock.patch.object(
                        storage.os, "pwrite", side_effect=observed_pwrite
                    ), mock.patch.object(
                        storage.os, "pread", side_effect=observed_pread
                    ), mock.patch.object(
                        storage.os, "stat", side_effect=observed_stat
                    ) as patched_stat:
                        dir_fd = set(storage.os.supports_dir_fd)
                        dir_fd.discard(original_open)
                        dir_fd.discard(original_stat)
                        dir_fd.update((patched_open, patched_stat))
                        nofollow = set(
                            storage.os.supports_follow_symlinks
                        )
                        nofollow.discard(original_stat)
                        nofollow.add(patched_stat)
                        with mock.patch.object(
                            storage.os, "supports_dir_fd", dir_fd
                        ), mock.patch.object(
                            storage.os,
                            "supports_follow_symlinks",
                            nofollow,
                        ):
                            sys.settrace(tracer)
                            try:
                                snapshot = scan._materialize_historical_window_staging_snapshot(
                                    capability=capability
                                )
                                if case in ("short_write", "short_read"):
                                    self.assertIsNone(snapshot.close())
                                    self.assertIsNone(snapshot.close())
                            except BaseException as caught:
                                error = caught
                finally:
                    sys.settrace(prior_trace)
                try:
                    if case in ("short_write", "short_read"):
                        self.assertIsNone(error)
                        self.assertIs(
                            type(snapshot),
                            storage.HistoricalRunStagingSnapshot,
                        )
                    else:
                        self.assertIs(type(error), rpc._ArchiveRpcError)
                        self.assertEqual(
                            (error.reason_code, error.failure_kind),
                            (
                                "authority_mismatch",
                                "historical_window_spool_handoff_failed",
                            ),
                        )
                        self.assertIsNone(error.__context__)
                    self.assertIs(type(observation["raw_fd"]), int)
                    self.assertGreater(
                        observation["raw_pwrite_calls"], 0
                    )
                    if case == "short_write":
                        self.assertGreater(
                            observation["raw_pwrite_calls"], 1
                        )
                    if case in ("short_read", "premature_eof", "corrupt_prefix"):
                        self.assertGreater(
                            observation["raw_pread_calls"], 0
                        )
                    if case in (
                        "premature_eof", "corrupt_prefix", "path_identity"
                    ):
                        self.assertTrue(observation["mutation_fired"])
                        self.assertEqual(
                            observation["writer_returns"], 0
                        )
                    else:
                        self.assertEqual(
                            observation["writer_returns"], 1
                        )
                    self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
                finally:
                    if snapshot is not None:
                        snapshot.close()
                    fixture.capability = None
                    fixture.close()

    def test_slice4_source_frame_transplant_and_descriptor_race_reconcile(
        self,
    ):
        from tests.test_historical_foundry_scan import (
            _Task4bOfflineCapabilityFixture,
        )

        rpc = importlib.import_module("scripts.historical_foundry_rpc")
        scan = importlib.import_module("scripts.historical_foundry_scan")
        storage = importlib.import_module("scripts.historical_foundry_storage")

        for case in ("frame_transplant", "descriptor_race"):
            with self.subTest(case=case):
                fixture = _Task4bOfflineCapabilityFixture()
                original_pread = storage.os.pread
                original_fstat = storage.os.fstat
                observation = {
                    "target_fd": None,
                    "target_offset": None,
                    "target_length": None,
                    "armed": False,
                    "fired": False,
                    "issuer_calls": 0,
                    "raw_writer_calls": 0,
                }
                prior_trace = sys.gettrace()

                def observed_pread(fd, byte_count, offset):
                    if (
                        case == "frame_transplant"
                        and not observation["fired"]
                        and byte_count == 8
                    ):
                        observation["armed"] = True
                        observation["fired"] = True
                        return original_pread(
                            fd,
                            byte_count,
                            offset + 16,
                        )
                    if (
                        case == "descriptor_race"
                        and not observation["armed"]
                        and byte_count == 8
                    ):
                        observation["target_fd"] = fd
                        observation["armed"] = True
                    return original_pread(fd, byte_count, offset)

                def observed_fstat(fd):
                    details = original_fstat(fd)
                    if (
                        case == "descriptor_race"
                        and observation["armed"]
                        and not observation["fired"]
                        and fd == observation["target_fd"]
                    ):
                        observation["fired"] = True
                        fields = list(details)
                        fields[1] += 1
                        return os.stat_result(fields)
                    return details

                def tracer(frame, event, _argument):
                    if frame.f_code.co_filename == storage.__file__:
                        if (
                            frame.f_code.co_name
                            == "_task4b_write_raw_chunk"
                            and event == "call"
                        ):
                            observation["raw_writer_calls"] += 1
                    elif (
                        frame.f_code.co_filename == scan.__file__
                        and frame.f_code.co_name
                        == "_issue_task4b_capture_replay_event"
                        and event == "call"
                    ):
                        observation["issuer_calls"] += 1
                    return tracer

                error = None
                try:
                    capability = fixture.mint()
                    with mock.patch.object(
                        storage.os, "pread", side_effect=observed_pread
                    ), mock.patch.object(
                        storage.os, "fstat", side_effect=observed_fstat
                    ):
                        sys.settrace(tracer)
                        try:
                            scan._materialize_historical_window_staging_snapshot(
                                capability=capability
                            )
                        except BaseException as caught:
                            error = caught
                finally:
                    sys.settrace(prior_trace)
                try:
                    self.assertTrue(observation["armed"])
                    self.assertTrue(observation["fired"])
                    self.assertEqual(observation["issuer_calls"], 0)
                    self.assertEqual(observation["raw_writer_calls"], 0)
                    self.assertIs(type(error), rpc._ArchiveRpcError)
                    self.assertEqual(
                        (error.reason_code, error.failure_kind),
                        (
                            "authority_mismatch",
                            "historical_window_reconciliation_mismatch",
                        ),
                    )
                    self.assertIsNone(error.__context__)
                    self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
                finally:
                    fixture.capability = None
                    fixture.close()


class HistoricalFoundryStorageTask4bTypedChunkTests(unittest.TestCase):
    _TYPED_ROLES = ("headers", "reserves", "prices", "fees")

    @staticmethod
    def _capture_relative(full_path):
        parts = full_path.split("/")
        for index, part in enumerate(parts):
            if part.startswith(".staging-"):
                return "/".join(parts[index + 1:])
        return None

    def _run_member_capture(
        self,
        *,
        context_factory=None,
        gzip_mutation=None,
        before_materialize=None,
    ):
        from tests.test_historical_foundry_scan import (
            _Task4bOfflineCapabilityFixture,
            _small_context,
        )

        rpc = importlib.import_module("scripts.historical_foundry_rpc")
        scan = importlib.import_module("scripts.historical_foundry_scan")
        storage = importlib.import_module(
            "scripts.historical_foundry_storage"
        )
        if context_factory is None:
            context_factory = _small_context
        fixture = _Task4bOfflineCapabilityFixture(
            context_factory=context_factory
        )
        original_open = storage.os.open
        original_close = storage.os.close
        original_fstat = storage.os.fstat
        original_pread = storage.os.pread
        original_pwrite = storage.os.pwrite
        original_ftruncate = storage.os.ftruncate
        original_unlink = storage.os.unlink
        original_fsync = storage.os.fsync
        original_listdir = storage.os.listdir
        fd_paths = {}
        fd_directories = {}
        audit_sessions = {}
        inventory_fds = set()
        inventory_finalized = [False]
        sequence = [0]
        observation = {
            "created_paths": [],
            "open_events": [],
            "audit_read_paths_before_cleanup": (),
            "audit_sessions": [],
            "audit_complete_returns": [],
            "directory_fsyncs": [],
            "tree_enumerations": [],
            "member_bytes": {},
            "payloads": [],
            "consumer_payloads": [],
            "post_roots": (),
            "bound_prefinalization_digests": None,
            "bound_reconciliation_digests": None,
            "digest_plan": None,
            "digest_frozen_pre_ledger": None,
            "digest_compact_projection": None,
            "digest_final_anchor": None,
            "transaction_joins": (),
            "transaction_raw_chunks": (),
            "transaction_typed_chunks": (),
            "provisional_join_ids": {},
            "provisional_join_checks": [],
            "final_join_ids": (),
            "join_identity_preserved": False,
            "owner_source_join_identity": False,
            "capture_generation": None,
            "capture_state_at_cleanup": None,
            "capture_phase_at_cleanup": None,
            "consumerless_owner_keys_at_cleanup": None,
            "claimed_finalization_identity": None,
            "cleanup_entries": 0,
            "spool_unlinks": [],
            "quota_transition_calls": [],
            "quota_transition_rows": [],
            "quota_lifecycle_events": [],
            "snapshot_allocations": 0,
            "source_move_calls": 0,
            "helper_calls": {
                "_plan_historical_typed_root_append": 0,
                "_require_historical_gzip_member_size": 0,
                "_require_historical_capture_inventory_size": 0,
            },
            "gzip_mutation_fired": False,
            "error_pair": None,
            "error": None,
            "data_entries": None,
        }
        cleanup_started = [False]
        provisional_join_keys = (
            "schema", "exchange_index", "logical_batch_index",
            "attempt_index", "request_byte_count", "request_sha256",
            "request_ids", "wire_byte_count", "wire_sha256",
            "decoded_byte_count", "decoded_sha256", "response_ids",
            "spool_member_index", "spool_offset", "spool_length",
            "spool_member_sha256", "segment", "segment_local_index",
            "leaf_index", "wire_hash_authority", "raw_chunk_path",
            "raw_chunk_offset",
        )
        final_join_keys = provisional_join_keys + (
            "typed_role", "typed_chunk_refs",
        )

        def read_member(path, dir_fd):
            fd = original_open(
                path,
                os.O_RDONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=dir_fd,
            )
            try:
                size = original_fstat(fd).st_size
                chunks = []
                offset = 0
                while offset < size:
                    chunk = original_pread(fd, size - offset, offset)
                    if not chunk:
                        raise AssertionError("capture member reread was short")
                    chunks.append(chunk)
                    offset += len(chunk)
                return b"".join(chunks)
            finally:
                original_close(fd)

        def mutate_gzip(path, dir_fd, relative):
            payload = read_member(path, dir_fd)
            if gzip_mutation == "trailing":
                changed = payload + b"\x00"
            elif gzip_mutation == "crc":
                changed = bytearray(payload)
                changed[-8] ^= 1
                changed = bytes(changed)
            elif gzip_mutation == "profile":
                changed = bytearray(payload)
                changed[4] = 1
                changed = bytes(changed)
            elif gzip_mutation == "bomb":
                bomb_buffer = io.BytesIO()
                with gzip.GzipFile(
                    filename="",
                    mode="wb",
                    compresslevel=9,
                    fileobj=bomb_buffer,
                    mtime=0,
                ) as handle:
                    small_block = b"0" * 65_536
                    for _unused in range(256):
                        handle.write(small_block)
                    handle.write(b"0")
                changed = bomb_buffer.getvalue()
                if (
                    len(changed) >= 65_536
                    or int.from_bytes(changed[-4:], "little")
                    != 16_777_217
                ):
                    raise AssertionError("gzip bomb fixture differs")
            else:
                raise AssertionError("unexpected gzip mutation")
            fd = original_open(
                path,
                os.O_WRONLY | os.O_NOFOLLOW | os.O_CLOEXEC,
                dir_fd=dir_fd,
            )
            try:
                written = 0
                while written < len(changed):
                    count = original_pwrite(
                        fd, changed[written:], written
                    )
                    if type(count) is not int or count <= 0:
                        raise AssertionError("gzip mutation write failed")
                    written += count
                original_ftruncate(fd, len(changed))
            finally:
                original_close(fd)
            observation["gzip_mutation_fired"] = True
            observation["gzip_mutation_path"] = relative

        def observed_open(path, flags, *args, **kwargs):
            parent_fd = kwargs.get("dir_fd")
            parent_path = fd_paths.get(parent_fd)
            if type(path) is str:
                if parent_path is not None:
                    full_path = parent_path + "/" + path
                elif path == "raw":
                    fd_paths[parent_fd] = "<data>"
                    fd_directories[parent_fd] = True
                    full_path = "<data>/raw"
                else:
                    full_path = path
            else:
                full_path = None
            relative = (
                self._capture_relative(full_path)
                if type(full_path) is str else None
            )
            access = flags & os.O_ACCMODE
            create = bool(flags & os.O_CREAT)
            if (
                gzip_mutation is not None
                and not observation["gzip_mutation_fired"]
                and access == os.O_RDONLY
                and type(relative) is str
                and relative.endswith(".json.gz")
            ):
                mutate_gzip(path, parent_fd, relative)
            fd = original_open(path, flags, *args, **kwargs)
            if type(full_path) is str:
                fd_paths[fd] = full_path
                fd_directories[fd] = bool(flags & os.O_DIRECTORY)
            if relative == "scan/capture_inventory.json" and create:
                inventory_fds.add(fd)
            if (
                inventory_finalized[0]
                and relative
                and access == os.O_RDONLY
                and not create
            ):
                sequence[0] += 1
                audit_sessions[fd] = {
                    "path": relative,
                    "opened_at": sequence[0],
                    "fstat_count": 0,
                    "identities": [],
                    "reads": [],
                    "closed": False,
                    "closed_at": None,
                }
            if relative:
                observation["open_events"].append(
                    (relative, access, create)
                )
                if create:
                    observation["created_paths"].append(relative)
            return fd

        def observed_fstat(fd):
            result = original_fstat(fd)
            session = audit_sessions.get(fd)
            if session is not None:
                sequence[0] += 1
                session["fstat_count"] += 1
                session["identities"].append((
                    sequence[0],
                    (
                        result.st_dev,
                        result.st_ino,
                        result.st_mode,
                        result.st_nlink,
                        result.st_size,
                    ),
                ))
            return result

        def observed_pread(fd, count, offset):
            result = original_pread(fd, count, offset)
            session = audit_sessions.get(fd)
            if session is not None:
                sequence[0] += 1
                session["reads"].append((
                    sequence[0], offset, len(result)
                ))
            return result

        def observed_close(fd):
            result = original_close(fd)
            if fd in inventory_fds:
                inventory_fds.remove(fd)
                inventory_finalized[0] = True
                sequence[0] += 1
                observation["inventory_finalized_at"] = sequence[0]
            session = audit_sessions.pop(fd, None)
            if session is not None:
                sequence[0] += 1
                session["closed"] = True
                session["closed_at"] = sequence[0]
                observation["audit_sessions"].append(session)
            fd_paths.pop(fd, None)
            fd_directories.pop(fd, None)
            return result

        def fd_label(fd):
            path = fd_paths.get(fd)
            if type(path) is not str:
                return None
            relative = self._capture_relative(path)
            if relative is not None:
                return relative or "<staging>"
            if path.endswith("/historical-foundry-replay"):
                return "<replay>"
            if path.endswith("/raw"):
                return "<raw>"
            if path == "<data>":
                return "<data>"
            return None

        def observed_fsync(fd):
            result = original_fsync(fd)
            label = fd_label(fd) if fd_directories.get(fd) else None
            if label is not None:
                sequence[0] += 1
                observation["directory_fsyncs"].append(
                    (sequence[0], label)
                )
            return result

        def observed_listdir(path="."):
            result = original_listdir(path)
            fd = path if type(path) is int else None
            label = fd_label(fd)
            if label is not None:
                sequence[0] += 1
                observation["tree_enumerations"].append((
                    sequence[0], label, tuple(sorted(result))
                ))
            return result

        def observed_unlink(path, *args, **kwargs):
            parent_fd = kwargs.get("dir_fd")
            parent_path = fd_paths.get(parent_fd)
            full_path = (
                parent_path + "/" + path
                if type(parent_path) is str and type(path) is str
                else path
            )
            relative = (
                self._capture_relative(full_path)
                if type(full_path) is str else None
            )
            if relative and "." in relative.rsplit("/", 1)[-1]:
                observation["member_bytes"][relative] = read_member(
                    path, parent_fd
                )
                observation["quota_lifecycle_events"].append(
                    ("unlink", relative)
                )
            if (
                type(path) is str
                and path.startswith(
                    ".historical-foundry-exchange-spool-"
                )
                and path.endswith(".bin")
            ):
                observation["spool_unlinks"].append(
                    "failure_cleanup" if cleanup_started[0] else "step6"
                )
            return original_unlink(path, *args, **kwargs)

        prior_trace = sys.gettrace()

        def capture_phase(owner):
            if type(owner) is not dict:
                return None
            phases = [
                value
                for key, value in owner.items()
                if type(key) is str
                and key.startswith("_task4b")
                and key.endswith("phase")
            ]
            return phases[0] if len(phases) == 1 else None

        def tracer(frame, event, argument):
            filename = frame.f_code.co_filename
            name = frame.f_code.co_name
            if filename == scan.__file__:
                if (
                    name == "_issue_task4b_capture_replay_event"
                    and event == "call"
                ):
                    payload = frame.f_locals.get("payload")
                    if type(payload) is tuple:
                        observation["payloads"].append(copy.deepcopy(payload))
                elif (
                    name
                    == "_consume_production_historical_window_capture_replay_event_for_storage"
                    and event == "return"
                    and type(argument) is tuple
                ):
                    detached_argument = copy.deepcopy(argument)
                    observation["consumer_payloads"].append(
                        detached_argument
                    )
                    if detached_argument[0] == "root":
                        caller = frame.f_back
                        source_record = (
                            caller.f_locals.get("source_record")
                            if caller is not None else None
                        )
                        joins = (
                            source_record.get("exchange_joins")
                            if type(source_record) is dict else None
                        )
                        indices = detached_argument[1].get(
                            "success_exchange_indices"
                        )
                        if type(joins) is list and type(indices) is tuple:
                            for exchange_index in indices:
                                join = (
                                    joins[exchange_index - 1]
                                    if type(exchange_index) is int
                                    and 0 < exchange_index <= len(joins)
                                    else None
                                )
                                valid = (
                                    type(join) is dict
                                    and tuple(join) == provisional_join_keys
                                )
                                observation[
                                    "provisional_join_checks"
                                ].append(valid)
                                if valid:
                                    observation["provisional_join_ids"][
                                        exchange_index
                                    ] = id(join)
                elif (
                    name == "_new_task4b_exchange_replay"
                    and event == "return"
                ):
                    roots = frame.f_locals.get("post_roots")
                    record = frame.f_locals.get("record")
                    if type(roots) is tuple:
                        observation["post_roots"] = copy.deepcopy(roots)
                    if type(record) is dict:
                        pre = record.get("prefinalization_digests")
                        reconciliation = record.get("replay_digests")
                        if type(pre) is tuple:
                            observation[
                                "bound_prefinalization_digests"
                            ] = tuple(pre)
                        if type(reconciliation) is tuple:
                            observation[
                                "bound_reconciliation_digests"
                            ] = tuple(reconciliation)
                        plan = record.get("plan")
                        frozen_pre_ledger = record.get(
                            "frozen_pre_ledger"
                        )
                        compact_projection = record.get(
                            "compact_projection"
                        )
                        final_anchor = (
                            compact_projection.get("boundaries", {}).get(
                                "final_anchor_header"
                            )
                            if type(compact_projection) is dict else None
                        )
                        if (
                            type(plan) is dict
                            and type(frozen_pre_ledger) is tuple
                            and type(compact_projection) is dict
                            and type(final_anchor) is dict
                        ):
                            observation["digest_plan"] = copy.deepcopy(plan)
                            observation["digest_frozen_pre_ledger"] = (
                                copy.deepcopy(frozen_pre_ledger)
                            )
                            observation["digest_compact_projection"] = (
                                copy.deepcopy(compact_projection)
                            )
                            observation["digest_final_anchor"] = (
                                copy.deepcopy(final_anchor)
                            )
            elif filename == storage.__file__:
                if (
                    name == "_cleanup_task4b_capture_staging"
                    and event == "call"
                ):
                    sequence[0] += 1
                    observation["cleanup_started_at"] = sequence[0]
                    cleanup_started[0] = True
                    observation["cleanup_entries"] += 1
                    observation["audit_read_paths_before_cleanup"] = tuple(
                        path
                        for path, access, create in observation["open_events"]
                        if access == os.O_RDONLY and not create
                    )
                    record = frame.f_locals.get("record")
                    if type(record) is dict:
                        observation["capture_generation"] = record.get(
                            "capture_generation"
                        )
                        observation["capture_state_at_cleanup"] = record.get(
                            "state"
                        )
                        observation["capture_phase_at_cleanup"] = (
                            record.get("_task4b_capture_phase")
                        )
                        observation[
                            "consumerless_owner_keys_at_cleanup"
                        ] = tuple(
                            key
                            for key in (
                                "_task4b_raw_exchange_records",
                                "_task4b_exchange_joins",
                                "_task4b_raw_chunks",
                                "_task4b_typed_chunks",
                                "_task4b_capture_phase",
                            )
                            if key in record
                        )
                        claimed = record.get("claimed_finalization")
                        try:
                            identity = claimed["identity"]
                            detached_identity = dict(identity)
                        except (KeyError, TypeError, ValueError):
                            detached_identity = None
                        if detached_identity is not None:
                            observation[
                                "claimed_finalization_identity"
                            ] = json.loads(json.dumps(
                                detached_identity,
                                ensure_ascii=False,
                                allow_nan=False,
                                sort_keys=True,
                                separators=(",", ":"),
                            ))
                        joins = record.get("_task4b_exchange_joins")
                        raw_chunks = record.get("_task4b_raw_chunks")
                        typed_chunks = record.get("_task4b_typed_chunks")
                        if type(joins) is tuple:
                            observation["transaction_joins"] = copy.deepcopy(
                                joins
                            )
                        if type(raw_chunks) is tuple:
                            observation["transaction_raw_chunks"] = copy.deepcopy(
                                raw_chunks
                            )
                        if type(typed_chunks) is tuple:
                            observation["transaction_typed_chunks"] = copy.deepcopy(
                                typed_chunks
                            )
                elif (
                    (
                        name == "_materialize_task4b_capture_core"
                        and event == "exception"
                    )
                    or (
                        name == "_task4b_install_capture_snapshot"
                        and event == "call"
                    )
                ):
                    owner = frame.f_locals.get("owner")
                    if capture_phase(owner) == "audit_complete":
                        source_record = frame.f_locals.get("source_record")
                        source_joins = (
                            source_record.get("exchange_joins")
                            if type(source_record) is dict else None
                        )
                        owner_joins = (
                            owner.get("_task4b_exchange_joins")
                            if type(owner) is dict else None
                        )
                        source_raw_chunks = (
                            source_record.get("raw_chunks")
                            if type(source_record) is dict else None
                        )
                        source_typed_chunks = (
                            source_record.get("typed_chunks")
                            if type(source_record) is dict else None
                        )
                        if type(source_joins) is list:
                            observation["transaction_joins"] = copy.deepcopy(
                                source_joins
                            )
                        if type(source_raw_chunks) is list:
                            observation[
                                "transaction_raw_chunks"
                            ] = copy.deepcopy(source_raw_chunks)
                        if type(source_typed_chunks) is list:
                            observation[
                                "transaction_typed_chunks"
                            ] = copy.deepcopy(source_typed_chunks)
                        if (
                            type(source_joins) is list
                            and type(owner_joins) is tuple
                            and len(source_joins) == len(owner_joins)
                        ):
                            final_ids = tuple(
                                id(join) for join in source_joins
                            )
                            observation["final_join_ids"] = final_ids
                            observation["owner_source_join_identity"] = all(
                                source_join is owner_join
                                for source_join, owner_join in zip(
                                    source_joins, owner_joins
                                )
                            )
                            observation["join_identity_preserved"] = (
                                all(
                                    type(join) is dict
                                    and tuple(join) == final_join_keys
                                    for join in source_joins
                                )
                                and all(
                                    observation["provisional_join_ids"].get(
                                        exchange_index
                                    ) == id(join)
                                    for exchange_index, join in enumerate(
                                        source_joins, 1
                                    )
                                )
                            )
                        sequence[0] += 1
                        observation["audit_complete_returns"].append((
                            sequence[0], "audit_complete"
                        ))
                elif name == "_prepare_handle" and event == "call":
                    if any(
                        type(value) is type
                        and value.__name__ == "HistoricalRunStagingSnapshot"
                        for value in frame.f_locals.values()
                    ):
                        observation["snapshot_allocations"] += 1
                elif (
                    name in (
                        "_install_quota_reserve_transition",
                        "_install_quota_commit_transition",
                        "_install_quota_abort_transition",
                    )
                    and event in ("call", "return")
                ):
                    caller = frame.f_back
                    entry = (
                        caller.f_locals.get("entry")
                        if caller is not None else None
                    )
                    parent_path = (
                        fd_paths.get(entry.get("parent_fd"))
                        if type(entry) is dict else None
                    )
                    full_path = (
                        parent_path + "/" + entry["name"]
                        if type(parent_path) is str
                        and type(entry.get("name")) is str
                        else None
                    )
                    relative = (
                        self._capture_relative(full_path)
                        if type(full_path) is str else None
                    )
                    if event == "call":
                        observation["quota_transition_calls"].append(name)
                        observation["quota_lifecycle_events"].append(
                            (name, relative)
                        )
                    else:
                        next_quota = frame.f_locals.get("next_quota")
                        reservation = (
                            next_quota.get("reservation")
                            if type(next_quota) is dict else None
                        )
                        observation["quota_transition_rows"].append({
                            "transition": name,
                            "path": relative,
                            "committed_physical_bytes": next_quota.get(
                                "committed_physical_bytes"
                            ) if type(next_quota) is dict else None,
                            "committed_members": next_quota.get(
                                "committed_members"
                            ) if type(next_quota) is dict else None,
                            "provisional_physical_bytes": next_quota.get(
                                "provisional_physical_bytes"
                            ) if type(next_quota) is dict else None,
                            "provisional_members": next_quota.get(
                                "provisional_members"
                            ) if type(next_quota) is dict else None,
                            "reservation_physical_bytes": reservation.get(
                                "physical_bytes"
                            ) if type(reservation) is dict else None,
                        })
                elif (
                    "source" in name and "move" in name and event == "call"
                ):
                    observation["source_move_calls"] += 1
                elif name in observation["helper_calls"] and event == "call":
                    observation["helper_calls"][name] += 1
            return tracer

        restore = None
        snapshot = None
        try:
            capability = fixture.mint()
            if before_materialize is not None:
                restore = before_materialize(storage, observation)
            with mock.patch.object(
                storage.os, "open", side_effect=observed_open
            ) as patched_open, mock.patch.object(
                storage.os, "unlink", side_effect=observed_unlink
            ) as patched_unlink, mock.patch.object(
                storage.os, "fstat", side_effect=observed_fstat
            ), mock.patch.object(
                storage.os, "pread", side_effect=observed_pread
            ), mock.patch.object(
                storage.os, "close", side_effect=observed_close
            ), mock.patch.object(
                storage.os, "fsync", side_effect=observed_fsync
            ), mock.patch.object(
                storage.os, "listdir", side_effect=observed_listdir
            ):
                supported = set(storage.os.supports_dir_fd)
                supported.discard(original_open)
                supported.discard(original_unlink)
                supported.add(patched_open)
                supported.add(patched_unlink)
                with mock.patch.object(
                    storage.os, "supports_dir_fd", supported
                ):
                    sys.settrace(tracer)
                    try:
                        snapshot = scan._materialize_historical_window_staging_snapshot(
                            capability=capability
                        )
                        observation["snapshot_type"] = type(snapshot)
                        snapshot.close()
                        snapshot.close()
                    except BaseException as error:
                        if type(error) is rpc._ArchiveRpcError:
                            observation["error_pair"] = (
                                error.reason_code, error.failure_kind
                            )
                        else:
                            observation["error"] = error
        finally:
            sys.settrace(prior_trace)
            if restore is not None:
                restore()
            if snapshot is not None:
                snapshot.close()
            if fixture.data_dir is not None and fixture.data_dir.exists():
                observation["data_entries"] = tuple(
                    path.name for path in fixture.data_dir.iterdir()
                )
            fixture.capability = None
            fixture.close()
        return observation

    def test_slice5_typed_guards_atomic_planner_and_abc_gzip_kat(self):
        storage = importlib.import_module(
            "scripts.historical_foundry_storage"
        )

        self.assertEqual(
            storage._plan_historical_typed_root_append(
                current_decoded_size=2,
                current_row_count=0,
                candidate_row_encoded_lengths=(16_777_214,),
            ),
            ("append_current", 16_777_216),
        )
        self.assertEqual(
            storage._plan_historical_typed_root_append(
                current_decoded_size=16_777_210,
                current_row_count=1,
                candidate_row_encoded_lengths=(6,),
            ),
            ("flush_then_append", 8),
        )
        self.assertEqual(
            storage._require_historical_gzip_member_size(
                byte_count=16_842_752
            ),
            16_842_752,
        )
        self.assertEqual(
            storage._require_historical_capture_inventory_size(
                byte_count=16_777_216
            ),
            16_777_216,
        )

        invalid_calls = (
            lambda: storage._plan_historical_typed_root_append(
                current_decoded_size=2,
                current_row_count=0,
                candidate_row_encoded_lengths=(16_777_215,),
            ),
            lambda: storage._plan_historical_typed_root_append(
                current_decoded_size=3,
                current_row_count=0,
                candidate_row_encoded_lengths=(1,),
            ),
            lambda: storage._plan_historical_typed_root_append(
                current_decoded_size=2,
                current_row_count=False,
                candidate_row_encoded_lengths=(1,),
            ),
            lambda: storage._plan_historical_typed_root_append(
                current_decoded_size=2,
                current_row_count=0,
                candidate_row_encoded_lengths=[1],
            ),
            lambda: storage._plan_historical_typed_root_append(
                current_decoded_size=2,
                current_row_count=0,
                candidate_row_encoded_lengths=(0,),
            ),
            lambda: storage._require_historical_gzip_member_size(
                byte_count=16_842_753
            ),
            lambda: storage._require_historical_gzip_member_size(
                byte_count=True
            ),
            lambda: storage._require_historical_capture_inventory_size(
                byte_count=16_777_217
            ),
            lambda: storage._require_historical_capture_inventory_size(
                byte_count=True
            ),
        )
        for index, call in enumerate(invalid_calls):
            with self.subTest(invalid=index):
                with self.assertRaises(ValueError):
                    call()

        buffer = io.BytesIO()
        with gzip.GzipFile(
            filename="",
            mode="wb",
            compresslevel=9,
            fileobj=buffer,
            mtime=0,
        ) as handle:
            handle.write(b"abc")
        self.assertEqual(
            buffer.getvalue().hex(),
            "1f8b08000000000002ff4b4c4a0600c241243503000000",
        )

    def test_slice5_natural_typed_chunks_are_atomic_and_extend_22_to_24(
        self,
    ):
        observed = self._run_member_capture()
        self.assertIsNone(observed["error_pair"])
        self.assertIsNone(observed["error"])
        self.assertEqual(observed["snapshot_allocations"], 1)
        self.assertIs(
            observed["snapshot_type"],
            importlib.import_module(
                "scripts.historical_foundry_storage"
            ).HistoricalRunStagingSnapshot,
        )
        typed_members = {
            path: payload
            for path, payload in observed["member_bytes"].items()
            if path.endswith(".json.gz")
        }
        self.assertEqual(
            set(typed_members),
            {
                role + "/00000001.json.gz"
                for role in self._TYPED_ROLES
            },
        )
        expected_counts = {
            "headers": 2, "reserves": 4, "prices": 2, "fees": 2
        }
        rows_by_role = {}
        for role in self._TYPED_ROLES:
            path = role + "/00000001.json.gz"
            physical = typed_members[path]
            self.assertLessEqual(len(physical), 16_842_752)
            self.assertEqual(
                physical[:10].hex(), "1f8b08000000000002ff"
            )
            decoded = gzip.decompress(physical)
            self.assertLessEqual(len(decoded), 16_777_216)
            rows = json.loads(decoded.decode("utf-8"))
            self.assertIs(type(rows), list)
            self.assertTrue(rows)
            self.assertEqual(len(rows), expected_counts[role])
            canonical = json.dumps(
                rows,
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
            self.assertEqual(decoded, canonical)
            self.assertFalse(decoded.endswith(b"\n"))
            rows_by_role[role] = rows
        self.assertEqual(
            sum(len(rows) for rows in rows_by_role.values()), 10
        )
        self.assertEqual(
            [row["number"] for row in rows_by_role["headers"]],
            sorted(row["number"] for row in rows_by_role["headers"]),
        )
        self.assertEqual(
            [row["block_number"] for row in rows_by_role["reserves"]],
            sorted(
                row["block_number"] for row in rows_by_role["reserves"]
            ),
        )
        for offset in range(0, len(rows_by_role["reserves"]), 2):
            self.assertEqual(
                tuple(
                    row["venue_id"]
                    for row in rows_by_role["reserves"][offset:offset + 2]
                ),
                ("uniswap_v2", "sushiswap_v2"),
            )
        for role in ("prices", "fees"):
            self.assertEqual(
                [row["block_number"] for row in rows_by_role[role]],
                sorted(row["block_number"] for row in rows_by_role[role]),
            )

        joins = observed["transaction_joins"]
        self.assertTrue(joins)
        self.assertEqual(
            len(observed["provisional_join_ids"]), len(joins)
        )
        self.assertEqual(
            observed["provisional_join_checks"], [True] * len(joins)
        )
        self.assertEqual(len(observed["final_join_ids"]), len(joins))
        self.assertTrue(observed["join_identity_preserved"])
        self.assertTrue(observed["owner_source_join_identity"])
        compact_keys = (
            "schema", "exchange_index", "logical_batch_index",
            "attempt_index", "request_byte_count", "request_sha256",
            "request_ids", "wire_byte_count", "wire_sha256",
            "decoded_byte_count", "decoded_sha256", "response_ids",
            "spool_member_index", "spool_offset", "spool_length",
            "spool_member_sha256",
        )
        expected_keys = compact_keys + (
            "segment", "segment_local_index", "leaf_index",
            "wire_hash_authority", "raw_chunk_path", "raw_chunk_offset",
            "typed_role", "typed_chunk_refs",
        )
        self.assertEqual(len(expected_keys), 24)
        self.assertTrue(all(tuple(join) == expected_keys for join in joins))
        by_root = {}
        for join in joins:
            role = join["typed_role"]
            self.assertIn(
                role,
                (
                    "anchor_stage", "lower_observation", "headers",
                    "reserves", "prices", "fees", "final_anchor",
                ),
            )
            refs = join["typed_chunk_refs"]
            self.assertIs(type(refs), list)
            if role in self._TYPED_ROLES:
                self.assertEqual(len(refs), 1)
                self.assertEqual(
                    tuple(refs[0]),
                    ("path", "first_row_index", "row_count"),
                )
                self.assertIn(refs[0]["path"], typed_members)
            else:
                self.assertEqual(refs, [])
            by_root.setdefault(join["logical_batch_index"], []).append(join)
        for root_joins in by_root.values():
            if root_joins[0]["typed_role"] in self._TYPED_ROLES:
                self.assertTrue(all(
                    join["typed_chunk_refs"]
                    == root_joins[0]["typed_chunk_refs"]
                    for join in root_joins
                ))
        final_anchor_roots = [
            root for root in observed["post_roots"]
            if root.get("segment") == "window_root"
            and root.get("kind") == "final_anchor"
        ]
        final_anchor_events = [
            payload for payload in observed["payloads"]
            if payload[0] == "root" and payload[2] == "final_anchor"
        ]
        self.assertEqual(len(final_anchor_roots), 1)
        self.assertEqual(len(final_anchor_events), 1)
        self.assertEqual(final_anchor_events[0][3:], (None, 0, None))
        final_anchor_logical_index = final_anchor_roots[0][
            "logical_batch_index"
        ]
        self.assertTrue(by_root[final_anchor_logical_index])
        self.assertTrue(all(
            join["typed_role"] == "final_anchor"
            and join["typed_chunk_refs"] == []
            for join in by_root[final_anchor_logical_index]
        ))
        self.assertNotIn("final_anchor", {
            path.split("/", 1)[0] for path in typed_members
        })
        typed_inventory = observed["transaction_typed_chunks"]
        self.assertEqual(len(typed_inventory), 4)
        self.assertEqual(
            tuple(row["role"] for row in typed_inventory),
            self._TYPED_ROLES,
        )
        for row in typed_inventory:
            self.assertEqual(
                tuple(row),
                (
                    "path", "role", "chunk_index", "block_start",
                    "block_stop", "row_count", "decoded_byte_count",
                    "decoded_sha256", "gzip_byte_count", "gzip_sha256",
                ),
            )
            physical = typed_members[row["path"]]
            decoded = gzip.decompress(physical)
            self.assertEqual(row["row_count"], len(json.loads(decoded)))
            self.assertEqual(row["decoded_byte_count"], len(decoded))
            self.assertEqual(
                row["decoded_sha256"], hashlib.sha256(decoded).hexdigest()
            )
            self.assertEqual(row["gzip_byte_count"], len(physical))
            self.assertEqual(
                row["gzip_sha256"], hashlib.sha256(physical).hexdigest()
            )
        self.assertEqual(observed["data_entries"], ())

    def test_slice5_gzip_profile_corruption_trailing_crc_and_bomb_reject(
        self,
    ):
        for mutation in ("trailing", "crc", "profile", "bomb"):
            with self.subTest(mutation=mutation):
                observed = self._run_member_capture(
                    gzip_mutation=mutation
                )
                self.assertTrue(observed["gzip_mutation_fired"])
                self.assertEqual(
                    observed["error_pair"],
                    (
                        "authority_mismatch",
                        "historical_window_spool_handoff_failed",
                    ),
                )
                if "scan/capture_inventory.json" in observed["member_bytes"]:
                    inventory = json.loads(observed["member_bytes"][
                        "scan/capture_inventory.json"
                    ])
                    row = next(
                        typed_row
                        for typed_row in inventory["typed_chunks"]
                        if typed_row["path"]
                        == observed["gzip_mutation_path"]
                    )
                    physical = observed["member_bytes"][row["path"]]
                    self.assertNotEqual(
                        hashlib.sha256(physical).hexdigest(),
                        row["gzip_sha256"],
                    )
                self.assertNotIn("step6", observed["spool_unlinks"])
                self.assertEqual(observed["audit_complete_returns"], [])
                reserve_name = "_install_quota_reserve_transition"
                commit_name = "_install_quota_commit_transition"
                abort_name = "_install_quota_abort_transition"
                transition_rows = observed["quota_transition_rows"]
                self.assertEqual(
                    observed["quota_transition_calls"],
                    [row["transition"] for row in transition_rows],
                )
                self.assertGreater(len(transition_rows), 2)
                self.assertEqual(len(transition_rows) % 2, 0)
                transition_pairs = [
                    transition_rows[index:index + 2]
                    for index in range(0, len(transition_rows), 2)
                ]
                self.assertTrue(all(
                    pair[0]["transition"] == reserve_name
                    and pair[1]["transition"] == commit_name
                    and pair[0]["path"] == pair[1]["path"]
                    for pair in transition_pairs
                ))
                self.assertNotIn(abort_name, observed["quota_transition_calls"])
                transition_paths = [
                    pair[0]["path"] for pair in transition_pairs
                ]
                self.assertNotIn(None, transition_paths)
                self.assertEqual(
                    len(transition_paths), len(set(transition_paths))
                )
                self.assertIn(
                    observed["gzip_mutation_path"], transition_paths
                )
                committed_bytes = transition_rows[0][
                    "committed_physical_bytes"
                ]
                committed_members = transition_rows[0][
                    "committed_members"
                ]
                for pair_index, (reserve_row, terminal_row) in enumerate(
                    transition_pairs
                ):
                    physical_bytes = reserve_row[
                        "reservation_physical_bytes"
                    ]
                    self.assertIs(type(physical_bytes), int)
                    self.assertGreater(physical_bytes, 0)
                    self.assertEqual(
                        (
                            reserve_row["committed_physical_bytes"],
                            reserve_row["committed_members"],
                            reserve_row["provisional_physical_bytes"],
                            reserve_row["provisional_members"],
                        ),
                        (
                            committed_bytes, committed_members,
                            physical_bytes, 1,
                        ),
                    )
                    committed_bytes += physical_bytes
                    committed_members += 1
                    self.assertEqual(
                        (
                            terminal_row["committed_physical_bytes"],
                            terminal_row["committed_members"],
                            terminal_row["provisional_physical_bytes"],
                            terminal_row["provisional_members"],
                            terminal_row["reservation_physical_bytes"],
                        ),
                        (
                            committed_bytes, committed_members, 0, 0, None,
                        ),
                    )
                lifecycle = observed["quota_lifecycle_events"]
                self.assertLess(
                    lifecycle.index((
                        commit_name, observed["gzip_mutation_path"]
                    )),
                    lifecycle.index((
                        "unlink", observed["gzip_mutation_path"]
                    )),
                )
                self.assertEqual(observed["snapshot_allocations"], 0)
                self.assertEqual(observed["data_entries"], ())

    def test_slice5_production_uses_captured_typed_and_size_helpers_after_rebind(
        self,
    ):
        replacement_calls = {
            "_plan_historical_typed_root_append": 0,
            "_require_historical_gzip_member_size": 0,
            "_require_historical_capture_inventory_size": 0,
        }

        def before(storage, _observation):
            originals = {
                name: getattr(storage, name) for name in replacement_calls
            }
            for name in replacement_calls:
                def replacement(*_args, _name=name, **_kwargs):
                    replacement_calls[_name] += 1
                    raise AssertionError(_name + " rebound helper was called")

                setattr(storage, name, replacement)

            def restore():
                for name, original in originals.items():
                    setattr(storage, name, original)

            return restore

        observed = self._run_member_capture(
            before_materialize=before
        )
        self.assertIsNone(observed["error_pair"])
        self.assertIsNone(observed["error"])
        self.assertEqual(observed["snapshot_allocations"], 1)
        self.assertEqual(replacement_calls, {
            "_plan_historical_typed_root_append": 0,
            "_require_historical_gzip_member_size": 0,
            "_require_historical_capture_inventory_size": 0,
        })
        self.assertTrue(all(
            count > 0 for count in observed["helper_calls"].values()
        ))
        self.assertTrue(any(
            path.endswith(".json.gz")
            for path in observed["member_bytes"]
        ))
        self.assertIn(
            "scan/capture_inventory.json", observed["member_bytes"]
        )
        self.assertEqual(observed["data_entries"], ())


class HistoricalFoundryStorageTask4bInventoryTests(unittest.TestCase):
    def test_slice5_inventory_is_self_contained_reconstructible_and_audited_through_step5(
        self,
    ):
        observer = HistoricalFoundryStorageTask4bTypedChunkTests()
        observed = observer._run_member_capture()
        self.assertIsNone(observed["error_pair"])
        self.assertIsNone(observed["error"])
        self.assertEqual(observed["snapshot_allocations"], 1)
        inventory_path = "scan/capture_inventory.json"
        self.assertIn(inventory_path, observed["member_bytes"])
        inventory_bytes = observed["member_bytes"][inventory_path]
        inventory = json.loads(inventory_bytes.decode("utf-8"))
        expected_top_fields = (
            "schema", "source_identity", "receipt_inventory_sha256",
            "prefinalization_digests", "reconciliation_digests", "range",
            "request_range", "configs", "raw_chunks", "typed_chunks",
            "post_roots", "exchanges",
        )
        self.assertEqual(set(inventory), set(expected_top_fields))
        self.assertEqual(
            tuple(inventory), tuple(sorted(expected_top_fields))
        )
        self.assertEqual(
            inventory["schema"],
            "historical_foundry_capture_inventory/v1",
        )
        self.assertFalse(inventory_bytes.endswith(b"\n"))
        self.assertLessEqual(len(inventory_bytes), 16_777_216)

        source_identity = inventory["source_identity"]
        self.assertEqual(
            set(source_identity),
            {
                "schema", "repository_head", "python", "sources",
                "project_inputs", "toolchain", "executor_artifact",
                "resource_policy", "endpoint_identity", "collection",
            },
        )
        self.assertEqual(
            source_identity["schema"],
            "historical_foundry_capture_source_identity/v1",
        )
        self.assertNotIn("configs", source_identity)
        claimed_identity = observed["claimed_finalization_identity"]
        self.assertIs(type(claimed_identity), dict)
        claimed_configs = claimed_identity["configs"]
        expected_source_identity = dict(claimed_identity)
        expected_source_identity.pop("configs")
        expected_source_identity["schema"] = (
            "historical_foundry_capture_source_identity/v1"
        )
        self.assertEqual(source_identity, expected_source_identity)

        config_rows = inventory["configs"]
        self.assertIs(type(config_rows), list)
        self.assertEqual(
            tuple(row["role"] for row in config_rows),
            ("policy", "authority", "toolchain"),
        )
        self.assertEqual(
            tuple(row["path"] for row in config_rows),
            ("policy.json", "authority.json", "toolchain.json"),
        )
        policy_id = config_rows[0]["policy_id"]
        self.assertRegex(policy_id, r"^policy:[0-9a-f]{64}$")
        for index, row in enumerate(config_rows):
            self.assertEqual(
                tuple(row),
                tuple(sorted((
                    "role", "path", "schema", "byte_count", "sha256",
                    "policy_id",
                ))),
            )
            self.assertEqual(
                row["policy_id"], policy_id if index == 0 else None
            )
            payload = observed["member_bytes"][row["path"]]
            self.assertEqual(row["byte_count"], len(payload))
            self.assertEqual(
                row["sha256"], hashlib.sha256(payload).hexdigest()
            )

        raw_keys = (
            "path", "byte_count", "sha256", "exchange_index_start",
            "exchange_index_stop", "exchange_count", "request_id_start",
            "request_id_stop",
        )
        typed_keys = (
            "path", "role", "chunk_index", "block_start", "block_stop",
            "row_count", "decoded_byte_count", "decoded_sha256",
            "gzip_byte_count", "gzip_sha256",
        )
        self.assertTrue(inventory["raw_chunks"])
        self.assertTrue(inventory["typed_chunks"])
        self.assertTrue(all(
            tuple(row) == tuple(sorted(raw_keys))
            for row in inventory["raw_chunks"]
        ))
        self.assertTrue(all(
            tuple(row) == tuple(sorted(typed_keys))
            for row in inventory["typed_chunks"]
        ))

        member_paths = {
            row["path"] for row in config_rows
        } | {
            row["path"] for row in inventory["raw_chunks"]
        } | {
            row["path"] for row in inventory["typed_chunks"]
        } | {inventory_path}
        self.assertEqual(
            member_paths, set(observed["member_bytes"])
        )
        inventory_create_positions = [
            index
            for index, (path, _access, create) in enumerate(
                observed["open_events"]
            )
            if path == inventory_path and create
        ]
        self.assertEqual(len(inventory_create_positions), 1)
        after_inventory = observed["open_events"][
            inventory_create_positions[0] + 1:
        ]
        audit_read_paths = [
            path
            for path, access, create in after_inventory
            if access == os.O_RDONLY and not create
        ]
        for path in sorted(member_paths):
            self.assertGreaterEqual(audit_read_paths.count(path), 1)
        self.assertTrue(set(member_paths).issubset(
            set(observed["audit_read_paths_before_cleanup"])
        ))
        sessions_by_path = {}
        for session in observed["audit_sessions"]:
            sessions_by_path.setdefault(session["path"], []).append(session)

        def complete_descriptor_session(session, expected_size):
            identity_events = session["identities"]
            identities = [identity for _position, identity in identity_events]
            reads = session["reads"]
            if (
                not session["closed"]
                or session["fstat_count"] < 2
                or len(identities) < 2
                or not reads
                or any(identity != identities[0] for identity in identities)
                or not stat.S_ISREG(identities[0][2])
                or identities[0][3] != 1
                or identities[0][4] != expected_size
                or not (
                    session["opened_at"] < identity_events[0][0]
                    < min(position for position, _offset, _length in reads)
                    <= max(position for position, _offset, _length in reads)
                    < identity_events[-1][0] < session["closed_at"]
                )
            ):
                return False
            cursor = 0
            for _position, offset, length in sorted(
                reads, key=lambda row: (row[1], row[0])
            ):
                if offset > cursor:
                    return False
                cursor = max(cursor, offset + length)
            return cursor >= expected_size

        for path in sorted(member_paths):
            self.assertIn(path, sessions_by_path)
            self.assertTrue(all(
                complete_descriptor_session(
                    session, len(observed["member_bytes"][path])
                )
                for session in sessions_by_path[path]
            ))

        inventory_finalized_at = observed.get("inventory_finalized_at")
        cleanup_started_at = observed.get("cleanup_started_at")
        self.assertIs(type(inventory_finalized_at), int)
        self.assertIs(type(cleanup_started_at), int)
        self.assertEqual(len(observed["audit_complete_returns"]), 1)
        audit_complete_at, audit_result = observed[
            "audit_complete_returns"
        ][0]
        self.assertEqual(audit_result, "audit_complete")
        self.assertIsNone(observed["capture_phase_at_cleanup"])
        self.assertEqual(
            observed["consumerless_owner_keys_at_cleanup"], ()
        )
        self.assertLess(inventory_finalized_at, audit_complete_at)
        self.assertLess(audit_complete_at, cleanup_started_at)
        freeze_audit_sessions = [
            session for session in observed["audit_sessions"]
            if session["opened_at"] < audit_complete_at
        ]
        self.assertTrue(freeze_audit_sessions)
        self.assertTrue(all(
            session["closed_at"] < audit_complete_at
            for session in freeze_audit_sessions
        ))

        role_members = {}
        staging_members = set()
        for path in member_paths:
            if "/" in path:
                role, basename = path.split("/", 1)
                role_members.setdefault(role, set()).add(basename)
                staging_members.add(role)
            else:
                staging_members.add(path)
        expected_roles = {
            "rpc", "scan", "headers", "reserves", "prices", "fees"
        }
        self.assertEqual(set(role_members), expected_roles)
        post_inventory_fsyncs = [
            (position, label)
            for position, label in observed["directory_fsyncs"]
            if inventory_finalized_at < position < audit_complete_at
        ]
        fsync_positions = {}
        for position, label in post_inventory_fsyncs:
            fsync_positions.setdefault(label, []).append(position)
        self.assertTrue(expected_roles.issubset(fsync_positions))
        for label in ("<staging>", "<replay>", "<raw>", "<data>"):
            self.assertIn(label, fsync_positions)
        self.assertLess(
            max(max(fsync_positions[role]) for role in expected_roles),
            min(fsync_positions["<staging>"]),
        )
        self.assertLess(
            max(fsync_positions["<staging>"]),
            min(fsync_positions["<replay>"]),
        )
        self.assertLess(
            max(fsync_positions["<replay>"]),
            min(fsync_positions["<raw>"]),
        )
        self.assertLess(
            max(fsync_positions["<raw>"]),
            min(fsync_positions["<data>"]),
        )
        enumerations = {}
        for position, label, names in observed["tree_enumerations"]:
            if inventory_finalized_at < position < audit_complete_at:
                enumerations.setdefault(label, []).append(set(names))
        self.assertIn("<staging>", enumerations)
        self.assertIn(staging_members, enumerations["<staging>"])
        for role, basenames in role_members.items():
            self.assertIn(role, enumerations)
            self.assertIn(basenames, enumerations[role])
        all_fsync_positions = [
            position for position, _label in post_inventory_fsyncs
        ]
        enumeration_positions = [
            position
            for position, _label, _names
            in observed["tree_enumerations"]
            if inventory_finalized_at < position < audit_complete_at
        ]
        session_open_positions = [
            session["opened_at"] for session in freeze_audit_sessions
        ]
        session_close_positions = [
            session["closed_at"] for session in freeze_audit_sessions
        ]
        self.assertLess(inventory_finalized_at, min(all_fsync_positions))
        self.assertLess(max(all_fsync_positions), min(enumeration_positions))
        self.assertLess(
            max(enumeration_positions), min(session_open_positions)
        )
        self.assertLess(max(session_close_positions), audit_complete_at)

        def canonical(value):
            return json.dumps(
                value,
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")

        def decimal_token(value):
            sign, digits, exponent = value.as_tuple()
            coefficient = 0
            for digit in digits:
                coefficient = coefficient * 10 + digit
            if coefficient == 0:
                self.assertEqual(sign, 0)
                return "0"
            self.assertEqual(sign, 0)
            if exponent >= 0:
                self.assertEqual((coefficient, exponent), (1, 0))
                return "1"
            scale = 10 ** (-exponent)
            self.assertLessEqual(coefficient, scale)
            if coefficient == scale:
                return "1"
            trailing = 0
            for digit in reversed(digits):
                if digit != 0:
                    break
                trailing += 1
            compact_digits = (
                digits[:len(digits) - trailing] if trailing else digits
            )
            compact_exponent = exponent + trailing
            count = len(compact_digits)
            scientific_exponent = compact_exponent + count - 1
            digit_text = "".join(str(digit) for digit in compact_digits)
            mantissa = (
                digit_text
                if count == 1
                else digit_text[0] + "." + digit_text[1:]
            )
            return mantissa + "e" + str(scientific_exponent)

        def canonical_hash_projection(value):
            if type(value) is dict:
                return {
                    key: canonical_hash_projection(nested)
                    for key, nested in value.items()
                }
            if type(value) in (list, tuple):
                return [
                    canonical_hash_projection(nested) for nested in value
                ]
            if type(value) is Decimal:
                return decimal_token(value)
            return value

        def independently_typed_hash(domain, value):
            payload = json.dumps(
                canonical_hash_projection(value),
                ensure_ascii=False,
                allow_nan=False,
                sort_keys=True,
                separators=(",", ":"),
            ).encode("utf-8")
            frame = len(payload).to_bytes(8, "big") + payload
            return hashlib.sha256(domain + b"\0" + frame).hexdigest()

        def inventory_digest(domain, rows):
            digest = hashlib.sha256()
            digest.update(domain)
            digest.update(b"\0")
            for row in rows:
                payload = canonical(row)
                digest.update(len(payload).to_bytes(8, "big"))
                digest.update(payload)
            return digest.hexdigest()

        compact_keys = (
            "schema", "exchange_index", "logical_batch_index",
            "attempt_index", "request_byte_count", "request_sha256",
            "request_ids", "wire_byte_count", "wire_sha256",
            "decoded_byte_count", "decoded_sha256", "response_ids",
            "spool_member_index", "spool_offset", "spool_length",
            "spool_member_sha256",
        )
        extension_keys = (
            "segment", "segment_local_index", "leaf_index",
            "wire_hash_authority", "raw_chunk_path", "raw_chunk_offset",
            "typed_role", "typed_chunk_refs",
        )
        exchanges = inventory["exchanges"]
        self.assertTrue(exchanges)
        self.assertTrue(all(
            set(exchange) == set(compact_keys + extension_keys)
            for exchange in exchanges
        ))
        self.assertTrue(all(
            tuple(exchange) == tuple(sorted(compact_keys + extension_keys))
            for exchange in exchanges
        ))
        self.assertEqual(
            tuple(exchange["exchange_index"] for exchange in exchanges),
            tuple(range(1, len(exchanges) + 1)),
        )
        receipt_rows = []
        post_leaves = []
        raw_payloads = {
            row["path"]: observed["member_bytes"][row["path"]]
            for row in inventory["raw_chunks"]
        }
        all_request_ids = []
        for exchange in exchanges:
            receipt = {key: exchange[key] for key in compact_keys}
            receipt["schema"] = (
                "historical_foundry_exchange_spool_receipt/v1"
            )
            receipt_rows.append(receipt)
            leaf = {
                "schema": "historical_foundry_leaf_ledger/v1",
                "segment": exchange["segment"],
                "segment_local_index": exchange["segment_local_index"],
                "leaf_index": exchange["leaf_index"],
                "request_ids": exchange["request_ids"],
                "request_count": len(exchange["request_ids"]),
                "canonical_request_sha256": exchange["request_sha256"],
                "response_ids": exchange["response_ids"],
                "exchange_index": exchange["exchange_index"],
                "logical_batch_index": exchange["logical_batch_index"],
                "attempt_index": exchange["attempt_index"],
                "request_byte_count": exchange["request_byte_count"],
                "decoded_byte_count": exchange["decoded_byte_count"],
                "decoded_sha256": exchange["decoded_sha256"],
                "wire_byte_count": exchange["wire_byte_count"],
                "wire_sha256": exchange["wire_sha256"],
                "wire_hash_authority": exchange["wire_hash_authority"],
                "spool_member_index": exchange["spool_member_index"],
                "spool_offset": exchange["spool_offset"],
                "spool_length": exchange["spool_length"],
                "spool_member_sha256": exchange["spool_member_sha256"],
            }
            post_leaves.append(leaf)
            payload = raw_payloads[exchange["raw_chunk_path"]]
            cursor = exchange["raw_chunk_offset"]
            request_size = int.from_bytes(payload[cursor:cursor + 8], "big")
            request_start = cursor + 8
            request_stop = request_start + request_size
            decoded_size = int.from_bytes(
                payload[request_stop:request_stop + 8], "big"
            )
            decoded_start = request_stop + 8
            frame_stop = decoded_start + decoded_size
            request = payload[request_start:request_stop]
            decoded = payload[decoded_start:frame_stop]
            frame_bytes = payload[cursor:frame_stop]
            self.assertEqual(request_size, exchange["request_byte_count"])
            self.assertEqual(decoded_size, exchange["decoded_byte_count"])
            self.assertEqual(
                hashlib.sha256(request).hexdigest(),
                exchange["request_sha256"],
            )
            self.assertEqual(
                hashlib.sha256(decoded).hexdigest(),
                exchange["decoded_sha256"],
            )
            self.assertEqual(len(frame_bytes), exchange["spool_length"])
            self.assertEqual(
                hashlib.sha256(frame_bytes).hexdigest(),
                exchange["spool_member_sha256"],
            )
            all_request_ids.extend(exchange["request_ids"])
        self.assertEqual(
            inventory_digest(
                b"historical_foundry_exchange_spool_receipt_inventory/v1",
                receipt_rows,
            ),
            inventory["receipt_inventory_sha256"],
        )

        post_roots = inventory["post_roots"]
        self.assertEqual(len(post_roots), len(observed["post_roots"]))
        self.assertEqual(
            canonical(post_roots), canonical(observed["post_roots"])
        )
        leaves_by_root = {}
        for leaf in post_leaves:
            leaves_by_root.setdefault(
                leaf["logical_batch_index"], []
            ).append(leaf)
        for root in post_roots:
            leaves = leaves_by_root[root["logical_batch_index"]]
            self.assertEqual(
                tuple(leaf["exchange_index"] for leaf in leaves),
                tuple(root["success_exchange_indices"]),
            )
            self.assertEqual(root["leaf_count"], len(leaves))
            self.assertEqual(
                root["leaf_ledger_sha256"],
                inventory_digest(
                    b"historical_foundry_leaf_ledger/v1", leaves
                ),
            )

        typed_rows_by_path = {}
        for row in inventory["typed_chunks"]:
            physical = observed["member_bytes"][row["path"]]
            self.assertEqual(row["gzip_byte_count"], len(physical))
            self.assertEqual(
                row["gzip_sha256"], hashlib.sha256(physical).hexdigest()
            )
            decoded = gzip.decompress(physical)
            self.assertEqual(row["decoded_byte_count"], len(decoded))
            self.assertEqual(
                row["decoded_sha256"], hashlib.sha256(decoded).hexdigest()
            )
            typed_rows = json.loads(decoded.decode("utf-8"))
            self.assertEqual(decoded, canonical(typed_rows))
            self.assertEqual(row["row_count"], len(typed_rows))
            typed_rows_by_path[row["path"]] = typed_rows

        typed_domains = {
            "headers": b"historical_foundry_header_inventory/v1",
            "reserves": b"historical_foundry_reserve_inventory/v1",
            "prices": b"historical_foundry_price_inventory/v1",
            "fees": b"historical_foundry_fee_inventory/v1",
        }
        roots_by_index = {
            root["logical_batch_index"]: root for root in post_roots
        }
        exchanges_by_root = {}
        for exchange in exchanges:
            exchanges_by_root.setdefault(
                exchange["logical_batch_index"], []
            ).append(exchange)
        for logical_index, root_exchanges in exchanges_by_root.items():
            root = roots_by_index[logical_index]
            role = root_exchanges[0]["typed_role"]
            self.assertTrue(all(
                exchange["typed_role"] == role
                for exchange in root_exchanges
            ))
            refs = root_exchanges[0]["typed_chunk_refs"]
            self.assertTrue(all(
                exchange["typed_chunk_refs"] == refs
                for exchange in root_exchanges
            ))
            if role not in typed_domains:
                self.assertEqual(refs, [])
                continue
            self.assertEqual(len(refs), 1)
            self.assertEqual(
                tuple(refs[0]),
                tuple(sorted(("path", "first_row_index", "row_count"))),
            )
            rows = typed_rows_by_path[refs[0]["path"]][
                refs[0]["first_row_index"]:
                refs[0]["first_row_index"] + refs[0]["row_count"]
            ]
            self.assertEqual(len(rows), root["typed_row_count"])
            self.assertEqual(
                inventory_digest(typed_domains[role], rows),
                root["typed_logical_sha256"],
            )

        request_range = inventory["request_range"]
        self.assertEqual(
            tuple(request_range),
            tuple(sorted((
                "first_request_id", "last_request_id", "request_count",
            ))),
        )
        self.assertEqual(
            all_request_ids,
            list(range(1, request_range["last_request_id"] + 1)),
        )
        self.assertEqual(request_range["first_request_id"], 1)
        self.assertEqual(
            request_range["request_count"], request_range["last_request_id"]
        )
        rebuilt_collection = {
            "logical_batch_count": len(post_roots),
            "successful_exchange_count": len(exchanges),
            "request_count": sum(
                len(exchange["request_ids"]) for exchange in exchanges
            ),
            "response_count": sum(
                len(exchange["response_ids"]) for exchange in exchanges
            ),
            "wire_byte_count": sum(
                exchange["wire_byte_count"] for exchange in exchanges
            ),
            "decoded_byte_count": sum(
                exchange["decoded_byte_count"] for exchange in exchanges
            ),
        }
        self.assertEqual(
            source_identity["collection"], rebuilt_collection
        )
        self.assertEqual(
            claimed_identity["collection"], rebuilt_collection
        )
        header_rows = [
            row
            for chunk in inventory["typed_chunks"]
            if chunk["role"] == "headers"
            for row in typed_rows_by_path[chunk["path"]]
        ]
        range_row = inventory["range"]
        self.assertEqual(
            tuple(range_row),
            tuple(sorted((
                "lower_bound_number", "anchor_number",
                "cutoff_timestamp", "block_count",
            ))),
        )
        self.assertEqual(range_row["lower_bound_number"], header_rows[0]["number"])
        self.assertEqual(range_row["anchor_number"], header_rows[-1]["number"])
        self.assertEqual(range_row["block_count"], len(header_rows))
        policy = json.loads(observed["member_bytes"]["policy.json"])
        self.assertEqual(
            range_row["cutoff_timestamp"],
            header_rows[-1]["timestamp"] - policy["lookback_seconds"],
        )

        finish_payloads = [
            payload for payload in observed["payloads"]
            if payload[0] == "finish"
        ]
        self.assertEqual(len(finish_payloads), 1)
        finish = finish_payloads[0]
        self.assertEqual(finish[1], len(exchanges))
        bound_prefinalization = observed[
            "bound_prefinalization_digests"
        ]
        bound_reconciliation = observed[
            "bound_reconciliation_digests"
        ]
        self.assertIs(type(bound_prefinalization), tuple)
        self.assertEqual(len(bound_prefinalization), 5)
        self.assertEqual(
            bound_prefinalization[0],
            "historical_foundry_prefinalization_digest_binding/v1",
        )
        self.assertTrue(all(
            type(value) is str
            and len(value) == 64
            and set(value) <= set("0123456789abcdef")
            for value in bound_prefinalization[1:]
        ))
        self.assertIs(type(bound_reconciliation), tuple)
        self.assertEqual(len(bound_reconciliation), 6)
        self.assertEqual(
            bound_reconciliation[0],
            "historical_foundry_reconciliation_digest_binding/v1",
        )
        self.assertIs(type(bound_reconciliation[1]), int)
        self.assertIs(type(bound_reconciliation[3]), int)
        self.assertTrue(all(
            type(bound_reconciliation[index]) is str
            and len(bound_reconciliation[index]) == 64
            and set(bound_reconciliation[index])
            <= set("0123456789abcdef")
            for index in (2, 4, 5)
        ))
        self.assertEqual(
            bound_prefinalization[3], bound_reconciliation[5]
        )
        digest_plan = observed["digest_plan"]
        digest_pre_ledger = observed["digest_frozen_pre_ledger"]
        digest_compact = observed["digest_compact_projection"]
        digest_final_anchor = observed["digest_final_anchor"]
        self.assertIs(type(digest_plan), dict)
        self.assertIs(type(digest_pre_ledger), tuple)
        self.assertIs(type(digest_compact), dict)
        self.assertIs(type(digest_final_anchor), dict)
        rebuilt_prefinalization = (
            "historical_foundry_prefinalization_digest_binding/v1",
            independently_typed_hash(
                b"historical_foundry_prefinalization_plan/v1",
                digest_plan,
            ),
            independently_typed_hash(
                b"historical_foundry_prefinalization_pre_ledger/v1",
                digest_pre_ledger,
            ),
            independently_typed_hash(
                b"historical_foundry_prefinalization_compact_projection/v1",
                digest_compact,
            ),
            independently_typed_hash(
                b"historical_foundry_prefinalization_final_anchor/v1",
                digest_final_anchor,
            ),
        )
        self.assertEqual(rebuilt_prefinalization, bound_prefinalization)
        self.assertEqual(finish[3], bound_prefinalization)
        self.assertEqual(finish[4], bound_reconciliation)
        self.assertEqual(
            tuple(inventory["prefinalization_digests"]),
            bound_prefinalization,
        )
        self.assertEqual(
            tuple(inventory["reconciliation_digests"]),
            bound_reconciliation,
        )

        # Rebuild the complete inventory from the authenticated event stream
        # and detached physical members.  No value below is selected from the
        # parsed inventory under test.
        exchange_events = [
            payload for payload in observed["payloads"]
            if payload[0] == "exchange"
        ]
        root_events = [
            payload for payload in observed["payloads"]
            if payload[0] == "root"
        ]
        self.assertEqual(len(exchange_events), len(exchanges))
        self.assertEqual(len(root_events), len(post_roots))

        rebuilt_configs = []
        config_hash_fields = {
            "policy": "policy_physical_sha256",
            "authority": "authority_physical_sha256",
            "toolchain": "toolchain_physical_sha256",
        }
        for role, path in (
            ("policy", "policy.json"),
            ("authority", "authority.json"),
            ("toolchain", "toolchain.json"),
        ):
            payload = observed["member_bytes"][path]
            decoded_config = json.loads(payload.decode("utf-8"))
            physical_hash = hashlib.sha256(payload).hexdigest()
            self.assertEqual(
                physical_hash, claimed_configs[config_hash_fields[role]]
            )
            rebuilt_configs.append({
                "role": role,
                "path": path,
                "schema": decoded_config["schema"],
                "byte_count": len(payload),
                "sha256": physical_hash,
                "policy_id": (
                    claimed_configs["policy_id"]
                    if role == "policy" else None
                ),
            })

        event_compacts = [payload[1] for payload in exchange_events]
        raw_locations = {}
        rebuilt_raw_chunks = []
        exchange_cursor = 0
        raw_member_paths = sorted(
            path for path in observed["member_bytes"]
            if path.startswith("rpc/") and path.endswith(".bin")
        )
        for path in raw_member_paths:
            payload = observed["member_bytes"][path]
            offset = 0
            chunk_compacts = []
            while offset < len(payload):
                self.assertLess(exchange_cursor, len(event_compacts))
                compact = event_compacts[exchange_cursor]
                request_size = int.from_bytes(
                    payload[offset:offset + 8], "big"
                )
                request_start = offset + 8
                request_stop = request_start + request_size
                decoded_size = int.from_bytes(
                    payload[request_stop:request_stop + 8], "big"
                )
                decoded_start = request_stop + 8
                frame_stop = decoded_start + decoded_size
                self.assertLessEqual(frame_stop, len(payload))
                self.assertEqual(
                    request_size, compact["request_byte_count"]
                )
                self.assertEqual(
                    decoded_size, compact["decoded_byte_count"]
                )
                self.assertEqual(
                    hashlib.sha256(
                        payload[request_start:request_stop]
                    ).hexdigest(),
                    compact["request_sha256"],
                )
                self.assertEqual(
                    hashlib.sha256(
                        payload[decoded_start:frame_stop]
                    ).hexdigest(),
                    compact["decoded_sha256"],
                )
                raw_locations[compact["exchange_index"]] = (path, offset)
                chunk_compacts.append(compact)
                offset = frame_stop
                exchange_cursor += 1
            self.assertTrue(chunk_compacts)
            request_ids = [
                request_id
                for compact in chunk_compacts
                for request_id in compact["request_ids"]
            ]
            rebuilt_raw_chunks.append({
                "path": path,
                "byte_count": len(payload),
                "sha256": hashlib.sha256(payload).hexdigest(),
                "exchange_index_start": chunk_compacts[0][
                    "exchange_index"
                ],
                "exchange_index_stop": chunk_compacts[-1][
                    "exchange_index"
                ],
                "exchange_count": len(chunk_compacts),
                "request_id_start": request_ids[0],
                "request_id_stop": request_ids[-1],
            })
        self.assertEqual(exchange_cursor, len(event_compacts))

        role_order = {role: index for index, role in enumerate(
            observer._TYPED_ROLES
        )}
        rebuilt_typed_chunks = []
        physical_typed_rows = {}
        for path in sorted(
            (
                path for path in observed["member_bytes"]
                if path.endswith(".json.gz")
            ),
            key=lambda value: (
                role_order[value.split("/", 1)[0]], value
            ),
        ):
            role, basename = path.split("/", 1)
            physical = observed["member_bytes"][path]
            decoded = gzip.decompress(physical)
            rows = json.loads(decoded.decode("utf-8"))
            self.assertEqual(decoded, canonical(rows))
            block_key = "number" if role == "headers" else "block_number"
            block_numbers = [row[block_key] for row in rows]
            chunk_index = int(basename[:-len(".json.gz")])
            rebuilt_typed_chunks.append({
                "path": path,
                "role": role,
                "chunk_index": chunk_index,
                "block_start": block_numbers[0],
                "block_stop": block_numbers[-1],
                "row_count": len(rows),
                "decoded_byte_count": len(decoded),
                "decoded_sha256": hashlib.sha256(decoded).hexdigest(),
                "gzip_byte_count": len(physical),
                "gzip_sha256": hashlib.sha256(physical).hexdigest(),
            })
            physical_typed_rows[path] = rows

        root_authority = {}
        rebuilt_post_roots = []
        role_cursors = {role: 0 for role in observer._TYPED_ROLES}
        role_paths = {
            role: [
                row["path"] for row in rebuilt_typed_chunks
                if row["role"] == role
            ]
            for role in observer._TYPED_ROLES
        }
        for payload in root_events:
            self.assertEqual(len(payload), 6)
            root = payload[1]
            role = payload[2]
            rebuilt_post_roots.append(root)
            if role in observer._TYPED_ROLES:
                root_rows = json.loads(payload[3].decode("utf-8"))
                self.assertEqual(len(root_rows), payload[4])
                self.assertEqual(
                    inventory_digest(typed_domains[role], root_rows),
                    payload[5],
                )
                matches = []
                for path in role_paths[role]:
                    rows = physical_typed_rows[path]
                    for first in range(0, len(rows) - len(root_rows) + 1):
                        if rows[first:first + len(root_rows)] == root_rows:
                            matches.append((path, first))
                self.assertEqual(len(matches), 1)
                path, first = matches[0]
                self.assertEqual(first, role_cursors[role])
                role_cursors[role] += len(root_rows)
                refs = [{
                    "path": path,
                    "first_row_index": first,
                    "row_count": len(root_rows),
                }]
            else:
                self.assertEqual(payload[3:], (None, 0, None))
                refs = []
            root_authority[root["logical_batch_index"]] = (role, refs)
        for role in observer._TYPED_ROLES:
            self.assertEqual(
                role_cursors[role],
                sum(len(physical_typed_rows[path]) for path in role_paths[role]),
            )

        rebuilt_exchanges = []
        for payload in exchange_events:
            compact, leaf = payload[1], payload[2]
            logical_index = compact["logical_batch_index"]
            role, refs = root_authority[logical_index]
            raw_path, raw_offset = raw_locations[
                compact["exchange_index"]
            ]
            rebuilt = dict(compact)
            rebuilt.update({
                "segment": leaf["segment"],
                "segment_local_index": leaf["segment_local_index"],
                "leaf_index": leaf["leaf_index"],
                "wire_hash_authority": leaf["wire_hash_authority"],
                "raw_chunk_path": raw_path,
                "raw_chunk_offset": raw_offset,
                "typed_role": role,
                "typed_chunk_refs": refs,
            })
            self.assertEqual(tuple(rebuilt), compact_keys + extension_keys)
            rebuilt_exchanges.append(rebuilt)
        event_leaves_by_root = {}
        for payload in exchange_events:
            event_leaves_by_root.setdefault(
                payload[1]["logical_batch_index"], []
            ).append(payload[2])
        for root in rebuilt_post_roots:
            leaves = event_leaves_by_root[root["logical_batch_index"]]
            self.assertEqual(
                tuple(leaf["exchange_index"] for leaf in leaves),
                tuple(root["success_exchange_indices"]),
            )
            self.assertEqual(root["leaf_count"], len(leaves))
            self.assertEqual(
                root["leaf_ledger_sha256"],
                inventory_digest(
                    b"historical_foundry_leaf_ledger/v1", leaves
                ),
            )
        rebuilt_post_leaves = [
            payload[2] for payload in exchange_events
        ]
        rebuilt_reconciliation = (
            "historical_foundry_reconciliation_digest_binding/v1",
            len(rebuilt_post_roots),
            inventory_digest(
                b"historical_foundry_reconciliation_post_root_ledger/v1",
                rebuilt_post_roots,
            ),
            len(rebuilt_post_leaves),
            inventory_digest(
                b"historical_foundry_reconciliation_post_leaf_ledger/v1",
                rebuilt_post_leaves,
            ),
            rebuilt_prefinalization[3],
        )
        self.assertEqual(
            rebuilt_reconciliation, bound_reconciliation
        )

        rebuilt_receipts = []
        rebuilt_request_ids = []
        for exchange in rebuilt_exchanges:
            receipt = {key: exchange[key] for key in compact_keys}
            receipt["schema"] = (
                "historical_foundry_exchange_spool_receipt/v1"
            )
            rebuilt_receipts.append(receipt)
            rebuilt_request_ids.extend(exchange["request_ids"])
        rebuilt_receipt_digest = inventory_digest(
            b"historical_foundry_exchange_spool_receipt_inventory/v1",
            rebuilt_receipts,
        )
        rebuilt_header_rows = [
            row
            for chunk in rebuilt_typed_chunks
            if chunk["role"] == "headers"
            for row in physical_typed_rows[chunk["path"]]
        ]
        rebuilt_range = {
            "lower_bound_number": rebuilt_header_rows[0]["number"],
            "anchor_number": rebuilt_header_rows[-1]["number"],
            "cutoff_timestamp": (
                rebuilt_header_rows[-1]["timestamp"]
                - json.loads(observed["member_bytes"]["policy.json"])[
                    "lookback_seconds"
                ]
            ),
            "block_count": len(rebuilt_header_rows),
        }
        rebuilt_request_range = {
            "first_request_id": 1,
            "last_request_id": rebuilt_request_ids[-1],
            "request_count": len(rebuilt_request_ids),
        }
        self.assertEqual(
            rebuilt_request_ids,
            list(range(1, rebuilt_request_ids[-1] + 1)),
        )
        independently_rebuilt_inventory = {
            "schema": "historical_foundry_capture_inventory/v1",
            "source_identity": expected_source_identity,
            "receipt_inventory_sha256": rebuilt_receipt_digest,
            "prefinalization_digests": rebuilt_prefinalization,
            "reconciliation_digests": rebuilt_reconciliation,
            "range": rebuilt_range,
            "request_range": rebuilt_request_range,
            "configs": rebuilt_configs,
            "raw_chunks": rebuilt_raw_chunks,
            "typed_chunks": rebuilt_typed_chunks,
            "post_roots": rebuilt_post_roots,
            "exchanges": rebuilt_exchanges,
        }
        self.assertEqual(
            canonical(independently_rebuilt_inventory), inventory_bytes
        )

        self.assertEqual(observed["cleanup_entries"], 1)
        self.assertEqual(observed["capture_generation"], 1)
        self.assertNotEqual(
            observed["capture_state_at_cleanup"], "capture_frozen"
        )
        quota_rows = observed["quota_transition_rows"]
        self.assertEqual(len(quota_rows), 18)
        self.assertEqual(
            observed["quota_transition_calls"],
            [row["transition"] for row in quota_rows],
        )
        quota_pairs = [
            quota_rows[index:index + 2]
            for index in range(0, len(quota_rows), 2)
        ]
        self.assertEqual(len(quota_pairs), 9)
        self.assertTrue(all(
            pair[0]["transition"]
            == "_install_quota_reserve_transition"
            and pair[1]["transition"]
            == "_install_quota_commit_transition"
            and pair[0]["path"] == pair[1]["path"]
            and pair[0]["path"] is not None
            for pair in quota_pairs
        ))
        self.assertEqual(
            len({pair[0]["path"] for pair in quota_pairs}), 9
        )
        self.assertEqual(observed["snapshot_allocations"], 1)
        self.assertEqual(observed["source_move_calls"], 7)
        self.assertEqual(len(observed["spool_unlinks"]), 1)
        self.assertEqual(observed["data_entries"], ())


class HistoricalFoundryStorageTask4bSurfaceTests(unittest.TestCase):
    def test_task4b_pure_helper_boundaries_and_signatures(self):
        storage = importlib.import_module("scripts.historical_foundry_storage")

        class IntSubclass(int):
            pass

        expected_parameters = {
            "_plan_historical_raw_chunk_append": (
                "current_chunk_byte_count", "request_byte_count",
                "decoded_byte_count",
            ),
            "_require_historical_capture_inventory_size": ("byte_count",),
            "_require_historical_gzip_member_size": ("byte_count",),
            "_plan_historical_typed_root_append": (
                "current_decoded_size", "current_row_count",
                "candidate_row_encoded_lengths",
            ),
        }
        for name, parameters in expected_parameters.items():
            helper = getattr(storage, name)
            signature = inspect.signature(helper)
            self.assertEqual(tuple(signature.parameters), parameters)
            self.assertTrue(all(
                parameter.kind is inspect.Parameter.KEYWORD_ONLY
                for parameter in signature.parameters.values()
            ))

        self.assertEqual(
            storage._plan_historical_raw_chunk_append(
                current_chunk_byte_count=0,
                request_byte_count=4_194_304,
                decoded_byte_count=8_388_608,
            ),
            ("append_current", 12_582_928),
        )
        self.assertEqual(
            storage._plan_historical_raw_chunk_append(
                current_chunk_byte_count=16_777_216,
                request_byte_count=0,
                decoded_byte_count=0,
            ),
            ("flush_then_append", 16),
        )
        self.assertEqual(
            storage._require_historical_capture_inventory_size(
                byte_count=16_777_216
            ),
            16_777_216,
        )
        self.assertEqual(
            storage._require_historical_gzip_member_size(byte_count=16_842_752),
            16_842_752,
        )
        self.assertEqual(
            storage._plan_historical_typed_root_append(
                current_decoded_size=2,
                current_row_count=0,
                candidate_row_encoded_lengths=(1,),
            ),
            ("append_current", 3),
        )
        raw_cases = (
            (
                {
                    "current_chunk_byte_count": 16_777_200,
                    "request_byte_count": 0,
                    "decoded_byte_count": 0,
                },
                ("append_current", 16_777_216),
            ),
            (
                {
                    "current_chunk_byte_count": 16_777_201,
                    "request_byte_count": 0,
                    "decoded_byte_count": 0,
                },
                ("flush_then_append", 16),
            ),
            (
                {
                    "current_chunk_byte_count": 0,
                    "request_byte_count": 4_194_304,
                    "decoded_byte_count": 8_388_608,
                },
                ("append_current", 12_582_928),
            ),
        )
        for arguments, expected in raw_cases:
            with self.subTest(raw=arguments):
                result = storage._plan_historical_raw_chunk_append(
                    **arguments
                )
                self.assertEqual(result, expected)
                self.assertIs(type(result), tuple)
                self.assertIs(type(result[0]), str)
                self.assertIs(type(result[1]), int)

        typed_cases = (
            (
                {
                    "current_decoded_size": 2,
                    "current_row_count": 0,
                    "candidate_row_encoded_lengths": (16_777_214,),
                },
                ("append_current", 16_777_216),
            ),
            (
                {
                    "current_decoded_size": 16_777_210,
                    "current_row_count": 1,
                    "candidate_row_encoded_lengths": (6,),
                },
                ("flush_then_append", 8),
            ),
            (
                {
                    "current_decoded_size": 16_777_210,
                    "current_row_count": 1,
                    "candidate_row_encoded_lengths": (2, 3),
                },
                ("flush_then_append", 8),
            ),
        )
        for arguments, expected in typed_cases:
            with self.subTest(typed=arguments):
                result = storage._plan_historical_typed_root_append(
                    **arguments
                )
                self.assertEqual(result, expected)
                self.assertIs(type(result), tuple)
                self.assertIs(type(result[0]), str)
                self.assertIs(type(result[1]), int)

        for helper, cap in (
            (storage._require_historical_capture_inventory_size, 16_777_216),
            (storage._require_historical_gzip_member_size, 16_842_752),
        ):
            with self.subTest(helper=helper.__name__, value=0):
                self.assertIs(type(helper(byte_count=0)), int)
            with self.subTest(helper=helper.__name__, value=cap):
                self.assertEqual(helper(byte_count=cap), cap)
            for invalid in (-1, cap + 1, False, IntSubclass(1)):
                with self.subTest(helper=helper.__name__, invalid=invalid):
                    with self.assertRaises(ValueError):
                        helper(byte_count=invalid)

        raw_invalid = (
            {
                "current_chunk_byte_count": 0,
                "request_byte_count": 4_194_305,
                "decoded_byte_count": 0,
            },
            {
                "current_chunk_byte_count": 0,
                "request_byte_count": 0,
                "decoded_byte_count": 8_388_609,
            },
            {
                "current_chunk_byte_count": 16_777_217,
                "request_byte_count": 0,
                "decoded_byte_count": 0,
            },
            {
                "current_chunk_byte_count": 0,
                "request_byte_count": -1,
                "decoded_byte_count": 0,
            },
            {
                "current_chunk_byte_count": 0,
                "request_byte_count": 0,
                "decoded_byte_count": False,
            },
            {
                "current_chunk_byte_count": IntSubclass(0),
                "request_byte_count": 0,
                "decoded_byte_count": 0,
            },
        )
        for arguments in raw_invalid:
            with self.subTest(raw_invalid=arguments):
                with self.assertRaises(ValueError):
                    storage._plan_historical_raw_chunk_append(**arguments)

        typed_invalid = (
            {
                "current_decoded_size": 2,
                "current_row_count": 0,
                "candidate_row_encoded_lengths": (16_777_215,),
            },
            {
                "current_decoded_size": 16_777_217,
                "current_row_count": 1,
                "candidate_row_encoded_lengths": (1,),
            },
            {
                "current_decoded_size": 3,
                "current_row_count": 0,
                "candidate_row_encoded_lengths": (1,),
            },
            {
                "current_decoded_size": 2,
                "current_row_count": 1,
                "candidate_row_encoded_lengths": (1,),
            },
            {
                "current_decoded_size": 2,
                "current_row_count": 0,
                "candidate_row_encoded_lengths": [1],
            },
            {
                "current_decoded_size": 2,
                "current_row_count": 0,
                "candidate_row_encoded_lengths": (0,),
            },
            {
                "current_decoded_size": 2,
                "current_row_count": False,
                "candidate_row_encoded_lengths": (1,),
            },
            {
                "current_decoded_size": IntSubclass(2),
                "current_row_count": 0,
                "candidate_row_encoded_lengths": (1,),
            },
            {
                "current_decoded_size": 2,
                "current_row_count": 0,
                "candidate_row_encoded_lengths": (IntSubclass(1),),
            },
        )
        for arguments in typed_invalid:
            with self.subTest(typed_invalid=arguments):
                with self.assertRaises(ValueError):
                    storage._plan_historical_typed_root_append(**arguments)

        for call in (
            lambda: storage._require_historical_capture_inventory_size(
                byte_count=16_777_217
            ),
            lambda: storage._require_historical_gzip_member_size(
                byte_count=16_842_753
            ),
            lambda: storage._plan_historical_raw_chunk_append(
                current_chunk_byte_count=False,
                request_byte_count=0,
                decoded_byte_count=0,
            ),
            lambda: storage._plan_historical_typed_root_append(
                current_decoded_size=2,
                current_row_count=0,
                candidate_row_encoded_lengths=(),
            ),
        ):
            with self.assertRaises(ValueError):
                call()

    def test_task4b_storage_surface_tuple_and_closed_reader(self):
        storage = importlib.import_module("scripts.historical_foundry_storage")
        expected_names = (
            "_HistoricalWindowCaptureReplaySource",
            "_HistoricalWindowCaptureReplaySource.__enter__",
            "_HistoricalWindowCaptureReplaySource._bind_reconciliation_from_bound_scan",
            "_HistoricalWindowCaptureReplaySource.__iter__",
            "_HistoricalWindowCaptureReplaySource.__next__",
            "_HistoricalWindowCaptureReplaySource.__exit__",
            "_HistoricalWindowCaptureReplaySource.close",
            "_ConsumedProductionHistoricalWindowCapabilityView._materialize_staging_snapshot_from_bound_scan",
            "HistoricalRunStagingSnapshot",
            "HistoricalRunStagingSnapshot.read_frozen_member",
            "HistoricalRunStagingSnapshot.frozen_identity_projection",
            "HistoricalRunStagingSnapshot.reread_frozen_members_unchanged",
            "HistoricalRunStagingSnapshot.close",
            "HistoricalRunStagingSnapshot.__enter__",
            "HistoricalRunStagingSnapshot.__exit__",
            "open_validated_run",
            "HistoricalRunSnapshot",
            "HistoricalRunSnapshot.read_member",
            "HistoricalRunSnapshot.identity_projection",
            "HistoricalRunSnapshot.reread_unchanged",
            "HistoricalRunSnapshot.close",
        )
        self.assertEqual(storage._TASK4B_STORAGE_LOCAL_SURFACE_NAMES, expected_names)
        self.assertEqual(len(storage._TASK4B_STORAGE_LOCAL_SURFACE_OBJECTS), 21)
        self.assertNotIn(
            "_plan_historical_raw_chunk_append", expected_names
        )

        signature_rows = (
            (
                storage._HistoricalWindowCaptureReplaySource.__enter__,
                ("self",), (),
            ),
            (
                storage._HistoricalWindowCaptureReplaySource
                ._bind_reconciliation_from_bound_scan,
                ("self", "expected_view", "expected_reconciliation"),
                ("expected_view", "expected_reconciliation"),
            ),
            (
                storage._HistoricalWindowCaptureReplaySource.__iter__,
                ("self",), (),
            ),
            (
                storage._HistoricalWindowCaptureReplaySource.__next__,
                ("self",), (),
            ),
            (
                storage._HistoricalWindowCaptureReplaySource.__exit__,
                ("self", "error_type", "error", "traceback"), (),
            ),
            (
                storage._HistoricalWindowCaptureReplaySource.close,
                ("self",), (),
            ),
            (
                storage.HistoricalRunStagingSnapshot.read_frozen_member,
                ("self", "relative_path", "expected_sha256", "max_bytes"),
                ("expected_sha256", "max_bytes"),
            ),
            (
                storage.HistoricalRunStagingSnapshot
                .frozen_identity_projection,
                ("self",), (),
            ),
            (
                storage.HistoricalRunStagingSnapshot
                .reread_frozen_members_unchanged,
                ("self",), (),
            ),
            (
                storage.HistoricalRunStagingSnapshot.close,
                ("self",), (),
            ),
            (
                storage.HistoricalRunStagingSnapshot.__enter__,
                ("self",), (),
            ),
            (
                storage.HistoricalRunStagingSnapshot.__exit__,
                ("self", "error_type", "error", "traceback"), (),
            ),
            (
                storage.HistoricalRunSnapshot.read_member,
                ("self", "relative_path", "expected_sha256", "max_bytes"),
                ("expected_sha256", "max_bytes"),
            ),
            (
                storage.HistoricalRunSnapshot.identity_projection,
                ("self",), (),
            ),
            (
                storage.HistoricalRunSnapshot.reread_unchanged,
                ("self",), (),
            ),
            (
                storage.HistoricalRunSnapshot.close,
                ("self",), (),
            ),
            (
                storage.open_validated_run,
                ("data_dir", "run_id", "expected_manifest_sha256"),
                ("data_dir", "run_id", "expected_manifest_sha256"),
            ),
        )
        for function, parameter_names, keyword_only in signature_rows:
            with self.subTest(signature=function.__qualname__):
                signature = inspect.signature(function)
                self.assertEqual(
                    tuple(signature.parameters), parameter_names
                )
                self.assertEqual(
                    tuple(
                        name for name, parameter
                        in signature.parameters.items()
                        if parameter.kind is inspect.Parameter.KEYWORD_ONLY
                    ),
                    keyword_only,
                )

        class Hostile:
            def __getattribute__(self, _name):
                raise AssertionError("closed reader touched a caller argument")

        with self.assertRaises(storage.HistoricalFoundryStorageError):
            storage.open_validated_run(
                data_dir=Hostile(), run_id=Hostile(),
                expected_manifest_sha256=Hostile(),
            )
        for authority_type in (
            storage.HistoricalRunSnapshot,
            storage.HistoricalRunStagingSnapshot,
            storage._HistoricalWindowCaptureReplaySource,
        ):
            with self.subTest(authority=authority_type.__name__):
                with self.assertRaises(storage.HistoricalFoundryStorageError):
                    authority_type()
                with self.assertRaises(TypeError):
                    type("ForbiddenTask4bAuthority", (authority_type,), {})
                clone = object.__new__(authority_type)
                self.assertFalse(hasattr(clone, "__dict__"))
                self.assertEqual(
                    repr(clone), authority_type.__name__ + "(<redacted>)"
                )
                with self.assertRaises(TypeError):
                    copy.copy(clone)
                with self.assertRaises(TypeError):
                    copy.deepcopy(clone)
                with self.assertRaises(TypeError):
                    pickle.dumps(clone)

    def test_task4b_scan_surface_rebinding_is_detected_before_capability_consume(self):
        from tests.test_historical_foundry_scan import (
            HistoricalFoundryScanTask3bIntegratedTests,
        )

        storage = importlib.import_module("scripts.historical_foundry_storage")
        rpc = importlib.import_module("scripts.historical_foundry_rpc")
        scan = importlib.import_module("scripts.historical_foundry_scan")
        case_method = (
            HistoricalFoundryScanTask3bIntegratedTests
            .test_scheduler_owns_complete_offline_run_through_capability_delivery
        )
        lines, start = inspect.getsourcelines(case_method)
        consume_line = start + next(
            index for index, line in enumerate(lines)
            if "view = storage.consume_production_historical_window_capability(" in line
        )
        capability = [None]
        control = GeneratorExit("capture-task4b-bound-capability")
        prior_trace = sys.gettrace()

        def tracer(frame, event, _argument):
            if (
                frame.f_code is case_method.__code__
                and event == "line"
                and frame.f_lineno == consume_line
            ):
                capability[0] = frame.f_locals["capability"]
                sys.settrace(prior_trace)
                raise control
            return tracer

        case = HistoricalFoundryScanTask3bIntegratedTests(
            methodName=case_method.__name__
        )
        try:
            sys.settrace(tracer)
            with self.assertRaises(GeneratorExit) as caught:
                case.test_scheduler_owns_complete_offline_run_through_capability_delivery()
        finally:
            sys.settrace(prior_trace)
        self.assertIs(caught.exception, control)
        original = scan._ProductionHistoricalWindowCaptureReplayEvent
        original_exported = scan._TASK4B_SCAN_LOCAL_SURFACE_OBJECTS
        replacement_exported = list(original_exported)
        replacement_exported[1] = object
        replacement_exported = tuple(replacement_exported)
        view = None
        try:
            scan._ProductionHistoricalWindowCaptureReplayEvent = object
            scan._TASK4B_SCAN_LOCAL_SURFACE_OBJECTS = replacement_exported
            with self.assertRaises(rpc._ArchiveRpcError) as caught:
                view = storage.consume_production_historical_window_capability(
                    capability=capability[0]
                )
        finally:
            scan._ProductionHistoricalWindowCaptureReplayEvent = original
            scan._TASK4B_SCAN_LOCAL_SURFACE_OBJECTS = original_exported
            if view is not None:
                try:
                    view.close()
                except storage.HistoricalFoundryStorageError:
                    pass
            try:
                capability[0].close()
            except storage.HistoricalFoundryStorageError:
                pass
        self.assertEqual(
            (caught.exception.reason_code, caught.exception.failure_kind),
            ("authority_mismatch", "final_identity_drift"),
        )

    def test_task4b_checker_slot_replacement_cannot_bypass_scan_drift(self):
        from tests.test_historical_foundry_scan import (
            HistoricalFoundryScanTask3bIntegratedTests,
        )

        storage = importlib.import_module("scripts.historical_foundry_storage")
        rpc = importlib.import_module("scripts.historical_foundry_rpc")
        scan = importlib.import_module("scripts.historical_foundry_scan")
        case_method = (
            HistoricalFoundryScanTask3bIntegratedTests
            .test_scheduler_owns_complete_offline_run_through_capability_delivery
        )
        lines, start = inspect.getsourcelines(case_method)
        consume_line = start + next(
            index for index, line in enumerate(lines)
            if "view = storage.consume_production_historical_window_capability(" in line
        )
        captured = {}
        control = GeneratorExit("capture-task4b-checker-slot")
        prior_trace = sys.gettrace()

        def tracer(frame, event, _argument):
            if event != "line":
                return tracer
            if (
                frame.f_code.co_filename == storage.__file__
                and frame.f_code.co_name
                == "_mint_production_historical_window_capability_core"
            ):
                binding_record = frame.f_locals.get("binding_record")
                checker = (
                    binding_record.get("task4b_currentness_checker")
                    if type(binding_record) is dict else None
                )
                if callable(checker):
                    captured["binding_record"] = binding_record
                    captured["checker"] = checker
            elif (
                frame.f_code is case_method.__code__
                and frame.f_lineno == consume_line
            ):
                captured["capability"] = frame.f_locals["capability"]
                sys.settrace(prior_trace)
                raise control
            return tracer

        case = HistoricalFoundryScanTask3bIntegratedTests(
            methodName=case_method.__name__
        )
        try:
            sys.settrace(tracer)
            with self.assertRaises(GeneratorExit) as interrupted:
                case.test_scheduler_owns_complete_offline_run_through_capability_delivery()
        finally:
            sys.settrace(prior_trace)
        self.assertIs(interrupted.exception, control)
        self.assertEqual(
            set(captured), {"binding_record", "checker", "capability"}
        )

        binding_record = captured["binding_record"]
        checker_reference = weakref.ref(captured.pop("checker"))
        self.assertIsNotNone(checker_reference())
        binding_record["task4b_currentness_checker"] = lambda: None
        original_materializer = (
            scan._materialize_historical_window_staging_snapshot
        )
        view = None
        try:
            scan._materialize_historical_window_staging_snapshot = lambda **_kwargs: None
            with self.assertRaises(rpc._ArchiveRpcError) as caught:
                view = storage.consume_production_historical_window_capability(
                    capability=captured["capability"]
                )
        finally:
            scan._materialize_historical_window_staging_snapshot = (
                original_materializer
            )
            if view is not None:
                try:
                    view.close()
                except storage.HistoricalFoundryStorageError:
                    pass
            try:
                captured["capability"].close()
            except storage.HistoricalFoundryStorageError:
                pass
        self.assertEqual(
            (caught.exception.reason_code, caught.exception.failure_kind),
            ("authority_mismatch", "final_identity_drift"),
        )
        gc.collect()
        self.assertIsNone(checker_reference())

    def test_task4b_terminalized_bindings_release_per_binding_checkers(self):
        from tests.test_historical_foundry_scan import (
            HistoricalFoundryScanTask3bIntegratedTests,
        )

        storage = importlib.import_module("scripts.historical_foundry_storage")
        checker_references = []
        seen = set()
        prior_trace = sys.gettrace()

        def tracer(frame, event, _argument):
            if (
                event == "line"
                and frame.f_code.co_name
                == "_mint_production_historical_window_capability_core"
                and frame.f_code.co_filename == storage.__file__
            ):
                binding_record = frame.f_locals.get("binding_record")
                checker = (
                    binding_record.get("task4b_currentness_checker")
                    if type(binding_record) is dict else None
                )
                if callable(checker) and id(checker) not in seen:
                    seen.add(id(checker))
                    checker_references.append(weakref.ref(checker))
            return tracer

        try:
            sys.settrace(tracer)
            for _unused in range(2):
                case = HistoricalFoundryScanTask3bIntegratedTests(
                    methodName=(
                        "test_scheduler_owns_complete_offline_run_through_"
                        "capability_delivery"
                    )
                )
                case.test_scheduler_owns_complete_offline_run_through_capability_delivery()
        finally:
            sys.settrace(prior_trace)
        gc.collect()
        self.assertEqual(len(checker_references), 2)
        self.assertTrue(all(
            reference() is None for reference in checker_references
        ))


def _slice6_materialize_snapshot(*, spool_unlinks=None):
    from tests.test_historical_foundry_scan import (
        _Task4bOfflineCapabilityFixture,
    )

    scan = importlib.import_module("scripts.historical_foundry_scan")
    fixture = _Task4bOfflineCapabilityFixture()
    try:
        capability = fixture.mint()
        if spool_unlinks is None:
            snapshot = scan._materialize_historical_window_staging_snapshot(
                capability=capability
            )
        else:
            storage = importlib.import_module(
                "scripts.historical_foundry_storage"
            )
            original_unlink = storage.os.unlink

            def observed_unlink(path, *args, **kwargs):
                if (
                    type(path) is str
                    and path.startswith(
                        ".historical-foundry-exchange-spool-"
                    )
                    and path.endswith(".bin")
                ):
                    spool_unlinks.append((path, kwargs.get("dir_fd")))
                return original_unlink(path, *args, **kwargs)

            with mock.patch.object(
                storage.os, "unlink", side_effect=observed_unlink
            ) as patched_unlink:
                supported = set(storage.os.supports_dir_fd)
                supported.discard(original_unlink)
                supported.add(patched_unlink)
                with mock.patch.object(
                    storage.os, "supports_dir_fd", supported
                ):
                    snapshot = (
                        scan._materialize_historical_window_staging_snapshot(
                            capability=capability
                        )
                    )
        fixture.capability = None
        return fixture, snapshot
    except BaseException:
        fixture.close()
        raise


def _slice6_snapshot_inventory(snapshot):
    projection = dict(snapshot.frozen_identity_projection())
    payload = snapshot.read_frozen_member(
        "scan/capture_inventory.json",
        expected_sha256=projection["capture_inventory_sha256"],
        max_bytes=16_777_216,
    )
    return projection, json.loads(payload.decode("utf-8"))


class HistoricalFoundryStorageTask4bQuotaTests(unittest.TestCase):
    def test_slice6_same_quota_survives_all_owner_moves_and_output_is_physically_double_debited(self):
        fixture = None
        snapshot = None
        spool_unlinks = []
        quota_ids = {}
        reservations = []
        quota_transitions = []
        transition_journals = []
        storage = importlib.import_module(
            "scripts.historical_foundry_storage"
        )
        prior_trace = sys.gettrace()

        def tracer(frame, event, argument):
            if frame.f_code.co_filename != storage.__file__:
                return tracer
            name = frame.f_code.co_name
            if name == "_prepare_handle" and event == "return":
                record = frame.f_locals.get("record")
                quota = record.get("quota") if type(record) is dict else None
                if quota is not None:
                    quota_ids[type(argument).__name__] = id(quota)
            owner = frame.f_locals.get("owner")
            if (
                type(owner) is dict
                and owner.get("state") == "capture_materializing"
                and owner.get("quota") is not None
            ):
                quota_ids["capture_materializing"] = id(owner["quota"])
            if name == "_task4b_reserve_output_quota" and event == "call":
                caller = frame.f_back
                reservations.append((
                    caller.f_code.co_name,
                    caller.f_locals.get("target"),
                    frame.f_locals.get("physical_bytes"),
                ))
            if (
                name in (
                    "_install_quota_reserve_transition",
                    "_install_quota_commit_transition",
                    "_install_quota_abort_transition",
                )
                and event == "call"
            ):
                prior = frame.f_locals.get("prior_owner")
                quota_transitions.append((
                    name,
                    id(prior.get("quota")) if type(prior) is dict else None,
                ))
            if (
                name in (
                    "_install_quota_reserve_transition",
                    "_install_quota_commit_transition",
                )
                and event == "return"
            ):
                quota_frame = frame.f_back
                writer = quota_frame.f_back if quota_frame is not None else None
                entry = (
                    writer.f_locals.get("entry")
                    if writer is not None else None
                )
                transition_journals.append((
                    name,
                    entry.get("quota_state") if type(entry) is dict else None,
                    entry.get("quota_token") is not None
                    if type(entry) is dict else False,
                ))
            return tracer

        try:
            sys.settrace(tracer)
            fixture, snapshot = _slice6_materialize_snapshot(
                spool_unlinks=spool_unlinks
            )
            sys.settrace(prior_trace)
            with self.subTest(contract="step6-spool-retirement-before-delivery"):
                self.assertEqual(len(spool_unlinks), 1)
                self.assertIs(type(spool_unlinks[0][0]), str)
                self.assertIs(type(spool_unlinks[0][1]), int)
            projection, inventory = _slice6_snapshot_inventory(snapshot)
            inventory_bytes = len(snapshot.read_frozen_member(
                "scan/capture_inventory.json",
                expected_sha256=projection["capture_inventory_sha256"],
                max_bytes=16_777_216,
            ))
            frozen_bytes = (
                sum(row["byte_count"] for row in inventory["configs"])
                + sum(row["byte_count"] for row in inventory["raw_chunks"])
                + sum(
                    row["gzip_byte_count"]
                    for row in inventory["typed_chunks"]
                )
                + inventory_bytes
            )
            frozen_members = (
                len(inventory["configs"])
                + len(inventory["raw_chunks"])
                + len(inventory["typed_chunks"])
                + 1
            )
            spool_bytes = sum(
                row["spool_length"] for row in inventory["exchanges"]
            )
            spool_members = len(inventory["exchanges"])
            expected_reservations = sorted(
                row["byte_count"] for row in inventory["configs"]
            ) + sorted(
                row["byte_count"] for row in inventory["raw_chunks"]
            ) + sorted(
                row["gzip_byte_count"] for row in inventory["typed_chunks"]
            ) + [inventory_bytes]
            observed_reservations = [row[2] for row in reservations]

            expected_owner_types = (
                "_HistoricalWindowExchangeSpool",
                "_ProductionHistoricalWindowCapability",
                "_ConsumedProductionHistoricalWindowCapabilityView",
                "capture_materializing",
                "HistoricalRunStagingSnapshot",
            )
            self.assertEqual(
                tuple(name for name in expected_owner_types if name in quota_ids),
                expected_owner_types,
            )
            self.assertEqual(
                len({quota_ids[name] for name in expected_owner_types}), 1
            )
            self.assertTrue(quota_transitions)
            self.assertEqual(
                {quota_id for _name, quota_id in quota_transitions},
                {quota_ids["HistoricalRunStagingSnapshot"]},
            )
            self.assertTrue(transition_journals)
            self.assertTrue(all(
                state == (
                    "reserving"
                    if name == "_install_quota_reserve_transition"
                    else "committing"
                )
                and has_token
                for name, state, has_token in transition_journals
            ))
            self.assertEqual(
                sorted(observed_reservations), sorted(expected_reservations)
            )
            self.assertEqual(
                projection["frozen_physical_byte_count"], frozen_bytes
            )
            self.assertEqual(
                projection["frozen_member_count"], frozen_members
            )
            self.assertEqual(
                projection["quota_committed_physical_bytes"],
                spool_bytes + frozen_bytes,
            )
            self.assertEqual(
                projection["quota_committed_member_count"],
                spool_members + frozen_members,
            )
            typed_reservations = sorted(
                physical
                for caller, target, physical in reservations
                if caller == "_task4b_write_capture_member"
                and type(target) is str
                and target.endswith(".json.gz")
            )
            typed_physical = sum(
                row["gzip_byte_count"] for row in inventory["typed_chunks"]
            )
            typed_decoded = sum(
                row["decoded_byte_count"] for row in inventory["typed_chunks"]
            )
            self.assertEqual(sum(typed_reservations), typed_physical)
            self.assertNotEqual(typed_physical, typed_decoded)
            committed = (
                projection["quota_committed_physical_bytes"],
                projection["quota_committed_member_count"],
            )
            transition_count = len(quota_transitions)
            sys.settrace(tracer)
            snapshot.close()
            sys.settrace(prior_trace)
            self.assertIsNone(snapshot.close())
            self.assertEqual(len(quota_transitions), transition_count)
            self.assertEqual(
                committed,
                (
                    projection["quota_committed_physical_bytes"],
                    projection["quota_committed_member_count"],
                ),
            )
        finally:
            sys.settrace(prior_trace)
            if snapshot is not None:
                try:
                    snapshot.close()
                except BaseException:
                    pass
            if fixture is not None:
                fixture.close()

    def test_slice6_integer_reserve_limits_are_inclusive_without_cap_sized_payloads(self):
        storage = importlib.import_module(
            "scripts.historical_foundry_storage"
        )
        opened = []
        original_open = storage.os.open

        def observed_open(*args, **kwargs):
            opened.append((args, kwargs))
            return original_open(*args, **kwargs)

        with mock.patch.object(storage.os, "open", side_effect=observed_open):
            self.assertEqual(
                storage._require_historical_gzip_member_size(
                    byte_count=16_842_752
                ),
                16_842_752,
            )
            self.assertEqual(
                storage._require_historical_capture_inventory_size(
                    byte_count=16_777_216
                ),
                16_777_216,
            )
            self.assertEqual(
                storage._plan_historical_raw_chunk_append(
                    current_chunk_byte_count=16_777_200,
                    request_byte_count=0,
                    decoded_byte_count=0,
                ),
                ("append_current", 16_777_216),
            )
            invalid_calls = (
                lambda: storage._require_historical_gzip_member_size(
                    byte_count=16_842_753
                ),
                lambda: storage._require_historical_capture_inventory_size(
                    byte_count=16_777_217
                ),
                lambda: storage._require_historical_gzip_member_size(
                    byte_count=True
                ),
                lambda: storage._require_historical_capture_inventory_size(
                    byte_count=False
                ),
            )
            for call in invalid_calls:
                with self.assertRaises(ValueError):
                    call()
        self.assertEqual(opened, [])

        temporary = tempfile.TemporaryDirectory(dir="/private/tmp")
        data_dir = Path(temporary.name)
        os.chmod(str(data_dir), 0o700)

        def claimed(name):
            directory = data_dir / name
            directory.mkdir(mode=0o700)
            spool = storage._open_historical_window_exchange_spool(
                data_dir=directory
            )
            storage._issue_historical_window_exchange_transfer_for_test(
                spool=spool, **_valid_transfer_arguments()
            )
            quota = storage._get_historical_window_run_quota_for_test(
                spool=spool
            )
            return spool, quota

        try:
            spool, quota = claimed("exact")
            quota._reserve_for_test(
                physical_bytes=8_589_934_592, members=200_000
            )
            quota._commit_reservation_for_test()
            self.assertEqual(
                tuple(storage._project_historical_window_exchange_spool_for_test(
                    spool_or_sealed=spool
                ).values()),
                (
                    "quota_test_only", 8_589_934_592, 200_000,
                    0, 0, 0, 0, None,
                ),
            )
            spool.close()
            for name, physical_bytes, members in (
                ("byte-plus-one", 8_589_934_593, 1),
                ("member-plus-one", 1, 200_001),
            ):
                spool, quota = claimed(name)
                with self.assertRaises(storage.HistoricalFoundryStorageError):
                    quota._reserve_for_test(
                        physical_bytes=physical_bytes, members=members
                    )
                self.assertEqual(
                    storage._project_historical_window_exchange_spool_for_test(
                        spool_or_sealed=spool
                    )["state"],
                    "closed",
                )
                self.assertIsNone(spool.close())
        finally:
            temporary.cleanup()

    def test_slice6_output_reservation_rollback_commit_no_credit_and_io_transition_failures(self):
        from tests.test_historical_foundry_scan import (
            _Task4bOfflineCapabilityFixture,
        )

        rpc = importlib.import_module("scripts.historical_foundry_rpc")
        scan = importlib.import_module("scripts.historical_foundry_scan")
        storage = importlib.import_module(
            "scripts.historical_foundry_storage"
        )
        cases = (
            "write", "fsync", "reread",
            "reserve-install", "commit-install", "abort-install",
        )
        for case in cases:
            with self.subTest(case=case):
                before_fds = frozenset(
                    int(name) for name in os.listdir("/dev/fd")
                    if name.isdigit()
                )
                fixture = _Task4bOfflineCapabilityFixture()
                original_pwrite = storage.os.pwrite
                original_pread = storage.os.pread
                original_fsync = storage.os.fsync
                output_fds = set()
                output_payloads = []
                output_member_for_fd = {}
                reread_members = set()
                fsynced_members = set()
                transition_counts = {
                    "_install_quota_reserve_transition": 0,
                    "_install_quota_commit_transition": 0,
                    "_install_quota_abort_transition": 0,
                }
                states = []
                post_effect_journals = []
                cleanup_control = asyncio.CancelledError(
                    "slice6 abort-install cleanup control"
                )
                loop_control = SystemExit(
                    "slice6 quota transition made no cleanup progress"
                )
                prior_trace = sys.gettrace()

                def observed_pwrite(fd, payload, offset):
                    if offset == 0:
                        output_fds.add(fd)
                        output_payloads.append(len(payload))
                        output_member_for_fd[fd] = len(output_payloads)
                    if case in ("write", "abort-install") and len(
                        output_payloads
                    ) == 4 and offset == 0:
                        raise OSError("slice6 output write sentinel")
                    return original_pwrite(fd, payload, offset)

                def observed_fsync(fd):
                    member = output_member_for_fd.get(fd)
                    if member is not None and member not in fsynced_members:
                        fsynced_members.add(member)
                        if case == "fsync" and len(fsynced_members) == 4:
                            raise OSError("slice6 output fsync sentinel")
                    return original_fsync(fd)

                def observed_pread(fd, count, offset):
                    member = output_member_for_fd.get(fd)
                    if member is not None and member not in reread_members:
                        reread_members.add(member)
                        if case == "reread" and len(reread_members) == 4:
                            raise OSError("slice6 output reread sentinel")
                    return original_pread(fd, count, offset)

                def tracer(frame, event, _argument):
                    if frame.f_code.co_filename != storage.__file__:
                        return tracer
                    name = frame.f_code.co_name
                    if name in transition_counts and event == "call":
                        transition_counts[name] += 1
                        if sum(transition_counts.values()) > 40:
                            raise loop_control
                        if (
                            case == "reserve-install"
                            and name == "_install_quota_reserve_transition"
                            and transition_counts[name] == 4
                        ):
                            raise OSError("slice6 reserve install sentinel")
                        if (
                            case == "commit-install"
                            and name == "_install_quota_commit_transition"
                            and transition_counts[name] == 4
                        ):
                            raise OSError("slice6 commit install sentinel")
                        if (
                            case == "abort-install"
                            and name == "_install_quota_abort_transition"
                            and transition_counts[name] == 1
                        ):
                            raise cleanup_control
                    if name in transition_counts and event == "return":
                        quota = frame.f_locals.get("next_quota")
                        if type(quota) is dict:
                            states.append((
                                name,
                                quota.get("committed_physical_bytes"),
                                quota.get("committed_members"),
                                quota.get("provisional_physical_bytes"),
                                quota.get("provisional_members"),
                                quota.get("reservation") is not None,
                            ))
                        quota_frame = frame.f_back
                        caller = (
                            quota_frame.f_back
                            if quota_frame is not None else None
                        )
                        entry = (
                            caller.f_locals.get("entry")
                            if caller is not None else None
                        )
                        post_effect_journals.append((
                            name,
                            entry.get("quota_state")
                            if type(entry) is dict else None,
                            entry.get("quota_token") is not None
                            if type(entry) is dict else False,
                        ))
                    return tracer

                escaped = None
                result = None
                remaining = None
                try:
                    capability = fixture.mint()
                    with mock.patch.object(
                        storage.os, "pwrite", side_effect=observed_pwrite
                    ), mock.patch.object(
                        storage.os, "fsync", side_effect=observed_fsync
                    ), mock.patch.object(
                        storage.os, "pread", side_effect=observed_pread
                    ):
                        sys.settrace(tracer)
                        try:
                            result = scan._materialize_historical_window_staging_snapshot(
                                capability=capability
                            )
                        except BaseException as error:
                            escaped = error
                finally:
                    sys.settrace(prior_trace)
                    fixture.capability = None
                    remaining = tuple(fixture.data_dir.iterdir())
                    fixture.close()
                self.assertIsNone(result)
                if case == "abort-install":
                    self.assertIs(escaped, cleanup_control)
                else:
                    self.assertIs(type(escaped), rpc._ArchiveRpcError)
                    self.assertEqual(
                        (escaped.reason_code, escaped.failure_kind),
                        (
                            "authority_mismatch",
                            "historical_window_spool_handoff_failed",
                        ),
                    )
                    self.assertIsNone(escaped.__cause__)
                    self.assertIsNone(escaped.__context__)
                self.assertEqual(remaining, ())
                self.assertGreaterEqual(
                    len(output_payloads),
                    3 if case == "reserve-install" else 4,
                )
                self.assertIsNot(escaped, loop_control)
                first_reserve = next(
                    row for row in states
                    if row[0] == "_install_quota_reserve_transition"
                )
                spool_bytes, spool_members = first_reserve[1:3]
                retained_members = max(
                    row[2] for row in states
                    if row[0] == "_install_quota_commit_transition"
                )
                retained_bytes = max(
                    row[1] for row in states
                    if row[0] == "_install_quota_commit_transition"
                )
                expected_output_members = 3
                expected_output_bytes = sum(
                    output_payloads[:expected_output_members]
                )
                self.assertGreater(spool_members, 0)
                self.assertGreater(spool_bytes, 0)
                self.assertEqual(
                    (retained_bytes, retained_members),
                    (
                        spool_bytes + expected_output_bytes,
                        spool_members + expected_output_members,
                    ),
                )
                self.assertTrue(any(
                    row[3:6] == (0, 0, False)
                    for row in states
                    if row[0] in (
                        "_install_quota_commit_transition",
                        "_install_quota_abort_transition",
                    )
                ))
                expected_journal_states = {
                    "_install_quota_reserve_transition": "reserving",
                    "_install_quota_commit_transition": "committing",
                    "_install_quota_abort_transition": "aborting",
                }
                self.assertTrue(post_effect_journals)
                self.assertTrue(all(
                    state == expected_journal_states[name] and has_token
                    for name, state, has_token in post_effect_journals
                ))
                after_fds = frozenset(
                    int(name) for name in os.listdir("/dev/fd")
                    if name.isdigit()
                )
                self.assertEqual(after_fds, before_fds)


class HistoricalFoundryStorageTask4bSnapshotTests(unittest.TestCase):
    def test_slice6_generation_one_snapshot_moves_every_source_slot_and_reads_exact_frozen_tree(self):
        rpc = importlib.import_module("scripts.historical_foundry_rpc")
        scan = importlib.import_module("scripts.historical_foundry_scan")
        storage = importlib.import_module(
            "scripts.historical_foundry_storage"
        )
        fixture = None
        snapshot = None
        moves = []
        binding_empty_before_revoke = []
        generation_rows = []
        snapshot_allocations = [0]
        dup_calls = []
        move_phase = [False]
        post_move_close = []
        repeat_pair = []
        repeat_observed = []
        delivery_owner_ids = []
        prior_trace = sys.gettrace()
        original_dup = storage.os.dup

        def observed_dup(fd):
            if move_phase[0]:
                dup_calls.append(fd)
            return original_dup(fd)

        def tracer(frame, event, argument):
            if frame.f_code.co_filename == scan.__file__:
                if (
                    not post_move_close
                    and frame.f_code.co_name
                    == "_materialize_historical_window_staging_snapshot"
                    and event == "line"
                    and frame.f_locals.get("transaction") is None
                    and type(frame.f_locals.get("snapshot"))
                    is storage.HistoricalRunStagingSnapshot
                ):
                    post_move_close.append(
                        frame.f_locals["view"].close()
                    )
                return tracer
            if frame.f_code.co_filename != storage.__file__:
                return tracer
            name = frame.f_code.co_name
            if name == "_prepare_handle" and event == "return":
                record = frame.f_locals.get("record")
                if type(record) is dict:
                    generation_rows.append((
                        type(argument).__name__,
                        record.get("owner_generation"),
                        record.get("capture_generation"),
                    ))
                if type(argument) is storage.HistoricalRunStagingSnapshot:
                    snapshot_allocations[0] += 1
            if name == "_task4b_move_source_descriptor_slot":
                if event == "call":
                    move_phase[0] = True
                    row = frame.f_locals.get("row")
                    if type(row) is tuple and type(row[1]) is int:
                        details = os.fstat(row[1])
                        moves.append({
                            "kind": frame.f_locals.get("row_kind"),
                            "index": frame.f_locals.get("index"),
                            "fd": row[1],
                            "identity": (details.st_dev, details.st_ino),
                            "returned": False,
                        })
                elif event == "return" and moves:
                    emptied, slot = argument
                    current = moves[-1]
                    current["returned"] = (
                        type(emptied) is tuple
                        and emptied[1] is None
                        and type(slot) is dict
                        and slot.get("fd") == current["fd"]
                        and slot.get("close_state") == "pending"
                    )
            if name == "_close_bound_source_rows" and event == "call":
                record = frame.f_locals.get("binding_record")
                if type(record) is dict:
                    rows = record.get("ancestry_rows", ()) + record.get(
                        "source_rows", ()
                    )
                    binding_empty_before_revoke.append(
                        bool(rows) and all(row[1] is None for row in rows)
                    )
            if name == "_task4b_install_capture_snapshot" and event == "return":
                move_phase[0] = False
            if (
                name == "_task4b_acknowledge_snapshot_delivery"
                and event == "line"
                and type(frame.f_locals.get("entry")) is tuple
                and type(frame.f_locals.get("owner")) is dict
            ):
                delivery_owner_ids.append((
                    id(frame.f_locals["owner"]),
                    id(frame.f_locals["entry"][1]),
                ))
            return tracer

        try:
            with mock.patch.object(
                storage.os, "dup", side_effect=observed_dup
            ):
                sys.settrace(tracer)
                fixture, snapshot = _slice6_materialize_snapshot()
            sys.settrace(prior_trace)
            projection, inventory = _slice6_snapshot_inventory(snapshot)
            self.assertEqual(
                tuple(projection),
                (
                    "schema", "stage", "generation",
                    "capture_inventory_sha256", "frozen_member_count",
                    "frozen_physical_byte_count",
                    "quota_committed_physical_bytes",
                    "quota_committed_member_count",
                ),
            )
            self.assertEqual(
                tuple(projection.values())[:3],
                (
                    "historical_foundry_staging_snapshot_identity/v1",
                    "capture_frozen",
                    1,
                ),
            )
            self.assertNotIn("owner_generation", projection)
            self.assertEqual(snapshot_allocations[0], 1)
            self.assertEqual(dup_calls, [])
            self.assertEqual(
                tuple((row["kind"], row["index"]) for row in moves),
                (
                    ("ancestry", 0), ("ancestry", 1),
                    ("source", 0), ("source", 1), ("source", 2),
                    ("source", 3),
                ),
            )
            self.assertTrue(all(row["returned"] for row in moves))
            self.assertEqual(
                len({row["fd"] for row in moves}), len(moves)
            )
            self.assertEqual(binding_empty_before_revoke, [True])
            gc.collect()
            for row in moves:
                details = os.fstat(row["fd"])
                self.assertEqual(
                    (details.st_dev, details.st_ino), row["identity"]
                )
            owner_generations = [
                generation
                for _kind, generation, _capture in generation_rows
                if type(generation) is int
            ]
            self.assertEqual(owner_generations, sorted(owner_generations))
            self.assertGreater(owner_generations[-1], owner_generations[0])
            self.assertEqual(generation_rows[-1][2], 1)
            self.assertEqual(post_move_close, [None])
            self.assertTrue(delivery_owner_ids)
            self.assertTrue(all(
                owner_id == registry_owner_id
                for owner_id, registry_owner_id in delivery_owner_ids
            ))
            self.assertEqual(snapshot_allocations[0], 1)
            members = [
                (row["path"], row["sha256"], 1_048_576)
                for row in inventory["configs"]
            ] + [
                (row["path"], row["sha256"], 16_777_216)
                for row in inventory["raw_chunks"]
            ] + [
                (row["path"], row["gzip_sha256"], 16_842_752)
                for row in inventory["typed_chunks"]
            ]
            for path, digest, maximum in members:
                with self.subTest(path=path):
                    payload = snapshot.read_frozen_member(
                        path,
                        expected_sha256=digest,
                        max_bytes=maximum,
                    )
                    self.assertIs(type(payload), bytes)
                    self.assertEqual(hashlib.sha256(payload).hexdigest(), digest)
            self.assertIsNone(snapshot.reread_frozen_members_unchanged())
            self.assertIs(snapshot.__enter__(), snapshot)
            snapshot.__exit__(None, None, None)
            self.assertIsNone(snapshot.close())
            for row in moves:
                with self.assertRaises(OSError):
                    os.fstat(row["fd"])

            repeat_fixture = None
            repeat_snapshot = None
            repeat_error = None
            repeat_fired = [False]
            repeat_allocations = [0]

            def repeat_tracer(frame, event, argument):
                if frame.f_code.co_filename == scan.__file__:
                    if (
                        not repeat_fired[0]
                        and frame.f_code.co_name
                        == "_materialize_historical_window_staging_snapshot"
                        and event == "line"
                        and frame.f_locals.get("transaction") is None
                        and type(frame.f_locals.get("snapshot"))
                        is storage.HistoricalRunStagingSnapshot
                    ):
                        repeat_fired[0] = True
                        view = frame.f_locals.get("view")
                        saved = sys.gettrace()
                        sys.settrace(None)
                        try:
                            self.assertIsNone(view.close())
                            try:
                                view._materialize_staging_snapshot_from_bound_scan()
                            except BaseException as error:
                                repeat_observed.append((
                                    type(error).__name__,
                                    getattr(error, "reason_code", None),
                                    getattr(error, "failure_kind", None),
                                ))
                                if type(error) is rpc._ArchiveRpcError:
                                    repeat_pair.append((
                                        error.reason_code, error.failure_kind
                                    ))
                        finally:
                            sys.settrace(saved)
                    return repeat_tracer
                if frame.f_code.co_filename != storage.__file__:
                    return repeat_tracer
                name = frame.f_code.co_name
                if (
                    name == "_prepare_handle"
                    and event == "return"
                    and type(argument) is storage.HistoricalRunStagingSnapshot
                ):
                    repeat_allocations[0] += 1
                return repeat_tracer

            try:
                sys.settrace(repeat_tracer)
                try:
                    repeat_fixture, repeat_snapshot = (
                        _slice6_materialize_snapshot()
                    )
                except BaseException as error:
                    repeat_error = error
            finally:
                sys.settrace(prior_trace)
                if repeat_snapshot is not None:
                    repeat_snapshot.close()
                if repeat_fixture is not None:
                    repeat_fixture.close()
            self.assertTrue(repeat_fired[0])
            self.assertEqual(
                repeat_observed,
                [("HistoricalFoundryStorageError", None, None)],
            )
            self.assertEqual(repeat_pair, [])
            self.assertEqual(repeat_allocations[0], 1)
            self.assertIsNone(repeat_error)
            self.assertIsNotNone(repeat_snapshot)
        finally:
            sys.settrace(prior_trace)
            if snapshot is not None:
                try:
                    snapshot.close()
                except BaseException:
                    pass
            if fixture is not None:
                fixture.close()

    def test_slice6_snapshot_member_guards_precheck_before_open_and_reject_gzip_bombs(self):
        rpc = importlib.import_module("scripts.historical_foundry_rpc")
        storage = importlib.import_module(
            "scripts.historical_foundry_storage"
        )
        fixture = None
        snapshot = None
        try:
            fixture, snapshot = _slice6_materialize_snapshot()
            projection, inventory = _slice6_snapshot_inventory(snapshot)
            raw = inventory["raw_chunks"][0]
            typed = inventory["typed_chunks"][0]
            config = inventory["configs"][0]
            original_open = storage.os.open
            open_calls = [0]

            class IntSubclass(int):
                pass

            def counted_open(*args, **kwargs):
                open_calls[0] += 1
                return original_open(*args, **kwargs)

            invalid = (
                ("unknown.bin", "0" * 64, 1),
                (raw["path"], "0" * 64, 16_777_216),
                (raw["path"], raw["sha256"], True),
                (raw["path"], raw["sha256"], IntSubclass(16_777_216)),
                (raw["path"], raw["sha256"], 16_777_217),
                (config["path"], config["sha256"], 1_048_577),
                (
                    "scan/capture_inventory.json",
                    projection["capture_inventory_sha256"],
                    16_777_217,
                ),
                (
                    typed["path"], typed["gzip_sha256"],
                    16_842_753,
                ),
            )
            with mock.patch.object(
                storage.os, "open", side_effect=counted_open
            ) as patched_open:
                supported = set(storage.os.supports_dir_fd)
                supported.discard(original_open)
                supported.add(patched_open)
                with mock.patch.object(
                    storage.os, "supports_dir_fd", supported
                ):
                    for path, digest, maximum in invalid:
                        before = open_calls[0]
                        with self.subTest(path=path, maximum=maximum):
                            with self.assertRaises(Exception):
                                snapshot.read_frozen_member(
                                    path,
                                    expected_sha256=digest,
                                    max_bytes=maximum,
                                )
                        self.assertEqual(open_calls[0], before)
                    natural = (
                        (raw["path"], raw["sha256"], 16_777_216),
                        (
                            "scan/capture_inventory.json",
                            projection["capture_inventory_sha256"],
                            16_777_216,
                        ),
                        (typed["path"], typed["gzip_sha256"], 16_842_752),
                    )
                    for path, digest, maximum in natural:
                        before = open_calls[0]
                        payload = snapshot.read_frozen_member(
                            path,
                            expected_sha256=digest,
                            max_bytes=maximum,
                        )
                        self.assertIs(type(payload), bytes)
                        self.assertGreater(open_calls[0], before)
        finally:
            if snapshot is not None:
                try:
                    snapshot.close()
                except BaseException:
                    pass
            if fixture is not None:
                fixture.close()

        for mutation in ("crc-footer", "isize-bomb", "canonical-profile"):
            with self.subTest(mutation=mutation):
                fixture = None
                snapshot = None
                try:
                    fixture, snapshot = _slice6_materialize_snapshot()
                    projection, inventory = _slice6_snapshot_inventory(snapshot)
                    typed = inventory["typed_chunks"][0]
                    candidates = tuple(
                        fixture.data_dir.rglob(typed["path"])
                    )
                    self.assertEqual(len(candidates), 1)
                    target = candidates[0]
                    before = os.stat(str(target), follow_symlinks=False)
                    fd = os.open(
                        str(target), os.O_RDWR | os.O_NOFOLLOW | os.O_CLOEXEC
                    )
                    try:
                        physical = os.pread(fd, before.st_size, 0)
                        changed = bytearray(physical)
                        if mutation == "crc-footer":
                            changed[-8] ^= 1
                        elif mutation == "isize-bomb":
                            changed[-4:] = (16_777_217).to_bytes(4, "little")
                        else:
                            changed[8] = 2 if changed[8] != 2 else 4
                        self.assertEqual(len(changed), len(physical))
                        self.assertEqual(
                            os.pwrite(fd, bytes(changed), 0), len(changed)
                        )
                        os.fsync(fd)
                    finally:
                        os.close(fd)
                    after = os.stat(str(target), follow_symlinks=False)
                    self.assertEqual(
                        (after.st_dev, after.st_ino, after.st_size),
                        (before.st_dev, before.st_ino, before.st_size),
                    )
                    with self.assertRaises(rpc._ArchiveRpcError) as caught:
                        snapshot.read_frozen_member(
                            typed["path"],
                            expected_sha256=typed["gzip_sha256"],
                            max_bytes=16_842_752,
                        )
                    self.assertEqual(
                        (
                            caught.exception.reason_code,
                            caught.exception.failure_kind,
                        ),
                        (
                            "authority_mismatch",
                            "historical_window_spool_handoff_failed",
                        ),
                    )
                    self.assertIsNone(caught.exception.__cause__)
                    self.assertIsNone(caught.exception.__context__)
                    self.assertIsNone(snapshot.close())
                finally:
                    if snapshot is not None:
                        try:
                            snapshot.close()
                        except BaseException:
                            pass
                    if fixture is not None:
                        fixture.close()

    def test_slice6_snapshot_authority_currentness_transplant_and_filesystem_drift_matrix(self):
        rpc = importlib.import_module("scripts.historical_foundry_rpc")
        storage = importlib.import_module(
            "scripts.historical_foundry_storage"
        )
        drift_cases = (
            "storage-local-surface", "bound-authority-replacement",
            "stale-owner-generation", "stale-capture-generation",
            "wrong-owner", "member-inode-swap", "ancestry-swap",
        )
        for case in drift_cases:
            with self.subTest(case=case):
                fixture = None
                snapshot = None
                original_surface = None
                original_method = None
                original_stat = storage.os.stat
                original_fstat = storage.os.fstat
                stat_patch = None
                support_patch = None
                follow_patch = None
                fstat_patch = None
                foreign_fd = None
                held_ancestry_fd = [None]
                mutation_fired = [False]
                prior_trace = sys.gettrace()

                def capture_tracer(frame, event, _argument):
                    if (
                        frame.f_code.co_filename == storage.__file__
                        and frame.f_code.co_name
                        == "_task4b_move_source_descriptor_slot"
                        and event == "call"
                        and frame.f_locals.get("row_kind") == "ancestry"
                        and held_ancestry_fd[0] is None
                    ):
                        held_ancestry_fd[0] = frame.f_locals["row"][1]
                    return capture_tracer

                try:
                    sys.settrace(capture_tracer)
                    fixture, snapshot = _slice6_materialize_snapshot()
                    sys.settrace(prior_trace)
                    projection, inventory = _slice6_snapshot_inventory(snapshot)

                    if case == "storage-local-surface":
                        original_surface = (
                            storage._TASK4B_STORAGE_LOCAL_SURFACE_OBJECTS
                        )
                        storage._TASK4B_STORAGE_LOCAL_SURFACE_OBJECTS = tuple(
                            list(original_surface[:-1]) + [lambda: None]
                        )
                    elif case == "bound-authority-replacement":
                        original_method = (
                            storage.HistoricalRunStagingSnapshot
                            .frozen_identity_projection
                        )
                        storage.HistoricalRunStagingSnapshot.frozen_identity_projection = (
                            lambda _self: {}
                        )
                    elif case in (
                        "stale-owner-generation",
                        "stale-capture-generation",
                        "wrong-owner",
                    ):
                        def mutate_tracer(frame, event, _argument):
                            if (
                                not mutation_fired[0]
                                and frame.f_code.co_filename == storage.__file__
                                and frame.f_code.co_name
                                == "_task4b_current_snapshot_owner"
                                and event == "line"
                                and type(frame.f_locals.get("owner")) is dict
                            ):
                                mutation_fired[0] = True
                                owner = frame.f_locals["owner"]
                                if case == "stale-owner-generation":
                                    owner["owner_generation"] += 1
                                elif case == "stale-capture-generation":
                                    owner["capture_generation"] = 0
                                else:
                                    owner["_task4b_snapshot_handle"] = object()
                            return mutate_tracer
                        sys.settrace(mutate_tracer)
                    elif case == "member-inode-swap":
                        target = inventory["typed_chunks"][0]
                        basename = target["path"].split("/")[-1]
                        foreign = os.stat(__file__, follow_symlinks=False)

                        def swapped_stat(path, *args, **kwargs):
                            if (
                                not mutation_fired[0]
                                and path == basename
                                and type(kwargs.get("dir_fd")) is int
                                and kwargs.get("follow_symlinks") is False
                            ):
                                mutation_fired[0] = True
                                return foreign
                            return original_stat(path, *args, **kwargs)

                        stat_patch = mock.patch.object(
                            storage.os, "stat", side_effect=swapped_stat
                        )
                        patched_stat = stat_patch.start()
                        supported = set(storage.os.supports_dir_fd)
                        supported.discard(original_stat)
                        supported.add(patched_stat)
                        follow = set(storage.os.supports_follow_symlinks)
                        follow.discard(original_stat)
                        follow.add(patched_stat)
                        support_patch = mock.patch.object(
                            storage.os, "supports_dir_fd", supported
                        )
                        follow_patch = mock.patch.object(
                            storage.os, "supports_follow_symlinks", follow
                        )
                        support_patch.start()
                        follow_patch.start()
                    else:
                        foreign_fd = os.open(
                            __file__, os.O_RDONLY | os.O_CLOEXEC
                        )
                        foreign = original_fstat(foreign_fd)

                        def swapped_fstat(fd):
                            if (
                                not mutation_fired[0]
                                and fd == held_ancestry_fd[0]
                            ):
                                mutation_fired[0] = True
                                return foreign
                            return original_fstat(fd)

                        fstat_patch = mock.patch.object(
                            storage.os, "fstat", side_effect=swapped_fstat
                        )
                        fstat_patch.start()

                    with self.assertRaises(rpc._ArchiveRpcError) as caught:
                        if case == "bound-authority-replacement":
                            original_method(snapshot)
                        elif case == "member-inode-swap":
                            target = inventory["typed_chunks"][0]
                            snapshot.read_frozen_member(
                                target["path"],
                                expected_sha256=target["gzip_sha256"],
                                max_bytes=16_842_752,
                            )
                        else:
                            snapshot.frozen_identity_projection()
                    expected_kind = (
                        "historical_window_spool_handoff_failed"
                        if case == "member-inode-swap"
                        else "final_identity_drift"
                    )
                    self.assertEqual(
                        (
                            caught.exception.reason_code,
                            caught.exception.failure_kind,
                        ),
                        ("authority_mismatch", expected_kind),
                    )
                    self.assertIsNone(caught.exception.__cause__)
                    self.assertIsNone(caught.exception.__context__)
                    if case in (
                        "stale-owner-generation", "stale-capture-generation",
                        "wrong-owner", "member-inode-swap", "ancestry-swap",
                    ):
                        self.assertTrue(mutation_fired[0])
                    self.assertIsNone(snapshot.close())
                finally:
                    sys.settrace(prior_trace)
                    if original_surface is not None:
                        storage._TASK4B_STORAGE_LOCAL_SURFACE_OBJECTS = (
                            original_surface
                        )
                    if original_method is not None:
                        storage.HistoricalRunStagingSnapshot.frozen_identity_projection = (
                            original_method
                        )
                    for patcher in (
                        follow_patch, support_patch, stat_patch, fstat_patch,
                    ):
                        if patcher is not None:
                            patcher.stop()
                    if type(foreign_fd) is int:
                        try:
                            os.close(foreign_fd)
                        except OSError:
                            pass
                    if snapshot is not None:
                        try:
                            snapshot.close()
                        except BaseException:
                            pass
                    if fixture is not None:
                        fixture.close()

        fixture = None
        snapshot = None
        try:
            fixture, snapshot = _slice6_materialize_snapshot()
            true_projection = snapshot.frozen_identity_projection()
            for operation in (
                lambda: copy.copy(snapshot),
                lambda: copy.deepcopy(snapshot),
            ):
                with self.assertRaises(TypeError):
                    operation()
            clone = object.__new__(storage.HistoricalRunStagingSnapshot)
            for operation in (
                lambda: clone.frozen_identity_projection(),
                lambda: storage.HistoricalRunStagingSnapshot
                .frozen_identity_projection(clone),
            ):
                with self.assertRaises(storage.HistoricalFoundryStorageError):
                    operation()
            spec = importlib.util.spec_from_file_location(
                "_slice6_storage_reload_probe", storage.__file__
            )
            reloaded = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(reloaded)
            with self.assertRaises(reloaded.HistoricalFoundryStorageError):
                reloaded.HistoricalRunStagingSnapshot.frozen_identity_projection(
                    snapshot
                )
            self.assertEqual(
                snapshot.frozen_identity_projection(), true_projection
            )
        finally:
            if snapshot is not None:
                snapshot.close()
            if fixture is not None:
                fixture.close()

        from tests.test_historical_foundry_scan import (
            _Task4bOfflineCapabilityFixture,
        )
        scan = importlib.import_module("scripts.historical_foundry_scan")
        fixture = _Task4bOfflineCapabilityFixture()
        repeat_binder = []
        repeat_fired = [False]
        prior_trace = sys.gettrace()

        def binder_tracer(frame, event, _argument):
            if (
                not repeat_fired[0]
                and frame.f_code.co_filename == storage.__file__
                and frame.f_code.co_name
                == "_bind_reconciliation_from_bound_scan"
                and event == "return"
            ):
                repeat_fired[0] = True
                source = frame.f_locals.get("self")
                saved = sys.gettrace()
                sys.settrace(None)
                try:
                    try:
                        source._bind_reconciliation_from_bound_scan(
                            expected_view=frame.f_locals.get("expected_view"),
                            expected_reconciliation=frame.f_locals.get(
                                "expected_reconciliation"
                            ),
                        )
                    except rpc._ArchiveRpcError as error:
                        repeat_binder.append((
                            error.reason_code, error.failure_kind
                        ))
                finally:
                    sys.settrace(saved)
            return binder_tracer

        try:
            capability = fixture.mint()
            sys.settrace(binder_tracer)
            binder_snapshot = (
                scan._materialize_historical_window_staging_snapshot(
                    capability=capability
                )
            )
            self.assertEqual(
                binder_snapshot.frozen_identity_projection()["generation"], 1
            )
            binder_snapshot.close()
        finally:
            sys.settrace(prior_trace)
            fixture.capability = None
            fixture.close()
        self.assertTrue(repeat_fired[0])
        self.assertEqual(
            repeat_binder,
            [("authority_mismatch", "historical_window_capability_invalid")],
        )

    def test_slice6_strong_live_snapshot_survives_gc_until_explicit_idempotent_close(self):
        storage = importlib.import_module(
            "scripts.historical_foundry_storage"
        )
        consumerless_keys = (
            "_task4b_raw_exchange_records",
            "_task4b_exchange_joins",
            "_task4b_raw_chunks",
            "_task4b_typed_chunks",
            "_task4b_capture_phase",
        )
        publication_owner_keys = []
        prior_trace = sys.gettrace()

        def publication_tracer(frame, event, _argument):
            if (
                frame.f_code.co_filename == storage.__file__
                and frame.f_code.co_name == "_prepare_handle"
                and event == "call"
                and frame.f_locals.get("authority_class")
                is storage.HistoricalRunStagingSnapshot
            ):
                owner = frame.f_locals.get("record")
                publication_owner_keys.append(tuple(
                    key for key in consumerless_keys if key in owner
                ))
            return publication_tracer

        try:
            sys.settrace(publication_tracer)
            fixture, snapshot = _slice6_materialize_snapshot()
        finally:
            sys.settrace(prior_trace)
        projection, inventory = _slice6_snapshot_inventory(snapshot)
        self.assertEqual(publication_owner_keys, [()])
        raw = inventory["raw_chunks"][0]
        frozen_raw = snapshot.read_frozen_member(
            raw["path"],
            expected_sha256=raw["sha256"],
            max_bytes=16_777_216,
        )
        self.assertIs(type(frozen_raw), bytes)
        self.assertEqual(hashlib.sha256(frozen_raw).hexdigest(), raw["sha256"])
        member_hashes = tuple(
            (row["path"], row["sha256"])
            for row in inventory["configs"] + inventory["raw_chunks"]
        ) + tuple(
            (row["path"], row["gzip_sha256"])
            for row in inventory["typed_chunks"]
        )
        reference = weakref.ref(snapshot)
        snapshot = None
        gc.collect()
        recovered = reference()
        try:
            self.assertIs(type(recovered), storage.HistoricalRunStagingSnapshot)
            self.assertEqual(
                recovered.frozen_identity_projection(), projection
            )
            self.assertIsNone(recovered.reread_frozen_members_unchanged())
            recovered_projection, recovered_inventory = (
                _slice6_snapshot_inventory(recovered)
            )
            self.assertEqual(recovered_projection, projection)
            self.assertEqual(
                tuple(
                    (row["path"], row["sha256"])
                    for row in recovered_inventory["configs"]
                    + recovered_inventory["raw_chunks"]
                ) + tuple(
                    (row["path"], row["gzip_sha256"])
                    for row in recovered_inventory["typed_chunks"]
                ),
                member_hashes,
            )
            self.assertIsNone(recovered.close())
            self.assertIsNone(recovered.close())
        finally:
            recovered = None
            gc.collect()
            fixture.close()
        self.assertIsNone(reference())
        for authority_class in (
            storage._HistoricalWindowCaptureReplaySource,
            storage._ConsumedProductionHistoricalWindowCapabilityView,
            storage.HistoricalRunStagingSnapshot,
        ):
            self.assertNotIn("__del__", vars(authority_class))


class HistoricalFoundryStorageTask4bControlFlowTests(unittest.TestCase):
    def test_slice6_four_controls_at_every_new_transition_boundary(self):
        from tests.test_historical_foundry_scan import (
            _Task4bOfflineCapabilityFixture,
        )

        scan = importlib.import_module("scripts.historical_foundry_scan")
        storage = importlib.import_module(
            "scripts.historical_foundry_storage"
        )
        move_slots = []
        probe_fixture = None
        probe_snapshot = None
        prior_trace = sys.gettrace()

        def discover_moves(frame, event, _argument):
            if (
                frame.f_code.co_filename == storage.__file__
                and frame.f_code.co_name
                == "_task4b_move_source_descriptor_slot"
                and event == "call"
            ):
                move_slots.append((
                    frame.f_locals.get("row_kind"),
                    frame.f_locals.get("index"),
                ))
            return discover_moves

        try:
            sys.settrace(discover_moves)
            probe_fixture, probe_snapshot = _slice6_materialize_snapshot()
        finally:
            sys.settrace(prior_trace)
            if probe_snapshot is not None:
                probe_snapshot.close()
            if probe_fixture is not None:
                probe_fixture.close()
        self.assertEqual(
            tuple(move_slots),
            (
                ("ancestry", 0), ("ancestry", 1),
                ("source", 0), ("source", 1), ("source", 2),
                ("source", 3),
            ),
        )

        boundaries = (
            "source-publication", "snapshot-publication",
            "quota-reserve-pre", "quota-reserve-post",
            "quota-commit-pre", "quota-commit-post",
            "quota-abort-pre", "quota-abort-post",
            *("move-{}-{}-{}".format(position, kind, index)
              for kind, index in move_slots
              for position in ("pre", "post")),
            "spool-unlink-pre", "spool-unlink-post",
            "snapshot-issue", "storage-assignment",
            "scan-assignment",
        )
        control_classes = (
            KeyboardInterrupt, SystemExit, GeneratorExit,
            asyncio.CancelledError,
        )
        for boundary in boundaries:
            for control_class in control_classes:
                control = control_class(
                    "slice6 {} {}".format(boundary, control_class.__name__)
                )
                with self.subTest(
                    boundary=boundary, control=control_class.__name__
                ):
                    before_fds = frozenset(
                        int(name) for name in os.listdir("/dev/fd")
                        if name.isdigit()
                    )
                    fixture = _Task4bOfflineCapabilityFixture()
                    original_unlink = storage.os.unlink
                    original_fsync = storage.os.fsync
                    original_close = storage.os.close
                    original_pwrite = storage.os.pwrite
                    fired = [False]
                    abort_body_fired = [False]
                    output_member_count = [0]
                    moved_fds = []
                    snapshot_reference = [None]
                    spool_helper_active = [False]
                    spool_file_fd = [None]
                    spool_parent_fd = [None]
                    spool_unlink_effects = []
                    spool_retirement_actions = []
                    spool_quota_before = []
                    spool_retirement_states = []
                    spool_quota_reader = [None]
                    spool_patchers = []
                    result = None
                    escaped = None
                    remaining_before_manual_close = None
                    leaked_moved_fds = []
                    opcode_cache = {}

                    def instruction(frame):
                        rows = opcode_cache.get(frame.f_code)
                        if rows is None:
                            rows = {
                                row.offset: (row.opname, row.argval)
                                for row in dis.get_instructions(frame.f_code)
                            }
                            opcode_cache[frame.f_code] = rows
                        return rows.get(frame.f_lasti)

                    def observed_unlink(path, *args, **kwargs):
                        is_spool = (
                            spool_helper_active[0]
                            and
                            boundary.startswith("spool-unlink-")
                            and type(path) is str
                            and path.startswith(
                                ".historical-foundry-exchange-spool-"
                            )
                            and path.endswith(".bin")
                        )
                        if is_spool and not fired[0]:
                            fired[0] = True
                            if boundary == "spool-unlink-post":
                                outcome = original_unlink(
                                    path, *args, **kwargs
                                )
                                spool_unlink_effects.append((
                                    path, kwargs.get("dir_fd")
                                ))
                                spool_retirement_actions.append("unlink")
                                raise control
                            raise control
                        outcome = original_unlink(path, *args, **kwargs)
                        if is_spool:
                            spool_unlink_effects.append((
                                path, kwargs.get("dir_fd")
                            ))
                            spool_retirement_actions.append("unlink")
                        return outcome

                    def observed_fsync(fd):
                        outcome = original_fsync(fd)
                        if (
                            spool_helper_active[0]
                            and spool_unlink_effects
                            and fd == spool_parent_fd[0]
                        ):
                            spool_retirement_actions.append("parent-fsync")
                        return outcome

                    def observed_close(fd):
                        outcome = original_close(fd)
                        if (
                            spool_helper_active[0]
                            and spool_unlink_effects
                            and fd == spool_file_fd[0]
                        ):
                            spool_retirement_actions.append("spool-close")
                        return outcome

                    def observed_pwrite(fd, payload, offset):
                        if boundary.startswith("quota-abort-") and offset == 0:
                            output_member_count[0] += 1
                            if output_member_count[0] == 4:
                                abort_body_fired[0] = True
                                raise OSError("slice6 abort trigger")
                        return original_pwrite(fd, payload, offset)

                    def tracer(frame, event, argument):
                        filename = frame.f_code.co_filename
                        name = frame.f_code.co_name
                        if filename == storage.__file__:
                            if (
                                boundary.startswith("spool-unlink-")
                                and name
                                == "_task4b_retire_committed_spool"
                            ):
                                owner = frame.f_locals.get("owner")
                                owner_journal = (
                                    owner.get("_task4b_spool_retirement")
                                    if type(owner) is dict else None
                                )
                                if event == "call":
                                    quota_reader = frame.f_back.f_locals.get(
                                        "_quota_record_for_owner"
                                    )
                                    if not callable(quota_reader):
                                        raise AssertionError(
                                            "slice6 quota reader differs"
                                        )
                                    spool_helper_active[0] = True
                                    spool_file_fd[0] = owner.get("file_fd")
                                    spool_parent_fd[0] = (
                                        owner.get("chain")[-1][0]
                                    )
                                    spool_quota_reader[0] = quota_reader
                                    fsync_patcher = mock.patch.object(
                                        storage.os,
                                        "fsync",
                                        side_effect=observed_fsync,
                                    )
                                    close_patcher = mock.patch.object(
                                        storage.os,
                                        "close",
                                        side_effect=observed_close,
                                    )
                                    fsync_patcher.start()
                                    close_patcher.start()
                                    spool_patchers.extend((
                                        close_patcher, fsync_patcher,
                                    ))
                                    quota = quota_reader(owner)
                                    spool_quota_before.append((
                                        quota["committed_physical_bytes"],
                                        quota["committed_members"],
                                    ))
                                elif (
                                    event == "line"
                                    and not spool_retirement_states
                                    and owner_journal
                                    is frame.f_locals.get("journal")
                                    and owner_journal == {"phase": "done"}
                                    and owner.get("file_fd") is None
                                    and owner.get("basename") is None
                                    and owner.get("file_identity") is None
                                ):
                                    quota = spool_quota_reader[0](owner)
                                    spool_retirement_states.append((
                                        dict(owner_journal),
                                        owner.get("file_fd"),
                                        owner.get("basename"),
                                        owner.get("file_identity"),
                                        (
                                            quota[
                                                "committed_physical_bytes"
                                            ],
                                            quota["committed_members"],
                                        ),
                                    ))
                                elif event == "return":
                                    spool_helper_active[0] = False
                            if (
                                name == "_prepare_handle"
                                and event == "return"
                                and type(argument)
                                is storage.HistoricalRunStagingSnapshot
                            ):
                                snapshot_reference[0] = weakref.ref(argument)
                            if event == "call" and boundary == "snapshot-issue" and (
                                name == "_prepare_handle"
                                and any(
                                    type(value) is type
                                    and value.__name__
                                    == "HistoricalRunStagingSnapshot"
                                    for value in frame.f_locals.values()
                                )
                            ) and not fired[0]:
                                fired[0] = True
                                raise control
                            if (
                                event
                                == (
                                    "call"
                                    if boundary.endswith("-pre")
                                    else "return"
                                )
                                and not fired[0]
                                and boundary.startswith(
                                    ("quota-reserve-", "quota-commit-")
                                )
                                and name == {
                                    "quota-reserve": "_install_quota_reserve_transition",
                                    "quota-commit": "_install_quota_commit_transition",
                                }[boundary.rsplit("-", 1)[0]]
                            ):
                                fired[0] = True
                                raise control
                            if (
                                boundary.startswith("quota-abort-")
                                and abort_body_fired[0]
                                and not fired[0]
                                and name == "_install_quota_abort_transition"
                                and event
                                == (
                                    "call"
                                    if boundary.endswith("-pre")
                                    else "return"
                                )
                            ):
                                fired[0] = True
                                raise control
                            if name == "_task4b_move_source_descriptor_slot" and event == "call":
                                row = frame.f_locals.get("row")
                                if type(row) is tuple and type(row[1]) is int:
                                    moved_fds.append(row[1])
                            if (
                                boundary.startswith("move-")
                                and not fired[0]
                                and name == "_task4b_move_bound_source_authority"
                                and event == "line"
                            ):
                                _prefix, position, kind, index_text = (
                                    boundary.split("-")
                                )
                                index = int(index_text)
                                record = frame.f_locals.get("binding_record")
                                key = kind + "_rows"
                                rows = record.get(key) if type(record) is dict else None
                                slots = frame.f_locals.get("authority", {}).get(
                                    kind + "_slots", ()
                                )
                                if (
                                    type(rows) is tuple
                                    and index < len(rows)
                                    and len(slots) > index
                                    and frame.f_locals.get("owner", {}).get(
                                        "_task4b_snapshot_source_authority"
                                    ) is frame.f_locals.get("authority")
                                    and (
                                        (
                                            position == "pre"
                                            and type(rows[index][1]) is int
                                            and slots[index].get("move_state")
                                            in ("pending", "attempting")
                                        )
                                        or (
                                            position == "post"
                                            and rows[index][1] is None
                                        )
                                    )
                                ):
                                    fired[0] = True
                                    raise control
                            if (
                                boundary == "source-publication"
                                and not fired[0]
                                and name == "_materialize_task4b_capture_core"
                                and event == "line"
                            ):
                                source = frame.f_locals.get("source")
                                source_record = frame.f_locals.get("source_record")
                                owner = frame.f_locals.get("owner")
                                if (
                                    source is not None
                                    and type(source_record) is dict
                                    and source_record.get("source") is source
                                    and type(owner) is dict
                                    and owner.get("capture_replay_source") is source
                                ):
                                    fired[0] = True
                                    raise control
                            if (
                                boundary == "snapshot-publication"
                                and not fired[0]
                                and name == "_task4b_install_capture_snapshot"
                                and event == "line"
                            ):
                                owner = frame.f_locals.get("owner")
                                snapshot = frame.f_locals.get("snapshot")
                                guard = frame.f_locals.get("delivery_guard")
                                if (
                                    type(snapshot)
                                    is storage.HistoricalRunStagingSnapshot
                                    and type(owner) is dict
                                    and owner.get("_task4b_snapshot_handle") is snapshot
                                    and type(guard) is list
                                    and guard[0] is not None
                                ):
                                    fired[0] = True
                                    raise control
                            if (
                                boundary == "storage-assignment"
                                and not fired[0]
                                and name == "_task4b_install_capture_snapshot"
                            ):
                                frame.f_trace_opcodes = True
                                if event == "opcode" and instruction(frame) == (
                                    "STORE_FAST", "snapshot"
                                ):
                                    fired[0] = True
                                    raise control
                        elif (
                            filename == scan.__file__
                            and boundary == "scan-assignment"
                            and name
                            == "_materialize_historical_window_staging_snapshot"
                        ):
                            frame.f_trace_opcodes = True
                            if (
                                not fired[0]
                                and snapshot_reference[0] is not None
                                and event == "opcode"
                                and instruction(frame) == ("STORE_FAST", "snapshot")
                            ):
                                fired[0] = True
                                raise control
                        return tracer

                    try:
                        capability = fixture.mint()
                        with mock.patch.object(
                            storage.os, "unlink", side_effect=observed_unlink
                        ) as patched_unlink:
                            with mock.patch.object(
                                storage.os,
                                "pwrite",
                                side_effect=observed_pwrite,
                            ):
                                supported = set(storage.os.supports_dir_fd)
                                supported.discard(original_unlink)
                                supported.add(patched_unlink)
                                with mock.patch.object(
                                    storage.os, "supports_dir_fd", supported
                                ):
                                    sys.settrace(tracer)
                                    try:
                                        result = scan._materialize_historical_window_staging_snapshot(
                                            capability=capability
                                        )
                                    except BaseException as error:
                                        escaped = error
                    finally:
                        sys.settrace(prior_trace)
                        while spool_patchers:
                            spool_patchers.pop().stop()
                        fixture.capability = None
                        live_snapshot = (
                            snapshot_reference[0]()
                            if snapshot_reference[0] is not None else None
                        )
                        remaining_before_manual_close = tuple(
                            fixture.data_dir.iterdir()
                        )
                        if result is not None:
                            result.close()
                        elif live_snapshot is not None:
                            live_snapshot.close()
                        try:
                            fixture.close()
                        except BaseException:
                            pass
                        for fd in moved_fds:
                            try:
                                os.fstat(fd)
                            except OSError:
                                continue
                            leaked_moved_fds.append(fd)
                            os.close(fd)
                    fired_observed = fired[0]
                    no_result_observed = result is None
                    self.assertIs(escaped, control)
                    self.assertIsNone(control.__context__)
                    self.assertIsNone(control.__cause__)
                    self.assertEqual(remaining_before_manual_close, ())
                    self.assertEqual(leaked_moved_fds, [])
                    if boundary.startswith("spool-unlink-"):
                        self.assertEqual(len(spool_unlink_effects), 1)
                        self.assertEqual(
                            spool_unlink_effects[0][1], spool_parent_fd[0]
                        )
                        self.assertEqual(
                            spool_retirement_actions,
                            ["unlink", "parent-fsync", "spool-close"],
                        )
                        self.assertEqual(len(spool_quota_before), 1)
                        self.assertGreater(spool_quota_before[0][0], 0)
                        self.assertGreater(spool_quota_before[0][1], 0)
                        self.assertEqual(
                            spool_retirement_states,
                            [(
                                {"phase": "done"},
                                None,
                                None,
                                None,
                                spool_quota_before[0],
                            )],
                        )
                    after_fds = frozenset(
                        int(name) for name in os.listdir("/dev/fd")
                        if name.isdigit()
                    )
                    self.assertEqual(after_fds, before_fds)

        cleanup_phases = (
            "capture_cleanup", "source_cleanup", "revoke",
            "retire", "cleanup", "release",
        )
        cleanup_callees = {
            "capture_cleanup": "_cleanup_task4b_capture_staging",
            "source_cleanup": "_task4b_close_snapshot_source_authority",
            "revoke": "_revoke_bound_source",
            "retire": "_retire_lineage",
            "cleanup": "_cleanup_resources",
            "release": "_retire_nonowner_handle",
        }
        for phase in cleanup_phases:
            for control_class in control_classes:
                control = control_class(
                    "slice6 cleanup {} {}".format(phase, control_class.__name__)
                )
                with self.subTest(
                    cleanup_phase=phase, control=control_class.__name__
                ):
                    fixture, snapshot = _slice6_materialize_snapshot()
                    fired = [False]
                    escaped = None
                    remaining = None
                    prior_trace = sys.gettrace()

                    def cleanup_tracer(frame, event, _argument):
                        if (
                            not fired[0]
                            and frame.f_code.co_filename == storage.__file__
                            and frame.f_code.co_name == cleanup_callees[phase]
                            and event == "call"
                            and (
                                phase != "release"
                                or (
                                    frame.f_locals.get("handle") is snapshot
                                    and frame.f_back is not None
                                    and frame.f_back.f_code.co_name
                                    == "_close_moved_owner"
                                    and frame.f_locals.get("registry")
                                    is frame.f_back.f_locals.get("registry")
                                )
                            )
                        ):
                            fired[0] = True
                            raise control
                        return cleanup_tracer

                    try:
                        sys.settrace(cleanup_tracer)
                        try:
                            snapshot.close()
                        except BaseException as error:
                            escaped = error
                    finally:
                        sys.settrace(prior_trace)
                        try:
                            snapshot.close()
                        except BaseException:
                            pass
                        remaining = tuple(fixture.data_dir.iterdir())
                        fixture.close()
                    self.assertTrue(fired[0])
                    self.assertIs(escaped, control)
                    self.assertEqual(remaining, ())

    def test_slice6_body_cleanup_priority_and_resumable_fd_close(self):
        storage = importlib.import_module(
            "scripts.historical_foundry_storage"
        )
        rpc = importlib.import_module("scripts.historical_foundry_rpc")
        scan = importlib.import_module("scripts.historical_foundry_scan")
        from tests.test_historical_foundry_scan import (
            _Task4bOfflineCapabilityFixture,
        )

        for body_kind in ("control", "ordinary"):
            with self.subTest(body_kind=body_kind):
                fixture, snapshot = _slice6_materialize_snapshot()
                original_close = storage.os.close
                cleanup_control = SystemExit(
                    "slice6 first cleanup control"
                )
                close_calls = []

                def close_then_control(fd):
                    close_calls.append(fd)
                    original_close(fd)
                    if len(close_calls) == 1:
                        raise cleanup_control
                    return None

                body = (
                    KeyboardInterrupt("slice6 context body control")
                    if body_kind == "control"
                    else RuntimeError("slice6 ordinary context body")
                )
                escaped = None
                try:
                    with mock.patch.object(
                        storage.os, "close", side_effect=close_then_control
                    ):
                        try:
                            with snapshot:
                                raise body
                        except BaseException as error:
                            escaped = error
                    self.assertIs(
                        escaped,
                        body if body_kind == "control" else cleanup_control,
                    )
                    self.assertEqual(len(close_calls), len(set(close_calls)))
                    self.assertIsNone(snapshot.close())
                    self.assertIsNone(snapshot.close())
                finally:
                    try:
                        snapshot.close()
                    except BaseException:
                        pass
                    fixture.close()

        fixture = _Task4bOfflineCapabilityFixture()
        try:
            capability = fixture.mint()
            with mock.patch.object(
                storage.os,
                "pwrite",
                side_effect=OSError("slice6 ordinary body sentinel"),
            ):
                with self.assertRaises(rpc._ArchiveRpcError) as caught:
                    scan._materialize_historical_window_staging_snapshot(
                        capability=capability
                    )
            self.assertEqual(
                (caught.exception.reason_code, caught.exception.failure_kind),
                (
                    "authority_mismatch",
                    "historical_window_spool_handoff_failed",
                ),
            )
            self.assertIsNone(caught.exception.__cause__)
            self.assertIsNone(caught.exception.__context__)
        finally:
            fixture.capability = None
            fixture.close()

        fixture, snapshot = _slice6_materialize_snapshot()
        original_close = storage.os.close
        target = {"fd": None, "calls": []}
        entered_control = GeneratorExit("slice6 entered close control")

        def entered_close(fd):
            if target["fd"] is None:
                target["fd"] = fd
            if fd == target["fd"]:
                target["calls"].append(fd)
                original_close(fd)
                raise entered_control
            return original_close(fd)

        escaped = None
        try:
            with mock.patch.object(
                storage.os, "close", side_effect=entered_close
            ):
                try:
                    snapshot.close()
                except BaseException as error:
                    escaped = error
            self.assertIs(escaped, entered_control)
            self.assertIs(type(target["fd"]), int)
            with self.assertRaises(OSError):
                original_close(target["fd"])
            self.assertIsNone(snapshot.close())
            self.assertEqual(target["calls"], [target["fd"]])
        finally:
            try:
                snapshot.close()
            except BaseException:
                pass
            fixture.close()

    def test_slice6_snapshot_read_context_exit_and_delivery_controls(self):
        from tests.test_historical_foundry_scan import (
            _Task4bOfflineCapabilityFixture,
        )

        scan = importlib.import_module("scripts.historical_foundry_scan")
        storage = importlib.import_module(
            "scripts.historical_foundry_storage"
        )
        rpc = importlib.import_module("scripts.historical_foundry_rpc")
        controls = (
            KeyboardInterrupt, SystemExit, GeneratorExit,
            asyncio.CancelledError,
        )

        for surface in ("read", "context", "explicit-close"):
            for control_class in controls:
                with self.subTest(
                    surface=surface, control=control_class.__name__
                ):
                    fixture, snapshot = _slice6_materialize_snapshot()
                    reference = weakref.ref(snapshot)
                    control = control_class(
                        "slice6 {} control".format(surface)
                    )
                    fired = [False]
                    escaped = None
                    prior_trace = sys.gettrace()

                    def tracer(frame, event, _argument):
                        if frame.f_code.co_filename != storage.__file__:
                            return tracer
                        if (
                            surface == "read"
                            and not fired[0]
                            and frame.f_code.co_name
                            == "_task4b_reread_capture_member"
                            and event == "line"
                            and type(frame.f_locals.get("fd")) is int
                        ):
                            fired[0] = True
                            raise control
                        if (
                            surface == "explicit-close"
                            and not fired[0]
                            and frame.f_code.co_name
                            == "_task4b_close_snapshot_source_authority"
                            and event == "call"
                        ):
                            fired[0] = True
                            raise control
                        return tracer

                    try:
                        if surface == "context":
                            fired[0] = True
                            try:
                                with snapshot:
                                    raise control
                            except BaseException as error:
                                escaped = error
                        else:
                            sys.settrace(tracer)
                            try:
                                if surface == "read":
                                    projection, inventory = (
                                        _slice6_snapshot_inventory(snapshot)
                                    )
                                    raw = inventory["raw_chunks"][0]
                                    snapshot.read_frozen_member(
                                        raw["path"],
                                        expected_sha256=raw["sha256"],
                                        max_bytes=16_777_216,
                                    )
                                else:
                                    snapshot.close()
                            except BaseException as error:
                                escaped = error
                    finally:
                        sys.settrace(prior_trace)
                    fired_observed = fired[0]
                    identity_observed = escaped is control
                    context_observed = control.__context__
                    self.assertIsNone(snapshot.close())
                    self.assertIsNone(snapshot.close())
                    snapshot = None
                    escaped = None
                    control = None
                    gc.collect()
                    reference_cleared = reference() is None
                    fixture.close()
                    self.assertTrue(fired_observed)
                    self.assertTrue(identity_observed)
                    self.assertIsNone(context_observed)
                    self.assertTrue(reference_cleared)

        for builder_name in (
            "_bound_source_drift",
            "_task4b_bound_error",
            "_task4b_snapshot_error",
        ):
            for control_class in controls:
                with self.subTest(
                    error_builder=builder_name,
                    control=control_class.__name__,
                ):
                    fixture = None
                    snapshot = None
                    view = None
                    control = control_class(
                        "slice6 {} constructor control".format(builder_name)
                    )
                    fired = [False]
                    escaped = None
                    remaining = None
                    prior_trace = sys.gettrace()
                    original_source_next = (
                        storage._HistoricalWindowCaptureReplaySource.__next__
                    )
                    original_exported = (
                        storage._TASK4B_STORAGE_LOCAL_SURFACE_OBJECTS
                    )

                    def constructor_tracer(frame, event, _argument):
                        caller = frame.f_back
                        if (
                            not fired[0]
                            and frame.f_code.co_filename == rpc.__file__
                            and frame.f_code.co_name == "__init__"
                            and event == "call"
                            and caller is not None
                            and caller.f_code.co_filename == storage.__file__
                            and caller.f_code.co_name == builder_name
                        ):
                            fired[0] = True
                            raise control
                        return constructor_tracer

                    try:
                        if builder_name == "_bound_source_drift":
                            fixture = _Task4bOfflineCapabilityFixture()
                            capability = fixture.mint()
                            view = storage.consume_production_historical_window_capability(
                                capability=capability
                            )
                            fixture.capability = None

                            def replacement_next(source):
                                return original_source_next(source)

                            replacement_exported = list(original_exported)
                            replacement_exported[4] = replacement_next
                            storage._HistoricalWindowCaptureReplaySource.__next__ = (
                                replacement_next
                            )
                            storage._TASK4B_STORAGE_LOCAL_SURFACE_OBJECTS = tuple(
                                replacement_exported
                            )
                            sys.settrace(constructor_tracer)
                            try:
                                view._materialize_staging_snapshot_from_bound_scan()
                            except BaseException as error:
                                escaped = error
                        elif builder_name == "_task4b_bound_error":
                            fixture = _Task4bOfflineCapabilityFixture()
                            capability = fixture.mint()
                            with mock.patch.object(
                                storage.os,
                                "pwrite",
                                side_effect=OSError(
                                    "slice6 bound builder ordinary trigger"
                                ),
                            ):
                                sys.settrace(constructor_tracer)
                                try:
                                    scan._materialize_historical_window_staging_snapshot(
                                        capability=capability
                                    )
                                except BaseException as error:
                                    escaped = error
                            fixture.capability = None
                        else:
                            fixture, snapshot = _slice6_materialize_snapshot()
                            _projection, inventory = _slice6_snapshot_inventory(
                                snapshot
                            )
                            raw = inventory["raw_chunks"][0]
                            with mock.patch.object(
                                storage.os,
                                "pread",
                                side_effect=OSError(
                                    "slice6 snapshot builder ordinary trigger"
                                ),
                            ):
                                sys.settrace(constructor_tracer)
                                try:
                                    snapshot.read_frozen_member(
                                        raw["path"],
                                        expected_sha256=raw["sha256"],
                                        max_bytes=16_777_216,
                                    )
                                except BaseException as error:
                                    escaped = error
                    finally:
                        sys.settrace(prior_trace)
                        storage._HistoricalWindowCaptureReplaySource.__next__ = (
                            original_source_next
                        )
                        storage._TASK4B_STORAGE_LOCAL_SURFACE_OBJECTS = (
                            original_exported
                        )
                    if fixture is not None:
                        remaining = tuple(fixture.data_dir.iterdir())
                    fired_observed = fired[0]
                    identity_observed = escaped is control
                    context_observed = control.__context__
                    try:
                        if snapshot is not None:
                            snapshot.close()
                        if view is not None:
                            view.close()
                    finally:
                        if fixture is not None:
                            fixture.close()
                    self.assertTrue(fired_observed)
                    self.assertTrue(identity_observed)
                    self.assertIsNone(context_observed)
                    self.assertEqual(remaining, ())

        for control_class in controls:
            with self.subTest(
                error_builder="_bound_source_drift-active-except",
                control=control_class.__name__,
            ):
                fixture = _Task4bOfflineCapabilityFixture()
                capability = fixture.mint()
                original_stat = storage.os.stat
                config_stats = [0]
                fired = [False]
                escaped = None
                control = control_class(
                    "slice6 active drift constructor "
                    + control_class.__name__
                )
                prior_trace = sys.gettrace()

                class DriftedConfigStat:
                    def __init__(self, value):
                        self._value = value
                        self.st_ino = value.st_ino + 1

                    def __getattr__(self, name):
                        return getattr(self._value, name)

                def drifting_stat(path, *args, **kwargs):
                    value = original_stat(path, *args, **kwargs)
                    if path == "config" and type(kwargs.get("dir_fd")) is int:
                        config_stats[0] += 1
                        if config_stats[0] == 3:
                            return DriftedConfigStat(value)
                    return value

                def active_drift_tracer(frame, event, _argument):
                    caller = frame.f_back
                    if (
                        not fired[0]
                        and frame.f_code.co_filename == rpc.__file__
                        and frame.f_code.co_name == "__init__"
                        and event == "call"
                        and caller is not None
                        and caller.f_code.co_filename == storage.__file__
                        and caller.f_code.co_name == "_bound_source_drift"
                    ):
                        fired[0] = True
                        raise control
                    return active_drift_tracer

                try:
                    with mock.patch.object(
                        storage.os, "stat", side_effect=drifting_stat
                    ) as patched_stat:
                        dir_fd = set(storage.os.supports_dir_fd)
                        dir_fd.discard(original_stat)
                        dir_fd.add(patched_stat)
                        nofollow = set(storage.os.supports_follow_symlinks)
                        nofollow.discard(original_stat)
                        nofollow.add(patched_stat)
                        with mock.patch.object(
                            storage.os, "supports_dir_fd", dir_fd
                        ), mock.patch.object(
                            storage.os,
                            "supports_follow_symlinks",
                            nofollow,
                        ):
                            sys.settrace(active_drift_tracer)
                            try:
                                scan._materialize_historical_window_staging_snapshot(
                                    capability=capability
                                )
                            except BaseException as error:
                                escaped = error
                finally:
                    sys.settrace(prior_trace)
                    fixture.capability = None
                context_observed = control.__context__
                remaining = tuple(fixture.data_dir.iterdir())
                fixture.close()
                self.assertGreaterEqual(config_stats[0], 3)
                self.assertTrue(fired[0])
                self.assertIs(escaped, control)
                self.assertIsNone(context_observed)
                self.assertEqual(remaining, ())

        for control_class in controls:
            with self.subTest(
                error_builder="_task4b_bound_error-active-except",
                control=control_class.__name__,
            ):
                fixture = _Task4bOfflineCapabilityFixture()
                capability = fixture.mint()
                control = control_class(
                    "slice6 active bound constructor "
                    + control_class.__name__
                )
                mutated = [False]
                fired = [False]
                escaped = None
                prior_trace = sys.gettrace()

                def active_bound_tracer(frame, event, _argument):
                    if (
                        not mutated[0]
                        and frame.f_code.co_filename == storage.__file__
                        and frame.f_code.co_name
                        == "_materialize_task4b_capture_core"
                        and event == "line"
                        and "task4b_rows" in frame.f_locals
                        and "binder" in frame.f_locals
                        and "finalization" not in frame.f_locals
                    ):
                        mutated[0] = True
                        frame.f_locals["owner"]["claimed_finalization"] = None
                    caller = frame.f_back
                    if (
                        not fired[0]
                        and frame.f_code.co_filename == rpc.__file__
                        and frame.f_code.co_name == "__init__"
                        and event == "call"
                        and caller is not None
                        and caller.f_code.co_filename == storage.__file__
                        and caller.f_code.co_name == "_task4b_bound_error"
                    ):
                        fired[0] = True
                        raise control
                    return active_bound_tracer

                try:
                    sys.settrace(active_bound_tracer)
                    try:
                        scan._materialize_historical_window_staging_snapshot(
                            capability=capability
                        )
                    except BaseException as error:
                        escaped = error
                finally:
                    sys.settrace(prior_trace)
                    fixture.capability = None
                context_observed = control.__context__
                remaining = tuple(fixture.data_dir.iterdir())
                fixture.close()
                self.assertTrue(mutated[0])
                self.assertTrue(fired[0])
                self.assertIs(escaped, control)
                self.assertIsNone(context_observed)
                self.assertEqual(remaining, ())

        for control_class in controls:
            with self.subTest(
                surface="invalid-read-constructor",
                control=control_class.__name__,
            ):
                fixture, snapshot = _slice6_materialize_snapshot()
                control = control_class(
                    "slice6 invalid read constructor "
                    + control_class.__name__
                )
                fired = [False]
                escaped = None
                prior_trace = sys.gettrace()

                def invalid_read_tracer(frame, event, _argument):
                    caller = frame.f_back
                    if (
                        not fired[0]
                        and frame.f_code.co_filename == rpc.__file__
                        and frame.f_code.co_name == "__init__"
                        and event == "call"
                        and caller is not None
                        and caller.f_code.co_filename == storage.__file__
                        and caller.f_code.co_name == "_task4b_snapshot_error"
                    ):
                        fired[0] = True
                        raise control
                    return invalid_read_tracer

                try:
                    sys.settrace(invalid_read_tracer)
                    try:
                        snapshot.read_frozen_member(
                            "../not-in-inventory",
                            expected_sha256="0" * 64,
                            max_bytes=1,
                        )
                    except BaseException as error:
                        escaped = error
                finally:
                    sys.settrace(prior_trace)
                context_observed = control.__context__
                remaining_before_manual_close = tuple(
                    fixture.data_dir.iterdir()
                )
                terminal_error = None
                try:
                    snapshot.frozen_identity_projection()
                except BaseException as error:
                    terminal_error = error
                try:
                    snapshot.close()
                    snapshot.close()
                finally:
                    fixture.close()
                self.assertTrue(fired[0])
                self.assertIs(escaped, control)
                self.assertIsNone(context_observed)
                self.assertIs(
                    type(terminal_error),
                    storage.HistoricalFoundryStorageError,
                )
                self.assertIsNone(terminal_error.__cause__)
                self.assertIsNone(terminal_error.__context__)
                self.assertEqual(remaining_before_manual_close, ())

        for constructor_control in (None,) + controls:
            label = (
                "ordinary"
                if constructor_control is None
                else constructor_control.__name__
            )
            with self.subTest(
                surface="projection-ack-active-except",
                control=label,
            ):
                fixture = _Task4bOfflineCapabilityFixture()
                capability = fixture.mint()
                snapshot_holder = [None]
                control = (
                    None
                    if constructor_control is None
                    else constructor_control(
                        "slice6 projection ack constructor " + label
                    )
                )
                mutated = [False]
                fired = [False]
                escaped = None
                prior_trace = sys.gettrace()

                def active_projection_tracer(frame, event, _argument):
                    if (
                        frame.f_code.co_filename == storage.__file__
                        and frame.f_code.co_name == "_prepare_handle"
                        and event == "return"
                        and type(_argument)
                        is storage.HistoricalRunStagingSnapshot
                    ):
                        snapshot_holder[0] = _argument
                    if (
                        not mutated[0]
                        and frame.f_code.co_filename == storage.__file__
                        and frame.f_code.co_name
                        == "_task4b_acknowledge_snapshot_delivery"
                        and event == "line"
                    ):
                        mutated[0] = True
                        frame.f_locals["owner"][
                            "_task4b_delivery_guard_phase"
                        ] = "drifted"
                    caller = frame.f_back
                    if (
                        control is not None
                        and not fired[0]
                        and frame.f_code.co_filename == rpc.__file__
                        and frame.f_code.co_name == "__init__"
                        and event == "call"
                        and caller is not None
                        and caller.f_code.co_filename == storage.__file__
                        and caller.f_code.co_name == "_task4b_snapshot_error"
                    ):
                        fired[0] = True
                        raise control
                    return active_projection_tracer

                try:
                    sys.settrace(active_projection_tracer)
                    try:
                        scan._materialize_historical_window_staging_snapshot(
                            capability=capability
                        )
                    except BaseException as error:
                        escaped = error
                finally:
                    sys.settrace(prior_trace)
                    fixture.capability = None
                remaining = tuple(fixture.data_dir.iterdir())
                context_observed = escaped.__context__
                snapshot = snapshot_holder[0]
                terminal_error = None
                try:
                    snapshot.frozen_identity_projection()
                except BaseException as error:
                    terminal_error = error
                try:
                    snapshot.close()
                    snapshot.close()
                finally:
                    fixture.close()
                self.assertTrue(mutated[0])
                if control is None:
                    self.assertIs(type(escaped), rpc._ArchiveRpcError)
                    self.assertEqual(
                        (escaped.reason_code, escaped.failure_kind),
                        ("authority_mismatch", "final_identity_drift"),
                    )
                else:
                    self.assertTrue(fired[0])
                    self.assertIs(escaped, control)
                self.assertIsNone(escaped.__cause__)
                self.assertIsNone(context_observed)
                self.assertIs(
                    type(terminal_error),
                    storage.HistoricalFoundryStorageError,
                )
                self.assertIsNone(terminal_error.__cause__)
                self.assertIsNone(terminal_error.__context__)
                self.assertEqual(remaining, ())

        projection_method = (
            storage.HistoricalRunStagingSnapshot.frozen_identity_projection
        )
        projection_lines, projection_start = inspect.getsourcelines(
            projection_method
        )
        projection_return_line = projection_start + next(
            index for index, line in enumerate(projection_lines)
            if "return dict(projection)" in line
        )
        for control_class in controls:
            with self.subTest(
                surface="projection-return",
                control=control_class.__name__,
            ):
                fixture, snapshot = _slice6_materialize_snapshot()
                reference = weakref.ref(snapshot)
                control = control_class(
                    "slice6 projection return " + control_class.__name__
                )
                fired = [False]
                escaped = None
                prior_trace = sys.gettrace()

                def projection_tracer(frame, event, _argument):
                    if (
                        not fired[0]
                        and frame.f_code is projection_method.__code__
                        and event == "line"
                        and frame.f_lineno == projection_return_line
                    ):
                        fired[0] = True
                        raise control
                    return projection_tracer

                try:
                    sys.settrace(projection_tracer)
                    try:
                        snapshot.frozen_identity_projection()
                    except BaseException as error:
                        escaped = error
                finally:
                    sys.settrace(prior_trace)
                remaining_before_manual_close = tuple(
                    fixture.data_dir.iterdir()
                )
                fired_observed = fired[0]
                identity_observed = escaped is control
                context_observed = control.__context__
                terminal_error = None
                try:
                    snapshot.frozen_identity_projection()
                except BaseException as error:
                    terminal_error = error
                terminal_error_type = type(terminal_error)
                terminal_error_cause = terminal_error.__cause__
                terminal_error_context = terminal_error.__context__
                self.assertIsNone(snapshot.close())
                self.assertIsNone(snapshot.close())
                snapshot = None
                escaped = None
                control = None
                terminal_error = None
                gc.collect()
                reference_cleared = reference() is None
                fixture.close()
                self.assertTrue(fired_observed)
                self.assertTrue(identity_observed)
                self.assertIsNone(context_observed)
                self.assertIs(
                    terminal_error_type,
                    storage.HistoricalFoundryStorageError,
                )
                self.assertIsNone(terminal_error_cause)
                self.assertIsNone(terminal_error_context)
                self.assertEqual(remaining_before_manual_close, ())
                self.assertTrue(reference_cleared)

        for delivery in ("pre-assignment", "post-assignment"):
            for control_class in controls:
                with self.subTest(
                    delivery=delivery, control=control_class.__name__
                ):
                    fixture = _Task4bOfflineCapabilityFixture()
                    control = control_class(
                        "slice6 {} delivery".format(delivery)
                    )
                    reference = [None]
                    fired = [False]
                    escaped = None
                    result = None
                    prior_trace = sys.gettrace()
                    opcode_cache = {}

                    def instruction(frame):
                        rows = opcode_cache.get(frame.f_code)
                        if rows is None:
                            rows = {
                                row.offset: (row.opname, row.argval)
                                for row in dis.get_instructions(frame.f_code)
                            }
                            opcode_cache[frame.f_code] = rows
                        return rows.get(frame.f_lasti)

                    def delivery_tracer(frame, event, argument):
                        if (
                            frame.f_code.co_filename == storage.__file__
                            and frame.f_code.co_name == "_prepare_handle"
                            and event == "return"
                            and type(argument)
                            is storage.HistoricalRunStagingSnapshot
                        ):
                            reference[0] = weakref.ref(argument)
                        if (
                            frame.f_code.co_filename == scan.__file__
                            and frame.f_code.co_name
                            == "_materialize_historical_window_staging_snapshot"
                        ):
                            if delivery == "pre-assignment":
                                frame.f_trace_opcodes = True
                                if (
                                    not fired[0]
                                    and reference[0] is not None
                                    and event == "opcode"
                                    and instruction(frame)
                                    == ("STORE_FAST", "snapshot")
                                ):
                                    fired[0] = True
                                    raise control
                            elif not fired[0] and event == "line":
                                candidate = frame.f_locals.get("snapshot")
                                if type(candidate) is (
                                    storage.HistoricalRunStagingSnapshot
                                ):
                                    fired[0] = True
                                    raise control
                        return delivery_tracer

                    try:
                        capability = fixture.mint()
                        sys.settrace(delivery_tracer)
                        try:
                            result = scan._materialize_historical_window_staging_snapshot(
                                capability=capability
                            )
                        except BaseException as error:
                            escaped = error
                    finally:
                        sys.settrace(prior_trace)
                        fixture.capability = None
                    fired_observed = fired[0]
                    no_result_observed = result is None
                    identity_observed = escaped is control
                    context_observed = control.__context__
                    escaped = None
                    control = None
                    result = None
                    gc.collect()
                    leaked = (
                        reference[0]() if reference[0] is not None else None
                    )
                    remaining = tuple(fixture.data_dir.iterdir())
                    leaked_live_snapshot = leaked is not None
                    if leaked is not None:
                        leaked.close()
                    leaked = None
                    fixture.close()
                    gc.collect()
                    reference_cleared = (
                        reference[0] is None or reference[0]() is None
                    )
                    self.assertTrue(identity_observed)
                    self.assertIsNone(context_observed)
                    self.assertTrue(fired_observed)
                    self.assertTrue(no_result_observed)
                    self.assertFalse(leaked_live_snapshot)
                    self.assertTrue(reference_cleared)
                    self.assertEqual(remaining, ())


class HistoricalFoundryStorageTask4bMaximumIntegrationTests(
    unittest.TestCase
):
    _SMALL_COMMON_GOLDEN = (
        (
            "policy.json", 1621,
            "0f8f604a6c8087ce9e44ac6de4e81b71c65657d4c2dc05f862fda3306e2ba1f8",
        ),
        (
            "authority.json", 2220,
            "6156c67cedb03dbf21c86028553445118fe41a1732e8da40ac961060a457cd59",
        ),
        (
            "toolchain.json", 2435,
            "9af543d0b7744d2552d4d65b0bf6c5b9039bcfe65d4296bd82d1c736a577c42a",
        ),
        (
            "rpc/00000001.bin", 25920,
            "cbe96f72881dea3f1c27fc683c8108c799af49e09cccca83ca902a4cb461e102",
        ),
        (
            "headers/00000001.json.gz", 451,
            "c912d8a43f95a5a6f028d6d193c257a76da7d9f7d6cc6f5f71a56fef116108da",
        ),
        (
            "reserves/00000001.json.gz", 612,
            "95915782f446c1f7daa2bc832646c3fdc82a58c3b74424c8156d29b7460af01a",
        ),
        (
            "prices/00000001.json.gz", 542,
            "fba4e538297390643bbd09ade08e635e69259050f07ff641f9783f88bea58030",
        ),
        (
            "fees/00000001.json.gz", 268,
            "3d221de2ad977645a086c54ebd8b1f2e9363bc2c5b5ae571fe54030098bc8330",
        ),
        "8d726d93def20a52e4c6f4c3ec94fa18078ba058d13bdd777b1bbc9af121d6d9",
    )

    @staticmethod
    def _canonical(value):
        return json.dumps(
            value,
            sort_keys=True,
            separators=(",", ":"),
            ensure_ascii=True,
        ).encode("utf-8")

    @staticmethod
    def _live_source_envelope_ids(frame):
        jsonrpc_ids = set()
        lower_ids = set()
        active = frame
        while active is not None:
            for value in active.f_locals.values():
                candidates = (
                    value if type(value) in (list, tuple) else (value,)
                )
                for candidate in candidates:
                    if type(candidate) is not dict:
                        continue
                    keys = set(candidate)
                    if keys == {"jsonrpc", "id", "result"}:
                        jsonrpc_ids.add(id(candidate))
                    elif (
                        keys == {"request", "response"}
                        and type(candidate.get("request")) is dict
                        and type(candidate.get("response")) is dict
                    ):
                        lower_ids.add(id(candidate))
            active = active.f_back
        return jsonrpc_ids, lower_ids

    @classmethod
    def _observe_source_buffers(
        cls, *, frame, event, argument, observed, rpc, scan
    ):
        filename = frame.f_code.co_filename
        name = frame.f_code.co_name
        if (
            filename == rpc.__file__
            and name == "project_historical_anchor_capture"
        ):
            if event == "call":
                responses = frame.f_locals.get("responses")
                observed["anchor_raw_counts"].append(
                    len(responses) if type(responses) is tuple else None
                )
            elif event == "return":
                inventory = (
                    argument.get("request_inventory")
                    if type(argument) is dict else None
                )
                observed["anchor_inventory_counts"].append(
                    len(inventory) if type(inventory) is list else None
                )
        elif (
            filename == scan.__file__
            and name == "project_historical_lower_bound_capture"
        ):
            if event == "call":
                jsonrpc_ids, _lower_ids = cls._live_source_envelope_ids(
                    frame
                )
                observed["anchor_raw_at_lower"].append(len(jsonrpc_ids))
            elif event == "return":
                request_ids = (
                    argument.get("request_ids")
                    if type(argument) is dict else None
                )
                observed["lower_n"] = (
                    len(request_ids) if type(request_ids) is tuple else None
                )
        elif (
            filename == scan.__file__
            and name == "_project_lower_observation"
            and event == "call"
        ):
            _jsonrpc_ids, lower_ids = cls._live_source_envelope_ids(frame)
            projector = frame.f_back
            while projector is not None and not (
                projector.f_code.co_filename == scan.__file__
                and projector.f_code.co_name
                == "project_historical_lower_bound_capture"
            ):
                projector = projector.f_back
            probes = (
                projector.f_locals.get("compact_probes")
                if projector is not None else None
            )
            witness = (
                projector.f_locals.get("compact_witness")
                if projector is not None else None
            )
            compact_count = (
                (len(probes) if type(probes) is list else 0)
                + (len(witness) if type(witness) is list else 0)
                if projector is not None else None
            )
            observed["lower_metrics"].append(
                (len(lower_ids), compact_count)
            )
        elif (
            filename == scan.__file__
            and name == "_project_complete_historical_window_root"
            and event == "call"
        ):
            responses = frame.f_locals.get("responses")
            count = len(responses) if type(responses) is tuple else None
            jsonrpc_ids, lower_ids = cls._live_source_envelope_ids(frame)
            observed["window_counts"].append(count)
            observed["window_source_counts"].append(len(jsonrpc_ids))
            observed["window_lower_counts"].append(len(lower_ids))

    def _assert_source_buffers(self, observed):
        self.assertEqual(observed["anchor_raw_counts"], [48])
        self.assertEqual(observed["anchor_inventory_counts"], [48])
        self.assertEqual(observed["anchor_raw_at_lower"], [0])
        lower_n = observed["lower_n"]
        self.assertIs(type(lower_n), int)
        self.assertTrue(1 <= lower_n <= 66)
        self.assertEqual(len(observed["lower_metrics"]), lower_n)
        self.assertTrue(all(
            raw_count >= 1
            and type(compact_count) is int
            and compact_count >= 0
            and 1 <= raw_count + compact_count <= lower_n
            for raw_count, compact_count in observed["lower_metrics"]
        ))
        self.assertTrue(observed["window_counts"])
        self.assertTrue(all(
            type(count) is int and 1 <= count <= 40
            for count in observed["window_counts"]
        ))
        self.assertEqual(
            observed["window_source_counts"], observed["window_counts"]
        )
        self.assertEqual(
            observed["window_lower_counts"],
            [0] * len(observed["window_counts"]),
        )

    def _materialize(self, *, context_factory, split_reserve_root, tracer=None,
                     record_calls=True):
        from tests.test_historical_foundry_scan import (
            _Task4bOfflineCapabilityFixture,
        )

        scan = importlib.import_module("scripts.historical_foundry_scan")
        fixture = _Task4bOfflineCapabilityFixture(
            context_factory=context_factory,
            split_reserve_root=split_reserve_root,
            record_calls=record_calls,
        )
        snapshot = None
        prior_trace = sys.gettrace()
        try:
            capability = fixture.mint()
            if tracer is not None:
                sys.settrace(tracer)
            snapshot = scan._materialize_historical_window_staging_snapshot(
                capability=capability
            )
            fixture.capability = None
            return fixture, snapshot
        except BaseException:
            if snapshot is not None:
                try:
                    snapshot.close()
                except BaseException:
                    pass
            fixture.close()
            raise
        finally:
            sys.settrace(prior_trace)

    def _snapshot_evidence(self, snapshot, *, retain_typed_bytes):
        from tests.test_historical_foundry_scan import (
            _task4b_executing_python_identity,
        )

        projection = dict(snapshot.frozen_identity_projection())
        inventory_bytes = snapshot.read_frozen_member(
            "scan/capture_inventory.json",
            expected_sha256=projection["capture_inventory_sha256"],
            max_bytes=16_777_216,
        )
        inventory = json.loads(inventory_bytes.decode("utf-8"))
        self.assertEqual(
            inventory["source_identity"]["python"],
            _task4b_executing_python_identity(),
        )
        common = []
        typed_bytes = []
        member_groups = (
            (inventory["configs"], "byte_count", "sha256", 1_048_576),
            (inventory["raw_chunks"], "byte_count", "sha256", 16_777_216),
            (
                inventory["typed_chunks"],
                "gzip_byte_count", "gzip_sha256", 16_842_752,
            ),
        )
        for rows, size_key, digest_key, maximum in member_groups:
            for row in rows:
                payload = snapshot.read_frozen_member(
                    row["path"],
                    expected_sha256=row[digest_key],
                    max_bytes=maximum,
                )
                self.assertEqual(len(payload), row[size_key])
                self.assertEqual(
                    hashlib.sha256(payload).hexdigest(), row[digest_key]
                )
                common.append((
                    row["path"], row[size_key], row[digest_key],
                ))
                if retain_typed_bytes and digest_key == "gzip_sha256":
                    typed_bytes.append((row["path"], payload))
                del payload
        source_identity = inventory["source_identity"]
        common_source_identity = {
            key: value for key, value in source_identity.items()
            if key != "python"
        }
        inventory["source_identity"] = common_source_identity
        try:
            common.append(hashlib.sha256(
                self._canonical(inventory)
            ).hexdigest())
        finally:
            inventory["source_identity"] = source_identity
        serialized_projection = self._canonical(projection)
        for forbidden in (b"z" * 32, ("7a" * 32).encode("ascii")):
            self.assertNotIn(forbidden, inventory_bytes)
            self.assertNotIn(forbidden, serialized_projection)
        return {
            "projection": projection,
            "inventory": inventory,
            "inventory_bytes": inventory_bytes,
            "common": tuple(common),
            "typed_bytes": tuple(typed_bytes),
        }

    def _small_materialization(self, *, split_reserve_root):
        from tests.test_historical_foundry_scan import _three_block_context

        fixture = None
        snapshot = None
        try:
            fixture, snapshot = self._materialize(
                context_factory=_three_block_context,
                split_reserve_root=split_reserve_root,
            )
            evidence = self._snapshot_evidence(
                snapshot, retain_typed_bytes=True
            )
            evidence["context_issuance_assertions"] = (
                fixture.context_issuance_assertions
            )
            evidence["reserve_attempts"] = tuple(fixture.reserve_attempts)
            evidence["transport_call_count"] = fixture.transport_call_count
            self.assertIsNone(snapshot.close())
            self.assertIsNone(snapshot.close())
            snapshot = None
            self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
            return evidence
        finally:
            if snapshot is not None:
                try:
                    snapshot.close()
                except BaseException:
                    pass
            if fixture is not None:
                fixture.close()

    def maximum_window_materializes_once(self):
        from tests.test_historical_foundry_scan import (
            _maximum_task4b_context,
        )

        rpc = importlib.import_module("scripts.historical_foundry_rpc")
        scan = importlib.import_module("scripts.historical_foundry_scan")
        storage = importlib.import_module(
            "scripts.historical_foundry_storage"
        )
        observed = {
            "phase": None,
            "phase_overlaps": [],
            "anchor_raw_counts": [],
            "anchor_inventory_counts": [],
            "anchor_raw_at_lower": [],
            "lower_n": None,
            "lower_metrics": [],
            "window_counts": [],
            "window_source_counts": [],
            "window_lower_counts": [],
            "source_frames": 0,
            "outstanding_frames": 0,
            "max_outstanding_frames": 0,
            "frame_violations": [],
            "header_inventory_ids": set(),
            "header_row_tuple_ids": set(),
            "header_inventory_rows": [],
            "typed_builder_ids": set(),
            "typed_builder_peak_rows": 0,
            "typed_builder_peak_bytes": 0,
            "append_calls": 0,
            "append_returns": 0,
            "append_violations": [],
            "planner_calls": 0,
            "planner_returns": 0,
            "planner_violations": [],
            "snapshot_allocations": 0,
        }

        def enter_phase(label):
            if observed["phase"] is not None:
                observed["phase_overlaps"].append(
                    (observed["phase"], label)
                )
            observed["phase"] = label

        def leave_phase(label):
            if observed["phase"] != label:
                observed["phase_overlaps"].append(
                    (observed["phase"], label + "-return")
                )
            observed["phase"] = None

        def tracer(frame, event, argument):
            filename = frame.f_code.co_filename
            name = frame.f_code.co_name
            self._observe_source_buffers(
                frame=frame,
                event=event,
                argument=argument,
                observed=observed,
                rpc=rpc,
                scan=scan,
            )
            if (
                filename == rpc.__file__
                and name == "project_historical_anchor_capture"
            ):
                if event == "call":
                    enter_phase("anchor")
                elif event == "return":
                    leave_phase("anchor")
            elif (
                filename == scan.__file__
                and name == "project_historical_lower_bound_capture"
            ):
                if event == "call":
                    enter_phase("lower")
                elif event == "return":
                    leave_phase("lower")
            elif (
                filename == scan.__file__
                and name == "_project_complete_historical_window_root"
            ):
                if event == "call":
                    enter_phase("window")
                elif event == "return":
                    leave_phase("window")
            if (
                filename == scan.__file__
                and event == "return"
                and type(argument) is dict
                and argument.get("schema")
                == "historical_foundry_header_inventory/v1"
            ):
                rows = argument.get("rows")
                observed["header_inventory_ids"].add(id(argument))
                if type(rows) is tuple:
                    observed["header_row_tuple_ids"].add(id(rows))
                    observed["header_inventory_rows"].append(len(rows))
            if (
                filename == storage.__file__
                and name == "__next__"
                and event == "return"
                and type(argument) is tuple
                and len(argument) == 3
                and type(argument[0]) is dict
                and type(argument[1]) is bytes
                and type(argument[2]) is bytes
            ):
                if observed["outstanding_frames"] != 0:
                    observed["frame_violations"].append("overlap")
                if observed["phase"] is not None:
                    observed["frame_violations"].append("phase-overlap")
                observed["source_frames"] += 1
                observed["outstanding_frames"] += 1
                observed["max_outstanding_frames"] = max(
                    observed["max_outstanding_frames"],
                    observed["outstanding_frames"],
                )
            if (
                filename == scan.__file__
                and name
                == "_consume_production_historical_window_capture_replay_event_for_storage"
                and event == "return"
                and type(argument) is tuple
                and argument
                and argument[0] == "exchange"
            ):
                if observed["outstanding_frames"] != 1:
                    observed["frame_violations"].append("consume")
                else:
                    observed["outstanding_frames"] = 0
            if filename == storage.__file__:
                record = frame.f_locals.get("record")
                builder = (
                    record.get("typed_builder")
                    if type(record) is dict else None
                )
                if type(builder) is dict:
                    observed["typed_builder_ids"].add(id(builder))
                    row_bytes = builder.get("row_bytes")
                    if type(row_bytes) is list:
                        observed["typed_builder_peak_rows"] = max(
                            observed["typed_builder_peak_rows"],
                            len(row_bytes),
                        )
                    decoded_size = builder.get("decoded_size")
                    if type(decoded_size) is int:
                        observed["typed_builder_peak_bytes"] = max(
                            observed["typed_builder_peak_bytes"],
                            decoded_size,
                        )
                if name == "_task4b_append_typed_root":
                    if event == "call":
                        observed["append_calls"] += 1
                    elif event == "return":
                        observed["append_returns"] += 1
                        row_count = frame.f_locals.get("row_count")
                        if (
                            type(argument) is not tuple
                            or len(argument) != 2
                            or type(argument[0]) is not list
                            or len(argument[0]) != 1
                            or argument[0][0].get("row_count") != row_count
                            or type(argument[1]) is not list
                            or len(argument[1]) != row_count
                        ):
                            observed["append_violations"].append(row_count)
                elif name == "_plan_historical_typed_root_append":
                    if event == "call":
                        observed["planner_calls"] += 1
                        lengths = frame.f_locals.get(
                            "candidate_row_encoded_lengths"
                        )
                        if type(lengths) is tuple and lengths:
                            standalone = 2 + sum(lengths) + len(lengths) - 1
                            if standalone > 16_777_216:
                                observed["planner_violations"].append(
                                    ("standalone", standalone)
                                )
                    elif event == "return":
                        observed["planner_returns"] += 1
                        if (
                            type(argument) is not tuple
                            or len(argument) != 2
                            or argument[0] not in (
                                "append_current", "flush_then_append"
                            )
                            or type(argument[1]) is not int
                            or argument[1] > 16_777_216
                        ):
                            observed["planner_violations"].append(argument)
                elif (
                    name == "_prepare_handle"
                    and event == "return"
                    and type(argument)
                    is storage.HistoricalRunStagingSnapshot
                ):
                    observed["snapshot_allocations"] += 1
            return tracer

        fixture = None
        snapshot = None
        try:
            fixture, snapshot = self._materialize(
                context_factory=_maximum_task4b_context,
                split_reserve_root=False,
                tracer=tracer,
                record_calls=False,
            )
            released_ids = (
                observed["header_inventory_ids"]
                | observed["header_row_tuple_ids"]
                | observed["typed_builder_ids"]
            )
            gc.collect()
            self.assertFalse(any(
                id(value) in released_ids for value in gc.get_objects()
            ))
            evidence = self._snapshot_evidence(
                snapshot, retain_typed_bytes=False
            )
            inventory = evidence["inventory"]
            self.assertEqual(inventory["range"]["block_count"], 50_401)
            typed_counts = {
                role: sum(
                    row["row_count"] for row in inventory["typed_chunks"]
                    if row["role"] == role
                )
                for role in ("headers", "reserves", "prices", "fees")
            }
            self.assertEqual(typed_counts, {
                "headers": 50_401,
                "reserves": 100_802,
                "prices": 50_401,
                "fees": 50_401,
            })
            root_counts = {
                kind: sum(
                    1 for row in inventory["post_roots"]
                    if row.get("segment") == "window_root"
                    and row.get("kind") == kind
                )
                for kind in (
                    "header", "reserve", "price", "fee_history",
                    "final_anchor",
                )
            }
            self.assertEqual(root_counts, {
                "header": 1_261,
                "reserve": 2_521,
                "price": 1_261,
                "fee_history": 50,
                "final_anchor": 1,
            })
            typed_root_count = sum(root_counts[kind] for kind in (
                "header", "reserve", "price", "fee_history"
            ))
            self.assertEqual(observed["append_calls"], typed_root_count)
            self.assertEqual(observed["append_returns"], typed_root_count)
            self.assertEqual(observed["planner_calls"], typed_root_count)
            self.assertEqual(observed["planner_returns"], typed_root_count)
            self.assertEqual(observed["append_violations"], [])
            self.assertEqual(observed["planner_violations"], [])
            self.assertEqual(observed["typed_builder_ids"].__len__(), 1)
            self.assertLessEqual(
                observed["typed_builder_peak_bytes"], 16_777_216
            )
            self.assertGreater(observed["typed_builder_peak_rows"], 0)
            self._assert_source_buffers(observed)
            self.assertEqual(
                len(observed["window_counts"]), sum(root_counts.values())
            )
            self.assertIsNone(observed["phase"])
            self.assertEqual(observed["phase_overlaps"], [])
            self.assertEqual(observed["frame_violations"], [])
            self.assertEqual(observed["outstanding_frames"], 0)
            self.assertEqual(observed["max_outstanding_frames"], 1)
            self.assertEqual(
                observed["source_frames"], len(inventory["exchanges"])
            )
            self.assertEqual(observed["snapshot_allocations"], 1)
            self.assertEqual(observed["header_inventory_ids"].__len__(), 1)
            self.assertEqual(observed["header_row_tuple_ids"].__len__(), 1)
            self.assertTrue(observed["header_inventory_rows"])
            self.assertEqual(set(observed["header_inventory_rows"]), {50_401})
            self.assertEqual(fixture.context_issuance_assertions, 1)
            self.assertEqual(fixture.calls, [])
            self.assertLessEqual(fixture.response_seed_count, 114)
            self.assertEqual(
                fixture.transport_call_count, len(inventory["exchanges"])
            )
            self.assertEqual(
                fixture.transport_request_count,
                inventory["request_range"]["request_count"],
            )
            self.assertEqual(evidence["projection"]["generation"], 1)
            self.assertIsNone(snapshot.reread_frozen_members_unchanged())
            self.assertIsNone(snapshot.close())
            snapshot = None
            self.assertEqual(tuple(fixture.data_dir.iterdir()), ())
        finally:
            if snapshot is not None:
                try:
                    snapshot.close()
                except BaseException:
                    pass
            if fixture is not None:
                fixture.close()

    def test_small_split_unsplit_and_repeat_determinism(self):
        split = self._small_materialization(split_reserve_root=True)
        unsplit_a = self._small_materialization(split_reserve_root=False)
        unsplit_b = self._small_materialization(split_reserve_root=False)
        for evidence in (split, unsplit_a, unsplit_b):
            self.assertEqual(evidence["context_issuance_assertions"], 1)
            self.assertEqual(evidence["projection"]["generation"], 1)

        split_inventory = split["inventory"]
        reserve_roots = [
            row for row in split_inventory["post_roots"]
            if row.get("segment") == "window_root"
            and row.get("kind") == "reserve"
        ]
        self.assertEqual(len(reserve_roots), 1)
        reserve_root = reserve_roots[0]
        self.assertEqual(reserve_root["request_count"], 6)
        self.assertEqual(reserve_root["leaf_count"], 2)
        self.assertEqual(
            tuple(
                row["request_count"]
                for row in reserve_root["observed_http_413_intervals"]
            ),
            (6,),
        )
        reserve_leaves = [
            row for row in split_inventory["exchanges"]
            if row["logical_batch_index"]
            == reserve_root["logical_batch_index"]
        ]
        self.assertEqual(
            tuple(tuple(row["request_ids"]) for row in reserve_leaves),
            tuple(split["reserve_attempts"][1:3]),
        )
        self.assertEqual(
            tuple(len(row["request_ids"]) for row in reserve_leaves),
            (3, 3),
        )
        self.assertEqual(
            reserve_leaves[0]["typed_chunk_refs"],
            reserve_leaves[1]["typed_chunk_refs"],
        )
        self.assertEqual(len(reserve_leaves[0]["typed_chunk_refs"]), 1)
        self.assertEqual(
            reserve_leaves[0]["typed_chunk_refs"][0]["row_count"], 6
        )

        unsplit_reserve_roots = [
            row for row in unsplit_a["inventory"]["post_roots"]
            if row.get("segment") == "window_root"
            and row.get("kind") == "reserve"
        ]
        self.assertEqual(len(unsplit_reserve_roots), 1)
        self.assertEqual(unsplit_reserve_roots[0]["leaf_count"], 1)
        self.assertEqual(
            unsplit_reserve_roots[0]["observed_http_413_intervals"], []
        )
        self.assertEqual(len(unsplit_a["reserve_attempts"]), 1)

        def root_role(root):
            if root["segment"] in ("anchor_stage", "lower_observation"):
                return root["segment"]
            return {
                "header": "headers",
                "reserve": "reserves",
                "price": "prices",
                "fee_history": "fees",
                "final_anchor": "final_anchor",
            }[root["kind"]]

        expected_role_counts = {
            "anchor_stage": 3,
            "lower_observation": 4,
            "headers": 1,
            "reserves": 1,
            "prices": 1,
            "fees": 1,
            "final_anchor": 1,
        }
        for evidence in (split, unsplit_a, unsplit_b):
            roots = evidence["inventory"]["post_roots"]
            role_counts = {role: 0 for role in expected_role_counts}
            for root in roots:
                role_counts[root_role(root)] += 1
            self.assertEqual(role_counts, expected_role_counts)
            self.assertEqual(len(roots), sum(expected_role_counts.values()))
            self.assertEqual(
                len({root["logical_batch_index"] for root in roots}),
                len(roots),
            )
        self.assertEqual(split["typed_bytes"], unsplit_a["typed_bytes"])
        self.assertEqual(unsplit_a["typed_bytes"], unsplit_b["typed_bytes"])
        self.assertEqual(
            unsplit_a["inventory_bytes"], unsplit_b["inventory_bytes"]
        )
        self.assertEqual(
            unsplit_a["projection"], unsplit_b["projection"]
        )

    def test_small_dual_runtime_parity_golden(self):
        evidence = self._small_materialization(split_reserve_root=False)
        self.assertEqual(evidence["context_issuance_assertions"], 1)
        self.assertEqual(evidence["common"], self._SMALL_COMMON_GOLDEN)


class HistoricalFoundryScenarioStorageNativeTests(unittest.TestCase):
    def test_task6_member_caps_and_authority_types_are_closed(self):
        import scripts.historical_foundry_storage as storage

        for role, limit in (
            ("overlay", 8_388_608),
            ("receipt", 8_388_608),
            ("result", 8_388_608),
            ("trace", 16_777_216),
        ):
            with self.subTest(role=role):
                self.assertIsNone(
                    storage._validate_historical_scenario_member_size(
                        role=role, byte_count=limit
                    )
                )
                with self.assertRaises(ValueError):
                    storage._validate_historical_scenario_member_size(
                        role=role, byte_count=limit + 1
                    )
        for authority in (
            storage.ScenarioEvidenceSink,
            storage.ValidatedHistoricalReplayLedger,
        ):
            with self.assertRaises((TypeError, RuntimeError)):
                authority()

    def test_task6_directory_commit_is_kernel_noreplace(self):
        import scripts.historical_foundry_storage as storage

        with tempfile.TemporaryDirectory() as directory:
            parent_fd = os.open(directory, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.mkdir("source", 0o700, dir_fd=parent_fd)
                os.mkdir("destination", 0o700, dir_fd=parent_fd)
                source_before = os.stat(
                    "source", dir_fd=parent_fd, follow_symlinks=False
                )
                destination_before = os.stat(
                    "destination", dir_fd=parent_fd, follow_symlinks=False
                )
                with self.assertRaises(FileExistsError):
                    storage._task6_rename_directory_noreplace(
                        parent_fd=parent_fd, source_name="source",
                        destination_name="destination",
                    )
                source_after = os.stat(
                    "source", dir_fd=parent_fd, follow_symlinks=False
                )
                destination_after = os.stat(
                    "destination", dir_fd=parent_fd, follow_symlinks=False
                )
                self.assertEqual(
                    (source_after.st_dev, source_after.st_ino),
                    (source_before.st_dev, source_before.st_ino),
                )
                self.assertEqual(
                    (destination_after.st_dev, destination_after.st_ino),
                    (destination_before.st_dev, destination_before.st_ino),
                )
            finally:
                os.close(parent_fd)


if __name__ == "__main__":
    unittest.main()
