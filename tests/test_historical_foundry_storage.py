from __future__ import annotations

import importlib
import importlib.util
import ast
import asyncio
import copy
import gc
import hashlib
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


if __name__ == "__main__":
    unittest.main()
