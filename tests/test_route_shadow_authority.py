"""Tests for the fail-closed route Shadow authority loader."""

from __future__ import annotations

import hashlib
import inspect
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

from scripts.route_shadow_authority import load_committed_route_shadow_authority
import scripts.route_shadow_authority as route_shadow_authority


_FALSE_AUTHORITY_BYTES = (
    b'{"enabled":false,"schema":"route_shadow_enabled/v1",'
    b'"transaction_id":null}'
)
_TRANSACTION_ID = "a" * 64
_TRUE_AUTHORITY_BYTES = (
    b'{"enabled":true,"schema":"route_shadow_enabled/v1",'
    b'"transaction_id":"' + _TRANSACTION_ID.encode("ascii") + b'"}'
)
_TRANSACTION_FALSE_BYTES = (
    b'{"enabled":false,"schema":"route_shadow_enabled/v1",'
    b'"transaction_id":"' + _TRANSACTION_ID.encode("ascii") + b'"}'
)


def _write_authority(data_dir, payload):
    operational = data_dir / "routes" / "shadow" / "operational"
    operational.mkdir(parents=True)
    path = operational / "enabled.json"
    path.write_bytes(payload)
    return path


class RouteShadowAuthorityTests(unittest.TestCase):
    def test_absent_authority_is_disabled(self):
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory)

            self.assertEqual(
                load_committed_route_shadow_authority(data_dir),
                {
                    "schema": "route_shadow_authority_view/v1",
                    "status": "disabled",
                    "transaction_id": None,
                    "authority_sha256": None,
                    "primary_unit_projection_sha256": None,
                    "reason_code": None,
                },
            )

    def test_canonical_genesis_false_is_disabled_without_live_probe(self):
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory)
            _write_authority(data_dir, _FALSE_AUTHORITY_BYTES)
            calls = []

            def must_not_probe():
                calls.append(True)
                raise AssertionError(
                    "feature-off authority must not perform a live probe"
                )

            probe = route_shadow_authority._make_authority_live_probe_for_test(
                must_not_probe
            )

            self.assertEqual(
                route_shadow_authority._load_authority_with_probe(data_dir, probe),
                {
                    "schema": "route_shadow_authority_view/v1",
                    "status": "disabled",
                    "transaction_id": None,
                    "authority_sha256": hashlib.sha256(
                        _FALSE_AUTHORITY_BYTES
                    ).hexdigest(),
                    "primary_unit_projection_sha256": None,
                    "reason_code": None,
                },
            )
            self.assertEqual(calls, [])

    def test_public_loader_has_no_probe_or_configuration_injection(self):
        signature = inspect.signature(load_committed_route_shadow_authority)
        self.assertEqual(list(signature.parameters), ["data_dir"])
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory)
            for keyword in (
                "live_probe",
                "unit",
                "properties",
                "timeout",
                "executable",
                "clock",
            ):
                with self.subTest(keyword=keyword):
                    with self.assertRaises(TypeError):
                        load_committed_route_shadow_authority(
                            data_dir, **{keyword: object()}
                        )
            self.assertEqual(list(data_dir.iterdir()), [])

    def test_sealed_live_probe_has_one_no_argument_operation(self):
        probe = route_shadow_authority._make_authority_live_probe_for_test(
            lambda: {"fixed": "projection"}
        )
        self.assertEqual(list(inspect.signature(probe.sample).parameters), [])
        self.assertEqual(probe.sample(), {"fixed": "projection"})
        with self.assertRaises(TypeError):
            probe.sample("unit-name")
        with self.assertRaises(TypeError):
            class _UnsealedProbe(route_shadow_authority._AuthorityLiveProbe):
                pass

    def test_canonical_true_is_invalid_without_live_probe(self):
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory)
            _write_authority(data_dir, _TRUE_AUTHORITY_BYTES)
            calls = []
            probe = route_shadow_authority._make_authority_live_probe_for_test(
                lambda: calls.append(True)
            )

            self.assertEqual(
                route_shadow_authority._load_authority_with_probe(data_dir, probe),
                {
                    "schema": "route_shadow_authority_view/v1",
                    "status": "invalid",
                    "transaction_id": _TRANSACTION_ID,
                    "authority_sha256": hashlib.sha256(
                        _TRUE_AUTHORITY_BYTES
                    ).hexdigest(),
                    "primary_unit_projection_sha256": None,
                    "reason_code": "enable_contract_not_available",
                },
            )
            self.assertEqual(calls, [])

    def test_transaction_backed_false_and_aborted_like_evidence_are_invalid(self):
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory)
            authority_path = _write_authority(data_dir, _TRANSACTION_FALSE_BYTES)
            transaction = (
                authority_path.parent / "enable-transactions" / _TRANSACTION_ID
            )
            transaction.mkdir(parents=True)
            (transaction / "terminal.json").write_bytes(
                b'{"outcome":"aborted","reason_code":"owned_rollback_complete"}'
            )
            calls = []
            probe = route_shadow_authority._make_authority_live_probe_for_test(
                lambda: calls.append(True)
            )

            self.assertEqual(
                route_shadow_authority._load_authority_with_probe(data_dir, probe),
                {
                    "schema": "route_shadow_authority_view/v1",
                    "status": "invalid",
                    "transaction_id": _TRANSACTION_ID,
                    "authority_sha256": hashlib.sha256(
                        _TRANSACTION_FALSE_BYTES
                    ).hexdigest(),
                    "primary_unit_projection_sha256": None,
                    "reason_code": "enable_contract_not_available",
                },
            )
            self.assertEqual(calls, [])

    def test_malformed_or_noncanonical_authority_is_invalid(self):
        malformed = (
            b"",
            b"not-json",
            b'{"enabled":false,"schema":"route_shadow_enabled/v1"}',
            b'{"enabled":0,"schema":"route_shadow_enabled/v1",'
            b'"transaction_id":null}',
            b'{"enabled":true,"schema":"route_shadow_enabled/v1",'
            b'"transaction_id":null}',
            b'{"enabled":false,"schema":"route_shadow_enabled/v2",'
            b'"transaction_id":null}',
            b'{"enabled":false,"extra":null,'
            b'"schema":"route_shadow_enabled/v1","transaction_id":null}',
            b'{"enabled":false,"enabled":false,'
            b'"schema":"route_shadow_enabled/v1","transaction_id":null}',
            _FALSE_AUTHORITY_BYTES + b"\n",
            b'{"enabled":false, "schema":"route_shadow_enabled/v1",'
            b'"transaction_id":null}',
            b'{"enabled":false,"schema":"route_shadow_enabled/v1",'
            b'"transaction_id":"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA'
            b'AAAAAAAAAAAAAAAA"}',
            b"[" * 1100 + b"0" + b"]" * 1100,
            b'{"enabled":false,"schema":"route_shadow_enabled/v1",'
            b'"transaction_id":'
            + b"[" * 1100
            + b"null"
            + b"]" * 1100
            + b"}",
        )
        for payload in malformed:
            with self.subTest(payload=payload):
                with tempfile.TemporaryDirectory() as directory:
                    data_dir = Path(directory)
                    _write_authority(data_dir, payload)
                    self.assertEqual(
                        load_committed_route_shadow_authority(data_dir),
                        {
                            "schema": "route_shadow_authority_view/v1",
                            "status": "invalid",
                            "transaction_id": None,
                            "authority_sha256": hashlib.sha256(payload).hexdigest(),
                            "primary_unit_projection_sha256": None,
                            "reason_code": "authority_evidence_invalid",
                        },
                    )

    def test_symlink_hardlink_directory_and_symlink_ancestor_are_unsafe(self):
        cases = ("symlink", "hardlink", "directory", "ancestor")
        for case in cases:
            with self.subTest(case=case):
                with tempfile.TemporaryDirectory() as directory:
                    root = Path(directory)
                    data_dir = root / "data"
                    data_dir.mkdir()
                    target = root / "target.json"
                    target.write_bytes(_FALSE_AUTHORITY_BYTES)
                    if case == "ancestor":
                        real_routes = root / "real-routes"
                        (real_routes / "shadow" / "operational").mkdir(parents=True)
                        (real_routes / "shadow" / "operational" / "enabled.json").write_bytes(
                            _FALSE_AUTHORITY_BYTES
                        )
                        (data_dir / "routes").symlink_to(real_routes, target_is_directory=True)
                    else:
                        operational = data_dir / "routes" / "shadow" / "operational"
                        operational.mkdir(parents=True)
                        authority = operational / "enabled.json"
                        if case == "symlink":
                            authority.symlink_to(target)
                        elif case == "hardlink":
                            os.link(target, authority)
                        else:
                            authority.mkdir()

                    self.assertEqual(
                        load_committed_route_shadow_authority(data_dir),
                        {
                            "schema": "route_shadow_authority_view/v1",
                            "status": "invalid",
                            "transaction_id": None,
                            "authority_sha256": None,
                            "primary_unit_projection_sha256": None,
                            "reason_code": "authority_evidence_invalid",
                        },
                    )

    def test_oversized_authority_is_rejected_before_read(self):
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory)
            _write_authority(data_dir, b"x" * (3 * 1024 + 1))
            with patch.object(
                route_shadow_authority.os,
                "pread",
                side_effect=AssertionError("oversized authority must not be read"),
            ):
                result = load_committed_route_shadow_authority(data_dir)
            self.assertEqual(result["status"], "invalid")
            self.assertIsNone(result["authority_sha256"])
            self.assertEqual(result["reason_code"], "authority_evidence_invalid")

    def test_directory_identity_change_is_fail_closed(self):
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory)
            _write_authority(data_dir, _FALSE_AUTHORITY_BYTES)
            with patch.object(
                route_shadow_authority,
                "_recheck_directory_chain",
                side_effect=route_shadow_authority._AuthorityUnsafe(
                    "directory changed"
                ),
            ):
                result = load_committed_route_shadow_authority(data_dir)
            self.assertEqual(result["status"], "invalid")
            self.assertIsNone(result["authority_sha256"])
            self.assertEqual(result["reason_code"], "authority_evidence_invalid")

    def test_directory_disappearance_after_file_read_is_not_missing_authority(self):
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory)
            authority = _write_authority(data_dir, _FALSE_AUTHORITY_BYTES)
            operational = authority.parent
            moved = operational.parent / "operational-moved"
            real_pread = route_shadow_authority.os.pread
            did_move = []

            def read_then_move(descriptor, size, offset):
                block = real_pread(descriptor, size, offset)
                if not did_move:
                    operational.rename(moved)
                    did_move.append(True)
                return block

            with patch.object(
                route_shadow_authority.os, "pread", side_effect=read_then_move
            ):
                result = load_committed_route_shadow_authority(data_dir)

            self.assertEqual(result["status"], "invalid")
            self.assertIsNone(result["authority_sha256"])
            self.assertEqual(result["reason_code"], "authority_evidence_invalid")

    def test_absent_authority_directory_swap_after_second_stat_is_unsafe(self):
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory)
            shadow = data_dir / "routes" / "shadow"
            operational = shadow / "operational"
            replacement = shadow / "operational-replacement"
            moved = shadow / "operational-moved"
            operational.mkdir(parents=True)
            replacement.mkdir()
            (replacement / "enabled.json").write_bytes(_TRUE_AUTHORITY_BYTES)
            real_stat = route_shadow_authority.os.stat
            authority_stats = []

            def stat_then_swap(path, *args, **kwargs):
                if path == "enabled.json":
                    authority_stats.append(True)
                    if len(authority_stats) == 2:
                        operational.rename(moved)
                        replacement.rename(operational)
                return real_stat(path, *args, **kwargs)

            with patch.object(
                route_shadow_authority.os, "stat", side_effect=stat_then_swap
            ):
                result = load_committed_route_shadow_authority(data_dir)

            self.assertEqual(len(authority_stats), 2)
            self.assertEqual(result["status"], "invalid")
            self.assertIsNone(result["authority_sha256"])
            self.assertEqual(result["reason_code"], "authority_evidence_invalid")

    def test_data_dir_symlink_is_unsafe(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            real_data = root / "real-data"
            _write_authority(real_data, _FALSE_AUTHORITY_BYTES)
            linked_data = root / "linked-data"
            linked_data.symlink_to(real_data, target_is_directory=True)

            result = load_committed_route_shadow_authority(linked_data)

            self.assertEqual(result["status"], "invalid")
            self.assertIsNone(result["authority_sha256"])
            self.assertEqual(result["reason_code"], "authority_evidence_invalid")

    def test_authority_path_replacement_at_read_barrier_is_unsafe(self):
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory)
            authority = _write_authority(data_dir, _FALSE_AUTHORITY_BYTES)
            replacement = authority.parent / "replacement.json"
            replacement.write_bytes(_TRUE_AUTHORITY_BYTES)
            real_pread = route_shadow_authority.os.pread
            did_replace = []

            def read_then_replace(descriptor, size, offset):
                block = real_pread(descriptor, size, offset)
                if not did_replace:
                    authority.unlink()
                    replacement.rename(authority)
                    did_replace.append(True)
                return block

            with patch.object(
                route_shadow_authority.os, "pread", side_effect=read_then_replace
            ):
                result = load_committed_route_shadow_authority(data_dir)

            self.assertEqual(result["status"], "invalid")
            self.assertIsNone(result["authority_sha256"])
            self.assertEqual(result["reason_code"], "authority_evidence_invalid")

    def test_authority_file_rename_aba_during_open_is_unsafe(self):
        with tempfile.TemporaryDirectory() as directory:
            data_dir = Path(directory)
            authority = _write_authority(data_dir, _FALSE_AUTHORITY_BYTES)
            parked = authority.parent / "parked.json"
            interloper = authority.parent / "interloper.json"
            interloper.write_bytes(_FALSE_AUTHORITY_BYTES)
            real_open = route_shadow_authority.os.open
            did_aba = []

            def open_during_aba(path, flags, *args, **kwargs):
                if path == "enabled.json" and not did_aba:
                    authority.rename(parked)
                    interloper.rename(authority)
                    descriptor = real_open(path, flags, *args, **kwargs)
                    authority.rename(interloper)
                    parked.rename(authority)
                    did_aba.append(True)
                    return descriptor
                return real_open(path, flags, *args, **kwargs)

            with patch.object(
                route_shadow_authority.os, "open", side_effect=open_during_aba
            ):
                result = load_committed_route_shadow_authority(data_dir)

            self.assertEqual(did_aba, [True])
            self.assertEqual(result["status"], "invalid")
            self.assertIsNone(result["authority_sha256"])
            self.assertEqual(result["reason_code"], "authority_evidence_invalid")

    def test_data_dir_rename_aba_during_open_is_unsafe(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            data_dir = root / "data"
            _write_authority(data_dir, _FALSE_AUTHORITY_BYTES)
            detached = root / "data-detached"
            foreign_data = root / "foreign-data"
            _write_authority(foreign_data, _FALSE_AUTHORITY_BYTES)
            real_open = route_shadow_authority.os.open
            did_aba = []

            def open_foreign_data_then_restore(path, flags, *args, **kwargs):
                if path == "data" and not did_aba:
                    data_dir.rename(detached)
                    foreign_data.rename(data_dir)
                    descriptor = real_open(path, flags, *args, **kwargs)
                    data_dir.rename(foreign_data)
                    detached.rename(data_dir)
                    did_aba.append(True)
                    return descriptor
                return real_open(path, flags, *args, **kwargs)

            with patch.object(
                route_shadow_authority.os,
                "open",
                side_effect=open_foreign_data_then_restore,
            ):
                result = load_committed_route_shadow_authority(data_dir)

            self.assertEqual(did_aba, [True])
            self.assertEqual(result["status"], "invalid")
            self.assertIsNone(result["authority_sha256"])
            self.assertEqual(result["reason_code"], "authority_evidence_invalid")


if __name__ == "__main__":
    unittest.main()
