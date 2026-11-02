"""Failure-cleanup tests for reopened historical run snapshots."""

from __future__ import annotations

import os
import unittest
from unittest import mock


class HistoricalRunSnapshotCleanupTests(unittest.TestCase):
    @staticmethod
    def _open_finalized_run():
        from tests.test_historical_route_publication import (
            HistoricalCorePublicationTests,
        )

        return HistoricalCorePublicationTests._open_real_task7_lease()

    @staticmethod
    def _close_finalized_run(run, finalized, lease):
        from tests.test_historical_route_publication import (
            HistoricalCorePublicationTests,
        )

        try:
            if lease is not None:
                lease.close()
        finally:
            HistoricalCorePublicationTests._close_real_task7_run(
                run, finalized
            )

    @staticmethod
    def _run_snapshot_registry(storage):
        closure = dict(zip(
            storage.open_validated_run.__code__.co_freevars,
            (
                cell.cell_contents
                for cell in storage.open_validated_run.__closure__
            ),
        ))
        return closure["run_snapshot_registry"]

    def test_repeated_final_reread_failures_retire_provisional_snapshots(self):
        import scripts.historical_foundry_storage as storage

        run = finalized = lease = None
        captured = []
        registry = None
        try:
            run, finalized, lease, identity = self._open_finalized_run()
            registry = self._run_snapshot_registry(storage)
            registry_before = set(registry)

            def fail_final_reread(snapshot):
                captured.append(snapshot)
                raise storage.HistoricalFoundryStorageError()

            with mock.patch.object(
                storage.HistoricalRunSnapshot,
                "reread_unchanged",
                fail_final_reread,
            ):
                for _attempt in range(2):
                    with self.assertRaises(
                        storage.HistoricalFoundryStorageError
                    ):
                        storage.open_validated_run(
                            data_dir=run["fixture"].data_dir,
                            run_id=identity["run_id"],
                            expected_manifest_sha256=identity[
                                "run_manifest_sha256"
                            ],
                        )

            self.assertEqual(set(registry), registry_before)
            self.assertEqual(len(captured), 2)
            for snapshot in captured:
                with self.assertRaises(
                    storage.HistoricalFoundryStorageError
                ):
                    snapshot.identity_projection()
        finally:
            if registry is not None:
                for snapshot in captured:
                    registry.pop(id(snapshot), None)
            if run is not None:
                self._close_finalized_run(run, finalized, lease)

    def test_failed_open_snapshot_cannot_close_reused_file_descriptors(self):
        import scripts.historical_foundry_storage as storage

        run = finalized = lease = None
        opened = []
        captured = []
        captured_chain_fds = []
        registry = None
        try:
            run, finalized, lease, identity = self._open_finalized_run()
            registry = self._run_snapshot_registry(storage)

            def fail_final_reread(snapshot):
                entry = registry[id(snapshot)]
                captured.append(snapshot)
                captured_chain_fds.extend(
                    row[0] for row in entry[1]["chain"]
                )
                raise storage.HistoricalFoundryStorageError()

            with mock.patch.object(
                storage.HistoricalRunSnapshot,
                "reread_unchanged",
                fail_final_reread,
            ):
                with self.assertRaises(
                    storage.HistoricalFoundryStorageError
                ):
                    storage.open_validated_run(
                        data_dir=run["fixture"].data_dir,
                        run_id=identity["run_id"],
                        expected_manifest_sha256=identity[
                            "run_manifest_sha256"
                        ],
                    )

            target_fds = set(captured_chain_fds)
            while not target_fds.issubset(opened):
                opened.append(os.open(os.devnull, os.O_RDONLY))
                self.assertLess(len(opened), 256)

            try:
                captured[0].close()
            except storage.HistoricalFoundryStorageError:
                pass

            unexpectedly_closed = []
            for descriptor in opened:
                try:
                    os.fstat(descriptor)
                except OSError:
                    unexpectedly_closed.append(descriptor)
            self.assertEqual(unexpectedly_closed, [])
        finally:
            for descriptor in opened:
                try:
                    os.close(descriptor)
                except OSError:
                    pass
            if registry is not None:
                for snapshot in captured:
                    registry.pop(id(snapshot), None)
            if run is not None:
                self._close_finalized_run(run, finalized, lease)


if __name__ == "__main__":
    unittest.main()
