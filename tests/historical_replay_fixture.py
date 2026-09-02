"""Local published historical replay fixture for dashboard contracts."""

from __future__ import annotations

import hashlib
import os
from pathlib import Path
from unittest import mock


class PublishedHistoricalReplayFixture:
    """Own one fully published local bundle and all of its held resources."""

    def __init__(self) -> None:
        import scripts.historical_foundry_verifier as verifier
        import scripts.historical_route_publication as publication
        from tests.test_historical_route_publication import (
            HistoricalCorePublicationTests,
        )

        self._helper = HistoricalCorePublicationTests
        self.run = self.finalized = self.context = self.subject = None
        lease = core_stage = None
        try:
            self.run, self.finalized, lease, _identity = (
                self._helper._open_real_task7_lease(
                    include_newer_mixed_rows=True
                )
            )
            self.data_dir = self.run["fixture"].data_dir
            self.raw_root = (
                self.data_dir / "raw" / "historical-foundry-replay"
            )
            self.historical_root = (
                self.data_dir / "routes" / "historical"
            )
            core_stage = publication.stage_historical_replay_core(
                data_dir=self.data_dir,
                config=self.run["config"],
                publication_lease=lease,
            )
            lease = None
            self.context = publication.publish_historical_replay_core(
                data_dir=self.data_dir, staged_core=core_stage
            )
            core_stage = None
            staged = publication.stage_historical_replay_bundle(
                data_dir=self.data_dir,
                raw_root=self.raw_root,
                context=self.context,
            )
            self.subject = staged["verification_subject"]
            request = verifier._connected_request_for_subject(self.subject)
            observation = dict(
                verifier._build_connected_observation_for_retained_fixture(
                    request
                )
            )
            observation["evidence_mode"] = "production_connected"
            observation["process_id"] = os.getpid() + 1
            observation["process_identity_sha256"] = hashlib.sha256(
                str(observation["process_id"]).encode("ascii")
            ).hexdigest()
            with mock.patch.object(
                verifier,
                "_invoke_connected_historical_verification_engine",
                return_value=("production_connected", observation),
            ):
                verification = (
                    verifier.run_connected_historical_verification(
                        self.subject, mode="publish"
                    )
                )
            self.pointer = publication.publish_historical_replay_bundle(
                data_dir=self.data_dir,
                pointer_publication=staged["pointer_publication"],
                final_pointer_bytes=verification["final_pointer_bytes"],
            )
            self.bundle_path = (
                self.historical_root / "bundles"
                / self.pointer["replay_id"]
            )
        except BaseException:
            if core_stage is not None:
                core_stage.close()
            if lease is not None:
                lease.close()
            self.close()
            raise

    def close(self) -> None:
        if self.subject is not None:
            try:
                self.subject.close()
            except Exception:
                pass
            self.subject = None
        if self.context is not None:
            try:
                self.context.close()
            except Exception:
                pass
            self.context = None
        if self.run is not None:
            self._helper._close_real_task7_run(
                self.run, self.finalized
            )
            self.run = self.finalized = None

    def __enter__(self) -> "PublishedHistoricalReplayFixture":
        return self

    def __exit__(self, _error_type, _error, _traceback) -> None:
        self.close()


def historical_fixture_roots(
    fixture: PublishedHistoricalReplayFixture,
) -> tuple[Path, Path]:
    return fixture.historical_root, fixture.raw_root
