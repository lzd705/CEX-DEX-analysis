import copy
import csv
import hashlib
import json
import shutil
import tempfile
import unittest
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from dashboard import server
from scripts import fetch_dex_depth
from scripts.check_dashboard_release import (
    ReleaseCheckError,
    validate_release_health,
)
from scripts.execution_cost import EXECUTION_COST_COLUMNS
from scripts.run_uniswap_v3_canary import run_canary
from test_uniswap_v3_exact_publication import ExactCandidateFixture


def canonical_receipt_bytes(receipt):
    return (
        json.dumps(
            receipt,
            ensure_ascii=True,
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    ).encode("utf-8")


def write_csv(path, fieldnames, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames, lineterminator="\n")
        writer.writeheader()
        writer.writerows(
            {field: row.get(field, "") for field in fieldnames} for row in rows
        )


def complete_public_candidate(fixture, observed_at="2026-08-27T00:00:00+00:00"):
    fixture.depth_rows = [
        {
            field: row.get(field, "")
            for field in fetch_dex_depth.DEX_DEPTH_COLUMNS
        }
        for row in fixture.depth_rows
    ]
    fixture.execution_rows = [
        {field: row.get(field, "") for field in EXECUTION_COST_COLUMNS}
        for row in fixture.execution_rows
    ]
    for row in fixture.depth_rows + fixture.execution_rows:
        row["observed_at"] = observed_at
        row["response_received_at"] = observed_at
    for row in fixture.depth_rows:
        row["reason_code"] = "observed"
    return fixture.validate()


class UniswapV3ExactSidecarTest(unittest.TestCase):
    def setUp(self):
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        self.root = Path(temporary_directory.name)
        self.fixture = ExactCandidateFixture(self.root / "candidate")

    def _publisher_call(self, receipt, publish_dir, output_dir):
        with patch.object(
            fetch_dex_depth,
            "require_aligned_depth_execution_lineage",
        ), patch.object(
            fetch_dex_depth,
            "validate_execution_snapshot",
        ), patch.object(
            fetch_dex_depth,
            "validate_passing_coverage_report",
            side_effect=({"gate": "depth"}, {"gate": "execution"}),
        ):
            return fetch_dex_depth.publish_full_publication_bundle(
                self.fixture.depth_rows,
                self.fixture.execution_rows,
                output_dir=output_dir,
                publish_dir=publish_dir,
                preflight_reports={
                    "dex_depth": {"status": "pass"},
                    "dex_execution_cost": {"status": "pass"},
                },
                exact_validation_receipt=receipt,
                authority_path=self.fixture.authority_path,
            )

    def _protected_publication(self, publish_dir):
        publish_dir.mkdir(parents=True, exist_ok=True)
        paths = {
            publish_dir / fetch_dex_depth.HISTORY_FILENAME: b"old-history\n",
            publish_dir / fetch_dex_depth.LATEST_FILENAME: b"old-depth-latest\n",
            publish_dir / fetch_dex_depth.CURRENT_FILENAME: b"old-depth-current\n",
            publish_dir / fetch_dex_depth.EXECUTION_LATEST_FILENAME: (
                b"old-execution\n"
            ),
            publish_dir
            / fetch_dex_depth.UNISWAP_V3_EXACT_LATEST_FILENAME: b"old-receipt\n",
        }
        for path, payload in paths.items():
            path.write_bytes(payload)
        return paths

    def test_canary_and_production_return_the_identical_shared_receipt(self):
        expected = self.fixture.validate()
        evidence_root = self.root / "canary"

        def collect_tvl(_pools, *, raw_root, sleep_seconds):
            shutil.copytree(self.fixture.tvl_directory, raw_root / self.fixture.tvl_snapshot_id)
            return self.fixture.tvl_snapshot_id, copy.deepcopy(self.fixture.inventory)

        def collect_depth(_pools, *, raw_root, sleep_seconds):
            shutil.copytree(
                self.fixture.depth_directory,
                raw_root / self.fixture.depth_snapshot_id,
            )
            return (
                self.fixture.depth_snapshot_id,
                copy.deepcopy(self.fixture.depth_rows),
                copy.deepcopy(self.fixture.execution_rows),
            )

        with patch(
            "scripts.run_uniswap_v3_canary.collect_tvl",
            side_effect=collect_tvl,
        ), patch(
            "scripts.run_uniswap_v3_canary.collect_dex_depth_with_execution",
            side_effect=collect_depth,
        ):
            result = run_canary(
                evidence_root,
                authority_path=self.fixture.authority_path,
            )

        self.assertFalse(result["published"])
        self.assertEqual(result["uniswap_v3_exact_validation"], expected)
        raw_receipt = (
            evidence_root
            / "depth"
            / self.fixture.depth_snapshot_id
            / fetch_dex_depth.UNISWAP_V3_EXACT_RAW_RECEIPT_FILENAME
        )
        self.assertEqual(
            raw_receipt.read_bytes(), canonical_receipt_bytes(expected)
        )

    def test_explicit_gate_writes_private_receipt_before_publication(self):
        arguments = SimpleNamespace(
            tvl_csv=self.root / "inventory.csv",
            output_dir=self.root / "processed",
            raw_root=self.fixture.depth_raw_root,
            tvl_raw_root=self.fixture.tvl_raw_root,
            publish_local=False,
            publish_dir=self.root / "published",
            sleep_seconds=0,
            tokens=None,
            chains=None,
            market_id=None,
            merge_publish=False,
            require_uniswap_v3_exact_validation=True,
        )
        observed = {}

        def publish_fixture(*_args, **kwargs):
            receipt = kwargs["exact_validation_receipt"]
            receipt_path = (
                self.fixture.depth_directory
                / fetch_dex_depth.UNISWAP_V3_EXACT_RAW_RECEIPT_FILENAME
            )
            observed["receipt"] = receipt_path.read_bytes()
            self.assertFalse(arguments.output_dir.exists())
            return ({"row_count": 2}, {"execution_row_count": 20})

        with patch.object(fetch_dex_depth, "parse_args", return_value=arguments), patch.object(
            fetch_dex_depth,
            "load_pool_inventory",
            return_value=self.fixture.inventory,
        ), patch.object(
            fetch_dex_depth,
            "collect_dex_depth_with_execution",
            return_value=(
                self.fixture.depth_snapshot_id,
                self.fixture.depth_rows,
                self.fixture.execution_rows,
            ),
        ), patch.object(
            fetch_dex_depth,
            "preflight_publication_bundle",
            return_value={},
        ), patch.object(
            fetch_dex_depth,
            "publish_full_publication_bundle",
            side_effect=publish_fixture,
        ), patch(
            "builtins.print",
        ):
            fetch_dex_depth.main()

        self.assertEqual(observed["receipt"], canonical_receipt_bytes(self.fixture.validate()))

    def test_full_publication_atomically_includes_exact_receipt(self):
        receipt = complete_public_candidate(self.fixture)
        publish_dir = self.root / "published-success"
        output_dir = self.root / "processed-success"

        self._publisher_call(receipt, publish_dir, output_dir)

        receipt_path = publish_dir / fetch_dex_depth.UNISWAP_V3_EXACT_LATEST_FILENAME
        self.assertEqual(receipt_path.read_bytes(), canonical_receipt_bytes(receipt))
        self.assertEqual(
            hashlib.sha256(receipt_path.read_bytes()).hexdigest(),
            hashlib.sha256(canonical_receipt_bytes(receipt)).hexdigest(),
        )

    def test_tampered_or_missing_receipt_rejects_before_any_write(self):
        receipt = complete_public_candidate(self.fixture)
        for name, candidate_receipt in (
            ("missing", None),
            ("tampered", {**receipt, "depth_rows_sha256": "f" * 64}),
        ):
            with self.subTest(name=name):
                publish_dir = self.root / ("published-" + name)
                output_dir = self.root / ("processed-" + name)
                protected = self._protected_publication(publish_dir)
                with self.assertRaisesRegex(ValueError, "exact.*receipt|receipt.*exact"):
                    self._publisher_call(candidate_receipt, publish_dir, output_dir)
                self.assertEqual(
                    {path: path.read_bytes() for path in protected},
                    protected,
                )
                self.assertFalse(output_dir.exists())

    def test_rehashed_receipt_cannot_hide_candidate_authority_drift(self):
        cases = ("depth_token", "execution_token")
        for name in cases:
            with self.subTest(name=name):
                fresh = ExactCandidateFixture(self.root / ("authority-" + name))
                forged = complete_public_candidate(fresh)
                if name == "depth_token":
                    fresh.depth_rows[0]["token0_address"] = "0x" + "f" * 40
                    forged["depth_rows_sha256"] = (
                        fetch_dex_depth.publication_rows_sha256(
                            fresh.depth_rows,
                            identity=fetch_dex_depth.dex_market_id,
                        )
                    )
                else:
                    fresh.execution_rows[0]["target_token_address"] = (
                        "0x" + "f" * 40
                    )
                    forged["execution_rows_sha256"] = (
                        fetch_dex_depth.publication_rows_sha256(
                            fresh.execution_rows,
                            identity=lambda row: (
                                row.get("market_id"),
                                row.get("direction"),
                                row.get("requested_notional_usd"),
                            ),
                        )
                    )
                with self.assertRaisesRegex(ValueError, "authority|exact public"):
                    fetch_dex_depth.validate_uniswap_v3_exact_public_receipt(
                        forged,
                        fresh.depth_rows,
                        fresh.execution_rows,
                        authority_path=fresh.authority_path,
                    )

    def test_receipt_rejects_an_unreferenced_usd_source_hash(self):
        receipt = complete_public_candidate(self.fixture)
        receipt["geckoterminal_raw_response_sha256"] = sorted(
            receipt["geckoterminal_raw_response_sha256"] + ["f" * 64]
        )

        with self.assertRaisesRegex(ValueError, "USD hashes|pool evidence"):
            fetch_dex_depth.validate_uniswap_v3_exact_public_receipt(
                receipt,
                self.fixture.depth_rows,
                self.fixture.execution_rows,
                authority_path=self.fixture.authority_path,
            )

    def test_sidecar_replace_failure_restores_every_prior_public_byte(self):
        receipt = complete_public_candidate(self.fixture)
        publish_dir = self.root / "published-failure"
        output_dir = self.root / "processed-failure"
        protected = self._protected_publication(publish_dir)
        sidecar = publish_dir / fetch_dex_depth.UNISWAP_V3_EXACT_LATEST_FILENAME
        from scripts import atomic_publication

        real_replace = atomic_publication.os.replace
        failed = {"value": False}

        def fail_sidecar_once(source, destination):
            if (
                Path(destination) == sidecar
                and ".stage" in Path(source).name
                and not failed["value"]
            ):
                failed["value"] = True
                raise OSError("injected exact sidecar replacement failure")
            return real_replace(source, destination)

        with patch(
            "scripts.atomic_publication.os.replace",
            side_effect=fail_sidecar_once,
        ), self.assertRaisesRegex(OSError, "exact sidecar"):
            self._publisher_call(receipt, publish_dir, output_dir)

        self.assertTrue(failed["value"])
        self.assertEqual({path: path.read_bytes() for path in protected}, protected)


class UniswapV3ExactHealthTest(unittest.TestCase):
    def setUp(self):
        temporary_directory = tempfile.TemporaryDirectory()
        self.addCleanup(temporary_directory.cleanup)
        self.root = Path(temporary_directory.name)
        self.fixture = ExactCandidateFixture(self.root / "candidate")
        self.observed_at = "2026-08-27T00:00:00+00:00"
        self.receipt = complete_public_candidate(self.fixture, self.observed_at)
        self.depth_path = self.root / fetch_dex_depth.LATEST_FILENAME
        self.execution_path = self.root / fetch_dex_depth.EXECUTION_LATEST_FILENAME
        self.receipt_path = (
            self.root / fetch_dex_depth.UNISWAP_V3_EXACT_LATEST_FILENAME
        )
        write_csv(
            self.depth_path,
            fetch_dex_depth.DEX_DEPTH_COLUMNS,
            self.fixture.depth_rows,
        )
        write_csv(
            self.execution_path,
            EXECUTION_COST_COLUMNS,
            self.fixture.execution_rows,
        )
        self.receipt_path.write_bytes(canonical_receipt_bytes(self.receipt))

    def health(self, now):
        return server.uniswap_v3_exact_health(
            depth_path=self.depth_path,
            execution_path=self.execution_path,
            receipt_path=self.receipt_path,
            authority_path=self.fixture.authority_path,
            now=now,
        )

    def test_current_health_exposes_exact_identity_counts_and_hashes(self):
        result = self.health(datetime(2026, 8, 27, 1, tzinfo=timezone.utc))

        self.assertEqual(result["status"], "current")
        self.assertEqual(result["authority_market_ids"], self.fixture.market_ids)
        self.assertEqual(result["depth_observed_count"], 2)
        self.assertEqual(result["depth_required_count"], 2)
        self.assertEqual(result["execution_observed_scenario_count"], 20)
        self.assertEqual(result["execution_required_scenario_count"], 20)
        self.assertEqual(result["shared_finalized_block"], {"number": 123, "hash": "0x" + "a" * 64})
        self.assertEqual(result["observed_at"], self.observed_at)
        self.assertEqual(result["observation_age_hours"], 1.0)
        for field in (
            "receipt_sha256",
            "authority_sha256",
            "depth_rows_sha256",
            "execution_rows_sha256",
        ):
            self.assertRegex(result[field], r"^[0-9a-f]{64}$")

    def test_stale_health_preserves_exact_facts_but_marks_scope_stale(self):
        result = self.health(datetime(2026, 8, 27, 3, tzinfo=timezone.utc))

        self.assertEqual(result["status"], "stale")
        self.assertEqual(result["depth_observed_count"], 2)
        self.assertEqual(result["execution_observed_scenario_count"], 20)
        self.assertEqual(result["observation_age_hours"], 3.0)

    def test_missing_receipt_is_explicit(self):
        self.receipt_path.unlink()

        result = self.health(datetime(2026, 8, 27, 1, tzinfo=timezone.utc))

        self.assertEqual(result["status"], "missing")
        self.assertEqual(result["depth_required_count"], 2)
        self.assertEqual(result["execution_required_scenario_count"], 20)
        self.assertEqual(result["depth_observed_count"], 2)
        self.assertEqual(result["execution_observed_scenario_count"], 20)
        self.assertIsNone(result["receipt_sha256"])

    def test_tampered_receipt_or_public_rows_are_invalid(self):
        cases = ("receipt", "execution")
        for name in cases:
            with self.subTest(name=name):
                if name == "receipt":
                    forged = {**self.receipt, "execution_rows_sha256": "f" * 64}
                    self.receipt_path.write_bytes(canonical_receipt_bytes(forged))
                else:
                    self.receipt_path.write_bytes(canonical_receipt_bytes(self.receipt))
                    rows = copy.deepcopy(self.fixture.execution_rows)
                    rows[0]["quote_amount"] = "999"
                    write_csv(self.execution_path, EXECUTION_COST_COLUMNS, rows)
                result = self.health(
                    datetime(2026, 8, 27, 1, tzinfo=timezone.utc)
                )
                self.assertEqual(result["status"], "invalid")
                self.assertEqual(result["depth_observed_count"], 2)
                self.assertEqual(result["execution_observed_scenario_count"], 20)

    def test_invalid_exact_scope_degrades_health_without_hiding_core_facts(self):
        handler = object.__new__(server.MarketMonitorHandler)
        handler.path = "/health"
        core = {
            "metadata": {
                "storage": {"engine": "sqlite"},
                "freshness": {"overall_status": "current"},
                "cex_instrument_lifecycle": {
                    "absence_market_count": 1,
                    "applied_market_count": 1,
                    "stale_evidence_market_count": 0,
                },
            }
        }
        exact = {
            "status": "invalid",
            "authority_market_ids": self.fixture.market_ids,
        }
        with patch.object(server, "build_market_payload", return_value=core), patch.object(
            server,
            "uniswap_v3_exact_health",
            return_value=exact,
        ), patch.object(server.MarketMonitorHandler, "send_json") as send_json:
            handler.do_GET()

        payload, status = send_json.call_args.args[0], send_json.call_args.args[1:]
        self.assertEqual(status, ())
        self.assertEqual(payload["status"], "ok")
        self.assertTrue(payload["data_ready"])
        self.assertEqual(payload["data_status"], "stale")
        self.assertEqual(payload["uniswap_v3_exact"], exact)


class UniswapV3ExactReleaseTest(unittest.TestCase):
    @staticmethod
    def exact_health():
        return {
            "status": "current",
            "authority_market_ids": [
                "dex:eth:uniswap_v3:0x1d42064fc4beb5f8aaf85f4617ae8b3b5b8bd801:UNI",
                "dex:eth:uniswap_v3:0x3470447f3cecffac709d3e783a307790b0208d60:UNI",
            ],
            "depth_observed_count": 2,
            "depth_required_count": 2,
            "execution_observed_scenario_count": 20,
            "execution_required_scenario_count": 20,
            "authority_sha256": hashlib.sha256(
                fetch_dex_depth.V3_EXECUTION_AUTHORITY_PATH.read_bytes()
            ).hexdigest(),
            "depth_rows_sha256": "b" * 64,
            "execution_rows_sha256": "c" * 64,
            "receipt_sha256": "d" * 64,
            "shared_finalized_block": {"number": 123, "hash": "0x" + "e" * 64},
            "observed_at": "2026-08-27T00:00:00+00:00",
            "observation_age_hours": 1.0,
            "max_age_hours": 2.0,
        }

    def health(self, exact):
        return {
            "data_status": "current",
            "application_sha": "a" * 40,
            "asset_sha": "b" * 64,
            "asset_version": ("a" * 12) + "-" + ("b" * 12),
            "freshness": {},
            "cex_instrument_lifecycle": {},
            "uniswap_v3_exact": exact,
        }

    def test_release_requires_current_exact_identities_counts_and_hash_shapes(self):
        valid = self.exact_health()
        with patch(
            "scripts.check_dashboard_release.validate_source_freshness"
        ), patch("scripts.check_dashboard_release.validate_lifecycle_freshness"):
            validate_release_health(self.health(valid))

            cases = {
                "missing": None,
                "stale": {**valid, "status": "stale"},
                "market_ids": {**valid, "authority_market_ids": valid["authority_market_ids"][:1]},
                "depth_count": {**valid, "depth_observed_count": 1},
                "execution_count": {**valid, "execution_observed_scenario_count": 19},
                "receipt_hash": {**valid, "receipt_sha256": "not-a-sha"},
                "block_hash": {**valid, "shared_finalized_block": {"number": 123, "hash": "0x1234"}},
            }
            for name, exact in cases.items():
                with self.subTest(name=name), self.assertRaisesRegex(
                    ReleaseCheckError,
                    "Uniswap V3 exact",
                ):
                    validate_release_health(self.health(exact))


if __name__ == "__main__":
    unittest.main()
