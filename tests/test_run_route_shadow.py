import csv
import fcntl
import hashlib
import inspect
import json
import os
import sqlite3
import tempfile
import time
import unittest
from contextlib import nullcontext
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

from scripts import run_route_shadow
from scripts.run_route_shadow import (
    load_run_ledger,
    normalize_systemd_result,
    reconcile_shadow_run,
    run_shadow_once,
)


NOW = datetime(2026, 8, 2, 12, 0, 0, tzinfo=timezone.utc)


class PublicSurfaceTests(unittest.TestCase):
    def test_public_wrapper_has_only_fixed_data_dir_and_expected_phase(self):
        self.assertEqual(
            str(inspect.signature(run_shadow_once)),
            "(data_dir: 'Path', *, expected_phase: 'Optional[str]' = None) -> 'dict'",
        )
        with self.assertRaises(TypeError):
            run_shadow_once(Path("/tmp/x"), now=NOW)

    def test_cli_exposes_no_clock_collector_or_resource_override(self):
        parser = run_route_shadow.build_parser()
        for argv in (
            ["run", "--data-dir", "/tmp/x", "--now", NOW.isoformat()],
            ["run", "--data-dir", "/tmp/x", "--deadline-seconds", "1"],
            ["run", "--data-dir", "/tmp/x", "--max-workers", "9"],
        ):
            with self.subTest(argv=argv):
                with self.assertRaises(SystemExit):
                    parser.parse_args(argv)

    def test_monotonic_utc_projection_is_integer_exact_and_bounded(self):
        self.assertEqual(
            run_route_shadow._utc_from_monotonic(NOW, 10, 10), NOW
        )
        self.assertEqual(
            run_route_shadow._utc_text(
                run_route_shadow._utc_from_monotonic(
                    NOW, 10, 1_234_567_900
                )
            ),
            "2026-08-02T12:00:01.234567Z",
        )
        for start, current in (
            (True, 1),
            (1.0, 2),
            ("1", 2),
            (-1, 0),
            (2, 1),
        ):
            with self.subTest(start=start, current=current):
                with self.assertRaisesRegex(ValueError, "projection"):
                    run_route_shadow._utc_from_monotonic(
                        NOW, start, current
                    )
        with self.assertRaisesRegex(ValueError, "out of range"):
            run_route_shadow._utc_from_monotonic(
                NOW, 0, 10 ** 40
            )


class ShadowLockPriorityTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.data_dir = Path(self.temporary.name) / "data"
        (self.data_dir / "collection").mkdir(parents=True)
        self.lock_path = self.data_dir / "collection/collection.lock"

    @staticmethod
    def _enabled_authority():
        return {
            "schema": "route_shadow_authority_view/v1",
            "status": "enabled",
            "transaction_id": "a" * 64,
            "authority_sha256": "b" * 64,
            "primary_unit_projection_sha256": "c" * 64,
            "reason_code": None,
        }

    def _run_busy_shadow(self, environment, factory):
        with self.lock_path.open("a+") as lock_handle:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            with patch.dict(os.environ, environment, clear=True), patch.object(
                run_route_shadow,
                "_run_shadow_owned",
                side_effect=AssertionError("busy run must call zero sources"),
            ), patch.object(
                run_route_shadow,
                "load_committed_route_shadow_authority",
                return_value=self._enabled_authority(),
            ), patch.object(
                run_route_shadow,
                "_trusted_utc_now",
                return_value=NOW,
            ), patch.object(
                run_route_shadow,
                "_new_manual_run_id",
                **factory,
            ):
                return run_shadow_once(self.data_dir)

    def test_busy_lock_commits_one_started_terminal_closure_and_zero_sources(self):
        with self.lock_path.open("a+") as lock_handle:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            with patch.object(
                run_route_shadow,
                "_run_shadow_owned",
                side_effect=AssertionError("busy run must call zero sources"),
            ), patch.object(
                run_route_shadow,
                "load_committed_route_shadow_authority",
                return_value={
                    "schema": "route_shadow_authority_view/v1",
                    "status": "enabled",
                    "transaction_id": "a" * 64,
                    "authority_sha256": "b" * 64,
                    "primary_unit_projection_sha256": "c" * 64,
                    "reason_code": None,
                },
            ), patch.object(
                run_route_shadow,
                "_trusted_utc_now",
                return_value=NOW,
            ), patch.object(
                run_route_shadow,
                "_new_manual_run_id",
                return_value="manual-busy",
            ):
                result = run_shadow_once(self.data_dir)

        self.assertEqual(result["outcome"], "skipped_locked")
        run_dir = self.data_dir / "routes/shadow/ledger/manual-busy"
        self.assertEqual(
            sorted(path.name for path in run_dir.iterdir()),
            ["started.json", "terminal.json"],
        )
        ledger = load_run_ledger(self.data_dir, "manual-busy")
        self.assertFalse(ledger["terminal"]["lock_acquired"])
        self.assertIsNone(ledger["terminal"]["verification_sha256"])

    def test_busy_manual_run_ignores_ambient_invocation_id_without_dispatch_marker(self):
        result = self._run_busy_shadow(
            {"INVOCATION_ID": "1" * 32},
            {"return_value": "manual-ambient"},
        )

        self.assertEqual(result["run_id"], "manual-ambient")
        ledger = load_run_ledger(self.data_dir, "manual-ambient")
        self.assertIsNone(ledger["started"]["dispatch_id"])
        self.assertIsNone(ledger["started"]["invocation_id"])
        self.assertIsNone(ledger["terminal"]["dispatch_id"])

    def test_busy_manual_run_uses_explicit_id_without_dispatch_marker(self):
        result = self._run_busy_shadow(
            {
                "INVOCATION_ID": "1" * 32,
                "ROUTE_SHADOW_RUN_ID": "manual-explicit",
            },
            {"side_effect": AssertionError("explicit manual ID must win")},
        )

        self.assertEqual(result["run_id"], "manual-explicit")
        ledger = load_run_ledger(self.data_dir, "manual-explicit")
        self.assertIsNone(ledger["started"]["dispatch_id"])
        self.assertIsNone(ledger["started"]["invocation_id"])

    def test_scheduled_run_binds_valid_dispatch_and_invocation_ids(self):
        dispatch = "2" * 32
        invocation = "3" * 32
        phase = {
            "phase": "canary",
            "phase_state_sha256": hashlib.sha256(
                b"route-shadow-phase/implicit-canary/v1\n"
            ).hexdigest(),
            "phase_transition_id": None,
            "state": None,
        }
        with patch.dict(
            os.environ,
            {
                "ROUTE_SHADOW_DISPATCH_ID": dispatch,
                "INVOCATION_ID": invocation,
            },
            clear=True,
        ), patch.object(
            run_route_shadow,
            "load_committed_route_shadow_authority",
            side_effect=(self._enabled_authority(), self._enabled_authority()),
        ), patch(
            "scripts.route_publication.load_active_phase_state",
            return_value=phase,
        ), patch(
            "scripts.route_shadow_inputs.build_shadow_universe",
            side_effect=ValueError("fixture source failure"),
        ), patch.object(
            run_route_shadow, "_trusted_utc_now", return_value=NOW
        ):
            result = run_shadow_once(self.data_dir)

        self.assertEqual(result["status"], "terminal")
        ledger = load_run_ledger(self.data_dir, invocation)
        self.assertEqual(ledger["started"]["run_id"], invocation)
        self.assertEqual(ledger["started"]["dispatch_id"], dispatch)
        self.assertEqual(ledger["started"]["invocation_id"], invocation)
        self.assertEqual(ledger["terminal"]["dispatch_id"], dispatch)

    def test_dispatch_marker_rejects_missing_or_invalid_scheduled_identity(self):
        dispatch = "4" * 32
        invocation = "5" * 32
        invalid_environments = (
            {"ROUTE_SHADOW_DISPATCH_ID": dispatch},
            {
                "ROUTE_SHADOW_DISPATCH_ID": dispatch,
                "INVOCATION_ID": "A" * 32,
            },
            {
                "ROUTE_SHADOW_DISPATCH_ID": "D" * 32,
                "INVOCATION_ID": invocation,
            },
            {
                "ROUTE_SHADOW_DISPATCH_ID": dispatch,
                "INVOCATION_ID": invocation,
                "ROUTE_SHADOW_RUN_ID": "6" * 32,
            },
            {
                "ROUTE_SHADOW_DISPATCH_ID": dispatch,
                "INVOCATION_ID": invocation,
                "ROUTE_SHADOW_RUN_ID": "",
            },
        )
        for environment in invalid_environments:
            with self.subTest(environment=environment), patch.dict(
                os.environ, environment, clear=True
            ), patch.object(
                run_route_shadow,
                "load_committed_route_shadow_authority",
                return_value=self._enabled_authority(),
            ), patch.object(
                run_route_shadow,
                "_open_collection_lock",
                side_effect=AssertionError("invalid identity must not open lock"),
            ):
                with self.assertRaises(ValueError):
                    run_shadow_once(self.data_dir)

        self.assertFalse((self.data_dir / "routes/shadow/ledger").exists())

    def test_disabled_authority_makes_zero_source_or_ledger_calls(self):
        with patch.object(
            run_route_shadow,
            "load_committed_route_shadow_authority",
            return_value={
                "schema": "route_shadow_authority_view/v1",
                "status": "disabled",
                "transaction_id": None,
                "authority_sha256": None,
                "primary_unit_projection_sha256": None,
                "reason_code": None,
            },
        ), patch.object(
            run_route_shadow,
            "_open_collection_lock",
            side_effect=AssertionError("disabled authority must not touch lock"),
        ):
            result = run_shadow_once(self.data_dir)
        self.assertEqual(result["status"], "disabled")
        self.assertFalse((self.data_dir / "routes/shadow/ledger").exists())

    def test_authority_is_replayed_after_lock_and_drift_starts_zero_sources(self):
        enabled = {
            "schema": "route_shadow_authority_view/v1",
            "status": "enabled",
            "transaction_id": "a" * 64,
            "authority_sha256": "b" * 64,
            "primary_unit_projection_sha256": "c" * 64,
            "reason_code": None,
        }
        disabled = {
            "schema": "route_shadow_authority_view/v1",
            "status": "disabled",
            "transaction_id": None,
            "authority_sha256": None,
            "primary_unit_projection_sha256": None,
            "reason_code": None,
        }
        with patch.object(
            run_route_shadow,
            "load_committed_route_shadow_authority",
            side_effect=(enabled, disabled),
        ) as loader, patch.object(
            run_route_shadow,
            "_run_shadow_owned",
            side_effect=AssertionError("authority drift must start zero sources"),
        ), patch.object(
            run_route_shadow, "_new_manual_run_id", return_value="manual-drift"
        ):
            result = run_shadow_once(self.data_dir)

        self.assertEqual(loader.call_count, 2)
        self.assertEqual(result["status"], "disabled")
        self.assertFalse((self.data_dir / "routes/shadow/ledger").exists())

    def test_enabled_owner_invokes_real_pipeline_and_closes_failure_ledger(self):
        enabled = {
            "schema": "route_shadow_authority_view/v1",
            "status": "enabled",
            "transaction_id": "a" * 64,
            "authority_sha256": "b" * 64,
            "primary_unit_projection_sha256": "c" * 64,
            "reason_code": None,
        }
        phase = {
            "phase": "canary",
            "phase_state_sha256": hashlib.sha256(
                b"route-shadow-phase/implicit-canary/v1\n"
            ).hexdigest(),
            "phase_transition_id": None,
            "state": None,
        }
        calls = []

        def fail_after_start(*_args, **_kwargs):
            calls.append("universe")
            raise ValueError("fixture source failure")

        with patch.object(
            run_route_shadow,
            "load_committed_route_shadow_authority",
            side_effect=(enabled, enabled),
        ), patch(
            "scripts.route_publication.load_active_phase_state",
            return_value=phase,
        ), patch(
            "scripts.route_shadow_inputs.build_shadow_universe",
            side_effect=fail_after_start,
        ), patch.object(
            run_route_shadow, "_new_manual_run_id", return_value="manual-owned"
        ), patch.object(
            run_route_shadow, "_trusted_utc_now", return_value=NOW
        ) as trusted_clock:
            result = run_shadow_once(self.data_dir)

        self.assertEqual(calls, ["universe"])
        self.assertEqual(
            trusted_clock.call_count,
            1,
            "an owned attempt must take exactly one trusted UTC sample",
        )
        self.assertEqual(result["status"], "terminal")
        self.assertEqual(result["outcome"], "failed")
        ledger = load_run_ledger(self.data_dir, "manual-owned")
        self.assertEqual(ledger["status"], "terminal")
        self.assertIsNotNone(ledger["started"])
        self.assertIsNotNone(ledger["verification"])
        self.assertTrue(ledger["terminal"]["lock_acquired"])

    def test_expected_phase_mismatch_is_an_acquired_zero_source_closure(self):
        enabled = {
            "schema": "route_shadow_authority_view/v1",
            "status": "enabled",
            "transaction_id": "a" * 64,
            "authority_sha256": "b" * 64,
            "primary_unit_projection_sha256": "c" * 64,
            "reason_code": None,
        }
        phase = {
            "phase": "full",
            "phase_state_sha256": "d" * 64,
            "phase_transition_id": "e" * 64,
            "state": {"schema": "fixture"},
        }
        with patch.object(
            run_route_shadow,
            "load_committed_route_shadow_authority",
            side_effect=(enabled, enabled),
        ), patch(
            "scripts.route_publication.load_active_phase_state",
            return_value=phase,
        ), patch(
            "scripts.route_shadow_inputs.build_shadow_universe",
            side_effect=AssertionError("phase mismatch must read zero sources"),
        ), patch.object(
            run_route_shadow, "_new_manual_run_id", return_value="manual-phase"
        ), patch.object(
            run_route_shadow, "_trusted_utc_now", return_value=NOW
        ):
            result = run_shadow_once(self.data_dir, expected_phase="canary")

        self.assertEqual(result["status"], "terminal")
        ledger = load_run_ledger(self.data_dir, "manual-phase")
        self.assertEqual(ledger["started"]["phase"], "full")
        self.assertEqual(
            ledger["verification"]["primary_failure_class"],
            "lineage_invalid",
        )
        self.assertEqual(ledger["terminal"]["outcome"], "failed")

    def _exercise_real_private_joint_pipeline(
        self, *, delete_typed_manifest_after_attach=False,
        registry_drift_checkpoint=None,
        registry_drift_field="adapter_registry_sha256",
        registry_error_checkpoint=None,
        late_response=False,
        supported_rpc_unavailable=False,
    ):
        from scripts import collect_route_cohort as collection_module
        from scripts import route_shadow_inputs
        from scripts.route_publication import (
            load_latest_route_cohort,
            load_latest_shadow_result,
        )
        from tests import test_route_shadow_inputs as input_fixture_module
        from tests.test_route_shadow_inputs import (
            ProductionInputFixture,
            write_csv,
        )

        tracked_pool = "0xd3d2e2692501a5c9ca623199d38826e513033a17"
        uni = "0x1f9840a85d5af5bf1d1762f925bdaddc4201f984"
        weth = "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2"
        if supported_rpc_unavailable:
            with patch.object(input_fixture_module, "POOL", tracked_pool):
                fixture = ProductionInputFixture(self.temporary.name)
            from scripts.execution_cost import EXECUTION_COST_COLUMNS
            from scripts.fetch_dex_depth import DEX_DEPTH_COLUMNS
            from scripts.fetch_tvl import TVL_COLUMNS

            def rewrite_rows(path, fields, mutate):
                with path.open(newline="", encoding="utf-8") as handle:
                    rows = list(csv.DictReader(handle))
                for row in rows:
                    mutate(row)
                write_csv(path, fields, rows)

            rewrite_rows(
                fixture.data_dir / "dex_depth_latest.csv",
                DEX_DEPTH_COLUMNS,
                lambda row: row.update({"pool_name": "UNI/WETH"}),
            )
            rewrite_rows(
                fixture.data_dir / "dex_execution_cost_latest.csv",
                EXECUTION_COST_COLUMNS,
                lambda row: row.update({
                    "quote_token_address": weth,
                    "quote_token_decimals": "18",
                }),
            )
            rewrite_rows(
                fixture.data_dir / "dex_pool_tvl_latest.csv",
                TVL_COLUMNS,
                lambda row: row.update({
                    "pool_name": "UNI/WETH",
                    "base_token_id": "eth_" + uni,
                    "quote_token_id": "eth_" + weth,
                    "base_token_price_usd": "100",
                    "quote_token_price_usd": "3000",
                }),
            )
            connection = sqlite3.connect(
                str(fixture.data_dir / "market_facts.sqlite3")
            )
            try:
                connection.execute(
                    "UPDATE dex_pool_daily SET pool_name = 'UNI/WETH'"
                )
                connection.commit()
            finally:
                connection.close()
        else:
            fixture = ProductionInputFixture(self.temporary.name)
        for row in fixture.cex_rows:
            if row["date"] >= "2026-07-03":
                row["quote_volume_usd"] = "100"
        write_csv(
            fixture.data_dir / "cex_exchange_volume_daily.csv",
            [
                "date", "token_symbol", "exchange", "cex_symbol", "open",
                "high", "low", "close", "base_volume", "quote_volume_usd",
            ],
            fixture.cex_rows,
        )
        connection = sqlite3.connect(
            str(fixture.data_dir / "market_facts.sqlite3")
        )
        try:
            connection.execute(
                "UPDATE cex_market_daily SET quote_volume_usd = '100' "
                "WHERE date >= '2026-07-03'"
            )
            connection.commit()
        finally:
            connection.close()
        fixture.rebind_database_cex_source()
        self.data_dir = fixture.data_dir
        (self.data_dir / "routes").mkdir(parents=True, exist_ok=True)
        public_latest = self.data_dir / "routes/latest.json"
        public_latest.write_bytes(b"public-route-sentinel\n")
        enabled = {
            "schema": "route_shadow_authority_view/v1",
            "status": "enabled",
            "transaction_id": "a" * 64,
            "authority_sha256": "b" * 64,
            "primary_unit_projection_sha256": "c" * 64,
            "reason_code": None,
        }
        shadow_root = self.data_dir / "routes/shadow"
        (shadow_root / "gates").mkdir(parents=True)
        (shadow_root / "transitions").mkdir()
        gate_bytes = json.dumps({
            "schema": "route_shadow_gate/v1",
            "phase": "canary",
            "blocking_reasons": [],
        }, ensure_ascii=False, sort_keys=True, separators=(",", ":")).encode(
            "utf-8"
        )
        gate_sha = hashlib.sha256(gate_bytes).hexdigest()
        (shadow_root / "gates" / (gate_sha + ".json")).write_bytes(gate_bytes)
        phase_identity = {
            "schema": "route_shadow_phase/v1",
            "prior_phase": "canary",
            "phase": "full",
            "evaluated_at": "2026-08-02T11:59:00Z",
            "gate_evidence_sha256": gate_sha,
            "storage_admission_sha256": "1" * 64,
            "anchored_joint_pointer_sha256": "2" * 64,
            "primary_schedule_guard_sha256": "3" * 64,
            "schedule_envelope_sha256": None,
            "phase_identity_id": "4" * 64,
        }
        transition_id = hashlib.sha256(json.dumps(
            phase_identity,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")).hexdigest()
        phase_state = {**phase_identity, "transition_id": transition_id}
        phase_bytes = json.dumps(
            phase_state,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
        (shadow_root / "transitions" / (transition_id + ".json")).write_bytes(
            phase_bytes
        )
        (shadow_root / "phase.json").write_bytes(phase_bytes)
        real_collect = collection_module.collect_route_cohort
        real_attach = collection_module.attach_typed_source_lineage
        from scripts import route_publication as publication_module
        real_publish_shadow = publication_module.publish_shadow_result
        generation_calls = []
        registry_snapshot_calls = []
        collection_errors = []
        publication_errors = []
        real_generation = route_shadow_inputs.current_source_generation
        real_registry_snapshot = run_route_shadow._route_cost_registry_snapshot
        captured_registry_snapshot = real_registry_snapshot()
        state_observed_at = (
            "2026-08-02T12:00:01Z"
            if late_response else "2026-08-02T12:00:00Z"
        )

        def registry_snapshot_with_drift():
            call_index = len(registry_snapshot_calls)
            if call_index == registry_error_checkpoint:
                registry_snapshot_calls.append("unavailable")
                raise ValueError("route-cost registry is unavailable")
            snapshot = dict(captured_registry_snapshot)
            if call_index == registry_drift_checkpoint:
                snapshot[registry_drift_field] = "f" * 64
            registry_snapshot_calls.append(snapshot)
            return snapshot

        def fake_cex(leg, *, snapshot_id, raw_path, **_kwargs):
            raw = b'{"book":"joint-runner"}'
            raw_path.write_bytes(raw)
            return {
                "market_id": leg["market_id"],
                "market_type": "cex",
                "exchange": "binance",
                "cex_symbol": "UNI/USDT",
                "token_symbol": "UNI",
                "snapshot_id": snapshot_id,
                "status": "observed",
                "available": True,
                "reason_code": "observed",
                "state_observed_at": state_observed_at,
                "source_endpoint": "https://api.example.test/depth",
                "raw_response_sha256": hashlib.sha256(raw).hexdigest(),
                "source_quote_asset": "USDT",
                "quote_to_usd": "1",
                "quote_conversion_method": "USDT=USD proxy",
            }

        def fake_dex(
            leg, *, snapshot_id, raw_path, fixed_block_number,
            fixed_block_timestamp, fixed_chain_id=None,
            fixed_block_header=None, typed_source_payload_sink=None,
            **_kwargs
        ):
            raw = b'{"pool":"joint-runner"}'
            raw_path.write_bytes(raw)
            raw_sha = hashlib.sha256(raw).hexdigest()
            token0 = "0x" + "2" * 40
            token1 = uni
            token0_price = "1"
            token1_price = "100"
            if supported_rpc_unavailable:
                from scripts.fetch_dex_depth import ROUTE_V2_FEE_PROOF_SHA256
                from scripts.route_quantity import V2PoolState, V2_FEE_FORMULA

                self.assertEqual(fixed_block_number, 20_000_000)
                self.assertEqual(fixed_chain_id, "0x1")
                self.assertIsNotNone(fixed_block_header)
                token0, token1 = uni, weth
                token0_price, token1_price = "100", "3000"
                state = V2PoolState(
                    chain="eth", chain_id=1, dex="uniswap_v2",
                    pool_address=tracked_pool,
                    token0_address=token0, token1_address=token1,
                    token0_decimals=18, token1_decimals=18,
                    reserve0_raw=1_000_000 * 10 ** 18,
                    reserve1_raw=30_000 * 10 ** 18,
                    reserve_timestamp_last_raw=1_704_067_200,
                    fee_bps=30, fee_numerator=9_970,
                    fee_denominator=10_000, fee_formula=V2_FEE_FORMULA,
                    fee_proof_sha256=ROUTE_V2_FEE_PROOF_SHA256,
                    block_number=fixed_block_number,
                    block_hash=fixed_block_header["hash"],
                    block_header_sha256=hashlib.sha256(json.dumps(
                        fixed_block_header, ensure_ascii=False, sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8")).hexdigest(),
                    observed_at=fixed_block_timestamp,
                    raw_response_sha256=raw_sha,
                )
                integer_fields = {
                    "chain_id", "token0_decimals", "token1_decimals",
                    "reserve0_raw", "reserve1_raw",
                    "reserve_timestamp_last_raw", "fee_bps",
                    "fee_numerator", "fee_denominator", "block_number",
                }
                state_payload = {
                    "schema": "route_v2_pool_state/v1",
                    **{
                        field: (
                            str(getattr(state, field))
                            if field in integer_fields
                            else getattr(state, field)
                        )
                        for field in (
                            "chain", "chain_id", "dex", "pool_address",
                            "token0_address", "token1_address",
                            "token0_decimals", "token1_decimals",
                            "reserve0_raw", "reserve1_raw",
                            "reserve_timestamp_last_raw", "fee_bps",
                            "fee_numerator", "fee_denominator", "fee_formula",
                            "fee_proof_sha256", "block_number", "block_hash",
                            "block_header_sha256", "observed_at",
                            "raw_response_sha256", "state_id",
                        )
                    },
                }
                self.assertIsNotNone(typed_source_payload_sink)
                typed_source_payload_sink({
                    "role": "dex_pool_state",
                    "payload": json.dumps(
                        state_payload, ensure_ascii=False, sort_keys=True,
                        separators=(",", ":"),
                    ).encode("utf-8"),
                })
            return {
                "market_id": leg["market_id"],
                "market_type": "dex",
                "chain": "eth",
                "dex": "uniswap_v2",
                "pool_address": leg["market_id"].split(":", 4)[3],
                "token_symbol": "UNI",
                "snapshot_id": snapshot_id,
                "status": "observed",
                "available": True,
                "reason_code": "observed",
                "state_observed_at": state_observed_at,
                "source_endpoint": "https://rpc.example.test",
                "raw_response_sha256": raw_sha,
                "block_number": str(fixed_block_number),
                "block_timestamp": fixed_block_timestamp,
                "token0_address": token0,
                "token1_address": token1,
                "token0_price_usd": token0_price,
                "token1_price_usd": token1_price,
            }

        def collect_with_local_workers(universe, **kwargs):
            kwargs.update({
                "cex_collector": fake_cex,
                "dex_collector": fake_dex,
                "dex_block_resolver": lambda _chain, **_ignored: ({
                    "block_number": 20_000_000,
                    "block_timestamp": "2026-08-02T11:59:45Z",
                    "chain_id": "0x1",
                    "block_header": {
                        "number": hex(20_000_000),
                        "hash": (
                            "0xd24fd73f794058a3807db926d8898c6481e902b7edb91ce0d"
                            "479d6760f276183"
                        ),
                        "parent_hash": "0x" + "a" * 64,
                        "timestamp": hex(int(datetime(
                            2026, 8, 2, 11, 59, 45, tzinfo=timezone.utc
                        ).timestamp())),
                        "base_fee_per_gas": "0x64",
                        "gas_used": "0x1",
                        "gas_limit": "0x2",
                    },
                } if supported_rpc_unavailable else {
                    "block_number": 123,
                    "block_timestamp": "2026-08-02T11:59:45Z",
                }),
                "executor_factory": ThreadPoolExecutor,
                # Thread workers cannot close the parent process lock. This is
                # a test-only transport seam; production keeps the fork FD gate.
                "child_close_fds": (),
            })
            try:
                return real_collect(universe, **kwargs)
            except BaseException as error:
                collection_errors.append(repr(error))
                raise

        def attach_then_optionally_delete(*args, **kwargs):
            result = real_attach(*args, **kwargs)
            if delete_typed_manifest_after_attach:
                (
                    fixture.data_dir
                    / "raw/route-cohort/manual-joint/typed-manifest.json"
                ).unlink()
            return result

        def counted_generation(*args, **kwargs):
            value = real_generation(*args, **kwargs)
            generation_calls.append(value)
            return value

        def capture_publish_error(*args, **kwargs):
            try:
                return real_publish_shadow(*args, **kwargs)
            except BaseException as error:
                publication_errors.append(repr(error))
                raise

        original_write = route_shadow_inputs.write_run_universe

        def install_then_mutate_original(root, run_id, universe, manifest):
            installed = original_write(root, run_id, universe, manifest)
            universe["routes"].clear()
            universe["selected_legs"].clear()
            return installed

        monotonic_start = 1_000_000_000
        monotonic_samples = iter((
            monotonic_start,
            monotonic_start,
            monotonic_start + 2_000_000_000,
            monotonic_start + 2_100_000_000,
            monotonic_start + 2_200_000_000,
            monotonic_start + 2_300_000_000,
            monotonic_start + 3_000_000_000,
        )) if late_response else None

        trace_profile_path = Path(self.temporary.name) / "trace-private.json"
        rpc_requests = []

        class TimeoutOpener:
            addheaders = []

            def open(self, request, timeout):
                rpc_requests.append((request.data, timeout))
                raise TimeoutError("RUNNER-TRACE-SECRET")

        trace_profile = {
            "schema": "route_cost_trace_rpc_profile/v1",
            "profile_id": "runner-trace-mainnet",
            "endpoint_id": "runner-ethereum-trace",
            "rpc_url": "https://runner-trace-secret.invalid/private-url",
            "authorization": "Bearer RUNNER-TRACE-SECRET",
        }
        if supported_rpc_unavailable:
            trace_profile_path.write_bytes(
                json.dumps(
                    trace_profile, ensure_ascii=False, sort_keys=True,
                    separators=(",", ":"),
                ).encode("utf-8") + b"\n"
            )
            trace_profile_path.chmod(0o600)

        profile_environment = (
            {"MARKET_ROUTE_TRACE_RPC_PROFILE": str(trace_profile_path)}
            if supported_rpc_unavailable else {}
        )
        attach_context = (
            nullcontext()
            if supported_rpc_unavailable
            else patch.object(
                collection_module,
                "attach_typed_source_lineage",
                side_effect=attach_then_optionally_delete,
            )
        )
        publication_context = (
            nullcontext()
            if supported_rpc_unavailable
            else patch.object(
                publication_module,
                "publish_shadow_result",
                side_effect=capture_publish_error,
            )
        )
        with patch.dict(
            os.environ, profile_environment, clear=True
        ), patch(
            "scripts.route_cost_collector.urllib.request.build_opener",
            return_value=TimeoutOpener(),
        ), patch.object(
            run_route_shadow,
            "load_committed_route_shadow_authority",
            side_effect=(enabled, enabled),
        ), patch.object(
            route_shadow_inputs, "PROJECT_ROOT", fixture.project_root
        ), patch.object(
            route_shadow_inputs,
            "current_source_generation",
            side_effect=counted_generation,
        ), patch.object(
            route_shadow_inputs,
            "write_run_universe",
            side_effect=install_then_mutate_original,
        ), patch.object(
            collection_module,
            "collect_route_cohort",
            side_effect=collect_with_local_workers,
        ), attach_context, publication_context, patch.object(
            run_route_shadow,
            "_new_manual_run_id",
            return_value="manual-joint",
        ), patch.object(
            run_route_shadow,
            "_route_cost_registry_snapshot",
            side_effect=registry_snapshot_with_drift,
        ), patch.object(
            run_route_shadow, "_trusted_utc_now", return_value=NOW
        ), patch.object(
            run_route_shadow.time,
            "monotonic_ns",
            side_effect=(
                (lambda: next(monotonic_samples))
                if monotonic_samples is not None
                else time.monotonic_ns
            ),
        ):
            result = run_shadow_once(self.data_dir, expected_phase="full")

        ledger = load_run_ledger(self.data_dir, "manual-joint")
        self.assertEqual(result["status"], "terminal")
        self.assertEqual(public_latest.read_bytes(), b"public-route-sentinel\n")
        if registry_drift_checkpoint is not None:
            self.assertEqual(
                ledger["verification"]["primary_failure_class"],
                "source_generation_drift",
            )
            self.assertEqual(
                ledger["verification"]["source_generation_error_count"], 1
            )
            self.assertEqual(
                len(registry_snapshot_calls), registry_drift_checkpoint + 1
            )
            self.assertEqual(
                len(generation_calls), registry_drift_checkpoint - 1
            )
            self.assertIsNone(ledger["terminal"]["joint_pointer_sha256"])
            self.assertFalse((shadow_root / "latest.json").exists())
            return
        if registry_error_checkpoint is not None:
            self.assertEqual(
                ledger["verification"]["primary_failure_class"],
                "source_generation_drift",
            )
            self.assertEqual(
                ledger["verification"]["source_generation_error_count"], 1
            )
            self.assertIsNone(ledger["terminal"]["joint_pointer_sha256"])
            self.assertFalse((shadow_root / "latest.json").exists())
            return
        if delete_typed_manifest_after_attach:
            self.assertEqual(len(generation_calls), 2)
            self.assertEqual(len(registry_snapshot_calls), 3)
            self.assertEqual(
                ledger["verification"]["primary_failure_class"],
                "lineage_invalid",
            )
            self.assertEqual(ledger["terminal"]["outcome"], "failed")
            self.assertIsNone(ledger["terminal"]["joint_pointer_sha256"])
            self.assertFalse((shadow_root / "latest.json").exists())
            self.assertFalse(publication_errors)
            return
        self.assertEqual(len(generation_calls), 4)
        self.assertEqual(len(registry_snapshot_calls), 5)
        self.assertEqual(
            ledger["verification"]["last_completed_stage"],
            "joint_pointer",
            {
                "ledger": ledger,
                "collection_errors": collection_errors,
                "publication_errors": publication_errors,
            },
        )
        self.assertIsNotNone(ledger["verification"]["typed_source_manifest_sha256"])
        self.assertIsNotNone(ledger["verification"]["route_cost_evidence_sha256"])
        self.assertIsNotNone(ledger["terminal"]["joint_pointer_sha256"])
        self.assertEqual(
            load_latest_route_cohort(self.data_dir / "routes/core")["cohort"][
                "raw_evidence_run_id"
            ],
            "manual-joint",
        )
        self.assertEqual(
            load_latest_shadow_result(self.data_dir / "routes/shadow")["pointer"][
                "run_id"
            ],
            "manual-joint",
        )
        self.assertFalse(publication_errors)
        if supported_rpc_unavailable:
            self.assertEqual(len(rpc_requests), 1)
            requests = json.loads(rpc_requests[0][0].decode("utf-8"))
            self.assertEqual([row["id"] for row in requests], list(range(1, 12)))
            self.assertEqual(
                [row["method"] for row in requests[:3]],
                ["eth_chainId", "eth_getBlockByNumber", "eth_feeHistory"],
            )
            self.assertEqual(
                [row["method"] for row in requests[3:]],
                [
                    "eth_getCode", "eth_getCode", "eth_call", "eth_getCode",
                    "eth_call", "eth_call", "eth_getCode", "eth_getCode",
                ],
            )
            self.assertGreater(rpc_requests[0][1], 0)
            self.assertLessEqual(rpc_requests[0][1], 10)
            sidecar_path = (
                shadow_root / "runs/manual-joint/route-cost-evidence.json"
            )
            sidecar_bytes = sidecar_path.read_bytes()
            sidecar = json.loads(sidecar_bytes)
            self.assertEqual(sidecar["selected_market_count"], 1)
            self.assertEqual(
                sidecar["selected_markets"][0]["structural_support_status"],
                "supported",
            )
            self.assertEqual(sidecar["transcript_count"], 10)
            self.assertEqual(
                {(row["status"], row["completed_stage"], row["reason_code"])
                 for row in sidecar["transcripts"]},
                {("unavailable", "none", "rpc_unavailable")},
            )
            self.assertEqual(sidecar["chain_evidence"], [])
            self.assertEqual(sidecar["market_evidence"], [])
            self.assertIsNone(sidecar["native_price_evidence"])
            core_state_ids = {
                row["core_pool_state_id"] for row in sidecar["transcripts"]
            }
            self.assertEqual(len(core_state_ids), 1)
            self.assertNotIn(None, core_state_ids)
            self.assertEqual(
                sidecar["submission_connector_profile_identity"]["status"],
                "missing",
            )
            self.assertEqual(
                sidecar["submission_policy_snapshot"]["reason_code"],
                "submission_connector_missing",
            )
            core = load_latest_route_cohort(
                self.data_dir / "routes/core"
            )["cohort"]
            dex_leg = next(
                row for row in core["legs"] if row["market_type"] == "dex"
            )
            pool_member = next(
                row for row in dex_leg["typed_source_lineage"]["members"]
                if row["role"] == "dex_pool_state"
            )
            self.assertEqual(pool_member["status"], "observed")
            self.assertEqual(
                {row["core_pool_state_sha256"] for row in sidecar["transcripts"]},
                {pool_member["sha256"]},
            )
            typed_manifest_bytes = (
                self.data_dir
                / "raw/route-cohort/manual-joint/typed-manifest.json"
            ).read_bytes()
            typed_manifest_sha = hashlib.sha256(typed_manifest_bytes).hexdigest()
            self.assertEqual(
                ledger["verification"]["typed_source_manifest_sha256"],
                typed_manifest_sha,
            )
            self.assertEqual(
                ledger["terminal"]["typed_source_manifest_sha256"],
                typed_manifest_sha,
            )
            shadow = load_latest_shadow_result(shadow_root)
            sidecar_sha = hashlib.sha256(sidecar_bytes).hexdigest()
            self.assertEqual(
                shadow["pointer"]["route_cost_evidence_sha256"], sidecar_sha
            )
            self.assertEqual(
                shadow["audit"]["route_cost_evidence_sha256"], sidecar_sha
            )
            self.assertEqual(
                ledger["verification"]["route_cost_evidence_sha256"],
                sidecar_sha,
            )
            self.assertEqual(
                ledger["terminal"]["route_cost_evidence_sha256"], sidecar_sha
            )
            self.assertEqual(
                ledger["terminal"]["joint_pointer_sha256"],
                shadow["pointer_sha256"],
            )
            persisted = b"".join(
                path.read_bytes()
                for path in self.data_dir.rglob("*") if path.is_file()
            )
            self.assertNotIn(b"RUNNER-TRACE-SECRET", persisted)
            self.assertNotIn(b"runner-trace-secret.invalid", persisted)
            self.assertFalse(any(
                row["status"] == "observed" for row in sidecar["transcripts"]
            ))
        if late_response:
            core = load_latest_route_cohort(
                self.data_dir / "routes/core"
            )["cohort"]
            self.assertEqual(
                core["collection_started_at"],
                "2026-08-02T12:00:00Z",
            )
            self.assertEqual(
                core["collection_completed_at"],
                "2026-08-02T12:00:02Z",
            )
            self.assertEqual(ledger["terminal"]["finished_at"], NOW.replace(
                second=3
            ).isoformat().replace("+00:00", "Z"))
            self.assertEqual(ledger["terminal"]["duration_seconds"], "3")

    def test_enabled_owner_runs_the_real_private_joint_pipeline(self):
        self._exercise_real_private_joint_pipeline()

    def test_owned_pipeline_does_not_use_the_legacy_unavailable_cost_builder(self):
        with patch(
            "scripts.route_cost_evidence."
            "build_unavailable_route_cost_evidence_manifest",
            side_effect=AssertionError(
                "owned Shadow runs must use the sealed production collector"
            ),
        ):
            self._exercise_real_private_joint_pipeline()

    def test_supported_retained_uni_weth_timeout_reaches_real_joint_publication(self):
        self._exercise_real_private_joint_pipeline(
            supported_rpc_unavailable=True
        )

    def test_owned_timestamps_follow_monotonic_time_after_the_sole_utc_sample(self):
        self._exercise_real_private_joint_pipeline(late_response=True)

    def test_deleted_typed_manifest_closes_the_run_without_a_joint_pointer(self):
        self._exercise_real_private_joint_pipeline(
            delete_typed_manifest_after_attach=True
        )

    def test_adapter_registry_drift_at_checkpoint_one_fails_closed(self):
        self._exercise_real_private_joint_pipeline(registry_drift_checkpoint=1)

    def test_adapter_registry_drift_at_checkpoint_two_fails_closed(self):
        self._exercise_real_private_joint_pipeline(registry_drift_checkpoint=2)

    def test_adapter_registry_drift_at_checkpoint_three_fails_closed(self):
        self._exercise_real_private_joint_pipeline(registry_drift_checkpoint=3)

    def test_adapter_registry_drift_at_checkpoint_four_fails_closed(self):
        self._exercise_real_private_joint_pipeline(registry_drift_checkpoint=4)

    def test_connector_registry_drift_at_checkpoint_one_fails_closed(self):
        self._exercise_real_private_joint_pipeline(
            registry_drift_checkpoint=1,
            registry_drift_field="connector_key_registry_sha256",
        )

    def test_connector_registry_drift_at_checkpoint_two_fails_closed(self):
        self._exercise_real_private_joint_pipeline(
            registry_drift_checkpoint=2,
            registry_drift_field="connector_key_registry_sha256",
        )

    def test_connector_registry_drift_at_checkpoint_three_fails_closed(self):
        self._exercise_real_private_joint_pipeline(
            registry_drift_checkpoint=3,
            registry_drift_field="connector_key_registry_sha256",
        )

    def test_connector_registry_drift_at_checkpoint_four_fails_closed(self):
        self._exercise_real_private_joint_pipeline(
            registry_drift_checkpoint=4,
            registry_drift_field="connector_key_registry_sha256",
        )

    def test_registry_becoming_unreadable_at_checkpoint_fails_as_generation_drift(self):
        self._exercise_real_private_joint_pipeline(
            registry_error_checkpoint=1
        )

    def test_canary_projects_full_universe_to_exact_literal_tokens(self):
        allowlist = sorted(run_route_shadow._CANARY_TOKEN_ALLOWLIST)
        routes = []
        legs = []
        for index, symbol in enumerate(allowlist + ["UNI"]):
            buy = "cex:{}:buy".format(symbol)
            sell = "cex:{}:sell".format(symbol)
            routes.append({
                "route_id": "route-{}".format(index),
                "token_symbol": symbol,
                "buy_market_id": buy,
                "sell_market_id": sell,
            })
            legs.extend((
                {"market_id": buy, "token_symbol": symbol},
                {"market_id": sell, "token_symbol": symbol},
            ))
        universe = {
            "schema": "route_universe/v1",
            "routes": routes,
            "selected_legs": legs,
        }

        projected = run_route_shadow._project_canary_universe(universe)

        self.assertEqual(
            {route["token_symbol"] for route in projected["routes"]},
            set(allowlist),
        )
        self.assertNotIn(
            "UNI", {leg["token_symbol"] for leg in projected["selected_legs"]}
        )
        referenced = {
            route[field]
            for route in projected["routes"]
            for field in ("buy_market_id", "sell_market_id")
        }
        self.assertEqual(
            {leg["market_id"] for leg in projected["selected_legs"]},
            referenced,
        )

    def test_canary_projection_rejects_a_missing_literal_token(self):
        symbols = sorted(run_route_shadow._CANARY_TOKEN_ALLOWLIST - {"PEPE"})
        routes = [
            {
                "route_id": "route-{}".format(index),
                "token_symbol": symbol,
                "buy_market_id": "{}-a".format(symbol),
                "sell_market_id": "{}-b".format(symbol),
            }
            for index, symbol in enumerate(symbols)
        ]
        legs = [
            {"market_id": route[field], "token_symbol": route["token_symbol"]}
            for route in routes
            for field in ("buy_market_id", "sell_market_id")
        ]
        with self.assertRaisesRegex(ValueError, "canary.*incomplete"):
            run_route_shadow._project_canary_universe({
                "routes": routes, "selected_legs": legs,
            })

    def test_collection_and_ledger_ancestors_must_not_be_symlinks(self):
        external = Path(self.temporary.name) / "external"
        external.mkdir()
        data = Path(self.temporary.name) / "unsafe-data"
        data.symlink_to(external, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, "ancestry|unsafe|changed"):
            run_route_shadow._open_collection_lock(data)
        with self.assertRaisesRegex(ValueError, "ancestry|unsafe|changed"):
            run_route_shadow._ensure_ledger_root(data)
        self.assertFalse((external / "collection/collection.lock").exists())
        self.assertFalse((external / "routes/shadow/ledger").exists())

    def test_fresh_collection_directory_can_create_and_reopen_one_lock_inode(self):
        fresh_data = Path(self.temporary.name) / "fresh-data"
        descriptor = run_route_shadow._open_collection_lock(fresh_data)
        try:
            opened = os.fstat(descriptor)
            path = os.stat(
                fresh_data / "collection/collection.lock",
                follow_symlinks=False,
            )
            self.assertEqual(
                (opened.st_dev, opened.st_ino),
                (path.st_dev, path.st_ino),
            )
            fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
        finally:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_UN)
            finally:
                os.close(descriptor)


class LedgerContractTests(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.data_dir = Path(self.temporary.name) / "data"

    def test_systemd_normalization_matrix_is_literal(self):
        cases = (
            (("success", "exited", "0"), "success"),
            (("timeout", "killed", "15"), "timeout"),
            (("oom-kill", "killed", "9"), "oom"),
            (("signal", "killed", "TERM"), "failed"),
            (("watchdog", "killed", "6"), "failed"),
            (("", "", ""), "unexplained"),
            (("future", "exited", "0"), "unexplained"),
        )
        for raw, expected in cases:
            with self.subTest(raw=raw):
                self.assertEqual(normalize_systemd_result(*raw), expected)

    def test_reconcile_without_started_creates_terminal_service_not_started(self):
        result = reconcile_shadow_run(
            self.data_dir,
            run_id="1" * 32,
            dispatch_id="2" * 32,
            service_result="signal",
            exit_code="killed",
            exit_status="TERM",
        )
        self.assertEqual(result["terminal"]["lock_acquired"], None)
        self.assertEqual(
            result["terminal"]["reason_code"],
            "pre_started_lock_state_unknown",
        )
        self.assertEqual(result["service"]["normalized_outcome"], "unexplained")
        self.assertEqual(
            sorted(
                path.name
                for path in (
                    self.data_dir / "routes/shadow/ledger" / ("1" * 32)
                ).iterdir()
            ),
            ["service.json", "terminal.json"],
        )

    def test_reconcile_started_only_timeout_installs_full_failure_closure(self):
        run_id = "1" * 32
        dispatch_id = "2" * 32
        run_dir = self.data_dir / "routes/shadow/ledger" / run_id
        started = run_route_shadow.validate_started({
            "schema": run_route_shadow.RUN_STARTED_SCHEMA,
            "run_id": run_id,
            "dispatch_id": dispatch_id,
            "phase": "canary",
            "phase_state_sha256": hashlib.sha256(
                b"route-shadow-phase/implicit-canary/v1\n"
            ).hexdigest(),
            "phase_transition_id": None,
            "invocation_id": run_id,
            "started_at": "2026-08-02T12:00:00Z",
            "boot_id": "3" * 32,
            "monotonic_ns": 1,
        })
        run_route_shadow._write_member(run_dir, "started.json", started)

        result = reconcile_shadow_run(
            self.data_dir,
            run_id=run_id,
            dispatch_id=dispatch_id,
            service_result="timeout",
            exit_code="killed",
            exit_status="15",
        )
        replay = load_run_ledger(self.data_dir, run_id)
        self.assertEqual(result["terminal"]["outcome"], "timeout")
        self.assertEqual(result["terminal"]["duration_status"], "not_evaluated")
        self.assertIsNone(result["terminal"]["duration_seconds"])
        self.assertEqual(
            replay["verification"]["primary_failure_class"], "timeout"
        )
        self.assertEqual(replay["service"]["normalized_outcome"], "timeout")
        self.assertEqual(
            sorted(path.name for path in run_dir.iterdir()),
            ["service.json", "started.json", "terminal.json", "verification.json"],
        )
        self.assertEqual(
            reconcile_shadow_run(
                self.data_dir,
                run_id=run_id,
                dispatch_id=dispatch_id,
                service_result="timeout",
                exit_code="killed",
                exit_status="15",
            ),
            result,
        )

    def test_reconcile_started_only_oom_installs_full_failure_closure(self):
        run_id = "4" * 32
        dispatch_id = "5" * 32
        run_dir = self.data_dir / "routes/shadow/ledger" / run_id
        started = run_route_shadow.validate_started({
            "schema": run_route_shadow.RUN_STARTED_SCHEMA,
            "run_id": run_id,
            "dispatch_id": dispatch_id,
            "phase": "canary",
            "phase_state_sha256": hashlib.sha256(
                b"route-shadow-phase/implicit-canary/v1\n"
            ).hexdigest(),
            "phase_transition_id": None,
            "invocation_id": run_id,
            "started_at": "2026-08-02T12:00:00Z",
            "boot_id": "6" * 32,
            "monotonic_ns": 1,
        })
        run_route_shadow._write_member(run_dir, "started.json", started)

        result = reconcile_shadow_run(
            self.data_dir,
            run_id=run_id,
            dispatch_id=dispatch_id,
            service_result="oom-kill",
            exit_code="killed",
            exit_status="9",
        )
        replay = load_run_ledger(self.data_dir, run_id)
        self.assertEqual(result["terminal"]["outcome"], "oom")
        self.assertEqual(result["terminal"]["duration_status"], "not_evaluated")
        self.assertIsNone(result["terminal"]["duration_seconds"])
        self.assertEqual(replay["verification"]["primary_failure_class"], "oom")
        self.assertEqual(replay["service"]["normalized_outcome"], "oom")

    def test_unknown_and_candidate_members_fail_closed(self):
        run_dir = self.data_dir / "routes/shadow/ledger/manual-1"
        run_dir.mkdir(parents=True)
        (run_dir / "unexpected.json").write_text("{}", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "unknown|unsafe"):
            load_run_ledger(self.data_dir, "manual-1")
        (run_dir / "unexpected.json").unlink()
        (run_dir / "candidate.json").write_text("{}", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "candidate_contract_not_available"):
            load_run_ledger(self.data_dir, "manual-1")

    def _install_acquired_failure(
        self, run_id="manual-crosswire", *, verification_overrides=None,
        terminal_overrides=None,
    ):
        run_dir = self.data_dir / "routes/shadow/ledger" / run_id
        started = run_route_shadow.validate_started({
            "schema": run_route_shadow.RUN_STARTED_SCHEMA,
            "run_id": run_id,
            "dispatch_id": None,
            "phase": "canary",
            "phase_state_sha256": hashlib.sha256(
                b"route-shadow-phase/implicit-canary/v1\n"
            ).hexdigest(),
            "phase_transition_id": None,
            "invocation_id": None,
            "started_at": "2026-08-02T12:00:00Z",
            "boot_id": "1" * 32,
            "monotonic_ns": 100,
        })
        started_bytes = run_route_shadow._write_member(
            run_dir, "started.json", started
        )
        verification = {
            "schema": run_route_shadow.RUN_VERIFICATION_SCHEMA,
            "run_id": run_id,
            "dispatch_id": None,
            "started_sha256": hashlib.sha256(started_bytes).hexdigest(),
            "verified_at": "2026-08-02T12:00:01Z",
            "primary_failure_class": "transient_collection",
            "collector_process_started_count": 1,
            "collector_process_reaped_count": 1,
            "orphan_process_count": 0,
            "primary_publication_interference_count": 0,
            "core_orphan_count": 0,
            "pointer_interference_count": 0,
            "lineage_error_count": 0,
            "unsafe_path_error_count": 0,
            "source_generation_error_count": 0,
            "resource_limit_error_count": 0,
            "runtime_limit_error_count": 0,
            "last_completed_stage": "core",
            "result_status": "failed",
            "typed_source_manifest_sha256": "2" * 64,
            "route_cost_evidence_sha256": "3" * 64,
            "run_capture_admission_sha256": "4" * 64,
            "run_admission_sha256": "5" * 64,
            "storage_admission_status": "verified",
            "reason_codes": ["transient_collection"],
        }
        verification.update(verification_overrides or {})
        verification = run_route_shadow.validate_verification(verification)
        verification_bytes = run_route_shadow._write_member(
            run_dir, "verification.json", verification
        )
        terminal = {
            "schema": run_route_shadow.RUN_TERMINAL_SCHEMA,
            "run_id": run_id,
            "dispatch_id": None,
            "outcome": "failed",
            "finished_at": "2026-08-02T12:00:02Z",
            "lock_acquired": True,
            "duration_status": "evaluated",
            "duration_seconds": "2",
            "route_cohort_id": "cohort:" + "6" * 64,
            "started_sha256": hashlib.sha256(started_bytes).hexdigest(),
            "verification_sha256": hashlib.sha256(
                verification_bytes
            ).hexdigest(),
            "runtime_evidence_sha256": None,
            "run_capture_admission_sha256": "4" * 64,
            "run_admission_sha256": "5" * 64,
            "storage_admission_status": "verified",
            "typed_source_manifest_sha256": "2" * 64,
            "route_cost_evidence_sha256": "3" * 64,
            "joint_pointer_sha256": None,
            "reason_code": "transient_collection",
        }
        terminal.update(terminal_overrides or {})
        terminal = run_route_shadow.validate_terminal(terminal)
        run_route_shadow._write_member(run_dir, "terminal.json", terminal)
        return run_dir

    def test_loader_rejects_every_verification_terminal_crosswire(self):
        fields = (
            "typed_source_manifest_sha256",
            "route_cost_evidence_sha256",
            "run_capture_admission_sha256",
            "run_admission_sha256",
        )
        for index, field in enumerate(fields):
            with self.subTest(field=field):
                run_id = "manual-crosswire-{}".format(index)
                self._install_acquired_failure(
                    run_id,
                    terminal_overrides={field: "f" * 64},
                )
                with self.assertRaisesRegex(
                    ValueError, "verification|terminal|lineage|storage|differ"
                ):
                    load_run_ledger(self.data_dir, run_id)

        run_id = "manual-crosswire-storage"
        self._install_acquired_failure(
            run_id,
            verification_overrides={
                "primary_failure_class": "unexplained",
                "run_capture_admission_sha256": None,
                "run_admission_sha256": None,
                "storage_admission_status": "not_evaluated",
                "reason_codes": ["storage_not_evaluated", "unexplained"],
            },
        )
        with self.assertRaisesRegex(
            ValueError, "verification|terminal|lineage|storage|differ"
        ):
            load_run_ledger(self.data_dir, run_id)

    def test_validator_rejects_evidence_free_success_and_bad_service_identity(self):
        verification = {
            "schema": run_route_shadow.RUN_VERIFICATION_SCHEMA,
            "run_id": "manual-invalid",
            "dispatch_id": None,
            "started_sha256": "1" * 64,
            "verified_at": "2026-08-02T12:00:01Z",
            "primary_failure_class": "none",
            **{field: 0 for field in (
                "collector_process_started_count",
                "collector_process_reaped_count", "orphan_process_count",
                "primary_publication_interference_count", "core_orphan_count",
                "pointer_interference_count", "lineage_error_count",
                "unsafe_path_error_count", "source_generation_error_count",
                "resource_limit_error_count", "runtime_limit_error_count",
            )},
            "last_completed_stage": "none",
            "result_status": "verified",
            "typed_source_manifest_sha256": None,
            "route_cost_evidence_sha256": None,
            "run_capture_admission_sha256": None,
            "run_admission_sha256": None,
            "storage_admission_status": "not_evaluated",
            "reason_codes": ["storage_not_evaluated"],
        }
        with self.assertRaisesRegex(ValueError, "successful|inconsistent"):
            run_route_shadow.validate_verification(verification)

        service = {
            "schema": run_route_shadow.SERVICE_SCHEMA,
            "service_kind": "worker",
            "dispatch_id": "2" * 32,
            "run_id": "1" * 32,
            "attempt_id": "1" * 32,
            "unit_name": "cex-dex-route-shadow-worker@{}.service".format(
                "1" * 32
            ),
            "invocation_id": "1" * 32,
            "terminal_sha256": "3" * 64,
            "runtime_evidence_sha256": None,
            "service_result": "success",
            "exit_code": "exited",
            "exit_status": "0",
            "normalized_outcome": "success",
            "started_at": "2026-08-02T12:00:00Z",
            "finished_at": "2026-08-02T12:00:01Z",
            "reason_code": None,
        }
        for field, invalid in (
            ("dispatch_id", int("2" * 32)),
            ("unit_name", service["unit_name"] + "\n"),
            ("invocation_id", "../unsafe"),
            ("finished_at", "2026-08-02T11:59:59Z"),
            ("reason_code", "arbitrary"),
        ):
            with self.subTest(field=field):
                mutated = dict(service)
                mutated[field] = invalid
                with self.assertRaises(ValueError):
                    run_route_shadow.validate_service(mutated)


if __name__ == "__main__":
    unittest.main()
