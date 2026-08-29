import fcntl
import hashlib
import io
import json
import os
import stat
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from scripts import uniswap_v3_launch as launch
from scripts.run_collection_cycle import build_step_commands


class LaunchFilesystemTest(unittest.TestCase):
    SNAPSHOT_ID = "20260829T000000Z-v3"

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.data_dir = self.root / "published"
        self.data_dir.mkdir(mode=0o700)
        self.originals = {}
        for index, name in enumerate(launch.PUBLIC_BUNDLE_NAMES[:-1]):
            payload = "old-{}-{}\n".format(index, name).encode("ascii")
            path = self.data_dir / name
            path.write_bytes(payload)
            os.chmod(path, 0o640 + (index % 2) * 4)
            self.originals[name] = payload

    def tearDown(self):
        self.temporary.cleanup()

    def _write_sidecar(self, payload=b'{"old":true}\n'):
        path = self.data_dir / launch.PUBLIC_BUNDLE_NAMES[-1]
        path.write_bytes(payload)
        os.chmod(path, 0o600)
        self.originals[path.name] = payload

    def _make_stage(self, baseline, basename="candidate"):
        stage = self.root / basename
        stage.mkdir(mode=0o700)
        for index, name in enumerate(launch.PUBLIC_BUNDLE_NAMES):
            payload = "new-{}-{}\n".format(index, name).encode("ascii")
            (stage / name).write_bytes(payload)
        receipt = {"depth_snapshot_id": self.SNAPSHOT_ID}
        receipt_bytes = launch.canonical_json_bytes(receipt)
        (stage / launch.PUBLIC_BUNDLE_NAMES[-1]).write_bytes(receipt_bytes)
        raw_dir = stage / "raw/dex-depth" / self.SNAPSHOT_ID
        raw_dir.mkdir(parents=True, mode=0o700)
        (raw_dir / launch.RAW_RECEIPT_NAME).write_bytes(receipt_bytes)
        return stage

    def test_snapshot_records_fixed_order_hash_mode_and_absent_sidecar(self):
        manifest = launch.snapshot_public_bundle(self.data_dir)

        self.assertEqual(manifest["order"], list(launch.PUBLIC_BUNDLE_NAMES))
        self.assertEqual(
            manifest["files"][launch.PUBLIC_BUNDLE_NAMES[-1]],
            {"exists": False},
        )
        first = manifest["files"][launch.PUBLIC_BUNDLE_NAMES[0]]
        self.assertEqual(first["sha256"], hashlib.sha256(
            self.originals[launch.PUBLIC_BUNDLE_NAMES[0]]
        ).hexdigest())
        self.assertEqual(first["mode"], 0o640)

    def test_real_subprocess_help_and_plan_are_runnable_and_project_read_only(self):
        cache_root = self.root / "bytecode-cache"
        environment = dict(os.environ)
        environment.pop("PYTHONDONTWRITEBYTECODE", None)
        environment["PYTHONPYCACHEPREFIX"] = str(cache_root)
        script = Path(__file__).resolve().parents[1] / "scripts/uniswap_v3_launch.py"

        help_result = subprocess.run(
            [sys.executable, str(script), "--help"],
            cwd=script.parents[1],
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(help_result.returncode, 0, help_result.stderr)
        self.assertIn("usage:", help_result.stdout)

        absent_data = self.root / "absent-data"
        absent_launch = self.root / "absent-launch"
        absent_stage = self.root / "absent-stage"
        plan_result = subprocess.run(
            [
                sys.executable,
                str(script),
                "preflight",
                "--data-dir", str(absent_data),
                "--launch-dir", str(absent_launch),
                "--stage-dir", str(absent_stage),
                "--target-sha", "a" * 40,
                "--previous-app-sha", "b" * 40,
            ],
            cwd=script.parents[1],
            env=environment,
            capture_output=True,
            text=True,
            check=False,
        )
        self.assertEqual(plan_result.returncode, 0, plan_result.stderr)
        self.assertIn('"execute":false', plan_result.stdout)
        self.assertFalse(absent_data.exists())
        self.assertFalse(absent_launch.exists())
        self.assertFalse(absent_stage.exists())
        project_cache = cache_root / str(script.parents[1]).lstrip(os.sep)
        self.assertFalse(project_cache.exists())

    def test_stage_validator_reads_the_runner_dex_price_output_path(self):
        stage = self.root / "candidate-path-integration"
        stage.mkdir(mode=0o700)
        commands = build_step_commands(
            "dex_depth",
            publish_local=True,
            python_executable=sys.executable,
            data_dir=stage,
            require_uniswap_v3_exact_validation=True,
        )
        dex_command = next(
            command
            for name, command in commands
            if name == "dex_depth"
        )
        tvl_path = Path(dex_command[dex_command.index("--tvl-csv") + 1])
        tvl_path.parent.mkdir(mode=0o700)
        tvl_path.write_text("pool_id\npool-from-processed\n")
        (stage / "dex_depth_latest.csv").write_text("market_id\nmarket\n")
        (stage / "dex_execution_cost_latest.csv").write_text(
            "market_id\nmarket\n"
        )
        receipt = {"depth_snapshot_id": self.SNAPSHOT_ID}
        receipt_bytes = launch.canonical_json_bytes(receipt)
        (stage / "uniswap_v3_exact_latest.json").write_bytes(receipt_bytes)
        raw_dir = stage / "raw/dex-depth" / self.SNAPSHOT_ID
        raw_dir.mkdir(parents=True, mode=0o700)
        (raw_dir / launch.RAW_RECEIPT_NAME).write_bytes(receipt_bytes)

        with patch.object(
            launch,
            "validate_uniswap_v3_exact_candidate",
            return_value=receipt,
        ) as validate_candidate, patch.object(
            launch,
            "validate_uniswap_v3_exact_public_receipt",
            return_value=receipt,
        ):
            self.assertEqual(launch._validate_stage_candidate(stage), receipt)

        inventory = validate_candidate.call_args.args[0]
        self.assertEqual(inventory, [{"pool_id": "pool-from-processed"}])

    def test_snapshot_rejects_symlink_special_file_and_symlink_root(self):
        target = self.data_dir / launch.PUBLIC_BUNDLE_NAMES[0]
        target.unlink()
        target.symlink_to(self.data_dir / launch.PUBLIC_BUNDLE_NAMES[1])
        with self.assertRaisesRegex(ValueError, "regular non-symlink"):
            launch.snapshot_public_bundle(self.data_dir)

        target.unlink()
        os.mkfifo(target)
        with self.assertRaisesRegex(ValueError, "regular non-symlink"):
            launch.snapshot_public_bundle(self.data_dir)

        alias = self.root / "published-alias"
        alias.symlink_to(self.data_dir, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, "regular non-symlink"):
            launch.snapshot_public_bundle(alias)

    def test_snapshot_bounded_read_rejects_oversized_file(self):
        path = self.data_dir / launch.PUBLIC_BUNDLE_NAMES[0]
        with path.open("wb") as handle:
            handle.truncate(launch.MAX_PUBLIC_FILE_BYTES + 1)
        with self.assertRaisesRegex(ValueError, "bounded"):
            launch.snapshot_public_bundle(self.data_dir)

    def test_verify_bundle_state_rejects_byte_mode_presence_and_manifest_drift(self):
        baseline = launch.snapshot_public_bundle(self.data_dir)
        cases = ("bytes", "mode", "presence")
        for case in cases:
            with self.subTest(case=case):
                with tempfile.TemporaryDirectory(dir=self.root) as clone_name:
                    clone = Path(clone_name)
                    for name, payload in self.originals.items():
                        (clone / name).write_bytes(payload)
                    for name, record in baseline["files"].items():
                        if record["exists"]:
                            os.chmod(clone / name, record["mode"])
                    if case == "bytes":
                        (clone / launch.PUBLIC_BUNDLE_NAMES[0]).write_bytes(b"drift\n")
                    elif case == "mode":
                        os.chmod(clone / launch.PUBLIC_BUNDLE_NAMES[0], 0o600)
                    else:
                        (clone / launch.PUBLIC_BUNDLE_NAMES[-1]).write_bytes(b"now\n")
                    with self.assertRaisesRegex(ValueError, "baseline.*drift"):
                        launch.verify_bundle_state(clone, baseline, state="baseline")

        malformed = dict(baseline)
        malformed["order"] = list(reversed(malformed["order"]))
        with self.assertRaisesRegex(ValueError, "manifest"):
            launch.verify_bundle_state(self.data_dir, malformed, state="baseline")

    def test_backup_is_private_canonical_and_preserves_original_bytes_and_modes(self):
        self._write_sidecar()
        launch_dir = self.root / "launch"

        result = launch.create_backup(
            self.data_dir,
            launch_dir,
            target_sha="a" * 40,
            previous_app_sha="b" * 40,
        )

        self.assertEqual(stat.S_IMODE(launch_dir.stat().st_mode), 0o700)
        backup_dir = launch_dir / "backup"
        self.assertEqual(stat.S_IMODE(backup_dir.stat().st_mode), 0o700)
        for name in launch.PUBLIC_BUNDLE_NAMES:
            backup_path = backup_dir / name
            self.assertEqual(backup_path.read_bytes(), self.originals[name])
            self.assertEqual(stat.S_IMODE(backup_path.stat().st_mode), 0o600)
            self.assertEqual(
                result["baseline"]["files"][name]["mode"],
                stat.S_IMODE((self.data_dir / name).stat().st_mode),
            )
        manifest_path = backup_dir / "manifest.json"
        self.assertEqual(stat.S_IMODE(manifest_path.stat().st_mode), 0o600)
        self.assertEqual(
            manifest_path.read_bytes(),
            launch.canonical_json_bytes(result),
        )
        self.assertNotIn(str(self.data_dir), manifest_path.read_text())

    def test_backup_records_absent_sidecar_without_creating_an_empty_backup(self):
        launch_dir = self.root / "launch"
        result = launch.create_backup(
            self.data_dir,
            launch_dir,
            target_sha="a" * 40,
            previous_app_sha="b" * 40,
        )
        sidecar = launch.PUBLIC_BUNDLE_NAMES[-1]
        self.assertEqual(result["baseline"]["files"][sidecar], {"exists": False})
        self.assertFalse((launch_dir / "backup" / sidecar).exists())

    def test_backup_refuses_reuse_and_invalid_sha(self):
        launch_dir = self.root / "launch"
        with self.assertRaisesRegex(ValueError, "SHA"):
            launch.create_backup(
                self.data_dir,
                launch_dir,
                target_sha="main",
                previous_app_sha="b" * 40,
            )
        launch.create_backup(
            self.data_dir,
            launch_dir,
            target_sha="a" * 40,
            previous_app_sha="b" * 40,
        )
        with self.assertRaises((FileExistsError, ValueError)):
            launch.create_backup(
                self.data_dir,
                launch_dir,
                target_sha="a" * 40,
                previous_app_sha="b" * 40,
            )

    def test_prepare_stage_inputs_requires_fresh_sibling_and_copies_only_inputs(self):
        (self.data_dir / "market_facts.sqlite3").write_bytes(b"sqlite\n")
        (self.data_dir / "dex_pool_volume_daily.csv").write_bytes(b"date\n")
        baseline = launch.snapshot_public_bundle(self.data_dir)
        stage = self.root / "candidate"

        result = launch.prepare_stage_inputs(self.data_dir, stage, baseline)

        processed = launch.processed_dir_for(stage)
        self.assertEqual(stat.S_IMODE(stage.stat().st_mode), 0o700)
        self.assertEqual(stat.S_IMODE(processed.stat().st_mode), 0o700)
        self.assertEqual((stage / "market_facts.sqlite3").read_bytes(), b"sqlite\n")
        self.assertEqual((stage / "dex_pool_volume_daily.csv").read_bytes(), b"date\n")
        self.assertEqual(
            (stage / launch.PUBLIC_BUNDLE_NAMES[0]).read_bytes(),
            self.originals[launch.PUBLIC_BUNDLE_NAMES[0]],
        )
        self.assertEqual(set(path.name for path in stage.iterdir()), {
            "market_facts.sqlite3",
            "dex_pool_volume_daily.csv",
            launch.PUBLIC_BUNDLE_NAMES[0],
        })
        self.assertFalse((stage / "raw").exists())
        self.assertEqual(list(processed.iterdir()), [])
        self.assertNotEqual(result["stage_root_sha256"], result["processed_root_sha256"])

    def test_prepare_stage_inputs_refuses_live_alias_descendant_existing_and_drift(self):
        (self.data_dir / "market_facts.sqlite3").write_bytes(b"sqlite\n")
        (self.data_dir / "dex_pool_volume_daily.csv").write_bytes(b"date\n")
        baseline = launch.snapshot_public_bundle(self.data_dir)

        unsafe = (
            self.data_dir / "stage",
            self.root / "existing",
            self.root / "alias",
        )
        unsafe[1].mkdir()
        unsafe[2].symlink_to(self.data_dir, target_is_directory=True)
        for path in unsafe:
            with self.subTest(path=path), self.assertRaises((ValueError, FileExistsError)):
                launch.prepare_stage_inputs(self.data_dir, path, baseline)

        (self.data_dir / launch.PUBLIC_BUNDLE_NAMES[0]).write_bytes(b"drift\n")
        with self.assertRaisesRegex(ValueError, "baseline.*drift"):
            launch.prepare_stage_inputs(self.data_dir, self.root / "fresh", baseline)
        self.assertFalse((self.root / "fresh").exists())

    def test_promote_uses_baseline_cas_and_fixed_five_file_forward_bundle(self):
        self._write_sidecar()
        baseline = launch.snapshot_public_bundle(self.data_dir)
        stage = self._make_stage(baseline)

        with patch.object(launch, "_validate_stage_candidate") as validate:
            promotion = launch.promote_stage(self.data_dir, stage, baseline)

        validate.assert_called_once_with(stage)
        self.assertEqual(
            [
                (self.data_dir / name).read_bytes()
                for name in launch.PUBLIC_BUNDLE_NAMES
            ],
            [
                (stage / name).read_bytes()
                for name in launch.PUBLIC_BUNDLE_NAMES
            ],
        )
        launch.verify_bundle_state(self.data_dir, promotion["promoted"], state="promoted")

    def test_promote_refuses_live_drift_and_stage_symlink_before_atomic_helper(self):
        self._write_sidecar()
        baseline = launch.snapshot_public_bundle(self.data_dir)
        stage = self._make_stage(baseline)
        (self.data_dir / launch.PUBLIC_BUNDLE_NAMES[0]).write_bytes(b"drift\n")
        with patch.object(launch, "atomic_replace_bundle") as atomic:
            with self.assertRaisesRegex(ValueError, "baseline.*drift"):
                launch.promote_stage(self.data_dir, stage, baseline)
        atomic.assert_not_called()

        (self.data_dir / launch.PUBLIC_BUNDLE_NAMES[0]).write_bytes(
            self.originals[launch.PUBLIC_BUNDLE_NAMES[0]]
        )
        os.chmod(
            self.data_dir / launch.PUBLIC_BUNDLE_NAMES[0],
            baseline["files"][launch.PUBLIC_BUNDLE_NAMES[0]]["mode"],
        )
        staged_path = stage / launch.PUBLIC_BUNDLE_NAMES[0]
        staged_path.unlink()
        staged_path.symlink_to(stage / launch.PUBLIC_BUNDLE_NAMES[1])
        with patch.object(launch, "atomic_replace_bundle") as atomic:
            with self.assertRaisesRegex(ValueError, "regular non-symlink"):
                launch.promote_stage(self.data_dir, stage, baseline)
        atomic.assert_not_called()

    def test_promote_rejects_candidate_changed_during_task4_revalidation(self):
        self._write_sidecar()
        baseline = launch.snapshot_public_bundle(self.data_dir)
        stage = self._make_stage(baseline)
        original_live = {
            name: (self.data_dir / name).read_bytes()
            for name in launch.PUBLIC_BUNDLE_NAMES
        }

        def mutate_candidate(_stage):
            (_stage / launch.PUBLIC_BUNDLE_NAMES[0]).write_bytes(b"raced\n")

        with patch.object(
            launch,
            "_validate_stage_candidate",
            side_effect=mutate_candidate,
        ):
            with self.assertRaisesRegex(ValueError, "staged candidate.*drift"):
                launch.promote_stage(self.data_dir, stage, baseline)

        self.assertEqual(
            {
                name: (self.data_dir / name).read_bytes()
                for name in launch.PUBLIC_BUNDLE_NAMES
            },
            original_live,
        )

    def test_promote_requires_identical_canonical_stage_private_receipt(self):
        self._write_sidecar()
        baseline = launch.snapshot_public_bundle(self.data_dir)
        for case in ("missing", "tampered"):
            with self.subTest(case=case):
                stage = self._make_stage(baseline, "candidate-{}".format(case))
                raw_path = (
                    stage / "raw/dex-depth" / self.SNAPSHOT_ID
                    / launch.RAW_RECEIPT_NAME
                )
                if case == "missing":
                    raw_path.unlink()
                else:
                    raw_path.write_bytes(b'{"depth_snapshot_id":"other"}\n')
                with patch.object(launch, "_validate_stage_candidate", return_value={}):
                    with self.assertRaisesRegex(ValueError, "private|retained|receipt"):
                        launch.promote_stage(self.data_dir, stage, baseline)

    def test_promote_rejects_live_private_receipt_collision(self):
        self._write_sidecar()
        baseline = launch.snapshot_public_bundle(self.data_dir)
        stage = self._make_stage(baseline)
        live_raw = self.data_dir / "raw/dex-depth" / self.SNAPSHOT_ID
        live_raw.mkdir(parents=True)
        live_receipt = live_raw / launch.RAW_RECEIPT_NAME
        live_receipt.write_bytes(b"collision\n")
        os.chmod(live_receipt, 0o600)
        original_live = {
            name: (self.data_dir / name).read_bytes()
            for name in launch.PUBLIC_BUNDLE_NAMES
        }

        with patch.object(launch, "_validate_stage_candidate", return_value={}):
            with self.assertRaisesRegex(ValueError, "trusted.*differs|collision"):
                launch.promote_stage(self.data_dir, stage, baseline)

        self.assertEqual(
            {name: (self.data_dir / name).read_bytes() for name in launch.PUBLIC_BUNDLE_NAMES},
            original_live,
        )

    def test_reused_trusted_receipt_and_directories_require_private_safe_modes(self):
        self._write_sidecar()
        baseline = launch.snapshot_public_bundle(self.data_dir)
        stage = self._make_stage(baseline)
        live_raw = self.data_dir / "raw/dex-depth" / self.SNAPSHOT_ID
        live_raw.mkdir(parents=True)
        live_receipt = live_raw / launch.RAW_RECEIPT_NAME
        live_receipt.write_bytes(
            (stage / launch.PUBLIC_BUNDLE_NAMES[-1]).read_bytes()
        )
        os.chmod(live_receipt, 0o666)

        with patch.object(launch, "_validate_stage_candidate", return_value={}):
            with self.assertRaisesRegex(ValueError, "mode|0600|trusted"):
                launch.promote_stage(self.data_dir, stage, baseline)

        os.chmod(live_receipt, 0o600)
        os.chmod(live_raw.parent, 0o777)
        with patch.object(launch, "_validate_stage_candidate", return_value={}):
            with self.assertRaisesRegex(ValueError, "writable|directory|mode"):
                launch.promote_stage(self.data_dir, stage, baseline)

    def test_public_promote_failure_removes_only_launch_created_private_receipt(self):
        self._write_sidecar()
        baseline = launch.snapshot_public_bundle(self.data_dir)
        stage = self._make_stage(baseline)
        live_receipt = (
            self.data_dir / "raw/dex-depth" / self.SNAPSHOT_ID
            / launch.RAW_RECEIPT_NAME
        )

        def fail_public(_items):
            self.assertTrue(live_receipt.is_file())
            raise OSError("injected public promote failure")

        with patch.object(launch, "_validate_stage_candidate", return_value={}), patch.object(
            launch, "atomic_replace_bundle", side_effect=fail_public
        ):
            with self.assertRaisesRegex(OSError, "injected public"):
                launch.promote_stage(self.data_dir, stage, baseline)

        self.assertFalse(live_receipt.exists())
        launch.verify_bundle_state(self.data_dir, baseline, state="baseline")

    def test_restore_returns_initially_absent_sidecar_to_absence_and_modes(self):
        launch_dir = self.root / "launch"
        backup = launch.create_backup(
            self.data_dir,
            launch_dir,
            target_sha="a" * 40,
            previous_app_sha="b" * 40,
        )
        stage = self._make_stage(backup["baseline"])
        with patch.object(launch, "_validate_stage_candidate"):
            promotion = launch.promote_stage(
                self.data_dir, stage, backup["baseline"]
            )

        restored = launch.restore_backup(
            self.data_dir, launch_dir / "backup", promotion
        )

        self.assertEqual(restored["restored"], backup["baseline"])
        for name in launch.PUBLIC_BUNDLE_NAMES[:-1]:
            path = self.data_dir / name
            self.assertEqual(path.read_bytes(), self.originals[name])
            self.assertEqual(
                stat.S_IMODE(path.stat().st_mode),
                backup["baseline"]["files"][name]["mode"],
            )
        self.assertFalse((self.data_dir / launch.PUBLIC_BUNDLE_NAMES[-1]).exists())
        self.assertFalse(
            (
                self.data_dir / "raw/dex-depth" / self.SNAPSHOT_ID
                / launch.RAW_RECEIPT_NAME
            ).exists()
        )

    def test_restore_preserves_byte_identical_preexisting_live_private_receipt(self):
        self._write_sidecar()
        launch_dir = self.root / "launch"
        backup = launch.create_backup(
            self.data_dir,
            launch_dir,
            target_sha="a" * 40,
            previous_app_sha="b" * 40,
        )
        stage = self._make_stage(backup["baseline"])
        stage_receipt = (
            stage / "raw/dex-depth" / self.SNAPSHOT_ID / launch.RAW_RECEIPT_NAME
        ).read_bytes()
        live_dir = self.data_dir / "raw/dex-depth" / self.SNAPSHOT_ID
        live_dir.mkdir(parents=True)
        live_receipt = live_dir / launch.RAW_RECEIPT_NAME
        live_receipt.write_bytes(stage_receipt)
        os.chmod(live_receipt, 0o600)
        with patch.object(launch, "_validate_stage_candidate", return_value={}):
            promotion = launch.promote_stage(
                self.data_dir, stage, backup["baseline"]
            )
        self.assertFalse(promotion["trusted_receipt"]["created"])

        launch.restore_backup(self.data_dir, launch_dir / "backup", promotion)

        self.assertEqual(live_receipt.read_bytes(), stage_receipt)

    def test_restore_refuses_trusted_private_receipt_drift(self):
        self._write_sidecar()
        launch_dir = self.root / "launch"
        backup = launch.create_backup(
            self.data_dir,
            launch_dir,
            target_sha="a" * 40,
            previous_app_sha="b" * 40,
        )
        stage = self._make_stage(backup["baseline"])
        with patch.object(launch, "_validate_stage_candidate", return_value={}):
            promotion = launch.promote_stage(
                self.data_dir, stage, backup["baseline"]
            )
        live_receipt = (
            self.data_dir / "raw/dex-depth" / self.SNAPSHOT_ID
            / launch.RAW_RECEIPT_NAME
        )
        live_receipt.write_bytes(b"third-party raw drift\n")

        with self.assertRaisesRegex(ValueError, "trusted.*drift"):
            launch.restore_backup(
                self.data_dir, launch_dir / "backup", promotion
            )
        launch.verify_bundle_state(
            self.data_dir, promotion["promoted"], state="promoted"
        )

    def test_restore_refuses_promoted_generation_drift(self):
        self._write_sidecar()
        launch_dir = self.root / "launch"
        backup = launch.create_backup(
            self.data_dir,
            launch_dir,
            target_sha="a" * 40,
            previous_app_sha="b" * 40,
        )
        stage = self._make_stage(backup["baseline"])
        with patch.object(launch, "_validate_stage_candidate"):
            promotion = launch.promote_stage(
                self.data_dir, stage, backup["baseline"]
            )
        (self.data_dir / launch.PUBLIC_BUNDLE_NAMES[1]).write_bytes(b"third-party\n")

        with self.assertRaisesRegex(ValueError, "promoted.*drift"):
            launch.restore_backup(
                self.data_dir, launch_dir / "backup", promotion
            )
        self.assertEqual(
            (self.data_dir / launch.PUBLIC_BUNDLE_NAMES[1]).read_bytes(),
            b"third-party\n",
        )

    def test_restore_rechecks_promoted_cas_inside_replace_or_remove_transaction(self):
        self._write_sidecar()
        launch_dir = self.root / "launch"
        backup = launch.create_backup(
            self.data_dir,
            launch_dir,
            target_sha="a" * 40,
            previous_app_sha="b" * 40,
        )
        stage = self._make_stage(backup["baseline"])
        with patch.object(launch, "_validate_stage_candidate"):
            promotion = launch.promote_stage(
                self.data_dir, stage, backup["baseline"]
            )
        real_verify = launch.verify_bundle_state
        calls = {"count": 0}

        def drift_after_second_verify(data_dir, manifest, *, state):
            real_verify(data_dir, manifest, state=state)
            calls["count"] += 1
            if calls["count"] == 2:
                (self.data_dir / launch.PUBLIC_BUNDLE_NAMES[2]).write_bytes(
                    b"late third-party drift\n"
                )

        with patch.object(
            launch,
            "verify_bundle_state",
            side_effect=drift_after_second_verify,
        ):
            with self.assertRaisesRegex(ValueError, "promoted.*drift"):
                launch.restore_backup(
                    self.data_dir,
                    launch_dir / "backup",
                    promotion,
                )
        self.assertEqual(
            (self.data_dir / launch.PUBLIC_BUNDLE_NAMES[2]).read_bytes(),
            b"late third-party drift\n",
        )

    def test_restore_rejects_canonical_backup_manifest_metadata_tamper(self):
        self._write_sidecar()
        launch_dir = self.root / "launch"
        backup = launch.create_backup(
            self.data_dir,
            launch_dir,
            target_sha="a" * 40,
            previous_app_sha="b" * 40,
        )
        stage = self._make_stage(backup["baseline"])
        with patch.object(launch, "_validate_stage_candidate"):
            promotion = launch.promote_stage(
                self.data_dir, stage, backup["baseline"]
            )
        manifest_path = launch_dir / "backup/manifest.json"
        manifest = launch.read_receipt(manifest_path)
        manifest["copies"][0]["sha256"] = "0" * 64
        manifest_path.write_bytes(launch.canonical_json_bytes(manifest))

        with self.assertRaisesRegex(ValueError, "backup.*manifest|backup file"):
            launch.restore_backup(
                self.data_dir,
                launch_dir / "backup",
                promotion,
            )

    def test_restore_failure_at_every_replace_or_remove_restores_promoted_bytes(self):
        for fail_at in range(1, len(launch.PUBLIC_BUNDLE_NAMES) + 2):
            with self.subTest(fail_at=fail_at):
                case = self.root / "case-{}".format(fail_at)
                case.mkdir(mode=0o700)
                data = case / "published"
                data.mkdir(mode=0o700)
                for index, name in enumerate(launch.PUBLIC_BUNDLE_NAMES[:-1]):
                    (data / name).write_bytes(
                        "old-{}\n".format(index).encode("ascii")
                    )
                launch_dir = case / "launch"
                backup = launch.create_backup(
                    data,
                    launch_dir,
                    target_sha="a" * 40,
                    previous_app_sha="b" * 40,
                )
                stage = case / "candidate"
                stage.mkdir(mode=0o700)
                for index, name in enumerate(launch.PUBLIC_BUNDLE_NAMES):
                    (stage / name).write_bytes(
                        "new-{}\n".format(index).encode("ascii")
                    )
                receipt = {"depth_snapshot_id": self.SNAPSHOT_ID}
                receipt_bytes = launch.canonical_json_bytes(receipt)
                (stage / launch.PUBLIC_BUNDLE_NAMES[-1]).write_bytes(receipt_bytes)
                raw_dir = stage / "raw/dex-depth" / self.SNAPSHOT_ID
                raw_dir.mkdir(parents=True)
                (raw_dir / launch.RAW_RECEIPT_NAME).write_bytes(receipt_bytes)
                with patch.object(launch, "_validate_stage_candidate"):
                    promotion = launch.promote_stage(data, stage, backup["baseline"])
                promoted_bytes = {
                    name: (data / name).read_bytes()
                    for name in launch.PUBLIC_BUNDLE_NAMES
                }
                live_receipt = (
                    data / "raw/dex-depth" / self.SNAPSHOT_ID
                    / launch.RAW_RECEIPT_NAME
                )
                promoted_raw = live_receipt.read_bytes()

                real_replace = launch.os.replace
                real_unlink = launch.os.unlink
                calls = {"count": 0}

                def fail_replace(source, destination):
                    if Path(destination).name in launch.PUBLIC_BUNDLE_NAMES:
                        calls["count"] += 1
                        if calls["count"] == fail_at:
                            raise OSError("injected restore commit failure")
                    return real_replace(source, destination)

                def fail_unlink(path, *args, **kwargs):
                    if Path(path).name in (
                        launch.PUBLIC_BUNDLE_NAMES + (launch.RAW_RECEIPT_NAME,)
                    ):
                        calls["count"] += 1
                        if calls["count"] == fail_at:
                            raise OSError("injected restore commit failure")
                    return real_unlink(path, *args, **kwargs)

                with patch.object(launch.os, "replace", side_effect=fail_replace), patch.object(
                    launch.os, "unlink", side_effect=fail_unlink
                ):
                    with self.assertRaisesRegex(OSError, "injected restore"):
                        launch.restore_backup(data, launch_dir / "backup", promotion)

                self.assertEqual(
                    {
                        name: (data / name).read_bytes()
                        for name in launch.PUBLIC_BUNDLE_NAMES
                    },
                    promoted_bytes,
                )
                self.assertEqual(live_receipt.read_bytes(), promoted_raw)

    def test_canonical_receipt_reader_rejects_tamper_noncanonical_and_symlink(self):
        launch_dir = self.root / "launch"
        launch_dir.mkdir(mode=0o700)
        receipt = {
            "schema": launch.RECEIPT_SCHEMA,
            "phase": "preflight",
            "predecessor_receipt_sha256": None,
            "target_sha": "a" * 40,
        }
        path = launch_dir / "01-preflight.json"
        launch.write_receipt(path, receipt)
        self.assertEqual(launch.read_receipt(path), receipt)
        self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)

        path.write_text(json.dumps(receipt, indent=2) + "\n")
        with self.assertRaisesRegex(ValueError, "canonical"):
            launch.read_receipt(path)
        path.unlink()
        target = self.root / "receipt-target"
        target.write_bytes(launch.canonical_json_bytes(receipt))
        path.symlink_to(target)
        with self.assertRaisesRegex(ValueError, "regular non-symlink"):
            launch.read_receipt(path)

    def test_launch_and_backup_receipt_reads_require_mode_0600(self):
        launch_dir = self.root / "private-receipts"
        launch_dir.mkdir(mode=0o700)
        phase_path = launch_dir / "phase.json"
        launch.write_receipt(phase_path, {"schema": "test"})
        os.chmod(phase_path, 0o666)
        with self.assertRaisesRegex(ValueError, "0600|mode"):
            launch.read_receipt(phase_path)

        self._write_sidecar()
        backup = launch.create_backup(
            self.data_dir,
            launch_dir,
            target_sha="a" * 40,
            previous_app_sha="b" * 40,
        )
        manifest_path = launch_dir / "backup/manifest.json"
        os.chmod(manifest_path, 0o666)
        with self.assertRaisesRegex(ValueError, "0600|mode"):
            launch._load_backup_manifest(manifest_path.parent)
        self.assertEqual(backup["schema"], launch.BACKUP_SCHEMA)


class FakeProcess:
    def __init__(self):
        self.terminated = False

    def poll(self):
        return None

    def terminate(self):
        self.terminated = True

    def wait(self, timeout=None):
        return 0

    def kill(self):
        self.terminated = True


class FakeRunner:
    def __init__(self, *, target_sha, previous_sha):
        self.target_sha = target_sha
        self.previous_sha = previous_sha
        self.commands = []
        self.started = []
        self.processes = []
        self.live_lock_path = None
        self.enabled = {
            "cex-dex-daily.timer": "enabled",
            "cex-dex-depth.timer": "disabled",
        }
        self.active = {
            "cex-dex-daily.timer": "active",
            "cex-dex-depth.timer": "inactive",
            "cex-dex-daily.service": "inactive",
            "cex-dex-depth.service": "inactive",
        }

    def _assert_live_lock_held(self):
        if self.live_lock_path is None:
            return
        descriptor = os.open(str(self.live_lock_path), os.O_RDWR)
        try:
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                pass
            else:
                raise AssertionError("live collection lock was not held")
        finally:
            os.close(descriptor)

    def run(self, command, *, env=None):
        command = list(command)
        self.commands.append((command, dict(env or {})))
        if command[:3] == ["git", "rev-parse", "HEAD"]:
            return launch.CommandResult(0, self.target_sha + "\n", "")
        if any(part.endswith("check_dashboard_health.py") for part in command):
            self._assert_live_lock_held()
            payload = {
                "status": "ok",
                "data_ready": True,
                "data_status": "current",
                "application_sha": self.previous_sha,
            }
            return launch.CommandResult(0, json.dumps(payload) + "\n", "")
        if any(part.endswith("check_dashboard_release.py") for part in command):
            self._assert_live_lock_held()
            return launch.CommandResult(
                0,
                json.dumps({
                    "status": "passed",
                    "application_sha": command[
                        command.index("--expected-application-sha") + 1
                    ],
                }) + "\n",
                "",
            )
        if command[:3] == ["systemctl", "--user", "is-enabled"]:
            value = self.enabled[command[3]]
            return launch.CommandResult(0 if value == "enabled" else 1, value + "\n", "")
        if command[:3] == ["systemctl", "--user", "is-active"]:
            value = self.active[command[3]]
            return launch.CommandResult(0 if value == "active" else 3, value + "\n", "")
        if command[:3] == ["systemctl", "--user", "disable"]:
            unit = command[-1]
            self.enabled[unit] = "disabled"
            self.active[unit] = "inactive"
            return launch.CommandResult(0, "", "")
        if command[:3] == ["systemctl", "--user", "enable"]:
            self.enabled[command[3]] = "enabled"
            return launch.CommandResult(0, "", "")
        if command[:3] == ["systemctl", "--user", "start"]:
            self.active[command[3]] = "active"
            return launch.CommandResult(0, "", "")
        if command[:3] == ["systemctl", "--user", "stop"]:
            self.active[command[3]] = "inactive"
            return launch.CommandResult(0, "", "")
        if any(part.endswith("run_collection_cycle.py") for part in command):
            self._assert_live_lock_held()
            stage = Path(command[command.index("--data-dir") + 1])
            for index, name in enumerate(launch.PUBLIC_BUNDLE_NAMES):
                (stage / name).write_bytes(
                    "candidate-{}\n".format(index).encode("ascii")
                )
            receipt = {
                "depth_snapshot_id": LaunchFilesystemTest.SNAPSHOT_ID,
            }
            receipt_bytes = launch.canonical_json_bytes(receipt)
            (stage / launch.PUBLIC_BUNDLE_NAMES[-1]).write_bytes(receipt_bytes)
            raw_dir = (
                stage / "raw/dex-depth" / LaunchFilesystemTest.SNAPSHOT_ID
            )
            raw_dir.mkdir(parents=True)
            (raw_dir / launch.RAW_RECEIPT_NAME).write_bytes(receipt_bytes)
            return launch.CommandResult(0, json.dumps({"status": "passed"}) + "\n", "")
        raise AssertionError("unexpected command: {!r}".format(command))

    def start(self, command, *, env=None):
        self._assert_live_lock_held()
        process = FakeProcess()
        self.started.append((list(command), dict(env or {})))
        self.processes.append(process)
        return process


class LaunchOrchestrationTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary.name)
        self.data_dir = self.root / "published"
        self.data_dir.mkdir(mode=0o700)
        for index, name in enumerate(launch.PUBLIC_BUNDLE_NAMES[:-1]):
            (self.data_dir / name).write_bytes(
                "old-{}\n".format(index).encode("ascii")
            )
        (self.data_dir / "market_facts.sqlite3").write_bytes(b"sqlite\n")
        (self.data_dir / "dex_pool_volume_daily.csv").write_bytes(b"date\n")
        (self.data_dir / "collection").mkdir(mode=0o700)
        self.launch_dir = self.root / "launch"
        self.stage_dir = self.root / "candidate"
        self.target_sha = "a" * 40
        self.previous_sha = "b" * 40
        self.runner = FakeRunner(
            target_sha=self.target_sha,
            previous_sha=self.previous_sha,
        )
        self.runner.live_lock_path = self.data_dir / "collection/collection.lock"
        self.config = launch.LaunchConfig(
            data_dir=self.data_dir,
            launch_dir=self.launch_dir,
            stage_dir=self.stage_dir,
            target_sha=self.target_sha,
            previous_app_sha=self.previous_sha,
            live_base_url="http://127.0.0.1:8765",
            stage_port=18765,
        )

    def tearDown(self):
        self.temporary.cleanup()

    def _run_to_stage(self):
        launch.execute_phase("preflight", self.config, self.runner)
        launch.execute_phase("pause", self.config, self.runner)
        launch.execute_phase("backup", self.config, self.runner)
        return launch.execute_phase("stage", self.config, self.runner)

    def test_default_plan_has_zero_side_effects_and_redacts_paths(self):
        absent_launch = self.root / "never-created"
        output = io.StringIO()
        code = launch.main(
            [
                "preflight",
                "--data-dir", str(self.root / "missing-live"),
                "--launch-dir", str(absent_launch),
                "--stage-dir", str(self.root / "missing-stage"),
                "--target-sha", self.target_sha,
                "--previous-app-sha", self.previous_sha,
            ],
            runner=self.runner,
            stdout=output,
        )

        self.assertEqual(code, 0)
        self.assertFalse(absent_launch.exists())
        self.assertEqual(self.runner.commands, [])
        self.assertEqual(self.runner.started, [])
        plan = output.getvalue()
        self.assertNotIn(str(self.root), plan)
        self.assertNotIn("MARKET_", plan)
        self.assertIn('"execute":false', plan)

    def test_pause_captures_exact_timer_state_and_verifies_fixed_services_inactive(self):
        launch.execute_phase("preflight", self.config, self.runner)
        receipt = launch.execute_phase("pause", self.config, self.runner)

        self.assertEqual(receipt["timer_states"], {
            "cex-dex-daily.timer": {"active": "active", "enabled": "enabled"},
            "cex-dex-depth.timer": {"active": "inactive", "enabled": "disabled"},
        })
        commands = [item[0] for item in self.runner.commands]
        self.assertIn(
            ["systemctl", "--user", "disable", "--now", "cex-dex-daily.timer"],
            commands,
        )
        self.assertIn(
            ["systemctl", "--user", "stop", "cex-dex-depth.service"],
            commands,
        )
        self.assertTrue(all("sudo" not in command for command in commands))
        mentioned_units = {
            item
            for command in commands
            for item in command
            if item.endswith((".timer", ".service"))
        }
        self.assertEqual(mentioned_units, set(launch.MANAGED_UNITS))

    def test_pause_command_failure_restores_captured_timer_states_without_receipt(self):
        launch.execute_phase("preflight", self.config, self.runner)
        real_run = self.runner.run

        def fail_second_disable(command, *, env=None):
            if list(command) == [
                "systemctl", "--user", "disable", "--now",
                "cex-dex-depth.timer",
            ]:
                return launch.CommandResult(1, "", "injected")
            return real_run(command, env=env)

        with patch.object(self.runner, "run", side_effect=fail_second_disable):
            with self.assertRaisesRegex(RuntimeError, "disable managed timer"):
                launch.execute_phase("pause", self.config, self.runner)

        self.assertEqual(self.runner.enabled["cex-dex-daily.timer"], "enabled")
        self.assertEqual(self.runner.active["cex-dex-daily.timer"], "active")
        self.assertEqual(self.runner.enabled["cex-dex-depth.timer"], "disabled")
        self.assertEqual(self.runner.active["cex-dex-depth.timer"], "inactive")
        self.assertFalse(
            (self.launch_dir / launch.RECEIPT_FILES["pause"]).exists()
        )

    def test_pause_receipt_failure_restores_original_timer_state_and_allows_retry(self):
        launch.execute_phase("preflight", self.config, self.runner)
        with patch.object(
            launch,
            "write_receipt",
            side_effect=OSError("injected pause receipt failure"),
        ):
            with self.assertRaisesRegex(OSError, "pause receipt"):
                launch.execute_phase("pause", self.config, self.runner)

        self.assertEqual(self.runner.enabled["cex-dex-daily.timer"], "enabled")
        self.assertEqual(self.runner.active["cex-dex-daily.timer"], "active")
        self.assertEqual(self.runner.enabled["cex-dex-depth.timer"], "disabled")
        self.assertEqual(self.runner.active["cex-dex-depth.timer"], "inactive")
        self.assertFalse(
            (self.launch_dir / launch.RECEIPT_FILES["pause"]).exists()
        )
        receipt = launch.execute_phase("pause", self.config, self.runner)
        self.assertEqual(receipt["phase"], "pause")

    def test_stage_runs_full_unfiltered_exact_collection_only_against_stage_roots(self):
        receipt = self._run_to_stage()

        collection_commands = [
            (command, env)
            for command, env in self.runner.commands
            if any(part.endswith("run_collection_cycle.py") for part in command)
        ]
        self.assertEqual(len(collection_commands), 1)
        command, env = collection_commands[0]
        self.assertIn("--profile", command)
        self.assertEqual(command[command.index("--profile") + 1], "dex_depth")
        self.assertIn("--publish-local", command)
        self.assertIn("--require-uniswap-v3-exact-validation", command)
        self.assertNotIn("--market-id", command)
        self.assertNotIn("--tokens", command)
        self.assertEqual(command[command.index("--data-dir") + 1], str(self.stage_dir))
        self.assertNotIn(str(self.data_dir), command)
        self.assertEqual(env["MARKET_DATA_DIR"], str(self.stage_dir))
        self.assertEqual(env["PYTHONDONTWRITEBYTECODE"], "1")
        self.assertEqual(receipt["stage_roots"], {
            "data": launch._path_binding(self.stage_dir),
            "processed": launch._path_binding(launch.processed_dir_for(self.stage_dir)),
        })
        self.assertFalse((self.data_dir / "raw").exists())
        self.assertFalse(launch.processed_dir_for(self.data_dir).exists())

    def test_stage_rejects_required_input_drift_since_preflight(self):
        launch.execute_phase("preflight", self.config, self.runner)
        launch.execute_phase("pause", self.config, self.runner)
        launch.execute_phase("backup", self.config, self.runner)
        (self.data_dir / "market_facts.sqlite3").write_bytes(b"late drift\n")

        with self.assertRaisesRegex(ValueError, "required input.*drift"):
            launch.execute_phase("stage", self.config, self.runner)

        self.assertFalse(self.stage_dir.exists())
        self.assertFalse(launch.processed_dir_for(self.stage_dir).exists())

    def test_verify_stage_uses_required_overrides_and_unchanged_release_checker(self):
        self._run_to_stage()
        with patch.object(launch, "_validate_stage_candidate", return_value={}):
            receipt = launch.execute_phase("verify-stage", self.config, self.runner)

        self.assertEqual(len(self.runner.started), 1)
        dashboard_command, env = self.runner.started[0]
        self.assertTrue(any(part.endswith("dashboard/server.py") for part in dashboard_command))
        self.assertEqual(env["MARKET_DATA_DIR"], str(self.data_dir))
        self.assertEqual(
            env["MARKET_DEX_DEPTH_DATA"],
            str(self.stage_dir / "dex_depth_latest.csv"),
        )
        self.assertEqual(
            env["MARKET_DEX_EXECUTION_COST_DATA"],
            str(self.stage_dir / "dex_execution_cost_latest.csv"),
        )
        self.assertEqual(
            env["MARKET_UNISWAP_V3_EXACT_DATA"],
            str(self.stage_dir / "uniswap_v3_exact_latest.json"),
        )
        self.assertEqual(
            env["MARKET_UNISWAP_V3_EXACT_RAW_ROOT"],
            str(self.stage_dir / "raw/dex-depth"),
        )
        self.assertEqual(env["CEX_DEX_RELEASE_SHA"], self.target_sha)
        release_commands = [
            command
            for command, _env in self.runner.commands
            if any(part.endswith("check_dashboard_release.py") for part in command)
        ]
        self.assertEqual(len(release_commands), 1)
        self.assertEqual(
            release_commands[0][release_commands[0].index("--expected-application-sha") + 1],
            self.target_sha,
        )
        self.assertTrue(self.runner.processes[0].terminated)
        self.assertEqual(receipt["release_evidence"]["application_sha"], self.target_sha)

    def test_receipt_chain_rejects_replay_reorder_tamper_and_stage_drift(self):
        with self.assertRaisesRegex(ValueError, "predecessor"):
            launch.execute_phase("pause", self.config, self.runner)
        launch.execute_phase("preflight", self.config, self.runner)
        with self.assertRaisesRegex(FileExistsError, "already completed"):
            launch.execute_phase("preflight", self.config, self.runner)
        launch.execute_phase("pause", self.config, self.runner)
        launch.execute_phase("backup", self.config, self.runner)
        stage_receipt = launch.execute_phase("stage", self.config, self.runner)

        path = self.launch_dir / launch.RECEIPT_FILES["stage"]
        value = launch.read_receipt(path)
        value["target_sha"] = "c" * 40
        path.write_bytes(launch.canonical_json_bytes(value))
        with self.assertRaisesRegex(ValueError, "chain|target"):
            launch.execute_phase("verify-stage", self.config, self.runner)
        path.write_bytes(launch.canonical_json_bytes(stage_receipt))
        (self.stage_dir / launch.PUBLIC_BUNDLE_NAMES[0]).write_bytes(b"stage drift\n")
        with self.assertRaisesRegex(ValueError, "staged.*drift"):
            launch.execute_phase("verify-stage", self.config, self.runner)

    def test_receipt_chain_rejects_a_missing_consecutive_phase(self):
        self.launch_dir.mkdir(mode=0o700)
        preflight = {
            "schema": launch.RECEIPT_SCHEMA,
            "phase": "preflight",
            "predecessor_receipt_sha256": None,
            "target_sha": self.target_sha,
            "previous_app_sha": self.previous_sha,
        }
        launch.write_receipt(
            self.launch_dir / launch.RECEIPT_FILES["preflight"],
            preflight,
        )
        backup = {
            "schema": launch.RECEIPT_SCHEMA,
            "phase": "backup",
            "predecessor_receipt_sha256": hashlib.sha256(
                launch.canonical_json_bytes(preflight)
            ).hexdigest(),
            "target_sha": self.target_sha,
            "previous_app_sha": self.previous_sha,
            "baseline": launch.snapshot_public_bundle(self.data_dir),
            "baseline_sha256": "0" * 64,
        }
        launch.write_receipt(
            self.launch_dir / launch.RECEIPT_FILES["backup"],
            backup,
        )

        with self.assertRaisesRegex(ValueError, "consecutive|missing|order"):
            launch._load_predecessor("stage", self.config.normalized())

    def test_promote_restore_and_resume_require_release_evidence_and_restore_timer_states(self):
        self._run_to_stage()
        with patch.object(launch, "_validate_stage_candidate", return_value={}):
            launch.execute_phase("verify-stage", self.config, self.runner)
            launch.execute_phase("promote", self.config, self.runner)
            launch.execute_phase("restore", self.config, self.runner)
        receipt = launch.execute_phase("resume", self.config, self.runner)

        self.assertEqual(receipt["release_evidence"]["application_sha"], self.previous_sha)
        self.assertEqual(self.runner.enabled["cex-dex-daily.timer"], "enabled")
        self.assertEqual(self.runner.active["cex-dex-daily.timer"], "active")
        self.assertEqual(self.runner.enabled["cex-dex-depth.timer"], "disabled")
        self.assertEqual(self.runner.active["cex-dex-depth.timer"], "inactive")
        self.assertEqual(receipt["restored_timer_states"], {
            "cex-dex-daily.timer": {"active": "active", "enabled": "enabled"},
            "cex-dex-depth.timer": {"active": "inactive", "enabled": "disabled"},
        })

    def test_forward_resume_requires_target_sha_release_evidence(self):
        self._run_to_stage()
        with patch.object(launch, "_validate_stage_candidate", return_value={}):
            launch.execute_phase("verify-stage", self.config, self.runner)
            launch.execute_phase("promote", self.config, self.runner)
        live_receipt = (
            self.data_dir / "raw/dex-depth" / LaunchFilesystemTest.SNAPSHOT_ID
            / launch.RAW_RECEIPT_NAME
        )
        self.assertTrue(live_receipt.is_file())
        receipt = launch.execute_phase("resume", self.config, self.runner)
        self.assertEqual(receipt["release_evidence"]["application_sha"], self.target_sha)

    def test_resume_failure_at_each_timer_restore_command_returns_to_safe_pause(self):
        self._run_to_stage()
        with patch.object(launch, "_validate_stage_candidate", return_value={}):
            launch.execute_phase("verify-stage", self.config, self.runner)
            launch.execute_phase("promote", self.config, self.runner)
        resume_path = self.launch_dir / launch.RECEIPT_FILES["resume"]
        restore_commands = [
            ["systemctl", "--user", "enable", "cex-dex-daily.timer"],
            ["systemctl", "--user", "start", "cex-dex-daily.timer"],
            ["systemctl", "--user", "disable", "cex-dex-depth.timer"],
            ["systemctl", "--user", "stop", "cex-dex-depth.timer"],
        ]
        real_run = self.runner.run
        for failing_command in restore_commands:
            with self.subTest(command=failing_command):
                if resume_path.exists():
                    resume_path.unlink()
                for unit in launch.TIMER_UNITS:
                    self.runner.enabled[unit] = "disabled"
                    self.runner.active[unit] = "inactive"
                failed = {"value": False}

                def fail_once(command, *, env=None):
                    if list(command) == failing_command and not failed["value"]:
                        failed["value"] = True
                        return launch.CommandResult(1, "", "injected")
                    return real_run(command, env=env)

                with patch.object(self.runner, "run", side_effect=fail_once):
                    with self.assertRaisesRegex(RuntimeError, "restore timer"):
                        launch.execute_phase("resume", self.config, self.runner)

                self.assertFalse(resume_path.exists())
                for unit in launch.TIMER_UNITS:
                    self.assertEqual(self.runner.enabled[unit], "disabled")
                    self.assertEqual(self.runner.active[unit], "inactive")
                for unit in launch.SERVICE_UNITS:
                    self.assertEqual(self.runner.active[unit], "inactive")
                self.assertEqual(
                    launch.execute_phase("resume", self.config, self.runner)["phase"],
                    "resume",
                )

    def test_resume_receipt_failure_returns_to_safe_pause_and_allows_retry(self):
        self._run_to_stage()
        with patch.object(launch, "_validate_stage_candidate", return_value={}):
            launch.execute_phase("verify-stage", self.config, self.runner)
            launch.execute_phase("promote", self.config, self.runner)
        resume_path = self.launch_dir / launch.RECEIPT_FILES["resume"]
        real_write = launch.write_receipt

        def fail_resume_receipt(path, receipt):
            if Path(path) == resume_path:
                raise OSError("injected resume receipt failure")
            return real_write(path, receipt)

        with patch.object(launch, "write_receipt", side_effect=fail_resume_receipt):
            with self.assertRaisesRegex(OSError, "resume receipt"):
                launch.execute_phase("resume", self.config, self.runner)

        self.assertFalse(resume_path.exists())
        for unit in launch.TIMER_UNITS:
            self.assertEqual(self.runner.enabled[unit], "disabled")
            self.assertEqual(self.runner.active[unit], "inactive")
        self.assertEqual(
            launch.execute_phase("resume", self.config, self.runner)["phase"],
            "resume",
        )

    def test_restore_rejects_completed_forward_resume_ledger(self):
        self._run_to_stage()
        with patch.object(launch, "_validate_stage_candidate", return_value={}):
            launch.execute_phase("verify-stage", self.config, self.runner)
            launch.execute_phase("promote", self.config, self.runner)
        launch.execute_phase("resume", self.config, self.runner)
        for unit in launch.TIMER_UNITS:
            self.runner.enabled[unit] = "disabled"
            self.runner.active[unit] = "inactive"

        with self.assertRaisesRegex(ValueError, "ledger|order"):
            launch.execute_phase("restore", self.config, self.runner)

    def test_forward_resume_rejects_trusted_live_receipt_drift_before_release(self):
        self._run_to_stage()
        with patch.object(launch, "_validate_stage_candidate", return_value={}):
            launch.execute_phase("verify-stage", self.config, self.runner)
            launch.execute_phase("promote", self.config, self.runner)
        live_receipt = (
            self.data_dir / "raw/dex-depth" / LaunchFilesystemTest.SNAPSHOT_ID
            / launch.RAW_RECEIPT_NAME
        )
        live_receipt.write_bytes(b"late trusted drift\n")

        with self.assertRaisesRegex(ValueError, "trusted.*drift"):
            launch.execute_phase("resume", self.config, self.runner)

        self.assertEqual(self.runner.enabled["cex-dex-daily.timer"], "disabled")
        self.assertEqual(self.runner.active["cex-dex-daily.timer"], "inactive")
        self.assertFalse(
            (self.launch_dir / launch.RECEIPT_FILES["resume"]).exists()
        )

    def test_receipts_never_contain_absolute_paths_environment_rpc_or_secrets(self):
        self._run_to_stage()
        for path in sorted(self.launch_dir.glob("*.json")):
            text = path.read_text()
            self.assertNotIn(str(self.root), text)
            self.assertNotIn("MARKET_", text)
            self.assertNotIn("RPC", text.upper())
            self.assertNotIn("SECRET", text.upper())
            self.assertNotIn("http://", text)


if __name__ == "__main__":
    unittest.main()
