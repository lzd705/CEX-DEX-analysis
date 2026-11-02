"""Private route-inventory evidence and route-mode eligibility tests."""

from contextlib import redirect_stderr, redirect_stdout
import csv
from decimal import Decimal, localcontext
import hashlib
import io
import json
import os
from pathlib import Path
import stat
import tempfile
import traceback
import unittest
from unittest import mock

from scripts.route_inventory import (
    INVENTORY_EVIDENCE_VERSION,
    INVENTORY_PROFILE_COLUMNS,
    classify_route_mode_evidence,
    inventory_capacity_for_route,
    load_validated_inventory_profile,
)


PROFILE_HASH = "a" * 64
OTHER_PROFILE_HASH = "b" * 64
SOURCE_HASH = "c" * 64
OBSERVED_AT = "2026-08-01T12:00:00Z"
NOW = "2026-08-01T12:01:00Z"
VALID_UNTIL = "2026-08-01T12:02:00Z"

BUY_MARKET = "cex:alpha:AAVE/USDT"
SELL_MARKET = "cex:beta:AAVE/USDT"
TOKEN = "AAVE"
DEX_BUY_MARKET = (
    "dex:eth:uniswap_v3:"
    "0x1111111111111111111111111111111111111111:AAVE"
)

PRIVATE_SENTINELS = (
    "ACCOUNT_ID_SENTINEL",
    "WALLET_ADDRESS_SENTINEL",
    "API_KEY_SENTINEL",
    "PRIVATE_PROFILE_PATH_SENTINEL",
)


def _route(mode="prepositioned_inventory"):
    route_id = "route:{}:{}->{}:{}".format(
        TOKEN,
        BUY_MARKET,
        SELL_MARKET,
        mode,
    )
    return {
        "route_id": route_id,
        "token_symbol": TOKEN,
        "buy_market_id": BUY_MARKET,
        "sell_market_id": SELL_MARKET,
        "route_mode": mode,
    }


def _atomic_route(*, sell_chain="eth"):
    buy_market = (
        "dex:eth:uniswap_v3:"
        "0x1111111111111111111111111111111111111111:AAVE"
    )
    sell_market = (
        "dex:{}:sushiswap:"
        "0x2222222222222222222222222222222222222222:AAVE"
    ).format(sell_chain)
    mode = "atomic_onchain"
    return {
        "route_id": "route:{}:{}->{}:{}".format(
            TOKEN,
            buy_market,
            sell_market,
            mode,
        ),
        "token_symbol": TOKEN,
        "buy_market_id": buy_market,
        "sell_market_id": sell_market,
        "route_mode": mode,
    }


def _canonical_sha256(value):
    encoded = json.dumps(
        value,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _evidence_binding_sha256(evidence):
    return _canonical_sha256({
        key: value
        for key, value in evidence.items()
        if key != "evidence_binding_sha256"
    })


def _atomic_evidence(route, **overrides):
    evidence = {
        "evidence_type": "composed_route_simulation",
        "status": "complete",
        "route_id": route["route_id"],
        "buy_market_id": route["buy_market_id"],
        "sell_market_id": route["sell_market_id"],
        "cohort_state_id": "cohort-2026-08-01T12:00:00Z",
        "target_asset": TOKEN,
        "target_quantity": "99.5",
        "composed_call_sha256": SOURCE_HASH,
        "route_outcome_sha256": OTHER_PROFILE_HASH,
        "observed_at": OBSERVED_AT,
        "valid_until": VALID_UNTIL,
        "source_record_sha256": "d" * 64,
    }
    evidence.update(overrides)
    if "evidence_binding_sha256" not in overrides:
        evidence["evidence_binding_sha256"] = _evidence_binding_sha256(
            evidence
        )
    return evidence


def _transfer_evidence(route, **overrides):
    evidence = {
        "evidence_type": "transfer",
        "status": "complete",
        "route_id": route["route_id"],
        "asset": TOKEN,
        "quantity": "99.5",
        "capacity_quantity": "100",
        "from_market_id": route["buy_market_id"],
        "to_market_id": route["sell_market_id"],
        "from_state_id": "buy-state-1",
        "to_state_id": "sell-state-1",
        "observed_at": OBSERVED_AT,
        "valid_until": VALID_UNTIL,
        "source_record_sha256": SOURCE_HASH,
    }
    evidence.update(overrides)
    if "evidence_binding_sha256" not in overrides:
        evidence["evidence_binding_sha256"] = _evidence_binding_sha256(
            evidence
        )
    return evidence


def _expected_inventory_request(route=None, **overrides):
    current = route or _route()
    request = {
        "route_id": current["route_id"],
        "buy_market_id": current["buy_market_id"],
        "sell_market_id": current["sell_market_id"],
        "buy_quote_asset": "USDT",
        "buy_quote_quantity": "9999.99",
        "sell_token_asset": TOKEN,
        "sell_net_token_quantity": "99.5",
        "target_asset": TOKEN,
        "target_quantity": "99.5",
    }
    request.update(overrides)
    return request


def _expected_atomic_request(route, *, evidence=None, **overrides):
    current_evidence = evidence or _atomic_evidence(route)
    request = {
        "route_id": route["route_id"],
        "buy_market_id": route["buy_market_id"],
        "sell_market_id": route["sell_market_id"],
        "cohort_state_id": current_evidence["cohort_state_id"],
        "target_asset": current_evidence["target_asset"],
        "target_quantity": current_evidence["target_quantity"],
        "composed_call_sha256": current_evidence["composed_call_sha256"],
        "route_outcome_sha256": current_evidence["route_outcome_sha256"],
        "atomic_source_record_sha256": current_evidence[
            "source_record_sha256"
        ],
        "atomic_evidence_binding_sha256": current_evidence[
            "evidence_binding_sha256"
        ],
    }
    request.update(overrides)
    return request


def _expected_rebalance_request(route, *, evidence=None, **overrides):
    current_evidence = evidence or _transfer_evidence(route)
    request = _expected_inventory_request(route)
    request.update({
        "transfer_asset": current_evidence["asset"],
        "transfer_quantity": current_evidence["quantity"],
        "transfer_capacity_quantity": current_evidence[
            "capacity_quantity"
        ],
        "transfer_from_market_id": current_evidence["from_market_id"],
        "transfer_to_market_id": current_evidence["to_market_id"],
        "transfer_from_state_id": current_evidence["from_state_id"],
        "transfer_to_state_id": current_evidence["to_state_id"],
        "transfer_source_record_sha256": current_evidence[
            "source_record_sha256"
        ],
        "transfer_evidence_binding_sha256": current_evidence[
            "evidence_binding_sha256"
        ],
    })
    request.update(overrides)
    return request


class InventoryProfileFixture:
    def row(self, **overrides):
        row = {
            "profile_id": PROFILE_HASH,
            "market_id": BUY_MARKET,
            "asset": "USDT",
            "available_quantity": "10000.00",
            "observed_at": OBSERVED_AT,
            "valid_until": VALID_UNTIL,
            "source_record_sha256": SOURCE_HASH,
        }
        row.update(overrides)
        return row

    def route_rows(self, **sell_overrides):
        sell = self.row(
            market_id=SELL_MARKET,
            asset=TOKEN,
            available_quantity="100.00",
        )
        sell.update(sell_overrides)
        return [self.row(), sell]

    def write_profile(self, directory, rows, *, mode=0o600, name="inventory.csv"):
        path = Path(directory) / name
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=INVENTORY_PROFILE_COLUMNS)
            writer.writeheader()
            writer.writerows(rows)
        path.chmod(mode)
        # macOS exposes /var through a system symlink. The production contract
        # rejects every parent symlink, so fixtures use the canonical path.
        return path.resolve()


class PrivateInventoryProfileTests(unittest.TestCase, InventoryProfileFixture):
    def test_loads_owner_only_profile_from_exact_rows_without_public_secrets(self):
        with tempfile.TemporaryDirectory(
            prefix="PRIVATE_PROFILE_PATH_SENTINEL-"
        ) as directory:
            path = self.write_profile(directory, self.route_rows())
            stdout = io.StringIO()
            stderr = io.StringIO()
            with redirect_stdout(stdout), redirect_stderr(stderr):
                rows = load_validated_inventory_profile(path, now=NOW)

        self.assertEqual(INVENTORY_EVIDENCE_VERSION, "1")
        self.assertEqual(rows, [
            {
                "profile_hash": PROFILE_HASH,
                "market_id": BUY_MARKET,
                "asset": "USDT",
                "available_quantity": "10000",
                "observed_at": OBSERVED_AT,
                "valid_until": VALID_UNTIL,
            },
            {
                "profile_hash": PROFILE_HASH,
                "market_id": SELL_MARKET,
                "asset": TOKEN,
                "available_quantity": "100",
                "observed_at": OBSERVED_AT,
                "valid_until": VALID_UNTIL,
            },
        ])
        serialized = json.dumps(rows, sort_keys=True)
        self.assertNotIn("source_record_sha256", serialized)
        self.assertNotIn(str(path), serialized)
        self.assertEqual(stdout.getvalue(), "")
        self.assertEqual(stderr.getvalue(), "")
        for sentinel in PRIVATE_SENTINELS:
            self.assertNotIn(sentinel, serialized + stdout.getvalue() + stderr.getvalue())

    def test_rejects_non_absolute_symlink_and_non_owner_only_profiles(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self.write_profile(directory, self.route_rows())
            link = Path(directory) / "inventory-link.csv"
            link.symlink_to(path)
            with self.assertRaisesRegex(ValueError, "absolute trusted path"):
                load_validated_inventory_profile(Path(path.name), now=NOW)
            with self.assertRaisesRegex(
                ValueError,
                "regular owner-only|unavailable|symlink",
            ):
                load_validated_inventory_profile(link, now=NOW)

            for mode in (0o640, 0o604, 0o700):
                with self.subTest(mode=oct(mode)):
                    path.chmod(mode)
                    with self.assertRaisesRegex(ValueError, "owner-only"):
                        load_validated_inventory_profile(path, now=NOW)

    def test_rejects_repository_and_traversal_paths_as_untrusted(self):
        project_root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory(dir=project_root) as directory:
            path = self.write_profile(directory, self.route_rows())
            with self.assertRaisesRegex(ValueError, "outside the repository"):
                load_validated_inventory_profile(path, now=NOW)

        with tempfile.TemporaryDirectory() as directory:
            path = self.write_profile(directory, self.route_rows())
            traversal = path.parent / ".." / path.parent.name / path.name
            with self.assertRaisesRegex(ValueError, "absolute trusted path"):
                load_validated_inventory_profile(traversal, now=NOW)

    def test_rejects_hardlink_that_bypasses_repository_boundary(self):
        project_root = Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory(dir=project_root) as repository_directory:
            repository_path = self.write_profile(
                repository_directory,
                self.route_rows(),
            )
            with tempfile.TemporaryDirectory() as external_directory:
                external_path = (
                    Path(external_directory).resolve() / "inventory.csv"
                )
                os.link(repository_path, external_path)
                with self.assertRaisesRegex(
                    ValueError,
                    "single-link|regular owner-only|repository",
                ):
                    load_validated_inventory_profile(external_path, now=NOW)

    def test_rejects_profile_swaps_and_requires_no_follow_support(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self.write_profile(directory, self.route_rows())
            replacement = Path(directory) / "replacement.csv"
            replacement.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
            replacement.chmod(0o600)
            original_open = os.open
            swapped = []

            def open_then_swap(target, flags, *args, **kwargs):
                if os.fspath(target) == path.name:
                    if not swapped:
                        swapped.append(True)
                    elif len(swapped) == 1:
                        swapped.append(True)
                        os.replace(str(replacement), str(path))
                return original_open(target, flags, *args, **kwargs)

            with mock.patch(
                "scripts.route_inventory.os.open",
                side_effect=open_then_swap,
            ):
                with self.assertRaisesRegex(ValueError, "changed"):
                    load_validated_inventory_profile(path, now=NOW)

            path = self.write_profile(directory, self.route_rows())
            with mock.patch("scripts.route_inventory.os.O_NOFOLLOW", None):
                with self.assertRaisesRegex(ValueError, "secure open"):
                    load_validated_inventory_profile(path, now=NOW)

    def test_rejects_parent_symlinks_and_parent_replacement_races(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            real_parent = root / "real-parent"
            real_parent.mkdir()
            path = self.write_profile(real_parent, self.route_rows())
            parent_link = root / "parent-link"
            parent_link.symlink_to(real_parent, target_is_directory=True)

            with self.assertRaisesRegex(
                ValueError,
                "unavailable|changed|symlink",
            ):
                load_validated_inventory_profile(
                    parent_link / path.name,
                    now=NOW,
                )

        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            trusted_parent = root / "trusted-parent"
            alternate_parent = root / "alternate-parent"
            trusted_parent.mkdir()
            alternate_parent.mkdir()
            path = self.write_profile(trusted_parent, self.route_rows())
            alternate_path = alternate_parent / path.name
            alternate_path.write_text(
                path.read_text(encoding="utf-8"),
                encoding="utf-8",
            )
            alternate_path.chmod(0o600)
            # A pinned, reverified dirfd chain must detect and reject the
            # parent replacement even though both files are otherwise valid.
            original_open = os.open
            swapped = []

            def swap_parent_before_file_open(target, flags, *args, **kwargs):
                target_text = os.fspath(target)
                if not swapped and target_text in {str(path), path.name}:
                    swapped.append(True)
                    moved = root / "trusted-parent-original"
                    trusted_parent.rename(moved)
                    trusted_parent.symlink_to(
                        alternate_parent,
                        target_is_directory=True,
                    )
                return original_open(target, flags, *args, **kwargs)

            with mock.patch(
                "scripts.route_inventory.os.open",
                side_effect=swap_parent_before_file_open,
            ):
                with self.assertRaisesRegex(
                    ValueError,
                    "changed|unavailable|symlink",
                ):
                    load_validated_inventory_profile(path, now=NOW)

    def test_rejects_unknown_columns_without_echoing_private_values(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "inventory.csv"
            fieldnames = INVENTORY_PROFILE_COLUMNS + (
                "account_id",
                "wallet_address",
                "api_key",
            )
            row = self.row()
            row.update({
                "account_id": PRIVATE_SENTINELS[0],
                "wallet_address": PRIVATE_SENTINELS[1],
                "api_key": PRIVATE_SENTINELS[2],
            })
            with path.open("w", encoding="utf-8", newline="") as handle:
                writer = csv.DictWriter(handle, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerow(row)
            path.chmod(0o600)

            try:
                load_validated_inventory_profile(path, now=NOW)
            except ValueError as error:
                rendered = "".join(
                    traceback.format_exception(type(error), error, error.__traceback__)
                )
            else:
                self.fail("unknown private columns must fail closed")

        self.assertIn("columns", rendered)
        for sentinel in PRIVATE_SENTINELS[:3]:
            self.assertNotIn(sentinel, rendered)

    def test_rejects_duplicates_mixed_profiles_and_empty_profiles(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self.write_profile(directory, [self.row(), self.row()])
            with self.assertRaisesRegex(ValueError, "duplicate"):
                load_validated_inventory_profile(path, now=NOW)

            rows = self.route_rows()
            rows[1]["profile_id"] = OTHER_PROFILE_HASH
            path = self.write_profile(directory, rows)
            with self.assertRaisesRegex(ValueError, "one opaque profile"):
                load_validated_inventory_profile(path, now=NOW)

            path = self.write_profile(directory, [])
            with self.assertRaisesRegex(ValueError, "at least one row"):
                load_validated_inventory_profile(path, now=NOW)

    def test_rejects_future_expired_and_reversed_validity(self):
        cases = (
            ({"observed_at": "2026-08-01T12:01:00.001Z"}, "future"),
            ({"valid_until": NOW}, "stale"),
            ({"valid_until": OBSERVED_AT}, "after observed_at"),
        )
        with tempfile.TemporaryDirectory() as directory:
            for overrides, message in cases:
                with self.subTest(overrides=overrides):
                    path = self.write_profile(directory, [self.row(**overrides)])
                    with self.assertRaisesRegex(ValueError, message):
                        load_validated_inventory_profile(path, now=NOW)

    def test_rejects_nonopaque_hashes_noncanonical_identity_and_inexact_quantities(self):
        cases = (
            ({"profile_id": "account-123"}, "opaque profile"),
            ({"source_record_sha256": "not-a-hash"}, "source_record_sha256"),
            ({"market_id": "cex:Alpha:AAVE/USDT"}, "market_id"),
            ({"asset": "usdt"}, "asset"),
            ({"available_quantity": "01.0"}, "available_quantity"),
            ({"available_quantity": "1e4"}, "available_quantity"),
            ({"available_quantity": "-1"}, "available_quantity"),
            ({"available_quantity": "NaN"}, "available_quantity"),
        )
        with tempfile.TemporaryDirectory() as directory:
            for overrides, message in cases:
                with self.subTest(overrides=overrides):
                    path = self.write_profile(directory, [self.row(**overrides)])
                    with self.assertRaisesRegex(ValueError, message):
                        load_validated_inventory_profile(path, now=NOW)

    def test_missing_private_path_is_absent_from_exception_trace(self):
        sensitive_path = Path(
            "/private/account/PRIVATE_PROFILE_PATH_SENTINEL/inventory.csv"
        )
        try:
            load_validated_inventory_profile(sensitive_path, now=NOW)
        except ValueError as error:
            rendered = "".join(
                traceback.format_exception(type(error), error, error.__traceback__)
            )
        else:
            self.fail("missing inventory profile must fail closed")

        self.assertNotIn(str(sensitive_path), rendered)
        self.assertNotIn(PRIVATE_SENTINELS[3], rendered)


class InventoryCapacityTests(unittest.TestCase, InventoryProfileFixture):
    def load_rows(self, rows=None, *, now=NOW):
        with tempfile.TemporaryDirectory() as directory:
            path = self.write_profile(directory, rows or self.route_rows())
            return load_validated_inventory_profile(path, now=now)

    def capacity(self, rows, **overrides):
        arguments = {
            "buy_quote_asset": "USDT",
            "buy_quote_quantity": Decimal("9999.99"),
            "sell_token_asset": TOKEN,
            "sell_net_token_quantity": Decimal("99.50"),
            "now": NOW,
        }
        arguments.update(overrides)
        return inventory_capacity_for_route(_route(), rows, **arguments)

    def test_prepositioned_route_requires_buy_quote_and_sell_net_token(self):
        rows = self.load_rows()
        with localcontext() as context:
            context.prec = 4
            result = self.capacity(rows)

        self.assertEqual(result, {
            "inventory_evidence_version": "1",
            "status": "inventory_sufficient",
            "reason_code": None,
            "route_id": _route()["route_id"],
            "buy_market_id": BUY_MARKET,
            "sell_market_id": SELL_MARKET,
            "buy_quote_asset": "USDT",
            "buy_quote_quantity": "9999.99",
            "sell_token_asset": TOKEN,
            "sell_net_token_quantity": "99.5",
            "target_asset": TOKEN,
            "target_quantity": "99.5",
            "inventory_request_sha256": _canonical_sha256(
                _expected_inventory_request()
            ),
            "strict_capacity_asset": TOKEN,
            "strict_capacity_quantity": "99.5",
            "inventory_profile_hash": PROFILE_HASH,
            "observed_at": OBSERVED_AT,
            "valid_until": VALID_UNTIL,
        })

    def test_buy_or_sell_shortfall_is_insufficient_with_no_numeric_capacity(self):
        cases = (
            self.route_rows(available_quantity="99.49"),
            [
                self.row(available_quantity="9999.98"),
                self.row(
                    market_id=SELL_MARKET,
                    asset=TOKEN,
                    available_quantity="100",
                ),
            ],
        )
        for profile_rows in cases:
            with self.subTest(profile_rows=profile_rows):
                result = self.capacity(self.load_rows(profile_rows))
                self.assertEqual(result["status"], "inventory_insufficient")
                self.assertEqual(result["reason_code"], "inventory_insufficient")
                self.assertIsNone(result["strict_capacity_asset"])
                self.assertIsNone(result["strict_capacity_quantity"])
                serialized = json.dumps(result, sort_keys=True)
                self.assertNotIn("available_quantity", serialized)

    def test_missing_wrong_asset_or_expired_record_is_unavailable(self):
        wrong_asset_rows = [
            self.row(asset="USDC"),
            self.row(
                market_id=SELL_MARKET,
                asset=TOKEN,
                available_quantity="100",
            ),
        ]
        loaded_before_expiry = self.load_rows(
            self.route_rows(),
            now="2026-08-01T12:00:30Z",
        )
        cases = (
            [],
            self.load_rows(wrong_asset_rows),
            loaded_before_expiry,
        )
        now_values = (NOW, NOW, VALID_UNTIL)
        for rows, now_value in zip(cases, now_values):
            with self.subTest(rows=rows, now=now_value):
                result = self.capacity(rows, now=now_value)
                self.assertEqual(result["status"], "inventory_unavailable")
                self.assertEqual(result["reason_code"], "inventory_unavailable")
                self.assertIsNone(result["strict_capacity_quantity"])

    def test_route_and_required_quantities_must_be_canonical_and_exact(self):
        rows = self.load_rows()
        bad_route = _route()
        bad_route["buy_market_id"] = "cex:alpha:aave/USDT"
        with self.assertRaisesRegex(ValueError, "market_id"):
            inventory_capacity_for_route(
                bad_route,
                rows,
                buy_quote_asset="USDT",
                buy_quote_quantity=Decimal("10"),
                sell_token_asset=TOKEN,
                sell_net_token_quantity=Decimal("1"),
                now=NOW,
            )
        with self.assertRaisesRegex(ValueError, "buy_quote_asset"):
            self.capacity(rows, buy_quote_asset="USDC")
        with self.assertRaisesRegex(ValueError, "exact Decimal"):
            self.capacity(rows, buy_quote_quantity=9999.99)
        with self.assertRaisesRegex(ValueError, "positive"):
            self.capacity(rows, sell_net_token_quantity=Decimal("0"))

    def test_capacity_projection_never_echoes_private_rows_or_source_hashes(self):
        rows = self.load_rows()
        rows[0]["account_id"] = PRIVATE_SENTINELS[0]
        rows[0]["wallet_address"] = PRIVATE_SENTINELS[1]
        rows[0]["api_key"] = PRIVATE_SENTINELS[2]
        rows[0]["profile_path"] = PRIVATE_SENTINELS[3]

        result = self.capacity(rows)
        serialized = json.dumps(result, sort_keys=True)

        self.assertEqual(result["inventory_profile_hash"], PROFILE_HASH)
        self.assertNotIn(SOURCE_HASH, serialized)
        for sentinel in PRIVATE_SENTINELS:
            self.assertNotIn(sentinel, serialized)

    def test_dex_buy_requires_authoritative_market_rules_quantity_quote(self):
        route = _route()
        route["buy_market_id"] = DEX_BUY_MARKET
        route["route_id"] = "route:{}:{}->{}:{}".format(
            TOKEN,
            DEX_BUY_MARKET,
            SELL_MARKET,
            route["route_mode"],
        )
        profile_rows = [
            self.row(
                market_id=DEX_BUY_MARKET,
                asset="WETH",
                available_quantity="5",
            ),
            self.row(
                market_id=SELL_MARKET,
                asset=TOKEN,
                available_quantity="100",
            ),
        ]
        rows = self.load_rows(profile_rows)
        arguments = {
            "buy_quote_asset": "WETH",
            "buy_quote_quantity": Decimal("4.5"),
            "sell_token_asset": TOKEN,
            "sell_net_token_quantity": Decimal("99.5"),
            "now": NOW,
        }

        absent = inventory_capacity_for_route(route, rows, **arguments)
        self.assertEqual(absent["status"], "inventory_unavailable")
        self.assertEqual(
            absent["reason_code"],
            "dex_buy_quantity_quote_unavailable",
        )

        quote = {
            "evidence_type": "market_rules_quantity_quote",
            "market_id": DEX_BUY_MARKET,
            "base_asset": TOKEN,
            "quote_asset": "WETH",
            "quote_debit_asset": "WETH",
            "quote_debit_quantity": "4.5",
            "target_base_asset": TOKEN,
            "target_base_quantity": "99.5",
            "market_rules_sha256": SOURCE_HASH,
            "quantity_quote_sha256": OTHER_PROFILE_HASH,
            "observed_at": OBSERVED_AT,
            "valid_until": VALID_UNTIL,
            "source_record_sha256": "d" * 64,
        }
        wrong_quantity = inventory_capacity_for_route(
            route,
            rows,
            dex_buy_quantity_quote={
                **quote,
                "quote_debit_quantity": "4.4",
            },
            **arguments,
        )
        self.assertEqual(
            wrong_quantity["reason_code"],
            "dex_buy_quantity_quote_unavailable",
        )

        self_signed = inventory_capacity_for_route(
            route,
            rows,
            dex_buy_quantity_quote=quote,
            **arguments,
        )
        self.assertEqual(self_signed["status"], "inventory_unavailable")
        self.assertEqual(
            self_signed["reason_code"],
            "dex_buy_quantity_quote_unavailable",
        )

        integrity_bound_quote = {
            **quote,
            "evidence_binding_sha256": _evidence_binding_sha256(quote),
        }
        integrity_only = inventory_capacity_for_route(
            route,
            rows,
            dex_buy_quantity_quote=integrity_bound_quote,
            **arguments,
        )
        self.assertEqual(integrity_only["status"], "inventory_unavailable")
        self.assertEqual(
            integrity_only["reason_code"],
            "dex_buy_authoritative_upstream_unavailable",
        )

    def test_inventory_evidence_cannot_replay_across_route_or_quantity(self):
        rows = self.load_rows()
        evidence = self.capacity(rows)

        same = classify_route_mode_evidence(
            _route(),
            expected_request=_expected_inventory_request(),
            inventory_evidence=evidence,
            now=NOW,
        )
        changed_quantity = classify_route_mode_evidence(
            _route(),
            expected_request=_expected_inventory_request(
                sell_net_token_quantity="99.6",
                target_quantity="99.6",
            ),
            inventory_evidence=evidence,
            now=NOW,
        )
        replay_route = _route()
        replay_route["sell_market_id"] = "cex:gamma:AAVE/USDT"
        replay_route["route_id"] = "route:{}:{}->{}:{}".format(
            TOKEN,
            BUY_MARKET,
            replay_route["sell_market_id"],
            replay_route["route_mode"],
        )
        changed_route = classify_route_mode_evidence(
            replay_route,
            expected_request=_expected_inventory_request(replay_route),
            inventory_evidence=evidence,
            now=NOW,
        )

        self.assertEqual(same["classification"], "mode_evidence_eligible")
        self.assertTrue(same["mode_evidence_eligible"])
        for result in (changed_quantity, changed_route):
            self.assertEqual(result["classification"], "research_estimate")
            self.assertFalse(result["mode_evidence_eligible"])
            self.assertEqual(result["reason_code"], "inventory_request_mismatch")


class RouteModeEvidenceTests(unittest.TestCase, InventoryProfileFixture):
    def sufficient_inventory(self, *, mode="prepositioned_inventory"):
        with tempfile.TemporaryDirectory() as directory:
            path = self.write_profile(directory, self.route_rows())
            rows = load_validated_inventory_profile(path, now=NOW)
        return inventory_capacity_for_route(
            _route(mode),
            rows,
            buy_quote_asset="USDT",
            buy_quote_quantity=Decimal("9999.99"),
            sell_token_asset=TOKEN,
            sell_net_token_quantity=Decimal("99.5"),
            now=NOW,
        )

    def transfer_evidence(self, route, **overrides):
        return _transfer_evidence(route, **overrides)

    def composed_simulation(self, route, **overrides):
        return _atomic_evidence(route, **overrides)

    def test_prepositioned_inventory_controls_strict_eligibility(self):
        sufficient = self.sufficient_inventory()
        executable = classify_route_mode_evidence(
            _route(),
            expected_request=_expected_inventory_request(),
            inventory_evidence=sufficient,
            now=NOW,
        )
        unavailable = classify_route_mode_evidence(
            _route(),
            expected_request=_expected_inventory_request(),
            inventory_evidence={
                **sufficient,
                "status": "inventory_unavailable",
                "reason_code": "inventory_unavailable",
                "strict_capacity_asset": None,
                "strict_capacity_quantity": None,
            },
            now=NOW,
        )

        self.assertEqual(executable["classification"], "mode_evidence_eligible")
        self.assertTrue(executable["mode_evidence_eligible"])
        self.assertEqual(executable["reason_codes"], [])
        self.assertEqual(executable["maximum_proved_capacity_quantity"], "99.5")
        self.assertIsNone(unavailable["maximum_proved_capacity_quantity"])
        self.assertEqual(unavailable["classification"], "research_estimate")
        self.assertFalse(unavailable["mode_evidence_eligible"])
        self.assertEqual(unavailable["reason_code"], "inventory_unavailable")

    def test_independent_dex_quotes_never_prove_atomic_route(self):
        route = _atomic_route()
        independent_quotes = {
            **self.composed_simulation(route),
            "evidence_type": "independent_dex_leg_quotes",
        }

        result = classify_route_mode_evidence(
            route,
            expected_request=_expected_atomic_request(route),
            atomic_route_simulation=independent_quotes,
            now=NOW,
        )

        self.assertEqual(result["classification"], "research_estimate")
        self.assertFalse(result["mode_evidence_eligible"])
        self.assertEqual(
            result["reason_code"],
            "atomic_route_simulation_unavailable",
        )

    def test_current_composed_same_state_simulation_can_prove_atomic_route(self):
        route = _atomic_route()
        result = classify_route_mode_evidence(
            route,
            expected_request=_expected_atomic_request(route),
            atomic_route_simulation=self.composed_simulation(route),
            now=NOW,
        )

        self.assertEqual(result["classification"], "mode_evidence_eligible")
        self.assertTrue(result["mode_evidence_eligible"])
        self.assertIsNone(result["reason_code"])
        self.assertEqual(result["maximum_proved_capacity_quantity"], "99.5")

    def test_atomic_evidence_is_bound_to_expected_state_target_call_and_outcome(self):
        route = _atomic_route()
        valid = self.composed_simulation(route)
        cases = (
            _expected_atomic_request(
                route,
                cohort_state_id="cohort-other",
            ),
            _expected_atomic_request(route, target_quantity="99.6"),
            _expected_atomic_request(route, composed_call_sha256="e" * 64),
            _expected_atomic_request(route, route_outcome_sha256="f" * 64),
        )
        for expected in cases:
            with self.subTest(expected=expected):
                result = classify_route_mode_evidence(
                    route,
                    expected_request=expected,
                    atomic_route_simulation=valid,
                    now=NOW,
                )
                self.assertEqual(result["classification"], "research_estimate")
                self.assertFalse(result["mode_evidence_eligible"])
                self.assertEqual(
                    result["reason_code"],
                    "atomic_route_simulation_unavailable",
                )

        self_report_only = {
            **valid,
            "same_cohort_state": True,
        }
        rejected = classify_route_mode_evidence(
            route,
            expected_request=_expected_atomic_request(route),
            atomic_route_simulation=self_report_only,
            now=NOW,
        )
        self.assertFalse(rejected["mode_evidence_eligible"])

    def test_atomic_binding_rejects_coordinated_field_changes_with_reused_hash(self):
        route = _atomic_route()
        original = self.composed_simulation(route)
        tampered = self.composed_simulation(
            route,
            target_quantity="123.456",
            composed_call_sha256="e" * 64,
            route_outcome_sha256="f" * 64,
            evidence_binding_sha256=original["evidence_binding_sha256"],
        )
        expected = _expected_atomic_request(route, evidence=tampered)

        result = classify_route_mode_evidence(
            route,
            expected_request=expected,
            atomic_route_simulation=tampered,
            now=NOW,
        )

        self.assertEqual(result["classification"], "research_estimate")
        self.assertFalse(result["mode_evidence_eligible"])
        self.assertEqual(
            result["reason_code"],
            "atomic_route_simulation_unavailable",
        )

    def test_cross_chain_or_stale_composed_simulation_remains_estimate_only(self):
        cross_chain = _atomic_route(sell_chain="arb")
        cross_chain_result = classify_route_mode_evidence(
            cross_chain,
            expected_request=_expected_atomic_request(cross_chain),
            atomic_route_simulation=self.composed_simulation(cross_chain),
            now=NOW,
        )
        route = _atomic_route()
        stale_result = classify_route_mode_evidence(
            route,
            expected_request=_expected_atomic_request(route),
            atomic_route_simulation=self.composed_simulation(
                route,
                valid_until=NOW,
            ),
            now=NOW,
        )

        self.assertEqual(
            cross_chain_result["reason_code"],
            "unsupported_cross_chain_settlement",
        )
        self.assertEqual(
            stale_result["reason_code"],
            "atomic_route_simulation_unavailable",
        )
        self.assertFalse(cross_chain_result["mode_evidence_eligible"])
        self.assertFalse(stale_result["mode_evidence_eligible"])

    def test_rebalance_requires_both_current_transfer_and_inventory_evidence(self):
        route = _route("rebalance_required")
        inventory = self.sufficient_inventory(mode="rebalance_required")
        transfer = self.transfer_evidence(route)

        expected = _expected_rebalance_request(route)
        missing_both = classify_route_mode_evidence(
            route,
            expected_request=expected,
            now=NOW,
        )
        missing_transfer = classify_route_mode_evidence(
            route,
            expected_request=expected,
            inventory_evidence=inventory,
            now=NOW,
        )
        missing_inventory = classify_route_mode_evidence(
            route,
            expected_request=expected,
            transfer_evidence=transfer,
            now=NOW,
        )
        complete = classify_route_mode_evidence(
            route,
            expected_request=expected,
            inventory_evidence=inventory,
            transfer_evidence=transfer,
            now=NOW,
        )

        for result in (missing_both, missing_transfer, missing_inventory):
            self.assertEqual(result["classification"], "research_estimate")
            self.assertFalse(result["mode_evidence_eligible"])
        self.assertEqual(missing_both["reason_codes"], [
            "inventory_unavailable",
            "rebalance_transfer_evidence_unavailable",
        ])
        self.assertEqual(
            missing_transfer["reason_code"],
            "rebalance_transfer_evidence_unavailable",
        )
        self.assertEqual(missing_inventory["reason_code"], "inventory_unavailable")
        self.assertEqual(complete["classification"], "mode_evidence_eligible")
        self.assertTrue(complete["mode_evidence_eligible"])
        self.assertEqual(complete["maximum_proved_capacity_quantity"], "99.5")

    def test_rebalance_transfer_binds_route_asset_quantity_markets_states_and_capacity(self):
        route = _route("rebalance_required")
        inventory = self.sufficient_inventory(mode="rebalance_required")
        expected = _expected_rebalance_request(route)
        valid = self.transfer_evidence(route)
        cases = (
            {**valid, "asset": "USDT"},
            {**valid, "quantity": "99.4"},
            {**valid, "capacity_quantity": "99.49"},
            {**valid, "from_market_id": SELL_MARKET},
            {**valid, "to_market_id": BUY_MARKET},
            {**valid, "from_state_id": "other-buy-state"},
            {**valid, "to_state_id": "other-sell-state"},
        )
        for transfer in cases:
            with self.subTest(transfer=transfer):
                result = classify_route_mode_evidence(
                    route,
                    expected_request=expected,
                    inventory_evidence=inventory,
                    transfer_evidence=transfer,
                    now=NOW,
                )
                self.assertEqual(result["classification"], "research_estimate")
                self.assertFalse(result["mode_evidence_eligible"])
                self.assertEqual(
                    result["reason_code"],
                    "rebalance_transfer_evidence_unavailable",
                )

    def test_transfer_binding_rejects_coordinated_changes_with_reused_hash(self):
        route = _route("rebalance_required")
        inventory = self.sufficient_inventory(mode="rebalance_required")
        original = self.transfer_evidence(route)
        tampered = self.transfer_evidence(
            route,
            quantity="99.5",
            capacity_quantity="150",
            from_state_id="buy-state-other",
            to_state_id="sell-state-other",
            evidence_binding_sha256=original["evidence_binding_sha256"],
        )
        expected = _expected_rebalance_request(route, evidence=tampered)

        result = classify_route_mode_evidence(
            route,
            expected_request=expected,
            inventory_evidence=inventory,
            transfer_evidence=tampered,
            now=NOW,
        )

        self.assertEqual(result["classification"], "research_estimate")
        self.assertFalse(result["mode_evidence_eligible"])
        self.assertEqual(
            result["reason_code"],
            "rebalance_transfer_evidence_unavailable",
        )

    def test_route_mode_projection_drops_all_private_evidence_fields(self):
        route = _route("rebalance_required")
        inventory = self.sufficient_inventory(mode="rebalance_required")
        transfer = self.transfer_evidence(route)
        transfer.update({
            "account_id": PRIVATE_SENTINELS[0],
            "wallet_address": PRIVATE_SENTINELS[1],
            "api_key": PRIVATE_SENTINELS[2],
            "profile_path": PRIVATE_SENTINELS[3],
        })

        result = classify_route_mode_evidence(
            route,
            expected_request=_expected_rebalance_request(route),
            inventory_evidence=inventory,
            transfer_evidence=transfer,
            now=NOW,
        )
        serialized = json.dumps(result, sort_keys=True)

        for sentinel in PRIVATE_SENTINELS:
            self.assertNotIn(sentinel, serialized)


if __name__ == "__main__":
    unittest.main()
