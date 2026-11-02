"""CEX fee fact adapters, private-profile, and redaction tests."""

from __future__ import annotations

import csv
import json
import os
import stat
import tempfile
import traceback
import unittest
from datetime import datetime
from decimal import Decimal, localcontext
from pathlib import Path
from urllib.parse import urlparse
from unittest import mock

import scripts.cex_fee_facts as fee_facts
from scripts.cex_fee_facts import (
    PRIVATE_FEE_PROFILE_COLUMNS,
    PUBLIC_FEE_SCHEDULE_COLUMNS,
    collect_cex_fee_snapshot,
    load_validated_fee_profile,
    normalize_binance_taker_fee,
    normalize_bybit_taker_fee,
    normalize_okx_taker_fee,
)


PROFILE_ID = "a" * 64
OBSERVED_AT = "2026-08-01T12:00:00Z"
VALID_UNTIL = "2026-08-01T12:05:00Z"
NOW = "2026-08-01T12:01:00Z"
HASH = "b" * 64
SENTINELS = (
    "API_KEY_SENTINEL",
    "SECRET_SENTINEL",
    "PASSPHRASE_SENTINEL",
    "ACCOUNT_ID_SENTINEL",
    "AUTHORIZATION_SENTINEL",
    "/private/account/fee-profile.csv",
)


def binance_fixture():
    return {
        "symbol": "AAVEUSDT",
        "standardCommission": {
            "maker": "0.0008",
            "taker": "0.001",
            "buyer": "0.0001",
            "seller": "0.0002",
        },
        "specialCommission": {
            "maker": "0",
            "taker": "0.00002",
            "buyer": "0.00003",
            "seller": "0.00004",
        },
        "taxCommission": {
            "maker": "0",
            "taker": "0.000001",
            "buyer": "0.000002",
            "seller": "0.000003",
        },
        "discount": {
            "enabledForAccount": True,
            "enabledForSymbol": True,
            "discountAsset": "BNB",
            "discount": "0.75",
        },
        "apiKey": SENTINELS[0],
        "secret": SENTINELS[1],
        "accountId": SENTINELS[3],
        "Authorization": SENTINELS[4],
        "privateContext": {
            "passphrase": SENTINELS[2],
            "profilePath": SENTINELS[5],
        },
    }


def bybit_fixture():
    return {
        "retCode": 0,
        "retMsg": "OK",
        "result": {
            "category": "spot",
            "list": [
                {
                    "symbol": "AAVEUSDT",
                    "takerFeeRate": "0.0006",
                    "makerFeeRate": "0.0001",
                    "accountId": SENTINELS[3],
                }
            ],
        },
        "time": 1785585600000,
        "apiKey": SENTINELS[0],
        "secret": SENTINELS[1],
        "privateContext": {
            "passphrase": SENTINELS[2],
            "authorization": SENTINELS[4],
            "profilePath": SENTINELS[5],
        },
    }


def okx_fixture():
    return {
        "code": "0",
        "msg": "",
        "data": [
            {
                "instType": "SPOT",
                "level": "Lv1",
                "maker": "-0.0008",
                "taker": "-0.001",
                "ts": "1785585600000",
                "passphrase": SENTINELS[2],
            }
        ],
        "Authorization": SENTINELS[4],
        "privateContext": {
            "apiKey": SENTINELS[0],
            "secret": SENTINELS[1],
            "accountId": SENTINELS[3],
            "profilePath": SENTINELS[5],
        },
    }


class OfficialResponseNormalizerTests(unittest.TestCase):
    def test_binance_combines_side_rates_and_discount_exactly(self):
        evidence = normalize_binance_taker_fee(
            binance_fixture(),
            side="buy",
            profile_id=PROFILE_ID,
            observed_at=OBSERVED_AT,
            valid_until=VALID_UNTIL,
            discount_asset_funded=True,
        )

        # ((0.001 + 0.0001) * 0.75 + 0.00002 + 0.00003
        #  + 0.000001 + 0.000002) * 10_000 = 8.78 bps.
        self.assertEqual(evidence["venue"], "binance")
        self.assertEqual(evidence["instrument"], "AAVEUSDT")
        self.assertEqual(evidence["side"], "buy")
        self.assertEqual(evidence["taker_fee_bps"], "8.78")
        self.assertEqual(evidence["fee_asset"], "BNB")
        self.assertEqual(evidence["observed_at"], OBSERVED_AT)
        self.assertEqual(evidence["valid_until"], VALID_UNTIL)
        self.assertEqual(evidence["profile_id"], PROFILE_ID)
        self.assertRegex(evidence["source_record_sha256"], r"^[0-9a-f]{64}$")
        self.assertIn("standard", evidence["basis"])
        self.assertIn("special", evidence["basis"])
        self.assertIn("tax", evidence["basis"])
        self.assertIn("discount", evidence["basis"])
        self.assertNotIn("maker", evidence["basis"])

        serialized = json.dumps(evidence, sort_keys=True)
        for sentinel in SENTINELS:
            self.assertNotIn(sentinel, serialized)

    def test_binance_requires_explicit_discount_funding_state(self):
        with self.assertRaisesRegex(ValueError, "discount_asset_funded"):
            normalize_binance_taker_fee(
                binance_fixture(),
                side="sell",
                profile_id=PROFILE_ID,
                observed_at=OBSERVED_AT,
                valid_until=VALID_UNTIL,
                discount_asset_funded=None,
            )

    def test_binance_rejects_unproved_discount_fee_asset(self):
        response = binance_fixture()
        response["discount"]["discountAsset"] = "DOGE"
        with self.assertRaisesRegex(ValueError, "BNB"):
            normalize_binance_taker_fee(
                response,
                side="buy",
                profile_id=PROFILE_ID,
                observed_at=OBSERVED_AT,
                valid_until=VALID_UNTIL,
                discount_asset_funded=True,
            )

    def test_binance_without_discount_uses_received_asset(self):
        response = binance_fixture()
        response["discount"]["enabledForSymbol"] = False
        evidence = normalize_binance_taker_fee(
            response,
            side="sell",
            profile_id=PROFILE_ID,
            observed_at=OBSERVED_AT,
            valid_until=VALID_UNTIL,
            discount_asset_funded=False,
            received_asset="USDT",
        )

        self.assertEqual(evidence["taker_fee_bps"], "12.64")
        self.assertEqual(evidence["fee_asset"], "USDT")

    def test_bybit_binds_instrument_side_fee_asset_and_response_time(self):
        evidence = normalize_bybit_taker_fee(
            bybit_fixture(),
            instrument="AAVEUSDT",
            side="buy",
            fee_asset="AAVE",
            profile_id=PROFILE_ID,
            valid_until=VALID_UNTIL,
        )

        self.assertEqual(evidence["venue"], "bybit")
        self.assertEqual(evidence["instrument"], "AAVEUSDT")
        self.assertEqual(evidence["side"], "buy")
        self.assertEqual(evidence["taker_fee_bps"], "6")
        self.assertEqual(evidence["fee_asset"], "AAVE")
        self.assertEqual(evidence["observed_at"], OBSERVED_AT)
        self.assertEqual(evidence["profile_id"], PROFILE_ID)
        self.assertIn("authenticated spot takerFeeRate", evidence["basis"])

        serialized = json.dumps(evidence, sort_keys=True)
        for sentinel in SENTINELS:
            self.assertNotIn(sentinel, serialized)

    def test_bybit_rejects_error_and_ambiguous_instrument(self):
        response = bybit_fixture()
        response["result"]["list"].append(
            {"symbol": "BTCUSDT", "takerFeeRate": "0.001", "makerFeeRate": "0.001"}
        )
        with self.assertRaisesRegex(ValueError, "instrument"):
            normalize_bybit_taker_fee(
                response,
                instrument="ETHUSDT",
                side="sell",
                fee_asset="USDT",
                profile_id=PROFILE_ID,
                valid_until=VALID_UNTIL,
            )
        response["retCode"] = 10001
        with self.assertRaisesRegex(ValueError, "success"):
            normalize_bybit_taker_fee(
                response,
                instrument="AAVEUSDT",
                side="sell",
                fee_asset="USDT",
                profile_id=PROFILE_ID,
                valid_until=VALID_UNTIL,
            )

    def test_bybit_requires_exact_spot_category(self):
        for category in ("linear", "SPOT", None):
            response = bybit_fixture()
            if category is None:
                del response["result"]["category"]
            else:
                response["result"]["category"] = category
            with self.subTest(category=category):
                with self.assertRaisesRegex(ValueError, "exactly spot"):
                    normalize_bybit_taker_fee(
                        response,
                        instrument="AAVEUSDT",
                        side="sell",
                        fee_asset="USDT",
                        profile_id=PROFILE_ID,
                        valid_until=VALID_UNTIL,
                    )

    def test_okx_converts_negative_commission_to_positive_exact_cost(self):
        evidence = normalize_okx_taker_fee(
            okx_fixture(),
            instrument="AAVE-USDT",
            side="sell",
            fee_asset="USDT",
            profile_id=PROFILE_ID,
            valid_until=VALID_UNTIL,
        )

        self.assertEqual(evidence["venue"], "okx")
        self.assertEqual(evidence["instrument"], "AAVE-USDT")
        self.assertEqual(evidence["side"], "sell")
        self.assertEqual(evidence["taker_fee_bps"], "10")
        self.assertEqual(evidence["fee_asset"], "USDT")
        self.assertEqual(evidence["observed_at"], OBSERVED_AT)
        self.assertIn("negative commission encoding", evidence["basis"])
        self.assertNotIn("Lv1", json.dumps(evidence, sort_keys=True))

        serialized = json.dumps(evidence, sort_keys=True)
        for sentinel in SENTINELS:
            self.assertNotIn(sentinel, serialized)

    def test_okx_rejects_rebate_as_nonnegative_cost(self):
        response = okx_fixture()
        response["data"][0]["taker"] = "0.0001"
        with self.assertRaisesRegex(ValueError, "rebate"):
            normalize_okx_taker_fee(
                response,
                instrument="AAVE-USDT",
                side="sell",
                fee_asset="USDT",
                profile_id=PROFILE_ID,
                valid_until=VALID_UNTIL,
            )

    def test_float_nonfinite_and_nonopaque_values_are_rejected(self):
        response = bybit_fixture()
        response["result"]["list"][0]["takerFeeRate"] = 0.0006
        with self.assertRaisesRegex(ValueError, "exact Decimal"):
            normalize_bybit_taker_fee(
                response,
                instrument="AAVEUSDT",
                side="buy",
                fee_asset="AAVE",
                profile_id=PROFILE_ID,
                valid_until=VALID_UNTIL,
            )
        response = bybit_fixture()
        with self.assertRaisesRegex(ValueError, "opaque"):
            normalize_bybit_taker_fee(
                response,
                instrument="AAVEUSDT",
                side="buy",
                fee_asset="AAVE",
                profile_id="account-123",
                valid_until=VALID_UNTIL,
            )


class PrivateFeeProfileTests(unittest.TestCase):
    def write_profile(self, directory, rows, mode=0o600):
        path = Path(directory) / "private-fees.csv"
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=PRIVATE_FEE_PROFILE_COLUMNS)
            writer.writeheader()
            writer.writerows(rows)
        path.chmod(mode)
        return path

    def row(self, **overrides):
        values = {
            "profile_id": PROFILE_ID,
            "venue": "crypto_com",
            "instrument": "AAVE/USDT",
            "side": "buy",
            "taker_fee_bps": "7.5",
            "fee_asset": "AAVE",
            "basis": "authenticated_taker_fee",
            "observed_at": OBSERVED_AT,
            "valid_until": VALID_UNTIL,
            "source_record_sha256": HASH,
        }
        values.update(overrides)
        return values

    def test_loads_owner_only_fresh_generic_profile(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self.write_profile(directory, [self.row()])
            rows = load_validated_fee_profile(path, now=NOW)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["taker_fee_bps"], "7.5")
        self.assertEqual(rows[0]["profile_id"], PROFILE_ID)
        self.assertEqual(
            rows[0]["basis"],
            "validated authenticated taker fee on requested notional",
        )
        serialized = json.dumps(rows, sort_keys=True)
        self.assertNotIn(str(path), serialized)
        for sentinel in SENTINELS:
            self.assertNotIn(sentinel, serialized)

    def test_rejects_group_or_world_readable_profiles(self):
        with tempfile.TemporaryDirectory() as directory:
            for mode in (0o640, 0o604):
                with self.subTest(mode=oct(mode)):
                    path = self.write_profile(directory, [self.row()], mode=mode)
                    with self.assertRaisesRegex(ValueError, "owner-only"):
                        load_validated_fee_profile(path, now=NOW)

    def test_rejects_symlinks_duplicates_stale_and_nonopaque_ids(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self.write_profile(directory, [self.row()])
            link = Path(directory) / "fees-link.csv"
            link.symlink_to(path)
            with self.assertRaisesRegex(ValueError, "regular owner-only"):
                load_validated_fee_profile(link, now=NOW)

            path = self.write_profile(directory, [self.row(), self.row()])
            with self.assertRaisesRegex(ValueError, "duplicate"):
                load_validated_fee_profile(path, now=NOW)

            path = self.write_profile(
                directory,
                [self.row(valid_until="2026-08-01T12:00:30Z")],
            )
            with self.assertRaisesRegex(ValueError, "stale"):
                load_validated_fee_profile(path, now=NOW)

            path = self.write_profile(
                directory,
                [self.row(profile_id="account-123")],
            )
            with self.assertRaisesRegex(ValueError, "opaque"):
                load_validated_fee_profile(path, now=NOW)

    def test_rejects_unknown_columns(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "private-fees.csv"
            with path.open("w", encoding="utf-8", newline="") as handle:
                handle.write(
                    ",".join(PRIVATE_FEE_PROFILE_COLUMNS + ("api_key",)) + "\n"
                )
            path.chmod(0o600)
            with self.assertRaisesRegex(ValueError, "columns"):
                load_validated_fee_profile(path, now=NOW)

            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)

    def test_rejects_uncontrolled_basis_and_wrong_received_fee_asset(self):
        with tempfile.TemporaryDirectory() as directory:
            for basis in (
                "SECRET_SENTINEL",
                "account=ACCOUNT_ID_SENTINEL",
                "/private/account/fee-profile.csv",
            ):
                with self.subTest(basis=basis):
                    path = self.write_profile(
                        directory,
                        [self.row(basis=basis)],
                    )
                    with self.assertRaisesRegex(ValueError, "basis code"):
                        load_validated_fee_profile(path, now=NOW)

            path = self.write_profile(
                directory,
                [self.row(fee_asset="DOGE")],
            )
            with self.assertRaisesRegex(ValueError, "received fee_asset"):
                load_validated_fee_profile(path, now=NOW)

    def test_rejects_profile_swapped_after_path_check(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self.write_profile(directory, [self.row()])
            replacement = Path(directory) / "replacement.csv"
            replacement.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
            replacement.chmod(0o600)
            original_lstat = os.lstat
            swapped = []

            def lstat_then_swap(target):
                metadata = original_lstat(target)
                if not swapped and str(target) == str(path):
                    swapped.append(True)
                    path.unlink()
                    path.symlink_to(replacement)
                return metadata

            with mock.patch(
                "scripts.cex_fee_facts.os.lstat",
                side_effect=lstat_then_swap,
            ):
                with self.assertRaisesRegex(
                    ValueError, "changed|regular owner-only|unavailable"
                ):
                    load_validated_fee_profile(path, now=NOW)

    def test_rejects_regular_file_inode_swap_after_path_check(self):
        with tempfile.TemporaryDirectory() as directory:
            path = self.write_profile(directory, [self.row()])
            replacement = Path(directory) / "replacement.csv"
            replacement.write_text(path.read_text(encoding="utf-8"), encoding="utf-8")
            replacement.chmod(0o600)
            original_lstat = os.lstat
            swapped = []

            def lstat_then_swap(target):
                metadata = original_lstat(target)
                if not swapped and str(target) == str(path):
                    swapped.append(True)
                    os.replace(str(replacement), str(path))
                return metadata

            with mock.patch(
                "scripts.cex_fee_facts.os.lstat",
                side_effect=lstat_then_swap,
            ):
                with self.assertRaisesRegex(ValueError, "changed"):
                    load_validated_fee_profile(path, now=NOW)

    def test_missing_private_path_is_absent_from_exception_trace(self):
        sensitive_path = "/private/account/SECRET_SENTINEL/fees.csv"
        try:
            load_validated_fee_profile(sensitive_path, now=NOW)
        except ValueError as error:
            rendered = "".join(
                traceback.format_exception(type(error), error, error.__traceback__)
            )
        else:
            self.fail("missing private profile must fail")
        self.assertNotIn(sensitive_path, rendered)
        self.assertNotIn("SECRET_SENTINEL", rendered)


class FeeCollectorTests(unittest.TestCase):
    def common(self):
        return {
            "cohort_id": "cohort-1",
            "opportunity_id": "route-1:10000",
            "leg": "buy",
            "market_id": "cex:binance:AAVE/USDT",
            "venue": "binance",
            "instrument": "AAVE/USDT",
            "side": "buy",
            "requested_notional_usd": Decimal("10000"),
            "target_token_quantity": Decimal("100"),
            "now": NOW,
        }

    def test_authenticated_client_projects_exact_component(self):
        class Client:
            def fetch_authenticated_fee(self, *, venue, instrument):
                self.last_call = (venue, instrument)
                return binance_fixture()

        client = Client()
        messages = []
        row = collect_cex_fee_snapshot(
            **self.common(),
            client=client,
            profile_id=PROFILE_ID,
            observed_at=OBSERVED_AT,
            valid_until=VALID_UNTIL,
            discount_asset_funded=True,
            logger=messages.append,
        )

        self.assertEqual(client.last_call, ("binance", "AAVEUSDT"))
        self.assertEqual(row["value_status"], "authenticated")
        self.assertIs(row["strict_eligible"], True)
        self.assertEqual(row["rate_bps"], "8.78")
        self.assertEqual(row["amount_usd"], "8.78")
        self.assertIn("fee_asset=BNB", row["basis"])
        combined = json.dumps(row, sort_keys=True) + "\n" + "\n".join(messages)
        for sentinel in SENTINELS:
            self.assertNotIn(sentinel, combined)

    def test_missing_authentication_is_unavailable_not_default_fee(self):
        row = collect_cex_fee_snapshot(**self.common())

        self.assertEqual(row["value_status"], "unavailable")
        self.assertIs(row["strict_eligible"], False)
        self.assertIsNone(row["rate_bps"])
        self.assertIsNone(row["amount_usd"])
        self.assertEqual(row["reason_code"], "cex_fee_authentication_missing")

    def test_authenticated_evidence_is_owned_by_explicit_now_interval(self):
        class Client:
            def fetch_authenticated_fee(self, *, venue, instrument):
                return bybit_fixture()

        common = self.common()
        common.update(
            venue="bybit",
            market_id="cex:bybit:AAVE/USDT",
        )

        at_observation = dict(common, now=OBSERVED_AT)
        row = collect_cex_fee_snapshot(
            **at_observation,
            client=Client(),
            profile_id=PROFILE_ID,
            valid_until=VALID_UNTIL,
            fee_asset="AAVE",
        )
        self.assertEqual(row["value_status"], "authenticated")
        self.assertIs(row["strict_eligible"], True)

        future = dict(common, now="2026-08-01T11:59:59Z")
        row = collect_cex_fee_snapshot(
            **future,
            client=Client(),
            profile_id=PROFILE_ID,
            valid_until=VALID_UNTIL,
            fee_asset="AAVE",
        )
        self.assertEqual(row["value_status"], "failed")
        self.assertIs(row["strict_eligible"], False)
        self.assertEqual(row["reason_code"], "cex_fee_observation_in_future")

        at_expiry = dict(common, now=VALID_UNTIL)
        row = collect_cex_fee_snapshot(
            **at_expiry,
            client=Client(),
            profile_id=PROFILE_ID,
            valid_until=VALID_UNTIL,
            fee_asset="AAVE",
        )
        self.assertEqual(row["value_status"], "stale")
        self.assertIs(row["strict_eligible"], False)
        self.assertEqual(row["reason_code"], "cex_fee_evidence_expired")

    def test_market_id_venue_and_canonical_instrument_must_match(self):
        class Client:
            calls = 0

            def fetch_authenticated_fee(self, *, venue, instrument):
                self.calls += 1
                return binance_fixture()

        client = Client()
        cases = (
            {"venue": "bybit"},
            {"instrument": "AAVE/USDC"},
            {"market_id": "cex:binance:AAVEUSDT"},
            {"market_id": "cex:binance:aave/USDT"},
        )
        for overrides in cases:
            values = self.common()
            values.update(overrides)
            with self.subTest(overrides=overrides):
                with self.assertRaisesRegex(ValueError, "market_id|venue|instrument"):
                    collect_cex_fee_snapshot(
                        **values,
                        client=client,
                        profile_id=PROFILE_ID,
                        observed_at=OBSERVED_AT,
                        valid_until=VALID_UNTIL,
                        discount_asset_funded=True,
                    )
        self.assertEqual(client.calls, 0)

    def test_fee_asset_is_derived_and_arbitrary_doge_is_rejected(self):
        class Client:
            calls = 0

            def fetch_authenticated_fee(self, *, venue, instrument):
                self.calls += 1
                return bybit_fixture()

        common = self.common()
        common.update(
            venue="bybit",
            market_id="cex:bybit:AAVE/USDT",
        )
        client = Client()
        with self.assertRaisesRegex(ValueError, "fee_asset"):
            collect_cex_fee_snapshot(
                **common,
                client=client,
                profile_id=PROFILE_ID,
                valid_until=VALID_UNTIL,
                fee_asset="DOGE",
            )
        self.assertEqual(client.calls, 0)

    def test_okx_sell_derives_quote_fee_asset_and_native_instrument(self):
        class Client:
            def fetch_authenticated_fee(self, *, venue, instrument):
                self.last_call = (venue, instrument)
                return okx_fixture()

        client = Client()
        common = self.common()
        common.update(
            leg="sell",
            side="sell",
            venue="okx",
            market_id="cex:okx:AAVE/USDT",
        )
        row = collect_cex_fee_snapshot(
            **common,
            client=client,
            profile_id=PROFILE_ID,
            valid_until=VALID_UNTIL,
        )
        self.assertEqual(client.last_call, ("okx", "AAVE-USDT"))
        self.assertEqual(row["value_status"], "authenticated")
        self.assertIn("fee_asset=USDT", row["basis"])

    def test_client_failure_and_logs_never_disclose_sensitive_material(self):
        class Client:
            api_key = SENTINELS[0]
            secret = SENTINELS[1]
            passphrase = SENTINELS[2]
            account_id = SENTINELS[3]
            authorization = SENTINELS[4]
            profile_path = SENTINELS[5]

            def fetch_authenticated_fee(self, *, venue, instrument):
                raise RuntimeError(" ".join(SENTINELS))

        messages = []
        row = collect_cex_fee_snapshot(
            **self.common(),
            client=Client(),
            profile_id=PROFILE_ID,
            observed_at=OBSERVED_AT,
            valid_until=VALID_UNTIL,
            discount_asset_funded=True,
            logger=messages.append,
        )
        combined = json.dumps(row, sort_keys=True) + "\n" + "\n".join(messages)

        self.assertEqual(row["value_status"], "failed")
        self.assertEqual(row["reason_code"], "cex_fee_authenticated_fetch_failed")
        for sentinel in SENTINELS:
            self.assertNotIn(sentinel, combined)

    def write_public_schedule(self, directory, **overrides):
        path = Path(directory) / "public-fees.csv"
        row = {
            "venue": "binance",
            "instrument_pattern": "*",
            "side": "both",
            "min_taker_fee_bps": "4.5",
            "max_taker_fee_bps": "10",
            "fee_asset": "received_asset",
            "basis": "official_spot_taker_fee_range",
            "checked_at": OBSERVED_AT,
            "valid_until": "2026-08-08T12:00:00Z",
            "source_url": "https://www.binance.com/en/fee/trading",
        }
        row.update(overrides)
        with path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(handle, fieldnames=PUBLIC_FEE_SCHEDULE_COLUMNS)
            writer.writeheader()
            writer.writerow(row)
        return path

    def test_public_schedule_is_opt_in_non_strict_reference_scenario_only(self):
        with tempfile.TemporaryDirectory() as directory:
            schedule = self.write_public_schedule(directory)
            row = collect_cex_fee_snapshot(
                **self.common(),
                allow_public_estimate=True,
                public_schedule_path=schedule,
            )

        self.assertEqual(row["value_status"], "bounded_estimate")
        self.assertIs(row["strict_eligible"], False)
        self.assertEqual(row["rate_bps"], "10")
        self.assertEqual(row["amount_usd"], "10")
        self.assertIn("[4.5,10] bps", row["basis"])
        self.assertIn(
            "maximum reviewed public reference rate projected for a non-strict "
            "research scenario",
            row["basis"],
        )
        self.assertIn(
            "not an authenticated account, regional, or pair-specific fee",
            row["basis"],
        )
        self.assertNotIn("conservative upper bound", row["basis"])
        self.assertIn("fee_asset=AAVE", row["basis"])
        serialized = json.dumps(row, sort_keys=True)
        for sentinel in SENTINELS:
            self.assertNotIn(sentinel, serialized)

    def test_public_collector_rejects_caller_supplied_parsed_schedule(self):
        class ForgedSnapshot:
            raw_bytes = b"not a fee schedule"
            file_device = 0
            file_inode = 0

            @staticmethod
            def rows():
                return [{
                    "venue": "binance",
                    "instrument_pattern": "AAVE/USDT",
                    "side": "both",
                    "min_taker_fee_bps": "0",
                    "max_taker_fee_bps": "999",
                    "fee_asset": "received_asset",
                    "basis": "forged caller rows",
                    "checked_at": OBSERVED_AT,
                    "valid_until": VALID_UNTIL,
                    "source_url": "http://attacker.invalid/fees",
                }]

        with self.assertRaises((TypeError, ValueError)):
            collect_cex_fee_snapshot(
                **self.common(),
                allow_public_estimate=True,
                public_schedule_snapshot=ForgedSnapshot(),
            )

    def test_private_snapshot_resolver_revalidates_authoritative_bytes(self):
        forged_row = {
            "venue": "binance",
            "instrument_pattern": "AAVE/USDT",
            "side": "both",
            "min_taker_fee_bps": "0",
            "max_taker_fee_bps": "999",
            "fee_asset": "received_asset",
            "basis": "forged caller rows",
            "checked_at": OBSERVED_AT,
            "valid_until": VALID_UNTIL,
            "source_url": "http://attacker.invalid/fees",
        }
        forged = fee_facts._PublicFeeScheduleSnapshot(
            raw_bytes=b"not a fee schedule",
            file_device=1,
            file_inode=1,
            normalized_rows=(tuple(forged_row.items()),),
        )

        with self.assertRaisesRegex(ValueError, "columns"):
            fee_facts._collect_cex_fee_snapshot_from_schedule_snapshot(
                **self.common(),
                public_schedule_snapshot=forged,
            )

    def test_public_schedule_never_silently_replaces_missing_auth(self):
        with tempfile.TemporaryDirectory() as directory:
            schedule = self.write_public_schedule(directory)
            row = collect_cex_fee_snapshot(
                **self.common(),
                public_schedule_path=schedule,
            )
        self.assertEqual(row["value_status"], "unavailable")

    def test_public_schedule_rejects_stale_reversed_and_non_https_bounds(self):
        with tempfile.TemporaryDirectory() as directory:
            cases = (
                ({"valid_until": "2026-08-01T12:00:30Z"}, "stale"),
                (
                    {
                        "min_taker_fee_bps": "11",
                        "max_taker_fee_bps": "10",
                    },
                    "reversed",
                ),
                ({"source_url": "http://example.test/fees"}, "HTTPS"),
                (
                    {
                        "source_url": (
                            "https://user:SECRET_SENTINEL@example.test/fees"
                        )
                    },
                    "URL",
                ),
                (
                    {"source_url": "https://example.test/fees?account=SECRET"},
                    "URL",
                ),
                (
                    {"source_url": "https://example.test/fees#SECRET"},
                    "URL",
                ),
                ({"basis": "SECRET_SENTINEL"}, "basis code"),
                ({"basis": "account=ACCOUNT_ID_SENTINEL"}, "basis code"),
                ({"basis": "/private/account/fee-profile.csv"}, "basis code"),
                ({"fee_asset": "DOGE"}, "received_asset"),
            )
            for index, (overrides, message) in enumerate(cases):
                with self.subTest(index=index):
                    schedule = self.write_public_schedule(directory, **overrides)
                    with self.assertRaisesRegex(ValueError, message):
                        collect_cex_fee_snapshot(
                            **self.common(),
                            allow_public_estimate=True,
                            public_schedule_path=schedule,
                        )

    def test_public_schedule_rejects_wrong_official_host_and_overlong_window(self):
        with tempfile.TemporaryDirectory() as directory:
            cases = (
                (
                    {
                        "source_url": (
                            "https://www.binance.com.attacker.test/fees"
                        )
                    },
                    "source host",
                ),
                (
                    {
                        "source_url": (
                            "https://www.bybit.com/en/help-center/article/"
                            "Trading-Fee-Structure"
                        )
                    },
                    "source host",
                ),
                (
                    {"valid_until": "2026-08-31T12:00:01Z"},
                    "30 days",
                ),
            )
            for overrides, message in cases:
                with self.subTest(overrides=overrides):
                    schedule = self.write_public_schedule(directory, **overrides)
                    with self.assertRaisesRegex(ValueError, message):
                        collect_cex_fee_snapshot(
                            **self.common(),
                            allow_public_estimate=True,
                            public_schedule_path=schedule,
                        )

            schedule = self.write_public_schedule(
                directory,
                valid_until="2026-08-31T12:00:00Z",
            )
            row = collect_cex_fee_snapshot(
                **self.common(),
                allow_public_estimate=True,
                public_schedule_path=schedule,
            )
            self.assertEqual(row["value_status"], "bounded_estimate")

    def test_public_schedule_accepts_each_approved_root_host(self):
        cases = (
            ("binance", "https://binance.com"),
            ("bybit", "https://bybit.com"),
            ("okx", "https://okx.com"),
        )
        with tempfile.TemporaryDirectory() as directory:
            for venue, source_url in cases:
                with self.subTest(venue=venue):
                    schedule = self.write_public_schedule(
                        directory,
                        venue=venue,
                        source_url=source_url,
                    )
                    common = self.common()
                    common.update(
                        venue=venue,
                        market_id="cex:{}:AAVE/USDT".format(venue),
                    )
                    row = collect_cex_fee_snapshot(
                        **common,
                        allow_public_estimate=True,
                        public_schedule_path=schedule,
                    )
                    self.assertEqual(row["value_status"], "bounded_estimate")

    def test_tracked_live_research_schedule_is_reviewed_and_bounded(self):
        schedule = (
            Path(__file__).resolve().parents[1]
            / "config"
            / "cex_public_fee_schedules.csv"
        )
        with schedule.open("r", encoding="utf-8", newline="") as handle:
            tracked = list(csv.DictReader(handle))

        expected = {
            "binance": {
                "minimum": "10",
                "maximum": "10",
                "source": "https://www.binance.com/en-IN/fee/trading",
            },
            "bybit": {
                "minimum": "10",
                "maximum": "20",
                "source": (
                    "https://www.bybit.com/en/help-center/article/"
                    "Trading-Fee-Structure"
                ),
            },
        }
        self.assertEqual([row["venue"] for row in tracked], ["binance", "bybit"])
        checked_clock = datetime.strptime(
            "2026-09-05T10:00:00Z", "%Y-%m-%dT%H:%M:%SZ"
        )
        for raw in tracked:
            with self.subTest(venue=raw["venue"], contract="tracked row"):
                venue_expected = expected[raw["venue"]]
                self.assertEqual(raw["instrument_pattern"], "UNI/USDT")
                self.assertNotIn("*", raw["instrument_pattern"])
                self.assertEqual(raw["side"], "both")
                self.assertEqual(raw["min_taker_fee_bps"], venue_expected["minimum"])
                self.assertEqual(raw["max_taker_fee_bps"], venue_expected["maximum"])
                self.assertEqual(raw["fee_asset"], "received_asset")
                self.assertEqual(raw["basis"], "official_spot_taker_fee_range")
                self.assertEqual(raw["source_url"], venue_expected["source"])
                self.assertIn(
                    urlparse(raw["source_url"]).hostname,
                    {"www.binance.com", "www.bybit.com"},
                )
                checked = datetime.strptime(
                    raw["checked_at"], "%Y-%m-%dT%H:%M:%SZ"
                )
                valid_until = datetime.strptime(
                    raw["valid_until"], "%Y-%m-%dT%H:%M:%SZ"
                )
                self.assertLessEqual(checked, checked_clock)
                self.assertLess(checked_clock, valid_until)
                self.assertGreater(valid_until, checked)
                self.assertLessEqual(
                    (valid_until - checked).total_seconds(),
                    7 * 24 * 60 * 60,
                )

        for venue, venue_expected in expected.items():
            for side, received_asset in (("buy", "UNI"), ("sell", "USDT")):
                common = self.common()
                common.update(
                    venue=venue,
                    market_id="cex:{}:UNI/USDT".format(venue),
                    instrument="UNI/USDT",
                    side=side,
                    leg=side,
                    now="2026-09-05T10:00:00Z",
                )
                row = collect_cex_fee_snapshot(
                    **common,
                    allow_public_estimate=True,
                    public_schedule_path=schedule,
                )

                with self.subTest(venue=venue, side=side, contract="projection"):
                    self.assertEqual(row["value_status"], "bounded_estimate")
                    self.assertIs(row["strict_eligible"], False)
                    self.assertEqual(row["rate_bps"], venue_expected["maximum"])
                    self.assertEqual(row["amount_usd"], venue_expected["maximum"])
                    self.assertEqual(row["source"], venue_expected["source"])
                    self.assertEqual(row["observed_at"], "2026-09-04T10:00:00Z")
                    self.assertEqual(row["valid_until"], "2026-09-11T10:00:00Z")
                    self.assertIn("fee_asset={}".format(received_asset), row["basis"])

        unmatched = self.common()
        unmatched.update(now="2026-09-05T10:00:00Z")
        row = collect_cex_fee_snapshot(
            **unmatched,
            allow_public_estimate=True,
            public_schedule_path=schedule,
        )
        self.assertEqual(row["value_status"], "unavailable")
        self.assertIs(row["strict_eligible"], False)
        self.assertEqual(row["reason_code"], "cex_fee_public_bound_unavailable")
        for field in (
            "rate_bps",
            "amount_usd",
            "observed_at",
            "valid_until",
            "source_record_sha256",
        ):
            self.assertIsNone(row[field])

    def test_generic_private_profile_projects_authenticated_component(self):
        common = self.common()
        common.update(
            venue="crypto_com",
            market_id="cex:crypto_com:AAVE/USDT",
        )
        with tempfile.TemporaryDirectory() as directory:
            helper = PrivateFeeProfileTests()
            path = helper.write_profile(directory, [helper.row()])
            row = collect_cex_fee_snapshot(
                **common,
                private_profile_path=path,
                profile_id=PROFILE_ID,
            )

        self.assertEqual(row["value_status"], "authenticated")
        self.assertEqual(row["rate_bps"], "7.5")
        self.assertEqual(row["amount_usd"], "7.5")
        self.assertIn("fee_asset=AAVE", row["basis"])
        serialized = json.dumps(row, sort_keys=True)
        self.assertNotIn(str(path), serialized)
        for sentinel in SENTINELS:
            self.assertNotIn(sentinel, serialized)

    def test_amount_calculation_is_independent_of_decimal_context(self):
        class Client:
            def fetch_authenticated_fee(self, *, venue, instrument):
                response = bybit_fixture()
                response["result"]["list"][0]["takerFeeRate"] = (
                    "0.001234567890123456789"
                )
                return response

        common = self.common()
        common.update(
            venue="bybit",
            market_id="cex:bybit:AAVE/USDT",
            requested_notional_usd=Decimal("12345.67890123456789"),
        )
        with localcontext() as context:
            context.prec = 4
            row = collect_cex_fee_snapshot(
                **common,
                client=Client(),
                profile_id=PROFILE_ID,
                valid_until=VALID_UNTIL,
                fee_asset="AAVE",
            )

        self.assertEqual(row["rate_bps"], "12.34567890123456789")
        self.assertEqual(
            row["amount_usd"],
            "15.24157875323883675019051998750190521",
        )


if __name__ == "__main__":
    unittest.main()
