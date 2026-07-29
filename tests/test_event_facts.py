import csv
import json
import tempfile
import unittest
from pathlib import Path

from dashboard.event_facts import (
    EventBundleError,
    build_event_payload,
    load_latest_event_rows,
    resolve_event_bundle,
)
from scripts.event_facts import (
    CURATED_COLUMNS,
    DEFAULT_INPUT,
    DEFAULT_RECORD_ROOT,
    DEFAULT_TOKEN_CONFIG,
    EventFactValidationError,
    build_event_bundle,
    effective_date_bounds,
    latest_event_rows,
    load_allowed_tokens,
    normalize_event_rows,
    normalize_precise_time,
    read_curated_rows,
)


def write_csv(path, rows):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=CURATED_COLUMNS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


class EventFactsTest(unittest.TestCase):
    def setUp(self):
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.root = Path(self.temporary_directory.name)
        self.record_root = self.root / "records"
        self.record_root.mkdir()
        self.source_url = "https://example.test/official-event"
        self.checked_at = "2026-07-29T08:30:00Z"
        self.record_path = self.record_root / "official.json"
        self.record_path.write_text(
            json.dumps(
                {
                    "record_schema": "source_check/v1",
                    "source_url": self.source_url,
                    "checked_at_utc": self.checked_at,
                    "facts": {"schedule": {"statement": "A verified test fact"}},
                }
            ),
            encoding="utf-8",
        )

    def tearDown(self):
        self.temporary_directory.cleanup()

    def candidate(self):
        row = {column: "" for column in CURATED_COLUMNS}
        row.update(
            {
                "event_id": "arb-unlock-2026-08-15",
                "revision": "1",
                "token_symbol": "ARB",
                "event_type": "unlock",
                "event_subtype": "scheduled_release",
                "event_name": "Team allocation release",
                "lifecycle": "scheduled",
                "effective_at": "2026-07-29T16:30+08:00",
                "effective_at_precision": "minute",
                "amount_token": "1000.000",
                "percent_of_supply": "1.2500",
                "size_relation": "up_to",
                "chain": "arbitrum",
                "source_kind": "official_project",
                "evidence_status": "primary_confirmed",
                "source_url": self.source_url,
                "source_checked_at_utc": self.checked_at,
                "source_record_file": "official.json",
                "record_locator": "facts.schedule",
                "recorded_at_utc": self.checked_at,
                "revision_reason": "initial",
            }
        )
        return row

    def normalize(self, rows):
        return normalize_event_rows(
            rows,
            allowed_tokens={"ARB", "MORPHO"},
            record_root=self.record_root,
        )

    def test_normalizes_time_decimals_and_source_record_hash(self):
        rows = self.normalize([self.candidate()])

        self.assertEqual(rows[0]["effective_at"], "2026-07-29T08:30Z")
        self.assertEqual(rows[0]["amount_token"], "1000")
        self.assertEqual(rows[0]["percent_of_supply"], "1.25")
        self.assertEqual(len(rows[0]["record_sha256"]), 64)
        self.assertEqual(
            effective_date_bounds(
                rows[0]["effective_at"],
                rows[0]["effective_at_precision"],
            ),
            ("2026-07-29", "2026-07-29"),
        )

    def test_precision_never_manufactures_midnight(self):
        self.assertEqual(
            normalize_precise_time(
                "2026-07",
                "month",
                field="effective_at",
                required=True,
            ),
            ("2026-07", "month"),
        )
        self.assertEqual(
            effective_date_bounds("2026-02", "month"),
            ("2026-02-01", "2026-02-28"),
        )
        with self.assertRaisesRegex(
            EventFactValidationError,
            "does not match day precision",
        ):
            normalize_precise_time(
                "2026-07-29T00:00:00Z",
                "day",
                field="effective_at",
                required=True,
            )

    def test_rejects_semantically_invalid_or_unsupported_rows(self):
        changes = [
            ("token_symbol", "UNKNOWN", "not present"),
            ("source_url", "http://example.test/event", "HTTPS"),
            ("source_kind", "official_exchange", "project, governance, or onchain"),
            ("amount_usd", "100", "amount_usd_basis"),
            ("percent_of_supply", "100.1", "cannot exceed"),
            ("size_relation", "", "size_relation is required"),
            ("effective_at", "2026-07", "does not match minute precision"),
            ("source_record_file", "../escape.json", "inside the source-record root"),
            ("record_locator", "facts.missing", "does not exist"),
        ]
        for field, value, message in changes:
            with self.subTest(field=field):
                row = self.candidate()
                row[field] = value
                with self.assertRaisesRegex(EventFactValidationError, message):
                    self.normalize([row])

    def test_requires_versioned_fact_source_record(self):
        record = json.loads(self.record_path.read_text(encoding="utf-8"))
        record.pop("record_schema")
        self.record_path.write_text(json.dumps(record), encoding="utf-8")
        with self.assertRaisesRegex(
            EventFactValidationError,
            "record_schema=source_check/v1",
        ):
            self.normalize([self.candidate()])

        record["record_schema"] = "source_check/v1"
        record["anything"] = {"statement": "Not below the facts namespace"}
        self.record_path.write_text(json.dumps(record), encoding="utf-8")
        row = self.candidate()
        row["record_locator"] = "anything"
        with self.assertRaisesRegex(EventFactValidationError, "below facts"):
            self.normalize([row])

    def test_occurred_event_cannot_be_recorded_before_it_happens(self):
        row = self.candidate()
        row.update(
            {
                "lifecycle": "occurred",
                "effective_at": "2099-01-01",
                "effective_at_precision": "day",
            }
        )
        with self.assertRaisesRegex(
            EventFactValidationError,
            "effective_at cannot follow recorded_at_utc",
        ):
            self.normalize([row])

    def test_cex_listing_requires_official_market_identity_and_no_size(self):
        row = self.candidate()
        row.update(
            {
                "event_id": "morpho-okx-listing",
                "token_symbol": "MORPHO",
                "event_type": "cex_listing",
                "event_subtype": "spot_trading_start",
                "event_name": "MORPHO spot trading start",
                "source_kind": "official_exchange",
                "evidence_status": "primary_confirmed",
                "amount_token": "",
                "percent_of_supply": "",
                "size_relation": "",
                "chain": "",
                "venue": "okx",
                "market_symbol": "MORPHO/USDT",
                "market_id": "cex:okx:MORPHO/USDT",
            }
        )
        rows = self.normalize([row])
        self.assertEqual(rows[0]["market_id"], "cex:okx:MORPHO/USDT")

        row["market_id"] = "cex:okx:MORPHO"
        with self.assertRaisesRegex(EventFactValidationError, "must equal"):
            self.normalize([row])

    def test_revision_history_is_contiguous_ordered_and_material(self):
        first = self.candidate()
        second = dict(first)
        second.update(
            {
                "revision": "2",
                "lifecycle": "postponed",
                "recorded_at_utc": "2026-07-29T09:30:00Z",
                "revision_reason": "Official source postponed the date",
            }
        )
        rows = self.normalize([second, first])
        self.assertEqual(latest_event_rows(rows)[0]["lifecycle"], "postponed")

        non_contiguous = dict(second)
        non_contiguous["revision"] = "3"
        with self.assertRaisesRegex(EventFactValidationError, "contiguous"):
            self.normalize([first, non_contiguous])

        unchanged = dict(first)
        unchanged.update(
            {
                "revision": "2",
                "recorded_at_utc": "2026-07-29T09:30:00Z",
                "revision_reason": "No real change",
            }
        )
        with self.assertRaisesRegex(EventFactValidationError, "no material change"):
            self.normalize([first, unchanged])

    def test_build_bundle_and_api_preserve_nulls_and_lineage(self):
        input_path = self.root / "event_facts.csv"
        output_root = self.root / "published"
        write_csv(input_path, [self.candidate()])

        manifest = build_event_bundle(
            input_path,
            record_root=self.record_root,
            output_root=output_root,
            token_config=DEFAULT_TOKEN_CONFIG,
        )
        rows, loaded_manifest = load_latest_event_rows(output_root)
        payload = build_event_payload(
            rows,
            manifest=loaded_manifest,
            token="arb",
            start="2026-07-29",
            end="2026-07-29",
        )

        self.assertEqual(manifest["event_count"], 1)
        self.assertEqual(payload["event_count"], 1)
        event = payload["events"][0]
        self.assertIsNone(event["size"]["amount_usd"])
        self.assertIsNone(event["market"]["market_id"])
        self.assertEqual(event["revision_lineage"]["reason"], "initial")
        serialized = json.dumps(payload)
        self.assertNotIn("future_return", serialized)
        self.assertNotIn('"impact"', serialized)

    def test_event_payload_uses_interval_overlap_and_keeps_cancelled(self):
        row = self.candidate()
        row.update(
            {
                "lifecycle": "cancelled",
                "effective_at": "2026-02",
                "effective_at_precision": "month",
            }
        )
        normalized = self.normalize([row])
        manifest = {"bundle_id": "abc", "built_at_utc": self.checked_at}
        payload = build_event_payload(
            normalized,
            manifest=manifest,
            start="2026-02-28",
            end="2026-02-28",
        )
        self.assertEqual(payload["event_count"], 1)
        self.assertEqual(payload["events"][0]["lifecycle"], "cancelled")
        self.assertEqual(
            payload["events"][0]["time"]["effective_date_start"],
            "2026-02-01",
        )

    def test_bundle_checksum_fails_closed_after_tampering(self):
        input_path = self.root / "event_facts.csv"
        output_root = self.root / "published"
        write_csv(input_path, [self.candidate()])
        manifest = build_event_bundle(
            input_path,
            record_root=self.record_root,
            output_root=output_root,
            token_config=DEFAULT_TOKEN_CONFIG,
        )
        bundle = resolve_event_bundle(output_root)
        database_path = bundle["database_path"]
        with database_path.open("ab") as handle:
            handle.write(b"tamper")

        with self.assertRaisesRegex(EventBundleError, "checksum"):
            resolve_event_bundle(output_root)
        with self.assertRaisesRegex(
            EventFactValidationError,
            "checksum validation",
        ):
            build_event_bundle(
                input_path,
                record_root=self.record_root,
                output_root=output_root,
                token_config=DEFAULT_TOKEN_CONFIG,
            )
        self.assertEqual(manifest["event_count"], 1)

    def test_published_revision_history_is_append_only(self):
        input_path = self.root / "event_facts.csv"
        output_root = self.root / "published"
        first = self.candidate()
        write_csv(input_path, [first])
        build_event_bundle(
            input_path,
            record_root=self.record_root,
            output_root=output_root,
            token_config=DEFAULT_TOKEN_CONFIG,
        )

        rewritten = dict(first)
        rewritten["event_name"] = "Rewritten published fact"
        write_csv(input_path, [rewritten])
        with self.assertRaisesRegex(
            EventFactValidationError,
            "published revision is immutable",
        ):
            build_event_bundle(
                input_path,
                record_root=self.record_root,
                output_root=output_root,
                token_config=DEFAULT_TOKEN_CONFIG,
            )

        second = dict(first)
        second.update(
            {
                "revision": "2",
                "lifecycle": "postponed",
                "recorded_at_utc": "2026-07-29T09:30:00Z",
                "revision_reason": "Official source postponed the date",
            }
        )
        write_csv(input_path, [first, second])
        manifest = build_event_bundle(
            input_path,
            record_root=self.record_root,
            output_root=output_root,
            token_config=DEFAULT_TOKEN_CONFIG,
        )
        self.assertEqual(manifest["revision_count"], 2)
        self.assertEqual(manifest["event_count"], 1)

        other = dict(first)
        other["event_id"] = "arb-unlock-another-event"
        write_csv(input_path, [other])
        with self.assertRaisesRegex(
            EventFactValidationError,
            "published revision cannot be deleted",
        ):
            build_event_bundle(
                input_path,
                record_root=self.record_root,
                output_root=output_root,
                token_config=DEFAULT_TOKEN_CONFIG,
            )

    def test_committed_official_facts_build_to_expected_latest_counts(self):
        with tempfile.TemporaryDirectory() as output_name:
            manifest = build_event_bundle(
                DEFAULT_INPUT,
                record_root=DEFAULT_RECORD_ROOT,
                output_root=Path(output_name),
                token_config=DEFAULT_TOKEN_CONFIG,
            )
            rows, _ = load_latest_event_rows(Path(output_name))

        self.assertEqual(manifest["event_count"], 17)
        self.assertEqual(manifest["revision_count"], 17)
        self.assertEqual(manifest["event_type_counts"], {
            "airdrop": 1,
            "cex_listing": 1,
            "unlock": 15,
        })
        self.assertEqual(manifest["lifecycle_counts"], {
            "occurred": 9,
            "scheduled": 8,
        })
        self.assertEqual(
            manifest["evidence_status_counts"],
            {"primary_confirmed": 17},
        )
        self.assertEqual(
            {
                row["source_url"]
                for row in rows
            },
            {
                "https://docs.starknet.io/learn/protocol/strk",
                "https://blog.eigenfoundation.org/claims-s1-p1/",
                "https://www.okx.com/help/okx-to-list-morpho-morpho-for-spot-trading",
            },
        )

    def test_header_template_matches_builder_schema(self):
        template = (
            Path(__file__).resolve().parents[1]
            / "data/templates/event_facts_curated.csv"
        )
        with template.open(newline="", encoding="utf-8") as handle:
            template_columns = list(csv.DictReader(handle).fieldnames)
        self.assertEqual(template_columns, CURATED_COLUMNS)
        self.assertEqual(len(read_curated_rows(DEFAULT_INPUT)), 17)
        self.assertIn("STRK", load_allowed_tokens(DEFAULT_TOKEN_CONFIG))


if __name__ == "__main__":
    unittest.main()
