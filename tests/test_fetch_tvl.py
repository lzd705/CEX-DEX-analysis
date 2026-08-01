import csv
import json
import sqlite3
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from dashboard.snapshot_refresh import (
    evaluate_snapshot_refresh,
    read_snapshot_fact_state,
)
from scripts.fetch_tvl import (
    CURRENT_FILENAME,
    HISTORY_FILENAME,
    LATEST_FILENAME,
    MAX_POOLS_PER_REQUEST,
    TVL_COLUMNS,
    atomic_write_csv,
    chunks,
    collect_tvl,
    load_pools_from_csv,
    load_pools_from_database,
    merge_exact_publication,
    multi_pool_url,
    pool_key,
    publish_exact_snapshot,
    publish_snapshot,
    rows_from_payload,
    validate_snapshot,
)
from scripts.publication_gate import CoverageRegressionError


def pool(token="UNI", chain="eth", address="0xPool", dex="uniswap_v3"):
    return {
        "token_symbol": token,
        "chain": chain,
        "dex": dex,
        "pool_address": address,
        "pool_name": f"{token} / USDC",
    }


def source_item(chain="eth", address="0xPool", reserve="1234.56"):
    return {
        "id": f"{chain}_{address}",
        "type": "pool",
        "attributes": {
            "address": address,
            "name": "UNI / USDC 0.3%",
            "reserve_in_usd": reserve,
            "base_token_price_usd": "7.25",
            "quote_token_price_usd": "1.00",
            "volume_usd": {"h24": "456.78"},
            "pool_created_at": "2021-05-05T00:00:00Z",
        },
        "relationships": {
            "dex": {"data": {"id": "uniswap_v3"}},
            "base_token": {"data": {"id": "eth_0xuni"}},
            "quote_token": {"data": {"id": "eth_0xusdc"}},
        },
    }


class FetchTvlTest(unittest.TestCase):
    def test_batches_do_not_exceed_multi_pool_limit(self):
        batches = list(chunks(list(range(MAX_POOLS_PER_REQUEST + 1)), MAX_POOLS_PER_REQUEST))
        self.assertEqual([len(batch) for batch in batches], [30, 1])

    def test_pool_key_preserves_case_sensitive_non_evm_address(self):
        self.assertEqual(pool_key("ETH", "0xAbCd"), ("eth", "0xabcd"))
        self.assertEqual(pool_key("Solana", "AbCd"), ("solana", "AbCd"))

    def test_multi_pool_url_keeps_comma_separated_addresses(self):
        url = multi_pool_url("eth", ["0xone", "0xtwo"])
        self.assertEqual(
            url,
            "https://api.geckoterminal.com/api/v2/networks/eth/pools/multi/0xone,0xtwo",
        )

    def test_rows_from_payload_preserves_source_tvl_and_lineage(self):
        raw = json.dumps({"data": [source_item()]}).encode()
        rows = rows_from_payload(
            [pool()],
            json.loads(raw),
            snapshot_id="snapshot-1",
            request_started_at="2026-07-27T00:00:00+00:00",
            response_received_at="2026-07-27T00:00:01+00:00",
            source_endpoint="https://example.test/pools",
            raw_sha256="abc123",
        )
        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["status"], "observed")
        self.assertEqual(rows[0]["tvl_usd"], "1234.56")
        self.assertEqual(rows[0]["base_token_price_usd"], "7.25")
        self.assertEqual(rows[0]["source_dex"], "uniswap_v3")
        self.assertEqual(rows[0]["raw_response_sha256"], "abc123")
        self.assertEqual(rows[0]["tvl_method"], "geckoterminal_reserve_in_usd")

    def test_rows_from_payload_marks_missing_and_not_found_without_zero_fill(self):
        missing_item = source_item(address="0xMissing", reserve=None)
        rows = rows_from_payload(
            [pool(address="0xMissing"), pool(address="0xAbsent")],
            {"data": [missing_item]},
            snapshot_id="snapshot-1",
            request_started_at="2026-07-27T00:00:00+00:00",
            response_received_at="2026-07-27T00:00:01+00:00",
            source_endpoint="https://example.test/pools",
            raw_sha256="abc123",
        )
        by_address = {row["pool_address"]: row for row in rows}
        self.assertEqual(by_address["0xMissing"]["status"], "missing")
        self.assertEqual(by_address["0xMissing"]["tvl_usd"], "")
        self.assertEqual(by_address["0xAbsent"]["status"], "not_found")
        self.assertEqual(by_address["0xAbsent"]["tvl_usd"], "")

    def test_database_inventory_is_unique_by_token_and_pool(self):
        with tempfile.TemporaryDirectory() as directory_name:
            database = Path(directory_name) / "facts.sqlite3"
            connection = sqlite3.connect(database)
            try:
                connection.execute(
                    """
                    CREATE TABLE dex_pool_daily (
                        token_symbol TEXT, chain TEXT, dex TEXT,
                        pool_address TEXT, pool_name TEXT
                    )
                    """
                )
                connection.executemany(
                    "INSERT INTO dex_pool_daily VALUES (?, ?, ?, ?, ?)",
                    [
                        ("UNI", "eth", "uniswap_v3", "0xpool", "UNI / USDC 0.3%"),
                        ("UNI", "eth", "uniswap_v3", "0xpool", "UNI / USDC 0.05%"),
                        ("AAVE", "eth", "uniswap_v3", "0xaave", "AAVE / USDC"),
                    ],
                )
                connection.commit()
            finally:
                connection.close()

            rows = load_pools_from_database(database)

        self.assertEqual(len(rows), 2)
        self.assertEqual({row["token_symbol"] for row in rows}, {"UNI", "AAVE"})
        self.assertEqual(
            next(row for row in rows if row["token_symbol"] == "UNI")["pool_name"],
            "UNI / USDC 0.05%",
        )

    def test_csv_inventory_deduplicates_daily_rows(self):
        with tempfile.TemporaryDirectory() as directory_name:
            csv_path = Path(directory_name) / "dex.csv"
            with csv_path.open("w", newline="", encoding="utf-8") as handle:
                writer = csv.DictWriter(
                    handle,
                    fieldnames=[
                        "date",
                        "token_symbol",
                        "chain",
                        "dex",
                        "pool_address",
                        "pool_name",
                    ],
                )
                writer.writeheader()
                writer.writerow(
                    {
                        "date": "2026-01-01",
                        **pool(address="0xPool"),
                    }
                )
                writer.writerow(
                    {
                        "date": "2026-01-02",
                        **pool(address="0xPool"),
                    }
                )

            rows = load_pools_from_csv(csv_path)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["token_symbol"], "UNI")

    def test_collect_tvl_writes_raw_response_and_manifest(self):
        response = json.dumps({"data": [source_item()]}).encode()

        def fake_request(_url):
            return json.loads(response), response

        with tempfile.TemporaryDirectory() as directory_name:
            raw_root = Path(directory_name)
            snapshot_id, rows = collect_tvl(
                [pool()],
                raw_root=raw_root,
                request=fake_request,
                sleep_seconds=0,
            )
            snapshot_dir = raw_root / snapshot_id
            manifest = json.loads((snapshot_dir / "manifest.json").read_text())

        self.assertEqual(rows[0]["status"], "observed")
        self.assertEqual(manifest["pool_count"], 1)
        self.assertEqual(manifest["status_counts"]["observed"], 1)
        self.assertEqual(len(manifest["raw_files"]), 1)

    def test_validate_snapshot_requires_complete_inventory_and_observed_fact(self):
        observed = rows_from_payload(
            [pool()],
            {"data": [source_item()]},
            snapshot_id="snapshot-1",
            request_started_at="2026-07-27T00:00:00+00:00",
            response_received_at="2026-07-27T00:00:01+00:00",
            source_endpoint="https://example.test/pools",
            raw_sha256="abc123",
        )
        validate_snapshot([pool()], observed)
        with self.assertRaisesRegex(ValueError, "coverage"):
            validate_snapshot([pool(), pool(token="AAVE", address="0xaave")], observed)

    def test_validate_snapshot_rejects_missing_or_noncanonical_observation_time(self):
        observed = rows_from_payload(
            [pool()],
            {"data": [source_item()]},
            snapshot_id="snapshot-1",
            request_started_at="2026-07-27T00:00:00+00:00",
            response_received_at="2026-07-27T00:00:01+00:00",
            source_endpoint="https://example.test/pools",
            raw_sha256="abc123",
        )
        for invalid_time in (
            "",
            "2026-07-27T00:00:01",
            "2026-07-27T08:00:01+08:00",
            "not-a-time",
        ):
            with self.subTest(observed_at=invalid_time):
                malformed = [{**observed[0], "observed_at": invalid_time}]
                with self.assertRaisesRegex(ValueError, "observed_at"):
                    validate_snapshot([pool()], malformed)

    def test_validate_snapshot_binds_tvl_value_to_observation_status(self):
        observed = rows_from_payload(
            [pool()],
            {"data": [source_item()]},
            snapshot_id="snapshot-1",
            request_started_at="2026-07-27T00:00:00+00:00",
            response_received_at="2026-07-27T00:00:01+00:00",
            source_endpoint="https://example.test/pools",
            raw_sha256="abc123",
        )
        with self.assertRaisesRegex(ValueError, "observed TVL"):
            validate_snapshot([pool()], [{**observed[0], "tvl_usd": ""}])
        with self.assertRaisesRegex(ValueError, "non-observed TVL"):
            validate_snapshot(
                [pool()],
                [{**observed[0], "status": "missing", "tvl_usd": "123"}],
                allow_no_observed=True,
            )
        validate_snapshot([pool()], [{**observed[0], "tvl_usd": "0"}])

    def test_exact_candidate_accepts_only_terminal_missing_or_not_found(self):
        terminal = rows_from_payload(
            [pool(address="0xMissing")],
            {"data": [source_item(address="0xMissing", reserve=None)]},
            snapshot_id="candidate-1",
            request_started_at="2026-07-27T00:00:00+00:00",
            response_received_at="2026-07-27T00:00:01+00:00",
            source_endpoint="https://example.test/pools",
            raw_sha256="b" * 64,
        )
        with self.assertRaisesRegex(ValueError, "no observed"):
            validate_snapshot([pool(address="0xMissing")], terminal)
        validate_snapshot(
            [pool(address="0xMissing")],
            terminal,
            allow_terminal_only=True,
        )

        retryable = [
            {
                **terminal[0],
                "status": "failed",
                "error": "URLError: temporary source failure",
            }
        ]
        with self.assertRaisesRegex(ValueError, "terminal non-retryable"):
            validate_snapshot(
                [pool(address="0xMissing")],
                retryable,
                allow_terminal_only=True,
            )

    def test_publish_appends_history_and_replaces_latest(self):
        first = rows_from_payload(
            [pool()],
            {"data": [source_item(reserve="100")]},
            snapshot_id="snapshot-1",
            request_started_at="2026-07-27T00:00:00+00:00",
            response_received_at="2026-07-27T00:00:01+00:00",
            source_endpoint="https://example.test/pools",
            raw_sha256="hash-1",
        )
        second = rows_from_payload(
            [pool()],
            {"data": [source_item(reserve="200")]},
            snapshot_id="snapshot-2",
            request_started_at="2026-07-27T01:00:00+00:00",
            response_received_at="2026-07-27T01:00:01+00:00",
            source_endpoint="https://example.test/pools",
            raw_sha256="hash-2",
        )
        with tempfile.TemporaryDirectory() as output_name, tempfile.TemporaryDirectory() as publish_name:
            output = Path(output_name)
            published = Path(publish_name)
            publish_snapshot(first, output_dir=output, publish_dir=published)
            result = publish_snapshot(second, output_dir=output, publish_dir=published)
            with (published / HISTORY_FILENAME).open(newline="", encoding="utf-8") as handle:
                history = list(csv.DictReader(handle))
            with (published / LATEST_FILENAME).open(newline="", encoding="utf-8") as handle:
                latest = list(csv.DictReader(handle))
            with (output / CURRENT_FILENAME).open(newline="", encoding="utf-8") as handle:
                current = list(csv.DictReader(handle))

        self.assertEqual(result["history_row_count"], 2)
        self.assertEqual(len(history), 2)
        self.assertEqual(latest[0]["tvl_usd"], "200")
        self.assertEqual(current[0]["snapshot_id"], "snapshot-2")
        self.assertEqual(set(latest[0]), set(TVL_COLUMNS))

    def test_exact_pool_merge_preserves_other_pool_and_appends_only_target_history(self):
        pools = [pool(), pool(token="AAVE", address="0xAave")]
        baseline = rows_from_payload(
            pools,
            {
                "data": [
                    source_item(address="0xPool", reserve="100"),
                    source_item(address="0xAave", reserve="200"),
                ]
            },
            snapshot_id="baseline-1",
            request_started_at="2026-07-27T00:00:00+00:00",
            response_received_at="2026-07-27T00:00:01+00:00",
            source_endpoint="https://example.test/pools",
            raw_sha256="a" * 64,
        )
        candidate = rows_from_payload(
            [pools[1]],
            {"data": [source_item(address="0xAave", reserve="250")]},
            snapshot_id="candidate-2",
            request_started_at="2026-07-27T01:00:00+00:00",
            response_received_at="2026-07-27T01:00:01+00:00",
            source_endpoint="https://example.test/pools",
            raw_sha256="b" * 64,
        )
        target = "dex:eth:uniswap_v3:0xaave:AAVE"

        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            published = root / "local"
            publish_snapshot(
                baseline,
                output_dir=root / "processed",
                publish_dir=published,
            )
            merged = merge_exact_publication(
                candidate,
                target_market_id=target,
                publish_dir=published,
            )
            publish_exact_snapshot(
                merged,
                target_market_id=target,
                history_rows_to_append=candidate,
                output_dir=root / "processed",
                publish_dir=published,
            )
            with (published / LATEST_FILENAME).open(
                newline="",
                encoding="utf-8",
            ) as handle:
                latest = list(csv.DictReader(handle))
            with (published / HISTORY_FILENAME).open(
                newline="",
                encoding="utf-8",
            ) as handle:
                history = list(csv.DictReader(handle))

        by_token = {row["token_symbol"]: row for row in latest}
        self.assertEqual(by_token["AAVE"]["tvl_usd"], "250")
        self.assertEqual(by_token["UNI"]["tvl_usd"], "100")
        self.assertEqual(
            {row["snapshot_id"] for row in latest},
            {"candidate-2"},
        )
        self.assertEqual(len(history), 3)

    def test_exact_pool_merge_can_insert_one_cataloged_missing_tvl_row(self):
        baseline_pool = pool()
        missing_pool = pool(token="AAVE", address="0xAave")
        baseline = rows_from_payload(
            [baseline_pool],
            {"data": [source_item(address="0xPool", reserve="100")]},
            snapshot_id="baseline-1",
            request_started_at="2026-07-27T00:00:00+00:00",
            response_received_at="2026-07-27T00:00:01+00:00",
            source_endpoint="https://example.test/pools",
            raw_sha256="a" * 64,
        )
        candidate = rows_from_payload(
            [missing_pool],
            {"data": [source_item(address="0xAave", reserve="250")]},
            snapshot_id="candidate-2",
            request_started_at="2026-07-27T01:00:00+00:00",
            response_received_at="2026-07-27T01:00:01+00:00",
            source_endpoint="https://example.test/pools",
            raw_sha256="b" * 64,
        )

        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            published = root / "local"
            publish_snapshot(
                baseline,
                output_dir=root / "processed",
                publish_dir=published,
            )
            merged = merge_exact_publication(
                candidate,
                target_market_id="dex:eth:uniswap_v3:0xaave:AAVE",
                publish_dir=published,
            )
            publish_exact_snapshot(
                merged,
                target_market_id="dex:eth:uniswap_v3:0xaave:AAVE",
                history_rows_to_append=candidate,
                output_dir=root / "processed",
                publish_dir=published,
            )
            with (published / LATEST_FILENAME).open(
                newline="",
                encoding="utf-8",
            ) as handle:
                latest = list(csv.DictReader(handle))
            with (published / HISTORY_FILENAME).open(
                newline="",
                encoding="utf-8",
            ) as handle:
                history = list(csv.DictReader(handle))

        self.assertEqual(len(merged), 2)
        by_token = {row["token_symbol"]: row for row in merged}
        self.assertEqual(by_token["UNI"]["tvl_usd"], "100")
        self.assertEqual(by_token["AAVE"]["tvl_usd"], "250")
        self.assertEqual(
            {row["snapshot_id"] for row in merged},
            {"candidate-2"},
        )
        self.assertEqual(len(latest), 2)
        self.assertEqual(len(history), 2)

    def test_exact_pool_publication_can_insert_terminal_absence_and_satisfy_postcondition(self):
        baseline_pool = pool()
        target_pool = pool(token="AAVE", address="0xAave")
        target_market_id = "dex:eth:uniswap_v3:0xaave:AAVE"
        request = {
            "token_symbol": "AAVE",
            "market_id": target_market_id,
            "fact_type": "tvl",
        }
        baseline = rows_from_payload(
            [baseline_pool],
            {"data": [source_item(address="0xPool", reserve="100")]},
            snapshot_id="baseline-1",
            request_started_at="2026-07-27T00:00:00+00:00",
            response_received_at="2026-07-27T00:00:01+00:00",
            source_endpoint="https://example.test/pools",
            raw_sha256="a" * 64,
        )
        terminal_candidate = rows_from_payload(
            [target_pool],
            {"data": []},
            snapshot_id="candidate-2",
            request_started_at="2026-07-27T01:00:00+00:00",
            response_received_at="2026-07-27T01:00:01+00:00",
            source_endpoint="https://example.test/pools",
            raw_sha256="b" * 64,
        )

        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            published = root / "local"
            publish_snapshot(
                baseline,
                output_dir=root / "processed",
                publish_dir=published,
            )
            before = read_snapshot_fact_state(published, request)
            merged = merge_exact_publication(
                terminal_candidate,
                target_market_id=target_market_id,
                publish_dir=published,
            )
            result = publish_exact_snapshot(
                merged,
                target_market_id=target_market_id,
                history_rows_to_append=terminal_candidate,
                output_dir=root / "processed",
                publish_dir=published,
            )
            after = read_snapshot_fact_state(published, request)
            with (published / LATEST_FILENAME).open(
                newline="",
                encoding="utf-8",
            ) as handle:
                latest = list(csv.DictReader(handle))

        postcondition = evaluate_snapshot_refresh(before, after)
        self.assertEqual(terminal_candidate[0]["status"], "not_found")
        self.assertTrue(postcondition.succeeded)
        self.assertEqual(
            postcondition.resolution,
            "confirmed_absence",
        )
        self.assertEqual(result["publication_gate"]["mode"], "exact_target_recovery/v1")
        by_token = {row["token_symbol"]: row for row in latest}
        self.assertEqual(by_token["AAVE"]["status"], "not_found")
        self.assertEqual(by_token["AAVE"]["tvl_usd"], "")
        self.assertEqual(by_token["UNI"]["tvl_usd"], "100")

    def test_exact_pool_publication_recovers_one_target_from_low_coverage_baseline(self):
        pools = [pool(), pool(token="AAVE", address="0xAave")]
        target_market_id = "dex:eth:uniswap_v3:0xaave:AAVE"
        baseline = rows_from_payload(
            pools,
            {
                "data": [
                    source_item(address="0xPool", reserve="100"),
                    source_item(address="0xAave", reserve="200"),
                ]
            },
            snapshot_id="baseline-1",
            request_started_at="2026-07-27T00:00:00+00:00",
            response_received_at="2026-07-27T00:00:01+00:00",
            source_endpoint="https://example.test/pools",
            raw_sha256="a" * 64,
        )
        baseline[1].update(status="failed", tvl_usd="", error="source outage")
        candidate = rows_from_payload(
            [pools[1]],
            {"data": [source_item(address="0xAave", reserve="250")]},
            snapshot_id="candidate-2",
            request_started_at="2026-07-27T01:00:00+00:00",
            response_received_at="2026-07-27T01:00:01+00:00",
            source_endpoint="https://example.test/pools",
            raw_sha256="b" * 64,
        )

        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            published = root / "local"
            published.mkdir()
            for filename in (LATEST_FILENAME, CURRENT_FILENAME):
                atomic_write_csv(published / filename, baseline)
            merged = merge_exact_publication(
                candidate,
                target_market_id=target_market_id,
                publish_dir=published,
            )
            result = publish_exact_snapshot(
                merged,
                target_market_id=target_market_id,
                history_rows_to_append=candidate,
                output_dir=root / "processed",
                publish_dir=published,
            )

        self.assertTrue(result["publication_gate"]["passed"])
        self.assertEqual(result["publication_gate"]["candidate"]["usable_bps"], 10000)
        self.assertEqual(
            result["publication_gate"]["thresholds"]["minimum_candidate_usable_bps"],
            0,
        )

    def test_exact_pool_publication_resolves_terminal_on_all_failed_baseline(self):
        target_pool = pool(token="AAVE", address="0xAave")
        target_market_id = "dex:eth:uniswap_v3:0xaave:AAVE"
        baseline = rows_from_payload(
            [target_pool],
            {"data": [source_item(address="0xAave", reserve="200")]},
            snapshot_id="baseline-1",
            request_started_at="2026-07-27T00:00:00+00:00",
            response_received_at="2026-07-27T00:00:01+00:00",
            source_endpoint="https://example.test/pools",
            raw_sha256="a" * 64,
        )
        baseline[0].update(status="failed", tvl_usd="", error="source outage")
        terminal_candidate = rows_from_payload(
            [target_pool],
            {"data": []},
            snapshot_id="candidate-2",
            request_started_at="2026-07-27T01:00:00+00:00",
            response_received_at="2026-07-27T01:00:01+00:00",
            source_endpoint="https://example.test/pools",
            raw_sha256="b" * 64,
        )

        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            published = root / "local"
            published.mkdir()
            for filename in (LATEST_FILENAME, CURRENT_FILENAME):
                atomic_write_csv(published / filename, baseline)
            merged = merge_exact_publication(
                terminal_candidate,
                target_market_id=target_market_id,
                publish_dir=published,
            )
            result = publish_exact_snapshot(
                merged,
                target_market_id=target_market_id,
                history_rows_to_append=terminal_candidate,
                output_dir=root / "processed",
                publish_dir=published,
            )

        self.assertTrue(result["publication_gate"]["passed"])
        self.assertEqual(merged[0]["status"], "not_found")
        self.assertEqual(
            result["publication_gate"]["exact_target"]["resolution"],
            "confirmed_terminal_absence",
        )

    def test_exact_pool_publication_rolls_back_every_public_replace(self):
        pools = [pool(), pool(token="AAVE", address="0xAave")]
        baseline = rows_from_payload(
            pools,
            {
                "data": [
                    source_item(address="0xPool", reserve="100"),
                    source_item(address="0xAave", reserve="200"),
                ]
            },
            snapshot_id="baseline-1",
            request_started_at="2026-07-27T00:00:00+00:00",
            response_received_at="2026-07-27T00:00:01+00:00",
            source_endpoint="https://example.test/pools",
            raw_sha256="a" * 64,
        )
        candidate = rows_from_payload(
            [pools[1]],
            {"data": [source_item(address="0xAave", reserve="250")]},
            snapshot_id="candidate-2",
            request_started_at="2026-07-27T01:00:00+00:00",
            response_received_at="2026-07-27T01:00:01+00:00",
            source_endpoint="https://example.test/pools",
            raw_sha256="b" * 64,
        )
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            published = root / "local"
            output = root / "processed"
            publish_snapshot(
                baseline,
                output_dir=output,
                publish_dir=published,
            )
            merged = merge_exact_publication(
                candidate,
                target_market_id="dex:eth:uniswap_v3:0xaave:AAVE",
                publish_dir=published,
            )
            protected = [
                published / HISTORY_FILENAME,
                published / LATEST_FILENAME,
                published / CURRENT_FILENAME,
            ]
            originals = {path: path.read_bytes() for path in protected}

            from scripts import atomic_publication

            real_replace = atomic_publication.os.replace
            for fail_at in range(1, len(protected) + 1):
                calls = {"count": 0}

                def fail_once(source, destination):
                    calls["count"] += 1
                    if calls["count"] == fail_at:
                        raise OSError("injected publication failure")
                    return real_replace(source, destination)

                with self.subTest(fail_at=fail_at), patch(
                    "scripts.atomic_publication.os.replace",
                    side_effect=fail_once,
                ):
                    with self.assertRaises(OSError):
                        publish_exact_snapshot(
                            merged,
                            target_market_id="dex:eth:uniswap_v3:0xaave:AAVE",
                            history_rows_to_append=candidate,
                            output_dir=output,
                            publish_dir=published,
                        )
                self.assertEqual(
                    {path: path.read_bytes() for path in protected},
                    originals,
                )

    def test_terminal_absence_insert_rolls_back_every_public_replace(self):
        baseline_pool = pool()
        target_pool = pool(token="AAVE", address="0xAave")
        target_market_id = "dex:eth:uniswap_v3:0xaave:AAVE"
        baseline = rows_from_payload(
            [baseline_pool],
            {"data": [source_item(address="0xPool", reserve="100")]},
            snapshot_id="baseline-1",
            request_started_at="2026-07-27T00:00:00+00:00",
            response_received_at="2026-07-27T00:00:01+00:00",
            source_endpoint="https://example.test/pools",
            raw_sha256="a" * 64,
        )
        candidate = rows_from_payload(
            [target_pool],
            {"data": []},
            snapshot_id="candidate-2",
            request_started_at="2026-07-27T01:00:00+00:00",
            response_received_at="2026-07-27T01:00:01+00:00",
            source_endpoint="https://example.test/pools",
            raw_sha256="b" * 64,
        )
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            published = root / "local"
            output = root / "processed"
            publish_snapshot(
                baseline,
                output_dir=output,
                publish_dir=published,
            )
            merged = merge_exact_publication(
                candidate,
                target_market_id=target_market_id,
                publish_dir=published,
            )
            protected = [
                published / HISTORY_FILENAME,
                published / LATEST_FILENAME,
                published / CURRENT_FILENAME,
            ]
            originals = {path: path.read_bytes() for path in protected}

            from scripts import atomic_publication

            real_replace = atomic_publication.os.replace
            for fail_at in range(1, len(protected) + 1):
                calls = {"count": 0}

                def fail_once(source, destination):
                    calls["count"] += 1
                    if calls["count"] == fail_at:
                        raise OSError("injected publication failure")
                    return real_replace(source, destination)

                with self.subTest(fail_at=fail_at), patch(
                    "scripts.atomic_publication.os.replace",
                    side_effect=fail_once,
                ):
                    with self.assertRaises(OSError):
                        publish_exact_snapshot(
                            merged,
                            target_market_id=target_market_id,
                            history_rows_to_append=candidate,
                            output_dir=output,
                            publish_dir=published,
                        )
                self.assertEqual(
                    {path: path.read_bytes() for path in protected},
                    originals,
                )

    def test_coverage_regression_preserves_every_published_tvl_file(self):
        pools = [
            pool(token=f"T{index}", address=f"0xPool{index}")
            for index in range(5)
        ]
        baseline = rows_from_payload(
            pools,
            {
                "data": [
                    source_item(address=item["pool_address"], reserve="100")
                    for item in pools
                ]
            },
            snapshot_id="healthy",
            request_started_at="2026-07-27T00:00:00+00:00",
            response_received_at="2026-07-27T00:00:01+00:00",
            source_endpoint="https://example.test/pools",
            raw_sha256="healthy-hash",
        )
        degraded = [
            {
                **row,
                "snapshot_id": "degraded",
                "observed_at": "2026-07-27T01:00:01+00:00",
                "status": "failed" if index >= 3 else "observed",
                "tvl_usd": "" if index >= 3 else row["tvl_usd"],
                "error": "source outage" if index >= 3 else "",
            }
            for index, row in enumerate(baseline)
        ]
        with tempfile.TemporaryDirectory() as directory_name:
            root = Path(directory_name)
            output = root / "processed"
            published = root / "local"
            publish_snapshot(
                baseline,
                output_dir=output,
                publish_dir=published,
            )
            protected_paths = [
                published / CURRENT_FILENAME,
                published / LATEST_FILENAME,
                published / HISTORY_FILENAME,
            ]
            before = {path: path.read_bytes() for path in protected_paths}

            with self.assertRaises(CoverageRegressionError):
                publish_snapshot(
                    degraded,
                    output_dir=output,
                    publish_dir=published,
                )

            self.assertEqual(
                {path: path.read_bytes() for path in protected_paths},
                before,
            )
            with (output / CURRENT_FILENAME).open(
                newline="",
                encoding="utf-8",
            ) as handle:
                processed = list(csv.DictReader(handle))
            self.assertEqual(
                {row["snapshot_id"] for row in processed},
                {"degraded"},
            )


if __name__ == "__main__":
    unittest.main()
