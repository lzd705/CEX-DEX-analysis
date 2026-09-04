"""Build a disposable Current Opportunity site from one sealed synthetic KAT.

The fixture is deliberately separate from production market data.  Repository
SHA-256 pins seal the full asset; one SSH signature authenticates only its
submission-policy snapshot.  The asset is replayed through the normal DEX
research finalizer, published as the normal five-file route bundle, and
projected through the read-only demo API using a fixed fixture clock.
"""

from __future__ import annotations

import base64
import binascii
import copy
from datetime import datetime, timezone
import gzip
import hashlib
import json
from pathlib import Path
import re
import tempfile
from typing import Any, Dict, Mapping, Optional

from dashboard.opportunity_facts import build_opportunity_payload
from scripts.route_opportunity_pipeline import (
    finalize_eth_uniswap_v2_research_opportunities,
)
from scripts.route_publication import (
    load_latest_complete_route_bundle,
    publish_route_cohort_bundle,
    publish_shadow_result,
)
from scripts.route_shadow_audit import build_shadow_audit
from scripts.route_shadow_inputs import (
    typed_source_lineage_observed_members,
    write_run_universe,
)


DEMO_CONTRACT = "opportunity_current_demo_summary/v1"
DEMO_EVIDENCE_MODE = "offline_sha256_sealed_fixture_with_signed_policy"
DEMO_VERIFICATION_STATUS = "fixture_integrity_verified"
DEMO_TEMPORAL_SCOPE = "fixed_fixture_clock"
DEMO_EXECUTION_CLAIM = "synthetic_fixture_no_execution"
DEMO_SIMULATION_BASIS = "sha256_sealed_repository_known_answer_fixture"
DEMO_SIGNED_SCOPE = "submission_policy_snapshot_only"
DEMO_RESEARCH_MEV_BPS = "25"
DEMO_TOKEN_PAIR = "AAA/WETH"
DEMO_ASSET_PATH = (
    Path(__file__).resolve().parent
    / "fixtures"
    / "current_opportunity_demo_v1.json.gz.b64"
)
DEMO_ASSET_RAW_SHA256 = (
    "7e0af472e466edbd73290e1a66dc60fe6e49b0accbd092ef5ae8cc5a6904a7fc"
)
DEMO_ASSET_COMPRESSED_SHA256 = (
    "125ff415d228718940778e25ec92048f222b356b1b52d25c2c9526e6c3156248"
)
DEMO_ASSET_RAW_SIZE = 453_617
DEMO_ASSET_COMPRESSED_SIZE = 45_134
_MAX_ENCODED_ASSET_BYTES = 65_536
_SAFE_FILENAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]{0,127}\Z", re.ASCII)


def _canonical_json_bytes(value: Any) -> bytes:
    return json.dumps(
        value,
        allow_nan=False,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")


def _decode_member(value: Any, *, label: str, maximum: int) -> bytes:
    if not isinstance(value, str) or not value or len(value) > maximum * 2:
        raise RuntimeError("{} is invalid".format(label))
    try:
        decoded = base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError) as error:
        raise RuntimeError("{} is invalid".format(label)) from error
    if not 0 < len(decoded) <= maximum:
        raise RuntimeError("{} is invalid".format(label))
    return decoded


def _fixed_datetime(value: Any) -> datetime:
    if not isinstance(value, str) or not value.endswith("Z"):
        raise RuntimeError("demo fixture evaluation time is invalid")
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ").replace(
            tzinfo=timezone.utc
        )
    except ValueError as error:
        raise RuntimeError("demo fixture evaluation time is invalid") from error
    return parsed


def load_demo_fixture_bundle(
    path: Path = DEMO_ASSET_PATH,
) -> Dict[str, Any]:
    """Load one exact, bounded repository asset before any decompression."""

    asset_path = Path(path)
    try:
        encoded = asset_path.read_bytes()
    except OSError as error:
        raise RuntimeError("current Opportunity demo asset is unavailable") from error
    if (
        not encoded.endswith(b"\n")
        or len(encoded) > _MAX_ENCODED_ASSET_BYTES
        or any(byte not in b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/=\n"
               for byte in encoded)
    ):
        raise RuntimeError("current Opportunity demo asset encoding is invalid")
    try:
        compressed = base64.b64decode(encoded.strip(), validate=True)
    except (binascii.Error, ValueError) as error:
        raise RuntimeError(
            "current Opportunity demo asset encoding is invalid"
        ) from error
    if (
        len(compressed) != DEMO_ASSET_COMPRESSED_SIZE
        or hashlib.sha256(compressed).hexdigest()
        != DEMO_ASSET_COMPRESSED_SHA256
    ):
        raise RuntimeError("current Opportunity demo asset hash differs")
    try:
        raw = gzip.decompress(compressed)
    except (OSError, EOFError) as error:
        raise RuntimeError("current Opportunity demo asset is invalid") from error
    if (
        len(raw) != DEMO_ASSET_RAW_SIZE
        or hashlib.sha256(raw).hexdigest() != DEMO_ASSET_RAW_SHA256
    ):
        raise RuntimeError("current Opportunity demo asset content differs")
    try:
        value = json.loads(raw.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise RuntimeError("current Opportunity demo asset is invalid") from error
    if (
        not isinstance(value, dict)
        or set(value) != {
            "schema", "metadata", "cohort", "universe", "baseline",
            "raw_members", "typed_members", "cost_evidence",
        }
        or value.get("schema") != "current_opportunity_demo_fixture/v1"
        or raw != _canonical_json_bytes(value)
    ):
        raise RuntimeError("current Opportunity demo asset schema is invalid")
    metadata = value.get("metadata")
    if (
        not isinstance(metadata, dict)
        or metadata.get("token_pair") != DEMO_TOKEN_PAIR
        or metadata.get("research_mev_bps") != DEMO_RESEARCH_MEV_BPS
        or metadata.get("network_access") != "not_performed"
        or metadata.get("execution_claim") != DEMO_EXECUTION_CLAIM
    ):
        raise RuntimeError("current Opportunity demo metadata is invalid")
    return value


class CurrentOpportunityDemoFixture:
    """Own, validate, publish, project, and clean one disposable KAT tree."""

    def __init__(self) -> None:
        self._temporary = tempfile.TemporaryDirectory(
            prefix="current-opportunity-demo-"
        )
        self.data_dir = Path(self._temporary.name).resolve()
        self.routes_root = self.data_dir / "routes"
        self._closed = False
        try:
            fixture = load_demo_fixture_bundle()
            metadata = fixture["metadata"]
            self.reference_time = _fixed_datetime(metadata["evaluation_time"])
            self._install_sources(fixture)
            self.pointer = self._publish(fixture)
            self._assert_ready()
        except BaseException:
            self.close()
            raise

    def _install_sources(self, fixture: Mapping[str, Any]) -> None:
        cohort = fixture.get("cohort")
        raw_members = fixture.get("raw_members")
        typed_members = fixture.get("typed_members")
        if (
            not isinstance(cohort, Mapping)
            or not isinstance(raw_members, list)
            or not isinstance(typed_members, list)
        ):
            raise RuntimeError("current Opportunity demo source inventory is invalid")
        run_id = cohort.get("raw_evidence_run_id")
        if (
            not isinstance(run_id, str)
            or _SAFE_FILENAME.fullmatch(run_id) is None
        ):
            raise RuntimeError("current Opportunity demo run ID is invalid")
        expected_markets = {
            row.get("market_id")
            for row in cohort.get("legs", [])
            if isinstance(row, Mapping)
        }
        if None in expected_markets or not expected_markets:
            raise RuntimeError("current Opportunity demo market inventory is invalid")

        raw_by_market: Dict[str, bytes] = {}
        for member in raw_members:
            if not isinstance(member, Mapping) or set(member) != {
                "market_id", "payload_base64"
            }:
                raise RuntimeError("current Opportunity demo raw member is invalid")
            market_id = member.get("market_id")
            if not isinstance(market_id, str) or market_id in raw_by_market:
                raise RuntimeError("current Opportunity demo raw member is invalid")
            payload = _decode_member(
                member.get("payload_base64"),
                label="current Opportunity demo raw member",
                maximum=1_048_576,
            )
            try:
                parsed = json.loads(payload.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as error:
                raise RuntimeError(
                    "current Opportunity demo raw member is invalid"
                ) from error
            if (
                payload != _canonical_json_bytes(parsed)
                or not isinstance(parsed, Mapping)
                or parsed.get("market_id") != market_id
                or parsed.get("demo_fixture") is not True
                or parsed.get("network_access") != "not_performed"
            ):
                raise RuntimeError("current Opportunity demo raw member is invalid")
            raw_by_market[market_id] = payload
        if set(raw_by_market) != expected_markets:
            raise RuntimeError("current Opportunity demo raw inventory differs")

        raw_root = self.data_dir / "raw" / "route-cohort"
        accepted_root = raw_root / run_id / "accepted"
        accepted_root.mkdir(parents=True, mode=0o700)
        legs_by_market = {
            row["market_id"]: row for row in cohort["legs"]
        }
        for market_id, payload in raw_by_market.items():
            if hashlib.sha256(payload).hexdigest() != legs_by_market[
                market_id
            ].get("raw_response_sha256"):
                raise RuntimeError("current Opportunity demo raw hash differs")
            member_root = accepted_root / hashlib.sha256(
                market_id.encode("utf-8")
            ).hexdigest()
            member_root.mkdir(mode=0o700)
            (member_root / "response.json").write_bytes(payload)

        expected_typed = {}
        manifest_members = []
        for leg in cohort["legs"]:
            lineage = leg.get("typed_source_lineage")
            try:
                members = typed_source_lineage_observed_members(
                    lineage,
                    market_type=leg.get("market_type"),
                )
            except (TypeError, ValueError) as error:
                raise RuntimeError("current Opportunity demo typed lineage is invalid")
            for descriptor in members:
                if not isinstance(descriptor, Mapping):
                    raise RuntimeError("current Opportunity demo typed lineage is invalid")
                filename = descriptor.get("filename")
                if (
                    not isinstance(filename, str)
                    or _SAFE_FILENAME.fullmatch(filename) is None
                    or filename in expected_typed
                ):
                    raise RuntimeError("current Opportunity demo typed lineage is invalid")
                expected_typed[filename] = (leg["market_id"], descriptor)
                manifest_members.append({
                    "market_id": leg["market_id"],
                    **descriptor,
                })

        typed_root = raw_root / run_id / "typed"
        typed_root.mkdir(mode=0o700)
        seen_typed = set()
        for member in typed_members:
            if not isinstance(member, Mapping) or set(member) != {
                "market_id", "filename", "payload_base64"
            }:
                raise RuntimeError("current Opportunity demo typed member is invalid")
            filename = member.get("filename")
            market_id = member.get("market_id")
            expected = expected_typed.get(filename)
            if (
                expected is None
                or expected[0] != market_id
                or filename in seen_typed
            ):
                raise RuntimeError("current Opportunity demo typed inventory differs")
            payload = _decode_member(
                member.get("payload_base64"),
                label="current Opportunity demo typed member",
                maximum=8 * 1024 * 1024,
            )
            descriptor = expected[1]
            if (
                len(payload) != descriptor.get("size")
                or hashlib.sha256(payload).hexdigest()
                != descriptor.get("sha256")
            ):
                raise RuntimeError("current Opportunity demo typed hash differs")
            (typed_root / filename).write_bytes(payload)
            seen_typed.add(filename)
        if seen_typed != set(expected_typed):
            raise RuntimeError("current Opportunity demo typed inventory differs")

        manifest_members.sort(
            key=lambda row: (row["market_id"], row["role"])
        )
        (raw_root / run_id / "typed-manifest.json").write_bytes(
            _canonical_json_bytes({
                "schema": "route_typed_source_manifest/v1",
                "raw_evidence_run_id": run_id,
                "member_count": len(manifest_members),
                "members": manifest_members,
            })
        )

        self.raw_root = raw_root
        self.source_root = typed_root

    def _publish(self, fixture: Mapping[str, Any]) -> Dict[str, Any]:
        cohort = copy.deepcopy(fixture["cohort"])
        universe = copy.deepcopy(fixture["universe"])
        baseline = copy.deepcopy(fixture["baseline"])
        cost_evidence = copy.deepcopy(fixture["cost_evidence"])
        run_id = cohort["raw_evidence_run_id"]
        core_root = self.routes_root / "core"
        shadow_root = self.routes_root / "shadow"
        core_pointer = publish_route_cohort_bundle(
            cohort, core_root=core_root
        )
        write_run_universe(shadow_root, run_id, universe, baseline)
        cost_bytes = _canonical_json_bytes(cost_evidence)
        cost_path = shadow_root / "runs" / run_id / "route-cost-evidence.json"
        cost_path.write_bytes(cost_bytes)
        audit = build_shadow_audit(
            cohort,
            core_pointer=core_pointer,
            run={
                "run_id": run_id,
                "phase_state_sha256": hashlib.sha256(
                    b"route-shadow-phase/implicit-canary/v1\n"
                ).hexdigest(),
                "phase_transition_id": None,
                "route_universe_sha256": cost_evidence[
                    "route_universe_sha256"
                ],
                "baseline_manifest_sha256": hashlib.sha256(
                    _canonical_json_bytes(baseline)
                ).hexdigest(),
                "candidate_source_generation": cohort[
                    "candidate_source_generation"
                ],
                "route_cost_evidence_sha256": hashlib.sha256(
                    cost_bytes
                ).hexdigest(),
            },
            phase="canary",
            audit_finished_at="2026-08-01T12:00:04Z",
        )
        joint = publish_shadow_result(
            shadow_root, core_pointer=core_pointer, audit=audit
        )
        pointer = finalize_eth_uniswap_v2_research_opportunities(
            data_dir=self.data_dir,
            shadow_run_id=run_id,
            expected_joint_pointer_sha256=joint["pointer_sha256"],
            research_mev_bps=DEMO_RESEARCH_MEV_BPS,
        )
        loaded = load_latest_complete_route_bundle(
            self.routes_root, core_root=core_root
        )
        if loaded.get("pointer") != pointer:
            raise RuntimeError("current Opportunity demo publication differs")
        return pointer

    def _loaded(self) -> Dict[str, Any]:
        if self._closed:
            raise RuntimeError("current Opportunity demo fixture is closed")
        loaded = load_latest_complete_route_bundle(
            self.routes_root,
            core_root=self.routes_root / "core",
        )
        if loaded.get("pointer") != self.pointer:
            raise RuntimeError("current Opportunity demo pointer changed")
        return loaded

    def build_payload(
        self,
        *,
        token: Optional[str] = None,
        venue: Optional[str] = None,
        notional_usd: Optional[str] = None,
        opportunity_class: Optional[str] = None,
        route_type: Optional[str] = None,
        availability: Optional[str] = None,
        sort: Optional[str] = None,
        direction: Optional[str] = None,
    ) -> Dict[str, Any]:
        loaded = self._loaded()
        payload = build_opportunity_payload(
            loaded["opportunities"],
            manifest=loaded["manifest"],
            legs=loaded["legs"],
            cost_components=loaded["cost_components"],
            route_candidates=loaded["bundle"]["routes"],
            manifest_sha256=loaded["manifest_sha256"],
            core_context=loaded["bundle"]["core_context"],
            token=token,
            venue=venue,
            notional_usd=notional_usd,
            opportunity_class=opportunity_class,
            route_type=route_type,
            availability=availability,
            sort=sort,
            direction=direction,
            now=self.reference_time,
        )
        metadata = payload.get("metadata")
        if not isinstance(metadata, dict):
            raise RuntimeError("current Opportunity demo payload is invalid")
        metadata.update({
            "contract_version": DEMO_CONTRACT,
            "demo_fixture": True,
            "evidence_mode": DEMO_EVIDENCE_MODE,
            "verification_status": DEMO_VERIFICATION_STATUS,
            "validation_boundary": "production_dex_research_finalizer",
            "temporal_scope": DEMO_TEMPORAL_SCOPE,
            "clock_basis": "frozen_fixture_evaluation_time",
            "execution_claim": DEMO_EXECUTION_CLAIM,
            "execution_status": "not_run",
            "simulation_basis": DEMO_SIMULATION_BASIS,
            "signed_scope": DEMO_SIGNED_SCOPE,
            "live_rpc": False,
            "network_scope": "loopback_only",
            "token_pair": DEMO_TOKEN_PAIR,
            "venue_model": "two synthetic Uniswap V2 pools",
            "research_mev_bps": DEMO_RESEARCH_MEV_BPS,
            "fixture_evaluated_at": self.reference_time.isoformat().replace(
                "+00:00", "Z"
            ),
            "fixture_asset_sha256": DEMO_ASSET_RAW_SHA256,
        })
        return payload

    def _assert_ready(self) -> None:
        loaded = self._loaded()
        published = loaded.get("opportunities")
        if not isinstance(published, list) or len(published) != 5:
            raise RuntimeError("current Opportunity demo publication is invalid")
        published_by_id = {
            row.get("opportunity_id"): row
            for row in published
            if isinstance(row, Mapping)
        }
        if len(published_by_id) != 5:
            raise RuntimeError("current Opportunity demo publication is invalid")
        payload = self.build_payload(
            opportunity_class="estimate",
            route_type="dex_dex",
            availability="available",
            sort="net_edge_usd",
            direction="desc",
        )
        routes = payload.get("routes")
        if not isinstance(routes, list) or len(routes) != 5:
            raise RuntimeError("current Opportunity demo scenario grid is invalid")
        for route in routes:
            components = route.get("cost_components")
            published_route = published_by_id.get(route.get("opportunity_id"))
            mev = [
                row for row in components or []
                if row.get("component_type") == "mev_buffer"
            ]
            if (
                route.get("opportunity_class") != "research_estimate"
                or route.get("availability")
                != {"status": "available", "reason": None}
                or route.get("net_edge_usd") is None
                or not isinstance(components, list)
                or len(components) != 10
                or len(mev) != 1
                or mev[0].get("value_status") != "assumed"
                or str(mev[0].get("rate_bps")) != DEMO_RESEARCH_MEV_BPS
                or not isinstance(published_route, Mapping)
                or published_route.get("strict_eligible") is not False
                or published_route.get("strict_ready_for_publication") is not False
                or published_route.get("publication_attestation_sha256") is not None
            ):
                raise RuntimeError("current Opportunity demo result is invalid")

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._temporary.cleanup()

    def __enter__(self) -> "CurrentOpportunityDemoFixture":
        return self

    def __exit__(self, _type: Any, _value: Any, _traceback: Any) -> None:
        self.close()
