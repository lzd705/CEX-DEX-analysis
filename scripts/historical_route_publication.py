"""Isolated publication boundary for the historical replay private core.

The historical entry points deliberately have no live-root defaults and accept
neither caller-built core projections nor caller-selected raw readers.
"""

from datetime import datetime, timezone
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Dict, Mapping, NoReturn, Tuple
import fcntl
import hashlib
import json
import os
import re
import stat
import weakref
import zlib

from scripts.historical_foundry_contracts import (
    HistoricalFoundryConfigSet,
    load_historical_foundry_config_set,
)
from scripts.historical_foundry_replay import (
    ValidatedHistoricalScenarioInputs,
    _issue_validated_historical_scenario_inputs_from_publication,
    build_historical_scenario_projection,
    build_historical_core_projection,
    build_historical_research_universe,
    validate_selected_historical_run,
)
from scripts.route_cohort import canonical_route_id
from scripts.route_cost_topology import (
    HISTORICAL_ATOMIC_COMPONENT_MATRIX,
    _validate_historical_atomic_cost_component_matrix,
)
import scripts.historical_foundry_storage as _historical_storage
import scripts.route_publication as _route_publication


class HistoricalRoutePublicationError(ValueError):
    """Raised when historical private-core authority or bytes are invalid."""


_STAGE_ISSUER = object()
_CONTEXT_ISSUER = object()
_STAGE_REGISTRY = {}
_CONTEXT_REGISTRY = {}
_SCENARIO_CONTEXT_ISSUER = object()
_SCENARIO_CONTEXT_REGISTRY = {}

_CONTEXT_SCHEMA = "historical_replay_build_context/v1"
_POINTER_SCHEMA = "route_historical_replay_core_pointer/v1"
_BUNDLE_STAGE = "route_historical_replay_core/v1"
_MANIFEST_SCHEMA = "route_historical_replay_core_manifest/v1"
_TEMPORAL_SCOPE = "historical_replay"
_EXECUTION_CLAIM = "historical_counterfactual_state_override_next_block"
_CORE_FILES = frozenset((
    "manifest.json", "route_candidates.csv", "route_cohort.sqlite3",
    "route_legs.csv", "route_timing.csv",
))
_NOTIONALS = [1000, 5000, 10000, 50000, 100000]
_VENUES = ("uniswap_v2", "sushiswap_v2")
_MAX_MEMBER_BYTES = 8_388_608
_MAX_DECODED_MEMBER_BYTES = 16_777_216

_COST_PROOF_FIELDS = frozenset((
    "schema", "scenario_key", "policy_sha256", "receipt_sha256",
    "trace_sha256", "adapter_proof_sha256", "rows",
    "proof_inputs_hash",
))
_COST_PROOF_ROW_FIELDS = frozenset((
    "grain", "component", "value_status", "embedded",
    "amount_usd_exact", "rate_bps_exact", "proof_role",
    "proof_sha256",
))


def _canonical_bytes(value: Any) -> bytes:
    return json.dumps(
        value, allow_nan=False, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _json_file_bytes(value: Any) -> bytes:
    return json.dumps(
        value, allow_nan=False, ensure_ascii=False, indent=2,
        sort_keys=True,
    ).encode("utf-8") + b"\n"


def _sha(value: bytes) -> str:
    return hashlib.sha256(value).hexdigest()


def _plain(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _plain(item) for key, item in value.items()}
    if isinstance(value, (tuple, list)):
        return [_plain(item) for item in value]
    return value


def _freeze(value: Any) -> Any:
    if isinstance(value, Mapping):
        return MappingProxyType({
            key: _freeze(item) for key, item in value.items()
        })
    if isinstance(value, list):
        return tuple(_freeze(item) for item in value)
    return value


def _initialize_validated_historical_cost_proof_inputs():
    issuer = object()
    registry = {}

    @dataclass(frozen=True, init=False, repr=False)
    class ValidatedHistoricalCostProofInputs:
        scenario_key: str
        proof_inputs_hash: str
        object_value: Mapping[str, Any]

        def __new__(cls, *args: Any, **kwargs: Any) -> Any:
            del cls, args, kwargs
            raise HistoricalRoutePublicationError(
                "validated historical cost proof construction is private"
            )

        def __repr__(self) -> str:
            return "ValidatedHistoricalCostProofInputs(<redacted>)"

        def __reduce_ex__(self, _protocol: int) -> Any:
            raise TypeError(
                "validated historical cost proof is not serializable"
            )

    def require(value: Any) -> Mapping[str, Any]:
        entry = registry.get(id(value))
        if (
            type(value) is not ValidatedHistoricalCostProofInputs
            or entry is None
            or entry[0]() is not value
            or entry[1].get("issuer") is not issuer
        ):
            raise HistoricalRoutePublicationError(
                "validated historical cost proof capability is invalid"
            )
        record = entry[1]
        for field in ("scenario_key", "proof_inputs_hash", "object_value"):
            try:
                current = object.__getattribute__(value, field)
            except AttributeError as error:
                raise HistoricalRoutePublicationError(
                    "validated historical cost proof capability is invalid"
                ) from error
            if current != record[field]:
                raise HistoricalRoutePublicationError(
                    "validated historical cost proof capability differs"
                )
        return record["object_value"]

    def issue(proof: Mapping[str, Any]) -> Any:
        frozen = _freeze(_plain(proof))
        value = object.__new__(ValidatedHistoricalCostProofInputs)
        fields = {
            "scenario_key": frozen["scenario_key"],
            "proof_inputs_hash": frozen["proof_inputs_hash"],
            "object_value": frozen,
        }
        for field, field_value in fields.items():
            object.__setattr__(value, field, field_value)
        value_id = id(value)
        record = {"issuer": issuer, **fields}

        def retire(reference: weakref.ReferenceType) -> None:
            current = registry.get(value_id)
            if current is not None and current[0] is reference:
                registry.pop(value_id, None)

        registry[value_id] = (weakref.ref(value, retire), record)
        return value

    return ValidatedHistoricalCostProofInputs, issue, require


(
    ValidatedHistoricalCostProofInputs,
    _issue_validated_historical_cost_proof_inputs,
    _validated_historical_cost_proof_object,
) = _initialize_validated_historical_cost_proof_inputs()
del _initialize_validated_historical_cost_proof_inputs


def _snapshot_matches(current: Any, expected: Any) -> bool:
    if current is None or expected is None:
        return current is expected
    return (
        current[0] == expected[0]
        and _route_publication._stable_file_metadata(current[1])
        == _route_publication._stable_file_metadata(expected[1])
    )


def _close_descriptors_robustly(*descriptors: Any) -> None:
    """Attempt every owned close without reversing an established result."""
    for descriptor in descriptors:
        if descriptor is not None:
            try:
                os.close(descriptor)
            except OSError:
                pass


def _install_pointer_cas(
    *, core_fd: int, core_root: Path, expected_snapshot: Any,
    pointer: Mapping[str, Any], after_install: Any,
) -> Any:
    replaced = False
    installed = result = None
    pointer_bytes = _json_file_bytes(pointer)
    try:
        fcntl.flock(core_fd, fcntl.LOCK_EX)
        current = _route_publication._optional_pointer_snapshot_at(core_fd)
        if not _snapshot_matches(current, expected_snapshot):
            raise HistoricalRoutePublicationError("publication_race")
        _route_publication._replace_pointer_bytes_at(core_fd, pointer_bytes)
        replaced = True
        installed = _route_publication._optional_pointer_snapshot_at(core_fd)
        if installed is None or installed[0] != pointer_bytes:
            raise HistoricalRoutePublicationError(
                "historical core pointer state is uncertain"
            )
        _route_publication._fsync_directory(core_root, directory_fd=core_fd)
        current = _route_publication._optional_pointer_snapshot_at(core_fd)
        if not _snapshot_matches(current, installed):
            raise HistoricalRoutePublicationError(
                "historical core pointer state is uncertain"
            )
        result = after_install()
        current = _route_publication._optional_pointer_snapshot_at(core_fd)
        if not _snapshot_matches(current, installed):
            raise HistoricalRoutePublicationError("publication_race")
        return result
    except BaseException as error:
        if result is not None:
            try:
                result.close()
            except Exception:
                pass
        if replaced:
            try:
                current = _route_publication._optional_pointer_snapshot_at(
                    core_fd
                )
                owned = (
                    _snapshot_matches(current, installed)
                    if installed is not None
                    else current is not None and current[0] == pointer_bytes
                )
                if not owned:
                    raise HistoricalRoutePublicationError(
                        "publication_race"
                    )
                if expected_snapshot is None:
                    os.unlink("latest.json", dir_fd=core_fd)
                else:
                    _route_publication._replace_pointer_bytes_at(
                        core_fd, expected_snapshot[0]
                    )
                _route_publication._fsync_directory(
                    core_root, directory_fd=core_fd
                )
                restored = (
                    _route_publication._optional_pointer_snapshot_at(core_fd)
                )
                if (
                    expected_snapshot is None and restored is not None
                    or expected_snapshot is not None
                    and (
                        restored is None
                        or restored[0] != expected_snapshot[0]
                    )
                ):
                    raise HistoricalRoutePublicationError(
                        "historical core pointer rollback failed"
                    )
            except Exception as rollback_error:
                raise HistoricalRoutePublicationError(
                    "publication_race"
                ) from rollback_error
        raise error
    finally:
        try:
            fcntl.flock(core_fd, fcntl.LOCK_UN)
        except OSError:
            pass


def _rfc3339(timestamp: int) -> str:
    return datetime.fromtimestamp(
        timestamp, tz=timezone.utc
    ).isoformat(timespec="seconds").replace("+00:00", "Z")


def _read_source_member(
    source: object, members: Mapping[str, Mapping[str, Any]], path: str,
) -> bytes:
    descriptor = members.get(path)
    if (
        type(descriptor) is not dict
        or set(descriptor) != {"path", "byte_count", "sha256"}
        or descriptor.get("path") != path
        or type(descriptor.get("byte_count")) is not int
        or not 0 < descriptor["byte_count"] <= _MAX_MEMBER_BYTES
        or type(descriptor.get("sha256")) is not str
        or re.fullmatch(r"[0-9a-f]{64}", descriptor["sha256"]) is None
    ):
        raise HistoricalRoutePublicationError(
            "historical raw member descriptor is invalid"
        )
    value = source.read_member(
        path, expected_sha256=descriptor["sha256"],
        max_bytes=descriptor["byte_count"],
    )
    if len(value) != descriptor["byte_count"] or _sha(value) != descriptor["sha256"]:
        raise HistoricalRoutePublicationError(
            "historical raw member bytes differ"
        )
    return value


def _decode_canonical_object(value: bytes, label: str) -> Dict[str, Any]:
    try:
        decoded = json.loads(value.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise HistoricalRoutePublicationError(
            "{} is invalid".format(label)
        ) from error
    if type(decoded) is not dict or _canonical_bytes(decoded) != value:
        raise HistoricalRoutePublicationError(
            "{} is not canonical".format(label)
        )
    return decoded


def _decompress_single_gzip_member_bounded(
    value: bytes, *, expected_size: int,
) -> bytes:
    if (
        type(value) is not bytes
        or type(expected_size) is not int
        or not 0 < expected_size <= _MAX_DECODED_MEMBER_BYTES
    ):
        raise HistoricalRoutePublicationError(
            "historical gzip decoded size is invalid"
        )
    inflater = zlib.decompressobj(zlib.MAX_WBITS | 16)
    decoded = bytearray()
    pending = value
    try:
        while pending and not inflater.eof:
            remaining = expected_size + 1 - len(decoded)
            if remaining <= 0:
                raise HistoricalRoutePublicationError(
                    "historical gzip member exceeds its decoded bound"
                )
            previous_size = len(pending)
            chunk = inflater.decompress(pending, remaining)
            decoded.extend(chunk)
            pending = inflater.unconsumed_tail
            if pending and len(pending) >= previous_size and not chunk:
                raise HistoricalRoutePublicationError(
                    "historical gzip member made no bounded progress"
                )
    except zlib.error as error:
        raise HistoricalRoutePublicationError(
            "historical gzip member is invalid"
        ) from error
    if (
        len(decoded) != expected_size
        or not inflater.eof
        or pending
        or inflater.unconsumed_tail
        or inflater.unused_data
    ):
        raise HistoricalRoutePublicationError(
            "historical gzip member differs"
        )
    return bytes(decoded)


def _build_run_evidence_from_source(
    *, config: HistoricalFoundryConfigSet, source: object,
) -> tuple:
    try:
        identity = dict(source.identity_projection())
        source.reread_unchanged()
    except Exception as error:
        raise HistoricalRoutePublicationError(
            "historical raw source is invalid"
        ) from error
    if (
        set(identity) != {
            "schema", "stage", "run_id", "run_manifest_sha256",
            "member_count", "selection_status",
        }
        or identity.get("schema")
        != "historical_foundry_run_snapshot_identity/v1"
        or identity.get("stage") != "complete"
        or type(identity.get("run_id")) is not str
        or re.fullmatch(r"run:[0-9a-f]{64}", identity["run_id"]) is None
        or type(identity.get("run_manifest_sha256")) is not str
        or re.fullmatch(
            r"[0-9a-f]{64}", identity["run_manifest_sha256"]
        ) is None
        or type(identity.get("member_count")) is not int
        or identity["member_count"] <= 1
        or identity.get("selection_status")
        != "found_publishable_profitable_block"
    ):
        raise HistoricalRoutePublicationError(
            "historical raw source identity is invalid"
        )
    manifest_bytes = source.read_member(
        "run_manifest.json",
        expected_sha256=identity["run_manifest_sha256"],
        max_bytes=_MAX_MEMBER_BYTES,
    )
    if _sha(manifest_bytes) != identity["run_manifest_sha256"]:
        raise HistoricalRoutePublicationError(
            "historical run manifest bytes differ"
        )
    manifest = _decode_canonical_object(
        manifest_bytes, "historical run manifest"
    )
    if (
        set(manifest) != {
            "schema", "run_id", "repository_head", "source_identity",
            "source_identity_sha256", "policy_sha256",
            "authority_sha256", "toolchain_sha256",
            "scan_inventory_sha256", "prefilter_grid_digest", "window",
            "chain_id", "prefilter_row_count", "candidate_block_count",
            "scenario_denominator", "initial_replay_required_count",
            "selection_status", "selected_block",
            "selected_scenario_count", "unresolved_candidate_count",
            "simulated_scenario_count", "resolved_candidate_count",
            "reverted_scenario_count", "positive_scenario_count",
            "member_count", "members", "publication_eligible",
        }
        or manifest.get("schema") != "historical_foundry_run_manifest/v1"
        or manifest.get("run_id") != identity.get("run_id")
        or type(manifest.get("member_count")) is not int
        or manifest.get("member_count") + 1 != identity.get("member_count")
        or manifest.get("selection_status")
        != "found_publishable_profitable_block"
        or manifest.get("publication_eligible") is not True
        or type(manifest.get("source_identity")) is not dict
        or _sha(_canonical_bytes(manifest["source_identity"]))
        != manifest.get("source_identity_sha256")
        or manifest.get("repository_head")
        != manifest["source_identity"].get("repository_head")
    ):
        raise HistoricalRoutePublicationError(
            "historical run manifest identity differs"
        )
    member_rows = manifest.get("members")
    if type(member_rows) is not list:
        raise HistoricalRoutePublicationError(
            "historical run member inventory is invalid"
        )
    if any(
        type(row) is not dict
        or set(row) != {"path", "byte_count", "sha256"}
        or type(row.get("path")) is not str
        or not row["path"]
        or row["path"].startswith("/")
        or any(part in ("", ".", "..") for part in row["path"].split("/"))
        or type(row.get("byte_count")) is not int
        or row["byte_count"] <= 0
        or type(row.get("sha256")) is not str
        or re.fullmatch(r"[0-9a-f]{64}", row["sha256"]) is None
        for row in member_rows
    ):
        raise HistoricalRoutePublicationError(
            "historical run member inventory is invalid"
        )
    members = {row["path"]: row for row in member_rows}
    if len(members) != len(member_rows):
        raise HistoricalRoutePublicationError(
            "historical run member inventory is invalid"
        )
    if len(member_rows) != manifest["member_count"]:
        raise HistoricalRoutePublicationError(
            "historical run member count differs"
        )
    for role in ("policy", "authority", "toolchain"):
        config_member = _read_source_member(
            source, members, "{}.json".format(role)
        )
        expected_config = getattr(config, role)
        if (
            config_member != expected_config.physical_bytes
            or _sha(config_member) != expected_config.physical_sha256
            or manifest.get("{}_sha256".format(role))
            != expected_config.physical_sha256
        ):
            raise HistoricalRoutePublicationError(
                "historical {} config bytes differ".format(role)
            )
    candidate_bytes = _read_source_member(
        source, members, "candidate_manifest.json"
    )
    selection_bytes = _read_source_member(source, members, "selection.json")
    typed_manifest_bytes = _read_source_member(
        source, members, "typed_manifest.json"
    )
    capture_bytes = _read_source_member(
        source, members, "scan/capture_inventory.json"
    )
    candidate = _decode_canonical_object(
        candidate_bytes, "historical candidate manifest"
    )
    selection = _decode_canonical_object(
        selection_bytes, "historical selection"
    )
    typed_manifest = _decode_canonical_object(
        typed_manifest_bytes, "historical typed manifest"
    )
    capture = _decode_canonical_object(
        capture_bytes, "historical capture inventory"
    )
    capture_configs = capture.get("configs")
    if (
        capture.get("schema")
        != "historical_foundry_capture_inventory/v1"
        or capture.get("source_identity") != manifest["source_identity"]
        or _sha(_canonical_bytes(capture["source_identity"]))
        != manifest["source_identity_sha256"]
        or capture.get("range") != manifest.get("window")
        or type(capture_configs) is not list
        or len(capture_configs) != 3
    ):
        raise HistoricalRoutePublicationError(
            "historical capture lineage differs"
        )
    for index, role in enumerate(("policy", "authority", "toolchain")):
        descriptor = capture_configs[index]
        loaded = getattr(config, role)
        path = "{}.json".format(role)
        if (
            type(descriptor) is not dict
            or set(descriptor) != {
                "role", "path", "schema", "byte_count", "sha256",
                "policy_id",
            }
            or descriptor.get("role") != role
            or descriptor.get("path") != path
            or descriptor.get("schema") != loaded.value.get("schema")
            or descriptor.get("byte_count") != len(loaded.physical_bytes)
            or descriptor.get("sha256") != loaded.physical_sha256
            or members.get(path) != {
                "path": path,
                "byte_count": len(loaded.physical_bytes),
                "sha256": loaded.physical_sha256,
            }
        ):
            raise HistoricalRoutePublicationError(
                "historical capture config descriptor differs"
            )
    if (
        selection.get("staging_inventory_sha256")
        != manifest.get("scan_inventory_sha256")
        or selection.get("status") != manifest["selection_status"]
        or selection.get("selected_block") != manifest.get("selected_block")
        or typed_manifest.get("selected_block") != manifest.get("selected_block")
    ):
        raise HistoricalRoutePublicationError(
            "historical run final member binding differs"
        )
    header_descriptors = [
        row for row in capture.get("typed_chunks", [])
        if type(row) is dict and row.get("role") == "headers"
        and row.get("block_start") <= manifest["window"]["anchor_number"]
        <= row.get("block_stop")
    ]
    if len(header_descriptors) != 1:
        raise HistoricalRoutePublicationError(
            "historical anchor header inventory is invalid"
        )
    header_descriptor = header_descriptors[0]
    header_path = header_descriptor.get("path")
    if (
        set(header_descriptor) != {
            "role", "chunk_index", "path", "block_start", "block_stop",
            "row_count", "decoded_byte_count", "decoded_sha256",
            "gzip_byte_count", "gzip_sha256",
        }
        or type(header_path) is not str
        or type(header_descriptor.get("decoded_byte_count")) is not int
        or not 0 < header_descriptor["decoded_byte_count"]
        <= _MAX_DECODED_MEMBER_BYTES
        or type(header_descriptor.get("decoded_sha256")) is not str
        or re.fullmatch(
            r"[0-9a-f]{64}", header_descriptor["decoded_sha256"]
        ) is None
        or members.get(header_path) != {
            "path": header_path,
            "byte_count": header_descriptor.get("gzip_byte_count"),
            "sha256": header_descriptor.get("gzip_sha256"),
        }
    ):
        raise HistoricalRoutePublicationError(
            "historical anchor header descriptor is invalid"
        )
    header_gzip = _read_source_member(
        source, members, header_path
    )
    try:
        header_bytes = _decompress_single_gzip_member_bounded(
            header_gzip,
            expected_size=header_descriptor["decoded_byte_count"],
        )
        header_rows = json.loads(header_bytes)
    except (
        HistoricalRoutePublicationError, UnicodeDecodeError,
        json.JSONDecodeError,
    ) as error:
        raise HistoricalRoutePublicationError(
            "historical anchor header bytes are invalid"
        ) from error
    if (
        _sha(header_bytes) != header_descriptor.get("decoded_sha256")
        or len(header_bytes) != header_descriptor.get("decoded_byte_count")
        or type(header_rows) is not list
    ):
        raise HistoricalRoutePublicationError(
            "historical anchor header bytes differ"
        )
    anchor_rows = [
        row for row in header_rows if type(row) is dict
        and row.get("number") == manifest["window"]["anchor_number"]
    ]
    if len(anchor_rows) != 1:
        raise HistoricalRoutePublicationError(
            "historical anchor header differs"
        )
    anchor_timestamp = anchor_rows[0].get("timestamp")
    selected_block = selection.get("selected_block")
    if type(anchor_timestamp) is not int or type(selected_block) is not dict:
        raise HistoricalRoutePublicationError(
            "historical selected block differs"
        )

    venues: Dict[str, Any] = {}
    typed_members = []
    markets = typed_manifest.get("markets")
    if type(markets) is not list or len(markets) != 2:
        raise HistoricalRoutePublicationError(
            "historical typed markets differ"
        )
    for market, venue_id in zip(markets, _VENUES):
        if type(market) is not dict or market.get("venue_id") != venue_id:
            raise HistoricalRoutePublicationError(
                "historical typed market order differs"
            )
        pool_payload = None
        for member in market.get("members", []):
            if type(member) is not dict:
                raise HistoricalRoutePublicationError(
                    "historical typed member differs"
                )
            raw = _read_source_member(source, members, member["path"])
            payload = _decode_canonical_object(
                raw, "historical typed payload"
            )
            role = member.get("role")
            if role == "dex_pool_state":
                adapter = "route_quantity_quote_for_v2_pool/v1"
                schema = "route_v2_pool_state/v1"
                logical = payload.get("state_id", "").split(":", 1)[-1]
                pool_payload = payload
            elif role == "dex_usd_price_context":
                adapter = "route_dex_usd_price_context/v1"
                schema = "route_dex_usd_price_context/v1"
                logical = member["sha256"]
            else:
                raise HistoricalRoutePublicationError(
                    "historical typed member role differs"
                )
            typed_members.append({
                "descriptor": {
                    "market_id": market["market_id"], "role": role,
                    "adapter_id": adapter, "content_schema": schema,
                    "path": member["path"],
                    "filename": member["path"].rsplit("/", 1)[-1],
                    "byte_count": member["byte_count"],
                    "sha256": member["sha256"],
                    "logical_generation": logical,
                },
                "payload_hex": raw.hex(),
            })
        if pool_payload is None:
            raise HistoricalRoutePublicationError(
                "historical pool member is missing"
            )
        venues[venue_id] = {
            "pair_address": market["pair_address"],
            "factory_pair_forward": market["factory_pair_forward"],
            "factory_pair_reverse": market["factory_pair_reverse"],
            "reserve_uni_raw": int(pool_payload["reserve0_raw"]),
            "reserve_weth_raw": int(pool_payload["reserve1_raw"]),
            "reserve_timestamp_last_raw": int(
                pool_payload["reserve_timestamp_last_raw"]
            ),
            "raw_response_sha256": pool_payload["raw_response_sha256"],
        }
    routes = []
    for buy, sell in (
        ("uniswap_v2", "sushiswap_v2"),
        ("sushiswap_v2", "uniswap_v2"),
    ):
        route = {
            "token_symbol": "UNI",
            "buy_market_id": next(
                row["market_id"] for row in markets
                if row["venue_id"] == buy
            ),
            "sell_market_id": next(
                row["market_id"] for row in markets
                if row["venue_id"] == sell
            ),
            "route_mode": "atomic_onchain",
        }
        routes.append({**route, "route_id": canonical_route_id(route)})
    route_by_direction = {
        "uniswap_to_sushiswap": routes[0]["route_id"],
        "sushiswap_to_uniswap": routes[1]["route_id"],
    }
    scenarios = [
        {
            "route_id": route_by_direction[row["direction"]],
            "requested_notional_usd": row["requested_notional_usd"],
            "receipt_status": row["status"],
        }
        for row in selection["selected_scenarios"]
    ]
    selected = {
        "anchor_timestamp": _rfc3339(anchor_timestamp),
        "block_timestamp": _rfc3339(selected_block["timestamp"]),
        "block_number": selected_block["number"],
        "block_hash": selected_block["hash"],
        "block_header_sha256": hashlib.sha256(
            _canonical_bytes(selected_block)
        ).hexdigest(),
        "venues": venues,
        "routes": routes,
    }
    evidence = {
        "schema": "historical_foundry_selected_run_closed/v1",
        "run_id": identity["run_id"],
        "snapshot_run_id": identity["run_id"],
        "manifest_sha256": identity["run_manifest_sha256"],
        "policy_sha256": manifest["policy_sha256"],
        "authority_sha256": manifest["authority_sha256"],
        "toolchain_sha256": manifest["toolchain_sha256"],
        "scan_inventory_sha256": manifest["scan_inventory_sha256"],
        "selection": selected,
        "selection_sha256": hashlib.sha256(
            _canonical_bytes(selected)
        ).hexdigest(),
        "scenarios": scenarios,
        "typed_members": typed_members,
        "task7_candidate_manifest_hex": candidate_bytes.hex(),
        "task7_selection_hex": selection_bytes.hex(),
        "task7_typed_manifest_hex": typed_manifest_bytes.hex(),
    }
    evidence["evidence_sha256"] = hashlib.sha256(
        _canonical_bytes(evidence)
    ).hexdigest()
    source.reread_unchanged()
    return evidence, manifest["source_identity_sha256"]


def _historical_cohort(
    *, validated_run: Mapping[str, Any], universe: Mapping[str, Any],
    core_projection: Mapping[str, Any],
) -> Dict[str, Any]:
    selection = validated_run["selection"]
    observed_at = selection["block_timestamp"]
    generation = hashlib.sha256(_canonical_bytes({
        "schema": "historical_route_candidate_generation/v1",
        "run_id": validated_run["run_id"],
        "manifest_sha256": validated_run["manifest_sha256"],
        "selection_sha256": validated_run["selection_sha256"],
        "policy_sha256": validated_run["policy_sha256"],
        "authority_sha256": validated_run["authority_sha256"],
    })).hexdigest()
    collection_generation = core_projection["universe_sha256"]
    routes = []
    for route in core_projection["routes"]:
        routes.append({
            **dict(route), "route_class": "candidate",
            "settlement_reason": None,
            "requested_notionals_usd": list(_NOTIONALS),
            "candidate_source_generation": generation,
            "buy_reference_volume_usd": None,
            "sell_reference_volume_usd": None,
            "route_volume_usd": None,
            "route_volume_basis": "minimum_leg_source_horizon_usd",
        })
    descriptor_by_market = {
        row["descriptor"]["market_id"]: row
        for row in core_projection["typed_members"]
        if row["descriptor"]["role"] == "dex_pool_state"
    }
    legs = []
    for market in core_projection["markets"]:
        typed = descriptor_by_market[market["market_id"]]
        payload = json.loads(bytes.fromhex(typed["payload_hex"]))
        legs.append({
            "leg_id": market["market_id"],
            "market_id": market["market_id"],
            "market_type": "dex", "token_symbol": "UNI",
            "status": "observed", "available": True,
            "reason_code": "observed",
            "state_observed_at": observed_at,
            "snapshot_id": validated_run["run_id"],
            "source_endpoint": "",
            "raw_response_sha256": payload["raw_response_sha256"],
            "fixed_block_number": str(selection["block_number"]),
            "fixed_block_timestamp": observed_at,
        })
    route_rows = [{
        **route, "validated_at": observed_at, "skew_seconds": "0",
        "timing_status": "within_sla", "reason_code": None,
    } for route in routes]
    cohort = {
        "schema": _route_publication.ROUTE_COHORT_SCHEMA,
        "candidate_source_generation": generation,
        "collection_input_generation": collection_generation,
        "source_state": {
            "candidate_source_generation": generation,
            "collection_input_generation": collection_generation,
        },
        "raw_evidence_run_id": validated_run["run_id"],
        "target_observed_at": observed_at,
        "collection_started_at": observed_at,
        "collection_completed_at": observed_at,
        "collection_deadline_at": observed_at,
        "skew_sla_seconds": "60", "route_age_sla_seconds": "120",
        "selection_window": {
            "start": universe["provenance_window"]["start_date"],
            "end": universe["provenance_window"]["end_date"],
        },
        "requested_notionals_usd": list(_NOTIONALS),
        "legs": sorted(legs, key=lambda row: row["market_id"]),
        "routes": sorted(routes, key=lambda row: row["route_id"]),
        "route_rows": sorted(route_rows, key=lambda row: row["route_id"]),
    }
    cohort["route_cohort_id"] = "cohort:" + hashlib.sha256(
        _canonical_bytes(cohort)
    ).hexdigest()
    cohort["fingerprint"] = hashlib.sha256(
        _canonical_bytes(cohort)
    ).hexdigest()
    return cohort


def _derive_historical_core(
    *, config: HistoricalFoundryConfigSet, source: object,
) -> Dict[str, Any]:
    evidence, source_identity_sha256 = _build_run_evidence_from_source(
        config=config, source=source
    )
    validated = validate_selected_historical_run(
        config=config, run_evidence=evidence
    )
    universe = build_historical_research_universe(
        config=config, validated_run=validated
    )
    core = build_historical_core_projection(
        config=config, validated_run=validated, universe=universe
    )
    cohort = _historical_cohort(
        validated_run=validated, universe=universe, core_projection=core,
    )
    return {
        "evidence": evidence, "validated": validated, "universe": universe,
        "core": core, "cohort": cohort,
        "source_identity_sha256": source_identity_sha256,
    }


def _historical_manifest(
    *, config: HistoricalFoundryConfigSet, derived: Mapping[str, Any],
    files: Mapping[str, Mapping[str, Any]], source_identity_sha256: str,
) -> Dict[str, Any]:
    evidence = derived["evidence"]
    cohort = derived["cohort"]
    selected = evidence["selection"]
    return {
        "schema": _MANIFEST_SCHEMA, "bundle_stage": _BUNDLE_STAGE,
        "cohort_schema": _route_publication.ROUTE_COHORT_SCHEMA,
        "temporal_scope": _TEMPORAL_SCOPE,
        "execution_claim": _EXECUTION_CLAIM,
        "route_cohort_id": cohort["route_cohort_id"],
        "cohort_fingerprint": cohort["fingerprint"],
        "candidate_source_generation": cohort["candidate_source_generation"],
        "collection_input_generation": cohort["collection_input_generation"],
        "raw_evidence_run_id": evidence["run_id"],
        "raw_run_manifest_sha256": evidence["manifest_sha256"],
        "selection_sha256": evidence["selection_sha256"],
        "scan_inventory_sha256": evidence["scan_inventory_sha256"],
        "policy_sha256": config.policy.physical_sha256,
        "authority_sha256": config.authority.physical_sha256,
        "toolchain_sha256": config.toolchain.physical_sha256,
        "source_identity_sha256": source_identity_sha256,
        "selected_block": {
            "number": selected["block_number"],
            "hash": selected["block_hash"],
            "timestamp": selected["block_timestamp"],
            "header_sha256": selected["block_header_sha256"],
        },
        "counts": {"candidates": 2, "legs": 2, "timing": 2},
        "files": {name: dict(files[name]) for name in sorted(files)},
    }


def _validate_historical_cohort(
    *, cohort: Mapping[str, Any], derived: Mapping[str, Any],
) -> None:
    if (
        type(cohort) is not dict
        or set(cohort) != _route_publication._TOP_LEVEL_FIELDS
        or cohort.get("schema") != _route_publication.ROUTE_COHORT_SCHEMA
        or re.fullmatch(
            r"run:[0-9a-f]{64}", cohort.get("raw_evidence_run_id", "")
        ) is None
        or cohort["raw_evidence_run_id"] != derived["evidence"]["run_id"]
        or cohort.get("requested_notionals_usd") != _NOTIONALS
        or len(cohort.get("routes", ())) != 2
        or len(cohort.get("legs", ())) != 2
        or len(cohort.get("route_rows", ())) != 2
    ):
        raise HistoricalRoutePublicationError(
            "historical route cohort shape differs"
        )
    expected_cohort = _historical_cohort(
        validated_run=derived["validated"], universe=derived["universe"],
        core_projection=derived["core"],
    )
    try:
        exact_match = (
            _canonical_bytes(cohort) == _canonical_bytes(expected_cohort)
        )
    except (TypeError, ValueError) as error:
        raise HistoricalRoutePublicationError(
            "historical route cohort is not canonical"
        ) from error
    if not exact_match:
        raise HistoricalRoutePublicationError(
            "historical route cohort differs from trusted derivation"
        )
    expected_markets = {
        row["market_id"] for row in derived["core"]["markets"]
    }
    expected_routes = {
        row["route_id"] for row in derived["core"]["routes"]
    }
    generation = cohort["candidate_source_generation"]
    evidence = derived["evidence"]
    expected_generation = _sha(_canonical_bytes({
        "schema": "historical_route_candidate_generation/v1",
        "run_id": evidence["run_id"],
        "manifest_sha256": evidence["manifest_sha256"],
        "selection_sha256": evidence["selection_sha256"],
        "policy_sha256": evidence["policy_sha256"],
        "authority_sha256": evidence["authority_sha256"],
    }))
    if (
        generation != expected_generation
        or cohort.get("collection_input_generation")
        != derived["core"]["universe_sha256"]
        or cohort.get("source_state") != {
            "candidate_source_generation": expected_generation,
            "collection_input_generation": derived["core"][
                "universe_sha256"
            ],
        }
    ):
        raise HistoricalRoutePublicationError(
            "historical route cohort generation lineage differs"
        )
    for route in cohort["routes"]:
        try:
            _route_publication._validate_route_candidate(
                route, candidate_generation=generation,
                requested_notionals=_NOTIONALS,
            )
        except _route_publication.RoutePublicationError as error:
            raise HistoricalRoutePublicationError(
                "historical route candidate differs"
            ) from error
    if {row["route_id"] for row in cohort["routes"]} != expected_routes:
        raise HistoricalRoutePublicationError(
            "historical route inventory differs"
        )
    try:
        _route_publication.validate_route_cohort_rows(
            cohort["routes"], cohort["legs"]
        )
        legs = _route_publication._validate_leg_rows(
            cohort["legs"],
            raw_evidence_run_id=cohort["raw_evidence_run_id"],
            collection_completed_at=cohort["collection_completed_at"],
            collection_deadline_at=cohort["collection_deadline_at"],
        )
    except (TypeError, ValueError) as error:
        raise HistoricalRoutePublicationError(
            "historical route leg inventory differs"
        ) from error
    if set(legs) != expected_markets:
        raise HistoricalRoutePublicationError(
            "historical route market inventory differs"
        )
    selected = derived["evidence"]["selection"]
    expected_leg_fields = {
        "leg_id", "market_id", "market_type", "token_symbol", "status",
        "available", "reason_code", "state_observed_at", "snapshot_id",
        "source_endpoint", "raw_response_sha256", "fixed_block_number",
        "fixed_block_timestamp",
    }
    for row in cohort["legs"]:
        if (
            type(row) is not dict
            or set(row) != expected_leg_fields
            or row.get("fixed_block_number") != str(selected["block_number"])
            or row.get("fixed_block_timestamp")
            != selected["block_timestamp"]
            or row.get("state_observed_at") != selected["block_timestamp"]
            or row.get("snapshot_id") != derived["evidence"]["run_id"]
        ):
            raise HistoricalRoutePublicationError(
                "historical route fixed-block lineage differs"
            )
    routes_by_id = {row["route_id"]: row for row in cohort["routes"]}
    timing_ids = []
    for row in cohort["route_rows"]:
        route = routes_by_id.get(row.get("route_id"))
        if (
            type(row) is not dict
            or route is None
            or set(row) != set(_route_publication._ROUTE_FIELDS) | {
                "validated_at", "skew_seconds", "timing_status",
                "reason_code",
            }
            or any(row.get(key) != value for key, value in route.items())
            or row.get("validated_at") != selected["block_timestamp"]
            or row.get("skew_seconds") != "0"
            or row.get("timing_status") != "within_sla"
            or row.get("reason_code") is not None
        ):
            raise HistoricalRoutePublicationError(
                "historical route timing lineage differs"
            )
        timing_ids.append(row["route_id"])
    if (
        len(routes_by_id) != len(cohort["routes"])
        or len(set(timing_ids)) != len(timing_ids)
        or set(timing_ids) != expected_routes
    ):
        raise HistoricalRoutePublicationError(
            "historical route timing inventory differs"
        )
    without_hashes = {
        key: value for key, value in cohort.items()
        if key not in {"route_cohort_id", "fingerprint"}
    }
    expected_id = "cohort:" + _sha(_canonical_bytes(without_hashes))
    expected_fingerprint = _sha(_canonical_bytes({
        **without_hashes, "route_cohort_id": expected_id,
    }))
    if (
        cohort.get("route_cohort_id") != expected_id
        or cohort.get("fingerprint") != expected_fingerprint
    ):
        raise HistoricalRoutePublicationError(
            "historical route cohort identity differs"
        )


def _build_artifacts(
    *, config: HistoricalFoundryConfigSet, derived: Mapping[str, Any],
) -> tuple:
    _validate_historical_cohort(
        cohort=derived["cohort"], derived=derived
    )
    representation, files = (
        _route_publication
        ._core_representation_artifact_bytes_from_validated_cohort(
            derived["cohort"]
        )
    )
    manifest = _historical_manifest(
        config=config, derived=derived, files=files,
        source_identity_sha256=derived["source_identity_sha256"],
    )
    manifest_bytes = _json_file_bytes(manifest)
    return {
        **representation, "manifest.json": manifest_bytes,
    }, manifest, _sha(manifest_bytes)


def _pointer(manifest: Mapping[str, Any], manifest_sha256: str) -> Dict[str, Any]:
    return {
        "schema": _POINTER_SCHEMA, "bundle_stage": _BUNDLE_STAGE,
        "route_cohort_id": manifest["route_cohort_id"],
        "manifest_sha256": manifest_sha256,
    }


def _validate_bundle(
    *, bundle: Path, expected_derived: Mapping[str, Any],
    expected_manifest: Mapping[str, Any], expected_manifest_sha256: str,
    bundle_fd: Any = None, expected_bundle_details: Any = None,
) -> None:
    owns_bundle_fd = bundle_fd is None
    if bundle_fd is None:
        bundle, bundle_fd, _details = _route_publication._open_verified_directory(
            bundle, "historical route core bundle"
        )
    elif expected_bundle_details is not None and (
        _route_publication._stable_file_metadata(os.fstat(bundle_fd))
        != _route_publication._stable_file_metadata(expected_bundle_details)
    ):
        raise HistoricalRoutePublicationError(
            "historical route core bundle identity differs"
        )
    try:
        if set(os.listdir(bundle_fd)) != _CORE_FILES:
            raise HistoricalRoutePublicationError(
                "historical core file inventory differs"
            )
        values = {}
        hashes = {}
        for name in sorted(_CORE_FILES):
            limit = (
                _route_publication._MAX_SQLITE_BYTES
                if name == "route_cohort.sqlite3"
                else _route_publication._MAX_CSV_BYTES
                if name.endswith(".csv")
                else _route_publication._MAX_JSON_BYTES
            )
            values[name], hashes[name], _unused = (
                _route_publication._read_bounded_bytes_at(
                    bundle_fd, name, limit=limit,
                    label="historical core {}".format(name),
                )
            )
        if (
            hashes["manifest.json"] != expected_manifest_sha256
            or values["manifest.json"] != _json_file_bytes(expected_manifest)
        ):
            raise HistoricalRoutePublicationError(
                "historical core manifest differs"
            )
        manifest = json.loads(values["manifest.json"])
        for name, descriptor in manifest["files"].items():
            if hashes[name] != descriptor["sha256"]:
                raise HistoricalRoutePublicationError(
                    "historical core artifact hash differs"
                )
        cohort = expected_derived["cohort"]
        csv_specs = (
            ("route_candidates.csv", _route_publication.CANDIDATE_COLUMNS,
             cohort["routes"], _route_publication._candidate_csv_row),
            ("route_legs.csv", _route_publication.LEG_COLUMNS,
             cohort["legs"], _route_publication._leg_csv_row),
            ("route_timing.csv", _route_publication.TIMING_COLUMNS,
             cohort["route_rows"], _route_publication._timing_csv_row),
        )
        for name, columns, rows, projector in csv_specs:
            parsed = _route_publication._read_csv_rows_bytes(
                values[name], columns=columns, label=name
            )
            _route_publication._validate_csv_projection(
                parsed, rows, route_cohort_id=cohort["route_cohort_id"],
                projector=projector, label=name,
            )
        sqlite_value = _route_publication._read_and_validate_sqlite_at(
            bundle_fd, "route_cohort.sqlite3",
            route_cohort_id=cohort["route_cohort_id"],
        )
        if (
            sqlite_value[0] != values["route_cohort.sqlite3"]
            or sqlite_value[2] != cohort
            or sqlite_value[3] != cohort["routes"]
            or sqlite_value[4] != cohort["legs"]
            or sqlite_value[5] != cohort["route_rows"]
        ):
            raise HistoricalRoutePublicationError(
                "historical core SQLite projection differs"
            )
        if set(os.listdir(bundle_fd)) != _CORE_FILES:
            raise HistoricalRoutePublicationError(
                "historical core file inventory changed"
            )
    except _route_publication.RoutePublicationError as error:
        raise HistoricalRoutePublicationError(
            "historical core bundle is invalid"
        ) from error
    finally:
        if owns_bundle_fd:
            os.close(bundle_fd)


def _context_projection(
    *, manifest: Mapping[str, Any], manifest_sha256: str,
    pointer: Mapping[str, Any],
) -> Dict[str, Any]:
    return {
        "schema": _CONTEXT_SCHEMA,
        "temporal_scope": _TEMPORAL_SCOPE,
        "execution_claim": _EXECUTION_CLAIM,
        "run_id": manifest["raw_evidence_run_id"],
        "run_manifest_sha256": manifest["raw_run_manifest_sha256"],
        "selection_sha256": manifest["selection_sha256"],
        "selected_block": dict(manifest["selected_block"]),
        "policy_sha256": manifest["policy_sha256"],
        "authority_sha256": manifest["authority_sha256"],
        "toolchain_sha256": manifest["toolchain_sha256"],
        "core_manifest_sha256": manifest_sha256,
        "core_pointer_sha256": _sha(_json_file_bytes(pointer)),
        "core_pointer": dict(pointer),
    }


def _issue_context(
    *, source: object, projection: Mapping[str, Any], owns_source: bool,
    stage_record: Any = None, stage_owner: Any = None,
    published_record: Any = None,
) -> "HistoricalReplayBuildContext":
    value = object.__new__(HistoricalReplayBuildContext)
    value_id = id(value)
    projection_bytes = _canonical_bytes(projection)
    record = {
        "issuer": _CONTEXT_ISSUER, "state": "held", "source": source,
        "projection_bytes": projection_bytes,
        "projection_sha256": _sha(projection_bytes),
        "owns_source": owns_source,
        "stage_record": stage_record, "stage_owner": stage_owner,
        "published_record": published_record,
    }
    if stage_record is not None:
        stage_record["borrow_count"] += 1
    def retire(reference: weakref.ReferenceType) -> None:
        current = _CONTEXT_REGISTRY.get(value_id)
        if current is not None and current[0] is reference:
            _CONTEXT_REGISTRY.pop(value_id, None)
            retired = current[1]
            if retired.get("state") == "held":
                retired["state"] = "gc_closed"
                borrowed = retired.get("stage_record")
                if borrowed is not None and borrowed.get("borrow_count", 0) > 0:
                    borrowed["borrow_count"] -= 1
                if retired.get("owns_source") is True:
                    try:
                        retired["source"].close()
                    except Exception:
                        pass
                retired["source"] = None
                retired["stage_owner"] = None
    _CONTEXT_REGISTRY[value_id] = (weakref.ref(value, retire), record)
    return value


def _issue_stage(record: Dict[str, Any]) -> "_StagedHistoricalReplayCore":
    value = object.__new__(_StagedHistoricalReplayCore)
    value_id = id(value)
    record.update({
        "issuer": _STAGE_ISSUER, "state": "held", "borrow_count": 0,
    })
    def retire(reference: weakref.ReferenceType) -> None:
        current = _STAGE_REGISTRY.get(value_id)
        if current is not None and current[0] is reference:
            _STAGE_REGISTRY.pop(value_id, None)
            retired = current[1]
            if retired.get("state") == "held":
                retired["state"] = "gc_closed"
                try:
                    _remove_stage_path(retired)
                except Exception:
                    pass
                try:
                    retired["source"].close()
                except Exception:
                    pass
                retired["source"] = None
    _STAGE_REGISTRY[value_id] = (weakref.ref(value, retire), record)
    return value


def _stage_record(value: object) -> Mapping[str, Any]:
    entry = _STAGE_REGISTRY.get(id(value))
    if (
        type(value) is not _StagedHistoricalReplayCore
        or entry is None
        or entry[0]() is not value
        or entry[1].get("issuer") is not _STAGE_ISSUER
        or entry[1].get("state") != "held"
    ):
        raise HistoricalRoutePublicationError(
            "historical replay core stage is invalid"
        )
    return entry[1]


def _context_record(value: object) -> Mapping[str, Any]:
    entry = _CONTEXT_REGISTRY.get(id(value))
    if (
        type(value) is not HistoricalReplayBuildContext
        or entry is None
        or entry[0]() is not value
        or entry[1].get("issuer") is not _CONTEXT_ISSUER
        or entry[1].get("state") != "held"
    ):
        raise HistoricalRoutePublicationError(
            "historical replay build context is invalid"
        )
    return entry[1]


class HistoricalReplayBuildContext:
    __slots__ = ("__weakref__",)

    def __new__(cls, *args: Any, **kwargs: Any) -> NoReturn:
        del cls, args, kwargs
        raise HistoricalRoutePublicationError(
            "historical replay build context construction is private"
        )

    def __repr__(self) -> str:
        return "HistoricalReplayBuildContext(<redacted>)"

    def __reduce_ex__(self, protocol: int) -> NoReturn:
        del protocol
        raise TypeError("historical replay build context is not serializable")

    def __setattr__(self, name: str, value: Any) -> NoReturn:
        del name, value
        raise AttributeError("historical replay build context is immutable")

    def identity_projection(self) -> Mapping[str, Any]:
        record = _context_record(self)
        return MappingProxyType(json.loads(record["projection_bytes"]))

    def reread_unchanged(self) -> None:
        record = _context_record(self)
        _validate_context_current(record)
        return None

    def close(self) -> None:
        record = _context_record(self)
        source = record.get("source")
        if record.get("owns_source") is True:
            source.close()
        record["state"] = "closed"
        stage_record = record.get("stage_record")
        if stage_record is not None:
            stage_record["borrow_count"] -= 1
        record["source"] = None
        record["stage_owner"] = None
        _CONTEXT_REGISTRY.pop(id(self), None)
        return None

    def __enter__(self) -> "HistoricalReplayBuildContext":
        _context_record(self)
        return self

    def __exit__(
        self,
        error_type: Any,
        error: Any,
        traceback: Any,
    ) -> None:
        del error_type, traceback
        try:
            return self.close()
        except BaseException as cleanup_error:
            if error is not None and not isinstance(error, Exception):
                raise error
            raise cleanup_error


class _StagedHistoricalReplayCore:
    __slots__ = ("__weakref__",)

    def __new__(cls, *args: Any, **kwargs: Any) -> NoReturn:
        del cls, args, kwargs
        raise HistoricalRoutePublicationError(
            "historical replay core stage construction is private"
        )

    def __repr__(self) -> str:
        return "_StagedHistoricalReplayCore(<redacted>)"

    def __reduce_ex__(self, protocol: int) -> NoReturn:
        del protocol
        raise TypeError("historical replay core stage is not serializable")

    def __setattr__(self, name: str, value: Any) -> NoReturn:
        del name, value
        raise AttributeError("historical replay core stage is immutable")

    def close(self) -> None:
        record = _stage_record(self)
        if record.get("borrow_count") != 0:
            raise HistoricalRoutePublicationError(
                "historical replay core stage is borrowed"
            )
        record["state"] = "closed"
        source = record.get("source")
        record["source"] = None
        _STAGE_REGISTRY.pop(id(self), None)
        cleanup_error = None
        try:
            _remove_stage_path(record)
        except Exception as error:
            cleanup_error = error
        try:
            source.close()
        except Exception as error:
            if cleanup_error is None:
                cleanup_error = error
        if cleanup_error is not None:
            raise HistoricalRoutePublicationError(
                "historical replay core stage cleanup failed"
            ) from cleanup_error
        return None

    def __enter__(self) -> "_StagedHistoricalReplayCore":
        _stage_record(self)
        return self

    def __exit__(
        self,
        error_type: Any,
        error: Any,
        traceback: Any,
    ) -> None:
        del error_type, traceback
        try:
            return self.close()
        except BaseException as cleanup_error:
            if error is not None and not isinstance(error, Exception):
                raise error
            raise cleanup_error


def _remove_stage_path(record: Mapping[str, Any]) -> None:
    if record.get("renamed") is True:
        return None
    stage = record.get("stage_path")
    bundles = record.get("bundles")
    name = record.get("stage_name")
    if (
        not isinstance(stage, Path) or not isinstance(bundles, Path)
        or type(name) is not str or not name.startswith(".historical-core-")
        or stage.parent != bundles or stage.name != name
    ):
        raise HistoricalRoutePublicationError(
            "historical replay core stage cleanup authority differs"
        )
    bundles_path, bundles_fd, _details = (
        _route_publication._open_verified_directory(
            bundles, "historical route core bundles"
        )
    )
    stage_fd = None
    try:
        _route_publication._verify_directory_entry(
            bundles_fd, name, record["stage_details"],
            "historical route core stage cleanup target",
        )
        stage_fd, current = _route_publication._open_directory_at(
            bundles_fd, name, "historical route core stage cleanup target"
        )
        if (
            current.st_dev != record["stage_details"].st_dev
            or current.st_ino != record["stage_details"].st_ino
            or set(os.listdir(stage_fd)) != _CORE_FILES
        ):
            raise HistoricalRoutePublicationError(
                "historical replay core stage cleanup target differs"
            )
        for filename in sorted(_CORE_FILES):
            current_file = os.stat(
                filename, dir_fd=stage_fd, follow_symlinks=False
            )
            if stat.S_ISREG(current_file.st_mode):
                file_fd, file_details = (
                    _route_publication._open_regular_file_at(
                        stage_fd, filename,
                        label="historical route core stage member",
                    )
                )
                try:
                    reread = os.stat(
                        filename, dir_fd=stage_fd, follow_symlinks=False
                    )
                    if (
                        _route_publication._stable_file_metadata(reread)
                        != _route_publication._stable_file_metadata(
                            file_details
                        )
                    ):
                        raise HistoricalRoutePublicationError(
                            "historical replay core stage cleanup target differs"
                        )
                finally:
                    os.close(file_fd)
            elif stat.S_ISLNK(current_file.st_mode):
                reread = os.stat(
                    filename, dir_fd=stage_fd, follow_symlinks=False
                )
                if (
                    _route_publication._stable_file_metadata(reread)
                    != _route_publication._stable_file_metadata(current_file)
                ):
                    raise HistoricalRoutePublicationError(
                        "historical replay core stage cleanup target differs"
                    )
            else:
                raise HistoricalRoutePublicationError(
                    "historical replay core stage cleanup target differs"
                )
            os.unlink(filename, dir_fd=stage_fd)
        _route_publication._verify_directory_entry(
            bundles_fd, name, record["stage_details"],
            "historical route core stage cleanup target",
        )
        os.close(stage_fd)
        stage_fd = None
        os.rmdir(name, dir_fd=bundles_fd)
        _route_publication._fsync_directory(
            bundles_path, directory_fd=bundles_fd
        )
        return None
    finally:
        if stage_fd is not None:
            os.close(stage_fd)
        os.close(bundles_fd)


def _remove_partial_stage_at(
    *, bundles: Path, bundles_fd: int, stage_name: str,
    stage_fd: int, initial_stage_details: os.stat_result,
) -> None:
    """Remove only the still-open stage generation created by this call."""
    try:
        current = os.stat(
            stage_name, dir_fd=bundles_fd, follow_symlinks=False
        )
        if (
            current.st_dev != initial_stage_details.st_dev
            or current.st_ino != initial_stage_details.st_ino
            or os.fstat(stage_fd).st_dev != initial_stage_details.st_dev
            or os.fstat(stage_fd).st_ino != initial_stage_details.st_ino
        ):
            return None
        names = set(os.listdir(stage_fd))
        if not names.issubset(_CORE_FILES):
            return None
        for filename in sorted(names):
            file_fd, opened = _route_publication._open_regular_file_at(
                stage_fd, filename,
                label="historical route core partial stage member",
            )
            try:
                current_file = os.stat(
                    filename, dir_fd=stage_fd, follow_symlinks=False
                )
                if (
                    _route_publication._stable_file_metadata(current_file)
                    != _route_publication._stable_file_metadata(opened)
                ):
                    return None
                os.unlink(filename, dir_fd=stage_fd)
            finally:
                os.close(file_fd)
        current = os.stat(
            stage_name, dir_fd=bundles_fd, follow_symlinks=False
        )
        if (
            current.st_dev != initial_stage_details.st_dev
            or current.st_ino != initial_stage_details.st_ino
        ):
            return None
        os.rmdir(stage_name, dir_fd=bundles_fd)
        _route_publication._fsync_directory(
            bundles, directory_fd=bundles_fd
        )
    except (_route_publication.RoutePublicationError, OSError):
        return None


def _validate_held_stage(record: Mapping[str, Any]) -> None:
    core_root, core_fd, _current_core = (
        _route_publication._open_verified_directory(
            record["core_root"], "historical route core root"
        )
    )
    bundles_fd = stage_fd = None
    try:
        _route_publication._verify_open_path_snapshot(
            core_root, record["core_details"],
            "historical route core root",
        )
        bundles_fd, current_bundles = _route_publication._open_directory_at(
            core_fd, "bundles", "historical route core bundles"
        )
        if (
            _route_publication._stable_file_metadata(current_bundles)
            != _route_publication._stable_file_metadata(
                record["bundles_details"]
            )
        ):
            raise HistoricalRoutePublicationError(
                "historical route core bundles changed"
            )
        _route_publication._verify_directory_entry_snapshot(
            bundles_fd, record["stage_name"], record["stage_details"],
            "historical route core stage",
        )
        stage_fd, current_stage = _route_publication._open_directory_at(
            bundles_fd, record["stage_name"],
            "historical route core stage",
        )
        if (
            _route_publication._stable_file_metadata(current_stage)
            != _route_publication._stable_file_metadata(
                record["stage_details"]
            )
        ):
            raise HistoricalRoutePublicationError(
                "historical route core stage changed"
            )
        source = record["source"]
        _historical_storage._validate_historical_run_publication_source(
            source=source
        )
        current = _derive_historical_core(
            config=record["config"], source=source
        )
        if (
            current["evidence"] != record["derived"]["evidence"]
            or current["cohort"] != record["derived"]["cohort"]
            or current["source_identity_sha256"]
            != record["derived"]["source_identity_sha256"]
        ):
            raise HistoricalRoutePublicationError(
                "historical replay core source changed"
            )
        _validate_bundle(
            bundle=record["stage_path"],
            expected_derived=record["derived"],
            expected_manifest=record["manifest"],
            expected_manifest_sha256=record["manifest_sha256"],
            bundle_fd=stage_fd,
            expected_bundle_details=record["stage_details"],
        )
        _route_publication._verify_open_path_snapshot(
            core_root, record["core_details"],
            "historical route core root",
        )
        if (
            _route_publication._stable_file_metadata(os.fstat(bundles_fd))
            != _route_publication._stable_file_metadata(
                record["bundles_details"]
            )
            or _route_publication._stable_file_metadata(os.fstat(stage_fd))
            != _route_publication._stable_file_metadata(
                record["stage_details"]
            )
        ):
            raise HistoricalRoutePublicationError(
                "historical route core stage changed during validation"
            )
        _route_publication._verify_directory_entry_snapshot(
            bundles_fd, record["stage_name"], record["stage_details"],
            "historical route core stage",
        )
    except _route_publication.RoutePublicationError as error:
        raise HistoricalRoutePublicationError(
            "historical route core stage identity differs"
        ) from error
    finally:
        if stage_fd is not None:
            os.close(stage_fd)
        if bundles_fd is not None:
            os.close(bundles_fd)
        os.close(core_fd)
    if record["pointer"] != _pointer(
        record["manifest"], record["manifest_sha256"]
    ):
        raise HistoricalRoutePublicationError(
            "historical replay core prospective pointer differs"
        )
    return None


def _validate_published_context(record: Mapping[str, Any]) -> None:
    held = record["published_record"]
    core_root, core_fd, current_core = (
        _route_publication._open_verified_directory(
            held["core_root"], "historical route core root"
        )
    )
    bundles_fd = bundle_fd = None
    try:
        if (
            _route_publication._stable_file_metadata(current_core)
            != _route_publication._stable_file_metadata(held["core_details"])
        ):
            raise HistoricalRoutePublicationError(
                "historical route core root changed"
            )
        current_pointer = _route_publication._optional_pointer_snapshot_at(
            core_fd
        )
        if not _snapshot_matches(current_pointer, held["pointer_snapshot"]):
            raise HistoricalRoutePublicationError(
                "historical core pointer changed"
            )
        bundles_fd, current_bundles = _route_publication._open_directory_at(
            core_fd, "bundles", "historical route core bundles"
        )
        if (
            _route_publication._stable_file_metadata(current_bundles)
            != _route_publication._stable_file_metadata(
                held["bundles_details"]
            )
        ):
            raise HistoricalRoutePublicationError(
                "historical route core bundles changed"
            )
        bundle_fd, current_bundle = _route_publication._open_directory_at(
            bundles_fd, held["bundle_name"],
            "historical route core bundle",
        )
        if (
            _route_publication._stable_file_metadata(current_bundle)
            != _route_publication._stable_file_metadata(held["bundle_details"])
        ):
            raise HistoricalRoutePublicationError(
                "historical route core bundle changed"
            )
        current = _derive_historical_core(
            config=held["config"], source=record["source"]
        )
        if (
            current["evidence"] != held["derived"]["evidence"]
            or current["cohort"] != held["derived"]["cohort"]
            or current["source_identity_sha256"]
            != held["derived"]["source_identity_sha256"]
        ):
            raise HistoricalRoutePublicationError(
                "historical raw source changed"
            )
        _validate_bundle(
            bundle=held["bundle_path"],
            expected_derived=held["derived"],
            expected_manifest=held["manifest"],
            expected_manifest_sha256=held["manifest_sha256"],
            bundle_fd=bundle_fd,
            expected_bundle_details=held["bundle_details"],
        )
        _route_publication._verify_open_path_snapshot(
            core_root, held["core_details"],
            "historical route core root",
        )
        final_pointer = _route_publication._optional_pointer_snapshot_at(
            core_fd
        )
        if (
            not _snapshot_matches(final_pointer, held["pointer_snapshot"])
            or _route_publication._stable_file_metadata(os.fstat(bundles_fd))
            != _route_publication._stable_file_metadata(
                held["bundles_details"]
            )
            or _route_publication._stable_file_metadata(os.fstat(bundle_fd))
            != _route_publication._stable_file_metadata(
                held["bundle_details"]
            )
        ):
            raise HistoricalRoutePublicationError(
                "historical route core changed during validation"
            )
        _route_publication._verify_directory_entry_snapshot(
            bundles_fd, held["bundle_name"], held["bundle_details"],
            "historical route core bundle",
        )
    except _route_publication.RoutePublicationError as error:
        raise HistoricalRoutePublicationError(
            "historical route core held identity differs"
        ) from error
    finally:
        if bundle_fd is not None:
            os.close(bundle_fd)
        if bundles_fd is not None:
            os.close(bundles_fd)
        os.close(core_fd)
    return None


def _validate_context_current(record: Mapping[str, Any]) -> None:
    projection_bytes = record.get("projection_bytes")
    if (
        type(projection_bytes) is not bytes
        or _sha(projection_bytes) != record.get("projection_sha256")
    ):
        raise HistoricalRoutePublicationError(
            "historical replay build context projection differs"
        )
    stage_record = record.get("stage_record")
    if stage_record is not None:
        _validate_held_stage(stage_record)
        expected = stage_record["projection"]
    elif record.get("published_record") is not None:
        _validate_published_context(record)
        held = record["published_record"]
        expected = _context_projection(
            manifest=held["manifest"],
            manifest_sha256=held["manifest_sha256"],
            pointer=_pointer(held["manifest"], held["manifest_sha256"]),
        )
    else:
        raise HistoricalRoutePublicationError(
            "historical replay build context ancestry is invalid"
        )
    if projection_bytes != _canonical_bytes(expected):
        raise HistoricalRoutePublicationError(
            "historical replay build context projection binding differs"
        )


def stage_historical_replay_core(
    *,
    data_dir: Path,
    config: HistoricalFoundryConfigSet,
    publication_lease: object,
) -> _StagedHistoricalReplayCore:
    if not isinstance(data_dir, Path) or type(config) is not HistoricalFoundryConfigSet:
        raise HistoricalRoutePublicationError(
            "historical publication input is invalid"
        )
    source = None
    stage_path = None
    stage_name = None
    initial_stage_details = None
    bundles = None
    stage_fd = None
    bundles_fd = None
    try:
        source = _historical_storage._consume_historical_run_publication_lease(
            lease=publication_lease
        )
        derived = _derive_historical_core(config=config, source=source)
        core_root = _route_publication._ensure_real_directory(
            data_dir / "routes" / "historical" / "core"
        )
        core_root, core_fd, _core_details = (
            _route_publication._open_verified_directory(
                core_root, "historical route core root"
            )
        )
        try:
            bundles_fd, _bundles_details = (
                _route_publication._ensure_directory_at(
                    core_fd, "bundles", "historical route core bundles"
                )
            )
            bundles = core_root / "bundles"
            stage_name, stage_path, stage_fd, initial_stage_details = (
                _route_publication._make_unique_directory_at(
                    bundles_fd, prefix=".historical-core-",
                    display_parent=bundles,
                )
            )
            artifacts, manifest, manifest_sha256 = _build_artifacts(
                config=config, derived=derived
            )
            for name in sorted(artifacts):
                _route_publication._write_new_bytes_at(
                    stage_fd, name, artifacts[name]
                )
            _route_publication._fsync_directory(
                stage_path, directory_fd=stage_fd
            )
            pointer = _pointer(manifest, manifest_sha256)
            _validate_bundle(
                bundle=stage_path, expected_derived=derived,
                expected_manifest=manifest,
                expected_manifest_sha256=manifest_sha256,
            )
            projection = _context_projection(
                manifest=manifest, manifest_sha256=manifest_sha256,
                pointer=pointer,
            )
            stage_details = os.fstat(stage_fd)
            bundles_details = os.fstat(bundles_fd)
            core_details = os.fstat(core_fd)
            pointer_snapshot = (
                _route_publication._optional_pointer_snapshot_at(core_fd)
            )
            stage = _issue_stage({
                "source": source, "config": config,
                "data_dir": _route_publication._absolute_without_symlink_resolution(
                    data_dir
                ),
                "core_root": core_root, "bundles": bundles,
                "stage_name": stage_name, "stage_path": stage_path,
                "stage_details": stage_details, "renamed": False,
                "bundles_details": bundles_details,
                "core_details": core_details,
                "pointer_snapshot": pointer_snapshot,
                "derived": derived, "manifest": manifest,
                "manifest_sha256": manifest_sha256, "pointer": pointer,
                "projection": projection,
            })
            source = None
            stage_path = None
            return stage
        finally:
            if (
                stage_path is not None
                and bundles is not None
                and bundles_fd is not None
                and stage_name is not None
                and stage_fd is not None
                and initial_stage_details is not None
            ):
                _remove_partial_stage_at(
                    bundles=bundles, bundles_fd=bundles_fd,
                    stage_name=stage_name, stage_fd=stage_fd,
                    initial_stage_details=initial_stage_details,
                )
            if stage_fd is not None:
                os.close(stage_fd)
            if bundles_fd is not None:
                os.close(bundles_fd)
            os.close(core_fd)
    except BaseException as error:
        if source is not None:
            try:
                source.close()
            except Exception:
                pass
        if not isinstance(error, Exception):
            raise
        if isinstance(error, HistoricalRoutePublicationError):
            raise
        raise HistoricalRoutePublicationError(
            "historical replay core staging failed"
        ) from error


def load_validated_historical_replay_core_at(
    *,
    staged_core: _StagedHistoricalReplayCore,
) -> HistoricalReplayBuildContext:
    record = _stage_record(staged_core)
    _validate_held_stage(record)
    return _issue_context(
        source=record["source"], projection=record["projection"],
        owns_source=False, stage_record=record, stage_owner=staged_core,
    )


def publish_historical_replay_core(
    *,
    data_dir: Path,
    staged_core: _StagedHistoricalReplayCore,
) -> HistoricalReplayBuildContext:
    record = _stage_record(staged_core)
    if (
        not isinstance(data_dir, Path)
        or _route_publication._absolute_without_symlink_resolution(data_dir)
        != record["data_dir"]
        or record["borrow_count"] != 0
    ):
        raise HistoricalRoutePublicationError(
            "historical replay core publish authority differs"
        )
    core_root = record["core_root"]
    core_fd = bundles_fd = None
    pre_source = fresh_context = None
    renamed = False
    try:
        _validate_held_stage(record)
        core_root, core_fd, current_core = (
            _route_publication._open_verified_directory(
                record["core_root"], "historical route core root"
            )
        )
        if (
            _route_publication._stable_file_metadata(current_core)
            != _route_publication._stable_file_metadata(record["core_details"])
        ):
            raise HistoricalRoutePublicationError("publication_race")
        bundles_fd, current_bundles = _route_publication._open_directory_at(
            core_fd, "bundles", "historical route core bundles"
        )
        if (
            _route_publication._stable_file_metadata(current_bundles)
            != _route_publication._stable_file_metadata(
                record["bundles_details"]
            )
        ):
            raise HistoricalRoutePublicationError("publication_race")
        _route_publication._verify_directory_entry_snapshot(
            bundles_fd, record["stage_name"], record["stage_details"],
            "historical route core stage",
        )
        final_name = record["pointer"]["route_cohort_id"]
        final_path = record["bundles"] / final_name
        _route_publication._rename_directory_noreplace_at(
            bundles_fd, record["stage_name"], bundles_fd, final_name,
            destination_display=final_path,
        )
        renamed = True
        record["renamed"] = True
        _route_publication._verify_directory_entry(
            bundles_fd, final_name, record["stage_details"],
            "historical route core final bundle",
        )
        final_details = os.stat(
            final_name, dir_fd=bundles_fd, follow_symlinks=False
        )
        _route_publication._fsync_directory(
            record["bundles"], directory_fd=bundles_fd
        )
        _validate_bundle(
            bundle=final_path, expected_derived=record["derived"],
            expected_manifest=record["manifest"],
            expected_manifest_sha256=record["manifest_sha256"],
        )
        _route_publication._verify_directory_entry_snapshot(
            bundles_fd, final_name, final_details,
            "historical route core final bundle",
        )
        pre_source = _historical_storage.open_validated_run(
            data_dir=data_dir,
            run_id=record["manifest"]["raw_evidence_run_id"],
            expected_manifest_sha256=record["manifest"][
                "raw_run_manifest_sha256"
            ],
        )
        pre_derived = _derive_historical_core(
            config=record["config"], source=pre_source
        )
        if (
            pre_derived["evidence"] != record["derived"]["evidence"]
            or pre_derived["cohort"] != record["derived"]["cohort"]
        ):
            raise HistoricalRoutePublicationError(
                "historical committed raw source differs"
            )
        pre_source.close()
        pre_source = None
        def validate_installed_pointer():
            context = load_latest_historical_replay_core(data_dir=data_dir)
            try:
                if dict(context.identity_projection()) != record["projection"]:
                    raise HistoricalRoutePublicationError(
                        "historical committed context differs"
                    )
                record["source"].close()
                record["source"] = None
                return context
            except BaseException:
                context.close()
                raise

        fresh_context = _install_pointer_cas(
            core_fd=core_fd, core_root=core_root,
            expected_snapshot=record["pointer_snapshot"],
            pointer=record["pointer"],
            after_install=validate_installed_pointer,
        )
        record["state"] = "published"
        _STAGE_REGISTRY.pop(id(staged_core), None)
        result = fresh_context
        fresh_context = None
        return result
    except BaseException as error:
        if fresh_context is not None:
            try:
                fresh_context.close()
            except Exception:
                pass
        if pre_source is not None:
            try:
                pre_source.close()
            except Exception:
                pass
        if not renamed:
            try:
                _remove_stage_path(record)
            except Exception:
                pass
        try:
            if record.get("source") is not None:
                record["source"].close()
        except Exception:
            pass
        record["source"] = None
        record["state"] = "failed"
        _STAGE_REGISTRY.pop(id(staged_core), None)
        if not isinstance(error, Exception):
            raise
        if isinstance(error, HistoricalRoutePublicationError):
            raise
        raise HistoricalRoutePublicationError(
            "historical replay core publication failed"
        ) from error
    finally:
        _close_descriptors_robustly(bundles_fd, core_fd)


def load_latest_historical_replay_core(
    *,
    data_dir: Path,
) -> HistoricalReplayBuildContext:
    if not isinstance(data_dir, Path):
        raise HistoricalRoutePublicationError(
            "historical publication input is invalid"
        )
    core_root, core_fd, _details = _route_publication._open_verified_directory(
        data_dir / "routes" / "historical" / "core",
        "historical route core root",
    )
    source = None
    bundles_fd = bundle_fd = None
    try:
        pointer_snapshot = _route_publication._optional_pointer_snapshot_at(
            core_fd
        )
        if pointer_snapshot is None:
            raise HistoricalRoutePublicationError(
                "historical core pointer is missing"
            )
        pointer_bytes = pointer_snapshot[0]
        try:
            pointer = json.loads(pointer_bytes)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise HistoricalRoutePublicationError(
                "historical core pointer is invalid"
            ) from error
        if (
            set(pointer) != {
                "schema", "bundle_stage", "route_cohort_id",
                "manifest_sha256",
            }
            or pointer.get("schema") != _POINTER_SCHEMA
            or pointer.get("bundle_stage") != _BUNDLE_STAGE
            or pointer_bytes != _json_file_bytes(pointer)
            or type(pointer.get("route_cohort_id")) is not str
            or re.fullmatch(
                r"cohort:[0-9a-f]{64}", pointer["route_cohort_id"]
            ) is None
            or type(pointer.get("manifest_sha256")) is not str
            or re.fullmatch(
                r"[0-9a-f]{64}", pointer["manifest_sha256"]
            ) is None
        ):
            raise HistoricalRoutePublicationError(
                "historical core pointer schema is invalid"
            )
        bundles_fd, bundles_details = _route_publication._open_directory_at(
            core_fd, "bundles", "historical route core bundles"
        )
        bundle_fd, bundle_details = _route_publication._open_directory_at(
            bundles_fd, pointer["route_cohort_id"],
            "historical route core bundle",
        )
        bundle_path = (
            core_root / "bundles" / pointer["route_cohort_id"]
        )
        manifest_bytes, manifest_sha, _manifest_details = (
            _route_publication._read_bounded_bytes_at(
                bundle_fd, "manifest.json",
                limit=_route_publication._MAX_JSON_BYTES,
                label="historical core manifest",
            )
        )
        if manifest_sha != pointer["manifest_sha256"]:
            raise HistoricalRoutePublicationError(
                "historical core pointer manifest binding differs"
            )
        try:
            manifest = json.loads(manifest_bytes)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise HistoricalRoutePublicationError(
                "historical core manifest is invalid"
            ) from error
        config = load_historical_foundry_config_set()
        if any(
            manifest.get("{}_sha256".format(role))
            != getattr(config, role).physical_sha256
            for role in ("policy", "authority", "toolchain")
        ):
            raise HistoricalRoutePublicationError(
                "historical core config binding differs"
            )
        source = _historical_storage.open_validated_run(
            data_dir=data_dir, run_id=manifest["raw_evidence_run_id"],
            expected_manifest_sha256=manifest["raw_run_manifest_sha256"],
        )
        derived = _derive_historical_core(config=config, source=source)
        artifacts, expected_manifest, expected_manifest_sha = _build_artifacts(
            config=config, derived=derived
        )
        del artifacts
        expected_pointer = _pointer(
            expected_manifest, expected_manifest_sha
        )
        if pointer != expected_pointer:
            raise HistoricalRoutePublicationError(
                "historical core pointer content differs"
            )
        _validate_bundle(
            bundle=bundle_path, expected_derived=derived,
            expected_manifest=expected_manifest,
            expected_manifest_sha256=expected_manifest_sha,
            bundle_fd=bundle_fd,
            expected_bundle_details=bundle_details,
        )
        current_pointer = _route_publication._optional_pointer_snapshot_at(
            core_fd
        )
        if not _snapshot_matches(current_pointer, pointer_snapshot):
            raise HistoricalRoutePublicationError(
                "historical core pointer changed during validation"
            )
        projection = _context_projection(
            manifest=expected_manifest,
            manifest_sha256=expected_manifest_sha,
            pointer=expected_pointer,
        )
        published_record = {
            "config": config, "core_root": core_root,
            "core_details": os.fstat(core_fd),
            "pointer_snapshot": current_pointer,
            "bundles_details": bundles_details,
            "bundle_name": pointer["route_cohort_id"],
            "bundle_path": bundle_path, "bundle_details": bundle_details,
            "derived": derived, "manifest": expected_manifest,
            "manifest_sha256": expected_manifest_sha,
        }
        context = _issue_context(
            source=source, projection=projection, owns_source=True,
            published_record=published_record,
        )
        context.reread_unchanged()
        source = None
        return context
    except _route_publication.RoutePublicationError as error:
        raise HistoricalRoutePublicationError(
            "historical core loading failed"
        ) from error
    finally:
        if source is not None:
            source.close()
        if bundle_fd is not None:
            os.close(bundle_fd)
        if bundles_fd is not None:
            os.close(bundles_fd)
        os.close(core_fd)


def _require_historical_replay_build_context(
    *,
    context: object,
) -> Mapping[str, Any]:
    record = _context_record(context)
    _validate_context_current(record)
    return MappingProxyType(json.loads(record["projection_bytes"]))


def _decode_gzip_canonical_object(value: bytes, label: str) -> Dict[str, Any]:
    inflater = zlib.decompressobj(zlib.MAX_WBITS | 16)
    try:
        decoded = inflater.decompress(value, _MAX_DECODED_MEMBER_BYTES + 1)
        decoded += inflater.flush()
    except zlib.error as error:
        raise HistoricalRoutePublicationError(
            "{} is invalid".format(label)
        ) from error
    if (
        len(decoded) > _MAX_DECODED_MEMBER_BYTES
        or not inflater.eof
        or inflater.unused_data
        or inflater.unconsumed_tail
    ):
        raise HistoricalRoutePublicationError(
            "{} is invalid".format(label)
        )
    return _decode_canonical_object(decoded, label)


def _canonical_decimal_or_none(value: Any, label: str) -> Any:
    if value is None:
        return None
    if type(value) is not str or not value or value != value.strip():
        raise HistoricalRoutePublicationError("{} is invalid".format(label))
    try:
        from decimal import Decimal, InvalidOperation
        number = Decimal(value)
    except (InvalidOperation, TypeError, ValueError) as error:
        raise HistoricalRoutePublicationError(
            "{} is invalid".format(label)
        ) from error
    canonical = format(number, "f")
    if "." in canonical:
        canonical = canonical.rstrip("0").rstrip(".")
    if number == 0:
        canonical = "0"
    if not number.is_finite() or number < 0 or value != canonical:
        raise HistoricalRoutePublicationError("{} is invalid".format(label))
    return value


def _historical_exact_terminating_decimal(
    numerator: int, denominator: int,
) -> str:
    if (
        type(numerator) is not int
        or type(denominator) is not int
        or numerator < 0
        or denominator <= 0
    ):
        raise HistoricalRoutePublicationError(
            "historical exact decimal input is invalid"
        )
    integer, remainder = divmod(numerator, denominator)
    if remainder == 0:
        return str(integer)
    digits = []
    while remainder and len(digits) <= 4_096:
        remainder *= 10
        digit, remainder = divmod(remainder, denominator)
        digits.append(str(digit))
    if remainder:
        raise HistoricalRoutePublicationError(
            "historical exact decimal is nonterminating"
        )
    return "{}.{}".format(integer, "".join(digits).rstrip("0"))


def _validate_historical_cost_proof(
    *, proof: Mapping[str, Any], scenario_key: str,
    policy_sha256: str, receipt_sha256: str, trace_sha256: str,
    adapter_proof_sha256: str,
) -> Mapping[str, Any]:
    if (
        type(proof) is not dict
        or frozenset(proof) != _COST_PROOF_FIELDS
        or proof.get("schema")
        != "historical_foundry_cost_proof_inputs/v1"
        or proof.get("scenario_key") != scenario_key
        or proof.get("policy_sha256") != policy_sha256
        or proof.get("receipt_sha256") != receipt_sha256
        or proof.get("trace_sha256") != trace_sha256
        or proof.get("adapter_proof_sha256") != adapter_proof_sha256
        or type(proof.get("rows")) is not list
        or len(proof["rows"]) != 9
    ):
        raise HistoricalRoutePublicationError(
            "historical cost proof input is invalid"
        )
    roles = (
        "receipt", "receipt", "receipt", "receipt", "receipt",
        "receipt", "receipt", "trace", "policy",
    )
    role_hash = {
        "receipt": receipt_sha256,
        "trace": trace_sha256,
        "policy": policy_sha256,
    }
    for row, shape, role in zip(
        proof["rows"], HISTORICAL_ATOMIC_COMPONENT_MATRIX, roles
    ):
        if (
            type(row) is not dict
            or frozenset(row) != _COST_PROOF_ROW_FIELDS
            or (
                row.get("grain"), row.get("component"),
                row.get("value_status"), row.get("embedded"),
            ) != shape
            or row.get("proof_role") != role
            or row.get("proof_sha256") != role_hash[role]
        ):
            raise HistoricalRoutePublicationError(
                "historical cost proof row is invalid"
            )
        amount = _canonical_decimal_or_none(
            row.get("amount_usd_exact"), "historical cost proof amount"
        )
        rate = _canonical_decimal_or_none(
            row.get("rate_bps_exact"), "historical cost proof rate"
        )
        if (
            row["value_status"] in ("bounded_estimate", "assumed")
            and amount is None
            or row["value_status"] == "not_applicable"
            and (amount is not None or rate is not None)
        ):
            raise HistoricalRoutePublicationError(
                "historical cost proof numeric state is invalid"
            )
    unsigned = {
        key: value for key, value in proof.items()
        if key != "proof_inputs_hash"
    }
    expected_hash = _sha(
        b"historical_foundry_cost_proof_inputs/v1\0"
        + _canonical_bytes(unsigned)
    )
    if proof.get("proof_inputs_hash") != expected_hash:
        raise HistoricalRoutePublicationError(
            "historical cost proof hash differs"
        )
    return proof


def _validate_historical_feed_validity_boundary(
    *, updated_at: int, block_timestamp: int, max_age_seconds: int,
    valid_until: int,
) -> None:
    if (
        any(type(value) is not int for value in (
            updated_at, block_timestamp, max_age_seconds, valid_until,
        ))
        or updated_at <= 0
        or max_age_seconds <= 0
        or block_timestamp < updated_at
        or block_timestamp - updated_at > max_age_seconds
        or valid_until != updated_at + max_age_seconds + 1
    ):
        raise HistoricalRoutePublicationError(
            "historical price validity boundary differs"
        )
    return None


def _historical_scenario_material(
    *, context: HistoricalReplayBuildContext, scenario_key: str,
    validate_context: bool,
) -> Mapping[str, Any]:
    if type(scenario_key) is not str or not scenario_key:
        raise HistoricalRoutePublicationError(
            "historical scenario key is invalid"
        )
    record = _context_record(context)
    if validate_context:
        _validate_context_current(record)
    source = record["source"]
    try:
        identity = dict(source.identity_projection())
    except Exception as error:
        raise HistoricalRoutePublicationError(
            "historical raw source identity is invalid"
        ) from error
    manifest_sha256 = identity.get("run_manifest_sha256")
    if (
        type(manifest_sha256) is not str
        or re.fullmatch(r"[0-9a-f]{64}", manifest_sha256) is None
    ):
        raise HistoricalRoutePublicationError(
            "historical raw source identity is invalid"
        )
    manifest_bytes = source.read_member(
        "run_manifest.json", expected_sha256=manifest_sha256,
        max_bytes=_MAX_MEMBER_BYTES,
    )
    if _sha(manifest_bytes) != manifest_sha256:
        raise HistoricalRoutePublicationError(
            "historical run manifest bytes differ"
        )
    manifest = _decode_canonical_object(
        manifest_bytes, "historical run manifest"
    )
    member_rows = manifest.get("members")
    if (
        type(member_rows) is not list
        or len(member_rows) != manifest.get("member_count")
        or any(
            type(row) is not dict
            or set(row) != {"path", "byte_count", "sha256"}
            for row in member_rows
        )
    ):
        raise HistoricalRoutePublicationError(
            "historical run member inventory is invalid"
        )
    members = {row["path"]: row for row in member_rows}
    if len(members) != len(member_rows):
        raise HistoricalRoutePublicationError(
            "historical run member inventory is invalid"
        )
    read_descriptors = [{
        "path": "run_manifest.json",
        "byte_count": len(manifest_bytes),
        "sha256": manifest_sha256,
    }]

    def read(path: str) -> bytes:
        value = _read_source_member(source, members, path)
        read_descriptors.append(dict(members[path]))
        return value

    raw_config = {}
    config_objects = {}
    for role in ("policy", "authority", "toolchain"):
        raw_config[role] = read(role + ".json")
        try:
            config_objects[role] = json.loads(raw_config[role])
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise HistoricalRoutePublicationError(
                "historical {} config is invalid".format(role)
            ) from error
        if (
            type(config_objects[role]) is not dict
            or _sha(raw_config[role]) != manifest.get(role + "_sha256")
        ):
            raise HistoricalRoutePublicationError(
                "historical {} config differs".format(role)
            )
    candidate = _decode_canonical_object(
        read("candidate_manifest.json"), "historical candidate manifest"
    )
    selection = _decode_canonical_object(
        read("selection.json"), "historical selection"
    )
    typed_manifest = _decode_canonical_object(
        read("typed_manifest.json"), "historical typed manifest"
    )
    capture_inventory = _decode_canonical_object(
        read("scan/capture_inventory.json"),
        "historical capture inventory",
    )
    prefilter_inventory = _decode_canonical_object(
        read("scan/prefilter_inventory.json"),
        "historical prefilter inventory",
    )
    selected_block = selection.get("selected_block")
    if (
        type(selected_block) is not dict
        or selected_block != typed_manifest.get("selected_block")
        or selected_block != manifest.get("selected_block")
        or candidate.get("selected_block") not in (None, selected_block)
    ):
        raise HistoricalRoutePublicationError(
            "historical selected block differs"
        )
    selected_number = selected_block["number"]

    def decoded_chunk(descriptor: Mapping[str, Any], label: str) -> Any:
        if (
            type(descriptor) is not dict
            or type(descriptor.get("path")) is not str
            or type(descriptor.get("decoded_byte_count")) is not int
            or type(descriptor.get("decoded_sha256")) is not str
            or descriptor.get("path") not in members
            or members[descriptor["path"]] != {
                "path": descriptor["path"],
                "byte_count": descriptor.get("gzip_byte_count"),
                "sha256": descriptor.get("gzip_sha256"),
            }
        ):
            raise HistoricalRoutePublicationError(
                "{} descriptor is invalid".format(label)
            )
        compressed = read(descriptor["path"])
        decoded = _decompress_single_gzip_member_bounded(
            compressed,
            expected_size=descriptor["decoded_byte_count"],
        )
        if _sha(decoded) != descriptor["decoded_sha256"]:
            raise HistoricalRoutePublicationError(
                "{} bytes differ".format(label)
            )
        try:
            return json.loads(decoded)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise HistoricalRoutePublicationError(
                "{} is invalid".format(label)
            ) from error

    selected_capture_chunks = {}
    for role in ("headers", "fees"):
        descriptors = [
            row for row in capture_inventory.get("typed_chunks", [])
            if type(row) is dict and row.get("role") == role
            and row.get("block_start") <= selected_number
            <= row.get("block_stop")
        ]
        if len(descriptors) != 1:
            raise HistoricalRoutePublicationError(
                "historical {} chunk inventory differs".format(role)
            )
        rows = decoded_chunk(
            descriptors[0], "historical {} chunk".format(role)
        )
        if type(rows) is not list:
            raise HistoricalRoutePublicationError(
                "historical {} rows are invalid".format(role)
            )
        matches = [
            row for row in rows
            if type(row) is dict and row.get("block_number") == selected_number
        ]
        if role == "headers":
            matches = [
                row for row in rows
                if type(row) is dict and row.get("number") == selected_number
            ]
        if len(matches) != 1:
            raise HistoricalRoutePublicationError(
                "historical selected {} row differs".format(role)
            )
        selected_capture_chunks[role] = matches[0]
    if {
        key: selected_capture_chunks["headers"].get(key)
        for key in selected_block
    } != selected_block:
        raise HistoricalRoutePublicationError(
            "historical selected header differs"
        )
    prefilter_descriptors = prefilter_inventory.get("prefilter_chunks")
    if type(prefilter_descriptors) is not list or not prefilter_descriptors:
        raise HistoricalRoutePublicationError(
            "historical prefilter chunk inventory differs"
        )
    prefilter_rows = []
    for descriptor in prefilter_descriptors:
        decoded = decoded_chunk(
            descriptor, "historical prefilter chunk"
        )
        if type(decoded) is not list:
            raise HistoricalRoutePublicationError(
                "historical prefilter rows are invalid"
            )
        prefilter_rows.extend(decoded)
    selected_rows = selection.get("selected_scenarios")
    matches = [
        row for row in selected_rows
        if type(row) is dict and row.get("scenario_key") == scenario_key
    ] if type(selected_rows) is list else []
    if len(matches) != 1:
        raise HistoricalRoutePublicationError(
            "historical scenario selection differs"
        )
    selected = matches[0]
    prefilter_matches = [
        row for row in prefilter_rows
        if type(row) is dict and row.get("scenario_key") == scenario_key
    ]
    if len(prefilter_matches) != 1:
        raise HistoricalRoutePublicationError(
            "historical prefilter scenario differs"
        )
    prefilter_row = prefilter_matches[0]
    if (
        selected.get("status") != 1
        or selected.get("classification") != "replay_success"
        or selected.get("block_number") != selected_block.get("number")
        or prefilter_row.get("block_number") != selected_block.get("number")
        or prefilter_row.get("direction") != selected.get("direction")
        or prefilter_row.get("requested_notional_usd")
        != selected.get("requested_notional_usd")
    ):
        raise HistoricalRoutePublicationError(
            "historical selected scenario is not proved"
        )
    pools = {}
    feeds = {}
    feed_sha_by_venue = {}
    markets = typed_manifest.get("markets")
    if type(markets) is not list or len(markets) != 2:
        raise HistoricalRoutePublicationError(
            "historical typed market inventory differs"
        )
    for market, venue in zip(markets, _VENUES):
        if (
            type(market) is not dict
            or market.get("venue_id") != venue
            or market.get("pair_address") not in (
                "0xd3d2e2692501a5c9ca623199d38826e513033a17",
                "0xdafd66636e2561b0284edde37e42d192f2844d40",
            )
        ):
            raise HistoricalRoutePublicationError(
                "historical typed market differs"
            )
        market_members = market.get("members")
        if type(market_members) is not list or len(market_members) != 2:
            raise HistoricalRoutePublicationError(
                "historical typed member inventory differs"
            )
        for member in market_members:
            if type(member) is not dict or member.get("path") not in members:
                raise HistoricalRoutePublicationError(
                    "historical typed member descriptor differs"
                )
            raw = read(member["path"])
            payload = _decode_canonical_object(
                raw, "historical typed scenario dependency"
            )
            if (
                member.get("byte_count") != len(raw)
                or member.get("sha256") != _sha(raw)
            ):
                raise HistoricalRoutePublicationError(
                    "historical typed member bytes differ"
                )
            role = member.get("role")
            if role == "dex_pool_state":
                pools[venue] = payload
            elif role == "dex_usd_price_context":
                feeds[venue] = payload
                feed_sha_by_venue[venue] = member["sha256"]
            else:
                raise HistoricalRoutePublicationError(
                    "historical typed member role differs"
                )
    for venue in _VENUES:
        pool = pools.get(venue)
        feed = feeds.get(venue)
        if (
            type(pool) is not dict
            or pool.get("schema") != "route_v2_pool_state/v1"
            or pool.get("dex") != venue
            or int(pool.get("block_number", -1)) != selected_block["number"]
            or pool.get("block_hash") != selected_block["hash"]
            or type(feed) is not dict
            or feed.get("schema") != "route_dex_usd_price_context/v1"
            or feed.get("venue_id") != venue
            or int(feed.get("block_number", -1)) != selected_block["number"]
            or feed.get("block_hash") != selected_block["hash"]
        ):
            raise HistoricalRoutePublicationError(
                "historical typed scenario dependency differs"
            )
    comparable_feed_fields = {
        key for key in feeds[_VENUES[0]]
        if key not in {"market_id", "venue_id"}
    }
    if any(
        {
            key: feeds[_VENUES[0]][key] for key in comparable_feed_fields
        } != {
            key: feeds[_VENUES[1]][key] for key in comparable_feed_fields
        }
        for _unused in (0,)
    ):
        raise HistoricalRoutePublicationError(
            "historical price contexts differ"
        )
    feed = feeds[_VENUES[0]]
    updated_at = int(feed["updated_at"])
    valid_until = int(feed["valid_until"])
    max_age = config_objects["policy"]["max_eth_usd_age_seconds"]
    block_timestamp = selected_block["timestamp"]
    _validate_historical_feed_validity_boundary(
        updated_at=updated_at,
        block_timestamp=block_timestamp,
        max_age_seconds=max_age,
        valid_until=valid_until,
    )
    scenario_root = "foundry/{}/{}".format(
        selected_block["number"], scenario_key
    )
    overlay_bytes = read(scenario_root + "/overlay.json")
    receipt_bytes = read(scenario_root + "/receipt.json")
    trace_bytes = read(scenario_root + "/trace.json.gz")
    result_bytes = read(scenario_root + "/result.json")
    overlay = _decode_canonical_object(
        overlay_bytes, "historical scenario overlay"
    )
    receipt = _decode_canonical_object(
        receipt_bytes, "historical scenario receipt"
    )
    trace = _decode_gzip_canonical_object(
        trace_bytes, "historical scenario trace"
    )
    result = _decode_canonical_object(
        result_bytes, "historical scenario result"
    )
    hashes = {
        "overlay_sha256": _sha(overlay_bytes),
        "receipt_sha256": _sha(receipt_bytes),
        "trace_sha256": _sha(trace_bytes),
        "result_sha256": _sha(result_bytes),
    }
    if any(selected.get(field) != value for field, value in hashes.items()):
        raise HistoricalRoutePublicationError(
            "historical scenario member binding differs"
        )
    proof_authority = result.get("proof_authority")
    if type(proof_authority) is not dict:
        raise HistoricalRoutePublicationError(
            "historical scenario proof authority is invalid"
        )
    adapter_sha = config_objects["toolchain"]["executor_build"][
        "creation_bytecode_sha256"
    ]
    proof = _validate_historical_cost_proof(
        proof=result.get("cost_proof_inputs"),
        scenario_key=scenario_key,
        policy_sha256=manifest["policy_sha256"],
        receipt_sha256=hashes["receipt_sha256"],
        trace_sha256=hashes["trace_sha256"],
        adapter_proof_sha256=adapter_sha,
    )
    try:
        amount_denominator = 10 ** (
            18 + proof_authority["feed_decimals"]
        )
        fee_numerator = proof_authority["v2_fee_numerator"]
        fee_denominator = proof_authority["v2_fee_denominator"]
        fee_units = fee_denominator - fee_numerator
        first_pool_fee = _historical_exact_terminating_decimal(
            proof_authority["amount_weth_in_wei"]
            * proof_authority["eth_usd_answer"] * fee_units,
            amount_denominator * fee_denominator,
        )
        second_pool_fee = _historical_exact_terminating_decimal(
            proof_authority["actual_first_leg_uni_raw"]
            * proof_authority["second_leg_reserve_weth_raw"]
            * proof_authority["eth_usd_answer"] * fee_units,
            proof_authority["second_leg_reserve_uni_raw"]
            * amount_denominator * fee_denominator,
        )
        gas_amount = _historical_exact_terminating_decimal(
            receipt["gasUsed"] * receipt["effectiveGasPrice"]
            * proof_authority["eth_usd_answer"],
            amount_denominator,
        )
        mev_bps = int(proof_authority["acceptance_mev_bps"])
        mev_amount = _historical_exact_terminating_decimal(
            selected["requested_notional_usd"] * mev_bps, 10_000
        )
    except (KeyError, TypeError, ValueError) as error:
        raise HistoricalRoutePublicationError(
            "historical cost proof arithmetic input is invalid"
        ) from error
    expected_amounts = (
        first_pool_fee, "0", "0", second_pool_fee, "0", "0",
        gas_amount, None, mev_amount,
    )
    expected_rates = (
        "30", "0", "0", "30", "0", "0", None, None,
        str(mev_bps),
    )
    if any(
        row["amount_usd_exact"] != amount
        or row["rate_bps_exact"] != rate
        for row, amount, rate in zip(
            proof["rows"], expected_amounts, expected_rates
        )
    ):
        raise HistoricalRoutePublicationError(
            "historical cost proof arithmetic differs"
        )
    if (
        selected.get("proof_inputs_hash") != proof["proof_inputs_hash"]
        or result.get("schema") != "historical_foundry_replay_result/v1"
        or result.get("scenario_key") != scenario_key
        or result.get("status") != 1
        or result.get("classification") != "replay_success"
        or result.get("overlay_sha256") != hashes["overlay_sha256"]
        or result.get("receipt_sha256") != hashes["receipt_sha256"]
        or result.get("trace_sha256") != hashes["trace_sha256"]
        or result.get("fork_header") != selected_block
        or receipt.get("schema") != "historical_foundry_receipt/v1"
        or receipt.get("scenario_key") != scenario_key
        or receipt.get("status") != 1
        or receipt.get("gasUsed") != selected.get("gas_used")
        or receipt.get("effectiveGasPrice")
        != selected.get("effective_gas_price")
        or trace.get("schema") != "historical_foundry_trace/v1"
        or trace.get("scenario_key") != scenario_key
        or trace.get("failed") is not False
        or trace.get("gasprice_opcode_addresses") != []
        or result.get("balances") != trace.get("balances")
        or result.get("actual_deltas") != trace.get("actual_deltas")
        or result.get("pair_closure") != trace.get("pair_closure")
        or result.get("trace_closure", {}).get("calls")
        != trace.get("calls")
        or result.get("receipt_closure", {}).get("status") != 1
        or result.get("gas", {}).get("gas_cost_wei")
        != receipt["gasUsed"] * receipt["effectiveGasPrice"]
        or proof_authority.get("policy_sha256")
        != manifest["policy_sha256"]
        or proof_authority.get("authority_sha256")
        != manifest["authority_sha256"]
        or proof_authority.get("toolchain_sha256")
        != manifest["toolchain_sha256"]
        or proof_authority.get("adapter_proof_sha256") != adapter_sha
        or proof_authority.get("executor_runtime_sha256")
        != config_objects["toolchain"]["executor_build"][
            "deployed_runtime_sha256"
        ]
        or overlay.get("scenario_key") != scenario_key
        or overlay.get("block_number") != selected_block["number"]
        or overlay.get("block_hash") != selected_block["hash"]
        or overlay.get("executor_runtime_sha256")
        != proof_authority.get("executor_runtime_sha256")
    ):
        raise HistoricalRoutePublicationError(
            "historical scenario execution closure differs"
        )
    routes = []
    for buy, sell in (
        ("uniswap_v2", "sushiswap_v2"),
        ("sushiswap_v2", "uniswap_v2"),
    ):
        buy_market = next(
            market for market in markets if market["venue_id"] == buy
        )
        sell_market = next(
            market for market in markets if market["venue_id"] == sell
        )
        route_value = {
            "token_symbol": "UNI",
            "buy_market_id": buy_market["market_id"],
            "sell_market_id": sell_market["market_id"],
            "route_mode": "atomic_onchain",
        }
        route_value["route_id"] = canonical_route_id(route_value)
        routes.append(route_value)
    route = routes[
        0 if selected.get("direction") == "uniswap_to_sushiswap" else 1
    ]
    context_projection = json.loads(record["projection_bytes"])
    context_projection_sha256 = record["projection_sha256"]
    descriptor_set = sorted(read_descriptors, key=lambda row: row["path"])
    source_descriptor_set_sha256 = _sha(_canonical_bytes(descriptor_set))
    scenario_projection = {
        "schema": "historical_foundry_scenario_inputs/v1",
        "scenario_key": scenario_key,
        "context_projection_sha256": context_projection_sha256,
        "source_descriptor_set_sha256": source_descriptor_set_sha256,
        "proof_inputs_hash": proof["proof_inputs_hash"],
        "cohort_id": context_projection["core_pointer"]["route_cohort_id"],
        "route_id": route["route_id"],
        "route": route,
        "selected_block": selected_block,
        "selection_scenario": selected,
        "prefilter_scenario": prefilter_row,
        "fee": selected_capture_chunks["fees"],
        "policy_fees": config_objects["policy"]["fees"],
        "pools": pools,
        "price": feed,
        "feed_sha256_by_venue": feed_sha_by_venue,
        "v2_formula": config_objects["authority"]["v2_formula"],
        "overlay": overlay,
        "receipt": receipt,
        "trace": trace,
        "result": result,
        "proof_inputs": proof,
        **hashes,
    }
    canonical_projection_bytes = _canonical_bytes(scenario_projection)
    try:
        source.reread_unchanged()
    except Exception as error:
        raise HistoricalRoutePublicationError(
            "historical raw source changed during scenario validation"
        ) from error
    return {
        "scenario_key": scenario_key,
        "context_projection_sha256": context_projection_sha256,
        "source_descriptor_set_sha256": source_descriptor_set_sha256,
        "proof_inputs_hash": proof["proof_inputs_hash"],
        "proof": proof,
        "descriptor_set": descriptor_set,
        "canonical_projection_bytes": canonical_projection_bytes,
    }


def load_historical_cost_proof_inputs_for_build_context(
    *, context: HistoricalReplayBuildContext, scenario_key: str,
) -> ValidatedHistoricalCostProofInputs:
    material = _historical_scenario_material(
        context=context, scenario_key=scenario_key, validate_context=True
    )
    return _issue_validated_historical_cost_proof_inputs(material["proof"])


def _issue_validated_historical_scenario_inputs(
    *, context: HistoricalReplayBuildContext, scenario_key: str,
) -> ValidatedHistoricalScenarioInputs:
    material = _historical_scenario_material(
        context=context, scenario_key=scenario_key, validate_context=True
    )
    inputs = _issue_validated_historical_scenario_inputs_from_publication(
        scenario_key=material["scenario_key"],
        context_projection_sha256=material["context_projection_sha256"],
        source_descriptor_set_sha256=material[
            "source_descriptor_set_sha256"
        ],
        proof_inputs_hash=material["proof_inputs_hash"],
        canonical_projection_bytes=material["canonical_projection_bytes"],
    )
    input_id = id(inputs)
    context_reference = weakref.ref(context)
    record = {
        "issuer": _SCENARIO_CONTEXT_ISSUER,
        "context_reference": context_reference,
        "scenario_key": scenario_key,
        "context_projection_sha256": material[
            "context_projection_sha256"
        ],
        "source_descriptor_set_sha256": material[
            "source_descriptor_set_sha256"
        ],
        "proof_inputs_hash": material["proof_inputs_hash"],
        "canonical_projection_bytes": material["canonical_projection_bytes"],
    }

    def retire(reference: weakref.ReferenceType) -> None:
        current = _SCENARIO_CONTEXT_REGISTRY.get(input_id)
        if current is not None and current[0] is reference:
            _SCENARIO_CONTEXT_REGISTRY.pop(input_id, None)

    _SCENARIO_CONTEXT_REGISTRY[input_id] = (
        weakref.ref(inputs, retire), record
    )
    return inputs


def _require_historical_scenario_inputs_current(
    *, context: HistoricalReplayBuildContext,
    inputs: ValidatedHistoricalScenarioInputs,
) -> None:
    entry = _SCENARIO_CONTEXT_REGISTRY.get(id(inputs))
    if (
        type(inputs) is not ValidatedHistoricalScenarioInputs
        or entry is None
        or entry[0]() is not inputs
        or entry[1].get("issuer") is not _SCENARIO_CONTEXT_ISSUER
        or entry[1]["context_reference"]() is not context
    ):
        raise HistoricalRoutePublicationError(
            "historical scenario capability context differs"
        )
    record = entry[1]
    try:
        current = _historical_scenario_material(
            context=context, scenario_key=record["scenario_key"],
            validate_context=True,
        )
    except HistoricalRoutePublicationError:
        raise
    except Exception as error:
        raise HistoricalRoutePublicationError(
            "historical scenario capability is stale"
        ) from error
    for field in (
        "scenario_key", "context_projection_sha256",
        "source_descriptor_set_sha256", "proof_inputs_hash",
        "canonical_projection_bytes",
    ):
        if current[field] != record[field]:
            raise HistoricalRoutePublicationError(
                "historical scenario capability is stale"
            )
    return None


def _validate_historical_cost_rows_for_build_context(
    *, context: HistoricalReplayBuildContext,
    inputs: ValidatedHistoricalScenarioInputs,
    route: Mapping[str, Any], rows: Tuple[Mapping[str, Any], ...],
) -> None:
    from scripts.route_opportunity import route_opportunity_id

    _require_historical_scenario_inputs_current(
        context=context, inputs=inputs
    )
    try:
        projection = json.loads(inputs.canonical_projection_bytes)
        proof_rows = projection["proof_inputs"]["rows"]
        opportunity_id = route_opportunity_id(
            projection["route_id"],
            projection["selection_scenario"]["requested_notional_usd"],
        )
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise HistoricalRoutePublicationError(
            "historical scenario cost projection is invalid"
        ) from error
    expected = {
        (row["grain"], row["component"]): row for row in proof_rows
    }
    _validate_historical_atomic_cost_component_matrix(
        route,
        rows,
        expected_cohort_id=projection["cohort_id"],
        expected_opportunity_id=opportunity_id,
        expected_pool_fee_source_sha256_by_leg={
            leg: expected[(leg, "pool_swap_fee")]["proof_sha256"]
            for leg in ("buy", "sell")
        },
        expected_pool_fee_amount_usd_by_leg={
            leg: expected[(leg, "pool_swap_fee")]["amount_usd_exact"]
            for leg in ("buy", "sell")
        },
        expected_zero_fee_proof_sha256_by_key={
            key: expected[key]["proof_sha256"]
            for key in (
                ("buy", "router_or_integrator_fee"),
                ("buy", "token_transfer_tax"),
                ("sell", "router_or_integrator_fee"),
                ("sell", "token_transfer_tax"),
            )
        },
        expected_gas_amount_usd=expected[
            ("route", "network_gas")
        ]["amount_usd_exact"],
        expected_gas_source_sha256=expected[
            ("route", "network_gas")
        ]["proof_sha256"],
        expected_transfer_source_sha256=expected[
            ("route", "rebalancing_or_transfer")
        ]["proof_sha256"],
        expected_mev_amount_usd=expected[
            ("route", "mev_buffer")
        ]["amount_usd_exact"],
        expected_policy_sha256=expected[
            ("route", "mev_buffer")
        ]["proof_sha256"],
    )
    return None


def _build_historical_scenario_for_publication(
    *, context: HistoricalReplayBuildContext, scenario_key: str,
) -> Mapping[str, Any]:
    inputs = _issue_validated_historical_scenario_inputs(
        context=context, scenario_key=scenario_key
    )
    _require_historical_scenario_inputs_current(
        context=context, inputs=inputs
    )
    canonical_projection_bytes = build_historical_scenario_projection(inputs)
    _require_historical_scenario_inputs_current(
        context=context, inputs=inputs
    )
    try:
        projection = json.loads(canonical_projection_bytes)
        rows = tuple(projection["cost_components"])
        opportunity = projection["opportunity"]
        raw_inputs = json.loads(inputs.canonical_projection_bytes)
    except (KeyError, TypeError, json.JSONDecodeError) as error:
        raise HistoricalRoutePublicationError(
            "historical scenario serialization is invalid"
        ) from error
    _validate_historical_cost_rows_for_build_context(
        context=context, inputs=inputs, route=raw_inputs["route"],
        rows=rows,
    )
    return MappingProxyType({
        "schema": "historical_scenario_publication_build/v1",
        "scenario_key": scenario_key,
        "proof_inputs_hash": inputs.proof_inputs_hash,
        "canonical_projection_bytes": canonical_projection_bytes,
        "opportunity": MappingProxyType(opportunity),
        "cost_components": tuple(MappingProxyType(row) for row in rows),
    })
