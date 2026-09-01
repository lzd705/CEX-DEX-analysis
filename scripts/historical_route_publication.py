"""Isolated publication boundary for the historical replay private core.

The historical entry points deliberately have no live-root defaults and accept
neither caller-built core projections nor caller-selected raw readers.
"""

from datetime import datetime, timezone
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType
from typing import Any, Dict, Mapping, NoReturn, Optional, Tuple
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
import scripts.historical_foundry_replay as _historical_replay
from scripts.historical_foundry_replay import (
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
_MAX_SCENARIO_TRACE_BYTES = 16_777_216
_MAX_GZIP_MEMBER_BYTES = 16_842_752

_HISTORICAL_COMPLETE_STAGE = "route_historical_foundry_replay/v1"
_HISTORICAL_COMPLETE_MANIFEST_SCHEMA = (
    "route_historical_replay_manifest/v1"
)
_HISTORICAL_REPLAY_EVIDENCE_SCHEMA = (
    "historical_foundry_replay_evidence/v1"
)
_HISTORICAL_REPLAY_EVIDENCE_FILENAME = "replay_evidence.json"
_HISTORICAL_COMPLETE_FILES = frozenset((
    "manifest.json", "route_legs.csv", "cost_components.csv",
    "route_opportunities.csv", "route_cohort.sqlite3",
    _HISTORICAL_REPLAY_EVIDENCE_FILENAME,
))
_HISTORICAL_COMPLETE_ARTIFACT_FILES = frozenset(
    _HISTORICAL_COMPLETE_FILES - {"manifest.json"}
)

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

    def issue(proof: Mapping[str, Any], owner: object) -> Any:
        try:
            owner_reference = weakref.ref(owner)
        except TypeError as error:
            raise HistoricalRoutePublicationError(
                "validated historical cost proof owner is invalid"
            ) from error
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
        record = {
            "issuer": issuer, "owner_reference": owner_reference, **fields,
        }

        def retire(reference: weakref.ReferenceType) -> None:
            current = registry.get(value_id)
            if current is not None and current[0] is reference:
                registry.pop(value_id, None)

        registry[value_id] = (weakref.ref(value, retire), record)
        return value

    def require_for_owner(value: Any, owner: object) -> Mapping[str, Any]:
        proof = require(value)
        entry = registry.get(id(value))
        if entry is None or entry[1]["owner_reference"]() is not owner:
            raise HistoricalRoutePublicationError(
                "validated historical cost proof ancestry differs"
            )
        return proof

    published_installed = [False]

    def bind_published_loader(material_reader: Any) -> Any:
        if (
            published_installed[0]
            or material_reader is not globals().get(
                "_historical_published_cost_proof_material"
            )
        ):
            raise HistoricalRoutePublicationError(
                "historical published proof loader installer is invalid"
            )

        def load(
            *, validated_view: Any, scenario_key: str,
        ) -> Any:
            material, expected_hash = material_reader(
                validated_view=validated_view,
                scenario_key=scenario_key,
            )
            proof = issue(material["proof"], validated_view)
            if proof.proof_inputs_hash != expected_hash:
                raise HistoricalRoutePublicationError(
                    "historical published proof hash differs"
                )
            require_for_owner(proof, validated_view)
            return proof

        published_installed[0] = True
        globals().pop(
            "_bind_historical_published_cost_proof_loader", None
        )
        return load

    installed = [False]

    def bind_loader(material_reader: Any) -> Any:
        if (
            installed[0]
            or material_reader is not globals().get(
                "_historical_scenario_material"
            )
        ):
            raise HistoricalRoutePublicationError(
                "historical cost proof loader installer is invalid"
            )

        def load(
            *, context: "HistoricalReplayBuildContext", scenario_key: str,
        ) -> Any:
            material = material_reader(
                context=context, scenario_key=scenario_key,
                validate_context=True,
            )
            return issue(material["proof"], context)

        installed[0] = True
        globals().pop("_bind_historical_cost_proof_loader", None)
        return load

    return (
        ValidatedHistoricalCostProofInputs, bind_loader, require,
        require_for_owner, bind_published_loader,
    )


(
    ValidatedHistoricalCostProofInputs,
    _bind_historical_cost_proof_loader,
    _validated_historical_cost_proof_object,
    _require_historical_cost_proof_owner,
    _bind_historical_published_cost_proof_loader,
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
    path_parts = path.split("/") if type(path) is str else []
    is_scenario_trace = (
        len(path_parts) == 4
        and path_parts[0] == "foundry"
        and path_parts[3] == "trace.json.gz"
    )
    if is_scenario_trace:
        maximum_size = _MAX_SCENARIO_TRACE_BYTES
    elif type(path) is str and path.endswith(".json.gz"):
        maximum_size = _MAX_GZIP_MEMBER_BYTES
    else:
        maximum_size = _MAX_MEMBER_BYTES
    if (
        type(descriptor) is not dict
        or set(descriptor) != {"path", "byte_count", "sha256"}
        or descriptor.get("path") != path
        or type(descriptor.get("byte_count")) is not int
        or not 0 < descriptor["byte_count"] <= maximum_size
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


def _historical_core_member_snapshots_at(
    bundle_fd: int,
) -> Mapping[str, os.stat_result]:
    try:
        if set(os.listdir(bundle_fd)) != _CORE_FILES:
            raise HistoricalRoutePublicationError(
                "historical immutable core file inventory differs"
            )
    except OSError as error:
        raise HistoricalRoutePublicationError(
            "historical immutable core file inventory differs"
        ) from error
    snapshots = {}
    for name in sorted(_CORE_FILES):
        member_fd = None
        try:
            member_fd, before = _route_publication._open_regular_file_at(
                bundle_fd, name,
                label="historical immutable core {}".format(name),
            )
            opened = os.fstat(member_fd)
            current = os.stat(
                name, dir_fd=bundle_fd, follow_symlinks=False
            )
            if (
                _route_publication._stable_file_metadata(opened)
                != _route_publication._stable_file_metadata(before)
                or _route_publication._stable_file_metadata(current)
                != _route_publication._stable_file_metadata(before)
            ):
                raise HistoricalRoutePublicationError(
                    "historical immutable core member changed"
                )
            snapshots[name] = opened
        except _route_publication.RoutePublicationError as error:
            raise HistoricalRoutePublicationError(
                "historical immutable core member identity differs"
            ) from error
        except OSError as error:
            raise HistoricalRoutePublicationError(
                "historical immutable core member identity differs"
            ) from error
        finally:
            if member_fd is not None:
                os.close(member_fd)
    return MappingProxyType(snapshots)


def _require_historical_core_member_snapshots_at(
    bundle_fd: int,
    expected: Mapping[str, os.stat_result],
) -> None:
    current = _historical_core_member_snapshots_at(bundle_fd)
    if set(current) != _CORE_FILES or any(
        _route_publication._stable_file_metadata(current[name])
        != _route_publication._stable_file_metadata(expected[name])
        for name in _CORE_FILES
    ):
        raise HistoricalRoutePublicationError(
            "historical immutable core member snapshot differs"
        )
    return None


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
    published_record: Any = None, immutable_record: Any = None,
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
        "immutable_record": immutable_record,
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
        _close_descriptors_robustly(bundle_fd, bundles_fd, core_fd)
    return None


def _validate_immutable_context(record: Mapping[str, Any]) -> None:
    """Reread one pinned historical core without consulting core/latest."""
    held = record["immutable_record"]
    core_root, core_fd, current_core = (
        _route_publication._open_verified_directory(
            held["core_root"], "historical immutable core root"
        )
    )
    bundles_fd = bundle_fd = None
    try:
        if not _route_publication._same_inode(
            current_core, held["core_details"]
        ):
            raise HistoricalRoutePublicationError(
                "historical immutable core root identity differs"
            )
        bundles_fd, current_bundles = _route_publication._open_directory_at(
            core_fd, "bundles", "historical immutable core bundles"
        )
        if not _route_publication._same_inode(
            current_bundles, held["bundles_details"]
        ):
            raise HistoricalRoutePublicationError(
                "historical immutable core bundles identity differs"
            )
        bundle_fd, current_bundle = _route_publication._open_directory_at(
            bundles_fd, held["bundle_name"],
            "historical immutable core bundle",
        )
        if (
            _route_publication._stable_file_metadata(current_bundle)
            != _route_publication._stable_file_metadata(
                held["bundle_details"]
            )
        ):
            raise HistoricalRoutePublicationError(
                "historical immutable core bundle changed"
            )
        _require_historical_core_member_snapshots_at(
            bundle_fd, held["member_details"]
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
                "historical immutable raw source changed"
            )
        expected_pointer = _pointer(
            held["manifest"], held["manifest_sha256"]
        )
        if (
            _sha(_json_file_bytes(expected_pointer))
            != held["pointer_sha256"]
        ):
            raise HistoricalRoutePublicationError(
                "historical immutable core pointer binding differs"
            )
        _validate_bundle(
            bundle=held["bundle_path"],
            expected_derived=held["derived"],
            expected_manifest=held["manifest"],
            expected_manifest_sha256=held["manifest_sha256"],
            bundle_fd=bundle_fd,
            expected_bundle_details=held["bundle_details"],
        )
        _require_historical_core_member_snapshots_at(
            bundle_fd, held["member_details"]
        )
        _route_publication._verify_open_path_identity(
            core_root, held["core_details"],
            "historical immutable core root",
        )
        _route_publication._verify_directory_entry(
            core_fd, "bundles", held["bundles_details"],
            "historical immutable core bundles",
        )
        if (
            _route_publication._stable_file_metadata(os.fstat(bundle_fd))
            != _route_publication._stable_file_metadata(
                held["bundle_details"]
            )
        ):
            raise HistoricalRoutePublicationError(
                "historical immutable core bundle changed during validation"
            )
        _route_publication._verify_directory_entry_snapshot(
            bundles_fd, held["bundle_name"], held["bundle_details"],
            "historical immutable core bundle",
        )
    except _route_publication.RoutePublicationError as error:
        raise HistoricalRoutePublicationError(
            "historical immutable core held identity differs"
        ) from error
    finally:
        _close_descriptors_robustly(bundle_fd, bundles_fd, core_fd)
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
    elif record.get("immutable_record") is not None:
        _validate_immutable_context(record)
        held = record["immutable_record"]
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
        _close_descriptors_robustly(bundle_fd, bundles_fd, core_fd)


def _load_immutable_historical_replay_core(
    *, data_dir: Path, route_cohort_id: str,
    expected_manifest_sha256: str, expected_pointer_sha256: str,
) -> HistoricalReplayBuildContext:
    """Load one manifest-pinned core without reading the mutable pointer."""
    if (
        not isinstance(data_dir, Path)
        or type(route_cohort_id) is not str
        or re.fullmatch(r"cohort:[0-9a-f]{64}", route_cohort_id) is None
        or type(expected_manifest_sha256) is not str
        or re.fullmatch(
            r"[0-9a-f]{64}", expected_manifest_sha256
        ) is None
        or type(expected_pointer_sha256) is not str
        or re.fullmatch(r"[0-9a-f]{64}", expected_pointer_sha256) is None
    ):
        raise HistoricalRoutePublicationError(
            "historical immutable core input is invalid"
        )
    core_root, core_fd, core_details = (
        _route_publication._open_verified_directory(
            data_dir / "routes" / "historical" / "core",
            "historical immutable core root",
        )
    )
    source = None
    bundles_fd = bundle_fd = None
    try:
        bundles_fd, bundles_details = _route_publication._open_directory_at(
            core_fd, "bundles", "historical immutable core bundles"
        )
        bundle_fd, bundle_details = _route_publication._open_directory_at(
            bundles_fd, route_cohort_id,
            "historical immutable core bundle",
        )
        bundle_path = core_root / "bundles" / route_cohort_id
        manifest_bytes, manifest_sha256, _manifest_details = (
            _route_publication._read_bounded_bytes_at(
                bundle_fd, "manifest.json",
                limit=_route_publication._MAX_JSON_BYTES,
                label="historical immutable core manifest",
            )
        )
        if manifest_sha256 != expected_manifest_sha256:
            raise HistoricalRoutePublicationError(
                "historical immutable core manifest hash differs"
            )
        try:
            manifest = json.loads(manifest_bytes)
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise HistoricalRoutePublicationError(
                "historical immutable core manifest is invalid"
            ) from error
        if (
            type(manifest) is not dict
            or manifest_bytes != _json_file_bytes(manifest)
            or manifest.get("schema") != _MANIFEST_SCHEMA
            or manifest.get("bundle_stage") != _BUNDLE_STAGE
            or manifest.get("route_cohort_id") != route_cohort_id
            or re.fullmatch(
                r"run:[0-9a-f]{64}",
                manifest.get("raw_evidence_run_id", ""),
            ) is None
            or re.fullmatch(
                r"[0-9a-f]{64}",
                manifest.get("raw_run_manifest_sha256", ""),
            ) is None
        ):
            raise HistoricalRoutePublicationError(
                "historical immutable core manifest schema differs"
            )
        config = load_historical_foundry_config_set()
        if any(
            manifest.get("{}_sha256".format(role))
            != getattr(config, role).physical_sha256
            for role in ("policy", "authority", "toolchain")
        ):
            raise HistoricalRoutePublicationError(
                "historical immutable core config binding differs"
            )
        source = _historical_storage.open_validated_run(
            data_dir=data_dir,
            run_id=manifest["raw_evidence_run_id"],
            expected_manifest_sha256=manifest[
                "raw_run_manifest_sha256"
            ],
        )
        derived = _derive_historical_core(config=config, source=source)
        artifacts, expected_manifest, rebuilt_manifest_sha256 = (
            _build_artifacts(config=config, derived=derived)
        )
        del artifacts
        if (
            rebuilt_manifest_sha256 != expected_manifest_sha256
            or manifest != expected_manifest
            or manifest_bytes != _json_file_bytes(expected_manifest)
        ):
            raise HistoricalRoutePublicationError(
                "historical immutable core manifest content differs"
            )
        expected_pointer = _pointer(
            expected_manifest, rebuilt_manifest_sha256
        )
        if (
            _sha(_json_file_bytes(expected_pointer))
            != expected_pointer_sha256
        ):
            raise HistoricalRoutePublicationError(
                "historical immutable core pointer hash differs"
            )
        _validate_bundle(
            bundle=bundle_path,
            expected_derived=derived,
            expected_manifest=expected_manifest,
            expected_manifest_sha256=rebuilt_manifest_sha256,
            bundle_fd=bundle_fd,
            expected_bundle_details=bundle_details,
        )
        member_details = _historical_core_member_snapshots_at(bundle_fd)
        _route_publication._verify_open_path_identity(
            core_root, core_details, "historical immutable core root"
        )
        _route_publication._verify_directory_entry(
            core_fd, "bundles", bundles_details,
            "historical immutable core bundles",
        )
        _route_publication._verify_directory_entry_snapshot(
            bundles_fd, route_cohort_id, bundle_details,
            "historical immutable core bundle",
        )
        projection = _context_projection(
            manifest=expected_manifest,
            manifest_sha256=rebuilt_manifest_sha256,
            pointer=expected_pointer,
        )
        immutable_record = {
            "config": config,
            "core_root": core_root,
            "core_details": core_details,
            "bundles_details": bundles_details,
            "bundle_name": route_cohort_id,
            "bundle_path": bundle_path,
            "bundle_details": bundle_details,
            "member_details": member_details,
            "derived": derived,
            "manifest": expected_manifest,
            "manifest_sha256": rebuilt_manifest_sha256,
            "pointer_sha256": expected_pointer_sha256,
        }
        context = _issue_context(
            source=source,
            projection=projection,
            owns_source=True,
            immutable_record=immutable_record,
        )
        context.reread_unchanged()
        source = None
        return context
    except HistoricalRoutePublicationError:
        raise
    except _route_publication.RoutePublicationError as error:
        raise HistoricalRoutePublicationError(
            "historical immutable core loading failed"
        ) from error
    except Exception as error:
        raise HistoricalRoutePublicationError(
            "historical immutable core loading failed"
        ) from error
    finally:
        try:
            if source is not None:
                try:
                    source.close()
                except Exception:
                    pass
        finally:
            _close_descriptors_robustly(
                bundle_fd, bundles_fd, core_fd
            )


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


def _historical_swap_calldata_sha256(
    *, amount_in_raw: int, path: list, recipient: str, deadline: int,
) -> str:
    def word(value: int) -> bytes:
        if type(value) is not int or not 0 <= value < 2 ** 256:
            raise HistoricalRoutePublicationError(
                "historical successful router call is invalid"
            )
        return value.to_bytes(32, "big")

    try:
        raw = bytes.fromhex("38ed1739") + b"".join((
            word(amount_in_raw), word(0), word(160),
            b"\0" * 12 + bytes.fromhex(recipient[2:]),
            word(deadline), word(2),
            b"\0" * 12 + bytes.fromhex(path[0][2:]),
            b"\0" * 12 + bytes.fromhex(path[1][2:]),
        ))
    except (TypeError, ValueError):
        raise HistoricalRoutePublicationError(
            "historical successful router call is invalid"
        ) from None
    return _sha(raw)


def _validate_historical_retained_execution(
    *, overlay: Mapping[str, Any], receipt: Mapping[str, Any],
    trace: Mapping[str, Any], result: Mapping[str, Any],
    prefilter: Mapping[str, Any], fee: Mapping[str, Any],
    authority: Mapping[str, Any],
) -> None:
    transaction = overlay.get("transaction")
    synthetic = overlay.get("synthetic_block")
    if type(transaction) is not dict or type(synthetic) is not dict:
        raise HistoricalRoutePublicationError(
            "historical transaction envelope is invalid"
        )
    calldata = transaction.get("input")
    try:
        raw = bytes.fromhex(calldata[2:])
    except (AttributeError, ValueError):
        raise HistoricalRoutePublicationError(
            "historical transaction calldata is invalid"
        ) from None
    direction_index = {
        "uniswap_to_sushiswap": 0,
        "sushiswap_to_uniswap": 1,
    }.get(prefilter.get("direction"))
    if (
        direction_index is None
        or len(raw) != 68
        or raw[:4] != bytes.fromhex("64cc5eae")
        or int.from_bytes(raw[4:36], "big") != direction_index
        or int.from_bytes(raw[36:68], "big")
        != prefilter.get("amount_weth_in_wei")
        or transaction.get("calldata_sha256") != _sha(raw)
    ):
        raise HistoricalRoutePublicationError(
            "historical transaction calldata differs"
        )
    expected_effective = (
        fee.get("next_base_fee_per_gas", -1)
        + fee.get("p50_priority_fee_per_gas", -1)
    )
    if (
        receipt.get("effectiveGasPrice") != expected_effective
        or receipt.get("maxPriorityFeePerGas")
        != fee.get("p50_priority_fee_per_gas")
        or transaction.get("maxPriorityFeePerGas")
        != fee.get("p50_priority_fee_per_gas")
        or receipt.get("maxFeePerGas") != transaction.get("maxFeePerGas")
    ):
        raise HistoricalRoutePublicationError(
            "historical p50 gas envelope differs"
        )
    tokens = {
        row.get("role"): row.get("address")
        for row in authority.get("tokens", ())
        if type(row) is dict
    }
    routers = {
        row.get("venue_id"): row.get("router_address")
        for row in authority.get("venues", ())
        if type(row) is dict
    }
    if (
        set(tokens) != {"uni", "weth"}
        or set(routers) != {"uniswap_v2", "sushiswap_v2"}
    ):
        raise HistoricalRoutePublicationError(
            "historical router authority differs"
        )
    buy, sell = (
        ("uniswap_v2", "sushiswap_v2")
        if direction_index == 0
        else ("sushiswap_v2", "uniswap_v2")
    )
    executor = transaction.get("to")
    deadline = synthetic.get("timestamp", -1) + 60
    call_specs = (
        (
            [2], "first_leg", routers[buy],
            prefilter.get("amount_weth_in_wei"),
            [tokens["weth"], tokens["uni"]],
        ),
        (
            [5], "second_leg", routers[sell],
            prefilter.get("first_amount_out_raw"),
            [tokens["uni"], tokens["weth"]],
        ),
    )
    expected_calls = []
    for call_path, leg, router, amount, path in call_specs:
        expected_calls.append({
            "call_path": call_path,
            "leg": leg,
            "router": router,
            "calldata_sha256": _historical_swap_calldata_sha256(
                amount_in_raw=amount, path=path,
                recipient=executor, deadline=deadline,
            ),
            "amount_in_raw": amount,
            "amount_out_min_raw": 0,
            "path": path,
            "recipient": executor,
            "deadline": deadline,
            "value": 0,
        })
    if (
        trace.get("successful_calls") != expected_calls
        or result.get("trace_closure", {}).get("successful_calls")
        != expected_calls
    ):
        raise HistoricalRoutePublicationError(
            "historical successful router calls differ"
        )
    baseline = result.get("pair_closure")
    if type(baseline) is not dict:
        raise HistoricalRoutePublicationError(
            "historical pair transition is invalid"
        )
    expected_post = {
        venue: dict(value)
        for venue, value in baseline.items()
        if type(value) is dict
    }
    if set(expected_post) != {"uniswap_v2", "sushiswap_v2"}:
        raise HistoricalRoutePublicationError(
            "historical pair transition is invalid"
        )
    amount = prefilter.get("amount_weth_in_wei")
    first = prefilter.get("first_amount_out_raw")
    second = prefilter.get("second_amount_out_raw")
    if any(type(value) is not int or value <= 0 for value in (
        amount, first, second,
    )):
        raise HistoricalRoutePublicationError(
            "historical pair transition is invalid"
        )
    expected_post[buy]["reserve_uni_raw"] -= first
    expected_post[buy]["pair_uni_balance_raw"] -= first
    expected_post[buy]["reserve_weth_raw"] += amount
    expected_post[buy]["pair_weth_balance_raw"] += amount
    expected_post[sell]["reserve_uni_raw"] += first
    expected_post[sell]["pair_uni_balance_raw"] += first
    expected_post[sell]["reserve_weth_raw"] -= second
    expected_post[sell]["pair_weth_balance_raw"] -= second
    if (
        trace.get("post_pair_state") != expected_post
        or result.get("post_pair_state") != expected_post
    ):
        raise HistoricalRoutePublicationError(
            "historical pair transition differs"
        )


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
    _validate_historical_retained_execution(
        overlay=overlay, receipt=receipt, trace=trace, result=result,
        prefilter=prefilter_row, fee=selected_capture_chunks["fees"],
        authority=config_objects["authority"],
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
        "core_manifest_sha256": context_projection[
            "core_manifest_sha256"
        ],
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


load_historical_cost_proof_inputs_for_build_context = (
    _bind_historical_cost_proof_loader(_historical_scenario_material)
)


def _install_historical_scenario_capability(
    *, capability_type: Any, raw_mint: Any, require_projection: Any,
) -> None:
    if (
        capability_type is not _historical_replay.ValidatedHistoricalScenarioInputs
        or not callable(raw_mint)
        or require_projection is not (
            _historical_replay._validated_historical_scenario_projection
        )
        or "_issue_validated_historical_scenario_inputs" in globals()
    ):
        raise HistoricalRoutePublicationError(
            "historical scenario capability installer is invalid"
        )
    material_reader = _historical_scenario_material
    context_issuer = object()
    context_registry = {}

    def issue(
        *, context: HistoricalReplayBuildContext, scenario_key: str,
    ) -> Any:
        try:
            material = material_reader(
                context=context, scenario_key=scenario_key,
                validate_context=True,
            )
        except HistoricalRoutePublicationError:
            raise
        except Exception as error:
            raise HistoricalRoutePublicationError(
                "historical scenario source validation failed"
            ) from error
        inputs = raw_mint(
            scenario_key=material["scenario_key"],
            context_projection_sha256=material[
                "context_projection_sha256"
            ],
            source_descriptor_set_sha256=material[
                "source_descriptor_set_sha256"
            ],
            proof_inputs_hash=material["proof_inputs_hash"],
            canonical_projection_bytes=material["canonical_projection_bytes"],
        )
        input_id = id(inputs)
        fields = {
            "context_reference": weakref.ref(context),
            "scenario_key": scenario_key,
            "context_projection_sha256": material[
                "context_projection_sha256"
            ],
            "source_descriptor_set_sha256": material[
                "source_descriptor_set_sha256"
            ],
            "proof_inputs_hash": material["proof_inputs_hash"],
            "canonical_projection_bytes": material[
                "canonical_projection_bytes"
            ],
        }
        record = {"issuer": context_issuer, **fields}

        def retire(reference: weakref.ReferenceType) -> None:
            current = context_registry.get(input_id)
            if current is not None and current[0] is reference:
                context_registry.pop(input_id, None)

        context_registry[input_id] = (
            weakref.ref(inputs, retire), record
        )
        return inputs

    def require_current(
        *, context: HistoricalReplayBuildContext, inputs: Any,
    ) -> None:
        try:
            require_projection(inputs)
        except (TypeError, ValueError) as error:
            raise HistoricalRoutePublicationError(
                "historical scenario capability context differs"
            ) from error
        entry = context_registry.get(id(inputs))
        if (
            type(inputs) is not capability_type
            or entry is None
            or entry[0]() is not inputs
            or entry[1].get("issuer") is not context_issuer
            or entry[1]["context_reference"]() is not context
        ):
            raise HistoricalRoutePublicationError(
                "historical scenario capability context differs"
            )
        record = entry[1]
        try:
            current = material_reader(
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

    globals()["ValidatedHistoricalScenarioInputs"] = capability_type
    globals()["_issue_validated_historical_scenario_inputs"] = issue
    globals()["_require_historical_scenario_inputs_current"] = require_current


_historical_replay._bind_historical_scenario_capability_to_publication(
    _install_historical_scenario_capability
)
del _install_historical_scenario_capability


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
    material = _historical_scenario_material(
        context=context, scenario_key=scenario_key, validate_context=True,
    )
    if (
        material["canonical_projection_bytes"]
        != inputs.canonical_projection_bytes
        or material["proof_inputs_hash"] != inputs.proof_inputs_hash
        or material["source_descriptor_set_sha256"]
        != inputs.source_descriptor_set_sha256
    ):
        raise HistoricalRoutePublicationError(
            "historical scenario source changed before serialization"
        )
    return MappingProxyType({
        "schema": "historical_scenario_publication_build/v1",
        "scenario_key": scenario_key,
        "proof_inputs_hash": inputs.proof_inputs_hash,
        "canonical_projection_bytes": canonical_projection_bytes,
        "canonical_input_bytes": inputs.canonical_projection_bytes,
        "canonical_source_descriptor_bytes": _canonical_bytes(
            material["descriptor_set"]
        ),
        "opportunity": MappingProxyType(opportunity),
        "cost_components": tuple(MappingProxyType(row) for row in rows),
    })


def _initialize_validated_historical_replay_bundle_view():
    installed = [False]

    def bind_runtime(
        *, validation_impl: Any, current_checker_function: Any,
        scenario_material_reader: Any,
    ) -> Tuple[Any, Any, Any]:
        if (
            installed[0]
            or validation_impl is not globals().get(
                "_validate_historical_replay_bundle_impl"
            )
            or current_checker_function is not globals().get(
                "_validate_historical_replay_bundle_view_current"
            )
            or scenario_material_reader is not globals().get(
                "_historical_scenario_material"
            )
        ):
            raise HistoricalRoutePublicationError(
                "validated historical replay bundle installer is invalid"
            )

        record_fields = frozenset((
            "replay_id", "route_cohort_id", "manifest_sha256",
            "data_dir", "raw_root", "bundle_path", "parent_details",
            "bundle_details", "read_specs", "file_bytes", "file_hashes",
            "file_details", "manifest", "bundle", "replay_evidence",
            "immutable_context",
        ))

        def close_context_silently(context: object) -> None:
            try:
                context.close()
            except Exception:
                pass

        def require_record(
            value: object, *, validate_current: bool = True,
        ) -> Mapping[str, Any]:
            if type(value) is not ValidatedHistoricalReplayBundleView:
                raise HistoricalRoutePublicationError(
                    "validated historical replay bundle view is invalid"
                )
            try:
                owner = object.__getattribute__(
                    value, "_validated_record"
                )
                finalizer = object.__getattribute__(
                    value, "_context_finalizer"
                )
            except AttributeError as error:
                raise HistoricalRoutePublicationError(
                    "validated historical replay bundle view is invalid"
                ) from error
            if (
                type(owner) is not MappingProxyType
                or set(owner) != {"owner_reference", "payload"}
                or type(owner.get("owner_reference"))
                is not weakref.ReferenceType
                or owner["owner_reference"]() is not value
                or type(owner.get("payload")) is not MappingProxyType
                or type(finalizer) is not weakref.finalize
                or not finalizer.alive
            ):
                raise HistoricalRoutePublicationError(
                    "validated historical replay bundle view is invalid"
                )
            record = owner["payload"]
            if set(record) != record_fields:
                raise HistoricalRoutePublicationError(
                    "validated historical replay bundle view is invalid"
                )
            finalizer_state = finalizer.peek()
            if (
                finalizer_state is None
                or finalizer_state[0] is not value
                or finalizer_state[1] is not close_context_silently
                or finalizer_state[2]
                != (record["immutable_context"],)
                or finalizer_state[3] != {}
            ):
                raise HistoricalRoutePublicationError(
                    "validated historical replay bundle view is invalid"
                )
            for field in (
                "replay_id", "route_cohort_id", "manifest_sha256"
            ):
                try:
                    current = object.__getattribute__(value, field)
                except AttributeError as error:
                    raise HistoricalRoutePublicationError(
                        "validated historical replay bundle view is invalid"
                    ) from error
                if current != record[field]:
                    raise HistoricalRoutePublicationError(
                        "validated historical replay bundle view differs"
                    )
            if validate_current:
                current_checker_function(record)
            return record

        class ValidatedHistoricalReplayBundleView:
            """Identity-only handle for a fully validated immutable bundle."""

            __slots__ = (
                "replay_id", "route_cohort_id", "manifest_sha256",
                "_validated_record", "_context_finalizer", "__weakref__",
            )

            def __new__(cls, *args: Any, **kwargs: Any) -> NoReturn:
                del cls, args, kwargs
                raise HistoricalRoutePublicationError(
                    "validated historical replay bundle construction is private"
                )

            def __setattr__(self, name: str, value: Any) -> NoReturn:
                del name, value
                raise HistoricalRoutePublicationError(
                    "validated historical replay bundle view is immutable"
                )

            def __delattr__(self, name: str) -> NoReturn:
                del name
                raise HistoricalRoutePublicationError(
                    "validated historical replay bundle view is immutable"
                )

            def __repr__(self) -> str:
                return "ValidatedHistoricalReplayBundleView(<redacted>)"

            def __reduce_ex__(self, protocol: int) -> NoReturn:
                del protocol
                raise TypeError(
                    "validated historical replay bundle is not serializable"
                )

            def reread_unchanged(self) -> None:
                require_record(self, validate_current=True)

            def close(self) -> None:
                record = require_record(self, validate_current=False)
                finalizer = object.__getattribute__(
                    self, "_context_finalizer"
                )
                record["immutable_context"].close()
                finalizer.detach()
                object.__setattr__(self, "_validated_record", None)
                object.__setattr__(self, "_context_finalizer", None)

            def __enter__(self) -> "ValidatedHistoricalReplayBundleView":
                require_record(self, validate_current=True)
                return self

            def __exit__(
                self, error_type: Any, error: Any, traceback: Any,
            ) -> None:
                del error_type, traceback
                try:
                    return self.close()
                except BaseException as cleanup_error:
                    if error is not None and not isinstance(error, Exception):
                        raise error
                    raise cleanup_error

        def issue(
            record: Dict[str, Any],
        ) -> ValidatedHistoricalReplayBundleView:
            if type(record) is not dict or set(record) != record_fields:
                raise HistoricalRoutePublicationError(
                    "validated historical replay bundle record is invalid"
                )
            frozen = _freeze(record)
            current_checker_function(frozen)
            value = object.__new__(ValidatedHistoricalReplayBundleView)
            owner = MappingProxyType({
                "owner_reference": weakref.ref(value),
                "payload": frozen,
            })
            finalizer = weakref.finalize(
                value, close_context_silently,
                frozen["immutable_context"],
            )
            try:
                for field in (
                    "replay_id", "route_cohort_id", "manifest_sha256"
                ):
                    object.__setattr__(value, field, frozen[field])
                object.__setattr__(value, "_validated_record", owner)
                object.__setattr__(value, "_context_finalizer", finalizer)
                require_record(value, validate_current=False)
            except BaseException:
                finalizer.detach()
                raise
            return value

        def validate(
            *, data_dir: Path, raw_root: Path, bundle_path: Path,
            expected_pointer_core: Optional[Mapping[str, Any]],
            expected_replay_id: Optional[str],
            require_directory_identity: bool, issue_view: bool,
        ) -> Mapping[str, Any]:
            return validation_impl(
                data_dir=data_dir, raw_root=raw_root,
                bundle_path=bundle_path,
                expected_pointer_core=expected_pointer_core,
                expected_replay_id=expected_replay_id,
                require_directory_identity=require_directory_identity,
                issue_view=issue_view, view_issuer=issue,
            )

        def published_material(
            *, validated_view: ValidatedHistoricalReplayBundleView,
            scenario_key: str,
        ) -> Tuple[Mapping[str, Any], str]:
            record = require_record(
                validated_view, validate_current=True
            )
            scenarios = [
                row for row in record["replay_evidence"]["scenarios"]
                if row["scenario_key"] == scenario_key
            ]
            if len(scenarios) != 1:
                raise HistoricalRoutePublicationError(
                    "historical published scenario is invalid"
                )
            material = scenario_material_reader(
                context=record["immutable_context"],
                scenario_key=scenario_key, validate_context=True,
            )
            scenario = scenarios[0]
            expected_sources = {
                item["path"]: {
                    "path": item["path"],
                    "byte_count": item["byte_count"],
                    "sha256": item["sha256"],
                }
                for item in scenario["source_members"]
            }
            material_sources = {
                item["path"]: item for item in material["descriptor_set"]
                if item["path"] in expected_sources
            }
            if (
                material["proof_inputs_hash"]
                != scenario["proof_inputs_hash"]
                or material_sources != expected_sources
            ):
                raise HistoricalRoutePublicationError(
                    "historical published proof source differs"
                )
            return material, scenario["proof_inputs_hash"]

        def subject_material(
            *, validated_view: ValidatedHistoricalReplayBundleView,
        ) -> Dict[str, Any]:
            record = require_record(
                validated_view, validate_current=True
            )
            manifest = _plain(record["manifest"])
            pointer_core = {
                "schema": "route_historical_replay_pointer/v1",
                "bundle_stage": _HISTORICAL_COMPLETE_STAGE,
                "replay_id": manifest["replay_id"],
                "route_cohort_id": manifest["route_cohort_id"],
                "manifest_sha256": record["manifest_sha256"],
            }
            _validate_historical_expected_pointer_core(
                expected=pointer_core, manifest=manifest,
                manifest_sha256=record["manifest_sha256"],
            )
            return {
                "validated_view": validated_view,
                "data_dir": record["data_dir"],
                "raw_root": record["raw_root"],
                "bundle_path": record["bundle_path"],
                "manifest": _plain(record["manifest"]),
                "bundle": _plain(record["bundle"]),
                "replay_evidence": _plain(record["replay_evidence"]),
                "pointer_core": pointer_core,
            }

        installed[0] = True
        return (
            ValidatedHistoricalReplayBundleView, validate,
            published_material, subject_material,
        )

    return bind_runtime


_bind_historical_replay_bundle_view_runtime = (
    _initialize_validated_historical_replay_bundle_view()
)
del _initialize_validated_historical_replay_bundle_view


def _historical_complete_roots(
    *, data_dir: Path, raw_root: Path,
) -> Tuple[Path, Path]:
    if not isinstance(data_dir, Path) or not isinstance(raw_root, Path):
        raise HistoricalRoutePublicationError(
            "historical complete bundle input is invalid"
        )
    data = _route_publication._absolute_without_symlink_resolution(data_dir)
    raw = _route_publication._absolute_without_symlink_resolution(raw_root)
    expected_raw = _route_publication._absolute_without_symlink_resolution(
        data / "raw" / "historical-foundry-replay"
    )
    if raw != expected_raw:
        raise HistoricalRoutePublicationError(
            "historical complete raw root differs"
        )
    return data, raw


def _historical_context_held_record(
    context: HistoricalReplayBuildContext,
) -> Mapping[str, Any]:
    record = _context_record(context)
    _validate_context_current(record)
    for name in ("stage_record", "published_record", "immutable_record"):
        held = record.get(name)
        if held is not None:
            if (
                type(held.get("derived")) is not dict
                or type(held.get("config")) is not HistoricalFoundryConfigSet
                or type(held.get("manifest")) is not dict
            ):
                raise HistoricalRoutePublicationError(
                    "historical complete context ancestry differs"
                )
            return held
    raise HistoricalRoutePublicationError(
        "historical complete context ancestry is invalid"
    )


def _historical_complete_replay_id(
    *, projection: Mapping[str, Any], route_cohort_id: str,
    overlay_set_sha256: str, scenario_set_sha256: str,
) -> str:
    identity = {
        "route_cohort_id": route_cohort_id,
        "historical_core_manifest_sha256": projection[
            "core_manifest_sha256"
        ],
        "historical_core_pointer_sha256": projection[
            "core_pointer_sha256"
        ],
        "run_id": projection["run_id"],
        "run_manifest_sha256": projection[
            "run_manifest_sha256"
        ],
        "selection_sha256": projection["selection_sha256"],
        "overlay_set_sha256": overlay_set_sha256,
        "scenario_set_sha256": scenario_set_sha256,
    }
    return "replay:" + _sha(
        b"route_historical_replay_identity/v1\0"
        + _canonical_bytes(identity)
    )


def _historical_complete_input_generations(
    *, cohort: Mapping[str, Any], projection: Mapping[str, Any],
    facts: Mapping[str, Any], opportunities: Tuple[Mapping[str, Any], ...],
    costs: Tuple[Mapping[str, Any], ...], source_identity_sha256: str,
) -> Dict[str, Any]:
    return {
        "candidate_source_generation": cohort[
            "candidate_source_generation"
        ],
        "collection_input_generation": cohort[
            "collection_input_generation"
        ],
        "raw_evidence_run_id": projection["run_id"],
        "raw_evidence_generation": projection["run_manifest_sha256"],
        "quantity_quote_generation": _sha(
            b"historical_quantity_quote_generation/v1\0"
            + _canonical_bytes([
                {
                    "scenario_key": row["scenario_key"],
                    "proof_inputs_hash": row["proof_inputs_hash"],
                }
                for row in facts["scenarios"]
            ])
        ),
        "cost_component_generation": _sha(
            b"historical_cost_component_generation/v1\0"
            + _canonical_bytes(list(costs))
        ),
        "classified_opportunity_generation": _sha(
            b"historical_classified_opportunity_generation/v1\0"
            + _canonical_bytes(list(opportunities))
        ),
        "fee_profile_generation": projection["policy_sha256"],
        "inventory_profile_generation": facts["scenario_set_sha256"],
        "typed_source_generation": source_identity_sha256,
        "adapter_versions": dict(
            _route_publication._COMPLETE_ADAPTER_VERSIONS
        ),
    }


def _validate_historical_replay_evidence_join(
    *, bundle: Mapping[str, Any], evidence: Mapping[str, Any],
) -> None:
    expected_fields = {
        "schema", "replay_id", "route_cohort_id", "run_id", "policy_id",
        "policy_sha256", "authority_sha256", "toolchain_sha256",
        "run_manifest_sha256", "selection_sha256", "temporal_scope",
        "execution_claim", "selected_block", "overlay_set_sha256",
        "scenario_count", "scenarios", "scenario_set_sha256",
    }
    scenarios = evidence.get("scenarios")
    if (
        type(evidence) is not dict
        or set(evidence) != expected_fields
        or evidence.get("schema") != _HISTORICAL_REPLAY_EVIDENCE_SCHEMA
        or re.fullmatch(r"replay:[0-9a-f]{64}", evidence.get("replay_id", ""))
        is None
        or evidence.get("route_cohort_id") != bundle["route_cohort_id"]
        or re.fullmatch(
            r"run:[0-9a-f]{64}", evidence.get("run_id", "")
        ) is None
        or re.fullmatch(
            r"policy:[0-9a-f]{64}", evidence.get("policy_id", "")
        ) is None
        or any(
            re.fullmatch(r"[0-9a-f]{64}", evidence.get(field, ""))
            is None
            for field in (
                "policy_sha256", "authority_sha256", "toolchain_sha256",
                "run_manifest_sha256", "selection_sha256",
                "overlay_set_sha256", "scenario_set_sha256",
            )
        )
        or type(evidence.get("selected_block")) is not dict
        or evidence.get("temporal_scope") != _TEMPORAL_SCOPE
        or evidence.get("execution_claim") != _EXECUTION_CLAIM
        or evidence.get("scenario_count") != 10
        or type(scenarios) is not list
        or len(scenarios) != 10
    ):
        raise HistoricalRoutePublicationError(
            "historical replay evidence schema differs"
        )
    if any(
        type(row) is not dict
        or type(row.get("route_id")) is not str
        or type(row.get("requested_notional_usd")) is not int
        for row in scenarios
    ):
        raise HistoricalRoutePublicationError(
            "historical replay scenario identity differs"
        )
    if scenarios != sorted(
        scenarios,
        key=lambda row: (
            row.get("route_id", ""), row.get("requested_notional_usd", -1)
        ),
    ):
        raise HistoricalRoutePublicationError(
            "historical replay evidence order differs"
        )
    opportunities = {
        row["opportunity_id"]: row for row in bundle["opportunities"]
    }
    if len(opportunities) != 10:
        raise HistoricalRoutePublicationError(
            "historical opportunity inventory differs"
        )
    observed = set()
    overlay_inventory = []
    scenario_fields = {
        "schema", "scenario_key", "opportunity_id", "route_id",
        "direction", "requested_notional_usd", "receipt_status",
        "opportunity_class", "core_manifest_sha256", "policy_sha256",
        "authority_sha256", "toolchain_sha256",
        "source_descriptor_set_sha256", "proof_inputs_hash",
        "selected_block", "overlay_sha256", "receipt_sha256",
        "trace_sha256", "result_sha256", "executor_creation_sha256",
        "executor_runtime_sha256", "calldata_sha256", "transaction",
        "receipt", "balances", "gross_edge_weth_raw",
        "gross_buy_cost_usd", "gross_sell_proceeds_usd",
        "gross_edge_usd", "research_net_edge_usd", "baseline",
        "stress_25", "stress_50", "stress_robust",
        "cost_component_set_sha256", "source_members",
    }
    selected_block_fields = {
        "number", "hash", "parent_hash", "state_root", "timestamp",
        "gas_limit", "gas_used", "base_fee_per_gas",
        "synthetic_child_number", "synthetic_child_timestamp",
        "synthetic_child_base_fee_per_gas",
        "p50_priority_fee_per_gas", "p90_priority_fee_per_gas", "eth_usd",
    }
    transaction_fields = {
        "sender", "executor", "type", "nonce", "gas_limit",
        "access_list", "max_priority_fee_per_gas", "max_fee_per_gas",
        "calldata", "calldata_sha256", "transaction_hash",
        "transaction_index",
    }
    receipt_fields = {
        "status", "block_number", "block_hash", "transaction_index",
        "gas_used", "effective_gas_price", "max_fee_per_gas",
        "max_priority_fee_per_gas", "transaction_hash",
        "projection_sha256",
    }
    balance_fields = {
        "initial_weth_raw", "initial_uni_raw", "input_weth_raw",
        "intermediate_uni_raw", "final_weth_raw", "final_uni_raw",
        "gross_weth_delta_raw",
    }
    economics_fields = {
        "name", "priority_fee_percentile", "mev_bps", "gas_used",
        "effective_gas_price", "gas_cost_usd", "mev_buffer_usd",
        "research_net_edge_usd", "positive_research_net",
    }
    eth_usd_fields = {
        "proxy_address", "round_id", "phase_id", "answer", "decimals",
        "started_at", "updated_at", "answered_in_round", "valid_until",
        "block_number", "block_hash",
    }
    selected_integer_fields = (
        "number", "timestamp", "gas_limit", "gas_used",
        "base_fee_per_gas", "synthetic_child_number",
        "synthetic_child_timestamp", "synthetic_child_base_fee_per_gas",
        "p50_priority_fee_per_gas", "p90_priority_fee_per_gas",
    )
    eth_usd_integer_fields = (
        "round_id", "phase_id", "answer", "decimals", "started_at",
        "updated_at", "answered_in_round", "valid_until", "block_number",
    )
    transaction_integer_fields = (
        "nonce", "gas_limit", "max_priority_fee_per_gas",
        "max_fee_per_gas", "transaction_index",
    )
    receipt_integer_fields = (
        "status", "block_number", "transaction_index", "gas_used",
        "effective_gas_price", "max_fee_per_gas",
        "max_priority_fee_per_gas",
    )
    for row in scenarios:
        if type(row) is not dict or set(row) != scenario_fields:
            raise HistoricalRoutePublicationError(
                "historical replay scenario is invalid"
            )
        selected_block = row["selected_block"]
        transaction = row["transaction"]
        receipt = row["receipt"]
        balances = row["balances"]
        source_members = row["source_members"]
        eth_usd = (
            selected_block.get("eth_usd")
            if type(selected_block) is dict else None
        )
        calldata_bytes = None
        if type(transaction) is dict:
            calldata = transaction.get("calldata")
            if (
                type(calldata) is str
                and calldata.startswith("0x")
                and len(calldata[2:]) % 2 == 0
            ):
                try:
                    calldata_bytes = bytes.fromhex(calldata[2:])
                except ValueError:
                    pass
        try:
            block_text, direction, notional_text = row[
                "scenario_key"
            ].split(":")
            scenario_block = int(block_text)
            scenario_notional = int(notional_text)
        except (AttributeError, TypeError, ValueError) as error:
            raise HistoricalRoutePublicationError(
                "historical replay scenario identity differs"
            ) from error
        if (
            row["schema"]
            != "historical_foundry_replay_publication_scenario_facts/v1"
            or str(scenario_block) != block_text
            or str(scenario_notional) != notional_text
            or direction not in {
                "uniswap_to_sushiswap", "sushiswap_to_uniswap"
            }
            or row["direction"] != direction
            or row["requested_notional_usd"] != scenario_notional
            or type(row["receipt_status"]) is not int
            or row["receipt_status"] != 1
            or type(row["gross_edge_weth_raw"]) is not int
            or type(row["stress_robust"]) is not bool
            or type(selected_block) is not dict
            or set(selected_block) != selected_block_fields
            or selected_block != evidence["selected_block"]
            or any(
                type(selected_block[field]) is not int
                or selected_block[field] < 0
                for field in selected_integer_fields
            )
            or any(
                re.fullmatch(
                    r"0x[0-9a-f]{64}", selected_block[field]
                    if type(selected_block[field]) is str else "",
                ) is None
                for field in ("hash", "parent_hash", "state_root")
            )
            or type(eth_usd) is not dict
            or set(eth_usd) != eth_usd_fields
            or re.fullmatch(
                r"0x[0-9a-f]{40}", eth_usd["proxy_address"]
                if type(eth_usd["proxy_address"]) is str else "",
            ) is None
            or any(
                type(eth_usd[field]) is not str
                or re.fullmatch(r"0|[1-9][0-9]*", eth_usd[field]) is None
                for field in eth_usd_integer_fields
            )
            or int(eth_usd["answer"]) <= 0
            or int(eth_usd["valid_until"]) <= int(eth_usd["updated_at"])
            or eth_usd["block_number"] != str(selected_block["number"])
            or eth_usd["block_hash"] != selected_block["hash"]
            or scenario_block != selected_block["number"]
            or type(transaction) is not dict
            or set(transaction) != transaction_fields
            or transaction["type"] != "0x2"
            or transaction["access_list"] != []
            or any(
                type(transaction[field]) is not int
                or transaction[field] < 0
                for field in transaction_integer_fields
            )
            or transaction["gas_limit"] <= 0
            or any(
                re.fullmatch(
                    r"0x[0-9a-f]{40}", transaction[field]
                    if type(transaction[field]) is str else "",
                ) is None
                for field in ("sender", "executor")
            )
            or re.fullmatch(
                r"0x[0-9a-f]{64}", transaction["transaction_hash"]
                if type(transaction["transaction_hash"]) is str else "",
            ) is None
            or calldata_bytes is None
            or _sha(calldata_bytes) != transaction["calldata_sha256"]
            or type(receipt) is not dict
            or set(receipt) != receipt_fields
            or any(
                type(receipt[field]) is not int or receipt[field] < 0
                for field in receipt_integer_fields
            )
            or receipt["status"] != 1
            or receipt["gas_used"] <= 0
            or any(
                re.fullmatch(
                    r"0x[0-9a-f]{64}", receipt[field]
                    if type(receipt[field]) is str else "",
                ) is None
                for field in ("block_hash", "transaction_hash")
            )
            or receipt["max_fee_per_gas"] != transaction["max_fee_per_gas"]
            or receipt["max_priority_fee_per_gas"]
            != transaction["max_priority_fee_per_gas"]
            or type(balances) is not dict
            or set(balances) != balance_fields
            or any(type(value) is not int for value in balances.values())
            or any(
                type(row[name]) is not dict
                or set(row[name]) != economics_fields
                or type(row[name]["positive_research_net"]) is not bool
                or type(row[name]["gas_used"]) is not int
                or row[name]["gas_used"] <= 0
                or type(row[name]["effective_gas_price"]) is not int
                or row[name]["effective_gas_price"] < 0
                for name in ("baseline", "stress_25", "stress_50")
            )
            or row["baseline"]["research_net_edge_usd"]
            != row["research_net_edge_usd"]
            or row["stress_robust"] is not (
                row["stress_25"]["positive_research_net"]
                and row["stress_50"]["positive_research_net"]
            )
            or row["policy_sha256"] != evidence["policy_sha256"]
            or row["authority_sha256"] != evidence["authority_sha256"]
            or row["toolchain_sha256"] != evidence["toolchain_sha256"]
            or transaction["calldata_sha256"] != row["calldata_sha256"]
            or transaction["transaction_hash"]
            != receipt["transaction_hash"]
            or transaction["transaction_index"]
            != receipt["transaction_index"]
            or receipt["projection_sha256"] != row["receipt_sha256"]
            or receipt["status"] != row["receipt_status"]
            or receipt["block_number"]
            != selected_block["synthetic_child_number"]
            or type(source_members) is not list
            or len(source_members) != 4
            or any(
                type(item) is not dict
                or set(item) != {"role", "path", "byte_count", "sha256"}
                for item in source_members
            )
            or any(
                re.fullmatch(r"[0-9a-f]{64}", row.get(field, "")) is None
                for field in (
                    "core_manifest_sha256", "policy_sha256",
                    "authority_sha256", "toolchain_sha256",
                    "source_descriptor_set_sha256", "proof_inputs_hash",
                    "overlay_sha256", "receipt_sha256", "trace_sha256",
                    "result_sha256", "executor_creation_sha256",
                    "executor_runtime_sha256", "calldata_sha256",
                    "cost_component_set_sha256",
                )
            )
        ):
            raise HistoricalRoutePublicationError(
                "historical replay scenario closure differs"
            )
        opportunity = opportunities.get(row.get("opportunity_id"))
        expected_direction = (
            "uniswap_to_sushiswap"
            if ":uniswap_v2:" in opportunity.get("buy_market_id", "")
            and ":sushiswap_v2:" in opportunity.get("sell_market_id", "")
            else "sushiswap_to_uniswap"
            if ":sushiswap_v2:" in opportunity.get("buy_market_id", "")
            and ":uniswap_v2:" in opportunity.get("sell_market_id", "")
            else None
        ) if opportunity is not None else None
        if (
            opportunity is None
            or row["opportunity_id"] in observed
            or row.get("route_id") != opportunity["route_id"]
            or str(row.get("requested_notional_usd"))
            != opportunity["requested_notional_usd"]
            or row.get("opportunity_class")
            != opportunity["opportunity_class"]
            or row.get("opportunity_class") != "research_estimate"
            or row.get("receipt_status") != 1
            or row.get("direction") != expected_direction
            or row.get("core_manifest_sha256")
            != bundle["core_manifest_sha256"]
            or opportunity.get("buy_core_manifest_sha256")
            != row.get("core_manifest_sha256")
            or opportunity.get("sell_core_manifest_sha256")
            != row.get("core_manifest_sha256")
            or opportunity.get("inventory_profile_hash")
            != row.get("proof_inputs_hash")
            or opportunity.get("cost_component_set_sha256")
            != row.get("cost_component_set_sha256")
            or any(
                opportunity.get(field) != row.get(field)
                for field in (
                    "gross_buy_cost_usd", "gross_sell_proceeds_usd",
                    "gross_edge_usd", "research_net_edge_usd",
                )
            )
            or [item.get("role") for item in source_members]
            != ["overlay", "receipt", "trace", "result"]
        ):
            raise HistoricalRoutePublicationError(
                "historical replay evidence join differs"
            )
        observed.add(row["opportunity_id"])
        source_root = "foundry/{}/{}".format(
            selected_block["number"], row["scenario_key"]
        )
        expected_sources = (
            ("overlay", "overlay.json", "overlay_sha256"),
            ("receipt", "receipt.json", "receipt_sha256"),
            ("trace", "trace.json.gz", "trace_sha256"),
            ("result", "result.json", "result_sha256"),
        )
        if any(
            item["path"] != "{}/{}".format(source_root, filename)
            or item["sha256"] != row[hash_field]
            or type(item["byte_count"]) is not int
            or item["byte_count"] <= 0
            for item, (_role, filename, hash_field) in zip(
                source_members, expected_sources
            )
        ):
            raise HistoricalRoutePublicationError(
                "historical replay source member differs"
            )
        overlay_inventory.append({
            "scenario_key": row["scenario_key"],
            "overlay_sha256": row["overlay_sha256"],
        })
    if observed != set(opportunities):
        raise HistoricalRoutePublicationError(
            "historical replay evidence inventory is not closed"
        )
    if (
        evidence["overlay_set_sha256"]
        != _sha(
            b"historical_foundry_overlay_set/v1\0"
            + _canonical_bytes(overlay_inventory)
        )
        or evidence["scenario_set_sha256"]
        != _sha(
            b"historical_foundry_scenario_set/v1\0"
            + _canonical_bytes(scenarios)
        )
        or not any(
            row.get("baseline", {}).get("positive_research_net") is True
            for row in scenarios
        )
    ):
        raise HistoricalRoutePublicationError(
            "historical replay evidence digest differs"
        )


def _build_historical_complete_payload(
    *, context: HistoricalReplayBuildContext,
) -> Mapping[str, Any]:
    projection = dict(_require_historical_replay_build_context(
        context=context
    ))
    held = _historical_context_held_record(context)
    cohort = held["derived"]["cohort"]
    route_cohort_id = projection["core_pointer"]["route_cohort_id"]
    if (
        cohort.get("route_cohort_id") != route_cohort_id
        or projection.get("core_manifest_sha256")
        != held["manifest_sha256"]
    ):
        raise HistoricalRoutePublicationError(
            "historical complete core lineage differs"
        )
    block_number = projection["selected_block"]["number"]
    scenario_keys = tuple(
        "{}:{}:{}".format(block_number, direction, notional)
        for direction in (
            "uniswap_to_sushiswap", "sushiswap_to_uniswap"
        )
        for notional in _NOTIONALS
    )
    built = tuple(
        _build_historical_scenario_for_publication(
            context=context, scenario_key=scenario_key
        )
        for scenario_key in scenario_keys
    )

    def expected_fact_from_sealed(row: Mapping[str, Any]) -> Dict[str, Any]:
        compact = json.loads(row["canonical_projection_bytes"])
        raw = json.loads(row["canonical_input_bytes"])
        descriptors = json.loads(row["canonical_source_descriptor_bytes"])
        scenario_key = compact["scenario_key"]
        block_text, direction, notional_text = scenario_key.split(":")
        block_number = int(block_text)
        notional = int(notional_text)
        selected = raw["selected_block"]
        synthetic = raw["overlay"]["synthetic_block"]
        fee = raw["fee"]
        price = raw["price"]
        transaction = raw["overlay"]["transaction"]
        receipt = raw["receipt"]
        trace_balances = raw["trace"]["balances"]
        trace_deltas = raw["trace"]["actual_deltas"]
        proof_authority = raw["result"]["proof_authority"]
        economics_authority = compact["economics_authority"]
        opportunity = compact["opportunity"]
        economics = compact["economics_scenarios"]
        descriptor_by_path = {
            descriptor["path"]: descriptor for descriptor in descriptors
        }
        source_members = []
        for role, filename in (
            ("overlay", "overlay.json"),
            ("receipt", "receipt.json"),
            ("trace", "trace.json.gz"),
            ("result", "result.json"),
        ):
            path = "foundry/{}/{}/{}".format(
                block_number, scenario_key, filename
            )
            source_members.append({
                "role": role, **dict(descriptor_by_path[path])
            })
        transaction_fact = {
            "sender": transaction["from"],
            "executor": transaction["to"],
            "type": transaction["type"],
            "nonce": transaction["nonce"],
            "gas_limit": transaction["gas"],
            "access_list": list(transaction["accessList"]),
            "max_priority_fee_per_gas": transaction[
                "maxPriorityFeePerGas"
            ],
            "max_fee_per_gas": transaction["maxFeePerGas"],
            "calldata": transaction["input"],
            "calldata_sha256": transaction["calldata_sha256"],
            "transaction_hash": receipt["transactionHash"],
            "transaction_index": receipt["transactionIndex"],
        }
        receipt_fact = {
            "status": receipt["status"],
            "block_number": receipt["blockNumber"],
            "block_hash": receipt["blockHash"],
            "transaction_index": receipt["transactionIndex"],
            "gas_used": receipt["gasUsed"],
            "effective_gas_price": receipt["effectiveGasPrice"],
            "max_fee_per_gas": receipt["maxFeePerGas"],
            "max_priority_fee_per_gas": receipt[
                "maxPriorityFeePerGas"
            ],
            "transaction_hash": receipt["transactionHash"],
            "projection_sha256": compact["receipt_sha256"],
        }
        selected_block = {
            **dict(selected),
            "synthetic_child_number": synthetic["number"],
            "synthetic_child_timestamp": synthetic["timestamp"],
            "synthetic_child_base_fee_per_gas": synthetic[
                "base_fee_per_gas"
            ],
            "p50_priority_fee_per_gas": fee[
                "p50_priority_fee_per_gas"
            ],
            "p90_priority_fee_per_gas": fee[
                "p90_priority_fee_per_gas"
            ],
            "eth_usd": {
                field: price[field] for field in (
                    "proxy_address", "round_id", "phase_id", "answer",
                    "decimals", "started_at", "updated_at",
                    "answered_in_round", "valid_until", "block_number",
                    "block_hash",
                )
            },
        }
        return {
            "schema": (
                "historical_foundry_replay_publication_"
                "scenario_facts/v1"
            ),
            "scenario_key": scenario_key,
            "opportunity_id": opportunity["opportunity_id"],
            "route_id": compact["route_id"],
            "direction": direction,
            "requested_notional_usd": notional,
            "receipt_status": compact["receipt_status"],
            "opportunity_class": opportunity["opportunity_class"],
            "core_manifest_sha256": compact["core_manifest_sha256"],
            "policy_sha256": proof_authority["policy_sha256"],
            "authority_sha256": proof_authority["authority_sha256"],
            "toolchain_sha256": proof_authority["toolchain_sha256"],
            "source_descriptor_set_sha256": raw[
                "source_descriptor_set_sha256"
            ],
            "proof_inputs_hash": compact["proof_inputs_hash"],
            "selected_block": selected_block,
            "overlay_sha256": compact["overlay_sha256"],
            "receipt_sha256": compact["receipt_sha256"],
            "trace_sha256": compact["trace_sha256"],
            "result_sha256": compact["result_sha256"],
            "executor_creation_sha256": proof_authority[
                "adapter_proof_sha256"
            ],
            "executor_runtime_sha256": proof_authority[
                "executor_runtime_sha256"
            ],
            "calldata_sha256": transaction_fact["calldata_sha256"],
            "transaction": transaction_fact,
            "receipt": receipt_fact,
            "balances": {
                "initial_weth_raw": trace_balances["initial_weth_raw"],
                "initial_uni_raw": trace_balances["initial_uni_raw"],
                "input_weth_raw": economics_authority[
                    "first_weth_in_raw"
                ],
                "intermediate_uni_raw": economics_authority[
                    "first_uni_out_raw"
                ],
                "final_weth_raw": trace_balances["final_weth_raw"],
                "final_uni_raw": trace_balances["final_uni_raw"],
                "gross_weth_delta_raw": trace_deltas["weth_raw"],
            },
            "gross_edge_weth_raw": compact["gross_edge_weth_raw"],
            "gross_buy_cost_usd": opportunity["gross_buy_cost_usd"],
            "gross_sell_proceeds_usd": opportunity[
                "gross_sell_proceeds_usd"
            ],
            "gross_edge_usd": opportunity["gross_edge_usd"],
            "research_net_edge_usd": opportunity[
                "research_net_edge_usd"
            ],
            "baseline": economics[0],
            "stress_25": economics[1],
            "stress_50": economics[2],
            "stress_robust": (
                economics[1]["positive_research_net"]
                and economics[2]["positive_research_net"]
            ),
            "cost_component_set_sha256": opportunity[
                "cost_component_set_sha256"
            ],
            "source_members": source_members,
        }

    try:
        expected_fact_scenarios = []
        for row in built:
            expected_fact_scenarios.append(expected_fact_from_sealed(row))
        expected_fact_scenarios.sort(key=lambda row: (
            row["route_id"], row["requested_notional_usd"]
        ))
        facts_bytes = (
            _historical_replay.build_historical_replay_publication_facts(
                tuple(row["canonical_projection_bytes"] for row in built),
                tuple(row["canonical_input_bytes"] for row in built),
                tuple(
                    row["canonical_source_descriptor_bytes"] for row in built
                ),
            )
        )
        facts = json.loads(facts_bytes)
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as error:
        raise HistoricalRoutePublicationError(
            "historical replay publication facts are invalid"
        ) from error
    expected_fact_fields = {
        "schema", "scenario_count", "core_manifest_sha256",
        "overlay_set_sha256", "scenario_set_sha256", "scenarios",
    }
    header_fields = (
        "number", "hash", "parent_hash", "state_root", "timestamp",
        "gas_limit", "gas_used", "base_fee_per_gas",
    )
    authoritative_block = projection["selected_block"]

    def selected_block_matches_context(row: Any) -> bool:
        if type(row) is not dict:
            return False
        selected_block = row.get("selected_block")
        if (
            type(selected_block) is not dict
            or any(field not in selected_block for field in header_fields)
        ):
            return False
        header = {
            field: selected_block[field] for field in header_fields
        }
        return (
            selected_block.get("number") == authoritative_block["number"]
            and selected_block.get("hash") == authoritative_block["hash"]
            and type(selected_block.get("timestamp")) is int
            and _rfc3339(selected_block["timestamp"])
            == authoritative_block["timestamp"]
            and _sha(_canonical_bytes(header))
            == authoritative_block["header_sha256"]
        )

    fact_scenarios = facts.get("scenarios") if type(facts) is dict else None
    expected_overlay_inventory = [
        {
            "scenario_key": row["scenario_key"],
            "overlay_sha256": row["overlay_sha256"],
        }
        for row in expected_fact_scenarios
    ]
    if (
        type(facts) is not dict
        or set(facts) != expected_fact_fields
        or facts_bytes != _canonical_bytes(facts)
        or facts.get("schema")
        != "historical_foundry_replay_publication_facts/v1"
        or facts.get("scenario_count") != 10
        or facts.get("core_manifest_sha256")
        != projection["core_manifest_sha256"]
        or type(fact_scenarios) is not list
        or len(fact_scenarios) != 10
        or fact_scenarios != expected_fact_scenarios
        or facts.get("overlay_set_sha256")
        != _sha(
            b"historical_foundry_overlay_set/v1\0"
            + _canonical_bytes(expected_overlay_inventory)
        )
        or facts.get("scenario_set_sha256")
        != _sha(
            b"historical_foundry_scenario_set/v1\0"
            + _canonical_bytes(expected_fact_scenarios)
        )
        or any(
            not selected_block_matches_context(row)
            for row in fact_scenarios
        )
        or len({
            _canonical_bytes(row["selected_block"])
            for row in fact_scenarios
        }) != 1
    ):
        raise HistoricalRoutePublicationError(
            "historical replay publication facts differ"
        )
    replay_id = _historical_complete_replay_id(
        projection=projection, route_cohort_id=route_cohort_id,
        overlay_set_sha256=facts["overlay_set_sha256"],
        scenario_set_sha256=facts["scenario_set_sha256"],
    )
    config = held["config"]
    evidence = {
        "schema": _HISTORICAL_REPLAY_EVIDENCE_SCHEMA,
        "replay_id": replay_id,
        "route_cohort_id": route_cohort_id,
        "run_id": projection["run_id"],
        "policy_id": config.policy.policy_id,
        "policy_sha256": projection["policy_sha256"],
        "authority_sha256": projection["authority_sha256"],
        "toolchain_sha256": projection["toolchain_sha256"],
        "run_manifest_sha256": projection["run_manifest_sha256"],
        "selection_sha256": projection["selection_sha256"],
        "temporal_scope": _TEMPORAL_SCOPE,
        "execution_claim": _EXECUTION_CLAIM,
        "selected_block": facts["scenarios"][0]["selected_block"],
        "overlay_set_sha256": facts["overlay_set_sha256"],
        "scenario_count": 10,
        "scenarios": facts["scenarios"],
        "scenario_set_sha256": facts["scenario_set_sha256"],
    }
    opportunities = tuple(sorted(
        (_plain(row["opportunity"]) for row in built),
        key=_route_publication._complete_opportunity_sort_key,
    ))
    costs = tuple(sorted(
        (
            _plain(cost)
            for row in built for cost in row["cost_components"]
        ),
        key=lambda row: (
            row["opportunity_id"], row["leg"], row["component_type"]
        ),
    ))
    bundle = {
        "schema": _route_publication.ROUTE_OPPORTUNITY_BUNDLE_STAGE,
        "route_cohort_id": route_cohort_id,
        "core_manifest_sha256": projection["core_manifest_sha256"],
        "core_pointer_sha256": projection["core_pointer_sha256"],
        "core_context": {
            field: cohort[field] for field in (
                "candidate_source_generation",
                "collection_input_generation", "raw_evidence_run_id",
                "collection_completed_at", "collection_deadline_at",
            )
        },
        "input_generations": _historical_complete_input_generations(
            cohort=cohort, projection=projection, facts=facts,
            opportunities=opportunities, costs=costs,
            source_identity_sha256=held["derived"][
                "source_identity_sha256"
            ],
        ),
        "routes": sorted(
            (_plain(row) for row in cohort["routes"]),
            key=lambda row: row["route_id"],
        ),
        "legs": sorted(
            (_plain(row) for row in cohort["legs"]),
            key=lambda row: row["market_id"],
        ),
        "cost_components": list(costs),
        "opportunities": list(opportunities),
    }
    try:
        bundle = _route_publication._validate_complete_logical_bundle_shared(
            bundle, historical_atomic=True
        )
    except _route_publication.RoutePublicationError as error:
        raise HistoricalRoutePublicationError(
            "historical complete logical bundle is invalid"
        ) from error
    _validate_historical_replay_evidence_join(
        bundle=bundle, evidence=evidence
    )
    context.reread_unchanged()
    return MappingProxyType({
        "replay_id": replay_id,
        "bundle": bundle,
        "replay_evidence": evidence,
    })


def _historical_complete_file_descriptors(
    *, file_bytes: Mapping[str, bytes], bundle: Mapping[str, Any],
    evidence: Mapping[str, Any],
) -> Dict[str, Dict[str, Any]]:
    return {
        "route_legs.csv": _route_publication._artifact_details_bytes(
            file_bytes["route_legs.csv"],
            schema=_route_publication.ROUTE_LEG_CSV_SCHEMA,
            logical_sha256=_route_publication._logical_rows_sha256(
                _route_publication.ROUTE_LEG_CSV_SCHEMA, bundle["legs"]
            ),
            row_count=len(bundle["legs"]),
        ),
        "cost_components.csv": _route_publication._artifact_details_bytes(
            file_bytes["cost_components.csv"],
            schema=_route_publication.COST_COMPONENT_CSV_SCHEMA,
            logical_sha256=_route_publication._logical_rows_sha256(
                _route_publication.COST_COMPONENT_CSV_SCHEMA,
                bundle["cost_components"],
            ),
            row_count=len(bundle["cost_components"]),
        ),
        "route_opportunities.csv": (
            _route_publication._artifact_details_bytes(
                file_bytes["route_opportunities.csv"],
                schema=_route_publication.ROUTE_OPPORTUNITY_CSV_SCHEMA,
                logical_sha256=_route_publication._logical_rows_sha256(
                    _route_publication.ROUTE_OPPORTUNITY_CSV_SCHEMA,
                    bundle["opportunities"],
                ),
                row_count=len(bundle["opportunities"]),
            )
        ),
        "route_cohort.sqlite3": _route_publication._artifact_details_bytes(
            file_bytes["route_cohort.sqlite3"],
            schema=_route_publication.ROUTE_OPPORTUNITY_SQLITE_SCHEMA,
            logical_sha256=(
                _route_publication._complete_database_logical_sha256(bundle)
            ),
            row_count=(
                len(bundle["legs"]) + len(bundle["cost_components"])
                + len(bundle["opportunities"])
            ),
        ),
        _HISTORICAL_REPLAY_EVIDENCE_FILENAME: (
            _route_publication._artifact_details_bytes(
                file_bytes[_HISTORICAL_REPLAY_EVIDENCE_FILENAME],
                schema=_HISTORICAL_REPLAY_EVIDENCE_SCHEMA,
                logical_sha256=_sha(_canonical_bytes(evidence)),
                row_count=len(evidence["scenarios"]),
            )
        ),
    }


def _historical_complete_manifest(
    *, payload: Mapping[str, Any],
    files: Mapping[str, Mapping[str, Any]],
) -> Dict[str, Any]:
    bundle = payload["bundle"]
    evidence = payload["replay_evidence"]
    opportunities = bundle["opportunities"]
    return {
        "schema": _HISTORICAL_COMPLETE_MANIFEST_SCHEMA,
        "bundle_stage": _HISTORICAL_COMPLETE_STAGE,
        "replay_id": payload["replay_id"],
        "route_cohort_id": bundle["route_cohort_id"],
        "historical_core_manifest_sha256": bundle[
            "core_manifest_sha256"
        ],
        "historical_core_pointer_sha256": bundle[
            "core_pointer_sha256"
        ],
        "temporal_scope": _TEMPORAL_SCOPE,
        "execution_claim": _EXECUTION_CLAIM,
        "policy_sha256": evidence["policy_sha256"],
        "authority_sha256": evidence["authority_sha256"],
        "toolchain_sha256": evidence["toolchain_sha256"],
        "run_id": evidence["run_id"],
        "run_manifest_sha256": evidence["run_manifest_sha256"],
        "selection_sha256": evidence["selection_sha256"],
        "selected_block": evidence["selected_block"],
        "requested_notionals_usd": list(_NOTIONALS),
        "counts": {
            "routes": len(bundle["routes"]),
            "legs": len(bundle["legs"]),
            "opportunities": len(opportunities),
            "cost_components": len(bundle["cost_components"]),
            "scenarios": len(evidence["scenarios"]),
            "foundry_verified": sum(
                row["receipt_status"] == 1
                for row in evidence["scenarios"]
            ),
            "research_estimate": sum(
                row["opportunity_class"] == "research_estimate"
                for row in evidence["scenarios"]
            ),
            "unavailable": sum(
                row["opportunity_class"] == "unavailable"
                for row in evidence["scenarios"]
            ),
            "strict_eligible": sum(
                row["strict_eligible"] is True for row in opportunities
            ),
            "executable_candidate": sum(
                row["opportunity_class"] == "executable_candidate"
                for row in opportunities
            ),
            "attested": sum(
                row["publication_attestation_sha256"] is not None
                for row in opportunities
            ),
            "positive_research_net": sum(
                row["baseline"]["positive_research_net"] is True
                for row in evidence["scenarios"]
            ),
        },
        "files": {name: dict(files[name]) for name in sorted(files)},
    }


def _historical_complete_artifacts(
    payload: Mapping[str, Any],
) -> Tuple[Dict[str, bytes], Dict[str, Any]]:
    try:
        representation, _unused = (
            _route_publication
            ._complete_representation_artifact_bytes_from_validated_bundle(
                payload["bundle"]
            )
        )
    except _route_publication.RoutePublicationError as error:
        raise HistoricalRoutePublicationError(
            "historical complete serialization failed"
        ) from error
    evidence_bytes = _json_file_bytes(payload["replay_evidence"])
    file_bytes = {
        **representation,
        _HISTORICAL_REPLAY_EVIDENCE_FILENAME: evidence_bytes,
    }
    files = _historical_complete_file_descriptors(
        file_bytes=file_bytes, bundle=payload["bundle"],
        evidence=payload["replay_evidence"],
    )
    manifest = _historical_complete_manifest(payload=payload, files=files)
    return {**file_bytes, "manifest.json": _json_file_bytes(manifest)}, manifest


def _validate_historical_complete_artifact_bytes(
    *, artifacts: Mapping[str, bytes], payload: Mapping[str, Any],
    manifest: Mapping[str, Any],
) -> None:
    if set(artifacts) != _HISTORICAL_COMPLETE_FILES:
        raise HistoricalRoutePublicationError(
            "historical complete artifact inventory differs"
        )
    try:
        representation = {
            name: artifacts[name]
            for name in _route_publication._COMPLETE_MANIFEST_ARTIFACT_FILENAMES
        }
        bundle, legs, costs, opportunities = (
            _route_publication._read_complete_representation_bytes(
                file_bytes=representation,
                route_cohort_id=payload["bundle"]["route_cohort_id"],
            )
        )
        bundle = _route_publication._validate_complete_logical_bundle_shared(
            bundle, historical_atomic=True
        )
        evidence = json.loads(
            artifacts[_HISTORICAL_REPLAY_EVIDENCE_FILENAME]
        )
    except (
        _route_publication.RoutePublicationError, UnicodeDecodeError,
        json.JSONDecodeError,
    ) as error:
        raise HistoricalRoutePublicationError(
            "historical complete representation is invalid"
        ) from error
    if (
        bundle != payload["bundle"]
        or legs != bundle["legs"]
        or costs != bundle["cost_components"]
        or opportunities != bundle["opportunities"]
        or artifacts[_HISTORICAL_REPLAY_EVIDENCE_FILENAME]
        != _json_file_bytes(evidence)
        or evidence != payload["replay_evidence"]
        or artifacts["manifest.json"] != _json_file_bytes(manifest)
    ):
        raise HistoricalRoutePublicationError(
            "historical complete representation differs"
        )
    _validate_historical_replay_evidence_join(
        bundle=bundle, evidence=evidence
    )


def stage_historical_replay_bundle(
    *, data_dir: Path, raw_root: Path,
    context: HistoricalReplayBuildContext,
) -> Mapping[str, Any]:
    """Build, validate, and install one immutable unpointed replay bundle."""
    data, raw = _historical_complete_roots(
        data_dir=data_dir, raw_root=raw_root
    )
    payload = _build_historical_complete_payload(context=context)
    artifacts, manifest = _historical_complete_artifacts(payload)
    _validate_historical_complete_artifact_bytes(
        artifacts=artifacts, payload=payload, manifest=manifest
    )
    manifest_sha256 = _sha(artifacts["manifest.json"])
    historical_root = _route_publication._ensure_real_directory(
        data / "routes" / "historical"
    )
    historical_root, historical_fd, historical_details = (
        _route_publication._open_verified_directory(
            historical_root, "historical complete root"
        )
    )
    bundles_fd = stage_fd = None
    stage_name = None
    stage_details = None
    renamed = False
    installed_by_call = False
    locked = False
    result = None
    try:
        fcntl.flock(historical_fd, fcntl.LOCK_EX)
        locked = True
        pointer_before = _route_publication._optional_pointer_snapshot_at(
            historical_fd
        )
        bundles_fd, bundles_details = _route_publication._ensure_directory_at(
            historical_fd, "bundles", "historical complete bundles"
        )
        bundles = historical_root / "bundles"
        replay_id = payload["replay_id"]
        final_path = bundles / replay_id
        if not _route_publication._entry_exists_at(bundles_fd, replay_id):
            stage_name, stage_path, stage_fd, stage_details = (
                _route_publication._make_unique_directory_at(
                    bundles_fd, prefix=".historical-replay-",
                    display_parent=bundles,
                )
            )
            for filename in sorted(artifacts):
                _route_publication._write_new_bytes_at(
                    stage_fd, filename, artifacts[filename]
                )
            _route_publication._fsync_directory(
                stage_path, directory_fd=stage_fd
            )
            for filename, expected in artifacts.items():
                current, digest, _details = (
                    _route_publication._read_bounded_bytes_at(
                        stage_fd, filename,
                        limit=(
                            _route_publication._MAX_SQLITE_BYTES
                            if filename.endswith(".sqlite3")
                            else _route_publication._MAX_CSV_BYTES
                            if filename.endswith(".csv")
                            else _route_publication._MAX_JSON_BYTES
                        ),
                        label="historical complete staged member",
                    )
                )
                if current != expected or digest != _sha(expected):
                    raise HistoricalRoutePublicationError(
                        "historical complete staged member differs"
                    )
            staged_validation = _validate_historical_replay_bundle(
                data_dir=data, raw_root=raw, bundle_path=stage_path,
                expected_pointer_core=None,
                expected_replay_id=replay_id,
                require_directory_identity=False,
                issue_view=False,
            )
            if staged_validation["manifest_sha256"] != manifest_sha256:
                raise HistoricalRoutePublicationError(
                    "historical complete staged manifest differs"
                )
            _route_publication._rename_directory_noreplace_at(
                bundles_fd, stage_name, bundles_fd, replay_id,
                destination_display=final_path,
            )
            renamed = True
            installed_by_call = True
            try:
                _route_publication._verify_directory_entry(
                    bundles_fd, replay_id, stage_details,
                    "historical complete bundle",
                )
                _route_publication._fsync_directory(
                    bundles, directory_fd=bundles_fd
                )
                committed_validation = _validate_historical_replay_bundle(
                    data_dir=data, raw_root=raw, bundle_path=final_path,
                    expected_pointer_core=None,
                    expected_replay_id=replay_id,
                    require_directory_identity=True,
                    issue_view=False,
                )
                if (
                    committed_validation["manifest_sha256"]
                    != manifest_sha256
                ):
                    raise HistoricalRoutePublicationError(
                        "historical complete committed manifest differs"
                    )
            except BaseException:
                _route_publication._remove_stage_directory_at(
                    bundles_fd, replay_id, stage_details
                )
                _route_publication._fsync_directory(
                    bundles, directory_fd=bundles_fd
                )
                renamed = False
                raise
        if not _snapshot_matches(
            _route_publication._optional_pointer_snapshot_at(historical_fd),
            pointer_before,
        ):
            raise HistoricalRoutePublicationError(
                "historical complete staging moved latest"
            )
        _route_publication._verify_directory_entry(
            historical_fd, "bundles", bundles_details,
            "historical complete bundles",
        )
        _route_publication._verify_open_path_identity(
            historical_root, historical_details,
            "historical complete root",
        )
        validated = validate_historical_replay_bundle(
            data_dir=data, raw_root=raw,
            bundle_path=(historical_root / "bundles" / payload["replay_id"]),
        )
        try:
            if validated["manifest_sha256"] != manifest_sha256:
                raise HistoricalRoutePublicationError(
                    "historical complete committed manifest differs"
                )
            context.reread_unchanged()
            result = MappingProxyType({
                "path": validated["path"],
                "replay_id": payload["replay_id"],
                "manifest_sha256": manifest_sha256,
                "pointer_core": validated["pointer_core"],
                "verification_subject": validated[
                    "verification_subject"
                ],
            })
        except BaseException:
            try:
                validated["validated_view"].close()
            except Exception:
                pass
            raise
    except BaseException as error:
        if installed_by_call and renamed:
            try:
                _route_publication._remove_stage_directory_at(
                    bundles_fd, payload["replay_id"], stage_details
                )
                _route_publication._fsync_directory(
                    historical_root / "bundles", directory_fd=bundles_fd
                )
                renamed = False
                installed_by_call = False
            except Exception as rollback_error:
                if not isinstance(error, Exception):
                    raise error
                raise HistoricalRoutePublicationError(
                    "historical complete rollback failed"
                ) from rollback_error
        if isinstance(error, _route_publication.RoutePublicationError):
            raise HistoricalRoutePublicationError(
                "historical complete staging failed"
            ) from error
        raise
    finally:
        if (
            not renamed and bundles_fd is not None and stage_name is not None
            and stage_details is not None
        ):
            try:
                _route_publication._remove_stage_directory_at(
                    bundles_fd, stage_name, stage_details
                )
            except Exception:
                pass
        if locked:
            try:
                fcntl.flock(historical_fd, fcntl.LOCK_UN)
            except Exception:
                pass
        _close_descriptors_robustly(stage_fd, bundles_fd, historical_fd)
    if result is None:
        raise HistoricalRoutePublicationError(
            "historical complete staging result is missing"
        )
    return result


def _validate_historical_expected_pointer_core(
    *, expected: Optional[Mapping[str, Any]], manifest: Mapping[str, Any],
    manifest_sha256: str,
) -> None:
    if expected is None:
        return None
    if (
        not isinstance(expected, Mapping)
        or set(expected) != {
            "schema", "bundle_stage", "replay_id", "route_cohort_id",
            "manifest_sha256",
        }
        or expected.get("schema") != "route_historical_replay_pointer/v1"
        or expected.get("bundle_stage") != _HISTORICAL_COMPLETE_STAGE
        or expected.get("replay_id") != manifest["replay_id"]
        or expected.get("route_cohort_id") != manifest["route_cohort_id"]
        or expected.get("manifest_sha256") != manifest_sha256
    ):
        raise HistoricalRoutePublicationError(
            "historical replay expected pointer core differs"
        )


def _validate_historical_replay_bundle_view_current(
    record: Mapping[str, Any],
) -> None:
    context = record.get("immutable_context")
    if type(context) is not HistoricalReplayBuildContext:
        raise HistoricalRoutePublicationError(
            "historical replay view core context is invalid"
        )
    context.reread_unchanged()
    try:
        source = _context_record(context)["source"]
        source.reread_unchanged()
    except Exception as error:
        raise HistoricalRoutePublicationError(
            "historical replay view raw source changed"
        ) from error
    parent, parent_fd, parent_details = (
        _route_publication._open_verified_directory(
            record["bundle_path"].parent,
            "historical complete bundles",
        )
    )
    bundle_fd = None
    file_fds = {}
    try:
        _route_publication._verify_open_path_identity(
            parent, record["parent_details"],
            "historical complete bundles",
        )
        bundle_fd, current_bundle = _route_publication._open_directory_at(
            parent_fd, record["replay_id"],
            "historical complete bundle",
        )
        if (
            _route_publication._stable_file_metadata(current_bundle)
            != _route_publication._stable_file_metadata(
                record["bundle_details"]
            )
            or set(os.listdir(bundle_fd)) != _HISTORICAL_COMPLETE_FILES
        ):
            raise HistoricalRoutePublicationError(
                "historical complete bundle changed"
            )
        for filename, (limit, label) in record["read_specs"].items():
            descriptor, before = _route_publication._open_regular_file_at(
                bundle_fd, filename, label=label
            )
            file_fds[filename] = descriptor
            value, digest, after = _route_publication._read_bounded_open_file(
                descriptor, before, limit=limit, label=label
            )
            if (
                value != record["file_bytes"][filename]
                or digest != record["file_hashes"][filename]
                or _route_publication._stable_file_metadata(after)
                != _route_publication._stable_file_metadata(
                    record["file_details"][filename]
                )
            ):
                raise HistoricalRoutePublicationError(
                    "historical complete bundle member changed"
                )
        _route_publication._verify_bundle_file_snapshots(
            bundle_fd, record["read_specs"], file_fds,
            record["file_details"], record["file_bytes"],
            record["file_hashes"], _HISTORICAL_COMPLETE_FILES,
        )
        _route_publication._verify_directory_entry_snapshot(
            parent_fd, record["replay_id"], record["bundle_details"],
            "historical complete bundle",
        )
        _route_publication._verify_open_path_identity(
            parent, parent_details, "historical complete bundles"
        )
    except _route_publication.RoutePublicationError as error:
        raise HistoricalRoutePublicationError(
            "historical replay view identity differs"
        ) from error
    finally:
        _close_descriptors_robustly(
            *tuple(file_fds.values()), bundle_fd, parent_fd
        )


def _validate_historical_replay_bundle_impl(
    *, data_dir: Path, raw_root: Path, bundle_path: Path,
    expected_pointer_core: Optional[Mapping[str, Any]],
    expected_replay_id: Optional[str],
    require_directory_identity: bool,
    issue_view: bool,
    view_issuer: Any,
) -> Mapping[str, Any]:
    """Fully reread one six-file replay against pinned core and raw proof."""
    data, raw = _historical_complete_roots(
        data_dir=data_dir, raw_root=raw_root
    )
    if (
        not isinstance(bundle_path, Path)
        or type(require_directory_identity) is not bool
        or type(issue_view) is not bool
        or expected_replay_id is not None
        and (
            type(expected_replay_id) is not str
            or re.fullmatch(r"replay:[0-9a-f]{64}", expected_replay_id)
            is None
        )
    ):
        raise HistoricalRoutePublicationError(
            "historical complete bundle path is invalid"
        )
    bundle = _route_publication._absolute_without_symlink_resolution(
        bundle_path
    )
    expected_parent = _route_publication._absolute_without_symlink_resolution(
        data / "routes" / "historical" / "bundles"
    )
    if bundle.parent != expected_parent:
        raise HistoricalRoutePublicationError(
            "historical complete bundle root differs"
        )
    try:
        _route_publication._require_relative_basename(
            bundle.name, "historical complete replay ID"
        )
    except _route_publication.RoutePublicationError as error:
        raise HistoricalRoutePublicationError(
            "historical complete replay ID is invalid"
        ) from error
    parent, parent_fd, parent_details = (
        _route_publication._open_verified_directory(
            expected_parent, "historical complete bundles"
        )
    )
    bundle_fd = None
    file_fds = {}
    immutable_context = None
    try:
        bundle_fd, bundle_details = _route_publication._open_directory_at(
            parent_fd, bundle.name, "historical complete bundle"
        )
        if set(os.listdir(bundle_fd)) != _HISTORICAL_COMPLETE_FILES:
            raise HistoricalRoutePublicationError(
                "historical complete file inventory differs"
            )
        read_specs = {
            "manifest.json": (
                _route_publication._MAX_JSON_BYTES,
                "historical complete manifest",
            ),
            "route_legs.csv": (
                _route_publication._MAX_CSV_BYTES,
                "historical complete route legs",
            ),
            "cost_components.csv": (
                _route_publication._MAX_CSV_BYTES,
                "historical complete costs",
            ),
            "route_opportunities.csv": (
                _route_publication._MAX_CSV_BYTES,
                "historical complete opportunities",
            ),
            "route_cohort.sqlite3": (
                _route_publication._MAX_SQLITE_BYTES,
                "historical complete SQLite",
            ),
            _HISTORICAL_REPLAY_EVIDENCE_FILENAME: (
                _route_publication._MAX_JSON_BYTES,
                "historical replay evidence",
            ),
        }
        file_bytes = {}
        file_hashes = {}
        file_details = {}
        for filename, (limit, label) in read_specs.items():
            descriptor, before = _route_publication._open_regular_file_at(
                bundle_fd, filename, label=label
            )
            file_fds[filename] = descriptor
            value, digest, after = _route_publication._read_bounded_open_file(
                descriptor, before, limit=limit, label=label
            )
            current = os.stat(
                filename, dir_fd=bundle_fd, follow_symlinks=False
            )
            if (
                _route_publication._stable_file_metadata(current)
                != _route_publication._stable_file_metadata(after)
            ):
                raise HistoricalRoutePublicationError(
                    "historical complete member changed during validation"
                )
            file_bytes[filename] = value
            file_hashes[filename] = digest
            file_details[filename] = after
        try:
            manifest = json.loads(file_bytes["manifest.json"])
            evidence = json.loads(
                file_bytes[_HISTORICAL_REPLAY_EVIDENCE_FILENAME]
            )
        except (UnicodeDecodeError, json.JSONDecodeError) as error:
            raise HistoricalRoutePublicationError(
                "historical complete JSON member is invalid"
            ) from error
        manifest_fields = {
            "schema", "bundle_stage", "replay_id", "route_cohort_id",
            "historical_core_manifest_sha256",
            "historical_core_pointer_sha256", "temporal_scope",
            "execution_claim", "policy_sha256", "authority_sha256",
            "toolchain_sha256", "run_id", "run_manifest_sha256",
            "selection_sha256", "selected_block",
            "requested_notionals_usd", "counts", "files",
        }
        manifest_sha256 = file_hashes["manifest.json"]
        if (
            type(manifest) is not dict
            or set(manifest) != manifest_fields
            or file_bytes["manifest.json"] != _json_file_bytes(manifest)
            or manifest.get("schema")
            != _HISTORICAL_COMPLETE_MANIFEST_SCHEMA
            or manifest.get("bundle_stage") != _HISTORICAL_COMPLETE_STAGE
            or re.fullmatch(
                r"replay:[0-9a-f]{64}", manifest.get("replay_id", "")
            ) is None
            or expected_replay_id is not None
            and manifest.get("replay_id") != expected_replay_id
            or require_directory_identity
            and manifest.get("replay_id") != bundle.name
            or re.fullmatch(
                r"cohort:[0-9a-f]{64}",
                manifest.get("route_cohort_id", ""),
            ) is None
            or manifest.get("requested_notionals_usd") != _NOTIONALS
            or type(manifest.get("files")) is not dict
            or set(manifest["files"])
            != _HISTORICAL_COMPLETE_ARTIFACT_FILES
            or file_bytes[_HISTORICAL_REPLAY_EVIDENCE_FILENAME]
            != _json_file_bytes(evidence)
        ):
            raise HistoricalRoutePublicationError(
                "historical complete manifest schema differs"
            )
        for filename, details in manifest["files"].items():
            if (
                type(details) is not dict
                or set(details) != {
                    "schema", "sha256", "logical_sha256", "row_count"
                }
                or details.get("sha256") != file_hashes[filename]
            ):
                raise HistoricalRoutePublicationError(
                    "historical complete file checksum differs"
                )
        representation = {
            name: file_bytes[name]
            for name in _route_publication._COMPLETE_MANIFEST_ARTIFACT_FILENAMES
        }
        try:
            (
                logical_bundle, legs, costs, opportunities,
            ) = _route_publication._read_complete_representation_bytes(
                file_bytes=representation,
                route_cohort_id=manifest["route_cohort_id"],
            )
            logical_bundle = (
                _route_publication._validate_complete_logical_bundle_shared(
                    logical_bundle, historical_atomic=True
                )
            )
        except _route_publication.RoutePublicationError as error:
            raise HistoricalRoutePublicationError(
                "historical complete economic representation is invalid"
            ) from error
        if (
            legs != logical_bundle["legs"]
            or costs != logical_bundle["cost_components"]
            or opportunities != logical_bundle["opportunities"]
            or logical_bundle["route_cohort_id"]
            != manifest["route_cohort_id"]
            or logical_bundle["core_manifest_sha256"]
            != manifest["historical_core_manifest_sha256"]
            or logical_bundle["core_pointer_sha256"]
            != manifest["historical_core_pointer_sha256"]
        ):
            raise HistoricalRoutePublicationError(
                "historical complete economic lineage differs"
            )
        _validate_historical_replay_evidence_join(
            bundle=logical_bundle, evidence=evidence
        )
        _validate_historical_expected_pointer_core(
            expected=expected_pointer_core, manifest=manifest,
            manifest_sha256=manifest_sha256,
        )
        immutable_context = _load_immutable_historical_replay_core(
            data_dir=data,
            route_cohort_id=manifest["route_cohort_id"],
            expected_manifest_sha256=manifest[
                "historical_core_manifest_sha256"
            ],
            expected_pointer_sha256=manifest[
                "historical_core_pointer_sha256"
            ],
        )
        rebuilt = _build_historical_complete_payload(
            context=immutable_context
        )
        if (
            rebuilt["replay_id"] != manifest["replay_id"]
            or rebuilt["bundle"] != logical_bundle
            or rebuilt["replay_evidence"] != evidence
        ):
            raise HistoricalRoutePublicationError(
                "historical complete raw proof reconstruction differs"
            )
        expected_files = _historical_complete_file_descriptors(
            file_bytes={
                name: file_bytes[name]
                for name in _HISTORICAL_COMPLETE_ARTIFACT_FILES
            },
            bundle=logical_bundle, evidence=evidence,
        )
        expected_manifest = _historical_complete_manifest(
            payload=rebuilt, files=expected_files
        )
        if manifest != expected_manifest:
            raise HistoricalRoutePublicationError(
                "historical complete manifest content differs"
            )
        _route_publication._verify_bundle_file_snapshots(
            bundle_fd, read_specs, file_fds, file_details, file_bytes,
            file_hashes, _HISTORICAL_COMPLETE_FILES,
        )
        _route_publication._verify_directory_entry_snapshot(
            parent_fd, bundle.name, bundle_details,
            "historical complete bundle",
        )
        _route_publication._verify_open_path_identity(
            parent, parent_details, "historical complete bundles"
        )
        result = {
            "path": bundle,
            "manifest_sha256": manifest_sha256,
            "manifest": _freeze(manifest),
            "bundle": _freeze(logical_bundle),
            "legs": tuple(_freeze(row) for row in legs),
            "cost_components": tuple(_freeze(row) for row in costs),
            "opportunities": tuple(_freeze(row) for row in opportunities),
            "replay_evidence": _freeze(evidence),
            "database_path": bundle / "route_cohort.sqlite3",
        }
        if issue_view:
            view = view_issuer({
                "replay_id": manifest["replay_id"],
                "route_cohort_id": manifest["route_cohort_id"],
                "manifest_sha256": manifest_sha256,
                "data_dir": data, "raw_root": raw,
                "bundle_path": bundle,
                "parent_details": parent_details,
                "bundle_details": bundle_details,
                "read_specs": read_specs,
                "file_bytes": file_bytes,
                "file_hashes": file_hashes,
                "file_details": file_details,
                "manifest": manifest,
                "bundle": logical_bundle,
                "replay_evidence": evidence,
                "immutable_context": immutable_context,
            })
            immutable_context = None
            try:
                routes_by_id = {
                    row["route_id"]: row
                    for row in logical_bundle["routes"]
                }
                opportunities_by_id = {
                    row["opportunity_id"]: row
                    for row in logical_bundle["opportunities"]
                }
                costs_by_opportunity = {}
                for row in logical_bundle["cost_components"]:
                    costs_by_opportunity.setdefault(
                        row["opportunity_id"], []
                    ).append(row)
                for scenario in evidence["scenarios"]:
                    opportunity = opportunities_by_id.get(
                        scenario["opportunity_id"]
                    )
                    route = (
                        routes_by_id.get(opportunity["route_id"])
                        if opportunity is not None else None
                    )
                    scenario_rows = tuple(costs_by_opportunity.get(
                        scenario["opportunity_id"], ()
                    ))
                    if route is None:
                        raise HistoricalRoutePublicationError(
                            "historical published cost route differs"
                        )
                    _validate_historical_cost_rows_for_published_view(
                        validated_view=view,
                        scenario_key=scenario["scenario_key"],
                        route=route, rows=scenario_rows,
                    )
            except BaseException:
                try:
                    view.close()
                except Exception:
                    pass
                raise
            result["validated_view"] = view
        return MappingProxyType(result)
    except _route_publication.RoutePublicationError as error:
        raise HistoricalRoutePublicationError(
            "historical complete bundle loading failed"
        ) from error
    finally:
        try:
            if immutable_context is not None:
                immutable_context.close()
        finally:
            _close_descriptors_robustly(
                *tuple(file_fds.values()), bundle_fd, parent_fd
            )


(
    ValidatedHistoricalReplayBundleView,
    _validate_historical_replay_bundle,
    _historical_published_cost_proof_material,
    _historical_verification_subject_material,
) = _bind_historical_replay_bundle_view_runtime(
    validation_impl=_validate_historical_replay_bundle_impl,
    current_checker_function=(
        _validate_historical_replay_bundle_view_current
    ),
    scenario_material_reader=_historical_scenario_material,
)
del _bind_historical_replay_bundle_view_runtime
del _validate_historical_replay_bundle_impl


_historical_verification_subject_material.__name__ = (
    "_historical_verification_subject_material"
)

import scripts.historical_foundry_verifier as _historical_verifier

_issue_historical_verification_subject_from_view = (
    _historical_verifier._bind_historical_verification_subject_material(
        _historical_verification_subject_material
    )
)
del _historical_verifier._bind_historical_verification_subject_material


def validate_historical_replay_bundle(
    *, data_dir: Path, raw_root: Path, bundle_path: Path,
    expected_pointer_core: Optional[Mapping[str, Any]] = None,
) -> Mapping[str, Any]:
    validated = _validate_historical_replay_bundle(
        data_dir=data_dir, raw_root=raw_root, bundle_path=bundle_path,
        expected_pointer_core=expected_pointer_core,
        expected_replay_id=None,
        require_directory_identity=True,
        issue_view=True,
    )
    try:
        subject = _issue_historical_verification_subject_from_view(
            validated["validated_view"]
        )
        result = dict(validated)
        result["pointer_core"] = MappingProxyType(dict(
            _historical_verification_subject_material(
                validated_view=validated["validated_view"]
            )["pointer_core"]
        ))
        result["verification_subject"] = subject
        return MappingProxyType(result)
    except BaseException:
        try:
            validated["validated_view"].close()
        except Exception:
            pass
        raise


_load_historical_cost_proof_inputs_for_published_view = (
    _bind_historical_published_cost_proof_loader(
        _historical_published_cost_proof_material
    )
)


def _validate_historical_cost_rows_for_published_view(
    *, validated_view: ValidatedHistoricalReplayBundleView,
    scenario_key: str, route: Mapping[str, Any],
    rows: Tuple[Mapping[str, Any], ...],
) -> None:
    """Bind published cost rows to proof owned by the validated view."""
    from scripts.route_opportunity import route_opportunity_id

    proof_capability = _load_historical_cost_proof_inputs_for_published_view(
        validated_view=validated_view, scenario_key=scenario_key,
    )
    proof = _require_historical_cost_proof_owner(
        proof_capability, validated_view
    )
    try:
        block_text, direction, notional_text = scenario_key.split(":")
        block_number = int(block_text)
        notional = int(notional_text)
        proof_rows = proof["rows"]
        expected = {
            (row["grain"], row["component"]): row
            for row in proof_rows
        }
        expected_direction = (
            "uniswap_to_sushiswap"
            if ":uniswap_v2:" in route["buy_market_id"]
            and ":sushiswap_v2:" in route["sell_market_id"]
            else "sushiswap_to_uniswap"
            if ":sushiswap_v2:" in route["buy_market_id"]
            and ":uniswap_v2:" in route["sell_market_id"]
            else None
        )
        expected_keys = tuple(
            (leg, component_type)
            for leg, component_type, _status, _embedded
            in HISTORICAL_ATOMIC_COMPONENT_MATRIX
        )
        row_by_key = {
            (row["leg"], row["component_type"]): row for row in rows
        }
        ordered_rows = tuple(row_by_key[key] for key in expected_keys)
        opportunity_id = route_opportunity_id(
            route["route_id"], str(notional)
        )
    except (KeyError, TypeError, ValueError, AttributeError) as error:
        raise HistoricalRoutePublicationError(
            "historical published cost projection is invalid"
        ) from error
    if (
        type(scenario_key) is not str
        or str(block_number) != block_text
        or block_number <= 0
        or str(notional) != notional_text
        or notional <= 0
        or direction != expected_direction
        or type(route) is not dict
        or type(rows) is not tuple
        or len(rows) != len(HISTORICAL_ATOMIC_COMPONENT_MATRIX)
        or len(row_by_key) != len(HISTORICAL_ATOMIC_COMPONENT_MATRIX)
        or tuple(
            (row["grain"], row["component"])
            for row in proof_rows
        ) != expected_keys
        or proof["scenario_key"] != scenario_key
    ):
        raise HistoricalRoutePublicationError(
            "historical published cost ancestry differs"
        )
    try:
        _validate_historical_atomic_cost_component_matrix(
            route,
            ordered_rows,
            expected_cohort_id=validated_view.route_cohort_id,
            expected_opportunity_id=opportunity_id,
            expected_pool_fee_source_sha256_by_leg={
                leg: expected[(leg, "pool_swap_fee")]["proof_sha256"]
                for leg in ("buy", "sell")
            },
            expected_pool_fee_amount_usd_by_leg={
                leg: expected[(leg, "pool_swap_fee")][
                    "amount_usd_exact"
                ]
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
    except (KeyError, TypeError, ValueError) as error:
        raise HistoricalRoutePublicationError(
            "historical published cost proof differs"
        ) from error
    return None
