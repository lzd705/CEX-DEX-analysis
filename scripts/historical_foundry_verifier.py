"""Connected verification closure for immutable historical replay bundles.

The public surface accepts only a bundle-derived opaque subject.  Transport,
subprocess, and clock authority are installed by the Task-8 controller; when
that authority is absent, connected verification fails closed.
"""

from dataclasses import dataclass
from importlib.machinery import SourceFileLoader
from pathlib import Path
from types import MappingProxyType
from typing import Any, Mapping
from datetime import datetime, timezone
import errno
import fcntl
import gzip
import hashlib
import json
import os
import re
import stat
import sys
import weakref

import scripts.route_publication as _route_publication


class HistoricalVerificationError(ValueError):
    """Raised when connected verification or its immutable closure fails."""


_POINTER_SCHEMA = "route_historical_replay_pointer/v1"
_BUNDLE_STAGE = "route_historical_foundry_replay/v1"
_REPORT_SCHEMA = "route_historical_replay_verification/v1"
_HEX_SHA256 = re.compile(r"[0-9a-f]{64}")
_REPLAY_ID = re.compile(r"replay:[0-9a-f]{64}")
_COHORT_ID = re.compile(r"cohort:[0-9a-f]{64}")
_MAX_REPORT_BYTES = 8_388_608
_CONNECTED_OBSERVATION_SCHEMA = (
    "historical_foundry_connected_verification_observation/v1"
)
_CONNECTED_PROJECTION_SCHEMA = (
    "historical_foundry_connected_verification_projection/v1"
)
_CONNECTED_REQUEST_SCHEMA = (
    "historical_foundry_connected_verification_request/v1"
)
_REPORT_FIELDS = frozenset((
    "schema", "status", "evidence_mode", "pointer_core_sha256",
    "replay_id", "route_cohort_id", "manifest_sha256", "run_id",
    "run_manifest_sha256", "raw_run_projection_sha256",
    "policy_sha256", "authority_sha256", "toolchain_sha256",
    "historical_core_manifest_sha256",
    "historical_core_pointer_sha256", "replay_evidence_sha256",
    "connected_projection_sha256", "process_identity_sha256",
    "connection_identity_sha256", "verifier_source_sha256",
    "verifier_toolchain_sha256", "provider_identity_sha256",
    "coverage_sha256", "prefilter_grid_digest",
    "prefilter_rows_sha256", "safe_exclusions_sha256",
    "candidate_resolution_sha256",
    "verification_scenario_set_sha256",
    "verification_scenario_count",
    "verification_scenario_results_sha256", "selected_block",
    "published_scenarios_sha256", "started_at", "finished_at",
    "verification_id",
))
_AUDIT_FRESH_REPORT_FIELDS = frozenset((
    "verification_id", "process_identity_sha256",
    "connection_identity_sha256", "started_at", "finished_at",
))


def _initialize_trusted_source_module_code_reader():
    loader_type = SourceFileLoader
    get_data = SourceFileLoader.get_data
    source_to_code = SourceFileLoader.source_to_code

    def read(loader, module_name, expected_file):
        """Compile tracked source without dispatching through loader state."""
        if type(loader) is not loader_type or type(module_name) is not str:
            return None, None
        try:
            state = object.__getattribute__(loader, "__dict__")
        except AttributeError:
            return None, None
        if type(state) is not dict or len(state) != 2:
            return None, None
        exact_state = {}
        for key, value in state.items():
            if (
                type(key) is not str
                or key not in ("name", "path")
                or key in exact_state
                or type(value) is not str
            ):
                return None, None
            exact_state[key] = value
        if exact_state.get("name") != module_name:
            return None, None
        try:
            loaded_file = Path(exact_state["path"]).resolve()
            expected_file = expected_file.resolve()
            if loaded_file != expected_file:
                return None, None
            source = get_data(loader, str(expected_file))
            code = source_to_code(
                loader, source, str(expected_file),
            )
        except (
            AttributeError, ImportError, OSError, SyntaxError, TypeError,
            ValueError,
        ):
            return None, None
        return loaded_file, code

    return read


_trusted_source_module_code = (
    _initialize_trusted_source_module_code_reader()
)
del _initialize_trusted_source_module_code_reader


def _historical_bundle_invalid(error=None):
    if error is None:
        return HistoricalVerificationError("historical_bundle_invalid")
    return HistoricalVerificationError("historical_bundle_invalid")


def _canonical_bytes(value):
    return json.dumps(
        value, allow_nan=False, ensure_ascii=False, sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _sha256(value):
    return hashlib.sha256(value).hexdigest()


def _plain(value):
    if isinstance(value, Mapping):
        return {key: _plain(nested) for key, nested in value.items()}
    if isinstance(value, (list, tuple)):
        return [_plain(nested) for nested in value]
    return value


def _freeze(value):
    if isinstance(value, Mapping):
        return MappingProxyType({
            key: _freeze(nested) for key, nested in value.items()
        })
    if isinstance(value, list):
        return tuple(_freeze(nested) for nested in value)
    return value


def _decode_json(value, label):
    try:
        decoded = json.loads(value)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise _historical_bundle_invalid(error)
    if type(decoded) not in (dict, list):
        raise _historical_bundle_invalid()
    return decoded


def _rfc3339(value):
    if type(value) is not str:
        raise _historical_bundle_invalid()
    try:
        parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        try:
            parsed = datetime.strptime(value, "%Y-%m-%dT%H:%M:%SZ")
        except ValueError as error:
            raise _historical_bundle_invalid(error)
    return parsed.replace(tzinfo=timezone.utc)


def _same_inode(left, right):
    return left.st_dev == right.st_dev and left.st_ino == right.st_ino


def _write_all(descriptor, value):
    offset = 0
    while offset < len(value):
        written = os.write(descriptor, value[offset:])
        if written <= 0:
            raise OSError("short verification report write")
        offset += written


def _open_and_read_report_at(directory_fd, filename):
    descriptor, before = _route_publication._open_regular_file_at(
        directory_fd, filename, label="historical verification report",
    )
    try:
        value, digest, after = _route_publication._read_bounded_open_file(
            descriptor, before, limit=_MAX_REPORT_BYTES,
            label="historical verification report",
        )
        current = os.stat(
            filename, dir_fd=directory_fd, follow_symlinks=False,
        )
        if (
            not stat.S_ISREG(current.st_mode)
            or current.st_nlink != 1
            or current.st_uid != os.geteuid()
            or stat.S_IMODE(current.st_mode) != 0o600
            or after.st_uid != os.geteuid()
            or stat.S_IMODE(after.st_mode) != 0o600
            or not _same_inode(after, current)
            or _route_publication._stable_file_metadata(after)
            != _route_publication._stable_file_metadata(current)
            or os.get_inheritable(descriptor)
        ):
            raise _historical_bundle_invalid()
        return value, digest, after, descriptor
    except BaseException:
        os.close(descriptor)
        raise


def _read_report_at(directory_fd, filename):
    value, digest, details, descriptor = _open_and_read_report_at(
        directory_fd, filename
    )
    try:
        return value, digest, details
    finally:
        os.close(descriptor)


def _unlink_created_report_if_owned(directory_fd, filename, opened):
    try:
        current = os.stat(
            filename, dir_fd=directory_fd, follow_symlinks=False,
        )
    except OSError:
        return None
    if stat.S_ISREG(current.st_mode) and _same_inode(current, opened):
        try:
            os.unlink(filename, dir_fd=directory_fd)
            os.fsync(directory_fd)
        except OSError:
            pass
    return None


@dataclass(frozen=True)
class VerificationReportInstallResult:
    path: Path
    sha256: str
    size: int
    disposition: str


class _HeldVerificationReportInstall:
    __slots__ = (
        "root", "root_fd", "root_details", "by_sha_fd",
        "by_sha_details", "report_fd", "report_details", "filename",
        "digest", "size", "locked", "closed",
    )

    def __init__(
        self, *, root, root_fd, root_details, by_sha_fd, by_sha_details,
        report_fd, report_details, filename, digest, size,
    ):
        self.root = root
        self.root_fd = root_fd
        self.root_details = root_details
        self.by_sha_fd = by_sha_fd
        self.by_sha_details = by_sha_details
        self.report_fd = report_fd
        self.report_details = report_details
        self.filename = filename
        self.digest = digest
        self.size = size
        self.locked = True
        self.closed = False

    def reread_unchanged(self, expected):
        if self.closed or type(expected) is not bytes:
            raise _historical_bundle_invalid()
        before = os.fstat(self.report_fd)
        if (
            _route_publication._stable_file_metadata(before)
            != _route_publication._stable_file_metadata(
                self.report_details
            )
            or os.get_inheritable(self.report_fd)
        ):
            raise _historical_bundle_invalid()
        os.lseek(self.report_fd, 0, os.SEEK_SET)
        actual, digest, after = _route_publication._read_bounded_open_file(
            self.report_fd, before, limit=_MAX_REPORT_BYTES,
            label="historical verification report",
        )
        current = os.stat(
            self.filename, dir_fd=self.by_sha_fd,
            follow_symlinks=False,
        )
        if (
            actual != expected
            or digest != self.digest
            or len(actual) != self.size
            or self.filename != digest + ".json"
            or not stat.S_ISREG(after.st_mode)
            or after.st_nlink != 1
            or after.st_uid != os.geteuid()
            or stat.S_IMODE(after.st_mode) != 0o600
            or _route_publication._stable_file_metadata(after)
            != _route_publication._stable_file_metadata(
                self.report_details
            )
            or _route_publication._stable_file_metadata(current)
            != _route_publication._stable_file_metadata(
                self.report_details
            )
        ):
            raise _historical_bundle_invalid()
        _route_publication._verify_directory_entry_snapshot(
            self.root_fd, "by-sha256", self.by_sha_details,
            "historical verification by-sha256",
        )
        _route_publication._verify_open_path_snapshot(
            self.root, self.root_details,
            "historical verification root",
        )

    def close(self):
        if self.closed:
            return None
        self.closed = True
        failure = None
        if self.locked:
            try:
                fcntl.flock(self.by_sha_fd, fcntl.LOCK_UN)
            except OSError as error:
                failure = error
            self.locked = False
        for descriptor in (
            self.report_fd, self.by_sha_fd, self.root_fd,
        ):
            try:
                os.close(descriptor)
            except OSError as error:
                if failure is None:
                    failure = error
        if failure is not None:
            raise _historical_bundle_invalid(failure) from failure
        return None


def _reread_held_verification_report(held, expected):
    try:
        held.reread_unchanged(expected)
    except HistoricalVerificationError:
        raise
    except Exception as error:
        raise _historical_bundle_invalid(error) from error


def _install_historical_verification_report_held(
    *, verification_root: Path, report_bytes: bytes,
):
    if (
        not isinstance(verification_root, Path)
        or type(report_bytes) is not bytes
        or not 0 < len(report_bytes) <= _MAX_REPORT_BYTES
        or not hasattr(os, "O_NOFOLLOW")
        or not hasattr(os, "O_CLOEXEC")
    ):
        raise _historical_bundle_invalid()
    digest = _sha256(report_bytes)
    filename = digest + ".json"
    root_fd = by_sha_fd = descriptor = report_fd = None
    created_details = None
    created = False
    locked = False
    transferred = False
    try:
        root = _route_publication._ensure_real_directory(verification_root)
        root, root_fd, root_details = (
            _route_publication._open_verified_directory(
                root, "historical verification root"
            )
        )
        by_sha_fd, by_sha_details = _route_publication._ensure_directory_at(
            root_fd, "by-sha256", "historical verification by-sha256"
        )
        os.fsync(root_fd)
        fcntl.flock(by_sha_fd, fcntl.LOCK_EX)
        locked = True
        flags = (
            os.O_RDWR | os.O_CREAT | os.O_EXCL
            | os.O_NOFOLLOW | os.O_CLOEXEC
        )
        try:
            descriptor = os.open(
                filename, flags, 0o600, dir_fd=by_sha_fd
            )
        except OSError as error:
            if error.errno != errno.EEXIST:
                raise
            disposition = "matched_existing"
        else:
            created = True
            created_details = os.fstat(descriptor)
            created_path = os.stat(
                filename, dir_fd=by_sha_fd, follow_symlinks=False,
            )
            if (
                not stat.S_ISREG(created_details.st_mode)
                or not stat.S_ISREG(created_path.st_mode)
                or created_details.st_nlink != 1
                or created_path.st_nlink != 1
                or created_details.st_uid != os.geteuid()
                or created_path.st_uid != os.geteuid()
                or stat.S_IMODE(created_details.st_mode) != 0o600
                or stat.S_IMODE(created_path.st_mode) != 0o600
                or not _same_inode(created_details, created_path)
                or os.get_inheritable(descriptor)
            ):
                raise _historical_bundle_invalid()
            _write_all(descriptor, report_bytes)
            os.fsync(descriptor)
            written = os.fstat(descriptor)
            written_path = os.stat(
                filename, dir_fd=by_sha_fd, follow_symlinks=False,
            )
            if (
                not _same_inode(created_details, written)
                or written.st_size != len(report_bytes)
                or not stat.S_ISREG(written.st_mode)
                or written.st_nlink != 1
                or written.st_uid != os.geteuid()
                or stat.S_IMODE(written.st_mode) != 0o600
                or _route_publication._stable_file_metadata(written)
                != _route_publication._stable_file_metadata(written_path)
                or os.get_inheritable(descriptor)
            ):
                raise _historical_bundle_invalid()
            os.lseek(descriptor, 0, os.SEEK_SET)
            observed, observed_sha256, reread_details = (
                _route_publication._read_bounded_open_file(
                    descriptor, written, limit=_MAX_REPORT_BYTES,
                    label="historical verification report",
                )
            )
            if (
                observed != report_bytes
                or observed_sha256 != digest
                or _route_publication._stable_file_metadata(
                    reread_details
                ) != _route_publication._stable_file_metadata(written_path)
            ):
                raise _historical_bundle_invalid()
            created_details = reread_details
            os.close(descriptor)
            descriptor = None
            os.fsync(by_sha_fd)
            disposition = "created"

        (
            actual, physical_sha256, read_details, report_fd,
        ) = _open_and_read_report_at(
            by_sha_fd, filename
        )
        if (
            actual != report_bytes
            or len(actual) != len(report_bytes)
            or physical_sha256 != digest
            or filename != physical_sha256 + ".json"
            or created and not _same_inode(created_details, read_details)
        ):
            raise _historical_bundle_invalid()
        root_details = os.fstat(root_fd)
        by_sha_details = os.fstat(by_sha_fd)
        _route_publication._verify_directory_entry_snapshot(
            root_fd, "by-sha256", by_sha_details,
            "historical verification by-sha256",
        )
        _route_publication._verify_open_path_snapshot(
            root, root_details, "historical verification root"
        )
        result = VerificationReportInstallResult(
            path=root / "by-sha256" / filename,
            sha256=digest, size=len(actual), disposition=disposition,
        )
        held = _HeldVerificationReportInstall(
            root=root, root_fd=root_fd, root_details=root_details,
            by_sha_fd=by_sha_fd, by_sha_details=by_sha_details,
            report_fd=report_fd, report_details=read_details,
            filename=filename, digest=digest, size=len(actual),
        )
        transferred = True
        return result, held
    except BaseException as error:
        if created and by_sha_fd is not None and created_details is not None:
            _unlink_created_report_if_owned(
                by_sha_fd, filename, created_details
            )
        if not isinstance(error, Exception):
            raise
        if isinstance(error, HistoricalVerificationError):
            raise
        raise _historical_bundle_invalid(error) from error
    finally:
        if not transferred and locked and by_sha_fd is not None:
            try:
                fcntl.flock(by_sha_fd, fcntl.LOCK_UN)
            except OSError:
                pass
        for value in (descriptor, report_fd, by_sha_fd, root_fd):
            if transferred and value in (report_fd, by_sha_fd, root_fd):
                continue
            if value is not None:
                try:
                    os.close(value)
                except OSError:
                    pass


def install_historical_verification_report(
    *, verification_root: Path, report_bytes: bytes,
) -> VerificationReportInstallResult:
    """Install one content-addressed report without replacing any object."""
    result, held = _install_historical_verification_report_held(
        verification_root=verification_root, report_bytes=report_bytes,
    )
    failure = None
    try:
        _reread_held_verification_report(held, report_bytes)
        return result
    except BaseException as error:
        failure = error
        raise
    finally:
        try:
            held.close()
        except Exception as close_error:
            if failure is None:
                raise
            raise failure from close_error


def historical_replay_pointer_core(
    pointer: Mapping[str, Any],
) -> Mapping[str, Any]:
    """Return the exact final pointer with only its report hash removed."""
    if (
        not isinstance(pointer, Mapping)
        or set(pointer) != {
            "schema", "bundle_stage", "replay_id", "route_cohort_id",
            "manifest_sha256", "verification_report_sha256",
        }
        or pointer.get("schema") != _POINTER_SCHEMA
        or pointer.get("bundle_stage") != _BUNDLE_STAGE
        or type(pointer.get("replay_id")) is not str
        or _REPLAY_ID.fullmatch(pointer["replay_id"]) is None
        or type(pointer.get("route_cohort_id")) is not str
        or _COHORT_ID.fullmatch(pointer["route_cohort_id"]) is None
        or type(pointer.get("manifest_sha256")) is not str
        or _HEX_SHA256.fullmatch(pointer["manifest_sha256"]) is None
        or type(pointer.get("verification_report_sha256")) is not str
        or _HEX_SHA256.fullmatch(
            pointer["verification_report_sha256"]
        ) is None
    ):
        raise HistoricalVerificationError(
            "historical replay pointer is invalid"
        )
    return MappingProxyType({
        key: pointer[key]
        for key in (
            "schema", "bundle_stage", "replay_id", "route_cohort_id",
            "manifest_sha256",
        )
    })


def _initialize_historical_verification_subject():
    issuer = object()
    installed = [False]
    trusted_source_module_code = _trusted_source_module_code

    def close_view_silently(view):
        try:
            view.close()
        except Exception:
            pass

    class HistoricalVerificationSubject:
        __slots__ = ("_validated_record", "_view_finalizer", "__weakref__")

        def __new__(cls, *args, **kwargs):
            del cls, args, kwargs
            raise HistoricalVerificationError(
                "historical verification subject construction is private"
            )

        def __repr__(self):
            return "HistoricalVerificationSubject(<redacted>)"

        def __setattr__(self, name, value):
            del name, value
            raise HistoricalVerificationError(
                "historical verification subject is immutable"
            )

        def __delattr__(self, name):
            del name
            raise HistoricalVerificationError(
                "historical verification subject is immutable"
            )

        def __reduce_ex__(self, protocol):
            del protocol
            raise TypeError(
                "historical verification subject is not serializable"
            )

        def identity_projection(self):
            record = require(self)
            material = record["material"]
            return MappingProxyType({
                "schema": "historical_verification_subject_identity/v1",
                "replay_id": material["pointer_core"]["replay_id"],
                "route_cohort_id": material["pointer_core"][
                    "route_cohort_id"
                ],
                "manifest_sha256": material["pointer_core"][
                    "manifest_sha256"
                ],
                "pointer_core_sha256": _sha256(_canonical_bytes(
                    material["pointer_core"]
                )),
            })

        def reread_unchanged(self):
            record = require(self)
            record["material"]["validated_view"].reread_unchanged()

        def close(self):
            record = require(self)
            finalizer = object.__getattribute__(self, "_view_finalizer")
            record["material"]["validated_view"].close()
            finalizer.detach()
            object.__setattr__(self, "_validated_record", None)
            object.__setattr__(self, "_view_finalizer", None)

    def require(value):
        if type(value) is not HistoricalVerificationSubject:
            raise HistoricalVerificationError(
                "historical verification subject is invalid"
            )
        try:
            owner = object.__getattribute__(value, "_validated_record")
            finalizer = object.__getattribute__(value, "_view_finalizer")
        except AttributeError as error:
            raise HistoricalVerificationError(
                "historical verification subject is invalid"
            ) from error
        if (
            type(owner) is not MappingProxyType
            or set(owner) != {"owner_reference", "payload"}
            or type(owner.get("owner_reference"))
            is not weakref.ReferenceType
            or owner["owner_reference"]() is not value
            or type(owner.get("payload")) is not MappingProxyType
            or owner["payload"].get("issuer") is not issuer
            or type(finalizer) is not weakref.finalize
            or not finalizer.alive
        ):
            raise HistoricalVerificationError(
                "historical verification subject is invalid"
            )
        record = owner["payload"]
        state = finalizer.peek()
        if (
            set(record) != {"issuer", "material"}
            or state is None
            or state[0] is not value
            or state[1] is not close_view_silently
            or state[2] != (record["material"]["validated_view"],)
            or state[3] != {}
        ):
            raise HistoricalVerificationError(
                "historical verification subject is invalid"
            )
        return record

    def issue(material):
        if type(material) is not dict:
            raise HistoricalVerificationError(
                "historical verification subject material is invalid"
            )
        value = object.__new__(HistoricalVerificationSubject)
        frozen_material = _freeze(material)
        payload = MappingProxyType({
            "issuer": issuer, "material": frozen_material,
        })
        owner = MappingProxyType({
            "owner_reference": weakref.ref(value), "payload": payload,
        })
        finalizer = weakref.finalize(
            value, close_view_silently,
            frozen_material["validated_view"],
        )
        try:
            object.__setattr__(value, "_validated_record", owner)
            object.__setattr__(value, "_view_finalizer", finalizer)
            require(value)
        except BaseException:
            finalizer.detach()
            raise
        return value

    def bind_material_reader(material_reader):
        publication_name = "scripts.historical_route_publication"
        publication = sys.modules.get(publication_name)
        namespace = getattr(material_reader, "__globals__", None)
        spec = getattr(publication, "__spec__", None)
        loader = getattr(spec, "loader", None)
        expected_file = Path(__file__).with_name(
            "historical_route_publication.py"
        ).resolve()
        try:
            caller = sys._getframe(1)
        except (AttributeError, ValueError):
            caller = None
        loaded_file, trusted_module_code = trusted_source_module_code(
            loader, publication_name, expected_file,
        )
        if (
            installed[0]
            or not callable(material_reader)
            or publication is None
            or material_reader is not getattr(
                publication,
                "_historical_verification_subject_material",
                None,
            )
            or namespace is not getattr(publication, "__dict__", None)
            or spec is None
            or getattr(spec, "name", None) != publication_name
            or type(loader) is not SourceFileLoader
            or loaded_file != expected_file
            or caller is None
            or caller.f_globals is not namespace
            or caller.f_code.co_name != "<module>"
            or caller.f_code != trusted_module_code
            or getattr(material_reader, "__name__", None)
            != "_historical_verification_subject_material"
        ):
            raise HistoricalVerificationError(
                "historical verification subject binder is invalid"
            )

        def issue_from_view(validated_view):
            material = material_reader(validated_view=validated_view)
            if (
                type(material) is not dict
                or set(material) != {
                    "validated_view", "data_dir", "raw_root",
                    "bundle_path", "manifest", "bundle",
                    "replay_evidence", "pointer_core",
                }
                or material.get("validated_view") is not validated_view
            ):
                raise HistoricalVerificationError(
                    "historical verification subject material is invalid"
                )
            return issue(material)

        installed[0] = True
        return issue_from_view

    return (
        HistoricalVerificationSubject, require, bind_material_reader,
    )


(
    HistoricalVerificationSubject,
    _require_historical_verification_subject,
    _bind_historical_verification_subject_material,
) = _initialize_historical_verification_subject()
del _initialize_historical_verification_subject


def _initialize_connected_historical_verification_engine():
    engine = None
    engine_kind = None
    trusted_source_module_code = _trusted_source_module_code

    def bind(value):
        """One-shot production bind used only by the Task-8 entrypoint."""
        nonlocal engine, engine_kind
        module_name = getattr(value, "__module__", None)
        module = sys.modules.get(module_name)
        namespace = getattr(value, "__globals__", None)
        spec = getattr(module, "__spec__", None)
        loader = getattr(spec, "loader", None)
        if module_name == "scripts.run_historical_foundry_replay":
            expected_file = Path(__file__).with_name(
                "run_historical_foundry_replay.py"
            ).resolve()
        else:
            expected_file = None
        try:
            caller = sys._getframe(1)
        except (AttributeError, ValueError):
            caller = None
        loaded_file, trusted_module_code = trusted_source_module_code(
            loader, module_name, expected_file,
        ) if expected_file is not None else (None, None)
        authentic_call_edge = (
            caller is not None
            and caller.f_globals is namespace
            and module_name == "scripts.run_historical_foundry_replay"
            and caller.f_code.co_name == "<module>"
            and caller.f_code == trusted_module_code
        )
        if (
            engine is not None
            or not callable(value)
            or module is None
            or namespace is not getattr(module, "__dict__", None)
            or value is not getattr(module, getattr(value, "__name__", ""), None)
            or spec is None
            or getattr(spec, "name", None) != module_name
            or type(loader) is not SourceFileLoader
            or expected_file is None
            or loaded_file != expected_file.resolve()
            or not authentic_call_edge
            or getattr(value, "__name__", "").startswith("<")
        ):
            raise HistoricalVerificationError(
                "historical connected engine binder is invalid"
            )
        engine = value
        engine_kind = "production_connected"
        globals().pop(
            "_bind_connected_historical_verification_engine", None
        )

    def invoke(request):
        if engine is None:
            raise HistoricalVerificationError(
                "historical connected authority is unavailable"
            )
        return engine_kind, engine(request)

    return bind, invoke


(
    _bind_connected_historical_verification_engine,
    _invoke_connected_historical_verification_engine,
) = _initialize_connected_historical_verification_engine()
del _initialize_connected_historical_verification_engine
del _trusted_source_module_code


def _source_member(source, members, relative_path):
    descriptor = members.get(relative_path)
    if (
        type(descriptor) is not dict
        or set(descriptor) != {"path", "byte_count", "sha256"}
        or descriptor.get("path") != relative_path
    ):
        raise _historical_bundle_invalid()
    try:
        value = source.read_member(
            relative_path,
            expected_sha256=descriptor["sha256"],
            max_bytes=max(descriptor["byte_count"], 1),
        )
    except Exception as error:
        raise _historical_bundle_invalid(error)
    if (
        len(value) != descriptor["byte_count"]
        or _sha256(value) != descriptor["sha256"]
    ):
        raise _historical_bundle_invalid()
    return value


def _decoded_chunk(source, members, descriptor):
    if (
        type(descriptor) is not dict
        or type(descriptor.get("path")) is not str
        or type(descriptor.get("decoded_byte_count")) is not int
        or descriptor["decoded_byte_count"] <= 0
        or descriptor["decoded_byte_count"] > 16_777_216
        or type(descriptor.get("decoded_sha256")) is not str
        or _HEX_SHA256.fullmatch(descriptor["decoded_sha256"]) is None
    ):
        raise _historical_bundle_invalid()
    compressed = _source_member(source, members, descriptor["path"])
    try:
        decoded = gzip.decompress(compressed)
    except (OSError, EOFError) as error:
        raise _historical_bundle_invalid(error)
    if (
        len(decoded) != descriptor["decoded_byte_count"]
        or _sha256(decoded) != descriptor["decoded_sha256"]
    ):
        raise _historical_bundle_invalid()
    rows = _decode_json(decoded, "historical connected chunk")
    if type(rows) is not list:
        raise _historical_bundle_invalid()
    return rows


def _typed_resolution(
    *, source, members, scenario_key, block_number,
):
    prefix = "foundry/{}/{}".format(block_number, scenario_key)
    resolution = {}
    for role, suffix in (
        ("overlay", "overlay.json"),
        ("receipt", "receipt.json"),
        ("trace", "trace.json.gz"),
        ("result", "result.json"),
    ):
        relative_path = prefix + "/" + suffix
        physical = _source_member(source, members, relative_path)
        if role == "trace":
            try:
                physical = gzip.decompress(physical)
            except (OSError, EOFError) as error:
                raise _historical_bundle_invalid(error)
        typed = _decode_json(
            physical, "historical connected scenario {}".format(role)
        )
        resolution[role + "_typed_sha256"] = _sha256(
            _canonical_bytes(typed)
        )
    return {
        "scenario_key": scenario_key,
        "resolution": resolution,
    }


def _close_connected_source(source):
    source.close()


def _build_retained_connected_projection(material):
    import scripts.historical_foundry_replay as replay
    import scripts.historical_foundry_storage as storage
    import scripts.historical_route_publication as publication
    from scripts.historical_foundry_contracts import (
        load_historical_foundry_config_set,
    )

    if (
        not isinstance(material, Mapping)
        or not isinstance(material.get("data_dir"), Path)
        or not isinstance(material.get("raw_root"), Path)
        or not isinstance(material.get("bundle_path"), Path)
        or not isinstance(material.get("manifest"), Mapping)
        or not isinstance(material.get("bundle"), Mapping)
        or not isinstance(material.get("replay_evidence"), Mapping)
        or not isinstance(material.get("pointer_core"), Mapping)
    ):
        raise _historical_bundle_invalid()
    view = material.get("validated_view")
    if view is not None:
        try:
            view.reread_unchanged()
        except Exception as error:
            raise _historical_bundle_invalid(error)
    manifest = _plain(material["manifest"])
    pointer_core = _plain(material["pointer_core"])
    replay_evidence = _plain(material["replay_evidence"])
    if (
        pointer_core != {
            "schema": _POINTER_SCHEMA,
            "bundle_stage": _BUNDLE_STAGE,
            "replay_id": manifest.get("replay_id"),
            "route_cohort_id": manifest.get("route_cohort_id"),
            "manifest_sha256": pointer_core.get("manifest_sha256"),
        }
        or pointer_core["manifest_sha256"]
        != _sha256(publication._json_file_bytes(manifest))
    ):
        raise _historical_bundle_invalid()
    run_id = manifest.get("run_id")
    run_manifest_sha256 = manifest.get("run_manifest_sha256")
    try:
        source = storage.open_validated_run(
            data_dir=material["data_dir"], run_id=run_id,
            expected_manifest_sha256=run_manifest_sha256,
        )
    except Exception as error:
        raise _historical_bundle_invalid(error)
    failure = None
    try:
        config = load_historical_foundry_config_set()
        evidence, _source_identity_sha256 = (
            publication._build_run_evidence_from_source(
                config=config, source=source
            )
        )
        validated = replay.validate_selected_historical_run(
            config=config, run_evidence=evidence
        )
        identity = dict(source.identity_projection())
        run_manifest_bytes = source.read_member(
            "run_manifest.json",
            expected_sha256=identity["run_manifest_sha256"],
            max_bytes=8_388_608,
        )
        run_manifest = _decode_json(
            run_manifest_bytes, "historical connected run manifest"
        )
        member_rows = run_manifest.get("members")
        if type(member_rows) is not list:
            raise _historical_bundle_invalid()
        members = {
            row.get("path"): row for row in member_rows
            if type(row) is dict
        }
        if len(members) != len(member_rows):
            raise _historical_bundle_invalid()
        selection = _decode_json(
            _source_member(source, members, "selection.json"),
            "historical connected selection",
        )
        candidate = _decode_json(
            _source_member(source, members, "candidate_manifest.json"),
            "historical connected candidates",
        )
        capture = _decode_json(
            _source_member(
                source, members, "scan/capture_inventory.json"
            ),
            "historical connected capture inventory",
        )
        prefilter_inventory = _decode_json(
            _source_member(
                source, members, "scan/prefilter_inventory.json"
            ),
            "historical connected prefilter inventory",
        )
        capture_rows = {
            "headers": [], "reserves": [], "prices": [], "fees": [],
        }
        descriptors = capture.get("typed_chunks")
        if type(descriptors) is not list:
            raise _historical_bundle_invalid()
        for descriptor in descriptors:
            role = descriptor.get("role") if type(descriptor) is dict else None
            if role in capture_rows:
                capture_rows[role].extend(
                    _decoded_chunk(source, members, descriptor)
                )
        if any(not capture_rows[role] for role in capture_rows):
            raise _historical_bundle_invalid()
        prefilter_descriptors = prefilter_inventory.get(
            "prefilter_chunks"
        )
        if type(prefilter_descriptors) is not list or not prefilter_descriptors:
            raise _historical_bundle_invalid()
        prefilter_rows = []
        for descriptor in prefilter_descriptors:
            prefilter_rows.extend(
                _decoded_chunk(source, members, descriptor)
            )
        selected_block = selection.get("selected_block")
        selected_scenarios = selection.get("selected_scenarios")
        if (
            type(selected_block) is not dict
            or type(selected_block.get("number")) is not int
            or type(selected_scenarios) is not list
            or len(selected_scenarios) != 10
        ):
            raise _historical_bundle_invalid()
        selected_number = selected_block["number"]
        selected_keys = {
            row.get("scenario_key") for row in selected_scenarios
            if type(row) is dict
        }
        if len(selected_keys) != 10:
            raise _historical_bundle_invalid()
        scenario_keys = []
        for row in prefilter_rows:
            if type(row) is not dict:
                raise _historical_bundle_invalid()
            key = row.get("scenario_key")
            if (
                row.get("block_number") == selected_number
                or row.get("block_number", -1) > selected_number
                and row.get("decision") == "replay_required"
            ):
                scenario_keys.append(key)
        if (
            len(scenario_keys) != len(set(scenario_keys))
            or not selected_keys.issubset(set(scenario_keys))
        ):
            raise _historical_bundle_invalid()
        safe_exclusions = [
            row for row in prefilter_rows
            if row.get("decision") == "safe_excluded"
        ]
        results = []
        row_by_key = {
            row["scenario_key"]: row for row in prefilter_rows
        }
        for scenario_key in scenario_keys:
            row = row_by_key.get(scenario_key)
            if row is None:
                raise _historical_bundle_invalid()
            results.append(_typed_resolution(
                source=source, members=members,
                scenario_key=scenario_key,
                block_number=row["block_number"],
            ))
        published_scenarios = replay_evidence.get(
            "scenarios"
        )
        if (
            type(published_scenarios) is not list
            or len(published_scenarios) != 10
            or {row.get("scenario_key") for row in published_scenarios}
            != selected_keys
            or validated["run_id"] != run_id
            or validated["manifest_sha256"] != run_manifest_sha256
        ):
            raise _historical_bundle_invalid()
        projection = {
            "schema": _CONNECTED_PROJECTION_SCHEMA,
            "run_id": run_id,
            "run_manifest_sha256": run_manifest_sha256,
            "raw_run_projection_sha256": _sha256(_canonical_bytes(
                _plain(validated)
            )),
            "policy_sha256": manifest.get("policy_sha256"),
            "authority_sha256": manifest.get("authority_sha256"),
            "toolchain_sha256": manifest.get("toolchain_sha256"),
            "historical_core_manifest_sha256": manifest.get(
                "historical_core_manifest_sha256"
            ),
            "historical_core_pointer_sha256": manifest.get(
                "historical_core_pointer_sha256"
            ),
            "replay_id": manifest.get("replay_id"),
            "route_cohort_id": manifest.get("route_cohort_id"),
            "manifest_sha256": pointer_core["manifest_sha256"],
            "replay_evidence_sha256": _sha256(_canonical_bytes(
                replay_evidence
            )),
            "pointer_core": _plain(pointer_core),
            "pointer_core_sha256": _sha256(_canonical_bytes(pointer_core)),
            "capture_rows": capture_rows,
            "capture_rows_sha256": _sha256(_canonical_bytes(capture_rows)),
            "prefilter_grid_digest": selection.get(
                "prefilter_grid_digest"
            ),
            "prefilter_rows": prefilter_rows,
            "prefilter_rows_sha256": _sha256(_canonical_bytes(
                prefilter_rows
            )),
            "safe_exclusions": safe_exclusions,
            "safe_exclusions_sha256": _sha256(_canonical_bytes(
                safe_exclusions
            )),
            "verification_scenario_keys": scenario_keys,
            "verification_scenario_set_sha256": _sha256(_canonical_bytes(
                scenario_keys
            )),
            "scenario_results": results,
            "scenario_results_sha256": _sha256(_canonical_bytes(results)),
            "selection_status": selection.get("status"),
            "selected_block": selected_block,
            "candidate_states": selection.get("candidate_states"),
            "candidate_resolution_sha256": _sha256(_canonical_bytes({
                "candidate_manifest": candidate,
                "candidate_states": selection.get("candidate_states"),
            })),
            "published_scenarios_sha256": _sha256(_canonical_bytes(
                published_scenarios
            )),
        }
        source.reread_unchanged()
        if view is not None:
            view.reread_unchanged()
        return projection
    except HistoricalVerificationError as error:
        failure = error
        raise
    except Exception as error:
        failure = _historical_bundle_invalid(error)
        raise failure from error
    except BaseException as error:
        failure = error
        raise
    finally:
        try:
            _close_connected_source(source)
        except Exception as close_error:
            if failure is None:
                raise _historical_bundle_invalid(close_error) from close_error
            raise failure from close_error


def _connected_request_for_subject(subject):
    record = _require_historical_verification_subject(subject)
    material = record["material"]
    subject.reread_unchanged()
    return {
        "schema": _CONNECTED_REQUEST_SCHEMA,
        "data_dir": material["data_dir"],
        "raw_root": material["raw_root"],
        "bundle_path": material["bundle_path"],
        "pointer_core": _plain(material["pointer_core"]),
    }


def _build_connected_observation_for_retained_fixture(request):
    """Local-process fixture adapter; it never claims an external RPC fetch."""
    import scripts.historical_route_publication as publication

    if (
        type(request) is not dict
        or set(request) != {
            "schema", "data_dir", "raw_root", "bundle_path",
            "pointer_core",
        }
        or request.get("schema") != _CONNECTED_REQUEST_SCHEMA
        or not isinstance(request.get("data_dir"), Path)
        or not isinstance(request.get("raw_root"), Path)
        or not isinstance(request.get("bundle_path"), Path)
        or type(request.get("pointer_core")) is not dict
    ):
        raise _historical_bundle_invalid()
    validated = publication._validate_historical_replay_bundle(
        data_dir=request["data_dir"], raw_root=request["raw_root"],
        bundle_path=request["bundle_path"],
        expected_pointer_core=request["pointer_core"],
        expected_replay_id=request["pointer_core"]["replay_id"],
        require_directory_identity=True, issue_view=False,
    )
    material = {
        "validated_view": None,
        "data_dir": request["data_dir"],
        "raw_root": request["raw_root"],
        "bundle_path": request["bundle_path"],
        "manifest": _plain(validated["manifest"]),
        "bundle": _plain(validated["bundle"]),
        "replay_evidence": _plain(validated["replay_evidence"]),
        "pointer_core": _plain(request["pointer_core"]),
    }
    started = datetime.now(timezone.utc)
    projection = _build_retained_connected_projection(material)
    finished = datetime.now(timezone.utc)
    connection_nonce = os.urandom(32)
    try:
        source_bytes = Path(__file__).read_bytes()
    except OSError as error:
        raise _historical_bundle_invalid(error)
    return {
        "schema": _CONNECTED_OBSERVATION_SCHEMA,
        "evidence_mode": "offline_test_fixture",
        "fresh_process": True,
        "fresh_connection": True,
        "process_id": os.getpid(),
        "process_identity_sha256": _sha256(
            str(os.getpid()).encode("ascii")
        ),
        "connection_identity_sha256": _sha256(connection_nonce),
        "provider_identity_sha256": _sha256(
            ("offline_test_fixture:" + projection["run_id"]).encode(
                "utf-8"
            )
        ),
        "verifier_source_sha256": _sha256(source_bytes),
        "verifier_toolchain_sha256": projection["toolchain_sha256"],
        "started_at": started.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "finished_at": finished.strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "projection": projection,
    }


def _validate_connected_historical_observation(*, subject, observation):
    _require_historical_verification_subject(subject)
    if (
        type(observation) is not dict
        or set(observation) != {
            "schema", "evidence_mode", "fresh_process",
            "fresh_connection", "process_id", "process_identity_sha256",
            "connection_identity_sha256", "provider_identity_sha256",
            "verifier_source_sha256", "verifier_toolchain_sha256",
            "started_at", "finished_at", "projection",
        }
        or observation.get("schema") != _CONNECTED_OBSERVATION_SCHEMA
        or observation.get("evidence_mode") not in (
            "offline_test_fixture", "production_connected",
        )
        or observation.get("fresh_process") is not True
        or observation.get("fresh_connection") is not True
        or type(observation.get("process_id")) is not int
        or observation["process_id"] <= 0
        or observation["process_id"] == os.getpid()
        or observation.get("process_identity_sha256") != _sha256(
            str(observation["process_id"]).encode("ascii")
        )
        or any(
            type(observation.get(field)) is not str
            or _HEX_SHA256.fullmatch(observation[field]) is None
            for field in (
                "process_identity_sha256", "connection_identity_sha256",
                "provider_identity_sha256", "verifier_source_sha256",
                "verifier_toolchain_sha256",
            )
        )
    ):
        raise _historical_bundle_invalid()
    started = _rfc3339(observation["started_at"])
    finished = _rfc3339(observation["finished_at"])
    if finished < started:
        raise _historical_bundle_invalid()
    expected = _build_retained_connected_projection(
        _require_historical_verification_subject(subject)["material"]
    )
    if observation.get("projection") != expected:
        raise _historical_bundle_invalid()
    subject.reread_unchanged()
    return observation


def _verification_report(observation):
    projection = observation["projection"]
    base = {
        "schema": _REPORT_SCHEMA,
        "status": (
            "verified"
            if observation["evidence_mode"] == "production_connected"
            else "structurally_validated"
        ),
        "evidence_mode": observation["evidence_mode"],
        "pointer_core_sha256": projection["pointer_core_sha256"],
        "replay_id": projection["replay_id"],
        "route_cohort_id": projection["route_cohort_id"],
        "manifest_sha256": projection["manifest_sha256"],
        "run_id": projection["run_id"],
        "run_manifest_sha256": projection["run_manifest_sha256"],
        "raw_run_projection_sha256": projection[
            "raw_run_projection_sha256"
        ],
        "policy_sha256": projection["policy_sha256"],
        "authority_sha256": projection["authority_sha256"],
        "toolchain_sha256": projection["toolchain_sha256"],
        "historical_core_manifest_sha256": projection[
            "historical_core_manifest_sha256"
        ],
        "historical_core_pointer_sha256": projection[
            "historical_core_pointer_sha256"
        ],
        "replay_evidence_sha256": projection[
            "replay_evidence_sha256"
        ],
        "connected_projection_sha256": _sha256(_canonical_bytes(
            projection
        )),
        "process_identity_sha256": observation[
            "process_identity_sha256"
        ],
        "connection_identity_sha256": observation[
            "connection_identity_sha256"
        ],
        "verifier_source_sha256": observation[
            "verifier_source_sha256"
        ],
        "verifier_toolchain_sha256": observation[
            "verifier_toolchain_sha256"
        ],
        "provider_identity_sha256": observation[
            "provider_identity_sha256"
        ],
        "coverage_sha256": projection["capture_rows_sha256"],
        "prefilter_grid_digest": projection["prefilter_grid_digest"],
        "prefilter_rows_sha256": projection["prefilter_rows_sha256"],
        "safe_exclusions_sha256": projection[
            "safe_exclusions_sha256"
        ],
        "candidate_resolution_sha256": projection[
            "candidate_resolution_sha256"
        ],
        "verification_scenario_set_sha256": projection[
            "verification_scenario_set_sha256"
        ],
        "verification_scenario_count": len(projection[
            "verification_scenario_keys"
        ]),
        "verification_scenario_results_sha256": projection[
            "scenario_results_sha256"
        ],
        "selected_block": projection["selected_block"],
        "published_scenarios_sha256": projection[
            "published_scenarios_sha256"
        ],
        "started_at": observation["started_at"],
        "finished_at": observation["finished_at"],
    }
    verification_id = "verification:" + _sha256(_canonical_bytes(base))
    return {**base, "verification_id": verification_id}


def _validate_exact_historical_verification_report(report_bytes):
    if type(report_bytes) is not bytes or not 0 < len(
        report_bytes
    ) <= _MAX_REPORT_BYTES:
        raise _historical_bundle_invalid()
    try:
        report = json.loads(report_bytes)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise _historical_bundle_invalid(error)
    base = dict(report) if type(report) is dict else None
    verification_id = (
        base.pop("verification_id", None) if base is not None else None
    )
    if (
        base is None
        or set(report) != _REPORT_FIELDS
        or _canonical_bytes(report) != report_bytes
        or report.get("schema") != _REPORT_SCHEMA
        or (
            report.get("status"), report.get("evidence_mode")
        ) not in (
            ("verified", "production_connected"),
            ("structurally_validated", "offline_test_fixture"),
        )
        or verification_id
        != "verification:" + _sha256(_canonical_bytes(base))
        or type(report.get("verification_scenario_count")) is not int
        or report["verification_scenario_count"] < 0
        or not isinstance(report.get("selected_block"), Mapping)
        or type(report.get("replay_id")) is not str
        or _REPLAY_ID.fullmatch(report["replay_id"]) is None
        or type(report.get("route_cohort_id")) is not str
        or _COHORT_ID.fullmatch(report["route_cohort_id"]) is None
        or type(report.get("run_id")) is not str
        or re.fullmatch(r"run:[0-9a-f]{64}", report["run_id"]) is None
        or any(
            type(report.get(field)) is not str
            or _HEX_SHA256.fullmatch(report[field]) is None
            for field in _REPORT_FIELDS
            if field.endswith("_sha256")
        )
    ):
        raise _historical_bundle_invalid()
    started = _rfc3339(report["started_at"])
    finished = _rfc3339(report["finished_at"])
    if finished < started:
        raise _historical_bundle_invalid()
    return MappingProxyType(report)


def _validate_retained_historical_verification_report(
    *, report_bytes, pointer_core,
):
    if not isinstance(pointer_core, Mapping):
        raise _historical_bundle_invalid()
    report = _validate_exact_historical_verification_report(report_bytes)
    pointer = _plain(pointer_core)
    if (
        report.get("status") != "verified"
        or report.get("evidence_mode") != "production_connected"
        or report.get("pointer_core_sha256")
        != _sha256(_canonical_bytes(pointer))
        or report.get("replay_id") != pointer.get("replay_id")
        or report.get("route_cohort_id") != pointer.get("route_cohort_id")
        or report.get("manifest_sha256")
        != pointer.get("manifest_sha256")
    ):
        raise _historical_bundle_invalid()
    return report


def _require_historical_audit_report_parity(
    *, retained_report_bytes, audit_report,
):
    retained = _validate_exact_historical_verification_report(
        retained_report_bytes
    )
    if not isinstance(audit_report, Mapping):
        raise _historical_bundle_invalid()
    audit = _validate_exact_historical_verification_report(
        _canonical_bytes(_plain(audit_report))
    )
    if (
        retained.get("status") != "verified"
        or retained.get("evidence_mode") != "production_connected"
        or audit.get("status") != "verified"
        or audit.get("evidence_mode") != "production_connected"
        or {
            key: retained[key]
            for key in _REPORT_FIELDS - _AUDIT_FRESH_REPORT_FIELDS
        }
        != {
            key: audit[key]
            for key in _REPORT_FIELDS - _AUDIT_FRESH_REPORT_FIELDS
        }
    ):
        raise _historical_bundle_invalid()
    return None


def _require_historical_verification_mode(mode):
    if type(mode) is not str or mode not in ("staged", "publish", "audit"):
        raise HistoricalVerificationError(
            "historical verification mode is invalid"
        )
    return mode


def run_connected_historical_verification(
    subject: "HistoricalVerificationSubject", *, mode: str,
) -> Mapping[str, Any]:
    """Verify a sealed subject without selecting a block or moving a pointer."""
    record = _require_historical_verification_subject(subject)
    mode = _require_historical_verification_mode(mode)
    subject.reread_unchanged()
    request = _connected_request_for_subject(subject)
    try:
        engine_result = _invoke_connected_historical_verification_engine(
            request
        )
    except Exception as error:
        raise _historical_bundle_invalid(error) from error
    if (
        type(engine_result) is not tuple
        or len(engine_result) != 2
        or engine_result[0] not in (
            "offline_test_fixture", "production_connected",
        )
        or type(engine_result[1]) is not dict
        or engine_result[1].get("evidence_mode") != engine_result[0]
    ):
        raise _historical_bundle_invalid()
    engine_kind, observation = engine_result
    observation = _validate_connected_historical_observation(
        subject=subject, observation=observation,
    )
    if (
        engine_kind != observation["evidence_mode"]
        or mode in ("publish", "audit")
        and engine_kind != "production_connected"
    ):
        raise _historical_bundle_invalid()
    report = _verification_report(observation)
    report_bytes = _canonical_bytes(report)
    try:
        if json.loads(report_bytes) != report:
            raise _historical_bundle_invalid()
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise _historical_bundle_invalid(error)
    report_sha256 = _sha256(report_bytes)
    if engine_kind == "production_connected":
        _validate_retained_historical_verification_report(
            report_bytes=report_bytes,
            pointer_core=record["material"]["pointer_core"],
        )
    install_result = None
    held_install = None
    failure = None
    try:
        if mode == "publish":
            verification_root = (
                record["material"]["data_dir"] / "routes" / "historical"
                / "verifications"
            )
            install_result, held_install = (
                _install_historical_verification_report_held(
                    verification_root=verification_root,
                    report_bytes=report_bytes,
                )
            )
            _reread_held_verification_report(
                held_install, report_bytes
            )
            subject.reread_unchanged()
        pointer_core = _plain(record["material"]["pointer_core"])
        final_pointer = {
            **pointer_core,
            "verification_report_sha256": report_sha256,
        }
        if dict(historical_replay_pointer_core(final_pointer)) != pointer_core:
            raise _historical_bundle_invalid()
        final_pointer_bytes = _canonical_bytes(final_pointer)
        if json.loads(final_pointer_bytes) != final_pointer:
            raise _historical_bundle_invalid()
        if held_install is not None:
            _reread_held_verification_report(
                held_install, report_bytes
            )
        subject.reread_unchanged()
        return MappingProxyType({
            "schema": "historical_connected_verification_result/v1",
            "mode": mode,
            "report": _freeze(report),
            "report_bytes": report_bytes,
            "report_sha256": report_sha256,
            "pointer_core": _freeze(pointer_core),
            "final_pointer": _freeze(final_pointer),
            "final_pointer_bytes": final_pointer_bytes,
            "install_result": install_result,
        })
    except BaseException as error:
        failure = error
        raise
    finally:
        if held_install is not None:
            try:
                held_install.close()
            except Exception as close_error:
                if failure is None:
                    raise
                raise failure from close_error
