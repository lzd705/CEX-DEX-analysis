"""Bootstrap and hold the reviewed historical Foundry toolchain.

This module has exactly two production entrypoints.  Bootstrap is an explicit
operator action; importing it, the dashboard, or the server performs no I/O.
"""

from __future__ import annotations

import base64
import binascii
import ctypes
import errno
import hashlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import signal
import ssl
import stat
import subprocess
import sys
import tarfile
import tempfile
import threading
import time
from types import MappingProxyType
from typing import Any, Dict, Mapping, Optional, Sequence, Tuple
import urllib.request


_PROJECT_ROOT = Path(__file__).resolve().parents[1]

_FOUNDRY_VERSION = "v1.7.1"
_FOUNDRY_RELEASE_COMMIT = "4072e48705af9d93e3c0f6e29e93b5e9a40caed8"
_FOUNDRY_ARCHIVE_URL = "https://github.com/foundry-rs/foundry/releases/download/v1.7.1/foundry_v1.7.1_darwin_arm64.tar.gz"
_FOUNDRY_ARCHIVE_SHA256 = "eacdc67718fac857cad9e19c7f6729dd80de731d09df81856391d093cfcab547"
_FOUNDRY_CHECKSUM_URL = "https://github.com/foundry-rs/foundry/releases/download/v1.7.1/foundry_v1.7.1_darwin_arm64.sha256"
_FOUNDRY_CHECKSUM_SHA256 = "91b21b7f96cfad4e40a0ef18077777c5732e244ed795d476e5bcd153e18e4b5c"
_FOUNDRY_SIGSTORE_URL = "https://github.com/foundry-rs/foundry/releases/download/v1.7.1/foundry_v1.7.1_darwin_arm64.sigstore.json"
_FOUNDRY_SIGSTORE_SHA256 = "d5930109b48c43a968ce8c0b2068c7d43e973a2b2604eb590a48c4c74a52159e"
_FOUNDRY_SPDX_URL = "https://github.com/foundry-rs/foundry/releases/download/v1.7.1/foundry_v1.7.1_darwin_arm64.spdx.json"
_FOUNDRY_SPDX_SHA256 = "2a20a6956e75c08ba5b6aa2acbf62d5236b998bf58be00b7561d68af5aa0de0b"
_SIGSTORE_ISSUER = "https://token.actions.githubusercontent.com"
_SIGSTORE_SAN = "https://github.com/foundry-rs/foundry/.github/workflows/release.yml@refs/tags/v1.7.1"

_SOLC_VERSION = "0.8.36+commit.8a079791"
_SOLC_SOURCE_COMMIT = "8a079791d9cca7a6c03fd6a8429b93aa3bddefed"
_SOLC_URL = "https://binaries.soliditylang.org/macosx-amd64/solc-macosx-amd64-v0.8.36+commit.8a079791"
_SOLC_SHA256 = "d4abcf0b3e24b7948ddfd64c374d26c3214648717777790ecb936979054a129d"

_FORGE_STD_VERSION = "v1.16.1"
_FORGE_STD_REPOSITORY = "https://github.com/foundry-rs/forge-std.git"
_FORGE_STD_COMMIT = "620536fa5277db4e3fd46772d5cbc1ea0696fb43"

_COMPILER_SETTINGS = {
    "append_cbor": False,
    "bytecode_hash": "none",
    "cbor_metadata": False,
    "evm_version": "osaka",
    "fork_hardfork": "osaka",
    "optimizer_enabled": True,
    "optimizer_runs": 200,
    "via_ir": False,
}
_REVIEWED_FIXED_WINDOW_HARDFORK_PROJECTION = MappingProxyType({
    "anchor_hardfork": "osaka",
    "lower_bound_hardfork": "osaka",
})

_SOURCE_TABLE = {
    "compiler_settings": _COMPILER_SETTINGS,
    "forge_std": {
        "commit": _FORGE_STD_COMMIT,
        "repository_url": _FORGE_STD_REPOSITORY,
        "version": _FORGE_STD_VERSION,
    },
    "foundry_release": {
        "archive_sha256": _FOUNDRY_ARCHIVE_SHA256,
        "archive_url": _FOUNDRY_ARCHIVE_URL,
        "checksum_sha256": _FOUNDRY_CHECKSUM_SHA256,
        "checksum_url": _FOUNDRY_CHECKSUM_URL,
        "release_commit": _FOUNDRY_RELEASE_COMMIT,
        "sigstore_issuer": _SIGSTORE_ISSUER,
        "sigstore_san": _SIGSTORE_SAN,
        "sigstore_sha256": _FOUNDRY_SIGSTORE_SHA256,
        "sigstore_url": _FOUNDRY_SIGSTORE_URL,
        "spdx_sha256": _FOUNDRY_SPDX_SHA256,
        "spdx_url": _FOUNDRY_SPDX_URL,
        "version": _FOUNDRY_VERSION,
    },
    "solc": {
        "artifact_sha256": _SOLC_SHA256,
        "artifact_url": _SOLC_URL,
        "source_commit": _SOLC_SOURCE_COMMIT,
        "version": _SOLC_VERSION,
    },
}


def _canonical_json_bytes(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
            allow_nan=False,
        ).encode("utf-8")
    except (TypeError, ValueError, OverflowError) as error:
        raise HistoricalFoundryToolchainError("toolchain_identity_invalid") from error


_SOURCE_LOCK_SHA256 = hashlib.sha256(
    b"historical_foundry_toolchain_source_lock/v1\n"
    + _canonical_json_bytes(_SOURCE_TABLE)
    + b"\n"
).hexdigest()

_FOUNDRY_TOML = """[profile.default]
src = "foundry/src"
test = "foundry/test"
script = "foundry/script"
out = "out"
cache_path = "cache"
libs = ["lib"]
solc = ".historical-foundry/toolchains/{source_lock}/bin/solc"
auto_detect_solc = false
offline = true
optimizer = true
optimizer_runs = 200
via_ir = false
evm_version = "osaka"
bytecode_hash = "none"
cbor_metadata = false
append_cbor = false
ffi = false
""".format(source_lock=_SOURCE_LOCK_SHA256).encode("utf-8")

_FOUNDRY_LOCK = """version = 1

[[dependencies]]
name = "forge-std"
source = "git"
repository = "https://github.com/foundry-rs/forge-std.git"
version = "v1.16.1"
rev = "620536fa5277db4e3fd46772d5cbc1ea0696fb43"
""".encode("utf-8")

_GITMODULES = """[submodule "lib/forge-std"]
\tpath = lib/forge-std
\turl = https://github.com/foundry-rs/forge-std.git
""".encode("utf-8")

_REVIEWED_PROJECT_FILES = MappingProxyType({
    "foundry.toml": _FOUNDRY_TOML,
    "foundry.lock": _FOUNDRY_LOCK,
    ".gitmodules": _GITMODULES,
})

_REVIEWED_ASSET_SHA256 = MappingProxyType({
    _FOUNDRY_ARCHIVE_URL: _FOUNDRY_ARCHIVE_SHA256,
    _FOUNDRY_CHECKSUM_URL: _FOUNDRY_CHECKSUM_SHA256,
    _FOUNDRY_SIGSTORE_URL: _FOUNDRY_SIGSTORE_SHA256,
    _FOUNDRY_SPDX_URL: _FOUNDRY_SPDX_SHA256,
    _SOLC_URL: _SOLC_SHA256,
})
_ASSET_FILENAMES = MappingProxyType({
    _FOUNDRY_ARCHIVE_URL: "foundry.tar.gz",
    _FOUNDRY_CHECKSUM_URL: "foundry.sha256",
    _FOUNDRY_SIGSTORE_URL: "foundry.sigstore.json",
    _FOUNDRY_SPDX_URL: "foundry.spdx.json",
    _SOLC_URL: "solc",
})

# Independently extracted from the exact hash-verified v1.7.1 archive by the
# connected bootstrap.  solc retains its separately reviewed artifact hash.
_EXPECTED_BINARY_SHA256: Mapping[str, str] = MappingProxyType({
    "anvil": "5c9f9aad323062b1c0421a63595741430acaea150da3611e38c45071e4cf4e28",
    "cast": "eb9a9dc730a0f178556b90d39a30212375ee6e7c754fee96fa95b2723878e220",
    "forge": "e729589084ca2f1479354353d1ec3d4789451b577f4cdee4e7dc57cae64a38fa",
})
_FORGE_STD_TREE_SHA256 = "b20e3e90b1aab4acb1295e9d107c95a224441d272e6e479e9de153a9f3f64ab5"

_BINARY_NAMES = ("forge", "cast", "anvil", "solc")
_FOUNDRY_BINARY_NAMES = ("forge", "cast", "anvil")
_HASH = re.compile(r"[0-9a-f]{64}\Z", re.ASCII)
_MAX_DOWNLOAD_BYTES = 256 * 1024 * 1024
_MAX_SIDECAR_BYTES = 16 * 1024 * 1024
_MAX_PROCESS_OUTPUT = 4 * 1024 * 1024
_MAX_EXECUTOR_ARTIFACT_BYTES = 16 * 1024 * 1024
_EXECUTOR_SOURCE = "foundry/src/TwoVenueV2Executor.sol"
_EXECUTOR_ARTIFACT_DIRECTORY = ("out", "TwoVenueV2Executor.sol")
_EXECUTOR_ARTIFACT_NAME = "TwoVenueV2Executor.json"
_KAT_FIXTURE_DIRECTORIES = ("tests", "fixtures")
_KAT_FIXTURE_NAME = "historical_foundry_kat.json"
_KAT_FORK_SOURCE = "foundry/test/TwoVenueV2Fork.t.sol"
_KAT_FORK_SOURCE_SHA256 = (
    "4950fe86ca1c177112fc0db7d920b2963d6d7109f7c328d25f5d257c698bc4de"
)
_MAX_KAT_FIXTURE_BYTES = 64 * 1024
_REVIEWED_HISTORICAL_FOUNDRY_KAT_BYTES = (
    b'{"archive_calls":[{"block_reference":"0x17d7840","calldata":"0x0902f1ac","method":"getReserves()","raw_response":"0x0000000000000000000000000000000000000000000051e38767437fac1d4c0f00000000000000000000000000000000000000000000001d6f8183a4807354760000000000000000000000000000000000000000000000000000000069f49013","response_sha256":"204e4b1706f10e75947b770017a684d4c3379a17dbd1ea54851f447544f58461","role":"uniswap_v2_uni_weth_reserves","target":"0xd3d2e2692501a5c9ca623199d38826e513033a17"},'
    b'{"block_reference":"0x17d7840","calldata":"0x0902f1ac","method":"getReserves()","raw_response":"0x0000000000000000000000000000000000000000000000bd762b5d69a8be9e1700000000000000000000000000000000000000000000000044406e0af95d0c040000000000000000000000000000000000000000000000000000000069f47c0f","response_sha256":"7411473045715ec073ac3cc12a47475135f8de7883f59ec9d49657e083d06e33","role":"sushiswap_v2_uni_weth_reserves","target":"0xdafd66636e2561b0284edde37e42d192f2844d40"},'
    b'{"block_reference":"0x17d7840","calldata":"0xfeaf968c","method":"latestRoundData()","raw_response":"0x000000000000000000000000000000000000000000000007000000000000701e000000000000000000000000000000000000000000000000000000353848f6320000000000000000000000000000000000000000000000000000000069f4963f0000000000000000000000000000000000000000000000000000000069f4964f000000000000000000000000000000000000000000000007000000000000701e","response_sha256":"e6b59059a6b3440c906a9a24b007a64b965977f2b99e746105f98ed1af5376ad","role":"chainlink_eth_usd_latest_round","target":"0x5f4ec3df9cbd43714fe2740f5e3616155c5b8419"}],'
    b'"block_header":{"base_fee":"0x478d0e7f","gas_limit":"0x3938700","gas_used":"0x2035c7b","hash":"0xf398976165ca4756c77fc6b61111fa1102d431eb03082417ecce38b36308d728","number_decimal":25000000,"number_hex":"0x17d7840","parent_hash":"0xc5a79102dcb47469ef357021c974bbbb92df3a1f3cfbcb5fdc0f9b36fb75e2c7","state_root":"0x055eba2b2b3daa967118fe831b0988cb27434e274f97f66cc67dcaa16dbe417f","timestamp_hex":"0x69f497f3","timestamp_utc":"2026-05-01T12:09:23Z"},'
    b'"chain_id":1,"pair_identities":[{"pair_address":"0xd3d2e2692501a5c9ca623199d38826e513033a17","venue_id":"uniswap_v2"},{"pair_address":"0xdafd66636e2561b0284edde37e42d192f2844d40","venue_id":"sushiswap_v2"}],"schema":"historical_foundry_kat/v1"}\n'
)


def _sealed_solc_argument() -> str:
    return str(
        _PROJECT_ROOT / ".historical-foundry" / "toolchains"
        / _SOURCE_LOCK_SHA256 / "bin" / "solc"
    )


class HistoricalFoundryToolchainError(RuntimeError):
    """Stable fail-closed toolchain boundary error."""


def _error(reason: str) -> HistoricalFoundryToolchainError:
    return HistoricalFoundryToolchainError(reason)


def _hash_bytes(payload: bytes) -> str:
    return hashlib.sha256(payload).hexdigest()


def _stable_metadata(metadata: os.stat_result) -> Tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_size,
        metadata.st_mode,
        metadata.st_uid,
        metadata.st_gid,
        metadata.st_nlink,
        getattr(metadata, "st_mtime_ns", int(metadata.st_mtime * 1000000000)),
        getattr(metadata, "st_ctime_ns", int(metadata.st_ctime * 1000000000)),
    )


def _directory_identity(metadata: os.stat_result) -> Tuple[int, ...]:
    return (
        metadata.st_dev,
        metadata.st_ino,
        metadata.st_mode,
        metadata.st_uid,
        metadata.st_gid,
    )


def _directory_identity_from_stable(metadata: Tuple[int, ...]) -> Tuple[int, ...]:
    return (metadata[0], metadata[1], metadata[3], metadata[4], metadata[5])


def _secure_file_flags() -> int:
    nofollow = getattr(os, "O_NOFOLLOW", None)
    if nofollow is None:
        raise _error("toolchain_nofollow_unavailable")
    return os.O_RDONLY | nofollow | getattr(os, "O_CLOEXEC", 0)


def _secure_directory_flags() -> int:
    directory = getattr(os, "O_DIRECTORY", None)
    if directory is None:
        raise _error("toolchain_nofollow_unavailable")
    return _secure_file_flags() | directory


def _forge_std_directory_entries(
    directory_fd: int,
    relative_prefix: str,
) -> Sequence[Mapping[str, Any]]:
    try:
        names = sorted(os.listdir(directory_fd))
    except OSError as error:
        raise _error("forge_std_tree_invalid") from error
    entries = []
    for name in names:
        if (
            not isinstance(name, str)
            or not name
            or name in {".", ".."}
            or "/" in name
            or "\x00" in name
            or (not relative_prefix and name == ".git")
        ):
            if not relative_prefix and name == ".git":
                continue
            raise _error("forge_std_tree_invalid")
        relative = name if not relative_prefix else relative_prefix + "/" + name
        try:
            path_metadata = os.stat(
                name, dir_fd=directory_fd, follow_symlinks=False
            )
        except OSError as error:
            raise _error("forge_std_tree_invalid") from error
        if stat.S_ISDIR(path_metadata.st_mode):
            try:
                child_fd = os.open(
                    name, _secure_directory_flags(), dir_fd=directory_fd
                )
            except OSError as error:
                raise _error("forge_std_tree_invalid") from error
            try:
                descriptor = os.fstat(child_fd)
                before = _stable_metadata(descriptor)
                if _stable_metadata(path_metadata) != before:
                    raise _error("forge_std_tree_invalid")
                entries.extend(_forge_std_directory_entries(child_fd, relative))
                after = os.fstat(child_fd)
                after_path = os.stat(
                    name, dir_fd=directory_fd, follow_symlinks=False
                )
                if (
                    _stable_metadata(after) != before
                    or _stable_metadata(after_path) != before
                ):
                    raise _error("forge_std_tree_invalid")
            except OSError as error:
                raise _error("forge_std_tree_invalid") from error
            finally:
                os.close(child_fd)
            continue
        if not stat.S_ISREG(path_metadata.st_mode) or path_metadata.st_nlink != 1:
            raise _error("forge_std_tree_invalid")
        try:
            file_fd = os.open(name, _secure_file_flags(), dir_fd=directory_fd)
        except OSError as error:
            raise _error("forge_std_tree_invalid") from error
        try:
            descriptor = os.fstat(file_fd)
            before = _stable_metadata(descriptor)
            if (
                _stable_metadata(path_metadata) != before
                or descriptor.st_uid != os.getuid()
            ):
                raise _error("forge_std_tree_invalid")
            payload = _read_fd(file_fd, _MAX_SIDECAR_BYTES)
            after = os.fstat(file_fd)
            after_path = os.stat(
                name, dir_fd=directory_fd, follow_symlinks=False
            )
            if (
                _stable_metadata(after) != before
                or _stable_metadata(after_path) != before
            ):
                raise _error("forge_std_tree_invalid")
            entries.append({
                "mode": format(stat.S_IMODE(descriptor.st_mode), "04o"),
                "path": relative,
                "sha256": _hash_bytes(payload),
                "size": len(payload),
            })
        except OSError as error:
            raise _error("forge_std_tree_invalid") from error
        finally:
            os.close(file_fd)
    return entries


def _forge_std_tree_sha256() -> str:
    root = _PROJECT_ROOT / "lib" / "forge-std"
    try:
        path_metadata = os.stat(str(root), follow_symlinks=False)
        root_fd = os.open(str(root), _secure_directory_flags())
    except OSError as error:
        raise _error("forge_std_tree_invalid") from error
    try:
        descriptor = os.fstat(root_fd)
        before = _stable_metadata(descriptor)
        if (
            not stat.S_ISDIR(descriptor.st_mode)
            or descriptor.st_uid != os.getuid()
            or _stable_metadata(path_metadata) != before
        ):
            raise _error("forge_std_tree_invalid")
        entries = _forge_std_directory_entries(root_fd, "")
        after = os.fstat(root_fd)
        after_path = os.stat(str(root), follow_symlinks=False)
        if (
            _stable_metadata(after) != before
            or _stable_metadata(after_path) != before
        ):
            raise _error("forge_std_tree_invalid")
    except OSError as error:
        raise _error("forge_std_tree_invalid") from error
    finally:
        os.close(root_fd)
    return hashlib.sha256(
        b"historical_foundry_forge_std_tree/v1\n"
        + _canonical_json_bytes(entries)
        + b"\n"
    ).hexdigest()


def _read_fd(fd: int, maximum: int) -> bytes:
    chunks = []
    offset = 0
    while True:
        chunk = os.pread(fd, min(1024 * 1024, maximum + 1 - offset), offset)
        if not chunk:
            return b"".join(chunks)
        chunks.append(chunk)
        offset += len(chunk)
        if offset > maximum:
            raise _error("toolchain_member_too_large")


def _reject_kat_duplicate_keys(
    pairs: Sequence[Tuple[str, Any]],
) -> Dict[str, Any]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise _error("connected_kat_fixture_invalid")
        result[key] = value
    return result


def _validate_reviewed_historical_foundry_kat_bytes(
    payload: bytes,
) -> Mapping[str, Any]:
    """Validate the sole reviewed fixed-block KAT fixture bytes."""
    if not isinstance(payload, bytes) or payload != _REVIEWED_HISTORICAL_FOUNDRY_KAT_BYTES:
        raise _error("connected_kat_fixture_invalid")
    parse_failed = False
    try:
        value = json.loads(
            payload[:-1].decode("utf-8"),
            object_pairs_hook=_reject_kat_duplicate_keys,
            parse_constant=lambda _token: (_ for _ in ()).throw(
                _error("connected_kat_fixture_invalid")
            ),
        )
    except HistoricalFoundryToolchainError:
        parse_failed = True
    except (UnicodeDecodeError, json.JSONDecodeError, TypeError, ValueError):
        parse_failed = True
    if parse_failed:
        raise _error("connected_kat_fixture_invalid")
    validation_failed = False
    try:
        header = value["block_header"]
        calls = value["archive_calls"]
        block_timestamp = int(header["timestamp_hex"], 16)
        decoded_rows = []
        for index, row in enumerate(calls):
            raw_text = row["raw_response"]
            if re.fullmatch(r"0x[0-9a-f]+", raw_text) is None:
                raise _error("connected_kat_fixture_invalid")
            if _hash_bytes(raw_text.encode("ascii")) != row["response_sha256"]:
                raise _error("connected_kat_fixture_invalid")
            raw = bytes.fromhex(raw_text[2:])
            expected_size = 96 if index < 2 else 160
            if len(raw) != expected_size:
                raise _error("connected_kat_fixture_invalid")
            decoded_rows.append(
                tuple(
                    int.from_bytes(raw[offset : offset + 32], "big")
                    for offset in range(0, len(raw), 32)
                )
            )
        for reserve0, reserve1, pair_timestamp in decoded_rows[:2]:
            if (
                reserve0 <= 0
                or reserve1 <= 0
                or pair_timestamp <= 0
                or pair_timestamp >= 2 ** 32
                or pair_timestamp > block_timestamp
            ):
                raise _error("connected_kat_fixture_invalid")
        round_id, answer, started_at, updated_at, answered_in_round = decoded_rows[2]
        if (
            round_id <= 0
            or answer <= 0
            or started_at <= 0
            or updated_at < started_at
            or updated_at > block_timestamp
            or answered_in_round < round_id
        ):
            raise _error("connected_kat_fixture_invalid")
    except HistoricalFoundryToolchainError:
        validation_failed = True
    except (KeyError, TypeError, ValueError, OverflowError):
        validation_failed = True
    if validation_failed:
        raise _error("connected_kat_fixture_invalid")
    return json.loads(_canonical_json_bytes(value).decode("utf-8"))


def _load_reviewed_historical_foundry_kat() -> Mapping[str, Any]:
    """Descriptor-reread the one fixed repository KAT member."""
    root_fd = None
    directory_chain = []
    fixture_fd = None
    payload = None
    failure_reason = None
    try:
        root_path_metadata = os.stat(str(_PROJECT_ROOT), follow_symlinks=False)
        root_fd = os.open(str(_PROJECT_ROOT), _secure_directory_flags())
        root_metadata = _stable_metadata(os.fstat(root_fd))
        if (
            not stat.S_ISDIR(os.fstat(root_fd).st_mode)
            or _stable_metadata(root_path_metadata) != root_metadata
        ):
            raise _error("connected_kat_fixture_unsafe")
        current_fd = root_fd
        for name in _KAT_FIXTURE_DIRECTORIES:
            child_fd, metadata = _open_project_directory(current_fd, name)
            directory_chain.append((child_fd, current_fd, name, metadata))
            current_fd = child_fd
        fixture_fd, fixture_metadata, first = _open_project_file(
            current_fd,
            _KAT_FIXTURE_NAME,
            _MAX_KAT_FIXTURE_BYTES,
        )
        second = _read_fd(fixture_fd, _MAX_KAT_FIXTURE_BYTES)
        if first != second:
            raise _error("connected_kat_fixture_changed")
        _assert_project_member_stable(
            current_fd, _KAT_FIXTURE_NAME, fixture_fd, fixture_metadata
        )
        for fd, parent_fd, name, metadata in directory_chain:
            _assert_project_member_stable(parent_fd, name, fd, metadata)
        if (
            _stable_metadata(os.fstat(root_fd)) != root_metadata
            or _stable_metadata(
                os.stat(str(_PROJECT_ROOT), follow_symlinks=False)
            )
            != root_metadata
        ):
            raise _error("connected_kat_fixture_changed")
        payload = first
    except HistoricalFoundryToolchainError:
        failure_reason = "connected_kat_fixture_unsafe"
    except OSError:
        failure_reason = "connected_kat_fixture_unavailable"
    finally:
        if fixture_fd is not None:
            os.close(fixture_fd)
        for fd, _parent_fd, _name, _metadata in reversed(directory_chain):
            os.close(fd)
        if root_fd is not None:
            os.close(root_fd)
    if failure_reason is not None:
        raise _error(failure_reason)
    if payload is None:
        raise _error("connected_kat_fixture_unavailable")
    return _validate_reviewed_historical_foundry_kat_bytes(payload)


def _hash_fd(fd: int) -> str:
    digest = hashlib.sha256()
    offset = 0
    while True:
        chunk = os.pread(fd, 1024 * 1024, offset)
        if not chunk:
            return digest.hexdigest()
        digest.update(chunk)
        offset += len(chunk)
        if offset > _MAX_DOWNLOAD_BYTES:
            raise _error("toolchain_binary_too_large")


def _read_stable_file(path: Path, maximum: int = _MAX_SIDECAR_BYTES) -> bytes:
    try:
        before_path = os.stat(str(path), follow_symlinks=False)
        fd = os.open(str(path), _secure_file_flags())
    except OSError as error:
        raise _error("toolchain_project_input_unavailable") from error
    try:
        before_fd = os.fstat(fd)
        if (
            not stat.S_ISREG(before_fd.st_mode)
            or before_fd.st_nlink != 1
            or _stable_metadata(before_path) != _stable_metadata(before_fd)
        ):
            raise _error("toolchain_project_input_unsafe")
        payload = _read_fd(fd, maximum)
        after_fd = os.fstat(fd)
        after_path = os.stat(str(path), follow_symlinks=False)
        if (
            _stable_metadata(before_fd) != _stable_metadata(after_fd)
            or _stable_metadata(before_fd) != _stable_metadata(after_path)
        ):
            raise _error("toolchain_project_input_unstable")
        return payload
    except OSError as error:
        raise _error("toolchain_project_input_unstable") from error
    finally:
        os.close(fd)


def _open_project_directory(parent_fd: int, name: str) -> Tuple[int, Tuple[int, ...]]:
    try:
        path_metadata = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        fd = os.open(name, _secure_directory_flags(), dir_fd=parent_fd)
        descriptor = os.fstat(fd)
    except OSError as error:
        raise _error("executor_artifact_unsafe") from error
    metadata = _stable_metadata(descriptor)
    if (
        not stat.S_ISDIR(descriptor.st_mode)
        or descriptor.st_uid != os.getuid()
        or stat.S_IMODE(descriptor.st_mode) & 0o022
        or _stable_metadata(path_metadata) != metadata
    ):
        os.close(fd)
        raise _error("executor_artifact_unsafe")
    return fd, metadata


def _open_project_file(
    parent_fd: int,
    name: str,
    maximum: int,
) -> Tuple[int, Tuple[int, ...], bytes]:
    try:
        path_metadata = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        fd = os.open(name, _secure_file_flags(), dir_fd=parent_fd)
        descriptor = os.fstat(fd)
    except OSError as error:
        raise _error("executor_artifact_unsafe") from error
    metadata = _stable_metadata(descriptor)
    if (
        not stat.S_ISREG(descriptor.st_mode)
        or descriptor.st_nlink != 1
        or descriptor.st_uid != os.getuid()
        or stat.S_IMODE(descriptor.st_mode) & 0o022
        or _stable_metadata(path_metadata) != metadata
    ):
        os.close(fd)
        raise _error("executor_artifact_unsafe")
    try:
        payload = _read_fd(fd, maximum)
        if (
            _stable_metadata(os.fstat(fd)) != metadata
            or _stable_metadata(
                os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            ) != metadata
        ):
            raise _error("executor_artifact_changed")
    except OSError as error:
        os.close(fd)
        raise _error("executor_artifact_changed") from error
    return fd, metadata, payload


def _assert_project_member_stable(
    parent_fd: int,
    name: str,
    fd: int,
    metadata: Tuple[int, ...],
) -> None:
    try:
        if (
            _stable_metadata(os.fstat(fd)) != metadata
            or _stable_metadata(
                os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
            ) != metadata
        ):
            raise _error("executor_artifact_changed")
    except OSError as error:
        raise _error("executor_artifact_changed") from error


def _remove_generated_tree(parent_fd: int, name: str) -> None:
    try:
        metadata = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
    except FileNotFoundError:
        return
    except OSError as error:
        raise _error("foundry_clean_failed") from error
    if not stat.S_ISDIR(metadata.st_mode) or metadata.st_uid != os.getuid():
        raise _error("foundry_clean_failed")
    try:
        directory_fd = os.open(name, _secure_directory_flags(), dir_fd=parent_fd)
    except OSError as error:
        raise _error("foundry_clean_failed") from error
    try:
        descriptor = os.fstat(directory_fd)
        if (
            not stat.S_ISDIR(descriptor.st_mode)
            or descriptor.st_uid != os.getuid()
            or descriptor.st_dev != metadata.st_dev
            or descriptor.st_ino != metadata.st_ino
        ):
            raise _error("foundry_clean_failed")
        for child in os.listdir(directory_fd):
            child_metadata = os.stat(
                child, dir_fd=directory_fd, follow_symlinks=False
            )
            if stat.S_ISDIR(child_metadata.st_mode):
                _remove_generated_tree(directory_fd, child)
            elif stat.S_ISREG(child_metadata.st_mode) and child_metadata.st_uid == os.getuid():
                os.unlink(child, dir_fd=directory_fd)
            else:
                raise _error("foundry_clean_failed")
    except OSError as error:
        raise _error("foundry_clean_failed") from error
    finally:
        os.close(directory_fd)
    try:
        os.rmdir(name, dir_fd=parent_fd)
    except OSError as error:
        raise _error("foundry_clean_failed") from error


def _typed_inventory_sha256(domain: bytes, value: Any) -> str:
    return hashlib.sha256(domain + b"\n" + _canonical_json_bytes(value) + b"\n").hexdigest()


def _decode_hex_bytes(value: Any, label: str) -> bytes:
    if not isinstance(value, str) or not value.startswith("0x"):
        raise _error(label)
    payload = value[2:]
    if len(payload) % 2 or re.fullmatch(r"[0-9a-fA-F]*", payload) is None:
        raise _error(label)
    try:
        return bytes.fromhex(payload)
    except ValueError as error:
        raise _error(label) from error


def _parse_executor_artifact(payload: bytes) -> Tuple[bytes, bytes, bytes]:
    try:
        value = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=lambda _token: (_ for _ in ()).throw(ValueError()),
        )
        creation = _decode_hex_bytes(
            value["bytecode"]["object"], "executor_creation_bytecode_invalid"
        )
        runtime = _decode_hex_bytes(
            value["deployedBytecode"]["object"], "executor_runtime_invalid"
        )
        immutable_references = value["deployedBytecode"].get(
            "immutableReferences", {}
        )
        if not isinstance(immutable_references, dict):
            raise ValueError
        immutable_bytes = _canonical_json_bytes(immutable_references) + b"\n"
    except (KeyError, TypeError, UnicodeDecodeError, json.JSONDecodeError, ValueError) as error:
        if isinstance(error, HistoricalFoundryToolchainError):
            raise
        raise _error("executor_artifact_invalid") from error
    if not creation or not runtime:
        raise _error("executor_artifact_invalid")
    return creation, runtime, immutable_bytes


def _submodule_commit() -> str:
    marker = _PROJECT_ROOT / "lib" / "forge-std" / ".git"
    payload = _read_stable_file(marker, 4096)
    try:
        text = payload.decode("utf-8")
    except UnicodeDecodeError as error:
        raise _error("forge_std_gitdir_invalid") from error
    if not text.startswith("gitdir: ") or not text.endswith("\n") or text.count("\n") != 1:
        raise _error("forge_std_gitdir_invalid")
    gitdir_text = text[len("gitdir: "):-1]
    if not gitdir_text or "\x00" in gitdir_text:
        raise _error("forge_std_gitdir_invalid")
    gitdir = Path(gitdir_text)
    if not gitdir.is_absolute():
        gitdir = marker.parent / gitdir
    try:
        resolved = gitdir.resolve(strict=True)
    except (OSError, RuntimeError) as error:
        raise _error("forge_std_gitdir_invalid") from error
    head = _read_stable_file(resolved / "HEAD", 256)
    try:
        commit = head.decode("ascii").strip()
    except UnicodeDecodeError as error:
        raise _error("forge_std_commit_invalid") from error
    if commit != _FORGE_STD_COMMIT or not re.fullmatch(r"[0-9a-f]{40}", commit):
        raise _error("forge_std_commit_invalid")
    return commit


def _verify_project_inputs() -> Dict[str, Any]:
    physical = {}
    for relative, expected in _REVIEWED_PROJECT_FILES.items():
        payload = _read_stable_file(_PROJECT_ROOT / relative)
        if payload != expected:
            raise _error("toolchain_project_input_mismatch")
        physical[relative] = _hash_bytes(payload)
    forge_std_tree_sha256 = _forge_std_tree_sha256()
    if forge_std_tree_sha256 != _FORGE_STD_TREE_SHA256:
        raise _error("forge_std_tree_invalid")
    return {
        "forge_std_commit": _submodule_commit(),
        "forge_std_tree_sha256": forge_std_tree_sha256,
        "physical_sha256": physical,
    }


def _download_bytes(url: str) -> bytes:
    request = urllib.request.Request(
        url,
        headers={"Accept": "application/octet-stream", "User-Agent": "historical-foundry-bootstrap/v1"},
        method="GET",
    )
    maximum = _MAX_DOWNLOAD_BYTES if url in (_FOUNDRY_ARCHIVE_URL, _SOLC_URL) else _MAX_SIDECAR_BYTES
    try:
        context = ssl.create_default_context(cafile="/etc/ssl/cert.pem")
        with urllib.request.urlopen(request, timeout=30, context=context) as response:
            if getattr(response, "status", 200) != 200:
                raise _error("toolchain_download_failed")
            payload = response.read(maximum + 1)
    except HistoricalFoundryToolchainError:
        raise
    except Exception as error:
        raise _error("toolchain_download_failed") from error
    if len(payload) > maximum:
        raise _error("toolchain_download_too_large")
    return payload


def _write_private_asset(path: Path, payload: bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(str(path), flags, 0o600)
        try:
            offset = 0
            while offset < len(payload):
                offset += os.write(fd, payload[offset:])
            os.fchmod(fd, 0o600)
            os.fsync(fd)
        finally:
            os.close(fd)
        metadata = os.stat(str(path), follow_symlinks=False)
    except OSError as error:
        raise _error("toolchain_download_staging_failed") from error
    if (
        not stat.S_ISREG(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or metadata.st_nlink != 1
        or stat.S_IMODE(metadata.st_mode) != 0o600
    ):
        raise _error("toolchain_download_staging_unsafe")


def _download_reviewed_assets(download_directory: Path) -> Dict[str, bytes]:
    assets = {}
    for url in (
        _FOUNDRY_ARCHIVE_URL,
        _FOUNDRY_CHECKSUM_URL,
        _FOUNDRY_SIGSTORE_URL,
        _FOUNDRY_SPDX_URL,
        _SOLC_URL,
    ):
        payload = _download_bytes(url)
        path = download_directory / _ASSET_FILENAMES[url]
        _write_private_asset(path, payload)
        maximum = (
            _MAX_DOWNLOAD_BYTES
            if url in (_FOUNDRY_ARCHIVE_URL, _SOLC_URL)
            else _MAX_SIDECAR_BYTES
        )
        payload = _read_stable_file(path, maximum)
        expected = _REVIEWED_ASSET_SHA256[url]
        if _hash_bytes(payload) != expected:
            raise _error("toolchain_asset_sha256_mismatch")
        assets[url] = payload
    return assets


def _verify_checksum_sidecar(payload: bytes, archive_sha256: str) -> None:
    expected = (
        archive_sha256 + "  foundry_v1.7.1_darwin_arm64.tar.gz\n"
    ).encode("ascii")
    alternate = (
        archive_sha256 + " *foundry_v1.7.1_darwin_arm64.tar.gz\n"
    ).encode("ascii")
    if payload not in (expected, alternate):
        raise _error("toolchain_checksum_projection_mismatch")


def _reject_duplicate_keys(pairs: Sequence[Tuple[str, Any]]) -> Dict[str, Any]:
    result = {}
    for key, value in pairs:
        if key in result:
            raise _error("toolchain_sigstore_invalid")
        result[key] = value
    return result


def _verify_sigstore_projection(payload: bytes, archive_sha256: str) -> None:
    try:
        value = json.loads(
            payload.decode("utf-8"),
            object_pairs_hook=_reject_duplicate_keys,
            parse_constant=lambda _token: (_ for _ in ()).throw(_error("toolchain_sigstore_invalid")),
        )
        signature = value["messageSignature"]
        message_digest = signature["messageDigest"]
        if message_digest["algorithm"] != "SHA2_256":
            raise _error("toolchain_sigstore_digest_mismatch")
        signed_digest = base64.b64decode(message_digest["digest"], validate=True)
        certificate = base64.b64decode(
            value["verificationMaterial"]["certificate"]["rawBytes"], validate=True
        )
    except HistoricalFoundryToolchainError:
        raise
    except (KeyError, TypeError, ValueError, UnicodeDecodeError, json.JSONDecodeError, binascii.Error) as error:
        raise _error("toolchain_sigstore_invalid") from error
    if signed_digest != bytes.fromhex(archive_sha256):
        raise _error("toolchain_sigstore_digest_mismatch")
    if certificate.count(_SIGSTORE_ISSUER.encode("utf-8")) < 1:
        raise _error("toolchain_sigstore_issuer_mismatch")
    if certificate.count(_SIGSTORE_SAN.encode("utf-8")) < 1:
        raise _error("toolchain_sigstore_san_mismatch")


def _extract_foundry_members(payload: bytes) -> Dict[str, bytes]:
    selected = {}
    try:
        with tarfile.open(fileobj=io.BytesIO(payload), mode="r:gz") as archive:
            for member in archive.getmembers():
                normalized = member.name[2:] if member.name.startswith("./") else member.name
                if normalized not in _FOUNDRY_BINARY_NAMES:
                    continue
                if normalized in selected or not member.isfile() or member.islnk() or member.issym():
                    raise _error("toolchain_archive_member_invalid")
                if member.size <= 0 or member.size > _MAX_DOWNLOAD_BYTES:
                    raise _error("toolchain_archive_member_invalid")
                handle = archive.extractfile(member)
                if handle is None:
                    raise _error("toolchain_archive_member_invalid")
                body = handle.read(_MAX_DOWNLOAD_BYTES + 1)
                if len(body) != member.size or len(body) > _MAX_DOWNLOAD_BYTES:
                    raise _error("toolchain_archive_member_invalid")
                selected[normalized] = body
    except HistoricalFoundryToolchainError:
        raise
    except (tarfile.TarError, OSError, EOFError) as error:
        raise _error("toolchain_archive_invalid") from error
    if tuple(sorted(selected)) != tuple(sorted(_FOUNDRY_BINARY_NAMES)):
        raise _error("toolchain_archive_member_missing")
    return selected


def _ensure_private_directory(path: Path) -> None:
    try:
        path.mkdir(mode=0o700)
    except FileExistsError:
        metadata = os.stat(str(path), follow_symlinks=False)
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or stat.S_IMODE(metadata.st_mode) != 0o700
        ):
            raise _error("toolchain_directory_unsafe")
    except OSError as error:
        raise _error("toolchain_install_failed") from error


def _write_binary(path: Path, payload: bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    try:
        fd = os.open(str(path), flags, 0o700)
        try:
            offset = 0
            while offset < len(payload):
                offset += os.write(fd, payload[offset:])
            os.fchmod(fd, 0o700)
            os.fsync(fd)
        finally:
            os.close(fd)
    except OSError as error:
        raise _error("toolchain_install_failed") from error


def _install_atomically(binaries: Mapping[str, bytes]) -> Path:
    base = _PROJECT_ROOT / ".historical-foundry"
    toolchains = base / "toolchains"
    _ensure_private_directory(base)
    _ensure_private_directory(toolchains)
    destination = toolchains / _SOURCE_LOCK_SHA256
    if destination.exists():
        return destination
    temporary = Path(tempfile.mkdtemp(prefix=".candidate-", dir=str(toolchains)))
    temporary.chmod(0o700)
    try:
        binary_dir = temporary / "bin"
        binary_dir.mkdir(mode=0o700)
        for name in _BINARY_NAMES:
            _write_binary(binary_dir / name, binaries[name])
        try:
            os.rename(str(temporary), str(destination))
        except FileExistsError:
            return destination
        return destination
    finally:
        if temporary.exists():
            shutil.rmtree(str(temporary))


def _directory_fd(parent_fd: int, name: str) -> Tuple[int, Tuple[int, ...]]:
    try:
        path_metadata = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
        fd = os.open(name, _secure_directory_flags(), dir_fd=parent_fd)
        metadata = os.fstat(fd)
    except OSError as error:
        raise _error("toolchain_directory_unsafe") from error
    if (
        not stat.S_ISDIR(metadata.st_mode)
        or metadata.st_uid != os.getuid()
        or stat.S_IMODE(metadata.st_mode) != 0o700
        or _stable_metadata(path_metadata) != _stable_metadata(metadata)
    ):
        os.close(fd)
        raise _error("toolchain_directory_unsafe")
    return fd, _stable_metadata(metadata)


def _open_toolchain(expected: Mapping[str, str]) -> "ReviewedHistoricalToolchain":
    if set(expected) != set(_BINARY_NAMES) or any(
        not isinstance(value, str) or _HASH.fullmatch(value) is None
        for value in expected.values()
    ):
        raise _error("toolchain_binary_identity_unreviewed")
    runtime_home = _PROJECT_ROOT / ".historical-foundry" / "runtime-home"
    _ensure_private_directory(runtime_home)
    try:
        root_path_metadata = os.stat(str(_PROJECT_ROOT), follow_symlinks=False)
        root_fd = os.open(str(_PROJECT_ROOT), _secure_directory_flags())
        root_metadata = os.fstat(root_fd)
    except OSError as error:
        raise _error("toolchain_root_unavailable") from error
    if (
        not stat.S_ISDIR(root_metadata.st_mode)
        or _stable_metadata(root_path_metadata) != _stable_metadata(root_metadata)
    ):
        os.close(root_fd)
        raise _error("toolchain_root_unavailable")
    directory_chain = [
        (root_fd, None, None, _stable_metadata(root_metadata))
    ]
    binaries = {}
    try:
        current = root_fd
        for name in (".historical-foundry", "toolchains", _SOURCE_LOCK_SHA256, "bin"):
            child, child_metadata = _directory_fd(current, name)
            directory_chain.append((child, current, name, child_metadata))
            current = child
        bin_fd = current
        for name in _BINARY_NAMES:
            try:
                path_before = os.stat(name, dir_fd=bin_fd, follow_symlinks=False)
                fd = os.open(name, _secure_file_flags(), dir_fd=bin_fd)
                descriptor = os.fstat(fd)
            except OSError as error:
                raise _error("toolchain_binary_unsafe") from error
            if (
                not stat.S_ISREG(descriptor.st_mode)
                or descriptor.st_nlink != 1
                or descriptor.st_uid != os.getuid()
                or stat.S_IMODE(descriptor.st_mode) != 0o700
                or _stable_metadata(path_before) != _stable_metadata(descriptor)
                or _hash_fd(fd) != expected[name]
            ):
                os.close(fd)
                raise _error("toolchain_binary_identity_mismatch")
            binaries[name] = (fd, _stable_metadata(descriptor), expected[name])
        capability = ReviewedHistoricalToolchain(directory_chain, binaries, expected)
        directory_chain = []
        return capability
    except Exception:
        for fd, _metadata, _digest in binaries.values():
            os.close(fd)
        raise
    finally:
        for fd, _parent_fd, _name, _metadata in reversed(directory_chain):
            os.close(fd)


def _validate_historical_process_output_counts(
    *, stdout_bytes: int, stderr_bytes: int
) -> None:
    if (
        type(stdout_bytes) is not int
        or type(stderr_bytes) is not int
        or stdout_bytes < 0
        or stderr_bytes < 0
        or stdout_bytes + stderr_bytes > 65_536
    ):
        raise ValueError("historical process output limit exceeded")
    return None


def _initialize_historical_process_lease_type():
    provenance = object()

    class _HistoricalProcessLease:
        __slots__ = (
            "_process", "_cleanup", "_binary_sha256", "_selected_block",
            "_hardfork", "_toolchain", "_output_threads",
            "_output_totals", "_output_capture", "_output_lock",
            "_output_overflow", "_output_control", "_launch_identity",
            "_owner_registry", "_reaped", "_closed",
        )

        def __init__(self, *, _provenance: object = None, **values: Any) -> None:
            if _provenance is not provenance:
                raise ValueError("historical process lease provenance is invalid")
            for name in self.__slots__:
                object.__setattr__(self, name, values[name])

        def __repr__(self) -> str:
            return "_HistoricalProcessLease(<sealed>)"

        def __setattr__(self, _name: str, _value: Any) -> None:
            raise AttributeError("historical process lease is immutable")

        def __reduce__(self) -> Any:
            raise TypeError("historical process lease is not serializable")

        def redacted_argv_projection(self) -> Mapping[str, Any]:
            if self._closed:
                raise ValueError("historical process lease is closed")
            return {
                "schema": "historical_foundry_anvil_argv/v1",
                "binary_sha256": self._binary_sha256,
                "fixed_arguments": (
                    "--chain-id", "1", "--fork-chain-id", "1",
                    "--accounts", "0", "--gas-price", "0",
                    "--disable-default-create2-deployer",
                    "--host", "127.0.0.1",
                    "--no-mining", "--no-cors", "--silent", "--order",
                    "fifo", "--steps-tracing", "--retries", "0",
                    "--timeout", "30000", "--no-storage-caching",
                ),
                "selected_block": self._selected_block,
                "hardfork": self._hardfork,
                "fork_url_kind": "loopback_relay",
            }

        def verified_launch_identity_projection(self) -> Mapping[str, Any]:
            if self._closed or type(self._launch_identity) is not dict:
                raise ValueError("historical process identity is unavailable")
            return json.loads(
                _canonical_json_bytes(self._launch_identity).decode("utf-8")
            )

        def _assert_output_within_limit(self) -> None:
            with self._output_lock:
                stdout_bytes, stderr_bytes = self._output_totals
                overflow = self._output_overflow.is_set()
            if overflow:
                raise ValueError("historical process output limit exceeded")
            _validate_historical_process_output_counts(
                stdout_bytes=stdout_bytes, stderr_bytes=stderr_bytes
            )
            return None

        def _captured_output_for_test(self) -> Tuple[bytes, bytes]:
            with self._output_lock:
                return tuple(bytes(value) for value in self._output_capture)

        def close(self) -> None:
            return self._close_with_budget(lambda cap: cap)

        def _close_with_budget(self, remaining: Any) -> None:
            if self._closed:
                return None
            if not callable(remaining):
                raise ValueError("historical process deadline is invalid")

            def timeout(cap: float) -> float:
                try:
                    value = remaining(cap)
                except TimeoutError:
                    return 0.0
                if (
                    type(value) not in (int, float)
                    or isinstance(value, bool)
                    or not 0 <= value <= cap
                ):
                    raise ValueError("historical process deadline is invalid")
                return float(value)

            process = self._process
            control = None
            ordinary = False
            timed_out = False
            reaped = self._reaped
            if not reaped:
                try:
                    timeout(5.0)
                    process.terminate()
                except BaseException as error:
                    if not isinstance(error, Exception):
                        control = error
                    else:
                        ordinary = True
                try:
                    process.wait(timeout=timeout(5.0))
                    reaped = True
                except subprocess.TimeoutExpired:
                    timed_out = True
                except BaseException as error:
                    timed_out = True
                    if not isinstance(error, Exception) and control is None:
                        control = error
                    elif isinstance(error, Exception):
                        ordinary = True
                if timed_out:
                    try:
                        timeout(5.0)
                        process.kill()
                    except BaseException as error:
                        if not isinstance(error, Exception) and control is None:
                            control = error
                        elif isinstance(error, Exception):
                            ordinary = True
                    try:
                        process.wait(timeout=timeout(5.0))
                        reaped = True
                    except subprocess.TimeoutExpired:
                        ordinary = True
                    except BaseException as error:
                        if not isinstance(error, Exception) and control is None:
                            control = error
                        elif isinstance(error, Exception):
                            ordinary = True
                if reaped:
                    object.__setattr__(self, "_reaped", True)
            for thread in self._output_threads:
                try:
                    thread.join(timeout=timeout(5.0))
                except BaseException as error:
                    if not isinstance(error, Exception) and control is None:
                        control = error
                    elif isinstance(error, Exception):
                        ordinary = True
            for stream_name in ("stdout", "stderr"):
                stream = getattr(process, stream_name, None)
                closer = getattr(stream, "close", None)
                if callable(closer):
                    try:
                        timeout(5.0)
                        closer()
                    except BaseException as error:
                        if not isinstance(error, Exception) and control is None:
                            control = error
                        elif isinstance(error, Exception):
                            ordinary = True
            for thread in self._output_threads:
                try:
                    thread.join(timeout=timeout(5.0))
                    if thread.is_alive():
                        ordinary = True
                except BaseException as error:
                    if not isinstance(error, Exception) and control is None:
                        control = error
                    elif isinstance(error, Exception):
                        ordinary = True
            try:
                self._assert_output_within_limit()
            except BaseException as error:
                if not isinstance(error, Exception) and control is None:
                    control = error
                elif isinstance(error, Exception):
                    ordinary = True
            threads_quiescent = all(
                not thread.is_alive() for thread in self._output_threads
            )
            with self._output_lock:
                output_control = self._output_control[0]
            if reaped and threads_quiescent:
                release_ok = True
                toolchain = self._toolchain
                if toolchain is not None:
                    try:
                        toolchain._assert_stable_binaries()
                    except BaseException as error:
                        release_ok = False
                        if not isinstance(error, Exception) and control is None:
                            control = error
                        elif isinstance(error, Exception):
                            ordinary = True
                if release_ok:
                    try:
                        self._cleanup()
                    except BaseException as error:
                        release_ok = False
                        if not isinstance(error, Exception) and control is None:
                            control = error
                        elif isinstance(error, Exception):
                            ordinary = True
                if release_ok:
                    owner_registry = self._owner_registry
                    if owner_registry is not None:
                        owner_registry.pop(id(self), None)
                    object.__setattr__(self, "_process", None)
                    object.__setattr__(self, "_cleanup", None)
                    object.__setattr__(self, "_toolchain", None)
                    object.__setattr__(self, "_output_threads", ())
                    object.__setattr__(self, "_closed", True)
            else:
                ordinary = True
            if output_control is not None and control is None:
                control = output_control
            if control is not None:
                raise control
            if ordinary:
                raise ValueError("historical process reap failed")
            return None

    def issue(
        *,
        process: Any,
        cleanup: Any,
        binary_sha256: str,
        selected_block: int,
        hardfork: str,
        toolchain: Any = None,
        launch_identity: Any = None,
        owner_registry: Any = None,
        _register: bool = True,
        _handoff_pending: Any = None,
    ) -> _HistoricalProcessLease:
        if (
            not callable(cleanup)
            or type(binary_sha256) is not str
            or re.fullmatch(r"[0-9a-f]{64}", binary_sha256) is None
            or type(selected_block) is not int
            or selected_block < 0
            or type(hardfork) is not str
            or not hardfork
        ):
            raise ValueError("historical process lease input is invalid")
        for name in ("terminate", "kill", "wait"):
            if not callable(getattr(process, name, None)):
                raise ValueError("historical process handle is invalid")
        output_totals = [0, 0]
        output_capture = [bytearray(), bytearray()]
        output_lock = threading.Lock()
        output_overflow = threading.Event()
        output_control = [None]
        output_threads = []

        def drain(stream: Any, index: int) -> None:
            try:
                while True:
                    chunk = stream.read(8_192)
                    if chunk in (b"", None):
                        break
                    if type(chunk) is not bytes:
                        output_overflow.set()
                        break
                    with output_lock:
                        output_totals[index] += len(chunk)
                        remaining_capture = 65_536 - sum(
                            len(value) for value in output_capture
                        )
                        if remaining_capture > 0:
                            output_capture[index].extend(
                                chunk[:remaining_capture]
                            )
                        if sum(output_totals) > 65_536:
                            output_overflow.set()
            except BaseException as error:
                if not isinstance(error, Exception):
                    with output_lock:
                        if output_control[0] is None:
                            output_control[0] = error
                output_overflow.set()

        lease = _HistoricalProcessLease(
            _provenance=provenance,
            _process=process,
            _cleanup=cleanup,
            _binary_sha256=binary_sha256,
            _selected_block=selected_block,
            _hardfork=hardfork,
            _toolchain=toolchain,
            _output_threads=(),
            _output_totals=output_totals,
            _output_capture=output_capture,
            _output_lock=output_lock,
            _output_overflow=output_overflow,
            _output_control=output_control,
            _launch_identity=launch_identity,
            _owner_registry=owner_registry,
            _reaped=False,
            _closed=False,
        )
        if _handoff_pending is not None:
            if owner_registry is None or _register:
                raise ValueError("historical process registry is invalid")
            _handoff_pending._state["lease"] = lease
            _handoff_pending.handoff(lease)
        elif owner_registry is not None and _register:
            if type(owner_registry) is not dict:
                raise ValueError("historical process registry is invalid")
            owner_registry[id(lease)] = lease
        for index, name in enumerate(("stdout", "stderr")):
            stream = getattr(process, name, None)
            if stream is not None and callable(getattr(stream, "read", None)):
                thread = threading.Thread(
                    target=drain, args=(stream, index), daemon=False,
                    name="historical-anvil-output-{}".format(name),
                )
                output_threads.append(thread)
        object.__setattr__(lease, "_output_threads", tuple(output_threads))
        for thread in output_threads:
            thread.start()
        return lease

    return _HistoricalProcessLease, issue


(
    _HistoricalProcessLease,
    _issue_historical_process_lease_for_test,
) = _initialize_historical_process_lease_type()
del _initialize_historical_process_lease_type


_DARWIN_EXPECTED_ANVIL_CDHASH = (
    "561b69d0257e574c3438465eb55cf4cef6852abc"
)
_DARWIN_POSIX_SPAWN_START_SUSPENDED = 0x0080
_DARWIN_POSIX_SPAWN_CLOEXEC_DEFAULT = 0x4000


def _destroy_darwin_spawn_object(
    *, slot: Dict[str, Any], value: Any, destroy: Any,
) -> None:
    if not value.value or slot.get("state") == "DESTROYED":
        slot["state"] = "DESTROYED"
        return None
    if slot.get("state") == "DESTROY_UNCERTAIN":
        raise _error("toolchain_process_cleanup_failed")
    slot["state"] = "ATTEMPTING"
    try:
        result = destroy(ctypes.byref(value))
    except BaseException:
        slot["state"] = "DESTROY_UNCERTAIN"
        raise
    if result != 0:
        slot["state"] = "OPEN"
        raise _error("toolchain_process_cleanup_failed")
    slot["state"] = "DESTROYED"
    return None


class _DarwinSpawnedProcess:
    __slots__ = ("_state", "_remaining", "pid", "stdout", "stderr", "returncode")

    def __init__(
        self, pid: Optional[int] = None, stdout: Any = None, stderr: Any = None,
        *, state: Optional[Dict[str, Any]] = None, remaining: Any = None,
    ) -> None:
        if state is None:
            state = {
                "pid": pid, "pid_cell": ctypes.c_int(pid or 0),
                "reaped": False, "returncode": None,
                "reap_uncertain": False,
            }
        if type(state) is not dict or not callable(remaining or (lambda cap: cap)):
            raise ValueError("historical process handle is invalid")
        observed_pid = state.get("pid")
        if type(observed_pid) is not int or observed_pid <= 0:
            raise ValueError("historical process handle is invalid")
        self._state = state
        self._remaining = remaining or (lambda cap: cap)
        self.pid = observed_pid
        self.stdout = stdout
        self.stderr = stderr
        self.returncode = state.get("returncode")

    def _record_status(self, status: int) -> int:
        if os.WIFEXITED(status):
            self.returncode = os.WEXITSTATUS(status)
        elif os.WIFSIGNALED(status):
            self.returncode = -os.WTERMSIG(status)
        else:
            raise _error("toolchain_process_failed")
        self._state["returncode"] = self.returncode
        self._state["reaped"] = True
        return self.returncode

    def _waitpid(self, options: int) -> Tuple[int, int]:
        if self._state.get("reaped"):
            return self.pid, int(self._state.get("wait_status", 0))
        if self._state.get("reap_uncertain"):
            raise _error("toolchain_process_reap_failed")
        while True:
            try:
                return os.waitpid(self.pid, options)
            except InterruptedError:
                self._remaining(5.0)
                continue
            except OSError as error:
                if error.errno == errno.ECHILD:
                    if self._state.get("reaped"):
                        return self.pid, int(self._state.get("wait_status", 0))
                    self._state["reap_uncertain"] = True
                    raise _error("toolchain_process_reap_failed") from error
                raise

    def poll(self) -> Optional[int]:
        if self.returncode is not None:
            return self.returncode
        observed, status = self._waitpid(os.WNOHANG)
        if observed == 0:
            return None
        if observed != self.pid:
            raise _error("toolchain_process_failed")
        self._state["wait_status"] = status
        return self._record_status(status)

    def wait(self, timeout: Optional[float] = None) -> int:
        if self.returncode is not None:
            return self.returncode
        if timeout is None:
            timeout = 5.0
        cap = float(timeout)
        while True:
            available = self._remaining(cap)
            if available <= 0:
                raise subprocess.TimeoutExpired("anvil", timeout)
            observed, status = self._waitpid(os.WNOHANG)
            if observed == self.pid:
                self._state["wait_status"] = status
                return self._record_status(status)
            if observed != 0:
                raise _error("toolchain_process_failed")
            time.sleep(min(0.01, available))

    def terminate(self) -> None:
        if self._state.get("reaped"):
            return None
        if self._state.get("reap_uncertain"):
            raise _error("toolchain_process_reap_failed")
        self._remaining(5.0)
        os.kill(self.pid, signal.SIGTERM)

    def kill(self) -> None:
        if self._state.get("reaped"):
            return None
        if self._state.get("reap_uncertain"):
            raise _error("toolchain_process_reap_failed")
        self._remaining(5.0)
        os.kill(self.pid, signal.SIGKILL)


class _PendingHistoricalSpawnLease:
    __slots__ = ("_state", "_cleanup", "_registry", "_remaining", "_closed")

    def __init__(
        self, *, state: Dict[str, Any], cleanup: Any,
        registry: Dict[int, Any], remaining: Any = None,
    ) -> None:
        self._state = state
        self._cleanup = cleanup
        self._registry = registry
        self._remaining = remaining or (lambda cap: cap)
        self._closed = False
        registry[id(self)] = self

    def disarm(self) -> None:
        self._registry.pop(id(self), None)
        self._closed = True

    def handoff(self, lease: Any) -> None:
        if self._closed or self._registry.get(id(self)) is not self:
            raise _error("toolchain_process_failed")
        self._registry[id(lease)] = lease
        self._state["lease"] = lease
        self._registry.pop(id(self))
        self._closed = True

    def close(self) -> None:
        if self._closed:
            return None
        process = self._state.get("process")
        if process is not None and not self._state.get("reaped"):
            process.kill()
            process.wait(timeout=self._remaining(5.0))
            self._state["reaped"] = True
        pid_cell = self._state.get("pid_cell")
        pid = pid_cell.value if isinstance(pid_cell, ctypes.c_int) else None
        if type(pid) is int and pid > 0 and not self._state.get("reaped"):
            if self._state.get("pid") is None:
                self._state["pid"] = pid
            elif self._state.get("pid") != pid:
                raise _error("toolchain_process_reap_failed")
            process = _DarwinSpawnedProcess(
                state=self._state, stdout=None, stderr=None,
                remaining=self._remaining,
            )
            process.kill()
            process.wait(timeout=self._remaining(5.0))
        for name in ("stdout_stream", "stderr_stream"):
            stream = self._state.get(name)
            if stream is not None and not stream.closed:
                stream.close()
        self._cleanup()
        self._registry.pop(id(self), None)
        self._closed = True
        return None


class _DarwinVinfoStat(ctypes.Structure):
    _fields_ = (
        ("vst_dev", ctypes.c_uint32),
        ("vst_mode", ctypes.c_uint16),
        ("vst_nlink", ctypes.c_uint16),
        ("vst_ino", ctypes.c_uint64),
        ("vst_uid", ctypes.c_uint32),
        ("vst_gid", ctypes.c_uint32),
        ("vst_atime", ctypes.c_int64),
        ("vst_atimensec", ctypes.c_int64),
        ("vst_mtime", ctypes.c_int64),
        ("vst_mtimensec", ctypes.c_int64),
        ("vst_ctime", ctypes.c_int64),
        ("vst_ctimensec", ctypes.c_int64),
        ("vst_birthtime", ctypes.c_int64),
        ("vst_birthtimensec", ctypes.c_int64),
        ("vst_size", ctypes.c_int64),
        ("vst_blocks", ctypes.c_int64),
        ("vst_blksize", ctypes.c_int32),
        ("vst_flags", ctypes.c_uint32),
        ("vst_gen", ctypes.c_uint32),
        ("vst_rdev", ctypes.c_uint32),
        ("vst_qspare", ctypes.c_int64 * 2),
    )


class _DarwinVnodeInfo(ctypes.Structure):
    _fields_ = (
        ("vi_stat", _DarwinVinfoStat),
        ("vi_type", ctypes.c_int),
        ("vi_pad", ctypes.c_int),
        ("vi_fsid", ctypes.c_int32 * 2),
    )


class _DarwinVnodeInfoPath(ctypes.Structure):
    _fields_ = (
        ("vip_vi", _DarwinVnodeInfo),
        ("vip_path", ctypes.c_char * 1024),
    )


class _DarwinProcRegionInfo(ctypes.Structure):
    _fields_ = (
        ("pri_protection", ctypes.c_uint32),
        ("pri_max_protection", ctypes.c_uint32),
        ("pri_inheritance", ctypes.c_uint32),
        ("pri_flags", ctypes.c_uint32),
        ("pri_offset", ctypes.c_uint64),
        ("pri_behavior", ctypes.c_uint32),
        ("pri_user_wired_count", ctypes.c_uint32),
        ("pri_user_tag", ctypes.c_uint32),
        ("pri_pages_resident", ctypes.c_uint32),
        ("pri_pages_shared_now_private", ctypes.c_uint32),
        ("pri_pages_swapped_out", ctypes.c_uint32),
        ("pri_pages_dirtied", ctypes.c_uint32),
        ("pri_ref_count", ctypes.c_uint32),
        ("pri_shadow_depth", ctypes.c_uint32),
        ("pri_share_mode", ctypes.c_uint32),
        ("pri_private_pages_resident", ctypes.c_uint32),
        ("pri_shared_pages_resident", ctypes.c_uint32),
        ("pri_obj_id", ctypes.c_uint32),
        ("pri_depth", ctypes.c_uint32),
        ("pri_address", ctypes.c_uint64),
        ("pri_size", ctypes.c_uint64),
    )


class _DarwinProcRegionWithPathInfo(ctypes.Structure):
    _fields_ = (
        ("prp_prinfo", _DarwinProcRegionInfo),
        ("prp_vip", _DarwinVnodeInfoPath),
    )


def _darwin_verified_launch_identity(
    *, pid: int, executable_fd: int, binary_sha256: str
) -> Dict[str, Any]:
    if sys.platform != "darwin":
        raise _error("toolchain_process_identity_unsupported")
    library = ctypes.CDLL(None, use_errno=True)
    cdhash = (ctypes.c_ubyte * 20)()
    csops = library.csops
    csops.argtypes = (
        ctypes.c_int, ctypes.c_uint, ctypes.c_void_p, ctypes.c_size_t,
    )
    csops.restype = ctypes.c_int
    if csops(pid, 5, ctypes.byref(cdhash), 20) != 0:
        raise _error("toolchain_process_identity_mismatch")
    observed_cdhash = bytes(cdhash).hex()
    if observed_cdhash != _DARWIN_EXPECTED_ANVIL_CDHASH:
        raise _error("toolchain_process_identity_mismatch")
    region = _DarwinProcRegionWithPathInfo()
    proc_pidinfo = library.proc_pidinfo
    proc_pidinfo.argtypes = (
        ctypes.c_int, ctypes.c_int, ctypes.c_uint64,
        ctypes.c_void_p, ctypes.c_int,
    )
    proc_pidinfo.restype = ctypes.c_int
    observed_size = proc_pidinfo(
        pid, 8, 0, ctypes.byref(region), ctypes.sizeof(region)
    )
    descriptor = os.fstat(executable_fd)
    region_stat = region.prp_vip.vip_vi.vi_stat
    if (
        observed_size != ctypes.sizeof(region)
        or int(region_stat.vst_dev) != int(descriptor.st_dev)
        or int(region_stat.vst_ino) != int(descriptor.st_ino)
        or _hash_fd(executable_fd) != binary_sha256
    ):
        raise _error("toolchain_process_identity_mismatch")
    return {
        "schema": "historical_foundry_anvil_launch_identity/v1",
        "binary_sha256": binary_sha256,
        "cdhash": observed_cdhash,
        "main_image_matches_materialized_inode": True,
        "resumed_after_identity_verification": True,
    }


def _darwin_spawn_suspended(
    *, executable_path: str, work_directory_fd: int,
    arguments: Tuple[str, ...], environment: Mapping[str, str],
    executable_fd: int, binary_sha256: str,
    spawn_state: Dict[str, Any], remaining: Any,
    finalize_lease: Any,
) -> Tuple[_DarwinSpawnedProcess, Dict[str, Any]]:
    if sys.platform != "darwin":
        raise _error("toolchain_process_identity_unsupported")
    library = ctypes.CDLL(None, use_errno=True)
    attr = ctypes.c_void_p()
    actions = ctypes.c_void_p()
    stdout_read = stdout_write = stderr_read = stderr_write = None
    devnull = None
    pid = spawn_state.get("pid_cell")
    if not isinstance(pid, ctypes.c_int):
        raise _error("toolchain_process_failed")
    if not callable(remaining) or not callable(finalize_lease):
        raise _error("toolchain_process_failed")
    resumed = False

    def close_descriptor(name: str) -> None:
        slots = spawn_state.setdefault("spawn_descriptor_cleanup", {})
        slot = slots.get(name)
        if (
            slot is None
            or slot.get("state") in (
                "UNALLOCATED", "CLOSED", "TRANSFERRED"
            )
        ):
            return None
        if slot.get("state") == "CLOSE_UNCERTAIN":
            raise _error("toolchain_process_cleanup_failed")
        slot["state"] = "ATTEMPTING"
        try:
            os.close(slot["fd"])
        except BaseException:
            try:
                os.fstat(slot["fd"])
            except OSError as observed:
                slot["state"] = (
                    "CLOSED" if observed.errno == errno.EBADF
                    else "CLOSE_UNCERTAIN"
                )
            else:
                slot["state"] = "CLOSE_UNCERTAIN"
            raise
        slot["state"] = "CLOSED"
        return None

    def cleanup_spawn_resources() -> None:
        for name, value, destroy_name in (
            ("attr", attr, "posix_spawnattr_destroy"),
            ("actions", actions, "posix_spawn_file_actions_destroy"),
        ):
            slot = spawn_state.setdefault(
                "spawn_object_cleanup", {}
            ).setdefault(name, {"state": "OPEN"})
            _destroy_darwin_spawn_object(
                slot=slot, value=value,
                destroy=getattr(library, destroy_name),
            )
            spawn_state[name + "_destroyed"] = True
        for name in (
            "stdout_read", "stdout_write", "stderr_read", "stderr_write",
            "devnull",
        ):
            close_descriptor(name)
        return None

    spawn_state["spawn_resource_cleanup"] = cleanup_spawn_resources
    try:
        if library.posix_spawnattr_init(ctypes.byref(attr)) != 0:
            raise _error("toolchain_process_failed")
        spawn_state["attr"] = attr
        if library.posix_spawn_file_actions_init(
            ctypes.byref(actions)
        ) != 0:
            raise _error("toolchain_process_failed")
        spawn_state["actions"] = actions
        flags = ctypes.c_short(
            _DARWIN_POSIX_SPAWN_START_SUSPENDED
            | _DARWIN_POSIX_SPAWN_CLOEXEC_DEFAULT
        )
        if library.posix_spawnattr_setflags(
            ctypes.byref(attr), flags
        ) != 0:
            raise _error("toolchain_process_failed")
        spawn_state["spawn_descriptor_cleanup"] = {
            name: {"fd": None, "state": "UNALLOCATED"}
            for name in (
                "stdout_read", "stdout_write", "stderr_read",
                "stderr_write", "devnull",
            )
        }
        stdout_read, stdout_write = os.pipe()
        spawn_state["spawn_descriptor_cleanup"]["stdout_read"].update({
            "fd": stdout_read, "state": "OPEN",
        })
        spawn_state["spawn_descriptor_cleanup"]["stdout_write"].update({
            "fd": stdout_write, "state": "OPEN",
        })
        stderr_read, stderr_write = os.pipe()
        spawn_state["spawn_descriptor_cleanup"]["stderr_read"].update({
            "fd": stderr_read, "state": "OPEN",
        })
        spawn_state["spawn_descriptor_cleanup"]["stderr_write"].update({
            "fd": stderr_write, "state": "OPEN",
        })
        devnull = os.open(os.devnull, os.O_RDONLY)
        spawn_state["spawn_descriptor_cleanup"]["devnull"].update({
            "fd": devnull, "state": "OPEN",
        })
        for source, target in (
            (devnull, 0), (stdout_write, 1), (stderr_write, 2)
        ):
            if library.posix_spawn_file_actions_adddup2(
                ctypes.byref(actions), source, target
            ) != 0:
                raise _error("toolchain_process_failed")
        for descriptor in (stdout_read, stderr_read):
            if library.posix_spawn_file_actions_addclose(
                ctypes.byref(actions), descriptor
            ) != 0:
                raise _error("toolchain_process_failed")
        addfchdir = library.posix_spawn_file_actions_addfchdir_np
        addfchdir.argtypes = (
            ctypes.POINTER(ctypes.c_void_p), ctypes.c_int
        )
        addfchdir.restype = ctypes.c_int
        if addfchdir(
            ctypes.byref(actions), work_directory_fd
        ) != 0:
            raise _error("toolchain_process_failed")
        encoded_arguments = tuple(os.fsencode(value) for value in arguments)
        argv = (ctypes.c_char_p * (len(encoded_arguments) + 1))(
            *encoded_arguments, None
        )
        encoded_environment = tuple(
            os.fsencode(key + "=" + value)
            for key, value in sorted(environment.items())
        )
        envp = (ctypes.c_char_p * (len(encoded_environment) + 1))(
            *encoded_environment, None
        )
        spawn = library.posix_spawn
        spawn.argtypes = (
            ctypes.POINTER(ctypes.c_int), ctypes.c_char_p,
            ctypes.POINTER(ctypes.c_void_p),
            ctypes.POINTER(ctypes.c_void_p),
            ctypes.POINTER(ctypes.c_char_p),
            ctypes.POINTER(ctypes.c_char_p),
        )
        spawn.restype = ctypes.c_int
        result = spawn(
            ctypes.byref(pid), os.fsencode(executable_path),
            ctypes.byref(actions), ctypes.byref(attr), argv, envp,
        )
        if result != 0 or pid.value <= 0:
            raise _error("toolchain_process_failed")
        spawn_state["pid"] = pid.value
        process = _DarwinSpawnedProcess(
            state=spawn_state, stdout=None, stderr=None,
            remaining=remaining,
        )
        spawn_state["process"] = process
        close_descriptor("stdout_write")
        stdout_write = None
        close_descriptor("stderr_write")
        stderr_write = None
        close_descriptor("devnull")
        devnull = None
        stopped_pid, status = process._waitpid(os.WUNTRACED | os.WNOHANG)
        while stopped_pid == 0:
            available = remaining(5.0)
            if available <= 0:
                raise _error("toolchain_process_identity_mismatch")
            time.sleep(min(0.005, available))
            stopped_pid, status = process._waitpid(
                os.WUNTRACED | os.WNOHANG
            )
        if (
            stopped_pid != pid.value
            or not os.WIFSTOPPED(status)
            or not (
                os.WSTOPSIG(status) == signal.SIGSTOP
                or status == 0x7f
            )
        ):
            raise _error("toolchain_process_identity_mismatch")
        identity = _darwin_verified_launch_identity(
            pid=pid.value, executable_fd=executable_fd,
            binary_sha256=binary_sha256,
        )
        stdout = os.fdopen(
            stdout_read, "rb", buffering=0, closefd=False
        )
        spawn_state["stdout_stream"] = stdout
        process.stdout = stdout
        spawn_state["spawn_descriptor_cleanup"]["stdout_read"][
            "state"
        ] = "TRANSFERRED"
        stdout_read = None
        stderr = os.fdopen(
            stderr_read, "rb", buffering=0, closefd=False
        )
        spawn_state["stderr_stream"] = stderr
        process.stderr = stderr
        spawn_state["spawn_descriptor_cleanup"]["stderr_read"][
            "state"
        ] = "TRANSFERRED"
        stderr_read = None
        cleanup_spawn_resources()
        finalize_lease(process, identity)
        remaining(5.0)
        spawn_state["resume_intent"] = True
        os.kill(pid.value, signal.SIGCONT)
        resumed = True
        spawn_state["resumed"] = True
        return process, identity
    except BaseException as original_error:
        # The pre-registered authority owns cleanup.  In particular, the
        # pid_cell may have been written even when posix_spawn did not return.
        spawn_state["resume_uncertain"] = bool(
            spawn_state.get("resume_intent") and not resumed
        )
        try:
            cleanup_spawn_resources()
        except BaseException as cleanup_error:
            spawn_state["spawn_cleanup_error"] = cleanup_error
            if not isinstance(original_error, Exception):
                raise original_error
            if not isinstance(cleanup_error, Exception):
                raise cleanup_error
        raise original_error


class ReviewedHistoricalToolchain:
    """Non-serializable held-descriptor capability for fixed invocations."""

    __slots__ = (
        "_bin_fd",
        "_directories",
        "_binaries",
        "_identity",
        "_closed",
        "_close_state",
        "_process_leases",
        "_historical_process_remaining",
    )

    def __init__(
        self,
        directories: Sequence[
            Tuple[int, Optional[int], Optional[str], Tuple[int, ...]]
        ],
        binaries: Mapping[str, Tuple[int, Tuple[int, ...], str]],
        expected: Mapping[str, str],
    ) -> None:
        self._directories = tuple(directories)
        self._bin_fd = self._directories[-1][0]
        self._binaries = dict(binaries)
        self._identity = _candidate_identity(expected, None)
        self._closed = False
        self._process_leases = {}
        self._historical_process_remaining = None
        self._close_state = {
            "phase": "binaries",
            "binaries": [fd for fd, _metadata, _digest in self._binaries.values()],
            "directories": [
                fd
                for fd, _parent_fd, _name, _metadata in reversed(self._directories)
            ],
            "attempted_fds": set(),
            "control": None,
            "ordinary": False,
        }

    def _bind_historical_anvil_process_budget(self, *, remaining: Any) -> None:
        if (
            self._closed or not callable(remaining)
            or self._historical_process_remaining is not None
        ):
            raise _error("toolchain_process_failed")
        self._historical_process_remaining = remaining
        return None

    def __repr__(self) -> str:
        return "ReviewedHistoricalToolchain(<sealed>)"

    def __reduce__(self) -> Any:
        raise TypeError("ReviewedHistoricalToolchain is not serializable")

    def __enter__(self) -> "ReviewedHistoricalToolchain":
        if self._closed:
            raise _error("toolchain_capability_closed")
        return self

    def __exit__(self, _type: Any, _value: Any, _traceback: Any) -> None:
        if _type is None:
            self._close()
            return
        try:
            self._close()
        except BaseException:
            pass

    def __del__(self) -> None:
        try:
            self._close()
        except Exception:
            pass

    @property
    def verified_identity(self) -> Mapping[str, Any]:
        return json.loads(_canonical_json_bytes(self._identity).decode("utf-8"))

    def verified_project_input_identity(self) -> Mapping[str, Any]:
        """Return the fixed URL-free project-input identity while held."""
        if self._closed:
            raise _error("toolchain_capability_closed")
        self._assert_stable_binaries()
        verified = _verify_project_inputs()
        self._assert_stable_binaries()
        physical = verified["physical_sha256"]
        return {
            "schema": "historical_foundry_project_input_identity/v1",
            "foundry_toml_sha256": physical["foundry.toml"],
            "foundry_lock_sha256": physical["foundry.lock"],
            "gitmodules_sha256": physical[".gitmodules"],
            "forge_std_commit": verified["forge_std_commit"],
            "forge_std_tree_sha256": verified["forge_std_tree_sha256"],
        }

    def _close(self) -> None:
        if self._closed:
            return
        process_control = None
        process_ordinary = False
        for lease in tuple(self._process_leases.values()):
            try:
                lease.close()
            except BaseException as error:
                if not isinstance(error, Exception):
                    if process_control is None:
                        process_control = error
                else:
                    process_ordinary = True
        if self._process_leases:
            if process_control is not None:
                raise process_control
            raise _error("toolchain_process_reap_failed") from None
        if process_control is not None:
            raise process_control
        if process_ordinary:
            raise _error("toolchain_process_reap_failed") from None
        state = self._close_state
        while state["phase"] != "closed":
            try:
                phase = state["phase"]
                if phase in ("binaries", "directories"):
                    remaining = state[phase]
                    if remaining:
                        fd = remaining[0]
                        if fd in state["attempted_fds"]:
                            del remaining[0]
                            continue
                        try:
                            state["attempted_fds"].add(fd); del remaining[0]; os.close(fd)
                        except BaseException as error:
                            if isinstance(error, Exception):
                                state["ordinary"] = True
                            elif state["control"] is None:
                                state["control"] = error
                        continue
                    state["phase"] = (
                        "directories" if phase == "binaries" else "finish"
                    )
                    continue
                if phase == "finish":
                    state["phase"] = "closed"; self._closed = True
                    continue
                raise _error("toolchain_descriptor_cleanup_failed")
            except BaseException as error:
                if isinstance(error, Exception):
                    state["ordinary"] = True
                elif state["control"] is None:
                    state["control"] = error

        control = state["control"]
        ordinary = state["ordinary"]
        state["control"] = None
        state["ordinary"] = False
        if control is not None:
            raise control
        if ordinary:
            raise _error("toolchain_descriptor_cleanup_failed") from None

    def _assert_stable_directories(self) -> None:
        try:
            for index, (directory_fd, parent_fd, name, held_metadata) in enumerate(
                self._directories
            ):
                descriptor = os.fstat(directory_fd)
                if index == 0:
                    path_metadata = os.stat(
                        str(_PROJECT_ROOT), follow_symlinks=False
                    )
                else:
                    path_metadata = os.stat(
                        name,
                        dir_fd=parent_fd,
                        follow_symlinks=False,
                    )
                if (
                    (
                        index == 0
                        and (
                            _directory_identity(descriptor)
                            != _directory_identity_from_stable(held_metadata)
                            or _directory_identity(path_metadata)
                            != _directory_identity_from_stable(held_metadata)
                        )
                    )
                    or (
                        index != 0
                        and (
                            _stable_metadata(descriptor) != held_metadata
                            or _stable_metadata(path_metadata) != held_metadata
                        )
                    )
                ):
                    raise _error("toolchain_directory_changed")
        except OSError as error:
            raise _error("toolchain_directory_changed") from error

    def _assert_stable_binaries(self) -> None:
        self._assert_stable_directories()
        try:
            for binary_name, (binary_fd, held_metadata, expected_digest) in self._binaries.items():
                descriptor = os.fstat(binary_fd)
                path_metadata = os.stat(
                    binary_name, dir_fd=self._bin_fd, follow_symlinks=False
                )
                if (
                    _stable_metadata(descriptor) != held_metadata
                    or _stable_metadata(path_metadata) != held_metadata
                    or _hash_fd(binary_fd) != expected_digest
                ):
                    raise _error("toolchain_binary_changed")
        except OSError as error:
            raise _error("toolchain_binary_changed") from error

    def _invoke(
        self,
        name: str,
        arguments: Tuple[str, ...],
        timeout: int = 30,
    ) -> subprocess.CompletedProcess:
        if self._closed or name not in self._binaries:
            raise _error("toolchain_capability_closed")
        fd, _held_metadata, _expected_digest = self._binaries[name]
        _verify_project_inputs()
        self._assert_stable_binaries()
        if os.chdir not in os.supports_fd:
            raise _error("toolchain_descriptor_exec_unsupported")
        held_bin_fd = self._bin_fd

        def enter_held_binary_directory() -> None:
            os.fchdir(held_bin_fd)
        environment = {
            "HOME": str(_PROJECT_ROOT / ".historical-foundry" / "runtime-home"),
            "LANG": "C",
            "LC_ALL": "C",
        }
        process_error: Optional[BaseException] = None
        completed: Optional[subprocess.CompletedProcess] = None
        try:
            completed = subprocess.run(
                (name,) + arguments,
                executable="./" + name,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                timeout=timeout,
                check=False,
                close_fds=True,
                pass_fds=(self._bin_fd, fd),
                preexec_fn=enter_held_binary_directory,
                env=environment,
            )
        except (OSError, subprocess.SubprocessError) as error:
            process_error = error
        process_failed = process_error is not None or completed is None
        process_error = None
        self._assert_stable_binaries()
        _verify_project_inputs()
        if process_failed:
            raise _error("toolchain_process_failed")
        if len(completed.stdout) > _MAX_PROCESS_OUTPUT or len(completed.stderr) > _MAX_PROCESS_OUTPUT:
            raise _error("toolchain_process_output_too_large")
        return completed

    def _spawn_historical_anvil_process(
        self,
        *,
        selected_block: int,
        hardfork: str,
        relay_port: int,
        anvil_port: int,
    ) -> _HistoricalProcessLease:
        if (
            self._closed
            or type(selected_block) is not int
            or selected_block < 0
            or hardfork != _COMPILER_SETTINGS["fork_hardfork"]
            or type(relay_port) is not int
            or type(anvil_port) is not int
            or not 1 <= relay_port <= 65_535
            or not 1 <= anvil_port <= 65_535
            or relay_port == anvil_port
        ):
            raise _error("toolchain_process_failed")
        remaining = self._historical_process_remaining
        self._historical_process_remaining = None
        if not callable(remaining):
            raise _error("toolchain_process_failed")
        self._assert_stable_binaries()
        anvil_fd, _metadata, anvil_sha256 = self._binaries["anvil"]
        private_parent = _PROJECT_ROOT
        work_directory = None
        arguments = (
            "anvil",
            "--fork-url", "http://127.0.0.1:{}".format(relay_port),
            "--fork-block-number", str(selected_block),
            "--chain-id", "1",
            "--fork-chain-id", "1",
            "--accounts", "0",
            "--gas-price", "0",
            "--disable-default-create2-deployer",
            "--hardfork", hardfork,
            "--host", "127.0.0.1",
            "--port", str(anvil_port),
            "--no-mining", "--no-cors", "--silent", "--order", "fifo",
            "--steps-tracing", "--retries", "0", "--timeout", "30000",
            "--no-storage-caching",
        )
        environment = {
            "HOME": str(_PROJECT_ROOT / ".historical-foundry" / "runtime-home"),
            "LANG": "C",
            "LC_ALL": "C",
            "NO_PROXY": "127.0.0.1",
            "no_proxy": "127.0.0.1",
        }
        work_fd = None
        executable_fd = None
        process = None
        executable_name = ".reviewed-anvil"
        retained_work_fd = None
        retained_executable_fd = None
        executable_identity = None
        launch_identity = None
        cleanup_state = {
            "unlink_attempted": False, "unlinked": False,
            "dir_fsynced": False, "executable_fd_close": "OPEN",
            "work_fd_close": "OPEN", "rmdir_attempted": False,
            "rmdir_done": False, "parent_fsynced": False,
        }
        spawn_state: Dict[str, Any] = {
            "pid": None, "pid_cell": ctypes.c_int(0),
            "process": None, "reaped": False, "returncode": None,
            "reap_uncertain": False, "lease": None,
        }
        pending = None

        def close_material_fd(slot_name: str, descriptor: int) -> None:
            state = cleanup_state[slot_name]
            if state == "CLOSED":
                return None
            if state == "CLOSE_UNCERTAIN":
                raise _error("toolchain_process_cleanup_failed")
            cleanup_state[slot_name] = "ATTEMPTING"
            try:
                os.close(descriptor)
            except BaseException:
                try:
                    os.fstat(descriptor)
                except OSError as observed:
                    if observed.errno == errno.EBADF:
                        cleanup_state[slot_name] = "CLOSED"
                    else:
                        cleanup_state[slot_name] = "CLOSE_UNCERTAIN"
                else:
                    cleanup_state[slot_name] = "CLOSE_UNCERTAIN"
                raise
            cleanup_state[slot_name] = "CLOSED"
            return None

        def cleanup() -> None:
            nonlocal executable_identity
            for stream_name in ("stdout_read", "stderr_read"):
                descriptor_cleanup = spawn_state.get(
                    "spawn_descriptor_cleanup", {}
                ).get(stream_name)
                if (
                    type(descriptor_cleanup) is dict
                    and descriptor_cleanup.get("state") == "TRANSFERRED"
                ):
                    descriptor_cleanup["state"] = "OPEN"
            spawn_resource_cleanup = spawn_state.get(
                "spawn_resource_cleanup"
            )
            if callable(spawn_resource_cleanup):
                spawn_resource_cleanup()
            if work_directory is None:
                return None
            if spawn_state.get("work_identity") is None:
                mkdir_state = spawn_state.get("mkdir_state")
                if mkdir_state == "INTENT":
                    return None
                if mkdir_state == "ATTEMPTING":
                    try:
                        os.stat(
                            work_directory.name,
                            dir_fd=self._directories[0][0],
                            follow_symlinks=False,
                        )
                    except FileNotFoundError:
                        return None
                    raise _error("toolchain_directory_unsafe")
                raise _error("toolchain_directory_unsafe")
            if (
                retained_executable_fd is not None
                and executable_identity is None
            ):
                descriptor = os.fstat(retained_executable_fd)
                try:
                    installed = os.stat(
                        executable_name, dir_fd=retained_work_fd,
                        follow_symlinks=False,
                    )
                except FileNotFoundError:
                    installed = None
                candidate_identity = (descriptor.st_dev, descriptor.st_ino)
                if (
                    not stat.S_ISREG(descriptor.st_mode)
                    or installed is None
                    or (installed.st_dev, installed.st_ino)
                    != candidate_identity
                ):
                    raise _error("toolchain_binary_changed")
                executable_identity = candidate_identity
            if (
                executable_identity is not None
                and cleanup_state["executable_fd_close"] == "OPEN"
            ):
                descriptor = os.fstat(retained_executable_fd)
                if (
                    not stat.S_ISREG(descriptor.st_mode)
                    or (descriptor.st_dev, descriptor.st_ino)
                    != executable_identity
                    or (
                        spawn_state.get("materialization_complete")
                        and _hash_fd(retained_executable_fd) != anvil_sha256
                    )
                ):
                    raise _error("toolchain_binary_changed")
            if executable_identity is not None and not cleanup_state["unlinked"]:
                try:
                    path_descriptor = os.stat(
                        executable_name, dir_fd=retained_work_fd,
                        follow_symlinks=False,
                    )
                except FileNotFoundError:
                    if not cleanup_state["unlink_attempted"]:
                        raise _error("toolchain_binary_changed")
                    descriptor = os.fstat(retained_executable_fd)
                    if descriptor.st_nlink != 0:
                        raise _error("toolchain_binary_changed")
                    cleanup_state["unlinked"] = True
                else:
                    if (
                        (path_descriptor.st_dev, path_descriptor.st_ino)
                        != executable_identity
                    ):
                        raise _error("toolchain_binary_changed")
                    cleanup_state["unlink_attempted"] = True
                    try:
                        os.unlink(executable_name, dir_fd=retained_work_fd)
                    except BaseException:
                        try:
                            missing = False
                            os.stat(
                                executable_name, dir_fd=retained_work_fd,
                                follow_symlinks=False,
                            )
                        except FileNotFoundError:
                            missing = True
                        if not missing or os.fstat(
                            retained_executable_fd
                        ).st_nlink != 0:
                            raise
                        cleanup_state["unlinked"] = True
                        raise
                    cleanup_state["unlinked"] = True
            if retained_work_fd is not None and not cleanup_state["dir_fsynced"]:
                os.fsync(retained_work_fd)
                cleanup_state["dir_fsynced"] = True
            if (
                retained_executable_fd is not None
                and cleanup_state["executable_fd_close"] != "CLOSED"
            ):
                close_material_fd(
                    "executable_fd_close", retained_executable_fd
                )
            if (
                retained_work_fd is not None
                and cleanup_state["work_fd_close"] != "CLOSED"
            ):
                close_material_fd("work_fd_close", retained_work_fd)
            parent_fd = self._directories[0][0]
            name = work_directory.name
            if not cleanup_state["rmdir_done"]:
                try:
                    current = os.stat(name, dir_fd=parent_fd, follow_symlinks=False)
                except FileNotFoundError:
                    if not cleanup_state["rmdir_attempted"]:
                        raise _error("toolchain_directory_unsafe")
                    cleanup_state["rmdir_done"] = True
                else:
                    if _directory_identity(current) != spawn_state["work_identity"]:
                        raise _error("toolchain_directory_unsafe")
                    cleanup_state["rmdir_attempted"] = True
                    os.rmdir(name, dir_fd=parent_fd)
                    cleanup_state["rmdir_done"] = True
            if not cleanup_state["parent_fsynced"]:
                os.fsync(parent_fd)
                cleanup_state["parent_fsynced"] = True
            if (
                pending is not None
                and spawn_state.get("lease") is not None
                and self._process_leases.get(id(pending)) is pending
            ):
                self._process_leases.pop(id(pending))
                pending._closed = True
            return None

        pending = _PendingHistoricalSpawnLease(
            state=spawn_state, cleanup=cleanup,
            registry=self._process_leases, remaining=remaining,
        )

        def finalize_lease(process_handle: Any, identity: Any) -> Any:
            lease = _issue_historical_process_lease_for_test(
                process=process_handle,
                cleanup=cleanup,
                binary_sha256=anvil_sha256,
                selected_block=selected_block,
                hardfork=hardfork,
                toolchain=self,
                launch_identity=identity,
                owner_registry=self._process_leases,
                _register=False,
                _handoff_pending=pending,
            )
            spawn_state["lease"] = lease
            return lease

        try:
            parent_fd = self._directories[0][0]
            work_name = ".historical-anvil-scenario-" + os.urandom(16).hex()
            work_directory = private_parent / work_name
            spawn_state["work_name"] = work_name
            spawn_state["mkdir_state"] = "INTENT"
            try:
                os.stat(work_name, dir_fd=parent_fd, follow_symlinks=False)
            except FileNotFoundError:
                pass
            else:
                raise _error("toolchain_directory_unsafe")
            spawn_state["mkdir_state"] = "ATTEMPTING"
            os.mkdir(work_name, 0o700, dir_fd=parent_fd)
            spawn_state["mkdir_state"] = "CREATED"
            work_path_metadata = os.stat(
                work_name, dir_fd=parent_fd,
                follow_symlinks=False,
            )
            spawn_state["work_identity"] = _directory_identity(work_path_metadata)
            work_fd = os.open(
                work_name,
                os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
                | getattr(os, "O_NOFOLLOW", 0)
                | getattr(os, "O_CLOEXEC", 0),
                dir_fd=parent_fd,
            )
            retained_work_fd = work_fd
            work_fd = None
            held_work = os.fstat(retained_work_fd)
            installed_work = os.stat(
                work_name, dir_fd=parent_fd, follow_symlinks=False
            )
            if (
                _directory_identity(held_work) != spawn_state["work_identity"]
                or _directory_identity(installed_work)
                != spawn_state["work_identity"]
                or held_work.st_uid != os.geteuid()
                or stat.S_IMODE(held_work.st_mode) != 0o700
                or not stat.S_ISDIR(held_work.st_mode)
            ):
                raise _error("toolchain_directory_unsafe")
            executable_fd = os.open(
                executable_name,
                os.O_RDWR | os.O_CREAT | os.O_EXCL
                | getattr(os, "O_NOFOLLOW", 0),
                0o700,
                dir_fd=retained_work_fd,
            )
            retained_executable_fd = executable_fd
            executable_fd = None
            descriptor = os.fstat(retained_executable_fd)
            path_descriptor = os.stat(
                executable_name, dir_fd=retained_work_fd,
                follow_symlinks=False,
            )
            executable_identity = (descriptor.st_dev, descriptor.st_ino)
            if (
                not stat.S_ISREG(descriptor.st_mode)
                or (path_descriptor.st_dev, path_descriptor.st_ino)
                != executable_identity
            ):
                raise _error("toolchain_binary_changed")
            offset = 0
            while True:
                chunk = os.pread(anvil_fd, 1_048_576, offset)
                if not chunk:
                    break
                written = 0
                while written < len(chunk):
                    count = os.write(retained_executable_fd, chunk[written:])
                    if count <= 0:
                        raise OSError("reviewed Anvil copy failed")
                    written += count
                offset += len(chunk)
            os.fchmod(retained_executable_fd, 0o700)
            os.fsync(retained_executable_fd)
            if _hash_fd(retained_executable_fd) != anvil_sha256:
                raise _error("toolchain_binary_changed")
            descriptor = os.fstat(retained_executable_fd)
            path_descriptor = os.stat(
                executable_name, dir_fd=retained_work_fd, follow_symlinks=False
            )
            if (
                not stat.S_ISREG(descriptor.st_mode)
                or (path_descriptor.st_dev, path_descriptor.st_ino)
                != executable_identity
            ):
                raise _error("toolchain_binary_changed")
            spawn_state["materialization_complete"] = True
            os.fsync(retained_work_fd)
            process, launch_identity = _darwin_spawn_suspended(
                executable_path=str(work_directory / executable_name),
                work_directory_fd=retained_work_fd,
                arguments=arguments,
                environment=environment,
                executable_fd=retained_executable_fd,
                binary_sha256=anvil_sha256,
                spawn_state=spawn_state,
                remaining=remaining,
                finalize_lease=finalize_lease,
            )
            spawn_state["process"] = process
            if spawn_state.get("lease") is None:
                finalize_lease(process, launch_identity)
            path_after = os.stat(
                executable_name, dir_fd=retained_work_fd,
                follow_symlinks=False
            )
            descriptor_after = os.fstat(retained_executable_fd)
            if (
                (path_after.st_dev, path_after.st_ino)
                != executable_identity
                or (descriptor_after.st_dev, descriptor_after.st_ino)
                != executable_identity
                or _hash_fd(retained_executable_fd) != anvil_sha256
            ):
                raise _error("toolchain_binary_changed")
        except BaseException as original_error:
            cleanup_error = None
            owner = spawn_state.get("lease") or pending
            if owner is not None:
                try:
                    if isinstance(owner, _HistoricalProcessLease):
                        owner._close_with_budget(remaining)
                    else:
                        owner.close()
                except BaseException as observed_cleanup_error:
                    cleanup_error = observed_cleanup_error
            if not isinstance(original_error, Exception):
                raise original_error
            if (
                cleanup_error is not None
                and not isinstance(cleanup_error, Exception)
            ):
                raise cleanup_error
            raise original_error
        finally:
            if executable_fd is not None:
                os.close(executable_fd)
            if work_fd is not None:
                os.close(work_fd)

        return spawn_state["lease"]

    def _verified_version(self, name: str) -> str:
        completed = self._invoke(name, ("--version",))
        if completed.returncode != 0:
            raise _error("toolchain_version_check_failed")
        try:
            output = completed.stdout.decode("ascii")
        except UnicodeDecodeError as error:
            raise _error("toolchain_version_check_failed") from error
        if name == "solc":
            if "Version: " + _SOLC_VERSION not in output:
                raise _error("toolchain_solc_version_mismatch")
            return _SOLC_VERSION
        return _parse_foundry_version(name, output)

    def _verify_versions_and_hardfork(self) -> None:
        for name in _BINARY_NAMES:
            self._verified_version(name)
        completed = self._invoke("anvil", ("--help",))
        if completed.returncode != 0:
            raise _error("fork_hardfork_unsupported")
        try:
            help_text = completed.stdout.decode("utf-8")
        except UnicodeDecodeError as error:
            raise _error("fork_hardfork_unsupported") from error
        _require_hardfork_support(help_text)
        _require_single_hardfork(
            _SOURCE_TABLE["compiler_settings"],
            _REVIEWED_FIXED_WINDOW_HARDFORK_PROJECTION,
        )

    def _clean_project_outputs(self) -> None:
        if self._closed:
            raise _error("toolchain_capability_closed")
        self._assert_stable_binaries()
        root_fd = self._directories[0][0]
        for name in ("out", "cache"):
            _remove_generated_tree(root_fd, name)
        self._assert_stable_binaries()

    def _verify_offline_tests(self) -> None:
        self._clean_project_outputs()
        commands = (
            (
                "build", "--offline", "--root", str(_PROJECT_ROOT),
                "--use", _sealed_solc_argument(),
            ),
            (
                "test", "--offline", "--root", str(_PROJECT_ROOT),
                "--use", _sealed_solc_argument(),
                "--match-path", "foundry/test/TwoVenueV2Unit.t.sol", "-vvv",
            ),
        )
        completed = None
        for arguments in commands:
            completed = self._invoke("forge", arguments, timeout=300)
            if completed.returncode != 0:
                raise _error("foundry_offline_tests_failed")
        if completed is None:
            raise _error("foundry_offline_tests_failed")
        try:
            output = (completed.stdout + completed.stderr).decode("utf-8")
        except UnicodeDecodeError as error:
            raise _error("foundry_offline_tests_failed") from error
        if (
            "Suite result: ok." not in output
            or re.search(r"(?<![0-9])([1-9][0-9]*) tests? passed", output) is None
        ):
            raise _error("foundry_offline_tests_failed")

    def _build_executor_artifact(self) -> Mapping[str, Any]:
        if self._closed:
            raise _error("toolchain_capability_closed")
        root_fd = self._directories[0][0]
        held_directories = []
        source_fd = None
        artifact_fd = None
        try:
            foundry_fd, foundry_metadata = _open_project_directory(root_fd, "foundry")
            held_directories.append((foundry_fd, root_fd, "foundry", foundry_metadata))
            src_fd, src_metadata = _open_project_directory(foundry_fd, "src")
            held_directories.append((src_fd, foundry_fd, "src", src_metadata))
            source_fd, source_metadata, source_bytes = _open_project_file(
                src_fd, "TwoVenueV2Executor.sol", _MAX_SIDECAR_BYTES
            )
            source_inventory = [{
                "path": _EXECUTOR_SOURCE,
                "sha256": _hash_bytes(source_bytes),
                "size": len(source_bytes),
            }]
            source_tree_sha256 = _typed_inventory_sha256(
                b"historical_foundry_executor_source_tree/v1",
                source_inventory,
            )

            self._clean_project_outputs()
            for arguments in (
                (
                    "build", "--offline", "--root", str(_PROJECT_ROOT),
                    "--use", _sealed_solc_argument(),
                    "--contracts", _EXECUTOR_SOURCE,
                    "--skip", "TwoVenueV2Fork.t.sol",
                ),
            ):
                completed = self._invoke("forge", arguments, timeout=300)
                if completed.returncode != 0:
                    raise _error("executor_build_failed")

            _assert_project_member_stable(
                src_fd,
                "TwoVenueV2Executor.sol",
                source_fd,
                source_metadata,
            )
            for fd, parent_fd, name, metadata in held_directories:
                _assert_project_member_stable(parent_fd, name, fd, metadata)
            self._assert_stable_directories()

            current_fd = root_fd
            artifact_directories = []
            for name in _EXECUTOR_ARTIFACT_DIRECTORY:
                child_fd, child_metadata = _open_project_directory(current_fd, name)
                artifact_directories.append((child_fd, current_fd, name, child_metadata))
                current_fd = child_fd
            try:
                if tuple(sorted(os.listdir(current_fd))) != (_EXECUTOR_ARTIFACT_NAME,):
                    raise _error("executor_artifact_inventory_mismatch")
            except OSError as error:
                raise _error("executor_artifact_inventory_mismatch") from error
            artifact_fd, artifact_metadata, artifact_bytes = _open_project_file(
                current_fd,
                _EXECUTOR_ARTIFACT_NAME,
                _MAX_EXECUTOR_ARTIFACT_BYTES,
            )
            creation, runtime, immutable_references = _parse_executor_artifact(
                artifact_bytes
            )
            artifact_path = "/".join(_EXECUTOR_ARTIFACT_DIRECTORY + (_EXECUTOR_ARTIFACT_NAME,))
            artifact_inventory = [{
                "path": artifact_path,
                "sha256": _hash_bytes(artifact_bytes),
                "size": len(artifact_bytes),
            }]
            result = {
                "source_tree_sha256": source_tree_sha256,
                "constructor_args": b"",
                "constructor_args_sha256": _hash_bytes(b""),
                "creation_bytecode": creation,
                "creation_bytecode_sha256": _hash_bytes(creation),
                "deployed_runtime": runtime,
                "deployed_runtime_sha256": _hash_bytes(runtime),
                "immutable_references": immutable_references,
                "immutable_references_sha256": _hash_bytes(immutable_references),
                "artifact_manifest_sha256": _typed_inventory_sha256(
                    b"historical_foundry_executor_artifact_manifest/v1",
                    artifact_inventory,
                ),
            }
            _assert_project_member_stable(
                current_fd,
                _EXECUTOR_ARTIFACT_NAME,
                artifact_fd,
                artifact_metadata,
            )
            for fd, parent_fd, name, metadata in artifact_directories:
                _assert_project_member_stable(parent_fd, name, fd, metadata)
            _assert_project_member_stable(
                src_fd,
                "TwoVenueV2Executor.sol",
                source_fd,
                source_metadata,
            )
            self._assert_stable_binaries()
            return result
        except HistoricalFoundryToolchainError:
            raise
        except OSError as error:
            raise _error("executor_artifact_changed") from error
        finally:
            if artifact_fd is not None:
                os.close(artifact_fd)
            for fd, _parent_fd, _name, _metadata in reversed(
                locals().get("artifact_directories", [])
            ):
                os.close(fd)
            if source_fd is not None:
                os.close(source_fd)
            for fd, _parent_fd, _name, _metadata in reversed(held_directories):
                os.close(fd)

    def _verify_connected_kat(self) -> None:
        fixture = _load_reviewed_historical_foundry_kat()
        endpoint = os.environ.get("DEX_DEPTH_RPC_ETH")
        if not isinstance(endpoint, str) or not endpoint.strip():
            raise _error("archive_state_unavailable")
        root_fd = self._directories[0][0]
        held_directories = []
        source_fd = None
        source_open_failed = False
        try:
            try:
                foundry_fd, foundry_metadata = _open_project_directory(
                    root_fd, "foundry"
                )
                held_directories.append(
                    (foundry_fd, root_fd, "foundry", foundry_metadata)
                )
                test_fd, test_metadata = _open_project_directory(
                    foundry_fd, "test"
                )
                held_directories.append(
                    (test_fd, foundry_fd, "test", test_metadata)
                )
                source_fd, source_metadata, source_bytes = _open_project_file(
                    test_fd, "TwoVenueV2Fork.t.sol", _MAX_SIDECAR_BYTES
                )
            except (HistoricalFoundryToolchainError, OSError):
                source_open_failed = True
            if source_open_failed or source_fd is None:
                raise _error("connected_kat_fixture_unavailable")
            source_digest = _hash_bytes(source_bytes)
            if source_digest != _KAT_FORK_SOURCE_SHA256:
                raise _error("connected_kat_fixture_invalid")

            def assert_held_source_stable() -> None:
                source_changed = False
                try:
                    _assert_project_member_stable(
                        test_fd,
                        "TwoVenueV2Fork.t.sol",
                        source_fd,
                        source_metadata,
                    )
                    if _hash_fd(source_fd) != source_digest:
                        source_changed = True
                    for fd, parent_fd, name, metadata in held_directories:
                        _assert_project_member_stable(
                            parent_fd, name, fd, metadata
                        )
                    self._assert_stable_directories()
                except (HistoricalFoundryToolchainError, OSError):
                    source_changed = True
                if source_changed:
                    raise _error("connected_kat_fixture_unavailable")

            assert_held_source_stable()
            header = fixture["block_header"]
            header_arguments = (
                "rpc",
                "--rpc-url",
                endpoint,
                "eth_getBlockByNumber",
                header["number_hex"],
                "false",
            )
            completed = None
            try:
                completed = self._invoke("cast", header_arguments)
            except HistoricalFoundryToolchainError:
                pass
            if completed is None or completed.returncode != 0:
                raise _error("archive_state_unavailable")
            assert_held_source_stable()
            live_header = None
            try:
                live_header = json.loads(
                    completed.stdout.decode("utf-8"),
                    object_pairs_hook=_reject_kat_duplicate_keys,
                    parse_constant=lambda _token: (
                        _ for _ in ()
                    ).throw(ValueError()),
                )
            except (
                HistoricalFoundryToolchainError,
                UnicodeDecodeError,
                json.JSONDecodeError,
                TypeError,
                ValueError,
            ):
                pass
            if not isinstance(live_header, dict):
                raise _error("archive_state_unavailable")
            live_projection = {
                "base_fee": live_header.get("baseFeePerGas"),
                "gas_limit": live_header.get("gasLimit"),
                "gas_used": live_header.get("gasUsed"),
                "hash": live_header.get("hash"),
                "number_hex": live_header.get("number"),
                "parent_hash": live_header.get("parentHash"),
                "state_root": live_header.get("stateRoot"),
                "timestamp_hex": live_header.get("timestamp"),
            }
            expected_projection = {
                "base_fee": header["base_fee"],
                "gas_limit": header["gas_limit"],
                "gas_used": header["gas_used"],
                "hash": header["hash"],
                "number_hex": header["number_hex"],
                "parent_hash": header["parent_hash"],
                "state_root": header["state_root"],
                "timestamp_hex": header["timestamp_hex"],
            }
            if live_projection != expected_projection:
                raise _error("authority_mismatch")
            live_number = None
            try:
                live_number = int(live_projection["number_hex"], 16)
            except (TypeError, ValueError):
                pass
            if live_number != header["number_decimal"]:
                raise _error("authority_mismatch")

            for row in fixture["archive_calls"]:
                call_object = _canonical_json_bytes({
                    "data": row["calldata"],
                    "to": row["target"],
                }).decode("ascii")
                arguments = (
                    "rpc",
                    "--rpc-url",
                    endpoint,
                    "eth_call",
                    call_object,
                    row["block_reference"],
                )
                completed = None
                try:
                    completed = self._invoke("cast", arguments)
                except HistoricalFoundryToolchainError:
                    pass
                if completed is None or completed.returncode != 0:
                    raise _error("archive_state_unavailable")
                assert_held_source_stable()
                raw_text = None
                try:
                    raw_text = completed.stdout.decode("ascii").strip()
                    if raw_text.startswith('"'):
                        raw_text = json.loads(raw_text)
                except (
                    UnicodeDecodeError,
                    json.JSONDecodeError,
                    TypeError,
                    ValueError,
                ):
                    raw_text = None
                if not isinstance(raw_text, str) or not raw_text:
                    raise _error("archive_state_unavailable")
                if raw_text != row["raw_response"]:
                    raise _error("authority_mismatch")

            self._verify_versions_and_hardfork()
            assert_held_source_stable()
            self._clean_project_outputs()
            assert_held_source_stable()
            completed = None
            try:
                completed = self._invoke(
                    "forge",
                    (
                        "test",
                        "--root",
                        str(_PROJECT_ROOT),
                        "--use",
                        _sealed_solc_argument(),
                        "--match-path",
                        _KAT_FORK_SOURCE,
                        "--fork-url",
                        endpoint,
                        "--fork-block-number",
                        str(header["number_decimal"]),
                        "-vvv",
                    ),
                    timeout=300,
                )
            except HistoricalFoundryToolchainError:
                pass
            if completed is None or completed.returncode != 0:
                raise _error("foundry_replay_failed")
            assert_held_source_stable()
            output = None
            try:
                output = (completed.stdout + completed.stderr).decode("utf-8")
            except UnicodeDecodeError:
                pass
            if output is None:
                raise _error("foundry_replay_failed")
            summaries = []
            summary_pattern = re.compile(
                r"Suite result: (ok|FAILED)\. ([0-9]+) passed; "
                r"([0-9]+) failed; ([0-9]+) skipped;(?: .*)?\Z"
            )
            for line in output.splitlines():
                match = summary_pattern.fullmatch(line.strip())
                if match is not None:
                    summaries.append(match.groups())
            if summaries != [("ok", "10", "0", "0")]:
                raise _error("foundry_replay_failed")
            assert_held_source_stable()
        finally:
            if source_fd is not None:
                os.close(source_fd)
            for fd, _parent_fd, _name, _metadata in reversed(held_directories):
                os.close(fd)


def _parse_foundry_version(name: str, output: str) -> str:
    if name not in _FOUNDRY_BINARY_NAMES or not isinstance(output, str):
        raise _error("toolchain_foundry_version_mismatch")
    lines = output.splitlines()
    if (
        len(lines) < 2
        or lines[0] != "{} Version: 1.7.1".format(name)
        or lines[1] != "Commit SHA: " + _FOUNDRY_RELEASE_COMMIT
    ):
        raise _error("toolchain_foundry_version_mismatch")
    versions = set(re.findall(r"(?<![0-9])([0-9]+\.[0-9]+\.[0-9]+)(?![0-9])", output))
    if versions != {"1.7.1"}:
        raise _error("toolchain_foundry_version_mismatch")
    return _FOUNDRY_VERSION


def _require_hardfork_support(help_text: str) -> None:
    if not isinstance(help_text, str) or re.search(r"(?<![a-z])osaka(?![a-z])", help_text.lower()) is None:
        raise _error("fork_hardfork_unsupported")


def _require_single_hardfork(
    compiler_settings: Mapping[str, Any],
    fixed_window_projection: Mapping[str, Any],
) -> None:
    if (
        not isinstance(compiler_settings, Mapping)
        or not isinstance(fixed_window_projection, Mapping)
        or set(fixed_window_projection)
        != {"anchor_hardfork", "lower_bound_hardfork"}
    ):
        raise _error("fork_window_mixed")
    evm_version = compiler_settings.get("evm_version")
    fork_hardfork = compiler_settings.get("fork_hardfork")
    projected = (
        fixed_window_projection.get("lower_bound_hardfork"),
        fixed_window_projection.get("anchor_hardfork"),
    )
    if (
        not isinstance(evm_version, str)
        or not isinstance(fork_hardfork, str)
        or evm_version != fork_hardfork
        or any(value != fork_hardfork for value in projected)
    ):
        raise _error("fork_window_mixed")


def _candidate_identity(
    binary_digests: Mapping[str, str],
    project_inputs: Optional[Mapping[str, Any]],
) -> Dict[str, Any]:
    if set(binary_digests) != set(_BINARY_NAMES):
        raise _error("toolchain_binary_identity_unreviewed")
    result = {
        "schema": "historical_foundry_toolchain_candidate/v1",
        "source_lock_sha256": _SOURCE_LOCK_SHA256,
        "foundry_release": dict(_SOURCE_TABLE["foundry_release"]),
        "binaries": [
            {"name": name, "sha256": binary_digests[name], "version": _FOUNDRY_VERSION}
            for name in _FOUNDRY_BINARY_NAMES
        ],
        "solc": {
            "artifact_sha256": _SOLC_SHA256,
            "artifact_url": _SOLC_URL,
            "sha256": binary_digests["solc"],
            "source_commit": _SOLC_SOURCE_COMMIT,
            "version": _SOLC_VERSION,
        },
        "forge_std": dict(_SOURCE_TABLE["forge_std"]),
        "compiler_settings": dict(_COMPILER_SETTINGS),
    }
    if project_inputs is not None:
        result["project_inputs"] = json.loads(
            _canonical_json_bytes(project_inputs).decode("utf-8")
        )
    return result


def bootstrap_historical_foundry_toolchain() -> Mapping[str, Any]:
    """Download, verify, atomically install, and return one candidate identity."""
    project_inputs = _verify_project_inputs()
    with tempfile.TemporaryDirectory(
        prefix="historical-foundry-download-"
    ) as download_name:
        download_directory = Path(download_name)
        download_directory.chmod(0o700)
        metadata = os.stat(str(download_directory), follow_symlinks=False)
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or metadata.st_uid != os.getuid()
            or stat.S_IMODE(metadata.st_mode) != 0o700
        ):
            raise _error("toolchain_download_staging_unsafe")
        assets = _download_reviewed_assets(download_directory)
        archive = assets[_FOUNDRY_ARCHIVE_URL]
        archive_digest = _hash_bytes(archive)
        _verify_checksum_sidecar(assets[_FOUNDRY_CHECKSUM_URL], archive_digest)
        _verify_sigstore_projection(assets[_FOUNDRY_SIGSTORE_URL], archive_digest)
        binaries = _extract_foundry_members(archive)
        binaries["solc"] = assets[_SOLC_URL]
    digests = {name: _hash_bytes(binaries[name]) for name in _BINARY_NAMES}
    if _EXPECTED_BINARY_SHA256 and dict(_EXPECTED_BINARY_SHA256) != {
        name: digests[name] for name in _FOUNDRY_BINARY_NAMES
    }:
        raise _error("toolchain_binary_identity_mismatch")
    _install_atomically(binaries)
    with _open_toolchain(digests) as capability:
        capability._verify_versions_and_hardfork()
    return _candidate_identity(digests, project_inputs)


def open_reviewed_historical_toolchain() -> "ReviewedHistoricalToolchain":
    """Return the held reviewed capability; ambient executable state is ignored."""
    _verify_project_inputs()
    expected = dict(_EXPECTED_BINARY_SHA256)
    expected.setdefault("solc", _SOLC_SHA256)
    return _open_toolchain(expected)


def _parse_cli(arguments: Sequence[str]) -> str:
    allowed = (
        "--bootstrap-reviewed",
        "--print-verified-identity",
        "--verify-offline-tests",
        "--verify-connected-kat",
    )
    supplied = tuple(arguments)
    if len(supplied) != 1 or supplied[0] not in allowed:
        sys.stderr.write("invalid_cli_arguments\n")
        raise SystemExit(2)
    return supplied[0]


def _print_identity(value: Mapping[str, Any]) -> None:
    sys.stdout.buffer.write(_canonical_json_bytes(value) + b"\n")


def _main(arguments: Sequence[str]) -> int:
    mode = _parse_cli(arguments)
    try:
        if mode == "--bootstrap-reviewed":
            _print_identity(bootstrap_historical_foundry_toolchain())
            return 0
        with open_reviewed_historical_toolchain() as capability:
            if mode != "--verify-connected-kat":
                capability._verify_versions_and_hardfork()
            if mode == "--print-verified-identity":
                _print_identity(capability.verified_identity)
            elif mode == "--verify-offline-tests":
                capability._verify_offline_tests()
                _print_identity({"schema": "historical_foundry_offline_verification/v1", "status": "verified"})
            else:
                capability._verify_connected_kat()
                _print_identity({"schema": "historical_foundry_connected_kat_verification/v1", "status": "verified"})
    except HistoricalFoundryToolchainError as error:
        sys.stderr.write(str(error) + "\n")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(_main(sys.argv[1:]))
