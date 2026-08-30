"""Sealed fresh-Anvil historical scenario replay boundary.

The public Task-6 surface accepts only the capability chain produced by the
capture, prefilter, toolchain, and executor-artifact modules.  Endpoint and
process details remain held by private leases.
"""

from __future__ import annotations

import hashlib
import gzip
import http.server
import http.client
import io
import json
import socket
import threading
import time
from types import MappingProxyType
from typing import Any, Dict, Mapping

from scripts.bootstrap_historical_foundry_toolchain import (
    open_reviewed_historical_toolchain,
)
from scripts.historical_foundry_contracts import (
    HistoricalFoundryConfigSet,
    ValidatedExecutorArtifact,
)
from scripts.route_cost_evidence import (
    keccak256,
    solidity_allowance_storage_key,
    solidity_balance_storage_key,
)


def _freeze(value: Any) -> Any:
    if type(value) is dict:
        return MappingProxyType({key: _freeze(nested) for key, nested in value.items()})
    if type(value) in (list, tuple):
        return tuple(_freeze(nested) for nested in value)
    return value


def _detach(value: Any) -> Any:
    if isinstance(value, Mapping):
        return {key: _detach(nested) for key, nested in value.items()}
    if type(value) in (list, tuple):
        return [_detach(nested) for nested in value]
    return value


def _initialize_replay_context_type():
    provenance = object()

    class HistoricalReplayContext:
        __slots__ = (
            "_config", "_staging", "_window", "_grid", "_artifact",
            "_runtime", "_runtime_sha256", "_toolchain", "_relay_lease",
            "_active_process_lease", "_closed",
        )

        def __init__(self, *, _provenance: object = None, **values: Any) -> None:
            if _provenance is not provenance:
                raise ValueError("historical replay context provenance is invalid")
            for name in self.__slots__:
                object.__setattr__(self, name, values[name])

        def __repr__(self) -> str:
            return "HistoricalReplayContext(<sealed>)"

        def __setattr__(self, _name: str, _value: Any) -> None:
            raise AttributeError("HistoricalReplayContext is immutable")

        def __copy__(self) -> Any:
            raise TypeError("HistoricalReplayContext is not copyable")

        def __deepcopy__(self, _memo: Any) -> Any:
            raise TypeError("HistoricalReplayContext is not copyable")

        def __reduce__(self) -> Any:
            raise TypeError("HistoricalReplayContext is not serializable")

        def close(self) -> None:
            if self._closed:
                return None
            control = None
            ordinary = False
            relay = self._relay_lease
            toolchain = self._toolchain
            for value in (relay, toolchain):
                closer = getattr(value, "close", None)
                if not callable(closer):
                    closer = getattr(value, "_close", None)
                if callable(closer):
                    try:
                        closer()
                    except BaseException as error:
                        if not isinstance(error, Exception) and control is None:
                            control = error
                        elif isinstance(error, Exception):
                            ordinary = True
            object.__setattr__(self, "_runtime", None)
            object.__setattr__(self, "_toolchain", None)
            object.__setattr__(self, "_relay_lease", None)
            object.__setattr__(self, "_closed", True)
            if control is not None:
                raise control
            if ordinary:
                raise ValueError("historical replay context cleanup failed")
            return None

        def __enter__(self) -> "HistoricalReplayContext":
            if self._closed:
                raise ValueError("historical replay context is closed")
            return self

        def __exit__(self, error_type: Any, error: Any, traceback: Any) -> None:
            del error_type, traceback
            try:
                self.close()
            except BaseException:
                if error is not None and not isinstance(error, Exception):
                    raise error
                raise

    def issue(**values: Any) -> HistoricalReplayContext:
        return HistoricalReplayContext(_provenance=provenance, **values)

    return HistoricalReplayContext, issue


HistoricalReplayContext, _issue_replay_context = _initialize_replay_context_type()
del _initialize_replay_context_type


def _require_context(context: Any) -> HistoricalReplayContext:
    if type(context) is not HistoricalReplayContext or context._closed:
        raise ValueError("historical replay context is invalid")
    context._staging.reread_frozen_members_unchanged()
    return context


def _validate_toolchain_identity(
    *, config: HistoricalFoundryConfigSet, toolchain: Any
) -> None:
    observed = toolchain.verified_identity
    expected = config.toolchain.value
    observed_binaries = {
        row.get("name"): row.get("sha256")
        for row in observed.get("binaries", ())
        if type(row) is dict
    }
    expected_binaries = {
        row["name"]: row["sha256"] for row in expected["binaries"]
    }
    if (
        observed_binaries != expected_binaries
        or observed.get("compiler_settings", {}).get("fork_hardfork")
        != expected["compiler_settings"]["fork_hardfork"]
        or observed.get("forge_std", {}).get("commit")
        != expected["forge_std"]["commit"]
    ):
        raise ValueError("historical replay toolchain identity differs")


def open_historical_replay_context(
    *,
    config: HistoricalFoundryConfigSet,
    staging: Any,
    window: Any,
    grid: Any,
    executor_artifact: ValidatedExecutorArtifact,
) -> HistoricalReplayContext:
    import scripts.historical_foundry_rpc as rpc
    import scripts.historical_foundry_scan as scan
    import scripts.historical_foundry_storage as storage

    if (
        type(config) is not HistoricalFoundryConfigSet
        or type(staging) is not storage.HistoricalRunStagingSnapshot
        or type(window) is not scan.ValidatedHistoricalWindow
        or type(grid) is not scan.ValidatedHistoricalPrefilterGrid
        or type(executor_artifact) is not ValidatedExecutorArtifact
    ):
        raise ValueError("historical replay capability is invalid")
    staging.reread_frozen_members_unchanged()
    identity = staging.frozen_identity_projection()
    if (
        identity.get("stage") != "prefilter_frozen"
        or identity.get("generation") != 2
        or window.scan_inventory_sha256 != identity.get("scan_inventory_sha256")
        or grid.scan_inventory_sha256 != identity.get("scan_inventory_sha256")
        or grid.row_count != window.block_count * 10
    ):
        raise ValueError("historical replay lineage differs")
    artifact_identity = executor_artifact.verified_identity
    expected_artifact = config.toolchain.value["executor_build"]
    for name, expected in expected_artifact.items():
        if artifact_identity.get(name) != expected:
            raise ValueError("historical replay executor identity differs")
    for name, loaded in (
        ("policy_physical_sha256", config.policy),
        ("authority_physical_sha256", config.authority),
        ("toolchain_physical_sha256", config.toolchain),
    ):
        if artifact_identity.get(name) != loaded.physical_sha256:
            raise ValueError("historical replay config identity differs")
    runtime = executor_artifact._deployed_runtime_for_state_override()
    if (
        type(runtime) is not bytes
        or hashlib.sha256(runtime).hexdigest()
        != artifact_identity["deployed_runtime_sha256"]
    ):
        raise ValueError("historical replay executor runtime differs")
    toolchain = None
    relay_lease = None
    try:
        toolchain = open_reviewed_historical_toolchain()
        _validate_toolchain_identity(config=config, toolchain=toolchain)
        relay_lease = storage._consume_historical_relay_lease_for_replay(
            staging=staging
        )
        rpc._require_historical_relay_lease(relay_lease)
        return _issue_replay_context(
            _config=config,
            _staging=staging,
            _window=window,
            _grid=grid,
            _artifact=executor_artifact,
            _runtime=runtime,
            _runtime_sha256=artifact_identity["deployed_runtime_sha256"],
            _toolchain=toolchain,
            _relay_lease=relay_lease,
            _active_process_lease=None,
            _closed=False,
        )
    except BaseException:
        if relay_lease is not None:
            relay_lease.close()
        if toolchain is not None:
            toolchain._close()
        raise


def _uint256_hex(value: int) -> str:
    if type(value) is not int or not 0 <= value < 1 << 256:
        raise ValueError("historical replay integer is invalid")
    return "0x" + value.to_bytes(32, "big").hex()


def _execute_calldata(direction: str, amount_weth_in: int) -> str:
    direction_index = {
        "uniswap_to_sushiswap": 0,
        "sushiswap_to_uniswap": 1,
    }.get(direction)
    if direction_index is None:
        raise ValueError("historical replay direction is invalid")
    selector = keccak256(b"execute(uint8,uint256)")[:4]
    return "0x" + (
        selector
        + direction_index.to_bytes(32, "big")
        + amount_weth_in.to_bytes(32, "big")
    ).hex()


def build_historical_state_override(
    *, context: HistoricalReplayContext, scenario: Any
) -> Mapping[str, Any]:
    checked = _require_context(context)
    import scripts.historical_foundry_scan as scan

    row = scan._validate_replay_scenario_for_context(
        scenario=scenario,
        staging=checked._staging,
        window=checked._window,
        grid=checked._grid,
    )
    authority = checked._config.authority.value
    policy = checked._config.policy.value
    tokens = {token["role"]: token for token in authority["tokens"]}
    venues = {venue["venue_id"]: venue for venue in authority["venues"]}
    executor = authority["executor"]["address"]
    sender = authority["sender"]["address"]
    amount_in = row["amount_weth_in_wei"]
    predicted_uni = row["first_amount_out_raw"]
    first_venue, second_venue = (
        ("uniswap_v2", "sushiswap_v2")
        if row["direction"] == "uniswap_to_sushiswap"
        else ("sushiswap_v2", "uniswap_v2")
    )
    accounts: Dict[str, Dict[str, Any]] = {
        sender: {"balance": 0, "nonce": authority["sender"]["nonce"], "storage": {}},
        executor: {
            "balance": 0,
            "nonce": authority["executor"]["prior_nonce"],
            "code": "0x" + checked._runtime.hex(),
            "code_sha256": checked._runtime_sha256,
            "storage": {},
        },
        tokens["weth"]["address"]: {
            "balance_delta": amount_in,
            "storage": {},
        },
        tokens["uni"]["address"]: {"storage": {}},
    }
    weth_balance_slot = solidity_balance_storage_key(
        executor, tokens["weth"]["balance_descriptor"]["slot"]
    )
    uni_balance_slot = solidity_balance_storage_key(
        executor, tokens["uni"]["balance_descriptor"]["slot"]
    )
    accounts[tokens["weth"]["address"]]["storage"][weth_balance_slot] = amount_in
    accounts[tokens["uni"]["address"]]["storage"][uni_balance_slot] = 0
    for token_role, token in tokens.items():
        for venue_id, venue in venues.items():
            value = 0
            if token_role == "weth" and venue_id == first_venue:
                value = amount_in
            elif token_role == "uni" and venue_id == second_venue:
                value = predicted_uni
            slot = solidity_allowance_storage_key(
                executor,
                venue["router_address"],
                token["allowance_descriptor"]["slot"],
            )
            accounts[token["address"]]["storage"][slot] = value
    priority_fee = row["fee"]["p50_priority_fee_per_gas"]
    max_fee = (
        policy["fees"]["max_fee_multiplier"] * row["child_base_fee_wei"]
        + priority_fee
    )
    gas_limit = policy["execution"]["transaction_gas_limit"]
    accounts[sender]["balance"] = gas_limit * max_fee
    calldata = _execute_calldata(row["direction"], amount_in)
    value = {
        "schema": "historical_foundry_state_override/v1",
        "scenario_key": row["scenario_key"],
        "block_number": row["block_number"],
        "block_hash": row["block_hash"],
        "state_root": row["header"]["state_root"],
        "executor_runtime_sha256": checked._runtime_sha256,
        "changed_accounts": sorted(accounts),
        "accounts": accounts,
        "prior_values": {
            "executor_code": "0x",
            "executor_nonce": 0,
            "executor_native_balance": 0,
            "executor_uni_balance": 0,
            "executor_weth_balance": 0,
            "allowances": [0, 0, 0, 0],
        },
        "synthetic_block": {
            "number": row["block_number"] + 1,
            "timestamp": row["header"]["timestamp"]
            + policy["execution"]["synthetic_timestamp_offset_seconds"],
            "base_fee_per_gas": row["child_base_fee_wei"],
        },
        "transaction": {
            "type": "0x2",
            "from": sender,
            "to": executor,
            "nonce": policy["execution"]["sender_nonce"],
            "gas": gas_limit,
            "maxPriorityFeePerGas": priority_fee,
            "maxFeePerGas": max_fee,
            "accessList": [],
            "value": 0,
            "input": calldata,
            "calldata_sha256": hashlib.sha256(
                bytes.fromhex(calldata[2:])
            ).hexdigest(),
        },
    }
    return _detach(_freeze(value))


def _start_historical_relay(*, context: HistoricalReplayContext) -> Any:
    checked = _require_context(context)
    import scripts.historical_foundry_rpc as rpc

    relay_authority = rpc._require_historical_relay_lease(
        checked._relay_lease
    )
    state: Dict[str, Any] = {"server": None, "thread": None, "closed": False}

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, _format: str, *args: Any) -> None:
            del args
            return None

        def do_POST(self) -> None:
            started = time.monotonic()
            try:
                header_bytes = sum(
                    len(name.encode("latin-1"))
                    + len(value.encode("latin-1")) + 4
                    for name, value in self.headers.items()
                )
                raw_length = self.headers.get("Content-Length")
                if (
                    self.path != "/"
                    or header_bytes > 65_536
                    or type(raw_length) is not str
                    or not raw_length.isdigit()
                    or self.headers.get("Transfer-Encoding") is not None
                ):
                    raise ValueError("historical relay inbound request is invalid")
                length = int(raw_length)
                if not 0 < length <= 4_194_304:
                    raise ValueError("historical relay inbound request is invalid")
                body = self.rfile.read(length)
                if len(body) != length:
                    raise ValueError("historical relay inbound request is invalid")
                response = rpc._relay_historical_archive_call(
                    relay_lease=relay_authority,
                    canonical_request_bytes=body,
                )
                elapsed = time.monotonic() - started
                rpc._validate_historical_relay_resource_counts(
                    inbound_header_bytes=header_bytes,
                    inbound_body_bytes=len(body),
                    upstream_request_bytes=len(body),
                    upstream_header_bytes=0,
                    upstream_wire_bytes=len(response),
                    upstream_decoded_bytes=len(response),
                    downstream_header_bytes=128,
                    downstream_body_bytes=len(response),
                    cumulative_wire_bytes=len(response),
                    cumulative_decoded_bytes=len(response),
                    elapsed_seconds=elapsed,
                )
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(response)))
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(response)
                self.wfile.flush()
            except BaseException:
                self.close_connection = True
                try:
                    self.send_error(400)
                except BaseException:
                    pass

    class Server(http.server.ThreadingHTTPServer):
        daemon_threads = True
        allow_reuse_address = False

        def handle_error(self, _request: Any, _client_address: Any) -> None:
            return None

    server = Server(("127.0.0.1", 0), Handler)
    port = server.server_address[1]
    thread = threading.Thread(
        target=server.serve_forever,
        kwargs={"poll_interval": 0.05},
        name="historical-foundry-relay",
        daemon=True,
    )
    state.update({"server": server, "thread": thread})
    thread.start()

    class RelayServerLease:
        __slots__ = ("port",)

        def __init__(self, value: int) -> None:
            object.__setattr__(self, "port", value)

        def __repr__(self) -> str:
            return "RelayServerLease(<sealed>)"

        def __setattr__(self, _name: str, _value: Any) -> None:
            raise AttributeError("RelayServerLease is immutable")

        def close(self) -> None:
            if state["closed"]:
                return None
            state["closed"] = True
            server.shutdown()
            server.server_close()
            thread.join(timeout=5.0)
            if thread.is_alive():
                raise ValueError("historical relay server cleanup failed")
            return None

    return RelayServerLease(port)


_HISTORICAL_LOCAL_RPC_METHODS = frozenset((
    "eth_chainId", "eth_getBlockByNumber", "eth_getBlockByHash",
    "eth_getTransactionByHash",
    "anvil_setBalance", "anvil_setNonce", "anvil_setCode",
    "anvil_setStorageAt", "eth_getBalance", "eth_getTransactionCount",
    "eth_getCode", "eth_getStorageAt", "evm_setNextBlockTimestamp",
    "anvil_setNextBlockBaseFeePerGas", "eth_sendTransaction",
    "anvil_mine", "eth_getTransactionReceipt", "debug_traceTransaction",
    "eth_call", "evm_setAutomine", "anvil_impersonateAccount",
    "anvil_stopImpersonatingAccount",
))


def _validate_historical_local_rpc_call(
    *, method: str, request_byte_count: int,
    decoded_response_byte_count: int, elapsed_seconds: Any
) -> None:
    if (
        type(method) is not str
        or method not in _HISTORICAL_LOCAL_RPC_METHODS
        or type(request_byte_count) is not int
        or not 0 <= request_byte_count <= 4_194_304
        or type(decoded_response_byte_count) is not int
        or not 0 <= decoded_response_byte_count <= 67_108_864
        or type(elapsed_seconds) not in (int, float)
        or elapsed_seconds < 0
    ):
        raise ValueError("historical local RPC boundary is invalid")
    if elapsed_seconds >= 30:
        raise TimeoutError("historical local RPC deadline exceeded")
    return None


def _validate_historical_scenario_elapsed(elapsed_seconds: float) -> None:
    if (
        type(elapsed_seconds) not in (int, float)
        or isinstance(elapsed_seconds, bool)
        or elapsed_seconds < 0
    ):
        raise ValueError("historical scenario elapsed time is invalid")
    if elapsed_seconds >= 120.0:
        raise TimeoutError("historical scenario deadline expired")
    return None


def _validate_historical_fork_base_header(
    *, raw_header: Any, expected_header: Any
) -> None:
    import scripts.historical_foundry_scan as scan

    try:
        observed = scan._normalized_from_raw(raw_header)
        expected = scan._validate_normalized_header(expected_header)
    except (TypeError, ValueError):
        raise ValueError("historical fork base differs") from None
    if observed != expected:
        raise ValueError("historical fork base differs")
    return None


def _extract_actual_first_leg_uni_raw(
    *, raw_receipt: Mapping[str, Any], uni_address: str,
    executor_address: str,
) -> int:
    if (
        type(raw_receipt) is not dict
        or type(uni_address) is not str
        or type(executor_address) is not str
    ):
        raise ValueError("historical receipt transfer evidence is invalid")
    topic = "0x" + keccak256(b"Transfer(address,address,uint256)").hex()
    recipient = "0x" + "0" * 24 + executor_address[2:].lower()
    amounts = []
    logs = raw_receipt.get("logs")
    if type(logs) is not list:
        raise ValueError("historical receipt transfer evidence is invalid")
    for row in logs:
        if (
            type(row) is not dict
            or type(row.get("address")) is not str
            or row["address"].lower() != uni_address.lower()
        ):
            continue
        topics = row.get("topics")
        if (
            type(topics) is not list
            or len(topics) != 3
            or type(topics[0]) is not str
            or topics[0].lower() != topic
            or type(topics[2]) is not str
            or topics[2].lower() != recipient
        ):
            continue
        data = row.get("data")
        if (
            type(data) is not str
            or len(data) != 66
            or not data.startswith("0x")
        ):
            raise ValueError("historical receipt transfer evidence is invalid")
        try:
            amounts.append(int(data[2:], 16))
        except ValueError:
            raise ValueError(
                "historical receipt transfer evidence is invalid"
            ) from None
    if not amounts or any(value <= 0 for value in amounts):
        raise ValueError("historical receipt transfer evidence is invalid")
    return sum(amounts)


def _balance_of_calldata(owner: str) -> str:
    if type(owner) is not str or len(owner) != 42 or not owner.startswith("0x"):
        raise ValueError("historical token balance owner is invalid")
    try:
        encoded = bytes.fromhex(owner[2:])
    except ValueError:
        raise ValueError("historical token balance owner is invalid") from None
    if len(encoded) != 20:
        raise ValueError("historical token balance owner is invalid")
    return "0x70a08231" + (b"\0" * 12 + encoded).hex()


def _parse_uint256_result(value: Any) -> int:
    if type(value) is not str or len(value) != 66 or not value.startswith("0x"):
        raise ValueError("historical token getter result is invalid")
    try:
        return int(value[2:], 16)
    except ValueError:
        raise ValueError("historical token getter result is invalid") from None


def _normalized_failed_router_calls(
    *, call_trace: Mapping[str, Any], router_order: tuple
) -> list:
    if type(call_trace) is not dict:
        raise ValueError("historical call trace is invalid")
    allowed = {value.lower(): index for index, value in enumerate(router_order)}
    observed = []

    def visit(node: Any) -> None:
        if type(node) is not dict:
            raise ValueError("historical call trace is invalid")
        target = node.get("to")
        if type(target) is str and target.lower() in allowed and node.get("error"):
            data = node.get("output")
            if (
                type(data) is not str
                or not data.startswith("0x")
                or len(data) < 10
                or len(data) % 2 != 0
            ):
                raise ValueError("historical call trace is invalid")
            try:
                raw = bytes.fromhex(data[2:])
            except ValueError:
                raise ValueError("historical call trace is invalid") from None
            observed.append({
                "call_path": [allowed[target.lower()]],
                "leg": (
                    "first_leg" if allowed[target.lower()] == 0
                    else "second_leg"
                ),
                "router": target.lower(),
                "revert_selector": "0x" + raw[:4].hex(),
                "revert_data_sha256": hashlib.sha256(raw).hexdigest(),
            })
        children = node.get("calls", [])
        if type(children) is not list:
            raise ValueError("historical call trace is invalid")
        for child in children:
            visit(child)

    visit(call_trace)
    return observed


def _reserve_historical_anvil_port() -> int:
    handle = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        handle.bind(("127.0.0.1", 0))
        port = handle.getsockname()[1]
    finally:
        handle.close()
    if type(port) is not int or not 1 <= port <= 65_535:
        raise ValueError("historical local port reservation failed")
    return port


def _execute_historical_local_rpc(
    *, context: HistoricalReplayContext, scenario: Any,
    override: Mapping[str, Any], anvil_port: int
) -> Mapping[str, Any]:
    checked = _require_context(context)
    import scripts.historical_foundry_scan as scan

    row = scan._validate_replay_scenario_for_context(
        scenario=scenario,
        staging=checked._staging,
        window=checked._window,
        grid=checked._grid,
    )
    if type(anvil_port) is not int or not 1 <= anvil_port <= 65_535:
        raise ValueError("historical local RPC endpoint is invalid")
    next_id = [1]

    def rpc_call(method: str, params: list) -> Any:
        identifier = next_id[0]
        next_id[0] += 1
        request_bytes = _canonical_json({
            "id": identifier, "jsonrpc": "2.0",
            "method": method, "params": params,
        })
        started = time.monotonic()
        _validate_historical_local_rpc_call(
            method=method, request_byte_count=len(request_bytes),
            decoded_response_byte_count=0, elapsed_seconds=0.0,
        )
        connection = http.client.HTTPConnection(
            "127.0.0.1", anvil_port, timeout=30.0
        )
        try:
            connection.request(
                "POST", "/", body=request_bytes,
                headers={
                    "Content-Type": "application/json",
                    "Content-Length": str(len(request_bytes)),
                    "Connection": "close",
                },
            )
            response = connection.getresponse()
            header_bytes = sum(
                len(name.encode("latin-1"))
                + len(value.encode("latin-1")) + 4
                for name, value in response.getheaders()
            )
            if response.status != 200 or header_bytes > 65_536:
                raise ValueError("historical local RPC response is invalid")
            payload = response.read(67_108_865)
        finally:
            connection.close()
        elapsed = time.monotonic() - started
        _validate_historical_local_rpc_call(
            method=method, request_byte_count=len(request_bytes),
            decoded_response_byte_count=len(payload),
            elapsed_seconds=elapsed,
        )
        def unique_object(pairs: list) -> dict:
            result = {}
            for key, value in pairs:
                if key in result:
                    raise ValueError("duplicate JSON member")
                result[key] = value
            return result

        try:
            decoded = json.loads(
                payload.decode("utf-8"), object_pairs_hook=unique_object
            )
        except Exception:
            raise ValueError("historical local RPC response is invalid") from None
        if (
            type(decoded) is not dict
            or decoded.get("id") != identifier
            or decoded.get("jsonrpc") != "2.0"
            or "error" in decoded
            or set(decoded) != {"id", "jsonrpc", "result"}
        ):
            raise ValueError("historical local RPC response is invalid")
        return decoded["result"]

    readiness_deadline = time.monotonic() + 30.0
    while True:
        process_lease = getattr(checked, "_active_process_lease", None)
        if process_lease is not None:
            process_lease._assert_output_within_limit()
        try:
            if rpc_call("eth_chainId", []) == "0x1":
                break
        except Exception:
            pass
        if time.monotonic() >= readiness_deadline:
            raise TimeoutError("historical Anvil readiness expired")
        time.sleep(0.05)
    selected = rpc_call(
        "eth_getBlockByNumber", [hex(override["block_number"]), False]
    )
    _validate_historical_fork_base_header(
        raw_header=selected, expected_header=row["header"]
    )
    for address, account in override["accounts"].items():
        prior_balance = int(rpc_call("eth_getBalance", [address, "latest"]), 16)
        if "balance_delta" in account:
            delta = account["balance_delta"]
            if type(delta) is not int or delta <= 0:
                raise ValueError("historical overlay balance delta is invalid")
            account["prior_balance"] = prior_balance
            account["balance"] = prior_balance + delta
        elif "balance" in account:
            if prior_balance != 0:
                raise ValueError("historical overlay prior balance differs")
            account["prior_balance"] = prior_balance
        if "nonce" in account and int(
            rpc_call("eth_getTransactionCount", [address, "latest"]), 16
        ) != 0:
            raise ValueError("historical overlay prior nonce differs")
        if "code" in account and rpc_call(
            "eth_getCode", [address, "latest"]
        ).lower() != "0x":
            raise ValueError("historical overlay prior code differs")
        for slot in account.get("storage", {}):
            if int(rpc_call(
                "eth_getStorageAt", [address, slot, "latest"]
            ), 16) != 0:
                raise ValueError("historical overlay prior storage differs")
    for address, account in override["accounts"].items():
        if "balance" in account:
            rpc_call("anvil_setBalance", [address, hex(account["balance"])])
        if "nonce" in account:
            rpc_call("anvil_setNonce", [address, hex(account["nonce"])])
        if "code" in account:
            rpc_call("anvil_setCode", [address, account["code"]])
        for slot, value in account.get("storage", {}).items():
            rpc_call("anvil_setStorageAt", [
                address, slot, _uint256_hex(value),
            ])
    for address, account in override["accounts"].items():
        if "balance" in account and int(
            rpc_call("eth_getBalance", [address, "latest"]), 16
        ) != account["balance"]:
            raise ValueError("historical overlay balance readback differs")
        if "nonce" in account and int(
            rpc_call("eth_getTransactionCount", [address, "latest"]), 16
        ) != account["nonce"]:
            raise ValueError("historical overlay nonce readback differs")
        if "code" in account and rpc_call(
            "eth_getCode", [address, "latest"]
        ).lower() != account["code"].lower():
            raise ValueError("historical overlay code readback differs")
        for slot, value in account.get("storage", {}).items():
            if int(rpc_call(
                "eth_getStorageAt", [address, slot, "latest"]
            ), 16) != value:
                raise ValueError("historical overlay storage readback differs")
    authority = checked._config.authority.value
    tokens = {token["role"]: token for token in authority["tokens"]}
    executor = authority["executor"]["address"]
    balance_call = _balance_of_calldata(executor)
    initial_weth = _parse_uint256_result(rpc_call("eth_call", [{
        "to": tokens["weth"]["address"], "data": balance_call,
    }, "latest"]))
    initial_uni = _parse_uint256_result(rpc_call("eth_call", [{
        "to": tokens["uni"]["address"], "data": balance_call,
    }, "latest"]))
    if initial_weth != row["amount_weth_in_wei"] or initial_uni != 0:
        raise ValueError("historical overlay token getter differs")
    override["getter_readback"] = {
        "executor_weth_balance_raw": initial_weth,
        "executor_uni_balance_raw": initial_uni,
    }
    rpc_call("evm_setNextBlockTimestamp", [
        override["synthetic_block"]["timestamp"]
    ])
    rpc_call("anvil_setNextBlockBaseFeePerGas", [
        hex(override["synthetic_block"]["base_fee_per_gas"])
    ])
    tx = override["transaction"]
    rpc_call("anvil_impersonateAccount", [tx["from"]])
    try:
        transaction_hash = rpc_call("eth_sendTransaction", [{
            "type": tx["type"], "from": tx["from"], "to": tx["to"],
            "nonce": hex(tx["nonce"]), "gas": hex(tx["gas"]),
            "maxPriorityFeePerGas": hex(tx["maxPriorityFeePerGas"]),
            "maxFeePerGas": hex(tx["maxFeePerGas"]),
            "accessList": tx["accessList"], "value": hex(tx["value"]),
            "input": tx["input"],
        }])
        rpc_call("anvil_mine", ["0x1"])
    finally:
        rpc_call("anvil_stopImpersonatingAccount", [tx["from"]])
    raw_receipt = rpc_call(
        "eth_getTransactionReceipt", [transaction_hash]
    )
    raw_trace = rpc_call(
        "debug_traceTransaction", [transaction_hash, {}]
    )
    call_trace = rpc_call(
        "debug_traceTransaction", [transaction_hash, {"tracer": "callTracer"}]
    )
    raw_transaction = rpc_call("eth_getTransactionByHash", [transaction_hash])
    child_block = rpc_call(
        "eth_getBlockByNumber", [hex(override["synthetic_block"]["number"]), False]
    )
    if (
        type(raw_receipt) is not dict
        or type(raw_trace) is not dict
        or type(call_trace) is not dict
        or type(raw_transaction) is not dict
        or type(child_block) is not dict
        or child_block.get("transactions") != [transaction_hash]
        or int(child_block.get("timestamp", "-1"), 16)
        != override["synthetic_block"]["timestamp"]
        or int(child_block.get("baseFeePerGas", "-1"), 16)
        != override["synthetic_block"]["base_fee_per_gas"]
        or raw_transaction.get("hash") != transaction_hash
        or raw_transaction.get("input", "").lower() != tx["input"].lower()
    ):
        raise ValueError("historical receipt or trace is invalid")
    receipt = {
        "schema": "historical_foundry_receipt/v1",
        "scenario_key": override["scenario_key"],
        "status": int(raw_receipt["status"], 16),
        "blockNumber": int(raw_receipt["blockNumber"], 16),
        "blockHash": raw_receipt["blockHash"].lower(),
        "transactionIndex": int(raw_receipt["transactionIndex"], 16),
        "gasUsed": int(raw_receipt["gasUsed"], 16),
        "effectiveGasPrice": int(raw_receipt["effectiveGasPrice"], 16),
        "maxFeePerGas": tx["maxFeePerGas"],
        "maxPriorityFeePerGas": tx["maxPriorityFeePerGas"],
        "transactionHash": raw_receipt["transactionHash"].lower(),
    }
    gasprice_addresses = []
    for step in raw_trace.get("structLogs", []):
        if type(step) is dict and step.get("op") == "GASPRICE":
            gasprice_addresses.append(override["transaction"]["to"])
    routers = {
        venue["venue_id"]: venue["router_address"]
        for venue in authority["venues"]
    }
    router_order = (
        (routers["uniswap_v2"], routers["sushiswap_v2"])
        if row["direction"] == "uniswap_to_sushiswap"
        else (routers["sushiswap_v2"], routers["uniswap_v2"])
    )
    failed_router_calls = _normalized_failed_router_calls(
        call_trace=call_trace, router_order=router_order
    )
    if receipt["status"] == 0:
        root_output = call_trace.get("output", raw_trace.get("returnValue"))
        receipt["revert_data"] = root_output
        actual_first_leg_uni = 0
    else:
        actual_first_leg_uni = _extract_actual_first_leg_uni_raw(
            raw_receipt=raw_receipt,
            uni_address=tokens["uni"]["address"],
            executor_address=executor,
        )
        if actual_first_leg_uni != row["first_amount_out_raw"]:
            raise ValueError("historical first-leg token delta differs")
    final_weth = _parse_uint256_result(rpc_call("eth_call", [{
        "to": tokens["weth"]["address"], "data": balance_call,
    }, "latest"]))
    residual_uni = _parse_uint256_result(rpc_call("eth_call", [{
        "to": tokens["uni"]["address"], "data": balance_call,
    }, "latest"]))
    trace = {
        "schema": "historical_foundry_trace/v1",
        "scenario_key": override["scenario_key"],
        "failed": raw_trace.get("failed") is True,
        "gasprice_opcode_addresses": sorted(set(gasprice_addresses)),
        "calls": failed_router_calls,
    }
    return {
        "selected_state": {
            "block_number": override["block_number"],
            "block_hash": override["block_hash"],
            "state_root": override["state_root"],
        },
        "token_deltas": {
            "initial_weth_raw": initial_weth,
            "initial_uni_raw": initial_uni,
            "actual_first_leg_uni_raw": actual_first_leg_uni,
            "final_weth_raw": final_weth,
            "residual_uni_raw": residual_uni,
        },
        "receipt": receipt,
        "trace": trace,
    }


def _canonical_json(value: Any) -> bytes:
    return json.dumps(
        value, ensure_ascii=False, allow_nan=False, sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")


def _deterministic_gzip(value: bytes) -> bytes:
    buffer = io.BytesIO()
    with gzip.GzipFile(
        filename="", mode="wb", compresslevel=9,
        fileobj=buffer, mtime=0,
    ) as handle:
        handle.write(value)
    return buffer.getvalue()


def _exact_terminating_decimal(numerator: int, denominator: int) -> str:
    if (
        type(numerator) is not int
        or type(denominator) is not int
        or numerator < 0
        or denominator <= 0
    ):
        raise ValueError("historical exact decimal input is invalid")
    integer, remainder = divmod(numerator, denominator)
    if remainder == 0:
        return str(integer)
    digits = []
    while remainder and len(digits) <= 4_096:
        remainder *= 10
        digit, remainder = divmod(remainder, denominator)
        digits.append(str(digit))
    if remainder:
        raise ValueError("historical exact decimal is nonterminating")
    return "{}.{}".format(integer, "".join(digits).rstrip("0"))


def _build_cost_proof_inputs(
    *, context: HistoricalReplayContext, row: Mapping[str, Any],
    receipt: Mapping[str, Any],
    receipt_sha256: str, trace_sha256: str
) -> Dict[str, Any]:
    checked = _require_context(context)
    if (
        not isinstance(row, Mapping)
        or type(receipt) is not dict
        or type(row.get("requested_notional_usd")) is not int
        or row["requested_notional_usd"] < 0
        or not isinstance(row.get("price"), Mapping)
        or type(row["price"].get("answer")) is not int
        or row["price"]["answer"] <= 0
        or type(row["price"].get("feed_decimals")) is not int
        or not 0 <= row["price"]["feed_decimals"] <= 255
        or type(receipt.get("gasUsed")) is not int
        or receipt["gasUsed"] < 0
        or type(receipt.get("effectiveGasPrice")) is not int
        or receipt["effectiveGasPrice"] < 0
    ):
        raise ValueError("historical cost proof input is invalid")
    scenario_key = row.get("scenario_key")
    if type(scenario_key) is not str or not scenario_key:
        raise ValueError("historical cost proof input is invalid")
    pool_fee = _exact_terminating_decimal(
        row["requested_notional_usd"] * 30, 10_000
    )
    gas_amount = _exact_terminating_decimal(
        receipt["gasUsed"] * receipt["effectiveGasPrice"]
        * row["price"]["answer"],
        10 ** (18 + row["price"]["feed_decimals"]),
    )
    mev_bps_exact = checked._config.policy.value["fees"]["acceptance_mev_bps"]
    if type(mev_bps_exact) is not str or not mev_bps_exact.isdigit():
        raise ValueError("historical cost proof input is invalid")
    mev_bps = int(mev_bps_exact)
    mev_amount = _exact_terminating_decimal(
        row["requested_notional_usd"] * mev_bps, 10_000
    )
    specs = (
        ("buy", "pool_swap_fee", "bounded_estimate", True, pool_fee, "30", "receipt"),
        ("buy", "router_or_integrator_fee", "bounded_estimate", False, "0", "0", "receipt"),
        ("buy", "token_transfer_tax", "bounded_estimate", False, "0", "0", "receipt"),
        ("sell", "pool_swap_fee", "bounded_estimate", True, pool_fee, "30", "receipt"),
        ("sell", "router_or_integrator_fee", "bounded_estimate", False, "0", "0", "receipt"),
        ("sell", "token_transfer_tax", "bounded_estimate", False, "0", "0", "receipt"),
        ("route", "network_gas", "assumed", False, gas_amount, None, "receipt"),
        ("route", "rebalancing_or_transfer", "not_applicable", False, None, None, "trace"),
        ("route", "mev_buffer", "assumed", False, mev_amount, mev_bps_exact, "policy"),
    )
    role_hash = {
        "receipt": receipt_sha256,
        "trace": trace_sha256,
        "policy": context._config.policy.physical_sha256,
    }
    proof = {
        "schema": "historical_foundry_cost_proof_inputs/v1",
        "scenario_key": scenario_key,
        "policy_sha256": checked._config.policy.physical_sha256,
        "receipt_sha256": receipt_sha256,
        "trace_sha256": trace_sha256,
        "adapter_proof_sha256": checked._artifact.verified_identity[
            "creation_bytecode_sha256"
        ],
        "rows": [{
            "grain": grain,
            "component": component,
            "value_status": status,
            "embedded": embedded,
            "amount_usd_exact": amount,
            "rate_bps_exact": rate,
            "proof_role": role,
            "proof_sha256": role_hash[role],
        } for grain, component, status, embedded, amount, rate, role in specs],
    }
    proof["proof_inputs_hash"] = hashlib.sha256(
        b"historical_foundry_cost_proof_inputs/v1\0"
        + _canonical_json(proof)
    ).hexdigest()
    return proof


def _open_scenario_evidence_sink(
    *, context: HistoricalReplayContext, scenario: Any
) -> Any:
    checked = _require_context(context)
    import scripts.historical_foundry_scan as scan
    import scripts.historical_foundry_storage as storage

    row = scan._validate_replay_scenario_for_context(
        scenario=scenario,
        staging=checked._staging,
        window=checked._window,
        grid=checked._grid,
    )
    token = scan._consume_replay_scenario_storage_token(
        scenario=scenario
    )
    return storage._open_historical_scenario_evidence_sink(
        staging=checked._staging,
        scenario_token=token,
        scenario_key=row["scenario_key"],
    )


def _classify_historical_revert(
    *,
    context: HistoricalReplayContext,
    scenario: Any,
    receipt: Mapping[str, Any],
    trace: Mapping[str, Any],
) -> str:
    checked = _require_context(context)
    import scripts.historical_foundry_scan as scan

    row = scan._validate_replay_scenario_for_context(
        scenario=scenario,
        staging=checked._staging,
        window=checked._window,
        grid=checked._grid,
    )
    if type(receipt) is not dict or type(trace) is not dict:
        return "unresolved"
    reason = row.get("reason")
    matrix_rows = tuple(
        candidate
        for candidate in checked._config.policy.value["closed_revert_matrix"]
        if candidate["prefilter_reason"] == reason
    )
    if len(matrix_rows) != 1:
        return "unresolved"
    matrix = matrix_rows[0]
    first_venue = (
        "uniswap_v2"
        if row["direction"] == "uniswap_to_sushiswap"
        else "sushiswap_v2"
    )
    second_venue = (
        "sushiswap_v2"
        if row["direction"] == "uniswap_to_sushiswap"
        else "uniswap_v2"
    )
    venue_id = first_venue if matrix["leg"] == "first_leg" else second_venue
    routers = {
        venue["venue_id"]: venue["router_address"]
        for venue in checked._config.authority.value["venues"]
    }
    calls = trace.get("calls")
    outer_selector = "0x" + keccak256(b"ExternalCallFailed()")[:4].hex()
    if (
        receipt.get("status") != 0
        or receipt.get("revert_data") != outer_selector
        or trace.get("failed") is not True
        or type(calls) is not list
        or len(calls) != 1
        or type(calls[0]) is not dict
    ):
        return "unresolved"
    call = calls[0]
    exact_call = {
        "call_path": [0],
        "leg": matrix["leg"],
        "router": routers[venue_id],
        "revert_selector": matrix["revert_selector"],
        "revert_data_sha256": matrix["revert_data_sha256"],
    }
    return "closed_revert" if call == exact_call else "unresolved"


def _classify_historical_outcome(
    *, context: HistoricalReplayContext, scenario: Any,
    receipt: Mapping[str, Any], trace: Mapping[str, Any],
) -> str:
    status = receipt.get("status") if type(receipt) is dict else None
    if status == 1:
        if (
            type(trace) is dict
            and trace.get("failed") is False
            and trace.get("gasprice_opcode_addresses") == []
        ):
            return "replay_success"
        return "unresolved"
    if status == 0:
        return _classify_historical_revert(
            context=context, scenario=scenario,
            receipt=receipt, trace=trace,
        )
    return "unresolved"


def _replay_historical_scenario(
    *, context: HistoricalReplayContext, scenario: Any, sink: Any
) -> Mapping[str, Any]:
    started = time.monotonic()
    checked = _require_context(context)
    import scripts.historical_foundry_storage as storage

    if type(sink) is not storage.ScenarioEvidenceSink:
        raise ValueError("historical scenario evidence sink is invalid")
    override = build_historical_state_override(
        context=checked, scenario=scenario
    )
    relay = None
    process = None
    ordinary_failure = False
    control = None
    try:
        relay = _start_historical_relay(context=checked)
        _validate_historical_scenario_elapsed(time.monotonic() - started)
        relay_port = getattr(relay, "port", None)
        anvil_port = _reserve_historical_anvil_port()
        if (
            type(relay_port) is not int
            or not 1 <= relay_port <= 65_535
            or relay_port == anvil_port
        ):
            raise ValueError("historical relay lease is invalid")
        process = checked._toolchain._spawn_historical_anvil_process(
            selected_block=override["block_number"],
            hardfork=checked._config.toolchain.value[
                "compiler_settings"
            ]["fork_hardfork"],
            relay_port=relay_port,
            anvil_port=anvil_port,
        )
        object.__setattr__(checked, "_active_process_lease", process)
        outcome = _execute_historical_local_rpc(
            context=checked, scenario=scenario,
            override=override, anvil_port=anvil_port,
        )
        process._assert_output_within_limit()
        _validate_historical_scenario_elapsed(time.monotonic() - started)
        if type(outcome) is not dict:
            raise ValueError("historical local RPC result is invalid")
        receipt = _detach(outcome.get("receipt"))
        trace = _detach(outcome.get("trace"))
        selected_state = _detach(outcome.get("selected_state"))
        token_deltas = _detach(outcome.get("token_deltas"))
        if (
            type(receipt) is not dict
            or type(trace) is not dict
            or type(selected_state) is not dict
            or type(token_deltas) is not dict
            or receipt.get("status") not in (0, 1)
            or receipt.get("scenario_key") != override["scenario_key"]
            or receipt.get("blockNumber")
            != override["synthetic_block"]["number"]
            or receipt.get("transactionIndex") != 0
        ):
            raise ValueError("historical local RPC result is invalid")
        classification = _classify_historical_outcome(
            context=checked, scenario=scenario,
            receipt=receipt, trace=trace,
        )
        if classification == "unresolved":
            raise ValueError("historical local RPC result is invalid")
        overlay_bytes = _canonical_json(override)
        receipt_bytes = _canonical_json(receipt)
        trace_bytes = _deterministic_gzip(_canonical_json(trace))
        receipt_sha = hashlib.sha256(receipt_bytes).hexdigest()
        trace_sha = hashlib.sha256(trace_bytes).hexdigest()
        proof = None
        if receipt["status"] == 1:
            import scripts.historical_foundry_scan as scan

            proof_row = scan._validate_replay_scenario_for_context(
                scenario=scenario,
                staging=checked._staging,
                window=checked._window,
                grid=checked._grid,
            )
            proof = _build_cost_proof_inputs(
                context=checked,
                row=proof_row,
                receipt=receipt,
                receipt_sha256=receipt_sha,
                trace_sha256=trace_sha,
            )
        result = {
            "schema": "historical_foundry_replay_result/v1",
            "scenario_key": override["scenario_key"],
            "status": receipt["status"],
            "classification": classification,
            "overlay_sha256": hashlib.sha256(overlay_bytes).hexdigest(),
            "receipt_sha256": receipt_sha,
            "trace_sha256": trace_sha,
        }
        if proof is not None:
            result["cost_proof_inputs"] = proof
        for role, payload in (
            ("overlay", overlay_bytes),
            ("receipt", receipt_bytes),
            ("trace", trace_bytes),
            ("result", _canonical_json(result)),
        ):
            sink.write_member(role=role, canonical_bytes=payload)
        return MappingProxyType({
            "scenario_key": override["scenario_key"],
            "selected_state": selected_state,
            "token_deltas": token_deltas,
            "gas_used": receipt["gasUsed"],
            "overlay_sha256": result["overlay_sha256"],
            "calldata_sha256": override["transaction"]["calldata_sha256"],
            "executor_runtime_sha256": checked._runtime_sha256,
            "receipt_sha256": receipt_sha,
            "trace_sha256": trace_sha,
            "classification": classification,
            "proof_inputs_hash": (
                proof["proof_inputs_hash"] if proof is not None else None
            ),
        })
    except BaseException as error:
        if not isinstance(error, Exception):
            control = error
        else:
            ordinary_failure = True
    finally:
        for lease in (process, relay):
            closer = getattr(lease, "close", None)
            if callable(closer):
                try:
                    closer()
                except BaseException as error:
                    if not isinstance(error, Exception) and control is None:
                        control = error
                    elif isinstance(error, Exception):
                        ordinary_failure = True
        object.__setattr__(checked, "_active_process_lease", None)
        try:
            _validate_historical_scenario_elapsed(
                time.monotonic() - started
            )
        except BaseException as error:
            if not isinstance(error, Exception) and control is None:
                control = error
            elif isinstance(error, Exception):
                ordinary_failure = True
        if control is not None:
            raise control
        if ordinary_failure:
            raise ValueError("historical scenario replay failed") from None
