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
import re
import socket
import sys
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


class HistoricalReplayError(RuntimeError):
    __slots__ = ("_category",)

    def __init__(self, category: str) -> None:
        allowed = {
            "fork_hardfork_unsupported", "fork_window_mixed",
            "foundry_replay_failed", "candidate_unresolved", "authority",
            "archive",
        }
        if category not in allowed:
            category = "foundry_replay_failed"
        RuntimeError.__init__(
            self, "historical scenario replay failed: " + category
        )
        object.__setattr__(self, "_category", category)

    @property
    def category(self) -> str:
        return self._category

    def __repr__(self) -> str:
        return "HistoricalReplayError({!r})".format(self._category)

    def __setattr__(self, _name: str, _value: Any) -> None:
        if _name in (
            "__traceback__", "__cause__", "__context__",
            "__suppress_context__",
        ):
            RuntimeError.__setattr__(self, _name, _value)
            return None
        raise AttributeError("HistoricalReplayError is immutable")

    def __init_subclass__(cls, **_kwargs: Any) -> None:
        raise TypeError("HistoricalReplayError is sealed")


class _HistoricalReplayBoundaryError(Exception):
    __slots__ = ("category",)

    def __init__(self, category: str) -> None:
        self.category = category
        Exception.__init__(self, category)


def _typed_historical_replay_error(error: Exception) -> HistoricalReplayError:
    if type(error) is HistoricalReplayError:
        return error
    if type(error) is _HistoricalReplayBoundaryError:
        return HistoricalReplayError(error.category)
    message = str(error)
    module_name = type(error).__module__
    if message in ("historical fork base differs", "fork_window_mixed"):
        category = "fork_window_mixed"
    elif message == "fork_hardfork_unsupported":
        category = "fork_hardfork_unsupported"
    elif (
        module_name == "scripts.historical_foundry_rpc"
        and hasattr(error, "reason_code")
        and hasattr(error, "failure_kind")
    ):
        category = "archive"
    else:
        category = "foundry_replay_failed"
    return HistoricalReplayError(category)


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
            "_active_process_lease", "_active_relay_lease", "_clock", "_run_deadline",
            "_scenario_deadline", "_closed",
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
            process = self._active_process_lease
            active_relay = self._active_relay_lease
            if process is not None:
                try:
                    process._close_with_budget(
                        lambda cap: _context_remaining(self, cap)
                    )
                except BaseException as error:
                    if not isinstance(error, Exception):
                        control = error
                    else:
                        ordinary = True
                if getattr(process, "_closed", False) is True:
                    object.__setattr__(self, "_active_process_lease", None)
            if active_relay is not None:
                try:
                    active_relay.close()
                except BaseException as error:
                    if not isinstance(error, Exception) and control is None:
                        control = error
                    elif isinstance(error, Exception):
                        ordinary = True
                closed_check = getattr(active_relay, "_is_closed", None)
                if callable(closed_check) and closed_check() is True:
                    object.__setattr__(self, "_active_relay_lease", None)
            if (
                self._active_process_lease is not None
                or self._active_relay_lease is not None
            ):
                if control is not None:
                    raise control
                raise ValueError("historical replay context cleanup failed")
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


def _remaining_historical_deadline(
    *, run_deadline: float, scenario_deadline: float,
    now: float, own_cap: float,
) -> float:
    values = (run_deadline, scenario_deadline, now, own_cap)
    if any(
        type(value) not in (int, float) or isinstance(value, bool)
        for value in values
    ) or own_cap <= 0:
        raise ValueError("historical replay deadline is invalid")
    remaining = min(own_cap, run_deadline - now, scenario_deadline - now)
    if remaining <= 0:
        raise TimeoutError("historical replay deadline expired")
    return float(remaining)


def _context_remaining(
    context: HistoricalReplayContext, own_cap: float
) -> float:
    now = context._clock()
    scenario_deadline = context._scenario_deadline
    if scenario_deadline is None:
        scenario_deadline = context._run_deadline
    return _remaining_historical_deadline(
        run_deadline=context._run_deadline,
        scenario_deadline=scenario_deadline,
        now=now, own_cap=own_cap,
    )


def _context_operation_remaining(
    context: HistoricalReplayContext, *, operation_deadline: float,
    own_cap: float,
) -> float:
    scenario_deadline = context._scenario_deadline
    if scenario_deadline is None:
        scenario_deadline = context._run_deadline
    return _remaining_historical_deadline(
        run_deadline=context._run_deadline,
        scenario_deadline=min(scenario_deadline, operation_deadline),
        now=context._clock(), own_cap=own_cap,
    )


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
    storage._verify_historical_replay_module_source(
        staging=staging,
        module_name="scripts.historical_foundry_anvil",
        module=sys.modules[__name__],
    )
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
    clock = time.monotonic
    try:
        toolchain = open_reviewed_historical_toolchain()
        _validate_toolchain_identity(config=config, toolchain=toolchain)
        relay_lease = storage._consume_historical_relay_lease_for_replay(
            staging=staging
        )
        rpc._require_historical_relay_lease(relay_lease)
        clock = relay_lease._clock
        opened_at = clock()
        if (
            type(opened_at) not in (int, float)
            or isinstance(opened_at, bool)
            or opened_at >= relay_lease._run_deadline
        ):
            raise TimeoutError("historical replay run deadline expired")
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
            _active_relay_lease=None,
            _clock=clock,
            _run_deadline=relay_lease._run_deadline,
            _scenario_deadline=None,
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


def _close_historical_relay_server(
    *, state: Mapping[str, Any], remaining: Any
) -> None:
    if not isinstance(state, Mapping) or not callable(remaining):
        raise ValueError("historical relay server cleanup failed")

    def budget(cap: float) -> float:
        try:
            value = remaining(cap)
        except TimeoutError:
            return 0.0
        if (
            type(value) not in (int, float)
            or isinstance(value, bool)
            or not 0 <= value <= cap
        ):
            raise ValueError("historical relay server cleanup failed")
        return float(value)

    server = state.get("server")
    thread = state.get("thread")
    lock = state.get("lock")
    if lock is None:
        handlers = tuple(state.get("handlers", ()))
        requests = tuple(state.get("requests", ()))
    else:
        with lock:
            handlers = tuple(state.get("handlers", ()))
            requests = tuple(state.get("requests", ()))
    if server is None or thread is None:
        raise ValueError("historical relay server cleanup failed")
    budget(5.0)
    stopper = getattr(server, "request_stop", None)
    if callable(stopper):
        stopper()
    else:
        server.shutdown()
    for request in requests:
        budget(5.0)
        try:
            request.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        request.close()
    server.server_close()
    thread.join(timeout=budget(5.0))
    for handler in handlers:
        handler.join(timeout=budget(5.0))
    if thread.is_alive() or any(handler.is_alive() for handler in handlers):
        raise ValueError("historical relay server cleanup failed")
    return None


def _start_historical_relay(
    *, context: HistoricalReplayContext, scenario: Any
) -> Any:
    checked = _require_context(context)
    import scripts.historical_foundry_rpc as rpc

    relay_authority = rpc._bind_historical_relay_scenario(
        relay_lease=checked._relay_lease,
        config=checked._config,
        scenario=scenario,
        absolute_deadline=(
            checked._scenario_deadline
            if checked._scenario_deadline is not None
            else min(
                checked._run_deadline,
                checked._clock() + 120.0,
            )
        ),
    )
    state: Dict[str, Any] = {
        "server": None, "thread": None, "closed": False,
        "handlers": set(), "requests": set(), "lock": threading.Lock(),
        "diagnostics": [], "control": None,
    }

    def record_handler_failure(error: BaseException) -> None:
        with state["lock"]:
            if (
                not isinstance(error, Exception)
                and state["control"] is None
            ):
                state["control"] = error
            else:
                state["diagnostics"].append((
                    "handler_error", "ordinary"
                ))
        return None

    class Handler(http.server.BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def setup(self) -> None:
            super().setup()
            self.request.settimeout(_context_remaining(checked, 30.0))
            current = threading.current_thread()
            current.name = "historical-foundry-relay-handler"
            with state["lock"]:
                state["handlers"].add(current)
                state["requests"].add(self.request)

        def finish(self) -> None:
            try:
                super().finish()
            finally:
                with state["lock"]:
                    state["handlers"].discard(threading.current_thread())
                    state["requests"].discard(self.request)

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
                self.request.settimeout(_context_remaining(checked, 30.0))
                body = self.rfile.read(length)
                if len(body) != length:
                    raise ValueError("historical relay inbound request is invalid")
                response = rpc._relay_historical_archive_call(
                    relay_lease=relay_authority,
                    canonical_request_bytes=body,
                )
                elapsed = time.monotonic() - started
                _context_remaining(checked, 30.0)
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
                self.request.settimeout(_context_remaining(checked, 30.0))
                self.wfile.write(response)
                self.wfile.flush()
            except BaseException as error:
                record_handler_failure(error)
                if not isinstance(error, Exception):
                    server.request_stop()
                self.close_connection = True
                if isinstance(error, Exception):
                    try:
                        self.send_error(400)
                    except BaseException:
                        pass

    class Server(http.server.ThreadingHTTPServer):
        daemon_threads = False
        block_on_close = False
        allow_reuse_address = False

        def request_stop(self) -> None:
            setattr(self, "_BaseServer__shutdown_request", True)

        def handle_error(self, _request: Any, _client_address: Any) -> None:
            return None

    server = Server(("127.0.0.1", 0), Handler)
    port = server.server_address[1]
    thread = threading.Thread(
        target=server.serve_forever,
        kwargs={"poll_interval": 0.05},
        name="historical-foundry-relay",
        daemon=False,
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
            try:
                _close_historical_relay_server(
                    state=state,
                    remaining=lambda cap: _context_remaining(checked, cap),
                )
                relay_authority.close()
            except BaseException:
                raise
            state["closed"] = True
            with state["lock"]:
                saved_control = state["control"]
                state["control"] = None
            if saved_control is not None:
                raise saved_control
            return None

        def _is_closed(self) -> bool:
            return state["closed"] is True

        def _diagnostics_for_test(self) -> Any:
            with state["lock"]:
                return tuple(state["diagnostics"])

        def _record_handler_failure_for_test(
            self, error: BaseException
        ) -> None:
            record_handler_failure(error)

    return RelayServerLease(port)


def _bind_historical_final_anchor_relay(
    *, context: HistoricalReplayContext, scenario: Any
) -> Any:
    """Bind a final reread facade without reacquiring endpoint authority."""
    checked = _require_context(context)
    import scripts.historical_foundry_rpc as rpc
    import scripts.historical_foundry_scan as scan

    scan._validate_replay_scenario_for_context(
        scenario=scenario, staging=checked._staging,
        window=checked._window, grid=checked._grid,
    )
    now = checked._clock()
    absolute_deadline = min(checked._run_deadline, now + 120.0)
    _remaining_historical_deadline(
        run_deadline=checked._run_deadline,
        scenario_deadline=absolute_deadline,
        now=now, own_cap=120.0,
    )
    return rpc._bind_historical_relay_scenario(
        relay_lease=checked._relay_lease,
        config=checked._config,
        scenario=scenario,
        absolute_deadline=absolute_deadline,
    )


_HISTORICAL_LOCAL_RPC_METHODS = frozenset((
    "eth_chainId", "eth_getBlockByNumber", "eth_getBlockByHash",
    "eth_getTransactionByHash",
    "anvil_setBalance", "anvil_setNonce", "anvil_setCode",
    "anvil_setStorageAt", "eth_getBalance", "eth_getTransactionCount",
    "eth_getCode", "eth_getStorageAt", "evm_setNextBlockTimestamp",
    "anvil_setCoinbase",
    "anvil_setNextBlockBaseFeePerGas", "eth_sendTransaction",
    "anvil_mine", "eth_getTransactionReceipt", "debug_traceTransaction",
    "eth_call", "evm_setAutomine", "anvil_impersonateAccount",
    "anvil_stopImpersonatingAccount",
))


def _validate_historical_local_coinbase_initialization(
    *, params: Any, expected_executor: str,
    already_initialized: bool, read_started: bool,
) -> None:
    if (
        type(expected_executor) is not str
        or re.fullmatch(r"0x[0-9a-f]{40}", expected_executor) is None
        or type(already_initialized) is not bool
        or type(read_started) is not bool
        or params != [expected_executor]
        or already_initialized
        or read_started
    ):
        raise ValueError("historical local coinbase initialization is invalid")
    return None


def _validate_historical_local_read_request(
    *, request: Any, expected_executor: str,
) -> Dict[str, str]:
    if (
        type(request) is not dict
        or set(request) != {"from", "to", "data"}
        or type(expected_executor) is not str
        or re.fullmatch(r"0x[0-9a-f]{40}", expected_executor) is None
        or request.get("from") != expected_executor
        or type(request.get("to")) is not str
        or re.fullmatch(r"0x[0-9a-f]{40}", request["to"]) is None
        or type(request.get("data")) is not str
        or re.fullmatch(r"0x(?:[0-9a-f]{2})*", request["data"]) is None
    ):
        raise ValueError("historical local read request is invalid")
    return dict(request)


def _historical_struct_trace_config() -> Dict[str, bool]:
    return {
        "disableStack": False,
        "disableStorage": False,
        "enableMemory": True,
        "enableReturnData": True,
    }


def _validate_historical_struct_trace_config(value: Any) -> None:
    if type(value) is not dict or value != _historical_struct_trace_config():
        raise ValueError("historical struct trace configuration is invalid")
    return None


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


def _decode_historical_local_rpc_response(
    *, payload: bytes, identifier: int
) -> Any:
    import scripts.historical_foundry_rpc as rpc

    if type(identifier) is not int or identifier < 0:
        raise ValueError("historical local RPC response is invalid")
    try:
        decoded = rpc._decode_historical_relay_json(
            payload, limit=67_108_864, require_canonical=False
        )
    except ValueError:
        raise ValueError("historical local RPC response is invalid") from None
    if (
        type(decoded) is not dict
        or set(decoded) != {"id", "jsonrpc", "result"}
        or decoded.get("id") != identifier
        or decoded.get("jsonrpc") != "2.0"
    ):
        raise ValueError("historical local RPC response is invalid")
    canonical = rpc._archive_canonical_bytes(decoded)
    try:
        canonical_decoded = rpc._decode_historical_relay_json(
            canonical, limit=67_108_864
        )
    except ValueError:
        raise ValueError("historical local RPC response is invalid") from None
    if canonical_decoded != decoded:
        raise ValueError("historical local RPC response is invalid")
    return canonical_decoded["result"]


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


def _validate_historical_pair_closure(
    *, expected: Mapping[str, Any], before: Mapping[str, Any],
    after: Mapping[str, Any]
) -> Dict[str, Any]:
    authority_fields = (
        "pair_address", "reserve_uni_raw", "reserve_weth_raw",
    )
    observed_fields = (
        "pair_address", "reserve_uni_raw", "reserve_weth_raw",
        "pair_uni_balance_raw", "pair_weth_balance_raw",
    )
    if (
        type(expected) is not dict
        or type(before) is not dict
        or type(after) is not dict
        or tuple(expected) != ("uniswap_v2", "sushiswap_v2")
        or tuple(before) != tuple(expected)
        or tuple(after) != tuple(expected)
    ):
        raise ValueError("historical pair closure is invalid")
    detached = {}
    for venue_id in expected:
        rows = (expected[venue_id], before[venue_id], after[venue_id])
        if any(type(value) is not dict for value in rows):
            raise ValueError("historical pair closure is invalid")
        if (
            tuple(rows[0]) != authority_fields
            or tuple(rows[1]) != observed_fields
            or tuple(rows[2]) != observed_fields
        ):
            raise ValueError("historical pair closure is invalid")
        authority = rows[0]
        reference = rows[1]
        if (
            type(reference["pair_address"]) is not str
            or any(
                type(reference[name]) is not int or reference[name] < 0
                for name in observed_fields[1:]
            )
            or rows[2] != reference
            or any(
                reference[name] != authority[name]
                for name in authority_fields
            )
        ):
            raise ValueError("historical pair closure differs")
        detached[venue_id] = dict(reference)
    return detached


def _validate_historical_transaction_envelope(
    *, raw_transaction: Mapping[str, Any],
    expected_transaction: Mapping[str, Any], transaction_hash: str,
    block_hash: str, block_number: int, transaction_index: int,
    chain_id: int,
) -> None:
    if (
        type(raw_transaction) is not dict
        or type(expected_transaction) is not dict
        or type(transaction_hash) is not str
        or type(block_hash) is not str
        or any(
            type(value) is not int or value < 0
            for value in (block_number, transaction_index, chain_id)
        )
    ):
        raise ValueError("historical transaction envelope is invalid")
    exact = {
        "type": expected_transaction.get("type"),
        "from": expected_transaction.get("from"),
        "to": expected_transaction.get("to"),
        "chainId": hex(chain_id),
        "nonce": hex(expected_transaction.get("nonce", -1)),
        "gas": hex(expected_transaction.get("gas", -1)),
        "maxPriorityFeePerGas": hex(
            expected_transaction.get("maxPriorityFeePerGas", -1)
        ),
        "maxFeePerGas": hex(expected_transaction.get("maxFeePerGas", -1)),
        "value": hex(expected_transaction.get("value", -1)),
        "input": expected_transaction.get("input"),
        "accessList": expected_transaction.get("accessList"),
        "hash": transaction_hash,
        "blockHash": block_hash,
        "blockNumber": hex(block_number),
        "transactionIndex": hex(transaction_index),
    }
    if any(raw_transaction.get(name) != value for name, value in exact.items()):
        raise ValueError("historical transaction envelope differs")
    return None


def _validate_historical_child_transaction_closure(
    *, raw_child_block: Mapping[str, Any], raw_receipt: Mapping[str, Any],
    raw_transaction: Mapping[str, Any], submitted_transaction_hash: str,
    base_block_number: int, base_block_hash: str,
) -> str:
    def exact_hash(value: Any) -> bool:
        return (
            type(value) is str and len(value) == 66
            and value.startswith("0x")
            and all(character in "0123456789abcdef" for character in value[2:])
        )

    if (
        type(raw_child_block) is not dict
        or type(raw_receipt) is not dict
        or type(raw_transaction) is not dict
        or type(base_block_number) is not int
        or base_block_number < 0
        or not exact_hash(base_block_hash)
        or not exact_hash(submitted_transaction_hash)
    ):
        raise ValueError("historical child transaction closure is invalid")
    child_number = base_block_number + 1
    child_hash = raw_child_block.get("hash")
    exact = (
        raw_child_block.get("number") == hex(child_number)
        and exact_hash(child_hash)
        and raw_child_block.get("parentHash") == base_block_hash
        and raw_child_block.get("transactions")
        == [submitted_transaction_hash]
        and raw_receipt.get("transactionHash")
        == submitted_transaction_hash
        and raw_receipt.get("blockHash") == child_hash
        and raw_receipt.get("blockNumber") == hex(child_number)
        and raw_receipt.get("transactionIndex") == "0x0"
        and raw_transaction.get("hash") == submitted_transaction_hash
        and raw_transaction.get("blockHash") == child_hash
        and raw_transaction.get("blockNumber") == hex(child_number)
        and raw_transaction.get("transactionIndex") == "0x0"
    )
    if not exact:
        raise ValueError("historical child transaction closure differs")
    return child_hash


def _validate_historical_raw_trace(
    *, raw_trace: Mapping[str, Any], expected_failed: bool,
    anvil_binary_sha256: str, trace_config: Mapping[str, Any],
) -> Dict[str, Any]:
    reviewed_anvil_sha256 = (
        "5c9f9aad323062b1c0421a63595741430acaea150da3611e38c45071e4cf4e28"
    )
    if (
        type(raw_trace) is not dict
        or type(expected_failed) is not bool
        or anvil_binary_sha256 != reviewed_anvil_sha256
        or type(trace_config) is not dict
        or trace_config != _historical_struct_trace_config()
        or not {"gas", "failed", "returnValue", "structLogs"}.issubset(raw_trace)
        or type(raw_trace["gas"]) is not int
        or raw_trace["gas"] < 0
        or type(raw_trace["failed"]) is not bool
        or raw_trace["failed"] is not expected_failed
        or type(raw_trace["returnValue"]) is not str
        or type(raw_trace["structLogs"]) is not list
    ):
        raise ValueError("historical execution trace is invalid")
    required = {
        "pc": int, "op": str, "gas": int, "gasCost": int,
        "depth": int, "stack": list, "memory": list,
        "refund": int, "returnData": str,
    }
    gasprice = []
    storage_omitted = 0
    storage_explicit = 0
    previous_depth = None
    for step in raw_trace["structLogs"]:
        if type(step) is not dict or not set(required).issubset(step):
            raise ValueError("historical execution trace is invalid")
        for name, expected_type in required.items():
            if type(step[name]) is not expected_type:
                raise ValueError("historical execution trace is invalid")
        if "storage" not in step:
            storage_omitted += 1
        else:
            storage = step["storage"]
            if type(storage) is not dict or any(
                type(slot) is not str
                or re.fullmatch(r"0x[0-9a-f]{64}", slot) is None
                or type(value) is not str
                or re.fullmatch(r"0x[0-9a-f]{64}", value) is None
                for slot, value in storage.items()
            ):
                raise ValueError("historical execution trace is invalid")
            storage_explicit += 1
        if (
            step["pc"] < 0 or step["gas"] < 0 or step["gasCost"] < 0
            or step["refund"] < 0
            or step["depth"] < 1
            or not step["returnData"].startswith("0x")
            or len(step["returnData"]) % 2 != 0
            or (
                previous_depth is not None
                and abs(step["depth"] - previous_depth) > 1
            )
        ):
            raise ValueError("historical execution trace is invalid")
        previous_depth = step["depth"]
        if step["op"] == "GASPRICE":
            gasprice.append("GASPRICE")
    return {
        "schema": "historical_foundry_sparse_storage_trace/v1",
        "anvil_binary_sha256": reviewed_anvil_sha256,
        "trace_config_sha256": hashlib.sha256(
            _canonical_json(dict(trace_config))
        ).hexdigest(),
        "storage_omitted_step_count": storage_omitted,
        "storage_explicit_step_count": storage_explicit,
        "gasprice_operations": gasprice,
    }


def _extract_actual_first_leg_uni_raw(
    *, raw_receipt: Mapping[str, Any], uni_address: str,
    executor_address: str, pair_address: str,
) -> int:
    if (
        type(raw_receipt) is not dict
        or type(uni_address) is not str
        or type(executor_address) is not str
        or type(pair_address) is not str
    ):
        raise ValueError("historical receipt transfer evidence is invalid")
    topic = "0x" + keccak256(b"Transfer(address,address,uint256)").hex()
    recipient = "0x" + "0" * 24 + executor_address[2:].lower()
    sender = "0x" + "0" * 24 + pair_address[2:].lower()
    amounts = []
    logs = raw_receipt.get("logs")
    if type(logs) is not list:
        raise ValueError("historical receipt transfer evidence is invalid")
    previous_log_index = -1
    for row in logs:
        if type(row) is not dict:
            raise ValueError("historical receipt transfer evidence is invalid")
        try:
            log_index = int(row.get("logIndex", ""), 16)
        except (TypeError, ValueError):
            raise ValueError("historical receipt transfer evidence is invalid") from None
        if (
            log_index <= previous_log_index
            or row.get("transactionIndex") != "0x0"
            or type(row.get("removed")) is not bool
            or row["removed"]
        ):
            raise ValueError("historical receipt transfer evidence is invalid")
        previous_log_index = log_index
        if (
            type(row.get("address")) is not str
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
        if type(topics[1]) is not str or topics[1].lower() != sender:
            raise ValueError("historical receipt transfer evidence is invalid")
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
    if len(amounts) != 1 or amounts[0] <= 0:
        raise ValueError("historical receipt transfer evidence is invalid")
    return amounts[0]


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
    *, call_trace: Mapping[str, Any], router_order: tuple,
    root_sender: str, root_executor: str, root_input: str,
    root_failed: bool, anvil_binary_sha256: str,
    call_trace_config: Mapping[str, Any],
) -> list:
    reviewed_anvil_sha256 = (
        "5c9f9aad323062b1c0421a63595741430acaea150da3611e38c45071e4cf4e28"
    )
    if (
        type(call_trace) is not dict
        or type(router_order) is not tuple
        or len(router_order) != 2
        or any(type(value) is not str for value in router_order)
        or any(
            type(value) is not str
            for value in (root_sender, root_executor, root_input)
        )
        or type(root_failed) is not bool
        or anvil_binary_sha256 != reviewed_anvil_sha256
        or type(call_trace_config) is not dict
        or call_trace_config != {"tracer": "callTracer"}
    ):
        raise ValueError("historical call trace is invalid")
    allowed = {value.lower(): index for index, value in enumerate(router_order)}
    observed = []

    def visit(node: Any, *, path: list, expected_from: str) -> None:
        required = {
            "type", "from", "to", "input", "output", "gas", "gasUsed",
        }
        if (
            type(node) is not dict
            or not required.issubset(node)
            or node["type"] not in (
                "CALL", "CALLCODE", "STATICCALL", "DELEGATECALL"
            )
            or type(node["from"]) is not str
            or node["from"].lower() != expected_from.lower()
            or type(node["to"]) is not str
            or any(type(node[name]) is not str for name in (
                "input", "output", "gas", "gasUsed",
            ))
            or (
                node["type"] in ("CALL", "CALLCODE")
                and type(node.get("value")) is not str
            )
            or (
                node["type"] in ("STATICCALL", "DELEGATECALL")
                and "value" in node and node["value"] != "0x0"
            )
            or (
                "calls" in node and type(node["calls"]) is not list
            )
        ):
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
                "call_path": list(path),
                "leg": (
                    "first_leg" if allowed[target.lower()] == 0
                    else "second_leg"
                ),
                "router": target.lower(),
                "revert_selector": "0x" + raw[:4].hex(),
                "revert_data_sha256": hashlib.sha256(raw).hexdigest(),
            })
        for child_index, child in enumerate(node.get("calls", ())):
            visit(child, path=path + [child_index], expected_from=node["to"])

    root_has_error = bool(call_trace.get("error"))
    if (
        not {"value", "calls"}.issubset(call_trace)
        or call_trace.get("type") != "CALL"
        or call_trace.get("from", "").lower() != root_sender.lower()
        or call_trace.get("to", "").lower() != root_executor.lower()
        or call_trace.get("input", "").lower() != root_input.lower()
        or root_has_error is not root_failed
    ):
        raise ValueError("historical call trace is invalid")
    visit(call_trace, path=[], expected_from=root_sender)
    router_paths = tuple(
        (row["router"], tuple(row["call_path"])) for row in observed
    )
    if len(router_paths) != len(set(router_paths)) or len(observed) > 1:
        raise ValueError("historical call trace is invalid")
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

    def rpc_call(
        method: str, params: list, *, absolute_deadline: Any = None
    ) -> Any:
        identifier = next_id[0]
        next_id[0] += 1
        request_bytes = _canonical_json({
            "id": identifier, "jsonrpc": "2.0",
            "method": method, "params": params,
        })
        started = time.monotonic()
        clock_started = checked._clock()
        call_deadline = min(
            checked._run_deadline,
            checked._scenario_deadline
            if checked._scenario_deadline is not None
            else checked._run_deadline,
            clock_started + 30.0,
            absolute_deadline
            if type(absolute_deadline) in (int, float)
            else checked._run_deadline,
        )
        _validate_historical_local_rpc_call(
            method=method, request_byte_count=len(request_bytes),
            decoded_response_byte_count=0, elapsed_seconds=0.0,
        )
        timeout = _context_operation_remaining(
            checked, operation_deadline=call_deadline, own_cap=30.0
        )
        connection = http.client.HTTPConnection(
            "127.0.0.1", anvil_port, timeout=timeout
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
            if connection.sock is not None:
                connection.sock.settimeout(_context_operation_remaining(
                    checked, operation_deadline=call_deadline, own_cap=30.0
                ))
            response = connection.getresponse()
            header_bytes = sum(
                len(name.encode("latin-1"))
                + len(value.encode("latin-1")) + 4
                for name, value in response.getheaders()
            )
            if response.status != 200 or header_bytes > 65_536:
                raise ValueError("historical local RPC response is invalid")
            if connection.sock is not None:
                connection.sock.settimeout(_context_operation_remaining(
                    checked, operation_deadline=call_deadline, own_cap=30.0
                ))
            payload = response.read(67_108_865)
        finally:
            connection.close()
        _context_operation_remaining(
            checked, operation_deadline=call_deadline, own_cap=30.0
        )
        elapsed = time.monotonic() - started
        _validate_historical_local_rpc_call(
            method=method, request_byte_count=len(request_bytes),
            decoded_response_byte_count=len(payload),
            elapsed_seconds=elapsed,
        )
        return _decode_historical_local_rpc_response(
            payload=payload, identifier=identifier
        )

    readiness_deadline = min(
        checked._run_deadline,
        checked._scenario_deadline
        if checked._scenario_deadline is not None
        else checked._run_deadline,
        checked._clock() + 30.0,
    )
    while True:
        process_lease = getattr(checked, "_active_process_lease", None)
        if process_lease is not None:
            process_lease._assert_output_within_limit()
        try:
            if rpc_call(
                "eth_chainId", [], absolute_deadline=readiness_deadline
            ) == "0x1":
                break
        except Exception:
            pass
        try:
            sleep_seconds = _context_operation_remaining(
                checked, operation_deadline=readiness_deadline,
                own_cap=0.05,
            )
        except TimeoutError:
            raise TimeoutError("historical Anvil readiness expired")
        time.sleep(sleep_seconds)
    selected = rpc_call(
        "eth_getBlockByNumber", [hex(override["block_number"]), False]
    )
    _validate_historical_fork_base_header(
        raw_header=selected, expected_header=row["header"]
    )
    authority = checked._config.authority.value
    tokens = {token["role"]: token for token in authority["tokens"]}
    executor = authority["executor"]["address"]
    coinbase_initialized = False
    read_started = False
    coinbase_params = [executor]
    _validate_historical_local_coinbase_initialization(
        params=coinbase_params, expected_executor=executor,
        already_initialized=coinbase_initialized, read_started=read_started,
    )
    if rpc_call("anvil_setCoinbase", coinbase_params) is not None:
        raise ValueError("historical local coinbase initialization is invalid")
    coinbase_initialized = True

    def read_pair_state() -> Dict[str, Any]:
        nonlocal read_started
        if not coinbase_initialized:
            raise ValueError("historical local coinbase initialization is invalid")
        read_started = True
        observed = {}
        ordered_roles = tuple(sorted(
            ("uni", "weth"), key=lambda role: tokens[role]["address"]
        ))
        for venue_id in ("uniswap_v2", "sushiswap_v2"):
            expected_reserves = row["reserves"][venue_id]
            pair = expected_reserves["pair_address"]
            raw_reserves = rpc_call("eth_call", [_validate_historical_local_read_request(request={
                "from": executor, "to": pair, "data": "0x0902f1ac",
            }, expected_executor=executor), "latest"])
            if (
                type(raw_reserves) is not str
                or not raw_reserves.startswith("0x")
                or len(raw_reserves) != 194
            ):
                raise ValueError("historical pair closure is invalid")
            try:
                words = tuple(
                    int(raw_reserves[2 + index * 64:2 + (index + 1) * 64], 16)
                    for index in range(3)
                )
            except ValueError:
                raise ValueError("historical pair closure is invalid") from None
            reserve_by_role = dict(zip(ordered_roles, words[:2]))
            balances = {}
            for role in ("uni", "weth"):
                balances[role] = _parse_uint256_result(rpc_call(
                    "eth_call", [_validate_historical_local_read_request(request={
                        "from": executor,
                        "to": tokens[role]["address"],
                        "data": _balance_of_calldata(pair),
                    }, expected_executor=executor), "latest"]
                ))
            observed[venue_id] = {
                "pair_address": pair,
                "reserve_uni_raw": reserve_by_role["uni"],
                "reserve_weth_raw": reserve_by_role["weth"],
                "pair_uni_balance_raw": balances["uni"],
                "pair_weth_balance_raw": balances["weth"],
            }
        return observed

    expected_pair_state = {
        venue_id: {
            "pair_address": row["reserves"][venue_id]["pair_address"],
            "reserve_uni_raw": row["reserves"][venue_id]["reserve_uni_raw"],
            "reserve_weth_raw": row["reserves"][venue_id]["reserve_weth_raw"],
        }
        for venue_id in ("uniswap_v2", "sushiswap_v2")
    }
    pair_state_before = read_pair_state()
    override["pair_balance_baseline"] = {
        venue_id: {
            "pair_address": value["pair_address"],
            "pair_uni_balance_raw": value["pair_uni_balance_raw"],
            "pair_weth_balance_raw": value["pair_weth_balance_raw"],
        }
        for venue_id, value in pair_state_before.items()
    }
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
    pair_state_after = read_pair_state()
    pair_closure = _validate_historical_pair_closure(
        expected=expected_pair_state,
        before=pair_state_before,
        after=pair_state_after,
    )
    balance_call = _balance_of_calldata(executor)
    initial_weth = _parse_uint256_result(rpc_call("eth_call", [_validate_historical_local_read_request(request={
        "from": executor, "to": tokens["weth"]["address"],
        "data": balance_call,
    }, expected_executor=executor), "latest"]))
    initial_uni = _parse_uint256_result(rpc_call("eth_call", [_validate_historical_local_read_request(request={
        "from": executor, "to": tokens["uni"]["address"],
        "data": balance_call,
    }, expected_executor=executor), "latest"]))
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
    struct_trace_config = _historical_struct_trace_config()
    _validate_historical_struct_trace_config(struct_trace_config)
    raw_trace = rpc_call(
        "debug_traceTransaction", [transaction_hash, struct_trace_config]
    )
    call_trace = rpc_call(
        "debug_traceTransaction", [transaction_hash, {"tracer": "callTracer"}]
    )
    raw_transaction = rpc_call("eth_getTransactionByHash", [transaction_hash])
    child_block = rpc_call(
        "eth_getBlockByNumber", [hex(override["synthetic_block"]["number"]), False]
    )
    _validate_historical_child_transaction_closure(
        raw_child_block=child_block, raw_receipt=raw_receipt,
        raw_transaction=raw_transaction,
        submitted_transaction_hash=transaction_hash,
        base_block_number=override["block_number"],
        base_block_hash=override["block_hash"],
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
    _validate_historical_transaction_envelope(
        raw_transaction=raw_transaction,
        expected_transaction=tx,
        transaction_hash=transaction_hash,
        block_hash=receipt["blockHash"],
        block_number=receipt["blockNumber"],
        transaction_index=receipt["transactionIndex"],
        chain_id=1,
    )
    raw_trace_closure = _validate_historical_raw_trace(
        raw_trace=raw_trace, expected_failed=receipt["status"] == 0,
        anvil_binary_sha256=checked._active_process_lease._binary_sha256,
        trace_config=struct_trace_config,
    )
    gasprice_ops = raw_trace_closure["gasprice_operations"]
    gasprice_addresses = [tx["to"] for _value in gasprice_ops]
    routers = {
        venue["venue_id"]: venue["router_address"]
        for venue in authority["venues"]
    }
    first_venue = (
        "uniswap_v2"
        if row["direction"] == "uniswap_to_sushiswap"
        else "sushiswap_v2"
    )
    router_order = (
        (routers["uniswap_v2"], routers["sushiswap_v2"])
        if row["direction"] == "uniswap_to_sushiswap"
        else (routers["sushiswap_v2"], routers["uniswap_v2"])
    )
    failed_router_calls = _normalized_failed_router_calls(
        call_trace=call_trace, router_order=router_order,
        root_sender=tx["from"], root_executor=tx["to"],
        root_input=tx["input"], root_failed=receipt["status"] == 0,
        anvil_binary_sha256=checked._active_process_lease._binary_sha256,
        call_trace_config={"tracer": "callTracer"},
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
            pair_address=row["reserves"][first_venue]["pair_address"],
        )
        if actual_first_leg_uni != row["first_amount_out_raw"]:
            raise ValueError("historical first-leg token delta differs")
    final_weth = _parse_uint256_result(rpc_call("eth_call", [_validate_historical_local_read_request(request={
        "from": executor, "to": tokens["weth"]["address"],
        "data": balance_call,
    }, expected_executor=executor), "latest"]))
    residual_uni = _parse_uint256_result(rpc_call("eth_call", [_validate_historical_local_read_request(request={
        "from": executor, "to": tokens["uni"]["address"],
        "data": balance_call,
    }, expected_executor=executor), "latest"]))
    trace = {
        "schema": "historical_foundry_trace/v1",
        "scenario_key": override["scenario_key"],
        "failed": raw_trace.get("failed") is True,
        "gasprice_opcode_addresses": sorted(set(gasprice_addresses)),
        "struct_log_storage": {
            key: value for key, value in raw_trace_closure.items()
            if key != "gasprice_operations"
        },
        "struct_logs": raw_trace["structLogs"],
        "raw_trace_closure": {
            "gas": raw_trace["gas"],
            "failed": raw_trace["failed"],
            "return_value": raw_trace["returnValue"],
        },
        "calls": failed_router_calls,
        "fork_header": dict(row["header"]),
        "pair_closure": pair_closure,
        "balances": {
            "initial_weth_raw": initial_weth,
            "initial_uni_raw": initial_uni,
            "final_weth_raw": final_weth,
            "final_uni_raw": residual_uni,
        },
        "actual_deltas": {
            "first_leg_uni_raw": actual_first_leg_uni,
            "weth_raw": final_weth - initial_weth,
            "residual_uni_raw": residual_uni,
        },
    }
    return {
        "selected_state": {
            "block_number": override["block_number"],
            "block_hash": override["block_hash"],
            "state_root": override["state_root"],
            "pair_closure": pair_closure,
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
    receipt: Mapping[str, Any], token_deltas: Mapping[str, Any],
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
        or type(token_deltas) is not dict
        or set(token_deltas) != {
            "initial_weth_raw", "initial_uni_raw",
            "actual_first_leg_uni_raw", "final_weth_raw",
            "residual_uni_raw",
        }
        or any(type(value) is not int or value < 0 for value in token_deltas.values())
        or token_deltas["initial_uni_raw"] != 0
        or token_deltas["residual_uni_raw"] != 0
        or token_deltas["initial_weth_raw"] != row.get("amount_weth_in_wei")
        or token_deltas["actual_first_leg_uni_raw"]
        != row.get("first_amount_out_raw")
        or token_deltas["final_weth_raw"] != row.get("second_amount_out_raw")
    ):
        raise ValueError("historical cost proof input is invalid")
    scenario_key = row.get("scenario_key")
    if type(scenario_key) is not str or not scenario_key:
        raise ValueError("historical cost proof input is invalid")
    amount_denominator = 10 ** (18 + row["price"]["feed_decimals"])
    expected_amount_weth = (
        row["requested_notional_usd"] * amount_denominator
        // row["price"]["answer"]
    )
    if expected_amount_weth != token_deltas["initial_weth_raw"]:
        raise ValueError("historical cost proof input is invalid")
    formula = checked._config.authority.value["v2_formula"]
    fee_numerator = formula.get("fee_numerator")
    fee_denominator = formula.get("fee_denominator")
    if (
        type(fee_numerator) is not int
        or type(fee_denominator) is not int
        or not 0 < fee_numerator < fee_denominator
    ):
        raise ValueError("historical cost proof input is invalid")
    fee_units = fee_denominator - fee_numerator
    fee_bps_numerator = fee_units * 10_000
    fee_bps, fee_bps_remainder = divmod(
        fee_bps_numerator, fee_denominator
    )
    if fee_bps_remainder or fee_bps != 30:
        raise ValueError("historical cost proof input is invalid")
    first_pool_fee = _exact_terminating_decimal(
        token_deltas["initial_weth_raw"] * row["price"]["answer"]
        * fee_units,
        amount_denominator * fee_denominator,
    )
    second_venue = (
        "sushiswap_v2"
        if row.get("direction") == "uniswap_to_sushiswap"
        else "uniswap_v2"
    )
    second_reserves = row.get("reserves", {}).get(second_venue, {})
    if (
        not isinstance(second_reserves, Mapping)
        or type(second_reserves.get("reserve_uni_raw")) is not int
        or second_reserves["reserve_uni_raw"] <= 0
        or type(second_reserves.get("reserve_weth_raw")) is not int
        or second_reserves["reserve_weth_raw"] <= 0
    ):
        raise ValueError("historical cost proof input is invalid")
    second_pool_fee = _exact_terminating_decimal(
        token_deltas["actual_first_leg_uni_raw"]
        * second_reserves["reserve_weth_raw"]
        * row["price"]["answer"] * fee_units,
        second_reserves["reserve_uni_raw"]
        * amount_denominator * fee_denominator,
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
        ("buy", "pool_swap_fee", "bounded_estimate", True, first_pool_fee, str(fee_bps), "receipt"),
        ("buy", "router_or_integrator_fee", "bounded_estimate", False, "0", "0", "receipt"),
        ("buy", "token_transfer_tax", "bounded_estimate", False, "0", "0", "receipt"),
        ("sell", "pool_swap_fee", "bounded_estimate", True, second_pool_fee, str(fee_bps), "receipt"),
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


def _advance_historical_replay_context(
    *, context: HistoricalReplayContext, ledger: Any
) -> None:
    if type(context) is not HistoricalReplayContext or context._closed:
        raise ValueError("historical replay context is invalid")
    checked = context
    import scripts.historical_foundry_scan as scan

    successor, window, grid = scan._advance_validated_replay_authorities(
        ledger=ledger, window=checked._window, grid=checked._grid
    )
    object.__setattr__(checked, "_staging", successor)
    object.__setattr__(checked, "_window", window)
    object.__setattr__(checked, "_grid", grid)
    return None


def _issue_next_historical_replay_scenario(
    *, context: HistoricalReplayContext, scenario_key: str
) -> Any:
    checked = _require_context(context)
    import scripts.historical_foundry_scan as scan

    return scan._issue_validated_replay_scenario(
        staging=checked._staging, window=checked._window,
        grid=checked._grid, scenario_key=scenario_key,
    )


def _sealed_executor_router_call_path(
    *, context: HistoricalReplayContext, matrix: Mapping[str, Any]
) -> Any:
    """Bind executor call positions to the reviewed build and policy authority."""
    checked = _require_context(context)
    expected_build = checked._config.toolchain.value["executor_build"]
    observed_build = checked._artifact.verified_identity
    if (
        any(
            observed_build.get(name) != expected
            for name, expected in expected_build.items()
        )
        or observed_build.get("policy_physical_sha256")
        != checked._config.policy.physical_sha256
        or observed_build.get("authority_physical_sha256")
        != checked._config.authority.physical_sha256
        or observed_build.get("toolchain_physical_sha256")
        != checked._config.toolchain.physical_sha256
        or checked._runtime_sha256
        != expected_build.get("deployed_runtime_sha256")
    ):
        return None
    key = (
        matrix.get("prefilter_reason"), matrix.get("leg"),
        matrix.get("revert_selector"), matrix.get("revert_data_sha256"),
    )
    reviewed_paths = {
        (
            "first_leg_zero_output", "first_leg", "0x08c379a0",
            "6798eb314455c46925e230068a2e4849cf2340aefa7480b4aece1cdc6ae36ba7",
        ): [2],
        (
            "second_leg_zero_liquidity", "second_leg", "0x08c379a0",
            "9de19b1bd02b49383b079e33eb28592b7125d02f86cad8e24358a74830d1fe0b",
        ): [5],
    }
    path = reviewed_paths.get(key)
    return None if path is None else list(path)


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
    expected_path = _sealed_executor_router_call_path(
        context=checked, matrix=matrix
    )
    if expected_path is None:
        return "unresolved"
    exact_call = {
        "call_path": expected_path,
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


def _replay_historical_scenario_untyped(
    *, context: HistoricalReplayContext, scenario: Any, sink: Any
) -> Mapping[str, Any]:
    checked = _require_context(context)
    started = checked._clock()
    _remaining_historical_deadline(
        run_deadline=checked._run_deadline,
        scenario_deadline=checked._run_deadline,
        now=started, own_cap=120.0,
    )
    object.__setattr__(
        checked, "_scenario_deadline",
        min(checked._run_deadline, started + 120.0),
    )
    import scripts.historical_foundry_storage as storage

    if type(sink) is not storage.ScenarioEvidenceSink:
        raise ValueError("historical scenario evidence sink is invalid")
    override = build_historical_state_override(
        context=checked, scenario=scenario
    )
    relay = None
    process = None
    ordinary_failure = False
    ordinary_error = None
    control = None
    try:
        relay = _start_historical_relay(
            context=checked, scenario=scenario
        )
        object.__setattr__(checked, "_active_relay_lease", relay)
        _context_remaining(checked, 120.0)
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
        _context_remaining(checked, 120.0)
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
            raise HistoricalReplayError("candidate_unresolved")
        overlay_bytes = _canonical_json(override)
        receipt_bytes = _canonical_json(receipt)
        trace_bytes = _deterministic_gzip(_canonical_json(trace))
        receipt_sha = hashlib.sha256(receipt_bytes).hexdigest()
        trace_sha = hashlib.sha256(trace_bytes).hexdigest()
        import scripts.historical_foundry_scan as scan

        proof_row = scan._validate_replay_scenario_for_context(
            scenario=scenario,
            staging=checked._staging,
            window=checked._window,
            grid=checked._grid,
        )
        proof = None
        if receipt["status"] == 1:
            proof = _build_cost_proof_inputs(
                context=checked,
                row=proof_row,
                receipt=receipt,
                token_deltas=token_deltas,
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
            "fork_header": trace["fork_header"],
            "pair_closure": trace["pair_closure"],
            "balances": trace["balances"],
            "actual_deltas": trace["actual_deltas"],
            "gas": {
                "gas_used": receipt["gasUsed"],
                "effective_gas_price": receipt["effectiveGasPrice"],
                "gas_cost_wei": (
                    receipt["gasUsed"] * receipt["effectiveGasPrice"]
                ),
            },
            "receipt_closure": {
                "status": receipt["status"],
                "block_number": receipt["blockNumber"],
                "block_hash": receipt["blockHash"],
                "transaction_index": receipt["transactionIndex"],
                "transaction_hash": receipt["transactionHash"],
            },
            "trace_closure": {
                "failed": trace["failed"],
                "gasprice_opcode_addresses": trace[
                    "gasprice_opcode_addresses"
                ],
                "calls": trace["calls"],
                "raw_trace_closure": trace["raw_trace_closure"],
                "struct_log_storage": trace["struct_log_storage"],
            },
            "proof_authority": {
                "policy_sha256": checked._config.policy.physical_sha256,
                "authority_sha256": checked._config.authority.physical_sha256,
                "toolchain_sha256": checked._config.toolchain.physical_sha256,
                "executor_source_tree_sha256": checked._artifact.verified_identity[
                    "source_tree_sha256"
                ],
                "executor_constructor_args_sha256": checked._artifact.verified_identity[
                    "constructor_args_sha256"
                ],
                "anvil_binary_sha256": trace["struct_log_storage"][
                    "anvil_binary_sha256"
                ],
                "trace_config_sha256": trace["struct_log_storage"][
                    "trace_config_sha256"
                ],
                "adapter_proof_sha256": checked._artifact.verified_identity[
                    "creation_bytecode_sha256"
                ],
                "executor_runtime_sha256": checked._runtime_sha256,
                "executor_immutable_references_sha256": checked._artifact.verified_identity[
                    "immutable_references_sha256"
                ],
                "executor_artifact_manifest_sha256": checked._artifact.verified_identity[
                    "artifact_manifest_sha256"
                ],
                "requested_notional_usd": proof_row[
                    "requested_notional_usd"
                ],
                "amount_weth_in_wei": proof_row["amount_weth_in_wei"],
                "actual_first_leg_uni_raw": token_deltas[
                    "actual_first_leg_uni_raw"
                ],
                "direction": proof_row["direction"],
                "second_leg_pair_address": proof_row["reserves"][
                    "sushiswap_v2"
                    if proof_row["direction"] == "uniswap_to_sushiswap"
                    else "uniswap_v2"
                ]["pair_address"],
                "second_leg_reserve_uni_raw": proof_row["reserves"][
                    "sushiswap_v2"
                    if proof_row["direction"] == "uniswap_to_sushiswap"
                    else "uniswap_v2"
                ]["reserve_uni_raw"],
                "second_leg_reserve_weth_raw": proof_row["reserves"][
                    "sushiswap_v2"
                    if proof_row["direction"] == "uniswap_to_sushiswap"
                    else "uniswap_v2"
                ]["reserve_weth_raw"],
                "eth_usd_answer": proof_row["price"]["answer"],
                "feed_decimals": proof_row["price"]["feed_decimals"],
                "v2_fee_numerator": checked._config.authority.value[
                    "v2_formula"
                ]["fee_numerator"],
                "v2_fee_denominator": checked._config.authority.value[
                    "v2_formula"
                ]["fee_denominator"],
                "acceptance_mev_bps": checked._config.policy.value[
                    "fees"
                ]["acceptance_mev_bps"],
            },
        }
        if proof is not None:
            result["cost_proof_inputs"] = proof
        process._close_with_budget(
            lambda cap: _context_remaining(checked, cap)
        )
        if getattr(process, "_closed", False) is not True:
            raise ValueError("historical process reap failed")
        object.__setattr__(checked, "_active_process_lease", None)
        process = None
        relay.close()
        closed_check = getattr(relay, "_is_closed", None)
        if not callable(closed_check) or closed_check() is not True:
            raise ValueError("historical relay cleanup failed")
        object.__setattr__(checked, "_active_relay_lease", None)
        relay = None
        _context_remaining(checked, 120.0)
        for role, payload in (
            ("overlay", overlay_bytes),
            ("receipt", receipt_bytes),
            ("trace", trace_bytes),
            ("result", _canonical_json(result)),
        ):
            sink.write_member(role=role, canonical_bytes=payload)
        ledger = sink.validated_ledger()
        _advance_historical_replay_context(
            context=checked, ledger=ledger
        )
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
            "trace_storage_omitted_step_count": trace[
                "struct_log_storage"
            ]["storage_omitted_step_count"],
            "trace_storage_explicit_step_count": trace[
                "struct_log_storage"
            ]["storage_explicit_step_count"],
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
            ordinary_error = _typed_historical_replay_error(error)
    finally:
        for lease in (process, relay):
            closer = None
            if lease is process:
                bounded_closer = getattr(lease, "_close_with_budget", None)
                if callable(bounded_closer):
                    closer = lambda: bounded_closer(
                        lambda cap: _context_remaining(checked, cap)
                    )
            if closer is None:
                closer = getattr(lease, "close", None)
            if callable(closer):
                try:
                    closer()
                except BaseException as error:
                    if not isinstance(error, Exception) and control is None:
                        control = error
                    elif isinstance(error, Exception):
                        ordinary_failure = True
                        if ordinary_error is None:
                            ordinary_error = _typed_historical_replay_error(error)
            if lease is process and getattr(lease, "_closed", False) is True:
                object.__setattr__(checked, "_active_process_lease", None)
            if lease is relay:
                closed_check = getattr(lease, "_is_closed", None)
                if callable(closed_check) and closed_check() is True:
                    object.__setattr__(checked, "_active_relay_lease", None)
        try:
            _context_remaining(checked, 120.0)
        except BaseException as error:
            if not isinstance(error, Exception) and control is None:
                control = error
            elif isinstance(error, Exception):
                ordinary_failure = True
                if ordinary_error is None:
                    ordinary_error = _typed_historical_replay_error(error)
        object.__setattr__(checked, "_scenario_deadline", None)
        if control is not None:
            raise control
        if ordinary_failure:
            if ordinary_error is None:
                ordinary_error = HistoricalReplayError(
                    "foundry_replay_failed"
                )
            raise ordinary_error from None


def _replay_historical_scenario(
    *, context: HistoricalReplayContext, scenario: Any, sink: Any
) -> Mapping[str, Any]:
    try:
        checked = _require_context(context)
        import scripts.historical_foundry_scan as scan
        import scripts.historical_foundry_storage as storage

        if type(sink) is not storage.ScenarioEvidenceSink:
            raise _HistoricalReplayBoundaryError("authority")
        scan._validate_replay_scenario_for_context(
            scenario=scenario, staging=checked._staging,
            window=checked._window, grid=checked._grid,
        )
    except (KeyboardInterrupt, SystemExit):
        raise
    except _HistoricalReplayBoundaryError as error:
        raise HistoricalReplayError(error.category) from None
    except Exception:
        raise HistoricalReplayError("authority") from None
    try:
        return _replay_historical_scenario_untyped(
            context=checked, scenario=scenario, sink=sink
        )
    except (KeyboardInterrupt, SystemExit):
        raise
    except Exception as error:
        raise _typed_historical_replay_error(error) from None
