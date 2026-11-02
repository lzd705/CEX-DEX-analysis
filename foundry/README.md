# Historical UNI/WETH fork replay

Source commit: `83e66079f679c8133c6a6415238d27377d42293b`.

[TwoVenueV2Executor](src/TwoVenueV2Executor.sol) follows WETH → UNI → WETH through
the registry-fixed Uniswap and SushiSwap routers. It validates router identities,
sender, input inventory, and residual state. The current harness passes
`amountOutMin = 0` to each router swap; its swap deadline is `block.timestamp + 60`.

- [Fork tests](test/TwoVenueV2Fork.t.sol) and [unit tests](test/TwoVenueV2Unit.t.sol).
- [Toolchain configuration](../config/historical_foundry_replay_toolchain.json).
- [Replay policy](../config/historical_foundry_replay_policy.json).
- [Bootstrap utility](../scripts/bootstrap_historical_foundry_toolchain.py).
- [Replay entry](../scripts/run_historical_foundry_replay.py).

The independent historical presentation workflow starts with
`python3 scripts/run_research.py historical-demo --port 0` from the repository root.
