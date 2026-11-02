# Uniswap V3 exact pricing

Source commit: `401eaad12965560ffd67e60caad60d2a267e206d`.

- [Integer swap engine](scripts/uniswap_v3_math.py): exact input/output, fee rounding, price limits, and tick crossing.
- [Collection and verification](scripts/fetch_dex_depth.py): fixed-state depth/quotes and same-block QuoterV2 comparisons.
- [UNI pool registry](config/uniswap_v3_execution_markets.json): UNI/USDT and UNI/WETH on Ethereum.
- [Two-pool canary entry](scripts/run_uniswap_v3_canary.py).
- [Math tests](tests/test_uniswap_v3_math.py).
- [Original source overview](SOURCE_README.md).

From the repository root: `python3 scripts/run_research.py v3-math`.
From this directory, other focused tests can use `python3 -B -m unittest discover -s tests -p 'test_uniswap_v3_*.py' -v`.
