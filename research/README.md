# Research modules

Run `python3 scripts/run_research.py list` from the repository root.

| Module | Implementation | Offline entry |
| --- | --- | --- |
| V3 exact pricing | [Integer engine](v3_exact/scripts/uniswap_v3_math.py), [collector](v3_exact/scripts/fetch_dex_depth.py), [two-pool registry](v3_exact/config/uniswap_v3_execution_markets.json) | `python3 scripts/run_research.py v3-math` |
| SHIB V2/V2 | [Pool comparison](shib_v2v2/scripts/shib_v2_research.py), [public evidence](shib_v2v2/data/public/research/shib-v2v2/evidence.json) | `python3 scripts/run_research.py shib-replay` |
| UNI CEX/V3 | [Published experiment](../experiments/uni_cex_v3_20261102/README.md) | `python3 scripts/run_research.py uni-v3-replay` |

[sources.json](sources.json) records every original file's commit, path, mode, and
SHA-256. Check the imported source files with `python3 scripts/verify_research_sources.py`.
Module subprocesses use their own working directory and Python import context.
