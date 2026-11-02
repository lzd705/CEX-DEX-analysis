# Research implementation index

## Start here

| Question | Implementation / evidence |
| --- | --- |
| How is data collected and validated? | [Collection cycle](../scripts/run_collection_cycle.py), [operations](collection-operations.md), [data lifecycle](../data/README.md) |
| How does CEX execution consume liquidity? | [Book collector](../scripts/fetch_cex_depth.py), [quantity engine](../scripts/route_quantity.py) |
| How do V3 rounding and tick crossing work? | [Integer engine](../research/v3_exact/scripts/uniswap_v3_math.py), [math tests](../research/v3_exact/tests/test_uniswap_v3_math.py) |
| How are V3 quotes checked? | [State collector](../research/v3_exact/scripts/fetch_dex_depth.py), [canary](../research/v3_exact/scripts/run_uniswap_v3_canary.py) |
| How are SHIB pool edges reproduced? | [Calculation](../research/shib_v2v2/scripts/shib_v2_research.py), [evidence](../research/shib_v2v2/data/public/research/shib-v2v2/evidence.json) |
| How is historical two-leg execution modeled? | [Foundry harness](../foundry/src/TwoVenueV2Executor.sol), [toolchain](../foundry/README.md) |
| What happened in the 51-block UNI experiment? | [Experiment guide](../experiments/uni_cex_v3_20261102/README.md), [report](uni-cex-v3-experiment-2026-11-02.md) |

## Fixed source versions

- Root application and Foundry: `83e66079f679c8133c6a6415238d27377d42293b`.
- V3 exact module: `401eaad12965560ffd67e60caad60d2a267e206d`.
- SHIB V2/V2 module: `b16ee24d79806a0de655b28caa3ce6f9d248ba1e`.
- November experiment: working-tree code and retained response hashes in its [evidence index](../experiments/uni_cex_v3_20261102/evidence-index.json).

[Source inventory](../research/sources.json) maps original paths to the published
tree. The original application overview is preserved under [source-readmes](source-readmes/application-83e66079.md).

## Reproduction entry

`python3 scripts/run_research.py list` lists the fixed commands. V3 and SHIB
execute in their own module directories with isolated import contexts. UNI replay
uses the root quantity engine and its published experiment. Results go to new
local output directories, separate from committed input/reference data.

For full V3 module tests:

```bash
cd research/v3_exact
python3 -B -m unittest discover -s tests -p 'test_uniswap_v3_*.py' -v
```

For the SHIB module tests:

```bash
cd research/shib_v2v2
python3 -B -m unittest discover -s tests -p 'test_shib_v2_research.py' -v
```
