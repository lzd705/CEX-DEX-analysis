# UNI CEX ↔ Uniswap V3: 51-block experiment

Observed on **2026-11-02, 13:10:45–13:20:41 HKT**.

The experiment pairs public Binance UNI/USDT order books with block-hash-pinned
Ethereum Uniswap V3 UNI/USDT QuoterV2 quotes. Its pool fee is 30 bp. Both route
directions use a fixed net UNI quantity for each of five initial size anchors:
$1K, $5K, $10K, $50K, and $100K.

## Observation and scenario inventory

| Item | Count |
| --- | ---: |
| Polls | 60 |
| Distinct accepted blocks | 51 |
| Duplicate-head polls | 9 |
| DEX quotes: 51 × 2 directions × 5 sizes | 510 |
| Fee/Gas rows: 51 × 2 × 5 × 6 fee assumptions × 7 Gas assumptions | 21,420 |
| Delay scenario rows, including explicit tail/missing results | 13,770 |
| Positive fee/Gas cash-flow rows | 0 |

The 51 blocks are the independent state observations. The larger row counts
describe parameter scenarios evaluated on those same states.

## Reproduce offline

From the repository root:

```bash
python3 scripts/run_research.py uni-v3-replay
```

The command verifies the retained capture and creates a new output directory
under ignored `data/local/`. It compares all four regenerated analysis files
byte-for-byte against `results/` and prints counts, hashes, and the output path.
An explicit new destination can be selected with `--output-dir`.

The replay uses the retained source data and pure quote/cash-flow functions.
Its regression test runs with socket connections denied and checks that all
capture and reference-result file hashes remain unchanged.

## Files

- [capture/manifest.json](capture/manifest.json): identities, collection configuration, rule evidence, raw response inventory, and sample hash.
- [capture/samples.jsonl](capture/samples.jsonl): 60 collection slots with books, block identity, Quoter outputs, and timing metadata.
- `capture/raw/`: original public order-book, rule, and JSON-RPC responses.
- [results/fee_gas_timeseries.csv](results/fee_gas_timeseries.csv): both route directions across the fee/Gas grid.
- [results/delay_pairs.csv](results/delay_pairs.csv): exact +1/+2/+3 block matching and actual elapsed times.
- [results/network_gas_scenarios.csv](results/network_gas_scenarios.csv): assumed transaction units with observed network fee inputs.
- [results/summary.json](results/summary.json): grouped results and observation-age statistics.
- [evidence-index.json](evidence-index.json): publication source hashes, privacy review, and experiment identity.
- [FILES.sha256](FILES.sha256): published dataset and guide integrity inventory.

## Calculation

Six CEX fee assumptions are 0, 1, 2, 5, 10, and 20 bp. Seven Gas assumptions
are 0, 0.1, 0.5, 1, 2, 5, and 10 USDT. The pool fee and price impact are already
embedded in Quoter inputs/outputs. The CEX fee model includes receiving-asset
fees, quantity gross-up, and the experiment's conservative fee-increment rule.

For the $1K anchor, fee=10 bp and Gas=1 USDT, the best sampled
Binance-buy → V3-sell cash flow is **−2.578299 USDT**. At zero CEX fee and zero
Gas, both directions and all sizes remain negative in this window.

Delay analysis matches exact block offsets, holds the UNI quantity fixed,
and evaluates both complete repricing and a conditional CEX-first cash flow.
The observation/decision delays are measured in seconds. Initial-positive
signal survival is recorded as undefined for this zero-positive sample.

## Implementation

- [Collector and cash flows](../../scripts/uni_cex_v3_experiment.py).
- [Raw verifier and analysis](../../scripts/analyze_uni_cex_v3_experiment.py).
- [Offline replay](../../scripts/replay_uni_cex_v3.py).
- [Detailed experiment report](../../docs/uni-cex-v3-experiment-2026-11-02.md).
- [Replay regression tests](../../tests/test_replay_uni_cex_v3.py).
