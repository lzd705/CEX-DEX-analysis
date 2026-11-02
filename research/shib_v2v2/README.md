# SHIB/WETH V2 pool research

Source commit: `b16ee24d79806a0de655b28caa3ce6f9d248ba1e`.

- [Research calculation](scripts/shib_v2_research.py).
- [Registry](config/shib_v2_research_pools.json).
- [Retained public evidence](data/public/research/shib-v2v2/evidence.json).
- [Reference snapshot](data/public/research/shib-v2v2/latest.json).
- [Research contract](docs/shib-v2v2-research-contract.md).
- [Original source overview](SOURCE_README.md).

From the repository root: `python3 scripts/run_research.py shib-replay`.
The command rebuilds the snapshot from its public evidence and reports byte identity.
