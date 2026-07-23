# Public data boundary

Production market CSVs are not versioned with application code.

The website reads a reviewed snapshot from `data/local/` or
`MARKET_DATA_DIR`. Deployment should mount that directory read-only. Git keeps
only schemas, configuration, import code, and tests.
