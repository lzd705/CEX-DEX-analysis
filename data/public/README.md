# Public data boundary

Production market CSVs and SQLite databases are not versioned with application
code.

The website reads `market_facts.sqlite3` from `data/local/` or
`MARKET_DATA_DIR`. Deployment should mount that directory read-only when
administrator refresh is disabled. Git keeps only schemas, configuration,
import code, tests, and documentation. See `data/README.md`.
