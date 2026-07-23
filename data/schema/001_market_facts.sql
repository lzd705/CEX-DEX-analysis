PRAGMA foreign_keys = ON;
PRAGMA journal_mode = DELETE;
PRAGMA synchronous = FULL;

CREATE TABLE schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL
);

CREATE TABLE dataset_snapshots (
    snapshot_id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL,
    available_start TEXT NOT NULL,
    available_end TEXT NOT NULL,
    token_count INTEGER NOT NULL CHECK (token_count > 0),
    cex_row_count INTEGER NOT NULL CHECK (cex_row_count > 0),
    dex_row_count INTEGER NOT NULL CHECK (dex_row_count > 0),
    cex_source_name TEXT NOT NULL,
    dex_source_name TEXT NOT NULL,
    cex_source_bytes INTEGER NOT NULL,
    dex_source_bytes INTEGER NOT NULL,
    cex_sha256 TEXT NOT NULL,
    dex_sha256 TEXT NOT NULL
);

CREATE TABLE import_runs (
    run_id TEXT PRIMARY KEY,
    snapshot_id TEXT NOT NULL REFERENCES dataset_snapshots(snapshot_id),
    imported_at TEXT NOT NULL,
    source_directory TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('published'))
);

CREATE TABLE dataset_state (
    singleton_id INTEGER PRIMARY KEY CHECK (singleton_id = 1),
    snapshot_id TEXT NOT NULL REFERENCES dataset_snapshots(snapshot_id),
    import_run_id TEXT NOT NULL REFERENCES import_runs(run_id)
);

CREATE TABLE tokens (
    token_symbol TEXT PRIMARY KEY,
    first_observed_date TEXT NOT NULL,
    last_observed_date TEXT NOT NULL
);

CREATE TABLE cex_market_daily (
    date TEXT NOT NULL,
    token_symbol TEXT NOT NULL REFERENCES tokens(token_symbol),
    exchange TEXT NOT NULL,
    cex_symbol TEXT NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    base_volume REAL,
    quote_volume_usd REAL,
    PRIMARY KEY (date, token_symbol, exchange, cex_symbol)
);

CREATE TABLE dex_pool_daily (
    date TEXT NOT NULL,
    token_symbol TEXT NOT NULL REFERENCES tokens(token_symbol),
    chain TEXT NOT NULL,
    dex TEXT NOT NULL,
    pool_address TEXT NOT NULL,
    pool_name TEXT NOT NULL,
    open REAL,
    high REAL,
    low REAL,
    close REAL,
    dex_volume_usd REAL,
    pool_tvl_usd REAL,
    PRIMARY KEY (date, token_symbol, chain, pool_address)
);

CREATE INDEX idx_cex_token_date
    ON cex_market_daily (token_symbol, date);
CREATE INDEX idx_cex_date
    ON cex_market_daily (date);
CREATE INDEX idx_dex_token_date
    ON dex_pool_daily (token_symbol, date);
CREATE INDEX idx_dex_date
    ON dex_pool_daily (date);

PRAGMA user_version = 1;
