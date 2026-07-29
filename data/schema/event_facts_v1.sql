PRAGMA foreign_keys = ON;
PRAGMA journal_mode = DELETE;
PRAGMA synchronous = FULL;

CREATE TABLE event_fact_revisions (
    event_id TEXT NOT NULL,
    revision INTEGER NOT NULL CHECK (revision > 0),
    token_symbol TEXT NOT NULL,
    event_type TEXT NOT NULL
        CHECK (event_type IN ('unlock', 'airdrop', 'cex_listing')),
    event_subtype TEXT NOT NULL
        CHECK (
            event_subtype IN (
                'scheduled_release',
                'claim_start',
                'spot_trading_start'
            )
        ),
    event_name TEXT NOT NULL,
    lifecycle TEXT NOT NULL
        CHECK (
            lifecycle IN (
                'scheduled',
                'occurred',
                'postponed',
                'cancelled',
                'superseded'
            )
        ),
    announced_at TEXT NOT NULL,
    announced_at_precision TEXT NOT NULL
        CHECK (
            announced_at_precision IN ('second', 'minute', 'day', 'month')
            OR (announced_at = '' AND announced_at_precision = '')
        ),
    effective_at TEXT NOT NULL,
    effective_at_precision TEXT NOT NULL
        CHECK (effective_at_precision IN ('second', 'minute', 'day', 'month')),
    amount_token TEXT NOT NULL,
    amount_usd TEXT NOT NULL,
    amount_usd_basis TEXT NOT NULL
        CHECK (amount_usd_basis IN ('', 'source_reported')),
    percent_of_supply TEXT NOT NULL,
    size_relation TEXT NOT NULL
        CHECK (size_relation IN ('', 'exact', 'up_to', 'approximately')),
    venue TEXT NOT NULL,
    market_symbol TEXT NOT NULL,
    market_id TEXT NOT NULL,
    chain TEXT NOT NULL,
    related_address TEXT NOT NULL,
    related_tx_hash TEXT NOT NULL,
    source_kind TEXT NOT NULL
        CHECK (
            source_kind IN (
                'official_project',
                'official_governance',
                'official_exchange',
                'onchain_transaction'
            )
        ),
    evidence_status TEXT NOT NULL
        CHECK (
            evidence_status IN (
                'primary_confirmed',
                'cross_checked',
                'onchain_observed'
            )
        ),
    source_url TEXT NOT NULL,
    source_published_at TEXT NOT NULL,
    source_published_at_precision TEXT NOT NULL
        CHECK (
            source_published_at_precision IN ('second', 'minute', 'day', 'month')
            OR (
                source_published_at = ''
                AND source_published_at_precision = ''
            )
        ),
    source_checked_at_utc TEXT NOT NULL,
    source_record_file TEXT NOT NULL,
    record_locator TEXT NOT NULL,
    recorded_at_utc TEXT NOT NULL,
    revision_reason TEXT NOT NULL,
    notes TEXT NOT NULL,
    record_sha256 TEXT NOT NULL,
    PRIMARY KEY (event_id, revision)
);

CREATE INDEX idx_event_fact_token_time
    ON event_fact_revisions (token_symbol, effective_at);
CREATE INDEX idx_event_fact_type_time
    ON event_fact_revisions (event_type, effective_at);
CREATE INDEX idx_event_fact_lifecycle
    ON event_fact_revisions (lifecycle);

CREATE VIEW event_facts_latest AS
SELECT revisions.*
FROM event_fact_revisions AS revisions
JOIN (
    SELECT event_id, MAX(revision) AS revision
    FROM event_fact_revisions
    GROUP BY event_id
) AS latest
    ON latest.event_id = revisions.event_id
   AND latest.revision = revisions.revision;

PRAGMA user_version = 1;
