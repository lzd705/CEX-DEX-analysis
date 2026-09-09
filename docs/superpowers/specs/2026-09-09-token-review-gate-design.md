# Token Contract Review Gate Design

## Goal

Replace every direct “new Token contract → onboarding job” path with a durable,
single-reviewer approval gate. A submission must not write the runtime Token
registry, start collection, or publish data until the configured administrator
approves it and then explicitly starts onboarding.

## Confirmed product decision

- Approval policy: one approval from the only configured administrator (`1/1`).
- Reviewer mailbox: supplied by the operator as `clubc@connect.ust.hk`, but loaded
  through `TOKEN_REVIEWER_EMAIL`; it is not embedded in application code or
  committed deployment defaults.
- Email is a notification only. Its link opens the authenticated admin review
  page and never approves or starts work by itself.
- Approval and onboarding start are separate authenticated actions. This makes
  the exact point at which collection becomes authorized visible and auditable.

## State and data model

Use a dedicated SQLite file at `TOKEN_REVIEW_DB_PATH`, defaulting to
`<MARKET_DATA_DIR>/admin/token_reviews.sqlite3`. SQLite supplies atomic creation,
deduplication, compare-and-swap revisions, and an audit ledger without adding a
dependency.

Each request stores:

- a random 128-bit `request_id` and monotonically increasing `revision`;
- canonical chain, contract address, requested history days, submitter, and UTC
  timestamps;
- the bounded, source-validated candidate shown to the submitter and its
  canonical SHA-256 digest;
- status (`pending_review`, `approved`, `rejected`, `onboarding_starting`, or
  `onboarding_queued`);
- reviewer decision identity and timestamp;
- per-request notification state, attempt count, safe error code, retry time,
  and sent time;
- onboarding job id or a stable start error code; and
- append-only, bounded audit events for creation, notification, decision, and
  onboarding transitions.

The canonical `chain:contract_address` has a unique constraint. Repeated public
or admin submissions return the existing request and never enqueue another
email. A rejected request remains rejected; creating a fresh review requires a
future explicit operator workflow rather than public resubmission.

## Flow

1. Resolve uses the existing chain allowlist, address normalization, fixed
   source origin, response-size cap, and exact token/pool identity validation.
2. Submit re-resolves the candidate and checks the confirmed symbol.
3. In one database transaction, insert `pending_review`, notification outbox
   state, and the creation audit event. Commit before calling any mailer.
4. If email is enabled and fully configured, claim one delivery attempt, send a
   plain-text message, then persist `sent` or a stable retryable failure. If
   disabled or incomplete, the review remains valid and visibly reports that no
   message was sent.
5. The sole authenticated administrator reviews the persisted evidence and
   submits approve or reject with CSRF and `expected_revision`.
6. Approval changes only the review state. It does not touch the Token registry
   and does not start a worker.
7. A second authenticated, CSRF-protected Start action reserves
   `onboarding_starting`, invokes the existing onboarding service with only the
   server-stored canonical fields, and records the returned job id as
   `onboarding_queued`. A failure returns the request to `approved` with a stable
   retryable error.

## Email configuration and retry rules

The environment supplies `TOKEN_REVIEW_EMAIL_ENABLED`, `TOKEN_REVIEWER_EMAIL`,
`TOKEN_REVIEW_BASE_URL`, `TOKEN_REVIEW_SMTP_HOST`,
`TOKEN_REVIEW_SMTP_PORT`, `TOKEN_REVIEW_SMTP_STARTTLS`,
`TOKEN_REVIEW_SMTP_FROM`, `TOKEN_REVIEW_SMTP_USERNAME`, and
`TOKEN_REVIEW_SMTP_PASSWORD`. No request field can select a recipient, relay,
sender, callback, or review URL.

The email subject is fixed, headers are built with `EmailMessage`, and the body
contains only bounded validated fields. The review URL is built from the
configured base URL plus `/admin.html#token-review=<request_id>`. The contract
address is displayed as text and never interpolated into a URL.

Only failed, disabled, or unconfigured notifications can be retried by the
authenticated administrator. Retries use `expected_revision`, a 60-second
cooldown, and a maximum of three attempts. Duplicate submissions never retry.
SMTP cannot guarantee exactly-once delivery, so an in-progress attempt is not
automatically retried after an ambiguous crash.

## HTTP and UI contract

- Existing `POST /api/actions/tokens` becomes submit-for-review and returns a
  public projection containing request id, Token identity, review status,
  created time, dedupe flag, and a non-sensitive notification status.
- Existing `POST /api/admin/tokens` has the same review-only semantics with the
  administrator-selected history window.
- `GET /api/admin/token-reviews` lists bounded review projections and audit
  history for the authenticated administrator.
- `POST /api/admin/token-reviews/<id>/decision` accepts exactly `decision` and
  `expected_revision`.
- `POST /api/admin/token-reviews/<id>/notification/retry` accepts exactly
  `expected_revision`.
- `POST /api/admin/token-reviews/<id>/start` accepts exactly
  `expected_revision`.

All admin mutations require an authenticated login session and CSRF token.
Open-local admin mode may view requests but cannot decide, resend, or start
onboarding because it does not identify the configured reviewer.

## Security and failure semantics

- Public resolve and submit keep the existing per-client rate and concurrency
  limits. Their persisted daily budget counts review requests rather than jobs.
- The source HTTP client refuses redirects, preventing a source response from
  forwarding a fixed-origin lookup to another host.
- Unknown database contracts, malformed rows, invalid configuration, mail
  failures, stale revisions, invalid state transitions, and worker-start
  failures all fail closed with stable codes.
- Public responses omit reviewer address, SMTP settings, internal paths, raw
  exceptions, audit actor details, and worker output.
- Tests use a fake mailer only. This change does not send real email, deploy, or
  enable public write surfaces.

