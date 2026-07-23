# Administrator Operations

## Local setup

Generate a password verifier:

```bash
python3 scripts/admin_password.py
```

Copy `.env.example` to `.env`, set `ADMIN_USERNAME`, and paste the generated
value as `ADMIN_PASSWORD_HASH`. `.env` is ignored by Git. Restart the server,
then open:

```text
http://127.0.0.1:8765/admin.html
```

The plaintext password is never written by the setup script.

To run the administrator workspace without a login, set:

```env
ADMIN_LOGIN_REQUIRED=false
```

Open mode skips authentication but keeps Token/date validation and permits only
one queued or running refresh job at a time. Anyone who can reach
`/admin.html` can request a data refresh while this mode is enabled.

## Refresh contract

The administrator selects one of the 30 configured Tokens and an inclusive UTC
date range. Current exchange adapters support a rolling refresh only:

- `end_date` must be the latest completed UTC day;
- the window must contain 1 to 180 days;
- adding a new Token is not supported by this form.

The server queues one job at a time. A job:

1. seeds `data/processed/` from the currently published `data/local/` snapshot;
2. refreshes the selected Token from CEX and DEX sources;
3. upserts rows by venue/pool/date without deleting other Tokens or older dates;
4. validates both detailed CSV schemas;
5. builds an indexed SQLite database with source hashes and an import record;
6. validates database integrity and row counts;
7. publishes the reviewed CSV copies and atomically replaces
   `market_facts.sqlite3` as the runtime commit point under `data/local/`.

Job state and server-only logs live under `data/local/admin/jobs/`.
The public server opens `market_facts.sqlite3` read-only. Existing requests
continue using the previous complete file until replacement finishes.

## Security boundary

- Passwords use PBKDF2-SHA256 and are stored only as an environment verifier.
- Sessions are random, server-side, expire after eight hours, and use an
  HttpOnly SameSite cookie.
- Data-changing requests require a session-specific CSRF token.
- Repeated login failures are rate limited.
- Open mode is opt-in through `ADMIN_LOGIN_REQUIRED=false`.
- Pipeline commands use fixed argument arrays without a shell.
- Production requires HTTPS and `ADMIN_COOKIE_SECURE=true`.
- The data directory must be writable only by the deployment service account.

The administrator page does not make the collector capable of arbitrary
historical backfills. Supporting an older `end_date` requires source-specific
pagination changes and separate tests.
